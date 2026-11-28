// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BinaryHeap;
use std::mem::size_of;

use risingwave_common_estimate_size::EstimateSize;

use crate::memory::{MemoryContext, MonitoredGlobalAlloc};

pub struct MemMonitoredHeap<T> {
    inner: BinaryHeap<T>,
    mem_ctx: MemoryContext,
    /// The estimated heap size of the items in the heap, counted in `mem_ctx`.
    items_heap_size: usize,
}

impl<T: Ord + EstimateSize> MemMonitoredHeap<T> {
    pub fn new_with(mem_ctx: MemoryContext) -> Self {
        Self {
            inner: BinaryHeap::new(),
            mem_ctx,
            items_heap_size: 0,
        }
    }

    pub fn with_capacity(capacity: usize, mem_ctx: MemoryContext) -> Self {
        let inner = BinaryHeap::with_capacity(capacity);
        mem_ctx.force_add((capacity * size_of::<T>()) as i64);
        Self {
            inner,
            mem_ctx,
            items_heap_size: 0,
        }
    }

    pub fn push(&mut self, item: T) {
        let prev_cap = self.inner.capacity();
        let item_heap = item.estimated_heap_size();
        self.items_heap_size += item_heap;
        self.inner.push(item);
        let new_cap = self.inner.capacity();
        self.mem_ctx
            .force_add(((new_cap - prev_cap) * size_of::<T>() + item_heap) as i64);
    }

    pub fn pop(&mut self) -> Option<T> {
        let prev_cap = self.inner.capacity();
        let item = self.inner.pop();
        let item_heap = item.as_ref().map(|i| i.estimated_heap_size()).unwrap_or(0);
        self.items_heap_size -= item_heap;
        let new_cap = self.inner.capacity();
        self.mem_ctx
            .force_add(-(((prev_cap - new_cap) * size_of::<T>() + item_heap) as i64));

        item
    }

    /// Reserves room for exactly `additional` more items, counted in the memory context.
    pub fn reserve_exact(&mut self, additional: usize) {
        let prev_cap = self.inner.capacity();
        self.inner.reserve_exact(additional);
        let new_cap = self.inner.capacity();
        self.mem_ctx
            .force_add(((new_cap - prev_cap) * size_of::<T>()) as i64);
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn peek(&self) -> Option<&T> {
        self.inner.peek()
    }

    /// The items, sorted. Only the buffer of the returned vector is counted in the memory
    /// context: the heap memory of the items is no longer counted.
    pub fn into_sorted_vec(mut self) -> Vec<T, MonitoredGlobalAlloc> {
        let inner = std::mem::take(&mut self.inner);
        let old_cap = inner.capacity();
        let alloc = MonitoredGlobalAlloc::with_memory_context(self.mem_ctx.clone());
        let vec = inner.into_iter_sorted();

        let mut ret = Vec::with_capacity_in(vec.len(), alloc);
        ret.extend(vec);

        self.mem_ctx
            .force_add(-((old_cap * size_of::<T>() + self.items_heap_size) as i64));
        self.items_heap_size = 0;
        ret
    }

    pub fn mem_context(&self) -> &MemoryContext {
        &self.mem_ctx
    }
}

impl<T> Extend<T> for MemMonitoredHeap<T>
where
    T: Ord + EstimateSize,
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let old_cap = self.inner.capacity();
        let mut items_heap_size = 0usize;
        let items = iter.into_iter();
        self.inner.reserve_exact(items.size_hint().0);
        for item in items {
            items_heap_size += item.estimated_heap_size();
            self.inner.push(item);
        }

        let new_cap = self.inner.capacity();
        self.items_heap_size += items_heap_size;

        let diff = (new_cap - old_cap) * size_of::<T>() + items_heap_size;
        self.mem_ctx.force_add(diff as i64);
    }
}

impl<T> Drop for MemMonitoredHeap<T> {
    fn drop(&mut self) {
        // What the heap still holds is given back, like the allocations of a monitored vector.
        self.mem_ctx
            .force_add(-((self.inner.capacity() * size_of::<T>() + self.items_heap_size) as i64));
    }
}

#[cfg(test)]
mod tests {
    use super::MemMonitoredHeap;
    use crate::memory::MemoryContext;
    use crate::metrics::LabelGuardedIntGauge;

    #[test]
    fn test_heap() {
        let gauge = LabelGuardedIntGauge::test_int_gauge::<4>();
        let mem_ctx = MemoryContext::root(gauge.clone(), u64::MAX);

        let mut heap = MemMonitoredHeap::<u8>::new_with(mem_ctx);
        assert_eq!(0, gauge.get());

        heap.push(9u8);
        heap.push(1u8);
        assert_eq!(heap.inner.capacity() as i64, gauge.get());

        heap.pop().unwrap();
        assert_eq!(heap.inner.capacity() as i64, gauge.get());

        assert!(!heap.is_empty());
    }

    #[test]
    fn test_heap_reserve_exact() {
        let gauge = LabelGuardedIntGauge::test_int_gauge::<4>();
        let mem_ctx = MemoryContext::root(gauge.clone(), u64::MAX);

        let mut heap = MemMonitoredHeap::<u64>::new_with(mem_ctx);
        heap.reserve_exact(3);
        assert_eq!(heap.capacity(), 3);
        assert_eq!(24, gauge.get());
        heap.push(1);
        heap.push(2);
        heap.push(3);
        assert_eq!(heap.capacity(), 3);
        assert_eq!(24, gauge.get());
        drop(heap.into_sorted_vec());
        assert_eq!(0, gauge.get());
    }

    #[test]
    fn test_heap_dropped_gives_back_its_items() {
        let gauge = LabelGuardedIntGauge::test_int_gauge::<4>();
        let mem_ctx = MemoryContext::root(gauge.clone(), u64::MAX);

        let mut heap = MemMonitoredHeap::<Vec<u64>>::with_capacity(4, mem_ctx);
        heap.push(vec![1, 2, 3]);
        heap.push(Vec::with_capacity(10));
        assert!(gauge.get() >= (4 * std::mem::size_of::<Vec<u64>>() + 13 * 8) as i64);
        drop(heap);
        assert_eq!(0, gauge.get());
    }

    #[test]
    fn test_heap_drop() {
        let gauge = LabelGuardedIntGauge::test_int_gauge::<4>();
        let mem_ctx = MemoryContext::root(gauge.clone(), u64::MAX);

        let vec = {
            let mut heap = MemMonitoredHeap::<u8>::new_with(mem_ctx);
            assert_eq!(0, gauge.get());

            heap.push(9u8);
            heap.push(1u8);
            assert_eq!(heap.inner.capacity() as i64, gauge.get());

            heap.into_sorted_vec()
        };

        assert_eq!(2, gauge.get());

        drop(vec);

        assert_eq!(0, gauge.get());
    }
}
