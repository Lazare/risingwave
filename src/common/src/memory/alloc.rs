// Copyright 2023 RisingWave Labs
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

use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;

use allocator_api2::alloc::{AllocError as AllocErrorApi2, Allocator as AllocatorApi2};

use crate::memory::MemoryContext;

pub type MonitoredGlobalAlloc = MonitoredAlloc<Global>;

pub struct MonitoredAlloc<A: Allocator> {
    ctx: MemoryContext,
    alloc: A,
}

impl<A: Allocator> MonitoredAlloc<A> {
    pub fn new(ctx: MemoryContext, alloc: A) -> Self {
        Self { ctx, alloc }
    }
}

impl MonitoredGlobalAlloc {
    pub fn with_memory_context(ctx: MemoryContext) -> Self {
        Self { ctx, alloc: Global }
    }

    pub fn for_test() -> Self {
        Self::with_memory_context(MemoryContext::none())
    }
}

unsafe impl<A: Allocator> Allocator for MonitoredAlloc<A> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ret = self.alloc.allocate(layout)?;
        // We don't throw an AllocError if the memory context is out of memory, otherwise the whole process will crash.
        // The memory is allocated anyway: count it even beyond the limit of the context, or its
        // deallocation would be subtracted without having been added.
        self.ctx.force_add(layout.size() as i64);
        Ok(ret)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            self.alloc.deallocate(ptr, layout);
            self.ctx.force_add(-(layout.size() as i64));
        }
    }
}

unsafe impl<A: Allocator> AllocatorApi2 for MonitoredAlloc<A> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocErrorApi2> {
        let ret = self.alloc.allocate(layout).map_err(|_| AllocErrorApi2)?;
        // Keep memory accounting behavior consistent with the std::alloc::Allocator path.
        self.ctx.force_add(layout.size() as i64);
        Ok(ret)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            self.alloc.deallocate(ptr, layout);
            self.ctx.force_add(-(layout.size() as i64));
        }
    }
}

impl<A: Allocator + Clone> Clone for MonitoredAlloc<A> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            alloc: self.alloc.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Atomic;
    use risingwave_common_metrics::TrAdderAtomic;

    use super::*;

    /// An allocation beyond the limit of the context is made anyway: it must be counted, or its
    /// deallocation would make the context count less than what it holds.
    #[test]
    fn test_allocation_beyond_the_limit_is_counted() {
        let root = MemoryContext::root(TrAdderAtomic::new(0), 1024);
        let ctx = MemoryContext::new(Some(root.clone()), TrAdderAtomic::new(0));
        let small: Vec<u64, _> = Vec::with_capacity_in(16, ctx.global_allocator());
        assert_eq!(ctx.get_bytes_used(), 128);
        let large: Vec<u64, _> = Vec::with_capacity_in(1024, ctx.global_allocator());
        assert_eq!(ctx.get_bytes_used(), 128 + 8192);
        assert_eq!(root.get_bytes_used(), 128 + 8192);
        // The context is over its limit: nothing else is granted.
        assert!(!ctx.add(1));
        drop(large);
        assert_eq!(root.get_bytes_used(), 128);
        drop(small);
        assert_eq!(ctx.get_bytes_used(), 0);
        assert_eq!(root.get_bytes_used(), 0);
    }
}
