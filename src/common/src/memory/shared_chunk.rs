// Copyright 2026 RisingWave Labs
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

use std::sync::atomic::{AtomicI64, Ordering};

use crate::memory::MemoryContext;

/// The memory of the columns of a chunk that several chunks share, e.g. the slices that a hash
/// exchange sends to its outputs: each output gets the whole columns with the visibility of its
/// rows.
///
/// It is counted once, in the context given at creation, and released when the last chunk that
/// shares the columns is dropped (the chunks hold it in an `Arc`). An operator that keeps one of
/// these chunks counts its share in its own context and takes that share out of this count with
/// [`adopt`](Self::adopt), so that the block is never counted twice.
pub struct SharedChunkMemory {
    mem_context: MemoryContext,
    /// The bytes of the whole block.
    bytes: i64,
    /// What is still counted in `mem_context`: the block minus the shares adopted by operators.
    counted: AtomicI64,
}

impl SharedChunkMemory {
    /// Counts `bytes` in `mem_context`, whatever its limit: the block is already allocated.
    pub fn new(mem_context: MemoryContext, bytes: i64) -> Self {
        let bytes = bytes.max(0);
        mem_context.force_add(bytes);
        Self {
            mem_context,
            bytes,
            counted: AtomicI64::new(bytes),
        }
    }

    /// The bytes of the whole block.
    pub fn bytes(&self) -> i64 {
        self.bytes
    }

    /// The bytes still counted here.
    pub fn counted(&self) -> i64 {
        self.counted.load(Ordering::Relaxed)
    }

    /// Takes at most `share` bytes out of this count, for an operator that counts them in its own
    /// context. Returns what was taken.
    pub fn adopt(&self, share: i64) -> i64 {
        let share = share.max(0);
        let mut taken = 0;
        let _ = self
            .counted
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |counted| {
                taken = share.min(counted);
                Some(counted - taken)
            });
        if taken > 0 {
            self.mem_context.add(-taken);
        }
        taken
    }
}

impl Drop for SharedChunkMemory {
    fn drop(&mut self) {
        self.mem_context.add(-self.counted.load(Ordering::Relaxed));
    }
}

impl std::fmt::Debug for SharedChunkMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedChunkMemory")
            .field("bytes", &self.bytes)
            .field("counted", &self.counted())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prometheus::core::Atomic;
    use risingwave_common_metrics::TrAdderAtomic;

    use super::*;

    #[test]
    fn test_counted_once_and_adopted_shares() {
        let query = MemoryContext::root(TrAdderAtomic::new(0), 0);
        let operator = MemoryContext::new(Some(query.clone()), TrAdderAtomic::new(0));
        let block = Arc::new(SharedChunkMemory::new(query.clone(), 1000));
        // Counted beyond the limit: the block is there.
        assert_eq!(query.get_bytes_used(), 1000);
        // An operator keeps a slice of a quarter of the rows: it counts its share, which leaves
        // the count of the block.
        let taken = block.adopt(250);
        assert_eq!(taken, 250);
        operator.force_add(taken);
        assert_eq!(query.get_bytes_used(), 1000);
        assert_eq!(block.counted(), 750);
        // Never more than what is left.
        assert_eq!(block.adopt(10_000), 750);
        assert_eq!(block.adopt(1), 0);
        operator.force_add(750);
        assert_eq!(query.get_bytes_used(), 1000);
        // The last holder drops the block: what is still counted here is released.
        drop(block);
        assert_eq!(query.get_bytes_used(), 1000);
        drop(operator);
        assert_eq!(query.get_bytes_used(), 0);
        // Not adopted: released with the last holder.
        let block = Arc::new(SharedChunkMemory::new(query.clone(), 1000));
        let copies = vec![block.clone(), block.clone()];
        drop(block);
        assert_eq!(query.get_bytes_used(), 1000);
        drop(copies);
        assert_eq!(query.get_bytes_used(), 0);
    }
}
