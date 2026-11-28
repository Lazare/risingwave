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

//! The memory of the chunks that wait in the output channels of a task, until their consumer
//! takes them (local exchange) or until they are handed to the gRPC stream of a remote consumer.
//!
//! Each chunk is counted once in the memory context of the task when it enters the channels, and
//! released when the last of its copies leaves them: the slices that a hash shuffle sends to its
//! outputs share the columns of the chunk, and so share its count. Optionally, the bytes that wait
//! are bounded: the producer then waits for its consumers before it sends more.

use std::sync::Arc;

use risingwave_common::array::DataChunk;
use risingwave_common::memory::MemoryContext;
use risingwave_common_estimate_size::EstimateSize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The bytes that the permits of the bound stand for.
const PERMIT_BYTES: u64 = 1 << 10;

/// What the output channels of a task count and bound. Shared by all its outputs.
#[derive(Clone)]
pub struct OutputBuffer {
    mem_context: MemoryContext,
    /// Bounds the bytes that wait in the channels, in permits of [`PERMIT_BYTES`].
    limit: Option<(Arc<Semaphore>, u32)>,
}

impl OutputBuffer {
    /// Counts the chunks in `mem_context`, and bounds them to `limit_bytes` when it is not 0.
    pub fn new(mem_context: MemoryContext, limit_bytes: u64) -> Self {
        let limit = (limit_bytes > 0).then(|| {
            let permits = limit_bytes
                .div_ceil(PERMIT_BYTES)
                .clamp(1, Semaphore::MAX_PERMITS.min(u32::MAX as usize) as u64)
                as u32;
            (Arc::new(Semaphore::new(permits as usize)), permits)
        });
        Self { mem_context, limit }
    }

    /// Neither counted nor bounded.
    pub fn unbounded() -> Self {
        Self::new(MemoryContext::none(), 0)
    }

    /// Counts `chunk` while it waits in the channels, after waiting for room when the buffer is
    /// bounded. A chunk larger than the whole bound waits for the buffer to be empty.
    pub async fn hold(&self, chunk: &DataChunk) -> HeldBytes {
        let bytes = chunk.estimated_heap_size() as u64;
        let permit = match &self.limit {
            Some((semaphore, max_permits)) => {
                let permits = bytes.div_ceil(PERMIT_BYTES).clamp(1, *max_permits as u64) as u32;
                // The semaphore is never closed.
                semaphore.clone().acquire_many_owned(permits).await.ok()
            }
            None => None,
        };
        self.mem_context.force_add(bytes as i64);
        HeldBytes(Some(Arc::new(Held {
            mem_context: self.mem_context.clone(),
            bytes: bytes as i64,
            _permit: permit,
        })))
    }

    /// The bytes counted for the chunks that currently wait.
    pub fn held_bytes(&self) -> i64 {
        self.mem_context.get_bytes_used()
    }
}

/// The count of a chunk in the output channels, shared by its copies, released when the last of
/// them is dropped.
#[derive(Clone, Default)]
pub struct HeldBytes(Option<Arc<Held>>);

impl HeldBytes {
    pub fn none() -> Self {
        Self(None)
    }
}

impl std::fmt::Debug for HeldBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("HeldBytes")
            .field(&self.0.as_ref().map_or(0, |held| held.bytes))
            .finish()
    }
}

struct Held {
    mem_context: MemoryContext,
    bytes: i64,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Drop for Held {
    fn drop(&mut self) {
        self.mem_context.add(-self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::test_prelude::DataChunkTestExt;

    use super::*;

    fn chunk() -> DataChunk {
        DataChunk::from_pretty(
            "I T
             1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
             2 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
    }

    #[tokio::test]
    async fn test_chunk_counted_once_while_held() {
        let mem_context = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), 0);
        let buffer = OutputBuffer::new(mem_context.clone(), 0);
        let chunk = chunk();
        let bytes = chunk.estimated_heap_size() as i64;
        let held = buffer.hold(&chunk).await;
        // Counted even beyond the limit of the context: the chunk is there.
        assert_eq!(mem_context.get_bytes_used(), bytes);
        // The slices of a shuffle share the count, released with the last of them.
        let mut slices = vec![held.clone(), held.clone(), held];
        assert_eq!(buffer.held_bytes(), bytes);
        slices.truncate(1);
        assert_eq!(buffer.held_bytes(), bytes);
        drop(slices);
        assert_eq!(buffer.held_bytes(), 0);
    }

    #[tokio::test]
    async fn test_bounded_buffer_waits_for_room() {
        let mem_context =
            MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let chunk = chunk();
        let bytes = chunk.estimated_heap_size() as u64;
        // Room for one chunk and a half.
        let buffer = OutputBuffer::new(mem_context, bytes * 3 / 2);
        let first = buffer.hold(&chunk).await;
        let waiter = {
            let buffer = buffer.clone();
            let chunk = chunk.clone();
            tokio::spawn(async move { buffer.hold(&chunk).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        // The first chunk leaves the channels: the second one enters.
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buffer.held_bytes(), bytes as i64);
        drop(second);
        // A chunk larger than the whole bound waits for an empty buffer, never forever.
        let small = OutputBuffer::new(MemoryContext::none(), 1);
        let held = small.hold(&chunk).await;
        drop(held);
        let _held = tokio::time::timeout(Duration::from_secs(5), small.hold(&chunk))
            .await
            .unwrap();
    }
}
