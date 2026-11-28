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

//! The memory of the chunks that a task sends to its consumers.
//!
//! A chunk is counted once in the memory context of the task (a child of the memory of its query)
//! when it enters the output channels: the count is attached to the chunk
//! ([`SharedChunkMemory`]), shared by the slices that a hash shuffle sends to its outputs and by
//! every chunk that keeps sharing the columns downstream, and released when the last of them is
//! dropped. The consumers of a stage run the same plan and usually all keep their slices or all
//! drop them: sharing is the common case, so the slices are not copied. An operator that keeps a
//! slice counts its share in its own context and takes it out of the shared count.
//!
//! Optionally, the bytes that wait in the channels are bounded: the producer then waits for its
//! consumers before it sends more. The bound only covers the time spent in the channels (and in
//! the `GetData` stream of a remote consumer): an operator that keeps a slice must never block the
//! producer that it waits for.

use std::sync::Arc;

use risingwave_common::array::DataChunk;
use risingwave_common::memory::{MemoryContext, SharedChunkMemory};
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

    /// Prepares `chunk` to enter the channels: waits for room when the buffer is bounded (a chunk
    /// larger than the whole bound waits for the buffer to be empty), and counts its columns once,
    /// until the last chunk that shares them is dropped. Columns already counted upstream (a chunk
    /// that came from another exchange and still shares its columns) are not counted again.
    ///
    /// Returns the chunk, and the hold on the bound, released when the chunk leaves the channels.
    pub async fn hold(&self, chunk: DataChunk) -> (DataChunk, HeldBytes) {
        let bytes = chunk.estimated_heap_size() as u64;
        let permit = match &self.limit {
            Some((semaphore, max_permits)) => {
                let permits = bytes.div_ceil(PERMIT_BYTES).clamp(1, *max_permits as u64) as u32;
                // The semaphore is never closed.
                semaphore.clone().acquire_many_owned(permits).await.ok()
            }
            None => None,
        };
        let chunk = if chunk.shared_memory().is_some() {
            chunk
        } else {
            let memory = SharedChunkMemory::new(self.mem_context.clone(), bytes as i64);
            chunk.with_shared_memory(Arc::new(memory))
        };
        (chunk, HeldBytes::new(MemoryContext::none(), 0, permit))
    }

    /// Counts `bytes` held outside of any chunk, e.g. a serialized chunk waiting for gRPC.
    pub fn count(&self, bytes: usize) -> HeldBytes {
        self.mem_context.force_add(bytes as i64);
        HeldBytes::new(self.mem_context.clone(), bytes as i64, None)
    }

    /// The bytes counted in the memory of the task for its output.
    pub fn held_bytes(&self) -> i64 {
        self.mem_context.get_bytes_used()
    }
}

/// Holds on the output buffer (room in the bound, bytes counted outside of chunks), released when
/// the last copy is dropped.
#[derive(Clone, Default)]
pub struct HeldBytes(Vec<Arc<Held>>);

impl HeldBytes {
    fn new(mem_context: MemoryContext, bytes: i64, permit: Option<OwnedSemaphorePermit>) -> Self {
        if bytes == 0 && permit.is_none() {
            return Self::none();
        }
        Self(vec![Arc::new(Held {
            mem_context,
            bytes,
            _permit: permit,
        })])
    }

    pub fn none() -> Self {
        Self(vec![])
    }

    /// Holds both.
    pub fn and(mut self, other: HeldBytes) -> Self {
        self.0.extend(other.0);
        self
    }
}

impl std::fmt::Debug for HeldBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("HeldBytes")
            .field(&self.0.iter().map(|held| held.bytes).sum::<i64>())
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
    async fn test_chunk_counted_once_until_its_last_holder() {
        let mem_context = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), 0);
        let buffer = OutputBuffer::new(mem_context.clone(), 0);
        let input = chunk();
        let bytes = input.estimated_heap_size() as i64;
        let (sent, held) = buffer.hold(input).await;
        // Counted even beyond the limit of the context: the chunk is there.
        assert_eq!(buffer.held_bytes(), bytes);
        // The slices share the count, also once they left the channels.
        let slices = vec![
            sent.clone(),
            sent.with_visibility(sent.visibility().clone()),
            sent,
        ];
        drop(held);
        assert_eq!(buffer.held_bytes(), bytes);
        // Sent again by a task that forwards it: not counted twice.
        let (forwarded, _held) = buffer.hold(slices[0].clone()).await;
        assert_eq!(buffer.held_bytes(), bytes);
        drop(forwarded);
        // A fully visible chunk is not copied by `compact_vis`: it still shares the count.
        assert!(slices[1].clone().compact_vis().shared_memory().is_some());
        // A copy of the visible rows does not share the count.
        let copy = slices[1]
            .with_visibility(risingwave_common::bitmap::Bitmap::from_bool_slice(&[
                true, false,
            ]))
            .compact_vis();
        assert!(copy.shared_memory().is_none());
        drop(slices);
        assert_eq!(buffer.held_bytes(), 0);
        drop(copy);
        // Bytes held outside of chunks.
        let serialized = buffer.count(100);
        assert_eq!(buffer.held_bytes(), 100);
        drop(serialized);
        assert_eq!(buffer.held_bytes(), 0);
    }

    #[tokio::test]
    async fn test_bounded_buffer_waits_for_room() {
        let mem_context =
            MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let input = chunk();
        let bytes = input.estimated_heap_size() as u64;
        // Room for one chunk and a half.
        let buffer = OutputBuffer::new(mem_context, bytes * 3 / 2);
        let (first, first_held) = buffer.hold(input.clone()).await;
        let waiter = {
            let buffer = buffer.clone();
            let input = input.clone();
            tokio::spawn(async move { buffer.hold(input).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        // The first chunk leaves the channels: the second one enters, although a consumer still
        // keeps the first one.
        drop(first_held);
        let (_second, _second_held) = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        drop(first);
        // A chunk larger than the whole bound waits for an empty buffer, never forever.
        let small = OutputBuffer::new(MemoryContext::none(), 1);
        let (_, held) = small.hold(input.clone()).await;
        drop(held);
        let _held = tokio::time::timeout(Duration::from_secs(5), small.hold(input))
            .await
            .unwrap();
    }
}
