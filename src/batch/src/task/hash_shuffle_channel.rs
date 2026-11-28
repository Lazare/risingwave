// Copyright 2022 RisingWave Labs
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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::hash_util::Crc32FastBuilder;
use risingwave_pb::batch_plan::exchange_info::HashInfo;
use risingwave_pb::batch_plan::*;
use tokio::sync::mpsc;

use crate::error::BatchError::{Internal, SenderError};
use crate::error::{BatchError, Result as BatchResult, SharedResult};
use crate::task::channel::{ChanReceiver, ChanReceiverImpl, ChanSender, ChanSenderImpl};
use crate::task::data_chunk_in_channel::DataChunkInChannel;
use crate::task::output_buffer::OutputBuffer;

#[derive(Clone)]
pub struct HashShuffleSender {
    senders: Vec<mpsc::Sender<SharedResult<Option<DataChunkInChannel>>>>,
    hash_info: HashInfo,
    buffer: OutputBuffer,
    /// `batch_compact_shuffle_output`: the rows of each output are copied into chunks of their
    /// own, sent when full, instead of sending each output the whole chunk with a visibility.
    compact: Option<CompactOutputs>,
}

/// The chunks being filled for each output, in the compact mode.
struct CompactOutputs {
    chunk_size: usize,
    /// Created with the first chunk, whose types they take.
    builders: Vec<DataChunkBuilder>,
}

impl Clone for CompactOutputs {
    /// The sender is cloned before it sends anything: the clone starts with no rows.
    fn clone(&self) -> Self {
        Self {
            chunk_size: self.chunk_size,
            builders: vec![],
        }
    }
}

impl Debug for HashShuffleSender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashShuffleSender")
            .field("hash_info", &self.hash_info)
            .field("compact", &self.compact.is_some())
            .finish()
    }
}

pub struct HashShuffleReceiver {
    receiver: mpsc::Receiver<SharedResult<Option<DataChunkInChannel>>>,
}

fn generate_hash_values(chunk: &DataChunk, hash_info: &HashInfo) -> BatchResult<Vec<usize>> {
    let output_count = hash_info.output_count as usize;

    let hasher_builder = Crc32FastBuilder;

    let hash_values = chunk
        .get_hash_values(
            &hash_info
                .key
                .iter()
                .map(|idx| *idx as usize)
                .collect::<Vec<_>>(),
            hasher_builder,
        )
        .iter_mut()
        .map(|hash_value| hash_value.value() as usize % output_count)
        .collect::<Vec<_>>();
    Ok(hash_values)
}

/// The visibility of the rows of `chunk` for each of the `output_count` outputs, given the output
/// of each row: one pass over the rows.
pub(super) fn output_visibilities(
    chunk: &DataChunk,
    output_count: usize,
    outputs: &[usize],
) -> Vec<Bitmap> {
    let mut builders = (0..output_count)
        .map(|_| BitmapBuilder::zeroed(outputs.len()))
        .collect::<Vec<_>>();
    for (row, output) in outputs.iter().enumerate() {
        builders[*output].set(row, true);
    }
    builders
        .into_iter()
        .map(|builder| builder.finish() & chunk.visibility())
        .collect()
}

/// The returned chunks must have cardinality > 0.
fn generate_new_data_chunks(
    chunk: &DataChunk,
    hash_info: &exchange_info::HashInfo,
    hash_values: &[usize],
) -> Vec<DataChunk> {
    let output_count = hash_info.output_count as usize;
    let mut res = Vec::with_capacity(output_count);
    for (sink_id, vis_map) in output_visibilities(chunk, output_count, hash_values)
        .into_iter()
        .enumerate()
    {
        let new_data_chunk = chunk.with_visibility(vis_map);
        trace!(
            "send to sink:{}, cardinality:{}",
            sink_id,
            new_data_chunk.cardinality()
        );
        res.push(new_data_chunk);
    }
    res
}

impl ChanSender for HashShuffleSender {
    async fn send(&mut self, chunk: DataChunk) -> BatchResult<()> {
        self.send_chunk(chunk).await
    }

    async fn close(self, error: Option<Arc<BatchError>>) -> BatchResult<()> {
        self.send_done(error).await
    }
}

impl HashShuffleSender {
    async fn send_chunk(&mut self, chunk: DataChunk) -> BatchResult<()> {
        if let Some(compact) = &mut self.compact {
            let hash_values = generate_hash_values(&chunk, &self.hash_info)?;
            let new_data_chunks = generate_new_data_chunks(&chunk, &self.hash_info, &hash_values);
            if compact.builders.is_empty() {
                compact.builders = (0..self.senders.len())
                    .map(|_| DataChunkBuilder::new(chunk.data_types(), compact.chunk_size))
                    .collect();
            }
            for (sink_id, new_data_chunk) in new_data_chunks.into_iter().enumerate() {
                if new_data_chunk.cardinality() == 0 || self.senders[sink_id].is_closed() {
                    continue;
                }
                let full_chunks = compact.builders[sink_id]
                    .append_chunk(new_data_chunk)
                    .collect::<Vec<_>>();
                for full_chunk in full_chunks {
                    send_one(&self.senders[sink_id], &self.buffer, full_chunk).await;
                }
            }
            return all_outputs_closed(&self.senders);
        }

        // Counted once: the slices share the columns of the chunk, and its count.
        let (chunk, held) = self.buffer.hold(chunk).await;
        let hash_values = generate_hash_values(&chunk, &self.hash_info)?;
        let new_data_chunks = generate_new_data_chunks(&chunk, &self.hash_info, &hash_values);
        for (sink_id, new_data_chunk) in new_data_chunks.into_iter().enumerate() {
            trace!(
                "send to sink:{}, cardinality:{}",
                sink_id,
                new_data_chunk.cardinality()
            );
            // The reason we need to add this filter only in HashShuffleSender is that
            // `generate_new_data_chunks` may generate an empty chunk.
            // An output whose consumer stopped reading is skipped (see `all_outputs_closed`).
            if new_data_chunk.cardinality() > 0 && !self.senders[sink_id].is_closed() {
                let _ = self.senders[sink_id]
                    .send(Ok(Some(DataChunkInChannel::new(
                        new_data_chunk,
                        held.clone(),
                    ))))
                    .await;
            }
        }
        all_outputs_closed(&self.senders)
    }

    async fn send_done(mut self, error: Option<Arc<BatchError>>) -> BatchResult<()> {
        // The rows still being gathered are sent before the end, unless the task failed.
        if error.is_none()
            && let Some(compact) = &mut self.compact
        {
            for (sink_id, builder) in compact.builders.iter_mut().enumerate() {
                if let Some(chunk) = builder.consume_all()
                    && !self.senders[sink_id].is_closed()
                {
                    send_one(&self.senders[sink_id], &self.buffer, chunk).await;
                }
            }
        }
        send_end(self.senders, error).await
    }
}

/// Sends a chunk of its own to one output, counted in the buffer. Nothing happens when the
/// consumer of the output stopped reading.
async fn send_one(
    sender: &mpsc::Sender<SharedResult<Option<DataChunkInChannel>>>,
    buffer: &OutputBuffer,
    chunk: DataChunk,
) {
    let (chunk, held) = buffer.hold(chunk).await;
    let _ = sender
        .send(Ok(Some(DataChunkInChannel::new(chunk, held))))
        .await;
}

/// A task with several outputs (hash, consistent hash, broadcast) keeps sending to the outputs
/// whose consumer still reads when another consumer stopped reading early (a limit above it in its
/// own task, a top-n per group without order under a limit): the other consumers read the same
/// producers and need the rest of their rows and the end of their input, or they would wait for
/// them forever. The task stops, with [`SenderError`], only when every consumer stopped.
pub(super) fn all_outputs_closed(
    senders: &[mpsc::Sender<SharedResult<Option<DataChunkInChannel>>>],
) -> BatchResult<()> {
    if !senders.is_empty() && senders.iter().all(|sender| sender.is_closed()) {
        Err(SenderError)
    } else {
        Ok(())
    }
}

/// Sends the end of the input (or the error of the task) to every output still read. See
/// [`all_outputs_closed`].
pub(super) async fn send_end(
    senders: Vec<mpsc::Sender<SharedResult<Option<DataChunkInChannel>>>>,
    error: Option<Arc<BatchError>>,
) -> BatchResult<()> {
    let closed = all_outputs_closed(&senders);
    for sender in senders {
        if !sender.is_closed() {
            let _ = sender
                .send(error.clone().map(Err).unwrap_or(Ok(None)))
                .await;
        }
    }
    closed
}

impl ChanReceiver for HashShuffleReceiver {
    async fn recv(&mut self) -> SharedResult<Option<DataChunkInChannel>> {
        match self.receiver.recv().await {
            Some(data_chunk) => data_chunk,
            // Early close should be treated as error.
            None => Err(Arc::new(Internal(anyhow!("broken hash_shuffle_channel")))),
        }
    }
}

/// `compact_chunk_size`: the size of the chunks sent to each output in the compact mode
/// (`batch_compact_shuffle_output`), `None` to send slices of the input chunks.
pub fn new_hash_shuffle_channel(
    shuffle: &ExchangeInfo,
    output_channel_size: usize,
    buffer: OutputBuffer,
    compact_chunk_size: Option<usize>,
) -> (ChanSenderImpl, Vec<ChanReceiverImpl>) {
    let hash_info = match shuffle.distribution {
        Some(exchange_info::Distribution::HashInfo(ref v)) => v.clone(),
        _ => exchange_info::HashInfo::default(),
    };

    let output_count = hash_info.output_count as usize;
    let mut senders = Vec::with_capacity(output_count);
    let mut receivers = Vec::with_capacity(output_count);
    for _ in 0..output_count {
        let (s, r) = mpsc::channel(output_channel_size);
        senders.push(s);
        receivers.push(r);
    }
    let channel_sender = ChanSenderImpl::HashShuffle(HashShuffleSender {
        senders,
        hash_info,
        buffer,
        compact: compact_chunk_size.map(|chunk_size| CompactOutputs {
            chunk_size: chunk_size.max(1),
            builders: vec![],
        }),
    });
    let channel_receivers = receivers
        .into_iter()
        .map(|receiver| ChanReceiverImpl::HashShuffle(HashShuffleReceiver { receiver }))
        .collect::<Vec<_>>();
    (channel_sender, channel_receivers)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::memory::MemoryContext;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common_estimate_size::EstimateSize;
    use risingwave_pb::batch_plan::exchange_info::DistributionMode;

    use super::*;

    const OUTPUTS: usize = 4;

    fn hash_exchange() -> ExchangeInfo {
        ExchangeInfo {
            mode: DistributionMode::Hash as i32,
            distribution: Some(exchange_info::Distribution::HashInfo(HashInfo {
                output_count: OUTPUTS as u32,
                key: vec![0],
            })),
        }
    }

    fn chunk(from: i64, rows: i64) -> DataChunk {
        DataChunk::new(
            vec![
                I64Array::from_iter(from..from + rows).into_ref(),
                I64Array::from_iter((from..from + rows).map(|v| v * 10)).into_ref(),
            ],
            rows as usize,
        )
    }

    /// The rows received by each output, and the chunks received.
    async fn receive_all(receivers: Vec<ChanReceiverImpl>) -> (Vec<Vec<i64>>, Vec<DataChunk>) {
        let mut rows = vec![];
        let mut chunks = vec![];
        for mut receiver in receivers {
            let mut output_rows = vec![];
            while let Some(chunk) = receiver.recv().await.unwrap() {
                let chunk = chunk.into_data_chunk();
                let compacted = chunk.clone().compact_vis();
                output_rows.extend(compacted.column_at(0).as_int64().iter().map(|v| v.unwrap()));
                chunks.push(chunk);
            }
            rows.push(output_rows);
        }
        (rows, chunks)
    }

    #[test]
    fn test_output_visibilities() {
        let chunk = chunk(0, 6).with_visibility(Bitmap::from_bool_slice(&[
            true, true, false, true, true, true,
        ]));
        let visibilities = output_visibilities(&chunk, 3, &[0, 1, 0, 2, 2, 0]);
        let as_bools = |b: &Bitmap| b.iter().collect::<Vec<_>>();
        assert_eq!(
            as_bools(&visibilities[0]),
            [true, false, false, false, false, true]
        );
        assert_eq!(
            as_bools(&visibilities[1]),
            [false, true, false, false, false, false]
        );
        assert_eq!(
            as_bools(&visibilities[2]),
            [false, false, false, true, true, false]
        );
    }

    /// The slices that the outputs receive share one count of the chunk, released when the last
    /// of them is dropped, wherever it went.
    #[tokio::test]
    async fn test_shared_chunk_counted_once() {
        let mem_context =
            MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let (mut sender, receivers) = new_hash_shuffle_channel(
            &hash_exchange(),
            8,
            OutputBuffer::new(mem_context.clone(), 0),
            None,
        );
        let input = chunk(0, 1000);
        let bytes = input.estimated_heap_size() as i64;
        sender.send(input).await.unwrap();
        assert_eq!(mem_context.get_bytes_used(), bytes);
        sender.close(None).await.unwrap();
        let (rows, mut chunks) = receive_all(receivers).await;
        assert_eq!(chunks.len(), OUTPUTS);
        // The consumers hold the slices: still counted, once.
        assert_eq!(mem_context.get_bytes_used(), bytes);
        // A consumer that keeps its slice counts its share elsewhere, which leaves the count.
        let share = chunks[0].kept_bytes() as i64;
        assert!(share > 0 && share < bytes);
        chunks[0].adopt_shared_memory();
        assert_eq!(mem_context.get_bytes_used(), bytes - share);
        // Released with the last slice only.
        let last = chunks.pop().unwrap();
        drop(chunks);
        assert_eq!(mem_context.get_bytes_used(), bytes - share);
        assert!(last.is_sole_holder_of_shared_memory());
        drop(last);
        assert_eq!(mem_context.get_bytes_used(), 0);
        let mut all = rows.concat();
        all.sort_unstable();
        assert_eq!(all, (0..1000).collect::<Vec<_>>());
    }

    /// In the compact mode, each output receives full chunks of its own rows, every row once,
    /// and the rows still being gathered when the sender closes.
    #[tokio::test]
    async fn test_compact_mode_sends_full_chunks_of_own_rows() {
        let mem_context =
            MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let (mut sender, receivers) = new_hash_shuffle_channel(
            &hash_exchange(),
            64,
            OutputBuffer::new(mem_context.clone(), 0),
            Some(100),
        );
        for i in 0..10 {
            sender.send(chunk(i * 100, 100)).await.unwrap();
        }
        // Only full chunks were sent so far: fewer rows than sent are counted in the channels.
        let held = mem_context.get_bytes_used();
        assert!(held > 0);
        sender.close(None).await.unwrap();
        let (rows, chunks) = receive_all(receivers).await;
        for chunk in &chunks {
            // Chunks of their own rows: nothing hidden, nothing shared.
            assert!(chunk.visibility().all());
            assert!(chunk.cardinality() <= 100);
        }
        // Full chunks, except the last one of each output.
        let partial = chunks.iter().filter(|c| c.cardinality() < 100).count();
        assert!(partial <= OUTPUTS, "{partial} partial chunks");
        let mut all = rows.concat();
        all.sort_unstable();
        assert_eq!(all, (0..1000).collect::<Vec<_>>());
        // Each chunk of its own carries its count, released with it.
        assert!(mem_context.get_bytes_used() > 0);
        drop(chunks);
        assert_eq!(mem_context.get_bytes_used(), 0);
        // The same rows go to the same output as without the compact mode.
        let (mut plain_sender, plain_receivers) =
            new_hash_shuffle_channel(&hash_exchange(), 64, OutputBuffer::unbounded(), None);
        for i in 0..10 {
            plain_sender.send(chunk(i * 100, 100)).await.unwrap();
        }
        plain_sender.close(None).await.unwrap();
        let (plain_rows, _) = receive_all(plain_receivers).await;
        for (mut compact, mut plain) in rows.into_iter().zip(plain_rows) {
            compact.sort_unstable();
            plain.sort_unstable();
            assert_eq!(compact, plain);
        }
    }

    /// A consumer that stops reading early does not deprive the others: they still receive all
    /// their rows and the end of their input. The sender only fails once every consumer stopped.
    #[tokio::test]
    async fn test_outputs_still_read_when_one_consumer_stops() {
        for compact in [None, Some(100)] {
            let (mut reference, reference_receivers) =
                new_hash_shuffle_channel(&hash_exchange(), 64, OutputBuffer::unbounded(), compact);
            let (mut sender, mut receivers) =
                new_hash_shuffle_channel(&hash_exchange(), 64, OutputBuffer::unbounded(), compact);
            drop(receivers.remove(0));
            for i in 0..10 {
                reference.send(chunk(i * 100, 100)).await.unwrap();
                sender.send(chunk(i * 100, 100)).await.unwrap();
            }
            reference.close(None).await.unwrap();
            sender.close(None).await.unwrap();
            let (expected, _) = receive_all(reference_receivers).await;
            let (rows, _) = receive_all(receivers).await;
            assert_eq!(rows, expected[1..].to_vec(), "compact {compact:?}");
            assert!(rows.iter().all(|output| !output.is_empty()));
        }

        let (mut sender, receivers) =
            new_hash_shuffle_channel(&hash_exchange(), 64, OutputBuffer::unbounded(), None);
        drop(receivers);
        assert!(matches!(sender.send(chunk(0, 100)).await, Err(SenderError)));
        assert!(matches!(sender.close(None).await, Err(SenderError)));
    }

    /// A bounded buffer makes the sender wait for its consumers.
    #[tokio::test]
    async fn test_bounded_buffer_makes_the_sender_wait() {
        let input = chunk(0, 1000);
        let bytes = input.estimated_heap_size() as u64;
        // Room for one chunk and a half, many chunks per channel.
        let (mut sender, mut receivers) = new_hash_shuffle_channel(
            &hash_exchange(),
            64,
            OutputBuffer::new(MemoryContext::none(), bytes * 3 / 2),
            None,
        );
        sender.send(input.clone()).await.unwrap();
        let second = tokio::spawn(async move {
            sender.send(input).await.unwrap();
            sender
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!second.is_finished());
        // The consumers take their slices of the first chunk: the second one is sent.
        for receiver in &mut receivers {
            drop(receiver.recv().await.unwrap());
        }
        let sender = tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap();
        sender.close(None).await.unwrap();
    }

    /// Prints what the shuffle costs on wide rows: the visibilities of the outputs (one pass
    /// against the former loop over the outputs for each row), sending slices against copying
    /// the rows of each output into chunks of their own, and compacting a slice on the consumer
    /// side. Run with `--run-ignored only --no-capture`.
    #[tokio::test]
    #[ignore]
    async fn measure_shuffle_cost() {
        use std::time::Instant;

        use risingwave_common::array::Utf8Array;

        const ROUNDS: usize = 200;
        let rows = 1024usize;
        let values = (0..rows).map(|i| format!("{i:0>1000}")).collect::<Vec<_>>();
        let wide = DataChunk::new(
            vec![
                I64Array::from_iter(0..rows as i64).into_ref(),
                Utf8Array::from_iter(values.iter().map(|v| v.as_str())).into_ref(),
            ],
            rows,
        );
        let bytes = wide.estimated_heap_size();
        for outputs in [4usize, 16, 64] {
            let hash_values = (0..rows).map(|i| (i * 7919) % outputs).collect::<Vec<_>>();

            let start = Instant::now();
            for _ in 0..ROUNDS {
                let mut vis_maps = vec![vec![]; outputs];
                for hash in &hash_values {
                    for (sink_id, vis_map) in vis_maps.iter_mut().enumerate() {
                        vis_map.push(*hash == sink_id);
                    }
                }
                let maps = vis_maps
                    .iter()
                    .map(|v| Bitmap::from_bool_slice(v) & wide.visibility())
                    .collect::<Vec<_>>();
                std::hint::black_box(maps);
            }
            let former = start.elapsed() / ROUNDS as u32;

            let start = Instant::now();
            for _ in 0..ROUNDS {
                std::hint::black_box(output_visibilities(&wide, outputs, &hash_values));
            }
            let one_pass = start.elapsed() / ROUNDS as u32;

            let visibilities = output_visibilities(&wide, outputs, &hash_values);
            let start = Instant::now();
            for _ in 0..ROUNDS {
                let slices = visibilities
                    .iter()
                    .map(|v| wide.with_visibility(v.clone()))
                    .collect::<Vec<_>>();
                std::hint::black_box(slices);
            }
            let slices = start.elapsed() / ROUNDS as u32;

            let mut builders = (0..outputs)
                .map(|_| DataChunkBuilder::new(wide.data_types(), 1024))
                .collect::<Vec<_>>();
            let start = Instant::now();
            for _ in 0..ROUNDS {
                for (builder, v) in builders.iter_mut().zip(&visibilities) {
                    let full = builder
                        .append_chunk(wide.with_visibility(v.clone()))
                        .collect::<Vec<_>>();
                    std::hint::black_box(full);
                }
            }
            let copies = start.elapsed() / ROUNDS as u32;

            let slice = wide.with_visibility(visibilities[0].clone());
            let start = Instant::now();
            for _ in 0..ROUNDS {
                let _ = std::hint::black_box(slice.clone().compact_vis());
            }
            let compact_one = start.elapsed() / ROUNDS as u32;

            println!(
                "shuffle cost: chunk of {rows} rows, {bytes} bytes, {outputs} outputs: \
                 visibilities {former:?} (former) vs {one_pass:?} (one pass); \
                 slices {slices:?} vs copies into chunks {copies:?}; \
                 compacting one slice on the consumer side {compact_one:?}"
            );
        }
    }
}
