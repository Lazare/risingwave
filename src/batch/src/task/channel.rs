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

use std::sync::Arc;

use risingwave_common::array::DataChunk;
use risingwave_pb::batch_plan::ExchangeInfo;
use risingwave_pb::batch_plan::exchange_info::DistributionMode as ShuffleDistributionMode;

use crate::error::{BatchError, Result, SharedResult};
use crate::task::broadcast_channel::{BroadcastReceiver, BroadcastSender, new_broadcast_channel};
use crate::task::consistent_hash_shuffle_channel::{
    ConsistentHashShuffleReceiver, ConsistentHashShuffleSender, new_consistent_shuffle_channel,
};
use crate::task::data_chunk_in_channel::DataChunkInChannel;
use crate::task::fifo_channel::{FifoReceiver, FifoSender, new_fifo_channel};
use crate::task::hash_shuffle_channel::{
    HashShuffleReceiver, HashShuffleSender, new_hash_shuffle_channel,
};
use crate::task::output_buffer::OutputBuffer;

pub(super) trait ChanSender: Send {
    /// This function will block until there's enough resource to process the chunk.
    /// Currently, it will only be called from single thread.
    /// `None` is sent as a mark of the ending of channel.
    async fn send(&mut self, chunk: DataChunk) -> Result<()>;

    /// Close this data channel.
    ///
    /// If finished correctly, we should pass `None`, otherwise we should pass `BatchError`. In
    /// either case we should stop sending more data.
    async fn close(self, error: Option<Arc<BatchError>>) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum ChanSenderImpl {
    HashShuffle(HashShuffleSender),
    ConsistentHashShuffle(ConsistentHashShuffleSender),
    Fifo(FifoSender),
    Broadcast(BroadcastSender),
}

impl ChanSenderImpl {
    pub(super) async fn send(&mut self, chunk: DataChunk) -> Result<()> {
        match self {
            Self::HashShuffle(sender) => sender.send(chunk).await,
            Self::ConsistentHashShuffle(sender) => sender.send(chunk).await,
            Self::Fifo(sender) => sender.send(chunk).await,
            Self::Broadcast(sender) => sender.send(chunk).await,
        }
    }

    pub(super) async fn close(self, error: Option<Arc<BatchError>>) -> Result<()> {
        match self {
            Self::HashShuffle(sender) => sender.close(error).await,
            Self::ConsistentHashShuffle(sender) => sender.close(error).await,
            Self::Fifo(sender) => sender.close(error).await,
            Self::Broadcast(sender) => sender.close(error).await,
        }
    }
}

pub(super) trait ChanReceiver: Send {
    /// Returns `None` if there's no more data to read.
    /// Otherwise it will wait until there's data.
    async fn recv(&mut self) -> SharedResult<Option<DataChunkInChannel>>;
}

pub enum ChanReceiverImpl {
    HashShuffle(HashShuffleReceiver),
    ConsistentHashShuffle(ConsistentHashShuffleReceiver),
    Fifo(FifoReceiver),
    Broadcast(BroadcastReceiver),
}

impl ChanReceiverImpl {
    pub(super) async fn recv(&mut self) -> SharedResult<Option<DataChunkInChannel>> {
        match self {
            Self::HashShuffle(receiver) => receiver.recv().await,
            Self::ConsistentHashShuffle(receiver) => receiver.recv().await,
            Self::Broadcast(receiver) => receiver.recv().await,
            Self::Fifo(receiver) => receiver.recv().await,
        }
    }
}

/// How a task sends its output.
#[derive(Clone)]
pub struct OutputOptions {
    /// The chunks that each output channel holds at most.
    pub channel_size: usize,
    /// Counts, and optionally bounds in bytes, the chunks that wait in the channels.
    pub buffer: OutputBuffer,
    /// `batch_compact_shuffle_output`: a hash shuffle sends each output chunks of its own rows,
    /// of this size, instead of slices of its input chunks.
    pub compact_shuffle_chunk_size: Option<usize>,
}

/// Output-channel is a synchronous, bounded single-producer-multiple-consumer queue.
/// The producer is the local task executor, the consumer is
/// [`BatchExchangeService`](risingwave_pb::task_service::batch_exchange_service_server::BatchExchangeService).
/// The implementation depends on the shuffling strategy.
pub fn create_output_channel(
    shuffle: &ExchangeInfo,
    options: OutputOptions,
) -> Result<(ChanSenderImpl, Vec<ChanReceiverImpl>)> {
    let OutputOptions {
        channel_size,
        buffer,
        compact_shuffle_chunk_size,
    } = options;
    match shuffle.get_mode()? {
        ShuffleDistributionMode::Single => Ok(new_fifo_channel(channel_size, buffer)),
        ShuffleDistributionMode::Hash => Ok(new_hash_shuffle_channel(
            shuffle,
            channel_size,
            buffer,
            compact_shuffle_chunk_size,
        )),
        ShuffleDistributionMode::ConsistentHash => Ok(new_consistent_shuffle_channel(
            shuffle,
            channel_size,
            buffer,
        )),
        ShuffleDistributionMode::Broadcast => {
            Ok(new_broadcast_channel(shuffle, channel_size, buffer))
        }
        ShuffleDistributionMode::Unspecified => unreachable!(),
    }
}
