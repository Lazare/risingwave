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

use anyhow::Context;
use risingwave_pb::task_service::GetDataResponse;
use tonic::Status;

use crate::error::Result;
use crate::task::output_buffer::HeldBytes;

pub type GetDataResponseResult = std::result::Result<GetDataResponse, Status>;

/// A response of the `GetData` stream, with the count of its chunk in the memory of the task that
/// produced it: released when the stream hands the response to gRPC.
pub type GetDataItem = (GetDataResponseResult, HeldBytes);

type ExchangeDataSender = tokio::sync::mpsc::Sender<GetDataResponseResult>;

pub trait ExchangeWriter: Send {
    async fn write(&mut self, resp: GetDataResponseResult) -> Result<()>;

    /// Writes the response of a chunk counted as `held` in the memory of its task. By default
    /// the count is released before the response is written.
    async fn write_held(&mut self, resp: GetDataResponseResult, held: HeldBytes) -> Result<()> {
        drop(held);
        self.write(resp).await
    }
}

/// Writes to the `GetData` stream of a remote consumer, the responses staying counted in the
/// memory of the task until the stream hands them to gRPC.
pub struct HeldExchangeWriter {
    sender: tokio::sync::mpsc::Sender<GetDataItem>,
    written_chunks: usize,
}

impl HeldExchangeWriter {
    pub fn new(sender: tokio::sync::mpsc::Sender<GetDataItem>) -> Self {
        Self {
            sender,
            written_chunks: 0,
        }
    }

    pub fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

impl ExchangeWriter for HeldExchangeWriter {
    async fn write(&mut self, resp: GetDataResponseResult) -> Result<()> {
        self.write_held(resp, HeldBytes::none()).await
    }

    async fn write_held(&mut self, resp: GetDataResponseResult, held: HeldBytes) -> Result<()> {
        self.sender
            .send((resp, held))
            .await
            .context("failed to write data to ExchangeWriter")?;
        self.written_chunks += 1;
        Ok(())
    }
}

pub struct GrpcExchangeWriter {
    sender: ExchangeDataSender,
    written_chunks: usize,
}

impl GrpcExchangeWriter {
    pub fn new(sender: ExchangeDataSender) -> Self {
        Self {
            sender,
            written_chunks: 0,
        }
    }

    pub fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

impl ExchangeWriter for GrpcExchangeWriter {
    async fn write(&mut self, data: GetDataResponseResult) -> Result<()> {
        self.sender
            .send(data)
            .await
            .context("failed to write data to ExchangeWriter")?;
        self.written_chunks += 1;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::task_service::GetDataResponse;

    use crate::rpc::service::exchange::{ExchangeWriter, GrpcExchangeWriter};

    #[tokio::test]
    async fn test_exchange_writer() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let mut writer = GrpcExchangeWriter::new(tx);
        writer.write(Ok(GetDataResponse::default())).await.unwrap();
        assert_eq!(writer.written_chunks(), 1);
    }

    /// The chunk of a response written to a remote consumer stays counted until the stream
    /// hands the response to gRPC.
    #[tokio::test]
    async fn test_held_writer_keeps_the_count_until_taken() {
        use risingwave_common::memory::MemoryContext;
        use risingwave_common::metrics::LabelGuardedIntGauge;

        use crate::rpc::service::exchange::HeldExchangeWriter;
        use crate::task::output_buffer::OutputBuffer;

        let mem_context =
            MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let buffer = OutputBuffer::new(mem_context.clone(), 0);
        let held = buffer.count(100);
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let mut writer = HeldExchangeWriter::new(tx);
        writer
            .write_held(Ok(GetDataResponse::default()), held)
            .await
            .unwrap();
        assert_eq!(mem_context.get_bytes_used(), 100);
        let item = rx.recv().await.unwrap();
        assert_eq!(mem_context.get_bytes_used(), 100);
        drop(item);
        assert_eq!(mem_context.get_bytes_used(), 0);
        assert_eq!(writer.written_chunks(), 1);
    }

    #[tokio::test]
    async fn test_write_to_closed_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        drop(rx);
        let mut writer = GrpcExchangeWriter::new(tx);
        let res = writer.write(Ok(GetDataResponse::default())).await;
        assert!(res.is_err());
    }
}
