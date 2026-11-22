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

use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::anyhow;
use futures_async_stream::try_stream;
use futures_util::AsyncReadExt;
use opendal::Operator;
use opendal::layers::RetryLayer;
use opendal::services::{Fs, Memory};
use risingwave_common::array::DataChunk;
use risingwave_common::util::batch_spill_config::batch_spill_base_dir;
use risingwave_pb::Message;
use risingwave_pb::data::DataChunk as PbDataChunk;
use thiserror_ext::AsReport;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use twox_hash::XxHash64;

use crate::error::{BatchError, Result};
use crate::monitor::BatchSpillMetrics;

pub const DEFAULT_SPILL_PARTITION_NUM: usize = 20;
const RW_MANAGED_SPILL_DIR: &str = "rw_batch_spill/";
const DEFAULT_IO_BUFFER_SIZE: usize = 256 * 1024;
const DEFAULT_IO_CONCURRENT_TASK: usize = 8;

#[derive(Clone)]
pub enum SpillBackend {
    Disk,
    /// Only for testing purpose
    Memory,
}

/// `SpillOp` is used to manage the spill directory of the spilling executor and it will drop the directory with a RAII style.
/// Overrides the number of spill files that batch queries of this process may keep open.
const SPILL_FILE_LIMIT_ENV: &str = "RW_BATCH_SPILL_FILE_LIMIT";
/// Share of the open file limit of the process that is considered usable at all.
const NOFILE_USABLE_RATIO: f64 = 0.9;
/// File descriptors left to the rest of the server: client connections, object store
/// connections, cache files.
const NOFILE_RESERVED: u64 = 1024;
/// Share of the remaining file descriptors given to spill files.
const SPILL_FILE_RATIO: f64 = 0.5;
/// Used when the open file limit of the process is unknown or unlimited.
const DEFAULT_SPILL_FILE_LIMIT: usize = 32768;
const MIN_SPILL_FILE_LIMIT: usize = 64;
/// How long an operator waits for spill files before its query fails.
const SPILL_FILE_WAIT: Duration = Duration::from_secs(60);

/// The number of spill files allowed for a process whose soft limit of open files is given.
fn spill_file_limit_for(nofile_soft_limit: Option<u64>) -> usize {
    match nofile_soft_limit {
        Some(soft) => {
            let usable =
                ((soft as f64 * NOFILE_USABLE_RATIO) as u64).saturating_sub(NOFILE_RESERVED);
            ((usable as f64 * SPILL_FILE_RATIO) as usize)
                .clamp(MIN_SPILL_FILE_LIMIT, Semaphore::MAX_PERMITS)
        }
        None => DEFAULT_SPILL_FILE_LIMIT,
    }
}

/// The spill files that all batch operators of the process may keep open at the same time.
///
/// Every spilling operator opens tens of files at once. Without a bound, a few wide queries
/// exhaust the open file limit of the process, which then cannot even accept connections.
struct SpillFileBudget {
    limit: usize,
    semaphore: Arc<Semaphore>,
}

impl SpillFileBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            semaphore: Arc::new(Semaphore::new(limit)),
        }
    }

    fn global() -> &'static Self {
        static BUDGET: LazyLock<SpillFileBudget> = LazyLock::new(|| {
            let from_env = std::env::var(SPILL_FILE_LIMIT_ENV)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|limit| *limit > 0);
            let limit = from_env.unwrap_or_else(|| {
                let soft = match rlimit::Resource::NOFILE.get_soft() {
                    Ok(rlimit::INFINITY) | Err(_) => None,
                    Ok(soft) => Some(soft),
                };
                spill_file_limit_for(soft)
            });
            tracing::info!(limit, "batch spill file budget");
            SpillFileBudget::new(limit.min(Semaphore::MAX_PERMITS))
        });
        &BUDGET
    }

    /// Takes `n` files from the budget, all at once so that two operators cannot block each
    /// other with half of their files. Waits for a bounded time, then fails the query.
    async fn acquire(&self, n: usize, wait: Duration) -> Result<OwnedSemaphorePermit> {
        let limit_error = || BatchError::SpillFileLimit {
            needed: n,
            limit: self.limit,
        };
        if n > self.limit {
            return Err(limit_error());
        }
        let acquire = self.semaphore.clone().acquire_many_owned(n as u32);
        match tokio::time::timeout(wait, acquire).await {
            Ok(permit) => Ok(permit.expect("the spill file semaphore is never closed")),
            Err(_) => {
                tracing::warn!(
                    needed = n,
                    limit = self.limit,
                    available = self.semaphore.available_permits(),
                    "no spill file available after {}s, failing the batch task",
                    wait.as_secs()
                );
                Err(limit_error())
            }
        }
    }
}

/// A spill file open for writing. It counts in the spill file budget until it is closed.
pub struct SpillWriter {
    writer: opendal::Writer,
    permit: Option<OwnedSemaphorePermit>,
}

impl SpillWriter {
    pub async fn write(&mut self, bs: impl Into<opendal::Buffer>) -> opendal::Result<()> {
        self.writer.write(bs).await
    }

    pub async fn close(&mut self) -> opendal::Result<opendal::Metadata> {
        let result = self.writer.close().await;
        self.permit = None;
        result
    }
}

/// A spill file open for reading. It counts in the spill file budget until it is dropped.
pub struct SpillReader {
    reader: opendal::Reader,
    permit: Option<OwnedSemaphorePermit>,
}

pub struct SpillOp {
    pub op: Operator,
    /// `None` for the backends that do not use files.
    file_budget: Option<&'static SpillFileBudget>,
    /// Files taken from the budget by [`SpillOp::reserve_files`] and not opened yet.
    reserved_files: std::sync::Mutex<Option<OwnedSemaphorePermit>>,
}

impl SpillOp {
    fn batch_spill_root() -> PathBuf {
        batch_spill_base_dir().join(RW_MANAGED_SPILL_DIR)
    }

    pub fn create(path: impl AsRef<Path>, spill_backend: SpillBackend) -> Result<SpillOp> {
        let path = path.as_ref();
        if !path.is_relative() {
            bail!("Spill path must be relative, but got {:?}", path);
        }

        let root = Self::batch_spill_root().join(path);

        let op = match spill_backend {
            SpillBackend::Disk => {
                let builder = Fs::default().root(&root.to_string_lossy());
                Operator::new(builder)?
                    .layer(RetryLayer::default())
                    .finish()
            }
            SpillBackend::Memory => {
                let builder = Memory::default().root(&root.to_string_lossy());
                Operator::new(builder)?
                    .layer(RetryLayer::default())
                    .finish()
            }
        };
        let file_budget = match spill_backend {
            SpillBackend::Disk => Some(SpillFileBudget::global()),
            SpillBackend::Memory => None,
        };
        Ok(SpillOp {
            op,
            file_budget,
            reserved_files: std::sync::Mutex::new(None),
        })
    }

    /// Takes from the spill file budget the `n` files that the operator is about to open with
    /// [`SpillOp::writer_with`], all at once. Waits when the budget is exhausted, and fails
    /// when it stays so.
    pub async fn reserve_files(&self, n: usize) -> Result<()> {
        let Some(budget) = self.file_budget else {
            return Ok(());
        };
        let permit = budget.acquire(n, SPILL_FILE_WAIT).await?;
        let mut reserved = self.reserved_files.lock().unwrap();
        match reserved.as_mut() {
            Some(existing) => existing.merge(permit),
            None => *reserved = Some(permit),
        }
        Ok(())
    }

    /// One file of the budget: from the reserved ones if any, from the budget otherwise.
    async fn file_permit(&self) -> Result<Option<OwnedSemaphorePermit>> {
        let Some(budget) = self.file_budget else {
            return Ok(None);
        };
        {
            let mut reserved = self.reserved_files.lock().unwrap();
            if let Some(permits) = reserved.as_mut() {
                if permits.num_permits() <= 1 {
                    return Ok(reserved.take());
                }
                return Ok(permits.split(1));
            }
        }
        Ok(Some(budget.acquire(1, SPILL_FILE_WAIT).await?))
    }

    pub async fn clean_spill_directory() -> opendal::Result<()> {
        static LOCK: LazyLock<Mutex<usize>> = LazyLock::new(|| Mutex::new(0));
        let _guard = LOCK.lock().await;

        let root = Self::batch_spill_root();

        let builder = Fs::default().root(&root.to_string_lossy());

        let op: Operator = Operator::new(builder)?
            .layer(RetryLayer::default())
            .finish();

        op.remove_all("/").await
    }

    pub async fn writer_with(&self, name: &str) -> Result<SpillWriter> {
        let permit = self.file_permit().await?;
        let writer = self
            .op
            .writer_with(name)
            .concurrent(DEFAULT_IO_CONCURRENT_TASK)
            .chunk(DEFAULT_IO_BUFFER_SIZE)
            .await?;
        Ok(SpillWriter { writer, permit })
    }

    pub async fn reader_with(&self, name: &str) -> Result<SpillReader> {
        let permit = self.file_permit().await?;
        let reader = self
            .op
            .reader_with(name)
            .chunk(DEFAULT_IO_BUFFER_SIZE)
            .await?;
        Ok(SpillReader { reader, permit })
    }

    /// spill file content will look like the below.
    ///
    /// ```text
    /// [proto_len]
    /// [proto_bytes]
    /// ...
    /// [proto_len]
    /// [proto_bytes]
    /// ```
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn read_stream(reader: SpillReader, spill_metrics: Arc<BatchSpillMetrics>) {
        // The file counts in the budget for as long as the stream lives.
        let SpillReader {
            reader,
            permit: _permit,
        } = reader;
        let mut reader = reader.into_futures_async_read(..).await?;
        let mut buf = [0u8; 4];
        loop {
            if let Err(err) = reader.read_exact(&mut buf).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(anyhow!(err).into());
                }
            }
            let len = u32::from_le_bytes(buf) as usize;
            spill_metrics.batch_spill_read_bytes.inc_by(len as u64 + 4);
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).await.map_err(|e| anyhow!(e))?;
            let chunk_pb: PbDataChunk = Message::decode(buf.as_slice()).map_err(|e| anyhow!(e))?;
            let chunk = DataChunk::from_protobuf(&chunk_pb)?;
            yield chunk;
        }
    }
}

impl Drop for SpillOp {
    fn drop(&mut self) {
        let op = self.op.clone();
        tokio::task::spawn(async move {
            let result = op.remove_all("/").await;
            if let Err(error) = result {
                error!(
                    error = %error.as_report(),
                    "Failed to remove spill directory"
                );
            }
        });
    }
}

impl DerefMut for SpillOp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}

impl Deref for SpillOp {
    type Target = Operator;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

#[derive(Default, Clone, Copy)]
pub struct SpillBuildHasher(pub u64);

impl BuildHasher for SpillBuildHasher {
    type Hasher = XxHash64;

    fn build_hasher(&self) -> Self::Hasher {
        XxHash64::with_seed(self.0)
    }
}

pub const SPILL_AT_LEAST_MEMORY: u64 = 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spill_file_limit_for() {
        // Typical container limits.
        assert_eq!(spill_file_limit_for(Some(1048576)), 471347);
        assert_eq!(spill_file_limit_for(Some(65536)), 28979);
        // Too low to leave anything after the reserve: the minimum applies.
        assert_eq!(spill_file_limit_for(Some(1024)), MIN_SPILL_FILE_LIMIT);
        assert_eq!(spill_file_limit_for(None), DEFAULT_SPILL_FILE_LIMIT);
    }

    fn spill_op_with_budget(limit: usize) -> (SpillOp, &'static SpillFileBudget) {
        let budget: &'static SpillFileBudget = Box::leak(Box::new(SpillFileBudget::new(limit)));
        let mut op = SpillOp::create("test_spill_file_budget/", SpillBackend::Memory).unwrap();
        op.file_budget = Some(budget);
        (op, budget)
    }

    #[tokio::test]
    async fn test_spill_file_budget_is_released_on_close() {
        let (op, budget) = spill_op_with_budget(4);
        op.reserve_files(4).await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 0);

        let mut writers = vec![];
        for i in 0..4 {
            writers.push(op.writer_with(&format!("p{i}")).await.unwrap());
        }
        // The writers use the reserved files, not new ones.
        assert_eq!(budget.semaphore.available_permits(), 0);
        assert!(op.reserved_files.lock().unwrap().is_none());

        for (i, writer) in writers.iter_mut().enumerate() {
            writer.write(vec![0u8; 8]).await.unwrap();
            writer.close().await.unwrap();
            assert_eq!(budget.semaphore.available_permits(), i + 1);
        }

        // A reader counts for as long as it lives.
        let reader = op.reader_with("p0").await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 3);
        drop(reader);
        assert_eq!(budget.semaphore.available_permits(), 4);
    }

    #[tokio::test]
    async fn test_spill_file_budget_exhausted() {
        let (op, budget) = spill_op_with_budget(4);

        // More than the whole budget can never be served: fail at once.
        let err = op.reserve_files(5).await.unwrap_err();
        assert!(matches!(
            err,
            BatchError::SpillFileLimit {
                needed: 5,
                limit: 4
            }
        ));

        // The budget is taken by another operator: wait, then fail.
        let held = budget.acquire(3, Duration::ZERO).await.unwrap();
        let err = budget
            .acquire(2, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(matches!(err, BatchError::SpillFileLimit { needed: 2, .. }));

        // It is served as soon as the other operator closes its files.
        let waiter = tokio::spawn(async move { budget.acquire(2, Duration::from_secs(10)).await });
        tokio::task::yield_now().await;
        drop(held);
        let permit = waiter.await.unwrap().unwrap();
        assert_eq!(permit.num_permits(), 2);
        assert_eq!(budget.semaphore.available_permits(), 2);
    }
}
