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

//! The spool of `batch_share_identical_subplans`: a sub-plan computed once per task and read by
//! several nodes of the plan fragment.
//!
//! Every node of a spool, the producer (built with the sub-plan as its input) as well as the
//! readers (built without input), is a [`SpoolExecutor`] holding the same [`Spool`]. The first of
//! them to be read starts the materialization: the sub-plan runs to its end and its rows are kept,
//! in memory while the memory of the operator allows it, all on disk otherwise. Then every node
//! returns all the rows, in the order of the sub-plan.
//!
//! Deadlocks. The materialization is a shared future ([`Shared`]): any node that waits for it
//! drives it, and all of them are woken when it progresses. It never waits for a node of the
//! spool: its sub-plan contains none of them (a reader is a leaf, and the producer's sub-plan
//! is a strict sub-tree of the plan, which cannot contain a copy of itself), and the sub-plans of
//! nested spools are strictly smaller, so no spool waits for itself. No lock is held while it
//! runs. A parent that stops reading one node of the spool (a join reading its build side first)
//! does not block the others: the next one to be read drives the materialization.
//!
//! Towards other tasks, the materialization behaves like any blocking operator (a hash
//! aggregation or a sort) placed where the first node of the spool is read: it reads its inputs
//! to their end before returning a row. The readers read nothing from other tasks.
//!
//! Memory. The rows kept in memory are counted in the memory context of a spillable operator of
//! the query. When the context refuses more, the rows already kept are written to a spill file,
//! released from the context, and all the next rows go to the file: the spool then holds no
//! memory. Without spilling, the query fails as a hash aggregation would. The rows, in memory or on
//! disk, are released when the last node of the spool is dropped.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures_async_stream::try_stream;
use futures_util::future::{BoxFuture, FutureExt, Shared};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::memory::MemoryContext;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::Message;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result, SharedResult};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::monitor::BatchSpillMetrics;
use crate::spill::spill_op::{SpillBackend, SpillOp, SpillWriter};

/// The file of the spill directory of a spool that holds its rows.
const SPOOL_FILE: &str = "rows";

type Materialization = Shared<BoxFuture<'static, SharedResult<Arc<SpoolData>>>>;

/// The state of a spool in a task, shared by its nodes.
#[derive(Default)]
pub struct Spool {
    /// The sub-plan, set when the producer is built, taken when the materialization starts.
    source: Mutex<Option<SpoolSource>>,
    /// The materialization, created by the first node that is read.
    materialization: Mutex<Option<Materialization>>,
}

impl Spool {
    fn set_source(&self, source: SpoolSource) -> Result<()> {
        let mut slot = self.source.lock().unwrap();
        if slot.is_some() {
            bail!("a spool has two producers");
        }
        *slot = Some(source);
        Ok(())
    }

    /// The materialization of the sub-plan, started if it is not yet.
    fn materialization(&self) -> Materialization {
        self.materialization
            .lock()
            .unwrap()
            .get_or_insert_with(|| {
                let source = self.source.lock().unwrap().take();
                async move {
                    match source {
                        Some(source) => source.materialize().await.map(Arc::new).map_err(Arc::new),
                        None => Err(Arc::new(BatchError::Internal(anyhow::anyhow!(
                            "the sub-plan of a spool is missing"
                        )))),
                    }
                }
                .boxed()
                .shared()
            })
            .clone()
    }
}

/// The sub-plan of a spool, and what its materialization needs.
struct SpoolSource {
    input: BoxedExecutor,
    mem_context: MemoryContext,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
    /// The spill directory, relative to the spill root, unique to the spool.
    spill_dir: String,
}

impl SpoolSource {
    async fn materialize(self) -> Result<SpoolData> {
        let Self {
            input,
            mem_context,
            spill_backend,
            spill_metrics,
            spill_dir,
        } = self;
        let mut chunks = Vec::new();
        // The memory of `chunks` counted in `mem_context`.
        let mut memory = 0i64;
        let mut spill: Option<(SpillOp, SpillWriter)> = None;

        let mut stream = input.execute();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?.compact_vis();
            if chunk.cardinality() == 0 {
                continue;
            }
            if let Some((_, writer)) = &mut spill {
                write_chunk(writer, &chunk, &spill_metrics).await?;
                continue;
            }
            let size = chunk.estimated_heap_size() as i64;
            if mem_context.add(size) {
                memory += size;
                chunks.push(chunk);
                continue;
            }
            // The memory of the operator is exhausted: move all the rows to disk, in order.
            let Some(spill_backend) = &spill_backend else {
                return Err(BatchError::OutOfMemory(mem_context.mem_limit()));
            };
            let op = SpillOp::create(&spill_dir, spill_backend.clone())?;
            let mut writer = op.writer_with(SPOOL_FILE).await?;
            for kept in chunks.drain(..) {
                write_chunk(&mut writer, &kept, &spill_metrics).await?;
            }
            write_chunk(&mut writer, &chunk, &spill_metrics).await?;
            mem_context.add(-memory);
            memory = 0;
            spill = Some((op, writer));
        }

        let spilled = match spill {
            Some((op, mut writer)) => {
                writer.close().await?;
                Some(op)
            }
            None => None,
        };
        Ok(SpoolData {
            chunks,
            spilled,
            spill_metrics,
            _mem_context: mem_context,
        })
    }
}

/// Appends `chunk` to a spill file, in the format of [`SpillOp::read_stream`].
async fn write_chunk(
    writer: &mut SpillWriter,
    chunk: &DataChunk,
    spill_metrics: &BatchSpillMetrics,
) -> Result<()> {
    let buf = Message::encode_to_vec(&chunk.to_protobuf());
    let len = (buf.len() as u32).to_le_bytes();
    spill_metrics
        .batch_spill_write_bytes
        .inc_by((buf.len() + len.len()) as u64);
    writer.write(len.to_vec()).await?;
    writer.write(buf).await?;
    Ok(())
}

/// The rows of a materialized spool.
struct SpoolData {
    /// The rows kept in memory, when none was spilled.
    chunks: Vec<DataChunk>,
    /// The spill directory whose file holds all the rows, when they did not fit in memory.
    spilled: Option<SpillOp>,
    spill_metrics: Arc<BatchSpillMetrics>,
    /// Counts `chunks` in the memory of the query until they are dropped.
    _mem_context: MemoryContext,
}

/// A node of a spool: the producer or a reader.
pub struct SpoolExecutor {
    spool: Arc<Spool>,
    schema: Schema,
    identity: String,
}

impl Executor for SpoolExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        Self::do_execute(self.spool)
    }
}

impl SpoolExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(spool: Arc<Spool>) {
        let materialization = spool.materialization();
        let data = materialization.await?;
        for chunk in &data.chunks {
            yield chunk.clone();
        }
        if let Some(op) = &data.spilled {
            let reader = op.reader_with(SPOOL_FILE).await?;
            #[for_await]
            for chunk in SpillOp::read_stream(reader, data.spill_metrics.clone()) {
                yield chunk?;
            }
        }
    }
}

impl BoxedExecutorBuilder for SpoolExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::Spool)?;
        let identity = source.plan_node().get_identity().clone();
        let schema = Schema::new(node.schema.iter().map(Field::from).collect());
        let spool = source
            .spools()
            .get_or_create(node.spool_id, Spool::default)?;
        let input = inputs.pop();
        if !inputs.is_empty() {
            bail!("a spool node has at most one input");
        }
        if let Some(input) = input {
            let context = source.context();
            spool.set_source(SpoolSource {
                input,
                mem_context: context.create_spillable_executor_mem_context(&identity),
                spill_backend: context
                    .get_config()
                    .enable_spill
                    .then_some(SpillBackend::Disk),
                spill_metrics: context.spill_metrics(),
                spill_dir: format!("spool-{}-{}/", node.spool_id, uuid::Uuid::new_v4()),
            })?;
        }
        Ok(Box::new(Self {
            spool,
            schema,
            identity,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use risingwave_common::catalog::schema_test_utils::ii;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::test_prelude::DataChunkTestExt;

    use super::*;

    /// An input that counts its executions, yields to the runtime before each chunk, and fails
    /// at the end when `fail` is set.
    struct TestInput {
        chunks: Vec<DataChunk>,
        executions: Arc<AtomicUsize>,
        fail: bool,
        schema: Schema,
    }

    impl Executor for TestInput {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn identity(&self) -> &str {
            "TestInput"
        }

        fn execute(self: Box<Self>) -> BoxedDataChunkStream {
            self.do_execute()
        }
    }

    impl TestInput {
        #[try_stream(boxed, ok = DataChunk, error = BatchError)]
        async fn do_execute(self: Box<Self>) {
            self.executions.fetch_add(1, Ordering::SeqCst);
            for chunk in self.chunks {
                tokio::task::yield_now().await;
                yield chunk;
            }
            if self.fail {
                Err(BatchError::Internal(anyhow::anyhow!("input failed")))?;
            }
        }
    }

    fn chunks() -> Vec<DataChunk> {
        vec![
            DataChunk::from_pretty(
                "i i
                 1 2
                 . 4
                 1 2",
            ),
            DataChunk::from_pretty(
                "i i
                 5 .
                 7 8 D",
            ),
            DataChunk::from_pretty(
                "i i
                 9 10",
            ),
        ]
    }

    fn expected_rows() -> Vec<OwnedRow> {
        rows_of(&chunks())
    }

    fn rows_of(chunks: &[DataChunk]) -> Vec<OwnedRow> {
        chunks
            .iter()
            .flat_map(|chunk| chunk.rows().map(|row| row.to_owned_row()))
            .collect()
    }

    fn test_input(chunks: Vec<DataChunk>, fail: bool) -> (BoxedExecutor, Arc<AtomicUsize>) {
        let executions = Arc::new(AtomicUsize::new(0));
        let input = TestInput {
            chunks,
            executions: executions.clone(),
            fail,
            schema: ii(),
        };
        (Box::new(input), executions)
    }

    fn spool_of(
        input: BoxedExecutor,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
    ) -> Arc<Spool> {
        let spool = Arc::new(Spool::default());
        spool
            .set_source(SpoolSource {
                input,
                mem_context,
                spill_backend,
                spill_metrics: BatchSpillMetrics::for_test(),
                spill_dir: format!("spool-test-{}/", uuid::Uuid::new_v4()),
            })
            .unwrap();
        spool
    }

    fn node(spool: &Arc<Spool>) -> BoxedExecutor {
        Box::new(SpoolExecutor {
            spool: spool.clone(),
            schema: ii(),
            identity: "SpoolExecutor".to_owned(),
        })
    }

    async fn collect(node: BoxedExecutor) -> Result<Vec<OwnedRow>> {
        let mut rows = vec![];
        let mut stream = node.execute();
        while let Some(chunk) = stream.next().await {
            rows.extend(rows_of(&[chunk?]));
        }
        Ok(rows)
    }

    /// A memory context bounded to `limit` bytes, in an unbounded parent standing for the query.
    fn bounded_context(limit: u64) -> (MemoryContext, MemoryContext) {
        let query = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let operator = MemoryContext::new_with_mem_limit(
            Some(query.clone()),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
            limit,
        );
        (query, operator)
    }

    #[tokio::test]
    async fn test_every_node_reads_all_rows_computed_once() {
        let (input, executions) = test_input(chunks(), false);
        let (query, operator) = bounded_context(u64::MAX);
        let spool = spool_of(input, operator, None);
        let nodes = [node(&spool), node(&spool), node(&spool)];
        drop(spool);
        for node in nodes {
            assert_eq!(collect(node).await.unwrap(), expected_rows());
        }
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        // The rows are released with the last node.
        assert_eq!(query.get_bytes_used(), 0);
    }

    #[tokio::test]
    async fn test_rows_stay_counted_while_a_node_remains() {
        let (input, _) = test_input(chunks(), false);
        let (query, operator) = bounded_context(u64::MAX);
        let spool = spool_of(input, operator, None);
        let first = node(&spool);
        let second = node(&spool);
        drop(spool);
        assert_eq!(collect(first).await.unwrap(), expected_rows());
        assert!(query.get_bytes_used() > 0);
        assert_eq!(collect(second).await.unwrap(), expected_rows());
        assert_eq!(query.get_bytes_used(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_nodes() {
        let (input, executions) = test_input(chunks(), false);
        let spool = spool_of(input, MemoryContext::none(), None);
        let (a, b) = tokio::join!(collect(node(&spool)), collect(node(&spool)));
        assert_eq!(a.unwrap(), expected_rows());
        assert_eq!(b.unwrap(), expected_rows());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    /// A node that started the materialization and is then left aside by its parent does not
    /// block another node: the materialization goes on when the other node is read.
    #[tokio::test]
    async fn test_node_left_aside_does_not_block_others() {
        let (input, executions) = test_input(chunks(), false);
        let spool = spool_of(input, MemoryContext::none(), None);
        let mut first = node(&spool).execute();
        // Polled once: the input yields to the runtime, so the materialization is pending.
        assert!(first.next().now_or_never().is_none());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(collect(node(&spool)).await.unwrap(), expected_rows());
        let mut rows = vec![];
        while let Some(chunk) = first.next().await {
            rows.extend(rows_of(&[chunk.unwrap()]));
        }
        assert_eq!(rows, expected_rows());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    /// A node dropped in the middle of the materialization leaves it to the others.
    #[tokio::test]
    async fn test_node_dropped_during_materialization() {
        let (input, executions) = test_input(chunks(), false);
        let spool = spool_of(input, MemoryContext::none(), None);
        let mut first = node(&spool).execute();
        assert!(first.next().now_or_never().is_none());
        drop(first);
        assert_eq!(collect(node(&spool)).await.unwrap(), expected_rows());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_spill_when_memory_is_short() {
        // Room for the first chunk only: the second one goes over the limit.
        let one_chunk = chunks()[0].estimated_heap_size() as u64;
        let (query, operator) = bounded_context(one_chunk);
        let (input, executions) = test_input(chunks(), false);
        let spool = spool_of(input, operator, Some(SpillBackend::Memory));
        let first = node(&spool);
        let second = node(&spool);
        let third = node(&spool);
        assert_eq!(collect(first).await.unwrap(), expected_rows());
        // All the rows went to disk: the spool holds no memory any more.
        assert_eq!(query.get_bytes_used(), 0);
        assert!(
            spool
                .materialization()
                .now_or_never()
                .unwrap()
                .unwrap()
                .spilled
                .is_some()
        );
        drop(spool);
        assert_eq!(collect(second).await.unwrap(), expected_rows());
        assert_eq!(collect(third).await.unwrap(), expected_rows());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(query.get_bytes_used(), 0);
    }

    #[tokio::test]
    async fn test_spill_from_the_first_chunk() {
        let (input, _) = test_input(chunks(), false);
        let (query, operator) = bounded_context(0);
        let spool = spool_of(input, operator, Some(SpillBackend::Memory));
        let (a, b) = tokio::join!(collect(node(&spool)), collect(node(&spool)));
        assert_eq!(a.unwrap(), expected_rows());
        assert_eq!(b.unwrap(), expected_rows());
        assert_eq!(query.get_bytes_used(), 0);
    }

    #[tokio::test]
    async fn test_out_of_memory_without_spill() {
        let (input, _) = test_input(chunks(), false);
        let (query, operator) = bounded_context(0);
        let spool = spool_of(input, operator, None);
        let first = node(&spool);
        let second = node(&spool);
        drop(spool);
        assert!(collect(first).await.is_err());
        assert!(collect(second).await.is_err());
        assert_eq!(query.get_bytes_used(), 0);
    }

    #[tokio::test]
    async fn test_input_error_reaches_every_node() {
        let (input, executions) = test_input(chunks(), true);
        let spool = spool_of(input, MemoryContext::none(), None);
        let first = node(&spool);
        let second = node(&spool);
        assert!(collect(first).await.is_err());
        assert!(collect(second).await.is_err());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_empty_input() {
        let (input, executions) = test_input(vec![], false);
        let spool = spool_of(input, MemoryContext::none(), None);
        assert!(collect(node(&spool)).await.unwrap().is_empty());
        assert!(collect(node(&spool)).await.unwrap().is_empty());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_missing_producer_fails_instead_of_waiting() {
        let spool = Arc::new(Spool::default());
        assert!(collect(node(&spool)).await.is_err());
    }

    #[tokio::test]
    async fn test_two_producers_are_rejected() {
        let (input, _) = test_input(chunks(), false);
        let spool = spool_of(input, MemoryContext::none(), None);
        let (input, _) = test_input(chunks(), false);
        let second = SpoolSource {
            input,
            mem_context: MemoryContext::none(),
            spill_backend: None,
            spill_metrics: BatchSpillMetrics::for_test(),
            spill_dir: "spool-test-second/".to_owned(),
        };
        assert!(spool.set_source(second).is_err());
    }
}
