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

use std::marker::PhantomData;
use std::mem::swap;
use std::sync::Arc;

use bytes::Bytes;
use futures_async_stream::try_stream;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use itertools::Itertools;
use prometheus::core::Atomic;
use risingwave_batch::task::ExecutorMemory;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder, FilterByBitmap};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::{MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::encode_chunk;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::Message;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::data::DataChunk as PbDataChunk;

use super::top_n::{HeapElem, TopNHeap};
use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::monitor::BatchSpillMetrics;
use crate::spill::spill_op::{
    DEFAULT_SPILL_PARTITION_NUM, SPILL_AT_LEAST_MEMORY, SpillBackend, SpillBuildHasher, SpillOp,
    SpillWriter,
};

/// How many times the rows of a top-n per group may be partitioned again when a partition does
/// not fit in memory either. Beyond, the rows are kept in memory, counted beyond the limit: the
/// rows of one group always go to the same partition, so a group whose rows do not fit alone
/// cannot be split.
const MAX_SPILL_DEPTH: usize = 3;

/// Group Top-N Executor
///
/// For each group, use a N-heap to store the smallest N rows.
///
/// **Unordered.** Without order, `WITH TIES` and offset, the rows of a group are all equal for
/// the heap, which keeps the first `limit` rows of each group in input order (a row never replaces
/// an equal one). The executor then emits these rows as they arrive, chunk by chunk, and only
/// keeps the group keys seen, with a count of the rows emitted per key: it does not block, its
/// memory is that of the keys, and a limit above it can stop it early. It returns the same rows as
/// the heaps, in another order (a top-n per group has no output order).
///
/// **Spill** (`batch_spill_group_topn`). With an order, the heaps hold up to `limit + offset`
/// rows per group until the input ends. When the memory context of the operator, a spillable
/// operator of its query, refuses more, the rows of the heaps and the rest of the input are
/// written to partitions by a hash of the group key, and each partition is then processed by a
/// new executor, one after another. A top-n per group of the rows of the heaps and the rest of the
/// input is the top-n per group of the whole input: the heaps only dropped rows that rank after
/// `limit + offset` rows of their group (or after the ties of the last one).
pub struct GroupTopNExecutor<K: HashKey> {
    child: BoxedExecutor,
    column_orders: Vec<ColumnOrder>,
    offset: usize,
    limit: usize,
    group_key: Vec<usize>,
    with_ties: bool,
    schema: Schema,
    identity: String,
    chunk_size: usize,
    mem_ctx: MemoryContext,
    memory: ExecutorMemory,
    spill: Option<GroupTopNSpill>,
    _phantom: PhantomData<K>,
}

/// How a top-n per group with an order spills (`batch_spill_group_topn`).
#[derive(Clone)]
pub struct GroupTopNSpill {
    /// The memory context of the operator as a spillable operator of its query. What the heaps
    /// hold is charged to it after each input chunk: when it refuses more, the heaps are spilled.
    pub mem_ctx: MemoryContext,
    pub backend: SpillBackend,
    pub metrics: Arc<BatchSpillMetrics>,
    /// The heaps are only spilled when they hold at least this many bytes.
    pub min_bytes: i64,
    /// How many times the rows were already partitioned.
    pub depth: usize,
}

impl GroupTopNSpill {
    fn can_spill(&self) -> bool {
        self.depth < MAX_SPILL_DEPTH
    }
}

pub struct GroupTopNExecutorBuilder {
    child: BoxedExecutor,
    column_orders: Vec<ColumnOrder>,
    offset: usize,
    limit: usize,
    group_key: Vec<usize>,
    group_key_types: Vec<DataType>,
    with_ties: bool,
    identity: String,
    chunk_size: usize,
    memory: ExecutorMemory,
    spill: Option<GroupTopNSpill>,
}

impl HashKeyDispatcher for GroupTopNExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        let mut executor = GroupTopNExecutor::<K>::new(
            self.child,
            self.column_orders,
            self.offset,
            self.limit,
            self.with_ties,
            self.group_key,
            self.identity,
            self.chunk_size,
            self.memory,
        );
        executor.spill = self.spill;
        Box::new(executor)
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

impl BoxedExecutorBuilder for GroupTopNExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let top_n_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GroupTopN
        )?;

        let column_orders: Vec<_> = top_n_node
            .column_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let group_key = top_n_node
            .group_key
            .iter()
            .map(|x| *x as usize)
            .collect_vec();
        let child_schema = child.schema();
        let group_key_types = group_key
            .iter()
            .map(|x| child_schema.fields[*x].data_type())
            .collect();

        let identity = source.plan_node().get_identity().clone();
        let offset = top_n_node.get_offset() as usize;
        let with_ties = top_n_node.get_with_ties();
        let context = source.context();

        // `batch_spill_group_topn`: the heaps are counted in a context of their own, charged to
        // the context of a spillable operator. An unordered top-n per group keeps no rows.
        let spill = (top_n_node.spill
            && !is_unordered(&column_orders, offset, with_ties)
            && context.get_config().enable_spill)
            .then(|| GroupTopNSpill {
                mem_ctx: context.create_spillable_executor_mem_context(&identity),
                backend: SpillBackend::Disk,
                metrics: context.spill_metrics(),
                min_bytes: SPILL_AT_LEAST_MEMORY as i64,
                depth: 0,
            });
        let memory = match spill {
            Some(_) => ExecutorMemory::unmanaged(heap_meter()),
            None => context.create_executor_memory(&identity),
        };

        let builder = Self {
            child,
            column_orders,
            offset,
            limit: top_n_node.get_limit() as usize,
            group_key,
            group_key_types,
            with_ties,
            identity,
            chunk_size: context.get_config().developer.chunk_size,
            memory,
            spill,
        };

        Ok(builder.dispatch())
    }
}

/// A context that counts the memory of the heaps of a spilling top-n per group, without limit:
/// what it counts is charged to the context of the operator.
fn heap_meter() -> MemoryContext {
    MemoryContext::root(TrAdderAtomic::new(0), u64::MAX)
}

/// Whether any `limit` rows of a group are a valid result: no order, no `WITH TIES`, no offset.
fn is_unordered(column_orders: &[ColumnOrder], offset: usize, with_ties: bool) -> bool {
    column_orders.is_empty() && offset == 0 && !with_ties
}

impl<K: HashKey> GroupTopNExecutor<K> {
    pub fn new(
        child: BoxedExecutor,
        column_orders: Vec<ColumnOrder>,
        offset: usize,
        limit: usize,
        with_ties: bool,
        group_key: Vec<usize>,
        identity: String,
        chunk_size: usize,
        memory: impl Into<ExecutorMemory>,
    ) -> Self {
        let schema = child.schema().clone();
        let memory = memory.into();
        let mem_ctx = memory.meter();
        Self {
            child,
            column_orders,
            offset,
            limit,
            with_ties,
            group_key,
            schema,
            identity,
            chunk_size,
            mem_ctx,
            memory,
            spill: None,
            _phantom: PhantomData,
        }
    }

    fn is_unordered(&self) -> bool {
        is_unordered(&self.column_orders, self.offset, self.with_ties)
    }
}

impl<K: HashKey> Executor for GroupTopNExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        if self.is_unordered() {
            self.do_execute_unordered()
        } else {
            self.do_execute()
        }
    }
}

/// Bytes counted by hand in a memory context, given back when dropped.
struct CountedBytes {
    ctx: MemoryContext,
    bytes: i64,
}

impl CountedBytes {
    fn new(ctx: MemoryContext) -> Self {
        Self { ctx, bytes: 0 }
    }

    fn add(&mut self, bytes: i64) {
        self.ctx.force_add(bytes);
        self.bytes += bytes;
    }
}

impl Drop for CountedBytes {
    fn drop(&mut self) {
        self.ctx.force_add(-self.bytes);
    }
}

/// What the heaps of a spilling top-n per group hold, charged to the context of the operator.
/// Given back when dropped.
struct ChargedMemory {
    ctx: MemoryContext,
    charged: i64,
}

impl ChargedMemory {
    fn new(ctx: MemoryContext) -> Self {
        Self { ctx, charged: 0 }
    }

    /// Charges `used` bytes in all. Returns `false` when the context refuses more and the heaps
    /// must be spilled: only when `spillable` and they hold at least `min_bytes`. Otherwise the
    /// memory is counted whatever the limits, since it is already used.
    fn charge(&mut self, used: i64, spillable: bool, min_bytes: i64) -> bool {
        let diff = used - self.charged;
        if diff <= 0 || self.ctx.add(diff) {
            if diff < 0 {
                self.ctx.add(diff);
            }
            self.charged = used;
            return true;
        }
        if spillable && used >= min_bytes {
            return false;
        }
        self.ctx.force_add(diff);
        self.charged = used;
        true
    }

    fn release(&mut self) {
        self.ctx.add(-self.charged);
        self.charged = 0;
    }
}

impl Drop for ChargedMemory {
    fn drop(&mut self) {
        self.release();
    }
}

impl<K: HashKey> GroupTopNExecutor<K> {
    /// Without order, `WITH TIES` and offset: emits the first `limit` rows of each group as they
    /// arrive. See [`GroupTopNExecutor`].
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute_unordered(self: Box<Self>) {
        if self.limit == 0 {
            return Ok(());
        }
        // The rows emitted per group key: the count is only needed when `limit` > 1.
        let mut emitted =
            HashMap::<K, usize, PrecomputedBuildHasher, MonitoredGlobalAlloc>::with_hasher_in(
                PrecomputedBuildHasher,
                self.mem_ctx.global_allocator(),
            );
        // The memory that the keys hold outside the table (variable-size keys).
        let mut key_bytes = CountedBytes::new(self.mem_ctx.clone());
        let mut memory = self.memory;

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = chunk?;
            let keys = K::build_many(self.group_key.as_slice(), &chunk);
            let mut kept = BitmapBuilder::zeroed(chunk.capacity());
            let mut kept_rows = 0;
            for (row_id, key) in keys
                .into_iter()
                .enumerate()
                .filter_by_bitmap(chunk.visibility())
            {
                let keep = match emitted.entry(key) {
                    Entry::Occupied(mut entry) => {
                        let count = entry.get_mut();
                        let keep = *count < self.limit;
                        if keep {
                            *count += 1;
                        }
                        keep
                    }
                    Entry::Vacant(entry) => {
                        key_bytes.add(entry.key().estimated_heap_size() as i64);
                        entry.insert(1);
                        true
                    }
                };
                if keep {
                    kept.set(row_id, true);
                    kept_rows += 1;
                }
            }
            memory.settle().await?;
            if kept_rows == 0 {
                continue;
            }
            let output = chunk.with_visibility(kept.finish());
            // Few rows kept: copy them, rather than keeping the whole chunk alive downstream.
            if kept_rows * 2 < output.capacity() {
                yield output.compact_vis();
            } else {
                yield output;
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let this = *self;
        if this.limit == 0 {
            return Ok(());
        }
        let child_schema = this.child.schema().clone();
        let mut groups =
            HashMap::<K, TopNHeap, PrecomputedBuildHasher, MonitoredGlobalAlloc>::with_hasher_in(
                PrecomputedBuildHasher,
                this.mem_ctx.global_allocator(),
            );
        let mut memory = this.memory;
        let mut charged = this
            .spill
            .as_ref()
            .map(|spill| ChargedMemory::new(spill.mem_ctx.clone()));
        let mut need_to_spill = false;

        let mut input = this.child.execute();
        #[for_await]
        for chunk in &mut input {
            let chunk = Arc::new(chunk?);
            let keys = K::build_many(this.group_key.as_slice(), &chunk);

            for (row_id, (encoded_row, key)) in encode_chunk(&chunk, &this.column_orders)?
                .into_iter()
                .zip_eq_fast(keys.into_iter())
                .enumerate()
                .filter_by_bitmap(chunk.visibility())
            {
                // Most groups may hold fewer rows than the limit: the heaps grow with their rows
                // instead of reserving the limit up front.
                let heap = groups.entry(key).or_insert_with(|| {
                    TopNHeap::new_lazy(
                        this.limit,
                        this.offset,
                        this.with_ties,
                        this.mem_ctx.clone(),
                    )
                });
                heap.push(HeapElem::new(encoded_row, chunk.row_at(row_id).0));
            }
            match (&mut charged, &this.spill) {
                (Some(charged), Some(spill)) => {
                    if !charged.charge(
                        this.mem_ctx.get_bytes_used(),
                        spill.can_spill(),
                        spill.min_bytes,
                    ) {
                        need_to_spill = true;
                        break;
                    }
                }
                _ => memory.settle().await?,
            }
        }

        if need_to_spill {
            let spill = this.spill.clone().unwrap();
            tracing::info!(
                identity = %this.identity,
                depth = spill.depth,
                bytes = this.mem_ctx.get_bytes_used(),
                "batch group top-n spills to disk"
            );
            let mut manager = GroupTopNSpillManager::new(
                &spill,
                &this.identity,
                DEFAULT_SPILL_PARTITION_NUM,
                this.group_key.clone(),
                child_schema.data_types(),
                this.chunk_size,
            )?;
            manager.init_writers().await?;

            // The rows of the heaps, those of the offset included, then the rest of the input.
            let mut builder = DataChunkBuilder::new(child_schema.data_types(), this.chunk_size);
            for (_, h) in &mut groups {
                let mut heap = TopNHeap::empty();
                swap(&mut heap, h);
                for elem in heap.into_elems() {
                    if let Some(chunk) = builder.append_one_row(elem.row()) {
                        manager.write_chunk(chunk).await?;
                    }
                }
            }
            if let Some(chunk) = builder.consume_all() {
                manager.write_chunk(chunk).await?;
            }
            drop(groups);
            if let Some(charged) = &mut charged {
                charged.release();
            }
            #[for_await]
            for chunk in input {
                manager.write_chunk(chunk?).await?;
            }
            manager.close_writers().await?;

            // Each partition holds whole groups: its top-n per group is part of the result.
            for partition in 0..manager.partition_num {
                let stream = manager.read_partition(partition).await?;
                let mut sub_spill = spill.clone();
                sub_spill.depth += 1;
                let mut sub = GroupTopNExecutor::<K>::new(
                    Box::new(WrapStreamExecutor::new(child_schema.clone(), stream)),
                    this.column_orders.clone(),
                    this.offset,
                    this.limit,
                    this.with_ties,
                    this.group_key.clone(),
                    format!("{}-sub{}", this.identity, partition),
                    this.chunk_size,
                    ExecutorMemory::unmanaged(heap_meter()),
                );
                sub.spill = Some(sub_spill);
                #[for_await]
                for chunk in Box::new(sub).execute() {
                    yield chunk?;
                }
                manager.clear_partition(partition).await?;
            }
            return Ok(());
        }

        let mut chunk_builder = DataChunkBuilder::new(this.schema.data_types(), this.chunk_size);
        for (_, h) in &mut groups {
            let mut heap = TopNHeap::empty();
            swap(&mut heap, h);
            for ele in heap.dump() {
                if let Some(spilled) = chunk_builder.append_one_row(ele.row()) {
                    yield spilled
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
        }
    }
}

/// Writes the rows of a spilling top-n per group to partitions by a hash of the group key, and
/// reads them back. One file per partition: the rows have the schema of the input, those that
/// the heaps kept first, then the rest of the input.
struct GroupTopNSpillManager {
    op: SpillOp,
    partition_num: usize,
    writers: Vec<SpillWriter>,
    builders: Vec<DataChunkBuilder>,
    hasher: SpillBuildHasher,
    group_key: Vec<usize>,
    data_types: Vec<DataType>,
    chunk_size: usize,
    metrics: Arc<BatchSpillMetrics>,
}

impl GroupTopNSpillManager {
    fn new(
        spill: &GroupTopNSpill,
        identity: &str,
        partition_num: usize,
        group_key: Vec<usize>,
        data_types: Vec<DataType>,
        chunk_size: usize,
    ) -> Result<Self> {
        let suffix_uuid = uuid::Uuid::new_v4();
        let op = SpillOp::create(
            format!("{}-{}/", identity, suffix_uuid),
            spill.backend.clone(),
        )?;
        Ok(Self {
            op,
            partition_num,
            writers: Vec::with_capacity(partition_num),
            builders: Vec::with_capacity(partition_num),
            // A seed of its own: a partition that is spilled again is split by another hash.
            hasher: SpillBuildHasher(suffix_uuid.as_u64_pair().1),
            group_key,
            data_types,
            chunk_size,
            metrics: spill.metrics.clone(),
        })
    }

    async fn init_writers(&mut self) -> Result<()> {
        self.op.reserve_files(self.partition_num).await?;
        for partition in 0..self.partition_num {
            let writer = self.op.writer_with(&Self::file_name(partition)).await?;
            self.writers.push(writer);
            self.builders.push(DataChunkBuilder::new(
                self.data_types.clone(),
                self.chunk_size,
            ));
        }
        Ok(())
    }

    fn file_name(partition: usize) -> String {
        format!("rows-p{}", partition)
    }

    async fn write_chunk(&mut self, chunk: DataChunk) -> Result<()> {
        let hash_codes = chunk.get_hash_values(&self.group_key, self.hasher);
        let (columns, vis) = chunk.into_parts_v2();
        for partition in 0..self.partition_num {
            let partition_vis = vis.clone()
                & Bitmap::from_iter(
                    hash_codes
                        .iter()
                        .map(|code| code.value() as usize % self.partition_num == partition),
                );
            let partition_chunk = DataChunk::from_parts(columns.clone(), partition_vis);
            for output in self.builders[partition].append_chunk(partition_chunk) {
                Self::write_out(&mut self.writers[partition], &self.metrics, output).await?;
            }
        }
        Ok(())
    }

    async fn write_out(
        writer: &mut SpillWriter,
        metrics: &BatchSpillMetrics,
        chunk: DataChunk,
    ) -> Result<()> {
        let chunk_pb: PbDataChunk = chunk.to_protobuf();
        let buf = Message::encode_to_vec(&chunk_pb);
        let len_bytes = Bytes::copy_from_slice(&(buf.len() as u32).to_le_bytes());
        metrics
            .batch_spill_write_bytes
            .inc_by((buf.len() + len_bytes.len()) as u64);
        writer.write(len_bytes).await?;
        writer.write(buf).await?;
        Ok(())
    }

    async fn close_writers(&mut self) -> Result<()> {
        for partition in 0..self.partition_num {
            if let Some(output) = self.builders[partition].consume_all() {
                Self::write_out(&mut self.writers[partition], &self.metrics, output).await?;
            }
        }
        for mut writer in self.writers.drain(..) {
            writer.close().await?;
        }
        Ok(())
    }

    async fn read_partition(&mut self, partition: usize) -> Result<BoxedDataChunkStream> {
        let reader = self.op.reader_with(&Self::file_name(partition)).await?;
        Ok(SpillOp::read_stream(reader, self.metrics.clone()))
    }

    async fn clear_partition(&mut self, partition: usize) -> Result<()> {
        self.op.delete(&Self::file_name(partition)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use risingwave_common::array::{Array, I32Array};
    use risingwave_common::catalog::Field;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_group_top_n_executor() {
        let parent_mem = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                ],
            };
            let mut mock_executor = MockExecutor::new(schema);
            mock_executor.add(DataChunk::from_pretty(
                "i i i
             1 5 1
             2 4 1
             3 3 1
             4 2 1
             5 1 1
             1 6 2
             2 5 2
             3 4 2
             4 3 2
             5 2 2
             ",
            ));
            let column_orders = vec![
                ColumnOrder {
                    column_index: 1,
                    order_type: OrderType::ascending(),
                },
                ColumnOrder {
                    column_index: 0,
                    order_type: OrderType::ascending(),
                },
            ];
            let mem_ctx = MemoryContext::new(
                Some(parent_mem.clone()),
                LabelGuardedIntGauge::test_int_gauge::<4>(),
            );
            let top_n_executor = (GroupTopNExecutorBuilder {
                child: Box::new(mock_executor),
                column_orders,
                offset: 1,
                limit: 3,
                with_ties: false,
                group_key: vec![2],
                group_key_types: vec![DataType::Int32],
                identity: "GroupTopNExecutor".to_owned(),
                chunk_size: CHUNK_SIZE,
                memory: mem_ctx.into(),
                spill: None,
            })
            .dispatch();

            let fields = &top_n_executor.schema().fields;
            assert_eq!(fields[0].data_type, DataType::Int32);
            assert_eq!(fields[1].data_type, DataType::Int32);

            let mut stream = top_n_executor.execute();
            let res = stream.next().await;

            assert!(res.is_some());
            if let Some(res) = res {
                let res = res.unwrap();
                assert!(
                    res == DataChunk::from_pretty(
                        "
                    i i i
                    4 2 1
                    3 3 1
                    2 4 1
                    4 3 2
                    3 4 2
                    2 5 2
                    "
                    ) || res
                        == DataChunk::from_pretty(
                            "
                    i i i
                    4 3 2
                    3 4 2
                    2 5 2
                    4 2 1
                    3 3 1
                    2 4 1
                    "
                        )
                );
            }

            let res = stream.next().await;
            assert!(res.is_none());
        }

        assert_eq!(0, parent_mem.get_bytes_used());
    }

    /// The heaps of the groups grow with their rows: 1000 groups of one row each, with a limit
    /// of 1000 rows per group, do not reserve room for 1000 rows per group.
    #[tokio::test]
    async fn test_group_top_n_heaps_grow_with_their_rows() {
        const GROUPS: i32 = 1000;
        const LIMIT: usize = 1000;
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::new(
            vec![
                I32Array::from_iter(0..GROUPS).into_ref(),
                I32Array::from_iter(0..GROUPS).into_ref(),
            ],
            GROUPS as usize,
        ));
        let mem_ctx = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let top_n_executor = (GroupTopNExecutorBuilder {
            child: Box::new(mock_executor),
            column_orders: vec![ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            }],
            offset: 0,
            limit: LIMIT,
            with_ties: false,
            group_key: vec![0],
            group_key_types: vec![DataType::Int32],
            identity: "GroupTopNExecutor".to_owned(),
            // One output chunk per two groups: the other groups are still in memory when the
            // first chunk is out.
            chunk_size: 2,
            memory: mem_ctx.clone().into(),
            spill: None,
        })
        .dispatch();

        let mut stream = top_n_executor.execute();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.cardinality(), 2);
        let reserved_up_front = (GROUPS as usize - 2) * LIMIT * std::mem::size_of::<HeapElem>();
        let used = mem_ctx.get_bytes_used() as usize;
        assert!(
            used < reserved_up_front / 10,
            "{used} bytes used, {reserved_up_front} bytes for heaps of the limit"
        );

        let mut rows = first.cardinality();
        while let Some(chunk) = stream.next().await {
            rows += chunk.unwrap().cardinality();
        }
        assert_eq!(rows, GROUPS as usize);
        drop(stream);
        assert_eq!(mem_ctx.get_bytes_used(), 0);
    }

    /// Rows `(k, v, c)`: `rows` rows, `k` in `0..groups`, `v` distinct, `c` = 0 everywhere, a
    /// varchar `w` to make wide rows. Split into chunks of `chunk_rows` rows.
    fn input_chunks(rows: i32, groups: i32, chunk_rows: usize) -> (Schema, Vec<DataChunk>) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let mut builder = DataChunkBuilder::new(schema.data_types(), chunk_rows);
        let mut chunks = vec![];
        for i in 0..rows {
            // `v` is a permutation of `0..rows`: the order is total.
            let v = (i * 7919) % rows;
            let row = OwnedRow::new(vec![
                Some(ScalarImpl::Int32((i * 31) % groups)),
                Some(ScalarImpl::Int32(v)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Utf8(format!("{:0>64}", i).into())),
            ]);
            if let Some(chunk) = builder.append_one_row(row) {
                chunks.push(chunk);
            }
        }
        if let Some(chunk) = builder.consume_all() {
            chunks.push(chunk);
        }
        (schema, chunks)
    }

    fn mock(schema: &Schema, chunks: &[DataChunk]) -> BoxedExecutor {
        let mut mock_executor = MockExecutor::new(schema.clone());
        for chunk in chunks {
            mock_executor.add(chunk.clone());
        }
        Box::new(mock_executor)
    }

    /// The output rows, as a sorted list of strings.
    async fn collect_rows(executor: BoxedExecutor) -> Vec<String> {
        let mut rows = vec![];
        let mut stream = executor.execute();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            rows.extend(
                chunk
                    .rows()
                    .map(|row| format!("{:?}", row.into_owned_row())),
            );
        }
        rows.sort();
        rows
    }

    fn group_top_n(
        child: BoxedExecutor,
        column_orders: Vec<ColumnOrder>,
        offset: usize,
        limit: usize,
        with_ties: bool,
        memory: ExecutorMemory,
        spill: Option<GroupTopNSpill>,
        chunk_size: usize,
    ) -> BoxedExecutor {
        (GroupTopNExecutorBuilder {
            child,
            column_orders,
            offset,
            limit,
            with_ties,
            group_key: vec![0],
            group_key_types: vec![DataType::Int32],
            identity: "GroupTopNExecutor".to_owned(),
            chunk_size,
            memory,
            spill,
        })
        .dispatch()
    }

    fn order_by(column_index: usize, order_type: OrderType) -> Vec<ColumnOrder> {
        vec![ColumnOrder {
            column_index,
            order_type,
        }]
    }

    fn unlimited() -> MemoryContext {
        MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX)
    }

    /// Without order, the rows are emitted as they arrive, and they are those that the heaps
    /// keep: the first `limit` rows of each group in input order (here, the heaps are used by
    /// ordering on a constant column, which makes every row of a group equal).
    #[tokio::test]
    async fn test_unordered_group_top_n_keeps_the_rows_of_the_heaps() {
        let (schema, chunks) = input_chunks(5000, 37, 300);
        for limit in [1, 3, 1000] {
            let streamed = collect_rows(group_top_n(
                mock(&schema, &chunks),
                vec![],
                0,
                limit,
                false,
                unlimited().into(),
                None,
                CHUNK_SIZE,
            ))
            .await;
            let heaps = collect_rows(group_top_n(
                mock(&schema, &chunks),
                order_by(2, OrderType::ascending()),
                0,
                limit,
                false,
                unlimited().into(),
                None,
                CHUNK_SIZE,
            ))
            .await;
            assert_eq!(streamed, heaps, "limit {limit}");
            assert_eq!(streamed.len(), (37 * limit).min(5000));
        }
    }

    /// The unordered top-n per group emits the rows of the first input chunk before it reads the
    /// second one, and keeps only the group keys: a limit above it can stop it early.
    #[tokio::test]
    async fn test_unordered_group_top_n_does_not_block() {
        let (schema, chunks) = input_chunks(2000, 10, 1000);
        let first = chunks[0].clone();
        // The input fails if it is read beyond its first chunk.
        let stream = futures::stream::iter(vec![Ok(first)])
            .chain(futures::stream::poll_fn(
                |_| -> std::task::Poll<Option<Result<DataChunk>>> {
                    panic!("the second input chunk must not be read")
                },
            ))
            .boxed();
        let mem_ctx = unlimited();
        let executor = group_top_n(
            Box::new(WrapStreamExecutor::new(schema, stream)),
            vec![],
            0,
            2,
            false,
            mem_ctx.clone().into(),
            None,
            CHUNK_SIZE,
        );
        let mut output = executor.execute();
        let chunk = output.next().await.unwrap().unwrap();
        // Two rows of each of the 10 groups, copied out of the input chunk.
        assert_eq!(chunk.cardinality(), 20);
        assert_eq!(chunk.capacity(), 20);
        // Only the 10 keys are held: far less than the 64 KB of varchars of the input chunk.
        let used = mem_ctx.get_bytes_used();
        assert!(used > 0);
        assert!(used < 16 << 10, "{used} bytes used");
        drop(output);
        assert_eq!(mem_ctx.get_bytes_used(), 0);
    }

    /// The keys of variable size are counted with their bytes.
    #[tokio::test]
    async fn test_unordered_group_top_n_counts_its_keys() {
        let (schema, chunks) = input_chunks(3000, 3000, 1000);
        let mem_ctx = unlimited();
        let executor = (GroupTopNExecutorBuilder {
            child: mock(&schema, &chunks),
            column_orders: vec![],
            offset: 0,
            limit: 1,
            with_ties: false,
            // A varchar key of 64 bytes per row.
            group_key: vec![3],
            group_key_types: vec![DataType::Varchar],
            identity: "GroupTopNExecutor".to_owned(),
            chunk_size: CHUNK_SIZE,
            memory: mem_ctx.clone().into(),
            spill: None,
        })
        .dispatch();
        let mut output = executor.execute();
        let mut rows = 0;
        let mut peak = 0;
        while let Some(chunk) = output.next().await {
            rows += chunk.unwrap().cardinality();
            peak = peak.max(mem_ctx.get_bytes_used());
        }
        assert_eq!(rows, 3000);
        // After the last chunk, its 3000 keys are held (released when the input ends).
        assert!(peak >= 3000 * 64, "{peak} bytes used");
        drop(output);
        assert_eq!(mem_ctx.get_bytes_used(), 0);
    }

    /// The memory of `DISTINCT ON (k)` without order over wide rows: the heaps (here reached by
    /// ordering on a constant column) hold one row per group until the input ends, the unordered
    /// executor only the keys.
    #[tokio::test]
    async fn test_unordered_group_top_n_memory_compared_with_heaps() {
        const GROUPS: i32 = 5000;
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Varchar),
            ],
        };
        // 4 rows per group, rows of about 2 KB.
        let mut builder = DataChunkBuilder::new(schema.data_types(), 1024);
        let mut chunks = vec![];
        for i in 0..GROUPS * 4 {
            let row = OwnedRow::new(vec![
                Some(ScalarImpl::Int32(i % GROUPS)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Utf8(format!("{:0>2000}", i).into())),
            ]);
            if let Some(chunk) = builder.append_one_row(row) {
                chunks.push(chunk);
            }
        }
        if let Some(chunk) = builder.consume_all() {
            chunks.push(chunk);
        }

        // The heaps: everything is held when the first output row is out.
        let heaps_ctx = unlimited();
        let mut heaps = group_top_n(
            mock(&schema, &chunks),
            order_by(1, OrderType::ascending()),
            0,
            1,
            false,
            heaps_ctx.clone().into(),
            None,
            1,
        )
        .execute();
        let _first = heaps.next().await.unwrap().unwrap();
        let heaps_peak = heaps_ctx.get_bytes_used();
        let mut rows = 1;
        while let Some(chunk) = heaps.next().await {
            rows += chunk.unwrap().cardinality();
        }
        assert_eq!(rows, GROUPS as usize);

        // The unordered executor: the most it holds after an output chunk.
        let streamed_ctx = unlimited();
        let mut streamed = group_top_n(
            mock(&schema, &chunks),
            vec![],
            0,
            1,
            false,
            streamed_ctx.clone().into(),
            None,
            CHUNK_SIZE,
        )
        .execute();
        let mut streamed_peak = 0;
        let mut rows = 0;
        while let Some(chunk) = streamed.next().await {
            rows += chunk.unwrap().cardinality();
            streamed_peak = streamed_peak.max(streamed_ctx.get_bytes_used());
        }
        assert_eq!(rows, GROUPS as usize);
        println!(
            "DISTINCT ON over {} rows of 2 KB, {GROUPS} groups: heaps {heaps_peak} bytes, \
             unordered {streamed_peak} bytes",
            GROUPS * 4
        );
        assert!(heaps_peak > (GROUPS as i64) * 2000);
        assert!(
            streamed_peak * 5 < heaps_peak,
            "heaps {heaps_peak} bytes, unordered {streamed_peak} bytes"
        );
    }

    fn spill_config(mem_ctx: MemoryContext) -> GroupTopNSpill {
        GroupTopNSpill {
            mem_ctx,
            backend: SpillBackend::Memory,
            metrics: BatchSpillMetrics::for_test(),
            min_bytes: 0,
            depth: 0,
        }
    }

    /// With an order, the spilling top-n per group returns the same rows as the one that keeps
    /// its heaps in memory: once spilled at the first chunk (a context that refuses everything,
    /// partitions spilled again up to the maximum depth), and with a context that holds a part
    /// of the heaps.
    #[tokio::test]
    async fn test_spilling_group_top_n_returns_the_same_rows() {
        let (schema, chunks) = input_chunks(6000, 101, 500);
        let cases = [
            (order_by(1, OrderType::ascending()), 0, 2, false),
            (order_by(1, OrderType::descending()), 1, 3, false),
            // Ties on a constant column: every row of a group.
            (order_by(2, OrderType::ascending()), 0, 1, true),
        ];
        for (orders, offset, limit, with_ties) in cases {
            let expected = collect_rows(group_top_n(
                mock(&schema, &chunks),
                orders.clone(),
                offset,
                limit,
                with_ties,
                unlimited().into(),
                None,
                CHUNK_SIZE,
            ))
            .await;
            for limit_bytes in [0, 64 << 10] {
                let spill_ctx = MemoryContext::new_with_mem_limit(
                    None,
                    LabelGuardedIntGauge::test_int_gauge::<4>(),
                    limit_bytes,
                );
                let meter = unlimited();
                let actual = collect_rows(group_top_n(
                    mock(&schema, &chunks),
                    orders.clone(),
                    offset,
                    limit,
                    with_ties,
                    meter.clone().into(),
                    Some(spill_config(spill_ctx.clone())),
                    CHUNK_SIZE,
                ))
                .await;
                assert_eq!(
                    actual, expected,
                    "{orders:?} offset {offset} limit {limit} ties {with_ties} memory {limit_bytes}"
                );
                // Everything was given back.
                assert_eq!(spill_ctx.get_bytes_used(), 0);
                assert_eq!(meter.get_bytes_used(), 0);
            }
        }
    }

    /// A spilling top-n per group that fits in its memory does not spill, and keeps its heaps
    /// counted in the context of the operator.
    #[tokio::test]
    async fn test_spilling_group_top_n_charges_its_heaps() {
        let (schema, chunks) = input_chunks(1000, 10, 100);
        let spill_ctx = unlimited();
        let meter = unlimited();
        let executor = group_top_n(
            mock(&schema, &chunks),
            order_by(1, OrderType::ascending()),
            0,
            1000,
            false,
            meter.clone().into(),
            Some(spill_config(spill_ctx.clone())),
            // One output chunk per row: the heaps are still counted when the first one is out.
            1,
        );
        let mut output = executor.execute();
        let first = output.next().await.unwrap().unwrap();
        assert_eq!(first.cardinality(), 1);
        assert!(spill_ctx.get_bytes_used() > 0);
        let mut rows = 1;
        while let Some(chunk) = output.next().await {
            rows += chunk.unwrap().cardinality();
        }
        assert_eq!(rows, 1000);
        drop(output);
        assert_eq!(spill_ctx.get_bytes_used(), 0);
    }

    #[test]
    fn test_is_unordered() {
        assert!(is_unordered(&[], 0, false));
        assert!(!is_unordered(&[], 1, false));
        assert!(!is_unordered(&[], 0, true));
        assert!(!is_unordered(
            &order_by(0, OrderType::ascending()),
            0,
            false
        ));
    }
}
