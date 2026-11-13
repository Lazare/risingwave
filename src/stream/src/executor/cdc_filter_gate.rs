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

use std::ops::Bound;

use risingwave_common::array::Op;
use risingwave_common::row::RowExt;

use crate::executor::prelude::*;
use crate::task::{FragmentId, LocalBarrierManager};

/// Rows emitted per chunk when held rows are released.
const RELEASE_CHUNK_SIZE: usize = 1024;

/// Gate placed after a `CdcFilter` when selective barriers are enabled (design D4 in
/// `docs/dev/src/design/selective-barriers.md`).
///
/// The fragment of a `CdcFilter` receives every barrier, like the shared CDC source before it,
/// but its downstream table may sleep. The gate only lets rows through during an epoch opened by
/// a barrier that *releases* its fragment: meta then guarantees that the downstream receives this
/// barrier and the next one, which seals the rows. Otherwise:
/// - rows are held and written to the state table, because the source commits their offset at
///   the next barrier and a crash must not lose them;
/// - meta is told that rows are pending (a ring), so that it releases the fragment at a next
///   barrier, after which held rows are emitted, in order, and deleted from the state table.
///
/// After a recovery, rows that were held are read back and emitted after the first barrier that
/// releases the fragment.
pub struct CdcFilterGateExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    input: Executor,
    /// `(seq, <columns of the input>)`, primary key `seq`.
    state_table: StateTable<S>,
    barrier_manager: LocalBarrierManager,
}

impl<S: StateStore> CdcFilterGateExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        input: Executor,
        state_table: StateTable<S>,
        barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            actor_ctx,
            input,
            state_table,
            barrier_manager,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            actor_ctx,
            input,
            mut state_table,
            barrier_manager,
        } = self;
        let fragment_id = actor_ctx.fragment_id;
        let data_types = input.schema().data_types();
        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        // Whether the last barrier carried selective information, in which case meta needs rings.
        let mut selective = barrier.selective.is_some();
        // Whether rows may be emitted in the current epoch.
        let mut open = barrier.is_fragment_released(fragment_id);
        let mut rang = false;
        // The state table must be initialized after yielding the first barrier: it waits for the
        // previous epoch to be committed, which needs this barrier to be collected.
        yield Message::Barrier(barrier);
        state_table.init_epoch(first_epoch).await?;
        let mut held = HeldRows::load(&state_table).await?;
        if !open && !held.is_empty() {
            ring(&barrier_manager, fragment_id, selective, &mut rang);
        }
        if open {
            for chunk in held.release(&mut state_table, &data_types) {
                yield Message::Chunk(chunk);
            }
        }

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact_vis();
                    if chunk.cardinality() == 0 {
                        continue;
                    }
                    // Ring even when open, so that meta keeps releasing a busy table.
                    ring(&barrier_manager, fragment_id, selective, &mut rang);
                    if open {
                        yield Message::Chunk(chunk);
                    } else {
                        held.hold(&mut state_table, &chunk);
                    }
                }
                Message::Watermark(watermark) => {
                    // The CDC source emits no watermark. Never let one overtake held rows.
                    if open {
                        yield Message::Watermark(watermark);
                    }
                }
                Message::Barrier(barrier) => {
                    let post_commit = state_table.commit(barrier.epoch).await?;
                    selective = barrier.selective.is_some();
                    open = barrier.is_fragment_released(fragment_id);
                    rang = false;
                    // A ring may have been consumed by a barrier that could not release the
                    // fragment (e.g. a command barrier): ring again while rows are held.
                    if !open && !held.is_empty() {
                        ring(&barrier_manager, fragment_id, selective, &mut rang);
                    }
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_ctx.id);
                    yield Message::Barrier(barrier);
                    post_commit.post_yield_barrier(update_vnode_bitmap).await?;
                    if open {
                        // Emitted in the epoch opened by this barrier, which the downstream
                        // receives, and deleted in the same epoch.
                        for chunk in held.release(&mut state_table, &data_types) {
                            yield Message::Chunk(chunk);
                        }
                    }
                }
            }
        }
    }
}

/// Tell meta (once per epoch) that this fragment has rows for its downstream.
fn ring(
    barrier_manager: &LocalBarrierManager,
    fragment_id: FragmentId,
    selective: bool,
    rang: &mut bool,
) {
    if selective && !*rang {
        barrier_manager.report_pending_input(fragment_id);
        *rang = true;
    }
}

/// Rows held by the gate, in arrival order, as stored in the state table (`seq` first).
struct HeldRows {
    rows: Vec<OwnedRow>,
    next_seq: i64,
}

impl HeldRows {
    async fn load<S: StateStore>(state_table: &StateTable<S>) -> StreamExecutorResult<Self> {
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let iter = state_table
            .iter_with_prefix(None::<OwnedRow>, sub_range, Default::default())
            .await?;
        pin_mut!(iter);
        let mut rows = vec![];
        while let Some(row) = iter.next().await {
            rows.push(row?.into_owned_row());
        }
        // Rows are scanned in `seq` order.
        let next_seq = rows
            .last()
            .map_or(0, |row| row.datum_at(0).unwrap().into_int64() + 1);
        if !rows.is_empty() {
            tracing::info!(rows = rows.len(), "cdc filter gate recovered held rows");
        }
        Ok(Self { rows, next_seq })
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn hold<S: StateStore>(&mut self, state_table: &mut StateTable<S>, chunk: &StreamChunk) {
        for (op, row) in chunk.rows() {
            // The CDC source is append-only.
            debug_assert_eq!(op, Op::Insert);
            let row = OwnedRow::new(vec![Some(ScalarImpl::Int64(self.next_seq))])
                .chain(row)
                .into_owned_row();
            self.next_seq += 1;
            state_table.insert(&row);
            self.rows.push(row);
        }
    }

    /// Delete held rows from the state table and return them as chunks, in order.
    fn release<S: StateStore>(
        &mut self,
        state_table: &mut StateTable<S>,
        data_types: &[DataType],
    ) -> Vec<StreamChunk> {
        let rows = std::mem::take(&mut self.rows);
        let chunks = rows
            .chunks(RELEASE_CHUNK_SIZE)
            .map(|rows| {
                // Drop the `seq` column.
                let rows: Vec<_> = rows
                    .iter()
                    .map(|row| (Op::Insert, OwnedRow::new(row.as_inner()[1..].to_vec())))
                    .collect();
                StreamChunk::from_rows(&rows, data_types)
            })
            .collect();
        for row in rows {
            state_table.delete(row);
        }
        chunks
    }
}

impl<S: StateStore> Execute for CdcFilterGateExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    use crate::executor::{Barrier, SelectiveBarrierInfo};

    async fn create_executor(store: &MemoryStateStore) -> (MessageSender, BoxedMessageStream) {
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![]);
        let state_table = StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(1),
                vec![
                    ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
                    ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
                ],
                vec![OrderType::ascending()],
                vec![0],
                0,
            ),
            store.clone(),
            None,
        )
        .await;
        let exec = CdcFilterGateExecutor::new(
            ActorContext::for_test(123),
            source,
            state_table,
            LocalBarrierManager::for_test(),
        );
        (tx, Box::new(exec).execute())
    }

    /// A selective barrier for fragment 0 (the fragment of `ActorContext::for_test`).
    fn selective_barrier(epoch: u64, released: bool) -> Barrier {
        let mut barrier = Barrier::new_test_barrier(test_epoch(epoch));
        barrier.selective = Some(Arc::new(SelectiveBarrierInfo {
            sleeping_fragments: Default::default(),
            waking_fragments: Default::default(),
            released_fragments: released.then_some(0.into()).into_iter().collect(),
            cdc_routes: vec![],
        }));
        barrier
    }

    fn chunk(rows: &str) -> StreamChunk {
        StreamChunk::from_pretty(&format!("I\n{rows}"))
    }

    #[tokio::test]
    async fn test_hold_and_release() {
        let store = MemoryStateStore::new();
        let (mut tx, mut exec) = create_executor(&store).await;

        // Without selective barriers, rows flow through.
        tx.send_barrier(Barrier::new_test_barrier(test_epoch(1)));
        exec.expect_barrier().await;
        tx.push_chunk(chunk("+ 1"));
        assert_eq!(exec.expect_chunk().await, chunk("+ 1"));

        // Not released: rows are held, across barriers that do not release the fragment.
        tx.send_barrier(selective_barrier(2, false));
        exec.expect_barrier().await;
        tx.push_chunk(chunk("+ 2\n+ 3"));
        tx.send_barrier(selective_barrier(3, false));
        exec.expect_barrier().await;
        tx.push_chunk(chunk("+ 4"));

        // Released: held rows are emitted after the barrier, in order, then rows flow.
        tx.send_barrier(selective_barrier(4, true));
        exec.expect_barrier().await;
        assert_eq!(exec.expect_chunk().await, chunk("+ 2\n+ 3\n+ 4"));
        tx.push_chunk(chunk("+ 5"));
        assert_eq!(exec.expect_chunk().await, chunk("+ 5"));
        tx.send_barrier(selective_barrier(5, false));
        exec.expect_barrier().await;
        exec.next_unwrap_pending();
    }

    #[tokio::test]
    async fn test_recover_held_rows() {
        let store = MemoryStateStore::new();
        {
            let (mut tx, mut exec) = create_executor(&store).await;
            tx.send_barrier(selective_barrier(1, false));
            exec.expect_barrier().await;
            tx.push_chunk(chunk("+ 1\n+ 2"));
            // The rows are persisted with this barrier, then the actor stops (crash).
            tx.send_barrier(selective_barrier(2, false));
            exec.expect_barrier().await;
            exec.next_unwrap_pending();
        }

        // After recovery, held rows are emitted after the first barrier that releases the
        // fragment, then deleted.
        let (tx, mut exec) = create_executor(&store).await;
        tx.send_barrier(Barrier::new_test_barrier(test_epoch(3)));
        exec.expect_barrier().await;
        assert_eq!(exec.expect_chunk().await, chunk("+ 1\n+ 2"));
        tx.send_barrier(Barrier::new_test_barrier(test_epoch(4)));
        exec.expect_barrier().await;

        let (tx, mut exec) = create_executor(&store).await;
        tx.send_barrier(Barrier::new_test_barrier(test_epoch(5)));
        exec.expect_barrier().await;
        exec.next_unwrap_pending();
        drop(tx);
    }
}
