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

//! Per-table gate of a shared CDC source for selective barriers (design D1 in
//! `docs/dev/src/design/selective-barriers.md`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::ops::Bound;

use risingwave_common::array::Op;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::hash::table_distribution::SINGLETON_VNODE;
use risingwave_common::row::RowDeserializer;
use risingwave_common::types::JsonbVal;

use crate::executor::prelude::*;
use crate::executor::{Barrier, CdcRoute};
use crate::task::{FragmentId, LocalBarrierManager};

/// Column index of `_rw_table_name` in the rows of a shared CDC source.
const RW_TABLE_NAME_COLUMN_IDX: usize = 2;
/// Held rows are stored in the split state table of the source, under keys with this prefix,
/// which no split id uses. Keys are `<prefix>/<fragment id>/<seq>`, zero padded so that they are
/// scanned in order.
const HELD_KEY_PREFIX: &str = "__rw_held__";
/// Rows emitted per chunk when held rows are released.
const RELEASE_CHUNK_SIZE: usize = 1024;

/// Routes the rows of a shared CDC source per table. The `CdcFilter` fragment of a table may
/// sleep: the rows of the table are then held, persisted in the split state table in the same
/// epoch as the offset that covers them, and meta is told (a ring on behalf of the `CdcFilter`
/// fragment) to release this fragment at a next barrier. After that barrier, held rows are
/// emitted in order and deleted.
///
/// Inactive (every row passes) until a selective barrier carries routes for this source, and
/// after any barrier without selective information.
pub struct CdcRouter {
    source_fragment: FragmentId,
    barrier_manager: LocalBarrierManager,
    /// `_rw_table_name` -> `CdcFilter` fragment.
    routes: HashMap<String, FragmentId>,
    /// Whether the last barrier was selective, i.e. routed tables may sleep.
    selective: bool,
    /// Routed fragments released by the last barrier: their rows may be emitted.
    open: HashSet<FragmentId>,
    /// Rows held per fragment, as stored in the state table `(key, value)`, with the row.
    held: BTreeMap<FragmentId, Vec<(OwnedRow, OwnedRow)>>,
    next_seq: u64,
    /// Fragments rung for in the current epoch.
    rang: HashSet<FragmentId>,
}

impl CdcRouter {
    pub fn new(source_fragment: FragmentId, barrier_manager: LocalBarrierManager) -> Self {
        Self {
            source_fragment,
            barrier_manager,
            routes: HashMap::new(),
            selective: false,
            open: HashSet::new(),
            held: BTreeMap::new(),
            next_seq: 0,
            rang: HashSet::new(),
        }
    }

    fn active(&self) -> bool {
        self.selective && !self.routes.is_empty()
    }

    /// Read back the rows held before a recovery. Must be called after the state table is
    /// initialized.
    pub async fn load<S: StateStore>(
        &mut self,
        state_table: &StateTable<S>,
        data_types: &[DataType],
    ) -> StreamExecutorResult<()> {
        let start = OwnedRow::new(vec![Some(ScalarImpl::Utf8(
            format!("{HELD_KEY_PREFIX}/").into(),
        ))]);
        // '0' follows '/' in byte order.
        let end = OwnedRow::new(vec![Some(ScalarImpl::Utf8(
            format!("{HELD_KEY_PREFIX}0").into(),
        ))]);
        let sub_range = &(Bound::Included(start), Bound::Excluded(end));
        // The split state table is a singleton, read with a prefix hint of its whole key: scan
        // its only vnode instead.
        let iter = state_table
            .iter_with_vnode(SINGLETON_VNODE, sub_range, Default::default())
            .await?;
        pin_mut!(iter);
        let deserializer = RowDeserializer::new(data_types.to_vec());
        let mut count = 0;
        while let Some(stored) = iter.next().await {
            let stored = stored?;
            let key = stored.datum_at(0).unwrap().into_utf8();
            let mut parts = key.split('/').skip(1);
            let fragment: u32 = parts.next().unwrap().parse().unwrap();
            let seq: u64 = parts.next().unwrap().parse().unwrap();
            let encoded = stored
                .datum_at(1)
                .unwrap()
                .into_jsonb()
                .as_str()
                .map_err(|e| anyhow::anyhow!(e))?
                .to_owned();
            let row = deserializer
                .deserialize(decode_hex(&encoded).as_slice())
                .map_err(|e| anyhow::anyhow!(e))?;
            self.next_seq = self.next_seq.max(seq + 1);
            self.held
                .entry(fragment.into())
                .or_default()
                .push((stored, row));
            count += 1;
        }
        if count > 0 {
            tracing::info!(rows = count, "cdc router recovered held rows");
        }
        Ok(())
    }

    /// Update the routes and the released fragments from a barrier, before it is yielded.
    pub fn on_barrier(&mut self, barrier: &Barrier) {
        self.rang.clear();
        match &barrier.selective {
            None => {
                self.selective = false;
                self.open.clear();
            }
            Some(info) => {
                self.selective = true;
                let routes: Vec<&CdcRoute> = info
                    .cdc_routes
                    .iter()
                    .filter(|route| route.source_fragment == self.source_fragment)
                    .collect();
                if !info.cdc_routes.is_empty() {
                    self.routes = routes
                        .into_iter()
                        .map(|route| (route.table_name.clone(), route.fragment))
                        .collect();
                }
                self.open = self
                    .routes
                    .values()
                    .filter(|fragment| info.released_fragments.contains(*fragment))
                    .copied()
                    .collect();
            }
        }
        // Rows still held for a fragment that this barrier does not release: ring again, as the
        // ring may have been consumed by a barrier that could not release it.
        let closed: Vec<FragmentId> = self
            .held
            .keys()
            .filter(|fragment| self.active() && !self.open.contains(*fragment))
            .copied()
            .collect();
        for fragment in closed {
            self.ring(fragment);
        }
    }

    /// After the barrier is yielded: emit the held rows that may now be emitted (all of them
    /// when the router is inactive) and delete them from the state table, in the new epoch.
    /// Rows held for a fragment that is no longer routed (dropped table) are discarded.
    pub fn release<S: StateStore>(
        &mut self,
        state_table: &mut StateTable<S>,
        data_types: &[DataType],
    ) -> Vec<StreamChunk> {
        let active = self.active();
        let routed: HashSet<FragmentId> = self.routes.values().copied().collect();
        let releasable: Vec<FragmentId> = self
            .held
            .keys()
            .filter(|fragment| {
                !active || self.open.contains(*fragment) || !routed.contains(*fragment)
            })
            .copied()
            .collect();
        let mut chunks = vec![];
        for fragment in releasable {
            let rows = self.held.remove(&fragment).unwrap();
            // A table dropped while its rows were held: nobody consumes them any more.
            let emit = !active || routed.contains(&fragment);
            for batch in rows.chunks(RELEASE_CHUNK_SIZE) {
                if emit {
                    let ops: Vec<_> = batch.iter().map(|(_, row)| (Op::Insert, row)).collect();
                    chunks.push(StreamChunk::from_rows(&ops, data_types));
                }
                for (stored, _) in batch {
                    state_table.delete(stored);
                }
            }
        }
        chunks
    }

    /// Filter a chunk to emit: rows of routed tables that may not be emitted in this epoch are
    /// held and persisted. Returns the chunk with these rows made invisible.
    pub fn route<S: StateStore>(
        &mut self,
        chunk: StreamChunk,
        state_table: &mut StateTable<S>,
    ) -> StreamChunk {
        if !self.active() {
            return chunk;
        }
        let (data, ops) = chunk.into_parts();
        let mut visibility = BitmapBuilder::with_capacity(data.capacity());
        for (idx, visible) in data.visibility().iter().enumerate() {
            let row = data.row_at_unchecked_vis(idx);
            let fragment = visible
                .then(|| row.datum_at(RW_TABLE_NAME_COLUMN_IDX))
                .flatten()
                .and_then(|name| self.routes.get(name.into_utf8()))
                .copied();
            let Some(fragment) = fragment else {
                visibility.append(visible);
                continue;
            };
            // Ring even when open, so that meta keeps releasing a busy table.
            self.ring(fragment);
            if self.open.contains(&fragment) {
                visibility.append(true);
                continue;
            }
            debug_assert_eq!(ops[idx], Op::Insert);
            let row = row.into_owned_row();
            let key = format!(
                "{HELD_KEY_PREFIX}/{:010}/{:020}",
                fragment.as_raw_id(),
                self.next_seq
            );
            self.next_seq += 1;
            let stored = OwnedRow::new(vec![
                Some(ScalarImpl::Utf8(key.into())),
                Some(ScalarImpl::Jsonb(JsonbVal::from(
                    serde_json::Value::String(encode_hex(&row.value_serialize())),
                ))),
            ]);
            state_table.insert(&stored);
            self.held.entry(fragment).or_default().push((stored, row));
            visibility.append(false);
        }
        StreamChunk::with_visibility(ops, data.columns().to_vec(), visibility.finish())
    }

    fn ring(&mut self, fragment: FragmentId) {
        if self.selective && self.rang.insert(fragment) {
            self.barrier_manager.report_pending_input(fragment);
        }
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

fn decode_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::SelectiveBarrierInfo;

    const TYPES: [DataType; 3] = [DataType::Int64, DataType::Int64, DataType::Varchar];

    async fn state_table(store: &MemoryStateStore) -> StateTable<MemoryStateStore> {
        StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(1),
                vec![
                    ColumnDesc::unnamed(ColumnId::new(0), DataType::Varchar),
                    ColumnDesc::unnamed(ColumnId::new(1), DataType::Jsonb),
                ],
                vec![OrderType::ascending()],
                vec![0],
                // Like the split state table of a source.
                1,
            ),
            store.clone(),
            None,
        )
        .await
    }

    /// Selective barrier at `epoch` releasing the given fragments, with routes `a` -> 11 and
    /// `b` -> 12 of source fragment 1 if `routes`.
    fn barrier(epoch: u64, released: &[u32], routes: bool) -> Barrier {
        let mut barrier = Barrier::new_test_barrier(test_epoch(epoch));
        let cdc_routes = if routes {
            [("a", 11), ("b", 12)]
                .into_iter()
                .map(|(name, fragment)| CdcRoute {
                    source_fragment: 1.into(),
                    fragment: fragment.into(),
                    table_name: name.to_owned(),
                })
                .collect()
        } else {
            vec![]
        };
        barrier.selective = Some(Arc::new(SelectiveBarrierInfo {
            sleeping_fragments: Default::default(),
            waking_fragments: Default::default(),
            released_fragments: released.iter().map(|id| (*id).into()).collect(),
            cdc_routes,
        }));
        barrier
    }

    fn rows(chunk: &StreamChunk) -> Vec<OwnedRow> {
        chunk.rows().map(|(_, row)| row.into_owned_row()).collect()
    }

    fn chunk(rows: &str) -> StreamChunk {
        StreamChunk::from_pretty(&format!("I I T\n{rows}"))
    }

    async fn commit(table: &mut StateTable<MemoryStateStore>, epoch: u64) {
        table
            .commit_for_test(EpochPair::new_test_epoch(test_epoch(epoch)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_route_hold_release_and_recover() {
        let store = MemoryStateStore::new();
        let mut table = state_table(&store).await;
        table
            .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
            .await
            .unwrap();
        let mut router = CdcRouter::new(1.into(), LocalBarrierManager::for_test());

        // Inactive: every row passes.
        let input = chunk("+ 1 1 a\n+ 2 2 b");
        assert_eq!(router.route(input.clone(), &mut table).compact_vis(), input);

        // Routes received, `a` released: rows of `b` and of unknown tables are kept apart.
        router.on_barrier(&barrier(2, &[11], true));
        commit(&mut table, 2).await;
        assert!(router.release(&mut table, &TYPES).is_empty());
        let out = router
            .route(chunk("+ 1 1 a\n+ 2 2 b\n+ 3 3 c\n+ 4 4 b"), &mut table)
            .compact_vis();
        assert_eq!(out, chunk("+ 1 1 a\n+ 3 3 c"));

        // Persisted with the next barrier, which does not release `b`; then a crash.
        router.on_barrier(&barrier(3, &[], false));
        commit(&mut table, 3).await;
        assert!(router.release(&mut table, &TYPES).is_empty());
        drop(table);

        // Recovery: held rows are read back and released, in order, by a barrier that does.
        let mut table = state_table(&store).await;
        table
            .init_epoch(EpochPair::new_test_epoch(test_epoch(4)))
            .await
            .unwrap();
        let mut router = CdcRouter::new(1.into(), LocalBarrierManager::for_test());
        router.load(&table, &TYPES).await.unwrap();
        router.on_barrier(&barrier(5, &[12], true));
        commit(&mut table, 5).await;
        let released = router.release(&mut table, &TYPES);
        assert_eq!(released.len(), 1);
        assert_eq!(rows(&released[0]), rows(&chunk("+ 2 2 b\n+ 4 4 b")));
        commit(&mut table, 6).await;

        // Nothing is left in the state table.
        let mut router = CdcRouter::new(1.into(), LocalBarrierManager::for_test());
        router.load(&table, &TYPES).await.unwrap();
        assert!(router.held.is_empty());
    }

    #[tokio::test]
    async fn test_non_selective_barrier_releases_everything() {
        let store = MemoryStateStore::new();
        let mut table = state_table(&store).await;
        table
            .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
            .await
            .unwrap();
        let mut router = CdcRouter::new(1.into(), LocalBarrierManager::for_test());
        router.on_barrier(&barrier(1, &[], true));
        let out = router.route(chunk("+ 1 1 a\n+ 2 2 b"), &mut table);
        assert_eq!(out.cardinality(), 0);
        router.on_barrier(&Barrier::new_test_barrier(test_epoch(2)));
        commit(&mut table, 2).await;
        let released = router.release(&mut table, &TYPES);
        assert_eq!(released.len(), 2);
        // Rows now pass through.
        assert_eq!(router.route(chunk("+ 3 3 a"), &mut table).cardinality(), 1);
    }
}
