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

//! Selective barrier injection: deliver periodic barriers only to the fragments whose inputs may
//! have changed. See `docs/dev/src/design/selective-barriers.md` for the rules and the proof of
//! consistency. This module only decides which fragments receive a barrier.

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::FragmentTypeFlag;
use risingwave_common::types::DataType;
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::expr_node::{RexNode, Type as ExprType};
use risingwave_pb::id::{ActorId, FragmentId, TableId, WorkerId};
use risingwave_pb::stream_plan::now_node::Mode as NowNodeMode;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbSelectiveBarrierInfo, StreamNode};

use crate::controller::fragment::InflightFragmentInfo;

/// Deliver a barrier to every fragment at least every this many barriers, so that a lost ring
/// cannot delay data forever.
const FULL_BARRIER_INTERVAL: u32 = 30;

/// Per-database state of the selective barrier scheduler. A fresh state is created on recovery,
/// so the first barriers after a recovery are delivered to every fragment.
#[derive(Debug, Default)]
pub(super) struct SelectiveBarrierState {
    /// Origins that rang since the last injected barrier.
    pending_rings: HashSet<FragmentId>,
    /// Origins released by the previous barrier (`R_{t-1}`): their descendants must receive the
    /// next barrier, which closes the epoch the released data belongs to.
    prev_released: HashSet<FragmentId>,
    /// Fragments that received the previous barrier. `None` until the first selective barrier,
    /// which is delivered to every fragment.
    prev_woken: Option<HashSet<FragmentId>>,
    barriers_since_full: u32,
    /// For each `Now` fragment with a rounding granularity, the time bucket in which it was last
    /// released.
    now_buckets: HashMap<FragmentId, u64>,
}

/// Which fragments, actors and tables take part in a barrier.
pub(super) struct SelectiveBarrierPlan {
    pub info: PbSelectiveBarrierInfo,
    /// Every fragment receives the barrier: the caller keeps its own actor and table sets.
    pub full: bool,
    /// Actors to collect, per worker. Every worker of the graph is present, possibly with no
    /// actor, so that its epoch chain stays contiguous.
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    /// Tables of the woken fragments, which start the new epoch.
    pub table_ids_to_sync: HashSet<TableId>,
}

/// Static view of the fragment graph needed by the scheduler.
struct GraphView {
    downstreams: HashMap<FragmentId, Vec<FragmentId>>,
    /// Origins that only emit data after ringing (tables fed by DML only).
    gated_origins: HashSet<FragmentId>,
    /// Fragments that must receive every barrier: other origins (connector sources, `Now`,
    /// values, ...) and sinks.
    always_awake: HashSet<FragmentId>,
    /// `Now` fragments, with the period (ms) at which their output may change; 0 means that it
    /// may change at every barrier. They receive every barrier but are only released (and so
    /// only advance and wake their descendants) when their output may change.
    now_periods: HashMap<FragmentId, u64>,
    /// Whether the graph contains operators not supported by selective barriers yet.
    unsupported: bool,
}

impl GraphView {
    fn new<'a>(fragments: impl Iterator<Item = &'a InflightFragmentInfo>) -> Self {
        let mut downstreams: HashMap<FragmentId, Vec<FragmentId>> = HashMap::new();
        let mut upstream_count: HashMap<FragmentId, usize> = HashMap::new();
        let mut gated_origins = HashSet::new();
        let mut always_awake = HashSet::new();
        let mut now_periods = HashMap::new();
        let mut unsupported = false;
        for fragment in fragments {
            let fragment_id = fragment.fragment_id;
            let mask = fragment.fragment_type_mask;
            upstream_count.entry(fragment_id).or_default();
            let mut connector_source = false;
            visit_stream_node(&fragment.nodes, |node| match &node.node_body {
                Some(NodeBody::Merge(merge)) => {
                    downstreams
                        .entry(merge.upstream_fragment_id)
                        .or_default()
                        .push(fragment_id);
                    *upstream_count.entry(fragment_id).or_default() += 1;
                }
                Some(NodeBody::Source(source)) if source.source_inner.is_some() => {
                    connector_source = true;
                }
                // Every table has this operator to receive sinks into the table. It merges
                // several upstream fragments outside of the merge executor, which does not
                // support sleeping upstreams yet: only a table without such sink is supported.
                Some(NodeBody::UpstreamSinkUnion(union)) if !union.init_upstreams.is_empty() => {
                    unsupported = true;
                }
                _ => {}
            });
            // Cross-database snapshot backfill follows the barriers of another database.
            if mask.contains(FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan) {
                unsupported = true;
            }
            let is_dml_table = mask.contains(FragmentTypeFlag::Dml)
                && !connector_source
                && !mask.contains_any([
                    FragmentTypeFlag::Now,
                    FragmentTypeFlag::Values,
                    FragmentTypeFlag::SourceScan,
                    FragmentTypeFlag::FsFetch,
                    FragmentTypeFlag::StreamCdcScan,
                    FragmentTypeFlag::CdcFilter,
                ]);
            if is_dml_table {
                gated_origins.insert(fragment_id);
            }
            if mask.contains(FragmentTypeFlag::Sink) {
                always_awake.insert(fragment_id);
            }
            if mask.contains(FragmentTypeFlag::Now) {
                always_awake.insert(fragment_id);
                now_periods.insert(fragment_id, now_fragment_period_ms(&fragment.nodes));
            }
        }
        for (fragment_id, count) in upstream_count {
            if count == 0 && !gated_origins.contains(&fragment_id) {
                always_awake.insert(fragment_id);
            }
        }
        // A gated origin with an upstream (e.g. a table fed by a sink) is not supported.
        if gated_origins
            .iter()
            .any(|fragment_id| always_awake.contains(fragment_id))
        {
            unsupported = true;
        }
        Self {
            downstreams,
            gated_origins,
            always_awake,
            now_periods,
            unsupported,
        }
    }

    /// Origins released by every barrier: always awake roots other than `Now` (connector sources,
    /// values, ...). Sinks have no descendant to wake.
    fn always_released(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.always_awake
            .iter()
            .filter(|fragment_id| !self.now_periods.contains_key(*fragment_id))
            .copied()
    }

    fn descendants(&self, seeds: impl IntoIterator<Item = FragmentId>) -> HashSet<FragmentId> {
        let mut seen: HashSet<FragmentId> = HashSet::new();
        let mut stack: Vec<FragmentId> = seeds.into_iter().collect();
        while let Some(fragment_id) = stack.pop() {
            if seen.insert(fragment_id)
                && let Some(downstreams) = self.downstreams.get(&fragment_id)
            {
                stack.extend(downstreams.iter().copied());
            }
        }
        seen
    }
}

impl SelectiveBarrierState {
    pub(super) fn on_ring(&mut self, fragment_ids: impl IntoIterator<Item = FragmentId>) {
        self.pending_rings.extend(fragment_ids);
    }

    /// Whether some fragment may be asleep, i.e. barriers must keep carrying selective
    /// information so that fragments can wake up.
    pub(super) fn is_active(&self) -> bool {
        self.prev_woken.is_some()
    }

    /// Plan the next barrier.
    ///
    /// `may_sleep` is false when the barrier must be delivered to every fragment: it carries a
    /// command, it is not a checkpoint, or the database is not eligible (e.g. a job is being
    /// created). Such a barrier still carries selective information so that fragments that were
    /// asleep know that they wake up.
    pub(super) fn plan(
        &mut self,
        fragments: &[&InflightFragmentInfo],
        may_sleep: bool,
        barrier_time_ms: u64,
    ) -> SelectiveBarrierPlan {
        let view = GraphView::new(fragments.iter().copied());
        let all: HashSet<FragmentId> = fragments.iter().map(|f| f.fragment_id).collect();

        // `Now` fragments are released when their output may change: at every barrier without a
        // known rounding granularity, otherwise when the rounded time changes.
        let now_buckets = &mut self.now_buckets;
        now_buckets.retain(|fragment_id, _| view.now_periods.contains_key(fragment_id));
        let released_now: Vec<FragmentId> = view
            .now_periods
            .iter()
            .filter(|(fragment_id, period)| {
                if **period == 0 {
                    return true;
                }
                let bucket = barrier_time_ms / **period;
                now_buckets.insert(**fragment_id, bucket) != Some(bucket)
            })
            .map(|(fragment_id, _)| *fragment_id)
            .collect();

        // Origins released by this barrier: gated origins that rang, `Now` fragments whose output
        // may change, and all other origins.
        let released: HashSet<FragmentId> = self
            .pending_rings
            .drain()
            .filter(|fragment_id| view.gated_origins.contains(fragment_id))
            .chain(view.always_released())
            .chain(released_now)
            .collect();

        let full = !may_sleep
            || view.unsupported
            || self.prev_woken.is_none()
            || self.barriers_since_full + 1 >= FULL_BARRIER_INTERVAL;
        let woken: HashSet<FragmentId> = if full {
            all.clone()
        } else {
            view.descendants(released.iter().chain(self.prev_released.iter()).copied())
                .into_iter()
                .chain(view.always_awake.iter().copied())
                .filter(|fragment_id| all.contains(fragment_id))
                .collect()
        };

        let waking: Vec<FragmentId> = match &self.prev_woken {
            Some(prev_woken) => woken.difference(prev_woken).copied().collect(),
            // No fragment slept before the first selective barrier.
            None => vec![],
        };
        let sleeping: Vec<FragmentId> = all.difference(&woken).copied().collect();
        tracing::debug!(
            full,
            may_sleep,
            unsupported = view.unsupported,
            first = self.prev_woken.is_none(),
            woken = woken.len(),
            sleeping = sleeping.len(),
            waking = waking.len(),
            released = ?released,
            "selective barrier plan"
        );

        let mut node_actors: HashMap<WorkerId, HashSet<ActorId>> = HashMap::new();
        let mut table_ids_to_sync = HashSet::new();
        for fragment in fragments {
            let is_woken = woken.contains(&fragment.fragment_id);
            for (actor_id, actor) in &fragment.actors {
                let actors = node_actors.entry(actor.worker_id).or_default();
                if is_woken {
                    actors.insert(*actor_id);
                }
            }
            if is_woken {
                table_ids_to_sync.extend(fragment.state_table_ids.iter().copied());
            }
        }

        self.barriers_since_full = if full {
            0
        } else {
            self.barriers_since_full + 1
        };
        let released_fragment_ids = released.iter().map(|id| id.as_raw_id()).collect();
        self.prev_released = released;
        self.prev_woken = Some(woken);

        SelectiveBarrierPlan {
            info: PbSelectiveBarrierInfo {
                sleeping_fragment_ids: sleeping.into_iter().map(|id| id.as_raw_id()).collect(),
                waking_fragment_ids: waking.into_iter().map(|id| id.as_raw_id()).collect(),
                released_fragment_ids,
            },
            full,
            node_actors,
            table_ids_to_sync,
        }
    }

    /// Selective information for a barrier that carries a command. Such a barrier is delivered
    /// to every fragment with the actor and table sets of the command; it only tells fragments
    /// that were asleep that they wake up. Afterwards every fragment is awake, so the scheduler
    /// restarts as after its first selective barrier.
    pub(super) fn plan_command<'a>(
        &mut self,
        fragments: impl Iterator<Item = &'a InflightFragmentInfo>,
    ) -> PbSelectiveBarrierInfo {
        let waking: Vec<u32> = match &self.prev_woken {
            Some(prev_woken) => fragments
                .map(|fragment| fragment.fragment_id)
                .filter(|fragment_id| !prev_woken.contains(fragment_id))
                .map(|fragment_id| fragment_id.as_raw_id())
                .collect(),
            None => vec![],
        };
        tracing::debug!(waking = waking.len(), "selective barrier plan for command");
        // Held data of origins that rang is released by this barrier, which every fragment
        // receives; the next barrier is full again and closes the epoch.
        self.pending_rings.clear();
        self.prev_released.clear();
        self.prev_woken = None;
        self.barriers_since_full = 0;
        // `Now` fragments are not released by a command barrier: they do not advance at it.
        PbSelectiveBarrierInfo {
            sleeping_fragment_ids: vec![],
            waking_fragment_ids: waking,
            released_fragment_ids: vec![],
        }
    }
}

/// Upper bound of the release period derived from `date_trunc`. Time zone offsets are multiples
/// of 15 minutes, so a coarser unit in a time zone may change at any 15-minute boundary in UTC.
const MAX_NOW_PERIOD_MS: u64 = 15 * 60 * 1000;

/// Period (ms) at which the output of a `Now` fragment may change, or 0 if it may change at every
/// barrier. Only `date_trunc(unit, now)` projections are recognized: the rounded value only
/// changes at multiples of the unit.
fn now_fragment_period_ms(nodes: &StreamNode) -> u64 {
    let mut update_current = false;
    let mut period: Option<u64> = None;
    let mut every_barrier = false;
    visit_stream_node(nodes, |node| match &node.node_body {
        Some(NodeBody::Now(now)) => {
            update_current = matches!(now.mode, Some(NowNodeMode::UpdateCurrent(_)));
        }
        Some(NodeBody::Project(project)) => {
            for expr in &project.select_list {
                match date_trunc_period_ms(expr) {
                    Some(p) => period = Some(period.map_or(p, |q: u64| q.min(p))),
                    None => every_barrier = true,
                }
            }
        }
        _ => {}
    });
    if !update_current || every_barrier {
        return 0;
    }
    period.unwrap_or(0)
}

/// `date_trunc('<unit>', <input>[, <tz>])` with a constant unit: the period of the unit (capped).
fn date_trunc_period_ms(expr: &ExprNode) -> Option<u64> {
    if expr.function_type() != ExprType::DateTrunc {
        return None;
    }
    let Some(RexNode::FuncCall(call)) = &expr.rex_node else {
        return None;
    };
    let unit = call.children.first()?;
    let Some(RexNode::Constant(datum)) = &unit.rex_node else {
        return None;
    };
    let unit = deserialize_datum(datum.body.as_slice(), &DataType::Varchar).ok()??;
    let period_ms = match unit.as_utf8().to_lowercase().as_str() {
        "second" | "seconds" => 1000,
        "minute" | "minutes" => 60 * 1000,
        "hour" | "hours" | "day" | "days" | "week" | "weeks" | "month" | "months" | "quarter"
        | "quarters" | "year" | "years" | "decade" | "decades" | "century" | "centuries"
        | "millennium" | "millennia" => MAX_NOW_PERIOD_MS,
        _ => return None,
    };
    Some(period_ms.min(MAX_NOW_PERIOD_MS))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::FragmentTypeMask;
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_pb::stream_plan::{MergeNode, SourceNode, StreamNode};

    use super::*;
    use crate::controller::fragment::InflightActorInfo;

    fn merge(upstream: u32) -> StreamNode {
        StreamNode {
            node_body: Some(NodeBody::Merge(Box::new(MergeNode {
                upstream_fragment_id: upstream.into(),
                ..Default::default()
            }))),
            ..Default::default()
        }
    }

    fn fragment(
        id: u32,
        flags: &[FragmentTypeFlag],
        inputs: Vec<StreamNode>,
        actor: u32,
        worker: u32,
        table: u32,
    ) -> InflightFragmentInfo {
        let mut mask = FragmentTypeMask::empty();
        for flag in flags {
            mask.add(*flag);
        }
        let nodes = if inputs.is_empty() {
            // A table without connector: a source node without source inner.
            StreamNode {
                node_body: Some(NodeBody::Source(Box::new(SourceNode {
                    source_inner: None,
                }))),
                ..Default::default()
            }
        } else {
            StreamNode {
                input: inputs,
                ..Default::default()
            }
        };
        InflightFragmentInfo {
            fragment_id: id.into(),
            distribution_type: DistributionType::Hash,
            fragment_type_mask: mask,
            vnode_count: 1,
            nodes,
            actors: HashMap::from([(
                actor.into(),
                InflightActorInfo {
                    worker_id: worker.into(),
                    vnode_bitmap: None,
                    splits: vec![],
                },
            )]),
            state_table_ids: HashSet::from([table.into()]),
        }
    }

    /// Two DML tables (1, 2), a `Now` fragment (5), a view on table 1 (3), a join of the two
    /// tables (4) and a temporal filter on table 2 (6).
    fn graph() -> Vec<InflightFragmentInfo> {
        use FragmentTypeFlag::*;
        vec![
            fragment(1, &[Source, Dml, Mview], vec![], 1, 1, 11),
            fragment(2, &[Source, Dml, Mview], vec![], 2, 1, 12),
            fragment(3, &[Mview], vec![merge(1)], 3, 1, 13),
            fragment(4, &[Mview], vec![merge(1), merge(2)], 4, 2, 14),
            fragment(5, &[Now], vec![merge(99)], 5, 2, 15),
            fragment(6, &[Mview], vec![merge(5), merge(2)], 6, 2, 16),
        ]
    }

    fn ids(ids: &[u32]) -> HashSet<u32> {
        ids.iter().copied().collect()
    }

    fn woken(plan: &SelectiveBarrierPlan, all: &[u32]) -> HashSet<u32> {
        let sleeping: HashSet<u32> = plan.info.sleeping_fragment_ids.iter().copied().collect();
        all.iter()
            .copied()
            .filter(|id| !sleeping.contains(id))
            .collect()
    }

    const ALL: [u32; 6] = [1, 2, 3, 4, 5, 6];

    #[test]
    fn test_sleep_ring_and_closing_barrier() {
        let mut graph = graph();
        // `Now` has no upstream fragment in practice: make it a root.
        graph[4].nodes = StreamNode::default();
        let fragments: Vec<_> = graph.iter().collect();
        let mut state = SelectiveBarrierState::default();

        // The first selective barrier is delivered to every fragment.
        let plan = state.plan(&fragments, true, 0);
        assert!(plan.full);
        assert!(plan.info.sleeping_fragment_ids.is_empty());
        assert!(plan.info.waking_fragment_ids.is_empty());

        // Nothing rang: only `Now` and its descendant receive the barrier.
        let plan = state.plan(&fragments, true, 0);
        assert!(!plan.full);
        assert_eq!(woken(&plan, &ALL), ids(&[5, 6]));
        assert!(plan.info.waking_fragment_ids.is_empty());
        // Worker 1 has no woken actor but still receives the barrier.
        assert_eq!(
            plan.node_actors.get(&WorkerId::from(1)),
            Some(&HashSet::new())
        );
        assert_eq!(
            plan.node_actors.get(&WorkerId::from(2)),
            Some(&HashSet::from([5.into(), 6.into()]))
        );
        assert_eq!(
            plan.table_ids_to_sync,
            HashSet::from([15.into(), 16.into()])
        );

        // Table 1 rings: it wakes up with its descendants.
        state.on_ring([1.into()]);
        let plan = state.plan(&fragments, true, 0);
        assert_eq!(woken(&plan, &ALL), ids(&[1, 3, 4, 5, 6]));
        assert_eq!(
            plan.info
                .waking_fragment_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            ids(&[1, 3, 4])
        );

        // Closing barrier: the data released by the previous barrier must be sealed.
        let plan = state.plan(&fragments, true, 0);
        assert_eq!(woken(&plan, &ALL), ids(&[1, 3, 4, 5, 6]));
        assert!(plan.info.waking_fragment_ids.is_empty());

        // Then they sleep again.
        let plan = state.plan(&fragments, true, 0);
        assert_eq!(woken(&plan, &ALL), ids(&[5, 6]));
    }

    #[test]
    fn test_ring_of_non_gated_fragment_is_ignored() {
        let mut graph = graph();
        graph[4].nodes = StreamNode::default();
        let fragments: Vec<_> = graph.iter().collect();
        let mut state = SelectiveBarrierState::default();
        state.plan(&fragments, true, 0);
        state.plan(&fragments, true, 0);
        // Fragment 3 is not an origin: its ring does not wake it.
        state.on_ring([3.into()]);
        let plan = state.plan(&fragments, true, 0);
        assert_eq!(woken(&plan, &ALL), ids(&[5, 6]));
    }

    #[test]
    fn test_command_and_full_barriers() {
        let mut graph = graph();
        graph[4].nodes = StreamNode::default();
        let fragments: Vec<_> = graph.iter().collect();
        let mut state = SelectiveBarrierState::default();
        state.plan(&fragments, true, 0);
        state.plan(&fragments, true, 0);

        // A barrier that may not sleep is delivered to every fragment and wakes the sleepers.
        let plan = state.plan(&fragments, false, 0);
        assert!(plan.full);
        assert!(plan.info.sleeping_fragment_ids.is_empty());
        assert_eq!(
            plan.info
                .waking_fragment_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            ids(&[1, 2, 3, 4])
        );

        // A command barrier wakes the sleepers, then the scheduler restarts with a full barrier.
        state.plan(&fragments, true, 0);
        let info = state.plan_command(fragments.iter().copied());
        assert!(info.sleeping_fragment_ids.is_empty());
        assert_eq!(
            info.waking_fragment_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            ids(&[1, 2, 3, 4])
        );
        let plan = state.plan(&fragments, true, 0);
        assert!(plan.full);
        assert!(plan.info.waking_fragment_ids.is_empty());
    }

    #[test]
    fn test_periodic_full_barrier() {
        let mut graph = graph();
        graph[4].nodes = StreamNode::default();
        let fragments: Vec<_> = graph.iter().collect();
        let mut state = SelectiveBarrierState::default();
        assert!(state.plan(&fragments, true, 0).full);
        for _ in 1..FULL_BARRIER_INTERVAL {
            assert!(!state.plan(&fragments, true, 0).full);
        }
        assert!(state.plan(&fragments, true, 0).full);
    }

    #[test]
    fn test_unsupported_graph_is_always_full() {
        let mut graph = graph();
        graph[4].nodes = StreamNode::default();
        graph[2]
            .fragment_type_mask
            .add(FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan);
        let fragments: Vec<_> = graph.iter().collect();
        let mut state = SelectiveBarrierState::default();
        for _ in 0..3 {
            assert!(state.plan(&fragments, true, 0).full);
        }
    }

    fn date_trunc(unit: &str) -> ExprNode {
        use risingwave_common::types::ScalarImpl;
        use risingwave_common::util::value_encoding::serialize_datum;
        use risingwave_pb::data::PbDatum;
        use risingwave_pb::expr::FunctionCall;
        let constant = ExprNode {
            rex_node: Some(RexNode::Constant(PbDatum {
                body: serialize_datum(Some(ScalarImpl::Utf8(unit.into()))),
            })),
            ..Default::default()
        };
        let input = ExprNode {
            rex_node: Some(RexNode::InputRef(0)),
            ..Default::default()
        };
        ExprNode {
            function_type: ExprType::DateTrunc as i32,
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![constant, input],
            })),
            ..Default::default()
        }
    }

    fn now_fragment(select_list: Vec<ExprNode>) -> StreamNode {
        use risingwave_pb::stream_plan::{NowModeUpdateCurrent, NowNode, ProjectNode};
        StreamNode {
            node_body: Some(NodeBody::Project(Box::new(ProjectNode {
                select_list,
                ..Default::default()
            }))),
            input: vec![StreamNode {
                node_body: Some(NodeBody::Now(Box::new(NowNode {
                    state_table: None,
                    mode: Some(NowNodeMode::UpdateCurrent(NowModeUpdateCurrent {})),
                }))),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn test_now_period() {
        assert_eq!(
            now_fragment_period_ms(&now_fragment(vec![date_trunc("minute")])),
            60_000
        );
        assert_eq!(
            now_fragment_period_ms(&now_fragment(vec![date_trunc("second")])),
            1_000
        );
        assert_eq!(
            now_fragment_period_ms(&now_fragment(vec![date_trunc("day")])),
            MAX_NOW_PERIOD_MS
        );
        // An expression that is not rounded may change at every barrier.
        let raw = ExprNode {
            rex_node: Some(RexNode::InputRef(0)),
            ..Default::default()
        };
        assert_eq!(
            now_fragment_period_ms(&now_fragment(vec![date_trunc("minute"), raw])),
            0
        );
        assert_eq!(now_fragment_period_ms(&now_fragment(vec![])), 0);
    }

    #[test]
    fn test_now_released_when_rounded_time_changes() {
        let mut graph = graph();
        graph[4].nodes = now_fragment(vec![date_trunc("minute")]);
        let fragments: Vec<_> = graph.iter().collect();
        let mut state = SelectiveBarrierState::default();
        let released = |plan: &SelectiveBarrierPlan| -> HashSet<u32> {
            plan.info.released_fragment_ids.iter().copied().collect()
        };

        // First barrier: full, `Now` is released (first bucket).
        let plan = state.plan(&fragments, true, 60_000);
        assert!(released(&plan).contains(&5));
        // Closing barrier for its descendant, then only `Now` itself stays awake.
        let plan = state.plan(&fragments, true, 61_000);
        assert!(!released(&plan).contains(&5));
        assert_eq!(woken(&plan, &ALL), ids(&[5, 6]));
        let plan = state.plan(&fragments, true, 62_000);
        assert_eq!(woken(&plan, &ALL), ids(&[5]));
        assert!(released(&plan).is_empty());
        // The next minute releases `Now` and wakes its descendant.
        let plan = state.plan(&fragments, true, 120_000);
        assert!(released(&plan).contains(&5));
        assert_eq!(woken(&plan, &ALL), ids(&[5, 6]));
        assert_eq!(
            plan.info
                .waking_fragment_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            ids(&[6])
        );
    }
}
