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

//! Phased start of the stages of a distributed batch query (`batch_phased_stages`).
//!
//! Without it, every stage starts as soon as its children have created their tasks: all the
//! scans of the query run at once, and the producers of the probe side of a hash join fill their
//! output channels while the join still reads its build side.
//!
//! A hash join (and a nested loop join) reads its build input to the end before it reads the
//! first row of its probe input. So the stages that feed its probe side, and the stages below
//! them, can wait until the stages that feed its build side have finished: by then the join has
//! read (nearly) all of its build input, and asks for its probe input right after. The consumer
//! stage itself starts as before: it only needs to know where the tasks of its deferred children
//! will run, which the scheduler decides in advance (`StageExecution::preassign_workers`). The
//! compute nodes let a consumer wait for a producer task that does not exist yet
//! (`BatchManager::wait_for_task`).
//!
//! **No deadlock.** A stage waits only for stages that feed the build side of a join of one of
//! its ancestor stages, never for its own ancestors or descendants, and a stage in the wait set
//! of another has a strictly smaller wait set: the waits form no cycle. A build stage finishes
//! once its consumer has read all of its output, which needs nothing from the deferred side: the
//! join reads its build input first, and only operators that read all of their inputs may stand
//! between the join and the build exchanges (`drains_inputs`); otherwise (a `LIMIT` on the build
//! side) the join is left alone. Operators that need several inputs at the same time (union,
//! merge sort, a join whose sides are both deferred) get them together: a deferred subtree starts
//! as a whole, and a consumer that is not a hash or nested loop join never defers anything.
//!
//! **Early stop.** A stage whose consumers have all finished produces rows that nobody reads (a
//! satisfied `LIMIT`, a join that stopped reading): the query runner stops it, and the stages
//! below it, instead of waiting for them to end (`stages_to_stop`).

use std::collections::{HashMap, HashSet};

use crate::optimizer::plan_node::BatchPlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, StageGraph, StageId};

/// For each stage that must not start at once, the stages that must have finished first.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StagePhases {
    wait_for: HashMap<StageId, HashSet<StageId>>,
}

impl StagePhases {
    /// Analyzes the stages of a query.
    pub fn new(stage_graph: &StageGraph) -> Self {
        let mut wait_for: HashMap<StageId, HashSet<StageId>> = HashMap::new();
        // Parents before children: the wait set of a stage is known when its plan is visited.
        let order: Vec<StageId> = stage_graph.stage_ids_by_topo_order().collect();
        for stage_id in order.into_iter().rev() {
            let inherited = wait_for.get(&stage_id).cloned().unwrap_or_default();
            let mut found = HashMap::new();
            visit(&stage_graph.stages[&stage_id].root, &inherited, &mut found);
            for (child, waits) in found {
                if !waits.is_empty() {
                    wait_for.entry(child).or_default().extend(waits);
                }
            }
        }
        Self { wait_for }
    }

    /// The stages that must have finished before `stage_id` starts.
    #[cfg(test)]
    pub fn wait_set(&self, stage_id: &StageId) -> Option<&HashSet<StageId>> {
        self.wait_for.get(stage_id)
    }

    /// Whether the stage starts later than in the default schedule.
    pub fn is_deferred(&self, stage_id: &StageId) -> bool {
        self.wait_for.contains_key(stage_id)
    }

    /// The deferred stages.
    pub fn deferred_stages(&self) -> impl Iterator<Item = &StageId> {
        self.wait_for.keys()
    }

    /// Whether the stage may start now, given the stages that are done (finished or stopped).
    pub fn may_start(&self, stage_id: &StageId, done: &HashSet<StageId>) -> bool {
        self.wait_for
            .get(stage_id)
            .is_none_or(|waits| waits.iter().all(|s| done.contains(s)))
    }
}

/// Visits the plan of a stage. `waits` is the wait set of the node: what its exchanges must
/// wait for. Records the wait set of each child stage in `found`.
fn visit(
    node: &ExecutionPlanNode,
    waits: &HashSet<StageId>,
    found: &mut HashMap<StageId, HashSet<StageId>>,
) {
    if node.plan_node_type == BatchPlanNodeType::BatchExchange {
        if let Some(child) = node.source_stage_id {
            found
                .entry(child)
                .or_default()
                .extend(waits.iter().copied());
        }
        return;
    }
    // The input read to the end first, and the one read after it.
    let sides = match node.plan_node_type {
        // Probe side: left, build side: right (`HashJoinExecutor`).
        BatchPlanNodeType::BatchHashJoin if node.children.len() == 2 => Some((1, 0)),
        // The left side is buffered, then the right side is streamed (`NestedLoopJoinExecutor`).
        BatchPlanNodeType::BatchNestedLoopJoin if node.children.len() == 2 => Some((0, 1)),
        _ => None,
    };
    if let Some((build, probe)) = sides {
        let build_node = &node.children[build];
        let mut build_stages = HashSet::new();
        exchanges_below(build_node, &mut build_stages);
        if !build_stages.is_empty() && drains_inputs(build_node) {
            let probe_waits: HashSet<StageId> = waits.union(&build_stages).copied().collect();
            visit(build_node, waits, found);
            visit(&node.children[probe], &probe_waits, found);
            return;
        }
    }
    for child in &node.children {
        visit(child, waits, found);
    }
}

/// The child stages read by the exchanges of a sub-plan of a stage.
fn exchanges_below(node: &ExecutionPlanNode, stages: &mut HashSet<StageId>) {
    if node.plan_node_type == BatchPlanNodeType::BatchExchange {
        stages.extend(node.source_stage_id);
        return;
    }
    for child in &node.children {
        exchanges_below(child, stages);
    }
}

/// Whether the sub-plan, once it has returned all its rows, has read all the rows of all its
/// exchanges: every operator with inputs in it reads them to the end. Operators that may stop
/// early (`LIMIT`, the limited distinct, one row at most...) or that we are not sure about make
/// it `false`.
fn drains_inputs(node: &ExecutionPlanNode) -> bool {
    use BatchPlanNodeType::*;
    if node.children.is_empty() {
        return true;
    }
    let drains = matches!(
        node.plan_node_type,
        BatchProject
            | BatchFilter
            | BatchHashAgg
            | BatchSortAgg
            | BatchSimpleAgg
            | BatchSort
            | BatchTopN
            | BatchGroupTopN
            | BatchHashJoin
            | BatchLookupJoin
            | BatchUnion
            | BatchProjectSet
            | BatchHopWindow
            | BatchExpand
            | BatchOverWindow
    );
    drains && node.children.iter().all(drains_inputs)
}

/// The stages to stop when `completed` has finished: its child stages whose parents are all done,
/// and recursively below them, that are not done yet. They produce rows that nobody will read.
pub fn stages_to_stop(
    stage_graph: &StageGraph,
    parents: impl Fn(&StageId) -> Vec<StageId>,
    completed: StageId,
    done: &HashSet<StageId>,
) -> Vec<StageId> {
    let mut done = done.clone();
    done.insert(completed);
    let mut to_stop = Vec::new();
    let mut queue = vec![completed];
    while let Some(stage) = queue.pop() {
        let Some(children) = stage_graph.get_child_stages(&stage) else {
            continue;
        };
        let mut children: Vec<StageId> = children.iter().copied().collect();
        children.sort();
        for child in children {
            if done.contains(&child) {
                continue;
            }
            if parents(&child).iter().all(|p| done.contains(p)) {
                done.insert(child);
                to_stop.push(child);
                queue.push(child);
            }
        }
    }
    to_stop
}

#[cfg(test)]
mod tests {
    use risingwave_pb::batch_plan::plan_node::NodeBody;

    use super::*;
    use crate::optimizer::plan_node::PlanNodeId;

    fn node(
        plan_node_type: BatchPlanNodeType,
        children: Vec<ExecutionPlanNode>,
    ) -> ExecutionPlanNode {
        ExecutionPlanNode {
            plan_node_id: PlanNodeId(0),
            plan_node_type,
            // The analysis only looks at the type of the nodes.
            node: NodeBody::Values(Default::default()),
            schema: vec![],
            children,
            source_stage_id: None,
        }
    }

    fn exchange(stage: u32) -> ExecutionPlanNode {
        let mut node = node(BatchPlanNodeType::BatchExchange, vec![]);
        node.source_stage_id = Some(stage.into());
        node
    }

    fn scan() -> ExecutionPlanNode {
        node(BatchPlanNodeType::BatchSeqScan, vec![])
    }

    fn hash_join(probe: ExecutionPlanNode, build: ExecutionPlanNode) -> ExecutionPlanNode {
        node(BatchPlanNodeType::BatchHashJoin, vec![probe, build])
    }

    fn set(stages: &[u32]) -> HashSet<StageId> {
        stages.iter().map(|s| (*s).into()).collect()
    }

    /// Analyzes a plan given as `(stage, root, children)`, stage 0 being the root stage.
    fn phases(stages: Vec<(u32, ExecutionPlanNode, Vec<u32>)>) -> StagePhases {
        StagePhases::new(&StageGraph::for_test(stages))
    }

    fn waits(phases: &StagePhases, stage: u32) -> HashSet<StageId> {
        phases.wait_set(&stage.into()).cloned().unwrap_or_default()
    }

    /// Stage 1: hash join of exchange 2 (probe) and exchange 3 (build); 2 and 3 scan.
    fn simple_join() -> Vec<(u32, ExecutionPlanNode, Vec<u32>)> {
        vec![
            (0, exchange(1), vec![1]),
            (1, hash_join(exchange(2), exchange(3)), vec![2, 3]),
            (2, scan(), vec![]),
            (3, scan(), vec![]),
        ]
    }

    #[test]
    fn test_probe_waits_for_build() {
        let phases = phases(simple_join());
        assert_eq!(waits(&phases, 2), set(&[3]));
        // The build side, the join stage and the root start at once.
        assert!(!phases.is_deferred(&3.into()));
        assert!(!phases.is_deferred(&1.into()));
        assert!(!phases.is_deferred(&0.into()));
        assert!(!phases.may_start(&2.into(), &set(&[])));
        assert!(phases.may_start(&2.into(), &set(&[3])));
    }

    #[test]
    fn test_order_of_stage_start() {
        // Simulates the scheduler: a stage starts when it may and its children are known (either
        // scheduled, or deferred and placed in advance); a started stage is scheduled at once and
        // a leaf finishes at once, a parent when its children have finished.
        let graph = StageGraph::for_test(simple_join());
        let phases = StagePhases::new(&graph);
        let mut started: Vec<u32> = vec![];
        let mut done = HashSet::new();
        for _ in 0..10 {
            for stage in [3u32, 2, 1, 0] {
                let id: StageId = stage.into();
                if started.contains(&stage) || !phases.may_start(&id, &done) {
                    continue;
                }
                let children_known = graph
                    .get_child_stages_unchecked(&id)
                    .iter()
                    .all(|c| started.contains(&u32::from(*c)) || phases.is_deferred(c));
                if children_known {
                    started.push(stage);
                }
            }
            // Leaves finish; then their parents.
            for stage in started.clone() {
                let id: StageId = stage.into();
                if graph
                    .get_child_stages_unchecked(&id)
                    .iter()
                    .all(|c| done.contains(c))
                {
                    done.insert(id);
                }
            }
        }
        // The build side starts first, the consumers next, the probe side only after the build
        // side has finished.
        assert_eq!(started, vec![3, 1, 0, 2]);
    }

    #[test]
    fn test_descendants_of_the_probe_side_wait_too() {
        // Stage 2 (probe side) aggregates the output of stage 4.
        let phases = phases(vec![
            (0, exchange(1), vec![1]),
            (1, hash_join(exchange(2), exchange(3)), vec![2, 3]),
            (
                2,
                node(BatchPlanNodeType::BatchHashAgg, vec![exchange(4)]),
                vec![4],
            ),
            (3, scan(), vec![]),
            (4, scan(), vec![]),
        ]);
        assert_eq!(waits(&phases, 2), set(&[3]));
        assert_eq!(waits(&phases, 4), set(&[3]));
        assert!(!phases.is_deferred(&3.into()));
    }

    #[test]
    fn test_nested_joins() {
        // Stage 1: join(probe: join(probe: ex 2, build: ex 3), build: ex 4).
        let phases = phases(vec![
            (0, exchange(1), vec![1]),
            (
                1,
                hash_join(hash_join(exchange(2), exchange(3)), exchange(4)),
                vec![2, 3, 4],
            ),
            (2, scan(), vec![]),
            (3, scan(), vec![]),
            (4, scan(), vec![]),
        ]);
        // The outer build side first, then the inner build side, then the inner probe side.
        assert!(!phases.is_deferred(&4.into()));
        assert_eq!(waits(&phases, 3), set(&[4]));
        assert_eq!(waits(&phases, 2), set(&[3, 4]));
        // No cycle: each stage of a wait set has a strictly smaller wait set.
        for stage in [2u32, 3] {
            for other in waits(&phases, stage) {
                let inner = phases.wait_set(&other).cloned().unwrap_or_default();
                assert!(inner.is_subset(&waits(&phases, stage)));
                assert!(!inner.contains(&other));
            }
        }
    }

    #[test]
    fn test_join_in_another_stage_below_the_probe_side() {
        // Stage 2, on the probe side of stage 1, joins stages 4 (probe) and 5 (build).
        let phases = phases(vec![
            (0, exchange(1), vec![1]),
            (1, hash_join(exchange(2), exchange(3)), vec![2, 3]),
            (2, hash_join(exchange(4), exchange(5)), vec![4, 5]),
            (3, scan(), vec![]),
            (4, scan(), vec![]),
            (5, scan(), vec![]),
        ]);
        assert_eq!(waits(&phases, 2), set(&[3]));
        assert_eq!(waits(&phases, 5), set(&[3]));
        assert_eq!(waits(&phases, 4), set(&[3, 5]));
    }

    #[test]
    fn test_no_deferral_without_a_join_that_reads_one_side_first() {
        // Union, merge sort exchange (one child stage), nested loop below a union...: every input
        // is read at the same time, nothing waits.
        let union = phases(vec![
            (0, exchange(1), vec![1]),
            (
                1,
                node(
                    BatchPlanNodeType::BatchUnion,
                    vec![exchange(2), exchange(3)],
                ),
                vec![2, 3],
            ),
            (2, scan(), vec![]),
            (3, scan(), vec![]),
        ]);
        assert_eq!(union, StagePhases::default());

        let agg = phases(vec![
            (0, exchange(1), vec![1]),
            (
                1,
                node(BatchPlanNodeType::BatchSortAgg, vec![exchange(2)]),
                vec![2],
            ),
            (2, scan(), vec![]),
        ]);
        assert_eq!(agg, StagePhases::default());
    }

    #[test]
    fn test_union_on_the_probe_side_starts_together() {
        // The probe side is a union of two stages: both start together, after the build side.
        let phases = phases(vec![
            (0, exchange(1), vec![1]),
            (
                1,
                hash_join(
                    node(
                        BatchPlanNodeType::BatchUnion,
                        vec![exchange(2), exchange(3)],
                    ),
                    exchange(4),
                ),
                vec![2, 3, 4],
            ),
            (2, scan(), vec![]),
            (3, scan(), vec![]),
            (4, scan(), vec![]),
        ]);
        assert_eq!(waits(&phases, 2), set(&[4]));
        assert_eq!(waits(&phases, 3), set(&[4]));
        // The union of the build side would also start together.
        assert!(!phases.is_deferred(&4.into()));
    }

    #[test]
    fn test_build_side_that_may_stop_early() {
        // A limit on the build side may stop reading the build exchange: its stage would never
        // finish, so the probe side must not wait for it.
        let phases = phases(vec![
            (0, exchange(1), vec![1]),
            (
                1,
                hash_join(
                    exchange(2),
                    node(BatchPlanNodeType::BatchLimit, vec![exchange(3)]),
                ),
                vec![2, 3],
            ),
            (2, scan(), vec![]),
            (3, scan(), vec![]),
        ]);
        assert_eq!(phases, StagePhases::default());
    }

    #[test]
    fn test_build_side_within_the_stage() {
        // The build side scans in the join stage itself: nothing to wait for.
        let phases = phases(vec![
            (0, exchange(1), vec![1]),
            (1, hash_join(exchange(2), scan()), vec![2]),
            (2, scan(), vec![]),
        ]);
        assert_eq!(phases, StagePhases::default());
    }

    #[test]
    fn test_nested_loop_join_defers_its_right_side() {
        let phases = phases(vec![
            (0, exchange(1), vec![1]),
            (
                1,
                node(
                    BatchPlanNodeType::BatchNestedLoopJoin,
                    vec![exchange(2), exchange(3)],
                ),
                vec![2, 3],
            ),
            (2, scan(), vec![]),
            (3, scan(), vec![]),
        ]);
        assert_eq!(waits(&phases, 3), set(&[2]));
        assert!(!phases.is_deferred(&2.into()));
    }

    #[test]
    fn test_stages_to_stop() {
        // Root 0 <- 1 (limit) <- 2 <- 3, and 1 <- 4.
        let graph = StageGraph::for_test(vec![
            (0, exchange(1), vec![1]),
            (
                1,
                node(
                    BatchPlanNodeType::BatchLimit,
                    vec![node(
                        BatchPlanNodeType::BatchUnion,
                        vec![exchange(2), exchange(4)],
                    )],
                ),
                vec![2, 4],
            ),
            (
                2,
                node(BatchPlanNodeType::BatchProject, vec![exchange(3)]),
                vec![3],
            ),
            (3, scan(), vec![]),
            (4, scan(), vec![]),
        ]);
        let parents = |s: &StageId| graph.parents_for_test(s);
        // Stage 1 finished (its limit is satisfied): everything below it stops, except what is
        // already done.
        let mut stop = stages_to_stop(&graph, parents, 1.into(), &set(&[4]));
        stop.sort();
        assert_eq!(stop, vec![2.into(), 3.into()]);
        // A leaf that finishes stops nothing.
        assert!(stages_to_stop(&graph, parents, 3.into(), &set(&[])).is_empty());
    }
}
