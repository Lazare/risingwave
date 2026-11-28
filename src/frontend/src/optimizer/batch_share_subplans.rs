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

//! Computing once the identical sub-plans of a batch stage (`batch_share_identical_subplans`).
//!
//! The batch optimizer turns the plan into a tree first (`DAG_TO_TREE`): a view or a CTE read
//! twice is planned, and executed, twice. [`share_identical_batch_subplans`] runs on the final
//! batch plan, after its distribution. Within one stage, it replaces the copies of a sub-plan by
//! one producer and readers ([`BatchSpool`]): each task computes the sub-plan once, keeps its rows
//! and gives all of them to every copy.
//!
//! Only copies of the same stage are shared: they run in the same tasks, and a reader in task `i`
//! receives exactly what the copy computed in task `i` (same scan partitions, same rows from each
//! exchange). A copy may contain exchanges: only the producer keeps them, so the stages below the
//! other copies are not run at all. Copies in different stages are left as they are.
//!
//! A sub-plan is shared when it:
//! - appears at least twice in the stage (structural equality of the batch nodes, which compares
//!   everything but the node ids, including distributions and orders);
//! - only has nodes that compute the same rows from the same tables each time they run (scans,
//!   exchanges, projections, filters, joins, aggregations, sorts, top-n, limits, unions, over
//!   windows, ...): no source, no external query, no DML, no system table;
//! - has no impure expression (`random()`, `now()` that was not folded, volatile UDF...) and no
//!   user-defined aggregate;
//! - contains an aggregation, a join, a sort, a top-n or an over window: a chain of scan, filter and
//!   projection costs less to compute again than to keep;
//! - has a copy that is not below a `LIMIT`, or computes all its rows before its first one anyway
//!   (an aggregation, a sort or a top-n at its root): the spool computes the whole sub-plan before
//!   giving its first row, where a join below a limit may stop early.
//!
//! The largest shared sub-plans win: the copies inside a reader are gone, and a sub-plan repeated
//! inside the producer and elsewhere is shared again on its own.
//!
//! Correctness: a reader returns all the rows of the producer, in the order the producer computed
//! them, so any order or distribution property of the copy holds. Where a sub-plan may return
//! different rows each time it runs (ties of a limit or a top-n, the order of `array_agg` without
//! `ORDER BY`), every copy now gets the same rows, one of the results the copies could each have
//! computed.

use std::collections::{HashMap, HashSet};

use risingwave_expr::aggregate::AggType;
use risingwave_expr::window_function::WindowFuncKind;

use crate::expr::{ExprImpl, ExprVisitor, is_impure};
use crate::optimizer::plan_node::{
    BatchPlanNodeType, BatchPlanRef as PlanRef, BatchSpool, VisitExprsRecursive,
};

/// The stage of a node: the position, as the input indexes from the root of the plan, of the
/// exchange that starts the stage; empty for the root stage.
type Stage = Vec<u32>;

/// A sub-plan in a stage.
type Key = (Stage, PlanRef);

/// Shares the identical sub-plans of each stage of `plan` (see the module documentation). Returns
/// `plan` itself when nothing is shared.
#[allow(clippy::mutable_key_type)]
pub fn share_identical_batch_subplans(plan: PlanRef) -> PlanRef {
    let mut counter = Counter::default();
    counter.visit(&plan, &Stage::new(), &mut Vec::new(), false);
    let Counter { counts, unlimited } = counter;
    let repeated: HashSet<Key> = counts
        .into_iter()
        .filter(|(key, count)| *count >= 2 && unlimited.contains(key))
        .map(|(key, _)| key)
        .collect();
    if repeated.is_empty() {
        return plan;
    }

    let mut sharer = Sharer {
        repeated,
        pure: HashMap::new(),
        spools: HashMap::new(),
        readers: HashSet::new(),
    };
    let (shared, changed) = sharer.rewrite(&plan, &Stage::new(), &mut Vec::new());
    if !changed {
        return plan;
    }
    // A producer whose other copies were all inside the readers of a larger shared sub-plan has
    // no reader: it computes its input as before.
    let unread: HashSet<u32> = sharer
        .spools
        .values()
        .filter(|id| !sharer.readers.contains(*id))
        .copied()
        .collect();
    if unread.is_empty() {
        shared
    } else {
        remove_unread(&shared, &unread).0
    }
}

/// The stage of the inputs of `node`, at `path` in `stage`.
fn input_stage(node: &PlanRef, stage: &Stage, path: &[u32]) -> Stage {
    if node.node_type() == BatchPlanNodeType::BatchExchange {
        path.to_vec()
    } else {
        stage.clone()
    }
}

/// Whether `node` computes the same rows from the same inputs each time it runs.
fn is_allowed(node: &PlanRef) -> bool {
    use BatchPlanNodeType::*;
    let builtin = |calls: &[crate::optimizer::plan_node::generic::PlanAggCall]| {
        calls
            .iter()
            .all(|call| matches!(call.agg_type, AggType::Builtin(_)))
    };
    match node.node_type() {
        BatchSeqScan | BatchExchange | BatchProject | BatchFilter | BatchValues | BatchUnion
        | BatchLimit | BatchTopN | BatchGroupTopN | BatchSort | BatchHopWindow | BatchExpand
        | BatchProjectSet | BatchMaxOneRow | BatchHashJoin | BatchNestedLoopJoin
        | BatchLookupJoin => true,
        BatchHashAgg => node
            .as_batch_hash_agg()
            .is_some_and(|agg| builtin(agg.agg_calls())),
        BatchSortAgg => node
            .as_batch_sort_agg()
            .is_some_and(|agg| builtin(agg.agg_calls())),
        BatchSimpleAgg => node
            .as_batch_simple_agg()
            .is_some_and(|agg| builtin(agg.agg_calls())),
        BatchOverWindow => node.as_batch_over_window().is_some_and(|over_window| {
            over_window.window_functions().iter().all(|func| {
                !matches!(
                    func.kind,
                    WindowFuncKind::Aggregate(AggType::UserDefined(_) | AggType::WrapScalar(_))
                )
            })
        }),
        _ => false,
    }
}

/// Whether `node` costs enough to be worth keeping its rows instead of computing them again.
fn is_costly(node: &PlanRef) -> bool {
    use BatchPlanNodeType::*;
    matches!(
        node.node_type(),
        BatchHashAgg
            | BatchSortAgg
            | BatchSimpleAgg
            | BatchHashJoin
            | BatchNestedLoopJoin
            | BatchLookupJoin
            | BatchOverWindow
            | BatchGroupTopN
            | BatchTopN
            | BatchSort
    )
}

/// Whether `node` computes all its rows before returning the first one.
fn computes_all_rows_first(node: &PlanRef) -> bool {
    use BatchPlanNodeType::*;
    match node.node_type() {
        BatchHashAgg | BatchSimpleAgg | BatchSort | BatchTopN | BatchGroupTopN => true,
        BatchProject | BatchFilter | BatchExchange => {
            node.inputs().iter().all(computes_all_rows_first)
        }
        _ => false,
    }
}

/// Counts the copies of each sub-plan that may be shared, by stage.
#[derive(Default)]
#[allow(clippy::mutable_key_type)]
struct Counter {
    counts: HashMap<Key, usize>,
    /// The sub-plans with a copy that is not below a limit.
    unlimited: HashSet<Key>,
}

impl Counter {
    /// Visits the sub-plan of `node` at `path` in `stage`, below a limit or not. Returns whether
    /// all its nodes are allowed and whether one of them is costly.
    #[allow(clippy::mutable_key_type)]
    fn visit(
        &mut self,
        node: &PlanRef,
        stage: &Stage,
        path: &mut Vec<u32>,
        under_limit: bool,
    ) -> (bool, bool) {
        let input_stage = input_stage(node, stage, path);
        // `BatchLimitDistinct` (`batch_limit_distinct`) also stops reading its input early.
        let input_under_limit = under_limit
            || matches!(
                node.node_type(),
                BatchPlanNodeType::BatchLimit | BatchPlanNodeType::BatchLimitDistinct
            );
        let mut allowed = is_allowed(node);
        let mut costly = is_costly(node);
        for (i, input) in node.inputs().iter().enumerate() {
            path.push(i as u32);
            let (input_allowed, input_costly) =
                self.visit(input, &input_stage, path, input_under_limit);
            path.pop();
            allowed &= input_allowed;
            costly |= input_costly;
        }
        if allowed && costly {
            let key = (stage.clone(), node.clone());
            if !under_limit || computes_all_rows_first(node) {
                self.unlimited.insert(key.clone());
            }
            *self.counts.entry(key).or_insert(0) += 1;
        }
        (allowed, costly)
    }
}

/// Finds an impure expression.
#[derive(Default)]
struct ImpureFinder {
    impure: bool,
}

impl ExprVisitor for ImpureFinder {
    fn visit_expr(&mut self, expr: &ExprImpl) {
        self.impure |= is_impure(expr);
    }
}

/// Replaces the copies of the repeated sub-plans with spools.
#[allow(clippy::mutable_key_type)]
struct Sharer {
    /// The sub-plans with at least two copies in their stage.
    repeated: HashSet<Key>,
    /// Whether the expressions of a repeated sub-plan are all pure.
    pure: HashMap<PlanRef, bool>,
    /// The spool of each shared sub-plan.
    spools: HashMap<Key, u32>,
    /// The spools that have at least one reader.
    readers: HashSet<u32>,
}

impl Sharer {
    fn is_pure(&mut self, node: &PlanRef) -> bool {
        if let Some(pure) = self.pure.get(node) {
            return *pure;
        }
        let mut finder = ImpureFinder::default();
        node.visit_exprs_recursive(&mut finder);
        self.pure.insert(node.clone(), !finder.impure);
        !finder.impure
    }

    /// Rewrites the sub-plan of `node` at `path` in `stage`. Returns the new sub-plan and whether
    /// it changed.
    fn rewrite(&mut self, node: &PlanRef, stage: &Stage, path: &mut Vec<u32>) -> (PlanRef, bool) {
        let key = (stage.clone(), node.clone());
        if self.repeated.contains(&key) && self.is_pure(node) {
            if let Some(&spool_id) = self.spools.get(&key) {
                self.readers.insert(spool_id);
                return (BatchSpool::new_reader(spool_id, node).into(), true);
            }
            let spool_id = self.spools.len() as u32 + 1;
            self.spools.insert(key, spool_id);
            let (input, _) = self.rewrite_inputs(node, stage, path);
            return (BatchSpool::new_producer(spool_id, input).into(), true);
        }
        self.rewrite_inputs(node, stage, path)
    }

    fn rewrite_inputs(
        &mut self,
        node: &PlanRef,
        stage: &Stage,
        path: &mut Vec<u32>,
    ) -> (PlanRef, bool) {
        let input_stage = input_stage(node, stage, path);
        let mut changed = false;
        let mut inputs = Vec::new();
        for (i, input) in node.inputs().iter().enumerate() {
            path.push(i as u32);
            let (new_input, input_changed) = self.rewrite(input, &input_stage, path);
            path.pop();
            changed |= input_changed;
            inputs.push(new_input);
        }
        if changed {
            (node.clone_root_with_inputs(&inputs), true)
        } else {
            (node.clone(), false)
        }
    }
}

/// Replaces the producers of the spools `unread` with their inputs. Returns the new sub-plan and
/// whether it changed.
fn remove_unread(node: &PlanRef, unread: &HashSet<u32>) -> (PlanRef, bool) {
    if let Some(spool) = node.as_batch_spool()
        && unread.contains(&spool.spool_id())
        && let Some(input) = spool.producer_input()
    {
        return (remove_unread(&input, unread).0, true);
    }
    let mut changed = false;
    let mut inputs = Vec::new();
    for input in node.inputs() {
        let (new_input, input_changed) = remove_unread(&input, unread);
        changed |= input_changed;
        inputs.push(new_input);
    }
    if changed {
        (node.clone_root_with_inputs(&inputs), true)
    } else {
        (node.clone(), false)
    }
}
