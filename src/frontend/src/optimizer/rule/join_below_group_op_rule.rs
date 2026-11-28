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

//! A join pushed below a per-group operator (`batch_push_join_below_group_ops`).
//!
//! A "latest row per key" relation, `SELECT DISTINCT ON (k) * FROM x ORDER BY k, ts DESC` (a
//! top-n per group), an aggregation grouped by `k` or a window partitioned by `k`, is often joined
//! with a small selection of the keys: `... JOIN t ON x.k = t.u WHERE t.state = 'open'`. The
//! per-group operator still processes every key of `x` and the join drops most of them afterwards.
//! [`JoinBelowGroupOpRule`] moves the join below the operator, so that it only sees the groups that
//! the join keeps:
//!
//! ```text
//! Join(Inner, x.k = t.u)                 TopN(group x.k, order x.ts DESC, limit 1)
//! ├─TopN(group x.k, order ..., limit 1)  └─Join(Inner, x.k = t.u)
//! │ └─x                          =>        ├─x
//! └─t (unique on u, filtered)              └─t
//! ```
//!
//! The join then goes on down, through filters, projections and the branches of a `UNION ALL`,
//! into the per-group operators below (aggregations on the same key, for example).
//!
//! Why the result is the same. Let `G` be the columns that the operator groups (or partitions, or
//! deduplicates) by, and let every column of the operator that the condition of the join reads be
//! one of `G`, of a type whose equal values are identical (see [`is_simple_type`]). All the rows of
//! a group then agree on every column that the condition reads, so the condition has the same value
//! for every row of the group and a given row `r` of the other side `T`:
//!
//! - inner join, `T` unique on the columns that the condition compares with `=` (the unique key
//!   checks of `unique_key_simplify_rule.rs`, a join with a known key included): a group meets at
//!   most one row `r` of `T`. Below the operator, the join keeps all the rows of the group, each
//!   extended with the same `r`, or none of them. The operator sees the same rows in each kept
//!   group (and the same order of them), so it computes the same result for it, extended with `r`;
//! - left semi join: the join keeps all the rows of a group or none of them, whatever the
//!   duplicates of `T`.
//!
//! Per operator: a top-n per group and a deduplication keep the same rows of a kept group; a
//! window function partitioned by `G` (every window function of the operator) sees the same rows
//! of its partition in the same order; an aggregation computes the same aggregates on the same
//! rows, and the columns of `T` are carried by an `internal_last_seen_value` each (they hold one
//! value per group; the calls that nothing reads are pruned), except those that the condition
//! equates with a group key, which are copied from it.
//! A filter or a projection between the join and the operator commutes with an inner or semi
//! join; a `UNION ALL` distributes over it.
//!
//! The condition must not hold an impure expression (evaluated on more rows below the operator)
//! and must compare at least one column of `G` with a column of `T` with `=`.
//!
//! When: the rewrite is exact, but it moves the join below the operator, where it reads every
//! input row of the operator instead of one row per group. It is worth it when the join keeps few
//! of the rows. With estimates of both (statistics and, with
//! `batch_cardinality_feedback_learning`, the learned ratios), the join is pushed when its
//! estimated output is at most [`SELECTIVE_JOIN_RATIO`] of the input of the operator. Without
//! them, only when `T` is restricted (a filter, a scan predicate, a semi join or a limit somewhere
//! in it) and the condition equates every column of `G` with a column of `T`. An aggregation is
//! never pushed below an inner join with a whole table without filter, where
//! `AggJoinTransposeRule` (`batch_push_agg_below_join`) does the opposite.
//!
//! Batch only. In a streaming job the join below the operator would keep every input row of the
//! operator in its state, instead of one row (or one aggregated value) per group: not a reduction
//! of the state in general.

use std::collections::BTreeSet;

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::PbAggKind;
use risingwave_pb::plan_common::JoinType;

use super::cost_based_join_ordering_rule::estimated_rows;
use super::never_true_predicate_rule::is_simple_type;
use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::is_unique_on_compared_columns;
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::generic::{GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalFilter, LogicalJoin, LogicalProject, LogicalUnion, PlanTreeNode,
    PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// The largest ratio of the estimated output rows of the join to the input rows of the per-group
/// operator for which the join is pushed below the operator.
pub const SELECTIVE_JOIN_RATIO: f64 = 0.25;

/// Pushes an inner or left semi join below a per-group operator of one of its sides (a top-n per
/// group, a deduplication, a window partitioned by the group, an aggregation), when the condition
/// only reads the group columns of that side. See the module documentation.
pub struct JoinBelowGroupOpRule {}

impl JoinBelowGroupOpRule {
    pub fn create() -> BoxedRule {
        Box::new(JoinBelowGroupOpRule {})
    }
}

impl Rule<Logical> for JoinBelowGroupOpRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        if join.should_be_temporal_join() || join.on().conjunctions.iter().any(|e| e.is_impure()) {
            return None;
        }
        match join.join_type() {
            JoinType::Inner => push_left(join).or_else(|| push_left(&swap(join))),
            JoinType::LeftSemi => push_left(join),
            _ => None,
        }
    }
}

/// Renames the columns read by an expression.
struct MapRefs<F: Fn(usize) -> usize>(F);

impl<F: Fn(usize) -> usize> ExprRewriter for MapRefs<F> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new((self.0)(input_ref.index()), input_ref.return_type()).into()
    }
}

/// The same inner join with its sides swapped.
fn swap(join: &LogicalJoin) -> LogicalJoin {
    let left_len = join.left().schema().len();
    let right_len = join.right().schema().len();
    let map = |i: usize| {
        if i < left_len {
            i + right_len
        } else {
            i - left_len
        }
    };
    let on = join.on().clone().rewrite_expr(&mut MapRefs(map));
    let output = join.output_indices().iter().map(|&i| map(i)).collect();
    LogicalJoin::with_output_indices(join.right(), join.left(), JoinType::Inner, on, output)
}

/// Pushes the join below the per-group operators of its left side.
fn push_left(join: &LogicalJoin) -> Option<PlanRef> {
    let left = join.left();
    let right = join.right();
    let inner = join.join_type() == JoinType::Inner;
    let push = Push {
        join_type: join.join_type(),
        right_types: right.schema().data_types(),
        right_restricted: is_restricted(&right),
        right_whole_table: reads_whole_table(&right),
        right,
    };
    // A group meets at most one row of the other side.
    if inner && !is_unique_on_compared_columns(join).1 {
        return None;
    }
    let sunk = push.sink(&left, join.on())?;
    let types = sunk.schema().data_types();
    let exprs = join
        .output_indices()
        .iter()
        .map(|&i| InputRef::new(i, types[i].clone()).into())
        .collect();
    Some(LogicalProject::create(sunk, exprs))
}

/// What stays the same while the join goes down its left side.
struct Push {
    /// `Inner` or `LeftSemi`.
    join_type: JoinType,
    /// The right side.
    right: PlanRef,
    right_types: Vec<DataType>,
    /// Whether the right side is restricted (see [`is_restricted`]).
    right_restricted: bool,
    /// Whether the right side reads a whole table without filter.
    right_whole_table: bool,
}

/// A per-group operator: its input, and the columns it groups by, as pairs (output column, input
/// column).
struct GroupOp {
    input: PlanRef,
    group: Vec<(usize, usize)>,
}

impl GroupOp {
    fn of(plan: &PlanRef) -> Option<GroupOp> {
        let (input, group): (PlanRef, Vec<(usize, usize)>) =
            if let Some(top_n) = plan.as_logical_top_n() {
                (
                    top_n.input(),
                    top_n.group_key().iter().map(|&c| (c, c)).collect(),
                )
            } else if let Some(dedup) = plan.as_logical_dedup() {
                (
                    dedup.input(),
                    dedup.dedup_cols().iter().map(|&c| (c, c)).collect(),
                )
            } else if let Some(over_window) = plan.as_logical_over_window() {
                // The columns that partition every window function.
                let mut common: Option<BTreeSet<usize>> = None;
                for function in over_window.window_functions() {
                    let columns: BTreeSet<usize> =
                        function.partition_by.iter().map(|i| i.index()).collect();
                    common = Some(match common {
                        None => columns,
                        Some(c) => c.intersection(&columns).copied().collect(),
                    });
                }
                (
                    over_window.input(),
                    common?.into_iter().map(|c| (c, c)).collect(),
                )
            } else {
                let agg = plan.as_logical_agg()?;
                if !agg.grouping_sets().is_empty() {
                    return None;
                }
                (agg.input(), agg.group_key().indices().enumerate().collect())
            };
        (!group.is_empty()).then_some(GroupOp { input, group })
    }

    fn input_of(&self, output: usize) -> Option<usize> {
        self.group
            .iter()
            .find(|(o, _)| *o == output)
            .map(|&(_, i)| i)
    }
}

impl Push {
    fn right_len(&self) -> usize {
        self.right_types.len()
    }

    fn inner(&self) -> bool {
        self.join_type == JoinType::Inner
    }

    /// `on`, over the columns of a left side of `left_len` columns and of the right side, over
    /// those of a new left side of `new_left_len` columns, the left columns renamed by `map`.
    fn remap(
        &self,
        on: &Condition,
        left_len: usize,
        new_left_len: usize,
        map: impl Fn(usize) -> usize,
    ) -> Condition {
        on.clone().rewrite_expr(&mut MapRefs(|i| {
            if i < left_len {
                map(i)
            } else {
                new_left_len + i - left_len
            }
        }))
    }

    /// The columns of the right side, after `offset` columns of a left side (inner join only).
    fn right_refs(&self, offset: usize) -> Vec<ExprImpl> {
        if !self.inner() {
            return vec![];
        }
        self.right_types
            .iter()
            .enumerate()
            .map(|(j, t)| InputRef::new(offset + j, t.clone()).into())
            .collect()
    }

    /// The join of `left` with the right side as it is, with all the columns of both sides.
    fn plain_join(&self, left: &PlanRef, on: &Condition) -> PlanRef {
        LogicalJoin::new(left.clone(), self.right.clone(), self.join_type, on.clone()).into()
    }

    /// The join of `left` with the right side on `on` (over the columns of `left`, then of the
    /// right side), with the columns of `left` then (inner join) of the right side, computed with
    /// the join below the per-group operators of `left`. A column of the right side that is not
    /// required may hold `NULL` instead. `None` when the join reaches no per-group operator, or
    /// should not go below it.
    fn sink(&self, left: &PlanRef, on: &Condition) -> Option<PlanRef> {
        let left_len = left.schema().len();
        let read = on.collect_input_refs(left_len + self.right_len());
        if let Some(project) = left.as_logical_project() {
            let input = project.input();
            let input_len = input.schema().len();
            // The columns that the condition reads must be columns of the input.
            let mut map = vec![0; left_len];
            for c in read.ones().filter(|&c| c < left_len) {
                map[c] = project.exprs()[c].as_input_ref()?.index();
            }
            let on = self.remap(on, left_len, input_len, |c| map[c]);
            let below = self.sink(&input, &on)?;
            let mut exprs = project.exprs().clone();
            exprs.extend(self.right_refs(input_len));
            return Some(LogicalProject::new(below, exprs).into());
        }
        if let Some(filter) = left.as_logical_filter() {
            let below = self.sink(&filter.input(), on)?;
            return Some(LogicalFilter::new(below, filter.predicate().clone()).into());
        }
        if let Some(union) = left.as_logical_union() {
            if !union.all() || union.source_col().is_some() {
                return None;
            }
            let mut pushed = false;
            let branches = union
                .inputs()
                .iter()
                .map(|branch| match self.sink(branch, on) {
                    Some(sunk) => {
                        pushed = true;
                        sunk
                    }
                    None => self.plain_join(branch, on),
                })
                .collect();
            return pushed.then(|| LogicalUnion::create(true, branches));
        }

        let op = GroupOp::of(left)?;
        let left_types = left.schema().data_types();
        // Every column of the operator that the condition reads is a group column, of a type whose
        // equal values are identical.
        for c in read.ones().filter(|&c| c < left_len) {
            if op.input_of(c).is_none() || !is_simple_type(&left_types[c]) {
                return None;
            }
        }
        // The group columns compared with `=` to a column of the right side, and for each column
        // of the right side, a group column equal to it.
        let mut equated = BTreeSet::new();
        let mut right_equal: Vec<Option<usize>> = vec![None; self.right_len()];
        for expr in &on.conjunctions {
            if let Some((a, b)) = expr
                .as_eq_cond()
                .or_else(|| expr.as_is_not_distinct_from_cond())
                && a.index() < left_len
                && b.index() >= left_len
            {
                equated.insert(a.index());
                right_equal[b.index() - left_len].get_or_insert(a.index());
            }
        }
        if equated.is_empty() {
            return None;
        }
        let is_agg = left.as_logical_agg().is_some();
        if is_agg && self.inner() && self.right_whole_table {
            return None;
        }
        let full_key = op.group.iter().all(|(o, _)| equated.contains(o));
        let input = op.input.clone();
        let input_len = input.schema().len();
        let on = self.remap(on, left_len, input_len, |c| op.input_of(c).unwrap());
        if !self.worth_pushing(&input, &on, full_key) {
            return None;
        }
        let below = self
            .sink(&input, &on)
            .unwrap_or_else(|| self.plain_join(&input, &on));
        let right_len = self.right_len();

        if let Some(agg) = left.as_logical_agg() {
            let group_len = agg.group_key().len();
            let call_len = agg.agg_calls().len();
            // The columns of the right side that no group column equals: one value per group,
            // kept by an `internal_last_seen_value` (pruned when it is not read above).
            let carried: Vec<usize> = (0..right_len)
                .filter(|&j| self.inner() && right_equal[j].is_none())
                .collect();
            let mut core = agg.core().clone();
            for &j in &carried {
                let data_type = self.right_types[j].clone();
                core.agg_calls.push(PlanAggCall {
                    agg_type: PbAggKind::InternalLastSeenValue.into(),
                    return_type: data_type.clone(),
                    inputs: vec![InputRef::new(input_len + j, data_type)],
                    distinct: false,
                    order_by: vec![],
                    filter: Condition::true_cond(),
                    direct_args: vec![],
                });
            }
            core.input = below;
            let new_agg: PlanRef = LogicalAgg::from(core).into();
            let types = new_agg.schema().data_types();
            // The output of the aggregation, then the columns of the right side.
            let mut exprs: Vec<ExprImpl> = (0..group_len + call_len)
                .map(|i| InputRef::new(i, types[i].clone()).into())
                .collect();
            if self.inner() {
                for (j, equal) in right_equal.iter().enumerate() {
                    let index = match *equal {
                        // Equal to the group column `c` (an output column of the aggregation
                        // below `group_len`), of a type whose equal values are identical.
                        Some(c) => c,
                        None => {
                            group_len + call_len + carried.iter().position(|&a| a == j).unwrap()
                        }
                    };
                    exprs.push(InputRef::new(index, types[index].clone()).into());
                }
            }
            return Some(LogicalProject::new(new_agg, exprs).into());
        }

        // A top-n, a deduplication or a window: the rows go through with their columns.
        let new_op = left.clone_root_with_inputs(&[below]);
        if left.as_logical_over_window().is_some() && self.inner() {
            // The window functions come after the columns of the right side: put them back after
            // the columns of the input.
            let types = new_op.schema().data_types();
            let exprs = (0..input_len)
                .chain(input_len + right_len..types.len())
                .chain(input_len..input_len + right_len)
                .map(|i| InputRef::new(i, types[i].clone()).into())
                .collect();
            return Some(LogicalProject::new(new_op, exprs).into());
        }
        Some(new_op)
    }

    /// Whether the join is worth computing below a per-group operator of input `input`.
    fn worth_pushing(&self, input: &PlanRef, on: &Condition, full_key: bool) -> bool {
        // A semi join keeps at most the rows that the inner join outputs: its estimate is that of
        // the inner join (the estimate of a semi join is a fixed fraction of its input).
        let joined: PlanRef = LogicalJoin::new(
            input.clone(),
            self.right.clone(),
            JoinType::Inner,
            on.clone(),
        )
        .into();
        match (estimated_rows(input), estimated_rows(&joined)) {
            (Some(rows), Some(joined_rows)) => joined_rows <= SELECTIVE_JOIN_RATIO * rows,
            _ => self.right_restricted && full_key,
        }
    }
}

/// Whether `plan` holds a restriction of the rows it reads: a filter, a scan predicate, constant
/// rows, a limit or a semi or anti join, possibly below projections and inner joins.
fn is_restricted(plan: &PlanRef) -> bool {
    if plan.as_logical_filter().is_some()
        || plan.as_logical_values().is_some()
        || plan.as_logical_limit().is_some()
    {
        return true;
    }
    if let Some(scan) = plan.as_logical_scan() {
        return !scan.predicate().always_true();
    }
    if let Some(project) = plan.as_logical_project() {
        return is_restricted(&project.input());
    }
    if let Some(join) = plan.as_logical_join() {
        return match join.join_type() {
            JoinType::Inner => is_restricted(&join.left()) || is_restricted(&join.right()),
            JoinType::LeftSemi | JoinType::RightSemi | JoinType::LeftAnti | JoinType::RightAnti => {
                true
            }
            _ => false,
        };
    }
    false
}

/// Whether `plan` reads a whole table, through projections only, without filter.
fn reads_whole_table(plan: &PlanRef) -> bool {
    let mut node = plan.clone();
    while let Some(project) = node.as_logical_project() {
        node = project.input();
    }
    node.as_logical_scan()
        .is_some_and(|scan| scan.predicate().always_true() && scan.as_of().is_none())
}
