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

//! An aggregation over a join computed on one side of the join, before the join (Yan and Larson,
//! "Eager Aggregation and Lazy Aggregation", VLDB 1995; Calcite `AggregateJoinTransposeRule`;
//! PostgreSQL eager aggregation). Only used on demand: `streaming_push_agg_below_join` and
//! `batch_push_agg_below_join` for the exact form, `batch_partial_agg_below_join` for the partial
//! form.

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::never_true_predicate_rule::is_simple_type;
use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::{is_unique_on_compared_columns, unique_keys_of};
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::cardinality_learning::{
    CARDINALITY_LEARNING_STORE, RatioKind, learned_ratio,
};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalJoin, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::{Condition, IndexSet, Substitute};

/// The largest group ratio (groups per input row, the largest value learned by
/// `batch_cardinality_feedback_learning`) of a partial aggregation below an outer join or into a
/// branch of a `UNION ALL`: the partial aggregation must at least halve the rows that go on.
pub(super) const PARTIAL_AGG_MAX_GROUP_RATIO: f64 = 0.5;

/// The same below an inner join, which may drop most rows of the aggregated side: the partial
/// aggregation then reads rows that the join would have dropped, so it must reduce them more.
const PARTIAL_AGG_MAX_GROUP_RATIO_INNER: f64 = 0.1;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Exact form, in the plan of a streaming job.
    ExactStream,
    /// Exact form, in a batch query.
    ExactBatch,
    /// Partial form, in a batch query, chosen with the learned group ratios.
    PartialBatch,
}

/// Computes an aggregation over a join on one side of the join (the aggregated side), before the
/// join, when every row of that side meets the same rows of the other side as all the rows of its
/// group.
///
/// Exact form (`streaming_push_agg_below_join`, `batch_push_agg_below_join`):
///
/// ```text
/// Agg(group f.k, d.name: sum(f.x), count(*))       Project(f.k, d.name, s, n)
/// └─Join(LeftOuter, f.k = d.id)              =>    └─Join(LeftOuter, f.k = d.id)
///   ├─f                                              ├─Agg(group f.k: s = sum(f.x), n = count(*))
///   └─d, with the key (id)                           │ └─f
///                                                    └─d
/// ```
///
/// Partial form (`batch_partial_agg_below_join`): the other side is not unique; the pushed
/// aggregation computes partial results that an aggregation above the join finishes (`sum` of
/// `sum`, `sum0` of `count`, `min` of `min`...):
///
/// ```text
/// Agg(group f.k: sum(f.x), count(*))               Agg(group f.k: sum(s), sum0(n))
/// └─Join(Inner, f.k = e.k)                   =>    └─Join(Inner, f.k = e.k)
///   ├─f                                              ├─Agg(group f.k: s = sum(f.x), n = count(*))
///   └─e                                              │ └─f
///                                                    └─e
/// ```
///
/// Conditions, with `A` the aggregated side and `B` the other one:
///
/// - The join is inner, or outer with `A` as its preserved side (an unmatched row of `A` is then
///   in the output once, with `NULL`s for `B`, before and after). Not full, semi, anti or as-of
///   joins, and no temporal join.
/// - The columns of `A` that the condition reads are in the new group key (see below). A conjunct
///   that reads a column of `A` otherwise than in `a = b` or `a IS NOT DISTINCT FROM b` with a column
///   of `B` must read columns of simple types (two equal values are identical) that are group key
///   columns of the aggregation. Then all the rows of a new group meet the same rows of `B`.
/// - Every group key of the aggregation is a deterministic expression of the columns of one side.
///   The new group key is the group keys of `A` and the join columns of `A`.
/// - Every aggregate reads columns of `A` only (arguments, `FILTER`, `ORDER BY`), or is `min`,
///   `max` or `internal_last_seen_value` of a column of `B` without `FILTER` or `ORDER BY`: the
///   value of `B` is the same for every row of a new group (exact form), or repeated rows do not
///   change these aggregates (partial form).
/// - The new aggregation reduces something: `A` has no unique key within the new group key.
///
/// Exact form, also:
///
/// - `B` is unique on the columns that the condition binds (`is_unique_on_compared_columns`): a
///   row of `A` meets at most one row of `B`, so the rows of `B` (and the group keys taken from
///   them) are the same for all the rows of a new group.
/// - The groups do not change: every join column of `A` is a group key column of the aggregation,
///   or (inner join only) is compared with `=` or `IS NOT DISTINCT FROM` to a column of `B` that
///   is one; a group then is exactly one new group, with the same rows of `A`. An inner join drops
///   a whole new group or keeps it whole, so aggregating before or after the join is the same, for
///   every aggregate (including `DISTINCT`, ordered or user-defined ones).
/// - The group key is not empty: without group key, an empty input still gives one row, which an
///   inner join above the new aggregation could drop.
/// - Batch only: the join is outer, or `B` reads a whole table without filter. An inner join with
///   a filtered `B` may drop most rows of `A`: aggregating `A` first would then read and group rows
///   that the join drops, and is left to the plan as written.
/// - Streaming only: no side reads a source or a table with a watermark, so that the aggregation of
///   a job that emits on window close is never moved below a join.
///
/// Partial form, also:
///
/// - Every aggregate of `A` decomposes (see [`total_of_partial`]); the partial aggregate is the
///   same aggregate over the rows of `A`, with its `FILTER`, and the final one combines them with
///   the same result type (`sum(int)` is `bigint` in both, `count` finished by `sum0`). A row of
///   the new aggregation stands for all the rows of its new group, which meet the same rows of `B`
///   and fall into the same groups above: summing its partial sum once per row of `B` it meets is
///   summing each of its rows once per row of `B`.
/// - The new group key is not empty.
/// - Chosen by cost with the learned ratios (`batch_cardinality_feedback_learning`): the largest
///   group ratio learned for the new aggregation is at most [`PARTIAL_AGG_MAX_GROUP_RATIO`] below
///   an outer join, [`PARTIAL_AGG_MAX_GROUP_RATIO_INNER`] below an inner join. Nothing learned,
///   nothing pushed.
///
/// Expressions computed in projections between the aggregation and the join are computed on the
/// rows of `A` before the join: below an inner join, on rows that the join may drop (as
/// PostgreSQL's eager aggregation does), so an error of such an expression on a row without match
/// is raised. They must be deterministic.
pub struct AggJoinTransposeRule {
    mode: Mode,
}

impl AggJoinTransposeRule {
    pub fn create_for_stream() -> BoxedRule {
        Box::new(AggJoinTransposeRule {
            mode: Mode::ExactStream,
        })
    }

    pub fn create_for_batch() -> BoxedRule {
        Box::new(AggJoinTransposeRule {
            mode: Mode::ExactBatch,
        })
    }

    pub fn create_partial_for_batch() -> BoxedRule {
        Box::new(AggJoinTransposeRule {
            mode: Mode::PartialBatch,
        })
    }
}

impl Rule<Logical> for AggJoinTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        if !agg.grouping_sets().is_empty() {
            return None;
        }
        if self.mode != Mode::PartialBatch && agg.group_key().is_empty() {
            return None;
        }
        let (join_plan, to_join) = through_projects(agg.input());
        let join = join_plan.as_logical_join()?;
        let (left, right) = (join.left(), join.right());
        if join.should_be_temporal_join() || is_as_of_scan(&left) || is_as_of_scan(&right) {
            return None;
        }
        if self.mode == Mode::ExactStream && reads_watermarked_input(&join_plan) {
            return None;
        }
        let internal_types: Vec<DataType> = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields())
            .map(|field| field.data_type())
            .collect();
        // The input columns of the aggregation as expressions of the internal columns of the join.
        let mut to_internal_columns = Substitute {
            mapping: join
                .output_indices()
                .iter()
                .map(|&index| InputRef::new(index, internal_types[index].clone()).into())
                .collect(),
        };
        let to_internal: Vec<ExprImpl> = to_join
            .into_iter()
            .map(|expr| to_internal_columns.rewrite_expr(expr))
            .collect();
        let sides: &[bool] = match join.join_type() {
            JoinType::Inner => &[true, false],
            JoinType::LeftOuter => &[true],
            JoinType::RightOuter => &[false],
            _ => return None,
        };
        sides.iter().find_map(|&agg_left| {
            Pushdown {
                mode: self.mode,
                agg,
                join,
                internal_types: &internal_types,
                to_internal: &to_internal,
                agg_left,
            }
            .apply()
        })
    }
}

/// One attempt of [`AggJoinTransposeRule`], with the aggregated side on the left (`agg_left`) or
/// on the right of the join.
struct Pushdown<'a> {
    mode: Mode,
    agg: &'a LogicalAgg,
    join: &'a LogicalJoin,
    internal_types: &'a [DataType],
    /// The input columns of the aggregation as expressions of the internal columns of the join.
    to_internal: &'a [ExprImpl],
    agg_left: bool,
}

/// Where a group key or an aggregate of the aggregation is computed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    /// From the columns of the aggregated side, by the new aggregation.
    Agg,
    /// From the columns of the other side, above the join.
    Other,
}

impl Pushdown<'_> {
    fn apply(&self) -> Option<PlanRef> {
        let join = self.join;
        let (left, right) = (join.left(), join.right());
        let left_len = left.schema().len();
        let internal_len = self.internal_types.len();
        let (side_a, side_b) = if self.agg_left {
            (left.clone(), right.clone())
        } else {
            (right.clone(), left.clone())
        };
        let a_range: Range<usize> = if self.agg_left {
            0..left_len
        } else {
            left_len..internal_len
        };
        let exact = self.mode != Mode::PartialBatch;
        let inner = join.join_type() == JoinType::Inner;
        let refs = |expr: &ExprImpl| -> Vec<usize> {
            expr.collect_input_refs(internal_len).ones().collect()
        };
        let in_a = |expr: &ExprImpl| refs(expr).iter().all(|index| a_range.contains(index));
        let in_b = |expr: &ExprImpl| refs(expr).iter().all(|index| !a_range.contains(index));

        if exact {
            let (left_unique, right_unique) = is_unique_on_compared_columns(join);
            let other_unique = if self.agg_left {
                right_unique
            } else {
                left_unique
            };
            if !other_unique {
                return None;
            }
            if self.mode == Mode::ExactBatch && inner && !reads_whole_table(&side_b) {
                return None;
            }
        }

        // The columns of the aggregated side that the condition reads (internal indices), the
        // columns of the other side they are compared with by `=` or `IS NOT DISTINCT FROM`, and
        // those read otherwise, which must be group key columns themselves.
        let mut join_columns = BTreeSet::new();
        let mut partners: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut read_otherwise = BTreeSet::new();
        for conjunct in &join.on().conjunctions {
            let a_refs: Vec<usize> = refs(conjunct)
                .into_iter()
                .filter(|index| a_range.contains(index))
                .collect();
            if a_refs.is_empty() {
                continue;
            }
            if !conjunct.is_pure() {
                return None;
            }
            if let Some((x, y)) = conjunct
                .as_eq_cond()
                .or_else(|| conjunct.as_is_not_distinct_from_cond())
            {
                let (a, b) = if a_range.contains(&x.index()) {
                    (x, y)
                } else {
                    (y, x)
                };
                if a_range.contains(&a.index()) && !a_range.contains(&b.index()) {
                    join_columns.insert(a.index());
                    partners.entry(a.index()).or_default().push(b.index());
                    continue;
                }
            }
            for index in a_refs {
                if !is_simple_type(&self.internal_types[index]) {
                    return None;
                }
                join_columns.insert(index);
                read_otherwise.insert(index);
            }
        }

        // The side of each group key, in the order of the output of the aggregation.
        let group_indices: Vec<usize> = self.agg.group_key().indices().collect();
        let mut group_sides = Vec::with_capacity(group_indices.len());
        for &index in &group_indices {
            let expr = &self.to_internal[index];
            if !expr.is_pure() {
                return None;
            }
            group_sides.push(if in_a(expr) {
                Side::Agg
            } else if in_b(expr) {
                Side::Other
            } else {
                return None;
            });
        }
        // The internal columns that are group keys as they are.
        let grouped_columns: BTreeSet<usize> = group_indices
            .iter()
            .filter_map(|&index| self.to_internal[index].as_input_ref().map(|r| r.index()))
            .collect();
        if exact {
            for column in &join_columns {
                let determined = grouped_columns.contains(column)
                    || (inner
                        && !read_otherwise.contains(column)
                        && partners.get(column).is_some_and(|partners| {
                            partners
                                .iter()
                                .any(|partner| grouped_columns.contains(partner))
                        }));
                if !determined {
                    return None;
                }
            }
        }

        // The side of each aggregate; the final aggregate of each pushed one (partial form).
        let agg_input_len = self.to_internal.len();
        let mut call_sides = Vec::with_capacity(self.agg.agg_calls().len());
        let mut totals = Vec::with_capacity(self.agg.agg_calls().len());
        for call in self.agg.agg_calls() {
            let columns = call_columns(call, agg_input_len);
            if columns
                .iter()
                .any(|&column| !self.to_internal[column].is_pure())
            {
                return None;
            }
            if columns
                .iter()
                .all(|&column| in_a(&self.to_internal[column]))
            {
                call_sides.push(Side::Agg);
                totals.push(if exact {
                    None
                } else {
                    Some(total_of_partial(call)?)
                });
            } else if is_value_call(call) && in_b(&self.to_internal[call.inputs[0].index()]) {
                call_sides.push(Side::Other);
                totals.push(None);
            } else {
                return None;
            }
        }

        // The input of the new aggregation: a projection of the aggregated side. Its group key
        // columns first, so that they are the first columns of its output, in that order.
        let a_offset = a_range.start;
        let mut to_a_columns = Substitute {
            mapping: (0..internal_len)
                .map(|index| {
                    let local = if a_range.contains(&index) {
                        index - a_offset
                    } else {
                        index
                    };
                    InputRef::new(local, self.internal_types[index].clone()).into()
                })
                .collect(),
        };
        let mut pa_exprs: Vec<ExprImpl> = vec![];
        // Input column of the aggregation, or internal column of the join, to column of the
        // projection.
        let mut pa_of_agg_input: HashMap<usize, usize> = HashMap::new();
        let mut pa_of_internal: HashMap<usize, usize> = HashMap::new();
        for (&index, side) in group_indices.iter().zip(&group_sides) {
            if *side != Side::Agg {
                continue;
            }
            let expr = &self.to_internal[index];
            let position = push(&mut pa_exprs, to_a_columns.rewrite_expr(expr.clone()));
            pa_of_agg_input.insert(index, position);
            if let Some(input_ref) = expr.as_input_ref() {
                pa_of_internal.entry(input_ref.index()).or_insert(position);
            }
        }
        for &column in &join_columns {
            if !pa_of_internal.contains_key(&column) {
                let expr = InputRef::new(column - a_offset, self.internal_types[column].clone());
                let position = push(&mut pa_exprs, expr.into());
                pa_of_internal.insert(column, position);
            }
        }
        let new_group_len = pa_exprs.len();
        if new_group_len == 0 {
            return None;
        }
        for (call, side) in self.agg.agg_calls().iter().zip(&call_sides) {
            if *side != Side::Agg {
                continue;
            }
            for column in call_columns(call, agg_input_len) {
                if !pa_of_agg_input.contains_key(&column) {
                    let expr = to_a_columns.rewrite_expr(self.to_internal[column].clone());
                    let position = push(&mut pa_exprs, expr);
                    pa_of_agg_input.insert(column, position);
                }
            }
        }

        // The new aggregation reduces nothing if the aggregated side is unique on its group key.
        let grouped_a_columns: BTreeSet<usize> = pa_exprs[..new_group_len]
            .iter()
            .filter_map(|expr| expr.as_input_ref().map(|r| r.index()))
            .collect();
        if unique_keys_of(&side_a)
            .iter()
            .any(|key| key.is_subset(&grouped_a_columns))
        {
            return None;
        }

        let pa = LogicalProject::create(side_a, pa_exprs);
        let pushed_calls: Vec<PlanAggCall> = self
            .agg
            .agg_calls()
            .iter()
            .zip(&call_sides)
            .filter(|(_, side)| **side == Side::Agg)
            .map(|(call, _)| remap_call(call, &pa_of_agg_input))
            .collect();
        let pushed_len = pushed_calls.len();
        let new_agg: PlanRef =
            Agg::new(pushed_calls, (0..new_group_len).collect::<IndexSet>(), pa).into();

        if self.mode == Mode::PartialBatch {
            let ctx = new_agg.ctx();
            if !ctx
                .session_ctx()
                .config()
                .batch_cardinality_feedback_learning()
            {
                return None;
            }
            let largest = if inner {
                PARTIAL_AGG_MAX_GROUP_RATIO_INNER
            } else {
                PARTIAL_AGG_MAX_GROUP_RATIO
            };
            let learned = learned_ratio(
                &**CARDINALITY_LEARNING_STORE,
                RatioKind::GroupRatio,
                &new_agg,
            )?;
            if learned.max > largest {
                return None;
            }
        }

        // The new join, with the new aggregation in place of the aggregated side.
        let agg_len = new_group_len + pushed_len;
        let (a_new_offset, b_offset, b_new_offset) = if self.agg_left {
            (0, left_len, agg_len)
        } else {
            (left_len, 0, 0)
        };
        let mut to_new_internal = Substitute {
            mapping: (0..internal_len)
                .map(|index| {
                    let new_index = if a_range.contains(&index) {
                        // Only the join columns are read by the condition; the others keep a
                        // placeholder that is never read.
                        pa_of_internal
                            .get(&index)
                            .map_or(index, |position| a_new_offset + position)
                    } else {
                        b_new_offset + index - b_offset
                    };
                    InputRef::new(new_index, self.internal_types[index].clone()).into()
                })
                .collect(),
        };
        let on = join.on().clone().rewrite_expr(&mut to_new_internal);
        let new_join: PlanRef = if self.agg_left {
            LogicalJoin::new(new_agg, side_b, join.join_type(), on).into()
        } else {
            LogicalJoin::new(side_b, new_agg, join.join_type(), on).into()
        };

        // The group keys, then the aggregates (or what the final aggregation reads), over the new
        // join.
        let mut exprs: Vec<ExprImpl> = vec![];
        for (&index, side) in group_indices.iter().zip(&group_sides) {
            exprs.push(match side {
                Side::Agg => InputRef::new(
                    a_new_offset + pa_of_agg_input[&index],
                    self.to_internal[index].return_type(),
                )
                .into(),
                Side::Other => to_new_internal.rewrite_expr(self.to_internal[index].clone()),
            });
        }
        let mut pushed_position = 0;
        for (call, side) in self.agg.agg_calls().iter().zip(&call_sides) {
            exprs.push(match side {
                Side::Agg => {
                    let position = a_new_offset + new_group_len + pushed_position;
                    pushed_position += 1;
                    InputRef::new(position, call.return_type.clone()).into()
                }
                Side::Other => {
                    let value = to_new_internal
                        .rewrite_expr(self.to_internal[call.inputs[0].index()].clone());
                    if exact && value.return_type() != call.return_type {
                        value.cast_explicit(&call.return_type).ok()?
                    } else {
                        value
                    }
                }
            });
        }
        let project = LogicalProject::create(new_join, exprs);
        if exact {
            return Some(project);
        }

        // Partial form: the final aggregation over the projection above.
        let group_len = group_indices.len();
        let final_calls = self
            .agg
            .agg_calls()
            .iter()
            .zip(&call_sides)
            .zip(totals)
            .enumerate()
            .map(|(position, ((call, side), total))| {
                let column = group_len + position;
                match side {
                    Side::Agg => {
                        total_call(call, total.expect("a pushed aggregate decomposes"), column)
                    }
                    Side::Other => PlanAggCall {
                        inputs: vec![InputRef::new(column, call.inputs[0].return_type())],
                        ..call.clone()
                    },
                }
            })
            .collect();
        Some(Agg::new(final_calls, (0..group_len).collect::<IndexSet>(), project).into())
    }
}

/// The position of `expr` in `exprs`, where it is appended if it is not there yet.
fn push(exprs: &mut Vec<ExprImpl>, expr: ExprImpl) -> usize {
    if let Some(position) = exprs.iter().position(|e| *e == expr) {
        position
    } else {
        exprs.push(expr);
        exprs.len() - 1
    }
}

/// The final aggregate that combines the results of the partial aggregate `call`, read from the
/// column `column` of its input.
pub(super) fn total_call(call: &PlanAggCall, total: AggType, column: usize) -> PlanAggCall {
    PlanAggCall {
        agg_type: total,
        return_type: call.return_type.clone(),
        inputs: vec![InputRef::new(column, call.return_type.clone())],
        distinct: false,
        order_by: vec![],
        filter: Condition::true_cond(),
        direct_args: vec![],
    }
}

/// The aggregate that combines the partial results of `call` (the same aggregate over subsets of
/// the rows of a group, with its `FILTER`) into its result over the group, with the same result
/// type; `None` if `call` does not decompose:
///
/// - `count` (not `DISTINCT`): `sum0` of the counts, `0` without rows like `count`;
/// - `sum` (not `DISTINCT`, not of a float, whose rounding depends on the order of the
///   additions): `sum` of the sums in the type of the sum (`sum(int)` is a `bigint` at both levels);
///   `NULL` parts (no row, or only `NULL`s) are ignored, and a group without value is `NULL`;
/// - `min`, `max`, `bool_and`, `bool_or`, `bit_and`, `bit_or`, `DISTINCT` or not: themselves;
/// - `internal_last_seen_value` (a column that the group key determines): itself.
///
/// Not ordered (`ORDER BY`), ordered-set, distinct `count` or `sum`, `bit_xor`, `first_value`,
/// `last_value`, `string_agg`, `array_agg`, user-defined aggregates...
pub(super) fn total_of_partial(call: &PlanAggCall) -> Option<AggType> {
    let AggType::Builtin(kind) = &call.agg_type else {
        return None;
    };
    if !call.order_by.is_empty() || !call.direct_args.is_empty() {
        return None;
    }
    match kind {
        PbAggKind::Count if !call.distinct && call.return_type == DataType::Int64 => {
            Some(PbAggKind::Sum0.into())
        }
        PbAggKind::Sum
            if !call.distinct
                && !matches!(call.return_type, DataType::Float32 | DataType::Float64) =>
        {
            Some(PbAggKind::Sum.into())
        }
        PbAggKind::Min
        | PbAggKind::Max
        | PbAggKind::BoolAnd
        | PbAggKind::BoolOr
        | PbAggKind::BitAnd
        | PbAggKind::BitOr => Some((*kind).into()),
        PbAggKind::InternalLastSeenValue if !call.distinct => Some((*kind).into()),
        _ => None,
    }
}

/// Whether `call` is `min`, `max` or `internal_last_seen_value` of one column, without `FILTER` or
/// `ORDER BY`: its result is that column when the column holds one value in the group, and it
/// does not depend on how many times each row is repeated.
fn is_value_call(call: &PlanAggCall) -> bool {
    matches!(
        call.agg_type,
        AggType::Builtin(PbAggKind::Min | PbAggKind::Max | PbAggKind::InternalLastSeenValue)
    ) && call.inputs.len() == 1
        && call.filter.always_true()
        && call.order_by.is_empty()
        && call.direct_args.is_empty()
}

/// The input columns that an aggregate reads: its arguments, `ORDER BY` and `FILTER`.
fn call_columns(call: &PlanAggCall, input_len: usize) -> BTreeSet<usize> {
    let mut columns: BTreeSet<usize> = call.inputs.iter().map(|input| input.index()).collect();
    columns.extend(call.order_by.iter().map(|order| order.column_index));
    for conjunct in &call.filter.conjunctions {
        columns.extend(conjunct.collect_input_refs(input_len).ones());
    }
    columns
}

/// Replaces the input columns of an expression by the columns of a mapping.
struct Remap<'a>(&'a HashMap<usize, usize>);

impl ExprRewriter for Remap<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(self.0[&input_ref.index()], input_ref.return_type()).into()
    }
}

/// The aggregate `call` over the columns given by `mapping`.
fn remap_call(call: &PlanAggCall, mapping: &HashMap<usize, usize>) -> PlanAggCall {
    let mut call = call.clone();
    call.inputs = call
        .inputs
        .iter()
        .map(|input| InputRef::new(mapping[&input.index()], input.return_type()))
        .collect();
    for order in &mut call.order_by {
        order.column_index = mapping[&order.column_index];
    }
    call.filter = call.filter.rewrite_expr(&mut Remap(mapping));
    call
}

/// The node below the projections at the top of `plan`, and each column of `plan` as an
/// expression of the columns of that node.
pub(super) fn through_projects(plan: PlanRef) -> (PlanRef, Vec<ExprImpl>) {
    let mut exprs: Vec<ExprImpl> = (0..plan.schema().len())
        .map(|index| InputRef::new(index, plan.schema()[index].data_type()).into())
        .collect();
    let mut node = plan;
    while let Some(project) = node.as_logical_project() {
        let mut substitute = Substitute {
            mapping: project.exprs().clone(),
        };
        exprs = exprs
            .into_iter()
            .map(|expr| substitute.rewrite_expr(expr))
            .collect();
        let next = project.input();
        node = next;
    }
    (node, exprs)
}

/// Whether `plan` is a scan in the past (`FOR SYSTEM_TIME AS OF`), as read by a temporal join.
fn is_as_of_scan(plan: &PlanRef) -> bool {
    plan.as_logical_scan()
        .is_some_and(|scan| scan.as_of().is_some())
}

/// Whether `plan` reads a whole table, through projections only, without filter.
fn reads_whole_table(plan: &PlanRef) -> bool {
    let (node, _) = through_projects(plan.clone());
    node.as_logical_scan()
        .is_some_and(|scan| scan.predicate().always_true() && scan.as_of().is_none())
}

/// Whether `plan` reads a source, or a table with a watermark.
fn reads_watermarked_input(plan: &PlanRef) -> bool {
    if plan.as_logical_source().is_some() {
        return true;
    }
    if let Some(scan) = plan.as_logical_scan() {
        return scan.table().watermark_columns.count_ones(..) > 0;
    }
    plan.inputs().iter().any(reads_watermarked_input)
}
