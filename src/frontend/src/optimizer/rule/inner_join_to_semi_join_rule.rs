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

//! An inner join whose one side is unique on the compared columns and is not read above becomes a
//! semi join, and a semi or anti join drops the `DISTINCT` of the side it only tests
//! (`batch_inner_join_to_semi_join`). The rules of Calcite's `SemiJoinRule` family
//! (`JoinOnUniqueToSemiJoin`, `ProjectToSemiJoin`, `JoinToSemiJoin`), for batch queries.

use risingwave_pb::plan_common::JoinType;

use super::cost_based_join_ordering_rule::estimated_rows;
use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::is_unique_on_compared_columns;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    EqJoinPredicate, LogicalJoin, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};

/// Turns an inner join into a semi join when no column of one side is in its output and that side
/// is unique on the columns that the condition binds.
///
/// ```text
/// Join(Inner, t.k = d.k, output: [t.*])          Join(LeftSemi, t.k = d.k, output: [t.*])
/// ├─Scan(t)                                 =>   ├─Scan(t)
/// └─Agg(group_key: [u.k])                        └─Agg(group_key: [u.k])
///   └─Scan(u)                                      └─Scan(u)
/// ```
///
/// - A row of the kept side matches at most one row of the other side: the condition binds a
///   unique key of the other side (`is_unique_on_compared_columns`: equalities between the two
///   sides and equalities with constants, on types whose equal values are identical). The other
///   conjuncts of the condition only remove matches. So the inner join outputs each row of the
///   kept side once if it has a match and never otherwise: exactly what the semi join outputs.
/// - No column of the other side is read above the join (its output columns all come from the
///   kept side), so the semi join, which only outputs the kept side, has the same output.
/// - The condition must hold an equality between the two sides, so that both joins are hash joins.
///
/// Used after the join ordering and the column pruning, which leave in the output of a join only
/// the columns that are read: the inner join was ordered as an inner join, and the semi join can
/// still have its sides chosen by `CostBasedOuterJoinSidesRule`. Exact: `NULL`s never match under
/// `=`, and under `IS NOT DISTINCT FROM` the unique key treats `NULL` as a value.
pub struct InnerJoinToSemiJoinRule;

impl Rule<Logical> for InnerJoinToSemiJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        if join.join_type() != JoinType::Inner {
            return None;
        }
        let left_len = join.left().schema().len();
        let right_len = join.right().schema().len();
        if !EqJoinPredicate::create(left_len, right_len, join.on().clone()).has_eq() {
            return None;
        }
        let output = join.output_indices();
        let right_is_unused = output.iter().all(|&index| index < left_len);
        let left_is_unused = output.iter().all(|&index| index >= left_len);
        if !right_is_unused && !left_is_unused {
            return None;
        }
        let (left_is_unique, right_is_unique) = is_unique_on_compared_columns(join);
        let (left, right, on, _, output_indices) = join.clone().decompose();
        if right_is_unused && right_is_unique {
            // The internal columns of a left semi join are those of its left side.
            return Some(
                LogicalJoin::with_output_indices(
                    left,
                    right,
                    JoinType::LeftSemi,
                    on,
                    output_indices,
                )
                .into(),
            );
        }
        if left_is_unused && left_is_unique {
            // The internal columns of a right semi join are those of its right side.
            let output_indices = output_indices
                .into_iter()
                .map(|index| index - left_len)
                .collect();
            return Some(
                LogicalJoin::with_output_indices(
                    left,
                    right,
                    JoinType::RightSemi,
                    on,
                    output_indices,
                )
                .into(),
            );
        }
        None
    }
}

impl InnerJoinToSemiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(InnerJoinToSemiJoinRule)
    }
}

/// An aggregation that keeps this fraction of its input rows or more is removed from below a semi
/// or anti join: the hash table of the aggregation and the build side of the join would hold
/// together more rows than the build side alone.
const MIN_GROUP_RATIO_TO_REMOVE: f64 = 0.5;

/// Removes the aggregation without aggregate calls (`DISTINCT`, `GROUP BY` without aggregates)
/// from the side that a semi or anti join only tests, when the estimates say that it removes few
/// duplicates.
///
/// ```text
/// Join(LeftSemi, t.k = u.k)                 Join(LeftSemi, t.k = u.k)
/// ├─Scan(t)                            =>   ├─Scan(t)
/// └─Agg(group_key: [u.k])                   └─Project(u.k)
///   └─Scan(u)                                 └─Scan(u)
/// ```
///
/// Exact: a semi join keeps a row of its kept side when some row of the other side satisfies the
/// condition, an anti join when none does; neither depends on how many such rows there are, and
/// the aggregation outputs each distinct combination of its group key once, with the same values
/// (grouping treats `NULL`s as equal, and the condition sees the same values). An aggregation
/// with an empty group key returns a row over an empty input, and is never removed.
///
/// Not always cheaper: the batch hash join holds every row of its build side, duplicates included,
/// where the aggregation held each group once. The aggregation is removed when the estimates of
/// the cost-based join ordering (with the learned ratios of `batch_cardinality_feedback_learning`
/// when it is on) say that it keeps at least half of its input rows; without an estimate it stays.
pub struct SemiJoinDistinctRemoveRule;

impl Rule<Logical> for SemiJoinDistinctRemoveRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let (left, right, on, join_type, output_indices) = join.clone().decompose();
        let (left, right) = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => (left, distinct_input(&right)?),
            JoinType::RightSemi | JoinType::RightAnti => (distinct_input(&left)?, right),
            _ => return None,
        };
        Some(LogicalJoin::with_output_indices(left, right, join_type, on, output_indices).into())
    }
}

impl SemiJoinDistinctRemoveRule {
    pub fn create() -> BoxedRule {
        Box::new(SemiJoinDistinctRemoveRule)
    }
}

/// The input of an aggregation without aggregate calls, projected on its group key (the same
/// columns as the output of the aggregation), when the aggregation removes few duplicates. A
/// projection of columns of the aggregation above it (a subquery that selects some of them) is
/// kept as the projection of the corresponding group key columns: a semi or anti join only tests
/// whether a row with these values exists.
fn distinct_input(plan: &PlanRef) -> Option<PlanRef> {
    let (agg_plan, columns) = match plan.as_logical_project() {
        Some(project) => (project.input(), Some(project.try_as_projection()?)),
        None => (plan.clone(), None),
    };
    let agg = agg_plan.as_logical_agg()?;
    if !agg.agg_calls().is_empty() || !agg.grouping_sets().is_empty() || agg.group_key().is_empty()
    {
        return None;
    }
    let input = agg.input();
    let groups = estimated_rows(&agg_plan)?;
    let rows = estimated_rows(&input)?;
    if groups < MIN_GROUP_RATIO_TO_REMOVE * rows {
        return None;
    }
    // The output columns of the aggregation are its group key columns.
    let group_key: Vec<usize> = agg.group_key().indices().collect();
    let columns = columns.unwrap_or_else(|| (0..group_key.len()).collect());
    Some(
        LogicalProject::with_out_col_idx(
            input,
            columns.into_iter().map(|column| group_key[column]),
        )
        .into(),
    )
}
