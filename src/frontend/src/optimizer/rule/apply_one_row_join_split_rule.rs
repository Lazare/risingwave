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

use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::{LogicalApply, PlanTreeNodeUnary};

/// Splits the apply of several subqueries of a select list into one apply per subquery, when
/// every subquery returns exactly one row (an aggregation without group key).
///
/// The planner puts the subqueries of a select list under one apply, joined with `FULL OUTER JOIN
/// ON true`. When both sides of that join return exactly one row, it is their cross product, and
/// applying them one after the other gives the same row:
///
/// ```text
///   LogicalApply [left outer, on true]             LogicalApply [left outer, on true]
///   /           \                                  /                     \
/// LHS      LogicalJoin [full outer, on true]  =>  LogicalApply [...]       B
///           /         \                         /             \
///          A           B                      LHS              A
/// ```
///
/// Each apply can then be unnested alone, see `PullUpCorrelatedPredicateAggRule`. Used on demand,
/// behind `streaming_simplify_subquery_unnesting` and `batch_simplify_subquery_unnesting`.
pub struct ApplyOneRowJoinSplitRule {}

impl Rule<Logical> for ApplyOneRowJoinSplitRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        if join_type != JoinType::LeftOuter || !on.always_true() {
            return None;
        }
        let (a, b) = one_row_cross_join(&right)?;
        let inner = LogicalApply::create(
            left,
            a,
            JoinType::LeftOuter,
            on.clone(),
            correlated_id,
            correlated_indices.clone(),
            max_one_row,
        );
        Some(LogicalApply::create(
            inner,
            b,
            JoinType::LeftOuter,
            on,
            correlated_id,
            correlated_indices,
            max_one_row,
        ))
    }
}

/// The two sides of a `FULL OUTER JOIN ON true` that outputs all their columns, when both return
/// exactly one row.
fn one_row_cross_join(plan: &PlanRef) -> Option<(PlanRef, PlanRef)> {
    let join = plan.as_logical_join()?;
    let (left, right, on, join_type, output_indices) = join.clone().decompose();
    let all_columns = left.schema().len() + right.schema().len();
    if join_type != JoinType::FullOuter
        || !on.always_true()
        || output_indices != (0..all_columns).collect::<Vec<_>>()
        || !returns_exactly_one_row(&left)
        || !returns_exactly_one_row(&right)
    {
        return None;
    }
    Some((left, right))
}

/// Whether the plan returns exactly one row whatever its input: an aggregation without group key,
/// under projections, or the cross product of such plans.
fn returns_exactly_one_row(plan: &PlanRef) -> bool {
    if let Some(project) = plan.as_logical_project() {
        return returns_exactly_one_row(&project.input());
    }
    if let Some(agg) = plan.as_logical_agg() {
        return agg.group_key().is_empty() && agg.grouping_sets().is_empty();
    }
    one_row_cross_join(plan).is_some()
}

impl ApplyOneRowJoinSplitRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyOneRowJoinSplitRule {})
    }
}
