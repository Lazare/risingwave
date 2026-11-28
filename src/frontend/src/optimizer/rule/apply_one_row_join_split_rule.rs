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
use super::{ApplyResult, FallibleRule};
use crate::expr::CorrelatedId;
use crate::optimizer::plan_node::{LogicalApply, PlanTreeNodeUnary};
use crate::utils::Condition;

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
///
/// An apply that is not unnested alone goes through the general unnesting, which joins its own
/// copy of the domain (the distinct values of the outer columns, read from the outer side). One
/// apply for all the subqueries joins the domain once; split, each subquery joins it again, and
/// the outer side of each apply contains the applies before it. A streaming job shares these
/// copies; a batch query copies them (the plan is a tree). So the batch variant
/// ([`ApplyOneRowJoinSplitRule::create_for_batch`]) splits only when the rule that follows it in
/// its stage (`companion`) unnests alone, without a domain, every subquery but at most one: that
/// one joins the only domain, as the apply of all the subqueries does.
pub struct ApplyOneRowJoinSplitRule {
    companion: Option<BoxedRule>,
}

impl Rule<Logical> for ApplyOneRowJoinSplitRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        if join_type != JoinType::LeftOuter || !on.always_true() {
            return None;
        }
        let (a, b) = one_row_cross_join(&right)?;
        if let Some(companion) = &self.companion {
            let not_unnested_alone = |sub: &PlanRef| {
                count_not_unnested_alone(
                    companion,
                    &left,
                    sub,
                    &on,
                    correlated_id,
                    &correlated_indices,
                    max_one_row,
                )
            };
            if not_unnested_alone(&a) + not_unnested_alone(&b) > 1 {
                return None;
            }
        }
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
        Box::new(ApplyOneRowJoinSplitRule { companion: None })
    }

    /// The variant for batch queries: splits only when `companion` unnests the apply of every
    /// subquery but at most one alone.
    pub fn create_for_batch(companion: BoxedRule) -> BoxedRule {
        Box::new(ApplyOneRowJoinSplitRule {
            companion: Some(companion),
        })
    }
}

/// The number of one-row subqueries of `sub` (the sides of its `FULL OUTER JOIN ON true`,
/// recursively) whose apply over `left` `companion` does not unnest alone.
fn count_not_unnested_alone(
    companion: &BoxedRule,
    left: &PlanRef,
    sub: &PlanRef,
    on: &Condition,
    correlated_id: CorrelatedId,
    correlated_indices: &[usize],
    max_one_row: bool,
) -> usize {
    if let Some((a, b)) = one_row_cross_join(sub) {
        return [a, b]
            .iter()
            .map(|side| {
                count_not_unnested_alone(
                    companion,
                    left,
                    side,
                    on,
                    correlated_id,
                    correlated_indices,
                    max_one_row,
                )
            })
            .sum();
    }
    let apply = LogicalApply::create(
        left.clone(),
        sub.clone(),
        JoinType::LeftOuter,
        on.clone(),
        correlated_id,
        correlated_indices.to_vec(),
        max_one_row,
    );
    match FallibleRule::apply(companion.as_ref(), apply) {
        ApplyResult::Ok(_) => 0,
        _ => 1,
    }
}
