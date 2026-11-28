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

use super::ProjectMergeRule;
use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary};

/// Removes from the subquery of a correlated `EXISTS`, `NOT EXISTS`, `IN` or `NOT IN` the
/// operators that can not change its result, as PostgreSQL does in `simplify_EXISTS_query`.
///
/// ```text
/// SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.k ORDER BY u.ts DESC LIMIT 1)
///
/// Apply(LeftSemi, on: true)                     Apply(LeftSemi, on: true)
/// ├─Scan(t)                                     ├─Scan(t)
/// └─Project(1)                            =>    └─Project(1)
///   └─TopN(u.ts DESC, limit 1)                    └─Filter(u.k = t.k)  -- correlated
///     └─Project(1, u.ts)                            └─Scan(u)
///       └─Filter(u.k = t.k)  -- correlated
///         └─Scan(u)
/// ```
///
/// The planner writes such a subquery as a left semi (`EXISTS`, `IN`) or left anti (`NOT EXISTS`,
/// `NOT IN`) apply. With the limit, the simple unnesting does not apply and the general one reads
/// the outer side again for the domain and keeps a top-n per correlated value; without it,
/// `PullUpCorrelatedPredicateRule` turns the apply into a semi or anti join on the correlated
/// columns.
///
/// # What is removed
///
/// Below the right side of the apply, through projections only (never through a filter, which
/// may select among the rows a limit keeps, nor through a set-returning function):
///
/// - When the condition of the apply does not read the right side (`EXISTS`, `NOT EXISTS`): a
///   limit or a top-n with a limit of at least one row and no offset, with or without ties and
///   group key. The result is not empty iff its input is not empty.
/// - In all cases (also `IN`, `NOT IN`, whose condition compares a value of the right side): an
///   aggregation without aggregate calls and with a group key (`DISTINCT`, `GROUP BY` without
///   aggregates), replaced with the projection of its group key. It outputs the distinct rows of
///   that projection: the same set of rows, without duplicates. A semi or anti join only tells
///   whether a matching row exists, whatever the condition, so it depends on the set of rows of
///   its right side, not on their number.
///
/// Projections compute a row from a row: they keep both properties. An `ORDER BY` without a
/// limit is already dropped by the planner.
///
/// # What is not removed
///
/// - A limit of zero rows (the subquery is empty: an empty relation) or with an offset (`EXISTS
///   (... OFFSET m)` asks for more than `m` rows).
/// - A limit under `IN` / `NOT IN`: it chooses the values that are compared.
/// - An aggregation with aggregate calls, or without group key (a scalar aggregation returns one
///   row even over an empty input), or under a filter (`HAVING`).
/// - Anything below a set-returning function, a filter or any other operator, and a limit or an
///   aggregation right above a set-returning function of the select list (left as it is, as
///   PostgreSQL does, although removing the operator alone would keep the function).
/// - An uncorrelated subquery: it is not decorrelated, and its limit saves work (a batch query
///   reads one row; the join of a streaming job keeps one row in its state).
///
/// The rewrite holds for every row of the outer side and every content of the tables, so it is
/// valid for a streaming job, whose plan is frozen when it is created; duplicates and
/// retractions are covered by the set argument above.
///
/// Used on demand, behind `streaming_simplify_exists_subquery` and
/// `batch_simplify_exists_subquery`, before the simple unnesting.
pub struct ExistsSubquerySimplifyRule {}

impl Rule<Logical> for ExistsSubquerySimplifyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        if !matches!(apply.join_type(), JoinType::LeftSemi | JoinType::LeftAnti)
            || apply.max_one_row()
            || apply.correlated_indices().is_empty()
        {
            return None;
        }
        let left_len = apply.left().schema().len();
        let right = apply.right();
        let right_len = right.schema().len();
        let existence_only = apply
            .on_condition()
            .collect_input_refs(left_len + right_len)
            .ones()
            .all(|index| index < left_len);
        let simplified = simplify(&right, existence_only)?;
        Some(apply.clone_with_left_right(apply.left(), simplified).into())
    }
}

impl ExistsSubquerySimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(ExistsSubquerySimplifyRule {})
    }
}

/// The plan without the operators that can not change the result of a semi or anti join whose
/// right side it is (see [`ExistsSubquerySimplifyRule`]), or `None` if nothing is removed.
/// `existence_only` tells that only whether the plan returns a row matters.
fn simplify(plan: &PlanRef, existence_only: bool) -> Option<PlanRef> {
    if let Some(project) = plan.as_logical_project() {
        let input = simplify(&project.input(), existence_only)?;
        return Some(project_onto(input, project.exprs().clone()));
    }
    if existence_only && let Some(limit) = plan.as_logical_limit() {
        let input = limit.input();
        if limit.limit() == 0 || limit.offset() != 0 || has_set_returning_select_list(&input) {
            return None;
        }
        return Some(simplify(&input, existence_only).unwrap_or(input));
    }
    if existence_only && let Some(top_n) = plan.as_logical_top_n() {
        let input = top_n.input();
        if top_n.limit_attr().limit() == 0
            || top_n.offset() != 0
            || has_set_returning_select_list(&input)
        {
            return None;
        }
        return Some(simplify(&input, existence_only).unwrap_or(input));
    }
    let agg = plan.as_logical_agg()?;
    let input = agg.input();
    if !agg.agg_calls().is_empty()
        || agg.group_key().is_empty()
        || !agg.grouping_sets().is_empty()
        || has_set_returning_select_list(&input)
    {
        return None;
    }
    let group_key: Vec<ExprImpl> = agg
        .group_key()
        .indices()
        .map(|index| InputRef::new(index, input.schema()[index].data_type()).into())
        .collect();
    let input = simplify(&input, existence_only).unwrap_or(input);
    Some(project_onto(input, group_key))
}

/// Whether the plan is a set-returning function of its input, under projections only: the select
/// list of the subquery has one. Such a subquery is left as it is, as PostgreSQL does.
fn has_set_returning_select_list(plan: &PlanRef) -> bool {
    if let Some(project) = plan.as_logical_project() {
        return has_set_returning_select_list(&project.input());
    }
    plan.as_logical_project_set().is_some()
}

/// A projection of `exprs` over `input`, merged with `input` if it is a projection too, so that
/// `PullUpCorrelatedPredicateRule` finds the filter right below it. Not merged when that would
/// evaluate an impure expression, or a function call, several times.
fn project_onto(input: PlanRef, exprs: Vec<ExprImpl>) -> PlanRef {
    if let Some(inner) = input.as_logical_project()
        && inner.exprs().iter().all(ExprImpl::is_pure)
        && let Some(merged) = ProjectMergeRule::merge_project_exprs(&exprs, inner.exprs(), true)
    {
        return LogicalProject::create(inner.input(), merged);
    }
    LogicalProject::create(input, exprs)
}
