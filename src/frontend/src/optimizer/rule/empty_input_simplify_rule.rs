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

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalJoin, LogicalProject, LogicalValues, PlanTreeNodeBinary, PlanTreeNodeUnary,
};

/// Simplifies the operators that [`EmptyRelationPropagateRule`] leaves untouched: those that
/// still produce rows from an empty input (a `LogicalValues` without rows), or that produce no
/// rows whatever their input. Must be applied bottom-up, after `EmptyRelationPropagateRule`.
///
/// - `Join` whose optional side is empty: no row of the other side finds a match, whatever the
///   join condition.
///   - `LeftOuter` / `FullOuter` with an empty right side: a projection of the left side, with
///     `NULL` for the columns of the right side. Symmetrically for `RightOuter` / `FullOuter`
///     with an empty left side.
///   - `LeftAnti` with an empty right side: the left side. Symmetrically for `RightAnti`.
///   - `FullOuter` with both sides empty: empty.
///
///   The join and its state disappear. As-of joins are left untouched.
/// - `Agg` without group key and without grouping sets over an empty input: the single row it
///   emits, `0` for the counting aggregates and `NULL` for the others. Only for the aggregates
///   listed in [`agg_result_on_empty_input`].
/// - `Limit` / `TopN` with a limit of 0: empty.
/// - `Share` of an empty relation: empty.
///
/// The rule is only used by the streaming optimizer, on demand.
///
/// [`EmptyRelationPropagateRule`]: super::EmptyRelationPropagateRule
pub struct EmptyInputSimplifyRule;

fn is_empty(plan: &PlanRef) -> bool {
    plan.as_logical_values()
        .is_some_and(|values| values.rows().is_empty())
}

fn empty_like(plan: &PlanRef) -> PlanRef {
    LogicalValues::create(vec![], plan.schema().clone(), plan.ctx())
}

impl Rule<Logical> for EmptyInputSimplifyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if let Some(join) = plan.as_logical_join() {
            return Self::apply_join(&plan, join);
        }
        if let Some(agg) = plan.as_logical_agg() {
            return Self::apply_agg(&plan, agg);
        }
        let produces_nothing = if let Some(limit) = plan.as_logical_limit() {
            limit.limit() == 0
        } else if let Some(top_n) = plan.as_logical_top_n() {
            top_n.limit_attr().limit() == 0
        } else if let Some(share) = plan.as_logical_share() {
            is_empty(&share.input())
        } else {
            false
        };
        produces_nothing.then(|| empty_like(&plan))
    }
}

impl EmptyInputSimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(EmptyInputSimplifyRule)
    }

    fn apply_join(plan: &PlanRef, join: &LogicalJoin) -> Option<PlanRef> {
        let (left, right) = (join.left(), join.right());
        let (left_empty, right_empty) = (is_empty(&left), is_empty(&right));
        // Which side is kept, and whether the columns of the other side are part of the internal
        // columns of the join, in which case they are replaced with `NULL`.
        let (keep_left, pad) = match join.join_type() {
            JoinType::FullOuter if left_empty && right_empty => return Some(empty_like(plan)),
            JoinType::LeftOuter | JoinType::FullOuter if right_empty => (true, true),
            JoinType::RightOuter | JoinType::FullOuter if left_empty => (false, true),
            JoinType::LeftAnti if right_empty => (true, false),
            JoinType::RightAnti if left_empty => (false, false),
            _ => return None,
        };
        let left_len = left.schema().len();
        let column = |input: &PlanRef, index: usize| -> ExprImpl {
            InputRef::new(index, input.schema()[index].data_type.clone()).into()
        };
        let null = |input: &PlanRef, index: usize| -> ExprImpl {
            ExprImpl::literal_null(input.schema()[index].data_type.clone())
        };
        // The output indices refer to the internal columns of the join: the columns of the left
        // side followed by those of the right side, or only the columns of the kept side for an
        // anti join.
        let exprs: Vec<ExprImpl> = join
            .output_indices()
            .iter()
            .map(|&index| match (keep_left, pad) {
                (true, false) => column(&left, index),
                (false, false) => column(&right, index),
                (true, true) if index < left_len => column(&left, index),
                (true, true) => null(&right, index - left_len),
                (false, true) if index < left_len => null(&left, index),
                (false, true) => column(&right, index - left_len),
            })
            .collect();
        let kept = if keep_left { left } else { right };
        Some(LogicalProject::create(kept, exprs))
    }

    fn apply_agg(plan: &PlanRef, agg: &LogicalAgg) -> Option<PlanRef> {
        if !agg.group_key().is_empty() || !agg.grouping_sets().is_empty() || !is_empty(&agg.input())
        {
            return None;
        }
        // Without group key, the output columns are exactly the aggregate calls.
        let row: Vec<ExprImpl> = agg
            .agg_calls()
            .iter()
            .map(|call| agg_result_on_empty_input(&call.agg_type, &call.return_type))
            .collect::<Option<_>>()?;
        Some(LogicalValues::create(
            vec![row],
            plan.schema().clone(),
            plan.ctx(),
        ))
    }
}

/// Returns the result of an aggregate function over no rows, for the built-in aggregates whose
/// result is known: `0` for the counting ones, `NULL` for the others. The `DISTINCT`, `ORDER BY`
/// and `FILTER` clauses of a call do not change the result over no rows.
fn agg_result_on_empty_input(agg_type: &AggType, return_type: &DataType) -> Option<ExprImpl> {
    let AggType::Builtin(kind) = agg_type else {
        return None;
    };
    match kind {
        PbAggKind::Count | PbAggKind::Sum0 | PbAggKind::ApproxCountDistinct => {
            (*return_type == DataType::Int64).then(|| ExprImpl::literal_bigint(0))
        }
        PbAggKind::Sum
        | PbAggKind::Min
        | PbAggKind::Max
        | PbAggKind::Avg
        | PbAggKind::StringAgg
        | PbAggKind::ArrayAgg
        | PbAggKind::FirstValue
        | PbAggKind::LastValue
        | PbAggKind::VarPop
        | PbAggKind::VarSamp
        | PbAggKind::StddevPop
        | PbAggKind::StddevSamp
        | PbAggKind::BitAnd
        | PbAggKind::BitOr
        | PbAggKind::BitXor
        | PbAggKind::BoolAnd
        | PbAggKind::BoolOr
        | PbAggKind::JsonbAgg
        | PbAggKind::JsonbObjectAgg => Some(ExprImpl::literal_null(return_type.clone())),
        _ => None,
    }
}
