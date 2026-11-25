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

//! Removes, below an operator that does not see duplicates, the left outer joins whose right
//! side is not used.
//!
//! A left outer join keeps every row of its left side, once or more: when none of its right
//! columns is used, it only repeats rows. `UnusedOuterJoinEliminateRule` removes it when the
//! right side is unique on the compared columns. Without that, the repeated rows are still
//! invisible to an operator that only depends on the *set* of the rows of its input:
//!
//! - an aggregation whose calls are `min`, `max`, `bool_and`, `bool_or` or `DISTINCT` calls (and
//!   a `DISTINCT` itself, an aggregation without calls);
//! - a top-1 per group, when the rows of a group that tie on the order are equal on every column:
//!   every column is in the group key or the order, or holds the same constant in every row. The
//!   join changes the stream key of the rows, which breaks the ties of the top-1: only equal rows
//!   may tie, so which of them is kept does not matter;
//! - the right side of a left semi or anti join, which only asks whether a row matches.
//!
//! The joins are looked for below the operator through projections, filters, unions, inner and
//! left outer joins, and the left side of semi and anti joins: none of them turns a repeated row
//! into a different one, so the set of the rows that the operator receives is unchanged. A shared
//! sub-plan is not entered: its other consumers may see the duplicates.
//!
//! Only used by the streaming optimizer, on demand (`streaming_merge_lookups_under_dedup`).

use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::column_facts_simplify_rule::column_facts;
use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::{GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalJoin, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};

pub struct UnusedJoinUnderDedupRule;

impl UnusedJoinUnderDedupRule {
    pub fn create() -> BoxedRule {
        Box::new(UnusedJoinUnderDedupRule)
    }
}

impl Rule<Logical> for UnusedJoinUnderDedupRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if let Some(agg) = plan.as_logical_agg() {
            if !agg.grouping_sets().is_empty() || !agg.agg_calls().iter().all(sees_a_set) {
                return None;
            }
            let input = strip(&agg.input())?;
            return Some(agg.clone_with_input(input).into());
        }
        if let Some(top_n) = plan.as_logical_top_n() {
            let limit = top_n.limit_attr();
            if limit.limit() != 1 || limit.with_ties() || top_n.offset() != 0 {
                return None;
            }
            let input = top_n.input();
            let constants = column_facts(&input).constant_columns();
            let is_determined = (0..input.schema().len()).all(|column| {
                top_n.group_key().contains(&column)
                    || top_n
                        .topn_order()
                        .column_orders
                        .iter()
                        .any(|order| order.column_index == column)
                    || constants.contains(&column)
            });
            if !is_determined {
                return None;
            }
            let input = strip(&input)?;
            return Some(top_n.clone_with_input(input).into());
        }
        let join = plan.as_logical_join()?;
        if !matches!(join.join_type(), JoinType::LeftSemi | JoinType::LeftAnti) {
            return None;
        }
        let right = strip(&join.right())?;
        Some(join.clone_with_left_right(join.left(), right).into())
    }
}

/// Whether an aggregate call only depends on the set of the rows of its input.
fn sees_a_set(call: &PlanAggCall) -> bool {
    call.distinct
        || matches!(
            call.agg_type,
            AggType::Builtin(
                PbAggKind::Min | PbAggKind::Max | PbAggKind::BoolAnd | PbAggKind::BoolOr
            )
        )
}

/// The plan without the left outer joins whose right side is not used, with the same columns;
/// `None` when there is none.
fn strip(plan: &PlanRef) -> Option<PlanRef> {
    if let Some(join) = plan.as_logical_join() {
        let left_len = join.left().schema().len();
        if join.join_type() == JoinType::LeftOuter
            && join.output_indices().iter().all(|&index| index < left_len)
        {
            // Every row of the left side, once.
            let left = join.left();
            let stripped = strip(&left);
            let left = stripped.unwrap_or(left);
            let exprs: Vec<ExprImpl> = join
                .output_indices()
                .iter()
                .map(|&index| {
                    InputRef::new(index, left.schema().fields()[index].data_type()).into()
                })
                .collect();
            return Some(LogicalProject::new(left, exprs).into());
        }
        let (strip_left, strip_right) = match join.join_type() {
            JoinType::Inner | JoinType::LeftOuter => (true, true),
            JoinType::LeftSemi | JoinType::LeftAnti => (true, false),
            _ => (false, false),
        };
        let left = strip_left.then(|| strip(&join.left())).flatten();
        let right = strip_right.then(|| strip(&join.right())).flatten();
        if left.is_none() && right.is_none() {
            return None;
        }
        return Some(
            join.clone_with_left_right(
                left.unwrap_or_else(|| join.left()),
                right.unwrap_or_else(|| join.right()),
            )
            .into(),
        );
    }
    if plan.as_logical_project().is_some() || plan.as_logical_filter().is_some() {
        let input = strip(&plan.inputs()[0])?;
        return Some(plan.clone_with_inputs(&[input]));
    }
    if let Some(union) = plan.as_logical_union() {
        if !union.all() {
            return None;
        }
        let inputs = plan.inputs();
        let stripped: Vec<Option<PlanRef>> = inputs.iter().map(strip).collect();
        if stripped.iter().all(Option::is_none) {
            return None;
        }
        let inputs: Vec<PlanRef> = stripped
            .into_iter()
            .zip_eq_fast(inputs.iter())
            .map(|(stripped, input)| stripped.unwrap_or_else(|| input.clone()))
            .collect();
        return Some(plan.clone_with_inputs(&inputs));
    }
    None
}
