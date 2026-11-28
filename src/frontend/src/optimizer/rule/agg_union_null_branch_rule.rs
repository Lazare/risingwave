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

//! Removes the work of the branches of a `UNION ALL` that only feed `NULL` to the aggregation
//! above it (Calcite `AggregateUnionNullBranchRule`).
//!
//! ```text
//! select sum(m) from (                        select sum(m) from (
//!   select k, x as m from a                     select k, x as m from a) v
//!   union all                           =>
//!   select k, null::int from b) v
//!
//! select k, sum(m) from (                     select k, sum(m) from (
//!   select k, sum(x) as m from a group by k     select k, sum(x) as m from a group by k
//!   union all                           =>      union all
//!   select k, null from b group by k) v         select k, null from b) v
//! group by k                                  group by k
//! ```
//!
//! A branch is irrelevant when every aggregate call ignores the rows whose argument is `NULL`
//! (`sum`, `sum0`, `min`, `max`, `count(x)`, `avg`, `stddev_*`, `var_*`, `bit_*`, `bool_*`) and the
//! argument of every call is a column that is `NULL` in every row of the branch
//! ([`always_null_columns`]: `NULL` literals of a projection, `x IS NULL` filters). A single
//! `count(*)`, or any other call (`array_agg`, `string_agg`, `first_value`, ...), keeps every
//! branch.
//!
//! - Without group key, an irrelevant branch contributes nothing: it is removed. When every
//!   branch is removed, the aggregation reads an empty relation and returns its row over no input
//!   (`count` 0, the others `NULL`), the same as over rows whose arguments are all `NULL`.
//! - With a group key, an irrelevant branch still creates groups: it stays. The number of its
//!   copies of a row does not matter (no call counts them), so an aggregation without calls at
//!   the top of the branch (a `GROUP BY` or `DISTINCT`, possibly under a projection) becomes a
//!   projection of its group key.
//!
//! In a streaming job, a removed branch no longer scans, backfills or keeps state; a removed
//! `DISTINCT` no longer keeps its state, and its duplicates do not grow the state of the
//! aggregation above (`min` and `max` skip `NULL` arguments, the others keep one value per group).
//! On demand, behind `streaming_prune_null_union_branches` and `batch_prune_null_union_branches`.

use std::collections::BTreeSet;

use risingwave_expr::aggregate::{AggType, PbAggKind};

use super::null_key_join_rule::always_null_columns;
use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::generic::{GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalProject, LogicalUnion, LogicalValues, PlanTreeNode, PlanTreeNodeUnary,
};

/// See the documentation of the module.
pub struct AggUnionNullBranchRule {}

impl AggUnionNullBranchRule {
    pub fn create() -> BoxedRule {
        Box::new(AggUnionNullBranchRule {})
    }
}

/// The column whose `NULL` makes the call ignore the row, if the call ignores such rows.
fn null_ignoring_argument(call: &PlanAggCall) -> Option<usize> {
    let AggType::Builtin(kind) = &call.agg_type else {
        return None;
    };
    let ignores_null = matches!(
        kind,
        PbAggKind::Sum
            | PbAggKind::Sum0
            | PbAggKind::Min
            | PbAggKind::Max
            | PbAggKind::Count
            | PbAggKind::Avg
            | PbAggKind::StddevPop
            | PbAggKind::StddevSamp
            | PbAggKind::VarPop
            | PbAggKind::VarSamp
            | PbAggKind::BitAnd
            | PbAggKind::BitOr
            | PbAggKind::BitXor
            | PbAggKind::BoolAnd
            | PbAggKind::BoolOr
    );
    // `count(*)` has no argument: it counts every row.
    if !ignores_null || call.inputs.len() != 1 || !call.direct_args.is_empty() {
        return None;
    }
    Some(call.inputs[0].index())
}

/// The output columns of a branch that are `NULL` in every row: [`always_null_columns`], and for
/// an aggregation the group key columns that are `NULL` in every row of its input (a `DISTINCT`
/// of a `NULL` literal).
fn branch_null_columns(branch: &PlanRef) -> BTreeSet<usize> {
    if let Some(agg) = branch.as_logical_agg()
        && agg.grouping_sets().is_empty()
    {
        let input_null_columns = always_null_columns(&agg.input());
        return agg
            .group_key()
            .indices()
            .enumerate()
            .filter(|(_, column)| input_null_columns.contains(column))
            .map(|(output, _)| output)
            .collect();
    }
    always_null_columns(branch)
}

/// The branch without its top aggregation without calls (under a projection or not): its group
/// key columns, each row once per row of its input.
fn without_distinct(branch: &PlanRef) -> Option<PlanRef> {
    let as_projection = |plan: &PlanRef| -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        if !agg.agg_calls().is_empty()
            || !agg.grouping_sets().is_empty()
            || agg.group_key().is_empty()
        {
            return None;
        }
        Some(LogicalProject::with_out_col_idx(agg.input(), agg.group_key().indices()).into())
    };
    if let Some(project) = branch.as_logical_project() {
        let input = as_projection(&project.input())?;
        return Some(project.clone_with_input(input).into());
    }
    as_projection(branch)
}

impl Rule<Logical> for AggUnionNullBranchRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        if !agg.grouping_sets().is_empty() || agg.agg_calls().is_empty() {
            return None;
        }
        let input = agg.input();
        let union = input.as_logical_union()?;
        if !union.all() || union.source_col().is_some() {
            return None;
        }
        let arguments: Vec<usize> = agg
            .agg_calls()
            .iter()
            .map(null_ignoring_argument)
            .collect::<Option<_>>()?;
        let branches = input.inputs();
        let irrelevant: Vec<bool> = branches
            .iter()
            .map(|branch| {
                let null_columns = branch_null_columns(branch);
                arguments
                    .iter()
                    .all(|argument| null_columns.contains(argument))
            })
            .collect();
        if !irrelevant.iter().any(|&irrelevant| irrelevant) {
            return None;
        }

        let new_input: PlanRef = if agg.group_key().is_empty() {
            let mut kept: Vec<PlanRef> = branches
                .iter()
                .zip(&irrelevant)
                .filter(|(_, irrelevant)| !**irrelevant)
                .map(|(branch, _)| branch.clone())
                .collect();
            match kept.len() {
                0 => LogicalValues::create(vec![], input.schema().clone(), input.ctx()),
                1 => kept.pop().expect("one branch"),
                _ => LogicalUnion::create(true, kept),
            }
        } else {
            let mut changed = false;
            let new_branches: Vec<PlanRef> = branches
                .iter()
                .zip(&irrelevant)
                .map(|(branch, &irrelevant)| {
                    if irrelevant && let Some(new_branch) = without_distinct(branch) {
                        changed = true;
                        new_branch
                    } else {
                        branch.clone()
                    }
                })
                .collect();
            if !changed {
                return None;
            }
            LogicalUnion::create(true, new_branches)
        };
        Some(agg.clone_with_input(new_input).into())
    }
}
