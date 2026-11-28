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

//! Partial aggregation pushed into the branches of a `UNION ALL`, in batch queries, on demand
//! (`batch_partial_agg_into_union`; Calcite `AggregateUnionTransposeRule`, PostgreSQL
//! partitionwise aggregation).

use std::collections::BTreeSet;

use super::agg_join_transpose_rule::{
    PARTIAL_AGG_MAX_GROUP_RATIO, through_projects, total_call, total_of_partial,
};
use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::{agg_over_one_row, unique_keys_of};
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::cardinality_learning::{
    CARDINALITY_LEARNING_STORE, RatioKind, learned_ratio,
};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::{LogicalProject, LogicalUnion, PlanTreeNode, PlanTreeNodeUnary};
use crate::utils::IndexSet;

/// Aggregates each branch of a `UNION ALL` partially before the union, and finishes the
/// aggregation above it:
///
/// ```text
/// Agg(group k: count(*), sum(x))           Agg(group k: sum0(n), sum(s))
/// └─Union all                         =>   └─Union all
///   ├─a                                      ├─Agg(group k: n = count(*), s = sum(x))
///   └─b                                      │ └─a
///                                            └─Project(k, 1, x::bigint)
///                                              └─b
/// ```
///
/// - Every aggregate decomposes ([`total_of_partial`]): the partial aggregate is the aggregate
///   itself over the rows of a branch, with its `FILTER`; the final one combines the partial
///   results with the same result type (`count` by `sum0`, `sum(int)` as a `bigint` at both
///   levels, `min` of `min`...). A group of the union is the union of the groups with its key in
///   the branches, so combining the per-branch results gives the result over the group. Without
///   group key, a branch gives one row even when it is empty (a count of 0, `NULL` for the others),
///   which the final aggregate ignores or adds as 0: an empty union still gives one row, with the
///   same values.
/// - Chosen per branch, by cost: a branch without unique key within the group key is aggregated
///   when the aggregation has no group key (one row per branch), or when the largest group ratio
///   learned for the partial aggregation of the branch (`batch_cardinality_feedback_learning`) is
///   at most [`PARTIAL_AGG_MAX_GROUP_RATIO`]. Another branch gives each of its rows as a
///   partial result of its own (`count(*)` is `1`, `sum(x)` is `x`, see `agg_over_one_row`). The
///   rule applies when at least one branch is aggregated.
/// - Projections between the aggregation and the union are computed in each branch, they must be
///   deterministic. Not for grouping sets, `UNION` without `ALL` (an aggregation over a
///   `UNION ALL` by then anyway) or a union with a source column.
///
/// Batch only: in a streaming job, the partial aggregations would add operators and states.
pub struct AggUnionTransposeRule;

impl AggUnionTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(AggUnionTransposeRule)
    }
}

impl Rule<Logical> for AggUnionTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        if !agg.grouping_sets().is_empty() {
            return None;
        }
        let (union_plan, to_union) = through_projects(agg.input());
        let union = union_plan.as_logical_union()?;
        if !union.all() || union.source_col().is_some() || union.inputs().len() < 2 {
            return None;
        }
        if to_union.iter().any(|expr| !expr.is_pure()) {
            return None;
        }
        let totals = agg
            .agg_calls()
            .iter()
            .map(total_of_partial)
            .collect::<Option<Vec<_>>>()?;
        let group_key = agg.group_key();
        let group_len = group_key.len();
        let identity = to_union
            .iter()
            .enumerate()
            .all(|(index, expr)| expr.as_input_ref().is_some_and(|r| r.index() == index))
            && to_union.len() == union_plan.schema().len();
        let learning = plan
            .ctx()
            .session_ctx()
            .config()
            .batch_cardinality_feedback_learning();
        let group_columns: BTreeSet<usize> = group_key.indices().collect();

        let mut aggregated = false;
        let mut branches = Vec::with_capacity(union.inputs().len());
        for branch in union.inputs() {
            // The input of the aggregation, for the rows of this branch.
            let input = if identity {
                branch.clone()
            } else {
                LogicalProject::create(branch.clone(), to_union.clone())
            };
            let partial: PlanRef =
                Agg::new(agg.agg_calls().clone(), group_key.clone(), input.clone()).into();
            // A branch unique on the group key (an aggregation already, or at most one row) would
            // not be reduced; this also stops the rule on the aggregation it created.
            let unique = unique_keys_of(&input)
                .iter()
                .any(|key| key.is_subset(&group_columns));
            let worth = !unique
                && (group_len == 0
                    || (learning
                        && learned_ratio(
                            &CARDINALITY_LEARNING_STORE,
                            RatioKind::GroupRatio,
                            &partial,
                        )
                        .is_some_and(|learned| learned.max <= PARTIAL_AGG_MAX_GROUP_RATIO)));
            if worth {
                aggregated = true;
                branches.push(partial);
            } else {
                // Each row as a partial result of its own, in the columns of the partial
                // aggregation: the group key, then one column per aggregate.
                let mut exprs: Vec<ExprImpl> = group_key
                    .indices()
                    .map(|index| InputRef::new(index, input.schema()[index].data_type()).into())
                    .collect();
                for call in agg.agg_calls() {
                    exprs.push(agg_over_one_row(call)?);
                }
                branches.push(LogicalProject::create(input, exprs));
            }
        }
        if !aggregated {
            return None;
        }
        let union = LogicalUnion::create(true, branches);
        let final_calls = agg
            .agg_calls()
            .iter()
            .zip(totals)
            .enumerate()
            .map(|(position, (call, total))| total_call(call, total, group_len + position))
            .collect();
        Some(Agg::new(final_calls, (0..group_len).collect::<IndexSet>(), union).into())
    }
}
