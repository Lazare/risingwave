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

//! Semi and anti joins placed by the cost-based ordering of the inner joins of a batch query
//! (`batch_semi_join_ordering`): Calcite's `SemiJoinJoinTransposeRule` and PostgreSQL's
//! `reduce_unique_semijoins`, chosen by the estimates of `cost_based_join_ordering_rule.rs`.

use std::collections::BTreeSet;

use risingwave_pb::plan_common::JoinType;

use super::cost_based_join_ordering_rule::{
    SEMI_JOIN_FRACTION, hash_join_cost, multi_join_cost, relation_cost,
};
use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::is_unique_on_compared_columns;
use crate::optimizer::PlanTreeNode;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    EqJoinPredicate, LogicalJoin, LogicalMultiJoin, LogicalMultiJoinBuilder, LogicalProject,
    PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::ColIndexMapping;

/// A placement replaces the semi join above the inner joins only when its estimated cost is lower
/// by this factor: the estimates are rough.
const PLACEMENT_MARGIN: f64 = 1.25;

/// Places a left semi or anti join whose left side is a set of inner joins (a multi-join, or a
/// two-way inner join) where the estimates of the cost-based join ordering say it costs least:
///
/// - **pushed down** into the input of the inner joins that holds all the columns of the left
///   side that its condition reads (Calcite's `SemiJoinJoinTransposeRule`):
///
///   ```text
///   LeftSemi(a.x = c.x)                  MultiJoin(a.k = b.k)
///   ├─MultiJoin(a.k = b.k)          =>   ├─LeftSemi(a.x = c.x)
///   │ ├─a                                │ ├─a
///   │ └─b                                │ └─c
///   └─c                                  └─b
///   ```
///
///   Exact for semi and anti joins: they keep a row of their left side according to its columns
///   that the condition reads only, here the columns of `a`; a row of the inner joins is kept
///   exactly when its part from `a` is, so filtering `a` first and joining keeps the same rows,
///   with their multiplicities. Worth it when the semi join removes rows of `a` before they meet
///   the other inputs, or when `a` has fewer rows than the joined result.
///
/// - **merged** into the inner joins as one more input, when it is a semi join (not anti) whose
///   right side is unique on the columns that the condition binds (`is_unique_on_compared_columns`,
///   PostgreSQL's `reduce_unique_semijoins`): a row of the left side then matches at most one row,
///   so the inner join with the right side, whose columns are not output, returns the rows of the
///   semi join. The ordering may then join the right side early (a small filtered table first, a
///   lookup...). The condition must hold an equality between the two sides.
///
/// The costs compared are those of the cost-based ordering (rows of the tables from the storage
/// statistics, learned ratios with `batch_cardinality_feedback_learning`, widths of the rows; the
/// rows kept by a semi or anti join are a fixed fraction of its left side): the join tree of the
/// inner joins plus the hash semi join above them, against the join tree with the semi join
/// pushed into an input (the hash semi join over that input, which then counts for a fraction of
/// its rows), or with the right side merged as an input. The semi join moves only when that costs
/// less by [`PLACEMENT_MARGIN`]; without an estimate for every input it stays where it is.
///
/// The multi-join that results is ordered by `CostBasedJoinOrderingRule`. Applied bottom-up, so
/// that a semi join stacked over another one sees the multi-join the lower one was pushed into.
pub struct SemiJoinPlacementRule;

impl Rule<Logical> for SemiJoinPlacementRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let semi = plan.as_logical_join()?;
        let join_type = semi.join_type();
        if !matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            return None;
        }
        let (left, right) = (semi.left(), semi.right());
        let multi_join = as_multi_join(&left)?;
        let left_len = left.schema().len();
        let right_len = right.schema().len();
        let inputs = multi_join.inputs().to_vec();
        let col_nums = multi_join.input_col_nums();
        let total_cols: usize = col_nums.iter().sum();
        // The input of each internal column of the multi-join, and its index in that input.
        let mut column_input = vec![];
        for (input, &num) in col_nums.iter().enumerate() {
            column_input.extend((0..num).map(|column| (input, column)));
        }
        // The internal column of the multi-join of each output column (the left side).
        let left_internal = multi_join.output_indices().to_vec();
        if left_internal.len() != left_len {
            return None;
        }
        let on = semi.on().clone();
        let referenced_inputs: BTreeSet<usize> = on
            .collect_input_refs(left_len + right_len)
            .ones()
            .filter(|&column| column < left_len)
            .map(|column| column_input[left_internal[column]].0)
            .collect();

        // The cost of the semi join kept above the inner joins.
        let joined = multi_join_cost(&multi_join)?;
        let tested = relation_cost(&right)?;
        let semi_output_rows = joined.rows * SEMI_JOIN_FRACTION;
        let keep_cost = joined.cost
            + tested.cost
            + hash_join_cost(&joined, &tested, semi_output_rows, joined.width);

        let mut best: Option<(f64, PlanRef, &str)> = None;
        let mut consider = |cost: f64, joined: PlanRef, how: &'static str| {
            if cost * PLACEMENT_MARGIN < keep_cost
                && best
                    .as_ref()
                    .is_none_or(|(best_cost, _, _)| cost < *best_cost)
            {
                best = Some((cost, joined, how));
            }
        };

        // Pushed into the only input whose columns the condition reads.
        if let [input] = referenced_inputs.iter().copied().collect::<Vec<_>>()[..] {
            let input_len = col_nums[input];
            let offset: usize = col_nums[..input].iter().sum();
            // Left columns to the columns of the input, right columns after them.
            let mut mapping = vec![None; left_len + right_len];
            for (column, &internal) in left_internal.iter().enumerate() {
                let (owner, local) = column_input[internal];
                if owner == input {
                    debug_assert_eq!(internal - offset, local);
                    mapping[column] = Some(local);
                }
            }
            for column in 0..right_len {
                mapping[left_len + column] = Some(input_len + column);
            }
            let pushed_on = on
                .clone()
                .rewrite_expr(&mut ColIndexMapping::new(mapping, input_len + right_len));
            let pushed: PlanRef = LogicalJoin::with_output_indices(
                inputs[input].clone(),
                right.clone(),
                join_type,
                pushed_on,
                (0..input_len).collect(),
            )
            .into();
            let mut new_inputs = inputs.clone();
            new_inputs[input] = pushed.clone();
            let new_multi_join = LogicalMultiJoin::new(
                new_inputs,
                multi_join.on().clone(),
                multi_join.output_indices().to_vec(),
            );
            if let (Some(original), Some(filtered), Some(new_joined)) = (
                relation_cost(&inputs[input]),
                relation_cost(&pushed),
                multi_join_cost(&new_multi_join),
            ) {
                // The join tree counts the pushed semi join as an input that reads its own rows:
                // count instead the reading of the original input, of the right side, and the
                // hash semi join over them.
                let cost = new_joined.cost - filtered.cost
                    + original.cost
                    + tested.cost
                    + hash_join_cost(&original, &tested, filtered.rows, original.width);
                consider(cost, new_multi_join.into(), "pushed into an input");
            }
        }

        // Merged as an input when the right side is unique on the compared columns.
        if join_type == JoinType::LeftSemi
            && EqJoinPredicate::create(left_len, right_len, on.clone()).has_eq()
            && is_unique_on_compared_columns(semi).1
        {
            // Left columns to the internal columns of the multi-join, right columns after them.
            let mut mapping: Vec<Option<usize>> = left_internal
                .iter()
                .map(|&internal| Some(internal))
                .collect();
            mapping.extend((0..right_len).map(|column| Some(total_cols + column)));
            let merged_on =
                on.rewrite_expr(&mut ColIndexMapping::new(mapping, total_cols + right_len));
            let mut new_inputs = inputs;
            new_inputs.push(right);
            let new_multi_join = LogicalMultiJoin::new(
                new_inputs,
                multi_join.on().clone().and(merged_on),
                multi_join.output_indices().to_vec(),
            );
            if let Some(merged) = multi_join_cost(&new_multi_join) {
                consider(
                    merged.cost,
                    new_multi_join.into(),
                    "merged as an inner join",
                );
            }
        }

        let (cost, joined_plan, how) = best?;
        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace(format!(
                "Semi join placement: {:?} join {} at the estimated cost {} instead of {}",
                join_type,
                how,
                cost.round(),
                keep_cost.round()
            ));
        }
        // The multi-join outputs the columns of the left side; the semi join output some of them.
        let output = semi.output_indices();
        if output.iter().copied().eq(0..left_len) {
            Some(joined_plan)
        } else {
            Some(LogicalProject::with_out_col_idx(joined_plan, output.iter().copied()).into())
        }
    }
}

impl SemiJoinPlacementRule {
    pub fn create() -> BoxedRule {
        Box::new(SemiJoinPlacementRule)
    }
}

/// The inner joins of `plan` as one multi-join with the same output columns: a multi-join,
/// possibly under a projection of its columns, or a tree of inner joins and filters of at least
/// two inputs.
fn as_multi_join(plan: &PlanRef) -> Option<LogicalMultiJoin> {
    if let Some(multi_join) = plan.as_logical_multi_join() {
        return Some(LogicalMultiJoin::new(
            multi_join.inputs().to_vec(),
            multi_join.on().clone(),
            multi_join.output_indices().to_vec(),
        ));
    }
    if let Some(project) = plan.as_logical_project()
        && let Some(columns) = project.try_as_projection()
        && let Some(multi_join) = project.input().as_logical_multi_join()
    {
        let output_indices = multi_join.output_indices();
        return Some(LogicalMultiJoin::new(
            multi_join.inputs().to_vec(),
            multi_join.on().clone(),
            columns
                .into_iter()
                .map(|column| output_indices[column])
                .collect(),
        ));
    }
    let builder = LogicalMultiJoinBuilder::new(plan.clone());
    if builder.inputs().len() < 2 {
        return None;
    }
    Some(builder.build())
}
