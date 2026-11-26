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

use std::collections::{BTreeSet, HashSet};

use risingwave_pb::plan_common::JoinType;

use super::never_true_predicate_rule::fold_join_without_match;
use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{PlanNodeId, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::utils::Condition;

/// Removes the work of a join whose condition compares with `=` a column that is `NULL` in every
/// row of one of its inputs: `NULL = x` is `NULL` for every `x` and every type, so no pair of rows
/// satisfies the condition, whatever its other conjuncts.
///
/// ```text
/// Join(Inner, a.b_id = b.id)                 Values([])
/// ├─Scan(a)                            =>
/// └─Filter(b.id IS NULL)
///   └─Scan(b)
/// ```
///
/// The pattern comes from a filter `c IS NULL` on the output of a join that predicate pushdown
/// moves below the join, next to the scan of the side that `c` comes from.
///
/// The join is rewritten as [`NeverTruePredicateRule`] rewrites a join whose condition is never
/// true, only when the column is on a side whose rows must match to be useful:
///
/// - `Inner`, `LeftSemi`, `RightSemi`: either side; the join becomes an empty relation;
/// - `LeftOuter`, `LeftAnti`: the right side, which is replaced with an empty relation (every row
///   of the left side is padded with `NULL`s, or kept);
/// - `RightOuter`, `RightAnti`: the left side, symmetrically;
/// - `FullOuter` and as-of joins are left untouched.
///
/// `EmptyRelationPropagateRule` and `EmptyInputSimplifyRule`, applied after this rule in the same
/// stage, then remove the join and what is above it.
///
/// A column is known to be `NULL` in every row of a plan node (see [`always_null_columns`]) only
/// from a `c IS NULL` conjunct of a filter or of a scan predicate, followed up through filters,
/// shares and projections of input references, or from a `NULL` literal of a projection. These
/// are properties of the rows that a node can output, so they hold for every row ever produced,
/// under retractions too. Only `=` is recognized, not `IS NOT DISTINCT FROM`, which matches
/// `NULL`s. An operand of `=` is the column itself or a cast of it (a cast of `NULL` is `NULL`).
///
/// For a streaming job (`create_for_stream`), the rule leaves the join untouched if a sub-plan
/// that it would remove may carry a watermark (a scan of a table with a watermark, a source,
/// `now()`): the watermark could be required above, e.g. by `EMIT ON WINDOW CLOSE`.
///
/// The rule is only used on demand: `batch_simplify_null_join_keys`,
/// `streaming_simplify_null_join_keys`.
///
/// [`NeverTruePredicateRule`]: super::NeverTruePredicateRule
pub struct NullKeyJoinRule {
    for_stream: bool,
}

impl NullKeyJoinRule {
    pub fn create_for_batch() -> BoxedRule {
        Box::new(NullKeyJoinRule { for_stream: false })
    }

    pub fn create_for_stream() -> BoxedRule {
        Box::new(NullKeyJoinRule { for_stream: true })
    }
}

impl Rule<Logical> for NullKeyJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let (left, right) = (join.left(), join.right());
        // The sides whose `NULL` columns are looked at, and the sides that the rewrite removes.
        let (check_left, check_right, remove_left, remove_right) = match join.join_type() {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => (true, true, true, true),
            JoinType::LeftOuter | JoinType::LeftAnti => (false, true, false, true),
            JoinType::RightOuter | JoinType::RightAnti => (true, false, true, false),
            _ => return None,
        };
        let left_null = if check_left {
            always_null_columns(&left)
        } else {
            BTreeSet::new()
        };
        let right_null = if check_right {
            always_null_columns(&right)
        } else {
            BTreeSet::new()
        };
        if left_null.is_empty() && right_null.is_empty() {
            return None;
        }
        let left_len = left.schema().len();
        let is_null_column = |index: usize| {
            if index < left_len {
                left_null.contains(&index)
            } else {
                right_null.contains(&(index - left_len))
            }
        };
        let never_matches = join.on().conjunctions.iter().any(|conjunct| {
            conjunct.as_function_call().is_some_and(|call| {
                call.func_type() == ExprType::Equal
                    && call.inputs().len() == 2
                    && call
                        .inputs()
                        .iter()
                        .any(|operand| column_of(operand).is_some_and(is_null_column))
            })
        });
        if !never_matches {
            return None;
        }
        if self.for_stream
            && ((remove_left && may_carry_watermark(&left, &mut HashSet::new()))
                || (remove_right && may_carry_watermark(&right, &mut HashSet::new())))
        {
            return None;
        }
        fold_join_without_match(&plan, join)
    }
}

/// The column that an operand of `=` reads, if it is a column or a cast of a column: `NULL` in
/// that column makes the operand `NULL`.
fn column_of(expr: &ExprImpl) -> Option<usize> {
    match expr {
        ExprImpl::InputRef(input_ref) => Some(input_ref.index()),
        ExprImpl::FunctionCall(call)
            if call.func_type() == ExprType::Cast && call.inputs().len() == 1 =>
        {
            column_of(&call.inputs()[0])
        }
        _ => None,
    }
}

/// The columns of a condition that one of its conjuncts requires to be `NULL`: `c IS NULL` with
/// `c` a column. The indices are those of the columns that the condition refers to.
fn is_null_conjuncts(condition: &Condition) -> impl Iterator<Item = usize> + '_ {
    condition.conjunctions.iter().filter_map(|conjunct| {
        let call = conjunct.as_function_call()?;
        if call.func_type() != ExprType::IsNull {
            return None;
        }
        match call.inputs() {
            [ExprImpl::InputRef(input_ref)] => Some(input_ref.index()),
            _ => None,
        }
    })
}

/// The output columns of a plan node that are `NULL` in every row it outputs. Conservative: a
/// column that is not recognized is not reported.
///
/// - `Filter`: the columns of its input, and those of its `c IS NULL` conjuncts.
/// - `Scan`: the columns of the `c IS NULL` conjuncts of its predicate. The predicate refers to
///   the columns of the table, which are mapped to the output columns of the scan.
/// - `Project`: the input references to such columns of its input, and the `NULL` literals.
/// - `Share`: the columns of its input.
fn always_null_columns(plan: &PlanRef) -> BTreeSet<usize> {
    if let Some(filter) = plan.as_logical_filter() {
        let mut columns = always_null_columns(&filter.input());
        columns.extend(is_null_conjuncts(filter.predicate()));
        return columns;
    }
    if let Some(scan) = plan.as_logical_scan() {
        let table_columns: BTreeSet<usize> = is_null_conjuncts(scan.predicate()).collect();
        return scan
            .output_col_idx()
            .iter()
            .enumerate()
            .filter(|(_, table_column)| table_columns.contains(table_column))
            .map(|(output_column, _)| output_column)
            .collect();
    }
    if let Some(project) = plan.as_logical_project() {
        let input = always_null_columns(&project.input());
        return project
            .exprs()
            .iter()
            .enumerate()
            .filter(|(_, expr)| match expr {
                ExprImpl::InputRef(input_ref) => input.contains(&input_ref.index()),
                ExprImpl::Literal(literal) => literal.get_data().is_none(),
                _ => false,
            })
            .map(|(index, _)| index)
            .collect();
    }
    if let Some(share) = plan.as_logical_share() {
        return always_null_columns(&share.input());
    }
    BTreeSet::new()
}

/// Whether a sub-plan may produce a watermark: it holds a scan of a table with a watermark, a
/// source or `now()`. A shared sub-plan is visited once.
fn may_carry_watermark(plan: &PlanRef, visited: &mut HashSet<PlanNodeId>) -> bool {
    if !visited.insert(plan.id()) {
        return false;
    }
    if plan.as_logical_now().is_some() || plan.as_logical_source().is_some() {
        return true;
    }
    if let Some(scan) = plan.as_logical_scan()
        && scan.table().watermark_columns.ones().next().is_some()
    {
        return true;
    }
    plan.inputs()
        .iter()
        .any(|input| may_carry_watermark(input, visited))
}
