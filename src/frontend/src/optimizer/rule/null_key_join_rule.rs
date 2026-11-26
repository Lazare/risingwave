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

use std::collections::{BTreeSet, HashMap};

use risingwave_pb::plan_common::JoinType;

use super::never_true_predicate_rule::{fold_join_without_match, may_carry_watermark};
use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType, InputRef};
use crate::optimizer::PlanTreeNode;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalJoin, LogicalProject, LogicalUnion, PlanNodeId, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
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
/// The column may be on either side, whatever the type of the join: no pair of rows matches. The
/// join is rewritten as [`NeverTruePredicateRule`] rewrites a join whose condition is never true:
///
/// - `Inner`, `LeftSemi`, `RightSemi`: the join becomes an empty relation;
/// - `LeftOuter`, `LeftAnti`: the right side is replaced with an empty relation (every row of the
///   left side is padded with `NULL`s, or kept). With the `NULL` column on the left (preserved)
///   side, no row of the right side is ever used either;
/// - `RightOuter`, `RightAnti`: the left side, symmetrically;
/// - `FullOuter`: every row of each side is output once, padded with `NULL`s: the join becomes a
///   `UNION ALL` of the two sides, each projected with `NULL`s for the columns of the other;
/// - as-of joins are left untouched.
///
/// `EmptyRelationPropagateRule` and `EmptyInputSimplifyRule`, applied after this rule in the same
/// stage, then remove the join and what is above it.
///
/// A column is known to be `NULL` in every row of a plan node (see [`always_null_columns`]) only
/// from a `c IS NULL` conjunct of a filter or of a scan predicate, followed up through filters,
/// shares, projections of input references (or casts of them), joins, unions (a column `NULL` in
/// every input), and the operators that output the columns of their input as they are; or from a
/// `NULL` literal of a projection. These are properties of the rows that a node can output, so
/// they hold for every row ever produced, under retractions too. Only `=` is recognized, not `IS NOT DISTINCT FROM`,
/// which matches `NULL`s. An operand of `=` is the column itself or a cast of it (a cast of `NULL`
/// is `NULL`).
///
/// For a streaming job (`create_for_stream`), the rule leaves the join untouched if a sub-plan
/// that it would remove may carry a watermark (a scan of a table with a watermark, a source with
/// a watermark, `now()`): the watermark could be required above, e.g. by
/// `EMIT ON WINDOW CLOSE`. A full join is left untouched if either side may carry one (the union
/// does not pass on a watermark that only one of its inputs has).
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
        if !matches!(
            join.join_type(),
            JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftOuter
                | JoinType::LeftAnti
                | JoinType::RightOuter
                | JoinType::RightAnti
                | JoinType::FullOuter
        ) {
            return None;
        }
        let (left, right) = (join.left(), join.right());
        let left_null = always_null_columns(&left);
        let right_null = always_null_columns(&right);
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
        if join.join_type() == JoinType::FullOuter {
            if self.for_stream && (may_carry_watermark(&left) || may_carry_watermark(&right)) {
                return None;
            }
            return Some(full_join_without_match(join));
        }
        // In streaming, the parts that may carry a watermark are kept.
        fold_join_without_match(&plan, join, self.for_stream)
    }
}

/// A full join in which no pair of rows matches: every row of each side, padded with `NULL`s for
/// the columns of the other side, in a `UNION ALL`.
fn full_join_without_match(join: &LogicalJoin) -> PlanRef {
    let (left, right) = (join.left(), join.right());
    let left_len = left.schema().len();
    // The output indices refer to the columns of the left side followed by those of the right.
    let padded = |side: &PlanRef, is_left: bool| -> PlanRef {
        let exprs: Vec<ExprImpl> = join
            .output_indices()
            .iter()
            .map(|&index| {
                let (from_left, side_index) = if index < left_len {
                    (true, index)
                } else {
                    (false, index - left_len)
                };
                let data_type = if from_left {
                    left.schema()[side_index].data_type.clone()
                } else {
                    right.schema()[side_index].data_type.clone()
                };
                if from_left == is_left {
                    InputRef::new(side_index, data_type).into()
                } else {
                    ExprImpl::literal_null(data_type)
                }
            })
            .collect();
        LogicalProject::create(side.clone(), exprs)
    };
    LogicalUnion::create(true, vec![padded(&left, true), padded(&right, false)])
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
/// - `Project`: the input references to such columns of its input or casts of them, and the
///   `NULL` literals.
/// - `Share`, `Limit`, `TopN`, `Dedup`, `OverWindow`: the columns of their input (an over window
///   outputs the columns of its input first).
/// - `Union`: the columns that are `NULL` in every input.
/// - `Join`: the columns of either side that are `NULL` in that side: a row of the output holds
///   the values of a row of that side, or `NULL`s for the padded side of an outer join.
fn always_null_columns(plan: &PlanRef) -> BTreeSet<usize> {
    null_columns(plan, &mut HashMap::new())
}

/// See [`always_null_columns`]. The result of each node is computed once (`memo`), so that a
/// sub-plan shared by several consumers is not walked again.
fn null_columns(
    plan: &PlanRef,
    memo: &mut HashMap<PlanNodeId, BTreeSet<usize>>,
) -> BTreeSet<usize> {
    if let Some(columns) = memo.get(&plan.id()) {
        return columns.clone();
    }
    let columns = null_columns_of_node(plan, memo);
    memo.insert(plan.id(), columns.clone());
    columns
}

fn null_columns_of_node(
    plan: &PlanRef,
    memo: &mut HashMap<PlanNodeId, BTreeSet<usize>>,
) -> BTreeSet<usize> {
    if let Some(filter) = plan.as_logical_filter() {
        let mut columns = null_columns(&filter.input(), memo);
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
        let input = null_columns(&project.input(), memo);
        return project
            .exprs()
            .iter()
            .enumerate()
            .filter(|(_, expr)| match expr {
                ExprImpl::Literal(literal) => literal.get_data().is_none(),
                _ => column_of(expr).is_some_and(|index| input.contains(&index)),
            })
            .map(|(index, _)| index)
            .collect();
    }
    if plan.as_logical_share().is_some()
        || plan.as_logical_limit().is_some()
        || plan.as_logical_top_n().is_some()
        || plan.as_logical_dedup().is_some()
        || plan.as_logical_over_window().is_some()
    {
        let [input] = &plan.inputs()[..] else {
            return BTreeSet::new();
        };
        let len = input.schema().len();
        return null_columns(input, memo)
            .into_iter()
            .filter(|&index| index < len)
            .collect();
    }
    if let Some(union) = plan.as_logical_union() {
        let mut inputs = union.inputs().into_iter();
        let Some(first) = inputs.next() else {
            return BTreeSet::new();
        };
        return inputs.fold(null_columns(&first, memo), |columns, input| {
            if columns.is_empty() {
                return columns;
            }
            let other = null_columns(&input, memo);
            columns.intersection(&other).copied().collect()
        });
    }
    if let Some(join) = plan.as_logical_join() {
        let left_null = null_columns(&join.left(), memo);
        let right_null = null_columns(&join.right(), memo);
        let left_len = join.left().schema().len();
        // The internal columns of the join: those of the left side followed by those of the right
        // side, or only those of the side that a semi or anti join outputs.
        let is_null_internal = |index: usize| match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti => left_null.contains(&index),
            JoinType::RightSemi | JoinType::RightAnti => right_null.contains(&index),
            _ if index < left_len => left_null.contains(&index),
            _ => right_null.contains(&(index - left_len)),
        };
        return join
            .output_indices()
            .iter()
            .enumerate()
            .filter(|(_, index)| is_null_internal(**index))
            .map(|(output, _)| output)
            .collect();
    }
    BTreeSet::new()
}
