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

use fixedbitset::FixedBitSet;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use super::values_join_inline_rule::in_list;
use crate::expr::{Expr, ExprImpl, ExprType, InputRef, to_conjunctions, to_disjunctions};
use crate::optimizer::plan_node::generic::{
    GenericPlanRef, can_push_left_from_on, can_push_right_from_on,
};
use crate::optimizer::plan_node::{LogicalJoin, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::utils::Condition;

/// The largest number of disjuncts of an `OR` that the rule looks into.
const MAX_DISJUNCTS: usize = 16;

/// Derives, from a conjunct `d_1 OR ... OR d_n` of the condition of a join that refers to both
/// sides, a restriction of each side: the `OR` of the parts of the disjuncts that refer to that side
/// only. The original condition is kept; predicate pushdown then moves the restrictions into the
/// sides. This is PostgreSQL's `extract_restriction_or_clauses` (`orclauses.c`) and Calcite's
/// `ExpandDisjunctionForJoinInputsRule`.
///
/// ```text
/// Join(Inner, on: a.k = b.k AND ((a.x = 1 AND b.y = 2) OR (a.x = 3 AND b.y = 4)))
/// gains the conjuncts (a.x = 1 OR a.x = 3) and (b.y = 2 OR b.y = 4)
/// ```
///
/// Correctness: if the `OR` is true for a pair of rows, one of its disjuncts `d_i` is true, so each
/// of its conjuncts is true, in particular the conjunction `p_i` of those that refer to side `X`
/// only; so `p_1 OR ... OR p_n` is true. A pair for which the derived predicate is not true can not
/// satisfy the condition: adding it as a conjunct changes nothing, for any type of join. A side is
/// restricted only when every disjunct has a part on it (otherwise nothing is implied), and only
/// when predicate pushdown moves a conjunct of the `ON` condition on that side into it
/// (`can_push_left_from_on`, `can_push_right_from_on`: both sides of an inner join, the optional
/// side of an outer join, the left side of a left semi join), so that the derived conjunct does
/// not stay in the join.
///
/// The parts are conjuncts that can not fail and are deterministic: column references, literals,
/// comparisons, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, `IS [NOT] DISTINCT FROM`, `IN`, `NOT`,
/// `AND`, `OR` of them. Evaluating them on rows that the join would not have compared can then
/// neither raise an error nor give a different answer. Temporal and as-of joins are left untouched.
///
/// The restriction of a side is written without redundancy: the equalities and `IN` lists that
/// compare a column with constants are merged into one `IN` list without duplicates
/// (`c = 1 OR c = 2 OR c IN (1, 3)` becomes `c IN (1, 2, 3)`), and duplicate parts are removed.
/// It is not derived when it can not remove a row: a single `IN` list (or equality) on a column
/// whose values other than `NULL` are a known set of literals that the list covers, such as a
/// `CASE` that returns 0 or 1 compared with 0 and with 1 (see [`column_values`]). The join
/// condition already rejects the `NULL`s of that column, so nothing is lost.
///
/// The rule is only used on demand (`batch_derive_or_restrictions`,
/// `streaming_derive_or_restrictions`).
pub struct OrRestrictionDeriveRule {}

impl OrRestrictionDeriveRule {
    pub fn create() -> BoxedRule {
        Box::new(OrRestrictionDeriveRule {})
    }
}

impl Rule<Logical> for OrRestrictionDeriveRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let join_type = join.join_type();
        if join.is_asof_join() || join.should_be_temporal_join() {
            return None;
        }
        let left_len = join.left().schema().len();
        let total = left_len + join.right().schema().len();
        let mut sides = vec![];
        if can_push_left_from_on(join_type) {
            sides.push(columns(total, 0..left_len));
        }
        if can_push_right_from_on(join_type) {
            sides.push(columns(total, left_len..total));
        }
        if sides.is_empty() {
            return None;
        }
        let on = join.on();
        let mut derived = vec![];
        for expr in &on.conjunctions {
            let disjuncts = to_disjunctions(expr.clone());
            if disjuncts.len() < 2 || disjuncts.len() > MAX_DISJUNCTS {
                continue;
            }
            let refs = expr.collect_input_refs(total);
            for side in &sides {
                // An `OR` on one side only is pushed down as it is.
                if refs.is_subset(side) {
                    continue;
                }
                let parts: Option<Vec<ExprImpl>> = disjuncts
                    .iter()
                    .map(|disjunct| {
                        let part: Vec<ExprImpl> = to_conjunctions(disjunct.clone())
                            .into_iter()
                            .filter(|conjunct| {
                                conjunct.collect_input_refs(total).is_subset(side)
                                    && is_safe(conjunct)
                            })
                            .collect();
                        (!part.is_empty()).then(|| ExprImpl::and(part))
                    })
                    .collect();
                if let Some(parts) = parts {
                    let restriction = merge_in_lists(parts);
                    if !covers_all_values(join, left_len, &restriction) {
                        derived.push(restriction);
                    }
                }
            }
        }
        if derived.is_empty() {
            return None;
        }
        let new_on = on.clone().and(Condition {
            conjunctions: derived,
        });
        // Already derived (the join is reached again through a shared sub-plan).
        if new_on == *on {
            return None;
        }
        Some(join.clone_with_cond(new_on).into())
    }
}

/// The set of the columns `range` among `total` columns.
fn columns(total: usize, range: std::ops::Range<usize>) -> FixedBitSet {
    let mut set = FixedBitSet::with_capacity(total);
    set.insert_range(range);
    set
}

/// Whether the expression is made only of column references, literals and functions that are
/// deterministic and can not fail.
fn is_safe(expr: &ExprImpl) -> bool {
    match expr {
        ExprImpl::InputRef(_) | ExprImpl::Literal(_) => true,
        ExprImpl::FunctionCall(call) => {
            matches!(
                call.func_type(),
                ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::LessThan
                    | ExprType::LessThanOrEqual
                    | ExprType::GreaterThan
                    | ExprType::GreaterThanOrEqual
                    | ExprType::IsNull
                    | ExprType::IsNotNull
                    | ExprType::IsTrue
                    | ExprType::IsNotTrue
                    | ExprType::IsFalse
                    | ExprType::IsNotFalse
                    | ExprType::IsDistinctFrom
                    | ExprType::IsNotDistinctFrom
                    | ExprType::In
                    | ExprType::Not
                    | ExprType::And
                    | ExprType::Or
            ) && call.inputs().iter().all(is_safe)
        }
        _ => false,
    }
}

/// The `OR` of the parts, with the equalities and `IN` lists that compare a column with constants
/// merged into one `IN` list per column without duplicates (an equality for a single value), and
/// the duplicate parts removed. Equivalent: `c IN (v_1, ..., v_n)` is `c = v_1 OR ... OR c = v_n`,
/// with the same `NULL` results, and `OR` is commutative, associative and idempotent.
fn merge_in_lists(parts: Vec<ExprImpl>) -> ExprImpl {
    // The columns compared with constants, in the order they appear, with their values.
    let mut lists: Vec<(InputRef, Vec<ExprImpl>)> = vec![];
    let mut others: Vec<ExprImpl> = vec![];
    for disjunct in parts.into_iter().flat_map(to_disjunctions) {
        let compared = disjunct
            .as_eq_const()
            .map(|(column, value)| (column, vec![value]))
            .or_else(|| disjunct.as_in_const_list())
            // Only values of the type of the column go into one `IN` list.
            .filter(|(column, values)| {
                values
                    .iter()
                    .all(|value| value.return_type() == column.return_type())
            });
        let Some((column, values)) = compared else {
            if !others.contains(&disjunct) {
                others.push(disjunct);
            }
            continue;
        };
        let position = match lists.iter().position(|(other, _)| *other == column) {
            Some(position) => position,
            None => {
                lists.push((column, vec![]));
                lists.len() - 1
            }
        };
        lists[position].1.extend(values);
    }
    ExprImpl::or(
        lists
            .into_iter()
            .map(|(column, values)| in_list(&column, values))
            .chain(others),
    )
}

/// Whether the restriction is one `IN` list (or equality) on a column whose values other than
/// `NULL` are a known set of literals, all in the list: it then removes only the rows where the
/// column is `NULL`, which the join condition rejects anyway.
fn covers_all_values(join: &LogicalJoin, left_len: usize, restriction: &ExprImpl) -> bool {
    let Some((column, list)) = restriction
        .as_eq_const()
        .map(|(column, value)| (column, vec![value]))
        .or_else(|| restriction.as_in_const_list())
    else {
        return false;
    };
    let index = column.index();
    let values = if index < left_len {
        column_values(&join.left(), index)
    } else {
        column_values(&join.right(), index - left_len)
    };
    values.is_some_and(|values| values.iter().all(|value| list.contains(value)))
}

/// The values other than `NULL` that a column of a plan can hold, when they are a known set of
/// literals: a literal, or a `CASE` whose results are all literals, in a projection, or the
/// literals of a `VALUES`, followed through the operators that pass the column through and only
/// remove rows or add `NULL`s.
fn column_values(plan: &PlanRef, index: usize) -> Option<Vec<ExprImpl>> {
    if let Some(project) = plan.as_logical_project() {
        return match &project.exprs()[index] {
            ExprImpl::InputRef(input_ref) => column_values(&project.input(), input_ref.index()),
            expr => result_literals(expr),
        };
    }
    if let Some(values) = plan.as_logical_values() {
        let mut list = vec![];
        for row in values.rows() {
            add_literal(&mut list, &row[index])?;
        }
        return Some(list);
    }
    if let Some(join) = plan.as_logical_join() {
        let internal = join.output_indices()[index];
        let left_len = join.left().schema().len();
        return match join.join_type() {
            // The internal columns of a semi or anti join are those of the side it keeps.
            JoinType::LeftSemi | JoinType::LeftAnti => column_values(&join.left(), internal),
            JoinType::RightSemi | JoinType::RightAnti => column_values(&join.right(), internal),
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                if internal < left_len {
                    column_values(&join.left(), internal)
                } else {
                    column_values(&join.right(), internal - left_len)
                }
            }
            _ => None,
        };
    }
    if plan.as_logical_union().is_some() {
        let mut list = vec![];
        for input in plan.inputs() {
            for value in column_values(&input, index)? {
                if !list.contains(&value) {
                    list.push(value);
                }
            }
        }
        return Some(list);
    }
    if let Some(agg) = plan.as_logical_agg() {
        // The output columns are the group key columns, then the aggregate calls.
        if !agg.grouping_sets().is_empty() || index >= agg.group_key().len() {
            return None;
        }
        let column = agg.group_key().indices().nth(index)?;
        return column_values(&agg.input(), column);
    }
    if plan.as_logical_filter().is_some()
        || plan.as_logical_share().is_some()
        || plan.as_logical_dedup().is_some()
        || plan.as_logical_top_n().is_some()
        || plan.as_logical_limit().is_some()
    {
        let inputs = plan.inputs();
        if let [input] = &inputs[..] {
            return column_values(input, index);
        }
    }
    None
}

/// The literals other than `NULL` that an expression can return, when it is a literal or a `CASE`
/// whose results are all literals (no `ELSE` returns `NULL`).
fn result_literals(expr: &ExprImpl) -> Option<Vec<ExprImpl>> {
    let mut list = vec![];
    match expr {
        ExprImpl::Literal(_) => add_literal(&mut list, expr)?,
        ExprImpl::FunctionCall(call) if call.func_type() == ExprType::Case => {
            // The inputs are `condition, result` pairs, then the `ELSE` result if any.
            let inputs = call.inputs();
            let results = inputs
                .iter()
                .skip(1)
                .step_by(2)
                .chain(inputs.last().filter(|_| inputs.len() % 2 == 1));
            for result in results {
                add_literal(&mut list, result)?;
            }
        }
        _ => return None,
    }
    Some(list)
}

/// Adds a literal to the list unless it is `NULL` or already there; `None` if the expression is
/// not a literal.
fn add_literal(list: &mut Vec<ExprImpl>, expr: &ExprImpl) -> Option<()> {
    let ExprImpl::Literal(literal) = expr else {
        return None;
    };
    if literal.get_data().is_some() && !list.contains(expr) {
        list.push(expr.clone());
    }
    Some(())
}
