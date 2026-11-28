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
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::JoinType;

use super::never_true_predicate_rule::is_simple_type;
use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// The largest number of constant rows that the rule inlines.
const MAX_ROWS: usize = 64;

/// Replaces a join with a small relation of constant rows (a `LogicalValues` of at most 64 rows of
/// constants) with a filter and a projection of the other side, as PostgreSQL 18 turns
/// `x IN (VALUES ...)` into `x = ANY(...)` (`convert_VALUES_to_ANY`). It extends
/// [`ConstantRowJoinRule`], which handles one row and inner joins only.
///
/// ```text
/// Join(Inner, on: t.code = v.code)                      Project(t.k, CASE WHEN t.code = 'a' THEN 'A'
/// ├─Scan(t)                                     =>                    WHEN t.code = 'b' THEN 'B' END)
/// └─Values([('a', 'A'), ('b', 'B')] as v(code, label))   └─Filter(t.code IN ('a', 'b'))
///                                                          └─Scan(t)
/// ```
///
/// Let `cond_i` be the conjuncts of the condition that refer to the constant side, with the values
/// of row `i` in place of its columns, and `kept` the conjuncts that only refer to the other side
/// (the kept side). A row `l` of the kept side matches row `i` iff `kept(l) AND cond_i(l)` is true.
///
/// - Semi join: `l` is kept iff some row matches, i.e. iff `kept AND (cond_1 OR ... OR cond_n)` is
///   true. Anti join: iff no row matches, i.e. iff that predicate is not true (`IS NOT TRUE`: a
///   `NULL` comparison does not match). Duplicate rows do not matter.
/// - Inner and outer joins: a row of the kept side must match at most one constant row, otherwise
///   the join outputs it several times. The rule requires a conjunct `k = v.c` (the key) between a
///   column of the kept side and a column `c` of the constant side of a type whose equal values
///   are identical ([`is_simple_type`]), whose values are distinct literals. Inner join: the filter
///   above, and each column `j` of the constant side becomes
///   `CASE WHEN cond_1 THEN r_1[j] ... WHEN cond_n THEN r_n[j] END` (the column of the key becomes
///   `k` itself, equal and therefore identical to the value). Left join (the constant side on the
///   right; or a right join with the constant side on the left): no filter, the guards of the
///   `CASE` are `kept AND cond_i`, and a row without match gets `NULL`s, as the join pads it.
///
/// Rows whose key, or any column of the constant side compared with `=`, is a `NULL` literal never
/// match: they are dropped first. When the only conjunct with the constant side is `k = v.c`, the
/// predicate is `k IN (c_1, ..., c_n)` (scan ranges, no `OR`). Every conjunct of the condition must
/// be pure (no `now()`, no volatile function), because `cond_i` is evaluated several times.
///
/// The constant side has no watermark and its rows never change: the result has no join state and
/// no fragment for the constant rows. The rule is only used on demand
/// (`batch_inline_values_join`, `streaming_inline_values_join`).
///
/// [`ConstantRowJoinRule`]: super::ConstantRowJoinRule
pub struct ValuesJoinInlineRule {}

impl ValuesJoinInlineRule {
    pub fn create() -> BoxedRule {
        Box::new(ValuesJoinInlineRule {})
    }
}

impl Rule<Logical> for ValuesJoinInlineRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let join_type = join.join_type();
        let (left, right) = (join.left(), join.right());
        let (left_len, right_len) = (left.schema().len(), right.schema().len());
        // The constant side is the side whose rows are neither preserved nor output on their own.
        let values_is_right = match join_type {
            JoinType::Inner => {
                if constant_rows(&right).is_some() {
                    true
                } else {
                    constant_rows(&left)?;
                    false
                }
            }
            JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => true,
            JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti => false,
            _ => return None,
        };
        let (values, kept) = if values_is_right {
            (right, left)
        } else {
            (left, right)
        };
        let rows = constant_rows(&values)?;
        let on = join.on();
        if on.conjunctions.iter().any(|expr| !expr.is_pure()) {
            return None;
        }
        let sides = Sides {
            values_is_right,
            left_len,
            values_len: values.schema().len(),
        };
        let total = left_len + right_len;
        let (with_values, kept_only): (Vec<&ExprImpl>, Vec<&ExprImpl>) =
            on.conjunctions.iter().partition(|expr| {
                expr.collect_input_refs(total)
                    .ones()
                    .any(|index| sides.values_col(index).is_some())
            });

        // The conjuncts `k = v.c`: (column of the kept side, column of the constant side).
        let equalities: Vec<(InputRef, usize)> = with_values
            .iter()
            .filter_map(|expr| {
                let (a, b) = expr.as_eq_cond()?;
                let (kept_col, values_col) = match (
                    sides.kept_col(a.index()),
                    sides.values_col(b.index()),
                    sides.values_col(a.index()),
                    sides.kept_col(b.index()),
                ) {
                    (Some(k), Some(v), _, _) | (_, _, Some(v), Some(k)) => (k, v),
                    _ => return None,
                };
                (a.return_type() == b.return_type())
                    .then(|| (InputRef::new(kept_col, a.return_type()), values_col))
            })
            .collect();
        // A row with a `NULL` literal compared with `=` never matches.
        let rows: Vec<&Vec<ExprImpl>> = rows
            .iter()
            .filter(|row| equalities.iter().all(|(_, col)| !row[*col].is_null()))
            .collect();
        if rows.is_empty() {
            // Nothing matches: left to the rules that remove empty relations.
            return None;
        }

        let mut kept_rewriter = KeptSideRewriter { sides: &sides };
        let kept_only: Vec<ExprImpl> = kept_only
            .into_iter()
            .map(|expr| kept_rewriter.rewrite_expr(expr.clone()))
            .collect();
        let conds: Vec<ExprImpl> = rows
            .iter()
            .map(|row| {
                let mut rewriter = RowRewriter { sides: &sides, row };
                ExprImpl::and(
                    with_values
                        .iter()
                        .map(|expr| rewriter.rewrite_expr((*expr).clone())),
                )
            })
            .collect();
        // Whether some row matches, given that the conjuncts of the kept side hold.
        let matched = match (with_values.as_slice(), equalities.as_slice()) {
            ([_], [(kept_col, values_col)]) => in_list(
                kept_col,
                rows.iter().map(|row| row[*values_col].clone()).collect(),
            ),
            _ => ExprImpl::or(conds.iter().cloned()),
        };

        let output_indices = join.output_indices();
        match join_type {
            JoinType::LeftSemi | JoinType::RightSemi => {
                let predicate = Condition {
                    conjunctions: kept_only.into_iter().chain([matched]).collect(),
                };
                Some(project_kept(
                    LogicalFilter::create(kept.clone(), predicate),
                    &kept,
                    output_indices,
                ))
            }
            JoinType::LeftAnti | JoinType::RightAnti => {
                let any_match = ExprImpl::and(kept_only.into_iter().chain([matched]));
                let predicate = Condition {
                    conjunctions: vec![is_not_true(any_match)],
                };
                Some(project_kept(
                    LogicalFilter::create(kept.clone(), predicate),
                    &kept,
                    output_indices,
                ))
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter => {
                // A row of the kept side must match at most one row: the key.
                let (key_ref, key_col) = equalities.iter().find(|(kept_col, values_col)| {
                    is_simple_type(&kept_col.return_type())
                        && distinct_literals(rows.iter().map(|row| &row[*values_col]))
                })?;
                let inner = join_type == JoinType::Inner;
                let guards: Vec<ExprImpl> = if inner {
                    conds
                } else {
                    conds
                        .into_iter()
                        .map(|cond| ExprImpl::and(kept_only.iter().cloned().chain([cond])))
                        .collect()
                };
                let values_types = values.schema().data_types();
                let kept_types = kept.schema().data_types();
                let exprs: Vec<ExprImpl> = output_indices
                    .iter()
                    .map(|&index| {
                        if let Some(kept_col) = sides.kept_col(index) {
                            return InputRef::new(kept_col, kept_types[kept_col].clone()).into();
                        }
                        let col = sides.values_col(index).unwrap();
                        if inner && col == *key_col {
                            return key_ref.clone().into();
                        }
                        let column: Vec<&ExprImpl> = rows.iter().map(|row| &row[col]).collect();
                        if inner && column.iter().all(|value| *value == column[0]) {
                            return column[0].clone();
                        }
                        case_of(&guards, &column, values_types[col].clone())
                    })
                    .collect();
                let input = if inner {
                    let predicate = Condition {
                        conjunctions: kept_only.into_iter().chain([matched]).collect(),
                    };
                    LogicalFilter::create(kept, predicate)
                } else {
                    kept
                };
                Some(LogicalProject::create(input, exprs))
            }
            _ => None,
        }
    }
}

/// Returns the rows of a `LogicalValues` of at most [`MAX_ROWS`] rows of constants, read directly
/// or through a share (a common table expression read several times).
fn constant_rows(plan: &PlanRef) -> Option<Vec<Vec<ExprImpl>>> {
    let plan = match plan.as_logical_share() {
        Some(share) => share.input(),
        None => plan.clone(),
    };
    let values = plan.as_logical_values()?;
    let rows = values.rows();
    if rows.is_empty() || rows.len() > MAX_ROWS {
        return None;
    }
    let fields = plan.schema().fields();
    rows.iter()
        .all(|row| {
            row.len() == fields.len()
                && row
                    .iter()
                    .zip_eq_fast(fields.iter())
                    .all(|(expr, field)| expr.is_const() && expr.return_type() == field.data_type)
        })
        .then(|| rows.to_vec())
}

/// Whether the expressions are literals with pairwise distinct values.
fn distinct_literals<'a>(exprs: impl Iterator<Item = &'a ExprImpl>) -> bool {
    let mut seen = vec![];
    for expr in exprs {
        let ExprImpl::Literal(literal) = expr else {
            return false;
        };
        let value = literal.get_data();
        if seen.contains(&value) {
            return false;
        }
        seen.push(value);
    }
    true
}

/// `k IN (v_1, ..., v_n)` without duplicates, or `k = v_1` for a single value.
fn in_list(key: &InputRef, values: Vec<ExprImpl>) -> ExprImpl {
    let mut list: Vec<ExprImpl> = vec![];
    for value in values {
        if !list.contains(&value) {
            list.push(value);
        }
    }
    let key: ExprImpl = key.clone().into();
    if list.len() == 1 {
        let value = list.pop().unwrap();
        return FunctionCall::new_unchecked(ExprType::Equal, vec![key, value], DataType::Boolean)
            .into();
    }
    FunctionCall::new_unchecked(
        ExprType::In,
        [key].into_iter().chain(list).collect(),
        DataType::Boolean,
    )
    .into()
}

fn is_not_true(expr: ExprImpl) -> ExprImpl {
    FunctionCall::new_unchecked(ExprType::IsNotTrue, vec![expr], DataType::Boolean).into()
}

/// `CASE WHEN guard_1 THEN value_1 ... WHEN guard_n THEN value_n END`, or `NULL` when every value
/// is `NULL`.
fn case_of(guards: &[ExprImpl], values: &[&ExprImpl], return_type: DataType) -> ExprImpl {
    if values.iter().all(|value| value.is_null()) {
        return ExprImpl::literal_null(return_type);
    }
    let mut inputs = vec![];
    for (guard, value) in guards.iter().zip_eq_fast(values) {
        inputs.push(guard.clone());
        inputs.push((*value).clone());
    }
    FunctionCall::new_unchecked(ExprType::Case, inputs, return_type).into()
}

/// The output of a semi or anti join, whose output indices refer to the columns of its kept side.
fn project_kept(input: PlanRef, kept: &PlanRef, output_indices: &[usize]) -> PlanRef {
    if output_indices.iter().copied().eq(0..kept.schema().len()) {
        return input;
    }
    let types = kept.schema().data_types();
    let exprs = output_indices
        .iter()
        .map(|&index| InputRef::new(index, types[index].clone()).into())
        .collect();
    LogicalProject::create(input, exprs)
}

/// The columns of the two sides of the join, in the condition (left columns, then right ones)
/// and in the output indices of an inner or outer join, which index the same columns.
struct Sides {
    values_is_right: bool,
    left_len: usize,
    values_len: usize,
}

impl Sides {
    /// The column of the constant side, if the column of the join is one.
    fn values_col(&self, index: usize) -> Option<usize> {
        if self.values_is_right {
            index.checked_sub(self.left_len)
        } else {
            (index < self.values_len).then_some(index)
        }
    }

    /// The column of the kept side, if the column of the join is one.
    fn kept_col(&self, index: usize) -> Option<usize> {
        if self.values_is_right {
            (index < self.left_len).then_some(index)
        } else {
            index.checked_sub(self.values_len)
        }
    }
}

/// Rewrites a conjunct of the kept side only over the kept side.
struct KeptSideRewriter<'a> {
    sides: &'a Sides,
}

impl ExprRewriter for KeptSideRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let index = self
            .sides
            .kept_col(input_ref.index())
            .expect("a conjunct of the kept side only");
        InputRef::new(index, input_ref.return_type()).into()
    }
}

/// Rewrites a conjunct over the kept side, with the values of a constant row in place of the
/// columns of the constant side.
struct RowRewriter<'a> {
    sides: &'a Sides,
    row: &'a [ExprImpl],
}

impl ExprRewriter for RowRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        match self.sides.values_col(input_ref.index()) {
            Some(col) => self.row[col].clone(),
            None => InputRef::new(
                self.sides.kept_col(input_ref.index()).unwrap(),
                input_ref.return_type(),
            )
            .into(),
        }
    }
}
