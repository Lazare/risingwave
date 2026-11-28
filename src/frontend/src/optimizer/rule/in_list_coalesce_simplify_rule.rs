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

use risingwave_common::types::{DataType, ScalarImpl};

use super::never_true_predicate_rule::{constant_value, is_simple_type, typed_constant};
use super::null_contradiction_rule::is_deterministic;
use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall};
use crate::optimizer::plan_node::{ExprRewritable, LogicalFilter, PlanTreeNodeUnary};
use crate::utils::Condition;

/// Two small simplifications of the expressions of a plan node:
///
/// 1. In a conjunction evaluated with filter semantics (the predicate of a filter or of a scan,
///    the condition of a join), the conjuncts that restrict the same expression to lists of
///    constants are intersected: `c IN (1, 2) AND c IN (1, 3, 4)` becomes `c = 1`, and an empty
///    intersection becomes `FALSE` (the never-true predicate rules then remove the work).
/// 2. In every expression of the node, the arguments of `COALESCE` that repeat an earlier
///    argument are removed: `coalesce(x, x, y, x)` becomes `coalesce(x, y)`, and `coalesce(x, x)`
///    becomes `x`.
///
/// # Correctness
///
/// 1. A row is kept iff every conjunct is `TRUE`. `e IN (v1, ...)` (and `e = v`) is `TRUE` iff
///    `e` is not `NULL` and equal to one of the non-`NULL` values; a `NULL` item only turns a
///    `FALSE` into a `NULL`, which drops the row as well. So the conjuncts on `e` are all `TRUE`
///    iff `e` is equal to a value of every list, i.e. to a value of their intersection. Only for
///    the types whose equal values are identical ([`is_simple_type`]), with constants of exactly
///    the type of `e`, and a deterministic `e` (columns, constants, pure functions) that has the
///    same value in every conjunct.
/// 2. `COALESCE` returns its first non-`NULL` argument. A later argument equal to an earlier one
///    is only evaluated when the earlier one was `NULL`; deterministic, it is `NULL` again, so
///    skipping it changes nothing. In a streaming job a failing argument is `NULL`, both times.
///
/// Both hold for every row, so for the changes of a streaming job too.
pub struct InListCoalesceSimplifyRule;

impl Rule<Logical> for InListCoalesceSimplifyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let mut dedup = CoalesceDedup { changed: false };
        let rewritten = plan.rewrite_exprs(&mut dedup);
        let mut changed = dedup.changed;
        let plan = if changed { rewritten } else { plan };

        let intersected = if let Some(filter) = plan.as_logical_filter() {
            intersect_in_lists(filter.predicate())
                .map(|predicate| LogicalFilter::create(filter.input(), predicate))
        } else if let Some(scan) = plan.as_logical_scan() {
            intersect_in_lists(scan.predicate())
                .map(|predicate| scan.clone_with_predicate(predicate).into())
        } else if let Some(join) = plan.as_logical_join() {
            intersect_in_lists(join.on()).map(|on| join.clone_with_cond(on).into())
        } else {
            None
        };
        let plan = match intersected {
            Some(intersected) => {
                changed = true;
                intersected
            }
            None => plan,
        };
        changed.then_some(plan)
    }
}

impl InListCoalesceSimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(InListCoalesceSimplifyRule)
    }
}

/// Removes the arguments of `COALESCE` that repeat an earlier argument, see 2. of
/// [`InListCoalesceSimplifyRule`].
struct CoalesceDedup {
    changed: bool,
}

impl ExprRewriter for CoalesceDedup {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();
        let inputs: Vec<ExprImpl> = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        if func_type != ExprType::Coalesce {
            return FunctionCall::new_unchecked(func_type, inputs, ret).into();
        }
        let mut kept: Vec<ExprImpl> = Vec::with_capacity(inputs.len());
        let mut removed = false;
        for input in inputs {
            if kept.contains(&input) && is_deterministic(&input) {
                removed = true;
            } else {
                kept.push(input);
            }
        }
        if !removed {
            return FunctionCall::new_unchecked(func_type, kept, ret).into();
        }
        self.changed = true;
        if kept.len() == 1 && kept[0].return_type() == ret {
            return kept.pop().unwrap();
        }
        FunctionCall::new_unchecked(func_type, kept, ret).into()
    }
}

/// A conjunct that restricts an expression to a list of constants: `e IN (v1, ...)` or `e = v`.
struct InList {
    /// The restricted expression.
    tested: ExprImpl,
    /// The non-`NULL` values, with the constant expressions that hold them.
    values: Vec<(ScalarImpl, ExprImpl)>,
}

/// Returns the list of a conjunct `e IN (c1, ...)`, `e = c` or `c = e`, where `e` is a
/// deterministic expression of a simple type that is not a constant, and the `c` are constants of
/// exactly the same type (`NULL` items are dropped).
fn in_list(expr: &ExprImpl) -> Option<InList> {
    let call = expr.as_function_call()?;
    let tested_type = |e: &ExprImpl| -> Option<DataType> {
        let data_type = e.return_type();
        (is_simple_type(&data_type) && is_deterministic(e) && constant_value(e).is_none())
            .then_some(data_type)
    };
    match (call.func_type(), call.inputs()) {
        (ExprType::In, [tested, items @ ..]) => {
            let data_type = tested_type(tested)?;
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                if let Some(value) = typed_constant(item, &data_type)? {
                    values.push((value, item.clone()));
                }
            }
            Some(InList {
                tested: tested.clone(),
                values,
            })
        }
        (ExprType::Equal, [left, right]) => {
            let (tested, constant) = if constant_value(right).is_some() {
                (left, right)
            } else {
                (right, left)
            };
            let data_type = tested_type(tested)?;
            let value = typed_constant(constant, &data_type)??;
            Some(InList {
                tested: tested.clone(),
                values: vec![(value, constant.clone())],
            })
        }
        _ => None,
    }
}

/// Intersects the conjuncts that restrict the same expression to lists of constants, see 1. of
/// [`InListCoalesceSimplifyRule`]. `None` if no expression is restricted by two conjuncts.
fn intersect_in_lists(predicate: &Condition) -> Option<Condition> {
    let conjuncts = &predicate.conjunctions;
    // The restricted expressions, with the positions of their conjuncts and their lists.
    let mut groups: Vec<(ExprImpl, Vec<usize>, Vec<InList>)> = vec![];
    for (i, conjunct) in conjuncts.iter().enumerate() {
        let Some(list) = in_list(conjunct) else {
            continue;
        };
        match groups
            .iter_mut()
            .find(|(tested, _, _)| *tested == list.tested)
        {
            Some((_, positions, lists)) => {
                positions.push(i);
                lists.push(list);
            }
            None => groups.push((list.tested.clone(), vec![i], vec![list])),
        }
    }
    let mut replaced: Vec<Option<ExprImpl>> = conjuncts.iter().cloned().map(Some).collect();
    let mut changed = false;
    for (tested, positions, lists) in groups {
        if positions.len() < 2 {
            continue;
        }
        changed = true;
        let (first, rest) = lists.split_first().unwrap();
        let mut kept: Vec<(ScalarImpl, ExprImpl)> = vec![];
        for (value, expr) in &first.values {
            if kept.iter().any(|(v, _)| v == value) {
                continue;
            }
            if rest
                .iter()
                .all(|list| list.values.iter().any(|(v, _)| v == value))
            {
                kept.push((value.clone(), expr.clone()));
            }
        }
        let merged: ExprImpl = match kept.len() {
            0 => ExprImpl::literal_bool(false),
            1 => FunctionCall::new_unchecked(
                ExprType::Equal,
                vec![tested, kept.pop().unwrap().1],
                DataType::Boolean,
            )
            .into(),
            _ => FunctionCall::new_unchecked(
                ExprType::In,
                std::iter::once(tested)
                    .chain(kept.into_iter().map(|(_, expr)| expr))
                    .collect(),
                DataType::Boolean,
            )
            .into(),
        };
        replaced[positions[0]] = Some(merged);
        for &position in &positions[1..] {
            replaced[position] = None;
        }
    }
    changed.then(|| Condition {
        conjunctions: replaced.into_iter().flatten().collect(),
    })
}
