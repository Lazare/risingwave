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

use std::collections::HashSet;

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::plan_node::LogicalValues;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::utils::Condition;

/// Replaces a `LogicalFilter` or a `LogicalScan` whose predicate contains both `x IS NULL` and a
/// null-rejecting condition on the same column `x` with an empty `LogicalValues`.
///
/// # Soundness
///
/// The rule only looks at the *top-level conjuncts* of a predicate that is evaluated with filter
/// semantics, i.e. a row is kept iff the whole conjunction evaluates to `TRUE`; `NULL` and `FALSE`
/// both drop the row. This holds for the predicate of `LogicalFilter` and for the predicate of
/// `LogicalScan` (which is lowered to scan ranges plus a `BatchFilter`).
///
/// For any row, either `x` is not `NULL`, then the conjunct `x IS NULL` is `FALSE`; or `x` is
/// `NULL`, then the null-rejecting conjunct is `NULL` or `FALSE` (see [`null_rejected_columns`]).
/// In both cases one conjunct is not `TRUE`, so the conjunction is not `TRUE` and the row is
/// dropped. Hence no row can pass and the node produces an empty relation.
///
/// The rule never descends into expressions, so the pattern is not recognized under `NOT`, `OR`,
/// `CASE`, `COALESCE`, etc., and it never touches projected expressions or join conditions.
///
/// The rule is only used by the batch optimizer.
pub struct ContradictoryNullFilterRule;

impl Rule<Logical> for ContradictoryNullFilterRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let predicate = if let Some(filter) = plan.as_logical_filter() {
            filter.predicate()
        } else {
            plan.as_logical_scan()?.predicate()
        };
        if is_null_contradiction(predicate) {
            Some(LogicalValues::create(
                vec![],
                plan.schema().clone(),
                plan.ctx(),
            ))
        } else {
            None
        }
    }
}

impl ContradictoryNullFilterRule {
    pub fn create() -> BoxedRule {
        Box::new(ContradictoryNullFilterRule)
    }
}

/// Returns whether the conjunction contains `IsNull(#i)` together with a conjunct that can not be
/// `TRUE` when `#i` is `NULL`.
pub(super) fn is_null_contradiction(predicate: &Condition) -> bool {
    let is_null_columns: HashSet<usize> = predicate
        .conjunctions
        .iter()
        .filter_map(is_null_column)
        .collect();
    if is_null_columns.is_empty() {
        return false;
    }
    predicate
        .conjunctions
        .iter()
        .flat_map(null_rejected_columns)
        .any(|idx| is_null_columns.contains(&idx))
}

/// Returns `Some(i)` iff the expression is exactly `IsNull(#i)`.
fn is_null_column(expr: &ExprImpl) -> Option<usize> {
    let call = expr.as_function_call()?;
    if call.func_type() != ExprType::IsNull {
        return None;
    }
    match call.inputs() {
        [input] => Some(input.as_input_ref()?.index()),
        _ => None,
    }
}

/// Returns the columns `#i` for which the expression is guaranteed to evaluate to `NULL` or
/// `FALSE` (never `TRUE`) whenever `#i` is `NULL`.
///
/// This is an explicit allow-list. Only a *direct* column reference as an immediate argument of
/// the following functions is accepted:
///
/// - `IsNotNull(#i)`: `FALSE` when `#i` is `NULL` by definition.
/// - `#i <cmp> e` / `e <cmp> #i` with `<cmp>` in `=`, `<>`, `<`, `<=`, `>`, `>=`: all
///   implementations of these functions (`risingwave_expr_impl::scalar::cmp`) are strict, i.e.
///   they return `NULL` when any argument is `NULL`.
/// - `In(#i, c1, c2, ...)`: `InExpression` returns `NULL` when the left side is `NULL`.
///
/// Anything else (`IS [NOT] DISTINCT FROM`, `= ANY(array)`, a column wrapped in a cast or in any
/// other function, ...) is deliberately not recognized.
pub(super) fn null_rejected_columns(expr: &ExprImpl) -> Vec<usize> {
    let Some(call) = expr.as_function_call() else {
        return vec![];
    };
    let input_ref_index = |e: &ExprImpl| e.as_input_ref().map(|i| i.index());
    match (call.func_type(), call.inputs()) {
        (ExprType::IsNotNull, [input]) => input_ref_index(input).into_iter().collect(),
        (
            ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual,
            [left, right],
        ) => [left, right]
            .into_iter()
            .filter_map(input_ref_index)
            .collect(),
        (ExprType::In, [left, ..]) => input_ref_index(left).into_iter().collect(),
        _ => vec![],
    }
}
