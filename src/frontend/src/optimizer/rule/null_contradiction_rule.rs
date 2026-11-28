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

use risingwave_common::types::ScalarImpl;

use super::never_true_predicate_rule::{
    fold_join_without_match, has_temporal_lookup, may_carry_watermark,
};
use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType, FunctionCall, is_impure_func_call};
use crate::optimizer::plan_node::LogicalValues;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::utils::Condition;

/// Removes the work that depends on a conjunction holding both `E IS NULL` and a conjunct that
/// can not be `TRUE` when `E` is `NULL`, for any deterministic expression `E` (a column, a
/// `COALESCE` of several columns, a cast...), compared structurally.
///
/// ```text
/// Filter(coalesce(a, b) IS NULL AND coalesce(a, b) ~ '^[a-z]+$')       Values([])
/// └─Scan(t)                                                      =>
/// ```
///
/// - A `LogicalFilter` or a `LogicalScan` (`scan_ranges: [c IS NULL]` and a filter on `c`) with
///   such a predicate becomes an empty `LogicalValues`.
/// - A `LogicalJoin` with such a condition never matches a pair of rows, and is rewritten as
///   [`NeverTruePredicateRule`] rewrites a join whose condition is never true (see
///   [`fold_join_without_match`]).
///
/// The empty relations are then propagated up by `EmptyRelationPropagateRule`.
///
/// This generalizes `is_null_contradiction` (`ContradictoryNullFilterRule`,
/// `NeverTruePredicateRule`), which only knows a direct column compared with `=`, `<`, ..., `IN`
/// or tested with `IS NOT NULL`. See [`has_null_contradiction`] for what is recognized.
///
/// # Correctness
///
/// The predicates of a filter, of a scan and of a join are evaluated with filter semantics: a
/// row (a pair of rows) is kept iff the whole conjunction is `TRUE`; `NULL` and `FALSE` both drop
/// it. For any row, either `E` is not `NULL`, and `E IS NULL` is `FALSE`; or `E` is `NULL`, and the
/// other conjunct is `NULL` or `FALSE` (see [`not_true_when_null`]). One conjunct is not `TRUE` in
/// both cases, so no row passes. `E` evaluates to the same value in both conjuncts because it only
/// holds columns, constants and pure functions. In a streaming job, an expression that fails is
/// `NULL` (each function call is wrapped so); a failure of `E` makes it `NULL` in both places,
/// which is still covered. The rewrite holds for every content of the inputs, so it holds for the
/// changes of a streaming job too.
///
/// For a streaming job (`create_for_stream`), a part of the plan that may carry a watermark, and a
/// filter or a scan that reads a table `AS OF`, are never replaced, as in
/// [`NeverTruePredicateRule`].
///
/// [`NeverTruePredicateRule`]: super::NeverTruePredicateRule
pub struct NullContradictionRule {
    for_stream: bool,
}

impl Rule<Logical> for NullContradictionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if let Some(join) = plan.as_logical_join() {
            if !has_null_contradiction(join.on()) {
                return None;
            }
            return fold_join_without_match(&plan, join, self.for_stream);
        }
        let predicate = if let Some(filter) = plan.as_logical_filter() {
            filter.predicate()
        } else {
            plan.as_logical_scan()?.predicate()
        };
        if !has_null_contradiction(predicate)
            || (self.for_stream && (may_carry_watermark(&plan) || has_temporal_lookup(&plan)))
        {
            return None;
        }
        Some(LogicalValues::create(
            vec![],
            plan.schema().clone(),
            plan.ctx(),
        ))
    }
}

impl NullContradictionRule {
    /// For batch queries.
    pub fn create() -> BoxedRule {
        Box::new(NullContradictionRule { for_stream: false })
    }

    /// For streaming jobs: the parts of the plan that may carry a watermark are kept.
    pub fn create_for_stream() -> BoxedRule {
        Box::new(NullContradictionRule { for_stream: true })
    }
}

/// Returns whether the conjunction holds a conjunct `E IS NULL` (or `NOT (E IS NOT NULL)`), with
/// `E` deterministic (see [`is_deterministic`]), and another conjunct that can not be `TRUE` when
/// `E` is `NULL` (see [`not_true_when_null`]).
///
/// When `E` is `COALESCE(e1, e2, ...)`, `GREATEST(...)` or `LEAST(...)`, each argument is `NULL`
/// too when `E` is (these functions are `NULL` only when all their arguments are), recursively: a
/// conjunct that can not be `TRUE` when one of them is `NULL` contradicts `E IS NULL` as well.
pub(super) fn has_null_contradiction(predicate: &Condition) -> bool {
    let conjuncts = &predicate.conjunctions;
    conjuncts.iter().enumerate().any(|(i, conjunct)| {
        let Some(tested) = is_null_operand(conjunct) else {
            return false;
        };
        if !is_deterministic(tested) {
            return false;
        }
        let mut null_exprs = vec![];
        collect_null_exprs(tested, &mut null_exprs);
        conjuncts
            .iter()
            .enumerate()
            .any(|(j, other)| j != i && not_true_when_null(other, &null_exprs))
    })
}

/// Returns `E` iff the expression is `E IS NULL` or `NOT (E IS NOT NULL)`.
fn is_null_operand(expr: &ExprImpl) -> Option<&ExprImpl> {
    let call = expr.as_function_call()?;
    match (call.func_type(), call.inputs()) {
        (ExprType::IsNull, [input]) => Some(input),
        (ExprType::Not, [input]) => {
            let inner = input.as_function_call()?;
            match (inner.func_type(), inner.inputs()) {
                (ExprType::IsNotNull, [input]) => Some(input),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Whether an expression only holds columns, constants and calls of pure built-in functions, so
/// that it evaluates to the same value wherever it appears in the predicate of a row. Anything
/// else (user-defined functions, subqueries, `now()`, correlated references, ...) is refused.
pub(super) fn is_deterministic(expr: &ExprImpl) -> bool {
    match expr {
        ExprImpl::InputRef(_) | ExprImpl::Literal(_) => true,
        ExprImpl::FunctionCall(call) => {
            !is_impure_func_call(call) && call.inputs().iter().all(is_deterministic)
        }
        _ => false,
    }
}

/// The expressions that are `NULL` whenever `expr` is: `expr` itself and, through `COALESCE`,
/// `GREATEST` and `LEAST`, each of their arguments.
fn collect_null_exprs<'a>(expr: &'a ExprImpl, out: &mut Vec<&'a ExprImpl>) {
    out.push(expr);
    if let Some(call) = expr.as_function_call()
        && matches!(
            call.func_type(),
            ExprType::Coalesce | ExprType::Greatest | ExprType::Least
        )
    {
        for input in call.inputs() {
            collect_null_exprs(input, out);
        }
    }
}

/// Whether a conjunct can not be `TRUE` on a row where all of `null_exprs` are `NULL`:
///
/// - it is `NULL` then, see [`null_when_null`];
/// - the constant `FALSE`;
/// - `a AND b` with `a` or `b` not `TRUE`, `a OR b` with both not `TRUE`, `a IS TRUE` with `a` not
///   `TRUE` (`NULL` and `FALSE` are both not `TRUE`);
/// - `e IS NOT NULL`, `NOT (e IS NULL)`, or `e IS NOT DISTINCT FROM c` (`c` a non-`NULL`
///   constant), with `e` `NULL` then.
///
/// `NOT`, `CASE`, `COALESCE` and the other functions are only looked into through
/// [`null_when_null`]: e.g. `NOT (e IS NULL)` is recognized, `NOT (e IS NOT NULL)` is not.
fn not_true_when_null(expr: &ExprImpl, null_exprs: &[&ExprImpl]) -> bool {
    if null_when_null(expr, null_exprs) {
        return true;
    }
    if let ExprImpl::Literal(literal) = expr {
        return *literal.get_data() == Some(ScalarImpl::Bool(false));
    }
    let Some(call) = expr.as_function_call() else {
        return false;
    };
    let is_non_null_constant = |e: &ExprImpl| {
        e.as_literal()
            .is_some_and(|literal| literal.get_data().is_some())
    };
    match (call.func_type(), call.inputs()) {
        (ExprType::And, inputs) => inputs.iter().any(|e| not_true_when_null(e, null_exprs)),
        (ExprType::Or, inputs) => {
            !inputs.is_empty() && inputs.iter().all(|e| not_true_when_null(e, null_exprs))
        }
        (ExprType::IsTrue, [input]) => not_true_when_null(input, null_exprs),
        (ExprType::IsNotNull, [input]) => null_when_null(input, null_exprs),
        (ExprType::Not, [input]) => input.as_function_call().is_some_and(|inner| {
            inner.func_type() == ExprType::IsNull
                && matches!(inner.inputs(), [e] if null_when_null(e, null_exprs))
        }),
        (ExprType::IsNotDistinctFrom, [a, b]) => {
            (null_when_null(a, null_exprs) && is_non_null_constant(b))
                || (null_when_null(b, null_exprs) && is_non_null_constant(a))
        }
        _ => false,
    }
}

/// Whether an expression is surely `NULL` on a row where all of `null_exprs` are `NULL`:
///
/// - it is one of `null_exprs` (structural equality), or the constant `NULL`;
/// - a call of a strict function (see [`is_strict`]) with such an argument;
/// - `e IN (...)` with `e` such an expression;
/// - `COALESCE`, `GREATEST` or `LEAST` whose arguments are all such expressions.
///
/// `false` means unknown.
pub(super) fn null_when_null(expr: &ExprImpl, null_exprs: &[&ExprImpl]) -> bool {
    if null_exprs.contains(&expr) {
        return true;
    }
    match expr {
        ExprImpl::Literal(literal) => literal.get_data().is_none(),
        ExprImpl::FunctionCall(call) => null_when_null_call(call, null_exprs),
        _ => false,
    }
}

fn null_when_null_call(call: &FunctionCall, null_exprs: &[&ExprImpl]) -> bool {
    let func_type = call.func_type();
    let inputs = call.inputs();
    if is_strict(func_type) {
        return inputs.iter().any(|e| null_when_null(e, null_exprs));
    }
    match func_type {
        ExprType::In => inputs
            .first()
            .is_some_and(|e| null_when_null(e, null_exprs)),
        ExprType::Coalesce | ExprType::Greatest | ExprType::Least => {
            !inputs.is_empty() && inputs.iter().all(|e| null_when_null(e, null_exprs))
        }
        _ => false,
    }
}

/// The built-in functions that return `NULL` whenever one of their arguments is `NULL`, for
/// every signature: they are declared in `risingwave_expr_impl` with non-optional arguments,
/// which makes the generated code return `NULL` without calling the function when an argument is
/// `NULL`. An explicit allow-list: a function missing here is only not recognized.
fn is_strict(func_type: ExprType) -> bool {
    matches!(
        func_type,
        // Comparisons and boolean negation.
        ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
            | ExprType::Not
            // Arithmetic.
            | ExprType::Add
            | ExprType::Subtract
            | ExprType::Multiply
            | ExprType::Divide
            | ExprType::Modulus
            | ExprType::Neg
            | ExprType::Abs
            | ExprType::Ceil
            | ExprType::Floor
            | ExprType::Extract
            // Casts.
            | ExprType::Cast
            // Strings and patterns.
            | ExprType::Like
            | ExprType::ILike
            | ExprType::RegexpEq
            | ExprType::StartsWith
            | ExprType::Upper
            | ExprType::Lower
            | ExprType::Trim
            | ExprType::Ltrim
            | ExprType::Rtrim
            | ExprType::Substr
            | ExprType::Length
            | ExprType::CharLength
            | ExprType::OctetLength
            | ExprType::Replace
            | ExprType::SplitPart
            | ExprType::Position
            | ExprType::Md5
            | ExprType::ConcatOp
            // JSON access (`->`, `->>`).
            | ExprType::JsonbAccess
            | ExprType::JsonbAccessStr
    )
}
