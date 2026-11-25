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

use std::cmp::Ordering;
use std::collections::BTreeMap;

use risingwave_common::types::{DataType, DefaultOrd, ScalarImpl};
use risingwave_pb::plan_common::JoinType;

use super::contradictory_null_filter_rule::is_null_contradiction;
use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalJoin, LogicalValues, PlanTreeNodeBinary};
use crate::utils::Condition;

/// Removes the work that depends on a predicate that can never be `TRUE`.
///
/// - A `LogicalFilter` or a `LogicalScan` with such a predicate becomes an empty `LogicalValues`.
/// - A `LogicalJoin` with such a condition never matches a row:
///   - `Inner`, `LeftSemi`, `RightSemi`: the join becomes an empty `LogicalValues`;
///   - `LeftOuter`, `LeftAnti`: the right input is replaced with an empty `LogicalValues`;
///   - `RightOuter`, `RightAnti`: the left input is replaced with an empty `LogicalValues`;
///   - `FullOuter` and as-of joins are left untouched.
///
/// See [`is_never_true`] for the predicates that are recognized.
pub struct NeverTruePredicateRule;

impl Rule<Logical> for NeverTruePredicateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if let Some(join) = plan.as_logical_join() {
            return Self::apply_join(&plan, join);
        }
        let predicate = if let Some(filter) = plan.as_logical_filter() {
            filter.predicate()
        } else {
            plan.as_logical_scan()?.predicate()
        };
        is_never_true(predicate).then(|| empty_like(&plan))
    }
}

impl NeverTruePredicateRule {
    pub fn create() -> BoxedRule {
        Box::new(NeverTruePredicateRule)
    }

    fn apply_join(plan: &PlanRef, join: &LogicalJoin) -> Option<PlanRef> {
        if !is_never_true(join.on()) {
            return None;
        }
        let (left, right) = (join.left(), join.right());
        match join.join_type() {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => Some(empty_like(plan)),
            JoinType::LeftOuter | JoinType::LeftAnti if !is_empty(&right) => {
                Some(join.clone_with_left_right(left, empty_like(&right)).into())
            }
            JoinType::RightOuter | JoinType::RightAnti if !is_empty(&left) => {
                Some(join.clone_with_left_right(empty_like(&left), right).into())
            }
            _ => None,
        }
    }
}

fn is_empty(plan: &PlanRef) -> bool {
    plan.as_logical_values()
        .is_some_and(|values| values.rows().is_empty())
}

fn empty_like(plan: &PlanRef) -> PlanRef {
    LogicalValues::create(vec![], plan.schema().clone(), plan.ctx())
}

/// Returns whether a predicate evaluated with filter semantics (a row is kept iff the whole
/// conjunction is `TRUE`; `NULL` and `FALSE` both drop it) can never be `TRUE`, whatever the row.
///
/// Only the *top-level conjuncts* are inspected, and one of the following must hold:
///
/// 1. A conjunct is a constant expression that evaluates to `FALSE` or `NULL`, e.g. `1 = 0`, or
///    `'b' = 'a'` once a filter on a constant column is pushed into a branch of a `UNION ALL`.
/// 2. A conjunct is a comparison with a constant `NULL`, e.g. `x = NULL`: all the comparison
///    functions are strict, so the conjunct is always `NULL`.
/// 3. `x IS NULL` comes with a conjunct that rejects `NULL` for the same column, see
///    [`is_null_contradiction`].
/// 4. The conjuncts that compare one column with constants can not hold together, e.g.
///    `x = 1 AND x = 2`, `x IN (1, 2) AND x IN (3)`, `x = 1 AND x <> 1`, `x > 5 AND x < 3`, see
///    [`has_value_contradiction`].
///
/// A constant expression only holds literals and pure functions, so that `now()`, volatile
/// functions and user-defined functions that are not immutable are never evaluated here. A
/// constant expression that fails to evaluate is ignored.
pub(super) fn is_never_true(predicate: &Condition) -> bool {
    predicate.conjunctions.iter().any(is_never_true_conjunct)
        || is_null_contradiction(predicate)
        || has_value_contradiction(predicate)
}

/// Returns the value of a constant expression: `None` if the expression is not constant or fails
/// to evaluate, `Some(None)` for a constant `NULL`.
fn constant_value(expr: &ExprImpl) -> Option<Option<ScalarImpl>> {
    expr.try_fold_const()?.ok()
}

fn is_comparison(func_type: ExprType) -> bool {
    matches!(
        func_type,
        ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
    )
}

fn is_never_true_conjunct(expr: &ExprImpl) -> bool {
    match constant_value(expr) {
        Some(None) | Some(Some(ScalarImpl::Bool(false))) => return true,
        Some(Some(_)) => return false,
        None => {}
    }
    // A strict comparison with a constant `NULL` is `NULL`, whatever the other side.
    expr.as_function_call().is_some_and(|call| {
        is_comparison(call.func_type())
            && call.inputs().len() == 2
            && call
                .inputs()
                .iter()
                .any(|input| constant_value(input) == Some(None))
    })
}

/// The values a column may take according to the conjuncts that compare it with constants.
#[derive(Default)]
struct Domain {
    /// One entry per `=` or `IN` conjunct: the column must be equal to one of the values.
    allowed: Vec<Vec<ScalarImpl>>,
    /// The values of the `<>` and `NOT IN` conjuncts.
    excluded: Vec<ScalarImpl>,
    /// The tightest lower bound, and whether it is inclusive.
    lower: Option<(ScalarImpl, bool)>,
    /// The tightest upper bound, and whether it is inclusive.
    upper: Option<(ScalarImpl, bool)>,
}

impl Domain {
    fn add_lower(&mut self, value: ScalarImpl, inclusive: bool) {
        self.lower = Some(match self.lower.take() {
            None => (value, inclusive),
            Some((current, current_inclusive)) => match value.default_cmp(&current) {
                Ordering::Greater => (value, inclusive),
                Ordering::Equal => (current, current_inclusive && inclusive),
                Ordering::Less => (current, current_inclusive),
            },
        });
    }

    fn add_upper(&mut self, value: ScalarImpl, inclusive: bool) {
        self.upper = Some(match self.upper.take() {
            None => (value, inclusive),
            Some((current, current_inclusive)) => match value.default_cmp(&current) {
                Ordering::Less => (value, inclusive),
                Ordering::Equal => (current, current_inclusive && inclusive),
                Ordering::Greater => (current, current_inclusive),
            },
        });
    }

    /// Whether a value satisfies the bounds and the exclusions.
    fn accepts(&self, value: &ScalarImpl) -> bool {
        !self.excluded.contains(value)
            && self
                .lower
                .as_ref()
                .is_none_or(|(bound, inclusive)| match value.default_cmp(bound) {
                    Ordering::Greater => true,
                    Ordering::Equal => *inclusive,
                    Ordering::Less => false,
                })
            && self
                .upper
                .as_ref()
                .is_none_or(|(bound, inclusive)| match value.default_cmp(bound) {
                    Ordering::Less => true,
                    Ordering::Equal => *inclusive,
                    Ordering::Greater => false,
                })
    }

    /// Whether no value satisfies all the constraints. `false` when unsure: the types are not
    /// assumed to be discrete, so that `x > 1 AND x < 2` is not reported.
    fn is_empty(&self) -> bool {
        if let Some((first, rest)) = self.allowed.split_first() {
            return !first
                .iter()
                .any(|value| rest.iter().all(|set| set.contains(value)) && self.accepts(value));
        }
        match (&self.lower, &self.upper) {
            (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
                match lower.default_cmp(upper) {
                    Ordering::Greater => true,
                    Ordering::Equal => {
                        !(*lower_inclusive && *upper_inclusive) || self.excluded.contains(lower)
                    }
                    Ordering::Less => false,
                }
            }
            _ => false,
        }
    }
}

/// The types for which the equality and the order of `ScalarImpl` are those of the SQL operators,
/// and for which two equal values are indistinguishable.
fn is_simple_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Varchar
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
    )
}

/// Returns `(i, type)` iff the expression is exactly the column `#i` of a simple type.
fn simple_column(expr: &ExprImpl) -> Option<(usize, DataType)> {
    let input_ref = expr.as_input_ref()?;
    let data_type = input_ref.return_type();
    is_simple_type(&data_type).then(|| (input_ref.index(), data_type))
}

/// Returns the value of a constant expression of the given type; `Some(None)` for `NULL`.
fn typed_constant(expr: &ExprImpl, data_type: &DataType) -> Option<Option<ScalarImpl>> {
    if expr.return_type() != *data_type {
        return None;
    }
    constant_value(expr)
}

/// Returns the non-`NULL` values of a list of constants of the given type, or `None` if an item
/// is not such a constant. The second value tells whether the list holds a `NULL`.
fn typed_constants(exprs: &[ExprImpl], data_type: &DataType) -> Option<(Vec<ScalarImpl>, bool)> {
    let mut values = Vec::with_capacity(exprs.len());
    let mut has_null = false;
    for expr in exprs {
        match typed_constant(expr, data_type)? {
            Some(value) => values.push(value),
            None => has_null = true,
        }
    }
    Some((values, has_null))
}

/// Records what a conjunct says about a column, if it has one of the recognized forms. Anything
/// else is ignored, which is always safe: a missing constraint only widens the domain.
///
/// The column must be a *direct* reference to a column of a simple type (see
/// [`is_simple_type`]), compared with constants of exactly the same type:
///
/// - `#i <cmp> c` and `c <cmp> #i`, with `<cmp>` in `=`, `<>`, `<`, `<=`, `>`, `>=`;
/// - `#i IN (c1, c2, ...)`: a `NULL` item never makes the conjunct `TRUE` and is dropped;
/// - `NOT (#i IN (c1, c2, ...))`, only without a `NULL` item;
/// - a boolean column `#i` alone, and `NOT #i`.
fn collect_constraint(expr: &ExprImpl, domains: &mut BTreeMap<usize, Domain>) {
    if let Some((index, data_type)) = simple_column(expr) {
        if data_type == DataType::Boolean {
            let domain = domains.entry(index).or_default();
            domain.allowed.push(vec![ScalarImpl::Bool(true)]);
        }
        return;
    }
    let Some(call) = expr.as_function_call() else {
        return;
    };
    match (call.func_type(), call.inputs()) {
        (ExprType::Not, [input]) => {
            if let Some((index, DataType::Boolean)) = simple_column(input) {
                let domain = domains.entry(index).or_default();
                domain.allowed.push(vec![ScalarImpl::Bool(false)]);
            } else if let Some(inner) = input.as_function_call()
                && inner.func_type() == ExprType::In
                && let [left, list @ ..] = inner.inputs()
                && let Some((index, data_type)) = simple_column(left)
                && let Some((values, false)) = typed_constants(list, &data_type)
            {
                domains.entry(index).or_default().excluded.extend(values);
            }
        }
        (ExprType::In, [left, list @ ..]) => {
            if let Some((index, data_type)) = simple_column(left)
                && let Some((values, _)) = typed_constants(list, &data_type)
            {
                domains.entry(index).or_default().allowed.push(values);
            }
        }
        (func_type, [left, right]) if is_comparison(func_type) => {
            // Normalize to `#i <cmp> c`.
            let (index, value, func_type) = if let Some((index, data_type)) = simple_column(left)
                && let Some(Some(value)) = typed_constant(right, &data_type)
            {
                (index, value, func_type)
            } else if let Some((index, data_type)) = simple_column(right)
                && let Some(Some(value)) = typed_constant(left, &data_type)
            {
                let flipped = match func_type {
                    ExprType::LessThan => ExprType::GreaterThan,
                    ExprType::LessThanOrEqual => ExprType::GreaterThanOrEqual,
                    ExprType::GreaterThan => ExprType::LessThan,
                    ExprType::GreaterThanOrEqual => ExprType::LessThanOrEqual,
                    other => other,
                };
                (index, value, flipped)
            } else {
                return;
            };
            let domain = domains.entry(index).or_default();
            match func_type {
                ExprType::Equal => domain.allowed.push(vec![value]),
                ExprType::NotEqual => domain.excluded.push(value),
                ExprType::GreaterThan => domain.add_lower(value, false),
                ExprType::GreaterThanOrEqual => domain.add_lower(value, true),
                ExprType::LessThan => domain.add_upper(value, false),
                ExprType::LessThanOrEqual => domain.add_upper(value, true),
                _ => unreachable!("not a comparison"),
            }
        }
        _ => {}
    }
}

/// Returns whether the conjuncts that compare one column with constants can not all be `TRUE` for
/// the same row.
///
/// Each recognized conjunct (see [`collect_constraint`]) is `TRUE` only if the column is not
/// `NULL` and its value satisfies the constraint. If no value satisfies all the constraints on a
/// column, at least one of these conjuncts is not `TRUE` for any row.
fn has_value_contradiction(predicate: &Condition) -> bool {
    let mut domains = BTreeMap::new();
    for expr in &predicate.conjunctions {
        collect_constraint(expr, &mut domains);
    }
    domains.values().any(Domain::is_empty)
}
