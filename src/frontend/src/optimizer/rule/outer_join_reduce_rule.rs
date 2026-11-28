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
use risingwave_common::types::ScalarImpl;
use risingwave_pb::plan_common::JoinType;

use super::column_facts_simplify_rule::column_facts;
use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprRewriter, ExprType, InputRef};
use crate::optimizer::plan_expr_visitor::Strong;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::{Condition, Substitute};

/// Reduces an outer join whose padded rows a predicate above rejects, as PostgreSQL does in
/// `reduce_outer_joins`:
///
/// 1. Outer to inner (or full to left / right): a conjunct of a filter right above the join, or
///    of the condition of a join whose input is the outer join and does not keep the rows of
///    that input that match nothing (inner, semi, the optional side of a left or right join, the
///    right side of a left anti join...), can not be `TRUE` when all the columns of a padded
///    side are `NULL` (see [`rejects_nulls`]). The padded rows of that side are all rejected:
///    the join does not need to produce them.
///
///    ```text
///    Filter(r.c + 1 > t.x)                      Filter(r.c + 1 > t.x)
///    └─Join(LeftOuter, t.k = r.id)        =>    └─Join(Inner, t.k = r.id)
///    ```
///
///    This is what `LogicalJoin::simplify_outer` does during predicate pushdown, with a real
///    null-rejection test instead of a comparison of a column: the strict functions that
///    [`Strong`] lists (arithmetic, casts, `LIKE`...; not `lower` or `upper`), `IS NOT NULL`,
///    `IN`, `OR` of such conjuncts, and the conditions of a join above.
///
/// 2. Outer to anti: a top-level conjunct `r.c IS NULL` of a filter right above a left join,
///    where `r.c` is a column of the right side that is never `NULL` in a matched row: the
///    filter keeps exactly the rows of the left side that match nothing, padded with `NULL`s.
///
///    ```text
///    Filter(r.k IS NULL)                        Project(t.*, NULL::int AS r.k)
///    └─Join(LeftOuter, t.k = r.k)         =>    └─Join(LeftAnti, t.k = r.k)
///    ```
///
///    `r.c` is never `NULL` in a matched row when the column facts of the right input say so
///    (a `NOT NULL` column, a filter below, a count), or when the join condition can not be `TRUE`
///    when `r.c` is `NULL` (`t.k = r.c`). The same for the left side of a right join (a right
///    anti join). For a full join only the column facts count, since the unmatched rows of the
///    right side keep their own `r.c`: `r.c IS NULL` then keeps the unmatched rows of the left
///    side (a left anti join). The other conjuncts of the filter stay above, evaluated on the
///    `NULL`s of the padded side, except those that only read that side and are now `TRUE` (its
///    `IS NULL` conjuncts, among others), which are removed.
///
/// # Correctness
///
/// An outer join outputs the pairs that match, plus the rows of a preserved side that match
/// nothing, padded with `NULL`s on the other side. 1: the rejecting conjunct is `NULL` or `FALSE`
/// on every padded row whatever its other columns, so the filter (or the join above, which
/// evaluates its condition with filter semantics and does not keep the rows of this input that
/// match nothing) never keeps one: the result is the same without them. 2: in a matched row `r.c`
/// is not `NULL`, so `r.c IS NULL` keeps the padded rows only, which are the rows of the left side
/// without a match, i.e. the left anti join, each once. Both are identities of relational algebra
/// that hold for every content of the inputs, duplicates included, so they hold for the changes
/// of a streaming job too: the plan of a streaming job is frozen when it is created.
///
/// # Not rewritten
///
/// Conjuncts with `now()` (temporal filters keep their form); as-of joins; anti joins over a
/// temporal join (`FOR SYSTEM_TIME AS OF`), which streaming can not run; `r.c IS NULL` on a
/// column that a matched row may hold `NULL` in; a filter or a join that is not directly above
/// the outer join.
///
/// Used on demand, behind `streaming_reduce_outer_joins` and `batch_reduce_outer_joins`.
pub struct OuterJoinReduceRule {}

impl Rule<Logical> for OuterJoinReduceRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if let Some(filter) = plan.as_logical_filter() {
            let input = filter.input();
            let join = input.as_logical_join()?;
            return reduce_below_filter(filter.predicate(), join)
                .or_else(|| anti_join_below_filter(filter.predicate(), join));
        }
        reduce_inputs_of_join(plan.as_logical_join()?)
    }
}

impl OuterJoinReduceRule {
    pub fn create() -> BoxedRule {
        Box::new(OuterJoinReduceRule {})
    }
}

/// The sides that an outer join pads with `NULL`s, left and right.
fn padded_sides(join_type: JoinType) -> Option<(bool, bool)> {
    match join_type {
        JoinType::LeftOuter => Some((false, true)),
        JoinType::RightOuter => Some((true, false)),
        JoinType::FullOuter => Some((true, true)),
        _ => None,
    }
}

/// The join type that pads the given sides.
fn join_type_padding(left: bool, right: bool) -> JoinType {
    match (left, right) {
        (true, true) => JoinType::FullOuter,
        (true, false) => JoinType::RightOuter,
        (false, true) => JoinType::LeftOuter,
        (false, false) => JoinType::Inner,
    }
}

/// Whether a join keeps the rows of the given side of its inputs that match nothing.
fn keeps_unmatched(join_type: JoinType, left: bool) -> bool {
    match join_type {
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => false,
        JoinType::LeftOuter | JoinType::LeftAnti => left,
        JoinType::RightOuter | JoinType::RightAnti => !left,
        // Full outer joins, as-of joins: left as they are.
        _ => true,
    }
}

/// The output columns of an outer join (left or right outer, full) that come from the given side,
/// shifted by `offset`, in a set of `len` columns.
fn columns_of_side(join: &LogicalJoin, left: bool, offset: usize, len: usize) -> FixedBitSet {
    let left_len = join.left().schema().len();
    let mut columns = FixedBitSet::with_capacity(len);
    for (output, &internal) in join.output_indices().iter().enumerate() {
        if (internal < left_len) == left {
            columns.insert(output + offset);
        }
    }
    columns
}

/// The outer join with fewer padded sides when one of the conjuncts (over its output columns
/// shifted by `offset`, in a set of `len` columns) rejects the padded rows of a side, or `None`.
fn reduce_with_conjuncts(
    join: &LogicalJoin,
    conjuncts: &[ExprImpl],
    offset: usize,
    len: usize,
) -> Option<LogicalJoin> {
    let (mut padded_left, mut padded_right) = padded_sides(join.join_type())?;
    let rejects = |left: bool| {
        let columns = columns_of_side(join, left, offset, len);
        conjuncts
            .iter()
            .any(|expr| expr.count_nows() == 0 && rejects_nulls(expr, &columns))
    };
    padded_left &= !rejects(true);
    padded_right &= !rejects(false);
    let join_type = join_type_padding(padded_left, padded_right);
    if join_type == join.join_type() {
        return None;
    }
    let (left, right, on, _, output_indices) = join.clone().decompose();
    Some(LogicalJoin::with_output_indices(
        left,
        right,
        join_type,
        on,
        output_indices,
    ))
}

/// 1, for a filter right above an outer join.
fn reduce_below_filter(predicate: &Condition, join: &LogicalJoin) -> Option<PlanRef> {
    let len = join.output_indices().len();
    let reduced = reduce_with_conjuncts(join, &predicate.conjunctions, 0, len)?;
    Some(LogicalFilter::create(reduced.into(), predicate.clone()))
}

/// 1, for the inputs of a join that are outer joins whose rows that match nothing the join does
/// not keep. The condition of the join refers to the columns of its left input, then of its right
/// input.
fn reduce_inputs_of_join(join: &LogicalJoin) -> Option<PlanRef> {
    let join_type = join.join_type();
    if matches!(join_type, JoinType::AsofInner | JoinType::AsofLeftOuter) {
        return None;
    }
    let (left, right) = (join.left(), join.right());
    let left_len = left.schema().len();
    let len = left_len + right.schema().len();
    let conjuncts = &join.on().conjunctions;
    let reduce = |input: &PlanRef, is_left: bool, offset: usize| -> Option<PlanRef> {
        if keeps_unmatched(join_type, is_left) {
            return None;
        }
        let input_join = input.as_logical_join()?;
        Some(reduce_with_conjuncts(input_join, conjuncts, offset, len)?.into())
    };
    let new_left = reduce(&left, true, 0);
    let new_right = reduce(&right, false, left_len);
    if new_left.is_none() && new_right.is_none() {
        return None;
    }
    Some(
        join.clone_with_left_right(new_left.unwrap_or(left), new_right.unwrap_or(right))
            .into(),
    )
}

/// The output column `c` if the expression is exactly `c IS NULL`.
fn is_null_column(expr: &ExprImpl) -> Option<usize> {
    let call = expr.as_function_call()?;
    match (call.func_type(), call.inputs()) {
        (ExprType::IsNull, [input]) => Some(input.as_input_ref()?.index()),
        _ => None,
    }
}

/// 2, for a filter right above an outer join.
fn anti_join_below_filter(predicate: &Condition, join: &LogicalJoin) -> Option<PlanRef> {
    let join_type = join.join_type();
    let (padded_left, padded_right) = padded_sides(join_type)?;
    let is_temporal = |input: PlanRef| input.as_logical_scan().is_some_and(|s| s.as_of().is_some());
    if is_temporal(join.left()) || is_temporal(join.right()) {
        return None;
    }
    let left_len = join.left().schema().len();
    let left_facts = column_facts(&join.left());
    let right_facts = column_facts(&join.right());
    // Whether the internal column is never `NULL` in a row of its side that the join outputs
    // with its own values.
    let never_null_unpadded = |internal: usize| {
        let facts = if internal < left_len {
            left_facts.is_non_null(internal)
        } else {
            right_facts.is_non_null(internal - left_len)
        };
        if facts {
            return true;
        }
        // Without the facts, only the matched rows are known: a full join also outputs the
        // unmatched rows of both sides.
        if join_type == JoinType::FullOuter {
            return false;
        }
        let mut columns = FixedBitSet::with_capacity(left_len + join.right().schema().len());
        columns.insert(internal);
        join.on()
            .conjunctions
            .iter()
            .any(|expr| expr.count_nows() == 0 && rejects_nulls(expr, &columns))
    };
    // The padded side whose `IS NULL` conjunct keeps exactly the padded rows: the right one first.
    let output_indices = join.output_indices();
    let tested_side = [(false, padded_right), (true, padded_left)]
        .into_iter()
        .filter(|(_, padded)| *padded)
        .map(|(left, _)| left)
        .find(|&left| {
            predicate.conjunctions.iter().any(|expr| {
                is_null_column(expr).is_some_and(|output| {
                    let internal = output_indices[output];
                    (internal < left_len) == left && never_null_unpadded(internal)
                })
            })
        })?;
    // The anti join keeps the other side.
    let keep_left = !tested_side;
    let (left, right, on, _, output_indices) = join.clone().decompose();
    let anti_type = if keep_left {
        JoinType::LeftAnti
    } else {
        JoinType::RightAnti
    };
    let anti = LogicalJoin::new(left, right, anti_type, on);
    let schema = join.schema();
    let exprs = output_indices
        .iter()
        .enumerate()
        .map(|(output, &internal)| {
            let data_type = schema[output].data_type();
            match (keep_left, internal < left_len) {
                (true, true) => InputRef::new(internal, data_type).into(),
                (false, false) => InputRef::new(internal - left_len, data_type).into(),
                _ => ExprImpl::literal_null(data_type),
            }
        })
        .collect::<Vec<ExprImpl>>();
    // The conjuncts that only read the padded side, now `NULL`, and are `TRUE` on it (the `IS
    // NULL` conjuncts, among others) are removed.
    let mut substitute = Substitute {
        mapping: exprs.clone(),
    };
    let conjunctions = predicate
        .conjunctions
        .iter()
        .filter(|expr| {
            !matches!(
                substitute.rewrite_expr((*expr).clone()).try_fold_const(),
                Some(Ok(Some(ScalarImpl::Bool(true))))
            )
        })
        .cloned()
        .collect();
    let project = LogicalProject::create(anti.into(), exprs);
    Some(LogicalFilter::create(project, Condition { conjunctions }))
}

/// Whether the predicate can not be `TRUE` on a row whose columns `null_columns` are all `NULL`,
/// whatever its other columns:
///
/// - it is `NULL` then, by [`Strong`] (strict functions of those columns, `AND` and `OR` whose
///   operands are all `NULL`, ...);
/// - the constant `FALSE`;
/// - `a AND b` with `a` or `b` rejecting, `a OR b` with both rejecting, `a IS TRUE` with `a`
///   rejecting (`NULL` and `FALSE` are both not `TRUE`);
/// - `e IS NOT NULL`, `NOT (e IS NULL)`, `e IN (...)` or `e IS NOT DISTINCT FROM c` (`c` a
///   non-`NULL` constant), with `e` `NULL` then.
fn rejects_nulls(expr: &ExprImpl, null_columns: &FixedBitSet) -> bool {
    if is_null_when(expr, null_columns) {
        return true;
    }
    if let ExprImpl::Literal(literal) = expr
        && *literal.get_data() == Some(ScalarImpl::Bool(false))
    {
        return true;
    }
    let Some(call) = expr.as_function_call() else {
        return false;
    };
    let is_non_null_constant = |expr: &ExprImpl| {
        expr.as_literal()
            .is_some_and(|literal| literal.get_data().is_some())
    };
    match (call.func_type(), call.inputs()) {
        (ExprType::And, inputs) => inputs.iter().any(|e| rejects_nulls(e, null_columns)),
        (ExprType::Or, inputs) => {
            !inputs.is_empty() && inputs.iter().all(|e| rejects_nulls(e, null_columns))
        }
        (ExprType::IsTrue, [input]) => rejects_nulls(input, null_columns),
        (ExprType::IsNotNull, [input]) | (ExprType::In, [input, ..]) => {
            is_null_when(input, null_columns)
        }
        (ExprType::Not, [input]) => match input.as_function_call() {
            Some(inner) if inner.func_type() == ExprType::IsNull => {
                inner.inputs().iter().all(|e| is_null_when(e, null_columns))
            }
            _ => false,
        },
        (ExprType::IsNotDistinctFrom, [a, b]) => {
            (is_null_when(a, null_columns) && is_non_null_constant(b))
                || (is_null_when(b, null_columns) && is_non_null_constant(a))
        }
        _ => false,
    }
}

/// [`Strong::is_null`], except over `greatest` and `least`, which skip their `NULL` arguments in
/// RisingWave (as in PostgreSQL) while `Strong` takes them as strict.
fn is_null_when(expr: &ExprImpl, null_columns: &FixedBitSet) -> bool {
    !has_null_skipping_call(expr) && Strong::is_null(expr, null_columns.clone())
}

/// Whether the expression calls `greatest` or `least`, through function calls (the only
/// expressions that [`Strong`] looks into).
fn has_null_skipping_call(expr: &ExprImpl) -> bool {
    match expr {
        ExprImpl::FunctionCall(call) => {
            matches!(call.func_type(), ExprType::Greatest | ExprType::Least)
                || call.inputs().iter().any(has_null_skipping_call)
        }
        _ => false,
    }
}
