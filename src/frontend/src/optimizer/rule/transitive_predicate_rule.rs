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

use std::collections::HashMap;

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use super::column_facts_simplify_rule::column_facts;
use super::prelude::{PlanRef, *};
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Literal, default_rewrite_expr,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    EqJoinPredicate, LogicalFilter, PlanNodeId, PlanTreeNodeBinary, PlanTreeNodeUnary,
    respects_column_equality,
};
use crate::utils::Condition;

/// At most this many predicates are kept for a plan node, to bound the planning time.
const MAX_PREDICATES: usize = 32;
/// At most this many values in the `IN` list derived for a column of a union or of a `VALUES`.
const MAX_IN_LIST: usize = 64;

/// Copies to one side of a join the predicates that hold on every row of its other side and only
/// read the columns of the equi-join keys, rewritten on the keys of the first side (Calcite
/// `JoinPushTransitivePredicatesRule`, the constants of the equivalence classes of PostgreSQL).
///
/// ```text
/// Join(o.region = s.region AND o.id = s.order_id)        Join(o.region = s.region AND ...)
/// ├─Scan(orders, region = 'EU' AND id > 1000)      =>    ├─Scan(orders, region = 'EU' AND ...)
/// └─Scan(shipments)                                      └─Filter(region = 'EU' AND order_id > 1000)
///                                                          └─Scan(shipments)
/// ```
///
/// Predicate pushdown already derives, through the equalities of a join, the predicates that it
/// pushes into the join from above or from its condition. A predicate that is already inside an
/// input (the `WHERE` of a view, the predicate of a scan, a constant column of a projection, the
/// tags of the branches of a `UNION ALL`) never meets the join; this rule copies it. The
/// predicates that hold on every row of a plan node are found by [`pulled_predicates`].
///
/// Correctness: a predicate `p(l)` holds (is `TRUE`) on every row of the side `L`. For a pair of
/// rows `(l, r)` that satisfies the equalities `l.k = r.k`, `p(r.k)` gives the same result as
/// `p(l.k)` when the two values are indistinguishable by `p` (see [`respects_column_equality`]),
/// so it is `TRUE`: the rows of `R` for which `p` is not `TRUE` match no row of `L`. Removing them
/// changes the result of the join only where an unmatched row of `R` is output, which decides the
/// directions:
///
/// - `Inner`, `LeftSemi`, `RightSemi`: both ways;
/// - `LeftOuter`, `LeftAnti`: from the left side into the right side only (an unmatched left row
///   is output, an unmatched right row is not); `RightOuter`, `RightAnti` symmetrically;
/// - `FullOuter`, as-of joins and temporal joins (a side that is a scan `AS OF`): never.
///
/// Only the keys compared with `=` on columns of the same type are used, and the keys compared with
/// `IS NOT DISTINCT FROM` when both columns are never `NULL`. Only deterministic predicates are
/// copied: input references, literals and pure functions, no `now()`, sub-query or user-defined
/// function. Duplicates and retractions are not a concern: a deterministic row filter keeps or
/// drops a row the same way whenever it is inserted or deleted. A predicate is not copied when the
/// other side already holds it, nor `k IS NOT NULL` (implied by `=`), so applying the rule again
/// adds nothing: it is applied bottom-up, then predicate pushdown moves the new filters into the
/// scans and derives them further down through the joins below; the caller repeats this a bounded
/// number of times.
pub struct TransitivePredicateRule;

impl Rule<Logical> for TransitivePredicateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let (into_right, into_left) = match join.join_type() {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => (true, true),
            JoinType::LeftOuter | JoinType::LeftAnti => (true, false),
            JoinType::RightOuter | JoinType::RightAnti => (false, true),
            _ => return None,
        };
        let left = join.left();
        let right = join.right();
        if is_versioned_scan(&left) || is_versioned_scan(&right) {
            return None;
        }
        let left_len = left.schema().len();
        let right_len = right.schema().len();
        let eq = EqJoinPredicate::create(left_len, right_len, join.on().clone());
        // The pairs `(left column, right column)` of the keys whose equal values match.
        let mut pairs = vec![];
        let mut facts = None;
        for (l, r, null_safe) in eq.eq_keys() {
            if l.return_type() != r.return_type() {
                continue;
            }
            let r_index = r.index() - left_len;
            if *null_safe {
                let (left_facts, right_facts) =
                    facts.get_or_insert_with(|| (column_facts(&left), column_facts(&right)));
                if !(left_facts.is_non_null(l.index()) && right_facts.is_non_null(r_index)) {
                    continue;
                }
            }
            pairs.push((l.index(), r_index));
        }
        if pairs.is_empty() {
            return None;
        }

        let mut memo = HashMap::new();
        let left_predicates = pulled_predicates(&left, &mut memo);
        let right_predicates = pulled_predicates(&right, &mut memo);
        let new_right = if into_right {
            copy_through_keys(&left_predicates, &right_predicates, &pairs, left_len)
        } else {
            vec![]
        };
        let new_left = if into_left {
            let reversed: Vec<_> = pairs.iter().map(|&(l, r)| (r, l)).collect();
            copy_through_keys(&right_predicates, &left_predicates, &reversed, right_len)
        } else {
            vec![]
        };
        if new_left.is_empty() && new_right.is_empty() {
            return None;
        }
        let with_filter = |input: PlanRef, conjunctions: Vec<ExprImpl>| {
            if conjunctions.is_empty() {
                input
            } else {
                LogicalFilter::create(input, Condition { conjunctions })
            }
        };
        let left = with_filter(left, new_left);
        let right = with_filter(right, new_right);
        Some(join.clone_with_left_right(left, right).into())
    }
}

impl TransitivePredicateRule {
    pub fn create() -> BoxedRule {
        Box::new(TransitivePredicateRule)
    }
}

/// A scan `AS OF` (temporal join, time travel): its side of a join is left as it is.
fn is_versioned_scan(plan: &PlanRef) -> bool {
    plan.as_logical_scan()
        .is_some_and(|scan| scan.as_of().is_some())
}

/// The predicates of `from` that read only key columns, rewritten on the columns of the other side
/// (`pairs`: `(column of from, column of to)`), that `to` does not already hold.
fn copy_through_keys(
    from: &[ExprImpl],
    to: &[ExprImpl],
    pairs: &[(usize, usize)],
    from_len: usize,
) -> Vec<ExprImpl> {
    let mut mapping = HashMap::new();
    for &(f, t) in pairs {
        mapping.entry(f).or_insert(t);
    }
    let mut derived: Vec<ExprImpl> = vec![];
    for predicate in from {
        if is_not_null_test(predicate) || !respects_column_equality(predicate) {
            continue;
        }
        let refs = predicate.collect_input_refs(from_len);
        if refs.ones().any(|column| !mapping.contains_key(&column)) {
            continue;
        }
        let Some(mapped) = map_columns(predicate, &mapping) else {
            continue;
        };
        if !to.contains(&mapped) && !derived.contains(&mapped) {
            derived.push(mapped);
        }
    }
    derived
}

/// `c IS NOT NULL` on a column: implied by the `=` of the keys, never worth a filter.
fn is_not_null_test(expr: &ExprImpl) -> bool {
    expr.as_function_call().is_some_and(|call| {
        call.func_type() == ExprType::IsNotNull
            && call.inputs().len() == 1
            && call.inputs()[0].as_input_ref().is_some()
    })
}

/// Rewrites the columns of `expr` with `mapping`, keeping their types; `None` if a column is not
/// in `mapping`.
fn map_columns(expr: &ExprImpl, mapping: &HashMap<usize, usize>) -> Option<ExprImpl> {
    struct Mapper<'a> {
        mapping: &'a HashMap<usize, usize>,
        failed: bool,
    }
    impl ExprRewriter for Mapper<'_> {
        fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
            match self.mapping.get(&input_ref.index()) {
                Some(&index) => InputRef::new(index, input_ref.return_type()).into(),
                None => {
                    self.failed = true;
                    input_ref.into()
                }
            }
        }
    }
    let mut mapper = Mapper {
        mapping,
        failed: false,
    };
    let mapped = mapper.rewrite_expr(expr.clone());
    (!mapper.failed).then_some(mapped)
}

/// Whether `expr` is made of columns, literals and pure functions only: the same result for the
/// same values of its columns, at any time and on any node.
fn is_deterministic(expr: &ExprImpl) -> bool {
    fn simple(expr: &ExprImpl) -> bool {
        match expr {
            ExprImpl::InputRef(_) | ExprImpl::Literal(_) => true,
            ExprImpl::FunctionCall(call) => call.inputs().iter().all(simple),
            _ => false,
        }
    }
    simple(expr) && !expr.is_impure()
}

/// The predicates that hold (are `TRUE`) on every row that `plan` produces, on its output columns.
/// A node that is not listed has none, which is always safe.
///
/// - `Scan`, `Filter`: the conjuncts of the predicate (on the output columns of a scan), added to
///   those of the input for a filter;
/// - `Values`: a column with literals only, never `NULL`, is `IN` the list of its values;
/// - `Project`: the predicates of the input in which every column is replaced by an output
///   column computed by the same pure expression (an input reference, usually); an output column
///   that is a literal, not `NULL`, is equal to it;
/// - `Join`: the predicates of a side that is not padded with `NULL`s (both for an inner join,
///   with its condition; the left side of a left outer join; the side kept by a semi or anti
///   join), on the output columns;
/// - `Union`: the predicates that every input holds, and a column that every input fixes to a few
///   values (`c = v`, `c IN (...)`, a literal column) is `IN` all of them;
/// - `Agg` without grouping sets: the predicates on the group key columns;
/// - `Share`, `Dedup`, `TopN`, `Limit`, `OverWindow`: the predicates of the input, whose rows they
///   only select (an over window adds its columns after those of its input).
///
/// Only deterministic predicates are kept, at most [`MAX_PREDICATES`] per node.
pub(super) fn pulled_predicates(
    plan: &PlanRef,
    memo: &mut HashMap<PlanNodeId, Vec<ExprImpl>>,
) -> Vec<ExprImpl> {
    if let Some(predicates) = memo.get(&plan.id()) {
        return predicates.clone();
    }
    let len = plan.schema().len();
    let mut predicates: Vec<ExprImpl> = vec![];
    for predicate in derive_pulled_predicates(plan, memo) {
        if predicates.len() >= MAX_PREDICATES {
            break;
        }
        if is_deterministic(&predicate)
            && !predicate.collect_input_refs(len).is_clear()
            && !predicates.contains(&predicate)
        {
            predicates.push(predicate);
        }
    }
    memo.insert(plan.id(), predicates.clone());
    predicates
}

fn derive_pulled_predicates(
    plan: &PlanRef,
    memo: &mut HashMap<PlanNodeId, Vec<ExprImpl>>,
) -> Vec<ExprImpl> {
    if let Some(scan) = plan.as_logical_scan() {
        if scan.as_of().is_some() {
            return vec![];
        }
        // The predicate refers to the columns of the table.
        let mut mapping = HashMap::new();
        for (output, &column) in scan.output_col_idx().iter().enumerate() {
            mapping.entry(column).or_insert(output);
        }
        return scan
            .predicate()
            .conjunctions
            .iter()
            .filter_map(|conjunct| map_columns(conjunct, &mapping))
            .collect();
    }
    if let Some(filter) = plan.as_logical_filter() {
        let mut predicates = filter.predicate().conjunctions.clone();
        predicates.extend(pulled_predicates(&filter.input(), memo));
        return predicates;
    }
    if let Some(values) = plan.as_logical_values() {
        let rows = values.rows();
        if rows.is_empty() {
            return vec![];
        }
        let schema = plan.schema();
        return (0..schema.len())
            .filter_map(|column| {
                let lists: Option<Vec<_>> = rows
                    .iter()
                    .map(|row| {
                        row[column]
                            .as_literal()
                            .map(|literal| vec![(**literal).clone()])
                    })
                    .collect();
                in_list(column, &schema.fields()[column].data_type, lists?)
            })
            .collect();
    }
    if let Some(project) = plan.as_logical_project() {
        let input = pulled_predicates(&project.input(), memo);
        let mut outputs = HashMap::new();
        let mut predicates: Vec<ExprImpl> = vec![];
        for (index, expr) in project.exprs().iter().enumerate() {
            if let Some(literal) = expr.as_literal() {
                if literal.get_data().is_some() {
                    predicates.push(
                        FunctionCall::new_unchecked(
                            ExprType::Equal,
                            vec![
                                InputRef::new(index, expr.return_type()).into(),
                                expr.clone(),
                            ],
                            DataType::Boolean,
                        )
                        .into(),
                    );
                }
            } else if !expr.is_impure() {
                outputs.entry(expr.clone()).or_insert(index);
            }
        }
        predicates.extend(
            input
                .iter()
                .filter_map(|predicate| project_back(predicate, &outputs)),
        );
        return predicates;
    }
    if let Some(join) = plan.as_logical_join() {
        let left_len = join.left().schema().len();
        let shifted_right = |memo: &mut HashMap<PlanNodeId, Vec<ExprImpl>>| {
            let right_len = join.right().schema().len();
            let mapping: HashMap<_, _> = (0..right_len).map(|i| (i, i + left_len)).collect();
            pulled_predicates(&join.right(), memo)
                .iter()
                .filter_map(|predicate| map_columns(predicate, &mapping))
                .collect::<Vec<_>>()
        };
        // The predicates on the internal columns of the join: those of the left side, then those
        // of the right side.
        let internal: Vec<ExprImpl> = match join.join_type() {
            JoinType::Inner => {
                let mut predicates = pulled_predicates(&join.left(), memo);
                predicates.extend(shifted_right(memo));
                predicates.extend(join.on().conjunctions.iter().cloned());
                predicates
            }
            JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => {
                pulled_predicates(&join.left(), memo)
            }
            JoinType::RightOuter => shifted_right(memo),
            // The internal columns of a right semi or anti join are those of the right side.
            JoinType::RightSemi | JoinType::RightAnti => pulled_predicates(&join.right(), memo),
            _ => vec![],
        };
        let mut mapping = HashMap::new();
        for (output, &internal) in join.output_indices().iter().enumerate() {
            mapping.entry(internal).or_insert(output);
        }
        return internal
            .iter()
            .filter_map(|predicate| map_columns(predicate, &mapping))
            .collect();
    }
    if plan.as_logical_union().is_some() {
        let inputs = plan.inputs();
        let Some((first, rest)) = inputs.split_first() else {
            return vec![];
        };
        let first_predicates = pulled_predicates(first, memo);
        let rest_predicates: Vec<_> = rest
            .iter()
            .map(|input| pulled_predicates(input, memo))
            .collect();
        let mut predicates: Vec<ExprImpl> = first_predicates
            .iter()
            .filter(|predicate| {
                rest_predicates
                    .iter()
                    .all(|other| other.contains(*predicate))
            })
            .cloned()
            .collect();
        let schema = plan.schema();
        for column in 0..schema.len() {
            let lists: Option<Vec<_>> = std::iter::once(&first_predicates)
                .chain(rest_predicates.iter())
                .map(|branch| fixed_values(branch, column))
                .collect();
            if let Some(lists) = lists
                && let Some(predicate) = in_list(column, &schema.fields()[column].data_type, lists)
            {
                predicates.push(predicate);
            }
        }
        return predicates;
    }
    if let Some(agg) = plan.as_logical_agg() {
        if !agg.grouping_sets().is_empty() {
            return vec![];
        }
        // The output columns are the group key columns, then the aggregate calls.
        let mapping: HashMap<_, _> = agg
            .group_key()
            .indices()
            .enumerate()
            .map(|(output, input)| (input, output))
            .collect();
        return pulled_predicates(&agg.input(), memo)
            .iter()
            .filter_map(|predicate| map_columns(predicate, &mapping))
            .collect();
    }
    if plan.as_logical_share().is_some()
        || plan.as_logical_dedup().is_some()
        || plan.as_logical_top_n().is_some()
        || plan.as_logical_limit().is_some()
        || plan.as_logical_over_window().is_some()
    {
        let inputs = plan.inputs();
        if let [input] = &inputs[..] {
            return pulled_predicates(input, memo);
        }
    }
    vec![]
}

/// Rewrites a predicate on the input of a projection on its output: every sub-expression that the
/// projection computes (`outputs`, by expression) is replaced with its output column. `None` if a
/// column of the input is left.
fn project_back(predicate: &ExprImpl, outputs: &HashMap<ExprImpl, usize>) -> Option<ExprImpl> {
    struct Back<'a> {
        outputs: &'a HashMap<ExprImpl, usize>,
        failed: bool,
    }
    impl ExprRewriter for Back<'_> {
        fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
            if let Some(&index) = self.outputs.get(&expr) {
                return InputRef::new(index, expr.return_type()).into();
            }
            if expr.as_input_ref().is_some() {
                self.failed = true;
                return expr;
            }
            default_rewrite_expr(self, expr)
        }
    }
    let mut back = Back {
        outputs,
        failed: false,
    };
    let rewritten = back.rewrite_expr(predicate.clone());
    (!back.failed).then_some(rewritten)
}

/// The values that a predicate of `predicates` fixes `column` to: `column = v` or
/// `column IN (v, ...)` with literals that are not `NULL`.
fn fixed_values(predicates: &[ExprImpl], column: usize) -> Option<Vec<Literal>> {
    predicates.iter().find_map(|predicate| {
        let call = predicate.as_function_call()?;
        let inputs = call.inputs();
        let (column_expr, values) = match call.func_type() {
            ExprType::Equal if inputs.len() == 2 => {
                if inputs[0].as_input_ref().is_some() {
                    (&inputs[0], &inputs[1..])
                } else {
                    (&inputs[1], &inputs[..1])
                }
            }
            ExprType::In if inputs.len() >= 2 => (&inputs[0], &inputs[1..]),
            _ => return None,
        };
        if column_expr.as_input_ref()?.index() != column {
            return None;
        }
        values
            .iter()
            .map(|value| {
                let literal = value.as_literal()?;
                literal.get_data().is_some().then(|| (**literal).clone())
            })
            .collect()
    })
}

/// `column IN (values)` for the union of the lists, or `column = value` for a single value; `None`
/// if a value is `NULL` or the list is too long.
fn in_list(column: usize, data_type: &DataType, lists: Vec<Vec<Literal>>) -> Option<ExprImpl> {
    let mut values: Vec<Literal> = vec![];
    for literal in lists.into_iter().flatten() {
        if literal.get_data().is_none() || literal.return_type() != *data_type {
            return None;
        }
        if !values.contains(&literal) {
            values.push(literal);
            if values.len() > MAX_IN_LIST {
                return None;
            }
        }
    }
    let column: ExprImpl = InputRef::new(column, data_type.clone()).into();
    let mut inputs = vec![column];
    let func_type = match values.len() {
        0 => return None,
        1 => ExprType::Equal,
        _ => ExprType::In,
    };
    inputs.extend(values.into_iter().map(ExprImpl::from));
    Some(FunctionCall::new_unchecked(func_type, inputs, DataType::Boolean).into())
}
