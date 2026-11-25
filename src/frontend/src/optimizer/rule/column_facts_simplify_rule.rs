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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::{DataType, Datum, DefaultOrd, ScalarImpl};
use risingwave_common::util::value_encoding::DatumFromProtoExt;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;

use super::contradictory_null_filter_rule::null_rejected_columns;
use super::never_true_predicate_rule::{
    column_comparison, constant_value, fold_join_without_match, is_never_true, is_simple_type,
    simple_column,
};
use super::prelude::{PlanRef, *};
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Literal, default_rewrite_expr,
    fold_boolean_constant, try_get_bool_constant,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, LogicalScan, PlanNodeId, PlanTreeNodeBinary,
    PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// What is known about the columns of a plan node, for every row it will ever produce.
#[derive(Debug, Default, Clone)]
pub(super) struct ColumnFacts {
    /// The columns that hold the same non-`NULL` constant in every row. Only for the simple
    /// types of [`is_simple_type`], for which two equal values are indistinguishable.
    constants: BTreeMap<usize, Literal>,
    /// The columns that are never `NULL`, besides the constant ones.
    non_null: BTreeSet<usize>,
}

impl ColumnFacts {
    fn constant(&self, index: usize) -> Option<&Literal> {
        self.constants.get(&index)
    }

    /// The columns that hold the same non-`NULL` constant in every row.
    pub(super) fn constant_columns(&self) -> BTreeSet<usize> {
        self.constants.keys().copied().collect()
    }

    fn is_non_null(&self, index: usize) -> bool {
        self.non_null.contains(&index) || self.constants.contains_key(&index)
    }

    /// Adds the facts of another set about the same columns.
    fn merge(&mut self, other: ColumnFacts) {
        for (index, literal) in other.constants {
            self.constants.entry(index).or_insert(literal);
        }
        self.non_null.extend(other.non_null);
    }

    /// The same facts, with every column index moved by `offset`.
    fn shift(self, offset: usize) -> Self {
        Self {
            constants: self
                .constants
                .into_iter()
                .map(|(index, literal)| (index + offset, literal))
                .collect(),
            non_null: self
                .non_null
                .into_iter()
                .map(|index| index + offset)
                .collect(),
        }
    }

    /// The facts about the output columns of an operator whose output column `o` is its
    /// input column `mapping[o]`.
    fn select(&self, mapping: impl IntoIterator<Item = usize>) -> Self {
        let mut facts = Self::default();
        for (output, input) in mapping.into_iter().enumerate() {
            if let Some(literal) = self.constant(input) {
                facts.constants.insert(output, literal.clone());
            } else if self.is_non_null(input) {
                facts.non_null.insert(output);
            }
        }
        facts
    }

    /// The facts that hold for all the rows of a union: those that hold for every input.
    fn intersect(self, other: &ColumnFacts) -> Self {
        let mut facts = Self::default();
        for (index, literal) in self.constants {
            if other.constant(index) == Some(&literal) {
                facts.constants.insert(index, literal);
            } else if other.is_non_null(index) {
                facts.non_null.insert(index);
            }
        }
        for index in self.non_null {
            if other.is_non_null(index) {
                facts.non_null.insert(index);
            }
        }
        facts
    }
}

/// The facts that hold for every row that satisfies all the given conjuncts (each is `TRUE`):
///
/// - `#i = c` with a column of a simple type and a non-`NULL` constant of the same type: the
///   column is the constant;
/// - a boolean column `#i` alone, or `NOT #i`: the column is `TRUE`, or `FALSE`;
/// - a conjunct that rejects `NULL` for `#i` (see [`null_rejected_columns`]): the column is not
///   `NULL`.
fn predicate_facts<'a>(conjuncts: impl IntoIterator<Item = &'a ExprImpl>) -> ColumnFacts {
    let mut facts = ColumnFacts::default();
    for expr in conjuncts {
        facts.non_null.extend(null_rejected_columns(expr));
        if let Some((index, ExprType::Equal, value)) = column_comparison(expr)
            && let Some((_, data_type)) = expr
                .as_function_call()
                .and_then(|call| call.inputs().iter().find_map(simple_column))
        {
            facts
                .constants
                .entry(index)
                .or_insert_with(|| Literal::new(Some(value), data_type));
        } else if let Some((index, DataType::Boolean)) = simple_column(expr) {
            facts
                .constants
                .entry(index)
                .or_insert_with(|| Literal::new(Some(ScalarImpl::Bool(true)), DataType::Boolean));
        } else if let Some(call) = expr.as_function_call()
            && call.func_type() == ExprType::Not
            && let [input] = call.inputs()
            && let Some((index, DataType::Boolean)) = simple_column(input)
        {
            facts
                .constants
                .entry(index)
                .or_insert_with(|| Literal::new(Some(ScalarImpl::Bool(false)), DataType::Boolean));
        }
    }
    facts
}

/// What is known about the value of an expression over rows with the given facts: its constant
/// value if any, and whether it is never `NULL`.
///
/// An expression is only reported as never `NULL` when it can not fail either, because a failed
/// expression evaluates to `NULL` in a streaming job: a column, a constant, an `IS ...` test, or
/// a `COALESCE` with such an argument.
fn expr_facts(expr: &ExprImpl, input: &ColumnFacts) -> (Option<Literal>, bool) {
    if let Some(input_ref) = expr.as_input_ref() {
        let index = input_ref.index();
        return (input.constant(index).cloned(), input.is_non_null(index));
    }
    match constant_value(expr) {
        Some(Some(value)) => {
            let data_type = expr.return_type();
            let literal = is_simple_type(&data_type).then(|| Literal::new(Some(value), data_type));
            return (literal, true);
        }
        Some(None) => return (None, false),
        None => {}
    }
    let Some(call) = expr.as_function_call() else {
        return (None, false);
    };
    let non_null = match call.func_type() {
        ExprType::IsNull
        | ExprType::IsNotNull
        | ExprType::IsTrue
        | ExprType::IsNotTrue
        | ExprType::IsFalse
        | ExprType::IsNotFalse
        | ExprType::IsDistinctFrom
        | ExprType::IsNotDistinctFrom => true,
        ExprType::Coalesce => call
            .inputs()
            .iter()
            .any(|input_expr| expr_facts(input_expr, input).1),
        _ => false,
    };
    (None, non_null)
}

/// Returns whether the result of an aggregate function is never `NULL`.
fn agg_is_non_null(agg_type: &AggType) -> bool {
    matches!(
        agg_type,
        AggType::Builtin(PbAggKind::Count | PbAggKind::Sum0 | PbAggKind::ApproxCountDistinct)
    )
}

/// Returns whether a column of a table declared `NOT NULL` never holds a `NULL`.
///
/// A table drops the rows that violate its `NOT NULL` constraints, whatever writes them (DML, its
/// connector, a sink into the table), so every stored row satisfies them. A column with a
/// `DEFAULT` needs more care: the rows written before the column was added read the default
/// value evaluated when it was added, which must not be `NULL` either.
fn is_not_null_column(column: &ColumnDesc) -> bool {
    if column.nullable {
        return false;
    }
    match &column.generated_or_default_column {
        None => true,
        Some(GeneratedOrDefaultColumn::DefaultColumn(default_column)) => {
            default_column.snapshot_value.as_ref().is_some_and(|value| {
                matches!(Datum::from_protobuf(value, &column.data_type), Ok(Some(_)))
            })
        }
        Some(GeneratedOrDefaultColumn::GeneratedColumn(_)) => false,
    }
}

/// The facts about the columns of a scanned table that come from its constraints, indexed by the
/// columns of the table.
fn table_constraint_facts(scan: &LogicalScan) -> ColumnFacts {
    let mut facts = ColumnFacts::default();
    let table = scan.table();
    if table.is_user_table() && !table.has_generated_column() {
        for (index, column) in table.columns().iter().enumerate() {
            if is_not_null_column(&column.column_desc) {
                facts.non_null.insert(index);
            }
        }
    }
    facts
}

/// Derives the [`ColumnFacts`] of a plan node from those of its inputs. A node that is not
/// listed here has no known facts, which is always safe.
///
/// - `Values`: a column with the same constant in every row.
/// - `Scan`, `Filter`: the facts of the predicate (see [`predicate_facts`]), added to those of
///   the input for a filter. For the scan of a table, the columns declared `NOT NULL` are never
///   `NULL` (see [`is_not_null_column`]); not for a table with a generated column, whose
///   constraints are checked on the columns that accept DML only.
/// - `Project`: see [`expr_facts`].
/// - `Join`: the facts of a side hold for its columns in the output, unless the join pads the
///   side with `NULL` (the right side of a left outer join, the left side of a right outer join,
///   both sides of a full outer join). The facts of the condition of an inner join hold too.
/// - `Union`: the facts that hold for every input.
/// - `Agg` without grouping sets: the facts of the group key columns, and the counting
///   aggregates are never `NULL`.
/// - `Share`, `Dedup`, `TopN`, `Limit`: the facts of the input, whose rows they only select.
pub(super) fn column_facts(plan: &PlanRef) -> ColumnFacts {
    column_facts_inner(plan, &mut HashMap::new())
}

fn column_facts_inner(plan: &PlanRef, memo: &mut HashMap<PlanNodeId, ColumnFacts>) -> ColumnFacts {
    if let Some(facts) = memo.get(&plan.id()) {
        return facts.clone();
    }
    let facts = derive_column_facts(plan, memo);
    memo.insert(plan.id(), facts.clone());
    facts
}

fn derive_column_facts(plan: &PlanRef, memo: &mut HashMap<PlanNodeId, ColumnFacts>) -> ColumnFacts {
    if let Some(values) = plan.as_logical_values() {
        let mut facts = ColumnFacts::default();
        let Some((first, rest)) = values.rows().split_first() else {
            return facts;
        };
        for (index, expr) in first.iter().enumerate() {
            let (literal, non_null) = expr_facts(expr, &ColumnFacts::default());
            let others: Vec<_> = rest
                .iter()
                .map(|row| expr_facts(&row[index], &ColumnFacts::default()))
                .collect();
            if let Some(literal) = literal
                && others
                    .iter()
                    .all(|(other, _)| other.as_ref() == Some(&literal))
            {
                facts.constants.insert(index, literal);
            } else if non_null && others.iter().all(|(_, non_null)| *non_null) {
                facts.non_null.insert(index);
            }
        }
        return facts;
    }
    if let Some(scan) = plan.as_logical_scan() {
        // The predicate refers to the columns of the table.
        let mut facts = table_constraint_facts(scan);
        facts.merge(predicate_facts(&scan.predicate().conjunctions));
        return facts.select(scan.output_col_idx().iter().copied());
    }
    if let Some(filter) = plan.as_logical_filter() {
        let mut facts = column_facts_inner(&filter.input(), memo);
        facts.merge(predicate_facts(&filter.predicate().conjunctions));
        return facts;
    }
    if let Some(project) = plan.as_logical_project() {
        let input = column_facts_inner(&project.input(), memo);
        let mut facts = ColumnFacts::default();
        for (index, expr) in project.exprs().iter().enumerate() {
            match expr_facts(expr, &input) {
                (Some(literal), _) => {
                    facts.constants.insert(index, literal);
                }
                (None, true) => {
                    facts.non_null.insert(index);
                }
                (None, false) => {}
            }
        }
        return facts;
    }
    if let Some(join) = plan.as_logical_join() {
        let left_len = join.left().schema().len();
        let left = column_facts_inner(&join.left(), memo);
        let right = column_facts_inner(&join.right(), memo);
        // The facts about the internal columns of the join.
        let internal = match join.join_type() {
            JoinType::Inner => {
                let mut facts = left;
                facts.merge(right.shift(left_len));
                facts.merge(predicate_facts(&join.on().conjunctions));
                facts
            }
            JoinType::LeftOuter => left,
            JoinType::RightOuter => right.shift(left_len),
            // The internal columns of a semi or anti join are those of the side it keeps.
            JoinType::LeftSemi | JoinType::LeftAnti => left,
            JoinType::RightSemi | JoinType::RightAnti => right,
            _ => ColumnFacts::default(),
        };
        return internal.select(join.output_indices().iter().copied());
    }
    if plan.as_logical_union().is_some() {
        let mut inputs = plan.inputs().into_iter();
        let Some(first) = inputs.next() else {
            return ColumnFacts::default();
        };
        let mut facts = column_facts_inner(&first, memo);
        for input in inputs {
            facts = facts.intersect(&column_facts_inner(&input, memo));
        }
        return facts;
    }
    if let Some(agg) = plan.as_logical_agg() {
        if !agg.grouping_sets().is_empty() {
            return ColumnFacts::default();
        }
        // The output columns are the group key columns, then the aggregate calls.
        let mut facts = column_facts_inner(&agg.input(), memo).select(agg.group_key().indices());
        let group_key_len = agg.group_key().len();
        for (index, call) in agg.agg_calls().iter().enumerate() {
            if agg_is_non_null(&call.agg_type) {
                facts.non_null.insert(group_key_len + index);
            }
        }
        return facts;
    }
    if plan.as_logical_share().is_some()
        || plan.as_logical_dedup().is_some()
        || plan.as_logical_top_n().is_some()
        || plan.as_logical_limit().is_some()
    {
        let inputs = plan.inputs();
        if let [input] = &inputs[..] {
            return column_facts_inner(input, memo);
        }
    }
    ColumnFacts::default()
}

/// Replaces the columns that are known to be constant with their value, and `IS [NOT] NULL` on an
/// expression that is known to be never `NULL` (see [`expr_facts`]) with its result.
struct FactsRewriter<'a> {
    facts: &'a ColumnFacts,
}

impl ExprRewriter for FactsRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        match self.facts.constant(input_ref.index()) {
            Some(literal) => literal.clone().into(),
            None => input_ref.into(),
        }
    }

    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let func_type = func_call.func_type();
        if matches!(func_type, ExprType::IsNull | ExprType::IsNotNull)
            && let [input] = func_call.inputs()
            && expr_facts(input, self.facts).1
        {
            return ExprImpl::literal_bool(func_type == ExprType::IsNotNull);
        }
        let (func_type, inputs, return_type) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new_unchecked(func_type, inputs, return_type).into()
    }
}

/// Evaluates the constant sub-expressions, and removes the branches of the `CASE` expressions
/// whose condition is a constant. A constant sub-expression that fails to evaluate is kept.
struct ConstantFolder;

impl ExprRewriter for ConstantFolder {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if !matches!(expr, ExprImpl::Literal(_))
            && let Some(Ok(value)) = expr.try_fold_const()
        {
            return Literal::new(value, expr.return_type()).into();
        }
        default_rewrite_expr(self, expr)
    }

    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, return_type) = func_call.decompose();
        let inputs: Vec<_> = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        if func_type == ExprType::Case {
            simplify_case(inputs, return_type)
        } else {
            FunctionCall::new_unchecked(func_type, inputs, return_type).into()
        }
    }
}

/// Simplifies `CASE WHEN c1 THEN r1 WHEN c2 THEN r2 ... [ELSE e] END`, given as
/// `[c1, r1, c2, r2, ..., e]`:
///
/// - a branch whose condition is the constant `FALSE` or `NULL` is never taken and is removed;
/// - a branch whose condition is the constant `TRUE` is always taken when it is reached: the
///   branches after it are removed and its result becomes the `ELSE` result.
///
/// Without any branch left, the expression is its `ELSE` result, or `NULL`.
fn simplify_case(inputs: Vec<ExprImpl>, return_type: DataType) -> ExprImpl {
    let original = inputs.clone();
    let rebuild = |inputs: Vec<ExprImpl>| -> ExprImpl {
        FunctionCall::new_unchecked(ExprType::Case, inputs, return_type.clone()).into()
    };
    let mut inputs = inputs;
    let mut otherwise = if inputs.len() % 2 == 1 {
        inputs.pop()
    } else {
        None
    };
    let mut branches = Vec::with_capacity(inputs.len());
    let mut iter = inputs.into_iter();
    while let (Some(condition), Some(result)) = (iter.next(), iter.next()) {
        let is_null_literal =
            matches!(&condition, ExprImpl::Literal(literal) if literal.get_data().is_none());
        match try_get_bool_constant(&condition) {
            Some(true) => {
                otherwise = Some(result);
                break;
            }
            Some(false) => {}
            None if is_null_literal => {}
            None => {
                branches.push(condition);
                branches.push(result);
            }
        }
    }
    if branches.is_empty() {
        let result = otherwise.unwrap_or_else(|| ExprImpl::literal_null(return_type.clone()));
        // The results of a `CASE` are cast to its type by the binder: keep the expression as it
        // is if that does not hold.
        return if result.return_type() == return_type {
            result
        } else {
            rebuild(original)
        };
    }
    branches.extend(otherwise);
    rebuild(branches)
}

/// Rewrites an expression with the facts about the columns it refers to, then folds what became
/// constant. Returns the expression unchanged if nothing applies to it.
fn simplify_expr(expr: &ExprImpl, facts: &ColumnFacts) -> ExprImpl {
    let rewritten = FactsRewriter { facts }.rewrite_expr(expr.clone());
    if rewritten == *expr {
        return rewritten;
    }
    let folded = ConstantFolder.rewrite_expr(rewritten);
    if folded.return_type() == DataType::Boolean {
        fold_boolean_constant(folded)
    } else {
        folded
    }
}

/// Whether a conjunct must be left as it is.
fn is_protected_conjunct(expr: &ExprImpl, is_join_condition: bool) -> bool {
    // A temporal filter compares a column with `now()`.
    expr.count_nows() > 0
        // The equalities between a column of each side are the keys of the join.
        || (is_join_condition
            && (expr.as_eq_cond().is_some() || expr.as_is_not_distinct_from_cond().is_some()))
}

/// Simplifies the conjuncts of a predicate evaluated with filter semantics, with the facts about
/// its input columns (`base`) and the facts that the other conjuncts imply. Returns `None` if
/// nothing changes.
///
/// The conjuncts are rewritten one after the other, each with the facts of the *current* form of
/// the others, so that two conjuncts can not be removed on behalf of each other: replacing one
/// conjunct with an expression that is equivalent whenever the others are `TRUE` keeps the
/// conjunction equivalent.
fn simplify_conjuncts(
    conjuncts: &[ExprImpl],
    base: &ColumnFacts,
    is_join_condition: bool,
) -> Option<Vec<ExprImpl>> {
    let mut current = conjuncts.to_vec();
    let mut changed = false;
    let mut position = 0;
    while position < current.len() {
        // A constant conjunct that is `TRUE` filters nothing.
        if constant_value(&current[position]) == Some(Some(ScalarImpl::Bool(true))) {
            current.remove(position);
            changed = true;
            continue;
        }
        if is_protected_conjunct(&current[position], is_join_condition) {
            position += 1;
            continue;
        }
        let mut facts = base.clone();
        facts.merge(predicate_facts(
            current
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != position)
                .map(|(_, expr)| expr),
        ));
        let rewritten = simplify_expr(&current[position], &facts);
        if rewritten == current[position] {
            position += 1;
            continue;
        }
        changed = true;
        if try_get_bool_constant(&rewritten) == Some(true) {
            current.remove(position);
        } else {
            current[position] = rewritten;
            position += 1;
        }
    }
    if let Some(reduced) = drop_redundant_bounds(&current) {
        current = reduced;
        changed = true;
    }
    changed.then_some(current)
}

/// Removes the bounds of a column that a tighter bound of the same column implies, e.g. `x > 3`
/// next to `x > 5`. Returns `None` if nothing changes.
fn drop_redundant_bounds(conjuncts: &[ExprImpl]) -> Option<Vec<ExprImpl>> {
    // Per column and per direction (`true` for a lower bound): the position of the tightest
    // bound so far, its value and whether it is inclusive.
    let mut tightest: BTreeMap<(usize, bool), (usize, ScalarImpl, bool)> = BTreeMap::new();
    let mut redundant = BTreeSet::new();
    for (position, expr) in conjuncts.iter().enumerate() {
        let Some((index, func_type, value)) = column_comparison(expr) else {
            continue;
        };
        let (is_lower, inclusive) = match func_type {
            ExprType::GreaterThan => (true, false),
            ExprType::GreaterThanOrEqual => (true, true),
            ExprType::LessThan => (false, false),
            ExprType::LessThanOrEqual => (false, true),
            _ => continue,
        };
        let Some((current_position, current, current_inclusive)) = tightest.get(&(index, is_lower))
        else {
            tightest.insert((index, is_lower), (position, value, inclusive));
            continue;
        };
        let ordering = if is_lower {
            value.default_cmp(current)
        } else {
            current.default_cmp(&value)
        };
        // The new bound is tighter if it is further inside, or the same but exclusive.
        let is_tighter = ordering.is_gt() || (ordering.is_eq() && !inclusive && *current_inclusive);
        if is_tighter {
            redundant.insert(*current_position);
            tightest.insert((index, is_lower), (position, value, inclusive));
        } else {
            redundant.insert(position);
        }
    }
    if redundant.is_empty() {
        return None;
    }
    Some(
        conjuncts
            .iter()
            .enumerate()
            .filter(|(position, _)| !redundant.contains(position))
            .map(|(_, expr)| expr.clone())
            .collect(),
    )
}

/// Simplifies the expressions of a plan node with what is known about its input columns (see
/// [`column_facts`]). Must be applied bottom-up, before [`NeverTruePredicateRule`] so that a
/// predicate that became constant is recognized.
///
/// - `Filter`, `Scan` and `Join` predicates (see [`simplify_conjuncts`]): a column known to be
///   constant is replaced with its value, `x IS NULL` on a column that is never `NULL` becomes
///   `FALSE`, the conjuncts that became `TRUE` and the redundant bounds are removed. The
///   equalities between the two sides of a join and the temporal filters are left as they are;
///   as-of joins are left untouched.
/// - `Project`: the same replacements inside the expressions. An expression that is just a
///   column is kept, so that the keys and the watermarks of the plan still see a column.
///
/// The rule is only used by the streaming optimizer, on demand.
///
/// [`NeverTruePredicateRule`]: super::NeverTruePredicateRule
pub struct ColumnFactsSimplifyRule;

impl Rule<Logical> for ColumnFactsSimplifyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if let Some(filter) = plan.as_logical_filter() {
            let input = filter.input();
            let conjuncts = simplify_conjuncts(
                &filter.predicate().conjunctions,
                &column_facts(&input),
                false,
            )?;
            return Some(LogicalFilter::create(input, to_condition(conjuncts)));
        }
        if let Some(scan) = plan.as_logical_scan() {
            // The predicate refers to the columns of the table, as the facts of its constraints.
            let conjuncts = simplify_conjuncts(
                &scan.predicate().conjunctions,
                &table_constraint_facts(scan),
                false,
            )?;
            return Some(scan.clone_with_predicate(to_condition(conjuncts)).into());
        }
        if let Some(join) = plan.as_logical_join() {
            return Self::apply_join(&plan, join);
        }
        let project = plan.as_logical_project()?;
        let input = project.input();
        let facts = column_facts(&input);
        let exprs: Vec<ExprImpl> = project
            .exprs()
            .iter()
            .map(|expr| {
                if expr.as_input_ref().is_some() {
                    expr.clone()
                } else {
                    simplify_expr(expr, &facts)
                }
            })
            .collect();
        (exprs != *project.exprs()).then(|| LogicalProject::create(input, exprs))
    }
}

impl ColumnFactsSimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(ColumnFactsSimplifyRule)
    }

    fn apply_join(plan: &PlanRef, join: &LogicalJoin) -> Option<PlanRef> {
        if matches!(
            join.join_type(),
            JoinType::AsofInner | JoinType::AsofLeftOuter
        ) {
            return None;
        }
        // The condition is evaluated on a row of each side, whatever the type of the join: the
        // facts of both sides hold for it.
        let left_len = join.left().schema().len();
        let mut facts = column_facts(&join.left());
        facts.merge(column_facts(&join.right()).shift(left_len));
        let simplified: Option<PlanRef> = simplify_conjuncts(&join.on().conjunctions, &facts, true)
            .map(|conjuncts| join.clone_with_cond(to_condition(conjuncts)).into());
        let current = simplified.as_ref().unwrap_or(plan);
        let current_join = current.as_logical_join()?;
        // The equalities between the two sides are kept as they are, since they are the keys of
        // the join, but with the constant columns replaced with their value they may show that
        // the condition never holds. E.g. `t.x = 3 AND t.x = 4` in the condition of a left join
        // becomes `t.x = s.c1 AND t.x = s.c2` with two constant columns computed below.
        let substituted = Condition {
            conjunctions: current_join
                .on()
                .conjunctions
                .iter()
                .filter(|expr| expr.count_nows() == 0)
                .map(|expr| simplify_expr(expr, &facts))
                .collect(),
        };
        if is_never_true(&substituted)
            && let Some(folded) = fold_join_without_match(current, current_join)
        {
            return Some(folded);
        }
        simplified
    }
}

fn to_condition(conjuncts: Vec<ExprImpl>) -> Condition {
    conjuncts
        .into_iter()
        .fold(Condition::true_cond(), |condition, expr| {
            condition.and(Condition::with_expr(expr))
        })
}
