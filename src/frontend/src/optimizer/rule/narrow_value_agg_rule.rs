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

//! Narrowing of the values that `first_value`, `last_value` and `internal_last_seen_value` keep
//! (`batch_narrow_value_aggregates`, `streaming_narrow_value_aggregates`).
//!
//! A "latest version per key" relation, `SELECT id, last_value(doc ORDER BY ts) AS doc FROM t
//! GROUP BY id`, is often read through a few fields of the value only (`doc->>'status'`). The
//! aggregation still keeps the whole value of each group: in memory in batch, in its state
//! table in streaming. [`NarrowValueAggRule`] moves the functions that read the value below the
//! aggregation, `f(last_value(x ORDER BY k))` becoming `last_value(f(x) ORDER BY k)`, so that the
//! aggregation only keeps what is read. [`NarrowValuePushRule`] then moves those functions further
//! down, into the branches of a `UNION ALL` and into the side of a join that the value comes
//! from, so that a lower aggregation of the value is narrowed in turn and the join and the union
//! carry the narrow value.
//!
//! Why the result is the same:
//! - The row that the aggregate picks depends on the order key (and, on ties, on the order of the
//!   input rows in batch or on the stream key in streaming), never on the value. Applying `f` to
//!   each row and then picking the row, or picking the row and then applying `f` to its value,
//!   gives `f` of the value of the same row, `NULL` included (`f` sees the same `NULL`).
//! - Several functions of the same value are packed into one `ROW(f1(x), f2(x), ...)` that one
//!   aggregate keeps, so that all of them come from the same row even when the order key ties.
//! - `f` is only made of functions that are deterministic, pure and cannot fail (see
//!   [`is_narrowing_function`]): evaluated on every input row instead of the selected one, it
//!   must not raise an error that the original query would not raise.
//! - An aggregate that may see no row (no group key, or a `FILTER` clause) returns `NULL`, and the
//!   original query applies `f` to that `NULL`: then `f` must return `NULL` on `NULL`
//!   ([`Narrowing::null_on_null`]); otherwise a smaller part of `f` is moved.
//! - `DISTINCT` aggregates are not rewritten (the distinct values of `f(x)` are not those of `x`).
//! - A value read in any other way (output as it is, compared in a join, a grouping key...) is
//!   not narrowed: the whole value is needed anyway.

use std::collections::{HashMap, HashSet};

use risingwave_common::types::{DataType, ScalarImpl, StructType};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::ProjectMergeRule;
use super::prelude::{PlanRef, *};
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, Literal,
    default_rewrite_expr,
};
use crate::optimizer::plan_expr_visitor::InputRefCounter;
use crate::optimizer::plan_node::generic::{GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalFilter, LogicalJoin, LogicalProject, LogicalScan, LogicalUnion,
    PlanTreeNode, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::{ColIndexMapping, Condition};

/// Whether a value of this type may be large enough to be worth narrowing.
fn is_wide(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Jsonb
            | DataType::Varchar
            | DataType::Bytea
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::Map(_)
    )
}

/// The functions that may be moved below an aggregation, a union or a join: deterministic, pure
/// and unable to fail on any input, so that evaluating them on more rows (every row of a group
/// instead of the selected one) changes nothing but the cost.
fn is_narrowing_function(call: &FunctionCall) -> bool {
    match call.func_type() {
        ExprType::JsonbAccess
        | ExprType::JsonbAccessStr
        | ExprType::JsonbExtractPath
        | ExprType::JsonbExtractPathVariadic
        | ExprType::JsonbExtractPathText
        | ExprType::JsonbExtractPathTextVariadic
        | ExprType::JsonbTypeof
        | ExprType::Field
        | ExprType::Row
        | ExprType::IsNull
        | ExprType::IsNotNull
        | ExprType::IsTrue
        | ExprType::IsNotTrue
        | ExprType::IsFalse
        | ExprType::IsNotFalse
        | ExprType::Equal
        | ExprType::NotEqual
        | ExprType::LessThan
        | ExprType::LessThanOrEqual
        | ExprType::GreaterThan
        | ExprType::GreaterThanOrEqual
        | ExprType::IsDistinctFrom
        | ExprType::IsNotDistinctFrom
        | ExprType::And
        | ExprType::Or
        | ExprType::Not
        | ExprType::Case
        | ExprType::Coalesce => true,
        ExprType::Cast => is_infallible_cast(&call.inputs()[0].return_type(), &call.return_type()),
        _ => false,
    }
}

/// The casts that cannot fail: to text from a scalar type whose text form does not depend on the
/// session, and the implicit widening between numeric types.
fn is_infallible_cast(source: &DataType, target: &DataType) -> bool {
    fn numeric_rank(data_type: &DataType) -> Option<usize> {
        match data_type {
            DataType::Int16 => Some(0),
            DataType::Int32 => Some(1),
            DataType::Int64 => Some(2),
            DataType::Decimal => Some(3),
            DataType::Float32 => Some(4),
            DataType::Float64 => Some(5),
            _ => None,
        }
    }
    if *target == DataType::Varchar {
        return matches!(
            source,
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Decimal
                | DataType::Float32
                | DataType::Float64
                | DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::Varchar
                | DataType::Jsonb
        );
    }
    matches!((numeric_rank(source), numeric_rank(target)), (Some(s), Some(t)) if s <= t)
}

/// The functions that return `NULL` when one of their arguments is `NULL`.
fn is_null_on_null_function(func_type: ExprType) -> bool {
    matches!(
        func_type,
        ExprType::JsonbAccess
            | ExprType::JsonbAccessStr
            | ExprType::JsonbExtractPath
            | ExprType::JsonbExtractPathVariadic
            | ExprType::JsonbExtractPathText
            | ExprType::JsonbExtractPathTextVariadic
            | ExprType::JsonbTypeof
            | ExprType::Field
            | ExprType::Cast
            | ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
            | ExprType::Not
    )
}

/// The reads of one column of a relation by the expressions of the operator above it.
struct Narrowing {
    /// The column.
    col: usize,
    /// The number of columns of the relation.
    col_num: usize,
    /// Whether the column may be `NULL` where the moved expressions would see another value: the
    /// result of an aggregate that saw no row, or the padding of an outer join. The moved
    /// expressions must then be `NULL` on `NULL`.
    null_on_null: bool,
}

impl Narrowing {
    /// Collects the largest sub-expressions of `expr` that read the column only through
    /// [`is_narrowing_function`]s ("units"). Returns false when `expr` reads the column in another
    /// way: as it is, or through another function.
    fn collect(&self, expr: &ExprImpl, units: &mut Vec<ExprImpl>) -> bool {
        if !expr.collect_input_refs(self.col_num).contains(self.col) {
            return true;
        }
        if expr.as_input_ref().is_none()
            && self.only_narrowing(expr)
            && (!self.null_on_null || self.null_on_null(expr))
        {
            if !units.contains(expr) {
                units.push(expr.clone());
            }
            return true;
        }
        match expr {
            ExprImpl::FunctionCall(call) => call.inputs().iter().all(|e| self.collect(e, units)),
            _ => false,
        }
    }

    /// Whether `expr` is made of literals, the column and narrowing functions only.
    fn only_narrowing(&self, expr: &ExprImpl) -> bool {
        match expr {
            ExprImpl::InputRef(input_ref) => input_ref.index() == self.col,
            ExprImpl::Literal(_) => true,
            ExprImpl::FunctionCall(call) => {
                is_narrowing_function(call) && call.inputs().iter().all(|e| self.only_narrowing(e))
            }
            _ => false,
        }
    }

    /// Whether `expr` is `NULL` when the column is `NULL`.
    fn null_on_null(&self, expr: &ExprImpl) -> bool {
        match expr {
            ExprImpl::InputRef(input_ref) => input_ref.index() == self.col,
            ExprImpl::FunctionCall(call) if is_null_on_null_function(call.func_type()) => {
                call.inputs().iter().any(|e| self.null_on_null(e))
            }
            _ => false,
        }
    }

    /// The units of the column in `exprs`, or `None` when the column is read in another way or not
    /// at all.
    fn units<'a>(&self, exprs: impl IntoIterator<Item = &'a ExprImpl>) -> Option<Vec<ExprImpl>> {
        let mut units = vec![];
        for expr in exprs {
            if !self.collect(expr, &mut units) {
                return None;
            }
        }
        (!units.is_empty()).then_some(units)
    }
}

/// Replaces the units by the columns that now hold them.
struct ReplaceUnits(HashMap<ExprImpl, ExprImpl>);

impl ExprRewriter for ReplaceUnits {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if let Some(replacement) = self.0.get(&expr) {
            return replacement.clone();
        }
        default_rewrite_expr(self, expr)
    }
}

/// Replaces the references to one column by another expression.
struct ReplaceColumn {
    col: usize,
    by: ExprImpl,
}

impl ExprRewriter for ReplaceColumn {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index() == self.col {
            self.by.clone()
        } else {
            input_ref.into()
        }
    }
}

fn replace_column(expr: &ExprImpl, col: usize, by: ExprImpl) -> ExprImpl {
    ReplaceColumn { col, by }.rewrite_expr(expr.clone())
}

/// The identity projection of `plan` followed by `extra`.
fn project_with(plan: PlanRef, extra: Vec<ExprImpl>) -> PlanRef {
    let mut exprs: Vec<ExprImpl> = plan
        .schema()
        .data_types()
        .into_iter()
        .enumerate()
        .map(|(i, t)| InputRef::new(i, t).into())
        .collect();
    exprs.extend(extra);
    LogicalProject::create(plan, exprs)
}

/// Renames the columns read by an expression: `InputRef(i)` becomes `InputRef(map(i))`, with the
/// same type.
struct Renumber<F: Fn(usize) -> usize>(F);

impl<F: Fn(usize) -> usize> ExprRewriter for Renumber<F> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new((self.0)(input_ref.index()), input_ref.return_type()).into()
    }
}

/// The columns of an operator between the reader of an aggregation and the aggregation: for each
/// of its columns, the column of the aggregation that it passes through.
struct Level(Vec<usize>);

impl Level {
    /// `expr`, over the columns of this operator, over the columns of the aggregation instead.
    fn to_agg(&self, expr: ExprImpl) -> ExprImpl {
        Renumber(|i| self.0[i]).rewrite_expr(expr)
    }

    /// `expr`, over the columns of the aggregation, over the columns of this operator instead.
    /// Every column of the aggregation that `expr` reads is one of this operator.
    fn from_agg(&self, expr: ExprImpl) -> ExprImpl {
        Renumber(|a| self.0.iter().position(|&c| c == a).unwrap()).rewrite_expr(expr)
    }

    /// Rewrites `expr`, over the columns of this operator, with `rewriter`, which works over the
    /// columns of the aggregation.
    fn rewrite(&self, expr: ExprImpl, rewriter: &mut ReplaceUnits) -> ExprImpl {
        self.from_agg(rewriter.rewrite_expr(self.to_agg(expr)))
    }

    fn rewrite_condition(&self, condition: &Condition, rewriter: &mut ReplaceUnits) -> Condition {
        Condition {
            conjunctions: condition
                .conjunctions
                .iter()
                .map(|e| self.rewrite(e.clone(), rewriter))
                .collect(),
        }
    }
}

/// The index of the field that `expr` reads, when it is `(col).k` directly.
fn field_of(expr: &ExprImpl, col: usize) -> Option<i32> {
    let call = expr.as_function_call()?;
    if call.func_type() != ExprType::Field || call.inputs()[0].as_input_ref()?.index() != col {
        return None;
    }
    match call.inputs()[1].as_literal()?.get_data() {
        Some(ScalarImpl::Int32(k)) => Some(*k),
        _ => None,
    }
}

/// `f(first_value(x ORDER BY k))` becomes `first_value(f(x) ORDER BY k)`, and the same for
/// `last_value` and `internal_last_seen_value`, when the result of the aggregate is only read
/// through such functions `f`, by a projection or an aggregation above it, possibly through
/// filters, limits and projections that only select columns. Several functions of the same
/// aggregate become one aggregate of `ROW(f1(x), f2(x), ...)`, whose fields are read above. See
/// the module documentation.
pub struct NarrowValueAggRule {}

impl NarrowValueAggRule {
    pub fn create() -> BoxedRule {
        Box::new(NarrowValueAggRule {})
    }
}

impl Rule<Logical> for NarrowValueAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        // The operator that reads the result of the aggregation: a projection, or an aggregation
        // that reads it only in the `FILTER` clauses of its calls.
        let consumer_input = if let Some(project) = plan.as_logical_project() {
            project.input()
        } else if let Some(agg) = plan.as_logical_agg() {
            agg.input()
        } else {
            return None;
        };
        // The filters, limits and projections that only select columns between them, top first.
        let mut chain = vec![];
        let mut node = consumer_input;
        while node.as_logical_filter().is_some()
            || node.as_logical_limit().is_some()
            || node
                .as_logical_project()
                .is_some_and(|p| p.is_all_inputref())
        {
            chain.push(node.clone());
            node = node.inputs()[0].clone();
        }
        let agg = node.as_logical_agg()?;
        if !agg.grouping_sets().is_empty() {
            return None;
        }
        let group_len = agg.group_key().len();
        let col_num = agg.schema().len();

        // The columns of each operator of the chain, as columns of the aggregation.
        let mut levels = vec![];
        let mut columns: Vec<usize> = (0..col_num).collect();
        for node in chain.iter().rev() {
            if let Some(project) = node.as_logical_project() {
                columns = project
                    .exprs()
                    .iter()
                    .map(|e| columns[e.as_input_ref().unwrap().index()])
                    .collect();
            }
            levels.push(Level(columns.clone()));
        }
        levels.reverse();
        let consumer_level = Level(columns);

        // The columns that the consumer reads as they are, and the expressions (over the columns
        // of the aggregation) that may read the result through narrowing functions.
        let mut bare_read = vec![false; col_num];
        let mut exprs: Vec<ExprImpl> = vec![];
        if let Some(project) = plan.as_logical_project() {
            exprs.extend(
                project
                    .exprs()
                    .iter()
                    .map(|e| consumer_level.to_agg(e.clone())),
            );
        } else {
            let consumer = plan.as_logical_agg().unwrap();
            let mut read = consumer.group_key().indices().collect::<Vec<_>>();
            for call in consumer.agg_calls() {
                read.extend(call.input_indices());
                read.extend(call.order_by.iter().map(|o| o.column_index));
                exprs.extend(
                    call.filter
                        .conjunctions
                        .iter()
                        .map(|e| consumer_level.to_agg(e.clone())),
                );
            }
            for i in read {
                bare_read[consumer_level.0[i]] = true;
            }
        }
        for (node, level) in chain.iter().zip_eq_fast(&levels) {
            if let Some(filter) = node.as_logical_filter() {
                exprs.extend(
                    filter
                        .predicate()
                        .conjunctions
                        .iter()
                        .map(|e| level.to_agg(e.clone())),
                );
            }
        }

        let input = agg.input();
        let input_len = input.schema().len();
        let mut extra_inputs: Vec<ExprImpl> = vec![];
        let mut new_calls = agg.agg_calls().clone();
        let mut replacements = HashMap::new();
        for (i, call) in agg.agg_calls().iter().enumerate() {
            let PlanAggCall {
                agg_type,
                return_type,
                inputs,
                distinct,
                order_by,
                filter,
                direct_args,
            } = call;
            if !matches!(
                agg_type,
                AggType::Builtin(
                    PbAggKind::FirstValue | PbAggKind::LastValue | PbAggKind::InternalLastSeenValue
                )
            ) || *distinct
                || inputs.len() != 1
                || !direct_args.is_empty()
                || !is_wide(return_type)
            {
                continue;
            }
            let arg = &inputs[0];
            // The value is the order key: kept by the aggregation anyway.
            if order_by.iter().any(|o| o.column_index == arg.index()) {
                continue;
            }
            let col = group_len + i;
            if bare_read[col] {
                continue;
            }
            let narrowing = Narrowing {
                col,
                col_num,
                // An aggregate that may see no row returns `NULL`.
                null_on_null: group_len == 0 || !filter.always_true(),
            };
            let Some(units) = narrowing.units(&exprs) else {
                continue;
            };
            // Every field of a row read as it is: nothing to narrow (the result of a previous
            // rewrite, for example).
            if let DataType::Struct(fields) = return_type {
                let read_fields: Option<HashSet<i32>> =
                    units.iter().map(|u| field_of(u, col)).collect();
                if read_fields.is_some_and(|f| f.len() == fields.len()) {
                    continue;
                }
            }
            let below: Vec<ExprImpl> = units
                .iter()
                .map(|u| replace_column(u, col, arg.clone().into()))
                .collect();
            let (new_input, new_type) = if below.len() == 1 {
                let new_type = below[0].return_type();
                replacements.insert(
                    units[0].clone(),
                    InputRef::new(col, new_type.clone()).into(),
                );
                (below.into_iter().next().unwrap(), new_type)
            } else {
                // One aggregate of a row, so that all the fields come from the same row.
                let new_type =
                    DataType::Struct(StructType::unnamed(below.iter().map(|e| e.return_type())));
                for (k, unit) in units.iter().enumerate() {
                    let field = FunctionCall::new_unchecked(
                        ExprType::Field,
                        vec![
                            InputRef::new(col, new_type.clone()).into(),
                            Literal::new(Some((k as i32).into()), DataType::Int32).into(),
                        ],
                        unit.return_type(),
                    );
                    replacements.insert(unit.clone(), field.into());
                }
                let row = FunctionCall::new_unchecked(ExprType::Row, below, new_type.clone());
                (row.into(), new_type)
            };
            new_calls[i].inputs = vec![InputRef::new(
                input_len + extra_inputs.len(),
                new_type.clone(),
            )];
            new_calls[i].return_type = new_type;
            extra_inputs.push(new_input);
        }
        if extra_inputs.is_empty() {
            return None;
        }

        let mut core = agg.core().clone();
        core.agg_calls = new_calls;
        core.input = project_with(input, extra_inputs);
        let mut result: PlanRef = LogicalAgg::from(core).into();

        let mut rewriter = ReplaceUnits(replacements);
        for (node, level) in chain.iter().zip(&levels).rev() {
            result = if let Some(filter) = node.as_logical_filter() {
                LogicalFilter::new(
                    result,
                    level.rewrite_condition(filter.predicate(), &mut rewriter),
                )
                .into()
            } else if let Some(limit) = node.as_logical_limit() {
                limit.clone_with_input(result).into()
            } else {
                // A projection that selects columns: the types of the narrowed ones changed.
                let project = node.as_logical_project().unwrap();
                let types = result.schema().data_types();
                let exprs = project
                    .exprs()
                    .iter()
                    .map(|e| {
                        let index = e.as_input_ref().unwrap().index();
                        InputRef::new(index, types[index].clone()).into()
                    })
                    .collect();
                LogicalProject::new(result, exprs).into()
            };
        }
        if let Some(project) = plan.as_logical_project() {
            let exprs = project
                .exprs()
                .iter()
                .map(|e| consumer_level.rewrite(e.clone(), &mut rewriter))
                .collect();
            Some(LogicalProject::create(result, exprs))
        } else {
            let consumer = plan.as_logical_agg().unwrap();
            let mut core = consumer.core().clone();
            for call in &mut core.agg_calls {
                call.filter = consumer_level.rewrite_condition(&call.filter, &mut rewriter);
            }
            core.input = result;
            Some(LogicalAgg::from(core).into())
        }
    }
}

/// Merges two projections like `ProjectMergeRule`, also when the outer one reads a computed column
/// of the inner one several times (the computation is then repeated, unless it is impure): the
/// narrowing functions of the outer one must see the expressions of the inner one to be moved
/// further down.
pub struct NarrowValueProjectMergeRule {}

impl NarrowValueProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(NarrowValueProjectMergeRule {})
    }
}

impl Rule<Logical> for NarrowValueProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer = plan.as_logical_project()?;
        let input = outer.input();
        let inner = input.as_logical_project()?;
        let mut counter = InputRefCounter::default();
        for expr in outer.exprs() {
            counter.visit_expr(expr);
        }
        if counter
            .counter
            .iter()
            .any(|(&index, &count)| count > 1 && inner.exprs()[index].is_impure())
        {
            return None;
        }
        let exprs = ProjectMergeRule::merge_project_exprs(outer.exprs(), inner.exprs(), false)?;
        Some(LogicalProject::new(inner.input(), exprs).into())
    }
}

/// Moves the narrowing functions that a projection applies to a wide column of a `UNION ALL` into
/// each branch, and those it applies to a wide column of one side of a join into that side, when
/// the projection reads the column only through them (and the join does not compare it). The
/// union, the join, and the aggregations below them then carry the narrow value only.
pub struct NarrowValuePushRule {}

impl NarrowValuePushRule {
    pub fn create() -> BoxedRule {
        Box::new(NarrowValuePushRule {})
    }
}

impl Rule<Logical> for NarrowValuePushRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_logical_project()?;
        let input = project.input();
        if let Some(union) = input.as_logical_union() {
            Self::push_into_union(project, union)
        } else if let Some(join) = input.as_logical_join() {
            Self::push_into_join(project, join)
        } else {
            None
        }
    }
}

impl NarrowValuePushRule {
    fn push_into_union(project: &LogicalProject, union: &LogicalUnion) -> Option<PlanRef> {
        if !union.all() || union.source_col().is_some() {
            return None;
        }
        let types = union.schema().data_types();
        if union
            .inputs()
            .iter()
            .any(|branch| branch.schema().data_types() != types)
        {
            return None;
        }
        let col_num = types.len();
        let mut units = vec![];
        for (col, data_type) in types.iter().enumerate() {
            if !is_wide(data_type) {
                continue;
            }
            let narrowing = Narrowing {
                col,
                col_num,
                null_on_null: false,
            };
            if let Some(col_units) = narrowing.units(project.exprs()) {
                units.extend(col_units);
            }
        }
        if units.is_empty() {
            return None;
        }
        // A column of the union is the column at the same index in every branch.
        let branches = union
            .inputs()
            .into_iter()
            .map(|branch| project_with(branch, units.clone()))
            .collect();
        let new_union = LogicalUnion::create(true, branches);
        let replacements = units
            .iter()
            .enumerate()
            .map(|(k, unit)| {
                (
                    unit.clone(),
                    InputRef::new(col_num + k, unit.return_type()).into(),
                )
            })
            .collect();
        let mut rewriter = ReplaceUnits(replacements);
        let exprs = project
            .exprs()
            .iter()
            .map(|e| rewriter.rewrite_expr(e.clone()))
            .collect();
        Some(LogicalProject::create(new_union, exprs))
    }

    fn push_into_join(project: &LogicalProject, join: &LogicalJoin) -> Option<PlanRef> {
        let join_type = join.join_type();
        if !matches!(
            join_type,
            JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter
                | JoinType::LeftSemi
                | JoinType::LeftAnti
        ) || join.should_be_temporal_join()
        {
            return None;
        }
        let left = join.left();
        let right = join.right();
        let left_len = left.schema().len();
        let right_len = right.schema().len();
        let output_indices = join.output_indices();
        let condition_cols = join.on().collect_input_refs(left_len + right_len);
        let out_types = join.schema().data_types();
        let col_num = out_types.len();
        let padded_left = matches!(join_type, JoinType::RightOuter | JoinType::FullOuter);
        let padded_right = matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter);
        // A table on the right side may be looked up by the join: keep it a plain scan.
        let right_may_be_looked_up = right
            .as_logical_scan()
            .is_some_and(|scan| scan_may_be_looked_up(scan, join));

        let mut left_units = vec![];
        let mut right_units = vec![];
        let mut sides = HashMap::new();
        for (out, data_type) in out_types.iter().enumerate() {
            let internal = output_indices[out];
            let on_left = internal < left_len;
            if !is_wide(data_type)
                || condition_cols.contains(internal)
                || output_indices.iter().filter(|&&i| i == internal).count() > 1
                || (!on_left && right_may_be_looked_up)
            {
                continue;
            }
            let narrowing = Narrowing {
                col: out,
                col_num,
                null_on_null: if on_left { padded_left } else { padded_right },
            };
            let Some(units) = narrowing.units(project.exprs()) else {
                continue;
            };
            for unit in units {
                let (side_units, local) = if on_left {
                    (&mut left_units, internal)
                } else {
                    (&mut right_units, internal - left_len)
                };
                let below =
                    replace_column(&unit, out, InputRef::new(local, data_type.clone()).into());
                sides.insert(unit, (on_left, side_units.len()));
                side_units.push(below);
            }
        }
        if sides.is_empty() {
            return None;
        }

        let (left_extra, right_extra) = (left_units.len(), right_units.len());
        let new_left = if left_units.is_empty() {
            left
        } else {
            project_with(left, left_units)
        };
        let new_right = if right_units.is_empty() {
            right
        } else {
            project_with(right, right_units)
        };
        // The internal columns: left, new left, right, new right.
        let mut mapping = ColIndexMapping::new(
            (0..left_len + right_len)
                .map(|i| Some(if i < left_len { i } else { i + left_extra }))
                .collect(),
            left_len + left_extra + right_len + right_extra,
        );
        let on: Condition = join.on().clone().rewrite_expr(&mut mapping);
        let mut new_output: Vec<usize> = output_indices
            .iter()
            .map(|&i| if i < left_len { i } else { i + left_extra })
            .collect();
        new_output.extend(left_len..left_len + left_extra);
        new_output.extend(
            left_len + left_extra + right_len..left_len + left_extra + right_len + right_extra,
        );
        let new_join =
            LogicalJoin::with_output_indices(new_left, new_right, join_type, on, new_output);

        let replacements = sides
            .into_iter()
            .map(|(unit, (on_left, k))| {
                let index = if on_left {
                    col_num + k
                } else {
                    col_num + left_extra + k
                };
                let data_type = unit.return_type();
                (unit, InputRef::new(index, data_type).into())
            })
            .collect();
        let mut rewriter = ReplaceUnits(replacements);
        let exprs = project
            .exprs()
            .iter()
            .map(|e| rewriter.rewrite_expr(e.clone()))
            .collect();
        Some(LogicalProject::create(new_join.into(), exprs))
    }
}

/// Whether the join may become a lookup join (batch) or a delta join (streaming) into the table
/// that `scan` reads: the join compares with `=` the first column of the primary key or of an
/// index of the table.
fn scan_may_be_looked_up(scan: &LogicalScan, join: &LogicalJoin) -> bool {
    let right_eq_cols: Vec<usize> = join
        .eq_indexes()
        .into_iter()
        .map(|(_, r)| scan.output_col_idx()[r])
        .collect();
    let pk_first = scan.primary_key().first().map(|o| o.column_index);
    if pk_first.is_some_and(|c| right_eq_cols.contains(&c)) {
        return true;
    }
    scan.table_indexes().iter().any(|index| {
        // The first column of the index, or `None` for an expression.
        index
            .secondary_to_primary_mapping
            .get(&0)
            .is_none_or(|c| right_eq_cols.contains(c))
    })
}

/// Whether `JoinProjectTransposeRule` must keep `project`, an input of a join, below the join:
/// when the rules above applied to the plan being optimized and the projection reads a wide column of its input only through
/// narrowing functions. Pulled above the join, it would make the join carry the whole value again.
pub fn keeps_narrowing_projection_below_join(project: &LogicalProject) -> bool {
    if !project.ctx().values_narrowed() {
        return false;
    }
    let input_types = project.input().schema().data_types();
    let col_num = input_types.len();
    input_types.iter().enumerate().any(|(col, data_type)| {
        is_wide(data_type)
            && Narrowing {
                col,
                col_num,
                null_on_null: false,
            }
            .units(project.exprs())
            .is_some()
    })
}
