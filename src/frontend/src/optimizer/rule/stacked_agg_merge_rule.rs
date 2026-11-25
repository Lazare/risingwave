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

//! Merge of two stacked aggregations into one, in streaming plans, on demand.

use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{LogicalProject, LogicalUnion, PlanTreeNodeUnary};
use crate::utils::{Condition, IndexSet, Substitute};

/// Merges an aggregation over another aggregation into one aggregation over the input of the
/// inner one, when the outer aggregation groups by inner group key columns only:
///
/// ```text
/// Agg(group g: sum(s), sum(c), max(m))          Project(g, s, c::numeric, m)
/// └─Agg(group g, w: s = sum(v), c = count(*),   └─Agg(group g: sum(v::bigint), count(*), max(v))
///       m = max(v))                                 └─input
///   └─input
/// ```
///
/// A group of the outer aggregation exists exactly when a row of the input has its key, in both
/// plans: the inner group key is not empty, so every inner group holds at least one input row.
/// Each outer aggregate is then one of:
///
/// - over an inner aggregate (neither has `DISTINCT` for `sum` and `count`; the outer one has no
///   `FILTER` or `ORDER BY`; the inner `FILTER` is kept):
///   - `sum` of `sum` of an integer or decimal: the sum over the input, of the argument cast to the
///     type of the inner sum. The value and the type are those of the stacked sums; floats are left
///     alone, the order of the additions would change their rounding;
///   - `sum` of `count`: the count over the input, cast to the type of the outer sum. Not without
///     outer group key: over an empty input the outer sum is `NULL`, the count would be 0;
///   - `min` of `min`, `max` of `max`, `bool_and` of `bool_and`, `bool_or` of `bool_or`: the same
///     aggregate over the input. `NULL`s are ignored at both levels, and a group whose values are
///     all `NULL` gives `NULL` at both levels.
/// - over an inner group key column `v` (redundant dedup below the outer aggregation):
///   - `count(DISTINCT v)` and `sum(DISTINCT v)` are the same over the input;
///   - `count(v)` and `sum(v)` are `count(DISTINCT v)` and `sum(DISTINCT v)` over the input when the
///     inner group key is within the outer group key and `v`: each inner group is then one distinct
///     `v` of an outer group. `NULL` is a group of the inner aggregation and is ignored by both.
///
///   Not `min` and `max` of a key column: they would give the same values but their state would
///   hold every input row instead of every distinct value, and not `count(*)`, which counts inner
///   groups. Both need an outer group key.
/// - An outer aggregation without aggregate (a `DISTINCT` or a `GROUP BY` on fewer columns above
///   another one) is a plain dedup of the input on the outer group key.
///
/// Projections between the two aggregations are looked through: an outer group key that they
/// compute from inner group key columns only is computed from the same columns of the input; any
/// other expression stops the rule. A `HAVING` of the inner
/// aggregation is a filter between them and stops the rule, so does a shared inner aggregation.
/// Grouping sets are not merged.
///
/// In streaming, the merged aggregation is an ordinary aggregation of the input: retractions and
/// rows moving between groups are handled by it as by any aggregation. The state and the exchange
/// of the inner aggregation go away; distinct aggregates keep a deduplication state that holds the
/// same rows as the state of the removed inner aggregation.
pub struct StackedAggMergeRule;

/// A column of the output of the inner aggregation.
enum InnerColumn {
    /// A group key column, as the column of the input of the inner aggregation.
    Key(usize),
    /// An aggregate call, as its index among the calls of the inner aggregation.
    Call(usize),
}

impl Rule<Logical> for StackedAggMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer = plan.as_logical_agg()?;
        if !outer.grouping_sets().is_empty() {
            return None;
        }
        // Each column of the outer input as an expression of the output of the inner aggregation,
        // through the projections in between (column pruning leaves one per level).
        let (inner_plan, to_inner) = through_projects(outer.input());
        let inner = inner_plan.as_logical_agg()?;
        if !inner.grouping_sets().is_empty() || inner.group_key().is_empty() {
            return None;
        }
        let input = inner.input();
        let input_len = input.schema().len();
        let inner_keys: Vec<usize> = inner.group_key().indices().collect();
        // A column of the outer input that is a column of the inner aggregation.
        let column = |outer_input_column: usize| -> Option<InnerColumn> {
            let inner_column = to_inner[outer_input_column].as_input_ref()?.index();
            Some(if inner_column < inner_keys.len() {
                InnerColumn::Key(inner_keys[inner_column])
            } else {
                InnerColumn::Call(inner_column - inner_keys.len())
            })
        };
        // The inner group key columns as expressions of the input.
        let key_exprs: Vec<ExprImpl> = inner_keys
            .iter()
            .map(|&key| InputRef::new(key, input.schema()[key].data_type()).into())
            .collect();

        // Expressions of the input appended after its columns: casts of arguments, and group keys
        // computed from inner group key columns.
        let mut extra_exprs: Vec<ExprImpl> = vec![];

        // The outer group key, as columns of the input (or of the appended expressions), in the
        // order of the outer output. A group key computed by a projection from inner group key
        // columns only is computed from the same columns of the input.
        let mut group_columns = vec![];
        for index in outer.group_key().indices() {
            match column(index) {
                Some(InnerColumn::Key(input_column)) => group_columns.push(input_column),
                Some(InnerColumn::Call(_)) => return None,
                None => {
                    let expr = &to_inner[index];
                    let reads_keys_only = expr
                        .collect_input_refs(inner.schema().len())
                        .ones()
                        .all(|column| column < inner_keys.len());
                    if !reads_keys_only || !expr.is_pure() {
                        return None;
                    }
                    let mut substitute = Substitute {
                        mapping: key_exprs.clone(),
                    };
                    extra_exprs.push(substitute.rewrite_expr(expr.clone()));
                    group_columns.push(input_len + extra_exprs.len() - 1);
                }
            }
        }
        let has_group_key = !group_columns.is_empty();
        if !has_group_key && outer.agg_calls().is_empty() {
            return None;
        }
        let new_group_key: IndexSet = group_columns.iter().copied().collect();

        // The merged calls, and the type the result of each must be cast to, if any.
        let mut new_calls: Vec<(PlanAggCall, Option<DataType>)> = vec![];
        for call in outer.agg_calls() {
            let AggType::Builtin(kind) = &call.agg_type else {
                return None;
            };
            if call.inputs.len() != 1
                || !call.direct_args.is_empty()
                || !call.order_by.is_empty()
                || !call.filter.always_true()
            {
                return None;
            }
            let new_call = match column(call.inputs[0].index())? {
                InnerColumn::Call(index) => {
                    let inner_call = &inner.agg_calls()[index];
                    let AggType::Builtin(inner_kind) = &inner_call.agg_type else {
                        return None;
                    };
                    if call.distinct
                        || !inner_call.direct_args.is_empty()
                        || !inner_call.order_by.is_empty()
                    {
                        return None;
                    }
                    match (*kind, *inner_kind) {
                        (PbAggKind::Sum, PbAggKind::Sum)
                            if !inner_call.distinct
                                && inner_call.inputs.len() == 1
                                && is_exact_number(&inner_call.inputs[0].return_type()) =>
                        {
                            let argument = &inner_call.inputs[0];
                            let argument_index = if argument.return_type() == inner_call.return_type
                            {
                                argument.index()
                            } else {
                                let cast = ExprImpl::from(argument.clone())
                                    .cast_explicit(&inner_call.return_type)
                                    .ok()?;
                                extra_exprs.push(cast);
                                input_len + extra_exprs.len() - 1
                            };
                            let new_call = PlanAggCall {
                                agg_type: PbAggKind::Sum.into(),
                                return_type: call.return_type.clone(),
                                inputs: vec![InputRef::new(
                                    argument_index,
                                    inner_call.return_type.clone(),
                                )],
                                distinct: false,
                                order_by: vec![],
                                filter: inner_call.filter.clone(),
                                direct_args: vec![],
                            };
                            (new_call, None)
                        }
                        (PbAggKind::Sum, PbAggKind::Count)
                            if !inner_call.distinct && has_group_key =>
                        {
                            (inner_call.clone(), Some(call.return_type.clone()))
                        }
                        (PbAggKind::Min, PbAggKind::Min)
                        | (PbAggKind::Max, PbAggKind::Max)
                        | (PbAggKind::BoolAnd, PbAggKind::BoolAnd)
                        | (PbAggKind::BoolOr, PbAggKind::BoolOr)
                            if inner_call.return_type == call.return_type =>
                        {
                            (inner_call.clone(), None)
                        }
                        _ => return None,
                    }
                }
                InnerColumn::Key(input_column) => {
                    if !has_group_key {
                        return None;
                    }
                    let data_type = input.schema()[input_column].data_type.clone();
                    match *kind {
                        PbAggKind::Count => {}
                        PbAggKind::Sum if is_exact_number(&data_type) => {}
                        _ => return None,
                    }
                    // Without `DISTINCT`, each value must be counted once per outer group: the
                    // inner group key must be within the outer group key and the argument.
                    if !call.distinct
                        && !inner_keys
                            .iter()
                            .all(|key| *key == input_column || group_columns.contains(key))
                    {
                        return None;
                    }
                    let new_call = PlanAggCall {
                        agg_type: call.agg_type.clone(),
                        return_type: call.return_type.clone(),
                        inputs: vec![InputRef::new(input_column, data_type)],
                        distinct: true,
                        order_by: vec![],
                        filter: Condition::true_cond(),
                        direct_args: vec![],
                    };
                    (new_call, None)
                }
            };
            new_calls.push(new_call);
        }

        let new_input = if extra_exprs.is_empty() {
            input
        } else {
            let exprs: Vec<ExprImpl> = (0..input_len)
                .map(|index| InputRef::new(index, input.schema()[index].data_type.clone()).into())
                .chain(extra_exprs)
                .collect();
            LogicalProject::create(input, exprs)
        };
        let group_key_len = new_group_key.len();
        let group_key_positions: Vec<usize> = new_group_key.indices().collect();
        let mut exprs: Vec<ExprImpl> = vec![];
        for (output, input_column) in group_columns.iter().enumerate() {
            let position = group_key_positions
                .iter()
                .position(|column| column == input_column)?;
            let data_type = outer.schema()[output].data_type.clone();
            exprs.push(InputRef::new(position, data_type).into());
        }
        let mut calls = vec![];
        for (index, (new_call, cast_to)) in new_calls.into_iter().enumerate() {
            let result: ExprImpl =
                InputRef::new(group_key_len + index, new_call.return_type.clone()).into();
            exprs.push(match cast_to {
                Some(data_type) => result.cast_explicit(&data_type).ok()?,
                None => result,
            });
            calls.push(new_call);
        }
        let new_agg: PlanRef = Agg::new(calls, new_group_key, new_input).into();
        Some(LogicalProject::create(new_agg, exprs))
    }
}

/// Integer and decimal types: a sum of them does not depend on the order of the additions.
fn is_exact_number(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Decimal
    )
}

impl StackedAggMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(StackedAggMergeRule)
    }
}

/// Follows the projections down from `plan`. Returns the first node that is not a projection,
/// and each column of `plan` as an expression of the columns of that node.
fn through_projects(plan: PlanRef) -> (PlanRef, Vec<ExprImpl>) {
    let mut exprs: Vec<ExprImpl> = (0..plan.schema().len())
        .map(|index| InputRef::new(index, plan.schema()[index].data_type()).into())
        .collect();
    let mut node = plan;
    while let Some(project) = node.as_logical_project() {
        let mut substitute = Substitute {
            mapping: project.exprs().clone(),
        };
        exprs = exprs
            .into_iter()
            .map(|expr| substitute.rewrite_expr(expr))
            .collect();
        let next = project.input();
        node = next;
    }
    (node, exprs)
}

/// Merges an aggregation over a `UNION ALL` whose branches aggregate their rows on the outer group
/// key into one aggregation over the union of the rows of the branches:
///
/// ```text
/// Agg(group g: sum(s), max(m))                 Agg(group g: sum(s), max(m))
/// └─Union all                                  └─Union all
///   ├─Agg(group g: s = sum(v), m = max(ts))      ├─Project(g, v::bigint, ts)
///   │ └─a                                        │ └─a
///   └─Agg(group g: s = sum(v), m = max(ts))      └─Project(g, v::bigint, ts)
///     └─Filter(v > 3) <- b                         └─Filter(v > 3) <- b
/// ```
///
/// - The outer aggregates are `sum`, `min`, `max`, `bool_and` and `bool_or`, without `DISTINCT`,
///   `FILTER` or `ORDER BY`.
/// - A branch that is an aggregation (through projections of columns) with a group key must have
///   the outer group key columns in its group key, and give each column that the outer aggregates
///   read with an aggregate that decomposes, as in [`StackedAggMergeRule`]: `sum` of `sum` of an
///   integer or a decimal, `sum` of `count` (with an outer group key), `min` of `min`, `max` of
///   `max`, `bool_and` and `bool_or` of themselves. It then contributes the rows of its input: the
///   argument of its aggregate, cast to the type of the aggregate, `NULL` where its `FILTER` does
///   not hold; `1` or `0` for a count. A group of such a branch exists exactly when its input has a
///   row with that key, so the outer groups are the same.
/// - Any other branch (rows, an aggregation with a `HAVING`, without group key) contributes its
///   rows as they are: the outer aggregation already reads them one by one.
///
/// At least one branch must be an aggregation that is removed.
pub struct StackedAggOverUnionMergeRule;

impl Rule<Logical> for StackedAggOverUnionMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer = plan.as_logical_agg()?;
        if !outer.grouping_sets().is_empty() {
            return None;
        }
        let (union_plan, to_union) = through_projects(outer.input());
        let union = union_plan.as_logical_union()?;
        if !union.all() || union.source_col().is_some() {
            return None;
        }
        let union_column =
            |column: usize| -> Option<usize> { Some(to_union[column].as_input_ref()?.index()) };
        let has_group_key = !outer.group_key().is_empty();
        let group_union_columns = outer
            .group_key()
            .indices()
            .map(union_column)
            .collect::<Option<Vec<_>>>()?;
        // The outer calls, each with its kind and the union column it reads.
        let mut calls = vec![];
        for call in outer.agg_calls() {
            let AggType::Builtin(kind) = &call.agg_type else {
                return None;
            };
            if call.inputs.len() != 1
                || call.distinct
                || !call.direct_args.is_empty()
                || !call.order_by.is_empty()
                || !call.filter.always_true()
                || !matches!(
                    kind,
                    PbAggKind::Sum
                        | PbAggKind::Min
                        | PbAggKind::Max
                        | PbAggKind::BoolAnd
                        | PbAggKind::BoolOr
                )
            {
                return None;
            }
            calls.push((*kind, union_column(call.inputs[0].index())?));
        }
        // The union columns that the merged aggregation reads, in the order of the new union.
        let mut needed: Vec<usize> = group_union_columns
            .iter()
            .copied()
            .chain(calls.iter().map(|(_, column)| *column))
            .collect();
        needed.sort_unstable();
        needed.dedup();
        if needed.iter().any(|column| {
            group_union_columns.contains(column) && calls.iter().any(|(_, c)| c == column)
        }) {
            return None;
        }

        let mut removed_aggregation = false;
        let mut branches = vec![];
        for branch in union_plan.inputs() {
            let (inner_plan, to_inner) = through_projects(branch.clone());
            let inner = inner_plan
                .as_logical_agg()
                .filter(|agg| agg.grouping_sets().is_empty() && !agg.group_key().is_empty());
            let Some(inner) = inner else {
                let exprs = needed
                    .iter()
                    .map(|&column| {
                        InputRef::new(column, branch.schema()[column].data_type()).into()
                    })
                    .collect();
                branches.push(LogicalProject::create(branch.clone(), exprs));
                continue;
            };
            removed_aggregation = true;
            let input = inner.input();
            let inner_keys: Vec<usize> = inner.group_key().indices().collect();
            let mut exprs = vec![];
            for &column in &needed {
                let inner_column = to_inner[column].as_input_ref()?.index();
                if group_union_columns.contains(&column) {
                    // A group key column of the outer aggregation: a group key column here.
                    let key = *inner_keys.get(inner_column)?;
                    exprs.push(InputRef::new(key, input.schema()[key].data_type()).into());
                    continue;
                }
                let call = &inner.agg_calls()[inner_column.checked_sub(inner_keys.len())?];
                let mut contribution = None;
                for (kind, _) in calls.iter().filter(|(_, c)| *c == column) {
                    let expr = contribution_of(call, *kind, has_group_key)?;
                    if contribution
                        .as_ref()
                        .is_some_and(|existing| *existing != expr)
                    {
                        return None;
                    }
                    contribution = Some(expr);
                }
                exprs.push(contribution?);
            }
            branches.push(LogicalProject::create(input, exprs));
        }
        if !removed_aggregation {
            return None;
        }
        let new_union: PlanRef = LogicalUnion::create(true, branches);
        let position = |column: usize| needed.iter().position(|c| *c == column).unwrap();
        let new_group_key: IndexSet = group_union_columns.iter().map(|c| position(*c)).collect();
        let new_calls = outer
            .agg_calls()
            .iter()
            .zip_eq(&calls)
            .map(|(call, (_, column))| {
                let mut call = call.clone();
                call.inputs = vec![InputRef::new(
                    position(*column),
                    new_union.schema()[position(*column)].data_type(),
                )];
                call
            })
            .collect_vec();
        let group_key_positions: Vec<usize> = new_group_key.indices().collect();
        let mut exprs: Vec<ExprImpl> = vec![];
        for (output, column) in group_union_columns.iter().enumerate() {
            let index = group_key_positions
                .iter()
                .position(|c| *c == position(*column))?;
            exprs.push(InputRef::new(index, outer.schema()[output].data_type()).into());
        }
        let group_key_len = group_key_positions.len();
        for (index, call) in new_calls.iter().enumerate() {
            exprs.push(InputRef::new(group_key_len + index, call.return_type.clone()).into());
        }
        let new_agg: PlanRef = Agg::new(new_calls, new_group_key, new_union).into();
        Some(LogicalProject::create(new_agg, exprs))
    }
}

impl StackedAggOverUnionMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(StackedAggOverUnionMergeRule)
    }
}

/// The value that a row of the input of `call` contributes to an outer aggregate `kind` of the
/// results of `call`, as an expression of that row, when the outer aggregate decomposes.
fn contribution_of(call: &PlanAggCall, kind: PbAggKind, has_group_key: bool) -> Option<ExprImpl> {
    let AggType::Builtin(inner_kind) = &call.agg_type else {
        return None;
    };
    if !call.direct_args.is_empty() || !call.order_by.is_empty() {
        return None;
    }
    let filter = call.filter.as_expr_unless_true();
    let guarded = |value: ExprImpl, otherwise: ExprImpl| -> ExprImpl {
        match &filter {
            Some(condition) => {
                let return_type = value.return_type();
                FunctionCall::new_unchecked(
                    ExprType::Case,
                    vec![condition.clone(), value, otherwise],
                    return_type,
                )
                .into()
            }
            None => value,
        }
    };
    match (kind, *inner_kind) {
        (PbAggKind::Sum, PbAggKind::Sum)
            if !call.distinct
                && call.inputs.len() == 1
                && is_exact_number(&call.inputs[0].return_type()) =>
        {
            let value = ExprImpl::from(call.inputs[0].clone())
                .cast_explicit(&call.return_type)
                .ok()?;
            Some(guarded(
                value,
                ExprImpl::literal_null(call.return_type.clone()),
            ))
        }
        (PbAggKind::Sum, PbAggKind::Count)
            if !call.distinct && has_group_key && call.return_type == DataType::Int64 =>
        {
            let one = match call.inputs.first() {
                None => ExprImpl::literal_bigint(1),
                Some(argument) => {
                    let is_not_null = FunctionCall::new_unchecked(
                        ExprType::IsNotNull,
                        vec![argument.clone().into()],
                        DataType::Boolean,
                    )
                    .into();
                    FunctionCall::new_unchecked(
                        ExprType::Case,
                        vec![
                            is_not_null,
                            ExprImpl::literal_bigint(1),
                            ExprImpl::literal_bigint(0),
                        ],
                        DataType::Int64,
                    )
                    .into()
                }
            };
            Some(guarded(one, ExprImpl::literal_bigint(0)))
        }
        (PbAggKind::Min, PbAggKind::Min)
        | (PbAggKind::Max, PbAggKind::Max)
        | (PbAggKind::BoolAnd, PbAggKind::BoolAnd)
        | (PbAggKind::BoolOr, PbAggKind::BoolOr)
            if call.inputs.len() == 1 =>
        {
            let value: ExprImpl = call.inputs[0].clone().into();
            if value.return_type() != call.return_type {
                return None;
            }
            Some(guarded(
                value,
                ExprImpl::literal_null(call.return_type.clone()),
            ))
        }
        _ => None,
    }
}
