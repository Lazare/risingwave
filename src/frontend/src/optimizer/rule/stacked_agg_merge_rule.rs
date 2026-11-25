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

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{LogicalProject, PlanTreeNodeUnary};
use crate::utils::{Condition, IndexSet};

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
/// A projection of columns between the two aggregations is looked through. A `HAVING` of the inner
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
        let outer_input = outer.input();
        // The output column of the inner aggregation that each column of the outer input is.
        let (inner_plan, to_inner): (PlanRef, Vec<usize>) =
            if let Some(project) = outer_input.as_logical_project() {
                let columns = project
                    .exprs()
                    .iter()
                    .map(|expr| expr.as_input_ref().map(|input_ref| input_ref.index()))
                    .collect::<Option<Vec<_>>>()?;
                (project.input(), columns)
            } else {
                (
                    outer_input.clone(),
                    (0..outer_input.schema().len()).collect(),
                )
            };
        let inner = inner_plan.as_logical_agg()?;
        if !inner.grouping_sets().is_empty() || inner.group_key().is_empty() {
            return None;
        }
        let input = inner.input();
        let input_len = input.schema().len();
        let inner_keys: Vec<usize> = inner.group_key().indices().collect();
        let column = |outer_input_column: usize| -> InnerColumn {
            let inner_column = to_inner[outer_input_column];
            if inner_column < inner_keys.len() {
                InnerColumn::Key(inner_keys[inner_column])
            } else {
                InnerColumn::Call(inner_column - inner_keys.len())
            }
        };

        // The outer group key, as columns of the input, in the order of the outer output.
        let group_columns = outer
            .group_key()
            .indices()
            .map(|index| match column(index) {
                InnerColumn::Key(input_column) => Some(input_column),
                InnerColumn::Call(_) => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let has_group_key = !group_columns.is_empty();
        if !has_group_key && outer.agg_calls().is_empty() {
            return None;
        }
        let new_group_key: IndexSet = group_columns.iter().copied().collect();

        // Expressions of the input appended after its columns, for the casts of the arguments.
        let mut extra_exprs: Vec<ExprImpl> = vec![];
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
            let new_call = match column(call.inputs[0].index()) {
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
            input.clone()
        } else {
            let exprs: Vec<ExprImpl> = (0..input_len)
                .map(|index| InputRef::new(index, input.schema()[index].data_type.clone()).into())
                .chain(extra_exprs)
                .collect();
            LogicalProject::create(input.clone(), exprs)
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
