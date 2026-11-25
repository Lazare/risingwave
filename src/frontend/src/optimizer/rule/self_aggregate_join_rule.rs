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

//! A table joined back on its key with an aggregation, grouped by that key, of rows derived from
//! itself: the aggregation evaluated per row of the table. Used for streaming plans only, behind
//! `streaming_unnest_lateral_functions`.

use std::collections::{BTreeSet, HashMap};

use risingwave_common::catalog::TableId;
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::lateral_table_function_rule::{PaddingTest, padding_test, per_row_agg_call, supported};
use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{map_input_refs, pure_scan, referred_columns};
use crate::expr::{
    Expr, ExprImpl, ExprType, FunctionCall, InputRef, TableFunction, TableFunctionType,
};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, LogicalProjectSet, LogicalScan, PlanAggCall,
    PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::{Condition, IndexSet};

/// Evaluates per row of a table an aggregation of rows derived from that table, grouped by its
/// key, that the table is joined back with on its key.
///
/// ```text
///   LogicalJoin [left outer or inner, T.key = A.key]
///   /                      \
/// [T: projections, filters]  LogicalAgg [group by T2.key]
///                              |
///                            [from T2 only: projections, filters, ProjectSet, joins with
///                             other relations on their left side]
///                              |
///                            Scan T2 (the same table)
/// ```
///
/// The table is read once: the rows that the right side derives from a row of `T` are computed on
/// that row, keeping every row of `T` (a function that returns no row gives one padded row, see
/// `LateralTableFunctionRule`; a filter and an inner join become a flag on the rows instead of
/// dropping them: an inner join becomes a left outer join, matched when a column that its
/// condition compares for equality is not NULL). The aggregation is then grouped by the key of
/// `T`, each call filtered on the flag, and counts the flagged rows: a row of `T` with no flagged
/// row had no group on the right side, so its right columns are NULL (left outer join) or the row
/// is dropped (inner join).
///
/// Sound because the key of `T` is a unique key of the table (proven from its definition, never
/// assumed): the row of the left side and the group that the condition matches it with come from
/// the same row of `T`, and every column of the group key is computed from that row only.
pub struct SelfAggregateJoinRule {}

impl Rule<Logical> for SelfAggregateJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        match join.join_type() {
            JoinType::LeftOuter | JoinType::Inner => {
                if let Some(result) = rewrite(join) {
                    return Some(result);
                }
                if join.join_type() == JoinType::Inner {
                    return rewrite(&swap(join, JoinType::Inner));
                }
                None
            }
            JoinType::RightOuter => rewrite(&swap(join, JoinType::LeftOuter)),
            _ => None,
        }
    }
}

impl SelfAggregateJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(SelfAggregateJoinRule {})
    }
}

/// The join with its two sides exchanged, of the given type, with the same output.
fn swap(join: &LogicalJoin, join_type: JoinType) -> LogicalJoin {
    let left_len = join.left().schema().len();
    let right_len = join.right().schema().len();
    let moved = |index: usize| {
        if index < left_len {
            index + right_len
        } else {
            index - left_len
        }
    };
    let on = Condition {
        conjunctions: join
            .on()
            .conjunctions
            .iter()
            .map(|e| {
                map_input_refs(e, |r| {
                    InputRef::new(moved(r.index()), r.return_type()).into()
                })
            })
            .collect(),
    };
    let output = join.output_indices().iter().map(|&index| moved(index));
    LogicalJoin::with_output_indices(join.right(), join.left(), join_type, on, output.collect())
}

fn rewrite(join: &LogicalJoin) -> Option<PlanRef> {
    let (left, right, on, join_type, output_indices) = join.clone().decompose();
    if !matches!(join_type, JoinType::LeftOuter | JoinType::Inner) {
        return None;
    }
    let left_len = left.schema().len();
    let pure = pure_scan(&left)?;
    let table = pure.scan.table().clone();
    let key: Vec<usize> = table.stream_key();
    if key.is_empty() {
        return None;
    }

    // The right side: an aggregation, maybe under a projection.
    let (agg_plan, right_exprs) = match right.as_logical_project() {
        Some(project) => (project.input(), Some(project.exprs().clone())),
        None => (right.clone(), None),
    };
    let agg = agg_plan.as_logical_agg()?;
    if !agg.grouping_sets().is_empty() || !agg.agg_calls().iter().all(per_row_agg_call) {
        return None;
    }
    let group_key: Vec<usize> = agg.group_key().indices().collect();
    let derived = agg.input();
    // Each column of the group key, as an expression of the row of the table it comes from.
    let group_traces: Vec<ExprImpl> = group_key
        .iter()
        .map(|&column| trace(&derived, column, table.id))
        .collect::<Option<_>>()?;
    // The key column of the table that each output column of the right side is, if it is one.
    let right_key_column = |output: usize| -> Option<usize> {
        let agg_output = match &right_exprs {
            Some(exprs) => exprs[output].as_input_ref()?.index(),
            None => output,
        };
        let trace = group_traces.get(agg_output)?;
        let column = trace.as_input_ref()?.index();
        key.contains(&column).then_some(column)
    };

    // The condition: only equalities of a key column of the table on both sides, covering the
    // whole key.
    let mut covered = BTreeSet::new();
    // The key columns compared with `=`, which a NULL never matches.
    let mut compared_with_equal = BTreeSet::new();
    for conjunct in &on.conjunctions {
        let ExprImpl::FunctionCall(call) = conjunct else {
            return None;
        };
        if !matches!(
            call.func_type(),
            ExprType::Equal | ExprType::IsNotDistinctFrom
        ) {
            return None;
        }
        let [ExprImpl::InputRef(a), ExprImpl::InputRef(b)] = call.inputs() else {
            return None;
        };
        let (l, r) = if a.index() < left_len && b.index() >= left_len {
            (a.index(), b.index() - left_len)
        } else if b.index() < left_len && a.index() >= left_len {
            (b.index(), a.index() - left_len)
        } else {
            return None;
        };
        let left_column = pure.exprs[l].as_input_ref()?.index();
        if !key.contains(&left_column) || right_key_column(r) != Some(left_column) {
            return None;
        }
        covered.insert(left_column);
        if call.func_type() == ExprType::Equal {
            compared_with_equal.insert(left_column);
        }
    }
    if covered.len() != key.len() {
        return None;
    }

    // The rows of the table, read once, with the columns that both sides need.
    let mut needed: BTreeSet<usize> = key.iter().copied().collect();
    needed.extend(referred_columns(&pure.exprs));
    needed.extend(referred_columns(&pure.predicate));
    needed.extend(referred_columns(&group_traces));
    let leaf = leaf_scan(&derived)?;
    if leaf.table().id != table.id || leaf.as_of().is_some() {
        return None;
    }
    needed.extend(leaf.output_col_idx().iter().copied());
    needed.extend(referred_columns(&leaf.predicate().conjunctions));
    let needed: Vec<usize> = needed.into_iter().collect();
    let position: HashMap<usize, usize> = needed.iter().enumerate().map(|(p, &c)| (c, p)).collect();
    let to_rows = |expr: &ExprImpl| {
        map_input_refs(expr, |r| {
            InputRef::new(position[&r.index()], r.return_type()).into()
        })
    };
    let scan: PlanRef = pure.scan.clone_with_output_indices(needed.clone()).into();
    let rows: PlanRef = LogicalFilter::create(
        scan,
        Condition {
            conjunctions: pure.predicate.iter().map(to_rows).collect(),
        },
    );
    let rows_len = needed.len();
    let ctx = Ctx {
        rows: rows.clone(),
        rows_len,
        position: &position,
        table_id: table.id,
    };

    // The derived rows of each row of the table: [table columns, flag, derived columns].
    let per_row = derive(&derived, &ctx)?;
    let offset = rows_len + 1;

    // The aggregation grouped by the key of the table.
    let flag = Condition::with_expr(InputRef::new(rows_len, DataType::Boolean).into());
    let mut calls = vec![];
    let mut guards = vec![];
    for call in agg.agg_calls() {
        let mut call = call.clone();
        for input in &mut call.inputs {
            input.shift_with_offset(offset as isize);
        }
        for order in &mut call.order_by {
            order.column_index += offset;
        }
        let conjunctions = call
            .filter
            .conjunctions
            .iter()
            .map(|e| {
                map_input_refs(e, |r| {
                    InputRef::new(r.index() + offset, r.return_type()).into()
                })
            })
            .collect();
        call.filter = Condition { conjunctions }.and(flag.clone());
        if call.agg_type == AggType::Builtin(PbAggKind::Sum) {
            guards.push((
                calls.len(),
                count_call(call.inputs.clone(), call.filter.clone()),
            ));
        }
        calls.push(call);
    }
    let mut group: Vec<usize> = key.iter().map(|c| position[c]).collect();
    group.sort_unstable();
    let key_len = group.len();
    let calls_len = calls.len();
    let mut call_values: Vec<ExprImpl> = calls
        .iter()
        .enumerate()
        .map(|(j, call)| InputRef::new(key_len + j, call.return_type.clone()).into())
        .collect();
    for (g, (j, guard)) in guards.into_iter().enumerate() {
        let count = InputRef::new(key_len + calls_len + g, DataType::Int64);
        let return_type = call_values[j].return_type();
        call_values[j] = case_when(
            greater_than_zero(count),
            call_values[j].clone(),
            return_type,
        );
        calls.push(guard);
    }
    let matched_rows = InputRef::new(key_len + calls.len(), DataType::Int64);
    calls.push(count_call(vec![], flag));
    // The value of each column of the rows of the table after the aggregation.
    let mut row_values: Vec<ExprImpl> = vec![];
    for p in 0..rows_len {
        let data_type = rows.schema()[p].data_type();
        let at = match group.iter().position(|&g| g == p) {
            Some(at) => at,
            None => {
                calls.push(PlanAggCall {
                    agg_type: PbAggKind::InternalLastSeenValue.into(),
                    return_type: data_type.clone(),
                    inputs: vec![InputRef::new(p, data_type.clone())],
                    distinct: false,
                    order_by: vec![],
                    filter: Condition::true_cond(),
                    direct_args: vec![],
                });
                key_len + calls.len() - 1
            }
        };
        row_values.push(InputRef::new(at, data_type).into());
    }
    let mut result: PlanRef = Agg::new(calls, IndexSet::from(group), per_row).into();
    // The row of the table had a group on the right side, and the condition matches it.
    let mut matched = vec![greater_than_zero(matched_rows)];
    for column in &compared_with_equal {
        let value = row_values[position[column]].clone();
        matched.push(
            FunctionCall::new_unchecked(ExprType::IsNotNull, vec![value], DataType::Boolean).into(),
        );
    }
    let matched = and_all(matched);
    if join_type == JoinType::Inner {
        result = LogicalFilter::create(result, Condition::with_expr(matched.clone()));
    }

    // The columns of the join, before its output indices.
    let of_table = |expr: &ExprImpl| -> ExprImpl {
        map_input_refs(expr, |r| row_values[position[&r.index()]].clone())
    };
    let mut columns: Vec<ExprImpl> = pure.exprs.iter().map(of_table).collect();
    let agg_values: Vec<ExprImpl> = group_traces
        .iter()
        .map(of_table)
        .chain(call_values)
        .collect();
    let right_values: Vec<ExprImpl> = match &right_exprs {
        Some(exprs) => exprs
            .iter()
            .map(|e| map_input_refs(e, |r| agg_values[r.index()].clone()))
            .collect(),
        None => agg_values,
    };
    for value in right_values {
        columns.push(if join_type == JoinType::LeftOuter {
            let return_type = value.return_type();
            case_when(matched.clone(), value, return_type)
        } else {
            value
        });
    }
    let exprs = output_indices.iter().map(|&i| columns[i].clone()).collect();
    Some(LogicalProject::new(result, exprs).into())
}

struct Ctx<'a> {
    /// The rows of the table read by the left side, with the columns in `position`.
    rows: PlanRef,
    rows_len: usize,
    /// The position in `rows` of each column of the table that is read.
    position: &'a HashMap<usize, usize>,
    table_id: TableId,
}

/// The scan of the table at the bottom of the rows derived from it, following the left input of
/// joins.
fn leaf_scan(plan: &PlanRef) -> Option<LogicalScan> {
    if let Some(scan) = plan.as_logical_scan() {
        return Some(scan.clone());
    }
    if let Some(join) = plan.as_logical_join() {
        return leaf_scan(&join.left());
    }
    let inputs = plan.inputs();
    match &inputs[..] {
        [input] => leaf_scan(input),
        _ => None,
    }
}

/// An output column of the derived rows, as an expression of the columns of the row of the table
/// it comes from, when it is computed from that row only.
fn trace(plan: &PlanRef, column: usize, table_id: TableId) -> Option<ExprImpl> {
    if let Some(scan) = plan.as_logical_scan() {
        if scan.table().id != table_id || scan.as_of().is_some() {
            return None;
        }
        let table_column = scan.output_col_idx()[column];
        let data_type = scan.table().columns()[table_column].data_type().clone();
        return Some(InputRef::new(table_column, data_type).into());
    }
    let substitute = |expr: &ExprImpl, input: &PlanRef| -> Option<ExprImpl> {
        if !expr.is_pure() || expr.has_table_function() {
            return None;
        }
        let mut failed = false;
        let traced = map_input_refs(expr, |r| match trace(input, r.index(), table_id) {
            Some(e) => e,
            None => {
                failed = true;
                r.into()
            }
        });
        (!failed).then_some(traced)
    };
    if let Some(project) = plan.as_logical_project() {
        return substitute(&project.exprs()[column], &project.input());
    }
    if let Some(filter) = plan.as_logical_filter() {
        return trace(&filter.input(), column, table_id);
    }
    if let Some(share) = plan.as_logical_share() {
        return trace(&share.input(), column, table_id);
    }
    if let Some(project_set) = plan.as_logical_project_set() {
        if column == 0 {
            return None;
        }
        return substitute(&project_set.select_list()[column - 1], &project_set.input());
    }
    if let Some(join) = plan.as_logical_join() {
        let index = join.output_indices()[column];
        if index >= join.left().schema().len() {
            return None;
        }
        return trace(&join.left(), index, table_id);
    }
    None
}

/// The rows derived from each row of the table, evaluated on the rows of `ctx`, with the layout
/// [columns of the table, flag, columns of `plan`]. Every row of the table gives at least one
/// row; the flag tells the rows that `plan` gives.
fn derive(plan: &PlanRef, ctx: &Ctx<'_>) -> Option<PlanRef> {
    let n = ctx.rows_len;
    let offset = n + 1;
    let shift = |expr: &ExprImpl| {
        map_input_refs(expr, |r| {
            InputRef::new(r.index() + offset, r.return_type()).into()
        })
    };
    let carried = |plan: &PlanRef| -> Vec<ExprImpl> {
        (0..n)
            .map(|i| InputRef::new(i, plan.schema()[i].data_type()).into())
            .collect()
    };
    let flag_of = |_plan: &PlanRef| -> ExprImpl { InputRef::new(n, DataType::Boolean).into() };
    let own = |plan: &PlanRef, len: usize| -> Vec<ExprImpl> {
        (0..len)
            .map(|k| InputRef::new(offset + k, plan.schema()[offset + k].data_type()).into())
            .collect()
    };

    if let Some(scan) = plan.as_logical_scan() {
        if scan.table().id != ctx.table_id || scan.as_of().is_some() {
            return None;
        }
        let to_rows = |expr: &ExprImpl| {
            map_input_refs(expr, |r| {
                InputRef::new(ctx.position[&r.index()], r.return_type()).into()
            })
        };
        let mut exprs = carried(&ctx.rows);
        exprs.push(and_all(
            scan.predicate().conjunctions.iter().map(to_rows).collect(),
        ));
        for &column in scan.output_col_idx() {
            let data_type = scan.table().columns()[column].data_type().clone();
            exprs.push(InputRef::new(ctx.position[&column], data_type).into());
        }
        return Some(LogicalProject::new(ctx.rows.clone(), exprs).into());
    }
    if let Some(share) = plan.as_logical_share() {
        return derive(&share.input(), ctx);
    }
    if let Some(project) = plan.as_logical_project() {
        let input = derive(&project.input(), ctx)?;
        let mut exprs = carried(&input);
        exprs.push(flag_of(&input));
        exprs.extend(project.exprs().iter().map(shift));
        return Some(LogicalProject::new(input, exprs).into());
    }
    if let Some(filter) = plan.as_logical_filter() {
        let input = derive(&filter.input(), ctx)?;
        let len = filter.input().schema().len();
        let mut exprs = carried(&input);
        let mut flag = vec![flag_of(&input)];
        flag.extend(filter.predicate().conjunctions.iter().map(shift));
        exprs.push(and_all(flag));
        exprs.extend(own(&input, len));
        return Some(LogicalProject::new(input, exprs).into());
    }
    if let Some(project_set) = plan.as_logical_project_set() {
        let input = derive(&project_set.input(), ctx)?;
        let items = project_set.select_list();
        // [rows columns, flag, items, twins, one row]
        let mut select_list: Vec<ExprImpl> = (0..offset)
            .map(|i| InputRef::new(i, input.schema()[i].data_type()).into())
            .collect();
        let mut parts = vec![];
        let mut twins = vec![];
        for (i, item) in items.iter().enumerate() {
            let item = shift(item);
            match &item {
                ExprImpl::TableFunction(function) => {
                    if !supported(function) {
                        return None;
                    }
                    match padding_test(function) {
                        PaddingTest::NeverNull => {
                            parts.push(is_not_null(1 + offset + i, function.return_type.clone()))
                        }
                        PaddingTest::Twin(twin) => twins.push(twin),
                        PaddingTest::Unknown => return None,
                    }
                }
                other => {
                    if other.has_table_function() || other.has_subquery() || !other.is_pure() {
                        return None;
                    }
                }
            }
            select_list.push(item);
        }
        for twin in twins {
            parts.push(is_not_null(1 + select_list.len(), twin.return_type()));
            select_list.push(twin);
        }
        let one_row = TableFunction::new(
            TableFunctionType::GenerateSeries,
            vec![ExprImpl::literal_int(1), ExprImpl::literal_int(1)],
        )
        .ok()?;
        select_list.push(one_row.into());
        let new: PlanRef = LogicalProjectSet::new(input.clone(), select_list).into();
        let mut exprs: Vec<ExprImpl> = (0..n)
            .map(|i| InputRef::new(1 + i, input.schema()[i].data_type()).into())
            .collect();
        let any_row = parts.into_iter().reduce(|a, b| {
            FunctionCall::new_unchecked(ExprType::Or, vec![a, b], DataType::Boolean).into()
        })?;
        exprs.push(and_all(vec![
            InputRef::new(1 + n, DataType::Boolean).into(),
            any_row,
        ]));
        exprs.push(InputRef::new(0, DataType::Int64).into());
        exprs.extend(items.iter().enumerate().map(|(i, item)| -> ExprImpl {
            InputRef::new(1 + offset + i, item.return_type()).into()
        }));
        return Some(LogicalProject::new(new, exprs).into());
    }
    if let Some(join) = plan.as_logical_join() {
        let (join_left, join_right, on, join_type, output_indices) = join.clone().decompose();
        let old_left_len = join_left.schema().len();
        let input = derive(&join_left, ctx)?;
        let conjunctions: Vec<ExprImpl> = on.conjunctions.iter().map(shift).collect();
        let mut flag = vec![flag_of(&input)];
        match join_type {
            JoinType::LeftOuter => {}
            JoinType::Inner => {
                // Matched when a column of the right side that the condition compares for
                // equality is not NULL.
                let matched = on.conjunctions.iter().find_map(|conjunct| {
                    let ExprImpl::FunctionCall(call) = conjunct else {
                        return None;
                    };
                    if call.func_type() != ExprType::Equal {
                        return None;
                    }
                    call.inputs().iter().find_map(|side| match side {
                        ExprImpl::InputRef(r) if r.index() >= old_left_len => {
                            Some(is_not_null(r.index() + offset, r.return_type()))
                        }
                        _ => None,
                    })
                })?;
                flag.push(matched);
            }
            _ => return None,
        }
        let new: PlanRef = LogicalJoin::new(
            input.clone(),
            join_right,
            JoinType::LeftOuter,
            Condition { conjunctions },
        )
        .into();
        let mut exprs = carried(&new);
        exprs.push(and_all(flag));
        for &index in &output_indices {
            exprs.push(
                InputRef::new(index + offset, new.schema()[index + offset].data_type()).into(),
            );
        }
        return Some(LogicalProject::new(new, exprs).into());
    }
    None
}

fn and_all(exprs: Vec<ExprImpl>) -> ExprImpl {
    exprs
        .into_iter()
        .reduce(|a, b| {
            FunctionCall::new_unchecked(ExprType::And, vec![a, b], DataType::Boolean).into()
        })
        .unwrap_or_else(|| ExprImpl::literal_bool(true))
}

fn is_not_null(index: usize, data_type: DataType) -> ExprImpl {
    FunctionCall::new_unchecked(
        ExprType::IsNotNull,
        vec![InputRef::new(index, data_type).into()],
        DataType::Boolean,
    )
    .into()
}

fn greater_than_zero(count: InputRef) -> ExprImpl {
    FunctionCall::new_unchecked(
        ExprType::GreaterThan,
        vec![count.into(), ExprImpl::literal_bigint(0)],
        DataType::Boolean,
    )
    .into()
}

fn case_when(condition: ExprImpl, value: ExprImpl, return_type: DataType) -> ExprImpl {
    FunctionCall::new_unchecked(ExprType::Case, vec![condition, value], return_type).into()
}

fn count_call(inputs: Vec<InputRef>, filter: Condition) -> PlanAggCall {
    PlanAggCall {
        agg_type: PbAggKind::Count.into(),
        return_type: DataType::Int64,
        inputs,
        distinct: false,
        order_by: vec![],
        filter,
        direct_args: vec![],
    }
}
