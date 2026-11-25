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
use super::repeated_lookup_merge_rule::{map_input_refs, referred_columns};
use crate::catalog::table_catalog::TableType;
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

    // The right side: an aggregation, maybe under a projection, of rows derived from a table.
    let (agg_plan, right_exprs) = match right.as_logical_project() {
        Some(project) => (project.input(), Some(project.exprs().clone())),
        None => (right.clone(), None),
    };
    let agg = agg_plan.as_logical_agg()?;
    if !agg.grouping_sets().is_empty() || !agg.agg_calls().iter().all(per_row_agg_call) {
        return None;
    }
    let derived = agg.input();
    let leaf = leaf_scan(&derived)?;
    if leaf.as_of().is_some() || matches!(leaf.table().table_type(), TableType::Internal) {
        return None;
    }
    let table = leaf.table().clone();
    let key: Vec<usize> = table.stream_key();
    if key.is_empty() {
        return None;
    }
    let group_key: Vec<usize> = agg.group_key().indices().collect();
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

    // The condition: only equalities between a column of the left side and a key column of the
    // table in the group key, covering the whole key. The columns of the left side must be the
    // key columns of a row of the same table that the left side carries, see `widen`.
    let mut compared = vec![];
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
        let column = right_key_column(r)?;
        compared.push((l, column));
        if call.func_type() == ExprType::Equal {
            compared_with_equal.insert(column);
        }
    }
    let covered: BTreeSet<usize> = compared.iter().map(|&(_, c)| c).collect();
    if covered.len() != key.len() {
        return None;
    }
    // A group per row of the left side.
    let mut group: Vec<usize> = left.stream_key()?.to_vec();
    group.sort_unstable();
    group.dedup();
    if group.is_empty() {
        return None;
    }

    // The columns of the table that the derived rows need, carried by the left side.
    let mut needed: BTreeSet<usize> = key.iter().copied().collect();
    needed.extend(referred_columns(&group_traces));
    needed.extend(leaf.output_col_idx().iter().copied());
    needed.extend(referred_columns(&leaf.predicate().conjunctions));
    let widened = widen(&left, &compared, &needed, table.id)?;
    let rows = widened.plan;
    let position = widened.positions;
    let rows_len = rows.schema().len();
    let ctx = Ctx {
        rows: rows.clone(),
        rows_len,
        position: &position,
        table_id: table.id,
    };

    // The derived rows of each row of the left side: [its columns, flag, derived columns].
    let per_row = derive(&derived, &ctx)?;
    let offset = rows_len + 1;

    // The aggregation grouped by the stream key of the left side.
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
    // The value of each column of the rows after the aggregation.
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
    let mut columns: Vec<ExprImpl> = row_values[..left_len].to_vec();
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

/// The left side with the columns of a row of the table appended to its output.
struct Widened {
    plan: PlanRef,
    /// The position in the output of `plan` of each appended column of the table.
    positions: HashMap<usize, usize>,
}

/// Appends to the output of `plan` the `needed` columns of the row of the table whose key columns
/// are the given output columns of `plan` (`(output column, key column of the table)`).
///
/// The row is found down a path of projections that pass the key columns as they are, filters,
/// and joins that keep every row of the side of the path (inner joins, the outer side of an outer
/// join), to a scan of the table or a share of such a scan. On that path every row of `plan`
/// carries one real row of the table, whose key columns are the given output columns. A scan
/// reads the needed columns; a shared scan must already read them, as the share is not changed.
fn widen(
    plan: &PlanRef,
    columns: &[(usize, usize)],
    needed: &BTreeSet<usize>,
    table_id: TableId,
) -> Option<Widened> {
    if let Some(scan) = plan.as_logical_scan() {
        if scan.table().id != table_id || scan.as_of().is_some() {
            return None;
        }
        let output = scan.output_col_idx();
        if !columns.iter().all(|&(o, c)| output[o] == c) {
            return None;
        }
        let mut new_output = output.clone();
        for &c in needed {
            if !new_output.contains(&c) {
                new_output.push(c);
            }
        }
        let positions = needed
            .iter()
            .map(|&c| (c, new_output.iter().position(|&o| o == c).unwrap()))
            .collect();
        return Some(Widened {
            plan: scan.clone_with_output_indices(new_output).into(),
            positions,
        });
    }
    if let Some(share) = plan.as_logical_share() {
        // The share is kept as it is: its scan must already read the needed columns.
        let input = share.input();
        let scan = input.as_logical_scan()?;
        if scan.table().id != table_id || scan.as_of().is_some() {
            return None;
        }
        let output = scan.output_col_idx();
        if !columns.iter().all(|&(o, c)| output[o] == c) {
            return None;
        }
        let positions = needed
            .iter()
            .map(|&c| Some((c, output.iter().position(|&o| o == c)?)))
            .collect::<Option<_>>()?;
        return Some(Widened {
            plan: plan.clone(),
            positions,
        });
    }
    if let Some(project) = plan.as_logical_project() {
        let inner_columns = columns
            .iter()
            .map(|&(o, c)| Some((project.exprs()[o].as_input_ref()?.index(), c)))
            .collect::<Option<Vec<_>>>()?;
        let input = widen(&project.input(), &inner_columns, needed, table_id)?;
        let mut exprs = project.exprs().clone();
        let mut positions = HashMap::new();
        for &c in needed {
            let at = input.positions[&c];
            positions.insert(c, exprs.len());
            exprs.push(InputRef::new(at, input.plan.schema()[at].data_type()).into());
        }
        return Some(Widened {
            plan: LogicalProject::new(input.plan, exprs).into(),
            positions,
        });
    }
    if let Some(filter) = plan.as_logical_filter() {
        let input = widen(&filter.input(), columns, needed, table_id)?;
        return Some(Widened {
            plan: LogicalFilter::new(input.plan, filter.predicate().clone()).into(),
            positions: input.positions,
        });
    }
    if let Some(join) = plan.as_logical_join() {
        let (join_left, join_right, on, join_type, output_indices) = join.clone().decompose();
        let join_left_len = join_left.schema().len();
        let inner: Vec<(usize, usize)> = columns
            .iter()
            .map(|&(o, c)| (output_indices[o], c))
            .collect();
        let on_left = inner.iter().all(|&(i, _)| i < join_left_len)
            && matches!(join_type, JoinType::Inner | JoinType::LeftOuter);
        let on_right = inner.iter().all(|&(i, _)| i >= join_left_len)
            && matches!(join_type, JoinType::Inner | JoinType::RightOuter);
        let (new_left, new_right, moved_on, mut new_output, carried): (
            PlanRef,
            PlanRef,
            Condition,
            Vec<usize>,
            HashMap<usize, usize>,
        ) = if on_left {
            let widened = widen(&join_left, &inner, needed, table_id)?;
            let added = widened.plan.schema().len() - join_left_len;
            let moved = |i: usize| if i < join_left_len { i } else { i + added };
            let on = Condition {
                conjunctions: on
                    .conjunctions
                    .iter()
                    .map(|e| {
                        map_input_refs(e, |r| {
                            InputRef::new(moved(r.index()), r.return_type()).into()
                        })
                    })
                    .collect(),
            };
            let output = output_indices.iter().map(|&i| moved(i)).collect();
            (widened.plan, join_right, on, output, widened.positions)
        } else if on_right {
            let inner: Vec<(usize, usize)> =
                inner.iter().map(|&(i, c)| (i - join_left_len, c)).collect();
            let widened = widen(&join_right, &inner, needed, table_id)?;
            let carried = widened
                .positions
                .iter()
                .map(|(&c, &p)| (c, join_left_len + p))
                .collect();
            (join_left, widened.plan, on, output_indices.clone(), carried)
        } else {
            return None;
        };
        let mut positions = HashMap::new();
        for &c in needed {
            let at = carried[&c];
            let position = match new_output.iter().position(|&i| i == at) {
                Some(position) => position,
                None => {
                    new_output.push(at);
                    new_output.len() - 1
                }
            };
            positions.insert(c, position);
        }
        let new: PlanRef =
            LogicalJoin::with_output_indices(new_left, new_right, join_type, moved_on, new_output)
                .into();
        return Some(Widened {
            plan: new,
            positions,
        });
    }
    None
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
            input,
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
