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
//! itself: the aggregation evaluated per row of the table. Used on demand, behind
//! `streaming_unnest_lateral_functions` for streaming plans and `batch_unnest_lateral_functions` for
//! batch plans.

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
///
/// The variant for batch plans (`create_for_batch`) computes `sum` as it is: a batch `sum` over no
/// row is NULL, where a streaming `sum` returns 0 once its non-NULL values are retracted.
pub struct SelfAggregateJoinRule {
    for_batch: bool,
}

impl Rule<Logical> for SelfAggregateJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let guard_sum = !self.for_batch;
        match join.join_type() {
            JoinType::LeftOuter | JoinType::Inner => {
                if let Some(result) = rewrite(join, guard_sum) {
                    return Some(result);
                }
                if join.join_type() == JoinType::Inner {
                    return rewrite(&swap(join, JoinType::Inner), guard_sum);
                }
                None
            }
            JoinType::RightOuter => rewrite(&swap(join, JoinType::LeftOuter), guard_sum),
            _ => None,
        }
    }
}

impl SelfAggregateJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(SelfAggregateJoinRule { for_batch: false })
    }

    /// The variant for batch plans, see the rule.
    pub fn create_for_batch() -> BoxedRule {
        Box::new(SelfAggregateJoinRule { for_batch: true })
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

fn rewrite(join: &LogicalJoin, guard_sum: bool) -> Option<PlanRef> {
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
    // key columns of a row of the same table that the left side carries, see `attach`.
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

    // The columns of the table that the derived rows need.
    let mut needed: BTreeSet<usize> = key.iter().copied().collect();
    needed.extend(referred_columns(&group_traces));
    needed.extend(leaf.output_col_idx().iter().copied());
    needed.extend(referred_columns(&leaf.predicate().conjunctions));

    let per_row = PerRow {
        table_id: table.id,
        key: &key,
        needed: &needed,
        derived: &derived,
        agg_calls: agg.agg_calls(),
        group_traces: &group_traces,
        right_exprs: right_exprs.as_ref(),
        outer: join_type == JoinType::LeftOuter,
        compared_with_equal: &compared_with_equal,
        guard_sum,
    };
    let (new_left, extras) = attach(&left, &compared, &per_row)?;
    // The extra columns: the columns of the right side, then whether the row is matched.
    let right_len = right.schema().len();
    if extras.len() != right_len + 1 {
        return None;
    }
    let mut result = new_left;
    if join_type == JoinType::Inner {
        let matched = InputRef::new(extras[right_len], DataType::Boolean);
        result = LogicalFilter::create(result, Condition::with_expr(matched.into()));
    }
    let exprs = output_indices
        .iter()
        .map(|&i| {
            let at = if i < left_len {
                i
            } else {
                extras[i - left_len]
            };
            InputRef::new(at, result.schema()[at].data_type()).into()
        })
        .collect();
    Some(LogicalProject::new(result, exprs).into())
}

/// The column of the table that each output column of the input of a share is, when the input is
/// the scan of the table under projections that pick columns and filters: one row per row of the
/// table that it reads.
fn shared_table_columns(plan: &PlanRef, table_id: TableId) -> Option<Vec<Option<usize>>> {
    if let Some(scan) = plan.as_logical_scan() {
        if scan.table().id != table_id || scan.as_of().is_some() {
            return None;
        }
        return Some(scan.output_col_idx().iter().map(|&c| Some(c)).collect());
    }
    if let Some(project) = plan.as_logical_project() {
        let input = shared_table_columns(&project.input(), table_id)?;
        return Some(
            project
                .exprs()
                .iter()
                .map(|e| e.as_input_ref().and_then(|r| input[r.index()]))
                .collect(),
        );
    }
    if let Some(filter) = plan.as_logical_filter() {
        return shared_table_columns(&filter.input(), table_id);
    }
    None
}

/// What the aggregation of the right side needs to be computed on the rows of the table.
struct PerRow<'a> {
    table_id: TableId,
    key: &'a [usize],
    needed: &'a BTreeSet<usize>,
    derived: &'a PlanRef,
    agg_calls: &'a [PlanAggCall],
    group_traces: &'a [ExprImpl],
    right_exprs: Option<&'a Vec<ExprImpl>>,
    outer: bool,
    compared_with_equal: &'a BTreeSet<usize>,
    /// A `sum` is NULL when it saw no non-NULL value, see `SelfAggregateJoinRule`.
    guard_sum: bool,
}

impl PerRow<'_> {
    /// The rows of the table (a scan, or a share of a scan) with the columns of the right side
    /// and the matched flag appended: the derived rows of each row are aggregated, grouped by the
    /// key of the table.
    fn build(
        &self,
        rows: PlanRef,
        position: &HashMap<usize, usize>,
        original_len: usize,
    ) -> Option<PlanRef> {
        let rows_len = rows.schema().len();
        let ctx = Ctx {
            rows: rows.clone(),
            rows_len,
            position,
            table_id: self.table_id,
        };
        let per_row = derive(self.derived, &ctx)?;
        let offset = rows_len + 1;
        let flag = Condition::with_expr(InputRef::new(rows_len, DataType::Boolean).into());
        let mut calls = vec![];
        let mut guards = vec![];
        for call in self.agg_calls {
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
            if self.guard_sum && call.agg_type == AggType::Builtin(PbAggKind::Sum) {
                guards.push((
                    calls.len(),
                    count_call(call.inputs.clone(), call.filter.clone()),
                ));
            }
            calls.push(call);
        }
        let mut group: Vec<usize> = self.key.iter().map(|c| position[c]).collect();
        group.sort_unstable();
        group.dedup();
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
        let agg: PlanRef = Agg::new(calls, IndexSet::from(group), per_row).into();
        // The row of the table had a group on the right side, and the condition matches it.
        let mut matched = vec![greater_than_zero(matched_rows)];
        for column in self.compared_with_equal {
            let value = row_values[position[column]].clone();
            matched.push(
                FunctionCall::new_unchecked(ExprType::IsNotNull, vec![value], DataType::Boolean)
                    .into(),
            );
        }
        let matched = and_all(matched);
        let of_table = |expr: &ExprImpl| -> ExprImpl {
            map_input_refs(expr, |r| row_values[position[&r.index()]].clone())
        };
        let agg_values: Vec<ExprImpl> = self
            .group_traces
            .iter()
            .map(of_table)
            .chain(call_values)
            .collect();
        let right_values: Vec<ExprImpl> = match self.right_exprs {
            Some(exprs) => exprs
                .iter()
                .map(|e| map_input_refs(e, |r| agg_values[r.index()].clone()))
                .collect(),
            None => agg_values,
        };
        // [the columns of the rows as they were, the right side, matched]
        let mut exprs: Vec<ExprImpl> = row_values[..original_len].to_vec();
        for value in right_values {
            exprs.push(if self.outer {
                let return_type = value.return_type();
                case_when(matched.clone(), value, return_type)
            } else {
                value
            });
        }
        exprs.push(matched);
        Some(LogicalProject::new(agg, exprs).into())
    }
}

/// Appends to the output of `plan` the columns of the right side computed on the row of the
/// table whose key columns are the given output columns of `plan` (`(output column, key column
/// of the table)`), and returns their positions.
///
/// The row is found down a path of projections that pass the key columns as they are, filters,
/// and joins that keep every row of the side of the path (inner joins, the outer side of an outer
/// join), to a scan of the table or a share of such a scan. On that path every row of `plan`
/// carries one real row of the table, whose key columns are the given output columns: the
/// aggregation is computed there, grouped by the key of the table, and its columns are passed up.
/// A scan reads the columns that the derived rows need; a shared scan must already read them, as
/// the share is not changed.
fn attach(
    plan: &PlanRef,
    columns: &[(usize, usize)],
    per_row: &PerRow<'_>,
) -> Option<(PlanRef, Vec<usize>)> {
    let leaf_scan_of = |scan: &LogicalScan| -> bool {
        scan.table().id == per_row.table_id
            && scan.as_of().is_none()
            && columns.iter().all(|&(o, c)| scan.output_col_idx()[o] == c)
    };
    let appended = |new: PlanRef, original_len: usize| -> (PlanRef, Vec<usize>) {
        let extras = (original_len..new.schema().len()).collect();
        (new, extras)
    };
    if let Some(scan) = plan.as_logical_scan() {
        if !leaf_scan_of(scan) {
            return None;
        }
        let output = scan.output_col_idx();
        let mut new_output = output.clone();
        for &c in per_row.needed {
            if !new_output.contains(&c) {
                new_output.push(c);
            }
        }
        let position: HashMap<usize, usize> = new_output
            .iter()
            .enumerate()
            .map(|(p, &c)| (c, p))
            .rev()
            .collect();
        let rows: PlanRef = scan.clone_with_output_indices(new_output).into();
        let new = per_row.build(rows, &position, output.len())?;
        return Some(appended(new, output.len()));
    }
    if let Some(share) = plan.as_logical_share() {
        // The share is kept as it is: the table columns of its output must hold what is needed.
        let columns_of_share = shared_table_columns(&share.input(), per_row.table_id)?;
        if !columns.iter().all(|&(o, c)| columns_of_share[o] == Some(c)) {
            return None;
        }
        let mut position = HashMap::new();
        for &c in per_row.needed {
            position.insert(c, columns_of_share.iter().position(|&o| o == Some(c))?);
        }
        let len = columns_of_share.len();
        let new = per_row.build(plan.clone(), &position, len)?;
        return Some(appended(new, len));
    }
    if let Some(project) = plan.as_logical_project() {
        let inner_columns = columns
            .iter()
            .map(|&(o, c)| Some((project.exprs()[o].as_input_ref()?.index(), c)))
            .collect::<Option<Vec<_>>>()?;
        let (input, extras) = attach(&project.input(), &inner_columns, per_row)?;
        let mut exprs = project.exprs().clone();
        let original_len = exprs.len();
        for at in extras {
            exprs.push(InputRef::new(at, input.schema()[at].data_type()).into());
        }
        return Some(appended(
            LogicalProject::new(input, exprs).into(),
            original_len,
        ));
    }
    if let Some(filter) = plan.as_logical_filter() {
        let (input, extras) = attach(&filter.input(), columns, per_row)?;
        return Some((
            LogicalFilter::new(input, filter.predicate().clone()).into(),
            extras,
        ));
    }
    if let Some(join) = plan.as_logical_join() {
        let (join_left, join_right, on, join_type, output_indices) = join.clone().decompose();
        let join_left_len = join_left.schema().len();
        let inner: Vec<(usize, usize)> = columns
            .iter()
            .map(|&(o, c)| (output_indices[o], c))
            .collect();
        let original_len = output_indices.len();
        if inner.iter().all(|&(i, _)| i < join_left_len)
            && matches!(join_type, JoinType::Inner | JoinType::LeftOuter)
        {
            let (new_left, extras) = attach(&join_left, &inner, per_row)?;
            let added = new_left.schema().len() - join_left_len;
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
            let mut new_output: Vec<usize> = output_indices.iter().map(|&i| moved(i)).collect();
            new_output.extend(extras);
            let new: PlanRef =
                LogicalJoin::with_output_indices(new_left, join_right, join_type, on, new_output)
                    .into();
            return Some(appended(new, original_len));
        }
        if inner.iter().all(|&(i, _)| i >= join_left_len)
            && matches!(join_type, JoinType::Inner | JoinType::RightOuter)
        {
            let inner: Vec<(usize, usize)> =
                inner.iter().map(|&(i, c)| (i - join_left_len, c)).collect();
            let (new_right, extras) = attach(&join_right, &inner, per_row)?;
            let mut new_output = output_indices;
            new_output.extend(extras.into_iter().map(|p| join_left_len + p));
            let new: PlanRef =
                LogicalJoin::with_output_indices(join_left, new_right, join_type, on, new_output)
                    .into();
            return Some(appended(new, original_len));
        }
        return None;
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
