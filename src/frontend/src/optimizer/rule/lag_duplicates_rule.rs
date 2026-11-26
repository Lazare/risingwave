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

//! Duplicates found by comparing a column with its value on the previous row of a window ordered
//! by that column (`batch_rewrite_lag_duplicates`, batch only).

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncKind};

use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{and_all, map_input_refs, referred_columns};
use crate::expr::{
    Expr, ExprImpl, ExprType, FunctionCall, InputRef, TableFunction, TableFunctionType,
};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalProject, LogicalProjectSet, PlanTreeNodeUnary,
};
use crate::utils::IndexSet;

/// Rewrites a search of duplicates written with `lag`, which sorts each window partition on one
/// node, into an aggregation grouped by the compared column:
///
/// ```text
/// Project(x, w)                                 Project(x, x)
/// └─Filter(x = w)                               └─ProjectSet(x, generate_series(2, c))
///   └─OverWindow(w = lag(x) OVER                  └─Agg(group p, x: c = count(*))
///         (PARTITION BY p ORDER BY x))              └─Filter(x IS NOT NULL)
///     └─input                                         └─input
/// ```
///
/// `lag(x)` is planned as `first_value(x) OVER (.. ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)`
/// (`lead(x)` with `1 FOLLOWING`); both are matched, as `last_value` with the same one-row frame.
///
/// Why the result is the same: within a partition, the rows are ordered by `x` first (after
/// columns of the partition key, constant in the partition), so the rows with the same non-NULL
/// value `v` of `x` are contiguous, whatever the direction, the place of `NULL`s and the order of
/// the ties. Among the `k` rows of `v`, the first one (the last one with `1 FOLLOWING`) compares
/// `v` with another value or with the `NULL` of a missing row, and each of the `k - 1` others with
/// `v`: `x = w` holds on exactly `k - 1` rows. `x = w` never holds when `x` is `NULL`. Above the
/// filter only `x`, `w` (equal to `x` there) and the partition columns are read, so the rows that
/// pass are `k - 1` copies of `(p, v, v)`, which the rewritten plan produces: one group per
/// partition and value, expanded into `count - 1` rows (`generate_series(2, c)` is empty when
/// `c = 1`). The partition is now by `x`, so the work is distributed, and the aggregation can
/// spill.
///
/// When the consumer is an aggregation grouped by the partition columns and `x` (or `w`) with only
/// `count(*)` (or the count of `x` or `w`, never `NULL` there), the expansion is not built: each
/// group of the consumer is one partition and value, with `count - 1` rows:
///
/// ```text
/// Agg(group x: count(*))                        Project(x, c - 1)
/// └─Project(x)                                  └─Filter(c > 1)
///   └─Filter(x = w)                               └─Agg(group x: c = count(*))
///     └─OverWindow(w = lag(x) ..)                   └─Filter(x IS NOT NULL)
///                                                     └─input
/// ```
///
/// Partition columns that are constants (`PARTITION BY true`) are not grouped by.
///
/// Not rewritten:
/// - another column of the row read above (an input column, the partition column of a constant,
///   another window function): the row dropped among the duplicates would matter;
/// - an offset other than 1 (`lag(x, 2)`), `IGNORE NULLS`, another frame (`ROWS 1 PRECEDING` is
///   `BETWEEN 1 PRECEDING AND CURRENT ROW`: the first row compares `x` with itself), another
///   function, an argument other than the first ordering column;
/// - `x IS NOT DISTINCT FROM w` or another comparison: the `NULL`s of `x` and of the missing row
///   would count;
/// - another conjunct in the filter, unless it reads only the partition columns, `x` and `w` and
///   is pure: it is then a condition on the group, applied to the input rows;
/// - `x` or a partition column of a type where distinct values can be equal (`decimal`, floats,
///   `interval`, composite types): the rows read above would hold the value of another row of the
///   group; only the types listed in [`supported_type`].
pub struct LagDuplicatesRule;

impl LagDuplicatesRule {
    pub fn create() -> BoxedRule {
        Box::new(LagDuplicatesRule)
    }
}

impl Rule<Logical> for LagDuplicatesRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if plan.as_logical_agg().is_some() {
            return rewrite_counting_agg(&plan);
        }
        let project = plan.as_logical_project()?;
        let duplicates = Duplicates::find(&project.input())?;
        for column in referred_columns(project.exprs()) {
            duplicates.group_position(column)?;
        }
        let (agg, count) = duplicates.counting_agg();
        // One row per partition column and value, `count - 1` times.
        let key_len = duplicates.group.len();
        let mut select_list: Vec<ExprImpl> = (0..key_len)
            .map(|i| InputRef::new(i, agg.schema()[i].data_type()).into())
            .collect();
        let series = TableFunction::new(
            TableFunctionType::GenerateSeries,
            vec![ExprImpl::literal_bigint(2), count.into()],
        )
        .ok()?;
        select_list.push(series.into());
        let expanded: PlanRef = LogicalProjectSet::new(agg, select_list).into();
        // The first column of a `ProjectSet` is its row id.
        let exprs = project
            .exprs()
            .iter()
            .map(|expr| {
                map_input_refs(expr, |input_ref| {
                    let position = duplicates.group_position(input_ref.index()).unwrap();
                    InputRef::new(1 + position, input_ref.return_type()).into()
                })
            })
            .collect();
        Some(LogicalProject::new(expanded, exprs).into())
    }
}

/// The aggregation over the duplicates: `count(*)` grouped by the partition columns and `x`, with
/// the count of each group decreased by 1 and the groups of one row removed.
fn rewrite_counting_agg(plan: &PlanRef) -> Option<PlanRef> {
    let agg = plan.as_logical_agg()?;
    if !agg.grouping_sets().is_empty() {
        return None;
    }
    // The columns of the filter read by the aggregation, through the projections between them.
    let mut columns: Vec<usize> = agg.group_key().indices().collect();
    let mut calls_columns = vec![];
    for call in agg.agg_calls() {
        let plain_count = matches!(call.agg_type, AggType::Builtin(PbAggKind::Count))
            && call.return_type == DataType::Int64
            && !call.distinct
            && call.order_by.is_empty()
            && call.filter.always_true()
            && call.direct_args.is_empty()
            && call.inputs.len() <= 1;
        if !plain_count {
            return None;
        }
        calls_columns.extend(call.inputs.iter().map(|input| input.index()));
    }
    let group_len = columns.len();
    columns.extend(calls_columns);
    let mut input = agg.input();
    while let Some(project) = input.as_logical_project() {
        for column in &mut columns {
            *column = project.exprs()[*column].as_input_ref()?.index();
        }
        input = project.input();
    }
    let duplicates = Duplicates::find(&input)?;
    let positions: Vec<usize> = columns[..group_len]
        .iter()
        .map(|&column| duplicates.group_position(column))
        .collect::<Option<_>>()?;
    // Each group of the aggregation must be one partition and value: its key covers all of them.
    let covered: IndexSet = positions.iter().copied().collect();
    if covered.len() != duplicates.group.len() {
        return None;
    }
    // A counted column must be `x` or `w`, never `NULL` in the rows that pass the filter (a
    // partition column may be `NULL`).
    if !columns[group_len..].iter().all(|&column| {
        column < duplicates.output_len() && duplicates.input_column(column) == duplicates.column
    }) {
        return None;
    }
    let (counts, count) = duplicates.counting_agg();
    let having = FunctionCall::new(
        ExprType::GreaterThan,
        vec![count.clone().into(), ExprImpl::literal_bigint(1)],
    )
    .ok()?;
    let groups = LogicalFilter::create_with_expr(counts, having.into());
    let mut exprs: Vec<ExprImpl> = positions
        .iter()
        .map(|&position| InputRef::new(position, groups.schema()[position].data_type()).into())
        .collect();
    let count_minus_one: ExprImpl = FunctionCall::new(
        ExprType::Subtract,
        vec![count.into(), ExprImpl::literal_bigint(1)],
    )
    .ok()?
    .into();
    exprs.extend(agg.agg_calls().iter().map(|_| count_minus_one.clone()));
    Some(LogicalProject::new(groups, exprs).into())
}

/// The types whose equal values are identical: the value of `x` kept by the aggregation for a
/// group is then the value of each of its rows. Not `decimal` (`1.0 = 1.00` but the aggregation
/// keeps the normalized value), floats (`0.0 = -0.0`), `interval` (`'1 day' = '24 hours'`) or
/// composite types.
fn supported_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Serial
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
            | DataType::Varchar
            | DataType::Bytea
    )
}

/// `Filter(x = w AND ..)` over `OverWindow(w = lag(x) OVER (PARTITION BY p ORDER BY x))`.
struct Duplicates {
    /// The input of the window.
    input: PlanRef,
    /// The width of the input of the window.
    input_len: usize,
    /// The number of window functions.
    window_len: usize,
    /// The compared column `x`, in the input of the window.
    column: usize,
    /// The window column `w`, in the output of the filter.
    window_column: usize,
    /// The group key: the partition columns that are not constants and `x`, in the input of the
    /// window, in increasing order (the order of the columns of the aggregation).
    group: Vec<usize>,
    /// The other conjuncts of the filter, over the input of the window.
    conditions: Vec<ExprImpl>,
}

impl Duplicates {
    fn find(plan: &PlanRef) -> Option<Self> {
        let filter = plan.as_logical_filter()?;
        let over_window = filter.input();
        let over_window = over_window.as_logical_over_window()?;
        let input = over_window.input();
        let input_len = input.schema().len();

        // The comparison `x = w`.
        let conjunctions = &filter.predicate().conjunctions;
        let (comparison, column, window_column) =
            conjunctions.iter().enumerate().find_map(|(i, expr)| {
                let (first, second) = expr.as_eq_cond()?;
                (first.index() < input_len && second.index() >= input_len).then_some((
                    i,
                    first.index(),
                    second.index(),
                ))
            })?;
        let function = &over_window.window_functions()[window_column - input_len];
        let one_row_frames = [
            Frame::rows(FrameBound::Preceding(1), FrameBound::Preceding(1)),
            Frame::rows(FrameBound::Following(1), FrameBound::Following(1)),
        ];
        let value_of_row = matches!(
            function.kind,
            WindowFuncKind::Aggregate(AggType::Builtin(
                PbAggKind::FirstValue | PbAggKind::LastValue
            ))
        ) && !function.ignore_nulls
            && one_row_frames.contains(&function.frame)
            && function.args.len() == 1
            && function.args[0].index() == column;
        if !value_of_row {
            return None;
        }

        // Ordered by `x` first, after partition columns.
        let partition: Vec<usize> = function.partition_by.iter().map(|p| p.index()).collect();
        let first_order = function
            .order_by
            .iter()
            .find(|order| !partition.contains(&order.column_index))?;
        if first_order.column_index != column || partition.contains(&column) {
            return None;
        }

        // `PARTITION BY true` puts all the rows in one partition: no need to group by it.
        let constants = input.as_logical_project();
        let mut group: Vec<usize> = partition
            .into_iter()
            .filter(|&p| !constants.is_some_and(|project| project.exprs()[p].is_const()))
            .collect();
        group.push(column);
        group.sort_unstable();
        // The values of the group key are those of the rows (partition columns included, they may
        // be read above).
        if !group
            .iter()
            .all(|&g| supported_type(&input.schema()[g].data_type()))
        {
            return None;
        }

        let mut found = Self {
            input,
            input_len,
            window_len: over_window.window_functions().len(),
            column,
            window_column,
            group,
            conditions: vec![],
        };

        // The other conjuncts are conditions on the partition columns and `x`.
        for (i, expr) in conjunctions.iter().enumerate() {
            if i == comparison {
                continue;
            }
            if !expr.is_pure() || expr.has_subquery() || expr.has_table_function() {
                return None;
            }
            for referred in referred_columns([expr]) {
                found.group_position(referred)?;
            }
            let condition = map_input_refs(expr, |input_ref| {
                InputRef::new(
                    found.input_column(input_ref.index()),
                    input_ref.return_type(),
                )
                .into()
            });
            found.conditions.push(condition);
        }
        Some(found)
    }

    /// The width of the output of the filter.
    fn output_len(&self) -> usize {
        self.input_len + self.window_len
    }

    /// The column of the input of the window equal to a column of the output of the filter.
    fn input_column(&self, column: usize) -> usize {
        if column == self.window_column {
            self.column
        } else {
            column
        }
    }

    /// The position in the group key of a column of the output of the filter, if it is `x`, `w`
    /// or a partition column that is not a constant.
    fn group_position(&self, column: usize) -> Option<usize> {
        if column >= self.input_len && column != self.window_column {
            return None;
        }
        let column = self.input_column(column);
        self.group.iter().position(|&g| g == column)
    }

    /// `Agg(group key: count(*))` over the rows of the input where `x` is not `NULL` and the other
    /// conditions hold, and the count column.
    fn counting_agg(&self) -> (PlanRef, InputRef) {
        let x = InputRef::new(self.column, self.input.schema()[self.column].data_type());
        let mut conditions = vec![
            FunctionCall::new_unchecked(ExprType::IsNotNull, vec![x.into()], DataType::Boolean)
                .into(),
        ];
        conditions.extend(self.conditions.iter().cloned());
        let rows = LogicalFilter::create_with_expr(self.input.clone(), and_all(conditions));
        let agg: PlanRef = Agg::new(
            vec![PlanAggCall::count_star()],
            self.group.iter().copied().collect(),
            rows,
        )
        .into();
        (agg, InputRef::new(self.group.len(), DataType::Int64))
    }
}
