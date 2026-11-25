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

//! Per-row evaluation of the correlated subqueries that read no table, only columns of the outer
//! row through set-returning functions. Used for streaming plans only, behind
//! `streaming_unnest_lateral_functions`.
//!
//! The general unnesting evaluates such a subquery on the distinct values of the correlated
//! columns (the domain, read again from the outer side and aggregated), and joins the result back
//! with `IS NOT DISTINCT FROM`. Evaluated on each outer row instead, the subquery needs no domain,
//! no join and no second read of the outer side.
//!
//! RisingWave has no outer `ProjectSet` (one that keeps a row padded with NULLs when the
//! set-returning functions return no row). It is emulated exactly: the set-returning functions of a
//! `ProjectSet` are zipped, the shorter ones padded with NULLs, so one more function that returns
//! exactly one row (`generate_series(1, 1)`) makes the `ProjectSet` return one padded row when the
//! others return none, and changes nothing otherwise. A table function that fails on a row is
//! treated the same way: it returns no row, and the row is padded, as the outer join does.
//!
//! Whether a row is padded is known when every set-returning function never returns NULL (it is
//! then NULL only on the padded row), or has a twin that returns as many rows, never NULL, and
//! fails on the same rows (`generate_subscripts(a, 1)` for `unnest(a)` of a one-dimensional array,
//! `jsonb_array_elements(j)` for `jsonb_array_elements_text(j)`): the twin is zipped too.
//!
//! The rewritten plans evaluate the functions on each outer row rather than once per distinct
//! value of the correlated columns: only built-in deterministic functions are accepted.

use fixedbitset::FixedBitSet;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::expr::{
    CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall,
    InputRef, TableFunction, TableFunctionType,
};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::{
    LogicalOverWindow, LogicalProject, LogicalProjectSet, PlanAggCall, PlanTreeNodeUnary,
};
use crate::utils::{Condition, IndexSet};

/// Evaluates per outer row a lateral subquery over set-returning functions of the outer row.
///
/// ```text
///   LogicalApply [on true]                       LogicalProject
///   /            \                                     |
/// LHS        [nodes of the subquery]      =>   [the same nodes, carrying the LHS columns]
///                  |                                   |
///          LogicalProjectSet (srf(corr))      LogicalProjectSet (srf(lhs), generate_series(1, 1))
///                  |                                   |
///            LogicalValues (one row)                  LHS
/// ```
///
/// Three forms of the subquery:
/// - `LEFT JOIN LATERAL (SELECT f(srf(..)) ...) ON true`, a single projection over the functions:
///   the outer `ProjectSet` emulated as above; a projected expression that would not be NULL on
///   the padded row is wrapped in `CASE WHEN <row is not padded> THEN .. END`. The inner form is
///   `ApplyTableFunctionToProjectSetRule`.
/// - an aggregation without group key over the functions (a scalar subquery, `ARRAY(..)`,
///   `EXISTS`), in a left outer or an inner apply: the aggregation is grouped by the stream key of
///   the outer side (a group holds the rows of one outer row) over the outer `ProjectSet`, carries
///   the other columns of the outer side with `internal_last_seen_value`, and filters every
///   aggregate call on the rows that are not padded, so that an outer row whose functions return
///   no row gets the value of the aggregation over an empty input. `sum` is computed as
///   `CASE WHEN count(x) > 0 THEN sum(x) END`: when an update of the outer row retracts all the
///   non-NULL values of its group while new rows come in, a streaming `sum` returns 0, not NULL.
/// - window functions over the functions in an inner apply: the windows are partitioned by the
///   stream key of the outer side.
pub struct LateralTableFunctionRule {}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Shape {
    Plain,
    Agg,
    Window,
}

impl Rule<Logical> for LateralTableFunctionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, _, max_one_row) = apply.clone().decompose();
        if !on.always_true() {
            return None;
        }

        // The nodes of the subquery above its `ProjectSet`, from the top.
        let mut nodes: Vec<PlanRef> = vec![];
        let mut cur = right.clone();
        let project_set = loop {
            if let Some(project_set) = cur.as_logical_project_set() {
                break project_set.clone();
            }
            let next = if let Some(project) = cur.as_logical_project() {
                project.input()
            } else if let Some(agg) = cur.as_logical_agg() {
                agg.input()
            } else {
                cur.as_logical_over_window()?.input()
            };
            nodes.push(cur);
            cur = next;
        };
        let aggs = nodes
            .iter()
            .filter(|n| n.as_logical_agg().is_some())
            .count();
        let windows = nodes
            .iter()
            .filter(|n| n.as_logical_over_window().is_some())
            .count();
        let shape = match (aggs, windows) {
            (0, 0) => Shape::Plain,
            (1, 0) => Shape::Agg,
            (0, 1) => Shape::Window,
            _ => return None,
        };
        match shape {
            Shape::Plain => {
                if join_type != JoinType::LeftOuter || max_one_row || nodes.len() != 1 {
                    return None;
                }
            }
            Shape::Agg => {
                if !matches!(join_type, JoinType::LeftOuter | JoinType::Inner) {
                    return None;
                }
                // Only projections below the aggregation.
                let agg_pos = nodes.iter().position(|n| n.as_logical_agg().is_some())?;
                if nodes[agg_pos + 1..]
                    .iter()
                    .any(|n| n.as_logical_project().is_none())
                {
                    return None;
                }
                let agg = nodes[agg_pos].as_logical_agg()?;
                if !agg.group_key().is_empty()
                    || !agg.grouping_sets().is_empty()
                    || !agg.agg_calls().iter().all(per_row_agg_call)
                {
                    return None;
                }
            }
            Shape::Window => {
                if join_type != JoinType::Inner || max_one_row {
                    return None;
                }
            }
        }
        // A group or a partition must hold the rows of one outer row: it is keyed by the stream
        // key of the outer side. The other columns of the outer side, which may not be a key
        // (`_rw_timestamp`), are carried by the aggregation.
        let stream_key: Vec<usize> = match shape {
            Shape::Plain => vec![],
            Shape::Agg | Shape::Window => {
                let mut key = left.stream_key()?.to_vec();
                key.sort_unstable();
                key.dedup();
                if key.is_empty() {
                    return None;
                }
                key
            }
        };

        let outer = shape != Shape::Window;
        // Whether the padded row must be told from the rows of the functions.
        let need_flag = match shape {
            Shape::Plain => {
                let project = nodes[0].as_logical_project()?;
                !project
                    .exprs()
                    .iter()
                    .all(|e| null_when_padded(e, project_set.select_list()))
            }
            Shape::Agg => true,
            Shape::Window => false,
        };
        let left_len = left.schema().len();
        let mut rewriter = CorrelatedRewriter::new(correlated_id, left_len);
        let base = build_base(&left, &project_set, &mut rewriter, outer, need_flag)?;
        if need_flag && !base.has_flag {
            return None;
        }

        let result = match shape {
            Shape::Plain => {
                let project = nodes[0].as_logical_project()?;
                let mut exprs = identity(&left, left_len);
                rewriter.offset = left_len + 1;
                for expr in project.exprs() {
                    let rewritten = rewriter.rewrite_expr(expr.clone());
                    if null_when_padded(expr, project_set.select_list()) {
                        exprs.push(rewritten);
                    } else {
                        if !base.has_flag {
                            return None;
                        }
                        let return_type = rewritten.return_type();
                        exprs.push(
                            FunctionCall::new_unchecked(
                                ExprType::Case,
                                vec![base.flag(), rewritten],
                                return_type,
                            )
                            .into(),
                        );
                    }
                }
                LogicalProject::new(base.plan, exprs).into()
            }
            Shape::Agg | Shape::Window => {
                if shape == Shape::Agg && !base.has_flag {
                    return None;
                }
                // Layout of `cur`: the columns of the outer side, then (below an aggregation) the
                // flag of the rows that are not padded, then the columns of the subquery's node.
                let mut cur = base.plan;
                let mut offset = left_len + 1;
                for node in nodes.iter().rev() {
                    if let Some(project) = node.as_logical_project() {
                        let mut exprs = identity(&cur, offset);
                        rewriter.offset = offset;
                        exprs.extend(
                            project
                                .exprs()
                                .iter()
                                .map(|e| rewriter.rewrite_expr(e.clone())),
                        );
                        cur = LogicalProject::new(cur, exprs).into();
                    } else if let Some(over_window) = node.as_logical_over_window() {
                        let mut functions = over_window.window_functions().to_vec();
                        for function in &mut functions {
                            for arg in &mut function.args {
                                arg.shift_with_offset(offset as isize);
                            }
                            for order in &mut function.order_by {
                                order.column_index += offset;
                            }
                            for partition in &mut function.partition_by {
                                partition.shift_with_offset(offset as isize);
                            }
                            function.partition_by = stream_key
                                .iter()
                                .map(|&i| InputRef::new(i, left.schema()[i].data_type()))
                                .chain(function.partition_by.drain(..))
                                .collect();
                        }
                        cur = LogicalOverWindow::new(functions, cur).into();
                    } else {
                        let agg = node.as_logical_agg()?;
                        cur = per_row_agg(
                            agg.agg_calls(),
                            cur,
                            offset,
                            &stream_key,
                            left_len,
                            &mut rewriter,
                        );
                        offset = left_len;
                    }
                }
                let mut exprs = identity(&cur, left_len);
                exprs.extend((0..right.schema().len()).map(|k| {
                    InputRef::new(offset + k, cur.schema()[offset + k].data_type()).into()
                }));
                LogicalProject::new(cur, exprs).into()
            }
        };
        if !rewriter.touched || rewriter.unexpected {
            return None;
        }
        Some(result)
    }
}

impl LateralTableFunctionRule {
    pub fn create() -> BoxedRule {
        Box::new(LateralTableFunctionRule {})
    }
}

/// The aggregation of a subquery evaluated per row: grouped by the stream key of the outer side,
/// which carries its other columns with `internal_last_seen_value`; each call is filtered on the
/// rows that are not padded. `input` has the layout [outer columns, flag, columns of the input of
/// the aggregation]. The result has the layout [outer columns, calls].
fn per_row_agg(
    calls: &[PlanAggCall],
    input: PlanRef,
    offset: usize,
    stream_key: &[usize],
    left_len: usize,
    rewriter: &mut CorrelatedRewriter,
) -> PlanRef {
    let flag = Condition::with_expr(InputRef::new(left_len, DataType::Boolean).into());
    let mut new_calls = vec![];
    let mut guards = vec![];
    for call in calls {
        let mut call = call.clone();
        for input in &mut call.inputs {
            input.shift_with_offset(offset as isize);
        }
        for order in &mut call.order_by {
            order.column_index += offset;
        }
        rewriter.offset = offset;
        let conjunctions = call
            .filter
            .conjunctions
            .drain(..)
            .map(|e| rewriter.rewrite_expr(e))
            .collect();
        call.filter = Condition { conjunctions }.and(flag.clone());
        if call.agg_type == AggType::Builtin(PbAggKind::Sum) {
            guards.push((
                new_calls.len(),
                PlanAggCall {
                    agg_type: PbAggKind::Count.into(),
                    return_type: DataType::Int64,
                    inputs: call.inputs.clone(),
                    distinct: false,
                    order_by: vec![],
                    filter: call.filter.clone(),
                    direct_args: vec![],
                },
            ));
        }
        new_calls.push(call);
    }
    let key_len = stream_key.len();
    let calls_len = new_calls.len();
    let mut outputs: Vec<ExprImpl> = new_calls
        .iter()
        .enumerate()
        .map(|(j, call)| InputRef::new(key_len + j, call.return_type.clone()).into())
        .collect();
    for (g, (j, guard)) in guards.into_iter().enumerate() {
        let count = InputRef::new(key_len + calls_len + g, DataType::Int64);
        let is_positive = FunctionCall::new_unchecked(
            ExprType::GreaterThan,
            vec![count.into(), ExprImpl::literal_bigint(0)],
            DataType::Boolean,
        );
        let return_type = outputs[j].return_type();
        outputs[j] = FunctionCall::new_unchecked(
            ExprType::Case,
            vec![is_positive.into(), outputs[j].clone()],
            return_type,
        )
        .into();
        new_calls.push(guard);
    }
    // The columns of the outer side: the group key, or carried.
    let mut left_columns: Vec<ExprImpl> = vec![];
    for i in 0..left_len {
        let data_type = input.schema()[i].data_type();
        let position = match stream_key.iter().position(|&k| k == i) {
            Some(position) => position,
            None => {
                new_calls.push(PlanAggCall {
                    agg_type: PbAggKind::InternalLastSeenValue.into(),
                    return_type: data_type.clone(),
                    inputs: vec![InputRef::new(i, data_type.clone())],
                    distinct: false,
                    order_by: vec![],
                    filter: Condition::true_cond(),
                    direct_args: vec![],
                });
                key_len + new_calls.len() - 1
            }
        };
        left_columns.push(InputRef::new(position, data_type).into());
    }
    let group_key = IndexSet::from(stream_key.to_vec());
    let agg: PlanRef = Agg::new(new_calls, group_key, input).into();
    left_columns.extend(outputs);
    LogicalProject::new(agg, left_columns).into()
}

/// The aggregate calls whose value over the rows of one outer row is computed exactly by a
/// streaming aggregation grouped by that row, with a filter.
fn per_row_agg_call(call: &PlanAggCall) -> bool {
    if call.distinct {
        return false;
    }
    match &call.agg_type {
        AggType::Builtin(PbAggKind::Count) => call.return_type == DataType::Int64,
        AggType::Builtin(PbAggKind::Sum) => call.inputs.len() == 1,
        AggType::Builtin(
            PbAggKind::Min
            | PbAggKind::Max
            | PbAggKind::ArrayAgg
            | PbAggKind::StringAgg
            | PbAggKind::JsonbAgg
            | PbAggKind::JsonbObjectAgg,
        ) => true,
        _ => false,
    }
}

/// The `ProjectSet` of the subquery evaluated on the outer side, with the layout [outer columns,
/// flag of the rows that are not padded, `projected_row_id`, select list of the subquery].
struct Base {
    plan: PlanRef,
    left_len: usize,
    has_flag: bool,
}

impl Base {
    fn flag(&self) -> ExprImpl {
        InputRef::new(self.left_len, DataType::Boolean).into()
    }
}

fn build_base(
    left: &PlanRef,
    project_set: &LogicalProjectSet,
    rewriter: &mut CorrelatedRewriter,
    outer: bool,
    need_flag: bool,
) -> Option<Base> {
    let values = project_set.input();
    if !values.as_logical_values()?.is_empty_scalar() {
        return None;
    }
    let left_len = left.schema().len();
    let items = project_set.select_list();
    let mut select_list = identity(left, left_len);
    let mut flag_parts: Vec<ExprImpl> = vec![];
    let mut twins = vec![];
    let mut has_flag = true;
    rewriter.offset = 0;
    for (i, item) in items.iter().enumerate() {
        let item = rewriter.rewrite_expr(item.clone());
        match &item {
            ExprImpl::TableFunction(table_function) => {
                if !supported(table_function) {
                    return None;
                }
                if need_flag {
                    match padding_test(table_function) {
                        PaddingTest::NeverNull => {
                            let column = 1 + left_len + i;
                            flag_parts.push(is_not_null(InputRef::new(
                                column,
                                table_function.return_type.clone(),
                            )));
                        }
                        PaddingTest::Twin(twin) => twins.push(twin),
                        PaddingTest::Unknown => has_flag = false,
                    }
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
        let column = 1 + select_list.len();
        flag_parts.push(is_not_null(InputRef::new(column, twin.return_type())));
        select_list.push(twin);
    }
    if outer {
        let one_row = TableFunction::new(
            TableFunctionType::GenerateSeries,
            vec![ExprImpl::literal_int(1), ExprImpl::literal_int(1)],
        )
        .ok()?;
        select_list.push(one_row.into());
    }
    let project_set: PlanRef = LogicalProjectSet::new(left.clone(), select_list).into();

    // On an inner `ProjectSet` no row is padded.
    let flag = if !outer {
        ExprImpl::literal_bool(true)
    } else if need_flag && has_flag {
        flag_parts
            .into_iter()
            .reduce(|a, b| {
                FunctionCall::new_unchecked(ExprType::Or, vec![a, b], DataType::Boolean).into()
            })
            .unwrap_or_else(|| ExprImpl::literal_bool(true))
    } else {
        // Never read: the forms that need the flag are refused.
        ExprImpl::literal_bool(true)
    };
    let mut exprs: Vec<ExprImpl> = (0..left_len)
        .map(|i| InputRef::new(1 + i, left.schema()[i].data_type()).into())
        .collect();
    exprs.push(flag);
    exprs.push(InputRef::new(0, DataType::Int64).into());
    exprs.extend(items.iter().enumerate().map(|(i, item)| -> ExprImpl {
        InputRef::new(1 + left_len + i, item.return_type()).into()
    }));
    Some(Base {
        plan: LogicalProject::new(project_set, exprs).into(),
        left_len,
        has_flag: !outer || (need_flag && has_flag),
    })
}

/// Built-in table functions, deterministic, that the rule evaluates per row.
fn supported(table_function: &TableFunction) -> bool {
    table_function.user_defined.is_none()
        && matches!(
            table_function.function_type,
            TableFunctionType::GenerateSeries
                | TableFunctionType::Range
                | TableFunctionType::Unnest
                | TableFunctionType::RegexpMatches
                | TableFunctionType::GenerateSubscripts
                | TableFunctionType::JsonbArrayElements
                | TableFunctionType::JsonbArrayElementsText
                | TableFunctionType::JsonbEach
                | TableFunctionType::JsonbEachText
                | TableFunctionType::JsonbObjectKeys
        )
        && table_function
            .args
            .iter()
            .all(|arg| arg.is_pure() && !arg.has_table_function() && !arg.has_subquery())
}

/// How to tell the padded row of an outer `ProjectSet` from the rows of a set-returning function.
enum PaddingTest {
    /// The function never returns NULL: it is NULL on the padded row only.
    NeverNull,
    /// A function that returns as many rows, never NULL, and fails on the same rows.
    Twin(ExprImpl),
    Unknown,
}

fn padding_test(function: &TableFunction) -> PaddingTest {
    let twin = match (function.function_type, function.args.as_slice()) {
        // `unnest` flattens a nested array, `generate_subscripts` does not.
        (TableFunctionType::Unnest, [array]) => match array.return_type() {
            DataType::List(list) if !matches!(list.elem(), DataType::List(_)) => {
                TableFunction::new(
                    TableFunctionType::GenerateSubscripts,
                    vec![array.clone(), ExprImpl::literal_int(1)],
                )
            }
            _ => return PaddingTest::Unknown,
        },
        // The text of a JSON `null` element is not NULL today, but it is in PostgreSQL: do not
        // rely on it in a plan that is kept.
        (TableFunctionType::JsonbArrayElementsText, [json]) => {
            TableFunction::new(TableFunctionType::JsonbArrayElements, vec![json.clone()])
        }
        (TableFunctionType::Unnest | TableFunctionType::JsonbArrayElementsText, _) => {
            return PaddingTest::Unknown;
        }
        _ => return PaddingTest::NeverNull,
    };
    match twin {
        Ok(twin) => PaddingTest::Twin(twin.into()),
        Err(_) => PaddingTest::Unknown,
    }
}

/// Whether an expression of the projection over the `ProjectSet` of the subquery is NULL on the
/// padded row, where every set-returning function gives NULL.
fn null_when_padded(expr: &ExprImpl, items: &[ExprImpl]) -> bool {
    match expr {
        ExprImpl::InputRef(input_ref) => {
            input_ref.index() >= 1
                && matches!(
                    items.get(input_ref.index() - 1),
                    Some(ExprImpl::TableFunction(_))
                )
        }
        ExprImpl::FunctionCall(call) => {
            matches!(
                call.func_type(),
                ExprType::Field
                    | ExprType::ArrayAccess
                    | ExprType::JsonbAccess
                    | ExprType::JsonbAccessStr
                    | ExprType::Cast
            ) && call
                .inputs()
                .first()
                .is_some_and(|first| null_when_padded(first, items))
                && call.inputs()[1..]
                    .iter()
                    .all(|input| matches!(input, ExprImpl::Literal(_)))
        }
        _ => false,
    }
}

fn is_not_null(input_ref: InputRef) -> ExprImpl {
    FunctionCall::new_unchecked(
        ExprType::IsNotNull,
        vec![input_ref.into()],
        DataType::Boolean,
    )
    .into()
}

fn identity_refs(plan: &PlanRef, len: usize) -> Vec<InputRef> {
    (0..len)
        .map(|i| InputRef::new(i, plan.schema()[i].data_type()))
        .collect()
}

fn identity(plan: &PlanRef, len: usize) -> Vec<ExprImpl> {
    identity_refs(plan, len)
        .into_iter()
        .map(ExprImpl::from)
        .collect()
}

/// Shifts the input references of the subquery by `offset`, and turns its correlated references
/// to the outer side into input references to the columns of the outer side, which come first.
struct CorrelatedRewriter {
    correlated_id: CorrelatedId,
    left_len: usize,
    offset: usize,
    /// A correlated reference to the outer side was found.
    touched: bool,
    /// A correlated reference to a column the outer side does not have.
    unexpected: bool,
}

impl CorrelatedRewriter {
    fn new(correlated_id: CorrelatedId, left_len: usize) -> Self {
        Self {
            correlated_id,
            left_len,
            offset: 0,
            touched: false,
            unexpected: false,
        }
    }
}

impl ExprRewriter for CorrelatedRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }

    fn rewrite_correlated_input_ref(&mut self, input_ref: CorrelatedInputRef) -> ExprImpl {
        if input_ref.correlated_id() != self.correlated_id {
            return input_ref.into();
        }
        if input_ref.index() >= self.left_len {
            self.unexpected = true;
            return input_ref.into();
        }
        self.touched = true;
        InputRef::new(input_ref.index(), input_ref.return_type()).into()
    }
}

/// Turns `unnest(ARRAY(SELECT srf(<outer columns>)))`, used only as a source of rows, into
/// `srf(<outer columns>)`: the rows of the unnested array are the rows of the function (a bag:
/// the order of the array does not matter), and an empty or NULL array gives no row, as a
/// function that returns no row. `COALESCE(<array>, '{}')` is looked through.
///
/// ```text
///  [projections, filters: do not read the array]     [the same]
///        |                                                 |
///  LogicalProjectSet [.., unnest($c)]                LogicalProjectSet [.., srf(lhs)]
///        |                                                 |
///  LogicalProject [.., $c = coalesce(arr, '{}')]  => LogicalProject [.., $c = NULL, lhs]
///        |                                                 |
///  LogicalApply [left outer, on true]                     LHS
///   /          \
/// LHS     ARRAY(SELECT srf(corr))
/// ```
///
/// The subquery of `ARRAY(..)` must be the function alone: no condition, no `LIMIT`, no other
/// function, no expression over its rows. The `ProjectSet` above must have no other
/// set-returning function, and its row number must not be read (`WITH ORDINALITY`).
pub struct UnnestArrayOfTableFunctionRule {}

impl Rule<Logical> for UnnestArrayOfTableFunctionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        // The projections and filters above the `ProjectSet`, from the top.
        let mut chain: Vec<PlanRef> = vec![];
        let mut cur = plan;
        let project_set = loop {
            if let Some(project_set) = cur.as_logical_project_set() {
                break project_set.clone();
            }
            let next = if let Some(project) = cur.as_logical_project() {
                project.input()
            } else {
                cur.as_logical_filter()?.input()
            };
            chain.push(cur);
            cur = next;
        };
        if chain.is_empty() {
            return None;
        }
        // The columns of the `ProjectSet` that the chain reads, all the columns of its top being
        // read.
        let mut reads = FixedBitSet::with_capacity(chain[0].schema().len());
        reads.insert_range(..);
        for node in &chain {
            let input_len = match node.as_logical_project() {
                Some(project) => project.input().schema().len(),
                None => node.as_logical_filter()?.input().schema().len(),
            };
            let mut below = FixedBitSet::with_capacity(input_len);
            if let Some(project) = node.as_logical_project() {
                for k in reads.ones() {
                    below.union_with(&project.exprs()[k].collect_input_refs(input_len));
                }
            } else {
                let filter = node.as_logical_filter()?;
                below.union_with(&reads);
                for conjunction in &filter.predicate().conjunctions {
                    below.union_with(&conjunction.collect_input_refs(input_len));
                }
            }
            reads = below;
        }
        let items = project_set.select_list();
        let input = project_set.input();
        let input_len = input.schema().len();

        // The only set-returning function: `unnest` of a column of the input.
        let mut unnest_pos = None;
        for (i, item) in items.iter().enumerate() {
            if let ExprImpl::TableFunction(_) = item {
                if unnest_pos.is_some() {
                    return None;
                }
                unnest_pos = Some(i);
            } else if item.has_table_function() {
                return None;
            }
        }
        let unnest_pos = unnest_pos?;
        let ExprImpl::TableFunction(unnest) = &items[unnest_pos] else {
            return None;
        };
        if unnest.function_type != TableFunctionType::Unnest {
            return None;
        }
        let [unnest_arg] = unnest.args.as_slice() else {
            return None;
        };
        let ExprImpl::InputRef(array_ref) = strip_coalesce_empty(unnest_arg) else {
            return None;
        };
        let array_col = array_ref.index();

        // The array is read by the `unnest` only: elsewhere it is at most passed through, to a
        // column that nothing above reads. Neither is the row number.
        if reads.contains(0) {
            return None;
        }
        for (i, item) in items.iter().enumerate() {
            if i == unnest_pos || !item.collect_input_refs(input_len).contains(array_col) {
                continue;
            }
            let passed_through = matches!(item, ExprImpl::InputRef(r) if r.index() == array_col);
            if !passed_through || reads.contains(1 + i) {
                return None;
            }
        }

        // The input: a projection over the apply of `ARRAY(..)`, or the apply itself.
        let (q_exprs, apply_plan) = if let Some(project) = input.as_logical_project() {
            (project.exprs().clone(), project.input())
        } else {
            (identity(&input, input_len), input.clone())
        };
        let apply = apply_plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, _, _) = apply.clone().decompose();
        if !on.always_true() || !matches!(join_type, JoinType::LeftOuter | JoinType::Inner) {
            return None;
        }
        let left_len = left.schema().len();
        if right.schema().len() != 1 {
            return None;
        }
        match strip_coalesce_empty(&q_exprs[array_col]) {
            ExprImpl::InputRef(r) if r.index() == left_len => {}
            _ => return None,
        }
        for (k, expr) in q_exprs.iter().enumerate() {
            if k != array_col && expr.collect_input_refs(left_len + 1).contains(left_len) {
                return None;
            }
        }
        let function = array_of_table_function(&right)?;
        if function.return_type != unnest.return_type {
            return None;
        }

        // The projection keeps its columns, the array becomes NULL (nothing reads it), and the
        // columns of the outer side are appended for the arguments of the function.
        let q_len = q_exprs.len();
        let mut new_q = q_exprs;
        new_q[array_col] = ExprImpl::literal_null(new_q[array_col].return_type());
        new_q.extend(identity(&left, left_len));
        let new_q: PlanRef = LogicalProject::new(left, new_q).into();

        let mut rewriter = CorrelatedRewriter::new(correlated_id, left_len);
        let function = rewriter.rewrite_expr(function.into());
        if rewriter.unexpected {
            return None;
        }
        // The correlated references became references to the columns of the outer side, which
        // are appended after the columns of the projection.
        let function = InputRefShift { offset: q_len }.rewrite_expr(function);
        let mut new_items = items.clone();
        new_items[unnest_pos] = function;
        let mut result: PlanRef = LogicalProjectSet::new(new_q, new_items).into();
        for node in chain.iter().rev() {
            result = if let Some(project) = node.as_logical_project() {
                project.clone_with_input(result).into()
            } else {
                node.as_logical_filter()?.clone_with_input(result).into()
            };
        }
        Some(result)
    }
}

impl UnnestArrayOfTableFunctionRule {
    pub fn create() -> BoxedRule {
        Box::new(UnnestArrayOfTableFunctionRule {})
    }
}

struct InputRefShift {
    offset: usize,
}

impl ExprRewriter for InputRefShift {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}

/// The set-returning function of `ARRAY(SELECT srf(..))`: a projection `COALESCE($0, '{}')` (or
/// `$0`) over an `array_agg` without group key, filter, `DISTINCT` or other call, of the only
/// column of a `ProjectSet` over one row of values, through projections that only pick columns.
fn array_of_table_function(right: &PlanRef) -> Option<TableFunction> {
    let mut cur = right.clone();
    if let Some(project) = cur.as_logical_project() {
        let [expr] = project.exprs().as_slice() else {
            return None;
        };
        match strip_coalesce_empty(expr) {
            ExprImpl::InputRef(r) if r.index() == 0 => {}
            _ => return None,
        }
        cur = project.input();
    }
    let agg = cur.as_logical_agg()?;
    if !agg.group_key().is_empty() || !agg.grouping_sets().is_empty() {
        return None;
    }
    let [call] = agg.agg_calls().as_slice() else {
        return None;
    };
    if call.agg_type != AggType::Builtin(PbAggKind::ArrayAgg)
        || call.distinct
        || !call.filter.always_true()
        || call.inputs.len() != 1
    {
        return None;
    }
    let mut index = call.inputs[0].index();
    let mut cur = agg.input();
    loop {
        if let Some(project) = cur.as_logical_project() {
            let ExprImpl::InputRef(r) = &project.exprs()[index] else {
                return None;
            };
            index = r.index();
            cur = project.input();
            continue;
        }
        let project_set = cur.as_logical_project_set()?;
        if !project_set.input().as_logical_values()?.is_empty_scalar() {
            return None;
        }
        let [item] = project_set.select_list().as_slice() else {
            return None;
        };
        if index != 1 {
            return None;
        }
        let ExprImpl::TableFunction(function) = item else {
            return None;
        };
        if !supported(function) {
            return None;
        }
        return Some((**function).clone());
    }
}

/// `COALESCE(e, <empty array>)` is `e` for `unnest`: both give no row.
fn strip_coalesce_empty(expr: &ExprImpl) -> &ExprImpl {
    if let ExprImpl::FunctionCall(call) = expr
        && call.func_type() == ExprType::Coalesce
        && let [first, second] = call.inputs()
        && is_empty_array(second)
    {
        return strip_coalesce_empty(first);
    }
    expr
}

fn is_empty_array(expr: &ExprImpl) -> bool {
    match expr {
        ExprImpl::Literal(literal) => {
            matches!(literal.get_data(), Some(ScalarImpl::List(list)) if list.is_empty())
        }
        ExprImpl::FunctionCall(call) => match call.func_type() {
            ExprType::Array => call.inputs().is_empty(),
            ExprType::Cast => call.inputs().first().is_some_and(is_empty_array),
            _ => false,
        },
        _ => false,
    }
}
