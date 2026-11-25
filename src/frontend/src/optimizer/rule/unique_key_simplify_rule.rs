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

//! Simplifications that rely on the unique keys of a plan node: a set of columns on which no two
//! rows of the node agree, `NULL` being a value like any other.
//!
//! The rules replace a stateful operator with a projection, or remove it. The result then relies
//! on the keys being exact, where the removed operator would have tolerated or reported a
//! duplicate. They are only used by the streaming optimizer, on demand.

use std::collections::{BTreeSet, HashMap};

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::column_facts_simplify_rule::{column_facts, scan_predicate_constant_columns};
use super::never_true_predicate_rule::is_simple_type;
use super::prelude::{PlanRef, *};
use crate::catalog::table_catalog::TableType;
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalJoin, LogicalProject, LogicalScan, PlanNodeId, PlanTreeNodeBinary,
    PlanTreeNodeUnary,
};

/// A set of columns on which no two rows agree. The empty set means at most one row.
type Key = BTreeSet<usize>;

/// Returns the known unique keys of a plan node. A node that is not listed here has no known
/// key, which is always safe.
///
/// The stream key of a logical node is *not* used: before the plan is rewritten for streaming,
/// the stream key of a union is not a key yet, and every node above the union inherits it.
///
/// - `Scan`: the key of the table, if its columns are read or fixed by the predicate (see
///   [`scan_keys`]).
/// - `Values` with at most one row, `Limit` of at most one row: the empty key.
/// - `Filter`, `Share`, `Limit`: the keys of the input, whose rows they only select.
/// - `TopN`: the keys of the input, and the group key if it keeps one row per group.
/// - `Dedup`: the keys of the input, and the columns it deduplicates on.
/// - `Agg` without grouping sets: the group key.
/// - `Project`: the keys of the input whose columns are all projected as they are.
/// - `Join`: see [`join_keys`].
fn unique_keys(plan: &PlanRef, memo: &mut HashMap<PlanNodeId, Vec<Key>>) -> Vec<Key> {
    if let Some(keys) = memo.get(&plan.id()) {
        return keys.clone();
    }
    let mut keys = derive_unique_keys(plan, memo);
    // A column that holds the same value in every row does not tell two rows apart: a table read
    // with `id = 7` on its key holds at most one row.
    let constants = column_facts(plan).constant_columns();
    for key in &mut keys {
        key.retain(|column| !constants.contains(column));
    }
    memo.insert(plan.id(), keys.clone());
    keys
}

/// The key of a scanned table, on the output columns of the scan.
///
/// A key column that the predicate of the scan fixes to a constant is not needed to tell two rows
/// apart, and the scan usually does not output it any more: `d.id = 7` pushed into the scan of
/// `d` leaves at most one row, whatever the columns that are read.
fn scan_keys(scan: &LogicalScan) -> Vec<Key> {
    let table = scan.table();
    if matches!(table.table_type(), TableType::Internal) {
        return vec![];
    }
    let constants = scan_predicate_constant_columns(scan);
    table
        .stream_key()
        .into_iter()
        .filter(|column| !constants.contains(column))
        .map(|column| {
            scan.output_col_idx()
                .iter()
                .position(|&output| output == column)
        })
        .collect::<Option<Key>>()
        .into_iter()
        .collect()
}

fn derive_unique_keys(plan: &PlanRef, memo: &mut HashMap<PlanNodeId, Vec<Key>>) -> Vec<Key> {
    if let Some(scan) = plan.as_logical_scan() {
        return scan_keys(scan);
    }
    if let Some(values) = plan.as_logical_values() {
        return if values.rows().len() <= 1 {
            vec![Key::new()]
        } else {
            vec![]
        };
    }
    if let Some(agg) = plan.as_logical_agg() {
        return if agg.grouping_sets().is_empty() {
            // The output columns are the group key columns, then the aggregate calls.
            vec![(0..agg.group_key().len()).collect()]
        } else {
            vec![]
        };
    }
    if let Some(project) = plan.as_logical_project() {
        // The first output column that is exactly the input column, for each input column.
        let mut output_of = HashMap::new();
        for (output, expr) in project.exprs().iter().enumerate() {
            if let Some(input_ref) = expr.as_input_ref() {
                output_of.entry(input_ref.index()).or_insert(output);
            }
        }
        return map_keys(unique_keys(&project.input(), memo), &output_of);
    }
    if let Some(join) = plan.as_logical_join() {
        return join_keys(join, memo);
    }
    let input_keys = |memo: &mut HashMap<PlanNodeId, Vec<Key>>| -> Vec<Key> {
        let inputs = plan.inputs();
        match &inputs[..] {
            [input] => unique_keys(input, memo),
            _ => vec![],
        }
    };
    if plan.as_logical_filter().is_some() || plan.as_logical_share().is_some() {
        return input_keys(memo);
    }
    if let Some(limit) = plan.as_logical_limit() {
        let mut keys = input_keys(memo);
        if limit.limit() <= 1 {
            keys.push(Key::new());
        }
        return keys;
    }
    if let Some(top_n) = plan.as_logical_top_n() {
        let mut keys = input_keys(memo);
        if top_n.limit_attr().max_one_row() {
            keys.push(top_n.group_key().iter().copied().collect());
        }
        return keys;
    }
    if let Some(dedup) = plan.as_logical_dedup() {
        let mut keys = input_keys(memo);
        keys.push(dedup.dedup_cols().iter().copied().collect());
        return keys;
    }
    vec![]
}

/// Keeps the keys whose columns all have an image in the mapping, with their columns mapped.
fn map_keys(keys: Vec<Key>, mapping: &HashMap<usize, usize>) -> Vec<Key> {
    keys.into_iter()
        .filter_map(|key| {
            key.iter()
                .map(|column| mapping.get(column).copied())
                .collect::<Option<Key>>()
        })
        .collect()
}

fn shift_key(key: &Key, offset: usize) -> Key {
    key.iter().map(|column| column + offset).collect()
}

/// Returns whether a key of the plan node is made of the given columns only: no two rows of the
/// node agree on all the given columns.
fn has_key_within(
    plan: &PlanRef,
    columns: &BTreeSet<usize>,
    memo: &mut HashMap<PlanNodeId, Vec<Key>>,
) -> bool {
    unique_keys(plan, memo)
        .iter()
        .any(|key| key.is_subset(columns))
}

/// The columns of each side of a join that its condition binds: for a given row of the other
/// side, every matching row holds the same value in these columns. Indexed by the columns of
/// each side.
///
/// A column is bound by a conjunct `l = r` or `l IS NOT DISTINCT FROM r` between a column of each
/// side, or `c = <constant>`. Only for the simple types of [`is_simple_type`], for which two
/// equal values are indistinguishable, so that "equal" and "same key" mean the same.
fn bound_columns(join: &LogicalJoin) -> (BTreeSet<usize>, BTreeSet<usize>) {
    let left_len = join.left().schema().len();
    let mut bound_left = BTreeSet::new();
    let mut bound_right = BTreeSet::new();
    let mut bind = |input_ref: &InputRef| {
        if !is_simple_type(&input_ref.return_type()) {
            return;
        }
        if input_ref.index() < left_len {
            bound_left.insert(input_ref.index());
        } else {
            bound_right.insert(input_ref.index() - left_len);
        }
    };
    for expr in &join.on().conjunctions {
        if let Some((first, second)) = expr
            .as_eq_cond()
            .or_else(|| expr.as_is_not_distinct_from_cond())
        {
            // Ordered by index: the first one is on the left side if the two sides differ.
            if first.index() < left_len
                && second.index() >= left_len
                && is_simple_type(&first.return_type())
                && is_simple_type(&second.return_type())
            {
                bind(&first);
                bind(&second);
            }
        } else if let Some((input_ref, _)) = expr.as_eq_const() {
            bind(&input_ref);
        }
    }
    (bound_left, bound_right)
}

/// Returns whether the left side, then the right side, of a join is unique on the columns that
/// the condition of the join binds: a row of the other side matches at most one row.
pub(super) fn is_unique_on_compared_columns(join: &LogicalJoin) -> (bool, bool) {
    let (bound_left, bound_right) = bound_columns(join);
    let mut memo = HashMap::new();
    (
        has_key_within(&join.left(), &bound_left, &mut memo),
        has_key_within(&join.right(), &bound_right, &mut memo),
    )
}

/// Returns the keys of a join.
///
/// When the condition binds a key of the right side (see [`bound_columns`]), a row of the left
/// side matches at most one row of the right side, so the keys of the left side are keys of an
/// inner or left outer join. Symmetrically for the left side and an inner or right outer join.
/// For an inner join, the union of a key of each side is a key too. A semi or anti join only
/// selects rows of the side it keeps. Full outer and as-of joins have no known key.
fn join_keys(join: &LogicalJoin, memo: &mut HashMap<PlanNodeId, Vec<Key>>) -> Vec<Key> {
    let (left, right) = (join.left(), join.right());
    let left_len = left.schema().len();
    let left_keys = unique_keys(&left, memo);
    let right_keys = unique_keys(&right, memo);
    let (bound_left, bound_right) = bound_columns(join);
    let left_is_unique = left_keys.iter().any(|key| key.is_subset(&bound_left));
    let right_is_unique = right_keys.iter().any(|key| key.is_subset(&bound_right));
    let shifted_right_keys = || -> Vec<Key> {
        right_keys
            .iter()
            .map(|key| shift_key(key, left_len))
            .collect()
    };
    // The keys over the internal columns of the join.
    let mut keys = vec![];
    match join.join_type() {
        JoinType::Inner => {
            for left_key in &left_keys {
                for right_key in &right_keys {
                    let mut key = left_key.clone();
                    key.extend(shift_key(right_key, left_len));
                    keys.push(key);
                }
            }
            if right_is_unique {
                keys.extend(left_keys.iter().cloned());
            }
            if left_is_unique {
                keys.extend(shifted_right_keys());
            }
        }
        JoinType::LeftOuter if right_is_unique => keys.extend(left_keys.iter().cloned()),
        JoinType::RightOuter if left_is_unique => keys.extend(shifted_right_keys()),
        // The internal columns of a semi or anti join are those of the side it keeps.
        JoinType::LeftSemi | JoinType::LeftAnti => keys.extend(left_keys.iter().cloned()),
        JoinType::RightSemi | JoinType::RightAnti => keys.extend(right_keys.iter().cloned()),
        _ => {}
    }
    let mut output_of = HashMap::new();
    for (output, internal) in join.output_indices().iter().enumerate() {
        output_of.entry(*internal).or_insert(output);
    }
    map_keys(keys, &output_of)
}

/// Removes an outer join whose optional side is not used and can not multiply the rows of the
/// other side.
///
/// ```text
/// Join(LeftOuter, on: f.dim_id = d.id, output: [f.id, f.amt])        Project(f.id, f.amt)
/// ├─Scan(f)                                                     =>   └─Scan(f)
/// └─Scan(d), with the key (id)
/// ```
///
/// - No column of the optional side is in the output of the join.
/// - The condition binds a key of the optional side (see [`bound_columns`]): a row of the other
///   side matches at most one row, and is in the output exactly once whether it matches or not.
///   The other conjuncts of the condition only remove matches.
///
/// The join, its states and the scan of the optional side disappear. An inner join can not be
/// removed this way: a row without a match is not in its output.
pub struct UnusedOuterJoinEliminateRule;

impl Rule<Logical> for UnusedOuterJoinEliminateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let (left, right) = (join.left(), join.right());
        let left_len = left.schema().len();
        let output = join.output_indices();
        let (bound_left, bound_right) = bound_columns(join);
        let mut memo = HashMap::new();
        let right_is_unused = output.iter().all(|&index| index < left_len);
        let left_is_unused = output.iter().all(|&index| index >= left_len);
        match join.join_type() {
            JoinType::LeftOuter if right_is_unused => {
                has_key_within(&right, &bound_right, &mut memo)
                    .then(|| LogicalProject::with_out_col_idx(left, output.iter().copied()).into())
            }
            JoinType::RightOuter if left_is_unused => has_key_within(&left, &bound_left, &mut memo)
                .then(|| {
                    LogicalProject::with_out_col_idx(
                        right,
                        output.iter().map(|&index| index - left_len),
                    )
                    .into()
                }),
            _ => None,
        }
    }
}

impl UnusedOuterJoinEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(UnusedOuterJoinEliminateRule)
    }
}

/// Removes an aggregation, a top-n or a dedup whose groups all hold exactly one row, because the
/// group key contains a key of the input.
///
/// - `Agg` with a group key and without grouping sets: a projection that computes every aggregate
///   over its single row (see [`agg_over_one_row`]). `SELECT DISTINCT` is such an aggregation
///   without aggregate. Left untouched if one of its aggregates is not listed there.
/// - `TopN` without offset that keeps at least one row per group: its input.
/// - `Dedup`: its input.
///
/// The group key must not be empty for an aggregation: without group key it emits a row over an
/// empty input.
pub struct AggOnKeyEliminateRule;

impl Rule<Logical> for AggOnKeyEliminateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let mut memo = HashMap::new();
        if let Some(agg) = plan.as_logical_agg() {
            return Self::apply_agg(agg, &mut memo);
        }
        if let Some(top_n) = plan.as_logical_top_n() {
            let input = top_n.input();
            let group_key: BTreeSet<usize> = top_n.group_key().iter().copied().collect();
            return (top_n.offset() == 0
                && top_n.limit_attr().limit() >= 1
                && has_key_within(&input, &group_key, &mut memo))
            .then_some(input);
        }
        let dedup = plan.as_logical_dedup()?;
        let input = dedup.input();
        let dedup_cols: BTreeSet<usize> = dedup.dedup_cols().iter().copied().collect();
        has_key_within(&input, &dedup_cols, &mut memo).then_some(input)
    }
}

impl AggOnKeyEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(AggOnKeyEliminateRule)
    }

    fn apply_agg(agg: &LogicalAgg, memo: &mut HashMap<PlanNodeId, Vec<Key>>) -> Option<PlanRef> {
        if agg.group_key().is_empty() || !agg.grouping_sets().is_empty() {
            return None;
        }
        let input = agg.input();
        let group_key: BTreeSet<usize> = agg.group_key().indices().collect();
        if !has_key_within(&input, &group_key, memo) {
            return None;
        }
        // The output columns are the group key columns, then the aggregate calls.
        let mut exprs: Vec<ExprImpl> = agg
            .group_key()
            .indices()
            .map(|index| InputRef::new(index, input.schema()[index].data_type.clone()).into())
            .collect();
        for call in agg.agg_calls() {
            exprs.push(agg_over_one_row(call)?);
        }
        Some(LogicalProject::create(input, exprs))
    }
}

fn case_when(condition: ExprImpl, then: ExprImpl, otherwise: ExprImpl) -> ExprImpl {
    let return_type = then.return_type();
    FunctionCall::new_unchecked(
        ExprType::Case,
        vec![condition, then, otherwise],
        return_type,
    )
    .into()
}

/// Returns the result of an aggregate call over a group of exactly one row, as an expression of
/// that row, for the built-in aggregates whose result has a simple form:
///
/// - `count(*)` is `1`, `count(x)` is `1` if `x` is not `NULL` and `0` otherwise;
/// - `sum`, `min`, `max`, `first_value`, `last_value`, `bool_and`, `bool_or`, `bit_and`, `bit_or`,
///   `bit_xor` of `x` are `x`, cast to the type of the aggregate; so is the internal aggregate
///   that carries a column that the group key determines;
/// - `string_agg(x, delimiter)` is `x`.
///
/// With a `FILTER` clause that is not `TRUE` for the row, the group is empty for the call: a
/// `count` is `0`, the others are `NULL`. The `DISTINCT` and `ORDER BY` clauses do not change the
/// result over one row.
fn agg_over_one_row(call: &PlanAggCall) -> Option<ExprImpl> {
    let AggType::Builtin(kind) = &call.agg_type else {
        return None;
    };
    if !call.direct_args.is_empty() {
        return None;
    }
    let argument = |index: usize| -> ExprImpl { call.inputs[index].clone().into() };
    let (value, on_empty) = match (*kind, call.inputs.len()) {
        (PbAggKind::Count, 0) if call.return_type == DataType::Int64 => {
            (ExprImpl::literal_bigint(1), ExprImpl::literal_bigint(0))
        }
        (PbAggKind::Count, 1) if call.return_type == DataType::Int64 => {
            let is_not_null = FunctionCall::new_unchecked(
                ExprType::IsNotNull,
                vec![argument(0)],
                DataType::Boolean,
            )
            .into();
            (
                case_when(
                    is_not_null,
                    ExprImpl::literal_bigint(1),
                    ExprImpl::literal_bigint(0),
                ),
                ExprImpl::literal_bigint(0),
            )
        }
        (
            PbAggKind::Sum
            | PbAggKind::Min
            | PbAggKind::Max
            | PbAggKind::FirstValue
            | PbAggKind::LastValue
            | PbAggKind::InternalLastSeenValue
            | PbAggKind::BoolAnd
            | PbAggKind::BoolOr
            | PbAggKind::BitAnd
            | PbAggKind::BitOr
            | PbAggKind::BitXor,
            1,
        )
        | (PbAggKind::StringAgg, 2) => (
            argument(0).cast_explicit(&call.return_type).ok()?,
            ExprImpl::literal_null(call.return_type.clone()),
        ),
        _ => return None,
    };
    Some(match call.filter.as_expr_unless_true() {
        Some(filter) => case_when(filter, value, on_empty),
        None => value,
    })
}
