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

//! Cardinality feedback of batch queries (`batch_cardinality_feedback`), planner side: the
//! operators of a final batch plan whose output rows are counted at execution, with the rows the
//! planner estimates for them and the fingerprint of their sub-plan. It reads the plan and never
//! changes it.
//!
//! # Operators
//!
//! The operators that matter for join ordering: scans of tables (`seq_scan`, after their scan
//! ranges), filters, joins (`hash_join`, `lookup_join`, `nested_loop_join`) and aggregations
//! (`hash_agg`, `sort_agg`, `simple_agg`; the first phase of a two-phase aggregation is
//! `hash_agg_partial`, `sort_agg_partial` or `simple_agg_partial`, without estimate: its rows
//! depend on the parallelism).
//!
//! # Estimates
//!
//! The model of the cost-based join ordering (`cost_based_join_ordering_rule`), applied to the
//! batch operators: the rows of a table come from the storage statistics; a scan range or a
//! filter conjunct keeps [`EQ_CONST_SELECTIVITY`] (`column = constant`) or
//! [`FILTER_SELECTIVITY`] (anything else) of the rows, and a scan range or filter that fixes the
//! whole primary key keeps one row; an inner join on columns that cover a key of one side keeps
//! one row of that side per row of the other side ([`equi_join_selectivity`]); an aggregation
//! returns one row without group key, as many rows as its input when it groups by a key of its
//! input, [`AGG_GROUP_FRACTION`] of them otherwise; outer joins return their preserved side, semi
//! and anti joins [`SEMI_JOIN_FRACTION`] of it. An operator whose input has no estimate (a table
//! without statistics, a source, a table function...) has no estimate: it is never invented.
//!
//! # Fingerprint
//!
//! A 64-bit FNV-1a hash of a canonical text of the sub-plan below the operator (the operator
//! included), so that the same sub-plan is recognised across queries. What is normalised:
//!
//! - a column is identified by the table and column id it is read from (a column computed by a
//!   projection or an aggregation, by the fingerprint of its expression or of the aggregation),
//!   never by its position: the columns that a query outputs and their order do not matter;
//! - constants are replaced by a placeholder with their type: literals, parameters and every
//!   constant sub-expression (`'2026-01-01'::date + 1`); the list of an `IN` of constants becomes
//!   one placeholder, whatever its length; the scan ranges of a scan keep their shape (the key
//!   columns fixed by equalities, which bounds a range has, whether they are inclusive) and not
//!   their values, and several ranges of the same shape count once;
//! - the conjuncts of a filter or of a join condition are sorted, and so are the operands of
//!   `=`, `<>`, `AND`, `OR`, `+`, `*`, `IS [NOT] DISTINCT FROM`;
//! - the inputs of an inner or full outer join are sorted, a right outer, semi or anti join is
//!   written as the left one with its inputs swapped; the physical join (hash, lookup, nested
//!   loop) is not part of the fingerprint but of the operator kind beside it;
//! - an aggregation is its input and its sorted group key columns (the aggregate calls do not
//!   change its rows); a two-phase aggregation has the fingerprint of the one-phase aggregation;
//! - projections, exchanges and sorts are transparent: they do not change the rows;
//! - the other operators are their name and their inputs (a limit hides its value).
//!
//! The fingerprint holds the ids of the tables and columns read: a table dropped and created again
//! gets new fingerprints. Only hashes leave the planner: the text that is hashed, which holds
//! no constant anyway, is never kept.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::scan_range::ScanRange;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::plan_common::JoinType;

use super::plan_node::generic::{Agg, GenericPlanRef, Join};
use super::plan_node::{BatchPlanRef, BatchSeqScan, PlanNodeId};
use super::rule::{
    AGG_GROUP_FRACTION, EQ_CONST_SELECTIVITY, FILTER_SELECTIVITY, SEMI_JOIN_FRACTION,
    conjunct_selectivity, equi_join_selectivity,
};
use crate::catalog::table_catalog::TableType;
use crate::expr::{Expr, ExprImpl, ExprType};
use crate::utils::{Condition, IndexSet};

/// An operator whose output rows are counted.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedbackOperator {
    /// The kind of operator, e.g. `hash_join`.
    pub kind: &'static str,
    /// The estimated output rows, `None` when unknown.
    pub estimated_rows: Option<f64>,
    /// The fingerprint of the sub-plan of the operator.
    pub fingerprint: u64,
}

/// The operators of the final batch plan `plan` whose output rows are counted, by id of plan node.
pub fn feedback_operators(
    plan: &BatchPlanRef,
    stats: &HummockVersionStats,
) -> BTreeMap<u32, FeedbackOperator> {
    let mut analyzer = Analyzer {
        stats,
        recording: true,
        operators: BTreeMap::new(),
    };
    analyzer.visit(plan);
    analyzer.operators
}

/// Estimated rows of a relation.
#[derive(Debug, Clone, Copy)]
struct Estimate {
    rows: f64,
    /// Rows of the table the relation reads, before its filters (or its rows if it does not just
    /// read a table): the number of distinct values of a key of the relation.
    domain: f64,
}

impl Estimate {
    fn of_rows(rows: f64) -> Self {
        let rows = rows.max(1.0);
        Estimate { rows, domain: rows }
    }
}

/// What the analysis knows about the output of an operator.
struct Info {
    fingerprint: u64,
    /// The identity of each output column.
    columns: Vec<u64>,
    estimate: Option<Estimate>,
    /// Output columns that hold a unique key, if known (empty: at most one row).
    key: Option<Vec<usize>>,
}

struct Analyzer<'a> {
    stats: &'a HummockVersionStats,
    /// False below the right side of a lookup join, which is not an executed input.
    recording: bool,
    operators: BTreeMap<u32, FeedbackOperator>,
}

impl Analyzer<'_> {
    fn record(&mut self, id: PlanNodeId, kind: &'static str, info: &Info) {
        self.record_with_estimate(id, kind, info.fingerprint, info.estimate.map(|e| e.rows));
    }

    fn record_with_estimate(
        &mut self,
        id: PlanNodeId,
        kind: &'static str,
        fingerprint: u64,
        estimated_rows: Option<f64>,
    ) {
        if !self.recording {
            return;
        }
        if let Ok(id) = u32::try_from(id.0) {
            self.operators.insert(
                id,
                FeedbackOperator {
                    kind,
                    estimated_rows,
                    fingerprint,
                },
            );
        }
    }

    fn visit(&mut self, plan: &BatchPlanRef) -> Info {
        let id = plan.plan_base().id();
        if let Some(scan) = plan.as_batch_seq_scan() {
            let info = self.seq_scan(scan);
            self.record(id, "seq_scan", &info);
            return info;
        }
        if let Some(filter) = plan.as_batch_filter() {
            let input = self.visit(&plan.inputs()[0]);
            let info = filter_info(input, filter.predicate());
            self.record(id, "filter", &info);
            return info;
        }
        if let Some(project) = plan.as_batch_project() {
            let input = self.visit(&plan.inputs()[0]);
            return project_info(input, project.exprs());
        }
        if plan.as_batch_exchange().is_some() || plan.as_batch_sort().is_some() {
            return self.visit(&plan.inputs()[0]);
        }
        if let Some(limit) = plan.as_batch_limit() {
            let input = self.visit(&plan.inputs()[0]);
            return Info {
                fingerprint: hash(&["limit", &hex(input.fingerprint)]),
                estimate: input
                    .estimate
                    .map(|e| Estimate::of_rows(e.rows.min(limit.limit() as f64))),
                ..input
            };
        }
        if let Some(values) = plan.as_batch_values() {
            let width = plan.schema().len();
            let fingerprint = hash(&["values", &width.to_string()]);
            let rows = values.row_count();
            return Info {
                fingerprint,
                columns: derived_columns(fingerprint, width),
                estimate: Some(Estimate::of_rows(rows as f64)),
                key: if rows <= 1 { Some(vec![]) } else { None },
            };
        }
        if let Some(join) = plan.as_batch_hash_join() {
            return self.join(id, "hash_join", join.core(), false);
        }
        if let Some(join) = plan.as_batch_lookup_join() {
            return self.join(id, "lookup_join", join.core(), true);
        }
        if let Some(join) = plan.as_batch_nested_loop_join() {
            return self.join(id, "nested_loop_join", join.core(), false);
        }
        if let Some(agg) = plan.as_batch_hash_agg() {
            return self.agg(id, "hash_agg", agg.core());
        }
        if let Some(agg) = plan.as_batch_sort_agg() {
            return self.agg(id, "sort_agg", agg.core());
        }
        if let Some(agg) = plan.as_batch_simple_agg() {
            return self.agg(id, "simple_agg", &agg.core);
        }
        // Any other operator: its name and its inputs, without estimate.
        let inputs = plan
            .inputs()
            .iter()
            .map(|input| hex(self.visit(input).fingerprint))
            .collect_vec();
        let name = format!("{:?}", plan.node_type());
        let fingerprint = hash(
            &std::iter::once(name.as_str())
                .chain(inputs.iter().map(String::as_str))
                .collect_vec(),
        );
        Info {
            fingerprint,
            columns: derived_columns(fingerprint, plan.schema().len()),
            estimate: None,
            key: None,
        }
    }

    fn seq_scan(&self, scan: &BatchSeqScan) -> Info {
        let core = scan.core();
        let table = &core.table_catalog;
        let column = |index: usize| column_identity(table.id, table.columns()[index].column_id());
        let pk = table
            .pk()
            .iter()
            .map(|order| order.column_index)
            .collect_vec();

        let ranges: BTreeSet<String> = scan
            .scan_ranges()
            .iter()
            .map(|range| scan_range_shape(range, &pk, &column))
            .collect();
        let mut parts = vec![
            "scan".to_owned(),
            table.id.as_raw_id().to_string(),
            ranges.into_iter().join(";"),
        ];
        if scan.limit().is_some() {
            parts.push("limit".to_owned());
        }
        let fingerprint = hash(&parts.iter().map(String::as_str).collect_vec());

        let estimate = self
            .stats
            .table_stats
            .get(&table.id.as_raw_id())
            .map(|stats| {
                let table_rows = (stats.total_key_count.max(0) as f64).max(1.0);
                let mut rows = if scan.scan_ranges().is_empty() {
                    table_rows
                } else {
                    let ranges: f64 = scan
                        .scan_ranges()
                        .iter()
                        .map(|range| scan_range_rows(range, pk.len(), table_rows))
                        .sum();
                    ranges.min(table_rows)
                };
                if let Some(limit) = scan.limit() {
                    rows = rows.min(*limit as f64);
                }
                Estimate {
                    rows: rows.max(1.0),
                    domain: table_rows,
                }
            });
        let key = if matches!(table.table_type(), TableType::Internal) {
            None
        } else {
            pk.iter()
                .map(|column| core.output_col_idx.iter().position(|c| c == column))
                .collect::<Option<Vec<_>>>()
        };
        Info {
            fingerprint,
            columns: core.output_col_idx.iter().map(|&i| column(i)).collect(),
            estimate,
            key,
        }
    }

    fn join(
        &mut self,
        id: PlanNodeId,
        kind: &'static str,
        core: &Join<BatchPlanRef>,
        lookup: bool,
    ) -> Info {
        let left = self.visit(&core.left);
        // The right side of a lookup join is the table it reads by key, not an executed input.
        let recording = self.recording;
        self.recording = recording && !lookup;
        let right = self.visit(&core.right);
        self.recording = recording;

        let left_len = left.columns.len();
        let concat = left
            .columns
            .iter()
            .chain(right.columns.iter())
            .copied()
            .collect_vec();
        let condition = core.on.as_condition();
        let conjuncts = sorted_conjuncts(&condition, &concat);

        let (name, first, second) = match core.join_type {
            JoinType::Inner | JoinType::FullOuter => {
                let (a, b) = if left.fingerprint <= right.fingerprint {
                    (left.fingerprint, right.fingerprint)
                } else {
                    (right.fingerprint, left.fingerprint)
                };
                (format!("{:?}", core.join_type), a, b)
            }
            JoinType::RightOuter => ("LeftOuter".to_owned(), right.fingerprint, left.fingerprint),
            JoinType::RightSemi => ("LeftSemi".to_owned(), right.fingerprint, left.fingerprint),
            JoinType::RightAnti => ("LeftAnti".to_owned(), right.fingerprint, left.fingerprint),
            other => (format!("{:?}", other), left.fingerprint, right.fingerprint),
        };
        let (first, second) = (hex(first), hex(second));
        let fingerprint = hash(
            &["join", name.as_str(), first.as_str(), second.as_str()]
                .into_iter()
                .chain(conjuncts.iter().map(String::as_str))
                .collect_vec(),
        );

        // Columns before the projection of the join.
        let internal: &[u64] = match core.join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => &left.columns,
            JoinType::RightSemi | JoinType::RightAnti => &right.columns,
            _ => &concat,
        };
        let columns = core
            .output_indices
            .iter()
            .map(|&i| internal.get(i).copied().unwrap_or(fingerprint))
            .collect_vec();
        let internal_key = match core.join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => left.key.clone(),
            JoinType::RightSemi | JoinType::RightAnti => right.key.clone(),
            _ => None,
        };
        let key = internal_key.and_then(|key| {
            key.iter()
                .map(|column| core.output_indices.iter().position(|i| i == column))
                .collect::<Option<Vec<_>>>()
        });

        let estimate = join_estimate(core.join_type, &left, &right, left_len, &condition);
        let info = Info {
            fingerprint,
            columns,
            estimate,
            key,
        };
        self.record(id, kind, &info);
        info
    }

    fn agg(&mut self, id: PlanNodeId, kind: &'static str, core: &Agg<BatchPlanRef>) -> Info {
        if let Some((partial_id, partial)) = two_phase_partial(core) {
            // The second phase of a two-phase aggregation: both phases get the fingerprint of the
            // one-phase aggregation, its estimate goes to the second phase only.
            let input = self.visit(&partial.input);
            let info = agg_info(input, &partial.group_key, partial.agg_calls.len());
            self.record_with_estimate(partial_id, partial_kind(kind), info.fingerprint, None);
            self.record(id, kind, &info);
            return info;
        }
        let input = self.visit(&core.input);
        let info = agg_info(input, &core.group_key, core.agg_calls.len());
        self.record(id, kind, &info);
        info
    }
}

fn partial_kind(kind: &'static str) -> &'static str {
    match kind {
        "hash_agg" => "hash_agg_partial",
        "sort_agg" => "sort_agg_partial",
        _ => "simple_agg_partial",
    }
}

/// The first phase of a two-phase aggregation whose second phase is `total`: the aggregation
/// below an exchange whose aggregate calls `total` turns into total ones, as the planner builds
/// them (`partial_to_total_agg_call`).
fn two_phase_partial(total: &Agg<BatchPlanRef>) -> Option<(PlanNodeId, Agg<BatchPlanRef>)> {
    total.input.as_batch_exchange()?;
    let below = total.input.inputs().into_iter().next()?;
    let partial = if let Some(agg) = below.as_batch_hash_agg() {
        agg.core().clone()
    } else if let Some(agg) = below.as_batch_sort_agg() {
        agg.core().clone()
    } else {
        below.as_batch_simple_agg()?.core.clone()
    };
    let group_len = partial.group_key.len();
    let is_two_phase = total.group_key.to_vec() == (0..group_len).collect_vec()
        && total.agg_calls.len() == partial.agg_calls.len()
        && partial
            .agg_calls
            .iter()
            .zip_eq_fast(total.agg_calls.iter())
            .enumerate()
            .all(|(i, (partial_call, total_call))| {
                partial_call.agg_type.partial_to_total().is_some()
                    && partial_call.partial_to_total_agg_call(group_len + i) == *total_call
            });
    let partial_id = below.plan_base().id();
    is_two_phase.then_some((partial_id, partial))
}

fn agg_info(input: Info, group_key: &IndexSet, agg_calls: usize) -> Info {
    let group_columns = group_key
        .indices()
        .map(|g| input.columns.get(g).copied().unwrap_or(input.fingerprint))
        .collect_vec();
    let sorted_group = group_columns.iter().sorted().map(|c| hex(*c)).collect_vec();
    let input_fingerprint = hex(input.fingerprint);
    let fingerprint = hash(
        &["agg", input_fingerprint.as_str()]
            .into_iter()
            .chain(sorted_group.iter().map(String::as_str))
            .collect_vec(),
    );
    let estimate = input.estimate.map(|input_estimate| {
        let rows = if group_key.is_empty() {
            1.0
        } else if input
            .key
            .as_ref()
            .is_some_and(|key| key.iter().all(|column| group_key.contains(*column)))
        {
            input_estimate.rows
        } else {
            input_estimate.rows * AGG_GROUP_FRACTION
        };
        Estimate::of_rows(rows)
    });
    let mut columns = group_columns;
    columns.extend((0..agg_calls).map(|i| hash(&["agg_call", &hex(fingerprint), &i.to_string()])));
    Info {
        fingerprint,
        columns,
        estimate,
        key: Some((0..group_key.len()).collect()),
    }
}

fn filter_info(input: Info, predicate: &Condition) -> Info {
    let conjuncts = sorted_conjuncts(predicate, &input.columns);
    let input_fingerprint = hex(input.fingerprint);
    let fingerprint = hash(
        &["filter", input_fingerprint.as_str()]
            .into_iter()
            .chain(conjuncts.iter().map(String::as_str))
            .collect_vec(),
    );
    let fixed: BTreeSet<usize> = predicate
        .conjunctions
        .iter()
        .filter_map(|conjunct| conjunct.as_eq_const().map(|(column, _)| column.index()))
        .collect();
    let key_fixed = input
        .key
        .as_ref()
        .is_some_and(|key| key.iter().all(|column| fixed.contains(column)));
    let estimate = input.estimate.map(|estimate| {
        let rows = if key_fixed {
            1.0
        } else {
            predicate
                .conjunctions
                .iter()
                .fold(estimate.rows, |rows, conjunct| {
                    rows * conjunct_selectivity(conjunct)
                })
        };
        Estimate {
            rows: rows.max(1.0),
            domain: estimate.domain,
        }
    });
    Info {
        fingerprint,
        estimate,
        ..input
    }
}

fn project_info(input: Info, exprs: &[ExprImpl]) -> Info {
    let columns = exprs
        .iter()
        .map(|expr| match expr {
            ExprImpl::InputRef(input_ref) => input
                .columns
                .get(input_ref.index())
                .copied()
                .unwrap_or(input.fingerprint),
            _ => hash(&["expr", &normalize_expr(expr, &input.columns)]),
        })
        .collect();
    let key = input.key.as_ref().and_then(|key| {
        key.iter()
            .map(|column| {
                exprs
                    .iter()
                    .position(|expr| matches!(expr, ExprImpl::InputRef(r) if r.index() == *column))
            })
            .collect::<Option<Vec<_>>>()
    });
    Info {
        columns,
        key,
        ..input
    }
}

fn join_estimate(
    join_type: JoinType,
    left: &Info,
    right: &Info,
    left_len: usize,
    condition: &Condition,
) -> Option<Estimate> {
    let (l, r) = (left.estimate, right.estimate);
    let rows = match join_type {
        JoinType::Inner => {
            let (l, r) = (l?, r?);
            let mut left_columns = BTreeSet::new();
            let mut right_columns = BTreeSet::new();
            let mut others = 1.0;
            for conjunct in &condition.conjunctions {
                match conjunct.as_eq_cond() {
                    Some((x, y)) if x.index() < left_len && y.index() >= left_len => {
                        left_columns.insert(x.index());
                        right_columns.insert(y.index() - left_len);
                    }
                    _ => others *= conjunct_selectivity(conjunct),
                }
            }
            let covers = |key: &Option<Vec<usize>>, columns: &BTreeSet<usize>| {
                key.as_ref()
                    .is_some_and(|key| key.iter().all(|column| columns.contains(column)))
            };
            let selectivity = if left_columns.is_empty() {
                1.0
            } else {
                equi_join_selectivity(
                    (covers(&left.key, &left_columns), l.rows, l.domain),
                    (covers(&right.key, &right_columns), r.rows, r.domain),
                )
            };
            l.rows * r.rows * selectivity * others
        }
        JoinType::LeftOuter | JoinType::AsofInner | JoinType::AsofLeftOuter => l?.rows,
        JoinType::RightOuter => r?.rows,
        JoinType::FullOuter => l?.rows + r?.rows,
        JoinType::LeftSemi | JoinType::LeftAnti => l?.rows * SEMI_JOIN_FRACTION,
        JoinType::RightSemi | JoinType::RightAnti => r?.rows * SEMI_JOIN_FRACTION,
        JoinType::Unspecified => return None,
    };
    Some(Estimate::of_rows(rows))
}

/// Rows of a table of `table_rows` rows in one scan range.
fn scan_range_rows(range: &ScanRange, pk_len: usize, table_rows: f64) -> f64 {
    if range.eq_conds.len() >= pk_len && pk_len > 0 {
        return 1.0;
    }
    let bounded = [&range.range.0, &range.range.1]
        .iter()
        .filter(|bound| !matches!(bound, Bound::Unbounded))
        .count();
    table_rows
        * EQ_CONST_SELECTIVITY.powi(range.eq_conds.len() as i32)
        * FILTER_SELECTIVITY.powi(bounded as i32)
}

/// The shape of a scan range, without its values: the columns of the primary key fixed by
/// equalities, then the kind and the width of each bound of the range on the next columns.
fn scan_range_shape(range: &ScanRange, pk: &[usize], column: &impl Fn(usize) -> u64) -> String {
    let eq_len = range.eq_conds.len();
    let eq = pk.iter().take(eq_len).map(|&c| hex(column(c))).join(",");
    let bound = |bound: &Bound<Vec<risingwave_common::types::Datum>>| match bound {
        Bound::Included(values) => format!("i{}", bound_columns(pk, eq_len, values.len(), column)),
        Bound::Excluded(values) => format!("e{}", bound_columns(pk, eq_len, values.len(), column)),
        Bound::Unbounded => "u".to_owned(),
    };
    format!(
        "eq[{}]lo[{}]hi[{}]",
        eq,
        bound(&range.range.0),
        bound(&range.range.1)
    )
}

fn bound_columns(pk: &[usize], from: usize, len: usize, column: &impl Fn(usize) -> u64) -> String {
    pk.iter()
        .skip(from)
        .take(len)
        .map(|&c| hex(column(c)))
        .join(",")
}

/// The normalised text of each conjunct of `condition`, sorted.
fn sorted_conjuncts(condition: &Condition, columns: &[u64]) -> Vec<String> {
    condition
        .conjunctions
        .iter()
        .map(|conjunct| normalize_expr(conjunct, columns))
        .sorted()
        .collect()
}

/// The text of an expression with its columns replaced by their identity and its constants by a
/// placeholder of their type.
pub(crate) fn normalize_expr(expr: &ExprImpl, columns: &[u64]) -> String {
    if expr.is_const() {
        return format!("?:{}", expr.return_type());
    }
    match expr {
        ExprImpl::InputRef(input_ref) => match columns.get(input_ref.index()) {
            Some(column) => format!("#{}", hex(*column)),
            None => format!("#?{}", input_ref.index()),
        },
        ExprImpl::Parameter(_) => format!("?:{}", expr.return_type()),
        ExprImpl::FunctionCall(call) => {
            let inputs = call.inputs();
            let mut args = if call.func_type() == ExprType::In
                && inputs.len() > 1
                && inputs[1..].iter().all(is_constant)
            {
                vec![
                    normalize_expr(&inputs[0], columns),
                    format!("?list:{}", inputs[1].return_type()),
                ]
            } else {
                inputs
                    .iter()
                    .map(|input| normalize_expr(input, columns))
                    .collect_vec()
            };
            if matches!(
                call.func_type(),
                ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::And
                    | ExprType::Or
                    | ExprType::Add
                    | ExprType::Multiply
                    | ExprType::IsDistinctFrom
                    | ExprType::IsNotDistinctFrom
            ) {
                args.sort();
            }
            format!(
                "{:?}:{}({})",
                call.func_type(),
                call.return_type(),
                args.join(",")
            )
        }
        ExprImpl::FunctionCallWithLambda(call) => format!(
            "lambda:{:?}:{}({})",
            call.func_type(),
            expr.return_type(),
            call.base()
                .inputs()
                .iter()
                .map(|input| normalize_expr(input, columns))
                .join(",")
        ),
        ExprImpl::UserDefinedFunction(udf) => format!(
            "udf:{}:{}({})",
            udf.catalog.name,
            expr.return_type(),
            udf.args
                .iter()
                .map(|input| normalize_expr(input, columns))
                .join(",")
        ),
        // Not expected in a batch plan: only the kind of expression and its type.
        _ => format!("{}:{}", expr.variant_name(), expr.return_type()),
    }
}

fn is_constant(expr: &ExprImpl) -> bool {
    matches!(expr, ExprImpl::Parameter(_)) || expr.is_const()
}

fn column_identity(table_id: TableId, column_id: risingwave_common::catalog::ColumnId) -> u64 {
    hash(&[
        "column",
        &table_id.as_raw_id().to_string(),
        &column_id.get_id().to_string(),
    ])
}

/// Identities of the columns of an operator that are computed by the operator.
fn derived_columns(fingerprint: u64, width: usize) -> Vec<u64> {
    (0..width)
        .map(|i| hash(&["output", &hex(fingerprint), &i.to_string()]))
        .collect()
}

/// The fingerprint as shown to users: 16 hexadecimal digits.
pub fn hex(fingerprint: u64) -> String {
    format!("{:016x}", fingerprint)
}

/// FNV-1a over the parts, each followed by a separator: stable across versions and processes.
fn hash(parts: &[&str]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for part in parts {
        for byte in part.bytes().chain(std::iter::once(0x1f)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_sqlparser::parser::Parser;

    use super::*;
    use crate::handler::HandlerArgs;
    use crate::handler::query::gen_batch_plan_by_statement;
    use crate::optimizer::OptimizerContext;
    use crate::test_utils::{LocalFrontend, set_table_row_counts_for_test};

    async fn frontend() -> LocalFrontend {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql("create table t1 (id int primary key, a int, b varchar, c int)")
            .await
            .unwrap();
        frontend
            .run_sql("create table t2 (id int primary key, t1_id int, x int, y int)")
            .await
            .unwrap();
        frontend
    }

    fn operators(frontend: &LocalFrontend, sql: &str) -> BTreeMap<u32, FeedbackOperator> {
        let session = frontend.session_ref();
        let stmt = Parser::parse_sql(sql).unwrap().remove(0);
        let handler_args = HandlerArgs::new(session.clone(), &stmt, Arc::from(sql)).unwrap();
        let context = OptimizerContext::from_handler_args(handler_args);
        let plan = gen_batch_plan_by_statement(&session, context.into(), stmt)
            .unwrap()
            .unwrap_rw()
            .unwrap()
            .plan;
        let stats = session
            .env()
            .catalog_reader()
            .read_guard()
            .table_stats()
            .clone();
        feedback_operators(&plan, &stats)
    }

    /// The (kind, fingerprint) of the counted operators, sorted.
    fn fingerprints(frontend: &LocalFrontend, sql: &str) -> Vec<(&'static str, u64)> {
        operators(frontend, sql)
            .into_values()
            .map(|op| (op.kind, op.fingerprint))
            .sorted()
            .collect()
    }

    #[tokio::test]
    async fn test_same_query_with_other_literals_has_the_same_fingerprints() {
        let frontend = frontend().await;
        let a = fingerprints(
            &frontend,
            "select t1.b, t2.x from t1 join t2 on t1.id = t2.t1_id \
             where t1.a = 1 and t1.b in ('p', 'q') and t2.y > 10",
        );
        let b = fingerprints(
            &frontend,
            "select t1.b, t2.x from t1 join t2 on t1.id = t2.t1_id \
             where t1.a = 42 and t1.b in ('r', 's', 't') and t2.y > 99",
        );
        assert!(!a.is_empty());
        assert!(a.iter().any(|(kind, _)| kind.ends_with("join")));
        assert!(a.iter().any(|(kind, _)| *kind == "filter"));
        assert_eq!(a, b);

        // The columns output and their order do not matter either.
        let c = fingerprints(
            &frontend,
            "select t2.y, t1.c, t1.id from t1 join t2 on t1.id = t2.t1_id \
             where t1.a = 7 and t1.b in ('p', 'z') and t2.y > 3",
        );
        assert_eq!(a, c);

        let agg_a = fingerprints(
            &frontend,
            "select a, count(*) from t1 where c < 5 group by a",
        );
        let agg_b = fingerprints(
            &frontend,
            "select count(*), sum(c), a from t1 where c < 500 group by a",
        );
        assert_eq!(agg_a, agg_b);
    }

    #[tokio::test]
    async fn test_other_join_columns_or_predicates_have_other_fingerprints() {
        let frontend = frontend().await;
        let join_fingerprint = |sql: &str| {
            fingerprints(&frontend, sql)
                .into_iter()
                .filter(|(kind, _)| kind.ends_with("join"))
                .map(|(_, fingerprint)| fingerprint)
                .collect_vec()
        };
        let on_id = join_fingerprint("select * from t1 join t2 on t1.id = t2.t1_id");
        let on_a = join_fingerprint("select * from t1 join t2 on t1.a = t2.x");
        let on_a_y = join_fingerprint("select * from t1 join t2 on t1.a = t2.y");
        assert_eq!(on_id.len(), 1);
        assert_eq!(on_a.len(), 1);
        assert_ne!(on_id, on_a);
        assert_ne!(on_a, on_a_y);

        // Another comparison, another column or another table: other filters.
        let filter = |sql: &str| {
            fingerprints(&frontend, sql)
                .into_iter()
                .filter(|(kind, _)| *kind == "filter")
                .collect_vec()
        };
        assert_ne!(
            filter("select * from t1 where a = 1"),
            filter("select * from t1 where a > 1")
        );
        assert_ne!(
            filter("select * from t1 where a = 1"),
            filter("select * from t1 where c = 1")
        );
        assert_ne!(
            filter("select * from t1 where c = 1"),
            filter("select * from t2 where x = 1")
        );
        // The operands of `=` and the conjuncts are sorted.
        assert_eq!(
            filter("select * from t1 where a = c and b = 'x'"),
            filter("select * from t1 where b = 'y' and c = a")
        );
    }

    #[tokio::test]
    async fn test_estimates_from_statistics_and_unknown_without() {
        let frontend = frontend().await;
        let sql = "select * from t1 join t2 on t1.id = t2.t1_id where t2.x = 3";
        // Without statistics, nothing is estimated.
        let without = operators(&frontend, sql);
        assert!(!without.is_empty());
        assert!(without.values().all(|op| op.estimated_rows.is_none()));

        let session = frontend.session_ref();
        set_table_row_counts_for_test(
            &session,
            &BTreeMap::from([("t1".to_owned(), 1000), ("t2".to_owned(), 10000)]),
        )
        .unwrap();
        let with = operators(&frontend, sql);
        let estimate = |kind: &str| {
            with.values()
                .filter(|op| op.kind == kind)
                .filter_map(|op| op.estimated_rows)
                .collect_vec()
        };
        // `t2.x = 3` keeps a tenth of the rows of `t2`.
        assert_eq!(estimate("filter"), vec![1000.0]);
        // The join is on the key of `t1`: one row of `t1` per filtered row of `t2`.
        let joins = with
            .values()
            .filter(|op| op.kind.ends_with("join"))
            .collect_vec();
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].estimated_rows, Some(1000.0));
        // The fingerprints do not depend on the statistics.
        assert_eq!(
            without
                .values()
                .map(|op| (op.kind, op.fingerprint))
                .sorted()
                .collect_vec(),
            with.values()
                .map(|op| (op.kind, op.fingerprint))
                .sorted()
                .collect_vec()
        );
    }

    #[test]
    fn test_hash_is_stable() {
        // FNV-1a of "a" followed by the separator 0x1f.
        let mut expected: u64 = 0xcbf29ce484222325;
        for byte in [b'a', 0x1f] {
            expected ^= u64::from(byte);
            expected = expected.wrapping_mul(0x100000001b3);
        }
        assert_eq!(hash(&["a"]), expected);
        assert_ne!(hash(&["ab", "c"]), hash(&["a", "bc"]));
        assert_eq!(hex(0xab), "00000000000000ab");
    }
}
