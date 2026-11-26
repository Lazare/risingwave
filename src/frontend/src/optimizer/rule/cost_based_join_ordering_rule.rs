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

//! Cost-based ordering of the inner joins of a batch query (`batch_cost_based_join_ordering`).
//!
//! The number of rows of each table comes from the storage statistics that the meta node sends to
//! the frontend (`total_key_count` of each state table, also shown by `rw_table_stats`). From them
//! the rules estimate the rows of each input of a join and of every intermediate result, and
//! choose the join tree of least estimated cost:
//!
//! - an input reads a table: its rows, times a fixed fraction per filter conjunct
//!   ([`EQ_CONST_SELECTIVITY`] for `column = constant`, [`FILTER_SELECTIVITY`] otherwise), and
//!   at most one row when its filter fixes a unique key;
//! - a join on columns that hold a unique key of one side (a key to foreign key join) keeps one
//!   row of that side per row of the other side: its selectivity is one over the rows of the
//!   table of the key side, before its filters. A join on other columns is assumed to return
//!   [`NON_KEY_JOIN_FANOUT`] times the larger side. The estimate of a set of inputs is the
//!   product of their rows and of the selectivities of the conditions between them, whatever
//!   the order of the joins;
//! - the cost of a hash join counts its probe side (left), its build side (right, at a higher
//!   price since it is held in memory) and its output; a join whose right side is a table that
//!   the planner will read by key (a lookup join) costs a lookup per row of its left side, and
//!   the table is not scanned.
//!
//! The join tree is found by dynamic programming over the connected sets of inputs up to
//! [`MAX_DP_RELATIONS`] inputs, greedily beyond. Only the inner joins merged into a
//! `LogicalMultiJoin` (and two-way inner joins) are ordered: outer, semi and anti joins stay
//! where they are, as inputs of the inner joins. Inputs that are not linked by an equality are
//! cross joined in the order of the heuristic ordering.
//!
//! When an input has no estimate (a table without statistics, a source, a table function...),
//! the join keeps the heuristic order: the plan is then the one of the heuristic ordering.

use std::collections::BTreeSet;

use itertools::Itertools;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::unique_keys_of;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalJoin, LogicalMultiJoin, LogicalMultiJoinBuilder, LogicalScan, PlanTreeNode,
    PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::plan_visitor::{CardinalityVisitor, PlanVisitor};
use crate::utils::Condition;

/// Inputs ordered by dynamic programming over their connected subsets; greedy beyond.
const MAX_DP_RELATIONS: usize = 10;
/// Fraction of the rows kept by a `column = constant` conjunct.
const EQ_CONST_SELECTIVITY: f64 = 0.1;
/// Fraction of the rows kept by any other conjunct.
const FILTER_SELECTIVITY: f64 = 0.25;
/// Fraction of the input rows that a grouped aggregation returns, unless it groups by a key.
const AGG_GROUP_FRACTION: f64 = 0.1;
/// Fraction of the rows of the kept side that a semi or anti join returns.
const SEMI_JOIN_FRACTION: f64 = 0.5;
/// Rows returned by a join on columns that are not a key of either side, per row of the larger
/// side.
const NON_KEY_JOIN_FANOUT: f64 = 2.0;
/// Cost of a row of the left (probe) side of a hash join.
const PROBE_COST: f64 = 1.0;
/// Cost of a row of the right (build) side of a hash join, held in memory.
const BUILD_COST: f64 = 3.0;
/// Cost of a row of the left side of a lookup join: one read by key of the right table.
const LOOKUP_COST: f64 = 2.0;
/// Cost of an output row of a join.
const OUTPUT_COST: f64 = 1.0;

/// Orders the inputs of a `LogicalMultiJoin` by their estimated cost, or by the heuristic of
/// `LeftDeepTreeJoinOrderingRule` when an input has no estimate.
pub struct CostBasedJoinOrderingRule {}

impl Rule<Logical> for CostBasedJoinOrderingRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_multi_join()?;
        let heuristic = join.heuristic_ordering().ok()?;
        if let Some(reordered) = cost_based_join(join, &heuristic) {
            return Some(reordered);
        }
        // Same as `LeftDeepTreeJoinOrderingRule`.
        Some(join.as_reordered_left_deep_join(&heuristic))
    }
}

impl CostBasedJoinOrderingRule {
    pub fn create() -> BoxedRule {
        Box::new(CostBasedJoinOrderingRule {})
    }
}

/// Swaps the sides of a two-way inner join (not merged into a `LogicalMultiJoin`, which needs
/// three inputs) when the estimated cost is lower: the smaller side becomes the build side, or
/// the right side becomes a table read by key. Without estimates the join is kept as it is.
pub struct CostBasedJoinSidesRule {}

impl Rule<Logical> for CostBasedJoinSidesRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        if join.join_type() != JoinType::Inner {
            return None;
        }
        let builder = LogicalMultiJoinBuilder::new(plan.clone());
        if builder.inputs().len() != 2 {
            return None;
        }
        cost_based_join(&builder.build(), &[0, 1])
    }
}

impl CostBasedJoinSidesRule {
    pub fn create() -> BoxedRule {
        Box::new(CostBasedJoinSidesRule {})
    }
}

/// Builds the join tree of least estimated cost of the inputs of `join`, or returns `None` if an
/// input has no estimate or if the tree is the left-deep tree of `heuristic`.
fn cost_based_join(join: &LogicalMultiJoin, heuristic: &[usize]) -> Option<PlanRef> {
    let ctx = join.base.ctx();
    let lookup_join_enabled = ctx.session_ctx().config().batch_enable_lookup_join();
    let catalog_reader = ctx.session_ctx().env().catalog_reader().read_guard();
    let estimator = Estimator {
        stats: catalog_reader.table_stats(),
        lookup_join_enabled,
    };
    let graph = estimator.graph(join)?;
    drop(catalog_reader);

    let tree = graph.best_tree(heuristic)?;
    let mut ordering = vec![];
    tree.leaves(&mut ordering);
    let left_deep = JoinTree::left_deep(heuristic);
    if tree == left_deep {
        return None;
    }
    if ctx.is_explain_trace() {
        ctx.trace(format!(
            "Cost-based join ordering: estimated rows of the inputs {:?}, join tree {} instead of {}",
            graph.estimates.iter().map(|e| e.rows.round()).collect_vec(),
            tree,
            left_deep
        ));
    }
    let inputs = join.inputs();
    let joined = tree.build(&inputs);
    Some(join.finish_reordered_join(joined, &ordering))
}

/// A join tree over the inputs of a multi-join. The right side of a join is its build side, or
/// the table read by key of a lookup join.
#[derive(Debug, Clone, PartialEq)]
enum JoinTree {
    Leaf(usize),
    Join(Box<JoinTree>, Box<JoinTree>),
}

impl JoinTree {
    fn left_deep(ordering: &[usize]) -> Self {
        let mut tree = JoinTree::Leaf(ordering[0]);
        for &input in &ordering[1..] {
            tree = JoinTree::Join(Box::new(tree), Box::new(JoinTree::Leaf(input)));
        }
        tree
    }

    fn leaves(&self, out: &mut Vec<usize>) {
        match self {
            JoinTree::Leaf(input) => out.push(*input),
            JoinTree::Join(left, right) => {
                left.leaves(out);
                right.leaves(out);
            }
        }
    }

    /// Joins the inputs without condition: the condition of the multi-join is applied above and
    /// pushed down later.
    fn build(&self, inputs: &[PlanRef]) -> PlanRef {
        match self {
            JoinTree::Leaf(input) => inputs[*input].clone(),
            JoinTree::Join(left, right) => LogicalJoin::new(
                left.build(inputs),
                right.build(inputs),
                JoinType::Inner,
                Condition::true_cond(),
            )
            .into(),
        }
    }
}

impl std::fmt::Display for JoinTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinTree::Leaf(input) => write!(f, "{}", input),
            JoinTree::Join(left, right) => write!(f, "({} x {})", left, right),
        }
    }
}

/// Estimate of a relation.
#[derive(Debug, Clone, Copy)]
struct Estimate {
    /// Estimated rows.
    rows: f64,
    /// Rows of the table the relation reads, before its filters (or its rows if it does not just
    /// read a table): the number of distinct values of a key of the relation, for the
    /// selectivity of a join on that key.
    domain: f64,
    /// Rows read to produce the relation: its cost when it is not read by key.
    read: f64,
}

impl Estimate {
    fn of_rows(rows: f64) -> Self {
        let rows = rows.max(1.0);
        Estimate {
            rows,
            domain: rows,
            read: rows,
        }
    }

    fn filtered(self, conjunctions: &[ExprImpl]) -> Self {
        let rows = conjunctions
            .iter()
            .fold(self.rows, |rows, expr| rows * conjunct_selectivity(expr));
        Estimate {
            rows: rows.max(1.0),
            ..self
        }
    }
}

fn conjunct_selectivity(expr: &ExprImpl) -> f64 {
    if expr.as_eq_const().is_some() {
        EQ_CONST_SELECTIVITY
    } else {
        FILTER_SELECTIVITY
    }
}

/// Whether one of the unique keys `keys` is made of the given columns only.
fn has_key_within(keys: &[BTreeSet<usize>], columns: &BTreeSet<usize>) -> bool {
    keys.iter().any(|key| key.is_subset(columns))
}

struct Estimator<'a> {
    stats: &'a HummockVersionStats,
    lookup_join_enabled: bool,
}

impl Estimator<'_> {
    /// The estimate of an input of a join: [`Self::estimate`], bounded by the cardinality that
    /// the plan proves (at most one row for an aggregation without group, a `LIMIT`...). A plan
    /// without estimate but with a proven bound is estimated at that bound.
    fn input_estimate(&self, plan: &PlanRef) -> Option<Estimate> {
        let bound = CardinalityVisitor.visit(plan.clone()).hi();
        match (self.estimate(plan), bound) {
            (Some(estimate), Some(bound)) => Some(Estimate {
                rows: estimate.rows.min(bound as f64).max(1.0),
                ..estimate
            }),
            (Some(estimate), None) => Some(estimate),
            (None, Some(bound)) => Some(Estimate::of_rows(bound as f64)),
            (None, None) => None,
        }
    }

    fn estimate(&self, plan: &PlanRef) -> Option<Estimate> {
        if let Some(scan) = plan.as_logical_scan() {
            let stats = self.stats.table_stats.get(&scan.table().id.as_raw_id())?;
            let table_rows = (stats.total_key_count.max(0) as f64).max(1.0);
            let mut estimate = Estimate {
                rows: table_rows,
                domain: table_rows,
                read: table_rows,
            }
            .filtered(&scan.predicate().conjunctions);
            if unique_keys_of(plan).iter().any(|key| key.is_empty()) {
                // The filter fixes a unique key.
                estimate.rows = 1.0;
            }
            return Some(estimate);
        }
        if let Some(filter) = plan.as_logical_filter() {
            let mut estimate = self
                .estimate(&filter.input())?
                .filtered(&filter.predicate().conjunctions);
            if unique_keys_of(plan).iter().any(|key| key.is_empty()) {
                estimate.rows = 1.0;
            }
            return Some(estimate);
        }
        if let Some(project) = plan.as_logical_project() {
            return self.estimate(&project.input());
        }
        if let Some(share) = plan.as_logical_share() {
            return self.estimate(&share.input());
        }
        if let Some(values) = plan.as_logical_values() {
            return Some(Estimate::of_rows(values.rows().len() as f64));
        }
        if let Some(agg) = plan.as_logical_agg() {
            let input = self.estimate(&agg.input())?;
            let rows = if agg.group_key().is_empty() {
                1.0
            } else if unique_keys_of(&agg.input())
                .iter()
                .any(|key| key.iter().all(|column| agg.group_key().contains(*column)))
            {
                input.rows
            } else {
                input.rows * AGG_GROUP_FRACTION
            };
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows(rows)
            });
        }
        if let Some(limit) = plan.as_logical_limit() {
            let input = self.estimate(&limit.input())?;
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows(input.rows.min(limit.limit() as f64))
            });
        }
        if let Some(top_n) = plan.as_logical_top_n() {
            let input = self.estimate(&top_n.input())?;
            let rows = if top_n.group_key().is_empty() {
                input.rows.min(top_n.limit_attr().limit() as f64)
            } else {
                input.rows
            };
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows(rows)
            });
        }
        if let Some(dedup) = plan.as_logical_dedup() {
            let input = self.estimate(&dedup.input())?;
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows(input.rows)
            });
        }
        if let Some(union) = plan.as_logical_union() {
            let inputs: Vec<_> = union
                .inputs()
                .iter()
                .map(|input| self.estimate(input))
                .collect::<Option<_>>()?;
            return Some(Estimate {
                read: inputs.iter().map(|e| e.read).sum(),
                ..Estimate::of_rows(inputs.iter().map(|e| e.rows).sum())
            });
        }
        if let Some(join) = plan.as_logical_join() {
            return self.join_estimate(plan, join);
        }
        if let Some(multi_join) = plan.as_logical_multi_join() {
            let graph = self.graph(multi_join)?;
            let all = (1u64 << graph.estimates.len()) - 1;
            return Some(Estimate {
                read: graph.estimates.iter().map(|e| e.read).sum(),
                ..Estimate::of_rows(graph.rows(all))
            });
        }
        None
    }

    fn join_estimate(&self, plan: &PlanRef, join: &LogicalJoin) -> Option<Estimate> {
        let side = |input: PlanRef| self.estimate(&input);
        let rows = match join.join_type() {
            JoinType::Inner => {
                let builder = LogicalMultiJoinBuilder::new(plan.clone());
                if builder.inputs().len() < 2 {
                    // A temporal join: one row of the right side per row of the left side, at
                    // most.
                    side(join.left())?.rows
                } else {
                    let graph = self.graph(&builder.build())?;
                    graph.rows((1u64 << graph.estimates.len()) - 1)
                }
            }
            JoinType::LeftOuter | JoinType::AsofInner | JoinType::AsofLeftOuter => {
                side(join.left())?.rows
            }
            JoinType::RightOuter => side(join.right())?.rows,
            JoinType::FullOuter => side(join.left())?.rows + side(join.right())?.rows,
            JoinType::LeftSemi | JoinType::LeftAnti => side(join.left())?.rows * SEMI_JOIN_FRACTION,
            JoinType::RightSemi | JoinType::RightAnti => {
                side(join.right())?.rows * SEMI_JOIN_FRACTION
            }
            JoinType::Unspecified => return None,
        };
        Some(Estimate::of_rows(rows))
    }

    /// The join graph of a multi-join, or `None` if an input has no estimate.
    fn graph(&self, join: &LogicalMultiJoin) -> Option<JoinGraph> {
        let inputs = join.inputs();
        let n = inputs.len();
        if n == 0 || n >= 64 {
            return None;
        }
        let estimates: Vec<Estimate> = inputs
            .iter()
            .map(|input| self.input_estimate(input))
            .collect::<Option<_>>()?;
        let keys: Vec<Vec<BTreeSet<usize>>> = inputs.iter().map(unique_keys_of).collect();

        // The input of each column of the multi-join, and its index in that input.
        let col_nums = join.input_col_nums();
        let mut column_input = vec![];
        for (input, &num) in col_nums.iter().enumerate() {
            column_input.extend((0..num).map(|column| (input, column)));
        }
        let mask_of = |expr: &ExprImpl| -> u64 {
            expr.collect_input_refs(column_input.len())
                .ones()
                .fold(0u64, |mask, column| mask | (1u64 << column_input[column].0))
        };

        let (pairs, others) = join.on().clone().split_by_input_col_nums(&col_nums, true);
        let mut neighbours = vec![0u64; n];
        let mut eq_columns = vec![vec![BTreeSet::new(); n]; n];
        let mut factors = vec![];
        for ((a, b), condition) in &pairs {
            let (a, b) = (*a, *b);
            neighbours[a] |= 1 << b;
            neighbours[b] |= 1 << a;
            let mut columns = [BTreeSet::new(), BTreeSet::new()];
            for conjunct in &condition.conjunctions {
                let Some((x, y)) = conjunct.as_eq_cond() else {
                    // Not expected: only equalities are split by pairs of inputs.
                    factors.push((mask_of(conjunct), FILTER_SELECTIVITY));
                    continue;
                };
                let (x_input, x_column) = column_input[x.index()];
                let (y_input, y_column) = column_input[y.index()];
                eq_columns[x_input][y_input].insert(x_column);
                eq_columns[y_input][x_input].insert(y_column);
                columns[usize::from(x_input == b)].insert(x_column);
                columns[usize::from(y_input == b)].insert(y_column);
            }
            let a_key = has_key_within(&keys[a], &columns[0]);
            let b_key = has_key_within(&keys[b], &columns[1]);
            let (ea, eb) = (estimates[a], estimates[b]);
            let selectivity = match (a_key, b_key) {
                (true, true) => 1.0 / ea.domain.max(eb.domain),
                (true, false) => 1.0 / ea.domain,
                (false, true) => 1.0 / eb.domain,
                (false, false) => (NON_KEY_JOIN_FANOUT / ea.rows.min(eb.rows)).min(1.0),
            };
            factors.push(((1 << a) | (1 << b), selectivity));
        }
        for conjunct in &others.conjunctions {
            let mask = mask_of(conjunct);
            if mask != 0 {
                factors.push((mask, conjunct_selectivity(conjunct)));
            }
        }

        let lookup_columns = inputs
            .iter()
            .map(|input| {
                if self.lookup_join_enabled {
                    input.as_logical_scan().and_then(scan_lookup_columns)
                } else {
                    None
                }
            })
            .collect();

        Some(JoinGraph {
            estimates,
            neighbours,
            factors,
            eq_columns,
            lookup_columns,
        })
    }
}

/// The columns of a scan that a lookup join on its primary table must be given: the shortest
/// prefix of the order key that holds the distribution key, as in
/// `LogicalJoin::gen_batch_lookup_join`. Lookups into indexes are not taken into account.
fn scan_lookup_columns(scan: &LogicalScan) -> Option<BTreeSet<usize>> {
    let table = scan.table();
    let order_key = table.order_column_indices().collect_vec();
    let mut prefix_len = 1;
    for column in &table.distribution_key {
        let position = order_key.iter().position(|c| c == column)?;
        prefix_len = prefix_len.max(position + 1);
    }
    if order_key.len() < prefix_len {
        return None;
    }
    order_key[..prefix_len]
        .iter()
        .map(|column| scan.output_col_idx().iter().position(|c| c == column))
        .collect()
}

/// The inputs of a multi-join, the conditions between them and the estimates.
struct JoinGraph {
    estimates: Vec<Estimate>,
    /// The inputs linked to each input by an equality, as a bit set.
    neighbours: Vec<u64>,
    /// The selectivity of the conditions, with the inputs they refer to as a bit set.
    factors: Vec<(u64, f64)>,
    /// `eq_columns[r][i]`: the columns of input `r` equal to a column of input `i`.
    eq_columns: Vec<Vec<BTreeSet<usize>>>,
    /// The columns of each input that a lookup into it needs, if it can be read by key.
    lookup_columns: Vec<Option<BTreeSet<usize>>>,
}

/// A sub-tree of the join tree being built.
#[derive(Clone)]
struct Planned {
    inputs: u64,
    cost: f64,
    rows: f64,
    tree: JoinTree,
}

fn bits(mask: u64) -> impl Iterator<Item = usize> {
    (0..64).filter(move |i| mask & (1u64 << *i) != 0)
}

impl JoinGraph {
    /// Estimated rows of the join of a set of inputs, the same whatever the order of the joins.
    fn rows(&self, inputs: u64) -> f64 {
        let mut rows: f64 = bits(inputs).map(|i| self.estimates[i].rows).product();
        for (mask, selectivity) in &self.factors {
            if mask & inputs == *mask {
                rows *= selectivity;
            }
        }
        rows.max(1.0)
    }

    fn linked(&self, left: u64, right: u64) -> bool {
        bits(left).any(|i| self.neighbours[i] & right != 0)
    }

    /// Whether the planner will read `right` by key with the columns that `left` gives.
    fn is_lookup(&self, left: u64, right: u64) -> bool {
        if right.count_ones() != 1 {
            return false;
        }
        let r = right.trailing_zeros() as usize;
        let Some(required) = &self.lookup_columns[r] else {
            return false;
        };
        let mut given = BTreeSet::new();
        for i in bits(left) {
            given.extend(self.eq_columns[r][i].iter().copied());
        }
        required.is_subset(&given)
    }

    fn leaf(&self, input: usize) -> Planned {
        Planned {
            inputs: 1 << input,
            cost: self.estimates[input].read,
            rows: self.estimates[input].rows,
            tree: JoinTree::Leaf(input),
        }
    }

    /// The cost of joining `left` (probe side) with `right` (build side, or read by key).
    fn join_cost(&self, left: &Planned, right: &Planned, rows: f64) -> f64 {
        if self.is_lookup(left.inputs, right.inputs) {
            left.cost + left.rows * LOOKUP_COST + rows * OUTPUT_COST
        } else {
            left.cost
                + right.cost
                + left.rows * PROBE_COST
                + right.rows * BUILD_COST
                + rows * OUTPUT_COST
        }
    }

    fn join(&self, left: &Planned, right: &Planned) -> Planned {
        let inputs = left.inputs | right.inputs;
        let rows = self.rows(inputs);
        Planned {
            inputs,
            cost: self.join_cost(left, right, rows),
            rows,
            tree: JoinTree::Join(Box::new(left.tree.clone()), Box::new(right.tree.clone())),
        }
    }

    /// The join tree of all the inputs: the best tree of each set of inputs linked by equalities,
    /// cross joined in the order in which the heuristic ordering puts them.
    fn best_tree(&self, heuristic: &[usize]) -> Option<JoinTree> {
        let mut components: Vec<u64> = vec![];
        for &input in heuristic {
            if components.iter().any(|c| c & (1 << input) != 0) {
                continue;
            }
            // The inputs linked to `input`.
            let mut component = 1u64 << input;
            loop {
                let next = bits(component).fold(component, |m, i| m | self.neighbours[i]);
                if next == component {
                    break;
                }
                component = next;
            }
            components.push(component);
        }
        let mut tree: Option<JoinTree> = None;
        for component in components {
            let planned = if component.count_ones() as usize <= MAX_DP_RELATIONS {
                self.dp(component)?
            } else {
                self.greedy(component)?
            };
            tree = Some(match tree {
                None => planned.tree,
                Some(left) => JoinTree::Join(Box::new(left), Box::new(planned.tree)),
            });
        }
        tree
    }

    /// Dynamic programming over the connected subsets of `component`.
    fn dp(&self, component: u64) -> Option<Planned> {
        let members = bits(component).collect_vec();
        let m = members.len();
        let global = |local: usize| -> u64 {
            bits(local as u64).fold(0, |mask, k| mask | (1 << members[k]))
        };
        let mut best: Vec<Option<Planned>> = vec![None; 1 << m];
        for (k, &input) in members.iter().enumerate() {
            best[1 << k] = Some(self.leaf(input));
        }
        // Every proper subset of `set` is smaller than `set`.
        for set in 1..(1usize << m) {
            if set.count_ones() < 2 {
                continue;
            }
            let rows = self.rows(global(set));
            let mut chosen: Option<Planned> = None;
            let mut left = (set - 1) & set;
            while left > 0 {
                let right = set & !left;
                if let (Some(l), Some(r)) = (&best[left], &best[right])
                    && self.linked(l.inputs, r.inputs)
                {
                    let cost = self.join_cost(l, r, rows);
                    if chosen.as_ref().is_none_or(|c| cost < c.cost) {
                        chosen = Some(Planned {
                            inputs: l.inputs | r.inputs,
                            cost,
                            rows,
                            tree: JoinTree::Join(
                                Box::new(l.tree.clone()),
                                Box::new(r.tree.clone()),
                            ),
                        });
                    }
                }
                left = (left - 1) & set;
            }
            best[set] = chosen;
        }
        best[(1 << m) - 1].take()
    }

    /// Greedy join of the pair of linked sub-trees of fewest estimated rows, then of lowest
    /// cost.
    fn greedy(&self, component: u64) -> Option<Planned> {
        let mut planned = bits(component).map(|i| self.leaf(i)).collect_vec();
        while planned.len() > 1 {
            let mut chosen: Option<(usize, usize, Planned)> = None;
            for (i, left) in planned.iter().enumerate() {
                for (j, right) in planned.iter().enumerate() {
                    if i == j || !self.linked(left.inputs, right.inputs) {
                        continue;
                    }
                    let joined = self.join(left, right);
                    let better = match &chosen {
                        None => true,
                        Some((_, _, c)) => (joined.rows, joined.cost) < (c.rows, c.cost),
                    };
                    if better {
                        chosen = Some((i, j, joined));
                    }
                }
            }
            let (i, j, joined) = chosen?;
            planned = planned
                .into_iter()
                .enumerate()
                .filter(|(k, _)| *k != i && *k != j)
                .map(|(_, p)| p)
                .collect();
            planned.push(joined);
        }
        planned.pop()
    }
}
