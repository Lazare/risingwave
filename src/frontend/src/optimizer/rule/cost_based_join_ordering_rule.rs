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
//! - the width of a row of each input is estimated in bytes, from the average size of a row of
//!   the tables (`total_key_size + total_value_size` over `total_key_count`) shared out among
//!   their columns by the weights of their types, or from the types alone ([`type_width`]), for
//!   the columns that the plan above still reads ([`required_output_columns`]);
//! - the cost of a hash join counts the bytes of its probe side (left), of its build side (right,
//!   at a higher price since it is held in memory) and of its output: the build side is the side
//!   with fewer bytes, not fewer rows. A join whose right side is a table that the planner will
//!   read by key (a lookup join) costs a lookup per row of its left side, and the table is not
//!   scanned.
//!
//! The join tree is found by dynamic programming over the connected sets of inputs up to
//! [`MAX_DP_RELATIONS`] inputs, greedily beyond. Only the inner joins merged into a
//! `LogicalMultiJoin` (and two-way inner joins) are ordered: outer, semi and anti joins stay
//! where they are, as inputs of the inner joins. Inputs that are not linked by an equality are
//! cross joined in the order of the heuristic ordering.
//!
//! When an input has no estimate (a table without statistics, a source, a table function...),
//! the join keeps the heuristic order: the plan is then the one of the heuristic ordering.
//!
//! After the column pruning, [`CostBasedOuterJoinSidesRule`] also swaps the sides of a left outer,
//! semi or anti join into the right join of the other side, when its right side holds far more
//! bytes than its left side (a hash join builds its right side).
//!
//! With `batch_cardinality_feedback_learning`, the ratios measured by earlier executions
//! (`optimizer::cardinality_learning`) replace the fixed guesses: the selectivity of the filters
//! of an input, the groups of an aggregation, the fan-out of a join between two inputs, each
//! blended with the guess according to the number of observations. The bytes of a side are its
//! rows times its width; the build side of a hash join is costed with its pessimistic rows, from
//! the largest ratios observed (its memory), times its width. The join tree of the learned
//! estimates replaces the one of the fixed guesses only when, with the learned estimates, it costs
//! less by more than [`PLAN_CHANGE_FACTOR`]; without learned ratios the plan is the one above. The
//! sides of outer, semi and anti joins use the learned estimates the same way, under their own
//! margin.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::plan_common::JoinType;

use super::join_commute_rule::swap_join_sides;
use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::unique_keys_of;
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::cardinality_learning::{
    CARDINALITY_LEARNING_STORE, LearnInfo, LearnedRatio, LearningStore, PLAN_CHANGE_FACTOR,
    RatioKind, edge_key_of, learned_ratio, logical_relation,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    EqJoinPredicate, LogicalJoin, LogicalMultiJoin, LogicalMultiJoinBuilder, LogicalScan,
    PlanNodeId, PlanTreeNode, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::plan_visitor::{CardinalityVisitor, PlanVisitor};
use crate::utils::Condition;

/// Inputs ordered by dynamic programming over their connected subsets; greedy beyond.
const MAX_DP_RELATIONS: usize = 10;
/// Fraction of the rows kept by a `column = constant` conjunct.
pub(crate) const EQ_CONST_SELECTIVITY: f64 = 0.1;
/// Fraction of the rows kept by any other conjunct.
pub(crate) const FILTER_SELECTIVITY: f64 = 0.25;
/// Fraction of the input rows that a grouped aggregation returns, unless it groups by a key.
pub(crate) const AGG_GROUP_FRACTION: f64 = 0.1;
/// Fraction of the rows of the kept side that a semi or anti join returns.
pub(crate) const SEMI_JOIN_FRACTION: f64 = 0.5;
/// Rows returned by a join on columns that are not a key of either side, per row of the larger
/// side.
pub(crate) const NON_KEY_JOIN_FANOUT: f64 = 2.0;
/// Cost of a byte of the left (probe) side of a hash join.
const PROBE_COST: f64 = 1.0;
/// Cost of a byte of the right (build) side of a hash join, held in memory.
const BUILD_COST: f64 = 3.0;
/// Cost of a byte of a lookup join, per row of its left side: the left row and the row read by
/// key from the right table.
const LOOKUP_COST: f64 = 1.0;
/// Cost of a byte of the output of a join.
const OUTPUT_COST: f64 = 1.0;
/// Bytes added to the width of every row (the overhead of a row in a hash table or a chunk).
const ROW_OVERHEAD_BYTES: f64 = 8.0;
/// A left outer, semi or anti join becomes the right join of the other side only when its cost
/// is at least this many times lower: the estimates are rough.
const OUTER_SWAP_MARGIN: f64 = 2.0;

/// The output columns of each plan node that the nodes above it read, by node id. A node that
/// is not listed reads all its output columns.
pub type RequiredColumns = HashMap<PlanNodeId, BTreeSet<usize>>;

/// Orders the inputs of a `LogicalMultiJoin` by their estimated cost, or by the heuristic of
/// `LeftDeepTreeJoinOrderingRule` when an input has no estimate.
pub struct CostBasedJoinOrderingRule {
    required: Arc<RequiredColumns>,
}

impl Rule<Logical> for CostBasedJoinOrderingRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_multi_join()?;
        let heuristic = join.heuristic_ordering().ok()?;
        if let Some(reordered) = cost_based_join(join, &heuristic, self.required.get(&plan.id())) {
            return Some(reordered);
        }
        // Same as `LeftDeepTreeJoinOrderingRule`.
        Some(join.as_reordered_left_deep_join(&heuristic))
    }
}

impl CostBasedJoinOrderingRule {
    /// `required`: the output columns read by the plan above each node, see
    /// [`required_output_columns`].
    pub fn create(required: Arc<RequiredColumns>) -> BoxedRule {
        Box::new(CostBasedJoinOrderingRule { required })
    }
}

/// Swaps the sides of a two-way inner join (not merged into a `LogicalMultiJoin`, which needs
/// three inputs) when the estimated cost is lower: the smaller side becomes the build side, or
/// the right side becomes a table read by key. Without estimates the join is kept as it is.
/// The sides of the join may be any plan (an aggregation, a top-n...) with an estimate.
pub struct CostBasedJoinSidesRule {
    required: Arc<RequiredColumns>,
}

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
        cost_based_join(&builder.build(), &[0, 1], self.required.get(&plan.id()))
    }
}

impl CostBasedJoinSidesRule {
    /// `required`: the output columns read by the plan above each node, see
    /// [`required_output_columns`].
    pub fn create(required: Arc<RequiredColumns>) -> BoxedRule {
        Box::new(CostBasedJoinSidesRule { required })
    }
}

/// Turns a left outer, semi or anti join into the right join of the other side when its right
/// side, the build side of the hash join, holds far more bytes than its left side: the hash
/// table is then built on the smaller side, and the larger one is streamed. Used after the
/// column pruning, when the inputs only read the columns that are used, and after
/// `JoinCommuteRule`, which would turn the right join back. Not applied when the join may be a
/// lookup join (its right side is a table read by its key, or a table with an index), nor when a
/// side has no estimate.
pub struct CostBasedOuterJoinSidesRule {}

impl Rule<Logical> for CostBasedOuterJoinSidesRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        if !matches!(
            join.join_type(),
            JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
        ) {
            return None;
        }
        let (left, right) = (join.left(), join.right());
        let predicate =
            EqJoinPredicate::create(left.schema().len(), right.schema().len(), join.on().clone());
        if !predicate.has_eq() {
            return None;
        }
        let ctx = plan.ctx();
        let lookup_join_enabled = ctx.session_ctx().config().batch_enable_lookup_join();
        if lookup_join_enabled && let Some(scan) = right.as_logical_scan() {
            let given: BTreeSet<usize> = predicate.right_eq_indexes().into_iter().collect();
            let by_key = scan_lookup_columns(scan).is_some_and(|key| key.is_subset(&given));
            if by_key || !scan.table_indexes().is_empty() {
                return None;
            }
        }
        let learning = ctx
            .session_ctx()
            .config()
            .batch_cardinality_feedback_learning();
        let catalog_reader = ctx.session_ctx().env().catalog_reader().read_guard();
        // With `batch_cardinality_feedback_learning`, the learned estimates, the build side with
        // its pessimistic rows; the same as the fixed guesses when nothing is learned.
        let estimator = Estimator {
            stats: catalog_reader.table_stats(),
            lookup_join_enabled,
            learned: learning.then_some(&**CARDINALITY_LEARNING_STORE),
        };
        let l = estimator.input_estimate(&left)?;
        let r = estimator.input_estimate(&right)?;
        let l_width = estimator.row_width(&left, None);
        let r_width = estimator.row_width(&right, None);
        drop(catalog_reader);
        let (l_bytes, r_bytes) = (l.rows * l_width, r.rows * r_width);
        let as_is = l_bytes * PROBE_COST + r.hi * r_width * BUILD_COST;
        let swapped = r_bytes * PROBE_COST + l.hi * l_width * BUILD_COST;
        if swapped * OUTER_SWAP_MARGIN >= as_is {
            return None;
        }
        if ctx.is_explain_trace() {
            ctx.trace(format!(
                "Cost-based join sides: {:?} join with {} bytes on the left and {} on the right \
                 becomes the right join of the other side",
                join.join_type(),
                l_bytes.round(),
                r_bytes.round()
            ));
        }
        Some(swap_join_sides(join).into())
    }
}

impl CostBasedOuterJoinSidesRule {
    pub fn create() -> BoxedRule {
        Box::new(CostBasedOuterJoinSidesRule {})
    }
}

/// Builds the join tree of least estimated cost of the inputs of `join`, or returns `None` if an
/// input has no estimate or if the tree is the left-deep tree of `heuristic`.
/// `required` are the output columns of `join` that the plan above reads (all if `None`).
fn cost_based_join(
    join: &LogicalMultiJoin,
    heuristic: &[usize],
    required: Option<&BTreeSet<usize>>,
) -> Option<PlanRef> {
    let ctx = join.base.ctx();
    let lookup_join_enabled = ctx.session_ctx().config().batch_enable_lookup_join();
    let learning = ctx
        .session_ctx()
        .config()
        .batch_cardinality_feedback_learning();
    // Implied equalities (`batch_transitive_predicates`): the inputs that the equalities of the
    // multi-join link only through other inputs are linked too, see [`ImpliedEqualities`].
    let original = join;
    let implied = (ctx.session_ctx().config().batch_transitive_predicates()
        && join.inputs().len() < 64)
        .then(|| ImpliedEqualities::of(join))
        .filter(|implied| !implied.equalities.is_empty());
    let augmented = implied.as_ref().map(|implied| implied.augment(join));
    let join = augmented.as_ref().unwrap_or(join);
    let separate_implied = |graph: &mut JoinGraph| {
        if let Some(implied) = &implied {
            graph.separate_implied(implied);
        }
    };
    let catalog_reader = ctx.session_ctx().env().catalog_reader().read_guard();
    let estimator = Estimator {
        stats: catalog_reader.table_stats(),
        lookup_join_enabled,
        learned: None,
    };
    let mut graph = estimator.graph(join, required)?;
    separate_implied(&mut graph);
    let mut tree = graph.best_tree(heuristic)?;

    // The learned estimates (`batch_cardinality_feedback_learning`): their join tree replaces the
    // one of the fixed guesses only when it costs clearly less with the learned estimates.
    if learning {
        let learned_estimator = Estimator {
            stats: catalog_reader.table_stats(),
            lookup_join_enabled,
            learned: Some(&**CARDINALITY_LEARNING_STORE),
        };
        if let Some(mut learned_graph) = learned_estimator.graph(join, required)
            && let Some(candidate) = {
                separate_implied(&mut learned_graph);
                learned_graph.best_tree(heuristic)
            }
            && candidate != tree
        {
            let current_cost = learned_graph.planned(&tree).cost;
            let candidate_cost = learned_graph.planned(&candidate).cost;
            let change = current_cost > PLAN_CHANGE_FACTOR * candidate_cost;
            if ctx.is_explain_trace() {
                ctx.trace(format!(
                    "Cost-based join ordering with learned estimates: join tree {} of learned cost {} instead of {} of learned cost {}: {}",
                    candidate,
                    candidate_cost.round(),
                    tree,
                    current_cost.round(),
                    if change { "changed" } else { "kept" }
                ));
            }
            if change {
                tree = candidate;
                graph = learned_graph;
            }
        }
    }
    drop(catalog_reader);

    let mut ordering = vec![];
    tree.leaves(&mut ordering);
    let left_deep = JoinTree::left_deep(heuristic);
    if tree == left_deep {
        return None;
    }
    if ctx.is_explain_trace() {
        ctx.trace(format!(
            "Cost-based join ordering: estimated rows of the inputs {:?}, bytes per row {:?}, join tree {} instead of {}",
            graph.estimates.iter().map(|e| e.rows.round()).collect_vec(),
            graph.widths.iter().map(|w| w.round()).collect_vec(),
            tree,
            left_deep
        ));
    }
    let inputs = join.inputs();
    let joined = tree.build(&inputs);
    // Only the implied equalities that the tree needs: those between two sub-trees that no
    // equality of the same class links.
    if let Some(implied) = &implied {
        return Some(
            implied
                .needed_by(original, &tree)
                .finish_reordered_join(joined, &ordering),
        );
    }
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
    /// Pessimistic rows, with the largest learned ratios (`batch_cardinality_feedback_learning`),
    /// for the memory of a build side; the same as `rows` when nothing is learned.
    hi: f64,
}

impl Estimate {
    fn of_rows(rows: f64) -> Self {
        let rows = rows.max(1.0);
        Estimate {
            rows,
            domain: rows,
            read: rows,
            hi: rows,
        }
    }

    /// Estimated rows and pessimistic rows (at least the estimated ones).
    fn of_rows_hi(rows: f64, hi: f64) -> Self {
        let estimate = Self::of_rows(rows);
        Estimate {
            hi: hi.max(estimate.rows),
            ..estimate
        }
    }

    fn filtered(self, conjunctions: &[ExprImpl]) -> Self {
        let rows = conjunctions
            .iter()
            .fold(self.rows, |rows, expr| rows * conjunct_selectivity(expr));
        let hi = conjunctions
            .iter()
            .fold(self.hi, |rows, expr| rows * conjunct_selectivity(expr));
        let rows = rows.max(1.0);
        Estimate {
            rows,
            hi: hi.max(rows),
            ..self
        }
    }

    /// The relation is `base` (`base_rows` rows, `base_hi` pessimistic rows) times a ratio whose
    /// fixed guess gave this estimate, and that was learned as `learned`.
    fn with_learned_ratio(self, base_rows: f64, base_hi: f64, learned: &LearnedRatio) -> Self {
        let guess = self.rows / base_rows.max(1.0);
        let ratio = learned.blend(guess);
        let rows = (base_rows * ratio).max(1.0);
        Estimate {
            rows,
            hi: (base_hi * ratio.max(learned.max)).max(rows),
            ..self
        }
    }
}

/// The fraction of the rows that a conjunct of a filter keeps. Shared with the cardinality
/// feedback of batch queries (`optimizer::cardinality_feedback`).
pub(crate) fn conjunct_selectivity(expr: &ExprImpl) -> f64 {
    if expr.as_eq_const().is_some() {
        EQ_CONST_SELECTIVITY
    } else {
        FILTER_SELECTIVITY
    }
}

/// The selectivity of the equalities between two inputs, each given as (whether the equalities
/// cover a unique key of the input, its estimated rows, the rows of the table it reads before its
/// filters). A key to foreign key join keeps one row of the key side per row of the other side;
/// a join on other columns returns [`NON_KEY_JOIN_FANOUT`] times the larger side. Shared with the
/// cardinality feedback of batch queries (`optimizer::cardinality_feedback`).
pub(crate) fn equi_join_selectivity(a: (bool, f64, f64), b: (bool, f64, f64)) -> f64 {
    let (a_key, a_rows, a_domain) = a;
    let (b_key, b_rows, b_domain) = b;
    match (a_key, b_key) {
        (true, true) => 1.0 / a_domain.max(b_domain),
        (true, false) => 1.0 / a_domain,
        (false, true) => 1.0 / b_domain,
        (false, false) => (NON_KEY_JOIN_FANOUT / a_rows.min(b_rows)).min(1.0),
    }
}

/// Whether one of the unique keys `keys` is made of the given columns only.
fn has_key_within(keys: &[BTreeSet<usize>], columns: &BTreeSet<usize>) -> bool {
    keys.iter().any(|key| key.is_subset(columns))
}

struct Estimator<'a> {
    stats: &'a HummockVersionStats,
    lookup_join_enabled: bool,
    /// The learned ratios, when used (`batch_cardinality_feedback_learning`).
    learned: Option<&'a LearningStore>,
}

impl Estimator<'_> {
    /// The ratio learned for the relation `plan`, if used and known.
    fn learned(&self, kind: RatioKind, plan: &PlanRef) -> Option<LearnedRatio> {
        learned_ratio(self.learned?, kind, plan)
    }

    /// The estimate of an input of a join: [`Self::estimate`], bounded by the cardinality that
    /// the plan proves (at most one row for an aggregation without group, a `LIMIT`...). A plan
    /// without estimate but with a proven bound is estimated at that bound.
    fn input_estimate(&self, plan: &PlanRef) -> Option<Estimate> {
        let bound = CardinalityVisitor.visit(plan.clone()).hi();
        match (self.estimate(plan), bound) {
            (Some(estimate), Some(bound)) => {
                let rows = estimate.rows.min(bound as f64).max(1.0);
                Some(Estimate {
                    rows,
                    hi: estimate.hi.min(bound as f64).max(rows),
                    ..estimate
                })
            }
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
                hi: table_rows,
            }
            .filtered(&scan.predicate().conjunctions);
            if !scan.predicate().always_true()
                && let Some(learned) = self.learned(RatioKind::Selectivity, plan)
            {
                estimate = estimate.with_learned_ratio(table_rows, table_rows, &learned);
            }
            if unique_keys_of(plan).iter().any(|key| key.is_empty()) {
                // The filter fixes a unique key.
                estimate.rows = 1.0;
                estimate.hi = 1.0;
            }
            return Some(estimate);
        }
        if let Some(filter) = plan.as_logical_filter() {
            let input = self.estimate(&filter.input())?;
            let mut estimate = input.filtered(&filter.predicate().conjunctions);
            if let Some(learned) = self.learned(RatioKind::Selectivity, plan) {
                estimate = estimate.with_learned_ratio(input.rows, input.hi, &learned);
            }
            if unique_keys_of(plan).iter().any(|key| key.is_empty()) {
                estimate.rows = 1.0;
                estimate.hi = 1.0;
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
            let (rows, hi) = if agg.group_key().is_empty() {
                (1.0, 1.0)
            } else if unique_keys_of(&agg.input())
                .iter()
                .any(|key| key.iter().all(|column| agg.group_key().contains(*column)))
            {
                (input.rows, input.hi)
            } else if let Some(learned) = self.learned(RatioKind::GroupRatio, plan) {
                let ratio = learned.blend(AGG_GROUP_FRACTION);
                (input.rows * ratio, input.hi * ratio.max(learned.max))
            } else {
                (
                    input.rows * AGG_GROUP_FRACTION,
                    input.hi * AGG_GROUP_FRACTION,
                )
            };
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows_hi(rows, hi)
            });
        }
        if let Some(limit) = plan.as_logical_limit() {
            let input = self.estimate(&limit.input())?;
            let limit = limit.limit() as f64;
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows_hi(input.rows.min(limit), input.hi.min(limit))
            });
        }
        if let Some(top_n) = plan.as_logical_top_n() {
            let input = self.estimate(&top_n.input())?;
            let (rows, hi) = if top_n.group_key().is_empty() {
                let limit = top_n.limit_attr().limit() as f64;
                (input.rows.min(limit), input.hi.min(limit))
            } else {
                (input.rows, input.hi)
            };
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows_hi(rows, hi)
            });
        }
        if let Some(over_window) = plan.as_logical_over_window() {
            // The window functions add columns to every row.
            let input = self.estimate(&over_window.input())?;
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows_hi(input.rows, input.hi)
            });
        }
        if let Some(dedup) = plan.as_logical_dedup() {
            let input = self.estimate(&dedup.input())?;
            return Some(Estimate {
                read: input.read,
                ..Estimate::of_rows_hi(input.rows, input.hi)
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
                ..Estimate::of_rows_hi(
                    inputs.iter().map(|e| e.rows).sum(),
                    inputs.iter().map(|e| e.hi).sum(),
                )
            });
        }
        if let Some(join) = plan.as_logical_join() {
            return self.join_estimate(plan, join);
        }
        if let Some(multi_join) = plan.as_logical_multi_join() {
            let graph = self.graph(multi_join, None)?;
            let all = (1u64 << graph.estimates.len()) - 1;
            return Some(Estimate {
                read: graph.estimates.iter().map(|e| e.read).sum(),
                ..Estimate::of_rows_hi(graph.rows(all), graph.rows_hi(all))
            });
        }
        None
    }

    fn join_estimate(&self, plan: &PlanRef, join: &LogicalJoin) -> Option<Estimate> {
        let side = |input: PlanRef| self.estimate(&input);
        let (rows, hi) = match join.join_type() {
            JoinType::Inner => {
                let builder = LogicalMultiJoinBuilder::new(plan.clone());
                if builder.inputs().len() < 2 {
                    // A temporal join: one row of the right side per row of the left side, at
                    // most.
                    let left = side(join.left())?;
                    (left.rows, left.hi)
                } else {
                    let graph = self.graph(&builder.build(), None)?;
                    let all = (1u64 << graph.estimates.len()) - 1;
                    (graph.rows(all), graph.rows_hi(all))
                }
            }
            JoinType::LeftOuter | JoinType::AsofInner | JoinType::AsofLeftOuter => {
                let left = side(join.left())?;
                (left.rows, left.hi)
            }
            JoinType::RightOuter => {
                let right = side(join.right())?;
                (right.rows, right.hi)
            }
            JoinType::FullOuter => {
                let (left, right) = (side(join.left())?, side(join.right())?);
                (left.rows + right.rows, left.hi + right.hi)
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                let left = side(join.left())?;
                (left.rows * SEMI_JOIN_FRACTION, left.hi * SEMI_JOIN_FRACTION)
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                let right = side(join.right())?;
                (
                    right.rows * SEMI_JOIN_FRACTION,
                    right.hi * SEMI_JOIN_FRACTION,
                )
            }
            JoinType::Unspecified => return None,
        };
        Some(Estimate::of_rows_hi(rows, hi))
    }

    /// Bytes of a row of `plan`, for the given output columns (all if `None`).
    fn row_width(&self, plan: &PlanRef, required: Option<&BTreeSet<usize>>) -> f64 {
        let widths = self.column_widths(plan);
        let bytes: f64 = match required {
            Some(required) => required.iter().filter_map(|&c| widths.get(c)).sum(),
            None => widths.iter().sum(),
        };
        bytes + ROW_OVERHEAD_BYTES
    }

    /// Bytes of each output column of `plan`. Always known: from the storage statistics of the
    /// tables when they give sizes, from the data types otherwise.
    fn column_widths(&self, plan: &PlanRef) -> Vec<f64> {
        let by_type = || {
            plan.schema()
                .fields()
                .iter()
                .map(|field| type_width(&field.data_type))
                .collect_vec()
        };
        if let Some(scan) = plan.as_logical_scan() {
            return self.scan_column_widths(scan);
        }
        if let Some(filter) = plan.as_logical_filter() {
            return self.column_widths(&filter.input());
        }
        if let Some(limit) = plan.as_logical_limit() {
            return self.column_widths(&limit.input());
        }
        if let Some(top_n) = plan.as_logical_top_n() {
            return self.column_widths(&top_n.input());
        }
        if let Some(dedup) = plan.as_logical_dedup() {
            return self.column_widths(&dedup.input());
        }
        if let Some(over_window) = plan.as_logical_over_window() {
            // The columns of the input, then the results of the window functions.
            let mut widths = self.column_widths(&over_window.input());
            let types = by_type();
            widths.extend(
                types
                    .get(widths.len()..)
                    .unwrap_or_default()
                    .iter()
                    .copied(),
            );
            return widths;
        }
        if let Some(share) = plan.as_logical_share() {
            return self.column_widths(&share.input());
        }
        if let Some(project) = plan.as_logical_project() {
            let input = self.column_widths(&project.input());
            return project
                .exprs()
                .iter()
                .map(|expr| match expr.as_input_ref() {
                    Some(input_ref) => input[input_ref.index()],
                    None => type_width(&expr.return_type()),
                })
                .collect();
        }
        if let Some(agg) = plan.as_logical_agg() {
            if !agg.grouping_sets().is_empty() {
                return by_type();
            }
            let input_plan = agg.input();
            let input = self.column_widths(&input_plan);
            let input_schema = input_plan.schema();
            let mut widths = agg.group_key().indices().map(|i| input[i]).collect_vec();
            // An aggregate that returns the type of its argument (`first_value`, `last_value`,
            // `min`, `max`, `internal_last_seen_value`, `string_agg`...) is as wide as it.
            widths.extend(
                agg.agg_calls()
                    .iter()
                    .map(|call| match call.inputs.first() {
                        Some(arg)
                            if input_schema.fields()[arg.index()].data_type == call.return_type =>
                        {
                            input[arg.index()]
                        }
                        _ => type_width(&call.return_type),
                    }),
            );
            return widths;
        }
        if let Some(join) = plan.as_logical_join() {
            let mut internal = self.column_widths(&join.left());
            internal.extend(self.column_widths(&join.right()));
            return join
                .output_indices()
                .iter()
                .map(|&i| internal.get(i).copied().unwrap_or(ROW_OVERHEAD_BYTES))
                .collect();
        }
        if let Some(multi_join) = plan.as_logical_multi_join() {
            let internal = multi_join
                .inputs()
                .iter()
                .flat_map(|input| self.column_widths(input))
                .collect_vec();
            return multi_join
                .output_indices()
                .iter()
                .map(|&i| internal[i])
                .collect();
        }
        if let Some(union) = plan.as_logical_union() {
            let mut widths = by_type();
            for input in union.inputs() {
                for (width, input_width) in
                    widths.iter_mut().zip_eq_fast(self.column_widths(&input))
                {
                    *width = width.max(input_width);
                }
            }
            return widths;
        }
        by_type()
    }

    /// Bytes of each output column of a scan: the average bytes of a row of the table in storage,
    /// shared out among all its columns by the weights of their types; the type widths if the
    /// statistics give no size.
    fn scan_column_widths(&self, scan: &LogicalScan) -> Vec<f64> {
        let table = scan.table();
        let types = table
            .columns
            .iter()
            .map(|column| type_width(column.data_type()))
            .collect_vec();
        let row_bytes = self
            .stats
            .table_stats
            .get(&table.id.as_raw_id())
            .filter(|stats| stats.total_key_count > 0)
            .map(|stats| {
                (stats.total_key_size.max(0) + stats.total_value_size.max(0)) as f64
                    / stats.total_key_count as f64
            })
            .filter(|bytes| *bytes > 0.0);
        let scale = match row_bytes {
            Some(row_bytes) => row_bytes / types.iter().sum::<f64>().max(1.0),
            None => 1.0,
        };
        scan.output_col_idx()
            .iter()
            .map(|&column| types[column] * scale)
            .collect()
    }

    /// The join graph of a multi-join, or `None` if an input has no estimate. `required` are the
    /// output columns of the multi-join that the plan above reads (all if `None`): the width of
    /// each input counts them and the columns of the conditions.
    fn graph(
        &self,
        join: &LogicalMultiJoin,
        required: Option<&BTreeSet<usize>>,
    ) -> Option<JoinGraph> {
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
        // What the learning knows of each input, when learned ratios are used.
        let relations: Option<Vec<(LearnInfo, Vec<u64>)>> = self
            .learned
            .map(|_| inputs.iter().map(logical_relation).collect());

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
                    factors.push((mask_of(conjunct), FILTER_SELECTIVITY, FILTER_SELECTIVITY));
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
            let selectivity =
                equi_join_selectivity((a_key, ea.rows, ea.domain), (b_key, eb.rows, eb.domain));
            // A learned fan-out of the edge: output rows per row of the larger input, so a
            // selectivity of the fan-out over the rows of the smaller input.
            let learned = relations.as_ref().and_then(|relations| {
                let a_columns = columns[0].iter().copied().collect_vec();
                let b_columns = columns[1].iter().copied().collect_vec();
                let key = edge_key_of(
                    (
                        &relations[a].0,
                        relations[a].1.as_slice(),
                        a_columns.as_slice(),
                    ),
                    (
                        &relations[b].0,
                        relations[b].1.as_slice(),
                        b_columns.as_slice(),
                    ),
                )?;
                self.learned?.get(RatioKind::FanOut, key)
            });
            let (selectivity, selectivity_hi) = match learned {
                Some(learned) => {
                    let smaller = ea.rows.min(eb.rows).max(1.0);
                    let fan_out = learned.blend(selectivity * smaller);
                    (
                        (fan_out / smaller).min(1.0),
                        (fan_out.max(learned.max) / smaller).min(1.0),
                    )
                }
                None => (selectivity, selectivity),
            };
            factors.push(((1 << a) | (1 << b), selectivity, selectivity_hi));
        }
        for conjunct in &others.conjunctions {
            let mask = mask_of(conjunct);
            if mask != 0 {
                let selectivity = conjunct_selectivity(conjunct);
                factors.push((mask, selectivity, selectivity));
            }
        }

        // The columns of each input that are read: by the conditions and by the plan above.
        let mut input_required = vec![BTreeSet::new(); n];
        let output_indices = join.output_indices();
        let mut internal: BTreeSet<usize> = match required {
            Some(required) => required
                .iter()
                .filter_map(|&o| output_indices.get(o).copied())
                .collect(),
            None => output_indices.iter().copied().collect(),
        };
        for conjunct in &join.on().conjunctions {
            internal.extend(conjunct.collect_input_refs(column_input.len()).ones());
        }
        for column in internal {
            let (input, local) = column_input[column];
            input_required[input].insert(local);
        }
        let widths = inputs
            .iter()
            .zip_eq_fast(&input_required)
            .map(|(input, required)| self.row_width(input, Some(required)))
            .collect();

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
            widths,
            neighbours,
            factors,
            eq_columns,
            lookup_columns,
            implied: vec![],
            written: vec![],
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
    /// Bytes of a row of each input, for the columns that are read.
    widths: Vec<f64>,
    /// The inputs linked to each input by an equality, as a bit set.
    neighbours: Vec<u64>,
    /// The selectivity of the conditions, with the inputs they refer to as a bit set, and the
    /// pessimistic selectivity (the same unless learned).
    factors: Vec<(u64, f64, f64)>,
    /// `eq_columns[r][i]`: the columns of input `r` equal to a column of input `i`.
    eq_columns: Vec<Vec<BTreeSet<usize>>>,
    /// The columns of each input that a lookup into it needs, if it can be read by key.
    lookup_columns: Vec<Option<BTreeSet<usize>>>,
    /// Implied equalities (`batch_transitive_predicates`): the selectivity of the equalities
    /// between two inputs that are only implied by others, `(a, b, selectivity, pessimistic)`,
    /// and the inputs linked to each input by a written equality. See
    /// [`JoinGraph::implied_selectivity`].
    implied: Vec<(usize, usize, f64, f64)>,
    written: Vec<u64>,
}

/// A sub-tree of the join tree being built.
#[derive(Clone)]
struct Planned {
    inputs: u64,
    cost: f64,
    rows: f64,
    /// Pessimistic rows, for the memory of a build side.
    rows_hi: f64,
    tree: JoinTree,
}

fn bits(mask: u64) -> impl Iterator<Item = usize> {
    (0..64).filter(move |i| mask & (1u64 << *i) != 0)
}

impl JoinGraph {
    /// Estimated rows of the join of a set of inputs, the same whatever the order of the joins.
    fn rows(&self, inputs: u64) -> f64 {
        let mut rows: f64 = bits(inputs).map(|i| self.estimates[i].rows).product();
        for (mask, selectivity, _) in &self.factors {
            if mask & inputs == *mask {
                rows *= selectivity;
            }
        }
        rows *= self.implied_selectivity(inputs, false);
        rows.max(1.0)
    }

    /// Pessimistic rows of the join of a set of inputs: the same as [`Self::rows`] unless
    /// ratios are learned.
    fn rows_hi(&self, inputs: u64) -> f64 {
        let mut rows: f64 = bits(inputs).map(|i| self.estimates[i].hi).product();
        for (mask, _, selectivity) in &self.factors {
            if mask & inputs == *mask {
                rows *= selectivity;
            }
        }
        rows *= self.implied_selectivity(inputs, true);
        rows.max(1.0)
    }

    /// Bytes of a row of the join of a set of inputs.
    fn width(&self, inputs: u64) -> f64 {
        bits(inputs).map(|i| self.widths[i]).sum()
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
            cost: self.estimates[input].read * self.widths[input],
            rows: self.estimates[input].rows,
            rows_hi: self.estimates[input].hi,
            tree: JoinTree::Leaf(input),
        }
    }

    /// The cost and rows of the join tree `tree`.
    fn planned(&self, tree: &JoinTree) -> Planned {
        match tree {
            JoinTree::Leaf(input) => self.leaf(*input),
            JoinTree::Join(left, right) => self.join(&self.planned(left), &self.planned(right)),
        }
    }

    /// The cost of joining `left` (probe side) with `right` (build side, or read by key).
    /// In bytes: with the same width on both sides, the side choice is the one by rows.
    fn join_cost(&self, left: &Planned, right: &Planned, rows: f64) -> f64 {
        let (left_width, right_width) = (self.width(left.inputs), self.width(right.inputs));
        let output = rows * (left_width + right_width) * OUTPUT_COST;
        if self.is_lookup(left.inputs, right.inputs) {
            left.cost + left.rows * (left_width + right_width) * LOOKUP_COST + output
        } else {
            left.cost
                + right.cost
                + left.rows * left_width * PROBE_COST
                + right.rows_hi * right_width * BUILD_COST
                + output
        }
    }

    fn join(&self, left: &Planned, right: &Planned) -> Planned {
        let inputs = left.inputs | right.inputs;
        let rows = self.rows(inputs);
        Planned {
            inputs,
            cost: self.join_cost(left, right, rows),
            rows,
            rows_hi: self.rows_hi(inputs),
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
            let rows_hi = self.rows_hi(global(set));
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
                            rows_hi,
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

/// Bytes of a value of the type, when the statistics give no size, and the weight of a column
/// when the average size of a row of its table is shared out among its columns. Text, binary
/// and JSON values get large defaults.
pub(crate) fn type_width(data_type: &DataType) -> f64 {
    match data_type {
        DataType::Boolean => 1.0,
        DataType::Int16 => 2.0,
        DataType::Int32 | DataType::Float32 | DataType::Date => 4.0,
        DataType::Int64
        | DataType::Float64
        | DataType::Serial
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz => 8.0,
        DataType::Decimal | DataType::Interval => 16.0,
        DataType::Int256 => 32.0,
        DataType::Varchar => 64.0,
        DataType::Bytea => 256.0,
        DataType::Jsonb => 1024.0,
        _ => 128.0,
    }
}

/// The output columns of every node of `root` that the nodes above it read, for the width of
/// the inputs of joins before the column pruning: the scans still read every column at that
/// point. An estimate only: a node that is not handled here reads all the columns of its inputs.
pub fn required_output_columns(root: &PlanRef) -> RequiredColumns {
    let mut required = RequiredColumns::new();
    collect_required(root, (0..root.schema().len()).collect(), &mut required);
    required
}

fn collect_required(plan: &PlanRef, columns: BTreeSet<usize>, required: &mut RequiredColumns) {
    let entry = required.entry(plan.id()).or_default();
    entry.extend(columns);
    let columns = entry.clone();
    let inputs = plan.inputs();
    for (input, input_columns) in inputs
        .iter()
        .zip_eq_fast(input_required_columns(plan, &columns))
    {
        collect_required(input, input_columns, required);
    }
}

/// The input columns that the expressions refer to.
fn expr_columns<'a>(exprs: impl Iterator<Item = &'a ExprImpl>, num: usize) -> BTreeSet<usize> {
    exprs
        .flat_map(|expr| expr.collect_input_refs(num).ones().collect_vec())
        .collect()
}

/// The columns of each input of `plan` that it reads to produce the output columns `columns`.
fn input_required_columns(plan: &PlanRef, columns: &BTreeSet<usize>) -> Vec<BTreeSet<usize>> {
    if let Some(project) = plan.as_logical_project() {
        let num = project.input().schema().len();
        return vec![expr_columns(
            columns.iter().filter_map(|&c| project.exprs().get(c)),
            num,
        )];
    }
    if let Some(filter) = plan.as_logical_filter() {
        let num = filter.input().schema().len();
        let mut input = expr_columns(filter.predicate().conjunctions.iter(), num);
        input.extend(columns);
        return vec![input];
    }
    if let Some(agg) = plan.as_logical_agg() {
        let num = agg.input().schema().len();
        let mut input: BTreeSet<usize> = agg.group_key().indices().collect();
        for set in agg.grouping_sets() {
            input.extend(set.indices());
        }
        for call in agg.agg_calls() {
            input.extend(call.inputs.iter().map(|i| i.index()));
            input.extend(call.order_by.iter().map(|o| o.column_index));
            input.extend(expr_columns(call.filter.conjunctions.iter(), num));
        }
        return vec![input];
    }
    if let Some(join) = plan.as_logical_join() {
        let left_num = join.left().schema().len();
        let num = left_num + join.right().schema().len();
        let mut internal = expr_columns(join.on().conjunctions.iter(), num);
        internal.extend(columns.iter().filter_map(|&c| join.output_indices().get(c)));
        return vec![
            internal.iter().copied().filter(|&c| c < left_num).collect(),
            internal
                .iter()
                .filter(|&&c| c >= left_num)
                .map(|&c| c - left_num)
                .collect(),
        ];
    }
    if let Some(multi_join) = plan.as_logical_multi_join() {
        let col_nums = multi_join.input_col_nums();
        let num = col_nums.iter().sum();
        let mut internal = expr_columns(multi_join.on().conjunctions.iter(), num);
        internal.extend(
            columns
                .iter()
                .filter_map(|&c| multi_join.output_indices().get(c)),
        );
        let mut inputs = vec![];
        let mut offset = 0;
        for num in col_nums {
            inputs.push(
                internal
                    .range(offset..offset + num)
                    .map(|&c| c - offset)
                    .collect(),
            );
            offset += num;
        }
        return inputs;
    }
    if plan.as_logical_limit().is_some() || plan.as_logical_share().is_some() {
        return vec![columns.clone()];
    }
    if let Some(top_n) = plan.as_logical_top_n() {
        let mut input = columns.clone();
        input.extend(top_n.group_key().iter().copied());
        input.extend(
            top_n
                .topn_order()
                .column_orders
                .iter()
                .map(|o| o.column_index),
        );
        return vec![input];
    }
    if let Some(dedup) = plan.as_logical_dedup() {
        let mut input = columns.clone();
        input.extend(dedup.dedup_cols().iter().copied());
        return vec![input];
    }
    if let Some(union) = plan.as_logical_union()
        && union.all()
    {
        return plan.inputs().iter().map(|_| columns.clone()).collect();
    }
    plan.inputs()
        .iter()
        .map(|input| (0..input.schema().len()).collect())
        .collect()
}

// Implied equalities of the cost-based join ordering (`batch_transitive_predicates`, rule
// `TransitivePredicateRule` for the predicates).

/// The equalities between the inputs of a multi-join that its written equalities imply but do not
/// state: `a.x = b.y AND b.y = c.z` implies `a.x = c.z` (PostgreSQL
/// `generate_base_implied_equalities_no_const`). Without them, the ordering only joins `a` with
/// `c` after `b`, or with a cross join.
///
/// The columns compared with `=` (same type, `IS NOT DISTINCT FROM` excluded) form equivalence
/// classes. For each class and each pair of inputs that hold one of its columns but that no
/// written equality links, the equality of the first column of each input is implied: `=` is an
/// equivalence relation for every type that a hash join can compare (it is reflexive on the
/// values it is `TRUE` for, symmetric and transitive), so the condition of the multi-join, an
/// `AND` of inner join conditions, holds only on rows where the implied equality holds too.
/// Adding it never changes the result. The multi-join only merges inner joins: no equality crosses
/// an outer join.
struct ImpliedEqualities {
    /// `(a, b, class, a.x = b.y)`: an implied equality between the inputs `a < b`.
    equalities: Vec<(usize, usize, usize, ExprImpl)>,
    /// `class_links[class][i]`: the inputs linked to input `i` by a written equality of `class`.
    class_links: Vec<Vec<u64>>,
    /// `written[i]`: the inputs linked to input `i` by a written equality.
    written: Vec<u64>,
}

impl ImpliedEqualities {
    fn of(join: &LogicalMultiJoin) -> Self {
        let col_nums = join.input_col_nums();
        let mut column_input = vec![];
        for (input, &num) in col_nums.iter().enumerate() {
            column_input.extend(std::iter::repeat_n(input, num));
        }
        let n = col_nums.len();
        let mut parent: Vec<usize> = (0..column_input.len()).collect();
        fn find(parent: &mut [usize], mut x: usize) -> usize {
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        }
        let mut written = vec![0u64; n];
        let mut edges = vec![];
        let mut types = HashMap::new();
        for conjunct in &join.on().conjunctions {
            let Some((x, y)) = conjunct.as_eq_cond() else {
                continue;
            };
            if x.return_type() != y.return_type() {
                continue;
            }
            let (a, b) = (column_input[x.index()], column_input[y.index()]);
            if a != b {
                written[a] |= 1 << b;
                written[b] |= 1 << a;
            }
            let (rx, ry) = (find(&mut parent, x.index()), find(&mut parent, y.index()));
            parent[rx] = ry;
            types.insert(x.index(), x.return_type());
            types.insert(y.index(), y.return_type());
            edges.push((x.index(), y.index()));
        }
        // The classes, by root column, with their columns in order.
        let mut classes: Vec<(usize, Vec<usize>)> = vec![];
        for column in types.keys().copied().sorted() {
            let root = find(&mut parent, column);
            match classes.iter_mut().find(|(r, _)| *r == root) {
                Some((_, members)) => members.push(column),
                None => classes.push((root, vec![column])),
            }
        }
        let mut class_links = vec![vec![0u64; n]; classes.len()];
        for (x, y) in edges {
            let root = find(&mut parent, x);
            let class = classes.iter().position(|(r, _)| *r == root).unwrap();
            let (a, b) = (column_input[x], column_input[y]);
            if a != b {
                class_links[class][a] |= 1 << b;
                class_links[class][b] |= 1 << a;
            }
        }
        let mut equalities = vec![];
        let mut implied_pairs = vec![];
        for (class, (_, members)) in classes.iter().enumerate() {
            // The first column of the class in each input.
            let mut firsts: Vec<(usize, usize)> = vec![];
            for &column in members {
                let input = column_input[column];
                if !firsts.iter().any(|(i, _)| *i == input) {
                    firsts.push((input, column));
                }
            }
            for (k, &(a, x)) in firsts.iter().enumerate() {
                for &(b, y) in &firsts[k + 1..] {
                    let (a, b, x, y) = if a < b { (a, b, x, y) } else { (b, a, y, x) };
                    if written[a] & (1 << b) != 0 || implied_pairs.contains(&(a, b)) {
                        continue;
                    }
                    implied_pairs.push((a, b));
                    let data_type = types[&x].clone();
                    let equality: ExprImpl = crate::expr::FunctionCall::new_unchecked(
                        crate::expr::ExprType::Equal,
                        vec![
                            crate::expr::InputRef::new(x, data_type.clone()).into(),
                            crate::expr::InputRef::new(y, data_type).into(),
                        ],
                        DataType::Boolean,
                    )
                    .into();
                    equalities.push((a, b, class, equality));
                }
            }
        }
        ImpliedEqualities {
            equalities,
            class_links,
            written,
        }
    }

    /// The multi-join with all the implied equalities in its condition, for the estimates.
    fn augment(&self, join: &LogicalMultiJoin) -> LogicalMultiJoin {
        let mut on = join.on().clone();
        on.conjunctions
            .extend(self.equalities.iter().map(|(_, _, _, e)| e.clone()));
        join.clone_with_cond(on)
    }

    /// The multi-join with the implied equalities that `tree` needs: an equality between `a` and
    /// `b` is needed where the tree joins the sub-tree of `a` with the sub-tree of `b`, unless a
    /// written equality of the same class already links these two sub-trees.
    fn needed_by(&self, join: &LogicalMultiJoin, tree: &JoinTree) -> LogicalMultiJoin {
        let mut on = join.on().clone();
        for (a, b, class, equality) in &self.equalities {
            let Some((left, right)) = tree.split_of(*a, *b) else {
                continue;
            };
            let links = &self.class_links[*class];
            if !bits(left).any(|i| links[i] & right != 0) {
                on.conjunctions.push(equality.clone());
            }
        }
        join.clone_with_cond(on)
    }
}

impl JoinTree {
    /// The inputs of the tree, as a bit set.
    fn mask(&self) -> u64 {
        match self {
            JoinTree::Leaf(input) => 1 << input,
            JoinTree::Join(left, right) => left.mask() | right.mask(),
        }
    }

    /// The inputs of the two sides of the join of the tree that brings `a` and `b` together.
    fn split_of(&self, a: usize, b: usize) -> Option<(u64, u64)> {
        let JoinTree::Join(left, right) = self else {
            return None;
        };
        let (l, r) = (left.mask(), right.mask());
        let both = (1u64 << a) | (1u64 << b);
        if l & both == both {
            left.split_of(a, b)
        } else if r & both == both {
            right.split_of(a, b)
        } else if (l | r) & both == both {
            Some((l, r))
        } else {
            None
        }
    }
}

impl JoinGraph {
    /// Takes the factors of the implied equalities out of [`JoinGraph::factors`]: the factor of
    /// the equalities between two inputs comes first among those of their bit set, and an implied
    /// equality is the only one between its two inputs.
    fn separate_implied(&mut self, implied: &ImpliedEqualities) {
        for (a, b, _, _) in &implied.equalities {
            let mask = (1u64 << a) | (1u64 << b);
            if self.implied.iter().any(|(x, y, _, _)| (x, y) == (a, b)) {
                continue;
            }
            if let Some(position) = self.factors.iter().position(|(m, _, _)| *m == mask) {
                let (_, selectivity, selectivity_hi) = self.factors.remove(position);
                self.implied.push((*a, *b, selectivity, selectivity_hi));
            }
        }
        self.written.clone_from(&implied.written);
    }

    /// The selectivity of the implied equalities within `inputs`, so that they are not counted
    /// twice: an implied equality counts only when the inputs it links are not already linked
    /// within `inputs` by written equalities or by the implied equalities counted before it. For
    /// `a.x = b.y AND b.y = c.z`, `{a, c}` counts `a.x = c.z`, and `{a, b, c}` does not.
    fn implied_selectivity(&self, inputs: u64, pessimistic: bool) -> f64 {
        if self.implied.is_empty() {
            return 1.0;
        }
        let mut label: Vec<usize> = (0..self.neighbours.len()).collect();
        let merge = |label: &mut [usize], a: usize, b: usize| {
            let (from, to) = (label[b], label[a]);
            for l in label.iter_mut() {
                if *l == from {
                    *l = to;
                }
            }
        };
        for i in bits(inputs) {
            for j in bits(self.written[i] & inputs) {
                if label[i] != label[j] {
                    merge(&mut label, i, j);
                }
            }
        }
        let mut selectivity = 1.0;
        for &(a, b, s, s_hi) in &self.implied {
            if inputs & (1 << a) != 0 && inputs & (1 << b) != 0 && label[a] != label[b] {
                selectivity *= if pessimistic { s_hi } else { s };
                merge(&mut label, a, b);
            }
        }
        selectivity
    }
}
