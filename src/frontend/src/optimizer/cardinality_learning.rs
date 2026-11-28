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

//! Cardinality feedback learning of batch queries (`batch_cardinality_feedback_learning`), step 2
//! of the cardinality feedback: the rows counted at execution (`optimizer::cardinality_feedback`,
//! `scheduler::cardinality_feedback`) become ratios, kept per key of sub-plan, that the estimates
//! of the planner use instead of their fixed guesses.
//!
//! # Ratios
//!
//! Ratios, not rows, so that they still hold when the tables grow:
//!
//! - [`RatioKind::Selectivity`]: the rows a filter keeps per input row. For a scan, the input is
//!   the table (the rows counted by the scan when it reads the whole table, the storage
//!   statistics when it reads key ranges), the output the scan or the filter right above it that
//!   applies the rest of its predicate;
//! - [`RatioKind::GroupRatio`]: the groups of an aggregation per input row;
//! - [`RatioKind::FanOut`]: the output rows of an inner join per row of its larger input, kept
//!   for the edge between the two relations that its equalities link.
//!
//! Only complete counts are learned: a partial count (failed or cancelled query, operator stopped
//! by a limit) is a lower bound and is ignored.
//!
//! # Keys
//!
//! The ratios are kept by key of relation, computed the same way from the logical plan (by the
//! planner) and from the final batch plan (when the query runs), so that a sub-plan seen at
//! execution is recognised during planning:
//!
//! - a leaf is a table and the predicate of its scan, before the planner splits it into key
//!   ranges and a filter (the batch scan carries the key computed from the logical scan);
//! - a filter, an aggregation (input and sorted group columns) and an inner join (its sorted base
//!   relations and conjuncts, whatever the order of the joins) are keyed from their inputs;
//! - columns are identified by table and column id, constants are placeholders: one key covers
//!   every literal value, as the fingerprints of the feedback do;
//! - the edge of a join is the pair of base relations and the columns of each that its
//!   equalities compare.
//!
//! Anything else (outer joins, unions, sources...) has no key, and nothing above it is learned.
//!
//! # Distribution and confidence
//!
//! One key covers several literal values, so each ratio keeps a small distribution: the number of
//! observations, the minimum, the maximum and a mean of `log10(ratio)` that decays (a plain mean
//! of the first five observations, then each new one weighs [`DECAY`]). An estimate blends the
//! fixed guess and the learned mean in log space, the weight of the guess being [`GUESS_WEIGHT`]
//! to the power of the number of observations; the memory decisions use the maximum (the build
//! side of a hash join, the top-n per group).
//!
//! The ratios are kept in the memory of the frontend, at most [`LEARNING_CAPACITY`] of them; the
//! least recently observed tenth is evicted when full. System view:
//! `rw_catalog.rw_batch_cardinality_learning`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_pb::plan_common::JoinType;

use super::cardinality_feedback::{column_identity, hash, hex, normalize_expr, sorted_conjuncts};
use super::plan_node::generic::GenericPlanRef;
use super::plan_node::{LogicalPlanRef, PlanTreeNode, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::catalog::TableCatalog;
use crate::expr::ExprImpl;
use crate::utils::Condition;

/// The maximum number of ratios kept in the memory of a frontend.
pub const LEARNING_CAPACITY: usize = 10_000;
/// The weight of the fixed guess after one observation, in log space; after `n` observations it
/// is this to the power `n`.
pub const GUESS_WEIGHT: f64 = 0.25;
/// The smallest weight of a new observation in the decayed mean of `log10(ratio)`.
pub const DECAY: f64 = 0.2;
/// A join tree chosen with the learned estimates replaces the one of the fixed guesses only when
/// its learned cost is lower by more than this factor. With a factor of 2 the two sides of a join
/// could never be swapped: the cost of a hash join counts its build side 3 times and its probe
/// side once, so swapping them gains less than 2.
pub const PLAN_CHANGE_FACTOR: f64 = 1.5;
/// The largest `limit + offset` of a top-n per group that the learned group ratio may allow
/// (`batch_over_window_to_topn`).
pub const LEARNED_MAX_TOP_N_ROWS_PER_GROUP: u64 = 1000;

/// The ratios learned by the frontend.
pub static CARDINALITY_LEARNING_STORE: LazyLock<Arc<LearningStore>> =
    LazyLock::new(|| Arc::new(LearningStore::new(LEARNING_CAPACITY)));

/// What a ratio measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RatioKind {
    /// Rows kept by a filter per input row.
    Selectivity,
    /// Groups of an aggregation per input row.
    GroupRatio,
    /// Output rows of an inner join per row of its larger input.
    FanOut,
}

impl RatioKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RatioKind::Selectivity => "selectivity",
            RatioKind::GroupRatio => "group_ratio",
            RatioKind::FanOut => "fan_out",
        }
    }
}

/// The distribution of a learned ratio.
#[derive(Debug, Clone, PartialEq)]
pub struct LearnedRatio {
    pub observations: u64,
    /// Decayed mean of `log10(ratio)`.
    pub mean_log10: f64,
    pub min: f64,
    pub max: f64,
}

impl LearnedRatio {
    fn first(ratio: f64) -> Self {
        LearnedRatio {
            observations: 1,
            mean_log10: ratio.log10(),
            min: ratio,
            max: ratio,
        }
    }

    fn observe(&mut self, ratio: f64) {
        let weight = (1.0 / (self.observations as f64 + 1.0)).max(DECAY);
        self.mean_log10 += weight * (ratio.log10() - self.mean_log10);
        self.min = self.min.min(ratio);
        self.max = self.max.max(ratio);
        self.observations += 1;
    }

    /// The decayed mean of the ratio (geometric).
    pub fn mean(&self) -> f64 {
        10f64.powf(self.mean_log10)
    }

    /// The weight of the learned mean against the fixed guess, from 0 to 1.
    pub fn confidence(&self) -> f64 {
        1.0 - GUESS_WEIGHT.powi(self.observations.min(64) as i32)
    }

    /// The fixed guess `guess` moved towards the learned mean, in log space.
    pub fn blend(&self, guess: f64) -> f64 {
        let weight = self.confidence();
        let guess = guess.max(f64::MIN_POSITIVE);
        10f64.powf((1.0 - weight) * guess.log10() + weight * self.mean_log10)
    }
}

/// The ratios of a frontend, bounded.
pub struct LearningStore {
    capacity: usize,
    inner: Mutex<LearningInner>,
}

#[derive(Default)]
struct LearningInner {
    /// The ratio and the order of its last observation, for the eviction.
    ratios: HashMap<(RatioKind, u64), (LearnedRatio, u64)>,
    sequence: u64,
}

impl LearningStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(LearningInner::default()),
        }
    }

    /// Adds a measured ratio. Ratios that are not finite and positive are ignored.
    pub fn observe(&self, kind: RatioKind, key: u64, ratio: f64) {
        if !ratio.is_finite() || ratio <= 0.0 {
            return;
        }
        let mut inner = self.inner.lock();
        inner.sequence += 1;
        let sequence = inner.sequence;
        if !inner.ratios.contains_key(&(kind, key)) && inner.ratios.len() >= self.capacity {
            // Evict the least recently observed tenth of the ratios.
            let evicted = (self.capacity / 10).max(1);
            let mut by_age = inner
                .ratios
                .iter()
                .map(|(key, (_, sequence))| (*sequence, *key))
                .collect_vec();
            by_age.sort_unstable_by_key(|(sequence, _)| *sequence);
            for (_, key) in by_age.into_iter().take(evicted) {
                inner.ratios.remove(&key);
            }
        }
        match inner.ratios.get_mut(&(kind, key)) {
            Some((learned, last)) => {
                learned.observe(ratio);
                *last = sequence;
            }
            None => {
                inner
                    .ratios
                    .insert((kind, key), (LearnedRatio::first(ratio), sequence));
            }
        }
    }

    pub fn get(&self, kind: RatioKind, key: u64) -> Option<LearnedRatio> {
        self.inner
            .lock()
            .ratios
            .get(&(kind, key))
            .map(|(learned, _)| learned.clone())
    }

    /// All the ratios: kind, key, distribution.
    pub fn ratios(&self) -> Vec<(RatioKind, u64, LearnedRatio)> {
        self.inner
            .lock()
            .ratios
            .iter()
            .map(|((kind, key), (learned, _))| (*kind, *key, learned.clone()))
            .collect()
    }

    pub fn clear(&self) {
        *self.inner.lock() = LearningInner::default();
    }
}

/// Where the rows of a relation are counted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowSource {
    /// The rows counted for the plan node with this id.
    Node(u32),
    /// A number of rows known when planning (the rows of a table from the storage statistics).
    Rows(f64),
}

/// A ratio to learn from the counts of an execution.
#[derive(Debug, Clone, PartialEq)]
pub struct LearnSpec {
    pub kind: RatioKind,
    pub key: u64,
    /// The plan node whose counted rows are the numerator.
    pub output: u32,
    /// The denominator: the largest of these.
    pub inputs: Vec<RowSource>,
}

impl LearnSpec {
    /// The measured ratio, from the counts of the plan nodes (rows, partial), or `None` when a
    /// count is missing or partial.
    pub fn ratio(&self, counts: &HashMap<u32, (u64, bool)>) -> Option<f64> {
        let (output, partial) = counts.get(&self.output)?;
        if *partial || self.inputs.is_empty() {
            return None;
        }
        let mut input: f64 = 0.0;
        for source in &self.inputs {
            let rows = match source {
                RowSource::Node(id) => {
                    let (rows, partial) = counts.get(id)?;
                    if *partial {
                        return None;
                    }
                    *rows as f64
                }
                RowSource::Rows(rows) => *rows,
            };
            input = input.max(rows);
        }
        Some((*output as f64).max(1.0) / input.max(1.0))
    }
}

/// The base relations and conjuncts of an inner join, flattened.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Joined {
    relations: Vec<u64>,
    conjuncts: Vec<String>,
}

impl Joined {
    fn key(&self) -> u64 {
        let relations = self
            .relations
            .iter()
            .sorted()
            .map(|r| hex(*r))
            .collect_vec();
        let conjuncts = self.conjuncts.iter().sorted().cloned().collect_vec();
        hash_parts(
            "join",
            relations
                .into_iter()
                .chain(std::iter::once("on".to_owned()))
                .chain(conjuncts),
        )
    }
}

/// What the learning knows of the output of an operator, from the logical plan (planning) or
/// from the batch plan (execution): both use the methods below, so that they agree.
#[derive(Debug, Clone, PartialEq)]
pub struct LearnInfo {
    /// The key of the relation, `None` when it cannot be learned.
    pub key: Option<u64>,
    /// The base relation of each output column, for the edges of the joins above.
    pub origins: Vec<Option<u64>>,
    /// For an inner join, its base relations and conjuncts.
    joined: Option<Joined>,
}

impl LearnInfo {
    /// A relation that cannot be learned.
    pub fn opaque(width: usize) -> Self {
        LearnInfo {
            key: None,
            origins: vec![None; width],
            joined: None,
        }
    }

    /// A base relation of the joins above it, with the key `key`.
    pub fn base(key: Option<u64>, width: usize) -> Self {
        LearnInfo {
            key,
            origins: vec![key; width],
            joined: None,
        }
    }

    /// A filter over this relation, with its normalised conjuncts.
    pub fn filter(&self, conjuncts: Vec<String>) -> Self {
        if let Some(joined) = &self.joined {
            let mut joined = joined.clone();
            joined.conjuncts.extend(conjuncts);
            return LearnInfo {
                key: Some(joined.key()),
                origins: self.origins.clone(),
                joined: Some(joined),
            };
        }
        let key = self.key.map(|input| {
            hash_parts(
                "filter",
                std::iter::once(hex(input)).chain(conjuncts.into_iter().sorted()),
            )
        });
        LearnInfo::base(key, self.origins.len())
    }

    /// A projection of this relation.
    pub fn project(&self, exprs: &[ExprImpl]) -> Self {
        LearnInfo {
            key: self.key,
            origins: exprs
                .iter()
                .map(|expr| match expr {
                    ExprImpl::InputRef(input_ref) => {
                        self.origins.get(input_ref.index()).copied().flatten()
                    }
                    _ => None,
                })
                .collect(),
            joined: self.joined.clone(),
        }
    }

    /// An aggregation of this relation grouped by the columns of identities `group`.
    pub fn agg(&self, group: &[u64], width: usize) -> Self {
        LearnInfo::base(self.key.map(|input| agg_key(input, group)), width)
    }

    /// The inner join of `inputs` on the normalised `conjuncts`, whose output is the columns
    /// `output_indices` of the concatenated inputs.
    pub fn inner_join(
        inputs: &[&LearnInfo],
        conjuncts: Vec<String>,
        output_indices: &[usize],
    ) -> Self {
        let concat = inputs
            .iter()
            .flat_map(|input| input.origins.iter().copied())
            .collect_vec();
        let origins = output_indices
            .iter()
            .map(|&i| concat.get(i).copied().flatten())
            .collect_vec();
        let mut joined = Joined {
            relations: vec![],
            conjuncts,
        };
        for input in inputs {
            match (&input.joined, input.key) {
                (Some(inner), _) => {
                    joined.relations.extend(inner.relations.iter().copied());
                    joined.conjuncts.extend(inner.conjuncts.iter().cloned());
                }
                (None, Some(key)) => joined.relations.push(key),
                (None, None) => {
                    return LearnInfo {
                        key: None,
                        origins,
                        joined: None,
                    };
                }
            }
        }
        LearnInfo {
            key: Some(joined.key()),
            origins,
            joined: Some(joined),
        }
    }
}

/// The key of a leaf: a table and the predicate of its scan (on the columns of the table).
pub fn leaf_key(table: &TableCatalog, predicate: &Condition) -> u64 {
    let columns = table
        .columns()
        .iter()
        .map(|column| column_identity(table.id, column.column_id()))
        .collect_vec();
    hash_parts(
        "leaf",
        std::iter::once(table.id.as_raw_id().to_string())
            .chain(sorted_conjuncts(predicate, &columns)),
    )
}

/// The key of an aggregation of the relation `input` grouped by the columns `group`.
pub fn agg_key(input: u64, group: &[u64]) -> u64 {
    hash_parts(
        "agg",
        std::iter::once(hex(input)).chain(group.iter().sorted().map(|c| hex(*c))),
    )
}

/// The key of the edge between two base relations joined by equalities, from the relation, the
/// identities of the columns and the compared columns of each side; `None` unless all the
/// compared columns of each side come from one base relation.
pub fn edge_key_of(
    left: (&LearnInfo, &[u64], &[usize]),
    right: (&LearnInfo, &[u64], &[usize]),
) -> Option<u64> {
    let mut sides = [
        edge_side(left.0, left.1, left.2)?,
        edge_side(right.0, right.1, right.2)?,
    ];
    sides.sort();
    Some(hash_parts("edge", sides))
}

/// One side of an edge: its base relation and the sorted identities of the compared columns.
fn edge_side(info: &LearnInfo, columns: &[u64], indices: &[usize]) -> Option<String> {
    let mut origin: Option<u64> = None;
    for &i in indices {
        let column_origin = info.origins.get(i).copied().flatten()?;
        if origin.is_some_and(|o| o != column_origin) {
            return None;
        }
        origin = Some(column_origin);
    }
    let origin = origin?;
    let ids = indices
        .iter()
        .map(|&i| columns.get(i).copied())
        .collect::<Option<Vec<_>>>()?;
    let ids = ids.into_iter().sorted().dedup().map(hex).join(",");
    Some(format!("{}:{}", hex(origin), ids))
}

fn hash_parts(tag: &str, parts: impl IntoIterator<Item = String>) -> u64 {
    let parts = std::iter::once(tag.to_owned()).chain(parts).collect_vec();
    hash(&parts.iter().map(String::as_str).collect_vec())
}

/// The identity of each column of a projection, as the cardinality feedback computes it.
fn project_columns(exprs: &[ExprImpl], input_columns: &[u64]) -> Vec<u64> {
    exprs
        .iter()
        .map(|expr| match expr {
            ExprImpl::InputRef(input_ref) => input_columns
                .get(input_ref.index())
                .copied()
                .unwrap_or_default(),
            _ => hash(&["expr", &normalize_expr(expr, input_columns)]),
        })
        .collect()
}

fn opaque_columns(width: usize) -> Vec<u64> {
    (0..width)
        .map(|i| hash(&["learning_opaque", &i.to_string()]))
        .collect()
}

/// What the learning knows of a logical plan, with the identity of its output columns.
pub fn logical_relation(plan: &LogicalPlanRef) -> (LearnInfo, Vec<u64>) {
    if let Some(scan) = plan.as_logical_scan() {
        let table = scan.table();
        let columns = scan
            .output_col_idx()
            .iter()
            .map(|&i| column_identity(table.id, table.columns()[i].column_id()))
            .collect_vec();
        let key = leaf_key(table, scan.predicate());
        return (LearnInfo::base(Some(key), columns.len()), columns);
    }
    if let Some(filter) = plan.as_logical_filter() {
        let (input, columns) = logical_relation(&filter.input());
        let conjuncts = sorted_conjuncts(filter.predicate(), &columns);
        return (input.filter(conjuncts), columns);
    }
    if let Some(project) = plan.as_logical_project() {
        let (input, input_columns) = logical_relation(&project.input());
        let columns = project_columns(project.exprs(), &input_columns);
        return (input.project(project.exprs()), columns);
    }
    if let Some(share) = plan.as_logical_share() {
        return logical_relation(&share.input());
    }
    if let Some(agg) = plan.as_logical_agg() {
        let (input, input_columns) = logical_relation(&agg.input());
        let group = agg
            .group_key()
            .indices()
            .map(|i| input_columns.get(i).copied().unwrap_or_default())
            .collect_vec();
        let info = input.agg(&group, plan.schema().len());
        let key = hex(info.key.unwrap_or_default());
        let mut columns = group;
        columns.extend(
            (0..agg.agg_calls().len()).map(|i| hash(&["learning_agg_call", &key, &i.to_string()])),
        );
        return (info, columns);
    }
    if let Some(join) = plan.as_logical_join()
        && join.join_type() == JoinType::Inner
    {
        let (left, left_columns) = logical_relation(&join.left());
        let (right, right_columns) = logical_relation(&join.right());
        let concat = left_columns
            .iter()
            .chain(right_columns.iter())
            .copied()
            .collect_vec();
        let conjuncts = sorted_conjuncts(join.on(), &concat);
        let info = LearnInfo::inner_join(&[&left, &right], conjuncts, join.output_indices());
        let columns = join
            .output_indices()
            .iter()
            .map(|&i| concat.get(i).copied().unwrap_or_default())
            .collect();
        return (info, columns);
    }
    if let Some(join) = plan.as_logical_multi_join() {
        let inputs = join.inputs().iter().map(logical_relation).collect_vec();
        let concat = inputs
            .iter()
            .flat_map(|(_, columns)| columns.iter().copied())
            .collect_vec();
        let conjuncts = sorted_conjuncts(join.on(), &concat);
        let infos = inputs.iter().map(|(info, _)| info).collect_vec();
        let info = LearnInfo::inner_join(&infos, conjuncts, join.output_indices());
        let columns = join
            .output_indices()
            .iter()
            .map(|&i| concat.get(i).copied().unwrap_or_default())
            .collect();
        return (info, columns);
    }
    let width = plan.schema().len();
    (LearnInfo::opaque(width), opaque_columns(width))
}

/// The learned ratio of the relation `plan`, if any.
pub fn learned_ratio(
    store: &LearningStore,
    kind: RatioKind,
    plan: &LogicalPlanRef,
) -> Option<LearnedRatio> {
    let key = logical_relation(plan).0.key?;
    store.get(kind, key)
}

/// Whether a top-n per group of `rows_per_group` rows (`limit + offset`) over `input`,
/// partitioned by the columns `partition_by`, is known to hold no more rows than its input
/// (`batch_cardinality_feedback_learning`): the largest group ratio learned for these columns,
/// times the rows per group, is at most 1. The heaps of the groups then hold no more rows than the
/// sort that the top-n replaces.
pub fn learned_top_n_fits(
    input: &LogicalPlanRef,
    partition_by: &[usize],
    rows_per_group: u64,
) -> bool {
    if partition_by.is_empty()
        || rows_per_group > LEARNED_MAX_TOP_N_ROWS_PER_GROUP
        || !input
            .ctx()
            .session_ctx()
            .config()
            .batch_cardinality_feedback_learning()
    {
        return false;
    }
    let (info, columns) = logical_relation(input);
    let Some(key) = info.key else {
        return false;
    };
    let Some(group) = partition_by
        .iter()
        .map(|&i| columns.get(i).copied())
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let Some(learned) = CARDINALITY_LEARNING_STORE.get(RatioKind::GroupRatio, agg_key(key, &group))
    else {
        return false;
    };
    learned.max * rows_per_group as f64 <= 1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution_and_decay() {
        let store = LearningStore::new(100);
        assert_eq!(store.get(RatioKind::Selectivity, 1), None);
        store.observe(RatioKind::Selectivity, 1, 1.0);
        store.observe(RatioKind::Selectivity, 1, 0.01);
        let learned = store.get(RatioKind::Selectivity, 1).unwrap();
        assert_eq!(learned.observations, 2);
        assert_eq!((learned.min, learned.max), (0.01, 1.0));
        // A plain mean of the first observations: log10 0 and -2.
        assert!((learned.mean_log10 + 1.0).abs() < 1e-9);
        assert!((learned.mean() - 0.1).abs() < 1e-9);
        // Another kind or key is another ratio.
        assert_eq!(store.get(RatioKind::GroupRatio, 1), None);
        assert_eq!(store.get(RatioKind::Selectivity, 2), None);

        // After many observations of 1, each new one weighs `DECAY`: the mean forgets the old
        // ones, the minimum and maximum do not.
        for _ in 0..50 {
            store.observe(RatioKind::Selectivity, 1, 1.0);
        }
        let learned = store.get(RatioKind::Selectivity, 1).unwrap();
        assert!(learned.mean_log10.abs() < 1e-3);
        assert_eq!((learned.min, learned.max), (0.01, 1.0));
        store.observe(RatioKind::Selectivity, 1, 0.001);
        let learned = store.get(RatioKind::Selectivity, 1).unwrap();
        // log10 0.001 = -3, weighed `DECAY`.
        assert!((learned.mean_log10 - (-3.0 * DECAY)).abs() < 1e-2);
        assert_eq!(learned.min, 0.001);

        // Ratios that are not finite and positive are ignored.
        store.observe(RatioKind::FanOut, 3, f64::NAN);
        store.observe(RatioKind::FanOut, 3, 0.0);
        assert_eq!(store.get(RatioKind::FanOut, 3), None);

        store.clear();
        assert!(store.ratios().is_empty());
    }

    #[test]
    fn test_confidence_grows_with_observations() {
        let mut learned = LearnedRatio::first(1.0);
        // One observation: the guess keeps a weight of `GUESS_WEIGHT` in log space.
        let one = learned.blend(0.01);
        assert!((one.log10() - (-2.0 * GUESS_WEIGHT)).abs() < 1e-9);
        learned.observe(1.0);
        let two = learned.blend(0.01);
        assert!(two > one);
        for _ in 0..10 {
            learned.observe(1.0);
        }
        assert!(learned.blend(0.01) > 0.99);
        assert!(learned.confidence() < 1.0 + 1e-12);
    }

    #[test]
    fn test_store_is_bounded() {
        let store = LearningStore::new(10);
        for key in 0..10 {
            store.observe(RatioKind::GroupRatio, key, 0.5);
        }
        // Key 0 is observed again: key 1 is now the least recently observed.
        store.observe(RatioKind::GroupRatio, 0, 0.5);
        store.observe(RatioKind::GroupRatio, 10, 0.5);
        assert!(store.ratios().len() <= 10);
        assert!(store.get(RatioKind::GroupRatio, 0).is_some());
        assert!(store.get(RatioKind::GroupRatio, 10).is_some());
        assert!(store.get(RatioKind::GroupRatio, 1).is_none());
    }

    #[test]
    fn test_partial_counts_are_not_learned() {
        let spec = LearnSpec {
            kind: RatioKind::FanOut,
            key: 7,
            output: 3,
            inputs: vec![RowSource::Node(1), RowSource::Rows(50.0)],
        };
        let complete = HashMap::from([(1, (200, false)), (3, (400, false))]);
        // 400 rows out of the larger input, 200 rows.
        assert_eq!(spec.ratio(&complete), Some(2.0));
        let partial_input = HashMap::from([(1, (200, true)), (3, (400, false))]);
        assert_eq!(spec.ratio(&partial_input), None);
        let partial_output = HashMap::from([(1, (200, false)), (3, (400, true))]);
        assert_eq!(spec.ratio(&partial_output), None);
        let missing = HashMap::from([(3, (400, false))]);
        assert_eq!(spec.ratio(&missing), None);
        // No row out of 50: counted as one row.
        let empty = HashMap::from([(1, (0, false)), (3, (0, false))]);
        assert_eq!(spec.ratio(&empty), Some(1.0 / 50.0));
    }

    #[test]
    fn test_keys_do_not_depend_on_order() {
        let a = LearnInfo::base(Some(1), 2);
        let b = LearnInfo::base(Some(2), 1);
        let c = LearnInfo::base(Some(3), 1);
        let ab = LearnInfo::inner_join(&[&a, &b], vec!["x".to_owned()], &[0, 1, 2]);
        let abc = LearnInfo::inner_join(&[&ab, &c], vec!["y".to_owned()], &[0, 1, 2, 3]);
        let ca = LearnInfo::inner_join(&[&c, &a], vec!["y".to_owned()], &[0, 1, 2]);
        let cab = LearnInfo::inner_join(&[&ca, &b], vec!["x".to_owned()], &[0, 1, 2, 3]);
        assert!(abc.key.is_some());
        assert_eq!(abc.key, cab.key);
        assert_eq!(abc.origins, vec![Some(1), Some(1), Some(2), Some(3)]);
        // An input without key: the join has none, its columns keep their origin.
        let opaque = LearnInfo::opaque(1);
        let a_opaque = LearnInfo::inner_join(&[&a, &opaque], vec![], &[0, 2]);
        assert_eq!(a_opaque.key, None);
        assert_eq!(a_opaque.origins, vec![Some(1), None]);

        // Edges: the same pair of relations and columns, in any order.
        let columns_a: &[u64] = &[11, 12];
        let columns_b: &[u64] = &[21];
        let columns_ab: &[u64] = &[11, 12, 21];
        let columns_c: &[u64] = &[31];
        let first: &[usize] = &[0];
        let second: &[usize] = &[1];
        let ab_edge = edge_key_of((&a, columns_a, second), (&b, columns_b, first));
        let ba_edge = edge_key_of((&b, columns_b, first), (&a, columns_a, second));
        assert!(ab_edge.is_some());
        assert_eq!(ab_edge, ba_edge);
        assert_ne!(
            ab_edge,
            edge_key_of((&a, columns_a, first), (&b, columns_b, first))
        );
        // Columns of two relations on one side: no edge.
        let both: &[usize] = &[0, 2];
        assert_eq!(
            edge_key_of((&ab, columns_ab, both), (&c, columns_c, first)),
            None
        );
        // Columns without origin: no edge.
        assert_eq!(
            edge_key_of((&opaque, columns_c, first), (&c, columns_c, first)),
            None
        );

        // A filter or an aggregation of a relation has another key, which the filter conjuncts
        // and the group columns (in any order) decide.
        let f1 = a.filter(vec!["p".to_owned(), "q".to_owned()]);
        let f2 = a.filter(vec!["q".to_owned(), "p".to_owned()]);
        assert_eq!(f1.key, f2.key);
        assert_ne!(f1.key, a.key);
        assert_eq!(f1.origins, vec![f1.key, f1.key]);
        assert_eq!(a.agg(&[11, 12], 2).key, a.agg(&[12, 11], 2).key);
        assert_ne!(a.agg(&[11], 2).key, a.agg(&[12], 2).key);
    }
}
