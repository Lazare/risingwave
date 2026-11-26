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

//! Cardinality feedback of batch queries (`batch_cardinality_feedback`), execution side.
//!
//! The planner picks the operators to count, with their estimate and fingerprint
//! (`optimizer::cardinality_feedback`). The query execution marks their plan nodes
//! (`PlanNode.cardinality_feedback_id`), and their executors count the rows they output:
//!
//! - executors run by the frontend (local execution, root stage of the distributed execution)
//!   add to the counters of the query directly;
//! - a task of the distributed execution sends its counts with its final status (finished,
//!   failed, aborted or cancelled), which the stage runner adds to the counters of the query;
//! - a plan that the local execution sends to a compute node sends its counts after its last
//!   chunk, which the exchange of the frontend adds to the counters of the query.
//!
//! The counts are recorded when the query is over (the last reference to its
//! [`QueryCardinalityFeedback`] is dropped): then every task that reports has reported. The
//! count of an operator is partial, a lower bound, when the query did not succeed (failed,
//! cancelled, timed out, or its output was not fully read), when one of its executors was stopped
//! before the end of its output (by a limit above it), or when fewer executors reported than
//! the query execution started. An operator without any count is not recorded.
//!
//! The records are kept in the memory of the frontend, at most [`CARDINALITY_FEEDBACK_CAPACITY`]
//! of them, by fingerprint and kind of operator; the least recently seen ones are evicted first.
//! They hold counts and hashes only, never values. System view:
//! `rw_catalog.rw_batch_cardinality_feedback`.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use risingwave_batch::executor::CardinalityCounters;
use risingwave_common::log::LogSuppressor;

use crate::optimizer::cardinality_feedback::{FeedbackOperator, feedback_operators, hex};
use crate::optimizer::plan_node::{BatchPlanRef, PlanNodeId};
use crate::optimizer::{PlanVisitor, SysTableVisitor};
use crate::scheduler::plan_fragmenter::ExecutionPlanNode;
use crate::session::SessionImpl;

/// The maximum number of records kept in the memory of a frontend.
pub const CARDINALITY_FEEDBACK_CAPACITY: usize = 10_000;

/// The records of the frontend.
pub static CARDINALITY_FEEDBACK_STORE: LazyLock<Arc<CardinalityFeedbackStore>> =
    LazyLock::new(|| Arc::new(CardinalityFeedbackStore::new(CARDINALITY_FEEDBACK_CAPACITY)));

/// A warning is logged when the rows of an operator differ from its estimate by more than this
/// factor...
const WARN_RATIO: f64 = 100.0;
/// ... and by more than this number of rows.
const WARN_ROWS: f64 = 100_000.0;

static WARN_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(|| LogSuppressor::per_minute(10));

/// The cardinality feedback of the final batch plan `plan`, if the session asks for it. Queries
/// that read system catalogs are not observed: they have no statistics, and reading the records
/// is one of them.
pub fn plan_cardinality_feedback(
    session: &SessionImpl,
    plan: &BatchPlanRef,
) -> Option<Arc<QueryCardinalityFeedback>> {
    if !session.config().batch_cardinality_feedback() {
        return None;
    }
    if SysTableVisitor::default().visit(plan.clone()) {
        return None;
    }
    let operators = {
        let catalog = session.env().catalog_reader().read_guard();
        feedback_operators(plan, catalog.table_stats())
    };
    if operators.is_empty() {
        return None;
    }
    Some(Arc::new(QueryCardinalityFeedback::new(
        operators,
        CARDINALITY_FEEDBACK_STORE.clone(),
    )))
}

/// The cardinality feedback of one execution of a query.
pub struct QueryCardinalityFeedback {
    /// The counted operators, by id of plan node.
    operators: BTreeMap<u32, FeedbackOperator>,
    counters: Arc<CardinalityCounters>,
    /// The number of executors of each operator that the query execution starts.
    expected_instances: Mutex<HashMap<u32, u32>>,
    succeeded: AtomicBool,
    store: Arc<CardinalityFeedbackStore>,
}

impl std::fmt::Debug for QueryCardinalityFeedback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCardinalityFeedback")
            .field("operators", &self.operators.len())
            .finish_non_exhaustive()
    }
}

/// What an execution observed of an operator.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorOutcome {
    pub fingerprint: u64,
    pub kind: &'static str,
    pub estimated_rows: Option<f64>,
    pub actual_rows: u64,
    /// The count is a lower bound.
    pub partial: bool,
}

impl QueryCardinalityFeedback {
    pub fn new(
        operators: BTreeMap<u32, FeedbackOperator>,
        store: Arc<CardinalityFeedbackStore>,
    ) -> Self {
        Self {
            operators,
            counters: Arc::new(CardinalityCounters::default()),
            expected_instances: Mutex::new(HashMap::new()),
            succeeded: AtomicBool::new(false),
            store,
        }
    }

    /// The id under which the executors of the plan node `id` count their rows, if counted.
    pub fn feedback_id(&self, id: PlanNodeId) -> Option<u32> {
        u32::try_from(id.0)
            .ok()
            .filter(|id| self.operators.contains_key(id))
    }

    /// The counters of the query.
    pub fn counters(&self) -> Arc<CardinalityCounters> {
        self.counters.clone()
    }

    /// The query execution starts `instances` executors of the plan of a stage whose root is `root`.
    pub fn expect_stage(&self, root: &ExecutionPlanNode, instances: u32) {
        if let Some(id) = self.feedback_id(root.plan_node_id) {
            self.expect_instances(id, instances);
        }
        for child in &root.children {
            self.expect_stage(child, instances);
        }
    }

    fn expect_instances(&self, id: u32, instances: u32) {
        *self.expected_instances.lock().entry(id).or_default() += instances;
    }

    /// The query returned all its rows.
    pub fn set_succeeded(&self) {
        self.succeeded.store(true, Ordering::Relaxed);
    }

    /// What the execution observed, operator by operator.
    pub fn outcomes(&self) -> Vec<OperatorOutcome> {
        let succeeded = self.succeeded.load(Ordering::Relaxed);
        let expected = self.expected_instances.lock();
        let counts = self.counters.to_protobuf();
        counts
            .iter()
            .filter(|count| count.instances > 0)
            .filter_map(|count| {
                let operator = self.operators.get(&count.node_id)?;
                let expected = expected.get(&count.node_id).copied().unwrap_or(0);
                let partial =
                    !succeeded || count.completed < count.instances || count.instances < expected;
                Some(OperatorOutcome {
                    fingerprint: operator.fingerprint,
                    kind: operator.kind,
                    estimated_rows: operator.estimated_rows,
                    actual_rows: count.rows,
                    partial,
                })
            })
            .collect()
    }
}

impl Drop for QueryCardinalityFeedback {
    fn drop(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        for outcome in self.outcomes() {
            warn_if_far_from_estimate(&outcome);
            self.store.record(&outcome, now);
        }
    }
}

/// Logs a warning (rate-limited) when a complete count is far from its estimate: only the
/// fingerprint, the kind and the two numbers.
fn warn_if_far_from_estimate(outcome: &OperatorOutcome) {
    let Some(estimated) = outcome.estimated_rows else {
        return;
    };
    if outcome.partial {
        return;
    }
    let actual = outcome.actual_rows as f64;
    let ratio = actual.max(estimated).max(1.0) / actual.min(estimated).max(1.0);
    if ratio > WARN_RATIO
        && (actual - estimated).abs() > WARN_ROWS
        && let Ok(suppressed_count) = WARN_SUPPRESSOR.check()
    {
        tracing::warn!(
            fingerprint = %hex(outcome.fingerprint),
            kind = outcome.kind,
            estimated_rows = estimated.round() as u64,
            actual_rows = outcome.actual_rows,
            suppressed_count,
            "batch operator output far from its estimate"
        );
    }
}

/// A record of the feedback: one fingerprint and kind of operator.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeedbackRecord {
    /// The estimate of the last execution.
    pub estimated_rows: Option<f64>,
    pub last_actual_rows: u64,
    pub last_partial: bool,
    pub executions: u64,
    pub partial_executions: u64,
    /// Sum and number of `log10(actual / estimated)` over the complete executions with an
    /// estimate (both sides at least 1).
    log10_ratio_sum: f64,
    log10_ratio_count: u64,
    /// Microseconds since the Unix epoch.
    pub last_seen_micros: i64,
    /// Order of the last update, for the eviction.
    sequence: u64,
}

impl FeedbackRecord {
    /// The mean of `log10(actual / estimated)` over the complete executions with an estimate.
    pub fn mean_log10_ratio(&self) -> Option<f64> {
        if self.log10_ratio_count > 0 {
            Some(self.log10_ratio_sum / self.log10_ratio_count as f64)
        } else {
            None
        }
    }
}

/// The records of a frontend, bounded.
pub struct CardinalityFeedbackStore {
    capacity: usize,
    inner: Mutex<StoreInner>,
}

#[derive(Default)]
struct StoreInner {
    records: HashMap<(u64, &'static str), FeedbackRecord>,
    sequence: u64,
}

impl CardinalityFeedbackStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(StoreInner::default()),
        }
    }

    pub fn record(&self, outcome: &OperatorOutcome, now_micros: i64) {
        let mut inner = self.inner.lock();
        inner.sequence += 1;
        let sequence = inner.sequence;
        let key = (outcome.fingerprint, outcome.kind);
        if !inner.records.contains_key(&key) && inner.records.len() >= self.capacity {
            // Evict the least recently seen tenth of the records.
            let evicted = (self.capacity / 10).max(1);
            let mut by_age = inner
                .records
                .iter()
                .map(|(key, record)| (record.sequence, *key))
                .collect::<Vec<_>>();
            by_age.sort_unstable_by_key(|(sequence, _)| *sequence);
            for (_, key) in by_age.into_iter().take(evicted) {
                inner.records.remove(&key);
            }
        }
        let record = inner.records.entry(key).or_default();
        record.estimated_rows = outcome.estimated_rows;
        record.last_actual_rows = outcome.actual_rows;
        record.last_partial = outcome.partial;
        record.executions += 1;
        if outcome.partial {
            record.partial_executions += 1;
        } else if let Some(estimated) = outcome.estimated_rows {
            let actual = (outcome.actual_rows as f64).max(1.0);
            record.log10_ratio_sum += (actual / estimated.max(1.0)).log10();
            record.log10_ratio_count += 1;
        }
        record.last_seen_micros = now_micros;
        record.sequence = sequence;
    }

    /// All the records: fingerprint, kind of operator, record.
    pub fn records(&self) -> Vec<(u64, &'static str, FeedbackRecord)> {
        self.inner
            .lock()
            .records
            .iter()
            .map(|((fingerprint, kind), record)| (*fingerprint, *kind, record.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::batch_plan::OperatorCardinality;

    use super::*;

    fn operator(kind: &'static str, fingerprint: u64, estimate: Option<f64>) -> FeedbackOperator {
        FeedbackOperator {
            kind,
            estimated_rows: estimate,
            fingerprint,
        }
    }

    fn count(node_id: u32, rows: u64, completed: bool) -> OperatorCardinality {
        OperatorCardinality {
            node_id,
            rows,
            instances: 1,
            completed: completed as u32,
        }
    }

    fn feedback(store: &Arc<CardinalityFeedbackStore>) -> QueryCardinalityFeedback {
        QueryCardinalityFeedback::new(
            BTreeMap::from([
                (1, operator("seq_scan", 11, Some(1000.0))),
                (2, operator("filter", 22, None)),
                (3, operator("hash_join", 33, Some(10.0))),
                (4, operator("hash_agg", 44, Some(5.0))),
            ]),
            store.clone(),
        )
    }

    #[test]
    fn test_counts_of_the_tasks_of_a_stage_are_summed() {
        let store = Arc::new(CardinalityFeedbackStore::new(100));
        let feedback = feedback(&store);
        // Nodes 1 and 2 run in a stage of two tasks, node 3 in the root stage of the frontend.
        feedback.expect_instances(1, 2);
        feedback.expect_instances(2, 2);
        feedback.expect_instances(3, 1);
        assert_eq!(feedback.feedback_id(PlanNodeId(3)), Some(3));
        assert_eq!(feedback.feedback_id(PlanNodeId(9)), None);

        let counters = feedback.counters();
        counters.merge(&[count(1, 600, true), count(2, 60, true)]);
        counters.merge(&[count(1, 400, true), count(2, 40, true)]);
        let join = counters.register(3);
        join.add_rows(7);
        // The executor of node 3 reaches the end of its output.
        counters.merge(&[OperatorCardinality {
            node_id: 3,
            rows: 0,
            instances: 0,
            completed: 1,
        }]);
        feedback.set_succeeded();

        let outcomes = feedback.outcomes();
        assert_eq!(
            outcomes,
            vec![
                OperatorOutcome {
                    fingerprint: 11,
                    kind: "seq_scan",
                    estimated_rows: Some(1000.0),
                    actual_rows: 1000,
                    partial: false,
                },
                OperatorOutcome {
                    fingerprint: 22,
                    kind: "filter",
                    estimated_rows: None,
                    actual_rows: 100,
                    partial: false,
                },
                OperatorOutcome {
                    fingerprint: 33,
                    kind: "hash_join",
                    estimated_rows: Some(10.0),
                    actual_rows: 7,
                    partial: false,
                },
            ]
        );

        // Recorded when the query is over; node 4 was never counted and is not recorded.
        drop(feedback);
        let mut records = store.records();
        records.sort_by_key(|(fingerprint, _, _)| *fingerprint);
        assert_eq!(records.len(), 3);
        let (fingerprint, kind, scan) = &records[0];
        assert_eq!((*fingerprint, *kind), (11, "seq_scan"));
        assert_eq!(scan.last_actual_rows, 1000);
        assert_eq!(scan.executions, 1);
        assert_eq!(scan.partial_executions, 0);
        assert_eq!(scan.mean_log10_ratio(), Some(0.0));
        assert_eq!(records[1].2.mean_log10_ratio(), None);
    }

    #[test]
    fn test_partial_counts() {
        let store = Arc::new(CardinalityFeedbackStore::new(100));

        // A task of the stage of node 1 did not report (the query was cancelled).
        let feedback_1 = feedback(&store);
        feedback_1.expect_instances(1, 2);
        feedback_1.counters().merge(&[count(1, 600, true)]);
        feedback_1.set_succeeded();
        assert!(feedback_1.outcomes()[0].partial);

        // An executor was stopped before the end of its output (a limit above it).
        let feedback_2 = feedback(&store);
        feedback_2.expect_instances(1, 2);
        feedback_2
            .counters()
            .merge(&[count(1, 600, true), count(1, 10, false)]);
        feedback_2.set_succeeded();
        assert!(feedback_2.outcomes()[0].partial);

        // The query failed: every count is a lower bound.
        let feedback_3 = feedback(&store);
        feedback_3.expect_instances(1, 1);
        feedback_3.counters().merge(&[count(1, 600, true)]);
        let outcomes = feedback_3.outcomes();
        assert!(outcomes[0].partial);
        assert_eq!(outcomes[0].actual_rows, 600);

        drop(feedback_1);
        drop(feedback_2);
        drop(feedback_3);
        let records = store.records();
        assert_eq!(records.len(), 1);
        let record = &records[0].2;
        assert_eq!(record.executions, 3);
        assert_eq!(record.partial_executions, 3);
        assert!(record.last_partial);
        assert_eq!(record.mean_log10_ratio(), None);
    }

    #[test]
    fn test_store_is_bounded_and_keeps_the_recent_records() {
        let store = CardinalityFeedbackStore::new(20);
        let outcome = |fingerprint: u64, actual_rows: u64| OperatorOutcome {
            fingerprint,
            kind: "filter",
            estimated_rows: Some(10.0),
            actual_rows,
            partial: false,
        };
        for fingerprint in 0..20 {
            store.record(&outcome(fingerprint, 1000), fingerprint as i64);
        }
        // Fingerprint 0 is seen again: it is now the most recent one.
        store.record(&outcome(0, 10), 100);
        store.record(&outcome(20, 10), 101);
        let records = store.records();
        assert!(records.len() <= 20);
        let fingerprints: Vec<u64> = records.iter().map(|(f, _, _)| *f).collect();
        assert!(fingerprints.contains(&0));
        assert!(fingerprints.contains(&20));
        // The least recently seen ones (1 and 2) were evicted.
        assert!(!fingerprints.contains(&1));
        assert!(!fingerprints.contains(&2));
        let zero = &records.iter().find(|(f, _, _)| *f == 0).unwrap().2;
        assert_eq!(zero.executions, 2);
        assert_eq!(zero.last_actual_rows, 10);
        assert_eq!(zero.last_seen_micros, 100);
        // log10(1000 / 10) = 2, then log10(10 / 10) = 0.
        assert_eq!(zero.mean_log10_ratio(), Some(1.0));
    }
}
