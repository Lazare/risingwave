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

//! Memory of one batch query on a compute node.
//!
//! All the tasks that a query runs on a node share one memory context, bounded to a share of the
//! batch memory of the node, so that a single query cannot take it all.
//!
//! Inside it, the spillable operators (hash agg, hash join, sort) share a pool. Each of them has
//! a small amount reserved, that the others cannot take, and uses the rest of the pool on a first
//! come, first served basis. An operator spills when the pool cannot give it more, and gives back
//! what it took when it ends.
//!
//! The peaks are recorded with one atomic operation per accounted allocation, and written once
//! when the query leaves the node, to tell whether the limits should be raised or lowered.
//!
//! With `batch_spill_on_memory_wait`, an operator that cannot spill and must wait for memory asks
//! the spillable operators of its own query to give back what they took from the shared part of
//! the pool: their contexts stop growing, so the next of them that needs more spills to disk and
//! releases its memory, which the waiting operator then takes from the pool.
//!
//! When every query that runs on the node waits for memory that only another waiting query could
//! release, one of them, the most recently admitted, is chosen to fail, so that the others can
//! proceed with the memory that it releases.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use risingwave_common::config::BatchConfig;
use risingwave_common::memory::{MemCounter, MemoryContext};
use risingwave_pb::task_service::GetBatchLimitsResponse;
use tokio::sync::Notify;

use crate::error::{BatchError, Result};

/// A query is reported when it leaves the node only if it used at least this much memory, or if
/// one of its operators reached its limit.
const REPORT_MIN_PEAK_BYTES: i64 = 64 << 20;

/// A wait for memory is reported in the logs each time it has lasted this much longer.
const LONG_WAIT_REPORT_INTERVAL: Duration = Duration::from_secs(30);

/// How long an operator that asked the spillable operators of its query to spill keeps waiting
/// for them, even when no other query can release memory.
const SPILL_REQUEST_GRACE: Duration = Duration::from_secs(60);

/// The memory limits of the queries of a node, derived from its batch memory limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryMemoryLimits {
    /// The batch memory of the node that queries may really use. `u64::MAX` when the node has no
    /// batch limit, in which case nothing below is enforced.
    pub usable: u64,
    /// What a single query may use on the node.
    pub query_limit: u64,
    /// What a single query may use when its spillable operators cannot all get their minimum
    /// within `query_limit`.
    pub query_max_limit: u64,
    /// The memory reserved for each spillable operator.
    pub operator_min: u64,
    /// The memory that a spillable operator adds to the pool of its query.
    pub operator_avg: u64,
}

impl QueryMemoryLimits {
    pub fn new(batch_mem_limit: u64, config: &BatchConfig) -> Self {
        let operator_min = (config.spillable_operator_min_memory_mb as u64) << 20;
        let operator_avg =
            ((config.spillable_operator_avg_memory_mb as u64) << 20).max(operator_min);
        if batch_mem_limit == u64::MAX {
            return Self {
                usable: u64::MAX,
                query_limit: u64::MAX,
                query_max_limit: u64::MAX,
                operator_min,
                operator_avg,
            };
        }
        let ratio = |r: f64| {
            if r.is_finite() {
                r.clamp(0.0, 1.0)
            } else {
                1.0
            }
        };
        let usable = batch_mem_limit as f64 * ratio(config.memory_usable_ratio);
        let query_limit = (usable * ratio(config.query_memory_ratio)) as u64;
        Self {
            usable: usable as u64,
            query_limit,
            query_max_limit: ((usable * ratio(config.query_memory_max_ratio)) as u64)
                .max(query_limit),
            operator_min,
            operator_avg,
        }
    }

    pub fn is_unlimited(&self) -> bool {
        self.usable == u64::MAX
    }

    /// The least memory that a query running `instances` spillable operators needs.
    pub fn min_memory(&self, instances: u32) -> u64 {
        self.operator_min.saturating_mul(instances as u64)
    }

    /// The limit of a query that runs `instances` spillable operators, all its operators
    /// included.
    ///
    /// The frontend lowers the parallelism of a query until the reserved memory of its spillable
    /// operators fits in `query_limit`. A query that still needs more without any parallelism is
    /// given `query_max_limit`.
    pub fn query_limit(&self, instances: u32) -> u64 {
        if self.is_unlimited() {
            u64::MAX
        } else if self.min_memory(instances) > self.query_limit {
            self.query_max_limit
        } else {
            self.query_limit
        }
    }

    /// The pool that the spillable operators of a query share, when it runs `instances` of them.
    ///
    /// Each operator adds `operator_avg` to the pool, within the limit of the query. With an
    /// unknown number of operators, the pool is the limit of the query.
    pub fn operator_pool(&self, instances: u32) -> u64 {
        let query_limit = self.query_limit(instances);
        if instances == 0 || self.is_unlimited() {
            return query_limit;
        }
        self.operator_avg
            .saturating_mul(instances as u64)
            .min(query_limit)
    }

    /// The part of the pool reserved for each of the `instances` spillable operators of a query.
    pub fn operator_reserved(&self, instances: u32) -> u64 {
        if instances == 0 || self.is_unlimited() {
            return 0;
        }
        self.operator_min
            .min(self.operator_pool(instances) / instances as u64)
    }

    /// The memory that a query reserves on the node before it starts: the pool of its spillable
    /// operators. Queries without such operators reserve nothing.
    pub fn reservation(&self, instances: u32) -> u64 {
        if instances == 0 || self.is_unlimited() {
            return 0;
        }
        self.operator_pool(instances)
    }

    pub fn to_protobuf(self) -> GetBatchLimitsResponse {
        if self.is_unlimited() {
            return GetBatchLimitsResponse::default();
        }
        GetBatchLimitsResponse {
            limited: true,
            query_memory_bytes: self.query_limit,
            query_memory_max_bytes: self.query_max_limit,
            spillable_operator_min_memory_bytes: self.operator_min,
        }
    }
}

#[derive(Default)]
struct QueryMemoryStats {
    /// Spillable operators created on this node, and those that the pool refused to grow.
    operators: AtomicU64,
    operators_at_limit: AtomicU64,
    /// The highest peak among the spillable operators.
    max_operator_peak: AtomicI64,
    /// The peak of the whole query, all operators included.
    query_peak: AtomicI64,
    /// Whether the query as a whole reached its limit.
    query_at_limit: AtomicBool,
    /// How long the query waited for its memory reservation.
    queued_ms: AtomicU64,
    /// The most memory that the operators of the query that cannot spill held outside the pool
    /// of the query, and how long they waited for it.
    overflow_peak: AtomicI64,
    overflow: AtomicI64,
    overflow_wait_ms: AtomicU64,
    /// How many times an operator that cannot spill asked the spillable operators to spill.
    spill_requests: AtomicU64,
}

/// Counts the memory of the whole query and records its peak.
struct QueryCounter {
    used: AtomicI64,
    stats: Arc<QueryMemoryStats>,
}

impl MemCounter for QueryCounter {
    fn add(&self, bytes: i64) {
        let used = self.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if bytes > 0 {
            self.stats.query_peak.fetch_max(used, Ordering::Relaxed);
        }
    }

    fn get_bytes_used(&self) -> i64 {
        self.used.load(Ordering::Relaxed)
    }

    fn on_limit_exceeded(&self) {
        self.stats.query_at_limit.store(true, Ordering::Relaxed);
    }
}

/// Counts what the spillable operators of a query take from the shared part of their pool.
struct SharedPoolCounter(AtomicI64);

impl MemCounter for SharedPoolCounter {
    fn add(&self, bytes: i64) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }

    fn get_bytes_used(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Counts the memory of one spillable operator and records its peak.
struct OperatorCounter {
    used: AtomicI64,
    peak: AtomicI64,
    at_limit: AtomicBool,
    stats: Arc<QueryMemoryStats>,
}

impl MemCounter for OperatorCounter {
    fn add(&self, bytes: i64) {
        let used = self.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if bytes > 0 {
            self.peak.fetch_max(used, Ordering::Relaxed);
        }
    }

    fn get_bytes_used(&self) -> i64 {
        self.used.load(Ordering::Relaxed)
    }

    fn on_limit_exceeded(&self) {
        if !self.at_limit.swap(true, Ordering::Relaxed) {
            self.stats
                .operators_at_limit
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for OperatorCounter {
    fn drop(&mut self) {
        self.stats
            .max_operator_peak
            .fetch_max(self.peak.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// The batch memory of the node, in MB, that queries take before they use it.
///
/// A query reserves the pool of its spillable operators before it starts: queries wait for it in
/// arrival order, and may only reserve the usable part of the memory. The operators that cannot
/// spill take what they need beyond the pool of their query while they run, up to the whole
/// memory, and are served before the queries that wait to start.
struct MemoryBudget {
    usable_mb: u64,
    total_mb: u64,
    /// How long an operator waits for memory beyond the pool of its query. `None`: no limit.
    overflow_wait_limit: Option<Duration>,
    state: Mutex<BudgetState>,
    notify: Notify,
}

#[derive(Default)]
struct BudgetState {
    taken_mb: u64,
    /// Tickets of the queries that wait to start, in arrival order.
    queue: VecDeque<u64>,
    next_ticket: u64,
    /// Operators that wait for memory beyond the pool of their query.
    overflow_waiters: usize,
    /// Queries that run tasks, and those among them with an operator that waits for memory.
    active_queries: usize,
    blocked_queries: usize,
    /// The admission numbers of the blocked queries. Queries are numbered when they are
    /// admitted: a higher number is a younger query.
    blocked_admissions: BTreeSet<u64>,
    next_admission: u64,
    /// Operators that wait while the spillable operators of their query may still release memory.
    spilling_waiters: usize,
    /// The blocked query chosen to fail because every active query was blocked, until its
    /// operators stop waiting.
    victim: Option<u64>,
}

/// Memory taken from a [`MemoryBudget`], given back on drop.
struct BudgetPermit {
    budget: Arc<MemoryBudget>,
    mb: u64,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        self.budget.release(self.mb);
    }
}

/// Removes a query from the queue when it stops waiting, served or not.
struct QueueTicket<'a> {
    budget: &'a MemoryBudget,
    ticket: u64,
}

impl Drop for QueueTicket<'_> {
    fn drop(&mut self) {
        self.budget.state.lock().queue.retain(|t| *t != self.ticket);
        self.budget.notify.notify_waiters();
    }
}

/// Marks an operator, and its query, as waiting for memory beyond the pool of the query.
struct OverflowWaiter<'a> {
    budget: &'a MemoryBudget,
    query: &'a QueryMemory,
    /// Whether the operator counts in `spilling_waiters`.
    spilling: bool,
}

impl Drop for OverflowWaiter<'_> {
    fn drop(&mut self) {
        {
            let mut state = self.budget.state.lock();
            state.overflow_waiters -= 1;
            if self.spilling {
                state.spilling_waiters -= 1;
            }
            if self.query.overflow_waiters.fetch_sub(1, Ordering::SeqCst) == 1 {
                state.blocked_queries -= 1;
                let admission = self.query.admission.load(Ordering::SeqCst);
                state.blocked_admissions.remove(&admission);
                if state.victim == Some(admission) {
                    state.victim = None;
                }
            }
        }
        self.budget.notify.notify_waiters();
    }
}

/// Why an operator gets no memory beyond the pool of its query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverflowFailure {
    /// No running query can release the memory, or the wait lasted too long.
    NoMemory,
    /// Every running query waited for memory: this query was chosen to fail so that the others
    /// can proceed.
    Victim,
}

impl MemoryBudget {
    fn new(usable_mb: u64, total_mb: u64, overflow_wait_limit: Option<Duration>) -> Self {
        Self {
            usable_mb,
            total_mb: total_mb.max(usable_mb),
            overflow_wait_limit,
            state: Mutex::new(BudgetState::default()),
            notify: Notify::new(),
        }
    }

    #[cfg(test)]
    fn available_mb(&self) -> u64 {
        self.usable_mb - self.state.lock().taken_mb
    }

    fn release(&self, mb: u64) {
        self.state.lock().taken_mb -= mb;
        self.notify.notify_waiters();
    }

    /// The state of the budget, for the logs of the waits that last.
    fn describe(&self) -> String {
        let state = self.state.lock();
        format!(
            "taken_mb={} usable_mb={} total_mb={} queued_queries={} overflow_waiters={} active_queries={} blocked_queries={} spilling_waiters={} victim={:?}",
            state.taken_mb,
            self.usable_mb,
            self.total_mb,
            state.queue.len(),
            state.overflow_waiters,
            state.active_queries,
            state.blocked_queries,
            state.spilling_waiters,
            state.victim
        )
    }

    /// A query starts running tasks. Returns its admission number: younger queries get higher
    /// numbers.
    fn activate(&self) -> u64 {
        let admission = {
            let mut state = self.state.lock();
            state.active_queries += 1;
            state.next_admission += 1;
            state.next_admission
        };
        // The operators that wait must check again whether another query can still release memory.
        self.notify.notify_waiters();
        admission
    }

    fn deactivate(&self) {
        self.state.lock().active_queries -= 1;
        self.notify.notify_waiters();
    }

    /// Reserves `mb` for a query that starts. Waits for its turn. Cancel safe.
    async fn reserve(self: &Arc<Self>, mb: u64) -> BudgetPermit {
        let mb = mb.min(self.usable_mb);
        let ticket = {
            let mut state = self.state.lock();
            let ticket = state.next_ticket;
            state.next_ticket += 1;
            state.queue.push_back(ticket);
            ticket
        };
        let _ticket = QueueTicket {
            budget: self,
            ticket,
        };
        let start = Instant::now();
        let mut next_report = LONG_WAIT_REPORT_INTERVAL;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock();
                if state.queue.front() == Some(&ticket)
                    && state.overflow_waiters == 0
                    && state.taken_mb + mb <= self.usable_mb
                {
                    state.taken_mb += mb;
                    return BudgetPermit {
                        budget: self.clone(),
                        mb,
                    };
                }
            }
            let _ = tokio::time::timeout(LONG_WAIT_REPORT_INTERVAL, notified).await;
            if start.elapsed() >= next_report {
                next_report = start.elapsed() + LONG_WAIT_REPORT_INTERVAL;
                tracing::warn!(
                    waited_secs = start.elapsed().as_secs(),
                    reservation_mb = mb,
                    budget = self.describe(),
                    "batch query still waits for its memory reservation"
                );
            }
        }
    }

    /// Takes `mb` for an operator of a running query, beyond the pool of the query.
    ///
    /// `try_pool` is tried first, each time memory may have been released. Then the memory of
    /// the node that nobody took. When there is none, `request_spill` is called once, to ask the
    /// spillable operators of the query to release memory into its pool, and the operator waits
    /// for them or for another query to release some. It fails when no other query runs that is
    /// not waiting itself (after a grace period when its own spillable operators were asked to
    /// spill), or when the wait lasts longer than the limit of the budget: a query that runs may
    /// hold its memory for hours, or wait for something that never comes. Cancel safe.
    ///
    /// When every active query waits, and none of them may still get memory from its own
    /// spillable operators, they would all fail. Instead, one of them is chosen to fail: the
    /// youngest, the one admitted last. Its operators fail with [`OverflowFailure::Victim`], the
    /// others keep waiting for the memory that it releases when it ends, and a new victim is
    /// chosen if they are still all blocked then. The youngest query has done the least work, and
    /// the oldest query is never chosen while another one is blocked, so every query eventually
    /// gets to run alone. A query alone on the node fails as before: no other query can release
    /// memory.
    async fn take_overflow(
        self: &Arc<Self>,
        mb: u64,
        query: &QueryMemory,
        operator: &str,
        mut try_pool: impl FnMut() -> bool,
        mut request_spill: impl FnMut() -> Option<SpillRequest>,
    ) -> std::result::Result<Option<BudgetPermit>, OverflowFailure> {
        let mut waiter: Option<OverflowWaiter<'_>> = None;
        let mut spill_request: Option<SpillRequest> = None;
        let start = Instant::now();
        let mut next_report = LONG_WAIT_REPORT_INTERVAL;
        let admission = query.admission.load(Ordering::SeqCst);
        loop {
            if start.elapsed() >= next_report {
                next_report = start.elapsed() + LONG_WAIT_REPORT_INTERVAL;
                tracing::warn!(
                    operator,
                    waited_secs = start.elapsed().as_secs(),
                    needed_mb = mb,
                    spill_requested = spill_request.is_some(),
                    budget = self.describe(),
                    "batch operator still waits for memory beyond the pool of its query"
                );
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            // The query was chosen to fail: its other operators fail at once.
            if query.memory_victim.load(Ordering::SeqCst) {
                return Err(OverflowFailure::Victim);
            }
            if try_pool() {
                return Ok(None);
            }
            {
                let mut state = self.state.lock();
                if state.taken_mb + mb <= self.total_mb {
                    state.taken_mb += mb;
                    return Ok(Some(BudgetPermit {
                        budget: self.clone(),
                        mb,
                    }));
                }
                if waiter.is_none() {
                    state.overflow_waiters += 1;
                    if query.overflow_waiters.fetch_add(1, Ordering::SeqCst) == 0 {
                        state.blocked_queries += 1;
                        state.blocked_admissions.insert(admission);
                    }
                    waiter = Some(OverflowWaiter {
                        budget: self,
                        query,
                        spilling: false,
                    });
                }
            }
            if spill_request.is_none() {
                spill_request = request_spill();
            }
            // The spillable operators of the query may still release memory into its pool.
            let spilling = spill_request
                .as_ref()
                .is_some_and(|r| r.revocable && start.elapsed() < SPILL_REQUEST_GRACE);
            let mut victim_chosen = false;
            let failure = {
                let mut state = self.state.lock();
                let waiter = waiter.as_mut().expect("registered above");
                if waiter.spilling != spilling {
                    if spilling {
                        state.spilling_waiters += 1;
                    } else {
                        state.spilling_waiters -= 1;
                    }
                    waiter.spilling = spilling;
                }
                // Every active query waits, and no waiting operator may still get memory from the
                // spillable operators of its query.
                let stuck = state.active_queries <= state.blocked_queries
                    && state.spilling_waiters == 0
                    && state.victim.is_none();
                if state.victim == Some(admission) {
                    Some(OverflowFailure::Victim)
                } else if !stuck {
                    None
                } else if state.blocked_queries <= 1 {
                    // Alone on the node: no other query can release memory.
                    Some(OverflowFailure::NoMemory)
                } else {
                    let youngest = *state
                        .blocked_admissions
                        .last()
                        .expect("a blocked query is registered");
                    state.victim = Some(youngest);
                    victim_chosen = true;
                    tracing::warn!(
                        operator,
                        victim_admission = youngest,
                        own_admission = admission,
                        "every running batch query waits for memory: the youngest one is chosen to fail"
                    );
                    (youngest == admission).then_some(OverflowFailure::Victim)
                }
            };
            match failure {
                Some(OverflowFailure::Victim) => {
                    query.memory_victim.store(true, Ordering::SeqCst);
                    tracing::warn!(
                        operator,
                        waited_secs = start.elapsed().as_secs(),
                        needed_mb = mb,
                        spill_requested = spill_request.is_some(),
                        budget = self.describe(),
                        "batch operator fails: its query was chosen to fail so that the other queries waiting for memory can proceed"
                    );
                    return Err(OverflowFailure::Victim);
                }
                Some(OverflowFailure::NoMemory) => {
                    tracing::warn!(
                        operator,
                        waited_secs = start.elapsed().as_secs(),
                        needed_mb = mb,
                        spill_requested = spill_request.is_some(),
                        budget = self.describe(),
                        "batch operator fails: no running query can release the memory that it needs"
                    );
                    return Err(OverflowFailure::NoMemory);
                }
                None => {}
            }
            if victim_chosen {
                // Wake the operators of the victim.
                self.notify.notify_waiters();
            }
            if let Some(limit) = self.overflow_wait_limit
                && start.elapsed() >= limit
            {
                tracing::warn!(
                    operator,
                    waited_secs = start.elapsed().as_secs(),
                    needed_mb = mb,
                    spill_requested = spill_request.is_some(),
                    budget = self.describe(),
                    "batch operator gives up waiting for memory beyond the pool of its query"
                );
                return Err(OverflowFailure::NoMemory);
            }
            // The pool of the query is not watched: check it again from time to time.
            let _ = tokio::time::timeout(Duration::from_secs(1), notified).await;
        }
    }
}

/// The reservation of a query that runs tasks on the node.
struct ActiveQuery {
    budget: Arc<MemoryBudget>,
    _permit: Option<BudgetPermit>,
}

impl Drop for ActiveQuery {
    fn drop(&mut self) {
        self.budget.deactivate();
    }
}

/// The memory of one query on this node, shared by all its tasks.
pub struct QueryMemory {
    query_id: String,
    context: MemoryContext,
    /// The part of the pool that is not reserved, charged with what the spillable operators use
    /// beyond their reservation.
    shared_pool: MemoryContext,
    /// Between the spillable operators and `shared_pool`: what they use beyond their reservation.
    /// Its limit is lowered while an operator that cannot spill asks them to spill.
    spill_gate: MemoryContext,
    /// `batch_spill_on_memory_wait`: an operator that cannot spill and waits for memory asks the
    /// spillable operators of the query to spill.
    spill_on_memory_wait: AtomicBool,
    /// The operators that currently ask the spillable operators to spill.
    spill_requests: Mutex<usize>,
    query_limit: u64,
    operator_pool: u64,
    operator_reserved: u64,
    spillable_operator_instances: u32,
    /// The memory, in MB, that the query reserves on the node before it starts.
    reservation_mb: u32,
    /// The reservation, shared by all the tasks of the query that are running.
    reservation: tokio::sync::Mutex<Option<ActiveQuery>>,
    /// The operators of the query that wait for memory beyond its pool.
    overflow_waiters: AtomicUsize,
    /// The number given to the query when it was admitted (see [`MemoryBudget::activate`]).
    admission: AtomicU64,
    /// Whether the query was chosen to fail because every running query waited for memory.
    memory_victim: AtomicBool,
    /// The tasks of the query that are running on the node.
    running_tasks: AtomicUsize,
    stats: Arc<QueryMemoryStats>,
    registry: Weak<QueryMemoryRegistryInner>,
}

impl QueryMemory {
    /// The memory context of the query: for the operators that cannot spill.
    pub fn context(&self) -> MemoryContext {
        self.context.clone()
    }

    /// A memory context for a spillable operator: its reserved memory is always granted, the rest
    /// comes from the pool that the spillable operators of the query share.
    pub fn spillable_operator_context(&self) -> MemoryContext {
        self.stats.operators.fetch_add(1, Ordering::Relaxed);
        let counter = OperatorCounter {
            used: AtomicI64::new(0),
            peak: AtomicI64::new(0),
            at_limit: AtomicBool::new(false),
            stats: self.stats.clone(),
        };
        MemoryContext::new_with_reservation(
            self.spill_gate.clone(),
            self.context.clone(),
            self.operator_reserved,
            counter,
        )
    }

    /// The memory of an operator that cannot spill, counted in `meter`.
    pub fn executor_memory(self: &Arc<Self>, meter: MemoryContext) -> ExecutorMemory {
        let Some(registry) = self.registry.upgrade() else {
            return ExecutorMemory::unmanaged(meter);
        };
        if registry.limits.is_unlimited() {
            return ExecutorMemory::unmanaged(meter);
        }
        ExecutorMemory {
            meter,
            managed: Some(ManagedMemory {
                operator: String::new(),
                query: self.clone(),
                budget: registry.budget.clone(),
                settled: 0,
                pool: MemoryContext::new(
                    Some(self.shared_pool.clone()),
                    PlainCounter(AtomicI64::new(0)),
                ),
                overflow: MemoryContext::new(
                    Some(registry.root.clone()),
                    PlainCounter(AtomicI64::new(0)),
                ),
                overflow_bytes: 0,
                overflow_permits: Vec::new(),
            }),
        }
    }

    pub fn operator_pool(&self) -> u64 {
        self.operator_pool
    }

    pub fn operator_reserved(&self) -> u64 {
        self.operator_reserved
    }

    pub fn query_limit(&self) -> u64 {
        self.query_limit
    }

    /// `batch_spill_on_memory_wait`: from now on, an operator of the query that cannot spill and
    /// waits for memory asks the spillable operators of the query to spill.
    pub fn enable_spill_on_memory_wait(&self) {
        self.spill_on_memory_wait.store(true, Ordering::Relaxed);
    }

    fn spills_on_memory_wait(&self) -> bool {
        self.spill_on_memory_wait.load(Ordering::Relaxed)
    }

    /// Asks the spillable operators of the query to release `needed` bytes into the pool: what
    /// they may use beyond their reservations is capped below what they use now, so that the next
    /// of them that needs more spills to disk and gives its memory back. The cap is lifted when
    /// the last request ends.
    fn request_spill(self: &Arc<Self>, needed: i64, operator: &str) -> SpillRequest {
        let held = self.spill_gate.get_bytes_used();
        {
            let mut requests = self.spill_requests.lock();
            *requests += 1;
            let cap = (held - needed).max(0) as u64;
            if cap < self.spill_gate.mem_limit() {
                self.spill_gate.set_mem_limit(cap);
            }
        }
        self.stats.spill_requests.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            query_id = self.query_id,
            operator,
            needed_mb = needed >> 20,
            spillable_held_mb = held >> 20,
            "batch operator waits for memory: the spillable operators of its query are asked to spill"
        );
        SpillRequest {
            query: self.clone(),
            revocable: held > 0,
        }
    }

    /// Waits until the memory that the query reserves is available on the node. Queries are
    /// served in arrival order. The tasks of a query share one reservation, held for as long as
    /// one of them holds the returned guard.
    ///
    /// Cancel safe: when the task that waits is cancelled, another task of the query takes over.
    pub async fn admit(self: &Arc<Self>) -> AdmissionGuard {
        let admitted = || AdmissionGuard {
            query: Some(self.clone()),
        };
        let Some(registry) = self.registry.upgrade() else {
            return AdmissionGuard { query: None };
        };
        if registry.limits.is_unlimited() {
            return AdmissionGuard { query: None };
        }
        let mut reservation = self.reservation.lock().await;
        if reservation.is_none() {
            let start = Instant::now();
            let permit = if self.reservation_mb == 0 {
                None
            } else {
                Some(registry.budget.reserve(self.reservation_mb as u64).await)
            };
            let admission = registry.budget.activate();
            self.admission.store(admission, Ordering::SeqCst);
            *reservation = Some(ActiveQuery {
                budget: registry.budget.clone(),
                _permit: permit,
            });
            let queued_ms = start.elapsed().as_millis() as u64;
            self.stats.queued_ms.fetch_add(queued_ms, Ordering::Relaxed);
            if queued_ms >= 1000 {
                tracing::info!(
                    query_id = self.query_id,
                    queued_ms,
                    reservation_mb = self.reservation_mb,
                    "batch query waited for its memory reservation"
                );
            }
        }
        // Counted while the reservation is locked, so that it cannot be released in between.
        self.running_tasks.fetch_add(1, Ordering::SeqCst);
        admitted()
    }
}

/// A request of an operator that cannot spill to the spillable operators of its query. They
/// may grow again once the last request of the query is dropped.
struct SpillRequest {
    query: Arc<QueryMemory>,
    /// Whether the spillable operators held memory beyond their reservations when asked: if not,
    /// there is nothing to wait for.
    revocable: bool,
}

impl Drop for SpillRequest {
    fn drop(&mut self) {
        let mut requests = self.query.spill_requests.lock();
        *requests -= 1;
        if *requests == 0 {
            self.query.spill_gate.set_mem_limit(u64::MAX);
        }
    }
}

/// The memory of an operator that cannot spill to disk.
///
/// The operator counts what it uses in [`meter`](Self::meter), which never refuses, and calls
/// [`settle`](Self::settle) each time it has taken a chunk in: the memory is then taken from the
/// pool of the query, and beyond it from the memory of the node that no query reserved. When
/// there is none left, the operator waits for another query to release some.
pub struct ExecutorMemory {
    meter: MemoryContext,
    managed: Option<ManagedMemory>,
}

struct ManagedMemory {
    /// The identity of the operator, for the errors and the logs.
    operator: String,
    query: Arc<QueryMemory>,
    budget: Arc<MemoryBudget>,
    /// What was settled so far, and where it was taken from.
    settled: i64,
    pool: MemoryContext,
    overflow: MemoryContext,
    overflow_bytes: i64,
    overflow_permits: Vec<BudgetPermit>,
}

struct PlainCounter(AtomicI64);

impl MemCounter for PlainCounter {
    fn add(&self, bytes: i64) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }

    fn get_bytes_used(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl From<MemoryContext> for ExecutorMemory {
    fn from(mem_context: MemoryContext) -> Self {
        Self::unmanaged(mem_context)
    }
}

impl ExecutorMemory {
    /// The operator counts its memory in `mem_context`, whose limits are the only ones applied.
    pub fn unmanaged(mem_context: MemoryContext) -> Self {
        Self {
            meter: mem_context,
            managed: None,
        }
    }

    /// Names the operator in the errors and logs about its memory.
    pub fn with_identity(mut self, identity: &str) -> Self {
        if let Some(managed) = &mut self.managed {
            managed.operator = identity.to_owned();
        }
        self
    }

    /// Whether the memory is bounded by the node: when it is not, [`settle`](Self::settle) never
    /// waits nor fails.
    pub fn is_managed(&self) -> bool {
        self.managed.is_some()
    }

    /// The context in which the operator counts the memory that it uses.
    pub fn meter(&self) -> MemoryContext {
        self.meter.clone()
    }

    /// Takes the memory that the operator counted since the last call, or gives back what it
    /// released.
    pub async fn settle(&mut self) -> Result<()> {
        let Some(managed) = &mut self.managed else {
            return Ok(());
        };
        let diff = self.meter.get_bytes_used() - managed.settled;
        if diff > 0 {
            managed.grow(diff).await?;
        } else if diff < 0 {
            managed.shrink(-diff);
        }
        managed.settled += diff;
        Ok(())
    }
}

impl ManagedMemory {
    async fn grow(&mut self, bytes: i64) -> Result<()> {
        // Once the operator went beyond the pool, it stays there until it has given it all back,
        // so that what is released is always the memory taken last. Except when it may ask the
        // spillable operators of its query to spill: the memory that they release goes to the
        // pool, where it must be able to take it.
        let spill_on_wait = self.query.spills_on_memory_wait();
        let from_pool = self.overflow_bytes == 0 || spill_on_wait;
        if from_pool && self.pool.add(bytes) {
            return Ok(());
        }
        const MB: i64 = 1 << 20;
        let held_mb: u64 = self.overflow_permits.iter().map(|p| p.mb).sum();
        let needed_mb = ((self.overflow_bytes + bytes + MB - 1) / MB) as u64;
        if needed_mb > held_mb {
            let start = Instant::now();
            let pool = &self.pool;
            let query = &self.query;
            let operator = self.operator.as_str();
            let taken = self
                .budget
                .take_overflow(
                    needed_mb - held_mb,
                    query,
                    operator_name(operator),
                    || from_pool && pool.add(bytes),
                    || spill_on_wait.then(|| query.request_spill(bytes, operator_name(operator))),
                )
                .await;
            let stats = &self.query.stats;
            stats
                .overflow_wait_ms
                .fetch_add(start.elapsed().as_millis() as u64, Ordering::Relaxed);
            match taken {
                Ok(None) => return Ok(()),
                Ok(Some(permit)) => self.overflow_permits.push(permit),
                Err(failure) => {
                    let operator = operator_name(&self.operator).to_owned();
                    let mb = ((self.settled + bytes) >> 20) as u64;
                    return Err(match failure {
                        OverflowFailure::NoMemory => BatchError::NoMemoryLeft(operator, mb),
                        OverflowFailure::Victim => BatchError::MemoryWaitVictim(operator, mb),
                    });
                }
            }
        }
        // Counted for the node only: the limit of the query does not apply. The permit guarantees
        // the room in the budget; counted whatever the context says, as its release is.
        self.overflow.force_add(bytes);
        self.overflow_bytes += bytes;
        let stats = &self.query.stats;
        let overflow = stats.overflow.fetch_add(bytes, Ordering::Relaxed) + bytes;
        stats.overflow_peak.fetch_max(overflow, Ordering::Relaxed);
        Ok(())
    }

    fn shrink(&mut self, bytes: i64) {
        let from_overflow = bytes.min(self.overflow_bytes);
        if from_overflow > 0 {
            self.overflow.add(-from_overflow);
            self.overflow_bytes -= from_overflow;
            self.query
                .stats
                .overflow
                .fetch_sub(from_overflow, Ordering::Relaxed);
            if self.overflow_bytes == 0 {
                self.overflow_permits.clear();
            }
        }
        if bytes > from_overflow {
            self.pool.add(-(bytes - from_overflow));
        }
    }
}

/// The name of an operator in the errors and logs, when it was not given.
fn operator_name(operator: &str) -> &str {
    if operator.is_empty() {
        "unknown operator"
    } else {
        operator
    }
}

impl Drop for ManagedMemory {
    fn drop(&mut self) {
        // The contexts give back what they hold when they are dropped.
        self.query
            .stats
            .overflow
            .fetch_sub(self.overflow_bytes, Ordering::Relaxed);
    }
}

/// Held by a task of a query while it runs. The memory reservation of the query is released
/// when its last running task ends.
pub struct AdmissionGuard {
    /// `None` when the query reserves nothing.
    query: Option<Arc<QueryMemory>>,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let Some(query) = &self.query else {
            return;
        };
        if query.running_tasks.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        // When the lock is taken, a task of the query is being admitted: it keeps the reservation.
        if let Ok(mut reservation) = query.reservation.try_lock()
            && query.running_tasks.load(Ordering::SeqCst) == 0
        {
            *reservation = None;
        }
    }
}

impl Drop for QueryMemory {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut queries = registry.queries.lock();
            // A new task of the query may have registered a new entry in the meantime.
            if queries
                .get(&self.query_id)
                .is_some_and(|q| q.strong_count() == 0)
            {
                queries.remove(&self.query_id);
            }
        }

        let stats = &self.stats;
        let query_peak = stats.query_peak.load(Ordering::Relaxed);
        let operators_at_limit = stats.operators_at_limit.load(Ordering::Relaxed);
        let query_at_limit = stats.query_at_limit.load(Ordering::Relaxed);
        let queued_ms = stats.queued_ms.load(Ordering::Relaxed);
        let overflow_peak = stats.overflow_peak.load(Ordering::Relaxed);
        if query_peak < REPORT_MIN_PEAK_BYTES
            && operators_at_limit == 0
            && !query_at_limit
            && queued_ms < 1000
            && overflow_peak == 0
            && stats.spill_requests.load(Ordering::Relaxed) == 0
            && !self.memory_victim.load(Ordering::Relaxed)
        {
            return;
        }
        let mb = |bytes: u64| {
            if bytes == u64::MAX {
                "unlimited".to_owned()
            } else {
                (bytes >> 20).to_string()
            }
        };
        tracing::info!(
            query_id = self.query_id,
            query_peak_mb = query_peak >> 20,
            query_limit_mb = %mb(self.query_limit),
            query_at_limit,
            max_operator_peak_mb = stats.max_operator_peak.load(Ordering::Relaxed) >> 20,
            operator_pool_mb = %mb(self.operator_pool),
            operator_reserved_mb = self.operator_reserved >> 20,
            operators = stats.operators.load(Ordering::Relaxed),
            operators_at_limit,
            spillable_operator_instances = self.spillable_operator_instances,
            reservation_mb = self.reservation_mb,
            queued_ms,
            overflow_peak_mb = overflow_peak >> 20,
            overflow_wait_ms = stats.overflow_wait_ms.load(Ordering::Relaxed),
            spill_requests = stats.spill_requests.load(Ordering::Relaxed),
            memory_victim = self.memory_victim.load(Ordering::Relaxed),
            "batch query memory"
        );
    }
}

struct QueryMemoryRegistryInner {
    root: MemoryContext,
    limits: QueryMemoryLimits,
    budget: Arc<MemoryBudget>,
    queries: Mutex<HashMap<String, Weak<QueryMemory>>>,
}

/// The memory of the queries that currently run tasks on this node.
#[derive(Clone)]
pub struct QueryMemoryRegistry {
    inner: Arc<QueryMemoryRegistryInner>,
}

impl QueryMemoryRegistry {
    pub fn new(root: MemoryContext, config: &BatchConfig) -> Self {
        let limits = QueryMemoryLimits::new(root.mem_limit(), config);
        tracing::info!(
            batch_mem_limit = root.mem_limit(),
            usable = limits.usable,
            query_limit = limits.query_limit,
            query_max_limit = limits.query_max_limit,
            operator_min = limits.operator_min,
            operator_avg = limits.operator_avg,
            "batch query memory limits"
        );
        let budget = if limits.is_unlimited() {
            MemoryBudget::new(0, 0, None)
        } else {
            MemoryBudget::new(
                limits.usable >> 20,
                root.mem_limit() >> 20,
                (config.overflow_memory_wait_secs > 0)
                    .then(|| Duration::from_secs(config.overflow_memory_wait_secs)),
            )
        };
        Self {
            inner: Arc::new(QueryMemoryRegistryInner {
                root,
                limits,
                budget: Arc::new(budget),
                queries: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn limits(&self) -> QueryMemoryLimits {
        self.inner.limits
    }

    /// The memory of the given query, created on the first task of the query on this node.
    pub fn get_or_create(
        &self,
        query_id: &str,
        spillable_operator_instances: u32,
    ) -> Arc<QueryMemory> {
        let mut queries = self.inner.queries.lock();
        if let Some(existing) = queries.get(query_id).and_then(Weak::upgrade) {
            return existing;
        }
        let limits = self.inner.limits;
        let stats = Arc::new(QueryMemoryStats::default());
        let counter = QueryCounter {
            used: AtomicI64::new(0),
            stats: stats.clone(),
        };
        let context = MemoryContext::new_with_mem_limit(
            Some(self.inner.root.clone()),
            counter,
            limits.query_limit(spillable_operator_instances),
        );
        let operator_pool = limits.operator_pool(spillable_operator_instances);
        let operator_reserved = limits.operator_reserved(spillable_operator_instances);
        let shared_pool = MemoryContext::new_with_mem_limit(
            Some(context.clone()),
            SharedPoolCounter(AtomicI64::new(0)),
            operator_pool.saturating_sub(
                operator_reserved.saturating_mul(spillable_operator_instances as u64),
            ),
        );
        let spill_gate = MemoryContext::new_with_mem_limit(
            Some(shared_pool.clone()),
            PlainCounter(AtomicI64::new(0)),
            u64::MAX,
        );
        let query = Arc::new(QueryMemory {
            query_id: query_id.to_owned(),
            context,
            shared_pool,
            spill_gate,
            spill_on_memory_wait: AtomicBool::new(false),
            spill_requests: Mutex::new(0),
            query_limit: limits.query_limit(spillable_operator_instances),
            operator_pool,
            operator_reserved,
            spillable_operator_instances,
            // Never more than the whole semaphore, so that the query is eventually served.
            reservation_mb: (limits.reservation(spillable_operator_instances) >> 20)
                .min(self.inner.budget.usable_mb)
                .min(u32::MAX as u64) as u32,
            reservation: tokio::sync::Mutex::new(None),
            overflow_waiters: AtomicUsize::new(0),
            admission: AtomicU64::new(0),
            memory_victim: AtomicBool::new(false),
            running_tasks: AtomicUsize::new(0),
            stats,
            registry: Arc::downgrade(&self.inner),
        });
        queries.insert(query_id.to_owned(), Arc::downgrade(&query));
        query
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.queries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Atomic;
    use risingwave_common::metrics::TrAdderAtomic;

    use super::*;

    const MB: u64 = 1 << 20;

    fn registry(batch_mem_limit: u64) -> (QueryMemoryRegistry, MemoryContext) {
        let root = MemoryContext::root(TrAdderAtomic::new(0), batch_mem_limit);
        let config = BatchConfig {
            memory_usable_ratio: 0.9,
            query_memory_ratio: 0.25,
            query_memory_max_ratio: 0.5,
            spillable_operator_min_memory_mb: 16,
            spillable_operator_avg_memory_mb: 64,
            ..Default::default()
        };
        (QueryMemoryRegistry::new(root.clone(), &config), root)
    }

    #[test]
    fn test_limits() {
        // Usable: 3600 MB. Normal share: 900 MB. Largest share: 1800 MB.
        let (registry, _root) = registry(4000 * MB);
        let limits = registry.limits();
        assert_eq!(limits.query_limit, 900 * MB);
        assert_eq!(limits.query_max_limit, 1800 * MB);
        assert_eq!(limits.operator_min, 16 * MB);
        assert_eq!(limits.operator_avg, 64 * MB);

        // Each operator adds the average to the pool, which the query reserves on the node.
        assert_eq!(limits.query_limit(4), 900 * MB);
        assert_eq!(limits.operator_pool(4), 256 * MB);
        assert_eq!(limits.operator_reserved(4), 16 * MB);
        assert_eq!(limits.reservation(4), 256 * MB);
        // The pool never exceeds the share of the query.
        assert_eq!(limits.operator_pool(40), 900 * MB);
        assert_eq!(limits.reservation(40), 900 * MB);
        // The reserved memory does not fit in the normal share: the query gets the largest one.
        assert_eq!(limits.query_limit(60), 1800 * MB);
        assert_eq!(limits.operator_pool(60), 1800 * MB);
        assert_eq!(limits.operator_reserved(60), 16 * MB);
        assert_eq!(limits.reservation(60), 1800 * MB);
        // More operators than the frontend should have accepted: what is reserved shrinks.
        assert_eq!(limits.operator_reserved(200), 9 * MB);
        // Unknown number of operators: the pool is the share of the query, nothing is reserved,
        // neither for the operators nor on the node.
        assert_eq!(limits.operator_pool(0), 900 * MB);
        assert_eq!(limits.operator_reserved(0), 0);
        assert_eq!(limits.reservation(0), 0);
    }

    #[test]
    fn test_no_batch_limit() {
        let (registry, _root) = registry(u64::MAX);
        let query = registry.get_or_create("q", 4);
        assert_eq!(query.operator_pool(), u64::MAX);
        assert!(query.spillable_operator_context().add(1 << 40));
    }

    #[test]
    fn test_operators_share_the_pool() {
        // 4 operators: a pool of 256 MB, of which 4 x 16 MB are reserved and 192 MB shared.
        let (registry, root) = registry(4000 * MB);
        let query = registry.get_or_create("q", 4);
        assert_eq!(query.operator_pool(), 256 * MB);
        assert_eq!(query.operator_reserved(), 16 * MB);

        let op1 = query.spillable_operator_context();
        let op2 = query.spillable_operator_context();
        // The first operator takes its reservation and the whole shared part.
        assert!(op1.add(208 * MB as i64));
        assert!(!op1.add(1));
        assert_eq!(op1.get_bytes_used(), 208 * MB as i64);
        // The second one still has what is reserved for it, and nothing more.
        assert!(op2.add(16 * MB as i64));
        assert!(!op2.add(1));
        assert_eq!(op1.parent_bytes_used_by_others(), 0);
        assert_eq!(op2.parent_bytes_used_by_others(), 192 * MB as i64);

        // What the first one gives back can be taken by the second one.
        assert!(op1.add(-(100 * MB as i64)));
        assert!(op2.add(100 * MB as i64));
        assert!(!op2.add(1));
        assert_eq!(query.context().get_bytes_used(), 224 * MB as i64);

        // An operator that cannot spill is only bounded by the query, not by the pool.
        let other = MemoryContext::new(Some(query.context()), TrAdderAtomic::new(0));
        assert!(other.add(676 * MB as i64));
        assert!(!other.add(1));
        assert_eq!(root.get_bytes_used(), 900 * MB as i64);

        let stats = query.stats.clone();
        assert_eq!(stats.operators.load(Ordering::Relaxed), 2);
        assert_eq!(stats.operators_at_limit.load(Ordering::Relaxed), 2);
        assert_eq!(stats.query_peak.load(Ordering::Relaxed), 900 * MB as i64);
        assert!(stats.query_at_limit.load(Ordering::Relaxed));

        // An operator that ends gives everything back.
        drop(op1);
        assert_eq!(op2.parent_bytes_used_by_others(), 0);
        assert!(other.add(-(676 * MB as i64)));
        assert!(op2.add(92 * MB as i64));
        assert!(!op2.add(1));
        drop((op2, other));
        assert_eq!(
            stats.max_operator_peak.load(Ordering::Relaxed),
            208 * MB as i64
        );
        assert_eq!(query.context().get_bytes_used(), 0);
        assert_eq!(root.get_bytes_used(), 0);
    }

    #[test]
    fn test_tasks_share_the_query() {
        let (registry, root) = registry(4000 * MB);
        let task1 = registry.get_or_create("q1", 3);
        let task2 = registry.get_or_create("q1", 3);
        let other = registry.get_or_create("q2", 3);
        assert!(Arc::ptr_eq(&task1, &task2));
        assert!(!Arc::ptr_eq(&task1, &other));
        assert_eq!(registry.len(), 2);

        // Two tasks of a query share its limit, another query has its own.
        assert!(task1.spillable_operator_context().add(1));
        assert!(task1.context().add(900 * MB as i64));
        assert!(!task2.context().add(1));
        assert!(other.context().add(900 * MB as i64));

        drop((task1, task2));
        assert_eq!(registry.len(), 1);
        drop(other);
        assert_eq!(registry.len(), 0);
        assert_eq!(root.get_bytes_used(), 0);
    }

    #[tokio::test]
    async fn test_admission_queue() {
        // Usable: 3600 MB. The reserved memory of the 60 operators of each of these queries does
        // not fit in the normal share: each query reserves the largest share, 1800 MB.
        let (registry, _root) = registry(4000 * MB);
        let q1 = registry.get_or_create("q1", 60);
        let q2 = registry.get_or_create("q2", 60);
        let q3 = registry.get_or_create("q3", 60);
        let light = registry.get_or_create("light", 0);

        let g1 = q1.admit().await;
        // A second task of the same query does not reserve again.
        let g1_bis = registry.get_or_create("q1", 60).admit().await;
        let g2 = q2.admit().await;
        assert_eq!(registry.inner.budget.available_mb(), 3600 - 2 * 1800);

        // The third query does not fit: it waits.
        let waiter = {
            let q3 = q3.clone();
            tokio::spawn(async move { q3.admit().await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        // A query that reserves nothing is never queued.
        let _light = light.admit().await;

        // The reservation of a query is kept while one of its tasks runs, and released when the
        // last one ends, even though the query itself is still known to the node.
        drop(g1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        drop(g1_bis);
        let g3 = waiter.await.unwrap();
        assert_eq!(registry.inner.budget.available_mb(), 3600 - 2 * 1800);
        assert!(q3.stats.queued_ms.load(Ordering::Relaxed) >= 50);

        // A later task of a query whose reservation was released reserves again.
        drop(g2);
        let g1 = q1.admit().await;
        assert_eq!(registry.inner.budget.available_mb(), 3600 - 2 * 1800);

        drop((g1, g3));
        assert_eq!(registry.inner.budget.available_mb(), 3600);
    }
    fn meter() -> MemoryContext {
        MemoryContext::root(TrAdderAtomic::new(0), u64::MAX)
    }

    #[tokio::test]
    async fn test_executor_memory_overflows_the_pool() {
        // Batch memory: 4000 MB, of which 3600 MB can be reserved by queries.
        // 4 operators: a pool of 256 MB, of which 192 MB are shared.
        let (registry, root) = registry(4000 * MB);
        let query = registry.get_or_create("q", 4);
        let _guard = query.admit().await;
        let budget = registry.inner.budget.clone();
        assert_eq!(budget.available_mb(), 3600 - 256);

        let mut memory = query.executor_memory(meter());
        let meter = memory.meter();
        // Taken from the pool of the query first.
        meter.add(100 * MB as i64);
        memory.settle().await.unwrap();
        assert_eq!(query.context().get_bytes_used(), 100 * MB as i64);
        assert_eq!(budget.available_mb(), 3600 - 256);
        // Then from the memory that no query reserved, beyond the limit of the query.
        meter.add(3000 * MB as i64);
        memory.settle().await.unwrap();
        assert_eq!(query.context().get_bytes_used(), 100 * MB as i64);
        assert_eq!(root.get_bytes_used(), 3100 * MB as i64);
        assert_eq!(budget.available_mb(), 3600 - 256 - 3000);
        // Up to the whole batch memory, never beyond: no other query can release any.
        meter.add(800 * MB as i64);
        let error = memory.settle().await.unwrap_err();
        assert!(matches!(error, BatchError::NoMemoryLeft(..)), "{error:?}");
        meter.add(-(800 * MB as i64));
        meter.add(744 * MB as i64);
        memory.settle().await.unwrap();
        assert_eq!(root.get_bytes_used(), 3844 * MB as i64);

        // What is released goes back to the node first, then to the pool.
        meter.add(-(3744 * MB as i64));
        memory.settle().await.unwrap();
        assert_eq!(budget.available_mb(), 3600 - 256);
        assert_eq!(root.get_bytes_used(), 100 * MB as i64);
        meter.add(50 * MB as i64);
        memory.settle().await.unwrap();
        meter.add(4000 * MB as i64);
        assert!(memory.settle().await.is_err());
        drop(memory);
        assert_eq!(root.get_bytes_used(), 0);
        assert_eq!(budget.available_mb(), 3600 - 256);
        assert_eq!(
            query.stats.overflow_peak.load(Ordering::Relaxed),
            3744 * MB as i64
        );
        assert_eq!(query.stats.overflow.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_executor_memory_waits_for_other_queries() {
        let (registry, root) = registry(4000 * MB);
        let budget = registry.inner.budget.clone();
        let q1 = registry.get_or_create("q1", 4);
        let q2 = registry.get_or_create("q2", 4);
        let q3 = registry.get_or_create("q3", 4);
        let g1 = q1.admit().await;
        let _g2 = q2.admit().await;

        // The first query takes most of the batch memory.
        let mut m1 = q1.executor_memory(meter());
        m1.meter().add(3000 * MB as i64);
        m1.settle().await.unwrap();

        // The second one cannot get what it needs: it waits for the first one.
        let mut m2 = q2.executor_memory(meter());
        m2.meter().add(1000 * MB as i64);
        let waiter = tokio::spawn(async move {
            m2.settle().await.unwrap();
            m2
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        // A query that wants to start waits behind the operator, although its pool would fit.
        let starter = {
            let q3 = q3.clone();
            tokio::spawn(async move { q3.admit().await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!starter.is_finished());

        // The first query ends: the operator is served, then the query that waited to start.
        drop(m1);
        drop(g1);
        let m2 = waiter.await.unwrap();
        let _g3 = starter.await.unwrap();
        assert_eq!(root.get_bytes_used(), 1000 * MB as i64);
        assert!(q2.stats.overflow_wait_ms.load(Ordering::Relaxed) >= 50);
        drop(m2);
        assert_eq!(budget.available_mb(), 3600 - 2 * 256);
    }

    #[tokio::test]
    async fn test_executor_memory_fails_when_every_query_waits() {
        let (registry, _root) = registry(4000 * MB);
        let q1 = registry.get_or_create("q1", 4);
        let q2 = registry.get_or_create("q2", 4);
        let _g1 = q1.admit().await;
        let _g2 = q2.admit().await;

        let mut m1 = q1.executor_memory(meter());
        m1.meter().add(1500 * MB as i64);
        m1.settle().await.unwrap();
        let mut m2 = q2.executor_memory(meter());
        m2.meter().add(1500 * MB as i64);
        m2.settle().await.unwrap();

        // The first query waits for the second one...
        m1.meter().add(1000 * MB as i64);
        let waiter = tokio::spawn(async move {
            m1.settle().await.unwrap();
            m1
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        // ... which cannot wait for the first one in turn: the second query, the youngest, is
        // chosen to fail, and the first one goes on.
        m2.meter().add(1000 * MB as i64);
        let error = m2.settle().await.unwrap_err();
        assert!(
            matches!(error, BatchError::MemoryWaitVictim(..)),
            "{error:?}"
        );
        assert!(error.to_string().contains("chosen to fail"), "{error}");
        drop(m2);
        waiter.await.unwrap();
    }

    /// Spawns a task that takes `bytes` more for `memory`, and returns the memory with the
    /// result.
    fn grow_in_background(
        mut memory: ExecutorMemory,
        bytes: u64,
    ) -> tokio::task::JoinHandle<(Result<()>, ExecutorMemory)> {
        memory.meter().add(bytes as i64);
        tokio::spawn(async move {
            let result = memory.settle().await;
            (result, memory)
        })
    }

    #[tokio::test]
    async fn test_memory_victim_is_the_youngest_query() {
        // Batch memory: 4000 MB. Two queries take 256 MB each for their pools, and 1500 MB each
        // beyond them: 488 MB are left.
        let (registry, root) = registry(4000 * MB);
        let q1 = registry.get_or_create("q1", 4);
        let q2 = registry.get_or_create("q2", 4);
        let g1 = q1.admit().await;
        let g2 = q2.admit().await;
        let mut m1 = q1.executor_memory(meter());
        m1.meter().add(1500 * MB as i64);
        m1.settle().await.unwrap();
        let mut m2 = q2.executor_memory(meter());
        m2.meter().add(1500 * MB as i64);
        m2.settle().await.unwrap();

        // The younger query waits first: the older one still runs, it may release memory.
        let young = grow_in_background(q2.executor_memory(meter()), 1000 * MB);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!young.is_finished());

        // Then two operators of the older query wait: every query is blocked. Without a victim,
        // all three operators would fail. The youngest query is chosen, although it was not the
        // last one to wait, and only its operator fails.
        let old_a = grow_in_background(q1.executor_memory(meter()), 1000 * MB);
        let old_b = grow_in_background(q1.executor_memory(meter()), 300 * MB);
        let (result, young_memory) = tokio::time::timeout(Duration::from_secs(5), young)
            .await
            .unwrap()
            .unwrap();
        let error = result.unwrap_err();
        assert!(
            matches!(&error, BatchError::MemoryWaitVictim(op, _) if op == "unknown operator"),
            "{error:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!old_a.is_finished());
        assert!(!old_b.is_finished());

        // Any other operator of the chosen query that needs memory fails at once.
        let mut late = q2.executor_memory(meter());
        late.meter().add(1000 * MB as i64);
        let error = late.settle().await.unwrap_err();
        assert!(
            matches!(error, BatchError::MemoryWaitVictim(..)),
            "{error:?}"
        );
        assert!(q2.memory_victim.load(Ordering::SeqCst));
        assert!(!q1.memory_victim.load(Ordering::SeqCst));

        // The chosen query ends: the older one gets the memory that it released.
        drop((young_memory, late, m2, g2));
        let (result_a, memory_a) = tokio::time::timeout(Duration::from_secs(5), old_a)
            .await
            .unwrap()
            .unwrap();
        result_a.unwrap();
        let (result_b, memory_b) = tokio::time::timeout(Duration::from_secs(5), old_b)
            .await
            .unwrap()
            .unwrap();
        result_b.unwrap();
        {
            let state = registry.inner.budget.state.lock();
            assert_eq!(state.victim, None);
            assert_eq!(state.blocked_queries, 0);
            assert!(state.blocked_admissions.is_empty());
            assert_eq!(state.active_queries, 1);
        }
        drop((memory_a, memory_b, m1, g1));
        assert_eq!(root.get_bytes_used(), 0);
        assert_eq!(registry.inner.budget.available_mb(), 3600);
    }

    #[tokio::test]
    async fn test_no_victim_while_a_query_may_spill() {
        // A query whose operator asked its own spillable operators to spill may still get memory:
        // the other blocked query keeps waiting instead of failing or choosing a victim.
        let (registry, root) = registry(4000 * MB);
        let q1 = registry.get_or_create("q1", 4);
        q1.enable_spill_on_memory_wait();
        let g1 = q1.admit().await;
        let q2 = registry.get_or_create("q2", 4);
        let g2 = q2.admit().await;
        // The spillable operator of the first query holds 150 MB of the shared part of its pool.
        let spillable = q1.spillable_operator_context();
        assert!(spillable.add(166 * MB as i64));
        // The second query takes the rest of the node.
        let mut holder = q2.executor_memory(meter());
        holder.meter().add((4000 - 512) * MB as i64);
        holder.settle().await.unwrap();
        assert_eq!(registry.inner.budget.state.lock().taken_mb, 4000);

        let spilling = grow_in_background(
            q1.executor_memory(meter()).with_identity("GroupTopN 1"),
            100 * MB,
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        let blocked = grow_in_background(q2.executor_memory(meter()), 300 * MB);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        // Both wait: the first one for its spillable operator, the second one for the first.
        assert!(!spilling.is_finished());
        assert!(!blocked.is_finished());
        {
            let state = registry.inner.budget.state.lock();
            assert_eq!(state.victim, None);
            assert_eq!(state.blocked_queries, 2);
            assert_eq!(state.spilling_waiters, 1);
        }

        // The spillable operator spills: the first operator is served from the pool.
        assert!(spillable.add(-(166 * MB as i64)));
        let (result, memory) = tokio::time::timeout(Duration::from_secs(5), spilling)
            .await
            .unwrap()
            .unwrap();
        result.unwrap();
        assert_eq!(registry.inner.budget.state.lock().spilling_waiters, 0);
        assert!(!q1.memory_victim.load(Ordering::SeqCst));
        assert!(!q2.memory_victim.load(Ordering::SeqCst));

        // The second query gets its memory when its own holder ends.
        drop(holder);
        let (result, blocked_memory) = tokio::time::timeout(Duration::from_secs(5), blocked)
            .await
            .unwrap()
            .unwrap();
        result.unwrap();
        drop((memory, blocked_memory, spillable, g1, g2));
        assert_eq!(root.get_bytes_used(), 0);
    }

    /// A query alone on a node of 4000 MB: one operator that cannot spill holds all the memory
    /// of the node beyond the pool of the query, and a spillable operator holds 150 MB of the
    /// shared part of the pool (192 MB). Returns the query, the memory of the first operator and
    /// the context of the spillable one.
    async fn query_holding_the_node(
        registry: &QueryMemoryRegistry,
        spill_on_memory_wait: bool,
    ) -> (
        Arc<QueryMemory>,
        AdmissionGuard,
        ExecutorMemory,
        MemoryContext,
    ) {
        let query = registry.get_or_create("q", 4);
        if spill_on_memory_wait {
            query.enable_spill_on_memory_wait();
        }
        let guard = query.admit().await;
        let mut holder = query.executor_memory(meter()).with_identity("Holder");
        holder.meter().add((4000 - 256) * MB as i64);
        holder.settle().await.unwrap();
        assert_eq!(registry.inner.budget.state.lock().taken_mb, 4000);
        let spillable = query.spillable_operator_context();
        assert!(spillable.add(166 * MB as i64));
        assert_eq!(query.spill_gate.get_bytes_used(), 150 * MB as i64);
        (query, guard, holder, spillable)
    }

    #[tokio::test]
    async fn test_executor_memory_names_the_operator_when_it_fails() {
        let (registry, _root) = registry(4000 * MB);
        let (query, _guard, _holder, _spillable) = query_holding_the_node(&registry, false).await;
        // Nothing left on the node and no other query: without spill requests, the operator
        // fails at once, and the error names it.
        let mut memory = query.executor_memory(meter()).with_identity("GroupTopN 42");
        memory.meter().add(100 * MB as i64);
        let error = memory.settle().await.unwrap_err();
        assert!(matches!(error, BatchError::NoMemoryLeft(..)), "{error:?}");
        assert!(error.to_string().contains("GroupTopN 42"), "{error}");
        assert_eq!(query.stats.spill_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_executor_memory_asks_the_spillable_operators_to_spill() {
        let (registry, root) = registry(4000 * MB);
        let (query, _guard, holder, spillable) = query_holding_the_node(&registry, true).await;

        // The operator needs 100 MB: the pool lacks them, the node has nothing left, and no
        // other query runs. It asks the spillable operator to spill and waits for it.
        let mut memory = query.executor_memory(meter()).with_identity("GroupTopN 42");
        memory.meter().add(100 * MB as i64);
        let waiter = tokio::spawn(async move {
            memory.settle().await.unwrap();
            memory
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!waiter.is_finished());
        assert_eq!(query.stats.spill_requests.load(Ordering::Relaxed), 1);

        // The spillable operator can no longer grow beyond its reservation: it spills, and gives
        // its memory back.
        assert!(!spillable.add(1));
        assert!(spillable.add(-(166 * MB as i64)));
        // Its reservation is still granted, and beyond it what the waiting operator does not
        // need: 150 - 100 MB.
        assert!(spillable.add(16 * MB as i64));
        assert!(spillable.add(50 * MB as i64));
        assert!(!spillable.add(1));

        // The waiting operator takes what was released from the pool.
        let memory = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.shared_pool.get_bytes_used(), 150 * MB as i64);
        // The request ended: the spillable operator may grow again, within the pool.
        assert_eq!(query.spill_gate.mem_limit(), u64::MAX);
        assert!(spillable.add(42 * MB as i64));
        assert!(!spillable.add(1));

        drop((memory, holder, spillable));
        assert_eq!(root.get_bytes_used(), 0);
    }

    #[tokio::test]
    async fn test_spill_request_does_not_wait_for_operators_that_hold_nothing() {
        let (registry, _root) = registry(4000 * MB);
        let (query, _guard, _holder, spillable) = query_holding_the_node(&registry, true).await;
        // The spillable operator holds nothing beyond its reservation: there is nothing to wait
        // for, the operator fails at once, as without spill requests.
        assert!(spillable.add(-(150 * MB as i64)));
        // Another operator that cannot spill fills the pool.
        let mut other = query.executor_memory(meter());
        other.meter().add(192 * MB as i64);
        other.settle().await.unwrap();
        let mut memory = query.executor_memory(meter()).with_identity("LookupJoin 7");
        memory.meter().add(100 * MB as i64);
        let start = Instant::now();
        let error = memory.settle().await.unwrap_err();
        assert!(error.to_string().contains("LookupJoin 7"), "{error}");
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(query.stats.spill_requests.load(Ordering::Relaxed), 1);
        // The request ended with the wait.
        assert_eq!(query.spill_gate.mem_limit(), u64::MAX);
    }
}
