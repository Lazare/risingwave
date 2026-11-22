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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use parking_lot::Mutex;
use risingwave_common::config::BatchConfig;
use risingwave_common::memory::{MemCounter, MemoryContext};
use risingwave_pb::task_service::GetBatchLimitsResponse;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A query is reported when it leaves the node only if it used at least this much memory, or if
/// one of its operators reached its limit.
const REPORT_MIN_PEAK_BYTES: i64 = 64 << 20;

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

/// The memory of one query on this node, shared by all its tasks.
pub struct QueryMemory {
    query_id: String,
    context: MemoryContext,
    /// The part of the pool that is not reserved, charged with what the spillable operators use
    /// beyond their reservation.
    shared_pool: MemoryContext,
    query_limit: u64,
    operator_pool: u64,
    operator_reserved: u64,
    spillable_operator_instances: u32,
    /// The memory, in MB, that the query reserves on the node before it starts.
    reservation_mb: u32,
    /// The reservation, shared by all the tasks of the query that are running.
    reservation: tokio::sync::Mutex<Option<OwnedSemaphorePermit>>,
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
            self.shared_pool.clone(),
            self.context.clone(),
            self.operator_reserved,
            counter,
        )
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
        if self.reservation_mb == 0 {
            return AdmissionGuard { query: None };
        }
        let mut reservation = self.reservation.lock().await;
        if reservation.is_none() {
            let start = Instant::now();
            *reservation = registry
                .admission
                .clone()
                .acquire_many_owned(self.reservation_mb)
                .await
                .ok();
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
        if query_peak < REPORT_MIN_PEAK_BYTES
            && operators_at_limit == 0
            && !query_at_limit
            && queued_ms < 1000
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
            "batch query memory"
        );
    }
}

struct QueryMemoryRegistryInner {
    root: MemoryContext,
    limits: QueryMemoryLimits,
    /// The usable batch memory of the node, in MB, that queries reserve before they start.
    admission: Arc<Semaphore>,
    admission_mb: usize,
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
        let admission_mb = if limits.is_unlimited() {
            0
        } else {
            ((limits.usable >> 20) as usize).min(Semaphore::MAX_PERMITS)
        };
        Self {
            inner: Arc::new(QueryMemoryRegistryInner {
                root,
                limits,
                admission: Arc::new(Semaphore::new(admission_mb)),
                admission_mb,
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
            operator_pool
                .saturating_sub(operator_reserved.saturating_mul(spillable_operator_instances as u64)),
        );
        let query = Arc::new(QueryMemory {
            query_id: query_id.to_owned(),
            context,
            shared_pool,
            query_limit: limits.query_limit(spillable_operator_instances),
            operator_pool,
            operator_reserved,
            spillable_operator_instances,
            // Never more than the whole semaphore, so that the query is eventually served.
            reservation_mb: ((limits.reservation(spillable_operator_instances) >> 20) as usize)
                .min(self.inner.admission_mb)
                .min(u32::MAX as usize) as u32,
            reservation: tokio::sync::Mutex::new(None),
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
        assert_eq!(
            registry.inner.admission.available_permits(),
            3600 - 2 * 1800
        );

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
        assert_eq!(
            registry.inner.admission.available_permits(),
            3600 - 2 * 1800
        );
        assert!(q3.stats.queued_ms.load(Ordering::Relaxed) >= 50);

        // A later task of a query whose reservation was released reserves again.
        drop(g2);
        let g1 = q1.admit().await;
        assert_eq!(
            registry.inner.admission.available_permits(),
            3600 - 2 * 1800
        );

        drop((g1, g3));
        assert_eq!(registry.inner.admission.available_permits(), 3600);
    }
}
