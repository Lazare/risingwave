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

//! The memory limits that compute nodes apply to batch queries, as known by the frontend, and
//! the choice of the parallelism of a query from them.
//!
//! Each spillable operator (hash agg, hash join, sort) needs a minimum of memory to work
//! properly, and a query may only use a share of the batch memory of a node. A plan whose
//! operators, multiplied by its parallelism, cannot all get their minimum within that share is
//! run with a lower parallelism. Without any parallelism, it may use a larger share; beyond it,
//! the query is rejected.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerNode;
use risingwave_rpc_client::ComputeClientPoolRef;
use thiserror_ext::AsReport;

/// How long the limits of a compute node are used before they are asked again.
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// The memory limits that a compute node applies to batch queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    /// What a single query may use on the node.
    pub query_memory: u64,
    /// What a single query may use on the node when it runs without parallelism.
    pub query_memory_max: u64,
    /// The least memory that each spillable operator needs.
    pub operator_min_memory: u64,
}

/// What to do with a plan fragmented with a given parallelism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelismDecision {
    /// The spillable operators all get their minimum: run the plan as it is.
    Keep,
    /// Fragment the plan again with this lower parallelism.
    Lower(usize),
    /// Even without parallelism, the plan needs more memory than a query may use.
    Reject { needed: u64, allowed: u64 },
}

impl BatchLimits {
    /// The most restrictive of two sets of limits.
    fn min(self, other: Self) -> Self {
        Self {
            query_memory: self.query_memory.min(other.query_memory),
            query_memory_max: self.query_memory_max.min(other.query_memory_max),
            operator_min_memory: self.operator_min_memory.max(other.operator_min_memory),
        }
    }

    /// Decides what to do with a plan that, fragmented with `parallelism`, runs
    /// `spillable_operator_instances` spillable operators over all its stages and tasks.
    pub fn decide(
        &self,
        parallelism: usize,
        spillable_operator_instances: u32,
    ) -> ParallelismDecision {
        let needed = self
            .operator_min_memory
            .saturating_mul(spillable_operator_instances as u64);
        if needed <= self.query_memory {
            return ParallelismDecision::Keep;
        }
        if parallelism > 1 {
            // The instances of the stages that follow the parallelism shrink in proportion.
            // Those of the other stages do not, so the result is checked again by the caller.
            let scaled =
                (parallelism as u128 * self.query_memory as u128 / needed as u128) as usize;
            return ParallelismDecision::Lower(scaled.clamp(1, parallelism - 1));
        }
        if needed <= self.query_memory_max {
            ParallelismDecision::Keep
        } else {
            ParallelismDecision::Reject {
                needed,
                allowed: self.query_memory_max,
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Entry {
    /// `None` when the node does not bound the memory of batch queries, or could not be asked.
    limits: Option<BatchLimits>,
    fetched_at: Instant,
    fetching: bool,
}

/// The limits of the compute nodes, asked in the background and kept for a while.
///
/// Planning never waits for a compute node: until a node has answered, no limit is known for it
/// and the parallelism is not lowered. The node still enforces its limits by itself.
static CACHE: LazyLock<Mutex<HashMap<HostAddr, Entry>>> = LazyLock::new(Default::default);

/// The limits that apply to a query scheduled on the given compute nodes: the most restrictive
/// ones. `None` when no limit is known yet.
pub fn batch_limits(
    workers: &[WorkerNode],
    client_pool: &ComputeClientPoolRef,
) -> Option<BatchLimits> {
    let mut result: Option<BatchLimits> = None;
    let mut cache = CACHE.lock();
    for worker in workers {
        let Some(host) = worker.host.as_ref() else {
            continue;
        };
        let addr = HostAddr::from(host);
        let entry = cache.get(&addr).copied();
        if let Some(limits) = entry.and_then(|e| e.limits) {
            result = Some(result.map_or(limits, |r| r.min(limits)));
        }
        let stale = entry.is_none_or(|e| !e.fetching && e.fetched_at.elapsed() >= REFRESH_INTERVAL);
        if stale {
            cache.insert(
                addr.clone(),
                Entry {
                    limits: entry.and_then(|e| e.limits),
                    fetched_at: Instant::now(),
                    fetching: true,
                },
            );
            tokio::spawn(fetch(addr, client_pool.clone()));
        }
    }
    result
}

async fn fetch(addr: HostAddr, client_pool: ComputeClientPoolRef) {
    let fetched = async {
        let client = client_pool.get_by_addr(addr.clone()).await?;
        client.get_batch_limits().await
    }
    .await;
    let mut cache = CACHE.lock();
    let previous = cache.get(&addr).and_then(|e| e.limits);
    let limits = match fetched {
        Ok(resp) => resp.limited.then_some(BatchLimits {
            query_memory: resp.query_memory_bytes,
            query_memory_max: resp.query_memory_max_bytes,
            operator_min_memory: resp.spillable_operator_min_memory_bytes,
        }),
        Err(e) => {
            tracing::warn!(
                %addr,
                error = %e.as_report(),
                "failed to get the batch memory limits of a compute node"
            );
            previous
        }
    };
    if limits != previous {
        tracing::info!(%addr, ?limits, "batch memory limits of a compute node");
    }
    cache.insert(
        addr,
        Entry {
            limits,
            fetched_at: Instant::now(),
            fetching: false,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1 << 20;

    fn limits() -> BatchLimits {
        BatchLimits {
            query_memory: 8192 * MB,
            query_memory_max: 16384 * MB,
            operator_min_memory: 64 * MB,
        }
    }

    #[test]
    fn test_keep() {
        // 21 operators x 6 tasks x 64 MB = 8064 MB fit in 8192 MB.
        assert_eq!(limits().decide(6, 21 * 6), ParallelismDecision::Keep);
        // No spillable operator: nothing to bound.
        assert_eq!(limits().decide(32, 0), ParallelismDecision::Keep);
    }

    #[test]
    fn test_lower() {
        // 21 operators x 32 tasks x 64 MB = 43008 MB: about 5 times too much.
        assert_eq!(limits().decide(32, 21 * 32), ParallelismDecision::Lower(6));
        // Always strictly lower, even when the proportion rounds to the same value.
        assert_eq!(limits().decide(2, 129), ParallelismDecision::Lower(1));
        // And never below 1.
        assert_eq!(limits().decide(4, 100_000), ParallelismDecision::Lower(1));
    }

    #[test]
    fn test_without_parallelism() {
        // 200 operators x 64 MB = 12800 MB: more than the normal share, within the larger one.
        assert_eq!(limits().decide(1, 200), ParallelismDecision::Keep);
        // 300 operators x 64 MB = 19200 MB: more than a query may ever use.
        assert_eq!(
            limits().decide(1, 300),
            ParallelismDecision::Reject {
                needed: 19200 * MB,
                allowed: 16384 * MB
            }
        );
    }

    #[test]
    fn test_most_restrictive() {
        let other = BatchLimits {
            query_memory: 4096 * MB,
            query_memory_max: 32768 * MB,
            operator_min_memory: 128 * MB,
        };
        assert_eq!(
            limits().min(other),
            BatchLimits {
                query_memory: 4096 * MB,
                query_memory_max: 16384 * MB,
                operator_min_memory: 128 * MB,
            }
        );
    }
}
