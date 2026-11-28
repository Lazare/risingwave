// Copyright 2022 RisingWave Labs
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

use std::sync::Arc;

use prometheus::core::Atomic;
use risingwave_common::catalog::SysCatalogReaderRef;
use risingwave_common::config::BatchConfig;
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::metrics_reader::MetricsReader;
use risingwave_common::util::addr::{HostAddr, is_local_address};
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_storage::StateStoreImpl;

use crate::error::Result;
use crate::executor::CardinalityCounters;
use crate::monitor::{BatchMetrics, BatchMetricsInner, BatchSpillMetrics};
use crate::task::query_memory::{ExecutorMemory, QueryMemory};
use crate::task::{BatchEnvironment, MISSING_TASK_WAIT, TaskId, TaskOutput, TaskOutputId};
use crate::worker_manager::worker_node_manager::WorkerNodeManagerRef;

/// Context for batch task execution.
///
/// This context is specific to one task execution, and should *not* be shared by different tasks.
pub trait BatchTaskContext: Send + Sync + 'static {
    /// Get task output identified by `task_output_id`.
    ///
    /// Returns error if the task of `task_output_id` doesn't run in same worker as current task.
    fn get_task_output(&self, task_output_id: TaskOutputId) -> Result<TaskOutput>;

    /// Waits, for a bounded time, until the task `task_id` exists on this node: a consumer may
    /// ask for the output of a task that the frontend creates later (`batch_phased_stages`).
    fn wait_for_local_task(&self, _task_id: &TaskId) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(async {})
    }

    /// Get system catalog reader, used to read system table.
    fn catalog_reader(&self) -> SysCatalogReaderRef;

    /// Whether `peer_addr` is in same as current task.
    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool;

    fn dml_manager(&self) -> DmlManagerRef;

    fn state_store(&self) -> StateStoreImpl;

    /// Get batch metrics.
    /// None indicates that not collect task metrics.
    fn batch_metrics(&self) -> Option<BatchMetrics>;

    fn spill_metrics(&self) -> Arc<BatchSpillMetrics>;

    /// Get compute client pool. This is used in grpc exchange to avoid creating new compute client
    /// for each grpc call.
    fn client_pool(&self) -> ComputeClientPoolRef;

    /// Get config for batch environment
    fn get_config(&self) -> &BatchConfig;

    fn source_metrics(&self) -> Arc<SourceMetrics>;

    fn create_executor_mem_context(&self, executor_id: &str) -> MemoryContext;

    /// Creates the memory context of an operator that spills to disk when it lacks memory. Where
    /// spilling is enabled, the context is bounded to the part of the operator in its query.
    fn create_spillable_executor_mem_context(&self, executor_id: &str) -> MemoryContext {
        self.create_executor_mem_context(executor_id)
    }

    /// Creates the memory of an operator that cannot spill to disk. Where queries are bounded,
    /// it takes its memory from the pool of the query, then from the memory left on the node.
    fn create_executor_memory(&self, executor_id: &str) -> ExecutorMemory {
        ExecutorMemory::unmanaged(self.create_executor_mem_context(executor_id))
    }

    /// The memory of the query that the task belongs to, where queries are bounded.
    fn query_memory(&self) -> Option<Arc<QueryMemory>> {
        None
    }

    fn worker_node_manager(&self) -> Option<WorkerNodeManagerRef>;

    /// Get metrics reader for reading channel delta stats and other metrics.
    fn metrics_reader(&self) -> Arc<dyn MetricsReader>;

    /// The counters of the cardinality feedback (`batch_cardinality_feedback`), when the plan
    /// has nodes whose output rows are counted.
    fn cardinality_counters(&self) -> Option<Arc<CardinalityCounters>> {
        None
    }

    /// How the task sends its output to its consumers.
    fn exchange_settings(&self) -> ExchangeSettings {
        ExchangeSettings::default()
    }
}

/// How a task sends its output, from the session variables of its query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExchangeSettings {
    /// `batch_compact_shuffle_output`: a hash shuffle copies the rows of each output into chunks
    /// of their own instead of sending each output the whole chunk with a visibility.
    pub compact_shuffle: bool,
    /// `batch_exchange_buffer_max_mb`, in bytes: the bytes that the output channels of the task
    /// may hold at most, 0 for no bound beyond their number of chunks.
    pub buffer_limit_bytes: u64,
}

/// Batch task context on compute node.
#[derive(Clone)]
pub struct ComputeNodeContext {
    env: BatchEnvironment,

    batch_metrics: BatchMetrics,

    mem_context: MemoryContext,

    /// The memory of the query that the task belongs to. `None` in tests.
    query_memory: Option<Arc<QueryMemory>>,

    cardinality_counters: Option<Arc<CardinalityCounters>>,

    exchange_settings: ExchangeSettings,
}

impl BatchTaskContext for ComputeNodeContext {
    fn get_task_output(&self, task_output_id: TaskOutputId) -> Result<TaskOutput> {
        self.env
            .task_manager()
            .take_output(&task_output_id.to_prost())
    }

    fn wait_for_local_task(&self, task_id: &TaskId) -> futures::future::BoxFuture<'static, ()> {
        let task_manager = self.env.task_manager();
        let task_id = task_id.clone();
        Box::pin(async move {
            task_manager
                .wait_for_task(&task_id, MISSING_TASK_WAIT)
                .await
        })
    }

    fn catalog_reader(&self) -> SysCatalogReaderRef {
        unimplemented!("not supported in distributed mode")
    }

    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool {
        is_local_address(self.env.server_address(), peer_addr)
    }

    fn dml_manager(&self) -> DmlManagerRef {
        self.env.dml_manager_ref()
    }

    fn state_store(&self) -> StateStoreImpl {
        self.env.state_store()
    }

    fn batch_metrics(&self) -> Option<BatchMetrics> {
        Some(self.batch_metrics.clone())
    }

    fn spill_metrics(&self) -> Arc<BatchSpillMetrics> {
        self.env.spill_metrics()
    }

    fn client_pool(&self) -> ComputeClientPoolRef {
        self.env.client_pool()
    }

    fn get_config(&self) -> &BatchConfig {
        self.env.config()
    }

    fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.env.source_metrics()
    }

    fn create_executor_mem_context(&self, _executor_id: &str) -> MemoryContext {
        let counter = TrAdderAtomic::new(0);
        MemoryContext::new(Some(self.mem_context.clone()), counter)
    }

    fn create_spillable_executor_mem_context(&self, executor_id: &str) -> MemoryContext {
        match &self.query_memory {
            // Without spilling, the operator would fail at its part instead of spilling.
            Some(query_memory) if self.env.config().enable_spill => {
                query_memory.spillable_operator_context()
            }
            _ => self.create_executor_mem_context(executor_id),
        }
    }

    fn create_executor_memory(&self, executor_id: &str) -> ExecutorMemory {
        match &self.query_memory {
            Some(query_memory) => query_memory
                .executor_memory(MemoryContext::root(TrAdderAtomic::new(0), u64::MAX))
                .with_identity(executor_id),
            None => ExecutorMemory::unmanaged(self.create_executor_mem_context(executor_id)),
        }
    }

    fn query_memory(&self) -> Option<Arc<QueryMemory>> {
        self.query_memory.clone()
    }

    fn worker_node_manager(&self) -> Option<WorkerNodeManagerRef> {
        None
    }

    fn metrics_reader(&self) -> Arc<dyn MetricsReader> {
        unimplemented!("metrics_reader not supported in compute node context")
    }

    fn cardinality_counters(&self) -> Option<Arc<CardinalityCounters>> {
        self.cardinality_counters.clone()
    }

    fn exchange_settings(&self) -> ExchangeSettings {
        self.exchange_settings
    }
}

impl ComputeNodeContext {
    pub fn for_test() -> Arc<dyn BatchTaskContext> {
        Arc::new(Self {
            env: BatchEnvironment::for_test(),
            batch_metrics: BatchMetricsInner::for_test(),
            mem_context: MemoryContext::none(),
            query_memory: None,
            cardinality_counters: None,
            exchange_settings: ExchangeSettings::default(),
        })
    }

    /// Creates the context of a task of the given query, which runs
    /// `spillable_operator_instances` spillable operators in total (0 if unknown).
    /// `cardinality_counters`: the counters of the plan nodes counted by the cardinality feedback,
    /// see [`CardinalityCounters::for_plan`]. `spill_on_memory_wait`: `batch_spill_on_memory_wait`
    /// of the query.
    pub fn create(
        env: BatchEnvironment,
        query_id: &str,
        spillable_operator_instances: u32,
        spill_on_memory_wait: bool,
        cardinality_counters: Option<Arc<CardinalityCounters>>,
        exchange_settings: ExchangeSettings,
    ) -> Arc<dyn BatchTaskContext> {
        let query_memory = env.task_manager().query_memory(
            query_id,
            spillable_operator_instances,
            spill_on_memory_wait,
        );
        let mem_context = query_memory.context();
        let batch_metrics = Arc::new(BatchMetricsInner::new(
            env.task_manager().metrics(),
            env.executor_metrics(),
            env.iceberg_scan_metrics(),
        ));
        Arc::new(Self {
            env,
            batch_metrics,
            mem_context,
            query_memory: Some(query_memory),
            cardinality_counters,
            exchange_settings,
        })
    }
}
