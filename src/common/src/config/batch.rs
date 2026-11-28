// Copyright 2025 RisingWave Labs
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

use risingwave_common_proc_macro::serde_prefix_all;

use super::*;

/// The section `[batch]` in `risingwave.toml`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchConfig {
    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,

    #[serde(default)]
    #[config_doc(nested)]
    pub developer: BatchDeveloperConfig,

    /// This is the max number of queries per sql session.
    #[serde(default)]
    pub distributed_query_limit: Option<u64>,

    /// This is the max number of batch queries per frontend node.
    #[serde(default)]
    pub max_batch_queries_per_frontend_node: Option<u64>,

    #[serde(default = "default::batch::enable_barrier_read")]
    pub enable_barrier_read: bool,

    /// Timeout for a batch query in seconds.
    #[serde(default = "default::batch::statement_timeout_in_sec")]
    pub statement_timeout_in_sec: u32,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    #[serde(default)]
    /// frontend compute runtime worker threads
    pub frontend_compute_runtime_worker_threads: Option<usize>,

    /// This is the secs used to mask a worker unavailable temporarily.
    #[serde(default = "default::batch::mask_worker_temporary_secs")]
    pub mask_worker_temporary_secs: usize,

    /// Keywords on which SQL option redaction is based in the query log.
    /// A SQL option with a name containing any of these keywords will be redacted.
    #[serde(default = "default::batch::redact_sql_option_keywords")]
    pub redact_sql_option_keywords: Vec<String>,

    /// Enable the spill out to disk feature for batch queries.
    #[serde(default = "default::batch::enable_spill")]
    pub enable_spill: bool,

    /// The share of the batch memory limit of a node that batch queries may really use. The rest
    /// is a safety margin: the accounted memory is lower than the allocated one, and the batch
    /// shares the process with other components.
    #[serde(default = "default::batch::memory_usable_ratio")]
    pub memory_usable_ratio: f64,

    /// The share of the usable batch memory of a node that a single query may use on that node.
    /// The spillable operators of a query that reaches it spill to disk.
    #[serde(default = "default::batch::query_memory_ratio")]
    pub query_memory_ratio: f64,

    /// The share of the usable batch memory of a node that a single query may use when it cannot
    /// run within `query_memory_ratio` even without parallelism. A query that needs more is
    /// rejected.
    #[serde(default = "default::batch::query_memory_max_ratio")]
    pub query_memory_max_ratio: f64,

    /// The memory, in MB, reserved for each spillable operator (hash agg, hash join, sort) of a
    /// query. The parallelism of a query is lowered until the reserved memory of its operators
    /// fits in the memory of the query, and the query is rejected when it never does.
    #[serde(default = "default::batch::spillable_operator_min_memory_mb")]
    pub spillable_operator_min_memory_mb: usize,

    /// The memory, in MB, that each spillable operator adds to the pool shared by the spillable
    /// operators of its query, within the memory of the query. An operator may use more than its
    /// reserved memory as long as the pool has room, and spills otherwise.
    #[serde(default = "default::batch::spillable_operator_avg_memory_mb")]
    pub spillable_operator_avg_memory_mb: usize,

    /// How long, in seconds, an operator that needs memory beyond the pool of its query waits for
    /// other queries to release some, before its query fails with an error saying that the batch
    /// memory is exhausted. The wait otherwise lasts until another query ends, which may never
    /// happen while that query waits for something else. 0 waits without limit.
    #[serde(default = "default::batch::overflow_memory_wait_secs")]
    pub overflow_memory_wait_secs: u64,
}

/// The subsections `[batch.developer]`.
///
/// It is put at [`BatchConfig::developer`].
#[serde_prefix_all("batch_", mode = "alias")]
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchDeveloperConfig {
    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::connector_message_buffer_size")]
    pub connector_message_buffer_size: usize,

    /// The size of the channel used for output to exchange/shuffle.
    #[serde(default = "default::developer::batch_output_channel_size")]
    pub output_channel_size: usize,

    #[serde(default = "default::developer::batch_receiver_channel_size")]
    pub receiver_channel_size: usize,

    #[serde(default = "default::developer::batch_root_stage_channel_size")]
    pub root_stage_channel_size: usize,

    /// The size of a chunk produced by `RowSeqScanExecutor`
    #[serde(default = "default::developer::batch_chunk_size")]
    pub chunk_size: usize,

    /// The number of the connections for batch remote exchange between two nodes.
    /// If not specified, the value of `server.connection_pool_size` will be used.
    #[serde(default = "default::developer::batch_exchange_connection_pool_size")]
    pub(super) exchange_connection_pool_size: Option<u16>,

    #[serde(default)]
    pub compute_client_config: RpcClientConfig,

    #[serde(default)]
    pub frontend_client_config: RpcClientConfig,

    #[serde(default = "default::developer::batch_local_execute_buffer_size")]
    pub local_execute_buffer_size: usize,
}

pub mod default {
    pub use crate::config::default::developer;

    pub mod batch {
        pub fn enable_barrier_read() -> bool {
            false
        }

        pub fn enable_spill() -> bool {
            true
        }

        pub fn memory_usable_ratio() -> f64 {
            0.9
        }

        pub fn query_memory_ratio() -> f64 {
            0.3
        }

        pub fn query_memory_max_ratio() -> f64 {
            0.7
        }

        pub fn spillable_operator_min_memory_mb() -> usize {
            8
        }

        pub fn spillable_operator_avg_memory_mb() -> usize {
            64
        }

        pub fn overflow_memory_wait_secs() -> u64 {
            300
        }

        pub fn statement_timeout_in_sec() -> u32 {
            // 1 hour
            60 * 60
        }

        pub fn mask_worker_temporary_secs() -> usize {
            30
        }

        pub fn redact_sql_option_keywords() -> Vec<String> {
            [
                "credential",
                "key",
                "password",
                "private",
                "secret",
                "token",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        }
    }
}
