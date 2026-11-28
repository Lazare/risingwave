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

//! Memory of a distributed batch query beyond its spillable operators, estimated from its plan
//! (`batch_admission_by_estimate`).
//!
//! A compute node admits a query when the pool of its spillable operators fits in its budget.
//! The output channels of the tasks and the iterators of the table scans are not part of that
//! pool: several queries with wide rows and many stages can be admitted together and then hold
//! several times the budget. This estimate is reserved at admission on top of the pool.
//!
//! - **Exchanges.** Each task of a stage other than the root keeps up to `output_channel_size`
//!   chunks (plus the one being sent) of `chunk_size` rows in its output channels (see the
//!   design note `batch-memory-safety.md`, section 7): `(slots + 1) x chunk_size x W` per task,
//!   with `W` the estimated width of a row of the output of the stage, bounded by
//!   `batch_exchange_buffer_max_mb` when it is set.
//! - **Scans.** Each table scan of a stage holds at least one decoded block per virtual node it
//!   reads (256 per table, 64 KB each) and a few levels: [`SCAN_BYTES`] per scan.
//!
//! Widths come from the schema: fixed-size types exactly, variable-size types at an assumed
//! average. The sum assumes every buffer is full at the same time: it is conservative. It is
//! not blocking for small queries: below [`MIN_RESERVED_BYTES`], nothing is reserved.

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::Field as PbField;

use crate::optimizer::plan_node::BatchPlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, StageGraph};

/// Below this estimate, nothing is reserved: the exchange buffers of small or narrow queries
/// fit in the room that the node keeps beyond the reservations.
pub const MIN_RESERVED_BYTES: u64 = 256 << 20;

/// The memory held by the iterators of one table scan: one decoded block of 64 KB per virtual
/// node (256) on two levels.
pub const SCAN_BYTES: u64 = 256 * 2 * (64 << 10);

/// Assumed average width of a variable-size value, by type.
const VARCHAR_BYTES: u64 = 32;
const JSONB_BYTES: u64 = 256;
const NESTED_BYTES: u64 = 128;

/// The settings that the estimate depends on.
#[derive(Debug, Clone, Copy)]
pub struct EstimateSettings {
    /// `output_channel_size`: chunks per output channel.
    pub output_channel_size: usize,
    /// Rows per chunk.
    pub chunk_size: usize,
    /// `batch_exchange_buffer_max_mb` in bytes, 0 for none.
    pub exchange_buffer_max_bytes: u64,
}

/// The estimated width of a value of the given type in a chunk.
fn value_width(data_type: &DataType) -> u64 {
    match data_type {
        DataType::Boolean => 1,
        DataType::Int16 => 2,
        DataType::Int32 | DataType::Float32 | DataType::Date => 4,
        DataType::Int64
        | DataType::Float64
        | DataType::Serial
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz => 8,
        DataType::Decimal | DataType::Interval => 16,
        DataType::Int256 => 32,
        // The value and its offset.
        DataType::Varchar | DataType::Bytea => VARCHAR_BYTES + 4,
        DataType::Jsonb => JSONB_BYTES + 4,
        _ => NESTED_BYTES,
    }
}

/// The estimated width of a row with the given schema: its values and their null flags.
pub fn row_width(schema: &[PbField]) -> u64 {
    schema
        .iter()
        .map(|field| {
            field
                .data_type
                .as_ref()
                .map_or(NESTED_BYTES, |t| value_width(&DataType::from(t)))
                + 1
        })
        .sum::<u64>()
        .max(1)
}

fn count_scans(node: &ExecutionPlanNode) -> u64 {
    let own = matches!(
        node.plan_node_type,
        BatchPlanNodeType::BatchSeqScan | BatchPlanNodeType::BatchLogSeqScan
    ) as u64;
    own + node.children.iter().map(count_scans).sum::<u64>()
}

/// The estimated memory of the exchange buffers and scans of all the stages, in bytes.
pub fn estimate_bytes(stage_graph: &StageGraph, settings: EstimateSettings) -> u64 {
    let slots = settings.output_channel_size as u64 + 1;
    let default_parallelism = stage_graph.batch_parallelism().max(1) as u64;
    stage_graph
        .stages
        .iter()
        .map(|(stage_id, stage)| {
            let tasks = stage.parallelism.map_or(default_parallelism, |p| p as u64);
            let scans = count_scans(&stage.root).saturating_mul(SCAN_BYTES);
            // The output of the root stage goes to the frontend.
            if *stage_id == stage_graph.root_stage_id {
                return scans;
            }
            let mut per_task = slots
                .saturating_mul(settings.chunk_size as u64)
                .saturating_mul(row_width(&stage.root.schema));
            if settings.exchange_buffer_max_bytes > 0 {
                per_task = per_task.min(settings.exchange_buffer_max_bytes);
            }
            per_task.saturating_mul(tasks).saturating_add(scans)
        })
        .fold(0, u64::saturating_add)
}

/// What the query reserves at admission beyond its spillable operators: the estimate, or 0 when
/// it is small.
pub fn extra_reservation_bytes(stage_graph: &StageGraph, settings: EstimateSettings) -> u64 {
    let estimate = estimate_bytes(stage_graph, settings);
    if estimate < MIN_RESERVED_BYTES {
        0
    } else {
        estimate
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::batch_plan::plan_node::NodeBody;

    use super::*;
    use crate::optimizer::plan_node::PlanNodeId;

    const MB: u64 = 1 << 20;

    fn settings() -> EstimateSettings {
        EstimateSettings {
            output_channel_size: 64,
            chunk_size: 1024,
            exchange_buffer_max_bytes: 0,
        }
    }

    fn field(data_type: DataType) -> PbField {
        PbField {
            data_type: Some(data_type.to_protobuf()),
            name: "c".to_owned(),
        }
    }

    fn node(
        plan_node_type: BatchPlanNodeType,
        schema: Vec<PbField>,
        children: Vec<ExecutionPlanNode>,
    ) -> ExecutionPlanNode {
        ExecutionPlanNode {
            plan_node_id: PlanNodeId(0),
            plan_node_type,
            node: NodeBody::Values(Default::default()),
            schema,
            children,
            source_stage_id: None,
        }
    }

    fn exchange(stage: u32, schema: Vec<PbField>) -> ExecutionPlanNode {
        let mut exchange = node(BatchPlanNodeType::BatchExchange, schema, vec![]);
        exchange.source_stage_id = Some(stage.into());
        exchange
    }

    /// Root <- join stage 1 <- scan stages 2 and 3, all with the given schema; 4 tasks per
    /// stage.
    fn join_query(schema: Vec<PbField>) -> StageGraph {
        let scan = || node(BatchPlanNodeType::BatchSeqScan, schema.clone(), vec![]);
        StageGraph::for_test(vec![
            (0, exchange(1, schema.clone()), vec![1]),
            (
                1,
                node(
                    BatchPlanNodeType::BatchHashJoin,
                    schema.clone(),
                    vec![exchange(2, schema.clone()), exchange(3, schema.clone())],
                ),
                vec![2, 3],
            ),
            (2, scan(), vec![]),
            (3, scan(), vec![]),
        ])
    }

    #[test]
    fn test_row_width() {
        assert_eq!(
            row_width(&[field(DataType::Int32), field(DataType::Int64)]),
            4 + 1 + 8 + 1
        );
        assert_eq!(row_width(&[field(DataType::Varchar)]), 37);
        assert_eq!(row_width(&[field(DataType::Jsonb)]), 261);
        assert_eq!(row_width(&[]), 1);
    }

    #[test]
    fn test_small_query_reserves_nothing() {
        // Two integer columns: 3 stages x 4 tasks x 65 x 1024 x 14 bytes = 10.7 MB of buffers,
        // and two scans of 32 MB.
        let graph = join_query(vec![field(DataType::Int32), field(DataType::Int64)]);
        let estimate = estimate_bytes(&graph, settings());
        assert_eq!(estimate, 3 * 4 * 65 * 1024 * 14 + 2 * SCAN_BYTES);
        assert!(estimate < MIN_RESERVED_BYTES);
        assert_eq!(extra_reservation_bytes(&graph, settings()), 0);
    }

    #[test]
    fn test_wide_rows_reserve_their_buffers() {
        // Eight JSON columns: about 2 KB per row, 130 MB per stage of 4 tasks.
        let graph = join_query(vec![field(DataType::Jsonb); 8]);
        let reserved = extra_reservation_bytes(&graph, settings());
        assert_eq!(reserved, 3 * 4 * 65 * 1024 * (8 * 261) + 2 * SCAN_BYTES);
        assert!(reserved > 1600 * MB);

        // With a bound in bytes on the output channels of each task, the bound counts.
        let bounded = EstimateSettings {
            exchange_buffer_max_bytes: 16 * MB,
            ..settings()
        };
        assert_eq!(
            estimate_bytes(&graph, bounded),
            3 * 4 * 16 * MB + 2 * SCAN_BYTES
        );
    }
}
