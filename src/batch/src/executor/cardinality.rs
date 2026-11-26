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

//! Cardinality feedback of batch queries (session variable `batch_cardinality_feedback`): the
//! executors of the plan nodes that the frontend marks (`PlanNode.cardinality_feedback_id`) count
//! the rows they output, so that the frontend can compare them with its estimates.
//!
//! The counting adds the cardinality of each chunk to an atomic counter: one relaxed atomic
//! addition per chunk, never per row. Nothing is counted, and no executor is wrapped, for a plan
//! without marked nodes.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use futures_async_stream::try_stream;
use parking_lot::Mutex;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::{OperatorCardinality, PlanNode};

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, Executor};

/// The counters of one plan node.
#[derive(Debug, Default)]
pub struct NodeCardinality {
    rows: AtomicU64,
    instances: AtomicU32,
    completed: AtomicU32,
}

impl NodeCardinality {
    pub fn add_rows(&self, rows: usize) {
        self.rows.fetch_add(rows as u64, Ordering::Relaxed);
    }

    fn mark_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    pub fn instances(&self) -> u32 {
        self.instances.load(Ordering::Relaxed)
    }

    pub fn completed(&self) -> u32 {
        self.completed.load(Ordering::Relaxed)
    }
}

/// The counters of the marked plan nodes of a task (on a compute node) or of a query (in the
/// frontend, where the counts of the tasks of the other nodes are merged into it).
#[derive(Debug, Default)]
pub struct CardinalityCounters {
    nodes: Mutex<BTreeMap<u32, Arc<NodeCardinality>>>,
}

impl CardinalityCounters {
    /// The counters of a task that executes `plan`, if one of its nodes is marked.
    pub fn for_plan(plan: Option<&PlanNode>) -> Option<Arc<Self>> {
        fn has_marked_node(node: &PlanNode) -> bool {
            node.cardinality_feedback_id.is_some() || node.children.iter().any(has_marked_node)
        }
        plan.filter(|plan| has_marked_node(plan))
            .map(|_| Arc::new(Self::default()))
    }

    fn node(&self, node_id: u32) -> Arc<NodeCardinality> {
        self.nodes.lock().entry(node_id).or_default().clone()
    }

    /// Registers an executor of the node `node_id` and returns the counters it adds its rows to.
    pub fn register(&self, node_id: u32) -> Arc<NodeCardinality> {
        let node = self.node(node_id);
        node.instances.fetch_add(1, Ordering::Relaxed);
        node
    }

    /// Adds the counts of another task.
    pub fn merge(&self, counts: &[OperatorCardinality]) {
        for count in counts {
            let node = self.node(count.node_id);
            node.rows.fetch_add(count.rows, Ordering::Relaxed);
            node.instances.fetch_add(count.instances, Ordering::Relaxed);
            node.completed.fetch_add(count.completed, Ordering::Relaxed);
        }
    }

    /// The current counts, by node id.
    pub fn to_protobuf(&self) -> Vec<OperatorCardinality> {
        self.nodes
            .lock()
            .iter()
            .map(|(node_id, node)| OperatorCardinality {
                node_id: *node_id,
                rows: node.rows(),
                instances: node.instances(),
                completed: node.completed(),
            })
            .collect()
    }
}

/// Counts the rows output by its child, chunk by chunk, and whether the output ended normally.
pub struct CountingExecutor {
    child: BoxedExecutor,
    counter: Arc<NodeCardinality>,
}

impl CountingExecutor {
    pub fn new(child: BoxedExecutor, counter: Arc<NodeCardinality>) -> Self {
        Self { child, counter }
    }
}

impl Executor for CountingExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        self.child.identity()
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn execute(self: Box<Self>) {
        let counter = self.counter;
        #[for_await]
        for chunk in self.child.execute() {
            let chunk = chunk?;
            counter.add_rows(chunk.cardinality());
            yield chunk;
        }
        // Not reached when the output is dropped early or fails: the count is then partial.
        counter.mark_completed();
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn mock_input(chunks: &[&str]) -> BoxedExecutor {
        let schema = Schema::new(vec![Field::unnamed(DataType::Int32)]);
        let mut input = MockExecutor::new(schema);
        for chunk in chunks {
            input.add(DataChunk::from_pretty(chunk));
        }
        Box::new(input)
    }

    #[tokio::test]
    async fn test_counting_executor_counts_visible_rows_per_chunk() {
        let counters = CardinalityCounters::default();
        let executor = Box::new(CountingExecutor::new(
            mock_input(&["i\n 1\n 2\n 3", "i\n 4 D\n 5"]),
            counters.register(7),
        ));
        let mut stream = executor.execute();
        while let Some(chunk) = stream.next().await {
            chunk.unwrap();
        }
        drop(stream);
        // The row marked `D` is not visible.
        assert_eq!(
            counters.to_protobuf(),
            vec![OperatorCardinality {
                node_id: 7,
                rows: 4,
                instances: 1,
                completed: 1,
            }]
        );
    }

    #[tokio::test]
    async fn test_counting_executor_stopped_early_is_not_completed() {
        let counters = CardinalityCounters::default();
        let executor = Box::new(CountingExecutor::new(
            mock_input(&["i\n 1\n 2", "i\n 3"]),
            counters.register(1),
        ));
        let mut stream = executor.execute();
        stream.next().await.unwrap().unwrap();
        // The consumer stops after the first chunk, as a limit above would.
        drop(stream);
        assert_eq!(
            counters.to_protobuf(),
            vec![OperatorCardinality {
                node_id: 1,
                rows: 2,
                instances: 1,
                completed: 0,
            }]
        );
    }

    #[test]
    fn test_merge_counts_of_tasks() {
        let query = CardinalityCounters::default();
        // A node counted in the query itself (e.g. the root stage run by the frontend).
        query.register(1).add_rows(5);
        // Two tasks of a stage report the same nodes.
        let task_1 = vec![
            OperatorCardinality {
                node_id: 2,
                rows: 10,
                instances: 1,
                completed: 1,
            },
            OperatorCardinality {
                node_id: 3,
                rows: 4,
                instances: 1,
                completed: 1,
            },
        ];
        let task_2 = vec![
            OperatorCardinality {
                node_id: 2,
                rows: 7,
                instances: 1,
                completed: 1,
            },
            OperatorCardinality {
                node_id: 3,
                rows: 1,
                instances: 1,
                completed: 0,
            },
        ];
        query.merge(&task_1);
        query.merge(&task_2);
        assert_eq!(
            query.to_protobuf(),
            vec![
                OperatorCardinality {
                    node_id: 1,
                    rows: 5,
                    instances: 1,
                    completed: 0,
                },
                OperatorCardinality {
                    node_id: 2,
                    rows: 17,
                    instances: 2,
                    completed: 2,
                },
                OperatorCardinality {
                    node_id: 3,
                    rows: 5,
                    instances: 2,
                    completed: 1,
                },
            ]
        );
    }

    #[test]
    fn test_counters_only_for_marked_plans() {
        let mut plan = PlanNode {
            children: vec![PlanNode::default()],
            ..Default::default()
        };
        assert!(CardinalityCounters::for_plan(Some(&plan)).is_none());
        assert!(CardinalityCounters::for_plan(None).is_none());
        plan.children[0].cardinality_feedback_id = Some(3);
        assert!(CardinalityCounters::for_plan(Some(&plan)).is_some());
    }
}
