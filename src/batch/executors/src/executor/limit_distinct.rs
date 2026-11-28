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

use std::marker::PhantomData;

use futures_async_stream::try_stream;
use hashbrown::HashSet;
use itertools::Itertools;
use risingwave_batch::task::ExecutorMemory;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::{BitmapBuilder, FilterByBitmap};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::{MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::types::DataType;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

/// Limit distinct executor (`batch_limit_distinct`): outputs the distinct values of the group key
/// columns of its input, at most `limit` of them. It emits each value with the chunk where it
/// first sees it, and stops reading its input once it has emitted `limit` values: dropping the
/// input stream ends the input (an exchange ends its producers as it does under a limit). It keeps
/// the values emitted so far, `limit` at most, in a hash set counted in its memory context. `NULL`
/// values are equal, as for `DISTINCT`.
pub struct LimitDistinctExecutor<K: HashKey> {
    child: BoxedExecutor,
    group_key: Vec<usize>,
    limit: usize,
    schema: Schema,
    identity: String,
    mem_ctx: MemoryContext,
    memory: ExecutorMemory,
    _phantom: PhantomData<K>,
}

pub struct LimitDistinctExecutorBuilder {
    child: BoxedExecutor,
    group_key: Vec<usize>,
    group_key_types: Vec<DataType>,
    limit: usize,
    identity: String,
    memory: ExecutorMemory,
}

impl HashKeyDispatcher for LimitDistinctExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(LimitDistinctExecutor::<K>::new(
            self.child,
            self.group_key,
            self.limit,
            self.identity,
            self.memory,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

impl BoxedExecutorBuilder for LimitDistinctExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::LimitDistinct
        )?;

        let group_key = node.group_key.iter().map(|x| *x as usize).collect_vec();
        let child_schema = child.schema();
        let group_key_types = group_key
            .iter()
            .map(|x| child_schema.fields[*x].data_type())
            .collect();
        let identity = source.plan_node().get_identity().clone();

        let builder = Self {
            child,
            group_key,
            group_key_types,
            limit: node.get_limit() as usize,
            identity: identity.clone(),
            memory: source.context().create_executor_memory(&identity),
        };
        Ok(builder.dispatch())
    }
}

impl<K: HashKey> LimitDistinctExecutor<K> {
    pub fn new(
        child: BoxedExecutor,
        group_key: Vec<usize>,
        limit: usize,
        identity: String,
        memory: impl Into<ExecutorMemory>,
    ) -> Self {
        let child_schema = child.schema();
        let schema = Schema {
            fields: group_key
                .iter()
                .map(|i| child_schema.fields[*i].clone())
                .collect::<Vec<Field>>(),
        };
        let memory = memory.into();
        let mem_ctx = memory.meter();
        Self {
            child,
            group_key,
            limit,
            schema,
            identity,
            mem_ctx,
            memory,
            _phantom: PhantomData,
        }
    }
}

impl<K: HashKey> Executor for LimitDistinctExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl<K: HashKey> LimitDistinctExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        if self.limit == 0 {
            return Ok(());
        }
        let mut seen = HashSet::<K, PrecomputedBuildHasher, MonitoredGlobalAlloc>::with_hasher_in(
            PrecomputedBuildHasher,
            self.mem_ctx.global_allocator(),
        );
        let mut memory = self.memory;
        let mut emitted = 0;

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = chunk?;
            let keys = K::build_many(&self.group_key, &chunk);
            let mut new_values = BitmapBuilder::zeroed(chunk.capacity());
            for (row_id, key) in keys
                .into_iter()
                .enumerate()
                .filter_by_bitmap(chunk.visibility())
            {
                if emitted == self.limit {
                    break;
                }
                if seen.insert(key) {
                    new_values.set(row_id, true);
                    emitted += 1;
                }
            }
            memory.settle().await?;
            let output = chunk
                .project(&self.group_key)
                .with_visibility(new_values.finish())
                .compact_vis();
            if output.cardinality() > 0 {
                yield output;
            }
            if emitted == self.limit {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::test_prelude::DataChunkTestExt;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn mock_input(chunks: &[&str]) -> BoxedExecutor {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        for chunk in chunks {
            mock_executor.add(DataChunk::from_pretty(chunk));
        }
        Box::new(mock_executor)
    }

    async fn run(chunks: &[&str], group_key: Vec<usize>, limit: usize) -> Vec<DataChunk> {
        let mem_ctx = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        let executor = Box::new(LimitDistinctExecutor::<
            risingwave_common::hash::SerializedKey,
        >::new(
            mock_input(chunks),
            group_key,
            limit,
            "LimitDistinctExecutor".to_owned(),
            mem_ctx,
        ));
        let mut stream = executor.execute();
        let mut output = vec![];
        while let Some(chunk) = stream.next().await {
            output.push(chunk.unwrap());
        }
        output
    }

    #[tokio::test]
    async fn test_limit_distinct() {
        let chunks = [
            "i i
             1 10
             1 11
             . 12
             2 13",
            "i i
             . 14
             2 15
             3 16
             4 17",
            "i i
             5 18",
        ];
        // The first three distinct values of the first column, NULL counted once; the third
        // chunk is never read.
        let output = run(&chunks, vec![0], 3).await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0],
            DataChunk::from_pretty(
                "i
                 1
                 .
                 2"
            )
        );
        // More values asked than there are: every distinct value once.
        let output = run(&chunks, vec![0], 100).await;
        let rows: usize = output.iter().map(|c| c.cardinality()).sum();
        assert_eq!(rows, 6);
        // The value of the fourth row of the second chunk ends the output.
        let output = run(&chunks, vec![0], 5).await;
        assert_eq!(output.len(), 2);
        assert_eq!(
            output[1],
            DataChunk::from_pretty(
                "i
                 3
                 4"
            )
        );
    }
}
