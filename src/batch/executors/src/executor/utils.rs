// Copyright 2024 RisingWave Labs
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

use futures::StreamExt;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::PbScanRange;
use risingwave_pb::plan_common::StorageTableDesc;
pub use risingwave_storage::table::batch_table::PkScanRange as ScanRange;

use crate::error::{BatchError, Result};
use crate::executor::{BoxedDataChunkStream, Executor};

pub type BoxedDataChunkListStream = BoxStream<'static, Result<Vec<DataChunk>>>;

/// A chunk that an operator keeps for a while, compacted when fewer than half of its rows are
/// visible: a slice of a hash exchange shares the columns of the whole chunk with the slices of
/// the other consumers, and keeping it would keep the whole chunk alive, counted in full by every
/// consumer that keeps a slice. Compacting copies the visible rows only, and drops the reference
/// to the shared columns. A chunk with at least half of its rows visible is kept as it is: copying
/// would cost more than it saves.
pub fn compact_if_sparse(chunk: DataChunk) -> DataChunk {
    if chunk.cardinality() * 2 < chunk.capacity() {
        chunk.compact_vis()
    } else {
        chunk
    }
}

/// Read at least `rows` rows.
#[try_stream(boxed, ok = Vec<DataChunk>, error = BatchError)]
pub async fn batch_read(mut stream: BoxedDataChunkStream, rows: usize) {
    let mut cnt = 0;
    let mut chunk_list = vec![];
    while let Some(build_chunk) = stream.next().await {
        // Kept until the batch is joined.
        let build_chunk = compact_if_sparse(build_chunk?);
        cnt += build_chunk.cardinality();
        chunk_list.push(build_chunk);
        if cnt < rows {
            continue;
        } else {
            yield chunk_list;
            cnt = 0;
            chunk_list = vec![];
        }
    }
    if !chunk_list.is_empty() {
        yield chunk_list;
    }
}

pub struct BufferChunkExecutor {
    schema: Schema,
    chunk_list: Vec<DataChunk>,
}

impl Executor for BufferChunkExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "BufferChunkExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl BufferChunkExecutor {
    pub fn new(schema: Schema, chunk_list: Vec<DataChunk>) -> Self {
        Self { schema, chunk_list }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self) {
        for chunk in self.chunk_list {
            yield chunk
        }
    }
}

pub struct DummyExecutor {
    pub schema: Schema,
}

impl Executor for DummyExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "dummy"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        DummyExecutor::do_nothing()
    }
}

impl DummyExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_nothing() {}
}

pub struct WrapStreamExecutor {
    schema: Schema,
    stream: BoxedDataChunkStream,
}

impl WrapStreamExecutor {
    pub fn new(schema: Schema, stream: BoxedDataChunkStream) -> Self {
        Self { schema, stream }
    }
}

impl Executor for WrapStreamExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "WrapStreamExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.stream
    }
}

pub fn build_scan_ranges_from_pb(
    scan_ranges: &Vec<PbScanRange>,
    table_desc: &StorageTableDesc,
) -> Result<Vec<ScanRange>> {
    Ok(ScanRange::build_from_protobuf(scan_ranges, table_desc)?)
}

pub fn build_scan_range_from_pb(
    scan_range: &PbScanRange,
    table_desc: &StorageTableDesc,
) -> Result<ScanRange> {
    Ok(ScanRange::from_protobuf(scan_range, table_desc)?)
}

#[cfg(test)]
mod tests {
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::test_prelude::DataChunkTestExt;

    use super::*;

    #[test]
    fn test_compact_if_sparse() {
        let chunk = DataChunk::from_pretty(
            "I
             1
             2
             3
             4",
        );
        // A quarter visible: compacted.
        let sparse = compact_if_sparse(
            chunk.with_visibility(Bitmap::from_bool_slice(&[false, true, false, false])),
        );
        assert_eq!(sparse.capacity(), 1);
        assert_eq!(sparse, DataChunk::from_pretty("I\n 2"));
        // Half visible: kept as it is, sharing the columns.
        let half = chunk.with_visibility(Bitmap::from_bool_slice(&[true, false, true, false]));
        let kept = compact_if_sparse(half.clone());
        assert_eq!(kept.capacity(), 4);
        assert!(std::sync::Arc::ptr_eq(
            kept.column_at(0),
            chunk.column_at(0)
        ));
        assert_eq!(kept.cardinality(), 2);
    }
}
