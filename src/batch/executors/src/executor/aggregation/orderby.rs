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

use std::ops::Range;

use anyhow::Context;
use risingwave_common::array::{ListValue, Op, RowRef, StreamChunk, StructValue};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{
    DataType, Datum, ListType, ScalarImpl, ScalarRefImpl, StructType, ToOwnedDatum,
};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{
    AggStateDyn, AggregateFunction, AggregateState, BoxedAggregateFunction,
};
use risingwave_expr::{ExprError, Result};

/// `ProjectionOrderBy` is a wrapper of `AggregateFunction` that sorts rows by given columns and
/// then projects columns.
pub struct ProjectionOrderBy {
    inner: BoxedAggregateFunction,
    arg_types: Vec<DataType>,
    arg_indices: Vec<usize>,
    order_col_indices: Vec<usize>,
    order_types: Vec<OrderType>,
    kept_rows: KeptRows,
    /// Type of the encoded state: a list of `struct<order key, arguments...>`.
    encoded_state_type: DataType,
}

/// The ordered rows that the inner aggregate function needs to compute its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeptRows {
    /// All the rows.
    All,
    /// Only the first row in the order, e.g. for `first_value`.
    First,
    /// Only the last row in the order, e.g. for `last_value`.
    Last,
}

#[derive(Debug)]
struct State {
    unordered_values: Vec<(OrderKey, OwnedRow)>,
    unordered_values_estimated_heap_size: usize,
}

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.unordered_values.capacity() * std::mem::size_of::<(OrderKey, OwnedRow)>()
            + self.unordered_values_estimated_heap_size
    }
}

impl AggStateDyn for State {}

type OrderKey = Box<[u8]>;

impl ProjectionOrderBy {
    pub fn new(
        arg_types: Vec<DataType>,
        arg_indices: Vec<usize>,
        column_orders: Vec<ColumnOrder>,
        kept_rows: KeptRows,
        inner: BoxedAggregateFunction,
    ) -> Self {
        let (order_col_indices, order_types) = column_orders
            .into_iter()
            .map(|c| (c.column_index, c.order_type))
            .unzip();
        let encoded_row_type =
            StructType::unnamed(std::iter::once(DataType::Bytea).chain(arg_types.iter().cloned()));
        let encoded_state_type = ListType::new(encoded_row_type.into()).into();
        Self {
            inner,
            arg_types,
            arg_indices,
            order_col_indices,
            order_types,
            kept_rows,
            encoded_state_type,
        }
    }

    fn push_row(&self, state: &mut State, row: RowRef<'_>) -> Result<()> {
        let key =
            memcmp_encoding::encode_row(row.project(&self.order_col_indices), &self.order_types)
                .context("failed to encode row")?;
        // When a single row is kept, skip the rows that cannot replace it. Rows with equal keys
        // have no defined order, so either of them can be kept.
        let replaced = match (self.kept_rows, state.unordered_values.first()) {
            (KeptRows::All, _) | (_, None) => false,
            (KeptRows::First, Some((kept_key, _))) => {
                if key[..] >= kept_key[..] {
                    return Ok(());
                }
                true
            }
            (KeptRows::Last, Some((kept_key, _))) => {
                if key[..] < kept_key[..] {
                    return Ok(());
                }
                true
            }
        };
        let projected_row = row.project(&self.arg_indices).to_owned_row();
        self.push_entry(state, key.into(), projected_row, replaced);
        Ok(())
    }

    /// Adds an entry to the state, or replaces the single kept entry if `replaced` is true.
    fn push_entry(&self, state: &mut State, key: OrderKey, row: OwnedRow, replaced: bool) {
        let entry_size = key.len() + row.estimated_heap_size();
        if replaced {
            state.unordered_values[0] = (key, row);
            state.unordered_values_estimated_heap_size = entry_size;
        } else {
            state.unordered_values.push((key, row));
            state.unordered_values_estimated_heap_size += entry_size;
        }
    }

    fn encoded_row_type(&self) -> &DataType {
        self.encoded_state_type.as_list_elem()
    }
}

#[async_trait::async_trait]
impl AggregateFunction for ProjectionOrderBy {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::new(State {
            unordered_values: vec![],
            unordered_values_estimated_heap_size: 0,
        })))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = state.downcast_mut::<State>();
        if self.kept_rows == KeptRows::All {
            state.unordered_values.reserve(input.cardinality());
        }
        for (op, row) in input.rows() {
            assert_eq!(op, Op::Insert, "only support append");
            self.push_row(state, row)?;
        }
        Ok(())
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        let state = state.downcast_mut::<State>();
        if self.kept_rows == KeptRows::All {
            state.unordered_values.reserve(range.len());
        }
        for (op, row) in input.rows_in(range) {
            assert_eq!(op, Op::Insert, "only support append");
            self.push_row(state, row)?;
        }
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        let mut inner_state = self.inner.create_state()?;
        // sort
        let mut rows = state.unordered_values.clone();
        rows.sort_unstable_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));
        // build chunk
        let mut chunk_builder = DataChunkBuilder::new(self.arg_types.clone(), 1024);
        for (_, row) in rows {
            if let Some(data_chunk) = chunk_builder.append_one_row(row) {
                let chunk = StreamChunk::from(data_chunk);
                self.inner.update(&mut inner_state, &chunk).await?;
            }
        }
        if let Some(data_chunk) = chunk_builder.consume_all() {
            let chunk = StreamChunk::from(data_chunk);
            self.inner.update(&mut inner_state, &chunk).await?;
        }
        self.inner.get_result(&inner_state).await
    }

    /// Encodes the kept rows as a list of `struct<order key, arguments...>`, so that the batch
    /// hash aggregation can spill the state to disk.
    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        let rows = state.unordered_values.iter().map(|(key, row)| {
            let fields = std::iter::once(Some(ScalarImpl::Bytea(key.clone())))
                .chain(row.iter().map(|datum| datum.to_owned_datum()))
                .collect();
            Some(ScalarImpl::Struct(StructValue::new(fields)))
        });
        let list = ListValue::from_datum_iter(self.encoded_row_type(), rows);
        Ok(Some(list.into()))
    }

    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        let mut state = State {
            unordered_values: vec![],
            unordered_values_estimated_heap_size: 0,
        };
        let list = match datum {
            Some(ScalarImpl::List(list)) => list,
            _ => return Err(ExprError::InvalidState("expect a list of rows".into())),
        };
        state.unordered_values.reserve_exact(list.len());
        for row in list.iter() {
            let Some(ScalarRefImpl::Struct(row)) = row else {
                return Err(ExprError::InvalidState("expect a struct".into()));
            };
            let mut fields = row.iter_fields_ref();
            let Some(Some(ScalarRefImpl::Bytea(key))) = fields.next() else {
                return Err(ExprError::InvalidState("expect an order key".into()));
            };
            let row = OwnedRow::new(fields.map(|datum| datum.to_owned_datum()).collect());
            self.push_entry(&mut state, key.into(), row, false);
        }
        Ok(AggregateState::Any(Box::new(state)))
    }

    fn encoded_state_type(&self) -> DataType {
        self.encoded_state_type.clone()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{ListValue, StreamChunk};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::ScalarImpl;
    use risingwave_expr::aggregate::AggCall;

    use super::super::build;

    #[tokio::test]
    async fn array_agg_with_order() {
        let chunk = StreamChunk::from_pretty(
            " i    i
            + 123  3
            + 456  2
            + 789  2
            + 321  9",
        );
        let agg = build(&AggCall::from_pretty(
            "(array_agg:int4[] $0:int4 orderby $1:asc $0:desc)",
        ))
        .unwrap();
        let mut state = agg.create_state().unwrap();
        agg.update(&mut state, &chunk).await.unwrap();
        assert_eq!(
            agg.get_result(&state).await.unwrap(),
            Some(ListValue::from_iter([789, 456, 123, 321]).into())
        );
    }

    /// Encodes and decodes the state between two chunks, as the batch hash aggregation does when
    /// it spills to disk.
    #[tokio::test]
    async fn encode_and_decode_state() {
        let chunk1 = StreamChunk::from_pretty(
            " i    i
            + 123  3
            + 456  2
            + 789  .",
        );
        let chunk2 = StreamChunk::from_pretty(
            " i    i
            + 321  9
            + .    1",
        );
        for (pretty, expected) in [
            (
                "(array_agg:int4[] $0:int4 orderby $1:asc)",
                Some(
                    ListValue::from_iter([None, Some(456), Some(123), Some(321), Some(789)]).into(),
                ),
            ),
            ("(first_value:int4 $0:int4 orderby $1:asc)", None),
            (
                "(first_value:int4 $0:int4 orderby $1:desc)",
                Some(789.into()),
            ),
            ("(last_value:int4 $0:int4 orderby $1:asc)", Some(789.into())),
            ("(last_value:int4 $0:int4 orderby $1:desc)", None),
        ] {
            let agg = build(&AggCall::from_pretty(pretty)).unwrap();
            let state_type = agg.encoded_state_type();

            // A state without any row can be encoded as well.
            let empty = agg.encode_state(&agg.create_state().unwrap()).unwrap();
            let mut state = agg.decode_state(empty).unwrap();

            agg.update(&mut state, &chunk1).await.unwrap();
            let encoded = agg.encode_state(&state).unwrap();
            // The encoded state must fit in an array of the declared type.
            let mut builder = state_type.create_array_builder(1);
            builder.append(&encoded);
            let mut state = agg.decode_state(builder.finish().datum_at(0)).unwrap();
            agg.update(&mut state, &chunk2).await.unwrap();

            assert_eq!(agg.get_result(&state).await.unwrap(), expected, "{pretty}");
        }
    }

    /// `first_value` and `last_value` only keep one row per group.
    #[tokio::test]
    async fn first_last_value_keep_one_row() {
        let chunk = StreamChunk::from_pretty(
            " i    i
            + 123  3
            + 456  2
            + 789  2
            + 321  9",
        );
        for pretty in [
            "(first_value:int4 $0:int4 orderby $1:asc)",
            "(last_value:int4 $0:int4 orderby $1:asc)",
        ] {
            let agg = build(&AggCall::from_pretty(pretty)).unwrap();
            let mut state = agg.create_state().unwrap();
            agg.update(&mut state, &chunk).await.unwrap();
            agg.update(&mut state, &chunk).await.unwrap();
            let Some(ScalarImpl::List(rows)) = agg.encode_state(&state).unwrap() else {
                panic!("expect a list");
            };
            assert_eq!(rows.len(), 1, "{pretty}");
        }
    }

    #[tokio::test]
    async fn string_agg_with_order() {
        let chunk = StreamChunk::from_pretty(
            " T   T i i
            + aaa _ 1 3
            + bbb _ 0 4
            + ccc _ 0 8
            + ddd _ 1 3",
        );
        let agg = build(&AggCall::from_pretty(
            "(string_agg:varchar $0:varchar $1:varchar orderby $2:asc $3:desc $0:desc)",
        ))
        .unwrap();
        let mut state = agg.create_state().unwrap();
        agg.update(&mut state, &chunk).await.unwrap();
        assert_eq!(
            agg.get_result(&state).await.unwrap(),
            Some("ccc_bbb_ddd_aaa".into())
        );
    }
}
