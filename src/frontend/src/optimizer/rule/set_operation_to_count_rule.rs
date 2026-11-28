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

//! `INTERSECT` and `EXCEPT` (distinct) computed by one counting aggregation over a `UNION ALL` of
//! their inputs, instead of semi or anti joins followed by a `DISTINCT` (Calcite
//! `IntersectToDistinctRule`, `MinusToDistinctRule`).
//!
//! ```text
//! Intersect(distinct) / Except(distinct)     Project(c0, .., cn)
//! ├─input 0                                  └─Filter(cnt0 > 0 AND cnt1 > 0)        (INTERSECT)
//! └─input 1                            =>      │       (cnt0 > 0 AND cnt1 = 0)        (EXCEPT)
//!                                              └─Agg(group by c0 .. cn: count(m0) as cnt0,
//!                                                │                       count(m1) as cnt1)
//!                                                └─Union(all)
//!                                                  ├─Project(c0 .. cn, m0 = 1, m1 = NULL)
//!                                                  │ └─input 0
//!                                                  └─Project(c0 .. cn, m0 = NULL, m1 = 1)
//!                                                    └─input 1
//! ```
//!
//! A group of the aggregation is a distinct row of the inputs (grouping treats `NULL`s as equal,
//! as the set operations do), and `cnt i` is the number of its copies in input `i`: the row is in
//! every input (`INTERSECT`) or in the first one and in no other one (`EXCEPT`). Each output row
//! is a group, so there are no duplicates. With more than two inputs (after the merge of stacked
//! set operations), every input has its own count.
//!
//! The rule only applies when every column has a type in which equal values are identical
//! ([`has_exact_equality`]): the value that a group outputs is the value of one of its rows, which
//! may come from another input than the first one (in a streaming job, the row that created the
//! group). With `numeric` (`1.0` and `1.00`), floats (`0.0` and `-0.0`) or intervals, the joins
//! always output the value of the first input; the aggregation may not.
//!
//! In a streaming job, the aggregation keeps each distinct row of the inputs once, with its
//! counts, instead of the two join states (every row of each input), their degree table and the
//! state of the `DISTINCT` above the join. On demand, behind `streaming_set_operation_to_count`
//! and `batch_set_operation_to_count`. `INTERSECT ALL` and `EXCEPT ALL` are not supported by the
//! binder, so they never reach the rule.

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::PbAggKind;

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{LogicalFilter, LogicalProject, LogicalUnion, PlanTreeNode};
use crate::utils::Condition;

/// Whether two equal values of the type are always identical: same text, same result of every
/// function. Not for `numeric` (scale), floats (`-0.0`), intervals (`'1 day'` and `'24 hours'`),
/// `jsonb` (numbers inside) and composite types.
pub(super) fn has_exact_equality(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Int256
            | DataType::Serial
            | DataType::Varchar
            | DataType::Bytea
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
    )
}

/// See the documentation of the module.
pub struct SetOperationToCountRule {}

impl Rule<Logical> for SetOperationToCountRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let intersect = if let Some(intersect) = plan.as_logical_intersect() {
            if intersect.all() {
                return None;
            }
            true
        } else if let Some(except) = plan.as_logical_except() {
            if except.all() {
                return None;
            }
            false
        } else {
            return None;
        };
        let inputs = plan.inputs();
        let fields = plan.schema().fields().to_vec();
        if inputs.len() < 2
            || fields.is_empty()
            || !fields
                .iter()
                .all(|field| has_exact_equality(&field.data_type()))
        {
            return None;
        }
        // The inputs of a `UNION ALL` must have the same types.
        if !inputs.iter().all(|input| {
            input.schema().len() == fields.len()
                && (input.schema().fields().iter())
                    .zip(&fields)
                    .all(|(field, other)| field.data_type() == other.data_type())
        }) {
            return None;
        }
        let width = fields.len();
        let count = inputs.len();

        // Each input with one marker column per input: `1` in its own, `NULL` in the others.
        let marked: Vec<PlanRef> = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| {
                let mut exprs: Vec<ExprImpl> = fields
                    .iter()
                    .enumerate()
                    .map(|(column, field)| InputRef::new(column, field.data_type()).into())
                    .collect();
                exprs.extend((0..count).map(|marker| {
                    if marker == index {
                        ExprImpl::literal_int(1)
                    } else {
                        ExprImpl::literal_null(DataType::Int32)
                    }
                }));
                LogicalProject::create(input.clone(), exprs)
            })
            .collect();
        let union = LogicalUnion::create(true, marked);

        // `count(m i)`: the number of copies of the row in input `i`.
        let calls: Vec<PlanAggCall> = (0..count)
            .map(|marker| PlanAggCall {
                agg_type: PbAggKind::Count.into(),
                return_type: DataType::Int64,
                inputs: vec![InputRef::new(width + marker, DataType::Int32)],
                distinct: false,
                order_by: vec![],
                filter: Condition::true_cond(),
                direct_args: vec![],
            })
            .collect();
        let agg: PlanRef = Agg::new(calls, (0..width).collect(), union).into();

        let conjunctions = (0..count)
            .map(|marker| {
                let present = marker == 0 || intersect;
                FunctionCall::new_unchecked(
                    if present {
                        ExprType::GreaterThan
                    } else {
                        ExprType::Equal
                    },
                    vec![
                        InputRef::new(width + marker, DataType::Int64).into(),
                        ExprImpl::literal_bigint(0),
                    ],
                    DataType::Boolean,
                )
                .into()
            })
            .collect();
        let filter = LogicalFilter::create(agg, Condition { conjunctions });
        Some(LogicalProject::with_out_col_idx(filter, 0..width).into())
    }
}

impl SetOperationToCountRule {
    pub fn create() -> BoxedRule {
        Box::new(SetOperationToCountRule {})
    }
}
