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

//! This module contains wrappers over `Aggregator` trait that implement `order by`, `filter`,
//! `distinct` and project.

mod distinct;
mod filter;
mod orderby;
mod projection;

use risingwave_expr::Result;
use risingwave_expr::aggregate::{
    AggCall, AggType, BoxedAggregateFunction, PbAggKind, build_append_only,
};

use self::distinct::Distinct;
use self::filter::*;
use self::orderby::{KeptRows, ProjectionOrderBy};
use self::projection::Projection;

/// Build an `BoxedAggregateFunction` from `AggCall`.
pub fn build(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    let mut aggregator = build_append_only(agg)?;

    if agg.distinct {
        aggregator = Box::new(Distinct::new(aggregator));
    }
    if agg.column_orders.is_empty() {
        aggregator = Box::new(Projection::new(agg.args.val_indices().to_vec(), aggregator));
    } else {
        // `first_value` and `last_value` only read one of the ordered rows, so the other rows do
        // not need to be kept in the state.
        let kept_rows = match agg.agg_type {
            AggType::Builtin(PbAggKind::FirstValue) => KeptRows::First,
            AggType::Builtin(PbAggKind::LastValue) => KeptRows::Last,
            _ => KeptRows::All,
        };
        aggregator = Box::new(ProjectionOrderBy::new(
            agg.args.arg_types().to_vec(),
            agg.args.val_indices().to_vec(),
            agg.column_orders.clone(),
            kept_rows,
            aggregator,
        ));
    }
    if let Some(expr) = &agg.filter {
        aggregator = Box::new(Filter::new(expr.clone(), aggregator));
    }

    Ok(aggregator)
}
