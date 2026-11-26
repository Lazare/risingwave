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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;
use crate::optimizer::cardinality_feedback::hex;
use crate::scheduler::cardinality_feedback::CARDINALITY_FEEDBACK_STORE;

/// The cardinality feedback of the batch queries run by this frontend with
/// `batch_cardinality_feedback` on: per fingerprint of sub-plan and kind of operator, the rows
/// estimated by the planner and the rows counted at execution. It holds counts and hashes only.
#[derive(Fields)]
#[primary_key(fingerprint, operator_kind)]
struct RwBatchCardinalityFeedback {
    /// The fingerprint of the sub-plan of the operator, 16 hexadecimal digits.
    fingerprint: String,
    /// E.g. `seq_scan`, `filter`, `hash_join`, `lookup_join`, `hash_agg`.
    operator_kind: String,
    /// The estimate of the last execution, null when unknown.
    estimated_rows: Option<f64>,
    /// The rows counted at the last execution.
    last_actual_rows: i64,
    /// Whether the last count is partial (a lower bound).
    last_partial: bool,
    executions: i64,
    /// Executions whose count is partial: failed, cancelled or timed-out queries, or operators
    /// stopped early by a limit above them.
    partial_executions: i64,
    /// Mean of `log10(actual rows / estimated rows)` over the complete executions with an
    /// estimate: 2 means 100 times more rows than estimated.
    mean_log10_ratio: Option<f64>,
    last_seen: Timestamptz,
}

#[system_catalog(table, "rw_catalog.rw_batch_cardinality_feedback")]
fn read_rw_batch_cardinality_feedback(
    _reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwBatchCardinalityFeedback>> {
    Ok(CARDINALITY_FEEDBACK_STORE
        .records()
        .into_iter()
        .map(|(fingerprint, kind, record)| RwBatchCardinalityFeedback {
            fingerprint: hex(fingerprint),
            operator_kind: kind.to_owned(),
            estimated_rows: record.estimated_rows,
            last_actual_rows: i64::try_from(record.last_actual_rows).unwrap_or(i64::MAX),
            last_partial: record.last_partial,
            executions: i64::try_from(record.executions).unwrap_or(i64::MAX),
            partial_executions: i64::try_from(record.partial_executions).unwrap_or(i64::MAX),
            mean_log10_ratio: record.mean_log10_ratio(),
            last_seen: Timestamptz::from_micros_uncheck(record.last_seen_micros),
        })
        .collect())
}
