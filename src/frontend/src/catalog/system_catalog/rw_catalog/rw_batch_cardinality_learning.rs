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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;
use crate::optimizer::cardinality_feedback::hex;
use crate::optimizer::cardinality_learning::CARDINALITY_LEARNING_STORE;

/// The ratios learned by this frontend from the batch queries it observed
/// (`batch_cardinality_feedback_learning`): per key of sub-plan and kind of ratio, the
/// distribution of the measured ratios. It holds ratios and hashes only.
#[derive(Fields)]
#[primary_key(learning_key, ratio_kind)]
struct RwBatchCardinalityLearning {
    /// The key of the sub-plan (or of the edge between two relations of a join), 16
    /// hexadecimal digits.
    learning_key: String,
    /// `selectivity` (rows kept by a filter per input row), `group_ratio` (groups of an
    /// aggregation per input row) or `fan_out` (output rows of a join per row of its larger input).
    ratio_kind: String,
    observations: i64,
    /// The decayed geometric mean of the ratios.
    mean_ratio: f64,
    min_ratio: f64,
    max_ratio: f64,
}

#[system_catalog(table, "rw_catalog.rw_batch_cardinality_learning")]
fn read_rw_batch_cardinality_learning(
    _reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwBatchCardinalityLearning>> {
    Ok(CARDINALITY_LEARNING_STORE
        .ratios()
        .into_iter()
        .map(|(kind, key, learned)| RwBatchCardinalityLearning {
            learning_key: hex(key),
            ratio_kind: kind.as_str().to_owned(),
            observations: i64::try_from(learned.observations).unwrap_or(i64::MAX),
            mean_ratio: learned.mean(),
            min_ratio: learned.min,
            max_ratio: learned.max,
        })
        .collect())
}
