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

//! Rule `BatchLimitDistinctRule` (`batch_limit_distinct`): a `LIMIT n OFFSET k` over a distinct
//! (a hash aggregation with a group key and no aggregate calls, such as `SELECT DISTINCT` or
//! `UNION` without `ALL`) reads the distinct values with a [`BatchLimitDistinct`] that stops after
//! `n + k` values, instead of a hash aggregation that reads its whole input first. The limit stays
//! above (it applies the offset).
//!
//! When the input of the distinct is a `UNION ALL`, each branch gets a local `BatchLimitDistinct`
//! with `n + k` values too: if the union has `n + k` distinct values, either one branch has
//! `n + k` of them and emits `n + k` values, or every branch emits all its distinct values; the
//! union of what the branches emit holds `n + k` distinct values in both cases (or all of them),
//! and the operator above keeps `n + k` of them.

use super::prelude::*;
use crate::optimizer::plan_node::{BatchLimitDistinct, PlanTreeNodeUnary};
use crate::planner::LIMIT_ALL_COUNT;

pub struct BatchLimitDistinctRule {}

/// Replaces the distinct below `plan` (through projections) by a limited distinct of `limit`
/// values. `None` when there is none.
fn limit_distinct(plan: &PlanRef, limit: u64) -> Option<PlanRef> {
    if let Some(project) = plan.as_batch_project() {
        let input = limit_distinct(&project.input(), limit)?;
        return Some(project.clone_with_input(input).into());
    }
    let agg = plan.as_batch_hash_agg()?;
    let core = agg.core();
    if core.group_key.is_empty() || !core.agg_calls.is_empty() || !core.grouping_sets.is_empty() {
        return None;
    }
    let mut core = core.clone();
    // A batch union is always a `UNION ALL` (`LogicalUnion::to_batch`).
    if core.input.as_batch_union().is_some() {
        let branches: Vec<PlanRef> = core
            .input
            .inputs()
            .into_iter()
            .map(|branch| {
                let mut branch_core = core.clone();
                branch_core.input = branch;
                BatchLimitDistinct::new(branch_core, limit, false).into()
            })
            .collect();
        core.input = core.input.clone_root_with_inputs(&branches);
    }
    Some(BatchLimitDistinct::new(core, limit, true).into())
}

impl Rule<Batch> for BatchLimitDistinctRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let limit = plan.as_batch_limit()?;
        // `OFFSET` without `LIMIT` reads every value: nothing to stop early.
        let values = limit.limit().checked_add(limit.offset())?;
        if values == 0 || values >= LIMIT_ALL_COUNT {
            return None;
        }
        let input = limit_distinct(&limit.input(), values)?;
        Some(limit.clone_with_input(input).into())
    }
}

impl BatchLimitDistinctRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchLimitDistinctRule {})
    }
}
