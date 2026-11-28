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

//! `streaming_preserve_mv_key`: the operator that gives a streaming job its key is not removed.
//!
//! The key of a materialized view is the stream key of its plan. An aggregation, a top-1 per
//! group or a dedup at the top of the plan makes it the group key. `AggOnKeyEliminateRule` removes
//! such an operator when its input is already unique on the group key, and the key of the view
//! then becomes the stream key of the input, which is wider when the input is a join: the stream
//! key of a join holds the stream keys of both sides, even when one side has at most one row per
//! row of the other.
//!
//! ```text
//! Project(a.id, b.v)                           Project(a.id, b.v)
//! └─TopN(1 per a.id)        stream key [a.id]  └─Join(Left, a.b_id = b.id)  stream key [a.id, b.id]
//!   └─Join(Left, a.b_id = b.id)
//! ```
//!
//! The wider key can not be narrowed again without a stateful operator: the rows of a key can
//! come from two actors (a row whose join key changes is deleted by one and inserted by the
//! other), in any order. A wider key changes the view for its consumers: the primary key of the
//! table and its distribution key (always a subset of the primary key) change, a batch lookup join
//! by the old key is no longer possible, and the rules that rely on the key of a scanned table (an
//! aggregation, a top-1 or a join on that key is not needed) no longer apply downstream.
//!
//! With the variable, the stage that removes such operators is applied below the top-most one,
//! when removing it would change the key. The other operators are still removed.

use std::collections::BTreeSet;

use crate::error::Result;
use crate::optimizer::OptimizationStage;
use crate::optimizer::plan_node::LogicalPlanRef as PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;

/// Applies `stage` to `plan`, except to the operator that gives the plan its stream key (see
/// [`key_operator_depth`]), if removing that operator would change the key. The projections and
/// filters above it and the operator itself are kept as they are; the stage is applied to its
/// input.
pub fn optimize_below_key_operator(plan: PlanRef, stage: &OptimizationStage) -> Result<PlanRef> {
    match key_operator_depth(&plan) {
        Some(depth) => optimize_below(plan, depth, stage),
        None => plan.optimize_by_rules(stage),
    }
}

fn optimize_below(plan: PlanRef, depth: usize, stage: &OptimizationStage) -> Result<PlanRef> {
    let input = plan.inputs()[0].clone();
    let input = if depth == 0 {
        input.optimize_by_rules(stage)?
    } else {
        optimize_below(input, depth - 1, stage)?
    };
    Ok(plan.clone_with_inputs(&[input]))
}

/// The depth, below the projections and filters at the top of the plan, of the first aggregation
/// with a group key, top-n that keeps one row per group, or dedup, if the stream key of its input
/// is not within its group key: removing it would then replace the key of the plan with a wider
/// or different one. `None` otherwise, or if another operator comes first.
fn key_operator_depth(plan: &PlanRef) -> Option<usize> {
    let mut node = plan.clone();
    let mut depth = 0;
    while node.as_logical_project().is_some() || node.as_logical_filter().is_some() {
        node = node.inputs()[0].clone();
        depth += 1;
    }
    let group_key: BTreeSet<usize> = if let Some(agg) = node.as_logical_agg() {
        if agg.group_key().is_empty() || !agg.grouping_sets().is_empty() {
            return None;
        }
        agg.group_key().indices().collect()
    } else if let Some(top_n) = node.as_logical_top_n() {
        if !top_n.limit_attr().max_one_row() || top_n.group_key().is_empty() {
            return None;
        }
        top_n.group_key().iter().copied().collect()
    } else {
        let dedup = node.as_logical_dedup()?;
        dedup.dedup_cols().iter().copied().collect()
    };
    let input = node.inputs()[0].clone();
    let keeps_key = input
        .stream_key()
        .is_some_and(|key| key.iter().all(|column| group_key.contains(column)));
    (!keeps_key).then_some(depth)
}
