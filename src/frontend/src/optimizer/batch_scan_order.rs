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

//! Batch table scans whose order no operator uses (`batch_scan_max_open_vnode_iterators`).
//!
//! A batch scan of a whole table reports the order of its primary key, and then merges the
//! iterators of all its vnodes, all open at once, to produce it. Most of the time nothing above
//! the scan uses that order: a hash join, a hash aggregation, a sort or an exchange that does not
//! merge sorted inputs. Such a scan may read its vnodes in any order, a few at a time.
//!
//! The order of a scan is unused when every path from the root of the plan to the scan goes
//! through an operator that does not depend on the order of its input (hash join, union, hash or
//! simple aggregation without `ORDER BY` in its calls, sort, top-n, exchange that does not merge
//! sorted inputs), with only projections and filters (which keep the order and pass on the need of
//! their parent) in between. Every other operator is assumed to use the order of its input.

use crate::optimizer::plan_node::BatchPlanRef as PlanRef;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;

/// Marks the table scans of `plan` whose order no operator uses.
pub fn mark_scans_with_unused_order(plan: PlanRef) -> PlanRef {
    rewrite(&plan, true).0
}

/// Rewrites `node`, whose output order is used by its parent when `order_used`. Returns the new
/// node and whether it changed.
fn rewrite(node: &PlanRef, order_used: bool) -> (PlanRef, bool) {
    if let Some(scan) = node.as_batch_seq_scan() {
        if !order_used && !scan.order_unused() && !node.order().is_any() {
            return (scan.clone().with_order_unused().into(), true);
        }
        return (node.clone(), false);
    }
    let input_order_used = input_order_used(node, order_used);
    let mut changed = false;
    let mut inputs = Vec::new();
    for input in node.inputs() {
        let (new_input, input_changed) = rewrite(&input, input_order_used);
        changed |= input_changed;
        inputs.push(new_input);
    }
    if changed {
        (node.clone_root_with_inputs(&inputs), true)
    } else {
        (node.clone(), false)
    }
}

/// Whether `node` may use the order of its inputs, given whether its parent uses its own order.
fn input_order_used(node: &PlanRef, order_used: bool) -> bool {
    // An exchange without an order concatenates its inputs: only a merging exchange keeps it.
    if node.as_batch_exchange().is_some() {
        return !node.order().is_any();
    }
    // They do not depend on the order of their inputs, and their output has no order (a union),
    // or one of its own (a sort, a top-n).
    if node.as_batch_hash_join().is_some()
        || node.as_batch_union().is_some()
        || node.as_batch_sort().is_some()
        || node.as_batch_top_n().is_some()
    {
        return false;
    }
    // Aggregate functions with an `ORDER BY` are kept on the safe side.
    if let Some(agg) = node.as_batch_hash_agg() {
        return agg.agg_calls().iter().any(|call| !call.order_by.is_empty());
    }
    if let Some(agg) = node.as_batch_simple_agg() {
        return agg.agg_calls().iter().any(|call| !call.order_by.is_empty());
    }
    // They keep the order of their input: they need it as much as their parent.
    if node.as_batch_project().is_some() || node.as_batch_filter().is_some() {
        return order_used;
    }
    true
}
