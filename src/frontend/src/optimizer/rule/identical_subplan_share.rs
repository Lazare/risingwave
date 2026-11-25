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

//! Sharing, late in the streaming optimizer, of the sub-plans that became identical.
//!
//! The common sub-plan sharing of the streaming optimizer runs before predicate pushdown, subquery
//! unnesting and column pruning. Two sub-plans that only become identical after them (the same
//! join with a filter written above one copy and pushed into the other, the same domain of two
//! subqueries, an aggregation of a view whose unused aggregates were pruned) are computed twice:
//! two states, two backfills.
//!
//! [`share_identical_subplans`] shares them again, at the end of the streaming optimizer, with the
//! same structural equality as the early sharing (column names are ignored). What it shares is
//! narrower than the early sharing, because a share is a fragment boundary (a no-shuffle exchange
//! to every consumer):
//!
//! - only a sub-plan with a stateful operator (join, aggregation, top-n, dedup, over window,
//!   limit) that reads a table or a source. Removing a copy then removes its state, its actors and
//!   its backfills. A chain of scan, filter and projection costs no state; sharing it trades one
//!   backfill for one more fragment between the scan and each consumer, so it is left duplicated,
//!   like the early sharing leaves a bare scan;
//! - a share that existed before is kept as it is, even with a single consumer: a plan without a
//!   new duplicate is returned unchanged.
//!
//! Correctness: the consumers of a share read the same stream as they would read from their own
//! copy, at the same epochs; a top-n or dedup that picks one row among ties picks the same row for
//! every consumer instead of possibly different ones, which both plans allow.

use std::collections::{HashMap, HashSet};

use super::prelude::PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    EndoPlan, LogicalShare, PlanNodeId, PlanTreeNodeUnary, VisitPlan,
};
use crate::utils::{Endo, Visit};

/// Shares the identical sub-plans of `plan` that are worth sharing (see the module
/// documentation). Returns the new plan and the number of new shares; with no new share, `plan`
/// itself is returned.
#[allow(clippy::mutable_key_type)]
pub fn share_identical_subplans(plan: PlanRef) -> (PlanRef, usize) {
    let mut merger = Merger::default();
    let merged = merger.apply(plan.clone());

    let mut counter = Counter::default();
    counter.visit(&merged);

    // The new shares with several consumers whose sub-plan is worth sharing.
    let new_shares: HashSet<PlanNodeId> = counter
        .counts
        .iter()
        .filter(|(id, count)| {
            **count >= 2 && !merger.kept.contains(*id) && is_worth_sharing(&counter.inputs[*id])
        })
        .map(|(id, _)| *id)
        .collect();
    if new_shares.is_empty() {
        return (plan, 0);
    }
    let count = new_shares.len();
    let mut keep = merger.kept;
    keep.extend(new_shares);
    let mut pruner = Pruner {
        keep: &keep,
        cache: HashMap::new(),
    };
    (pruner.apply(merged), count)
}

/// Whether a sub-plan holds a stateful operator and reads a table or a source.
fn is_worth_sharing(plan: &PlanRef) -> bool {
    let mut has_state = false;
    let mut reads = false;
    let mut visited = HashSet::new();
    let mut stack = vec![plan.clone()];
    while let Some(node) = stack.pop() {
        if !visited.insert(node.id()) {
            continue;
        }
        has_state |= node.as_logical_join().is_some()
            || node.as_logical_agg().is_some()
            || node.as_logical_top_n().is_some()
            || node.as_logical_dedup().is_some()
            || node.as_logical_over_window().is_some()
            || node.as_logical_limit().is_some();
        reads |= node.as_logical_scan().is_some() || node.as_logical_source().is_some();
        if has_state && reads {
            return true;
        }
        stack.extend(node.inputs());
    }
    false
}

/// Wraps every node in a share, the same share for structurally equal nodes. A share of the input
/// plan is not wrapped again: it is replaced with the share of its input, which is recorded in
/// `kept` so that it survives the pruning. Shares of the input plan are never modified, so that
/// the input plan stays valid, and so are the keys of `cache`, which are nodes of the input plan.
#[derive(Default)]
#[allow(clippy::mutable_key_type)]
struct Merger {
    cache: HashMap<PlanRef, LogicalShare>,
    /// The result for each share of the input plan, so that its input is looked up once.
    shares: HashMap<PlanNodeId, PlanRef>,
    kept: HashSet<PlanNodeId>,
}

#[allow(clippy::mutable_key_type)]
impl Endo<PlanRef> for Merger {
    fn apply(&mut self, t: PlanRef) -> PlanRef {
        if let Some(share) = t.as_logical_share() {
            if let Some(shared) = self.shares.get(&share.id()) {
                return shared.clone();
            }
            let shared = self.apply(share.input());
            self.kept.insert(shared.id());
            self.shares.insert(share.id(), shared.clone());
            return shared;
        }
        if let Some(share) = self.cache.get(&t) {
            return share.clone().into();
        }
        let share = LogicalShare::new(self.tree_apply(t.clone()));
        self.cache.entry(t).or_insert(share).clone().into()
    }
}

/// Counts the consumers of every share, visiting the input of a share once.
#[derive(Default)]
struct Counter {
    counts: HashMap<PlanNodeId, u64>,
    inputs: HashMap<PlanNodeId, PlanRef>,
}

impl VisitPlan for Counter {
    fn visited<F>(&mut self, plan: &PlanRef, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        if self.counts.get(&plan.id()).is_none_or(|c| *c <= 1) {
            f(self);
        }
    }
}

impl Visit<PlanRef> for Counter {
    fn visit(&mut self, t: &PlanRef) {
        if let Some(share) = t.as_logical_share() {
            *self.counts.entry(share.id()).or_insert(0) += 1;
            self.inputs
                .entry(share.id())
                .or_insert_with(|| share.input());
        }
        self.dag_visit(t);
    }
}

/// Removes the shares that are not in `keep`.
struct Pruner<'a> {
    keep: &'a HashSet<PlanNodeId>,
    cache: HashMap<PlanNodeId, PlanRef>,
}

impl EndoPlan for Pruner<'_> {
    fn cached<F>(&mut self, plan: PlanRef, mut f: F) -> PlanRef
    where
        F: FnMut(&mut Self) -> PlanRef,
    {
        if let Some(result) = self.cache.get(&plan.id()) {
            return result.clone();
        }
        let result = f(self);
        self.cache.entry(plan.id()).or_insert(result).clone()
    }
}

impl Endo<PlanRef> for Pruner<'_> {
    fn pre(&mut self, t: PlanRef) -> PlanRef {
        let pruned_input = t
            .as_logical_share()
            .filter(|share| !self.keep.contains(&share.id()))
            .map(|share| share.input());
        match pruned_input {
            Some(input) => self.pre(input),
            None => t,
        }
    }

    fn apply(&mut self, t: PlanRef) -> PlanRef {
        self.dag_apply(t)
    }
}
