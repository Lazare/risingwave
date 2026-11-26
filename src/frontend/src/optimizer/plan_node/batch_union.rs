// Copyright 2022 RisingWave Labs
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

use risingwave_pb::batch_plan::UnionNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    BatchExchange, BatchPlanNodeType, BatchPlanRef as PlanRef, ExprRewritable, ToBatchPb,
    ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{PlanBase, PlanTreeNode, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchUnion` implements [`super::LogicalUnion`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchUnion {
    pub base: PlanBase<Batch>,
    core: generic::Union<PlanRef>,
}

impl BatchUnion {
    pub fn new(core: generic::Union<PlanRef>) -> Self {
        let dist = if core
            .inputs
            .iter()
            .all(|input| *input.distribution() == Distribution::Single)
        {
            Distribution::Single
        } else {
            Distribution::SomeShard
        };

        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        BatchUnion { base, core }
    }

    /// A union whose inputs are all distributed as `dist` in the same stage, see
    /// [`Self::to_distributed_union`].
    fn new_with_dist(core: generic::Union<PlanRef>, dist: Distribution) -> Self {
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        BatchUnion { base, core }
    }

    /// The upstream plan: every input is gathered into a single task.
    fn to_single_union(&self, inputs: Vec<PlanRef>) -> Result<PlanRef> {
        let new_inputs: Result<Vec<_>> = inputs
            .into_iter()
            .map(|input| {
                RequiredDist::single().batch_enforce_if_not_satisfies(input, &Order::any())
            })
            .collect();
        Ok(self.clone_with_inputs(&new_inputs?))
    }

    /// `batch_distributed_union`: runs the union in a parallel stage instead of a single task.
    ///
    /// Each input is planned as before (`to_distributed`); only the exchange above it changes.
    /// All inputs are brought to one distribution `target`, the same for all of them, so that
    /// the union, in the stage of its parent, is distributed as `target`:
    /// - required `HashShard(k)` or `UpstreamHashShard(k, t)` (a join or a lookup join above):
    ///   that distribution, the exchange the parent would have put above the union;
    /// - required shard by some key (a hash aggregation, the first side of a hash join): the
    ///   `HashShard` that an input already has, if it satisfies the requirement, otherwise the
    ///   hash of the required key, as the parent would have done;
    /// - no requirement (a count, a top-n or a limit above, which then run in two phases): the
    ///   `HashShard` that an input already has, otherwise the hash of all the columns.
    ///
    /// An input already distributed as `target` stays where it is, without exchange, only when
    /// the part of its plan that would run in the stage of the union holds nothing that decides
    /// the tasks of a stage (a scan, a source, a lookup join) and nothing that must run in a
    /// single task (see [`colocatable`]). Every other input gets an exchange to `target`, which
    /// puts its plan in a stage of its own. A `UNION ALL` only concatenates its inputs: each row
    /// of each input reaches exactly one task of the union, so the result is the same multiset.
    ///
    /// Returns `None` to keep the single union: a single distribution required, a broadcast,
    /// all inputs already single (values, a global aggregation: nothing to parallelize), or no
    /// column to hash.
    fn to_distributed_union(
        &self,
        inputs: &[PlanRef],
        required_dist: &RequiredDist,
    ) -> Option<PlanRef> {
        if inputs
            .iter()
            .all(|input| *input.distribution() == Distribution::Single)
        {
            return None;
        }
        // An input that is already hash distributed in a way the requirement accepts.
        let input_hash_dist = || {
            inputs.iter().find_map(|input| match input.distribution() {
                dist @ Distribution::HashShard(_)
                    if dist.satisfies(required_dist) && colocatable(input) =>
                {
                    Some(dist.clone())
                }
                _ => None,
            })
        };
        let target = match required_dist {
            RequiredDist::PhysicalDist(
                dist @ (Distribution::HashShard(_) | Distribution::UpstreamHashShard(_, _)),
            ) => dist.clone(),
            RequiredDist::ShardByKey(key) | RequiredDist::ShardByExactKey(key) => {
                input_hash_dist().unwrap_or_else(|| Distribution::HashShard(key.ones().collect()))
            }
            RequiredDist::Any | RequiredDist::AnyShard => match input_hash_dist() {
                Some(dist) => dist,
                None if self.base.schema().is_empty() => return None,
                None => Distribution::HashShard((0..self.base.schema().len()).collect()),
            },
            RequiredDist::PhysicalDist(
                Distribution::Single | Distribution::Broadcast | Distribution::SomeShard,
            ) => return None,
        };
        let new_inputs = inputs
            .iter()
            .map(|input| {
                if *input.distribution() == target && colocatable(input) {
                    input.clone()
                } else {
                    BatchExchange::new(input.clone(), Order::any(), target.clone()).into()
                }
            })
            .collect();
        let mut core = self.core.clone();
        core.inputs = new_inputs;
        Some(Self::new_with_dist(core, target).into())
    }
}

/// Whether `plan`, already distributed as the union needs, can run in the stage of the union:
/// the nodes down to the exchanges (the part of `plan` that runs in that stage) only transform
/// the rows of their own task. A scan, a source or a lookup join decides the tasks of its stage
/// (the fragmenter expects one of them per stage), and a node distributed as `Single` would run
/// once per task of a parallel stage.
fn colocatable(plan: &PlanRef) -> bool {
    use BatchPlanNodeType as T;
    match plan.node_type() {
        T::BatchExchange => true,
        _ if *plan.distribution() == Distribution::Single => false,
        T::BatchProject
        | T::BatchFilter
        | T::BatchHashAgg
        | T::BatchSortAgg
        | T::BatchHashJoin
        | T::BatchUnion
        | T::BatchProjectSet
        | T::BatchExpand
        | T::BatchHopWindow
        | T::BatchOverWindow
        | T::BatchGroupTopN => plan.inputs().iter().all(colocatable),
        _ => false,
    }
}

/// Whether `batch_distributed_union` is on and `plan` is a union, possibly below projections
/// and filters. Those pass the distribution required by their parent down to the union.
pub(crate) fn passes_required_dist_to_union(plan: &PlanRef) -> bool {
    fn reaches_union(plan: &PlanRef) -> bool {
        match plan.node_type() {
            BatchPlanNodeType::BatchUnion => true,
            BatchPlanNodeType::BatchProject | BatchPlanNodeType::BatchFilter => {
                reaches_union(&plan.inputs()[0])
            }
            _ => false,
        }
    }
    plan.ctx().session_ctx().config().batch_distributed_union() && reaches_union(plan)
}

/// Converts the input of a parent that gathers it or hashes it itself after `to_distributed`.
/// With `batch_distributed_union` and a union below projections and filters, the required
/// distribution is passed down to the union instead; any other input is converted as before.
pub(crate) fn input_to_distributed_with_required(
    input: &PlanRef,
    required_dist: &RequiredDist,
) -> Result<PlanRef> {
    if passes_required_dist_to_union(input) {
        input.to_distributed_with_required(&Order::any(), required_dist)
    } else {
        required_dist.batch_enforce_if_not_satisfies(input.to_distributed()?, &Order::any())
    }
}

impl_distill_by_unit!(BatchUnion, core, "BatchUnion");

impl PlanTreeNode<Batch> for BatchUnion {
    fn inputs(&self) -> smallvec::SmallVec<[PlanRef; 2]> {
        smallvec::SmallVec::from_vec(self.core.inputs.clone())
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        // For batch query, we don't need to clone `source_col`, so just use new.
        let mut new = self.core.clone();
        new.inputs = inputs.to_vec();
        // Keep the distribution of a distributed union (`batch_distributed_union`) as long as
        // its inputs keep it. `new` only derives `Single` or `SomeShard`.
        let dist = self.base.distribution();
        if !matches!(dist, Distribution::Single | Distribution::SomeShard)
            && inputs.iter().all(|input| input.distribution() == dist)
        {
            return Self::new_with_dist(new, dist.clone()).into();
        }
        Self::new(new).into()
    }
}

impl ToDistributedBatch for BatchUnion {
    fn to_distributed(&self) -> Result<PlanRef> {
        self.to_distributed_with_required(&Order::any(), &RequiredDist::Any)
    }

    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &RequiredDist,
    ) -> Result<PlanRef> {
        let inputs: Vec<_> = self
            .inputs()
            .iter()
            .map(|input| input.to_distributed())
            .collect::<Result<_>>()?;
        let distributed = if self
            .base
            .ctx()
            .session_ctx()
            .config()
            .batch_distributed_union()
        {
            self.to_distributed_union(&inputs, required_dist)
        } else {
            None
        };
        let ret = match distributed {
            Some(plan) => plan,
            // TODO: use round robin distribution to improve it
            None => self.to_single_union(inputs)?,
        };
        let ret = required_order.enforce_if_not_satisfies(ret)?;
        required_dist.batch_enforce_if_not_satisfies(ret, required_order)
    }
}

impl ToBatchPb for BatchUnion {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Union(UnionNode {})
    }
}

impl ToLocalBatch for BatchUnion {
    fn to_local(&self) -> Result<PlanRef> {
        let new_inputs: Result<Vec<_>> = self
            .inputs()
            .iter()
            .map(|input| {
                RequiredDist::single()
                    .batch_enforce_if_not_satisfies(input.to_local()?, &Order::any())
            })
            .collect();
        Ok(self.clone_with_inputs(&new_inputs?))
    }
}

impl ExprRewritable<Batch> for BatchUnion {}

impl ExprVisitable for BatchUnion {}
