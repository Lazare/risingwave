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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::FieldDisplay;
use risingwave_pb::batch_plan::LimitDistinctNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNodeUnary, ToBatchPb,
    ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::ColIndexMappingRewriteExt;

/// `BatchLimitDistinct` (`batch_limit_distinct`) outputs the distinct values of the group key of
/// its input, as a hash aggregation without aggregate calls would, but at most `limit` of them:
/// it emits each value the first time it sees it and stops reading its input once it has emitted
/// `limit` values. It does not block, and keeps `limit` values in memory at most. It replaces such
/// an aggregation under a `LIMIT` (`BatchLimitDistinctRule`), which stays above it.
///
/// A `global` operator outputs values that are distinct over the whole query (the one right under
/// the limit). A local one (`global == false`) only outputs values distinct within its own task:
/// the copies put into the branches of a `UNION ALL` below a global one, and the first phase of a
/// global one over a distributed input.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchLimitDistinct {
    pub base: PlanBase<Batch>,
    core: generic::Agg<PlanRef>,
    limit: u64,
    global: bool,
}

impl BatchLimitDistinct {
    /// `core` is an aggregation with a group key and without aggregate calls or grouping sets.
    pub fn new(core: generic::Agg<PlanRef>, limit: u64, global: bool) -> Self {
        assert!(!core.group_key.is_empty());
        assert!(core.agg_calls.is_empty());
        assert!(core.grouping_sets.is_empty());
        let dist = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(core.input.distribution());
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        BatchLimitDistinct {
            base,
            core,
            limit,
            global,
        }
    }

    /// A global operator over a distributed input: a local operator in each task, a gather, and
    /// the global operator over the values gathered. Each task stops after `limit` values: if the
    /// input has `limit` distinct values, either one task has `limit` of them and emits `limit`
    /// values, or every task emits all its values; the gather holds `limit` distinct values in
    /// both cases (or all of them), and the global operator keeps `limit` of them.
    fn two_phase(&self, input: PlanRef) -> Result<PlanRef> {
        if !self.global
            || input.distribution().satisfies(&RequiredDist::single())
            || self
                .core
                .hash_agg_dist_satisfied_by_input_dist(input.distribution())
        {
            // A local operator, or a global one whose tasks hold disjoint values.
            return Ok(self.clone_with_input(input).into());
        }
        let mut local_core = self.core.clone();
        local_core.input = input;
        let local: PlanRef = Self::new(local_core, self.limit, false).into();
        let group_key_len = self.core.group_key.len();
        let gathered =
            RequiredDist::single().batch_enforce_if_not_satisfies(local, &Order::any())?;
        let global = generic::Agg::new(vec![], (0..group_key_len).collect(), gathered);
        Ok(Self::new(global, self.limit, true).into())
    }
}

impl PlanTreeNodeUnary<Batch> for BatchLimitDistinct {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core, self.limit, self.global)
    }
}

impl_plan_tree_node_for_unary! { Batch, BatchLimitDistinct }

impl Distill for BatchLimitDistinct {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let input_schema = self.core.input.schema();
        let group_key = Pretty::Array(
            self.core
                .group_key
                .indices()
                .map(|i| Pretty::display(&FieldDisplay(&input_schema.fields[i])))
                .collect(),
        );
        let mut fields = vec![
            ("group_key", group_key),
            ("limit", Pretty::debug(&self.limit)),
        ];
        if !self.global {
            fields.push(("per_task", Pretty::debug(&true)));
        }
        childless_record("BatchLimitDistinct", fields)
    }
}

impl ToDistributedBatch for BatchLimitDistinct {
    fn to_distributed(&self) -> Result<PlanRef> {
        self.two_phase(self.input().to_distributed()?)
    }
}

impl ToLocalBatch for BatchLimitDistinct {
    fn to_local(&self) -> Result<PlanRef> {
        self.two_phase(self.input().to_local()?)
    }
}

impl ToBatchPb for BatchLimitDistinct {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::LimitDistinct(LimitDistinctNode {
            group_key: self.core.group_key.indices().map(|i| i as u32).collect(),
            limit: self.limit,
        })
    }
}

impl ExprRewritable<Batch> for BatchLimitDistinct {}

impl ExprVisitable for BatchLimitDistinct {}
