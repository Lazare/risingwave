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
use risingwave_pb::batch_plan::SpoolNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use smallvec::SmallVec;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNode, ToBatchPb, ToDistributedBatch,
};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;

/// `BatchSpool` reads a sub-plan that several nodes of the same stage compute identically
/// (`batch_share_identical_subplans`, see `share_identical_batch_subplans`).
///
/// The *producer* has the sub-plan as its input. The *readers* have no input: at execution, each
/// task computes the sub-plan once, when the first of the producer and the readers with the same
/// `spool_id` is read, keeps its rows (spilled to disk when the memory of the query is short),
/// and gives every one of them all the rows, in the same order.
///
/// The node only exists in the final batch plan: it is added after the distribution of the plan
/// and cannot be converted again.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSpool {
    pub base: PlanBase<Batch>,
    spool_id: u32,
    /// The sub-plan for the producer, `None` for a reader.
    input: Option<PlanRef>,
}

impl BatchSpool {
    /// The node that computes `input` for the spool `spool_id`.
    pub fn new_producer(spool_id: u32, input: PlanRef) -> Self {
        let base = input.plan_base().clone_with_new_plan_id();
        Self {
            base,
            spool_id,
            input: Some(input),
        }
    }

    /// A node that reads the rows of the spool `spool_id`, in place of `copy`, a sub-plan equal
    /// to the input of its producer. It keeps the properties of `copy`.
    pub fn new_reader(spool_id: u32, copy: &PlanRef) -> Self {
        let base = copy.plan_base().clone_with_new_plan_id();
        Self {
            base,
            spool_id,
            input: None,
        }
    }

    pub fn spool_id(&self) -> u32 {
        self.spool_id
    }

    /// The sub-plan of a producer, `None` for a reader.
    pub fn producer_input(&self) -> Option<PlanRef> {
        self.input.clone()
    }
}

impl PlanTreeNode<Batch> for BatchSpool {
    fn inputs(&self) -> SmallVec<[PlanRef; 2]> {
        self.input.iter().cloned().collect()
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        match (&self.input, inputs) {
            (Some(_), [input]) => Self::new_producer(self.spool_id, input.clone()).into(),
            (None, []) => self.clone().into(),
            _ => panic!(
                "a spool producer has one input and a spool reader none, got {} inputs",
                inputs.len()
            ),
        }
    }
}

impl Distill for BatchSpool {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let name = if self.input.is_some() {
            "BatchSpool"
        } else {
            "BatchSpoolRead"
        };
        childless_record(name, vec![("spool", Pretty::display(&self.spool_id))])
    }
}

impl ToDistributedBatch for BatchSpool {
    fn to_distributed(&self) -> Result<PlanRef> {
        unreachable!("a spool is only added to the final batch plan")
    }
}

impl ToLocalBatch for BatchSpool {
    fn to_local(&self) -> Result<PlanRef> {
        unreachable!("a spool is only added to the final batch plan")
    }
}

impl ToBatchPb for BatchSpool {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Spool(SpoolNode {
            spool_id: self.spool_id,
            schema: self.base.schema().to_prost(),
        })
    }
}

impl ExprRewritable<Batch> for BatchSpool {}

impl ExprVisitable for BatchSpool {}
