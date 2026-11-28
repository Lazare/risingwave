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

use std::cmp::max;

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, Scalar};

use super::utils::impl_distill_by_unit;
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, PlanBase, PredicatePushdown,
    ToBatch, ToStream,
};
use crate::Explain;
use crate::error::Result;
use crate::expr::{ExprImpl, InputRef, Literal};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::stream_union::StreamUnion;
use crate::optimizer::plan_node::{
    BatchHashAgg, BatchUnion, ColumnPruningContext, LogicalProject, PlanTreeNode,
    PlanTreeNodeUnary, PredicatePushdownContext, RewriteStreamContext, ToStreamContext, generic,
};
use crate::optimizer::property::{Distribution, RequiredDist};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalUnion` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalUnion {
    pub base: PlanBase<Logical>,
    core: generic::Union<PlanRef>,
}

impl LogicalUnion {
    pub fn new(all: bool, inputs: Vec<PlanRef>) -> Self {
        assert!(Schema::all_type_eq(inputs.iter().map(|x| x.schema())));
        Self::new_with_source_col(all, inputs, None)
    }

    /// It is used by streaming processing. We need to use `source_col` to identify the record came
    /// from which source input.
    pub fn new_with_source_col(all: bool, inputs: Vec<PlanRef>, source_col: Option<usize>) -> Self {
        let core = generic::Union {
            all,
            inputs,
            source_col,
        };
        let base = PlanBase::new_logical_with_core(&core);
        LogicalUnion { base, core }
    }

    pub fn create(all: bool, inputs: Vec<PlanRef>) -> PlanRef {
        LogicalUnion::new(all, inputs).into()
    }

    pub fn all(&self) -> bool {
        self.core.all
    }

    pub fn source_col(&self) -> Option<usize> {
        self.core.source_col
    }

    /// `streaming_distributed_union`: the distribution key by which the inputs of this union are
    /// shuffled to satisfy `required` directly, instead of being shuffled by the stream key of the
    /// union and the union then shuffled again by its parent. `None` keeps the default plan.
    ///
    /// The key must be a subset of the stream key of the union: every change of a row of the
    /// union (identified by its stream key) then has the same key, hence goes through the same
    /// exchange channel to the same actor, in order, as with the default shuffle by the whole
    /// stream key (which is the case `key == stream key`). The union then also satisfies the
    /// invariant asserted by `StreamExchange::new`, a distribution key inside the stream key.
    fn distributed_target(&self, required: &RequiredDist) -> Option<Vec<usize>> {
        if !streaming_distributed_union_enabled(&self.base.ctx())
            || !self.all()
            || self.source_col().is_none()
        {
            return None;
        }
        let stream_key = self.base.stream_key()?;
        // The default distribution already satisfies the requirement: no exchange to remove.
        if Distribution::HashShard(stream_key.to_vec()).satisfies(required) {
            return None;
        }
        // The distribution that the parent would enforce above the union (`RequiredDist::to_dist`).
        let key = match required {
            RequiredDist::ShardByKey(cols) | RequiredDist::ShardByExactKey(cols) => {
                cols.ones().collect_vec()
            }
            RequiredDist::PhysicalDist(Distribution::HashShard(key)) => key.clone(),
            _ => return None,
        };
        if key.is_empty() || !key.iter().all(|col| stream_key.contains(col)) {
            return None;
        }
        Some(key)
    }
}

/// Whether `streaming_distributed_union` applies to this streaming job (not to the plan built
/// without the prototype's rewrites to report what they saved).
fn streaming_distributed_union_enabled(ctx: &crate::optimizer::OptimizerContextRef) -> bool {
    ctx.session_ctx().config().streaming_distributed_union() && !ctx.is_stream_prune_baseline()
}

/// `streaming_distributed_union`: whether `plan`, a union possibly below projections, would
/// satisfy `required` by shuffling its inputs directly (see `LogicalUnion::distributed_target`).
/// The projections pass the requirement down through their column mapping.
pub(crate) fn union_takes_required_dist(plan: &PlanRef, required: &RequiredDist) -> bool {
    if !streaming_distributed_union_enabled(&plan.ctx()) {
        return false;
    }
    if let Some(union) = plan.as_logical_union() {
        return union.distributed_target(required).is_some();
    }
    if let Some(project) = plan.as_logical_project() {
        return union_takes_required_dist(
            &project.input(),
            &project.input_required_dist_for_union(required),
        );
    }
    false
}

/// Converts the input of a parent that shuffles it by `required` itself after `to_stream`. With
/// `streaming_distributed_union` and a union (possibly below projections) that can satisfy
/// `required` by shuffling its inputs directly, the requirement is passed down to the union; any
/// other input is converted as before. The parent still enforces `required` on the result.
pub(crate) fn input_to_stream_for_required_dist(
    input: &PlanRef,
    required: &RequiredDist,
    ctx: &mut ToStreamContext,
) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
    if union_takes_required_dist(input, required) {
        input.to_stream_with_dist_required(required, ctx)
    } else {
        input.to_stream(ctx)
    }
}

impl PlanTreeNode<Logical> for LogicalUnion {
    fn inputs(&self) -> smallvec::SmallVec<[PlanRef; 2]> {
        self.core.inputs.clone().into_iter().collect()
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        Self::new_with_source_col(self.all(), inputs.to_vec(), self.core.source_col).into()
    }
}

impl_distill_by_unit!(LogicalUnion, core, "LogicalUnion");

impl ColPrunable for LogicalUnion {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // A `UNION` without `ALL` removes the rows that are equal on every column: the columns that
        // the parent does not read still decide which rows are duplicates, so none is pruned below
        // it (this matters when the columns are pruned before the union becomes an aggregation).
        if !self.all() {
            let all_cols = (0..self.schema().len()).collect_vec();
            let new_inputs = self
                .inputs()
                .iter()
                .map(|input| input.prune_col(&all_cols, ctx))
                .collect_vec();
            let set_op = self.clone_with_inputs(&new_inputs);
            if required_cols == all_cols.as_slice() {
                return set_op;
            }
            return LogicalProject::with_out_col_idx(set_op, required_cols.iter().copied()).into();
        }
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.prune_col(required_cols, ctx))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl ExprRewritable<Logical> for LogicalUnion {}

impl ExprVisitable for LogicalUnion {}

impl PredicatePushdown for LogicalUnion {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.predicate_pushdown(predicate.clone(), ctx))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl ToBatch for LogicalUnion {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.to_batch())
            .try_collect()?;
        let new_logical = generic::Union {
            all: true,
            inputs: new_inputs,
            source_col: None,
        };
        // We still need to handle !all even if we already have `UnionToDistinctRule`, because it
        // can be generated by index selection which is an optimization during the `to_batch`.
        // Convert union to union all + agg
        if !self.all() {
            let batch_union = BatchUnion::new(new_logical).into();
            Ok(BatchHashAgg::new(
                generic::Agg::new(vec![], (0..self.base.schema().len()).collect(), batch_union)
                    .with_enable_two_phase(false),
            )
            .into())
        } else {
            Ok(BatchUnion::new(new_logical).into())
        }
    }
}

impl ToStream for LogicalUnion {
    fn to_stream(
        &self,
        ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        // TODO: use round robin distribution instead of using hash distribution of all inputs.
        let dist = RequiredDist::hash_shard(self.base.stream_key().unwrap_or_else(|| {
            panic!(
                "should always have a stream key in the stream plan but not, sub plan: {}",
                PlanRef::from(self.clone()).explain_to_string()
            )
        }));
        let new_inputs: Result<Vec<_>> = self
            .inputs()
            .iter()
            .map(|input| input.to_stream_with_dist_required(&dist, ctx))
            .collect();
        let core = self.core.clone_with_inputs(new_inputs?);
        assert!(
            self.all(),
            "After UnionToDistinctRule, union should become union all"
        );
        Ok(StreamUnion::new(core).into())
    }

    fn to_stream_with_dist_required(
        &self,
        required_dist: &RequiredDist,
        ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        // `streaming_distributed_union`: shuffle each input by the key the parent needs, so that
        // the union is distributed as the parent requires, without an exchange above it.
        let Some(key) = self.distributed_target(required_dist) else {
            let plan = self.to_stream(ctx)?;
            return required_dist.streaming_enforce_if_not_satisfies(plan);
        };
        // The inputs have the schema of the union: the key designates the same columns in each.
        let input_dist = RequiredDist::hash_shard(&key);
        let new_inputs: Vec<_> = self
            .inputs()
            .iter()
            .map(|input| input.to_stream_with_dist_required(&input_dist, ctx))
            .try_collect()?;
        let core = self.core.clone_with_inputs(new_inputs);
        // `StreamUnion::new` checks that every input is distributed as `HashShard(key)`.
        let plan: crate::optimizer::plan_node::StreamPlanRef = StreamUnion::new(core).into();
        self.base.ctx().add_stream_prune_rule_applied(1);
        required_dist.streaming_enforce_if_not_satisfies(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        type FixedState = std::hash::BuildHasherDefault<std::hash::DefaultHasher>;
        type TypeMap<T> = std::collections::HashMap<DataType, T, FixedState>;

        let original_schema = self.base.schema().clone();
        let original_schema_len = original_schema.len();
        let mut rewrites = vec![];
        for input in &self.core.inputs {
            rewrites.push(input.logical_rewrite_for_stream(ctx)?);
        }

        let original_schema_contain_all_input_stream_keys =
            rewrites.iter().all(|(new_input, col_index_mapping)| {
                let original_schema_new_pos = (0..original_schema_len)
                    .map(|x| col_index_mapping.map(x))
                    .collect_vec();
                new_input
                    .expect_stream_key()
                    .iter()
                    .all(|x| original_schema_new_pos.contains(x))
            });

        if original_schema_contain_all_input_stream_keys {
            // Add one more column at the end of the original schema to identify the record came
            // from which input. [original_schema + source_col]
            let new_inputs = rewrites
                .into_iter()
                .enumerate()
                .map(|(i, (new_input, col_index_mapping))| {
                    // original_schema
                    let mut exprs = (0..original_schema_len)
                        .map(|x| {
                            ExprImpl::InputRef(
                                InputRef::new(
                                    col_index_mapping.map(x),
                                    original_schema.fields[x].data_type.clone(),
                                )
                                .into(),
                            )
                        })
                        .collect_vec();
                    // source_col
                    exprs.push(ExprImpl::Literal(
                        Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32).into(),
                    ));
                    LogicalProject::create(new_input, exprs)
                })
                .collect_vec();
            let new_union = LogicalUnion::new_with_source_col(
                self.all(),
                new_inputs,
                Some(original_schema_len),
            );
            // We have already used project to map rewrite input to the origin schema, so we can use
            // identity with the new schema len.
            let out_col_change =
                ColIndexMapping::identity_or_none(original_schema_len, new_union.schema().len());
            Ok((new_union.into(), out_col_change))
        } else {
            // In order to ensure all inputs have the same schema for new union, we construct new
            // schema like that: [original_schema + merged_stream_key + source_col]
            // where merged_stream_key is merged by the types of each input stream key.
            // If all inputs have the same stream key column types, we have a small merged_stream_key. Otherwise, we will have a large merged_stream_key.

            let (merged_stream_key_types, types_offset) = {
                let mut max_types_counter = TypeMap::default();
                for (new_input, _) in &rewrites {
                    let mut types_counter = TypeMap::default();
                    for x in new_input.expect_stream_key() {
                        types_counter
                            .entry(new_input.schema().fields[*x].data_type())
                            .and_modify(|x| *x += 1)
                            .or_insert(1);
                    }
                    for (key, val) in types_counter {
                        max_types_counter
                            .entry(key)
                            .and_modify(|x| *x = max(*x, val))
                            .or_insert(val);
                    }
                }

                let mut merged_stream_key_types = vec![];
                let mut types_offset = TypeMap::default();
                let mut offset = 0;
                for (key, val) in max_types_counter {
                    let _ = types_offset.insert(key.clone(), offset);
                    offset += val;
                    merged_stream_key_types.extend(std::iter::repeat_n(key.clone(), val));
                }

                (merged_stream_key_types, types_offset)
            };

            let input_stream_key_nulls = merged_stream_key_types
                .iter()
                .map(|t| ExprImpl::Literal(Literal::new(None, t.clone()).into()))
                .collect_vec();

            let new_inputs = rewrites
                .into_iter()
                .enumerate()
                .map(|(i, (new_input, col_index_mapping))| {
                    // original_schema
                    let mut exprs = (0..original_schema_len)
                        .map(|x| {
                            ExprImpl::InputRef(
                                InputRef::new(
                                    col_index_mapping.map(x),
                                    original_schema.fields[x].data_type.clone(),
                                )
                                .into(),
                            )
                        })
                        .collect_vec();
                    // merged_stream_key
                    let mut input_stream_keys = input_stream_key_nulls.clone();
                    let mut types_counter = TypeMap::default();
                    for stream_key_idx in new_input.expect_stream_key() {
                        let data_type =
                            new_input.schema().fields[*stream_key_idx].data_type.clone();
                        let count = *types_counter
                            .entry(data_type.clone())
                            .and_modify(|x| *x += 1)
                            .or_insert(1);
                        let type_start_offset = *types_offset.get(&data_type).unwrap();

                        input_stream_keys[type_start_offset + count - 1] =
                            ExprImpl::InputRef(InputRef::new(*stream_key_idx, data_type).into());
                    }
                    exprs.extend(input_stream_keys);
                    // source_col
                    exprs.push(ExprImpl::Literal(
                        Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32).into(),
                    ));
                    LogicalProject::create(new_input, exprs)
                })
                .collect_vec();

            let new_union = LogicalUnion::new_with_source_col(
                self.all(),
                new_inputs,
                Some(original_schema_len + merged_stream_key_types.len()),
            );
            // We have already used project to map rewrite input to the origin schema, so we can use
            // identity with the new schema len.
            let out_col_change =
                ColIndexMapping::identity_or_none(original_schema_len, new_union.schema().len());
            Ok((new_union.into(), out_col_change))
        }
    }

    fn try_better_locality(&self, columns: &[usize]) -> Option<PlanRef> {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.try_better_locality(columns))
            .collect::<Option<Vec<PlanRef>>>()?;
        Some(self.clone_with_inputs(&new_inputs))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::Field;

    use super::*;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;

    #[tokio::test]
    async fn test_prune_union() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty, "v3"),
        ];
        let values1 = LogicalValues::new(vec![], Schema { fields }, ctx);

        let values2 = values1.clone();
        let required_cols = vec![1, 2];

        // `UNION ALL`: the inputs are pruned to the required columns.
        let union: PlanRef =
            LogicalUnion::new(true, vec![values1.clone().into(), values2.clone().into()]).into();
        let plan = union.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(union.clone()),
        );
        let union = plan.as_logical_union().unwrap();
        assert_eq!(union.base.schema().len(), 2);

        // `UNION`: every column decides which rows are duplicates, so the union keeps them all
        // and a projection above it outputs the required ones.
        let union: PlanRef = LogicalUnion::new(false, vec![values1.into(), values2.into()]).into();
        let plan = union.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(union.clone()),
        );
        assert_eq!(plan.schema().len(), 2);
        let project = plan.as_logical_project().unwrap();
        let union = project.input();
        let union = union.as_logical_union().unwrap();
        assert_eq!(union.base.schema().len(), 3);
    }

    #[tokio::test]
    async fn test_union_to_batch() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty, "v3"),
        ];
        let values1 = LogicalValues::new(vec![], Schema { fields }, ctx);

        let values2 = values1.clone();

        let union = LogicalUnion::new(false, vec![values1.into(), values2.into()]);

        let plan = union.to_batch().unwrap();
        let agg: &BatchHashAgg = plan.as_batch_hash_agg().unwrap();
        let agg_input = agg.input();
        let union = agg_input.as_batch_union().unwrap();

        assert_eq!(union.inputs().len(), 2);
    }
}
