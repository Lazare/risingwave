// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::plan_common::JoinType;

use super::keeps_narrowing_projection_below_join;
use super::prelude::{PlanRef, *};
use crate::expr::{ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalJoin, LogicalProject};
use crate::utils::IndexRewriter;

/// Before this rule:
/// `join(project_a(t1),project_b(t2))`
///
/// After this rule:
/// `new_project(new_join(t1,t2))`
/// `new_join` is a full out join.
/// `new_project` is a projection combine `project_a` and `project_b` and it only output the
/// columns that are output in the original `join`.
///
/// A right semi or anti join only outputs its right columns, numbered from 0 (its output
/// indices index the right side alone), so the expressions of a projection pulled up from its
/// right side read the columns of `new_join` from 0, not after the left columns.
pub struct JoinProjectTransposeRule {}

impl Rule<Logical> for JoinProjectTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;

        let (left, right, on, join_type, _) = join.clone().decompose();

        let (left_input_index_on_condition, right_input_index_on_condition) =
            join.input_idx_on_condition();

        let full_output_len = left.schema().len() + right.schema().len();
        let right_output_len = right.schema().len();
        let left_output_len = left.schema().len();
        let mut full_proj_exprs = Vec::with_capacity(full_output_len);

        let mut old_i2new_i = ColIndexMapping::empty(0, 0);

        let mut has_new_left: bool = false;
        let mut has_new_right: bool = false;

        // The output of a right semi or anti join only holds its right columns: the expressions
        // pulled up from the right side read them from 0, and the output indices of the join
        // index the right side alone (that is, the entries of `full_proj_exprs` after the left
        // ones).
        let right_only_output = matches!(join_type, JoinType::RightSemi | JoinType::RightAnti);

        // prepare for pull up left child.
        let new_left = if let Some(project) = left.as_logical_project()
            && left_input_index_on_condition
                .iter()
                .all(|index| project.exprs()[*index].as_input_ref().is_some())
            && join_type != JoinType::RightAnti
            && join_type != JoinType::RightSemi
            && join_type != JoinType::RightOuter
            && join_type != JoinType::FullOuter
            // Rule `NarrowValuePushRule`: keep a projection that narrows a value below the join.
            && !keeps_narrowing_projection_below_join(project)
        {
            let (exprs, child) = project.clone().decompose();

            old_i2new_i = old_i2new_i.union(
                &join
                    .i2l_col_mapping_ignore_join_type()
                    .composite(&project.o2i_col_mapping()),
            );

            full_proj_exprs.extend(exprs);

            has_new_left = true;

            child
        } else {
            old_i2new_i = old_i2new_i.union(&join.i2l_col_mapping_ignore_join_type());

            for i in 0..left_output_len {
                full_proj_exprs.push(
                    InputRef {
                        index: i,
                        data_type: left.schema().data_types()[i].clone(),
                    }
                    .into(),
                );
            }

            left
        };

        // prepare for pull up right child.
        let new_right = if let Some(project) = right.as_logical_project()
            && right_input_index_on_condition
                .iter()
                .all(|index| project.exprs()[*index].as_input_ref().is_some())
            && join_type != JoinType::LeftAnti
            && join_type != JoinType::LeftSemi
            && join_type != JoinType::LeftOuter
            && join_type != JoinType::FullOuter
            // Rule `NarrowValuePushRule`: keep a projection that narrows a value below the join.
            && !keeps_narrowing_projection_below_join(project)
        {
            let (exprs, child) = project.clone().decompose();

            old_i2new_i = old_i2new_i.union(
                &join
                    .i2r_col_mapping_ignore_join_type()
                    .composite(&project.o2i_col_mapping())
                    .clone_with_offset(new_left.schema().len()),
            );

            let right_offset = if right_only_output {
                0
            } else {
                new_left.schema().len()
            };
            let mut index_writer = IndexRewriter::new(
                ColIndexMapping::identity(child.schema().len()).clone_with_offset(right_offset),
            );
            full_proj_exprs.extend(
                exprs
                    .into_iter()
                    .map(|expr| index_writer.rewrite_expr(expr)),
            );

            has_new_right = true;

            child
        } else {
            old_i2new_i = old_i2new_i.union(
                &join
                    .i2r_col_mapping_ignore_join_type()
                    .clone_with_offset(new_left.schema().len()),
            );

            let right_offset = if right_only_output {
                0
            } else {
                new_left.schema().len()
            };
            for i in 0..right_output_len {
                full_proj_exprs.push(
                    InputRef {
                        index: i + right_offset,
                        data_type: right.schema().data_types()[i].clone(),
                    }
                    .into(),
                );
            }

            right
        };

        // No project will be pulled up
        if !has_new_left && !has_new_right {
            return None;
        }

        let new_cond = on.rewrite_expr(&mut IndexRewriter::new(old_i2new_i));
        let new_join = LogicalJoin::new(new_left, new_right, join_type, new_cond);

        // remain only the columns that are output in the original join
        let output_offset = if right_only_output {
            left_output_len
        } else {
            0
        };
        let new_proj_exprs = join
            .output_indices()
            .iter()
            .map(|i| full_proj_exprs[output_offset + *i].clone())
            .collect_vec();
        let new_project = LogicalProject::new(new_join.into(), new_proj_exprs);

        Some(new_project.into())
    }
}

impl JoinProjectTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(JoinProjectTransposeRule {})
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{ExprImpl, FunctionCall, assert_eq_input_ref};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::optimizer::plan_node::generic::GenericPlanRef;
    use crate::utils::Condition;

    /// A `join_type` join of a 1-column left side `l0` and of the projection `r0, r1::varchar`
    /// of a right side `r0, r1` (a big int), on `l0 = r0`.
    fn join_over_right_projection(join_type: JoinType) -> PlanRef {
        let ctx = OptimizerContext::mock();
        let left = LogicalValues::new(
            vec![],
            Schema::new(vec![Field::with_name(DataType::Int32, "l0")]),
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema::new(vec![
                Field::with_name(DataType::Int32, "r0"),
                Field::with_name(DataType::Int64, "r1"),
            ]),
            ctx,
        );
        let cast = ExprImpl::from(InputRef::new(1, DataType::Int64))
            .cast_explicit(&DataType::Varchar)
            .unwrap();
        let project = LogicalProject::new(
            right.into(),
            vec![InputRef::new(0, DataType::Int32).into(), cast],
        );
        let on = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    InputRef::new(0, DataType::Int32).into(),
                    InputRef::new(1, DataType::Int32).into(),
                ],
            )
            .unwrap(),
        ));
        LogicalJoin::new(
            left.into(),
            project.into(),
            join_type,
            Condition::with_expr(on),
        )
        .into()
    }

    /// The projection pulled up from the right side reads the right columns of the new join:
    /// after the left column for the joins that output both sides, from 0 for the right semi and
    /// anti joins, which only output their right side. It used to read them after the left
    /// columns for every join type, which dropped the cast of a right semi or anti join (and read
    /// another column).
    #[test]
    fn test_pull_up_right_projection_keeps_the_output() {
        for join_type in [
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::Inner,
            JoinType::RightOuter,
        ] {
            let join = join_over_right_projection(join_type);
            let pulled = JoinProjectTransposeRule::create()
                .apply(join.clone())
                .unwrap();
            assert!(join.schema().type_eq(pulled.schema()), "{join_type:?}");
            let project = pulled.as_logical_project().unwrap();
            let exprs = project.exprs();
            let right_offset = match join_type {
                JoinType::RightSemi | JoinType::RightAnti => 0,
                _ => 1,
            };
            assert_eq!(exprs.len(), right_offset + 2, "{join_type:?}");
            assert_eq_input_ref!(&exprs[right_offset], right_offset);
            let cast = exprs[right_offset + 1].as_function_call().unwrap();
            assert_eq!(cast.func_type(), Type::Cast, "{join_type:?}");
            assert_eq_input_ref!(&cast.inputs()[0], right_offset + 1);
        }
    }
}
