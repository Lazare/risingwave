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

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::LogicalJoin;

/// Convert right join type to left join type:
///
/// `RightOuter` => `LeftOuter`
///
/// `RightSemi` => `LeftSemi`
///
/// `RightAnti` => `LeftAnti`
pub struct JoinCommuteRule {}
impl Rule<Logical> for JoinCommuteRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join: &LogicalJoin = plan.as_logical_join()?;
        let join_type = join.join_type();
        match join_type {
            JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti => {
                let (left, right, on, join_type, output_indices) = join.clone().decompose();

                let left_len = left.schema().len();
                let right_len = right.schema().len();

                let new_output_indices = swapped_output_indices(join_type, output_indices, left_len, right_len);

                let mut condition_rewriter = Rewriter {
                    join_left_len: left_len,
                    join_left_offset: right_len as isize,
                    join_right_offset: -(left_len as isize),
                };
                let new_on = on.rewrite_expr(&mut condition_rewriter);

                let new_join = LogicalJoin::with_output_indices(
                    right,
                    left,
                    Self::inverse_join_type(join_type),
                    new_on,
                    new_output_indices,
                );

                Some(new_join.into())
            }
            JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::FullOuter
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter
            | JoinType::Unspecified => None,
        }
    }
}

/// The same join with its sides swapped and the inverse join type (a left outer join becomes a
/// right outer join...), for `CostBasedOuterJoinSidesRule` (`batch_cost_based_join_ordering`).
/// The same rewrite as [`JoinCommuteRule`], for any join type but the as-of joins.
pub(super) fn swap_join_sides(join: &LogicalJoin) -> LogicalJoin {
    let (left, right, on, join_type, output_indices) = join.clone().decompose();
    let left_len = left.schema().len();
    let right_len = right.schema().len();
    let new_output_indices = swapped_output_indices(join_type, output_indices, left_len, right_len);
    let mut condition_rewriter = Rewriter {
        join_left_len: left_len,
        join_left_offset: right_len as isize,
        join_right_offset: -(left_len as isize),
    };
    let new_on = on.rewrite_expr(&mut condition_rewriter);
    LogicalJoin::with_output_indices(
        right,
        left,
        JoinCommuteRule::inverse_join_type(join_type),
        new_on,
        new_output_indices,
    )
}

/// The output indices of a join once its sides are swapped. The output of a semi or anti join
/// only holds the columns of the side it keeps, which stays the side kept once swapped (a left
/// semi join becomes a right semi join): its indices do not change. The output of the other joins
/// holds the columns of both sides, the left ones first.
fn swapped_output_indices(
    join_type: JoinType,
    output_indices: Vec<usize>,
    left_len: usize,
    right_len: usize,
) -> Vec<usize> {
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
            output_indices
        }
        _ => output_indices
            .into_iter()
            .map(|i| {
                if i < left_len {
                    i + right_len
                } else {
                    i - left_len
                }
            })
            .collect_vec(),
    }
}

struct Rewriter {
    join_left_len: usize,
    join_left_offset: isize,
    join_right_offset: isize,
}
impl ExprRewriter for Rewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index < self.join_left_len {
            InputRef::new(
                (input_ref.index() as isize + self.join_left_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        } else {
            InputRef::new(
                (input_ref.index() as isize + self.join_right_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        }
    }
}

impl JoinCommuteRule {
    pub fn create() -> BoxedRule {
        Box::new(JoinCommuteRule {})
    }

    fn inverse_join_type(join_type: JoinType) -> JoinType {
        match join_type {
            JoinType::Unspecified => JoinType::Unspecified,
            JoinType::Inner => JoinType::Inner,
            JoinType::LeftOuter => JoinType::RightOuter,
            JoinType::RightOuter => JoinType::LeftOuter,
            JoinType::FullOuter => JoinType::FullOuter,
            JoinType::LeftSemi => JoinType::RightSemi,
            JoinType::LeftAnti => JoinType::RightAnti,
            JoinType::RightSemi => JoinType::LeftSemi,
            JoinType::RightAnti => JoinType::LeftAnti,
            JoinType::AsofInner | JoinType::AsofLeftOuter => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{FunctionCall, assert_eq_input_ref};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::optimizer::plan_node::generic::GenericPlanRef;
    use crate::utils::Condition;

    /// A join of a 3-column left side `l0, l1, l2` and a 1-column right side `r0` on `l1 = r0`.
    fn join(join_type: JoinType, output_indices: Vec<usize>) -> (LogicalJoin, Vec<Field>) {
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = ["l0", "l1", "l2", "r0"]
            .iter()
            .map(|name| Field::with_name(DataType::Int32, *name))
            .collect();
        let left = LogicalValues::new(vec![], Schema::new(fields[0..3].to_vec()), ctx.clone());
        let right = LogicalValues::new(vec![], Schema::new(fields[3..4].to_vec()), ctx);
        let on = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                risingwave_pb::expr::expr_node::Type::Equal,
                vec![
                    InputRef::new(1, DataType::Int32).into(),
                    InputRef::new(3, DataType::Int32).into(),
                ],
            )
            .unwrap(),
        ));
        let join = LogicalJoin::with_output_indices(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
            output_indices,
        );
        (join, fields)
    }

    fn output_names(join: &LogicalJoin) -> Vec<String> {
        let plan: PlanRef = join.clone().into();
        plan.schema().fields().iter().map(|f| f.name.clone()).collect()
    }

    #[test]
    fn test_swap_semi_and_anti_joins_keeps_their_output() {
        for (join_type, swapped_type) in [
            (JoinType::LeftSemi, JoinType::RightSemi),
            (JoinType::LeftAnti, JoinType::RightAnti),
        ] {
            let (join, _) = join(join_type, vec![2, 0]);
            let swapped = swap_join_sides(&join);
            assert_eq!(swapped.join_type(), swapped_type);
            assert_eq!(output_names(&swapped), ["l2", "l0"]);
            // The condition now reads the right side first.
            let on: ExprImpl = swapped.on().clone().into();
            let call = on.as_function_call().unwrap();
            assert_eq_input_ref!(&call.inputs()[0], 2);
            assert_eq_input_ref!(&call.inputs()[1], 0);
            // Swapping back gives the first join again.
            let back = swap_join_sides(&swapped);
            assert_eq!(back.join_type(), join_type);
            assert_eq!(output_names(&back), ["l2", "l0"]);
        }
    }

    #[test]
    fn test_swap_inner_and_outer_joins_keeps_their_output() {
        for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::FullOuter] {
            let (join, _) = join(join_type, vec![3, 0, 2]);
            let swapped = swap_join_sides(&join);
            assert_eq!(output_names(&swapped), ["r0", "l0", "l2"]);
        }
    }

    #[test]
    fn test_commute_right_semi_join_keeps_its_output() {
        let (join, _) = join(JoinType::RightSemi, vec![0]);
        let plan = JoinCommuteRule::create().apply(join.into()).unwrap();
        let commuted = plan.as_logical_join().unwrap();
        assert_eq!(commuted.join_type(), JoinType::LeftSemi);
        assert_eq!(output_names(commuted), ["r0"]);
    }
}
