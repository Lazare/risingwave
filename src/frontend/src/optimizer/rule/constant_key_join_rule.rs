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

use risingwave_pb::plan_common::JoinType;

use super::column_facts_simplify_rule::{ColumnFacts, column_facts, expr_facts};
use super::cost_based_join_ordering_rule::estimated_rows;
use super::join_commute_rule::swap_join_sides;
use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprType, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalJoin, PlanTreeNodeBinary};
use crate::utils::Condition;

/// Batch only (`batch_broadcast_cross_join`): prepares the joins that are cross joins for a
/// nested loop join that copies its left side to every task.
///
/// 1. An equality between the two sides whose operands are the same non-`NULL` constant in every
///    row of their sides (`a.one = b.one` with `1 AS one` projected on both sides, the usual way
///    to write a cross join that a streaming job accepts) is `TRUE` for every pair of rows: it is
///    removed. Planned as a hash join, it sends every row of both sides to the one task of the
///    hash of the constant; without it, the join is a nested loop join.
/// 2. An inner join without equality between its sides whose right side is estimated smaller than
///    its left side has its sides swapped: the left side of a nested loop join is the one that it
///    buffers, and the one that `BatchNestedLoopJoin` copies to every task.
///
/// See `BatchNestedLoopJoin::to_distributed` for the distribution.
///
/// # Correctness
///
/// 1. The column facts (see [`column_facts`]) say that the operand of each side holds the same
///    non-`NULL` constant in every row of that side, for the simple types whose equal values are
///    identical: the equality is `TRUE` for every pair, so removing a conjunct that is always
///    `TRUE` keeps the condition, whatever the type of the join. 2. Swapping the sides of an inner
///    join, with the output columns mapped back, is an identity.
pub struct ConstantKeyJoinRule;

impl Rule<Logical> for ConstantKeyJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        if matches!(
            join.join_type(),
            JoinType::AsofInner | JoinType::AsofLeftOuter | JoinType::Unspecified
        ) {
            return None;
        }
        let removed = remove_constant_equalities(join);
        let current = removed.as_ref().unwrap_or(join);
        let swapped = (current.join_type() == JoinType::Inner
            && current.eq_indexes().is_empty()
            && matches!(
                (estimated_rows(&current.left()), estimated_rows(&current.right())),
                (Some(left), Some(right)) if right < left
            ))
        .then(|| swap_join_sides(current));
        swapped.or(removed).map(Into::into)
    }
}

impl ConstantKeyJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ConstantKeyJoinRule)
    }
}

/// The join without the equalities between its sides whose operands are the same constant, or
/// `None` if it has none.
fn remove_constant_equalities(join: &LogicalJoin) -> Option<LogicalJoin> {
    let left_len = join.left().schema().len();
    let left_facts = column_facts(&join.left());
    let right_facts = column_facts(&join.right());
    let conjunctions: Vec<ExprImpl> = join
        .on()
        .conjunctions
        .iter()
        .filter(|expr| !is_constant_equality(expr, left_len, &left_facts, &right_facts))
        .cloned()
        .collect();
    (conjunctions.len() != join.on().conjunctions.len())
        .then(|| join.clone_with_cond(Condition { conjunctions }))
}

/// Whether the conjunct is `l = r`, `l` a column of the left side and `r` a column of the right
/// side (in either order), both holding the same non-`NULL` constant in every row.
fn is_constant_equality(
    expr: &ExprImpl,
    left_len: usize,
    left_facts: &ColumnFacts,
    right_facts: &ColumnFacts,
) -> bool {
    let Some(call) = expr.as_function_call() else {
        return false;
    };
    let (ExprType::Equal, [a, b]) = (call.func_type(), call.inputs()) else {
        return false;
    };
    let (Some(a), Some(b)) = (a.as_input_ref(), b.as_input_ref()) else {
        return false;
    };
    let (left, right) = match (a.index() < left_len, b.index() < left_len) {
        (true, false) => (a, b),
        (false, true) => (b, a),
        _ => return false,
    };
    let left_ref: ExprImpl = InputRef::new(left.index(), left.return_type()).into();
    let left_constant = expr_facts(&left_ref, left_facts).0;
    let right_ref: ExprImpl = InputRef::new(right.index() - left_len, right.return_type()).into();
    let right_constant = expr_facts(&right_ref, right_facts).0;
    matches!((left_constant, right_constant), (Some(l), Some(r)) if l == r)
}
