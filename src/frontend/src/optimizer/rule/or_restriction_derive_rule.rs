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

use fixedbitset::FixedBitSet;

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType, to_conjunctions, to_disjunctions};
use crate::optimizer::plan_node::PlanTreeNodeBinary;
use crate::optimizer::plan_node::generic::{
    GenericPlanRef, can_push_left_from_on, can_push_right_from_on,
};
use crate::utils::Condition;

/// The largest number of disjuncts of an `OR` that the rule looks into.
const MAX_DISJUNCTS: usize = 16;

/// Derives, from a conjunct `d_1 OR ... OR d_n` of the condition of a join that refers to both
/// sides, a restriction of each side: the `OR` of the parts of the disjuncts that refer to that side
/// only. The original condition is kept; predicate pushdown then moves the restrictions into the
/// sides. This is PostgreSQL's `extract_restriction_or_clauses` (`orclauses.c`) and Calcite's
/// `ExpandDisjunctionForJoinInputsRule`.
///
/// ```text
/// Join(Inner, on: a.k = b.k AND ((a.x = 1 AND b.y = 2) OR (a.x = 3 AND b.y = 4)))
/// gains the conjuncts (a.x = 1 OR a.x = 3) and (b.y = 2 OR b.y = 4)
/// ```
///
/// Correctness: if the `OR` is true for a pair of rows, one of its disjuncts `d_i` is true, so each
/// of its conjuncts is true, in particular the conjunction `p_i` of those that refer to side `X`
/// only; so `p_1 OR ... OR p_n` is true. A pair for which the derived predicate is not true can not
/// satisfy the condition: adding it as a conjunct changes nothing, for any type of join. A side is
/// restricted only when every disjunct has a part on it (otherwise nothing is implied), and only
/// when predicate pushdown moves a conjunct of the `ON` condition on that side into it
/// (`can_push_left_from_on`, `can_push_right_from_on`: both sides of an inner join, the optional
/// side of an outer join, the left side of a left semi join), so that the derived conjunct does
/// not stay in the join.
///
/// The parts are conjuncts that can not fail and are deterministic: column references, literals,
/// comparisons, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, `IS [NOT] DISTINCT FROM`, `IN`, `NOT`,
/// `AND`, `OR` of them. Evaluating them on rows that the join would not have compared can then
/// neither raise an error nor give a different answer. Temporal and as-of joins are left untouched.
///
/// The rule is only used on demand (`batch_derive_or_restrictions`,
/// `streaming_derive_or_restrictions`).
pub struct OrRestrictionDeriveRule {}

impl OrRestrictionDeriveRule {
    pub fn create() -> BoxedRule {
        Box::new(OrRestrictionDeriveRule {})
    }
}

impl Rule<Logical> for OrRestrictionDeriveRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let join_type = join.join_type();
        if join.is_asof_join() || join.should_be_temporal_join() {
            return None;
        }
        let left_len = join.left().schema().len();
        let total = left_len + join.right().schema().len();
        let mut sides = vec![];
        if can_push_left_from_on(join_type) {
            sides.push(columns(total, 0..left_len));
        }
        if can_push_right_from_on(join_type) {
            sides.push(columns(total, left_len..total));
        }
        if sides.is_empty() {
            return None;
        }
        let on = join.on();
        let mut derived = vec![];
        for expr in &on.conjunctions {
            let disjuncts = to_disjunctions(expr.clone());
            if disjuncts.len() < 2 || disjuncts.len() > MAX_DISJUNCTS {
                continue;
            }
            let refs = expr.collect_input_refs(total);
            for side in &sides {
                // An `OR` on one side only is pushed down as it is.
                if refs.is_subset(side) {
                    continue;
                }
                let parts: Option<Vec<ExprImpl>> = disjuncts
                    .iter()
                    .map(|disjunct| {
                        let part: Vec<ExprImpl> = to_conjunctions(disjunct.clone())
                            .into_iter()
                            .filter(|conjunct| {
                                conjunct.collect_input_refs(total).is_subset(side)
                                    && is_safe(conjunct)
                            })
                            .collect();
                        (!part.is_empty()).then(|| ExprImpl::and(part))
                    })
                    .collect();
                if let Some(parts) = parts {
                    derived.push(ExprImpl::or(parts));
                }
            }
        }
        if derived.is_empty() {
            return None;
        }
        let new_on = on.clone().and(Condition {
            conjunctions: derived,
        });
        // Already derived (the join is reached again through a shared sub-plan).
        if new_on == *on {
            return None;
        }
        Some(join.clone_with_cond(new_on).into())
    }
}

/// The set of the columns `range` among `total` columns.
fn columns(total: usize, range: std::ops::Range<usize>) -> FixedBitSet {
    let mut set = FixedBitSet::with_capacity(total);
    set.insert_range(range);
    set
}

/// Whether the expression is made only of column references, literals and functions that are
/// deterministic and can not fail.
fn is_safe(expr: &ExprImpl) -> bool {
    match expr {
        ExprImpl::InputRef(_) | ExprImpl::Literal(_) => true,
        ExprImpl::FunctionCall(call) => {
            matches!(
                call.func_type(),
                ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::LessThan
                    | ExprType::LessThanOrEqual
                    | ExprType::GreaterThan
                    | ExprType::GreaterThanOrEqual
                    | ExprType::IsNull
                    | ExprType::IsNotNull
                    | ExprType::IsTrue
                    | ExprType::IsNotTrue
                    | ExprType::IsFalse
                    | ExprType::IsNotFalse
                    | ExprType::IsDistinctFrom
                    | ExprType::IsNotDistinctFrom
                    | ExprType::In
                    | ExprType::Not
                    | ExprType::And
                    | ExprType::Or
            ) && call.inputs().iter().all(is_safe)
        }
        _ => false,
    }
}
