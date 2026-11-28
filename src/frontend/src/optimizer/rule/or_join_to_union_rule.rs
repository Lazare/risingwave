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

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::expr::{
    ExprImpl, ExprType, ExprVisitor, FunctionCall, InputRef, to_conjunctions, to_disjunctions,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalJoin, LogicalProject, LogicalShare, LogicalUnion, PlanTreeNodeBinary,
    VisitExprsRecursive,
};
use crate::utils::Condition;

/// The largest number of disjuncts of the `OR` that the rule splits.
const MAX_DISJUNCTS: usize = 4;

/// Splits a join whose condition has no equality between its sides, but a conjunct
/// `d_1 OR ... OR d_n` (2 to 4 disjuncts) in which every disjunct has one, into equi-joins on the
/// disjuncts (Calcite's `JoinExpandOrToUnionRule`). Without the rule, batch runs a nested-loop join
/// and streaming can not plan the query at all.
///
/// Let `c_i = d_i AND r`, `r` the other conjuncts of the condition, so that the condition is
/// `c_1 OR ... OR c_n`. Writing `A ⋈ B`, `A ⋉ B` and `A ▷ B` for the inner, semi and anti joins:
///
/// - Inner join: `UNION ALL` over `i` of `A ⋈ B` on `c_i AND (d_1) IS NOT TRUE AND ... AND
///   (d_{i-1}) IS NOT TRUE`. A pair of rows satisfies the condition iff some `c_i` is true; it is
///   output by the branch of the first such `i` only (in that branch `r` is true, so
///   `c_j IS NOT TRUE` is `d_j IS NOT TRUE`). `IS NOT TRUE` keeps the pairs for which `d_j` is
///   `NULL`, which do not satisfy `d_j` either: no pair is lost or output twice.
/// - Left semi join: `UNION ALL` over `i` of `R_{i-1} ⋉ B` on `c_i`, where `R_0 = A` and
///   `R_i = R_{i-1} ▷ B` on `c_i` are the rows of `A` without a match on `c_1`, ..., `c_i`. A row of
///   `A` is output by the semi join iff it matches some `c_i`; it is output by the branch of the
///   first such `i` only.
/// - Left anti join: `R_n`, the rows of `A` without a match on any `c_i`: a row has no match on
///   the condition iff it has none on each `c_i`.
/// - Left outer join: the inner join above, `UNION ALL` the rows of `R_n` padded with `NULL`s (the
///   rows of `A` without a match, which the outer join pads).
///
/// Every branch is an equi-join on the equalities of its disjunct. The conjuncts of the condition
/// are evaluated in several joins: they must be pure. The join types that can be rewritten
/// without swapping the sides are the only ones handled (right joins are rare and are turned
/// into left joins later in the plan). Temporal joins are left untouched (their right side must
/// stay a scan).
///
/// In streaming (`create_for_stream`), the sides of the join and the `R_i` read by two joins are
/// shared (when the plan may share sub-plans), so that each is computed once; each join keeps its
/// own state. Otherwise (batch) each join reads its sides again: the rule then requires that two
/// reads of a side give the same rows (no volatile expression, no source), which a share ensures. The rule is only used on demand (`batch_or_join_to_union`,
/// `streaming_or_join_to_union`).
pub struct OrJoinToUnionRule {
    for_stream: bool,
}

impl OrJoinToUnionRule {
    /// For batch queries: the sides are read once per join that uses them.
    pub fn create() -> BoxedRule {
        Box::new(OrJoinToUnionRule { for_stream: false })
    }

    /// For streaming jobs: the sides are shared.
    pub fn create_for_stream() -> BoxedRule {
        Box::new(OrJoinToUnionRule { for_stream: true })
    }

    /// Whether the sides of the join are shared by the joins that read them.
    fn shares(&self, plan: &PlanRef) -> bool {
        self.for_stream && plan.ctx().session_ctx().config().enable_share_plan()
    }

    /// The plan, shared if it is read by several joins of a streaming job.
    fn share(&self, plan: PlanRef) -> PlanRef {
        if self.shares(&plan) && plan.as_logical_share().is_none() {
            LogicalShare::create(plan)
        } else {
            plan
        }
    }
}

impl Rule<Logical> for OrJoinToUnionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let join_type = join.join_type();
        if !matches!(
            join_type,
            JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
        ) || join.should_be_temporal_join()
        {
            return None;
        }
        let (left, right) = (join.left(), join.right());
        let (left_len, right_len) = (left.schema().len(), right.schema().len());
        let on = join.on();
        if on.conjunctions.iter().any(|expr| !expr.is_pure()) {
            return None;
        }
        // Without shares, each join reads the sides again: two reads must give the same rows.
        if !self.shares(&plan) && (!reads_same_rows(&left) || !reads_same_rows(&right)) {
            return None;
        }
        let has_eq_key = |conjunctions: Vec<ExprImpl>| {
            !Condition { conjunctions }
                .split_eq_keys(left_len, right_len)
                .0
                .is_empty()
        };
        // A join with an equality between its sides is already a hash join.
        if has_eq_key(on.conjunctions.clone()) {
            return None;
        }
        let (position, disjuncts) = on.conjunctions.iter().enumerate().find_map(|(i, expr)| {
            let disjuncts = to_disjunctions(expr.clone());
            ((2..=MAX_DISJUNCTS).contains(&disjuncts.len())
                && disjuncts
                    .iter()
                    .all(|disjunct| has_eq_key(to_conjunctions(disjunct.clone()))))
            .then_some((i, disjuncts))
        })?;
        let others: Vec<ExprImpl> = (on.conjunctions.iter().enumerate())
            .filter(|(i, _)| *i != position)
            .map(|(_, expr)| expr.clone())
            .collect();
        // `c_i`: the disjunct `i` and the other conjuncts of the condition.
        let cond = |i: usize| Condition {
            conjunctions: to_conjunctions(disjuncts[i].clone())
                .into_iter()
                .chain(others.iter().cloned())
                .collect(),
        };
        let n = disjuncts.len();
        let output_indices = join.output_indices().clone();
        let right = self.share(right);
        // `c_i` and `d_j IS NOT TRUE` for `j < i`: the pairs of the branch `i` of the inner join.
        let inner_branches = |left: &PlanRef| -> Vec<PlanRef> {
            (0..n)
                .map(|i| {
                    let mut on = cond(i);
                    on.conjunctions
                        .extend(disjuncts[..i].iter().cloned().map(is_not_true));
                    LogicalJoin::with_output_indices(
                        left.clone(),
                        right.clone(),
                        JoinType::Inner,
                        on,
                        output_indices.clone(),
                    )
                    .into()
                })
                .collect()
        };
        // `R_k`: the rows of `left` without a match on `c_1`, ..., `c_k`.
        let without_match = |left: PlanRef, k: usize| -> PlanRef {
            (0..k).fold(left, |rows, i| {
                LogicalJoin::new(rows, right.clone(), JoinType::LeftAnti, cond(i)).into()
            })
        };
        match join_type {
            JoinType::Inner => {
                let left = self.share(left);
                Some(LogicalUnion::create(true, inner_branches(&left)))
            }
            JoinType::LeftSemi => {
                let mut branches = vec![];
                let mut rows = self.share(left);
                for i in 0..n {
                    branches.push(
                        LogicalJoin::with_output_indices(
                            rows.clone(),
                            right.clone(),
                            JoinType::LeftSemi,
                            cond(i),
                            output_indices.clone(),
                        )
                        .into(),
                    );
                    if i + 1 < n {
                        rows = LogicalJoin::new(rows, right.clone(), JoinType::LeftAnti, cond(i))
                            .into();
                        // Read by the next semi join and by the next anti join, if any.
                        if i + 2 < n {
                            rows = self.share(rows);
                        }
                    }
                }
                Some(LogicalUnion::create(true, branches))
            }
            JoinType::LeftAnti => {
                let rows = without_match(left, n - 1);
                Some(
                    LogicalJoin::with_output_indices(
                        rows,
                        right.clone(),
                        JoinType::LeftAnti,
                        cond(n - 1),
                        join.output_indices().clone(),
                    )
                    .into(),
                )
            }
            JoinType::LeftOuter => {
                let left = self.share(left);
                let mut branches = inner_branches(&left);
                let unmatched = without_match(left.clone(), n);
                let left_types = left.schema().data_types();
                let right_types = right.schema().data_types();
                let exprs = output_indices
                    .iter()
                    .map(|&index| match index.checked_sub(left_len) {
                        None => InputRef::new(index, left_types[index].clone()).into(),
                        Some(index) => ExprImpl::literal_null(right_types[index].clone()),
                    })
                    .collect();
                branches.push(LogicalProject::create(unmatched, exprs));
                Some(LogicalUnion::create(true, branches))
            }
            _ => None,
        }
    }
}

fn is_not_true(expr: ExprImpl) -> ExprImpl {
    FunctionCall::new_unchecked(ExprType::IsNotTrue, vec![expr], DataType::Boolean).into()
}

/// Whether two reads of the plan give the same rows: no volatile expression (`random()`...) and no
/// source, whose two readers may see different messages.
fn reads_same_rows(plan: &PlanRef) -> bool {
    fn without_source(plan: &PlanRef) -> bool {
        plan.as_logical_source().is_none()
            && plan.as_logical_kafka_scan().is_none()
            && plan.inputs().iter().all(without_source)
    }
    let mut finder = ImpureFinder { found: false };
    plan.visit_exprs_recursive(&mut finder);
    !finder.found && without_source(plan)
}

struct ImpureFinder {
    found: bool,
}

impl ExprVisitor for ImpureFinder {
    fn visit_expr(&mut self, expr: &ExprImpl) {
        self.found |= expr.is_impure();
    }
}
