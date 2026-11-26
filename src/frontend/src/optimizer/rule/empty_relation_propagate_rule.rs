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

use super::never_true_predicate_rule::may_carry_watermark;
use super::prelude::{PlanRef, *};
use crate::optimizer::PlanTreeNode;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalUnion, LogicalValues, PlanTreeNodeBinary};

/// Propagates an empty relation (a `LogicalValues` without rows) upward through operators that
/// are guaranteed to produce no rows from it. Must be applied bottom-up.
///
/// Handled operators:
///
/// - Row-wise / partition-wise unary operators always produce no rows from no rows:
///   `Filter`, `Project`, `ProjectSet`, `Expand`, `HopWindow`, `Limit`, `TopN`, `Dedup`,
///   `OverWindow`.
/// - `Agg` **with** a non-empty group key and without grouping sets: no input rows means no
///   groups. An `Agg` without group key still emits one row over an empty input (e.g.
///   `count(*) = 0`), so it is left untouched.
/// - `Join`: every output row of the listed join types is derived from at least one row of the
///   empty side, regardless of the join condition:
///   - left side empty: `Inner`, `LeftOuter`, `LeftSemi`, `LeftAnti`, `RightSemi`;
///   - right side empty: `Inner`, `RightOuter`, `LeftSemi`, `RightSemi`, `RightAnti`.
///
///   The other combinations (e.g. `LeftOuter` / `LeftAnti` / `FullOuter` with an empty right
///   side) still output rows of the non-empty side and are left untouched, as well as as-of
///   joins.
/// - `Union All`: empty inputs contribute nothing and are dropped. With no input left the union
///   is empty; with exactly one input left the union is that input.
///
/// The removed sub-plans are never executed. This is fine for batch queries: they are read-only,
/// and a volatile expression that is evaluated zero times can not affect the result. A runtime
/// error that would have been raised by a removed sub-plan is no longer raised.
///
/// For a streaming job (`create_for_stream`), a join is not replaced with an empty relation if
/// its other side may carry a watermark, see [`may_carry_watermark`].
///
/// [`may_carry_watermark`]: super::never_true_predicate_rule::may_carry_watermark
pub struct EmptyRelationPropagateRule {
    for_stream: bool,
}

fn is_empty(plan: &PlanRef) -> bool {
    plan.as_logical_values()
        .is_some_and(|values| values.rows().is_empty())
}

impl Rule<Logical> for EmptyRelationPropagateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let to_empty = || {
            Some(LogicalValues::create(
                vec![],
                plan.schema().clone(),
                plan.ctx(),
            ))
        };

        if let Some(union) = plan.as_logical_union() {
            return Self::apply_union(&plan, union);
        }

        if let Some(join) = plan.as_logical_join() {
            let empty_if_left_empty = matches!(
                join.join_type(),
                JoinType::Inner
                    | JoinType::LeftOuter
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
            );
            let empty_if_right_empty = matches!(
                join.join_type(),
                JoinType::Inner
                    | JoinType::RightOuter
                    | JoinType::LeftSemi
                    | JoinType::RightSemi
                    | JoinType::RightAnti
            );
            return if ((empty_if_left_empty && is_empty(&join.left()))
                || (empty_if_right_empty && is_empty(&join.right())))
                && !(self.for_stream && may_carry_watermark(&plan))
            {
                to_empty()
            } else {
                None
            };
        }

        let propagate = if let Some(agg) = plan.as_logical_agg() {
            !agg.group_key().is_empty() && agg.grouping_sets().is_empty()
        } else {
            plan.as_logical_filter().is_some()
                || plan.as_logical_project().is_some()
                || plan.as_logical_project_set().is_some()
                || plan.as_logical_expand().is_some()
                || plan.as_logical_hop_window().is_some()
                || plan.as_logical_limit().is_some()
                || plan.as_logical_top_n().is_some()
                || plan.as_logical_dedup().is_some()
                || plan.as_logical_over_window().is_some()
        };
        if !propagate {
            return None;
        }
        let inputs = plan.inputs();
        match &inputs[..] {
            [input] if is_empty(input) => to_empty(),
            _ => None,
        }
    }
}

impl EmptyRelationPropagateRule {
    /// For batch queries.
    pub fn create() -> BoxedRule {
        Box::new(EmptyRelationPropagateRule { for_stream: false })
    }

    /// For streaming jobs: a join whose other side may carry a watermark is kept.
    pub fn create_for_stream() -> BoxedRule {
        Box::new(EmptyRelationPropagateRule { for_stream: true })
    }

    fn apply_union(plan: &PlanRef, union: &LogicalUnion) -> Option<PlanRef> {
        // `UNION` (distinct) is converted to `UNION ALL` + `Agg` by `UnionToDistinctRule`, and
        // `source_col` is only used by streaming.
        if !union.all() || union.source_col().is_some() {
            return None;
        }
        let inputs = union.inputs();
        let mut remaining: Vec<_> = inputs.iter().filter(|i| !is_empty(i)).cloned().collect();
        if remaining.len() == inputs.len() {
            return None;
        }
        Some(match remaining.len() {
            0 => LogicalValues::create(vec![], plan.schema().clone(), plan.ctx()),
            // All inputs of a union have the same column types and column order as the union.
            1 => remaining.pop().unwrap(),
            _ => LogicalUnion::create(true, remaining),
        })
    }
}
