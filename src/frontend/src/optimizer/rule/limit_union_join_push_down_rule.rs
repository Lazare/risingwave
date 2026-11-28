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

//! Rule `LimitUnionJoinPushDownRule` (batch only, `batch_push_down_limit`): a copy of a limit or
//! of a top-n goes into the inputs of a `UNION ALL` and into the preserved side of an outer join
//! below it, while the limit or top-n itself stays above.
//!
//! - `Limit(n, offset k)` over `UNION ALL(b1, .., bm)`: every branch gets `Limit(n + k)`. The
//!   upper limit reads `n + k` rows at most, and any `n + k` rows of the union will do: the branches
//!   cut to `n + k` rows each still hold `n + k` rows together (or all the rows of the union).
//! - `TopN(order, n, offset k)` over `UNION ALL`: every branch gets `TopN(order, n + k)`: each of
//!   the first `n + k` rows of the union in that order is among the first `n + k` rows of its own
//!   branch (Calcite's `SortUnionTransposeRule`). `WITH TIES` (no offset): every branch gets
//!   `TopN(order, n) WITH TIES`, which keeps all the rows of the branch that are not after the
//!   `n`-th row of the union.
//! - `Limit(n, offset k)` over `LEFT JOIN(L, R)`: `L` gets `Limit(n + k)`: every row of `L` gives
//!   at least one row of the join, whatever the condition, and only depends on itself and `R`; so
//!   the join of `n + k` rows of `L` gives `n + k` rows of the true join at least (or the whole
//!   join). A unique key on `R` is not needed. `RIGHT JOIN` is symmetric.
//! - `TopN(order, n, offset k)` over `LEFT JOIN(L, R)`, when every column of `order` comes from
//!   `L`: `L` gets `TopN(order, n + k)` (Calcite's `SortJoinTransposeRule`); the rows of the join
//!   carry the order key of their row of `L`, and every row of `L` gives at least one row.
//!
//! Not rewritten: a `UNION` without `ALL` (a distinct above a union all), inner, semi, anti and
//! full joins (a row cut on one side may be the one that matches, or that keeps a row of the
//! other side from being padded with `NULL`s), a top-n per group, a top-n ordered by a column of
//! the side padded with `NULL`s, a filter between the limit and the union or join (it is pushed
//! into the branches before this rule runs when it can be), and a branch that already returns
//! `n + k` rows at most.
//!
//! The rule sees through the projections between the limit and the union or join (a projection
//! keeps one row per row; a top-n needs its order columns to be plain columns of the projection's
//! input). It only adds copies below: the upper limit and the projections are kept as they are.

use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalLimit, LogicalTopN, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::property::Order;
use crate::planner::LIMIT_ALL_COUNT;

pub struct LimitUnionJoinPushDownRule {}

/// What a branch receives: the number of rows it may be cut to, and the order of the cut (none
/// for a plain limit).
struct Cut {
    rows: u64,
    /// `None` for a limit, the order and `WITH TIES` for a top-n.
    order: Option<(Order, bool)>,
}

impl Cut {
    /// The same cut over the input of a projection, `None` when an order column is not a plain
    /// column of the input.
    fn through_project(&self, o2i: impl Fn(usize) -> Option<usize>) -> Option<Self> {
        let order = match &self.order {
            None => None,
            Some((order, with_ties)) => Some((map_order(order, &o2i)?, *with_ties)),
        };
        Some(Self {
            rows: self.rows,
            order,
        })
    }

    /// The limit or top-n over `input`, `None` when `input` already returns `rows` rows at most.
    fn apply_to(&self, input: PlanRef) -> Option<PlanRef> {
        if max_rows(&input).is_some_and(|max| max <= self.rows) {
            return None;
        }
        Some(match &self.order {
            None => {
                // A limit right above another one: one limit, with the smaller count.
                if let Some(limit) = input.as_logical_limit() {
                    LogicalLimit::create(limit.input(), self.rows, limit.offset())
                } else {
                    LogicalLimit::create(input, self.rows, 0)
                }
            }
            Some((order, with_ties)) => {
                LogicalTopN::new(input, self.rows, 0, *with_ties, order.clone(), vec![]).into()
            }
        })
    }
}

fn map_order(order: &Order, map: impl Fn(usize) -> Option<usize>) -> Option<Order> {
    Some(Order::new(
        order
            .column_orders
            .iter()
            .map(|o| Some(ColumnOrder::new(map(o.column_index)?, o.order_type)))
            .collect::<Option<Vec<_>>>()?,
    ))
}

/// An upper bound of the rows of `plan`, through projections: limits, top-n without ties or
/// groups, values and aggregations without groups.
fn max_rows(plan: &PlanRef) -> Option<u64> {
    if let Some(project) = plan.as_logical_project() {
        return max_rows(&project.input());
    }
    if let Some(limit) = plan.as_logical_limit() {
        return Some(limit.limit());
    }
    if let Some(top_n) = plan.as_logical_top_n() {
        if top_n.group_key().is_empty() && !top_n.limit_attr().with_ties() {
            return Some(top_n.limit_attr().limit());
        }
        return None;
    }
    if let Some(values) = plan.as_logical_values() {
        return Some(values.rows().len() as u64);
    }
    if let Some(agg) = plan.as_logical_agg()
        && agg.group_key().is_empty()
    {
        return Some(1);
    }
    None
}

/// Puts copies of `cut` into the union or outer join below `plan` (through projections). `None`
/// when nothing changes.
fn push_cut(plan: &PlanRef, cut: &Cut) -> Option<PlanRef> {
    if let Some(project) = plan.as_logical_project() {
        let o2i = project.o2i_col_mapping();
        let cut = cut.through_project(|i| o2i.try_map(i))?;
        let input = push_cut(&project.input(), &cut)?;
        return Some(project.clone_with_input(input).into());
    }
    if let Some(union) = plan.as_logical_union() {
        if !union.all() {
            return None;
        }
        let mut changed = false;
        let new_inputs: Vec<PlanRef> = plan
            .inputs()
            .iter()
            .map(|input| match cut.apply_to(input.clone()) {
                Some(new_input) => {
                    changed = true;
                    new_input
                }
                None => input.clone(),
            })
            .collect();
        return changed.then(|| plan.clone_root_with_inputs(&new_inputs));
    }
    if let Some(join) = plan.as_logical_join() {
        let left_len = join.left().schema().len();
        let output_indices = join.output_indices();
        // The side kept whole by the join, and the columns of that side among the internal
        // columns of the join (the left columns, then the right ones).
        let (left_side, side_columns) = match join.join_type() {
            JoinType::LeftOuter => (true, 0..left_len),
            JoinType::RightOuter => (false, left_len..join.internal_column_num()),
            _ => return None,
        };
        let cut = cut.through_project(|i| {
            let internal = *output_indices.get(i)?;
            if side_columns.contains(&internal) {
                Some(internal - side_columns.start)
            } else {
                None
            }
        })?;
        return if left_side {
            let left = cut.apply_to(join.left())?;
            Some(plan.clone_root_with_inputs(&[left, join.right()]))
        } else {
            let right = cut.apply_to(join.right())?;
            Some(plan.clone_root_with_inputs(&[join.left(), right]))
        };
    }
    None
}

impl Rule<Logical> for LimitUnionJoinPushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let (input, n, k, order) = if let Some(limit) = plan.as_logical_limit() {
            (limit.input(), limit.limit(), limit.offset(), None)
        } else {
            let top_n = plan.as_logical_top_n()?;
            if !top_n.group_key().is_empty() {
                return None;
            }
            let limit_attr = top_n.limit_attr();
            (
                top_n.input(),
                limit_attr.limit(),
                top_n.offset(),
                Some((top_n.topn_order().clone(), limit_attr.with_ties())),
            )
        };
        // `OFFSET` without `LIMIT` reads every row: nothing to cut. `LIMIT 0` is left to the
        // pruning of empty relations.
        let rows = n.checked_add(k)?;
        if rows == 0 || rows >= LIMIT_ALL_COUNT {
            return None;
        }
        let new_input = push_cut(&input, &Cut { rows, order })?;
        Some(plan.clone_root_with_inputs(&[new_input]))
    }
}

impl LimitUnionJoinPushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(LimitUnionJoinPushDownRule {})
    }
}
