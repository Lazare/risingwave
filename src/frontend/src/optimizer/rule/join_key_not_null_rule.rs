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

//! `IS NOT NULL` filters derived from the equalities of a join condition
//! (`batch_join_keys_not_null`), Calcite's `JoinDeriveIsNotNullFilterRule`, for batch queries.

use std::collections::BTreeSet;

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use super::column_facts_simplify_rule::column_facts;
use super::cost_based_join_ordering_rule::may_be_lookup_join_side;
use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalFilter, LogicalJoin, PlanTreeNodeBinary};
use crate::utils::Condition;

/// Filters out, below a join, the rows of an input whose column compared with `=` to a column of
/// the other input is `NULL`, when those rows can never be in the output of the join.
///
/// ```text
/// Join(Inner, a.k = b.k)                    Join(Inner, a.k = b.k)
/// ├─Scan(a)                            =>   ├─Filter(a.k IS NOT NULL)
/// └─Scan(b)                                 │ └─Scan(a)
///                                           └─Filter(b.k IS NOT NULL)
///                                             └─Scan(b)
/// ```
///
/// The predicate pushdown that follows moves the filters into the scans, before the exchanges of
/// the hash join: the rows with a `NULL` key are no longer shuffled.
///
/// - Only top-level conjuncts `l = r` of the condition between a column of each side: `NULL = x`
///   is `NULL` for every `x` and every type, so a row whose key is `NULL` matches no row of the
///   other side, whatever the other conjuncts. Not `IS NOT DISTINCT FROM`, which matches `NULL`s.
/// - Only the sides whose rows without a match are not in the output: both sides of an inner or
///   semi join, the right side of a left outer or left anti join (a left row without a match is
///   padded or kept), the left side of a right outer or right anti join. Not full outer and as-of
///   joins.
/// - Not for a column that is already known to be never `NULL` (`NOT NULL` constraint, a filter or
///   a condition below that rejects `NULL`, see `column_facts`): the filter would be redundant.
///   This also makes the rule apply once.
/// - Not for the right side of a join that may be a lookup join: the table is read by key, not
///   scanned, and a lookup with a `NULL` key finds nothing anyway.
pub struct JoinKeyNotNullRule;

impl Rule<Logical> for JoinKeyNotNullRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        let (filter_left, filter_right) = match join.join_type() {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => (true, true),
            JoinType::LeftOuter | JoinType::LeftAnti => (false, true),
            JoinType::RightOuter | JoinType::RightAnti => (true, false),
            _ => return None,
        };
        let (left, right) = (join.left(), join.right());
        let left_len = left.schema().len();
        let mut left_keys = BTreeSet::new();
        let mut right_keys = BTreeSet::new();
        for expr in &join.on().conjunctions {
            // Ordered by index: the first one is on the left side if the two sides differ.
            if let Some((first, second)) = expr.as_eq_cond()
                && first.index() < left_len
                && second.index() >= left_len
            {
                left_keys.insert(first.index());
                right_keys.insert(second.index() - left_len);
            }
        }
        if left_keys.is_empty() {
            return None;
        }
        let filter_right = filter_right && !may_be_lookup_join_side(&right, &right_keys);
        let new_left = if filter_left {
            not_null_filter(&left, &left_keys)
        } else {
            None
        };
        let new_right = if filter_right {
            not_null_filter(&right, &right_keys)
        } else {
            None
        };
        if new_left.is_none() && new_right.is_none() {
            return None;
        }
        let (_, _, on, join_type, output_indices) = join.clone().decompose();
        Some(
            LogicalJoin::with_output_indices(
                new_left.unwrap_or(left),
                new_right.unwrap_or(right),
                join_type,
                on,
                output_indices,
            )
            .into(),
        )
    }
}

impl JoinKeyNotNullRule {
    pub fn create() -> BoxedRule {
        Box::new(JoinKeyNotNullRule)
    }
}

/// A filter `c IS NOT NULL` over the input for each of the given columns that may be `NULL`, or
/// `None` if none may be.
fn not_null_filter(input: &PlanRef, columns: &BTreeSet<usize>) -> Option<PlanRef> {
    let facts = column_facts(input);
    let schema = input.schema();
    let conjunctions: Vec<ExprImpl> = columns
        .iter()
        .filter(|&&column| !facts.is_non_null(column))
        .map(|&column| {
            FunctionCall::new_unchecked(
                ExprType::IsNotNull,
                vec![InputRef::new(column, schema.fields()[column].data_type.clone()).into()],
                DataType::Boolean,
            )
            .into()
        })
        .collect();
    if conjunctions.is_empty() {
        return None;
    }
    Some(LogicalFilter::create(
        input.clone(),
        Condition { conjunctions },
    ))
}
