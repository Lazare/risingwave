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

//! Removes the default branch of a top-1 per key over a union, when the other side of the join
//! that reads the top-1 already carries the row that the default branch would give.
//!
//! A status is often computed as the best row per key of a union of candidate rows, one branch
//! per status, with a constant priority; the lowest priority comes from the table itself, so
//! that every row of the table has a status:
//!
//! ```text
//! Join(Inner, on: t.id = s.id)
//! ├─Scan(t)
//! └─TopN(limit 1, group: [id], order: [p DESC])
//!   └─Union
//!     ├─Project(t2.id, 'initiated', 1)      <- the default branch, another read of `t`
//!     │ └─Scan(t2)
//!     └─Project(x.id, 'active', 2)
//!       └─...
//! ```
//!
//! The default branch never wins against another branch (its priority is strictly worse), its
//! rows of a key are all equal on the columns that are read, and it has a row for the key of
//! every row of `t`: the row of `t` that the other side carries is one. So the top-1 of the
//! other branches gives the status when it finds a row, and the default branch otherwise:
//!
//! ```text
//! Project(t.*, CASE WHEN s.id IS NOT NULL THEN s.status ELSE 'initiated' END, ...)
//! └─Join(LeftOuter, on: t.id = s.id)
//!   ├─Scan(t)
//!   └─TopN(limit 1, group: [id], order: [p DESC])
//!     └─Project(x.id, 'active', 2)
//!       └─...
//! ```
//!
//! The table needs no unique key: the default branch only has to have a row for the key, and the
//! carried row is one. The choice of the top-1 among the other branches is unchanged: their rows
//! are the same, and their relative order (also on ties) does not depend on the default branch.
//! Only used by the streaming optimizer, on demand (`streaming_merge_lookups_under_dedup`).

use std::cmp::Ordering;
use std::collections::BTreeSet;

use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::sort_util::cmp_datum;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{
    carried_witness, commute, lookup_of, map_input_refs, referred_columns,
};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, LogicalUnion, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::Condition;

pub struct TopOneDefaultBranchRule;

impl TopOneDefaultBranchRule {
    pub fn create() -> BoxedRule {
        Box::new(TopOneDefaultBranchRule)
    }
}

impl Rule<Logical> for TopOneDefaultBranchRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        match join.join_type() {
            JoinType::LeftOuter => remove_default_branch(join),
            JoinType::Inner => {
                remove_default_branch(join).or_else(|| remove_default_branch(&commute(join)))
            }
            _ => None,
        }
    }
}

/// The values that a column of a plan can hold, when they are known: a literal, or a `CASE`
/// whose results are literals (and `NULL` without `ELSE`), looking through projections, filters
/// and shared sub-plans.
fn branch_values(plan: &PlanRef, column: usize) -> Option<Vec<Datum>> {
    if let Some(project) = plan.as_logical_project() {
        return match &project.exprs()[column] {
            ExprImpl::Literal(literal) => Some(vec![literal.get_data().clone()]),
            ExprImpl::InputRef(input_ref) => branch_values(&project.input(), input_ref.index()),
            ExprImpl::FunctionCall(call) if call.func_type() == ExprType::Case => {
                let inputs = call.inputs();
                let mut values = vec![];
                for (position, input) in inputs.iter().enumerate() {
                    // The conditions are at even positions, except a last `ELSE`.
                    let is_value = position % 2 == 1 || position == inputs.len() - 1;
                    if !is_value {
                        continue;
                    }
                    let ExprImpl::Literal(literal) = input else {
                        return None;
                    };
                    values.push(literal.get_data().clone());
                }
                if inputs.len() % 2 == 0 {
                    // No `ELSE`: `NULL` when no condition holds.
                    values.push(None);
                }
                Some(values)
            }
            _ => None,
        };
    }
    if let Some(filter) = plan.as_logical_filter() {
        return branch_values(&filter.input(), column);
    }
    if let Some(share) = plan.as_logical_share() {
        return branch_values(&share.input(), column);
    }
    None
}

fn case_when(condition: ExprImpl, then: ExprImpl, otherwise: Option<ExprImpl>) -> ExprImpl {
    let return_type = then.return_type();
    let mut inputs = vec![condition, then];
    inputs.extend(otherwise);
    FunctionCall::new_unchecked(ExprType::Case, inputs, return_type).into()
}

fn and_all(exprs: Vec<ExprImpl>) -> Option<ExprImpl> {
    exprs.into_iter().reduce(|left, right| {
        FunctionCall::new_unchecked(ExprType::And, vec![left, right], DataType::Boolean).into()
    })
}

fn is_not_null(expr: ExprImpl) -> ExprImpl {
    FunctionCall::new_unchecked(ExprType::IsNotNull, vec![expr], DataType::Boolean).into()
}

/// Rewrites the join if its right side is a top-1 per key over a union with a default branch
/// that the left side makes redundant. See the documentation of the module.
fn remove_default_branch(join: &LogicalJoin) -> Option<PlanRef> {
    let left = join.left();
    let left_len = left.schema().len();

    // The projections and filters above the top-1, from the top.
    let mut node = join.right();
    let mut above: Vec<PlanRef> = vec![];
    while node.as_logical_project().is_some() || node.as_logical_filter().is_some() {
        above.push(node.clone());
        node = node.inputs()[0].clone();
    }
    let top_n = node.as_logical_top_n()?;
    let limit = top_n.limit_attr();
    if limit.limit() != 1 || limit.with_ties() || top_n.offset() != 0 {
        return None;
    }
    let group_key = top_n.group_key().to_vec();
    let first_order = top_n.topn_order().column_orders.first()?.clone();
    if group_key.is_empty() {
        return None;
    }
    let union_plan = top_n.input();
    let union = union_plan.as_logical_union()?;
    if !union.all() {
        return None;
    }
    let union_len = union_plan.schema().len();
    // The columns of the right side, and the conjuncts that its rows satisfy, over the columns
    // of the union.
    let mut outputs: Vec<ExprImpl> = (0..union_len)
        .map(|index| InputRef::new(index, union_plan.schema().fields()[index].data_type()).into())
        .collect();
    let mut filters: Vec<ExprImpl> = vec![];
    let is_movable = |expr: &ExprImpl| expr.is_pure() && expr.count_nows() == 0;
    for node in above.iter().rev() {
        let over_union =
            |expr: &ExprImpl| map_input_refs(expr, |input_ref| outputs[input_ref.index()].clone());
        if let Some(project) = node.as_logical_project() {
            if !project.exprs().iter().all(is_movable) {
                return None;
            }
            outputs = project.exprs().iter().map(over_union).collect();
        } else if let Some(filter) = node.as_logical_filter() {
            if !filter.predicate().conjunctions.iter().all(is_movable) {
                return None;
            }
            filters.extend(filter.predicate().conjunctions.iter().map(over_union));
        }
    }

    // The condition: one equality per column of the group key, with a column of the left side.
    let mut compared: Vec<Option<usize>> = vec![None; group_key.len()];
    for conjunct in &join.on().conjunctions {
        let (first, second) = conjunct.as_eq_cond()?;
        if first.index() >= left_len
            || second.index() < left_len
            || first.return_type() != second.return_type()
        {
            return None;
        }
        let union_column = outputs[second.index() - left_len].as_input_ref()?.index();
        let position = group_key
            .iter()
            .position(|&column| column == union_column)?;
        if compared[position].replace(first.index()).is_some() {
            return None;
        }
    }
    let compared: Vec<usize> = compared.into_iter().collect::<Option<_>>()?;

    // The union columns that the result depends on.
    let mut used = referred_columns(outputs.iter().chain(&filters));
    used.extend(group_key.iter().copied());
    if let Some(source) = union.source_col()
        && (used.contains(&source) || source == first_order.column_index)
    {
        return None;
    }

    // The values of the first order column in each branch.
    let branches = union_plan.inputs().to_vec();
    let values: Vec<Option<Vec<Datum>>> = branches
        .iter()
        .map(|branch| branch_values(branch, first_order.column_index))
        .collect();
    let order_type = first_order.order_type;
    for (index, branch) in branches.iter().enumerate() {
        // The default branch: a read of a relation, its group key columns are columns of the
        // relation, and every other column that is read is a literal.
        let Some([default_value]) = values[index].as_deref() else {
            continue;
        };
        let Some(lookup) = lookup_of(branch) else {
            continue;
        };
        let exprs = lookup.exprs.clone();
        let key_columns: Option<Vec<usize>> = group_key
            .iter()
            .map(|&column| Some(exprs[column].as_input_ref()?.index()))
            .collect();
        let Some(key_columns) = key_columns else {
            continue;
        };
        let defaults_are_literals = used
            .iter()
            .filter(|&&column| !group_key.contains(&column))
            .all(|&column| matches!(exprs[column], ExprImpl::Literal(_)));
        if !defaults_are_literals || default_value.is_none() {
            continue;
        }
        // Strictly worse than every value of every other branch (`NULL` included, as the order
        // places it): it never wins, not even on a tie.
        let is_worst = values.iter().enumerate().all(|(other, other_values)| {
            other == index
                || other_values.as_ref().is_some_and(|other_values| {
                    other_values.iter().all(|value| {
                        cmp_datum(default_value, value, order_type) == Ordering::Greater
                    })
                })
        });
        if !is_worst {
            continue;
        }
        // The left side carries a row of the relation whose key columns are the compared ones.
        if !carried_witness(&left, &lookup, &key_columns, &compared) {
            continue;
        }
        let others: Vec<PlanRef> = branches
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, branch)| branch.clone())
            .collect();
        if others.is_empty() {
            return None;
        }
        let new_union: PlanRef = if others.len() == 1 && union.source_col().is_none() {
            others[0].clone()
        } else {
            LogicalUnion::new_with_source_col(true, others, union.source_col()).into()
        };
        let new_top_n: PlanRef = top_n.clone_with_input(new_union).into();
        return Some(rebuild(
            join,
            &left,
            new_top_n,
            &Rewritten {
                outputs: &outputs,
                filters: &filters,
                used: &used,
                group_key: &group_key,
                compared: &compared,
                default_exprs: &exprs,
            },
        ));
    }
    None
}

/// What [`rebuild`] needs, over the columns of the union.
struct Rewritten<'a> {
    /// The columns of the former right side.
    outputs: &'a [ExprImpl],
    /// The conjuncts that the rows of the former right side satisfy.
    filters: &'a [ExprImpl],
    /// The columns of the union that the above refer to, and the group key.
    used: &'a BTreeSet<usize>,
    group_key: &'a [usize],
    /// For each column of the group key, the column of the left side compared with it.
    compared: &'a [usize],
    /// The columns of the default branch, over the columns of its relation.
    default_exprs: &'a [ExprImpl],
}

/// Joins the left side with the top-1 of the other branches. A column of the union holds the
/// value of the top-1 when it finds a row, and the value of the default branch otherwise: the
/// compared column for the group key, its literal for the others. The former right side has a
/// row when either finds one (the default branch does when the compared columns are not `NULL`)
/// and the row satisfies the filters above the top-1.
fn rebuild(join: &LogicalJoin, left: &PlanRef, new_top_n: PlanRef, r: &Rewritten<'_>) -> PlanRef {
    let left_len = left.schema().len();
    // The right side: the group key first, to tell a found row, then the used columns.
    let mut columns: Vec<usize> = r.group_key.to_vec();
    columns.extend(
        r.used
            .iter()
            .copied()
            .filter(|column| !r.group_key.contains(column)),
    );
    let right_exprs: Vec<ExprImpl> = columns
        .iter()
        .map(|&column| {
            InputRef::new(column, new_top_n.schema().fields()[column].data_type()).into()
        })
        .collect();
    let right: PlanRef = LogicalProject::new(new_top_n, right_exprs).into();
    let left_column = |index: usize| -> ExprImpl {
        InputRef::new(index, left.schema().fields()[index].data_type()).into()
    };
    let right_column = |position: usize| -> ExprImpl {
        InputRef::new(
            left_len + position,
            right.schema().fields()[position].data_type(),
        )
        .into()
    };
    // The same equalities: the group key comes first on the right side.
    let on = r
        .compared
        .iter()
        .enumerate()
        .map(|(position, &column)| {
            FunctionCall::new_unchecked(
                ExprType::Equal,
                vec![left_column(column), right_column(position)],
                DataType::Boolean,
            )
            .into()
        })
        .collect();
    let new_join: PlanRef = LogicalJoin::new(
        left.clone(),
        right.clone(),
        JoinType::LeftOuter,
        Condition { conjunctions: on },
    )
    .into();
    let found_row = is_not_null(right_column(0));
    let default_found = and_all(
        r.compared
            .iter()
            .map(|&column| is_not_null(left_column(column)))
            .collect(),
    )
    .expect("the group key is not empty");
    // The value of a column of the union.
    let value = |union_column: usize| -> ExprImpl {
        let position = columns
            .iter()
            .position(|&column| column == union_column)
            .expect("the column is used");
        let default = match r
            .group_key
            .iter()
            .position(|&column| column == union_column)
        {
            Some(key) => left_column(r.compared[key]),
            None => r.default_exprs[union_column].clone(),
        };
        case_when(found_row.clone(), right_column(position), Some(default))
    };
    let over_values = |expr: &ExprImpl| map_input_refs(expr, |input_ref| value(input_ref.index()));
    let exists = FunctionCall::new_unchecked(
        ExprType::Or,
        vec![found_row.clone(), default_found],
        DataType::Boolean,
    )
    .into();
    let mut matched = vec![exists];
    matched.extend(r.filters.iter().map(over_values));
    let matched = and_all(matched).expect("not empty");
    let is_inner = join.join_type() == JoinType::Inner;
    let mut exprs = vec![];
    for &index in join.output_indices() {
        if index < left_len {
            exprs.push(left_column(index));
            continue;
        }
        let expr = over_values(&r.outputs[index - left_len]);
        exprs.push(if is_inner {
            expr
        } else {
            case_when(matched.clone(), expr, None)
        });
    }
    let input = if is_inner {
        LogicalFilter::new(
            new_join,
            Condition {
                conjunctions: vec![matched],
            },
        )
        .into()
    } else {
        new_join
    };
    LogicalProject::new(input, exprs).into()
}
