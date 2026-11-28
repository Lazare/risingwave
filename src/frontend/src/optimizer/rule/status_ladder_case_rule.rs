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

//! Picks, with one `CASE`, the best of the branches of a "status ladder" that read the same
//! relation.
//!
//! A status ladder keeps, for each key, the row of the branch with the highest priority:
//!
//! ```sql
//! SELECT k, status FROM (
//!   SELECT k, status, row_number() OVER (PARTITION BY k ORDER BY priority DESC) AS rn FROM (
//!     SELECT k, 'new' AS status, 1 AS priority FROM t
//!     UNION ALL SELECT k, 'open', 2 FROM t WHERE p1
//!     UNION ALL SELECT k, 'done', 3 FROM t WHERE p2
//!     UNION ALL SELECT k, 'late', 4 FROM u
//!   )
//! ) WHERE rn = 1
//! ```
//!
//! The planner makes it a `TopN` (limit 1 per group) over the union in streaming plans, and keeps
//! the `OverWindow` and the filter on the row number in batch plans; both forms are handled. Under that `TopN`, the
//! branches that filter and project the same relation `t` with the same key are replaced with
//! one branch that gives, for each row of `t`, only its best row: the literals of the first
//! branch, by priority, whose filter holds.
//!
//! ```text
//! TopN(limit 1, group: k, order: priority DESC)
//! └─Union
//!   ├─Project(k, CASE WHEN p2 THEN 'done' WHEN p1 THEN 'open' ELSE 'new' END,
//!   │         CASE WHEN p2 THEN 3 WHEN p1 THEN 2 ELSE 1 END)
//!   │ └─Scan(t)
//!   └─Project(k, 'late', 4)
//!     └─Scan(u)
//! ```
//!
//! Exact under the `TopN`: a row of `t` gave one candidate per branch whose filter holds, all for
//! the same key; the `TopN` never keeps one of lower priority than another candidate of the same
//! key, so only the best one of each row matters. A `NULL` filter falls through the `CASE` as it
//! dropped the row. Overlapping filters and duplicate rows need nothing more. The branches must
//! only output the key and literals, and two of them with the same priority must output the same
//! literals (otherwise the `TopN` chose between them arbitrarily).
//!
//! When the branches of the union all read the relation, and the relation is unique on the key,
//! each key has at most one row: the `TopN` is useless and removed.
//!
//! Branches that read an aggregation of the same relation, with the same group key, each with its
//! own filter below the aggregation (a state defined as "the keys that have a row of kind x", a
//! `GROUP BY` or `DISTINCT` per state), are merged first into one aggregation of the relation,
//! filtered by the disjunction of their filters, that counts the rows of each branch with
//! `count(*) FILTER (WHERE <its filter>)` and computes the calls of each branch with its filter
//! added; each branch then reads that aggregation and keeps the groups where its count is
//! positive, which are the groups that its own aggregation had, with the same values:
//!
//! ```text
//! Union                                     Project(k, CASE WHEN c2 > 0 THEN 'b' ELSE 'a' END, ..)
//! ├─Project(k, 'a', 1)                      └─Agg(group: k, c1: count(*) FILTER (WHERE p1),
//! │ └─Agg(group: k)                     =>  │                c2: count(*) FILTER (WHERE p2))
//! │   └─Filter(p1) <- Scan(t)                 └─Filter(p1 OR p2)
//! └─Project(k, 'b', 2)                          └─Scan(t)
//!   └─Agg(group: k)
//!     └─Filter(p2) <- Scan(t)
//! ```
//!
//! The merged aggregation holds one group per key of any of the branches, where the branches held
//! one per key each, and the `TopN` one row per group instead of one per branch. When the merged
//! branch is the whole union and the key is the group key, the `TopN` is removed as above.
//!
//! Only used on demand: `streaming_status_ladder_case` for streaming plans, where a branch with
//! `now()` is a join at this stage and stays a separate branch, and `batch_status_ladder_case`
//! for batch plans, where `now()` is a constant.

use fixedbitset::FixedBitSet;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::window_function::WindowFuncKind;

use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{and_all, map_input_refs};
use super::union_scan_merge_rule::{
    Base, Branch, branch_of, or, position_in, read_base, split_predicates,
};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{LogicalProject, LogicalUnion, PlanTreeNodeUnary};
use crate::utils::{Condition, IndexSet};

/// The value of a priority literal, if it is an integer.
fn priority_of(expr: &ExprImpl) -> Option<i64> {
    let ExprImpl::Literal(literal) = expr else {
        return None;
    };
    match literal.get_data() {
        Some(ScalarImpl::Int16(value)) => Some(*value as i64),
        Some(ScalarImpl::Int32(value)) => Some(*value as i64),
        Some(ScalarImpl::Int64(value)) => Some(*value),
        _ => None,
    }
}

fn is_literal(expr: &ExprImpl) -> bool {
    matches!(expr, ExprImpl::Literal(_))
}

/// Whether the base is unique on the columns of the key, which are columns of the base.
fn is_unique_on(branch: &Branch, key: &[ExprImpl]) -> bool {
    let Some(columns) = key
        .iter()
        .map(|expr| expr.as_input_ref().map(|input_ref| input_ref.index()))
        .collect::<Option<Vec<usize>>>()
    else {
        return false;
    };
    // The key of the table (unique, a `NULL` included), or the stream key of the shared sub-plan.
    let unique: Vec<usize> = match branch.base {
        Base::Table(_) => branch
            .plan
            .as_logical_scan()
            .expect("a scan")
            .table()
            .stream_key(),
        Base::Share(_) => match branch.plan.stream_key() {
            Some(key) => key.to_vec(),
            None => return false,
        },
    };
    !unique.is_empty() && unique.iter().all(|column| columns.contains(column))
}

/// Picks, with one `CASE`, the best of the branches of a status ladder that read the same
/// relation. See the documentation of the module.
pub struct StatusLadderCaseRule;

impl Rule<Logical> for StatusLadderCaseRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let Some(top_n) = plan.as_logical_top_n() else {
            return apply_to_row_number(&plan);
        };
        let limit = top_n.limit_attr();
        if limit.limit() != 1 || limit.with_ties() || top_n.offset() != 0 {
            return None;
        }
        let group_key: Vec<usize> = top_n.group_key().to_vec();
        let [order] = top_n.topn_order().column_orders.as_slice() else {
            return None;
        };
        let descending = order.order_type.is_descending();
        let priority_column = order.column_index;
        if group_key.is_empty() || group_key.contains(&priority_column) {
            return None;
        }
        let input = top_n.input();
        let (reduced, keeps_one_row) =
            reduce_ladder(&input, &group_key, priority_column, descending)?;
        if keeps_one_row {
            // Each key has at most one row: the `TopN` keeps it.
            return Some(reduced);
        }
        Some(top_n.clone_with_input(reduced).into())
    }
}

/// The top-1 of a status ladder as batch plans keep it: `Filter(row_number = 1)` over an
/// `OverWindow` with the one call `row_number() OVER (PARTITION BY .. ORDER BY ..)`.
fn apply_to_row_number(plan: &PlanRef) -> Option<PlanRef> {
    let filter = plan.as_logical_filter()?;
    let over_window_plan = filter.input();
    let over_window = over_window_plan.as_logical_over_window()?;
    let [call] = over_window.window_functions() else {
        return None;
    };
    if !matches!(call.kind, WindowFuncKind::RowNumber) {
        return None;
    }
    let [order] = call.order_by.as_slice() else {
        return None;
    };
    let group_key: Vec<usize> = call.partition_by.iter().map(|r| r.index()).collect();
    let descending = order.order_type.is_descending();
    let priority_column = order.column_index;
    if group_key.is_empty() || group_key.contains(&priority_column) {
        return None;
    }
    // The conjuncts of the filter on the row number alone must keep the first row only.
    let input = over_window.input();
    let row_number = input.schema().len();
    let mut columns = FixedBitSet::with_capacity(row_number + 1);
    columns.insert(row_number);
    let (on_row_number, _) = filter.predicate().clone().split_disjoint(&columns);
    if on_row_number.conjunctions.is_empty()
        || !on_row_number.conjunctions.iter().all(keeps_first_row_only)
    {
        return None;
    }
    let (reduced, keeps_one_row) = reduce_ladder(&input, &group_key, priority_column, descending)?;
    if keeps_one_row {
        // Each key has at most one row, whose row number is 1.
        let mut exprs: Vec<ExprImpl> = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| InputRef::new(index, field.data_type()).into())
            .collect();
        exprs.push(
            ExprImpl::literal_bigint(1)
                .cast_implicit(&call.return_type)
                .ok()?,
        );
        let numbered = LogicalProject::create(reduced, exprs);
        return Some(filter.clone_with_input(numbered).into());
    }
    let numbered: PlanRef = over_window.clone_with_input(reduced).into();
    Some(filter.clone_with_input(numbered).into())
}

/// Whether a conjunct on the row number alone keeps the first row only (`= 1`, `<= 1`, `< 2`).
fn keeps_first_row_only(conjunct: &ExprImpl) -> bool {
    let value = |v: ExprImpl| -> Option<i64> {
        let v = v
            .cast_implicit(&DataType::Int64)
            .ok()?
            .fold_const()
            .ok()??;
        Some(*v.as_int64())
    };
    if let Some((_, v)) = conjunct.as_eq_const() {
        return value(v) == Some(1);
    }
    if let Some((_, cmp, v)) = conjunct.as_comparison_const() {
        return match cmp {
            ExprType::LessThanOrEqual => value(v) == Some(1),
            ExprType::LessThan => value(v) == Some(2),
            _ => false,
        };
    }
    false
}

/// Reduces the union under the top-1 of a status ladder: see the documentation of the module.
/// Returns the new input of the top-1, and whether it holds at most one row per key, in which
/// case it replaces the top-1.
fn reduce_ladder(
    input: &PlanRef,
    group_key: &[usize],
    priority_column: usize,
    descending: bool,
) -> Option<(PlanRef, bool)> {
    let union = input.as_logical_union()?;
    if !union.all() || union.source_col().is_some() {
        return None;
    }
    let width = input.schema().len();
    let inputs = input.inputs();

    // The branches that only filter and project a relation, output the key and literals, and
    // a priority that is an integer.
    let mut branches: Vec<Option<(Branch, i64)>> = inputs
        .iter()
        .map(|input| {
            let branch = branch_of(input, false)?;
            if branch.exprs.len() != width {
                return None;
            }
            let literals_only = (0..width)
                .filter(|column| !group_key.contains(column))
                .all(|column| is_literal(&branch.exprs[column]));
            if !literals_only {
                return None;
            }
            let priority = priority_of(&branch.exprs[priority_column])?;
            Some((branch, priority))
        })
        .collect();

    // The branches that read an aggregation of the same base with the same group key are
    // replaced with branches that read one merged aggregation: they then read the same base.
    merge_aggregations(&inputs, &mut branches, group_key, priority_column);

    // The groups of branches that read the same base with the same key.
    let key_of = |branch: &Branch| -> Vec<ExprImpl> {
        group_key
            .iter()
            .map(|&column| branch.exprs[column].clone())
            .collect()
    };
    let mut groups: Vec<Vec<usize>> = vec![];
    for (index, branch) in branches.iter().enumerate() {
        let Some((branch, _)) = branch else { continue };
        match groups.iter_mut().find(|group| {
            let (first, _) = branches[group[0]].as_ref().expect("a branch");
            first.base == branch.base && key_of(first) == key_of(branch)
        }) {
            Some(group) => group.push(index),
            None => groups.push(vec![index]),
        }
    }
    // The members of a group in the order in which the `TopN` prefers them. Two members with
    // the same priority must output the same row: the group is left alone otherwise.
    let groups: Vec<Vec<usize>> = groups
        .into_iter()
        .filter_map(|mut group| {
            group.sort_by_key(|&index| {
                let (_, priority) = branches[index].as_ref().expect("a branch");
                if descending {
                    priority.saturating_neg()
                } else {
                    *priority
                }
            });
            let consistent = group.windows(2).all(|pair| {
                let (first, first_priority) = branches[pair[0]].as_ref().expect("a branch");
                let (second, second_priority) = branches[pair[1]].as_ref().expect("a branch");
                first_priority != second_priority || first.exprs == second.exprs
            });
            consistent.then_some(group)
        })
        .collect();
    let all_in_one = groups.len() == 1 && groups[0].len() == inputs.len();
    // A group of one branch changes nothing, unless it is the whole union over a unique key.
    let groups: Vec<Vec<usize>> = groups
        .into_iter()
        .filter(|group| group.len() >= 2)
        .collect();
    if groups.is_empty() {
        return None;
    }

    let mut new_inputs: Vec<Option<PlanRef>> = inputs.iter().cloned().map(Some).collect();
    for group in &groups {
        let members: Vec<&Branch> = group
            .iter()
            .map(|&index| &branches[index].as_ref().expect("a branch").0)
            .collect();
        new_inputs[group[0]] = Some(best_of(&members, group_key));
        for &index in &group[1..] {
            new_inputs[index] = None;
        }
    }
    let mut new_inputs: Vec<PlanRef> = new_inputs.into_iter().flatten().collect();
    if new_inputs.len() == 1 {
        let merged = new_inputs.pop().expect("one input");
        let first = &branches[groups[0][0]].as_ref().expect("a branch").0;
        return Some((merged, all_in_one && is_unique_on(first, &key_of(first))));
    }
    Some((LogicalUnion::new(true, new_inputs).into(), false))
}

impl StatusLadderCaseRule {
    pub fn create() -> BoxedRule {
        Box::new(StatusLadderCaseRule)
    }
}

/// One branch that gives, for each row of the base, the row of the first member, in the order of
/// the members, whose filter holds. The members read the same base with the same key.
fn best_of(members: &[&Branch], group_key: &[usize]) -> PlanRef {
    let width = members[0].exprs.len();
    // What every member requires is checked once in the read of the base, with one of what each
    // member requires besides, unless a member requires nothing more.
    let (common, own) = split_predicates(members);
    let mut below = common;
    if own.iter().all(|conjuncts| !conjuncts.is_empty()) {
        below.push(
            own.iter()
                .map(|conjuncts| and_all(conjuncts.clone()))
                .reduce(or)
                .expect("at least two members"),
        );
    }
    let (input, read) = read_base(members, below);
    let on_input = |expr: &ExprImpl| {
        map_input_refs(expr, |r| {
            InputRef::new(position_in(&read, r.index()), r.return_type()).into()
        })
    };

    // The conditions of the `CASE`: a member that requires nothing more is the `ELSE`, and the
    // members after it are never chosen.
    let mut cases: Vec<(Option<ExprImpl>, &Branch)> = vec![];
    for (member, conjuncts) in members.iter().zip_eq_fast(own) {
        if conjuncts.is_empty() {
            cases.push((None, *member));
            break;
        }
        cases.push((Some(on_input(&and_all(conjuncts))), *member));
    }
    let exprs: Vec<ExprImpl> = (0..width)
        .map(|column| {
            if group_key.contains(&column) {
                return on_input(&members[0].exprs[column]);
            }
            let values: Vec<&ExprImpl> = cases
                .iter()
                .map(|(_, member)| &member.exprs[column])
                .collect();
            if values.iter().all(|value| *value == values[0]) {
                return values[0].clone();
            }
            let return_type = values[0].return_type();
            let mut inputs = vec![];
            for ((condition, _), value) in cases.iter().zip_eq_fast(values) {
                if let Some(condition) = condition {
                    inputs.push(condition.clone());
                }
                inputs.push(value.clone());
            }
            FunctionCall::new_unchecked(ExprType::Case, inputs, return_type).into()
        })
        .collect();
    LogicalProject::create(input, exprs)
}

/// A branch of a status ladder that reads an aggregation: `(Project | Filter)* <- Agg <- input`,
/// where the input of the aggregation has the form of a [`Branch`].
struct AggBranch {
    /// The output columns of the branch, over the output columns of the aggregation without the
    /// group key columns that are literals: the group key of `group_exprs`, then the calls.
    exprs: Vec<ExprImpl>,
    /// The conjuncts that a row of the aggregation satisfies to give a row of the branch, over the
    /// same columns as `exprs`.
    predicate: Vec<ExprImpl>,
    /// The group key without its literals, over the columns of the base of the input.
    group_exprs: Vec<ExprImpl>,
    /// The calls of the aggregation, over its input columns.
    calls: Vec<PlanAggCall>,
    /// The input of the aggregation.
    input: Branch,
    enable_two_phase: bool,
}

/// Describes a branch that reads an aggregation with a group key, if it has the form of an
/// [`AggBranch`].
fn agg_branch_of(plan: &PlanRef) -> Option<AggBranch> {
    // The projection and the filters above the aggregation, over the output of the node reached.
    let mut exprs: Option<Vec<ExprImpl>> = None;
    let mut predicate: Vec<ExprImpl> = vec![];
    let mut node = plan.clone();
    loop {
        if let Some(project) = node.as_logical_project() {
            let through =
                |expr: &ExprImpl| map_input_refs(expr, |r| project.exprs()[r.index()].clone());
            exprs = Some(match &exprs {
                Some(exprs) => exprs.iter().map(through).collect(),
                None => project.exprs().clone(),
            });
            predicate = predicate.iter().map(through).collect();
            node = project.input();
        } else if let Some(filter) = node.as_logical_filter() {
            predicate.extend(filter.predicate().conjunctions.iter().cloned());
            node = filter.input();
        } else {
            break;
        }
    }
    let agg = node.as_logical_agg()?;
    if !agg.grouping_sets().is_empty() || agg.group_key().is_empty() {
        return None;
    }
    // A distinct call would count the rows of every branch in one set of distinct values.
    if agg.agg_calls().iter().any(|call| call.distinct) {
        return None;
    }
    let exprs = match exprs {
        Some(exprs) => exprs,
        None => (0..node.schema().len())
            .map(|index| -> ExprImpl {
                InputRef::new(index, node.schema()[index].data_type()).into()
            })
            .collect(),
    };
    if !exprs
        .iter()
        .chain(&predicate)
        .all(|expr| expr.is_pure() && expr.count_nows() == 0)
    {
        return None;
    }
    let input = branch_of(&agg.input(), false)?;
    // A group key column that is a literal (`SELECT DISTINCT k, 'x'`) does not change the groups:
    // it is left out of the group key, and the branch outputs the literal.
    let mut group_exprs: Vec<ExprImpl> = vec![];
    let mut output: Vec<ExprImpl> = vec![];
    for column in agg.group_key().indices() {
        let expr = &input.exprs[column];
        if is_literal(expr) {
            output.push(expr.clone());
        } else {
            output.push(InputRef::new(group_exprs.len(), expr.return_type()).into());
            group_exprs.push(expr.clone());
        }
    }
    if group_exprs.is_empty() {
        return None;
    }
    for (index, call) in agg.agg_calls().iter().enumerate() {
        output.push(InputRef::new(group_exprs.len() + index, call.return_type.clone()).into());
    }
    let on_output = |expr: &ExprImpl| map_input_refs(expr, |r| output[r.index()].clone());
    let exprs: Vec<ExprImpl> = exprs.iter().map(&on_output).collect();
    let predicate: Vec<ExprImpl> = predicate.iter().map(&on_output).collect();
    Some(AggBranch {
        exprs,
        predicate,
        group_exprs,
        calls: agg.agg_calls().clone(),
        input,
        enable_two_phase: agg.core().enable_two_phase,
    })
}

/// Replaces, in `branches`, the branches of the union that read an aggregation of the same base
/// with the same group key (and that are not branches of another form) with branches that read
/// one merged aggregation, see the documentation of the module. Groups of one branch are left
/// alone.
fn merge_aggregations(
    inputs: &[PlanRef],
    branches: &mut [Option<(Branch, i64)>],
    group_key: &[usize],
    priority_column: usize,
) {
    let width = inputs[0].schema().len();
    let candidates: Vec<Option<(AggBranch, i64)>> = inputs
        .iter()
        .zip_eq_fast(branches.iter())
        .map(|(input, branch)| {
            if branch.is_some() {
                return None;
            }
            let agg_branch = agg_branch_of(input)?;
            if agg_branch.exprs.len() != width {
                return None;
            }
            // The key is made of group key columns, the other columns are literals.
            let group_len = agg_branch.group_exprs.len();
            let well_formed = (0..width).all(|column| {
                let expr = &agg_branch.exprs[column];
                if group_key.contains(&column) {
                    expr.as_input_ref()
                        .is_some_and(|input_ref| input_ref.index() < group_len)
                } else {
                    is_literal(expr)
                }
            });
            if !well_formed {
                return None;
            }
            let priority = priority_of(&agg_branch.exprs[priority_column])?;
            Some((agg_branch, priority))
        })
        .collect();

    // The groups of branches with the same base, group key and key.
    let mut groups: Vec<Vec<usize>> = vec![];
    for (index, candidate) in candidates.iter().enumerate() {
        let Some((candidate, _)) = candidate else {
            continue;
        };
        let compatible = |other: &AggBranch| {
            other.input.base == candidate.input.base
                && other.group_exprs == candidate.group_exprs
                && group_key
                    .iter()
                    .all(|&column| other.exprs[column] == candidate.exprs[column])
        };
        match groups.iter_mut().find(|group| {
            let (first, _) = candidates[group[0]].as_ref().expect("a candidate");
            compatible(first)
        }) {
            Some(group) => group.push(index),
            None => groups.push(vec![index]),
        }
    }
    for group in groups.into_iter().filter(|group| group.len() >= 2) {
        let members: Vec<&AggBranch> = group
            .iter()
            .map(|&index| &candidates[index].as_ref().expect("a candidate").0)
            .collect();
        let merged = merged_aggregation(&members);
        for (&index, branch) in group.iter().zip_eq_fast(merged) {
            let priority = candidates[index].as_ref().expect("a candidate").1;
            branches[index] = Some((branch, priority));
        }
    }
}

/// The position of a value among `values`, added if it is not there yet.
fn position_of<T: PartialEq>(value: T, values: &mut Vec<T>) -> usize {
    match values.iter().position(|existing| *existing == value) {
        Some(position) => position,
        None => {
            values.push(value);
            values.len() - 1
        }
    }
}

/// One aggregation that computes the aggregations of the members, which read the same base with
/// the same group key; and each member as a [`Branch`] over it.
///
/// The aggregation reads the base filtered by what every member requires, and by one of what each
/// member requires besides (unless a member requires nothing more). It groups by the group key,
/// counts the rows of each member that requires more with `count(*) FILTER (WHERE <what it
/// requires besides>)`, and computes the calls of each member with that filter added to their own.
/// A member keeps the groups where its count is positive: a group has rows of that member exactly
/// then, and its calls then see exactly the rows that its own aggregation saw.
fn merged_aggregation(members: &[&AggBranch]) -> Vec<Branch> {
    let inputs: Vec<&Branch> = members.iter().map(|member| &member.input).collect();
    let (common, own) = split_predicates(&inputs);
    let mut below = common;
    if own.iter().all(|conjuncts| !conjuncts.is_empty()) {
        below.push(
            own.iter()
                .map(|conjuncts| and_all(conjuncts.clone()))
                .reduce(or)
                .expect("at least two members"),
        );
    }
    let (input, read) = read_base(&inputs, below);
    let group_len = members[0].group_exprs.len();
    // The input of the aggregation: the group key, then the columns of the read of the base, then
    // the other expressions that the calls read.
    let on_read = |expr: &ExprImpl| {
        map_input_refs(expr, |r| {
            InputRef::new(group_len + position_in(&read, r.index()), r.return_type()).into()
        })
    };
    let read_len = input.schema().len();
    let mut exprs: Vec<ExprImpl> = members[0]
        .group_exprs
        .iter()
        .map(|expr| {
            map_input_refs(expr, |r| {
                InputRef::new(position_in(&read, r.index()), r.return_type()).into()
            })
        })
        .collect();
    exprs.extend((0..read_len).map(|index| -> ExprImpl {
        InputRef::new(index, input.schema()[index].data_type()).into()
    }));

    let mut calls: Vec<PlanAggCall> = vec![];
    // For each member, the column of the aggregation of each of its output columns, and the
    // call that counts its rows if it requires more than the others.
    let mut maps: Vec<Vec<usize>> = vec![];
    let mut counts: Vec<Option<usize>> = vec![];
    for (member, conjuncts) in members.iter().zip_eq_fast(&own) {
        let filter: Vec<ExprImpl> = conjuncts.iter().map(&on_read).collect();
        // A column of the input of the member's aggregation, as a column of the new input.
        let column_of = |column: usize, exprs: &mut Vec<ExprImpl>| -> usize {
            let expr = on_read(&member.input.exprs[column]);
            match expr.as_input_ref() {
                Some(input_ref) => input_ref.index(),
                None => position_of(expr, exprs),
            }
        };
        counts.push((!filter.is_empty()).then(|| {
            let count = PlanAggCall::count_star().with_condition(Condition {
                conjunctions: filter.clone(),
            });
            position_of(count, &mut calls)
        }));
        let mut map: Vec<usize> = (0..group_len).collect();
        for call in &member.calls {
            let mut call = call.clone();
            for input_ref in &mut call.inputs {
                let column = column_of(input_ref.index(), &mut exprs);
                *input_ref = InputRef::new(column, input_ref.return_type());
            }
            for order in &mut call.order_by {
                order.column_index = column_of(order.column_index, &mut exprs);
            }
            let mut conjunctions: Vec<ExprImpl> = call
                .filter
                .conjunctions
                .iter()
                .map(|conjunct| {
                    map_input_refs(conjunct, |r| on_read(&member.input.exprs[r.index()]))
                })
                .collect();
            conjunctions.extend(filter.iter().cloned());
            call.filter = Condition { conjunctions };
            map.push(group_len + position_of(call, &mut calls));
        }
        maps.push(map);
    }
    let core: PlanRef = Agg::new(
        calls,
        (0..group_len).collect::<IndexSet>(),
        LogicalProject::create(input, exprs),
    )
    .with_enable_two_phase(members[0].enable_two_phase)
    .into();

    // Each member over the merged aggregation, read as it is, like a shared sub-plan.
    members
        .iter()
        .zip_eq_fast(maps)
        .zip_eq_fast(counts)
        .map(|((member, map), count)| {
            let on_core = |expr: &ExprImpl| {
                map_input_refs(expr, |r| {
                    InputRef::new(map[r.index()], r.return_type()).into()
                })
            };
            let mut predicate: Vec<ExprImpl> = vec![];
            if let Some(call) = count {
                let count: ExprImpl = InputRef::new(group_len + call, DataType::Int64).into();
                predicate.push(
                    FunctionCall::new_unchecked(
                        ExprType::GreaterThan,
                        vec![count, ExprImpl::literal_bigint(0)],
                        DataType::Boolean,
                    )
                    .into(),
                );
            }
            predicate.extend(member.predicate.iter().map(&on_core));
            Branch {
                base: Base::Share(core.id()),
                plan: core.clone(),
                predicate,
                exprs: member.exprs.iter().map(&on_core).collect(),
            }
        })
        .collect()
}
