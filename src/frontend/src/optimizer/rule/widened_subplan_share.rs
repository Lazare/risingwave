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

//! Sharing, late in the streaming optimizer, of the stateful sub-plans that compute the same thing
//! up to their columns and to filters that commute with them.
//!
//! Two reads of the same join or aggregation are rarely identical at the end of the optimizer:
//! column pruning keeps different columns in each copy, and predicate pushdown moves the filter of
//! one copy below its join. [`share_widened_subplans`] aligns such copies node by node and builds
//! one *core* that computes all of them:
//!
//! - a projection of the core keeps the columns of every copy, and computes the union of their
//!   expressions;
//! - a filter or a scan predicate keeps the conjuncts that every copy has; the other conjuncts of a
//!   copy are *residual*: they are lifted, as far as they commute, and applied by that copy above
//!   the shared core;
//! - a residual conjunct commutes with an inner join, with the preserved side of an outer join, with
//!   the left side of a semi or anti join, and with an aggregation when it only reads group key
//!   columns. Not with the optional side of an outer join, nor with the right side of a semi or
//!   anti join: the copy is then not merged;
//! - below an aggregation, the residual conjuncts of a copy that read other columns become a
//!   `FILTER` on the aggregate calls of that copy, and the copy keeps the groups where
//!   `count(*) FILTER (WHERE <conjuncts>)` is positive: the groups that its own input had. Without
//!   group key the aggregation emits one row in both plans, the `FILTER` is enough. This merges
//!   several aggregations of the same input and group key with different filters into one;
//! - joins must have the same type and condition, aggregations the same group key (their calls
//!   are united), scans the same table; any other node must be equal in every copy.
//!
//! Each copy is replaced with `Project(its columns) <- Filter(its residual conjuncts) <- Share(core)`.
//! Column pruning then narrows the core to the columns that its consumers read. Only copies rooted
//! at a join or an aggregation are merged: they remove a state, its actors and its backfills,
//! where a shared chain of scan, filter and projection would only add a fragment (see
//! `identical_subplan_share`). Existing shares are not looked into.
//!
//! Cost: the core keeps the rows of every copy; a filter lifted above a join no longer reduces the
//! state of that join.

use std::collections::{BTreeMap, HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::plan_common::JoinType;

use super::prelude::PlanRef;
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, LogicalShare, PlanNodeId, PlanTreeNodeBinary,
    PlanTreeNodeUnary,
};
use crate::utils::{Condition, IndexSet};

/// The largest number of copies of one shape that are compared.
const MAX_COPIES: usize = 16;

/// Shares the stateful sub-plans of `plan` that compute the same core (see the module
/// documentation). Returns the new plan and the number of copies replaced with a shared core;
/// with none, `plan` itself is returned.
pub fn share_widened_subplans(plan: PlanRef) -> (PlanRef, usize) {
    let mut candidates = vec![];
    let mut visited = HashSet::new();
    collect_candidates(&plan, &mut visited, &mut candidates);

    // The candidates of each shape, in the order of a depth-first traversal.
    let mut shapes: BTreeMap<String, Vec<PlanRef>> = BTreeMap::new();
    for candidate in candidates {
        shapes.entry(shape(&candidate)).or_default().push(candidate);
    }
    // Larger sub-plans first: a copy that is replaced takes its sub-plans with it.
    let mut groups = shapes
        .into_values()
        .filter(|copies| copies.len() >= 2)
        .collect_vec();
    groups.sort_by_key(|copies| std::cmp::Reverse(size(&copies[0])));

    let mut replaced_nodes: HashSet<PlanNodeId> = HashSet::new();
    let mut replacements: HashMap<PlanNodeId, PlanRef> = HashMap::new();
    for copies in groups {
        let mut remaining = copies
            .into_iter()
            .filter(|copy| !replaced_nodes.contains(&copy.id()))
            .take(MAX_COPIES)
            .collect_vec();
        // Greedily: the first copy with every other copy that merges with it, then the rest.
        while remaining.len() >= 2 {
            let mut chosen = vec![remaining.remove(0)];
            let mut merged = None;
            let mut rest = vec![];
            for copy in remaining {
                let mut attempt = chosen.clone();
                attempt.push(copy.clone());
                match merge(&attempt) {
                    Some(result) => {
                        chosen = attempt;
                        merged = Some(result);
                    }
                    None => rest.push(copy),
                }
            }
            remaining = rest;
            let Some(merged) = merged else {
                continue;
            };
            let share: PlanRef = LogicalShare::create(merged.plan);
            for (index, copy) in chosen.iter().enumerate() {
                mark_subtree(copy, &mut replaced_nodes);
                let mut replacement = share.clone();
                if !merged.residuals[index].is_empty() {
                    replacement = LogicalFilter::create(
                        replacement,
                        Condition {
                            conjunctions: merged.residuals[index].clone(),
                        },
                    );
                }
                let replacement = LogicalProject::with_out_col_idx(
                    replacement,
                    merged.maps[index].iter().copied(),
                )
                .into();
                replacements.insert(copy.id(), replacement);
            }
        }
    }
    if replacements.is_empty() {
        return (plan, 0);
    }
    let count = replacements.len();
    let mut memo = HashMap::new();
    (replace(&plan, &replacements, &mut memo), count)
}

/// The joins and aggregations of the plan, outside of shares.
fn collect_candidates(
    plan: &PlanRef,
    visited: &mut HashSet<PlanNodeId>,
    candidates: &mut Vec<PlanRef>,
) {
    if !visited.insert(plan.id()) || plan.as_logical_share().is_some() {
        return;
    }
    if plan.as_logical_join().is_some() || plan.as_logical_agg().is_some() {
        candidates.push(plan.clone());
    }
    for input in plan.inputs() {
        collect_candidates(&input, visited, candidates);
    }
}

/// A description of a sub-plan that ignores its columns, filters and projections: copies that can
/// be merged have the same shape.
fn shape(plan: &PlanRef) -> String {
    if let Some(project) = plan.as_logical_project() {
        return shape(&project.input());
    }
    if let Some(filter) = plan.as_logical_filter() {
        return shape(&filter.input());
    }
    if let Some(scan) = plan.as_logical_scan() {
        return format!("Scan({})", scan.table().id());
    }
    if plan.as_logical_share().is_some() {
        return format!("Share({})", plan.id().0);
    }
    let head = match plan.as_logical_join() {
        Some(join) => format!("Join{:?}", join.join_type()),
        None => format!("{:?}", plan.node_type()),
    };
    format!(
        "{head}({})",
        plan.inputs().iter().map(shape).collect_vec().join(",")
    )
}

fn size(plan: &PlanRef) -> usize {
    if plan.as_logical_share().is_some() {
        return 1;
    }
    1 + plan.inputs().iter().map(size).sum::<usize>()
}

fn mark_subtree(plan: &PlanRef, nodes: &mut HashSet<PlanNodeId>) {
    if !nodes.insert(plan.id()) || plan.as_logical_share().is_some() {
        return;
    }
    for input in plan.inputs() {
        mark_subtree(&input, nodes);
    }
}

/// Rebuilds the plan with the replacements, keeping the nodes whose sub-plan does not change.
fn replace(
    plan: &PlanRef,
    replacements: &HashMap<PlanNodeId, PlanRef>,
    memo: &mut HashMap<PlanNodeId, PlanRef>,
) -> PlanRef {
    if let Some(replacement) = replacements.get(&plan.id()) {
        return replacement.clone();
    }
    if plan.as_logical_share().is_some() {
        return plan.clone();
    }
    if let Some(result) = memo.get(&plan.id()) {
        return result.clone();
    }
    let inputs = plan.inputs();
    let new_inputs = inputs
        .iter()
        .map(|input| replace(input, replacements, memo))
        .collect_vec();
    let result = if inputs
        .iter()
        .zip_eq(&new_inputs)
        .all(|(old, new)| old.id() == new.id())
    {
        plan.clone()
    } else {
        plan.clone_root_with_inputs(&new_inputs)
    };
    memo.insert(plan.id(), result.clone());
    result
}

/// The core that computes several copies.
struct Merged {
    plan: PlanRef,
    /// For each copy, the column of the core that holds each of its columns.
    maps: Vec<Vec<usize>>,
    /// For each copy, the conjuncts over the columns of the core that select its rows.
    residuals: Vec<Vec<ExprImpl>>,
}

/// A mapping of the columns of a copy to the columns of the core.
fn mapping(map: &[usize], target_size: usize) -> ColIndexMapping {
    ColIndexMapping::new(
        map.iter().map(|&column| Some(column)).collect(),
        target_size,
    )
}

fn rewrite(expr: &ExprImpl, map: &[usize], target_size: usize) -> ExprImpl {
    mapping(map, target_size).rewrite_expr(expr.clone())
}

fn merge(copies: &[PlanRef]) -> Option<Merged> {
    if copies
        .iter()
        .any(|copy| copy.as_logical_project().is_some())
    {
        return merge_projects(copies);
    }
    if copies.iter().any(|copy| copy.as_logical_filter().is_some()) {
        return merge_filters(copies);
    }
    if copies.iter().all(|copy| copy.as_logical_scan().is_some()) {
        return merge_scans(copies);
    }
    if copies.iter().all(|copy| copy.as_logical_join().is_some()) {
        return merge_joins(copies);
    }
    if copies.iter().all(|copy| copy.as_logical_agg().is_some()) {
        return merge_aggs(copies);
    }
    // Any other node must be the same in every copy, columns included.
    if copies.iter().all(|copy| *copy == copies[0]) {
        let columns = copies[0].schema().len();
        return Some(Merged {
            plan: copies[0].clone(),
            maps: vec![(0..columns).collect(); copies.len()],
            residuals: vec![vec![]; copies.len()],
        });
    }
    None
}

/// A projection that keeps every column of the merged input and computes the expressions of every
/// copy once.
fn merge_projects(copies: &[PlanRef]) -> Option<Merged> {
    let peeled = copies
        .iter()
        .map(|copy| match copy.as_logical_project() {
            Some(project) => (project.exprs().clone(), project.input()),
            None => (
                (0..copy.schema().len())
                    .map(|index| InputRef::new(index, copy.schema()[index].data_type()).into())
                    .collect(),
                copy.clone(),
            ),
        })
        .collect_vec();
    let inputs = peeled.iter().map(|(_, input)| input.clone()).collect_vec();
    let child = merge(&inputs)?;
    let child_len = child.plan.schema().len();
    let mut exprs: Vec<ExprImpl> = (0..child_len)
        .map(|index| InputRef::new(index, child.plan.schema()[index].data_type()).into())
        .collect();
    let mut maps = vec![];
    for (index, (copy_exprs, _)) in peeled.iter().enumerate() {
        let mut map = vec![];
        for expr in copy_exprs {
            let expr = rewrite(expr, &child.maps[index], child_len);
            if let Some(input_ref) = expr.as_input_ref() {
                map.push(input_ref.index());
                continue;
            }
            if !expr.is_pure() {
                return None;
            }
            let position = match exprs.iter().position(|existing| *existing == expr) {
                Some(position) => position,
                None => {
                    exprs.push(expr);
                    exprs.len() - 1
                }
            };
            map.push(position);
        }
        maps.push(map);
    }
    Some(Merged {
        plan: LogicalProject::create(child.plan, exprs),
        maps,
        // The residual conjuncts read columns of the input, which the projection keeps in place.
        residuals: child.residuals,
    })
}

/// A filter with the conjuncts of every copy; the others are residual.
fn merge_filters(copies: &[PlanRef]) -> Option<Merged> {
    let peeled = copies
        .iter()
        .map(|copy| match copy.as_logical_filter() {
            Some(filter) => (filter.predicate().conjunctions.clone(), filter.input()),
            None => (vec![], copy.clone()),
        })
        .collect_vec();
    let inputs = peeled.iter().map(|(_, input)| input.clone()).collect_vec();
    let child = merge(&inputs)?;
    let child_len = child.plan.schema().len();
    let conjuncts = peeled
        .iter()
        .enumerate()
        .map(|(index, (conjuncts, _))| {
            conjuncts
                .iter()
                .map(|conjunct| rewrite(conjunct, &child.maps[index], child_len))
                .collect_vec()
        })
        .collect_vec();
    let (common, residuals) = split_common(&conjuncts, child.residuals)?;
    let plan = if common.is_empty() {
        child.plan
    } else {
        LogicalFilter::create(
            child.plan,
            Condition {
                conjunctions: common,
            },
        )
    };
    Some(Merged {
        plan,
        maps: child.maps,
        residuals,
    })
}

/// The conjuncts that every copy has, and for each copy its other conjuncts appended to
/// `residuals`. A residual conjunct must be pure: it is evaluated at another place.
fn split_common(
    conjuncts: &[Vec<ExprImpl>],
    mut residuals: Vec<Vec<ExprImpl>>,
) -> Option<(Vec<ExprImpl>, Vec<Vec<ExprImpl>>)> {
    let common = conjuncts[0]
        .iter()
        .filter(|conjunct| conjuncts.iter().all(|other| other.contains(conjunct)))
        .cloned()
        .collect_vec();
    for (index, copy_conjuncts) in conjuncts.iter().enumerate() {
        for conjunct in copy_conjuncts {
            if !common.contains(conjunct) {
                if !conjunct.is_pure() {
                    return None;
                }
                residuals[index].push(conjunct.clone());
            }
        }
    }
    Some((common, residuals))
}

/// Scans of the same table: the columns of every copy, the predicate conjuncts of every copy.
fn merge_scans(copies: &[PlanRef]) -> Option<Merged> {
    let scans = copies
        .iter()
        .map(|copy| copy.as_logical_scan().unwrap())
        .collect_vec();
    let first = scans[0];
    if scans
        .iter()
        .any(|scan| scan.table().id() != first.table().id() || scan.as_of() != first.as_of())
    {
        return None;
    }
    let table_len = first.table().columns.len();
    let conjuncts = scans
        .iter()
        .map(|scan| scan.predicate().conjunctions.clone())
        .collect_vec();
    // The conjuncts are on the columns of the table.
    let (common, table_residuals) = split_common(&conjuncts, vec![vec![]; scans.len()])?;
    let mut columns: Vec<usize> = scans
        .iter()
        .flat_map(|scan| scan.output_col_idx().iter().copied())
        .collect();
    for conjunct in table_residuals.iter().flatten() {
        columns.extend(conjunct.collect_input_refs(table_len).ones());
    }
    let columns = columns.into_iter().sorted().dedup().collect_vec();
    let position = |table_column: usize| columns.iter().position(|c| *c == table_column);
    let to_output = ColIndexMapping::new((0..table_len).map(position).collect(), columns.len());
    let maps = scans
        .iter()
        .map(|scan| {
            scan.output_col_idx()
                .iter()
                .map(|column| position(*column).unwrap())
                .collect_vec()
        })
        .collect_vec();
    let residuals = table_residuals
        .into_iter()
        .map(|conjuncts| {
            conjuncts
                .into_iter()
                .map(|conjunct| to_output.clone().rewrite_expr(conjunct))
                .collect_vec()
        })
        .collect_vec();
    let plan = first
        .clone_with_output_indices(columns)
        .clone_with_predicate(Condition {
            conjunctions: common,
        })
        .into();
    Some(Merged {
        plan,
        maps,
        residuals,
    })
}

/// Joins of the same type and condition. The residual conjuncts of a side are lifted when they
/// commute with the join.
fn merge_joins(copies: &[PlanRef]) -> Option<Merged> {
    let joins = copies
        .iter()
        .map(|copy| copy.as_logical_join().unwrap())
        .collect_vec();
    let join_type = joins[0].join_type();
    if joins.iter().any(|join| join.join_type() != join_type) {
        return None;
    }
    let (left_lifts, right_lifts, right_in_output) = match join_type {
        JoinType::Inner => (true, true, true),
        JoinType::LeftOuter => (true, false, true),
        JoinType::RightOuter => (false, true, true),
        JoinType::FullOuter => (false, false, true),
        JoinType::LeftSemi | JoinType::LeftAnti => (true, false, false),
        _ => return None,
    };
    let left = merge(&joins.iter().map(|join| join.left()).collect_vec())?;
    let right = merge(&joins.iter().map(|join| join.right()).collect_vec())?;
    if (!left_lifts && left.residuals.iter().any(|r| !r.is_empty()))
        || (!right_lifts && right.residuals.iter().any(|r| !r.is_empty()))
    {
        return None;
    }
    let left_len = left.plan.schema().len();
    let right_len = right.plan.schema().len();
    // For each copy, the columns of its left and right inputs in those of the core.
    let concat_maps = joins
        .iter()
        .enumerate()
        .map(|(index, _)| {
            left.maps[index]
                .iter()
                .copied()
                .chain(right.maps[index].iter().map(|column| left_len + column))
                .collect_vec()
        })
        .collect_vec();
    let conditions = joins
        .iter()
        .enumerate()
        .map(|(index, join)| {
            join.on()
                .conjunctions
                .iter()
                .map(|conjunct| rewrite(conjunct, &concat_maps[index], left_len + right_len))
                .collect_vec()
        })
        .collect_vec();
    let same_condition = conditions.iter().all(|condition| {
        condition.len() == conditions[0].len()
            && condition
                .iter()
                .all(|conjunct| conditions[0].contains(conjunct))
    });
    if !same_condition {
        return None;
    }
    let plan: PlanRef = LogicalJoin::new(
        left.plan,
        right.plan,
        join_type,
        Condition {
            conjunctions: conditions[0].clone(),
        },
    )
    .into();
    let maps = joins
        .iter()
        .enumerate()
        .map(|(index, join)| {
            join.output_indices()
                .iter()
                .map(|&internal| concat_maps[index][internal])
                .collect_vec()
        })
        .collect_vec();
    let residuals = (0..joins.len())
        .map(|index| {
            let mut conjuncts = left.residuals[index].clone();
            if right_in_output {
                let shift = (0..right_len).map(|c| left_len + c).collect_vec();
                conjuncts.extend(
                    right.residuals[index]
                        .iter()
                        .map(|conjunct| rewrite(conjunct, &shift, left_len + right_len)),
                );
            }
            conjuncts
        })
        .collect_vec();
    Some(Merged {
        plan,
        maps,
        residuals,
    })
}

/// Aggregations with the same group key. Their calls are united. The residual conjuncts of a copy
/// are lifted when they only read group key columns; otherwise they become a `FILTER` on the calls
/// of the copy, and the copy keeps the groups where its own input had rows.
fn merge_aggs(copies: &[PlanRef]) -> Option<Merged> {
    let aggs = copies
        .iter()
        .map(|copy| copy.as_logical_agg().unwrap())
        .collect_vec();
    if aggs.iter().any(|agg| !agg.grouping_sets().is_empty()) {
        return None;
    }
    let child = merge(&aggs.iter().map(|agg| agg.input()).collect_vec())?;
    let child_len = child.plan.schema().len();
    let group_keys = aggs
        .iter()
        .enumerate()
        .map(|(index, agg)| {
            agg.group_key()
                .indices()
                .map(|key| child.maps[index][key])
                .collect::<IndexSet>()
        })
        .collect_vec();
    if group_keys.iter().any(|key| *key != group_keys[0]) {
        return None;
    }
    let group_key = group_keys[0].clone();
    let group_columns: Vec<usize> = group_key.indices().collect();
    let key_position = |column: usize| group_columns.iter().position(|c| *c == column);

    let mut calls: Vec<PlanAggCall> = vec![];
    let mut maps = vec![];
    // For each copy, the call that counts its rows in a group, when it filters them.
    let mut existence = vec![];
    // For each copy, whether its residual conjuncts became a `FILTER` of its calls.
    let mut absorbed = vec![];
    for (index, agg) in aggs.iter().enumerate() {
        let residual = &child.residuals[index];
        let liftable = !group_columns.is_empty()
            && residual.iter().all(|conjunct| {
                conjunct
                    .collect_input_refs(child_len)
                    .ones()
                    .all(|column| key_position(column).is_some())
            });
        let call_filter = if residual.is_empty() || liftable {
            vec![]
        } else {
            residual.clone()
        };
        let mut map = agg
            .group_key()
            .indices()
            .map(|key| key_position(child.maps[index][key]).unwrap())
            .collect_vec();
        for call in agg.agg_calls() {
            let mut call = call.clone();
            call.rewrite_input_index(mapping(&child.maps[index], child_len));
            call.filter.conjunctions.extend(call_filter.iter().cloned());
            map.push(group_columns.len() + call_position(call, &mut calls));
        }
        maps.push(map);
        absorbed.push(!call_filter.is_empty());
        if !call_filter.is_empty() && !group_columns.is_empty() {
            let count = PlanAggCall::count_star().with_condition(Condition {
                conjunctions: call_filter,
            });
            existence.push(Some(call_position(count, &mut calls)));
        } else {
            existence.push(None);
        }
    }
    let residuals = aggs
        .iter()
        .enumerate()
        .map(|(index, _)| match existence[index] {
            Some(call) => {
                let count: ExprImpl =
                    InputRef::new(group_columns.len() + call, calls[call].return_type.clone())
                        .into();
                let positive = crate::expr::FunctionCall::new(
                    crate::expr::ExprType::GreaterThan,
                    vec![count, ExprImpl::literal_bigint(0)],
                )
                .ok()?;
                Some(vec![positive.into()])
            }
            None => {
                let residual = &child.residuals[index];
                if residual.is_empty() || absorbed[index] {
                    return Some(vec![]);
                }
                // Lifted: the conjuncts read group key columns only.
                let to_output = ColIndexMapping::new(
                    (0..child_len).map(key_position).collect(),
                    group_columns.len() + calls.len(),
                );
                Some(
                    residual
                        .iter()
                        .map(|conjunct| to_output.clone().rewrite_expr(conjunct.clone()))
                        .collect_vec(),
                )
            }
        })
        .collect::<Option<Vec<_>>>()?;
    let plan: PlanRef = Agg::new(calls, group_key, child.plan)
        .with_enable_two_phase(aggs[0].core().enable_two_phase)
        .into();
    Some(Merged {
        plan,
        maps,
        residuals,
    })
}

/// The position of a call among `calls`, added if it is not there yet.
fn call_position(call: PlanAggCall, calls: &mut Vec<PlanAggCall>) -> usize {
    match calls.iter().position(|existing| *existing == call) {
        Some(position) => position,
        None => {
            calls.push(call);
            calls.len() - 1
        }
    }
}
