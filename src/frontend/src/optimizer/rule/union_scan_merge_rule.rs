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

//! Reads once the table that several branches of a `UNION ALL` read, each with its own filter
//! and projection.
//!
//! ```text
//! Union(all)                       Project(CASE WHEN flag = 0 THEN a WHEN .. END, ..)
//! ├─Project(a, x)                  └─Filter(CASE WHEN flag = 0 THEN p0 WHEN flag = 1 THEN p1 END)
//! │ └─Filter(p0)              =>     └─Expand(one copy of the row per branch, numbered by flag)
//! │   └─Scan(t)                        └─Scan(t, predicate: p0 OR p1)
//! └─Project(b, y)
//!   └─Filter(p1)
//!     └─Scan(t)
//! ```
//!
//! Each row of the table gives one row per branch whose filter is true, computed from that row
//! only: the `Expand` makes one copy of the row per branch, the copy of a branch is kept when the
//! filter of the branch is true (a `NULL` filter drops it, as before), and the projection of the
//! branch computes its columns. The multiset of rows is exactly that of the union: a row that
//! passes two filters gives two rows, a duplicate row of the input gives duplicate rows. The
//! stream key is the key of the table and the number of the branch, as the key of the union was
//! the key of the branch and the number of the branch.
//!
//! The table is scanned and backfilled once instead of once per branch. Only used by the
//! streaming optimizer, on demand.
//!
//! The branches that are merged read either the same shared sub-plan (`Share`), whatever it
//! computes, which stays shared with its other consumers, or a scan of the same table.
//!
//! The branches that are not merged this way (a join, a table function above the scan of a
//! table) still read the same table in several places: [`UnionScanShareRule`] then reads it once,
//! in a shared scan that each of them filters and projects as its own scan did.

use std::collections::{BTreeSet, HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;

use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{
    PureScan, and_all, map_input_refs, pure_scan, referred_columns, table_column,
};
use crate::catalog::table_catalog::TableType;
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalExpand, LogicalFilter, LogicalProject, LogicalScan, LogicalShare, LogicalUnion,
    PlanNodeId, PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// What the branches that are merged read.
#[derive(PartialEq)]
enum Base {
    /// A shared sub-plan, whose output columns the expressions of the branch refer to.
    Share(PlanNodeId),
    /// A scan of a table, whose columns the expressions of the branch refer to.
    Table(TableId),
}

/// A branch of the union, of the form `(Project | Filter)* base`.
struct Branch {
    base: Base,
    /// The base: the shared sub-plan, or one of the scans of the table.
    plan: PlanRef,
    /// The conjuncts that a row of the base satisfies to give a row of the branch, over the
    /// columns of the base.
    predicate: Vec<ExprImpl>,
    /// The output columns of the branch, over the columns of the base.
    exprs: Vec<ExprImpl>,
}

fn is_movable(expr: &ExprImpl) -> bool {
    expr.is_pure() && expr.count_nows() == 0
}

/// A table whose columns carry a watermark is left alone: a column of the union that each branch
/// takes from another column would lose the watermark that the union derived.
fn has_watermark(pure: &PureScan) -> bool {
    pure.scan.table().watermark_columns.ones().next().is_some()
}

/// Whether the content of a shared sub-plan may be read by merged branches: nothing in it may
/// carry a watermark (a scan of a table with a watermark, a source, `now()`), for the same reason
/// as [`has_watermark`].
fn is_mergeable_share_content(plan: &PlanRef, visited: &mut HashSet<PlanNodeId>) -> bool {
    if !visited.insert(plan.id()) {
        return true;
    }
    if plan.as_logical_now().is_some() || plan.as_logical_source().is_some() {
        return false;
    }
    if let Some(scan) = plan.as_logical_scan()
        && scan.table().watermark_columns.ones().next().is_some()
    {
        return false;
    }
    plan.inputs()
        .iter()
        .all(|input| is_mergeable_share_content(input, visited))
}

/// Describes a branch of the union, if it has the form of a [`Branch`].
fn branch_of(plan: &PlanRef) -> Option<Branch> {
    // The projection and the filters above the base, over the output of the node reached.
    let mut exprs: Option<Vec<ExprImpl>> = None;
    let mut filter: Vec<ExprImpl> = vec![];
    let mut node = plan.clone();
    loop {
        if let Some(project) = node.as_logical_project() {
            let through =
                |expr: &ExprImpl| map_input_refs(expr, |r| project.exprs()[r.index()].clone());
            exprs = Some(match &exprs {
                Some(exprs) => exprs.iter().map(through).collect(),
                None => project.exprs().clone(),
            });
            filter = filter.iter().map(through).collect();
            node = project.input();
        } else if let Some(node_filter) = node.as_logical_filter() {
            filter.extend(node_filter.predicate().conjunctions.iter().cloned());
            node = node_filter.input();
        } else {
            break;
        }
    }

    // The base, and its output columns over the columns that the branch refers to.
    let (base, base_exprs, mut predicate): (Base, Vec<ExprImpl>, Vec<ExprImpl>) =
        if node.as_logical_share().is_some() {
            // Any shared sub-plan: each of its rows gives one row per branch whose filter is true.
            if !is_mergeable_share_content(&node, &mut HashSet::new()) {
                return None;
            }
            let identity = node
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| InputRef::new(index, field.data_type()).into())
                .collect();
            (Base::Share(node.id()), identity, vec![])
        } else if node.as_logical_scan().is_some() {
            let pure = pure_scan(&node)?;
            if has_watermark(&pure) {
                return None;
            }
            (
                Base::Table(pure.scan.table().id()),
                pure.exprs,
                pure.predicate,
            )
        } else {
            return None;
        };
    let through = |expr: &ExprImpl| map_input_refs(expr, |r| base_exprs[r.index()].clone());
    let exprs: Vec<ExprImpl> = match exprs {
        Some(exprs) => exprs.iter().map(through).collect(),
        None => base_exprs.clone(),
    };
    predicate.extend(filter.iter().map(through));
    if !exprs.iter().chain(&predicate).all(is_movable) {
        return None;
    }
    Some(Branch {
        base,
        plan: node,
        predicate,
        exprs,
    })
}

fn or(left: ExprImpl, right: ExprImpl) -> ExprImpl {
    FunctionCall::new_unchecked(ExprType::Or, vec![left, right], DataType::Boolean).into()
}

/// Reads once the table that several branches of a `UNION ALL` read. See the documentation of
/// the module.
pub struct UnionScanMergeRule;

impl Rule<Logical> for UnionScanMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let union = plan.as_logical_union()?;
        if !union.all() || union.source_col().is_some() {
            return None;
        }
        let fields = plan.schema().fields().to_vec();
        let inputs = plan.inputs();
        let branches: Vec<Option<Branch>> = inputs
            .iter()
            .map(|input| {
                branch_of(input).filter(|branch| {
                    branch.exprs.len() == fields.len()
                        && branch
                            .exprs
                            .iter()
                            .zip_eq_fast(fields.iter())
                            .all(|(expr, field)| expr.return_type() == field.data_type())
                })
            })
            .collect();

        // The groups of branches that read the same base, in the order of their first branch.
        let mut groups: Vec<Vec<usize>> = vec![];
        for (index, branch) in branches.iter().enumerate() {
            let Some(branch) = branch else { continue };
            match groups
                .iter_mut()
                .find(|group| branches[group[0]].as_ref().expect("a branch").base == branch.base)
            {
                Some(group) => group.push(index),
                None => groups.push(vec![index]),
            }
        }
        groups.retain(|group| group.len() >= 2);
        if groups.is_empty() {
            return None;
        }

        // The new inputs of the union: a merged group takes the place of its first branch.
        let mut new_inputs: Vec<Option<PlanRef>> = inputs.iter().cloned().map(Some).collect();
        for group in &groups {
            let members: Vec<&Branch> = group
                .iter()
                .map(|&index| branches[index].as_ref().expect("a branch"))
                .collect();
            new_inputs[group[0]] = Some(merge(&members));
            for &index in &group[1..] {
                new_inputs[index] = None;
            }
        }
        let mut new_inputs: Vec<PlanRef> = new_inputs.into_iter().flatten().collect();
        if new_inputs.len() == 1 {
            return new_inputs.pop();
        }
        Some(LogicalUnion::new(true, new_inputs).into())
    }
}

impl UnionScanMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionScanMergeRule)
    }
}

/// Builds the single read of the branches, which read the same base.
fn merge(members: &[&Branch]) -> PlanRef {
    let first = members[0];

    // What every branch requires is checked once below the `Expand`, with one of what each
    // branch requires besides; the latter is checked again for the copy of each branch.
    let common: Vec<ExprImpl> = first
        .predicate
        .iter()
        .filter(|conjunct| {
            members
                .iter()
                .all(|member| member.predicate.contains(conjunct))
        })
        .cloned()
        .collect();
    let own: Vec<Vec<ExprImpl>> = members
        .iter()
        .map(|member| {
            member
                .predicate
                .iter()
                .filter(|conjunct| !common.contains(conjunct))
                .cloned()
                .collect()
        })
        .collect();
    let mut below = common;
    if own.iter().all(|conjuncts| !conjuncts.is_empty()) {
        below.push(
            own.iter()
                .map(|conjuncts| and_all(conjuncts.clone()))
                .reduce(or)
                .expect("at least two branches"),
        );
    }

    // The input of the `Expand`, and the columns of the table that the single scan reads: a
    // column of the base is the column of the input at its position among them. The shared
    // sub-plan is read as it is.
    let (input, read): (PlanRef, Option<Vec<usize>>) = match first.base {
        Base::Share(_) => {
            let input = if below.is_empty() {
                first.plan.clone()
            } else {
                LogicalFilter::create(
                    first.plan.clone(),
                    Condition {
                        conjunctions: below,
                    },
                )
            };
            (input, None)
        }
        Base::Table(_) => {
            let scan: &LogicalScan = first.plan.as_logical_scan().expect("a scan");
            let read: Vec<usize> = referred_columns(
                members
                    .iter()
                    .flat_map(|member| member.exprs.iter().chain(&member.predicate)),
            )
            .into_iter()
            .collect();
            let scan = scan
                .clone_with_output_indices(read.clone())
                .clone_with_predicate(Condition {
                    conjunctions: below,
                });
            (scan.into(), Some(read))
        }
    };
    let position = |column: usize| match &read {
        None => column,
        Some(read) => read
            .iter()
            .position(|&other| other == column)
            .expect("the column is read"),
    };

    // One copy of each row per branch: the copy of the branch `i` has `flag = i`. Every subset
    // keeps every column, so that no column is replaced with `NULL`; the expressions below refer
    // to the original columns, in the second half of the output.
    let width = input.schema().len();
    let types: Vec<DataType> = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type())
        .collect();
    let expand: PlanRef = LogicalExpand::create(input, vec![(0..width).collect(); members.len()]);
    let flag_is = |branch: usize| -> ExprImpl {
        FunctionCall::new_unchecked(
            ExprType::Equal,
            vec![
                InputRef::new(2 * width, DataType::Int64).into(),
                ExprImpl::literal_bigint(branch as i64),
            ],
            DataType::Boolean,
        )
        .into()
    };
    let on_expand = |expr: &ExprImpl| {
        map_input_refs(expr, |r| {
            let column = position(r.index());
            InputRef::new(width + column, types[column].clone()).into()
        })
    };

    // The copy of a branch is kept when what the branch requires besides is true. `CASE`
    // evaluates the condition of a branch on the copies of that branch only.
    let filtered = if own.iter().all(|conjuncts| conjuncts.is_empty()) {
        expand
    } else {
        let mut inputs = vec![];
        for (branch, conjuncts) in own.into_iter().enumerate() {
            inputs.push(flag_is(branch));
            inputs.push(if conjuncts.is_empty() {
                ExprImpl::literal_bool(true)
            } else {
                on_expand(&and_all(conjuncts))
            });
        }
        let predicate: ExprImpl =
            FunctionCall::new_unchecked(ExprType::Case, inputs, DataType::Boolean).into();
        LogicalFilter::create(
            expand,
            Condition {
                conjunctions: vec![predicate],
            },
        )
    };

    // The columns of the copy of a branch are those of the branch.
    let exprs: Vec<ExprImpl> = (0..first.exprs.len())
        .map(|column| {
            let of_branch: Vec<ExprImpl> = members
                .iter()
                .map(|member| on_expand(&member.exprs[column]))
                .collect();
            if of_branch.iter().all(|expr| *expr == of_branch[0]) {
                return of_branch[0].clone();
            }
            let return_type = of_branch[0].return_type();
            let last = of_branch.len() - 1;
            let mut inputs = vec![];
            for (branch, expr) in of_branch.into_iter().enumerate() {
                // The copies of the last branch are the only ones left.
                if branch < last {
                    inputs.push(flag_is(branch));
                }
                inputs.push(expr);
            }
            FunctionCall::new_unchecked(ExprType::Case, inputs, return_type).into()
        })
        .collect();
    LogicalProject::create(filtered, exprs)
}

/// Whether the scan of a table may be shared by the branches of a union. As for the merged
/// branches, the scan of a table with a watermark is left alone.
fn is_shareable_scan(scan: &LogicalScan) -> bool {
    let table = scan.table();
    scan.as_of().is_none()
        && !matches!(table.table_type(), TableType::Internal)
        && !table.stream_key().is_empty()
        && table.watermark_columns.ones().next().is_none()
}

/// The scans of a plan that may be shared, outside the shared sub-plans, which are already read
/// once.
fn collect_scans(plan: &PlanRef, scans: &mut Vec<PlanRef>) {
    if plan.as_logical_share().is_some() {
        return;
    }
    if let Some(scan) = plan.as_logical_scan() {
        if is_shareable_scan(scan) {
            scans.push(plan.clone());
        }
        return;
    }
    for input in plan.inputs() {
        collect_scans(&input, scans);
    }
}

/// Replaces the scans of a plan, outside the shared sub-plans.
fn replace_scans(plan: &PlanRef, replacements: &HashMap<PlanNodeId, PlanRef>) -> PlanRef {
    if let Some(replacement) = replacements.get(&plan.id()) {
        return replacement.clone();
    }
    if plan.as_logical_share().is_some() {
        return plan.clone();
    }
    let inputs = plan.inputs();
    if inputs.is_empty() {
        return plan.clone();
    }
    let new_inputs: Vec<PlanRef> = inputs
        .iter()
        .map(|input| replace_scans(input, replacements))
        .collect();
    if new_inputs
        .iter()
        .zip_eq_fast(inputs.iter())
        .all(|(new, old)| new.id() == old.id())
    {
        return plan.clone();
    }
    plan.clone_root_with_inputs(&new_inputs)
}

/// Reads once, in a shared scan, the table that several branches of a union read, whatever the
/// branches do with its rows.
///
/// ```text
/// Union                                Union
/// ├─Join(on: a.x = c.k)                ├─Join(on: a.x = c.k)
/// │ ├─Filter(p0)                       │ ├─Filter(p0)
/// │ │ └─Scan(a, [x])              =>   │ │ └─Project(x)
/// │ └─Scan(c)                          │ │   └─Share
/// └─ProjectSet(unnest(a.arr))          │ │     └─Scan(a, [x, arr])
///   └─Scan(a, [arr])                   │ └─Scan(c)
///                                      └─ProjectSet(unnest(arr))
///                                        └─Project(arr)
///                                          └─Share (the same)
/// ```
///
/// Each scan is replaced with a filter and a projection of the shared scan that give its rows:
/// the rows of every branch are unchanged. The table is scanned and backfilled once instead of
/// once per scan. Only the scans of the table in at least two branches are shared, and only
/// outside the shared sub-plans of the branches. Applied after [`UnionScanMergeRule`], which
/// leaves fewer operators when the branches only filter and project the table.
pub struct UnionScanShareRule;

impl Rule<Logical> for UnionScanShareRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let union = plan.as_logical_union()?;
        if union.source_col().is_some() {
            return None;
        }
        let inputs = plan.inputs();

        // The scans of each table, with the branch they are in.
        let mut groups: Vec<(TableId, Vec<(usize, PlanRef)>)> = vec![];
        for (branch, input) in inputs.iter().enumerate() {
            let mut scans = vec![];
            collect_scans(input, &mut scans);
            for scan in scans {
                let table_id = scan.as_logical_scan().expect("a scan").table().id();
                match groups.iter_mut().find(|(id, _)| *id == table_id) {
                    Some((_, members)) => {
                        if members.iter().all(|(_, other)| other.id() != scan.id()) {
                            members.push((branch, scan));
                        }
                    }
                    None => groups.push((table_id, vec![(branch, scan)])),
                }
            }
        }

        let mut replacements: HashMap<PlanNodeId, PlanRef> = HashMap::new();
        for (_, members) in &groups {
            let branches: BTreeSet<usize> = members.iter().map(|(branch, _)| *branch).collect();
            if branches.len() < 2 {
                continue;
            }
            let scans: Vec<&LogicalScan> = members
                .iter()
                .map(|(_, scan)| scan.as_logical_scan().expect("a scan"))
                .collect();
            // The shared scan reads every column that one of the scans outputs or filters on.
            let mut columns: BTreeSet<usize> = BTreeSet::new();
            for scan in &scans {
                columns.extend(scan.output_col_idx().iter().copied());
                columns.extend(referred_columns(&scan.predicate().conjunctions));
            }
            let columns: Vec<usize> = columns.into_iter().collect();
            let position = |column: usize| {
                columns
                    .iter()
                    .position(|&other| other == column)
                    .expect("the column is read")
            };
            let shared: PlanRef = LogicalShare::create(
                scans[0]
                    .clone_with_output_indices(columns.clone())
                    .clone_with_predicate(Condition::true_cond())
                    .into(),
            );
            for ((_, member), scan) in members.iter().zip_eq_fast(scans.iter()) {
                // The predicate of a scan refers to the columns of the table.
                let input = if scan.predicate().always_true() {
                    shared.clone()
                } else {
                    let conjunctions = scan
                        .predicate()
                        .conjunctions
                        .iter()
                        .map(|conjunct| {
                            map_input_refs(conjunct, |r| {
                                InputRef::new(position(r.index()), r.return_type()).into()
                            })
                        })
                        .collect();
                    LogicalFilter::create(shared.clone(), Condition { conjunctions })
                };
                let exprs: Vec<ExprImpl> = scan
                    .output_col_idx()
                    .iter()
                    .map(|&column| {
                        InputRef::new(position(column), table_column(scan, column).return_type())
                            .into()
                    })
                    .collect();
                replacements.insert(member.id(), LogicalProject::create(input, exprs));
            }
        }
        if replacements.is_empty() {
            return None;
        }
        let new_inputs: Vec<PlanRef> = inputs
            .iter()
            .map(|input| replace_scans(input, &replacements))
            .collect();
        Some(plan.clone_root_with_inputs(&new_inputs))
    }
}

impl UnionScanShareRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionScanShareRule)
    }
}
