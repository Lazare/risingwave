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

//! Joins once, above a `UNION ALL`, the table that several of its branches join on the same
//! columns.
//!
//! ```text
//! Union(all)                                  Project(CASE tag WHEN 0 THEN .. WHEN 1 THEN .. END)
//! ├─Project                                   └─Join(on: u.key = c.k)
//! │ └─Join(on: a.x = c.k)                 =>    ├─Union(all)
//! │   ├─A                                       │ ├─Project(a.x as key, 0 as tag, ..)
//! │   └─Scan(c)                                 │ │ └─A
//! └─Project                                     │ └─Project(b.y as key, 1 as tag, ..)
//!   └─Join(on: b.y = c.k)                       │   └─B
//!     ├─B                                       └─Scan(c)
//!     └─Scan(c)
//! ```
//!
//! A join distributes over a union of its left input, for the inner, left outer, left semi and
//! left anti joins: `(A ⋈ C) ∪ (B ⋈ C) = (A ∪ B) ⋈ C`. No key is needed. The joined table is
//! scanned once and its join state is kept once; the rows of the branches are kept in one state
//! instead of one per branch. Only used by the streaming optimizer, on demand.
//!
//! What differs between the branches moves around the single join:
//! - the rows of the union carry the number of their branch (the *tag*);
//! - what a branch requires of the joined row besides the equalities (the rest of its join
//!   condition, the filter of its scan) is checked in the join condition for the rows of that
//!   branch only;
//! - the filter and the projection that a branch applies above its join are applied above the
//!   single join, to the rows of that branch only.

use std::collections::{BTreeMap, BTreeSet};

use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{
    PureScan, and_all, commute, map_input_refs, pure_scan, referred_columns, table_column,
};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, LogicalUnion, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// In the expressions of a [`Branch`], a column reference below this index is a column of the
/// left input of the join of the branch, and `JOINED + j` is the `j`-th expression of
/// [`Joined::exprs`].
const JOINED: usize = 1 << 24;

/// The expressions over the columns of the joined table that the branches use: each one is
/// computed once below the single join, so that an outer join pads it with `NULL` as it did.
#[derive(Default)]
struct Joined {
    exprs: Vec<ExprImpl>,
}

impl Joined {
    /// Returns the reference to an expression over the columns of the table.
    fn intern(&mut self, expr: &ExprImpl) -> ExprImpl {
        let position = self
            .exprs
            .iter()
            .position(|other| other == expr)
            .unwrap_or_else(|| {
                self.exprs.push(expr.clone());
                self.exprs.len() - 1
            });
        InputRef::new(JOINED + position, expr.return_type()).into()
    }
}

/// A branch of the union, of the form `Project? (Filter)* Join(left, joined table)`.
struct Branch {
    join_type: JoinType,
    left: PlanRef,
    pure: PureScan,
    /// The equalities of the join condition: a column of the left input, and the expression of
    /// the joined table that it is compared with, over the columns of the table. Sorted by the
    /// expression, to compare two branches.
    keys: Vec<(usize, ExprImpl)>,
    /// The conjuncts of the filter of the scan of the joined table, over its columns.
    scan_predicate: Vec<ExprImpl>,
    /// The rest of the join condition.
    rest: Vec<ExprImpl>,
    /// The filters above the join.
    filter: Vec<ExprImpl>,
    /// The output columns of the branch.
    exprs: Vec<ExprImpl>,
}

fn is_movable(expr: &ExprImpl) -> bool {
    expr.is_pure() && expr.count_nows() == 0
}

/// Describes a branch of the union, if it has the form of a [`Branch`]. The joined table is the
/// right side of the join, or its left side if `commuted`, which only an inner join allows.
fn branch_of(plan: &PlanRef, commuted: bool, joined: &mut Joined) -> Option<Branch> {
    // The projection and the filters above the join, over the output of the node reached.
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
    let join = node.as_logical_join()?;
    let commuted_join;
    let join = match (join.join_type(), commuted) {
        (JoinType::Inner, true) => {
            commuted_join = commute(join);
            &commuted_join
        }
        (
            JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti,
            false,
        ) => join,
        _ => return None,
    };
    let pure = pure_scan(&join.right())?;
    let left = join.left();
    let left_len = left.schema().len();

    // An internal column of the join, in the expressions of the branch.
    let internal = |input_ref: InputRef, joined: &mut Joined| -> ExprImpl {
        if input_ref.index() < left_len {
            input_ref.into()
        } else {
            joined.intern(&pure.exprs[input_ref.index() - left_len])
        }
    };
    let mut keys = vec![];
    let mut rest = vec![];
    for conjunct in &join.on().conjunctions {
        if let Some((first, second)) = conjunct.as_eq_cond()
            && first.index() < left_len
            && second.index() >= left_len
            && first.return_type() == second.return_type()
        {
            keys.push((first.index(), pure.exprs[second.index() - left_len].clone()));
        } else {
            rest.push(map_input_refs(conjunct, |r| internal(r, joined)));
        }
    }
    if keys.is_empty() {
        return None;
    }
    keys.sort_by_cached_key(|(_, expr)| format!("{expr:?}"));

    // The output columns of the join are a selection of its internal columns.
    let output = join.output_indices();
    let from_output = |expr: &ExprImpl, joined: &mut Joined| {
        map_input_refs(expr, |r| {
            let index = output[r.index()];
            internal(InputRef::new(index, r.return_type()), joined)
        })
    };
    let exprs: Vec<ExprImpl> = match exprs {
        Some(exprs) => exprs.iter().map(|e| from_output(e, joined)).collect(),
        None => join
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                from_output(&InputRef::new(index, field.data_type()).into(), joined)
            })
            .collect(),
    };
    let filter: Vec<ExprImpl> = filter.iter().map(|e| from_output(e, joined)).collect();
    if !exprs.iter().chain(&filter).chain(&rest).all(is_movable) {
        return None;
    }
    Some(Branch {
        join_type: join.join_type(),
        left,
        scan_predicate: pure.predicate.clone(),
        pure,
        keys,
        rest,
        filter,
        exprs,
    })
}

fn is_same_join(first: &Branch, second: &Branch) -> bool {
    first.join_type == second.join_type
        && first.pure.scan.table().id() == second.pure.scan.table().id()
        && first.keys.len() == second.keys.len()
        && first
            .keys
            .iter()
            .zip_eq_fast(second.keys.iter())
            .all(|((_, first), (_, second))| first == second)
}

fn tag_is(tag: usize, tag_column: usize) -> ExprImpl {
    FunctionCall::new_unchecked(
        ExprType::Equal,
        vec![
            InputRef::new(tag_column, DataType::Int32).into(),
            ExprImpl::literal_int(tag as i32),
        ],
        DataType::Boolean,
    )
    .into()
}

/// `(tag = 0 AND <conditions of branch 0>) OR (tag = 1 AND ..) OR ..`, or nothing when no branch
/// has a condition.
fn per_branch(conditions: Vec<Vec<ExprImpl>>, tag_column: usize) -> Option<ExprImpl> {
    if conditions.iter().all(|conjuncts| conjuncts.is_empty()) {
        return None;
    }
    conditions
        .into_iter()
        .enumerate()
        .map(|(tag, mut conjuncts)| {
            conjuncts.insert(0, tag_is(tag, tag_column));
            and_all(conjuncts)
        })
        .reduce(|left, right| {
            FunctionCall::new_unchecked(ExprType::Or, vec![left, right], DataType::Boolean).into()
        })
}

/// Joins once, above a `UNION ALL`, the table that several of its branches join on the same
/// columns. See the documentation of the module.
///
/// The branches that are merged have the same join type (inner, left outer, left semi or left
/// anti), join the same table, and compare columns of their other input with the same
/// expressions of the table. The other branches of the union are kept as they are.
pub struct CommonJoinOverUnionRule;

impl Rule<Logical> for CommonJoinOverUnionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let union = plan.as_logical_union()?;
        if !union.all() || union.source_col().is_some() {
            return None;
        }
        let inputs = plan.inputs();
        // Each branch as written, and with the sides of its join exchanged.
        let candidates: Vec<(usize, bool, Branch)> = inputs
            .iter()
            .enumerate()
            .flat_map(|(index, input)| {
                [false, true].into_iter().filter_map(move |commuted| {
                    let branch = branch_of(input, commuted, &mut Joined::default())?;
                    Some((index, commuted, branch))
                })
            })
            .collect();
        // The largest set of branches that share their join, one description per branch.
        let group: Vec<(usize, bool)> = candidates
            .iter()
            .filter_map(|(_, _, seed)| {
                let mut group: Vec<(usize, bool)> = vec![];
                for (index, commuted, other) in &candidates {
                    if is_same_join(seed, other) && group.iter().all(|(taken, _)| taken != index) {
                        group.push((*index, *commuted));
                    }
                }
                (group.len() >= 2).then_some(group)
            })
            .max_by_key(|group| group.len())?;
        // Described again together: the expressions of the joined table that the single join
        // computes are those of the merged branches.
        let mut joined = Joined::default();
        let members: Vec<Branch> = group
            .iter()
            .map(|&(index, commuted)| branch_of(&inputs[index], commuted, &mut joined))
            .collect::<Option<_>>()?;
        let members: Vec<&Branch> = members.iter().collect();
        let merged = merge(&members, &mut joined)?;
        let others: Vec<PlanRef> = inputs
            .iter()
            .enumerate()
            .filter(|(index, _)| group.iter().all(|(member, _)| member != index))
            .map(|(_, input)| input.clone())
            .collect();
        if others.is_empty() {
            return Some(merged);
        }
        let mut inputs = others;
        inputs.push(merged);
        Some(LogicalUnion::new(true, inputs).into())
    }
}

impl CommonJoinOverUnionRule {
    pub fn create() -> BoxedRule {
        Box::new(CommonJoinOverUnionRule)
    }
}

/// Builds the single join of the branches, which share their join (see [`is_same_join`]).
fn merge(members: &[&Branch], joined: &mut Joined) -> Option<PlanRef> {
    let first = members[0];
    let key_len = first.keys.len();
    let tag_column = key_len;

    // The filter of the single scan: what every branch requires, and one of what each requires
    // besides. The latter is checked again for the rows of each branch, in the join condition.
    let common: Vec<ExprImpl> = first
        .scan_predicate
        .iter()
        .filter(|conjunct| {
            members
                .iter()
                .all(|member| member.scan_predicate.contains(conjunct))
        })
        .cloned()
        .collect();
    let own: Vec<Vec<ExprImpl>> = members
        .iter()
        .map(|member| {
            member
                .scan_predicate
                .iter()
                .filter(|conjunct| !common.contains(conjunct))
                .cloned()
                .collect()
        })
        .collect();
    let mut scan_predicate = common;
    if own
        .iter()
        .all(|conjuncts: &Vec<ExprImpl>| !conjuncts.is_empty())
    {
        scan_predicate.push(
            own.iter()
                .map(|conjuncts| and_all(conjuncts.clone()))
                .reduce(|left, right| {
                    FunctionCall::new_unchecked(ExprType::Or, vec![left, right], DataType::Boolean)
                        .into()
                })
                .expect("at least two branches"),
        );
    }
    // What a branch requires of the joined row besides the equalities.
    let on_rest: Vec<Vec<ExprImpl>> = members
        .iter()
        .zip_eq_fast(own.iter())
        .map(|(member, own)| {
            let mut conjuncts = member.rest.clone();
            conjuncts.extend(own.iter().map(|conjunct| joined.intern(conjunct)));
            conjuncts
        })
        .collect();
    // The expressions of the joined table below the join: the compared ones first.
    let key_exprs: Vec<ExprImpl> = first.keys.iter().map(|(_, expr)| expr.clone()).collect();
    let right_exprs: Vec<ExprImpl> = key_exprs.iter().chain(&joined.exprs).cloned().collect();

    // The columns of the left inputs that each branch uses above its join or in the rest of its
    // condition, each in a column of the union of its type: a branch leaves the columns of the
    // others `NULL`.
    let mut slots: Vec<DataType> = vec![];
    let mut slot_of: Vec<BTreeMap<usize, usize>> = vec![];
    for (member, on_rest) in members.iter().zip_eq_fast(on_rest.iter()) {
        let used: BTreeSet<usize> = referred_columns(
            member
                .exprs
                .iter()
                .chain(&member.filter)
                .chain(on_rest.iter()),
        )
        .into_iter()
        .filter(|&column| column < JOINED)
        .collect();
        let mut taken = BTreeSet::new();
        let mut mapping = BTreeMap::new();
        for column in used {
            let data_type = member.left.schema().fields()[column].data_type();
            let slot = (0..slots.len())
                .find(|slot| slots[*slot] == data_type && !taken.contains(slot))
                .unwrap_or_else(|| {
                    slots.push(data_type.clone());
                    slots.len() - 1
                });
            taken.insert(slot);
            mapping.insert(column, slot);
        }
        slot_of.push(mapping);
    }
    let union_len = key_len + 1 + slots.len();

    // The union of the left inputs: the compared columns, the tag, the used columns.
    let union_inputs: Vec<PlanRef> = members
        .iter()
        .enumerate()
        .map(|(tag, member)| {
            let column = |index: usize| -> ExprImpl {
                InputRef::new(index, member.left.schema().fields()[index].data_type()).into()
            };
            let mut exprs: Vec<ExprImpl> =
                member.keys.iter().map(|(key, _)| column(*key)).collect();
            exprs.push(ExprImpl::literal_int(tag as i32));
            for (slot, data_type) in slots.iter().enumerate() {
                let used = slot_of[tag].iter().find(|(_, other)| **other == slot);
                exprs.push(match used {
                    Some((index, _)) => column(*index),
                    None => ExprImpl::literal_null(data_type.clone()),
                });
            }
            LogicalProject::new(member.left.clone(), exprs).into()
        })
        .collect();
    let union: PlanRef = LogicalUnion::new(true, union_inputs).into();

    // The single scan and the expressions of the joined table.
    let mut read = referred_columns(&right_exprs);
    read.extend(referred_columns(&scan_predicate));
    let read: Vec<usize> = read.into_iter().collect();
    let scan = first
        .pure
        .scan
        .clone_with_output_indices(read.clone())
        .clone_with_predicate(Condition {
            conjunctions: scan_predicate,
        });
    let right_exprs: Vec<ExprImpl> = right_exprs
        .iter()
        .map(|expr| {
            map_input_refs(expr, |r| {
                let position = read
                    .iter()
                    .position(|&column| column == r.index())
                    .expect("the column is read");
                InputRef::new(
                    position,
                    table_column(&first.pure.scan, r.index()).return_type(),
                )
                .into()
            })
        })
        .collect();
    let right: PlanRef = LogicalProject::new(scan.into(), right_exprs).into();

    // An expression of a branch, over the internal columns of the single join.
    let on_join = |tag: usize, expr: &ExprImpl| {
        map_input_refs(expr, |r| {
            let index = if r.index() >= JOINED {
                union_len + key_len + (r.index() - JOINED)
            } else {
                key_len + 1 + slot_of[tag][&r.index()]
            };
            InputRef::new(index, r.return_type()).into()
        })
    };
    let per_member = |conjuncts: Vec<&Vec<ExprImpl>>| -> Vec<Vec<ExprImpl>> {
        conjuncts
            .into_iter()
            .enumerate()
            .map(|(tag, conjuncts)| conjuncts.iter().map(|e| on_join(tag, e)).collect())
            .collect()
    };

    let mut on: Vec<ExprImpl> = first
        .keys
        .iter()
        .enumerate()
        .map(|(position, (_, expr))| {
            let data_type = expr.return_type();
            FunctionCall::new_unchecked(
                ExprType::Equal,
                vec![
                    InputRef::new(position, data_type.clone()).into(),
                    InputRef::new(union_len + position, data_type).into(),
                ],
                DataType::Boolean,
            )
            .into()
        })
        .collect();
    on.extend(per_branch(per_member(on_rest.iter().collect()), tag_column));
    let join: PlanRef = LogicalJoin::new(
        union,
        right,
        first.join_type,
        Condition { conjunctions: on },
    )
    .into();

    let filters = per_member(members.iter().map(|member| &member.filter).collect());
    let filtered = match per_branch(filters, tag_column) {
        Some(predicate) => LogicalFilter::new(
            join,
            Condition {
                conjunctions: vec![predicate],
            },
        )
        .into(),
        None => join,
    };
    let exprs: Vec<ExprImpl> = (0..first.exprs.len())
        .map(|column| {
            let of_branch: Vec<ExprImpl> = (0..members.len())
                .map(|tag| on_join(tag, &members[tag].exprs[column]))
                .collect();
            if of_branch.iter().all(|expr| *expr == of_branch[0]) {
                return of_branch[0].clone();
            }
            let return_type = of_branch[0].return_type();
            let mut inputs = vec![];
            for (tag, expr) in of_branch.into_iter().enumerate() {
                inputs.push(tag_is(tag, tag_column));
                inputs.push(expr);
            }
            FunctionCall::new_unchecked(ExprType::Case, inputs, return_type).into()
        })
        .collect();
    Some(LogicalProject::new(filtered, exprs).into())
}
