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

//! Joins once, above a `UNION ALL`, the relation that several of its branches join on the same
//! columns.
//!
//! ```text
//! Union(all)                                  Project(CASE tag WHEN 0 THEN .. WHEN 1 THEN .. END)
//! ├─Project                                   └─Join(on: u.key = c.k)
//! │ └─Join(on: a.x = c.k)                 =>    ├─Union(all)
//! │   ├─A                                       │ ├─Project(a.x as key, 0 as tag, ..)
//! │   └─C                                       │ │ └─A
//! └─Project                                     │ └─Project(b.y as key, 1 as tag, ..)
//!   └─Join(on: b.y = c.k)                       │   └─B
//!     ├─B                                       └─C
//!     └─C
//! ```
//!
//! A join distributes over a union of its left input, for the inner, left outer, left semi and
//! left anti joins: `(A ⋈ C) ∪ (B ⋈ C) = (A ∪ B) ⋈ C`. No key is needed. The joined relation is
//! computed once and its join state is kept once; the rows of the branches are kept in one state
//! instead of one per branch. Used on demand, behind `streaming_factor_union_join` for streaming
//! plans and `batch_factor_union_join` for batch plans.
//!
//! The joined relation `C` is the scan of a table, or a sub-plan (a view, a shared sub-plan,
//! joins, aggregations) that is the same plan in every merged branch and that gives the same rows
//! wherever it is computed (see [`is_deterministic`]).
//!
//! What differs between the branches moves around the single join:
//! - the rows of the union carry the number of their branch (the *tag*);
//! - what a branch requires of the joined row besides the equalities (the rest of its join
//!   condition, the filter of its scan or of the sub-plan) is checked in the join condition for
//!   the rows of that branch only;
//! - the filter and the projection that a branch applies above its join are applied above the
//!   single join, to the rows of that branch only.
//!
//! A semi or anti join that a branch applies above its join, on the columns of one input of that
//! join only, may first be moved below the join, into that input (see [`sink`]): the join below
//! can then be the one that the branches share.
//!
//! The union of the other inputs of the single join, and the union of the merged branches with
//! the branches left as they are, are looked at again: their branches may share another join.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{
    and_all, commute, map_input_refs, pure_scan, referred_columns, table_column,
};
use crate::catalog::table_catalog::TableType;
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, LogicalScan, LogicalUnion, PlanNodeId,
    PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// In the expressions of a [`Branch`], a column reference below this index is a column of the
/// left input of the join of the branch, and `JOINED + j` is the `j`-th expression of
/// [`Joined::exprs`].
const JOINED: usize = 1 << 24;

/// How many times a branch may move the semi or anti join at its top below the join under it
/// (see [`sink`]).
const MAX_SUNK: usize = 3;

/// The expressions over the columns of the joined relation that the branches use: each one is
/// computed once below the single join, so that an outer join pads it with `NULL` as it did.
#[derive(Default)]
struct Joined {
    exprs: Vec<ExprImpl>,
}

impl Joined {
    /// Returns the reference to an expression over the columns of the relation.
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

/// The relation that the join of a branch reads on its joined side.
enum Relation {
    /// The scan of a table: the expressions of the [`Side`] are over the columns of the table.
    Table(Box<LogicalScan>),
    /// Another sub-plan, which [`is_deterministic`]: the expressions of the [`Side`] are over
    /// its output columns.
    Plan(PlanRef),
}

/// The joined side of the join of a branch: the rows of the relation that satisfy the predicate,
/// computed by the expressions.
struct Side {
    relation: Relation,
    /// The conjuncts that a row of the relation satisfies to be joined.
    predicate: Vec<ExprImpl>,
    /// The output columns of the side.
    exprs: Vec<ExprImpl>,
}

/// Whether the two sides read the same rows, before their predicates.
fn is_same_relation(first: &Side, second: &Side) -> bool {
    match (&first.relation, &second.relation) {
        (Relation::Table(first), Relation::Table(second)) => {
            first.table().id() == second.table().id()
        }
        (Relation::Plan(first), Relation::Plan(second)) => {
            first.id() == second.id() || first == second
        }
        _ => false,
    }
}

/// A branch of the union, of the form `Project? (Filter)* Join(left, joined side)`.
struct Branch {
    join_type: JoinType,
    left: PlanRef,
    side: Side,
    /// The equalities of the join condition: a column of the left input, and the expression of
    /// the joined relation that it is compared with, over the columns of the relation. Sorted by
    /// the expression, to compare two branches.
    keys: Vec<(usize, ExprImpl)>,
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

/// Whether an aggregate call gives the same value whatever the order in which it sees the rows
/// of its group.
fn is_order_insensitive(call: &PlanAggCall) -> bool {
    matches!(
        call.agg_type,
        AggType::Builtin(
            PbAggKind::Count
                | PbAggKind::Sum
                | PbAggKind::Sum0
                | PbAggKind::Min
                | PbAggKind::Max
                | PbAggKind::Avg
                | PbAggKind::BitAnd
                | PbAggKind::BitOr
                | PbAggKind::BitXor
                | PbAggKind::BoolAnd
                | PbAggKind::BoolOr
                | PbAggKind::InternalLastSeenValue
        )
    ) && call.filter.conjunctions.iter().all(is_movable)
}

/// Whether a sub-plan gives the same rows wherever it is computed: it reads tables in the
/// present, and computes its rows with operators and expressions that only depend on the rows
/// they read. Two branches that join such a sub-plan join the same rows, so it can be computed
/// once for both. Anything else (`now()`, `random()`, user-defined functions, sources, a top-n
/// or a window function that may break ties differently, an aggregate that depends on the order
/// of the rows) is not merged.
fn is_deterministic(plan: &PlanRef, seen: &mut HashSet<PlanNodeId>) -> bool {
    if !seen.insert(plan.id()) {
        return true;
    }
    let allowed = if let Some(scan) = plan.as_logical_scan() {
        scan.as_of().is_none()
            && !matches!(scan.table().table_type(), TableType::Internal)
            && scan.predicate().conjunctions.iter().all(is_movable)
    } else if let Some(project) = plan.as_logical_project() {
        project.exprs().iter().all(is_movable)
    } else if let Some(filter) = plan.as_logical_filter() {
        filter.predicate().conjunctions.iter().all(is_movable)
    } else if let Some(join) = plan.as_logical_join() {
        matches!(
            join.join_type(),
            JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
        ) && join.on().conjunctions.iter().all(is_movable)
    } else if let Some(agg) = plan.as_logical_agg() {
        agg.agg_calls().iter().all(is_order_insensitive)
    } else if let Some(values) = plan.as_logical_values() {
        values.rows().iter().flatten().all(is_movable)
    } else if let Some(project_set) = plan.as_logical_project_set() {
        project_set.select_list().iter().all(is_movable)
    } else {
        plan.as_logical_share().is_some() || plan.as_logical_union().is_some()
    };
    allowed
        && plan
            .inputs()
            .iter()
            .all(|input| is_deterministic(input, seen))
}

/// Returns the joined side that a plan is, if it has the form of a [`Side`]: the scan of a table
/// with filters and projections (also through shared sub-plans), or else filters and projections
/// over a deterministic sub-plan. A shared sub-plan is kept as it is: it may have other
/// consumers.
fn side_of(plan: &PlanRef) -> Option<Side> {
    if let Some(pure) = pure_scan(plan) {
        return Some(Side {
            relation: Relation::Table(Box::new(pure.scan)),
            predicate: pure.predicate,
            exprs: pure.exprs,
        });
    }
    derived_side(plan)
}

fn derived_side(plan: &PlanRef) -> Option<Side> {
    if let Some(project) = plan.as_logical_project() {
        let mut side = derived_side(&project.input())?;
        let exprs: Vec<ExprImpl> = project
            .exprs()
            .iter()
            .map(|expr| map_input_refs(expr, |r| side.exprs[r.index()].clone()))
            .collect();
        if !exprs.iter().all(is_movable) {
            return None;
        }
        side.exprs = exprs;
        return Some(side);
    }
    if let Some(filter) = plan.as_logical_filter() {
        let mut side = derived_side(&filter.input())?;
        for conjunct in &filter.predicate().conjunctions {
            let conjunct = map_input_refs(conjunct, |r| side.exprs[r.index()].clone());
            if !is_movable(&conjunct) {
                return None;
            }
            side.predicate.push(conjunct);
        }
        return Some(side);
    }
    if !is_deterministic(plan, &mut HashSet::new()) {
        return None;
    }
    let exprs = plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| InputRef::new(index, field.data_type()).into())
        .collect();
    Some(Side {
        relation: Relation::Plan(plan.clone()),
        predicate: vec![],
        exprs,
    })
}

/// Moves a semi or anti join below the join of its left input, into the input of that join whose
/// columns its condition compares: `(L ⋈ X) ⋉ R = (L ⋉ R) ⋈ X`. Each row of the join below
/// carries the columns of one row of `L` as they are, so whether it has a match in `R` only
/// depends on that row of `L`. This holds when `L` is the left input of an inner, left outer,
/// left semi or left anti join, or either input of an inner join. The semi or anti join is moved
/// as deep as it can go. Returns the join below, which then has the semi or anti join in one of
/// its inputs.
fn sink(upper: &LogicalJoin) -> Option<LogicalJoin> {
    if !matches!(upper.join_type(), JoinType::LeftSemi | JoinType::LeftAnti) {
        return None;
    }
    let lower_plan = upper.left();
    let lower = lower_plan.as_logical_join()?;
    let lower_len = lower_plan.schema().len();
    let lower_left_len = lower.left().schema().len();
    // The internal columns of the join below that the condition compares.
    let used: BTreeSet<usize> = referred_columns(&upper.on().conjunctions)
        .into_iter()
        .filter(|&column| column < lower_len)
        .map(|column| lower.output_indices()[column])
        .collect();
    let on_left = used.iter().all(|&column| column < lower_left_len);
    let on_right = used.iter().all(|&column| column >= lower_left_len);
    let into_left = match lower.join_type() {
        JoinType::Inner if on_left => true,
        JoinType::Inner if on_right => false,
        JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti if on_left => true,
        _ => return None,
    };
    let (target, offset) = if into_left {
        (lower.left(), 0)
    } else {
        (lower.right(), lower_left_len)
    };
    let target_len = target.schema().len();
    let conjunctions = upper
        .on()
        .conjunctions
        .iter()
        .map(|conjunct| {
            map_input_refs(conjunct, |r| {
                let index = if r.index() < lower_len {
                    lower.output_indices()[r.index()] - offset
                } else {
                    r.index() - lower_len + target_len
                };
                InputRef::new(index, r.return_type()).into()
            })
        })
        .collect();
    let moved = LogicalJoin::with_output_indices(
        target,
        upper.right(),
        upper.join_type(),
        Condition { conjunctions },
        (0..target_len).collect(),
    );
    let moved: PlanRef = match sink(&moved) {
        Some(deeper) => deeper.into(),
        None => moved.into(),
    };
    let (left, right) = if into_left {
        (moved, lower.right())
    } else {
        (lower.left(), moved)
    };
    let output = upper
        .output_indices()
        .iter()
        .map(|&column| lower.output_indices()[column])
        .collect();
    Some(LogicalJoin::with_output_indices(
        left,
        right,
        lower.join_type(),
        lower.on().clone(),
        output,
    ))
}

/// Describes a branch of the union, if it has the form of a [`Branch`]. The semi or anti join at
/// the top of the branch is first moved `sunk` times below the join under it (see [`sink`]). The
/// joined relation is the right side of the join, or its left side if `commuted`, which only an
/// inner join allows.
fn branch_of(plan: &PlanRef, sunk: usize, commuted: bool, joined: &mut Joined) -> Option<Branch> {
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
    let mut join = node.as_logical_join()?.clone();
    for _ in 0..sunk {
        join = sink(&join)?;
    }
    let join = match (join.join_type(), commuted) {
        (JoinType::Inner, true) => commute(&join),
        (
            JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti,
            false,
        ) => join,
        _ => return None,
    };
    let side = side_of(&join.right())?;
    let left = join.left();
    let left_len = left.schema().len();

    // An internal column of the join, in the expressions of the branch.
    let internal = |input_ref: InputRef, joined: &mut Joined| -> ExprImpl {
        if input_ref.index() < left_len {
            input_ref.into()
        } else {
            joined.intern(&side.exprs[input_ref.index() - left_len])
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
            keys.push((first.index(), side.exprs[second.index() - left_len].clone()));
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
        side,
        keys,
        rest,
        filter,
        exprs,
    })
}

fn is_same_join(first: &Branch, second: &Branch) -> bool {
    first.join_type == second.join_type
        && is_same_relation(&first.side, &second.side)
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

/// A way to read a branch: the number of the input of the union, how many times its semi or
/// anti join is moved down, and whether the sides of its join are exchanged.
type Reading = (usize, usize, bool);

/// Joins once, above a `UNION ALL`, the relation that several of its branches join on the same
/// columns. See the documentation of the module.
///
/// The branches that are merged have the same join type (inner, left outer, left semi or left
/// anti), join the same relation, and compare columns of their other input with the same
/// expressions of the relation. The other branches of the union are kept as they are.
pub struct CommonJoinOverUnionRule;

impl Rule<Logical> for CommonJoinOverUnionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let union = plan.as_logical_union()?;
        if !union.all() || union.source_col().is_some() {
            return None;
        }
        let inputs = plan.inputs();
        // Each branch as written, and with the sides of its join exchanged; first with its semi
        // or anti join moved down, so that a branch read as written is preferred on a tie below.
        let readings = (1..=MAX_SUNK).chain([0]).flat_map(|sunk| {
            (0..inputs.len()).flat_map(move |index| {
                [false, true]
                    .into_iter()
                    .map(move |commuted| (index, sunk, commuted))
            })
        });
        let candidates: Vec<(Reading, Branch)> = readings
            .filter_map(|(index, sunk, commuted)| {
                let branch = branch_of(&inputs[index], sunk, commuted, &mut Joined::default())?;
                Some(((index, sunk, commuted), branch))
            })
            .collect();
        // The largest set of branches that share their join, one reading per branch: the first
        // one that moves its semi or anti join the least.
        let group: Vec<Reading> = candidates
            .iter()
            .filter_map(|(_, seed)| {
                let mut group: Vec<Reading> = vec![];
                for (reading, other) in &candidates {
                    if !is_same_join(seed, other) {
                        continue;
                    }
                    match group.iter_mut().find(|taken| taken.0 == reading.0) {
                        Some(taken) if reading.1 < taken.1 => *taken = *reading,
                        Some(_) => {}
                        None => group.push(*reading),
                    }
                }
                (group.len() >= 2).then_some(group)
            })
            .max_by_key(|group| group.len())?;
        // Described again together: the expressions of the joined relation that the single join
        // computes are those of the merged branches.
        let mut joined = Joined::default();
        let members: Vec<Branch> = group
            .iter()
            .map(|&(index, sunk, commuted)| branch_of(&inputs[index], sunk, commuted, &mut joined))
            .collect::<Option<_>>()?;
        let members: Vec<&Branch> = members.iter().collect();
        let merged = merge(&members, &mut joined)?;
        let others: Vec<PlanRef> = inputs
            .iter()
            .enumerate()
            .filter(|(index, _)| group.iter().all(|(member, _, _)| member != index))
            .map(|(_, input)| input.clone())
            .collect();
        if others.is_empty() {
            return Some(merged);
        }
        let mut inputs = others;
        inputs.push(merged);
        let union: PlanRef = LogicalUnion::new(true, inputs).into();
        // The branches left as they are may share another join.
        Some(self.apply(union.clone()).unwrap_or(union))
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

    // The filter of the joined relation: what every branch requires, and one of what each
    // requires besides. The latter is checked again for the rows of each branch, in the join
    // condition.
    let common: Vec<ExprImpl> = first
        .side
        .predicate
        .iter()
        .filter(|conjunct| {
            members
                .iter()
                .all(|member| member.side.predicate.contains(conjunct))
        })
        .cloned()
        .collect();
    let own: Vec<Vec<ExprImpl>> = members
        .iter()
        .map(|member| {
            member
                .side
                .predicate
                .iter()
                .filter(|conjunct| !common.contains(conjunct))
                .cloned()
                .collect()
        })
        .collect();
    let mut side_predicate = common;
    if own
        .iter()
        .all(|conjuncts: &Vec<ExprImpl>| !conjuncts.is_empty())
    {
        side_predicate.push(
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
    // The expressions of the joined relation below the join: the compared ones first.
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
    // The other inputs of the merged branches may share another join.
    let union = CommonJoinOverUnionRule
        .apply(union.clone())
        .unwrap_or(union);

    // The joined relation, read once, and its expressions.
    let right: PlanRef = match &first.side.relation {
        Relation::Table(scan) => {
            let mut read = referred_columns(&right_exprs);
            read.extend(referred_columns(&side_predicate));
            let read: Vec<usize> = read.into_iter().collect();
            let scan = scan
                .clone_with_output_indices(read.clone())
                .clone_with_predicate(Condition {
                    conjunctions: side_predicate,
                });
            let right_exprs: Vec<ExprImpl> = right_exprs
                .iter()
                .map(|expr| {
                    map_input_refs(expr, |r| {
                        let position = read
                            .iter()
                            .position(|&column| column == r.index())
                            .expect("the column is read");
                        InputRef::new(position, table_column(&scan, r.index()).return_type()).into()
                    })
                })
                .collect();
            LogicalProject::new(scan.into(), right_exprs).into()
        }
        Relation::Plan(plan) => {
            let input: PlanRef = if side_predicate.is_empty() {
                plan.clone()
            } else {
                LogicalFilter::new(
                    plan.clone(),
                    Condition {
                        conjunctions: side_predicate,
                    },
                )
                .into()
            };
            LogicalProject::new(input, right_exprs).into()
        }
    };

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
