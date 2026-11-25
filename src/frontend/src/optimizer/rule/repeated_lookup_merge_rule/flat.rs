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

//! Lookups nested in a tree of joins.
//!
//! The lookup that [`merge_lookup`] removes is a side of the join. Here it is a leaf deeper in a
//! tree of inner joins, or in the preserved side of a left outer join of the tree, and the
//! equality that designates its row may come from any join of the tree:
//!
//! ```text
//! Join(Inner, on: f.dim_id = x.id)
//! ├─Scan(f)
//! └─Join(Inner, on: x.id = d.id)          `d.id = x.id = f.dim_id`: `d` is the row that the
//!   ├─Scan(x)                             other lookup of `d` below `f` found
//!   └─Scan(d, [id, name])
//! ```
//!
//! The rows of such a tree are those of the cross product of its leaves that satisfy all the
//! conditions of its inner joins and filters, extended by its left outer joins. A left outer join
//! keeps every row of its preserved side, so it can be applied last: `(X ⟕ Y) ⋈ L` is
//! `(X ⋈ L) ⟕ Y` as long as the condition with `L` does not refer to `Y`, and a filter or an inner
//! join condition can be applied after the left outer joins of the columns that it does not
//! refer to. [`Lifter`] takes the lookup out of the tree that way: it keeps the joins that do not
//! need its columns in place, and returns the conditions and the left outer joins that do.
//!
//! - Under an inner join, the lookup is then joined with the rest of the tree, and that join is
//!   handed to [`merge_lookup`]. The left outer joins and the conditions that refer to them follow.
//! - Under the right side of a left outer, left semi or left anti join, its row is designated by
//!   the equalities of the right side and of the condition of the join, which hold in every pair
//!   of rows that the join matches. The columns of the lookup are computed from the instance in
//!   the left side, and the rest of the right side is joined instead. A semi or anti join only
//!   asks whether a row matches: the left outer joins of the right side, which never remove a
//!   row, are dropped.
//!
//! Columns are named by *virtual columns* while the tree is taken apart: the lookup has the first
//! ones, then every sub-plan that is kept as it is (a *piece*) gets new ones.

use std::collections::HashSet;

use risingwave_common::util::iter_util::ZipEqFast;

use super::*;

/// A plan whose output columns are known by their virtual columns.
struct Placed {
    plan: PlanRef,
    layout: Vec<usize>,
}

impl Placed {
    fn position(&self, column: usize) -> Option<usize> {
        self.layout.iter().position(|&other| other == column)
    }
}

/// A left outer join to apply above the rest of the tree.
struct Ext {
    right: Placed,
    /// The condition, over virtual columns.
    on: Vec<ExprImpl>,
}

/// A sub-plan of the tree without the lookup.
struct Lifted {
    /// The sub-plan without the lookup, and without the left outer joins of `exts`; `None` when
    /// the sub-plan is the lookup.
    rest: Option<Placed>,
    /// Each output column of the former sub-plan, over virtual columns.
    outputs: Vec<ExprImpl>,
    /// The conjuncts of the filters and inner joins of the sub-plan that `rest` can not apply,
    /// over virtual columns.
    lifted: Vec<ExprImpl>,
    /// The left outer joins whose preserved side held the lookup, from the lowest one.
    exts: Vec<Ext>,
}

fn is_movable(expr: &ExprImpl) -> bool {
    expr.is_pure() && expr.count_nows() == 0
}

fn columns_of(expr: &ExprImpl) -> BTreeSet<usize> {
    referred_columns([expr])
}

fn substitute(expr: &ExprImpl, columns: &[ExprImpl]) -> ExprImpl {
    map_input_refs(expr, |input_ref| columns[input_ref.index()].clone())
}

/// An expression over virtual columns, over the positions of the columns of a plan instead.
fn to_positions(expr: &ExprImpl, position: impl Fn(usize) -> Option<usize>) -> Option<ExprImpl> {
    let mut ok = true;
    let expr = map_input_refs(expr, |input_ref| match position(input_ref.index()) {
        Some(index) => InputRef::new(index, input_ref.return_type()).into(),
        None => {
            ok = false;
            input_ref.into()
        }
    });
    ok.then_some(expr)
}

/// Whether a condition compares a column of the left side with a column of the right side
/// with `=`: a streaming join needs one.
fn has_equi_condition(conjuncts: &[ExprImpl], left_len: usize) -> bool {
    conjuncts.iter().any(|conjunct| {
        conjunct
            .as_eq_cond()
            .is_some_and(|(first, second)| first.index() < left_len && second.index() >= left_len)
    })
}

/// The classes of columns that the conjuncts make equal, for the types on which two equal values
/// are indistinguishable. In every row that satisfies the conjuncts, the columns of a class hold
/// the same value, which is not `NULL`.
fn equal_classes(conjuncts: &[ExprImpl]) -> Vec<BTreeSet<usize>> {
    let mut classes: Vec<BTreeSet<usize>> = vec![];
    for (first, second) in equal_columns(conjuncts) {
        let mut merged = BTreeSet::from([first, second]);
        classes.retain(|class| {
            if class.contains(&first) || class.contains(&second) {
                merged.extend(class.iter().copied());
                false
            } else {
                true
            }
        });
        classes.push(merged);
    }
    classes
}

/// Whether a plan node is the node `target` or has it below.
fn reaches(plan: &PlanRef, target: PlanNodeId) -> bool {
    fn visit(plan: &PlanRef, target: PlanNodeId, visited: &mut HashSet<PlanNodeId>) -> bool {
        if plan.id() == target {
            return true;
        }
        // A shared sub-plan is met several times.
        if !visited.insert(plan.id()) {
            return false;
        }
        plan.inputs()
            .iter()
            .any(|input| visit(input, target, visited))
    }
    visit(plan, target, &mut HashSet::new())
}

/// Takes a lookup out of a tree of joins, see the documentation of the module.
struct Lifter {
    target: PlanNodeId,
    next: usize,
    /// The classes of equal virtual columns of the whole tree: a join that loses all its
    /// equalities to the lifted conjuncts is given the equalities of its sides that they imply.
    classes: Vec<BTreeSet<usize>>,
    /// The conjuncts of the filters and inner joins met on the way to the lookup, which hold in
    /// every row of the tree.
    seen: Vec<ExprImpl>,
    /// Whether only the existence of a row of the tree matters (the right side of a semi or anti
    /// join). Then every left outer join on the way is lifted, to be dropped: it never removes a
    /// row. And the lookup may be looked for inside a shared sub-plan: the sub-plan is not
    /// rewritten, its consumer gets a new one, which is only worth it when nothing of the
    /// sub-plan is left in that consumer (see `crossed_share`).
    through_share: bool,
    crossed_share: bool,
}

impl Lifter {
    fn new(
        target: PlanNodeId,
        next: usize,
        classes: Vec<BTreeSet<usize>>,
        through_share: bool,
    ) -> Self {
        Self {
            target,
            next,
            classes,
            seen: vec![],
            through_share,
            crossed_share: false,
        }
    }

    /// Gives new virtual columns to a piece.
    fn place(&mut self, plan: PlanRef) -> Placed {
        let len = plan.schema().len();
        let layout = (self.next..self.next + len).collect();
        self.next += len;
        Placed { plan, layout }
    }

    fn lift(&mut self, plan: &PlanRef) -> Option<Lifted> {
        if plan.id() == self.target {
            let outputs = plan
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| InputRef::new(index, field.data_type()).into())
                .collect();
            return Some(Lifted {
                rest: None,
                outputs,
                lifted: vec![],
                exts: vec![],
            });
        }
        if let Some(share) = plan.as_logical_share()
            && self.through_share
        {
            self.crossed_share = true;
            return self.lift(&share.input());
        }
        if let Some(project) = plan.as_logical_project() {
            if !project.exprs().iter().all(is_movable) {
                return None;
            }
            let mut child = self.lift(&project.input())?;
            child.outputs = project
                .exprs()
                .iter()
                .map(|expr| substitute(expr, &child.outputs))
                .collect();
            return Some(child);
        }
        if let Some(filter) = plan.as_logical_filter() {
            if !filter.predicate().conjunctions.iter().all(is_movable) {
                return None;
            }
            let mut child = self.lift(&filter.input())?;
            let conjuncts: Vec<ExprImpl> = filter
                .predicate()
                .conjunctions
                .iter()
                .map(|conjunct| substitute(conjunct, &child.outputs))
                .collect();
            self.seen.extend(conjuncts.iter().cloned());
            let mut kept = vec![];
            for conjunct in conjuncts {
                match child
                    .rest
                    .as_ref()
                    .and_then(|rest| to_positions(&conjunct, |column| rest.position(column)))
                {
                    Some(conjunct) => kept.push(conjunct),
                    None => child.lifted.push(conjunct),
                }
            }
            if let Some(rest) = &mut child.rest
                && !kept.is_empty()
            {
                rest.plan =
                    LogicalFilter::new(rest.plan.clone(), Condition { conjunctions: kept }).into();
            }
            return Some(child);
        }
        let join = plan.as_logical_join()?;
        let (left, right) = (join.left(), join.right());
        if join.join_type() == JoinType::LeftOuter && right.id() == self.target {
            // The lookup is the padded side of a left outer join: the join is lifted, to be
            // applied above with the lookup merged into it.
            let other = self.place(left);
            let internal: Vec<ExprImpl> = other
                .layout
                .iter()
                .zip_eq_fast(other.plan.schema().fields())
                .map(|(&column, field)| InputRef::new(column, field.data_type()).into())
                .chain(
                    right
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(index, field)| InputRef::new(index, field.data_type()).into()),
                )
                .collect();
            let on = join
                .on()
                .conjunctions
                .iter()
                .map(|conjunct| substitute(conjunct, &internal))
                .collect();
            let outputs = join
                .output_indices()
                .iter()
                .map(|&index| internal[index].clone())
                .collect();
            let layout = (0..right.schema().len()).collect();
            return Some(Lifted {
                rest: Some(other),
                outputs,
                lifted: vec![],
                exts: vec![Ext {
                    right: Placed {
                        plan: right,
                        layout,
                    },
                    on,
                }],
            });
        }
        let in_left = reaches(&left, self.target);
        match join.join_type() {
            JoinType::Inner => {}
            // The lookup must be in the preserved side.
            JoinType::LeftOuter if in_left => {}
            _ => return None,
        }
        let (child, other) = if in_left {
            let child = self.lift(&left)?;
            (child, self.place(right))
        } else {
            let other = self.place(left);
            (self.lift(&right)?, other)
        };
        let other_columns: Vec<ExprImpl> = other
            .layout
            .iter()
            .zip_eq_fast(other.plan.schema().fields())
            .map(|(&column, field)| InputRef::new(column, field.data_type()).into())
            .collect();
        let internal: Vec<ExprImpl> = if in_left {
            child
                .outputs
                .iter()
                .chain(&other_columns)
                .cloned()
                .collect()
        } else {
            other_columns
                .iter()
                .chain(&child.outputs)
                .cloned()
                .collect()
        };
        let on: Vec<ExprImpl> = join
            .on()
            .conjunctions
            .iter()
            .map(|conjunct| substitute(conjunct, &internal))
            .collect();
        let outputs = join
            .output_indices()
            .iter()
            .map(|&index| internal[index].clone())
            .collect();
        let Lifted {
            rest,
            mut lifted,
            mut exts,
            ..
        } = child;
        let rest = if join.join_type() == JoinType::Inner {
            self.seen.extend(on.iter().cloned());
            match rest {
                None => {
                    lifted.extend(on);
                    Some(other)
                }
                Some(rest) => {
                    let (first, second) = if in_left {
                        (rest, other)
                    } else {
                        (other, rest)
                    };
                    Some(self.inner_join(first, second, on, &mut lifted))
                }
            }
        } else {
            let in_place = rest
                .as_ref()
                .filter(|_| !self.through_share)
                .and_then(|rest| {
                    let position = |column: usize| {
                        rest.position(column)
                            .or_else(|| Some(rest.layout.len() + other.position(column)?))
                    };
                    on.iter()
                        .map(|conjunct| to_positions(conjunct, position))
                        .collect::<Option<Vec<_>>>()
                });
            match (rest, in_place) {
                (Some(rest), Some(conjunctions)) => {
                    let layout = rest.layout.iter().chain(&other.layout).copied().collect();
                    let plan = LogicalJoin::new(
                        rest.plan,
                        other.plan,
                        JoinType::LeftOuter,
                        Condition { conjunctions },
                    )
                    .into();
                    Some(Placed { plan, layout })
                }
                (rest, _) => {
                    exts.push(Ext { right: other, on });
                    rest
                }
            }
        };
        Some(Lifted {
            rest,
            outputs,
            lifted,
            exts,
        })
    }

    /// Joins two pieces with the conjuncts that they can apply; the others are lifted.
    fn inner_join(
        &self,
        first: Placed,
        second: Placed,
        on: Vec<ExprImpl>,
        lifted: &mut Vec<ExprImpl>,
    ) -> Placed {
        let layout: Vec<usize> = first.layout.iter().chain(&second.layout).copied().collect();
        let position = |column: usize| layout.iter().position(|&other| other == column);
        let mut conjunctions = vec![];
        for conjunct in on {
            match to_positions(&conjunct, position) {
                Some(conjunct) => conjunctions.push(conjunct),
                None => lifted.push(conjunct),
            }
        }
        let first_len = first.layout.len();
        if !has_equi_condition(&conjunctions, first_len) {
            // The equalities that the lifted conjuncts imply between the two pieces.
            for class in &self.classes {
                let in_first = class.iter().find_map(|&c| first.position(c));
                let in_second = class.iter().find_map(|&c| second.position(c));
                if let (Some(left), Some(right)) = (in_first, in_second) {
                    let data_type = first.plan.schema().fields()[left].data_type();
                    conjunctions.push(
                        FunctionCall::new_unchecked(
                            ExprType::Equal,
                            vec![
                                InputRef::new(left, data_type.clone()).into(),
                                InputRef::new(first_len + right, data_type).into(),
                            ],
                            DataType::Boolean,
                        )
                        .into(),
                    );
                }
            }
        }
        let plan = LogicalJoin::new(
            first.plan,
            second.plan,
            JoinType::Inner,
            Condition { conjunctions },
        )
        .into();
        Placed { plan, layout }
    }
}

/// The lookups that a tree of joins holds (see [`lookup_of`]): reached through projections,
/// filters, inner joins and the preserved side of left outer joins, and shared sub-plans if
/// `through_share`. A lookup may also be the padded side of a left outer join of the tree.
fn nested_lookups(plan: &PlanRef, through_share: bool, lookups: &mut Vec<PlanRef>) {
    if lookup_of(plan).is_some() {
        lookups.push(plan.clone());
        // A shared sub-plan with a key may hold other lookups.
        if pure_scan(plan).is_none()
            && through_share
            && let Some(share) = plan.as_logical_share()
        {
            nested_lookups(&share.input(), through_share, lookups);
        }
        return;
    }
    if let Some(project) = plan.as_logical_project() {
        nested_lookups(&project.input(), through_share, lookups);
    } else if let Some(filter) = plan.as_logical_filter() {
        nested_lookups(&filter.input(), through_share, lookups);
    } else if let Some(share) = plan.as_logical_share() {
        if through_share {
            nested_lookups(&share.input(), through_share, lookups);
        }
    } else if let Some(join) = plan.as_logical_join() {
        match join.join_type() {
            JoinType::Inner => {
                nested_lookups(&join.left(), through_share, lookups);
                nested_lookups(&join.right(), through_share, lookups);
            }
            JoinType::LeftOuter => {
                nested_lookups(&join.left(), through_share, lookups);
                let right = join.right();
                if lookup_of(&right).is_some() {
                    lookups.push(right);
                }
            }
            _ => {}
        }
    }
}

/// The relations that a plan reads, with the number of times that each is read: a read under a
/// shared sub-plan counts once per consumer.
fn read_relations(
    plan: &PlanRef,
    memo: &mut HashMap<PlanNodeId, HashMap<Relation, usize>>,
) -> HashMap<Relation, usize> {
    if let Some(tables) = memo.get(&plan.id()) {
        return tables.clone();
    }
    let mut tables: HashMap<Relation, usize> = HashMap::new();
    if let Some(scan) = plan.as_logical_scan() {
        tables.insert(Relation::Table(scan.table().id()), 1);
    }
    if plan.as_logical_share().is_some() {
        tables.insert(Relation::Share(plan.id()), 1);
    }
    for input in plan.inputs() {
        for (table, count) in read_relations(&input, memo) {
            *tables.entry(table).or_default() += count;
        }
    }
    memo.insert(plan.id(), tables.clone());
    tables
}

/// Removes a lookup nested in the tree of joins of `plan`, see the documentation of the module.
pub(super) fn merge_nested_lookup(plan: &PlanRef, join: &LogicalJoin) -> Option<PlanRef> {
    let (left, right) = (join.left(), join.right());
    let mut lookups = vec![];
    match join.join_type() {
        JoinType::Inner => nested_lookups(plan, false, &mut lookups),
        JoinType::LeftOuter => nested_lookups(&right, false, &mut lookups),
        JoinType::LeftSemi | JoinType::LeftAnti => nested_lookups(&right, true, &mut lookups),
        _ => return None,
    }
    // A lookup that is a side of the join is the one of `merge_lookup`.
    lookups.retain(|lookup| lookup.id() != left.id() && lookup.id() != right.id());
    if lookups.is_empty() {
        return None;
    }
    // Only a table that is read twice can be looked up twice.
    let tables = read_relations(plan, &mut HashMap::new());
    for lookup in lookups {
        let pure = lookup_of(&lookup)?;
        if tables.get(&pure.relation).is_none_or(|&count| count < 2) {
            continue;
        }
        let merged = if join.join_type() == JoinType::Inner {
            merge_in_inner_tree(plan, &lookup)
        } else {
            merge_in_right_side(join, &lookup, &pure)
        };
        if merged.is_some() {
            return merged;
        }
    }
    None
}

/// Runs the [`Lifter`] twice: once to find the classes of equal columns of the tree, once to
/// take the lookup out. `extra` gives the conjuncts of the condition of the join above the tree,
/// from the outputs of the tree. Also returns the conjuncts that hold in every row of the tree
/// (and of the join above), and whether the lookup was in a shared sub-plan.
fn lift_twice(
    tree: &PlanRef,
    lookup: &PlanRef,
    next: usize,
    through_share: bool,
    extra: impl Fn(&[ExprImpl]) -> Vec<ExprImpl>,
) -> Option<(Lifted, Vec<ExprImpl>, bool)> {
    let mut lifter = Lifter::new(lookup.id(), next, vec![], through_share);
    let first = lifter.lift(tree)?;
    let mut conjuncts = lifter.seen;
    conjuncts.extend(extra(&first.outputs));
    let classes = equal_classes(&conjuncts);
    let mut lifter = Lifter::new(lookup.id(), next, classes, through_share);
    let lifted = lifter.lift(tree)?;
    Some((lifted, conjuncts, lifter.crossed_share))
}

/// Whether a lifted left outer join is the one whose padded side is the lookup.
fn is_lookup_ext(ext: &Ext, lookup: &PlanRef) -> bool {
    ext.right.plan.id() == lookup.id()
}

/// The lookup is in the tree of inner joins `plan`: joined with the rest of the tree, it is a side
/// of a join, which [`merge_lookup`] removes.
fn merge_in_inner_tree(plan: &PlanRef, lookup: &PlanRef) -> Option<PlanRef> {
    let lookup_len = lookup.schema().len();
    let (lifted, _, _) = lift_twice(plan, lookup, lookup_len, false, |_| vec![])?;
    let rest = lifted.rest?;
    let ext_columns: BTreeSet<usize> = lifted
        .exts
        .iter()
        .flat_map(|ext| ext.right.layout.iter().copied())
        .collect();
    let padded = lifted.exts.iter().any(|ext| is_lookup_ext(ext, lookup));
    let (after, before): (Vec<ExprImpl>, Vec<ExprImpl>) =
        lifted.lifted.into_iter().partition(|conjunct| {
            // Without an inner join with the lookup, every conjunct is applied at the end.
            padded || !columns_of(conjunct).is_disjoint(&ext_columns)
        });
    let mut current = if padded {
        rest
    } else {
        let layout: Vec<usize> = rest.layout.iter().copied().chain(0..lookup_len).collect();
        let position = |column: usize| layout.iter().position(|&other| other == column);
        let conjunctions = before
            .iter()
            .map(|conjunct| to_positions(conjunct, position))
            .collect::<Option<Vec<_>>>()?;
        let join = LogicalJoin::new(
            rest.plan,
            lookup.clone(),
            JoinType::Inner,
            Condition {
                conjunctions: tidy(conjunctions),
            },
        );
        Placed {
            plan: merge_lookup(&join)?,
            layout,
        }
    };
    for ext in lifted.exts {
        if is_lookup_ext(&ext, lookup) {
            // The left outer join of the lookup, which `merge_lookup` removes.
            let left_len = current.layout.len();
            let position = |column: usize| {
                current
                    .position(column)
                    .or_else(|| Some(left_len + ext.right.position(column)?))
            };
            let conjunctions = ext
                .on
                .iter()
                .map(|conjunct| to_positions(conjunct, position))
                .collect::<Option<Vec<_>>>()?;
            let layout = current
                .layout
                .iter()
                .chain(&ext.right.layout)
                .copied()
                .collect();
            let join = LogicalJoin::new(
                current.plan,
                lookup.clone(),
                JoinType::LeftOuter,
                Condition {
                    conjunctions: tidy(conjunctions),
                },
            );
            current = Placed {
                plan: merge_lookup(&join)?,
                layout,
            };
            continue;
        }
        current = apply_ext(current, ext)?;
    }
    finish(current, &after, &lifted.outputs)
}

/// Applies a left outer join that was lifted.
fn apply_ext(current: Placed, ext: Ext) -> Option<Placed> {
    let left_len = current.layout.len();
    let position = |column: usize| {
        current
            .position(column)
            .or_else(|| Some(left_len + ext.right.position(column)?))
    };
    let conjunctions = ext
        .on
        .iter()
        .map(|conjunct| to_positions(conjunct, position))
        .collect::<Option<Vec<_>>>()?;
    if !has_equi_condition(&conjunctions, left_len) {
        return None;
    }
    let layout = current
        .layout
        .iter()
        .chain(&ext.right.layout)
        .copied()
        .collect();
    let plan = LogicalJoin::new(
        current.plan,
        ext.right.plan,
        JoinType::LeftOuter,
        Condition { conjunctions },
    )
    .into();
    Some(Placed { plan, layout })
}

/// Applies the remaining conjuncts, and computes the output columns.
fn finish(current: Placed, conjuncts: &[ExprImpl], outputs: &[ExprImpl]) -> Option<PlanRef> {
    let position = |column: usize| current.position(column);
    let conjunctions = conjuncts
        .iter()
        .map(|conjunct| to_positions(conjunct, position))
        .collect::<Option<Vec<_>>>()?;
    let exprs = outputs
        .iter()
        .map(|expr| to_positions(expr, position))
        .collect::<Option<Vec<_>>>()?;
    let input = if conjunctions.is_empty() {
        current.plan
    } else {
        LogicalFilter::new(current.plan, Condition { conjunctions }).into()
    };
    Some(LogicalProject::new(input, exprs).into())
}

/// The lookup is in the right side of a left outer, left semi or left anti join, and its row is
/// carried by the left side.
fn merge_in_right_side(join: &LogicalJoin, lookup: &PlanRef, pure: &Lookup) -> Option<PlanRef> {
    let join_type = join.join_type();
    let (left, right) = (join.left(), join.right());
    let left_len = left.schema().len();
    let lookup_len = lookup.schema().len();
    // The virtual columns: the lookup, then the left side, then the pieces of the right side.
    let left_columns: Vec<ExprImpl> = left
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| InputRef::new(lookup_len + index, field.data_type()).into())
        .collect();
    let condition = |right_outputs: &[ExprImpl]| -> Vec<ExprImpl> {
        let columns: Vec<ExprImpl> = left_columns.iter().chain(right_outputs).cloned().collect();
        join.on()
            .conjunctions
            .iter()
            .map(|conjunct| substitute(conjunct, &columns))
            .collect()
    };
    let through_share = join_type != JoinType::LeftOuter;
    let (lifted, mut proof, crossed_share) = lift_twice(
        &right,
        lookup,
        lookup_len + left_len,
        through_share,
        condition,
    )?;
    let mut conjuncts = condition(&lifted.outputs);
    conjuncts.extend(lifted.lifted.iter().cloned());
    // A conjunct on the columns of a lifted left outer join can not be applied in the condition of
    // the join: it removes rows of the right side, not pairs.
    let ext_columns: BTreeSet<usize> = lifted
        .exts
        .iter()
        .flat_map(|ext| ext.right.layout.iter().copied())
        .collect();
    if conjuncts
        .iter()
        .any(|conjunct| !columns_of(conjunct).is_disjoint(&ext_columns))
    {
        return None;
    }
    if join_type == JoinType::LeftOuter && lifted.rest.is_none() && lifted.exts.is_empty() {
        // The right side is the lookup itself: the case of `merge_lookup`.
        return None;
    }
    // The lookup may be the padded side of a left outer join of the right side: when it finds a
    // row, the condition of that join holds too.
    let lookup_ext = lifted.exts.iter().find(|ext| is_lookup_ext(ext, lookup));
    if let Some(ext) = lookup_ext {
        proof.extend(ext.on.iter().cloned());
    }
    let classes = equal_classes(&proof);

    // An instance of the table in the left side whose key designates the row of the lookup: each
    // column of the key of the lookup is in a class with a column of the left side that is bound
    // to the same column of the key.
    let is_left = |column: usize| (lookup_len..lookup_len + left_len).contains(&column);
    let mut memo = Memo::new();
    let key = pure.key.clone();
    let chosen_for = |instance: &Carried| -> Option<Vec<usize>> {
        key.iter()
            .map(|&key_column| {
                let lookup_columns: Vec<usize> = (0..lookup_len)
                    .filter(|&output| pure.key_column_at(output) == Some(key_column))
                    .collect();
                let bound = instance.bound.get(&key_column)?;
                let mut class_columns = classes
                    .iter()
                    .filter(|class| lookup_columns.iter().any(|c| class.contains(c)))
                    .flat_map(|class| class.iter().copied())
                    .filter(|&column| is_left(column))
                    .map(|column| column - lookup_len);
                // A column bound to the key, or the column that the left outer join of the
                // instance searched the same rows with (see `searched_as_bound`).
                let searched = instance
                    .outer_lookup
                    .as_ref()
                    .and_then(|lookup| lookup.keys.get(&key_column).copied());
                let found_with = searched_as_bound(instance, key_column, &pure.predicate);
                class_columns.find_map(|column| {
                    if bound.contains(&column) {
                        Some(column)
                    } else if searched == Some(column) {
                        found_with
                    } else {
                        None
                    }
                })
            })
            .collect()
    };
    let (instance, chosen) = carried(&left, &mut memo)
        .into_iter()
        .filter(|instance| instance.relation == pure.relation)
        .find_map(|instance| {
            let chosen = chosen_for(&instance)?;
            Some((instance, chosen))
        })?;

    // The columns of the lookup, computed from the row of the instance.
    let mut columns = referred_columns(&pure.exprs);
    columns.extend(referred_columns(&pure.predicate));
    let exposed = expose(
        &left,
        &Request {
            scan_id: instance.scan_id,
            columns: &columns,
            outer_lookup: None,
        },
        &mut memo,
    )?;
    let new_left = exposed.plan;
    let new_left_types: Vec<DataType> = new_left
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type())
        .collect();
    let new_left_len = new_left_types.len();
    let left_column =
        |index: usize| -> ExprImpl { InputRef::new(index, new_left_types[index].clone()).into() };
    let on_instance = |expr: &ExprImpl| {
        map_input_refs(expr, |input_ref| {
            left_column(exposed.positions[&input_ref.index()])
        })
    };
    // The lookup finds the row of the instance when the compared columns are not `NULL` and the
    // row satisfies its predicate.
    let facts = column_facts(&new_left);
    let mut found: Vec<ExprImpl> = chosen
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|&column| !facts.is_non_null(column))
        .map(|column| is_not_null(left_column(column)))
        .collect();
    found.extend(pure.predicate.iter().map(&on_instance));

    // The virtual columns over the new left side, then the rest of the right side.
    let rest_layout: Vec<usize> = lifted
        .rest
        .as_ref()
        .map(|rest| rest.layout.clone())
        .unwrap_or_default();
    let to_plan_with = |expr: &ExprImpl, lookup_values: &[ExprImpl]| -> Option<ExprImpl> {
        let mut ok = true;
        let expr = map_input_refs(expr, |input_ref| {
            let column = input_ref.index();
            if column < lookup_len {
                lookup_values[column].clone()
            } else if is_left(column) {
                left_column(column - lookup_len)
            } else if let Some(position) = rest_layout.iter().position(|&other| other == column) {
                InputRef::new(new_left_len + position, input_ref.return_type()).into()
            } else {
                ok = false;
                input_ref.into()
            }
        });
        ok.then_some(expr)
    };
    // The columns of the lookup. When it is the padded side of a left outer join of the right
    // side, they are those of the row of the instance when the condition of that join holds on
    // it and the lookup finds it, and `NULL` otherwise; and a row of the right side does not need
    // the lookup to find anything.
    let instance_values: Vec<ExprImpl> = pure.exprs.iter().map(&on_instance).collect();
    let lookup_values: Vec<ExprImpl> = match lookup_ext {
        Some(ext) => {
            let mut guard = ext
                .on
                .iter()
                .map(|conjunct| to_plan_with(conjunct, &instance_values))
                .collect::<Option<Vec<_>>>()?;
            guard.extend(found.iter().cloned());
            let guard = tidy(guard);
            instance_values
                .iter()
                .map(|value| when_all(&guard, value.clone()))
                .collect()
        }
        None => instance_values.clone(),
    };
    let to_plan = |expr: &ExprImpl| to_plan_with(expr, &lookup_values);
    let mut conjunctions = conjuncts.iter().map(&to_plan).collect::<Option<Vec<_>>>()?;
    if lookup_ext.is_none() {
        conjunctions.extend(found);
    }
    let conjunctions = tidy(conjunctions);

    if join_type != JoinType::LeftOuter {
        // Whether a row of the right side matches: the left outer joins that were lifted do not
        // change it.
        let joined = match lifted.rest {
            None => {
                let conjunctions = match join_type {
                    JoinType::LeftSemi => conjunctions,
                    _ if conjunctions.is_empty() => vec![ExprImpl::literal_bool(false)],
                    _ => vec![not_true(and_all(conjunctions))],
                };
                if conjunctions.is_empty() {
                    new_left
                } else {
                    LogicalFilter::new(new_left, Condition { conjunctions }).into()
                }
            }
            Some(rest) => {
                if !has_equi_condition(&conjunctions, new_left_len) {
                    return None;
                }
                let join =
                    LogicalJoin::new(new_left, rest.plan, join_type, Condition { conjunctions });
                // The rest of the right side may look up a row of the left side in turn. A rest
                // taken out of a shared sub-plan would be computed a second time: then it must
                // go too.
                let merged = merge_lookup(&join).or_else(|| witness_lookup(&join));
                match merged {
                    Some(merged) => merged,
                    None if crossed_share => return None,
                    None => join.into(),
                }
            }
        };
        let exprs = join
            .output_indices()
            .iter()
            .map(|&index| InputRef::new(index, left.schema().fields()[index].data_type()).into())
            .collect();
        return Some(LogicalProject::new(joined, exprs).into());
    }

    // A left outer join: the rest of the right side is joined with the same condition. A row of
    // the left side that matches nothing gets `NULL` in all the columns of the right side.
    let (base, gate) = match lifted.rest {
        None => (new_left, and_all(conjunctions)),
        Some(rest) => {
            if !has_equi_condition(&conjunctions, new_left_len) {
                return None;
            }
            // A column of the rest compared with `=` is not `NULL` in a matched row.
            let gate_column = conjunctions.iter().find_map(|conjunct| {
                let (first, second) = conjunct.as_eq_cond()?;
                (first.index() < new_left_len && second.index() >= new_left_len).then_some(second)
            })?;
            let join = LogicalJoin::new(
                new_left,
                rest.plan,
                JoinType::LeftOuter,
                Condition { conjunctions },
            );
            (join.into(), is_not_null(gate_column.into()))
        }
    };
    let base_len = base.schema().len();
    // The lifted left outer joins, over the plan: the lookup is computed from the instance.
    let mut current = base;
    let mut layout_len = base_len;
    let mut ext_positions: BTreeMap<usize, usize> = BTreeMap::new();
    for ext in lifted.exts {
        if is_lookup_ext(&ext, lookup) {
            // Its columns are computed from the row of the instance, see `lookup_values`.
            continue;
        }
        let ext_layout = ext.right.layout.clone();
        let mut ok = true;
        let mut conjunctions: Vec<ExprImpl> = ext
            .on
            .iter()
            .map(|conjunct| {
                map_input_refs(conjunct, |input_ref| {
                    let column = input_ref.index();
                    if let Some(&position) = ext_positions.get(&column) {
                        InputRef::new(position, input_ref.return_type()).into()
                    } else if let Some(position) = ext_layout.iter().position(|&c| c == column) {
                        InputRef::new(layout_len + position, input_ref.return_type()).into()
                    } else if let Some(expr) = to_plan(&input_ref.clone().into()) {
                        expr
                    } else {
                        ok = false;
                        input_ref.into()
                    }
                })
            })
            .collect();
        if !ok || !has_equi_condition(&conjunctions, layout_len) {
            return None;
        }
        conjunctions.push(gate.clone());
        let conjunctions = tidy(conjunctions);
        current = LogicalJoin::new(
            current,
            ext.right.plan,
            JoinType::LeftOuter,
            Condition { conjunctions },
        )
        .into();
        for (position, &column) in ext_layout.iter().enumerate() {
            ext_positions.insert(column, layout_len + position);
        }
        layout_len += ext_layout.len();
    }
    // The output columns. Those of the right side are computed from the row of the instance and
    // the rest of the right side, and must be `NULL` when the left row matches nothing: a column
    // of the rest or of a lifted left outer join already is.
    let mut exprs = vec![];
    for &index in join.output_indices() {
        if index < left_len {
            exprs.push(InputRef::new(index, left.schema().fields()[index].data_type()).into());
            continue;
        }
        let output = &lifted.outputs[index - left_len];
        let is_padded_column = output.as_input_ref().is_some_and(|input_ref| {
            let column = input_ref.index();
            ext_positions.contains_key(&column) || rest_layout.contains(&column)
        });
        let mut ok = true;
        let expr = map_input_refs(output, |input_ref| {
            if let Some(&position) = ext_positions.get(&input_ref.index()) {
                InputRef::new(position, input_ref.return_type()).into()
            } else if let Some(expr) = to_plan(&input_ref.clone().into()) {
                expr
            } else {
                ok = false;
                input_ref.into()
            }
        });
        if !ok {
            return None;
        }
        exprs.push(if is_padded_column {
            expr
        } else {
            when_all(std::slice::from_ref(&gate), expr)
        });
    }
    Some(LogicalProject::new(current, exprs).into())
}
