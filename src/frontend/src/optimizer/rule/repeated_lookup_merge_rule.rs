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

//! Removes a join that looks up a row of a table that the other side of the join already
//! carries.
//!
//! A stack of views often joins the same table several times on its key, each view for the
//! columns it needs:
//!
//! ```text
//! Join(on: f.dim_id = d.id, output: [f.id, d.name, d.grp])
//! ├─Join(on: f.dim_id = d.id, output: [f.id, f.dim_id, d.name])
//! │ ├─Scan(f, [id, dim_id])
//! │ └─Scan(d, [id, name])
//! └─Scan(d, [id, grp])
//! ```
//!
//! The key of `d` identifies one row, so both joins find the same row. The second join is
//! replaced with columns of the first one:
//!
//! ```text
//! Project(f.id, d.name, d.grp)
//! └─Join(on: f.dim_id = d.id, output: [f.id, f.dim_id, d.name, d.grp])
//!   ├─Scan(f, [id, dim_id])
//!   └─Scan(d, [id, name, grp])
//! ```
//!
//! A join, its two states and a scan of the table disappear. Only used by the streaming
//! optimizer, on demand.
//!
//! # Vocabulary
//!
//! - The *lookup* is the side of the join that is removed: a scan of a table `T`, possibly under
//!   `Project`, `Filter` and `Share` nodes (see [`PureScan`]), joined on the whole key of `T`.
//! - An *instance* is another scan of `T` in the other side of the join, which is kept (see
//!   [`Carried`]). In a row of the kept side, the instance is *matched* when the row was built
//!   from a row of `T`, and not from the `NULL`s of an outer join.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use risingwave_common::catalog::TableId;
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use super::column_facts_simplify_rule::column_facts;
use super::never_true_predicate_rule::is_simple_type;
use super::prelude::{PlanRef, *};
use super::unique_key_simplify_rule::is_unique_on_compared_columns;
use crate::catalog::table_catalog::TableType;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalProject, LogicalScan, PlanNodeId, PlanTreeNodeBinary,
    PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// Rewrites every column reference of an expression.
struct MapInputRef<F: FnMut(InputRef) -> ExprImpl>(F);

impl<F: FnMut(InputRef) -> ExprImpl> ExprRewriter for MapInputRef<F> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        (self.0)(input_ref)
    }
}

fn map_input_refs(expr: &ExprImpl, f: impl FnMut(InputRef) -> ExprImpl) -> ExprImpl {
    MapInputRef(f).rewrite_expr(expr.clone())
}

fn is_not_null(expr: ExprImpl) -> ExprImpl {
    FunctionCall::new_unchecked(ExprType::IsNotNull, vec![expr], DataType::Boolean).into()
}

/// The conjunction of the expressions, which must not be empty.
fn and_all(exprs: Vec<ExprImpl>) -> ExprImpl {
    exprs
        .into_iter()
        .reduce(|left, right| {
            FunctionCall::new_unchecked(ExprType::And, vec![left, right], DataType::Boolean).into()
        })
        .expect("at least one expression")
}

/// `CASE WHEN <all the conditions> THEN <value> END`, or the value if there is no condition.
fn when_all(conditions: &[ExprImpl], value: ExprImpl) -> ExprImpl {
    if conditions.is_empty() {
        return value;
    }
    let return_type = value.return_type();
    FunctionCall::new_unchecked(
        ExprType::Case,
        vec![and_all(conditions.to_vec()), value],
        return_type,
    )
    .into()
}

/// The columns that an expression over the columns of a table refers to.
fn referred_columns<'a>(exprs: impl IntoIterator<Item = &'a ExprImpl>) -> BTreeSet<usize> {
    let mut columns = BTreeSet::new();
    for expr in exprs {
        map_input_refs(expr, |input_ref| {
            columns.insert(input_ref.index());
            input_ref.into()
        });
    }
    columns
}

fn table_column(scan: &LogicalScan, column: usize) -> InputRef {
    InputRef::new(column, scan.table().columns()[column].data_type().clone())
}

/// A plan made of `Project`, `Filter` and `Share` nodes over the scan of one table: every row
/// comes from one row of the table, and is computed from that row only.
struct PureScan {
    scan: LogicalScan,
    /// The conjuncts that a row of the table satisfies to be read, over the columns of the table:
    /// the predicate of the scan and those of the filters.
    predicate: Vec<ExprImpl>,
    /// The output columns of the plan, over the columns of the table.
    exprs: Vec<ExprImpl>,
}

impl PureScan {
    /// The columns of the key of the table, among the columns of the table.
    fn key(&self) -> Vec<usize> {
        self.scan.table().stream_key()
    }

    /// The column of the key of the table that an output column of the plan exposes as it is.
    fn key_column_at(&self, output: usize) -> Option<usize> {
        let column = self.exprs[output].as_input_ref()?.index();
        self.key().contains(&column).then_some(column)
    }
}

/// Returns the [`PureScan`] form of a plan, if it has one.
///
/// A scan in the past (`FOR SYSTEM_TIME AS OF`) is not accepted: two of them do not read the
/// same rows. The expressions must be pure, as they are evaluated elsewhere in the plan.
fn pure_scan(plan: &PlanRef) -> Option<PureScan> {
    if let Some(scan) = plan.as_logical_scan() {
        let table = scan.table();
        if scan.as_of().is_some()
            || matches!(table.table_type(), TableType::Internal)
            || table.stream_key().is_empty()
        {
            return None;
        }
        return Some(PureScan {
            scan: scan.clone(),
            predicate: scan.predicate().conjunctions.clone(),
            exprs: scan
                .output_col_idx()
                .iter()
                .map(|&column| table_column(scan, column).into())
                .collect(),
        });
    }
    if let Some(share) = plan.as_logical_share() {
        return pure_scan(&share.input());
    }
    let is_movable = |expr: &ExprImpl| expr.is_pure() && expr.count_nows() == 0;
    if let Some(project) = plan.as_logical_project() {
        let mut pure = pure_scan(&project.input())?;
        let exprs: Vec<ExprImpl> = project
            .exprs()
            .iter()
            .map(|expr| map_input_refs(expr, |input_ref| pure.exprs[input_ref.index()].clone()))
            .collect();
        if !exprs.iter().all(is_movable) {
            return None;
        }
        pure.exprs = exprs;
        return Some(pure);
    }
    if let Some(filter) = plan.as_logical_filter() {
        let mut pure = pure_scan(&filter.input())?;
        for conjunct in &filter.predicate().conjunctions {
            let conjunct =
                map_input_refs(conjunct, |input_ref| pure.exprs[input_ref.index()].clone());
            if !is_movable(&conjunct) {
                return None;
            }
            pure.predicate.push(conjunct);
        }
        return Some(pure);
    }
    None
}

/// What a left outer join that looks up a row of a table by its whole key tells about the rows
/// where the lookup found nothing.
///
/// In a row of the join that is not itself padded by an outer join above:
/// - if the instance is matched, each column of `keys` holds the value of the key column of the
///   matched row;
/// - otherwise, no row of the table that the right side of the join reads has the values of
///   `keys` as its key.
#[derive(Clone)]
struct OuterLookup {
    /// The join that looks the row up.
    join_id: PlanNodeId,
    /// For each column of the key of the table, the column of the plan that it is compared with,
    /// if the plan still outputs it.
    keys: BTreeMap<usize, usize>,
}

/// An instance of a table that the rows of a plan node carry: each row is built from at most
/// one row of the table, read by the scan `scan_id`.
#[derive(Clone)]
struct Carried {
    scan_id: PlanNodeId,
    table_id: TableId,
    /// For a column of the table, the columns of the plan node that are bound to it: when such
    /// a column is not `NULL`, the instance is matched and the column holds the value of the
    /// column of the table in the matched row.
    ///
    /// These are the columns of the scan as they flow up the plan, and the columns that a filter
    /// or an inner join compares them with.
    bound: BTreeMap<usize, BTreeSet<usize>>,
    outer_lookup: Option<OuterLookup>,
}

impl Carried {
    /// Moves the columns of the plan node: `positions` gives the new positions of a column, if
    /// it is kept.
    fn map(mut self, positions: &HashMap<usize, Vec<usize>>) -> Self {
        for columns in self.bound.values_mut() {
            *columns = columns
                .iter()
                .flat_map(|column| positions.get(column).into_iter().flatten().copied())
                .collect();
        }
        if let Some(lookup) = &mut self.outer_lookup {
            // A column that is not kept is forgotten: the lookup then only serves along with a
            // bound column, see `merge_lookup`.
            lookup.keys = lookup
                .keys
                .iter()
                .filter_map(|(key, column)| Some((*key, *positions.get(column)?.first()?)))
                .collect();
        }
        self
    }

    /// Binds the columns that a filter or an inner join condition compares with a bound column:
    /// in every row that passes, both hold the same value, which is not `NULL`.
    fn bind_equal_columns(&mut self, equalities: &[(usize, usize)]) {
        loop {
            let mut changed = false;
            for columns in self.bound.values_mut() {
                for &(first, second) in equalities {
                    if columns.contains(&first) != columns.contains(&second) {
                        columns.extend([first, second]);
                        changed = true;
                    }
                }
            }
            if !changed {
                return;
            }
        }
    }
}

/// The pairs of columns that the conjuncts compare with `=`, for the types on which two equal
/// values are indistinguishable (see [`is_simple_type`]).
fn equal_columns(conjuncts: &[ExprImpl]) -> Vec<(usize, usize)> {
    conjuncts
        .iter()
        .filter_map(|conjunct| {
            let (first, second) = conjunct.as_eq_cond()?;
            (first.return_type() == second.return_type() && is_simple_type(&first.return_type()))
                .then_some((first.index(), second.index()))
        })
        .collect()
}

/// For a left outer join whose right side is a [`PureScan`] and whose condition is exactly one
/// equality per column of the key of the table, returns the right side and, for each column of
/// the key, the column of the left side that it is compared with.
fn outer_lookup_of(join: &LogicalJoin) -> Option<(PureScan, BTreeMap<usize, usize>)> {
    if join.join_type() != JoinType::LeftOuter {
        return None;
    }
    let pure = pure_scan(&join.right())?;
    let left_len = join.left().schema().len();
    let mut keys = BTreeMap::new();
    for conjunct in &join.on().conjunctions {
        let (first, second) = conjunct.as_eq_cond()?;
        if first.index() >= left_len
            || second.index() < left_len
            || first.return_type() != second.return_type()
            || !is_simple_type(&first.return_type())
        {
            return None;
        }
        let key = pure.key_column_at(second.index() - left_len)?;
        if keys.insert(key, first.index()).is_some() {
            return None;
        }
    }
    (keys.len() == pure.key().len()).then_some((pure, keys))
}

type Memo = HashMap<PlanNodeId, Vec<Carried>>;

/// Returns the instances of tables that the rows of a plan node carry. A node that is not listed
/// here carries none, which is always safe.
///
/// - `Scan`: itself, bound to the columns that it reads.
/// - `Share` over a [`PureScan`], `Project`, `Filter`: those of the input. A filter binds the
///   columns that it compares with a bound column.
/// - Inner join: those of both sides, and the condition binds columns as a filter does.
/// - Left and full outer joins: those of both sides. The columns of the padded side are `NULL`
///   in a padded row, as a bound column must be when the instance is not matched. A left outer
///   join that looks a row up by its key is recorded (see [`OuterLookup`]).
/// - Left semi and anti joins: those of the left side, whose rows they only select.
fn carried(plan: &PlanRef, memo: &mut Memo) -> Vec<Carried> {
    if let Some(instances) = memo.get(&plan.id()) {
        return instances.clone();
    }
    let instances = derive_carried(plan, memo);
    memo.insert(plan.id(), instances.clone());
    instances
}

fn derive_carried(plan: &PlanRef, memo: &mut Memo) -> Vec<Carried> {
    if let Some(scan) = plan.as_logical_scan() {
        if pure_scan(plan).is_none() {
            return vec![];
        }
        let mut bound: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        for (output, &column) in scan.output_col_idx().iter().enumerate() {
            bound.entry(column).or_default().insert(output);
        }
        return vec![Carried {
            scan_id: plan.id(),
            table_id: scan.table().id(),
            bound,
            outer_lookup: None,
        }];
    }
    if let Some(share) = plan.as_logical_share() {
        return if pure_scan(plan).is_some() {
            carried(&share.input(), memo)
        } else {
            vec![]
        };
    }
    if let Some(project) = plan.as_logical_project() {
        let mut positions: HashMap<usize, Vec<usize>> = HashMap::new();
        for (output, expr) in project.exprs().iter().enumerate() {
            if let Some(input_ref) = expr.as_input_ref() {
                positions.entry(input_ref.index()).or_default().push(output);
            }
        }
        return carried(&project.input(), memo)
            .into_iter()
            .map(|instance| instance.map(&positions))
            .collect();
    }
    if let Some(filter) = plan.as_logical_filter() {
        let equalities = equal_columns(&filter.predicate().conjunctions);
        let mut instances = carried(&filter.input(), memo);
        for instance in &mut instances {
            instance.bind_equal_columns(&equalities);
        }
        return instances;
    }
    let Some(join) = plan.as_logical_join() else {
        return vec![];
    };
    let left_len = join.left().schema().len();
    let mut instances = carried(&join.left(), memo);
    let join_type = join.join_type();
    match join_type {
        JoinType::Inner | JoinType::LeftOuter | JoinType::FullOuter => {
            let shift: HashMap<usize, Vec<usize>> = (0..join.right().schema().len())
                .map(|column| (column, vec![column + left_len]))
                .collect();
            for instance in carried(&join.right(), memo) {
                // A scan that both sides read through a `Share` is one node: keep one instance.
                if instances
                    .iter()
                    .all(|other| other.scan_id != instance.scan_id)
                {
                    instances.push(instance.map(&shift));
                }
            }
        }
        // The internal columns of a left semi or anti join are those of its left side.
        JoinType::LeftSemi | JoinType::LeftAnti => {}
        _ => return vec![],
    }
    if join_type == JoinType::Inner {
        let equalities = equal_columns(&join.on().conjunctions);
        for instance in &mut instances {
            instance.bind_equal_columns(&equalities);
        }
    }
    if let Some((pure, keys)) = outer_lookup_of(join)
        && let Some(instance) = instances
            .iter_mut()
            .find(|instance| instance.scan_id == pure.scan.id())
        // The instance is the one of the right side, not one that the left side reads too.
        && instance
            .bound
            .values()
            .flatten()
            .all(|&column| column >= left_len)
    {
        instance.outer_lookup = Some(OuterLookup {
            join_id: plan.id(),
            keys,
        });
    }
    let mut positions: HashMap<usize, Vec<usize>> = HashMap::new();
    for (output, &internal) in join.output_indices().iter().enumerate() {
        positions.entry(internal).or_default().push(output);
    }
    instances
        .into_iter()
        .map(|instance| instance.map(&positions))
        .collect()
}

/// What [`expose`] is asked for.
struct Request<'a> {
    /// The instance, by its scan.
    scan_id: PlanNodeId,
    /// The columns of the table to append to the output of the plan.
    columns: &'a BTreeSet<usize>,
    /// For an instance that is used through its [`OuterLookup`]: the join, and the predicate of
    /// the lookup that is merged into it, over the columns of the table.
    outer_lookup: Option<(PlanNodeId, &'a [ExprImpl])>,
}

/// The result of [`expose`].
struct Exposed {
    /// The plan, with the same output columns as before, followed by the requested ones that it
    /// did not output yet.
    plan: PlanRef,
    /// The position of each requested column of the table in the output of the plan.
    positions: BTreeMap<usize, usize>,
    /// For an instance that is used through its [`OuterLookup`]: the conjuncts that the rows of
    /// the table now satisfy to be found, over the columns of the table.
    predicate: Option<Vec<ExprImpl>>,
}

/// Rewrites a plan so that the columns of the table that are requested for one of the instances
/// that it carries (see [`carried`]) are appended to its output. The appended columns are those
/// of the matched row, and `NULL` when the instance is not matched.
fn expose(plan: &PlanRef, request: &Request<'_>, memo: &mut Memo) -> Option<Exposed> {
    if let Some(scan) = plan.as_logical_scan() {
        if plan.id() != request.scan_id {
            return None;
        }
        let mut output = scan.output_col_idx().clone();
        let mut positions = BTreeMap::new();
        for &column in request.columns {
            let position = output.iter().position(|&other| other == column);
            positions.insert(
                column,
                position.unwrap_or_else(|| {
                    output.push(column);
                    output.len() - 1
                }),
            );
        }
        return Some(Exposed {
            plan: scan.clone_with_output_indices(output).into(),
            positions,
            predicate: None,
        });
    }
    if let Some(share) = plan.as_logical_share() {
        // The rewritten scan is not shared any more: the other consumers keep the former one.
        return expose(&share.input(), request, memo);
    }
    if let Some(project) = plan.as_logical_project() {
        let input = expose(&project.input(), request, memo)?;
        let mut exprs = project.exprs().clone();
        let mut positions = BTreeMap::new();
        for (&column, &position) in &input.positions {
            positions.insert(column, exprs.len());
            let data_type = input.plan.schema().fields()[position].data_type();
            exprs.push(InputRef::new(position, data_type).into());
        }
        return Some(Exposed {
            plan: LogicalProject::new(input.plan, exprs).into(),
            positions,
            predicate: input.predicate,
        });
    }
    if let Some(filter) = plan.as_logical_filter() {
        // The columns are appended: the predicate refers to the same columns as before.
        let input = expose(&filter.input(), request, memo)?;
        return Some(Exposed {
            plan: LogicalFilter::new(input.plan, filter.predicate().clone()).into(),
            ..input
        });
    }
    let join = plan.as_logical_join()?;
    if let Some((join_id, lookup_predicate)) = request.outer_lookup
        && join_id == plan.id()
    {
        return widen_outer_lookup(join, request.columns, lookup_predicate);
    }
    let (left, right) = (join.left(), join.right());
    let left_len = left.schema().len();
    let mut output = join.output_indices().clone();
    let mut positions = BTreeMap::new();
    let in_left = carried(&left, memo)
        .iter()
        .any(|instance| instance.scan_id == request.scan_id);
    let (left, right, on, predicate) = if in_left {
        let input = expose(&left, request, memo)?;
        // The columns are appended to the left side: those of the right side move.
        let added = input.plan.schema().len() - left_len;
        let moved = |index: usize| {
            if index >= left_len {
                index + added
            } else {
                index
            }
        };
        let on = join
            .on()
            .clone()
            .rewrite_expr(&mut MapInputRef(|input_ref| {
                InputRef::new(moved(input_ref.index()), input_ref.return_type()).into()
            }));
        for index in &mut output {
            *index = moved(*index);
        }
        for (&column, &position) in &input.positions {
            positions.insert(column, output_position(&mut output, position));
        }
        (input.plan, right, on, input.predicate)
    } else {
        let input = expose(&right, request, memo)?;
        for (&column, &position) in &input.positions {
            positions.insert(column, output_position(&mut output, left_len + position));
        }
        (left, input.plan, join.on().clone(), input.predicate)
    };
    Some(Exposed {
        plan: LogicalJoin::with_output_indices(left, right, join.join_type(), on, output).into(),
        positions,
        predicate,
    })
}

/// Returns the position of an internal column of a join in its output, appending the column if
/// the join does not output it yet: a join does not output the same column twice.
fn output_position(output: &mut Vec<usize>, internal: usize) -> usize {
    output
        .iter()
        .position(|&other| other == internal)
        .unwrap_or_else(|| {
            output.push(internal);
            output.len() - 1
        })
}

/// Rewrites a left outer join that looks a row of a table up by its key (see [`OuterLookup`]),
/// so that it finds the rows of another lookup of the same table too, and outputs more columns
/// of the table.
///
/// ```text
/// Join(LeftOuter, on: f.dim_id = d.id)        Project(f.*, CASE WHEN d.id IS NOT NULL AND d.active THEN d.name END, ..)
/// ├─Scan(f)                               =>  └─Join(LeftOuter, on: f.dim_id = d.id)
/// └─Scan(d, [id, name], d.active)               ├─Scan(f)
///                                               └─Scan(d, [id, name, grp, active])
/// ```
///
/// The table has at most one row with a given key, so the join finds the row that it found
/// before whenever that row satisfies the former predicate. The scan reads the rows that
/// satisfy one of the two predicates, which is never more than what the two scans read.
fn widen_outer_lookup(
    join: &LogicalJoin,
    columns: &BTreeSet<usize>,
    lookup_predicate: &[ExprImpl],
) -> Option<Exposed> {
    let (pure, _) = outer_lookup_of(join)?;
    let left = join.left();
    let left_len = left.schema().len();
    // Whether the rows that the other lookup needs are already read.
    let is_covered = pure
        .predicate
        .iter()
        .all(|conjunct| lookup_predicate.contains(conjunct));
    let predicate = if is_covered {
        pure.predicate.clone()
    } else if lookup_predicate.is_empty() {
        vec![]
    } else {
        vec![
            FunctionCall::new_unchecked(
                ExprType::Or,
                vec![
                    and_all(pure.predicate.clone()),
                    and_all(lookup_predicate.to_vec()),
                ],
                DataType::Boolean,
            )
            .into(),
        ]
    };
    let key = pure.key();
    let mut read: BTreeSet<usize> = columns.clone();
    read.extend(key.iter().copied());
    read.extend(referred_columns(&pure.exprs));
    read.extend(referred_columns(&pure.predicate));
    let read: Vec<usize> = read.into_iter().collect();
    let position = |column: usize| {
        left_len
            + read
                .iter()
                .position(|&other| other == column)
                .expect("the column is read")
    };
    let scan = pure
        .scan
        .clone_with_output_indices(read.clone())
        .clone_with_predicate(Condition {
            conjunctions: predicate.clone(),
        });
    let to_join = |expr: &ExprImpl| {
        map_input_refs(expr, |input_ref| {
            InputRef::new(position(input_ref.index()), input_ref.return_type()).into()
        })
    };
    // The condition compares the same columns of the left side with the columns of the key.
    let on = join
        .on()
        .clone()
        .rewrite_expr(&mut MapInputRef(|input_ref| {
            if input_ref.index() < left_len {
                return input_ref.into();
            }
            let column = pure
                .key_column_at(input_ref.index() - left_len)
                .expect("the condition compares the columns of the key");
            InputRef::new(position(column), input_ref.return_type()).into()
        }));
    let new_join: PlanRef =
        LogicalJoin::new(left.clone(), scan.into(), JoinType::LeftOuter, on).into();
    // A matched row has a key that is not `NULL`: the condition compares it with `=`.
    let mut found = vec![is_not_null(to_join(
        &table_column(&pure.scan, key[0]).into(),
    ))];
    if !is_covered {
        found.extend(pure.predicate.iter().map(to_join));
    }
    let mut exprs: Vec<ExprImpl> = join
        .output_indices()
        .iter()
        .map(|&index| {
            if index < left_len {
                return InputRef::new(index, left.schema().fields()[index].data_type()).into();
            }
            let expr = to_join(&pure.exprs[index - left_len]);
            if is_covered && expr.as_input_ref().is_some() {
                // A column of the table, already `NULL` when no row is found.
                expr
            } else {
                when_all(&found, expr)
            }
        })
        .collect();
    let mut positions = BTreeMap::new();
    for &column in columns {
        positions.insert(column, exprs.len());
        exprs.push(to_join(&table_column(&pure.scan, column).into()));
    }
    Some(Exposed {
        plan: LogicalProject::new(new_join, exprs).into(),
        positions,
        predicate: Some(predicate),
    })
}

/// Removes an inner, left outer, left semi or left anti join that looks up a row of a table that
/// the other side of the join already carries. See the documentation of the module.
///
/// The lookup is removed when its condition compares every column of the key of the table with a
/// column that designates the row of one instance of the table in the other side:
///
/// - a column that is bound to the same column of the key (see [`Carried::bound`]): the row of
///   the lookup is the matched row of the instance. For instance the key of the instance
///   itself, or the column that an inner join compares it with;
/// - or the column that the left outer join of the instance compares with the same column of the
///   key (see [`OuterLookup`]): that join and the lookup search for the same row. The join of
///   the instance is then made to find the rows of both (see [`widen_outer_lookup`]).
///
/// The columns of the lookup are computed from the row of the instance. The row is found by the
/// lookup when the compared columns are not `NULL`, the row satisfies the predicate of the
/// lookup and the rest of the join condition holds: the rows of an inner or semi join are
/// filtered on that, those of an anti join on the opposite, and the columns of a left outer join
/// are `NULL` otherwise.
pub struct RepeatedLookupMergeRule;

impl Rule<Logical> for RepeatedLookupMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        match join.join_type() {
            JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => merge_lookup(join),
            JoinType::Inner => merge_lookup(join).or_else(|| merge_lookup(&commute(join))),
            _ => None,
        }
    }
}

impl RepeatedLookupMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(RepeatedLookupMergeRule)
    }
}

/// Returns the same inner join with its two sides exchanged.
fn commute(join: &LogicalJoin) -> LogicalJoin {
    let left_len = join.left().schema().len();
    let right_len = join.right().schema().len();
    let moved = |index: usize| {
        if index < left_len {
            index + right_len
        } else {
            index - left_len
        }
    };
    let on = join
        .on()
        .clone()
        .rewrite_expr(&mut MapInputRef(|input_ref| {
            InputRef::new(moved(input_ref.index()), input_ref.return_type()).into()
        }));
    let output = join.output_indices().iter().map(|&index| moved(index));
    LogicalJoin::with_output_indices(
        join.right(),
        join.left(),
        join.join_type(),
        on,
        output.collect(),
    )
}

/// For each column of the key of the table that the right side of a join reads, the columns of
/// the left side that the condition compares it with, and the conjunct that does.
fn compared_key_columns(
    join: &LogicalJoin,
    pure: &PureScan,
) -> BTreeMap<usize, Vec<(usize, usize)>> {
    let left_len = join.left().schema().len();
    let mut compared: BTreeMap<usize, Vec<(usize, usize)>> = BTreeMap::new();
    for (position, conjunct) in join.on().conjunctions.iter().enumerate() {
        if let Some((first, second)) = conjunct.as_eq_cond()
            && first.index() < left_len
            && second.index() >= left_len
            && first.return_type() == second.return_type()
            && is_simple_type(&first.return_type())
            && let Some(column) = pure.key_column_at(second.index() - left_len)
        {
            compared
                .entry(column)
                .or_default()
                .push((first.index(), position));
        }
    }
    compared
}

/// Removes the join if its right side is a lookup that its left side makes redundant.
fn merge_lookup(join: &LogicalJoin) -> Option<PlanRef> {
    let left = join.left();
    let pure = pure_scan(&join.right())?;
    let key = pure.key();
    let compared = compared_key_columns(join, &pure);
    if !key.iter().all(|column| compared.contains_key(column)) {
        return None;
    }

    let mut memo = Memo::new();
    for instance in carried(&left, &mut memo) {
        if instance.table_id != pure.scan.table().id() {
            continue;
        }
        // For each column of the key: the column of the left side that designates the row of the
        // instance, the conjunct that compares it, and whether the column is bound.
        let chosen: Option<Vec<(usize, usize, bool)>> = key
            .iter()
            .map(|column| {
                let candidates = &compared[column];
                let bound = instance.bound.get(column);
                let searched = instance
                    .outer_lookup
                    .as_ref()
                    .and_then(|lookup| lookup.keys.get(column));
                candidates
                    .iter()
                    .find(|(left_column, _)| bound.is_some_and(|c| c.contains(left_column)))
                    .map(|&(left_column, conjunct)| (left_column, conjunct, true))
                    .or_else(|| {
                        candidates
                            .iter()
                            .find(|(left_column, _)| searched == Some(left_column))
                            .map(|&(left_column, conjunct)| (left_column, conjunct, false))
                    })
            })
            .collect();
        let Some(chosen) = chosen else {
            continue;
        };
        // With a bound column, a row whose instance is not matched holds `NULL` in it, and the
        // lookup finds nothing either. Without any, the row is the one that the outer join of
        // the instance searches for.
        let through_outer_lookup = chosen.iter().all(|&(_, _, is_bound)| !is_bound);
        if let Some(plan) = replace_lookup(
            join,
            &pure,
            &instance,
            &chosen,
            through_outer_lookup,
            &mut memo,
        ) {
            return Some(plan);
        }
    }
    None
}

fn replace_lookup(
    join: &LogicalJoin,
    pure: &PureScan,
    instance: &Carried,
    chosen: &[(usize, usize, bool)],
    through_outer_lookup: bool,
    memo: &mut Memo,
) -> Option<PlanRef> {
    let left = join.left();
    let left_len = left.schema().len();
    let key = pure.key();
    let conjuncts = &join.on().conjunctions;
    let rest: Vec<&ExprImpl> = conjuncts
        .iter()
        .enumerate()
        .filter(|(position, _)| chosen.iter().all(|(_, conjunct, _)| conjunct != position))
        .map(|(_, conjunct)| conjunct)
        .collect();

    // The columns of the lookup that the join uses, and the columns of the table that they are
    // computed from.
    let mut used: BTreeSet<usize> = join
        .output_indices()
        .iter()
        .filter(|&&index| index >= left_len)
        .map(|&index| index - left_len)
        .collect();
    for index in referred_columns(rest.iter().copied()) {
        if index >= left_len {
            used.insert(index - left_len);
        }
    }
    let mut columns = referred_columns(used.iter().map(|&column| &pure.exprs[column]));
    columns.extend(referred_columns(&pure.predicate));
    if through_outer_lookup {
        columns.insert(key[0]);
    }

    let exposed = expose(
        &left,
        &Request {
            scan_id: instance.scan_id,
            columns: &columns,
            outer_lookup: match (&instance.outer_lookup, through_outer_lookup) {
                (Some(lookup), true) => Some((lookup.join_id, &pure.predicate)),
                (None, true) => return None,
                (_, false) => None,
            },
        },
        memo,
    )?;
    let new_left = exposed.plan;
    let left_column = |index: usize| -> ExprImpl {
        InputRef::new(index, new_left.schema().fields()[index].data_type()).into()
    };
    // An expression over the columns of the table, on the row of the instance.
    let on_instance = |expr: &ExprImpl| {
        map_input_refs(expr, |input_ref| {
            left_column(exposed.positions[&input_ref.index()])
        })
    };
    let lookup_column = |column: usize| on_instance(&pure.exprs[column]);

    // The conditions under which the lookup finds the row of the instance.
    let mut found = vec![];
    if through_outer_lookup {
        // A matched row has a key that is not `NULL`: its join compares it with `=`.
        found.push(is_not_null(left_column(exposed.positions[&key[0]])));
    } else {
        let facts = column_facts(&new_left);
        for &(column, _, _) in chosen {
            if !facts.is_non_null(column) {
                found.push(is_not_null(left_column(column)));
            }
        }
    }
    let is_only_matched = found.len() == 1 && through_outer_lookup;
    let read_predicate = exposed.predicate.unwrap_or_default();
    found.extend(
        pure.predicate
            .iter()
            .filter(|conjunct| !read_predicate.contains(conjunct))
            .map(on_instance),
    );
    found.extend(rest.iter().map(|conjunct| {
        map_input_refs(conjunct, |input_ref| {
            if input_ref.index() < left_len {
                input_ref.into()
            } else {
                lookup_column(input_ref.index() - left_len)
            }
        })
    }));
    let is_only_matched = is_only_matched && found.len() == 1;

    let join_type = join.join_type();
    // The joins that only keep the rows for which the lookup finds a row.
    let is_inner = matches!(join_type, JoinType::Inner | JoinType::LeftSemi);
    let exprs: Vec<ExprImpl> = join
        .output_indices()
        .iter()
        .map(|&index| {
            if index < left_len {
                return left_column(index);
            }
            let expr = lookup_column(index - left_len);
            let is_table_column = pure.exprs[index - left_len].as_input_ref().is_some();
            if is_inner || (is_only_matched && is_table_column) {
                // The rows are filtered below, or the column of the table is already `NULL`
                // when no row is found.
                expr
            } else {
                when_all(&found, expr)
            }
        })
        .collect();
    let conjunctions = match join_type {
        // An anti join keeps the rows for which the lookup finds no row: "found" is not true.
        JoinType::LeftAnti if found.is_empty() => vec![ExprImpl::literal_bool(false)],
        JoinType::LeftAnti => {
            let found_or_false = FunctionCall::new_unchecked(
                ExprType::Coalesce,
                vec![and_all(found), ExprImpl::literal_bool(false)],
                DataType::Boolean,
            );
            vec![
                FunctionCall::new_unchecked(
                    ExprType::Not,
                    vec![found_or_false.into()],
                    DataType::Boolean,
                )
                .into(),
            ]
        }
        _ if is_inner => found,
        _ => vec![],
    };
    let input = if conjunctions.is_empty() {
        new_left
    } else {
        LogicalFilter::new(new_left, Condition { conjunctions }).into()
    };
    Some(LogicalProject::new(input, exprs).into())
}

/// Prints, for every join of a plan whose two sides read the same relation (a table, or a shared
/// sub-plan), the operators between the join and the relation on each side, and whether each
/// side is unique on the columns that the join compares. Only when the environment variable
/// `RW_MERGE_LOOKUP_DIAG` is set: a development aid to find the shapes of repeated reads that the
/// rule does not handle.
pub fn diagnose_repeated_reads(plan: &PlanRef) {
    if std::env::var("RW_MERGE_LOOKUP_DIAG").is_err() {
        return;
    }
    let mut visited = BTreeSet::new();
    let mut stack = vec![plan.clone()];
    while let Some(node) = stack.pop() {
        if !visited.insert(node.id().0) {
            continue;
        }
        stack.extend(node.inputs());
        let Some(join) = node.as_logical_join() else {
            continue;
        };
        let left_paths = relation_paths(&join.left());
        let right_paths = relation_paths(&join.right());
        let (left_unique, right_unique) = is_unique_on_compared_columns(join);
        for (relation, right_path) in &right_paths {
            let Some(left_path) = left_paths.get(relation) else {
                continue;
            };
            eprintln!(
                "MERGE_DIAG\t{relation}\t{:?}\tR:{}\tL:{}\tright_unique={right_unique}\tleft_unique={left_unique}",
                join.join_type(),
                right_path.join(">"),
                left_path.join(">"),
            );
        }
    }
}

/// For each relation that a plan reads (a table, or a shared sub-plan, which is not entered), the
/// operators from the plan down to the relation, the shortest way. Projections and filters are
/// not listed.
fn relation_paths(plan: &PlanRef) -> BTreeMap<String, Vec<String>> {
    if let Some(scan) = plan.as_logical_scan() {
        return BTreeMap::from([(format!("table {}", scan.table().name), vec![])]);
    }
    if let Some(share) = plan.as_logical_share() {
        let kind = format!("{:?}", share.input().node_type()).replace("Logical", "");
        return BTreeMap::from([(format!("share {} of {kind}", plan.id().0), vec![])]);
    }
    let here = if let Some(join) = plan.as_logical_join() {
        Some(format!("Join{:?}", join.join_type()))
    } else if plan.as_logical_project().is_some() || plan.as_logical_filter().is_some() {
        None
    } else {
        Some(format!("{:?}", plan.node_type()).replace("Logical", ""))
    };
    let mut paths: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (index, input) in plan.inputs().iter().enumerate() {
        for (relation, mut path) in relation_paths(input) {
            if let Some(here) = &here {
                let side = if plan.as_logical_join().is_some() {
                    ["(l)", "(r)"][index.min(1)]
                } else {
                    ""
                };
                path.insert(0, format!("{here}{side}"));
            }
            match paths.get(&relation) {
                Some(other) if other.len() <= path.len() => {}
                _ => {
                    paths.insert(relation, path);
                }
            }
        }
    }
    paths
}
