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

//! `UNION`, `INTERSECT` and `EXCEPT` (distinct) of branches that filter the same input and output
//! the same columns become one filter of that input (Calcite `SetOpToFilterRule`).
//!
//! ```text
//! select a, b from t where p0  UNION      select a, b from t  =>  select distinct a, b from t
//! select a, b from t where p1                                     where p0 OR p1
//! ...                          INTERSECT  ...                     where p0 AND p1
//! ...                          EXCEPT     ...                     where p0 AND (p1) IS NOT TRUE
//! ```
//!
//! A branch has the form `(Project | Filter)* base`, where the base is a scan of a table or a
//! shared sub-plan (see [`branch_of`]): a row `x` of the base gives the output row `π(x)` when the
//! predicate `p(x)` of the branch is true. The branches merged read the same base and have the
//! same output expressions `π`.
//!
//! - `UNION`: `{π(x) | p0(x)} ∪ {π(x) | p1(x)} = {π(x) | p0(x) OR p1(x)}`, with no condition.
//! - `INTERSECT` and `EXCEPT`: two different rows `x`, `y` of the base may give the same output
//!   row, and only one of them pass a filter. The rewrite is exact when what each branch but the
//!   first requires besides the conjuncts common to all (its own conjuncts) has the same value on
//!   `x` and `y` whenever `π(x) = π(y)`: either its own conjuncts only read columns that `π`
//!   outputs as they are, of a type in which equal values are identical ([`has_exact_equality`]),
//!   or `π` outputs every column of the key of the table (two rows with the same output are then
//!   the same row). Proof for `INTERSECT`: an output row `v` of the set operation has a row `x0`
//!   of the first branch with `π(x0) = v`; the own conjuncts of every other branch `i`, true on
//!   some `xi` with `π(xi) = v`, are true on `x0`, which passes the merged filter. For `EXCEPT`,
//!   the merged filter keeps `x` when the first branch keeps it and the own conjuncts of every
//!   other branch are not true on it; they are then not true on any `y` with `π(y) = π(x)`.
//!
//! `NULL`s compare equal in set operations; a column of the base that `π` outputs as it is
//! therefore has the same value on `x` and `y`, `NULL` included. The predicates and output
//! expressions must be pure and must not read the outer row of a correlated subquery.
//!
//! The rule runs before `INTERSECT` and `EXCEPT` become semi and anti joins: it removes the join,
//! and for `UNION` the copies that `UnionScanMergeRule` would make of each row. Branches that do
//! not read the same base stay as they are: a `UNION` or `INTERSECT` keeps the other branches, an
//! `EXCEPT` merges into its first input the later inputs over the same base, and merges with `OR`
//! later inputs that read the same base (their union is subtracted). On demand, behind
//! `streaming_merge_set_operation_filters` and `batch_merge_set_operation_filters`.

use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;

use super::prelude::{PlanRef, *};
use super::repeated_lookup_merge_rule::{and_all, map_input_refs, referred_columns};
use super::set_operation_to_count_rule::has_exact_equality;
use super::union_scan_merge_rule::{
    Base, Branch, branch_of, or, position_in, read_base, split_predicates,
};
use crate::expr::{
    CorrelatedInputRef, Expr, ExprImpl, ExprType, ExprVisitor, FunctionCall, InputRef, Subquery,
};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::{LogicalExcept, LogicalIntersect, LogicalProject, LogicalUnion};

#[derive(Clone, Copy, PartialEq)]
enum SetOp {
    Union,
    Intersect,
    Except,
}

/// See the documentation of the module. The variant for batch plans (`create_for_batch`) also
/// merges the scans of a table with a watermark.
pub struct SetOperationFilterMergeRule {
    for_batch: bool,
}

/// Whether the expression reads the outer row of a correlated subquery, or holds a subquery.
fn has_correlated_input_ref(expr: &ExprImpl) -> bool {
    struct Has(bool);
    impl ExprVisitor for Has {
        fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) {
            self.0 = true;
        }

        fn visit_subquery(&mut self, _: &Subquery) {
            self.0 = true;
        }
    }
    let mut has = Has(false);
    has.visit_expr(expr);
    has.0
}

/// Whether the conjuncts have the same value on two rows of the base that give the same output
/// row of the branch. See the documentation of the module.
fn is_function_of_output(conjuncts: &[ExprImpl], branch: &Branch) -> bool {
    let output_columns = || {
        branch
            .exprs
            .iter()
            .filter_map(|expr| expr.as_input_ref().map(|input_ref| input_ref.index()))
    };
    if let Base::Table(_) = branch.base
        && let Some(scan) = branch.plan.as_logical_scan()
    {
        let key = scan.table().stream_key();
        if !key.is_empty()
            && key
                .iter()
                .all(|column| output_columns().any(|output| output == *column))
        {
            return true;
        }
    }
    let exact: Vec<usize> = branch
        .exprs
        .iter()
        .filter_map(|expr| expr.as_input_ref())
        .filter(|input_ref| has_exact_equality(&input_ref.return_type()))
        .map(|input_ref| input_ref.index())
        .collect();
    referred_columns(conjuncts)
        .iter()
        .all(|column| exact.contains(column))
}

fn and_or_true(conjuncts: Vec<ExprImpl>) -> ExprImpl {
    if conjuncts.is_empty() {
        ExprImpl::literal_bool(true)
    } else {
        and_all(conjuncts)
    }
}

fn is_not_true(expr: ExprImpl) -> ExprImpl {
    FunctionCall::new_unchecked(ExprType::IsNotTrue, vec![expr], DataType::Boolean).into()
}

/// One read of the base of the members, filtered by `predicate` (over the columns of the base),
/// projected by the output expressions that the members share.
fn build(members: &[&Branch], predicate: Vec<ExprImpl>) -> PlanRef {
    let (input, read) = read_base(members, predicate);
    let types: Vec<_> = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type())
        .collect();
    let exprs = members[0]
        .exprs
        .iter()
        .map(|expr| {
            map_input_refs(expr, |input_ref| {
                let column = position_in(&read, input_ref.index());
                InputRef::new(column, types[column].clone()).into()
            })
        })
        .collect();
    LogicalProject::create(input, exprs)
}

/// The union of the members: one read with the `OR` of their predicates.
fn build_union(members: &[&Branch]) -> PlanRef {
    let (mut predicate, own) = split_predicates(members);
    if own.iter().all(|conjuncts| !conjuncts.is_empty()) {
        predicate.push(
            own.into_iter()
                .map(and_all)
                .reduce(or)
                .expect("at least two members"),
        );
    }
    build(members, predicate)
}

impl SetOperationFilterMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(SetOperationFilterMergeRule { for_batch: false })
    }

    /// The variant for batch plans, see the rule.
    pub fn create_for_batch() -> BoxedRule {
        Box::new(SetOperationFilterMergeRule { for_batch: true })
    }

    /// The groups of at least two branches (indices into `branches`, among `candidates`) that read
    /// the same base with the same output expressions, in the order of their first branch.
    fn groups(
        branches: &[Option<Branch>],
        candidates: impl Iterator<Item = usize>,
    ) -> Vec<Vec<usize>> {
        let mut groups: Vec<Vec<usize>> = vec![];
        for index in candidates {
            let Some(branch) = &branches[index] else {
                continue;
            };
            match groups.iter_mut().find(|group| {
                let first = branches[group[0]].as_ref().expect("a branch");
                first.base == branch.base && first.exprs == branch.exprs
            }) {
                Some(group) => group.push(index),
                None => groups.push(vec![index]),
            }
        }
        groups.retain(|group| group.len() >= 2);
        groups
    }
}

impl Rule<Logical> for SetOperationFilterMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let op = if let Some(union) = plan.as_logical_union() {
            if union.all() || union.source_col().is_some() {
                return None;
            }
            SetOp::Union
        } else if let Some(intersect) = plan.as_logical_intersect() {
            if intersect.all() {
                return None;
            }
            SetOp::Intersect
        } else if let Some(except) = plan.as_logical_except() {
            if except.all() {
                return None;
            }
            SetOp::Except
        } else {
            return None;
        };
        let fields = plan.schema().fields().to_vec();
        let inputs = plan.inputs();
        if inputs.len() < 2 {
            return None;
        }
        let branches: Vec<Option<Branch>> = inputs
            .iter()
            .map(|input| {
                branch_of(input, self.for_batch).filter(|branch| {
                    branch.exprs.len() == fields.len()
                        && branch
                            .exprs
                            .iter()
                            .zip_eq_fast(fields.iter())
                            .all(|(expr, field)| expr.return_type() == field.data_type())
                        && !branch
                            .exprs
                            .iter()
                            .chain(&branch.predicate)
                            .any(has_correlated_input_ref)
                })
            })
            .collect();

        let mut new_inputs: Vec<Option<PlanRef>> = inputs.iter().cloned().map(Some).collect();
        let mut changed = false;
        match op {
            SetOp::Union | SetOp::Intersect => {
                for group in Self::groups(&branches, 0..inputs.len()) {
                    let members: Vec<&Branch> = group
                        .iter()
                        .map(|&index| branches[index].as_ref().expect("a branch"))
                        .collect();
                    let merged = if op == SetOp::Union {
                        build_union(&members)
                    } else {
                        let (common, own) = split_predicates(&members);
                        if !members
                            .iter()
                            .zip_eq_fast(&own)
                            .skip(1)
                            .all(|(member, conjuncts)| is_function_of_output(conjuncts, member))
                        {
                            continue;
                        }
                        let mut predicate = common;
                        for conjunct in own.into_iter().flatten() {
                            if !predicate.contains(&conjunct) {
                                predicate.push(conjunct);
                            }
                        }
                        build(&members, predicate)
                    };
                    new_inputs[group[0]] = Some(merged);
                    for &index in &group[1..] {
                        new_inputs[index] = None;
                    }
                    changed = true;
                }
            }
            SetOp::Except => {
                // The later inputs over the base of the first one are merged into it.
                let mut merged_into_first = vec![];
                if let Some(first) = &branches[0] {
                    let mut group = vec![0];
                    group.extend((1..inputs.len()).filter(|&index| {
                        branches[index].as_ref().is_some_and(|branch| {
                            branch.base == first.base && branch.exprs == first.exprs
                        })
                    }));
                    if group.len() >= 2 {
                        let members: Vec<&Branch> = group
                            .iter()
                            .map(|&index| branches[index].as_ref().expect("a branch"))
                            .collect();
                        let (common, own) = split_predicates(&members);
                        if members
                            .iter()
                            .zip_eq_fast(&own)
                            .skip(1)
                            .all(|(member, conjuncts)| is_function_of_output(conjuncts, member))
                        {
                            let mut predicate = common;
                            let mut own = own.into_iter();
                            predicate.extend(own.next().expect("the first member"));
                            for conjuncts in own {
                                predicate.push(is_not_true(and_or_true(conjuncts)));
                            }
                            new_inputs[0] = Some(build(&members, predicate));
                            for &index in &group[1..] {
                                new_inputs[index] = None;
                            }
                            merged_into_first = group;
                            changed = true;
                        }
                    }
                }
                // The later inputs over the same base are subtracted together.
                let rest = (1..inputs.len()).filter(|index| !merged_into_first.contains(index));
                for group in Self::groups(&branches, rest) {
                    let members: Vec<&Branch> = group
                        .iter()
                        .map(|&index| branches[index].as_ref().expect("a branch"))
                        .collect();
                    new_inputs[group[0]] = Some(build_union(&members));
                    for &index in &group[1..] {
                        new_inputs[index] = None;
                    }
                    changed = true;
                }
            }
        }
        if !changed {
            return None;
        }

        let new_inputs: Vec<PlanRef> = new_inputs.into_iter().flatten().collect();
        if new_inputs.len() == 1 {
            let input = new_inputs.into_iter().next().expect("one input");
            let width = input.schema().len();
            return Some(Agg::new(vec![], (0..width).collect(), input).into());
        }
        Some(match op {
            SetOp::Union => LogicalUnion::create(false, new_inputs),
            SetOp::Intersect => LogicalIntersect::create(false, new_inputs),
            SetOp::Except => LogicalExcept::create(false, new_inputs),
        })
    }
}
