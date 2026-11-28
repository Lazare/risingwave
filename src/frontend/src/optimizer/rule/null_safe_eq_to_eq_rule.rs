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

use risingwave_common::types::DataType;

use super::column_facts_simplify_rule::{
    ColumnFacts, column_facts, expr_facts, predicate_facts, table_constraint_facts,
};
use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall};
use crate::optimizer::plan_node::{LogicalFilter, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::utils::Condition;

/// Turns `a IS NOT DISTINCT FROM b` into `a = b` in a predicate evaluated with filter semantics
/// (the condition of a join, of a filter or of a scan), when `a` or `b` is never `NULL`.
///
/// ```text
/// Join(LeftSemi, t.id IS NOT DISTINCT FROM u.k)      Join(LeftSemi, t.id = u.k)
/// ├─Scan(t)          -- t.id: primary key       =>   ├─Scan(t)
/// └─Scan(u)                                          └─Scan(u)
/// ```
///
/// Subquery decorrelation joins the outer side with the subquery on `IS NOT DISTINCT FROM` the
/// correlated columns. A join on `=` keeps no row whose key is `NULL` (it can never match) and is
/// a plain equi-join for the other rules (unique keys, repeated lookups, `NullKeyJoinRule`).
///
/// # Correctness
///
/// `a IS NOT DISTINCT FROM b` and `a = b` agree when both operands are not `NULL` (the same
/// comparison, `general_is_not_distinct_from` and `general_eq`). When exactly one is `NULL`, the
/// first is `FALSE` and the second `NULL`; both `NULL` can not happen when one operand is never
/// `NULL`. A predicate evaluated with filter semantics keeps a row (or a pair of rows, for a join
/// condition, whatever the type of the join: a pair matches iff the condition is `TRUE`) only if
/// every top-level conjunct is `TRUE`, so `FALSE` and `NULL` reject alike. Hence only top-level
/// conjuncts are rewritten, never an operand of `NOT`, `OR`, `CASE` or a projected expression.
///
/// An operand is never `NULL` when [`expr_facts`] says so with the facts of the input (see
/// [`column_facts`]: `NOT NULL` columns, keys, constants, a filter below that rejects `NULL`) and
/// of the other conjuncts of the same predicate: replacing a conjunct with an expression that is
/// equivalent whenever the others are `TRUE` keeps the conjunction equivalent. For a join
/// condition, the facts of each side hold for its columns whatever the type of the join, because
/// the condition is evaluated on rows of the inputs, before any padding. The facts are properties
/// of every row the inputs will ever produce, so the rewrite is valid for a streaming job, whose
/// plan is frozen when it is created.
///
/// Not rewritten: a conjunct with `now()` (a temporal filter keeps its form), the condition of an
/// as-of join, and a comparison whose `=` would need an implicit cast (the binder already gave
/// both operands the same type, so this does not happen in practice).
///
/// Used on demand, behind `streaming_null_safe_eq_to_eq` and `batch_null_safe_eq_to_eq`.
pub struct NullSafeEqToEqRule {}

impl Rule<Logical> for NullSafeEqToEqRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if let Some(filter) = plan.as_logical_filter() {
            let input = filter.input();
            let predicate = rewrite_conjuncts(filter.predicate(), &column_facts(&input))?;
            return Some(LogicalFilter::create(input, predicate));
        }
        if let Some(scan) = plan.as_logical_scan() {
            // The predicate refers to the columns of the table, as the facts of its constraints.
            let predicate = rewrite_conjuncts(scan.predicate(), &table_constraint_facts(scan))?;
            return Some(scan.clone_with_predicate(predicate).into());
        }
        let join = plan.as_logical_join()?;
        if join.is_asof_join() {
            return None;
        }
        let left_len = join.left().schema().len();
        let mut facts = column_facts(&join.left());
        facts.merge(column_facts(&join.right()).shift(left_len));
        let on = rewrite_conjuncts(join.on(), &facts)?;
        Some(join.clone_with_cond(on).into())
    }
}

impl NullSafeEqToEqRule {
    pub fn create() -> BoxedRule {
        Box::new(NullSafeEqToEqRule {})
    }
}

/// Rewrites the top-level conjuncts `a IS NOT DISTINCT FROM b` of a predicate whose `a` or `b` is
/// never `NULL`, given the facts about its input columns. Returns `None` if nothing changes.
///
/// The conjuncts are rewritten one after the other, each with the facts of the *current* form of
/// the others (a conjunct already turned into `a = b` says that `a` and `b` are not `NULL`).
fn rewrite_conjuncts(predicate: &Condition, base: &ColumnFacts) -> Option<Condition> {
    let mut conjuncts = predicate.conjunctions.clone();
    let mut changed = false;
    for position in 0..conjuncts.len() {
        let Some(call) = conjuncts[position].as_function_call() else {
            continue;
        };
        let [left, right] = call.inputs() else {
            continue;
        };
        if call.func_type() != ExprType::IsNotDistinctFrom || conjuncts[position].count_nows() > 0 {
            continue;
        }
        let mut facts = base.clone();
        facts.merge(predicate_facts(
            conjuncts
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != position)
                .map(|(_, expr)| expr),
        ));
        if !expr_facts(left, &facts).1 && !expr_facts(right, &facts).1 {
            continue;
        }
        let Some(equal) = equal_without_cast(left, right) else {
            continue;
        };
        conjuncts[position] = equal;
        changed = true;
    }
    changed.then_some(Condition {
        conjunctions: conjuncts,
    })
}

/// `left = right`, if the comparison exists for the types of the operands as they are.
fn equal_without_cast(left: &ExprImpl, right: &ExprImpl) -> Option<ExprImpl> {
    let inputs = vec![left.clone(), right.clone()];
    let equal = FunctionCall::new(ExprType::Equal, inputs.clone()).ok()?;
    (equal.inputs() == inputs.as_slice() && equal.return_type() == DataType::Boolean)
        .then(|| equal.into())
}
