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
use risingwave_pb::plan_common::JoinType;

use super::never_true_predicate_rule::is_never_true;
use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalProject, LogicalValues, PlanTreeNodeBinary,
};

/// Replaces an inner join with a relation made of a single row of constants (a `LogicalValues`
/// with one row) with a filter and a projection of the other side: the columns of the constant
/// side are replaced with their value in the join condition, which becomes the predicate of the
/// filter, and in the output.
///
/// ```text
/// Join(Inner, on: t.a = v.x AND t.b > v.y)          Project(t.a, t.b, 1, 2)
/// ├─Scan(t)                                    =>   └─Filter(t.a = 1 AND t.b > 2)
/// └─Values([1, 2] as v(x, y))                         └─Scan(t)
/// ```
///
/// Every row of the other side is joined with that row or with nothing, so the join does not
/// need any state. A constant only holds literals and pure functions; the join is left untouched
/// if its condition refers to `now()`.
///
/// The rule is only used by the streaming optimizer, on demand.
pub struct ConstantRowJoinRule;

impl Rule<Logical> for ConstantRowJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_join()?;
        if join.join_type() != JoinType::Inner
            || join
                .on()
                .conjunctions
                .iter()
                .any(|expr| expr.count_nows() > 0)
        {
            return None;
        }
        let (left, right) = (join.left(), join.right());
        let left_len = left.schema().len();
        let (row, constant_is_right) = if let Some(row) = constant_row(&right) {
            (row, true)
        } else {
            (constant_row(&left)?, false)
        };
        // The types of the internal columns of the join: those of the left side, then those of
        // the right side.
        let data_types: Vec<DataType> = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields())
            .map(|field| field.data_type.clone())
            .collect();
        let mut rewriter = ConstantSideRewriter {
            row: &row,
            left_len,
            constant_is_right,
        };
        let predicate = join.on().clone().rewrite_expr(&mut rewriter);
        if is_never_true(&predicate) {
            return Some(LogicalValues::create(
                vec![],
                plan.schema().clone(),
                plan.ctx(),
            ));
        }
        let exprs: Vec<ExprImpl> = join
            .output_indices()
            .iter()
            .map(|&index| rewriter.column(index, data_types[index].clone()))
            .collect();
        let kept = if constant_is_right { left } else { right };
        Some(LogicalProject::create(
            LogicalFilter::create(kept, predicate),
            exprs,
        ))
    }
}

impl ConstantRowJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ConstantRowJoinRule)
    }
}

/// Returns the row of a `LogicalValues` that holds exactly one row of constants.
fn constant_row(plan: &PlanRef) -> Option<Vec<ExprImpl>> {
    let values = plan.as_logical_values()?;
    let [row] = values.rows() else {
        return None;
    };
    let fields = plan.schema().fields();
    (row.len() == fields.len()
        && row
            .iter()
            .zip(fields)
            .all(|(expr, field)| expr.is_const() && expr.return_type() == field.data_type))
    .then(|| row.clone())
}

/// Rewrites the references to the internal columns of the join, over the side that is kept.
struct ConstantSideRewriter<'a> {
    row: &'a [ExprImpl],
    left_len: usize,
    constant_is_right: bool,
}

impl ConstantSideRewriter<'_> {
    fn column(&self, index: usize, data_type: DataType) -> ExprImpl {
        match (self.constant_is_right, index < self.left_len) {
            (true, true) => InputRef::new(index, data_type).into(),
            (true, false) => self.row[index - self.left_len].clone(),
            (false, true) => self.row[index].clone(),
            (false, false) => InputRef::new(index - self.left_len, data_type).into(),
        }
    }
}

impl ExprRewriter for ConstantSideRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        self.column(input_ref.index(), input_ref.return_type())
    }
}
