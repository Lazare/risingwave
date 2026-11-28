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

//! Narrows the group key of an aggregation with the functional dependencies of its input, and
//! replaces the aggregates of values that are the same in every row of a group (Calcite
//! `AggregateRemoveDuplicateKeysRule`, `AggregateReduceFunctionsOnGroupKeysRule`,
//! `AggregateProjectPullUpConstantsRule`; PostgreSQL `remove_useless_groupby_columns`).
//!
//! ```text
//! select o.id, o.placed, sum(i.qty) from o join i on i.o_id = o.id group by o.id, o.placed
//!   => group by o.id, internal_last_seen_value(o.placed)       (o.id -> o.placed)
//! select a, b, count(*) from t where b = 3 group by a, b
//!   => group by a, internal_last_seen_value(b)                  (b is a constant)
//! select k, max(k), min(k) from t group by k
//!   => group by k, projected as k, k, k
//! ```
//!
//! - A group key column `g` that the other group key columns kept determine (the functional
//!   dependencies of the input, and the constant columns: `c = literal` filters, literals of a
//!   projection right below) holds the same value in every row of a group: the groups are the
//!   same without it, and its value is that of any row (`internal_last_seen_value`). At least one
//!   column is kept: an aggregation without group key returns a row on an empty input.
//!   `AggGroupBySimplifyRule` does the same only when the whole group key is a key of the input.
//! - `min`, `max`, `first_value`, `last_value` and `internal_last_seen_value` (without `FILTER`)
//!   of a column that the kept group key determines are that value: every row of the group has
//!   it (`NULL` included: the functional dependencies hold with `NULL` as a value, and `min` of
//!   `NULL`s is `NULL`). A kept group key column is read from the group key; another column from
//!   one `internal_last_seen_value` call, whose state is one value per group, where `min` and `max`
//!   over an input that retracts rows keep every value of the group.
//!
//! In a streaming job the group key is the key of the state and of the output (a materialized
//! view gets a narrower primary key). Not in a streaming job that may carry watermarks (a source,
//! `now()`, a table with a watermark below the aggregation): a column with a watermark must stay
//! in the group key for the state cleaning and `EMIT ON WINDOW CLOSE`. On demand, behind
//! `streaming_reduce_group_keys` and `batch_reduce_group_keys`.

use std::collections::{HashMap, HashSet};

use fixedbitset::FixedBitSet;
use risingwave_expr::aggregate::{AggType, PbAggKind};

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{LogicalProject, PlanNodeId, PlanTreeNodeUnary};
use crate::utils::{Condition, IndexSet};

/// See the documentation of the module.
pub struct AggGroupKeyReduceRule {
    for_stream: bool,
}

impl AggGroupKeyReduceRule {
    pub fn create_for_stream() -> BoxedRule {
        Box::new(AggGroupKeyReduceRule { for_stream: true })
    }

    pub fn create_for_batch() -> BoxedRule {
        Box::new(AggGroupKeyReduceRule { for_stream: false })
    }
}

/// Whether a plan may carry a watermark in a streaming job: a source, `now()`, or a scan of a
/// table with a watermark column. Shared sub-plans are visited once.
fn may_carry_watermark(plan: &PlanRef, visited: &mut HashSet<PlanNodeId>) -> bool {
    if !visited.insert(plan.id()) {
        return false;
    }
    if plan.as_logical_source().is_some() || plan.as_logical_now().is_some() {
        return true;
    }
    if let Some(scan) = plan.as_logical_scan()
        && scan.table().watermark_columns.ones().next().is_some()
    {
        return true;
    }
    plan.inputs()
        .iter()
        .any(|input| may_carry_watermark(input, visited))
}

/// What an output column of the aggregation becomes.
#[derive(Clone, Copy)]
enum Out {
    /// The kept group key column at this position of the new group key.
    Key(usize),
    /// The new aggregate call at this position.
    Call(usize),
}

impl Rule<Logical> for AggGroupKeyReduceRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        if !agg.grouping_sets().is_empty() {
            return None;
        }
        let input = agg.input();
        if self.for_stream && may_carry_watermark(&input, &mut HashSet::new()) {
            return None;
        }
        let width = input.schema().len();
        let group_key: Vec<usize> = agg.group_key().indices().collect();

        // The functional dependencies of the input, with the literals of a projection below.
        let mut fd = input.functional_dependency().clone();
        if let Some(project) = input.as_logical_project() {
            for (index, expr) in project.exprs().iter().enumerate() {
                if matches!(expr, ExprImpl::Literal(_)) {
                    fd.add_constant_columns(&[index]);
                }
            }
        }
        let bits = |columns: &[usize]| {
            let mut set = FixedBitSet::with_capacity(width);
            for &column in columns {
                set.insert(column);
            }
            set
        };

        // The group key columns that the others determine are removed, from the last one: at
        // least one column is kept.
        let mut kept = group_key.clone();
        for &column in group_key.iter().rev() {
            if kept.len() == 1 {
                break;
            }
            let rest: Vec<usize> = kept.iter().copied().filter(|&c| c != column).collect();
            if fd.is_determined_by(&bits(&rest), &bits(&[column])) {
                kept = rest;
            }
        }
        let new_group_key = IndexSet::from(kept.clone());
        let new_key_positions: Vec<usize> = new_group_key.indices().collect();
        let key_position = |column: usize| new_key_positions.iter().position(|&c| c == column);
        let kept_bits = bits(&kept);
        let is_determined = |column: usize| fd.is_determined_by(&kept_bits, &bits(&[column]));

        let mut changed = kept.len() < group_key.len();
        let mut new_calls: Vec<PlanAggCall> = vec![];
        // The `internal_last_seen_value` call of a column, by column.
        let mut last_seen: HashMap<usize, usize> = HashMap::new();
        let mut last_seen_of = |column: usize, new_calls: &mut Vec<PlanAggCall>| -> usize {
            *last_seen.entry(column).or_insert_with(|| {
                let data_type = input.schema().fields()[column].data_type();
                new_calls.push(PlanAggCall {
                    agg_type: PbAggKind::InternalLastSeenValue.into(),
                    return_type: data_type.clone(),
                    inputs: vec![InputRef::new(column, data_type)],
                    distinct: false,
                    order_by: vec![],
                    filter: Condition::true_cond(),
                    direct_args: vec![],
                });
                new_calls.len() - 1
            })
        };

        let mut outs: Vec<Out> = vec![];
        for &column in &group_key {
            outs.push(match key_position(column) {
                Some(position) => Out::Key(position),
                None => Out::Call(last_seen_of(column, &mut new_calls)),
            });
        }
        for call in agg.agg_calls() {
            let value_of_group = matches!(
                call.agg_type,
                AggType::Builtin(
                    PbAggKind::Min
                        | PbAggKind::Max
                        | PbAggKind::FirstValue
                        | PbAggKind::LastValue
                        | PbAggKind::InternalLastSeenValue
                )
            ) && call.inputs.len() == 1
                && call.filter.always_true()
                && call.direct_args.is_empty()
                && call.return_type == call.inputs[0].data_type;
            let column = call.inputs.first().map(|input_ref| input_ref.index());
            let out = match column {
                Some(column) if value_of_group && is_determined(column) => {
                    match key_position(column) {
                        Some(position) => {
                            changed = true;
                            Out::Key(position)
                        }
                        None => {
                            if !matches!(
                                call.agg_type,
                                AggType::Builtin(PbAggKind::InternalLastSeenValue)
                            ) {
                                changed = true;
                            }
                            Out::Call(last_seen_of(column, &mut new_calls))
                        }
                    }
                }
                _ => {
                    new_calls.push(call.clone());
                    Out::Call(new_calls.len() - 1)
                }
            };
            outs.push(out);
        }
        if !changed {
            return None;
        }

        let key_len = new_group_key.len();
        let new_agg: PlanRef = Agg::new(new_calls, new_group_key, input.clone()).into();
        let out_fields = outs.into_iter().map(|out| match out {
            Out::Key(position) => position,
            Out::Call(position) => key_len + position,
        });
        Some(LogicalProject::with_out_col_idx(new_agg, out_fields).into())
    }
}
