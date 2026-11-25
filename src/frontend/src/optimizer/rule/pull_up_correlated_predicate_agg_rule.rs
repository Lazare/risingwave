// Copyright 2024 RisingWave Labs
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

use fixedbitset::FixedBitSet;
use itertools::{Either, Itertools};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_expr_visitor::Strong;
use crate::optimizer::plan_node::generic::{Agg, GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::*;
use crate::optimizer::plan_visitor::{PlanCorrelatedIdFinder, PlanVisitor};
use crate::utils::{Condition, IndexSet};

/// Pull up correlated predicates from the right agg side of Apply to the `on` clause of Join.
///
/// Before:
///
/// ```text
///        Filter
///          |
///     LogicalApply
///    /            \
///  LHS          Project
///                 |
///                Agg [group by nothing]
///                 |
///               Project
///                 |
///               Filter [correlated_input_ref(yyy) = xxx]
/// ```
///
/// After:
///
/// ```text
///        Filter
///          |
///     LogicalJoin [yyy = xxx]
///    /            \
///  LHS          Project
///                 |
///                Agg [group by xxx]
///                 |
///               Project
///                 |
///               Filter
/// ```
///
/// The variant for streaming plans (`create_for_stream`) also applies when the result of the
/// aggregation over an empty input is not NULL (`count`, `EXISTS`, `coalesce(array_agg(..), ..)`):
/// a projection above the join gives the value of the empty input to the outer rows that no group
/// matched. It also applies to a scalar subquery still marked as returning at most one row (an
/// aggregation without group key does), and only to a left outer apply, the form of a subquery in
/// a select list or a condition.
pub struct PullUpCorrelatedPredicateAggRule {
    for_stream: bool,
}
impl Rule<Logical> for PullUpCorrelatedPredicateAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_filter = if let Some(top_filter) = plan.as_logical_filter() {
            top_filter.clone()
        } else {
            LogicalFilter::new(plan, Condition::true_cond())
        };
        let top_filter_input = top_filter.input();
        let apply = top_filter_input.as_logical_apply()?;
        let (apply_left, apply_right, apply_on, join_type, correlated_id, _, max_one_row) =
            apply.clone().decompose();

        if max_one_row && !self.for_stream {
            return None;
        }
        if self.for_stream && join_type != JoinType::LeftOuter {
            return None;
        }

        let top_project = if let Some(project) = apply_right.as_logical_project() {
            project.clone()
        } else {
            LogicalProject::with_mapping(
                apply_right.clone(),
                ColIndexMapping::identity(apply_right.schema().len()),
            )
        };
        let (top_proj_exprs, _) = top_project.clone().decompose();

        let input = top_project.input();
        let agg: &LogicalAgg = input.as_logical_agg()?;
        let (agg_calls, group_key, grouping_sets, input, _enable_two_phase) =
            agg.clone().decompose();
        // It could be too restrictive to require the group key to be empty. We can relax this in the future if necessary.
        if !group_key.is_empty() {
            return None;
        }
        assert!(grouping_sets.is_empty());
        // The value of every aggregate call over an empty input, for the variant for streaming.
        let empty_values = if self.for_stream {
            agg_calls
                .iter()
                .map(empty_input_value)
                .collect::<Option<Vec<_>>>()?
        } else {
            vec![]
        };
        // Subqueries with the same correlated filter share it: the variant for streaming looks
        // through the share, the general way would remove it too.
        let input = if self.for_stream {
            skip_share(input)
        } else {
            input
        };
        let bottom_project = if let Some(project) = input.as_logical_project() {
            project.clone()
        } else {
            LogicalProject::with_mapping(
                input.clone(),
                ColIndexMapping::identity(input.schema().len()),
            )
        };
        let (mut bottom_proj_exprs, _) = bottom_project.clone().decompose();
        let bottom_project_input = if self.for_stream {
            skip_share(bottom_project.input())
        } else {
            bottom_project.input()
        };
        let bottom_filter: &LogicalFilter = bottom_project_input.as_logical_filter()?;

        // Split predicates in LogicalFilter into correlated expressions and uncorrelated
        // expressions.
        let (cor_exprs, uncor_exprs): (Vec<ExprImpl>, Vec<ExprImpl>) = bottom_filter
            .predicate()
            .clone()
            .into_iter()
            .partition_map(|expr| {
                if expr.has_correlated_input_ref_by_correlated_id(correlated_id) {
                    Either::Left(expr)
                } else {
                    Either::Right(expr)
                }
            });

        // Ensure all correlated expressions look like [correlated_input_ref = input_ref]
        let mut cor_eq_exprs = vec![];
        for cor_expr in &cor_exprs {
            if let Some((input_ref, cor_input_ref)) = cor_expr.as_eq_correlated_input_ref() {
                if cor_input_ref.correlated_id() == correlated_id {
                    cor_eq_exprs.push((input_ref, cor_input_ref));
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
        let cor_eq_exprs_len = cor_eq_exprs.len();

        let filter = LogicalFilter::create(
            bottom_filter.input(),
            Condition {
                conjunctions: uncor_exprs,
            },
        );

        // Append `InputRef`s in the predicate expression to be pulled to the project, so that they
        // are accessible by the expression after it is pulled.
        bottom_proj_exprs.extend(
            cor_eq_exprs
                .iter()
                .map(|(input_ref, _)| ExprImpl::InputRef(input_ref.clone().into())),
        );

        let new_bottom_proj: PlanRef = LogicalProject::new(filter, bottom_proj_exprs).into();

        // The variant for streaming: the outputs that are not NULL over an empty input get, for the
        // outer rows that no group matches, the value they have over an empty input.
        let mut empty_outputs: Vec<Option<ExprImpl>> = vec![];
        if self.for_stream {
            let mut null_aggs = FixedBitSet::with_capacity(agg_calls.len());
            for (i, value) in empty_values.iter().enumerate() {
                if value.is_null() {
                    null_aggs.insert(i);
                }
            }
            let mut substitute = EmptyInputRewriter {
                values: &empty_values,
            };
            empty_outputs = top_proj_exprs
                .iter()
                .map(|expr| {
                    (!Strong::is_null(expr, null_aggs.clone()))
                        .then(|| fold_constant(substitute.rewrite_expr(expr.clone())))
                })
                .collect();
            // A condition of the apply could reject a matched group: its row would then be padded
            // with NULLs, not with the values over an empty input.
            if empty_outputs.iter().any(Option::is_some) && !apply_on.always_true() {
                return None;
            }
        }

        // We can apply this rule only if:
        // 1. The `group by + proj` returns null for empty input
        // 2. OR the top filter is null for empty input
        if !self.for_stream {
            // When group input is empty, if the agg is not `count`, it would return null.
            let null_agg_pos = agg_calls
                .iter()
                .positions(|agg_call| {
                    !matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::Count))
                })
                .collect_vec();

            // We don't have null args, so the output will never be null. Bail out.
            if null_agg_pos.is_empty() {
                return None;
            }

            // Try to prove that the expression is null-rejected by the top filter when not-count-aggs are null.
            let mut agg_null_bitset = FixedBitSet::with_capacity(agg.base.schema().len());
            for pos in null_agg_pos {
                agg_null_bitset.insert(pos);
            }

            // Shift the top project expressions to the right by apply_left schema len, because it is used to check null-rejected by the top filter.
            let apply_left_schema = apply_left.schema().len();
            let mut top_proj_all_null = true;
            let mut top_proj_null_bitset =
                FixedBitSet::with_capacity(top_project.base.schema().len() + apply_left_schema);
            for (i, expr) in top_proj_exprs.iter().enumerate() {
                if Strong::is_null(expr, agg_null_bitset.clone()) {
                    top_proj_null_bitset.insert(i + apply_left_schema);
                } else {
                    top_proj_all_null = false;
                }
            }

            let top_filter_any_null = top_filter
                .predicate()
                .conjunctions
                .iter()
                .any(|expr| Strong::is_null(expr, top_proj_null_bitset.clone()));
            let can_apply = top_proj_all_null || top_filter_any_null;

            if !can_apply {
                return None;
            }
        }

        // New agg with group key extracted from the cor_eq_exprs.
        let new_agg = Agg::new(
            agg_calls,
            IndexSet::from_iter(
                new_bottom_proj.schema().len() - cor_eq_exprs_len..new_bottom_proj.schema().len(),
            ),
            new_bottom_proj,
        );

        let mut shift_input_ref_rewriter = ShiftInputRefRewriter {
            offset: cor_eq_exprs_len,
        };
        // Shift the top project expressions to the right by cor_eq_exprs_len.
        let mut top_proj_exprs = top_proj_exprs
            .into_iter()
            .map(|expr| shift_input_ref_rewriter.rewrite_expr(expr))
            .collect_vec();
        top_proj_exprs.extend((0..new_agg.group_key.len()).map(|i| {
            ExprImpl::InputRef(
                InputRef::new(i, new_agg.schema().fields[i].data_type.clone()).into(),
            )
        }));

        let new_top_proj: PlanRef = LogicalProject::new(new_agg.into(), top_proj_exprs).into();

        let cor_eq_exprs = cor_eq_exprs
            .into_iter()
            .enumerate()
            .map(|(i, (input_ref, correlated_input_ref))| {
                ExprImpl::FunctionCall(
                    FunctionCall::new_unchecked(
                        ExprType::Equal,
                        vec![
                            InputRef::new(
                                correlated_input_ref.index(),
                                correlated_input_ref.return_type(),
                            )
                            .into(),
                            InputRef::new(
                                new_top_proj.schema().len() - cor_eq_exprs_len
                                    + i
                                    + apply_left.schema().len(),
                                input_ref.return_type(),
                            )
                            .into(),
                        ],
                        DataType::Boolean,
                    )
                    .into(),
                )
            })
            .collect_vec();

        // Check whether correlated_input_ref with same correlated_id exists for the join right
        // side. If yes, bail out and leave for general subquery unnesting to deal with
        let mut plan_correlated_id_finder = PlanCorrelatedIdFinder::default();
        plan_correlated_id_finder.visit(new_top_proj.clone());
        if plan_correlated_id_finder.contains(&correlated_id) {
            return None;
        }

        // Merge these expressions with LogicalApply into LogicalJoin.
        let on = apply_on.and(Condition {
            conjunctions: cor_eq_exprs,
        });

        let apply_len = apply.schema().len();
        let apply_left_len = apply_left.schema().len();
        if empty_outputs.iter().any(Option::is_some) {
            // Keep the first group key column: it is NULL exactly when no group matched, because the
            // join compares it with `=`.
            let key_position = apply_left_len + new_top_proj.schema().len() - cor_eq_exprs_len;
            let key_type = new_top_proj.schema().fields()
                [new_top_proj.schema().len() - cor_eq_exprs_len]
                .data_type
                .clone();
            let new_join: PlanRef = LogicalJoin::with_output_indices(
                apply_left,
                new_top_proj,
                join_type,
                on,
                (0..apply_len).chain([key_position]).collect(),
            )
            .into();
            let matched: ExprImpl = FunctionCall::new_unchecked(
                ExprType::IsNotNull,
                vec![InputRef::new(apply_len, key_type).into()],
                DataType::Boolean,
            )
            .into();
            let exprs = (0..apply_len)
                .map(|i| {
                    let data_type = new_join.schema().fields()[i].data_type.clone();
                    let column: ExprImpl = InputRef::new(i, data_type.clone()).into();
                    match i
                        .checked_sub(apply_left_len)
                        .and_then(|j| empty_outputs[j].clone())
                    {
                        Some(empty) => FunctionCall::new_unchecked(
                            ExprType::Case,
                            vec![matched.clone(), column, empty],
                            data_type,
                        )
                        .into(),
                        None => column,
                    }
                })
                .collect();
            let project: PlanRef = LogicalProject::new(new_join, exprs).into();
            return Some(if top_filter.predicate().always_true() {
                project
            } else {
                top_filter.clone_with_input(project).into()
            });
        }

        let new_join = LogicalJoin::with_output_indices(
            apply_left,
            new_top_proj,
            join_type,
            on,
            (0..apply_len).collect(),
        )
        .into();

        if top_filter.predicate().always_true() {
            Some(new_join)
        } else {
            Some(top_filter.clone_with_input(new_join).into())
        }
    }
}

struct ShiftInputRefRewriter {
    offset: usize,
}
impl ExprRewriter for ShiftInputRefRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}

/// Replaces the outputs of the aggregation with their values over an empty input.
struct EmptyInputRewriter<'a> {
    values: &'a [ExprImpl],
}
impl ExprRewriter for EmptyInputRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        self.values[input_ref.index()].clone()
    }
}

fn skip_share(plan: PlanRef) -> PlanRef {
    match plan.as_logical_share() {
        Some(share) => share.input(),
        None => plan,
    }
}

fn fold_constant(expr: ExprImpl) -> ExprImpl {
    match expr.try_fold_const() {
        Some(Ok(datum)) => Literal::new(datum, expr.return_type()).into(),
        _ => expr,
    }
}

/// The value of an aggregate call over an empty input, for the calls whose value is known.
fn empty_input_value(call: &PlanAggCall) -> Option<ExprImpl> {
    let AggType::Builtin(kind) = &call.agg_type else {
        return None;
    };
    let datum = match kind {
        PbAggKind::Count if call.return_type == DataType::Int64 => Some(ScalarImpl::Int64(0)),
        PbAggKind::Sum
        | PbAggKind::Min
        | PbAggKind::Max
        | PbAggKind::Avg
        | PbAggKind::StringAgg
        | PbAggKind::ArrayAgg
        | PbAggKind::FirstValue
        | PbAggKind::LastValue
        | PbAggKind::VarPop
        | PbAggKind::VarSamp
        | PbAggKind::StddevPop
        | PbAggKind::StddevSamp
        | PbAggKind::BitAnd
        | PbAggKind::BitOr
        | PbAggKind::BitXor
        | PbAggKind::BoolAnd
        | PbAggKind::BoolOr
        | PbAggKind::JsonbAgg
        | PbAggKind::JsonbObjectAgg
        | PbAggKind::ArgMin
        | PbAggKind::ArgMax => None,
        _ => return None,
    };
    Some(Literal::new(datum, call.return_type.clone()).into())
}

impl PullUpCorrelatedPredicateAggRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedPredicateAggRule { for_stream: false })
    }

    /// The variant for streaming plans, see the rule.
    pub fn create_for_stream() -> BoxedRule {
        Box::new(PullUpCorrelatedPredicateAggRule { for_stream: true })
    }
}
