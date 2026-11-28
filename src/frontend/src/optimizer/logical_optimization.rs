// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use risingwave_common::bail;
use thiserror_ext::AsReport as _;

use super::plan_node::{ConventionMarker, Logical, LogicalPlanRef};
use super::plan_visitor::has_logical_max_one_row;
use crate::error::Result;
use crate::expr::NowProcTimeFinder;
use crate::optimizer::PlanRef;
use crate::optimizer::heuristic_optimizer::{ApplyOrder, HeuristicOptimizer};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, VisitExprsRecursive,
};
use crate::optimizer::plan_rewriter::ShareSourceRewriter;
#[cfg(debug_assertions)]
use crate::optimizer::plan_visitor::InputRefValidator;
use crate::optimizer::plan_visitor::{
    HasMaxOneRowApply, PlanCheckApplyEliminationExt, PlanVisitor, has_logical_apply,
};
use crate::optimizer::rule::*;
use crate::utils::Condition;
use crate::{Binder, Explain, OptimizerContextRef, Planner};

impl<C: ConventionMarker> PlanRef<C> {
    fn optimize_by_rules_inner(
        self,
        heuristic_optimizer: &mut HeuristicOptimizer<'_, C>,
        stage_name: &str,
    ) -> Result<PlanRef<C>> {
        let ctx = self.ctx();

        let result = heuristic_optimizer.optimize(self);
        let stats = heuristic_optimizer.get_stats();

        if ctx.is_explain_trace() && stats.has_applied_rule() {
            ctx.trace(format!("{}:", stage_name));
            ctx.trace(format!("{}", stats));
            ctx.trace(match &result {
                Ok(plan) => plan.explain_to_string(),
                Err(error) => format!("Optimization failed: {}", error.as_report()),
            });
        }
        ctx.add_rule_applied(stats.total_applied());

        result
    }

    pub(crate) fn optimize_by_rules(
        self,
        OptimizationStage {
            stage_name,
            rules,
            apply_order,
        }: &OptimizationStage<C>,
    ) -> Result<PlanRef<C>> {
        self.optimize_by_rules_inner(&mut HeuristicOptimizer::new(apply_order, rules), stage_name)
    }

    pub(crate) fn optimize_by_rules_until_fix_point(
        mut self,
        OptimizationStage {
            stage_name,
            rules,
            apply_order,
        }: &OptimizationStage<C>,
    ) -> Result<PlanRef<C>> {
        loop {
            let mut heuristic_optimizer = HeuristicOptimizer::new(apply_order, rules);
            self = self.optimize_by_rules_inner(&mut heuristic_optimizer, stage_name)?;
            if !heuristic_optimizer.get_stats().has_applied_rule() {
                return Ok(self);
            }
        }
    }
}

pub struct OptimizationStage<C: ConventionMarker = Logical> {
    stage_name: String,
    rules: Vec<BoxedRule<C>>,
    apply_order: ApplyOrder,
}

impl<C: ConventionMarker> OptimizationStage<C> {
    pub fn new<S>(name: S, rules: Vec<BoxedRule<C>>, apply_order: ApplyOrder) -> Self
    where
        S: Into<String>,
    {
        OptimizationStage {
            stage_name: name.into(),
            rules,
            apply_order,
        }
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use risingwave_common::id::ObjectId;
use risingwave_pb::common::PbObjectType;
use risingwave_sqlparser::ast::Statement;

use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_visitor::RelationCollectorVisitor;
use crate::session::SessionImpl;

pub struct LogicalOptimizer {}

static DAG_TO_TREE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "DAG To Tree",
        vec![DagToTreeRule::create()],
        ApplyOrder::TopDown,
    )
});

static STREAM_GENERATE_SERIES_WITH_NOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert GENERATE_SERIES Ends With NOW",
        vec![GenerateSeriesWithNowRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_CONVERT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function Convert",
        vec![
            // Apply file scan rule first
            TableFunctionToFileScanRule::create(),
            // Apply internal backfill progress rule first
            TableFunctionToInternalBackfillProgressRule::create(),
            // Apply internal source backfill progress rule next
            TableFunctionToInternalSourceBackfillProgressRule::create(),
            // Apply internal get channel delta stats rule next
            TableFunctionToInternalGetChannelDeltaStatsRule::create(),
            // Apply postgres query rule next
            TableFunctionToPostgresQueryRule::create(),
            // Apply mysql query rule next
            TableFunctionToMySqlQueryRule::create(),
            // Apply project set rule last
            TableFunctionToProjectSetRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_FILE_SCAN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function To FileScan",
        vec![TableFunctionToFileScanRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_POSTGRES_QUERY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function To PostgresQuery",
        vec![TableFunctionToPostgresQueryRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_MYSQL_QUERY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function To MySQL",
        vec![TableFunctionToMySqlQueryRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_INTERNAL_BACKFILL_PROGRESS: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Table Function To Internal Backfill Progress",
            vec![TableFunctionToInternalBackfillProgressRule::create()],
            ApplyOrder::TopDown,
        )
    });

static TABLE_FUNCTION_TO_INTERNAL_SOURCE_BACKFILL_PROGRESS: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Table Function To Internal Source Backfill Progress",
            vec![TableFunctionToInternalSourceBackfillProgressRule::create()],
            ApplyOrder::TopDown,
        )
    });

static TABLE_FUNCTION_TO_INTERNAL_GET_CHANNEL_DELTA_STATS: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Table Function To Internal Get Channel Delta Stats",
            vec![TableFunctionToInternalGetChannelDeltaStatsRule::create()],
            ApplyOrder::TopDown,
        )
    });

static VALUES_EXTRACT_PROJECT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Values Extract Project",
        vec![ValuesExtractProjectRule::create()],
        ApplyOrder::TopDown,
    )
});

static SIMPLE_UNNESTING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simple Unnesting",
        vec![
            // Pull correlated predicates up the algebra tree to unnest simple subquery.
            PullUpCorrelatedPredicateRule::create(),
            // Pull correlated project expressions with values to inline scalar subqueries.
            PullUpCorrelatedProjectValueRule::create(),
            PullUpCorrelatedPredicateAggRule::create(),
            // Eliminate max one row
            MaxOneRowEliminateRule::create(),
            // Eliminate lateral table-function apply into a unary ProjectSet.
            ApplyTableFunctionToProjectSetRule::create(),
            // Convert apply to join.
            ApplyToJoinRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static STREAM_SIMPLE_UNNESTING_AGG: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Stream Simple Unnesting of Aggregations",
        vec![
            ApplyOneRowJoinSplitRule::create(),
            PullUpCorrelatedPredicateAggRule::create_for_stream(),
        ],
        ApplyOrder::BottomUp,
    )
});

// Rule `OuterJoinReduceRule` (`streaming_reduce_outer_joins`, `batch_reduce_outer_joins`): an
// outer join whose padded rows a filter or a join above rejects becomes an inner join, and one
// filtered on `r.c IS NULL` for a column never `NULL` in a matched row becomes an anti join.
// Top-down: a join reduced above lets the conditions it pushes down reach the joins below.
static REDUCE_OUTER_JOINS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Reduce Outer Joins",
        vec![OuterJoinReduceRule::create()],
        ApplyOrder::TopDown,
    )
});

// Rule `NullSafeEqToEqRule` (`streaming_null_safe_eq_to_eq`, `batch_null_safe_eq_to_eq`):
// `a IS NOT DISTINCT FROM b` becomes `a = b` in a predicate when `a` or `b` is never `NULL`.
// Bottom-up: a join condition rewritten below tells the join above that its keys are not `NULL`.
static NULL_SAFE_EQ_TO_EQ: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Null-Safe Equality To Equality",
        vec![NullSafeEqToEqRule::create()],
        ApplyOrder::BottomUp,
    )
});

// Rule `ExistsSubquerySimplifyRule` (`streaming_simplify_exists_subquery`,
// `batch_simplify_exists_subquery`): the limit, `DISTINCT` and `GROUP BY` without aggregates that
// can not change the result of a correlated `EXISTS` / `IN` subquery are removed before unnesting.
static SIMPLIFY_EXISTS_SUBQUERY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify Exists Subquery",
        vec![ExistsSubquerySimplifyRule::create()],
        ApplyOrder::TopDown,
    )
});

// Batch counterparts of `STREAM_PRUNE_EMPTY_RELATION`, `STREAM_SIMPLIFY_ON_UNIQUE_KEY` and
// `STREAM_MERGE_REPEATED_LOOKUPS`, with the same rules (`batch_simplify_column_facts`,
// `batch_simplify_on_unique_key`, `batch_merge_repeated_lookups`), see `reduce_batch_on_keys`.
static BATCH_SIMPLIFY_COLUMN_FACTS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify With Column Facts For Batch",
        vec![
            // In this order: a predicate simplified with what is known about its columns may
            // become never true, which may empty an input of the operator above.
            ColumnFactsSimplifyRule::create(),
            NeverTruePredicateRule::create(),
            EmptyRelationPropagateRule::create(),
            EmptyInputSimplifyRule::create(),
            ConstantRowJoinRule::create(),
        ],
        // Bottom-up so that an empty relation is propagated to the top in one pass.
        ApplyOrder::BottomUp,
    )
});

// Batch counterparts of stages of the streaming plan reduction prototype, each behind its own
// batch variable: the same rules, or their variants for batch plans.
// `batch_simplify_subquery_unnesting`: the variant of `PullUpCorrelatedPredicateAggRule` named
// "for stream" has nothing specific to streaming (no retraction is involved).
static BATCH_SIMPLE_UNNESTING_AGG: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Batch Simple Unnesting of Aggregations",
        vec![
            ApplyOneRowJoinSplitRule::create(),
            PullUpCorrelatedPredicateAggRule::create_for_stream(),
        ],
        ApplyOrder::BottomUp,
    )
});

static BATCH_SIMPLIFY_ON_UNIQUE_KEY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify On Unique Key For Batch",
        vec![
            UnusedOuterJoinEliminateRule::create(),
            AggOnKeyEliminateRule::create(),
        ],
        // Bottom-up: an operator removed below may give a key to the input of an operator above.
        ApplyOrder::BottomUp,
    )
});

static BATCH_MERGE_REPEATED_LOOKUPS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Repeated Lookups For Batch",
        vec![RepeatedLookupMergeRule::create()],
        // Bottom-up: a lookup merged below is carried by the input of the lookups above.
        ApplyOrder::BottomUp,
    )
});

// Rules `CostBasedJoinSidesRule` and `CostBasedJoinOrderingRule` (`batch_cost_based_join_ordering`)
// are built for each query (they are given the columns that the plan reads); the rule
// `CostBasedOuterJoinSidesRule` runs after the column pruning.
static BATCH_COST_BASED_OUTER_JOIN_SIDES: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Cost-Based Outer Join Sides For Batch",
        vec![CostBasedOuterJoinSidesRule::create()],
        ApplyOrder::TopDown,
    )
});

// Rule `LagDuplicatesRule` (`batch_rewrite_lag_duplicates`).
static BATCH_REWRITE_LAG_DUPLICATES: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Rewrite Lag Duplicates For Batch",
        vec![LagDuplicatesRule::create()],
        // Top-down: an aggregation counting the duplicates is rewritten before the projection
        // below it.
        ApplyOrder::TopDown,
    )
});

// `batch_unnest_lateral_functions`, see `STREAM_UNNEST_ARRAY_OF_FUNCTION`,
// `STREAM_UNNEST_LATERAL_FUNCTIONS` and `STREAM_SELF_AGGREGATE_JOIN`.
static BATCH_UNNEST_ARRAY_OF_FUNCTION: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Batch Unnest Array Of Lateral Function",
        vec![UnnestArrayOfTableFunctionRule::create()],
        ApplyOrder::TopDown,
    )
});

static BATCH_UNNEST_LATERAL_FUNCTIONS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Batch Unnest Lateral Functions",
        vec![
            ApplyOneRowJoinSplitRule::create(),
            LateralTableFunctionRule::create_for_batch(),
        ],
        ApplyOrder::BottomUp,
    )
});

static BATCH_SELF_AGGREGATE_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Batch Self Aggregate Join",
        vec![SelfAggregateJoinRule::create_for_batch()],
        ApplyOrder::BottomUp,
    )
});

// `batch_factor_union_join`: rule `CommonJoinOverUnionRule`.
static BATCH_FACTOR_UNION_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Factor Union Join For Batch",
        vec![CommonJoinOverUnionRule::create()],
        // Bottom-up: the branches of a union are reduced before the union is looked at.
        ApplyOrder::BottomUp,
    )
});

// `batch_merge_union_scans`: rule `UnionScanMergeRule`. Not `UnionScanShareRule`: a batch plan
// has no shared sub-plan.
static BATCH_MERGE_UNION_SCANS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Union Scans For Batch",
        vec![UnionScanMergeRule::create_for_batch()],
        ApplyOrder::BottomUp,
    )
});

// Batch join rules of the survey of Calcite and PostgreSQL rules, each behind its own variable.
// Rules `InnerJoinToSemiJoinRule` and `SemiJoinDistinctRemoveRule`
// (`batch_inner_join_to_semi_join`): the second one applies to the semi join that the first one
// makes, in the same pass.
static BATCH_INNER_JOIN_TO_SEMI_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Inner Join To Semi Join For Batch",
        vec![
            InnerJoinToSemiJoinRule::create(),
            SemiJoinDistinctRemoveRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

// Rule `JoinKeyNotNullRule` (`batch_join_keys_not_null`).
static BATCH_JOIN_KEYS_NOT_NULL: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Keys Not Null For Batch",
        vec![JoinKeyNotNullRule::create()],
        ApplyOrder::TopDown,
    )
});

// Rule `DateRangePredicateRule` (`batch_date_range_predicates`).
static BATCH_DATE_RANGE_PREDICATES: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Date Range Predicates For Batch",
        vec![DateRangePredicateRule::create()],
        ApplyOrder::TopDown,
    )
});

// Rule `SemiJoinPlacementRule` (`batch_semi_join_ordering`): bottom-up, so that a semi join
// stacked over another one sees the inner joins that the lower one was pushed into.
static BATCH_SEMI_JOIN_PLACEMENT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Semi Join Placement For Batch",
        vec![SemiJoinPlacementRule::create()],
        ApplyOrder::BottomUp,
    )
});

// Rule `UnionScanMergeRule` (`streaming_merge_union_scans`).
static STREAM_MERGE_UNION_SCANS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Union Scans For Stream",
        vec![UnionScanMergeRule::create()],
        // Bottom-up: the branches of a union are reduced before the union is looked at.
        ApplyOrder::BottomUp,
    )
});

// Merge of stacked aggregations, see `StackedAggMergeRule`.
static STREAM_MERGE_STACKED_AGGS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Stacked Aggregations For Stream",
        vec![
            StackedAggMergeRule::create(),
            StackedAggOverUnionMergeRule::create(),
        ],
        // Bottom-up: an aggregation merged below can be merged again with the one above.
        ApplyOrder::BottomUp,
    )
});

// Per-row evaluation of lateral set-returning functions (`streaming_unnest_lateral_functions`):
// `unnest(ARRAY(SELECT srf(..)))` first, from the top, before the apply of the `ARRAY(..)` is
// evaluated per row on its own.
static STREAM_UNNEST_ARRAY_OF_FUNCTION: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Stream Unnest Array Of Lateral Function",
        vec![UnnestArrayOfTableFunctionRule::create()],
        ApplyOrder::TopDown,
    )
});

// A table joined back on its key with an aggregation of rows derived from itself
// (`streaming_unnest_lateral_functions`), after the final column pruning.
static STREAM_SELF_AGGREGATE_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Stream Self Aggregate Join",
        vec![SelfAggregateJoinRule::create()],
        ApplyOrder::BottomUp,
    )
});

static STREAM_UNNEST_LATERAL_FUNCTIONS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Stream Unnest Lateral Functions",
        vec![
            ApplyOneRowJoinSplitRule::create(),
            LateralTableFunctionRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

// Rule `UnionScanShareRule` (`streaming_share_union_scans`).
static STREAM_SHARE_UNION_SCANS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Share Union Scans For Stream",
        vec![UnionScanShareRule::create()],
        // Bottom-up: the scans of an inner union are shared before the outer union is looked at.
        ApplyOrder::BottomUp,
    )
});

// Rule `StatusLadderCaseRule` (`streaming_status_ladder_case`, `batch_status_ladder_case`).
static STATUS_LADDER_CASE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Status Ladder Case",
        vec![StatusLadderCaseRule::create()],
        ApplyOrder::BottomUp,
    )
});

// Rule `SetOperationToCountRule` (`*_set_operation_to_count`).
static SET_OPERATION_TO_COUNT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Set Operation To Count",
        vec![SetOperationToCountRule::create()],
        ApplyOrder::BottomUp,
    )
});

// Rule `SetOperationFilterMergeRule` (`*_merge_set_operation_filters`).
static STREAM_MERGE_SET_OPERATION_FILTERS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Set Operation Filters",
        vec![SetOperationFilterMergeRule::create()],
        ApplyOrder::BottomUp,
    )
});

static BATCH_MERGE_SET_OPERATION_FILTERS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Set Operation Filters",
        vec![SetOperationFilterMergeRule::create_for_batch()],
        ApplyOrder::BottomUp,
    )
});

// Rule `AggGroupKeyReduceRule` (`*_reduce_group_keys`).
static STREAM_REDUCE_GROUP_KEYS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Reduce Group Keys",
        vec![AggGroupKeyReduceRule::create_for_stream()],
        ApplyOrder::TopDown,
    )
});

static BATCH_REDUCE_GROUP_KEYS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Reduce Group Keys",
        vec![AggGroupKeyReduceRule::create_for_batch()],
        ApplyOrder::TopDown,
    )
});

// Rule `AggUnionNullBranchRule` (`*_prune_null_union_branches`).
static PRUNE_NULL_UNION_BRANCHES: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Prune Null Union Branches",
        vec![AggUnionNullBranchRule::create()],
        ApplyOrder::TopDown,
    )
});

static SET_OPERATION_MERGE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Set Operation Merge",
        vec![
            UnionMergeRule::create(),
            IntersectMergeRule::create(),
            ExceptMergeRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static GENERAL_UNNESTING_TRANS_APPLY_WITH_SHARE: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "General Unnesting(Translate Apply)",
            vec![TranslateApplyRule::create(true)],
            ApplyOrder::TopDown,
        )
    });

static GENERAL_UNNESTING_TRANS_APPLY_WITHOUT_SHARE: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "General Unnesting(Translate Apply)",
            vec![TranslateApplyRule::create(false)],
            ApplyOrder::TopDown,
        )
    });

static GENERAL_UNNESTING_PUSH_DOWN_APPLY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "General Unnesting(Push Down Apply)",
        vec![
            ApplyEliminateRule::create(),
            ApplyAggTransposeRule::create(),
            ApplyDedupTransposeRule::create(),
            ApplyFilterTransposeRule::create(),
            ApplyProjectTransposeRule::create(),
            ApplyProjectSetTransposeRule::create(),
            ApplyTopNTransposeRule::create(),
            ApplyLimitTransposeRule::create(),
            ApplyJoinTransposeRule::create(),
            ApplyUnionTransposeRule::create(),
            ApplyOverWindowTransposeRule::create(),
            ApplyExpandTransposeRule::create(),
            ApplyHopWindowTransposeRule::create(),
            CrossJoinEliminateRule::create(),
            ApplyShareEliminateRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static TO_MULTI_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "To MultiJoin",
        vec![MergeMultiJoinRule::create()],
        ApplyOrder::TopDown,
    )
});

static LEFT_DEEP_JOIN_ORDERING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Ordering".to_owned(),
        vec![LeftDeepTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    )
});

static BUSHY_TREE_JOIN_ORDERING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Ordering".to_owned(),
        vec![BushyTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    )
});

static FILTER_WITH_NOW_TO_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push down filter with now into a left semijoin",
        vec![
            SplitNowAndRule::create(),
            SplitNowOrRule::create(),
            FilterWithNowToJoinRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static PUSH_CALC_OF_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push down the calculation of inputs of join's condition",
        vec![PushCalculationOfJoinRule::create()],
        ApplyOrder::TopDown,
    )
});

static CONVERT_DISTINCT_AGG_FOR_STREAM: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![UnionToDistinctRule::create(), DistinctAggRule::create(true)],
        ApplyOrder::TopDown,
    )
});

static CONVERT_DISTINCT_AGG_FOR_BATCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![
            UnionToDistinctRule::create(),
            DistinctAggRule::create(false),
        ],
        ApplyOrder::TopDown,
    )
});

static SIMPLIFY_AGG: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify Aggregation",
        vec![
            AggGroupBySimplifyRule::create(),
            AggCallMergeRule::create(),
            UnifyFirstLastValueRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static JOIN_COMMUTE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Commute".to_owned(),
        vec![JoinCommuteRule::create()],
        ApplyOrder::TopDown,
    )
});

static CONSTANT_OUTPUT_REMOVE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Constant Output Operator Remove",
        vec![EmptyAggRemoveRule::create()],
        ApplyOrder::TopDown,
    )
});

static PROJECT_REMOVE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Project Remove",
        vec![
            // merge should be applied before eliminate
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            JoinProjectTransposeRule::create(),
            // project-join merge should be applied after merge
            // eliminate and to values
            ProjectJoinMergeRule::create(),
            AggProjectMergeRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static SPLIT_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Split Over Window",
        vec![OverWindowSplitRule::create()],
        ApplyOrder::TopDown,
    )
});

// the `OverWindowToTopNRule` need to match the pattern of Proj-Filter-OverWindow so it is
// 1. conflict with `ProjectJoinMergeRule`, `AggProjectMergeRule` or other rules
// 2. should be after merge the multiple projects
static CONVERT_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Over Window",
        vec![
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            OverWindowToAggAndJoinRule::create(),
            OverWindowToTopNRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

// DataFusion cannot apply `OverWindowToTopNRule`
static CONVERT_OVER_WINDOW_FOR_BATCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Over Window",
        vec![
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            OverWindowToAggAndJoinRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

// `batch_over_window_to_topn`: `CONVERT_OVER_WINDOW_FOR_BATCH` with the batch variant of
// `OverWindowToTopNRule` (`row_number()` only, small limits per group), for the queries that
// DataFusion does not run.
static BATCH_CONVERT_OVER_WINDOW_TO_TOPN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Over Window To TopN For Batch",
        vec![
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            OverWindowToAggAndJoinRule::create(),
            OverWindowToTopNRule::create_for_batch(),
        ],
        ApplyOrder::TopDown,
    )
});

static MERGE_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Over Window",
        vec![OverWindowMergeRule::create()],
        ApplyOrder::TopDown,
    )
});

static REWRITE_LIKE_EXPR: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Rewrite Like Expr",
        vec![RewriteLikeExprRule::create()],
        ApplyOrder::TopDown,
    )
});

static TOP_N_AGG_ON_INDEX: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "TopN/SimpleAgg on Index",
        vec![
            TopNProjectTransposeRule::create(),
            TopNOnIndexRule::create(),
            MinMaxOnIndexRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static PROJECT_TOP_N_TRANSPOSE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Project TopN Transpose",
        vec![ProjectTopNTransposeRule::create()],
        ApplyOrder::TopDown,
    )
});

static ALWAYS_FALSE_FILTER: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Void always-false filter's downstream",
        vec![AlwaysFalseFilterRule::create()],
        ApplyOrder::TopDown,
    )
});

static BATCH_PRUNE_EMPTY_RELATION: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Prune Empty Relation For Batch",
        vec![
            ContradictoryNullFilterRule::create(),
            EmptyRelationPropagateRule::create(),
        ],
        // Must be bottom-up so that an empty relation is propagated to the top in one pass.
        ApplyOrder::BottomUp,
    )
});

static STREAM_PRUNE_EMPTY_RELATION: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Prune Empty Relation For Stream",
        vec![
            // In this order: a predicate simplified with what is known about its columns may
            // become never true, which may empty an input of the operator above. The streaming
            // variants never empty a part of the plan that may carry a watermark.
            ColumnFactsSimplifyRule::create_for_stream(),
            NeverTruePredicateRule::create_for_stream(),
            EmptyRelationPropagateRule::create_for_stream(),
            EmptyInputSimplifyRule::create_for_stream(),
            ConstantRowJoinRule::create_for_stream(),
        ],
        // Must be bottom-up so that an empty relation is propagated to the top in one pass.
        ApplyOrder::BottomUp,
    )
});

// Rule `NullKeyJoinRule` (`streaming_simplify_null_join_keys`, `batch_simplify_null_join_keys`):
// a join that compares with `=` a column known to be `NULL` matches nothing. The join, or its
// optional side, becomes an empty relation, which is then propagated up.
static STREAM_SIMPLIFY_NULL_JOIN_KEYS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify Null Join Keys For Stream",
        vec![
            NullKeyJoinRule::create_for_stream(),
            EmptyRelationPropagateRule::create_for_stream(),
            EmptyInputSimplifyRule::create_for_stream(),
        ],
        // Bottom-up so that an empty relation is propagated to the top in one pass.
        ApplyOrder::BottomUp,
    )
});

static BATCH_SIMPLIFY_NULL_JOIN_KEYS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify Null Join Keys For Batch",
        vec![
            NullKeyJoinRule::create_for_batch(),
            EmptyRelationPropagateRule::create(),
            EmptyInputSimplifyRule::create(),
        ],
        // Bottom-up so that an empty relation is propagated to the top in one pass.
        ApplyOrder::BottomUp,
    )
});

// Rules `NarrowValueAggRule` and `NarrowValuePushRule` (`streaming_narrow_value_aggregates`,
// `batch_narrow_value_aggregates`): the functions that read the result of `first_value` /
// `last_value` are aggregated instead of the whole value, and moved into the branches of a union
// and into the side of a join that the value comes from.
static NARROW_VALUE_AGGREGATES: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Narrow Value Aggregates",
        vec![NarrowValueAggRule::create(), NarrowValuePushRule::create()],
        ApplyOrder::TopDown,
    )
});

static NARROW_VALUE_MERGE_PROJECTS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Narrow Value Merge Projects",
        vec![NarrowValueProjectMergeRule::create()],
        ApplyOrder::TopDown,
    )
});

static STREAM_FACTOR_UNION_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Factor Union Join For Stream",
        vec![CommonJoinOverUnionRule::create()],
        // Bottom-up: the branches of a union are reduced before the union is looked at.
        ApplyOrder::BottomUp,
    )
});

static STREAM_MERGE_REPEATED_LOOKUPS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Repeated Lookups For Stream",
        vec![RepeatedLookupMergeRule::create()],
        // Bottom-up: a lookup merged below is carried by the input of the lookups above.
        ApplyOrder::BottomUp,
    )
});

// The default branch of a top-1 over a union, the unused joins below an operator that does not
// see duplicates (`streaming_merge_lookups_under_dedup`).
static STREAM_TOP_ONE_DEFAULT_BRANCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Remove Default Branch Of Top One For Stream",
        vec![
            TopOneDefaultBranchRule::create(),
            UnusedJoinUnderDedupRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static STREAM_SIMPLIFY_ON_UNIQUE_KEY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify On Unique Key For Stream",
        vec![
            UnusedOuterJoinEliminateRule::create(),
            AggOnKeyEliminateRule::create(),
        ],
        // Bottom-up: an operator removed below may give a key to the input of an operator above.
        ApplyOrder::BottomUp,
    )
});

// Rule `TransitivePredicateRule` (`streaming_transitive_predicates`, `batch_transitive_predicates`):
// the predicates that hold on every row of a side of a join are copied to its other side through
// the equalities of the join. Bottom-up, so that a join sees the filters that the rule added below
// it; the caller pushes the new filters down.
static TRANSITIVE_PREDICATES: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Transitive Predicates",
        vec![TransitivePredicateRule::create()],
        ApplyOrder::BottomUp,
    )
});

static LIMIT_PUSH_DOWN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push Down Limit",
        vec![LimitPushDownRule::create()],
        ApplyOrder::TopDown,
    )
});

// `batch_push_down_limit`: rule `LimitUnionJoinPushDownRule`. Top-down: the copy of a limit put
// into a branch is pushed further into the unions and outer joins below it.
static BATCH_PUSH_DOWN_LIMIT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push Down Limit Into Unions And Outer Joins For Batch",
        vec![LimitUnionJoinPushDownRule::create()],
        ApplyOrder::TopDown,
    )
});

static PULL_UP_HOP: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Pull Up Hop",
        vec![PullUpHopRule::create()],
        ApplyOrder::BottomUp,
    )
});

static SET_OPERATION_TO_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Set Operation To Join",
        vec![
            IntersectToSemiJoinRule::create(),
            ExceptToAntiJoinRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static GROUPING_SETS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Grouping Sets",
        vec![
            GroupingSetsToExpandRule::create(),
            ExpandToProjectRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static COMMON_SUB_EXPR_EXTRACT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Common Sub Expression Extract",
        vec![CommonSubExprExtractRule::create()],
        ApplyOrder::TopDown,
    )
});

static LOGICAL_FILTER_EXPRESSION_SIMPLIFY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Logical Filter Expression Simplify",
        vec![LogicalFilterExpressionSimplifyRule::create()],
        ApplyOrder::TopDown,
    )
});

static REWRITE_SOURCE_FOR_BATCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Rewrite Source For Batch",
        vec![SourceToKafkaScanRule::create()],
        ApplyOrder::TopDown,
    )
});

static MATERIALIZE_ICEBERG_SCAN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Materialize Iceberg Scan",
        vec![
            // When storage mode is auto, may rewrite Iceberg intermediate scan to Hummock scan based on statistics.
            IcebergEngineStorageSelectionRule::create(),
            // This converts LogicalIcebergIntermediateScan to LogicalIcebergScan with anti-joins
            // for delete files.
            IcebergIntermediateScanRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static ICEBERG_COUNT_STAR: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Iceberg Count Star Optimization",
        vec![IcebergCountStarRule::create()],
        ApplyOrder::BottomUp,
    )
});

static TOP_N_TO_VECTOR_SEARCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "TopN to Vector Search",
        vec![TopNToVectorSearchRule::create()],
        ApplyOrder::BottomUp,
    )
});

static CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_BATCH: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Correlated TopN to Vector Search",
            vec![CorrelatedTopNToVectorSearchRule::create(true)],
            ApplyOrder::BottomUp,
        )
    });

static CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_STREAM: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Correlated TopN to Vector Search",
            vec![CorrelatedTopNToVectorSearchRule::create(false)],
            ApplyOrder::BottomUp,
        )
    });

// Aggregation pushed below a join (`streaming_push_agg_below_join`, `batch_push_agg_below_join`,
// `batch_partial_agg_below_join`) and into the branches of a `UNION ALL`
// (`batch_partial_agg_into_union`). Top-down: an aggregation pushed below a join is pushed again
// below the join under it.
static STREAM_PUSH_AGG_BELOW_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push Aggregation Below Join For Stream",
        vec![AggJoinTransposeRule::create_for_stream()],
        ApplyOrder::TopDown,
    )
});

static BATCH_PUSH_AGG_BELOW_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push Aggregation Below Join For Batch",
        vec![AggJoinTransposeRule::create_for_batch()],
        ApplyOrder::TopDown,
    )
});

static BATCH_PARTIAL_AGG_BELOW_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Partial Aggregation Below Join For Batch",
        vec![AggJoinTransposeRule::create_partial_for_batch()],
        ApplyOrder::TopDown,
    )
});

static BATCH_PARTIAL_AGG_INTO_UNION: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Partial Aggregation Into Union For Batch",
        vec![AggUnionTransposeRule::create()],
        ApplyOrder::TopDown,
    )
});

static BATCH_MV_SELECTION: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Batch Mv Selection",
        vec![MvSelectionRule::create()],
        ApplyOrder::TopDown,
    )
});

impl LogicalOptimizer {
    pub fn predicate_pushdown(
        plan: LogicalPlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> LogicalPlanRef {
        let plan = plan.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(plan.clone()),
        );
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string());
        }
        plan
    }

    pub fn subquery_unnesting(
        mut plan: LogicalPlanRef,
        enable_share_plan: bool,
        for_stream: bool,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> Result<LogicalPlanRef> {
        // Bail our if no apply operators.
        if !has_logical_apply(plan.clone()) {
            return Ok(plan);
        }
        // Rule `ExistsSubquerySimplifyRule` (`streaming_simplify_exists_subquery`,
        // `batch_simplify_exists_subquery`): before the simple unnesting, so that a correlated
        // `EXISTS (... LIMIT 1)` becomes a semi join on the correlated columns instead of going
        // through the domain and a top-n per correlated value. On demand only.
        let simplify_exists_subquery = if for_stream {
            ctx.session_ctx()
                .config()
                .streaming_simplify_exists_subquery()
                && !ctx.is_stream_prune_baseline()
        } else {
            ctx.session_ctx().config().batch_simplify_exists_subquery()
        };
        if simplify_exists_subquery {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&SIMPLIFY_EXISTS_SUBQUERY)?;
            if for_stream {
                ctx.add_stream_prune_rule_applied(ctx.total_rule_applied() - applied_before);
            }
        }
        // Simple Unnesting.
        plan = plan.optimize_by_rules(&SIMPLE_UNNESTING)?;
        if for_stream
            && ctx
                .session_ctx()
                .config()
                .streaming_simplify_subquery_unnesting()
        {
            // A subquery that aggregates the rows equal to columns of the outer row becomes a left
            // join with the aggregation grouped by those columns, without the domain (the distinct
            // values of the outer columns, read again from the outer side) of the general way.
            plan = plan.optimize_by_rules_until_fix_point(&STREAM_SIMPLE_UNNESTING_AGG)?;
        }
        if for_stream
            && ctx
                .session_ctx()
                .config()
                .streaming_unnest_lateral_functions()
            && !ctx.is_stream_prune_baseline()
        {
            // A subquery that reads no table, only columns of the outer row through set-returning
            // functions, is evaluated on each outer row, without the domain of the general way.
            plan = plan.optimize_by_rules(&STREAM_UNNEST_ARRAY_OF_FUNCTION)?;
            plan = plan.optimize_by_rules_until_fix_point(&STREAM_UNNEST_LATERAL_FUNCTIONS)?;
        }
        // The same two stages for batch queries, each behind its own batch variable. In batch the
        // domain of the general way is read again from the outer side for every query.
        if !for_stream
            && ctx
                .session_ctx()
                .config()
                .batch_simplify_subquery_unnesting()
        {
            plan = plan.optimize_by_rules_until_fix_point(&BATCH_SIMPLE_UNNESTING_AGG)?;
        }
        if !for_stream && ctx.session_ctx().config().batch_unnest_lateral_functions() {
            plan = plan.optimize_by_rules(&BATCH_UNNEST_ARRAY_OF_FUNCTION)?;
            plan = plan.optimize_by_rules_until_fix_point(&BATCH_UNNEST_LATERAL_FUNCTIONS)?;
        }
        debug_assert!(!HasMaxOneRowApply().visit(plan.clone()));
        // Predicate push down before translate apply, because we need to calculate the domain
        // and predicate push down can reduce the size of domain.
        plan = Self::predicate_pushdown(plan, explain_trace, ctx);
        // In order to unnest values with correlated input ref, we need to extract project first.
        plan = plan.optimize_by_rules(&VALUES_EXTRACT_PROJECT)?;
        // General Unnesting.
        // Translate Apply, push Apply down the plan and finally replace Apply with regular inner
        // join.
        plan = if enable_share_plan {
            plan.optimize_by_rules(&GENERAL_UNNESTING_TRANS_APPLY_WITH_SHARE)?
        } else {
            plan.optimize_by_rules(&GENERAL_UNNESTING_TRANS_APPLY_WITHOUT_SHARE)?
        };
        plan = plan.optimize_by_rules_until_fix_point(&GENERAL_UNNESTING_PUSH_DOWN_APPLY)?;

        // Check if all `Apply`s are eliminated and the subquery is unnested.
        plan.check_apply_elimination()?;

        Ok(plan)
    }

    /// Rules `NarrowValueAggRule` and `NarrowValuePushRule` (`streaming_narrow_value_aggregates`,
    /// `batch_narrow_value_aggregates`). Each round moves the narrowing functions one level down:
    /// the whole value that a rewrite leaves unused is pruned, and the projection that it creates
    /// is merged with the one below, before the next round looks at them. Returns the number of
    /// rewrites.
    fn narrow_value_aggregates(
        mut plan: LogicalPlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> Result<(LogicalPlanRef, usize)> {
        let mut total = 0;
        for _ in 0..6 {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&NARROW_VALUE_AGGREGATES)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied == 0 {
                break;
            }
            total += applied;
            ctx.set_values_narrowed();
            plan = Self::column_pruning(plan, explain_trace, ctx);
            plan = plan.optimize_by_rules(&NARROW_VALUE_MERGE_PROJECTS)?;
        }
        Ok((plan, total))
    }

    pub fn column_pruning(
        mut plan: LogicalPlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> LogicalPlanRef {
        let required_cols = (0..plan.schema().len()).collect_vec();
        let mut column_pruning_ctx = ColumnPruningContext::new(plan.clone());
        plan = plan.prune_col(&required_cols, &mut column_pruning_ctx);
        // Column pruning may introduce additional projects, and filter can be pushed again.
        if explain_trace {
            ctx.trace("Prune Columns:");
            ctx.trace(plan.explain_to_string());
        }

        if column_pruning_ctx.need_second_round() {
            // Second round of column pruning and reuse the column pruning context.
            // Try to replace original share operator with the new one.
            plan = plan.prune_col(&required_cols, &mut column_pruning_ctx);
            if explain_trace {
                ctx.trace("Prune Columns (For DAG):");
                ctx.trace(plan.explain_to_string());
            }
        }
        plan
    }

    pub fn inline_now_proc_time(plan: LogicalPlanRef, ctx: &OptimizerContextRef) -> LogicalPlanRef {
        // If now() and proctime() are not found, bail out.
        let mut v = NowProcTimeFinder::default();
        plan.visit_exprs_recursive(&mut v);
        if !v.has() {
            return plan;
        }

        let mut v = ctx.session_ctx().pinned_snapshot().inline_now_proc_time();

        let plan = plan.rewrite_exprs_recursive(&mut v);

        if ctx.is_explain_trace() {
            ctx.trace("Inline Now and ProcTime:");
            ctx.trace(plan.explain_to_string());
        }
        plan
    }

    pub fn gen_optimized_logical_plan_for_stream(
        mut plan: LogicalPlanRef,
    ) -> Result<LogicalPlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string());
        }

        // Convert grouping sets at first because other agg rule can't handle grouping sets.
        plan = plan.optimize_by_rules(&GROUPING_SETS)?;
        // Remove nodes with constant output.
        plan = plan.optimize_by_rules(&CONSTANT_OUTPUT_REMOVE)?;
        // Remove project to make common sub-plan sharing easier.
        plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;

        // If share plan is disable, we need to remove all the share operator generated by the
        // binder, e.g. CTE and View. However, we still need to share source to ensure self
        // source join can return correct result.
        let enable_share_plan = ctx.session_ctx().config().enable_share_plan();
        if enable_share_plan {
            // Common sub-plan sharing.
            plan = plan.common_subplan_sharing();
            plan = plan.prune_share();
            if explain_trace {
                ctx.trace("Common Sub-plan Sharing:");
                ctx.trace(plan.explain_to_string());
            }
        } else {
            plan = plan.optimize_by_rules(&DAG_TO_TREE)?;

            // Replace source to share source.
            // Perform share source at the beginning so that we can benefit from predicate pushdown
            // and column pruning for the share operator.
            plan = ShareSourceRewriter::share_source(plan);
            if explain_trace {
                ctx.trace("Share Source:");
                ctx.trace(plan.explain_to_string());
            }
        }
        plan = plan.optimize_by_rules(&SET_OPERATION_MERGE)?;
        // Rule `SetOperationFilterMergeRule` (`streaming_merge_set_operation_filters`): a `UNION`,
        // `INTERSECT` or `EXCEPT` of filters over the same input becomes one filter, before the
        // set operations become joins or aggregations (`SetOperationToCountRule` below). On demand
        // only. A shared scan that only the merged branches read is no longer shared.
        if ctx
            .session_ctx()
            .config()
            .streaming_merge_set_operation_filters()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_MERGE_SET_OPERATION_FILTERS)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                if enable_share_plan {
                    plan = plan.prune_share();
                }
            }
        }
        // Rule `SetOperationToCountRule` (`streaming_set_operation_to_count`): `INTERSECT` and
        // `EXCEPT` become one counting aggregation before they become semi and anti joins. On
        // demand only: the plan of a streaming job is frozen when the job is created.
        if ctx
            .session_ctx()
            .config()
            .streaming_set_operation_to_count()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&SET_OPERATION_TO_COUNT)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
            }
        }
        plan = plan.optimize_by_rules(&SET_OPERATION_TO_JOIN)?;
        // Convert `generate_series` ends with `now()` to a `Now` source. Only for streaming mode.
        // Should be applied before converting table function to project set.
        plan = plan.optimize_by_rules(&STREAM_GENERATE_SERIES_WITH_NOW)?;
        // In order to unnest a table function, we need to convert it into a `project_set` first.
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_CONVERT)?;

        plan = plan.optimize_by_rules(&CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_STREAM)?;

        plan = Self::subquery_unnesting(plan, enable_share_plan, true, explain_trace, &ctx)?;
        if has_logical_max_one_row(plan.clone()) {
            // `MaxOneRow` is currently only used for the runtime check of
            // scalar subqueries, while it's not supported in streaming mode, so
            // we raise a precise error here.
            bail!("Scalar subquery might produce more than one row.");
        }

        // Same to batch plan optimization, this rule shall be applied before
        // predicate push down
        plan = plan.optimize_by_rules(&LOGICAL_FILTER_EXPRESSION_SIMPLIFY)?;

        // Predicate Push-down
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Rule `OuterJoinReduceRule` (`streaming_reduce_outer_joins`): after the predicate pushdown,
        // which leaves right above an outer join the conjuncts that it could not push into it, and
        // before join ordering, which then sees the inner joins. Each round pushes down again the
        // conjuncts that a reduced join lets through, which may reduce the joins below. On demand
        // only: the plan of a streaming job is frozen when the job is created.
        if ctx.session_ctx().config().streaming_reduce_outer_joins()
            && !ctx.is_stream_prune_baseline()
        {
            for _ in 0..3 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&REDUCE_OUTER_JOINS)?;
                let applied = ctx.total_rule_applied() - applied_before;
                if applied == 0 {
                    break;
                }
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `NullSafeEqToEqRule` (`streaming_null_safe_eq_to_eq`): after the predicate pushdown,
        // which puts the filters that prove a column not `NULL` next to the scans, and before join
        // ordering, which then sees plain equi-joins. On demand only: the plan of a streaming job
        // is frozen when the job is created.
        if ctx.session_ctx().config().streaming_null_safe_eq_to_eq()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&NULL_SAFE_EQ_TO_EQ)?;
            ctx.add_stream_prune_rule_applied(ctx.total_rule_applied() - applied_before);
        }

        // Rule `ValuesJoinInlineRule` (`streaming_inline_values_join`): a join with a small
        // relation of constant rows becomes a filter (`IN` list) and a projection (`CASE`) of its
        // other side. Before join ordering, which would take the constant rows as one of its
        // inputs; the new filter is then pushed down. On demand only.
        if ctx.session_ctx().config().streaming_inline_values_join()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Inline Values Join For Stream",
                vec![ValuesJoinInlineRule::create()],
                ApplyOrder::BottomUp,
            ))?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `OrRestrictionDeriveRule` (`streaming_derive_or_restrictions`): an `OR` of the
        // condition of a join that refers to both sides gives each side the `OR` of its own parts,
        // pushed into that side; the condition is kept. On demand only.
        if ctx
            .session_ctx()
            .config()
            .streaming_derive_or_restrictions()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Derive Or Restrictions For Stream",
                vec![OrRestrictionDeriveRule::create()],
                ApplyOrder::TopDown,
            ))?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `OrJoinToUnionRule` (`streaming_or_join_to_union`): a join without equality whose
        // condition is an `OR` of equi-conditions becomes equi-joins on the disjuncts (a `UNION ALL`
        // for inner and left joins, a chain of anti joins for semi and anti joins), instead of a
        // nested-loop join that streaming refuses. After the restrictions derived from the `OR`
        // above, which are then pushed below the shared sides. On demand only.
        if ctx.session_ctx().config().streaming_or_join_to_union()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Or Join To Union For Stream",
                vec![OrJoinToUnionRule::create_for_stream()],
                ApplyOrder::BottomUp,
            ))?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `TransitivePredicateRule` (`streaming_transitive_predicates`): after the first
        // predicate pushdown, which puts the filters of the views next to their scans, and before
        // join ordering. The copied filters are pushed down, then derived further down by the
        // pushdown through the joins below; a second round sees the filters in their scans. On
        // demand only: the plan of a streaming job is frozen when the job is created.
        if ctx.session_ctx().config().streaming_transitive_predicates()
            && !ctx.is_stream_prune_baseline()
        {
            for _ in 0..2 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&TRANSITIVE_PREDICATES)?;
                let applied = ctx.total_rule_applied() - applied_before;
                if applied == 0 {
                    break;
                }
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        if plan.ctx().session_ctx().config().enable_join_ordering() {
            // Merge inner joins and intermediate filters into multijoin
            // This rule assumes that filters have already been pushed down near to
            // their relevant joins.
            plan = plan.optimize_by_rules(&TO_MULTI_JOIN)?;

            // Reorder multijoin into join tree.
            if plan
                .ctx()
                .session_ctx()
                .config()
                .streaming_enable_bushy_join()
            {
                plan = plan.optimize_by_rules(&BUSHY_TREE_JOIN_ORDERING)?;
            } else {
                plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_ORDERING)?;
            }
        }

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // For stream, push down predicates with now into a left-semi join
        plan = plan.optimize_by_rules(&FILTER_WITH_NOW_TO_JOIN)?;

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN)?;

        plan = plan.optimize_by_rules(&SPLIT_OVER_WINDOW)?;
        // Must push down predicates again after split over window so that OverWindow can be
        // optimized to TopN.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        plan = plan.optimize_by_rules(&CONVERT_OVER_WINDOW)?;
        plan = plan.optimize_by_rules(&MERGE_OVER_WINDOW)?;

        let force_split_distinct_agg = ctx.session_ctx().config().force_split_distinct_agg();
        // TODO: better naming of the OptimizationStage
        // Convert distinct aggregates.
        plan = if force_split_distinct_agg {
            plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_BATCH)?
        } else {
            plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_STREAM)?
        };

        // Prune the aggregate calls that nobody reads before `UnifyFirstLastValueRule` packs the
        // `first_value`/`last_value` calls into one row: a field of that row can no longer be
        // pruned, so every packed column would be scanned and kept in the aggregation state.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        // Rules `NarrowValueAggRule` and `NarrowValuePushRule` (`streaming_narrow_value_aggregates`):
        // after the pruning above, so that the result of an aggregate is only read where it is
        // used, and before `UnifyFirstLastValueRule`, which packs the narrowed calls. On demand
        // only: the plan of a streaming job is frozen when the job is created.
        if ctx
            .session_ctx()
            .config()
            .streaming_narrow_value_aggregates()
            && !ctx.is_stream_prune_baseline()
        {
            let (narrowed, applied) = Self::narrow_value_aggregates(plan, explain_trace, &ctx)?;
            plan = narrowed;
            ctx.add_stream_prune_rule_applied(applied);
        }
        plan = plan.optimize_by_rules(&SIMPLIFY_AGG)?;

        plan = plan.optimize_by_rules(&JOIN_COMMUTE)?;

        // Do a final column pruning and predicate pushing down to clean up the plan.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // After the final predicate pushdown, the conjuncts that contradict each other have been
        // merged into the same filter, scan or join. Remove the sub-plans that are proven to be
        // empty, so that their fragments, backfills and states are never created. On demand
        // only: the plan of a streaming job is frozen when the job is created.
        if ctx.session_ctx().config().streaming_prune_empty_relation()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_PRUNE_EMPTY_RELATION)?;
            let applied = ctx.total_rule_applied() - applied_before;
            ctx.add_stream_prune_rule_applied(applied);
            if applied > 0 {
                // A simplified filter or projection may not use a column any more: an aggregate
                // call, or a whole side of a join, can then be pruned.
                plan = Self::column_pruning(plan, explain_trace, &ctx);
            }
        }

        // Rule `NullKeyJoinRule` (`streaming_simplify_null_join_keys`): after the final predicate
        // pushdown, a filter `c IS NULL` on the output of a join is next to the scan of the side
        // that `c` comes from, and a join that compares `c` with `=` matches nothing. On demand
        // only, as above.
        if ctx
            .session_ctx()
            .config()
            .streaming_simplify_null_join_keys()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_SIMPLIFY_NULL_JOIN_KEYS)?;
            let applied = ctx.total_rule_applied() - applied_before;
            ctx.add_stream_prune_rule_applied(applied);
            if applied > 0 {
                // The removed sub-plans may have been the only readers of some columns.
                plan = Self::column_pruning(plan, explain_trace, &ctx);
            }
        }

        // Remove the operators that the unique keys of their inputs make useless: an outer join
        // that is not used and can not multiply rows, an aggregation whose groups hold one row.
        // On demand only, as above. An operator that is removed may leave columns unused, which
        // may in turn make a join below unused: prune the columns and try again.
        if ctx
            .session_ctx()
            .config()
            .streaming_simplify_on_unique_key()
            && !ctx.is_stream_prune_baseline()
        {
            for _ in 0..3 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&STREAM_SIMPLIFY_ON_UNIQUE_KEY)?;
                let applied = ctx.total_rule_applied() - applied_before;
                if applied == 0 {
                    break;
                }
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
            }
        }

        // Remove the joins that look up a row of a table that their other side already carries.
        // After the simplifications above, which remove the lookups that are not used at all.
        // The filters and projections that replace the joins are then pushed down and pruned.
        if ctx
            .session_ctx()
            .config()
            .streaming_merge_repeated_lookups()
            && !ctx.is_stream_prune_baseline()
        {
            // A rewritten join is not looked at again in the same pass: a lookup that it still
            // holds (the second of two in its right side) is merged by the next pass.
            for _ in 0..3 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&STREAM_MERGE_REPEATED_LOOKUPS)?;
                let applied = ctx.total_rule_applied() - applied_before;
                if applied == 0 {
                    break;
                }
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // Remove the default branch of a top-1 per key over a union (a status computed from
        // candidate rows), when the other side of the join that reads it carries the row of the
        // default branch. On demand only.
        if ctx
            .session_ctx()
            .config()
            .streaming_merge_lookups_under_dedup()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_TOP_ONE_DEFAULT_BRANCH)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // Join once, above a union, the table that several of its branches join on the same
        // columns. The per-branch conditions that the rule leaves around the single join are
        // then pushed down and simplified: in a branch, the number of the branch is a constant.
        if ctx.session_ctx().config().streaming_factor_union_join()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_FACTOR_UNION_JOIN)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&STREAM_PRUNE_EMPTY_RELATION)?;
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // A table joined back on its key with an aggregation, grouped by that key, of rows derived
        // from itself (`streaming_unnest_lateral_functions`): the aggregation is evaluated on each
        // row of the table, which is read once.
        if ctx
            .session_ctx()
            .config()
            .streaming_unnest_lateral_functions()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_SELF_AGGREGATE_JOIN)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // Rule `StatusLadderCaseRule`: under a status ladder, the branches that read the same
        // relation become one branch that picks the best of them with a `CASE`. Before the merge
        // of the union scans below, which would otherwise make one copy of each row per branch.
        if ctx.session_ctx().config().streaming_status_ladder_case()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STATUS_LADDER_CASE)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // Rule `UnionScanMergeRule`: read once the table that several branches of a union read,
        // each with its own filter and projection. The filter that the single scan needs is then
        // pushed into it, and the columns that no branch uses are pruned.
        if ctx.session_ctx().config().streaming_merge_union_scans()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_MERGE_UNION_SCANS)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        // Rule `UnionScanShareRule`: read once, in a shared scan, a table that several branches of
        // a union still scan after the merge above. The filters of the branches are then pushed
        // into the shared scan, and the columns that no branch uses are pruned.
        if ctx.session_ctx().config().streaming_share_union_scans()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_SHARE_UNION_SCANS)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        // Rule `AggJoinTransposeRule` (`streaming_push_agg_below_join`): an aggregation over a join
        // whose other side is unique on the join columns is computed before the join, on the
        // aggregated side: the join keeps one row per group instead of one per row. After the
        // simplifications above, which may remove joins, and before the merge of stacked
        // aggregations below, which may merge the moved aggregation with one under it.
        if ctx.session_ctx().config().streaming_push_agg_below_join()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_PUSH_AGG_BELOW_JOIN)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        // Merge an aggregation over another one into a single aggregation of the input of the inner
        // one (`StackedAggMergeRule`), then share the stateful sub-plans that the simplifications
        // above made identical (`share_identical_subplans`): the common sub-plan sharing at the
        // beginning only saw them before predicate pushdown, unnesting and column pruning. Last,
        // because the rules above need a plan per consumer.
        if ctx
            .session_ctx()
            .config()
            .streaming_merge_stacked_aggregations()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_MERGE_STACKED_AGGS)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        if enable_share_plan
            && ctx
                .session_ctx()
                .config()
                .streaming_share_identical_subplans()
            && !ctx.is_stream_prune_baseline()
        {
            // Copies that differ by their columns or by filters that commute with them first:
            // column pruning then narrows each shared core to the columns its consumers read.
            let (shared, merged_copies) = share_widened_subplans(plan);
            plan = shared;
            if merged_copies > 0 {
                ctx.add_stream_prune_rule_applied(merged_copies);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
                if explain_trace {
                    ctx.trace("Share Widened Sub-plans:");
                    ctx.trace(plan.explain_to_string());
                }
            }
            let (shared, new_shares) = share_identical_subplans(plan);
            plan = shared;
            if new_shares > 0 {
                ctx.add_stream_prune_rule_applied(new_shares);
                if explain_trace {
                    ctx.trace("Share Identical Sub-plans:");
                    ctx.trace(plan.explain_to_string());
                }
            }
        }
        // Rule `AggUnionNullBranchRule` (`streaming_prune_null_union_branches`): after the final
        // column pruning, which leaves in the branches of a union only the columns that the
        // aggregation above reads, and before `AggGroupKeyReduceRule`. On demand only. The scans
        // of a removed branch, and the columns that a removed `GROUP BY` read, are then pruned.
        if ctx
            .session_ctx()
            .config()
            .streaming_prune_null_union_branches()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&PRUNE_NULL_UNION_BRANCHES)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        // Rule `AggGroupKeyReduceRule` (`streaming_reduce_group_keys`): after the final column
        // pruning, which leaves in the input of an aggregation only the columns that it reads, so
        // that the functional dependencies of the input are those of the columns read. On demand
        // only. The columns that a replaced aggregate read may no longer be needed.
        if ctx.session_ctx().config().streaming_reduce_group_keys()
            && !ctx.is_stream_prune_baseline()
        {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STREAM_REDUCE_GROUP_KEYS)?;
            let applied = ctx.total_rule_applied() - applied_before;
            if applied > 0 {
                ctx.add_stream_prune_rule_applied(applied);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        crate::optimizer::rule::diagnose_repeated_reads(&plan);

        plan = plan.optimize_by_rules(&CONSTANT_OUTPUT_REMOVE)?;
        plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;

        plan = plan.optimize_by_rules(&COMMON_SUB_EXPR_EXTRACT)?;

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        ctx.may_store_explain_logical(&plan);

        Ok(plan)
    }

    /// The simplifications of the streaming plan reduction that rely on what is known about the
    /// columns and on unique keys, applied to a batch query on demand: `batch_simplify_column_facts`
    /// (`ColumnFactsSimplifyRule` and the rules that remove what a never-true predicate empties),
    /// `batch_simplify_on_unique_key` (`UnusedOuterJoinEliminateRule`, `AggOnKeyEliminateRule`)
    /// and `batch_merge_repeated_lookups` (`RepeatedLookupMergeRule`).
    ///
    /// Applied after subquery unnesting and the first predicate pushdown, before join ordering, so
    /// that the joins that they remove are not ordered. The plan is a tree: the rules never meet a
    /// shared sub-plan. `now()` is already a constant, and a scan in the past is never a lookup.
    fn reduce_batch_on_keys(
        plan: LogicalPlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> Result<LogicalPlanRef> {
        Ok(Self::reduce_batch_on_keys_and_report(plan, explain_trace, ctx)?.0)
    }

    /// [`Self::reduce_batch_on_keys`], also telling whether one of its simplifications applied
    /// (`batch_reduce_keys_after_conversions` stops its rounds when none did).
    fn reduce_batch_on_keys_and_report(
        mut plan: LogicalPlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> Result<(LogicalPlanRef, bool)> {
        let mut reduced = false;
        let (column_facts, unique_key, repeated_lookups) = {
            let config = ctx.session_ctx().config();
            (
                config.batch_simplify_column_facts(),
                config.batch_simplify_on_unique_key(),
                config.batch_merge_repeated_lookups(),
            )
        };
        let mut need_column_pruning = unique_key || repeated_lookups;
        // Rule `NullKeyJoinRule` (`batch_simplify_null_join_keys`): a join that compares with
        // `=` a column that a filter pushed below it requires to be `NULL` matches nothing.
        if ctx.session_ctx().config().batch_simplify_null_join_keys() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&BATCH_SIMPLIFY_NULL_JOIN_KEYS)?;
            need_column_pruning |= ctx.total_rule_applied() != applied_before;
        }
        if column_facts {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&BATCH_SIMPLIFY_COLUMN_FACTS)?;
            // A simplified filter or projection may not use a column any more.
            let applied = ctx.total_rule_applied() != applied_before;
            need_column_pruning |= applied;
            reduced |= applied;
        }
        if !need_column_pruning {
            return Ok((plan, reduced));
        }
        // An outer join is only seen as unused once the columns that nobody reads are pruned, and
        // a lookup only widens the scan that it keeps with the columns that are read.
        plan = Self::column_pruning(plan, explain_trace, ctx);

        // An operator that is removed may leave columns unused, which may in turn make a join
        // below unused: prune the columns and try again.
        if unique_key {
            for _ in 0..3 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&BATCH_SIMPLIFY_ON_UNIQUE_KEY)?;
                if ctx.total_rule_applied() == applied_before {
                    break;
                }
                reduced = true;
                plan = Self::column_pruning(plan, explain_trace, ctx);
            }
        }

        // After the simplifications above, which remove the lookups that are not used at all. A
        // rewritten join is not looked at again in the same pass: a lookup that it still holds is
        // merged by the next pass.
        if repeated_lookups {
            for _ in 0..3 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&BATCH_MERGE_REPEATED_LOOKUPS)?;
                if ctx.total_rule_applied() == applied_before {
                    break;
                }
                reduced = true;
                plan = Self::predicate_pushdown(plan, explain_trace, ctx);
                plan = Self::column_pruning(plan, explain_trace, ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        Ok((plan, reduced))
    }

    /// Whether the plan reads an Iceberg table, which the DataFusion engine may run
    /// (`batch_over_window_to_topn`).
    fn reads_iceberg_table(plan: &LogicalPlanRef) -> bool {
        plan.as_logical_iceberg_intermediate_scan().is_some()
            || plan.inputs().iter().any(Self::reads_iceberg_table)
    }

    pub fn gen_optimized_logical_plan_for_batch(
        mut plan: LogicalPlanRef,
    ) -> Result<LogicalPlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string());
        }

        if ctx.session_ctx().config().enable_mv_selection() {
            let query_relations =
                RelationCollectorVisitor::collect_with(HashSet::new(), plan.clone());
            Self::register_batch_mview_candidates(ctx.session_ctx(), &ctx, &query_relations);
            plan = plan.optimize_by_rules(&BATCH_MV_SELECTION)?;
        }

        // Inline `NOW()` and `PROCTIME()`, only for batch queries.
        plan = Self::inline_now_proc_time(plan, &ctx);

        // Convert the dag back to the tree, because we don't support DAG plan for batch.
        plan = plan.optimize_by_rules(&DAG_TO_TREE)?;

        plan = plan.optimize_by_rules(&REWRITE_SOURCE_FOR_BATCH)?;
        plan = plan.optimize_by_rules(&GROUPING_SETS)?;
        plan = plan.optimize_by_rules(&REWRITE_LIKE_EXPR)?;
        plan = plan.optimize_by_rules(&SET_OPERATION_MERGE)?;
        // Rule `SetOperationFilterMergeRule` (`batch_merge_set_operation_filters`): a set operation
        // of filters over the same input becomes one filter, before the set operations become
        // joins or aggregations. On demand.
        if ctx
            .session_ctx()
            .config()
            .batch_merge_set_operation_filters()
        {
            plan = plan.optimize_by_rules(&BATCH_MERGE_SET_OPERATION_FILTERS)?;
        }
        // Rule `SetOperationToCountRule` (`batch_set_operation_to_count`): `INTERSECT` and `EXCEPT`
        // become one counting aggregation before they become semi and anti joins. On demand.
        if ctx.session_ctx().config().batch_set_operation_to_count() {
            plan = plan.optimize_by_rules(&SET_OPERATION_TO_COUNT)?;
        }
        plan = plan.optimize_by_rules(&SET_OPERATION_TO_JOIN)?;
        plan = plan.optimize_by_rules(&ALWAYS_FALSE_FILTER)?;
        // Table function should be converted into `file_scan` before `project_set`.
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_FILE_SCAN)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_POSTGRES_QUERY)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_MYSQL_QUERY)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_INTERNAL_BACKFILL_PROGRESS)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_INTERNAL_GET_CHANNEL_DELTA_STATS)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_INTERNAL_SOURCE_BACKFILL_PROGRESS)?;
        // In order to unnest a table function, we need to convert it into a `project_set` first.
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_CONVERT)?;

        plan = plan.optimize_by_rules(&CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_BATCH)?;

        plan = Self::subquery_unnesting(plan, false, false, explain_trace, &ctx)?;

        // Filter simplification must be applied before predicate push-down
        // otherwise the filter for some nodes (e.g., `LogicalScan`)
        // may not be properly applied.
        plan = plan.optimize_by_rules(&LOGICAL_FILTER_EXPRESSION_SIMPLIFY)?;

        // Predicate Push-down
        let mut last_total_rule_applied_before_predicate_pushdown = ctx.total_rule_applied();
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Rule `OuterJoinReduceRule` (`batch_reduce_outer_joins`): after the predicate pushdown,
        // which leaves right above an outer join the conjuncts that it could not push into it, and
        // before join ordering, which then sees the inner joins. Each round pushes down again the
        // conjuncts that a reduced join lets through, which may reduce the joins below.
        if ctx.session_ctx().config().batch_reduce_outer_joins() {
            for _ in 0..3 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&REDUCE_OUTER_JOINS)?;
                if ctx.total_rule_applied() == applied_before {
                    break;
                }
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `NullSafeEqToEqRule` (`batch_null_safe_eq_to_eq`): after the predicate pushdown,
        // which puts the filters that prove a column not `NULL` next to the scans, and before the
        // reductions on keys and join ordering, which then see plain equi-joins.
        if ctx.session_ctx().config().batch_null_safe_eq_to_eq() {
            plan = plan.optimize_by_rules(&NULL_SAFE_EQ_TO_EQ)?;
        }

        // Range form of the comparisons of `date_trunc`, `extract(year ...)` and casts to `date`
        // with a constant (`batch_date_range_predicates`): after the predicate pushdown, which
        // puts the conjuncts into the scans, where they become key ranges. The conjuncts are
        // rewritten in place: nothing needs to be pushed again.
        if ctx.session_ctx().config().batch_date_range_predicates() {
            plan = plan.optimize_by_rules(&BATCH_DATE_RANGE_PREDICATES)?;
        }

        // Simplifications with column facts and unique keys, and merge of repeated lookups, on
        // demand (`batch_simplify_column_facts`, `batch_simplify_on_unique_key`,
        // `batch_merge_repeated_lookups`): before join ordering.
        plan = Self::reduce_batch_on_keys(plan, explain_trace, &ctx)?;
        // Batch counterparts of stages of the streaming plan reduction prototype, each behind its
        // own batch variable, before join ordering: the joins of the branches of a union are
        // still as written, and the filters are next to the scans. The filters that a rewrite
        // leaves above are pushed down again, so that join ordering finds them where it expects.
        // - `batch_factor_union_join`: the relation that several branches of a `UNION ALL` join
        //   on the same columns is joined once above the union (`CommonJoinOverUnionRule`).
        // - `batch_unnest_lateral_functions`: a table joined back on its key with an aggregation
        //   of rows derived from itself computes the aggregation on its own rows
        //   (`SelfAggregateJoinRule`).
        // - `batch_merge_union_scans`: the table that several branches of a `UNION ALL` read,
        //   each with its own filter and projection, is read once (`UnionScanMergeRule`).
        for (enabled, stage) in [
            (
                ctx.session_ctx().config().batch_factor_union_join(),
                &BATCH_FACTOR_UNION_JOIN,
            ),
            (
                ctx.session_ctx().config().batch_unnest_lateral_functions(),
                &BATCH_SELF_AGGREGATE_JOIN,
            ),
            (
                ctx.session_ctx().config().batch_merge_union_scans(),
                &BATCH_MERGE_UNION_SCANS,
            ),
        ] {
            if !enabled {
                continue;
            }
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(stage)?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // Rule `ValuesJoinInlineRule` (`batch_inline_values_join`): a join with a small relation
        // of constant rows becomes a filter (`IN` list, scan ranges) and a projection (`CASE`) of
        // its other side, before join ordering. The new filter is then pushed down.
        if ctx.session_ctx().config().batch_inline_values_join() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Inline Values Join For Batch",
                vec![ValuesJoinInlineRule::create()],
                ApplyOrder::BottomUp,
            ))?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `OrRestrictionDeriveRule` (`batch_derive_or_restrictions`): an `OR` of the condition
        // of a join that refers to both sides gives each side the `OR` of its own parts (scan
        // ranges, fewer rows joined), pushed into that side; the condition is kept.
        if ctx.session_ctx().config().batch_derive_or_restrictions() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Derive Or Restrictions For Batch",
                vec![OrRestrictionDeriveRule::create()],
                ApplyOrder::TopDown,
            ))?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `OrJoinToUnionRule` (`batch_or_join_to_union`): a join without equality whose
        // condition is an `OR` of equi-conditions becomes hash joins on the disjuncts (a
        // `UNION ALL` for inner and left joins, a chain of anti joins for semi and anti joins)
        // instead of a nested-loop join, before join ordering.
        if ctx.session_ctx().config().batch_or_join_to_union() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Or Join To Union For Batch",
                vec![OrJoinToUnionRule::create()],
                ApplyOrder::BottomUp,
            ))?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Rule `TransitivePredicateRule` (`batch_transitive_predicates`): before join ordering, so
        // that the estimates of the inputs count the copied filters, as for streaming above.
        if ctx.session_ctx().config().batch_transitive_predicates() {
            for _ in 0..2 {
                let applied_before = ctx.total_rule_applied();
                plan = plan.optimize_by_rules(&TRANSITIVE_PREDICATES)?;
                if ctx.total_rule_applied() == applied_before {
                    break;
                }
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        if plan.ctx().session_ctx().config().enable_join_ordering() {
            // Merge inner joins and intermediate filters into multijoin
            // This rule assumes that filters have already been pushed down near to
            // their relevant joins.
            plan = plan.optimize_by_rules(&TO_MULTI_JOIN)?;

            // Cost-based join ordering (`batch_cost_based_join_ordering`): order the inner joins
            // by the estimated rows of the tables and of the intermediate results. A multi-join
            // without estimates for all its inputs gets the left-deep heuristic order, as below,
            // and a two-way join without estimates is kept as it is.
            if plan
                .ctx()
                .session_ctx()
                .config()
                .batch_cost_based_join_ordering()
            {
                // Semi and anti joins placed by the cost-based ordering
                // (`batch_semi_join_ordering`): pushed into an input of the inner joins below
                // them, or merged as one more inner join, before the inner joins are ordered.
                if plan.ctx().session_ctx().config().batch_semi_join_ordering() {
                    plan = plan.optimize_by_rules(&BATCH_SEMI_JOIN_PLACEMENT)?;
                }
                // The columns that the plan reads, for the widths of the inputs of the joins:
                // the scans still read all the columns of their tables at this point.
                let required = Arc::new(required_output_columns(&plan));
                plan = plan.optimize_by_rules(&OptimizationStage::new(
                    "Cost-Based Join Sides For Batch",
                    vec![CostBasedJoinSidesRule::create(required)],
                    ApplyOrder::TopDown,
                ))?;
                let required = Arc::new(required_output_columns(&plan));
                plan = plan.optimize_by_rules(&OptimizationStage::new(
                    "Cost-Based Join Ordering For Batch",
                    vec![CostBasedJoinOrderingRule::create(required)],
                    ApplyOrder::TopDown,
                ))?;
            }

            // Reorder multijoin into left-deep join tree.
            plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_ORDERING)?;
        }

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        if last_total_rule_applied_before_predicate_pushdown != ctx.total_rule_applied() {
            last_total_rule_applied_before_predicate_pushdown = ctx.total_rule_applied();
            plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        }

        // Aggregations pushed below joins and into the branches of a `UNION ALL`, each behind its
        // own variable: after join ordering, which then orders the joins as written, and before
        // the conversion of distinct aggregates and the column pruning, which apply to the moved
        // aggregations too.
        // - `batch_push_agg_below_join`: exact form of `AggJoinTransposeRule` (the other side is
        //   unique on the join columns; an inner join only when it reads a whole table);
        // - `batch_partial_agg_below_join`: partial form, when the learned group ratio of the
        //   partial aggregation is small (`batch_cardinality_feedback_learning`);
        // - `batch_partial_agg_into_union`: `AggUnionTransposeRule`.
        for (enabled, stage) in [
            (
                ctx.session_ctx().config().batch_push_agg_below_join(),
                &BATCH_PUSH_AGG_BELOW_JOIN,
            ),
            (
                ctx.session_ctx().config().batch_partial_agg_below_join(),
                &BATCH_PARTIAL_AGG_BELOW_JOIN,
            ),
            (
                ctx.session_ctx().config().batch_partial_agg_into_union(),
                &BATCH_PARTIAL_AGG_INTO_UNION,
            ),
        ] {
            if enabled {
                plan = plan.optimize_by_rules(stage)?;
            }
        }

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN)?;

        // `IS NOT NULL` filters on the columns that a join compares with `=`
        // (`batch_join_keys_not_null`): after the join ordering, which would count them in its
        // estimates, and after the calculation of the keys is pushed below the joins, so that
        // the keys are columns. The filters are pushed into the scans.
        if ctx.session_ctx().config().batch_join_keys_not_null() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&BATCH_JOIN_KEYS_NOT_NULL)?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            }
        }

        // Duplicates found with `lag` over a window ordered by the column
        // (`batch_rewrite_lag_duplicates`): after predicate pushdown, which puts the filter
        // `x = lag(x)` right above the window, and before the window functions are split
        // (`SPLIT_OVER_WINDOW`), so that the unused functions sharing the window are dropped with
        // it. No other batch stage rewrites such a window: `OverWindowToAggAndJoinRule` needs
        // windows without order, and the predicate pushdown that follows pushes the new
        // `x IS NOT NULL` into the scan.
        if ctx.session_ctx().config().batch_rewrite_lag_duplicates() {
            // A column pruning since the last predicate pushdown (another reduction of the batch
            // plan may prune early) may have left a projection between the filter and the window:
            // push the filter down again so that it sits right above the window.
            plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
            plan = plan.optimize_by_rules(&BATCH_REWRITE_LAG_DUPLICATES)?;
        }

        plan = plan.optimize_by_rules(&SPLIT_OVER_WINDOW)?;
        // Must push down predicates again after split over window so that OverWindow can be
        // optimized to TopN.
        if last_total_rule_applied_before_predicate_pushdown != ctx.total_rule_applied() {
            last_total_rule_applied_before_predicate_pushdown = ctx.total_rule_applied();
            plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        }
        // Rule `OverWindowToTopNRule` (`batch_over_window_to_topn`): `row_number() <= n` per
        // partition becomes a top-n per group, for small limits only (see the batch variant of
        // the rule). Not for a query that reads an Iceberg table while the DataFusion engine is
        // on: DataFusion cannot run a top-n per group, and the query would fall back to the batch
        // engine.
        let over_window_to_topn = {
            let config = ctx.session_ctx().config();
            config.batch_over_window_to_topn()
                && !(config.enable_datafusion_engine() && Self::reads_iceberg_table(&plan))
        };
        if over_window_to_topn {
            plan = plan.optimize_by_rules(&BATCH_CONVERT_OVER_WINDOW_TO_TOPN)?;
        } else {
            plan = plan.optimize_by_rules(&CONVERT_OVER_WINDOW_FOR_BATCH)?;
        }
        plan = plan.optimize_by_rules(&MERGE_OVER_WINDOW)?;

        // Convert distinct aggregates.
        plan = plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_BATCH)?;

        // Prune the aggregate calls that nobody reads before `UnifyFirstLastValueRule` packs the
        // `first_value`/`last_value` calls into one row: a field of that row can no longer be
        // pruned, so every packed column would be scanned and kept in the aggregation state.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        // Rules `NarrowValueAggRule` and `NarrowValuePushRule` (`batch_narrow_value_aggregates`):
        // after the pruning above and before `UnifyFirstLastValueRule`, as for streaming.
        if ctx.session_ctx().config().batch_narrow_value_aggregates() {
            plan = Self::narrow_value_aggregates(plan, explain_trace, &ctx)?.0;
        }
        plan = plan.optimize_by_rules(&SIMPLIFY_AGG)?;

        // Inner join to semi join on a unique side that is not read, and `DISTINCT` removed
        // under a semi or anti join (`batch_inner_join_to_semi_join`): after the join ordering
        // (the inner join was ordered as such) and the column pruning (the output of a join only
        // holds the columns read above it), before `JOIN_COMMUTE` turns a right semi join into
        // a left one and the sides of the semi joins are chosen by cost.
        if ctx.session_ctx().config().batch_inner_join_to_semi_join() {
            plan = plan.optimize_by_rules(&BATCH_INNER_JOIN_TO_SEMI_JOIN)?;
        }

        plan = plan.optimize_by_rules(&JOIN_COMMUTE)?;

        // Cost-based sides of the outer, semi and anti joins (`batch_cost_based_join_ordering`):
        // after the column pruning, which leaves in the inputs only the columns that are read,
        // and after `JOIN_COMMUTE`, which would turn a right join back into a left one.
        if ctx.session_ctx().config().batch_cost_based_join_ordering() {
            plan = plan.optimize_by_rules(&BATCH_COST_BASED_OUTER_JOIN_SIDES)?;
        }

        plan = plan.optimize_by_rules(&TOP_N_TO_VECTOR_SEARCH)?;

        // Do a final column pruning and predicate pushing down to clean up the plan.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        if last_total_rule_applied_before_predicate_pushdown != ctx.total_rule_applied() {
            let _ = ctx.total_rule_applied();
            plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        }

        // Second pass of the reductions on keys (`batch_reduce_keys_after_conversions`): the lag
        // rewrite, the window conversions (and their top-n) and the conversion of `UNION` and of
        // distinct aggregates ran after the first pass and created aggregations and top-n that it
        // never saw. Here every set operation is a `UNION ALL` (the others became aggregations or
        // joins), so the column pruning of the pass cannot change what a set operation removes.
        // A second round catches what the first one exposes (an aggregation removed leaves a left
        // join unused, a merged lookup gives a key to the aggregation above it); the bound keeps
        // the planning time in check.
        if ctx
            .session_ctx()
            .config()
            .batch_reduce_keys_after_conversions()
        {
            for _ in 0..2 {
                let (reduced_plan, reduced) =
                    Self::reduce_batch_on_keys_and_report(plan, explain_trace, &ctx)?;
                plan = reduced_plan;
                if !reduced {
                    break;
                }
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // After the final predicate pushdown, contradictory conjuncts like
        // `x IS NULL AND x IN (...)` have been merged into the same filter or scan. Remove the
        // sub-plans that are proven to be empty. Only for batch queries.
        plan = plan.optimize_by_rules(&BATCH_PRUNE_EMPTY_RELATION)?;

        // Rule `StatusLadderCaseRule`: under a status ladder, the branches that read the same
        // relation become one branch that picks the best of them with a `CASE`. On demand.
        if ctx.session_ctx().config().batch_status_ladder_case() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&STATUS_LADDER_CASE)?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
                plan = Self::column_pruning(plan, explain_trace, &ctx);
            }
        }

        // Rule `AggUnionNullBranchRule` (`batch_prune_null_union_branches`): after the final column
        // pruning, as for streaming. On demand.
        if ctx.session_ctx().config().batch_prune_null_union_branches() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&PRUNE_NULL_UNION_BRANCHES)?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }
        // Rule `AggGroupKeyReduceRule` (`batch_reduce_group_keys`): after the final column pruning,
        // as for streaming. On demand.
        if ctx.session_ctx().config().batch_reduce_group_keys() {
            let applied_before = ctx.total_rule_applied();
            plan = plan.optimize_by_rules(&BATCH_REDUCE_GROUP_KEYS)?;
            if ctx.total_rule_applied() != applied_before {
                plan = Self::column_pruning(plan, explain_trace, &ctx);
                plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;
            }
        }

        // Materialize Iceberg intermediate scans after predicate pushdown and column pruning.
        plan = plan.optimize_by_rules(&MATERIALIZE_ICEBERG_SCAN)?;

        plan = plan.optimize_by_rules(&CONSTANT_OUTPUT_REMOVE)?;
        plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;

        plan = plan.optimize_by_rules(&COMMON_SUB_EXPR_EXTRACT)?;

        // This need to be apply after PROJECT_REMOVE to ensure there is no projection between agg and iceberg scan.
        plan = plan.optimize_by_rules(&ICEBERG_COUNT_STAR)?;

        plan = plan.optimize_by_rules(&PULL_UP_HOP)?;

        // Rule `LimitUnionJoinPushDownRule` (`batch_push_down_limit`): a copy of a limit or top-n
        // goes into each branch of a `UNION ALL` and into the preserved side of an outer join
        // below it. After the last predicate pushdown, which has moved into the branches the
        // filters that the limit could not cross, and after the column pruning (only projections,
        // which the rule sees through, are left between them). Before `TOP_N_AGG_ON_INDEX`, so
        // that a top-n put on a scan can read an index ordered by its key, and before
        // `LIMIT_PUSH_DOWN` and the batch rule that puts a limit into a scan.
        if ctx.session_ctx().config().batch_push_down_limit() {
            plan = plan.optimize_by_rules(&BATCH_PUSH_DOWN_LIMIT)?;
        }

        plan = plan.optimize_by_rules(&TOP_N_AGG_ON_INDEX)?;

        plan = plan.optimize_by_rules(&PROJECT_TOP_N_TRANSPOSE)?;

        plan = plan.optimize_by_rules(&LIMIT_PUSH_DOWN)?;

        plan = plan.optimize_by_rules(&DAG_TO_TREE)?;

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        ctx.may_store_explain_logical(&plan);

        Ok(plan)
    }

    fn register_batch_mview_candidates(
        session: &SessionImpl,
        context: &OptimizerContextRef,
        query_relations: &HashSet<ObjectId>,
    ) {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let user_reader = session.env().user_info_reader().read_guard();
        let Some(current_user) = user_reader.get_user_by_name(&session.user_name()) else {
            return;
        };
        let mut mv_dependencies: HashMap<ObjectId, HashSet<ObjectId>> = HashMap::new();
        let mut mviews_with_source_dependency: HashSet<ObjectId> = HashSet::new();
        for dep in catalog_reader.iter_object_dependencies() {
            mv_dependencies
                .entry(dep.object_id)
                .or_default()
                .insert(dep.referenced_object_id);
            if dep.referenced_object_type == PbObjectType::Source {
                mviews_with_source_dependency.insert(dep.object_id);
            }
        }
        let db_name = session.database();
        let Ok(schemas) = catalog_reader.iter_schemas(&db_name) else {
            return;
        };

        for schema in schemas {
            for mv in schema.iter_created_mvs_with_acl(current_user) {
                let mv_object_id = mv.id().as_object_id();
                if mviews_with_source_dependency.contains(&mv_object_id) {
                    continue;
                }
                let is_subset = mv_dependencies
                    .get(&mv_object_id)
                    .is_some_and(|deps| deps.is_subset(query_relations));
                if !is_subset {
                    continue;
                }
                let Ok(stmt) = mv.create_sql_ast() else {
                    continue;
                };
                let Statement::CreateView {
                    materialized: true,
                    query,
                    ..
                } = stmt
                else {
                    continue;
                };
                let mut binder = Binder::new_for_batch(session);
                let Ok(bound_query) = binder.bind_query(&query) else {
                    continue;
                };
                let mut planner = Planner::new_for_batch_dql(context.clone());
                let Ok(plan_root) = planner.plan_query(bound_query) else {
                    continue;
                };
                context.add_batch_mview_candidate(mv.clone(), plan_root.plan.clone());
            }
        }
    }
}
