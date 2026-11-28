// Copyright 2022 RisingWave Labs
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

use std::collections::{BTreeMap, HashSet};

use either::Either;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{FunctionId, ObjectId, SecretId, TableId};
use risingwave_common::license::Feature;
use risingwave_pb::ddl_service::streaming_job_resource_type;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbStreamFragmentGraph, PbStreamNode};
use risingwave_sqlparser::ast::{EmitMode, Ident, ObjectName, Query};
use thiserror_ext::AsReport;

use super::RwPgResponse;
use crate::binder::{Binder, BoundQuery, BoundSetExpr};
use crate::catalog::check_column_name_not_reserved;
use crate::error::ErrorCode::{InvalidInputSyntax, ProtocolError};
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::HandlerArgs;
use crate::handler::util::{
    LongRunningNotificationAction, execute_with_long_running_notification,
    reject_internal_table_dependencies,
};
use crate::optimizer::backfill_order_strategy::plan_backfill_order;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    BackfillType, Explain, StreamPlanRef as PlanRef, ensure_sync_log_store_fragment_root,
};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, RelationCollectorVisitor};
use crate::planner::Planner;
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::{GraphJobType, build_graph_with_strategy};
use crate::utils::{MV_REFRESH_INTERVAL_SEC_KEY, ordinal};
use crate::{TableCatalog, WithOptions};

pub const RESOURCE_GROUP_KEY: &str = "resource_group";
pub const CLOUD_SERVERLESS_BACKFILL_ENABLED: &str = "cloud.serverless_backfill_enabled";

pub(crate) struct StreamingJobResourceOptions {
    pub resource_group: Option<String>,
    pub serverless_backfill_enabled: Option<bool>,
}

pub(crate) fn extract_streaming_job_resource_options(
    with_options: &mut WithOptions,
) -> StreamingJobResourceOptions {
    StreamingJobResourceOptions {
        resource_group: with_options.remove(RESOURCE_GROUP_KEY),
        serverless_backfill_enabled: with_options
            .remove(CLOUD_SERVERLESS_BACKFILL_ENABLED)
            .map(|value| value.parse::<bool>().unwrap_or(false)),
    }
}

pub(super) fn parse_column_names(columns: &[Ident]) -> Option<Vec<String>> {
    if columns.is_empty() {
        None
    } else {
        Some(columns.iter().map(|v| v.real_value()).collect())
    }
}

/// If columns is empty, it means that the user did not specify the column names.
/// In this case, we extract the column names from the query.
/// If columns is not empty, it means that user specify the column names and the user
/// should guarantee that the column names number are consistent with the query.
pub(super) fn get_column_names(
    bound: &BoundQuery,
    columns: Vec<Ident>,
) -> Result<Option<Vec<String>>> {
    let col_names = parse_column_names(&columns);
    if let BoundSetExpr::Select(select) = &bound.body {
        // `InputRef`'s alias will be implicitly assigned in `bind_project`.
        // If user provides columns name (col_names.is_some()), we don't need alias.
        // For other expressions (col_names.is_none()), we require the user to explicitly assign an
        // alias.
        if col_names.is_none() {
            for (i, alias) in select.aliases.iter().enumerate() {
                if alias.is_none() {
                    return Err(ErrorCode::BindError(format!(
                    "An alias must be specified for the {} expression (counting from 1) in result relation", ordinal(i+1)
                ))
                .into());
                }
            }
        }
    }

    Ok(col_names)
}

/// Bind and generate create MV plan, return plan and mv table info.
pub fn explain_create_mv_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    query: Query,
    name: ObjectName,
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
) -> Result<(PlanRef, TableCatalog)> {
    let mut binder = Binder::new_for_stream(session);
    let bound = binder.bind_query(&query)?;
    gen_create_mv_plan_bound(session, context, bound, name, columns, emit_mode, None)
}

/// Generate create MV plan from a bound query
pub fn gen_create_mv_plan_bound(
    session: &SessionImpl,
    context: OptimizerContextRef,
    query: BoundQuery,
    name: ObjectName,
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
    refresh_interval_sec: Option<u64>,
) -> Result<(PlanRef, TableCatalog)> {
    if session.config().create_compaction_group_for_mv() {
        context.warn_to_user("The session variable CREATE_COMPACTION_GROUP_FOR_MV has been deprecated. It will not take effect.");
    }

    // `streaming_preserve_mv_key`: the view is planned once without the plan reductions, to find
    // the distribution key it would have; `StreamMaterialize` keeps it if it is still valid.
    if session.config().streaming_preserve_mv_key() && !context.is_stream_prune_baseline() {
        let baseline: OptimizerContextRef = context.new_stream_prune_baseline().into();
        if let Ok((_, table)) = gen_create_mv_plan_bound(
            session,
            baseline,
            query.clone(),
            name.clone(),
            columns.clone(),
            emit_mode.clone(),
            refresh_interval_sec,
        ) {
            // Only visible columns: the hidden ones may differ between the two plans.
            let names: Option<Vec<String>> = table
                .distribution_key
                .iter()
                .map(|&index| {
                    let column = &table.columns[index];
                    (!column.is_hidden).then(|| column.name().to_owned())
                })
                .collect();
            if let Some(names) = names {
                context.set_preferred_mv_distribution(names);
            }
        }
    }

    let db_name = &session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let definition = context.normalized_sql().to_owned();

    let col_names = get_column_names(&query, columns)?;

    let emit_on_window_close = emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let mut plan_root = Planner::new_for_stream(context).plan_query(query)?;
    if let Some(col_names) = col_names {
        for name in &col_names {
            check_column_name_not_reserved(name)?;
        }
        plan_root.set_out_names(col_names)?;
    }

    let backfill_type = if refresh_interval_sec.is_some() {
        plan_root.require_snapshot_backfill_for_batch_refresh()?;
        BackfillType::SnapshotBackfill
    } else {
        plan_root.derive_backfill_type(true)
    };

    let materialize = plan_root.gen_materialize_plan(
        database_id,
        schema_id,
        table_name,
        definition,
        emit_on_window_close,
        backfill_type,
    )?;

    let mut table = materialize.table().clone();
    table.owner = session.user_id();

    let plan: PlanRef = ensure_sync_log_store_fragment_root(materialize.into());

    let ctx = plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Materialized View:");
        ctx.trace(plan.explain_to_string());
    }

    Ok((plan, table))
}

pub async fn handle_create_mv(
    handler_args: HandlerArgs,
    if_not_exists: bool,
    name: ObjectName,
    query: Query,
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
) -> Result<RwPgResponse> {
    let (dependent_relations, dependent_udfs, dependent_secrets, bound_query) = {
        let mut binder = Binder::new_for_stream(handler_args.session.as_ref());
        let bound_query = binder.bind_query(&query)?;
        (
            binder.included_relations().clone(),
            binder.included_udfs().clone(),
            binder.included_secrets().clone(),
            bound_query,
        )
    };
    handle_create_mv_bound(
        handler_args,
        if_not_exists,
        name,
        bound_query,
        dependent_relations,
        dependent_udfs,
        dependent_secrets,
        columns,
        emit_mode,
    )
    .await
}

pub(crate) fn resolve_streaming_job_resource_type(
    session: &SessionImpl,
    with_options: &mut WithOptions,
) -> Result<streaming_job_resource_type::ResourceType> {
    let StreamingJobResourceOptions {
        resource_group,
        serverless_backfill_enabled,
    } = extract_streaming_job_resource_options(with_options);

    if resource_group.is_some() {
        Feature::ResourceGroup.check_available()?;
    }

    let is_serverless_backfill = match serverless_backfill_enabled {
        Some(value) => value,
        None => {
            if resource_group.is_some() {
                false
            } else {
                session.config().enable_serverless_backfill()
            }
        }
    };

    if resource_group.is_some() && is_serverless_backfill {
        return Err(RwError::from(InvalidInputSyntax(
            "Please do not specify serverless backfilling and resource group together".to_owned(),
        )));
    }

    let resource_type = if is_serverless_backfill {
        assert_eq!(resource_group, None);
        streaming_job_resource_type::ResourceType::ServerlessBackfill(true)
    } else if let Some(group) = resource_group {
        streaming_job_resource_type::ResourceType::SpecificResourceGroup(group)
    } else {
        streaming_job_resource_type::ResourceType::Regular(true)
    };

    Ok(resource_type)
}

fn get_with_options(handler_args: HandlerArgs) -> WithOptions {
    let context = OptimizerContext::from_handler_args(handler_args);
    context.with_options().clone()
}

pub async fn handle_create_mv_bound(
    handler_args: HandlerArgs,
    if_not_exists: bool,
    name: ObjectName,
    query: BoundQuery,
    dependent_relations: HashSet<ObjectId>,
    dependent_udfs: HashSet<FunctionId>, // TODO(rc): merge with `dependent_relations`
    dependent_secrets: HashSet<SecretId>,
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    // Check cluster limits
    session.check_cluster_limits().await?;

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        name.clone(),
        StatementType::CREATE_MATERIALIZED_VIEW,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    reject_internal_table_dependencies(
        session.as_ref(),
        &dependent_relations,
        "CREATE MATERIALIZED VIEW",
    )?;

    let (table, graph, dependencies, resource_type, refresh_interval_sec) = {
        gen_create_mv_graph(
            handler_args,
            name,
            query,
            dependent_relations,
            dependent_udfs,
            dependent_secrets,
            columns,
            emit_mode,
        )?
    };

    // Ensure writes to `StreamJobTracker` are atomic.
    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                table.database_id,
                table.schema_id,
                table.name.clone(),
            ));

    let catalog_writer = session.catalog_writer()?;
    execute_with_long_running_notification(
        catalog_writer.create_materialized_view(
            table.to_prost(),
            graph,
            dependencies,
            resource_type,
            if_not_exists,
            refresh_interval_sec,
        ),
        &session,
        "CREATE MATERIALIZED VIEW",
        LongRunningNotificationAction::MonitorBackfillJob,
    )
    .await?;

    Ok(PgResponse::empty_result(
        StatementType::CREATE_MATERIALIZED_VIEW,
    ))
}

#[expect(clippy::type_complexity)]
pub(crate) fn gen_create_mv_graph(
    handler_args: HandlerArgs,
    name: ObjectName,
    query: BoundQuery,
    dependent_relations: HashSet<ObjectId>,
    dependent_udfs: HashSet<FunctionId>,
    dependent_secrets: HashSet<SecretId>,
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
) -> Result<(
    TableCatalog,
    PbStreamFragmentGraph,
    HashSet<ObjectId>,
    streaming_job_resource_type::ResourceType,
    Option<u64>,
)> {
    let mut with_options = get_with_options(handler_args.clone());
    let refresh_interval_sec = with_options.refresh_interval_sec()?;
    with_options.remove(MV_REFRESH_INTERVAL_SEC_KEY);
    let resource_type =
        resolve_streaming_job_resource_type(handler_args.session.as_ref(), &mut with_options)?;

    if !with_options.is_empty() {
        // get other useful fields by `remove`, the logic here is to reject unknown options.
        return Err(RwError::from(ProtocolError(format!(
            "unexpected options in WITH clause: {:?}",
            with_options.keys()
        ))));
    }

    // Kept to plan the job again without pruning, see `report_stream_prune_savings`.
    let prune_enabled = {
        let config = handler_args.session.config();
        config.streaming_prune_empty_relation()
            || config.streaming_simplify_on_unique_key()
            || config.streaming_merge_repeated_lookups()
            || config.streaming_merge_lookups_under_dedup()
            || config.streaming_factor_union_join()
            || config.streaming_simplify_subquery_unnesting()
            // Rule `UnionScanMergeRule`.
            || config.streaming_merge_union_scans()
            || config.streaming_merge_stacked_aggregations()
            || config.streaming_share_identical_subplans()
            || config.streaming_unnest_lateral_functions()
            // Rule `UnionScanShareRule`.
            || config.streaming_share_union_scans()
            // Rule `StatusLadderCaseRule`.
            || config.streaming_status_ladder_case()
            // Rule `NullKeyJoinRule`.
            || config.streaming_simplify_null_join_keys()
            // Rules `NarrowValueAggRule` and `NarrowValuePushRule`.
            || config.streaming_narrow_value_aggregates()
            // Rule `NullSafeEqToEqRule`.
            || config.streaming_null_safe_eq_to_eq()
            // Rule `ExistsSubquerySimplifyRule`.
            || config.streaming_simplify_exists_subquery()
            // Rule `OuterJoinReduceRule`.
            || config.streaming_reduce_outer_joins()
            // Rule `ValuesJoinInlineRule`.
            || config.streaming_inline_values_join()
            // Rule `OrRestrictionDeriveRule`.
            || config.streaming_derive_or_restrictions()
            // Rule `OrJoinToUnionRule`.
            || config.streaming_or_join_to_union()
            // Rule `TransitivePredicateRule`.
            || config.streaming_transitive_predicates()
            // Rule `AggJoinTransposeRule`.
            || config.streaming_push_agg_below_join()
            // Rule `SetOperationToCountRule`.
            || config.streaming_set_operation_to_count()
            // Rule `SetOperationFilterMergeRule`.
            || config.streaming_merge_set_operation_filters()
            // Rule `AggGroupKeyReduceRule`.
            || config.streaming_reduce_group_keys()
            // Rule `AggUnionNullBranchRule`.
            || config.streaming_prune_null_union_branches()
            // `LogicalUnion::to_stream_with_dist_required`.
            || config.streaming_distributed_union()
            // Rules `NullContradictionRule` and `InListCoalesceSimplifyRule`.
            || config.streaming_fold_null_contradictions()
            || config.streaming_simplify_in_lists_and_coalesce()
    };
    let baseline_args = prune_enabled.then(|| {
        (
            handler_args.clone(),
            query.clone(),
            name.clone(),
            columns.clone(),
            emit_mode.clone(),
        )
    });

    let context = OptimizerContext::from_handler_args(handler_args);
    let has_order_by = !query.order.is_empty();
    if has_order_by {
        context.warn_to_user(r#"The ORDER BY clause in the CREATE MATERIALIZED VIEW statement does not guarantee that the rows selected out of this materialized view is returned in this order.
It only indicates the physical clustering of the data, which may improve the performance of queries issued against this materialized view.
"#.to_owned());
    }

    let context: OptimizerContextRef = context.into();
    let session = context.session_ctx().as_ref();

    let (plan, table) = gen_create_mv_plan_bound(
        session,
        context.clone(),
        query,
        name,
        columns,
        emit_mode,
        refresh_interval_sec,
    )?;

    let backfill_order = plan_backfill_order(
        session,
        context.with_options().backfill_order_strategy(),
        plan.clone(),
    )?;

    // TODO(rc): To be consistent with UDF dependency check, we should collect relation dependencies
    // during binding instead of visiting the optimized plan.
    let dependencies = RelationCollectorVisitor::collect_with(dependent_relations, plan.clone())
        .into_iter()
        .chain(dependent_udfs.iter().copied().map_into())
        .chain(
            dependent_secrets
                .iter()
                .copied()
                .map(|id| id.as_object_id()),
        )
        .collect();

    let graph = build_graph_with_strategy(
        plan,
        Some(GraphJobType::MaterializedView),
        Some(backfill_order),
    )?;

    let rules_applied = context.stream_prune_rule_applied();
    if rules_applied > 0
        && let Some((handler_args, query, name, columns, emit_mode)) = baseline_args
    {
        let baseline = gen_create_mv_graph_without_pruning(
            handler_args,
            query,
            name,
            columns,
            emit_mode,
            refresh_interval_sec,
        );
        report_stream_prune_savings(session, &table.name, rules_applied, &graph, baseline);
    }
    if session.config().streaming_merge_repeated_lookups() {
        report_repeated_table_scans(session, &table.name, &graph);
    }

    Ok((
        table,
        graph,
        dependencies,
        resource_type,
        refresh_interval_sec,
    ))
}

/// Plans a materialized view without the pruning of its plan (`streaming_prune_empty_relation`,
/// `streaming_simplify_on_unique_key`), only to compare its fragment graph with the pruned one.
fn gen_create_mv_graph_without_pruning(
    handler_args: HandlerArgs,
    query: BoundQuery,
    name: ObjectName,
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
    refresh_interval_sec: Option<u64>,
) -> Result<PbStreamFragmentGraph> {
    let context = OptimizerContext::from_handler_args(handler_args);
    context.set_stream_prune_baseline();
    let context: OptimizerContextRef = context.into();
    let session = context.session_ctx().as_ref();
    let (plan, _table) = gen_create_mv_plan_bound(
        session,
        context.clone(),
        query,
        name,
        columns,
        emit_mode,
        refresh_interval_sec,
    )?;
    build_graph_with_strategy(plan, Some(GraphJobType::MaterializedView), None)
}

/// Size of a fragment graph.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FragmentGraphSize {
    pub fragments: usize,
    pub operators: usize,
    /// Scans of an upstream table or materialized view, each with its backfill.
    pub table_scans: usize,
}

impl FragmentGraphSize {
    pub(crate) fn of(graph: &PbStreamFragmentGraph) -> Self {
        fn visit(node: &PbStreamNode, size: &mut FragmentGraphSize) {
            size.operators += 1;
            if matches!(node.node_body, Some(NodeBody::StreamScan(_))) {
                size.table_scans += 1;
            }
            for input in &node.input {
                visit(input, size);
            }
        }
        let mut size = FragmentGraphSize {
            fragments: graph.fragments.len(),
            ..Default::default()
        };
        for fragment in graph.fragments.values() {
            if let Some(node) = &fragment.node {
                visit(node, &mut size);
            }
        }
        size
    }
}

/// Logs, and tells the user, the tables and materialized views that a streaming job still reads
/// several times once its repeated lookups are merged (`streaming_merge_repeated_lookups`): each
/// scan has its own backfill, and usually feeds its own join or aggregation state. These are the
/// shapes that the merge does not handle.
pub(crate) fn report_repeated_table_scans(
    session: &SessionImpl,
    job_name: &str,
    graph: &PbStreamFragmentGraph,
) {
    fn visit(node: &PbStreamNode, scans: &mut BTreeMap<TableId, usize>) {
        if let Some(NodeBody::StreamScan(scan)) = &node.node_body {
            *scans.entry(scan.table_id).or_default() += 1;
        }
        for input in &node.input {
            visit(input, scans);
        }
    }
    let mut scans = BTreeMap::new();
    for fragment in graph.fragments.values() {
        if let Some(node) = &fragment.node {
            visit(node, &mut scans);
        }
    }
    let repeated = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        scans
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(table_id, count)| {
                let name = match catalog_reader.get_any_table_by_id(table_id) {
                    Ok(table) => table.name.clone(),
                    Err(_) => format!("table {table_id}"),
                };
                format!("{name} ({count} scans)")
            })
            .join(", ")
    };
    if repeated.is_empty() {
        return;
    }
    tracing::info!(
        job_name,
        repeated,
        "a streaming job reads the same relations several times"
    );
    session.notice_to_user(format!(
        "streaming plan still reads several times: {repeated}"
    ));
}

/// Logs, and tells the user, what the pruning of the sub-plans proven empty saves for a streaming
/// job: the size of its fragment graph, compared with the graph planned without pruning.
pub(crate) fn report_stream_prune_savings(
    session: &SessionImpl,
    job_name: &str,
    rules_applied: usize,
    pruned: &PbStreamFragmentGraph,
    baseline: Result<PbStreamFragmentGraph>,
) {
    let pruned = FragmentGraphSize::of(pruned);
    match baseline {
        Ok(baseline) => {
            let baseline = FragmentGraphSize::of(&baseline);
            let fragments_saved = baseline.fragments.saturating_sub(pruned.fragments);
            tracing::info!(
                job_name,
                rules_applied,
                fragments = pruned.fragments,
                fragments_without_pruning = baseline.fragments,
                fragments_saved,
                operators = pruned.operators,
                operators_without_pruning = baseline.operators,
                table_scans = pruned.table_scans,
                table_scans_without_pruning = baseline.table_scans,
                "pruned the sub-plans proven empty of a streaming job"
            );
            session.notice_to_user(format!(
                "streaming plan pruning: {} fragment(s) instead of {} ({} saved), \
                 {} operator(s) instead of {}, {} table scan(s) instead of {}",
                pruned.fragments,
                baseline.fragments,
                fragments_saved,
                pruned.operators,
                baseline.operators,
                pruned.table_scans,
                baseline.table_scans,
            ));
        }
        // The plan without pruning may be rejected, e.g. `LIMIT 0`.
        Err(error) => {
            tracing::info!(
                job_name,
                rules_applied,
                fragments = pruned.fragments,
                operators = pruned.operators,
                table_scans = pruned.table_scans,
                error = %error.as_report(),
                "pruned the sub-plans proven empty of a streaming job, whose plan without \
                 pruning is rejected"
            );
            session.notice_to_user(format!(
                "streaming plan pruning: {} fragment(s), {} operator(s), {} table scan(s); \
                 the plan without pruning is rejected",
                pruned.fragments, pruned.operators, pruned.table_scans,
            ));
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW;
    use risingwave_common::catalog::{
        DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, ROW_ID_COLUMN_NAME, RW_TIMESTAMP_COLUMN_NAME,
    };
    use risingwave_common::types::{DataType, StructType};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    #[tokio::test]
    async fn test_create_mv_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kinesis')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1";
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t1")
            .unwrap();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let (table, _) = catalog_reader
            .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let city_type = StructType::new(vec![
            ("address", DataType::Varchar),
            ("zipcode", DataType::Varchar),
        ])
        // .with_ids([5, 6].map(ColumnId::new))
        .into();
        let expected_columns = maplit::hashmap! {
            ROW_ID_COLUMN_NAME => DataType::Serial,
            "country" => StructType::new(
                 vec![("address", DataType::Varchar),("city", city_type),("zipcode", DataType::Varchar)],
            )
            // .with_ids([3, 4, 7].map(ColumnId::new))
            .into(),
            RW_TIMESTAMP_COLUMN_NAME => DataType::Timestamptz,
        };
        assert_eq!(columns, expected_columns, "{columns:#?}");
    }

    /// When creating MV, a unique column name must be specified for each column
    #[tokio::test]
    async fn test_no_alias() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = "create table t(x varchar)";
        frontend.run_sql(sql).await.unwrap();

        // Aggregation without alias is ok.
        let sql = "create materialized view mv0 as select count(x) from t";
        frontend.run_sql(sql).await.unwrap();

        // Same aggregations without alias is forbidden, because it make the same column name.
        let sql = "create materialized view mv1 as select count(x), count(*) from t";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid input syntax: column \"count\" specified more than once"
        );

        // Literal without alias is forbidden.
        let sql = "create materialized view mv1 as select 1";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Bind error: An alias must be specified for the 1st expression (counting from 1) in result relation"
        );

        // some expression without alias is forbidden.
        let sql = "create materialized view mv1 as select x is null from t";
        let err = frontend.run_sql(sql).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Bind error: An alias must be specified for the 1st expression (counting from 1) in result relation"
        );
    }

    /// Creating MV with order by returns a special notice
    #[tokio::test]
    async fn test_create_mv_with_order_by() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = "create table t(x varchar)";
        frontend.run_sql(sql).await.unwrap();

        // Without order by
        let sql = "create materialized view mv1 as select * from t";
        let response = frontend.run_sql(sql).await.unwrap();
        assert_eq!(response.stmt_type(), CREATE_MATERIALIZED_VIEW);
        assert!(response.notices().is_empty());

        // With order by
        let sql = "create materialized view mv2 as select * from t order by x";
        let response = frontend.run_sql(sql).await.unwrap();
        assert_eq!(response.stmt_type(), CREATE_MATERIALIZED_VIEW);
    }
}
