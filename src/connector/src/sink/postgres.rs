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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use phf::phf_set;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{Row, RowExt};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use simd_json::prelude::ArrayTrait;
use thiserror_ext::AsReport;
use tokio_postgres::types::{Kind as PgKind, Type as PgType};

use super::{
    LogSinker, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkLogReader,
};
use crate::connector_common::{
    PgConnectionConfig, PostgresExternalTable, SslMode, TcpKeepaliveConfig, create_pg_client,
};
use crate::enforce_secret::EnforceSecret;
use crate::parser::scalar_adapter::{ScalarAdapter, validate_pg_type_to_rw_type};
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::postgres_pool::{DEFAULT_POOL_MAX_SIZE, PgPool, PgPooledClient};
use crate::sink::sink_group::{SINK_ORDER_LAST, SinkGroupMember, SinkGroupRegistry};
use crate::sink::{Result, Sink, SinkParam, SinkWriterParam};

pub const POSTGRES_SINK: &str = "postgres";

/// Number of times a chunk is written again after a connection failure before the sink fails.
const MAX_RECONNECT_RETRIES: u32 = 5;
const RECONNECT_BASE_BACKOFF: Duration = Duration::from_millis(200);

const CHECK_FOREIGN_KEY_SQL: &str = r#"
    SELECT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1
          AND t.relname = $2
          AND c.contype = 'f'
    )
"#;

const CHECK_PRIMARY_KEY_SQL: &str = r#"
    SELECT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1
          AND t.relname = $2
          AND i.indisprimary
    )
"#;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    #[serde_as(as = "DisplayFromStr")]
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "Default::default")]
    pub ssl_mode: SslMode,
    #[serde(rename = "ssl.root.cert")]
    pub ssl_root_cert: Option<String>,
    #[serde(default = "default_max_batch_rows")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,
    pub r#type: String, // accept "append-only" or "upsert"
    #[serde(default, rename = "tcp.keepalive.enable")]
    #[serde_as(as = "DisplayFromStr")]
    pub tcp_keepalive_enable: bool,

    #[serde(flatten)]
    pub tcp_keepalive: Option<TcpKeepaliveConfig>,

    /// The group of the sink, set by the frontend from the connection of the sink. The sinks of
    /// a group share a connection pool and can be ordered with `sink_order`.
    #[serde(default)]
    pub sink_group: Option<String>,
    /// `last`: write the data of an epoch after the other sinks of the group have committed it.
    #[serde(default)]
    pub sink_order: Option<String>,
    /// Maximum number of connections of the pool shared by the sinks of the group.
    #[serde(default, rename = "pool.max_size")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub pool_max_size: Option<usize>,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(PostgresConfig);

impl EnforceSecret for PostgresConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf_set! {
        "password", "ssl.root.cert"
    };
}

fn default_max_batch_rows() -> usize {
    1024
}

fn default_schema() -> String {
    "public".to_owned()
}

fn tcp_keepalive_from_config(config: &PostgresConfig) -> Option<TcpKeepaliveConfig> {
    if config.tcp_keepalive_enable {
        config
            .tcp_keepalive
            .clone()
            .or_else(|| Some(TcpKeepaliveConfig::default()))
    } else {
        None
    }
}

async fn ensure_no_foreign_key(config: &PostgresConfig) -> Result<()> {
    let pg_conn = config.pg_connection_config();
    let client = create_pg_client(&pg_conn, tcp_keepalive_from_config(config)).await?;

    ensure_no_foreign_key_with_client(&client, &config.schema, &config.table).await
}

async fn ensure_no_foreign_key_with_client(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<()> {
    let has_foreign_key = client
        .query_one(CHECK_FOREIGN_KEY_SQL, &[&schema, &table])
        .await
        .context("failed to check foreign key constraints")?
        .get::<_, bool>(0);

    if has_foreign_key {
        return Err(SinkError::Config(anyhow!(
            "Postgres sink does not support target table \"{}\".\"{}\" with foreign key constraints. Please remove foreign key constraints from the target table or choose a different sink table.",
            schema,
            table,
        )));
    }

    Ok(())
}

async fn ensure_primary_key_with_client(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<()> {
    let has_primary_key = client
        .query_one(CHECK_PRIMARY_KEY_SQL, &[&schema, &table])
        .await
        .context("failed to check the primary key")?
        .get::<_, bool>(0);

    if !has_primary_key {
        return Err(SinkError::Config(anyhow!(
            "Postgres table should define the primary key for non-append-only tables"
        )));
    }

    Ok(())
}

/// The type that decides how the values of a column are encoded: the base type of a domain, the
/// type itself otherwise.
fn encoding_type(ty: &PgType) -> PgType {
    match ty.kind() {
        PgKind::Domain(base) => encoding_type(base),
        _ => ty.clone(),
    }
}

impl PostgresConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<PostgresConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        if let Some(order) = &config.sink_order {
            if order != SINK_ORDER_LAST {
                return Err(SinkError::Config(anyhow!(
                    "`sink_order` must be `{}`",
                    SINK_ORDER_LAST
                )));
            }
            if config.sink_group.is_none() {
                return Err(SinkError::Config(anyhow!(
                    "`sink_order` requires the sink to use a connection (`connection = <name>`)"
                )));
            }
        }
        if config.pool_max_size == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`pool.max_size` must be greater than 0"
            )));
        }
        Ok(config)
    }

    pub fn is_last_in_group(&self) -> bool {
        self.sink_order.as_deref() == Some(SINK_ORDER_LAST)
    }

    pub fn pg_connection_config(&self) -> PgConnectionConfig {
        PgConnectionConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            ssl_mode: self.ssl_mode.clone(),
            ssl_root_cert: self.ssl_root_cert.clone(),
        }
    }
}

#[derive(Debug)]
pub struct PostgresSink {
    pub config: PostgresConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl PostgresSink {
    pub fn new(
        config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl EnforceSecret for PostgresSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            PostgresConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for PostgresSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = PostgresConfig::from_btreemap(param.properties)?;
        PostgresSink::new(config, schema, pk_indices, param.sink_type.is_append_only())
    }
}

impl Sink for PostgresSink {
    type LogSinker = PostgresSinkWriter;

    const SINK_NAME: &'static str = POSTGRES_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert Postgres sink (please define in `primary_key` field)"
            )));
        }

        ensure_no_foreign_key(&self.config).await?;

        // Verify our sink schema is compatible with Postgres
        {
            let pg_conn = self.config.pg_connection_config();
            let pg_table = PostgresExternalTable::connect(
                &pg_conn,
                &self.config.schema,
                &self.config.table,
                self.is_append_only,
                None,
            )
            .await
            .context(format!(
                "failed to connect to database: {}, schema: {}, table: {}",
                self.config.database, self.config.schema, self.config.table
            ))?;

            // Check that names and types match, order of columns doesn't matter.
            {
                let pg_columns = pg_table.column_descs();
                let sink_columns = self.schema.fields();
                if pg_columns.len() < sink_columns.len() {
                    return Err(SinkError::Config(anyhow!(
                        "Column count mismatch: Postgres table has {} columns, but sink schema has {} columns, sink should have less or equal columns to the Postgres table",
                        pg_columns.len(),
                        sink_columns.len()
                    )));
                }

                let pg_columns_lookup = pg_columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect::<BTreeMap<_, _>>();
                for sink_column in sink_columns {
                    let pg_column = pg_columns_lookup.get(&sink_column.name);
                    match pg_column {
                        None => {
                            return Err(SinkError::Config(anyhow!(
                                "Column `{}` not found in Postgres table `{}`",
                                sink_column.name,
                                self.config.table
                            )));
                        }
                        Some(pg_column) => {
                            if !validate_pg_type_to_rw_type(pg_column, &sink_column.data_type()) {
                                return Err(SinkError::Config(anyhow!(
                                    "Column `{}` in Postgres table `{}` has type `{}`, but sink schema defines it as type `{}`",
                                    sink_column.name,
                                    self.config.table,
                                    pg_column,
                                    sink_column.data_type()
                                )));
                            }
                        }
                    }
                }
            }

            // check that pk matches
            {
                let pg_pk_names = pg_table.pk_names();
                let sink_pk_names = self
                    .pk_indices
                    .iter()
                    .map(|i| &self.schema.fields()[*i].name)
                    .collect::<HashSet<_>>();
                if pg_pk_names.len() != sink_pk_names.len() {
                    return Err(SinkError::Config(anyhow!(
                        "Primary key mismatch: Postgres table has primary key on columns {:?}, but sink schema defines primary key on columns {:?}",
                        pg_pk_names,
                        sink_pk_names
                    )));
                }
                for name in pg_pk_names {
                    if !sink_pk_names.contains(name) {
                        return Err(SinkError::Config(anyhow!(
                            "Primary key mismatch: Postgres table has primary key on column `{}`, but sink schema does not define it as a primary key",
                            name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        PostgresSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
            &writer_param,
        )
        .await
    }
}

pub struct PostgresSinkWriter {
    is_append_only: bool,
    /// Connections are taken from the pool for each chunk. The pool is shared with the other
    /// sinks of the group, if any.
    pool: Arc<PgPool>,
    /// Ordering with the other sinks of the group, if any.
    group: Option<SinkGroupMember>,
    pk_indices: Vec<usize>,
    pk_types: Vec<PgType>,
    schema_types: Vec<PgType>,
    raw_insert_sql: Arc<String>,
    raw_upsert_sql: Arc<String>,
    raw_delete_sql: Arc<String>,
}

impl PostgresSinkWriter {
    async fn new(
        config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        writer_param: &SinkWriterParam,
    ) -> Result<Self> {
        let tcp_keepalive = tcp_keepalive_from_config(&config);

        let pg_conn = config.pg_connection_config();
        let pool = match &config.sink_group {
            Some(group) => PgPool::shared(
                group,
                pg_conn,
                tcp_keepalive,
                config.pool_max_size.unwrap_or(DEFAULT_POOL_MAX_SIZE),
            ),
            None => PgPool::new(pg_conn, tcp_keepalive, 1),
        };
        let group = config.sink_group.as_ref().map(|group| {
            SinkGroupRegistry::global().join(
                group,
                writer_param.sink_id,
                writer_param.actor_id,
                config.is_last_in_group(),
            )
        });

        let pk_indices_lookup = pk_indices.iter().copied().collect::<HashSet<_>>();

        let raw_insert_sql = create_insert_sql(&schema, &config.schema, &config.table);
        let raw_upsert_sql = create_upsert_sql(
            &schema,
            &config.schema,
            &config.table,
            &pk_indices,
            &pk_indices_lookup,
        );
        let raw_delete_sql = create_delete_sql(&schema, &config.schema, &config.table, &pk_indices);

        // Check the target table and the statements once, to fail early. This runs on a
        // connection of the pool: creating a sink opens no other connection.
        let schema_types = {
            let mut client = pool.acquire().await?;
            ensure_no_foreign_key_with_client(&client, &config.schema, &config.table).await?;
            if !is_append_only {
                ensure_primary_key_with_client(&client, &config.schema, &config.table).await?;
            }
            let insert_sql = client.prepare_cached(&raw_insert_sql).await?;
            client.prepare_cached(&raw_upsert_sql).await?;
            client.prepare_cached(&raw_delete_sql).await?;
            // The parameters of the insert statement are the columns of the sink, in order:
            // Postgres gives their types, with no schema discovery query.
            insert_sql.params().iter().map(encoding_type).collect_vec()
        };
        if schema_types.len() != schema.fields.len() {
            return Err(SinkError::Postgres(anyhow!(
                "the insert statement has {} parameters, but the sink has {} columns",
                schema_types.len(),
                schema.fields.len()
            )));
        }
        let pk_types = pk_indices
            .iter()
            .map(|i| schema_types[*i].clone())
            .collect_vec();

        let writer = Self {
            is_append_only,
            pool,
            group,
            pk_indices,
            pk_types,
            schema_types,
            raw_insert_sql: Arc::new(raw_insert_sql),
            raw_upsert_sql: Arc::new(raw_upsert_sql),
            raw_delete_sql: Arc::new(raw_delete_sql),
        };
        Ok(writer)
    }

    /// Writes a chunk in one transaction. After a connection failure, the chunk is written again
    /// on another connection: the transaction of the failed attempt is rolled back by Postgres,
    /// unless the failure happened while committing, in which case the chunk may be written twice
    /// (the sink is at-least-once).
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut attempt = 0;
        loop {
            let (result, connection_failed) = match self.pool.acquire().await {
                Ok(mut client) => {
                    // https://www.postgresql.org/docs/current/limits.html
                    // We have a limit of 65,535 parameters in a single query, as restricted by the PostgreSQL protocol.
                    let result = if self.is_append_only {
                        self.write_batch_append_only(&mut client, &chunk).await
                    } else {
                        self.write_batch_non_append_only(&mut client, &chunk).await
                    };
                    match result {
                        Ok(()) => (Ok(()), false),
                        Err(e) => {
                            let connection_failed = client.is_closed() || is_connection_error(&e);
                            // The state of the connection is unknown: do not reuse it.
                            client.discard();
                            (Err(SinkError::Postgres(e)), connection_failed)
                        }
                    }
                }
                Err(e) => (Err(SinkError::Postgres(e)), true),
            };
            match result {
                Err(e) if connection_failed && attempt < MAX_RECONNECT_RETRIES => {
                    let backoff = RECONNECT_BASE_BACKOFF * 2u32.pow(attempt);
                    attempt += 1;
                    tracing::warn!(
                        error = %e.as_report(),
                        attempt,
                        ?backoff,
                        "postgres sink connection failed, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                }
                result => return result,
            }
        }
    }

    async fn write_batch_append_only(
        &self,
        client: &mut PgPooledClient,
        chunk: &StreamChunk,
    ) -> anyhow::Result<()> {
        let insert_sql = client.prepare_cached(&self.raw_insert_sql).await?;
        let transaction = Arc::new(client.transaction().await?);
        let mut insert_futures = FuturesUnordered::new();
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    let pg_row = convert_row_to_pg_row(row, &self.schema_types);
                    let insert_sql = insert_sql.clone();
                    let raw_insert_sql = self.raw_insert_sql.clone();
                    let transaction = transaction.clone();
                    let future = async move {
                        transaction
                            .execute_raw(&insert_sql, &pg_row)
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to execute insert statement: {}, parameters: {:?}",
                                    raw_insert_sql, pg_row
                                )
                            })
                    };
                    insert_futures.push(future);
                }
                _ => {
                    tracing::error!(
                        "row ignored: an append-only sink should not receive update-insert, update-delete, or delete operations"
                    );
                }
            }
        }

        while let Some(result) = insert_futures.next().await {
            result?;
        }
        drop(insert_futures);
        if let Some(transaction) = Arc::into_inner(transaction) {
            transaction.commit().await?;
        } else {
            tracing::error!("transaction handle was lost");
        }

        Ok(())
    }

    async fn write_batch_non_append_only(
        &self,
        client: &mut PgPooledClient,
        chunk: &StreamChunk,
    ) -> anyhow::Result<()> {
        let delete_sql = client.prepare_cached(&self.raw_delete_sql).await?;
        let upsert_sql = client.prepare_cached(&self.raw_upsert_sql).await?;
        let transaction = Arc::new(client.transaction().await?);
        let mut delete_futures = FuturesUnordered::new();
        let mut upsert_futures = FuturesUnordered::new();
        for (op, row) in chunk.rows() {
            match op {
                Op::Delete | Op::UpdateDelete => {
                    let pg_row =
                        convert_row_to_pg_row(row.project(&self.pk_indices), &self.pk_types);
                    let delete_sql = delete_sql.clone();
                    let raw_delete_sql = self.raw_delete_sql.clone();
                    let transaction = transaction.clone();
                    let future = async move {
                        transaction
                            .execute_raw(&delete_sql, &pg_row)
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to execute delete statement: {}, parameters: {:?}",
                                    raw_delete_sql, pg_row
                                )
                            })
                    };
                    delete_futures.push(future);
                }
                Op::Insert | Op::UpdateInsert => {
                    let pg_row = convert_row_to_pg_row(row, &self.schema_types);
                    let upsert_sql = upsert_sql.clone();
                    let raw_upsert_sql = self.raw_upsert_sql.clone();
                    let transaction = transaction.clone();
                    let future = async move {
                        transaction
                            .execute_raw(&upsert_sql, &pg_row)
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to execute upsert statement: {}, parameters: {:?}",
                                    raw_upsert_sql, pg_row
                                )
                            })
                    };
                    upsert_futures.push(future);
                }
            }
        }
        while let Some(result) = delete_futures.next().await {
            result?;
        }
        while let Some(result) = upsert_futures.next().await {
            result?;
        }
        drop(delete_futures);
        drop(upsert_futures);
        if let Some(transaction) = Arc::into_inner(transaction) {
            transaction.commit().await?;
        } else {
            tracing::error!("transaction handle was lost");
        }
        Ok(())
    }
}

#[async_trait]
impl LogSinker for PostgresSinkWriter {
    async fn consume_log_and_sink(mut self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        loop {
            let (epoch, item) = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    if let Some(group) = &self.group {
                        // All the earlier epochs of this sink are committed. A `last` sink then
                        // waits for the other sinks of the group to commit this epoch.
                        group.start_epoch(epoch);
                        group.wait_others_finished(epoch).await;
                    }
                    self.write_batch(chunk).await?;
                    log_reader.truncate(TruncateOffset::Chunk { epoch, chunk_id })?;
                }
                LogStoreReadItem::Barrier { .. } => {
                    if let Some(group) = &self.group {
                        // Chunks are committed as they are read: the epoch is committed. This
                        // only updates memory, so a sink without data costs nothing here.
                        group.finish_epoch(epoch);
                    }
                    log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                }
            }
        }
    }
}

/// Whether the error comes from the connection (closed, or terminated by the server) rather than
/// from the statement.
fn is_connection_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
        .any(|pg_error| {
            pg_error.is_closed()
                || pg_error.code().is_some_and(|state| {
                    // Class 08: connection exception. Class 57P: operator intervention
                    // (admin shutdown, crash shutdown, cannot connect now).
                    state.code().starts_with("08") || state.code().starts_with("57P")
                })
        })
}

fn create_insert_sql(schema: &Schema, schema_name: &str, table_name: &str) -> String {
    let normalized_table_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );
    let number_of_columns = schema.len();
    let columns: String = schema
        .fields()
        .iter()
        .map(|field| quote_identifier(&field.name))
        .join(", ");
    let column_parameters: String = (0..number_of_columns)
        .map(|i| format!("${}", i + 1))
        .join(", ");
    format!("INSERT INTO {normalized_table_name} ({columns}) VALUES ({column_parameters})")
}

fn create_delete_sql(
    schema: &Schema,
    schema_name: &str,
    table_name: &str,
    pk_indices: &[usize],
) -> String {
    let normalized_table_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );
    let pk_indices = if pk_indices.is_empty() {
        (0..schema.len()).collect_vec()
    } else {
        pk_indices.to_vec()
    };
    let pk = {
        let pk_symbols = pk_indices
            .iter()
            .map(|pk_index| quote_identifier(&schema.fields()[*pk_index].name))
            .join(", ");
        format!("({})", pk_symbols)
    };
    let parameters: String = (0..pk_indices.len())
        .map(|i| format!("${}", i + 1))
        .join(", ");
    format!("DELETE FROM {normalized_table_name} WHERE {pk} in (({parameters}))")
}

fn create_upsert_sql(
    schema: &Schema,
    schema_name: &str,
    table_name: &str,
    pk_indices: &[usize],
    pk_indices_lookup: &HashSet<usize>,
) -> String {
    let insert_sql = create_insert_sql(schema, schema_name, table_name);
    if pk_indices.is_empty() {
        return insert_sql;
    }
    let pk_columns = pk_indices
        .iter()
        .map(|pk_index| quote_identifier(&schema.fields()[*pk_index].name))
        .collect_vec()
        .join(", ");
    let update_parameters: String = (0..schema.len())
        .filter(|i| !pk_indices_lookup.contains(i))
        .map(|i| {
            let column = quote_identifier(&schema.fields()[i].name);
            format!("{column} = EXCLUDED.{column}")
        })
        .collect_vec()
        .join(", ");
    if update_parameters.is_empty() {
        format!("{insert_sql} on conflict ({pk_columns}) do nothing")
    } else {
        format!("{insert_sql} on conflict ({pk_columns}) do update set {update_parameters}")
    }
}

/// Quote an identifier for PostgreSQL.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}

type PgDatum = Option<ScalarAdapter>;
type PgRow = Vec<PgDatum>;

fn convert_row_to_pg_row(row: impl Row, schema_types: &[PgType]) -> PgRow {
    let mut buffer = Vec::with_capacity(row.len());
    for (i, datum_ref) in row.iter().enumerate() {
        let pg_datum = datum_ref.map(|s| {
            match ScalarAdapter::from_scalar(s, &schema_types[i]) {
                Ok(scalar) => Some(scalar),
                Err(e) => {
                    tracing::error!(error=%e.as_report(), scalar=?s, "Failed to convert scalar to pg value");
                    None
                }
            }
        });
        buffer.push(pg_datum.flatten());
    }
    buffer
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use expect_test::{Expect, expect};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;

    fn check(actual: impl Display, expect: Expect) {
        let actual = actual.to_string();
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_create_insert_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let sql = create_insert_sql(&schema, schema_name, table_name);
        check(
            sql,
            expect![[r#"INSERT INTO "test_schema"."test_table" ("a", "b") VALUES ($1, $2)"#]],
        );
    }

    #[test]
    fn test_create_delete_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, schema_name, table_name, &[1]);
        check(
            sql,
            expect![[r#"DELETE FROM "test_schema"."test_table" WHERE ("b") in (($1))"#]],
        );
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, schema_name, table_name, &[0, 1]);
        check(
            sql,
            expect![[r#"DELETE FROM "test_schema"."test_table" WHERE ("a", "b") in (($1, $2))"#]],
        );
    }

    #[test]
    fn test_create_upsert_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let pk_indices_lookup = HashSet::from_iter([1]);
        let sql = create_upsert_sql(&schema, schema_name, table_name, &[1], &pk_indices_lookup);
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("a", "b") VALUES ($1, $2) on conflict ("b") do update set "a" = EXCLUDED."a""#
            ]],
        );
    }

    #[test]
    fn test_create_upsert_sql_all_columns_are_primary_keys() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "user_id".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "client_id".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let pk_indices_lookup = HashSet::from_iter([0, 1]);
        let sql = create_upsert_sql(
            &schema,
            schema_name,
            table_name,
            &[0, 1],
            &pk_indices_lookup,
        );
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("user_id", "client_id") VALUES ($1, $2) on conflict ("user_id", "client_id") do nothing"#
            ]],
        );
    }

    mod live {
        //! Tests against a Postgres server, ignored by default. They need `PGHOST`, `PGPORT` and
        //! `PGUSER` (a superuser with trust authentication):
        //! `cargo test -p risingwave_connector --lib sink::postgres::tests::live -- --ignored`

        use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

        use risingwave_common::catalog::Field;
        use risingwave_common::test_prelude::StreamChunkTestExt;
        use risingwave_common::types::DataType;

        use super::super::*;
        use crate::sink::catalog::SinkId;

        const TEST_DATABASE: &str = "rw_postgres_sink_live_test";

        fn connection_config(database: &str) -> PgConnectionConfig {
            PgConnectionConfig {
                host: std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_owned()),
                port: std::env::var("PGPORT")
                    .ok()
                    .and_then(|port| port.parse().ok())
                    .unwrap_or(5432),
                user: std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_owned()),
                password: String::new(),
                database: database.to_owned(),
                ssl_mode: SslMode::Disabled,
                ssl_root_cert: None,
            }
        }

        fn sink_config(table: &str, sink_type: &str, group: Option<&str>) -> PostgresConfig {
            let conn = connection_config(TEST_DATABASE);
            let mut properties = BTreeMap::from([
                ("host".to_owned(), conn.host),
                ("port".to_owned(), conn.port.to_string()),
                ("user".to_owned(), conn.user),
                ("password".to_owned(), conn.password),
                ("database".to_owned(), conn.database),
                ("schema".to_owned(), "s".to_owned()),
                ("table".to_owned(), table.to_owned()),
                ("type".to_owned(), sink_type.to_owned()),
            ]);
            if let Some(group) = group {
                properties.insert("sink_group".to_owned(), group.to_owned());
                properties.insert("pool.max_size".to_owned(), "2".to_owned());
            }
            PostgresConfig::from_btreemap(properties).unwrap()
        }

        fn fields(fields: &[(&str, DataType)]) -> Schema {
            Schema::new(
                fields
                    .iter()
                    .map(|(name, data_type)| Field {
                        data_type: data_type.clone(),
                        name: (*name).to_owned(),
                    })
                    .collect(),
            )
        }

        fn writer_param(id: u32) -> SinkWriterParam {
            let mut param = SinkWriterParam::for_test();
            param.sink_id = SinkId::new(id);
            param.actor_id = id.into();
            param
        }

        /// Number of server processes serving the test database.
        async fn backends(admin: &tokio_postgres::Client) -> i64 {
            admin
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = $1",
                    &[&TEST_DATABASE],
                )
                .await
                .unwrap()
                .get(0)
        }

        /// Waits for the number of server processes of the test database to be `expected`.
        async fn wait_for_backends(admin: &tokio_postgres::Client, expected: i64) -> i64 {
            let mut count = backends(admin).await;
            for _ in 0..100 {
                if count == expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                count = backends(admin).await;
            }
            count
        }

        #[tokio::test(flavor = "multi_thread")]
        #[ignore]
        async fn test_writer_uses_only_pool_connections() {
            let admin = create_pg_client(&connection_config("postgres"), None)
                .await
                .unwrap();
            // One statement at a time: these cannot run in a transaction.
            for sql in [
                format!("DROP DATABASE IF EXISTS {TEST_DATABASE} WITH (FORCE)"),
                format!("CREATE DATABASE {TEST_DATABASE}"),
            ] {
                admin.batch_execute(&sql).await.unwrap();
            }
            {
                let setup = create_pg_client(&connection_config(TEST_DATABASE), None)
                    .await
                    .unwrap();
                setup
                    .batch_execute(
                        r#"
                        CREATE SCHEMA s;
                        CREATE TYPE s.mood AS ENUM ('sad', 'happy');
                        CREATE DOMAIN s.label AS text;
                        CREATE TABLE s.t (
                            id int PRIMARY KEY,
                            name varchar,
                            u uuid,
                            n numeric,
                            e s.mood,
                            ts timestamptz
                        );
                        CREATE TABLE s.types (id int PRIMARY KEY, tags text[], moods s.mood[], d s.label);
                        CREATE TABLE s.no_pk (id int, name varchar);
                        CREATE TABLE s.parent (id int PRIMARY KEY);
                        CREATE TABLE s.child (id int PRIMARY KEY, parent_id int REFERENCES s.parent (id));
                        "#,
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(wait_for_backends(&admin, 0).await, 0);

            // The types come from the insert statement, for the columns of the sink only.
            {
                let schema = fields(&[
                    ("d", DataType::Varchar),
                    ("moods", DataType::Varchar.list()),
                    ("tags", DataType::Varchar.list()),
                    ("id", DataType::Int32),
                ]);
                let writer = PostgresSinkWriter::new(
                    sink_config("types", "upsert", None),
                    schema,
                    vec![3],
                    false,
                    &writer_param(100),
                )
                .await
                .unwrap();
                assert_eq!(writer.schema_types[0], PgType::TEXT);
                assert!(matches!(
                    writer.schema_types[1].kind(),
                    PgKind::Array(inner) if matches!(inner.kind(), PgKind::Enum(_))
                ));
                assert_eq!(writer.schema_types[2], PgType::TEXT_ARRAY);
                assert_eq!(writer.schema_types[3], PgType::INT4);
                assert_eq!(writer.pk_types, vec![PgType::INT4]);
                // A sink without a group has a pool of one connection.
                assert_eq!(backends(&admin).await, 1);
            }
            assert_eq!(wait_for_backends(&admin, 0).await, 0);

            // The checks of the target table still fail early.
            let simple = fields(&[("id", DataType::Int32), ("name", DataType::Varchar)]);
            let error = PostgresSinkWriter::new(
                sink_config("no_pk", "upsert", None),
                simple.clone(),
                vec![0],
                false,
                &writer_param(101),
            )
            .await
            .err()
            .unwrap();
            assert!(
                error
                    .to_report_string()
                    .contains("should define the primary key"),
                "{}",
                error.to_report_string()
            );
            PostgresSinkWriter::new(
                sink_config("no_pk", "append-only", None),
                simple.clone(),
                vec![],
                true,
                &writer_param(102),
            )
            .await
            .unwrap();
            let error = PostgresSinkWriter::new(
                sink_config("child", "upsert", None),
                fields(&[("id", DataType::Int32), ("parent_id", DataType::Int32)]),
                vec![0],
                false,
                &writer_param(103),
            )
            .await
            .err()
            .unwrap();
            assert!(
                error.to_report_string().contains("foreign key"),
                "{}",
                error.to_report_string()
            );
            let error = PostgresSinkWriter::new(
                sink_config("t", "upsert", None),
                fields(&[("id", DataType::Int32), ("missing", DataType::Varchar)]),
                vec![0],
                false,
                &writer_param(104),
            )
            .await
            .err()
            .unwrap();
            assert!(
                error.to_report_string().contains("missing"),
                "{}",
                error.to_report_string()
            );
            assert_eq!(wait_for_backends(&admin, 0).await, 0);

            // The sinks of a group are created and write with the connections of the pool only.
            let schema = fields(&[
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("u", DataType::Varchar),
                ("n", DataType::Varchar),
                ("e", DataType::Varchar),
                ("ts", DataType::Timestamptz),
            ]);
            let max_backends = Arc::new(AtomicI64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let sampler = {
                let max_backends = max_backends.clone();
                let stop = stop.clone();
                let sampler_client = create_pg_client(&connection_config("postgres"), None)
                    .await
                    .unwrap();
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        max_backends.fetch_max(backends(&sampler_client).await, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
            };
            let mut writers = futures::future::try_join_all((0..8).map(|i| {
                let schema = schema.clone();
                async move {
                    PostgresSinkWriter::new(
                        sink_config("t", "upsert", Some("live-test-group")),
                        schema,
                        vec![0],
                        false,
                        &writer_param(i),
                    )
                    .await
                }
            }))
            .await
            .unwrap();
            assert_eq!(
                writers[0].schema_types[..4],
                [PgType::INT4, PgType::VARCHAR, PgType::UUID, PgType::NUMERIC]
            );
            assert!(matches!(writers[0].schema_types[4].kind(), PgKind::Enum(_)));
            assert_eq!(writers[0].schema_types[5], PgType::TIMESTAMPTZ);

            futures::future::try_join_all(writers.iter_mut().enumerate().map(|(i, writer)| {
                let chunk = StreamChunk::from_pretty(&format!(
                    " i T T T T TZ
                    + {i} a 123e4567-e89b-12d3-a456-426614174000 1.5 sad 2024-01-01T00:00:00Z
                    + {} b 123e4567-e89b-12d3-a456-426614174001 2.5 happy 2024-01-01T00:00:00Z",
                    i + 100
                ));
                async move { writer.write_batch(chunk).await }
            }))
            .await
            .unwrap();
            futures::future::try_join_all(writers.iter_mut().enumerate().map(|(i, writer)| {
                let chunk = StreamChunk::from_pretty(&format!(
                    " i T T T T TZ
                    - {} b 123e4567-e89b-12d3-a456-426614174001 2.5 happy 2024-01-01T00:00:00Z
                    + {i} c 123e4567-e89b-12d3-a456-426614174000 3.5 happy 2024-01-01T00:00:00Z",
                    i + 100
                ));
                async move { writer.write_batch(chunk).await }
            }))
            .await
            .unwrap();

            stop.store(true, Ordering::Relaxed);
            sampler.await.unwrap();
            assert!(
                (1..=2).contains(&max_backends.load(Ordering::Relaxed)),
                "8 sinks of a group with `pool.max_size = 2` used {} connections",
                max_backends.load(Ordering::Relaxed)
            );
            let row = admin_query_one(
                "SELECT count(*), min(name), max(name), sum(n)::text, min(e::text), min(u::text) FROM s.t",
            )
            .await;
            assert_eq!(row.get::<_, i64>(0), 8);
            assert_eq!(row.get::<_, &str>(1), "c");
            assert_eq!(row.get::<_, &str>(2), "c");
            assert_eq!(row.get::<_, &str>(3), "28.0");
            assert_eq!(row.get::<_, &str>(4), "happy");
            assert_eq!(
                row.get::<_, &str>(5),
                "123e4567-e89b-12d3-a456-426614174000"
            );

            // The connections are closed with the last sink of the group.
            drop(writers);
            assert_eq!(wait_for_backends(&admin, 0).await, 0);

            // A statement abandoned by its caller does not leave a connection behind: the
            // connection is closed once the server has finished the statement.
            let pool = PgPool::new(connection_config(TEST_DATABASE), None, 1);
            let abandoned = tokio::time::timeout(Duration::from_millis(300), async {
                let client = pool.acquire().await.unwrap();
                client.batch_execute("SELECT pg_sleep(2)").await.unwrap();
            })
            .await;
            assert!(abandoned.is_err());
            drop(pool);
            assert_eq!(wait_for_backends(&admin, 0).await, 0);

            admin
                .batch_execute(&format!("DROP DATABASE {TEST_DATABASE} WITH (FORCE)"))
                .await
                .unwrap();
        }

        async fn admin_query_one(sql: &str) -> tokio_postgres::Row {
            let client = create_pg_client(&connection_config(TEST_DATABASE), None)
                .await
                .unwrap();
            client.query_one(sql, &[]).await.unwrap()
        }
    }
}
