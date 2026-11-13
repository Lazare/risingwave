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

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::{Context, bail};
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior};

use super::pgoutput::{self, LogicalMessage, Lsn, Oid, RelationBody};
use super::protocol::{
    ReplicationConnection, ReplicationMessage, format_lsn, parse_lsn, quote_literal,
};
use super::translator::{Relation, ResumePosition, Translator};
use super::value::{PgValueKind, builtin_value_kind};
use super::{AckRegistration, register_ack_channel};
use crate::connector_common::postgres::{
    PgConnectionConfig, create_pg_client, pg_connection_config_from_properties,
};
use crate::source::base::SourceMessage;
use crate::source::cdc::external::DebeziumOffset;
use crate::source::cdc::{CDC_TRANSACTIONAL_KEY, CDC_WAIT_FOR_STREAMING_START_TIMEOUT};

/// The batches of messages produced by the reader, or the error that terminated it.
pub type NativeCdcReceiver = mpsc::Receiver<anyhow::Result<Vec<SourceMessage>>>;

const CHANNEL_SIZE: usize = 16;
const MAX_BATCH_SIZE: usize = 1024;
/// How often the progress is reported to the upstream. It must be well below the
/// `wal_sender_timeout` of the upstream (60s by default), otherwise the connection is
/// terminated by the upstream.
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_START_TIMEOUT_SECS: u64 = 30;
const DEFAULT_PUBLICATION_NAME: &str = "rw_publication";
const SLOT_NAME_KEY: &str = "slot.name";
const PUBLICATION_NAME_KEY: &str = "publication.name";
/// Same key and default as the Debezium based engine.
const HEARTBEAT_INTERVAL_KEY: &str = "debezium.heartbeat.interval.ms";
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 300_000;
/// SQLSTATE `object_in_use`, raised when the slot is still held by another connection.
const SQLSTATE_OBJECT_IN_USE: &str = " 55006:";

/// Where to start the stream, decoded from the offset persisted in the state of the source.
#[derive(Debug, Default, PartialEq, Eq)]
struct StartPosition {
    /// The end of the last completely consumed transaction. `None` to start from the
    /// position of the slot.
    commit_lsn: Option<Lsn>,
    resume: Option<ResumePosition>,
}

fn parse_start_position(start_offset: Option<&str>) -> anyhow::Result<StartPosition> {
    let Some(offset) = start_offset.filter(|offset| !offset.trim().is_empty()) else {
        return Ok(StartPosition::default());
    };
    let offset: DebeziumOffset = serde_json::from_str(offset)
        .with_context(|| format!("invalid postgres cdc offset: {}", offset))?;
    let source_offset = offset.source_offset;
    let Some(commit_lsn) = source_offset.lsn_commit else {
        return Ok(StartPosition::default());
    };
    let resume = match (source_offset.lsn_proc, source_offset.txid) {
        (Some(lsn_proc), Some(xid)) if lsn_proc > commit_lsn => Some(ResumePosition {
            xid: xid as u32,
            lsn_proc,
        }),
        _ => None,
    };
    Ok(StartPosition {
        commit_lsn: Some(commit_lsn),
        resume,
    })
}

/// Start reading the changes of a shared Postgres CDC source with the native engine.
///
/// The returned channel yields the messages in the same format as the Debezium based engine.
/// The reader stops when the channel is dropped.
pub async fn start_postgres_cdc_stream(
    source_id: u64,
    properties: &BTreeMap<String, String>,
    start_offset: Option<&str>,
) -> anyhow::Result<NativeCdcReceiver> {
    let config = pg_connection_config_from_properties(properties)?;
    let slot = properties
        .get(SLOT_NAME_KEY)
        .with_context(|| format!("missing `{}` in postgres-cdc properties", SLOT_NAME_KEY))?
        .clone();
    let publication = properties
        .get(PUBLICATION_NAME_KEY)
        .cloned()
        .unwrap_or_else(|| DEFAULT_PUBLICATION_NAME.to_owned());
    let transactional = properties
        .get(CDC_TRANSACTIONAL_KEY)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let heartbeat_interval = match properties.get(HEARTBEAT_INTERVAL_KEY) {
        Some(v) => v
            .parse::<u64>()
            .with_context(|| format!("invalid `{}`: {}", HEARTBEAT_INTERVAL_KEY, v))?,
        None => DEFAULT_HEARTBEAT_INTERVAL_MS,
    };
    let start_timeout = properties
        .get(CDC_WAIT_FOR_STREAMING_START_TIMEOUT)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_START_TIMEOUT_SECS);
    let start = parse_start_position(start_offset)?;

    // Registering takes over from a previous reader of the same source, if any, which then
    // closes its connection and releases the slot.
    let (registration, ack_rx) =
        register_ack_channel(source_id, start.commit_lsn.unwrap_or_default());

    let init = async {
        let mut conn =
            ReplicationConnection::connect(&config, &format!("risingwave-cdc-{}", source_id))
                .await?;
        let slot_lsn = prepare_slot(&mut conn, &config, &slot, &publication, &start).await?;
        loop {
            match conn
                .start_replication(&slot, &publication, start.commit_lsn.unwrap_or_default())
                .await
            {
                Ok(()) => break,
                // The slot may be held for a while by the connection of a previous reader.
                Err(e) if format!("{:#}", e).contains(SQLSTATE_OBJECT_IN_USE) => {
                    tracing::info!(
                        source_id,
                        %slot,
                        error = %e.as_report(),
                        "replication slot is in use, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
        anyhow::Ok((conn, slot_lsn))
    };
    let (conn, slot_lsn) = tokio::time::timeout(Duration::from_secs(start_timeout), init)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "failed to start the native CDC reader before the timeout ({}s).\n\
                 HINT: You can increase the timeout by:\n\
                 - Alter the running source: ALTER SOURCE <source_name> CONNECTOR WITH ({} = '<larger_value>');\n\
                 - Set the session variable and recreate the source: SET cdc_source_wait_streaming_start_timeout = <larger_value>;",
                start_timeout,
                CDC_WAIT_FOR_STREAMING_START_TIMEOUT
            )
        })??;

    let commit_position = start.commit_lsn.unwrap_or(slot_lsn);
    tracing::info!(
        source_id,
        %slot,
        %publication,
        start_lsn = %format_lsn(commit_position),
        resume = ?start.resume,
        "native postgres cdc reader started"
    );

    let translator = Translator::new(
        source_id,
        &config.database,
        transactional,
        commit_position,
        start.resume,
    );
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    let task = ReaderTask {
        source_id,
        conn,
        translator,
        resolver: TypeResolver::new(config),
        ack_rx,
        heartbeat_interval: (heartbeat_interval > 0)
            .then(|| Duration::from_millis(heartbeat_interval)),
        _registration: registration,
    };
    tokio::spawn(async move {
        let error_tx = tx.clone();
        if let Err(e) = task.run(tx).await {
            tracing::warn!(source_id, error = %e.as_report(), "native postgres cdc reader failed");
            let _ = error_tx.send(Err(e)).await;
        } else {
            tracing::info!(source_id, "native postgres cdc reader stopped");
        }
    });
    Ok(rx)
}

/// Check the publication and the slot, creating the slot on the first start of the source.
/// Returns the confirmed flush LSN of the slot.
async fn prepare_slot(
    conn: &mut ReplicationConnection,
    config: &PgConnectionConfig,
    slot: &str,
    publication: &str,
    start: &StartPosition,
) -> anyhow::Result<Lsn> {
    let rows = conn
        .simple_query(&format!(
            "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = {}",
            quote_literal(publication)
        ))
        .await?;
    if rows.is_empty() {
        bail!(
            "publication `{}` does not exist in the upstream database `{}`",
            publication,
            config.database
        );
    }

    let rows = conn
        .simple_query(&format!(
            "SELECT plugin, database, confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = {}",
            quote_literal(slot)
        ))
        .await?;
    let lsn = match rows.first() {
        Some(row) => {
            let plugin = row.first().cloned().flatten().unwrap_or_default();
            if plugin != "pgoutput" {
                bail!(
                    "replication slot `{}` uses the plugin `{}`, expect `pgoutput`",
                    slot,
                    plugin
                );
            }
            let database = row.get(1).cloned().flatten().unwrap_or_default();
            if database != config.database {
                bail!(
                    "replication slot `{}` belongs to the database `{}`, expect `{}`",
                    slot,
                    database,
                    config.database
                );
            }
            row.get(2).cloned().flatten()
        }
        None => {
            if let Some(commit_lsn) = start.commit_lsn {
                bail!(
                    "replication slot `{}` does not exist anymore, but the source has consumed the changes up to {}. \
                     The changes after this position are lost, please recreate the source and its tables.",
                    slot,
                    format_lsn(commit_lsn)
                );
            }
            let rows = conn
                .simple_query(&format!(
                    "SELECT lsn FROM pg_catalog.pg_create_logical_replication_slot({}, 'pgoutput')",
                    quote_literal(slot)
                ))
                .await?;
            tracing::info!(%slot, "created the replication slot");
            rows.first().and_then(|row| row.first().cloned().flatten())
        }
    };
    lsn.as_deref().and_then(parse_lsn).with_context(|| {
        format!(
            "failed to get the position of the replication slot `{}`",
            slot
        )
    })
}

struct ReaderTask {
    source_id: u64,
    conn: ReplicationConnection,
    translator: Translator,
    resolver: TypeResolver,
    /// The position that is durably consumed, updated when a checkpoint is committed.
    ack_rx: watch::Receiver<Lsn>,
    heartbeat_interval: Option<Duration>,
    _registration: AckRegistration,
}

enum Event {
    Message(ReplicationMessage),
    StatusTick,
    Acked,
    /// The consumer of the messages is gone, or another reader of the source takes over.
    Stopped,
}

impl ReaderTask {
    async fn run(
        mut self,
        tx: mpsc::Sender<anyhow::Result<Vec<SourceMessage>>>,
    ) -> anyhow::Result<()> {
        let mut status_interval = tokio::time::interval(STATUS_INTERVAL);
        status_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_heartbeat = Instant::now();
        let mut received_lsn: Lsn = 0;
        let mut batch: Vec<SourceMessage> = vec![];

        loop {
            // Hand over the batch when there is nothing more to read without waiting.
            if !batch.is_empty()
                && (batch.len() >= MAX_BATCH_SIZE || !self.conn.has_buffered_message())
            {
                // Keep reporting the progress while waiting for the consumer, which can be
                // paused for long, otherwise the upstream terminates the connection.
                let permit = loop {
                    let event = tokio::select! {
                        permit = tx.reserve() => match permit {
                            Ok(permit) => break permit,
                            Err(_) => Event::Stopped,
                        },
                        _ = status_interval.tick() => Event::StatusTick,
                        changed = self.ack_rx.changed() => match changed {
                            Ok(()) => Event::Acked,
                            Err(_) => Event::Stopped,
                        },
                    };
                    match event {
                        Event::Stopped => return Ok(()),
                        _ => {
                            let flushed = *self.ack_rx.borrow_and_update();
                            self.conn
                                .send_standby_status(received_lsn, flushed, false)
                                .await?;
                        }
                    }
                };
                permit.send(Ok(std::mem::take(&mut batch)));
                continue;
            }

            let event = tokio::select! {
                message = self.conn.next_replication_message() => Event::Message(message?),
                _ = status_interval.tick() => Event::StatusTick,
                changed = self.ack_rx.changed() => match changed {
                    Ok(()) => Event::Acked,
                    Err(_) => Event::Stopped,
                },
                _ = tx.closed() => Event::Stopped,
            };
            match event {
                Event::Stopped => return Ok(()),
                Event::StatusTick => {
                    // Request a reply to learn the current position of the upstream, which
                    // advances the commit position when the captured tables are idle.
                    let flushed = *self.ack_rx.borrow_and_update();
                    self.conn
                        .send_standby_status(received_lsn, flushed, true)
                        .await?;
                }
                Event::Acked => {
                    let flushed = *self.ack_rx.borrow_and_update();
                    self.conn
                        .send_standby_status(received_lsn, flushed, false)
                        .await?;
                }
                Event::Message(ReplicationMessage::Keepalive {
                    wal_end,
                    reply_requested,
                }) => {
                    self.translator.advance_idle(wal_end);
                    if reply_requested {
                        let flushed = *self.ack_rx.borrow_and_update();
                        self.conn
                            .send_standby_status(received_lsn, flushed, false)
                            .await?;
                    }
                    // Persist the advanced position from time to time, so that the slot does
                    // not retain the WAL when there is no change to the captured tables.
                    if let Some(interval) = self.heartbeat_interval
                        && last_heartbeat.elapsed() >= interval
                        && self.translator.commit_position()
                            > self.translator.emitted_commit_position()
                        && let Some(heartbeat) = self.translator.heartbeat()
                    {
                        batch.push(heartbeat);
                        last_heartbeat = Instant::now();
                    }
                }
                Event::Message(ReplicationMessage::XLogData { wal_start, data }) => {
                    received_lsn = received_lsn.max(wal_start);
                    match pgoutput::decode(data)? {
                        LogicalMessage::Relation(body) => {
                            let kinds = self.resolver.resolve_relation(&body).await?;
                            // With the replica identity `FULL`, all the columns are flagged
                            // as key columns, the primary key has to be read from the catalog.
                            let primary_key = if body.replica_identity == b'f' {
                                Some(self.resolver.primary_key(body.rel_id).await?)
                            } else {
                                None
                            };
                            tracing::debug!(
                                source_id = self.source_id,
                                relation = body.rel_id,
                                namespace = %body.namespace,
                                name = %body.name,
                                "received relation"
                            );
                            self.translator.set_relation(
                                body.rel_id,
                                Relation::new(&body, kinds, primary_key.as_deref()),
                            );
                        }
                        message => self.translator.on_message(wal_start, message, &mut batch)?,
                    }
                }
            }
        }
    }
}

/// Resolves how to convert the values of the user-defined types (enums, domains, arrays of
/// them, extension types), which are only identified by their OID in the replication stream.
struct TypeResolver {
    config: PgConnectionConfig,
    cache: HashMap<Oid, PgValueKind>,
}

impl TypeResolver {
    fn new(config: PgConnectionConfig) -> Self {
        Self {
            config,
            cache: HashMap::new(),
        }
    }

    async fn resolve_relation(&mut self, body: &RelationBody) -> anyhow::Result<Vec<PgValueKind>> {
        // The catalog is only queried for the types that are neither built-in nor cached,
        // with a connection that is opened on demand and closed afterwards.
        let mut client = None;
        let mut kinds = Vec::with_capacity(body.columns.len());
        for column in &body.columns {
            let kind = match self.lookup(column.type_oid) {
                Some(kind) => kind,
                None => {
                    if client.is_none() {
                        client = Some(create_pg_client(&self.config, None).await.context(
                            "failed to connect to the upstream Postgres to resolve types",
                        )?);
                    }
                    let kind = self
                        .resolve_type(client.as_ref().unwrap(), column.type_oid)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to resolve the type {} of column {} of {}.{}",
                                column.type_oid, column.name, body.namespace, body.name
                            )
                        })?;
                    self.cache.insert(column.type_oid, kind.clone());
                    kind
                }
            };
            kinds.push(kind);
        }
        Ok(kinds)
    }

    /// The names of the primary key columns of a table.
    async fn primary_key(&self, rel_id: Oid) -> anyhow::Result<Vec<String>> {
        let client = create_pg_client(&self.config, None)
            .await
            .context("failed to connect to the upstream Postgres to read the primary key")?;
        let rows = client
            .query(
                "SELECT a.attname::text \
                 FROM pg_catalog.pg_index i \
                 JOIN pg_catalog.pg_attribute a \
                   ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = $1::int8 AND i.indisprimary",
                &[&(rel_id as i64)],
            )
            .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    fn lookup(&self, type_oid: Oid) -> Option<PgValueKind> {
        builtin_value_kind(type_oid).or_else(|| self.cache.get(&type_oid).cloned())
    }

    async fn resolve_type(
        &self,
        client: &tokio_postgres::Client,
        type_oid: Oid,
    ) -> anyhow::Result<PgValueKind> {
        let mut array_depth = 0;
        let mut current = type_oid;
        let wrap = |kind: PgValueKind, array_depth: usize, delimiter: u8| {
            (0..array_depth).fold(kind, |kind, _| {
                PgValueKind::array_with_delimiter(kind, delimiter)
            })
        };
        // Follow the domains and the arrays down to the base type.
        for _ in 0..16 {
            // The elements of the arrays of all the built-in types handled here are
            // separated by commas.
            if let Some(kind) = builtin_value_kind(current) {
                return Ok(wrap(kind, array_depth, b','));
            }
            let row = client
                .query_opt(
                    "SELECT typname::text, typtype::text, typcategory::text, typelem::int8, \
                     typbasetype::int8, typdelim::text \
                     FROM pg_catalog.pg_type WHERE oid = $1::int8",
                    &[&(current as i64)],
                )
                .await?
                .with_context(|| format!("type {} not found in pg_type", current))?;
            let name: String = row.get(0);
            let typtype: String = row.get(1);
            let category: String = row.get(2);
            let elem: i64 = row.get(3);
            let base: i64 = row.get(4);
            // The delimiter of the values of this type inside an array, e.g. `:` for the
            // `PostGIS` types.
            let delimiter: String = row.get(5);
            let delimiter = delimiter.bytes().next().unwrap_or(b',');
            if typtype == "d" && base != 0 {
                current = base as Oid;
            } else if category == "A" && elem != 0 {
                array_depth += 1;
                current = elem as Oid;
            } else {
                let kind = match name.as_str() {
                    "geometry" | "geography" => PgValueKind::Geometry,
                    // enums, composite types, ranges, pgvector, hstore, citext, ...
                    _ => PgValueKind::Text,
                };
                return Ok(wrap(kind, array_depth, delimiter));
            }
        }
        bail!("type {} is nested too deeply", type_oid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_start_position() {
        assert_eq!(
            parse_start_position(None).unwrap(),
            StartPosition::default()
        );
        assert_eq!(
            parse_start_position(Some("")).unwrap(),
            StartPosition::default()
        );
        assert!(parse_start_position(Some("not json")).is_err());

        // after a completely consumed transaction
        let offset = r#"{"sourcePartition":{"server":"RW_CDC_7"},"sourceOffset":{"lsn":1250,"txId":42,"ts_usec":1,"lsn_commit":1340,"lsn_proc":1340},"isHeartbeat":false}"#;
        assert_eq!(
            parse_start_position(Some(offset)).unwrap(),
            StartPosition {
                commit_lsn: Some(1340),
                resume: None
            }
        );

        // inside a transaction
        let offset = r#"{"sourcePartition":{"server":"RW_CDC_7"},"sourceOffset":{"lsn":1400,"txId":43,"ts_usec":1,"lsn_commit":1340,"lsn_proc":1400},"isHeartbeat":false}"#;
        assert_eq!(
            parse_start_position(Some(offset)).unwrap(),
            StartPosition {
                commit_lsn: Some(1340),
                resume: Some(ResumePosition {
                    xid: 43,
                    lsn_proc: 1400
                })
            }
        );

        // an offset of the Debezium engine before the first commit
        let offset = r#"{"sourcePartition":{"server":"RW_CDC_7"},"sourceOffset":{"last_snapshot_record":false,"lsn":1400,"txId":43,"ts_usec":1,"lsn_proc":1400},"isHeartbeat":false}"#;
        assert_eq!(
            parse_start_position(Some(offset)).unwrap(),
            StartPosition::default()
        );
    }
}
