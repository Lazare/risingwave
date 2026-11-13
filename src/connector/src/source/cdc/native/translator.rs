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

//! Translation of `pgoutput` messages into the CDC messages that the Debezium based engine
//! produces, so that everything downstream of the reader (parser, routing of the shared
//! source, backfill, offset commit) is shared by the two engines.
//!
//! # Offsets
//!
//! Each message carries an offset in the Debezium format with three positions:
//!
//! - `lsn`: the position of the change itself. Changes are streamed in commit order, so it is
//!   not monotonic across transactions.
//! - `lsn_commit`: the end of the last transaction that is completely emitted. The stream is
//!   restarted from this position and it is the position confirmed to the upstream.
//! - `lsn_proc`: where the reader is inside the transaction following `lsn_commit`. On
//!   restart, the changes of that transaction before this position are skipped.
//!
//! `(lsn_commit, lsn_proc)` never goes backwards. To make a completely emitted transaction
//! visible in the offsets without an extra message, the last change of a transaction is held
//! back until the commit is seen, and carries `lsn_commit = lsn_proc = end of the commit`.

use std::collections::HashMap;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::SourceType;
use risingwave_pb::connector_service::cdc_message::CdcMessageType;

use super::pgoutput::{LogicalMessage, Lsn, Oid, RelationBody, TupleValue};
use super::value::{PgValueKind, write_json_string, write_unavailable_value, write_value};
use crate::source::SplitId;
use crate::source::base::{SourceMessage, SourceMeta};
use crate::source::cdc::DebeziumCdcMeta;

/// A table known from a `Relation` message.
pub struct Relation {
    /// `schema.table`, the routing key of the shared source.
    full_table_name: String,
    /// `"schema":"..","table":".."`, part of the `source` field of the change events.
    source_names_json: Vec<u8>,
    columns: Vec<Column>,
    /// The indices of the primary key columns.
    key_indices: Vec<usize>,
}

struct Column {
    /// `"name":`
    name_json: Vec<u8>,
    kind: PgValueKind,
}

impl Relation {
    /// `kinds` are the conversions of the columns of `body`, in order.
    ///
    /// `primary_key` are the names of the primary key columns. If `None`, the replica identity
    /// columns flagged in `body` are used, which are the primary key columns unless the
    /// replica identity of the table is changed from the default.
    pub fn new(
        body: &RelationBody,
        kinds: Vec<PgValueKind>,
        primary_key: Option<&[String]>,
    ) -> Self {
        assert_eq!(body.columns.len(), kinds.len());
        let key_indices = body
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| match primary_key {
                Some(primary_key) => primary_key.contains(&column.name),
                None => column.is_key,
            })
            .map(|(i, _)| i)
            .collect();
        let mut source_names_json = vec![];
        source_names_json.extend_from_slice(b"\"schema\":");
        write_json_string(&mut source_names_json, &body.namespace);
        source_names_json.extend_from_slice(b",\"table\":");
        write_json_string(&mut source_names_json, &body.name);
        let columns = body
            .columns
            .iter()
            .zip_eq_fast(kinds)
            .map(|(column, kind)| {
                let mut name_json = vec![];
                write_json_string(&mut name_json, &column.name);
                name_json.push(b':');
                Column { name_json, kind }
            })
            .collect();
        Self {
            full_table_name: format!("{}.{}", body.namespace, body.name),
            source_names_json,
            columns,
            key_indices,
        }
    }

    /// Whether the primary key differs between the two tuples.
    fn is_key_changed(&self, old: &[TupleValue], new: &[TupleValue]) -> bool {
        self.key_indices
            .iter()
            .any(|&i| match (old.get(i), new.get(i)) {
                (Some(old @ TupleValue::Text(_)), Some(new @ TupleValue::Text(_))) => old != new,
                (Some(TupleValue::Null), Some(TupleValue::Text(_)))
                | (Some(TupleValue::Text(_)), Some(TupleValue::Null)) => true,
                _ => false,
            })
    }
}

/// The position to resume from inside a partially emitted transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumePosition {
    pub xid: u32,
    pub lsn_proc: Lsn,
}

struct PendingChange {
    lsn: Lsn,
    full_table_name: String,
    payload: Vec<u8>,
}

struct Transaction {
    xid: u32,
    final_lsn: Lsn,
    commit_ts_micros: i64,
    /// Whether the `BEGIN` transaction metadata has been emitted.
    begin_emitted: bool,
    event_count: u64,
    /// The last change, held back until the next message of the transaction.
    pending: Option<PendingChange>,
    /// Changes before this position have been emitted before the restart.
    skip_before: Option<Lsn>,
}

pub struct Translator {
    split_id: SplitId,
    /// `RW_CDC_<source_id>`, same as the logical server name of the Debezium engine.
    server_name: String,
    /// `"db":".."`
    database_json: Vec<u8>,
    /// Whether to emit the transaction metadata.
    transactional: bool,
    relations: HashMap<Oid, Relation>,
    /// The end of the last completely emitted transaction, or a later position at which
    /// no transaction to emit was in progress.
    commit_position: Lsn,
    /// The `lsn_commit` of the last emitted message.
    emitted_commit_position: Lsn,
    last_xid: u32,
    transaction: Option<Transaction>,
    resume: Option<ResumePosition>,
}

impl Translator {
    pub fn new(
        source_id: u64,
        database: &str,
        transactional: bool,
        commit_position: Lsn,
        resume: Option<ResumePosition>,
    ) -> Self {
        let mut database_json = b"\"db\":".to_vec();
        write_json_string(&mut database_json, database);
        Self {
            split_id: source_id.to_string().into(),
            server_name: format!("RW_CDC_{}", source_id),
            database_json,
            transactional,
            relations: HashMap::new(),
            commit_position,
            emitted_commit_position: commit_position,
            last_xid: resume.map(|r| r.xid).unwrap_or_default(),
            transaction: None,
            resume,
        }
    }

    pub fn set_relation(&mut self, rel_id: Oid, relation: Relation) {
        self.relations.insert(rel_id, relation);
    }

    /// The position from which the stream can be restarted without losing any change.
    pub fn commit_position(&self) -> Lsn {
        self.commit_position
    }

    /// The `lsn_commit` of the last emitted message.
    pub fn emitted_commit_position(&self) -> Lsn {
        self.emitted_commit_position
    }

    /// Advance the commit position to `wal_end`, the position reported by a keepalive of the
    /// server. All the transactions committed before it have been sent, so when no transaction
    /// is being received, the stream can be restarted from it. This is what keeps the slot
    /// moving when the captured tables are idle.
    pub fn advance_idle(&mut self, wal_end: Lsn) {
        if self.transaction.is_none() && wal_end > self.commit_position {
            self.commit_position = wal_end;
        }
    }

    /// Build a heartbeat message carrying the commit position, if no transaction is being
    /// received.
    pub fn heartbeat(&mut self) -> Option<SourceMessage> {
        if self.transaction.is_some() {
            return None;
        }
        let position = self.commit_position;
        let offset = self.offset(position, position, position, self.last_xid, 0, true);
        Some(SourceMessage {
            key: None,
            payload: None,
            offset,
            split_id: self.split_id.clone(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                String::new(),
                0,
                CdcMessageType::Heartbeat,
                SourceType::Postgres,
            )),
        })
    }

    /// Translate a message received at `lsn` and append the resulting messages to `out`.
    /// `Relation` messages must be registered with [`Self::set_relation`] instead.
    pub fn on_message(
        &mut self,
        lsn: Lsn,
        message: LogicalMessage,
        out: &mut Vec<SourceMessage>,
    ) -> anyhow::Result<()> {
        match message {
            LogicalMessage::Begin {
                final_lsn,
                commit_ts_micros,
                xid,
            } => {
                if self.transaction.is_some() {
                    bail!("received BEGIN of transaction {} inside a transaction", xid);
                }
                // Only the first transaction after a restart can be partially emitted.
                let skip_before = self
                    .resume
                    .take()
                    .filter(|resume| resume.xid == xid)
                    .map(|resume| resume.lsn_proc);
                self.transaction = Some(Transaction {
                    xid,
                    final_lsn,
                    commit_ts_micros,
                    begin_emitted: false,
                    event_count: 0,
                    pending: None,
                    skip_before,
                });
            }
            LogicalMessage::Commit { end_lsn, .. } => {
                let mut txn = self
                    .transaction
                    .take()
                    .context("received COMMIT outside a transaction")?;
                let position = end_lsn.max(self.commit_position);
                self.commit_position = position;
                self.last_xid = txn.xid;
                if let Some(pending) = txn.pending.take() {
                    let offset = self.offset(
                        pending.lsn,
                        position,
                        position,
                        txn.xid,
                        txn.commit_ts_micros,
                        false,
                    );
                    out.push(self.data_message(pending, offset, &txn));
                }
                if txn.begin_emitted {
                    let offset = self.offset(
                        position,
                        position,
                        position,
                        txn.xid,
                        txn.commit_ts_micros,
                        false,
                    );
                    out.push(self.transaction_message(&txn, false, offset));
                }
            }
            LogicalMessage::Insert { rel_id, new } => {
                self.on_change(lsn, rel_id, "c", None, Some(&new), out)?;
            }
            LogicalMessage::Update { rel_id, old, new } => {
                let key_changed = match (&old, self.relations.get(&rel_id)) {
                    (Some(old), Some(relation)) => relation.is_key_changed(old, &new),
                    _ => false,
                };
                if key_changed {
                    // Same as the Debezium engine: an update of the primary key is a delete
                    // of the old row followed by an insert of the new row.
                    self.on_change(lsn, rel_id, "d", old.as_deref(), None, out)?;
                    self.on_change(lsn, rel_id, "c", old.as_deref(), Some(&new), out)?;
                } else {
                    self.on_change(lsn, rel_id, "u", old.as_deref(), Some(&new), out)?;
                }
            }
            LogicalMessage::Delete { rel_id, old } => {
                self.on_change(lsn, rel_id, "d", Some(&old), None, out)?;
            }
            LogicalMessage::Relation(body) => {
                bail!(
                    "relation {}.{} must be registered before translating",
                    body.namespace,
                    body.name
                );
            }
            // Same as the Debezium engine, which skips the truncate events by default.
            LogicalMessage::Truncate { .. } | LogicalMessage::Ignored(_) => {}
        }
        Ok(())
    }

    fn on_change(
        &mut self,
        lsn: Lsn,
        rel_id: Oid,
        op: &str,
        before: Option<&[TupleValue]>,
        after: Option<&[TupleValue]>,
        out: &mut Vec<SourceMessage>,
    ) -> anyhow::Result<()> {
        let mut txn = self
            .transaction
            .take()
            .context("received a change outside a transaction")?;
        let result = self.on_change_inner(&mut txn, lsn, rel_id, op, before, after, out);
        self.transaction = Some(txn);
        result
    }

    fn on_change_inner(
        &mut self,
        txn: &mut Transaction,
        lsn: Lsn,
        rel_id: Oid,
        op: &str,
        before: Option<&[TupleValue]>,
        after: Option<&[TupleValue]>,
        out: &mut Vec<SourceMessage>,
    ) -> anyhow::Result<()> {
        // Several changes can share the same position (e.g. a multi-row insert), so the
        // changes at the resume position are emitted again rather than skipped.
        if let Some(skip_before) = txn.skip_before
            && lsn < skip_before
        {
            return Ok(());
        }
        let relation = self
            .relations
            .get(&rel_id)
            .with_context(|| format!("received a change of unknown relation {}", rel_id))?;

        let mut payload = Vec::with_capacity(256);
        payload.extend_from_slice(b"{\"schema\":null,\"payload\":{\"before\":");
        // For an insert, `before` is only given to look up the unchanged TOAST values.
        write_tuple(&mut payload, relation, before.filter(|_| op != "c"), None)?;
        payload.extend_from_slice(b",\"after\":");
        write_tuple(&mut payload, relation, after, before)?;
        payload.extend_from_slice(
            b",\"source\":{\"version\":\"risingwave-native\",\"connector\":\"postgresql\",\"name\":",
        );
        write_json_string(&mut payload, &self.server_name);
        write!(
            payload,
            ",\"ts_ms\":{},\"snapshot\":\"false\",",
            txn.commit_ts_micros / 1000
        )?;
        payload.extend_from_slice(&self.database_json);
        payload.push(b',');
        payload.extend_from_slice(&relation.source_names_json);
        write!(
            payload,
            ",\"txId\":{},\"lsn\":{},\"xmin\":null}},\"op\":\"{}\",\"ts_ms\":{},\"transaction\":null}}}}",
            txn.xid,
            lsn,
            op,
            now_millis()
        )?;
        let change = PendingChange {
            lsn,
            full_table_name: relation.full_table_name.clone(),
            payload,
        };

        let commit_position = self.commit_position;
        if let Some(previous) = txn.pending.take() {
            let offset = self.offset(
                previous.lsn,
                commit_position,
                previous.lsn.max(commit_position),
                txn.xid,
                txn.commit_ts_micros,
                false,
            );
            out.push(self.data_message(previous, offset, txn));
        } else if self.transactional && !txn.begin_emitted {
            // The metadata of the transactions without any change to emit is omitted.
            let offset = self.offset(
                lsn,
                commit_position,
                lsn.max(commit_position),
                txn.xid,
                txn.commit_ts_micros,
                false,
            );
            out.push(self.transaction_message(txn, true, offset));
            txn.begin_emitted = true;
        }
        txn.event_count += 1;
        txn.pending = Some(change);
        Ok(())
    }

    fn data_message(
        &self,
        change: PendingChange,
        offset: String,
        txn: &Transaction,
    ) -> SourceMessage {
        SourceMessage {
            key: None,
            payload: Some(change.payload),
            offset,
            split_id: self.split_id.clone(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                change.full_table_name,
                txn.commit_ts_micros / 1000,
                CdcMessageType::Data,
                SourceType::Postgres,
            )),
        }
    }

    fn transaction_message(&self, txn: &Transaction, begin: bool, offset: String) -> SourceMessage {
        let ts_ms = txn.commit_ts_micros / 1000;
        let payload = if begin {
            format!(
                "{{\"schema\":null,\"payload\":{{\"status\":\"BEGIN\",\"id\":\"{}:{}\",\"event_count\":null,\"data_collections\":null,\"ts_ms\":{}}}}}",
                txn.xid, txn.final_lsn, ts_ms
            )
        } else {
            format!(
                "{{\"schema\":null,\"payload\":{{\"status\":\"END\",\"id\":\"{}:{}\",\"event_count\":{},\"data_collections\":null,\"ts_ms\":{}}}}}",
                txn.xid, txn.final_lsn, txn.event_count, ts_ms
            )
        };
        SourceMessage {
            key: None,
            payload: Some(payload.into_bytes()),
            offset,
            split_id: self.split_id.clone(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                String::new(),
                ts_ms,
                CdcMessageType::TransactionMeta,
                SourceType::Postgres,
            )),
        }
    }

    /// Encode an offset in the format of the Debezium engine, see `DebeziumOffset`.
    fn offset(
        &mut self,
        lsn: Lsn,
        lsn_commit: Lsn,
        lsn_proc: Lsn,
        xid: u32,
        ts_usec: i64,
        is_heartbeat: bool,
    ) -> String {
        self.emitted_commit_position = lsn_commit;
        format!(
            "{{\"sourcePartition\":{{\"server\":\"{}\"}},\"sourceOffset\":{{\"lsn\":{},\"txId\":{},\"ts_usec\":{},\"lsn_commit\":{},\"lsn_proc\":{}}},\"isHeartbeat\":{}}}",
            self.server_name, lsn, xid, ts_usec, lsn_commit, lsn_proc, is_heartbeat
        )
    }
}

/// Write a tuple as a JSON object. The unchanged TOAST values are taken from `old`, the
/// previous version of the row, when it has them (replica identity `FULL`), otherwise they
/// are written as the placeholder that is resolved downstream.
fn write_tuple(
    buf: &mut Vec<u8>,
    relation: &Relation,
    tuple: Option<&[TupleValue]>,
    old: Option<&[TupleValue]>,
) -> anyhow::Result<()> {
    let Some(tuple) = tuple else {
        buf.extend_from_slice(b"null");
        return Ok(());
    };
    if tuple.len() != relation.columns.len() {
        bail!(
            "tuple of {} has {} columns, expect {}",
            relation.full_table_name,
            tuple.len(),
            relation.columns.len()
        );
    }
    buf.push(b'{');
    for (i, (column, value)) in relation.columns.iter().zip_eq_fast(tuple).enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.extend_from_slice(&column.name_json);
        let value = match (value, old.and_then(|old| old.get(i))) {
            (TupleValue::UnchangedToast, Some(old @ TupleValue::Text(_))) => old,
            _ => value,
        };
        match value {
            TupleValue::Null => buf.extend_from_slice(b"null"),
            TupleValue::UnchangedToast => write_unavailable_value(buf, &column.kind),
            TupleValue::Text(text) => {
                let text = std::str::from_utf8(text).with_context(|| {
                    format!("invalid utf-8 value in {}", relation.full_table_name)
                })?;
                write_value(buf, &column.kind, text);
            }
        }
    }
    buf.push(b'}');
    Ok(())
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::super::pgoutput::test_utils::{self, TOAST};
    use super::super::pgoutput::{LogicalMessage, decode};
    use super::super::value::builtin_value_kind;
    use super::*;
    use crate::source::cdc::external::postgres::PostgresOffset;

    const REL: Oid = 16400;
    const TS: i64 = 1_700_000_000_123_456;

    fn translator(transactional: bool, resume: Option<ResumePosition>) -> Translator {
        let mut t = Translator::new(7, "mydb", transactional, 1000, resume);
        // id int4 primary key, v text, tags text[]
        let body = test_utils::relation(
            REL,
            "public",
            "t",
            &[(true, "id", 23), (false, "v", 25), (false, "tags", 1009)],
        );
        let LogicalMessage::Relation(body) = decode(body).unwrap() else {
            unreachable!()
        };
        let kinds = body
            .columns
            .iter()
            .map(|c| builtin_value_kind(c.type_oid).unwrap())
            .collect();
        t.set_relation(REL, Relation::new(&body, kinds, None));
        t
    }

    fn feed(t: &mut Translator, lsn: Lsn, data: bytes::Bytes) -> Vec<SourceMessage> {
        let mut out = vec![];
        t.on_message(lsn, decode(data).unwrap(), &mut out).unwrap();
        out
    }

    fn payload(msg: &SourceMessage) -> serde_json::Value {
        serde_json::from_slice(msg.payload.as_ref().unwrap()).unwrap()
    }

    /// `(lsn, lsn_commit, lsn_proc)`
    fn positions(msg: &SourceMessage) -> (u64, u64, u64) {
        let offset = PostgresOffset::parse_debezium_offset(&msg.offset).unwrap();
        (
            offset.lsn,
            offset.lsn_commit.unwrap(),
            offset.lsn_proc.unwrap(),
        )
    }

    fn msg_type(msg: &SourceMessage) -> String {
        let SourceMeta::DebeziumCdc(meta) = &msg.meta else {
            unreachable!()
        };
        format!("{:?}", meta.msg_type)
    }

    #[test]
    fn test_changes_and_offsets() {
        let mut t = translator(false, None);
        assert!(feed(&mut t, 1100, test_utils::begin(1300, TS, 42)).is_empty());
        // the last change is held back until the next message
        assert!(
            feed(
                &mut t,
                1100,
                test_utils::insert(REL, &[Some("1"), Some("a"), Some("{x,y}")])
            )
            .is_empty()
        );
        let out = feed(
            &mut t,
            1200,
            test_utils::update(REL, None, &[Some("1"), Some(TOAST), None]),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(positions(&out[0]), (1100, 1000, 1100));
        let event = payload(&out[0]);
        assert_eq!(event["payload"]["op"], "c");
        assert_eq!(event["payload"]["before"], serde_json::Value::Null);
        assert_eq!(
            event["payload"]["after"],
            serde_json::json!({"id": 1, "v": "a", "tags": ["x", "y"]})
        );
        assert_eq!(event["payload"]["source"]["ts_ms"], TS / 1000);
        assert_eq!(event["payload"]["source"]["schema"], "public");
        assert_eq!(event["payload"]["source"]["table"], "t");
        assert_eq!(event["payload"]["source"]["db"], "mydb");
        let SourceMeta::DebeziumCdc(meta) = &out[0].meta else {
            unreachable!()
        };
        assert_eq!(meta.full_table_name, "public.t");
        assert_eq!(meta.source_ts_ms, TS / 1000);
        assert_eq!(out[0].split_id.as_ref(), "7");

        let out = feed(
            &mut t,
            1250,
            test_utils::delete(REL, b'K', &[Some("1"), None, None]),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(positions(&out[0]), (1200, 1000, 1200));
        let event = payload(&out[0]);
        assert_eq!(event["payload"]["op"], "u");
        assert_eq!(
            event["payload"]["after"],
            serde_json::json!({"id": 1, "v": "__debezium_unavailable_value", "tags": null})
        );

        // the last change of the transaction carries the end of the commit
        let out = feed(&mut t, 1340, test_utils::commit(1300, 1340, TS));
        assert_eq!(out.len(), 1);
        assert_eq!(positions(&out[0]), (1250, 1340, 1340));
        let event = payload(&out[0]);
        assert_eq!(event["payload"]["op"], "d");
        assert_eq!(
            event["payload"]["before"],
            serde_json::json!({"id": 1, "v": null, "tags": null})
        );
        assert_eq!(event["payload"]["after"], serde_json::Value::Null);
        assert_eq!(t.commit_position(), 1340);
        assert_eq!(t.emitted_commit_position(), 1340);
    }

    #[test]
    fn test_offsets_never_go_backwards() {
        let mut t = translator(false, None);
        let mut all = vec![];
        // transaction 1 commits at [1500, 1540)
        all.extend(feed(&mut t, 1400, test_utils::begin(1500, TS, 1)));
        all.extend(feed(
            &mut t,
            1400,
            test_utils::insert(REL, &[Some("1"), None, None]),
        ));
        all.extend(feed(&mut t, 1540, test_utils::commit(1500, 1540, TS)));
        // transaction 2 has written its changes before the commit of transaction 1
        all.extend(feed(&mut t, 1100, test_utils::begin(1600, TS, 2)));
        all.extend(feed(
            &mut t,
            1100,
            test_utils::insert(REL, &[Some("2"), None, None]),
        ));
        all.extend(feed(
            &mut t,
            1560,
            test_utils::insert(REL, &[Some("3"), None, None]),
        ));
        all.extend(feed(&mut t, 1640, test_utils::commit(1600, 1640, TS)));

        let offsets = all
            .iter()
            .map(|m| PostgresOffset::parse_debezium_offset(&m.offset).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(offsets.len(), 3);
        assert!(offsets.is_sorted());
        assert_eq!(positions(&all[0]), (1400, 1540, 1540));
        assert_eq!(positions(&all[1]), (1100, 1540, 1540));
        assert_eq!(positions(&all[2]), (1560, 1640, 1640));
    }

    #[test]
    fn test_transaction_metadata() {
        let mut t = translator(true, None);
        let mut all = vec![];
        // a transaction without any change is omitted
        all.extend(feed(&mut t, 1100, test_utils::begin(1150, TS, 1)));
        all.extend(feed(&mut t, 1190, test_utils::commit(1150, 1190, TS)));
        assert!(all.is_empty());
        assert_eq!(t.commit_position(), 1190);

        all.extend(feed(&mut t, 1200, test_utils::begin(1300, TS, 2)));
        all.extend(feed(
            &mut t,
            1200,
            test_utils::insert(REL, &[Some("1"), None, None]),
        ));
        all.extend(feed(
            &mut t,
            1210,
            test_utils::insert(REL, &[Some("2"), None, None]),
        ));
        all.extend(feed(&mut t, 1340, test_utils::commit(1300, 1340, TS)));
        assert_eq!(all.len(), 4);
        assert_eq!(
            all.iter().map(msg_type).collect::<Vec<_>>(),
            vec!["TransactionMeta", "Data", "Data", "TransactionMeta"]
        );
        let begin = payload(&all[0]);
        assert_eq!(begin["payload"]["status"], "BEGIN");
        assert_eq!(begin["payload"]["id"], "2:1300");
        let end = payload(&all[3]);
        assert_eq!(end["payload"]["status"], "END");
        assert_eq!(end["payload"]["id"], "2:1300");
        assert_eq!(end["payload"]["event_count"], 2);
        assert_eq!(positions(&all[0]), (1200, 1190, 1200));
        assert_eq!(positions(&all[3]), (1340, 1340, 1340));
    }

    #[test]
    fn test_resume_inside_transaction() {
        // The changes at 1200 and before had been emitted before the restart.
        let resume = ResumePosition {
            xid: 42,
            lsn_proc: 1200,
        };
        let mut t = translator(false, Some(resume));
        let mut all = vec![];
        all.extend(feed(&mut t, 1100, test_utils::begin(1300, TS, 42)));
        all.extend(feed(
            &mut t,
            1100,
            test_utils::insert(REL, &[Some("1"), None, None]),
        ));
        all.extend(feed(
            &mut t,
            1200,
            test_utils::insert(REL, &[Some("2"), None, None]),
        ));
        all.extend(feed(
            &mut t,
            1250,
            test_utils::insert(REL, &[Some("3"), None, None]),
        ));
        all.extend(feed(&mut t, 1340, test_utils::commit(1300, 1340, TS)));
        let ids = all
            .iter()
            .map(|m| payload(m)["payload"]["after"]["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![2, 3]);

        // Only the first transaction after the restart is filtered.
        let mut all = vec![];
        all.extend(feed(&mut t, 1150, test_utils::begin(1400, TS, 42)));
        all.extend(feed(
            &mut t,
            1150,
            test_utils::insert(REL, &[Some("4"), None, None]),
        ));
        all.extend(feed(&mut t, 1440, test_utils::commit(1400, 1440, TS)));
        assert_eq!(all.len(), 1);

        // A different first transaction is not filtered.
        let mut t = translator(false, Some(resume));
        let mut all = vec![];
        all.extend(feed(&mut t, 1100, test_utils::begin(1300, TS, 43)));
        all.extend(feed(
            &mut t,
            1100,
            test_utils::insert(REL, &[Some("1"), None, None]),
        ));
        all.extend(feed(&mut t, 1340, test_utils::commit(1300, 1340, TS)));
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_update_of_primary_key() {
        let mut t = translator(false, None);
        let mut all = vec![];
        all.extend(feed(&mut t, 1100, test_utils::begin(1300, TS, 1)));
        all.extend(feed(
            &mut t,
            1100,
            test_utils::update(
                REL,
                Some((b'K', &[Some("1"), None, None])),
                &[Some("2"), Some(TOAST), None],
            ),
        ));
        // the key is unchanged, the old tuple is sent because of the replica identity `FULL`
        all.extend(feed(
            &mut t,
            1200,
            test_utils::update(
                REL,
                Some((b'O', &[Some("2"), Some("big"), None])),
                &[Some("2"), Some(TOAST), Some("{x}")],
            ),
        ));
        all.extend(feed(&mut t, 1340, test_utils::commit(1300, 1340, TS)));
        let events = all
            .iter()
            .map(|m| payload(m)["payload"].clone())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["op"], "d");
        assert_eq!(events[0]["before"]["id"], 1);
        assert_eq!(events[0]["after"], serde_json::Value::Null);
        assert_eq!(events[1]["op"], "c");
        assert_eq!(events[1]["before"], serde_json::Value::Null);
        assert_eq!(
            events[1]["after"],
            serde_json::json!({"id": 2, "v": "__debezium_unavailable_value", "tags": null})
        );
        assert_eq!(events[2]["op"], "u");
        assert_eq!(events[2]["before"]["v"], "big");
        // the unchanged TOAST value is taken from the old tuple
        assert_eq!(
            events[2]["after"],
            serde_json::json!({"id": 2, "v": "big", "tags": ["x"]})
        );
    }

    #[test]
    fn test_primary_key_override() {
        let body = test_utils::relation(REL, "public", "t", &[(true, "id", 23), (true, "v", 25)]);
        let LogicalMessage::Relation(body) = decode(body).unwrap() else {
            unreachable!()
        };
        let kinds = vec![PgValueKind::Int, PgValueKind::Text];
        let text = |s: &'static str| TupleValue::Text(bytes::Bytes::from_static(s.as_bytes()));
        let old = [text("1"), text("a")];
        let new = [text("1"), text("b")];
        // replica identity `FULL` flags all the columns
        let relation = Relation::new(&body, kinds.clone(), None);
        assert!(relation.is_key_changed(&old, &new));
        let relation = Relation::new(&body, kinds, Some(&["id".to_owned()]));
        assert!(!relation.is_key_changed(&old, &new));
        assert!(relation.is_key_changed(&old, &[text("2"), text("a")]));
    }

    #[test]
    fn test_heartbeat() {
        let mut t = translator(false, None);
        t.advance_idle(2000);
        assert_eq!(t.commit_position(), 2000);
        let heartbeat = t.heartbeat().unwrap();
        assert!(heartbeat.is_cdc_heartbeat());
        assert_eq!(positions(&heartbeat), (2000, 2000, 2000));
        assert!(heartbeat.offset.contains("\"isHeartbeat\":true"));
        assert_eq!(t.emitted_commit_position(), 2000);

        // The position is not advanced while a transaction is being received.
        feed(&mut t, 2100, test_utils::begin(2300, TS, 5));
        t.advance_idle(2200);
        assert_eq!(t.commit_position(), 2000);
        assert!(t.heartbeat().is_none());
        // The changes after a keepalive are based on the advanced position.
        feed(
            &mut t,
            2100,
            test_utils::insert(REL, &[Some("1"), None, None]),
        );
        let out = feed(
            &mut t,
            2150,
            test_utils::insert(REL, &[Some("2"), None, None]),
        );
        assert_eq!(positions(&out[0]), (2100, 2000, 2100));
    }

    #[test]
    fn test_protocol_violations() {
        let mut t = translator(false, None);
        let mut out = vec![];
        let insert = decode(test_utils::insert(REL, &[Some("1"), None, None])).unwrap();
        assert!(t.on_message(1100, insert, &mut out).is_err());

        feed(&mut t, 1100, test_utils::begin(1300, TS, 1));
        let unknown = decode(test_utils::insert(1, &[Some("1")])).unwrap();
        assert!(t.on_message(1100, unknown, &mut out).is_err());
        let mismatch = decode(test_utils::insert(REL, &[Some("1")])).unwrap();
        assert!(t.on_message(1100, mismatch, &mut out).is_err());
    }
}
