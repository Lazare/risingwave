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

//! Decoder of the `pgoutput` logical replication protocol (protocol version 1).
//!
//! See <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>.

use anyhow::{Context, bail};
use bytes::{Buf, Bytes};

pub type Lsn = u64;
pub type Oid = u32;

/// Microseconds between the Unix epoch and the Postgres epoch (2000-01-01).
pub const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// The value of a column in a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    /// An unchanged TOAST value, the actual value is not sent.
    UnchangedToast,
    /// The value in text format.
    Text(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    /// Whether the column is part of the replica identity key.
    pub is_key: bool,
    pub name: String,
    pub type_oid: Oid,
    pub type_modifier: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationBody {
    pub rel_id: Oid,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalMessage {
    Begin {
        /// The LSN of the commit record of the transaction.
        final_lsn: Lsn,
        /// Commit timestamp, in microseconds since the Unix epoch.
        commit_ts_micros: i64,
        xid: u32,
    },
    Commit {
        /// The LSN of the commit record.
        commit_lsn: Lsn,
        /// The end LSN of the commit record, i.e. where the next record starts.
        end_lsn: Lsn,
        commit_ts_micros: i64,
    },
    Relation(RelationBody),
    Insert {
        rel_id: Oid,
        new: Vec<TupleValue>,
    },
    Update {
        rel_id: Oid,
        /// The old key or the old tuple, depending on the replica identity of the table.
        /// `None` if the replica identity columns are unchanged.
        old: Option<Vec<TupleValue>>,
        new: Vec<TupleValue>,
    },
    Delete {
        rel_id: Oid,
        /// The old key or the old tuple, depending on the replica identity of the table.
        old: Vec<TupleValue>,
    },
    Truncate {
        rel_ids: Vec<Oid>,
    },
    /// Messages that carry nothing we need (`Origin`, `Type`, `Message`, ...).
    Ignored(u8),
}

struct Reader {
    buf: Bytes,
}

impl Reader {
    fn ensure(&self, n: usize) -> anyhow::Result<()> {
        if self.buf.remaining() < n {
            bail!(
                "truncated pgoutput message: need {} bytes, {} left",
                n,
                self.buf.remaining()
            );
        }
        Ok(())
    }

    fn u8(&mut self) -> anyhow::Result<u8> {
        self.ensure(1)?;
        Ok(self.buf.get_u8())
    }

    fn u16(&mut self) -> anyhow::Result<u16> {
        self.ensure(2)?;
        Ok(self.buf.get_u16())
    }

    fn u32(&mut self) -> anyhow::Result<u32> {
        self.ensure(4)?;
        Ok(self.buf.get_u32())
    }

    fn i32(&mut self) -> anyhow::Result<i32> {
        self.ensure(4)?;
        Ok(self.buf.get_i32())
    }

    fn u64(&mut self) -> anyhow::Result<u64> {
        self.ensure(8)?;
        Ok(self.buf.get_u64())
    }

    fn i64(&mut self) -> anyhow::Result<i64> {
        self.ensure(8)?;
        Ok(self.buf.get_i64())
    }

    fn bytes(&mut self, n: usize) -> anyhow::Result<Bytes> {
        self.ensure(n)?;
        Ok(self.buf.split_to(n))
    }

    /// Read a null-terminated string.
    fn cstr(&mut self) -> anyhow::Result<String> {
        let end = self
            .buf
            .iter()
            .position(|b| *b == 0)
            .context("unterminated string in pgoutput message")?;
        let raw = self.buf.split_to(end);
        self.buf.advance(1);
        String::from_utf8(raw.to_vec()).context("invalid utf-8 string in pgoutput message")
    }

    fn tuple(&mut self) -> anyhow::Result<Vec<TupleValue>> {
        let n = self.u16()? as usize;
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            let value = match self.u8()? {
                b'n' => TupleValue::Null,
                b'u' => TupleValue::UnchangedToast,
                b't' => {
                    let len = self.i32()?;
                    if len < 0 {
                        bail!("negative column length {} in pgoutput tuple", len);
                    }
                    TupleValue::Text(self.bytes(len as usize)?)
                }
                b'b' => bail!("unexpected binary column value, only the text format is requested"),
                other => bail!("unknown column kind {:?} in pgoutput tuple", other as char),
            };
            values.push(value);
        }
        Ok(values)
    }
}

/// Decode one `pgoutput` message, i.e. the payload of an `XLogData` message.
pub fn decode(data: Bytes) -> anyhow::Result<LogicalMessage> {
    let mut r = Reader { buf: data };
    let tag = r.u8()?;
    let msg = match tag {
        b'B' => {
            let final_lsn = r.u64()?;
            let ts = r.i64()?;
            let xid = r.u32()?;
            LogicalMessage::Begin {
                final_lsn,
                commit_ts_micros: ts.saturating_add(PG_EPOCH_OFFSET_MICROS),
                xid,
            }
        }
        b'C' => {
            let _flags = r.u8()?;
            let commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            let ts = r.i64()?;
            LogicalMessage::Commit {
                commit_lsn,
                end_lsn,
                commit_ts_micros: ts.saturating_add(PG_EPOCH_OFFSET_MICROS),
            }
        }
        b'R' => {
            let rel_id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let replica_identity = r.u8()?;
            let n = r.u16()? as usize;
            let mut columns = Vec::with_capacity(n);
            for _ in 0..n {
                let flags = r.u8()?;
                let name = r.cstr()?;
                let type_oid = r.u32()?;
                let type_modifier = r.i32()?;
                columns.push(RelationColumn {
                    is_key: flags & 1 != 0,
                    name,
                    type_oid,
                    type_modifier,
                });
            }
            LogicalMessage::Relation(RelationBody {
                rel_id,
                // An empty namespace stands for `pg_catalog`.
                namespace: if namespace.is_empty() {
                    "pg_catalog".to_owned()
                } else {
                    namespace
                },
                name,
                replica_identity,
                columns,
            })
        }
        b'I' => {
            let rel_id = r.u32()?;
            match r.u8()? {
                b'N' => {}
                other => bail!("unexpected tuple kind {:?} in Insert", other as char),
            }
            LogicalMessage::Insert {
                rel_id,
                new: r.tuple()?,
            }
        }
        b'U' => {
            let rel_id = r.u32()?;
            let mut old = None;
            let mut kind = r.u8()?;
            if kind == b'K' || kind == b'O' {
                old = Some(r.tuple()?);
                kind = r.u8()?;
            }
            if kind != b'N' {
                bail!("unexpected tuple kind {:?} in Update", kind as char);
            }
            LogicalMessage::Update {
                rel_id,
                old,
                new: r.tuple()?,
            }
        }
        b'D' => {
            let rel_id = r.u32()?;
            match r.u8()? {
                b'K' | b'O' => {}
                other => bail!("unexpected tuple kind {:?} in Delete", other as char),
            }
            LogicalMessage::Delete {
                rel_id,
                old: r.tuple()?,
            }
        }
        b'T' => {
            let n = r.u32()? as usize;
            let _flags = r.u8()?;
            let mut rel_ids = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                rel_ids.push(r.u32()?);
            }
            LogicalMessage::Truncate { rel_ids }
        }
        other => LogicalMessage::Ignored(other),
    };
    Ok(msg)
}

#[cfg(test)]
pub(crate) mod test_utils {
    use bytes::{BufMut, BytesMut};

    use super::*;

    pub fn begin(final_lsn: Lsn, ts_micros_unix: i64, xid: u32) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(b'B');
        b.put_u64(final_lsn);
        b.put_i64(ts_micros_unix - PG_EPOCH_OFFSET_MICROS);
        b.put_u32(xid);
        b.freeze()
    }

    pub fn commit(commit_lsn: Lsn, end_lsn: Lsn, ts_micros_unix: i64) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(b'C');
        b.put_u8(0);
        b.put_u64(commit_lsn);
        b.put_u64(end_lsn);
        b.put_i64(ts_micros_unix - PG_EPOCH_OFFSET_MICROS);
        b.freeze()
    }

    /// `columns`: `(is_key, name, type_oid)`
    pub fn relation(
        rel_id: Oid,
        namespace: &str,
        name: &str,
        columns: &[(bool, &str, Oid)],
    ) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(b'R');
        b.put_u32(rel_id);
        b.put_slice(namespace.as_bytes());
        b.put_u8(0);
        b.put_slice(name.as_bytes());
        b.put_u8(0);
        b.put_u8(b'd');
        b.put_u16(columns.len() as u16);
        for (is_key, name, type_oid) in columns {
            b.put_u8(*is_key as u8);
            b.put_slice(name.as_bytes());
            b.put_u8(0);
            b.put_u32(*type_oid);
            b.put_i32(-1);
        }
        b.freeze()
    }

    /// `None` is NULL, `Some("\u{0}toast")` is an unchanged TOAST value.
    pub const TOAST: &str = "\u{0}toast";

    fn put_tuple(b: &mut BytesMut, values: &[Option<&str>]) {
        b.put_u16(values.len() as u16);
        for v in values {
            match v {
                None => b.put_u8(b'n'),
                Some(v) if *v == TOAST => b.put_u8(b'u'),
                Some(v) => {
                    b.put_u8(b't');
                    b.put_i32(v.len() as i32);
                    b.put_slice(v.as_bytes());
                }
            }
        }
    }

    pub fn insert(rel_id: Oid, new: &[Option<&str>]) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(b'I');
        b.put_u32(rel_id);
        b.put_u8(b'N');
        put_tuple(&mut b, new);
        b.freeze()
    }

    pub fn update(rel_id: Oid, old: Option<(u8, &[Option<&str>])>, new: &[Option<&str>]) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(b'U');
        b.put_u32(rel_id);
        if let Some((kind, old)) = old {
            b.put_u8(kind);
            put_tuple(&mut b, old);
        }
        b.put_u8(b'N');
        put_tuple(&mut b, new);
        b.freeze()
    }

    pub fn delete(rel_id: Oid, kind: u8, old: &[Option<&str>]) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(b'D');
        b.put_u32(rel_id);
        b.put_u8(kind);
        put_tuple(&mut b, old);
        b.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::test_utils::*;
    use super::*;

    #[test]
    fn test_decode_begin_commit() {
        let msg = decode(begin(100, 1_700_000_000_000_000, 42)).unwrap();
        assert_eq!(
            msg,
            LogicalMessage::Begin {
                final_lsn: 100,
                commit_ts_micros: 1_700_000_000_000_000,
                xid: 42
            }
        );
        let msg = decode(commit(100, 140, 1_700_000_000_000_000)).unwrap();
        assert_eq!(
            msg,
            LogicalMessage::Commit {
                commit_lsn: 100,
                end_lsn: 140,
                commit_ts_micros: 1_700_000_000_000_000
            }
        );
    }

    #[test]
    fn test_decode_relation_and_changes() {
        let msg = decode(relation(
            16400,
            "public",
            "t",
            &[(true, "id", 23), (false, "v", 25)],
        ))
        .unwrap();
        let LogicalMessage::Relation(rel) = msg else {
            panic!("expect relation")
        };
        assert_eq!(rel.rel_id, 16400);
        assert_eq!(rel.namespace, "public");
        assert_eq!(rel.name, "t");
        assert_eq!(rel.columns.len(), 2);
        assert!(rel.columns[0].is_key);
        assert_eq!(rel.columns[1].type_oid, 25);

        let msg = decode(insert(16400, &[Some("1"), None])).unwrap();
        assert_eq!(
            msg,
            LogicalMessage::Insert {
                rel_id: 16400,
                new: vec![TupleValue::Text(Bytes::from_static(b"1")), TupleValue::Null]
            }
        );

        let msg = decode(update(
            16400,
            Some((b'K', &[Some("1"), None])),
            &[Some("2"), Some(TOAST)],
        ))
        .unwrap();
        assert_eq!(
            msg,
            LogicalMessage::Update {
                rel_id: 16400,
                old: Some(vec![
                    TupleValue::Text(Bytes::from_static(b"1")),
                    TupleValue::Null
                ]),
                new: vec![
                    TupleValue::Text(Bytes::from_static(b"2")),
                    TupleValue::UnchangedToast
                ],
            }
        );

        let msg = decode(delete(16400, b'K', &[Some("2"), None])).unwrap();
        assert!(matches!(msg, LogicalMessage::Delete { rel_id: 16400, .. }));
    }

    #[test]
    fn test_decode_truncated() {
        let full = insert(16400, &[Some("hello")]);
        let truncated = full.slice(0..full.len() - 2);
        assert!(decode(truncated).is_err());
        assert!(decode(Bytes::new()).is_err());
    }
}
