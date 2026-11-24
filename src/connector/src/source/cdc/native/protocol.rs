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

//! A minimal client of the Postgres streaming replication protocol.
//!
//! `tokio-postgres` does not support the `CopyBoth` mode used by `START_REPLICATION`, so the
//! replication connection speaks the wire protocol directly. Only what a logical replication
//! client needs is implemented: startup, authentication, simple queries and the replication
//! stream.
//!
//! See <https://www.postgresql.org/docs/current/protocol-replication.html>.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::authentication::{md5_hash, sasl};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use super::pgoutput::{Lsn, PG_EPOCH_OFFSET_MICROS};
use crate::connector_common::postgres::{PgConnectionConfig, SslMode};

trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// A message of the replication stream, sent by the server in the `CopyBoth` mode.
#[derive(Debug)]
pub enum ReplicationMessage {
    XLogData {
        /// The starting point of the WAL data in this message.
        wal_start: Lsn,
        data: Bytes,
    },
    Keepalive {
        /// The current end of WAL sent by the server.
        wal_end: Lsn,
        reply_requested: bool,
    },
}

/// The rows of a simple query result, all values are in the text format.
pub type SimpleQueryRows = Vec<Vec<Option<String>>>;

pub struct ReplicationConnection {
    stream: Box<dyn AsyncStream>,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl ReplicationConnection {
    /// Connect to the upstream database with `replication=database`, so that both replication
    /// commands and ordinary SQL can be run with simple queries.
    pub async fn connect(
        config: &PgConnectionConfig,
        application_name: &str,
    ) -> anyhow::Result<Self> {
        let tcp = TcpStream::connect((config.host.as_str(), config.port))
            .await
            .with_context(|| format!("failed to connect to {}:{}", config.host, config.port))?;
        let stream = negotiate_tls(tcp, config).await?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
            write_buf: BytesMut::with_capacity(1024),
        };
        conn.startup(config, application_name).await?;
        Ok(conn)
    }

    async fn startup(
        &mut self,
        config: &PgConnectionConfig,
        application_name: &str,
    ) -> anyhow::Result<()> {
        let params = [
            ("user", config.user.as_str()),
            ("database", config.database.as_str()),
            ("replication", "database"),
            ("application_name", application_name),
            ("client_encoding", "UTF8"),
            // The session settings below decide the text format of the replicated values,
            // which `value.rs` relies on.
            ("DateStyle", "ISO"),
            ("IntervalStyle", "iso_8601"),
            ("TimeZone", "UTC"),
            ("extra_float_digits", "3"),
        ];
        frontend::startup_message(params, &mut self.write_buf)?;
        self.flush().await?;

        let mut scram: Option<sasl::ScramSha256> = None;
        loop {
            let (tag, mut body) = self.read_frame().await?;
            match tag {
                b'R' => {
                    if body.remaining() < 4 {
                        bail!("malformed authentication message");
                    }
                    match body.get_i32() {
                        // AuthenticationOk
                        0 => {}
                        // AuthenticationCleartextPassword
                        3 => {
                            frontend::password_message(
                                config.password.as_bytes(),
                                &mut self.write_buf,
                            )?;
                            self.flush().await?;
                        }
                        // AuthenticationMD5Password
                        5 => {
                            if body.remaining() < 4 {
                                bail!("malformed md5 authentication message");
                            }
                            let mut salt = [0u8; 4];
                            body.copy_to_slice(&mut salt);
                            let hash =
                                md5_hash(config.user.as_bytes(), config.password.as_bytes(), salt);
                            frontend::password_message(hash.as_bytes(), &mut self.write_buf)?;
                            self.flush().await?;
                        }
                        // AuthenticationSASL
                        10 => {
                            let mechanisms = body
                                .split(|b| *b == 0)
                                .filter(|m| !m.is_empty())
                                .map(|m| String::from_utf8_lossy(m).into_owned())
                                .collect::<Vec<_>>();
                            if !mechanisms.iter().any(|m| m == sasl::SCRAM_SHA_256) {
                                bail!(
                                    "unsupported SASL authentication mechanisms: {:?}",
                                    mechanisms
                                );
                            }
                            let state = sasl::ScramSha256::new(
                                config.password.as_bytes(),
                                sasl::ChannelBinding::unsupported(),
                            );
                            frontend::sasl_initial_response(
                                sasl::SCRAM_SHA_256,
                                state.message(),
                                &mut self.write_buf,
                            )?;
                            scram = Some(state);
                            self.flush().await?;
                        }
                        // AuthenticationSASLContinue
                        11 => {
                            let state = scram.as_mut().context("unexpected SASL continue")?;
                            state.update(&body)?;
                            frontend::sasl_response(state.message(), &mut self.write_buf)?;
                            self.flush().await?;
                        }
                        // AuthenticationSASLFinal
                        12 => {
                            let state = scram.as_mut().context("unexpected SASL final")?;
                            state.finish(&body)?;
                        }
                        other => bail!("unsupported authentication method {}", other),
                    }
                }
                b'E' => return Err(parse_error_response(&body)),
                // ReadyForQuery
                b'Z' => return Ok(()),
                // ParameterStatus, BackendKeyData, NoticeResponse
                _ => {}
            }
        }
    }

    /// Run a simple query and collect the rows of its (last) result set.
    pub async fn simple_query(&mut self, sql: &str) -> anyhow::Result<SimpleQueryRows> {
        frontend::query(sql, &mut self.write_buf)?;
        self.flush().await?;
        let mut rows = vec![];
        let mut error = None;
        loop {
            let (tag, mut body) = self.read_frame().await?;
            match tag {
                // DataRow
                b'D' => {
                    if body.remaining() < 2 {
                        bail!("malformed data row");
                    }
                    let n = body.get_i16();
                    let mut row = Vec::with_capacity(n.max(0) as usize);
                    for _ in 0..n {
                        if body.remaining() < 4 {
                            bail!("malformed data row");
                        }
                        let len = body.get_i32();
                        if len < 0 {
                            row.push(None);
                        } else {
                            let len = len as usize;
                            if body.remaining() < len {
                                bail!("malformed data row");
                            }
                            let value = body.split_to(len);
                            row.push(Some(String::from_utf8_lossy(&value).into_owned()));
                        }
                    }
                    rows.push(row);
                }
                b'E' => error = Some(parse_error_response(&body)),
                b'Z' => {
                    return match error {
                        Some(e) => Err(e.context(format!("failed to run `{}`", sql))),
                        None => Ok(rows),
                    };
                }
                // RowDescription, CommandComplete, EmptyQueryResponse, NoticeResponse, ...
                _ => {}
            }
        }
    }

    /// Start streaming the changes of the `publication` decoded by `pgoutput` from the logical
    /// replication `slot`. The server starts from `start_lsn`, or from the confirmed flush LSN
    /// of the slot if it is greater.
    pub async fn start_replication(
        &mut self,
        slot: &str,
        publication: &str,
        start_lsn: Lsn,
    ) -> anyhow::Result<()> {
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {})",
            quote_identifier(slot),
            format_lsn(start_lsn),
            quote_literal(&quote_identifier(publication)),
        );
        frontend::query(&sql, &mut self.write_buf)?;
        self.flush().await?;
        loop {
            let (tag, body) = self.read_frame().await?;
            match tag {
                // CopyBothResponse
                b'W' => return Ok(()),
                b'E' => {
                    return Err(
                        parse_error_response(&body).context(format!("failed to run `{}`", sql))
                    );
                }
                _ => {}
            }
        }
    }

    /// Whether a complete message has been received and can be read without waiting.
    pub fn has_buffered_message(&self) -> bool {
        self.read_buf.len() >= 5 && {
            let len = i32::from_be_bytes(self.read_buf[1..5].try_into().unwrap());
            len >= 4 && self.read_buf.len() > len as usize
        }
    }

    /// Read the next message of the replication stream.
    ///
    /// This method is cancellation safe.
    pub async fn next_replication_message(&mut self) -> anyhow::Result<ReplicationMessage> {
        loop {
            let (tag, mut body) = self.read_frame().await?;
            match tag {
                // CopyData
                b'd' => {
                    if !body.has_remaining() {
                        bail!("empty replication message");
                    }
                    match body.get_u8() {
                        b'w' => {
                            if body.remaining() < 24 {
                                bail!("malformed XLogData message");
                            }
                            let wal_start = body.get_u64();
                            let _wal_end = body.get_u64();
                            let _timestamp = body.get_i64();
                            return Ok(ReplicationMessage::XLogData {
                                wal_start,
                                data: body,
                            });
                        }
                        b'k' => {
                            if body.remaining() < 17 {
                                bail!("malformed keepalive message");
                            }
                            let wal_end = body.get_u64();
                            let _timestamp = body.get_i64();
                            let reply_requested = body.get_u8() != 0;
                            return Ok(ReplicationMessage::Keepalive {
                                wal_end,
                                reply_requested,
                            });
                        }
                        other => bail!("unknown replication message {:?}", other as char),
                    }
                }
                b'E' => return Err(parse_error_response(&body)),
                // CopyDone
                b'c' => bail!("the replication stream is ended by the upstream Postgres"),
                _ => {}
            }
        }
    }

    /// Report the position up to which the changes are durably processed, which allows the
    /// server to advance the slot and to recycle the WAL.
    pub async fn send_standby_status(
        &mut self,
        received_lsn: Lsn,
        flushed_lsn: Lsn,
        reply_requested: bool,
    ) -> anyhow::Result<()> {
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or_default();
        self.write_buf.put_u8(b'd');
        self.write_buf.put_i32(4 + 1 + 8 * 4 + 1);
        self.write_buf.put_u8(b'r');
        self.write_buf.put_u64(received_lsn.max(flushed_lsn));
        self.write_buf.put_u64(flushed_lsn);
        self.write_buf.put_u64(flushed_lsn);
        self.write_buf.put_i64(now_micros - PG_EPOCH_OFFSET_MICROS);
        self.write_buf.put_u8(reply_requested as u8);
        self.flush().await
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(&self.write_buf).await?;
        self.write_buf.clear();
        self.stream.flush().await?;
        Ok(())
    }

    fn try_take_frame(&mut self) -> anyhow::Result<Option<(u8, Bytes)>> {
        if self.read_buf.len() < 5 {
            return Ok(None);
        }
        let tag = self.read_buf[0];
        let len = i32::from_be_bytes(self.read_buf[1..5].try_into().unwrap());
        if len < 4 {
            bail!("invalid message length {} from the upstream Postgres", len);
        }
        let total = 1 + len as usize;
        if self.read_buf.len() < total {
            self.read_buf.reserve(total - self.read_buf.len());
            return Ok(None);
        }
        self.read_buf.advance(5);
        let body = self.read_buf.split_to(total - 5).freeze();
        Ok(Some((tag, body)))
    }

    async fn read_frame(&mut self) -> anyhow::Result<(u8, Bytes)> {
        loop {
            if let Some(frame) = self.try_take_frame()? {
                return Ok(frame);
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                bail!("connection closed by the upstream Postgres");
            }
        }
    }
}

#[cfg(not(madsim))]
async fn negotiate_tls(
    mut tcp: TcpStream,
    config: &PgConnectionConfig,
) -> anyhow::Result<Box<dyn AsyncStream>> {
    use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};

    if matches!(config.ssl_mode, SslMode::Disabled) {
        return Ok(Box::new(tcp));
    }

    let mut buf = BytesMut::new();
    frontend::ssl_request(&mut buf);
    tcp.write_all(&buf).await?;
    tcp.flush().await?;
    let mut response = [0u8; 1];
    tcp.read_exact(&mut response).await?;
    if response[0] != b'S' {
        if matches!(config.ssl_mode, SslMode::Preferred) {
            return Ok(Box::new(tcp));
        }
        bail!(
            "the upstream Postgres does not support SSL, which is required by ssl.mode = {}",
            config.ssl_mode
        );
    }

    let mut builder = SslConnector::builder(SslMethod::tls())?;
    let verify_hostname = match config.ssl_mode {
        SslMode::VerifyCa | SslMode::VerifyFull => {
            if let Some(ssl_root_cert) = &config.ssl_root_cert {
                builder
                    .set_ca_file(ssl_root_cert)
                    .map_err(|e| anyhow!("bad ssl root cert error: {}", e))?;
            }
            matches!(config.ssl_mode, SslMode::VerifyFull)
        }
        _ => {
            // the certificate is not verified for `prefer` and `require`
            builder.set_verify(SslVerifyMode::NONE);
            false
        }
    };
    let mut ssl_config = builder.build().configure()?;
    ssl_config.set_verify_hostname(verify_hostname);
    let ssl = ssl_config.into_ssl(&config.host)?;
    let mut stream = tokio_openssl::SslStream::new(ssl, tcp)?;
    std::pin::Pin::new(&mut stream)
        .connect()
        .await
        .map_err(|e| anyhow!("SSL handshake with the upstream Postgres failed: {}", e))?;
    Ok(Box::new(stream))
}

#[cfg(madsim)]
async fn negotiate_tls(
    tcp: TcpStream,
    config: &PgConnectionConfig,
) -> anyhow::Result<Box<dyn AsyncStream>> {
    let _ = SslMode::Disabled == config.ssl_mode;
    Ok(Box::new(tcp))
}

fn parse_error_response(mut body: &[u8]) -> anyhow::Error {
    let mut severity = "";
    let mut code = "";
    let mut message = "";
    let mut detail = None;
    while let Some((&field, rest)) = body.split_first() {
        if field == 0 {
            break;
        }
        let end = rest.iter().position(|b| *b == 0).unwrap_or(rest.len());
        let value = std::str::from_utf8(&rest[..end]).unwrap_or_default();
        match field {
            b'S' => severity = value,
            b'C' => code = value,
            b'M' => message = value,
            b'D' => detail = Some(value),
            _ => {}
        }
        body = rest.get(end + 1..).unwrap_or_default();
    }
    match detail {
        Some(detail) => anyhow!(
            "upstream Postgres {} {}: {} ({})",
            severity,
            code,
            message,
            detail
        ),
        None => anyhow!("upstream Postgres {} {}: {}", severity, code, message),
    }
}

pub fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub fn quote_literal(literal: &str) -> String {
    format!("'{}'", literal.replace('\'', "''"))
}

/// Format an LSN as `XXXXXXXX/XXXXXXXX`.
pub fn format_lsn(lsn: Lsn) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Parse an LSN formatted as `XXXXXXXX/XXXXXXXX`.
pub fn parse_lsn(s: &str) -> Option<Lsn> {
    let (high, low) = s.split_once('/')?;
    let high = u32::from_str_radix(high, 16).ok()? as u64;
    let low = u32::from_str_radix(low, 16).ok()? as u64;
    Some((high << 32) | low)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_format() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(0x16_B374_D848), "16/B374D848");
        assert_eq!(parse_lsn("16/B374D848"), Some(0x16_B374_D848));
        assert_eq!(parse_lsn("0/0"), Some(0));
        assert_eq!(parse_lsn("garbage"), None);
    }

    #[test]
    fn test_quote() {
        assert_eq!(quote_identifier("rw_pub"), "\"rw_pub\"");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn test_parse_error_response() {
        let body = b"SERROR\0C42704\0Mreplication slot \"s\" does not exist\0\0";
        let err = parse_error_response(body);
        assert_eq!(
            err.to_string(),
            "upstream Postgres ERROR 42704: replication slot \"s\" does not exist"
        );
    }
}
