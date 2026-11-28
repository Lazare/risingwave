# Native engine for Postgres CDC sources

| | |
|---|---|
| Kind | feature (connector) |
| Branch | `fork/pg-cdc-native-engine` |
| Status | integrated (`integration/v3.1.2`), present in rc4 to rc8 |
| Configuration | source option `cdc.engine = 'native'` (default `debezium`) |

## Summary

A `postgres-cdc` source can read the logical replication stream of the upstream database in Rust
(`pgoutput`, protocol version 1) instead of running the Debezium connector in the embedded JVM.
The source is validated against the upstream database in Rust as well. The engine produces the
same messages and offsets as the Debezium based engine, so everything downstream of the reader
(parser, routing of the shared source, backfill, offset commit) is shared by the two engines.

## Motivation

The Debezium engine runs in a JVM inside the compute node: it needs a Java runtime in the image,
JVM memory next to the Rust process, and a JNI hop for every event and every offset commit. For a
deployment whose only CDC upstream is Postgres, a Rust reader removes that dependency from the
data path. Not documented further in the branch.

## What it does

```sql
CREATE SOURCE pg_src WITH (
    connector = 'postgres-cdc',
    hostname = '...', port = '5432', username = '...', password = '...',
    database.name = 'app', slot.name = 'rw_slot',
    cdc.engine = 'native'
);
CREATE TABLE t1 (id int PRIMARY KEY, v text) FROM pg_src TABLE 'public.t1';
```

- `src/connector/src/source/cdc/native/`: a minimal client of the streaming replication protocol
  (`protocol.rs`, because `tokio-postgres` lacks the `CopyBoth` mode), a `pgoutput` decoder, a
  translator to Debezium-shaped messages and offsets (`lsn`, `lsn_commit`, `lsn_proc`), the
  conversion of Postgres text values to the JSON that Debezium produces under RisingWave's
  settings, and the validation (`wal_level`, slots, privileges, publication, table existence,
  primary key and column types) with the same error messages as the connector node.
- The last change of a transaction is held back until its commit is seen, so that
  `(lsn_commit, lsn_proc)` never goes backwards and a restart resumes inside a large transaction
  without loss.
- The consumed position is confirmed to the upstream after a checkpoint, also while the captured
  tables are idle (heartbeat).
- Offsets of a source read by the native engine are committed by the native reader, not through
  the JVM (`WaitCheckpointTask` in `src/connector/src/source/mod.rs`).

## Configuration

`cdc.engine` on the source: `debezium` (default) or `native`. The frontend rejects `native` for
any connector other than `postgres-cdc`, and rejects `auto.schema.change = 'true'` with `native`.

## Correctness / safety argument

The engine is opt-in per source, and the output format is the one of the Debezium engine, so the
parser and the downstream executors are unchanged. The e2e tests ingest the same upstream tables
with both engines and compare the results. Exactly-once relies on the same offset protocol as
Debezium: the stream restarts from `lsn_commit`, and changes before `lsn_proc` in the next
transaction are skipped.

## Status and versions

Integrated in `integration/v3.1.2`; present in every plan-reduction image (rc4 to rc8).

## Tests

- `e2e_test/source_inline/cdc/postgres/postgres_native_engine.slt`: all types, NULLs, special
  dates, unchanged TOAST values, primary key updates, deletes, a transaction across two tables,
  replica identity FULL, confirmation of the position while idle; results equal to the Debezium
  engine.
- `postgres_native_engine_extensions.slt`: pgvector vectors and PostGIS geometries.
- `postgres_native_engine_validate.slt`: validation errors, publication handling, partitioned
  tables.
- `postgres_native_engine_recovery.slt.serial`: restart while idle and in the middle of a large
  transaction.
- Unit tests in the `native` modules.

## Limits and risks

- Postgres only; schema change events (`auto.schema.change`) are not produced.
- Infinite dates and timestamps are not representable (NULL, as with Debezium).
- A geometry without SRID is ingested by the native engine where the Debezium engine gives NULL
  (documented in the extensions test).
- The replication connection expects `DateStyle=ISO`, `TimeZone=UTC`, `IntervalStyle=iso_8601`.
- A new protocol client is a new surface for subtle bugs (authentication methods, protocol
  versions); only `pgoutput` protocol version 1 is decoded.

## Upstream relation

Not proposed upstream. Upstream v3.1.2 reads Postgres CDC through Debezium only. Whether upstream
plans a native reader is not documented here.
