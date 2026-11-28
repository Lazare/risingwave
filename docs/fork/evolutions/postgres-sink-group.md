# Sink groups for Postgres sinks

| | |
|---|---|
| Kind | feature (connector) |
| Branch | `fork/postgres-sink-group` |
| Status | integrated (`integration/v3.1.2`), present in rc4 to rc8 |
| Configuration | `CREATE CONNECTION ... (type = 'postgres', pool.max_size = '4')`, sink options `connection`, `sink_order = 'last'` |
| Design note | [`docs/dev/src/design/sink-group.md`](../../dev/src/design/sink-group.md) |

## Summary

Postgres sinks that reference the same Postgres connection form a group: they share one bounded
connection pool per compute node, and a sink of the group may be declared `last`, in which case it
writes the data of an epoch only after every other sink of the group has committed that epoch.

## Motivation

Several sinks often write into the same Postgres database. Upstream, each sink actor opens its own
connection (many connections for many sinks times their parallelism), a broken connection fails
the sink, and nothing orders the sinks: a reader of the target database cannot tell whether the
tables are complete up to some point. A marker table written last gives readers that point.

## What it does

```sql
CREATE CONNECTION pg_target WITH (
    type = 'postgres', host = '...', port = '5432', user = '...', password = '...',
    database = 'target', pool.max_size = '4');

CREATE SINK s_f FROM f WITH (
    connector = 'postgres', connection = pg_target, table = 'f',
    type = 'upsert', primary_key = 'id');

CREATE SINK s_marker FROM marker WITH (
    connector = 'postgres', connection = pg_target, table = 'marker',
    type = 'upsert', primary_key = 'id', sink_order = 'last');
```

- Pool (`sink/postgres_pool.rs`): one pool per process, group and settings; a writer takes a
  connection for one chunk; broken connections are replaced and the chunk is retried up to 5 times
  with backoff before the sink fails.
- Ordering (`sink/sink_group.rs`): each sink actor has a frontier (epochs strictly below it are
  committed in Postgres); a `last` sink waits, without polling, until every other member's
  frontier passes the epoch it is about to write.
- Placement: the sinks of a group get their own fragment and are placed on one compute node chosen
  by rendezvous hashing and remembered by the meta node, because the pool and the frontiers are
  local to a process.
- The Postgres sink checks its target table on a connection of the pool.

## Configuration

- Connection type `postgres` with `host`, `port`, `user`, `password`, `database`, `ssl_mode`,
  `ssl.root.cert`, `tcp.keepalive.*`, `pool.max_size` (default 4).
- Sink option `connection = <name>`; the frontend adds the reserved option `sink_group`.
- Sink option `sink_order = 'last'`, which requires a connection.
- A sink without connection keeps its behavior, with a private pool of one connection.

## Correctness / safety argument

The ordering is at-least-once: when a `last` sink writes epoch `E`, every other sink of the group
has committed `E` at least once. Members are registered when the actors are spawned, before any of
them reads its log, so a `last` sink also waits for sinks that replay their log after a recovery.
See the design note, sections "Ordering", "Membership" and "Guarantee and limitations".

## Status and versions

Integrated in `integration/v3.1.2`; present in rc4 to rc8.

## Tests

- Unit tests of the registry (`sink_group.rs`), the pool (`postgres_pool.rs`) and the placement
  (`src/meta/src/controller/scale.rs`).
- `scripts/sink_group/sink_group_test.py`: end-to-end test with a Postgres target (connections,
  placement, ordering under kills of the compute node, delay of a `last` sink, no writes without
  traffic; `--no-last` as a control run).

## Limits and risks

- After a recovery a sink may replay epochs the `last` sink has already written: a strict order
  would need one transaction across all sinks of the group.
- All sinks of a group run on one compute node: the write throughput of the group is that node's.
- Members are compared by epoch: same database, and no sink of the group created with snapshot
  backfill.
- A slow or late sink holds back the `last` sinks of its group (a warning is logged every 30 s).
- Sinks created before the feature keep their placement until recreated; altering a connection
  does not move its sinks to another group.

## Upstream relation

Not proposed upstream. Upstream v3.1.2 has Postgres sinks with a connection per actor and no
ordering between sinks.
