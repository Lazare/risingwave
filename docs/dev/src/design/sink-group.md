# Sink groups: shared connection pool and `last` sinks

Status: first version on branch `feat/sink-group` (base `feat/selective-barriers`), Postgres sink
only.

## Goal

Several sinks often write to the same Postgres database. Today each sink actor opens its own
connection, and the sinks are independent: nothing says in which order they write the data of an
epoch.

A sink group provides:

- **a shared connection pool**: the sinks of a group take their connections from one bounded pool
  per compute node, which also replaces broken connections;
- **`last` sinks**: a sink of the group can be declared last. It writes the data of an epoch only
  after every other sink of the group has committed that epoch. Typical use: a marker table that
  tells readers of the target database that the other tables are complete up to some point.

## Usage

```sql
CREATE CONNECTION pg_main WITH (
    type = 'postgres', host = '...', port = '5432', user = '...', password = '...',
    database = '...', pool.max_size = '4');

CREATE SINK orders_sink FROM orders WITH (
    connector = 'postgres', connection = pg_main, table = 'orders',
    type = 'upsert', primary_key = 'id');

CREATE SINK marker_sink FROM marker WITH (
    connector = 'postgres', connection = pg_main, table = 'marker',
    type = 'upsert', primary_key = 'id', sink_order = 'last');
```

- The group of a sink is its connection: the frontend adds the option `sink_group` (reserved,
  rejected when given by the user) to a sink that references a Postgres connection.
- Connection properties: `host`, `port`, `user`, `password`, `database`, `ssl_mode`,
  `ssl.root.cert`, `tcp.keepalive.*`, `pool.max_size` (default 4). They cannot be repeated in the
  sink.
- `sink_order = 'last'` requires a connection. Several sinks of a group can be last; they are not
  ordered between themselves.
- A sink without connection behaves as before, with a private pool of one connection (it gains
  the reconnection described below).

## Connection pool

`sink/postgres_pool.rs`. One pool per process, group and connection settings, kept alive by the
sink writers that use it.

- A writer takes a connection for one chunk (one transaction) and gives it back. At most
  `pool.max_size` transactions run at the same time on a compute node for the group; the other
  writers wait.
- Statements are prepared once per connection and cached.
- A closed connection is dropped when it is taken from the pool and a new one is opened.
- When a write fails because the connection broke (or cannot be opened), the chunk is written
  again on another connection, up to 5 times with an exponential backoff (about 6 s), before the
  sink fails as before. The failed transaction is rolled back by Postgres, unless the failure
  happened during the commit: the chunk may then be written twice, which the sink already allowed
  (at-least-once).

## Ordering

`sink/sink_group.rs`. Each sink actor of a group is a member with a **frontier**: all its data of
epochs strictly lower than the frontier is committed in Postgres. The Postgres sink commits each
chunk before reading the next item of its log, so the frontier follows the log reader:

- a chunk of epoch `E` is read: the frontier becomes `E`;
- the barrier of epoch `E` is read: the frontier becomes `E + 1`.

Before writing a chunk of epoch `E`, a last sink waits until the frontier of every member that is
not last is greater than `E`. It is woken up by a notification when a frontier moves; there is no
polling.

Consequences:

- **A sink without data costs nothing and does not delay the last sinks.** It still receives every
  barrier from the in-memory buffer of its log store (even though the barrier of an empty epoch is
  no longer persisted), so its frontier advances in memory at every barrier, without any write to
  the state store or to Postgres.
- **A last sink writes after the end of the epoch.** It cannot know that the other sinks have
  nothing more for epoch `E` before they see the barrier of `E`: its data is delayed by at most
  one barrier interval when the others are idle, and by the time the slowest sink of the group
  needs to commit the epoch otherwise.
- A last sink without data does nothing.

### Membership

A last sink must also wait for the sinks that have not started consuming their log, in particular
after a recovery, where every sink replays the end of its log. The members are therefore
registered when the actors are spawned (`StreamActorManager::spawn_actor`), from the `sink_group`
option of the sink nodes of the fragment, with an empty frontier. All the actors of a recovery, or
of a new sink, are spawned from the same barrier request, before any of them can read its log.
The registration is dropped with the actor.

A sink being created, or late for any reason (slow target, rate limit, pause), holds back the last
sinks of its group until it has caught up. A warning is logged every 30 s while a last sink waits,
with the sink and the actor it waits for.

## Placement

The pool and the ordering only work between sink actors of the same process, so the sinks of a
group always run on the same compute node:

- **Own fragment.** The `Sink` node of `CREATE SINK ... FROM t` normally shares its fragment with
  the scan of `t`, which is no-shuffle with `t` and therefore placed with the actors of `t` on all
  the nodes (the `resource_group` of the sink does not apply to it). For a sink of a group, the
  planner adds a shuffle before the sink (`StreamSink::create`), so that the sink has
  its own fragment.
- **Single worker.** When the meta node places a fragment that holds a sink of a group
  (`EnsembleActorTemplate::render_new`, used when the job is created, rescheduled or recovered),
  it only considers one worker of the resource group of the sink, chosen by
  `choose_sink_group_worker`:
  - the first time a group is placed, the worker is chosen by hashing the group with each worker
    (rendezvous hashing), so that the groups are spread over the workers;
  - the choice is remembered by the meta node and kept as long as the worker is available. A
    choice computed only from the set of workers could not both spread the groups and stay the
    same when a worker is added, and the sinks of a group are not always placed at the same time
    (a new sink, a reschedule of one job);
  - when the worker is lost, the recovery places every sink of the group again; the first one
    chooses another worker and the others follow;
  - the choice is only kept in memory: after a restart of the meta node, the recovery places all
    the jobs again with the same set of workers, so the sinks of a group agree again (possibly on
    another worker than before).

The parallelism of the sink is unchanged: its actors all run on that node. To choose the node,
give the sinks of the group a `resource_group` with a single compute node. The sinks of a group
must use the same resource group: the choice is made per group and resource group.

Not covered: a sink created before this rule keeps its placement until it is recreated.

## Guarantee and limitations

- **At-least-once ordering.** When a last sink writes epoch `E`, every other sink of the group has
  committed `E` at least once. After a recovery, a sink may replay epochs that the last sink has
  already written: its target table goes back in time for the keys of these epochs, then catches
  up. A strict order would require writing all the sinks of the group in one transaction.
- **One compute node per group.** The registry and the pool are local to a process, so the sinks
  of a group are all placed on one compute node, see [Placement](#placement). The write throughput
  of the group is the one of this node.
- **Same epochs.** Members are compared by epoch, so they must receive the same barriers: same
  database (a connection belongs to a database), and no sink of the group created with snapshot
  backfill.
- **Sinks that sleep.** Sinks receive every barrier, also with selective barriers. If sinks were
  allowed to sleep, the registry would have to learn, for every barrier, that a sleeping sink is
  not part of it.
- Altering a connection does not move its sinks to another group or pool.

## Tests

- Unit tests of the registry (`sink_group.rs`) and of the pool sharing (`postgres_pool.rs`).
- `scripts/sink_group/sink_group_test.py`: end-to-end test with a Postgres target. It checks the
  number of connections, the placement of the sinks on one compute node, the order (a trigger on the marker table records, for every marker
  write, whether the rows of the batch are present in the other tables, with a slow sink and kills
  of the compute node), the delay added to a last sink when the others are idle, and that nothing
  is written without traffic. `--no-last` is a control run where violations are expected.

### Results

One compute node (parallelism 4, debug build), local Postgres, barrier interval 1 s, 5 sinks (2
data sinks of the same table, one of them slowed down by 0.3 s per statement, 1 marker sink, 2
ping sinks), `pool.max_size = 3`.

| Check | Result |
|---|---|
| Postgres connections, sinks without / with the connection | 20 / 3 |
| Markers written before the rows of their batch, with `last` (30 to 41 batches per run, 1 to 5 kills of the compute node, selective barriers on and off) | 0 |
| Same, control run without `last` (16 batches) | 16 |
| Delay of a `last` sink after a normal sink of the same table, other sinks idle | p50 1 to 50 ms, max 1.0 s (one barrier interval) |
| Connections closed in Postgres, idle or in a transaction | sinks go on, no recovery of the graph |
| 30 s without traffic | 0 write to Postgres, 0 checkpoint with writes out of 30 |

With two compute nodes (parallelism 4 each): the upstream tables run on both nodes and the sinks
of the group on one, before and after the kills; 40 connections without the connection, 3 with it;
0 marker out of 33 written before its rows.

Loss of the compute node that runs the sinks (killed and not restarted, two nodes): all the sinks
of the group moved together to the other node about 64 s after the kill, then 5 batches were
written with no marker ahead of its rows; when the node came back, the sinks were again all on
one node.

Unit tests of the placement in `controller/scale.rs`: the choice is kept when workers are added
or come back, changes when the worker is lost, spreads the groups, and two sinks of a group
rendered with other jobs get the same single worker.

`scripts/selective_barriers/sink_chaos_test.py` (a sink without connection) still passes.
