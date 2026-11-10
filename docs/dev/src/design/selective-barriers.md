# Selective barriers with exempted upstreams

Status: design, work in progress on branch `feat/selective-barriers` (base `v3.1.1`).

## Goal

Today every barrier is delivered to every actor of a database, even when most of the
graph received no data during the epoch. On large graphs (hundreds of jobs, ~10k
actors) this costs, on every barrier:

- one barrier message per channel and one barrier handling per actor,
- one seal event per state table instance, handled by a single task per compute node,
- writes from executors that persist something on every barrier even without data
  (`Now`, kv log store barrier rows, snapshot backfill progress).

The goal is to deliver a barrier only to the fragments that may have changed, while
keeping the current guarantee: **every committed epoch is a consistent snapshot of all
tables**, and batch reads stay as fresh as today.

## Definitions

- **Origin**: a fragment through which data enters the graph: source (including a
  shared CDC source), DML (`DmlExecutor`), `Now`. Origins with pending data *ring*.
- **Woken set** `W_t` for barrier `t`: fragments that receive barrier `t`.
- **Sleeping set** `S_t`: all other fragments of the database.

## Rules

1. **Wake by descendants.** Let `R_t` be the origins that release data right after
   barrier `t` (they rang before it). `W_t` = descendants of `R_t ∪ R_{t-1}`, or every
   fragment when the barrier carries a mutation or when a periodic heartbeat is due.
2. **Closing barrier.** Data released after barrier `t-1` belongs to the epoch opened by
   `t-1`, which only barrier `t` can seal: hence `R_{t-1}` in rule 1. A fragment sleeps
   only after a barrier during which it received no data.
3. **Upward closure.** A sleeping fragment only has sleeping ancestors (otherwise it
   would be a descendant of a woken fragment). Hence a woken merge may have sleeping
   upstreams, but a sleeping merge never has a woken upstream.
4. **Gates.** A fragment in `S_t` never receives data: origins hold their input until
   a barrier wakes them (they ring meanwhile). `Now` only emits on barriers. Hence a
   sleeping fragment writes nothing and its state does not change.
5. **Exempted upstreams.** A woken merge does not wait for the barrier of a sleeping
   upstream fragment. A dispatcher does not send the barrier to sleeping downstream
   fragments. Watermarks of sleeping upstreams keep their last value.
6. **Virtual commit.** Every checkpoint commits *all* tables of the database at the
   new epoch: woken tables with their data, sleeping tables without data. All tables
   therefore keep a common committed epoch, which is what batch reads and recovery
   rely on.
7. **Transitions only at checkpoints.** `W_t` may differ from `W_{t-1}` only if barrier
   `t-1` is a checkpoint. The feature is enabled only with `checkpoint_frequency = 1`
   in a first version.

## Turning the mode on and off

The mode is carried by the barriers themselves, so meta and compute nodes cannot
disagree. Meta starts with a selective barrier that sleeps no fragment: every origin sees
it and switches to *gated* mode (hold input data until the next barrier it receives, ring
when data is pending). A barrier without selective information switches origins back to
the normal mode. After a recovery, barriers are normal until meta enables the mode again.

If a fragment nevertheless wrote while sleeping, the state table refuses the epoch jump
(`written_in_epoch`) and the actor fails, which triggers a recovery instead of publishing
an inconsistent snapshot.

## Why it stays consistent

A sleeping fragment received no data since its last barrier (rules 2 and 4), so its
state at any later epoch equals its state at its last sealed epoch. Committing it at
the new epoch (rule 6) publishes exactly that state. Woken fragments compute the new
epoch from their woken inputs and from the unchanged state of sleeping inputs, which is
what a full barrier would have produced, because a sleeping input would have emitted
no data anyway.

## Crash and restart

Recovery already requires all tables of a database to share one committed epoch
(`resolve_jobs_committed_epoch`) and restarts every actor from one `Initial` barrier.
With rule 6 this invariant keeps holding, so nothing about sleeping state needs to be
persisted: after a crash every fragment restarts woken and aligned. Data held by a gate
is source data not yet consumed: source offsets are only committed for consumed data,
and DML statements are only acknowledged as durable after `FLUSH` (a barrier commit).

## Changes by component

| Component | Change | Code |
|---|---|---|
| Meta: injection | compute `W_t`; inject only to actors of `W_t`; omit workers with no woken actor | `barrier/checkpoint/state.rs`, `barrier/rpc.rs` |
| Meta: commit | keep committing every table (virtual commit) | `barrier/command.rs` (`collect_commit_epoch_info`) |
| Proto | `Barrier.sleeping_fragment_ids`; sync set per barrier | `proto/stream_plan.proto`, `proto/stream_service.proto` |
| Compute: barrier worker | sync only tables of `W_{t-1}`; drop the empty open epoch of tables entering `S_t` | `task/barrier_worker/managed_state.rs` |
| Storage: uploader | "abandon empty open epoch" for a set of tables | `hummock/event_handler/uploader/mod.rs` |
| Merge | fragment-level exemption from `sleeping_fragment_ids` | `executor/mod.rs` (`DispatchBarrierBuffer`), `merge.rs` |
| Dispatcher | skip dispatchers whose downstream fragment sleeps | `executor/dispatch.rs` |
| Epoch checks | accept `prev > last` on wake-up only | `wrapper/epoch_check.rs`, `common/table/state_table.rs` |
| Origins | gate + ring for DML, `Now` | `executor/dml.rs`, `now.rs` |
| Shared CDC (D1) | routes per table sent by meta, per-table gate in the source, held rows in the split state table | `barrier/checkpoint/selective.rs`, `executor/source/cdc_router.rs`, `source_executor.rs` |
| Shared CDC (D4, superseded) | gate after a `CdcFilter` that has a state table | `executor/cdc_filter_gate.rs` |

## Current limitations

Enabled with `[meta.developer] enable_selective_barrier = true` (off by default).

- Tables fed by DML, descendants of `Now` and tables of a shared CDC source (with their
  `CdcFilter`) can sleep, including CDC tables created before the feature. Connector sources,
  values and sinks receive every barrier, and so do their descendants.
- A `CdcFilter` whose table name has the form `a.b.c` (legacy SQL Server tables, rewritten by the
  executor) is not routed: it receives every barrier and its table never sleeps.
- A graph containing cross-database snapshot backfill, or a table that receives a sink
  (`sink into table`), always receives full barriers.
- While a job is being created, barriers are full.
- `FLUSH` uses two barriers, so that data released by a fragment waking up at the first one is
  committed by the second.
- A gated DML executor keeps the data of an epoch in memory until the next barrier; a very large
  transaction on a sleeping table is held entirely until meta wakes the table up.
- Every 30 barriers, a barrier is delivered to every fragment, so that a lost ring delays data
  by at most 30 barriers.

## Steps

1. Baseline: consistency SLT and crash/restart chaos test (done).
2. Compute and storage protocol, inactive unless a barrier carries `selective` (done):
   `Barrier.selective`, merge exemption, epoch checks relaxed only for waking fragments
   and only without writes, uploader `abandon_epoch`, sync set of selective checkpoints.
3. Meta scheduler and DML origins (done): ring channel (compute to meta), gated
   `DmlExecutor`, computation of `W_t`, injection to woken actors only, virtual commit,
   meta config flag off by default. `Now` and connector sources stay always awake.
4. `Now` at its rounding granularity (done).
5. Shared CDC source, design D4 (done, superseded by D1): `CdcFilter` stays awake and gates
   its table.
6. Shared CDC source, design D1 (done): the source gates each table, `CdcFilter` fragments sleep.
   The broadcast to every `CdcFilter` remains (see below).
7. Connector sources (Kafka, shared sources with `SourceBackfill`, tables with connector).

Each step must keep `e2e_test/streaming/selective_barrier/consistency.slt` and
`scripts/selective_barriers/chaos_test.py` green, with the feature both on and off.

## `now()`

A `Now` fragment receives every barrier but only advances (state, output, watermark) on a barrier
that *releases* it (`Barrier.selective.released_fragment_ids`). Meta reads the rounding of the
`Project` that follows `Now` in the same fragment: for `date_trunc('<unit>', now())` it releases
`Now` only when the rounded time changes (period capped at 15 minutes, since time zone offsets
are multiples of 15 minutes); otherwise it releases it at every barrier. Between releases, the
descendants of `Now` sleep. The output cannot be held in the dispatcher instead: `Now` commits
its last timestamp at every barrier, and downstream dynamic filters reject the deletion of a value
they never received after a recovery.

Dispatchers do not send a barrier to a sleeping downstream fragment. Only released fragments emit
data towards fragments that may sleep, so no data is ever sent to a sleeping fragment.

## Shared CDC source: design options

Constraint: everything a source emitted before barrier `t` is covered by the offset it commits at
`t`, so it must be sealed by every consumer at `t`. Data held for a sleeping table across a
barrier is lost on crash, and pausing the source until the table wakes up stalls every table.
Today the shared source broadcasts every chunk to all table fragments, each filtering its own
table (`CdcFilter`), so without routing every event would wake every table.

| Design | Mechanism | Wake-up latency | Extra IO per checkpoint | Crash | Size |
|---|---|---|---|---|---|
| D1 | Route per table in the source; hold rows of sleeping tables, persist them in a source state table in the same epoch as the offset; replay after recovery | +1 barrier, no stall | no extra object | exactly-once | ~1.5-2.5k lines |
| D2 | Hold in memory, commit the offset behind held rows | +1 barrier | none | duplicates after recovery | ~1-1.5k |
| D3 | Durable per-table log between the source and each table, consumed lazily | +1 to +2 barriers | all rows stored, reads on wake | exactly-once | ~3-4k |
| D4 | Keep the broadcast; `CdcFilter` stays awake and gates its table like DML, persisting held rows | +1 barrier | no extra object | exactly-once | ~0.8-1.2k |
| D5 | Separate partial graph or database for CDC | - | - | breaks the common epoch | rejected |

Plan: D4 as a prototype (validates sleep, wake-up and recovery of the CDC chain without touching
the dispatcher; the `CdcFilter` actors stay awake), then D1 (removes the broadcast).

## Shared CDC source: D1

D1 moves the gate of D4 from each `CdcFilter` into the source, so that `CdcFilter` fragments can
sleep with their table, and needs no new state table, so it applies to existing CDC tables.

- **Routes.** For a shared CDC source whose consumers are all `CdcFilter` fragments, meta reads
  the table name of each `CdcFilter` (`_rw_table_name = '<name>'`) and sends the routes
  (table name -> `CdcFilter` fragment) with the first selective barrier after each reset of the
  scheduler (recovery or command, the only times the graph changes). The source keeps the last
  routes it received.
- **Sleeping `CdcFilter`.** A routed `CdcFilter` is scheduled like a gated origin: it sleeps
  unless released, and is released when its source rings for it. Its descendants follow.
- **Per-table gate in the source.** After a selective barrier, a row of a routed table is emitted
  only if the barrier released the `CdcFilter` of the table. Otherwise it is held, written to the
  split state table of the source under a reserved key (`__rw_held__/<fragment>/<seq>`, value:
  the value-encoded row), so that it is committed with the offset that covers it, and the source
  rings for the `CdcFilter`. After a barrier that releases it, held rows are emitted in order and
  deleted. Rows of tables without route pass through. A barrier without selective information
  releases everything.
- **Recovery.** The source reads held rows back after initializing its state table, and emits
  them after the first barrier that releases their table (barriers after a recovery are not
  selective, so the first one). Held rows of a table that is no longer routed (dropped) are
  deleted.
- **Broadcast.** The dispatcher still sends every chunk to every `CdcFilter`, including sleeping
  ones. This is safe: a sleeping `CdcFilter` only receives rows of other tables (the rows of its
  table are held by the source), which it filters out, so it emits and writes nothing. Routing
  rows in the dispatcher would only save CPU.

## Shared CDC source: D4 (superseded)

Graph of a CDC table: shared source (singleton) → `CdcFilter` fragment (singleton, `NoShuffle`,
keeps the rows of its table) → exchange → table fragment (`StreamCdcScan`, `Materialize`) →
downstream jobs.

- **Awake gated fragments.** A `CdcFilter` with a state table receives every barrier, since the
  source may send it data at any time. It only lets rows through during an epoch opened by a
  barrier that *releases* it; meta releases it only when it rang. Its descendants are thus woken
  exactly as for a gated origin (rule 1), the `CdcFilter` playing the role of the origin.
- **Silent roots.** A root fragment whose downstream fragments are all `CdcFilter` fragments (the
  shared CDC source) receives every barrier but releases nothing: each `CdcFilter` decides when
  its own descendants wake up. A source with any other consumer keeps releasing every barrier.
- **Hold and persist.** Rows arriving while the gate is closed are inserted in the state table
  `(seq, payload, _rw_offset, _rw_table_name)` and kept in memory. They are committed at the next
  barrier together with the source offset that covers them, so a crash cannot lose them. The gate
  rings once per epoch whenever it sees rows (also while open, so that a busy table stays
  released), and rings again at each barrier that does not release it while rows are held (a ring
  can be consumed by a barrier that cannot release it, e.g. a command barrier).
- **Release.** At a barrier that releases the fragment, held rows are emitted *after* the barrier,
  in order, and deleted from the state table in the same epoch. Meta delivers the next barrier to
  the table, which seals both the rows downstream and their deletion.
- **Recovery.** Held rows are read back at the first barrier, and emitted after the first barrier
  that releases the fragment (barriers after a recovery are not selective, so the first one).
- **Cost.** One more empty state table sealed per CDC table at every barrier, and the held rows
  written twice (insert, then delete one epoch later) when a sleeping table wakes up. A busy table
  stays released and writes nothing to the `CdcFilter` state table.
- **Creation.** The frontend no longer creates the state table since D1. A `CdcFilter` that has
  one and is not routed by its source keeps this behavior.

DML on a CDC table: the table fragment is not an origin but can now sleep, so its DML gate holds
DML data while it sleeps. Meta accepts rings from every fragment with a DML executor, which
releases the fragment and wakes its descendants.

A gate only emits held data after a barrier that wakes its fragment if this barrier also releases
the fragment. Otherwise (a full barrier planned before the ring reached meta, which can happen
when several barriers are in flight), the next barrier may not be delivered to the fragment and
would not close the epoch of the data: the gate keeps holding and rings again.

## Benchmark: shared CDC workload

`scripts/selective_barriers/cdc_benchmark.py`, on one machine (10 cores, debug build, local file
system object store), feature on and off on the same cluster and data (same storage, restarted
with the other profile), same workload and seed, 60 s warmup then 300 s measured.

Graph: one shared Postgres CDC source, 100 CDC tables, one aggregate view per table, a chained
view on every 4th view, 6 decoupled sinks: 232 jobs, 657 fragments, 1088 actors, parallelism 2.
Workload, shaped like the production profile (in a 30-checkpoint window: ~6% of the jobs written
at every checkpoint, ~4% at 30-95%, the rest rarely): 6% hot tables (an update every 0.5 s on
average), 4% warm (every 2 s), 90% cold (every 120 s, i.e. about 0.75 wake-ups per second over
the cold tables). Barrier interval 1 s, `checkpoint_frequency = 1`.

| Metric (per 300 s window) | off | on | on (repeat) |
|---|---|---|---|
| jobs written per checkpoint | 24.6 | 22.2 | 22.0 |
| state tables written per checkpoint | 42.2 | 37.5 | 37.1 |
| SST objects uploaded per checkpoint | 1.0 | 1.0 | 1.0 |
| object store ops, compute | 2659 | 2045 | 2899 |
| object store ops, compactor | 2973 | 13599 (*) | 2993 |
| barrier latency p50 / p99 (histogram buckets) | 0.25 s / 0.39 s | 0.10 s / 0.16 s | 0.10 s / 0.25 s |
| CPU, compute node | 55% | 28% | 30% |
| CPU, meta node | 10% | 14% | 11% |
| CDC freshness of a cold table, p50 / max | 1.0 s / 2.0 s | 2.0 s / 2.9 s | 2.0 s / 3.0 s |

(*) First run after the setup: compaction of the initial backfill. The repeat, run after the
"off" run, shows no difference.

Reading:

- The cost of delivering barriers is halved: compute CPU -45%, barrier latency p50 -60%.
- The data written barely changes (-10% of jobs and state tables written per checkpoint) and
  object store operations do not change measurably: an idle job writes nothing to Hummock with or
  without the feature. What writes at every checkpoint without data are mostly decoupled sinks
  (kv log store), which still receive every barrier.
- A cold table becomes visible about one barrier later (it rings, then is released).
- To reduce the Blob Storage operations of the production deployment, the next lever is to let
  decoupled sinks sleep when their upstream is idle (no log store write without data), and to
  check what else writes at every checkpoint without data (`checkpoint_writers.sql`).

Same graph, two more workloads (90 s warmup, 300 s measured; runs ordered on, on, off, off):
a single table updated once per second (`--hot-tables 1 --hot-interval 1 --periodic`, freshness
measured on that table), and no traffic at all.

| Metric (per 300 s window) | 1 table, off | 1 table, on | idle, off | idle, on |
|---|---|---|---|---|
| jobs written per checkpoint | 9.9 | 9.8 | 6.0 | 6.0 |
| state tables written per checkpoint | 12.8 | 12.7 | 6.0 | 6.0 |
| object store ops, compute + compactor | 1482 | 1509 | 1392 | 1356 |
| barrier latency p50 / p99 | 0.25 s / 0.39 s | 0.10 s / 0.10 s | 0.25 s / 0.39 s | 0.10 s / 0.10 s |
| CPU, compute node | 49% | 15% | 46% | 11% |
| CPU, meta node | 9.5% | 9.2% | 9.1% | 9.8% |
| freshness p50 / max | 1.0 s / 2.0 s | 0.9 s / 2.0 s | - | - |

- Without traffic, the 6 jobs written at every checkpoint are exactly the 6 decoupled sinks, with
  or without the feature: they are the only writers without data.
- The cost of barriers alone is large without the feature (46% of a core for 1088 actors in a
  debug build, idle) and drops by about 75% with it.
- A table with continuous traffic keeps its freshness: it rings at every epoch and stays released.

## Decoupled sinks: no barrier row for an empty epoch

The benchmark without traffic shows that the only writers left are decoupled sinks: at every
barrier, the kv log store writes one barrier row per vnode, even when the epoch has no data. The
reader of the log store always receives barriers from its in-memory buffer; persisted barrier
rows are only read when the log is replayed from the state store (start, rewind, vnode change),
where the epoch after a barrier only has to be greater than it. The writer therefore no longer
persists the barrier of an epoch without data (as it already did while paused), except for
barriers carrying a schema change or a stop. A replay then sees one barrier less per skipped
epoch; a sink that commits every N checkpoints may commit a little later after a recovery.

This change does not depend on selective barriers. Benchmark without traffic (same graph, 6
decoupled sinks, 300 s):

| | before | after |
|---|---|---|
| checkpoints with writes | 301 | 1 |
| jobs / state tables written per checkpoint | 6 / 6 | 1 / 1 (only one checkpoint) |
| object store ops, compute + compactor | ~1390 | 1 |

`scripts/selective_barriers/sink_chaos_test.py` checks a decoupled upsert sink into Postgres with
bursts separated by idle periods and random kills (15 rounds with and without selective barriers).

## Benchmark with all changes

Same graph and workloads as above, with the log store change, on a fresh cluster (setup, 3 min
pause, then runs ordered off: mix, one table, idle; on: idle, one table, mix). Per 300 s window:

| Load | Mode | Jobs / state tables written per checkpoint | Checkpoints with writes | Object store ops, compute / compactor | Barrier p50 / p99 | CPU compute / meta | Freshness p50 / max |
|---|---|---|---|---|---|---|---|
| mix | off | 23.9 / 41.5 | 301 | 2524 / 2882 | 0.25 s / 0.39 s | 54% / 10% | 1.0 s / 2.1 s |
| mix | on | 20.5 / 35.6 | 300 | 2807 / 1010 | 0.10 s / 0.16 s | 27% / 9% | 2.0 s / 2.9 s |
| 1 table, 1/s | off | 5.0 / 8.0 | 291 | 512 / 1498 | 0.25 s / 0.25 s | 46% / 9% | 0.9 s / 2.1 s |
| 1 table, 1/s | on | 4.9 / 7.8 | 293 | 520 / 1002 | 0.10 s / 0.10 s | 14% / 10% | 1.0 s / 2.0 s |
| idle | off | 1.0 / 1.0 | 1 | 1 / 0 | 0.25 s / 0.39 s | 45% / 8% | - |
| idle | on | 1.0 / 1.0 | 1 | 1 / 0 | 0.10 s / 0.10 s | 10% / 9% | - |

Compactor operations depend on the backlog left by the previous run (the first run after a heavy
one is penalized) and are not comparable between runs of a 5-minute window; compute operations
(uploads and reads) are.
