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
| Origins | gate + ring for DML, `Now`, sources, shared CDC source (per output) | `executor/dml.rs`, `now.rs`, `source/*` |

## Steps

1. Baseline: consistency SLT and crash/restart chaos test (done).
2. Compute and storage protocol, inactive unless a barrier carries `selective` (done):
   `Barrier.selective`, merge exemption, epoch checks relaxed only for waking fragments
   and only without writes, uploader `abandon_epoch`, sync set of selective checkpoints.
3. Meta scheduler and DML origins: ring channel (compute to meta), gated
   `DmlExecutor`, computation of `W_t`, injection to woken actors only, virtual commit,
   meta config flag off by default. `Now` and connector sources stay always awake.
4. `Now` at its rounding granularity, then connector sources, then the shared CDC
   source with per-output gating and dispatcher skip.

Each step must keep `e2e_test/streaming/selective_barrier/consistency.slt` and
`scripts/selective_barriers/chaos_test.py` green, with the feature both on and off.
