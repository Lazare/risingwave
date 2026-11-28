# Selective barriers

| | |
|---|---|
| Kind | engine |
| Branch | `fork/selective-barriers` |
| Status | integrated (`integration/v3.1.2`), present in every plan-reduction image (rc4 to rc8) |
| Configuration | `[meta.developer] enable_selective_barrier`, default `false` |
| Design note | [`docs/dev/src/design/selective-barriers.md`](../../dev/src/design/selective-barriers.md) |

## Summary

Periodic barriers are delivered only to the fragments whose inputs may have produced data since
the previous barrier. The other fragments "sleep": they receive no barrier, write nothing, and
their state tables are still committed at every checkpoint ("virtual commit"), so that every
committed epoch stays a consistent snapshot of all tables.

## Motivation

In upstream RisingWave every barrier reaches every actor of a database. On a large graph
(hundreds of streaming jobs, thousands of actors) where most jobs are idle most of the time, the
cost of barriers alone dominates: one message per channel, one barrier handling per actor, one
seal per state table instance, and executors that write something at every barrier even without
data (`Now`, the barrier rows of the kv log store, snapshot backfill progress). The CPU of the
compute nodes and the barrier latency grow with the size of the graph, not with the amount of
data that changes.

## What it does

Synthetic example: tables `t1` and `t2` fed by DML, materialized views `mv1` over `t1` and `mv2`
over `t2`. Only `t1` receives an insert during an epoch.

- Without the feature: the barrier goes through the fragments of `t1`, `mv1`, `t2` and `mv2`.
- With the feature: the DML executor of `t1` holds the inserted rows until a barrier releases it
  and "rings" the meta node; the next barrier is delivered to `t1` and its descendants (`mv1`);
  `t2` and `mv2` sleep. At the checkpoint, the meta node commits every table at the new epoch,
  the sleeping ones without data.

Main rules (see the design note, sections "Rules" and "Why it stays consistent"):

- origins (DML, `Now`, shared CDC sources) gate their input and ring when data is pending;
- the woken set of a barrier is the set of descendants of the origins that released data at
  this barrier or at the previous one (the previous barrier's data needs a closing barrier);
- a woken merge does not wait for sleeping upstreams, a dispatcher skips sleeping downstreams;
- a fragment may fall asleep only after a checkpoint (needed with `checkpoint_frequency > 1`);
- every 30 barriers a barrier is delivered to every fragment, bounding the delay of a lost ring;
- an optional intermediate barrier at the middle of the interval halves the reaction time of a
  sleeping table that receives data.

Also part of the branch:

- `now()` advances only when its rounded value changes (`date_trunc('<unit>', now())` and
  deterministic expressions of it), so the descendants of a rounded `now()` sleep between two
  changes;
- shared CDC sources (design "D1"): the source routes rows per table and holds, in order, the rows
  of a sleeping table in its split state table, so that they are committed with the offset that
  covers them and replayed after a recovery;
- the kv log store of a decoupled sink no longer persists the barrier row of an epoch without
  data (independent of the selective mode).

## Configuration

`[meta.developer] enable_selective_barrier = true` in the configuration of the meta node. Off by
default. The mode is carried by the barriers themselves, so meta and compute nodes cannot
disagree; after a recovery barriers are normal until meta enables the mode again.

## Correctness / safety argument

A sleeping fragment received no data since its last barrier, so its state at any later epoch
equals its state at its last sealed epoch; committing it at the new epoch publishes exactly that
state. Woken fragments compute the new epoch from their woken inputs and the unchanged state of
sleeping inputs, which is what a full barrier would have produced. If a fragment nevertheless
wrote while sleeping, the state table refuses the epoch jump and the actor fails, which triggers
a recovery instead of publishing an inconsistent snapshot. Recovery needs nothing new: all tables
keep one committed epoch. See the design note, sections "Why it stays consistent", "Crash and
restart" and "Checkpoint frequency".

A fragment may sleep only if all its operators are input driven; the classification
(`is_input_driven` in `src/meta/src/barrier/checkpoint/selective.rs`) is an exhaustive match, so a
new operator must be classified explicitly.

## Status and versions

Integrated in `integration/v3.1.2` and in every image of the plan-reduction line (rc4 to rc8).
The branch is the sum of an earlier series of branches developed on v3.1.1, ported to v3.1.2.

## Tests

- SLT: `e2e_test/streaming/selective_barrier/` (`consistency.slt`, `freshness.slt`,
  `checkpoint_frequency.slt`, `now_rounding.slt`, `cdc/shared_cdc.slt`).
- Chaos and benchmark scripts: `scripts/selective_barriers/` (`chaos_test.py`,
  `cdc_chaos_test.py`, `sink_chaos_test.py`, `cdc_benchmark.py`, `run_tests.sh`).
- The validation workflow of the fork (see [ci-validation](ci-validation.md)) runs the streaming
  and batch SLT suites with the mode on, including `checkpoint_frequency > 1` profiles.

## Limits and risks

From the design note, section "Current limitations":

- connector sources (Kafka, shared sources with source backfill, tables with a connector), values
  and sinks receive every barrier, and so do their descendants;
- fragments with operators that are not input driven (watermark filter, delta join, unaligned
  join, source backfill, refreshable tables, ...) never sleep, nor do their descendants;
- barriers are full while a job is being created, and for graphs with cross-database snapshot
  backfill;
- a gated DML executor keeps the data of an epoch in memory until its table wakes up;
- a sleeping table becomes visible about one barrier later than without the feature;
- with `checkpoint_frequency = N > 1`, a woken fragment stays awake until the barrier after the
  next checkpoint, so fewer barriers are saved.

The feature touches the barrier protocol, the uploader of the state store and several executors:
it is the largest change of the fork and stays experimental (off by default).

## Upstream relation

Not proposed upstream. No equivalent is known in upstream RisingWave v3.1.2. The log store change
("no barrier row for an empty epoch") does not depend on selective barriers and could be proposed
on its own.
