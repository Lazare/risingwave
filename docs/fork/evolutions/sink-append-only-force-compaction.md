# `force_compaction` for `force_append_only` sinks

| | |
|---|---|
| Kind | feature (sink) |
| Branch | `fork/sink-append-only-force-compaction` |
| Status | integrated (`integration/v3.1.2`), present in rc4 to rc8 |
| Configuration | sink options `type = 'append-only'`, `force_append_only = 'true'`, `force_compaction = 'true'` |

## Summary

An append-only sink over a non-append-only input (`force_append_only = 'true'`) can now honor
`force_compaction = 'true'`: the sink executor buffers the chunks of the whole epoch, compacts them
on the stream key, and only then drops the remaining deletes and turns updates into inserts. A row
inserted and retracted within the same epoch is no longer emitted.

## Motivation

Upstream, `force_compaction` is only used by non-append-only sinks. With `force_append_only`, the
sink drops the `DELETE` half of every change: a row that is inserted and then deleted (or updated
several times) between two barriers still reaches the downstream system as one or several inserts
of transient values. Consumers of an append-only stream then see rows that never existed at any
committed epoch.

## What it does

Synthetic example: table `t1(id int primary key, v int)`, sink
`CREATE SINK s FROM t1 WITH (connector = '...', type = 'append-only', force_append_only = 'true',
force_compaction = 'true')`. Within one epoch: insert `(1, 10)`, `(2, 20)`, `(3, 30)`, delete
`id = 2`, update `id = 3` to `v = 100`.

- Before: the sink emits inserts `(1, 10)`, `(2, 20)`, `(3, 30)`, `(3, 100)`.
- After: the sink emits inserts `(1, 10)`, `(3, 100)`.

Code: `compact_before_ignore_delete` in `src/stream/src/executor/sink.rs`, enabled only when the
sink type is append-only, `ignore_delete` is set and `force_compaction` is true.

## Configuration

`force_compaction = 'true'` on an append-only sink with `force_append_only = 'true'`. Without
`force_compaction` the behavior is unchanged. For a genuinely append-only input nothing is
buffered.

## Correctness / safety argument

The compaction is the one already used for non-append-only sinks with `force_compaction`, applied
per epoch on the stream key, before the existing `ignore_delete` conversion. The net change of each
key over the epoch is what the downstream receives.

## Status and versions

Integrated in `integration/v3.1.2`; present in rc4 to rc8.

## Tests

`e2e_test/sink/force_compaction_sink.slt` (case with a blackhole sink, extended on the branch).
The test checks that the sink is accepted and runs; it does not inspect the emitted rows.

## Limits and risks

- The chunks of an epoch are held in memory until the barrier (`EstimatedVec`), as for the other
  compacting sinks.
- The test does not assert the content of the output (blackhole connector).

## Upstream relation

Small and self-contained: a plausible upstream PR candidate. Not proposed yet.
