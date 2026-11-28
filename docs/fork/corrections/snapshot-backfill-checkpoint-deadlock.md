# Snapshot backfill deadlock with `checkpoint_frequency` > 1

- **Kind**: upstream bug (streaming executor), hang: the barriers of the cluster stop being
  collected.
- **Affected versions**: upstream `v3.1.2`; fixed in the fork builds of integration `v3.1.2`
  from the first one (commit of 2026-11-24).

## Symptom

With non-checkpoint barriers (`checkpoint_frequency` > 1), creating a streaming job with snapshot
backfill could stop the collection of barriers of the main graph: no epoch is committed any more.
It needs two scans of the same upstream in the same job, one still consuming the log store and
one already consuming the upstream directly, joined downstream (for example a self-join of a
table in a new materialized view).

## Cause

While consuming the log store, `UpstreamBuffer::consumed_epoch` waited for the next checkpoint
barrier of the upstream before consuming the barrier of an epoch whenever no checkpoint barrier
was pending. The other scan, back-pressured by its downstream that waits for this barrier
(through the join), blocks the upstream actor, which never emits the checkpoint barrier.

## Fix

Only the upstream barrier of the epoch being consumed is awaited: pending non-checkpoint barriers
are consumed without the checkpoint barrier that ends their group. Part of commit `daef5b958c`
(squashed "fix(stream): do not wait for the next upstream checkpoint in snapshot backfill") on
branch `fork/fix-snapshot-backfill`.

## Tests

Unit test `test_consumed_epoch_does_not_wait_for_next_upstream_checkpoint` in
`src/stream/src/executor/backfill/snapshot_backfill/executor.rs`.

## Upstream status

Not checked against `main`. Upstream has other open snapshot backfill deadlock reports (for
example <https://github.com/risingwavelabs/risingwave/issues/26738>), not known to be the same.
