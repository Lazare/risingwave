# `now()` near 2021 during the snapshot of a snapshot backfill

- **Kind**: upstream bug (streaming executor `Now`), transient wrong results and a stall.
- **Affected versions**: upstream `v3.1.2`; fixed in the fork builds of integration `v3.1.2`
  from the first one (commit of 2026-11-24).

## Symptom

During the snapshot phase of a job created with snapshot backfill, its barriers carry fake epochs
starting from zero, and `now()` was derived from them: it was 2021-04-01 while the snapshot was
read, then jumped to the real time on the first real epoch. A temporal filter such as

```sql
create materialized view recent as select * from events where ts > now() - interval '2 days';
```

let every row of the snapshot through, then retracted almost all of them inside a single barrier,
which stalled the job and, through backpressure, its upstream.

## Cause

`NowExecutor` computes `now()` from the epoch of each barrier, and the fake epochs of the
snapshot phase are not times.

## Fix

The meta node fills the snapshot epoch of the job in `NowNode`, as it does for the snapshot
backfill scans, and the executor uses it while the barrier epochs are lower. Part of commit
`daef5b958c` (squashed "fix(stream): evaluate now() at the snapshot epoch during snapshot
backfill") on branch `fork/fix-snapshot-backfill`.

## Tests

Unit test `test_now_snapshot_backfill_epoch` in `src/stream/src/executor/now.rs`.

## Upstream status

Not checked against `main`.
