# Planning failure of a filtered scan under a union (stream key of the scan)

- **Kind**: upstream bug (planner crash: assertion failure while planning a streaming job).
- **Affected versions**: upstream `v3.1.2` (since the extension of scan stream keys for snapshot
  backfill, upstream #25783); fixed in the fork builds of integration `v3.1.2` from the first one
  (commit of 2026-11-24).

## Symptom

A materialized view that reads, under a `UNION ALL`, a filtered scan of a materialized view whose
order key is not part of its stream key failed to plan with "distribution key must be a subset of
stream key before shuffle to a different distribution".

```sql
create table t (id int primary key, k int, v int);
create table u (id int primary key, k int);
create materialized view mv as select id, k, v from t order by v;
create materialized view m as select id from u union all select k from mv where v = 1;
-- error: distribution key must be a subset of stream key ...
```

## Cause

`logical_rewrite_for_stream` extends the stream key of a table scan with the primary key of the
table for snapshot backfill, and the operators above the scan are planned with it.
`LogicalScan::predicate_pull_up` rebuilt the scan with the default stream key, narrower than
planned.

## Fix

`predicate_pull_up` keeps the stream key of the scan it rebuilds. Part of commit `daef5b958c`
(squashed "fix(frontend): keep the scan stream key when pulling up its predicate") on branch
`fork/fix-snapshot-backfill`.

## Tests

Planner case "union with a filtered scan of a materialized view ordered by a non-stream-key
column" in `union.yaml`.

## Upstream status

Reported upstream as <https://github.com/risingwavelabs/risingwave/issues/28046> (open on
2026-11-28).
