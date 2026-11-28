# Unread `first_value` / `last_value` columns kept in memory

- **Kind**: upstream bug (optimizer), excessive resource use: a batch query could hold a whole
  table in memory. Found on a workload of deduplicating views.
- **Affected versions**: upstream `v3.1.2`; fixed in the fork builds of integration `v3.1.2`
  from the first one (commit of 2026-11-24).

## Symptom

A view that deduplicates a table with one `last_value(col ORDER BY ts)` per column, grouped by an
entity key, queried for one column only, kept every column in the aggregation state, large
`jsonb` payloads included: a batch query over a table of about 12 GB held all of it in memory.

```sql
create table e (entity int, ts timestamp, a int, payload jsonb);
create view latest as
  select entity, last_value(a order by ts) as a, last_value(payload order by ts) as payload
  from e group by entity;
select entity, a from latest;
-- `payload` was scanned, shuffled and kept in the aggregation state although nothing reads it
```

## Cause

`UnifyFirstLastValueRule` packs the `first_value` / `last_value` calls of an aggregation that
share the same ordering into one call over a row. It ran before the first column pruning, so the
unread calls were packed too, and a field of the row can no longer be pruned.

## Fix

The columns are pruned before the aggregation simplification stage, in batch and in streaming
plans; plans whose calls are all read do not change. Part of commit `5b0ef92d9b` (squashed
"fix(optimizer): prune unused first_value/last_value before unifying them") on branch
`fork/optimizer`. The same commit also adds pruning of empty relations and contradictory NULL
filters in batch plans, which are optimizations, not corrections.

## Tests

Planner cases in `unify_first_last_value.yaml`.

## Upstream status

Not checked against `main`.
