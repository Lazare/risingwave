# Subqueries over set-returning functions, self-aggregate join back

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/plan-reduction-lateral` (streaming), `proto/plan-reduction-batch-unnest` (batch) |
| Status | prototype; streaming in rc4 (with a fix in rc5), batch in rc5; present in rc8 |
| Variables | `streaming_unnest_lateral_functions` (false), `batch_unnest_lateral_functions` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), ideas 9b and 9c |

## Summary

- 9b (`LateralTableFunctionRule`, `UnnestArrayOfTableFunctionRule`): a correlated subquery that reads
  no table, only columns of the outer row through set-returning functions (`LEFT JOIN LATERAL`,
  `ARRAY(SELECT srf(..))`, an aggregate or a window over them), is evaluated on each outer row with a
  `ProjectSet` instead of the general decorrelation.
- 9c (`SelfAggregateJoinRule`): a table left-joined on its whole key with an aggregation of rows
  derived from itself (through set-returning functions, filters and joins) computes the aggregation
  where the left side reads the table, without the second read and the join.

## Motivation

Extracting values from JSON or text per row (`jsonb_array_elements`, `regexp_matches`, `unnest`) in a
lateral subquery is common. The general decorrelation re-reads the outer side, aggregates its
distinct values into a domain, applies the functions to the domain and joins back: several scans,
aggregations and joins for what is a per-row computation.

## What it does

```sql
CREATE TABLE t1 (id int PRIMARY KEY, s varchar, a int[]);
SELECT t1.id, x.m FROM t1 LEFT JOIN LATERAL
  (SELECT (regexp_matches(t1.s, 'A([0-9]+)'))[1] AS m) x ON true;
-- before: two scans of t1, a domain aggregation, a ProjectSet and a hash join
-- after:  one scan of t1 and a stateless ProjectSet
SELECT t1.id, (SELECT max(v) FROM unnest(t1.a) v) FROM t1;
-- after:  ProjectSet over t1, aggregation grouped by the key of t1
```

An outer `ProjectSet` (keeping the row when the functions return nothing) does not exist in
RisingWave; the rule zips `generate_series(1, 1)` with the functions to obtain the padded row, and
uses twins that never return `NULL` to tell padded rows apart.

## Configuration

Both variables default false. `batch_unnest_lateral_functions` also enables 9c in batch.

## Correctness / safety argument

The padded row is exactly the row a left join would keep; a function that fails on a row returns no
row in both forms. Aggregates are filtered on non-padded rows so that an empty set gives the value
over an empty input. In streaming `sum` becomes `CASE WHEN count(x) > 0 THEN sum(x) END` to avoid the
streaming `sum` that returns 0 after retractions. 9c requires the compared column to be the whole
unique key of the table, proven from its definition. A fix shipped in rc5 keeps an outer side with a
`UNION ALL` (whose logical stream key is not yet unique) in the general way.

## Status and versions

Streaming in rc4, the `UNION ALL` key fix and batch since rc5; present in rc8.

## Tests

Planner `stream_unnest_lateral_functions.yaml`, `batch_plan_reduction_unnest.yaml`; differential
`e2e_test/streaming/unnest_lateral_functions.slt`, `e2e_test/batch/plan_reduction_unnest.slt`.

## Limits and risks

- Kept in the general way: conditions in the subquery, a subquery that reads a table, `LIMIT`,
  `DISTINCT` aggregates, aggregates outside a short list, user-defined table functions.
- Functions are evaluated per row rather than per distinct value (equal for deterministic functions).
- In batch the per-row aggregation needs every leaf of the outer side to be a table scan.

## Upstream relation

Not present in upstream RisingWave v3.1.2. Possible upstream candidate; not proposed.
