# Status ladder picked with one `CASE`

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/plan-reduction-union-scans` |
| Status | prototype in rc5; present in rc8 |
| Variables | `streaming_status_ladder_case` (false), `batch_status_ladder_case` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "SL" |

## Summary

`StatusLadderCaseRule`: under `row_number() OVER (PARTITION BY k ORDER BY priority DESC) = 1` over a
`UNION ALL` of branches `SELECT k, '<status>', <priority> FROM ... WHERE ...`, the branches that
filter and project the same relation with the same key become one branch that picks, per row, the
literals of the highest-priority branch whose filter holds. When nothing else is left and the
relation is unique on the key, the top-n is removed.

## Motivation

Same pattern as in [lookups under dedup](lookups-under-dedup.md): a status computed as the best of
several candidate branches over the same table. Each branch adds a scan and rows to the top-1.

## What it does

```sql
CREATE TABLE t (k int PRIMARY KEY, a bool, b bool);
SELECT k, status FROM (
  SELECT k, status, row_number() OVER (PARTITION BY k ORDER BY prio DESC) AS rn FROM (
    SELECT k, 'x' AS status, 3 AS prio FROM t WHERE a
    UNION ALL SELECT k, 'y', 2 FROM t WHERE b
    UNION ALL SELECT k, 'z', 1 FROM t) u) r
WHERE rn = 1;
-- after: SELECT k, CASE WHEN a THEN 'x' WHEN b THEN 'y' ELSE 'z' END FROM t
--        (no top-n: one branch left and t is unique on k)
```

## Configuration

Both variables default false. The stage runs before the union scan merge, so the `CASE` wins when
both apply.

## Correctness / safety argument

The candidates of one row share the key and the lower-priority ones are never kept; a `NULL` filter
falls through the `CASE` as it dropped the row. Required: limit 1 without ties or offset, one integer
literal priority, literals in the other columns, and equal literals for branches of equal priority
(otherwise the top-n chose arbitrarily and is left alone). In streaming a branch with `now()` stays
separate.

## Status and versions

Since rc5; present in rc8.

## Tests

Planner `status_ladder_case.yaml`; differential `e2e_test/streaming/status_ladder_case.slt`,
`e2e_test/batch/status_ladder_case.slt`.

## Limits and risks

Not done: replacing the top-1 by `max(priority)` (no state saved in streaming) and turning a default
branch from the entity table into a join.

## Upstream relation

No direct counterpart known. Narrow pattern.
