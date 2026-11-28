# Batch: second pass of the key reductions, window top-n

| | |
|---|---|
| Kind | optimizer rule (batch) |
| Branch | `proto/plan-reduction-batch-pass2` |
| Status | prototype in rc7; present in rc8 |
| Variables | `batch_reduce_keys_after_conversions` (false), `batch_over_window_to_topn` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "BP and BT" |

## Summary

- BP: the batch key reductions ([column facts](column-facts-and-empty-relations.md),
  [unique keys](unique-key-simplifications.md), [repeated lookups](repeated-lookup-merge.md)) run a
  second time after the stages that create aggregations (the `lag` rewrite, `UNION` to aggregation,
  distinct aggregates, window conversion), at most two rounds.
- BT: `row_number() OVER (PARTITION BY k ORDER BY o) <= n` with `n` (limit plus offset) at most 10
  becomes a top-n per group instead of a sort and a window.

## Motivation

Several late batch stages create exactly the operators that the key reductions remove, which then
stay. Upstream removed the window-to-top-n conversion from batch (DataFusion cannot run a top-n per
group), so a top-1 per key in batch always sorts every row.

## What it does

```sql
CREATE TABLE t (id int PRIMARY KEY, v int);
WITH o AS (SELECT id, lag(id) OVER (ORDER BY id) AS prev FROM t)
SELECT id, count(*) + 1 FROM o WHERE id = prev GROUP BY id;
-- after the lag rewrite: Filter(count > 1) <- Agg(group id: count) <- Scan(t)
-- BP: the aggregation is on the key of t (count = 1), 1 > 1 is never true: empty relation

SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY k ORDER BY ts DESC) AS rn FROM f) x
WHERE rn = 1;
-- BT: GroupTopN(k, ts DESC, 1) instead of Sort + OverWindow + Filter
```

## Configuration

Both default false. BP does nothing without the key reduction variables. With
[learning](cardinality-feedback.md) enabled, BT accepts `n` up to 1000 when the learned group ratio
shows few rows per group.

## Correctness / safety argument

BP: no new algebra, only a new position; at that point every set operation is a `UNION ALL`, so the
column pruning of the pass cannot change what a set operation removes. BT: the streaming rule
unchanged; among tied rows both forms keep an arbitrary one. The limit of 10 exists because the batch
group top-n cannot spill and its heaps live until the input ends; `rank()`, `dense_rank()` and larger
limits keep the window. Iceberg queries run by DataFusion keep the window.

## Status and versions

Since rc7; present in rc8.

## Tests

Planner `batch_reduce_keys_after_conversions.yaml`, `batch_over_window_to_topn.yaml`; differential
`e2e_test/batch/reduce_keys_after_conversions.slt`.

## Limits and risks

The group top-n uses memory that cannot spill (taken from the query pool, then from the unreserved
memory of the node). The union join factoring, union scan merge, status ladder and empty-relation
stages are not re-run in the second pass.

## Upstream relation

BT reverts an upstream decision for the non-DataFusion path; upstream discussion needed. BP is
specific to the fork's key reductions.
