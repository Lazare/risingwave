# Duplicates found with `lag` (batch)

| | |
|---|---|
| Kind | optimizer rule (batch) |
| Branch | `proto/plan-reduction-batch-lag` |
| Status | prototype in rc5; present in rc8 |
| Variables | `batch_rewrite_lag_duplicates` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "BL" |

## Summary

`LagDuplicatesRule`: a uniqueness check written as `x = lag(x) OVER (PARTITION BY p ORDER BY x)`, of
which nothing but `x`, the lagged value and the partition columns is read, becomes a count per value
(`GROUP BY p, x`) and `generate_series(2, count)` to produce the duplicates, instead of a sort of the
whole partition on one node.

## Motivation

Data quality tests often check uniqueness this way. With `PARTITION BY true`, the batch plan sorts
the whole relation on one node in one window partition that does not spill. No upstream batch stage
rewrites it.

## What it does

```sql
CREATE TABLE t1 (x int);
WITH ordered AS (SELECT x, lag(x) OVER (PARTITION BY true ORDER BY x) AS prev FROM t1
                 WHERE x IS NOT NULL)
SELECT x, count(*) + 1 AS n FROM ordered WHERE x = prev GROUP BY x;
-- before: HashAgg <- Filter(x = prev) <- OverWindow <- Sort <- Exchange(Single) <- Scan
-- after:  Filter(c > 1) <- HashAgg(x: c = count(*)) <- Exchange(Hash x) <- Filter(x IS NOT NULL) <- Scan
```

When the consumer is itself an aggregation on `x` with only counts, the expansion is skipped and
`c - 1` is used as the count.

## Configuration

`batch_rewrite_lag_duplicates`, default false.

## Correctness / safety argument

Within a partition rows are ordered by `x` first, so the `k` rows of a non-NULL value are contiguous
and `x = lag(x)` holds on exactly `k - 1` of them, never on a NULL `x`. Since only `x`, the lagged
value (equal to `x`) and partition columns are read, the result is `k - 1` copies of `(p, v, v)`.
Refused: other columns read above, other offsets or frames, `IGNORE NULLS`, `IS NOT DISTINCT FROM`,
types where equal values can differ (decimal scale, floats, intervals, composites).

## Status and versions

Since rc5; present in rc8.

## Tests

Planner `batch_rewrite_lag_duplicates.yaml`; differential `e2e_test/batch/rewrite_lag_duplicates.slt`.

On the validation set (planner-level counts), sorts and over windows drop by about 85% with all rc8
rules; this rule and the [window top-n](batch-second-pass-and-window-topn.md) are the rules that
remove sorts and windows, the split between them is not documented.

## Limits and risks

Narrow pattern; refused shapes keep the window.

## Upstream relation

Not in upstream RisingWave. Upstream interest not assessed.
