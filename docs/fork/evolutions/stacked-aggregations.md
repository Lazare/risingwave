# Merge of stacked aggregations

| | |
|---|---|
| Kind | optimizer rule (streaming) |
| Branch | `proto/plan-reduction-shared-aggs` |
| Status | prototype in rc4; present in rc8 |
| Variables | `streaming_merge_stacked_aggregations` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), idea 6 |

## Summary

`StackedAggMergeRule` and `StackedAggOverUnionMergeRule` merge an aggregation over another
aggregation (directly, through a projection, or through a `UNION ALL` of aggregating branches) into
one aggregation of the inner input, when the outer group key is made of inner group key columns and
the outer aggregates can be computed from the input.

## Motivation

Two aggregation levels appear when views are layered (a per-entity summary, then a per-group
summary) or when per-source aggregations are unioned and aggregated again. Each level is a stateful
operator in streaming.

## What it does

```sql
CREATE TABLE t (region int, city int, x int);
SELECT region, sum(s) FROM (SELECT region, city, sum(x) AS s FROM t GROUP BY region, city) q
GROUP BY region;
-- after: SELECT region, sum(x::bigint) ... FROM t GROUP BY region (one aggregation)
```

Pairs: `sum` of `sum` (integer and decimal, cast to the inner sum type), `sum` of `count`, `min` of
`min`, `max` of `max`, `bool_and`/`bool_or` of themselves; over a dedup, `count(DISTINCT v)` and
`sum(DISTINCT v)`, and `count(v)`/`sum(v)` become distinct aggregates when each inner group is one
distinct `v`.

## Configuration

`streaming_merge_stacked_aggregations`, default false. No batch variable.

## Correctness / safety argument

The inner group key must not be empty (an outer group then exists exactly when an input row has its
key in both plans). Floats are excluded (addition order changes rounding); `avg` of `avg` and `count`
of `count` are excluded; a `HAVING` or a shared inner aggregation stops the rule. Retractions need
nothing special. The streaming `sum` quirk (0 instead of `NULL` after retractions) can differ
between the two plans: the differential test wraps such sums in `coalesce`.

## Status and versions

Present in rc4; present in rc8.

## Tests

Planner `stream_share_and_stacked_aggs.yaml`; differential `e2e_test/streaming/share_and_stacked_aggs.slt`.

## Limits and risks

Two levels are sometimes written on purpose against skew: the variable keeps the rule optional.
Measured shapes of real view definitions rarely match (a `HAVING` between levels, or an aggregation
over a join of an aggregation, which is eager aggregation, not done).

## Upstream relation

Calcite `AGGREGATE_MERGE`, CockroachDB `FoldGroupingOperators`. Not in upstream RisingWave v3.1.2.
Possible upstream candidate.
