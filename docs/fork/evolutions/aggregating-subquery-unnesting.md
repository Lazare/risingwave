# Unnesting of aggregating correlated subqueries

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/streaming-simplify-subquery-unnesting` (streaming), `proto/plan-reduction-batch-unnest` (batch) |
| Status | prototype; streaming in rc4, batch in rc5; present in rc8 |
| Variables | `streaming_simplify_subquery_unnesting` (false), `batch_simplify_subquery_unnesting` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), idea 9 and "Batch counterparts" |

## Summary

A correlated subquery that aggregates the rows equal to columns of the outer row (`EXISTS`, `count`,
`min`, `ARRAY(...)` in a select list, under `OR` or in a `CASE`) becomes a left join with the
aggregation grouped by the compared columns, instead of the general decorrelation that reads the
outer side again to build a domain of its distinct values and joins back with
`IS NOT DISTINCT FROM`.

## Motivation

`PullUpCorrelatedPredicateAggRule` exists upstream but never applied to these plans: the apply of a
select-list subquery is marked "at most one row" and the rule refuses the mark; it refuses `count`
(the count bug), so every `EXISTS`; and several subqueries of one select list sit under one apply.
The general way costs an extra scan, a domain aggregation and a join per subquery.

## What it does

```sql
CREATE TABLE t1 (id int PRIMARY KEY, k int);
CREATE TABLE f (k int, v int);
SELECT t1.id, EXISTS (SELECT 1 FROM f WHERE f.k = t1.k) AS has_f,
       (SELECT count(*) FROM f WHERE f.k = t1.k) AS n
FROM t1;
-- before: t1 scanned again for a domain of k, joins with f, aggregation, null-safe joins back
-- after:  t1 LEFT JOIN (SELECT k, count(*) ... FROM f GROUP BY k) ON f.k = t1.k,
--         with CASE WHEN <matched> THEN .. ELSE false / 0 END
```

A second pass of a variant of the rule accepts the "at most one row" mark and left outer applies
only, gives `false`/`0`/empty array to unmatched rows with a `CASE`, and `ApplyOneRowJoinSplitRule`
splits the apply of several subqueries into one apply per subquery.

## Configuration

Both variables default false.

## Correctness / safety argument

The `CASE` gives every output its value over an empty input when every aggregate call has a known
value over an empty input and the apply has no condition; otherwise the general way is kept. Kept in
the general way: a correlated column in the aggregated expression, a non-equality correlation,
`IS NOT DISTINCT FROM`, a subquery that groups.

## Status and versions

Streaming present in rc4; batch since rc5; present in rc8.

## Tests

Planner `stream_simplify_subquery_unnesting.yaml`, `batch_plan_reduction_unnest.yaml`; differential
`e2e_test/streaming/simplify_subquery_unnesting.slt`, `e2e_test/batch/plan_reduction_unnest.slt`.

## Limits and risks

In batch, the aggregation covers the whole inner table grouped by the correlated columns, where the
domain restricted it to the outer keys: worse for a query that keeps few outer rows of a large inner
table (one reason the variable stays off by default).

Found on the way and recorded in the design note, not changed: `WHERE EXISTS (SELECT <aggregate
without GROUP BY> ...)` is planned as a semi join although it is always true (upstream behavior).

## Upstream relation

Extends an upstream rule; a natural upstream PR candidate. Not proposed yet.
