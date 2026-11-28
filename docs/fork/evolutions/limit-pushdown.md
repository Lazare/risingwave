# Limit and top-n pushed into unions and outer joins (batch)

| | |
|---|---|
| Kind | optimizer rule (batch) |
| Branch | `proto/plan-reduction-batch-limit` |
| Status | candidate (not in rc8) |
| Variables | `batch_push_down_limit` (false) |
| Design note | `streaming-plan-reduction.md`, section "BLM", on the branch |

## Summary

`LimitUnionJoinPushDownRule`: a copy of a `LIMIT n OFFSET k` or top-n with `n + k` rows goes into
each branch of a `UNION ALL` below it and into the preserved side of a left or right join (a top-n
only when ordered by columns of that side). The original limit stays above.

## Motivation

Upstream moves limits across projections and into scans, but nothing crosses a union or a join:
`... UNION ALL ... LIMIT 10` reads both inputs whole into one task, and `... LEFT JOIN ... LIMIT 10`
joins every row of the left side. Interactive exploration of views (`SELECT * FROM v LIMIT 10`) is
the typical case.

## What it does

```sql
CREATE TABLE a (x int, y int);
CREATE TABLE c (x int, y int);
CREATE TABLE l (id int PRIMARY KEY, k int);
CREATE TABLE r (k int PRIMARY KEY, w varchar);
SELECT x, y FROM a UNION ALL SELECT x, y FROM c ORDER BY x LIMIT 10 OFFSET 5;
-- after: TopN(x, 10, 5) <- Union <- [TopN(x, 15) <- Scan(a), TopN(x, 15) <- Scan(c)]
SELECT l.id, r.w FROM l LEFT JOIN r ON l.k = r.k LIMIT 10;
-- after: Limit(10) <- LeftJoin(Limit(10) <- Scan(l), Scan(r))
```

## Configuration

`batch_push_down_limit`, default false, batch only.

## Correctness / safety argument

Any `n + k` rows of a union are valid for the limit above; each of the first `n + k` rows of a union
in an order is among the first `n + k` of its branch. Every row of the preserved side of an outer
join gives at least one row of the join, depending only on that row. Rejected because wrong: full
outer joins (the design note gives a counterexample), inner, semi and anti joins, `UNION` without
`ALL`, a filter between the limit and the union or join.

## Status and versions

Candidate: branch based on the rc8 line, not merged into any image.

## Tests

Planner `batch_push_down_limit.yaml`; differential `e2e_test/batch/push_down_limit.slt` (38 queries
whose result does not depend on which rows a limit keeps, local and distributed modes).

## Limits and risks

The copies add operators: a top-n per branch sorts each branch; with a large `n + k` the gain is
small.

## Upstream relation

Calcite `SortUnionTransposeRule` and `SortJoinTransposeRule`. Good upstream PR candidate.
