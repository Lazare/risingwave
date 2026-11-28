# Sharing again the sub-plans that became identical (streaming)

| | |
|---|---|
| Kind | optimizer rule (streaming) |
| Branch | `proto/plan-reduction-shared-aggs` |
| Status | prototype in rc4; present in rc8 |
| Variables | `streaming_share_identical_subplans` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), idea 10 |

## Summary

Upstream shares common sub-plans at the beginning of the streaming optimizer, before predicate
pushdown, subquery unnesting and column pruning. Sub-plans that only become identical afterwards are
computed twice. `share_identical_subplans` runs at the end of the streaming optimizer and shares the
stateful sub-plans that are now identical; `share_widened_subplans` first aligns copies that differ
by their columns, by filters that commute with them, or by the order of inner joins.

## Motivation

A view joined and filtered in one place and the same join written with the filter in another; an
aggregation of a view whose other aggregates are pruned and the same aggregation written directly;
the domains of two subqueries correlated on the same column. Each copy has its own state, actors and
backfills.

## What it does

```sql
CREATE TABLE f (k int, v int, w int);
CREATE VIEW agg AS SELECT k, sum(v) AS sv, max(w) AS mw FROM f GROUP BY k;
SELECT a.k, a.sv, b.mw FROM agg a JOIN agg b ON a.k = b.k WHERE b.mw > 0;
-- the two copies differ after column pruning; widened sharing builds one aggregation
-- with both calls, shared by the two consumers
```

- Only sub-plans with a stateful operator that read a table or a source are shared (a bare scan
  chain would trade a backfill for an extra fragment).
- Widened copies: the core keeps the conjuncts of every copy; the residual conjuncts are lifted above
  joins and aggregations, or become `FILTER` clauses below an aggregation, with a
  `count(*) FILTER` to keep each copy's groups.
- Join order: copies with commuted inner joins or reordered inner joins are aligned, with safeguards
  (every built join must have an equality between its sides).

## Configuration

`streaming_share_identical_subplans`, default false.

## Correctness / safety argument

Same structural equality as the upstream early sharing; deterministic choice of the shared copy.
Lifted conjuncts move only where they commute (inner joins, preserved sides, group-key-only
conjuncts above aggregations). See the design note, idea 10.

## Status and versions

Present in rc4; present in rc8. The branch `proto/plan-reduction-shared-aggs` shows one commit that
is not an ancestor of rc8 ("a reordered copy only builds joins with an equality between their
sides"); the same patch (identical patch id) was squashed into the round-2 branch and is in rc4.

## Tests

Planner `stream_share_and_stacked_aggs.yaml`; differential `e2e_test/streaming/share_and_stacked_aggs.slt`.

## Limits and risks

- A share is a fragment boundary.
- A lifted filter no longer reduces the state of the shared core.
- Not done: filters on the optional side of an outer join, filters with `now()`.

## Upstream relation

Extends the upstream common sub-plan sharing. Possible upstream candidate.
