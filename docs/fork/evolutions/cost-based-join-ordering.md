# Cost-based join ordering with row widths (batch)

| | |
|---|---|
| Kind | optimizer rule (batch) |
| Branch | `proto/plan-reduction-batch-joinorder` |
| Status | prototype; first version (row counts) in rc5, width-aware cost and outer join sides in rc8 |
| Variables | `batch_cost_based_join_ordering` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "BJ" |

## Summary

The inner joins of a batch query are ordered by an estimated cost instead of the heuristic order
(`LeftDeepTreeJoinOrderingRule`, which walks the equality graph from its first edge with no notion of
table sizes). The cost counts bytes: rows times the width of the columns still read above. Left
outer, semi and anti joins may become right joins when that builds the hash table on the smaller
side.

## Motivation

With `FROM d JOIN f ON f.d_id = d.id JOIN e ON f.e_id = e.id`, a large `f` becomes the build side of
the first hash join because `d` is written first. Row counts alone are not enough either: a side
with fewer rows can hold far more bytes (wide JSON columns) than a side with many narrow rows, and
the build side is what is held in memory.

## What it does

- Statistics: the meta node sends the frontend the storage statistics of every state table (key
  count, key and value sizes; also in `rw_catalog.rw_table_stats`). No histograms, no distinct
  counts.
- Estimates: fixed selectivities per conjunct (0.1 for `column = constant`, 0.25 otherwise), one row
  when a unique key is fixed, key to foreign key joins at selectivity 1 over the key side, bounds from
  `CardinalityVisitor`.
- Width: per column, the average stored row size shared by type weights (or type widths without
  sizes), computed top-down from the columns still required.
- Cost: probe bytes 1, build bytes 3, output bytes 1; a lookup join when the planner will make one.
- Enumeration: dynamic programming over connected sets up to 10 inputs (bushy trees), greedy beyond.
- `CostBasedOuterJoinSidesRule` (rc8): a left outer, semi or anti join becomes the right join of the
  other side when that at least halves the estimated hash join cost, unless it may become a lookup
  join.

```sql
-- d: 100 rows, f: 10 M rows, e: 1 000 rows (statistics)
SELECT * FROM d JOIN f ON f.d_id = d.id JOIN e ON f.e_id = e.id;
-- heuristic: build on f; cost-based: probe f, build on d and e
```

## Configuration

`batch_cost_based_join_ordering`, default false. With
[cardinality feedback learning](cardinality-feedback.md), learned ratios replace the fixed guesses.

## Correctness / safety argument

Reordering inner joins is exact; conjuncts are placed at the lowest join that has their columns.
Outer joins only swap sides into the equivalent right join, which the batch hash join executor
supports. Fallback: if any input of a multi-join has no estimate (no statistics, a source, a table
function), the plan is exactly the one without the variable.

## Status and versions

First version in rc5; width-aware cost and outer join sides in rc8.

## Tests

Planner `batch_cost_based_join_ordering.yaml` (with the test-only options `table_row_counts` and
`table_sizes`); differential `e2e_test/batch/cost_based_join_ordering.slt` on tables of very
different sizes.

## Limits and risks

Index lookups are not modeled (index selection happens later); joins on non-key columns are guesses;
cross joins between unlinked sets keep the heuristic order; the estimates are only visible in
`EXPLAIN (TRACE)`. A wrong estimate can produce a worse plan than the written order.

## Upstream relation

Upstream batch has no cost-based join ordering in v3.1.2. Upstream PR candidate, but the statistics
plumbing and the cost model would need upstream discussion.
