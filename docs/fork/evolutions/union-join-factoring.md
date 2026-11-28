# Join common to the branches of a `UNION ALL`

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/streaming-merge-repeated-lookups`, `proto/plan-reduction-union-derived` (streaming), `proto/plan-reduction-batch-unnest` (batch) |
| Status | prototype; streaming in rc4, batch in rc5; present in rc8 |
| Variables | `streaming_factor_union_join` (false), `batch_factor_union_join` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), idea 7 and "Batch counterparts" |

## Summary

`CommonJoinOverUnionRule`: `(A ⋈ C) UNION ALL (B ⋈ C)` becomes `(A UNION ALL B) ⋈ C` for inner, left
outer, left semi and left anti joins. `C` is read once and, in streaming, its join state is kept once.
No key is needed.

## Motivation

Views often build one branch per kind of row, each branch joining the same dimension or the same
derived relation (a view or a subquery written twice). Each branch then scans and backfills the
relation and keeps its own join state.

## What it does

```sql
CREATE TABLE a (id int, d_id int);
CREATE TABLE b (id int, d_id int);
CREATE TABLE d (id int PRIMARY KEY, name varchar);
SELECT a.id, d.name FROM a JOIN d ON a.d_id = d.id
UNION ALL
SELECT b.id, d.name FROM b JOIN d ON b.d_id = d.id;
-- before: Union(HashJoin(a, d), HashJoin(b, d))
-- after:  HashJoin(Union(a + branch number, b + branch number), d)
```

- The joined relation is a table scan (with filters and projections, also through a shared
  sub-plan) or a derived relation that is the same plan in every merged branch.
- What differs between branches (rest of the condition, filter of the joined relation) is checked in
  the join condition for the rows of each branch, using the branch number; filters and projections
  above the join are applied per branch.
- Several joins and stacked semi/anti joins are factored; a semi or anti join above a branch's join
  is first moved into the input it reads.

## Configuration

`streaming_factor_union_join`, `batch_factor_union_join`; default false.

## Correctness / safety argument

Every row of the union meets the same rows of the relation as before, under the same condition, so
`NULL` keys and duplicates behave as in each branch; outer and anti joins stay exact because the
per-branch conditions are in the join condition. A derived relation must give the same rows wherever
it is computed: tables read in the present, deterministic expressions without `now()`, aggregates
that do not depend on row order. See the design note, idea 7.

## Status and versions

Streaming in rc4 (derived relations included); batch since rc5; present in rc8.

## Tests

Planner `stream_factor_union_join.yaml`, `batch_plan_reduction_unnest.yaml`; differential
`e2e_test/streaming/factor_union_join.slt`, `e2e_test/batch/plan_reduction_unnest.slt`.

## Limits and risks

- One join state holds the rows of every branch, and union rows are as wide as the widest branch.
- In batch, one hash table joins the rows of every branch.
- Not done: inner join in one branch and anti join in another, disjoint filters with different keys,
  joins common only after reordering.

## Upstream relation

Not present in upstream RisingWave v3.1.2. Possible upstream candidate; not proposed.
