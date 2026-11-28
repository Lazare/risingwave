# Unique-key simplifications

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/streaming-prune-empty-relation` (streaming), `proto/plan-reduction-batch-keys` (batch) |
| Status | prototype; streaming in rc4, batch in rc5; present in rc8 |
| Variables | `streaming_simplify_on_unique_key` (false), `batch_simplify_on_unique_key` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), ideas 4, 1 and 2 |

## Summary

Stronger derivation of unique keys through the plan (idea 4), then two eliminations that use them:
an outer join whose optional side is unused and unique on the join columns is removed (idea 1), and
an aggregation, `DISTINCT`, top-n or dedup whose groups hold exactly one row becomes a projection
(idea 2).

## Motivation

Layered views commonly join a dimension "just in case" and select none of its columns, or
deduplicate with `row_number() = 1` and then aggregate again on the same key. Each such operator
costs a join or an aggregation state in streaming and a hash table in batch, for nothing. Without
keys that survive top-n, dedup and left joins, these patterns are not recognized across layers of
views.

## What it does

```sql
CREATE TABLE f (id int PRIMARY KEY, d_id int, amt int);
CREATE TABLE d (id int PRIMARY KEY, name varchar);
-- idea 1
SELECT f.id, f.amt FROM f LEFT JOIN d ON f.d_id = d.id;
-- before: HashJoin(left) <- Scan(f), Scan(d)      after: Scan(f)
-- idea 2
SELECT id, sum(amt), count(*) FROM f GROUP BY id;
-- before: HashAgg(id) <- Scan(f)                   after: Project(id, amt::bigint, 1) <- Scan(f)
```

- Keys derived after top-n and dedup (the group key), through left and inner joins whose other side
  is unique on the join columns (`UniqueKeySimplifyRule`, `unique_key_simplify_rule.rs`).
- `UnusedOuterJoinEliminateRule`: sound when no column of the optional side is used above and the
  equalities bind every column of a key of the optional side.
- `AggOnKeyEliminateRule`: needs a non-empty group key and aggregates with a closed form over one
  row.
- Batch: also `DISTINCT ON`; runs in `reduce_batch_on_keys` after the first predicate pushdown,
  before join ordering, up to three passes with column pruning.

## Configuration

`streaming_simplify_on_unique_key` (new streaming jobs), `batch_simplify_on_unique_key` (batch
queries). Default false. A separate variable from the column facts because the result then relies
on the keys being exact.

## Correctness / safety argument

Keys come from table definitions (primary keys, the hidden row id) and from operators that make a
key (aggregation, top-1, dedup), never from data. An aggregation or join tolerates a stream key that
is only exact at barrier boundaries, a projection does not: once a stateful operator is replaced by
a projection, a wrong key would change results silently. Hence the opt-in.

Visible difference found by the differential test: a streaming `sum` keeps `0` once all its inputs
are retracted while its group still holds rows; the projection that replaces it returns `NULL`, as
the batch query does. A materialized view created with the variable can therefore show `NULL` where
the same view without it shows `0` (design note, idea 2).

## Status and versions

Streaming present in rc4; batch since rc5. In rc6 an upstream latent bug exposed by the batch
early column pruning (set operations without `ALL` pruned to zero columns) was fixed; see
`../corrections/`. Present in rc8.

## Tests

- Planner: `stream_simplify_on_unique_key.yaml`, `batch_plan_reduction_keys.yaml`.
- Differential: `e2e_test/batch/plan_reduction_keys.slt`; the streaming rules are exercised by the
  streaming differential tests of the plan-reduction line.

## Limits and risks

- An inner join cannot be removed this way (no foreign keys).
- The variant "unused join under `DISTINCT`" without uniqueness is in
  [lookups under dedup](lookups-under-dedup.md), streaming only.
- The `sum` difference above.

## Upstream relation

Comparable rules: PostgreSQL join removal (`analyzejoins.c`), Calcite `PROJECT_JOIN_REMOVE` and
`AGGREGATE_REMOVE`, CockroachDB `EliminateJoinUnderProjectLeft` and `EliminateGroupBy`, Materialize
`reduce_elision`. Upstream RisingWave has `AggGroupBySimplifyRule` only. Upstream PR candidate;
not proposed yet.
