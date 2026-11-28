# Joins on a column known to be `NULL`

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/plan-reduction-nullkey` |
| Status | prototype in rc7; present in rc8 |
| Variables | `streaming_simplify_null_join_keys` (false), `batch_simplify_null_join_keys` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "NK" |

## Summary

`NullKeyJoinRule`: when a filter below a join requires a column compared with `=` by the join to be
`NULL` (`c IS NULL`), no pair of rows can match. An inner or semi join becomes empty; a left outer or
anti join keeps its left side and its right side becomes empty; a full join becomes a `UNION ALL` of
its padded sides. The empty-relation rules then remove what is above.

## Motivation

A data quality test of the kind "no row has a `NULL` key" over a non-materialized view filters on
`key IS NULL`. Predicate pushdown moves the filter next to the scan of one join side, and the join
then compares a column that is `NULL` in every row of one input; the whole other side (joins,
aggregations, top-n) is still computed, for no possible match.

## What it does

```text
SELECT * FROM v WHERE b_id IS NULL      -- v: a JOIN b ON a.b_ref = b.id

Join(Inner, a.b_ref = b.id)              Values([])
├─Scan(a)                          =>
└─Filter(b.id IS NULL)
  └─Scan(b)
```

Facts are followed through filters, scan predicates, shares, projections of column references or
casts, unions (when every input has the fact), joins and pass-through operators; nothing through
aggregations.

## Configuration

Both variables default false. Stages: `STREAM_SIMPLIFY_NULL_JOIN_KEYS` right after the streaming
empty-relation stage; `BATCH_SIMPLIFY_NULL_JOIN_KEYS` at the start of `reduce_batch_on_keys`.

## Correctness / safety argument

`NULL = x` is `NULL` for every `x` and type, so no pair satisfies the condition, whatever the other
conjuncts, duplicates or empty inputs. The facts hold for every row ever produced, under retractions.
In streaming, a sub-plan that may carry a watermark is never removed. `IS NOT DISTINCT FROM`, as-of
joins and computed operands (`coalesce(c, 0) = x`) are not rewritten.

## Status and versions

Since rc7; present in rc8.

## Tests

Planner `batch_simplify_null_join_keys.yaml`, `stream_simplify_null_join_keys.yaml`; differential
`e2e_test/batch/simplify_null_join_keys.slt`, `e2e_test/streaming/simplify_null_join_keys.slt` (NULL
keys on both sides, duplicates, empty tables, key updates to and from `NULL`, deletes).

## Limits and risks

Not rewritten: a union where only some branches have the fact; `s.k IS NULL` above a left join on
its optional side (anti-join pattern, the filter stays above).

## Upstream relation

Not in upstream RisingWave v3.1.2. Small and self-contained: a good upstream PR candidate.
