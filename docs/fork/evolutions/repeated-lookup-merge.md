# Merge of repeated lookups

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/streaming-merge-repeated-lookups`, `proto/plan-reduction-flat-lookups` (streaming), `proto/plan-reduction-batch-keys` (batch) |
| Status | prototype; streaming in rc4, batch in rc5; present in rc8 |
| Variables | `streaming_merge_repeated_lookups` (false), `batch_merge_repeated_lookups` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), idea 3 |

## Summary

`RepeatedLookupMergeRule` removes a join that looks up, by the whole key of a table, a row that the
other side of the join already carries: several joins of the same table on its key from the same
columns become one, and the columns of the removed side are computed from the carried row.

## Motivation

Typical when two views each join the same dimension and a third view joins both views: the same
row of the dimension is read twice through two joins. Sharing the scan does not help: each join
keeps its own state for both of its sides.

## What it does

```sql
CREATE TABLE f (id int PRIMARY KEY, d_id int);
CREATE TABLE d (id int PRIMARY KEY, name varchar, grp varchar);
SELECT f.id, d1.name, d2.grp
FROM f LEFT JOIN d d1 ON f.d_id = d1.id
       LEFT JOIN d d2 ON f.d_id = d2.id;
-- before: two hash joins with d, two scans of d (or one shared scan)
-- after:  one left join with d, reading the rows that pass either filter;
--         d2.grp computed from the row of d1 under a CASE on its filter
```

- The *lookup* is a scan of a table (under projections, filters and shares) joined, inner or left,
  on the whole key with `=`. An *instance* is another scan of the same table in the kept side; the
  rule follows it up through projections, filters, joins, top-n, dedup, aggregations whose group
  key designates the row, table functions and (read-only) shared sub-plans, tracking which columns
  are bound to columns of the table.
- An inner lookup becomes a filter on "found", a left lookup a `CASE` on "found"; a semi or anti
  join on the key becomes a filter on "found" or its opposite.
- Flat form (`proto/plan-reduction-flat-lookups`): a tree of inner joins is seen as one flat join
  with classes of equal columns, so the lookup may be any leaf, nested lookups and lookups in the
  right side of left, semi and anti joins are handled, and a shared sub-plan with a unique key is a
  relation like a table. Up to three passes.
- Batch: the same rule and module, in `reduce_batch_on_keys` (shared sub-plans do not exist in
  batch, they are unfolded first).

## Configuration

`streaming_merge_repeated_lookups`, `batch_merge_repeated_lookups`; default false.

## Correctness / safety argument

Two reads of a table at the same epoch (streaming) or snapshot (batch) read the same rows; a
lookup on the whole key finds at most one row. Only simple key types where equal values are
indistinguishable are used; temporal joins (`AS OF`) and sides behind temporal filters are never
lookups. A shared sub-plan is never rewritten (its other consumers would compute it twice). In
streaming, `internal_last_seen_value` carries the row through an aggregation; its handling of
`NULL` inputs was an upstream bug fixed in the fork (see `../corrections/`). See the design note,
idea 3, for the full argument.

## Status and versions

Streaming in rc4 (flat form included); batch since rc5; present in rc8.

## Tests

- Planner: `stream_merge_repeated_lookups.yaml`, `batch_plan_reduction_keys.yaml`.
- Differential: `e2e_test/streaming/merge_repeated_lookups.slt`,
  `e2e_test/streaming/merge_lookups_flat.slt`, `e2e_test/batch/plan_reduction_keys.slt`.

## Limits and risks

- Not handled: a lookup written with `IS NOT DISTINCT FROM`, a key column compared with a constant,
  keys of types outside the simple ones, a key compared on a computed expression, two equal
  sub-plans that are not shared.
- The rule is large (`repeated_lookup_merge_rule.rs` and `flat.rs`); its gain depends heavily on
  the shapes of the views.

## Upstream relation

Related: PostgreSQL 18 `remove_useless_self_joins`, Materialize `redundant_join`, the "JoinOnKeys"
rule of the Amazon Athena fusion paper. Nothing comparable in upstream RisingWave v3.1.2. Possible
upstream candidate after more validation; not proposed.
