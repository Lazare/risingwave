# Updates between equal values with different texts dropped as no-ops downstream

- **Kind**: upstream bug (streaming, elimination of no-op updates), found by the differential test
  of the transitive predicates of the plan reduction prototype on 2026-11-28 (independent of any
  fork variable).
- **Affected versions**: upstream `v3.1.2` (elimination of `Insert` / `Delete` pairs since
  upstream #23483, 2025-10) and `main` (2026-11-28); every fork build up to the plan reduction
  release candidate rc8 included.

## Symptom

A materialized view kept the old text of a value after an update to an equal value written
differently, when the update also changed a column the view does not read (or when the view
reads it through a projection, a hash join, an aggregation or a window function):

```sql
create table t (id int primary key, k numeric, g int);
create materialized view m as select id, k::varchar as kt from t;
insert into t values (1, 1.0, 1);
update t set g = 2, k = 1.00 where id = 1;
select kt from m;          -- wrong: 1.0     correct: 1.00 (as `select k::varchar from t`)
```

This is how, over a join on numeric keys, the text of `s.k` selected by a materialized view
differed from the batch query for equal keys (`1.0` / `1.00`): the join outputs `s.k` itself, but
the view never received the update of `s` after the projection of its scan.

## Cause

`StreamChunk::eliminate_adjacent_noop_update`, applied by the dispatcher when it projects columns
out, by projections and by hash joins, removes an adjacent delete and insert of equal rows
(`row_ref(prev) == row_ref(curr)`, the SQL equality). The same equality decides that the output of
an aggregation (`OnlyOutputIfHasInput`, simple aggregation) or of a window function did not
change, and that a change-log update of snapshot backfill is a no-op. Equal values with different
representations (`1.0` / `1.00`, `0` / `-0`, `'1 day'` / `'24 hours'`) were taken for no change.

## Fix

These places use `risingwave_common::row::identical` (value and representation) instead of the
equality. Commits `34bb3a7631` and `a33e0d3f2d` (clippy fix, to squash into it) on branch `proto/plan-reduction-fix-correctness` (applies to
upstream code alone), with the table materialization case of
[upsert-equal-values-lost.md](upsert-equal-values-lost.md).

## Tests

`e2e_test/streaming/correctness_fixes.slt` (a view reading `k` and `f` through the projection of
its scan, updates of another column with an equal value of `k`). Unit test `test_identical` in
`src/common/src/row/mod.rs`.

## Upstream status

Not fixed on `main` (2026-11-28): the elimination still compares rows with `==`. No upstream issue
found.
