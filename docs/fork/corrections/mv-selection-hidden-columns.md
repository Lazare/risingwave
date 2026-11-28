# Materialized view selection read a hidden column instead of a visible one

- **Kind**: upstream bug (optimizer, `MvSelectionRule`, reached only with
  `enable_mv_selection = true`, default off), found on 2026-11-28 by the measurement of the fork's
  planner rules on the upstream planner tests (a `SET enable_mv_selection = true` of
  `mv_selection.yaml` stayed set for the next cases).
- **Affected versions**: upstream `v3.1.2` and `main` (2026-11-28); every fork build up to the
  plan reduction release candidate rc9 included.

## Symptom

```sql
create table t (x int, y int);
insert into t values (1, 10), (2, 10), (3, 10), (4, 10), (5, 20), (6, 20);
create materialized view mv as select x, y, row_number() over w from t window w as (partition by y order by x);
set enable_mv_selection = true;

select x, y, row_number() over w from t window w as (partition by y order by x);
select x, y from (select x, y, row_number() over w as rn from t window w as (partition by y order by x)) where rn <= 3;
```

Correct results: `(1, 10, 1) (2, 10, 2) (3, 10, 3) (4, 10, 4) (5, 20, 1) (6, 20, 2)`, and the
same rows without the row number and without `(4, 10)` for the second query.

A debug build panics while planning both queries: `MvSelectionRule fails to generate equivalent
plan`, with the output schema `[mv.x, mv.y, mv.t._row_id]` instead of `[x, y, row_number]`. A
release build has no such check: the plan reads the hidden row id of `t` (type `serial`) where
the query expects the row number (`bigint`), a wrong value or an error further on (not tried on a
release build).

## Cause

`MvSelectionRule` compares the logical plan of a batch query (or of a sub-plan) with the plan of
each materialized view the query may use, and replaces a match by a scan of the view's table.
`candidate_scan` scanned the first `N` columns of the table, `N` being the number of outputs of
the view's plan. The outputs of the plan are the visible columns of the view, but the table also
holds hidden columns (the stream key: the row id of the table, a join key...) at the position
where the stream plan produced them: here `x, y, t._row_id (hidden), row_number`, the row id
coming from the input of the over window, before the window column. The first three columns are
then `x, y, t._row_id`.

## Fix

`candidate_scan` scans the visible columns of the table, in order, and gives up the rewrite
unless they match the outputs of the view's plan one to one, types included. The join and
aggregation roll-up rewrites of the rule use the same scan.

Commit `f5f6cff298` on branch `proto/plan-reduction-bench-fixes` (applies to upstream code
alone).

## Tests

- Planner: `batch_correctness_fixes.yaml`, section `MvSelectionRule` (the over window view, a
  filter over it, and a view whose hidden row id comes after its visible columns, unchanged).
- End to end: `e2e_test/batch/correctness_fixes.slt` (results with and without
  `enable_mv_selection`).

## Upstream status

Not fixed on `main` (2026-11-28): `candidate_scan` still takes the first columns of the table. No
upstream issue found.
