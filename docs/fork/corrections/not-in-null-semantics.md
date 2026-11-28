# `NOT IN (subquery)` ignored NULL values

- **Kind**: upstream bug (planner), found by a survey of the optimizer rules of PostgreSQL and
  Calcite on 2026-11-28.
- **Affected versions**: upstream `v3.1.2` and `main` (2026-11-28); every fork build based on
  them up to the plan reduction release candidate rc8 included.

## Symptom

`x NOT IN (SELECT y ...)` in `WHERE` (or `HAVING`, `DELETE ... WHERE`, `UPDATE ... WHERE`) was
planned as a plain anti join on `x = y`. SQL keeps a row only when `x <> y` is true for every `y`:
no row at all when the subquery returns a NULL, and no row whose `x` is NULL unless the subquery
is empty. The anti join kept both, in batch queries and in materialized views.

```sql
create table a (id int, x int, k int);
create table b (y int, k int);
insert into a values (1, 1, 1), (2, 2, 1), (3, null, 1), (4, 5, 2);
insert into b values (1, 1), (null, 2);

select id from a where x not in (select y from b);
-- wrong: 2, 3, 4      correct: no row (b holds a NULL)

delete from b where y is null;
select id from a where x not in (select y from b);
-- wrong: 2, 3, 4      correct: 2, 4 (x NULL is unknown, not true)

delete from b;
select id from a where x not in (select y from b);
-- 1, 2, 3, 4 (empty subquery: every row, x NULL included; this case was right)

insert into b values (1, 1), (null, 2);
select id from a where x not in (select y from b where b.k = a.k);
-- wrong: 2, 3, 4      correct: 2 (k = 1: {1}, x NULL is unknown; k = 2: {NULL})
```

## Cause

`Planner::handle_exists_and_in` (`src/frontend/src/planner/select.rs`) replaces
`x NOT IN (q)` with a `LeftAnti` apply whose condition is `x = y`. An anti join keeps a row that
no row matches, and `x = y` never matches when `x` or `y` is NULL. Upstream has tracked it as a
known issue since 2022 (no null-aware anti join).

## Fix

The anti join on `x = y` is kept (it keeps its hash keys) and completed with the NULL cases:

- **Uncorrelated subquery**: one row is computed over the subquery, from `count(*)` and
  `count(*) FILTER (WHERE y IS NULL)`: 0 when it is empty, 2 when it returns a NULL, 1 otherwise.
  The rows of the anti join are kept when `CASE WHEN x IS NULL THEN 1 ELSE 2 END` is greater than
  that value. The comparison with one row is a dynamic filter in streaming plans and a
  nested-loop join against one row in batch plans:

  ```text
  Project
  └─DynamicFilter (rank > state)          -- batch: NestedLoopJoin with the single row
    ├─Project (CASE WHEN x IS NULL THEN 1 ELSE 2 END AS rank)
    │ └─HashJoin LeftAnti (x = y)
    └─Project (CASE WHEN count = 0 THEN 0 WHEN count_null > 0 THEN 2 ELSE 1 END AS state)
      └─SimpleAgg (count, count FILTER (y IS NULL))
  ```

- **Correlated subquery**: a second anti join on `x IS NULL OR y IS NULL`, planned from a second
  copy of the subquery; the unnesting moves the correlated conditions of the subquery into it as
  into the first one, so a correlated equality makes it a hash join.
- **`x` and `y` never NULL** (columns declared `NOT NULL`, null-rejecting filters, constants...):
  the plain anti join is exact and is kept (fork only, with the column facts of the plan reduction
  prototype).
- **Correlated subquery whose correlated conditions have no equality**
  (`... WHERE b.k > a.k`): the second anti join is a nested-loop join. Batch queries are planned
  exactly. A streaming job cannot run a nested-loop join, while the anti join on `x = y` could:
  to keep such jobs plannable, they keep the previous, inexact plan, with a notice, unless the
  session variable `streaming_exact_not_in_without_equality` is set (the job then fails to plan).

Commits on branch `proto/plan-reduction-fix-correctness`:

- `178f11ecc9` fix(planner): plan NOT IN over a subquery with its NULL semantics (applies to
  upstream code alone; adds the session variable);
- `04005a5d08` compilation fix of `178f11ecc9` (a missing import), to squash into it;
- `77fd6f8b48` feat(planner): keep the plain anti join of NOT IN when both sides are never NULL
  (needs the column facts of the plan reduction prototype);
- tests: `65d11b899c` (end-to-end), `a04212c589` (planner outputs).

In `HAVING`, `NOT IN` also needed the correction of
[in-subquery-in-having.md](in-subquery-in-having.md).

Not changed: `x NOT IN` in a select list or a `CASE`, or under `OR`, is still rejected ("not yet
implemented"); `(a, b) NOT IN (SELECT c, d ...)` is still rejected by the binder ("Subquery must
return only one column"); `x <> ALL (SELECT ...)` does not parse. Positive `IN` was right: a semi
join on `x = y` keeps exactly the rows for which `x IN (...)` is true.

## Tests

- Planner: `src/frontend/planner_test/tests/testdata/input/batch_correctness_fixes.yaml`
  (section `not in`), and the changed outputs of the existing `NOT IN` cases (TPC-H Q16,
  Nexmark, `subquery_expr`, `update`, ...).
- End to end, hand-computed expected results: `e2e_test/batch/correctness_fixes.slt` (NULLs on
  each side, empty subquery, duplicates, expressions, constants, `HAVING`, `DELETE`, correlated
  with and without equality) and `e2e_test/streaming/correctness_fixes.slt` (materialized views
  after the backfill and after each change of either side).

## Upstream status

Not fixed on `main` (2026-11-28): `handle_exists_and_in` is unchanged. Tracked upstream as
<https://github.com/risingwavelabs/risingwave/issues/4233> ("known issue: Anti-Join produces
incorrect results due to lack of null-aware anti-join", open since 2022-07). PostgreSQL 19
converts `NOT IN` to an anti join only when both sides are proven non-NULL.
