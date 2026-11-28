# `IN (subquery)` in `HAVING` compared the wrong column

- **Kind**: upstream bug (planner, rewriting of `HAVING` onto the aggregation), found on
  2026-11-28 while testing the `NOT IN` correction.
- **Affected versions**: upstream `v3.1.2` and `main` (2026-11-28); every fork build up to the
  plan reduction release candidate rc8 included.

## Symptom

In `HAVING x IN (SELECT ...)` or `HAVING x NOT IN (SELECT ...)`, the expression `x` kept the
column positions of the input of the aggregation instead of those of its output. The semi or anti
join then compared another column, or a column of the subquery itself, or failed to plan when `x`
holds an aggregate call.

```sql
create table t (id int, x int, k int);
create table s (k int);
insert into t values (1, 10, 1), (2, 20, 1), (3, 30, 2);
insert into s values (1);

select k, count(*) from t group by k having k in (select k from s);
-- wrong: (1, 2), (2, 1)      correct: (1, 2)
-- `k` is column 2 of the scan of `t` but column 0 of the aggregation, whose output has two
-- columns: column 2 of the semi join is `s.k`, so the condition became `s.k = s.k`, true for
-- every group as soon as `s` holds a non-NULL value.
```

## Cause

`LogicalAgg::create` rewrites the `HAVING` condition so that it refers to the group keys and the
aggregate calls, but `LogicalAggBuilder::rewrite_subquery` returned the subquery unchanged,
including the expression compared by `IN`, `SOME` or `ALL`, which belongs to the outer query.

## Fix

`rewrite_subquery` rewrites the compared expression of `IN`, `SOME` and `ALL` like the rest of
the condition. Commit `2411a9a66f` on branch `proto/plan-reduction-fix-correctness` (applies to
upstream code alone).

## Tests

- Planner: `batch_correctness_fixes.yaml` (`in having`, `not in having` with a group key and with
  `count(*)`).
- End to end: `e2e_test/batch/correctness_fixes.slt` (`HAVING k IN`, `HAVING k NOT IN`,
  `HAVING count(*) NOT IN`).

## Upstream status

Not fixed on `main` (2026-11-28): `rewrite_subquery` of `LogicalAggBuilder` is unchanged. No
upstream issue found.
