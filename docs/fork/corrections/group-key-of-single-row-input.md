# `GROUP BY` / `DISTINCT` over an input of at most one row returned a row of NULLs

- **Kind**: upstream bug (optimizer, `AggGroupBySimplifyRule`).
- **Affected versions**: upstream `v3.1.2`; fixed in the fork builds of integration `v3.1.2`
  from the first one (commit of 2026-11-24).

## Symptom

`AggGroupBySimplifyRule` reduces the group key of an aggregation with the functional dependencies
of its input. The key of an input that has at most one row (an aggregation without group key) is
empty, so `GROUP BY`, `DISTINCT`, `EXCEPT` and `INTERSECT` over such an input became an
aggregation without group key, which returns one row (of NULLs) over an empty input instead of no
row, in batch queries and in materialized views.

```sql
create table t (v int);
select distinct cnt from (select count(*) as cnt from t) x where cnt > 5;
-- wrong: one row, NULL     correct: no row
```

## Cause

The minimized group key may be empty; an aggregation with an empty group key is a scalar
aggregation, not a grouping with one group.

## Fix

The rule keeps one column of the group key. Part of commit `5b0ef92d9b` (squashed
"fix(frontend): keep a group key when grouping an input that has at most one row") on branch
`fork/optimizer`.

## Tests

Planner cases in `functional_dependency.yaml`.

## Upstream status

Fixed on `main` by <https://github.com/risingwavelabs/risingwave/pull/27594> ("keep at least one
group key in AggGroupBySimplifyRule", merged 2026-10-26), reported as
<https://github.com/risingwavelabs/risingwave/issues/27451>.
