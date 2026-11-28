# `EXISTS` over a correlated scalar aggregation was not always true

- **Kind**: upstream bug (optimizer, subquery unnesting). Recorded as a known issue in the design
  note of the plan reduction prototype on 2026-11-25; fixed upstream on 2026-11-10.
- **Affected versions**: upstream `v3.1.2` (fixed on `main` after it); every fork build up to the
  plan reduction release candidate rc8 included.

## Symptom

An aggregation without `GROUP BY` returns exactly one row, even over no input row, so
`EXISTS (SELECT max(y) FROM b WHERE b.x = a.x)` is always true and `NOT EXISTS` of it always
false. Both batch queries and materialized views returned only the outer rows with (or without)
a matching inner row.

```sql
create table a (x int);
create table b (x int, y int);
insert into a values (1), (2), (null);
insert into b values (1, 10);

select * from a where exists (select max(y) from b where b.x = a.x);
-- wrong: 1            correct: 1, 2, NULL
select * from a where not exists (select max(y) from b where b.x = a.x);
-- wrong: 2, NULL      correct: no row
```

## Cause

`PullUpCorrelatedPredicateAggRule` turns the scalar aggregation into an aggregation grouped by
the correlated column, joined back on it. That drops the row the aggregation returns for an outer
row without inner rows. The rule checked that this row of NULLs is rejected by the filter above
the apply, which is enough for a left outer apply (the join pads the missing row with NULLs) but
not for a semi or anti apply, which pads nothing and whose condition (`TRUE` for `EXISTS`)
accepted the row of NULLs.

## Fix

Port of the upstream fix: for a semi or anti apply, the rule applies only when the condition of
the apply rejects the row of NULLs of an empty input (`x IN (SELECT max(y) ...)` is still
rewritten, `EXISTS (SELECT max(y) ...)` no longer is). `approx_count_distinct`, like `count`, is
never NULL over an empty input. Only the general path of the rule changes; its streaming variant
of the fork already applies to left outer applies only.

Commit `ab284ab14c` on branch `proto/plan-reduction-fix-correctness`, port of upstream
`1852205d8717bf4dcddfec6d4ef2dc14d5f4e7ad`.

## Tests

- Planner: the three cases of the upstream pull request in `subquery_expr_correlated.yaml`.
- End to end: the upstream tests (`e2e_test/batch/subquery/subquery.slt.part`,
  `e2e_test/streaming/exists_correlated_scalar_agg.slt`), and `e2e_test/batch/correctness_fixes.slt`,
  `e2e_test/streaming/correctness_fixes.slt` (`EXISTS` / `NOT EXISTS` of `max`, `sum`, `count`
  through changes of the inner table).

## Upstream status

Fixed on `main` by <https://github.com/risingwavelabs/risingwave/pull/27813> (merged
2026-11-10), not in `v3.1.2`.
