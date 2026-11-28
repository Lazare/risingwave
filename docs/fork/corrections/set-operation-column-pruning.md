# Columns pruned below `UNION` (without `ALL`), `EXCEPT` and `INTERSECT`

- **Kind**: wrong result. The faulty pruning is upstream code (`ColPrunable` of the set
  operations) that upstream never reaches; the fork exposed it with the batch reductions on keys,
  which prune columns earlier. Found on 2026-11-26.
- **Affected versions**: upstream `v3.1.2` code, latent there. Fork: the plan reduction release
  candidates with the batch reductions on keys before rc6; fixed from rc6.

## Symptom

`UNION` (without `ALL`), `EXCEPT` and `INTERSECT` compare rows on every column: two rows that
differ only on a column the query does not read are not duplicates. Their column pruning kept
only the columns read above them, which changes which rows are duplicates. With the batch
reductions on keys, a `count(*)` over a `UNION` read no column at all, and counted one row over an
empty input (the union became an aggregation without group key).

```sql
create table d (id int, name varchar);
create table f (id int, label varchar);
insert into d values (1, 'a');
insert into f values (1, 'b');

select count(*) from (select id, name from d union select id, label from f) x;
-- correct: 2, since (1, 'a') and (1, 'b') differ; with `name` and `label` pruned below the union,
-- the duplicates are removed on `id` alone (or on no column at all): 1
select id from (select id, name from d intersect select id, label from f) x;
-- correct: no row; with the columns pruned below the intersect: 1
```

## Cause

`LogicalUnion` (without `ALL`), `LogicalExcept` and `LogicalIntersect` pruned their inputs to the
columns required by their parent. Upstream turns these set operations into aggregations and joins
before the first column pruning, so the code is never reached there.

## Fix

These set operations keep every column of their inputs; the pruning is done by a projection
above them. Commit `057742f253` (fix(optimizer): never prune the columns below a set operation
without ALL), on the batch key reductions branch of the plan reduction prototype.

## Tests

Planner cases in `batch_plan_reduction_keys.yaml` (count over a union without `ALL`, count over
an except, one column of an intersect) and the differential test
`e2e_test/batch/plan_reduction_keys.slt`.

## Upstream status

Not reachable upstream as far as known; the pruning code is unchanged on `main` (not checked in
detail). No upstream issue.
