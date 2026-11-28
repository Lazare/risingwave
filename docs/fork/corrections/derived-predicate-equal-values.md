# Predicates derived through `a = b` on types whose equal values differ

- **Kind**: upstream bug (optimizer, predicate pushdown), found by a survey of the optimizer rules
  of PostgreSQL and Calcite on 2026-11-28.
- **Affected versions**: upstream `v3.1.2` and `main` (2026-11-28, code from 2022); every fork
  build up to the plan reduction release candidate rc8 included.

## Symptom

When a join has the equality `a = b` and a predicate uses only `a`, the optimizer also applies
the same predicate to `b`. Equal values are not always indistinguishable: `1.0 = 1.00` for
`numeric` but their texts differ, `-0.0 = 0.0` for floats, `'1 day' = '24 hours'` for intervals.
A predicate that tells such values apart then removes rows of the other side that the join
matches. Batch queries and materialized views are both affected.

```sql
create table l (id int, k numeric);
create table r (id int, k numeric);
insert into l values (1, 1.0);
insert into r values (1, 1.00), (2, 1.0);

select l.id, r.id from l join r on l.k = r.k where l.k::varchar = '1.0';
-- wrong: (1, 2)       correct: (1, 1), (1, 2)

create table li (id int, iv interval);
create table ri (id int, iv interval);
insert into li values (1, interval '1 day');
insert into ri values (1, interval '24 hours'), (2, interval '1 day');

select li.id, ri.id from li join ri on li.iv = ri.iv where extract(hour from li.iv) = 0;
-- wrong: (1, 2)       correct: (1, 1), (1, 2)
```

## Cause

`derive_predicate_from_eq_condition` (`src/frontend/src/optimizer/plan_node/logical_join.rs`),
used by `LogicalJoin::predicate_pushdown`, rewrote any pure predicate whose columns are all join
keys onto the keys of the other side, requiring only that the two keys have the same type.

## Fix

A predicate is derived only when it gives the same result on two equal values
(`respects_column_equality`):

- its columns are of a type whose equal values are identical: `boolean`, `smallint`, `int`,
  `bigint`, `rw_int256`, `serial`, `varchar` (compared byte by byte, no collation), `bytea`,
  `date`, `time`, `timestamp`, `timestamptz` (one representation per instant; the session time
  zone changes the text of both sides the same way);
- or a column is only tested for NULL (`IS [NOT] NULL`), whatever its type;
- or a column of `numeric`, `real`, `double precision` or `interval`, whose order agrees with
  its equality, is only compared (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`,
  `IN (...)`) with values that do not depend on any column.

Anything else (casts to text, arithmetic, `extract`, `jsonb`, structs, lists, maps) is no longer
derived. Range and equality predicates on numeric keys, the common case, are still derived.

Commit `5e144b24d2` on branch `proto/plan-reduction-fix-correctness` (applies to upstream code
alone).

## Tests

- Planner: `batch_correctness_fixes.yaml`, section `derive` (not derived: text of a numeric, of a
  float, `extract` of an interval, arithmetic, a mixed predicate; still derived: comparisons, `IN`,
  `BETWEEN`, null tests, any predicate on integer, varchar and timestamp keys).
- End to end: `e2e_test/batch/correctness_fixes.slt` (numeric `1.0` / `1.00`, float `0` / `-0`,
  interval `1 day` / `24 hours`, from either side of the join).

## Upstream status

Not fixed on `main` (2026-11-28): `derive_predicate_from_eq_condition` is unchanged. No upstream
issue found.
