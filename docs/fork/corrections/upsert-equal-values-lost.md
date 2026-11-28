# Upsert or UPDATE between equal values with different texts was dropped (ghost rows downstream)

- **Kind**: upstream bug (streaming, table materialization), found by the differential test of
  the transitive predicates of the plan reduction prototype on 2026-11-28 (independent of any
  fork variable).
- **Affected versions**: upstream `v3.1.2` and `main` (2026-11-28); every fork build up to the
  plan reduction release candidate rc8 included.

## Symptom

Writing a row again with its primary key (`INSERT` over an existing key, which overwrites it)
with values equal to the previous ones but written differently (`1.0` then `1.00` for `numeric`,
`0` then `-0` for floats, `'1 day'` then `'24 hours'` for intervals) was treated as a no-op:

```sql
create table t (id int primary key, k numeric);
create materialized view m as select id from t where k::varchar = '1.0';
insert into t values (1, 1.0);
insert into t values (1, 1.00);
select k::varchar from t;      -- wrong: 1.0     correct: 1.00
select * from m;               -- wrong: 1       correct: no row
delete from t;
select * from m;               -- wrong: 1 (for ever)   correct: no row

insert into t values (1, 1.00);
update t set k = 1.0;
select k::varchar from t;      -- wrong: 1.00    correct: 1.0
```

## Cause

The materialization of a table with a primary key accumulates the changes of a chunk in a
`ChangeBuffer`, whose `into_records` drops the updates whose old and new rows are equal
(`old_row != new_row`, the SQL equality of each value). The dropped update was neither written to
the storage nor sent downstream, but the materialization cache had already taken the new row.
The delete then emitted the cached new row (`1.00`), which the filter of the view rejects, so the
view never deleted its row. The batch `UPDATE` executor also skipped the rows whose new values
were equal to the old ones (`row_delete == row_insert`), so an `UPDATE` to an equal value written
differently changed nothing.

## Fix

`risingwave_common::row::identical` compares two rows by value and representation (equal values
whose type has a single representation, otherwise equal values with the same text). The change
buffer drops only identical updates (trait `Identical`, implemented for the row types it holds),
and so do the other places that drop no-op updates (see
[noop-update-elimination-equal-values.md](noop-update-elimination-equal-values.md)), as well as
the batch `UPDATE` executor. Commits on branch `proto/plan-reduction-fix-correctness`, all
applying to upstream code alone: `34bb3a7631` (streaming), `a33e0d3f2d` (clippy fix of it, to
squash into it), `993657d223` (batch `UPDATE`).

## Tests

`e2e_test/streaming/correctness_fixes.slt`: the table itself and two materialized views through an
upsert of equal values (numeric and float), an update back, and a delete;
`e2e_test/batch/correctness_fixes.slt`: `UPDATE` and upsert to equal values. Unit test
`test_identical` in `src/common/src/row/mod.rs`.

## Upstream status

Not fixed on `main` (2026-11-28): `ChangeBuffer::into_records` still compares with `!=`, and the
batch `UPDATE` executor with `==`. No upstream issue found.
