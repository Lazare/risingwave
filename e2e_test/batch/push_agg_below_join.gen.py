"""Generates `e2e_test/batch/push_agg_below_join.slt`, the differential test of
`batch_push_agg_below_join` (exact form of `AggJoinTransposeRule`: an aggregation over a join whose
other side is unique on the join columns runs before the join), `batch_partial_agg_below_join`
(partial form, chosen with the learned group ratios) and `batch_partial_agg_into_union`
(`AggUnionTransposeRule`: partial aggregation in the branches of a `UNION ALL`).

Every query runs twice, with all three variables off, then on, through `INSERT ... SELECT` (the
batch plan), and both results must hold the same rows with the same multiplicities, after each
round of changes of the data. The learning (`batch_cardinality_feedback_learning`) is on in both
runs; the group ratios that the partial forms need are learned from plain aggregations of the
tables, run before the queries.

Usage: python3 e2e_test/batch/push_agg_below_join.gen.py e2e_test/batch/push_agg_below_join.slt
"""
import sys

F, D, D2, E, A, B = 'paj_f', 'paj_d', 'paj_d2', 'paj_e', 'paj_a', 'paj_b'
VARS = ['batch_push_agg_below_join', 'batch_partial_agg_below_join', 'batch_partial_agg_into_union']

# (name, columns of the result, query)
CASES = [
    # Exact form.
    ('left_unique', 'k int, name varchar, s bigint, n bigint',
     f"select f.k, d.name, sum(f.x) as s, count(*) as n from {F} f left join {D} d on f.k = d.id "
     f"group by f.k, d.name"),
    ('inner_whole', 'id int, name varchar, m int, c bigint',
     f"select d.id, d.name, max(f.x) as m, count(distinct f.y) as c from {F} f join {D} d "
     f"on f.k = d.id group by d.id, d.name"),
    ('right_unique', 'k int, region int, s numeric',
     f"select f.k, d.region, sum(f.y) as s from {D} d right join {F} f on f.k = d.id "
     f"group by f.k, d.region"),
    ('two_unique', 'k int, k2 int, name varchar, w int, s bigint',
     f"select f.k, f.k2, d.name, d2.w, sum(f.x) as s from {F} f left join {D} d on f.k = d.id "
     f"left join {D2} d2 on f.k2 = d2.id and d2.ver = 1 group by f.k, f.k2, d.name, d2.w"),
    ('value_of_unique', 'k int, mn varchar, mx int, p bigint',
     f"select f.k, min(d.name) as mn, max(d.region) as mx, count(*) filter (where f.x > 0) as p "
     f"from {F} f left join {D} d on f.k = d.id group by f.k"),
    ('ordered_agg', 'k int, l varchar',
     f"select f.k, string_agg(f.x::varchar, ',' order by f.id) as l from {F} f left join {D} d "
     f"on f.k = d.id group by f.k"),
    ('distinct_rows', 'k int, name varchar',
     f"select distinct f.k, d.name from {F} f left join {D} d on f.k = d.id"),
    ('null_safe', 'k int, name varchar, s bigint',
     f"select f.k, d.name, sum(f.x) as s from {F} f left join {D} d on f.k is not distinct from d.id "
     f"group by f.k, d.name"),
    ('having', 'k int, b int, n bigint',
     f"select f.k, f.x % 3 as b, count(*) as n from {F} f left join {D} d on f.k = d.id "
     f"group by f.k, f.x % 3 having count(*) > 30"),
    ('kept_filtered_inner', 'k int, s bigint',
     f"select f.k, sum(f.x) as s from {F} f join {D} d on f.k = d.id where d.region = 1 group by f.k"),
    ('kept_optional_group', 'name varchar, n bigint',
     f"select d.name, count(*) as n from {F} f left join {D} d on f.k = d.id group by d.name"),
    # Partial form below a join.
    ('partial_inner', 'k int, s bigint, n bigint, m bigint',
     f"select f.k, sum(f.x) as s, count(*) as n, min(f.y) as m from {F} f join {E} e on f.k = e.k "
     f"group by f.k"),
    ('partial_left', 'k int, w int, s numeric, n bigint, mw int',
     f"select f.k, e.w, sum(f.y) as s, count(f.x) as n, max(e.w) as mw from {F} f left join {E} e "
     f"on f.k = e.k group by f.k, e.w"),
    ('partial_scalar', 'n bigint, a boolean, b int, s bigint',
     f"select count(*) as n, bool_and(f.x > -3) as a, bit_or(f.x) as b, sum(f.x) as s "
     f"from {F} f join {E} e on f.k = e.k"),
    ('partial_null_safe', 'k int, n bigint',
     f"select f.k, count(*) as n from {F} f join {E} e on f.k is not distinct from e.k group by f.k"),
    ('kept_partial_distinct', 'k int, c bigint',
     f"select f.k, count(distinct f.x) as c from {F} f join {E} e on f.k = e.k group by f.k"),
    # Partial aggregation in the branches of a union.
    ('union_scalar', 'n bigint, s bigint, mn int, p bigint',
     f"select count(*) as n, sum(x) as s, min(x) as mn, count(x) filter (where x > 2) as p "
     f"from (select k, x from {A} union all select k, x from {B}) u"),
    ('union_grouped', 'k int, n bigint, s bigint, mx int, o boolean',
     f"select k, count(*) as n, sum(x) as s, max(x) as mx, bool_or(x > 10) as o "
     f"from (select k, x from {A} union all select k, x from {B}) u group by k"),
    ('union_one_learned', 'k int, n bigint, cx bigint',
     f"select k, count(*) as n, count(x) as cx from (select k, x from {A} union all "
     f"select k, x from {F} where x > 0) u group by k"),
    ('union_projected', 'k int, s bigint',
     f"select k, sum(x2) as s from (select k, x * 2 as x2 from (select k, x from {A} union all "
     f"select k, x from {B}) u0) u group by k"),
    ('union_three', 'n bigint, s bigint',
     f"select count(*) as n, sum(x) as s from (select x from {A} union all select x from {B} "
     f"union all select x from {F} where k is null) u"),
]

# The plain aggregations from which the group ratios are learned.
OBSERVED = [
    f"select k, count(*) from {F} group by k",
    f"select k, count(*) from {A} group by k",
    f"select k, count(*) from {B} group by k",
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows, retry=False):
    head = "query " + types + (" retry 30 backoff 1s" if retry else "")
    out.append(f"{head}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def set_vars(value):
    for var in VARS:
        stmt(f"set {var} = {value}")


def run_all(title):
    out.append(f"# {title}: every query runs without, then with the variables.\n")
    for name, _, sql in CASES:
        stmt(f"delete from paj_{name}_off")
        stmt(f"delete from paj_{name}_on")
        stmt(f"insert into paj_{name}_off {sql}")
        set_vars('true')
        stmt(f"insert into paj_{name}_on {sql}")
        set_vars('false')
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"paj_{name}_on", f"paj_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as paj_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


def observe(title):
    out.append(f"# {title}: the group ratios of the plain aggregations are learned.\n")
    for sql in OBSERVED:
        stmt(sql)
    query('T',
          "select count(*) >= {} from rw_catalog.rw_batch_cardinality_learning "
          "where ratio_kind = 'group_ratio' and max_ratio < 0.1".format(len(OBSERVED)),
          ["t"], retry=True)


out.append("""# Differential test of `batch_push_agg_below_join`, `batch_partial_agg_below_join` and
# `batch_partial_agg_into_union`: every query runs through `INSERT ... SELECT` without, then with
# the variables, and both results must hold the same rows with the same multiplicities, after each
# round of changes of the data.
#
# Generated by `python3 e2e_test/batch/push_agg_below_join.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt("set query_mode = distributed")
stmt("set batch_cardinality_feedback_learning = true")
stmt(f"create table {F} (id int primary key, k int, k2 int, x int, y bigint)")
stmt(f"create table {D} (id int primary key, name varchar, region int)")
stmt(f"create table {D2} (id int, ver int, w int, primary key (id, ver))")
stmt(f"create table {E} (k int, w int)")
stmt(f"create table {A} (id int primary key, k int, x int)")
stmt(f"create table {B} (k int, x int)")
for name, cols, _ in CASES:
    stmt(f"create table paj_{name}_off ({cols})")
    stmt(f"create table paj_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULL keys, duplicates, rows without a match, negative values, few groups.\n")
stmt(f"insert into {F} select g, case when g % 97 = 0 then null else g % 20 end, g % 7, g % 13 - 3, "
     f"case when g % 11 = 0 then null else g end from generate_series(1, 2000) g")
stmt(f"insert into {D} select g, case when g % 6 = 0 then null else 'n' || (g % 5) end, g % 4 "
     f"from generate_series(0, 15) g")
stmt(f"insert into {D2} select g / 2, g % 2 + 1, case when g % 5 = 0 then null else g end "
     f"from generate_series(0, 13) g")
stmt(f"insert into {E} select g % 25, case when g % 7 = 0 then null else g % 4 end "
     f"from generate_series(1, 60) g")
stmt(f"insert into {E} values (null, 1), (null, 2)")
stmt(f"insert into {A} select g, g % 30, case when g % 17 = 0 then null else g % 23 end "
     f"from generate_series(1, 1500) g")
stmt(f"insert into {B} select case when g % 50 = 0 then null else g % 15 end, "
     f"case when g % 9 = 0 then null else g % 31 end from generate_series(1, 1500) g")
observe("First data")
run_all("First data")

query('I', f"select count(*) from paj_left_unique_on", ["21"])
query('I', f"select n from paj_union_scalar_on", ["3000"])

out.append("# Second data: keys moved, values set to NULL, rows deleted on every side.\n")
stmt(f"update {F} set k = null where id % 89 = 0")
stmt(f"update {F} set x = null where id % 10 = 3")
stmt(f"delete from {F} where k = 3")
stmt(f"update {D} set name = 'n1' where id = 7")
stmt(f"delete from {D} where id in (2, 9)")
stmt(f"delete from {D2} where ver = 1 and id = 3")
stmt(f"delete from {E} where k = 4")
stmt(f"insert into {E} values (4, null), (21, 3), (21, 3)")
stmt(f"delete from {A} where k = 5")
stmt(f"update {B} set x = null where k = 6")
run_all("Second data")

out.append("# Third data: the unique and joined sides emptied, one branch of the unions emptied.\n")
stmt(f"delete from {D}")
stmt(f"delete from {D2}")
stmt(f"delete from {E}")
stmt(f"delete from {B}")
run_all("Empty joined sides")

out.append("# Fourth data: the aggregated side emptied.\n")
stmt(f"insert into {D} values (1, 'one', 1), (2, null, 2)")
stmt(f"insert into {E} values (1, 1), (1, 1), (null, 3)")
stmt(f"delete from {F}")
stmt(f"delete from {A}")
run_all("Empty aggregated side")

out.append("# Cleanup.\n")
set_vars('false')
stmt("set batch_cardinality_feedback_learning = false")
for name, _, _ in reversed(CASES):
    stmt(f"drop table paj_{name}_on")
    stmt(f"drop table paj_{name}_off")
for table in (B, A, E, D2, D, F):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
