"""Generates `e2e_test/batch/distributed_union.slt`, the differential test of
`batch_distributed_union` (a `UNION ALL` of a distributed batch query runs in parallel tasks
instead of one task: each input is sent to the tasks by a hash of the columns that the operator
above needs, or of all the columns). Every query runs twice, with and without the variable,
through `INSERT ... SELECT` (the batch plan), in local and in distributed query mode, and both
results must hold the same rows with the same multiplicities, after each round of changes of the
data.

Usage: python3 e2e_test/batch/distributed_union.gen.py e2e_test/batch/distributed_union.slt
"""
import sys

A, B, C, D, E = 'du_a', 'du_b', 'du_c', 'du_d', 'du_e'
VAR = 'batch_distributed_union'

# Three branches: two tables with a primary key, one without (duplicate rows).
U3 = (f"select id, k, v, s from {A} union all select id, k, v, s from {B} "
      f"union all select id, k, v, s from {C}")
# A branch that is always empty.
U_EMPTY = (f"select id, k, v, s from {A} union all select id, k, v, s from {E} "
           f"union all select id, k, v, s from {C} where v > 1000")

# (name, columns of the result, query)
CASES = [
    # No operator above: the result is gathered as before.
    ('plain', 'id int, k int, v int, s varchar', U3),
    ('plain_empty', 'id int, k int, v int, s varchar', U_EMPTY),
    # A hash aggregation above: each input is hashed on the group key.
    ('agg_k', 'k int, n bigint, sv bigint, ms varchar',
     f"select k, count(*) as n, sum(v) as sv, min(s) as ms from ({U3}) u group by k"),
    ('agg_k_empty', 'k int, n bigint, sv bigint',
     f"select k, count(*) as n, sum(v) as sv from ({U_EMPTY}) u group by k"),
    ('agg_two_keys', 'k int, s varchar, n bigint',
     f"select k, s, count(*) as n from ({U3}) u group by k, s"),
    ('agg_tag', 'tag int, k int, n bigint',
     f"select tag, k, count(*) as n from (select 1 as tag, k from {A} union all "
     f"select 2 as tag, k from {B} union all select 3 as tag, k from {E} "
     f"union all select 4 as tag, k from {C}) u group by tag, k"),
    ('having', 'k int',
     f"select k from ({U3}) u where k is not null group by k having count(*) > 1"),
    ('agg_filter_above', 'k int, n bigint',
     f"select k, count(*) as n from (select k, v from ({U3}) u where v > 0) x group by k"),
    ('agg_computed_key', 'k2 int, n bigint',
     f"select k2, count(*) as n from (select coalesce(k, -1) + 1 as k2 from ({U3}) u) x group by k2"),
    ('distinct_agg', 'k int, nd bigint',
     f"select k, count(distinct v) as nd from ({U3}) u group by k"),
    ('union_distinct', 'k int, v int',
     f"select k, v from {A} union select k, v from {C} union select k, v from {E}"),
    # Inputs that are aggregations, joins or values.
    ('agg_inputs', 'k int, n numeric',
     f"select k, sum(n) as n from (select k, count(*) as n from {A} group by k union all "
     f"select k, count(*) as n from {C} group by k) u group by k"),
    ('agg_inputs_other_key', 'n bigint, c bigint',
     f"select n, count(*) as c from (select k, count(*) as n from {A} group by k union all "
     f"select v, count(*) as n from {C} group by v) u group by n"),
    ('join_inputs', 'k int, n bigint',
     f"select k, count(*) as n from (select a.k from {A} a join {B} b on a.k = b.k union all "
     f"select k from {C}) u group by k"),
    ('values_input', 'k int, n bigint',
     f"select k, count(*) as n from (select k from {A} union all values (1), (null), (1), (42)) u "
     f"group by k"),
    ('global_agg_inputs', 'n bigint, m int',
     f"select count(*) as n, max(m) as m from (select max(k) as m from {A} union all "
     f"select max(k) as m from {C} union all select min(v) as m from {E}) u"),
    # A count, a top-n and a limit above: they run in two phases.
    ('count', 'n bigint',
     f"select count(*) as n from ({U3}) u"),
    ('count_empty', 'n bigint, nk bigint, sv bigint',
     f"select count(*) as n, count(k) as nk, sum(v) as sv from ({U_EMPTY}) u"),
    ('count_null', 'n bigint',
     f"select count(*) as n from (select * from ({U3}) u where s is null) x"),
    ('topn', 'k int, v int, s varchar',
     f"select k, v, s from ({U3}) u order by k nulls first, v nulls first, s nulls first limit 7"),
    ('topn_offset', 'k int, v int',
     f"select k, v from ({U3}) u order by v desc nulls last, k desc nulls last limit 4 offset 2"),
    ('limit_all', 'n bigint',
     f"select count(*) as n from (select * from ({U3}) u limit 1000) x"),
    # Joins above.
    ('join', 'k int, v int, w int',
     f"select u.k, u.v, d.w from ({U3}) u join {D} d on u.k = d.k"),
    ('left_join', 'k int, w int, v int',
     f"select d.k, d.w, u.v from {D} d left join ({U_EMPTY}) u on d.k = u.k"),
    ('full_join', 'k int, w int, v int',
     f"select d.k, d.w, u.v from {D} d full join ({U3}) u on d.k = u.k"),
    ('join_two_unions', 'k int, v1 int, v2 int',
     f"select x.k, x.v, y.v from (select k, v from {A} union all select k, v from {C}) x "
     f"join (select k, v from {B} union all select k, v from {D}) y on x.k = y.k"),
    ('lookup_join', 'k int, v int',
     f"select u.k, a.v from (select k from {B} union all select k from {C}) u join {A} a "
     f"on u.k = a.id"),
    ('semi_join', 'id int, k int',
     f"select id, k from {D} where k in (select k from {A} union all select v from {C})"),
    # A window above.
    ('window', 'k int, v int, s varchar, rn bigint',
     f"select k, v, s, row_number() over (partition by k order by v nulls first, s nulls first) "
     f"as rn from ({U3}) u"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    for mode in ('local', 'distributed'):
        out.append(f"# {title}, {mode} query mode: every query runs without, then with the "
                   f"variable.\n")
        stmt(f"set query_mode = {mode}")
        for name, _, sql in CASES:
            stmt(f"delete from du_{name}_off")
            stmt(f"delete from du_{name}_on")
            stmt(f"insert into du_{name}_off {sql}")
            stmt(f"set {VAR} = true")
            stmt(f"insert into du_{name}_on {sql}")
            stmt(f"set {VAR} = false")
        stmt("set query_mode = auto")
        out.append(f"# {title}, {mode} query mode: both results hold the same rows, each the same "
                   f"number of times.\n")
        for name, cols, _ in CASES:
            a, b, c = f"du_{name}_on", f"du_{name}_off", columns(cols)
            grouped = lambda t: f"select {c}, count(*) as du_rows from {t} group by {c}"
            query('III',
                  f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
                  f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
                  f"  (select count(*) from {a}) - (select count(*) from {b})",
                  ["0 0 0"])


out.append("""# Differential test of `batch_distributed_union`: every query runs through
# `INSERT ... SELECT` without, then with the variable, in local and in distributed query mode,
# and both results must hold the same rows with the same multiplicities, after each round of
# changes of the data.
#
# Generated by `python3 e2e_test/batch/distributed_union.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {A} (id int primary key, k int, v int, s varchar)")
stmt(f"create table {B} (id int primary key, k int, v int, s varchar)")
stmt(f"create table {C} (id int, k int, v int, s varchar)")
stmt(f"create table {D} (id int primary key, k int, w int, v int)")
stmt(f"create table {E} (id int primary key, k int, v int, s varchar)")
for name, cols, _ in CASES:
    stmt(f"create table du_{name}_off ({cols})")
    stmt(f"create table du_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULLs, duplicate rows, keys present in several tables, a key 0.\n")
stmt(f"insert into {A} values (1, 1, 5, 'x'), (2, 1, 0, 'y'), (3, null, 2, null), "
     f"(4, null, null, 'x'), (5, 2, 7, 'z'), (6, 0, 3, 'x'), (7, 9, -1, null), (8, 3, 3, 'y')")
stmt(f"insert into {B} values (1, 1, 5, 'x'), (2, 2, 1, 'y'), (3, null, null, null), "
     f"(4, 0, 0, 'z'), (5, 7, 9, 'x'), (6, 1, 5, 'x')")
stmt(f"insert into {C} values (1, 1, 5, 'x'), (1, 1, 5, 'x'), (2, null, 2, null), "
     f"(null, null, null, null), (null, null, null, null), (3, 2, 7, 'z'), (4, 5, 1, 'w'), "
     f"(5, 0, 3, 'x')")
stmt(f"insert into {D} values (1, 1, 10, 1), (2, 2, 20, 2), (3, null, 30, 3), (4, 0, 40, 0), "
     f"(5, 8, 50, null), (6, 1, 60, 5)")
run_all("First data")

query('I', f"select count(*) from du_plain_on", ["22"])
query('I', f"select n from du_count_on", ["22"])

out.append("# Second data: more rows in every table, rows written again with an existing key.\n")
stmt(f"insert into {A} values (9, 4, 4, 'w'), (10, 4, 4, 'w'), (1, 2, 6, 'x')")
stmt(f"insert into {B} values (7, null, 1, 'y'), (8, 9, 9, 'z'), (1, 3, 3, null)")
stmt(f"insert into {C} values (1, 1, 5, 'x'), (6, 7, 0, 'y'), (7, null, 8, null)")
stmt(f"insert into {D} values (7, 4, 70, 4), (8, null, 80, null)")
stmt(f"insert into {E} values (1, 1, 2000, 'e')")
run_all("Second data")

out.append("# Third data: keys set to NULL and back, deletes, one table emptied.\n")
stmt(f"update {A} set k = null where id in (5, 6)")
stmt(f"update {A} set k = 0 where id in (3, 4)")
stmt(f"update {B} set v = null where id = 2")
stmt(f"delete from {C} where k is null")
stmt(f"delete from {B} where id in (4, 5)")
stmt(f"delete from {E}")
run_all("Third data")

out.append("# Fourth data: only one branch left.\n")
stmt(f"delete from {B}")
stmt(f"delete from {C}")
run_all("Only the first branch")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table du_{name}_on")
    stmt(f"drop table du_{name}_off")
for table in (E, D, C, B, A):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
