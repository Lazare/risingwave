"""Generates `e2e_test/batch/or_join_to_union.slt`, the differential test of `batch_or_join_to_union`
(rule `OrJoinToUnionRule`: a join without equality whose condition is an `OR` of equi-conditions
becomes equi-joins on the disjuncts). Every query runs twice, with and without the variable,
through `INSERT ... SELECT` (the batch plan, a nested-loop join without the variable), and both
results must hold the same rows with the same multiplicities, after each round of changes.

Usage: python3 e2e_test/batch/or_join_to_union.gen.py e2e_test/batch/or_join_to_union.slt
"""
import sys

A, B, N = 'ojt_a', 'ojt_b', 'ojt_n'
VAR = 'batch_or_join_to_union'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('inner', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.id = b.id1 or a.id = b.id2"),
    ('inner_extra', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on (a.id = b.id1 or a.id = b.id2) and a.x > b.y"),
    ('inner_multi', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on (a.id = b.id1 and a.x = b.y) or (a.k = b.id2 and a.x > 0)"),
    ('inner_three', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.id = b.id1 or a.id = b.id2 or a.k = b.y"),
    ('inner_where', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a, {B} b where a.id = b.id1 or a.k = b.id2"),
    ('agg', 'n bigint, s bigint',
     f"select count(*) as n, coalesce(sum(b.y), 0) as s from {A} a join {B} b on a.id = b.id1 or a.id = b.id2"),
    ('left', 'id int, x int, bid int, y int',
     f"select a.id, a.x, b.id as bid, b.y from {A} a left join {B} b on a.id = b.id1 or a.id = b.id2"),
    ('left_extra', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a left join {B} b on (a.id = b.id1 or a.id = b.id2) and b.y > 0"),
    ('left_three', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a left join {B} b on a.id = b.id1 or a.id = b.id2 or a.k = b.y"),
    ('semi', 'id int',
     f"select a.id from {A} a where exists (select 1 from {B} b where a.id = b.id1 or a.id = b.id2)"),
    ('semi_three', 'id int, x int',
     f"select a.id, a.x from {A} a where exists (select 1 from {B} b where a.id = b.id1 or a.id = b.id2 or a.k = b.y)"),
    ('semi_extra', 'id int',
     f"select a.id from {A} a where exists (select 1 from {B} b where (a.id = b.id1 or a.k = b.id2) and b.y > a.x)"),
    ('anti', 'id int',
     f"select a.id from {A} a where not exists (select 1 from {B} b where a.id = b.id1 or a.id = b.id2)"),
    ('anti_three', 'id int',
     f"select a.id from {A} a where not exists (select 1 from {B} b where a.id = b.id1 or a.id = b.id2 or a.k = b.y)"),
    ('dup_inner', 'v int, bid int',
     f"select n.v, b.id as bid from {N} n join {B} b on n.k = b.id1 or n.k = b.id2"),
    ('dup_left', 'v int, bid int',
     f"select n.v, b.id as bid from {N} n left join {B} b on n.k = b.id1 or n.k = b.id2"),
    ('dup_semi', 'k int, v int',
     f"select n.k, n.v from {N} n where exists (select 1 from {B} b where n.k = b.id1 or n.k = b.id2)"),
    ('dup_anti', 'k int, v int',
     f"select n.k, n.v from {N} n where not exists (select 1 from {B} b where n.k = b.id1 or n.k = b.id2)"),
    # Left as they are by the rule (an equality outside the `OR` makes a hash join).
    ('kept_eq', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.y and (a.id = b.id1 or a.id = b.id2)"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    out.append(f"# {title}: every query runs without, then with the variable.\n")
    for name, _, sql in CASES:
        stmt(f"delete from ojt_{name}_off")
        stmt(f"delete from ojt_{name}_on")
        stmt(f"insert into ojt_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into ojt_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"ojt_{name}_on", f"ojt_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as ojt_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


def first_data():
    # b holds pairs matched by the first disjunct only, the second only, both (id1 = id2), none,
    # and NULLs in either key.
    stmt(f"insert into {A} values (1, 10, 5), (2, 20, 0), (3, 10, 7), (4, null, 1), (5, 30, null), "
         f"(6, 2, 3), (7, 7, 9)")
    stmt(f"insert into {B} values (1, 1, 2, 3), (2, 2, 2, 1), (3, null, 3, 10), (4, 3, null, 20), "
         f"(5, null, null, 2), (6, 9, 8, 30), (7, 1, 1, 6), (8, 4, 6, 7), (9, 5, 3, null)")
    stmt(f"insert into {N} values (1, 1), (1, 1), (2, 2), (null, 3), (3, 4), (9, 5), (2, null)")


def second_data():
    stmt(f"insert into {A} values (8, 20, 4), (9, null, null), (1, 30, 2)")
    stmt(f"insert into {B} values (10, 8, 9, 20), (11, 9, 9, null), (2, 7, 2, 30)")
    stmt(f"insert into {N} values (8, 6), (1, 1)")


def third_data():
    # Pairs move between the disjuncts: keys swapped, set equal, set to NULL.
    stmt(f"update {B} set id1 = id2 where id in (4, 8)")
    stmt(f"update {B} set id2 = null where id in (1, 7)")
    stmt(f"update {B} set id1 = 6, id2 = 7 where id = 6")
    stmt(f"update {B} set y = 10 where id = 3")
    stmt(f"update {A} set k = 3 where id = 4")
    stmt(f"update {A} set x = 30 where id in (1, 6)")
    stmt(f"delete from {B} where id in (5, 9)")
    stmt(f"delete from {A} where id = 3")
    stmt(f"delete from {N} where k = 1 and v = 1")


out.append("""# Differential test of `batch_or_join_to_union`: every query runs through `INSERT ... SELECT`
# without, then with the variable, and both results must hold the same rows with the same
# multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/or_join_to_union.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {A} (id int primary key, k int, x int)")
stmt(f"create table {B} (id int primary key, id1 int, id2 int, y int)")
stmt(f"create table {N} (k int, v int)")
for name, cols, _ in CASES:
    stmt(f"create table ojt_{name}_off ({cols})")
    stmt(f"create table ojt_{name}_on ({cols})")

run_all("Empty tables")
out.append("# First data: pairs matched by one disjunct, by both, by none; NULL keys; duplicates.\n")
first_data()
run_all("First data")
out.append("# A pair matched by both disjuncts is output once.\n")
query('I', "select count(*) from ojt_inner_on where id = 2 and bid = 2", ["1"])
query('I', "select count(*) from ojt_inner_on", ["10"])
out.append("# Second data: more rows, rows written again with an existing primary key.\n")
second_data()
run_all("Second data")
out.append("# Third data: pairs moved between the disjuncts, NULL keys, deletes.\n")
third_data()
run_all("Third data")
out.append("# Fourth data: empty right side, then empty left side.\n")
stmt(f"delete from {B}")
run_all("Empty right side")
stmt(f"delete from {A}")
stmt(f"delete from {N}")
run_all("Empty left side")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table ojt_{name}_on")
    stmt(f"drop table ojt_{name}_off")
for table in (N, B, A):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
