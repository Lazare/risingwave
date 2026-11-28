"""Generates `e2e_test/batch/reduce_group_keys.slt`, the differential test of
`batch_reduce_group_keys`
(rule `AggGroupKeyReduceRule`: the group key columns that the others determine are removed, aggregates of values of the group replaced).

Every query runs twice, with and without the variable, through `INSERT ... SELECT` (the batch
plan), and both results must hold the same rows with the same multiplicities, after each round of
changes of the data.

Usage: python3 e2e_test/batch/reduce_group_keys.gen.py e2e_test/batch/reduce_group_keys.slt
"""
import sys

P = 'gkb_'
VAR = 'batch_reduce_group_keys'

TABLES = [('o', 'id int primary key, placed date, cust int'), ('i', 'iid int primary key, o_id int, qty int'), ('t', 'k int primary key, a int, b int, c int')]

# (name, columns of the result with their types, query)
CASES = [
    # Rewritten by the rule.
    ('join_key', 'id int, placed date, n bigint, mq int', 'select o.id, o.placed, count(i.qty) as n, max(i.qty) as mq from {p}o o join {p}i i on i.o_id = o.id group by o.id, o.placed'),
    ('eq_key', 'id int, o_id int, n bigint', 'select o.id, i.o_id, count(*) as n from {p}o o join {p}i i on i.o_id = o.id group by o.id, i.o_id'),
    ('const_key', 'a int, b int, n bigint', 'select a, b, count(*) as n from {p}t where b = 3 group by a, b'),
    ('all_const', 'a int, b int, n bigint', 'select a, b, count(*) as n from {p}t where a = 1 and b = 3 group by a, b'),
    ('literal_key', 'a int, tag varchar, n bigint', "select a, 'x' as tag, count(*) as n from {p}t group by a, tag"),
    ('max_key', 'a int, mx int, mn int, n bigint', 'select a, max(a) as mx, min(a) as mn, count(*) as n from {p}t group by a'),
    ('max_determined', 'id int, p date, n bigint', 'select o.id, max(o.placed) as p, count(i.qty) as n from {p}o o join {p}i i on i.o_id = o.id group by o.id'),
    ('last_value_left', 'id int, lc int, n bigint', 'select o.id, last_value(o.cust order by i.iid) as lc, count(i.iid) as n from {p}o o left join {p}i i on i.o_id = o.id group by o.id'),
    ('first_value_key', 'a int, f int, n bigint', 'select a, first_value(a order by b) as f, count(c) as n from {p}t group by a'),
    ('simple_const', 'mb int, n bigint', 'select max(b) as mb, count(*) as n from {p}t where b = 3'),
    ('dist_key', 'id int, cust int, placed date, n bigint', 'select o.id, o.cust, o.placed, count(*) as n from {p}o o join {p}i i on i.o_id = o.id and i.iid = o.cust group by o.id, o.cust, o.placed'),
    ('sub_agg', 'a int, mb int, n bigint', 'select a, mb, count(*) as n from (select a, max(b) as mb from {p}t group by a) x group by a, mb'),
    # Left as they are by the rule.
    ('kept_left', 'iid int, placed date, n bigint', 'select i.iid, o.placed, count(*) as n from {p}i i left join {p}o o on i.o_id = o.id group by i.iid, o.placed'),
    ('kept_filter', 'a int, mx int', 'select a, max(a) filter (where b > 0) as mx from {p}t group by a'),
    ('kept_keys', 'a int, b int, n bigint', 'select a, b, count(*) as n from {p}t group by a, b'),
    ('sum_join_key', 'id int, placed date, s bigint', 'select o.id, o.placed, sum(i.qty) as s from {p}o o join {p}i i on i.o_id = o.id group by o.id, o.placed'),
]
CASES = [(name, cols, sql.format(p=P)) for name, cols, sql in CASES]

# Rounds of changes: (title, statements).
ROUNDS = [
    ('First data: NULLs, orders without items, items without orders', [
        "insert into {p}o values (1, '2026-01-01', 10), (2, '2026-01-02', null), (3, null, 30), (4, '2026-01-04', 40), (5, '2026-01-05', 10)",
        'insert into {p}i values (1, 1, 5), (2, 1, null), (3, 2, 7), (4, 3, 1), (5, 3, 1), (6, 9, 2), (7, null, 3), (8, 4, 0)',
        'insert into {p}t values (1, 1, 3, 1), (2, 1, 3, 2), (3, 2, 3, null), (4, null, 3, 4), (5, null, null, 5), (6, 2, 4, 6), (7, 1, 1, 7), (8, 3, null, null)',
    ]),
    ('Second data: more rows, rows written again with an existing primary key', [
        "insert into {p}o values (6, '2026-01-06', 60), (1, '2026-02-01', 11)",
        'insert into {p}i values (9, 6, 4), (10, 5, 5), (3, 1, 8)',
        'insert into {p}t values (9, 4, 3, 9), (1, 1, 2, 1)',
    ]),
    ('Third data: values of the determined columns changed, to and from NULL, deletes', [
        "update {p}o set placed = '2026-03-01' where id in (2, 4)",
        'update {p}o set placed = null, cust = null where id = 5',
        "update {p}o set placed = '2026-03-03', cust = 33 where id = 3",
        'update {p}i set o_id = 4 where iid in (4, 6)',
        'delete from {p}i where iid = 1',
        'delete from {p}o where id = 6',
        'update {p}t set b = 3 where k in (5, 6)',
        'update {p}t set a = null where k = 2',
        'delete from {p}t where k = 4',
    ]),
    ('Fourth data: the items emptied, then the orders', [
        'delete from {p}i',
        'delete from {p}o',
        'delete from {p}t where b = 3',
    ]),
    ('Fifth data: filled again', [
        "insert into {p}o values (1, '2026-01-01', 10), (2, null, null)",
        'insert into {p}i values (1, 1, 5), (2, 2, null), (3, 2, 2)',
        'insert into {p}t values (1, 1, 3, 1), (2, null, 3, null)',
    ]),
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
        stmt(f"delete from {P}{name}_off")
        stmt(f"delete from {P}{name}_on")
        stmt(f"insert into {P}{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into {P}{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"{P}{name}_on", f"{P}{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as {P}rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append(f"""# Differential test of `{VAR}`: every query runs through `INSERT ... SELECT` without, then
# with the variable, and both results must hold the same rows with the same multiplicities, after
# each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/reduce_group_keys.gen.py <this file>`: edit the list of queries there
# and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
for name, columns_ in TABLES:
    stmt(f"create table {P}{name} ({columns_})")
for name, cols, _ in CASES:
    stmt(f"create table {P}{name}_off ({cols})")
    stmt(f"create table {P}{name}_on ({cols})")

run_all("Empty tables")
for title, statements in ROUNDS:
    out.append(f"# {title}.\n")
    for sql in statements:
        stmt(sql.format(p=P))
    run_all(title)

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table {P}{name}_on")
    stmt(f"drop table {P}{name}_off")
for name, _ in reversed(TABLES):
    stmt(f"drop table {P}{name}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
