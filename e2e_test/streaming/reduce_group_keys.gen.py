"""Generates `e2e_test/streaming/reduce_group_keys.slt`, the differential test of
`streaming_reduce_group_keys`
(rule `AggGroupKeyReduceRule`: the group key columns that the others determine are removed, aggregates of values of the group replaced).

Every query is materialized with and without the variable, and both materialized views must hold
the same rows, with the same multiplicities, as the batch query, after the backfill and after each
round of changes.

Usage: python3 e2e_test/streaming/reduce_group_keys.gen.py e2e_test/streaming/reduce_group_keys.slt
"""
import sys

P = 'gks_'
VAR = 'streaming_reduce_group_keys'

TABLES = [('o', 'id int primary key, placed date, cust int'), ('i', 'iid int primary key, o_id int, qty int'), ('t', 'k int primary key, a int, b int, c int')]

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('join_key', 'id, placed, n, mq', 'select o.id, o.placed, count(i.qty) as n, max(i.qty) as mq from {p}o o join {p}i i on i.o_id = o.id group by o.id, o.placed'),
    ('eq_key', 'id, o_id, n', 'select o.id, i.o_id, count(*) as n from {p}o o join {p}i i on i.o_id = o.id group by o.id, i.o_id'),
    ('const_key', 'a, b, n', 'select a, b, count(*) as n from {p}t where b = 3 group by a, b'),
    ('all_const', 'a, b, n', 'select a, b, count(*) as n from {p}t where a = 1 and b = 3 group by a, b'),
    ('literal_key', 'a, tag, n', "select a, 'x' as tag, count(*) as n from {p}t group by a, tag"),
    ('max_key', 'a, mx, mn, n', 'select a, max(a) as mx, min(a) as mn, count(*) as n from {p}t group by a'),
    ('max_determined', 'id, p, n', 'select o.id, max(o.placed) as p, count(i.qty) as n from {p}o o join {p}i i on i.o_id = o.id group by o.id'),
    ('last_value_left', 'id, lc, n', 'select o.id, last_value(o.cust order by i.iid) as lc, count(i.iid) as n from {p}o o left join {p}i i on i.o_id = o.id group by o.id'),
    ('first_value_key', 'a, f, n', 'select a, first_value(a order by b) as f, count(c) as n from {p}t group by a'),
    ('simple_const', 'mb, n', 'select max(b) as mb, count(*) as n from {p}t where b = 3'),
    ('sub_agg', 'a, mb, n', 'select a, mb, count(*) as n from (select a, max(b) as mb from {p}t group by a) x group by a, mb'),
    # Left as they are by the rule.
    ('kept_left', 'iid, placed, n', 'select i.iid, o.placed, count(*) as n from {p}i i left join {p}o o on i.o_id = o.id group by i.iid, o.placed'),
    ('kept_filter', 'a, mx', 'select a, max(a) filter (where b > 0) as mx from {p}t group by a'),
    ('kept_keys', 'a, b, n', 'select a, b, count(*) as n from {p}t group by a, b'),
]
CASES = [(name, cols, sql.format(p=P)) for name, cols, sql in CASES]

# Rounds of changes: (title, statements); the first one runs before the views are created.
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


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    grouped = lambda t: f"select {cols}, count(*) as {P}rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def compare_all(title):
    out.append(f"# {title}: with and without the rule, every materialized view holds the rows of "
               f"the batch query.\n")
    for name, cols, sql in CASES:
        same_rows(f"{P}{name}_on", f"{P}{name}_off", cols)
        same_rows(f"{P}{name}_on", f"({sql}) q", cols)


out.append(f"""# Differential test of `{VAR}`: every query is materialized twice, with and without the
# variable, and both materialized views must hold the rows of the batch query, with the same
# multiplicities, after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/reduce_group_keys.gen.py <this file>`: edit the list of queries
# there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
for name, columns in TABLES:
    stmt(f"create table {P}{name} ({columns})")
title, statements = ROUNDS[0]
out.append(f"# {title}: before the materialized views are created, through the backfill.\n")
for sql in statements:
    stmt(sql.format(p=P))

for sfx in ('off', 'on'):
    stmt(f"set {VAR} = {'true' if sfx == 'on' else 'false'}")
    for name, _, sql in CASES:
        stmt(f"create materialized view {P}{name}_{sfx} as {sql}")
stmt(f"set {VAR} = false")
compare_all("After the backfill")

for title, statements in ROUNDS[1:]:
    out.append(f"# {title}.\n")
    for sql in statements:
        stmt(sql.format(p=P))
    compare_all(f"After: {title}")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view {P}{name}_{sfx}")
for name, _ in reversed(TABLES):
    stmt(f"drop table {P}{name}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
