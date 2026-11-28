"""Generates `e2e_test/batch/set_operation_to_count.slt`, the differential test of
`batch_set_operation_to_count`
(rule `SetOperationToCountRule`: `INTERSECT` and `EXCEPT` as one counting aggregation over a `UNION ALL`).

Every query runs twice, with and without the variable, through `INSERT ... SELECT` (the batch
plan), and both results must hold the same rows with the same multiplicities, after each round of
changes of the data.

Usage: python3 e2e_test/batch/set_operation_to_count.gen.py e2e_test/batch/set_operation_to_count.slt
"""
import sys

P = 'scb_'
VAR = 'batch_set_operation_to_count'

TABLES = [('t', 'k int primary key, a int, b varchar, d decimal'), ('u', 'k int primary key, a int, b varchar, d decimal'), ('w', 'a int, b varchar'), ('x', 'id int primary key, a int')]

# (name, columns of the result with their types, query)
CASES = [
    # Rewritten by the rule.
    ('intersect', 'a int, b varchar', 'select a, b from {p}t intersect select a, b from {p}u'),
    ('except', 'a int, b varchar', 'select a, b from {p}t except select a, b from {p}u'),
    ('intersect3', 'a int, b varchar', 'select a, b from {p}t intersect select a, b from {p}u intersect select a, b from {p}w'),
    ('except3', 'a int, b varchar', 'select a, b from {p}t except select a, b from {p}u except select a, b from {p}w'),
    ('except_intersect', 'a int, b varchar', 'select * from ((select a, b from {p}t except select a, b from {p}u) intersect select a, b from {p}w) q'),
    ('in_subquery', 'id int', 'select id from {p}x where a in (select a from {p}t intersect select a from {p}u)'),
    ('exprs', 'a1 int, ub varchar', 'select a + 1 as a1, upper(b) as ub from {p}t intersect select a, b from {p}u'),
    ('except_one', 'a int', 'select a from {p}t except select a from {p}u'),
    ('except_keys', 'k int, a int', 'select k, a from {p}t except select k, a from {p}u'),
    # Left as they are by the rule.
    ('numeric', 'd decimal', 'select d from {p}t intersect select d from {p}u'),
]
CASES = [(name, cols, sql.format(p=P)) for name, cols, sql in CASES]

# Rounds of changes: (title, statements).
ROUNDS = [
    ('First data: NULLs, duplicates, rows in one input only', [
        "insert into {p}t values (1, 1, 'a', 1.0), (2, 1, 'a', 1.00), (3, 2, null, 2), (4, null, 'b', null), (5, null, null, 3), (6, 3, 'c', 4), (7, 4, 'D', 5)",
        "insert into {p}u values (1, 1, 'a', 1.00), (2, 2, null, 2.0), (3, null, null, null), (4, 5, 'e', 6), (5, 2, 'C', 7), (6, 1, 'a', 1)",
        "insert into {p}w values (1, 'a'), (1, 'a'), (null, null), (2, null), (5, 'e'), (null, 'b')",
        'insert into {p}x values (1, 1), (2, 2), (3, null), (4, 4), (5, 5), (6, 1)',
    ]),
    ('Second data: more rows, rows written again with an existing primary key', [
        "insert into {p}t values (8, 5, 'e', 6), (9, 4, 'd', 5), (1, 9, 'z', 9)",
        "insert into {p}u values (7, 3, 'c', 4), (8, null, 'b', null), (9, 4, 'd', 5)",
        "insert into {p}w values (3, 'c'), (4, 'd')",
    ]),
    ('Third data: updates to and from NULL, deletes', [
        'update {p}t set a = null where k in (3, 6)',
        'update {p}u set b = null where k = 4',
        "update {p}u set a = 1, b = 'a' where k = 3",
        'delete from {p}t where k = 2',
        'delete from {p}u where k in (1, 6)',
        'delete from {p}w where a is null',
    ]),
    ('Fourth data: the second input emptied, then the first one', [
        'delete from {p}u',
    ]),
    ('Fifth data: the second input filled again, the first one emptied', [
        "insert into {p}u values (1, 1, 'a', 1), (2, null, null, null), (3, 9, 'z', 9)",
        'delete from {p}t',
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
# Generated by `python3 e2e_test/batch/set_operation_to_count.gen.py <this file>`: edit the list of queries there
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
