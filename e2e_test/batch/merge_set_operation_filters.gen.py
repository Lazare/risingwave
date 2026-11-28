"""Generates `e2e_test/batch/merge_set_operation_filters.slt`, the differential test of
`batch_merge_set_operation_filters`
(rule `SetOperationFilterMergeRule`: `UNION`, `INTERSECT`, `EXCEPT` of filters over the same table become one filter).

Every query runs twice, with and without the variable, through `INSERT ... SELECT` (the batch
plan), and both results must hold the same rows with the same multiplicities, after each round of
changes of the data.

Usage: python3 e2e_test/batch/merge_set_operation_filters.gen.py e2e_test/batch/merge_set_operation_filters.slt
"""
import sys

P = 'sfb_'
VAR = 'batch_merge_set_operation_filters'

TABLES = [('t', 'k int primary key, a int, b int, s varchar, d decimal'), ('u', 'k int primary key, a int, b int, s varchar, d decimal')]

# (name, columns of the result with their types, query)
CASES = [
    # Rewritten by the rule.
    ('union_rows', 'k int, a int, b int, s varchar, d decimal', 'select * from {p}t where a > 1 union select * from {p}t where b < 5'),
    ('union_col', 'a int', 'select a from {p}t where a > 1 union select a from {p}t where b < 5'),
    ('union3', 'a int', 'select a from {p}t where a > 1 union select a from {p}u union select a from {p}t where b < 5'),
    ('union_all_rows', 'a int, s varchar', 'select a, s from {p}t where a > 1 union select a, s from {p}t'),
    ('intersect_key', 'k int, a int', 'select k, a from {p}t where a > 1 intersect select k, a from {p}t where b < 5'),
    ('intersect_out', 'a int, b int', 'select a, b from {p}t where a > 1 intersect select a, b from {p}t where b < 5 or b is null'),
    ('intersect_common', 'a int', 'select a from {p}t where b > 0 and a > 1 intersect select a from {p}t where b > 0 and a < 5'),
    ('except_key', 'k int, a int', 'select k, a from {p}t where a > 1 except select k, a from {p}t where b < 5'),
    ('except_out', 'a int, s varchar', "select a, s from {p}t where a > 1 except select a, s from {p}t where s like 'x%'"),
    ('except_later', 'k int, a int', 'select k, a from {p}t except select k, a from {p}u where a < 0 except select k, a from {p}u where a > 10'),
    ('nulls', 'a int, b int', 'select a, b from {p}t where a is null intersect select a, b from {p}t where b is null'),
    # Left as they are by the rule.
    ('kept_intersect', 'a int', 'select a from {p}t where a > 1 intersect select a from {p}t where b < 5'),
    ('kept_except', 'a int', 'select a from {p}t where a > 1 except select a from {p}t where b < 5'),
    ('kept_numeric', 'd decimal', "select d from {p}t where d > 1 intersect select d from {p}t where d::varchar = '2.0'"),
    ('kept_exprs', 'a int', 'select a from {p}t where a > 1 intersect select b from {p}t where b < 5'),
    ('kept_tables', 'k int, a int', 'select k, a from {p}t where a > 1 intersect select k, a from {p}u where a < 5'),
]
CASES = [(name, cols, sql.format(p=P)) for name, cols, sql in CASES]

# Rounds of changes: (title, statements).
ROUNDS = [
    ('First data: NULLs, duplicate values, rows passing one filter, both or none', [
        "insert into {p}t values (1, 2, 1, 'xa', 1.0), (2, 2, 7, 'xa', 2.0), (3, 0, 3, 'y', 2.00), (4, null, 4, null, null), (5, 3, null, 'xb', 3), (6, null, null, 'z', 2.0), (7, 6, 9, 'x', 2), (8, 2, 1, 'xa', 1.0), (9, 1, 0, 'q', 0.5)",
        "insert into {p}u values (1, 2, 0, 'a', 1), (2, -1, 0, 'b', 2), (3, 11, 1, 'c', 3), (5, null, 1, null, null), (7, 6, 1, 'x', 2)",
    ]),
    ('Second data: more rows, rows written again with an existing primary key', [
        "insert into {p}t values (10, 4, 2, 'w', 4), (11, 4, 8, 'xw', 4.0), (1, 5, 6, 'v', 5)",
        "insert into {p}u values (10, -5, 2, 'd', 4), (4, 12, 0, 'e', 1)",
    ]),
    ('Third data: values moved across the filters, to and from NULL, deletes', [
        'update {p}t set a = 0 where k in (2, 5)',
        'update {p}t set b = null where k in (3, 10)',
        "update {p}t set s = 'xy' where k = 6",
        'update {p}t set a = 7, b = 1 where k = 4',
        'delete from {p}t where k in (7, 9)',
        'update {p}u set a = 3 where k = 2',
        'delete from {p}u where k = 3',
    ]),
    ('Fourth data: the second table emptied, then the first one', [
        'delete from {p}u',
        'delete from {p}t',
    ]),
    ('Fifth data: both tables filled again', [
        "insert into {p}t values (1, 2, 1, 'xa', 1.0), (2, null, 9, null, 2.0), (3, 3, 3, 'x', 2)",
        "insert into {p}u values (1, 2, 1, 'xa', 1), (3, 30, 3, 'x', 2)",
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
# Generated by `python3 e2e_test/batch/merge_set_operation_filters.gen.py <this file>`: edit the list of queries there
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
