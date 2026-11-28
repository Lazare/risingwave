"""Generates `e2e_test/streaming/merge_set_operation_filters.slt`, the differential test of
`streaming_merge_set_operation_filters`
(rule `SetOperationFilterMergeRule`: `UNION`, `INTERSECT`, `EXCEPT` of filters over the same table become one filter).

Every query is materialized with and without the variable, and both materialized views must hold
the same rows, with the same multiplicities, as the batch query, after the backfill and after each
round of changes.

Usage: python3 e2e_test/streaming/merge_set_operation_filters.gen.py e2e_test/streaming/merge_set_operation_filters.slt
"""
import sys

P = 'sfs_'
VAR = 'streaming_merge_set_operation_filters'

TABLES = [('t', 'k int primary key, a int, b int, s varchar, d decimal'), ('u', 'k int primary key, a int, b int, s varchar, d decimal')]

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('union_rows', 'k, a, b, s, d', 'select * from {p}t where a > 1 union select * from {p}t where b < 5'),
    ('union_col', 'a', 'select a from {p}t where a > 1 union select a from {p}t where b < 5'),
    ('union3', 'a', 'select a from {p}t where a > 1 union select a from {p}u union select a from {p}t where b < 5'),
    ('union_all_rows', 'a, s', 'select a, s from {p}t where a > 1 union select a, s from {p}t'),
    ('intersect_key', 'k, a', 'select k, a from {p}t where a > 1 intersect select k, a from {p}t where b < 5'),
    ('intersect_out', 'a, b', 'select a, b from {p}t where a > 1 intersect select a, b from {p}t where b < 5 or b is null'),
    ('intersect_common', 'a', 'select a from {p}t where b > 0 and a > 1 intersect select a from {p}t where b > 0 and a < 5'),
    ('except_key', 'k, a', 'select k, a from {p}t where a > 1 except select k, a from {p}t where b < 5'),
    ('except_out', 'a, s', "select a, s from {p}t where a > 1 except select a, s from {p}t where s like 'x%'"),
    ('except_later', 'k, a', 'select k, a from {p}t except select k, a from {p}u where a < 0 except select k, a from {p}u where a > 10'),
    ('nulls', 'a, b', 'select a, b from {p}t where a is null intersect select a, b from {p}t where b is null'),
    # Left as they are by the rule.
    ('kept_intersect', 'a', 'select a from {p}t where a > 1 intersect select a from {p}t where b < 5'),
    ('kept_except', 'a', 'select a from {p}t where a > 1 except select a from {p}t where b < 5'),
    ('kept_numeric', 'd', "select d from {p}t where d > 1 intersect select d from {p}t where d::varchar = '2.0'"),
    ('kept_exprs', 'a', 'select a from {p}t where a > 1 intersect select b from {p}t where b < 5'),
    ('kept_tables', 'k, a', 'select k, a from {p}t where a > 1 intersect select k, a from {p}u where a < 5'),
]
CASES = [(name, cols, sql.format(p=P)) for name, cols, sql in CASES]

# Rounds of changes: (title, statements); the first one runs before the views are created.
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
# Generated by `python3 e2e_test/streaming/merge_set_operation_filters.gen.py <this file>`: edit the list of queries
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
