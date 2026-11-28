"""Generates `e2e_test/streaming/prune_null_union_branches.slt`, the differential test of
`streaming_prune_null_union_branches`
(rule `AggUnionNullBranchRule`: the branches of a `UNION ALL` that only feed `NULL` to the aggregation above are removed).

Every query is materialized with and without the variable, and both materialized views must hold
the same rows, with the same multiplicities, as the batch query, after the backfill and after each
round of changes.

Usage: python3 e2e_test/streaming/prune_null_union_branches.gen.py e2e_test/streaming/prune_null_union_branches.slt
"""
import sys

P = 'nbs_'
VAR = 'streaming_prune_null_union_branches'

TABLES = [('a', 'k int primary key, x int, y int'), ('b', 'k int primary key, x int, y int'), ('c', 'k int primary key, x int')]

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('no_key', 's, n, mx', 'select coalesce(sum(m), 0) as s, count(m) as n, max(m) as mx from (select k, x as m from {p}a union all select k, null::int from {p}b) v'),
    ('two_null', 's1, m2', 'select coalesce(sum(m1), 0) as s1, min(m2) as m2 from (select x as m1, y as m2 from {p}a union all select null::int, null::int from {p}b union all select null::int, null::int from {p}c) v'),
    ('all_null', 's, n', 'select coalesce(sum(m), 0) as s, count(m) as n from (select null::int as m from {p}a union all select null::int from {p}b) v'),
    ('filter_null', 's', 'select coalesce(sum(m), 0) as s from (select x as m from {p}a union all select y from {p}b where y is null) v'),
    ('group_by', 'k, s', 'select k, coalesce(sum(m), 0) as s from (select k, sum(x) as m from {p}a group by k union all select k, null::bigint from {p}b group by k) v group by k'),
    ('group_distinct', 'k, mx, n', 'select k, max(m) as mx, count(m) as n from (select k, x as m from {p}a union all select distinct k, null::int from {p}b) v group by k'),
    ('group_distinct_y', 'y, mx', 'select y, max(m) as mx from (select y, x as m from {p}a union all select distinct y, null::int from {p}b) v group by y'),
    # Left as they are by the rule.
    ('kept_count_star', 's, n', 'select coalesce(sum(m), 0) as s, count(*) as n from (select k, x as m from {p}a union all select k, null::int from {p}b) v'),
    ('kept_per_call', 's1, s2', 'select coalesce(sum(m1), 0) as s1, coalesce(sum(m2), 0) as s2 from (select x as m1, null::int as m2 from {p}a union all select null::int, y from {p}b) v'),
    ('kept_array_agg', 'l', 'select array_agg(m order by m) as l from (select x as m from {p}a union all select null::int from {p}b) v'),
    ('kept_group', 'k, s', 'select k, coalesce(sum(m), 0) as s from (select k, x as m from {p}a union all select k, null::int from {p}b) v group by k'),
]
CASES = [(name, cols, sql.format(p=P)) for name, cols, sql in CASES]

# Rounds of changes: (title, statements); the first one runs before the views are created.
ROUNDS = [
    ('First data: NULLs, keys in one table only', [
        'insert into {p}a values (1, 1, 1), (2, 2, null), (3, null, 3), (4, 4, 1), (5, null, null)',
        'insert into {p}b values (1, 10, 1), (3, 30, null), (6, null, 6), (7, 70, null)',
        'insert into {p}c values (1, 1), (8, 8)',
    ]),
    ('Second data: more rows, rows written again with an existing primary key', [
        'insert into {p}a values (6, 6, 6), (1, 11, 2)',
        'insert into {p}b values (8, 80, 8), (1, 15, null)',
    ]),
    ('Third data: updates to and from NULL, deletes', [
        'update {p}a set x = null where k in (2, 4)',
        'update {p}a set x = 3 where k = 3',
        'update {p}b set y = null where k = 6',
        'delete from {p}a where k = 1',
        'delete from {p}b where k = 7',
    ]),
    ('Fourth data: the first table emptied', [
        'delete from {p}a',
    ]),
    ('Fifth data: the first table filled again, the second one emptied', [
        'insert into {p}a values (1, 1, 1), (2, null, 2)',
        'delete from {p}b',
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
# Generated by `python3 e2e_test/streaming/prune_null_union_branches.gen.py <this file>`: edit the list of queries
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
