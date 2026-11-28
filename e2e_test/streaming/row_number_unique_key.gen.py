"""Generates `e2e_test/streaming/row_number_unique_key.slt`, the differential test of the unique
key that a filter `row_number() OVER (PARTITION BY k ...) = n` (or `<= 1`, `< 2`) gives to its
output (the partition columns `k`; without `PARTITION BY`, the empty key: at most one row), used
by `streaming_simplify_on_unique_key` (rules `AggOnKeyEliminateRule`,
`UnusedOuterJoinEliminateRule`). The queries number the rows with a
second window function on the same window, so that the window is not turned into a top-n.

Every query is materialized with and without the variable, and both materialized views must hold
the same rows, with the same multiplicities, as the batch query, after the backfill and after each
round of changes. The rounds change which row of a partition comes first.

Usage: python3 e2e_test/streaming/row_number_unique_key.gen.py e2e_test/streaming/row_number_unique_key.slt
"""
import sys

P = 'rnks_'
VAR = 'streaming_simplify_on_unique_key'

TABLES = [('t', 'id int primary key, k int, k2 int, v int, w int'), ('u', 'id int primary key, k int, x int')]

# The rows of `t` numbered per partition `k`, with a second window function on the same window,
# a top-1 per (`k`, `k2`).
RN = ("(select id, k, k2, v, w, row_number() over (partition by k order by v, id) as rn, "
      "count(w) over (partition by k order by v, id) as sw from {p}t)")
RN2 = "(select id, k, k2, v, row_number() over (partition by k, k2 order by v desc, id) as rn from {p}t)"
# The rows of `t` numbered without partition, with a second window function on the same window.
RN0 = ("(select id, k, k2, v, w, row_number() over (order by v, id) as rn, "
       "count(w) over (order by v, id) as sw from {p}t)")

# (name, columns of the result with their types, query)
CASES = [
    # The group key, the distinct columns or the join columns hold the partition columns.
    ('having', 'k int', f'select k from {RN} s where rn = 1 group by k having count(*) > 1'),
    ('count', 'k int, n bigint, mv int, sw bigint',
     f'select k, count(*) as n, max(v) as mv, max(sw) as sw from {RN} s where rn = 1 group by k'),
    ('distinct', 'k int, v int, w int', f'select distinct k, v, w from {RN} s where rn = 1'),
    ('le_one', 'k int, n bigint', f'select k, count(*) as n from {RN} s where rn <= 1 group by k'),
    ('lt_two', 'k int, n bigint', f'select k, count(*) as n from {RN} s where 2 > rn group by k'),
    ('one_eq', 'k int, v int', f'select distinct k, v from {RN} s where 1 = rn'),
    ('more_conjuncts', 'k int, n bigint', f'select k, count(*) as n from {RN} s where rn = 1 and sw > 0 and v is not null group by k'),
    ('two_columns', 'k int, k2 int, n bigint, mv int', f'select k, k2, count(*) as n, max(v) as mv from {RN2} s where rn = 1 group by k, k2'),
    ('left_join', 'id int, x int', f'select u.id, u.x from {{p}}u u left join (select k, v from {RN} s where rn = 1) s on u.k = s.k'),
    ('group_superset', 'k int, v int, n bigint', f'select k, v, count(*) as n from {RN} s where rn = 1 group by k, v'),
    # Another row number than 1: still at most one row per partition.
    ('second_row', 'k int, n bigint', f'select k, count(*) as n from {RN} s where rn = 2 group by k'),
    ('second_join', 'id int, x int', f'select u.id, u.x from {{p}}u u left join (select k, v from {RN} s where rn = 2) s on u.k = s.k'),
    ('third_used_join', 'id int, v int', f'select u.id, s.v from {{p}}u u left join (select k, v from {RN} s where rn = 3) s on u.k = s.k'),
    # Without partition: at most one row in total.
    ('no_partition', 'k int, n bigint, sw bigint',
     f'select k, count(*) as n, max(sw) as sw from {RN0} s where rn = 1 group by k'),
    ('no_partition_second', 'k int, v int, w int', f'select distinct k, v, w from {RN0} s where rn = 2'),
    ('no_partition_join', 'id int, x int', f'select u.id, u.x from {{p}}u u left join (select k, v from {RN0} s where rn <= 1) s on u.k = s.k'),
    # Left as they are: ties, several rows per partition or in total, a filter on another column,
    # an aggregation without group key.
    ('rank', 'k int, n bigint',
     'select k, count(*) as n from (select k, v, rank() over (partition by k order by v) as rk from {p}t) s '
     'where rk = 1 group by k'),
    ('other_filter', 'k int, n bigint', f'select k, count(*) as n from {RN} s where v = 1 group by k'),
    ('first_rows', 'k int, n bigint', f'select k, count(*) as n from {RN} s where rn <= 2 group by k'),
    ('other_group', 'v int, n bigint', f'select v, count(*) as n from {RN} s where rn = 1 group by v'),
    ('used_join', 'id int, v int', f'select u.id, s.v from {{p}}u u left join (select k, v from {RN} s where rn <= 2) s on u.k = s.k'),
    ('kept_join', 'id int, x int', f'select u.id, u.x from {{p}}u u left join (select k, v from {RN} s where rn <= 2) s on u.k = s.k'),
    ('no_partition_two', 'k int, n bigint', f'select k, count(*) as n from {RN0} s where rn <= 2 group by k'),
    ('no_partition_scalar', 'n bigint, sw bigint', f'select count(*) as n, max(sw) as sw from {RN0} s where rn = 1'),
    ('later_rows', 'k int, n bigint', f'select k, count(*) as n from {RN} s where rn > 1 group by k'),
]
CASES = [(name, cols, sql.format(p=P)) for name, cols, sql in CASES]

# Rounds of changes: (title, statements).
ROUNDS = [
    ('First data: several rows per partition, ties on v, NULL partitions and NULL values', [
        'insert into {p}t values (1, 1, 1, 5, 10), (2, 1, 1, 3, 20), (3, 1, 2, 3, -5), (4, 2, 1, 7, 1), '
        '(5, null, 1, 1, 2), (6, null, null, 1, 3), (7, 3, 1, null, 4), (8, 3, 1, 2, null), (9, 4, 2, 1, 1)',
        'insert into {p}u values (1, 1, 100), (2, 2, 200), (3, null, 300), (4, 9, 400), (5, 1, 500)',
    ]),
    ('Second data: a new first row in a partition, a row moved to another partition', [
        'insert into {p}t values (10, 1, 2, 0, 7), (11, 2, 1, 7, 8)',
        'update {p}t set k = 2 where id = 9',
        'insert into {p}u values (6, 3, 600)',
    ]),
    ('Third data: the first rows changed, deleted, set to NULL', [
        'update {p}t set v = 9 where id = 10',
        'delete from {p}t where id = 2',
        'update {p}t set v = null where id = 4',
        'update {p}t set k = null where id = 8',
        'delete from {p}u where id = 2',
    ]),
    ('Fourth data: emptied', [
        'delete from {p}t',
        'delete from {p}u',
    ]),
    ('Fifth data: filled again', [
        'insert into {p}t values (1, 1, 1, 1, 1), (2, 1, 1, 1, 1), (3, 2, 2, null, null)',
        'insert into {p}u values (1, 1, 1), (2, 2, 2)',
    ]),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    grouped = lambda t: f"select {cols}, count(*) as {P}rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def compare_all(title):
    out.append(f"# {title}: with and without the variable, every materialized view holds the rows "
               f"of the batch query.\n")
    for name, cols, sql in CASES:
        c = columns(cols)
        same_rows(f"{P}{name}_on", f"{P}{name}_off", c)
        same_rows(f"{P}{name}_on", f"({sql}) q", c)


out.append(f"""# Differential test of the unique key that `row_number() = n` gives to the partition columns,
# under `{VAR}`: every query is materialized twice, with and without the variable, and both
# materialized views must hold the rows of the batch query, with the same multiplicities, after
# the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/row_number_unique_key.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
for name, columns_ in TABLES:
    stmt(f"create table {P}{name} ({columns_})")
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

out.append("# Spot checks: no partition holds two first rows; the counts per key.\n")
query('I', f"select count(*) from {P}having_on", ["0"])
query('II', f"select k, n from {P}count_on order by k", ["1 1", "2 1"])

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view {P}{name}_{sfx}")
for name, _ in reversed(TABLES):
    stmt(f"drop table {P}{name}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
