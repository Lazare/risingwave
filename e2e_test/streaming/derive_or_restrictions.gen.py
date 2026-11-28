"""Generates `e2e_test/streaming/derive_or_restrictions.slt`, the differential test of
`streaming_derive_or_restrictions` (rule `OrRestrictionDeriveRule`: an `OR` of the condition of a
join that refers to both sides gives each side the `OR` of its own parts): every query is
materialized with and without the variable, and both materialized views must hold the same rows,
with the same multiplicities, as the batch query, after the backfill and after each round of
changes.

Usage: python3 e2e_test/streaming/derive_or_restrictions.gen.py e2e_test/streaming/derive_or_restrictions.slt
"""
import sys

A, B, C, N = 'sdor_a', 'sdor_b', 'sdor_c', 'sdor_n'
VAR = 'streaming_derive_or_restrictions'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('inner', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k where (a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4)"),
    ('inner_is_null', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k where (a.x = 1 and b.y is null) or (a.x is null and b.y = 4)"),
    ('inner_one_side', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k where (a.x = 1 and b.y = 2) or b.y = 4"),
    ('inner_three', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k where (a.x in (1, 2) and b.y > 3) "
     f"or (a.x = 3 and b.z <= 0) or (a.s = 'p' and b.y is not distinct from b.z)"),
    ('inner_mixed', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k where (a.x = 1 and b.y = 2) "
     f"or (a.x = 3 and b.y = b.z) or (a.x = 2 and a.s = b.s and b.y = 1)"),
    ('inner_pk', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.k = b.k where (a.id = 1 and b.y = 2) or (a.id = 3 and b.y = 4)"),
    ('left', 'id int, bid int, y int',
     f"select a.id, b.id as bid, b.y from {A} a left join {B} b on a.k = b.k "
     f"and ((a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4))"),
    ('right', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a right join {B} b on a.k = b.k "
     f"and ((a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4))"),
    ('semi', 'id int',
     f"select a.id from {A} a where exists (select 1 from {B} b where b.k = a.k "
     f"and ((a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4)))"),
    ('agg', 'n bigint, s bigint',
     f"select count(*) as n, coalesce(sum(b.y), 0) as s from {A} a join {B} b on a.k = b.k "
     f"where (a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4)"),
    ('dup', 'v int, y int',
     f"select n.v, b.y from {N} n join {B} b on n.k = b.k where (n.v = 1 and b.y = 2) or (n.v = 2 and b.z = 3)"),
    ('unsafe_part', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k "
     f"where (lower(a.s) = 'p' and b.y = 2) or (lower(a.s) = 'q' and b.y = 4)"),
    ('three_tables', 'id int, bid int, cid int',
     f"select a.id, b.id as bid, c.id as cid from {A} a join {B} b on a.k = b.k join {C} c on c.k = b.k "
     f"where ((a.x = 1 and c.w = 2) or (a.x = 3 and c.w = 4)) and ((b.y = 2 and c.w = 2) or (b.y = 4 and c.w = 4))"),
    # Left as they are by the rule.
    ('kept_anti', 'id int',
     f"select a.id from {A} a where not exists (select 1 from {B} b where b.k = a.k "
     f"and ((a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4)))"),
    ('kept_left_preserved', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a left join {B} b on a.k = b.k "
     f"and ((a.x = 1 and b.y is null) or (a.x = 3 and b.z is null))"),
    ('kept_full', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a full join {B} b on a.k = b.k "
     f"and ((a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4))"),
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
    grouped = lambda t: f"select {cols}, count(*) as sdor_rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def compare_all(title):
    out.append(f"# {title}: with and without the rule, every materialized view holds the rows of "
               f"the batch query.\n")
    for name, cols, sql in CASES:
        c = columns(cols)
        same_rows(f"sdor_{name}_on", f"sdor_{name}_off", c)
        same_rows(f"sdor_{name}_on", f"({sql}) q", c)


def first_data():
    stmt(f"insert into {A} values (1, 1, 1, 'p'), (2, 1, 3, 'q'), (3, 2, 1, null), (4, 2, null, 'p'), "
         f"(5, null, 3, 'q'), (6, 3, 2, 'p'), (7, 3, 3, 'z'), (8, 4, 0, null)")
    stmt(f"insert into {B} values (1, 1, 2, 0, 'p'), (2, 1, 4, 4, 'q'), (3, 2, null, 3, 'p'), "
         f"(4, 2, 4, null, null), (5, null, 2, 2, 'q'), (6, 3, 1, 1, 'p'), (7, 3, 4, 3, 'z'), (8, 5, 2, 2, 'p')")
    stmt(f"insert into {C} values (1, 1, 2), (2, 2, 4), (3, 3, 2), (4, null, 4), (5, 1, 4)")
    stmt(f"insert into {N} values (1, 1), (1, 1), (2, 2), (null, 1), (3, 2), (2, null)")


def second_data():
    stmt(f"insert into {A} values (9, 5, 1, 'p'), (10, 1, null, null), (1, 2, 3, 'q')")
    stmt(f"insert into {B} values (9, 4, 4, 0, 'z'), (10, null, null, null, null), (1, 1, 4, 2, 'p')")
    stmt(f"insert into {N} values (1, 1), (3, 2)")


def third_data():
    stmt(f"update {A} set x = 3 where id in (3, 6)")
    stmt(f"update {A} set x = null where id = 7")
    stmt(f"update {A} set k = 3 where id = 4")
    stmt(f"update {B} set y = 2 where id in (2, 7)")
    stmt(f"update {B} set y = null where id = 6")
    stmt(f"update {C} set w = 2 where id = 2")
    stmt(f"delete from {B} where id in (3, 8)")
    stmt(f"delete from {A} where id = 2")
    stmt(f"delete from {N} where k = 1 and v = 1")


out.append("""# Differential test of `streaming_derive_or_restrictions`: every query is materialized twice,
# with and without the variable, and both materialized views must hold the rows of the batch query,
# with the same multiplicities, after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/derive_or_restrictions.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {A} (id int primary key, k int, x int, s varchar)")
stmt(f"create table {B} (id int primary key, k int, y int, z int, s varchar)")
stmt(f"create table {C} (id int primary key, k int, w int)")
stmt(f"create table {N} (k int, v int)")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
first_data()

for sfx in ('off', 'on'):
    stmt(f"set {VAR} = {'true' if sfx == 'on' else 'false'}")
    for name, _, sql in CASES:
        stmt(f"create materialized view sdor_{name}_{sfx} as {sql}")
stmt(f"set {VAR} = false")

compare_all("After the backfill")
query('I', "select count(*) from sdor_inner_on", ["3"])
out.append("# First round of changes: new rows, rows written again with an existing primary key.\n")
second_data()
compare_all("After the inserts")
out.append("# Second round of changes: compared columns changed, keys changed, deletes (retractions).\n")
third_data()
compare_all("After the updates and deletes")
out.append("# Third round of changes: the right side emptied and filled again, the left side emptied.\n")
stmt(f"delete from {B}")
compare_all("After the right side is emptied")
first_data_b = "insert into {B} values (1, 1, 2, 0, 'p'), (2, 1, 4, 4, 'q'), (3, 2, null, 3, 'p'), (6, 3, 1, 1, 'p')"
stmt(first_data_b.format(B=B))
compare_all("After the right side is filled again")
stmt(f"delete from {A}")
stmt(f"delete from {N}")
compare_all("After the left side is emptied")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view sdor_{name}_{sfx}")
for table in (N, C, B, A):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
