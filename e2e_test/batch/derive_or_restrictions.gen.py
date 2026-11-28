"""Generates `e2e_test/batch/derive_or_restrictions.slt`, the differential test of
`batch_derive_or_restrictions` (rule `OrRestrictionDeriveRule`: an `OR` of the condition of a join
that refers to both sides gives each side the `OR` of its own parts). Every query runs twice, with
and without the variable, through `INSERT ... SELECT` (the batch plan), and both results must hold
the same rows with the same multiplicities, after each round of changes of the data.

Usage: python3 e2e_test/batch/derive_or_restrictions.gen.py e2e_test/batch/derive_or_restrictions.slt
"""
import sys

A, B, C, N = 'dor_a', 'dor_b', 'dor_c', 'dor_n'
VAR = 'batch_derive_or_restrictions'

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
    # Restrictions written without redundancy (one `IN` list per column), or not derived when they
    # can not remove a row (a `CASE` or the constants of a union compared with all their values).
    ('merged_in', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k where (a.s = 'p' and b.y = 1) "
     f"or (a.s = 'q' and b.y = 2) or (a.s = 'p' and b.y = 3) or (a.s in ('p', 'z') and b.y = 4)"),
    ('same_value', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on a.k = b.k where (a.x = 1 and b.y = 2) or (a.x = 1 and b.y = 4)"),
    ('case_some_values', 'id int, bid int',
     f"select a.id, bb.id as bid from {A} a join (select id, k, case when y > 2 then 1 when y < 2 then 2 "
     f"else 0 end as f from {B}) bb on a.k = bb.k where (bb.f = 1 and a.x = 1) or (bb.f = 2 and a.x = 3)"),
    ('case_all_values', 'id int, bid int',
     f"select a.id, bb.id as bid from {A} a join (select id, k, case when y > 2 then 1 else 0 end as f "
     f"from {B}) bb on a.k = bb.k where (bb.f = 0 and a.x = 1) or (bb.f = 1 and a.x = 3)"),
    ('case_no_else', 'id int, bid int',
     f"select a.id, bb.id as bid from {A} a join (select id, k, case when y > 2 then 1 when y < 2 then 2 end as f "
     f"from {B}) bb on a.k = bb.k where (bb.f in (1, 2) and a.x = 1) or (bb.f = 1 and a.x = 3)"),
    ('union_tags', 'id int, uid int',
     f"select a.id, u.id as uid from {A} a join (select id, k, 'x' as tag from {B} union all "
     f"select id, k, 'y' as tag from {C}) u on a.k = u.k where (u.tag = 'x' and a.x = 1) or (u.tag = 'y' and a.x = 3)"),
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
    # No equality: nested loop in batch, not plannable in streaming.
    ('no_equality', 'id int, bid int',
     f"select a.id, b.id as bid from {A} a join {B} b on (a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4)"),
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
        stmt(f"delete from dor_{name}_off")
        stmt(f"delete from dor_{name}_on")
        stmt(f"insert into dor_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into dor_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"dor_{name}_on", f"dor_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as dor_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


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


out.append("""# Differential test of `batch_derive_or_restrictions`: every query runs through
# `INSERT ... SELECT` without, then with the variable, and both results must hold the same rows
# with the same multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/derive_or_restrictions.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {A} (id int primary key, k int, x int, s varchar)")
stmt(f"create table {B} (id int primary key, k int, y int, z int, s varchar)")
stmt(f"create table {C} (id int primary key, k int, w int)")
stmt(f"create table {N} (k int, v int)")
for name, cols, _ in CASES:
    stmt(f"create table dor_{name}_off ({cols})")
    stmt(f"create table dor_{name}_on ({cols})")

run_all("Empty tables")
out.append("# First data: NULLs in the keys and in the compared columns, duplicates, rows without a match.\n")
first_data()
run_all("First data")
query('I', f"select count(*) from dor_inner_on", ["3"])
out.append("# Second data: more rows, rows written again with an existing primary key.\n")
second_data()
run_all("Second data")
out.append("# Third data: compared columns changed, keys changed, deletes.\n")
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
    stmt(f"drop table dor_{name}_on")
    stmt(f"drop table dor_{name}_off")
for table in (N, C, B, A):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
