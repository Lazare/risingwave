"""Generates `e2e_test/batch/null_safe_eq_to_eq.slt`, the differential test of
`batch_null_safe_eq_to_eq` (rule `NullSafeEqToEqRule`: `a IS NOT DISTINCT FROM b` becomes `a = b`
in a join condition, a filter or a scan predicate when `a` or `b` is never NULL). Every query runs
twice, with and without the variable, through `INSERT ... SELECT` (the batch plan), and both
results must hold the same rows with the same multiplicities, after each round of changes of the
data.

Usage: python3 e2e_test/batch/null_safe_eq_to_eq.gen.py e2e_test/batch/null_safe_eq_to_eq.slt
"""
import sys

A, B, N = 'ne_a', 'ne_b', 'ne_n'
VAR = 'batch_null_safe_eq_to_eq'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule. A primary key column is not proven non-null: RisingWave does not
    # declare it NOT NULL.
    ('join_not_null', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.x is not distinct from b.k"),
    ('filter_constant', 'id int, k int, y int',
     f"select id, k, y from {B} where k is not distinct from 5"),
    ('join_is_not_null', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.y is not distinct from b.y where b.y is not null"),
    ('other_conjunct', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.y = b.id and a.y is not distinct from b.k"),
    ('left_join', 'id int, bid int, y int',
     f"select a.id, b.id as bid, b.y from {A} a left join {B} b on a.x is not distinct from b.k"),
    ('full_join', 'id int, bid int, y int',
     f"select a.id, b.id as bid, b.y from {A} a full join {B} b on a.x is not distinct from b.k"),
    ('filter_over_left', 'id int, y int',
     f"select a.id, b.y from {A} a left join {B} b on a.id = b.id where a.x is not distinct from b.k"),
    ('exists_domain', 'id int',
     f"select a.id from {A} a where exists (select 1 from {B} b where b.k = a.id limit 1)"),
    ('not_exists_domain', 'id int',
     f"select a.id from {A} a where not exists (select 1 from {B} b where b.k = a.id limit 1)"),
    ('scalar_domain', 'id int, m int',
     f"select a.id, (select max(b.y) from {B} b where b.k = a.x) as m from {A} a"),
    ('duplicates_left', 'v int, id int',
     f"select n.v, a.id from {N} n left join {A} a on a.id is not distinct from n.k"),
    ('duplicates_semi', 'k int, v int',
     f"select n.k, n.v from {N} n where exists (select 1 from {A} a where a.x = n.k limit 1)"),
    ('duplicates_anti', 'k int, v int',
     f"select n.k, n.v from {N} n where not exists (select 1 from {A} a where a.x = n.k limit 1)"),
    # Left as they are by the rule.
    ('kept_join_pk', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.id is not distinct from b.k"),
    ('kept_left_pk', 'id int, y int',
     f"select a.id, b.y from {A} a left join {B} b on a.id is not distinct from b.k"),
    ('kept_nullable', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.y is not distinct from b.y"),
    ('kept_or', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.id = b.id and ((a.x is not distinct from b.k) or b.y > 0)"),
    ('kept_not', 'id int, y int',
     f"select a.id, b.y from {A} a join {B} b on a.id = b.id and not (a.x is not distinct from b.k)"),
    ('kept_project', 'id int, same boolean',
     f"select a.id, a.x is not distinct from b.k as same from {A} a join {B} b on a.id = b.id"),
    ('kept_filter_over_left', 'id int, y int',
     f"select a.id, b.y from {A} a left join {B} b on a.id = b.id where a.y is not distinct from b.k"),
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
        stmt(f"delete from ne_{name}_off")
        stmt(f"delete from ne_{name}_on")
        stmt(f"insert into ne_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into ne_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"ne_{name}_on", f"ne_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as ne_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append("""# Differential test of `batch_null_safe_eq_to_eq`: every query runs through `INSERT ... SELECT`
# without, then with the variable, and both results must hold the same rows with the same
# multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/null_safe_eq_to_eq.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {A} (id int primary key, k int, x int not null, y int)")
stmt(f"create table {B} (id int primary key, k int, y int, kb bigint)")
stmt(f"create table {N} (k int, v int)")
for name, cols, _ in CASES:
    stmt(f"create table ne_{name}_off ({cols})")
    stmt(f"create table ne_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULLs in the compared columns, duplicates, rows without a match.\n")
stmt(f"insert into {A} values (1, 1, 1, 1), (2, null, 2, null), (3, 3, 5, 3), (4, null, 4, null), "
     f"(5, 5, 5, 7), (6, 0, 0, null)")
stmt(f"insert into {B} values (1, 1, 1, 1), (2, null, null, null), (3, 5, 3, 5), (4, null, 7, 4), "
     f"(5, 2, null, 2), (6, 5, 5, null), (7, 9, 0, 9), (8, null, null, 8)")
stmt(f"insert into {N} values (1, 1), (1, 1), (null, 2), (null, 2), (5, 3), (null, null), (9, 4), (0, 5)")
run_all("First data")

query('I', f"select count(*) from ne_full_join_on where id is null", ["4"])
query('I', f"select count(*) from ne_duplicates_anti_on where k is null", ["3"])

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {A} values (7, 2, 9, 2), (8, null, 8, 5), (1, null, 1, null)")
stmt(f"insert into {B} values (9, 7, 2, 7), (10, null, 5, null), (1, null, 1, 1)")
stmt(f"insert into {N} values (null, 6), (2, 7), (5, 3)")
run_all("Second data")

out.append("# Third data: values set to NULL and back, deletes.\n")
stmt(f"update {A} set y = null where id in (1, 3)")
stmt(f"update {A} set y = 4, k = 4 where id in (2, 4)")
stmt(f"update {B} set k = null where id in (3, 7)")
stmt(f"update {B} set k = 4, y = 4 where id in (2, 8)")
stmt(f"delete from {B} where id in (5, 6)")
stmt(f"delete from {A} where id = 5")
stmt(f"delete from {N} where k is null and v = 2")
run_all("Third data")

out.append("# Fourth data: only NULLs left in the compared columns of the right side, then no row.\n")
stmt(f"update {B} set k = null, y = null")
run_all("Only NULLs on the right side")
stmt(f"delete from {B}")
run_all("Empty right side")

out.append("# Fifth data: the right side filled again, the left side emptied.\n")
stmt(f"insert into {B} values (1, 1, 1, 1), (2, null, null, null), (3, 0, 0, 0)")
stmt(f"delete from {A}")
run_all("Empty left side")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table ne_{name}_on")
    stmt(f"drop table ne_{name}_off")
for table in (N, B, A):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
