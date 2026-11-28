"""Generates `e2e_test/batch/simplify_exists_subquery.slt`, the differential test of
`batch_simplify_exists_subquery` (rule `ExistsSubquerySimplifyRule`: the limit, `DISTINCT` and
`GROUP BY` without aggregates that can not change the result of a correlated `EXISTS` / `IN`
subquery are removed before unnesting). Every query runs twice, with and without the variable,
through `INSERT ... SELECT` (the batch plan), and both results must hold the same rows with the
same multiplicities, after each round of changes of the data.

Usage: python3 e2e_test/batch/simplify_exists_subquery.gen.py e2e_test/batch/simplify_exists_subquery.slt
"""
import sys

T, U, N = 'se_t', 'se_u', 'se_n'
VAR = 'batch_simplify_exists_subquery'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('exists_topn', 'id int',
     f"select t.id from {T} t where exists (select 1 from {U} u where u.k = t.k order by u.ts desc limit 1)"),
    ('exists_limit', 'id int',
     f"select t.id from {T} t where exists (select u.v from {U} u where u.k = t.k limit 3)"),
    ('not_exists_limit', 'id int',
     f"select t.id from {T} t where not exists (select 1 from {U} u where u.k = t.k limit 1)"),
    ('exists_distinct', 'id int',
     f"select t.id from {T} t where exists (select distinct u.v from {U} u where u.k = t.k)"),
    ('exists_group_by', 'id int',
     f"select t.id from {T} t where exists (select u.v from {U} u where u.k = t.k group by u.v)"),
    ('in_distinct', 'id int',
     f"select t.id from {T} t where t.y in (select distinct u.v from {U} u where u.k = t.k)"),
    ('not_in_distinct', 'id int',
     f"select t.id from {T} t where t.y not in (select distinct u.v from {U} u where u.k = t.k)"),
    ('exists_distinct_limit', 'id int',
     f"select t.id from {T} t where exists (select distinct s.v from "
     f"(select u.v from {U} u where u.k = t.k limit 2) s)"),
    ('exists_ties', 'id int',
     f"select t.id from {T} t where exists (select 1 from {U} u where u.k = t.k order by u.ts "
     f"fetch first 1 rows with ties)"),
    ('exists_non_equi', 'id int',
     f"select t.id from {T} t where exists (select 1 from {U} u where u.k = t.k and u.v > t.y limit 1)"),
    ('duplicates_semi', 'k int, y int',
     f"select n.k, n.y from {N} n where exists (select 1 from {U} u where u.k = n.k limit 1)"),
    ('duplicates_anti', 'k int, y int',
     f"select n.k, n.y from {N} n where not exists (select 1 from {U} u where u.k = n.k order by u.ts limit 1)"),
    ('duplicates_in', 'k int, y int',
     f"select n.k, n.y from {N} n where n.y in (select u.v from {U} u where u.k = n.k group by u.v)"),
    ('count_semi', 'c bigint',
     f"select count(*) as c from {T} t where exists (select 1 from {U} u where u.k = t.k limit 1)"),
    # Left as they are by the rule.
    ('kept_offset', 'id int',
     f"select t.id from {T} t where exists (select 1 from {U} u where u.k = t.k order by u.ts, u.id "
     f"limit 1 offset 1)"),
    ('kept_having', 'id int',
     f"select t.id from {T} t where exists (select u.v from {U} u where u.k = t.k group by u.v "
     f"having count(*) > 1)"),
    ('kept_in_limit', 'id int',
     f"select t.id from {T} t where t.y in (select u.v from {U} u where u.k = t.k order by u.ts, u.id limit 1)"),
    ('kept_filter_over_limit', 'id int',
     f"select t.id from {T} t where exists (select 1 from (select u.v from {U} u where u.k = t.k "
     f"order by u.ts desc, u.id limit 1) s where s.v > 0)"),
    ('kept_set_returning', 'id int',
     f"select t.id from {T} t where exists (select unnest(array[u.v, u.ts]) from {U} u where u.k = t.k limit 1)"),
    ('kept_select_list', 'id int, e boolean',
     f"select t.id, exists (select 1 from {U} u where u.k = t.k limit 1) as e from {T} t"),
    ('kept_uncorrelated', 'id int',
     f"select t.id from {T} t where exists (select 1 from {U} u where u.v > 0 limit 1)"),
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
        stmt(f"delete from se_{name}_off")
        stmt(f"delete from se_{name}_on")
        stmt(f"insert into se_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into se_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"se_{name}_on", f"se_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as se_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append("""# Differential test of `batch_simplify_exists_subquery`: every query runs through
# `INSERT ... SELECT` without, then with the variable, and both results must hold the same rows
# with the same multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/simplify_exists_subquery.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (id int primary key, k int, x int not null, y int)")
stmt(f"create table {U} (id int primary key, k int, ts int, v int)")
stmt(f"create table {N} (k int, y int)")
for name, cols, _ in CASES:
    stmt(f"create table se_{name}_off ({cols})")
    stmt(f"create table se_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: several rows per key (ties on `ts`), NULL keys and values, keys without rows.\n")
stmt(f"insert into {T} values (1, 1, 1, 10), (2, 2, 2, 20), (3, null, 3, 10), (4, 4, 4, null), "
     f"(5, 1, 5, 11), (6, 9, 6, 0)")
stmt(f"insert into {U} values (1, 1, 5, 10), (2, 1, 5, 11), (3, 1, 7, null), (4, 2, 1, 20), "
     f"(5, null, 2, 10), (6, 4, 3, 1), (7, 4, 3, 1), (8, 2, 9, -1)")
stmt(f"insert into {N} values (1, 10), (1, 10), (null, 10), (null, null), (2, 21), (4, 1), (4, 1), (7, 0)")
run_all("First data")

query('I', f"select count(*) from se_exists_topn_on", ["4"])
query('I', f"select count(*) from se_duplicates_anti_on", ["3"])

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {T} values (7, 7, 7, 70), (8, null, 8, null), (1, 1, 1, 11)")
stmt(f"insert into {U} values (9, 7, 1, 70), (10, 7, 1, 71), (11, 9, 0, 0), (1, 1, 5, 12)")
stmt(f"insert into {N} values (9, 0), (null, 5), (7, 70)")
run_all("Second data")

out.append("# Third data: keys and values set to NULL and back, deletes.\n")
stmt(f"update {U} set k = null where id in (4, 8)")
stmt(f"update {U} set k = 2 where id = 5")
stmt(f"update {U} set v = null where id in (6, 9)")
stmt(f"update {T} set k = 4 where id = 3")
stmt(f"update {T} set y = null where id = 2")
stmt(f"delete from {U} where id in (2, 3)")
stmt(f"delete from {T} where id = 6")
stmt(f"delete from {N} where k is null and y = 10")
run_all("Third data")

out.append("# Fourth data: only NULL keys left on the subquery side, then no row.\n")
stmt(f"update {U} set k = null")
run_all("Only NULL keys in the subquery")
stmt(f"delete from {U}")
run_all("Empty subquery table")

out.append("# Fifth data: the subquery side filled again, the outer side emptied.\n")
stmt(f"insert into {U} values (1, 1, 1, 10), (2, null, 1, 20), (3, 4, 2, null)")
stmt(f"delete from {T}")
stmt(f"delete from {N}")
run_all("Empty outer side")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table se_{name}_on")
    stmt(f"drop table se_{name}_off")
for table in (N, U, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
