"""Generates `e2e_test/streaming/simplify_subquery_unnesting.slt`, the differential test of the
unnesting of correlated subqueries without a domain in streaming plans: every query is
materialized with and without `streaming_simplify_subquery_unnesting`.

Usage: python3 e2e_test/streaming/simplify_subquery_unnesting.gen.py e2e_test/streaming/simplify_subquery_unnesting.slt
"""
import sys

P, R, Q, U = 'ssu_p', 'ssu_r', 'ssu_q', 'ssu_u'
PV = 'ssu_pv'  # view: the outer rows with an EXISTS column

CASES = [
    # Rewritten.
    ('exists_select', 'EXISTS in the select list',
     f"select p.id, exists (select 1 from {R} r where r.pid = p.id and r.s = 'x') as e from {P} p"),
    ('not_exists_select', 'NOT EXISTS in the select list',
     f"select p.id, not exists (select 1 from {R} r where r.pid = p.id) as e from {P} p"),
    ('exists_or', 'EXISTS under OR in WHERE',
     f"select p.id, p.v from {P} p where p.v > 2 or exists (select 1 from {R} r where r.pid = p.id)"),
    ('exists_case', 'EXISTS in a CASE, correlated on a nullable column that is not a key',
     f"select p.id, case when exists (select 1 from {R} r where r.pid = p.v) then 'y' else 'n' end as c from {P} p"),
    ('two_not_exists', 'two NOT EXISTS joined by AND',
     f"select p.id, not exists (select 1 from {R} r where r.s = 'x' and r.pid = p.id) "
     f"and not exists (select 1 from {Q} q where q.pid = p.id) as flag from {P} p"),
    ('view_filter', 'filter on the EXISTS column of a view',
     f"select id, v from {PV} where e"),
    ('view_not', 'filter on the negated EXISTS column of a view',
     f"select id, v from {PV} where not e"),
    ('nokey', 'outer side without a key, with duplicates and NULLs',
     f"select u.k, u.v, exists (select 1 from {R} r where r.pid = u.k) as e from {U} u"),
    ('min', 'scalar min',
     f"select p.id, (select min(r.x) from {R} r where r.pid = p.id) as m from {P} p"),
    ('count', 'scalar count',
     f"select p.id, (select count(*) from {R} r where r.pid = p.id) as c from {P} p"),
    ('sum_count', 'sum, count of a column and count of distinct values in two subqueries',
     f"select p.id, coalesce((select sum(r.x) from {R} r where r.pid = p.id), 0) as s, "
     f"(select count(r.x) + count(distinct r.s) from {R} r where r.pid = p.id) as c from {P} p"),
    ('avg_bool', 'avg and bool_or',
     f"select p.id, (select avg(r.x) from {R} r where r.pid = p.id) as a, "
     f"(select bool_or(r.s = 'x') from {R} r where r.pid = p.id) as b from {P} p"),
    ('array', 'ARRAY subquery, an empty array when nothing matches',
     f"select p.id, array(select r.x from {R} r where r.pid = p.id order by r.x) as xs from {P} p"),
    ('string_agg', 'string_agg with a default when nothing matches',
     f"select p.id, coalesce((select string_agg(r.s, ',' order by r.s) from {R} r where r.pid = p.id), '-') as ss from {P} p"),
    ('where_count', 'count compared in WHERE',
     f"select p.id from {P} p where p.g = (select count(*) from {R} r where r.pid = p.id)"),
    ('two_cols', 'correlated on two columns',
     f"select p.id, (select count(*) from {R} r where r.pid = p.id and r.x = p.v) as c from {P} p"),
    ('filter_clause', 'aggregate with a filter clause',
     f"select p.id, (select count(*) filter (where r.x > 1) from {R} r where r.pid = p.id) as c from {P} p"),
    ('count_expr', 'expression of a count that is not NULL over an empty input',
     f"select p.id, (select count(*) * 10 + 1 from {R} r where r.pid = p.id) as c from {P} p"),
    # Kept in the general way.
    ('kept_corr_arg', 'correlated column in the aggregated expression',
     f"select p.id, (select max(r.x + p.v) from {R} r where r.pid = p.id) as m from {P} p"),
    ('kept_not_distinct', 'IS NOT DISTINCT FROM written in the subquery',
     f"select p.id, p.v, (select count(*) from {R} r where r.pid is not distinct from p.v) as c from {P} p"),
]

# A streaming `sum` returns 0, where the query returns NULL, once all the non-NULL values of its
# group are retracted while the group still holds rows (with NULL values, or the padded row of
# the domain in the general way): the sum of `sum_count` is wrapped in `coalesce(.., 0)`, and
# `avg_bool` is compared with the query only.
EXACT = {'avg_bool'}

out = []
def switch(on):
    stmt(f"set streaming_simplify_subquery_unnesting = {'true' if on else 'false'}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"ssu_{name}_on", f"ssu_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_batch(name, sql):
    a = f"ssu_{name}_on"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from ({sql}) q) x),\n"
          f"  (select count(*) from (select * from ({sql}) q except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from ({sql}) q)",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the variable, every materialized view holds the same rows.\n")
    for name, _, _ in CASES:
        if name not in EXACT:
            compare(name)
    out.append(f"# {title}: with the variable, every materialized view holds the rows of its query.\n")
    for name, _, sql in CASES:
        compare_batch(name, sql)

out.append("""# Differential test of `streaming_simplify_subquery_unnesting`: every query is materialized twice,
# with and without the unnesting of its correlated subqueries without a domain, and both
# materialized views must hold the same rows, and the rows of the query, after the backfill and
# after each round of changes.
#
# Generated by `python3 e2e_test/streaming/simplify_subquery_unnesting.gen.py <this file>`: edit
# the list of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {P} (id int primary key, v int, g int)")
stmt(f"create table {R} (id int primary key, pid int, s varchar, x int)")
stmt(f"create table {Q} (id int primary key, pid int)")
stmt(f"create table {U} (k int, v int)")
stmt(f"create view {PV} as select p.id, p.v, exists (select 1 from {R} r where r.pid = p.id) as e from {P} p")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {P} values (1, 1, 2), (2, 2, 0), (3, null, 1), (4, 1, null), (5, 5, 0), (6, 3, 3)")
stmt(f"insert into {R} values (10, 1, 'x', 1), (11, 1, 'y', 2), (12, 1, 'x', null), (13, 2, null, 2), "
     f"(14, 3, 'x', 5), (15, null, 'x', 1), (16, 9, 'z', 9), (17, 3, 'z', null)")
stmt(f"insert into {Q} values (20, 1), (21, 4), (22, null), (23, 4)")
stmt(f"insert into {U} values (1, 1), (1, 1), (2, null), (null, 3), (7, 0), (null, null)")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view ssu_{name}_{sfx} as {sql}")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('IT', "select id, e from ssu_exists_select_on order by id",
      ["1 t", "2 f", "3 t", "4 f", "5 f", "6 f"])
query('II', "select id, c from ssu_count_on order by id",
      ["1 3", "2 1", "3 2", "4 0", "5 0", "6 0"])
query('IT', "select id, xs from ssu_array_on order by id",
      ["1 {1,2,NULL}", "2 {2}", "3 {5,NULL}", "4 {}", "5 {}", "6 {}"])

out.append("# First round of changes: new rows on every side, including matches for outer rows that had none.\n")
stmt(f"insert into {R} values (30, 4, 'x', 3), (31, 5, 'y', 0), (32, 5, 'y', 0), (33, 7, 'x', 1), (34, null, null, null)")
stmt(f"insert into {P} values (7, 7, 1), (8, null, null), (9, 4, 1)")
stmt(f"insert into {Q} values (24, 7)")
stmt(f"insert into {U} values (4, 4), (3, 3), (3, 3)")
out.append("# A row written again with an existing primary key replaces the previous one.\n")
stmt(f"insert into {R} values (13, 2, 'x', 4)")
stmt(f"insert into {P} values (2, null, 1)")
compare_all("After the inserts")

out.append("# Second round of changes: the last matching rows of some outer rows are deleted or moved away.\n")
stmt(f"delete from {R} where id in (14, 17)")
stmt(f"update {R} set pid = 6 where id = 13")
stmt(f"update {R} set pid = null where id = 30")
stmt(f"update {R} set x = null, s = null where id in (10, 11)")
stmt(f"update {R} set s = 'x' where id = 31")
stmt(f"update {P} set v = 1, g = 3 where id = 3")
stmt(f"update {P} set v = null where id = 4")
stmt(f"delete from {P} where id = 5")
stmt(f"delete from {Q} where id in (21, 23)")
stmt(f"delete from {U} where k = 1")
stmt(f"update {U} set k = 5 where k is null and v = 3")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the inner table is emptied and filled again.\n")
stmt(f"delete from {R}")
compare_all("After the inner table is emptied")
stmt(f"insert into {R} values (40, 1, 'x', 1), (41, 3, 'y', 2), (42, 3, 'x', 3), (43, 6, null, null), (44, null, 'x', 0)")
compare_all("After the inner table is filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view ssu_{name}_{sfx}")
stmt(f"drop view {PV}")
for table in (U, Q, R, P):
    stmt(f"drop table {table}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
