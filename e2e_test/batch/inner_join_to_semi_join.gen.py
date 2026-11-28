"""Generates `e2e_test/batch/inner_join_to_semi_join.slt`, the differential test of
`batch_inner_join_to_semi_join` (rules `InnerJoinToSemiJoinRule` and `SemiJoinDistinctRemoveRule`:
an inner join with a unique side that is not read becomes a semi join, and the `DISTINCT` of the
side that a semi or anti join tests is dropped when the estimates allow it). Every query runs
twice, with and without the variable, through `INSERT ... SELECT` (the batch plan), and both
results must hold the same rows with the same multiplicities, after each round of changes of the
data. A pause after each change lets the storage statistics reach the frontend, so that the
estimates exist.

Usage: python3 e2e_test/batch/inner_join_to_semi_join.gen.py e2e_test/batch/inner_join_to_semi_join.slt
"""
import sys

T, U, D = 'ijs_t', 'ijs_u', 'ijs_d'
P = 'ijs'
VAR = 'batch_inner_join_to_semi_join'
SETTINGS = ['batch_cost_based_join_ordering']

# (name, columns of the result, query)
CASES = [
    ('distinct_right', 'id int, k int, v int',
     f"select t.* from {T} t join (select distinct k from {U}) x on t.k = x.k"),
    ('distinct_left', 'id int, v int',
     f"select t.id, t.v from (select distinct k from {U}) x join {T} t on t.k = x.k"),
    ('table_key', 'id int, k int, v int',
     f"select t.* from {T} t join {D} d on t.k = d.id"),
    ('count', 'n bigint',
     f"select count(*) from {T} t join (select distinct k from {U}) x on t.k = x.k"),
    ('indf', 'id int, k int, v int',
     f"select t.* from {T} t join (select distinct k from {U}) x on t.k is not distinct from x.k"),
    ('extra_conjunct', 'id int, k int, v int',
     f"select t.* from {T} t join (select distinct k from {U}) x on t.k = x.k and x.k > t.v"),
    ('group_by_count', 'id int, v int',
     f"select t.id, t.v from {T} t join (select k, count(*) as c from {U} group by k) x on t.k = x.k"),
    ('in_distinct', 'id int, k int, v int',
     f"select * from {T} t where t.k in (select distinct k from {U})"),
    ('in_distinct_key', 'id int, k int, v int',
     f"select * from {T} t where t.k in (select distinct id from {U})"),
    ('anti_distinct', 'id int, k int, v int',
     f"select * from {T} t where not exists (select 1 from (select distinct k from {U}) x where x.k = t.k)"),
    ('anti_distinct_key', 'id int, k int, v int',
     f"select * from {T} t where not exists (select 1 from (select distinct id from {U}) x where x.id = t.k)"),
    ('inner_distinct_key', 'v int',
     f"select t.v from {T} t join (select distinct id from {U}) x on t.k = x.id"),
    ('filtered_distinct', 'id int, w int',
     f"select u.id, u.w from {U} u join (select distinct k from {T} where v > 1) x on u.k = x.k"),
    # Left as they are by the rule.
    ('kept_read', 'id int, k int, v int, xk int',
     f"select t.*, x.k from {T} t join (select distinct k from {U}) x on t.k + 1 = x.k"),
    ('kept_not_unique', 'id int, k int, v int',
     f"select t.* from {T} t join (select distinct k, w from {U}) x on t.k = x.k"),
    ('kept_non_key', 'id int, k int, v int',
     f"select t.* from {T} t join {U} u on t.k = u.k"),
    ('kept_no_eq', 'id int, k int, v int',
     f"select t.* from {T} t join (select max(k) as m from {U}) x on t.k > x.m"),
    ('kept_left', 'id int, k int, v int',
     f"select t.* from {T} t left join (select distinct k from {U}) x on t.k = x.k"),
    ('kept_scalar_agg', 'id int, k int, v int',
     f"select * from {T} t where exists (select 1 from (select count(*) as c from {U}) x where x.c = t.k)"),
    # Constant or one-row sides: left as inner joins.
    ('kept_const_values', 'id int, v int',
     f"select s.id, s.v from (select id, v, 1 as one from {T} union all select id, w, 1 as one from {U}) s "
     f"join (values (1)) as c(x) on s.one = c.x and s.v = any(array[1, 2, 3])"),
    ('kept_const_key', 'id int, v int',
     f"select s.id, s.v from (select id, v, 2 as two from {T}) s join {D} d on s.two = d.id"),
    ('kept_scalar_subquery', 'id int, k int, v int',
     f"select t.* from {T} t where t.v = (select max(w) from {U})"),
    ('kept_one_row_values', 'id int, k int, v int',
     f"select t.* from {T} t join (values (1)) as c(x) on t.k = c.x"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    out.append("sleep 5s\n")
    out.append(f"# {title}: every query runs without, then with the variable.\n")
    for name, _, sql in CASES:
        stmt(f"delete from {P}_{name}_off")
        stmt(f"delete from {P}_{name}_on")
        stmt(f"insert into {P}_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into {P}_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"{P}_{name}_on", f"{P}_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as {P}_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append(f"""# Differential test of `{VAR}`: every query runs through `INSERT ... SELECT` without, then
# with the variable, and both results must hold the same rows with the same multiplicities, after
# each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/inner_join_to_semi_join.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
for setting in SETTINGS:
    stmt(f"set {setting} = true")
stmt(f"create table {T} (id int primary key, k int, v int)")
stmt(f"create table {U} (id int primary key, k int, w int)")
stmt(f"create table {D} (id int primary key, name varchar)")
for name, cols, _ in CASES:
    stmt(f"create table {P}_{name}_off ({cols})")
    stmt(f"create table {P}_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: duplicates of k in u, NULL keys, rows without a match.\n")
stmt(f"insert into {T} values (1, 1, 0), (2, 1, 5), (3, 2, 1), (4, null, 2), (5, 7, 9), "
     f"(6, 3, null), (7, 2, 2), (8, 0, 0)")
stmt(f"insert into {U} select i, i % 4, i % 3 from generate_series(1, 200) as s(i)")
stmt(f"insert into {U} values (1000, null, 1), (1001, null, 2), (1002, 7, null)")
stmt(f"insert into {D} values (1, 'one'), (2, 'two'), (7, 'seven'), (9, null)")
run_all("First data")

query('I', f"select count(*) from {P}_count_on", ["1"])
query('I', f"select count(*) from {P}_in_distinct_on", ["7"])
query('I', f"select count(*) from {P}_anti_distinct_on", ["1"])

out.append("# Second data: more distinct values (the aggregation keeps most rows), changed rows.\n")
stmt(f"insert into {U} select i, i, i % 5 from generate_series(201, 3000) as s(i)")
stmt(f"update {T} set k = 250 where id = 8")
stmt(f"update {T} set k = null where id = 3")
stmt(f"insert into {T} values (9, 2999, 1), (10, 5000, 2), (11, 1000, 3)")
run_all("Second data")

out.append("# Third data: deletes, the distinct side emptied of some keys.\n")
stmt(f"delete from {U} where k = 2 or k is null")
stmt(f"delete from {D} where id = 1")
stmt(f"update {T} set v = 10 where k = 1")
run_all("Third data")

out.append("# Fourth data: the tested side emptied, then the kept side emptied.\n")
stmt(f"delete from {U}")
stmt(f"delete from {D}")
run_all("Empty tested side")
stmt(f"insert into {U} values (1, 1, 1), (2, 1, 1), (3, null, 2)")
stmt(f"insert into {D} values (1, 'one')")
stmt(f"delete from {T}")
run_all("Empty kept side")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table {P}_{name}_on")
    stmt(f"drop table {P}_{name}_off")
for table in (D, U, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
