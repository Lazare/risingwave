"""Generates `e2e_test/batch/push_down_limit.slt`, the differential test of `batch_push_down_limit`
(rule `LimitUnionJoinPushDownRule`: a copy of a limit or top-n goes into each branch of a
`UNION ALL` and into the preserved side of a left or right join). Every query runs twice, with and
without the variable, through `INSERT ... SELECT` (the batch plan), in local and distributed query
mode, and both results must hold the same rows with the same multiplicities, after each round of
changes of the data.

A `LIMIT` without a total order may return any rows, so every query has a result that does not
depend on which rows the limit keeps: a count of the rows of a limit, an `ORDER BY` of all the
output columns (rows that tie are equal), an order on a unique key, the columns that decide the
order only, `WITH TIES`, or a limit larger than the input.

Usage: python3 e2e_test/batch/push_down_limit.gen.py e2e_test/batch/push_down_limit.slt
"""
import sys

T1, T2, T3, L, R, RD = 'pl_t1', 'pl_t2', 'pl_t3', 'pl_l', 'pl_r', 'pl_rd'
VAR = 'batch_push_down_limit'
U13 = f"select a, b from {T1} union all select a, b from {T3}"

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule: unions.
    ('u_count', 'n bigint',
     f"select count(*) from (select a from {T1} union all select a from {T2} limit 5) s"),
    ('u_count_offset', 'n bigint',
     f"select count(*) from (select a from {T1} union all select a from {T3} limit 4 offset 3) s"),
    ('u_count_three', 'n bigint',
     f"select count(*) from (select a from {T1} union all select a from {T2} "
     f"union all select a from {T3} limit 9) s"),
    ('u_large', 'a int, b int',
     f"{U13} limit 1000"),
    ('u_topn', 'a int, b int',
     f"{U13} order by a, b limit 5 offset 2"),
    ('u_topn_desc', 'a int, b int',
     f"{U13} order by a desc nulls last, b desc limit 3"),
    ('u_topn_nulls_first', 'a int, b int',
     f"{U13} order by a nulls first, b limit 4"),
    ('u_ties', 'a int, b int',
     f"{U13} order by a fetch first 3 rows with ties"),
    ('u_filter', 'n bigint',
     f"select count(*) from (select * from ({U13}) u where b > 2 limit 3) s"),
    ('u_filter_topn', 'a int, b int',
     f"select * from ({U13}) u where b > 2 order by a, b limit 4"),
    ('u_expr_topn', 'a int, b1 int',
     f"select a, b + 1 as b1 from {T1} union all select a, b * 2 from {T3} order by a, b1 limit 4"),
    ('u_nested', 'x int',
     f"select x from (select a as x from {T1} union all select x from (select a + 1 as x from {T2} "
     f"union all select a from {T3}) s) v order by x limit 6"),
    ('u_branch_limited', 'n bigint',
     f"select count(*) from (select a from {T1} union all (select a from {T2} order by id limit 2) "
     f"limit 5) s"),
    ('u_branch_wider', 'n bigint',
     f"select count(*) from (select a from {T1} union all (select a from {T3} limit 100 offset 2) "
     f"limit 6) s"),
    ('u_join_branch', 'n bigint',
     f"select count(*) from (select l.x from {L} l left join {RD} d on l.k = d.k "
     f"union all select a from {T1} limit 6) s"),
    ('u_join_branch_topn', 'x int',
     f"select x from (select l.x from {L} l left join {RD} d on l.k = d.k "
     f"union all select a from {T1}) s order by x limit 5"),
    # Rewritten by the rule: outer joins.
    ('lj_count', 'n bigint',
     f"select count(*) from (select l.id, d.v from {L} l left join {RD} d on l.k = d.k limit 5) s"),
    ('lj_count_offset', 'n bigint',
     f"select count(*) from (select l.id, d.v from {L} l left join {RD} d on l.k = d.k "
     f"limit 4 offset 3) s"),
    ('lj_count_nonequi', 'n bigint',
     f"select count(*) from (select l.id, d.v from {L} l left join {RD} d on l.k < d.k limit 4) s"),
    ('lj_large', 'id int, v varchar',
     f"select l.id, d.v from {L} l left join {RD} d on l.k = d.k limit 1000"),
    ('lj_topn_unique', 'id int, x int, v varchar',
     f"select l.id, l.x, r.v from {L} l left join {R} r on l.k = r.k "
     f"order by l.x desc, l.id limit 4 offset 1"),
    ('lj_topn_dups', 'id int, x int',
     f"select id, x from (select l.id, l.x, d.v from {L} l left join {RD} d on l.k = d.k "
     f"order by l.id limit 5) s"),
    ('lj_topn_nulls', 'k int, n bigint',
     f"select k, count(*) from (select l.k, d.v from {L} l left join {RD} d on l.k = d.k "
     f"order by l.k nulls first limit 6) s group by k"),
    ('lj_ties', 'k int, id int, v varchar',
     f"select l.k, l.id, d.v from {L} l left join {RD} d on l.k = d.k "
     f"order by l.k fetch first 2 rows with ties"),
    ('lj_expr', 'x1 int, v varchar',
     f"select l.x + 1 as x1, r.v from {L} l left join {R} r on l.k = r.k order by l.id limit 3"),
    ('rj_count', 'n bigint',
     f"select count(*) from (select l.id, r.v from {L} l right join {R} r on l.k = r.k limit 3) s"),
    ('rj_topn', 'k int, v varchar',
     f"select k, v from (select l.id, r.k, r.v from {L} l right join {R} r on l.k = r.k "
     f"order by r.k limit 3) s"),
    ('rj_large', 'id int, k int',
     f"select l.id, d.k from {L} l right join {RD} d on l.k = d.k limit 1000"),
    # Left as they are by the rule.
    ('fj_count', 'n bigint',
     f"select count(*) from (select l.id, d.v from {L} l full join {RD} d on l.k = d.k limit 3) s"),
    ('fj_large', 'id int, v varchar',
     f"select l.id, d.v from {L} l full join {RD} d on l.k = d.k limit 1000"),
    ('fj_topn', 'id int, k int',
     f"select id, k from (select l.id, r.k from {L} l full join {R} r on l.k = r.k "
     f"order by l.id, r.k limit 4) s"),
    ('ij_count', 'n bigint',
     f"select count(*) from (select l.id, d.v from {L} l join {RD} d on l.k = d.k limit 3) s"),
    ('semi_count', 'n bigint',
     f"select count(*) from (select l.id from {L} l where exists "
     f"(select 1 from {RD} d where d.k = l.k) limit 2) s"),
    ('anti_count', 'n bigint',
     f"select count(*) from (select l.id from {L} l where not exists "
     f"(select 1 from {RD} d where d.k = l.k) limit 2) s"),
    ('lj_topn_right_key', 'id int, v varchar',
     f"select l.id, r.v from {L} l left join {R} r on l.k = r.k order by r.v, l.id limit 5"),
    ('lj_filter_kept', 'id int, v varchar',
     f"select l.id, r.v from {L} l left join {R} r on l.k = r.k "
     f"where coalesce(r.v, 'none') = 'none' limit 1000"),
    ('lj_inner_by_filter', 'n bigint',
     f"select count(*) from (select l.id, r.v from {L} l left join {R} r on l.k = r.k "
     f"where r.v is not null limit 2) s"),
    ('union_distinct', 'n bigint',
     f"select count(*) from (select a from {T1} union select a from {T3} limit 3) s"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    for mode in ('local', 'distributed'):
        out.append(f"# {title}, {mode} query mode: every query runs without, then with the "
                   f"variable.\n")
        stmt(f"set query_mode = {mode}")
        for name, _, sql in CASES:
            stmt(f"delete from pl_{name}_off")
            stmt(f"delete from pl_{name}_on")
            stmt(f"insert into pl_{name}_off {sql}")
            stmt(f"set {VAR} = true")
            stmt(f"insert into pl_{name}_on {sql}")
            stmt(f"set {VAR} = false")
        stmt("set query_mode = auto")
        out.append(f"# {title}, {mode} query mode: both results hold the same rows, each the same "
                   f"number of times.\n")
        for name, cols, _ in CASES:
            a, b, c = f"pl_{name}_on", f"pl_{name}_off", columns(cols)
            grouped = lambda t: f"select {c}, count(*) as pl_rows from {t} group by {c}"
            query('III',
                  f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
                  f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
                  f"  (select count(*) from {a}) - (select count(*) from {b})",
                  ["0 0 0"])


out.append("""# Differential test of `batch_push_down_limit`: every query runs through `INSERT ... SELECT`
# without, then with the variable, in local and distributed query mode, and both results must hold
# the same rows with the same multiplicities, after each round of changes of the data. Every query
# has a result that does not depend on which rows a limit keeps.
#
# Generated by `python3 e2e_test/batch/push_down_limit.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T1} (id int primary key, a int, b int)")
stmt(f"create table {T2} (id int primary key, a int, b int)")
stmt(f"create table {T3} (a int, b int)")
stmt(f"create table {L} (id int primary key, k int, x int)")
stmt(f"create table {R} (k int primary key, v varchar)")
stmt(f"create table {RD} (k int, v varchar)")
for name, cols, _ in CASES:
    stmt(f"create table pl_{name}_off ({cols})")
    stmt(f"create table pl_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULLs, duplicate rows, keys without a match on either side.\n")
stmt(f"insert into {T1} values (1, 1, 5), (2, 1, 5), (3, null, 2), (4, 3, null), (5, 2, 7), "
     f"(6, 0, 3), (7, 9, 1), (8, null, 9)")
stmt(f"insert into {T2} values (1, 4, 4), (2, 4, 4), (3, null, 1), (4, 6, 8), (5, 1, 0)")
stmt(f"insert into {T3} values (1, 5), (1, 5), (null, 2), (2, 2), (2, 3), (8, null), (0, 0), "
     f"(null, null), (5, 6)")
stmt(f"insert into {L} values (1, 1, 10), (2, 1, 20), (3, null, 30), (4, 2, null), (5, 7, 50), "
     f"(6, 0, 60), (7, 9, 70), (8, null, 80), (9, 2, 90)")
stmt(f"insert into {R} values (1, 'one'), (2, null), (0, 'zero'), (5, 'five'), (6, 'six')")
stmt(f"insert into {RD} values (1, 'a'), (1, 'b'), (1, 'a'), (2, 'c'), (null, 'n'), (null, null), "
     f"(4, 'd'), (9, null), (9, 'e')")
run_all("First data")

out.append("# The results are not empty, the limits cut the inputs.\n")
query('I', "select n from pl_u_count_on", ["5"])
query('I', "select n from pl_lj_count_on", ["5"])
query('I', "select count(*) from pl_u_ties_on", ["6"])
query('I', "select count(*) from pl_lj_ties_on", ["7"])

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {T1} values (9, 1, 1), (10, null, null), (1, 1, 6)")
stmt(f"insert into {T2} values (6, 0, 0), (7, 4, 4)")
stmt(f"insert into {T3} values (1, 5), (null, 2), (9, 9)")
stmt(f"insert into {L} values (10, 1, 100), (11, null, 110), (1, 5, 10)")
stmt(f"insert into {R} values (7, 'seven'), (1, 'uno')")
stmt(f"insert into {RD} values (7, 'f'), (7, 'g'), (5, null)")
run_all("Second data")

out.append("# Third data: updates and deletes.\n")
stmt(f"update {T1} set a = null where id in (5, 6)")
stmt(f"delete from {T2} where id in (1, 2)")
stmt(f"delete from {T3} where a = 1")
stmt(f"update {L} set k = 9 where id in (3, 4)")
stmt(f"delete from {L} where id = 2")
stmt(f"delete from {R} where k = 0")
stmt(f"delete from {RD} where k = 2")
run_all("Third data")

out.append("# Fourth data: the right sides and one branch emptied.\n")
stmt(f"delete from {R}")
stmt(f"delete from {RD}")
stmt(f"delete from {T2}")
run_all("Empty right sides")

out.append("# Fifth data: the right sides filled again, the left side and another branch emptied.\n")
stmt(f"insert into {R} values (1, 'one'), (2, 'two')")
stmt(f"insert into {RD} values (1, 'a'), (null, 'n'), (1, 'a')")
stmt(f"insert into {T2} values (1, 3, 3), (2, null, 3)")
stmt(f"delete from {L}")
stmt(f"delete from {T1}")
run_all("Empty left side")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table pl_{name}_on")
    stmt(f"drop table pl_{name}_off")
for table in (RD, R, L, T3, T2, T1):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
