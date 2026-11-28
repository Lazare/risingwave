"""Generates `e2e_test/batch/limit_distinct.slt`, the differential test of `batch_limit_distinct`
(rule `BatchLimitDistinctRule`, operator `BatchLimitDistinct`: a distinct under a `LIMIT` stops
after `limit + offset` distinct rows, with a per-task copy in each branch of a `UNION`). Every query
runs twice, with and without the variable, through `INSERT ... SELECT` (the batch plan), in local
and distributed query mode, and both results must hold the same rows with the same
multiplicities, after each round of changes of the data.

A `LIMIT` over a distinct may return any of the distinct rows, so every query has a result that
does not depend on which ones: a count of the rows of the limit, a count of the values that appear
twice in it (always 0: the rows are distinct), a limit larger than the number of distinct rows.

Usage: python3 e2e_test/batch/limit_distinct.gen.py e2e_test/batch/limit_distinct.slt
"""
import sys

T1, T2, T3 = 'ld_t1', 'ld_t2', 'ld_t3'
VAR = 'batch_limit_distinct'


def no_dups(cols, sql):
    return (f"select count(*) from (select {cols} from ({sql}) s group by {cols} "
            f"having count(*) > 1) x")


U12 = f"select a from {T1} union select a from {T2}"
U123 = f"select a, b from {T1} union select a, b from {T2} union select a, b from {T3}"

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('d_count', 'n bigint',
     f"select count(*) from (select distinct a from {T1} limit 3) s"),
    ('d_count_offset', 'n bigint',
     f"select count(*) from (select distinct a from {T1} limit 3 offset 2) s"),
    ('d_count_pairs', 'n bigint',
     f"select count(*) from (select distinct a, b from {T1} limit 40) s"),
    ('d_count_nulls', 'n bigint',
     f"select count(*) from (select distinct b from {T1} limit 100) s"),
    ('d_no_dups', 'n bigint',
     no_dups('a, b', f"select distinct a, b from {T1} limit 30")),
    ('d_large', 'a int, b int',
     f"select distinct a, b from {T1} limit 100000"),
    ('d_expr_large', 's int',
     f"select distinct a + b as s from {T1} limit 100000"),
    ('d_varchar_count', 'n bigint',
     f"select count(*) from (select distinct c from {T1} limit 5) s"),
    ('u_count', 'n bigint',
     f"select count(*) from ({U12} limit 3) s"),
    ('u_count_offset', 'n bigint',
     f"select count(*) from ({U12} limit 4 offset 1) s"),
    ('u_count_three', 'n bigint',
     f"select count(*) from ({U123} limit 25) s"),
    ('u_no_dups', 'n bigint',
     no_dups('a', f"{U12} limit 20")),
    ('u_no_dups_three', 'n bigint',
     no_dups('a, b', f"{U123} limit 60")),
    ('u_large', 'a int',
     f"{U12} limit 100000"),
    ('u_large_three', 'a int, b int',
     f"{U123} limit 100000"),
    # Left as they are by the rule.
    ('agg_calls_large', 'a int, n bigint',
     f"select a, count(*) from {T1} group by a limit 100000"),
    ('distinct_on_count', 'n bigint',
     f"select count(*) from (select distinct on (a) a, b from {T1} limit 3) s"),
    ('topn', 'a int',
     f"select distinct a from {T1} order by a limit 3"),
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
            stmt(f"delete from ld_{name}_off")
            stmt(f"delete from ld_{name}_on")
            stmt(f"insert into ld_{name}_off {sql}")
            stmt(f"set {VAR} = true")
            stmt(f"insert into ld_{name}_on {sql}")
            stmt(f"set {VAR} = false")
        stmt("set query_mode = auto")
        out.append(f"# {title}, {mode} query mode: both results hold the same rows, each the same "
                   f"number of times.\n")
        for name, cols, _ in CASES:
            a, b, c = f"ld_{name}_on", f"ld_{name}_off", columns(cols)
            grouped = lambda t: f"select {c}, count(*) as ld_rows from {t} group by {c}"
            query('III',
                  f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
                  f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
                  f"  (select count(*) from {a}) - (select count(*) from {b})",
                  ["0 0 0"])
        query('I', "select n from ld_d_no_dups_on", ["0"])
        query('I', "select n from ld_u_no_dups_on", ["0"])
        query('I', "select n from ld_u_no_dups_three_on", ["0"])


out.append("""# Differential test of `batch_limit_distinct`: every query runs through `INSERT ... SELECT`
# without, then with the variable, in local and distributed query mode, and both results must hold
# the same rows with the same multiplicities, after each round of changes of the data. Every query
# has a result that does not depend on which distinct rows a limit keeps.
#
# Generated by `python3 e2e_test/batch/limit_distinct.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T1} (id int primary key, a int, b int, c varchar)")
stmt(f"create table {T2} (id int primary key, a int, b int)")
stmt(f"create table {T3} (a int, b int)")
for name, cols, _ in CASES:
    stmt(f"create table ld_{name}_off ({cols})")
    stmt(f"create table ld_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULLs (equal for DISTINCT), duplicate rows, few distinct values.\n")
stmt(f"insert into {T1} values (1, 1, 5, 'x'), (2, 1, 5, 'x'), (3, null, 2, null), "
     f"(4, null, 2, null), (5, 2, null, 'y'), (6, 0, 3, 'z'), (7, 9, null, 'x'), (8, 1, 6, 'w')")
stmt(f"insert into {T2} values (1, 4, 4), (2, 4, 4), (3, null, 1), (4, 1, 0), (5, 2, 5)")
stmt(f"insert into {T3} values (1, 5), (1, 5), (null, 2), (null, null), (null, null), (8, 1)")
run_all("First data")

out.append("# The limits cut the distinct rows.\n")
query('I', "select n from ld_d_count_on", ["3"])
query('I', "select n from ld_u_count_on", ["3"])
query('I', "select n from ld_d_count_nulls_on", ["5"])

out.append("# Second data: many rows over several chunks and tasks, many duplicates.\n")
stmt(f"insert into {T1} select i, i % 50, i % 7, (i % 11)::varchar from generate_series(9, 5000) i")
stmt(f"insert into {T2} select i, i % 80, i % 3 from generate_series(6, 3000) i")
stmt(f"insert into {T3} select i % 30, null from generate_series(1, 2000) i")
run_all("Second data")

out.append("# Third data: updates and deletes, one branch emptied.\n")
stmt(f"update {T1} set a = null where id % 13 = 0")
stmt(f"delete from {T1} where id % 5 = 0")
stmt(f"delete from {T2}")
stmt(f"update {T3} set b = a where a % 2 = 0")
run_all("Third data")

out.append("# Fourth data: a single distinct row left in each table.\n")
stmt(f"delete from {T1} where id > 2")
stmt(f"insert into {T2} values (1, 1, 5)")
stmt(f"delete from {T3}")
stmt(f"insert into {T3} values (1, 5), (1, 5)")
run_all("One distinct row")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table ld_{name}_on")
    stmt(f"drop table ld_{name}_off")
for table in (T3, T2, T1):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
