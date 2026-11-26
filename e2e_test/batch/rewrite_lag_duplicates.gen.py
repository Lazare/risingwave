"""Generates `e2e_test/batch/rewrite_lag_duplicates.slt`, the differential test of the rewrite of
the duplicates found with `lag` in batch queries: every query is run with and without
`batch_rewrite_lag_duplicates`, its rows inserted into a table, and both tables must hold the same
multiset of rows after each change of the data.

Usage: python3 e2e_test/batch/rewrite_lag_duplicates.gen.py e2e_test/batch/rewrite_lag_duplicates.slt
"""
import sys

T = 'rld_t'

CASES = [
    # Rewritten.
    ('fused', 'count of the duplicates per value, as a uniqueness check', 'int, bigint',
     f"with ordered as (select x, lag(x) over (partition by true order by x) as prev from {T} where x is not null), "
     f"duplicates as (select * from ordered where x = prev) "
     f"select x as unique_field, count(*) + 1 as n_records from duplicates group by x"),
    ('rows', 'the duplicate rows themselves', 'int, int',
     f"with ordered as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select * from ordered where x = prev"),
    ('count_all', 'count without group key', 'bigint',
     f"with ordered as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select count(*) from ordered where x = prev"),
    ('lead', 'lead instead of lag', 'int, bigint',
     f"with ordered as (select x, lead(x) over (order by x) as next from {T}) "
     f"select x, count(*) from ordered where next = x group by x"),
    ('lag_minus_one', 'lag with offset -1 is lead', 'int, bigint',
     f"with ordered as (select x, lag(x, -1) over (order by x) as next from {T}) "
     f"select x, count(*) from ordered where x = next group by x"),
    ('desc', 'descending order, nulls first', 'int, int',
     f"with ordered as (select x, lag(x) over (order by x desc nulls first) as prev from {T}) "
     f"select x, prev from ordered where x = prev"),
    ('tie_break', 'tie-breaking order column after x', 'int, int',
     f"with ordered as (select x, lag(x) over (order by x, id desc) as prev from {T}) "
     f"select x, prev from ordered where x = prev"),
    ('first_value', 'first_value over the previous row', 'int, bigint',
     f"with ordered as (select x, first_value(x) over (order by x rows between 1 preceding and 1 preceding) as prev from {T}) "
     f"select x, count(*) from ordered where x = prev group by x"),
    ('varchar', 'varchar column', 'varchar, bigint',
     f"with ordered as (select s, lag(s) over (partition by true order by s) as prev from {T} where s is not null), "
     f"duplicates as (select * from ordered where s = prev) "
     f"select s, count(*) + 1 as n from duplicates group by s"),
    ('part_fused', 'partition column, grouped by it and x', 'int, int, bigint',
     f"with ordered as (select p, x, lag(x) over (partition by p order by x) as prev from {T}) "
     f"select p, x, count(*) from ordered where x = prev group by p, x"),
    ('part_rows', 'partition column, ordered by it first, rows read', 'int, int',
     f"with ordered as (select p, x, lag(x) over (partition by p order by p, x) as prev from {T}) "
     f"select x, p from ordered where x = prev"),
    ('part_by_x', 'partition column, grouped by x only', 'int, bigint',
     f"with ordered as (select p, x, lag(x) over (partition by p order by x) as prev from {T}) "
     f"select x, count(*) from ordered where x = prev group by x"),
    ('counts', 'count of the lag column and of x', 'int, bigint, bigint, bigint',
     f"with ordered as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select prev, count(prev), count(x), count(*) from ordered where x = prev group by prev"),
    ('max', 'other aggregate', 'int, bigint, int',
     f"with ordered as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select x, count(*), max(prev) from ordered where x = prev group by x"),
    ('condition', 'condition on x and the lag column', 'int, bigint',
     f"with ordered as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select x, count(*) from ordered where x = prev and prev > 2 and x <> 4 group by x"),
    ('other_windows', 'other window functions, not read above', 'int, bigint',
     f"with ordered as (select x, lag(x) over (order by x) as prev, lag(y) over (order by x) as prev_y, "
     f"row_number() over (order by y) as rn from {T}) "
     f"select x, count(*) from ordered where x = prev group by x"),
    ('exprs', 'expressions of x read above', 'int, int',
     f"with ordered as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select x + 1 as x1, prev * 2 as p2 from ordered where x = prev"),
    # Not rewritten: the results would differ if they were.
    ('kept_id', 'another column of the row read above', 'int, int',
     f"with ordered as (select id, x, lag(x) over (order by x, id) as prev from {T}) "
     f"select id, x from ordered where x = prev"),
    ('kept_not_distinct', 'is not distinct from', 'int, bigint',
     f"with ordered as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select x, count(*) from ordered where x is not distinct from prev group by x"),
    ('kept_offset', 'offset 2', 'int, bigint',
     f"with ordered as (select x, lag(x, 2) over (order by x) as prev from {T}) "
     f"select x, count(*) from ordered where x = prev group by x"),
    ('kept_frame', 'frame ending at the current row', 'int, bigint',
     f"with ordered as (select x, first_value(x) over (order by x rows 1 preceding) as prev from {T}) "
     f"select x, count(*) from ordered where x = prev group by x"),
    ('kept_condition', 'condition on another column', 'int, bigint',
     f"with ordered as (select x, y, lag(x) over (order by x, id) as prev from {T}) "
     f"select x, count(*) from ordered where x = prev and y > 3 group by x"),
]

VAR = 'batch_rewrite_lag_duplicates'

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(types):
    return [f"c{i + 1}" for i in range(len(types.split(',')))]


def run_all(title):
    out.append(f"# {title}: every query with and without the variable.\n")
    for name, _, _, sql in CASES:
        stmt(f"delete from rld_{name}_off")
        stmt(f"delete from rld_{name}_on")
    stmt(f"set {VAR} = false")
    for name, _, _, sql in CASES:
        stmt(f"insert into rld_{name}_off {sql}")
    stmt(f"set {VAR} = true")
    for name, _, _, sql in CASES:
        stmt(f"insert into rld_{name}_on {sql}")
    stmt(f"set {VAR} = false")
    out.append(f"# {title}: both tables hold the same rows, as many times each.\n")
    for name, _, types, _ in CASES:
        cols = ', '.join(columns(types))
        a = f"(select {cols}, count(*) as n from rld_{name}_on group by {cols})"
        b = f"(select {cols}, count(*) as n from rld_{name}_off group by {cols})"
        query('III',
              f"select\n  (select count(*) from ({a} except {b}) x),\n"
              f"  (select count(*) from ({b} except {a}) y),\n"
              f"  (select count(*) from rld_{name}_on) - (select count(*) from rld_{name}_off)",
              ["0 0 0"])


out.append(f"""# Differential test of `{VAR}`: every query is run twice, with and without the
# rewrite of the duplicates found with `lag`, its rows inserted into a table, and both tables must
# hold the same rows, as many times each, after each change of the data.
#
# Generated by `python3 e2e_test/batch/rewrite_lag_duplicates.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (id int primary key, p int, x int, y int, s varchar)")
for name, comment, types, _ in CASES:
    cols = ', '.join(f"{c} {t.strip()}" for c, t in zip(columns(types), types.split(',')))
    out.append(f"# {comment}\n")
    stmt(f"create table rld_{name}_off ({cols})")
    stmt(f"create table rld_{name}_on ({cols})")

out.append("# Duplicates, single values, NULLs of x, of p and of s.\n")
stmt(f"insert into {T} values (1, 1, 1, 1, 'a'), (2, 1, 1, 2, 'a'), (3, 2, 1, 3, 'b'), (4, 1, 2, 4, 'c'), "
     f"(5, null, 3, 5, 'c'), (6, null, 3, 6, 'c'), (7, 2, null, 7, null), (8, 2, null, 8, 'd'), "
     f"(9, 1, 4, 9, 'e'), (10, 2, 5, 10, 'e'), (11, 2, 5, 11, 'E'), (12, null, null, null, null)")
run_all("Initial rows")

out.append("# Spot checks of the uniqueness check: values present k times give k.\n")
query('II', "select c1, c2 from rld_fused_on order by c1", ["1 3", "3 2", "5 2"])
query('TI', "select c1, c2 from rld_varchar_on order by c1", ["a 2", "c 3", "e 2"])
query('III', "select c1, c2, c3 from rld_part_fused_on order by c1 nulls first, c2",
      ["NULL 3 1", "1 1 1", "2 5 1"])

out.append("# Many rows, many duplicates in several partitions.\n")
stmt(f"insert into {T} select 100 + i, i % 3, i % 17, i % 11, 'v' || (i % 13) from generate_series(1, 300) i")
stmt(f"insert into {T} select 500 + i, null::int, case when i % 4 = 0 then null else i % 5 end, i, null::varchar "
     f"from generate_series(1, 40) i")
run_all("After the inserts")

out.append("# Deletes and updates: some values lose their duplicates, NULLs move.\n")
stmt(f"delete from {T} where id between 100 and 250 or id in (1, 6)")
stmt(f"update {T} set x = null where id % 7 = 0")
stmt(f"update {T} set p = null where id % 5 = 0")
stmt(f"update {T} set x = 1000 + id where id % 3 = 0")
run_all("After the deletes and updates")

out.append("# No duplicates left: every query returns no row, or a count of 0.\n")
stmt(f"delete from {T} where id not in (select min(id) from {T} where x is not null group by x)")
run_all("Without duplicates")

out.append("# Empty table.\n")
stmt(f"delete from {T}")
run_all("Empty table")

out.append("# Cleanup.\n")
for name, _, _, _ in CASES:
    stmt(f"drop table rld_{name}_off")
    stmt(f"drop table rld_{name}_on")
stmt(f"drop table {T}")

with open(sys.argv[1], 'w') as f:
    f.write("\n".join(out))
