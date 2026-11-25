"""Generates `e2e_test/streaming/merge_union_scans.slt`, the differential test of the single read of
a table that several branches of a union read in streaming plans: every query is materialized with
and without `streaming_merge_union_scans` and `streaming_share_union_scans` (both on, or both off).

Every query names its output columns c1, c2, ...: the materialized views are compared as multisets
(each distinct row with its number of occurrences), since the rewrite must keep the duplicates.

Usage: python3 e2e_test/streaming/merge_union_scans.gen.py e2e_test/streaming/merge_union_scans.slt
"""
import sys

T, U, H, N, M, J = 'mus_t', 'mus_u', 'mus_h', 'mus_n', 'mus_m', 'mus_j'

# (name, comment, number of output columns, query)
CASES = [
    ('two', 'two branches with a different filter and projection', 2,
     f"select a as c1, x as c2 from {T} where a > 0 "
     f"union all select b, y from {T} where b > 0"),
    ('bool', 'exclusive filters on a boolean column, constants per branch', 3,
     f"select k as c1, 'yes' as c2, 5 as c3 from {T} where flag "
     f"union all select k, 'no', 4 from {T} where not flag"),
    ('plain', 'three branches without a filter', 2,
     f"select k as c1, 'r1' as c2 from {T} "
     f"union all select k, 'r2' from {T} union all select k, 'r3' from {T}"),
    ('overlap', 'overlapping filters: a row can pass both', 2,
     f"select k as c1, a as c2 from {T} where a > 0 "
     f"union all select k, b from {T} where b > 0 or a > 5"),
    ('nulls', 'filters that are NULL on NULL columns', 3,
     f"select k as c1, a as c2, 'gt' as c3 from {T} where a > b "
     f"union all select k, b, 'le' from {T} where not (a > b) "
     f"union all select k, null::int, 'eq' from {T} where x = y"),
    ('one_open', 'one branch without a filter and one with a filter', 3,
     f"select k as c1, 'all' as c2, 1 as c3 from {T} "
     f"union all select k, 'nulls', 2 from {T} where a is null"),
    ('common', 'a conjunct common to every branch', 2,
     f"select k as c1, a as c2 from {T} where a > 0 and b > 1 "
     f"union all select k, b from {T} where b < 5 and a > 0"),
    ('share', 'branches over the same shared sub-plan', 2,
     f"with c as (select k, a, b from {T} where a is not null) "
     f"select k as c1, a as c2 from c where b > 1 union all select k, b from c where b < 5 "
     f"union all select a, b from c"),
    ('share_other', 'the shared sub-plan also read by a join', 3,
     f"with c as (select k, a, b from {T} where a is not null) "
     f"select s.k as c1, s.v as c2, c.b as c3 from (select k, a as v from c where b > 1 "
     f"union all select k, b from c where b < 5) s join c on s.k = c.k"),
    ('same_twice', 'the same branch twice', 2,
     f"select k as c1, a as c2 from {T} where a > 0 "
     f"union all select k, a from {T} where a > 0"),
    ('two_tables', 'two tables, two branches each', 2,
     f"select k as c1, a as c2 from {T} where a > 0 "
     f"union all select k, c from {U} where c > 0 "
     f"union all select k, b from {T} where b > 0 "
     f"union all select k, a from {U} where a < 0"),
    ('three_two', 'three branches, two of them read the same table', 3,
     f"select k as c1, a as c2, 't' as c3 from {T} where a > 0 "
     f"union all select k, c, 'u' from {U} "
     f"union all select k, b, 't2' from {T} where b > 0"),
    ('computed', 'computed columns that differ between branches', 3,
     f"select k as c1, greatest(a, 0) as c2, 'x' as c3 from {T} where x = 'A' "
     f"union all select k, b + 1, 'y' from {T} where x = 'B'"),
    ('distinct', 'union without all', 3,
     f"select a as c1, x as c2, 'x' as c3 from {T} where x is not null "
     f"union select a, y, 'y' from {T} where y is not null"),
    ('agg', 'aggregation above the union', 3,
     f"select acc as c1, coalesce(sum(v), 0) as c2, count(*) as c3 from ("
     f"select a as acc, -b as v from {T} where x <> 'in' "
     f"union all select b, b from {T} where x <> 'out') s group by acc"),
    ('top1', 'top-1 per key above the union', 2,
     f"select k as c1, st as c2 from (select k, st, row_number() over (partition by k order by p desc) as rn from ("
     f"select k, 'low' as st, 1 as p from {H} where a > 0 "
     f"union all select k, 'high' as st, 2 as p from {H} where a > 10) as v) as r where rn = 1"),
    ('no_key', 'a table without a primary key holding duplicate rows', 2,
     f"select k as c1, a as c2 from {N} where a > 0 "
     f"union all select a, k from {N} where k > 0 or a is null"),
    ('share_join', 'branches over a shared join', 2,
     f"with c as (select {T}.k, {T}.a, {U}.c from {T} join {U} on {T}.a = {U}.a) "
     f"select k as c1, a as c2 from c where c > 0 union all select k, c from c where a > 0"),
    ('share_agg', 'branches over a shared aggregation', 2,
     f"with c as (select a, count(*) as n from {T} group by a) "
     f"select a as c1, n as c2 from c where n > 1 union all select n, a from c where a > 0"),
    # Scans shared by branches that do more than filter and project the table.
    ('join_plain', 'a branch with a join and a plain branch', 2,
     f"select k as c1, a as c2 from {T} where a > 0 "
     f"union all select {T}.k, {U}.c from {T} join {U} on {T}.k = {U}.k"),
    ('left_join_plain', 'a branch with a left join and a plain branch', 2,
     f"select t.k as c1, coalesce(m.v, 0) as c2 from {T} t left join {M} m on t.k = m.g where t.x = 'A' "
     f"union all select t.k, 1 from {T} t where t.x = 'B'"),
    ('two_joins', 'two joins on different columns', 2,
     f"select t.k as c1, m.v as c2 from {T} t join {M} m on t.a = m.g and m.v > 0 "
     f"union all select t.k, m.v from {T} t join {M} m on t.b = m.g and m.v < 0"),
    ('srf', 'two table functions and a plain branch', 2,
     f"select j.r as c1, x.key as c2 from {J} j cross join jsonb_each_text(j.f -> 'a') as x where kind in ('e', 'p') "
     f"union all select j.r, x.key from {J} j cross join jsonb_each_text(j.f -> 'b') as x where kind in ('e', 'p') "
     f"union all select r, kind from {J} where kind = 'p' and f ? 'a'"),
    ('anti', 'an anti join in one branch', 2,
     f"select k as c1, 'u' as c2 from {U} "
     f"union all select t.k, 't' from {T} t where not exists (select 1 from {U} u where u.k = t.k)"),
]

out = []
def switch(on):
    stmt(f"set streaming_merge_union_scans = {'true' if on else 'false'}")
    stmt(f"set streaming_share_union_scans = {'true' if on else 'false'}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def counted(relation, width):
    columns = ", ".join(f"c{i}" for i in range(1, width + 1))
    return f"select {columns}, count(*) as n from {relation} group by {columns}"

def same_multiset(a, b, width):
    return (f"select\n  (select count(*) from ({counted(a, width)} except {counted(b, width)}) x),\n"
            f"  (select count(*) from ({counted(b, width)} except {counted(a, width)}) y),\n"
            f"  (select count(*) from {a}) - (select count(*) from {b})")

def compare_all(title):
    out.append(f"# {title}: with and without the variable, every materialized view holds the same rows.\n")
    for name, _, width, _ in CASES:
        query('III', same_multiset(f"mus_{name}_on", f"mus_{name}_off", width), ["0 0 0"])
    out.append(f"# {title}: with the variable, every materialized view holds the rows of its query.\n")
    for name, _, width, sql in CASES:
        query('III', same_multiset(f"mus_{name}_on", f"({sql}) q", width), ["0 0 0"])

out.append("""# Differential test of `streaming_merge_union_scans`: every query is materialized twice, with and
# without the single read of the table that several branches of its union read, and both
# materialized views must hold the same multiset of rows, and the rows of the query, after the
# backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/merge_union_scans.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (k int primary key, a int, b int, x varchar, y varchar, flag boolean)")
stmt(f"create table {U} (k int primary key, a int, c int)")
stmt(f"create table {H} (r int primary key, k int, a int)")
stmt(f"create table {N} (k int, a int)")
stmt(f"create table {M} (r int primary key, g int, v int)")
stmt(f"create table {J} (r int primary key, kind varchar, f jsonb)")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {T} values (1, 1, 2, 'A', 'A', true), (2, 3, null, 'B', 'x', false), (3, null, 4, 'in', null, null), "
     f"(4, -1, -2, 'out', 'out', true), (5, 6, 1, null, 'y', false), (6, 0, 0, 'A', 'B', null), "
     f"(7, 2, 2, 'B', 'B', true), (8, null, null, null, null, null), (9, 7, 9, 'in', 'A', false)")
stmt(f"insert into {U} values (1, 1, 5), (2, -3, 0), (3, null, 2), (10, -1, null), (11, 4, -4)")
stmt(f"insert into {H} values (1, 1, 5), (2, 1, 20), (3, 2, 3), (4, 2, null), (5, 3, -1), (6, null, 15), (7, 4, 11), (8, 4, 2)")
stmt(f"insert into {N} values (1, 1), (1, 1), (2, null), (2, null), (0, 3), (-1, -1), (null, 2), (null, null)")
stmt(f"insert into {M} values (1, 1, 5), (2, 1, -5), (3, 2, null), (4, 6, 7), (5, null, 1), (6, 7, -2), (7, 3, 3)")
stmt(f"""insert into {J} values (1, 'e', '{{"a": {{"x": 1, "y": 2}}, "b": {{"z": 3}}}}'), (2, 'p', '{{"a": {{"x": 1}}}}'), """
     f"""(3, 'q', '{{"a": {{"w": 0}}}}'), (4, 'p', null), (5, null, '{{"b": {{"x": 1, "x2": 2}}}}'), (6, 'p', '{{"a": {{}}}}')""")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, _, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view mus_{name}_{sfx} as {sql}")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('IT', "select c1, c2 from mus_two_on order by c1, c2",
      ["1 A", "1 y", "2 A", "2 B", "2 B", "3 B", "4 NULL", "6 NULL", "7 in", "9 A"])
query('II', "select c1, c2 from mus_same_twice_on order by c1, c2",
      ["1 1", "1 1", "2 3", "2 3", "5 6", "5 6", "7 2", "7 2", "9 7", "9 7"])
query('II', "select c1, c2 from mus_overlap_on order by c1, c2",
      ["1 1", "1 2", "2 3", "3 4", "5 1", "5 6", "7 2", "7 2", "9 7", "9 9"])

out.append("# First round of changes: new rows, some written again with an existing primary key.\n")
stmt(f"insert into {T} values (10, 5, 5, 'A', 'B', true), (11, null, 3, 'B', 'B', false), (12, 8, -8, 'out', null, null)")
stmt(f"insert into {T} values (1, 2, 1, 'B', 'A', false)")
stmt(f"insert into {U} values (12, 6, 6), (1, -5, 1)")
stmt(f"insert into {H} values (9, 3, 50), (10, 5, 1)")
stmt(f"insert into {N} values (1, 1), (3, null), (null, 2)")
compare_all("After the inserts")

out.append("# Second round of changes: updates that move rows between branches, to and from NULL.\n")
stmt(f"update {T} set flag = not flag where k in (1, 2, 4)")
stmt(f"update {T} set flag = null where k = 7")
stmt(f"update {T} set a = -a, b = b + 3 where k in (5, 9, 10)")
stmt(f"update {T} set a = null where k = 7")
stmt(f"update {T} set a = 4, b = 1 where k = 8")
stmt(f"update {T} set x = 'A', y = 'A' where k = 2")
stmt(f"update {T} set x = null where k = 6")
stmt(f"update {U} set c = -c where k in (1, 11)")
stmt(f"update {U} set a = 9 where k = 3")
stmt(f"update {H} set a = 1 where r = 2")
stmt(f"update {H} set a = 12 where r = 3")
stmt(f"update {N} set a = 5 where k = 2")
stmt(f"update {M} set g = 2 where r = 1")
stmt(f"update {M} set v = -v where r in (4, 7)")
stmt(f"update {J} set kind = 'e' where r = 3")
stmt(f"""update {J} set f = '{{"a": {{"n": 9}}, "b": {{"m": 8}}}}' where r = 2""")
compare_all("After the updates")

out.append("# Third round of changes: deletes, including all the copies of a duplicate row.\n")
stmt(f"delete from {T} where k in (3, 9)")
stmt(f"delete from {T} where a is null")
stmt(f"delete from {U} where k = 2")
stmt(f"delete from {H} where k = 4")
stmt(f"delete from {N} where k = 1")
stmt(f"delete from {N} where a is null")
stmt(f"delete from {M} where r = 2")
stmt(f"delete from {J} where r = 1")
compare_all("After the deletes")

out.append("# Fourth round of changes: the table is emptied and filled again.\n")
stmt(f"delete from {T}")
compare_all("After the deletion of the whole table")
stmt(f"insert into {T} values (1, 1, 1, 'A', 'A', true), (2, 1, 1, 'A', 'A', true), (3, null, 5, 'B', 'in', false), (4, 9, null, 'out', 'B', null)")
compare_all("After the table is filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _, _ in reversed(CASES):
        stmt(f"drop materialized view mus_{name}_{sfx}")
for table in (J, M, N, H, U, T):
    stmt(f"drop table {table}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
