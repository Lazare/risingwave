"""Generates `e2e_test/streaming/share_and_stacked_aggs.slt`, the differential test of the late
sharing of identical sub-plans and of the merge of stacked aggregations in streaming plans: every
query is materialized with and without `streaming_share_identical_subplans` and
`streaming_merge_stacked_aggregations`.

Usage: python3 e2e_test/streaming/share_and_stacked_aggs.gen.py e2e_test/streaming/share_and_stacked_aggs.slt
"""
import sys

T, S, A, H, B = 'ssa_t', 'ssa_s', 'ssa_a', 'ssa_h', 'ssa_b'
V1, G1 = 'ssa_v1', 'ssa_g1'  # views: a join, an aggregation with an aggregate that is not read

CASES = [
    # Late sharing of identical sub-plans.
    ('share_join', 'the same join, filtered above a view and in the query',
     f"select id, v from {V1} where v > 0 "
     f"union all select a.id, h.v from {A} a join {H} h on a.k = h.sid where h.v > 0"),
    ('share_agg', 'the same aggregation, from a view whose other aggregate is pruned',
     f"select a.id, g1.c from {A} a join {G1} g1 on a.k = g1.sid "
     f"union all select b.id, x.c from {B} b join (select sid, count(*) as c from {H} group by sid) x on b.k = x.sid"),
    # The count is wrapped in `coalesce`: with and without the variables, the streaming plan of
    # two select-list subqueries gives NULL instead of 0 for an outer row whose correlated column
    # is NULL (the domains are joined back after a filter that drops the NULL key).
    ('share_domain', 'the domain of two subqueries with the same correlation',
     f"select a.id, (select max(v) from {H} h where h.sid = a.k) as m1, "
     f"coalesce((select count(*) from {H} h where h.sid = a.k and h.st = 'x'), 0) as c2 from {A} a"),
    ('share_cte', 'a sub-plan shared at the beginning',
     f"with j as (select a.id, h.v from {A} a join {H} h on a.k = h.sid where h.v > 0) "
     f"select id, v from j union all select id, v + 1 from j"),
    ('share_scans', 'identical filtered scans, not shared',
     f"select a.id from {A} a where exists (select 1 from {H} h where h.sid = a.k and h.v > 0) "
     f"union all select b.id from {B} b where exists (select 1 from {H} h where h.sid = b.k and h.v > 0)"),
    # Merge of stacked aggregations.
    ('stacked', 'sum of sums, sum of counts, max of maxes, min of mins on fewer columns',
     f"select g, coalesce(sum(sv), 0) as s, sum(c) as c, sum(cv) as cv, max(mx) as mx, min(mn) as mn from ("
     f"select g, w, sum(v) as sv, count(*) as c, count(v) as cv, max(v) as mx, min(v) as mn "
     f"from {T} group by g, w) x group by g"),
    ('types', 'sum of sums of bigint and numeric',
     f"select g, coalesce(sum(sb), 0) as sb, coalesce(sum(sn), 0) as sn from ("
     f"select g, w, sum(big) as sb, sum(n) as sn from {T} group by g, w) x group by g"),
    ('filter', 'inner aggregates with FILTER, bool_or and bool_and',
     f"select g, coalesce(sum(sp), 0) as sp, sum(cp) as cp, bool_or(bo) as bo, bool_and(ba) as ba from ("
     f"select g, w, sum(v) filter (where v > 1) as sp, count(*) filter (where w > 0) as cp, "
     f"bool_or(flag) as bo, bool_and(flag) as ba from {T} group by g, w) x group by g"),
    ('same_key', 'the same group key on both levels',
     f"select g, w, coalesce(sum(sv), 0) as s, max(mx) as mx from ("
     f"select g, w, sum(v) as sv, max(v) as mx from {T} group by g, w) x group by g, w"),
    ('scalar', 'without outer group key',
     f"select coalesce(sum(sv), 0) as s, max(mx) as mx, min(mn) as mn from ("
     f"select g, sum(v) as sv, max(v) as mx, min(n) as mn from {T} group by g) x"),
    ('distinct_aggs', 'a DISTINCT below count and sum',
     f"select g, count(v) as c, coalesce(sum(v), 0) as s from (select distinct g, v from {T}) d group by g"),
    ('distinct_fewer', 'a DISTINCT below a count distinct on fewer columns',
     f"select g, count(distinct v) as c from (select distinct g, v, w from {T}) d group by g"),
    ('distinct_distinct', 'DISTINCT of DISTINCT',
     f"select distinct g from (select distinct g, v from {T}) d"),
    ('group_noagg', 'GROUP BY on fewer columns without aggregate',
     f"select g from (select g, w, count(*) as c from {T} group by g, w) x group by g"),
    ('three_levels', 'three levels of sums',
     f"select g, coalesce(sum(s2), 0) as s from (select g, w, sum(s1) as s2 from ("
     f"select g, w, v, sum(n) as s1 from {T} group by g, w, v) x group by g, w) y group by g"),
    ('both', 'two stacked aggregations merged into the same aggregation, then shared',
     f"select x.g, x.s, y.m from "
     f"(select g, coalesce(sum(sv), 0) as s from (select g, w, sum(v) as sv from {T} group by g, w) i group by g) x "
     f"join (select g, coalesce(sum(sv), 0) as m from (select g, v, sum(v) as sv from {T} group by g, v) j group by g) y "
     f"on x.g = y.g"),
    # Kept.
    ('kept_count_count', 'count of counts counts inner groups',
     f"select g, count(c) as n, count(*) as m from (select g, w, count(*) as c from {T} group by g, w) x group by g"),
    ('kept_sum_count_scalar', 'sum of counts without outer group key',
     f"select sum(c) as c from (select g, count(*) as c from {T} group by g) x"),
    ('kept_having', 'a HAVING on the inner aggregation',
     f"select g, coalesce(sum(sv), 0) as s from (select g, w, sum(v) as sv from {T} group by g, w having count(*) > 1) x group by g"),
    ('kept_group_on_agg', 'outer group key on an aggregate',
     f"select c, count(*) as n from (select g, count(*) as c from {T} group by g) x group by c"),
    ('kept_count_more_cols', 'count of a column of a DISTINCT on more columns',
     f"select g, count(v) as c from (select distinct g, v, w from {T}) d group by g"),
    ('kept_min_key', 'min of a column of a DISTINCT',
     f"select g, min(v) as m from (select distinct g, v from {T}) d group by g"),
    ('kept_shared_inner', 'an inner aggregation shared by two consumers',
     f"with x as (select g, w, sum(v) as sv from {T} group by g, w) "
     f"select g, coalesce(sum(sv), 0) as s from x group by g union all select g, coalesce(sv, 0)::numeric from x where w = 1"),
    ('kept_union', 'sums of the aggregations of the branches of a union',
     f"select g, coalesce(sum(sv), 0) as s from (select g, sum(v) as sv from {T} group by g "
     f"union all select g, sum(v) as sv from {S} group by g) u group by g"),
    ('kept_semi', 'semi join over a DISTINCT',
     f"select id from {A} where k in (select distinct g from {S})"),
]

out = []
def switch(on):
    value = 'true' if on else 'false'
    stmt(f"set streaming_share_identical_subplans = {value}")
    stmt(f"set streaming_merge_stacked_aggregations = {value}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"ssa_{name}_on", f"ssa_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_batch(name, sql):
    a = f"ssa_{name}_on"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from ({sql}) q) x),\n"
          f"  (select count(*) from (select * from ({sql}) q except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from ({sql}) q)",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the variables, every materialized view holds the same rows.\n")
    for name, _, _ in CASES:
        compare(name)
    out.append(f"# {title}: with the variables, every materialized view holds the rows of its query.\n")
    for name, _, sql in CASES:
        compare_batch(name, sql)

out.append("""# Differential test of `streaming_share_identical_subplans` and
# `streaming_merge_stacked_aggregations`: every query is materialized twice, with and without the
# late sharing of identical sub-plans and the merge of stacked aggregations, and both materialized
# views must hold the same rows, and the rows of the query, after the backfill and after each round
# of changes.
#
# A streaming `sum` returns 0 where the query returns NULL once all the non-NULL values of its
# group are retracted while the group still holds rows: such sums are wrapped in `coalesce(.., 0)`.
#
# Generated by `python3 e2e_test/streaming/share_and_stacked_aggs.gen.py <this file>`: edit the
# list of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (id int primary key, g int, w int, v int, n numeric, big bigint, flag boolean)")
stmt(f"create table {S} (id int primary key, g int, v int)")
stmt(f"create table {A} (id int primary key, k int, x int)")
stmt(f"create table {H} (id int primary key, sid int, st varchar, v int)")
stmt(f"create table {B} (id int primary key, k int, y int)")
stmt(f"create view {V1} as select a.id, a.k, h.v from {A} a join {H} h on a.k = h.sid")
stmt(f"create view {G1} as select sid, count(*) as c, max(v) as m from {H} group by sid")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n"
           "# Duplicated (g, v) pairs, NULL group keys and NULL values.\n")
stmt(f"insert into {T} values "
     f"(1, 1, 1, 10, 1.5, 9000000000000000000, true), (2, 1, 1, 10, 2.5, 9000000000000000000, false), "
     f"(3, 1, 2, 20, null, 5, true), (4, 1, null, null, 0.25, null, null), "
     f"(5, 2, 1, 5, 1, 1, true), (6, 2, 1, 5, 1, 1, true), (7, 2, 3, null, null, null, false), "
     f"(8, null, 1, 7, 3, 3, true), (9, null, null, 7, 3, 3, null), (10, null, 2, null, null, null, false), "
     f"(11, 3, 1, 1, 1, 1, true), (12, 4, null, null, null, null, null)")
stmt(f"insert into {S} values (1, 1, 3), (2, 2, null), (3, null, 1), (4, 5, 5), (5, 5, 5)")
stmt(f"insert into {A} values (1, 1, 0), (2, 2, 1), (3, null, 2), (4, 3, 3), (5, 5, 0), (6, 1, 1)")
stmt(f"insert into {H} values (10, 1, 'x', 1), (11, 1, 'y', 2), (12, 1, 'x', null), (13, 2, null, 2), "
     f"(14, 3, 'x', -5), (15, null, 'x', 1), (16, 9, 'z', 9), (17, 3, 'z', null)")
stmt(f"insert into {B} values (1, 1, 0), (2, 3, 1), (3, null, 2), (4, 9, 3)")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view ssa_{name}_{sfx} as {sql}")
switch(False)

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('IIRII', "select g, s, c, mx, mn from ssa_stacked_on order by g",
      ["1 40 4 20 10", "2 10 3 5 5", "3 1 1 1 1", "4 0 1 NULL NULL", "NULL 14 3 7 7"])
query('III', "select g, c, s from ssa_distinct_aggs_on order by g",
      ["1 2 30", "2 1 5", "3 1 1", "4 0 0", "NULL 1 7"])
query('IRR', "select g, sb, sn from ssa_types_on order by g",
      ["1 18000000000000000005 4.25", "2 2 2", "3 1 1", "4 0 0", "NULL 6 6"])

out.append("# First round of changes: new rows, new groups, duplicates, and rows replaced by primary key.\n")
stmt(f"insert into {T} values (13, 1, 2, 20, 4, 4, false), (14, 5, 1, 2, 2, 2, true), (15, 5, 1, null, null, null, null), "
     f"(16, null, 1, 8, 1, 1, true), (17, 3, 2, 1, 1, 1, false)")
stmt(f"insert into {T} values (5, 2, 1, 6, 1.75, 6, false)")
stmt(f"insert into {S} values (6, 1, 4), (7, 4, 4)")
stmt(f"insert into {A} values (7, 9, 0), (8, 2, 2)")
stmt(f"insert into {H} values (30, 2, 'x', 3), (31, 5, 'y', 0), (32, 5, 'x', 4), (34, null, null, null)")
stmt(f"insert into {H} values (13, 2, 'x', 4)")
stmt(f"insert into {B} values (5, 5, 0), (6, 2, 0)")
compare_all("After the inserts")

out.append("# Second round of changes: rows move between groups, values become NULL, and the last rows\n"
           "# of some groups are deleted.\n")
stmt(f"update {T} set g = 2 where id in (1, 3)")
stmt(f"update {T} set w = 5 where id = 6")
stmt(f"update {T} set v = null, n = null, big = null where id in (2, 11)")
stmt(f"update {T} set flag = null where id in (5, 8)")
stmt(f"delete from {T} where id in (12, 14, 15)")
stmt(f"update {T} set g = null where id = 17")
stmt(f"delete from {S} where id in (4, 5)")
stmt(f"update {S} set g = 2 where id = 1")
stmt(f"update {A} set k = 1 where id = 3")
stmt(f"delete from {A} where id = 5")
stmt(f"delete from {H} where id in (14, 17)")
stmt(f"update {H} set sid = 6 where id = 13")
stmt(f"update {H} set v = -1 where id in (10, 30)")
stmt(f"update {H} set st = 'x' where id = 11")
stmt(f"delete from {B} where id = 2")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the tables are emptied, then filled again.\n")
stmt(f"delete from {T}")
stmt(f"delete from {H}")
stmt(f"delete from {S}")
compare_all("After the tables are emptied")
stmt(f"insert into {T} values (40, 1, 1, 3, 3, 3, true), (41, 1, 1, 3, 3, 3, true), (42, null, null, null, null, null, null), "
     f"(43, 2, 2, 4, 4, 4, false)")
stmt(f"insert into {H} values (40, 1, 'x', 1), (41, 3, 'y', 2), (42, 3, 'x', 3), (43, 6, null, null), (44, null, 'x', 0)")
stmt(f"insert into {S} values (40, 1, 1), (41, null, null)")
compare_all("After the tables are filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view ssa_{name}_{sfx}")
stmt(f"drop view {G1}")
stmt(f"drop view {V1}")
for table in (B, H, A, S, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
