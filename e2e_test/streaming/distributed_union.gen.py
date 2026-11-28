"""Generates `e2e_test/streaming/distributed_union.slt`, the differential test of
`streaming_distributed_union`: every query is materialized with and without the variable (inputs of
a `UNION ALL` shuffled directly by the key that the operator above needs, instead of by the stream
key of the union and then again above it), and both materialized views must hold the same rows,
also equal to the batch query, after the backfill and after each round of changes.

Usage: python3 e2e_test/streaming/distributed_union.gen.py e2e_test/streaming/distributed_union.slt
"""
import sys

A, B, C, D, E = 'sdu_a', 'sdu_b', 'sdu_c', 'sdu_d', 'sdu_e'

AB = f"(select id, v from {A} union all select id, v from {B})"
AB_SRC = f"(select id, v, 1 as src from {A} union all select id, v, 2 as src from {B})"
# Branches whose stream key is a group key: updates of the grouped column change the key of the
# rows of the union.
AGGS = (f"(select k, count(*) as cnt from {C} group by k "
        f"union all select k, count(*) as cnt from {B} group by k)")

# (name, columns, comment, query, settings set for both materialized views of the case)
CASES = [
    ('agg', 'id, cnt, s', 'hash aggregation over a union on its key',
     f"select id, count(*) as cnt, coalesce(sum(v), 0) as s from {AB} u group by id", []),
    ('agg3', 'id, mx, cv', 'hash aggregation over a union of three branches on its key',
     f"select id, max(v) as mx, count(v) as cv from (select id, v from {A} union all select id, v from {B} "
     f"union all select id, x as v from {E}) u group by id", []),
    ('agg_proj', 'id, cnt, s', 'hash aggregation with a projection between it and the union',
     f"select id, count(*) as cnt, coalesce(sum(w), 0) as s from (select id, v + 1 as w from {AB} u) x group by id", []),
    ('union_distinct', 'id', 'union distinct of keys',
     f"select id from {A} union select id from {B}", []),
    ('agg_of_aggs', 'k, cnt', 'hash aggregation over a union of aggregations on their group key',
     f"select k, sum(cnt) as cnt from {AGGS} u group by k", []),
    ('split', 'id, cnt, s, ms', 'branches that filter the same table: updates move rows between branches',
     f"select id, count(*) as cnt, coalesce(sum(v), 0) as s, min(src) as ms from "
     f"(select id, v, 1 as src from {A} where v > 0 union all select id, v, 2 as src from {A} where v <= 0 "
     f"union all select id, v, 3 as src from {B}) u group by id", []),
    ('topn', 'id, v, src', 'group top-n over a union on its key',
     f"select id, v, src from (select id, v, src, row_number() over (partition by id order by v, src) as rn "
     f"from {AB_SRC} u) x where rn <= 1", []),
    ('distinct_on', 'id, v, src', 'distinct on over a union on its key',
     f"select * from (select distinct on (id) id, v, src from {AB_SRC} u order by id, v desc, src) x", []),
    ('over_window', 'id, v, src, sv, rn', 'over window over a union on its key',
     f"select id, v, src, sum(coalesce(v, 0)) over (partition by id order by v, src) as sv, "
     f"row_number() over (partition by id order by src) as rn from {AB_SRC} u", []),
    ('join_left', 'id, v, w', 'hash join with the union on the left',
     f"select u.id, u.v, d.w from {AB} u join {D} d on u.id = d.id", []),
    ('join_right', 'id, w, v', 'hash join with the union on the right',
     f"select d.id, d.w, u.v from {D} d join {AB} u on d.id = u.id", []),
    ('left_join', 'id, x, v', 'left join of a table with a union on a non-key column of the table',
     f"select e.id, e.x, u.v from {E} e left join {AB} u on e.k = u.id", []),
    ('join_unions', 'id, v, v2', 'hash join of two unions on their keys',
     f"select u1.id, u1.v, u2.v as v2 from {AB} u1 join (select id, x as v from {E} "
     f"union all select id, w as v from {D}) u2 on u1.id = u2.id", []),
    ('join_aggs', 'k, cnt, w', 'hash join on the group key of a union of aggregations',
     f"select u.k, u.cnt, d.w from {AGGS} u join {D} d on u.k = d.id", []),
    ('nested', 'id, cnt', 'a union distinct inside a union all, under an aggregation',
     f"select id, count(*) as cnt from (select id from {A} union all (select id from {B} union select id from {E})) u group by id", []),
    ('factored', 'id, v, w', 'kept: union produced by the union-join factoring',
     f"select x.id, x.v, d.w from {A} x join {D} d on x.id = d.id "
     f"union all select y.id, y.v, d.w from {B} y join {D} d on y.id = d.id",
     ['streaming_factor_union_join']),
    # Kept: the default plan.
    ('kept_rowid', 'k, cnt', 'kept: group key outside the stream key (other branch keyed by a row id)',
     f"select k, count(*) as cnt from (select id as k, v from {A} union all select k, v from {C}) u group by k", []),
    ('kept_nonkey', 'k, cnt', 'kept: group key on a non-key column',
     f"select k, count(*) as cnt from (select k, v from {A} union all select k, v from {B}) u group by k", []),
    ('kept_single', 'cnt, s', 'kept: aggregation without group key',
     f"select count(*) as cnt, coalesce(sum(v), 0) as s from {AB} u", []),
    ('kept_root', 'id, v, src', 'kept: the union is the root of the materialized view',
     f"select id, v, src from {AB_SRC} u", []),
]

out = []

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")

def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name, cols, sql):
    # Multisets compared as the count of each distinct row (`EXCEPT` is a set operation).
    a, b = f"sdu_{name}_on", f"sdu_{name}_off"
    grouped = lambda t: f"select {cols}, count(*) as sdu_rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])
    # The materialized view with the variable also equals the batch query.
    q = f"({sql}) q"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(q)}) x),\n"
          f"  (select count(*) from ({grouped(q)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {q})",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the variable, every materialized view holds the same rows.\n")
    for name, cols, _, sql, _ in CASES:
        compare(name, cols, sql)

out.append("""# Differential test of `streaming_distributed_union`: every query is materialized twice, with and
# without the inputs of its union shuffled directly by the key of the operator above, and both
# materialized views must hold the same rows (also equal to the batch query) after the backfill
# and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/distributed_union.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {A} (id int primary key, k int, v int)")
stmt(f"create table {B} (id int primary key, k int, v int)")
stmt(f"create table {C} (k int, v int)")
stmt(f"create table {D} (id int primary key, w int)")
stmt(f"create table {E} (id int primary key, k int, x int)")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {A} values (1, 1, 5), (2, 1, -3), (3, 2, 0), (4, null, null), (5, 3, 7), (6, 2, 2)")
stmt(f"insert into {B} values (1, 2, 4), (3, 1, 1), (7, 3, null), (8, null, -1), (9, 1, 9)")
stmt(f"insert into {C} values (1, 10), (1, 11), (2, null), (null, 3), (4, 4), (4, 4)")
stmt(f"insert into {D} values (1, 100), (2, 200), (3, 300), (4, null), (7, 700), (10, 1000)")
stmt(f"insert into {E} values (1, 1, 11), (2, 3, null), (5, 9, 55), (11, null, 1), (12, 7, 12)")

for sfx in ('off', 'on'):
    stmt(f"set streaming_distributed_union = {'true' if sfx == 'on' else 'false'}")
    for name, _, comment, sql, settings in CASES:
        out.append(f"# {comment}\n")
        for s in settings:
            stmt(f"set {s} = true")
        stmt(f"create materialized view sdu_{name}_{sfx} as {sql}")
        for s in settings:
            stmt(f"set {s} = false")
stmt("set streaming_distributed_union = false")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('III', "select id, cnt, s from sdu_agg_on order by id",
      ["1 2 9", "2 1 -3", "3 2 1", "4 1 0", "5 1 7", "6 1 2", "7 1 0", "8 1 -1", "9 1 9"])
query('III', "select id, v, w from sdu_join_left_on order by id, v",
      ["1 4 100", "1 5 100", "2 -3 200", "3 0 300", "3 1 300", "4 NULL NULL", "7 NULL 700"])
query('II', "select k, cnt from sdu_agg_of_aggs_on order by k",
      ["1 4", "2 2", "3 1", "4 2", "NULL 2"])

out.append("# First round of changes: new rows in every table.\n")
stmt(f"insert into {A} values (7, 3, 8), (10, 4, -2), (11, null, 6)")
stmt(f"insert into {B} values (2, 2, 2), (10, 4, 10), (12, 1, null)")
stmt(f"insert into {C} values (3, 1), (null, null), (1, 12)")
stmt(f"insert into {D} values (11, 1100), (12, 1200), (9, 900)")
stmt(f"insert into {E} values (7, 1, 77), (10, 10, null)")
out.append("# A row written again with an existing primary key replaces the previous one.\n")
stmt(f"insert into {A} values (1, 2, -5)")
stmt(f"insert into {B} values (3, 3, 3)")
compare_all("After the inserts")

out.append("# Second round: updates move rows between branches (sign of v), change grouped columns (the\n"
           "# key of the aggregation branches), and keys change by a delete and an insert.\n")
stmt(f"update {A} set v = -v where id in (1, 2, 5)")
stmt(f"update {A} set v = 0 where id = 6")
stmt(f"update {A} set v = 4, k = 1 where id = 4")
stmt(f"update {A} set v = null where id = 7")
stmt(f"update {B} set k = 3 where id in (1, 9)")
stmt(f"update {B} set k = null, v = 5 where id = 7")
stmt(f"update {C} set k = 2 where k = 1 and v = 10")
stmt(f"update {C} set k = null where k = 4")
stmt(f"update {C} set k = 5 where k is null and v = 3")
stmt(f"update {D} set w = w + 1 where id in (1, 3)")
stmt(f"update {D} set w = null where id = 2")
stmt(f"update {E} set k = 2, x = 22 where id = 1")
stmt(f"update {E} set k = 1 where id = 11")
stmt(f"delete from {A} where id = 3")
stmt(f"insert into {A} values (13, 2, 0)")
stmt(f"delete from {B} where id = 8")
stmt(f"insert into {B} values (13, null, -1)")
stmt(f"delete from {D} where id = 7")
stmt(f"insert into {D} values (13, 1300)")
compare_all("After the updates")

out.append("# Third round: deletes, one table emptied and filled again.\n")
stmt(f"delete from {A} where v < 0 or v is null")
stmt(f"delete from {C} where v is null")
stmt(f"delete from {E} where id = 5")
compare_all("After the deletes")
stmt(f"delete from {B}")
compare_all("After one input is emptied")
stmt(f"insert into {B} values (1, 1, 1), (2, 2, 2), (3, null, null), (20, 3, -3), (13, 2, 13)")
compare_all("After the input is filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _, _, _ in reversed(CASES):
        stmt(f"drop materialized view sdu_{name}_{sfx}")
for table in (E, D, C, B, A):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
