"""Generates `e2e_test/batch/plan_reduction_keys.slt`, the differential test of the simplifications
of the streaming plan reduction applied to batch queries: `batch_simplify_column_facts`,
`batch_simplify_on_unique_key` and `batch_merge_repeated_lookups`. Every query runs twice, with and
without its variable, through `INSERT ... SELECT` (the batch plan), and both results must hold the
same rows with the same multiplicities.

Usage: python3 e2e_test/batch/plan_reduction_keys.gen.py e2e_test/batch/plan_reduction_keys.slt
"""
import sys

F, D, E, P, G, S, H, NN, NOPK = ('bk_f', 'bk_d', 'bk_e', 'bk_p', 'bk_g', 'bk_s', 'bk_h', 'bk_nn',
                                 'bk_nopk')
V1, V2, DA, SD = 'bk_v1', 'bk_v2', 'bk_da', 'bk_sd'

FACTS = 'batch_simplify_column_facts'
KEYS = 'batch_simplify_on_unique_key'
LOOKUPS = 'batch_merge_repeated_lookups'
ALL = (FACTS, KEYS, LOOKUPS)

# (name, variables turned on, columns of the result, query)
CASES = [
    # Column facts.
    ('contra_eq', (FACTS,), 'id int, amt int',
     f"select id, amt from {F} where amt = 1 and amt = 2"),
    ('contra_bounds', (FACTS,), 'id int',
     f"select id from {F} where amt > 5 and amt < 3"),
    ('nn_is_null', (FACTS,), 'id int',
     f"select id from {NN} where k is null"),
    ('nn_tests', (FACTS,), 'id int, kn boolean, k2 int, vn boolean',
     f"select id, k is null as kn, coalesce(k, -1) as k2, v is null as vn from {NN}"),
    ('case_const', (FACTS,), 'id int, c varchar',
     f"select id, case when amt = 1 then 'one' when amt = 2 then 'two' else 'other' end as c "
     f"from {F} where amt = 1"),
    ('agg_const', (FACTS,), 'dim_id int, c bigint',
     f"select dim_id, c from (select dim_id, max(label) as m, count(*) as c from {F} "
     f"where label = 'x' group by dim_id) t where m = 'x'"),
    ('count_empty', (FACTS,), 'c bigint, s bigint',
     f"select count(*) as c, sum(amt) as s from {F} where amt = 1 and amt = 2"),
    ('left_never', (FACTS,), 'id int, name varchar',
     f"select f.id, d.name from {F} f left join {D} d on f.dim_id = d.id and d.grp = 1 and d.grp = 2"),
    ('const_row_join', (FACTS,), 'id int, x int, y varchar',
     f"select f.id, v.x, v.y from {F} f join (values (1, 'a')) v(x, y) on f.amt = v.x"),
    ('bounds', (FACTS,), 'id int, amt int',
     f"select id, amt from {F} where amt > 1 and amt > 3 and amt <= 10 and amt < 20"),
    ('union_const', (FACTS,), 'k varchar, v int',
     f"select k, v from (select 'a' as k, id as v from {F} union all select 'b' as k, id as v from {D}) u "
     f"where k = 'a'"),
    ('or_nulls', (FACTS,), 'id int, label varchar',
     f"select id, label from {F} where (amt = 1 or label is null) and amt is not null and amt < 4"),
    ('kept_anti', (FACTS,), 'id int',
     f"select f.id from {F} f left join {D} d on f.dim_id = d.id where d.id is null"),
    # Unique keys.
    # A set operation without ALL depends on every column, even those its parent does not read:
    # pruned before it becomes an aggregation or a join, it once counted one row over no row.
    ('count_union_distinct', ALL, 'c bigint',
     f"select count(*) as c from (select id, name from {D} where grp is null "
     f"union select id, label from {F} where amt is null) x"),
    ('count_union_distinct_dups', ALL, 'c bigint',
     f"select count(*) as c from (select dim_id, label from {F} union select grp, name from {D}) x"),
    ('count_except', ALL, 'c bigint',
     f"select count(*) as c from (select dim_id, label from {F} except select grp, name from {D}) x"),
    ('one_col_intersect', ALL, 'dim_id int',
     f"select dim_id from (select dim_id, label from {F} intersect select grp, name from {D}) x"),
    ('unused_left', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {F} f left join {D} d on f.dim_id = d.id"),
    ('unused_left_extra', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {F} f left join {D} d on f.dim_id = d.id and d.grp > 1 and f.amt > 0"),
    ('unused_right', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {D} d right join {F} f on f.dim_id = d.id"),
    ('unused_const_key', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {F} f left join {D} d on d.id = 7 and f.amt > d.grp"),
    ('unused_agg', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {F} f left join (select grp, count(*) as cnt from {D} group by grp) g "
     f"on f.dim_id = g.grp"),
    ('unused_distinct_on', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {F} f left join (select distinct on (grp) grp, name from {D} "
     f"order by grp, id) x on f.dim_id = x.grp"),
    ('unused_two', (KEYS,), 'id int, code varchar',
     f"select f.id, e.code from {F} f left join {D} d on f.dim_id = d.id left join {E} e on f.amt = e.id"),
    ('agg_pk', (KEYS,),
     'id int, total bigint, cnt bigint, cnt_dim bigint, first_label varchar, max_amt int, total_price decimal',
     f"select id, sum(amt) as total, count(*) as cnt, count(dim_id) as cnt_dim, min(label) as first_label, "
     f"max(amt) as max_amt, sum(price) as total_price from {F} group by id"),
    ('agg_pk_filter', (KEYS,), 'id int, cnt bigint, total bigint',
     f"select id, count(*) filter (where amt > 5) as cnt, sum(amt) filter (where dim_id is not null) as total "
     f"from {F} group by id"),
    ('avg_pk', (KEYS,), 'id int, average decimal',
     f"select id, avg(amt) as average from {F} group by id"),
    ('string_agg_pk', (KEYS,), 'id int, labels varchar, all_pos boolean',
     f"select id, string_agg(label, ',') as labels, bool_and(amt > 0) as all_pos from {F} group by id"),
    ('distinct_pk', (KEYS,), 'id int, amt int',
     f"select distinct id, amt from {F}"),
    ('distinct_on_pk', (KEYS,), 'id int, amt int',
     f"select distinct on (id) id, amt from {F} order by id, amt"),
    ('agg_left_unique', (KEYS,), 'id int, name varchar, total bigint',
     f"select f.id, d.name, sum(f.amt) as total from {F} f left join {D} d on f.dim_id = d.id "
     f"group by f.id, d.name"),
    ('kept_not_key', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {F} f left join {D} d on f.dim_id = d.grp"),
    ('kept_nopk', (KEYS,), 'id int, amt int',
     f"select f.id, f.amt from {F} f left join {NOPK} n on f.dim_id = n.id"),
    ('kept_group_not_key', (KEYS,), 'dim_id int, total bigint',
     f"select dim_id, sum(amt) as total from {F} group by dim_id"),
    # Repeated lookups.
    ('two_left', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {D} a on f.dim_id = a.id "
     f"left join {D} b on f.dim_id = b.id"),
    ('three_left_views', (LOOKUPS,), 'id int, name varchar, grp int, active boolean',
     f"select v2.id, v2.name, v2.grp, d.active from {V2} v2 left join {D} d on v2.dim_id = d.id"),
    ('left_filters', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f "
     f"left join (select * from {D} where grp = 1) a on f.dim_id = a.id "
     f"left join (select * from {D} where grp = 2) b on f.dim_id = b.id"),
    ('left_then_filtered', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {D} a on f.dim_id = a.id "
     f"left join {DA} b on f.dim_id = b.id"),
    ('filtered_then_left', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {DA} a on f.dim_id = a.id "
     f"left join {D} b on f.dim_id = b.id"),
    ('left_extra', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {D} a on f.dim_id = a.id "
     f"left join {D} b on f.dim_id = b.id and f.amt > b.grp"),
    ('left_computed', (LOOKUPS,), 'id int, name varchar, label2 varchar, one int',
     f"select f.id, a.name, b.label2, b.one from {F} f left join {D} a on f.dim_id = a.id "
     f"left join (select id, coalesce(name, 'none') as label2, 1 as one from {D}) b on f.dim_id = b.id"),
    ('left_composite', (LOOKUPS,), 'id int, v int, w int',
     f"select g.id, x.v, y.w from {G} g left join {P} x on g.a = x.a and g.b = x.b "
     f"left join {P} y on g.a = y.a and g.b = y.b"),
    ('two_inner', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f join {D} a on f.dim_id = a.id join {D} b on f.dim_id = b.id"),
    ('inner_then_filtered', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f join {D} a on f.dim_id = a.id join {DA} b on f.dim_id = b.id"),
    ('inner_then_left', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f join {D} a on f.dim_id = a.id left join {D} b on f.dim_id = b.id"),
    ('left_then_inner', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {D} a on f.dim_id = a.id join {D} b on f.dim_id = b.id"),
    ('self_join', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select a.id, a.name, b.grp from {D} a join {D} b on a.id = b.id"),
    ('self_left_filtered', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select a.id, a.name, b.grp from {D} a left join {DA} b on a.id = b.id"),
    ('between', (LOOKUPS,), 'id int, name varchar, code varchar, grp int',
     f"select f.id, a.name, e.code, b.grp from {F} f join {D} a on f.dim_id = a.id "
     f"join {E} e on f.other_id = e.id join {D} b on f.dim_id = b.id"),
    ('exists_filtered', (LOOKUPS,), 'id int, name varchar',
     f"select a.id, a.name from {D} a where exists (select 1 from {DA} b where b.id = a.id)"),
    ('not_exists_filtered', (LOOKUPS,), 'id int, name varchar',
     f"select a.id, a.name from {D} a where not exists (select 1 from {DA} b where b.id = a.id)"),
    ('not_exists_after_left', (LOOKUPS,), 'id int, name varchar',
     f"select f.id, a.name from {F} f left join {D} a on f.dim_id = a.id "
     f"where not exists (select 1 from {DA} b where b.id = f.dim_id)"),
    ('not_exists_after_inner', (LOOKUPS,), 'id int, name varchar',
     f"select f.id, a.name from {F} f join {D} a on f.dim_id = a.id "
     f"where not exists (select 1 from {D} b where b.id = f.dim_id and b.grp = 2)"),
    ('agg_key', (LOOKUPS,), 'dim_id int, total bigint, grp int',
     f"select t.dim_id, t.total, b.grp from (select f.dim_id, sum(f.amt) as total from {F} f "
     f"join {D} a on f.dim_id = a.id group by f.dim_id) t left join {D} b on t.dim_id = b.id"),
    ('agg_over_left', (LOOKUPS,), 'dim_id int, n varchar, total bigint, grp int',
     f"select t.dim_id, t.n, t.total, b.grp from (select f.dim_id, max(a.name) as n, sum(f.amt) as total "
     f"from {F} f left join {D} a on f.dim_id = a.id group by f.dim_id) t left join {D} b on t.dim_id = b.id"),
    ('distinct_on', (LOOKUPS,), 'other_id int, name varchar, grp int',
     f"select x.other_id, x.name, b.grp from (select distinct on (f.other_id) f.other_id, f.dim_id, a.name "
     f"from {F} f join {D} a on f.dim_id = a.id order by f.other_id, f.id) x left join {D} b on x.dim_id = b.id"),
    ('nested_inner', (LOOKUPS,), 'id int, grp int, ts int, name varchar',
     f"select f.id, d.grp, x.ts, x.name from {F} f join {D} d on f.dim_id = d.id join {SD} x on d.id = x.id"),
    ('left_chain', (LOOKUPS,), 'id int, name varchar, v int, a int',
     f"select f.id, d.name, r.v, r.a from {F} f join {D} d on f.dim_id = d.id left join ("
     f"select d2.id, case when d2.active then 0 else coalesce(g.a, d2.grp) end as v, g.a "
     f"from {D} d2 join {S} s on d2.id = s.id left join {G} g on d2.id = g.id) r on d.id = r.id"),
    ('not_exists_chain', (LOOKUPS,), 'id int',
     f"select f.id from {F} f join {D} d on f.dim_id = d.id where d.grp < 10 and not exists ("
     f"select 1 from {S} s join {D} d2 on s.id = d2.id and d2.active left join {E} e on s.id = e.id "
     f"where s.id = f.dim_id)"),
    ('exists_not_key', (LOOKUPS,), 'id int, x int',
     f"select f.id, h.x from {F} f join {H} h on f.dim_id = h.id "
     f"where exists (select 1 from {H} h2 where h2.id = f.dim_id)"),
    ('not_exists_padded', (LOOKUPS,), 'id int, x int',
     f"select f.id, h.x from {F} f left join {H} h on f.other_id = h.id "
     f"where not exists (select 1 from {H} h2 where h2.id = h.id)"),
    ('kept_two_rows', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {D} a on f.dim_id = a.id "
     f"left join {D} b on f.other_id = b.id"),
    ('kept_extra_first', (LOOKUPS,), 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {D} a on f.dim_id = a.id and f.amt > a.grp "
     f"left join {D} b on f.dim_id = b.id"),
    ('kept_nopk_lookups', (LOOKUPS,), 'id int, name varchar, name2 varchar',
     f"select f.id, a.name, b.name as name2 from {F} f left join {NOPK} a on f.dim_id = a.id "
     f"left join {NOPK} b on f.dim_id = b.id"),
    # All three together.
    ('all_views', ALL, 'id int, grp int',
     f"select v2.id, v2.grp from {V2} v2 left join {E} e on v2.dim_id = e.id where v2.amt > 0 and v2.amt > 2"),
    ('all_agg_views', ALL, 'id int, n varchar, g bigint',
     f"select id, max(name) as n, sum(grp) as g from {V2} group by id"),
    ('all_lookups_facts', ALL, 'id int, name varchar, grp int',
     f"select f.id, a.name, b.grp from {F} f left join {D} a on f.dim_id = a.id "
     f"left join {DA} b on f.dim_id = b.id where f.amt = 1 or f.amt is null"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def switch(variables, on):
    for variable in variables:
        stmt(f"set {variable} = {'true' if on else 'false'}")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    out.append(f"# {title}: every query runs without, then with its variables.\n")
    for name, variables, _, sql in CASES:
        stmt(f"delete from bk_{name}_off")
        stmt(f"delete from bk_{name}_on")
        stmt(f"insert into bk_{name}_off {sql}")
        switch(variables, True)
        stmt(f"insert into bk_{name}_on {sql}")
        switch(variables, False)
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, _, cols, _ in CASES:
        a, b, c = f"bk_{name}_on", f"bk_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as bk_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append("""# Differential test of `batch_simplify_column_facts`, `batch_simplify_on_unique_key` and
# `batch_merge_repeated_lookups`: every query runs through `INSERT ... SELECT` without, then with
# its variables, and both results must hold the same rows with the same multiplicities, after each
# round of changes of the data.
#
# Generated by `python3 e2e_test/batch/plan_reduction_keys.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {F} (id int primary key, dim_id int, other_id int, amt int, label varchar, price decimal)")
stmt(f"create table {D} (id int primary key, name varchar, grp int, active boolean)")
stmt(f"create table {E} (id int primary key, code varchar)")
stmt(f"create table {P} (a int, b int, v int, w int, primary key (a, b))")
stmt(f"create table {G} (id int primary key, a int, b int)")
stmt(f"create table {S} (sid int primary key, id int, ts int)")
stmt(f"create table {H} (rid int primary key, id int, x int)")
stmt(f"create table {NN} (id int primary key, k int not null, v int)")
stmt(f"create table {NOPK} (id int, name varchar)")
stmt(f"create view {V1} as select f.id, f.dim_id, f.amt, d.name from {F} f left join {D} d on f.dim_id = d.id")
stmt(f"create view {V2} as select v1.*, d.grp from {V1} v1 left join {D} d on v1.dim_id = d.id")
stmt(f"create view {DA} as select * from {D} where active")
stmt(f"create view {SD} as select s.sid, s.id, s.ts, d.name, d.active from {S} s join {D} d on s.id = d.id")
for name, _, cols, _ in CASES:
    stmt(f"create table bk_{name}_off ({cols})")
    stmt(f"create table bk_{name}_on ({cols})")

out.append("# First data: NULLs, duplicates of the columns that are not keys, rows without a match.\n")
stmt(f"insert into {F} values (1, 1, 2, 1, 'x', 1.5), (2, 1, 3, 2, 'y', null), (3, 2, null, null, 'x', 2), "
     f"(4, null, 1, 1, null, 0), (5, 9, 2, 5, 'x', 3.25), (6, 3, 3, 1, 'z', 1), (7, 7, 7, 8, 'x', null), "
     f"(8, 2, 1, -1, null, 4)")
stmt(f"insert into {D} values (1, 'one', 1, true), (2, 'two', 2, false), (3, null, 1, null), "
     f"(4, 'four', null, true), (7, 'seven', 2, true), (8, 'eight', 3, false)")
stmt(f"insert into {E} values (1, 'a'), (2, null), (3, 'c'), (8, 'h')")
stmt(f"insert into {P} values (1, 1, 10, 100), (1, 2, 11, 101), (2, 1, null, 102), (3, 3, 13, null)")
stmt(f"insert into {G} values (1, 1, 1), (2, 1, 2), (3, 2, 2), (4, null, 1), (5, 3, 3), (6, 1, 1)")
stmt(f"insert into {S} values (1, 1, 10), (2, 2, 20), (3, 1, 30), (4, null, 40), (5, 9, 50)")
stmt(f"insert into {H} values (1, 1, 5), (2, 1, 6), (3, 2, null), (4, null, 7), (5, 3, 8)")
stmt(f"insert into {NN} values (1, 1, null), (2, 2, 3), (3, 0, 0)")
stmt(f"insert into {NOPK} values (1, 'a'), (1, 'b'), (2, null), (null, 'n')")
run_all("First data")

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {F} values (9, 4, 4, 3, 'x', 2.5), (10, 8, null, 1, 'w', null), (11, 1, 8, 1, 'x', 1), "
     f"(2, 3, 3, 1, 'y', 7)")
stmt(f"insert into {D} values (9, 'nine', 1, true), (3, 'three', 1, true), (2, 'two', null, true)")
stmt(f"insert into {E} values (4, 'd'), (9, 'i')")
stmt(f"insert into {G} values (7, 2, 1), (8, 3, 3)")
stmt(f"insert into {S} values (6, 3, 60), (7, 4, 70)")
stmt(f"insert into {H} values (6, 2, 9), (7, 4, null)")
stmt(f"insert into {NN} values (4, 7, 7)")
stmt(f"insert into {NOPK} values (2, 'c'), (3, 'd')")
run_all("Second data")

out.append("# Third data: deletes and values set to NULL.\n")
stmt(f"delete from {D} where id in (1, 8)")
stmt(f"update {D} set active = null where id = 7")
stmt(f"update {D} set grp = 2 where id = 4")
stmt(f"update {F} set dim_id = null, amt = 1 where id = 5")
stmt(f"update {F} set label = null where id in (1, 3)")
stmt(f"delete from {F} where id = 6")
stmt(f"delete from {P} where a = 1 and b = 2")
stmt(f"delete from {H} where id = 1")
stmt(f"delete from {NOPK} where id = 1")
run_all("Third data")

out.append("# Fourth data: the table that is looked up is emptied.\n")
stmt(f"delete from {D}")
run_all("Empty dimension")

out.append("# Cleanup.\n")
for name, _, _, _ in reversed(CASES):
    stmt(f"drop table bk_{name}_on")
    stmt(f"drop table bk_{name}_off")
for view in (SD, DA, V2, V1):
    stmt(f"drop view {view}")
for table in (NOPK, NN, H, S, G, P, E, D, F):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
