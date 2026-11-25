"""Generates `e2e_test/streaming/merge_repeated_lookups.slt`, the differential test of the merge
of repeated lookups in streaming plans: every query is materialized with and without
`streaming_merge_repeated_lookups`.

Usage: python3 e2e_test/streaming/merge_repeated_lookups.gen.py e2e_test/streaming/merge_repeated_lookups.slt
"""
import sys

F, D, E, P, G = 'mrl_f', 'mrl_d', 'mrl_e', 'mrl_p', 'mrl_g'
DA = 'mrl_da'  # view: the active rows of D
DM = 'mrl_dm'  # materialized view whose key can be NULL
V1, V2, W1, W2 = 'mrl_v1', 'mrl_v2', 'mrl_w1', 'mrl_w2'

CASES = [
    # Left outer lookups.
    ('two_left', 'two left lookups of the same row',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id left join {D} b on f.dim_id = b.id"),
    ('three_left_views', 'three left lookups through a stack of views',
     f"select v.id as fid, v.name as an, v.grp as bg, d.active as da from {V2} v left join {D} d on v.dim_id = d.id"),
    ('left_from_key', 'left lookup from the key that the first lookup returned',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id left join {D} b on a.id = b.id"),
    ('left_filter_after', 'left lookup with a filter after an unfiltered one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id left join {DA} b on f.dim_id = b.id"),
    ('left_unfiltered_after', 'unfiltered left lookup after a filtered one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {DA} a on f.dim_id = a.id left join {D} b on f.dim_id = b.id"),
    ('left_two_filters', 'two left lookups with different filters',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f "
     f"left join (select * from {D} where grp = 1) a on f.dim_id = a.id "
     f"left join (select * from {D} where grp = 2) b on f.dim_id = b.id"),
    ('left_four', 'four left lookups with and without filters',
     f"select f.id as fid, a.name as an, b.name as bn, c.grp as cg, e.active as ea from {F} f "
     f"left join (select * from {D} where grp = 1) a on f.dim_id = a.id "
     f"left join (select * from {D} where grp = 2) b on f.dim_id = b.id "
     f"left join {D} c on f.dim_id = c.id "
     f"left join {DA} e on f.dim_id = e.id"),
    ('left_same_filter', 'two left lookups with the same filter',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {DA} a on f.dim_id = a.id left join {DA} b on f.dim_id = b.id"),
    ('left_null_filter', 'left lookups with filters that can be NULL',
     f"select f.id as fid, a.name as an, b.grp as bg, c.name as cn from {F} f "
     f"left join (select * from {D} where grp > 1) a on f.dim_id = a.id "
     f"left join (select * from {D} where not active) b on f.dim_id = b.id "
     f"left join (select * from {D} where name is null) c on f.dim_id = c.id"),
    ('left_extra_cond', 'left lookup with an extra condition',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id left join {D} b on f.dim_id = b.id and f.amt > b.grp"),
    ('left_computed', 'left lookup of computed columns',
     f"select f.id as fid, a.name as an, b.label2 as bl, b.one as bo from {F} f left join {D} a on f.dim_id = a.id "
     f"left join (select id, coalesce(name, 'none') as label2, 1 as one from {D}) b on f.dim_id = b.id"),
    ('left_computed_first', 'left lookup after a lookup of computed columns',
     f"select f.id as fid, a.label2 as al, a.one as ao, b.grp as bg from {F} f "
     f"left join (select id, coalesce(name, 'none') as label2, 1 as one from {D} where active) a on f.dim_id = a.id "
     f"left join {D} b on f.dim_id = b.id"),
    ('left_composite', 'left lookups on a composite key',
     f"select g.id as gid, x.v as xv, y.w as yw from {G} g left join {P} x on g.a = x.a and g.b = x.b left join {P} y on g.a = y.a and g.b = y.b"),
    # Inner lookups.
    ('two_inner', 'two inner lookups of the same row',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f join {D} a on f.dim_id = a.id join {D} b on f.dim_id = b.id"),
    ('inner_views', 'two inner lookups through a stack of views', f"select * from {W2}"),
    ('inner_chain', 'inner lookup from the key that the first lookup returned',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f join {D} a on f.dim_id = a.id join {D} b on a.id = b.id"),
    ('inner_filter', 'inner lookup with a filter after an unfiltered one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f join {D} a on f.dim_id = a.id join {DA} b on f.dim_id = b.id"),
    ('inner_extra_cond', 'inner lookup with an extra condition',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f join {D} a on f.dim_id = a.id join {D} b on f.dim_id = b.id and b.grp < f.amt"),
    ('left_after_inner', 'left lookup after an inner one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f join {D} a on f.dim_id = a.id left join {D} b on f.dim_id = b.id"),
    ('filtered_left_after_inner', 'filtered left lookup after an inner one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f join {D} a on f.dim_id = a.id left join {DA} b on f.dim_id = b.id"),
    ('inner_after_left', 'inner lookup after a left one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id join {D} b on f.dim_id = b.id"),
    ('inner_after_filtered_left', 'inner lookup after a filtered left one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {DA} a on f.dim_id = a.id join {D} b on f.dim_id = b.id"),
    ('filtered_inner_after_left', 'filtered inner lookup after a left one',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id join {DA} b on f.dim_id = b.id"),
    ('self_join', 'self join of a table on its key',
     f"select a.id as aid, a.name as an, b.grp as bg from {D} a join {D} b on a.id = b.id"),
    ('self_left_filter', 'self left join of a table on its key with a filter',
     f"select a.id as aid, a.name as an, b.grp as bg from {D} a left join {DA} b on a.id = b.id"),
    ('in_between', 'lookup through another table in between',
     f"select f.id as fid, a.name as an, e.code as ec, b.grp as bg from {F} f "
     f"join {D} a on f.dim_id = a.id join {E} e on f.other_id = e.id join {D} b on f.dim_id = b.id"),
    ('in_between_left', 'left lookups of two tables, each twice',
     f"select f.id as fid, a.name as an, e.code as ec, b.grp as bg, e2.code as ec2 from {F} f "
     f"left join {D} a on f.dim_id = a.id left join {E} e on f.other_id = e.id "
     f"left join {D} b on f.dim_id = b.id left join {E} e2 on f.other_id = e2.id"),
    ('inner_composite', 'inner lookups on a composite key',
     f"select g.id as gid, x.v as xv, y.w as yw from {G} g join {P} x on g.a = x.a and g.b = x.b join {P} y on g.a = y.a and g.b = y.b"),
    ('mixed_composite', 'lookup on a composite key from the columns of both sides of the first one',
     f"select g.id as gid, x.v as xv, y.w as yw from {G} g join {P} x on g.a = x.a and g.b = x.b left join {P} y on x.a = y.a and g.b = y.b"),
    ('mixed_composite_left', 'left lookup on a composite key from the columns of both sides of a left one',
     f"select g.id as gid, x.v as xv, y.w as yw from {G} g left join {P} x on g.a = x.a and g.b = x.b left join {P} y on x.a = y.a and g.b = y.b"),
    # The kept side is more than a chain of lookups.
    ('nullable_side', 'lookups on the optional side of an outer join',
     f"select e.id as eid, y.fid as fid, y.an as an, y.bg as bg from {E} e left join "
     f"(select f.id as fid, f.other_id as oid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id left join {D} b on f.dim_id = b.id) y "
     f"on e.id = y.oid"),
    ('nullable_inner', 'inner lookups on the optional side of an outer join',
     f"select f.id as fid, x.an as an, x.bg as bg from {F} f left join "
     f"(select a.id as id, a.name as an, b.grp as bg from {D} a join {DA} b on a.id = b.id) x on f.dim_id = x.id"),
    ('lookup_of_nullable', 'lookup from the key of the optional side of an outer join',
     f"select e.id as eid, y.fid as fid, y.an as an, b.grp as bg from {E} e left join "
     f"(select f.id as fid, f.other_id as oid, f.dim_id as did, a.id as aid, a.name as an from {F} f join {D} a on f.dim_id = a.id) y "
     f"on e.id = y.oid left join {D} b on y.aid = b.id"),
    ('lookup_of_nullable_fk', 'lookup from the foreign key of the optional side of an outer join',
     f"select e.id as eid, y.fid as fid, y.an as an, b.grp as bg from {E} e left join "
     f"(select f.id as fid, f.other_id as oid, f.dim_id as did, a.name as an from {F} f left join {D} a on f.dim_id = a.id) y "
     f"on e.id = y.oid left join {DA} b on y.did = b.id"),
    ('filter_between', 'filter between two left lookups',
     f"select x.id as fid, x.an as an, b.grp as bg from "
     f"(select f.id, f.dim_id, a.name as an from {F} f left join {D} a on f.dim_id = a.id where a.name is not null or f.amt > 3) x "
     f"left join {D} b on x.dim_id = b.id"),
    ('semi_between', 'semi join between two lookups',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f join {D} a on f.dim_id = a.id join {D} b on f.dim_id = b.id "
     f"where f.other_id in (select id from {E})"),
    ('anti_between', 'anti join between two left lookups',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id left join {DA} b on f.dim_id = b.id "
     f"where not exists (select 1 from {E} e where e.id = f.other_id)"),
    ('agg_above', 'aggregation above two left lookups',
     f"select b.grp as bg, count(*) as cnt, sum(f.amt) as total, max(a.name) as an from {F} f "
     f"left join {D} a on f.dim_id = a.id left join {D} b on f.dim_id = b.id group by b.grp"),
    ('lookup_left_side', 'lookup written on the left side of the join',
     f"select a.grp as ag, x.fid as fid, x.bn as bn from {D} a join "
     f"(select f.id as fid, f.dim_id as did, b.name as bn from {F} f join {D} b on f.dim_id = b.id) x on a.id = x.did"),
    # A key that can be NULL.
    ('null_key_left', 'left lookups in a relation whose key can be NULL',
     f"select f.id as fid, a.cnt as ac, b.top as bt from {F} f left join {DM} a on f.amt = a.id left join {DM} b on f.amt = b.id"),
    ('null_key_self', 'self join on a key that can be NULL',
     f"select a.id as aid, a.cnt as ac, b.top as bt from {DM} a join {DM} b on a.id = b.id"),
    ('null_key_self_left', 'self left join on a key that can be NULL',
     f"select a.id as aid, a.cnt as ac, b.top as bt from {DM} a left join {DM} b on a.id = b.id"),
    ('null_key_chain', 'left lookup from a key that can be NULL',
     f"select d.id as did, a.cnt as ac, b.top as bt from {D} d left join {DM} a on d.grp = a.id left join {DM} b on a.id = b.id"),
    # Lookups that are kept.
    ('kept_two_rows', 'kept: lookups of two different rows',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id left join {D} b on f.other_id = b.id"),
    ('kept_not_key', 'kept: lookup on a column that is not the key',
     f"select f.id as fid, a.name as an, b.active as ba from {F} f left join {D} a on f.dim_id = a.grp left join {D} b on f.dim_id = b.grp"),
    ('kept_partial', 'kept: lookup on a part of a composite key',
     f"select g.id as gid, x.v as xv, y.w as yw from {G} g left join {P} x on g.a = x.a and g.b = x.b left join {P} y on g.a = y.a"),
    ('kept_crossed', 'kept: lookup on a composite key from crossed columns',
     f"select g.id as gid, x.v as xv, y.w as yw from {G} g left join {P} x on g.a = x.a and g.b = x.b left join {P} y on g.b = y.a and g.a = y.b"),
    ('kept_first_cond', 'kept: the first left lookup has an extra condition',
     f"select f.id as fid, a.name as an, b.grp as bg from {F} f left join {D} a on f.dim_id = a.id and f.amt > a.grp left join {D} b on f.dim_id = b.id"),
    ('kept_const_key', 'kept: a column of the composite key is compared with a constant',
     f"select g.id as gid, x.v as xv, y.w as yw from {G} g left join {P} x on g.a = x.a and g.b = x.b left join {P} y on g.a = y.a and y.b = 1"),
]

out = []
def switch(on):
    stmt(f"set streaming_merge_repeated_lookups = {'true' if on else 'false'}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"mrl_{name}_on", f"mrl_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the merge, every materialized view holds the same rows.\n")
    for name, _, _ in CASES:
        compare(name)

out.append("""# Differential test of `streaming_merge_repeated_lookups`: every query is materialized twice,
# with and without the merge of its repeated lookups, and both materialized views must hold the
# same rows after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/merge_repeated_lookups.gen.py <this file>`: edit the
# list of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {F} (id int primary key, dim_id int, other_id int, amt int, label varchar)")
stmt(f"create table {D} (id int primary key, name varchar, grp int, active boolean)")
stmt(f"create table {E} (id int primary key, code varchar)")
stmt(f"create table {P} (a int, b int, v int, w int, primary key (a, b))")
stmt(f"create table {G} (id int primary key, a int, b int)")
stmt(f"create view {DA} as select * from {D} where active")
stmt(f"create materialized view {DM} as select grp as id, count(*) as cnt, max(name) as top from {D} group by grp")
stmt(f"create view {V1} as select f.id, f.dim_id, f.amt, d.name from {F} f left join {D} d on f.dim_id = d.id")
stmt(f"create view {V2} as select v.*, d.grp from {V1} v left join {D} d on v.dim_id = d.id")
stmt(f"create view {W1} as select f.id, f.dim_id, f.amt, d.name, d.id as did from {F} f join {D} d on f.dim_id = d.id")
stmt(f"create view {W2} as select w.*, d.grp from {W1} w join {D} d on w.did = d.id")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {D} values (1, 'one', 1, true), (2, 'two', 1, false), (3, 'three', 2, true), "
     f"(4, null, null, null), (5, 'five', 2, null), (6, 'six', null, true), (7, null, 3, false)")
stmt(f"insert into {E} values (1, 'a'), (3, 'c'), (5, 'e'), (8, null)")
stmt(f"insert into {F} values (10, 1, 1, 5, 'x'), (11, 1, 3, 0, 'y'), (12, 3, null, 3, null), (13, null, 5, 2, 'z'), "
     f"(14, 99, 8, null, 'w'), (15, 2, 2, 1, 'x'), (16, 4, 1, 2, 'v'), (17, 5, 9, 1, 'u'), (18, 6, 3, 3, 't'), (19, 7, null, 4, 's')")
stmt(f"insert into {P} values (1, 1, 10, 100), (1, 2, 20, 200), (2, 1, 30, null), (3, 3, null, 400)")
stmt(f"insert into {G} values (1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2), (5, null, 1), (6, 3, null), (7, 1, 1)")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view mrl_{name}_{sfx} as {sql}")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('ITI', "select fid, an, bg from mrl_two_left_on order by fid",
      ["10 one 1", "11 one 1", "12 three 2", "13 NULL NULL", "14 NULL NULL", "15 two 1", "16 NULL NULL",
       "17 five 2", "18 six NULL", "19 NULL 3"])
query('ITI', "select fid, an, bg from mrl_left_two_filters_on order by fid",
      ["10 one NULL", "11 one NULL", "12 NULL 2", "13 NULL NULL", "14 NULL NULL", "15 two NULL", "16 NULL NULL",
       "17 NULL 2", "18 NULL NULL", "19 NULL NULL"])
query('ITI', "select fid, an, bg from mrl_inner_filter_on order by fid",
      ["10 one 1", "11 one 1", "12 three 2", "18 six NULL"])
query('IIT', "select aid, ac, bt is null from mrl_null_key_self_on order by aid", ["1 2 f", "2 2 f", "3 1 t"])

out.append("# First round of changes: new rows, lookups that start to match.\n")
stmt(f"insert into {D} values (99, 'far', 1, true), (8, 'eight', 2, false), (9, 'nine', 4, true)")
stmt(f"insert into {E} values (2, 'b'), (9, 'i')")
stmt(f"insert into {F} values (20, 8, 2, 7, 'r'), (21, 9, 9, 0, null), (22, 1, 5, 9, 'q'), (23, 100, 100, 1, 'p')")
stmt(f"insert into {P} values (2, 2, 40, 500), (3, 1, 50, 600)")
stmt(f"insert into {G} values (8, 3, 1), (9, 3, 3), (10, 4, 4)")
out.append("# A row written again with an existing primary key replaces the previous one.\n")
stmt(f"insert into {D} values (3, 'trois', 1, false)")
stmt(f"insert into {F} values (10, 2, 3, 6, 'x2')")
compare_all("After the inserts")

out.append("# Second round of changes: updates and deletes, which retract rows and move them between filters.\n")
stmt(f"update {D} set active = not active where id in (1, 2, 8)")
stmt(f"update {D} set active = true where id = 4")
stmt(f"update {D} set grp = 2 where id = 1")
stmt(f"update {D} set grp = 1, name = 'cinq' where id = 5")
stmt(f"update {D} set grp = null where id = 9")
stmt(f"delete from {D} where id = 6")
stmt(f"delete from {D} where id = 99")
stmt(f"update {F} set dim_id = 3, amt = amt + 2 where id = 11")
stmt(f"update {F} set dim_id = null where id = 15")
stmt(f"update {F} set dim_id = 1 where id = 13")
stmt(f"update {F} set other_id = 2 where id = 12")
stmt(f"delete from {F} where id = 16")
stmt(f"update {E} set code = 'cc' where id = 3")
stmt(f"delete from {E} where id = 1")
stmt(f"update {P} set v = v + 1, w = null where a = 1")
stmt(f"delete from {P} where a = 2 and b = 1")
stmt(f"update {G} set b = 2 where id = 1")
stmt(f"update {G} set a = 3 where id = 5")
stmt(f"delete from {G} where id = 4")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the dimension is emptied and filled again.\n")
stmt(f"delete from {D} where id > 2")
compare_all("After the deletion of most of the dimension")
stmt(f"insert into {D} values (3, 'three', 2, true), (4, 'four', 1, null), (5, 'five', 2, false), (100, 'hundred', 2, true)")
stmt(f"delete from {D} where id = 1")
compare_all("After the dimension is filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view mrl_{name}_{sfx}")
for view in (W2, W1, V2, V1):
    stmt(f"drop view {view}")
stmt(f"drop materialized view {DM}")
stmt(f"drop view {DA}")
for table in (G, P, E, D, F):
    stmt(f"drop table {table}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
