"""Generates `e2e_test/streaming/factor_union_join.slt`, the differential test of the join factored
out of the branches of a union in streaming plans: every query is materialized with and without
`streaming_factor_union_join`.

Usage: python3 e2e_test/streaming/factor_union_join.gen.py e2e_test/streaming/factor_union_join.slt
"""
import sys

F, D, E, G = 'fuj_f', 'fuj_d', 'fuj_e', 'fuj_g'
DA = 'fuj_da'  # view: the active rows of D
# Derived relations joined in several branches: a join with duplicates, an aggregation (with a
# NULL group), a left join.
DJ, DAGG, DL = 'fuj_dj', 'fuj_dagg', 'fuj_dl'
LABELS = f"(select id, coalesce(name, 'none') as label from {D})"

CASES = [
    ('inner', 'two branches with the same inner join',
     f"select f.id as id, d.name as dn from {F} f join {D} d on f.dim_id = d.id "
     f"union all select e.id, d.name from {E} e join {D} d on e.ref_id = d.id"),
    ('left_proj', 'left joins with different projections above',
     f"select f.id as id, f.label as txt, least(f.amt, d.grp) as n from {F} f left join {D} d on f.dim_id = d.id "
     f"union all select e.id, e.code, d.grp from {E} e left join {D} d on e.ref_id = d.id"),
    ('inner_filters', 'inner joins with a different filter of the joined table in each branch',
     f"select f.id as id, d.name as dn from {F} f join {D} d on f.dim_id = d.id where d.active "
     f"union all select e.id, d.name from {E} e join {D} d on e.ref_id = d.id where d.grp = 2"),
    ('inner_null_filters', 'inner joins with filters that can be NULL',
     f"select f.id as id, d.name as dn from {F} f join {D} d on f.dim_id = d.id where not d.active "
     f"union all select e.id, d.name from {E} e join {D} d on e.ref_id = d.id where d.grp > 1"),
    ('left_one_filter', 'left joins with a filtered joined table in one branch only',
     f"select f.id as id, d.name as dn from {F} f left join {DA} d on f.dim_id = d.id "
     f"union all select e.id, d.name from {E} e left join {D} d on e.ref_id = d.id"),
    ('left_two_filters', 'left joins with a different filter of the joined table in each branch',
     f"select f.id as id, d.name as dn, d.grp as dg from {F} f left join {DA} d on f.dim_id = d.id "
     f"union all select e.id, d.name, d.grp from {E} e left join (select * from {D} where grp = 2) d on e.ref_id = d.id"),
    ('left_conditions', 'left joins with a different extra condition in each branch',
     f"select f.id as id, d.name as dn from {F} f left join {D} d on f.dim_id = d.id and f.amt > d.grp "
     f"union all select e.id, d.name from {E} e left join {D} d on e.ref_id = d.id and d.name <> e.code"),
    ('filters_above', 'filters above the joins',
     f"select f.id as id, d.name as dn from {F} f left join {D} d on f.dim_id = d.id where f.amt > 3 or d.name is null "
     f"union all select e.id, d.name from {E} e left join {D} d on e.ref_id = d.id where e.code is not null"),
    ('computed_below', 'expression of the joined table computed below a left join',
     f"select f.id as id, x.label as label from {F} f left join {LABELS} x on f.dim_id = x.id "
     f"union all select e.id, x.label from {E} e left join {LABELS} x on e.ref_id = x.id"),
    ('computed_above', 'expression of the joined table computed above a left join',
     f"select f.id as id, coalesce(d.name, 'none') as label from {F} f left join {D} d on f.dim_id = d.id "
     f"union all select e.id, d.name || '!' from {E} e left join {D} d on e.ref_id = d.id"),
    ('semi', 'semi joins',
     f"select f.id as id, f.label as txt from {F} f where exists (select 1 from {D} d where d.id = f.dim_id) "
     f"union all select e.id, e.code from {E} e where exists (select 1 from {D} d where d.id = e.ref_id)"),
    ('anti', 'anti joins with a different filter of the joined table',
     f"select f.id as id, f.label as txt from {F} f where not exists (select 1 from {D} d where d.id = f.dim_id and d.active) "
     f"union all select e.id, e.code from {E} e where not exists (select 1 from {D} d where d.id = e.ref_id)"),
    ('anti_condition', 'anti joins with an extra condition in one branch',
     f"select f.id as id, f.label as txt from {F} f where not exists (select 1 from {D} d where d.id = f.dim_id and d.grp < f.amt) "
     f"union all select e.id, e.code from {E} e where not exists (select 1 from {D} d where d.id = e.ref_id and d.grp = 2)"),
    ('not_key', 'join on a column of the joined table that is not its key',
     f"select f.id as id, d.name as dn from {F} f join {D} d on f.amt = d.grp "
     f"union all select e.id, d.name from {E} e join {D} d on e.ref_id = d.grp"),
    ('not_key_left', 'left join on a column of the joined table that is not its key',
     f"select f.id as id, d.name as dn, d.id as did from {F} f left join {D} d on f.amt = d.grp and d.active "
     f"union all select e.id, d.name, d.id from {E} e left join {D} d on e.ref_id = d.grp"),
    ('three_two', 'three branches, two of which share their join',
     f"select f.id as id, d.name as txt from {F} f join {D} d on f.dim_id = d.id "
     f"union all select g.id, e.code from {G} g join {E} e on g.a = e.id "
     f"union all select e.id, d.name from {E} e join {D} d on e.ref_id = d.id"),
    ('three', 'three branches with the same join',
     f"select f.id as id, d.name as dn, 'f' as src from {F} f join {D} d on f.dim_id = d.id "
     f"union all select e.id, d.name, 'e' from {E} e join {D} d on e.ref_id = d.id "
     f"union all select g.id, d.name, 'g' from {G} g join {D} d on g.a = d.id"),
    ('two_columns', 'joins on two columns',
     f"select f.id as id, d.active as da from {F} f join {D} d on f.dim_id = d.id and f.amt = d.grp "
     f"union all select g.id, d.active from {G} g join {D} d on g.b = d.grp and g.a = d.id"),
    ('left_side', 'the joined table is written on the left side',
     f"select f.id as id, d.name as dn from {D} d join {F} f on f.dim_id = d.id "
     f"union all select e.id, d.name from {D} d join {E} e on e.ref_id = d.id"),
    ('same_input', 'both branches read the same input with different filters',
     f"select f.id as id, d.name as dn, 'big' as kind from {F} f join {D} d on f.dim_id = d.id where f.amt > 3 "
     f"union all select f.id, d.name, 'other' from {F} f left join {D} d on f.other_id = d.id where f.amt <= 3 or f.amt is null"),
    ('same_input_inner', 'both branches join the same input on different columns',
     f"select f.id as id, d.name as dn, 1 as via from {F} f join {D} d on f.dim_id = d.id "
     f"union all select f.id, d.name, 2 from {F} f join {D} d on f.other_id = d.id"),
    ('agg_above', 'aggregation above the union',
     f"select dn, count(*) as cnt, sum(amt) as total from (select d.name as dn, f.amt as amt from {F} f join {D} d on f.dim_id = d.id "
     f"union all select d.name, e.ref_id from {E} e join {D} d on e.ref_id = d.id) u group by dn"),
    ('union_distinct', 'union without all',
     f"select d.name as dn, d.grp as dg from {F} f join {D} d on f.dim_id = d.id "
     f"union select d.name, d.grp from {E} e join {D} d on e.ref_id = d.id"),
    # The joined side is a derived relation, the same in every branch.
    ('dv_inner', 'a view with a join, inner joins',
     f"select f.id as id, v.name as dn, v.a as va from {F} f join {DJ} v on f.dim_id = v.id "
     f"union all select e.id, v.name, v.a from {E} e join {DJ} v on e.ref_id = v.id"),
    ('dv_agg_left', 'an aggregating view, left joins',
     f"select f.id as id, v.cnt as cnt, v.mx as mx from {F} f left join {DAGG} v on f.amt = v.grp "
     f"union all select e.id, v.cnt, e.code from {E} e left join {DAGG} v on e.ref_id = v.grp"),
    ('dv_anti', 'anti joins with a view with a join',
     f"select f.id as id, f.label as txt from {F} f where not exists (select 1 from {DJ} v where v.id = f.dim_id) "
     f"union all select e.id, e.code from {E} e where not exists (select 1 from {DJ} v where v.id = e.ref_id)"),
    ('dv_semi_filters', 'semi joins with a different filter of the aggregating view',
     f"select f.id as id, f.label as txt from {F} f where exists (select 1 from {DAGG} v where v.grp = f.amt and v.cnt > 1) "
     f"union all select e.id, e.code from {E} e where exists (select 1 from {DAGG} v where v.grp = e.ref_id and v.mx is not null)"),
    ('dv_filters', 'inner joins with a different filter of the aggregating view',
     f"select f.id as id, v.cnt as cnt from {F} f join {DAGG} v on f.amt = v.grp where v.cnt > 1 "
     f"union all select e.id, v.cnt from {E} e join {DAGG} v on e.ref_id = v.grp where v.mx is not null"),
    ('dv_condition', 'an extra condition on the view in one branch',
     f"select f.id as id, v.name as dn from {F} f left join {DJ} v on f.dim_id = v.id and v.a > f.amt "
     f"union all select e.id, v.name from {E} e left join {DJ} v on e.ref_id = v.id"),
    ('dv_left_view', 'a view with a left join, left joins',
     f"select f.id as id, v.name as dn, v.a as va from {F} f left join {DL} v on f.dim_id = v.id "
     f"union all select e.id, v.name, v.a from {E} e left join {DL} v on e.ref_id = v.id"),
    ('dv_inline', 'the same aggregating subquery written in each branch',
     f"select f.id as id, v.cnt as cnt from {F} f join (select grp, count(*) as cnt from {D} group by grp) v on f.amt = v.grp "
     f"union all select e.id, v.cnt from {E} e join (select grp, count(*) as cnt from {D} group by grp) v on e.ref_id = v.grp"),
    # Several joins, and semi or anti joins above the join.
    ('chain', 'two joins common to the branches',
     f"select f.id as id, g.a as ga, d.name as dn from {F} f join {D} d on f.dim_id = d.id join {G} g on d.grp = g.b "
     f"union all select e.id, g.a, d.name from {E} e join {D} d on e.ref_id = d.id join {G} g on d.grp = g.b"),
    ('two_anti', 'two anti joins in each branch',
     f"select f.id as id, f.label as txt from {F} f where not exists (select 1 from {G} g where g.a = f.other_id and g.b > 1) "
     f"and not exists (select 1 from {D} d where d.id = f.other_id) "
     f"union all select e.id, e.code from {E} e where not exists (select 1 from {G} g where g.a = e.ref_id and g.b > 1) "
     f"and not exists (select 1 from {D} d where d.id = e.ref_id)"),
    ('anti_above_left', 'anti join on the other input above a left join in one branch',
     f"select f.id as id, d.name as dn from {F} f left join {D} d on f.dim_id = d.id "
     f"where not exists (select 1 from {G} g where g.a = f.other_id) "
     f"union all select e.id, d.name from {E} e left join {D} d on e.ref_id = d.id"),
    ('semi_above_inner', 'semi join on the joined table above inner joins',
     f"select f.id as id, d.name as dn from {F} f join {D} d on f.dim_id = d.id where exists (select 1 from {G} g where g.b = d.grp) "
     f"union all select e.id, d.name from {E} e join {D} d on e.ref_id = d.id where exists (select 1 from {G} g where g.b = d.grp)"),
    # Unions that are kept.
    ('kept_types', 'kept: an inner join and a left join',
     f"select f.id as id, d.name as dn from {F} f join {D} d on f.dim_id = d.id "
     f"union all select e.id, d.name from {E} e left join {D} d on e.ref_id = d.id"),
    ('kept_columns', 'kept: joins on different columns of the joined table',
     f"select f.id as id, d.name as dn from {F} f join {D} d on f.dim_id = d.id "
     f"union all select e.id, d.name from {E} e join {D} d on e.ref_id = d.grp"),
    ('kept_anti_padded', 'kept: anti join on the padded side of a left join',
     f"select f.id as id, d.name as dn from {F} f left join {D} d on f.dim_id = d.id "
     f"where not exists (select 1 from {G} g where g.a = d.grp) "
     f"union all select e.id, d.name from {E} e left join {D} d on e.ref_id = d.id"),
]

out = []
def switch(on):
    stmt(f"set streaming_factor_union_join = {'true' if on else 'false'}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"fuj_{name}_on", f"fuj_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the single join, every materialized view holds the same rows.\n")
    for name, _, _ in CASES:
        compare(name)

out.append("""# Differential test of `streaming_factor_union_join`: every query is materialized twice, with and
# without the join factored out of the branches of its union, and both materialized views must
# hold the same rows after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/factor_union_join.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {F} (id int primary key, dim_id int, other_id int, amt int, label varchar)")
stmt(f"create table {D} (id int primary key, name varchar, grp int, active boolean)")
stmt(f"create table {E} (id int primary key, code varchar, ref_id int)")
stmt(f"create table {G} (id int primary key, a int, b int)")
stmt(f"create view {DA} as select * from {D} where active")
stmt(f"create view {DJ} as select d.id, d.name, d.grp, g.a from {D} d join {G} g on d.grp = g.b")
stmt(f"create view {DAGG} as select grp, count(*) as cnt, max(name) as mx from {D} group by grp")
stmt(f"create view {DL} as select d.id, d.name, g.a from {D} d left join {G} g on d.grp = g.b")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {D} values (1, 'one', 1, true), (2, 'two', 1, false), (3, 'three', 2, true), "
     f"(4, null, null, null), (5, 'five', 2, null), (6, 'six', null, true), (7, null, 3, false)")
stmt(f"insert into {E} values (101, 'a', 1), (103, 'three', 3), (105, 'e', 5), (108, null, 8), (109, 'two', 2), (110, 'x', null)")
stmt(f"insert into {F} values (10, 1, 1, 5, 'x'), (11, 1, 3, 0, 'y'), (12, 3, null, 3, null), (13, null, 5, 2, 'z'), "
     f"(14, 99, 8, null, 'w'), (15, 2, 2, 1, 'x'), (16, 4, 1, 2, 'v'), (17, 5, 9, 1, 'u'), (18, 6, 3, 3, 't'), (19, 7, null, 4, 's')")
stmt(f"insert into {G} values (201, 1, 1), (202, 1, 2), (203, 2, 1), (204, 3, 2), (205, null, 1), (206, 5, null), (207, 101, 3)")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view fuj_{name}_{sfx} as {sql}")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('IT', "select id, dn from fuj_inner_on order by id",
      ["10 one", "11 one", "12 three", "15 two", "16 NULL", "17 five", "18 six", "19 NULL", "101 one", "103 three", "105 five", "109 two"])
query('IT', "select id, dn from fuj_left_one_filter_on order by id",
      ["10 one", "11 one", "12 three", "13 NULL", "14 NULL", "15 NULL", "16 NULL", "17 NULL", "18 six", "19 NULL",
       "101 one", "103 three", "105 five", "108 NULL", "109 two", "110 NULL"])
query('IT', "select id, txt from fuj_anti_on order by id",
      ["13 z", "14 w", "15 x", "16 v", "17 u", "19 s", "108 NULL", "110 x"])

out.append("# First round of changes: new rows on every side.\n")
stmt(f"insert into {D} values (99, 'far', 1, true), (8, 'eight', 2, false), (9, 'nine', 4, true)")
stmt(f"insert into {E} values (111, 'b', 9), (112, 'nine', 9), (113, 'k', 99)")
stmt(f"insert into {F} values (20, 8, 2, 7, 'r'), (21, 9, 9, 0, null), (22, 1, 5, 9, 'q'), (23, 100, 100, 1, 'p')")
stmt(f"insert into {G} values (208, 9, 4), (209, 8, 2), (210, 103, 1)")
out.append("# A row written again with an existing primary key replaces the previous one.\n")
stmt(f"insert into {D} values (3, 'trois', 1, false)")
stmt(f"insert into {F} values (10, 2, 3, 6, 'x2')")
stmt(f"insert into {E} values (101, 'a2', 2)")
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
stmt(f"update {F} set other_id = 2, amt = null where id = 12")
stmt(f"delete from {F} where id = 16")
stmt(f"update {E} set code = 'trois' where id = 103")
stmt(f"update {E} set ref_id = 7 where id = 105")
stmt(f"update {E} set ref_id = null where id = 109")
stmt(f"delete from {E} where id = 111")
stmt(f"update {G} set b = 2 where id = 201")
stmt(f"update {G} set a = 3 where id = 205")
stmt(f"delete from {G} where id = 204")
out.append("# Rows of the derived relations move: duplicates appear and go, groups change.\n")
stmt(f"insert into {G} values (211, 4, 1), (212, null, 2), (213, 2, null)")
stmt(f"update {G} set b = 3 where id = 202")
stmt(f"update {D} set grp = null where id = 7")
stmt(f"update {F} set amt = null where id = 17")
stmt(f"update {F} set amt = 2, other_id = 4 where id = 19")
stmt(f"update {E} set ref_id = 1 where id = 110")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the joined table is emptied and filled again.\n")
stmt(f"delete from {D} where id > 2")
compare_all("After the deletion of most of the joined table")
stmt(f"insert into {D} values (3, 'three', 2, true), (4, 'four', 1, null), (5, 'five', 2, false), (100, 'hundred', 2, true)")
stmt(f"delete from {D} where id = 1")
compare_all("After the joined table is filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view fuj_{name}_{sfx}")
for view in (DL, DAGG, DJ, DA):
    stmt(f"drop view {view}")
for table in (G, E, D, F):
    stmt(f"drop table {table}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
