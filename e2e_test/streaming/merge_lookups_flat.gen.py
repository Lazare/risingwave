"""Generates `e2e_test/streaming/merge_lookups_flat.slt`, the differential test of the merge of
repeated lookups nested in trees of joins, and of the semi and anti joins that the row of an
instance answers: every query is materialized with and without
`streaming_merge_repeated_lookups`.

Usage: python3 e2e_test/streaming/merge_lookups_flat.gen.py e2e_test/streaming/merge_lookups_flat.slt
"""
import sys

F, D, E, G, S, H = 'mlf_f', 'mlf_d', 'mlf_e', 'mlf_g', 'mlf_s', 'mlf_h'
SD = 'mlf_sd'  # view: S with the row of D that its id designates

CASES = [
    # A lookup nested in a tree of inner joins.
    ('inner_ancestor', 'nested inner lookup, the equality comes from the join above',
     f"select f.id as fid, d.grp, x.sid, x.ts, x.name from {F} f join {D} d on f.dim_id = d.id "
     f"join {SD} x on d.id = x.id"),
    ('inner_sibling', 'nested inner lookup, the equality comes from a sibling join',
     f"select f.id as fid, d.grp, d2.name, s.sid, s.ts from {F} f join {D} d on f.dim_id = d.id "
     f"join {S} s on s.id = f.dim_id join {D} d2 on d2.id = s.id"),
    ('inner_filter', 'nested inner lookup with a filter on the removed side',
     f"select f.id as fid, d.grp, x.sid, x.ts, x.name from {F} f join {D} d on f.dim_id = d.id "
     f"join {SD} x on d.id = x.id where x.active"),
    ('two_left_copies', 'two left-joined copies of a table joined on its key',
     f"select d.id as did, e.code, g.a, d2.name from {D} d left join {E} e on d.grp = e.id "
     f"join ({D} d2 left join {G} g on d2.grp = g.id) on d.id = d2.id"),
    # A lookup nested in the right side of a left outer join.
    ('left_chain', 'left join of a chain that looks up the row of the left side',
     f"select f.id as fid, d.name, r.sid, r.v, r.a from {F} f join {D} d on f.dim_id = d.id left join ("
     f"select d2.id, s.sid, case when d2.active then 0 else coalesce(g.a, d2.grp) end as v, g.a "
     f"from {D} d2 join {S} s on d2.id = s.id left join {G} g on d2.id = g.id) r on d.id = r.id"),
    ('left_copy', 'left join of a filtered and left-joined copy of the table of the left side',
     f"select f.id as fid, r.name, r.code, r.one from {F} f left join {D} d on f.dim_id = d.id left join ("
     f"select d2.id, d2.name, e.code, 1 as one from {D} d2 left join {E} e on d2.grp = e.id where d2.active"
     f") r on d.id = r.id"),
    ('left_through_lookup', 'left join of a chain whose joins all compare the removed lookup',
     f"select f.id as fid, d.name, y.sid, y.z from {F} f join {D} d on f.dim_id = d.id left join ("
     f"select g.id, s.sid, g.a + d2.grp as z from {G} g join {D} d2 on g.id = d2.id join {S} s on d2.id = s.id"
     f") y on d.id = y.id"),
    # Semi and anti joins.
    ('anti_chain', 'not exists on a chain that looks up the row of the left side',
     f"select f.id as fid from {F} f join {D} d on f.dim_id = d.id where d.grp < 10 and not exists ("
     f"select 1 from {S} s join {D} d2 on s.id = d2.id and d2.active left join {E} e on s.id = e.id "
     f"where s.id = f.dim_id)"),
    ('semi_chain', 'exists on a chain that looks up the row of the left side',
     f"select f.id as fid, d.name from {F} f left join {D} d on f.dim_id = d.id where exists ("
     f"select 1 from {S} s join {D} d2 on s.id = d2.id where s.id = d.id and d2.grp > 1)"),
    ('anti_carried', 'not exists on a chain whose rows the left side all carries',
     f"select f.id as fid, h.rid from {F} f join {D} d on f.dim_id = d.id join {H} h on h.id = d.id "
     f"where not exists (select 1 from {H} h2 join {D} d2 on h2.id = d2.id and d2.active "
     f"left join {E} e on h2.id = e.id where h2.id = f.dim_id)"),
    ('semi_witness', 'exists on a column that is not a key, carried by the left side',
     f"select f.id as fid, h.rid, h.x from {F} f join {H} h on f.dim_id = h.id "
     f"where exists (select 1 from {H} h2 where h2.id = f.dim_id)"),
    ('anti_witness_padded', 'not exists on a column that is not a key, carried by the padded side',
     f"select f.id as fid, h.rid, h.x from {F} f left join {H} h on f.other_id = h.id "
     f"where not exists (select 1 from {H} h2 where h2.id = h.id)"),
    # Kept.
    ('kept_witness_filter', 'exists with a filter that the carried row may fail',
     f"select f.id as fid, h.rid from {F} f join {H} h on f.dim_id = h.id "
     f"where exists (select 1 from {H} h2 where h2.id = f.dim_id and h2.x > 0)"),
    ('kept_anti_unbound', 'not exists on a column that the outer join compares but does not bind',
     f"select f.id as fid, h.rid from {F} f left join {H} h on f.other_id = h.id "
     f"where not exists (select 1 from {H} h2 where h2.id = f.other_id)"),
    ('kept_nonkey', 'nested lookup compared on a column that is not its key',
     f"select f.id as fid, d.grp, x.sid, x.name from {F} f join {D} d on f.dim_id = d.id "
     f"join (select s.sid, s.id, d2.name from {S} s join {D} d2 on s.id = d2.grp) x on d.id = x.id"),
    ('kept_padded', 'nested lookup in the padded side of a left join of the tree',
     f"select f.id as fid, d.grp, x.sid, x.name from {F} f join {D} d on f.dim_id = d.id "
     f"join (select s.sid, s.id, d2.name from {S} s left join {D} d2 on s.id = d2.id) x on d.id = x.id"),
    ('kept_ext_cond', 'left join whose condition tests a column of a left join of its right side',
     f"select f.id as fid, r.name, r.code from {F} f left join {D} d on f.dim_id = d.id left join ("
     f"select d2.id, d2.name, e.code from {D} d2 left join {E} e on d2.grp = e.id"
     f") r on d.id = r.id and r.code = 'x'"),
]

out = []
def switch(on):
    stmt(f"set streaming_merge_repeated_lookups = {'true' if on else 'false'}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"mlf_{name}_on", f"mlf_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_batch(name, sql):
    a = f"mlf_{name}_on"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from ({sql}) q) x),\n"
          f"  (select count(*) from (select * from ({sql}) q except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from ({sql}) q)",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the variable, every materialized view holds the same rows.\n")
    for name, _, _ in CASES:
        compare(name)
    out.append(f"# {title}: with the variable, every materialized view holds the rows of its query.\n")
    for name, _, sql in CASES:
        compare_batch(name, sql)

out.append("""# Differential test of the merge of repeated lookups nested in trees of joins
# (`streaming_merge_repeated_lookups`): every query is materialized twice, with and without the
# variable, and both materialized views must hold the same rows, and the rows of the query, after
# the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/merge_lookups_flat.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {F} (id int primary key, dim_id int, other_id int, amt int)")
stmt(f"create table {D} (id int primary key, name varchar, grp int, active boolean)")
stmt(f"create table {E} (id int primary key, code varchar)")
stmt(f"create table {G} (id int primary key, a int)")
stmt(f"create table {S} (sid int primary key, id int, ts int)")
stmt(f"create table {H} (rid int primary key, id int, x int)")
stmt(f"create view {SD} as select s.sid, s.id, s.ts, d.name, d.active from {S} s join {D} d on s.id = d.id")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {D} values (1, 'a', 1, true), (2, 'b', 2, false), (3, 'c', null, true), "
     f"(4, null, 3, null), (5, 'e', 1, true), (6, 'f', 2, false)")
stmt(f"insert into {F} values (10, 1, 2, 5), (11, 2, null, 6), (12, 3, 3, 7), (13, null, 1, 8), "
     f"(14, 9, 9, 9), (15, 1, 4, null), (16, 4, 1, 1), (17, 6, 6, 2)")
stmt(f"insert into {E} values (1, 'x'), (2, 'y'), (3, null)")
stmt(f"insert into {G} values (1, 10), (2, null), (3, 30), (5, 50)")
stmt(f"insert into {S} values (20, 1, 100), (21, 1, 101), (22, 2, 102), (23, null, 103), "
     f"(24, 4, 104), (25, 9, 105), (26, 6, 106)")
stmt(f"insert into {H} values (30, 1, 1), (31, 1, -1), (32, 2, 0), (33, 3, null), "
     f"(34, null, 5), (35, 6, 2), (36, 4, 4)")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view mlf_{name}_{sfx} as {sql}")

compare_all("After the backfill")

out.append("# First round of changes: new rows on every side, rows written again with the same key.\n")
stmt(f"insert into {D} values (7, 'g', 3, true), (9, 'i', 2, true)")
stmt(f"insert into {D} values (2, 'b2', 1, true)")
stmt(f"insert into {F} values (18, 7, 7, 3), (19, 5, 5, 4), (20, null, null, null)")
stmt(f"insert into {S} values (27, 7, 107), (28, 5, 108), (29, 3, 109)")
stmt(f"insert into {H} values (37, 7, 7), (38, 5, 0), (39, 9, 1)")
stmt(f"insert into {E} values (4, 'x')")
stmt(f"insert into {G} values (7, 70)")
compare_all("After the inserts")

out.append("# Second round of changes: rows of the looked-up table change or disappear under the others.\n")
stmt(f"update {D} set active = false where id in (1, 7)")
stmt(f"update {D} set grp = null, name = null where id = 5")
stmt(f"update {D} set grp = 4 where id = 3")
stmt(f"delete from {D} where id in (4, 9)")
stmt(f"update {F} set dim_id = 3, other_id = 7 where id = 14")
stmt(f"update {F} set dim_id = null where id = 15")
stmt(f"update {S} set id = 3 where sid = 21")
stmt(f"delete from {S} where sid = 24")
stmt(f"update {H} set id = 2 where rid = 30")
stmt(f"update {H} set x = 3 where rid = 31")
stmt(f"delete from {H} where rid = 36")
stmt(f"delete from {E} where id = 1")
stmt(f"update {G} set a = null where id = 3")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the looked-up table is emptied and filled again.\n")
stmt(f"delete from {D}")
compare_all("After the looked-up table is emptied")
stmt(f"insert into {D} values (1, 'a', 1, true), (2, 'b', 3, true), (3, null, null, false), "
     f"(5, 'e', 2, null), (6, 'f', 1, true), (7, 'g', 2, false)")
compare_all("After the looked-up table is filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view mlf_{name}_{sfx}")
stmt(f"drop view {SD}")
for table in (H, S, G, E, D, F):
    stmt(f"drop table {table}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
