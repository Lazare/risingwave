"""Generates `e2e_test/streaming/transitive_predicates.slt`, the differential test of
`streaming_transitive_predicates` (rule `TransitivePredicateRule`: a predicate that holds on every
row of one side of a join and only reads the columns compared with `=` is copied to the other
side): every query is materialized with and without the variable, and both materialized views must
hold the same rows, with the same multiplicities, as the batch query, after the backfill and after
each round of changes.

Usage: python3 e2e_test/streaming/transitive_predicates.gen.py e2e_test/streaming/transitive_predicates.slt
"""
import sys

O, S, S2, C, H, HS, EU = 'tp_o', 'tp_s', 'tp_s2', 'tp_c', 'tp_h', 'tp_hs', 'tp_eu'
VAR = 'streaming_transitive_predicates'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('inner_view', 'id, sid',
     f"select o.id, s.sid from {EU} o join {S} s on s.region = o.region and s.order_id = o.id"),
    ('pk_range', 'id, x',
     f"select o.id, s2.x from {EU} o join {S2} s2 on s2.region = o.region and s2.order_id = o.id"),
    ('only_keys', 'id, sid, w',
     f"select o.id, s.sid, s.w from (select * from {O} where amount > 5 and id > 10 "
     f"and amount + id < 100) o join {S} s on s.order_id = o.id"),
    ('chain', 'id, sid, cid, note',
     f"select o.id, s.sid, c.id as cid, c.note from {EU} o join {S} s on s.order_id = o.id "
     f"join {C} c on c.order_id = s.order_id"),
    ('chain_right', 'id, sid, cid',
     f"select o.id, s.sid, c.id as cid from {O} o join {S} s on s.order_id = o.id "
     f"join (select * from {C} where order_id between 12 and 14) c on c.order_id = s.order_id"),
    ('union_tags', 'tag, id, sid',
     f"select u.tag, u.id, s.sid from (select 'A' as tag, id from {O} union all "
     f"select 'B' as tag, id from {O} where amount > 5) u join {S} s on s.region = u.tag and s.order_id = u.id"),
    ('values', 'x, sid',
     f"select v.x, s.sid from (values (11), (12), (12), (40)) v(x) join {S} s on s.order_id = v.x"),
    ('agg_key', 'region, n, sid',
     f"select g.region, g.n, s.sid from (select region, count(*) as n from {O} "
     f"where region in ('EU', 'US') group by region) g join {S} s on s.region = g.region"),
    ('left', 'id, sid',
     f"select o.id, s.sid from {EU} o left join {S} s on s.region = o.region and s.order_id = o.id"),
    ('right', 'sid, id',
     f"select s0.sid, o.id from (select * from {S} where order_id > 11) s0 "
     f"right join (select * from {O} where id < 14) o on s0.order_id = o.id"),
    ('semi', 'id',
     f"select o.id from {O} o where o.id in (select order_id from {S} where order_id > 11)"),
    ('anti_into', 'id',
     f"select o.id from {EU} o where not exists (select 1 from {S} s where s.order_id = o.id)"),
    ('numeric_cmp', 'id, sid, w, sf',
     f"select o.id, s.sid, s.w, s.f as sf from (select * from {O} "
     f"where k > 1.2 and f between -1 and 1) o join {S} s on s.k = o.k and s.f = o.f"),
    ('indf_nonnull', 'id, sid',
     f"select o.id, s.sid from {EU} o join (select * from {S} where order_id is not null) s "
     f"on s.order_id is not distinct from o.id"),
    ('from_above', 'id, sid',
     f"select o.id, s.sid from {O} o join {S} s on s.order_id = o.id where o.id > 12"),
    ('contradiction', 'id, sid',
     f"select o.id, s.sid from (select * from {O} where id > 15 and id < 3) o join {S} s on s.order_id = o.id"),
    ('agg_over_join', 'region, n, m',
     f"select o.region, count(*) as n, count(s.sid) as m from {EU} o left join {S} s "
     f"on s.region = o.region and s.order_id = o.id group by o.region"),
    # Left as they are by the rule.
    ('left_kept', 'id, sid',
     f"select o.id, s.sid from {O} o left join (select * from {S} where region = 'EU') s on s.region = o.region"),
    ('full_kept', 'id, sid',
     f"select o.id, s.sid from {EU} o full join {S} s on s.region = o.region and s.order_id = o.id"),
    ('anti_kept', 'id',
     f"select o.id from {O} o where not exists (select 1 from {S} s where s.order_id = o.id and s.order_id > 11)"),
    ('now_kept', 'id, hid',
     f"select h.id, hs.id as hid from (select * from {H} where ts > now() - interval '1 hour') h "
     f"join {HS} hs on hs.ts = h.ts"),
    ('numeric_text_kept', 'id, sid, w',
     f"select o.id, s.sid, s.w from (select * from {O} where k::varchar = '1.0') o "
     f"join {S} s on s.k = o.k"),
    ('float_text_kept', 'id, sid',
     f"select o.id, s.sid from (select * from {O} where f::varchar = '-0') o join {S} s on s.f = o.f"),
    ('indf_kept', 'id, sid',
     f"select o.id, s.sid from (select * from {O} where id > 12 or id is null) o "
     f"join {S} s on s.order_id is not distinct from o.id"),
]

# Compared with the materialized view without the rule only, not with the batch query: with a
# filter on the text of a numeric column (1.0 and 1.00 are equal, with different texts), the
# materialized views with and without the rule keep one row more than the batch query after the
# rows of the left side are updated and deleted. The plan is the same with and without the rule
# (the predicate is not copied), so this does not come from the rule.
NOT_WITH_BATCH = {'numeric_text_kept'}

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    grouped = lambda t: f"select {cols}, count(*) as tp_rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def compare_all(title):
    out.append(f"# {title}: with and without the rule, every materialized view holds the rows of "
               f"the batch query.\n")
    for name, cols, sql in CASES:
        same_rows(f"tp_{name}_on", f"tp_{name}_off", cols)
        if name not in NOT_WITH_BATCH:
            same_rows(f"tp_{name}_on", f"({sql}) q", cols)


out.append("""# Differential test of `streaming_transitive_predicates`: every query is materialized twice,
# with and without the variable, and both materialized views must hold the rows of the batch query,
# with the same multiplicities, after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/transitive_predicates.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {O} (id int primary key, region varchar, amount int, k numeric, f float8)")
stmt(f"create table {S} (sid int primary key, order_id int, region varchar, w int, k numeric, f float8)")
stmt(f"create table {S2} (region varchar, order_id int, x int, primary key (region, order_id))")
stmt(f"create table {C} (id int primary key, order_id int, note varchar)")
stmt(f"create table {H} (id int primary key, ts timestamptz, v int)")
stmt(f"create table {HS} (id int primary key, ts timestamptz, w int)")
stmt(f"create view {EU} as select * from {O} where region = 'EU' and id > 10")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n"
           "# The text of an equi-join key is not compared with the batch query: a streaming join may\n"
           "# output the key of either side, which differ in text for equal numeric values.\n"
           "# Equal numeric values with different texts (1.0, 1.00), floats -0 and 0, NULL keys,\n"
           "# duplicate keys on the other side.\n")
stmt(f"insert into {O} values (1, 'EU', 3, 1.0, -0.0), (11, 'EU', 7, 1.00, 0.0), (12, 'EU', 9, 1.5, 0.5), "
     f"(13, 'US', 6, 2, 1), (14, null, 8, null, null), (15, 'EU', null, 1.0, 2), (16, 'A', 2, 3, -0.0)")
stmt(f"insert into {S} values (1, 11, 'EU', 1, 1.00, 0.0), (2, 11, 'EU', 2, 1.0, -0.0), (3, 12, 'EU', 3, 1.5, 0.5), "
     f"(4, 12, 'US', 4, 1.50, 0.5), (5, 1, 'EU', 5, 1, 0), (6, null, 'EU', 6, null, null), "
     f"(7, 13, null, 7, 2.0, 1), (8, 16, 'A', 8, 3, 0), (9, 11, 'B', 9, 1.0, 1), (10, 40, 'EU', 10, 9, 9)")
stmt(f"insert into {S2} values ('EU', 11, 110), ('EU', 12, 120), ('US', 13, 130), ('EU', 1, 10), "
     f"('EU', 15, 150), ('A', 16, 160)")
stmt(f"insert into {C} values (1, 11, 'c11'), (2, 12, 'c12'), (3, 12, 'c12b'), (4, 1, 'c1'), "
     f"(5, null, 'cn'), (6, 13, 'c13')")
stmt(f"insert into {H} values (1, '2000-01-01 00:00:00+00', 1), (2, '2100-01-01 00:00:00+00', 2), "
     f"(3, '2100-01-02 00:00:00+00', 3)")
stmt(f"insert into {HS} values (1, '2000-01-01 00:00:00+00', 1), (2, '2100-01-01 00:00:00+00', 2), "
     f"(3, '2100-01-01 00:00:00+00', 3)")

for sfx in ('off', 'on'):
    stmt(f"set {VAR} = {'true' if sfx == 'on' else 'false'}")
    for name, _, sql in CASES:
        stmt(f"create materialized view tp_{name}_{sfx} as {sql}")
stmt(f"set {VAR} = false")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('I', "select count(*) from tp_inner_view_on", ["3"])
query('I', "select count(*) from tp_contradiction_on", ["0"])

out.append("# First round of changes: new rows on both sides, keys that start to match.\n")
stmt(f"insert into {O} values (17, 'EU', 4, 1.0, 0), (18, 'B', 5, 1.5, 0.5), (19, 'EU', 6, 2, 1)")
stmt(f"insert into {S} values (11, 17, 'EU', 11, 1.0, 0), (12, 19, 'EU', 12, 2.00, 1), (13, 18, 'B', 13, 1.5, 0.5), "
     f"(14, 12, 'EU', 14, 1.5, -0.0), (15, 15, 'EU', 15, 1.00, 2)")
stmt(f"insert into {S2} values ('EU', 17, 170), ('B', 18, 180)")
stmt(f"insert into {C} values (7, 17, 'c17'), (8, 19, 'c19')")
stmt(f"insert into {H} values (4, '2100-01-01 00:00:00+00', 4)")
stmt(f"insert into {HS} values (4, '2100-01-02 00:00:00+00', 4)")
out.append("# A row written again with an existing primary key replaces the previous one.\n")
stmt(f"insert into {O} values (1, 'EU', 3, 1.00, 0.0)")
stmt(f"insert into {S} values (5, 1, 'US', 5, 1.0, 0)")
compare_all("After the inserts")

out.append("# Second round of changes: keys and filtered columns updated both ways, deletes (retractions).\n")
stmt(f"update {O} set region = 'US' where id = 11")
stmt(f"update {O} set region = 'EU' where id = 13")
stmt(f"update {O} set k = 1.20 where id = 12")
stmt(f"update {S} set order_id = 13 where sid = 3")
stmt(f"update {S} set region = 'EU' where sid in (4, 7)")
stmt(f"update {S} set order_id = null where sid = 1")
stmt(f"update {C} set order_id = 13 where id = 1")
stmt(f"delete from {S} where sid in (2, 9)")
stmt(f"delete from {O} where id = 15")
stmt(f"delete from {S2} where order_id = 12")
stmt(f"update {HS} set ts = '2100-01-01 00:00:00+00' where id = 4")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the right side emptied, then filled again; the left side emptied.\n")
stmt(f"delete from {S}")
compare_all("After the right side is emptied")
stmt(f"insert into {S} values (1, 11, 'EU', 1, 1.00, 0.0), (2, 13, 'EU', 2, 2, 1), (3, 12, 'EU', 3, 1.2, 0.5), "
     f"(4, null, null, 4, null, null), (5, 19, 'EU', 5, 2, 1)")
compare_all("After the right side is filled again")
stmt(f"delete from {O}")
compare_all("After the left side is emptied")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view tp_{name}_{sfx}")
stmt(f"drop view {EU}")
for table in (HS, H, C, S2, S, O):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
