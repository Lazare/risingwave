"""Generates `e2e_test/batch/transitive_predicates.slt`, the differential test of
`batch_transitive_predicates` (rule `TransitivePredicateRule`: a predicate that holds on every row
of one side of a join and only reads the columns compared with `=` is copied to the other side;
with `batch_cost_based_join_ordering`, the equalities implied by those of the inner joins link
their inputs). Every query runs twice, with and without the variable, through `INSERT ... SELECT`
(the batch plan), and both results must hold the same rows with the same multiplicities, after
each round of changes of the data.

Usage: python3 e2e_test/batch/transitive_predicates.gen.py e2e_test/batch/transitive_predicates.slt
"""
import sys

O, S, S2, C, EU = 'tpb_o', 'tpb_s', 'tpb_s2', 'tpb_c', 'tpb_eu'
TA, TB, TC, TD = 'tpb_ta', 'tpb_tb', 'tpb_tc', 'tpb_td'
VAR = 'batch_transitive_predicates'

# (name, typed columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('inner_view', 'id int, sid int',
     f"select o.id, s.sid from {EU} o join {S} s on s.region = o.region and s.order_id = o.id"),
    ('pk_range', 'id int, x int',
     f"select o.id, s2.x from {EU} o join {S2} s2 on s2.region = o.region and s2.order_id = o.id"),
    ('only_keys', 'id int, sid int, w int',
     f"select o.id, s.sid, s.w from (select * from {O} where amount > 5 and id > 10 "
     f"and amount + id < 100) o join {S} s on s.order_id = o.id"),
    ('chain', 'id int, sid int, cid int, note varchar',
     f"select o.id, s.sid, c.id as cid, c.note from {EU} o join {S} s on s.order_id = o.id "
     f"join {C} c on c.order_id = s.order_id"),
    ('union_tags', 'tag varchar, id int, x int',
     f"select u.tag, u.id, s2.x from (select 'A' as tag, id from {O} union all "
     f"select 'EU' as tag, id from {O} where amount > 5) u join {S2} s2 on s2.region = u.tag and s2.order_id = u.id"),
    ('values', 'x int, sid int',
     f"select v.x, s.sid from (values (11), (12), (12), (40)) v(x) join {S} s on s.order_id = v.x"),
    ('agg_key', 'region varchar, n bigint, sid int',
     f"select g.region, g.n, s.sid from (select region, count(*) as n from {O} "
     f"where region in ('EU', 'US') group by region) g join {S} s on s.region = g.region"),
    ('left', 'id int, sid int',
     f"select o.id, s.sid from {EU} o left join (select * from {S} where w > 1) s "
     f"on s.region = o.region and s.order_id = o.id"),
    ('right', 'sid int, id int',
     f"select s0.sid, o.id from (select * from {S} where order_id > 11) s0 "
     f"right join (select * from {O} where id < 14) o on s0.order_id = o.id"),
    ('semi', 'id int',
     f"select o.id from {O} o where o.id in (select order_id from {S} where order_id between 11 and 13)"),
    ('anti_into', 'id int',
     f"select o.id from {EU} o where not exists (select 1 from {S} s where s.order_id = o.id)"),
    ('numeric_cmp', 'id int, sid int, ks varchar, sf float8',
     f"select o.id, s.sid, s.k::varchar as ks, s.f as sf from (select * from {O} "
     f"where k > 1.2 and f between -1 and 1) o join {S} s on s.k = o.k and s.f = o.f"),
    ('indf_nonnull', 'id int, sid int',
     f"select o.id, s.sid from {EU} o join (select * from {S} where order_id is not null) s "
     f"on s.order_id is not distinct from o.id"),
    ('contradiction', 'id int, sid int',
     f"select o.id, s.sid from (select * from {O} where id > 15 and id < 3) o join {S} s on s.order_id = o.id"),
    ('agg_over_join', 'region varchar, n bigint, m bigint',
     f"select o.region, count(*) as n, count(s.sid) as m from {EU} o left join {S} s "
     f"on s.region = o.region and s.order_id = o.id group by o.region"),
    # Implied equalities in the cost-based join ordering.
    ('implied_chain', 'p int, q int, r int',
     f"select ta.p, tb.q, tc.r from {TA} ta join {TB} tb on ta.x = tb.y join {TC} tc on tb.y = tc.z "
     f"where ta.p < 3"),
    ('implied_four', 'p int, q int, r int, v int',
     f"select ta.p, tb.q, tc.r, td.v from {TA} ta join {TB} tb on ta.x = tb.y "
     f"join {TC} tc on tb.y = tc.z join {TD} td on td.z = tc.z"),
    ('implied_outer', 'p int, q int, r int',
     f"select ta.p, tb.q, tc.r from {TA} ta join {TB} tb on ta.x = tb.y left join {TC} tc on tb.y = tc.z"),
    # Left as they are by the rule.
    ('left_kept', 'id int, sid int',
     f"select o.id, s.sid from {O} o left join (select * from {S} where region = 'EU') s on s.region = o.region"),
    ('full_kept', 'id int, sid int',
     f"select o.id, s.sid from {EU} o full join {S} s on s.region = o.region and s.order_id = o.id"),
    ('anti_kept', 'id int',
     f"select o.id from {O} o where not exists (select 1 from {S} s where s.order_id = o.id and s.order_id > 11)"),
    ('numeric_text_kept', 'id int, sid int, ks varchar',
     f"select o.id, s.sid, s.k::varchar as ks from (select * from {O} where k::varchar = '1.0') o "
     f"join {S} s on s.k = o.k"),
    ('float_text_kept', 'id int, sid int',
     f"select o.id, s.sid from (select * from {O} where f::varchar = '-0') o join {S} s on s.f = o.f"),
    ('indf_kept', 'id int, sid int',
     f"select o.id, s.sid from (select * from {O} where id > 12) o "
     f"join {S} s on s.order_id is not distinct from o.id"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    out.append(f"# {title}: every query runs without, then with the variable.\n")
    for name, _, sql in CASES:
        stmt(f"delete from tpb_{name}_off")
        stmt(f"delete from tpb_{name}_on")
        stmt(f"insert into tpb_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into tpb_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"tpb_{name}_on", f"tpb_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as tpb_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append("""# Differential test of `batch_transitive_predicates`: every query runs through
# `INSERT ... SELECT` without, then with the variable, and both results must hold the same rows
# with the same multiplicities, after each round of changes of the data. The cost-based join
# ordering is on in both runs.
#
# Generated by `python3 e2e_test/batch/transitive_predicates.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt("set batch_cost_based_join_ordering = true")
stmt(f"create table {O} (id int primary key, region varchar, amount int, k numeric, f float8)")
stmt(f"create table {S} (sid int primary key, order_id int, region varchar, w int, k numeric, f float8)")
stmt(f"create table {S2} (region varchar, order_id int, x int, primary key (region, order_id))")
stmt(f"create table {C} (id int primary key, order_id int, note varchar)")
stmt(f"create table {TA} (x int primary key, p int)")
stmt(f"create table {TB} (id int primary key, y int, q int)")
stmt(f"create table {TC} (z int primary key, r int)")
stmt(f"create table {TD} (id int primary key, z int, v int)")
stmt(f"create view {EU} as select * from {O} where region = 'EU' and id > 10")
for name, cols, _ in CASES:
    stmt(f"create table tpb_{name}_off ({cols})")
    stmt(f"create table tpb_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: equal numeric values with different texts (1.0, 1.00), floats -0 and 0,\n"
           "# NULL keys, duplicate keys on the other side, rows without a match.\n")
stmt(f"insert into {O} values (1, 'EU', 3, 1.0, -0.0), (11, 'EU', 7, 1.00, 0.0), (12, 'EU', 9, 1.5, 0.5), "
     f"(13, 'US', 6, 2, 1), (14, null, 8, null, null), (15, 'EU', null, 1.0, 2), (16, 'A', 2, 3, -0.0)")
stmt(f"insert into {S} values (1, 11, 'EU', 1, 1.00, 0.0), (2, 11, 'EU', 2, 1.0, -0.0), (3, 12, 'EU', 3, 1.5, 0.5), "
     f"(4, 12, 'US', 4, 1.50, 0.5), (5, 1, 'EU', 5, 1, 0), (6, null, 'EU', 6, null, null), "
     f"(7, 13, null, 7, 2.0, 1), (8, 16, 'A', 8, 3, 0), (9, 11, 'B', 9, 1.0, 1), (10, 40, 'EU', 10, 9, 9)")
stmt(f"insert into {S2} values ('EU', 11, 110), ('EU', 12, 120), ('US', 13, 130), ('EU', 1, 10), "
     f"('EU', 15, 150), ('A', 16, 160)")
stmt(f"insert into {C} values (1, 11, 'c11'), (2, 12, 'c12'), (3, 12, 'c12b'), (4, 1, 'c1'), "
     f"(5, null, 'cn'), (6, 13, 'c13')")
stmt(f"insert into {TA} values (1, 1), (2, 2), (3, 3), (4, null)")
stmt(f"insert into {TB} select g, g % 7, g from generate_series(1, 200) g")
stmt(f"insert into {TB} values (1000, null, 0), (1001, 2, null)")
stmt(f"insert into {TC} values (1, 10), (2, 20), (5, 50), (6, null)")
stmt(f"insert into {TD} values (1, 1, 100), (2, 1, 101), (3, 2, 102), (4, null, 103), (5, 6, 104)")
run_all("First data")

out.append("# Second data: new rows, keys and filtered columns updated, deletes.\n")
stmt(f"insert into {O} values (17, 'EU', 4, 1.0, 0), (18, 'B', 5, 1.5, 0.5), (19, 'EU', 6, 2, 1)")
stmt(f"insert into {S} values (11, 17, 'EU', 11, 1.0, 0), (12, 19, 'EU', 12, 2.00, 1), (13, 18, 'B', 13, 1.5, 0.5), "
     f"(14, 12, 'EU', 14, 1.5, -0.0), (15, 15, 'EU', 15, 1.00, 2)")
stmt(f"update {O} set region = 'US' where id = 11")
stmt(f"update {O} set k = 1.20 where id = 12")
stmt(f"update {S} set order_id = null where sid = 1")
stmt(f"delete from {S} where sid in (2, 9)")
stmt(f"delete from {TB} where id % 5 = 0")
stmt(f"update {TC} set r = null where z = 1")
run_all("Second data")

out.append("# Third data: one side empty.\n")
stmt(f"delete from {S}")
stmt(f"delete from {TC}")
run_all("Third data")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table tpb_{name}_on")
    stmt(f"drop table tpb_{name}_off")
stmt(f"drop view {EU}")
for table in (TD, TC, TB, TA, C, S2, S, O):
    stmt(f"drop table {table}")
stmt("set batch_cost_based_join_ordering = false")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
