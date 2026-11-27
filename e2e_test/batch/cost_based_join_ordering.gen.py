"""Generates `e2e_test/batch/cost_based_join_ordering.slt`, the differential test of the cost-based
ordering of the joins of batch queries: every query runs with and without
`batch_cost_based_join_ordering`, and both results must hold the same rows (as multisets), on the
initial data and after a round of changes. The tables have very different sizes so that the
storage statistics lead to other join orders than the heuristic one.

Usage: python3 e2e_test/batch/cost_based_join_ordering.gen.py e2e_test/batch/cost_based_join_ordering.slt
"""
import sys

P = 'cbj_'
F, D1, D2, D3, G, H, A, B, C, M = (P + t for t in ('f', 'd1', 'd2', 'd3', 'g', 'h', 'a', 'b', 'c', 'm'))
# A narrow table with many rows and a table with a large JSON column: the cost of a join counts
# bytes, so the narrow side becomes the build side (and a left join a right join).
N, W = P + 'n', P + 'w'
TOP1 = (f"(select src, body from (select src, body, row_number() over (partition by src order by done_at desc) "
        f"as rn from {W}) t where rn = 1)")
LAST = f"(select src, last_value(body order by done_at) as body, count(*) as cnt from {W} group by src)"

STAR = (f"from {D1} d1 join {F} f on f.d1_id = d1.id join {D2} d2 on f.d2_id = d2.id")

# (name, types of the result columns, query, extra settings for both runs)
CASES = [
    ('star', 'int, int, int',
     f"select f.id, d1.a, d2.b {STAR}", None),
    ('star4', 'int, int, int, int',
     f"select f.id, d1.a, d2.b, d3.c {STAR} join {D3} d3 on f.d3_id = d3.id", None),
    ('star_no_lookup', 'int, int, int, int',
     f"select f.id, d1.a, d2.b, d3.c {STAR} join {D3} d3 on f.d3_id = d3.id",
     'batch_enable_lookup_join'),
    ('key_eq', 'int, int, int',
     f"select f.id, d1.a, d2.b {STAR} where d2.id = 7", None),
    ('lookup_from_small', 'int, int, int',
     f"select f.id, g.x, d1.a from {F} f join {G} g on g.id = f.v join {D1} d1 on f.id = d1.k where d1.a = 1", None),
    ('two_way_key', 'int, int',
     f"select f.id, d1.id from {F} f join {D1} d1 on f.id = d1.k where d1.a = 1", None),
    ('two_way_non_key', 'int, int, int',
     f"select a.x, a.y, b.y from {A} a join {B} b on a.x = b.x", None),
    ('two_way_non_key_rev', 'int, int, int',
     f"select a.x, a.y, b.y from {B} b join {A} a on a.x = b.x", None),
    ('chain', 'int, int, int',
     f"select a.x, b.x, c.y from {A} a join {B} b on a.y = b.x join {C} c on b.y = c.x", None),
    ('snowflake', 'int, int, int, int',
     f"select f.id, d1.a, h.y, d2.b from {H} h join {D2} d2 on d2.k = h.id join {F} f on f.d2_id = d2.id "
     f"join {D1} d1 on f.d1_id = d1.id where h.x = 1", None),
    ('left_input', 'int, int, int, int',
     f"select f.id, d1.a, d2.b, d3.c from {D2} d2 join ({F} f left join {D1} d1 on f.d1_id = d1.id) "
     f"on f.d2_id = d2.id join {D3} d3 on f.d3_id = d3.id", None),
    ('under_left', 'int, int, int, int',
     f"select d3.id, d3.c, f.id, d1.a from {D3} d3 left join ({D1} d1 join {F} f on f.d1_id = d1.id "
     f"join {D2} d2 on f.d2_id = d2.id) on f.d3_id = d3.id", None),
    ('semi_anti', 'int, int',
     f"select f.id, d1.a {STAR} where exists (select 1 from {G} g where g.id = f.v and g.x > 3) "
     f"and not exists (select 1 from {H} h where h.id = f.w)", None),
    ('agg_input', 'int, bigint, int, int',
     f"select s.v, s.cnt, d1.id, g.id from (select v, count(*) as cnt from {F} group by v) s "
     f"join {D1} d1 on d1.k = s.v join {G} g on g.x = d1.a", None),
    ('scalar_cross', 'int, int, int, int',
     f"select f.id, d1.a, d2.b, s.m {STAR}, (select max(x) as m from {G}) s where f.v > s.m", None),
    ('table_function', 'int, int, int, int',
     f"select f.id, d1.a, d2.b, s.n from {D1} d1 join {F} f on f.d1_id = d1.id "
     f"join generate_series(1, 5) s(n) on s.n = f.w join {D2} d2 on f.d2_id = d2.id", None),
    ('non_eq_conditions', 'int, int, int',
     f"select f.id, d1.a, d2.b from {D1} d1 join {F} f on f.d1_id = d1.id and f.v > d1.a "
     f"join {D2} d2 on f.d2_id = d2.id and d2.b <> d1.a", None),
    ('not_distinct', 'int, int, int',
     f"select f.id, d1.a, d2.b from {D1} d1 join {F} f on f.d1_id is not distinct from d1.id "
     f"join {D2} d2 on f.d2_id = d2.id", None),
    ('self_join', 'int, int, int',
     f"select x.id, y.id, d3.c from {F} x join {F} y on x.v = y.w join {D3} d3 on d3.id = x.d3_id", None),
    ('composite_key', 'int, int, int',
     f"select f.id, m.val, d1.a from {F} f join {M} m on m.k1 = f.d1_id and m.k2 = f.d3_id "
     f"join {D1} d1 on d1.id = f.d1_id", None),
    ('group_above', 'int, bigint, bigint',
     f"select d1.a, count(*), sum(f.v) {STAR} group by d1.a", None),
    ('twelve', 'int, int, int, int, int, int, int, int, int, int, int, int',
     f"select f.id, x1.a, x2.a, x3.b, x4.b, x5.c, x6.c, x7.x, x8.x, x9.x, x10.b, x11.a "
     f"from {D1} x1 join {F} f on f.d1_id = x1.id join {D1} x2 on f.v = x2.id "
     f"join {D2} x3 on f.d2_id = x3.id join {D2} x4 on f.w = x4.id join {D3} x5 on f.d3_id = x5.id "
     f"join {D3} x6 on x1.k = x6.id join {H} x7 on x2.k = x7.id join {G} x8 on x3.k = x8.id "
     f"join {H} x9 on x4.k = x9.id join {D2} x10 on x7.y = x10.id join {D1} x11 on x8.y = x11.id", None),
    ('wide_inner', 'int, varchar, timestamp',
     f"select n.id, w.body->>'k', w.done_at from {N} n join {W} w on n.src = w.src", None),
    ('wide_left', 'int, int, varchar',
     f"select n.id, w.id, w.body->>'k' from {N} n left join {W} w on n.src = w.src", None),
    ('wide_left_preserved', 'int, int',
     f"select w.id, n.n from {W} w left join {N} n on n.src = w.src", None),
    ('wide_semi', 'int, int',
     f"select n.id, n.n from {N} n where exists (select 1 from {W} w where w.src = n.src and w.id % 3 = 0)", None),
    ('wide_anti', 'int, int',
     f"select n.id, n.n from {N} n where not exists (select 1 from {W} w where w.src = n.src and w.id % 3 = 0)", None),
    ('wide_agg', 'int, varchar, bigint',
     f"select n.id, w.body->>'k', w.cnt from {N} n join {LAST} w on n.src = w.src", None),
    ('wide_agg_left', 'int, varchar, bigint',
     f"select n.id, w.body->>'k', w.cnt from {N} n left join {LAST} w on n.src = w.src", None),
    ('wide_top1', 'int, varchar',
     f"select n.id, w.body->>'k' from {N} n join {TOP1} w on n.src = w.src", None),
    ('wide_top1_left', 'int, varchar',
     f"select n.id, w.body->>'k' from {N} n left join {TOP1} w on n.src = w.src", None),
    ('wide_three', 'int, varchar, int',
     f"select n.id, w.body->>'k', d3.c from {N} n join {W} w on n.src = w.src join {D3} d3 on d3.id = n.n", None),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def run_all():
    for name, types, sql, off_setting in CASES:
        if off_setting:
            stmt(f"set {off_setting} = false")
        for sfx, on in (('off', 'false'), ('on', 'true')):
            stmt(f"set batch_cost_based_join_ordering = {on}")
            stmt(f"insert into {P}r_{sfx}_{name} {sql}")
        stmt("set batch_cost_based_join_ordering = false")
        if off_setting:
            stmt(f"set {off_setting} = true")


def compare_all(title):
    out.append(f"# {title}: with and without the cost-based join ordering, the same rows.\n")
    for name, types, _, _ in CASES:
        cols = ', '.join(f"c{i}" for i in range(len(types.split(','))))
        a, b = f"{P}r_on_{name}", f"{P}r_off_{name}"
        grouped = lambda t: f"select {cols}, count(*) as n from {t} group by {cols}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


def clear_all():
    for name, _, _, _ in CASES:
        for sfx in ('off', 'on'):
            stmt(f"delete from {P}r_{sfx}_{name}")


out.append("""# Differential test of `batch_cost_based_join_ordering`: every query runs twice, with and without
# the cost-based join ordering, and both results must hold the same rows. The tables have very
# different sizes, so that the storage statistics lead to other join orders than the heuristic
# one (the results do not depend on the statistics being up to date).
#
# Generated by `python3 e2e_test/batch/cost_based_join_ordering.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {F} (id int primary key, d1_id int, d2_id int, d3_id int, v int, w int)")
stmt(f"create table {D1} (id int primary key, a int, k int)")
stmt(f"create table {D2} (id int primary key, b int, k int)")
stmt(f"create table {D3} (id int primary key, c int)")
stmt(f"create table {G} (id int primary key, x int, y int)")
stmt(f"create table {H} (id int primary key, x int, y int)")
stmt(f"create table {A} (x int, y int)")
stmt(f"create table {B} (x int, y int)")
stmt(f"create table {C} (x int, y int)")
stmt(f"create table {M} (k1 int, k2 int, val int, primary key (k1, k2))")
stmt(f"create table {N} (id int primary key, src varchar, n int)")
stmt(f"create table {W} (id int primary key, src varchar, body jsonb, done_at timestamp)")
for name, types, _, _ in CASES:
    cols = ', '.join(f"c{i} {t.strip()}" for i, t in enumerate(types.split(',')))
    for sfx in ('off', 'on'):
        stmt(f"create table {P}r_{sfx}_{name} ({cols})")

out.append("# Initial data: a large table, small and medium ones, NULLs and keys without match.\n")
stmt(f"insert into {F} select i, case when i % 11 = 0 then null else i % 25 end, i % 210, i % 6, i % 500, i % 13 "
     f"from generate_series(0, 2999) i")
stmt(f"insert into {D1} select i, case when i % 7 = 0 then null else i % 4 end, (i * 37) % 3000 "
     f"from generate_series(0, 19) i")
stmt(f"insert into {D2} select i, i % 10, i % 50 from generate_series(0, 199) i")
stmt(f"insert into {D3} select i, i from generate_series(0, 4) i")
stmt(f"insert into {G} select i, i % 20, i % 20 from generate_series(0, 499) i")
stmt(f"insert into {H} select i, i % 3, i * 20 from generate_series(0, 9) i")
stmt(f"insert into {A} select case when i % 50 = 0 then null else i % 100 end, i % 37 from generate_series(0, 1999) i")
stmt(f"insert into {B} select i % 37, i % 7 from generate_series(0, 199) i")
stmt(f"insert into {C} select i % 7, i from generate_series(0, 9) i")
stmt(f"insert into {M} select i / 5, i % 5, i from generate_series(0, 99) i")
stmt(f"insert into {N} select i, case when i % 97 = 0 then null else 's' || (i % 500) end, i % 8 "
     f"from generate_series(0, 2999) i")
stmt(f"insert into {W} select i, case when i % 89 = 0 then null else 's' || (i % 400) end, "
     f"jsonb_build_object('k', i, 'pad', repeat('x', 2000)), "
     f"'2026-01-01 00:00:00'::timestamp + i * interval '1 minute' from generate_series(0, 999) i")

run_all()
compare_all("Initial data")

out.append("# Spot checks: the queries return rows.\n")
for name in ('star', 'star4', 'chain', 'snowflake', 'twelve', 'two_way_non_key', 'wide_inner', 'wide_left',
             'wide_agg', 'wide_top1', 'wide_three', 'wide_semi', 'wide_anti'):
    query('T', f"select count(*) > 0 from {P}r_on_{name}", ["t"])

out.append("# Changes: more rows in the large table, deleted and NULL keys, duplicates.\n")
stmt(f"insert into {F} select i, i % 25, i % 210, i % 7, i % 500, i % 13 from generate_series(3000, 4999) i")
stmt(f"delete from {D1} where id in (3, 4)")
stmt(f"update {D2} set b = null where id % 9 = 0")
stmt(f"update {F} set d2_id = null where id % 17 = 0")
stmt(f"insert into {B} select i % 37, i % 7 from generate_series(0, 49) i")
stmt(f"delete from {A} where x < 10")
stmt(f"insert into {C} values (null, 100), (3, 101), (3, 102)")
stmt(f"insert into {D3} values (5, null), (6, 6)")
stmt(f"delete from {W} where id % 10 = 0")
stmt(f"insert into {W} select i, 's' || (i % 450), jsonb_build_object('k', i, 'pad', repeat('y', 1000)), "
     f"'2026-01-01 00:00:00'::timestamp + i * interval '1 minute' from generate_series(1000, 1499) i")
stmt(f"insert into {N} select i, 's' || (i % 700), i % 8 from generate_series(3000, 3999) i")

clear_all()
run_all()
compare_all("After the changes")

out.append("# Cleanup.\n")
for name, _, _, _ in CASES:
    for sfx in ('off', 'on'):
        stmt(f"drop table {P}r_{sfx}_{name}")
for t in (F, D1, D2, D3, G, H, A, B, C, M, N, W):
    stmt(f"drop table {t}")

with open(sys.argv[1], 'w') as f:
    f.write("\n".join(out))
