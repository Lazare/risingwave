"""Generates `e2e_test/streaming/prune_empty_relation.slt`, the differential test of the pruning
of the sub-plans proven empty in streaming plans: every query is materialized with and without
`streaming_prune_empty_relation`.

Usage: python3 e2e_test/streaming/prune_empty_relation.gen.py e2e_test/streaming/prune_empty_relation.slt
"""
import sys

T, SS, R = 'per_t', 'per_s', 'per_r'
EMPTY_T = f"(select * from {T} where v1 = 1 and v1 = 2)"
EMPTY_S = f"(select * from {SS} where w = 1 and w = 2)"

CASES = [
    # (name, comment, query)
    ('eq_eq', 'two different equalities', f"select v1, v2, v3 from {T} where v1 = 1 and v1 = 2"),
    ('null_in', 'is null and in-list', f"select v1, v3 from {T} where v3 is null and v3 in ('a', 'b')"),
    ('eq_null', 'comparison with a null constant', f"select v1, v2 from {T} where v1 = null"),
    ('range', 'empty range', f"select v1, v2 from {T} where v1 > 5 and v1 < 3"),
    ('bool', 'boolean column and its negation', f"select v1, b from {T} where b and not b"),
    ('in_notin', 'in-list and not in-list', f"select v1, v3 from {T} where v1 in (1, 2) and v1 not in (2, 1, 3)"),
    ('const_false', 'constant false conjunct', f"select v1, v2 from {T} where 1 = 0 and v1 > 1"),
    ('kept_eq_in', 'kept: satisfiable equality and in-list', f"select v1, v2 from {T} where v1 = 1 and v1 in (1, 2)"),
    ('kept_point', 'kept: point range', f"select v1, v2 from {T} where v1 >= 2 and v1 <= 2"),
    ('kept_open', 'kept: open range', f"select v1, v2 from {T} where v1 > 1 and v1 < 3"),
    ('kept_or', 'kept: contradiction under a disjunction', f"select v1, v2 from {T} where v1 is null or (v1 = 1 and v1 = 2)"),
    ('kept_in_null', 'kept: in-list with a null item', f"select v1, v2 from {T} where v1 in (1, null) and v1 = 1"),
    ('union_two', 'union all: one branch removed',
     f"select kind, v from (select 'a' as kind, v1 as v from {T} union all select 'b' as kind, k as v from {SS} "
     f"union all select 'c' as kind, k as v from {R}) u where kind = 'a' or kind = 'c'"),
    ('union_one', 'union all: a single branch left',
     f"select kind, v from (select 'a' as kind, v1 as v from {T} union all select 'b' as kind, k as v from {SS}) u where kind = 'b'"),
    ('union_none', 'union all: every branch removed',
     f"select kind, v from (select 'a' as kind, v1 as v from {T} union all select 'b' as kind, k as v from {SS}) u where kind = 'z'"),
    ('union_distinct', 'union distinct: one branch removed',
     f"select kind, v from (select 'a' as kind, v1 as v from {T} union select 'b' as kind, k as v from {SS}) u where kind = 'a'"),
    ('inner_empty', 'inner join: a side is empty',
     f"select t.v1 as tv, s.w as sw from {T} t join {SS} s on t.v1 = s.k where s.w is null and s.w = 1"),
    ('left_right_empty', 'left join: the right side is empty',
     f"select t.v1 as tv, t.v3 as tv3, s.w as sw, s.k as sk from {T} t left join {EMPTY_S} s on t.v1 = s.k"),
    ('left_left_empty', 'left join: the left side is empty',
     f"select t.v1 as tv, s.w as sw from {EMPTY_T} t left join {SS} s on t.v1 = s.k"),
    ('right_left_empty', 'right join: the left side is empty',
     f"select t.v1 as tv, s.w as sw, s.k as sk from {EMPTY_T} t right join {SS} s on t.v1 = s.k"),
    ('full_right_empty', 'full join: the right side is empty',
     f"select t.v1 as tv, t.v2 as tv2, s.w as sw from {T} t full join {EMPTY_S} s on t.v1 = s.k"),
    ('full_left_empty', 'full join: the left side is empty',
     f"select t.v1 as tv, s.w as sw, s.k as sk from {EMPTY_T} t full join {SS} s on t.v1 = s.k"),
    ('full_both_empty', 'full join: both sides are empty',
     f"select t.v1 as tv, s.w as sw from {EMPTY_T} t full join {EMPTY_S} s on t.v1 = s.k"),
    ('semi_empty', 'semi join: the right side is empty',
     f"select v1, v2 from {T} where v1 in (select k from {SS} where w = 1 and w = 2)"),
    ('anti_empty', 'anti join: the right side is empty',
     f"select v1, v3 from {T} t where not exists (select 1 from {SS} s where s.k = t.v1 and w = 1 and w = 2)"),
    ('inner_on_false', 'inner join: the condition is never true',
     f"select t.v1 as tv, s.w as sw from {T} t join {SS} s on t.v1 = s.k and t.v2 = 3 and t.v2 = 4"),
    ('left_on_false', 'left join: the condition is never true',
     f"select t.v1 as tv, t.v2 as tv2, s.w as sw from {T} t left join {SS} s on t.v1 = s.k and t.v2 = 3 and t.v2 = 4"),
    ('kept_full_on_false', 'kept: full join whose condition is never true',
     f"select t.v1 as tv, s.w as sw from {T} t full join {SS} s on t.v1 = s.k and t.v1 = 3 and t.v1 = 4"),
    ('three_way', 'three-way join: only the optional side is removed',
     f"select t.v1 as tv, s.w as sw, r.x as rx from {T} t join {SS} s on t.v1 = s.k "
     f"left join (select * from {R} where x is null and x > 0) r on s.k = r.k"),
    ('left_is_null', 'left join used as an anti join, with an empty right side',
     f"select t.v1 as tv, coalesce(s.w, -1) as sw from {T} t left join {EMPTY_S} s on t.v1 = s.k where s.k is null"),
    ('agg_group', 'agg with group key over an empty input',
     f"select v2, count(*) as cnt from {T} where v1 = 1 and v1 = 2 group by v2"),
    ('agg_simple', 'agg without group key over an empty input',
     f"select count(*) as c1, count(v1) as c2, sum(v1) as s1, min(v3) as m1, max(ts) as m2, avg(v2) as a1, "
     f"bool_and(b) as b1, string_agg(v3, ',' order by v3) as sa from {T} where v1 = 1 and v1 = 2"),
    ('agg_distinct', 'distinct agg without group key over an empty input',
     f"select count(distinct v1) as c, sum(distinct v2) as s from {T} where v1 = 1 and v1 = 2"),
    ('agg_join', 'agg without group key over a join with an empty side',
     f"select count(*) as c, max(t.v2) as m from {T} t join {EMPTY_S} s on t.v1 = s.k"),
    ('agg_kept', 'agg without group key over a non-empty input',
     f"select count(*) as c, sum(v1) as s from {T} where v1 >= 2 and v1 <= 2"),
    ('distinct', 'distinct over an empty input', f"select distinct v2 from {T} where v1 = 1 and v1 = 2"),
    ('topn', 'top-n over an empty input', f"select v1, v2 from {EMPTY_T} x order by v2 limit 3"),
    ('group_topn', 'group top-n over an empty input',
     f"select v1, v2, v3 from (select *, row_number() over (partition by v2 order by v3) as rn from {T} "
     f"where v1 = 1 and v1 = 2) x where rn = 1"),
    ('share_twice', 'shared empty relation used twice',
     f"with c as (select * from {T} where v1 = 1 and v1 = 2) select a.v1 as av, b.v2 as bv from c a join c b on a.v1 = b.v1"),
    ('share_one', 'shared relation with one empty consumer',
     f"with c as (select v1, v2 from {T}) select v1, v2 from c where v1 = 1 and v1 = 2 union all select v1, v2 from c where v2 = 7"),
    ('temporal', 'contradiction next to a temporal filter',
     f"select v1, ts from {T} where ts > now() - interval '1 hour' and v1 is null and v1 = 3"),
    # Constant propagation.
    ('const_conj', 'constant propagated into another conjunct', f"select v1, v2 from {T} where v1 = 2 and v2 > v1"),
    ('const_proj', 'constant propagated into a projection', f"select v1, v1 + v2 as total from {T} where v1 = 2"),
    ('const_eqcol', 'constant propagated through an equality between columns',
     f"select v1, v2 from {T} where v1 = 3 and v2 = v1 and v2 = 4"),
    ('const_case', 'constant column of a union branch in a case expression',
     f"select case when kind = 'a' then v else -v end as r, case when kind = 'b' then 1 when v > 0 then 2 else 3 end as c "
     f"from (select 'a' as kind, v1 as v from {T} union all select 'b' as kind, k as v from {SS}) u where kind = 'a'"),
    ('const_having', 'constant group key removed by a having clause',
     f"select kind, count(*) as cnt from (select 'a' as kind, v1 from {T}) x group by kind having kind = 'b'"),
    ('const_join', 'constant propagated through an inner join',
     f"select t.v1 as tv, s.w as sw, t.v2 + s.k as total from {T} t join {SS} s on t.v1 = s.k where t.v1 = 2"),
    ('const_left', 'constant of the optional side of a left join',
     f"select t.v1 as tv, s.w as sw, s.w + 1 as w1, s.w is null as missing from {T} t "
     f"left join (select * from {SS} where w = 1) s on t.v1 = s.k"),
    # Known nullability.
    ('null_count', 'is null on a count', f"select v1, c from (select v1, count(*) as c from {T} group by v1) x where c is null"),
    ('null_count_proj', 'is null on a count in a projection',
     f"select v1, c is null as missing, c is not null as present from (select v1, count(*) as c from {T} group by v1) x"),
    ('null_coalesce', 'is null on a coalesce with a constant',
     f"select c, v2 from (select coalesce(v1, 0) as c, v2 from {T}) x where c is null"),
    ('null_implied', 'is not null implied by a comparison', f"select v1, v2 from {T} where v1 > 0 and v1 is not null"),
    ('null_or', 'is null under a disjunction', f"select v1, v2 from {T} where (v1 is null or v2 = 3) and v1 > 0"),
    ('null_join_key', 'is null on a join key of an inner join',
     f"select t.v1 as tv, s.w as sw from {T} t join {SS} s on t.v1 = s.k where s.k is null or s.w = 1"),
    ('kept_anti_pattern', 'kept: is null on the optional side of a left join',
     f"select t.v1 as tv, t.v2 as tv2 from {T} t left join {SS} s on t.v1 = s.k where s.k is null"),
    ('kept_null_sum', 'kept: is null on a sum',
     f"select v1, total from (select v1, sum(v2) as total from {T} group by v1) x where total is null"),
    ('kept_null_col', 'kept: is null on a plain column', f"select v1, v2 is null as missing from {T} where v1 is null"),
    # Redundant conjuncts.
    ('bounds_lower', 'redundant lower bounds', f"select v1, v2 from {T} where v1 > 1 and v1 > 0 and v1 >= 1"),
    ('bounds_upper', 'redundant upper bounds', f"select v1, v3 from {T} where v1 < 5 and v1 <= 5 and v3 < 'm' and v3 < 'c'"),
    # Join with a relation made of one row of constants.
    ('row_inner', 'inner join with a constant row and a condition',
     f"select t.v1 as tv, t.v2 as tv2, c.y as cy from {T} t join (values (1, 2)) as c(x, y) on t.v1 = c.x and t.v2 > c.y"),
    ('kept_row_two', 'kept: join with two constant rows',
     f"select t.v1 as tv, c.x as cx from {T} t join (values (1), (2)) as c(x) on t.v1 = c.x"),
    ('kept_row_left', 'kept: left join with a constant row',
     f"select t.v1 as tv, c.x as cx from {T} t left join (values (1)) as c(x) on t.v1 = c.x"),
]
# Queries that streaming only accepts with the pruning (a join without equality): the
# materialized view is compared with the batch execution of its query.
ON_ONLY = [
    ('row_cross', 'cross join with a constant row',
     f"select t.v1 as tv, c.x as cx, c.y as cy from {T} t cross join (values (1, 'a')) as c(x, y)"),
    ('row_left_side', 'constant row on the left side',
     f"select c.x as cx, s.k as sk, s.w as sw from (values (7)) as c(x) join {SS} s on s.w < c.x"),
    ('row_agg', 'join with the constant row of an aggregation over an empty input',
     f"select t.v1 as tv, a.c as ac from {T} t cross join (select count(*) as c from {SS} where w = 1 and w = 2) a"),
]
# Materialized views on top of the materialized views above, with the same suffix.
DOWNSTREAM = [
    ('down_empty_agg', 'on top of an empty materialized view',
     "select v1, count(*) as cnt from per_eq_eq{sfx} group by v1"),
    ('down_empty_join', 'join with an empty materialized view',
     f"select t.v1 as tv, e.v2 as ev from {T} t left join per_eq_eq{{sfx}} e on t.v1 = e.v1"),
    ('down_padded', 'on top of a left join replaced with a projection',
     "select tv, count(sw) as cnt, count(*) as total from per_left_right_empty{sfx} group by tv"),
    ('down_const_row', 'on top of an aggregation replaced with its constant row',
     f"select t.v1 as tv, a.c1 as c1, a.s1 as s1 from {T} t join per_agg_simple{{sfx}} a on a.c1 = 0"),
]

out = []
def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"per_{name}_on", f"per_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_batch(name, sql):
    a = f"per_{name}_on"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from ({sql}) q) x),\n"
          f"  (select count(*) from (select * from ({sql}) q except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from ({sql}) q)",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without pruning, every materialized view holds the same rows.\n")
    for name, _, _ in CASES + DOWNSTREAM:
        compare(name)
    out.append(f"# {title}: the materialized views that need the pruning hold the rows of their query.\n")
    for name, _, sql in ON_ONLY:
        compare_batch(name, sql)

out.append("""# Differential test of `streaming_prune_empty_relation`: every query is materialized twice, with
# and without the pruning of the sub-plans proven empty, and both materialized views must hold
# the same rows after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/prune_empty_relation.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (v1 int, v2 int, v3 varchar, b boolean, ts timestamp)")
stmt(f"create table {SS} (k int, w int)")
stmt(f"create table {R} (k int, x int)")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {T} values (1, 3, 'a', true, '2026-01-01 00:00:00'), (2, 4, 'b', false, '2026-01-02 00:00:00'), "
     f"(2, 7, null, null, null), (null, 3, 'c', true, '2026-01-03 00:00:00'), (6, null, 'a', false, '2026-01-04 00:00:00')")
stmt(f"insert into {SS} values (1, 1), (2, 2), (2, null), (null, 5), (7, 1)")
stmt(f"insert into {R} values (1, 10), (2, null), (5, 0)")

for sfx in ('off', 'on'):
    stmt(f"set streaming_prune_empty_relation = {'true' if sfx == 'on' else 'false'}")
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view per_{name}_{sfx} as {sql}")
    for name, comment, sql in DOWNSTREAM:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view per_{name}_{sfx} as {sql.format(sfx='_' + sfx)}")

for name, comment, sql in ON_ONLY:
    out.append(f"# {comment}\n")
    stmt(f"create materialized view per_{name}_on as {sql}")

out.append("# `LIMIT 0` is rejected in streaming without pruning, and empty with it.\n")
stmt("set streaming_prune_empty_relation = false")
out.append(f"statement error LIMIT 0 in streaming mode\ncreate materialized view per_limit0_off as select v1, v2 from {T} order by v1 limit 0;\n")
stmt("set streaming_prune_empty_relation = true")
stmt(f"create materialized view per_limit0_on as select v1, v2 from {T} order by v1 limit 0")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('ITII', "select tv, tv3, sw, sk from per_left_right_empty_on order by tv, tv3",
      ["1 a NULL NULL", "2 b NULL NULL", "2 NULL NULL NULL", "6 a NULL NULL", "NULL c NULL NULL"])
query('IIITTRTT', "select * from per_agg_simple_on", ["0 0 NULL NULL NULL NULL NULL NULL"])
query('II', "select c, s from per_agg_distinct_on", ["0 NULL"])
query('II', "select c, m from per_agg_join_on", ["0 NULL"])
query('TI', "select kind, v from per_union_one_on order by v", ["b 1", "b 2", "b 2", "b 7", "b NULL"])
query('I', "select count(*) from per_eq_eq_on", ["0"])
query('I', "select count(*) from per_limit0_on", ["0"])
query('III', "select tv, c1, s1 from per_down_const_row_on order by tv", ["1 0 NULL", "2 0 NULL", "2 0 NULL", "6 0 NULL", "NULL 0 NULL"])

out.append("# First round of changes: new rows, some of which satisfy one conjunct of a pruned predicate.\n")
stmt(f"insert into {T} values (1, 4, 'b', false, '2026-02-01 00:00:00'), (2, 3, 'a', true, null), "
     f"(3, 7, 'z', null, '2026-02-02 00:00:00'), (null, null, null, null, null), (5, 3, 'c', true, '2026-02-03 00:00:00')")
stmt(f"insert into {SS} values (1, 2), (3, 1), (5, null), (6, 2)")
stmt(f"insert into {R} values (3, 1), (6, null), (1, -1)")
compare_all("After the inserts")

out.append("# Second round of changes: updates and deletes, which retract rows.\n")
stmt(f"update {T} set v2 = v2 + 1 where v1 = 1")
stmt(f"update {T} set v1 = 2, v3 = null where v1 = 3")
stmt(f"delete from {T} where v1 = 6")
stmt(f"delete from {T} where v1 is null and v2 is null")
stmt(f"update {SS} set w = 2 where w = 1")
stmt(f"delete from {SS} where k = 2")
stmt(f"update {R} set x = 5 where x is null")
stmt(f"delete from {R} where k = 1")
compare_all("After the updates and deletes")

query('I', "select count(*) from per_eq_eq_on", ["0"])
query('I', "select count(*) from per_limit0_on", ["0"])
query('II', "select c, m from per_agg_join_on", ["0 NULL"])

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(DOWNSTREAM):
        stmt(f"drop materialized view per_{name}_{sfx}")
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view per_{name}_{sfx}")
stmt("drop materialized view per_limit0_on")
for name, _, _ in reversed(ON_ONLY):
    stmt(f"drop materialized view per_{name}_on")
stmt(f"drop table {R}")
stmt(f"drop table {SS}")
stmt(f"drop table {T}")
stmt("set streaming_prune_empty_relation = false")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES) + len(DOWNSTREAM) + len(ON_ONLY), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
