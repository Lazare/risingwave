"""Generates `e2e_test/streaming/prune_empty_relation.slt`, the differential test of the pruning
of the sub-plans proven empty in streaming plans: every query is materialized with and without
`streaming_prune_empty_relation`.

Usage: python3 e2e_test/streaming/prune_empty_relation.gen.py e2e_test/streaming/prune_empty_relation.slt
"""
import sys

T, SS, R, N = 'per_t', 'per_s', 'per_r', 'per_n'
F, D, E = 'per_f', 'per_d', 'per_e'  # with a primary key
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
    # NOT NULL constraints of a table.
    ('nn_is_null', 'is null on a not null column', f"select a, b from {N} where a is null"),
    ('nn_proj', 'is null on not null columns in a projection',
     f"select a, a is null as m1, coalesce(a, 0) is null as m2, c is null as m3, b is null as m4 from {N}"),
    ('nn_not_null', 'is not null on a not null column', f"select a, b from {N} where a is not null and b > 1"),
    ('nn_default', 'is null on a not null column with a default', f"select a, c from {N} where c is null or a = 1"),
    ('nn_inner', 'is null on a not null column through an inner join',
     f"select t.v1 as tv, n.a as na from {T} t join {N} n on t.v1 = n.b where n.a is null or n.c is null"),
    ('kept_nn_left', 'kept: is null on a not null column of the optional side of a left join',
     f"select t.v1 as tv, n.a as na from {T} t left join {N} n on t.v1 = n.b where n.a is null"),
    # Unused outer join on a unique key.
    ('uoj', 'unused left join on the primary key of the right side',
     f"select f.id as fid, f.amt as amt from {F} f left join {D} d on f.dim_id = d.id"),
    ('uoj_extra', 'unused left join with an extra condition',
     f"select f.id as fid, f.amt as amt from {F} f left join {D} d on f.dim_id = d.id and d.grp > 1 and f.amt > 0"),
    ('uoj_right', 'unused right join',
     f"select f.id as fid, f.amt as amt from {D} d right join {F} f on f.dim_id = d.id"),
    ('uoj_const', 'unused left join whose right key is bound by a constant',
     f"select f.id as fid, f.amt as amt from {F} f left join {D} d on d.id = 2 and f.amt > d.grp"),
    ('uoj_two', 'two left joins, only the unused one is removed',
     f"select f.id as fid, e.code as code from {F} f left join {D} d on f.dim_id = d.id left join {E} e on f.amt = e.id"),
    ('uoj_dedup', 'unused left join on a deduplicated right side',
     f"select f.id as fid, f.amt as amt from {F} f left join (select * from (select *, row_number() over "
     f"(partition by grp order by id) as rn from {D}) x where rn = 1) latest on f.dim_id = latest.grp"),
    ('uoj_agg', 'unused left join on an aggregated right side',
     f"select f.id as fid, f.amt as amt from {F} f left join (select grp, count(*) as cnt from {D} group by grp) g "
     f"on f.dim_id = g.grp"),
    ('uoj_under_agg', 'unused left join under an aggregation',
     f"select f.dim_id as dim_id, sum(f.amt) as total from {F} f left join {D} d on f.dim_id = d.id group by f.dim_id"),
    ('kept_uoj_used', 'kept: left join whose right side is used',
     f"select f.id as fid, d.name as name from {F} f left join {D} d on f.dim_id = d.id"),
    ('kept_uoj_anti', 'kept: left join whose right side is used in a filter above',
     f"select f.id as fid, f.amt as amt from {F} f left join {D} d on f.dim_id = d.id where d.id is null"),
    ('kept_uoj_not_key', 'kept: unused left join on a column that is not a key',
     f"select f.id as fid, f.amt as amt from {F} f left join {D} d on f.dim_id = d.grp"),
    ('kept_uoj_inner', 'kept: unused inner join',
     f"select f.id as fid, f.amt as amt from {F} f join {D} d on f.dim_id = d.id"),
    # Aggregation, distinct, top-n on a key.
    ('key_agg', 'aggregation on the primary key',
     f"select id, sum(amt) as total, count(*) as cnt, count(dim_id) as cnt_dim, min(label) as first_label, "
     f"max(amt) as max_amt, sum(price) as total_price from {F} group by id"),
    ('key_agg_filter', 'aggregation with filters on the primary key',
     f"select id, count(*) filter (where amt > 5) as cnt, sum(amt) filter (where dim_id is not null) as total "
     f"from {F} group by id"),
    ('key_avg', 'average on the primary key', f"select id, avg(amt) as average from {F} group by id"),
    ('key_agg_larger', 'aggregation on a group key larger than the primary key',
     f"select id, dim_id, sum(amt) as total, bool_and(amt > 3) as all_big from {F} group by id, dim_id"),
    ('key_distinct', 'distinct on columns that contain the primary key', f"select distinct id, amt from {F}"),
    ('key_distinct_agg', 'distinct on the output of an aggregation',
     f"select distinct grp, cnt from (select grp, count(*) as cnt from {D} group by grp) g"),
    ('key_agg_join', 'aggregation above a left join with a unique right side',
     f"select f.id as fid, d.name as name, sum(f.amt) as total from {F} f left join {D} d on f.dim_id = d.id "
     f"group by f.id, d.name"),
    ('key_top1', 'top-1 per primary key',
     f"select id, amt from (select *, row_number() over (partition by id order by amt) as rn from {F}) x where rn = 1"),
    ('key_top1_twice', 'top-1 over a top-1 on the same key',
     f"with latest as (select * from (select *, row_number() over (partition by grp order by id desc) as rn "
     f"from {D}) x where rn = 1) select grp, name from (select *, row_number() over (partition by grp order by name) "
     f"as rn2 from latest) y where rn2 = 1"),
    ('kept_key_agg', 'kept: aggregation on a column that is not a key',
     f"select dim_id, sum(amt) as total, count(*) as cnt from {F} group by dim_id"),
    ('kept_key_distinct', 'kept: distinct without the primary key', f"select distinct dim_id, amt from {F}"),
    ('kept_key_array', 'kept: aggregate without a one-row form', f"select id, array_agg(amt) as amounts from {F} group by id"),
    ('kept_key_join', 'kept: aggregation above an inner join that multiplies rows',
     f"select f.id as fid, sum(d.id) as total from {F} f join {D} d on f.dim_id = d.grp group by f.id"),
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
def switch(on):
    value = 'true' if on else 'false'
    stmt(f"set streaming_prune_empty_relation = {value}")
    stmt(f"set streaming_simplify_on_unique_key = {value}")

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

out.append("""# Differential test of `streaming_prune_empty_relation` and `streaming_simplify_on_unique_key`:
# every query is materialized twice, with and without the pruning of its plan, and both
# materialized views must hold the same rows after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/prune_empty_relation.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (v1 int, v2 int, v3 varchar, b boolean, ts timestamp)")
stmt(f"create table {SS} (k int, w int)")
stmt(f"create table {R} (k int, x int)")
stmt(f"create table {N} (a int not null, b int, c int not null default 5)")
stmt(f"create table {F} (id int primary key, dim_id int, amt int, label varchar, price decimal)")
stmt(f"create table {D} (id int primary key, name varchar, grp int)")
stmt(f"create table {E} (id int primary key, code varchar)")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {T} values (1, 3, 'a', true, '2026-01-01 00:00:00'), (2, 4, 'b', false, '2026-01-02 00:00:00'), "
     f"(2, 7, null, null, null), (null, 3, 'c', true, '2026-01-03 00:00:00'), (6, null, 'a', false, '2026-01-04 00:00:00')")
stmt(f"insert into {SS} values (1, 1), (2, 2), (2, null), (null, 5), (7, 1)")
stmt(f"insert into {R} values (1, 10), (2, null), (5, 0)")
stmt(f"insert into {N} (a, b) values (1, 1), (2, null), (3, 2)")
stmt(f"insert into {D} values (1, 'one', 1), (2, 'two', 1), (3, 'three', 2), (4, null, null)")
stmt(f"insert into {E} values (3, 'c'), (5, 'e')")
stmt(f"insert into {F} values (10, 1, 5, 'x', 1.5), (11, 1, 7, 'y', 2.50), (12, 3, 3, null, null), "
     f"(13, null, 9, 'z', 0), (14, 99, null, 'w', 7), (15, 2, 3, 'x', 1.50)")
out.append("# A table rejects the rows that violate its `NOT NULL` constraints.\n")
out.append(f"statement error null value in column\ninsert into {N} (a, b) values (null, 1);\n")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
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
switch(False)
out.append(f"statement error LIMIT 0 in streaming mode\ncreate materialized view per_limit0_off as select v1, v2 from {T} order by v1 limit 0;\n")
switch(True)
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
stmt(f"insert into {N} values (4, 2, 6), (5, 6, 7), (6, null, 8)")
stmt(f"insert into {D} values (5, 'five', 2), (99, 'far', 3)")
stmt(f"insert into {E} values (7, 'g'), (9, null)")
stmt(f"insert into {F} values (16, 5, 7, 'v', 3), (17, 4, 1, null, 2.25), (18, 1, 5, 'x', null)")
out.append("# A row written again with an existing primary key replaces the previous one.\n")
stmt(f"insert into {F} values (10, 2, 6, 'x2', 9)")
stmt(f"insert into {D} values (3, 'trois', 1)")
stmt(f"insert into {N} (a, b) select v1, v2 from {T} where v1 is not null and v2 is not null")
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
stmt(f"update {N} set b = b + 1, c = c + 1 where a = 1")
stmt(f"delete from {N} where a = 2")
stmt(f"update {F} set amt = amt + 2, dim_id = 3 where id = 11")
stmt(f"update {F} set dim_id = null where id = 15")
stmt(f"delete from {F} where id = 12")
stmt(f"update {D} set grp = 2, name = 'uno' where id = 1")
stmt(f"delete from {D} where id = 2")
stmt(f"delete from {D} where id = 99")
stmt(f"update {E} set code = 'cc' where id = 3")
stmt(f"delete from {E} where id = 7")
compare_all("After the updates and deletes")

query('I', "select count(*) from per_eq_eq_on", ["0"])
query('I', "select count(*) from per_limit0_on", ["0"])
query('II', "select c, m from per_agg_join_on", ["0 NULL"])
query('I', "select count(*) from per_nn_is_null_on", ["0"])

out.append("# A `NOT NULL` column added later: the rows written before read its default value.\n")
stmt(f"alter table {N} add column e int not null default 9")
ADDED = f"select a, e, e is null as missing from {N} where e is null or a = 1"
switch(False)
stmt(f"create materialized view per_nn_added_off as {ADDED}")
switch(True)
stmt(f"create materialized view per_nn_added_on as {ADDED}")
compare('nn_added')
stmt(f"insert into {N} (a, b, e) values (1, 1, 4), (7, 7, 7)")
stmt(f"insert into {N} (a, b) values (1, 5)")
compare('nn_added')
query('IIT', "select a, e, missing from per_nn_added_on order by e", ["1 4 f", "1 9 f", "1 9 f", "1 9 f", "1 9 f"])

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(DOWNSTREAM):
        stmt(f"drop materialized view per_{name}_{sfx}")
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view per_{name}_{sfx}")
stmt("drop materialized view per_nn_added_on")
stmt("drop materialized view per_nn_added_off")
stmt("drop materialized view per_limit0_on")
for name, _, _ in reversed(ON_ONLY):
    stmt(f"drop materialized view per_{name}_on")
stmt(f"drop table {E}")
stmt(f"drop table {D}")
stmt(f"drop table {F}")
stmt(f"drop table {N}")
stmt(f"drop table {R}")
stmt(f"drop table {SS}")
stmt(f"drop table {T}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES) + len(DOWNSTREAM) + len(ON_ONLY), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
