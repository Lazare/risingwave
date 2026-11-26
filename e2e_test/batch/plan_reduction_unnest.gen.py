"""Generates `e2e_test/batch/plan_reduction_unnest.slt`, the differential test of the batch
counterparts of stages of the streaming plan reduction prototype: every query is run as a batch
`INSERT ... SELECT` with and without its variable, and both results must hold the same rows (as
multisets), after each round of changes of the data.

- `batch_simplify_subquery_unnesting`: correlated subqueries that aggregate, without a domain;
- `batch_unnest_lateral_functions`: subqueries over set-returning functions of the outer row
  evaluated per row, and a table joined back on its key with an aggregation of itself;
- `batch_factor_union_join`: the join common to the branches of a `UNION ALL`;
- `batch_merge_union_scans`: the table read by several branches of a `UNION ALL`.

Usage: python3 e2e_test/batch/plan_reduction_unnest.gen.py e2e_test/batch/plan_reduction_unnest.slt
"""
import sys

P, R, Q, U = 'bpu_p', 'bpu_r', 'bpu_q', 'bpu_u'
T = 'bpu_t'
F, D, E, G = 'bpu_f', 'bpu_d', 'bpu_e', 'bpu_g'
DAGG, DJ, DNAMES = 'bpu_dagg', 'bpu_dj', 'bpu_dnames'
M, MW = 'bpu_m', 'bpu_mw'

UNNEST = 'batch_simplify_subquery_unnesting'
LATERAL = 'batch_unnest_lateral_functions'
UNION = 'batch_factor_union_join'
SCANS = 'batch_merge_union_scans'
VARIABLES = [UNNEST, LATERAL, UNION, SCANS]
# The prefix of the result tables of the queries of each variable.
PREFIX = {UNNEST: 'un', LATERAL: 'lf', UNION: 'uj', SCANS: 'us'}


def result(variable, name, sfx):
    return f"bpu_{PREFIX[variable]}_{name}_{sfx}"

# (variable, name, comment, columns compared, query). The columns compared are expressions over
# the output columns of the query: an array whose order is not defined by the query is sorted.
CASES = [
    # `batch_simplify_subquery_unnesting`: rewritten.
    (UNNEST, 'exists_select', 'EXISTS in the select list', ['id', 'e'],
     f"select p.id, exists (select 1 from {R} r where r.pid = p.id and r.s = 'x') as e from {P} p"),
    (UNNEST, 'not_exists_select', 'NOT EXISTS in the select list', ['id', 'e'],
     f"select p.id, not exists (select 1 from {R} r where r.pid = p.id) as e from {P} p"),
    (UNNEST, 'exists_or', 'EXISTS under OR in WHERE', ['id', 'v'],
     f"select p.id, p.v from {P} p where p.v > 2 or exists (select 1 from {R} r where r.pid = p.id)"),
    (UNNEST, 'exists_case', 'EXISTS in a CASE, correlated on a nullable column that is not a key', ['id', 'c'],
     f"select p.id, case when exists (select 1 from {R} r where r.pid = p.v) then 'y' else 'n' end as c from {P} p"),
    (UNNEST, 'two_not_exists', 'two NOT EXISTS joined by AND', ['id', 'flag'],
     f"select p.id, not exists (select 1 from {R} r where r.s = 'x' and r.pid = p.id) "
     f"and not exists (select 1 from {Q} q where q.pid = p.id) as flag from {P} p"),
    (UNNEST, 'nokey', 'outer side without a key, with duplicates and NULLs', ['k', 'v', 'e'],
     f"select u.k, u.v, exists (select 1 from {R} r where r.pid = u.k) as e from {U} u"),
    (UNNEST, 'min', 'scalar min', ['id', 'm'],
     f"select p.id, (select min(r.x) from {R} r where r.pid = p.id) as m from {P} p"),
    (UNNEST, 'count', 'scalar count', ['id', 'c'],
     f"select p.id, (select count(*) from {R} r where r.pid = p.id) as c from {P} p"),
    (UNNEST, 'sum_count', 'sum, count of a column and count of distinct values in two subqueries', ['id', 's', 'c'],
     f"select p.id, (select sum(r.x) from {R} r where r.pid = p.id) as s, "
     f"(select count(r.x) + count(distinct r.s) from {R} r where r.pid = p.id) as c from {P} p"),
    (UNNEST, 'avg_bool', 'avg and bool_or', ['id', 'a', 'b'],
     f"select p.id, (select avg(r.x) from {R} r where r.pid = p.id) as a, "
     f"(select bool_or(r.s = 'x') from {R} r where r.pid = p.id) as b from {P} p"),
    (UNNEST, 'array', 'ARRAY subquery, an empty array when nothing matches', ['id', 'xs::varchar'],
     f"select p.id, array(select r.x from {R} r where r.pid = p.id order by r.x) as xs from {P} p"),
    (UNNEST, 'string_agg', 'string_agg with a default when nothing matches', ['id', 'ss'],
     f"select p.id, coalesce((select string_agg(r.s, ',' order by r.s) from {R} r where r.pid = p.id), '-') as ss from {P} p"),
    (UNNEST, 'where_count', 'count compared in WHERE', ['id'],
     f"select p.id from {P} p where p.g = (select count(*) from {R} r where r.pid = p.id)"),
    (UNNEST, 'two_cols', 'correlated on two columns', ['id', 'c'],
     f"select p.id, (select count(*) from {R} r where r.pid = p.id and r.x = p.v) as c from {P} p"),
    (UNNEST, 'filter_clause', 'aggregate with a filter clause', ['id', 'c'],
     f"select p.id, (select count(*) filter (where r.x > 1) from {R} r where r.pid = p.id) as c from {P} p"),
    (UNNEST, 'count_expr', 'expression of a count that is not NULL over an empty input', ['id', 'c'],
     f"select p.id, (select count(*) * 10 + 1 from {R} r where r.pid = p.id) as c from {P} p"),
    (UNNEST, 'kept_corr_arg', 'kept: correlated column in the aggregated expression', ['id', 'm'],
     f"select p.id, (select max(r.x + p.v) from {R} r where r.pid = p.id) as m from {P} p"),
    (UNNEST, 'kept_not_distinct', 'kept: IS NOT DISTINCT FROM written in the subquery', ['id', 'v', 'c'],
     f"select p.id, p.v, (select count(*) from {R} r where r.pid is not distinct from p.v) as c from {P} p"),

    # `batch_unnest_lateral_functions`: set-returning functions of the outer row.
    (LATERAL, 'regexp_left', 'two LEFT JOIN LATERAL over regexp_matches', ['id', 'x', 'y'],
     f"select t.id, a.x, b.y from {T} t "
     f"left join lateral (select (regexp_matches(t.s, 'A([0-9]+)'))[1] as x) as a on true "
     f"left join lateral (select (regexp_matches(t.s, 'B([0-9]+)'))[1] as y) as b on true"),
    (LATERAL, 'json_text_left', 'LEFT JOIN of a table function of the outer row', ['id', 'x'],
     f"select t.id, e.x from {T} t left join jsonb_array_elements_text(t.j -> 'xs') as e(x) on true"),
    (LATERAL, 'ordinality', 'LEFT JOIN of unnest WITH ORDINALITY', ['id', 'v', 'o'],
     f"select t.id, x.v, x.o from {T} t left join unnest(t.arr) with ordinality as x(v, o) on true"),
    (LATERAL, 'const_col', 'LEFT JOIN LATERAL with a constant column, NULL on the padded row', ['id', 'one', 'v'],
     f"select t.id, l.one, l.v from {T} t left join lateral (select 1 as one, unnest(t.arr) as v) as l on true"),
    (LATERAL, 'series', 'LEFT JOIN of generate_series bounded by a column of the outer row', ['id', 'i'],
     f"select t.id, g.i from {T} t left join generate_series(1, t.n) as g(i) on true"),
    (LATERAL, 'array_of', 'ARRAY of a set-returning function of the outer row', ['id', 'array_sort(xs)::varchar'],
     f"select t.id, array(select jsonb_array_elements_text(coalesce(t.j -> 'xs', '[]'::jsonb))) as xs from {T} t"),
    (LATERAL, 'coalesce_array', 'COALESCE of an ARRAY of a set-returning function', ['id', 'array_sort(xs)::varchar'],
     f"select t.id, coalesce(array(select jsonb_array_elements_text(t.j -> 'xs')), array['none']) as xs from {T} t"),
    (LATERAL, 'max_filter', 'scalar max over a set-returning function, with a filter on the outer side', ['id', 'm'],
     f"select t.id, (select max(x) from jsonb_array_elements_text(t.j -> 'xs') as x) as m from {T} t where t.c = 'c'"),
    (LATERAL, 'sum', 'scalar sum, NULL without non-NULL values', ['id', 'total'],
     f"select t.id, (select sum((e ->> 'a')::bigint) from jsonb_array_elements(t.j -> 'ys') as e) as total from {T} t"),
    (LATERAL, 'count_exists', 'count and EXISTS over unnest in one select list', ['id', 'c', 'e'],
     f"select t.id, (select count(*) from unnest(t.arr) as v) as c, "
     f"exists (select 1 from unnest(t.arr) as v) as e from {T} t where t.arr is not null"),
    (LATERAL, 'string_agg', 'string_agg with ORDER BY over the keys of an object of the outer row', ['id', 'ks'],
     f"select t.id, (select string_agg(k, ',' order by k) from jsonb_object_keys(t.j -> 'obj') as k) as ks from {T} t"),
    (LATERAL, 'corr_arg', 'correlated column in the aggregated expression', ['id', 'm'],
     f"select t.id, (select max(v + t.n) from unnest(t.arr) as v) as m from {T} t"),
    (LATERAL, 'nokey', 'outer side without a key of its own, with duplicates', ['k', 'm'],
     f"select u.k, (select max(v) from unnest(u.arr) as v) as m from {U} u"),
    (LATERAL, 'unnest_array', 'unnest of a COALESCE of an ARRAY of a set-returning function', ['id', 'v'],
     f"select t1.id, x.v from (select id, coalesce(array(select jsonb_array_elements_text(coalesce(j -> 'xs', '[]'::jsonb))), "
     f"array[]::text[]) as arr from {T}) as t1 join unnest(t1.arr) as x(v) on true"),
    (LATERAL, 'window', 'running sum over unnest WITH ORDINALITY in a lateral subquery', ['id', 'v', 'run'],
     f"select t.id, x.v, x.run from {T} t cross join lateral (select v, sum(v) over (order by o rows unbounded preceding) as run "
     f"from unnest(t.arr) with ordinality as w(v, o)) as x"),
    (LATERAL, 'kept_union', 'kept: the outer side is a UNION ALL that repeats the keys', ['id', 'm'],
     f"select x.id, (select max(v) from unnest(x.arr) as v) as m from (select id, arr from {T} union all select id, arr from {T}) as x"),
    (LATERAL, 'self_max', 'self aggregate: max over a function of the row, with a filter on the outer side', ['id', 'm'],
     f"select t.id, a.m from {T} t left join (select t2.id, max(x) as m from {T} t2, jsonb_array_elements_text(t2.j -> 'xs') as x "
     f"group by t2.id) a on t.id = a.id where t.c = 'c'"),
    (LATERAL, 'self_sum', 'self aggregate: sum, NULL without non-NULL values', ['id', 'total'],
     f"select t.id, s.total from {T} t left join (select t.id, sum((e ->> 'x')::bigint) as total from {T} t "
     f"cross join jsonb_array_elements(t.j -> 'ys') as e group by t.id) s on t.id = s.id"),
    (LATERAL, 'self_child', 'self aggregate: inner join with a child table inside, and a test of the match', ['id', 'n', 'sx', 'matched'],
     f"select t.id, t.n, s.sx, s.id is not null as matched from {T} t left join (select t.id, sum(r.x) as sx from {T} t "
     f"join {R} r on t.id = r.pid group by t.id) s on t.id = s.id"),
    (LATERAL, 'self_inner', 'self aggregate: inner join, with a condition on the rows of the function', ['id', 'cnt'],
     f"select t.id, s.cnt from {T} t join (select t.id, count(*) as cnt from {T} t, unnest(t.arr) as v where v > 1 "
     f"group by t.id) s on t.id = s.id"),

    # `batch_factor_union_join`.
    (UNION, 'inner', 'two branches with the same inner join', ['id', 'name'],
     f"select f.id, d.name from {F} f join {D} d on f.dim_id = d.id "
     f"union all select e.id, d.name from {E} e join {D} d on e.rid = d.id"),
    (UNION, 'left_proj', 'left joins with different projections above', ['id', 'txt', 'n'],
     f"select f.id, f.label as txt, least(f.amt, d.grp) as n from {F} f left join {D} d on f.dim_id = d.id "
     f"union all select e.id, e.code, d.grp from {E} e left join {D} d on e.rid = d.id"),
    (UNION, 'inner_filters', 'inner joins with a different filter of the joined table in each branch', ['id', 'name'],
     f"select f.id, d.name from {F} f join {D} d on f.dim_id = d.id where d.active "
     f"union all select e.id, d.name from {E} e join {D} d on e.rid = d.id where d.grp = 2"),
    (UNION, 'left_filtered', 'left joins with a filtered joined table in one branch only', ['id', 'name'],
     f"select f.id, d.name from {F} f left join (select * from {D} where active) d on f.dim_id = d.id "
     f"union all select e.id, d.name from {E} e left join {D} d on e.rid = d.id"),
    (UNION, 'left_extra', 'left joins with a different extra condition in each branch', ['id', 'name'],
     f"select f.id, d.name from {F} f left join {D} d on f.dim_id = d.id and f.amt > d.grp "
     f"union all select e.id, d.name from {E} e left join {D} d on e.rid = d.id and d.name <> e.code"),
    (UNION, 'filters_above', 'filters above the joins', ['id', 'name'],
     f"select f.id, d.name from {F} f left join {D} d on f.dim_id = d.id where f.amt > 3 or d.name is null "
     f"union all select e.id, d.name from {E} e left join {D} d on e.rid = d.id where e.code is not null"),
    (UNION, 'semi', 'semi joins', ['id', 'label'],
     f"select f.id, f.label from {F} f where exists (select 1 from {D} d where d.id = f.dim_id) "
     f"union all select e.id, e.code from {E} e where exists (select 1 from {D} d where d.id = e.rid)"),
    (UNION, 'anti', 'anti joins with a different filter of the joined table', ['id', 'label'],
     f"select f.id, f.label from {F} f where not exists (select 1 from {D} d where d.id = f.dim_id and d.active) "
     f"union all select e.id, e.code from {E} e where not exists (select 1 from {D} d where d.id = e.rid)"),
    (UNION, 'non_key', 'join on a column of the joined table that is not its key', ['id', 'name'],
     f"select f.id, d.name from {F} f join {D} d on f.amt = d.grp "
     f"union all select e.id, d.name from {E} e join {D} d on e.rid = d.grp"),
    (UNION, 'three', 'three branches, two of them share their join', ['id', 'name', 'src'],
     f"select f.id, d.name, 'f' as src from {F} f join {D} d on f.dim_id = d.id "
     f"union all select g.id, e.code, 'g' from {G} g join {E} e on g.a = e.id "
     f"union all select e.id, d.name, 'e' from {E} e join {D} d on e.rid = d.id"),
    (UNION, 'two_cols', 'joins on two columns', ['id', 'active'],
     f"select f.id, d.active from {F} f join {D} d on f.dim_id = d.id and f.amt = d.grp "
     f"union all select g.id, d.active from {G} g join {D} d on g.b = d.grp and g.a = d.id"),
    (UNION, 'derived_agg', 'derived side: an aggregating view, left joins', ['id', 'cnt', 'mx'],
     f"select f.id, v.cnt, v.mx from {F} f left join {DAGG} v on f.amt = v.grp "
     f"union all select e.id, v.cnt, e.code from {E} e left join {DAGG} v on e.rid = v.grp"),
    (UNION, 'derived_join', 'derived side: a view with a join, inner joins', ['id', 'name', 'a'],
     f"select f.id, v.name, v.a from {F} f join {DJ} v on f.dim_id = v.id "
     f"union all select e.id, v.name, v.a from {E} e join {DJ} v on e.rid = v.id"),
    (UNION, 'kept_ordered_agg', 'kept: an aggregate that depends on the order of the rows', ['id', 'names'],
     f"select f.id, v.names from {F} f join {DNAMES} v on f.amt = v.grp "
     f"union all select e.id, v.names from {E} e join {DNAMES} v on e.rid = v.grp"),

    # `batch_merge_union_scans`.
    (SCANS, 'two', 'two branches with a different filter and projection', ['a', 'x'],
     f"select a, x from {M} where a > 0 union all select b, y from {M} where b > 0"),
    (SCANS, 'flag', 'exclusive filters on a boolean column, constants per branch', ['k', 's', 'p'],
     f"select k, 'yes' as s, 5 as p from {M} where flag union all select k, 'no', 4 from {M} where not flag"),
    (SCANS, 'roles', 'three branches without a filter', ['k', 'role'],
     f"select k, 'r1' as role from {M} union all select k, 'r2' from {M} union all select k, 'r3' from {M}"),
    (SCANS, 'one_filtered', 'one branch without a filter and one with a filter', ['k', 'st', 'p'],
     f"select k, 'all' as st, 1 as p from {M} union all select k, 'nulls', 2 from {M} where a is null"),
    (SCANS, 'common', 'a conjunct common to every branch', ['k', 'a'],
     f"select k, a from {M} where a > 0 and b > 1 union all select k, b from {M} where b < 5 and a > 0"),
    (SCANS, 'dup_branch', 'the same branch twice keeps its duplicates', ['k', 'a'],
     f"select k, a from {M} where a > 0 union all select k, a from {M} where a > 0"),
    (SCANS, 'two_tables', 'two tables, two branches each', ['k', 'a'],
     f"select k, a from {M} where a > 0 union all select id, v from {P} where v > 0 "
     f"union all select k, b from {M} where b > 0 union all select id, g from {P} where g < 1"),
    (SCANS, 'computed', 'computed columns differ between branches', ['k', 'v', 'kind'],
     f"select k, greatest(a, 0) as v, 'x' as kind from {M} where x = 'A' union all select k, b + 1, 'y' from {M} where x = 'B'"),
    (SCANS, 'guarded', 'an expression that fails on the rows its branch never sees', ['k', 'q'],
     f"select k, 10 / a as q from {M} where a <> 0 union all select k, b from {M} where a = 0"),
    (SCANS, 'nokey', 'a table without a key, with duplicates', ['k', 'v'],
     f"select k, v from {U} where v > 1 union all select k, v from {U} where v is null"),
    (SCANS, 'watermark', 'a table with a watermark', ['k', 'v'],
     f"select k, v from {MW} where v > 0 union all select k, -v from {MW} where v < 0"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def grouped(table, columns):
    exprs = ", ".join(f"{c} as c{i}" for i, c in enumerate(columns))
    keys = ", ".join(f"c{i}" for i in range(len(columns)))
    return f"select {keys}, count(*) as n from (select {exprs} from {table}) z group by {keys}"


def run_all(title):
    out.append(f"# {title}: every query with and without its variable.\n")
    for variable, name, comment, columns, sql in CASES:
        on, off = result(variable, name, 'on'), result(variable, name, 'off')
        out.append(f"# {comment} (`{variable}`)\n")
        stmt(f"delete from {off}")
        stmt(f"delete from {on}")
        stmt(f"set {variable} = false")
        stmt(f"insert into {off} {sql}")
        stmt(f"set {variable} = true")
        stmt(f"insert into {on} {sql}")
        stmt(f"set {variable} = false")
        query('III',
              f"select\n  (select count(*) from ({grouped(on, columns)} except {grouped(off, columns)}) x),\n"
              f"  (select count(*) from ({grouped(off, columns)} except {grouped(on, columns)}) y),\n"
              f"  (select count(*) from {on}) - (select count(*) from {off})",
              ["0 0 0"])


# The result tables of every case have their own names.
assert len({result(variable, name, 'on') for variable, name, _, _, _ in CASES}) == len(CASES)

out.append("""# Differential test of the batch counterparts of stages of the streaming plan reduction prototype
# (`batch_simplify_subquery_unnesting`, `batch_unnest_lateral_functions`, `batch_factor_union_join`,
# `batch_merge_union_scans`): every query is run as a batch `INSERT ... SELECT` with and without its
# variable, and both results must hold the same rows, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/plan_reduction_unnest.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
for variable in VARIABLES:
    stmt(f"set {variable} = false")
stmt(f"create table {P} (id int primary key, v int, g int)")
stmt(f"create table {R} (id int primary key, pid int, s varchar, x int)")
stmt(f"create table {Q} (id int primary key, pid int)")
stmt(f"create table {U} (k int, v int, arr int[])")
stmt(f"create table {T} (id int primary key, s varchar, c varchar, j jsonb, arr int[], n int)")
stmt(f"create table {F} (id int primary key, dim_id int, other_id int, amt int, label varchar)")
stmt(f"create table {D} (id int primary key, name varchar, grp int, active boolean)")
stmt(f"create table {E} (id int primary key, code varchar, rid int)")
stmt(f"create table {G} (id int primary key, a int, b int)")
stmt(f"create view {DAGG} as select grp, count(*) as cnt, max(name) as mx from {D} group by grp")
stmt(f"create view {DJ} as select d.id, d.name, d.grp, g.a from {D} d join {G} g on d.grp = g.b")
stmt(f"create view {DNAMES} as select grp, string_agg(name, ',' order by name) as names from {D} group by grp")
stmt(f"create table {M} (k int primary key, a int, b int, x varchar, y varchar, flag boolean)")
stmt(f"create table {MW} (k int, v int, ts timestamp, watermark for ts as ts - interval '1' second) append only")

out.append("# The result tables: one per query and setting, with the columns of the query.\n")
for variable, name, _, _, sql in CASES:
    for sfx in ('off', 'on'):
        stmt(f"create table {result(variable, name, sfx)} as {sql} limit 0")

out.append("# First data: NULLs, duplicates, outer rows without a match.\n")
stmt(f"insert into {P} values (1, 1, 2), (2, 2, 0), (3, null, 1), (4, 1, null), (5, 5, 0), (6, 3, 3)")
stmt(f"insert into {R} values (10, 1, 'x', 1), (11, 1, 'y', 2), (12, 1, 'x', null), (13, 2, null, 2), "
     f"(14, 3, 'x', 5), (15, null, 'x', 1), (16, 9, 'z', 9), (17, 3, 'z', null)")
stmt(f"insert into {Q} values (20, 1), (21, 4), (22, null), (23, 4)")
stmt(f"insert into {U} values (1, 1, '{{1,2}}'), (1, 1, '{{1,2}}'), (2, null, null), (null, 3, '{{}}'), "
     f"(7, 0, '{{3,NULL,3}}'), (null, null, '{{5}}')")
stmt(f"""insert into {T} values
  (1, 'A1 B2 A3', 'c', '{{"xs": ["a", "b"], "ys": [{{"a": 1, "x": 2}}, {{"a": 2}}], "obj": {{"k1": 1, "k2": 2}}}}', '{{1,2,3}}', 2),
  (2, 'none', 'c', '{{"xs": [], "ys": [], "obj": {{}}}}', '{{}}', 0),
  (3, null, 'K', null, null, null),
  (4, 'A7', 'K', '{{"xs": ["z", null, "z"], "ys": [{{"x": 5}}], "obj": {{"only": true}}}}', '{{NULL,5}}', 3),
  (5, 'B9 B8', 'c', '{{"ys": [{{"b": 1}}, {{"a": 3}}]}}', '{{2,2}}', -1),
  (6, 'A1', 'c', '{{"xs": ["q"], "obj": {{"b": 1, "a": 2}}}}', '{{4}}', 1)""")
stmt(f"insert into {F} values (1, 1, 2, 5, 'a'), (2, 2, null, 2, 'b'), (3, null, 1, 7, null), (4, 9, 3, 1, 'd'), (5, 1, 1, 0, 'e')")
stmt(f"insert into {D} values (1, 'one', 1, true), (2, 'two', 2, false), (3, null, 2, true), (4, 'four', null, null)")
stmt(f"insert into {E} values (1, 'x', 1), (2, null, 2), (3, 'z', null), (4, 'w', 7), (5, 'v', 3)")
stmt(f"insert into {G} values (1, 1, 2), (2, 3, 1), (3, null, null)")
stmt(f"insert into {M} values (1, 1, 2, 'a', 'b', true), (2, -1, 3, 'A', 'B', false), (3, null, 1, 'x', null, null), "
     f"(4, 0, 0, 'A', 'y', true), (5, 2, null, 'B', 'B', false), (6, 5, -2, null, 'z', true)")
stmt(f"insert into {MW} values (1, 1, '2024-01-01 00:00:00'), (1, 1, '2024-01-01 00:00:00'), "
     f"(2, -3, '2024-01-01 00:00:01'), (3, null, '2024-01-01 00:00:02'), (4, 0, '2024-01-01 00:00:03')")

run_all("First data")

out.append("# Spot checks of the rows themselves.\n")
query('IT', f"select id, e from {result(UNNEST, 'exists_select', 'on')} order by id",
      ["1 t", "2 f", "3 t", "4 f", "5 f", "6 f"])
query('II', f"select id, c from {result(UNNEST, 'count', 'on')} order by id",
      ["1 3", "2 1", "3 2", "4 0", "5 0", "6 0"])
query('IT', f"select id, xs from {result(UNNEST, 'array', 'on')} order by id",
      ["1 {1,2,NULL}", "2 {2}", "3 {5,NULL}", "4 {}", "5 {}", "6 {}"])
query('IIT', f"select id, c, e from {result(LATERAL, 'count_exists', 'on')} order by id",
      ["1 3 t", "2 0 f", "4 2 t", "5 2 t", "6 1 t"])
query('II', f"select id, total from {result(LATERAL, 'sum', 'on')} order by id",
      ["1 3", "2 NULL", "3 NULL", "4 NULL", "5 3", "6 NULL"])
query('II', f"select id, count(*) from {result(LATERAL, 'kept_union', 'on')} group by id order by id",
      ["1 2", "2 2", "3 2", "4 2", "5 2", "6 2"])

out.append("# Second round: new rows on every side, rows written again with an existing key, updates.\n")
stmt(f"insert into {R} values (30, 4, 'x', 3), (31, 5, 'y', 0), (32, 5, 'y', 0), (33, 7, 'x', 1), (34, null, null, null)")
stmt(f"insert into {R} values (13, 2, 'x', 4)")
stmt(f"insert into {P} values (7, 7, 1), (8, null, null), (9, 4, 1)")
stmt(f"insert into {P} values (2, null, 1)")
stmt(f"insert into {Q} values (24, 7)")
stmt(f"insert into {U} values (4, 4, '{{4}}'), (3, 3, '{{1}}'), (3, 3, '{{1}}')")
stmt(f"""update {T} set arr = '{{7,8}}', j = '{{"xs": ["n"], "ys": [{{"a": 4, "x": 1}}]}}' where id = 2""")
stmt(f"""insert into {T} values (7, 'A5A6', 'K', '{{"xs": ["m", "m"], "obj": {{"z": 0}}}}', '{{6,NULL}}', 2)""")
stmt(f"insert into {F} values (6, 3, 2, 4, 'f')")
stmt(f"update {F} set dim_id = 2 where id = 1")
stmt(f"insert into {D} values (5, 'five', 1, true)")
stmt(f"update {D} set active = true where id = 2")
stmt(f"insert into {E} values (6, 'u', 5)")
stmt(f"insert into {M} values (7, 3, 3, 'B', 'A', false), (1, -5, -5, 'a', 'b', null)")
stmt(f"insert into {MW} values (5, 2, '2024-01-01 00:00:04'), (5, 2, '2024-01-01 00:00:04')")
run_all("Second round")

out.append("# Third round: deletes, and the last matching rows of some outer rows moved away.\n")
stmt(f"delete from {R} where id in (14, 17)")
stmt(f"update {R} set pid = 6 where id = 13")
stmt(f"update {R} set x = null, s = null where id in (10, 11)")
stmt(f"delete from {P} where id = 5")
stmt(f"update {P} set v = 1 where id = 3")
stmt(f"delete from {Q} where id in (21, 23)")
stmt(f"delete from {U} where k = 1")
stmt(f"delete from {T} where id = 1")
stmt(f"update {T} set arr = null where id = 4")
stmt(f"delete from {F} where id = 3")
stmt(f"delete from {D} where id = 1")
stmt(f"update {E} set rid = null where id = 1")
stmt(f"delete from {M} where k = 2")
stmt(f"update {M} set flag = null where k = 4")
run_all("Third round")

out.append("# Fourth round: the inner and joined tables are emptied.\n")
stmt(f"delete from {R}")
stmt(f"delete from {Q}")
stmt(f"delete from {D}")
run_all("Fourth round")

out.append("# Cleanup.\n")
for variable, name, _, _, _ in reversed(CASES):
    for sfx in ('on', 'off'):
        stmt(f"drop table {result(variable, name, sfx)}")
for view in (DNAMES, DJ, DAGG):
    stmt(f"drop view {view}")
for table in (MW, M, G, E, D, F, T, U, Q, R, P):
    stmt(f"drop table {table}")
for variable in VARIABLES:
    stmt(f"set {variable} = false")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
