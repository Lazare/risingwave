"""Generates `e2e_test/streaming/unnest_lateral_functions.slt`, the differential test of the
per-row evaluation of correlated subqueries that read no table, only columns of the outer row
through set-returning functions: every query is materialized with and without
`streaming_unnest_lateral_functions`.

Usage: python3 e2e_test/streaming/unnest_lateral_functions.gen.py e2e_test/streaming/unnest_lateral_functions.slt
"""
import sys

T, U = 'ulf_t', 'ulf_u'

CASES = [
    # LEFT JOIN LATERAL: an outer ProjectSet.
    ('regexp', 'two LEFT JOIN LATERAL over regexp_matches, with a filter on the outer side',
     f"select t.id, a.x, b.y from {T} t "
     f"left join lateral (select (regexp_matches(t.s, 'A([0-9]+)'))[1] as x) as a on true "
     f"left join lateral (select (regexp_matches(t.s, 'B([0-9]+)'))[1] as y) as b on true "
     f"where t.c = 'K'"),
    ('left_jsonb', 'LEFT JOIN of jsonb_array_elements_text of the outer row',
     f"select t.id, e.x from {T} t left join jsonb_array_elements_text(t.j -> 'xs') as e(x) on true"),
    ('left_ordinality', 'LEFT JOIN of unnest WITH ORDINALITY',
     f"select t.id, x.v, x.o from {T} t left join unnest(t.arr) with ordinality as x(v, o) on true"),
    ('left_const', 'LEFT JOIN LATERAL with a constant column',
     f"select t.id, l.one, l.v from {T} t left join lateral (select 1 as one, unnest(t.arr) as v) as l on true"),
    ('left_series', 'LEFT JOIN of generate_series bounded by a column of the outer row',
     f"select t.id, g.i from {T} t left join generate_series(1, t.n) as g(i) on true"),
    ('left_each', 'LEFT JOIN of jsonb_each',
     f"select t.id, e.key, e.value from {T} t left join jsonb_each(t.j -> 'obj') as e on true"),
    # An aggregation per outer row.
    ('array', 'ARRAY of a set-returning function of the outer row',
     f"select t.id, array(select jsonb_array_elements_text(coalesce(t.j -> 'xs', '[]'::jsonb))) as xs from {T} t"),
    ('array_coalesce', 'COALESCE of an ARRAY of a set-returning function',
     f"select id, coalesce(array(select jsonb_array_elements_text(coalesce(t.j -> 'xs', '[]'::jsonb))), "
     f"array[]::text[]) as xs from {T} t"),
    ('array_ordered', 'ARRAY with ORDER BY over unnest',
     f"select t.id, array(select v from unnest(t.arr) as v order by v desc) as a from {T} t"),
    ('max_filter', 'scalar max, with a filter on the outer side',
     f"select t.id, (select max(x) from jsonb_array_elements_text(t.j -> 'xs') as x) as m from {T} t where t.c = 'c'"),
    ('sum_coalesce', 'scalar sum with a default',
     f"select t.id, coalesce((select sum((e ->> 'a')::bigint) from jsonb_array_elements(t.j -> 'ys') as e), 0) as total from {T} t"),
    ('sum_raw', 'scalar sum, NULL when no value is there',
     f"select t.id, (select sum((e ->> 'a')::bigint) from jsonb_array_elements(t.j -> 'ys') as e) as total from {T} t"),
    ('count', 'count over unnest',
     f"select t.id, (select count(*) from unnest(t.arr) as v) as c from {T} t"),
    ('exists', 'EXISTS over unnest',
     f"select t.id, exists (select 1 from unnest(t.arr) as v) as e from {T} t"),
    ('count_exists', 'count and EXISTS over unnest in one select list',
     f"select t.id, (select count(*) from unnest(t.arr) as v) as c, "
     f"exists (select 1 from unnest(t.arr) as v) as e from {T} t"),
    ('string_agg', 'string_agg with ORDER BY over the keys of an object',
     f"select t.id, (select string_agg(k, ',' order by k) from jsonb_object_keys(t.j -> 'obj') as k) as ks from {T} t"),
    ('corr_arg', 'correlated column in the aggregated expression',
     f"select t.id, (select max(v + t.n) from unnest(t.arr) as v) as m from {T} t"),
    # One subquery per select list: see REFERENCE below.
    ('nokey_max', 'outer side without a key, with duplicates and NULLs: max',
     f"select u.k, u.arr, (select max(v) from unnest(u.arr) as v) as m from {U} u"),
    ('nokey_array', 'outer side without a key, with duplicates and NULLs: ARRAY',
     f"select u.k, u.arr, array(select unnest(u.arr)) as a from {U} u"),
    # unnest of an ARRAY.
    ('unnest_array', 'unnest of a COALESCE of an ARRAY of a set-returning function',
     f"select t1.id, x.v from (select id, coalesce(array(select jsonb_array_elements_text("
     f"coalesce(j -> 'xs', '[]'::jsonb))), array[]::text[]) as arr from {T}) as t1 "
     f"join unnest(t1.arr) as x(v) on true"),
    ('unnest_array_kept', 'unnest of an ARRAY that is also selected',
     f"select t1.id, t1.arr, x.v from (select id, array(select jsonb_array_elements_text(j -> 'xs')) as arr "
     f"from {T}) as t1 join unnest(t1.arr) as x(v) on true"),
    # A window per outer row.
    ('window', 'running sum over unnest WITH ORDINALITY in a lateral subquery',
     f"select t.id, x.v, x.run from {T} t cross join lateral (select v, sum(v) over "
     f"(order by o rows unbounded preceding) as run from unnest(t.arr) with ordinality as w(v, o)) as x"),
    # Functions that fail on some rows (a JSON scalar where an array or an object is expected):
    # the failing row gives no row to the function, in both plans. Compared with each other only,
    # as the batch query fails.
    ('errors_left', 'LEFT JOIN of a function that fails on some rows',
     f"select t.id, e.x from {T} t left join jsonb_array_elements_text(t.j -> 'zs') as e(x) on true"),
    ('errors_count', 'count of a function that fails on some rows',
     f"select t.id, (select count(*) from jsonb_array_elements_text(t.j -> 'zs')) as c from {T} t"),
    ('errors_array', 'ARRAY of a function that fails on some rows',
     f"select t.id, array(select jsonb_array_elements_text(t.j -> 'zs')) as a from {T} t"),
]

# Compared with the query only: a streaming `sum` without the variable returns 0, where the query
# returns NULL, once all the non-NULL values of its group are retracted while the group still
# holds rows.
NOT_OFF = {'sum_raw', 'count_exists'}
# Compared with each other only: the batch query fails on the rows where a function fails; and
# a batch `ARRAY(..)` without `ORDER BY` does not keep the order of the rows of the function, which
# both streaming plans keep.
NOT_BATCH = {'errors_left', 'errors_count', 'errors_array', 'count_exists',
             'array', 'array_coalesce', 'nokey_array', 'unnest_array_kept'}
# Compared with another query. Without the variable, the subqueries of one select list are planned
# under a FULL OUTER JOIN of their results on the correlated value, followed by a filter that
# drops the NULL value: an outer row whose array is NULL gets NULL instead of 0 and false. The
# reference is the join of the views of each subquery alone.
REFERENCE = {
    'count_exists': "select a.id, a.c, b.e from ulf_count_off a join ulf_exists_off b on a.id = b.id",
}

out = []
def switch(on):
    stmt(f"set streaming_unnest_lateral_functions = {'true' if on else 'false'}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"ulf_{name}_on", f"ulf_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_batch(name, sql):
    a = f"ulf_{name}_on"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from ({sql}) q) x),\n"
          f"  (select count(*) from (select * from ({sql}) q except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from ({sql}) q)",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the variable, every materialized view holds the same rows.\n")
    for name, _, _ in CASES:
        if name not in NOT_OFF:
            compare(name)
    out.append(f"# {title}: with the variable, every materialized view holds the rows of its query.\n")
    for name, _, sql in CASES:
        if name not in NOT_BATCH:
            compare_batch(name, sql)
    out.append(f"# {title}: with the variable, every materialized view holds the rows of its reference.\n")
    for name, sql in REFERENCE.items():
        compare_batch(name, sql)

out.append("""# Differential test of `streaming_unnest_lateral_functions`: every query is materialized twice,
# with and without the per-row evaluation of its subqueries over set-returning functions of the
# outer row, and both materialized views must hold the same rows, and the rows of the query, after
# the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/unnest_lateral_functions.gen.py <this file>`: edit the
# list of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (id int primary key, s varchar, c varchar, j jsonb, arr int[], n int)")
stmt(f"create table {U} (k int, arr int[])")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"""insert into {T} values
  (1, 'A1 B2', 'K', '{{"xs": ["a", "b", "a"], "ys": [{{"a": 1}}, {{"a": 2}}], "obj": {{"p": 1, "q": [2]}}}}', array[1, 2, 2], 3),
  (2, 'A10 A11', 'K', '{{"xs": [], "ys": [], "obj": {{}}}}', array[]::int[], 0),
  (3, null, 'K', null, null, null),
  (4, 'xyz', 'c', '{{"xs": ["c", null, "c"], "zs": 5}}', array[null, 5]::int[], -1),
  (5, 'B7 A3', 'c', '{{"xs": ["d"], "ys": [{{"a": null}}, {{"b": 3}}], "zs": [1, "x"], "obj": {{"k": null}}}}', array[3, null, 3]::int[], 2),
  (6, '', null, '{{"zs": {{"a": 1}}}}', array[7], 1)""")
stmt(f"insert into {U} values (1, array[1, 2]), (1, array[1, 2]), (null, null), (2, array[]::int[]), "
     f"(3, array[null]::int[]), (3, array[4, 4])")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view ulf_{name}_{sfx} as {sql}")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('IT', f"select id, xs from ulf_array_on where id <> 4 order by id",
      ['1 {a,b,a}', '2 {}', '3 {}', '5 {d}', '6 {}'])
query('II', f"select id, c from ulf_count_exists_on order by id",
      ['1 3', '2 0', '3 0', '4 2', '5 3', '6 1'])
query('III', f"select id, count(*), count(x) from ulf_left_jsonb_on group by id order by id",
      ['1 3 3', '2 1 0', '3 1 0', '4 3 3', '5 1 1', '6 1 0'])

out.append("# First round of changes: new rows, rows written again with an existing primary key.\n")
stmt(f"""insert into {T} values
  (7, 'A5 A6 B8', 'K', '{{"xs": ["e", "e"], "ys": [{{"a": 4}}], "zs": [], "obj": {{"z": 0}}}}', array[9, 8, 9], 2),
  (8, null, 'c', '{{"ys": []}}', array[null, null]::int[], 4)""")
stmt(f"""insert into {T} values
  (2, 'A12', 'K', '{{"xs": ["f"], "ys": [{{"a": 5}}, {{"a": 6}}]}}', array[1], 1)""")
stmt(f"insert into {U} values (4, array[0]), (1, array[1, 2]), (null, array[6])")
compare_all("After the inserts")

out.append("# Second round of changes: the source columns are updated, set to NULL or emptied.\n")
out.append("# The sum of row 1 loses all its non-NULL values while the row keeps elements.\n")
stmt(f"""update {T} set j = '{{"xs": ["a"], "ys": [{{"a": null}}], "obj": {{"p": 2}}}}' where id = 1""")
stmt(f"update {T} set arr = array[]::int[], n = 0 where id = 1")
stmt(f"update {T} set s = 'B1 B2 A9', c = 'K' where id = 4")
stmt(f"""update {T} set j = '{{"xs": ["g", "h"], "zs": "bad"}}', arr = array[2, 1] where id = 3""")
stmt(f"update {T} set j = null, arr = null, s = null where id = 5")
stmt(f"update {T} set n = 5 where id = 6")
stmt(f"delete from {T} where id = 8")
stmt(f"update {U} set arr = array[5, null] where k = 2")
stmt(f"delete from {U} where k = 1")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the outer table is emptied and filled again.\n")
stmt(f"delete from {T}")
stmt(f"delete from {U}")
compare_all("After the outer tables are emptied")
stmt(f"""insert into {T} values
  (1, 'A1 B2', 'K', '{{"xs": ["a", "b"], "ys": [{{"a": 1}}], "zs": 3, "obj": {{"p": 1}}}}', array[1, 2], 2),
  (9, 'A7', 'c', '{{"xs": [], "zs": [[1]]}}', array[]::int[], 0)""")
stmt(f"insert into {U} values (1, array[1, 1]), (null, array[]::int[])")
compare_all("After the outer tables are filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view ulf_{name}_{sfx}")
for table in (U, T):
    stmt(f"drop table {table}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
