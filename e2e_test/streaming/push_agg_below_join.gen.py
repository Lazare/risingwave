"""Generates `e2e_test/streaming/push_agg_below_join.slt`, the differential test of
`streaming_push_agg_below_join` (rule `AggJoinTransposeRule`, exact form: an aggregation over a
join whose other side is unique on the join columns is computed before the join): every query is
materialized with and without the variable, and both materialized views must hold the same rows,
with the same multiplicities, as the batch query, after the backfill and after each round of
changes.

Sums are wrapped in `coalesce(.., 0)`: a streaming `sum` may return 0 instead of NULL once all the
non-NULL values of a group were retracted (a known quirk of the engine, not of the rule).

Usage: python3 e2e_test/streaming/push_agg_below_join.gen.py e2e_test/streaming/push_agg_below_join.slt
"""
import sys

F, D, D2, E = 'spaj_f', 'spaj_d', 'spaj_d2', 'spaj_e'
VAR = 'streaming_push_agg_below_join'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('left_unique', 'k int, name varchar, s bigint, n bigint',
     f"select f.k, d.name, coalesce(sum(f.x), 0) as s, count(*) as n from {F} f left join {D} d "
     f"on f.k = d.id group by f.k, d.name"),
    ('inner_filtered', 'k int, s bigint, n bigint',
     f"select f.k, coalesce(sum(f.x * 2), 0) as s, count(f.y) as n from {F} f join {D} d "
     f"on f.k = d.id where d.region = 1 group by f.k"),
    ('inner_partner', 'id int, name varchar, m int',
     f"select d.id, d.name, max(f.x) as m from {F} f join {D} d on f.k = d.id group by d.id, d.name"),
    ('right_unique', 'k int, region int, s bigint',
     f"select f.k, d.region, coalesce(sum(f.y), 0) as s from {D} d right join {F} f on f.k = d.id "
     f"group by f.k, d.region"),
    ('two_unique', 'k int, k2 int, name varchar, w int, n bigint',
     f"select f.k, f.k2, d.name, d2.w, count(*) as n from {F} f join {D} d on f.k = d.id "
     f"left join {D2} d2 on f.k2 = d2.id and d2.ver = 1 group by f.k, f.k2, d.name, d2.w"),
    ('distinct_aggs', 'k int, c bigint, p bigint',
     f"select f.k, count(distinct f.x) as c, count(*) filter (where f.x > 0) as p "
     f"from {F} f left join {D} d on f.k = d.id group by f.k"),
    ('value_of_unique', 'k int, mn varchar, mx int, n bigint',
     f"select f.k, min(d.name) as mn, max(d.region) as mx, count(*) as n from {F} f "
     f"left join {D} d on f.k = d.id group by f.k"),
    ('distinct_rows', 'k int, name varchar',
     f"select distinct f.k, d.name from {F} f left join {D} d on f.k = d.id"),
    ('computed_group_having', 'k int, b int, name varchar, n bigint',
     f"select f.k, f.x % 3 as b, d.name, count(*) as n from {F} f left join {D} d on f.k = d.id "
     f"group by f.k, f.x % 3, d.name having count(*) > 1"),
    ('non_equi_grouped', 'k int, x int, name varchar, n bigint',
     f"select f.k, f.x, d.name, count(*) as n from {F} f left join {D} d on f.k = d.id "
     f"and f.x > d.region group by f.k, f.x, d.name"),
    ('null_safe', 'k int, name varchar, n bigint',
     f"select f.k, d.name, count(*) as n from {F} f left join {D} d on f.k is not distinct from d.id "
     f"group by f.k, d.name"),
    # Left as they are by the rule.
    ('kept_not_unique', 'k int, s bigint',
     f"select f.k, coalesce(sum(f.x), 0) as s from {F} f join {E} e on f.k = e.k group by f.k"),
    ('kept_optional_group', 'id int, n bigint',
     f"select d.id, count(*) as n from {F} f left join {D} d on f.k = d.id group by d.id"),
    ('kept_reads_other', 'k int, s bigint',
     f"select f.k, coalesce(sum(d.region), 0) as s from {F} f left join {D} d on f.k = d.id group by f.k"),
    ('kept_non_equi', 'k int, n bigint',
     f"select f.k, count(*) as n from {F} f left join {D} d on f.k = d.id and f.x > d.region group by f.k"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    grouped = lambda t: f"select {cols}, count(*) as spaj_rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def compare_all(title):
    out.append(f"# {title}: with and without the rule, every materialized view holds the rows of "
               f"the batch query.\n")
    for name, cols, sql in CASES:
        c = columns(cols)
        same_rows(f"spaj_{name}_on", f"spaj_{name}_off", c)
        same_rows(f"spaj_{name}_on", f"({sql}) q", c)


out.append("""# Differential test of `streaming_push_agg_below_join`: every query is materialized twice, with
# and without the variable, and both materialized views must hold the rows of the batch query, with
# the same multiplicities, after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/push_agg_below_join.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {F} (id int primary key, k int, k2 int, x int, y bigint)")
stmt(f"create table {D} (id int primary key, name varchar, region int)")
stmt(f"create table {D2} (id int, ver int, w int, primary key (id, ver))")
stmt(f"create table {E} (k int, w int)")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {F} select g, case when g % 37 = 0 then null else g % 12 end, g % 5, g % 7 - 2, "
     f"case when g % 9 = 0 then null else g end from generate_series(1, 300) g")
stmt(f"insert into {D} select g, case when g % 4 = 0 then null else 'n' || (g % 3) end, g % 3 "
     f"from generate_series(0, 9) g")
stmt(f"insert into {D2} select g / 2, g % 2 + 1, case when g % 4 = 0 then null else g end "
     f"from generate_series(0, 7) g")
stmt(f"insert into {E} select g % 14, g % 3 from generate_series(1, 30) g")

for sfx in ('off', 'on'):
    stmt(f"set {VAR} = {'true' if sfx == 'on' else 'false'}")
    for name, _, sql in CASES:
        stmt(f"create materialized view spaj_{name}_{sfx} as {sql}")
stmt(f"set {VAR} = false")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('I', "select count(*) from spaj_left_unique_on", ["13"])
query('I', "select sum(n) from spaj_left_unique_on", ["300"])

out.append("# First round of changes: new rows, NULL keys and duplicates, a dimension row replaced.\n")
stmt(f"insert into {F} select g, case when g % 5 = 0 then null else g % 15 end, g % 5, g % 4, g "
     f"from generate_series(301, 400) g")
stmt(f"insert into {D} values (10, 'ten', 1), (11, null, 2), (12, 'n1', 0)")
stmt(f"insert into {D} values (1, 'one', 2)")
stmt(f"insert into {D2} values (5, 1, 50), (6, 1, null)")
stmt(f"insert into {E} values (13, 1), (null, 2)")
compare_all("After the inserts")

out.append("# Second round of changes: keys moved, values set to NULL, deletes (retractions).\n")
stmt(f"update {F} set k = null where id % 23 = 0")
stmt(f"update {F} set k = 3 where id % 29 = 0")
stmt(f"update {F} set x = null where id % 10 = 1")
stmt(f"delete from {F} where k = 4")
stmt(f"update {D} set name = 'n2', region = 1 where id = 5")
stmt(f"delete from {D} where id = 6")
stmt(f"insert into {D} values (20, 'n0', 0)")
stmt(f"delete from {D} where id in (2, 8)")
stmt(f"delete from {D2} where id = 1")
stmt(f"delete from {E} where k = 5")
compare_all("After the updates and deletes")

out.append("# Third round of changes: the unique side emptied, then filled again.\n")
stmt(f"delete from {D}")
stmt(f"delete from {D2}")
compare_all("After the unique sides are emptied")
stmt(f"insert into {D} select g, 'm' || g, g % 2 from generate_series(0, 14) g")
stmt(f"insert into {D2} values (0, 1, 1), (1, 1, 2), (2, 1, 3)")
compare_all("After the unique sides are filled again")

out.append("# Fourth round of changes: the aggregated side emptied.\n")
stmt(f"delete from {F}")
compare_all("After the aggregated side is emptied")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view spaj_{name}_{sfx}")
for table in (E, D2, D, F):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
