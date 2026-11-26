"""Generates `e2e_test/streaming/narrow_value_aggregates.slt`, the differential test of
`streaming_narrow_value_aggregates` (rules `NarrowValueAggRule` and `NarrowValuePushRule`: the
functions that read the result of `first_value` / `last_value` are aggregated instead of the whole
value, and moved into the branches of a union and the side of a join that the value comes from):
every query is materialized with and without the variable, and both materialized views must hold
the same rows, with the same multiplicities, as the batch query, after the backfill and after each
round of changes (inserts, updates, deletes).

Usage: python3 e2e_test/streaming/narrow_value_aggregates.gen.py e2e_test/streaming/narrow_value_aggregates.slt
"""
import sys

VAR = 'streaming_narrow_value_aggregates'
P = 'snv'

# Tables and views. The order keys are unique within a group (one NULL at most), so that every
# query has one result, except in the table with ties, `{P}_tie`.
TABLES = [
    f"create table {P}_ev (id int, ts int, fact jsonb, k int)",
    f"create table {P}_ev2 (id int, ts int, fact jsonb, k int)",
    f"create table {P}_t (id int primary key, x int, doc jsonb)",
    f"create table {P}_st (id int, ts int, rec struct<a varchar, b int>)",
    f"create table {P}_src (j jsonb, ts int)",
    f"create table {P}_l (s varchar)",
    f"create table {P}_r (s varchar, j jsonb, ts int)",
    f"create table {P}_e (e jsonb, ts int)",
    f"create table {P}_tie (id int, ts int, fact jsonb)",
]
VIEWS = [
    (f"{P}_v", f"select id, last_value(fact order by ts) as fact from {P}_ev group by id"),
    (f"{P}_v2", f"select id, last_value(fact order by ts) as fact from {P}_ev2 group by id"),
    (f"{P}_vf", f"select id, first_value(fact order by ts desc) as fact from {P}_ev group by id"),
    (f"{P}_vu", f"select id, last_value(fact order by ts) as fact from "
                f"(select id, ts, fact from {P}_ev union all select id, ts, fact from {P}_ev2) u group by id"),
    (f"{P}_lower_first", f"select s, first_value(j order by ts desc) as j, max(ts) as ts "
                         f"from (select l.s, r.j, r.ts from {P}_l l join {P}_r r on l.s = r.s) x group by s"),
    (f"{P}_latest", f"select v->>'id' as id, last_value(v order by ts) as fact from ("
                    f"select case when j->>'kind' = 'wrapped' then j->'inner' else j end as v, ts from {P}_src "
                    f"union all "
                    f"select case when j->>'kind' = 'wrapped' then j->'inner' else j end as v, ts "
                    f"from {P}_lower_first where j->>'kind' is distinct from 'skipped' "
                    f"union all "
                    f"select e->'data' as v, ts from {P}_e) u group by v->>'id'"),
    (f"{P}_tie_v", f"select id, last_value(fact order by ts) as fact from {P}_tie group by id"),
]

# (name, columns of the result (`jsonb` ones are compared as text), query)
CASES = [
    # Rewritten by the rules.
    ('one_field', 'id int, dir varchar', f"select id, fact->>'dir' as dir from {P}_v"),
    ('several_fields', 'id int, a varchar, c varchar, no_dir boolean',
     f"select id, fact->>'a' as a, fact->'b'->>'c' as c, fact->>'dir' is null as no_dir from {P}_v"),
    ('cast_ok', 'id int, n varchar', f"select id, (fact->'n')::varchar as n from {P}_v"),
    # The cast fails on rows that are not selected: it must stay above the aggregation.
    ('cast_fails_below', 'id int, n int', f"select id, (fact->>'n')::int as n from {P}_v"),
    ('fallible_function', 'id int, n int',
     f"select id, jsonb_array_length(fact->'arr') as n from {P}_v"),
    ('projection', 'dir varchar, key int',
     f"select doc->>'dir' as dir, key from (select id as key, fact as doc from {P}_v) w where key > 1"),
    ('first_value', 'id int, dir varchar', f"select id, fact->>'dir' as dir from {P}_vf"),
    ('agg_over_union', 'id int, dir varchar', f"select id, fact->>'dir' as dir from {P}_vu"),
    ('union_of_aggs', 'id int, dir varchar',
     f"select id, fact->>'dir' as dir from (select id, fact from {P}_v union all select id, fact from {P}_v2) u"),
    ('join_side', 'id int, dir varchar, x int',
     f"select v.id, v.fact->>'dir' as dir, t.x from {P}_v v join {P}_t t on v.id = t.id"),
    ('left_join_padded', 'id int, dir varchar, no_dir boolean',
     f"select t.id, v.fact->>'dir' as dir, (v.fact->>'dir') is null as no_dir "
     f"from {P}_t t left join {P}_v v on v.id = t.id"),
    ('field_join_key', 'id int, x int',
     f"select v.id, t.x from {P}_v v join {P}_t t on v.fact->>'ref' = t.doc->>'ref'"),
    ('count_filter', 'n bigint',
     f"select count(*) as n from (select * from {P}_v where fact->>'dir' is null) d"),
    ('no_group_key', 'a varchar', f"select last_value(fact order by ts)->>'a' as a from {P}_ev"),
    ('no_group_key_is_null', 'no_a boolean',
     f"select (last_value(fact order by ts)->>'a') is null as no_a from {P}_ev"),
    ('filter_clause', 'id int, a varchar',
     f"select id, coalesce(last_value(fact order by ts) filter (where k > 0)->>'a', 'none') as a "
     f"from {P}_ev group by id"),
    ('struct_field', 'id int, a varchar',
     f"select id, (last_value(rec order by ts)).a as a from {P}_st group by id"),
    ('latest_count_f', 'n bigint', f"select count(*) as n from {P}_latest where fact->>'f' is null"),
    ('latest_count_id', 'n bigint', f"select count(*) as n from {P}_latest where id is null"),
    ('latest_rows', 'id varchar, f varchar, g varchar',
     f"select id, fact->>'f' as f, fact->>'g' as g from {P}_latest"),
    ('lower_first', 's varchar, jid varchar, kind varchar',
     f"select s, j->>'id' as jid, j->>'kind' as kind from {P}_lower_first"),
    # Left as they are.
    ('kept_whole', 'id int, fact jsonb', f"select id, fact from {P}_v"),
    ('kept_whole_and_field', 'id int, fact jsonb, dir varchar',
     f"select id, fact, fact->>'dir' as dir from {P}_v"),
    ('kept_strip_nulls', 'id int, a varchar', f"select id, jsonb_strip_nulls(fact)->>'a' as a from {P}_v"),
]
# Ties on the order key: the row picked among the tied ones is not defined, but all the fields
# must come from the same row, which must be one of the last rows.
TIE_CASE = ('tie_pair', 'id int, a varchar, b varchar',
            f"select id, concat(fact->>'a', '') as a, concat(fact->>'b', '') as b from {P}_tie_v")


def compared(cols):
    """The columns of a result, the JSONB ones as text."""
    out = []
    for col in cols.split(', '):
        name, data_type = col.split()
        out.append(f"{name}::varchar" if data_type == 'jsonb' else name)
    return ', '.join(out)


out = []


def comment(text):
    """Comment lines, without the indentation of the source."""
    out.append("\n".join(line.strip() for line in text.strip("\n").split("\n")) + "\n")


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    c = compared(cols)
    grouped = lambda t: f"select {c}, count(*) as {P}_rows from {t} group by {c}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def tie_checks(table):
    """The fields of each row come from the same row, one of the last rows of its group."""
    query('I', f"select count(*) from {table} where substr(a, 2) is distinct from substr(b, 2)", ["0"])
    last = (f"select t.id, t.fact->>'a' as a from {P}_tie t where t.ts is not distinct from "
            f"(select case when count(*) filter (where t2.ts is null) > 0 then null else max(t2.ts) end "
            f"from {P}_tie t2 where t2.id = t.id)")
    query('I', f"select count(*) from {table} r where not exists "
               f"(select 1 from ({last}) c where c.id = r.id and c.a = r.a)", ["0"])
    query('I', f"select (select count(*) from {table}) - (select count(distinct id) from {P}_tie)", ["0"])


def compare_all(title):
    out.append(f"# {title}: with and without the rules, every materialized view holds the rows of "
               f"the batch query.\n")
    for name, cols, sql in CASES:
        same_rows(f"{P}_{name}_on", f"{P}_{name}_off", cols)
        same_rows(f"{P}_{name}_on", f"({sql}) q", cols)
    name, cols, _ = TIE_CASE
    out.append("# Ties: both views break them by the stream key, the same way.\n")
    same_rows(f"{P}_{name}_on", f"{P}_{name}_off", cols)
    tie_checks(f"{P}_{name}_on")


def setup():
    """The tables, the views and the rows present before the queries are planned."""
    stmt("SET RW_IMPLICIT_FLUSH TO true")
    for sql in TABLES:
        stmt(sql)
    for name, sql in VIEWS:
        stmt(f"create view {name} as {sql}")

    comment("""# The first rows, present before the queries run (the backfill of the materialized
    # views): a value that is NULL, a field that is missing or JSON null, a NULL order key (last in
    # ascending order), fields that a cast or a function can not read in rows that are not the last
    # ones.
    """)
    stmt(f"""insert into {P}_ev values
      (1, 1, '{{"dir": "in", "a": "a1", "b": {{"c": "c1"}}, "n": "abc", "ref": "r1", "arr": 5}}', 1),
      (1, 2, '{{"dir": "out", "a": "a2", "b": {{"c": "c2"}}, "n": "7", "ref": "r2", "arr": [1, 2]}}', 0),
      (1, 3, null, 2),
      (2, 1, '{{"a": "a3", "n": "8", "ref": "r1", "arr": []}}', 1),
      (2, null, '{{"dir": "up", "a": "a4", "n": "9", "ref": "r3", "arr": [1]}}', -1),
      (3, 5, '{{"dir": null, "a": null, "n": "1", "arr": [2]}}', 0),
      (4, 2, '{{"dir": "in", "a": "a5", "b": "flat", "n": "x", "arr": {{"o": 1}}}}', 3),
      (4, 6, '{{"dir": "in", "a": "a6", "b": {{"c": null}}, "n": "10", "arr": [3, 4, 5]}}', 3)""")
    stmt(f"""insert into {P}_ev2 values
      (1, 11, '{{"dir": "e2"}}', 1), (5, 12, '{{"dir": "e5", "a": "x"}}', 1), (5, 13, '{{"a": "y"}}', 0)""")
    stmt(f"""insert into {P}_t values
      (1, 10, '{{"ref": "r2"}}'), (2, 20, '{{"ref": "r3"}}'), (3, 30, null), (9, 90, '{{"ref": "r1"}}')""")
    stmt(f"insert into {P}_st values (1, 1, row('p', 1)), (1, 2, row('q', 2)), (2, 1, null), (2, 3, row(null, 3))")
    stmt(f"""insert into {P}_src values
      ('{{"id": "A", "f": "1"}}', 101),
      ('{{"kind": "wrapped", "inner": {{"id": "A", "f": null}}}}', 102),
      ('{{"id": "B"}}', 103),
      (null, 104),
      ('{{"kind": "wrapped", "inner": {{"id": "C", "f": "z", "g": "zz"}}}}', 105)""")
    stmt(f"insert into {P}_l values ('s1'), ('s1'), ('s2'), ('s3')")
    stmt(f"""insert into {P}_r values
      ('s1', '{{"id": "A", "f": "r"}}', 201),
      ('s1', '{{"kind": "skipped", "id": "D"}}', 202),
      ('s2', '{{"kind": "wrapped", "inner": {{"id": "B", "f": "w"}}}}', 203),
      ('s2', '{{"id": "E", "g": "e"}}', 204),
      ('s3', '{{"id": "F", "f": "q"}}', 205),
      ('s4', '{{"id": "G"}}', 206)""")
    stmt(f"""insert into {P}_e values
      ('{{"data": {{"id": "A", "f": "e"}}}}', 301), ('{{"data": {{"id": "H"}}}}', 302), ('{{"nodata": 1}}', 303)""")
    stmt(f"""insert into {P}_tie values
      (1, 1, '{{"a": "x1", "b": "y1"}}'), (1, 2, '{{"a": "x2", "b": "y2"}}'), (1, 2, '{{"a": "x3", "b": "y3"}}'),
      (1, 2, '{{"a": "x4", "b": "y4"}}'), (2, 5, '{{"a": "x5", "b": "y5"}}'), (2, 5, '{{"a": "x6", "b": "y6"}}'),
      (3, null, '{{"a": "x7", "b": "y7"}}'), (3, null, '{{"a": "x8", "b": "y8"}}'), (3, 1, '{{"a": "x9", "b": "y9"}}')""")



def scenario(check):
    """The rounds of changes of the data, with `check(title)` after each of them."""
    check("With the first rows (after the backfill)")

    out.append("# Spot checks of the rows themselves.\n")
    query('IT', f"select id, dir from {P}_one_field_on order by id", ["1 NULL", "2 up", "3 NULL", "4 in"])
    query('IT', f"select id, n from {P}_cast_fails_below_on order by id", ["1 NULL", "2 9", "3 1", "4 10"])
    query('I', f"select n from {P}_count_filter_on", ["2"])
    query('TTT', f"select id, f, g from {P}_latest_rows_on order by id",
          ["A e NULL", "B NULL NULL", "C z zz", "E NULL e", "F q NULL", "H NULL NULL", "NULL NULL NULL"])

    comment("""# First round of changes: a new last row in some groups, a new group, a new NULL order key (the
    # previous one is updated first, so that a group keeps one NULL key at most), rows with ties.
    """)
    stmt(f"update {P}_ev set ts = 4 where id = 2 and ts is null")
    stmt(f"""insert into {P}_ev values
      (1, 9, '{{"dir": "late", "a": "a7", "b": {{"c": "c7"}}, "n": "11", "ref": "r3", "arr": [9]}}', 1),
      (5, 1, '{{"a": "a8", "n": "oops", "arr": "no"}}', 0),
      (5, null, '{{"dir": "nul", "a": "a9", "n": "12", "ref": "r2", "arr": [0]}}', 4)""")
    stmt(f"insert into {P}_ev2 values (2, 14, '{{\"dir\": \"e2b\"}}', 2)")
    stmt(f"insert into {P}_t values (5, 50, '{{\"ref\": \"r3\"}}')")
    stmt(f"insert into {P}_st values (3, 1, row('r', 7)), (2, 4, row('s', 4))")
    stmt(f"insert into {P}_src values ('{{\"id\": \"B\", \"f\": \"late\"}}', 110)")
    stmt(f"insert into {P}_r values ('s3', '{{\"kind\": \"wrapped\", \"inner\": {{\"id\": \"F\", \"f\": \"new\"}}}}', 210)")
    stmt(f"insert into {P}_l values ('s4')")
    stmt(f"insert into {P}_tie values (2, 5, '{{\"a\": \"x10\", \"b\": \"y10\"}}'), (4, 1, '{{\"a\": \"x11\", \"b\": \"y11\"}}')")
    check("After the inserts")

    comment("""# Second round of changes: the value of a last row updated (also to NULL and to a value without
    # the field), the order key of a row updated so that another row becomes the last one, deletes of
    # last rows (retractions: the previous row becomes the result again).
    """)
    stmt(f"update {P}_ev set fact = '{{\"dir\": \"changed\", \"a\": \"a10\", \"n\": \"13\", \"arr\": [1]}}' where id = 4 and ts = 6")
    stmt(f"update {P}_ev set fact = null where id = 3")
    stmt(f"update {P}_ev set ts = 0 where id = 1 and ts = 9")
    stmt(f"delete from {P}_ev where id = 1 and ts = 3")
    stmt(f"update {P}_ev set k = 0 where id = 1")
    stmt(f"delete from {P}_ev2 where id = 5 and ts = 13")
    stmt(f"update {P}_t set doc = '{{\"ref\": \"r2\"}}' where id = 2")
    stmt(f"update {P}_st set rec = row(null, 0) where id = 1 and ts = 2")
    stmt(f"delete from {P}_src where ts = 110")
    stmt(f"update {P}_r set j = '{{\"id\": \"D\", \"f\": \"kept\"}}' where ts = 202")
    stmt(f"delete from {P}_e where ts = 301")
    stmt(f"delete from {P}_tie where id = 1 and ts = 2 and fact->>'a' = 'x3'")
    stmt(f"update {P}_tie set ts = 9 where id = 2 and fact->>'a' = 'x5'")
    check("After the updates and deletes")

    out.append("# Third round of changes: whole groups deleted, then every row of the main tables.\n")
    stmt(f"delete from {P}_ev where id in (2, 5)")
    stmt(f"delete from {P}_l where s = 's2'")
    stmt(f"delete from {P}_tie where id = 3")
    check("After whole groups are deleted")
    for table in ('ev', 'ev2', 'st', 'src', 'r', 'e', 'tie'):
        stmt(f"delete from {P}_{table}")
    check("After the tables are emptied")
    query('T', f"select coalesce(a, 'empty') from {P}_no_group_key_on", ["empty"])
    query('T', f"select no_a from {P}_no_group_key_is_null_on", ["t"])

    out.append("# Fourth round of changes: the tables filled again.\n")
    stmt(f"insert into {P}_ev values (1, 1, '{{\"dir\": \"again\", \"a\": \"b1\", \"n\": \"1\", \"arr\": []}}', 1), "
         f"(1, 2, '{{\"a\": \"b2\", \"n\": \"2\", \"arr\": [1]}}', 1)")
    stmt(f"insert into {P}_src values ('{{\"id\": \"A\", \"f\": \"2\"}}', 120)")
    stmt(f"insert into {P}_r values ('s1', '{{\"id\": \"Z\", \"f\": \"y\"}}', 220)")
    stmt(f"insert into {P}_tie values (1, 1, '{{\"a\": \"x12\", \"b\": \"y12\"}}'), (1, 1, '{{\"a\": \"x13\", \"b\": \"y13\"}}')")
    check("After the tables are filled again")



def drop_inputs():
    for name, _ in reversed(VIEWS):
        stmt(f"drop view {name}")
    for sql in reversed(TABLES):
        stmt(f"drop table {sql.split()[2]}")


if __name__ == '__main__':
    comment("""# Differential test of `streaming_narrow_value_aggregates`: every query is materialized twice,
    # with and without the variable, and both materialized views must hold the rows of the batch query,
    # with the same multiplicities, after the backfill and after each round of changes.
    #
    # Generated by `python3 e2e_test/streaming/narrow_value_aggregates.gen.py <this file>`: edit the
    # list of queries there and regenerate rather than editing by hand.
    """)
    setup()
    for sfx in ('off', 'on'):
        stmt(f"set {VAR} = {'true' if sfx == 'on' else 'false'}")
        for name, _, sql in CASES + [TIE_CASE]:
            stmt(f"create materialized view {P}_{name}_{sfx} as {sql}")
    stmt(f"set {VAR} = false")

    scenario(compare_all)
    out.append("# Cleanup.\n")
    for sfx in ('on', 'off'):
        for name, _, _ in reversed(CASES + [TIE_CASE]):
            stmt(f"drop materialized view {P}_{name}_{sfx}")

    drop_inputs()
    open(sys.argv[1], 'w').write("\n".join(out))
    print(len(CASES) + 1, 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
