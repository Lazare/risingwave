"""Generates `e2e_test/batch/push_join_below_group_ops.slt`, the differential test of
`batch_push_join_below_group_ops` (rule `JoinBelowGroupOpRule`: an inner or semi join on the group
columns of a top-n per group, a deduplication, a window or an aggregation goes below the operator)
and of `batch_narrow_value_aggregates` through the operators that pass their rows through (rule
`NarrowValuePushRule` below a top-n, a deduplication, a window).

Every query runs through `INSERT ... SELECT` (the batch plan) with every variable off, then with
each variable on and with both on, and every result must hold the same rows with the same
multiplicities as the first one, after each round of changes of the data. The order keys are unique
within each group (and across the branches of the unions), so that every query has one result.

Usage: python3 e2e_test/batch/push_join_below_group_ops.gen.py e2e_test/batch/push_join_below_group_ops.slt
"""
import sys

P = 'pjg'
JOIN = 'batch_push_join_below_group_ops'
NARROW = 'batch_narrow_value_aggregates'
MODES = [('join', [JOIN]), ('narrow', [NARROW]), ('both', [JOIN, NARROW])]

TABLES = [
    f"create table {P}_ea (id int primary key, k int, ts bigint, doc jsonb, g int)",
    f"create table {P}_eb (id int primary key, k int, ts bigint, doc jsonb, g int)",
    f"create table {P}_d (u int primary key, state varchar, w int)",
    f"create table {P}_dn (u int, state varchar, w int)",
    f"create table {P}_att (aid int primary key, tid int, x int)",
    f"create table {P}_tk (tid int primary key, kind varchar)",
]
VIEWS = [
    (f"{P}_va", f"select k, last_value(doc order by ts) as doc, max(ts) as ts from {P}_ea group by k"),
    (f"{P}_vb", f"select k, last_value(doc order by ts) as doc, max(ts) as ts from {P}_eb group by k"),
    (f"{P}_vab", f"select * from {P}_va union all select * from {P}_vb"),
    (f"{P}_vu", f"select distinct on (k) * from {P}_vab order by k, ts desc"),
    (f"{P}_vt", f"select distinct on (k) k, ts, doc from {P}_ea order by k, ts desc"),
    (f"{P}_vd", f"select distinct on (k) k, ts, doc from {P}_ea"),
    (f"{P}_vg", f"select k, g, last_value(doc order by ts) as doc from {P}_ea group by k, g"),
    (f"{P}_sel", f"select att.aid, att.x from {P}_att att join {P}_tk tk on att.tid = tk.tid "
                 f"where tk.kind = 'a'"),
]

# (name, columns of the result, query)
CASES = [
    # The join goes below the group operators.
    ('topn_union', 'u int, w int, x varchar, ts bigint',
     f"select d.u, d.w, vu.doc->'a'->>'b' as x, vu.ts from {P}_vu vu join {P}_d d on vu.k = d.u "
     f"where d.state = 'open'"),
    ('semi_not_unique', 'k int, x varchar',
     f"select vu.k, vu.doc->'a'->>'b' as x from {P}_vu vu "
     f"where vu.k in (select u from {P}_dn where state = 'open')"),
    ('right_side', 'w int, ts bigint',
     f"select d.w, vt.ts from {P}_d d join {P}_vt vt on vt.k = d.u where d.state = 'open'"),
    ('join_side', 'x int, c varchar',
     f"select sel.x, vu.doc->>'c' as c from {P}_vu vu join {P}_sel sel on vu.k = sel.aid"),
    ('window_rn', 'u int, c varchar',
     f"select d.u, s.doc->>'c' as c from (select k, doc, "
     f"row_number() over (partition by k order by ts desc) as rn from {P}_ea) s "
     f"join {P}_d d on s.k = d.u where d.state = 'open' and s.rn = 1"),
    ('window_count', 'u int, ts bigint, n bigint',
     f"select d.u, s.ts, s.n from (select k, ts, count(*) over (partition by k) as n from {P}_ea) s "
     f"join {P}_d d on s.k = d.u where d.state = 'open'"),
    ('dedup', 'w int, k int',
     f"select d.w, vd.k from {P}_vd vd join {P}_d d on vd.k = d.u where d.state = 'open'"),
    ('agg', 'k int, w int, c varchar',
     f"select va.k, d.w, va.doc->>'c' as c from {P}_va va join {P}_d d on va.k = d.u "
     f"where d.state = 'open'"),
    ('agg_semi', 'k int, c varchar',
     f"select va.k, va.doc->>'c' as c from {P}_va va "
     f"where exists (select 1 from {P}_dn dn where dn.u = va.k and dn.state = 'open')"),
    ('group_and_other', 'k int, ts bigint',
     f"select vu.k, vu.ts from {P}_vu vu join {P}_d d on vu.k = d.u and d.w > vu.k "
     f"where d.state = 'open'"),
    ('part_of_key', 'u int, g int, c varchar',
     f"select d.u, vg.g, vg.doc->>'c' as c from {P}_vg vg join {P}_d d on vg.k = d.u "
     f"where d.state = 'open'"),
    # Left as they are by the join rule.
    ('left_join', 'u int, ts bigint',
     f"select d.u, vu.ts from {P}_d d left join {P}_vu vu on vu.k = d.u where d.state = 'open'"),
    ('not_unique', 'u int, ts bigint',
     f"select dn.u, vu.ts from {P}_vu vu join {P}_dn dn on vu.k = dn.u where dn.state = 'open'"),
    ('other_column', 'u int, ts bigint',
     f"select d.u, vu.ts from {P}_vu vu join {P}_d d on vu.k = d.u and vu.ts = d.w "
     f"where d.state = 'open'"),
    # The narrowing functions below the group operators.
    ('narrow_fields', 'k int, c varchar, b varchar, n int',
     f"select k, doc->>'c' as c, doc->'a'->>'b' as b, (doc->>'n')::int as n from {P}_vu"),
    ('narrow_ordered_by_field', 'k int, c varchar',
     f"select distinct on (k) k, doc->>'c' as c from {P}_vab order by k, doc->>'o', ts"),
    ('narrow_top_n', 'k int, c varchar',
     f"select k, doc->>'c' as c from {P}_vab order by ts desc limit 3"),
    ('narrow_dedup', 'k int, c varchar',
     f"select distinct on (k) k, doc->>'c' as c from {P}_va"),
    ('narrow_window_rn', 'k int, c varchar',
     f"select k, c from (select k, doc->>'c' as c, "
     f"row_number() over (partition by k order by ts desc) as rn from {P}_vab) s where rn = 1"),
    ('narrow_window_count', 'k int, c varchar, n bigint',
     f"select k, doc->>'c' as c, count(*) over (partition by k) as n from {P}_vab"),
    ('narrow_window_reads_value', 'k int, c varchar, f varchar',
     f"select k, doc->>'c' as c, first_value(doc) over (partition by k order by ts) ->> 'c' as f "
     f"from {P}_vab"),
]

out = []


def comment(text):
    out.append("\n".join(line.strip() for line in text.strip("\n").split("\n")) + "\n")


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    c = ', '.join(col.split()[0] for col in cols.split(', '))
    grouped = lambda t: f"select {c}, count(*) as {P}_rows from {t} group by {c}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def run_all(title):
    out.append(f"# {title}: every query runs with every variable off, then in each mode.\n")
    for name, _, sql in CASES:
        stmt(f"delete from {P}_{name}_off")
        stmt(f"insert into {P}_{name}_off {sql}")
        for mode, variables in MODES:
            stmt(f"delete from {P}_{name}_{mode}")
            for var in variables:
                stmt(f"set {var} = true")
            stmt(f"insert into {P}_{name}_{mode} {sql}")
            for var in variables:
                stmt(f"set {var} = false")
    out.append(f"# {title}: every mode holds the rows of the plan without the rules.\n")
    for name, cols, _ in CASES:
        for mode, _ in MODES:
            same_rows(f"{P}_{name}_{mode}", f"{P}_{name}_off", cols)


comment("""# Differential test of `batch_push_join_below_group_ops` and of `batch_narrow_value_aggregates`
# through the operators that pass their rows through: every query runs through `INSERT ... SELECT`
# with every variable off, then with each of them and with both, and every result must hold the same
# rows with the same multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/push_join_below_group_ops.gen.py <this file>`: edit the
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
for sql in TABLES:
    stmt(sql)
for name, sql in VIEWS:
    stmt(f"create view {name} as {sql}")
for name, cols, _ in CASES:
    for mode in ['off'] + [m for m, _ in MODES]:
        stmt(f"create table {P}_{name}_{mode} ({cols})")

comment("""# The first rows: several rows per key, a NULL key, keys only in one of the event tables,
# a NULL value, fields that a cast can not read in rows that are not the last ones. The order
# keys `ts` are unique over both event tables.
""")
stmt(f"""insert into {P}_ea values
  (1, 1, 1, '{{"a": {{"b": "b1"}}, "c": "c1", "n": "x", "o": "2"}}', 1),
  (2, 1, 5, '{{"a": {{"b": "b2"}}, "c": "c2", "n": "2", "o": "1"}}', 1),
  (3, 2, 2, '{{"c": "c3", "n": "3", "o": "1"}}', 2),
  (4, 2, 7, null, 3),
  (5, 3, 3, '{{"a": {{"b": "b5"}}, "c": "c5", "n": "5"}}', 1),
  (6, 4, 4, '{{"a": "flat", "c": "c6", "n": "6", "o": "3"}}', 1),
  (7, 4, 9, '{{"a": {{"b": "b7"}}, "c": "c7", "n": "7", "o": "0"}}', 2),
  (8, null, 6, '{{"c": "cnull", "n": "8"}}', 1),
  (9, 6, 8, '{{"a": {{"b": "b9"}}, "c": "c9", "n": "9"}}', 1)""")
stmt(f"""insert into {P}_eb values
  (1, 1, 11, '{{"a": {{"b": "e1"}}, "c": "d1", "n": "11", "o": "0"}}', 1),
  (2, 2, 0, '{{"a": {{"b": "e2"}}, "c": "d2", "n": "bad"}}', 1),
  (3, 5, 12, '{{"a": {{"b": "e3"}}, "c": "d3", "n": "13", "o": "5"}}', 1),
  (4, 5, 10, '{{"c": "d4", "n": "14"}}', 2),
  (5, null, 13, '{{"c": "dnull"}}', 1)""")
stmt(f"""insert into {P}_d values
  (1, 'open', 10), (2, 'open', 1), (3, 'closed', 30), (4, 'open', 40), (5, 'open', 50),
  (7, 'open', 70), (8, null, 80)""")
stmt(f"""insert into {P}_dn values
  (1, 'open', 1), (1, 'open', 2), (2, 'closed', 3), (4, 'open', 4), (5, 'open', 5), (5, 'closed', 6),
  (null, 'open', 7)""")
comment("""# Many more keys that the other side does not hold, so that the join is selective whatever the
# statistics say.
""")
stmt(f"""insert into {P}_ea select 100 + i, 100 + i / 3, 1000 + i,
  jsonb_build_object('a', jsonb_build_object('b', 'fb' || i), 'c', 'fc' || i, 'n', i::varchar), 1
  from generate_series(0, 599) as s(i)""")
stmt(f"""insert into {P}_eb select 100 + i, 100 + i / 2, 5000 + i,
  jsonb_build_object('c', 'gc' || i, 'o', (i % 7)::varchar), 2
  from generate_series(0, 399) as s(i)""")
stmt(f"insert into {P}_tk values (1, 'a'), (2, 'b'), (3, 'a')")
stmt(f"insert into {P}_att values (1, 1, 100), (2, 2, 200), (3, 3, 300), (4, 1, 400), (5, null, 500)")

run_all("With the first rows")

comment("""# First round of changes: new last rows, a new key in both event tables, states changed, a
# key of the other side moved.
""")
stmt(f"""insert into {P}_ea values
  (10, 1, 14, '{{"a": {{"b": "b10"}}, "c": "c10", "n": "10", "o": "4"}}', 2),
  (11, 7, 15, '{{"c": "c11", "n": "11"}}', 1)""")
stmt(f"insert into {P}_eb values (6, 7, 16, '{{\"a\": {{\"b\": \"e6\"}}, \"c\": \"d6\", \"n\": \"16\"}}', 1)")
stmt(f"update {P}_d set state = 'open' where u = 3")
stmt(f"update {P}_d set state = 'closed' where u = 1")
stmt(f"update {P}_dn set state = 'open' where u = 2")
stmt(f"update {P}_tk set kind = 'a' where tid = 2")
stmt(f"update {P}_att set aid = 6 where aid = 4")
run_all("After the inserts and updates")

comment("""# Second round of changes: last rows deleted (the previous row becomes the result again), a
# value updated to NULL, keys of the other side deleted.
""")
stmt(f"delete from {P}_ea where id in (10, 7)")
stmt(f"update {P}_eb set doc = null where id = 3")
stmt(f"delete from {P}_d where u in (2, 5)")
stmt(f"delete from {P}_dn where u = 1")
stmt(f"delete from {P}_tk where tid = 3")
run_all("After the deletes")

out.append("# Third round of changes: one event table and the other side emptied.\n")
stmt(f"delete from {P}_eb")
stmt(f"delete from {P}_d")
run_all("After the tables are emptied")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    for mode in reversed(['off'] + [m for m, _ in MODES]):
        stmt(f"drop table {P}_{name}_{mode}")
for name, _ in reversed(VIEWS):
    stmt(f"drop view {name}")
for sql in reversed(TABLES):
    stmt(f"drop table {sql.split()[2]}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
