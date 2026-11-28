"""Generates `e2e_test/batch/group_topn_memory.slt`, the differential test of the batch top-n per
group:
- the executor streams a top-n per group without order, `WITH TIES` and offset (unconditional),
- `batch_streaming_unordered_group_topn`: the order columns that belong to the group key are
  dropped, which leaves more top-n per group unordered,
- `batch_spill_group_topn`: an ordered top-n per group spills when the memory of its query is short.

Every query runs twice, with both variables off and on, through `INSERT ... SELECT` (the batch
plan), in local and distributed query mode, after each round of changes of the data.

A top-n per group without order may return any rows of a group. Its results are compared in a way
that does not depend on which ones: the number of rows of each group must be the same with and
without the variables, and every row must be a row of the input of the top-n (`valid`). The
queries with a total order are compared row for row.

Usage: python3 e2e_test/batch/group_topn_memory.gen.py e2e_test/batch/group_topn_memory.slt
"""
import sys

EV = 'gt_ev'
VARS = ['batch_streaming_unordered_group_topn', 'batch_spill_group_topn']

# A union of aggregations with wide `jsonb` values: one row per group of each branch.
UNION = (
    f"select k, 'a' as src, jsonb_agg(payload order by id) as doc from {EV} group by k\n"
    f"    union all\n"
    f"    select v % 40 as k, 'b' as src, jsonb_agg(payload order by id) as doc from {EV} "
    f"where v % 3 = 0 group by v % 40\n"
    f"    union all\n"
    f"    select k, 'c' as src, jsonb_build_object('n', count(*), 'max', max(v)) as doc "
    f"from {EV} group by k"
)
ROWS = f"select k, v, payload from {EV}"

# (name, columns of the result, query, group key columns or None, rows the query picks from)
# With a group key, the query may pick any rows of a group: the counts per group and the validity
# of the rows are compared. Without, the order is total and the rows are compared.
CASES = [
    ('distinct_on_union', 'k int, src varchar, doc jsonb',
     f"select distinct on (k) k, src, doc from (\n    {UNION}\n) u",
     ['k'], f"select k, src, doc from (\n    {UNION}\n) u"),
    ('distinct_on_union_by_key', 'k int, src varchar, doc jsonb',
     f"select distinct on (k) k, src, doc from (\n    {UNION}\n) u order by k",
     ['k'], f"select k, src, doc from (\n    {UNION}\n) u"),
    ('distinct_on_two_keys', 'k int, src varchar, doc jsonb',
     f"select distinct on (k, src) k, src, doc from (\n    {UNION}\n) u order by k, src",
     ['k', 'src'], f"select k, src, doc from (\n    {UNION}\n) u"),
    ('distinct_on_rows', 'k int, v int, payload jsonb',
     f"select distinct on (k) k, v, payload from {EV}",
     ['k'], ROWS),
    ('window_by_key', 'k int, v int, payload jsonb',
     f"select k, v, payload from (select k, v, payload, row_number() over (partition by k "
     f"order by k) as rn from {EV}) s where rn <= 3",
     ['k'], ROWS),
    ('window_by_key_offset', 'k int, v int, payload jsonb',
     f"select k, v, payload from (select k, v, payload, row_number() over (partition by k "
     f"order by k) as rn from {EV}) s where rn between 2 and 4",
     ['k'], ROWS),
    ('distinct_on_limit_count', 'n bigint',
     f"select count(*) from (select * from (select distinct on (k) k, v from {EV}) s limit 5) x",
     None, None),
    # A total order: the same rows.
    ('distinct_on_union_ordered', 'k int, src varchar, doc jsonb',
     f"select distinct on (k) k, src, doc from (\n    {UNION}\n) u order by k, src desc",
     None, None),
    ('window_ordered', 'k int, v int, id int, payload jsonb',
     f"select k, v, id, payload from (select k, v, id, payload, row_number() over (partition by k "
     f"order by v desc, id) as rn from {EV}) s where rn <= 2",
     None, None),
    ('window_ordered_offset', 'k int, v int, id int',
     f"select k, v, id from (select k, v, id, row_number() over (partition by k "
     f"order by v, id desc) as rn from {EV}) s where rn between 3 and 5",
     None, None),
    ('distinct_on_ordered_by_value', 'k int, v int, id int',
     f"select distinct on (k) k, v, id from {EV} order by k, v, id",
     None, None),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def names(cols):
    return [col.split()[0] for col in cols.split(', ')]


def text(cols):
    """The columns as text: `jsonb` values compare as text."""
    return ', '.join(f"{c}::varchar as gt_{c}" for c in cols)


def same_rows(a, b, cols):
    grouped = lambda t: (f"select {text(cols)}, count(*) as gt_rows from {t} "
                         f"group by {', '.join(c + '::varchar' for c in cols)}")
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def valid(table, cols, candidates):
    cond = ' and '.join(f"(c.{c}::varchar is not distinct from x.{c}::varchar)" for c in cols)
    query('I',
          f"select count(*) from {table} x where not exists (\n"
          f"  select 1 from {candidates} c where {cond})",
          ["0"])


def run_all(title):
    for mode in ('local', 'distributed'):
        out.append(f"# {title}, {mode} query mode: every query runs without, then with the "
                   f"variables.\n")
        stmt(f"set query_mode = {mode}")
        for name, _, _, keys, picks_from in CASES:
            if keys is not None:
                stmt(f"delete from gt_{name}_cand")
                stmt(f"insert into gt_{name}_cand {picks_from}")
        for name, _, sql, _, _ in CASES:
            stmt(f"delete from gt_{name}_off")
            stmt(f"delete from gt_{name}_on")
            stmt(f"insert into gt_{name}_off {sql}")
            for var in VARS:
                stmt(f"set {var} = true")
            stmt(f"insert into gt_{name}_on {sql}")
            for var in VARS:
                stmt(f"set {var} = false")
        stmt("set query_mode = auto")
        out.append(f"# {title}, {mode} query mode: the same rows, or the same number of rows per "
                   f"group and rows of the input.\n")
        for name, cols, _, keys, picks_from in CASES:
            a, b = f"gt_{name}_on", f"gt_{name}_off"
            if keys is None:
                same_rows(a, b, names(cols))
            else:
                same_rows(a, b, keys)
                valid(a, names(cols), f"gt_{name}_cand")
                valid(b, names(cols), f"gt_{name}_cand")


out.append("""# Differential test of the batch top-n per group: streaming when unordered, and the variables
# `batch_streaming_unordered_group_topn` and `batch_spill_group_topn`. Every query runs through
# `INSERT ... SELECT` with the variables off, then on, in local and distributed query mode, after
# each round of changes of the data. A top-n per group without order may return any rows of a
# group: such results are compared by their number of rows per group, and each row must be a row
# of the input of the top-n. Queries with a total order are compared row for row.
#
# Generated by `python3 e2e_test/batch/group_topn_memory.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt("set batch_over_window_to_topn = true")
stmt(f"create table {EV} (id int primary key, k int, v int, payload jsonb)")
for name, cols, _, keys, _ in CASES:
    stmt(f"create table gt_{name}_off ({cols})")
    stmt(f"create table gt_{name}_on ({cols})")
    if keys is not None:
        # The rows that the query may pick from.
        stmt(f"create table gt_{name}_cand ({cols})")

run_all("Empty table")

out.append("# First data: NULL keys (one group), groups of one row, equal values.\n")
stmt(f"insert into {EV} values (1, 1, 5, '{{\"a\": 1}}'), (2, 1, 5, '{{\"a\": 2}}'), "
     f"(3, null, 2, null), (4, null, 2, '[]'), (5, 2, null, '{{}}'), (6, 0, 3, '\"x\"'), "
     f"(7, 9, null, null), (8, 1, 6, '{{\"a\": [1, 2]}}')")
run_all("First data")

out.append("# Second data: wide values over several chunks and tasks, many rows per group.\n")
stmt(f"insert into {EV} select i, case when i % 97 = 0 then null else i % 50 end, "
     f"(i * 7919) % 2000, jsonb_build_object('id', i, 'pad', repeat('x', 300)) "
     f"from generate_series(9, 3000) i")
run_all("Second data")

out.append("# The limit over the unordered top-n per group keeps 5 groups.\n")
query('I', "select n from gt_distinct_on_limit_count_on", ["5"])

out.append("# Third data: updates and deletes, groups emptied.\n")
stmt(f"update {EV} set k = null where id % 13 = 0")
stmt(f"delete from {EV} where id % 5 = 0 or k between 10 and 19")
stmt(f"update {EV} set payload = jsonb_build_object('id', id, 'v', v) where id % 3 = 0")
run_all("Third data")

out.append("# Cleanup.\n")
for name, _, _, keys, _ in reversed(CASES):
    if keys is not None:
        stmt(f"drop table gt_{name}_cand")
    stmt(f"drop table gt_{name}_on")
    stmt(f"drop table gt_{name}_off")
stmt(f"drop table {EV}")
for var in ['batch_over_window_to_topn'] + VARS:
    stmt(f"set {var} = false")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
