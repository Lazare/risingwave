"""Generates `e2e_test/batch/reduce_keys_after_conversions.slt`, the differential test of
`batch_reduce_keys_after_conversions` (second pass of the reductions on keys, after the stages that
create aggregations) and of `batch_over_window_to_topn` (`row_number() <= n` as a top-n per group).

Every query runs with every variable off, then in three modes: the reductions on keys with their
second pass, the top-n alone, and both. Its rows are inserted into one table per mode (INSERT ...
SELECT runs the batch plan), and each mode must hold the same multiset of rows as the run with
every variable off, after each change of the data.

Usage: python3 e2e_test/batch/reduce_keys_after_conversions.gen.py e2e_test/batch/reduce_keys_after_conversions.slt
"""
import sys

P = 'rkc'
T, D, F, EV, NOPK = f'{P}_t', f'{P}_d', f'{P}_f', f'{P}_ev', f'{P}_nopk'

LAG_KEY = f"with o as (select id, lag(id) over (order by id) as prev from {T})"
UNION_1 = f"(select id from {D} union select dim_id from {F})"
UNION_2 = f"(select id, name from {D} union select dim_id, label from {F})"


def latest(cond, order='prio desc, eid', func='row_number', cols='k, status'):
    return (f"(select {cols} from (select {cols}, {func}() over (partition by k order by {order}) as rn "
            f"from {EV}) x where {cond})")


CASES = [
    # Second pass: lag -> GROUP BY -> key.
    ('lag_key', 'uniqueness check of a key written with lag', 'int, bigint',
     f"{LAG_KEY} select id, count(*) + 1 as n from o where id = prev group by id"),
    ('lag_key_rows', 'duplicate rows of a key', 'int, int',
     f"{LAG_KEY} select id, prev from o where id = prev"),
    ('lag_key_count', 'count of the duplicates of a key', 'bigint',
     f"{LAG_KEY} select count(*) from o where id = prev"),
    ('lag_x', 'lag on a column that is not a key', 'int, bigint',
     f"with o as (select x, lag(x) over (order by x) as prev from {T}) "
     f"select x, count(*) + 1 as n from o where x = prev group by x"),
    ('lag_nopk', 'lag on a table without primary key', 'int, bigint',
     f"with o as (select a, lag(a) over (order by a) as prev from {NOPK}) "
     f"select a, count(*) + 1 as n from o where a = prev group by a"),
    # Second pass: UNION -> aggregation -> key.
    ('union_lj', 'left join to a union without all on its only column', 'int, int',
     f"select f.id, f.amt from {F} f left join {UNION_1} u on f.dim_id = u.id"),
    ('union_cnt', 'count per value of a union without all', 'int, bigint',
     f"select id, count(*) as n from {UNION_1} u group by id"),
    ('union_distinct', 'distinct over a union without all', 'int, varchar',
     f"select distinct id, name from {UNION_2} u"),
    ('union_count_all', 'count over a union without all of two columns', 'bigint',
     f"select count(*) from (select grp, name from {D} union select dim_id, label from {F}) u"),
    ('union_count_col', 'count of one column over a union without all of two columns', 'bigint',
     f"select count(grp) from (select grp, name from {D} union select dim_id, label from {F}) u"),
    ('union_lj_two', 'left join to a union without all on one of its two columns', 'int, int',
     f"select f.id, f.amt from {F} f left join {UNION_2} u on f.dim_id = u.id"),
    ('union_all_lj', 'left join to a union all', 'int, int',
     f"select f.id, f.amt from {F} f left join (select id from {D} union all select dim_id from {F}) u "
     f"on f.dim_id = u.id"),
    # Second pass: distinct aggregates.
    ('distinct_key', 'distinct count of a key per group', 'int, bigint, bigint',
     f"select grp, count(distinct id) as n, count(*) as c from {D} group by grp"),
    ('distinct_two', 'two distinct aggregates over a key', 'int, bigint, bigint',
     f"select grp, count(distinct id) as n, sum(distinct id) as s from {D} group by grp"),
    # Second pass: two rounds.
    ('rounds_lj', 'aggregation over a key removed, then the left join above it', 'int, int',
     f"{LAG_KEY}, dup as (select id, count(*) as n from o where id = prev group by id) "
     f"select f.id, f.amt from {F} f left join dup on f.dim_id = dup.id"),
    ('rounds_union', 'union converted, its key removes a distinct, then a join', 'int, int',
     f"select f.id, u.id as uid from {F} f "
     f"left join (select distinct id from {UNION_1} a) u on f.dim_id = u.id "
     f"left join {D} d on u.id = d.id"),
    # Top-n per group (with a total order: the rows kept are the same whatever the plan).
    ('top1', 'latest row per key', 'int, varchar', latest('rn = 1')[1:-1]),
    ('top3', 'three first rows per key, two order columns', 'int, varchar, int',
     latest('rn <= 3', order='prio, ts desc, eid', cols='k, status, ts')[1:-1]),
    ('strict', 'strict bound', 'int, varchar', latest('rn < 4', order='prio, eid')[1:-1]),
    ('offset', 'second and third rows per key', 'int, varchar',
     latest('rn > 1 and rn <= 3', order='prio, eid')[1:-1]),
    ('top10', 'ten first rows per key', 'int, varchar', latest('rn <= 10', order='prio, eid')[1:-1]),
    ('top11', 'eleven first rows per key (not rewritten)', 'int, varchar',
     latest('rn <= 11', order='prio, eid')[1:-1]),
    ('rn_read', 'row number read above', 'int, varchar, bigint',
     f"select k, status, rn from (select k, status, row_number() over (partition by k order by prio, eid) as rn "
     f"from {EV}) x where rn <= 2"),
    ('other_cond', 'other condition on the rows kept', 'int, varchar',
     latest("rn = 1 and status <> 'closed'")[1:-1]),
    ('global', 'no partition, a global top-n', 'int, varchar',
     f"select k, status from (select k, status, row_number() over (order by prio, eid) as rn from {EV}) x "
     f"where rn <= 5"),
    ('two_parts', 'partition by two columns', 'int, varchar, int',
     f"select k, status, ts from (select k, status, ts, row_number() over (partition by k, status order by ts desc, eid) "
     f"as rn from {EV}) x where rn = 1"),
    ('two_windows', 'two window functions', 'int, varchar',
     f"select k, status from (select k, status, row_number() over (partition by k order by prio, eid) as rn, "
     f"sum(prio) over (partition by k) as total from {EV}) x where rn = 1 and total > 3"),
    # Ties: row_number picks any of the tied rows, so only the tied columns are read; rank keeps
    # them all.
    ('ties_rn', 'row number over ties, tied columns only', 'int, int',
     latest('rn <= 2', order='prio desc', cols='k, prio')[1:-1]),
    ('ties_rn_one', 'first row over ties, tied columns only', 'int, int',
     latest('rn = 1', order='prio', cols='k, prio')[1:-1]),
    ('ties_rank', 'rank over ties (not rewritten)', 'int, varchar',
     latest('rn = 1', order='prio desc', func='rank')[1:-1]),
    ('ties_rank2', 'rank over ties, two ranks (not rewritten)', 'int, varchar',
     latest('rn <= 2', order='prio', func='rank')[1:-1]),
    # Top-n per group, then the second pass.
    ('topn_lj', 'left join to the latest row per key, unread', 'int, int',
     f"select t.id, t.v from {T} t left join {latest('rn = 1')} s on t.id = s.k"),
    ('topn_cnt', 'count per key of the latest row per key', 'int, bigint',
     f"select k, count(*) as n from {latest('rn = 1', cols='k')} x group by k"),
    ('topn_lj_two', 'left join to the two latest rows per key', 'int, int',
     f"select t.id, t.v from {T} t left join {latest('rn <= 2')} s on t.id = s.k"),
    ('topn_lj_read', 'left join to the latest row per key, its status read', 'int, varchar',
     f"select t.id, s.status from {T} t left join {latest('rn = 1')} s on t.id = s.k"),
]

BASE = ['batch_simplify_column_facts', 'batch_simplify_on_unique_key', 'batch_merge_repeated_lookups',
        'batch_rewrite_lag_duplicates']
PASS2 = 'batch_reduce_keys_after_conversions'
TOPN = 'batch_over_window_to_topn'
ALL_VARS = BASE + [PASS2, TOPN]
# Mode -> the variables set to true; every other one is false.
MODES = {
    'keys': BASE + [PASS2],
    'topn': [TOPN],
    'both': BASE + [PASS2, TOPN],
}

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(types):
    return [f"c{i + 1}" for i in range(len(types.split(',')))]


def set_vars(on):
    for var in ALL_VARS:
        stmt(f"set {var} = {'true' if var in on else 'false'}")


def run_all(title):
    out.append(f"# {title}: every query with every variable off, then in each mode.\n")
    for name, _, _, _ in CASES:
        for mode in ['off'] + list(MODES):
            stmt(f"delete from {P}_{name}_{mode}")
    set_vars([])
    for name, _, _, sql in CASES:
        stmt(f"insert into {P}_{name}_off {sql}")
    for mode, on in MODES.items():
        set_vars(on)
        for name, _, _, sql in CASES:
            stmt(f"insert into {P}_{name}_{mode} {sql}")
    set_vars([])
    out.append(f"# {title}: each mode holds the same rows as the run without the variables, as many "
               f"times each.\n")
    for name, _, types, _ in CASES:
        cols = ', '.join(columns(types))
        b = f"(select {cols}, count(*) as n from {P}_{name}_off group by {cols})"
        for mode in MODES:
            a = f"(select {cols}, count(*) as n from {P}_{name}_{mode} group by {cols})"
            query('III',
                  f"select\n  (select count(*) from ({a} except {b}) x),\n"
                  f"  (select count(*) from ({b} except {a}) y),\n"
                  f"  (select count(*) from {P}_{name}_{mode}) - (select count(*) from {P}_{name}_off)",
                  ["0 0 0"])


out.append(f"""# Differential test of `{PASS2}` and `{TOPN}`: every query is run with every
# variable off and in three modes (the reductions on keys with their second pass, the top-n alone,
# both), its rows inserted into one table per mode, and every mode must hold the same rows as the
# run without the variables, as many times each, after each change of the data.
#
# Generated by `python3 e2e_test/batch/reduce_keys_after_conversions.gen.py <this file>`: edit the
# list of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (id int primary key, x int, v int)")
stmt(f"create table {D} (id int primary key, name varchar, grp int)")
stmt(f"create table {F} (id int primary key, dim_id int, amt int, label varchar)")
stmt(f"create table {EV} (eid int primary key, k int, status varchar, prio int, ts int)")
stmt(f"create table {NOPK} (a int, b int)")
for name, comment, types, _ in CASES:
    cols = ', '.join(f"{c} {t.strip()}" for c, t in zip(columns(types), types.split(',')))
    out.append(f"# {comment}\n")
    for mode in ['off'] + list(MODES):
        stmt(f"create table {P}_{name}_{mode} ({cols})")

out.append("# Empty tables.\n")
run_all("Empty tables")

out.append("# Duplicates, NULLs, ties of the order columns, a NULL partition key, values shared by the"
           " branches of the unions.\n")
stmt(f"insert into {T} values (1, 1, 10), (2, 1, 20), (3, 2, null), (4, null, 40), (5, null, 50), "
     f"(6, 3, 60), (7, 3, 70), (8, 3, 80)")
stmt(f"insert into {NOPK} values (1, 1), (1, 2), (2, 3), (null, 4), (null, 5), (3, 6)")
stmt(f"insert into {D} values (1, 'a', 1), (2, 'b', 1), (3, 'c', null), (4, null, 2), (5, 'a', 2), "
     f"(6, 'a', null)")
stmt(f"insert into {F} values (1, 1, 10, 'a'), (2, 1, 20, 'x'), (3, 2, 30, 'b'), (4, null, 40, 'a'), "
     f"(5, 7, 50, null), (6, 7, null, 'y'), (7, 3, 70, 'c'), (8, null, null, null)")
stmt(f"insert into {EV} values "
     f"(1, 1, 'open', 1, 10), (2, 1, 'closed', 2, 20), (3, 1, 'held', 2, 30), (4, 1, 'late', null, 40), "
     f"(5, 2, 'open', 5, 10), (6, 2, 'open', 5, 10), (7, 2, 'closed', 5, 20), "
     f"(8, 3, 'new', 1, null), "
     f"(9, null, 'open', 3, 10), (10, null, 'closed', 3, 20), (11, null, null, 1, 30), "
     f"(12, 4, null, null, null), (13, 4, 'open', null, 5), "
     f"(14, 5, 'open', 1, 1), (15, 5, 'open', 2, 2), (16, 5, 'open', 3, 3), (17, 5, 'open', 4, 4), "
     f"(18, 5, 'open', 5, 5), (19, 5, 'open', 6, 6), (20, 5, 'open', 7, 7), (21, 5, 'open', 8, 8), "
     f"(22, 5, 'open', 9, 9), (23, 5, 'open', 10, 10), (24, 5, 'open', 11, 11), (25, 5, 'open', 12, 12)")
run_all("Initial rows")

out.append("# Spot checks.\n")
query('IT', f"select c1, c2 from {P}_top1_both order by c1 nulls first",
      ["NULL open", "1 late", "2 open", "3 new", "4 NULL", "5 open"])
query('I', f"select count(*) from {P}_lag_key_off", ["0"])
query('II', f"select c1, c2 from {P}_ties_rn_both order by c1 nulls first, c2 nulls first",
      ["NULL 3", "NULL 3", "1 NULL", "1 2", "2 5", "2 5", "3 1", "4 NULL", "4 NULL", "5 11", "5 12"])

out.append("# Many rows: many groups, more than ten rows in some groups, many ties.\n")
stmt(f"insert into {T} select 100 + i, i % 7, i from generate_series(1, 200) i")
stmt(f"insert into {NOPK} select i % 13, i from generate_series(1, 100) i")
stmt(f"insert into {D} select 100 + i, 'n' || (i % 9), i % 5 from generate_series(1, 150) i")
stmt(f"insert into {F} select 100 + i, case when i % 6 = 0 then null else 90 + i % 80 end, i, 'n' || (i % 7) "
     f"from generate_series(1, 300) i")
stmt(f"insert into {EV} select 100 + i, case when i % 17 = 0 then null else i % 23 end, "
     f"case i % 4 when 0 then 'open' when 1 then 'closed' when 2 then 'held' else null end, "
     f"case when i % 11 = 0 then null else i % 6 end, i % 9 from generate_series(1, 600) i")
run_all("After the inserts")

out.append("# Deletes and updates: groups shrink or vanish, keys and order columns change, NULLs move.\n")
stmt(f"delete from {T} where id % 3 = 0")
stmt(f"update {T} set x = null where id % 5 = 0")
stmt(f"delete from {D} where id % 4 = 1")
stmt(f"update {D} set name = null where id % 7 = 0")
stmt(f"update {F} set dim_id = dim_id + 1 where id % 5 = 0")
stmt(f"delete from {EV} where eid % 3 = 0 or k = 2")
stmt(f"update {EV} set prio = null where eid % 7 = 0")
stmt(f"update {EV} set k = null where eid % 13 = 0")
stmt(f"update {EV} set prio = 1 where k = 5")
run_all("After the deletes and updates")

out.append("# Some tables empty: the window input, the inner sides of the joins.\n")
stmt(f"delete from {EV}")
stmt(f"delete from {D}")
run_all("Without events and dimension rows")

out.append("# Cleanup.\n")
for name, _, _, _ in CASES:
    for mode in ['off'] + list(MODES):
        stmt(f"drop table {P}_{name}_{mode}")
for table in [T, D, F, EV, NOPK]:
    stmt(f"drop table {table}")

with open(sys.argv[1], 'w') as f:
    f.write("\n".join(out))
