"""Generates `e2e_test/batch/share_identical_subplans.slt`, the differential test of
`batch_share_identical_subplans` (batch spool: the identical sub-plans of a stage are computed
once per task and read again by the other copies). Every query runs twice, with and without the
variable, through `INSERT ... SELECT` (the batch plan), in the local and in the distributed query
mode, and both results must hold the same rows with the same multiplicities, after each round of
changes of the data.

The queries avoid ties where the result would depend on them (`last_value` ordered by a unique
column, a top-n ordered by a unique column), so that two runs of the same plan agree.

Usage: python3 e2e_test/batch/share_identical_subplans.gen.py e2e_test/batch/share_identical_subplans.slt
"""
import sys

T, D, L = 'sp_t', 'sp_d', 'sp_latest'
VAR = 'batch_share_identical_subplans'
MODES = ['local', 'distributed']

# (name, columns of the result, query)
CASES = [
    # Shared copies.
    ('self_join', 'k int, v int, n bigint',
     f"select a.k, a.v, b.n from {L} a join {L} b on a.k = b.k"),
    ('self_left', 'k int, n bigint, v int',
     f"select a.k, a.n, b.v from {L} a left join {L} b on a.k = b.k"),
    ('self_full', 'k int, k2 int',
     f"select a.k, b.k as k2 from {L} a full join {L} b on a.k = b.k + 1"),
    ('two_branches', 'name varchar, v int, n bigint',
     f"select d.name, a.v, b.n from {D} d join {L} a on d.k = a.k left join {L} b on d.k = b.k"),
    ('two_keys', 'name varchar, v int, v2 int',
     f"select d.name, a.v, b.v as v2 from {D} d join {L} a on d.k = a.k join {L} b on d.k2 = b.k"),
    ('union_same', 'k int, v int',
     f"select k, v from {L} union all select k, v from {L}"),
    ('union_filters', 'k int, v int',
     f"select k, v from {L} where v > 0 union all select k, n::int from {L} where n > 1"),
    ('filters_above', 'k int, v int, n bigint',
     f"select a.k, a.v, b.n from {L} a join {L} b on a.k = b.k where a.v > 0 and b.n > 1"),
    ('scalar_twice', 'k int, v int',
     f"select k, v from {T} where v > (select avg(v) from {T}) and ts > (select avg(v) from {T})"),
    ('two_scalar_aggs', 'c bigint, m int',
     f"select (select count(*) from {L}) as c, (select max(v) from {L}) as m"),
    ('agg_over_join', 'name varchar, c bigint, m int',
     f"with g as (select d.name, count(*) as c, max(t.v) as m from {T} t join {D} d on t.k = d.k "
     f"group by d.name) select a.name, a.c, b.m from g a join g b on a.name = b.name"),
    ('impure_above', 'k int, rn boolean, rn2 boolean',
     f"with x as (select k, sum(v) * random() as r from {T} group by k) "
     f"select a.k, a.r is null as rn, b.r is null as rn2 from x a join x b on a.k = b.k"),
    ('top_n', 'k int, v int',
     f"with top as (select k, v, ts from {T} order by v desc nulls last, ts limit 3) "
     f"select a.k, b.v from top a join top b on a.k = b.k"),
    ('three_copies', 'k int, v int, n bigint',
     f"select a.k, b.v, c.n from {L} a join {L} b on a.k = b.k join {L} c on a.v = c.k"),
    ('semi', 'k int, v int',
     f"select k, v from {L} a where exists (select 1 from {L} b where b.k = a.k and b.n > 1)"),
    ('anti', 'k int, v int',
     f"select k, v from {L} a where not exists (select 1 from {L} b where b.k = a.k + 1)"),
    # Copies left as they are.
    ('kept_impure', 'k int, rn boolean',
     f"with x as (select k, sum(v * random()) as r from {T} group by k) "
     f"select a.k, a.r is null as rn from x a join x b on a.k = b.k"),
    ('kept_scan', 'k int, v int',
     f"with x as (select k, v from {T} where v > 0) select a.k, b.v from x a join x b on a.k = b.k"),
    ('kept_pushed', 'k int, v int, n bigint',
     f"select a.k, a.v, b.n from {L} a left join {L} b on a.k = b.k and b.k > 5"),
    ('kept_diff_aggs', 'k int, c bigint, s bigint',
     f"select a.k, a.c, b.s from (select k, count(*) as c from {T} group by k) a "
     f"join (select k, sum(v) as s from {T} group by k) b on a.k = b.k"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    for mode in MODES:
        out.append(f"# {title}, {mode} mode: every query runs without, then with the variable.\n")
        stmt(f"set query_mode = {mode}")
        for name, _, sql in CASES:
            stmt(f"delete from sp_{name}_off")
            stmt(f"delete from sp_{name}_on")
            stmt(f"insert into sp_{name}_off {sql}")
            stmt(f"set {VAR} = true")
            stmt(f"insert into sp_{name}_on {sql}")
            stmt(f"set {VAR} = false")
        stmt("set query_mode = auto")
        out.append(f"# {title}, {mode} mode: both results hold the same rows, each the same number "
                   f"of times.\n")
        for name, cols, _ in CASES:
            a, b, c = f"sp_{name}_on", f"sp_{name}_off", columns(cols)
            grouped = lambda t: f"select {c}, count(*) as sp_rows from {t} group by {c}"
            query('III',
                  f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
                  f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
                  f"  (select count(*) from {a}) - (select count(*) from {b})",
                  ["0 0 0"])


out.append("""# Differential test of `batch_share_identical_subplans`: every query runs through
# `INSERT ... SELECT` without, then with the variable, in the local and in the distributed query
# mode, and both results must hold the same rows with the same multiplicities, after each round of
# changes of the data.
#
# Generated by `python3 e2e_test/batch/share_identical_subplans.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (k int, v int, ts int)")
stmt(f"create table {D} (k int primary key, name varchar, k2 int)")
stmt(f"create view {L} as select k, last_value(v order by ts) as v, count(*) as n from {T} group by k")
for name, cols, _ in CASES:
    stmt(f"create table sp_{name}_off ({cols})")
    stmt(f"create table sp_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULL keys and values, duplicate rows, groups of one row, keys without a "
           "match. `ts` is unique.\n")
stmt(f"insert into {T} values (1, 10, 1), (1, 20, 2), (1, 20, 3), (2, null, 4), (2, 5, 5), "
     f"(null, 7, 6), (null, null, 7), (3, 3, 8), (3, 3, 9), (4, -1, 10), (5, 0, 11), (6, 2, 12), "
     f"(7, 3, 13), (20, 1, 14)")
stmt(f"insert into {D} values (1, 'one', 2), (2, 'two', 3), (3, null, 1), (4, 'four', null), "
     f"(8, 'eight', 1), (9, 'two', 9)")
run_all("First data")

query('I', f"select count(*) from sp_self_join_on", ["8"])
query('I', f"select count(*) from sp_union_same_on", ["18"])

out.append("# Second data: more rows, among them a new last value for existing keys.\n")
stmt(f"insert into {T} values (1, null, 15), (2, 8, 16), (8, 4, 17), (8, 4, 18), (null, 1, 19), "
     f"(9, 9, 20), (3, 1, 21)")
stmt(f"insert into {D} values (5, 'five', 5), (6, null, null), (7, 'seven', 8)")
run_all("Second data")

out.append("# Third data: updates and deletes.\n")
stmt(f"update {T} set v = null where k = 3")
stmt(f"update {T} set k = 4 where ts in (12, 13)")
stmt(f"delete from {T} where k is null and v is null")
stmt(f"delete from {T} where ts = 2")
stmt(f"update {D} set k2 = null where k = 1")
stmt(f"delete from {D} where k = 9")
run_all("Third data")

out.append("# Fourth data: no row in the dimension table.\n")
stmt(f"delete from {D}")
run_all("Empty dimension table")

out.append("# Fifth data: the dimension table filled again, the fact table emptied.\n")
stmt(f"insert into {D} values (1, 'one', 2), (2, null, 1)")
stmt(f"delete from {T}")
run_all("Empty fact table")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table sp_{name}_on")
    stmt(f"drop table sp_{name}_off")
stmt(f"drop view {L}")
for table in (D, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
