"""Generates `e2e_test/batch/inline_values_join.slt`, the differential test of
`batch_inline_values_join` (rule `ValuesJoinInlineRule`: a join with a small relation of constant
rows becomes a filter and a projection of the other side). Every query runs twice, with and
without the variable, through `INSERT ... SELECT` (the batch plan), and both results must hold the
same rows with the same multiplicities, after each round of changes of the data.

Usage: python3 e2e_test/batch/inline_values_join.gen.py e2e_test/batch/inline_values_join.slt
"""
import sys

T, N = 'ivj_t', 'ivj_n'
VAR = 'batch_inline_values_join'
M = "with m(code, label) as (values ('a', 'Alpha'), ('b', 'Beta'), (null, 'None'))"

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('semi', 'k int, code varchar',
     f"select k, code from {T} where code in (select c from (values ('a'), ('b'), ('a'), (null)) v(c))"),
    ('semi_kept_cond', 'k int',
     f"select k from {T} where x > 0 and code in (select c from (values ('a'), ('c')) v(c))"),
    ('semi_pk', 'k int, x int',
     f"select k, x from {T} where k in (select c from (values (1), (3), (3), (null)) v(c))"),
    ('anti', 'k int',
     f"select k from {T} where not exists (select 1 from (values (1), (2)) v(c) where v.c = {T}.x)"),
    ('not_in', 'k int',
     f"select k from {T} where x not in (select c from (values (1), (2)) v(c))"),
    ('semi_range', 'k int',
     f"select k from {T} where exists (select 1 from (values (1, 3), (5, 7), (2, 4)) v(lo, hi) "
     f"where {T}.x between v.lo and v.hi)"),
    ('anti_range', 'k int',
     f"select k from {T} where not exists (select 1 from (values (1, 3), (5, 7)) v(lo, hi) "
     f"where {T}.x between v.lo and v.hi)"),
    ('semi_two_keys', 'k int',
     f"select k from {T} where exists (select 1 from (values ('a', 1), ('b', 2), ('a', 3)) v(c, n) "
     f"where v.c = {T}.code and v.n = {T}.x)"),
    ('inner', 'k int, code varchar, label varchar',
     f"{M} select {T}.k, m.code, m.label from {T} join m on {T}.code = m.code"),
    ('inner_left_side', 'label varchar, k int',
     f"{M} select m.label, {T}.k from m join {T} on {T}.code = m.code"),
    ('inner_extra', 'k int, label varchar, lo int',
     f"with m(code, lo, label) as (values ('a', 0, 'Alpha'), ('b', 5, 'Beta')) "
     f"select {T}.k, m.label, m.lo from {T} join m on {T}.code = m.code and {T}.x > m.lo"),
    ('inner_pk', 'code varchar, label varchar',
     f"select {T}.code, v.label from {T} join (values (1, 'one'), (3, 'three'), (null, 'none')) v(k, label) "
     f"on {T}.k = v.k"),
    ('inner_null_label', 'k int, label varchar',
     f"with m(code, label) as (values ('a', null::varchar), ('b', 'Beta')) "
     f"select {T}.k, m.label from {T} join m on {T}.code = m.code"),
    ('inner_same_value', 'k int, grp int, label varchar',
     f"with m(code, grp, label) as (values ('a', 1, 'Alpha'), ('b', 1, 'Beta')) "
     f"select {T}.k, m.grp, m.label from {T} join m on {T}.code = m.code"),
    ('left', 'k int, code varchar, label varchar',
     f"{M} select {T}.k, m.code, m.label from {T} left join m on {T}.code = m.code"),
    ('left_kept_cond', 'k int, label varchar',
     f"{M} select {T}.k, m.label from {T} left join m on {T}.code = m.code and {T}.x > 0"),
    ('left_extra', 'k int, label varchar',
     f"with m(code, lo, label) as (values ('a', 0, 'Alpha'), ('b', 5, 'Beta')) "
     f"select {T}.k, m.label from {T} left join m on {T}.code = m.code and {T}.x > m.lo"),
    ('right', 'label varchar, k int',
     f"{M} select m.label, {T}.k from m right join {T} on {T}.code = m.code"),
    ('left_bool', 'k int, l varchar',
     f"select {T}.k, m.l from {T} left join (values (true, 'yes'), (false, 'no')) m(f, l) on {T}.flag = m.f"),
    ('dup_inner', 'x int, label varchar',
     f"{M} select {N}.x, m.label from {N} join m on {N}.code = m.code"),
    ('dup_left', 'x int, label varchar',
     f"{M} select {N}.x, m.label from {N} left join m on {N}.code = m.code"),
    ('dup_semi', 'x int',
     f"select x from {N} where code in (select c from (values ('a'), ('a')) v(c))"),
    ('agg', 'label varchar, n bigint',
     f"{M} select m.label, count(*) as n from {T} join m on {T}.code = m.code group by m.label"),
    ('cte_twice', 'k int, n int',
     f"with m(code, n) as (values ('a', 1), ('b', 2)) "
     f"select {T}.k, m.n from {T} join m on {T}.code = m.code where {T}.x in (select n from m)"),
    # Left as they are by the rule.
    ('kept_dup_key', 'k int, label varchar',
     f"with m(code, label) as (values ('a', 'A1'), ('a', 'A2'), ('b', 'B')) "
     f"select {T}.k, m.label from {T} join m on {T}.code = m.code"),
    ('kept_dup_key_left', 'k int, label varchar',
     f"with m(code, label) as (values ('a', 'A1'), ('a', 'A2'), ('b', 'B')) "
     f"select {T}.k, m.label from {T} left join m on {T}.code = m.code"),
    ('kept_range_inner', 'k int, label varchar',
     f"select {T}.k, v.label from {T} join (values (1, 3, 'low'), (2, 6, 'mid')) v(lo, hi, label) "
     f"on {T}.x between v.lo and v.hi"),
    ('kept_full', 'k int, label varchar',
     f"{M} select {T}.k, m.label from {T} full join m on {T}.code = m.code"),
    ('kept_preserved', 'label varchar, k int',
     f"{M} select m.label, {T}.k from m left join {T} on {T}.code = m.code"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def run_all(title):
    out.append(f"# {title}: every query runs without, then with the variable.\n")
    for name, _, sql in CASES:
        stmt(f"delete from ivj_{name}_off")
        stmt(f"delete from ivj_{name}_on")
        stmt(f"insert into ivj_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into ivj_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"ivj_{name}_on", f"ivj_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as ivj_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append("""# Differential test of `batch_inline_values_join`: every query runs through
# `INSERT ... SELECT` without, then with the variable, and both results must hold the same rows
# with the same multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/inline_values_join.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (k int primary key, code varchar, x int, flag boolean)")
stmt(f"create table {N} (code varchar, x int)")
for name, cols, _ in CASES:
    stmt(f"create table ivj_{name}_off ({cols})")
    stmt(f"create table ivj_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: codes with and without a constant row, NULL codes and values, duplicates.\n")
stmt(f"insert into {T} values (1, 'a', 1, true), (2, 'b', 2, false), (3, 'c', 3, null), "
     f"(4, null, 5, true), (5, 'a', null, false), (6, 'b', 6, true), (7, 'a', -1, null), (8, 'z', 0, false)")
stmt(f"insert into {N} values ('a', 1), ('a', 1), ('b', 2), (null, 3), ('c', 4), ('a', null)")
run_all("First data")

query('I', f"select count(*) from ivj_left_on where label is null", ["3"])
query('I', f"select count(*) from ivj_semi_on", ["5"])
query('I', f"select count(*) from ivj_dup_inner_on", ["4"])

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {T} values (9, 'b', 4, true), (10, null, null, null), (1, 'b', 7, false)")
stmt(f"insert into {N} values ('b', 2), (null, null)")
run_all("Second data")

out.append("# Third data: codes set to NULL and back, deletes.\n")
stmt(f"update {T} set code = null where k in (2, 3)")
stmt(f"update {T} set code = 'a' where k = 4")
stmt(f"update {T} set x = 2 where k = 5")
stmt(f"delete from {T} where k in (6, 8)")
stmt(f"delete from {N} where code = 'a' and x = 1")
run_all("Third data")

out.append("# Fourth data: no row matches any constant row, then empty tables.\n")
stmt(f"update {T} set code = 'q', x = 100")
stmt(f"delete from {N} where code is not null")
run_all("No match")
stmt(f"delete from {T}")
stmt(f"delete from {N}")
run_all("Empty again")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table ivj_{name}_on")
    stmt(f"drop table ivj_{name}_off")
for table in (N, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
