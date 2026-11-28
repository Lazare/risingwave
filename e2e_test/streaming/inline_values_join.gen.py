"""Generates `e2e_test/streaming/inline_values_join.slt`, the differential test of
`streaming_inline_values_join` (rule `ValuesJoinInlineRule`: a join with a small relation of
constant rows becomes a filter and a projection of the other side): every query is materialized
with and without the variable, and both materialized views must hold the same rows, with the same
multiplicities, as the batch query, after the backfill and after each round of changes.

Usage: python3 e2e_test/streaming/inline_values_join.gen.py e2e_test/streaming/inline_values_join.slt
"""
import sys

T, N = 'sivj_t', 'sivj_n'
VAR = 'streaming_inline_values_join'
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

# Queries whose join has no equality: without the rule, streaming can not plan them (nested-loop
# join), so only the materialized view with the variable is created and compared with batch.
ON_ONLY = {'semi_range', 'anti_range'}
# Not plannable in streaming at all (inner join without equality, kept by the rule).
CASES = [case for case in CASES if case[0] != 'kept_range_inner']

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def columns(cols):
    return ', '.join(col.split()[0] for col in cols.split(', '))


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    grouped = lambda t: f"select {cols}, count(*) as sivj_rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def compare_all(title):
    out.append(f"# {title}: with and without the rule, every materialized view holds the rows of "
               f"the batch query.\n")
    for name, cols, sql in CASES:
        c = columns(cols)
        if name not in ON_ONLY:
            same_rows(f"sivj_{name}_on", f"sivj_{name}_off", c)
        same_rows(f"sivj_{name}_on", f"({sql}) q", c)


out.append("""# Differential test of `streaming_inline_values_join`: every query is materialized twice, with
# and without the variable, and both materialized views must hold the rows of the batch query,
# with the same multiplicities, after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/inline_values_join.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (k int primary key, code varchar, x int, flag boolean)")
stmt(f"create table {N} (code varchar, x int)")
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {T} values (1, 'a', 1, true), (2, 'b', 2, false), (3, 'c', 3, null), "
     f"(4, null, 5, true), (5, 'a', null, false), (6, 'b', 6, true), (7, 'a', -1, null), (8, 'z', 0, false)")
stmt(f"insert into {N} values ('a', 1), ('a', 1), ('b', 2), (null, 3), ('c', 4), ('a', null)")

for sfx in ('off', 'on'):
    stmt(f"set {VAR} = {'true' if sfx == 'on' else 'false'}")
    for name, _, sql in CASES:
        if sfx == 'off' and name in ON_ONLY:
            continue
        stmt(f"create materialized view sivj_{name}_{sfx} as {sql}")
stmt(f"set {VAR} = false")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('I', "select count(*) from sivj_left_on where label is null", ["3"])
query('I', "select count(*) from sivj_semi_on", ["5"])
query('I', "select count(*) from sivj_dup_inner_on", ["4"])

out.append("# First round of changes: new rows, rows written again with an existing primary key.\n")
stmt(f"insert into {T} values (9, 'b', 4, true), (10, null, null, null), (1, 'b', 7, false)")
stmt(f"insert into {N} values ('b', 2), (null, null), ('a', 1)")
compare_all("After the inserts")

out.append("# Second round of changes: codes set to NULL and back, deletes (retractions).\n")
stmt(f"update {T} set code = null where k in (2, 3)")
stmt(f"update {T} set code = 'a' where k = 4")
stmt(f"update {T} set x = 2 where k = 5")
stmt(f"update {T} set flag = not flag where k in (1, 4)")
stmt(f"delete from {T} where k in (6, 8)")
stmt(f"delete from {N} where code = 'a' and x = 1")
compare_all("After the updates and deletes")

out.append("# Third round of changes: no row matches any constant row, then emptied and filled again.\n")
stmt(f"update {T} set code = 'q', x = 100")
stmt(f"delete from {N} where code is not null")
compare_all("After no row matches")
stmt(f"delete from {T}")
stmt(f"delete from {N}")
compare_all("After the tables are emptied")
stmt(f"insert into {T} values (1, 'a', 1, true), (2, 'b', 5, null), (3, null, 2, false)")
stmt(f"insert into {N} values ('a', 1), ('b', 2), ('b', 2)")
compare_all("After the tables are filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        if sfx == 'off' and name in ON_ONLY:
            continue
        stmt(f"drop materialized view sivj_{name}_{sfx}")
for table in (N, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
