"""Generates `e2e_test/batch/simplify_null_join_keys.slt`, the differential test of
`batch_simplify_null_join_keys` (rule `NullKeyJoinRule`: a join whose condition compares with `=`
a column known to be NULL matches nothing). Every query runs twice, with and without the variable,
through `INSERT ... SELECT` (the batch plan), and both results must hold the same rows with the
same multiplicities, after each round of changes of the data.

Usage: python3 e2e_test/batch/simplify_null_join_keys.gen.py e2e_test/batch/simplify_null_join_keys.slt
"""
import sys

A, B, C, E, N, V = 'nk_a', 'nk_b', 'nk_c', 'nk_e', 'nk_n', 'nk_v'
VAR = 'batch_simplify_null_join_keys'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('inner_right', 'id int, label varchar',
     f"select a.id, b.label from {A} a join {B} b on a.bk = b.k where b.k is null"),
    ('inner_left', 'id int, label varchar',
     f"select a.id, b.label from {A} a join {B} b on a.bk = b.k where a.bk is null"),
    ('view', 'id int, x int, b_k int, label varchar',
     f"select * from {V} where b_k is null"),
    ('cast', 'id int, v int',
     f"select a.id, c.v from {A} a join {C} c on a.bk = c.kb where a.bk is null"),
    ('null_literal', 'id int, label varchar',
     f"select a.id, s.label from {A} a join (select null::int as k, label from {B}) s on a.bk = s.k"),
    ('count_inner', 'n bigint, s bigint, m int',
     f"select count(*) as n, sum(a.x) as s, max(b.id) as m from {A} a join {B} b on a.bk = b.k "
     f"where b.k is null"),
    ('left_right_null', 'id int, x int, label varchar',
     f"select a.id, a.x, b.label from {A} a left join (select * from {B} where k is null) b on a.bk = b.k"),
    ('left_extra', 'id int, label varchar, bid int',
     f"select a.id, b.label, b.id as bid from {A} a left join (select * from {B} where k is null) b "
     f"on a.bk = b.k and a.x > b.id"),
    ('right_left_null', 'x int, id int, label varchar',
     f"select a.x, b.id, b.label from (select * from {A} where bk is null) a right join {B} b on a.bk = b.k"),
    ('semi_right', 'id int',
     f"select a.id from {A} a where a.bk in (select k from {B} where k is null)"),
    ('semi_left', 'id int',
     f"select a.id from {A} a where a.bk is null and exists (select 1 from {B} b where b.k = a.bk)"),
    ('anti_right', 'id int',
     f"select a.id from {A} a where not exists (select 1 from {B} b where b.k = a.bk and b.k is null)"),
    ('scan_mapping', 'id int, label varchar',
     f"select a.id, s.label from {A} a join (select label, k from {B} where k is null) s on a.bk = s.k"),
    ('only_one', 'id int, label varchar, v int',
     f"select a.id, b.label, c.v from {A} a left join (select * from {B} where k is null) b on a.bk = b.k "
     f"join {C} c on a.id = c.id"),
    ('duplicates', 'v int, label varchar',
     f"select n.v, b.label from {N} n left join (select * from {B} where k is null) b on n.k = b.k"),
    ('duplicates_inner', 'v int, label varchar',
     f"select n.v, b.label from {N} n join {B} b on n.k = b.k where n.k is null"),
    ('not_in', 'id int',
     f"select a.id from {A} a where a.bk not in (select k from {B} where k is null)"),
    # The null key on the preserved side of an outer join, through unions and joins.
    ('preserved_union_agg', 'id int, lab varchar',
     f"select u.id, r.lab from (select id, bk from {A} where bk is null union all "
     f"select id, k from {B} where k is null) u "
     f"left join (select k, max(label) as lab from {B} group by k) r on u.bk = r.k"),
    ('preserved_union_literal', 'id int, label varchar',
     f"select u.id, b.label from (select id, null::int as bk from {A} union all "
     f"select id, k from {B} where k is null) u left join {B} b on u.bk = b.k"),
    ('preserved_topn_join', 'id int, v int',
     f"select x.id, c.v from (select id, bk from (select a.id, a.bk, "
     f"row_number() over (partition by a.id order by b.id) as rn from {A} a join {B} b on a.id = b.id "
     f"where a.bk is null) t where rn = 1) x left join {C} c on x.bk = c.kb"),
    ('preserved_right', 'x int, id int, label varchar',
     f"select a.x, b.id, b.label from {A} a right join (select * from {B} where k is null) b on a.bk = b.k"),
    ('preserved_anti', 'id int',
     f"select a.id from {A} a where a.bk is null and not exists (select 1 from {B} b where b.k = a.bk)"),
    ('full_left_null', 'id int, x int, label varchar',
     f"select a.id, a.x, b.label from (select * from {A} where bk is null) a full join {B} b on a.bk = b.k"),
    # Left as they are by the rule.
    ('kept_one_branch', 'id int, label varchar',
     f"select u.id, b.label from (select id, bk from {A} where bk is null union all "
     f"select id, k from {B}) u left join {B} b on u.bk = b.k"),
    ('kept_union_indf', 'id int, label varchar',
     f"select u.id, b.label from (select id, bk from {A} where bk is null union all "
     f"select id, k from {B} where k is null) u left join {B} b on u.bk is not distinct from b.k"),
    ('kept_indf', 'id int, label varchar',
     f"select a.id, b.label from {A} a join {B} b on a.bk is not distinct from b.k where b.k is null"),
    ('full_padded', 'id int, label varchar',
     f"select a.id, b.label from {A} a full join (select * from {B} where k is null) b on a.bk = b.k"),
    ('kept_label', 'id int, label varchar',
     f"select a.id, b.label from {A} a join {B} b on a.bk = b.k where b.label is null"),
    ('kept_mapping', 'id int, p int',
     f"select a.id, e.p from {A} a join {E} e on a.bk = e.w where e.u is null"),
    ('kept_anti_pattern', 'id int',
     f"select a.id from {A} a left join {B} b on a.bk = b.k where b.k is null"),
    ('preserved_left', 'id int, label varchar',
     f"select a.id, b.label from (select * from {A} where bk is null) a left join {B} b on a.bk = b.k"),
    ('kept_coalesce', 'id int, label varchar',
     f"select a.id, b.label from {A} a join {B} b on coalesce(a.bk, 0) = b.k where a.bk is null"),
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
        stmt(f"delete from nk_{name}_off")
        stmt(f"delete from nk_{name}_on")
        stmt(f"insert into nk_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into nk_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"nk_{name}_on", f"nk_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as nk_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append("""# Differential test of `batch_simplify_null_join_keys`: every query runs through
# `INSERT ... SELECT` without, then with the variable, and both results must hold the same rows
# with the same multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/simplify_null_join_keys.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {A} (id int primary key, bk int, x int)")
stmt(f"create table {B} (id int primary key, k int, label varchar)")
stmt(f"create table {C} (id int primary key, kb bigint, v int)")
stmt(f"create table {E} (p int primary key, u int, w int)")
stmt(f"create table {N} (k int, v int)")
stmt(f"create view {V} as select a.id, a.x, b.k as b_k, b.label from {A} a join {B} b on a.bk = b.k")
for name, cols, _ in CASES:
    stmt(f"create table nk_{name}_off ({cols})")
    stmt(f"create table nk_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULL keys on both sides, duplicates, rows without a match, a key 0.\n")
stmt(f"insert into {A} values (1, 1, 5), (2, 1, 0), (3, null, 2), (4, null, null), (5, 2, 7), "
     f"(6, 0, 3), (7, 9, 1), (8, null, 9)")
stmt(f"insert into {B} values (1, 1, 'one'), (2, 1, 'uno'), (3, null, 'nk'), (4, null, null), "
     f"(5, 0, 'zero'), (6, 2, null), (7, null, 'nk2')")
stmt(f"insert into {C} values (1, 1, 10), (2, null, 20), (3, 0, 30), (5, 2, 50), (8, null, 80)")
stmt(f"insert into {E} values (1, null, 1), (2, null, 2), (3, 5, 1), (4, null, null), (5, 7, 0)")
stmt(f"insert into {N} values (1, 1), (1, 1), (null, 2), (null, 2), (0, 3), (null, null), (2, 4)")
run_all("First data")

query('I', f"select count(*) from nk_left_right_null_on where label is not null", ["0"])
query('I', f"select count(*) from nk_anti_right_on", ["8"])
query('I', f"select count(*) from nk_kept_coalesce_on", ["3"])

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {A} values (9, null, 4), (10, 2, 2), (1, null, 5)")
stmt(f"insert into {B} values (8, 9, 'nine'), (9, null, 'nk3'), (1, null, 'one')")
stmt(f"insert into {C} values (9, null, 90), (10, 2, 100)")
stmt(f"insert into {E} values (6, null, 9), (7, null, 2)")
stmt(f"insert into {N} values (null, 5), (9, 6)")
run_all("Second data")

out.append("# Third data: keys set to NULL and back, deletes.\n")
stmt(f"update {A} set bk = 0 where id in (3, 4)")
stmt(f"update {A} set bk = null where id in (5, 6)")
stmt(f"update {B} set k = 2 where id = 3")
stmt(f"update {B} set k = null where id in (5, 8)")
stmt(f"delete from {B} where id in (2, 4)")
stmt(f"delete from {A} where id = 2")
stmt(f"update {E} set u = 1 where p = 1")
stmt(f"delete from {N} where k is null and v = 2")
run_all("Third data")

out.append("# Fourth data: only NULL keys left on the right side, then no row on the right side.\n")
stmt(f"delete from {B} where k is not null")
run_all("Only NULL keys on the right side")
stmt(f"delete from {B}")
run_all("Empty right side")

out.append("# Fifth data: the right side filled again, the left side emptied.\n")
stmt(f"insert into {B} values (1, 1, 'one'), (2, null, 'nk'), (3, 0, 'zero')")
stmt(f"delete from {A}")
stmt(f"delete from {N}")
run_all("Empty left side")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table nk_{name}_on")
    stmt(f"drop table nk_{name}_off")
stmt(f"drop view {V}")
for table in (N, E, C, B, A):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
