"""Generates `e2e_test/batch/fold_null_contradictions.slt`, the differential test of
`batch_fold_null_contradictions` (rule `NullContradictionRule`: a filter, a scan or a join whose
condition holds both `e IS NULL` and a conjunct that can not be true when `e` is NULL produces
nothing). Every query runs twice, with and without the variable, through `INSERT ... SELECT` (the
batch plan), and both results must hold the same rows with the same multiplicities, after each
round of changes of the data.

Usage: python3 e2e_test/batch/fold_null_contradictions.gen.py e2e_test/batch/fold_null_contradictions.slt
"""
import sys

T, R, S, W = 'fnc_t', 'fnc_r', 'fnc_s', 'fnc_w'
VAR = 'batch_fold_null_contradictions'
P = 'fnc'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule.
    ('coalesce_regexp', 'id int',
     f"select id from {T} where coalesce(a, b) is null and coalesce(a, b) ~ '^[a-z]+$'"),
    ('scan_range', 'x int',
     f"select x from {S} where id is null and id ~ '^[a-z]+$'"),
    ('view_like', 'k varchar, c int',
     f"select k, c from {W} where k is null and k like 'x%'"),
    ('coalesce_argument', 'id int',
     f"select id from {T} where coalesce(a, b) is null and b = 'x'"),
    ('cast_arithmetic', 'id int',
     f"select id from {T} where (c::varchar) is null and length(c::varchar) + 1 > 3"),
    ('or_strict', 'id int',
     f"select id from {T} where c is null and (c > 10 or upper(a || c::varchar) = 'X')"),
    ('jsonb', 'id int',
     f"select id from {T} where j->>'k' is null and j->>'k' = 'x'"),
    ('greatest_not_null', 'id int',
     f"select id from {T} where greatest(c, d) is null and (c is not null or d in (1, 2))"),
    ('count_join', 'n bigint, m int',
     f"select count(*) as n, max(r.v) as m from {T} t join {R} r on t.a = r.k "
     f"where coalesce(t.a, t.b) is null and upper(coalesce(t.a, t.b)) = 'X'"),
    ('union_branch', 'id int, c int',
     f"select id, c from {T} where c > 0 union all "
     f"select id, c from {T} where coalesce(c, d) is null and coalesce(c, d) <> 0"),
    ('right_join_empty', 'k varchar, c int',
     f"select r.k, q.c from {R} r right join (select c, coalesce(a, b) as key from {T}) q "
     f"on r.k = q.key where q.key is null and q.key ~ '^[0-9]+$'"),
    ('left_join_on', 'id int, k varchar',
     f"select t.id, r.k from {T} t left join {R} r "
     f"on coalesce(t.a, t.b) is null and coalesce(t.a, t.b) = r.k"),
    ('full_join_on', 'id int, k varchar',
     f"select t.id, r.k from {T} t full join {R} r on t.c is null and t.c = r.v"),
    ('anti_join_on', 'id int',
     f"select t.id from {T} t where not exists "
     f"(select 1 from {R} r where coalesce(t.a, t.b) is null and coalesce(t.a, t.b) = r.k)"),
    # Left as they are by the rule.
    ('kept_coalesce_constant', 'id int',
     f"select id from {T} where a is null and coalesce(a, 'z') = 'z'"),
    ('kept_greatest', 'id int',
     f"select id from {T} where c is null and greatest(c, 1) > 0"),
    ('kept_concat', 'id int',
     f"select id from {T} where a is null and concat(a, 'x') = 'x'"),
    ('kept_case', 'id int',
     f"select id from {T} where c is null and case when c is null then 1 else c end = 1"),
    ('kept_distinct', 'id int',
     f"select id from {T} where c is null and c is distinct from 1"),
    ('kept_not_is_not_null', 'id int',
     f"select id from {T} where coalesce(a, b) is null and not (coalesce(a, b) is not null)"),
    ('kept_inside_or', 'id int',
     f"select id from {T} where (coalesce(a, b) is null and coalesce(a, b) ~ '^x') or d = 1"),
    ('kept_is_null_or', 'id int',
     f"select id from {T} where a is null or a ~ '^x'"),
    ('kept_other_expression', 'id int',
     f"select id from {T} where coalesce(a, b) is null and coalesce(b, a) ~ '^x'"),
    ('kept_padded_side', 'id int',
     f"select t.id from {T} t left join {R} r on t.id = r.id where r.k is null and coalesce(r.k, t.a) = 'x'"),
    ('kept_padded_null', 'id int',
     f"select t.id from {T} t left join {R} r on t.id = r.id where r.v is null and t.c > 1"),
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
        stmt(f"delete from {P}_{name}_off")
        stmt(f"delete from {P}_{name}_on")
        stmt(f"insert into {P}_{name}_off {sql}")
        stmt(f"set {VAR} = true")
        stmt(f"insert into {P}_{name}_on {sql}")
        stmt(f"set {VAR} = false")
    out.append(f"# {title}: both results hold the same rows, each the same number of times.\n")
    for name, cols, _ in CASES:
        a, b, c = f"{P}_{name}_on", f"{P}_{name}_off", columns(cols)
        grouped = lambda t: f"select {c}, count(*) as {P}_rows from {t} group by {c}"
        query('III',
              f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
              f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
              f"  (select count(*) from {a}) - (select count(*) from {b})",
              ["0 0 0"])


out.append("""# Differential test of `batch_fold_null_contradictions`: every query runs through
# `INSERT ... SELECT` without, then with the variable, and both results must hold the same rows
# with the same multiplicities, after each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/fold_null_contradictions.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (id int primary key, a varchar, b varchar, c int, d int, j jsonb)")
stmt(f"create table {R} (id int primary key, k varchar, v int)")
stmt(f"create table {S} (id varchar primary key, x int)")
stmt(f"create view {W} as select coalesce(a, b) as k, c, d from {T}")
for name, cols, _ in CASES:
    stmt(f"create table {P}_{name}_off ({cols})")
    stmt(f"create table {P}_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: NULLs in every column, values that match the patterns and values that do not.\n")
stmt(f"insert into {T} values (1, 'x', null, 1, 2, '{{\"k\": \"x\"}}'), (2, null, null, null, 1, '{{}}'), "
     f"(3, null, 'x', null, null, null), (4, 'abc', 'X', 12, null, '{{\"k\": null}}'), "
     f"(5, null, null, null, null, '{{\"k\": \"y\"}}'), (6, '42', null, 0, 3, null), (7, 'x', 'x', 1, 1, null)")
stmt(f"insert into {R} values (1, 'x', 1), (2, null, null), (3, 'abc', 12), (4, '42', 0), (5, 'x', null)")
stmt(f"insert into {S} values ('abc', 1), ('42', 2), ('x', 3)")
run_all("First data")

query('I', f"select count(*) from {P}_left_join_on_on where k is not null", ["0"])
query('I', f"select count(*) from {P}_left_join_on_on", ["7"])
query('I', f"select count(*) from {P}_kept_coalesce_constant_on", ["3"])

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {T} values (8, null, null, 5, null, '{{\"k\": \"x\"}}'), (1, null, null, null, null, null)")
stmt(f"insert into {R} values (6, null, 5), (1, 'X', 1)")
stmt(f"insert into {S} values ('y', 4)")
run_all("Second data")

out.append("# Third data: values set to NULL and back, deletes.\n")
stmt(f"update {T} set a = null, b = null where id in (4, 6)")
stmt(f"update {T} set c = 7 where id = 2")
stmt(f"delete from {T} where id = 7")
stmt(f"delete from {R} where id in (2, 3)")
run_all("Third data")

out.append("# Fourth data: an empty table on one side.\n")
stmt(f"delete from {R}")
run_all("Empty right side")
stmt(f"delete from {T}")
run_all("Empty tables again")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table {P}_{name}_on")
    stmt(f"drop table {P}_{name}_off")
stmt(f"drop view {W}")
for table in (S, R, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
