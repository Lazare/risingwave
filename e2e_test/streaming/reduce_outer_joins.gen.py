"""Generates `e2e_test/streaming/reduce_outer_joins.slt`, the differential test of
`streaming_reduce_outer_joins` (rule `OuterJoinReduceRule`: an outer join whose padded rows a filter
or a join condition right above rejects becomes an inner join, and `LEFT JOIN r ... WHERE r.c IS
NULL` becomes an anti join when `r.c` is never NULL in a matched row): every query is materialized
with and without the variable, and both materialized views must hold the same rows, with the same
multiplicities, as the batch query, after the backfill and after each round of changes.

Usage: python3 e2e_test/streaming/reduce_outer_joins.gen.py e2e_test/streaming/reduce_outer_joins.slt
"""
import sys

T = 'sro_t'
R = 'sro_r'
S = 'sro_s'
N = 'sro_n'
VAR = 'streaming_reduce_outer_joins'

# (name, columns of the result, query)
CASES = [
    # Rewritten by the rule: outer join to inner join.
    ('left_like', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where lower(r.name) like 'a%'"),
    ('left_is_not_null', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where r.name is not null"),
    ('left_in', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where r.c in (1, 2)"),
    ('left_arithmetic', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where r.c + 1 > t.x"),
    ('left_or', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where r.c > 1 or r.name = 'x'"),
    ('left_coalesce', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where coalesce(r.c, r.k) > 0"),
    ('left_not', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where not (r.c > 5)"),
    ('left_is_true', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where (r.c > 5) is true"),
    ('right_abs', 'id, name',
     f"select t.id, r.name from {T} t right join {R} r on t.k = r.id where abs(t.y) > 1"),
    ('full_one_side', 'id, rid',
     f"select t.id, r.id as rid from {T} t full join {R} r on t.k = r.id where t.y is not null"),
    ('full_both_sides', 'id, rid',
     f"select t.id, r.id as rid from {T} t full join {R} r on t.k = r.id where t.y is not null and r.c is not null"),
    ('inner_above', 'id, v',
     f"select t.id, s.v from {T} t left join {R} r on t.k = r.id join {S} s on s.k = r.c"),
    ('semi_above', 'id',
     f"select t.id from {T} t left join {R} r on t.k = r.id where exists (select 1 from {S} s where s.k = r.c)"),
    # Rewritten by the rule: outer join to anti join.
    ('anti_key', 'id',
     f"select t.id from {T} t left join {R} r on t.k = r.k where r.k is null"),
    ('anti_nullable_eq', 'id, x',
     f"select t.id, t.x from {T} t left join {S} s on t.k = s.k where s.k is null"),
    ('anti_not_null', 'id',
     f"select t.id from {T} t left join {R} r on t.k = r.c where r.k is null"),
    ('anti_pk', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where r.id is null"),
    ('anti_other', 'id, v',
     f"select t.id, s.v from {T} t left join {S} s on t.k = s.k where s.k is null and coalesce(s.v, 0) = 0"),
    ('anti_right', 'id, name',
     f"select r.id, r.name from {T} t right join {R} r on t.k = r.id where t.id is null"),
    ('anti_full', 'id, rid',
     f"select t.id, r.id as rid from {T} t full join {R} r on t.k = r.c where r.k is null"),
    ('anti_count', 'n',
     f"select count(*) as n from {T} t left join {S} s on t.k = s.k where s.k is null"),
    ('anti_duplicates', 'k, v',
     f"select n.k, n.v from {N} n left join {S} s on n.k = s.k where s.k is null"),
    # Left as they are by the rule.
    ('kept_or_null', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where r.c is null or r.c > 5"),
    ('kept_coalesce', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where coalesce(r.c, 0) = 0"),
    ('kept_is_not_true', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where (r.c > 5) is not true"),
    ('kept_greatest', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where greatest(r.c, t.x) > 0"),
    ('kept_case', 'id, name',
     f"select t.id, r.name from {T} t left join {R} r on t.k = r.id where case when r.c is null then true else r.c > 5 end"),
    ('kept_left_above', 'id, v',
     f"select t.id, s.v from {T} t left join {R} r on t.k = r.id left join {S} s on s.k = r.c"),
    ('kept_anti_above', 'id',
     f"select t.id from {T} t left join {R} r on t.k = r.id where not exists (select 1 from {S} s where s.k = r.c)"),
    ('kept_nullable', 'id',
     f"select t.id from {T} t left join {S} s on t.k = s.k where s.v is null"),
    ('kept_full_eq', 'id, sid',
     f"select t.id, s.id as sid from {T} t full join {S} s on t.k = s.k where s.k is null"),
    ('kept_indf', 'id',
     f"select t.id from {T} t left join {S} s on t.k is not distinct from s.k where s.k is null"),
    ('kept_coalesce_on', 'id',
     f"select t.id from {T} t left join {S} s on t.k = coalesce(s.k, 0) where s.k is null"),
]

out = []


def stmt(sql):
    out.append(f"statement ok\n{sql};\n")


def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")


def same_rows(a, b, cols):
    """Both relations hold the same rows, each the same number of times."""
    grouped = lambda t: f"select {cols}, count(*) as sro_rows from {t} group by {cols}"
    query('III',
          f"select\n  (select count(*) from ({grouped(a)} except {grouped(b)}) x),\n"
          f"  (select count(*) from ({grouped(b)} except {grouped(a)}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])


def compare_all(title):
    out.append(f"# {title}: with and without the rule, every materialized view holds the rows of "
               f"the batch query.\n")
    for name, cols, sql in CASES:
        same_rows(f"sro_{name}_on", f"sro_{name}_off", cols)
        same_rows(f"sro_{name}_on", f"({sql}) q", cols)


out.append("""# Differential test of `streaming_reduce_outer_joins`: every query is materialized twice, with and without
# the variable, and both materialized views must hold the rows of the batch query, with the same
# multiplicities, after the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/reduce_outer_joins.gen.py <this file>`: edit the list of queries
# there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (id int primary key, k int, x int not null, y int)")
stmt(f"create table {R} (id int primary key, k int not null, c int, name varchar)")
stmt(f"create table {S} (id int primary key, k int, v int)")
stmt(f"create table {N} (k int, v int)")
out.append("# Rows present before the materialized views are created (first data: NULL keys and values, duplicates, rows without a match on both sides): they go\n# through the backfill.\n")
stmt(f"insert into {T} values (1, 1, 1, 5), (2, 2, 2, null), (3, null, 3, -4), (4, 4, 4, 0), (5, 9, 5, 2), (6, 0, 0, null), (7, 1, 7, 3)")
stmt(f"insert into {R} values (1, 1, 1, 'alpha'), (2, 2, null, 'beta'), (3, 3, 6, null), (4, 9, 2, 'axe'), (5, 4, null, 'x'), (6, 0, 0, 'a')")
stmt(f"insert into {S} values (1, 1, 10), (2, 1, null), (3, null, 30), (4, 2, 0), (5, 6, null), (6, null, null)")
stmt(f"insert into {N} values (1, 1), (1, 1), (null, 2), (null, 2), (5, 3), (2, null), (7, 7)")

for sfx in ('off', 'on'):
    stmt(f"set {VAR} = {'true' if sfx == 'on' else 'false'}")
    for name, _, sql in CASES:
        stmt(f"create materialized view sro_{name}_{sfx} as {sql}")
stmt(f"set {VAR} = false")

compare_all("After the backfill")

out.append("# Spot checks of the rows themselves.\n")
query('I', "select count(*) from sro_anti_key_on", ["1"])
query('I', "select count(*) from sro_anti_duplicates_on where k is null", ["2"])

out.append("# Second data: more rows, rows written again with an existing primary key.\n")
stmt(f"insert into {T} values (8, 6, 8, 9), (9, null, 9, null), (1, 3, 1, 5)")
stmt(f"insert into {R} values (7, 7, 7, 'apple'), (8, 1, null, null), (2, 2, 1, 'beta')")
stmt(f"insert into {S} values (7, 9, 90), (8, 7, null), (1, 1, 11)")
stmt(f"insert into {N} values (null, 8), (9, 9), (1, 1)")
compare_all("After: second data: more rows, rows written again with an existing primary key")

out.append("# Third data: values set to NULL and back, deletes.\n")
stmt(f"update {T} set k = null where id in (1, 4)")
stmt(f"update {T} set k = 7, y = null where id = 3")
stmt(f"update {R} set c = null, name = null where id in (1, 4)")
stmt(f"update {R} set c = 9 where id = 5")
stmt(f"update {S} set k = null where id in (1, 7)")
stmt(f"update {S} set k = 4 where id = 3")
stmt(f"delete from {R} where id = 6")
stmt(f"delete from {S} where id in (2, 4)")
stmt(f"delete from {T} where id = 5")
stmt(f"delete from {N} where k is null and v = 2")
compare_all("After: third data: values set to NULL and back, deletes")

out.append("# Fourth data: the optional side emptied.\n")
stmt(f"delete from {R}")
stmt(f"delete from {S}")
compare_all("After: fourth data: the optional side emptied")

out.append("# Fifth data: the optional side filled again, the preserved side emptied.\n")
stmt(f"insert into {R} values (1, 1, 1, 'a1'), (2, 2, null, null), (3, 0, 2, 'b')")
stmt(f"insert into {S} values (1, 1, 1), (2, null, null), (3, 0, 3)")
stmt(f"delete from {T}")
stmt(f"delete from {N}")
compare_all("After: fifth data: the optional side filled again, the preserved side emptied")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view sro_{name}_{sfx}")
for table in (N, S, R, T):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
