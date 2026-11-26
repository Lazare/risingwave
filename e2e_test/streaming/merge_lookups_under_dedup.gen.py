"""Generates `e2e_test/streaming/merge_lookups_under_dedup.slt`, the differential test of the
removals under consumers that do not see duplicates (`TopOneDefaultBranchRule`,
`UnusedJoinUnderDedupRule`): every query is materialized with and without
`streaming_merge_lookups_under_dedup`.

Usage: python3 e2e_test/streaming/merge_lookups_under_dedup.gen.py e2e_test/streaming/merge_lookups_under_dedup.slt
"""
import sys

T, A, B, Q, X = 'mud_t', 'mud_a', 'mud_b', 'mud_q', 'mud_x'


def status_view(name, branches, order='desc'):
    """A top-1 per id over a union of (id, status, priority) branches."""
    union = '\n        union all '.join(branches)
    return (f"create view {name} as select id, status, p from (\n"
            f"    select id, status, p, row_number() over (partition by id order by p {order}) as rn from (\n"
            f"        {union}\n"
            f"    ) u\n"
            f") z where rn = 1")


ST, SC, SN, STIE, SASC = 'mud_st', 'mud_sc', 'mud_sn', 'mud_stie', 'mud_sasc'
VIEWS = [
    # The default branch reads T with the lowest priority, the others are better.
    status_view(ST, [f"select id, 'initiated' as status, 1 as p from {T}",
                     f"select id, 'active', 2 from {A} where ok",
                     f"select id, 'blocked', 3 from {B}"]),
    # A priority given by a CASE of literals.
    status_view(SC, [f"select id, 'open' as status, 1 as p from {T}",
                     f"select id, case when ok then 'approved' else 'rejected' end, case when ok then 4 else 5 end from {A}",
                     f"select id, 'pending', 2 from {B}"]),
    # A priority that may be NULL, which comes first in a descending order.
    status_view(SN, [f"select id, 'initiated' as status, 1 as p from {T}",
                     f"select id, 'active', case when ok then 2 end from {A}"]),
    # Two other branches with the same priority: they tie.
    status_view(STIE, [f"select id, 'initiated' as status, 1 as p from {T}",
                       f"select id, 'active', 2 from {A}",
                       f"select id, 'blocked', 2 from {B}"]),
    # An ascending order: the default branch comes first, nothing is rewritten.
    status_view(SASC, [f"select id, 'initiated' as status, 1 as p from {T}",
                       f"select id, 'active', 2 from {A} where ok"], order='asc'),
]

CASES = [
    # The default branch of a status.
    ('inner_status', 'inner join with a status whose default branch reads the same table',
     f"select t.rev, t.name, s.status, s.p from {T} t join {ST} s on t.id = s.id"),
    ('left_status', 'left join with a status',
     f"select t.rev, s.status, s.p from {T} t left join {ST} s on t.id = s.id"),
    ('default_filter', 'rows whose status is the default one',
     f"select t.rev, t.name from {T} t join {ST} s on t.id = s.id where s.status = 'initiated'"),
    ('other_filter', 'rows whose status is not the default one',
     f"select t.rev from {T} t join {ST} s on t.id = s.id where s.status <> 'initiated'"),
    ('via_join', 'status of the rows of a join that carries the table',
     f"select x.k, t.rev, s.status from {X} x join {T} t on x.tid = t.id join {ST} s on x.tid = s.id"),
    ('case_priority', 'left join with a status whose priorities include a CASE of literals',
     f"select t.rev, s.status, s.p from {T} t left join {SC} s on t.id = s.id"),
    ('null_priority', 'a priority that may be NULL',
     f"select t.rev, s.status, s.p from {T} t join {SN} s on t.id = s.id"),
    ('tie', 'two other branches tie on the priority',
     f"select t.rev, s.status, s.p from {T} t left join {STIE} s on t.id = s.id"),
    ('asc_kept', 'an ascending order, kept',
     f"select t.rev, s.status, s.p from {T} t join {SASC} s on t.id = s.id"),
    # Unused left joins below an operator that does not see duplicates.
    ('topn_unused', 'unused left join below a top-1 whose other columns are constants',
     f"select id, status from (select id, status, row_number() over (partition by id order by p desc) as rn from ("
     f"select t.id, 'x' as status, 1 as p from {T} t left join {Q} q on t.id = q.id) u) z where rn = 1"),
    ('distinct_unused', 'unused left join below a DISTINCT',
     f"select distinct t.id, t.name from {T} t left join {Q} q on t.id = q.id"),
    ('minmax_unused', 'unused left join below min, max and bool_or',
     f"select t.id, min(t.name) as mn, max(t.rev) as mx, bool_or(t.flag) as f "
     f"from {T} t left join {Q} q on t.id = q.id group by t.id"),
    ('exists_unused', 'unused left join in the right side of an EXISTS',
     f"select x.k from {X} x where exists (select 1 from {T} t left join {Q} q on t.id = q.id where t.id = x.tid)"),
    ('union_unused', 'unused left join in a branch of a union below a DISTINCT',
     f"select distinct id from (select t.id from {T} t left join {Q} q on t.id = q.id union all select a.id from {A} a) u"),
]

# When two rows tie on the order of a top-1, the batch query may keep either: the materialized
# views are compared with each other only.
NO_BATCH = {'tie'}

out = []
def switch(on):
    stmt(f"set streaming_merge_lookups_under_dedup = {'true' if on else 'false'}")

def stmt(sql):
    out.append(f"statement ok\n{sql};\n")
def query(types, sql, rows):
    out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

def compare(name):
    a, b = f"mud_{name}_on", f"mud_{name}_off"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from {b}) x),\n"
          f"  (select count(*) from (select * from {b} except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from {b})",
          ["0 0 0"])

def compare_batch(name, sql):
    a = f"mud_{name}_on"
    query('III',
          f"select\n  (select count(*) from (select * from {a} except select * from ({sql}) q) x),\n"
          f"  (select count(*) from (select * from ({sql}) q except select * from {a}) y),\n"
          f"  (select count(*) from {a}) - (select count(*) from ({sql}) q)",
          ["0 0 0"])

def compare_all(title):
    out.append(f"# {title}: with and without the variable, every materialized view holds the same rows.\n")
    for name, _, _ in CASES:
        compare(name)
    out.append(f"# {title}: with the variable, every materialized view holds the rows of its query.\n")
    for name, _, sql in CASES:
        if name not in NO_BATCH:
            compare_batch(name, sql)

out.append("""# Differential test of the removals under consumers that do not see duplicates
# (`streaming_merge_lookups_under_dedup`): every query is materialized twice, with and without the
# variable, and both materialized views must hold the same rows, and the rows of the query, after
# the backfill and after each round of changes.
#
# Generated by `python3 e2e_test/streaming/merge_lookups_under_dedup.gen.py <this file>`: edit the
# list of queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {T} (rev int primary key, id int, name varchar, flag boolean)")
stmt(f"create table {A} (k int primary key, id int, ok boolean)")
stmt(f"create table {B} (k int primary key, id int)")
stmt(f"create table {Q} (k int primary key, id int, v int)")
stmt(f"create table {X} (k int primary key, tid int)")
for view in VIEWS:
    stmt(view)
out.append("# Rows present before the materialized views are created: they go through the backfill.\n")
stmt(f"insert into {T} values (1, 1, 'a', true), (2, 1, 'a2', false), (3, 2, 'b', null), (4, 3, null, true), "
     f"(5, null, 'n', true), (6, 5, 'e', false), (7, 7, 'g', true)")
stmt(f"insert into {A} values (10, 1, true), (11, 2, false), (12, null, true), (13, 9, true), (14, 7, true)")
stmt(f"insert into {B} values (20, 3), (21, 1), (22, null), (23, 7)")
stmt(f"insert into {Q} values (30, 1, 1), (31, 1, 2), (32, 2, 3), (33, null, 4)")
stmt(f"insert into {X} values (40, 1), (41, 2), (42, 5), (43, null), (44, 8)")

for sfx in ('off', 'on'):
    switch(sfx == 'on')
    for name, comment, sql in CASES:
        out.append(f"# {comment}\n")
        stmt(f"create materialized view mud_{name}_{sfx} as {sql}")

compare_all("After the backfill")

out.append("# Spot check: the statuses of the inner join.\n")
query('IT', f"select rev, status from mud_inner_status_on order by rev",
      ["1 blocked", "2 blocked", "3 initiated", "4 blocked", "6 initiated", "7 blocked"])

out.append("# First round: rows move to a better status, the table of the default branch gains rows, "
           "duplicates appear on the right side of the unused joins.\n")
stmt(f"insert into {A} values (15, 5, true), (16, 3, true), (17, 6, false)")
stmt(f"insert into {B} values (24, 2)")
stmt(f"insert into {T} values (8, 6, 'f', true), (9, 3, 'c2', false), (10, null, 'n2', null), (11, 9, 'i', true)")
stmt(f"insert into {Q} values (34, 1, 5), (35, 3, 6), (36, 3, 7), (37, 5, 8)")
stmt(f"insert into {X} values (45, 6), (46, 9)")
compare_all("After the first round")

out.append("# Second round: rows move back to the default status, the priorities change, the table of the "
           "default branch loses rows, duplicates disappear.\n")
stmt(f"delete from {A} where k = 15")
stmt(f"delete from {B} where k in (21, 24)")
stmt(f"update {A} set ok = false where k in (10, 14)")
stmt(f"update {A} set ok = true where k = 11")
stmt(f"update {A} set id = null where k = 16")
stmt(f"delete from {T} where rev in (1, 3)")
stmt(f"update {T} set id = 4 where rev = 6")
stmt(f"update {T} set id = null where rev = 7")
stmt(f"delete from {Q} where k in (30, 31, 35)")
stmt(f"update {Q} set id = 2 where k = 36")
stmt(f"update {X} set tid = 4 where k = 42")
compare_all("After the second round")

out.append("# Third round: the table of the default branch is emptied, then filled again; the other "
           "branches move back.\n")
stmt(f"delete from {T}")
compare_all("After the table of the default branch is emptied")
stmt(f"insert into {T} values (1, 1, 'a', true), (2, 1, 'a3', true), (3, 2, 'b', false), (4, 5, null, null), "
     f"(5, null, 'n', true), (6, 7, 'g', false)")
stmt(f"insert into {A} values (15, 5, true)")
stmt(f"insert into {B} values (21, 1)")
stmt(f"insert into {Q} values (30, 1, 1), (31, 1, 2), (38, 7, 9)")
compare_all("After the table of the default branch is filled again")

out.append("# Cleanup.\n")
for sfx in ('on', 'off'):
    for name, _, _ in reversed(CASES):
        stmt(f"drop materialized view mud_{name}_{sfx}")
for name in (SASC, STIE, SN, SC, ST):
    stmt(f"drop view {name}")
for table in (X, Q, B, A, T):
    stmt(f"drop table {table}")
switch(False)

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
