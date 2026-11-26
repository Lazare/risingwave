"""Generates `e2e_test/streaming/status_ladder_case.slt`, the differential test of the status ladder
picked with one `CASE` in streaming plans (`streaming_status_ladder_case`).

Every query is materialized four times: without the variable (the reference), with it, with
`streaming_merge_union_scans` only, and with both. The four materialized views must hold the same
multiset of rows (each distinct row with its number of occurrences), and the reference must hold
the rows of the batch query, after the backfill and after each round of changes.

The queries never let the top-1 choose between rows that differ: two rows of the same key and
priority always output the same values, so the result is deterministic.

Usage: python3 e2e_test/streaming/status_ladder_case.gen.py e2e_test/streaming/status_ladder_case.slt
"""
import sys


def ladder(branches, key='k', order='desc', keep='rn = 1', priority=False):
    union = ' union all '.join(branches)
    extra = ', p as c3' if priority else ''
    inner_extra = ', p' if priority else ''
    return (f"select {key} as c1, st as c2{extra} from (select {key}, st{inner_extra}, "
            f"row_number() over (partition by {key} order by p {order}) as rn from ({union}) u) x "
            f"where {keep}")


def cases(E, F, G):
    """The queries over the tables `E`, `F` and `G`: (name, comment, number of output columns,
    query). Also used by `e2e_test/batch/status_ladder_case.gen.py`."""
    OPEN = f"select k, 'open' as st, 1 as p from {F}"
    ACTIVE = f"select k, 'active', 2 from {F} where amt > 0"
    CLOSED = f"select k, 'closed', 3 from {F} where st = 'back'"
    HELD = f"select k, 'held', 4 from {G}"
    return [
        ('form1', 'three branches over the same table become one CASE, one branch over another table', 2,
         ladder([OPEN, ACTIVE, CLOSED, HELD])),
        ('same_table', 'every branch over a table that is not unique on the key: the top-1 stays', 2,
         ladder([OPEN, ACTIVE, CLOSED])),
        ('unique_default', 'every branch over a table unique on the key, with a default branch', 2,
         ladder([f"select id, 'new' as st, 1 as p from {E}",
                 f"select id, 'active', 2 from {E} where a > 0",
                 f"select id, 'closed', 3 from {E} where s = 'closed'"], key='id')),
        ('unique_no_default', 'every branch over a table unique on the key, without a default branch', 2,
         ladder([f"select id, 'active' as st, 2 as p from {E} where a > 0",
                 f"select id, 'closed', 3 from {E} where b > 0"], key='id')),
        ('ascending', 'ascending priority', 2,
         ladder([f"select k, 'first' as st, 1 as p from {F} where amt > 10",
                 f"select k, 'second', 2 from {F} where amt > 0",
                 f"select k, 'last', 9 from {F}"], order='asc')),
        ('with_priority', 'the priority is output too', 3,
         ladder([OPEN, ACTIVE, CLOSED], priority=True)),
        ('shared_join', 'branches over a shared join, and one over another table', 2,
         f"with c as (select {F}.k, {F}.amt, {F}.st from {F} join {G} on {F}.k = {G}.k) "
         + ladder(["select k, 'open' as st, 1 as p from c",
                   "select k, 'active', 2 from c where amt > 0",
                   f"select id, 'new', 0 from {E}"])),
        ('now_branch', 'a branch with now() stays a separate branch', 2,
         ladder([OPEN, ACTIVE, f"select k, 'late', 3 from {F} where ts < now()"])),
        # Ladders that are kept: they must hold the same rows anyway.
        ('kept_key', 'kept: the branches compute the key differently', 2,
         ladder([OPEN, f"select r, 'active', 2 from {F} where amt > 0"])),
        ('kept_top2', 'kept: two rows per key', 2,
         ladder([OPEN, ACTIVE, CLOSED], keep='rn <= 2')),
        ('kept_tables', 'kept: each branch over its own table', 2,
         ladder([f"select id, 'new' as st, 1 as p from {E}", f"select k, 'held', 2 from {G}"], key='id')),
    ]


# The rows of the tables, as `INSERT` statements over the tables `E`, `F` and `G`, by round of
# changes. Also used by `e2e_test/batch/status_ladder_case.gen.py`.
PAST, FUTURE = "'2001-01-01 00:00:00+00:00'", "'2101-01-01 00:00:00+00:00'"


def initial_rows(E, F, G):
    return [
        f"insert into {E} values (1, 1, 0, 'closed'), (2, 0, 5, null), (3, null, null, 'open'), (4, 7, 1, 'x'), "
        f"(5, -1, -1, 'closed'), (6, null, 2, null)",
        f"insert into {F} values (1, 1, 'back', 5, {PAST}), (2, 1, null, -1, {FUTURE}), (3, 2, 'x', 20, null), "
        f"(4, 2, 'back', null, {PAST}), (5, 3, null, null, null), (6, null, 'back', 3, {PAST}), (7, null, 'x', 0, {FUTURE}), "
        f"(8, 4, 'x', 11, {FUTURE}), (9, 4, 'x', 11, {FUTURE}), (10, 5, 'back', -5, null), (11, 6, null, 1, {PAST})",
        f"insert into {G} values (1, 1), (2, 3), (3, null), (4, 7), (5, 3)",
    ]


def rounds(E, F, G):
    """(title, statements) of each round of changes after the initial rows."""
    return [
        ("new rows, duplicates of a key, NULL keys, a row written again", [
            f"insert into {F} values (12, 8, 'back', 1, {PAST}), (13, 8, 'x', 2, {FUTURE}), (14, null, null, 9, null), "
            f"(15, 3, 'back', 0, {PAST})",
            f"delete from {F} where r = 2",
            f"insert into {F} values (2, 1, 'back', 4, {PAST})",
            f"insert into {E} values (7, 3, 3, 'closed'), (8, null, -2, null)",
            f"insert into {G} values (6, 8), (7, null)",
        ]),
        ("rows that move from one filter to another, and to NULL", [
            f"update {F} set st = 'back' where r in (3, 8)",
            f"update {F} set st = null where r in (1, 4)",
            f"update {F} set amt = -amt where r in (9, 11, 13)",
            f"update {F} set amt = null where r = 12",
            f"update {F} set k = 9 where r = 6",
            f"update {F} set k = null where r = 10",
            f"update {F} set ts = {FUTURE} where r = 1",
            f"update {F} set ts = {PAST} where r in (7, 9)",
            f"update {E} set a = null where id = 4",
            f"update {E} set s = 'closed', a = 2 where id = 2",
            f"update {E} set s = null where id = 1",
            f"update {E} set b = 9 where id = 6",
            f"update {G} set k = 2 where r = 1",
        ]),
        ("deletes", [
            f"delete from {F} where r in (5, 9, 14)",
            f"delete from {F} where k = 8",
            f"delete from {E} where id in (3, 5)",
            f"delete from {G} where k = 3",
        ]),
        ("the tables are emptied", [
            f"delete from {F}",
            f"delete from {E}",
            f"delete from {G}",
        ]),
        ("the tables are filled again", [
            f"insert into {F} values (1, 1, 'back', 1, {PAST}), (2, 1, 'back', 1, {PAST}), (3, 2, null, null, null), "
            f"(4, null, 'back', 2, {FUTURE})",
            f"insert into {E} values (1, 1, 1, 'closed'), (2, null, null, null)",
            f"insert into {G} values (1, 2)",
        ]),
    ]


TABLES = [
    "create table {E} (id int primary key, a int, b int, s varchar)",
    "create table {F} (r int primary key, k int, st varchar, amt int, ts timestamptz)",
    "create table {G} (r int primary key, k int)",
]

class Slt:
    """The records of a sqllogictest file."""

    def __init__(self):
        self.out = []

    def comment(self, text):
        self.out.append(f"# {text}\n")

    def stmt(self, sql):
        self.out.append(f"statement ok\n{sql};\n")

    def query(self, types, sql, rows):
        self.out.append(f"query {types}\n{sql};\n----\n" + "\n".join(rows) + "\n")

    def same_multiset(self, a, b, width):
        """Checks that the relations `a` and `b` hold the same multiset of rows."""
        self.query('III',
                   f"select\n  (select count(*) from ({counted(a, width)} except {counted(b, width)}) x),\n"
                   f"  (select count(*) from ({counted(b, width)} except {counted(a, width)}) y),\n"
                   f"  (select count(*) from {a}) - (select count(*) from {b})",
                   ["0 0 0"])

    def write(self, path):
        open(path, 'w').write("\n".join(self.out))
        print(sum(1 for o in self.out if o.startswith(('statement', 'query'))), 'records')


def counted(relation, width):
    columns = ", ".join(f"c{i}" for i in range(1, width + 1))
    return f"select {columns}, count(*) as n from {relation} group by {columns}"


HEADER = """# Differential test of `streaming_status_ladder_case`: every query is materialized without the
# variable, with it, with `streaming_merge_union_scans` only and with both, and the materialized
# views must hold the same multiset of rows, and the rows of the query, after the backfill and after
# each round of changes.
#
# Generated by `python3 e2e_test/streaming/status_ladder_case.gen.py <this file>`: edit the list
# of queries there and regenerate rather than editing by hand.
"""

# (suffix, streaming_status_ladder_case, streaming_merge_union_scans)
VARIANTS = [('ref', False, False), ('case', True, False), ('merge', False, True), ('both', True, True)]


def main():
    E, F, G = 'sl_e', 'sl_f', 'sl_g'
    queries = cases(E, F, G)
    slt = Slt()

    def compare_all(title):
        slt.comment(f"{title}: with and without the variables, every materialized view holds the same rows.")
        for name, _, width, _ in queries:
            for suffix, _, _ in VARIANTS[1:]:
                slt.same_multiset(f"sl_{name}_{suffix}", f"sl_{name}_ref", width)
        slt.comment(f"{title}: the reference holds the rows of its query.")
        for name, _, width, sql in queries:
            slt.same_multiset(f"sl_{name}_ref", f"({sql}) q", width)

    slt.out.append(HEADER)
    slt.stmt("SET RW_IMPLICIT_FLUSH TO true")
    for table in TABLES:
        slt.stmt(table.format(E=E, F=F, G=G))
    slt.comment("Rows present before the materialized views are created: they go through the backfill.")
    for sql in initial_rows(E, F, G):
        slt.stmt(sql)

    for suffix, case, merge in VARIANTS:
        slt.stmt(f"set streaming_status_ladder_case = {'true' if case else 'false'}")
        slt.stmt(f"set streaming_merge_union_scans = {'true' if merge else 'false'}")
        for name, comment, _, sql in queries:
            slt.comment(comment)
            slt.stmt(f"create materialized view sl_{name}_{suffix} as {sql}")
    slt.stmt("set streaming_status_ladder_case = false")
    slt.stmt("set streaming_merge_union_scans = false")

    compare_all("After the backfill")

    slt.comment("Spot checks of the rows themselves.")
    slt.query('IT', "select c1, c2 from sl_form1_case order by c1, c2",
              ["1 held", "2 closed", "3 held", "4 active", "5 closed", "6 active", "7 held", "NULL held"])
    slt.query('IT', "select c1, c2 from sl_unique_default_case order by c1",
              ["1 closed", "2 new", "3 new", "4 active", "5 closed", "6 new"])

    for title, statements in rounds(E, F, G):
        slt.comment(f"Changes: {title}.")
        for sql in statements:
            slt.stmt(sql)
        compare_all(f"After the changes ({title})")

    slt.comment("Cleanup.")
    for suffix, _, _ in reversed(VARIANTS):
        for name, _, _, _ in reversed(queries):
            slt.stmt(f"drop materialized view sl_{name}_{suffix}")
    for table in (G, F, E):
        slt.stmt(f"drop table {table}")
    slt.write(sys.argv[1])


if __name__ == '__main__':
    main()
