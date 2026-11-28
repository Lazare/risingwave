"""Generates `e2e_test/batch/date_range_predicates.slt`, the differential test of
`batch_date_range_predicates` (rule `DateRangePredicateRule`: comparisons of `date_trunc`,
`extract(year ...)`, `date_part('year', ...)` and casts to `date` with a constant become ranges of
the column). Every query runs twice, with and without the variable, through `INSERT ... SELECT`
(the batch plan), and both results must hold the same rows with the same multiplicities, after
each round of changes of the data. The values sit on, just before and just after the boundaries
of the ranges, and the queries run in the `UTC` time zone (rewritten) and in another one (kept).

Usage: python3 e2e_test/batch/date_range_predicates.gen.py e2e_test/batch/date_range_predicates.slt
"""
import sys

TS, D, TZ = 'drp_ts', 'drp_d', 'drp_tz'
P = 'drp'
VAR = 'batch_date_range_predicates'

# (name, columns of the result, query)
CASES = [
    ('trunc_month_eq', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('month', ts) = '2024-05-01 00:00:00'"),
    ('trunc_month_not_aligned', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('month', ts) = '2024-05-02 00:00:00'"),
    ('trunc_day_bounds', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('day', ts) > '2024-04-30 10:00:00' "
     f"and date_trunc('day', ts) <= '2024-05-01 10:00:00'"),
    ('trunc_day_lt_ge', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('day', ts) >= '2024-04-30 00:00:01' "
     f"and date_trunc('day', ts) < '2024-06-01 00:00:00'"),
    ('trunc_quarter_flipped', 'ts timestamp, v int',
     f"select * from {TS} where '2024-01-01 00:00:00' <= date_trunc('quarter', ts) "
     f"and date_trunc('quarter', ts) < '2024-07-01 00:00:00'"),
    ('trunc_week', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('week', ts) = '2024-04-29 00:00:00'"),
    ('trunc_hour_minute_second', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('hour', ts) >= '2024-04-30 23:30:00' "
     f"and date_trunc('minute', ts) <= '2024-05-01 00:00:30' and date_trunc('second', ts) <> '2024-05-01 00:00:00'"),
    ('trunc_year_gt', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('year', ts) > '2023-06-01 00:00:00'"),
    ('cast_date_eq', 'ts timestamp, v int',
     f"select * from {TS} where ts::date = '2024-05-01'"),
    ('cast_date_bounds', 'ts timestamp, v int',
     f"select * from {TS} where ts::date > '2024-04-30' and ts::date <= '2024-05-31'"),
    ('cast_date_lt', 'ts timestamp, v int',
     f"select * from {TS} where ts::date < '2024-05-01'"),
    ('trunc_of_date', 'd date, v int',
     f"select * from {D} where date_trunc('month', d) = '2024-05-01'"),
    ('trunc_of_date_bounds', 'd date, v int',
     f"select * from {D} where date_trunc('month', d) > '2024-04-15' and date_trunc('year', d) <= '2024-01-01'"),
    ('extract_year_eq', 'd date, v int',
     f"select * from {D} where extract(year from d) = 2024"),
    ('extract_year_between', 'd date, v int',
     f"select * from {D} where extract(year from d) between 2023 and 2024"),
    ('extract_year_ts', 'ts timestamp, v int',
     f"select * from {TS} where extract(year from ts) >= 2024"),
    ('date_part_fraction', 'd date, v int',
     f"select * from {D} where date_part('year', d) > 2023.5 and date_part('year', d) < 2025.5"),
    ('extract_year_fraction_eq', 'd date, v int',
     f"select * from {D} where extract(year from d) = 2024.5"),
    ('extract_year_negative', 'd date, v int',
     f"select * from {D} where extract(year from d) <= 0 or v = 1"),
    ('tz_trunc_day', 'tz timestamptz, v int',
     f"select * from {TZ} where date_trunc('day', tz) = '2024-05-01 00:00:00+00:00'"),
    ('tz_trunc_month_bounds', 'tz timestamptz, v int',
     f"select * from {TZ} where date_trunc('month', tz) >= '2024-04-15 00:00:00+00:00' "
     f"and date_trunc('month', tz) < '2024-05-15 00:00:00+00:00'"),
    ('tz_cast_date', 'tz timestamptz, v int',
     f"select * from {TZ} where tz::date = '2024-05-01'"),
    ('tz_extract_year', 'tz timestamptz, v int',
     f"select * from {TZ} where extract(year from tz) = 2024"),
    ('kept_or', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('month', ts) = '2024-05-01 00:00:00' or v = 1"),
    ('kept_decade', 'ts timestamp, v int',
     f"select * from {TS} where date_trunc('decade', ts) = '2020-01-01 00:00:00'"),
    ('kept_month_field', 'd date, v int',
     f"select * from {D} where extract(month from d) = 5"),
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


out.append(f"""# Differential test of `{VAR}`: every query runs through `INSERT ... SELECT` without, then
# with the variable, and both results must hold the same rows with the same multiplicities, after
# each round of changes of the data.
#
# Generated by `python3 e2e_test/batch/date_range_predicates.gen.py <this file>`: edit the list of
# queries there and regenerate rather than editing by hand.
""")
stmt("SET RW_IMPLICIT_FLUSH TO true")
stmt(f"create table {TS} (ts timestamp primary key, v int)")
stmt(f"create table {D} (d date primary key, v int)")
stmt(f"create table {TZ} (tz timestamptz primary key, v int)")
for name, cols, _ in CASES:
    stmt(f"create table {P}_{name}_off ({cols})")
    stmt(f"create table {P}_{name}_on ({cols})")

run_all("Empty tables")

out.append("# First data: values on, just before and just after the boundaries.\n")
stmt(f"insert into {TS} values ('2024-04-30 23:59:59.999999', 1), ('2024-05-01 00:00:00', 2), "
     f"('2024-05-01 00:00:00.000001', 3), ('2024-05-01 10:00:00', 4), ('2024-05-31 23:59:59.999999', 5), "
     f"('2024-06-01 00:00:00', 6), ('2023-12-31 23:59:59.999999', 7), ('2024-01-01 00:00:00', 8), "
     f"('2024-04-29 00:00:00', 9), ('2024-05-05 23:59:59', 10), ('2024-05-06 00:00:00', 11), "
     f"('2024-04-30 23:30:00', 12), ('2024-05-01 00:00:30.5', 13), ('2024-07-01 00:00:00', 14), "
     f"('2020-03-01 00:00:00', 15), ('2029-12-31 23:59:59', 16), ('2030-01-01 00:00:00', 17)")
stmt(f"insert into {D} values ('2023-12-31', 1), ('2024-01-01', 2), ('2024-04-30', 3), "
     f"('2024-05-01', 4), ('2024-05-31', 5), ('2024-06-01', 6), ('2024-12-31', 7), ('2025-01-01', 8), "
     f"('2022-12-31', 9), ('2023-01-01', 10), ('2025-12-31', 11), ('2026-01-01', 12)")
stmt(f"insert into {TZ} values ('2024-04-30 23:59:59.999999+00:00', 1), ('2024-05-01 00:00:00+00:00', 2), "
     f"('2024-05-01 23:59:59.999999+00:00', 3), ('2024-05-02 00:00:00+00:00', 4), "
     f"('2024-04-30 22:30:00+00:00', 5), ('2024-05-01 22:30:00+00:00', 6), "
     f"('2023-12-31 23:00:00+00:00', 7), ('2024-12-31 23:30:00+00:00', 8), ('2025-01-01 00:00:00+00:00', 9)")
run_all("First data, UTC")

query('I', f"select count(*) from {P}_trunc_month_eq_on", ["7"])
query('I', f"select count(*) from {P}_cast_date_eq_on", ["4"])
query('I', f"select count(*) from {P}_extract_year_eq_on", ["6"])
query('I', f"select count(*) from {P}_tz_trunc_day_on", ["3"])

out.append("# The same in another time zone: the rule leaves the timestamptz forms as they are.\n")
stmt("set timezone = 'Europe/Paris'")
run_all("First data, Europe/Paris")
stmt("set timezone = 'UTC'")

out.append("# Second data: values moved across the boundaries, NULL values, deletes.\n")
stmt(f"update {TS} set v = null where v in (2, 3)")
stmt(f"delete from {TS} where v = 6")
stmt(f"insert into {TS} values ('2024-05-15 12:00:00', null), ('1999-01-01 00:00:00', 20)")
stmt(f"delete from {D} where v in (2, 8)")
stmt(f"insert into {D} values ('2024-02-29', null), ('0001-01-01', 30)")
stmt(f"delete from {TZ} where v = 2")
stmt(f"insert into {TZ} values ('2024-05-01 12:00:00+02:00', 10)")
run_all("Second data, UTC")

out.append("# Cleanup.\n")
for name, _, _ in reversed(CASES):
    stmt(f"drop table {P}_{name}_on")
    stmt(f"drop table {P}_{name}_off")
for table in (TZ, D, TS):
    stmt(f"drop table {table}")

open(sys.argv[1], 'w').write("\n".join(out))
print(len(CASES), 'cases,', sum(1 for o in out if o.startswith(('statement', 'query'))), 'records')
