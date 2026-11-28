#!/usr/bin/env python3
"""Generates `phased_stages.slt`: distributed batch queries run with the scheduler variables
`batch_phased_stages` and `batch_admission_by_estimate` off and on, in every combination. The
expected results are computed here from the same synthetic data, so each setting is checked
against the same values: the results must not change.

Shapes: hash joins whose probe side starts after the build side (one, two and three levels),
a join with an empty build side, a union of joins (both inputs of the union together), joins
below a `LIMIT` (the stages whose consumers finished are stopped), an ordered top-n (merge sort
exchange), aggregations on both sides, semi and anti joins, a nested loop join, a build side
below a top-n, and wide JSONB rows (estimate above the threshold).

Usage: python3 phased_stages.gen.py phased_stages.slt
"""

import sys
from collections import Counter, defaultdict

N1, N2, N3, NJ = 20000, 3000, 1000, 2000

t1 = [(i, i % 500, "x" * (i % 50)) for i in range(1, N1 + 1)]
t2 = [(i, i % 700, i) for i in range(1, N2 + 1)]
t3 = [(i, i % 300, 2 * i) for i in range(1, N3 + 1)]
tj = [(i, i % 100) for i in range(1, NJ + 1)]


def by_k(rows):
    d = defaultdict(list)
    for r in rows:
        d[r[1]].append(r)
    return d


t2k, t3k, tjk = by_k(t2), by_k(t3), by_k(tj)
t1_count_k = Counter(r[1] for r in t1)


def q_simple():
    c = s = l = 0
    for (_, k, v) in t1:
        for (_, _, w) in t2k.get(k, []):
            c += 1
            s += w
            l += len(v)
    return [(c, s, l)]


def q_three():
    c = s = 0
    for (_, k, _) in t1:
        n2 = len(t2k.get(k, []))
        if n2 == 0:
            continue
        for (_, _, z) in t3k.get(k, []):
            c += n2
            s += z * n2
    return [(c, s)]


def q_union():
    c = s = 0
    for (_, k, _) in t1:
        for (_, _, w) in t2k.get(k, []):
            c += 1
            s += w
        for (_, _, z) in t3k.get(k, []):
            c += 1
            s += z
    return [(c, s)]


def q_ordered():
    rows = []
    for (i, k, _) in t1:
        for (_, _, w) in t2k.get(k, []):
            rows.append((i, w))
    rows.sort()
    return rows[:5]


def q_aggs():
    c = s = m = 0
    t2max = {k: max(r[2] for r in rs) for k, rs in t2k.items()}
    for k, n in t1_count_k.items():
        if k in t2max:
            c += 1
            s += n
            m += t2max[k]
    return [(c, s, m)]


def q_semi():
    return [(sum(1 for r in t1 if r[1] in t3k),)]


def q_anti():
    return [(sum(1 for r in t1 if r[1] not in t3k),)]


def q_nested_loop():
    c = 0
    small = [r for r in t2 if r[0] <= 100]
    for (_, _, z) in t3:
        for (_, _, w) in small:
            if z < w:
                c += 1
    return [(c,)]


def q_build_top_n():
    keys = [r[1] for r in sorted(t2)[:100]]
    return [(sum(t1_count_k[k] for k in keys),)]


def q_grouped_ordered():
    out = []
    for k in sorted(set(t3k) & set(t2k)):
        out.append((k, len(t3k[k]) * len(t2k[k])))
    return out[:3]


def q_wide():
    c = 0
    for (_, k) in tj:
        c += len(tjk[k])
    return [(c,)]


def q_left_empty():
    return [(N1,)]


QUERIES = [
    ("III", "SELECT count(*), sum(t2.w), sum(length(t1.v)) FROM t1 JOIN t2 ON t1.k = t2.k;", q_simple()),
    ("II", "SELECT count(*), sum(t3.z) FROM t1 JOIN t2 ON t1.k = t2.k JOIN t3 ON t2.k = t3.k;", q_three()),
    ("I", "SELECT count(*) FROM t1 JOIN empty_t ON t1.k = empty_t.k;", [(0,)]),
    ("I", "SELECT count(*) FROM t1 LEFT JOIN empty_t ON t1.k = empty_t.k;", q_left_empty()),
    (
        "II",
        "SELECT count(*), sum(x) FROM (SELECT t2.w AS x FROM t1 JOIN t2 ON t1.k = t2.k "
        "UNION ALL SELECT t3.z AS x FROM t1 JOIN t3 ON t1.k = t3.k) u;",
        q_union(),
    ),
    ("I", "SELECT count(*) FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.k = t2.k LIMIT 10) l;", [(10,)]),
    (
        "I",
        "SELECT count(*) FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.k = t2.k "
        "UNION ALL SELECT t1.id FROM t1 JOIN t3 ON t1.k = t3.k LIMIT 7) l;",
        [(7,)],
    ),
    ("II", "SELECT t1.id, t2.w FROM t1 JOIN t2 ON t1.k = t2.k ORDER BY t1.id, t2.w LIMIT 5;", q_ordered()),
    (
        "III",
        "SELECT count(*), sum(c), sum(m) FROM (SELECT k, count(*) AS c FROM t1 GROUP BY k) a "
        "JOIN (SELECT k, max(w) AS m FROM t2 GROUP BY k) b ON a.k = b.k;",
        q_aggs(),
    ),
    ("I", "SELECT count(*) FROM t1 WHERE k IN (SELECT k FROM t3);", q_semi()),
    ("I", "SELECT count(*) FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t3 WHERE t3.k = t1.k);", q_anti()),
    ("I", "SELECT count(*) FROM t3 JOIN (SELECT * FROM t2 WHERE id <= 100) s ON t3.z < s.w;", q_nested_loop()),
    (
        "I",
        "SELECT count(*) FROM t1 JOIN (SELECT k FROM t2 ORDER BY id LIMIT 100) b ON t1.k = b.k;",
        q_build_top_n(),
    ),
    (
        "II",
        "SELECT t3.k, count(*) FROM t3 JOIN t2 ON t3.k = t2.k GROUP BY t3.k ORDER BY t3.k LIMIT 3;",
        q_grouped_ordered(),
    ),
    (
        "I",
        "SELECT count(*) FROM (SELECT x.a, x.b, y.a AS c, y.b AS d FROM tj x JOIN tj y ON x.k = y.k) w "
        "WHERE length(a::varchar) + length(b::varchar) + length(c::varchar) + length(d::varchar) > 0;",
        q_wide(),
    ),
]

SETTINGS = [("false", "false"), ("true", "false"), ("false", "true"), ("true", "true")]


def fmt(row):
    return " ".join(str(x) for x in row)


def main(out):
    lines = [
        "# Generated by phased_stages.gen.py: do not edit by hand.",
        "# Distributed batch queries with `batch_phased_stages` and `batch_admission_by_estimate`",
        "# off and on: the results must be the same in every setting (expected values computed by",
        "# the generator from the same data).",
        "",
        "statement ok",
        "SET RW_IMPLICIT_FLUSH TO true;",
        "",
        "statement ok",
        "SET QUERY_MODE TO distributed;",
        "",
        "query TT",
        "SELECT current_setting('batch_phased_stages'), current_setting('batch_admission_by_estimate');",
        "----",
        "false false",
        "",
        "statement ok",
        "CREATE TABLE t1 (id INT PRIMARY KEY, k INT, v VARCHAR);",
        "",
        "statement ok",
        "CREATE TABLE t2 (id INT PRIMARY KEY, k INT, w INT);",
        "",
        "statement ok",
        "CREATE TABLE t3 (id INT PRIMARY KEY, k INT, z INT);",
        "",
        "statement ok",
        "CREATE TABLE empty_t (id INT PRIMARY KEY, k INT);",
        "",
        "statement ok",
        "CREATE TABLE tj (id INT PRIMARY KEY, k INT, a JSONB, b JSONB);",
        "",
        "statement ok",
        f"INSERT INTO t1 SELECT i, i % 500, repeat('x', i % 50) FROM generate_series(1, {N1}) i;",
        "",
        "statement ok",
        f"INSERT INTO t2 SELECT i, i % 700, i FROM generate_series(1, {N2}) i;",
        "",
        "statement ok",
        f"INSERT INTO t3 SELECT i, i % 300, 2 * i FROM generate_series(1, {N3}) i;",
        "",
        "statement ok",
        "INSERT INTO tj SELECT i, i % 100, jsonb_build_object('id', i, 'pad', repeat('a', 200)), "
        f"jsonb_build_object('id', -i, 'pad', repeat('b', 200)) FROM generate_series(1, {NJ}) i;",
        "",
    ]
    for phased, estimate in SETTINGS:
        lines += [
            "statement ok",
            f"SET batch_phased_stages = {phased};",
            "",
            "statement ok",
            f"SET batch_admission_by_estimate = {estimate};",
            "",
        ]
        for types, sql, expected in QUERIES:
            lines += [f"query {types}", sql, "----"] + [fmt(r) for r in expected] + [""]
    lines += [
        "statement ok",
        "SET batch_phased_stages = false;",
        "",
        "statement ok",
        "SET batch_admission_by_estimate = false;",
        "",
    ]
    for t in ["t1", "t2", "t3", "empty_t", "tj"]:
        lines += ["statement ok", f"DROP TABLE {t};", ""]
    with open(out, "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main(sys.argv[1])
