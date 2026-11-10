#!/usr/bin/env python3
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Crash/restart test of a shared Postgres CDC source with selective barriers (design D4).

Postgres is the source of truth. Each round changes a random subset of the upstream tables
(so that the other RisingWave tables sleep), and may kill RisingWave processes shortly after,
while the `CdcFilter` of a sleeping table holds rows whose offset the source has committed.
After each round, every RisingWave table must converge to its upstream table (no lost row),
and every view must equal its batch recomputation.

Requires a Postgres with `wal_level = logical`, reachable with the PG* environment variables
(PGHOST, PGPORT, PGUSER, PGDATABASE), and a cluster with persistent storage.

Usage (from the repository root, with the cluster started by risedev):
    PGHOST=localhost PGPORT=5433 PGUSER=postgres \\
    scripts/selective_barriers/cdc_chaos_test.py --profile selbar-on --port 4577 --rounds 20
"""

import argparse
import os
import random
import signal
import subprocess
import sys
import threading
import time

TABLES = ["sbx_a", "sbx_b", "sbx_c"]
SLOT = "sbx_slot"

PROCESS_PATTERNS = {
    "compute": "bin/risingwave/compute-node",
    "meta": "bin/risingwave/meta-node",
}
LOG_DIR = ".risingwave/log"

VIEWS = [
    "create materialized view sbx_a_by_g as select k % 5 as g, count(*) as c, sum(v) as s from sbx_a group by k % 5",
    "create materialized view sbx_ab as select a.k, a.v as va, b.v as vb from sbx_a a join sbx_b b on a.k = b.k",
    "create materialized view sbx_c_total as select count(*) as c, sum(v) as s from sbx_c",
]

# (name, query returning the number of mismatching rows)
CHECKS = [
    ("sbx_a_by_g", """select count(*) from (
        (select g, c, s from sbx_a_by_g except select k % 5, count(*), sum(v) from sbx_a group by k % 5)
        union all
        (select k % 5, count(*), sum(v) from sbx_a group by k % 5 except select g, c, s from sbx_a_by_g)) d"""),
    ("sbx_ab", """select count(*) from (
        (select k, va, vb from sbx_ab except select a.k, a.v, b.v from sbx_a a join sbx_b b on a.k = b.k)
        union all
        (select a.k, a.v, b.v from sbx_a a join sbx_b b on a.k = b.k except select k, va, vb from sbx_ab)) d"""),
    ("sbx_c_total", """select case when
        coalesce((select c from sbx_c_total), 0) = (select count(*) from sbx_c)
        and coalesce((select s from sbx_c_total), 0) = coalesce((select sum(v) from sbx_c), 0)
        then 0 else 1 end"""),
]


class SqlError(Exception):
    pass


def run(cmd, timeout):
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise SqlError(f"timeout: {cmd[-1][:80]}") from e
    if res.returncode != 0:
        raise SqlError(res.stderr.strip()[:500])
    return res.stdout.strip()


def pg(sql, timeout=60):
    """Run SQL on the upstream Postgres (connection from the PG* environment variables)."""
    return run(["psql", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout)


def check_no_panic():
    for name in os.listdir(LOG_DIR):
        path = os.path.join(LOG_DIR, name)
        with open(path, errors="replace") as f:
            for line in f:
                if "panicked at" in line:
                    sys.exit(f"panic found in {path}: {line.strip()}")


class Cluster:
    def __init__(self, args):
        self.args = args

    def psql(self, sql, timeout=120):
        return run(["psql", "-h", self.args.host, "-p", str(self.args.port), "-d", "dev", "-U", "root",
                    "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout)

    def kill(self, target):
        names = list(PROCESS_PATTERNS) if target == "all" else [target]
        for name in names:
            out = subprocess.run(["pgrep", "-f", PROCESS_PATTERNS[name]], capture_output=True, text=True).stdout
            for pid in out.split():
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def restart(self, attempts=3):
        out = ""
        check_no_panic()
        for _ in range(attempts):
            subprocess.run(["./risedev", "k"], capture_output=True, text=True)
            subprocess.run(["tmux", "-L", "risedev", "kill-server"], capture_output=True, text=True)
            time.sleep(1)
            res = subprocess.run(["./risedev", "d", self.args.profile], capture_output=True, text=True)
            out = res.stdout + res.stderr
            if "All services started successfully" in out:
                self.wait_ready()
                return
        raise RuntimeError("cluster restart failed:\n" + out[-2000:])

    def wait_ready(self, timeout=300):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                self.psql("select count(*) from sbx_ab", timeout=30)
                return
            except SqlError as e:
                last = e
                time.sleep(2)
        raise RuntimeError(f"cluster not ready after {timeout}s: {last}")


def read_rows(query_fn, table):
    rows = {}
    for line in query_fn(f"select k, v from {table}").splitlines():
        k, v = line.split("|")
        rows[int(k)] = int(v)
    return rows


def setup(cluster):
    for v in ["sbx_c_total", "sbx_ab", "sbx_a_by_g"]:
        cluster.psql(f"drop materialized view if exists {v}")
    for t in TABLES:
        cluster.psql(f"drop table if exists {t}")
    cluster.psql("drop source if exists sbx_src")
    pg(f"""SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
           WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL""")
    time.sleep(1)
    pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{SLOT}'")
    for t in TABLES:
        pg(f"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (k int PRIMARY KEY, v int)")
        pg(f"INSERT INTO {t} SELECT i, i FROM generate_series(1, 20) i")
    env = os.environ
    cluster.psql(f"""create source sbx_src with (
        connector = 'postgres-cdc',
        hostname = '{env.get("PGHOST", "localhost")}',
        port = '{env.get("PGPORT", "5432")}',
        username = '{env.get("PGUSER", env.get("USER", ""))}',
        password = '{env.get("PGPASSWORD", "")}',
        database.name = '{env.get("PGDATABASE", "postgres")}',
        schema.name = 'public',
        slot.name = '{SLOT}')""")
    for t in TABLES:
        cluster.psql(f"create table {t} (k int primary key, v int) from sbx_src table 'public.{t}'")
    for stmt in VIEWS:
        cluster.psql(stmt)


def wait_converged(cluster, timeout):
    """Wait until every RisingWave table equals its upstream table. Return the differences."""
    deadline = time.time() + timeout
    while True:
        diffs = {}
        for t in TABLES:
            expected = read_rows(pg, t)
            actual = read_rows(cluster.psql, t)
            if expected != actual:
                diffs[t] = {k: (expected.get(k), actual.get(k)) for k in set(expected) | set(actual)
                            if expected.get(k) != actual.get(k)}
        if not diffs or time.time() > deadline:
            return diffs
        time.sleep(1)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="selbar-on")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", default=4577, type=int)
    p.add_argument("--rounds", default=20, type=int)
    p.add_argument("--crash-rate", default=0.4, type=float, help="probability of a crash in a round")
    p.add_argument("--seed", default=int(time.time()), type=int)
    p.add_argument("--idle", default=4.0, type=float,
                   help="max seconds without any change before a round, so that tables sleep")
    p.add_argument("--converge-timeout", default=90, type=int)
    args = p.parse_args()

    rnd = random.Random(args.seed)
    print(f"seed={args.seed}")
    cluster = Cluster(args)
    setup(cluster)
    diffs = wait_converged(cluster, args.converge_timeout)
    if diffs:
        sys.exit(f"initial backfill did not converge: {diffs}")

    next_key = 1000
    stats = {"rounds": 0, "crashes": {}}
    for r in range(1, args.rounds + 1):
        time.sleep(rnd.uniform(0, args.idle))
        tables = rnd.sample(TABLES, rnd.randint(1, 2))
        statements = []
        for t in tables:
            keys = [int(k) for k in pg(f"SELECT k FROM {t}").split()]
            for _ in range(rnd.randint(1, 5)):
                new_keys = list(range(next_key, next_key + rnd.randint(1, 20)))
                next_key += len(new_keys)
                values = ", ".join(f"({k}, {rnd.randint(-100, 100)})" for k in new_keys)
                stmt = [f"INSERT INTO {t} VALUES {values} ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"]
                for k in rnd.sample(keys, min(len(keys), rnd.randint(0, 3))):
                    stmt.append(f"UPDATE {t} SET v = {rnd.randint(-100, 100)} WHERE k = {k}")
                for k in rnd.sample(keys, min(len(keys), rnd.randint(0, 2))):
                    stmt.append(f"DELETE FROM {t} WHERE k = {k}")
                statements.append("; ".join(stmt))
        # Keys of the tables collide so that the join produces rows.
        next_key -= rnd.randint(0, 20)

        crash = rnd.random() < args.crash_rate
        target = rnd.choice(["compute", "compute", "meta", "all"]) if crash else None
        killer = None
        if crash:
            killer = threading.Timer(rnd.uniform(0.0, 2.0), cluster.kill, args=(target,))
            killer.start()
        for s in statements:
            pg(s)
            time.sleep(rnd.uniform(0, 0.3))
        if killer:
            killer.join()
            time.sleep(0.5)
            stats["crashes"][target] = stats["crashes"].get(target, 0) + 1
            cluster.restart()

        diffs = wait_converged(cluster, args.converge_timeout)
        if diffs:
            sys.exit(f"round {r}: tables did not converge: { {t: dict(list(d.items())[:10]) for t, d in diffs.items()} }")
        for name, q in CHECKS:
            n = int(cluster.psql(q))
            if n != 0:
                sys.exit(f"round {r}: view {name} has {n} mismatching rows")
        check_no_panic()
        stats["rounds"] += 1
        print(f"round {r:3}: tables={','.join(tables):12} statements={len(statements):2} "
              f"crash={target or '-'} converged", flush=True)

    print(f"PASSED {stats}")


if __name__ == "__main__":
    main()
