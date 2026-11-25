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

"""Crash/restart test of decoupled sinks with idle epochs.

The kv log store of a decoupled sink does not persist the barrier of an epoch without data.
This test writes bursts of changes to a RisingWave table separated by idle periods (so that
many empty epochs are skipped in the persisted log), with a decoupled upsert sink into Postgres,
kills RisingWave processes at random, and checks after each round that the Postgres table
converges to the RisingWave table (nothing lost, nothing left over).

The Postgres upsert sink is not exactly-once: it may commit an epoch that RisingWave loses in a
crash (DML not confirmed by FLUSH). For keys touched in a round with a crash, Postgres may thus
hold any value written during the round; these keys are then rewritten through RisingWave so
that the next rounds start from equal tables. Other keys must always be equal.

Requires a Postgres reachable with the PG* environment variables and a cluster with persistent
storage.

Usage (from the repository root, with the cluster started by risedev):
    PGHOST=localhost PGPORT=5433 PGUSER=postgres \\
    scripts/selective_barriers/sink_chaos_test.py --profile selbar-on --port 4577 --rounds 15
"""

import argparse
import os
import random
import signal
import subprocess
import sys
import threading
import time

PROCESS_PATTERNS = {
    "compute": "bin/risingwave/compute-node",
    "meta": "bin/risingwave/meta-node",
}
LOG_DIR = ".risingwave/log"


class SqlError(Exception):
    pass


def run(cmd, timeout=120):
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise SqlError(f"timeout: {cmd[-1][:80]}") from e
    if res.returncode != 0:
        raise SqlError(res.stderr.strip()[:500])
    return res.stdout.strip()


def pg(sql):
    return run(["psql", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql])


def check_no_panic():
    for name in os.listdir(LOG_DIR):
        with open(os.path.join(LOG_DIR, name), errors="replace") as f:
            for line in f:
                if "panicked at" in line:
                    sys.exit(f"panic found in {name}: {line.strip()}")


class Cluster:
    def __init__(self, args):
        self.args = args

    def psql(self, sql, timeout=120):
        return run(["psql", "-h", "localhost", "-p", str(self.args.port), "-d", "dev", "-U", "root",
                    "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout)

    def psql_retry(self, sql, attempts=30):
        """Run a statement, retrying while the database recovers."""
        for i in range(attempts):
            try:
                return self.psql(sql)
            except SqlError:
                if i == attempts - 1:
                    raise
                time.sleep(2)

    def kill(self, target):
        for name in (list(PROCESS_PATTERNS) if target == "all" else [target]):
            out = subprocess.run(["pgrep", "-f", PROCESS_PATTERNS[name]], capture_output=True, text=True).stdout
            for pid in out.split():
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def restart(self):
        check_no_panic()
        for _ in range(3):
            subprocess.run(["./risedev", "k"], capture_output=True, text=True)
            subprocess.run(["tmux", "-L", "risedev", "kill-server"], capture_output=True, text=True)
            time.sleep(1)
            res = subprocess.run(["./risedev", "d", self.args.profile], capture_output=True, text=True)
            if "All services started successfully" in res.stdout + res.stderr:
                break
        else:
            raise RuntimeError("cluster restart failed")
        deadline = time.time() + 300
        while time.time() < deadline:
            try:
                self.psql("select count(*) from sbk_t", timeout=30)
                self.psql("delete from sbk_t where k = -1", timeout=30)
                # The database may still be recovering (e.g. the compute node reconnects).
                self.psql("flush", timeout=60)
                return
            except SqlError:
                time.sleep(2)
        raise RuntimeError("cluster not ready")


def rows(query_fn, table):
    out = {}
    for line in query_fn(f"select k, v from {table}").splitlines():
        k, v = line.split("|")
        out[int(k)] = int(v)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="selbar-on")
    p.add_argument("--port", default=4577, type=int)
    p.add_argument("--rounds", default=15, type=int)
    p.add_argument("--crash-rate", default=0.5, type=float)
    p.add_argument("--seed", default=int(time.time()), type=int)
    p.add_argument("--idle", default=5.0, type=float, help="max idle seconds between bursts")
    p.add_argument("--converge-timeout", default=90, type=int)
    args = p.parse_args()
    rnd = random.Random(args.seed)
    print(f"seed={args.seed}")
    cluster = Cluster(args)

    env = os.environ
    cluster.psql("drop sink if exists sbk_sink")
    cluster.psql("drop table if exists sbk_t")
    pg("DROP TABLE IF EXISTS sbk_target; CREATE TABLE sbk_target (k int PRIMARY KEY, v int)")
    cluster.psql("create table sbk_t (k int primary key, v int)")
    cluster.psql(f"""set sink_decouple = true; create sink sbk_sink from sbk_t with (
        connector = 'postgres', host = '{env.get("PGHOST", "localhost")}', port = '{env.get("PGPORT", "5432")}',
        user = '{env.get("PGUSER", "")}', password = '{env.get("PGPASSWORD", "")}',
        database = '{env.get("PGDATABASE", "postgres")}', table = 'sbk_target',
        type = 'upsert', primary_key = 'k')""")

    next_key = 1

    def dml(sql, crash):
        return cluster.psql(sql) if crash else cluster.psql_retry(sql)

    stats = {"rounds": 0, "crashes": {}}
    for r in range(1, args.rounds + 1):
        crash = rnd.random() < args.crash_rate
        target = rnd.choice(["compute", "compute", "meta", "all"]) if crash else None
        killer = None
        if crash:
            killer = threading.Timer(rnd.uniform(0.5, 4.0), cluster.kill, args=(target,))
            killer.start()
        # Values written in this round per key (None = deleted).
        written = {}
        try:
            # Bursts separated by idle periods: the idle epochs are not persisted in the log.
            for _ in range(rnd.randint(1, 4)):
                keys = list(range(next_key, next_key + rnd.randint(1, 30)))
                next_key += len(keys)
                values = {k: rnd.randint(0, 100) for k in keys}
                for k, v in values.items():
                    written.setdefault(k, set()).add(v)
                dml("insert into sbk_t values " + ", ".join(f"({k}, {v})" for k, v in values.items()), crash)
                existing = rows(cluster.psql if crash else cluster.psql_retry, "sbk_t")
                for k in rnd.sample(sorted(existing), min(len(existing), rnd.randint(0, 5))):
                    written.setdefault(k, set()).add(existing[k] + 1)
                    dml(f"update sbk_t set v = v + 1 where k = {k}", crash)
                for k in rnd.sample(sorted(existing), min(len(existing), rnd.randint(0, 2))):
                    written.setdefault(k, set()).add(None)
                    dml(f"delete from sbk_t where k = {k}", crash)
                time.sleep(rnd.uniform(0, args.idle))
        except SqlError:
            if not crash:
                raise
        if killer:
            killer.join()
            time.sleep(0.5)
            stats["crashes"][target] = stats["crashes"].get(target, 0) + 1
            cluster.restart()
        else:
            cluster.psql_retry("flush")

        if crash:
            # Wait until the keys not touched in this round are equal, check that touched keys
            # hold an allowed value, then rewrite them through RisingWave.
            deadline = time.time() + args.converge_timeout
            while True:
                expected, actual = rows(cluster.psql, "sbk_t"), rows(pg, "sbk_target")
                bad = {k: (expected.get(k), actual.get(k)) for k in set(expected) | set(actual)
                       if expected.get(k) != actual.get(k)
                       and (k not in written or actual.get(k) not in written[k] | {expected.get(k)})}
                if not bad:
                    break
                if time.time() > deadline:
                    sys.exit(f"round {r}: sink did not converge after crash: {dict(list(bad.items())[:10])}")
                time.sleep(1)
            touched = sorted(written)
            if touched:
                keys = ", ".join(map(str, touched))
                # Overwrite, then delete every touched key, so that the sink receives a delete
                # even for keys that RisingWave lost but Postgres received.
                cluster.psql_retry("insert into sbk_t values " + ", ".join(f"({k}, 0)" for k in touched))
                cluster.psql_retry("flush")
                cluster.psql_retry(f"delete from sbk_t where k in ({keys})")
                cluster.psql_retry("flush")
                values = [(k, expected[k]) for k in touched if k in expected]
                if values:
                    cluster.psql_retry("insert into sbk_t values " + ", ".join(f"({k}, {v})" for k, v in values))
                cluster.psql_retry("flush")
        deadline = time.time() + args.converge_timeout
        while True:
            expected, actual = rows(cluster.psql, "sbk_t"), rows(pg, "sbk_target")
            if expected == actual:
                break
            if time.time() > deadline:
                diff = {k: (expected.get(k), actual.get(k)) for k in set(expected) | set(actual)
                        if expected.get(k) != actual.get(k)}
                sys.exit(f"round {r}: sink did not converge: {dict(list(diff.items())[:10])}")
            time.sleep(1)
        check_no_panic()
        stats["rounds"] += 1
        print(f"round {r:3}: rows={len(expected):4} crash={target or '-'} converged", flush=True)
    print(f"PASSED {stats}")


if __name__ == "__main__":
    main()
