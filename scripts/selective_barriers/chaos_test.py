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

"""Crash/restart consistency test for selective barriers.

Runs a DML workload on a small streaming graph (the same as
e2e_test/streaming/selective_barrier/consistency.slt), randomly kills
cluster processes with SIGKILL while the workload runs, restarts the cluster
and checks after every round that:

  1. no row confirmed by a successful FLUSH is lost or altered (durability),
  2. every materialized view equals its batch recomputation (consistency),
  3. two views derived from the same table agree when read together.

Each round modifies a random subset of the tables, so that most rounds leave
part of the graph idle, which is the case selective barriers optimize.

The cluster must use persistent storage (e.g. sqlite meta store and a fs or
minio object store) so that a restart recovers the previous state.

Usage (from the repository root, with the cluster started by risedev):
    scripts/selective_barriers/chaos_test.py --profile selbar --port 4577 --rounds 40
"""

import argparse
import os
import random
import signal
import subprocess
import sys
import threading
import time

TABLES = ["sb_a", "sb_b", "sb_c"]

SCHEMA = [
    "create table sb_a (k int primary key, g int, v int)",
    "create table sb_b (k int primary key, g int, v int)",
    "create table sb_c (k int primary key, g int, v int)",
    "create table sb_ev (k int primary key, ts timestamptz)",
    "create materialized view sb_a_by_g as select g, count(*) as c, sum(v) as s from sb_a group by g",
    "create materialized view sb_a_total as select count(*) as c, sum(v) as s from sb_a",
    "create materialized view sb_b_by_g as select g, count(*) as c, sum(v) as s from sb_b group by g",
    "create materialized view sb_ab as select a.k, a.v as va, b.v as vb from sb_a a join sb_b b on a.k = b.k",
    "create materialized view sb_abc as select ab.k, ab.va, ab.vb, c.v as vc from sb_ab ab join sb_c c on ab.k = c.k",
    "create materialized view sb_abc_g as select k % 3 as g, count(*) as c, sum(va + vb + vc) as s from sb_abc group by k % 3",
    "create materialized view sb_c_only as select g, max(v) as m from sb_c group by g",
    "create materialized view sb_ev_past as select k from sb_ev where ts < now()",
    "create sink sb_sink as select * from sb_abc_g with (connector = 'blackhole')",
    "create index sb_c_idx on sb_c(v)",
]

# (name, query returning the number of mismatching rows)
CHECKS = [
    ("sb_a_by_g", """select count(*) from (
        (select g, c, s from sb_a_by_g except select g, count(*), sum(v) from sb_a group by g)
        union all
        (select g, count(*), sum(v) from sb_a group by g except select g, c, s from sb_a_by_g)) d"""),
    ("sb_b_by_g", """select count(*) from (
        (select g, c, s from sb_b_by_g except select g, count(*), sum(v) from sb_b group by g)
        union all
        (select g, count(*), sum(v) from sb_b group by g except select g, c, s from sb_b_by_g)) d"""),
    ("sb_ab", """select count(*) from (
        (select k, va, vb from sb_ab except select a.k, a.v, b.v from sb_a a join sb_b b on a.k = b.k)
        union all
        (select a.k, a.v, b.v from sb_a a join sb_b b on a.k = b.k except select k, va, vb from sb_ab)) d"""),
    ("sb_abc", """select count(*) from (
        (select k, va, vb, vc from sb_abc except
         select a.k, a.v, b.v, c.v from sb_a a join sb_b b on a.k = b.k join sb_c c on a.k = c.k)
        union all
        (select a.k, a.v, b.v, c.v from sb_a a join sb_b b on a.k = b.k join sb_c c on a.k = c.k
         except select k, va, vb, vc from sb_abc)) d"""),
    ("sb_abc_g", """select count(*) from (
        (select g, c, s from sb_abc_g except
         select a.k % 3, count(*), sum(a.v + b.v + c.v)
         from sb_a a join sb_b b on a.k = b.k join sb_c c on a.k = c.k group by a.k % 3)
        union all
        (select a.k % 3, count(*), sum(a.v + b.v + c.v)
         from sb_a a join sb_b b on a.k = b.k join sb_c c on a.k = c.k group by a.k % 3
         except select g, c, s from sb_abc_g)) d"""),
    ("sb_c_only", """select count(*) from (
        (select g, m from sb_c_only except select g, max(v) from sb_c group by g)
        union all
        (select g, max(v) from sb_c group by g except select g, m from sb_c_only)) d"""),
    ("sb_ev_past", """select count(*) from (
        (select k from sb_ev_past except select k from sb_ev where ts < now())
        union all
        (select k from sb_ev where ts < now() except select k from sb_ev_past)) d"""),
    ("same-epoch", """select case when
        coalesce((select c from sb_a_total), 0) = coalesce((select sum(c) from sb_a_by_g), 0)
        and coalesce((select s from sb_a_total), 0) = coalesce((select sum(s) from sb_a_by_g), 0)
        then 0 else 1 end"""),
]

PROCESS_PATTERNS = {
    "compute": "bin/risingwave/compute-node",
    "meta": "bin/risingwave/meta-node",
    "frontend": "bin/risingwave/frontend-node",
}


class SqlError(Exception):
    pass


class Cluster:
    def __init__(self, args):
        self.args = args

    def psql(self, sql, timeout=120):
        cmd = ["psql", "-h", self.args.host, "-p", str(self.args.port), "-d", "dev", "-U", "root",
               "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1",
               # Snapshot backfill is not supported by selective barriers yet: a graph containing
               # it always receives full barriers.
               "-c", "set streaming_use_snapshot_backfill = false", "-c", sql]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            raise SqlError(f"timeout: {sql[:80]}") from e
        if res.returncode != 0:
            raise SqlError(res.stderr.strip()[:500])
        return res.stdout.strip()

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
        for _ in range(attempts):
            if self.args.before_restart:
                subprocess.run([self.args.before_restart], capture_output=True)
            # Make sure nothing from the previous run is left, including the tmux
            # server used by risedev, which may refuse a new session otherwise.
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
                self.psql("select count(*) from sb_abc_g", timeout=30)
                # DML needs the vnode mappings, which the frontend may receive later than the
                # catalog after a restart.
                self.psql("delete from sb_a where k = -1", timeout=30)
                return
            except SqlError as e:
                last = e
                time.sleep(2)
        raise RuntimeError(f"cluster not ready after {timeout}s: {last}")


def read_table(cluster, table):
    rows = {}
    for line in cluster.psql(f"select k, v from {table}").splitlines():
        k, v = line.split("|")
        rows[int(k)] = int(v)
    return rows


def run_checks(cluster):
    failures = []
    for name, q in CHECKS:
        n = int(cluster.psql(q))
        if n != 0:
            failures.append(f"{name}: {n} mismatching rows")
    return failures


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="selbar")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", default=4577, type=int)
    p.add_argument("--rounds", default=40, type=int)
    p.add_argument("--crash-rate", default=0.3, type=float, help="probability of a crash in a round")
    p.add_argument("--seed", default=int(time.time()), type=int)
    p.add_argument("--no-setup", action="store_true", help="reuse an existing schema")
    p.add_argument("--before-restart", default=None,
                   help="executable run before each restart, e.g. to save the logs")
    p.add_argument("--pause-before-flush", default=0.0, type=float,
                   help="max seconds to wait between the DML statements and FLUSH, so that "
                        "periodic barriers deliver the data (with selective barriers, through rings)")
    p.add_argument("--idle", default=0.0, type=float,
                   help="max seconds without any change between rounds, so that fragments sleep")
    args = p.parse_args()

    rnd = random.Random(args.seed)
    print(f"seed={args.seed}")
    cluster = Cluster(args)

    if not args.no_setup:
        for t in ["sb_sink"]:
            try:
                cluster.psql(f"drop sink if exists {t}")
            except SqlError:
                pass
        for t in ["sb_abc_g", "sb_abc", "sb_ab", "sb_c_only", "sb_a_total", "sb_a_by_g", "sb_b_by_g", "sb_ev_past"]:
            cluster.psql(f"drop materialized view if exists {t}")
        cluster.psql("drop index if exists sb_c_idx")
        for t in TABLES + ["sb_ev"]:
            cluster.psql(f"drop table if exists {t}")
        for stmt in SCHEMA:
            cluster.psql(stmt)
        cluster.psql("insert into sb_ev values (1, now() - interval '10 days'), (2, now() + interval '10 days')")
        cluster.psql("flush")

    # Model of the durable content of each table: key -> value.
    model = {t: read_table(cluster, t) for t in TABLES}
    next_key = 1 + max([0] + [k for t in TABLES for k in model[t]])
    stats = {"rounds": 0, "crashes": {}, "uncertain_resolved": 0}

    for r in range(1, args.rounds + 1):
        tables = rnd.sample(TABLES, rnd.randint(1, len(TABLES)))
        # Pending operations of this round, per table: key -> new value (None = delete).
        pending = {t: {} for t in tables}
        statements = []
        for t in tables:
            new_keys = list(range(next_key, next_key + rnd.randint(5, 40)))
            next_key += len(new_keys)
            # At most one statement per key and round: without implicit flush, an UPDATE
            # reads the committed snapshot and would not see an INSERT of the same round.
            keys = sorted(set(model[t]) - set(new_keys))
            # Inserts use keys shared across tables so that joins produce rows.
            vals = {k: rnd.randint(-100, 100) for k in new_keys}
            for k, v in vals.items():
                pending[t][k] = v
            statements.append(f"insert into {t} values " + ", ".join(f"({k}, {k % 5}, {v})" for k, v in vals.items()))
            for k in rnd.sample(keys, min(len(keys), rnd.randint(0, 10))):
                v = rnd.randint(-100, 100)
                pending[t][k] = v
                statements.append(f"update {t} set v = {v} where k = {k}")
            for k in rnd.sample(keys, min(len(keys), rnd.randint(0, 5))):
                if k not in pending[t]:
                    pending[t][k] = None
                    statements.append(f"delete from {t} where k = {k}")
        # Make keys collide across tables to feed the joins.
        next_key = next_key - rnd.randint(0, 30)

        crash = rnd.random() < args.crash_rate
        target = rnd.choice(["compute", "compute", "meta", "all"]) if crash else None
        killer = None
        if crash:
            delay = rnd.uniform(0.0, 2.0)
            killer = threading.Timer(delay, cluster.kill, args=(target,))
            killer.start()

        ok = True
        try:
            for s in statements:
                cluster.psql(s, timeout=60)
                if args.pause_before_flush:
                    time.sleep(rnd.uniform(0, args.pause_before_flush / max(len(statements), 1)))
            if args.pause_before_flush:
                time.sleep(rnd.uniform(0, args.pause_before_flush))
            cluster.psql("flush", timeout=60)
        except SqlError as e:
            ok = False
            if not crash:
                print(f"round {r}: statement failed without crash: {e}", flush=True)
        if killer:
            killer.join()
            time.sleep(0.5)

        if crash:
            stats["crashes"][target] = stats["crashes"].get(target, 0) + 1
            cluster.restart()

        # Durability: the model plus, if the round was not confirmed, either
        # outcome for each touched key.
        for t in TABLES:
            actual = read_table(cluster, t)
            expected = dict(model[t])
            touched = pending.get(t, {})
            if ok:
                for k, v in touched.items():
                    if v is None:
                        expected.pop(k, None)
                    else:
                        expected[k] = v
                if actual != expected:
                    diff = {k: (expected.get(k), actual.get(k)) for k in set(expected) | set(actual)
                            if expected.get(k) != actual.get(k)}
                    sys.exit(f"round {r}: durability violated on {t}: {dict(list(diff.items())[:10])}")
            else:
                for k in set(expected) | set(actual) | set(touched):
                    allowed = {expected.get(k)}
                    if k in touched:
                        allowed.add(touched[k])
                    if actual.get(k) not in allowed:
                        sys.exit(f"round {r}: unexpected value on {t}[{k}]: {actual.get(k)} not in {allowed}")
                stats["uncertain_resolved"] += 1
            model[t] = actual

        failures = run_checks(cluster)
        if failures:
            sys.exit(f"round {r}: consistency violated: {failures}")
        stats["rounds"] += 1
        if args.idle:
            time.sleep(rnd.uniform(0, args.idle))
            # Read while fragments may be asleep (no FLUSH): sleeping tables are published at the
            # latest epoch without data, and every view must still be consistent.
            failures = run_checks(cluster)
            if failures:
                sys.exit(f"round {r}: consistency violated while idle: {failures}")
        print(f"round {r:3}: tables={','.join(tables):14} statements={len(statements):3} "
              f"confirmed={ok} crash={target or '-'} checks=ok", flush=True)

    print(f"PASSED {stats}")


if __name__ == "__main__":
    main()
