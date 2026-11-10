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

"""Benchmark of selective barriers on a shared Postgres CDC workload.

Builds (`setup`) a graph shaped like a production deployment: N CDC tables from one shared
Postgres CDC source, one aggregate view per table, chained views on some of them, and a few
decoupled sinks (which write at every barrier). Then (`run`) drives updates in Postgres with
three classes of tables:

  hot   updated about every barrier       (written at every checkpoint in production)
  warm  updated every few seconds         (written at 30-95% of checkpoints)
  cold  updated rarely (mean interval)    (the rest: these tables sleep with the feature)

and measures over the window: jobs and state tables written per checkpoint (from the Hummock
version deltas), SST objects uploaded per checkpoint, object store operations, barrier latency,
CPU time of each process, and CDC freshness (Postgres update -> visible in RisingWave).

Requires a Postgres with `wal_level = logical` reachable with the PG* environment variables.

Usage:
    cdc_benchmark.py setup --tables 100
    cdc_benchmark.py run --label on --duration 300 --out on.json
"""

import argparse
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
import urllib.request

SLOT = "bench_slot"
METRICS = {"compute": 1222, "meta": 1250, "compactor": 1260}
PROCESS_PATTERNS = {
    "compute": "bin/risingwave/compute-node",
    "meta": "bin/risingwave/meta-node",
    "compactor": "bin/risingwave/compactor",
    "frontend": "bin/risingwave/frontend-node",
}


def run(cmd, timeout=300):
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip()[:500])
    return res.stdout.strip()


def rw(sql, port, timeout=300):
    return run(["psql", "-h", "localhost", "-p", str(port), "-d", "dev", "-U", "root",
                "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout)


def pg(sql, timeout=300):
    return run(["psql", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout)


def classes(n, hot, warm):
    """Table index -> class, deterministic."""
    n_hot, n_warm = max(1, round(n * hot)), max(1, round(n * warm))
    return ["hot"] * n_hot + ["warm"] * n_warm + ["cold"] * (n - n_hot - n_warm)


def setup(args):
    n = args.tables
    for stmt in ["drop source if exists bench_src cascade"]:
        rw(stmt, args.port)
    pg(f"""SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
           WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL""")
    time.sleep(1)
    pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{SLOT}'")
    stmts = []
    for i in range(n):
        stmts.append(f"DROP TABLE IF EXISTS bench_t{i}; CREATE TABLE bench_t{i} (k int PRIMARY KEY, g int, v int);"
                     f"INSERT INTO bench_t{i} SELECT k, k % 10, k FROM generate_series(0, 199) k;")
    pg("".join(stmts))
    env = os.environ
    rw(f"""create source bench_src with (
        connector = 'postgres-cdc', hostname = '{env.get("PGHOST", "localhost")}',
        port = '{env.get("PGPORT", "5432")}', username = '{env.get("PGUSER", env.get("USER", ""))}',
        password = '{env.get("PGPASSWORD", "")}', database.name = '{env.get("PGDATABASE", "postgres")}',
        schema.name = 'public', slot.name = '{SLOT}')""", args.port)
    session = "set streaming_parallelism = 2; set background_ddl = false;"
    for i in range(n):
        rw(f"{session} create table bench_t{i} (k int primary key, g int, v int) "
           f"from bench_src table 'public.bench_t{i}'", args.port)
        rw(f"{session} create materialized view bench_m{i} as "
           f"select g, count(*) as c, sum(v) as s, max(v) as mx from bench_t{i} group by g", args.port)
        # Chains of views, as in production.
        if i % 4 == 0:
            rw(f"{session} create materialized view bench_c{i} as "
               f"select sum(c) as c, sum(s) as s from bench_m{i}", args.port)
        print(f"table {i + 1}/{n}", flush=True)
    # Decoupled sinks write their log store at every barrier, like most sinks in production.
    for i in range(args.sinks):
        rw(f"{session} set sink_decouple = true; create sink bench_s{i} from bench_m{i} "
           f"with (connector = 'blackhole')", args.port)
    print("setup done")


def scrape():
    """Sum of every metric sample per (process, name, labels)."""
    out = {}
    for proc, port in METRICS.items():
        try:
            text = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=30).read().decode()
        except OSError:
            continue
        for line in text.splitlines():
            if line.startswith("#"):
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9.eE+-]+|NaN|\+Inf)", line)
            if m:
                try:
                    out[(proc, m.group(1), m.group(2) or "")] = float(m.group(3))
                except ValueError:
                    pass
    return out


def cpu_seconds():
    tick = os.sysconf("SC_CLK_TCK")
    out = {}
    for name, pattern in PROCESS_PATTERNS.items():
        pids = run(["pgrep", "-f", pattern]).split() if subprocess.run(
            ["pgrep", "-f", pattern], capture_output=True).returncode == 0 else []
        total = 0.0
        for pid in pids:
            try:
                fields = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
                total += (int(fields[11]) + int(fields[12])) / tick
            except OSError:
                pass
        out[name] = total
    return out


DELTAS_SQL = """
SELECT id, string_agg(DISTINCT t.table_id::text, ',') AS tables, count(DISTINCT s.info ->> 'objectId') AS ssts
FROM (
    SELECT id, jsonb_path_query(group_deltas, '$.*.groupDeltas[*].newL0SubLevel.insertedTableInfos[*]') AS info
    FROM rw_hummock_version_deltas WHERE id > {last}
) s
CROSS JOIN LATERAL (SELECT jsonb_array_elements_text(s.info -> 'tableIds')::int AS table_id) t
GROUP BY id ORDER BY id
"""


class DeltaCollector(threading.Thread):
    """Collect, per version delta (one per checkpoint), the state tables that wrote."""

    def __init__(self, port, start_id):
        super().__init__(daemon=True)
        self.port, self.last, self.deltas, self.stop = port, start_id, {}, False

    def poll(self):
        out = rw(DELTAS_SQL.format(last=self.last), self.port, timeout=120)
        for line in out.splitlines():
            did, tables, ssts = line.split("|")
            self.deltas[int(did)] = ([int(t) for t in tables.split(",") if t], int(ssts))
            self.last = max(self.last, int(did))

    def run(self):
        while not self.stop:
            try:
                self.poll()
            except RuntimeError:
                pass
            time.sleep(5)


def table_to_job(port):
    mapping = {}
    for line in rw("select id, job_id from rw_internal_tables", port).splitlines():
        t, j = line.split("|")
        mapping[int(t)] = int(j)
    for q in ["select id from rw_tables", "select id from rw_materialized_views"]:
        for line in rw(q, port).splitlines():
            mapping[int(line)] = int(line)
    return mapping


def histogram_quantile(snap0, snap1, proc, name, q):
    buckets = []
    for (p, n, labels), v in snap1.items():
        if p == proc and n == name + "_bucket":
            le = re.search(r'le="([^"]+)"', labels).group(1)
            buckets.append((float("inf") if le == "+Inf" else float(le), v - snap0.get((p, n, labels), 0)))
    if not buckets:
        return None
    agg = {}
    for le, c in buckets:
        agg[le] = agg.get(le, 0) + c
    items = sorted(agg.items())
    total = items[-1][1]
    if total <= 0:
        return None
    for le, c in items:
        if c >= q * total:
            return le
    return None


def counter_diff(snap0, snap1, proc, name, label_key):
    out = {}
    for (p, n, labels), v in snap1.items():
        if p == proc and n == name:
            m = re.search(label_key + r'="([^"]+)"', labels)
            key = m.group(1) if m else ""
            out[key] = out.get(key, 0) + v - snap0.get((p, n, labels), 0)
    return {k: round(v) for k, v in out.items() if v}


class Driver(threading.Thread):
    """Apply updates to the Postgres tables at the rate of their class."""

    def __init__(self, args, kinds, rnd):
        super().__init__(daemon=True)
        self.args, self.kinds, self.rnd, self.stop, self.updates = args, kinds, rnd, False, 0
        self.mean = {"hot": args.hot_interval, "warm": args.warm_interval, "cold": args.cold_interval}
        self.psql = subprocess.Popen(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=0"], stdin=subprocess.PIPE,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        self.lock = threading.Lock()

    def send(self, sql):
        with self.lock:
            self.psql.stdin.write(sql + "\n")
            self.psql.stdin.flush()

    def run(self):
        now = time.time()
        due = [now + self.rnd.expovariate(1 / self.mean[k]) for k in self.kinds]
        while not self.stop:
            now = time.time()
            for i, k in enumerate(self.kinds):
                if due[i] <= now:
                    key = self.rnd.randrange(1, 200)
                    self.send(f"UPDATE bench_t{i} SET v = v + 1 WHERE k = {key};")
                    self.updates += 1
                    due[i] = now + self.rnd.expovariate(1 / self.mean[k])
            time.sleep(0.05)


def measure_freshness(args, kinds, rnd, driver, samples):
    """Update row 0 of a random cold table and wait until RisingWave shows it."""
    cold = [i for i, k in enumerate(kinds) if k == "cold"]
    i = rnd.choice(cold)
    marker = rnd.randrange(10 ** 6, 10 ** 9)
    start = time.time()
    driver.send(f"UPDATE bench_t{i} SET v = {marker} WHERE k = 0;")
    while time.time() - start < 60:
        if rw(f"select v from bench_t{i} where k = 0", args.port) == str(marker):
            samples.append(time.time() - start)
            return
        time.sleep(0.1)
    samples.append(float("inf"))


def run_bench(args):
    rnd = random.Random(args.seed)
    n = int(rw("select count(*) from rw_tables where name like 'bench_t%'", args.port))
    kinds = classes(n, args.hot, args.warm)
    driver = Driver(args, kinds, rnd)
    driver.start()
    print(f"[{args.label}] warmup {args.warmup}s, tables={n}", flush=True)
    time.sleep(args.warmup)

    mapping = table_to_job(args.port)
    start_id = int(rw("select coalesce(max(id), 0) from rw_hummock_version_deltas", args.port))
    collector = DeltaCollector(args.port, start_id)
    snap0, cpu0, t0 = scrape(), cpu_seconds(), time.time()
    collector.start()
    freshness = []
    next_probe = time.time() + 5
    while time.time() - t0 < args.duration:
        if time.time() >= next_probe:
            measure_freshness(args, kinds, rnd, driver, freshness)
            next_probe = time.time() + args.probe_interval
        time.sleep(0.5)
    snap1, cpu1, elapsed = scrape(), cpu_seconds(), time.time() - t0
    collector.stop = True
    collector.join()
    collector.poll()
    driver.stop = True

    deltas = [d for did, d in collector.deltas.items()]
    jobs_per_ckpt = [len({mapping.get(t, t) for t in tables}) for tables, _ in deltas]
    tables_per_ckpt = [len(tables) for tables, _ in deltas]
    ssts = [s for _, s in deltas]
    n_jobs = len(set(mapping.values()))

    def avg(xs):
        return round(sum(xs) / len(xs), 1) if xs else None

    fresh = sorted(freshness)
    result = {
        "label": args.label,
        "elapsed_s": round(elapsed),
        "jobs": n_jobs,
        "updates": driver.updates,
        "checkpoints_with_writes": len(deltas),
        "jobs_written_per_checkpoint": avg(jobs_per_ckpt),
        "state_tables_written_per_checkpoint": avg(tables_per_ckpt),
        "ssts_per_checkpoint": avg(ssts),
        "object_store_ops": {p: counter_diff(snap0, snap1, p, "object_store_operation_latency_count", "type")
                             for p in ["compute", "compactor"]},
        "barrier_latency_p50_s": histogram_quantile(snap0, snap1, "meta", "meta_barrier_duration_seconds", 0.5),
        "barrier_latency_p99_s": histogram_quantile(snap0, snap1, "meta", "meta_barrier_duration_seconds", 0.99),
        "cpu_percent": {k: round(100 * (cpu1[k] - cpu0[k]) / elapsed, 1) for k in cpu1},
        "freshness_p50_s": round(fresh[len(fresh) // 2], 2) if fresh else None,
        "freshness_max_s": round(fresh[-1], 2) if fresh else None,
        "freshness_samples": len(fresh),
    }
    print(json.dumps(result, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["setup", "run"])
    p.add_argument("--port", default=4577, type=int)
    p.add_argument("--tables", default=100, type=int)
    p.add_argument("--sinks", default=6, type=int, help="decoupled sinks, on the first (hot) views")
    p.add_argument("--hot", default=0.06, type=float, help="share of hot tables")
    p.add_argument("--warm", default=0.04, type=float, help="share of warm tables")
    p.add_argument("--hot-interval", default=0.5, type=float, help="mean seconds between updates")
    p.add_argument("--warm-interval", default=2.0, type=float)
    p.add_argument("--cold-interval", default=120.0, type=float)
    p.add_argument("--duration", default=300, type=int)
    p.add_argument("--warmup", default=60, type=int)
    p.add_argument("--probe-interval", default=10, type=float)
    p.add_argument("--seed", default=1, type=int)
    p.add_argument("--label", default="run")
    p.add_argument("--out", default=None)
    args = p.parse_args()
    if args.command == "setup":
        setup(args)
    else:
        run_bench(args)


if __name__ == "__main__":
    main()
