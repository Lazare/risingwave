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

"""End-to-end test of sink groups: shared Postgres connection pool and `last` sinks.

Postgres sinks that reference the same connection (`CREATE CONNECTION ... type = 'postgres'`)
share a bounded connection pool per compute node, and a sink of the group declared
`sink_order = 'last'` writes the data of an epoch after the other sinks have committed it.

Graph: a table `sg_t (k, batch)` is sunk twice (`sg_a`, and `sg_b` whose Postgres table is
slowed down by a trigger), and a marker sink writes `(batch, n)` from an aggregate of `sg_t`.
A trigger on the marker table records, for every marker write, how many rows of the batch are
present in `sg_a` and `sg_b`: with `sink_order = 'last'` on the marker sink, both must hold at
least `n` rows. A second table `sg_ping` is sunk by a `last` sink and by a normal sink, to measure
the delay added to a `last` sink when the other sinks have nothing to write.

Steps:
  1. connections: number of Postgres connections with and without the shared connection;
  2. placement: the sinks of the group run on a single compute node;
  3. ordering: batches, optional kills of the compute node, the trigger must report no violation
     (with --no-last, the marker sink is not ordered and violations are expected);
  4. node loss (with at least two compute nodes): the node that runs the sinks is killed and not
     restarted, the sinks must all move to another node and stay ordered;
  5. reactivity: delay of the `last` ping sink compared with the normal one, other sinks idle;
  6. reconnect: the connections of the sinks are closed in Postgres, the sinks must go on without
     a recovery of the streaming graph;
  7. idle: without traffic, nothing is written to Postgres nor to the state store.

Requires a Postgres reachable with the PG* environment variables, and a cluster with persistent
storage. It can run with several compute nodes: the sinks of a group are placed on one of them.

Usage (from the repository root, with the cluster started by risedev):
    PGHOST=localhost PGPORT=5433 PGUSER=postgres \\
    scripts/sink_group/sink_group_test.py --profile selbar-on --port 4577
"""

import argparse
import os
import random
import signal
import statistics
import subprocess
import sys
import time

COMPUTE_PATTERN = "bin/risingwave/compute-node"
LOG_DIR = ".risingwave/log"
ENV = os.environ


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

    def psql_retry(self, sql, attempts=60):
        """Run a statement, retrying while the database recovers."""
        for i in range(attempts):
            try:
                return self.psql(sql)
            except SqlError:
                if i == attempts - 1:
                    raise
                time.sleep(2)

    def kill_compute(self):
        out = subprocess.run(["pgrep", "-f", COMPUTE_PATTERN], capture_output=True, text=True).stdout
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
                self.psql("select count(*) from sg_t", timeout=30)
                self.psql("flush", timeout=60)
                return
            except SqlError:
                time.sleep(2)
        raise RuntimeError("cluster not ready")


PG_SCHEMA = """
DROP TABLE IF EXISTS sg_a, sg_b, sg_marker, sg_audit, sg_ping_last, sg_ping_free CASCADE;
CREATE TABLE sg_a (k int PRIMARY KEY, batch int);
CREATE TABLE sg_b (k int PRIMARY KEY, batch int);
CREATE TABLE sg_marker (batch int PRIMARY KEY, n bigint);
CREATE TABLE sg_ping_last (id int PRIMARY KEY, v int, at timestamptz DEFAULT clock_timestamp());
CREATE TABLE sg_ping_free (id int PRIMARY KEY, v int, at timestamptz DEFAULT clock_timestamp());
CREATE INDEX ON sg_a (batch);
CREATE INDEX ON sg_b (batch);
-- One row per write of a marker, with the rows of the batch present in the other tables.
CREATE TABLE sg_audit (seq bigserial PRIMARY KEY, batch int, n bigint, a bigint, b bigint);

CREATE OR REPLACE FUNCTION sg_marker_audit() RETURNS trigger AS $$
BEGIN
    INSERT INTO sg_audit (batch, n, a, b) VALUES (
        NEW.batch, NEW.n,
        (SELECT count(*) FROM sg_a WHERE batch = NEW.batch),
        (SELECT count(*) FROM sg_b WHERE batch = NEW.batch));
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER sg_marker_audit BEFORE INSERT OR UPDATE ON sg_marker
    FOR EACH ROW EXECUTE FUNCTION sg_marker_audit();

-- Slow down the writes of sg_b, so that an unordered marker sink is ahead of it.
CREATE OR REPLACE FUNCTION sg_b_slow() RETURNS trigger AS $$
BEGIN
    PERFORM pg_sleep(current_setting('sg.slow_seconds', true)::float);
    RETURN NULL;
END $$ LANGUAGE plpgsql;
"""


def pg_connections():
    """Connections opened by the sinks: everything but this session."""
    return int(pg("""SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'"""))


def pg_writes():
    return int(pg("""SELECT coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0) FROM pg_stat_user_tables
        WHERE relname LIKE 'sg\\_%'"""))


def wait_until(predicate, timeout, what):
    deadline = time.time() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.time() > deadline:
            sys.exit(f"timeout waiting for {what}")
        time.sleep(0.5)


SINKS = {
    # sink name: (source relation, Postgres table, primary key, is the ordered sink)
    "sg_sink_a": ("sg_t", "sg_a", "k", False),
    "sg_sink_b": ("sg_t", "sg_b", "k", False),
    "sg_sink_marker": ("sg_marker_mv", "sg_marker", "batch", True),
    "sg_sink_ping_last": ("sg_ping", "sg_ping_last", "id", True),
    "sg_sink_ping_free": ("sg_ping", "sg_ping_free", "id", False),
}

PG_OPTIONS = (f"host = '{ENV.get('PGHOST', 'localhost')}', port = '{ENV.get('PGPORT', '5432')}', "
              f"user = '{ENV.get('PGUSER', '')}', password = '{ENV.get('PGPASSWORD', '')}', "
              f"database = '{ENV.get('PGDATABASE', 'postgres')}'")


def create_sinks(cluster, use_connection, ordered):
    for name, (source, table, pk, is_last) in SINKS.items():
        options = "connection = sg_conn" if use_connection else PG_OPTIONS
        if is_last and ordered and use_connection:
            options += ", sink_order = 'last'"
        cluster.psql(f"""set sink_decouple = true; create sink {name} from {source} with (
            connector = 'postgres', {options}, table = '{table}', type = 'upsert', primary_key = '{pk}')""")


def drop_sinks(cluster):
    for name in SINKS:
        cluster.psql(f"drop sink if exists {name}")


def setup(cluster, args):
    drop_sinks(cluster)
    cluster.psql("drop materialized view if exists sg_marker_mv")
    cluster.psql("drop table if exists sg_t")
    cluster.psql("drop table if exists sg_ping")
    cluster.psql("drop connection if exists sg_conn")
    pg(PG_SCHEMA)
    pg(f"ALTER DATABASE {ENV.get('PGDATABASE', 'postgres')} SET sg.slow_seconds = '{args.slow_seconds}'")
    pg("""CREATE TRIGGER sg_b_slow AFTER INSERT OR UPDATE ON sg_b
        FOR EACH STATEMENT EXECUTE FUNCTION sg_b_slow()""")
    cluster.psql("create table sg_t (k int primary key, batch int)")
    cluster.psql("create table sg_ping (id int primary key, v int)")
    cluster.psql("create materialized view sg_marker_mv as select batch, count(*) as n from sg_t group by batch")
    cluster.psql(f"create connection sg_conn with (type = 'postgres', {PG_OPTIONS}, pool.max_size = '{args.pool_size}')")


def step_connections(cluster, args):
    """Number of Postgres connections without and with the shared connection."""
    base = pg_connections()
    create_sinks(cluster, use_connection=False, ordered=False)
    cluster.psql("insert into sg_ping values (0, 0)")
    cluster.psql("flush")
    wait_until(lambda: pg("select count(*) from sg_ping_free") == "1", 60, "sinks without connection")
    time.sleep(2)
    separate = pg_connections() - base
    drop_sinks(cluster)
    wait_until(lambda: pg_connections() <= base, 60, "connections of dropped sinks to close")

    create_sinks(cluster, use_connection=True, ordered=not args.no_last)
    cluster.psql("insert into sg_ping values (0, 1)")
    cluster.psql("flush")
    wait_until(lambda: pg("select v from sg_ping_free where id = 0") == "1", 60, "sinks with connection")
    time.sleep(2)
    shared = pg_connections() - base
    print(f"connections: {len(SINKS)} sinks, separate={separate} shared={shared} (pool.max_size={args.pool_size})")
    if shared > args.pool_size:
        sys.exit(f"FAILED: {shared} connections with a pool of {args.pool_size}")
    if separate <= shared:
        sys.exit("FAILED: the shared connection does not reduce the number of connections")
    return {"separate": separate, "shared": shared}


def step_placement(cluster):
    """The sinks of the group run on one compute node, whatever the placement of their upstream."""
    def workers(flag, names):
        return cluster.psql(f"""select count(distinct a.worker_id) from rw_actors a
            join rw_fragments f on f.fragment_id = a.fragment_id
            where '{flag}' = any(f.flags) and f.table_id in ({names})""")

    sinks = int(workers("SINK", "select id from rw_sinks where name like 'sg_sink_%'"))
    upstream = int(workers("MVIEW", "select id from rw_tables where name in ('sg_t', 'sg_ping')"))
    nodes = int(cluster.psql("select count(*) from rw_worker_nodes where type = 'WORKER_TYPE_COMPUTE_NODE'"))
    print(f"placement: compute nodes={nodes} nodes running the upstream tables={upstream} "
          f"nodes running the sinks of the group={sinks}")
    if sinks != 1:
        sys.exit(f"FAILED: the sinks of the group run on {sinks} compute nodes")
    return {"compute_nodes": nodes, "upstream_nodes": upstream, "sink_nodes": sinks}


def violations():
    return int(pg("select count(*) from sg_audit where a < n or b < n"))


def wait_converged(cluster, args, what):
    def converged():
        n = cluster.psql_retry("select count(*) from sg_t")
        markers = cluster.psql_retry("select count(*), coalesce(sum(n), 0) from sg_marker_mv")
        return (pg("select count(*) from sg_a") == n and pg("select count(*) from sg_b") == n
                and pg("select count(*), coalesce(sum(n), 0) from sg_marker") == markers)

    wait_until(converged, args.converge_timeout, f"{what} to converge")


def sink_workers(cluster):
    """(worker id, port) of the compute nodes that run the sinks of the group."""
    try:
        rows = cluster.psql("""select distinct w.id, w.port from rw_actors a
            join rw_fragments f on f.fragment_id = a.fragment_id
            join rw_worker_nodes w on w.id = a.worker_id
            where 'SINK' = any(f.flags) and f.table_id in (select id from rw_sinks where name like 'sg_sink_%')""")
    except SqlError:
        return []
    return [tuple(line.split("|")) for line in rows.splitlines()]


def step_node_loss(cluster, args):
    """The compute node that runs the sinks of the group is lost and does not come back: all the
    sinks must move together to another node, and the order must still hold."""
    before = sink_workers(cluster)
    nodes = int(cluster.psql("select count(*) from rw_worker_nodes where type = 'WORKER_TYPE_COMPUTE_NODE'"))
    if nodes < 2:
        print("node loss: skipped, it needs at least two compute nodes")
        return None
    assert len(before) == 1, before
    worker_id, port = before[0]
    violations_before = violations()
    # Kill the node and the script that runs it, so that it is not started again.
    subprocess.run(["pkill", "-9", "-f", f"run_command.sh .*compute-node-{port}.log"])
    subprocess.run(["pkill", "-9", "-f", f"compute-node .*--listen-addr [0-9.]*:{port} "])
    killed_at = time.time()

    def moved():
        workers = sink_workers(cluster)
        return len(workers) == 1 and workers[0][0] != worker_id and workers

    after = wait_until(moved, 300, "the sinks of the group to move to another compute node")
    moved_after = time.time() - killed_at
    next_key = int(cluster.psql_retry("select coalesce(max(k), 0) + 1 from sg_t"))
    batch = int(cluster.psql_retry("select coalesce(max(batch), 0) + 1 from sg_t"))
    batches = 5
    for b in range(batch, batch + batches):
        keys = range(next_key, next_key + 10)
        next_key += 10
        cluster.psql_retry("insert into sg_t values " + ", ".join(f"({k}, {b})" for k in keys))
        time.sleep(0.5)
    cluster.psql_retry("flush")
    wait_converged(cluster, args, "node loss")
    res = {"lost_worker": int(worker_id), "new_worker": int(after[0][0]), "moved_after_s": round(moved_after, 1),
           "batches": batches, "violations": violations() - violations_before}
    print(f"node loss: {res}")
    step_placement(cluster)
    # Bring the lost node back for the next steps.
    cluster.restart()
    step_placement(cluster)
    return res


def step_ordering(cluster, args, rnd):
    """Batches with a slow sink; the marker must be written after the rows of its batch."""
    next_key, batch, kills = 1, 0, 0
    for r in range(1, args.rounds + 1):
        kill = rnd.random() < args.crash_rate
        inserted = []
        try:
            for _ in range(rnd.randint(2, 6)):
                batch += 1
                keys = range(next_key, next_key + rnd.randint(1, 20))
                next_key = keys[-1] + 1
                sql = "insert into sg_t values " + ", ".join(f"({k}, {batch})" for k in keys)
                inserted.append(sql)
                cluster.psql(sql) if kill else cluster.psql_retry(sql)
                time.sleep(rnd.uniform(0, 1.5))
            if kill:
                cluster.kill_compute()
                kills += 1
        except SqlError:
            if not kill:
                raise
        if kill:
            time.sleep(0.5)
            cluster.restart()
            # The sinks are at-least-once: Postgres may hold rows of this round that RisingWave
            # lost in the crash (DML not confirmed by FLUSH). Insert them again, so that both
            # sides converge to the same rows.
            for sql in inserted:
                cluster.psql_retry(sql)
        cluster.psql_retry("flush")

        wait_converged(cluster, args, f"round {r}")
        check_no_panic()
        print(f"round {r:3}: batches={batch:4} kill={'compute' if kill else '-':7} "
              f"marker writes={pg('select count(*) from sg_audit')} violations={violations()}", flush=True)
    return {"batches": batch, "kills": kills, "marker_writes": int(pg("select count(*) from sg_audit")),
            "violations": violations()}


def step_reactivity(cluster, args):
    """Delay of a `last` sink compared with a normal sink of the same table, other sinks idle."""
    delays_last, delays_free = [], []
    for i in range(1, args.pings + 1):
        cluster.psql(f"insert into sg_ping values ({i}, {i})")
        sent = time.time()
        deadline = sent + 30
        seen = {}
        while len(seen) < 2:
            for table in ("sg_ping_last", "sg_ping_free"):
                if table not in seen and pg(f"select count(*) from {table} where id = {i}") == "1":
                    seen[table] = time.time() - sent
            if time.time() > deadline:
                sys.exit(f"timeout waiting for ping {i}: {seen}")
        delays_last.append(seen["sg_ping_last"])
        delays_free.append(seen["sg_ping_free"])
        time.sleep(0.7)
    # Delay measured in Postgres between the two writes of the same ping.
    lag = [float(x) for x in pg("""select extract(epoch from l.at - f.at) from sg_ping_last l
        join sg_ping_free f using (id) where id > 0 order by id""").splitlines()]
    res = {"last_p50": statistics.median(delays_last), "last_max": max(delays_last),
           "free_p50": statistics.median(delays_free), "free_max": max(delays_free),
           "lag_p50": statistics.median(lag), "lag_max": max(lag), "lag_min": min(lag)}
    print("reactivity: " + " ".join(f"{k}={v:.3f}s" for k, v in res.items()))
    return res


def log_count(text):
    count = 0
    for name in os.listdir(LOG_DIR):
        if name.startswith("compute-node"):
            with open(os.path.join(LOG_DIR, name), errors="replace") as f:
                count += sum(text in line for line in f)
    return count


def recoveries(cluster):
    return int(cluster.psql("select count(*) from rw_catalog.rw_event_logs where event_type like '%RECOVERY_START'"))


def step_reconnect(cluster, args):
    """Closing the connections of the sinks in Postgres must not fail the sinks."""
    before = recoveries(cluster)
    first = args.pings + 1
    for i in range(first, first + args.reconnects):
        killed = pg("""SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
            WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'""")
        cluster.psql(f"insert into sg_ping values ({i}, {i})")
        # Also close connections while the sinks may be writing.
        pg("""SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
            WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'""")
        wait_until(lambda: pg(f"select count(*) from sg_ping_last where id = {i}") == "1"
                   and pg(f"select count(*) from sg_ping_free where id = {i}") == "1",
                   60, f"ping {i} after closing {killed} connections")
    # Close a connection in the middle of a transaction of a sink: the chunk is written again.
    retries_before = log_count("postgres sink connection failed, retrying")
    pg("""CREATE OR REPLACE FUNCTION sg_ping_slow() RETURNS trigger AS $$
        BEGIN PERFORM pg_sleep(3); RETURN NULL; END $$ LANGUAGE plpgsql;
        CREATE TRIGGER sg_ping_slow AFTER INSERT OR UPDATE ON sg_ping_free
        FOR EACH STATEMENT EXECUTE FUNCTION sg_ping_slow()""")
    i = first + args.reconnects
    cluster.psql(f"insert into sg_ping values ({i}, {i})")
    wait_until(lambda: pg("""SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
        WHERE wait_event = 'PgSleep' AND pid <> pg_backend_pid()""") != "0", 30, "a sink writing sg_ping_free")
    pg("DROP TRIGGER sg_ping_slow ON sg_ping_free")
    wait_until(lambda: pg(f"select count(*) from sg_ping_last where id = {i}") == "1"
               and pg(f"select count(*) from sg_ping_free where id = {i}") == "1",
               60, f"ping {i} after closing a connection in a transaction")
    res = {"rounds": args.reconnects, "recoveries": recoveries(cluster) - before,
           "mid_write_retries": log_count("postgres sink connection failed, retrying") - retries_before}
    print(f"reconnect: {res}")
    return res


def step_idle(cluster, args):
    """Without traffic: no write to Postgres, no write to the state store."""
    cluster.psql("flush")
    # Postgres sessions report their table statistics up to 10 s after becoming idle.
    time.sleep(15)
    writes0 = pg_writes()
    delta0 = int(cluster.psql("select coalesce(max(id), 0) from rw_hummock_version_deltas"))
    time.sleep(args.idle)
    writes = pg_writes() - writes0
    checkpoints = int(cluster.psql(f"select count(*) from rw_hummock_version_deltas where id > {delta0}"))
    with_writes = int(cluster.psql(f"""select count(distinct id) from (
        select id, jsonb_path_query(group_deltas, '$.*.groupDeltas[*].newL0SubLevel.insertedTableInfos[*]') as info
        from rw_hummock_version_deltas where id > {delta0}) s"""))
    print(f"idle {args.idle}s: postgres writes={writes} checkpoints={checkpoints} checkpoints with writes={with_writes}")
    return {"pg_writes": writes, "checkpoints": checkpoints, "checkpoints_with_writes": with_writes}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="selbar-on")
    p.add_argument("--port", default=4577, type=int)
    p.add_argument("--rounds", default=10, type=int)
    p.add_argument("--crash-rate", default=0.3, type=float)
    p.add_argument("--seed", default=int(time.time()), type=int)
    p.add_argument("--pool-size", default=3, type=int)
    p.add_argument("--slow-seconds", default=0.3, type=float, help="delay of every write to sg_b")
    p.add_argument("--pings", default=20, type=int)
    p.add_argument("--reconnects", default=5, type=int)
    p.add_argument("--idle", default=30, type=int)
    p.add_argument("--converge-timeout", default=120, type=int)
    p.add_argument("--no-last", action="store_true",
                   help="control run: do not order the marker sink, violations are expected")
    args = p.parse_args()
    rnd = random.Random(args.seed)
    print(f"seed={args.seed}")
    cluster = Cluster(args)

    setup(cluster, args)
    result = {"connections": step_connections(cluster, args)}
    result["placement"] = step_placement(cluster)
    result["ordering"] = step_ordering(cluster, args, rnd)
    # The placement must be the same after the recoveries.
    step_placement(cluster)
    result["node_loss"] = step_node_loss(cluster, args)
    # The slow trigger is only needed for the ordering steps.
    pg("DROP TRIGGER sg_b_slow ON sg_b")
    result["reactivity"] = step_reactivity(cluster, args)
    result["reconnect"] = step_reconnect(cluster, args)
    result["idle"] = step_idle(cluster, args)
    check_no_panic()

    failures = []
    if args.no_last:
        if result["ordering"]["violations"] == 0:
            failures.append("control run without `last`: no violation observed, the test is not sensitive")
    else:
        if result["ordering"]["violations"] != 0:
            failures.append(f"{result['ordering']['violations']} markers written before the rows of their batch")
        if result["reactivity"]["lag_min"] < 0:
            failures.append("a `last` ping was written before the normal one")
    if result["node_loss"] and result["node_loss"]["violations"] != 0 and not args.no_last:
        failures.append(f"{result['node_loss']['violations']} markers written before their rows after a node loss")
    if result["reconnect"]["recoveries"] != 0:
        failures.append(f"{result['reconnect']['recoveries']} recoveries after closing the sink connections")
    if result["reconnect"]["mid_write_retries"] == 0:
        failures.append("the write interrupted by a closed connection was not retried")
    if result["idle"]["pg_writes"] != 0:
        failures.append(f"{result['idle']['pg_writes']} writes to Postgres without traffic")
    if result["idle"]["checkpoints_with_writes"] > 1:
        failures.append(f"{result['idle']['checkpoints_with_writes']} checkpoints with writes without traffic")
    if failures:
        sys.exit("FAILED: " + "; ".join(failures) + f"\n{result}")
    print(f"PASSED {result}")


if __name__ == "__main__":
    main()
