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

"""Source connectors and sinks under selective barriers.

One scenario per executor family that a connector goes through (connector source, table with
connector, shared source with backfill, file source, webhook, shared CDC source, sink with and
without log store, sink into table, ...). Each scenario alternates bursts of changes and idle
periods, long enough for the fragments that may sleep to fall asleep, and checks that the data
read from RisingWave or from the external system converges to the expected value. At the end, the
logs must show no panic and no recovery.

External systems are optional, a scenario is skipped without its system:
  - Postgres with `wal_level = logical`: PG* environment variables (PGHOST, PGPORT, PGUSER, ...)
  - Kafka API: --kafka-broker host:port and --kafka-container (docker container with `rpk`)
  - MySQL with row binlog: --mysql-host, --mysql-container (docker container with `mysql`)

Usage (from the repository root, with a cluster started by risedev):
    PGHOST=localhost PGPORT=5433 PGUSER=postgres PGDATABASE=postgres \\
    scripts/selective_barriers/connector_matrix.py --port 4577 \\
        --kafka-broker sb-kafka:9092 --kafka-container sb-kafka \\
        --mysql-host sb-mysql --mysql-container sb-mysql --mysql-password sbroot
"""

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.request

LOG_DIR = ".risingwave/log"
ARGS = None


class CheckFailed(Exception):
    pass


def sh(cmd, stdin=None, timeout=300):
    res = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        raise CheckFailed(f"{' '.join(cmd)[:120]}: {res.stderr.strip()[:600]}")
    return res.stdout


def rw(sql):
    """Run statements on RisingWave, one at a time, and return the output of the last one."""
    out = ""
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        out = sh(
            ["psql", "-h", "localhost", "-p", str(ARGS.port), "-d", "dev", "-U", "root", "-X", "-q",
             "-At", "-v", "ON_ERROR_STOP=1", "-c", stmt]
        )
    return out.strip()


def pg(sql):
    return sh(["psql", "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql]).strip()


def mysql(sql):
    return sh(
        ["docker", "exec", "-i", ARGS.mysql_container, "mysql", "-uroot",
         f"-p{ARGS.mysql_password}", "-N", "-B", ARGS.mysql_database, "-e", sql]
    ).strip()


def kafka_create(topic, partitions=2):
    sh(["docker", "exec", ARGS.kafka_container, "rpk", "topic", "create", topic, "-p",
        str(partitions)])


def kafka_produce(topic, rows, key=None):
    """Produce JSON rows, keyed by the column `key` if given."""
    if key is None:
        payload = "".join(json.dumps(r) + "\n" for r in rows)
        fmt = "%v\\n"
    else:
        payload = "".join(f"{json.dumps({key: r[key]})}\t{json.dumps(r)}\n" for r in rows)
        fmt = "%k\\t%v\\n"
    sh(["docker", "exec", "-i", ARGS.kafka_container, "rpk", "topic", "produce", topic, "-f", fmt],
       stdin=payload)


def eventually(what, fn, expected, timeout=90):
    deadline = time.time() + timeout
    last = None
    while True:
        try:
            last = fn()
        except CheckFailed as e:
            last = f"error: {e}"
        if last == expected:
            return
        if time.time() > deadline:
            raise CheckFailed(f"{what}: expected {expected!r}, got {last!r} after {timeout}s")
        time.sleep(0.5)


def meta_log_size():
    return sum(os.path.getsize(f) for f in glob.glob(f"{LOG_DIR}/meta-node-*.log"))


def meta_log_since(offset):
    text = ""
    for f in sorted(glob.glob(f"{LOG_DIR}/meta-node-*.log")):
        with open(f, errors="replace") as fh:
            text += fh.read()
    return text[offset:]


def idle(seconds=None):
    """Wait without any statement and return the largest number of sleeping fragments seen."""
    seconds = seconds or ARGS.idle
    offset = meta_log_size()
    time.sleep(seconds)
    sleeping = [int(m) for m in re.findall(r"selective barrier plan .*? sleeping=(\d+)",
                                           meta_log_since(offset))]
    return max(sleeping, default=None)


def fragments():
    return int(rw("select count(*) from rw_fragments"))


# --------------------------------------------------------------------------------------------
# Sources
# --------------------------------------------------------------------------------------------

def src_datagen_source():
    """Connector source (non shared) read by a materialized view."""
    rw("""
    set streaming_use_shared_source = false;
    create source m_dg (id int, v int) with (connector='datagen', fields.id.kind='sequence',
      fields.id.start='1', fields.id.end='3000', fields.v.kind='sequence', fields.v.start='1',
      fields.v.end='3000', datagen.rows.per.second='500') format plain encode json;
    create table m_dg_side (id int primary key, v int);
    """)
    rw("""set streaming_use_shared_source = false;
    create materialized view m_dg_mv as select count(*) c, sum(v) s from m_dg""")
    eventually("datagen rows", lambda: rw("select c, s from m_dg_mv"), f"3000|{3000 * 3001 // 2}")
    slept = idle()
    # A table without connector next to an endless source: it sleeps and still gets its data.
    rw("insert into m_dg_side values (1, 1), (2, 2)")
    eventually("side table", lambda: rw("select count(*) from m_dg_side"), "2", 20)
    return f"sleeping fragments while idle: {slept}"


def src_datagen_table():
    """Table with connector: connector source and DML in the same table."""
    rw("""
    create table m_dgt (id int, v int) with (connector='datagen', fields.id.kind='sequence',
      fields.id.start='1', fields.id.end='1000', fields.v.kind='sequence', fields.v.start='1',
      fields.v.end='1000', datagen.rows.per.second='500') format plain encode json;
    create materialized view m_dgt_mv as select count(*) c, sum(v) s from m_dgt;
    """)
    eventually("datagen table", lambda: rw("select c, s from m_dgt_mv"), f"1000|{500500}")
    slept = idle()
    rw("insert into m_dgt values (-1, 10), (-2, 20)")
    eventually("after insert", lambda: rw("select c, s from m_dgt_mv"), f"1002|{500530}", 20)
    idle()
    rw("delete from m_dgt where id <= 10 and id > 0")
    eventually("after delete", lambda: rw("select c, s from m_dgt_mv"), f"992|{500530 - 55}", 20)
    return f"sleeping fragments while idle: {slept}"


def src_posix_fs():
    """File source: list and fetch executors, as for s3, gcs and azblob."""
    root = tempfile.mkdtemp(prefix="sb_fs_src_")
    with open(f"{root}/f1.csv", "w") as f:
        f.write("a,b\n1,x\n2,y\n")
    with_clause = (f"with (connector='posix_fs', posix_fs.root='{root}', match_pattern='*.csv', "
                   "refresh.interval.sec='1') format plain encode csv "
                   "(without_header='false', delimiter=',')")
    rw(f"""
    create table m_fs_tab (a int, b varchar) {with_clause};
    create source m_fs_src (a int, b varchar) {with_clause};
    create materialized view m_fs_mv as select count(*) c, sum(a) s from m_fs_src;
    """)
    eventually("fs table", lambda: rw("select count(*), sum(a) from m_fs_tab"), "2|3", 120)
    eventually("fs source", lambda: rw("select c, s from m_fs_mv"), "2|3", 120)
    slept = idle()
    with open(f"{root}/f2.csv", "w") as f:
        f.write("a,b\n10,x\n20,y\n30,z\n")
    eventually("fs table, new file", lambda: rw("select count(*), sum(a) from m_fs_tab"), "5|63", 120)
    eventually("fs source, new file", lambda: rw("select c, s from m_fs_mv"), "5|63", 120)
    idle()
    rw("insert into m_fs_tab values (100, 'dml')")
    eventually("fs table, dml", lambda: rw("select count(*), sum(a) from m_fs_tab"), "6|163", 20)
    return f"sleeping fragments while idle: {slept}"


def src_webhook():
    """Webhook table: rows enter through the DML path of the table."""
    rw("create table m_wh (data jsonb) with (connector = 'webhook')")
    rw("create materialized view m_wh_mv as select count(*) c, sum((data->>'v')::int) s from m_wh")

    def post(v):
        req = urllib.request.Request(
            f"http://127.0.0.1:{ARGS.webhook_port}/webhook/dev/public/m_wh",
            data=json.dumps({"v": v}).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status != 200:
                raise CheckFailed(f"webhook status {resp.status}")

    for v in (1, 2, 3):
        post(v)
    eventually("webhook rows", lambda: rw("select c, s from m_wh_mv"), "3|6", 30)
    slept = idle()
    started = time.time()
    post(10)
    latency = time.time() - started
    post(20)
    eventually("webhook rows after idle", lambda: rw("select c, s from m_wh_mv"), "5|36", 30)
    return f"sleeping fragments while idle: {slept}, first request after idle: {latency:.1f}s"


def src_kafka():
    """Kafka: non shared source, shared source with backfill, table with connector, upsert."""
    broker = ARGS.kafka_broker
    kafka_create("m_k_plain")
    kafka_create("m_k_upsert")
    kafka_produce("m_k_plain", [{"id": i, "v": i} for i in range(1, 101)])
    opts = (f"with (connector='kafka', topic='m_k_plain', properties.bootstrap.server='{broker}', "
            "scan.startup.mode='earliest') format plain encode json")
    rw(f"""
    set streaming_use_shared_source = false;
    create source m_k_ns (id int, v int) {opts};
    """)
    rw("""set streaming_use_shared_source = false;
    create materialized view m_k_ns_mv as select count(*) c, sum(v) s from m_k_ns""")
    rw(f"""set streaming_use_shared_source = true;
    create source m_k_sh (id int, v int) {opts}""")
    rw("create materialized view m_k_sh_mv as select count(*) c, sum(v) s from m_k_sh")
    rw(f"create table m_k_tab (id int primary key, v int) {opts}")
    rw(f"""create table m_k_up (id int, v int, primary key (rw_key)) include key as rw_key
      with (connector='kafka', topic='m_k_upsert', properties.bootstrap.server='{broker}',
      scan.startup.mode='earliest') format upsert encode json""")
    rw("create table m_k_dim (id int primary key, label varchar)")
    rw("""create materialized view m_k_join as
      select count(*) c from m_k_tab t join m_k_dim d on d.id = t.id""")
    first = "100|5050"
    for name in ("m_k_ns_mv", "m_k_sh_mv"):
        eventually(name, lambda name=name: rw(f"select c, s from {name}"), first)
    eventually("kafka table", lambda: rw("select count(*), sum(v) from m_k_tab"), first)
    slept = idle()
    # A second materialized view on the shared source, created while everything is idle: its
    # source backfill starts from the beginning of the topic.
    rw("create materialized view m_k_sh_mv2 as select count(*) c, sum(v) s from m_k_sh")
    eventually("backfilled view", lambda: rw("select c, s from m_k_sh_mv2"), first)
    kafka_produce("m_k_plain", [{"id": i, "v": i} for i in range(101, 151)])
    second = f"150|{150 * 151 // 2}"
    for name in ("m_k_ns_mv", "m_k_sh_mv", "m_k_sh_mv2"):
        eventually(name + " after idle", lambda name=name: rw(f"select c, s from {name}"), second)
    eventually("kafka table after idle", lambda: rw("select count(*), sum(v) from m_k_tab"), second)
    idle()
    # The dimension table sleeps while the Kafka side is always awake.
    rw("insert into m_k_dim select i, 'x' from generate_series(1, 20) as g(i)")
    eventually("join with a sleeping table", lambda: rw("select c from m_k_join"), "20", 30)
    rw("insert into m_k_tab values (1000, 1)")
    rw("delete from m_k_tab where id <= 5")
    eventually("kafka table dml", lambda: rw("select count(*) from m_k_tab"), "146", 30)
    eventually("join after delete", lambda: rw("select c from m_k_join"), "15", 30)
    # Upsert topic: updates and deletes by key, in bursts.
    kafka_produce("m_k_upsert", [{"id": i, "v": i} for i in range(1, 11)], key="id")
    eventually("upsert", lambda: rw("select count(*), sum(v) from m_k_up"), "10|55")
    idle()
    kafka_produce("m_k_upsert", [{"id": i, "v": 100} for i in range(1, 4)], key="id")
    eventually("upsert after idle", lambda: rw("select count(*), sum(v) from m_k_up"), "10|349")
    return f"sleeping fragments while idle: {slept}"


def cdc_scenario(prefix, create_source, upstream, upstream_table):
    """Shared CDC source: three tables, changes on one of them while the others sleep, and
    transactions that span a sleeping table."""
    names = [f"{prefix}_a", f"{prefix}_b", f"{prefix}_c"]
    for name in names:
        upstream(f"drop table if exists {name}")
        upstream(f"create table {name} (id int primary key, v int)")
        upstream(f"insert into {name} values (1, 10), (2, 20)")
    rw(create_source)
    for name in names:
        rw(f"create table m_{name} (id int primary key, v int) from m_{prefix}_src "
           f"table '{upstream_table(name)}'")
    a, b, c = (f"m_{n}" for n in names)
    # Compares two tables in the same snapshot: a transaction must never be seen half applied.
    rw(f"""create materialized view m_{prefix}_cmp as
      select x.sa, y.sb from (select 1 as k, coalesce(sum(v), 0) as sa from {a}) x
      join (select 1 as k, coalesce(sum(v), 0) as sb from {b}) y on x.k = y.k""")
    for name in (a, b, c):
        eventually(name, lambda name=name: rw(f"select count(*), sum(v) from {name}"), "2|30", 120)
    slept = idle()
    upstream(f"insert into {names[0]} values (3, 30)")
    eventually("one table after idle", lambda: rw(f"select count(*), sum(v) from {a}"), "3|60", 60)
    idle()
    # Transfers between a and b in one transaction: sa + sb stays constant in every snapshot.
    total = "90"
    for i in range(20):
        upstream(f"begin; update {names[0]} set v = v - 1 where id = 1; "
                 f"update {names[1]} set v = v + 1 where id = 1; commit")
        if i % 5 == 4:
            time.sleep(2.5)
        seen = rw(f"select sa + sb from m_{prefix}_cmp")
        if seen != total:
            raise CheckFailed(f"transaction seen half applied: sa + sb = {seen}, expected {total}")
    eventually("transfers", lambda: rw(f"select sa, sb from m_{prefix}_cmp"), "40|50", 60)
    idle()
    upstream(f"delete from {names[2]} where id = 2")
    upstream(f"update {names[1]} set v = 0 where id = 2")
    eventually("delete after idle", lambda: rw(f"select count(*), sum(v) from {c}"), "1|10", 60)
    eventually("update after idle", lambda: rw(f"select sa, sb from m_{prefix}_cmp"), "40|30", 60)
    return f"sleeping fragments while idle: {slept}"


def src_postgres_cdc():
    """Shared Postgres CDC source, transactional as in production."""
    pg("select pg_terminate_backend(active_pid) from pg_replication_slots "
       "where slot_name = 'm_pgc_slot' and active_pid is not null")
    pg("select pg_drop_replication_slot(slot_name) from pg_replication_slots "
       "where slot_name = 'm_pgc_slot'")
    create = f"""create source m_pgc_src with (connector = 'postgres-cdc',
      hostname = '{os.environ["PGHOST"]}', port = '{os.environ.get("PGPORT", "5432")}',
      username = '{os.environ["PGUSER"]}', password = '{os.environ.get("PGPASSWORD", "")}',
      database.name = '{os.environ.get("PGDATABASE", "postgres")}', schema.name = 'public',
      slot.name = 'm_pgc_slot', transactional = 'true')"""
    return cdc_scenario("pgc", create, pg, lambda name: f"public.{name}")


def src_mysql_cdc():
    """Shared MySQL CDC source: same router, other table names and offsets."""
    create = f"""create source m_myc_src with (connector = 'mysql-cdc',
      hostname = '{ARGS.mysql_host}', port = '3306', username = 'root',
      password = '{ARGS.mysql_password}', database.name = '{ARGS.mysql_database}',
      server.id = '5701', transactional = 'true')"""
    return cdc_scenario("myc", create, mysql, lambda name: f"{ARGS.mysql_database}.{name}")


# --------------------------------------------------------------------------------------------
# Sinks
# --------------------------------------------------------------------------------------------

def sink_workload(table, check, with_deletes=True):
    """Bursts on `table (id int primary key, v int)` separated by idle periods. After each
    burst, `check(rows, ever)` is called with the expected content and with every id inserted so
    far (what an append-only target holds)."""
    rows = {}
    ever = set()
    slept = None
    for burst in range(4):
        base = burst * 100
        values = ", ".join(f"({base + i}, {base + i})" for i in range(1, 21))
        rw(f"insert into {table} values {values}")
        rows.update({base + i: base + i for i in range(1, 21)})
        ever.update(rows)
        if with_deletes and burst > 0:
            rw(f"update {table} set v = v + 1000 where id = {base - 99}")
            rows[base - 99] += 1000
            rw(f"delete from {table} where id = {base - 98}")
            del rows[base - 98]
        check(rows, ever)
        slept = idle()
    return slept


def digest(rows):
    return f"{len(rows)}|{sum(rows.values())}"


def pg_sink_options(table, extra):
    return (f"connector = 'postgres', host = '{os.environ['PGHOST']}', "
            f"port = '{os.environ.get('PGPORT', '5432')}', user = '{os.environ['PGUSER']}', "
            f"password = '{os.environ.get('PGPASSWORD', '')}', "
            f"database = '{os.environ.get('PGDATABASE', 'postgres')}', table = '{table}', {extra}")


def sink_postgres():
    """Native Postgres sink: upsert and append-only, with and without log store."""
    notes = []
    for decouple in ("true", "false"):
        t = f"m_pgs_{decouple[0]}"
        pg(f"drop table if exists {t}_up; create table {t}_up (id int primary key, v int)")
        pg(f"drop table if exists {t}_ap; create table {t}_ap (id int, v int)")
        rw(f"create table {t} (id int primary key, v int)")
        rw(f"set sink_decouple = {decouple}; create sink {t}_up_sink from {t} with ("
           + pg_sink_options(f"{t}_up", "type = 'upsert', primary_key = 'id'") + ")")
        rw(f"set sink_decouple = {decouple}; create sink {t}_ap_sink as select id, v from {t} "
           "with (" + pg_sink_options(
               f"{t}_ap", "type = 'append-only', force_append_only = 'true', "
               "force_compaction = 'true'") + ")")

        def check(rows, ever, t=t):
            eventually(f"{t} upsert", lambda: pg(f"select count(*), coalesce(sum(v), 0) from {t}_up"),
                       digest(rows))
            eventually(f"{t} append-only", lambda: pg(f"select count(distinct id) from {t}_ap"),
                       str(len(ever)))

        slept = sink_workload(t, check)
        notes.append(f"sink_decouple={decouple}: sleeping fragments while idle: {slept}")
    return "; ".join(notes)


def sink_postgres_group():
    """Postgres sinks of one connection (a sink group), one of them `sink_order = 'last'`, fed
    by a Postgres CDC table: the round trip used in production."""
    pg("select pg_terminate_backend(active_pid) from pg_replication_slots "
       "where slot_name = 'm_grp_slot' and active_pid is not null")
    pg("select pg_drop_replication_slot(slot_name) from pg_replication_slots "
       "where slot_name = 'm_grp_slot'")
    for name in ("m_grp_in", "m_grp_out", "m_grp_agg", "m_grp_last"):
        pg(f"drop table if exists {name}")
    pg("create table m_grp_in (id int primary key, v int)")
    pg("create table m_grp_out (id int primary key, v int)")
    pg("create table m_grp_agg (k int primary key, c bigint, s bigint)")
    pg("create table m_grp_last (k int primary key, c bigint, s bigint)")
    rw(f"""create source m_grp_src with (connector = 'postgres-cdc',
      hostname = '{os.environ["PGHOST"]}', port = '{os.environ.get("PGPORT", "5432")}',
      username = '{os.environ["PGUSER"]}', password = '{os.environ.get("PGPASSWORD", "")}',
      database.name = '{os.environ.get("PGDATABASE", "postgres")}', schema.name = 'public',
      slot.name = 'm_grp_slot', transactional = 'true')""")
    rw("create table m_grp_in (id int primary key, v int) from m_grp_src table 'public.m_grp_in'")
    rw("create materialized view m_grp_agg as select 1 as k, count(*) c, coalesce(sum(v), 0) s "
       "from m_grp_in")
    rw(f"""create connection m_grp_conn with (type = 'postgres', host = '{os.environ["PGHOST"]}',
      port = '{os.environ.get("PGPORT", "5432")}', user = '{os.environ["PGUSER"]}',
      password = '{os.environ.get("PGPASSWORD", "")}',
      database = '{os.environ.get("PGDATABASE", "postgres")}')""")
    rw("""create sink m_grp_out_sink from m_grp_in with (connector = 'postgres',
      connection = m_grp_conn, table = 'm_grp_out', type = 'upsert', primary_key = 'id')""")
    rw("""create sink m_grp_agg_sink from m_grp_agg with (connector = 'postgres',
      connection = m_grp_conn, table = 'm_grp_agg', type = 'upsert', primary_key = 'k')""")
    rw("""create sink m_grp_last_sink from m_grp_agg with (connector = 'postgres',
      connection = m_grp_conn, table = 'm_grp_last', type = 'upsert', primary_key = 'k',
      sink_order = 'last')""")
    rows = {}
    slept = None
    for burst in range(4):
        base = burst * 100
        pg("insert into m_grp_in select i, i from generate_series(%d, %d) as g(i)"
           % (base + 1, base + 20))
        rows.update({base + i: base + i for i in range(1, 21)})
        if burst > 0:
            pg(f"begin; update m_grp_in set v = v + 1000 where id = {base - 99}; "
               f"delete from m_grp_in where id = {base - 98}; commit")
            rows[base - 99] += 1000
            del rows[base - 98]
        want = digest(rows)
        eventually("round trip", lambda: pg("select count(*), coalesce(sum(v), 0) from m_grp_out"),
                   want)
        eventually("aggregate", lambda: pg("select c, s from m_grp_agg"), want)
        eventually("last sink", lambda: pg("select c, s from m_grp_last"), want)
        slept = idle()
    return f"sleeping fragments while idle: {slept}"


def sink_jdbc():
    """JDBC sink (Java connector node), upsert and append-only."""
    url = (f"jdbc:postgresql://{os.environ['PGHOST']}:{os.environ.get('PGPORT', '5432')}/"
           f"{os.environ.get('PGDATABASE', 'postgres')}?user={os.environ['PGUSER']}"
           f"&password={os.environ.get('PGPASSWORD', '')}")
    pg("drop table if exists m_jdbc_up; create table m_jdbc_up (id int primary key, v int)")
    pg("drop table if exists m_jdbc_ap; create table m_jdbc_ap (id int, v int)")
    rw("create table m_jdbc (id int primary key, v int)")
    rw(f"""create sink m_jdbc_up_sink from m_jdbc with (connector = 'jdbc', jdbc.url = '{url}',
      table.name = 'm_jdbc_up', type = 'upsert', primary_key = 'id')""")
    rw(f"""create sink m_jdbc_ap_sink from m_jdbc with (connector = 'jdbc', jdbc.url = '{url}',
      table.name = 'm_jdbc_ap', type = 'append-only', force_append_only = 'true')""")

    def check(rows, ever):
        eventually("jdbc upsert", lambda: pg("select count(*), coalesce(sum(v), 0) from m_jdbc_up"),
                   digest(rows))
        eventually("jdbc append-only", lambda: pg("select count(distinct id) from m_jdbc_ap"),
                   str(len(ever)))

    slept = sink_workload("m_jdbc", check)
    return f"sleeping fragments while idle: {slept}"


def sink_kafka():
    """Kafka sink, upsert and append-only, read back through Kafka tables."""
    broker = ARGS.kafka_broker
    kafka_create("m_ks_up", 1)
    kafka_create("m_ks_ap", 1)
    rw("create table m_ks (id int primary key, v int)")
    rw(f"""create sink m_ks_up_sink from m_ks with (connector = 'kafka', topic = 'm_ks_up',
      properties.bootstrap.server = '{broker}', primary_key = 'id') format upsert encode json""")
    rw(f"""create sink m_ks_ap_sink from m_ks with (connector = 'kafka', topic = 'm_ks_ap',
      properties.bootstrap.server = '{broker}')
      format plain encode json (force_append_only = 'true')""")
    rw(f"""create table m_ks_up_back (id int, v int, primary key (rw_key)) include key as rw_key
      with (connector = 'kafka', topic = 'm_ks_up', properties.bootstrap.server = '{broker}',
      scan.startup.mode = 'earliest') format upsert encode json""")
    rw(f"""create table m_ks_ap_back (id int, v int) with (connector = 'kafka',
      topic = 'm_ks_ap', properties.bootstrap.server = '{broker}', scan.startup.mode = 'earliest')
      format plain encode json""")

    def check(rows, ever):
        eventually("kafka upsert", lambda: rw("select count(*), coalesce(sum(v), 0) from m_ks_up_back"),
                   digest(rows))
        eventually("kafka append-only", lambda: rw("select count(distinct id) from m_ks_ap_back"),
                   str(len(ever)))

    slept = sink_workload("m_ks", check)
    return f"sleeping fragments while idle: {slept}"


def sink_file():
    """File sink (local directory, as s3, gcs, azblob, webhdfs and snowflake). The files are
    read directly: a file source could list a file of a local directory while it is written."""
    root = tempfile.mkdtemp(prefix="sb_fs_sink_")
    rw("create table m_fsk (id int primary key, v int)")
    rw(f"""create sink m_fsk_sink from m_fsk with (connector = 'fs', fs.path = '{root}',
      type = 'append-only', force_append_only = 'true', rollover_seconds = '1')
      format plain encode json (force_append_only = 'true')""")

    def written_ids():
        ids = set()
        for path in glob.glob(f"{root}/**/*.json", recursive=True):
            with open(path, errors="replace") as fh:
                for line in fh:
                    try:
                        ids.add(json.loads(line)["id"])
                    except (ValueError, KeyError):
                        pass  # a line being written
        return len(ids)

    def check(rows, ever):
        eventually("file sink", written_ids, len(ever), 60)

    slept = sink_workload("m_fsk", check, with_deletes=False)
    shutil.rmtree(root, ignore_errors=True)
    return f"sleeping fragments while idle: {slept}"


def sink_blackhole():
    """Blackhole sinks with and without log store, on a table and on a join."""
    rw("create table m_bh (id int primary key, v int)")
    rw("create table m_bh2 (id int primary key, v int)")
    rw("set sink_decouple = true; create sink m_bh_d from m_bh with (connector = 'blackhole')")
    rw("set sink_decouple = false; create sink m_bh_n from m_bh with (connector = 'blackhole')")
    rw("""set sink_decouple = true; create sink m_bh_j as
      select a.id, a.v, b.v as bv from m_bh a join m_bh2 b on a.id = b.id
      with (connector = 'blackhole')""")
    rw("create materialized view m_bh_mv as select count(*) c from m_bh a join m_bh2 b on a.id = b.id")
    slept = None
    for burst in range(3):
        rw(f"insert into m_bh select i, i from generate_series({burst * 10 + 1}, {burst * 10 + 10}) as g(i)")
        rw(f"insert into m_bh2 select i, i from generate_series({burst * 10 + 1}, {burst * 10 + 5}) as g(i)")
        eventually("join next to the sinks", lambda: rw("select c from m_bh_mv"), str(burst * 5 + 5), 30)
        slept = idle()
    return f"sleeping fragments while idle: {slept}"


def sink_into_table():
    """Sink into a table: the target table and its descendants stay awake with the sink, the
    rest of the database sleeps."""
    rw("create table m_sit_src (id int primary key, v int)")
    rw("create table m_sit_dst (id int primary key, v int)")
    rw("create materialized view m_sit_mv as select count(*) c, coalesce(sum(v), 0) s from m_sit_dst")
    rw("create table m_sit_other (id int primary key, v int)")
    rw("create materialized view m_sit_other_mv as select count(*) c from m_sit_other")
    before = idle()
    rw("create sink m_sit_sink into m_sit_dst as select id, v from m_sit_src")

    def check(rows, ever):
        eventually("sink into table", lambda: rw("select c, s from m_sit_mv"), digest(rows), 30)

    slept = sink_workload("m_sit_src", check)
    rw("insert into m_sit_dst values (-1, 5)")
    eventually("dml on the target", lambda: rw("select c from m_sit_mv"), "78", 30)
    rw("insert into m_sit_other values (1, 1)")
    eventually("another table", lambda: rw("select c from m_sit_other_mv"), "1", 30)
    # The target table (2 fragments, one of them its DML), its view and the sink stay awake.
    # Without selective barriers nothing sleeps and the log has no plan: nothing to compare.
    if before and (slept is None or slept < before - 4 or slept == 0):
        raise CheckFailed(f"sleeping fragments: {before} before the sink, {slept} with it")
    rw("drop sink m_sit_sink")
    after_drop = idle()
    rw("insert into m_sit_dst values (-2, 5)")
    eventually("dml after drop", lambda: rw("select c from m_sit_mv"), "79", 30)
    rw("create sink m_sit_sink2 into m_sit_dst as select id + 10000 as id, v from m_sit_src")
    eventually("second sink", lambda: rw("select c from m_sit_mv"), "156", 60)
    idle()
    rw("insert into m_sit_src values (5000, 1)")
    eventually("second sink after idle", lambda: rw("select c from m_sit_mv"), "157", 30)
    return (f"sleeping fragments before the sink: {before}, while it exists: {slept}, "
            f"after it is dropped: {after_drop}")


def sink_subscription():
    """Subscription: the change log of a table that sleeps between two bursts."""
    rw("create table m_sub (id int primary key, v int)")
    rw("create subscription m_sub_s from m_sub with (retention = '1D')")
    # `since begin()` starts at the creation time of the subscription, which is taken after the
    # barrier that creates it: a row written in the epoch opened by that barrier is before it.
    # Start a new epoch first (with or without selective barriers).
    rw("flush")
    rw("insert into m_sub values (1, 1), (2, 2)")
    rw("flush")
    slept = idle()
    rw("insert into m_sub values (3, 3)")
    rw("flush")
    idle()
    rw("update m_sub set v = 30 where id = 3")
    rw("delete from m_sub where id = 1")
    rw("flush")
    out = sh(["psql", "-h", "localhost", "-p", str(ARGS.port), "-d", "dev", "-U", "root", "-X", "-q",
              "-At", "-v", "ON_ERROR_STOP=1"],
             stdin="declare m_cur subscription cursor for m_sub_s since begin();\n"
                   "fetch 20 from m_cur;\n")
    ops = [line.split("|")[2] for line in out.strip().splitlines() if line.count("|") >= 3]
    want = ["Insert", "Insert", "Insert", "UpdateDelete", "UpdateInsert", "Delete"]
    if sorted(ops) != sorted(want):
        raise CheckFailed(f"subscription changes: expected {want}, got {ops} ({out.strip()[:300]})")
    return f"sleeping fragments while idle: {slept}"


SCENARIOS = [
    ("source: datagen source", src_datagen_source, None),
    ("source: table with connector (datagen) + DML", src_datagen_table, None),
    ("source: file source (posix_fs)", src_posix_fs, None),
    ("source: webhook table", src_webhook, None),
    ("source: kafka (plain, shared + backfill, table, upsert)", src_kafka, "kafka"),
    ("source: postgres-cdc shared, transactional", src_postgres_cdc, "pg"),
    ("source: mysql-cdc shared, transactional", src_mysql_cdc, "mysql"),
    ("sink: postgres (upsert, append-only, log store or not)", sink_postgres, "pg"),
    ("sink: postgres sink group + sink_order last, from CDC", sink_postgres_group, "pg"),
    ("sink: jdbc (upsert, append-only)", sink_jdbc, "pg"),
    ("sink: kafka (upsert, append-only)", sink_kafka, "kafka"),
    ("sink: file sink (fs)", sink_file, None),
    ("sink: blackhole (log store or not, join)", sink_blackhole, None),
    ("sink: sink into table", sink_into_table, None),
    ("sink: subscription", sink_subscription, None),
]


def log_problems():
    problems = []
    for f in glob.glob(f"{LOG_DIR}/*.log"):
        with open(f, errors="replace") as fh:
            text = fh.read()
        for pattern in ("panicked at", "database entering recovery",
                        "sync epoch exceeds latest epoch"):
            count = text.count(pattern)
            if count:
                problems.append(f"{os.path.basename(f)}: {count} x '{pattern}'")
    return problems


def main():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4566)
    parser.add_argument("--webhook-port", type=int, default=4560)
    parser.add_argument("--idle", type=float, default=5.0)
    parser.add_argument("--kafka-broker")
    parser.add_argument("--kafka-container")
    parser.add_argument("--mysql-host")
    parser.add_argument("--mysql-container")
    parser.add_argument("--mysql-password", default="")
    parser.add_argument("--mysql-database", default="sbdb")
    parser.add_argument("--only", help="substring of the scenario names to run")
    ARGS = parser.parse_args()
    available = {
        "pg": bool(os.environ.get("PGHOST")),
        "kafka": bool(ARGS.kafka_broker and ARGS.kafka_container),
        "mysql": bool(ARGS.mysql_host and ARGS.mysql_container),
    }
    results = []
    for name, fn, needs in SCENARIOS:
        if ARGS.only and ARGS.only not in name:
            continue
        if needs and not available[needs]:
            results.append((name, "SKIPPED", f"no {needs}"))
            continue
        started = time.time()
        try:
            note = fn() or ""
            status = "OK"
        except Exception as e:  # noqa: BLE001
            status = "FAILED"
            note = str(e)[:700]
            if not isinstance(e, CheckFailed):
                note += " | " + traceback.format_exc()[-400:]
        results.append((name, status, f"{note} ({time.time() - started:.0f}s)"))
        print(f"[{status}] {name}: {results[-1][2]}", flush=True)
    problems = log_problems()
    print(f"fragments at the end: {fragments()}")
    print("log problems:", problems or "none")
    failed = [r for r in results if r[1] == "FAILED"]
    print(f"{len([r for r in results if r[1] == 'OK'])} ok, {len(failed)} failed, "
          f"{len([r for r in results if r[1] == 'SKIPPED'])} skipped")
    sys.exit(1 if failed or problems else 0)


if __name__ == "__main__":
    main()
