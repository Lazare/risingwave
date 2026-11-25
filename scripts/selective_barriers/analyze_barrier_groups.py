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

"""Estimate how a streaming graph could be split into independent barrier groups.

Merge executors must receive the same barrier from every upstream actor, so the
smallest unit that can skip a barrier is a weakly connected component of the
fragment graph (dispatcher edges). This script computes those components from
the catalog of a running cluster and reports, for each one, its jobs, actors,
state tables and "drivers" (sources, DML tables, now()).

If one component holds almost all actors and state tables, selective barrier
injection cannot help that deployment.

Usage:
    analyze_barrier_groups.py [--host H] [--port P] [--db D] [--user U] [--json]

Only `psql` is required (no Python dependencies).
"""

import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict

NOW_RE = re.compile(r"\b(now\s*\(\s*\)|current_timestamp|proctime\s*\(\s*\))", re.I)


def query(args, sql):
    """Run a query with psql and return rows as lists of strings."""
    cmd = [
        "psql", "-h", args.host, "-p", str(args.port), "-d", args.db, "-U", args.user,
        "-X", "-A", "-t", "-F", "\x1f", "-R", "\x1e", "-c", sql,
    ]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    return [rec.split("\x1f") for rec in out.strip("\n").split("\x1e") if rec.strip()]


def parse_int_array(s):
    s = s.strip("{}")
    return [int(x) for x in s.split(",") if x]


def parse_text_array(s):
    s = s.strip("{}")
    return [x for x in s.split(",") if x]


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", default=4566, type=int)
    p.add_argument("--db", default="dev")
    p.add_argument("--user", default="root")
    p.add_argument("--json", action="store_true", help="print the full result as JSON")
    args = p.parse_args()

    fragments = query(args, "select fragment_id, table_id, upstream_fragment_ids, flags, state_table_ids from rw_fragments")
    actors = query(args, "select fragment_id, count(*) from rw_actors group by fragment_id")
    relations = query(args, "select id, name, relation_type, coalesce(definition, '') from rw_relations")

    rel = {int(r[0]): {"name": r[1], "type": r[2], "definition": r[3]} for r in relations}
    actor_count = {int(f): int(c) for f, c in actors}

    uf = UnionFind()
    frag_info = {}
    for fid, job, ups, flags, sts in fragments:
        fid, job = int(fid), int(job)
        frag_info[fid] = {
            "job": job,
            "flags": parse_text_array(flags),
            "state_tables": len(parse_int_array(sts)),
            "actors": actor_count.get(fid, 0),
        }
        uf.find(fid)
        for up in parse_int_array(ups):
            uf.union(fid, up)

    comps = defaultdict(lambda: {"fragments": 0, "actors": 0, "state_tables": 0, "jobs": set(), "drivers": set()})
    for fid, info in frag_info.items():
        c = comps[uf.find(fid)]
        c["fragments"] += 1
        c["actors"] += info["actors"]
        c["state_tables"] += info["state_tables"]
        c["jobs"].add(info["job"])
        job = rel.get(info["job"], {"name": f"job#{info['job']}", "type": "?", "definition": ""})
        if "SOURCE" in info["flags"] or "SOURCE_SCAN" in info["flags"] or "FS_FETCH" in info["flags"]:
            c["drivers"].add(f"source:{job['name']}")
        if "DML" in info["flags"]:
            c["drivers"].add(f"dml:{job['name']}")
        if "NOW" in info["flags"] or NOW_RE.search(job["definition"] or ""):
            c["drivers"].add(f"now:{job['name']}")

    result = []
    for root, c in comps.items():
        result.append({
            "fragments": c["fragments"],
            "actors": c["actors"],
            "state_tables": c["state_tables"],
            "jobs": sorted(rel.get(j, {"name": f"job#{j}"})["name"] for j in c["jobs"]),
            "drivers": sorted(c["drivers"]),
        })
    result.sort(key=lambda r: (-r["actors"], -r["state_tables"]))

    tot_actors = sum(r["actors"] for r in result) or 1
    tot_tables = sum(r["state_tables"] for r in result) or 1

    if args.json:
        json.dump({"components": result, "total_actors": tot_actors, "total_state_tables": tot_tables}, sys.stdout, indent=2)
        print()
        return

    print(f"{len(result)} independent barrier groups, {tot_actors} actors, {tot_tables} state tables\n")
    print(f"{'#':>3} {'jobs':>5} {'frags':>5} {'actors':>7} {'%act':>6} {'tables':>7} {'%tbl':>6}  drivers / jobs")
    for i, r in enumerate(result, 1):
        drivers = ", ".join(r["drivers"]) or "-"
        jobs = ", ".join(r["jobs"][:5]) + (" ..." if len(r["jobs"]) > 5 else "")
        print(f"{i:>3} {len(r['jobs']):>5} {r['fragments']:>5} {r['actors']:>7} {100 * r['actors'] / tot_actors:>5.1f}% "
              f"{r['state_tables']:>7} {100 * r['state_tables'] / tot_tables:>5.1f}%  {drivers}  [{jobs}]")

    biggest = result[0] if result else None
    if biggest:
        print(f"\nLargest group: {100 * biggest['actors'] / tot_actors:.1f}% of actors, "
              f"{100 * biggest['state_tables'] / tot_tables:.1f}% of state tables.")
        now_groups = [r for r in result if any(d.startswith("now:") for d in r["drivers"])]
        print(f"Groups driven by now(): {len(now_groups)} "
              f"({sum(r['actors'] for r in now_groups)} actors). These need barriers at the now() granularity.")


if __name__ == "__main__":
    main()
