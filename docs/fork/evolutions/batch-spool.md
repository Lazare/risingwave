# Shared sub-plan spool (batch)

| | |
|---|---|
| Kind | optimizer rule and executor (batch) |
| Branch | `proto/plan-reduction-batch-spool` |
| Status | candidate (not in rc8) |
| Variables | `batch_share_identical_subplans` (false) |
| Design note | `streaming-plan-reduction.md`, section "BS", on the branch |

## Summary

The final batch plan replaces the identical, deterministic copies of a sub-plan that aggregates,
joins, sorts or keeps a top-n within one stage by a producer (`BatchSpool`) and leaf readers
(`BatchSpoolRead`). The task computes the sub-plan once, keeps its rows (spilled to disk when the
memory of the query is short) and replays them for each copy. The exchanges and stages below the
readers disappear.

## Motivation

The batch optimizer turns the plan into a tree first: a view or CTE read twice is planned and
executed twice, its scans and aggregation memory paid each time.

## What it does

```sql
CREATE TABLE t (k int, v int, ts int);
CREATE VIEW latest AS SELECT k, last_value(v ORDER BY ts) AS v, count(*) AS n FROM t GROUP BY k;
SELECT a.k, a.v, b.n FROM latest a JOIN latest b ON a.k = b.k;
-- HashJoin(k) <- BatchSpool { spool: 1 } <- HashAgg(k) <- Exchange(hash k) <- Scan(t)
--             <- BatchSpoolRead { spool: 1 }
```

Three designs were weighed in the design note; this is the spool inside a task (no change to stages,
runners or task outputs). The first node read drives a shared materialization future; no lock is held
while it runs, so a node left aside by its parent cannot block the others.

## Configuration

`batch_share_identical_subplans`, default false.

## Correctness / safety argument

A reader gets exactly the rows its copy would have computed in the same task. Where a sub-plan may
return different rows each time (ties of a limit, `array_agg` without `ORDER BY`), every copy now
gets the same one of the possible results. Impure expressions, sources, DML and system tables keep
their copies; copies below a `LIMIT` are not shared unless their root computes all rows first anyway.
No wait cycle exists inside a task (a reader is a leaf, a nested spool is strictly smaller).

## Status and versions

Candidate: branch based on the rc8 line, not merged into any image. Rarely applicable on the
validation set described in the README.

## Tests

Planner `batch_share_identical_subplans.yaml`; unit tests in `spool.rs` and `spool_registry.rs`;
differential `e2e_test/batch/share_identical_subplans.slt` in local and distributed modes.

## Limits and risks

Copies in different stages are not shared (in distributed mode, union branches and joins on different
keys put copies in different stages). Copies that differ by their columns or by a pushed filter are
not shared. The consumer of the first node waits for the whole sub-plan.

## Upstream relation

No equivalent in upstream batch. Upstream interest not assessed.
