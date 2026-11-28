# Distributed `UNION ALL` (batch)

| | |
|---|---|
| Kind | optimizer rule (batch, distribution) |
| Branch | `proto/plan-reduction-batch-union-dist` |
| Status | prototype in rc8 |
| Variables | `batch_distributed_union` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "BU" |

## Summary

`BatchUnion::to_distributed` gathered every input into one task. With the variable, all inputs are
brought to one target distribution chosen from what the operator above requires (the hash of its
group or join key), or the hash of all columns, and the union runs in parallel in the stage of its
parent.

## Motivation

A hash aggregation or join over a union read all the rows of all branches through one task, twice
over the network (gather, then hash again). Views built as unions of branches and queried in batch
are therefore bottlenecked on a single task and a single node's memory.

## What it does

```sql
CREATE TABLE a (id int PRIMARY KEY, k int, v int);
CREATE TABLE c (k int, v int);
SELECT k, count(*) FROM (SELECT k, v FROM a UNION ALL SELECT k, v FROM c) u GROUP BY k;
-- before: HashAgg <- Exchange(Hash k) <- Union <- [Exchange(Single) <- Scan(a), Exchange(Single) <- Scan(c)]
-- after:  HashAgg <- Union <- [Exchange(Hash k) <- Scan(a), Exchange(Hash k) <- Scan(c)]
```

An input already distributed as the target stays in the union's stage without exchange only if that
part of its plan holds no scan, source or lookup join (which decide the tasks of a stage) and nothing
single-distributed. A required single distribution (root, sort, insert, nested loop join) keeps the
single union.

## Configuration

`batch_distributed_union`, default false. Local execution is unchanged.

## Correctness / safety argument

A `UNION ALL` concatenates its inputs; each row goes through exactly one exchange partition to one
task, so the union of the tasks is the same multiset. `HashShard(k)` is the same function in every
input, so rows with equal `k` from every branch meet in the same task. Upstream forced `Single`
because there is no round-robin exchange and the stage fragmenter assumes one scan per stage; the
rule respects both constraints. See the design note.

## Status and versions

New in rc8.

## Tests

Planner `batch_distributed_union.yaml`; differential `e2e_test/batch/distributed_union.slt` (29
queries, local and distributed query modes).

Validation set (planner-level counts): gathers of a whole input into one task (`Exchange Single`)
drop by about 44% from this rule alone (rc7 to rc8), about 63% with all rc8 rules.

## Limits and risks

Without a requirement, rows are hashed on all columns instead of gathered: same network volume,
spread over all tasks. A `count(*)` over a union pruned to no columns keeps the single union. Not
done: a round-robin exchange, partial aggregation pushed into branches.

## Upstream relation

Addresses a `TODO` of upstream (`BatchUnion::to_distributed`). Good upstream PR candidate.
