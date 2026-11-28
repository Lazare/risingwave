# Batch: cross joins and joins on a constant copied to every task

| | |
|---|---|
| Kind | optimizer rule and distribution (batch); performance defect of upstream plans, not a wrong result |
| Branch | `proto/plan-reduction-contradictions` |
| Status | prototype, candidate |
| Variables | `batch_broadcast_cross_join` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "BC" |

## Summary

- A join whose only equality between its sides compares the same constant on both sides
  (`a.one = b.one` with `1 AS one` projected on both sides) loses that equality: it is `TRUE` for
  every pair.
- A batch nested loop join (a cross join, or a join without equality) of type inner, right outer,
  right semi or right anti copies its left side to every task of its right side through a
  broadcast exchange, instead of gathering both sides into one task. The smaller side of an inner
  cross join is put on the left when the estimates say which one it is.

## Motivation

A streaming job can not run a cross join, so views that are also materialized write
`JOIN small ON big.one = small.one` with a constant `1 AS one` on both sides. Upstream plans it in
batch as a hash join on the constant:

```text
BatchHashJoin { type: Inner, predicate: 1:Int32 = 1:Int32 }
├─BatchExchange { dist: HashShard(1:Int32) }    every row of the large side to one task
└─BatchExchange { dist: HashShard(1:Int32) }
```

Every row of both sides is hashed to the same task: a single-task join hidden in a distributed
plan. An honest cross join is no better upstream: `BatchNestedLoopJoin` requires both inputs
`Single`. The batch planner has no broadcast exchange (`Distribution::Broadcast` was never produced
in batch plans, and its exchange information carried no output count), although the batch task
already has a broadcast output channel.

## What it does

```sql
CREATE TABLE t (id int PRIMARY KEY, v int);
CREATE TABLE c (id varchar PRIMARY KEY);
SELECT a.id, b.id FROM (SELECT id, 1 AS one FROM t) a JOIN (SELECT id, 1 AS one FROM c) b ON a.one = b.one;
-- before: HashJoin(1 = 1) <- [Exchange(HashShard(1)) <- Scan(t), Exchange(HashShard(1)) <- Scan(c)]
-- after:  NestedLoopJoin(true) <- [Exchange(Broadcast) <- Scan(c), Scan(t)]   (c estimated smaller)
```

- `ConstantKeyJoinRule` (last logical stage of batch queries): removes the equalities `l = r`
  between the sides whose operands hold the same non-`NULL` constant in every row (column facts,
  simple types), for any join type; puts the smaller estimated side of an inner join without
  equality on the left.
- `BatchNestedLoopJoin::to_distributed`: for inner and right outer / semi / anti joins, when the
  right side is not `Single`, the left side is required `Broadcast` and the join is distributed
  `SomeShard`. The executor buffers its left side, so each task joins every left row with its share
  of the right rows; these join types output nothing for a left row on its own, so the copies of a
  left row do not duplicate the output.
- `Distribution::to_prost`: a broadcast exchange tells its producers the number of consumer tasks
  (`BroadcastInfo { count }`), as a hash exchange does.

Local execution is unchanged (both inputs `Single`).

## Not done

- Left outer, left semi, left anti and full outer nested loop joins: the copies of a left row
  would each be output (padded, or tested for a match) once per task.
- A hash join that broadcasts its build side when it is small (a classic broadcast hash join): the
  constant-key case is turned into a nested loop join instead, which needs no key.
- Streaming: a streaming job has no nested loop join; the join on a constant stays a hash join on
  one vnode there.

## Risks

The broadcast exchange of batch plans is new to the batch query scheduler (the channel existed): the
differential test runs every query in the distributed mode. A large left side without estimates
is copied to every task (memory per task as before, network times the number of tasks).

## Tests

Planner test `batch_broadcast_cross_join.yaml`; differential test
`e2e_test/batch/broadcast_cross_join` (distributed mode, inner, right, semi, anti, aggregation
above, empty sides).
