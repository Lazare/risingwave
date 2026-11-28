# Batch memory accounting fixes and spill on memory wait

| | |
|---|---|
| Kind | engine (batch) |
| Branch | `proto/plan-reduction-batch-mem-safety` |
| Status | prototype in rc8 |
| Variables | `batch_spill_on_memory_wait` (false) |
| Design note | [`docs/dev/src/design/batch-memory-safety.md`](../../dev/src/design/batch-memory-safety.md), sections 2 and 3 |
| Related | [batch resource governance](batch-resource-governance.md), [exchange buffer accounting](exchange-buffer-accounting.md) |

## Summary

- Accounting: places where batch memory escaped or was miscounted by the memory contexts of the
  [resource governance](batch-resource-governance.md) are fixed (allocations refused by a context but
  then deallocated, monitored heaps, the buffer of a sort that spilled, the heaps of the group top-n
  reserved up front, DataFusion reservations beyond the limit, the first chunk of each input of the
  merge sort).
- Revocable memory (`batch_spill_on_memory_wait`): an operator that cannot spill and must wait for
  memory asks the spillable operators of its own query to spill, instead of waiting only for other
  queries.

## Motivation

The budget of a node only knows reservations and the memory of non-spillable operators beyond their
pool; a query could fail while its own hash aggregations held gigabytes they could write to disk,
and under-counting let the process hold several times what the budget showed.

## What it does

- `MemoryContext::force_add`: an allocation that a context refused is still counted when it happens,
  so its deallocation does not drive counters negative.
- A sort releases the count of buffered chunks once they are on disk; group top-n heaps grow with
  their rows instead of reserving `min(limit + offset, 1024)` rows per group.
- Spill gate: the spillable operators of a query charge what they use beyond their reserved minimum
  to a gate context; a waiting non-spillable operator lowers the gate's limit, the next spillable
  operator that asks for more is refused and spills, and the waiting operator takes the memory back
  from the pool (checked every second). A grace period of 60 s keeps such a waiter from counting as
  stuck.

Synthetic example: a query with a large hash join build and a group top-n on the same node. Without
the variable, when the node has no unreserved memory left, the top-n waits for other queries and
fails when every query waits; with it, the hash join spills its build side and the top-n proceeds.

## Configuration

`batch_spill_on_memory_wait`, default false. `overflow_memory_wait_secs` (see
[resource governance](batch-resource-governance.md)) still bounds the whole wait.

## Correctness / safety argument

A spill request never blocks: spillable operators spill when refused, they never wait. The only
spillable operator that waits (a hash join holding rows sharing one key) behaves as a non-spillable
one. Reserved minimums are never refused. Results do not change, only when operators spill.

## Status and versions

New in rc8.

## Tests

`e2e_test/batch/spill_on_memory_wait.slt`; unit tests of the memory contexts, the spill gate and the
fixed operators.

## Limits and risks

More disk I/O under pressure; while one operator waits, the cap applies to every spillable operator
of the query on the node. Only operators that are still growing (consuming their input) react.
Memory that escapes the accounting entirely (exchange buffers, storage iterators, prefetch on decoded
sizes) is listed as open in the design note; exchange buffers are addressed by the
[exchange buffer accounting](exchange-buffer-accounting.md) candidate. Spilling task outputs and
phased execution are design only.

## Upstream relation

The accounting fixes of upstream structures (`force_add`, monitored heap, sort, merge sort) are
upstream PR candidates; the spill gate depends on the fork's governance.
