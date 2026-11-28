# Resource governance of batch queries

| | |
|---|---|
| Kind | engine (batch) |
| Branch | `fork/batch-resource-governance`; follow-ups on `fix/batch-overflow-wait-limit` (plan-reduction line only) |
| Status | integrated (`integration/v3.1.2`); the follow-ups are in the plan-reduction images from rc4 |
| Configuration | `[batch]` `memory_usable_ratio` (0.9), `query_memory_ratio` (0.3), `query_memory_max_ratio` (0.7), `spillable_operator_min_memory_mb` (8), `spillable_operator_avg_memory_mb` (64), `overflow_memory_wait_secs` (300, follow-up); `[storage]` `batch_read_stale_snapshot_from_meta` (false) |
| Related | [batch memory safety and spill on memory wait](batch-memory-safety.md), [exchange buffer accounting](exchange-buffer-accounting.md) |

## Summary

A heavy batch query should not destabilize the node it runs on. The branch adds memory limits per
query and per spillable operator, admission of queries on the compute nodes before they start, a
lower parallelism for queries whose operators cannot all get their minimum memory, a budget of
spill files checked against the open file limit, and fail-fast behavior for queries whose snapshot
has left the recent versions.

## Motivation

Upstream, batch operators share the batch memory of a node without a per-query bound: one query
with several large hash aggregations or joins can take all of it, other queries then fail or the
process runs out of memory. Concurrent heavy queries (for example many data quality tests over
non-materialized views, run in parallel) all start at once, each pins its own snapshot, and a query
whose snapshot is no longer cached on the compute node reads old versions from the meta node key by
key, which can take hours.

## What it does

- `src/batch/src/task/query_memory.rs`: all the tasks of a query on a node share one memory
  context bounded to `query_memory_ratio` of the usable batch memory. Inside it, the spillable
  operators (hash aggregation, hash join, sort) share a pool: each has
  `spillable_operator_min_memory_mb` reserved, the rest is first come first served, and an operator
  spills when the pool refuses it. Operators that cannot spill (top-n, group top-n, lookup join,
  nested loop join) may take memory beyond the pool from the unreserved memory of the node.
- `src/frontend/src/scheduler/batch_limits.rs`: the frontend learns the limits of the compute nodes
  and lowers the parallelism of a plan until the reserved memory of its spillable operators fits;
  without parallelism a query may use up to `query_memory_max_ratio`, beyond that it is rejected.
- Admission (`AdmitQuery` RPC): a query is admitted on the compute nodes before its snapshot is
  pinned; queries wait in a queue when memory is short.
- The hash join counts the rows that share one key (which it cannot split) against the unreserved
  memory of the node.
- Spill files are bounded by a budget derived from the open file limit.
- The snapshot of a query is taken after admission and the versions it reads are pinned for the
  whole query; with `batch_read_stale_snapshot_from_meta = false` (default), a query whose snapshot
  is no longer among the recent versions fails fast with an explicit error.
- The stateful operators of the root stage run on a compute node.
- Waits for memory that last are logged; peaks are recorded per query.

Follow-ups on `fix/batch-overflow-wait-limit` (merged into the plan-reduction line, not into
`fork/batch-resource-governance` yet):

- `overflow_memory_wait_secs` (default 300): an operator waiting for memory beyond its query gives
  up after that time, so that queries do not wait forever for a query that is itself stuck; 0 waits
  without limit;
- a cancelled or timed-out task releases its memory reservation and ends even when nobody reads its
  output.

## Configuration

See the table above. All are node configuration entries (`risingwave.toml`), not session
variables.

## Correctness / safety argument

The limits change when a query runs, spills, waits or fails, never what it returns. The admission
reservation is released when the last task of the query ends (or is cancelled, follow-up).

## Status and versions

Integrated in `integration/v3.1.2`. The follow-ups are in the plan-reduction images since rc4
(rc4 was rebuilt under the same tag after a regression of the cancellation follow-up was fixed).

## Tests

Unit tests in `query_memory.rs`, `batch_limits.rs`, `spill_op.rs`, `mem_context.rs`, the hash join
and the task manager; storage test of the stale snapshot error
(`src/storage/hummock_test/src/hummock_storage_tests.rs`); for the follow-ups,
`test_cancel_task_blocked_on_a_full_output` and `test_output_taken_after_the_task_ended`.

## Limits and risks

- Memory that escapes the accounting (exchange buffers, storage iterators and prefetch) is not
  bounded by these limits; see [batch memory safety](batch-memory-safety.md).
- A too low `query_memory_ratio` makes queries spill or run with less parallelism; the defaults
  were changed during development and are not tuned for every workload.
- A queued query waits; clients with short timeouts see timeouts instead of out-of-memory errors.

## Upstream relation

Upstream has a batch memory limit and spilling operators, but no per-query admission or
parallelism fitting. The pieces are separable; the stale snapshot fail-fast is the smallest
candidate for an upstream PR. Not proposed yet.
