# Spilled aggregation states reloaded beyond the memory limit

- **Kind**: upstream bug (batch executor `HashAggExecutor`), memory use far beyond the batch
  memory limit, which can bring a compute node down.
- **Affected versions**: upstream `v3.1.2`; fixed in the fork builds of integration `v3.1.2`
  from the first one (commit of 2026-11-24).

## Symptom

After a batch hash aggregation spilled, a sub-executor loaded back the aggregation states of each
partition. When they did not fit in the memory limit it only logged "not enough memory to load
one partition agg state after spill ... so keep going" and went on: the memory context refuses to
count what exceeds its limit, so these states were held without being counted, and the real
memory of the node could grow far beyond the batch limit (a large `GROUP BY` with many distinct
groups under a low memory limit).

## Cause

No second spill when the reloaded states do not fit, and memory released from the context that
it had refused to count.

## Fix

Loading stops at the first chunk of states that does not fit, and what is loaded, the states left
in the partition and the input are spilled again with a new hasher, as already done when the
input does not fit; partitions smaller than `SPILL_AT_LEAST_MEMORY` are still loaded without
check, which ends the recursion. Memory that the context refused to count is not released. Part of
commit `c1480bc5d9` (squashed "fix(batch): spill again the agg states that do not fit when loaded
back") on branch `fork/fix-batch-hash-agg-spill`.

## Tests

Unit test `test_spill_hash_agg_respill_when_loading_states` in
`src/batch/executors/src/executor/hash_agg.rs` (does not spill a second time without the fix).

## Upstream status

Not checked against `main`; the open pull request
<https://github.com/risingwavelabs/risingwave/pull/27863> reworks the spill.
