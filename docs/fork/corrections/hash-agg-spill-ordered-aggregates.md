# Spill of a batch hash aggregation: error with ordered aggregates, groups emitted twice

- **Kind**: upstream bug (batch executor `HashAggExecutor`): an error on a valid query, and wrong
  results.
- **Affected versions**: upstream `v3.1.2`; fixed in the fork builds of integration `v3.1.2`
  from the first one (commit of 2026-11-24).

## Symptom

1. A batch hash aggregation that spilled to disk failed with "cannot encode state" when it had an
   aggregate with `ORDER BY`, such as `last_value(x ORDER BY y)`:

   ```sql
   -- with a memory limit low enough for the aggregation to spill
   select g, last_value(x order by y) from big group by g;
   -- error: cannot encode state
   ```

2. For any aggregate, the spilled state of a group and its remaining input rows were sent to
   different partitions, so a group could come out twice, each time with a partial result.

## Cause

1. The wrapper that orders the rows of an ordered aggregate keeps them in a state that had no
   encoding, and the spilled state column was typed with the return type of the aggregate.
2. The spilled states and the input rows were partitioned with different hashes.

## Fix

The state of the ordered wrapper is encoded as a list of `struct<order key, arguments...>`
(`AggregateFunction::encoded_state_type` gives the type of the spilled column); `first_value` and
`last_value` keep a single row per group; the spilled state of a group goes to the partition of
its remaining input rows. Part of commit `c1480bc5d9` (squashed "fix(batch): spill hash
aggregation with ordered aggregates") on branch `fork/fix-batch-hash-agg-spill`.

## Tests

Unit tests `test_spill_hash_agg_with_order_by` and `test_spill_hash_agg_with_remaining_input` in
`src/batch/executors/src/executor/hash_agg.rs`.

## Upstream status

The error is reported upstream as <https://github.com/risingwavelabs/risingwave/issues/21825>
(open on 2026-11-28); the open pull request
<https://github.com/risingwavelabs/risingwave/pull/27863> reworks the spill through hash
partitions.
