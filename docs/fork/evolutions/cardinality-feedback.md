# Batch cardinality feedback and learning

| | |
|---|---|
| Kind | optimizer (batch, adaptive estimates) |
| Branch | `proto/batch-cardinality-feedback` (step 1), `proto/batch-cardinality-feedback-learning` (step 2) |
| Status | prototype; step 1 in rc5, step 2 in rc8; steps 3 and 4 not started |
| Variables | `batch_cardinality_feedback` (false), `batch_cardinality_feedback_learning` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), sections "BF" and "BF step 2" |

## Summary

Batch queries count the rows output by their scans, filters, joins and aggregations; the frontend
compares them with the planner's estimates (step 1, observation only), then learns ratios
(selectivity, groups per input row, join fan-out) per normalized sub-plan and uses them in the
[cost-based join ordering](cost-based-join-ordering.md) and the [window top-n](batch-second-pass-and-window-topn.md)
limit (step 2).

## Motivation

The estimates of the cost-based join ordering are fixed guesses (no distinct counts or histograms).
A streaming plan is frozen and must stay conservative, but batch queries are planned at every run:
measured selectivities of earlier executions of the same sub-plans can correct the guesses as the
data evolves.

## What it does

Step 1 (`batch_cardinality_feedback`):

- the frontend marks the counted plan nodes; executors are wrapped in a `CountingExecutor`; counts
  flow back with the final task status (distributed) or after the last chunk (local);
- a fingerprint (64-bit hash of a canonical text of the sub-plan, constants replaced by
  placeholders) identifies a record; counts are partial when the query or the operator stopped early;
- system view `rw_catalog.rw_batch_cardinality_feedback`: hashes and counts only, no values or
  literals; up to 10 000 records per frontend; a rate-limited warning for estimates off by more than
  100 times and 100 000 rows.

Step 2 (`batch_cardinality_feedback_learning`):

- ratios keyed by relation, computed identically from the logical plan (planning) and the batch plan
  (execution); only complete counts are learned;
- per key: count, min, max and a decaying mean of `log10(ratio)`; estimates blend the guess and the
  learned mean (the guess weighs `0.25^n` after `n` observations); the pessimistic maximum drives the
  build side;
- the learned join tree replaces the default one only when it is cheaper by more than 1.5 times;
- system view `rw_catalog.rw_batch_cardinality_learning`.

## Configuration

Both default false; either variable makes the session observe its queries; learned ratios are shared
by the sessions of a frontend.

## Correctness / safety argument

Step 1 does not change plans (planner tests show identical plans with the variable on and off), and
without the variable no node is marked and no message changes. Step 2 only changes join order and
the top-n guard, whose results are independent of the estimates.

## Status and versions

Step 1 in rc5, step 2 in rc8. Step 3 (estimate intervals, worst-case gain) and step 4 (plan
regression guard) are planned in the design note, not started.

## Tests

Unit tests in `cardinality_feedback.rs`, `cardinality_learning.rs`, the batch counting executor and
the frontend counters; planner `batch_cardinality_feedback.yaml`,
`batch_cardinality_feedback_learning.yaml` (hook `cardinality_observations`); e2e
`e2e_test/batch/cardinality_feedback.slt`, `e2e_test/batch/cardinality_feedback_learning.slt`.

## Limits and risks

- Records and ratios are per frontend and lost on restart.
- Unions, top-n, table functions and sources are not counted; nothing above an outer join is learned.
- A learned ratio does not depend on the literal value (one distribution per key).
- Index scans and lookup joins with predicates are not matched with the logical plan.

## Upstream relation

No equivalent in upstream RisingWave. Comparable to adaptive statistics features of other engines.
Large and experimental; not an upstream candidate as is.
