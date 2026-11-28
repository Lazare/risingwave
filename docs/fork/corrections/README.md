# Corrections

This directory records every correction made in this fork of RisingWave to a **wrong result** or
a **crash** (a panic, an error on a valid query, a hang, or a resource use that brings a node
down), whether the faulty code comes from upstream RisingWave or from the fork itself. When the
changes of the fork are documented against upstream, these entries are the corrections, as
opposed to the features and optimizations of the fork.

One file per correction, named `<short-name>.md`, so that branches that add corrections merge
without conflicts. Each file gives:

- **Kind**: upstream bug (the faulty code is in an upstream release) or fork bug (the faulty code
  was added by the fork), and how it was found.
- **Affected versions**: the upstream tag where it was found (the fork is based on `v3.1.2`) and,
  for fork bugs or fixes shipped in fork builds, the fork release candidates concerned.
- **Symptom**: a minimal synthetic reproduction with the wrong and the correct result.
- **Cause**, **Fix** (commits and branch), **Tests**.
- **Upstream status**: whether upstream `main` has fixed it, with the issue or pull request when
  one exists. No issue or pull request is opened upstream from these notes.

| File | Summary | Kind |
|---|---|---|
| [not-in-null-semantics.md](not-in-null-semantics.md) | `NOT IN (subquery)` ignored NULL values | upstream |
| [derived-predicate-equal-values.md](derived-predicate-equal-values.md) | predicates derived through `a = b` on types whose equal values differ | upstream |
| [in-subquery-in-having.md](in-subquery-in-having.md) | `IN (subquery)` in `HAVING` compared the wrong column | upstream |
| [exists-scalar-aggregate.md](exists-scalar-aggregate.md) | `EXISTS` over a correlated scalar aggregation was not always true | upstream |
| [semi-anti-join-swap-output.md](semi-anti-join-swap-output.md) | swapping the sides of a semi or anti join shifted its output columns | upstream and fork |
| [group-key-of-single-row-input.md](group-key-of-single-row-input.md) | `GROUP BY` / `DISTINCT` over an input of at most one row returned a row of NULLs on an empty input | upstream |
| [first-last-value-pruning.md](first-last-value-pruning.md) | unread `first_value` / `last_value` columns kept in memory | upstream |
| [set-operation-column-pruning.md](set-operation-column-pruning.md) | columns pruned below `UNION` (without `ALL`), `EXCEPT`, `INTERSECT` | upstream code, exposed by the fork |
| [scan-stream-key-predicate-pull-up.md](scan-stream-key-predicate-pull-up.md) | planning failure of a filtered scan under a union (stream key) | upstream |
| [snapshot-backfill-checkpoint-deadlock.md](snapshot-backfill-checkpoint-deadlock.md) | snapshot backfill deadlock with `checkpoint_frequency` > 1 | upstream |
| [now-during-snapshot-backfill.md](now-during-snapshot-backfill.md) | `now()` near 2021 during the snapshot of a snapshot backfill | upstream |
| [hash-agg-spill-ordered-aggregates.md](hash-agg-spill-ordered-aggregates.md) | spill of a batch hash aggregation: error with ordered aggregates, groups emitted twice | upstream |
| [hash-agg-spill-reload-memory.md](hash-agg-spill-reload-memory.md) | spilled aggregation states reloaded beyond the memory limit | upstream |
