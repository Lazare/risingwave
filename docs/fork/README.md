# The fork

This repository is a fork of [RisingWave](https://github.com/risingwavelabs/risingwave). Its
upstream base is the tag **`v3.1.2`**. This directory describes every evolution the fork makes on
top of that tag, with its justification:

- [`evolutions/`](evolutions/): one file per feature, engine change, optimizer rule (or tightly
  related rule family), operational change or tool;
- [`corrections/`](corrections/): one file per correctness fix of upstream code (maintained
  separately).

Links to design notes point to `docs/dev/src/design/`. Those notes live on the branches that carry
the corresponding code (the `fork/*` branches and the integration branches), so the links resolve
there, not on a branch that holds only this documentation.

## Organisation

### `fork/*` branches and `integration/v3.1.2`

Every `fork/*` branch starts from the tag `v3.1.2` and holds one theme in one to three commits
(code, tests, docs), so that it can be carried alone to a new upstream version
(`git rebase --onto <new tag> <old tag> fork/<name>`).

`integration/v3.1.2` is the tag plus the `fork/*` branches merged `--no-ff` in the fixed order of
`scripts/fork/rebuild-integration.sh <tag> <integration branch> [remote]` (on `fork/ci-validation`):
fixes of upstream code first, then features, then tooling. Between two upstream versions, a change
is committed on the `fork/*` branch concerned and merged again into the integration branch.

Two workflows (see [ci-validation](evolutions/ci-validation.md)) check each branch alone (push to
`check/**`) and validate the integration branch end to end (push to `validate/**`).

### Plan-reduction prototype line

`integration/v3.1.2-plan-reduction` is `integration/v3.1.2` plus a stack of prototype branches
(`proto/*`), mostly optimizer rules, merged through intermediate branches (`proto/plan-reduction-int*`).
Every rule of this line is behind a session variable that is **off by default**. Images of this line
are built with the [multi-arch workflow](evolutions/ci-multiarch-image.md) and tagged
`integration-v3.1.2-plan-reduction-rc<N>`; **rc8** (commit `ef92fca4cd`) is the current one. The
main design note of the line is `docs/dev/src/design/streaming-plan-reduction.md` (it covers batch
rules too), with `docs/dev/src/design/batch-memory-safety.md` for batch memory.

First release candidate per evolution (rc4 is the earliest image considered here; "rc4" means
"present in rc4 or earlier"):

| Image | Commit | Adds |
|---|---|---|
| rc4 | `b67aee74d2` | all streaming rules of the first rounds; batch overflow wait limit and release of memory on cancellation |
| rc5 | `56616642f3` | batch key reductions, `lag` rewrite, batch unnesting / union rules, status ladder, cost-based join ordering (rows), cardinality feedback step 1 |
| rc6 | `1ba0860346` | a correction only (set operations and column pruning, see [`corrections/`](corrections/)) |
| rc7 | `75208b708d` | null join keys, batch second pass, window top-n |
| rc8 | `ef92fca4cd` | narrow value aggregates, distributed union, spill on memory wait and accounting fixes, cardinality learning, width-aware join cost |

### Enabling a session variable

The plan-reduction variables carry the `SESSION_INIT` flag: they can be set per session (`SET`), as
a default for every session (`ALTER SYSTEM SET`), or in the `[session_init]` section of
`risingwave.toml` (values quoted; only seeds parameters not yet persisted). Streaming variables act
when a streaming job is created: existing jobs keep their plan until they are recreated.

## Index

Kinds: feature, engine, optimizer rule, operations, tooling. Statuses: **integrated** (in a `fork/*`
branch merged into `integration/v3.1.2`, hence also in every plan-reduction image), **prototype in
rcN** (first image that contains it, present in rc8), **candidate** (code on a published branch, not
in any image), **in progress** (no published code).

| Evolution | Kind | Branch | Status | Variables or configuration (default) |
|---|---|---|---|---|
| [Selective barriers](evolutions/selective-barriers.md) | engine | `fork/selective-barriers` | integrated | `[meta.developer] enable_selective_barrier` (false) |
| [Native Postgres CDC engine](evolutions/pg-cdc-native-engine.md) | feature | `fork/pg-cdc-native-engine` | integrated | source option `cdc.engine` (`debezium`) |
| [Postgres sink groups](evolutions/postgres-sink-group.md) | feature | `fork/postgres-sink-group` | integrated | `postgres` connection, `sink_order = 'last'`, `pool.max_size` (4) |
| [`force_compaction` for `force_append_only` sinks](evolutions/sink-append-only-force-compaction.md) | feature | `fork/sink-append-only-force-compaction` | integrated | sink option `force_compaction` (false) |
| [UDF volatility](evolutions/udf-volatility.md) | feature | `fork/udf-volatility` | integrated | `IMMUTABLE` / `STABLE` / `VOLATILE` |
| [Batch resource governance](evolutions/batch-resource-governance.md) | engine | `fork/batch-resource-governance` | integrated (overflow wait follow-up: rc4) | `[batch]` memory ratios and limits, `overflow_memory_wait_secs` (300), `[storage] batch_read_stale_snapshot_from_meta` (false) |
| [Resource journal and open file limit](evolutions/resource-journal-and-nofile-limit.md) | operations | `fork/resource-journal-and-nofile-limit` | integrated | `RW_RESOURCE_JOURNAL_INTERVAL_SECS` (60) |
| [Default license](evolutions/license.md) | operations | `fork/license` | integrated | none |
| [Multi-arch image workflow](evolutions/ci-multiarch-image.md) | tooling | `fork/ci-multiarch-image` | integrated | workflow inputs |
| [Validation workflows and rebuild script](evolutions/ci-validation.md) | tooling | `fork/ci-validation` | integrated | `check/**`, `validate/**` |
| [Column facts and empty relations](evolutions/column-facts-and-empty-relations.md) | optimizer rule | `proto/streaming-prune-empty-relation`, `proto/plan-reduction-batch-keys` | prototype in rc4 (batch rc5) | `streaming_prune_empty_relation`, `batch_simplify_column_facts` (false) |
| [Unique-key simplifications](evolutions/unique-key-simplifications.md) | optimizer rule | same | prototype in rc4 (batch rc5) | `streaming_simplify_on_unique_key`, `batch_simplify_on_unique_key` (false) |
| [Repeated lookup merge](evolutions/repeated-lookup-merge.md) | optimizer rule | `proto/streaming-merge-repeated-lookups`, `proto/plan-reduction-flat-lookups`, `proto/plan-reduction-batch-keys` | prototype in rc4 (batch rc5) | `streaming_merge_repeated_lookups`, `batch_merge_repeated_lookups` (false) |
| [Duplicates a consumer does not see](evolutions/lookups-under-dedup.md) | optimizer rule | `proto/plan-reduction-flat-lookups` | prototype in rc4 | `streaming_merge_lookups_under_dedup` (false) |
| [Union join factoring](evolutions/union-join-factoring.md) | optimizer rule | `proto/streaming-merge-repeated-lookups`, `proto/plan-reduction-union-derived`, `proto/plan-reduction-batch-unnest` | prototype in rc4 (batch rc5) | `streaming_factor_union_join`, `batch_factor_union_join` (false) |
| [Union scans](evolutions/union-scans.md) | optimizer rule | `proto/plan-reduction-union-scans`, `proto/plan-reduction-batch-unnest` | prototype in rc4 (batch rc5) | `streaming_merge_union_scans`, `streaming_share_union_scans`, `batch_merge_union_scans` (false) |
| [Status ladder with one `CASE`](evolutions/status-ladder-case.md) | optimizer rule | `proto/plan-reduction-union-scans` | prototype in rc5 | `streaming_status_ladder_case`, `batch_status_ladder_case` (false) |
| [Aggregating subquery unnesting](evolutions/aggregating-subquery-unnesting.md) | optimizer rule | `proto/streaming-simplify-subquery-unnesting`, `proto/plan-reduction-batch-unnest` | prototype in rc4 (batch rc5) | `streaming_simplify_subquery_unnesting`, `batch_simplify_subquery_unnesting` (false) |
| [Lateral table functions, self-aggregate join](evolutions/lateral-table-functions.md) | optimizer rule | `proto/plan-reduction-lateral`, `proto/plan-reduction-batch-unnest` | prototype in rc4 (batch rc5) | `streaming_unnest_lateral_functions`, `batch_unnest_lateral_functions` (false) |
| [Stacked aggregations](evolutions/stacked-aggregations.md) | optimizer rule | `proto/plan-reduction-shared-aggs` | prototype in rc4 | `streaming_merge_stacked_aggregations` (false) |
| [Identical sub-plan sharing (streaming)](evolutions/identical-subplan-sharing.md) | optimizer rule | `proto/plan-reduction-shared-aggs` | prototype in rc4 | `streaming_share_identical_subplans` (false) |
| [Null join keys](evolutions/null-join-keys.md) | optimizer rule | `proto/plan-reduction-nullkey` | prototype in rc7 | `streaming_simplify_null_join_keys`, `batch_simplify_null_join_keys` (false) |
| [Narrow value aggregates](evolutions/narrow-value-aggregates.md) | optimizer rule | `proto/plan-reduction-narrow-value` | prototype in rc8 | `streaming_narrow_value_aggregates`, `batch_narrow_value_aggregates` (false) |
| [`lag` duplicates](evolutions/lag-duplicates.md) | optimizer rule | `proto/plan-reduction-batch-lag` | prototype in rc5 | `batch_rewrite_lag_duplicates` (false) |
| [Batch second pass, window top-n](evolutions/batch-second-pass-and-window-topn.md) | optimizer rule | `proto/plan-reduction-batch-pass2` | prototype in rc7 | `batch_reduce_keys_after_conversions`, `batch_over_window_to_topn` (false) |
| [Distributed union](evolutions/distributed-union.md) | optimizer rule | `proto/plan-reduction-batch-union-dist` | prototype in rc8 | `batch_distributed_union` (false) |
| [Cost-based join ordering with widths](evolutions/cost-based-join-ordering.md) | optimizer rule | `proto/plan-reduction-batch-joinorder` | prototype in rc5 (widths rc8) | `batch_cost_based_join_ordering` (false) |
| [Cardinality feedback and learning](evolutions/cardinality-feedback.md) | optimizer rule | `proto/batch-cardinality-feedback`, `proto/batch-cardinality-feedback-learning` | prototype in rc5 (learning rc8) | `batch_cardinality_feedback`, `batch_cardinality_feedback_learning` (false) |
| [Batch memory safety, spill on memory wait](evolutions/batch-memory-safety.md) | engine | `proto/plan-reduction-batch-mem-safety` | prototype in rc8 | `batch_spill_on_memory_wait` (false) |
| [Exchange buffer accounting](evolutions/exchange-buffer-accounting.md) | engine | `proto/plan-reduction-batch-exchange-mem` | candidate | `batch_exchange_buffer_max_mb` (0), `batch_compact_shuffle_output` (false) |
| [Shared sub-plan spool](evolutions/batch-spool.md) | optimizer rule | `proto/plan-reduction-batch-spool` | candidate | `batch_share_identical_subplans` (false) |
| [Limit / top-n pushdown](evolutions/limit-pushdown.md) | optimizer rule | `proto/plan-reduction-batch-limit` | candidate | `batch_push_down_limit` (false) |
| [Rules in progress](evolutions/in-progress-rules.md) (limit-distinct, null-safe equality, `EXISTS` simplification, outer join reduction) | optimizer rule | none published | in progress | not defined |

## Validation

The plan-reduction rules were validated on a validation set taken from a real production codebase:

- **batch**: about a thousand data quality tests (dbt data tests) over non-materialized views;
- **streaming**: several hundred materialized views and sinks.

The figures below compare the plans with every rc8 rule on against every rule off, rule by rule.
They are **planner-level counts** (operators in the plans), not run times.

| Operators in the plans | Batch (data quality tests) | Streaming (materialized views and sinks) |
|---|---|---|
| All operators | -39% | -9% |
| Joins | -51% | -11% |
| Aggregations | -44% | -15% |
| Scans | -40% | -10% |
| Exchanges | -49% | -10% |
| Gathers of a whole input into one task (`Exchange Single`) | -63% | - |
| Sorts and over windows | about -85% | - |

Rule-specific figures are in the files of the rules concerned
([narrow value aggregates](evolutions/narrow-value-aggregates.md),
[distributed union](evolutions/distributed-union.md), [shared sub-plan spool](evolutions/batch-spool.md)).
Every rule is written as a general algebraic rewrite with its own correctness argument; the
validation set only serves to find candidate patterns and measure gains. Each rule also has
synthetic differential end-to-end tests (every query run with and without its variable, results
compared as multisets after rounds of inserts, updates and deletes).

## Corrections

Fixes of upstream code are not evolutions and are documented in [`corrections/`](corrections/),
one file per fix. The fix branches are:

| Branch | Subject (from its commit) | In `integration/v3.1.2` |
|---|---|---|
| `fork/fix-snapshot-backfill` | snapshot backfill deadlock, scan stream key, `now()` epoch | yes |
| `fork/fix-batch-hash-agg-spill` | spill of the batch hash aggregation | yes |
| `fork/optimizer` | first/last value pruning, empty group key, empty relations in batch | yes |
| `fork/fix-last-seen-value-null` | `internal_last_seen_value` and `NULL` inputs | no (plan-reduction line only) |
| `fork/fix-webhook-unready-worker` | webhook request during recovery answers 503 | no (plan-reduction line only) |

Fixes made inside the plan-reduction line (for example the column pruning of set operations shipped
in rc6) are documented there too.
