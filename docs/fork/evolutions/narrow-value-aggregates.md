# Narrow values of `first_value` / `last_value`

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/plan-reduction-narrow-value` |
| Status | prototype in rc8 |
| Variables | `streaming_narrow_value_aggregates` (false), `batch_narrow_value_aggregates` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "NV" |

## Summary

When the result of `first_value`, `last_value` or `internal_last_seen_value` is only read through
deterministic functions that cannot fail (`->`, `->>`, comparisons, `CASE`, ...), the aggregation
keeps those functions of the value instead of the whole value: `f(last_value(x ORDER BY k))` becomes
`last_value(f(x) ORDER BY k)`, several of them packed into one `ROW(...)`. The narrowed expressions
are also pushed into the branches of a `UNION ALL` and into the side of a join that the value comes
from.

## Motivation

A view that deduplicates wide JSONB events per entity ("latest version per key") keeps the whole
document per group: in the state table in streaming, in memory in batch, and in every join, exchange
and union below. Consumers usually read a few fields only.

## What it does

```text
SELECT count(*) FROM v WHERE fact->>'dir' IS NULL
-- v: SELECT id, last_value(fact ORDER BY ts) AS fact FROM t GROUP BY id

Agg(count)                                   Agg(count)
└─Filter((last_value(fact)->>'dir') IS NULL) └─Filter(last_value($1))
  └─Agg(id; last_value(fact ORDER BY ts)) =>   └─Agg(id; last_value($1 ORDER BY ts))
    └─Scan(t)                                    └─Project(id, ts, (fact->>'dir') IS NULL AS $1)
                                                   └─Scan(t)
```

Rules: `NarrowValueAggRule`, `NarrowValuePushRule`, with `NarrowValueProjectMergeRule` between
rounds.

## Configuration

Both variables default false.

## Correctness / safety argument

The row an aggregate picks depends on the order key (and ties), never on the value, and `f` of the
picked value is `f` applied to the picked row. All fields come from one aggregate of a row, so they
come from the same row even under ties. Only deterministic, pure functions that cannot fail are
moved (they run on every input row instead of the picked one); a cast that may fail stays above.
Where the aggregate may see no row (no group key, `FILTER`, padded side of an outer join), only
expressions that are `NULL` on `NULL` are moved.

## Status and versions

New in rc8.

## Tests

Planner `batch_narrow_value_aggregates.yaml`, `stream_narrow_value_aggregates.yaml`; differential
`e2e_test/batch/narrow_value_aggregates.slt`, `e2e_test/streaming/narrow_value_aggregates.slt` (NULL
values and order keys, missing fields, ties, unions, joins, updates and deletes of the last rows).

Validation set (planner-level counts, see the README): aggregations of whole JSON values drop by
about 90% in the batch data tests and about 95% in the materialized views with this rule.

## Limits and risks

Not rewritten: `DISTINCT` aggregates, a value that is also its order key, a value read as it is,
narrow types (only JSONB, text, bytea, structs, lists and maps are narrowed). A right side of a join
that the planner may look up by key is left alone so that a lookup join stays possible.

## Upstream relation

Not in upstream RisingWave v3.1.2. Upstream PR candidate.
