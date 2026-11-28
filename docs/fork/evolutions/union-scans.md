# `UNION ALL` branches that read the same table

| | |
|---|---|
| Kind | optimizer rule (streaming and batch) |
| Branch | `proto/plan-reduction-union-scans` (streaming), `proto/plan-reduction-batch-unnest` (batch) |
| Status | prototype; streaming merge and share in rc4, batch in rc5; present in rc8 |
| Variables | `streaming_merge_union_scans` (false), `streaming_share_union_scans` (false), `batch_merge_union_scans` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), section "US" |

## Summary

- `UnionScanMergeRule`: branches of a `UNION ALL` of the form filter/projection over a scan of the
  same table (or the same shared sub-plan) read it once: one scan with the `OR` of the filters, an
  `Expand` that makes one copy of each row per branch, and per copy the filter and projection of its
  branch under a `CASE` on the branch flag.
- `UnionScanShareRule` (streaming only, separate variable): a table still scanned in several branches
  below joins or other operators is scanned once in a new shared scan; each former scan becomes a
  filter and projection of it.

## Motivation

Views often split one table into several branches (one per status, role or kind of row). Each branch
scans the table again; in streaming each scan is a backfill and a fragment.

## What it does

```text
select a, x from t where p0            Project(CASE WHEN flag = 0 THEN a ELSE b END, ..)
union all                         =>   └─Filter(CASE WHEN flag = 0 THEN p0 WHEN flag = 1 THEN p1 END)
select b, y from t where p1              └─Expand(two copies of each row, flag = 0 and 1)
                                           └─Scan(t, predicate: p0 OR p1)
```

## Configuration

`streaming_merge_union_scans`, `streaming_share_union_scans`, `batch_merge_union_scans`; default
false. The share is a separate variable because it removes scans and backfills but adds a fragment
boundary per shared scan.

## Correctness / safety argument

Exact bag semantics: each row gives one copy per branch and the copy is kept when the branch filter
is true (`NULL` drops it), so overlapping filters and duplicate rows behave as before. `CASE`
evaluates a branch's expressions only on its own copies. The `Expand` keeps the stream key of the
input plus the flag, as the union kept the branch key plus its number. Not merged: impure
expressions, `now()`, `AS OF` scans, tables with a watermark (streaming only).

## Status and versions

Streaming merge and share present in rc4; batch merge since rc5; present in rc8.

## Tests

Planner `stream_merge_union_scans.yaml`, `batch_plan_reduction_unnest.yaml`; differential
`e2e_test/streaming/merge_union_scans.slt`, `e2e_test/batch/plan_reduction_unnest.slt`.

## Limits and risks

- The `Expand` emits one copy per branch before the filters (transient, nothing in state).
- In batch, a branch that could use an index or a key range alone may read more through the merged
  scan.
- The share adds one fragment per shared scan.
- Not done: merging branches that each contain a join.

## Upstream relation

Not present in upstream RisingWave v3.1.2. Possible upstream candidate; not proposed.
