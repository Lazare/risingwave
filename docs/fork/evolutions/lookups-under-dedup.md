# Duplicates that a consumer does not see

| | |
|---|---|
| Kind | optimizer rule (streaming) |
| Branch | `proto/plan-reduction-flat-lookups` |
| Status | prototype in rc4; present in rc8 |
| Variables | `streaming_merge_lookups_under_dedup` (false) |
| Design note | [`streaming-plan-reduction.md`](../../dev/src/design/streaming-plan-reduction.md), idea 3, "Duplicates that a consumer does not see" |

## Summary

Two exact rules applied in one stage (`STREAM_TOP_ONE_DEFAULT_BRANCH`):

- `TopOneDefaultBranchRule`: under a top-1 per key over a `UNION ALL` (one branch per status with a
  priority), the default branch that can never win and only supplies a row for every key of a table
  that the other side of the join already carries is replaced by a left join with default literals.
- `UnusedJoinUnderDedupRule`: below a consumer insensitive to duplicates (`min`, `max`, `bool_and`,
  `bool_or`, `DISTINCT` aggregates, `DISTINCT`, the right side of a semi/anti join, a top-1 whose
  tied rows are equal), a left outer join that outputs no column of its right side is removed.

## Motivation

A common way to compute "the current status per entity" is a union of candidate rows, one branch per
status, plus a default branch that reads the entity table with the lowest priority, then a top-1 per
entity. The default branch doubles the rows and the state of the top-1. Separately, joins kept only
for their columns that are later unused still repeat rows below deduplicating consumers.

## What it does

```sql
CREATE TABLE e (id int PRIMARY KEY);
CREATE TABLE s (e_id int, ts timestamp);
CREATE MATERIALIZED VIEW cur AS
SELECT e.id, st.status FROM e JOIN (
  SELECT id, status FROM (
    SELECT id, status, row_number() OVER (PARTITION BY id ORDER BY prio DESC) AS rn FROM (
      SELECT e_id AS id, 'active' AS status, 2 AS prio FROM s
      UNION ALL
      SELECT id, 'new' AS status, 1 AS prio FROM e) u) r
  WHERE rn = 1) st ON st.id = e.id;
-- after: the 'new' branch is gone; e LEFT JOIN (top-1 of the other branches),
--        status = coalesce-like CASE with the literal 'new' when nothing is found
```

## Configuration

`streaming_merge_lookups_under_dedup`, default false. The doc comment of the variable only mentions
the default branch; the stage also runs `UnusedJoinUnderDedupRule`.

## Correctness / safety argument

The default branch's priority literal must be strictly worse, in the order of the top-1 (with its
`NULL` placement), than every value of the other branches, its rows of a key equal on the read
columns, and the carried row guarantees one row per key. The unused-join removal only applies where
the consumer's result does not depend on the multiplicity of rows. No assumption on undeclared keys.
See the design note.

## Status and versions

Present in rc4 (and earlier prototype images); present in rc8. No batch counterpart.

## Tests

Planner `stream_merge_lookups_under_dedup.yaml`; differential
`e2e_test/streaming/merge_lookups_under_dedup.slt`.

## Limits and risks

Not done: a top-1 or `DISTINCT ON` without a total order, consumers that count or sum, an inner join
whose match is only implied above the consumer.

## Upstream relation

No direct counterpart known. Narrow pattern; upstream interest not assessed.
