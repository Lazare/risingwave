# Streaming and batch plan reduction

Working notes of the plan-reduction prototype branches, which reduce streaming graphs and batch
plans at planning time. The work started with the streaming branches
`proto/streaming-prune-empty-relation` and `proto/streaming-merge-repeated-lookups` (idea 3,
derived from the former); the batch counterparts and the batch-only rules (BK, BL, BJ, BF, BP, BT,
BU and "Batch counterparts") followed on `proto/plan-reduction-*` and `proto/batch-*` branches.
The note records what is done, what is being done, and every idea that was considered, including
those that are not pursued for now. The file keeps its historical name,
`streaming-plan-reduction.md`.

A streaming job is planned once and its plan is frozen: a rewrite must hold for all future data,
including retractions, `NULL`s (three-valued logic) and watermarks. This is why everything below
is opt-in through session variables that are off by default, and why the batch optimizer keeps its
own rules. The batch rules are opt-in too, through `batch_` session variables that are off by
default.

## Status

| # | Idea | Status |
|---|------|--------|
| A | Predicates that are never true become an empty relation | done (prototype) |
| B | Propagation of an empty relation, outer and anti joins with an empty side, `LIMIT 0` | done (prototype) |
| C | Aggregation without `GROUP BY` over an empty input becomes its constant row | done (prototype) |
| D | Constant propagation, `CASE` with constant conditions | done (prototype) |
| E | Known nullability, including the `NOT NULL` constraints of tables | done (prototype) |
| F | Redundant conjuncts and bounds, filters that are always true | done (prototype) |
| G | Inner join with one row of constants becomes a filter and a projection | done (prototype) |
| H | Report of the fragments saved when a materialized view is created | done (prototype) |
| 4 | Stronger derivation of unique keys | done (prototype) |
| 1 | Elimination of an unused outer join on a unique key | done (prototype) |
| 2 | Elimination of an aggregation, `DISTINCT` or top-n on a key | done (prototype) |
| 3 | Elimination of a self-join on a key | in progress: repeated lookups of a table done on the branch `proto/streaming-merge-repeated-lookups`, lookups nested in trees of joins and semi or anti joins answered by a carried row done on the branch `proto/plan-reduction-flat-lookups`, aggregations next |
| 5 | Aggregation pushed below a join onto the unique side | done (prototype, branch `proto/plan-reduction-agg-join`): exact form for streaming and batch (`streaming_push_agg_below_join`, `batch_push_agg_below_join`), partial form for batch chosen with the learned group ratios (`batch_partial_agg_below_join`), see 5 |
| UA | Batch: partial aggregation in the branches of a `UNION ALL` (`batch_partial_agg_into_union`) | done (prototype, branch `proto/plan-reduction-agg-join`), see "UA" below |
| 6 | Merge of two stacked aggregations | done (prototype, branch `proto/plan-reduction-shared-aggs`): stacked aggregations, through a `UNION ALL` too, and a dedup below a distinct count; a dedup below a top-1 or a semi join not done (see 6) |
| 7 | Join common to the branches of a `UNION ALL` joined once above the union | done (prototype, branch of idea 3); derived joined side, several joins and semi or anti joins above the join on the branch `proto/plan-reduction-union-derived` |
| 8 | Aggregate of a constant over a group | done (prototype, branch of idea 3) |
| 9 | Leftovers of subquery decorrelation (domain re-scan, aggregation and null-safe join back) | aggregating subqueries done (prototype, branch `proto/streaming-simplify-subquery-unnesting`); laterals without a table not started |
| US | Branches of a `UNION ALL` that read the same table read it once (`Expand` per branch); scans of other branches shared (`streaming_share_union_scans`) | done (prototype, branch `proto/plan-reduction-union-scans`); dominance under a top-1 not done |
| 10 | Sharing again, within one query, the sub-plans that became identical, or that differ by their columns or by commuting filters; aggregations of one input with different filters merged with `FILTER` | done for stateful cores (prototype, branch `proto/plan-reduction-shared-aggs`) |
| BK | Column facts (A to G, 8), unique keys (1, 2) and repeated lookups (3) in batch queries | done (prototype, branch `proto/plan-reduction-batch-keys`, variables `batch_simplify_column_facts`, `batch_simplify_on_unique_key`, `batch_merge_repeated_lookups`) |
| BL | Batch: duplicates found with `lag` over a window ordered by the column, counted per value instead (`batch_rewrite_lag_duplicates`) | done (prototype, branch `proto/plan-reduction-batch-lag`) |
| B | Batch counterparts of 9, 9b, 9c, 7 and US, each behind a `batch_` variable | done (prototype, branch `proto/plan-reduction-batch-unnest`), see "Batch counterparts" |
| SL | Status ladder: the branches of a top-1 by priority that read the same relation picked with one `CASE` | done (prototype, branch `proto/plan-reduction-union-scans`); `GROUP BY` branches of the same relation merged into one aggregation and second pass after the sharing of widened copies done (branch `proto/plan-reduction-ladder-ext`); default branch rewritten into a join not done (not beneficial) |
| NK | Join on a column known to be `NULL` matches nothing (`batch_simplify_null_join_keys`, `streaming_simplify_null_join_keys`) | done (prototype, branch `proto/plan-reduction-nullkey`), see "NK" below |
| NV | `first_value` / `last_value` keep only the functions of the value that are read (`batch_narrow_value_aggregates`, `streaming_narrow_value_aggregates`) | done (prototype, branch `proto/plan-reduction-narrow-value`), see "NV" below |
| BJ | Batch: cost-based ordering of the inner joins from the row counts and sizes of the tables (`batch_cost_based_join_ordering`) | first version (prototype, branch `proto/plan-reduction-batch-joinorder`, see "BJ" below), width-aware since 2026-11-27 (the build side is the side with fewer bytes; left outer, semi and anti joins can become right joins); index lookups and column statistics not done |
| BF | Batch: cardinality feedback, step 1: count the rows of the operators at execution and compare them with the estimates (`batch_cardinality_feedback`) | first version (prototype, branch `proto/batch-cardinality-feedback`, see "BF" below): observation only, no plan changes |
| BL2 | Batch: cardinality feedback, step 2: learn ratios (selectivity, groups, join fan-out) from the counts and use them in the estimates of BJ and in the limit of BT (`batch_cardinality_feedback_learning`) | first version (prototype, branch `proto/batch-cardinality-feedback-learning`, see "BF step 2" below); plan regression guard (step 4) not done |
| BP | Batch: second pass of the reductions on keys after the stages that create aggregations (`batch_reduce_keys_after_conversions`) | done (prototype, branch `proto/plan-reduction-batch-pass2`, see "BP and BT" below) |
| BT | Batch: `row_number() <= n` per partition becomes a top-n per group, small `n` only (`batch_over_window_to_topn`) | done (prototype, branch `proto/plan-reduction-batch-pass2`, see "BP and BT" below) |
| BU | Batch: `UNION ALL` run in parallel tasks instead of one (`batch_distributed_union`) | done (prototype, branch `proto/plan-reduction-batch-union-dist`, see "BU" below) |
| BS | Batch: identical sub-plans of a stage computed once per task and read again through a spool (`batch_share_identical_subplans`) | first version (prototype, branch `proto/plan-reduction-batch-spool`, see "BS" below); copies in different stages not shared |
| BLM | Batch: a copy of a `LIMIT` / top-n pushed into the branches of a `UNION ALL` and into the preserved side of a left or right join (`batch_push_down_limit`) | done (prototype, branch `proto/plan-reduction-batch-limit`, see "BLM" below); full outer join rejected (wrong results) |
| BLD | Batch: a distinct under a `LIMIT` (`SELECT DISTINCT`, `UNION`) read by a non-blocking operator that stops after `limit + offset` rows, also in each branch of the union (`batch_limit_distinct`) | done (prototype, branch `proto/plan-reduction-batch-limit`, see "BLD" below); ordered variant not done |
| NE | `a IS NOT DISTINCT FROM b` becomes `a = b` when an operand is never `NULL` (`batch_null_safe_eq_to_eq`, `streaming_null_safe_eq_to_eq`) | done (prototype, branch `proto/plan-reduction-null-rules`, see "NE" below) |
| SE | Simplification of the subquery of a correlated `EXISTS` / `IN`: no limit, `DISTINCT` or `GROUP BY` without aggregates (`batch_simplify_exists_subquery`, `streaming_simplify_exists_subquery`) | done (prototype, branch `proto/plan-reduction-null-rules`, see "SE" below) |
| RO | Outer join reduction with a null-rejection test, and `LEFT JOIN ... WHERE r.c IS NULL` as an anti join (`batch_reduce_outer_joins`, `streaming_reduce_outer_joins`) | done (prototype, branch `proto/plan-reduction-null-rules`, see "RO" below) |
| IV | Join with a small constant `VALUES` becomes a filter (`IN` list) and a `CASE` (`batch_inline_values_join`, `streaming_inline_values_join`; survey rule 8) | done (prototype, branch `proto/plan-reduction-or-values`), see "IV" below |
| OR | Restrictions of each side derived from an `OR` of a join condition (`batch_derive_or_restrictions`, `streaming_derive_or_restrictions`; survey rule 6) | done (prototype, branch `proto/plan-reduction-or-values`), see "OR" below |
| OU | Join on an `OR` of equi-conditions as equi-joins combined with `UNION ALL` or anti joins (`batch_or_join_to_union`, `streaming_or_join_to_union`; survey rule 7) | done (prototype, branch `proto/plan-reduction-or-values`), see "OU" below |
| TP | Predicates copied to the other side of a join through its equalities, transitively; implied equalities in the batch cost-based join ordering (`streaming_transitive_predicates`, `batch_transitive_predicates`) | done (prototype, branch `proto/plan-reduction-transitive-preds`, see "TP" below) |
| S12 | `INTERSECT` / `EXCEPT` computed by one counting aggregation over a `UNION ALL` of the inputs (`streaming_set_operation_to_count`, `batch_set_operation_to_count`) | done (prototype, branch `proto/plan-reduction-setop-keys`), see "S12" below; `ALL` variants not supported by the binder |
| S13 | `UNION` / `INTERSECT` / `EXCEPT` of filters over the same input become one filter (`streaming_merge_set_operation_filters`, `batch_merge_set_operation_filters`) | done (prototype, branch `proto/plan-reduction-setop-keys`), see "S13" below |
| S9 | Group key columns that the other ones determine removed, `min` / `max` / `first_value` / `last_value` of a value of the group replaced (`streaming_reduce_group_keys`, `batch_reduce_group_keys`) | done for aggregations (prototype, branch `proto/plan-reduction-setop-keys`), see "S9" below; partition and order keys of windows and top-n not done |
| S10 | Branches of a `UNION ALL` that only feed `NULL` to the aggregation above removed (`streaming_prune_null_union_branches`, `batch_prune_null_union_branches`) | done (prototype, branch `proto/plan-reduction-setop-keys`), see "S10" below |
| SJ17 | Batch: inner join to semi join on a unique side that is not read; `DISTINCT` removed under a semi or anti join by estimates (`batch_inner_join_to_semi_join`) | done (prototype, branch `proto/plan-reduction-batch-joins`, see "SJ" below) |
| SJ18 | Batch: `IS NOT NULL` on equi-join keys, on the sides whose unmatched rows are not output (`batch_join_keys_not_null`) | done (prototype, branch `proto/plan-reduction-batch-joins`, see "SJ" below) |
| SJ15 | Batch: range form of `date_trunc`, `::date`, `extract(year ...)` compared with constants (`batch_date_range_predicates`) | done for `date`, `timestamp` and `timestamptz` in `UTC` (prototype, branch `proto/plan-reduction-batch-joins`, see "SJ" below); other time zones not done |
| SJ14 | Batch: semi and anti joins pushed into an input of the inner joins or merged as an inner join on a unique side, by the costs of BJ (`batch_semi_join_ordering`) | first version (prototype, branch `proto/plan-reduction-batch-joins`, see "SJ" below); semi joins reading several inputs not done |
| SJ16 | Batch: outer-join reordering with the identities of PostgreSQL | design only (see "SJ" below): too large and risky for this round |
| SU | Streaming: inputs of a `UNION ALL` shuffled directly by the key the operator above needs, when that key is part of the stream key of the union (`streaming_distributed_union`) | done (prototype, branch `proto/plan-reduction-stream-union-dist`, see "SU" below); key outside the stream key not done (see "SU") |
| MK | Streaming: the plan reductions keep the key of a materialized view (its primary key and distribution key) when the key without them is still valid (`streaming_preserve_mv_key`) | done (prototype, branch `proto/plan-reduction-mv-key-regime`), see "MK" below |
| NC | `e IS NULL` together with a conjunct that is strict in `e` (comparison, `LIKE`, regular expression, arithmetic, cast...), for any deterministic `e` (a `COALESCE` of columns...): the filter, scan or join matches nothing (`batch_fold_null_contradictions`, `streaming_fold_null_contradictions`) | done (prototype, branch `proto/plan-reduction-contradictions`), see "NC" below |
| IL | Intersection of the `IN` lists / equalities on the same expression, repeated arguments of `COALESCE` removed (`batch_simplify_in_lists_and_coalesce`, `streaming_simplify_in_lists_and_coalesce`) | done (prototype, branch `proto/plan-reduction-contradictions`), see "IL" below |
| EB | Batch executor: a hash join whose build side holds no key that can match does not execute its probe side, when the output can not depend on it and the probe side reads nothing from other tasks | done (unconditional, branch `proto/plan-reduction-contradictions`), see "EB" below |
| BC | Batch: cross join, or join on the same constant on both sides, as a nested loop join that copies its left side to every task (broadcast exchange) instead of one task (`batch_broadcast_cross_join`) | done (prototype, branch `proto/plan-reduction-contradictions`), see "BC" below |
| RK | Unique key of the partition columns of a `row_number() = n` filter over a window (the empty key without `PARTITION BY`), for the rules of 1 and 2 (`streaming_simplify_on_unique_key`, `batch_simplify_on_unique_key`) | done (prototype, branch `proto/plan-reduction-fix-cast-rownum`, see "RK" below) |
| GO | Batch: inner or semi join on the group columns of a top-n per group, a deduplication, a window or an aggregation computed below the operator, through `UNION ALL` into the operators below (`batch_push_join_below_group_ops`) | done for batch (prototype, branch `proto/plan-reduction-group-pushdown`, see "GO" below); streaming not done (no general reduction of the state) |
| NV2 | Narrow values through a top-n (per group or not), a deduplication, a window and a filter right above one of them (`batch_narrow_value_aggregates`, `streaming_narrow_value_aggregates`, a generalisation of NV) | done (prototype, branch `proto/plan-reduction-group-pushdown`, see "GO" below) |
| X | Sharing sub-plans or state between materialized views | **not for now** (see below) |
| — | Everything under "Not selected yet" | recorded only |

Decisions of 2026-11-25: the six ideas numbered 1 to 6 are worth exploring, starting with 4, 1
and 2; sharing between materialized views is explicitly left out for now because it is a very
large piece of work.

## What the prototype does (A to H)

Session variable `streaming_prune_empty_relation`. One bottom-up stage after the final predicate
pushdown of the streaming optimizer, rules in this order:

1. `ColumnFactsSimplifyRule` (D, E, F): derives, per plan node, the columns that hold one constant
   and the columns that are never `NULL`, and simplifies predicates and projections with them.
   - Never `NULL`: constants, `IS ...` tests, `COALESCE` with such an argument, counting
     aggregates, columns that a filter below rejects `NULL` for, and columns of a table declared
     `NOT NULL`. A table drops the rows that violate `NOT NULL` whatever writes them (DML,
     connector, sink into the table). Not used for a table with a generated column, nor for a
     column whose default at the time it was added is unknown or `NULL`.
   - Lost above the optional side of an outer join: `LEFT JOIN ... WHERE s.k IS NULL` is kept.
   - A plain column of a projection, the equalities between the two sides of a join and the
     temporal filters (`now()`) are left as they are.
2. `NeverTruePredicateRule` (A): on filters, scans and join conditions. A constant `FALSE` or
   `NULL` conjunct, a comparison with a `NULL` constant, `x IS NULL` with a conjunct that rejects
   `NULL`, or conjuncts comparing one column with constants that can not hold together.
3. `EmptyRelationPropagateRule` (B, shared with batch).
4. `EmptyInputSimplifyRule` (B, C).
5. `ConstantRowJoinRule` (G).

Limits that are known and accepted:

- Value contradictions and constants only for booleans, integers, varchar, date, time, timestamp
  and timestamptz, and only for a direct column reference compared with a constant of the same
  type. Not for decimal, floats, intervals, nor a column behind a cast.
- Types are not assumed to be discrete: `x > 1 AND x < 2` on an integer is kept.
- `IS NULL` on the key of an inner join is not simplified when the filter was already pushed below
  the join.
- `FULL OUTER JOIN` whose condition is never true, and as-of joins, are left untouched.
- Watermarks (fixed on the branch `proto/plan-reduction-nullkey`): a part of the plan that may
  carry a watermark (a scan of a table or materialized view with a watermark column, a source with
  a watermark, `now()`) is never replaced with an empty relation, whatever the rule (never-true
  predicate, join folding, propagation, `LIMIT 0`, constant row). Nor is the lookup side of a
  temporal join (a scan `AS OF`): emptied, it turns the temporal join into a hash join, which does
  not pass on the watermarks of its other side; its predicate is also kept as written when it is
  never true, and so is the condition of a join that is not folded, since a condition folded into
  `FALSE` loses its equalities. Without this, an `EMIT ON WINDOW CLOSE` job over a temporal join
  with a contradictory filter on the looked up side could not be planned any more. The cost: a
  contradiction next to a temporal filter keeps the `now()` side and the dynamic filter (over an
  empty input). Batch plans are not concerned (the batch variants of the rules are unchanged).

Open points:

- Selective barriers: an empty `VALUES` that is the root of a fragment is classified "always
  awake". A materialized view that is pruned to nothing keeps such a fragment. To be checked on a
  build that has both features.
- If a later version supports `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL` (parsed but not
  implemented today) or changing the query of an existing materialized view, the plans that relied
  on E must be invalidated.
- The gain on real materialized view definitions has not been measured yet.

## Selected ideas

A hazard is common to all of them: an aggregation or a join tolerates a stream whose key is only
exact at barrier boundaries, a projection does not. Once a stateful operator is replaced with a
projection, a wrong stream key changes the result silently instead of raising an inconsistency.
Hence a separate session variable, `streaming_simplify_on_unique_key`.

### 4. Stronger derivation of unique keys (enabler)

The optimizer knows the stream key of a node and its functional dependencies, but a single rule
reads the dependencies (`AggGroupBySimplifyRule`), and keys are lost inside a plan:

- after a top-1 or a dedup, the dependencies are those of the input although the stream key is the
  group key;
- a left outer join only keeps the dependencies of its left side, so the key of the left side is
  no longer a key of the output even when the right side is unique on the join key;
- a union has no dependency at all.

Wanted: (a) the group key as a key after top-n and dedup; (b) the keys of the left side as keys of
a left or inner join whose right side is unique on the join columns, and symmetrically; (c) later,
`(tag, key of the branch)` as a key of a `UNION ALL` whose branches hold distinct constants in a
tag column (needs D; only useful for the `UNION` rewrite below).

Without this, 1 to 3 rarely fire across layers of views, because staging views commonly dedup with
`row_number() = 1`.

### 1. Unused outer join on a unique key

`SELECT f.id, f.amt FROM f LEFT JOIN dim d ON f.dim_id = d.id` becomes `SELECT id, amt FROM f`.

- Removes the join with its two states and its degree table, the scan and the backfill of `dim`.
- Sound when no column of the optional side is used above, and the equalities of the condition
  bind every column of a key of the optional side to a column of the other side or to a constant:
  a row of the preserved side then matches at most one row. Extra conjuncts are harmless.
- An inner join can not be removed this way: it would need a foreign key and `NOT NULL`.
- Removing the join can only add watermark columns. The dependency on `dim` stays recorded, since
  dependencies come from the bound query, not from the plan.
- Variant not done: under a `DISTINCT`, uniqueness of the optional side is not needed.
- Seen in PostgreSQL (`analyzejoins.c`), Calcite (`PROJECT_JOIN_REMOVE`, `AGGREGATE_JOIN_REMOVE`,
  both in the streaming rule set of Flink), CockroachDB (`EliminateJoinUnderProjectLeft`).

### 2. Aggregation, DISTINCT or top-n on a key

When the group key contains a key of the input, every group holds exactly one row:

- `SELECT DISTINCT id, a FROM t` becomes `SELECT id, a FROM t`;
- `SELECT id, sum(x), count(*) FROM t GROUP BY id` becomes `SELECT id, x::bigint, 1 FROM t`;
- a top-n per group, or a dedup, on such a key is its input.

Needs a non-empty group key (an aggregation without group key emits a row over an empty input) and
aggregates with a closed form over one row. `NULL` in a key is fine: a key treats it as a value.

Visible difference, found by the differential test: a streaming `sum` keeps `0`, not `NULL`, once
all its inputs are retracted while its group still holds rows (a `FILTER` that the row does not
pass any more, or an argument that becomes `NULL`). The projection that replaces the aggregation
returns `NULL`, as the batch execution of the query does. A materialized view created with the
simplification can therefore hold `NULL` where the same view without it holds `0`.
Seen in Materialize (`reduce_elision`), Calcite (`AGGREGATE_REMOVE`), CockroachDB
(`EliminateDistinct`, `EliminateGroupBy`), Spark (`RemoveRedundantAggregates`).

### 3. Self-join on a key

`SELECT a.id, a.x, b.y FROM t a JOIN t b ON a.id = b.id WHERE a.p AND b.q` becomes
`SELECT id, x, y FROM t WHERE p AND q AND id IS NOT NULL`. Same for two aggregations of the same
input joined on their group key: one aggregation with a `FILTER` per call.

- **First step, raised on 2026-11-25: the same table joined several times on its key.**
  `f LEFT JOIN d d1 ON f.a = d1.id LEFT JOIN d d2 ON f.a = d2.id` reads the same row of `d`
  twice: by transitivity `d1.id = d2.id`, which is a self-join of `d` on its key. One join is
  enough and the columns of `d2` are those of `d1`. Typical when two views each join the same
  dimension and a third one joins both views. Sub-plan sharing already gives a single scan of
  `d`, but each join keeps its own state for both of its sides.
  - Sound when both joins read the same relation with the same filter, both conditions bind a
    key of it to the *same* column of the other side (not `buyer_id` and `seller_id`: those are
    two different rows), and the join types agree (both inner, both left, or inner then left).
  - With different filters on the two reads, the columns of the second one need a `CASE` on
    its filter: that is the general rule below.
  - The work: the two joins are often not adjacent after join ordering, the columns of the kept
    read must be widened and carried through the joins in between, and inner joins can be
    handled in the multi-join form while left joins can not.
- **Done on the branch `proto/streaming-merge-repeated-lookups`** (derived from the prototype
  branch, variable `streaming_merge_repeated_lookups`, rule `RepeatedLookupMergeRule`):
  - The *lookup* is the side that is removed: a scan of a table under `Project`, `Filter` and
    `Share` nodes only, joined (inner or left outer) on the *whole* key of the table with `=`,
    on the simple types for which equal values are indistinguishable.
  - An *instance* is another scan of the same table in the kept side. The analysis `carried`
    follows it up through `Project`, `Filter`, inner, left outer, full outer, left semi and anti
    joins, and tracks the columns that are *bound* to a column of the table: when such a column
    is not `NULL`, the instance is matched (not padded by an outer join) and the column holds
    the value of the matched row. The columns of the scan are bound, and so are the columns that
    a filter or an inner join compares them with (`f.dim_id = d.id`).
  - A lookup from bound columns finds the matched row of the instance. Its columns are computed
    from that row; "found" is: the compared columns are not `NULL`, the row satisfies the filter
    of the lookup, and the rest of the join condition holds. An inner lookup becomes a filter on
    that, a left lookup a `CASE` on that.
  - A left outer join does not bind the column of its left side: `f.dim_id` can hold a value
    that has no row in `d`. But a second lookup from the same column searches for the same row.
    The first join is then widened: its scan reads the rows that satisfy the filter of either
    lookup (`OR`), and the columns of each lookup are guarded by its own filter. Requires the
    condition of the first join to be exactly the equalities on the key.
  - The kept scan is widened with the columns that the removed lookup needs, appended to the
    output of every node on the way up, so that existing column references stay valid.
  - A semi or anti join on the key (`EXISTS` / `NOT EXISTS` on a table that the other side
    carries) becomes a filter on "found", or on its opposite.
  - Not followed yet: an instance below an aggregation, top-n, union or a shared sub-plan that
    is more than a scan; a lookup written as `IS NOT DISTINCT FROM`; a key column compared with
    a constant; a key of a type outside the simple types.
  - Since then the instance is also followed through top-n, limit and dedup, through an
    aggregation whose group key designates its row (the other columns are read with
    `internal_last_seen_value`), and into a shared sub-plan when it already outputs the columns
    that the removed lookup reads. A shared sub-plan is never rewritten: its other consumers
    would compute it twice.
  - Still out of reach: a lookup whose removed side is itself a chain of joins that ends with
    the scan, and a key equality that an ancestor join establishes. Both need the inner joins
    to be looked at as one flat join with classes of equal columns.
- **Lookups nested in a tree of joins, done on the branch `proto/plan-reduction-flat-lookups`**
  (same variable and rule, module `flat` of the rule):
  - A tree of inner joins, filters and projections is one flat join: the cross product of its
    leaves, filtered by all its conjuncts. A left outer join of the tree keeps every row of its
    preserved side and can be applied last: `(X LEFT JOIN Y) JOIN L` is
    `(X JOIN L) LEFT JOIN Y` when the condition with `L` does not refer to `Y`, and a conjunct
    can move above the left joins of the columns it does not refer to. The equalities of the
    conjuncts make *classes* of equal columns: `a.x = b.y AND b.y = c.z` puts `a.x`, `b.y` and
    `c.z` in one class, and in every row they hold the same value, which is not `NULL`.
  - The lookup is now any leaf of the tree reached through inner joins, filters, projections
    and the preserved side of left outer joins. It is taken out of the tree: the joins and
    filters that do not need its columns stay in place, the conjuncts that do are lifted, and
    so are the left outer joins whose condition reads it. The lookup is then joined with the
    rest of the tree and that join goes through the rule as before; the lifted left joins and
    conjuncts follow. `f JOIN d ON f.dim_id = d.id JOIN (s JOIN d d2 ON s.id = d2.id) x
    ON d.id = x.id` keeps one read of `d`, and `(d LEFT JOIN a ...) JOIN (d d2 LEFT JOIN b ...)
    ON d.id = d2.id` becomes `d LEFT JOIN b ... LEFT JOIN a ...`.
  - A join of the tree that loses all its equalities to the lifted conjuncts is given the
    equalities that the classes imply between its sides (`g.id = d2.id AND d2.id = s.id` leaves
    `g.id = s.id` when `d2` goes): a streaming join needs one.
  - The lookup may also be nested in the right side of a left outer, semi or anti join whose left
    side carries the row: each column of its key is in a class (of the right side and of the
    condition of the join) with a column of the left side bound to the same column of the
    instance. In a pair of rows that the join matches, the lookup then reads the row of the
    instance. The rest of the right side is joined instead, with the same conditions over the
    row of the instance, its filter and the non-`NULL` compared columns. For a left join, the
    columns of the right side that are computed (and not plain columns of the rest, which the
    join pads) are guarded by "the rest matched" (a column of the rest compared with `=` is not
    `NULL`), and the left joins lifted from the right side are applied with that guard. A
    conjunct on the columns of such a left join refuses the rewrite: it removes rows of the right
    side, not pairs. For a semi or anti join, the lifted left joins are dropped (they never
    remove a row).
  - A semi or anti join is also answered by the *row of the instance itself* when its condition
    compares columns of the table, key or not, with columns of the left side bound to the same
    columns of one instance, the other conjuncts refer to the left side only, and the scan of
    the instance has the filter of the right side: the carried row matches whenever these
    columns are not `NULL`, and nothing matches otherwise. `EXISTS` becomes a filter on that,
    `NOT EXISTS` its opposite. No uniqueness is needed: one matching row is enough. With a
    filter that the carried row may fail, another row could match: kept.
  - The columns bound to the same column of an instance hold the same value or are all `NULL`,
    so an inner join binds an instance of one side through the bound columns of the other side
    too: the equalities of the joins below are not lost when the instance is met higher.
  - The instance is also followed through a table function in the select list (`ProjectSet`):
    each of its rows comes from one row of its input.
  - A semi or anti join whose right side is a shared sub-plan (a view used twice) is looked into
    too, but only rewritten when nothing of the sub-plan is left in it: the rest would otherwise
    be computed a second time next to the shared one.
  - Second round:
    - *Derived relations*: a shared sub-plan with a unique key (the group key of an
      aggregation, the columns of a `DISTINCT`, the group key of a top-1, as the unique keys of
      idea 4 derive them) is a relation like a table: two reads of the *same* `Share` node read
      the same rows, so a read on its whole key is a lookup, and the `Share` node carries an
      instance whose output columns are bound. A sub-plan that is not shared is never matched
      with another one: two equal sub-plans are not proven to be equal.
    - A left join that looks a row up by its key, from the column that a lookup is compared
      with, searched the same rows when both have the same filter: the lookup finds a row
      exactly when the instance is matched, which a bound column of the key tells. Used for
      shared sub-plans (which can not be widened) and for the nested lookups.
    - *Padded lookups*: a lookup that is the padded side of a left join of the tree. Under an
      inner join, the left join is lifted and the lookup merged into it (`merge_lookup` handles
      a left lookup). In the right side of a left join, its columns are those of the carried row
      when the condition of its left join and its filter hold, `NULL` otherwise; the proof that
      it reads the carried row may use the condition of its own left join, which holds whenever
      it finds a row.
    - The merge stage runs up to three passes: a join rewritten in a pass is not looked at again
      in that pass, and may still hold a second lookup.
    - The conditions that a rewrite builds are tidied: repeated conjuncts go, `x = x` becomes
      `x IS NOT NULL`.
  - Not handled, deliberately: a lookup whose row is designated through a lifted left join, a
    key compared on a computed expression, two equal sub-plans that are not shared.
- **Duplicates that a consumer does not see** (branch `proto/plan-reduction-flat-lookups`,
  variable `streaming_merge_lookups_under_dedup`, rules `TopOneDefaultBranchRule` and
  `UnusedJoinUnderDedupRule`). Exact only, no assumption on keys that are not declared:
  - *Default branch of a status*: a top-1 per key over a `UNION ALL` (one branch per status,
    with a priority), joined (inner or left) on the group key with a side that carries a row of
    the table that the default branch reads. The default branch reads the table without a filter
    the carried row could fail, outputs the key and literals in the columns that are read, and
    its priority is a literal strictly worse, in the order of the top-1 (`NULL`s placed as the
    order places them), than every value of the other branches (literals, or `CASE`s of
    literals). It then never wins, not even on a tie; its rows of a key are equal on the read
    columns; and it has a row for every carried key (the carried row is one, whether or not the
    table is unique on that key). The top-1 of the other branches, left-joined, gives the status
    when it finds a row, the default literals otherwise; the filters above the top-1 apply to
    that value. The other branches keep their rows and their relative order, so the top-1
    keeps the same row among them.
  - *Unused left join*: below an aggregation of `min`, `max`, `bool_and`, `bool_or` or
    `DISTINCT` calls (or a `DISTINCT`), the right side of a semi or anti join, or a top-1 whose
    rows tying on the order are equal (every column in the group key, the order, or constant),
    a left outer join that outputs no right column only repeats rows, and is removed. It is
    looked for through projections, filters, unions, inner and left joins and the left side of
    semi and anti joins, never into a shared sub-plan.
  - Not done: a top-1 or `DISTINCT ON` without a total order (it keeps an arbitrary row, which a
    rewrite may change), a consumer that counts or sums, an inner join whose match is only
    implied by a join above the consumer. An unused inner join on the key is never removed:
    the row may be missing, which the join filters (an unused left join on a unique key is
    removed by idea 1). Redundancies that hold only because a consumer is insensitive to
    duplicates (a top-1 or a `min` above) are not used either.
- **Next for the aggregations**: `Agg(group k)` over the same input with different filters,
  joined on `k` from the same column, becomes one aggregation with a `FILTER` per call and a
  `count(*) FILTER` per former aggregation to tell whether its group existed.
- **Also planned**: a report at the creation of a materialized view of the relations that are
  still read several times, to find the next shapes worth handling.
- Both sides must be the same stream at the same epoch: not for temporal joins (a scan `AS OF`
  is never a lookup), nor for a side behind a temporal filter or an emit-on-window-close delay.
- The work is matching two sub-plans up to a column mapping, looking through shared sub-plans.
- Seen in the fusion paper of Amazon Athena (rule "JoinOnKeys"), PostgreSQL 18
  (`remove_useless_self_joins`), Materialize (`redundant_join`).

### 7. Join common to the branches of a `UNION ALL` (added on 2026-11-25, done on the branch)

`(A ⋈ C) UNION ALL (B ⋈ C)` is `(A UNION ALL B) ⋈ C` for the inner, left outer, left semi and
left anti joins: variable `streaming_factor_union_join`, rule `CommonJoinOverUnionRule`. No key is
needed. `C` is computed once and its join state kept once.

- The branches that are merged join the same relation, with the same join type, on the same
  expressions of the relation; their other inputs are unioned with the number of their branch.
- The joined relation is the scan of a table (with its filters and projections, also through a
  shared sub-plan), or a *derived* relation: filters and projections over a sub-plan that is the
  same plan in every merged branch (typically the shared sub-plan of a view or of a subquery
  written twice: joins, aggregations, unions). A shared sub-plan is kept as it is, since it may
  have other consumers; the filters and projections above it may differ between branches.
- The derived relation must give the same rows wherever it is computed: tables read in the
  present (no `FOR SYSTEM_TIME AS OF`, no source), projections, filters, joins (not as-of),
  unions, values, project sets, and aggregations whose calls do not depend on the order of the
  rows (`count`, `sum`, `min`, `max`, `avg`, bit and bool aggregates); every expression pure and
  without `now()`. Kept: temporal filters, user-defined functions, `string_agg`, `array_agg`,
  `first_value`..., top-n and window functions (ties may be broken differently).
- What differs between branches moves around the single join: the rest of a join condition and
  the filter of the joined relation are checked in the join condition for the rows of the
  branch (an outer or anti join stays exact); the filter and projection above a join are applied
  per branch above. `NULL` keys and duplicates of the joined relation behave as in each branch:
  every row of the union meets the same rows of the relation as before, under the same
  condition.
- An expression of the joined relation that a branch computed below an outer join is still
  computed below the single join, so that it is `NULL` without a match.
- Several joins: the union of the other inputs of the single join, and the union of the merged
  branches with the others, are looked at again. `(A ⋈ C ⋈ H) ∪ (B ⋈ C ⋈ H)` becomes
  `((A ∪ B) ⋈ C) ⋈ H`, and two stacked anti joins common to the branches are both factored.
- A semi or anti join above the join of a branch, whose condition only uses the columns of one
  input `L` of that join, is first moved into `L`: `(L ⋈ X) ⋉ R = (L ⋉ R) ⋈ X`, as every row of
  the join carries the columns of one row of `L` unchanged. Allowed for `L` the left input of an
  inner, left outer, left semi or left anti join, or either input of an inner join; not for the
  side of a left join that is padded with `NULL`. The join below can then be the shared one,
  as in `(A ⟕ C WHERE NOT EXISTS (.. A ..)) ∪ (B ⟕ C)`. A branch read as written is preferred.
- Cost to keep in mind: one join state holds the rows of every branch, and the rows of the union
  are as wide as the widest branch. A semi or anti join moved down keeps the rows of `L` in its
  state instead of the rows of the join.
- Not done: an inner join in one branch and an anti join on the same key in another (one left
  join split on `IS NULL`); branches whose filters on the joined relation are disjoint and whose
  join keys differ (one join on a `CASE` key); a join made redundant by the same join above the
  union; joins that are common only after reordering the joins of a branch; a non deterministic
  relation shared by one sub-plan (both branches already read the same rows there, but it is
  kept on purpose).

### 8. Aggregate of a constant (added on 2026-11-25, done on the branch)

`min`, `max`, `first_value` and `last_value` over a group return the argument of one of its rows,
so a constant argument gives a constant result, also when the planner packs it with other values
into a `ROW(...)`. Removes the filter of the pattern "keep the group if its last operation is
the one constant operation there is". Part of `streaming_prune_empty_relation`.

### 9. Leftovers of subquery decorrelation (added on 2026-11-25, aggregating subqueries done)

Variable `streaming_simplify_subquery_unnesting`. A subquery in a select list, under `OR` or in a
`CASE` (`EXISTS`, a scalar aggregate, `ARRAY(...)`) is planned as a left outer apply. The rule
`PullUpCorrelatedPredicateAggRule` turns an aggregation correlated by equalities into a left join
with the aggregation grouped by the compared columns, but in the streaming plans of such
subqueries it never applied:

- the apply of a select-list subquery is marked "at most one row", the rule refuses the mark, and
  the rule that clears it comes after it in the same single pass;
- it refuses `count` (the count bug: `count` over an empty input is 0, the outer join pads with
  NULL), so every `EXISTS`, planned as `count(*) >= 1`;
- several subqueries of one select list are under one apply, joined with `FULL OUTER JOIN ON
  true`, and subqueries with the same correlated filter share it.

They went the general way: the outer side read again, its distinct correlated values aggregated
into a domain, a left join of the domain with the subquery, the aggregation, and a null-safe join
back. With the variable, a second pass of a streaming variant of the rule:

- accepts the mark and left outer applies only (a semi join over an aggregation without group key
  is always true, see below);
- gives the outputs that are not NULL over an empty input their value over an empty input with
  `CASE WHEN <group key> IS NOT NULL THEN <output> ELSE <value> END` (false for `EXISTS`, 0 for
  `count`, the empty array for `ARRAY`), when every aggregate call has a known value over an empty
  input and the apply has no condition;
- `ApplyOneRowJoinSplitRule` splits the apply of several subqueries into one apply per subquery
  when each returns exactly one row, and the variant looks through the share of their filter.

Kept in the general way: a correlated column in the aggregated expression, a correlation that is
not an equality, `IS NOT DISTINCT FROM` written in the subquery, a subquery that groups its rows,
an aggregate whose value over an empty input is not listed, laterals without a table (a set
returning function of the outer row only, which needs an outer `ProjectSet` RisingWave does not
have). Batch plans do not change.

Found on the way, fixed since (port of the upstream fix, see
`docs/fork/corrections/exists-scalar-aggregate.md`): in the existing rule, `WHERE EXISTS (SELECT
min(x) FROM r WHERE r.k = t.k)`, which is always true (an aggregation without group key returns
one row), was planned as a semi join on `r.k = t.k`, and `NOT EXISTS` of the same as an anti
join: the semi join dropped the rows of `t` without a match where the query keeps every row, the
anti join kept them where the query returns none. The rule checked the null-rejection of a filter
above the apply but not the join type of the apply; for a semi or anti apply it now requires the
condition of the apply to reject the row of NULLs of an empty input.

### US. Branches of a `UNION ALL` that read the same table (added on 2026-11-25, done on the branch)

Variable `streaming_merge_union_scans`, rule `UnionScanMergeRule`. Views often split one table into
several branches of a `UNION ALL`, each with its own filter and projection (one branch per status,
per role, per kind of row). Each branch scans and backfills the table again. The rule
reads it once:

```text
select a, x from t where p0            Project(CASE WHEN flag = 0 THEN a ELSE b END, ..)
union all                         =>   └─Filter(CASE WHEN flag = 0 THEN p0 WHEN flag = 1 THEN p1 END)
select b, y from t where p1              └─Expand(two copies of each row, flag = 0 and 1)
                                           └─Scan(t, predicate: p0 OR p1)
```

- Exact bag semantics, no assumption on the filters: a row gives one copy per branch, the copy
  of a branch is kept when the filter of the branch is true (`NULL` drops it, as a filter does),
  so a row that passes two filters gives two rows and a duplicate row gives duplicate rows. The
  filters need not be exclusive; with exclusive filters a single `CASE` without copies would
  do, but proving exclusivity is not needed for the gain, which is the scan.
- `LogicalExpand` (used for grouping sets and distinct aggregates, stateless) makes the copies,
  every subset keeping every column, so no `NULL` column is built. Its stream key is the key of
  the input and the flag, as the key of the union was the key of the branch and its number: the
  materialized view keeps the same number of hidden columns. Its output distribution is
  `SomeShard`, so an exchange follows where the union had one per branch.
- `CASE` evaluates the condition and the expressions of a branch on the copies of that branch
  only, so an expression that fails on rows its branch never sees is not evaluated on them. The
  conjuncts common to every branch and the `OR` of the others go into the single scan.
- Merged: branches of the form `(Project | Filter)*` over a scan of the same table, or over the
  same shared sub-plan (`Share`), whatever it computes (a join, an aggregation...): the shared
  sub-plan stays shared with its other consumers, its rows are expanded instead of its scans.
  Several groups of one union are merged separately; the other branches stay in the union.
- Not merged: an impure expression or `now()` in a branch (a temporal filter is a join with
  `Now` at this stage), a scan `AS OF`, a table with a watermark (a column taken from different
  columns per branch would lose the watermark that the union derived), a shared sub-plan that
  holds `now()`, a source or such a table.
- Shared (`UnionScanShareRule`, its own variable `streaming_share_union_scans`, applied after
  the merge): a table that is still
  scanned in at least two branches, below a join, a table function, an anti join or anything
  else, is scanned once in a new `Share`; each former scan becomes a filter and a projection of
  it with the predicate and columns of that scan, so every branch sees the same rows. The scans
  inside an existing shared sub-plan are left alone (already read once), and so are the scans
  of a table that only one branch reads (a self-join inside a branch). This removes scans and
  backfills, not joins: the share adds a fragment boundary where the scan was inlined, hence a
  separate variable (measured on real views: many scans fewer, but more fragments).
- Rows per branch: the `Expand` emits one copy per branch before the filter, a transient cost
  in the same fragment; nothing is kept in state.
- Not done: merging branches with a join into one join (the copies of an `Expand` joined once,
  the join gated by the branch number) or with different table functions into one `ProjectSet`:
  exact, but the gated join keeps the copies of every branch in its state. With
  `streaming_factor_union_join` on, the joins common to several branches are factored first and
  the branches of the union it builds are then merged here.

Not done, the "status ladder" under a top-1 per key (`row_number() OVER (PARTITION BY k ORDER BY
priority DESC) = 1` above a union of constant-priority branches):

- *Dominated branches.* Under the top-1 only the highest priority per key matters, so two
  branches over the same row could emit one row with a `CASE` on the priority, and an anti join
  `NOT EXISTS (r)` in a lower branch is useless when a higher branch emits every key of `r`.
  Correct only when the top-1 keeps nothing but the priority-determined columns, when ties
  cannot change the result, and when no other consumer sees the union; the rule would have to
  match the window, its partition, its order and the branches together. The merge above
  already removes the extra scans of such branches; what remains is rows and an anti join.
- *Default branch.* A lowest-priority branch `select k, 'init' from t` under a top-1 that is
  joined back to `t` on `k` could become a `LEFT JOIN` with `coalesce(status, 'init')`. This
  rewrites a join above the top-1 from a fact about a branch below it, and only holds when the
  joined relation is exactly the scan of that branch (same filters, `k` not `NULL`). Both are
  left for a dedicated rule after the prototype.

### 9b. Subqueries over set-returning functions of the outer row (added on 2026-11-25, done)

Variable `streaming_unnest_lateral_functions`, rules `LateralTableFunctionRule` and
`UnnestArrayOfTableFunctionRule`. A correlated subquery that reads no table, only columns of the
outer row through set-returning functions, went the general way: the outer side read again, the
distinct values of the correlated columns aggregated into a domain, the functions applied to the
domain, a left join of the domain with them (an aggregation for a scalar subquery), and an
`IS NOT DISTINCT FROM` join back. With the variable the subquery is evaluated on each outer row:

- `LEFT JOIN LATERAL (SELECT f(srf(t.a)) ...) ON true`, `t LEFT JOIN srf(t.a) ON true`: a
  `ProjectSet` over `t`. RisingWave has no outer `ProjectSet` (one that keeps the row, padded with
  NULLs, when the functions return no row) and none is added: the functions of a `ProjectSet` are
  zipped and the shorter ones padded with NULLs, so `generate_series(1, 1)` zipped with them gives
  exactly the padded row when they return none. A function that fails on a row returns no row in
  both forms, and the row is padded. A projected expression that is not NULL on the padded row
  (a constant, `WITH ORDINALITY`) becomes `CASE WHEN <not padded> THEN .. END`.
- `ARRAY(SELECT srf(t.a))`, `(SELECT max(x) FROM srf(t.a) x)`, `EXISTS`, `count`, `sum`,
  `string_agg`, ...: the same `ProjectSet`, then the aggregation grouped by the stream key of `t`
  (one group per row of `t`, its other columns carried with `internal_last_seen_value`; grouping
  by all its columns would keep hidden columns such as `_rw_timestamp`), each call filtered on the
  rows that are not padded, so that a row whose functions return nothing gets the value over an empty input. A
  row is known to be padded when every function never returns NULL (`jsonb_array_elements`,
  `regexp_matches`, `generate_series`, `jsonb_object_keys`, ...) or has a twin zipped with it
  that returns as many rows and never NULL: `generate_subscripts(a, 1)` for `unnest(a)` of a
  one-dimensional array, `jsonb_array_elements(j)` for `jsonb_array_elements_text(j)` (whose text
  of a JSON null is `''` today but NULL in PostgreSQL: a kept plan does not rely on it). `sum(x)`
  becomes `CASE WHEN count(x) > 0 THEN sum(x) END`: an update of the array retracts the old values
  of the group while the new ones come in, and a streaming `sum` whose non-NULL values were all
  retracted returns 0 instead of NULL.
- `unnest(ARRAY(SELECT srf(t.a)))` (or of `COALESCE(.., '{}')`) used only as a source of rows:
  `srf(t.a)`, no aggregation at all. A bag: the order of the array does not matter. Not when the
  array is also selected, when the function returns arrays (`unnest` flattens them), or when the
  row number of the `unnest` is read.
- `CROSS JOIN LATERAL (SELECT v, sum(v) OVER (ORDER BY o) FROM unnest(t.a) WITH ORDINALITY ...)`:
  an inner `ProjectSet` and the window partitioned by the stream key of `t`.

Example: `SELECT t.id, a.x FROM t LEFT JOIN LATERAL (SELECT (regexp_matches(t.s, 'A([0-9]+)'))[1]
AS x) a ON true` was a scan of `t`, a second scan of `t`, an aggregation (the domain), a
`ProjectSet` and a hash join; it is one scan and one stateless `ProjectSet`. An `ARRAY(..)` per row
was two scans, two aggregations for the domains, a `ProjectSet`, two joins and an aggregation; it
is one scan, a `ProjectSet` and one aggregation.

Kept in the general way: a condition in the subquery (`WHERE` over the function, or `ON` other
than true), a subquery that reads a table, `LIMIT` or `ORDER BY .. LIMIT`, `DISTINCT` aggregates,
aggregates other than `count`, `sum`, `min`, `max`, `array_agg`, `string_agg`, `jsonb_agg`,
`jsonb_object_agg`, a window in a left join, `unnest` of a nested array where the padded row
must be known, an aggregation or a window over an outer side without a stream key, user-defined
or other table functions. The functions are zipped with a twin only when the padded row must be
known (an aggregation, or a projected expression that is not NULL on it). Batch plans do not change.

Not done:

- **`(regexp_matches(s, p))[1]` without the `g` flag as the scalar `(regexp_match(s, p))[1]`.**
  Equivalent in PostgreSQL, where `regexp_matches` without `g` returns at most one row, but not in
  RisingWave: its `regexp_matches` returns every match whatever the flags (a known difference with
  PostgreSQL). The outer `ProjectSet` above keeps every match, as the original plan does.
- The functions are evaluated on each row rather than once per distinct value: equal for
  deterministic functions; for a `jsonb` that is equal to another but written differently
  (`1.0` and `1`), the general way could give one row the text of the other one, the per-row
  evaluation gives each row its own, as the batch query does.
- Found on the way, not changed: without the variable, the subqueries of one select list over a
  correlated value (`SELECT (SELECT count(*) FROM unnest(t.a)), EXISTS (SELECT 1 FROM unnest(t.a))
  FROM t`) are planned under a `FULL OUTER JOIN` of their results on the domain, followed by a
  filter `a IS NOT NULL OR a IS NOT NULL` that drops the NULL value of the domain: a row of `t`
  whose `a` is NULL gets NULL instead of 0, false or an empty array (seen in streaming plans,
  batch not checked). Each subquery alone is right. The per-row evaluation gives 0, false and
  the empty array.
- A relation joined with an aggregate of a set-returning function of itself on its key
  (`t LEFT JOIN (SELECT t.id, max(x) FROM t, srf(t.a) x GROUP BY t.id) ON ..`): the same per-row
  aggregation once it is recognised, which needs the key of `t` and is the self-join of idea 3.
- Found on the way, fixed (branch `proto/plan-reduction-batch-unnest`): the aggregation and the
  window grouped by the stream key of the outer side trusted that key, but during the logical
  optimization the stream key of a `UNION ALL` is the union of the keys of its branches; the
  number of the branch is only added when the plan becomes a streaming plan. `SELECT x.id,
  (SELECT max(v) FROM unnest(x.a) v) FROM (SELECT id, a FROM t UNION ALL SELECT id, a FROM t) x`
  was grouped by `id` alone: one row per `id` instead of two. An outer side with a `UNION ALL`
  that no aggregation or dedup re-keys now stays in the general way.

### 9c. A table joined back with an aggregation of itself on its key (added on 2026-11-25, done)

Same variable, rule `SelfAggregateJoinRule`, in a stage after the final column pruning. `T LEFT
JOIN (SELECT T2.k, agg(..) FROM T T2, srf(T2.a) x [LEFT] JOIN U ON .. WHERE .. GROUP BY T2.k) a
ON T.k = a.k` read `T` twice and joined it with the aggregation. The left side may be any plan
that carries a real row of `T` whose key is the compared column: projections, filters, inner
joins and the outer side of outer joins down to a scan of `T` or a share of a scan of `T`. When
`k` is the whole unique key of `T` (proven from the definition of the table, never assumed), the
group matched by a row of the left side holds the rows derived from the row of `T` it carries,
so the aggregation is computed where the left side reads `T`, grouped by the key of `T`, and its
columns are passed up to the join (at this stage the output of a join no longer holds the stream
keys of its sides, so the left side itself has no key to group by):

- the derived rows are computed on the rows of the left side, keeping every row of `T`: the
  outer `ProjectSet` of 9b, a filter becomes a flag on the rows, an inner join with another
  relation becomes a left outer join whose match is the flag (a column that its condition
  compares for equality is not NULL), a left outer join stays;
- the aggregation is grouped by the key, carries the columns of the left side, filters every
  call on the flag and counts the flagged rows;
- a row of `T` without flagged rows had no group: the right columns are NULL (left outer join) or
  the row is dropped (inner join). Every column of the group key must be computed from the row of
  `T` only.

One read of `T`, no join, one aggregation instead of one aggregation and one join. Kept: a group
key that is not the whole key, a column that is only unique in the data (a stream key elsewhere),
a group key over the rows of the function, a condition on the aggregation, another table on the
left side.


### SL. Status ladder picked with one `CASE` (added on 2026-11-26, done on the branch)

Variables `streaming_status_ladder_case` and `batch_status_ladder_case`, rule
`StatusLadderCaseRule`. A status ladder keeps, per key, the row of the branch with the highest
priority: `row_number() OVER (PARTITION BY k ORDER BY priority DESC) = 1` over a `UNION ALL` of
branches `SELECT k, '<status>', <priority> FROM ... WHERE ...`, planned as a top-n with limit 1
per group over the union.

- The branches that filter and project the same relation (a table, or the same shared sub-plan)
  with the same key become one branch: for each row, the literals of the first branch by
  priority whose filter holds (`CASE WHEN f3 THEN 'c' WHEN f2 THEN 'b' ELSE 'a' END`), and the
  rows where no filter holds dropped. Exact under the top-1: the candidates of one row all have
  the same key, and one of lower priority than another is never kept; a `NULL` filter falls
  through the `CASE` as it dropped the row; overlapping filters and duplicates need nothing
  more. One row per row of the relation instead of one per branch whose filter holds, and one
  read (the merge of union scans would read once too, but with one copy per branch).
- When that branch is the whole union and the relation is unique on the key (the key of the
  table, or the stream key of the shared sub-plan), each key has at most one row and the top-n
  is removed.
- Required: limit 1 without ties and offset, one order column, the priority; every column other
  than the group key is a literal in each merged branch; the priority is an integer literal;
  two merged branches with the same priority output the same literals (otherwise the top-n
  chose between them arbitrarily, and is left alone).
- Streaming: a branch with `now()` is a join with `Now` at this stage and stays a separate
  branch. Batch: `now()` is a constant, so it joins the `CASE`. The stage runs before the merge
  of union scans, so the `CASE` wins when both apply.
- Not done: replacing the top-1 by `max(priority)` and a `CASE` from priority to status (exact,
  but in streaming both keep every row of a group in their state, nothing saved); turning a
  default branch read from the entity table into a left join of that table (a join replaces a
  union branch, no operator saved).

#### SL, extension (2026-11-28): `GROUP BY` branches, joins, second pass

Why the first version missed most ladders: in the usual way of writing the idiom, each state is
its own view, and the views of one ladder rarely read the same relation as plain filters. A state
is typically "the keys that have an event of some kind" (`SELECT k FROM events WHERE kind = 'x'
GROUP BY k`, or `SELECT DISTINCT`, or a `UNION` of such views), "the keys whose balance satisfies
a condition" (a filter over an aggregation view), or a join of several tables; the ladder itself
is written with `row_number() ... = 1`, `DISTINCT ON` (both a top-1 per key) or `max(row(priority,
status))`. When two states read the same relation, it is usually through their own aggregation
or join, which the first version did not look into.

- *`GROUP BY` branches of the same relation.* Branches `(Project | Filter)* <- Agg(group key G) <-
  (Project | Filter)* <- base` whose bases are the same table or the same shared sub-plan and whose
  group keys are the same expressions of the base, the key of the ladder being made of group key
  columns and the other columns literals, become branches over one aggregation:

  ```text
  Agg(group: G, c_i: count(*) FILTER (WHERE f_i) for each branch i with its own filter f_i,
      the calls of branch i with FILTER (WHERE f_i) added)
  └─Filter(common conjuncts AND (f_1 OR f_2 ...))
    └─base
  ```

  and branch `i` keeps the groups where `c_i > 0`, with its own conditions above its
  aggregation (`HAVING`). Exact: a group has rows of branch `i` exactly when `c_i > 0`, and the
  calls of branch `i` see exactly the rows its own aggregation saw (a `NULL` filter drops the row
  from the `FILTER` as from the `WHERE`). The branches then read the same relation and the `CASE`
  of the first version applies; when they make the whole union and the key is the group key, the
  top-1 is removed. A group key column that is a literal (`SELECT DISTINCT k, 'x'`) is left out
  of the group key (it does not change the groups). State: one group per key of any of the
  branches in one aggregation, where each branch kept one group per key in its own, and one row
  per group in the top-1 instead of one per branch; one read of the base. Not merged: different
  group keys, a distinct call (one set of distinct values would serve several branches), a branch
  with `now()` or over a table with a watermark (as in the first version), an aggregation without
  group key.
- *Second pass after the sharing of widened copies* (streaming, with
  `streaming_share_identical_subplans`). Copies of the same join or aggregation that differ by
  their columns and filters (two state views over the same base view) are merged late into one
  shared core by `share_widened_subplans`, after this rule ran: each copy becomes `Filter(c_i > 0)
  <- Share(core)`. The rule runs again right after, and the branches over the core become one
  `CASE` (and the top-1 is removed when the core is the whole union, unique on the key).
- *Branches that join the same tables* (the user's case "every branch joins, but the joins could
  be shared"). With `streaming_factor_union_join` (`batch_factor_union_join`), whose stage runs
  before this rule, the join common to the branches is computed once above the union and the
  branches become filters of one join, which the first version already merges with a `CASE`:
  one join instead of one per branch. Nothing is added for this case. Copies of a join under a
  `GROUP BY` are merged by the second pass. State of the factored join, checked on synthetic
  plans: when the branches filter the same side (`d.cat = 'x'`, `d.cat = 'y'`), the disjunction is
  pushed below the join, which keeps the rows of that side that some branch reads, fewer than the
  copies together; when one branch filters one side and another the other side, each side is kept
  whole, as one of the copies already kept it. When every branch filters both sides
  (`d.cat = 'x' AND e.ok` and `d.cat = 'y' AND e.kind = 'q'`), the disjunction
  `(x AND ok) OR (y AND q)` stays above the join, and the join keeps both sides whole where the
  copies kept filtered rows: a possible regression of the factoring, not of this rule. The exact
  fix is to also push the implied per-side disjunctions `(x OR y)` and `(ok OR q)` (not done).
- Not done, and why:
  - branches over different relations (the most common case, each state from its own events): no
    exact rewrite saves anything in streaming. The top-1 keeps one row per candidate; a chain of
    left joins from the entity table keeps the same rows in its join states and adds operators,
    `max(priority)` keeps every candidate too, a count per (key, priority) adds a state.
  - removing a `GROUP BY` / `DISTINCT` directly under a branch (exact under the top-1, duplicates
    of a key give the same candidate): it removes an operator but the top-1 then keeps every
    duplicate, unbounded in general.
  - `max(row(priority, status))` as the top-1 (an aggregation instead of a top-n): rare, and the
    same merge would apply; not matched.
  - branches that join the same tables with different join types (inner in one, left in another):
    a left join with a "matched" flag would be exact, but no workload showed it.

### NK. Join on a column known to be `NULL` (added on 2026-11-26, done on the branch)

Variables `batch_simplify_null_join_keys` and `streaming_simplify_null_join_keys`, rule
`NullKeyJoinRule`. Lifts the limit of A "`IS NULL` on the key of an inner join is not simplified
when the filter was already pushed below the join". A filter `c IS NULL` on the output of a join
is moved by predicate pushdown next to the scan of the side that `c` comes from (and derived for
the other column of `c = b`); the join then compares a column that is `NULL` in every row of one
of its inputs:

```text
SELECT * FROM v WHERE b_id IS NULL      -- v: a JOIN b ON a.b_ref = b.id

Join(Inner, a.b_ref = b.id)              Values([])
├─Scan(a)                          =>
└─Filter(b.id IS NULL)
  └─Scan(b)
```

- Columns `NULL` in every row of a node: a `c IS NULL` conjunct of a filter or of a scan
  predicate (the predicate of a scan refers to the columns of the table, mapped to the output
  columns of the scan), followed up through filters, shares and projections of input references
  or of casts of them (streaming computes the cast of a join key in a projection below the join),
  unions (a column `NULL` in every input), joins (a column of a side keeps the facts of that side:
  a padded column is `NULL` too), and the operators that output the columns of their input as
  they are (limit, top-n, dedup, over window); a `NULL` literal of a projection. Nothing is
  derived through aggregations. Since 2026-11-28 (branch `proto/plan-reduction-nullkey-left`), a
  projection also outputs such a column for an expression that is `NULL` whenever the known
  `NULL` columns of its input are: a function that is `NULL` when an argument is (`Strong`:
  comparisons, arithmetic, casts...), or an expression of those columns and constants that
  evaluates to `NULL` once they are replaced with `NULL` (pure functions only, an error of the
  evaluation proves nothing), such as `(j ->> 'f')::int`.
- A conjunct `l = r` of the join condition whose operand is such a column, or a cast of it, on
  either side, whatever the type of the join: no pair of rows matches. Since 2026-11-28, any
  conjunct that is `NULL` whenever those columns are, as above (`l = r` with an operand computed
  from such a column, `b.id > a.x` with `a.x` known `NULL`). An inner or semi join
  becomes empty; a left outer or left anti join keeps its left side, padded with `NULL`s or as it
  is, and its right side becomes empty, also when the `NULL` column is on the left (preserved)
  side, whose rows can then match nothing; symmetrically for the right joins. A full join
  becomes a `UNION ALL` of its two sides, each padded with `NULL`s. The join is rewritten as A
  rewrites a never-true condition, then B and C remove what is above it (an aggregation without
  `GROUP BY` becomes its constant row).
- The preserved-side case is the one of a `not_null` test over a view `v = l LEFT JOIN r ON
  l.k = r.k` filtered on `l.k IS NULL`, where `l` is a union of branches: the filter is pushed
  into every branch, and the whole right side (aggregations, top-n, joins) is never read.
- Valid: `NULL = x` is `NULL` for every `x` and every type, so no pair of rows satisfies the
  condition, whatever its other conjuncts, duplicates or empty inputs. The facts are properties
  of the rows a filter or a scan can output: they hold for every row ever produced, under
  retractions, so they are valid for a plan frozen when a streaming job is created.
- Chained left joins (a `not_null` test over a view `l LEFT JOIN m ON l.k = m.id LEFT JOIN r
  ON (m.doc ->> 'f') = r.k` filtered on `l.k IS NULL`): the first join's optional side becomes
  empty and its columns are padded with `NULL`, so the key of the second join, computed from
  `m.doc`, is `NULL` too. Before 2026-11-28 only the first join was rewritten: the projection
  computing `m.doc ->> 'f'` over the padded column hid the fact, and the second join still read
  and shuffled its whole right side, its key folded to a `NULL` constant only later.
- Not rewritten: `IS NOT DISTINCT FROM` (matches `NULL`s), as-of joins, an operand that is
  not `NULL` when its column is (`coalesce(c, 0) = x` can match), a union where only some branches have the
  `NULL` fact, and a filter `s.k IS NULL` above a left join on its optional side (the anti-join
  pattern: the filter stays above the join and is not read).
- Streaming and watermarks: the rule leaves the join untouched if a sub-plan it would remove holds
  a scan of a table with a watermark, a source with a watermark or `now()` (the same guard as the
  other rules of A and B in streaming, see the limits of A). Without this, an inner join with a
  temporal join's left side carrying the watermark of `EMIT ON WINDOW CLOSE` would become an
  empty `VALUES` without a watermark, and the job could no longer be planned. A left join whose
  optional side is removed keeps the watermarks of its preserved side. A full join is left
  untouched in streaming if either side may carry a watermark.
- Stages: `STREAM_SIMPLIFY_NULL_JOIN_KEYS` right after `STREAM_PRUNE_EMPTY_RELATION`, and
  `BATCH_SIMPLIFY_NULL_JOIN_KEYS` at the start of `reduce_batch_on_keys` (after the first
  predicate pushdown, before join ordering), each with `EmptyRelationPropagateRule` and
  `EmptyInputSimplifyRule`, then column pruning if anything changed.
- Tests: planner tests `batch_simplify_null_join_keys.yaml`, `stream_simplify_null_join_keys.yaml`;
  differential tests `e2e_test/batch/simplify_null_join_keys` and
  `e2e_test/streaming/simplify_null_join_keys` (NULL keys on both sides, duplicates, empty tables,
  updates of keys to and from `NULL`, deletes; chained left joins on a JSONB field or a strict
  expression of a padded column, a non-equality conjunct, and the kept `coalesce` / `IS NULL`
  expressions).

### NV. Narrow values of `first_value` / `last_value` (added on 2026-11-26, done on the branch)

Variables `batch_narrow_value_aggregates` and `streaming_narrow_value_aggregates`, rules
`NarrowValueAggRule` and `NarrowValuePushRule`. A "latest version per key" relation keeps a whole
value (a JSONB document, a wide row) per group, but is often read through a few fields only. The
aggregation keeps the whole value: in memory in batch, in the state table in streaming, and the
joins, exchanges and unions below it carry it too.

```text
SELECT count(*) FROM v WHERE fact->>'dir' IS NULL
-- v: SELECT id, last_value(fact ORDER BY ts) AS fact FROM t GROUP BY id

Agg(count)                                   Agg(count)
└─Filter((last_value(fact)->>'dir') IS NULL) └─Filter(last_value($1))
  └─Agg(id; last_value(fact ORDER BY ts)) =>   └─Agg(id; last_value($1 ORDER BY ts))
    └─Scan(t)                                    └─Project(id, ts, (fact->>'dir') IS NULL AS $1)
                                                   └─Scan(t)
```

- `NarrowValueAggRule`: `f(first_value(x ORDER BY k))` becomes `first_value(f(x) ORDER BY k)`, the
  same for `last_value` and `internal_last_seen_value`, when the result of the call is only read
  through such functions, by a projection or an aggregation (in its `FILTER` clauses) above it,
  through filters, limits and projections that only select columns. The largest sub-expressions
  that read the result only through allowed functions are moved; the rest stays above. Several
  of them become one aggregate of `ROW(f1(x), f2(x), ...)`, whose fields are read above.
- `NarrowValuePushRule`: a projection that reads a wide column of a `UNION ALL` only through
  allowed functions computes them in each branch; one that reads a wide column of one side of a
  join (not compared by the join) only through them computes them in that side. Only on the paths
  of value aggregates: the column is the result of a value aggregate below (through filters,
  column selections, allowed functions, unions and joins), or the projection is the input of an
  aggregation and the column feeds the argument of a value aggregate. Other projections over
  joins are left as they are, so that the plans without value aggregates do not change. A lower
  `first_value(j)` in a branch, under a `CASE` over `j`, is then narrowed in turn, and the join
  below it carries the narrow value. `JoinProjectTransposeRule` keeps such a projection below the
  join once the rules applied to the plan (it would otherwise pull it above again).
- Allowed functions: deterministic, pure and unable to fail, because they are evaluated on every
  input row instead of the selected one: `->`, `->>`, `#>`, `#>>`, `jsonb_extract_path(_text)`,
  `jsonb_typeof`, `?`, `?|`, `?&`, `@>`, `<@`, field access, `ROW`, comparisons, `IS [NOT] NULL / TRUE / FALSE / DISTINCT
  FROM`, `AND`, `OR`, `NOT`, `CASE`, `COALESCE`, casts to text from scalar types whose text does
  not depend on the session, widening numeric casts. A cast that may fail (`(x->>'n')::int`) or
  `jsonb_array_length` stays above, the field below it is moved.
- Valid: the row that the aggregate picks depends on the order key (and on ties on the input
  order in batch, on the stream key in streaming), never on the value, and `f` of the value of
  the picked row is `f` applied to the picked row, `NULL` included. All the fields come from one
  aggregate of a row, so they come from the same row even when the order key ties. An aggregate
  that may see no row (no group key, or a `FILTER` clause) returns `NULL`, and the query applies
  `f` to it: then only expressions that are `NULL` on `NULL` are moved (`IS NULL` and `COALESCE`
  stay above). The same holds for the padded side of an outer join. Retractions are handled by
  the aggregate as before: it keeps its rows ordered by the order key, with the narrow value.
- Not rewritten: `DISTINCT` aggregates, a value that is also its order key, a value read as it is
  (output, grouped by, compared in a join), through a function outside the list, or of a narrow
  type (only JSONB, text, bytea, structs, lists and maps are narrowed). `IGNORE NULLS` is not
  allowed in aggregates. A right side of a join that is a scan the join may look up (the first
  column of its key or of an index is compared) is left alone, so that a lookup join stays
  possible; a temporal join's right side too.
- Stage: after the column pruning that precedes `SIMPLIFY_AGG` (so that `UnifyFirstLastValueRule`
  packs the narrowed calls), in rounds: each round is followed by column pruning and a merge of
  the projections (`NarrowValueProjectMergeRule`, which also merges when a computed column is
  read several times, unless it is impure), so that the next round sees one level further down.
- Tests: planner tests `batch_narrow_value_aggregates.yaml`, `stream_narrow_value_aggregates.yaml`;
  differential tests `e2e_test/batch/narrow_value_aggregates` and
  `e2e_test/streaming/narrow_value_aggregates` (NULL values and order keys, missing fields,
  fields a cast cannot read in older rows, ties, unions, joins, a `CASE` over a lower
  `first_value`, updates and deletes of the last rows, emptied tables).

### NE. Null-safe equality on a non-null operand (added on 2026-11-28, done on the branch)

Variables `batch_null_safe_eq_to_eq` and `streaming_null_safe_eq_to_eq`, rule
`NullSafeEqToEqRule`. Subquery decorrelation joins the outer side with the subquery, and with the
domain, on `IS NOT DISTINCT FROM` the correlated columns. Several rules refuse or penalize such a
key (`RepeatedLookupMergeRule`, `NullKeyJoinRule`, the column facts protect it), and a hash join
on a null-safe key keeps the rows whose key is `NULL`, which can match. When one operand is never
`NULL`, the condition is a plain equality:

```text
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.id LIMIT 1)

Join(LeftSemi, t.id IS NOT DISTINCT FROM u.k)       Join(LeftSemi, t.id = u.k)
├─Scan(t)                                     =>    ├─Scan(t)
└─GroupTopN(u.k, limit 1)                           └─GroupTopN(u.k, limit 1)
  └─Filter(u.k IS NOT NULL)                           └─Filter(u.k IS NOT NULL)
```

- Where: the top-level conjuncts of a join condition (any join type but as-of), of a filter and
  of a scan predicate. Never an operand of `NOT`, `OR`, `CASE`, nor a projected expression.
- Never `NULL`: what the column facts of the input say (E: `NOT NULL` columns, constants, a filter
  below that rejects `NULL`, counts), a non-null literal, and the facts of
  the other conjuncts of the same predicate (`a.y = b.id AND a.y IS NOT DISTINCT FROM b.k`). For a
  join condition, the facts of each side, whatever the type of the join: the condition is
  evaluated on rows of the inputs, before padding.
- Valid: both forms are the same comparison when both operands are not `NULL`; with exactly one
  `NULL`, `IS NOT DISTINCT FROM` is `FALSE` and `=` is `NULL`, and a top-level conjunct of a
  predicate evaluated with filter semantics rejects the row (or the pair) in both cases; both
  `NULL` can not happen. The facts hold for every row ever produced, under retractions, so the
  rewrite is valid for a frozen streaming plan. Duplicates are not involved.
- A primary key column is not taken as never `NULL`: RisingWave does not declare it `NOT NULL`
  (`id INT PRIMARY KEY NOT NULL` does).
- Not rewritten: a conjunct with `now()`, a comparison that would need an implicit cast, an
  operand that is an expression that may fail or be `NULL` (a cast to a wider type is not proven
  non-null), both operands nullable.
- Stages: `NULL_SAFE_EQ_TO_EQ` (bottom-up) right after the first predicate pushdown, before join
  ordering, in both pipelines.
- Tests: planner tests `batch_null_safe_eq_to_eq.yaml`, `stream_null_safe_eq_to_eq.yaml`;
  differential tests `e2e_test/batch/null_safe_eq_to_eq` and `e2e_test/streaming/null_safe_eq_to_eq`
  (NULLs on both sides, duplicates, outer and full joins, `EXISTS` / `NOT EXISTS` / scalar
  subqueries through the domain, updates to and from `NULL`, emptied tables).

### SE. Simplification of the subquery of `EXISTS` (added on 2026-11-28, done on the branch)

Variables `batch_simplify_exists_subquery` and `streaming_simplify_exists_subquery`, rule
`ExistsSubquerySimplifyRule`, after PostgreSQL's `simplify_EXISTS_query`. People write
`EXISTS (SELECT 1 ... LIMIT 1)` or `EXISTS (SELECT DISTINCT ...)`; with the limit or the
aggregation, the simple unnesting does not apply, and the general one reads the outer side again
for the domain and keeps a top-n or an aggregation per correlated value:

```text
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.k ORDER BY u.ts DESC LIMIT 1)

Join(LeftSemi, t.k IS NOT DISTINCT FROM u.k)        Join(LeftSemi, t.k = u.k)
├─Scan(t)                                     =>    ├─Scan(t)
└─GroupTopN(u.k; u.ts DESC; limit 1)                └─Scan(u)
  └─Scan(u)
```

- Where: the right side of a correlated left semi or anti apply (`EXISTS`, `NOT EXISTS`, `IN`,
  `NOT IN` in a `WHERE` conjunct), through projections only, before the simple unnesting.
- `EXISTS` / `NOT EXISTS` (the condition of the apply does not read the subquery): a limit or a
  top-n (with or without ties) of at least one row without offset is removed: the result is not
  empty iff its input is not empty.
- All four: an aggregation without aggregate calls and with a group key (`DISTINCT`, `GROUP BY`
  without aggregates) becomes the projection of its group key: the same set of rows. A semi or
  anti join only tells whether a matching row exists, so it depends on the set of rows of its
  right side, whatever the condition; projections keep both properties.
- Not removed: `LIMIT 0` (an empty relation, see B), an offset, a limit under `IN` (it chooses the
  compared values), an aggregation with calls or without group key (a scalar aggregation returns
  one row; `EXISTS` over it is left to the correctness work on that case), anything under a filter
  (`HAVING`, a `WHERE` above a limited derived table), a limit or an aggregation right above a
  set-returning function of the select list (as PostgreSQL), an `EXISTS` elsewhere than in a
  `WHERE` conjunct (planned as a count), an uncorrelated subquery (not decorrelated; its limit
  saves work).
- Valid for every row of the outer side and every content of the tables, hence for a frozen
  streaming plan; duplicates and retractions are covered by the set argument.
- Streaming gain: the domain, its join and the group top-n or aggregation with their states are
  gone; the semi join keeps the subquery rows that the join with the domain kept anyway.
- Stage: `SIMPLIFY_EXISTS_SUBQUERY` at the start of `subquery_unnesting`, in both pipelines. The
  projections left above a removed operator are merged, unless an impure expression or a function
  call would be evaluated several times.
- Tests: planner tests `batch_simplify_exists_subquery.yaml`,
  `stream_simplify_exists_subquery.yaml`; differential tests
  `e2e_test/batch/simplify_exists_subquery` and `e2e_test/streaming/simplify_exists_subquery`
  (several rows per key with ties, NULL keys and values, duplicates on the outer side, `NOT IN`,
  updates and deletes, emptied tables).

### RO. Outer join reduction (added on 2026-11-28, done on the branch)

Variables `batch_reduce_outer_joins` and `streaming_reduce_outer_joins`, rule
`OuterJoinReduceRule`, after PostgreSQL's `reduce_outer_joins`. Predicate pushdown turns an outer
join into an inner one with `LogicalJoin::simplify_outer`, which only recognizes a comparison
(`=`, `<`, ...) with a plain column of the padded side among the conjuncts of the filter above.
Two reductions are missing:

```text
SELECT t.*, r.name FROM t LEFT JOIN r ON t.k = r.id WHERE r.c + 1 > t.x
Filter(r.c + 1 > t.x)                         Filter(r.c + 1 > t.x)
└─Join(LeftOuter, t.k = r.id)           =>    └─Join(Inner, t.k = r.id)

SELECT t.* FROM t LEFT JOIN r ON t.k = r.k WHERE r.k IS NULL
Filter(IsNull(r.k))                           Project(t.*, NULL)
└─Join(LeftOuter, t.k = r.k)            =>    └─Join(LeftAnti, t.k = r.k)
```

- Outer to inner (or full to left / right): a conjunct of the filter right above the join, or of
  the condition of a join whose input the outer join is and which does not keep the rows of that
  input that match nothing (inner and semi joins, the optional side of an outer join, the right
  side of a left anti join), can not be `TRUE` when all the columns of a padded side are `NULL`.
  The test: `Strong` (the expression is `NULL`; except over `greatest` / `least`, which skip
  `NULL`s in RisingWave although `Strong` lists them as strict), `FALSE`, `AND` with one such
  conjunct, `OR` with all, `IS TRUE`, `IS NOT NULL`, `NOT (e IS NULL)`, `IN`, `IS NOT DISTINCT
  FROM` a non-`NULL` constant. The strict functions that `Strong` lists (arithmetic, casts,
  `LIKE`, `trim`, `extract`...; not `lower` / `upper`, which it does not list), `IS NOT NULL`,
  `IN`, `OR` and the conditions of a join above are now recognized. Valid: every padded row is rejected, so the join need not
  produce it; an identity for every content of the inputs, hence for a frozen streaming plan.
- Outer to anti: a top-level conjunct `r.c IS NULL` above a left join where `r.c` is never `NULL`
  in a matched row: the column facts of the right input say so (a `NOT NULL` column, a count, a
  filter below), or the join condition rejects a `NULL` `r.c` (`t.k = r.c`). The filter then
  keeps exactly the unmatched left rows, each once: a left anti join, the right columns output as
  `NULL`, the other conjuncts kept above (evaluated on those `NULL`s), the `IS NULL` conjuncts of
  the right side removed. Symmetric for a right join. For a full join only the column facts
  count (the unmatched right rows keep their own `r.c`), and the result is the anti join of the
  other side.
- Not rewritten: conjuncts with `now()`, as-of joins, an anti join over a temporal join (`FOR
  SYSTEM_TIME AS OF`), `r.c IS NULL` on a column a matched row may hold `NULL` in (a nullable
  column outside the condition; a column only compared with `IS NOT DISTINCT FROM` or inside
  `coalesce`), `IS NULL`, `IS NOT TRUE`, `coalesce(r.c, 0)`, `CASE`, and a join above that keeps
  the unmatched rows of the outer join (a left join on its preserved side, an anti join).
- Streaming gain: an inner join has no degree table and emits no padded rows to retract when a
  match appears; it enters the multi-join and the key rules. An anti join keeps only the key
  columns of the right side, and the matched pairs (the fan-out) are no longer emitted to be
  filtered out.
- Stages: `REDUCE_OUTER_JOINS` (top-down) right after the first predicate pushdown, before
  `NullSafeEqToEqRule` and join ordering, in up to three rounds, each followed by predicate
  pushdown (a reduced join lets conjuncts through to the joins below).
- Join order: before join ordering, an outer join that would become inner while its padded side
  is itself an inner join (below projections and filters) is not reduced; a second pass
  (`REDUCE_OUTER_JOINS_AFTER_JOIN_ORDERING`, same rounds) reduces it after join ordering, where it
  keeps its place. Reduced before, the inner join of the padded side was merged into the join
  ordering of the joins above and ordered again, losing its order and its distribution (often on
  the keys of the outer join, as after the unnesting of a correlated subquery): one more exchange
  per such join on the upstream case "improve multi-scalar subqueries optimization time". An outer
  join over a padded table or aggregation is still reduced before join ordering, which may then
  move it.
- Tests: planner tests `batch_reduce_outer_joins.yaml`, `stream_reduce_outer_joins.yaml`;
  differential tests `e2e_test/batch/reduce_outer_joins` and `e2e_test/streaming/reduce_outer_joins`
  (NULL keys and values on both sides, duplicates on both sides, left, right and full joins, joins
  above, updates to and from `NULL`, emptied sides).

### 5. Aggregation pushed below a join onto the unique side

`SELECT f.cust_id, c.name, sum(f.amt) FROM f LEFT JOIN c ON f.cust_id = c.id GROUP BY f.cust_id,
c.name` becomes `(SELECT cust_id, sum(amt) FROM f GROUP BY cust_id) LEFT JOIN c ...`.

- No operator is removed, but the state of the join drops from one row per fact to one row per
  group, and an update of the dimension becomes one change instead of one per fact.
- Yan and Larson, "Eager Aggregation and Lazy Aggregation", VLDB 1995; Calcite
  `AggregateJoinTransposeRule`; PostgreSQL eager aggregation (`setup_eager_aggregation`);
  Materialize (`reduction_pushdown`).
- Status on 2026-11-28: done on the branch `proto/plan-reduction-agg-join`, rule
  `AggJoinTransposeRule` (`optimizer/rule/agg_join_transpose_rule.rs`).

**Exact form** (`streaming_push_agg_below_join`, `batch_push_agg_below_join`, default false). `A` is
the aggregated side, `B` the other one.

```text
Agg(group f.k, d.name: sum(f.x), count(*))       Project(f.k, d.name, s, n)
└─Join(LeftOuter, f.k = d.id)              =>    └─Join(LeftOuter, f.k = d.id)
  ├─f                                              ├─Agg(group f.k: s = sum(f.x), n = count(*))
  └─d, with the key (id)                           │ └─f
                                                   └─d
```

- Join: inner, or outer with `A` preserved (left join with `A` on the left, right join with `A` on
  the right). An unmatched row of `A` is then in the output once, padded with `NULL`s, before and
  after. Not full, semi, anti, as-of or temporal joins.
- `B` is unique on the columns that the condition binds (`is_unique_on_compared_columns`, the
  unique keys of idea 4): a row of `A` meets at most one row of `B`.
- The new group key is the group keys from `A` and the join columns of `A`. Every join column of
  `A` is a group key of the aggregation, or (inner join only) is compared with `=` or
  `IS NOT DISTINCT FROM` to a column of `B` that is one (`GROUP BY d.id` over `f JOIN d ON f.k =
  d.id`): the groups of the aggregation are then exactly the new groups, with the same rows of
  `A`. A conjunct that reads a column of `A` otherwise than in such an equality must read group key
  columns of simple types (two equal values are identical).
- Every group key is a deterministic expression of one side; every aggregate reads `A` only
  (arguments, `FILTER`, `ORDER BY`), or is `min`, `max` or `internal_last_seen_value` of a column of
  `B` without `FILTER` (constant over a new group, emitted from `B` above the join).
- Then every row of a new group meets the same row of `B`, or none: an inner join keeps or drops a
  whole new group, so every aggregate (including `DISTINCT`, ordered and user-defined ones) gives
  the same result before and after the join. The group key must not be empty (an empty input
  still gives one row, which an inner join could drop). `NULL` keys: a `NULL` join column matches
  nothing with `=`, for the whole new group, as for its rows before.
- Skipped when `A` is unique on the new group key (nothing to reduce). Streaming: skipped when a
  side reads a source or a table with a watermark (the aggregation of a job that emits on window
  close is not moved below a join). Batch: an inner join only when `B` reads a whole table (a
  filtered `B` may drop most rows of `A`, which aggregating first would read and group).
- Applied top-down: the moved aggregation is moved again below the next join (a star of
  dimensions). Streaming: after the other reductions, before the merge of stacked aggregations.
  Batch: after join ordering.
- Stream key: the materialized view keeps the group key as its key, but the key of `B` may be
  added as hidden columns (the key of the join above the aggregation).
- Expressions computed between the aggregation and the join are computed on the rows of `A`
  before the join: below an inner join, also on rows that the join drops (as PostgreSQL's eager
  aggregation does), so an expression that fails on such a row now fails the query.

**Partial form** (`batch_partial_agg_below_join`, batch only). Same conditions without the
uniqueness of `B` and without the group conditions; the aggregates of `A` must decompose (`count`,
`sum` of an integer, decimal or interval, `min`, `max`, `bool_and`, `bool_or`, `bit_and`,
`bit_or`; not `DISTINCT` `count` or `sum`, ordered, ordered-set, `first_value`, `string_agg`,
user-defined aggregates, nor `sum` of a float, whose rounding depends on the order of the
additions). The pushed aggregation computes them per new group; the final one, above the join,
combines them with the same result type (`sum` of `sum`, `sum0` of `count`, `min` of `min`...;
`sum(int)` is a `bigint` at both levels, as in the two-phase aggregation). A row of the pushed
aggregation stands for all the rows of its new group, which meet the same rows of `B`: summing its
partial sum once per row of `B` it meets is summing each of its rows once per row of `B`. `min`
and `max` of `B` are computed above, as repeated rows do not change them; other aggregates of `B`
stop the rule. Chosen by cost with the learned ratios (`batch_cardinality_feedback_learning`): the
largest group ratio learned for the pushed aggregation must be at most 0.5 below an outer join, 0.1
below an inner one (which may drop most rows of `A`); nothing learned, nothing pushed.

Not done: semi joins (an aggregation over `A` semi-joined on group columns could also run first),
the partial form in streaming (rejected: it adds an operator and state), and a cost for the exact
form below an inner join with a filtered `B`.

Tests: planner tests `stream_push_agg_below_join.yaml` and `batch_push_agg_below_join.yaml`
(rewritten: left, inner, right joins, a key of `B` in the group key, two dimensions, distinct,
ordered and filtered aggregates, `min`/`max` of `B`, `DISTINCT`, computed group keys, `HAVING`,
non-equi conditions on grouped columns, null-safe equality; kept: `B` not unique, join column not
grouped, aggregates of `B` or of both sides, full joins, `A` optional, no group key, non-equi
conditions on other columns or on a decimal, `A` unique on the group key, watermarks, temporal
joins, mixed group keys, volatile expressions, a filtered dimension in batch; partial form with
and without learned ratios, distinct counts, float sums, sums of `B`, learning off), and the
differential tests `e2e_test/streaming/push_agg_below_join.gen.py` (materialized with and without
the variable, compared with each other and with the batch query after the backfill and after
inserts, updates, deletes, an emptied and refilled dimension and an emptied fact table) and
`e2e_test/batch/push_agg_below_join.gen.py` (with UA below).

### 6. Merge of two stacked aggregations

`SELECT region, sum(s) FROM (SELECT region, city, sum(x) s FROM t GROUP BY region, city) GROUP BY
region` becomes `SELECT region, sum(x) FROM t GROUP BY region`.

- Sound when the upper key is a subset of the lower key, for `SUM` of `SUM`, `SUM` of `COUNT`,
  `MIN` of `MIN`, `MAX` of `MAX`. Not `AVG` of `AVG`, not `COUNT` of `COUNT`. Not when the lower
  aggregation is shared or filtered on its results.
- Two levels are sometimes written on purpose against skew: keep it optional.
- Calcite (`AGGREGATE_MERGE`), CockroachDB (`FoldGroupingOperators`).

**Done on the branch `proto/plan-reduction-shared-aggs`** (added on 2026-11-25): variable
`streaming_merge_stacked_aggregations`, rule `StackedAggMergeRule`, a bottom-up stage after the
other stages of the prototype (three levels merge into one).

- Matched: an aggregation over an aggregation, with at most a projection of columns in between,
  whose group key is made of inner group key columns. The inner group key must not be empty: an
  outer group then exists exactly when an input row has its key, in both plans. No grouping sets.
  A `HAVING` of the inner aggregation (a filter in between) or a shared inner aggregation stops
  the rule.
- Outer aggregates over an inner aggregate (no `DISTINCT` on the sums and counts, no `FILTER` or
  `ORDER BY` on the outer one; the inner `FILTER` is kept):
  - `sum` of `sum` of an integer or a decimal: `sum` of the argument cast to the type of the inner
    sum, so that type and overflow are those of the stacked sums (`sum(int)` is `bigint`, the sum
    of it `numeric`). Not floats: the order of the additions would change the rounding;
  - `sum` of `count`: the count, cast to `numeric`. Not without outer group key: over an empty
    input the outer sum is `NULL` and the count would be 0;
  - `min` of `min`, `max` of `max`, `bool_and` of `bool_and`, `bool_or` of `bool_or`: the same
    aggregate. `NULL`s are ignored at both levels; all-`NULL` gives `NULL` at both levels.
- Outer aggregates over an inner group key column `v`, i.e. a dedup below the outer aggregation
  (the "duplicate-insensitive context" below):
  - `count(DISTINCT v)`, `sum(DISTINCT v)`: the same over the input;
  - `count(v)`, `sum(v)`: `count(DISTINCT v)`, `sum(DISTINCT v)` when the inner group key is within
    the outer group key and `v`, so that each inner group is one distinct `v` of its outer group
    (`SELECT g, count(v) FROM (SELECT DISTINCT g, v FROM t) GROUP BY g`). The deduplication state
    of the distinct aggregate holds the same rows as the state of the removed aggregation;
  - not `min`/`max` of `v` (same values, but their state would hold every input row instead of
    every distinct value), not `count(*)` (it counts inner groups);
  - no aggregate at all: `DISTINCT` of `DISTINCT`, `GROUP BY` on fewer columns.
- Kept: `avg`, `count` of `count`, `max` of `min`, `sum(DISTINCT)` of `sum`, an outer group key on an
  aggregate, `count(v)` over a dedup on more columns than the outer key and `v`.
- Added in a second round:
  - an outer group key computed by a projection from inner group key columns only (`GROUP BY g +
    w` above `GROUP BY g, w, v`) is computed from the same columns of the input;
  - through a `UNION ALL` (rule `StackedAggOverUnionMergeRule`): an aggregation over the union of
    branches that aggregate on (a superset of) the outer group key becomes one aggregation over the
    union of the rows of the branches. A branch that is such an aggregation contributes, per input
    row, the argument of its aggregate (cast to the type of the aggregate, `NULL` where its
    `FILTER` does not hold), or `1`/`0` for a count; any other branch (rows, an aggregation with a
    `HAVING`) contributes its rows as they are, which the outer aggregation reads one by one
    anyway. Outer aggregates: `sum`, `min`, `max`, `bool_and`, `bool_or`, with the same pairs as
    above. At least one aggregation must go away.
- Why the shapes of real view definitions rarely matched the first version: two levels are
  usually separated by a `UNION ALL` of per-source aggregations, by a projection that computes the
  outer group key, or by a `HAVING`; a column pruning also leaves one projection per level. The
  first two are handled now; a `HAVING` stays (the filter reads aggregates), and an aggregation
  above a join of an aggregation is the eager/lazy aggregation of idea 5, not a merge.
- Not done, recorded:
  - `sum(coalesce(sum(x), 0))` is `coalesce(sum(x), 0)` when the outer group exists: exact, not
    done yet;
  - a dedup below a top-1 per key (`row_number() = 1` over a `UNION` or a `DISTINCT` whose rows
    only carry the key and constants): the top-1 picks the same row from the set of distinct
    rows, but its state holds every input row, where it held every distinct row, and ties between
    branches are resolved by the stream key, which changes. A trade of state, not a reduction;
  - a dedup on the build side of a semi or anti join: same trade, the join state then holds
    every row;
  - several aggregations of the same input and group key with different filters, joined on the key
    (one aggregation with a `FILTER` per call and a `count(*) FILTER` per former aggregation to
    tell whether its group existed): see the "next for the aggregations" of idea 3, it needs the
    join matching of that rule;
  - a scalar aggregation over an input of at most one row (a filter on the whole key of a table)
    keeps its two phases: a different rule (cardinality, not stacking).
- In streaming the merged aggregation is an ordinary aggregation of the input; retractions and rows
  moving between groups need nothing special. The known `sum` quirk (0 instead of `NULL` once the
  non-`NULL` values of a group are retracted) can differ in place between the two plans: the
  differential test wraps such sums in `coalesce`.
- Found on the way, not changed (without the variable): `SELECT sum(c) FROM (SELECT g, count(*) AS
  c FROM t GROUP BY g)` returns 0 in streaming once `t` is emptied, `NULL` in batch (the two-phase
  plan sums the counts with a sum that does not return to `NULL`).

### 10. Sharing again the sub-plans that became identical (added on 2026-11-25, done on the branch)

The common sub-plan sharing runs at the beginning of the streaming optimizer, before predicate
pushdown, subquery unnesting and column pruning. Sub-plans that only become identical after them
are computed twice: a join written in a view and filtered above it in one place, and written with
the filter in another; an aggregation of a view whose other aggregates are pruned, and the same
aggregation written in the query; the domains of two subqueries correlated on the same column.

Variable `streaming_share_identical_subplans`, function `share_identical_subplans`, run last in
the streaming optimizer (after the stacked aggregations): the rules above need one plan per
consumer and do not rewrite a shared sub-plan.

- Same structural equality as the early sharing (column names ignored). Deterministic: the first
  copy in a depth-first traversal becomes the shared one.
- A share is a fragment boundary (a no-shuffle exchange to each consumer). Only sub-plans with a
  stateful operator (join, aggregation, top-n, dedup, over window) that read a table or a source
  are shared: removing a copy removes its state, actors and backfills. A chain of scan, filter and
  projection holds no state; sharing it trades one backfill for one more fragment and hop between
  the scan and every consumer, so it is left duplicated, as the early sharing leaves a bare scan.
- The shares that existed before are kept as they are, and a plan without a new duplicate is
  returned unchanged.
- Found on the way, not changed (with and without the variable): `SELECT a.id, (SELECT max(v)
  FROM h WHERE h.sid = a.k), (SELECT count(*) FROM h WHERE h.sid = a.k AND ...) FROM a` gives
  `NULL` instead of 0 for the count of a row whose `a.k` is `NULL` in streaming: the two domains
  are full-outer-joined, filtered on `a.k IS NOT NULL`, then joined back with `IS NOT DISTINCT
  FROM`, so the row finds no match. Batch returns 0.
- **Widened sharing** (second round, same variable, `share_widened_subplans`, run before the
  sharing of identical sub-plans): copies rooted at a join or an aggregation that differ by their
  columns, or by filters that commute with them, are aligned node by node into one *core*:
  - projections: the core keeps every column of its input and computes the expressions of every
    copy once;
  - filters and scan predicates: the core keeps the conjuncts of every copy; the other conjuncts
    of a copy are *residual* and are lifted: through an inner join, the preserved side of an outer
    join, the left side of a semi or anti join, and an aggregation when they only read group key
    columns. Not through the optional side of an outer join: those copies are not merged;
  - below an aggregation, residual conjuncts that read other columns become a `FILTER` on the
    calls of that copy, and the copy keeps the groups where `count(*) FILTER (WHERE ...)` is
    positive, the groups its own input had. This merges several aggregations of the same input
    and group key that differ by their filters. Not for aggregations without group key: they are
    typically read by a dynamic filter, which needs a single column, and the projection that
    selects it from the shared core is merged into the join above (planning then fails on a
    nested-loop join);
  - joins must have the same type and condition, aggregations the same group key (calls are
    united), scans the same table; other nodes must be equal.
  Each copy becomes `Project <- Filter(residual) <- Share(core)`, then column pruning narrows the
  core to what its consumers read. The joins of the consumers stay: two consumers joining the
  merged aggregation on the same key still join it twice (idea 3 removes such repeated lookups of
  a table, not of a shared sub-plan).
  Cost: a lifted filter no longer reduces the state of the join or aggregation of the core.
- Third round, join order:
  - the sides of an inner join commute: the shape of an inner join sorts the shapes of its sides,
    and a copy whose sides are swapped with respect to the first copy is swapped back (same
    output columns) before the alignment;
  - a copy whose inner joins contain the leaves of the inner joins of another copy is reordered:
    the matched leaves are joined first, in the tree of the other copy, then the other leaves.
    Every conjunct of the inner joins and of the filters between them is placed at the lowest join
    that has all its columns, which is exact for inner joins;
  - `(x JOIN y) LEFT JOIN z` becomes `x JOIN (y LEFT JOIN z)` when the condition of the left join
    reads `y` and `z` only and `y` matches the left side of another copy's left join: the
    conditions of the inner join never read `z`, so they hold above the left join as well;
  - a copy is reordered only when the exposed join merges with the other copy (checked before),
    and a join chosen as the target of a reordering is not reordered itself;
  - safeguards, after a regression on a real view (a reordered copy joined next a table with no
    condition to the tables already joined, a cross join that streaming can not plan): the next
    table joined is always one that an equality connects to the tables already joined, every
    join that the reordering builds must have an equality between its sides, and the reordered
    copy must have the column types of the copy. Otherwise the copy is kept as it was.
- Still not done: filters on the optional side of an outer join, a filter with `now()` (it is a
  join with the `Now` source at that point and must be equal), reordering through outer joins
  other than the case above.

### Batch: column facts, unique keys and repeated lookups (added on 2026-11-26)

The same rules, applied to batch queries on demand, each family behind its own variable (default
false): `batch_simplify_column_facts` (A to G: `ColumnFactsSimplifyRule`, `NeverTruePredicateRule`,
`EmptyRelationPropagateRule`, `EmptyInputSimplifyRule`, `ConstantRowJoinRule`, including the
aggregate of a constant of idea 8), `batch_simplify_on_unique_key` (ideas 1 and 2:
`UnusedOuterJoinEliminateRule`, `AggOnKeyEliminateRule`) and `batch_merge_repeated_lookups` (idea 3:
`RepeatedLookupMergeRule` with its module `flat`; not the rules of
`streaming_merge_lookups_under_dedup`).

- Where: `reduce_batch_on_keys`, called by `gen_optimized_logical_plan_for_batch` after subquery
  unnesting and the first predicate pushdown, before join ordering, so that the joins that are
  removed are not ordered. In the order of the streaming optimizer: column facts, then the columns
  are pruned (an outer join is unused only once its output omits the optional side), then the
  unique keys (up to three passes with column pruning), then the lookups (up to three passes with
  predicate pushdown, column pruning and projection removal). The final `BATCH_PRUNE_EMPTY_RELATION`
  stays as it is.
- Why the rules hold in batch too: all the scans of a batch query read the same snapshot, as the
  two sides of a streaming join read the same epoch, so two scans of a table without
  `FOR SYSTEM_TIME AS OF` read the same rows (a scan in the past is never a lookup); keys are the
  keys of tables and materialized views (the hidden row id for a table without primary key), as in
  streaming; `now()` is a constant in a batch query, so the guards on temporal filters never
  trigger; `internal_last_seen_value`, which carries the row of an instance through an
  aggregation, has a batch implementation and a two-phase form. Batch removes shared sub-plans
  before (`DAG_TO_TREE`), so the lookups of a shared sub-plan (a CTE or a view read twice) never
  apply: two equal sub-plans are not proven to be equal.
- Expressions that may fail: a batch query fails where a streaming job writes `NULL`. The rules
  never claim that an expression that may fail is not `NULL`, and keep a constant that fails to
  evaluate. A merged lookup computes its columns under a `CASE` on "found", which is evaluated
  lazily; the conjuncts that it moves into a filter may be evaluated on rows that the join did not
  match, the same freedom that predicate pushdown already takes.
- Not handled in batch: a top-1 written with `row_number()`, which the batch optimizer keeps as a
  window function and a filter (the streaming one turns it into a top-n); `DISTINCT ON` is a top-1
  and is handled. Lookup joins and sort-based operators are chosen later, from the reduced plan.
- Tests: planner `batch_plan_reduction_keys.yaml`; differential `e2e_test/batch/plan_reduction_keys`
  (generator `plan_reduction_keys.gen.py`): every query runs through `INSERT ... SELECT` with and
  without its variables, after each of four rounds of changes of the data.

### BL. Batch: duplicates found with `lag` (added on 2026-11-26, done on the branch)

A uniqueness check is often written by comparing each value with the previous one in sorted
order:

```sql
with ordered as (select x, lag(x) over (partition by true order by x) as prev from t where x is not null),
duplicates as (select * from ordered where x = prev)
select x, count(*) + 1 as n from duplicates group by x;
```

The batch plan sorts the whole relation on one node, in one window partition that does not spill
(`BatchOverWindow` over `BatchSort` over a singleton exchange). No batch stage rewrites it:
`OverWindowToAggAndJoinRule` (`CONVERT_OVER_WINDOW_FOR_BATCH`) only takes windows without order,
`SPLIT_OVER_WINDOW` and `MERGE_OVER_WINDOW` only regroup window functions. `LagDuplicatesRule`
(`batch_rewrite_lag_duplicates`, default false, batch only) runs after the first predicate
pushdown, which leaves `Filter(x = w)` right above the window, and before `SPLIT_OVER_WINDOW`, so
that the unused window functions sharing the window go away with it.

- Shape: `Filter(x = w AND ...)` over `OverWindow(w = first_value(x) OVER (PARTITION BY p ORDER BY
  x ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING))` (that is `lag(x)`; `1 FOLLOWING` is `lead(x)`, and
  `lag(x, -1)`; `last_value` with the same one-row frame is the same), where the first order column
  that is not a partition column is `x`, and above the filter only `x`, `w` and the non-constant
  partition columns are read.
- Exactness: within a partition, rows are ordered by `x` first, so the `k` rows of a non-NULL
  value `v` are contiguous whatever the direction, the place of NULLs or the order of ties; `x = w`
  holds on exactly `k - 1` of them (all but the first in the frame's direction, which meets
  another value or the NULL of a missing row) and never on a NULL `x`. Since only `x`, `w` (equal
  to `x` there) and partition columns are read, the result is `k - 1` copies of `(p, v, v)`: the
  multiset of `row_number() OVER (PARTITION BY p, x) > 1`. Ties are interchangeable.
- Rewrite: `Agg(group p', x: c = count(*))` over `Filter(x IS NOT NULL)` of the input, where `p'`
  drops the constant partition columns (`PARTITION BY true`), then
  `ProjectSet(generate_series(2, c))` to produce `c - 1` rows per group. The work is distributed
  by the group key and the hash aggregation can spill. When the consumer is an aggregation whose
  group key covers `p'` and `x` (or `w`) with only `count(*)` or the count of `x` or `w`, the
  expansion is skipped: `Filter(c > 1)` and `c - 1` as the count (the `+ 1` of the query above then
  gives `k`).
- Other conjuncts of the filter that read only `p'`, `x` and `w` and are pure are conditions on
  the group, applied to the input rows (`w` replaced by `x`).
- Refused: another column read above (the dropped duplicate would matter: the row id, another
  window function, a partition column that is a constant, `count(y)`); an offset other than 1;
  `IGNORE NULLS`; another frame (`ROWS 1 PRECEDING` ends at the current row: the first row compares
  `x` with itself); `IS NOT DISTINCT FROM` (NULLs and the missing row would count, depending on
  where NULLs sort); any other comparison or an `OR`; `x` in the partition key; `x` or a partition
  column of a type where equal values can differ (`decimal` `1.0 = 1.00`, whose group keeps the normalized value; floats;
  `interval`; composite types). `lag` with a default value is not supported by the binder anyway.
- Tests: `batch_rewrite_lag_duplicates.yaml` (planner) and the differential
  `e2e_test/batch/rewrite_lag_duplicates.gen.py` (every query inserted into a table with and
  without the variable, multisets compared after inserts, deletes, updates, NULLs, no duplicates
  and an empty table).

## Batch counterparts (added on 2026-11-26, done on the branch `proto/plan-reduction-batch-unnest`)

Four batch variables, off by default, run the same rules in the batch optimizer
(`gen_optimized_logical_plan_for_batch`). Batch plans are not frozen and see no retraction, but
the general unnesting is just as costly: every query that holds such a subquery reads its outer
side again for the domain.

| Variable | Rules | Where |
|----------|-------|-------|
| `batch_simplify_subquery_unnesting` | `ApplyOneRowJoinSplitRule`, `PullUpCorrelatedPredicateAggRule::create_for_stream` (9) | subquery unnesting, after the simple unnesting |
| `batch_unnest_lateral_functions` | `UnnestArrayOfTableFunctionRule`, `LateralTableFunctionRule::create_for_batch` (9b) | subquery unnesting, after the stage above |
| | `SelfAggregateJoinRule::create_for_batch` (9c) | after the first predicate pushdown, before join ordering |
| `batch_factor_union_join` | `CommonJoinOverUnionRule` (7) | after the first predicate pushdown, before join ordering |
| `batch_merge_union_scans` | `UnionScanMergeRule::create_for_batch` (US) | after the first predicate pushdown, before join ordering |

- Before join ordering: the joins of the branches of a union are still as written (join ordering
  may reorder each branch differently), the filters are next to the scans, and the joins and
  unions that the rewrites build are ordered with the rest. A stage that rewrote something is
  followed by a predicate pushdown, so that join ordering finds the filters where it expects.
- What differs from streaming:
  - the variant of `PullUpCorrelatedPredicateAggRule` named "for stream" has nothing specific to
    streaming: the `CASE` that gives `count` its 0 and `EXISTS` its false is exact in batch;
  - `sum` evaluated per row (9b, 9c) is not guarded by a count: a batch `sum` over no value is
    `NULL`; the guard only works around the streaming `sum` that returns 0 after retractions;
  - the per-row aggregation of 9b needs a unique key of the outer side. In batch every leaf of the
    outer side must be the scan of a table: the row id of a source read by a batch query is not
    generated (it is `NULL`), so its stream key tells no rows apart;
  - a table with a watermark is merged by `UnionScanMergeRule` (the watermark only matters to a
    streaming union);
  - `ApplyOneRowJoinSplitRule::create_for_batch` splits the apply of the subqueries of a select
    list only when the rule that follows it in its stage unnests all of them alone but at most
    one: a subquery that goes through the general unnesting joins its own domain, and the outer
    side of each apply holds the applies before it. Streaming shares these copies; a batch plan
    copies them, and the upstream case "subqueries in case when exprs" (four `ARRAY` subqueries
    over functions of one outer row) grew from 8 to 80 joins. With at most one such subquery,
    there is one domain, as with one apply for all;
  - no `UnionScanShareRule` and no sharing of identical sub-plans: a batch plan has no shared
    sub-plan (`DAG_TO_TREE` runs first). Two copies of a view in two branches are matched by
    `CommonJoinOverUnionRule` as equal sub-plans: in one batch query they read the same snapshot.
- Costs to keep in mind, and why the variables stay off by default:
  - the aggregation of an aggregating subquery covers the whole inner table, grouped by the
    correlated columns, where the domain restricted it to the keys of the outer rows (possibly with
    a lookup join on an index): worse for a query that keeps few outer rows of a large inner table;
  - a merged scan reads the rows of every branch with the `OR` of their filters, and copies each
    row once per branch before the filter of the branch: a branch that could use an index or a
    range of the primary key alone may read more;
  - a factored join joins the rows of every branch with the relation in one hash table.
- Tests: planner cases in `batch_plan_reduction_unnest.yaml`, the differential batch test
  `e2e_test/batch/plan_reduction_unnest.gen.py` (every query run as an `INSERT ... SELECT` with and
  without its variable, the two results compared as multisets after each round of changes).

### BJ. Batch: cost-based join ordering (added on 2026-11-26, first version)

The batch planner orders the inner joins with a heuristic (`LeftDeepTreeJoinOrderingRule`): the
graph of equalities is walked from its first edge, with no idea of the sizes of the tables. With
`from d join f on f.d_id = d.id join e on f.e_id = e.id`, a large `f` becomes the build side of
the first hash join because `d` is written first, where reading `f` once and looking up `d` and
`e` by key would be cheaper. `batch_cost_based_join_ordering` (default false) orders them by
estimated rows instead (`cost_based_join_ordering_rule.rs`, stages
`BATCH_COST_BASED_JOIN_SIDES` and `BATCH_COST_BASED_JOIN_ORDERING`, run in
`gen_optimized_logical_plan_for_batch` just before the heuristic one).

- **Statistics.** The meta node sends the frontend the storage statistics of every state table
  (`HummockVersionStats`: `total_key_count`, key and value sizes; also shown by
  `rw_catalog.rw_table_stats`); the frontend keeps them in its catalog. The row count of a table,
  materialized view or index is its `total_key_count`, an approximation maintained at each
  checkpoint (one key per row). Nothing else is used: no histogram, no distinct counts.
- **Estimates.** A scan: the rows of the table, times 0.1 per `column = constant` conjunct and
  0.25 per other conjunct, and one row when the filter fixes a unique key (the unique keys of
  `unique_key_simplify_rule.rs`). A filter or a projection over an estimated input: the same. An
  aggregation: one row without group, the input rows when it groups by a key of its input, 10%
  otherwise. Outer joins: the preserved side; semi and anti joins: half of the kept side. Any
  bound that `CardinalityVisitor` proves (`LIMIT`, one-row aggregation, values) caps the
  estimate. A join of a set of inputs: the product of their rows and of the selectivities of the
  conditions between them, so that the estimate does not depend on the order. An equality on
  columns holding a unique key of one side has the selectivity 1 over the rows of the table of
  that side before its filters (key to foreign key: one row of the key side per row of the other
  side, fewer if the key side is filtered); on other columns, it returns twice the larger side.
- **Width (added on 2026-11-27).** A real plan built its hash table on 1.8 M rows holding about
  17 GB of JSON against 4.5 M rows of 40 bytes: fewer rows, a hundred times more memory. The
  cost therefore counts bytes. The width of a row of an input is the sum of the widths of the
  columns that the plan above still reads (a top-down pass, `required_output_columns`, since the
  scans still read every column before the column pruning). The width of a column of a table is
  the average size of a row in storage (`(total_key_size + total_value_size) / total_key_count`)
  shared out among the columns of the table by the weights of their types, or the type width
  alone without sizes (1 to 16 bytes for numbers and times, 64 for `varchar`, 256 for `bytea`,
  1024 for `jsonb`, 128 for the rest). Widths go through projections (an expression gets the
  width of its type), filters, top-n, window functions and aggregations (group key columns keep
  their width, an aggregate returning the type of its argument, such as `first_value` or
  `last_value` of a JSON column, keeps the width of the argument), plus 8 bytes of overhead per
  row. Widths are always known: the fallback only depends on the row counts.
- **Cost.** The batch hash join builds its hash table on its right side: probe bytes cost 1,
  build bytes 3 (they are held in memory), output bytes 1, plus the bytes read by the inputs.
  When the right side is a table scan whose key prefix (the shortest one that holds the
  distribution key) is given by equalities with the left side, the planner makes a lookup join:
  it costs the bytes of the left row and of the row read, per row of the left side, and the table
  is not scanned. The model follows what the planner does: a lookup join is not optional when it
  is possible. With the same width on both sides, the side choice is the one by rows.
- **Outer, semi and anti joins (added on 2026-11-27).** After the column pruning (exact widths)
  and after `JoinCommuteRule` (which turns right joins into left ones), a left outer, semi or anti
  join becomes the right join of the other side (`CostBasedOuterJoinSidesRule`) when that at
  least halves the estimated cost of its hash join (probe bytes + 3 x build bytes): it then
  builds the smaller side and streams the larger one (the batch hash join executor supports
  right outer, semi and anti joins). Not done when the join may become a lookup join (the right
  side is a table read by its key, or a table that has an index) or without estimates.
- **Enumeration.** Dynamic programming over the connected sets of inputs (bushy trees allowed),
  up to 10 inputs per set of inputs linked by equalities, greedy beyond (the pair of linked
  sub-trees of fewest estimated rows, then of lowest cost). Sets of inputs that no equality links
  are cross joined in the order of the heuristic. Only inner joins move: an outer, semi or anti
  join is one input of the inner joins around it, and the inner joins below it are ordered on
  their own. A two-way inner join (not merged into a multi-join, which needs three inputs) has its
  sides swapped when that costs less.
- **Fallback.** If an input of a multi-join has no estimate (a table without statistics, a
  source, a table function...), the multi-join gets the heuristic order, and a two-way join is
  kept as it is: the plan is then exactly the one without the variable.
- **Tests.** The planner tests have no data: a test-only planner test option `table_row_counts`
  (`test_utils::set_table_row_counts_for_test`) gives the frontend row counts by table name
  before the query is planned (`batch_cost_based_join_ordering.yaml`: star and chain queries,
  outer, semi and anti joins, scalar subquery, aggregation input, no statistics, partial
  statistics, twelve inputs; with the test-only option `table_sizes`, a narrow side against a
  JSON side as a table, an aggregation or a top-1 per group, inner and left joins, equal widths).
  The differential test `e2e_test/batch/cost_based_join_ordering.slt`
  (generated by its `.gen.py`) runs each query with and without the variable on tables of very
  different sizes, before and after changes, and compares the results as multisets.
- **Not done.** Lookups into indexes are not modeled (index selection happens after the
  ordering, on the pruned columns); no distinct counts or histograms, so a join on columns that
  are not keys is a guess; the cross joins between unlinked sets of inputs keep the heuristic
  order (a nested loop join caches its left side); the estimates are not shown in `EXPLAIN`
  (only in `EXPLAIN (TRACE)`).

### BF. Batch: cardinality feedback, step 1 (added on 2026-11-26, observation only)

The estimates of BJ are guesses (fixed selectivities, no distinct counts). The later steps will
replace them by selectivities measured on earlier executions of the same sub-plans. Step 1 only
measures: with `batch_cardinality_feedback` (default false, `SESSION_INIT`), each batch query
counts the rows output by its scans, filters, joins and aggregations, and the frontend keeps how
they compare with the estimates. No plan changes (planner tests `batch_cardinality_feedback.yaml`
show the same plans with the variable on and off).

- **Operators and estimates** (`optimizer/cardinality_feedback.rs`, run on the final batch plan
  in `gen_batch_plan_fragmenter`): `seq_scan` (after its scan ranges), `filter`, `hash_join`,
  `lookup_join`, `nested_loop_join`, `hash_agg`, `sort_agg`, `simple_agg`, and the first phase of a
  two-phase aggregation as `*_partial` (no estimate: its rows depend on the parallelism). The
  estimates apply the model of BJ (same constants and join selectivity, shared with
  `cost_based_join_ordering_rule.rs`) to the batch operators; without statistics, no estimate
  (null), never a made-up number. Queries that read system catalogs are not observed.
- **Fingerprint.** A 64-bit FNV-1a hash of a canonical text of the sub-plan below the operator.
  Columns are identified by the table and column id they come from (or by the fingerprint of the
  expression or aggregation that computes them), not by their position; every constant (literal,
  parameter, constant sub-expression) becomes a placeholder of its type, the list of an `IN` of
  constants one placeholder whatever its length, and a scan range its shape (key columns fixed,
  kinds of bounds); conjuncts and the operands of commutative operators are sorted, the inputs of
  inner and full joins too, and right joins are written as left ones; projections, exchanges and
  sorts are transparent; an aggregation is its input and its group key (not its calls), and a
  two-phase aggregation has the fingerprint of the one-phase one. The physical join is not part
  of the fingerprint but of the operator kind beside it. Same query with other literals: same
  fingerprints; other join columns, comparisons or tables: other fingerprints (unit tests).
  One fingerprint therefore covers every literal value: `v < 3` and `v < 13` share one record
  (executions add up, the last actual rows are those of the last execution, the e2e test
  asserts it). Step 2 has to decide whether a learned selectivity may depend on the literal,
  e.g. by keeping a distribution of selectivities per fingerprint rather than a single mean.
- **Counting** (`risingwave_batch::executor::cardinality`): the frontend sets
  `PlanNode.cardinality_feedback_id` on the counted nodes; the executor builder then wraps their
  executors in a `CountingExecutor` that adds `chunk.cardinality()` to an atomic counter per
  chunk, and notes whether the output reached its end. Without the variable no node is marked,
  no executor is wrapped, and the task status and exchange messages are unchanged.
- **Where the counts flow.** Executors run by the frontend (local execution, root stage of the
  distributed execution) count into the counters of the query. A distributed task sends its
  counts with its final task status (`TaskInfoResponse.cardinalities`), which the stage runner
  adds to the counters of the query, summing the tasks of a stage per plan node. A plan that the
  local execution sends to a compute node (`Execute`) sends its counts after its last chunk
  (`GetDataResponse` without chunk), which the exchange of the frontend adds in the same way.
- **Partial counts.** The counts are recorded when the query is over (the last reference to its
  `QueryCardinalityFeedback` is dropped, so after every report that can arrive). A count is
  partial (a lower bound) when the query did not return all its rows (failure, cancellation,
  timeout), when an executor was stopped before the end of its output (a limit above it), or when
  fewer executors reported than the frontend started (a task that never reported).
- **Exposure.** In the memory of each frontend, at most 10 000 records (constant
  `CARDINALITY_FEEDBACK_CAPACITY`; the least recently seen tenth is evicted when full), by
  fingerprint and operator kind: system view `rw_catalog.rw_batch_cardinality_feedback`
  (fingerprint in hex, operator kind, estimated rows of the last execution, last actual rows and
  whether it is partial, executions, partial executions, mean of `log10(actual / estimated)` over
  the complete executions with an estimate, last seen). Counts and hashes only, no values or
  literals. A rate-limited `WARN` log (10 per minute) reports a complete count more than 100 times
  and more than 100 000 rows away from its estimate, with the fingerprint, the kind and the two
  numbers.
- **Tests.** Unit tests: counting per chunk and early stop (`risingwave_batch`), sums over the
  tasks of a stage, partial counts, bounded store, fingerprint normalisation and estimates
  (frontend). End to end: `e2e_test/batch/cardinality_feedback.slt` (scan with filter, hash join,
  lookup join, aggregation, each in local and distributed mode; variable off; limit).
- **Not done.** Other operators (union, top-n, table functions, sources) are not counted; the
  records are per frontend and lost on restart; the estimates of the plan are not shown in
  `EXPLAIN`; nothing uses the records yet (step 2).

### BF step 2. Batch: learning from the counts (added on 2026-11-26)

With `batch_cardinality_feedback_learning` (default false, `SESSION_INIT`), the counts of step 1
become ratios that the estimates of BJ use instead of their fixed guesses
(`optimizer/cardinality_learning.rs`). The session observes its batch queries as with
`batch_cardinality_feedback` (either variable observes; the learned ratios are shared by the
sessions of the frontend).

- **Ratios, not rows**, so that they hold when the tables grow: `selectivity` (rows kept by the
  predicate of a scan per row of the table, or by a filter per input row), `group_ratio` (groups of
  an aggregation per input row), `fan_out` (output rows of an inner join per row of its larger
  input). The input rows are counted too: the child operator, or the table for a scan (the count
  of the scan when it reads the whole table, the storage statistics when it reads key ranges; the
  rows of the table read by a lookup join come from the estimate). Only complete counts are
  learned: a partial count (failure, cancellation, limit above) is ignored.
- **Keys shared by planning and execution.** A ratio is keyed by its relation, computed the same
  way from the logical plan (planning) and from the final batch plan (execution): a leaf is a
  table and the predicate of its logical scan, before it is split into key ranges and a filter
  (the batch scan carries the key computed from the logical scan, `BatchSeqScan::learning_leaf`);
  a filter, an aggregation (input and sorted group columns) and an inner join (sorted base
  relations and conjuncts, whatever the join order) are keyed from their inputs; columns are
  identified by table and column id and constants are placeholders, so one key covers every
  literal. The fan-out belongs to the edge between two base relations: the pair of relations and
  of compared columns, learned from any join whose equalities compare one base relation on each
  side. Outer joins, unions, sources... have no key: nothing above them is learned.
- **Distribution.** Per key: number of observations, minimum, maximum, and a mean of
  `log10(ratio)` that decays (plain mean of the first 5 observations, then weight 0.2 per new
  one). The estimate blends the fixed guess and the learned mean in log space, the guess weighing
  `0.25^n` after `n` observations (1/4 after one, 1/16 after two). Memory decisions use the
  maximum: each estimate also carries pessimistic rows (largest learned ratios), and the build
  side of a hash join is costed with them; the top-n guard uses the largest group ratio.
- **Join ordering (BJ).** The join tree is computed with the fixed guesses, as before, and with the
  learned estimates; the learned tree replaces the other one only when, with the learned
  estimates, it costs less by more than `PLAN_CHANGE_FACTOR` = 1.5 (a factor of 2 could never swap
  the two sides of a join: the build side costs 3 per row, the probe side 1). Without learned
  ratios the plan is the one of BJ. The plan of the previous execution is not kept (step 4).
- **Top-n per group (BT).** A `row_number() <= n` with `n` (limit + offset) above 10 becomes a
  top-n per group when the largest group ratio learned for the input of the window and its
  partition columns, times `n`, is at most 1 (the heaps then hold no more rows than the input,
  which the sort holds anyway), up to `n` = 1000. The group ratio comes from aggregations of the
  same input by the same columns (the window itself does not count its groups).
- **Exposure.** At most 10 000 ratios per frontend (least recently observed tenth evicted when
  full): system view `rw_catalog.rw_batch_cardinality_learning` (key in hex, kind, observations,
  mean, minimum and maximum ratio). Ratios and hashes only.
- **Tests.** Unit tests: distribution, decay and confidence, bounded store, partial counts ignored,
  order-independent keys and edges (`cardinality_learning.rs`), the ratios to learn from batch
  plans (`cardinality_feedback.rs`), complete counts only (`src/frontend/src/scheduler/cardinality_feedback.rs`).
  Planner tests `batch_cardinality_feedback_learning.yaml`, through the test hook
  `cardinality_observations` that records a query as executed with given counts: a two-way and a
  three-way join that change once an aggregation is observed with one group per row, one key for
  every literal, a small learned change kept by the change factor, the variable off, a top-n per
  group allowed by 100 observed groups and kept as a window with 5000 groups, a too large limit or
  other partition columns. End to end: `e2e_test/batch/cardinality_feedback_learning.slt` runs a
  join and a window several times, checks with `EXPLAIN` that the plan changes only after an
  execution was observed, and that the results stay the same.
- **Not done / limits.** The learned ratios are per frontend and lost on restart. Scans of indexes
  (index selection happens when the batch plan is built) and lookup joins into a table with a
  predicate (the predicate moves into the join) are not matched with the logical plan, nor is a
  sub-plan rewritten between join ordering and the batch plan (e.g. join conditions computed in a
  projection by `PUSH_CALC_OF_JOIN`). Join fan-outs are attributed to one edge when a join
  compares several pairs of relations or has other conditions: such joins are not learned. A
  learned ratio does not depend on the literal (one distribution per key). The group ratio of a
  window partition is learned from aggregations only. Step 3 (estimate intervals, gain of 2 in
  the worst case) and step 4 (plan regression guard, previous plan per query) are not done.

### BP and BT. Batch: second pass of the reductions on keys, window top-n (added on 2026-11-26)

**BP (`batch_reduce_keys_after_conversions`).** The reductions on keys of batch queries (column
facts, unique keys, repeated lookups: "BK") run once, after the first predicate pushdown. Several
later stages create the very operators that they remove: the lag rewrite (BL) creates
`Agg(group x: count)`, `CONVERT_DISTINCT_AGG_FOR_BATCH` turns a `UNION` into an aggregation and
splits the distinct aggregates, the window conversion creates aggregations and, with BT, top-n per
group. BP runs `reduce_batch_on_keys` again after the final column pruning and predicate pushdown,
at most two rounds, stopping when a round applied none of its rules; each round that did is
followed by a predicate pushdown, a column pruning and the projection cleanup.

```sql
create table t (id int primary key, v int);
with o as (select id, lag(id) over (order by id) as prev from t)
select id, count(*) + 1 from o where id = prev group by id;
-- after BL: Filter(count > 1) <- Agg(group id: count) <- Scan(t)
-- BP: the aggregation is on the key of t (count = 1), `1 > 1` is never true: an empty relation
```

- Exactness: no new algebra, the rules of BK with their own conditions; only the position is new.
  At this position every set operation is a `UNION ALL` (`INTERSECT` and `EXCEPT` became joins,
  `UNION` an aggregation), so the column pruning of the pass cannot change what a set operation
  removes (the early pass relies on the set operations keeping all their columns when pruned).
  Join ordering has turned the multi-joins back into binary joins; the aggregations still have one
  phase; `now()` is a constant and the plan is a tree.
- The second round catches what the first exposes (a lookup merged after the aggregation above
  it was looked at). The variable does nothing without the BK variables.

**BT (`batch_over_window_to_topn`).** `row_number() OVER (PARTITION BY k ORDER BY o) <= n` stays
`Sort` + `OverWindow` + `Filter` in batch: upstream took `OverWindowToTopNRule` out of the batch
window conversion because DataFusion cannot run a top-n per group. BT puts back a batch variant of
the rule in that stage: the window becomes `TopN(group k, order o, limit n)`, which also gives the
key `k` to the rows (a left join to the latest row per key whose columns are unused goes away with
BP).

- Memory: the batch `GroupTopN` keeps a heap per group in a hash map until its input ends. It
  cannot spill: its memory is that of an operator that cannot spill under the batch memory
  admission (taken from the pool of the query, then from the memory of the node that no query
  reserved, waiting or failing when there is none). The sort that it replaces can spill, and the
  window after it holds one partition at a time. Each heap allocates room for `limit + offset`
  rows when its group is created, even for a group of one row; with ties (`rank()`), a heap keeps
  every row tied with the last one, without bound. So BT only rewrites `row_number()`, and only
  when `limit + offset <= 10`: the state is then at most ten rows per group, close to a hash
  aggregation for a top-1, and never more rows than the sort would hold. `rank()`, `dense_rank()`,
  larger limits and lower bounds alone (`rn > 1`) keep the window.
- DataFusion: a query that reads an Iceberg table while `enable_datafusion_engine` is on keeps the
  window, so that it still runs in DataFusion.
- Exactness: the streaming rule, unchanged; among rows tied on the order columns, both forms keep
  an arbitrary one.

- Tests: `batch_reduce_keys_after_conversions.yaml` and `batch_over_window_to_topn.yaml`
  (planner: lag then key, `UNION` then key, distinct aggregates, top-1 then unused left join, the
  count over a `UNION` without `ALL` kept, limits around 10, rank kept, off by default), and the
  differential `e2e_test/batch/reduce_keys_after_conversions.gen.py` (every query run with every
  variable off and with BK + BP, BT, both; multisets compared on empty tables, NULLs, duplicates,
  ties, more than ten rows per group, after deletes and updates).
- Not done: the union join factoring, union scan merge, status ladder and empty-relation stages
  are not re-run in the pass.

### BU. Batch: distributed `UNION ALL` (added on 2026-11-26, done on the branch)

`BatchUnion::to_distributed` gathered every input into one task (`RequiredDist::single()` on
each input, with a TODO about a round-robin distribution). Everything above the union then ran in
that task, or was hashed again from it: a hash aggregation or a join over a union read all the
rows of all the branches through one task, twice over the network.

```sql
create table a (id int primary key, k int, v int);
create table c (k int, v int);
select k, count(*) from (select k, v from a union all select k, v from c) u group by k;
-- before: HashAgg <- Exchange(Hash k) <- Union <- [Exchange(Single) <- Scan(a), Exchange(Single) <- Scan(c)]
-- BU:     HashAgg <- Union <- [Exchange(Hash k) <- Scan(a), Exchange(Hash k) <- Scan(c)]
```

**Why upstream forced `Single`.** Two reasons, both still true:

- There is no round-robin exchange (`Distribution::SomeShard` becomes `DistributionMode::Single`
  in `to_prost`, `RequiredDist::AnyShard` is `todo!()` in `to_dist`). An input can only reach the
  tasks of a parallel stage by a hash, a consistent hash or a broadcast.
- The stage fragmenter assumes one table scan (or source, or lookup join) per stage: it reads the
  first scan it finds to decide the tasks of the stage and the vnodes of each task, and gives the
  same vnodes to every other scan of the stage. Two branches that scan different tables, kept in
  the stage of the union without an exchange, would scan the second table with the vnode mapping
  of the first (wrong when the vnode counts differ, and against the fragmenter's assumption in any
  case); a branch distributed as `Single` (values, a global aggregation) would run once per task
  and duplicate its rows. So "each input kept where it is, no exchange" is not safe in general.

**What BU does (variable `batch_distributed_union`, default false).** Each input is planned
exactly as before (`to_distributed`); only the exchange above it changes. All inputs are brought to
one distribution `target`, the same for all, and the union is then distributed as `target` in the
stage of its parent:

- the parent requires `HashShard(k)` (second side of a hash join) or `UpstreamHashShard(k, t)` (a
  lookup join): `target` is that distribution, the exchange the parent would have put above the
  union;
- the parent requires a shard by key (hash aggregation, `DISTINCT`, first side of a hash join,
  partitioned window, group top-n): the `HashShard` an input already has if it satisfies the
  requirement, otherwise the hash of the required key, as the parent would have done;
- no requirement (a count, a top-n, a limit or a sort above, which then run in two phases, the
  sort merged by the exchange above it): the `HashShard` an input already has, otherwise the hash
  of all the columns of the union;
- a required single distribution (root, sort, nested loop join, insert), a broadcast, inputs that
  are all single (values, global aggregations), or a union without columns: the single union, as
  before.

An input already distributed as `target` stays in the stage of the union, without exchange, only
when the part of its plan in that stage (down to its exchanges) holds only operators that
transform the rows of their own task (projection, filter, hash or sort aggregation, hash join,
union, project set, expand, hop window, window, group top-n) and nothing distributed as `Single`:
no scan, source or lookup join that would decide the tasks of the stage. Every other input gets an
exchange to `target`, which puts its plan in a stage of its own. Projections and filters (and
insert, max-one-row and group top-n, which used to convert their input before checking it) pass
the required distribution down to a union below them, through the column mapping of the
projection; without the variable, or without a union below, they convert their input as before.

- Exactness: a `UNION ALL` concatenates its inputs and its order is unspecified. Each input is
  computed once, by its own plan; each of its rows goes through exactly one exchange partition to
  exactly one task of the union, which emits it once: the union of the tasks is the same multiset.
  `HashShard(k)` is the same function in every input (same column types at the same positions, the
  same hash, the parallelism of the stage of the union as the partition count), so the rows with
  equal `k` from every branch meet in the same task, which is all the operator above relies on.
  A union tag (a constant column per branch) is hashed like any other column. `UNION` without
  `ALL` is a hash aggregation over a `UNION ALL`, handled as such. Local execution (`to_local`)
  does not change.
- Cost: with a requirement, the union replaces the gather into one task plus the hash exchange
  above it by one hash exchange per input; the operator above runs in parallel on its input
  partition. Without a requirement, the rows are hashed on all their columns instead of gathered:
  the same volume crosses the network, but into all the tasks; an operator above that only needs
  a single task (a plain `SELECT` of the union, an insert) keeps the single union. A
  `count(*)` whose union column pruning left without columns keeps it too (nothing to hash), but
  its inputs then send rows without data.
- Tests: `batch_distributed_union.yaml` (planner: union under a hash aggregation, a join of two
  unions, a lookup join, a window, a count, a top-n and a limit; inputs that are scans,
  aggregations already hashed on the key, joins and values; a tag column; `UNION`; forced
  two-phase aggregation; single unions kept; off by default), and the differential
  `e2e_test/batch/distributed_union.gen.py` (29 queries run with the variable off and on, in local
  and distributed query mode, multisets compared on empty tables, NULLs, duplicate rows, always
  empty branches, after updates and deletes, and with a single branch left).
- Not done: a round-robin exchange, which would let a union with no requirement keep its inputs
  in their stages without hashing; a partial aggregation pushed into each branch.

### BS. Batch: identical sub-plans computed once per task (added on 2026-11-26, first version)

The batch optimizer turns the plan into a tree before anything else (`DAG_TO_TREE`): a view or a
CTE read twice is planned and executed twice, its scans and its aggregation memory paid each time.
A batch query has no shared sub-plan at execution: a stage is a tree of executors, each task output
has one consumer (`take_output` takes the receiver), and the exchanges of a parent stage each read
their own child stage.

Three designs were weighed:

- (A) A shared child stage whose task outputs are read by several exchanges. It shares copies
  that sit in different stages, but it changes the stage graph (a child with several parents and
  exchange infos), the distributed and the local query runners (the local mode embeds the child
  plan into each exchange source, so a child is run once per reader), and the task outputs (one
  bounded channel per consumer today). It also brings the deadlock risk: two exchanges of the same
  parent task reading one shared output, the parent reading the first one to its end (a hash join
  build) while the producer is blocked on the full channel of the second one. Avoiding it needs an
  unbounded, spillable buffer per consumer, accounted in the query memory: a spool per output.
- (B) A spool inside a task: the copies of one stage run in the same tasks, so the task computes
  the sub-plan once, keeps its rows and replays them. Nothing changes in the stages, the runners
  or the task outputs; it works the same in local and distributed mode.
- Sharing only the scans (a shared scan stage) saves the reads but not the aggregation memory.

**BS is (B)**, the most contained and robust; (A) is the follow-up for copies in different stages.

`share_identical_batch_subplans` runs last, on the final distributed or local batch plan. It
groups the nodes by stage (the position of the exchange that starts the stage) and by structural
equality (every field of the batch nodes but their ids: distributions and orders included). A
sub-plan is shared when it has at least two copies in its stage, only nodes that compute the same
rows each time they run (scans, exchanges, projections, filters, joins, aggregations with builtin
functions, sorts, top-n, limits, unions, over windows, ...: no source, external query, DML or
system table), no impure expression, at least one aggregation, join, sort, top-n or over window,
and a copy that is not below a `LIMIT` unless it computes all its rows before the first one
anyway (an aggregation, sort or top-n at its root): a join below a limit may stop early, the
spool would compute it whole. The first copy becomes `BatchSpool { spool: n }` above the sub-plan, the others
`BatchSpoolRead { spool: n }`, a leaf: the exchanges below a reader are gone, so are the stages
below them. The largest sub-plans win; a smaller one repeated inside the producer and elsewhere is
shared again.

```sql
create table t (k int, v int, ts int);
create view latest as select k, last_value(v order by ts) as v, count(*) as n from t group by k;
select a.k, a.v, b.n from latest a join latest b on a.k = b.k;
-- HashJoin(k) <- BatchSpool { spool: 1 } <- HashAgg(k) <- Exchange(hash k) <- Scan(t)
--             <- BatchSpoolRead { spool: 1 }
-- one scan stage instead of two, one hash table of the aggregation instead of two
```

Execution (`SpoolExecutor`, one per node of the spool, all holding the same `Spool` of the plan
fragment from the `SpoolRegistry` of the executor builder):

- The first node read starts the materialization: the sub-plan runs to its end, its rows are kept,
  then every node returns all of them, in the order of the sub-plan (any order or distribution of
  the copy holds).
- Deadlocks. The materialization is a shared future: any node waiting for it drives it, all of
  them are woken when it progresses, and no lock is held while it runs. Its sub-plan contains no
  node of its own spool (a reader is a leaf, a sub-plan cannot contain a copy of itself), and a
  spool nested in it is strictly smaller, so no wait cycle exists inside a task. A parent that
  stops reading the node that started it (a hash join reading its build side first) does not
  block another node: the next one read drives it. Towards other tasks, the materialization is a
  blocking operator (like a hash aggregation) where the first node of the spool is read; the
  readers read nothing from other tasks. Unit tests cover a node left aside or dropped in the middle
  of the materialization, concurrent readers, errors and a missing producer.
- Memory. The rows are counted in the memory context of a spillable operator of the query (the
  same pool as hash aggregations, joins and sorts, and counted by the frontend when it fits the
  parallelism to the memory of the query). When the context refuses more, the rows kept are
  written to a spill file, released from the context, and the next rows go to the file: the
  spool then holds no memory. Without spilling the query fails as a hash aggregation would. The
  rows are released when the last node of the spool is dropped.

- Exactness: a reader gets exactly the rows that its copy would have computed in the same task
  (same scan partitions, same rows from each exchange). Where a sub-plan may return different
  rows each time (ties of a limit or top-n, `array_agg` without `ORDER BY`), every copy now gets
  the same rows, one of the results each copy could have computed. Impure expressions and
  user-defined aggregates keep their copies.
- Cost: the first rows of the consumer of the first node wait for the whole sub-plan; the rows
  are kept until the last node is done (spilled when memory is short).
- Tests: `batch_share_identical_subplans.yaml` (planner: self join, two join branches on the same
  or different keys, union branches, repeated scalar subquery, aggregation over a join, top-n,
  insert, three copies across stages; kept: impure expression, copies without aggregation or join,
  copies made different by a pushed filter, different aggregations; off by default), unit tests in
  `spool.rs` (executors) and `spool_registry.rs`, and the differential
  `e2e_test/batch/share_identical_subplans.gen.py` (every query with the variable off and on, in
  the local and the distributed query mode, multisets compared on empty tables, NULL keys and
  values, duplicates, after updates and deletes).
- Limits: copies in different stages are not shared. In distributed mode every input of a
  `UNION ALL` goes through its own exchange: copies under different branches are only shared when
  the whole branch is identical. Joins on different keys put the copies in different stages too.
  The local mode, where everything above the scans runs in one stage, shares them. Copies that
  differ by their columns or by a filter pushed into one of them are not shared: column pruning
  narrows each copy to what its consumer reads, so `latest a join latest b` where `a` reads one
  aggregate and `b` another keeps two aggregations. The widening of the streaming idea 10
  (`share_widened_subplans`, then `DAG_TO_TREE`) would make such copies identical, but every
  widened copy left in another stage would then compute the aggregates of all of them: not done,
  it needs to know the stages, which the logical plan does not.

### BLM. Batch: limit and top-n pushed into unions and outer joins (added on 2026-11-28)

**What existed.** `LimitPushDownRule` moves a limit below a projection, `TopNProjectTransposeRule`
and `ProjectTopNTransposeRule` move a top-n across a pure projection, and the batch rule
`BatchPushLimitToScanRule` puts a limit into the scan right below it. Nothing crossed a union or a
join: `SELECT .. FROM a UNION ALL SELECT .. FROM c LIMIT 10` read both tables whole into the single
task of the limit, and `.. FROM l LEFT JOIN r .. LIMIT 10` joined every row of `l`.

**What BLM does (variable `batch_push_down_limit`, default false, rule
`LimitUnionJoinPushDownRule`).** The limit or top-n stays where it is; a copy with `n + k` rows
(`LIMIT n OFFSET k`), without offset, goes below it:

```sql
create table a (x int, y int);
create table c (x int, y int);
create table l (id int primary key, k int, v int);
create table r (k int primary key, w varchar);
select x, y from a union all select x, y from c order by x limit 10 offset 5;
-- before: TopN(x, 10, 5) <- Union <- [Scan(a), Scan(c)]
-- BLM:    TopN(x, 10, 5) <- Union <- [TopN(x, 15) <- Scan(a), TopN(x, 15) <- Scan(c)]
select l.id, r.w from l left join r on l.k = r.k limit 10;
-- before: Limit(10) <- LeftJoin(Scan(l), Scan(r))
-- BLM:    Limit(10) <- LeftJoin(Limit(10) <- Scan(l), Scan(r))
```

- `Limit(n, k)` over `UNION ALL`: `Limit(n + k)` on each branch. Any `n + k` rows of the union are
  valid for the limit above, and the cut branches hold `n + k` rows together, or the whole union.
- `TopN(order, n, k)` over `UNION ALL`: `TopN(order, n + k)` on each branch (Calcite's
  `SortUnionTransposeRule`): each of the first `n + k` rows of the union is among the first
  `n + k` rows of its own branch. `WITH TIES` (no offset allowed): `TopN(order, n) WITH TIES` on
  each branch, which keeps every row of the branch whose key is not after the `n`-th key of the
  union.
- `Limit(n, k)` over `LEFT JOIN(L, R)`: `Limit(n + k)` on `L`. Every row of `L` gives at least one
  row of the join (its matches, or itself padded with `NULL`s), whatever the condition, and those
  rows depend only on that row and on `R`: the join of `n + k` rows of `L` is a part of the true
  join with `n + k` rows at least (or the whole join). A unique key of `R` on the join columns is
  not needed for this; it would only make the upper limit removable, which is not done.
- `TopN(order, n, k)` over `LEFT JOIN(L, R)` when every column of `order` comes from `L`:
  `TopN(order, n + k)` on `L` (Calcite's `SortJoinTransposeRule`). Every row of the join carries
  the order key of its row of `L`, and every row of `L` gives at least one row, so the first
  `n + k` rows of the join come from the first `n + k` rows of `L` (rows of `L` that tie at the
  bound give rows that tie as well). `WITH TIES` is pushed as `WITH TIES`. `RIGHT JOIN` is
  symmetric.
- The rule sees through the projections between the limit and the union or join (a projection
  keeps one row per row; for a top-n, each order column must be a plain column of the input of the
  projection). Applied top-down, the copy put into a branch is pushed further into the unions and
  outer joins below it. A branch that already returns `n + k` rows at most (a limit, a top-n
  without ties, values, an aggregation without groups) gets no copy; a copy put right above a
  limit with more rows becomes one limit with the smaller count. `OFFSET` without `LIMIT` and
  `LIMIT 0` are left alone.
- Where: after the last predicate pushdown (which has moved into the branches of the union, or
  into the preserved side of the join, the filters that a limit cannot cross; a filter left between
  the limit and the union or join stops the rule) and the column pruning, and before
  `TOP_N_AGG_ON_INDEX`, so that a top-n put on a scan may read an index ordered by its key, and
  before `LIMIT_PUSH_DOWN` and `BatchPushLimitToScanRule`, which then put the branch limits into
  the scans. Batch only: a plain `LIMIT` is not a streaming operator (it becomes a top-n), and a
  top-n per branch in streaming would add state for no clear gain.

**Not done, deliberately.**

- `UNION` without `ALL`: a distinct over a union all; a limit above a distinct cannot go below it
  (the rows cut may be the distinct ones).
- Inner, semi and anti joins: the row cut on one side may be the only one that matches (or, for an
  anti join, the one that does not).
- A top-n ordered by a column of the side padded with `NULL`s, a top-n per group, a top-n ordered
  by an expression computed above the union or join.
- Full outer join, on either side or both: wrong results. On `L`: with `L = {1, 3}`,
  `R = {1}` and `ON l = r`, the true join is `{(1, 1), (3, NULL)}`, so `LIMIT 1` must return one
  of these two rows. Cutting `L` to `{3}` gives the join `{(3, NULL), (NULL, 1)}`, and
  `LIMIT 1` may return `(NULL, 1)`, which is not a row of the true join: the row of `R` lost its
  match and was padded instead. On `R`, symmetric (`L = {1}`, `R = {1, 2}`, `R` cut to `{2}`
  gives `(1, NULL)`). Cutting both sides has the same problem. The top-n is wrong for the same
  reason. A correct variant would need the cut side's rows to be known unmatched beforehand,
  which is a join in itself.

**Tests.** `batch_push_down_limit.yaml` (planner: limit, offset, top-n, `WITH TIES` over a union
all, a filter above the union, expressions in the branches, nested unions, a branch with a wider
limit, a left join inside a branch, a limit inside a count, left joins on a key, without a key,
with a non-equality condition, right join, top-n on the preserved side, with expressions and with
ties; kept: top-n on a column of the padded side, full joins, inner, semi and anti joins, a left
join turned inner by a filter, a filter that stays above the join, `UNION`, an order on an
expression, branches already limited, offset only, top-n per group, off by default), and the
differential `e2e_test/batch/push_down_limit.gen.py` (38 queries whose result does not depend on
which rows a limit keeps: counts of the rows of a limit, an order on all the output columns or on
a unique key, `WITH TIES`, limits larger than the input; run with the variable off and on, in local
and distributed query mode, multisets compared on empty tables, NULLs, duplicate rows, unmatched
keys, after updates and deletes, with empty right sides and an empty left side).

**Risks.** The copies add operators: a top-n per branch sorts each branch (in two phases when it is
distributed) where the plan sorted the union once; with a large `n + k` and branches barely larger
than it, the gain is small. The copy on the preserved side of a join bounds the rows that the
join reads from that side, whatever the operator chosen for the join (hash, lookup, nested loop);
it does not help when the join is not where the time goes.

### BLD. Batch: limit over a distinct (added on 2026-11-28)

**Problem.** `SELECT DISTINCT a FROM t LIMIT 10` and `SELECT a FROM t1 UNION SELECT a FROM t2
LIMIT 10` plan a hash aggregation without aggregate calls under the limit. The hash aggregation is
blocking: it reads and deduplicates its whole input before the limit takes 10 rows, although the
first 10 distinct values seen would do.

**What BLD does (variable `batch_limit_distinct`, default false, rule `BatchLimitDistinctRule`,
operator `BatchLimitDistinct`, proto `LimitDistinctNode`, executor `LimitDistinctExecutor`).**
On the physical batch plan before its distribution, a `BatchLimit(n, k)` over a `BatchHashAgg`
with a group key, no aggregate calls and no grouping sets (through projections) gets a
`BatchLimitDistinct(n + k)` in place of the aggregation; the limit stays above and applies the
offset. The executor keeps a hash set of the values emitted so far (counted in its memory context,
`n + k` values at most), emits each value with the chunk where it first sees it (`NULL`s equal, as
for `DISTINCT`), and stops reading its input once it has emitted `n + k` values: it drops its input
stream, which ends the input as under a `BatchLimit` (an exchange ends its producers the same way).

```sql
create table t1 (a int, b int);
create table t2 (a int, b int);
select a from t1 union select a from t2 limit 10;
-- before: Limit(10) <- HashAgg(a) <- Exchange(Hash a) <- Union <- [Exchange(Single) <- Scan(t1), ..]
-- BLD:    Limit(10) <- LimitDistinct(a, 10) <- Union <- [Exchange(Single) <- LimitDistinct(a, 10, per task) <- Scan(t1), ..]
```

- `UNION` (an aggregation over a `UNION ALL`): each branch gets a per-task
  `BatchLimitDistinct(n + k)` too. Correctness: if the union has at least `n + k` distinct values,
  either one branch has `n + k` distinct values and emits `n + k` of them, or every branch emits
  all its distinct values; in both cases what the branches emit holds `n + k` distinct values (or
  every distinct value of the union), and the global operator above keeps `n + k` distinct values
  of them. With fewer distinct values in the union, every branch emits all its values and the
  global operator emits them all.
- Distribution: a global operator over an input distributed in several tasks becomes a per-task
  operator in each task (each stops after `n + k` values), a gather into one task, and the global
  operator again: the same argument with the tasks in place of the branches. Over an input already
  hashed on a part of the group key, or single, the operator runs once per task without gather
  (the tasks hold disjoint values). A per-task copy only promises values distinct within its task.
- Not rewritten: aggregations with calls (`GROUP BY` with `count`...), `DISTINCT ON` (an
  aggregation with `first_value` calls, or a top-n per group), a sort aggregation (it already
  streams its groups, so the limit stops it early), `OFFSET` without `LIMIT`, and an `ORDER BY`
  over the distinct (a top-n, not a limit; `WITH TIES` included).
- Not done: the ordered variant (`SELECT DISTINCT a .. ORDER BY a LIMIT n`): a top-n of distinct
  rows per branch would need a heap that deduplicates its rows, which no operator has (the top-n
  per group keeps rows per key, not distinct keys); a distinct then a top-n per branch still holds
  every distinct value of the branch. Left as it is.
- Tests: `batch_limit_distinct.yaml` (planner: distinct with a limit, with an offset, on two
  columns, of an expression, over a join hashed on the distinct column, `UNION` of two and three
  branches, inside a count, distributed and local plans; kept: aggregation with calls (also a
  distinct on a primary key, planned with `internal_last_seen_value`), `DISTINCT ON`, top-n, `WITH
  TIES`, offset only, off by default), the unit test of the executor (early end, `NULL`s, a limit
  larger than the values), and the differential `e2e_test/batch/limit_distinct.gen.py` (20 queries
  with a result that does not depend on which distinct rows the limit keeps: counts of the rows of
  the limit, counts of values that appear twice in it, which must be 0, and limits larger than the
  number of distinct rows; local and distributed query mode; NULLs, duplicates, several chunks and
  tasks, updates and deletes, an empty branch, one distinct row).

### IV. Join with a small constant `VALUES` (added on 2026-11-28, survey rule 8)

Source: PostgreSQL 18 `convert_VALUES_to_ANY` (`x IN (VALUES (1), (2))` becomes `x = ANY(...)`).
The prototype's `ConstantRowJoinRule` (G) only handled an inner join with *one* constant row; a
join with several rows stayed a hash join, which in streaming keeps the whole other input in its
state, plus a fragment for the constant rows.

```sql
create table t (k int primary key, code varchar, x int);
with m(code, label) as (values ('a', 'Alpha'), ('b', 'Beta'))
select t.k, m.label from t left join m on t.code = m.code;
-- IV: Project(t.k, CASE WHEN t.code = 'a' THEN 'Alpha' WHEN t.code = 'b' THEN 'Beta' END) <- Scan(t)
select * from t where t.code in (select c from (values ('a'), ('b')) v(c));
-- IV: Filter(t.code IN ('a', 'b')) <- Scan(t)
```

**Rule** (`ValuesJoinInlineRule`, variables `batch_inline_values_join` and
`streaming_inline_values_join`, default false). One side of the join is a `LogicalValues` of 1 to
64 rows of constants of the declared types, read directly, through a share (a CTE read twice) or
through projections (the column a subquery selects, computed on each row); it must be the side
whose rows are neither preserved nor output on their own: either side of an inner join, the right
side of a left outer, left semi or left anti join, the left side of their right counterparts. Let
`cond_i` be the conjuncts of the condition that refer to the constant side, with the values of row
`i` in place of its columns, and `kept` the conjuncts on the other side only. Every conjunct must
be pure, since `cond_i` is evaluated once per row.

- Semi join: `Filter(kept AND (cond_1 OR ... OR cond_n))`; anti join:
  `Filter((kept AND (cond_1 OR ... OR cond_n)) IS NOT TRUE)`. A row of the kept side is output by
  the semi join iff some constant row satisfies the condition, i.e. iff some `kept AND cond_i` is
  true; the anti join keeps it iff none is (`IS NOT TRUE`: a condition that is `NULL` does not
  match, as in the join). Duplicate constant rows do not change either. Any condition qualifies,
  ranges included.
- Inner join: a row of the kept side must match at most one constant row. The rule requires a
  conjunct `k = v.c` (the key), `k` a column of the kept side and `c` one of the constant side of
  the same type, a type whose equal values are identical (integers, `varchar`, `boolean`, dates
  and times, see `is_simple_type`), with distinct literal values. The join becomes
  `Project(Filter(kept AND matched))` where each column `j` of the constant side is
  `CASE WHEN cond_1 THEN r_1[j] ... WHEN cond_n THEN r_n[j] END`: exactly one `cond_i` is true on a
  row that passes the filter, the one of the row it joined. The key column is `k` itself (equal,
  hence identical), a column with the same value in every row is that value.
- Left join (constant side on the right, or a right join with it on the left): no filter, and the
  guards of the `CASE` are `kept AND cond_i`, the whole `ON` condition for row `i`. A row without
  a match gets `NULL` (no `ELSE`), which is the padding of the join.
- Rows whose key, or any column of the constant side compared with `=`, is a `NULL` literal never
  match and are dropped first (if none is left the rule does nothing: the rules on empty relations
  take over). When the only conjunct with the constant side is `k = v.c`, `matched` is the list
  `k IN (c_1, ..., c_n)` without duplicates (`k = c_1` for one value), which becomes scan ranges on
  a key in batch; otherwise it is the `OR` of the `cond_i`.
- `NOT IN` is planned as an anti join on `x = c`; the rule keeps exactly the semantics of that
  join (`(x IN (...)) IS NOT TRUE`), whatever they are: it only replaces the join, not the planning
  of `NOT IN`.

**Placement.** Batch and streaming: after the first predicate pushdown (subqueries are unnested and
`ON` conditions are in place), before join ordering, which would take the constant rows as an
input; the new filter is pushed down right after. The result has no join state, no exchange and
no fragment for the constant rows; the constant side has no watermark, the kept side keeps its own.

- Not done: inner and outer joins without a key of distinct literals (a range join, or duplicate
  keys, would multiply rows: the join is kept); a constant side under a filter;
  `decimal`, floats, intervals as keys of inner and outer joins (equal values may print
  differently, so the key column could not be replaced by `k`); full joins and the preserved side
  of an outer join (every constant row must be output).
- Tests: `stream_inline_values_join.yaml`, `batch_inline_values_join.yaml` (semi joins with
  duplicates and `NULL`s, a single value left, anti joins from `NOT EXISTS` and `NOT IN`, range
  conditions, inner joins with the constant side on either side, a constant column, an extra
  condition, left and right joins, a guard of the preserved side, a `NULL` key, a CTE read twice,
  a side with a watermark, 64 rows; kept: duplicate keys, a `decimal` key, the preserved side, full
  join, only `NULL` keys, 65 rows, no equality, a volatile condition; off by default), and the
  differentials `e2e_test/batch/inline_values_join.gen.py` and
  `e2e_test/streaming/inline_values_join.gen.py` (29 and 28 queries, results with the variable off
  and on, and the batch query, compared as multisets over `NULL` codes, duplicate rows, upserts,
  updates of the key and of the flag, deletes, no match at all and empty tables; the range
  queries, which streaming can only plan with the rule, are compared with batch only).

### OR. Restrictions derived from an `OR` (added on 2026-11-28, survey rule 6)

Sources: PostgreSQL `extract_restriction_or_clauses` (`orclauses.c`), Calcite
`ExpandDisjunctionForJoinInputsRule`. `Condition` only factors out the conjuncts common to every
disjunct of an `OR`; an `OR` whose disjuncts each restrict both sides stayed in the join, and each
side kept all its rows in the join (and its state, in streaming).

```sql
create table a (id int primary key, k int, x int);
create table b (id int primary key, k int, y int);
select a.id, b.y from a join b on a.k = b.k where (a.x = 1 and b.y = 2) or (a.x = 3 and b.y = 4);
-- OR: Scan(a) gets (a.x = 1 OR a.x = 3), Scan(b) gets (b.y = 2 OR b.y = 4); the join keeps the OR
```

**Rule** (`OrRestrictionDeriveRule`, variables `batch_derive_or_restrictions` and
`streaming_derive_or_restrictions`, default false). For each conjunct `d_1 OR ... OR d_n`
(2 to 16 disjuncts) of the condition of a join that refers to both sides, and for each side `X`
into which predicate pushdown moves an `ON` conjunct (`can_push_left_from_on`,
`can_push_right_from_on`: both sides of an inner join, the optional side of an outer join, the
outer side of a left semi join), let `p_i` be the conjunction of the conjuncts of `d_i` that only
refer to `X`. When every `p_i` is non-empty, `p_1 OR ... OR p_n` is added to the condition, which
keeps the `OR`; predicate pushdown then moves it into `X` (and, through the equalities of the join,
into the other side when it may).

- Exactness: a pair of rows that satisfies the `OR` satisfies some `d_i`, hence each of its
  conjuncts, hence `p_i`, hence the derived `OR`: adding an implied conjunct changes no result, for
  any join type. A disjunct without a part on `X` implies nothing on `X` (no derivation). Only
  where pushdown moves the conjunct into `X` (on the preserved side of an outer join, a conjunct of
  the `ON` condition does not remove rows, it would stay in the join for nothing).
- Safety: the parts are made only of column references, literals, comparisons, `IS [NOT] NULL`,
  `IS [NOT] TRUE/FALSE`, `IS [NOT] DISTINCT FROM`, `IN`, `NOT`, `AND` and `OR`: deterministic and
  unable to fail, since they are now evaluated on rows that the join would not have compared
  (`10 / b.z`, `lower(a.s)`, `random()`, `now()` are never derived). Temporal and as-of joins are
  left untouched.
- Form: the equalities and `IN` lists that compare a column with constants are merged into one
  `IN` list per column without duplicates (`c = 'p' OR c = 'q' OR c = 'p' OR c IN ('p', 'z')`
  becomes `c IN ('p', 'q', 'z')`, one equality for a single value), other parts are deduplicated,
  and the condition goes through the usual simplification of conditions.
- Useless restrictions are not derived: a restriction that is a single `IN` list (or equality) on
  a column whose values other than `NULL` are a known set of literals, all in the list, removes
  no row the join keeps (only `NULL`s, which the join condition rejects anyway). The known values
  come from a literal or a `CASE` whose results are all literals in a projection, or the literals
  of a `VALUES`, followed through filters, projections of the column, joins (each side, `NULL`s
  of outer joins aside), unions (all branches), group keys, shares, dedups, top-n and limits.
  Example: `CASE WHEN y > 2 THEN 1 ELSE 0 END = 0 OR ... = 1`, or the constant tag of each branch
  of a union compared with every tag: before this check, such a tautology was evaluated on every
  row for nothing. A `CASE` compared with some of its values is still derived when it is a
  column of a projection under the join (batch); in streaming it is usually inlined into the join
  condition, where `CASE` is not a safe part, and only the other side is restricted.
- Placement: batch and streaming, after the first predicate pushdown (the `WHERE` conjuncts of
  inner joins are in the join conditions), before join ordering; predicate pushdown right after.
  The rule adds a conjunct only if the condition changes (a join reached again through a shared
  sub-plan is not extended twice).
- Not done: a `WHERE` filter above an outer join (its preserved side could be restricted too);
  derivation inside a single scan for index ranges (Calcite `ExpandDisjunctionForTableRule`); the
  derived `IN` list is not intersected with an `IN` list already on that side (both stay as
  conjuncts of the same filter).
- Tests: `stream_derive_or_restrictions.yaml`, `batch_derive_or_restrictions.yaml` (inner joins with
  one or several parts per side, `IS NULL`, `IN`, a disjunct with one side only, no equality, left,
  right and semi joins, three tables, scan ranges on a primary key; kept: an `OR` on one side, the
  preserved side of a left join, anti and full joins, parts that may fail, volatile parts, `now()`;
  form: equalities and `IN` lists merged without duplicates, a value common to every disjunct
  (factored out before the rule), a `CASE` compared with some of its values; not derived: a `CASE` with or without `ELSE` compared
  with all its values, the constants of union branches compared with all of them; off by
  default), and the differentials `e2e_test/batch/derive_or_restrictions.gen.py` and
  `e2e_test/streaming/derive_or_restrictions.gen.py` (23 and 22 queries compared with the variable
  off and on, and with batch, over `NULL` keys and compared columns, duplicates, upserts, updates of
  the compared columns and of the keys, deletes and empty sides).

### OU. Join on an `OR` of equi-conditions (added on 2026-11-28, survey rule 7)

Source: Calcite `JoinExpandOrToUnionRule`. A join whose condition has no equality between its
sides, such as `a.id = b.id1 OR a.id = b.id2`, can only be a nested-loop join: batch compares every
pair of rows, streaming refuses the query ("streaming nested-loop join").

```sql
create table a (id int primary key, k int, x int);
create table b (id int primary key, id1 int, id2 int, y int);
select a.id, b.y from a join b on a.id = b.id1 or a.id = b.id2;
-- OU: Join(a.id = b.id1) UNION ALL Join(a.id = b.id2 AND (a.id = b.id1) IS NOT TRUE)
```

**Rule** (`OrJoinToUnionRule`, variables `batch_or_join_to_union` and `streaming_or_join_to_union`,
default false). The condition has no equality between the sides (`=` or `IS NOT DISTINCT FROM`
between a column of each), but a conjunct `d_1 OR ... OR d_n`, 2 to 4 disjuncts, each with such an
equality; every conjunct is pure (evaluated in several joins). Let `r` be the other conjuncts and
`c_i = d_i AND r`, so that the condition is `c_1 OR ... OR c_n`.

- Inner join: `UNION ALL` over `i` of `A JOIN B ON c_i AND d_1 IS NOT TRUE AND ... AND
  d_{i-1} IS NOT TRUE`. A pair satisfies the condition iff some `c_i` is true; it is output by the
  branch of the first such `i` and by no other (in branch `i`, `r` is true, so excluding `c_j` is
  excluding `d_j`). `IS NOT TRUE` lets through a pair for which `d_j` is `NULL` (it does not satisfy
  `d_j` either), where `NOT d_j` would lose it: each pair is output exactly once.
- Left semi join: `UNION ALL` over `i` of `R_{i-1} SEMI JOIN B ON c_i`, with `R_0 = A` and
  `R_i = R_{i-1} ANTI JOIN B ON c_i` (the rows of `A` with no match on `c_1`, ..., `c_i`). A row of
  `A` is in the result iff it matches some `c_i`; it is output by the branch of the first such `i`
  only, whatever the duplicates of `A` or `B`.
- Left anti join: `R_n`: a row has no match on `c_1 OR ... OR c_n` iff it has none on any `c_i`.
- Left outer join: the inner join above `UNION ALL` the rows of `R_n` padded with `NULL`s, which is
  the definition of the outer join (matched pairs, plus the unmatched rows padded).
- In streaming, the sides and the `R_i` read twice are shared (`LogicalShare`, when the plan may
  share sub-plans): each is computed once, each equi-join keeps its own state (the price, hence the
  cap of 4 disjuncts). In batch the sides are read once per join, so the rule requires that two
  reads give the same rows: no volatile expression (a `random()` filter pushed into a side would
  otherwise be drawn again in each branch) and no source. Right outer, right semi and right
  anti joins, full joins and temporal joins are left untouched.
- Placement: batch and streaming, after the first predicate pushdown and after rule OR (whose
  restrictions are then below the shared sides), before join ordering, which then orders the
  joins of each branch; predicate pushdown right after.
- Not done: right joins (rare: written as left joins), full joins (`FULL JOIN` would need the
  unmatched rows of both sides), disjuncts without an equality, more than 4 disjuncts. Rows that
  match through `IS NOT DISTINCT FROM` work like any other equality.
- Tests: `stream_or_join_to_union.yaml`, `batch_or_join_to_union.yaml` (inner joins with 2 or 3
  disjuncts, an extra conjunct, disjuncts of several conjuncts, the `OR` in a `WHERE` clause, an
  aggregation above, left, semi and anti joins; kept: a disjunct without equality, five disjuncts,
  full join, an equality outside the `OR`, a volatile condition; off by default), and the
  differentials `e2e_test/batch/or_join_to_union.gen.py` (19 queries, nested loop against the
  rewrite) and `e2e_test/streaming/or_join_to_union.gen.py` (the same 19 queries materialized with
  the variable and compared with batch; the query refused without the variable is checked to fail),
  over pairs matched by one disjunct, by both (`id1 = id2`), by none, `NULL` keys, duplicate rows on
  the left, upserts, keys moved between the disjuncts, deletes and empty sides.

### TP. Transitive predicates and implied equalities (added on 2026-11-28, done on the branch)

Variables `streaming_transitive_predicates` and `batch_transitive_predicates`, rule
`TransitivePredicateRule`; implied equalities in `cost_based_join_ordering_rule.rs`. Sources:
Calcite `JoinPushTransitivePredicatesRule` with `RelMdPredicates`, the equivalence classes of
PostgreSQL (`generate_base_implied_equalities_const` and `_no_const`).

Predicate pushdown derives, through the equalities of a join, the predicates that it pushes into
the join from above or from its condition (`derive_predicate_from_eq_condition`). A predicate that
is already inside an input never meets the join: the filter of a view, the predicate of a scan, a
constant column, the tags of the branches of a `UNION ALL`. The rule copies it:

```text
CREATE VIEW eu_o AS SELECT * FROM o WHERE region = 'EU' AND id > 1000;
SELECT ... FROM eu_o JOIN s ON s.region = eu_o.region AND s.order_id = eu_o.id;

Join(o.region = s.region AND o.id = s.order_id)       Join(...)
├─Scan(o, region = 'EU' AND id > 1000)          =>     ├─Scan(o, region = 'EU' AND id > 1000)
└─Scan(s)                                              └─Scan(s, region = 'EU' AND order_id > 1000)
```

- Predicates that hold on every row of a node (`pulled_predicates`): the conjuncts of filters and
  scan predicates; through projections (a predicate on the input rewritten on the output columns
  that compute the same pure expressions; a literal column `c = v`), the sides of a join that are
  not padded (with the condition of an inner join), unions (the predicates of every branch, and
  `c IN (...)` for a column that every branch fixes to a few values: the tags), `VALUES` (`c IN`
  the literals of the column), the group key of an aggregation, and the operators that only
  select rows (share, dedup, top-n, limit, over window). At most 32 per node, `IN` lists of at
  most 64 values.
- Copied: the deterministic predicates (columns, literals, pure functions: no `now()`, sub-query
  or user-defined function) whose columns are all keys compared with `=` (same type), or with
  `IS NOT DISTINCT FROM` when both columns are never `NULL`, rewritten on the keys of the other
  side, unless the other side already holds them, and except `k IS NOT NULL` (implied by `=`).
- Directions: inner and semi joins both ways; left outer and left anti joins from the left side
  into the right side only (an unmatched right row is never output; an unmatched left row is);
  right joins symmetrically; never for full outer, as-of or temporal joins.
- Valid: a predicate `p` holds on every row `l` of a side; for a matching pair `l.k = r.k`, `p`
  gives the same result on `r.k` when the two equal values are indistinguishable by `p`, so the
  rows of the other side for which `p` is not `TRUE` match nothing, and only an unmatched row of
  the other side could be output, which the directions above exclude. Equal values are
  indistinguishable for integers, booleans, `varchar`, `bytea`, dates, times and timestamps; for
  `numeric`, floats and `interval` (`1.0 = 1.00`, `-0.0 = 0.0`, `'1 day' = '24 hours'`) only
  comparisons with constants and `NULL` tests are copied (the same restriction as the correction
  of `derive_predicate_from_eq_condition`, see `docs/fork/corrections/`; the two copies of the
  check are to be merged into one helper). A deterministic row filter keeps or drops a row the
  same way when it is inserted and when it is deleted: exact under retractions and duplicates, for
  any future data, without statistics.
- Transitivity: the rule is applied bottom-up, so a join sees the filters that it added below it,
  through the inner joins of its inputs (`a.x = b.y`, then `b.y = c.z`: the filter of `a` reaches
  `c`); predicate pushdown then moves the new filters into the scans and derives them further down
  through the joins below. Two rounds at most; a round that adds nothing ends the loop.
- Gains: streaming, the rows of the other side that can not match never enter the join state, its
  backfill or the exchanges below the join; a constant on a prefix of the primary key becomes a
  `pk_scan_range`. Batch, the copied constants and ranges become scan ranges (with lookup joins off,
  or when the hash join is chosen) and fewer rows are shuffled.
- Implied equalities (batch, with `batch_cost_based_join_ordering`): the columns compared with `=`
  in the inner joins of a multi-join form equivalence classes; two inputs that hold columns of the
  same class but that no written equality links get the implied equality of their first columns
  (`a.x = b.y AND b.y = c.z` gives `a.x = c.z`), so the dynamic programming can join `a` with `c`
  first. `=` is an equivalence relation for every type a hash join compares, and the multi-join
  only merges inner joins: the result is unchanged. The estimate of a set of inputs counts the
  selectivity of an implied equality only when the inputs it links are not already linked within
  the set by written equalities (or implied ones counted before): `{a, c}` counts it, `{a, b, c}`
  does not. Only the implied equalities that the chosen tree needs are added to the condition:
  those between two sub-trees that no equality of the same class links. Without statistics, or
  when the tree is the heuristic one, the plan is unchanged.
- Not done: predicates on expressions of the keys (`a.k + 1 = b.j`); copies through `OR`
  conditions (idea 6 of the survey); implied equalities in the streaming orderings (the bushy
  ordering already derives them in one step); same-input implied equalities as filters.
- Risk: the batch estimates count a copied filter on both sides of a join whose selectivity
  already accounts for it (PostgreSQL documents the same problem); the learned selectivities of
  `batch_cardinality_feedback_learning` correct it once observed.
- Streaming: an `IN` list or an `OR` is not copied onto a column that may reach a column of the
  primary key or of an index of a scanned table (followed through projections, filters, joins,
  unions, group keys, hop windows and the operators that keep their input columns): the scan would
  become one scan per value under a `UNION ALL` (`StreamingIndexSelectionRule`), each subscribing
  to all the changes of the table and backfilling it (upstream case "test hop window subquery 1").
  A single value is copied (one range scan replaces the full scan). Batch reads the ranges in one
  scan and copies them all.
- An equality of two columns `x = y` is not copied into a side whose top inner join (below
  projections and filters) has `x` and `y` on different inputs while one of them already is a key
  of that join: the join would be keyed on `(x, x) = (z, y)`, a new distribution and one more
  exchange, for a filter `z = y` of one input (the six scalar subqueries of the upstream case
  "improve multi-scalar subqueries optimization time": one more exchange per subquery).
- Stages: `TRANSITIVE_PREDICATES` (bottom-up) after the first predicate pushdown and before join
  ordering, in both pipelines, each round followed by predicate pushdown.
- Tests: planner tests `stream_transitive_predicates.yaml`, `batch_transitive_predicates.yaml`;
  differential tests `e2e_test/streaming/transitive_predicates` and
  `e2e_test/batch/transitive_predicates` (numeric `1.0` / `1.00`, floats `-0` / `0`, `NULL` keys,
  duplicates, outer, semi and anti joins, unions, `VALUES`, contradictions, empty sides, updates of
  keys and of filtered columns, deletes).

### UA. Batch: partial aggregation in the branches of a `UNION ALL` (added on 2026-11-28, done on the branch)

Rule `AggUnionTransposeRule` (`optimizer/rule/agg_union_transpose_rule.rs`), variable
`batch_partial_agg_into_union` (default false); Calcite `AggregateUnionTransposeRule`, PostgreSQL
partitionwise aggregation.

```text
Agg(group k: count(*), sum(x))           Agg(group k: sum0(n), sum(s))
└─Union all                         =>   └─Union all
  ├─a                                      ├─Agg(group k: n = count(*), s = sum(x))
  └─b                                      │ └─a
                                           └─Project(k, 1, x::bigint)
                                             └─b
```

- Every aggregate decomposes, as in the partial form of 5; the partial aggregate is the aggregate
  itself over the rows of a branch, with its `FILTER`, and the final one combines the partial
  results with the same type. A group of the union is the union of the groups with its key in the
  branches. Without group key a branch gives one row even when empty (a count of 0, `NULL` for the
  others), which the final aggregate adds as 0 or ignores: an empty union still gives one row with
  the same values.
- Per branch, by cost: a branch without a unique key within the group key is aggregated when there
  is no group key (one row per branch, always worth it), or when the largest learned group ratio of
  its partial aggregation is at most 0.5. Another branch gives each row as a partial result of its
  own (`count(*)` is `1`, `count(x)` is `1` or `0`, `sum(x)` is `x` cast to the type of the sum,
  `FILTER` as a `CASE`), so that the union stays uniform. At least one branch must be aggregated;
  the rule then stops on what it built (the aggregated branches are unique on the group key).
- Projections between the aggregation and the union must be deterministic; they are computed in
  every branch. Not for grouping sets, unions with a source column or `UNION` (an aggregation over
  a `UNION ALL` by then).
- With BU, each branch then hashes its groups instead of its rows to the final aggregation.
- Tests: `batch_partial_agg_into_union.yaml` (no group key, both or one branch learned, a
  projection above the union, nothing learned, too many groups, a unique branch, distinct count,
  float sum, ordered aggregate, `UNION`, off by default) and the differential
  `e2e_test/batch/push_agg_below_join.gen.py` (all three batch variables off, then on, with the
  group ratios learned from plain aggregations of the tables; empty tables, NULL keys,
  duplicates, rows without match, updates and deletes, emptied sides).

### S12. `INTERSECT` / `EXCEPT` as a counting aggregation (added on 2026-11-28, done on the branch)

Variables `streaming_set_operation_to_count` and `batch_set_operation_to_count`, rule
`SetOperationToCountRule` (Calcite `IntersectToDistinctRule`, `MinusToDistinctRule`). Without it,
`INTERSECT` and `EXCEPT` (distinct) become left semi or anti joins on `IS NOT DISTINCT FROM` of
every column, followed by a `DISTINCT` (`IntersectToSemiJoinRule`, `ExceptToAntiJoinRule`).

```text
SELECT a, b FROM t INTERSECT SELECT a, b FROM u

Project(a, b)
└─Filter(c0 > 0 AND c1 > 0)                  -- EXCEPT: c0 > 0 AND c1 = 0
  └─Agg(group by a, b: count(m0) AS c0, count(m1) AS c1)
    └─Union(all)
      ├─Project(a, b, m0 = 1, m1 = NULL) <- t
      └─Project(a, b, m0 = NULL, m1 = 1) <- u
```

- A group is a distinct row of the inputs (grouping treats `NULL`s as equal, as the set
  operations do); `c i` counts its copies in input `i`. The row is output once when it is in every
  input (`INTERSECT`), or in the first one and in no other one (`EXCEPT`). More than two inputs
  (after the merge of stacked set operations) get one count each.
- Only when every column has a type in which equal values are identical (integers, `varchar`,
  `bytea`, `boolean`, dates and times): the value that a group outputs is that of one of its rows,
  possibly from another input than the first one (in streaming, the row that created the group).
  With `numeric` (`1.0` and `1.00`), floats (`0.0` and `-0.0`) or intervals, the joins output the
  value of the first input; the aggregation may not, so the rule is not applied.
- Streaming: one aggregation state with each distinct row of the inputs once, instead of the two
  join states (every row of each input), the degree table and the state of the `DISTINCT`. The
  counts are value states (`count`), retractable. Batch: one hash aggregation instead of a hash
  join and a hash aggregation. Without `batch_distributed_union` the `UNION ALL` gathers both
  inputs on one node before the exchange to the aggregation, which is worse than the distributed
  hash join: use the batch variable only together with `batch_distributed_union`.
- Not over constant rows only (every leaf of every input is a `VALUES`): constant folding reduces
  `SELECT 1 EXCEPT (SELECT 2 EXCEPT SELECT 1)` and the stacked set operations of constants of the
  upstream tests to one `Values`, which the aggregation over a `UNION ALL` of the rows prevented
  (found by the public benchmark measurement of 2026-11-28).
- Stage: `SET_OPERATION_TO_COUNT`, right after `SET_OPERATION_MERGE` and before
  `SET_OPERATION_TO_JOIN`, in both pipelines.
- `INTERSECT ALL` and `EXCEPT ALL` are rejected by the binder ("not implemented"); their rewrite
  (a `generate_series` over the smallest count or the difference) is not done.
- Tests: planner tests `stream_set_operation_to_count.yaml` (batch and streaming plans);
  differential tests `e2e_test/streaming/set_operation_to_count` and
  `e2e_test/batch/set_operation_to_count` (NULLs, duplicates, three inputs, `EXCEPT` then
  `INTERSECT`, under `IN`, inputs emptied and filled again, a `numeric` case left as it is).

### S13. Set operations of filters over the same input (added on 2026-11-28, done on the branch)

Variables `streaming_merge_set_operation_filters` and `batch_merge_set_operation_filters`, rule
`SetOperationFilterMergeRule` (Calcite `SetOpToFilterRule`). Extends US (`UnionScanMergeRule`,
which reads once the table of the branches of a `UNION ALL`) to the distinct set operations.

```text
SELECT k, a FROM t WHERE p0  UNION      SELECT k, a FROM t WHERE p1   =>  DISTINCT .. WHERE p0 OR p1
                             INTERSECT                                =>  DISTINCT .. WHERE p0 AND p1
                             EXCEPT                                   =>  DISTINCT .. WHERE p0 AND (p1) IS NOT TRUE
```

- Branches: the form `(Project | Filter)* base` of US (`branch_of`), the base a scan of a table
  (not in the past) or a shared sub-plan; the merged branches read the same base with the same
  output expressions `π`, pure and without correlated references.
- `UNION`: `{π(x) | p0(x)} ∪ {π(x) | p1(x)} = {π(x) | p0(x) OR p1(x)}` holds for any `π`.
- `INTERSECT` / `EXCEPT`: two rows `x`, `y` of the base with `π(x) = π(y)` may pass different
  filters. Exact when the conjuncts of each branch but the first, besides those common to all,
  have the same value on such rows: they read only columns that `π` outputs as they are, of a type
  in which equal values are identical, or `π` outputs the whole key of the table (two rows with the
  same output are then the same row). `NULL`s compare equal in set operations, so an output column
  has the same value, `NULL` included. Proof sketch in the rule's documentation.
- `UNION` and `INTERSECT` merge any group of branches over the same base and keep the others;
  `EXCEPT` merges into its first input the later inputs over the same base (`AND .. IS NOT TRUE`)
  and merges with `OR` the later inputs over another common base (their union is subtracted). A
  single input left becomes a `DISTINCT`.
- Streaming: an `INTERSECT` or `EXCEPT` loses its join (two states holding every row of the base)
  and the base is read once; a `UNION` reads once without the `Expand` copies of US. A table with a
  watermark is left alone in streaming (as in US). After the rule, a shared scan that only the
  merged branches read is unshared (`prune_share`).
- Stage: `STREAM_MERGE_SET_OPERATION_FILTERS` / `BATCH_MERGE_SET_OPERATION_FILTERS`, right after
  `SET_OPERATION_MERGE`, before S12 and `SET_OPERATION_TO_JOIN`.
- Not done: branches that differ by their projection (`INTERSECT` would need the inverse of the
  projection), `INTERSECT` / `EXCEPT` whose filters read columns that are not output without the
  key of the table.
- Tests: planner tests `stream_merge_set_operation_filters.yaml` (batch and streaming plans);
  differential tests `e2e_test/streaming/merge_set_operation_filters` and
  `e2e_test/batch/merge_set_operation_filters` (filters with NULLs, values moved across the
  filters by updates, three branches over two tables, kept cases: a filter on a column not output,
  a `numeric` column, different projections, different tables).

### S9. Group keys and aggregates of values of the group (added on 2026-11-28, done on the branch)

Variables `streaming_reduce_group_keys` and `batch_reduce_group_keys`, rule
`AggGroupKeyReduceRule` (Calcite `AggregateRemoveDuplicateKeysRule`,
`AggregateReduceFunctionsOnGroupKeysRule`, `AggregateProjectPullUpConstantsRule`; PostgreSQL
`remove_useless_groupby_columns`). `AggGroupBySimplifyRule` (always on) only narrows a group key
that is a key of the whole input, which column pruning makes rare once an aggregate reads another
column.

```text
SELECT o.id, o.placed, sum(i.qty) FROM o JOIN i ON i.o_id = o.id GROUP BY o.id, o.placed
  => GROUP BY o.id, internal_last_seen_value(o.placed)
SELECT a, b, count(*) FROM t WHERE b = 3 GROUP BY a, b    => GROUP BY a, last seen value of b
SELECT k, max(k), min(k) FROM t GROUP BY k                => GROUP BY k, projected three times
```

- The functional dependencies of the input (keys of tables and of aggregations, `a = b` and
  `a = literal` conjuncts of filters and inner joins, the left side of a left join) and the
  literal columns of a projection right below. A group key column that the other kept ones
  determine is removed, from the last one; at least one column is kept (an aggregation without
  group key returns a row on an empty input). Its value is that of any row of the group
  (`internal_last_seen_value`, which takes `NULL` as a value and keeps its state on retractions).
- `min`, `max`, `first_value`, `last_value`, `internal_last_seen_value` without `FILTER` of a column
  that the kept group key determines: a kept group key column is read from the group key, another
  one from a single `internal_last_seen_value` call. Valid with `NULL`s: the dependencies hold
  with `NULL` as a value, and `min` of `NULL`s is `NULL`.
- Streaming: a narrower state key (and primary key of a materialized view); a `min` / `max` over an
  input that retracts rows keeps every value of the group (materialized input state), replaced by
  one value. Not under a source, `now()` or a table with a watermark: a column with a watermark
  must stay in the group key (state cleaning, `EMIT ON WINDOW CLOSE`).
- Not for an aggregation without group key: it has a row when no row is left (empty input, all
  rows retracted in streaming, a partition without rows in the first phase of a two-phase batch
  aggregation), where `max` is `NULL` but `internal_last_seen_value` keeps the last value seen.
  The differential tests found this on a first version (`max(b) ... WHERE b = 3` without
  `GROUP BY`, wrong in both batch and streaming).
- A key column is kept when removing it would lose the distribution of the input: the hash
  distributions that the input will likely have once physical (a hash join by the key of its side
  that is not padded, a scan by the distribution key of its table, an aggregation by its group
  key, through projections and filters) are computed; when one is within the group key and would
  no longer be after the removal, the column stays (otherwise every input row is exchanged again,
  as on the upstream case "CorrelatedInputRef in ProjectSet and apply on condition refers to no
  table function": `GROUP BY x, k` over a join on `(x, k)` with `k -> x`).
- After the rule only column pruning runs; the projections are merged by the `PROJECT_REMOVE` of
  the pipeline. A second `PROJECT_REMOVE` pulled the computations of both sides of a join above it
  (`JoinProjectTransposeRule` once more), and the `ORDER BY` above then got a third stacked sort
  (TPC-DS q39).
- Stage: `STREAM_REDUCE_GROUP_KEYS` after the last stages of the prototype (after the final column
  pruning, so that the dependencies are those of the columns read), then column pruning;
  `BATCH_REDUCE_GROUP_KEYS` after the status ladder stage.
- Not done: partition keys of windows, group keys and order keys of top-n (the same reduction
  applies; smaller gains), keys made constant by an equivalence class (only direct `= literal`
  conjuncts and literals are seen).
- Tests: planner tests `stream_reduce_group_keys.yaml` (batch and streaming plans); differential
  tests `e2e_test/streaming/reduce_group_keys` and `e2e_test/batch/reduce_group_keys`
  (determined columns updated, set to and from `NULL`, orders without items, left joins, keys of a
  sub-aggregation, kept cases).

### S10. Branches of a `UNION ALL` that only feed `NULL` to an aggregation (added on 2026-11-28, done on the branch)

Variables `streaming_prune_null_union_branches` and `batch_prune_null_union_branches`, rule
`AggUnionNullBranchRule` (Calcite `AggregateUnionNullBranchRule`). "Metrics" views union per-kind
branches that each fill one measure and leave the others `NULL`.

```text
SELECT sum(m) FROM (SELECT x AS m FROM a UNION ALL SELECT NULL::int FROM b) v
  => SELECT sum(m) FROM (SELECT x AS m FROM a) v
SELECT k, sum(m) FROM (... UNION ALL SELECT k, NULL FROM b GROUP BY k) v GROUP BY k
  => the GROUP BY of the second branch becomes a projection
```

- A branch is irrelevant when every aggregate call ignores rows whose argument is `NULL` (`sum`,
  `sum0`, `min`, `max`, `count(x)`, `avg`, `stddev_*`, `var_*`, `bit_*`, `bool_*`) and the argument
  of every call is `NULL` in every row of the branch (`NULL` literals, `x IS NULL` filters, the
  analysis of NK; the group key columns of a `DISTINCT` at the top of the branch). One `count(*)`,
  `array_agg`, `string_agg`, `first_value`... keeps every branch, as does a branch that is `NULL`
  for some calls only.
- Without group key: irrelevant branches are removed; if none is left, the aggregation reads an
  empty `VALUES` and returns its row over no input (`count` 0, the others `NULL`), the same values
  as over rows whose arguments are all `NULL`.
- With a group key, an irrelevant branch still creates groups and stays; the number of copies of
  its rows does not matter (no call counts them), so an aggregation without calls at its top (a
  `GROUP BY` or `DISTINCT`, under a projection or not) becomes a projection of its group key.
- Streaming: a removed branch no longer scans, backfills or keeps state; a removed `DISTINCT` no
  longer keeps its state, and its duplicates do not grow the state above (`min` and `max` skip
  `NULL` arguments in their materialized input, the others keep a value per group).
- Stage: `PRUNE_NULL_UNION_BRANCHES`, right before S9 in both pipelines, then column pruning.
- Tests: planner tests `stream_prune_null_union_branches.yaml` (batch and streaming plans);
  differential tests `e2e_test/streaming/prune_null_union_branches` and
  `e2e_test/batch/prune_null_union_branches` (no group key, all branches `NULL`, a branch `NULL` by
  its filter, a `GROUP BY` and a `DISTINCT` removed, kept cases with `count(*)`, `array_agg`, a
  call per branch). Streaming sums are wrapped in `coalesce(.., 0)` (see the known quirk of a
  streaming `sum` whose values were all retracted).

### SJ. Batch join rules of the survey of Calcite and PostgreSQL rules (added on 2026-11-28)

Rules 14 to 18 of the survey of the rewrite rules of Calcite and PostgreSQL missing from the fork,
for batch queries only, each behind its own variable (default false). Branch
`proto/plan-reduction-batch-joins`.

**17. Inner join to semi join (`batch_inner_join_to_semi_join`, Calcite `SemiJoinRule`).**

```sql
select t.* from t join (select distinct k from u) x on t.k = x.k;
-- before: HashJoin(Inner, t.k = u.k) <- Scan(t), HashAgg(group u.k) <- Scan(u)
-- after:  HashJoin(LeftSemi, t.k = u.k) <- Scan(t), HashAgg(group u.k) <- Scan(u)
--         and, when the estimates say the aggregation keeps at least half of its rows:
--         HashJoin(LeftSemi, t.k = u.k) <- Scan(t), Scan(u)
```

- `InnerJoinToSemiJoinRule`: an inner join with an equality between its sides, no column of one
  side in its output, and that side unique on the columns its condition binds
  (`is_unique_on_compared_columns`: equalities between the sides or with constants, on types whose
  equal values are identical) becomes a left (or right) semi join with the same condition. A row
  of the kept side matches at most one row, so the inner join outputs it once or not at all: the
  semi join's output. The other conjuncts only remove matches. `IS NOT DISTINCT FROM` keys are
  fine (the unique key treats `NULL` as a value). The unique side is a `DISTINCT` subquery, a
  `GROUP BY` whose aggregates are not read (the column pruning drops them), or a table joined on
  its key (`count(*)` over a join to a dimension, a join used as a filter).
  Two exclusions: (1) the condition needs an equality between a column of each side where neither
  column is constant (`column_facts`): an equality between constant columns, such as
  `1 = 1` after a projected constant is joined with a one-row `VALUES (1)`, is a cross join in
  disguise, and a semi join on it would hash every row on the constant (all rows in one partition,
  a hidden single-task gather) and would keep the join, where the reductions on keys and constants
  fold the inner join away and let the other filters reach the scans; (2) the tested side must not
  hold at most one row (a one-row `VALUES`, possibly under projections, a scalar subquery): the
  inner join already outputs each kept row at most once, the semi join gains nothing, and the
  one-row join rules handle the inner join.
- `SemiJoinDistinctRemoveRule`: a semi or anti join whose tested side is an aggregation without
  aggregate calls and with a group key (an empty group key returns a row over an empty input) gets
  the input of the aggregation, projected on the group key, instead. Exact: a semi join keeps a
  row when some row matches, an anti join when none does, neither depends on how many. Not always
  cheaper: the batch hash join holds every row of its build side, duplicates included, where the
  aggregation held each group once (the hash table of the aggregation plus the build side of the
  distinct rows, against the build side of all the rows). Removed only when the estimates of BJ
  (learned with `batch_cardinality_feedback_learning`) say the aggregation keeps at least half of
  its input rows: grouped by a key of its input, or a learned group ratio of 0.5 or more. Without
  an estimate, or with the fixed guess of 10 % of groups, it stays. This also applies to
  `IN (SELECT DISTINCT ...)` as written.
- Stage `BATCH_INNER_JOIN_TO_SEMI_JOIN`, after the join ordering (the inner join was ordered as
  such) and the first column pruning (the output of a join holds only the columns read above it),
  just before `JOIN_COMMUTE` (a right semi join becomes a left one) and before
  `CostBasedOuterJoinSidesRule` (the sides of the semi join can still be swapped by cost).
- Not done: an inner join whose other side is read only through columns equal to columns of the
  kept side (they could be replaced); a semi join on a side that is not unique and that a
  `DISTINCT` could make unique (PostgreSQL's unique-ification, a cost decision).
- Tests: planner tests `batch_inner_join_to_semi_join.yaml` (distinct side on either side, table
  joined on its key, `count(*)`, null-safe key, extra conjunct; kept: a column read, not unique on
  the compared columns, no equality, outer join, equality between constant columns (alone and
  with the reductions that then fold the join), constant column against a table key, scalar
  subquery, one-row `VALUES`; removal of the aggregation with statistics on a
  key, with nothing learned, with a learned ratio high and low, under an anti join, empty group
  key kept; off by default); differential test `e2e_test/batch/inner_join_to_semi_join.gen.py`
  (23 queries with the variable off and on, compared as multisets, on empty tables, duplicates and
  `NULL` keys, a distinct side with few and with many groups, updates, deletes, each side emptied,
  with a pause after each change so that the storage statistics reach the frontend).

**18. `IS NOT NULL` on equi-join keys (`batch_join_keys_not_null`, Calcite
`JoinDeriveIsNotNullFilterRule`).** `JoinKeyNotNullRule` adds, below a join, a filter
`c IS NOT NULL` on each column of an input that a top-level conjunct `l = r` of the condition
compares with a column of the other input, on the sides whose rows without a match are not
output: both sides of an inner or semi join, the right side of a left outer or left anti join,
the left side of a right outer or right anti join. `NULL = x` is `NULL` for every `x`, so such a
row matches nothing, whatever the other conjuncts. The predicate pushdown that follows moves the
filters into the scans: the rows with a `NULL` key are no longer shuffled by the exchanges of the
hash join.

- Not for a column already known to be never `NULL` (column facts: `NOT NULL` constraint, a
  null-rejecting filter or join condition below, which also covers the keys of the inner joins
  below), so the rule adds a filter once and never twice; not for `IS NOT DISTINCT FROM`, full
  outer or as-of joins; not for the right side of a possible lookup join (a table read by key or
  with an index: it is not scanned, and a lookup with a `NULL` key finds nothing).
- Stage `BATCH_JOIN_KEYS_NOT_NULL` after the join ordering (the cost-based ordering would count
  the new conjuncts in its estimates, as the survey warns) and after `PUSH_CALC_OF_JOIN` (a key
  computed by an expression is a column of a projection below the join; the filter is pushed
  through it as `expr IS NOT NULL`), then a predicate pushdown. The fingerprints of the learned
  ratios of BF step 2 include the filters of a scan: a scan with the new filter is a new relation
  for the learning.
- Streaming: rejected by the design note (no state gain: a `NULL` key already costs no join state).
- Tests: planner tests `batch_join_keys_not_null.yaml` (inner, `NOT NULL` column, key filtered
  below, left, right, full, semi, anti, null-safe, lookup, expression key, three-way join; off by
  default); differential test `e2e_test/batch/join_keys_not_null.gen.py` (16 queries, `NULL` keys
  on both sides, duplicates, keys set to `NULL` and back, a side with only `NULL` keys, deletes).

**15. Range form of date predicates (`batch_date_range_predicates`, Calcite `DateRangeRules`).**

```sql
date_trunc('month', ts) = '2024-05-01'   =>  ts >= '2024-05-01 00:00:00' AND ts < '2024-06-01 00:00:00'
ts::date <= '2024-05-01'                 =>  ts < '2024-05-02 00:00:00'
extract(year from d) between 2020 and 2022  =>  d >= '2020-01-01' AND d < '2023-01-01'
date_trunc('month', d) = '2024-05-01'    =>  d >= '2024-05-01' AND d < '2024-06-01'   -- d a date
```

`DateRangePredicateRule` rewrites, in the predicate of a filter or of a scan (after the first
predicate pushdown, in place), each top-level conjunct comparing (`=`, `<`, `<=`, `>`, `>=`, the
constant on either side) with a constant: `date_trunc(unit, x)` for units `second` to `year` (not
`decade` and larger: their conventions for the years before Christ differ), `x::date`,
`extract(year from x)` and `date_part('year', x)`. The scan then turns the bounds into key ranges
when `x` leads the key of the table.

- Exactness: a truncation `f` is non-decreasing, idempotent and below its argument, so for an
  aligned value `a` (`f(a) = a`), `f(x) < a` exactly when `x < a`. With `up(c)` the smallest
  aligned value at or above `c` and `after(c)` the smallest above `c`: `f(x) < c` is
  `x < up(c)`, `>=` is `x >= up(c)`, `<=` is `x < after(c)`, `>` is `x >= after(c)`, `=` is
  `c <= x < after(c)` when `c` is aligned (never true otherwise: left as it is). The truncation
  is the one of `date_trunc` itself (`Timestamp::truncate_*`); the next aligned value adds one
  unit and is checked to be aligned and larger. A cast to `date` is the truncation to the day;
  the year of `extract` is an integer, `y(x) < n` exactly when `x` is before January 1 of `n`, for
  bounds from year 1 to 9999 (both conventions for the years before Christ make them smaller).
  `NULL` gives `NULL` in both forms. `x` must be deterministic (written twice for `=`).
- Time zones: a function of a `timestamptz` uses the session time zone, which RisingWave writes
  into the expression (`date_trunc(u, tz, zone)`, `extract(year from tz, zone)`,
  `(tz AT TIME ZONE zone)::date`, a `date` given to `date_trunc` as
  `d::timestamp AT TIME ZONE zone`) only after the logical optimization; during it the call has no
  zone argument (`date_trunc(u, tz)`, `tz::date`, `date_trunc(u, d::timestamptz)`). Both forms are
  handled, the first with an explicit `UTC` zone, the second when the session time zone (read
  from the optimizer context) is `UTC`; so is a text without offset compared with a
  `timestamptz` (`date_trunc('month', d) = '2024-05-01'`), a cast that cannot be folded before
  the zone is inlined and is read as in a `UTC` session. A `timestamptz` is handled only in `UTC`
  (or `Etc/UTC`): there a `timestamptz` behaves as a `timestamp`. In other zones a day or an hour
  may be shorter or longer, and the truncation of a local time that does not exist
  or exists twice depends on the zone: left as it is. The bounds then go through the increasing
  conversions of `x` (`AT TIME ZONE 'UTC'` and the casts between `timestamp` and `timestamptz` in
  a `UTC` session keep the value; `d::timestamp >= v` is `d >=` the first day at or after `v`), so
  that the bounds apply to the column itself.
- Not done: other time zones (a proof per zone, or bounds computed and checked in the zone);
  `date_trunc` compared with another expression than a constant; `extract` of other fields
  (`month` alone is not a range); conjuncts inside `OR`; join conditions.
- Tests: planner tests `batch_date_range_predicates.yaml` (each function and comparison, aligned
  and unaligned constants, constant on the left, week, quarter, hour, a `date` truncated, `between`,
  `date_part` with fractions, `timestamptz` in `UTC` and kept in another zone, a column that is not
  a key, kept cases: `decade`, `month` field, fraction with `=`, year bound 0, `OR`; off by
  default); differential test `e2e_test/batch/date_range_predicates.gen.py` (26 queries on values
  on, one microsecond before and after each boundary, in `UTC` and in `Europe/Paris`, after
  updates to `NULL`, deletes and inserts).

**14. Semi and anti joins in the cost-based ordering (`batch_semi_join_ordering`, Calcite
`SemiJoinJoinTransposeRule`, PostgreSQL `reduce_unique_semijoins`).** With
`batch_cost_based_join_ordering`, BJ orders the inner joins and leaves every semi or anti join
where the binder put it, above all the inner joins of the `FROM` clause. `SemiJoinPlacementRule`,
in its own bottom-up stage just before the stages of BJ, looks at a left semi or anti join whose
left side is the multi-join of those inner joins (or a two-way inner join), and compares the
estimated cost of three placements:

```sql
select f.* from f join d1 on f.d1_id = d1.id join d2 on f.d2_id = d2.id
where exists (select 1 from h where h.x = f.v);
-- kept:   LeftSemi(f.v = h.x) <- [Join(f, d1, d2)], h
-- pushed: Join(LeftSemi(f.v = h.x) <- f, h ; d1 ; d2), ordered by BJ
select ... where f.w in (select g.id from g where g.x = 1);
-- merged: Join(f, d1, d2, g) on ... AND f.w = g.id, ordered by BJ (g unique on id)
```

- Pushed into the input of the multi-join that holds all the left columns its condition reads
  (exact for semi and anti joins: they keep a row according to those columns only, so filtering
  that input first keeps the same rows of the join, with their multiplicities). The condition is
  remapped to the input, the multi-join keeps its condition and output.
- Merged as one more input of the multi-join (semi joins only): when its right side is unique on
  the columns its condition binds and the condition has an equality between the sides, the semi
  join is an inner join whose right columns are not output; its condition joins the condition of
  the multi-join, and BJ may then join the right side anywhere (a small filtered table early, a
  lookup).
- Costs: those of BJ (rows from the storage statistics, learned ratios with
  `batch_cardinality_feedback_learning`, widths, probe 1, build 3, output 1 per byte; the rows
  kept by a semi or anti join are half of its left side). Kept: the best join tree of the
  multi-join plus the reading of the right side and the hash semi join above it. Pushed: the best
  tree with the pushed input (which BJ counts as an input reading half of its rows), corrected to
  count the reading of the original input and of the right side and the hash semi join over them.
  Merged: the best tree of the multi-join with the right side. A placement replaces the kept one
  only when cheaper by a factor 1.25; without an estimate for every input nothing moves. Bottom-up,
  so that a semi join stacked over another sees the multi-join the lower one went into.
- Behaviour with the variable off: the stage does not run, BJ is unchanged. The rule never runs
  without BJ (it is inside its block) or without estimates.
- Not done: a semi join whose condition reads several inputs of the multi-join (it could go into
  the smallest sub-join holding them: an extension of the enumeration, below); semi joins on a
  non-unique side turned into inner joins over a `DISTINCT` of that side (PostgreSQL's
  unique-ification, which BJ could cost); lookup semi joins are costed as hash joins; right semi
  and anti joins (none exist at this point of the pipeline).
- Tests: planner tests `batch_semi_join_ordering.yaml` (semi join pushed below a multi-join, anti
  join below a two-way join, semi join on a unique side merged, semi join on a dimension, two
  stacked semi joins; kept: condition reading two inputs, no statistics, no cost-based ordering;
  off by default); differential test `e2e_test/batch/semi_join_ordering.gen.py` (12 queries with
  `batch_cost_based_join_ordering` on in both runs, a large fact table and small dimensions,
  duplicates and `NULL`s in the tested side, stacked semi and anti joins, `NOT IN`, aggregation
  above, after updates, deletes and with a side emptied; a pause after each change for the
  statistics).

**16. Outer-join reordering: design only, not implemented.** After study, this one is too large
and too risky for this round: it needs a new enumeration, and a wrong reordering of an outer join
gives wrong results silently. What it would take:

- The identities, from PostgreSQL's `optimizer/README` and `make_outerjoininfo`
  (`initsplan.c`), with `Pab` a condition reading only `A` and `B`:
  1. `(A ⟕ B on Pab) ⋈ C on Pac = (A ⋈ C on Pac) ⟕ B on Pab`: an inner join commutes with a left
     join when its condition does not read the nullable side `B`;
  2. `(A ⟕ B on Pab) ⟕ C on Pac = (A ⟕ C on Pac) ⟕ B on Pab`: two left joins from the same
     preserved side commute;
  3. `(A ⟕ B on Pab) ⟕ C on Pbc = A ⟕ (B ⟕ C on Pbc) on Pab` only if `Pbc` is strict in `B` (it is
     not true when the columns of `B` are `NULL`, `Strong` of `plan_expr_visitor/strong.rs`); not
     in the other direction without that proof;
  4. semi and anti joins follow 1 and 2 with their left side as the preserved side; full joins
     only commute with themselves (`A ⟗ B = B ⟗ A`) and associate only under strictness of both
     conditions; never across an as-of join.
  An inner join above a left join whose condition reads the nullable side is first reduced (the
  outer join becomes inner when the condition is strict: the reduction of rule 3 of the survey,
  owned by another branch), which is what makes identity 1 cover most real plans.
- The enumeration: PostgreSQL keeps one `SpecialJoinInfo` per outer join, with its minimal left
  and right sets (`min_lefthand`, `min_righthand`) widened by the conflicts the identities do not
  allow, and `join_is_legal` accepts a pair of sets only when every special join is either fully
  inside one of them or exactly matched by the pair. The fork's DP (`JoinGraph::dp`) would need:
  the multi-join builder to absorb left joins as such special edges (today it stops at any
  non-inner join), the legality test in `linked`, the right estimate for a left join (at least
  the rows of the preserved side), and `JoinTree::build` to emit the right join type and
  condition at the node that matches the special edge. Calcite's `DphypJoinReorderRule` uses
  hyper-edges for the same purpose.
- Gain: a large preserved side joined to dimensions by left joins and to a selective inner join
  (identity 1 lets the inner join filter first); chains of left joins to dimensions reordered so
  that a lookup or a small build side comes first. Batch only (streaming plans are frozen and
  have no statistics).
- Tests it would need: the differential test of every identity with `NULL`s on each side, rows
  without match, duplicates, and conditions that are strict and not strict in the nullable side,
  before any use on real queries.

### SU. Streaming: union distributed by the key of its parent (added on 2026-11-28)

`LogicalUnion::to_stream` always shuffles every input by the stream key of the union, which holds
the branch tag (`$src`). The operator above usually needs another distribution (its group key,
join key, partition key, dedup key), so a second exchange follows above the union:

```sql
create table a (id int primary key, v int);
create table b (id int primary key, v int);
select id, count(*) from (select id, v from a union all select id, v from b) u group by id;
-- before: HashAgg <- Exchange(Hash id) <- Union <- [Exchange(Hash id, $src) <- Project <- Scan(a), ...]
-- SU:     HashAgg <- Union <- [Exchange(Hash id) <- Project <- Scan(a), ...]
```

**What SU does (variable `streaming_distributed_union`, default false).** The union implements
`to_stream_with_dist_required`. When the requirement is `HashShard(k)` (second side of a hash
join), `ShardByKey(k)` or `ShardByExactKey(k)` (hash aggregation, group top-n, window, first side
of a hash join), the stream key of the union does not already satisfy it, and `k` (the columns the
parent would hash the union by, `RequiredDist::to_dist`) is a subset of the stream key of the
union, each input is converted with the requirement `HashShard(k)` (an exchange, or nothing if it
is already distributed exactly so) and the union is distributed as `HashShard(k)`, which the
parent sees satisfied. Otherwise the plan is the default one. Only for a union with a branch tag
(every union planned from a query; the unions of table plans have none).

The parents that used to convert their input and then hash it themselves (hash aggregation when it
does not try two phases, group top-n, `LogicalDedup`, over window, both modes) now pass their
requirement down when their input is a union, possibly below projections, that takes it
(`input_to_stream_for_required_dist`); a projection passes it through its column mapping when every
required column is a distinct input column (`LogicalProject::input_required_dist_for_union`; upstream
a projection never passes a shard requirement down). Joins already passed their requirements. With
the variable off, or without such a union, every node converts its input exactly as before.

- Exactness: the union is stateless and positional; the inputs have its schema, so `k` designates
  the same columns, with the same types, in every input, and the hash of a row is the same whichever
  branch it comes from. Every row reaches the actor of `hash(k)` of the parent's fragment, as with
  the exchange above the union. Let `K` be a value of the stream key of the union. Its rows come from
  one branch (the tag is in the key) and, in that branch, from one value of the branch's stream key
  (which is part of the union's): by the invariant every streaming plan keeps, they are emitted by
  one actor of the branch, in order. Since `k` is part of the stream key, they all have the same `k`,
  so they go through one exchange channel to one actor of the union, in order. The default plan is
  the case `k = stream key`; the same argument holds for any subset. The union's output therefore
  keeps "a stream key value lives in one actor" and its distribution key is inside its stream key,
  which `StreamExchange::new` asserts of the input of a hash exchange over a retractable stream.
  Moreover no exchange is put over the union: the parent gets the distribution it required, a
  projection between passes the requirement down only if it then provides exactly the required
  distribution (otherwise it converts its input as before), and the union falls back to the
  default plan if its distribution does not satisfy the requirement.
- Properties: the stream key, append-only kind, `EMIT ON WINDOW CLOSE` flag and watermark columns of
  `StreamUnion` are derived from its inputs as before and do not depend on the distribution; the
  watermark of a column is still the minimum over all inputs and actors. A union is the parent's
  input with exactly the distribution the removed exchange had, so the parent is planned the same.
- Inputs: a scan (`UpstreamHashShard`), a single input (a global aggregation, values) or an input
  hashed otherwise gets an exchange `HashShard(k)`; an input already distributed as `HashShard(k)`
  (an aggregation on the key below a projection) stays in the fragment of the parent without
  exchange, as an input distributed by the stream key already did.
- Nested unions: the inputs of a union are projections that append the tag; a requirement holding
  the outer tag cannot be mapped through them (a literal), so an inner union is converted as before
  and the projection above it adds the exchange the outer union needs. A requirement without the tag
  reaches the inner union, which applies the same test to its own stream key.
- Kept on purpose: a required single distribution (aggregation without group key, top-n without
  group key, forced two-phase aggregation), the root of a materialized view or a sink (the stream
  key of the union already satisfies the materialized view; a sink enforces its distribution after
  conversion, above the union), a union under a `Share`, and a key outside the stream key: the
  frequent case of a union whose branches do not all output their key, where the union appends
  key slots (`null` in the other branches) and the parent's key designates the original column,
  never the slot; and the union created by the union-join factoring, which outputs the join key
  twice (once for the join above, once among the union's columns): the stream key holds one copy
  and the join uses the other.
- Existing streaming jobs are not affected: their plans are frozen; only new jobs created with the
  variable on.
- Tests: `stream_distributed_union.yaml` (planner: union under a hash aggregation, with a projection
  between, of three branches, of aggregations already hashed on the key, `UNION`, group top-n,
  `DISTINCT ON`, over window, hash join with the union on either side, a left join, a join of two
  unions, nested unions; kept: key outside the stream key, the union created by the union-join
  factoring, non-key group column, constant columns of the branches, single distributions, forced
  two-phase aggregation, materialized view root, off by default), and the differential
  `e2e_test/streaming/distributed_union.gen.py` (20 queries materialized with and without the
  variable, compared with each other and with the batch query after the backfill, inserts, updates
  that move rows between branches, updates of grouped columns that change the key of aggregation
  branches, keys changed by a delete and an insert, deletes, an emptied and refilled input).

**Open idea: a key outside the stream key.** The exchanges and the union are stateless and work row
by row, so moving the hash exchange from above the union to each of its inputs gives every actor of
the parent the same events in each epoch as before, for any `k`: in the default plan an event goes
from an actor of a branch through one actor of the union to the parent, and only the events of one
branch actor and one stream key value keep their order; with the exchange on the input they keep
it per branch actor, a stronger order that the default plan can also produce. An update pair whose
key changes is split into a delete and an insert by the one hash exchange in both plans. The
`StreamUnion` would have the distribution and stream key the exchange had, an exchange whose
distribution key is outside the stream key, as every aggregation on a non-key column has today.
Not done: the union's output would no longer satisfy the invariant asserted by `StreamExchange::new`
(a later hash exchange over it would panic), and a frozen plan is not the place to
extend a shape that upstream asserts against. A narrower variant, the parent's key mapped to the
union's key slot when every branch fills the slot with the very column, does not help the
frequent case (the other branches fill the slot with `null`). It would cover the factored join
key, whose two copies are the same column of each branch, but the union's distribution key would
then be outside its stream key by position (equal by value), which the same assertion compares;
not done.

### MK. Streaming: the reductions keep the key of a materialized view (added on 2026-11-28)

Variable `streaming_preserve_mv_key`. The replay of a workload compares plans with the reductions
on; it used to create every materialized view with the variables off and only plan the queries
with them on. Creating the views with the variables on, as a deployment does, gave worse plans to
the consumers of the views, with the same rules: batch lookup joins by the key of a view became a
scan, a shuffle and a hash join, and streaming joins on the view needed a shuffle. The reductions
had changed the key of the views they read:

- **The operator that gives the view its key is removed.** A top-1 per key, a `DISTINCT ON`, a
  dedup or an aggregation grouped by a key, at the top of the view, over a join whose other side
  has at most one row per row (a left join on the primary key of the other side). The input is
  unique on the group key, so `AggOnKeyEliminateRule` (`streaming_simplify_on_unique_key`, idea 2)
  removes the operator. The key of the view then becomes the stream key of the join, which holds
  the stream keys of both sides (or the join key, for the side whose key is compared), so the
  primary key is wider, and the distribution key, a subset of it, may be a column the
  consumers do not join on:

  ```text
  SELECT DISTINCT ON (e.id) e.id, d.v FROM e LEFT JOIN d ON e.ref_id = d.id

  without the reductions: pk [id], distribution [id]
  with them:              pk [id, e.ref_id], distribution [e.ref_id]
  ```

  A lookup join by `id` needs the shortest prefix of the primary key that holds the
  distribution key, here the whole key. Downstream, a scan of the view is no longer known to be
  unique on `id`, so the rules that rely on that key (1, 2, 3) no longer apply to the consumers.
  The wider key can not be narrowed again without a stateful operator: the rows of one `id` can
  come from two actors (a row whose `ref_id` changes is deleted by the actor of the old value and
  inserted by the actor of the new one), and a narrower key would let the insert overtake the
  delete. The removed operator was what tolerated this.
- **The distribution comes from another operator.** A join removed by the reductions (a table
  joined with itself on its key, idea 3), or a projection moved below the top join (so that the
  top operator becomes the join, for which `StreamMaterialize` shuffles by the whole stream key),
  changes the distribution of the input of the materialize; the primary key may stay the same.

The fix, behind the variable:

- **The operator that gives the job its key is kept** (`optimize_below_key_operator`, used by the
  stage of idea 2): below the projections and filters at the top of the plan, the first
  aggregation with a group key, top-n that keeps one row per group or dedup is not removed if the
  stream key of its input is not within its group key. The stage is applied to its input: the
  other operators are still removed. Operators whose removal does not change the key (input key
  within the group key, a top-n of several rows per group) and operators deeper in the plan are
  removed as before.
- **The distribution key of the view is kept** (`StreamMaterialize::preferred_mv_distribution`):
  `gen_create_mv_plan_bound` plans the view once more without the reductions (the baseline plan
  of H) and records its distribution key, by the names of the visible columns. The materialize
  of the plan with the reductions shuffles by those columns if they are still part of its stream
  key (one exchange in the job of the view when the input is distributed otherwise). A
  distribution by hidden columns is not kept: they may differ between the two plans.
- Valid: keeping an operator of the original plan, and shuffling the input of the materialize by
  columns of its stream key (the rows of a key stay on one actor, in order), are always correct.
- Cost: the kept operator keeps its state (a top-1 per key keeps the rows of the view once more);
  the view is planned twice when it is created.
- Measure (replay of a sample workload, the views created with the variables of the
  reductions on, compared with the same queries planned over views created without them): the
  regression was +5 joins, +47 scans and +49 exchanges in 34 batch queries, +4 joins in the
  streaming jobs. Recreating with the original key the five views whose top operator was removed
  recovers all but +6 scans and +6 exchanges; recreating also the four views whose distribution
  had changed recovers everything (0 joins, scans and exchanges of difference).
- Not done: the order of the primary key (a consumer may need a prefix that the reductions moved
  after another column), and the views planned with an `ORDER BY`.
- Tests: planner tests `stream_preserve_mv_key.yaml` (the keys with and without the variable,
  the operators still removed, batch lookup joins and streaming joins on the views); differential
  test `e2e_test/streaming/preserve_mv_key` (views with and without the variable, views joined on
  their key, join keys moved between actors, deletes, emptied tables).

### NC. Contradictory `IS NULL` on any expression (added on 2026-11-28, done on the branch)

`NeverTruePredicateRule` (A) and `ContradictoryNullFilterRule` already empty a filter or a scan
with `c IS NULL` and `c = ...`, but only for a direct column and a short list of functions
(comparisons, `IN`, `IS NOT NULL`). Predicate pushdown through projections and unions produces
the same contradiction on expressions: a test `k IS NULL` on a column computed as
`coalesce(a.x, b.y, ...)` lands next to a filter of the view on the same expression, and a scan
range `id IS NULL` next to `id ~ '^...$'`:

```sql
create table t (id int primary key, a varchar, b varchar);
create view v as select id, coalesce(a, b) as k from t where coalesce(a, b) ~ '^[a-z]+$';
select * from v where k is null;
-- before: Filter(IsNull(Coalesce(a, b)) AND RegexpEq(Coalesce(a, b), ..)) <- Scan(t)
-- NC:     Values([])  (and every operator that only fed it is removed by B)
```

**What NC does (variables `batch_fold_null_contradictions`, `streaming_fold_null_contradictions`,
default false; rule `NullContradictionRule`).** In the conjunction of a filter, of a scan predicate
or of a join condition, a conjunct `e IS NULL` (or `NOT (e IS NOT NULL)`) with `e` deterministic
(columns, constants and pure built-in functions only, compared structurally) and another conjunct
that can not be `TRUE` when `e` is `NULL`. The second conjunct is recognized when it is `NULL`
then: `e` itself, a call of a strict function with such an argument (comparisons, `NOT`,
arithmetic, `abs`, `ceil`, `floor`, `extract`, casts, `LIKE` / `ILIKE`, `~`, `starts_with`,
`upper`, `lower`, `trim`, `substr`, `length`, `replace`, `split_part`, `position`, `md5`, `||` on
strings, `->` / `->>`), `x IN (...)` with such an `x`, `COALESCE` / `GREATEST` / `LEAST` whose
arguments all are; or `FALSE`, `x IS NOT NULL`, `NOT (x IS NULL)`, `x IS NOT DISTINCT FROM c`
(non-`NULL` constant) with such an `x`, `a AND b` with one such operand, `a OR b` with two, `a IS
TRUE`. When `e` is a `COALESCE` / `GREATEST` / `LEAST`, its arguments are `NULL` too, recursively:
`coalesce(a, b) IS NULL AND b = 'x'` is a contradiction as well. The filter or scan becomes an
empty `Values`; a join is rewritten as A rewrites a join whose condition is never true (inner and
semi: empty; left outer and left anti: the right side empty; right outer and right anti: the left
side empty; full outer and as-of joins left as they are). `EmptyRelationPropagateRule` and
`EmptyInputSimplifyRule` then remove what only fed it, in the same bottom-up stage.

**Correctness.** These predicates are evaluated with filter semantics: a row (a pair) is kept iff
the conjunction is `TRUE`. If `e` is not `NULL`, `e IS NULL` is `FALSE`; if it is, the other
conjunct is `NULL` or `FALSE`. `e` evaluates to the same value in both conjuncts because it is
deterministic. The functions of the allow-list are declared in `risingwave_expr_impl` with
non-optional arguments, which makes the generated code return `NULL` without calling the function
when an argument is `NULL`. In a streaming job every function call is wrapped so that a failure is
`NULL` (`NonStrict` around each node): a failing `e` is `NULL` in both conjuncts, still covered. The
rewrite holds for every content of the inputs, so for every change of a streaming job.

**Not rewritten.** `COALESCE` with a non-`NULL` argument, `GREATEST` / `LEAST` with one
(`greatest(c, 1)` is not `NULL`), `concat`, `CASE`, `IS DISTINCT FROM`, any function outside the
allow-list; the contradiction under an `OR` or a `NOT`; a volatile `e` (`random()`), user-defined
functions, subqueries; a conjunct on an expression that may be non-`NULL` while `e` is `NULL`
(`coalesce(a, b) IS NULL AND coalesce(a, c::varchar) ~ '^x'`; `coalesce(b, a)` is recognized,
since `a` and `b` are both `NULL`). In streaming, a part of the plan that may carry a watermark, and a filter or a
scan of the lookup side of a temporal join, are never emptied (as in A).

**Placement.** Batch: after `reduce_batch_on_keys` (before join ordering, so that the joins of a
removed sub-plan are not ordered) and again right before `BATCH_PRUNE_EMPTY_RELATION`, after the
final predicate pushdown. Streaming: after the first predicate pushdown (before the expressions of
a join condition are computed in a projection below the join, which hides `e` behind a column),
and again after the final predicate pushdown, right before `STREAM_PRUNE_EMPTY_RELATION`. A column
pruning follows when something was removed.

**Effect on the sample workload** (local analysis of the replayed plan of the data test that
motivated the rule, with the settings of the staging tests): its plan holds both shapes, `IsNull(Coalesce(E)) AND
RegexpEq(Coalesce(E), ..)` above a deep tree of outer joins, and a scan with `scan_ranges: [id IS
NULL]` under `RegexpEq(id, ..)` in the other branch of the same union. Both branches end in an
inner join or a left join whose left side would be empty, so the whole second branch of the union
of the preserved side goes: 189 of the 245 batch operators of the plan, among them 4 of its 5
group top-n (over a 13 GB input), 17 of 19 hash joins, 13 of 15 lookup joins, 29 of
35 scans and the hash join on a constant key. This is a manual simulation on the final plan (the
prebuilt replay binary does not contain the rule); the coordinator's replay with the branch is the
reference.

- Tests: planner tests `batch_fold_null_contradictions.yaml`, `stream_fold_null_contradictions.yaml`
  (rewritten and kept cases, variable off by default); differential tests
  `e2e_test/batch/fold_null_contradictions` and `e2e_test/streaming/fold_null_contradictions`
  (generators `*.gen.py`; the streaming one also plans and runs an `EMIT ON WINDOW CLOSE` job over a
  table with a watermark).

### IL. `IN` lists and repeated `COALESCE` arguments (added on 2026-11-28, done on the branch)

Two small cleanups of conjunctions that views stacked on views produce:
`source IN ('a', 'b') AND source IN ('a', 'c', 'd')` (one list per view) is `source = 'a'`, and
`coalesce(x, x, y, y, z)` (a column projected under two names and coalesced) is
`coalesce(x, y, z)`. A never-true intersection was already found by A for a direct column; the
non-empty one was left as two lists, and nothing removed the repeated arguments.

**What IL does (variables `batch_simplify_in_lists_and_coalesce`,
`streaming_simplify_in_lists_and_coalesce`, default false; rule `InListCoalesceSimplifyRule`).**

1. In the conjunction of a filter, of a scan predicate or of a join condition, the conjuncts
   `e IN (c1, ...)` and `e = c` on the same expression `e` (deterministic, not constant, of a type
   whose equal values are identical: integers, `varchar`, `boolean`, dates and times; constants of
   exactly that type) are replaced by one conjunct on the intersection of their non-`NULL` values:
   `e = v` for one value, `e IN (...)` for several, `FALSE` for none (then emptied by
   `NeverTruePredicateRule`, which runs in the same stage).
2. In every expression of the node (projections, filters, conditions, aggregations...), an
   argument of `COALESCE` that repeats an earlier argument is removed (deterministic arguments
   only); `coalesce(x, x)` becomes `x`.

**Correctness.** 1: a row is kept iff every conjunct is `TRUE`; `e IN (list)` is `TRUE` iff `e` is
not `NULL` and equal to a non-`NULL` value of the list (a `NULL` item turns a `FALSE` into a
`NULL`, which drops the row too), so the conjuncts on `e` are all `TRUE` iff `e` is in the
intersection. `numeric` (`1.0` and `1.00` are equal), floating point values and the other types
are left out. 2: `COALESCE` evaluates an argument only when all the earlier ones were `NULL`; a
repeated deterministic argument is then `NULL` again (a failure in a streaming job is `NULL` both
times).

**Not rewritten.** Lists under `OR` or `NOT` (`NOT IN` is not a list of allowed values), lists on
different expressions, `numeric` / floating point lists, volatile arguments of `COALESCE`.

- Placement: in the same stage as NC (before it), at the same places.
- Tests: planner tests `batch_simplify_in_lists_and_coalesce.yaml`,
  `stream_simplify_in_lists_and_coalesce.yaml`; differential tests
  `e2e_test/batch/simplify_in_lists_and_coalesce` and
  `e2e_test/streaming/simplify_in_lists_and_coalesce`.

### EB. Batch executor: empty build side of a hash join (added on 2026-11-28, done on the branch)

The batch hash join reads its build (right) side into a hash table, then streams its probe (left)
side. When the build side turns out to hold no key that can match (no row at all, or only `NULL`
keys that are not compared with `IS NOT DISTINCT FROM`), the output of some join types is empty
whatever the probe side, which was still read to its end:

| join type | output with no matching build key | probe side needed |
|---|---|---|
| inner, as-of inner, left semi, right semi | empty | no |
| right outer, right anti | the build rows, padded / unmatched | only if there are build rows |
| left outer, left anti, full outer, as-of left outer | every probe row | yes |

**What EB does (unconditional).** After the hash table is built (not spilling), when the table is
empty and the join is inner / as-of inner / left semi / right semi, or the build side has no row
and the join is right outer / right anti, the executor ends without executing its probe side,
**only when the probe side runs entirely in the task**: the builder walks the plan node of the
probe side and refuses any `Exchange` / `MergeSortExchange` in it. The partitions of a spilled join
read their probe side from a spill file of the task and qualify too.

Why only then: an exchange that is never executed never takes the output of the tasks of the stage
below; those tasks block on a full channel once their rows exceed its buffer, their stage never
completes, and the query keeps their memory until it is cancelled (the batch query scheduler waits
for every stage). Dropping an exchange *after* it was opened is fine (the producer sees a closed channel and
finishes), but opening it means polling the probe side, which runs every blocking operator below
first. So, in distributed plans, EB applies to the joins whose probe side is a pipeline over a
table already distributed by the join key (a self-join on a key, co-located scans), not to a probe
side behind a shuffle. The general fix is in the batch query scheduler (stop a stage once every
consumer of its output has completed), left for later: it changes the completion protocol of
queries.

**Correctness.** Exactly the definitions of the join types above; the probe side has no side
effect (a read-only plan in the task). No variable: the change only skips work whose result is
discarded, and only in the shape where nothing waits for it.

- Tests: unit tests in `hash_join.rs` (`test_empty_build_side_skips_probe_side`,
  `test_build_side_without_matching_key`: every join type, with and without a probe side in the
  task, `NULL` build keys); e2e `e2e_test/batch/join/hash_join_empty_build_side.slt` (local and
  distributed modes).
- Nested loop join, the converse (empty buffered left side): not done. Both inputs of a batch
  nested loop join are always behind an exchange (it runs as a single task), so the same reason
  forbids leaving its right side unexecuted.
- Effect on the sample workload: none on the data test that motivated it. Its join with the empty
  side reads the large side through exchanges (and in the replayed plan the large side is the build
  side); NC removes the work there at planning time instead.

### BC. Batch: cross joins copied to every task (added on 2026-11-28, done on the branch)

Views that are also materialized write a cross join as `JOIN small ON big.one = small.one` with
`1 AS one` on both sides, because a streaming job can not run a cross join. Upstream batch plans
it as `HashJoin(1 = 1)` with both inputs `Exchange(HashShard(1))`: every row of both sides goes to
the task of the hash of the constant. A real cross join is no better: `BatchNestedLoopJoin`
requires both inputs `Single`. This is upstream behaviour (also in the plans of the sample
workload without any fork variable); a performance defect, not a wrong result (note
`docs/fork/evolutions/broadcast-cross-join.md`).

**What BC does (variable `batch_broadcast_cross_join`, default false).**

- `ConstantKeyJoinRule`, last logical stage of a batch query: an equality between the two sides
  whose operands hold the same non-`NULL` constant in every row (column facts, simple types) is
  `TRUE` for every pair and is removed, whatever the join type; an inner join without equality
  between its sides gets its smaller side on the left when the cost-based estimates know both.
- `BatchNestedLoopJoin::to_distributed`: for inner, right outer, right semi and right anti joins
  whose right side is not `Single`, the left side (the one the executor buffers) is required
  `Broadcast` and the join is `SomeShard`. Each task joins a copy of every left row with its share
  of the right rows; these join types output nothing for a left row on its own, so the result is
  the same.
- `Distribution::to_prost`: `Broadcast` carries `BroadcastInfo { count }` (the number of consumer
  tasks), as `HashShard` carries its output count. The batch broadcast output channel already
  existed; batch plans never produced it.

**Not done.** Left outer / semi / anti and full nested loop joins (each copy of a left row would be
output or tested once per task); a broadcast hash join on a real key; streaming (no nested loop
join; the join on a constant stays on one vnode). Local execution keeps both inputs `Single`.

- Tests: planner test `batch_broadcast_cross_join.yaml`; differential test
  `e2e_test/batch/broadcast_cross_join` (distributed mode).

### RK. Unique key of `row_number() = n` (added on 2026-11-28, done on the branch)

A filter `row_number() OVER (PARTITION BY k ORDER BY ...) = 1` over a window keeps at most one row
per partition: `row_number()` numbers the rows of a partition 1, 2, 3... without ties, so two rows
of the filter's output that agree on `k` (a `NULL` partition is a partition like any other) are
the same row. The same holds for `row_number() = n` with any integer constant `n`: the numbers of a
partition are distinct, so at most one row of a partition has the number `n` (none if `n < 1`).
Without `PARTITION BY` the whole input is a single partition: at most one row passes, which the
unique keys write as the empty key. The unique keys of idea 4 (`unique_keys` in
`unique_key_simplify_rule.rs`) now hold `k` (or the empty key) for such a filter, the conjunct
written `rn = n`, `rn <= 1`, `rn < 2` or the other way round (`1 = rn`, `2 > rn`), whatever the
other conjuncts, the filter right above the window or above a projection of it (the column
pruning puts one there when an ordering column is not read above; the partition columns must be
projected as they are); and an over window passes the keys of its input through (its first columns
are its input columns, its rows those of its input). The window is not turned into a top-n for this.
Streaming rejects windows without `PARTITION BY`: the empty key only matters in batch.

When it matters: streaming turns a window with a single numbering function under such a filter
into a top-n, whose group key was already a key; the window stays when it has another function
on the same window (`lag`, a running `sum`...), and in batch unless `batch_over_window_to_topn`.
The rules of 1 and 2 then remove what the key makes useless:

```sql
-- No partition holds two first rows: the aggregation becomes a projection, the filter
-- `1 > 1` is never true and the query an empty relation.
select k from (select k, row_number() over (partition by k order by v) as rn from t) s
where rn = 1 group by k having count(*) > 1;
-- The DISTINCT and the left join on k are removed the same way.
select distinct k, v from (... where rn = 1) s;
select u.id from u left join (... where rn = 1) s on u.k = s.k;
```

It is a new fact for the existing rules, so it is behind their variables
(`streaming_simplify_on_unique_key`, `batch_simplify_on_unique_key`); the other readers of the
unique keys (merge of repeated lookups, aggregations pushed below joins or into unions, the
estimates of the cost-based join ordering) get the same fact behind their own variables. A key of
a shared sub-plan read twice designates the same row in both reads, as for the group key of a
top-1: a stream is computed once, and batch plans have no shared sub-plans at execution.

Not done:
- `rank()` and `dense_rank()`: ties all get 1, no key.
- `row_number() <= n` or `< n` keeping more than the number 1, `> n`: several rows per partition.
- An aggregation without group key over a one-row filter is kept: it emits a row over no rows.
- A partition column computed or dropped by the projection between the filter and the window.

Side effect on an existing output: in `batch_reduce_keys_after_conversions.yaml`, the duplicates
of a primary key searched with `lag(id) OVER (ORDER BY id)` and grouped by `id`: the window now
passes the key `id` of the table, so the first pass of the reductions on keys removes the outer
aggregation before `LagDuplicatesRule` runs; the rule then expands each `id` into
`generate_series(2, 1)` rows, none, instead of the former `count > 1` filter folded to an empty
relation. Same (empty) result, but no longer an empty relation at planning time.

Tests: `batch_row_number_unique_key.yaml` and `stream_row_number_unique_key.yaml` (planner:
`GROUP BY k HAVING count(*) > 1` folded to an empty relation, aggregates per key, `DISTINCT`,
`<= 1` and `2 > rn`, other conjuncts and another window function, two partition columns, a group
key holding more than the partition, an unused left join on `k`, `row_number() = 2` and `= 3`,
no partition with `= 1`, `= 2` and `<= 1` (aggregation, `DISTINCT`, left join on any column;
batch only);
kept: `rank() = 1`, `<= 2` with and without partition, `> 1`, a filter on another column, a group
key without the partition, a row number of another partition, `count(*)` without group key over
a window without partition, off by default), the differential
`e2e_test/batch/row_number_unique_key.gen.py` and `e2e_test/streaming/row_number_unique_key.gen.py`
(25 batch and 20 streaming queries, without the windows without partition, with and without the
variable, the streaming ones also compared with the batch query,
after rounds that insert a new first row, move a row to another partition, change, delete and set
to `NULL` the first rows, empty and refill the tables).
### GO. Join below a per-group operator; narrow values through it (added on 2026-11-28)

A frequent shape: a "latest row per key" view built as a top-n per group over a `UNION ALL` of
aggregations on the same key, each keeping a wide value, read through a selective join and a small
expression of the value:

```sql
create view va as select k, last_value(doc order by ts) as doc, max(ts) as ts from ea group by k;
create view vb as select k, last_value(doc order by ts) as doc, max(ts) as ts from eb group by k;
create view vu as select distinct on (k) * from (select * from va union all select * from vb) s
  order by k, ts desc;
select d.u, vu.doc->'a'->>'b' from vu join d on vu.k = d.u where d.state = 'open';
-- before: Project <- Join(k = u) <- [GroupTopN(k) <- Union <- [Agg(k) <- ea, Agg(k) <- eb], Filter <- d]
```

The aggregations and the top-n process every key of `ea` and `eb`, with the whole value, and the
join keeps a small fraction of the keys afterwards; the expression is applied above everything.

**GO (variable `batch_push_join_below_group_ops`, rule `JoinBelowGroupOpRule`).** An inner or left
semi join (either side for an inner join) whose condition reads, on the side of a per-group
operator, only its group columns goes below the operator, through the filters and projections
between them, and through the branches of a `UNION ALL` into the per-group operators below:

```text
-- after:  Project <- GroupTopN(k) <- Union <- [Project <- Agg(k) <- Join(k = u) <- [ea, Filter <- d], ...]
```

- Operators: a top-n with a group key (`DISTINCT ON ... ORDER BY`, `row_number() <= n` once
  converted), a deduplication (`DISTINCT ON` without `ORDER BY`), a window whose every function is
  partitioned by the compared columns (the filter `rn = 1` above it commutes with the join), an
  aggregation without grouping sets.
- Exactness. Let `G` be the group (partition, deduplication) columns and every column of the
  operator read by the condition be in `G`, of a type whose equal values are identical
  (`is_simple_type`: not `numeric`, floats or intervals). The rows of a group agree on those
  columns, so for a row `r` of the other side `T` the condition is true for all of them or none.
  Inner join: `T` is unique on the columns that the equalities compare (the unique keys of
  `unique_key_simplify_rule.rs`, which include a join with a lookup on the key of its second table,
  `att ⋈ tk on att.tid = tk.tid` being unique on the key of `att`), so a group meets at most one
  `r`: below the operator the join keeps the whole group with the same `r` appended to each row, or
  drops it. Semi join: whole groups are kept or dropped whatever the duplicates of `T`. The
  operator sees the same rows, in the same order, in each kept group: a top-n and a deduplication
  pick the same rows, a window computes the same values, an aggregation the same aggregates.
  Filters and projections commute with an inner or semi join; a `UNION ALL` distributes over it.
  An aggregation outputs only its group columns and aggregates: each column of `T` is carried by
  an `internal_last_seen_value` (all the rows of a kept group hold the same `r`; the calls that
  nothing reads are pruned), except those the condition equates with a group column, copied from
  it. Not with an
  impure condition (evaluated on every row of the groups instead of once per group), nor without
  an equality between `G` and `T`.
- When. The rewrite trades the operator's input for a join of every input row: worth it when the
  join keeps few rows. With estimates (statistics of the tables and, with
  `batch_cardinality_feedback_learning`, the learned ratios, through the estimator of BJ), the join
  goes below an operator when its estimated output (an inner join of the same sides for a semi
  join) is at most a quarter of the operator's input. Without estimates, only when `T` is
  restricted (a filter, a scan predicate, a semi or anti join or a limit inside it, through
  projections and inner joins) and the condition equates every column of `G` with a column of `T`.
  An aggregation is never pushed below an inner join with a whole table, where the opposite rule 5
  (`batch_push_agg_below_join`) applies: the two rules never undo each other.
- Placement: after the first predicate pushdown and the stages before join ordering (the filters of
  `T` are in it), before join ordering, which then orders the joins below the operators; a
  predicate pushdown follows a rewrite.
- Streaming: not done. The plan is frozen and the rewrite is exact, but the join below the operator
  keeps in its state every input row of the operator (all the rows of `ea`), where the operator
  kept one row or one aggregated value per group: the state is not reduced in general, only when
  the operator itself keeps all its rows (a `last_value` over a retractable input), and then the
  join state replaces it rather than shrinking it. A filtered small `T` does not change that: the
  state of the large side is the cost.
- Not done: an outer join (the padded groups would differ), a join that also compares a column
  that is not a group column (it would drop rows inside a group), a join that is not on a unique
  side (inner), a join with `T` reached only through another join of the tree (the multi-join is
  not split for it).

**NV2 (variables `batch_narrow_value_aggregates`, `streaming_narrow_value_aggregates`, rule
`NarrowValuePushRule`).** A generalisation of NV: the narrowing functions of a projection over a
top-n (with or without group key), a deduplication, a window, or a filter right above one of them,
go below that operator when it does not read the value (not a group, partition, order or
deduplication column, not an argument of a window function, not read by the filter), so that the
next round moves them into the union and the aggregations below. The operator picks the same rows
whatever the value and passes the columns of a picked row through, so `f` of its value is the same
before and after; the functions are those of NV (deterministic, pure, infallible: evaluated on
every row instead of the picked one) and no `NULL` is added. `from_value_agg` now also follows
these operators, so the existing pushes into unions and join sides see a value aggregated below
them. A filter is crossed only above one of these operators: above an aggregation,
`NarrowValueAggRule` already crosses it, and the plans of NV do not change. In the example above
(streaming, or batch after GO), `last_value(doc)` becomes `last_value(doc->'a'->>'b')` in both
aggregations, and the union, the top-n state and the join carry the text only.

- Tests: planner tests `batch_push_join_below_group_ops.yaml` (top-n over a union of aggregations,
  semi join on a side that is not unique, the operator on the right side, a join unique on the key
  of its first table, a window filtered on its row number, a deduplication, an aggregation with a
  column of `T` read above, a condition comparing a group column with `T`; with statistics: a small
  selection of a large input, a filtered `T` larger than the input, part of the group key; a
  volatile filter left above the operator; kept: left join, `T` not unique, a column that is not
  a group column, `T` not filtered without estimates, part of the group key without estimates, a
  join that keeps every row with statistics, a `numeric` group column),
  `batch_narrow_value_group_ops.yaml` and `stream_narrow_value_group_ops.yaml` (through a join and
  a top-n per group into a union of aggregations, several fields with a fallible cast left above,
  a top-n ordered by a field of the value, a top-n without group, a deduplication, windows; kept:
  the value output as it is, a window function reading the value, a deduplication on the value;
  batch with both variables); differential tests `e2e_test/batch/push_join_below_group_ops`
  (21 queries run without the variables, with each and with both, compared after inserts, updates
  of the selective side, deletes of last rows and emptied tables) and
  `e2e_test/streaming/narrow_value_group_ops` (8 queries materialized with and without the
  variable, compared with each other and with the batch query after the backfill and each round).

## Not for now: sharing between materialized views

Decision of 2026-11-25: too large for now.

- Streaming view matching: a new materialized view whose sub-plan equals an existing one scans it
  instead of building the same joins and state again. `mv_selection_rule.rs` does it for batch
  only. It creates hard dependencies between materialized views (drop, failure, backfill,
  parallelism), and the existing view must expose a superset of the columns.
- A cheaper first step: when a view is created, compare a fingerprint of its stateful sub-plans
  with those of the existing views and tell the user "this join already exists in view X". Fits
  the report H.
- Shared indexed state (McSherry et al., "Shared Arrangements", PVLDB 2020): in RisingWave this
  is the delta join over indexes, off by default, which trades state for lookup latency.
- Noria (Gjengset et al., OSDI 2018) merges equal sub-graphs across queries.

## Not selected yet

- **Facts derived from the definition of an upstream materialized view.** A view defined with
  `WHERE kind = 'A' AND x IS NOT NULL` guarantees these facts for all its rows; attaching them to
  its scans would let A and E fire across materialized layers. Needs storing or re-deriving the
  facts, and invalidation if the definition of a view can change.
- **Duplicate-insensitive context.** A `DISTINCT` under another `DISTINCT`, with only filters,
  projections, `UNION ALL` or inner joins in between, is useless. Directly stacked: done with 6. Not always a gain: a `DISTINCT`
  below a join shrinks the state of that join. Never for a `row_number() = 1`, which selects a row.
- **`UNION` rewrites.** `UNION` to `UNION ALL` when the branches hold no duplicate and are
  disjoint (needs 4c). Fusion of a `UNION ALL` of two filters over the same input into one pass:
  uncertain gain.
- **Aggregate then join back, to a top-n with ties.** `t JOIN (SELECT k, max(ts) ... GROUP BY k)`
  on `k` and `ts` becomes `rank() ... = 1`. Narrow pattern.
- **Null rejection as an analysis.** Done: see RO.
- **Sharing sub-plans again after normalization**, within one query. Must run after 1 and 2,
  which need per-consumer plans. Done for stateful sub-plans: see 10.
- **Semi join simplifications.** `(A semi B) semi B`; `k IN (SELECT k FROM A)` on `A` itself.
- **Multi-way join on a common key**, as a new operator without intermediate state (experimental
  in Flink). Large.
- **Running the simplification rules to a fixpoint**, as the simplifier of GHC does (Peyton Jones
  and Santos, "A transformation-based optimiser for Haskell", 1997): the rules above enable each
  other and column pruning. The key-based stage already loops with column pruning.
- **One analysis per node** for keys, constants, nullability, ranges and equivalences, as the
  `analysis` module of Materialize, instead of one ad hoc analysis per rule.
- **Equality saturation** (egg; RisingLight builds its planner on it): useful offline to discover
  or test rules, not as the optimizer of a frozen plan.

## Examined and rejected

- Inner join elimination through foreign keys: there is no foreign key constraint.
- Inferring `IS NOT NULL` on join keys to shrink state: a `NULL` join key already costs no state.
- Semi join to inner join on a unique side: same operators and state.
- General eager aggregation with count scaling: adds an operator and can add state.
- Rewriting any aggregation and join into a window function: RisingWave goes the other way.
- Magic sets: add joins; the useful part is the existing derivation of predicates across joins.
- Rewriting regular joins into temporal joins: not equivalent, updates of the dimension stop
  retracting.
- Partial state as in Noria: an execution model, not a rewrite.
- Narrowing a group key with functional dependencies across joins: removes nothing.
- Supercompilation and partial evaluation beyond constant propagation; laziness-specific
  transformations of functional compilers: no counterpart in a plan.
