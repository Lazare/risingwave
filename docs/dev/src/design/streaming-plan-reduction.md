# Reducing the streaming graph at planning time

Working notes of the prototype branches `proto/streaming-prune-empty-relation` and
`proto/streaming-merge-repeated-lookups` (idea 3, derived from the former): what is done, what
is being done, and every idea that was considered, including those that are not pursued for now.
Not listed in `SUMMARY.md` while this is a prototype.

A streaming job is planned once and its plan is frozen: a rewrite must hold for all future data,
including retractions, `NULL`s (three-valued logic) and watermarks. This is why everything below
is opt-in through session variables that are off by default, and why the batch optimizer keeps its
own rules.

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
| 3 | Elimination of a self-join on a key | in progress: repeated lookups of a table done on the branch `proto/streaming-merge-repeated-lookups`, aggregations next |
| 5 | Aggregation pushed below a join onto the unique side | selected, not started |
| 6 | Merge of two stacked aggregations | selected, not started |
| 7 | Join common to the branches of a `UNION ALL` joined once above the union | done (prototype, branch of idea 3) |
| 8 | Aggregate of a constant over a group | done (prototype, branch of idea 3) |
| 9 | Leftovers of subquery decorrelation (domain re-scan, aggregation and null-safe join back) | aggregating subqueries done (prototype, branch `proto/streaming-simplify-subquery-unnesting`); laterals without a table done (prototype, branch `proto/plan-reduction-lateral`, see 9b) |
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
needed. `C` is scanned once and its join state kept once.

- The branches that are merged join the same table, with the same join type, on the same
  expressions of the table; their other inputs are unioned with the number of their branch.
- What differs between branches moves around the single join: the rest of a join condition and
  the filter of a scan are checked in the join condition for the rows of the branch (an outer or
  anti join stays exact); the filter and projection above a join are applied per branch above.
- An expression of the joined table that a branch computed below an outer join is still
  computed below the single join, so that it is `NULL` without a match.
- Cost to keep in mind: one join state holds the rows of every branch, and the rows of the union
  are as wide as the widest branch.
- Not done: a common side that is more than the scan of a table, an inner join in one branch
  and an anti join on the same key in another (one left join split on `IS NULL`).

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

Found on the way, not changed: in the existing rule, `WHERE EXISTS (SELECT min(x) FROM r WHERE
r.k = t.k)`, which is always true (an aggregation without group key returns one row), is planned
as a semi join on `r.k = t.k`, and `NOT EXISTS` of the same as an anti join: the semi join
drops the rows of `t` without a match where the query keeps every row, the anti join keeps them
where the query returns none (seen in streaming plans, batch not checked). The rule checks the null-rejection of a filter above the apply but not the join
type of the apply.

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

### 9c. A table joined back with an aggregation of itself on its key (added on 2026-11-25, done)

Same variable, rule `SelfAggregateJoinRule`, in a stage after the final column pruning. `T LEFT
JOIN (SELECT T2.k, agg(..) FROM T T2, srf(T2.a) x [LEFT] JOIN U ON .. WHERE .. GROUP BY T2.k) a
ON T.k = a.k` read `T` twice and joined it with the aggregation; also when the left side is a
view of `T` (projections and filters) carrying its key. When `k` is the whole unique key of `T`
(proven from the definition of the table, never assumed), the group of a row of the left side
holds the rows derived from that same row of `T`, so:

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



`SELECT f.cust_id, c.name, sum(f.amt) FROM f LEFT JOIN c ON f.cust_id = c.id GROUP BY f.cust_id,
c.name` becomes `(SELECT cust_id, sum(amt) FROM f GROUP BY cust_id) LEFT JOIN c ...`.

- No operator is removed, but the state of the join drops from one row per fact to one row per
  group, and an update of the dimension becomes one change instead of one per fact.
- Sound when the join columns of the fact side and the key of the dimension side are within the
  group key, and every aggregated column comes from the fact side.
- The general form (partial aggregation and scaling by a count) is rejected: it adds an operator
  and can add state.
- Yan and Larson, "Eager Aggregation and Lazy Aggregation", VLDB 1995; Materialize
  (`reduction_pushdown`).

### 6. Merge of two stacked aggregations

`SELECT region, sum(s) FROM (SELECT region, city, sum(x) s FROM t GROUP BY region, city) GROUP BY
region` becomes `SELECT region, sum(x) FROM t GROUP BY region`.

- Sound when the upper key is a subset of the lower key, for `SUM` of `SUM`, `SUM` of `COUNT`,
  `MIN` of `MIN`, `MAX` of `MAX`. Not `AVG` of `AVG`, not `COUNT` of `COUNT`. Not when the lower
  aggregation is shared or filtered on its results.
- Two levels are sometimes written on purpose against skew: keep it optional.
- Calcite (`AGGREGATE_MERGE`), CockroachDB (`FoldGroupingOperators`).

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
  projections, `UNION ALL` or inner joins in between, is useless. Not always a gain: a `DISTINCT`
  below a join shrinks the state of that join. Never for a `row_number() = 1`, which selects a row.
- **`UNION` rewrites.** `UNION` to `UNION ALL` when the branches hold no duplicate and are
  disjoint (needs 4c). Fusion of a `UNION ALL` of two filters over the same input into one pass:
  uncertain gain.
- **Aggregate then join back, to a top-n with ties.** `t JOIN (SELECT k, max(ts) ... GROUP BY k)`
  on `k` and `ts` becomes `rank() ... = 1`. Narrow pattern.
- **Null rejection as an analysis.** Replace the syntactic test of the conversion of an outer join
  to an inner join (`simplify_outer`) with an evaluation of the predicate with `NULL` for the
  optional side.
- **Sharing sub-plans again after normalization**, within one query. Must run after 1 and 2,
  which need per-consumer plans.
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
