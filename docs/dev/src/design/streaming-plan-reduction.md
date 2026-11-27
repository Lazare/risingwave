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
| 3 | Elimination of a self-join on a key | in progress: repeated lookups of a table done on the branch `proto/streaming-merge-repeated-lookups`, lookups nested in trees of joins and semi or anti joins answered by a carried row done on the branch `proto/plan-reduction-flat-lookups`, aggregations next |
| 5 | Aggregation pushed below a join onto the unique side | selected, not started |
| 6 | Merge of two stacked aggregations | done (prototype, branch `proto/plan-reduction-shared-aggs`): stacked aggregations, through a `UNION ALL` too, and a dedup below a distinct count; a dedup below a top-1 or a semi join not done (see 6) |
| 7 | Join common to the branches of a `UNION ALL` joined once above the union | done (prototype, branch of idea 3); derived joined side, several joins and semi or anti joins above the join on the branch `proto/plan-reduction-union-derived` |
| 8 | Aggregate of a constant over a group | done (prototype, branch of idea 3) |
| 9 | Leftovers of subquery decorrelation (domain re-scan, aggregation and null-safe join back) | aggregating subqueries done (prototype, branch `proto/streaming-simplify-subquery-unnesting`); laterals without a table not started |
| US | Branches of a `UNION ALL` that read the same table read it once (`Expand` per branch); scans of other branches shared (`streaming_share_union_scans`) | done (prototype, branch `proto/plan-reduction-union-scans`); dominance under a top-1 not done |
| 10 | Sharing again, within one query, the sub-plans that became identical, or that differ by their columns or by commuting filters; aggregations of one input with different filters merged with `FILTER` | done for stateful cores (prototype, branch `proto/plan-reduction-shared-aggs`) |
| BK | Column facts (A to G, 8), unique keys (1, 2) and repeated lookups (3) in batch queries | done (prototype, branch `proto/plan-reduction-batch-keys`, variables `batch_simplify_column_facts`, `batch_simplify_on_unique_key`, `batch_merge_repeated_lookups`) |
| BL | Batch: duplicates found with `lag` over a window ordered by the column, counted per value instead (`batch_rewrite_lag_duplicates`) | done (prototype, branch `proto/plan-reduction-batch-lag`) |
| B | Batch counterparts of 9, 9b, 9c, 7 and US, each behind a `batch_` variable | done (prototype, branch `proto/plan-reduction-batch-unnest`), see "Batch counterparts" |
| SL | Status ladder: the branches of a top-1 by priority that read the same relation picked with one `CASE` | done (prototype, branch `proto/plan-reduction-union-scans`); default branch rewritten into a join not done |
| NK | Join on a column known to be `NULL` matches nothing (`batch_simplify_null_join_keys`, `streaming_simplify_null_join_keys`) | done (prototype, branch `proto/plan-reduction-nullkey`), see "NK" below |
| BJ | Batch: cost-based ordering of the inner joins from the row counts and sizes of the tables (`batch_cost_based_join_ordering`) | first version (prototype, branch `proto/plan-reduction-batch-joinorder`, see "BJ" below), width-aware since 2026-11-27 (the build side is the side with fewer bytes; left outer, semi and anti joins can become right joins); index lookups and column statistics not done |
| BF | Batch: cardinality feedback, step 1: count the rows of the operators at execution and compare them with the estimates (`batch_cardinality_feedback`) | first version (prototype, branch `proto/batch-cardinality-feedback`, see "BF" below): observation only, no plan changes |
| BP | Batch: second pass of the reductions on keys after the stages that create aggregations (`batch_reduce_keys_after_conversions`) | done (prototype, branch `proto/plan-reduction-batch-pass2`, see "BP and BT" below) |
| BT | Batch: `row_number() <= n` per partition becomes a top-n per group, small `n` only (`batch_over_window_to_topn`) | done (prototype, branch `proto/plan-reduction-batch-pass2`, see "BP and BT" below) |
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

Found on the way, not changed: in the existing rule, `WHERE EXISTS (SELECT min(x) FROM r WHERE
r.k = t.k)`, which is always true (an aggregation without group key returns one row), is planned
as a semi join on `r.k = t.k`, and `NOT EXISTS` of the same as an anti join: the semi join
drops the rows of `t` without a match where the query keeps every row, the anti join keeps them
where the query returns none (seen in streaming plans, batch not checked). The rule checks the null-rejection of a filter above the apply but not the join
type of the apply.

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
  or of casts of them (streaming computes the cast of a join key in a projection below the join);
  a `NULL` literal of a projection. Nothing is derived through joins, aggregations or unions.
- A conjunct `l = r` of the join condition whose operand is such a column, or a cast of it, on a
  side whose rows must match to be useful: either side of an inner or semi join (the join becomes
  empty), the right side of a left outer or left anti join (the right side becomes empty, the
  left rows are padded or all kept), the left side of the right joins. The join is rewritten as
  A rewrites a never-true condition, then B and C remove what is above it (an aggregation without
  `GROUP BY` becomes its constant row).
- Valid: `NULL = x` is `NULL` for every `x` and every type, so no pair of rows satisfies the
  condition, whatever its other conjuncts, duplicates or empty inputs. The facts are properties
  of the rows a filter or a scan can output: they hold for every row ever produced, under
  retractions, so they are valid for a plan frozen when a streaming job is created.
- Not rewritten: `IS NOT DISTINCT FROM` (matches `NULL`s), `FULL OUTER JOIN`, as-of joins, an
  operand that is another expression (`coalesce(c, 0) = x` can match), a `NULL` column of the
  preserved side of an outer join (the rewrite would be valid, but removes nothing), and a filter
  `s.k IS NULL` above a left join on its optional side (the anti-join pattern: the filter stays
  above the join and is not read).
- Streaming and watermarks: the rule leaves the join untouched if a sub-plan it would remove holds
  a scan of a table with a watermark, a source with a watermark or `now()` (the same guard as the
  other rules of A and B in streaming, see the limits of A). Without this, an inner join with a
  temporal join's left side carrying the watermark of `EMIT ON WINDOW CLOSE` would become an
  empty `VALUES` without a watermark, and the job could no longer be planned. A left join whose
  optional side is removed keeps the watermarks of its preserved side.
- Stages: `STREAM_SIMPLIFY_NULL_JOIN_KEYS` right after `STREAM_PRUNE_EMPTY_RELATION`, and
  `BATCH_SIMPLIFY_NULL_JOIN_KEYS` at the start of `reduce_batch_on_keys` (after the first
  predicate pushdown, before join ordering), each with `EmptyRelationPropagateRule` and
  `EmptyInputSimplifyRule`, then column pruning if anything changed.
- Tests: planner tests `batch_simplify_null_join_keys.yaml`, `stream_simplify_null_join_keys.yaml`;
  differential tests `e2e_test/batch/simplify_null_join_keys` and
  `e2e_test/streaming/simplify_null_join_keys` (NULL keys on both sides, duplicates, empty tables,
  updates of keys to and from `NULL`, deletes).

### 5. Aggregation pushed below a join onto the unique side

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
- Status on 2026-11-25: still not started. The branch of 6 left it out: it moves an aggregation
  rather than removing one, and needs the unique keys of the join sides (idea 4) at the same place.

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
- **Null rejection as an analysis.** Replace the syntactic test of the conversion of an outer join
  to an inner join (`simplify_outer`) with an evaluation of the predicate with `NULL` for the
  optional side.
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
