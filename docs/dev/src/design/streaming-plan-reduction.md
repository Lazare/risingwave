# Reducing the streaming graph at planning time

Working notes of the prototype branch `proto/streaming-prune-empty-relation`: what is done, what
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
| 4 | Stronger derivation of unique keys | in progress |
| 1 | Elimination of an unused outer join on a unique key | in progress |
| 2 | Elimination of an aggregation, `DISTINCT` or top-n on a key | in progress |
| 3 | Elimination of a self-join on a key | selected, not started |
| 5 | Aggregation pushed below a join onto the unique side | selected, not started |
| 6 | Merge of two stacked aggregations | selected, not started |
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
- Both sides must be the same stream at the same epoch: not for temporal joins, nor for a side
  behind a temporal filter or an emit-on-window-close delay.
- The work is matching two sub-plans up to a column mapping, looking through shared sub-plans.
- Seen in the fusion paper of Amazon Athena (rule "JoinOnKeys"), PostgreSQL 18
  (`remove_useless_self_joins`), Materialize (`redundant_join`).

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
