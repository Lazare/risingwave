# `greatest` / `least` treated as NULL when one argument is NULL

- **Kind**: upstream bug (optimizer, `Strong` null analysis, since 2024-03), found by an audit
  of the NULL rules of the optimizer on 2026-11-28.
- **Affected versions**: upstream `v3.1.2` and `main` (2026-11-28); every fork build up to the
  plan reduction release candidate rc8 included.

## Symptom

`Strong::is_null` (`src/frontend/src/optimizer/plan_expr_visitor/strong.rs`) decides whether an
expression is certainly NULL when some columns are NULL. It listed `greatest` and `least` among
the strict functions (NULL as soon as one argument is NULL). RisingWave, like PostgreSQL, skips
their NULL arguments: `greatest(1, NULL)` is 1.

```sql
create table a (x int, k int);
create table b (x int, y int);
insert into a values (0, 1), (10, 2), (3, null);
insert into b values (10, 20), (3, null);

select x from a where greatest(x, null) > 5;
-- wrong: no row (the filter became `false`)       correct: 10

select x from a where x in (select greatest(max(y), 0) from b where b.x = a.x);
-- wrong: no row                                    correct: 0
-- (x = 0 has no row in b: max(y) is NULL and greatest(NULL, 0) is 0)
```

In streaming jobs with `streaming_simplify_subquery_unnesting` (plan reduction prototype),
`(SELECT greatest(max(y), 0) FROM b WHERE b.x = a.x)` in a select list gave NULL instead of 0
for the outer rows without inner rows.

## Cause

Callers of `Strong`:

- `ExpressionSimplifyRewriter` (`logical_filter_expression_simplify_rule.rs`) replaces a filter
  condition of one column that `Strong` says is NULL (with no NULL column, i.e. because of NULL
  literals) by `false`;
- `PullUpCorrelatedPredicateAggRule` rewrites a correlated scalar aggregation into a grouped one
  when its projected value is NULL over an empty input (or the condition above rejects it), and
  its streaming variant pads the unmatched outer rows with NULL when `Strong` says the value is
  NULL over an empty input.

`LogicalJoin::simplify_outer` does not use `Strong` (only direct comparisons of columns), and
the null-rejection helpers of the prototype do not list `greatest` / `least`.

## Fix

`greatest` and `least` are NULL only when all their arguments are, like `coalesce`. The other
functions of the strict list were checked against their implementations
(`src/expr/impl/src/scalar`, `src/expr/core/src/scalar/like.rs`): comparisons, `NOT`, `LIKE`,
arithmetic, `AddWithTimeZone`, casts, `trim`, `ceil` / `floor`, `extract` and the vector
functions take no nullable argument (no `Option` parameter, no variadic row) and return NULL for
a NULL argument. `concat`, `concat_ws`, array and `jsonb` functions are not in the strict list
(they are assumed never certainly NULL), which is safe.

Commit `ef4ca86091` on branch `proto/plan-reduction-fix-correctness` (applies to upstream code
alone).

## Tests

- Unit test `test_greatest_least` in `strong.rs`.
- Planner: `batch_correctness_fixes.yaml`, section `greatest`.
- End to end: `e2e_test/batch/correctness_fixes.slt` and `e2e_test/streaming/correctness_fixes.slt`
  (filters, `IN` over a correlated aggregate, scalar subquery with the streaming unnesting, through
  changes of the inner table).

## Upstream status

Not fixed on `main` (2026-11-28): `Greatest` and `Least` are still in the strict list. No
upstream issue found.
