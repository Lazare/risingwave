# Output columns of right semi and anti joins read after the left columns

- **Kind**: wrong result (release builds) or planner panic (debug builds). Upstream code
  (`JoinProjectTransposeRule`, `LogicalJoin::predicate_pushdown`, `PullUpHopRule`), exposed by
  the fork (`batch_cost_based_join_ordering`); and two estimate helpers of the fork. Found on
  2026-11-28 by replaying batch queries through the planner of a debug build with the cost-based
  join ordering on (bisected to `batch_cost_based_join_ordering`).
- **Affected versions**: upstream code identical in `v3.1.2` and `main` (2026-11-28). Reachable in
  the fork since the cost-based sides of outer, semi and anti joins (plan reduction rc8,
  2026-11-27), only with `batch_cost_based_join_ordering`.

## Symptom

```sql
create table ok_t (k varchar, v int);         -- large
create table bad_t (k varchar, doc jsonb, ts int); -- small
set batch_cost_based_join_ordering = true;
select k, detail
from (select k, (last_value(doc order by ts) ->> 'r')::jsonb as detail from bad_t group by k) b
where not exists (select 1 from ok_t o where o.k = b.k);
```

With statistics where `ok_t` is much larger, the anti join becomes a right anti join (its build
side is then `bad_t`). The projection that computes `detail` is pulled above the join and
`detail` became the column `last_value(doc ...)` itself, typed `varchar`: a debug build panicked
with `JoinProjectTransposeRule fails to generate equivalent plan` (input `Jsonb`, output
`Varchar`); a release build planned the query with the wrong column and type (the whole document
instead of its `r` field, read as text). Correct: `detail` is `(doc_of_last_row ->> 'r')::jsonb`.

## Cause

A right semi or anti join only outputs its right columns, and its output indices number them from
0 (`Join::full_out_col_num`), while its condition reads the left columns followed by the right
ones. Upstream never has such a join late in the pipeline, because `JoinCommuteRule` turns right
joins into left joins before the last rewrites; `CostBasedOuterJoinSidesRule` creates them after
`JoinCommuteRule`. Then:

- `JoinProjectTransposeRule` (`rule/join_project_transpose_rule.rs`) took the output index `i` of
  the join as the `i`-th expression of the left columns followed by the right ones, and offset the
  right expressions by the number of left columns: the output read a left column and a bare right
  column instead of the computed expression.
- `LogicalJoin::predicate_pushdown` mapped a predicate on the output to the internal columns (the
  right columns, from 0) and then split it as if they were the left columns followed by the right
  ones: a right column could be taken for a left column (and moved into the condition of a right
  semi join) or for another right column. No reproduction through SQL found (a predicate rarely
  reaches such a join after the last pushdown), fixed for the same reason.
- `PullUpHopRule` classified the output columns of the join the same way.
- Fork: `Estimator::column_widths` and `input_required_columns` of the cost-based join ordering
  (estimates only, not reached in the current order of the stages).

## Fix

The output indices of a right semi or anti join index its right side: the rule takes the right
expressions from 0 and the output from the right entries; the predicate pushdown shifts the
predicate after the left columns and back; `PullUpHopRule` keeps the hop window below such a join;
the two estimate helpers add the left length. Release builds now also check that a rule keeps the
output types of the node it rewrites (`HeuristicOptimizer::optimize_node`) and fail the query
with an internal error instead of planning it with wrong types; debug builds panicked already.
Commit `50171a98fc` on branch `proto/plan-reduction-fix-cast-rownum`.

## Tests

Unit tests `test_pull_up_right_projection_keeps_the_output` (`join_project_transpose_rule.rs`:
right semi, right anti, inner and right outer joins keep the cast and read the right column) and
`test_predicate_pushdown_right_semi_anti_join` (`logical_join.rs`). Planner test
`batch_right_semi_anti_join_projection.yaml` (anti and semi joins with a cast on the kept side
turned into right joins, the same under a union joined downstream, the variable off). Differential
cases `cast_anti` and `cast_semi` in `e2e_test/batch/cost_based_join_ordering.gen.py`.

## Upstream status

Not fixed on `main` (2026-11-28): `JoinProjectTransposeRule`, `LogicalJoin::predicate_pushdown`
and `PullUpHopRule` are unchanged since `v3.1.2`. Latent upstream (no right semi or anti join
reaches them there); no upstream issue found.
