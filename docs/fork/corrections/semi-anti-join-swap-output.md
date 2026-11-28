# Swapping the sides of a semi or anti join shifted its output columns

- **Kind**: crash of the planner. Two places: `swap_join_sides`, fork code (cost-based batch join
  ordering of the plan reduction prototype), and `JoinCommuteRule`, upstream code. Found on
  2026-11-28 while testing the batch join ordering.
- **Affected versions**: fork: `swap_join_sides` was added and fixed within the plan reduction
  release candidate rc8 (`a8578b7b1c`, then `62d69f5ccb`), so no validated build carried it.
  Upstream: `JoinCommuteRule` in `v3.1.2` and `main` (2026-11-28).

## Symptom

A semi or anti join only outputs the columns of the side it keeps. Swapping its sides (a left
semi join becomes a right semi join) leaves that side kept, so its output indices must not change.
Both functions shifted them as for an inner join, whose output holds both sides, left first. The
result is an index outside the kept side: the planner panicked in `Join::schema`, or, with a wide
enough kept side, would have output other columns.

- Fork: a batch query with an `IN` / `EXISTS` subquery (a left semi join) whose sides
  `CostBasedOuterJoinSidesRule` swapped under `batch_cost_based_join_ordering`, with a one-column
  side, panicked during planning.
- Upstream: any right semi or right anti join reaching `JoinCommuteRule` (the stage that turns
  right joins into left joins) gets wrong indices. Upstream rarely builds right semi or anti
  joins before that stage, and no upstream SQL reproduction is known; the unit test
  `test_commute_right_semi_join_keeps_its_output` reproduces it on a plan.

## Cause

`JoinCommuteRule::apply` and `swap_join_sides` (`src/frontend/src/optimizer/rule/join_commute_rule.rs`)
mapped every output index `i` to `i + right_len` (left columns) or `i - left_len` (right
columns), whatever the join type.

## Fix

`swapped_output_indices` keeps the output indices of semi and anti joins and shifts those of the
other joins, for both functions. Commit `62d69f5ccb` on branch
`proto/plan-reduction-batch-joinorder` (the `JoinCommuteRule` part applies to upstream code
alone).

## Tests

Unit tests in `join_commute_rule.rs`: swapping a left semi and a left anti join keeps their output
and swapping back gives the first join; inner and outer joins still shift; commuting a right semi
join keeps its output.

## Upstream status

Not fixed on `main` (2026-11-28): `JoinCommuteRule` still shifts the indices of right semi and
right anti joins. No upstream issue found.
