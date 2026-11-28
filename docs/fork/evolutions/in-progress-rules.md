# Rules in progress

| | |
|---|---|
| Kind | optimizer rules |
| Branch | none published |
| Status | in progress |
| Variables | not defined yet |

These items are being worked on but no code has been published on the fork's remote at the time of
writing. The "Intent" column is inferred from the name of each item; it is not documented in code
or history yet, and may change.

| Item | Intent | Related records |
|---|---|---|
| Limit-distinct operator | Stop a `SELECT DISTINCT ... LIMIT n` as soon as `n` distinct rows are found, instead of deduplicating the whole input. | The [limit pushdown](limit-pushdown.md) candidate does not cross a `DISTINCT` (the rows cut may be the distinct ones). |
| Null-safe equality to equality | Rewrite `a IS NOT DISTINCT FROM b` into `a = b` when one side is known not to be `NULL`, so that the join and lookup rules that only accept `=` apply. | Several rules of the plan-reduction line list `IS NOT DISTINCT FROM` among the shapes they do not handle (repeated lookups, null join keys, subquery unnesting). |
| `EXISTS` subquery simplification | Simplify `EXISTS` subqueries whose result is known (for example an aggregation without `GROUP BY` always returns one row). | The design note records that `WHERE EXISTS (SELECT <aggregate without GROUP BY> ...)` is planned upstream as a semi join although it is always true (idea 9, "found on the way"). |
| Outer join reduction | Turn outer joins into inner joins when a predicate above rejects `NULL` on the optional side, decided by evaluating the predicate rather than by the syntactic test used today (`simplify_outer`). | Recorded as "Null rejection as an analysis" in the "Not selected yet" section of the design note. |

Status, tests and risks will be documented when the code is published.
