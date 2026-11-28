# Volatility of user-defined functions

| | |
|---|---|
| Kind | feature (SQL) |
| Branch | `fork/udf-volatility` |
| Status | integrated (`integration/v3.1.2`), present in rc4 to rc8 |
| Configuration | `IMMUTABLE` / `STABLE` / `VOLATILE` clause of `CREATE FUNCTION` |

## Summary

The volatility declared in `CREATE FUNCTION` is stored in the function catalog and used by the
purity analysis of the frontend: a function declared `IMMUTABLE` is pure when all its arguments
are. Unspecified, `STABLE` and `VOLATILE` functions stay impure, as before. The volatility is shown
in `rw_catalog.rw_functions.volatility`.

## Motivation

Upstream parses the volatility clause but treats every user-defined function as impure. A
streaming job over a retractable input must materialize impure expressions, and some places refuse
them outright (a filter, an aggregate `FILTER`, a table function in the select list). A
deterministic UDF, for example a parser of a text field, could therefore not be used in a `WHERE`
of a materialized view over a table that receives updates, although its result never changes for
the same input.

## What it does

```sql
CREATE FUNCTION is_even(n int) RETURNS boolean LANGUAGE javascript IMMUTABLE AS $$
export function is_even(n) { return n % 2 === 0; }
$$;
-- accepted with the fork; refused upstream (impure expression in WHERE on a retract stream)
CREATE MATERIALIZED VIEW mv AS SELECT * FROM t1 WHERE is_even(a);
```

- New enum `UdfVolatility` in `proto/expr.proto`, field `volatility` in `catalog.Function` and in
  `UserDefinedFunction`.
- `ImpureAnalyzer` (`src/frontend/src/expr/pure.rs`) visits the arguments of an immutable UDF or
  UDTF instead of marking the call impure.
- Stored in the meta catalog without a migration (`src/meta/model/src/function.rs`); functions
  created before the change read as unspecified.

## Configuration

The SQL clause only. No session variable.

## Correctness / safety argument

`IMMUTABLE` is a promise of the user, as in PostgreSQL. If a function declared immutable is not
deterministic, a streaming job can produce retractions that do not match earlier insertions. The
default (no clause) keeps the upstream behavior.

## Status and versions

Integrated in `integration/v3.1.2`; present in rc4 to rc8.

## Tests

- Unit test `test_udf_volatility` in `pure.rs`.
- `e2e_test/streaming/impure_expr_materialize.slt` (extended): immutable and volatile UDFs in
  filters and select lists of materialized views.
- `e2e_test/udf/tests/scalar/wasm_volatility.slt`.

## Limits and risks

- `STABLE` is treated like `VOLATILE`.
- A wrong `IMMUTABLE` declaration is not detected.

## Upstream relation

Matches PostgreSQL semantics. A plausible upstream PR candidate; not proposed yet.
