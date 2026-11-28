# Validation workflows and integration rebuild

| | |
|---|---|
| Kind | tooling |
| Branch | `fork/ci-validation` |
| Status | integrated (`integration/v3.1.2`) |
| Configuration | push to `check/**` (branch check), push to `validate/**` (full validation) |

## Summary

Two GitHub workflows and a script keep the `fork/*` branches independent and the integration
branch tested:

- `fork-branches-check.yml` (push to `check/**`): builds every code branch (`fork/*` except the
  tooling and documentation branches) on its own on the upstream tag, runs the unit tests of the
  main crates, then checks that all the branches merge without conflict with
  `scripts/fork/rebuild-integration.sh`.
- `fork-validation.yml`, workflow "Fork Validation" (push to `validate/**`): builds with the
  embedded UDF runtimes and runs `scripts/selective_barriers/ci_validate.sh`: a matrix of source
  and sink connectors (`connector_matrix.py`), sinks into tables, chaos tests, the streaming, batch
  and UDF SLT suites, unit tests, and an in-place upgrade from a base commit.
- `scripts/fork/rebuild-integration.sh <tag> <integration branch> [remote]`: rebuilds the
  integration branch as the tag plus the `fork/*` branches merged `--no-ff` in a fixed order (fixes
  of upstream code, then features, then tooling, then `fork/docs`).

## Motivation

Each fork branch must be carried alone to a new upstream version
(`git rebase --onto <new tag> <old tag> fork/<name>`). Without a check of each branch in isolation,
branches silently start depending on each other. The full validation needs services (Postgres,
Kafka, ...) and long suites that do not fit a development machine.

## What it does

See the summary. The check workflow fetches the base tag from the upstream repository, since the
fork does not hold upstream tags. The upgrade test waits for the end of the recovery after the
restart and retries the first writes while the cluster recovers (DML refused, webhook errors).

## Configuration

Trigger branches only. The workflows are public, like the repository: no private data may be used
in them.

## Correctness / safety argument

Not applicable (tooling).

## Status and versions

Integrated in `integration/v3.1.2`. The rebuild script lists 16 branches: the five fixes of
upstream code (`fork/fix-snapshot-backfill`, `fork/fix-batch-hash-agg-spill`,
`fork/fix-last-seen-value-null`, `fork/fix-webhook-unready-worker`, `fork/optimizer`), the eight
features, the two tooling branches and `fork/docs`. The validation workflow was named
`selective-barriers-validation.yml` until it was renamed `fork-validation.yml`; runs before the
rename appear under "Selective Barriers Validation".

## Tests

The workflows are the tests.

## Limits and risks

- `fork-validation.yml` tests every feature of the fork: it is only meaningful on an integration
  branch, not on a single `fork/*` branch. Its scripts still live under
  `scripts/selective_barriers/`, where the validation started.

## Upstream relation

Fork tooling; not an upstream candidate.
