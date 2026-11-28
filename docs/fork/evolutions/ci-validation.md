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

- `fork-branches-check.yml` (push to `check/**`): builds every `fork/*` branch on its own on the
  upstream tag, runs the unit tests of the main crates, then checks that the branches merge without
  conflict with `scripts/fork/rebuild-integration.sh`.
- `selective-barriers-validation.yml` (push to `validate/**`): builds with the embedded UDF
  runtimes and runs `scripts/selective_barriers/ci_validate.sh`: a matrix of source and sink
  connectors (`connector_matrix.py`), sinks into tables, chaos tests, the streaming, batch and UDF
  SLT suites, unit tests, and an in-place upgrade from a base commit.
- `scripts/fork/rebuild-integration.sh <tag> <integration branch> [remote]`: rebuilds the
  integration branch as the tag plus the `fork/*` branches merged `--no-ff` in a fixed order (fixes
  of upstream code, then features, then tooling).

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

Integrated in `integration/v3.1.2`. The rebuild script lists 13 branches: it does not yet include
the later fix branches `fork/fix-last-seen-value-null` and `fork/fix-webhook-unready-worker`, which
were merged into the plan-reduction line directly.

## Tests

The workflows are the tests.

## Limits and risks

- `selective-barriers-validation.yml` tests every feature of the fork: it is only meaningful on an
  integration branch, not on a single `fork/*` branch.
- The name of the validation workflow is historical: it validates the whole fork.

## Upstream relation

Fork tooling; not an upstream candidate.
