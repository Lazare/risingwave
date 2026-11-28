# Multi-architecture image workflow

| | |
|---|---|
| Kind | tooling |
| Branch | `fork/ci-multiarch-image` |
| Status | integrated (`integration/v3.1.2`); every release candidate image of the fork is built with it |
| Configuration | `workflow_dispatch` inputs `tag`, `amd64_target_cpu` (`x86-64-v3`), `jobs` (3), `runner_amd64`, `runner_arm64`; repository variable `IMAGE_REPOSITORY`, secrets `REGISTRY_USERNAME` / `REGISTRY_PASSWORD` |

## Summary

`.github/workflows/build-multiarch-image.yml` builds a `linux/amd64` + `linux/arm64` RisingWave
image on GitHub runners and pushes it under a single tag. Manual trigger only.

## Motivation

The fork needs images of its own branches for both architectures without the upstream build
infrastructure. Cross-compiling or emulating the other architecture is slow and fragile.

## What it does

- For each architecture, the binary is compiled (profile `production`, thin LTO, as the official
  images) on a runner of that architecture, together with the connector node and the dashboard.
- `docker/Dockerfile.prebuilt` assembles the image from the prebuilt binary; it requires the
  patched OpenJDK 21, as `docker/Dockerfile` does.
- The two images are pushed by digest, then joined under one tag (default
  `<branch>-<short sha>`). The release candidates of the plan-reduction line are tagged
  `integration-v3.1.2-plan-reduction-rc<N>`.

## Configuration

See the table above. Without `IMAGE_REPOSITORY`, the image goes to `ghcr.io/<owner>/risingwave`
with the workflow token.

## Correctness / safety argument

Nothing is cross-compiled or emulated: each image is built and started on its own architecture.

## Status and versions

Integrated in `integration/v3.1.2`.

## Tests

The workflow itself; no separate test.

## Limits and risks

- Long builds (about an hour per architecture on standard runners); `jobs` must be lowered when
  the build is killed for lack of memory.
- Default amd64 CPU baseline `x86-64-v3`: older CPUs need another value.

## Upstream relation

Fork tooling; not an upstream candidate.
