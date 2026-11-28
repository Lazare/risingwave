# Default license

| | |
|---|---|
| Kind | operations |
| Branch | `fork/license` |
| Status | integrated (`integration/v3.1.2`), present in rc4 to rc8 |
| Configuration | none (applies when `license_key` is unset or invalid) |

## Summary

The default license of a build becomes the `All` tier with no resource limit and no expiration.
Release builds no longer embed the upstream default key that caps the cluster at 4 RWUs; tests
that assert the default license blocks premium features are disabled.

## Motivation

The fork is built and deployed by its owner from source. Without a license key, upstream release
builds unlock the premium features only up to 4 cores, and debug builds use a test key. The fork
wants the same feature set at any cluster size without managing a key.

## What it does

- `src/license/src/manager.rs`: `License::default()` uses `Tier::All` instead of `Tier::Free`
  (`rwu_limit: None`, `exp: u64::MAX`).
- `src/license/src/key.rs`: the default key of release builds is empty, so the default license
  applies; the upstream 4-core key is kept for reference only.
- Tests that expect the default license to refuse a feature are marked `#[ignore]` (unit tests) or
  rewritten (SLT under `e2e_test/`, e.g. `e2e_test/sink/license.slt`,
  `e2e_test/error_ui/simple/license.slt`).

## Configuration

None. An explicit `license_key` still works as upstream.

## Correctness / safety argument

No effect on query results. The RisingWave source is licensed under Apache-2.0; the comments in
the code record that this is a modification of a derivative build.

## Status and versions

Integrated in `integration/v3.1.2`; present in rc4 to rc8.

## Tests

The adjusted license tests listed above; the simulation test `license_cpu_limit.rs` is adapted.

## Limits and risks

- Upstream tests about license gating no longer test gating in this build.
- Each upstream upgrade may add new license tests that need the same adjustment.

## Upstream relation

Specific to the fork; not an upstream candidate.
