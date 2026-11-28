# Resource journal and open file limit

| | |
|---|---|
| Kind | operations |
| Branch | `fork/resource-journal-and-nofile-limit` |
| Status | integrated (`integration/v3.1.2`), present in rc4 to rc8 |
| Configuration | environment variable `RW_RESOURCE_JOURNAL_INTERVAL_SECS` (default 60, `0` disables) |

## Summary

At startup every RisingWave process raises its soft limit of open file descriptors to the hard
limit and logs both. A plain OS thread then writes a journal line periodically with the resources
the process uses: resident memory, open file descriptors and the limits they are measured against,
as a warning above 80% of a limit, and reports stalls of the process.

## Motivation

A process killed by the kernel (out of memory) or frozen by resource exhaustion leaves no trace of
the reason in its own log. Container runtimes often start processes with a soft limit of 1024 open
files and a much higher hard limit; spilling batch operators, object store connections and client
connections all draw from it, and exhausting it makes the server refuse connections.

## What it does

`src/utils/runtime/src/resource_journal.rs`, called from the runtime entry point
(`src/utils/runtime/src/lib.rs`):

- `raise_nofile_limit()` at startup;
- `spawn_resource_journal()`: a thread that does not depend on any async runtime, so it keeps
  writing when the runtime is stuck; a delay of more than 5 s beyond its interval is reported as a
  stall;
- unknown values are written as a dash; Linux `/proc` is read for the counts;
- `risectl` disables the journal.

## Configuration

`RW_RESOURCE_JOURNAL_INTERVAL_SECS`: interval in seconds, `0` disables. No configuration for the
file limit (always raised to the hard limit).

## Correctness / safety argument

Observation only, plus a limit increase within the hard limit allowed by the environment.

## Status and versions

Integrated in `integration/v3.1.2`; present in rc4 to rc8.

## Tests

None specific (not documented in the branch).

## Limits and risks

- One log line per minute per process by default.
- Counts come from `/proc`: on other systems the values are dashes.

## Upstream relation

Small and self-contained; could be proposed upstream. Not proposed yet.
