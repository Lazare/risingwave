# Exchange buffer memory accounting (batch)

| | |
|---|---|
| Kind | engine (batch) |
| Branch | `proto/plan-reduction-batch-exchange-mem` |
| Status | candidate (not in rc8) |
| Variables | `batch_exchange_buffer_max_mb` (0 = no bound), `batch_compact_shuffle_output` (false) |
| Design note | `docs/dev/src/design/batch-memory-safety.md`, section 7 "Exchange buffers", on the branch |

## Summary

The chunks waiting in the output channels of batch tasks are counted in the memory of their query,
once per block: the slices of a hash shuffle or broadcast share one count attached to the chunk
(`SharedChunkMemory`), released when the last holder drops it. An optional byte bound limits what a
task's output channels hold, and an optional compact mode makes the hash shuffle build full chunks
per consumer.

## Motivation

Task output channels (64 chunks per output) and hash shuffles (each consumer receives the whole chunk
with a visibility bitmap) are not counted anywhere upstream. With wide rows and a high parallelism,
an exchange can hold gigabytes that no budget sees, typically when several heavy queries gather
`UNION ALL` inputs on one node.

## What it does

- Counting: a chunk entering the output channels is counted in a `task output` context of its query;
  the count travels with the serialized response through the `GetData` stream until gRPC takes it.
- Operators that keep slices (hash join build, nested loop join, merge sort) count their share of the
  block (`kept_bytes`) and take it out of the shared count instead of copying.
- The hash join, when it needs memory, copies the slices that nobody else shares any more and that
  show fewer than half of their rows, freeing the rest of the block.
- `batch_exchange_buffer_max_mb`: the output channels of a task hold at most this many bytes, on top
  of the bound in chunks.
- `batch_compact_shuffle_output`: the hash shuffle appends each consumer's rows to its own chunk
  builder and sends full chunks (one copy per row).
- Visibilities of shuffle outputs are computed in one pass over the rows.

## Configuration

Both session variables, `SESSION_INIT`; counting itself is always on in the branch.

## Correctness / safety argument

Counts are forced (memory already allocated cannot be refused): a query whose buffers grow leaves
less room to its spillable operators, which spill. The byte bound only blocks a producer whose
consumers do not read, like the existing bound in chunks. Results are unchanged.

## Status and versions

Candidate: branch based on the rc8 line, not merged into any image.

## Tests

`e2e_test/batch/exchange_buffer_memory.slt`; unit tests of the output buffer, the shared chunk count
and the hash shuffle channel.

## Limits and risks

Not counted yet: rows gathered by the compact mode, the root stage channel of the frontend, tonic and
HTTP/2 buffers. A holder that adopted its share and drops its slice while others hold the block
leaves its share uncounted until the block is freed. Touches `DataChunk` in `risingwave_common`.

Design only, in the same note: choosing the parallelism of a stage from the memory of its exchanges.

## Upstream relation

Upstream does not count exchange buffers. Upstream candidate after validation.
