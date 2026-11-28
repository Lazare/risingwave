# Batch memory safety

Status of the fork, November 2026. The admission of batch queries and the pools of their
operators are described in `src/batch/src/task/query_memory.rs`. This note lists where batch
memory escapes that accounting, what was fixed, and three mechanisms to let heavy queries run
side by side without failing: revocable memory (implemented, `batch_spill_on_memory_wait`),
spilling the output of operators that cannot spill, and phased execution.

## 1. What the budget counts

A query reserves the pool of its spillable operators (hash aggregation, hash join, sort) before
it starts. The budget of the node only knows these reservations and the memory that operators
which cannot spill (top-n, top-n per group, lookup join, nested loop join) take beyond the pool
of their query. Everything else that a query allocates is invisible to it: the budget may show
a few gigabytes taken while the process holds several times more.

## 2. Where memory escapes the budget

Sizes below use a query with parallelism `P` on one node, chunks of 1024 rows of `R` bytes.

| Place | Counted? | Size at stake | Status |
|---|---|---|---|
| Task output channels (`output_channel_size` = 64 chunks per output) | no | `64 x 1024 x R` per output channel; an exchange `Single` with `P` producers holds up to `P x 64` chunks, e.g. 16 x 64 x 3 MB = 3 GB for rows of 3 KB | **counted** (section 7); bound in bytes with `batch_exchange_buffer_max_mb` |
| Hash shuffle: each output gets the whole chunk with a visibility bitmap (`hash_shuffle_channel.rs`) | no | a chunk stays alive until the slowest of the `P` consumers reads its part: up to 64 full chunks per slow consumer | **counted once**; compact mode `batch_compact_shuffle_output` (section 7) |
| Storage scans: one iterator per vnode (256), all polled at once (`flatten_unordered(1024)` in `batch_table/mod.rs`) | no | at least one decoded block per SST level per vnode: 256 x levels x 64 KB, i.e. 80 MB per active scan with 5 levels | open |
| Storage prefetch (`prefetch_blocks`, up to 16 blocks per iterator) | node-wide cap = block cache capacity, counted on the **compressed** size while the **decoded** blocks are held | decoded size is 2-5 times the cap for compressible rows (JSON) | open: count `uncompressed_size` |
| Blocks pinned by iterators and evicted from the block cache | foyer subtracts evicted entries from its usage even when they are still referenced | as much as the iterators hold | open |
| `MonitoredGlobalAlloc` (hash tables, vectors of operators): an allocation refused by the context was not counted, its deallocation was | under-counted, then negative | a hash table that grows near the limit: its new table (2x the old one) is not counted and the old one is subtracted: 3x the old table missing, and the counters of the query and of the node go below what is held | **fixed**: `MemoryContext::force_add` |
| `MemMonitoredHeap`, merge sort: same asymmetry | under-counted | small | **fixed** |
| Sort that spills: the buffered chunks stay counted after they are written to disk, and its sub-sorts share the context | over-counted, then sub-sorts spill again or run uncounted | the whole buffer of the sort (its share of the pool) for the rest of the sort | **fixed**: released after the spill and at the end |
| JSONB, varchar estimates (`JsonbVal`, `Box<str>`, `OwnedRow`) | exact (byte length of the buffer) | none | checked, nothing to fix |
| `first_value` / `last_value` with `ORDER BY` in a hash aggregation | counted; only the kept row per group is stored (`KeptRows`) | groups x (key + row): the whole JSON value per group | counted correctly; reducing it is a planner matter (narrow the aggregated value) |
| Lookup join: outer batch (>= 512 rows) | no | small | open, low priority |
| Lookup join: inner rows of a batch | counted, settled per chunk | grows with the fan-out of the keys | ok |
| Top-n per group: heaps reserved `min(limit + offset, 1024)` rows per group up front | counted | 32 bytes x reserved rows x groups, e.g. 32 MB per million groups with a limit of 1 000 | **fixed**: heaps grow with their rows |

The errors of operators that cannot spill name the operator (its identity) and the WARN logs of
the memory waits name it too.

## 3. Revocable memory (`batch_spill_on_memory_wait`, implemented)

**Problem.** An operator that cannot spill takes memory from the pool of its query, then from the
memory of the node that no query reserved, then waits. It fails at once when every running query
is waiting, even when the spillable operators of its own query hold gigabytes they could write
to disk.

**Mechanism.** The spillable operators of a query charge what they use beyond their reserved
minimum to a context, the *spill gate*, placed between them and the shared part of the pool.
When an operator that cannot spill is about to wait (no memory in the pool, none left on the
node), it lowers the limit of the gate to what the spillable operators hold minus what it needs.
The next spillable operator that asks for more is refused and spills: the hash aggregation
writes its groups and its input to disk partitions, the sort writes its buffer, the hash join
writes its build side. They give their memory back to the pool, where the waiting operator takes
it (it checks the pool every second). Their reserved minimum is never refused. The limit of the
gate is lifted when the last waiting operator of the query is served or gives up.

**Which operators react, and where.** Only those that grow: a hash aggregation while it consumes
its input (also its sub-aggregations), a sort while it buffers, a hash join while it builds. An
operator that already produces its output (aggregation emitting its groups, join probing, sort
emitting) does not allocate and keeps its memory until it ends. Operators of other tasks of the
query (other stages, other exchanges) react while the waiting task is blocked; an input of the
waiting operator in the same task cannot run while the task waits, so it cannot help.

**Deadlocks.** A spill request never blocks: spillable operators never wait for memory, they spill
when refused. The only spillable operator that waits is the hash join that holds rows sharing one
key (it cannot split them): it waits as an operator that cannot spill and may itself ask the
others to spill. A waiting operator does not count as *stuck* while it waits for its own spillable
operators (a grace period of 60 s after its request, when they held memory beyond their
minimum): without the variable it would fail at once when every query waits.

**Interaction with the overflow wait limit and with admission.** `overflow_memory_wait_secs` still
bounds the whole wait. Queries waiting to start still wait behind operators waiting for memory:
the request only changes what the query does with its own pool. With the variable, an operator
that already took memory beyond the pool may also take memory from the pool again (the memory
released by spilling goes there).

**Gain.** It turns failures of operators that cannot spill (a few hundred megabytes to a gigabyte
missing) into spills of the hash aggregations and joins of the same query, as long as they are
still consuming their input. It does not reduce the memory that escapes the accounting (section
2): that memory must be counted first.

**Cost and risk.** More spilling under pressure (disk I/O). The cap applies to all spillable
operators of the query on the node while one operator waits. Default off.

## 4. Spilling the output of operators that cannot spill (design only)

**Where output is buffered today.** Each task writes into its output channels (`fifo`, `hash`,
`consistent hash`, `broadcast`), bounded mpsc channels of `output_channel_size` chunks (64) per
output, not counted anywhere. Local consumers read the channel directly; remote ones through
`GetData` streams (another bounded channel of 64 responses per stream on the producer side).

**What it would take.** A spilling output channel: the sender counts each chunk it buffers in the
memory of the query (`ExecutorMemory` of the task output, so the budget sees it); when the
buffer exceeds a share of the pool, chunks are written to a spill file per output (the
`SpillOp` used by the hash aggregation: protobuf-encoded chunks) and read back in order when the
consumer catches up. A producer whose consumer is blocked then holds a bounded amount of memory
instead of 64 chunks per output. An operator that cannot spill while computing (top-n per group,
a non-spilling aggregation) gains nothing from it while it computes; only its finished output
could be written out, which matters when its consumer is slow.

**Effort.** Medium: one channel implementation wrapping the existing ones, its accounting, reading
back in order, cleanup on cancel. Counting the channels without spilling is a small first step
and makes the budget see them (then the admission and the wait logic apply).

**Cost.** Disk writes of every buffered chunk beyond the threshold; latency only when consumers
are slow, which is when it matters.

**Expected gain.** Bounded by the channel sizes: `P x 64` chunks per exchange. With rows of a few
kilobytes and a parallelism of 16, a few gigabytes per heavy query; this is likely a large part of
the memory that escapes the budget when several heavy queries gather `UNION ALL` inputs on one
node. Counting it is a prerequisite.

## 5. Phased execution (design only)

**Today.** The scheduler starts the leaf stages, and each parent stage as soon as all its children
have created their tasks (`QueryRunner::run`, `all_children_scheduled`): all the stages of a query
run at once, pipelined, and every operator of every stage holds its memory at the same time.

**What it would take.** A session variable (e.g. `batch_phased_execution`) and a stage dependency
rule in `QueryRunner`: start a parent stage only when its children have *finished*, their outputs
being kept until the parent reads them. The outputs must then be materialized: in memory they
would be the whole intermediate result, so it needs the spilling output of section 4 (each
finished task keeps its output on disk until it is read). For a `UNION ALL` of heavy branches,
the same rule can be applied to the children of the union stage one after another.

**Gain.** The peak of a query becomes the peak of its heaviest stage instead of the sum of its
stages: with a few equally heavy stages, a reduction of the same order as their number.

**Cost.** Latency: no pipelining between stages, and every intermediate result is written to and
read from disk once. Risk: the snapshot of the query must stay pinned for the whole duration
(already the case), and the admission reservation is held longer. Effort: large; it depends on
section 4.

## 6. Order of work

1. Count what escapes: storage prefetch on decoded sizes, task output channels (as
   `ExecutorMemory` of the task), the outer batch of the lookup join.
2. Spilling output channels (section 4).
3. Phased execution as an option for the heaviest queries (section 5).

## 7. Exchange buffers (implemented)

**Counting.** Each chunk that a task sends is counted in the memory of its query (a context
`task output`, child of the query context) when it enters the output channels, and released
when it leaves them (`output_buffer.rs`):

- a local consumer takes it from the channel;
- a remote consumer: the count travels with the serialized response through the `GetData`
  stream (`receiver_channel_size`, 1000 responses) and is released when gRPC takes the response;
- the slices that a hash shuffle (or a consistent hash shuffle, or a broadcast) sends to its
  outputs share the columns of the input chunk: they share one count, released with the last of
  them.

The count is forced (an allocation that already happened cannot be refused): a query whose
buffers grow leaves less room to its spillable operators, which then spill, and the memory of the
node sees it.

**Over-count on the consumer side.** A local consumer of a hash shuffle receives the whole input
chunk with a visibility that keeps about `1/Q` of its rows (`Q` outputs). An operator that kept it
counted the full chunk (`estimated_heap_size` counts the shared columns), `Q` times over all the
consumers, and kept the whole chunk alive. The operators that keep chunks for a while (hash join
build side, nested loop join left side, the outer batches of the lookup join) now compact a chunk
when fewer than half of its rows are visible (`compact_if_sparse`): they copy the visible rows,
count that copy in their own context, and drop their reference to the shared block. A chunk with
at least half of its rows visible is kept as it is and counted in full by the operator: at most
two consumers can keep such a slice of one block. The sort already compacted every chunk; the
other operators keep rows, not chunks.

**Lifetime of the count.** The block is counted once from the moment it enters the channels until
its last slice leaves them (taken by a local consumer, or handed to gRPC). Afterwards, a consumer
that streams the slice drops it right away, and a consumer that keeps it either compacts it or
counts it itself. A small window remains uncounted: a slice being processed by a streaming
operator, and the columns that an operator reuses in its output (a projection of input columns)
while that output waits downstream.

**Compact mode (`batch_compact_shuffle_output`, default false).** The hash shuffle appends the rows
of each output to a chunk builder of its own and sends full chunks: no chunk is pinned by the
slowest consumer, consumers get full chunks, and the count is exact. Cost: one copy of each row,
and up to one partial chunk per output gathered in the producer (not counted). The visibilities of
the outputs are now computed in one pass over the rows instead of one pass per output.

**Bound in bytes (`batch_exchange_buffer_max_mb`, default 0 = none).** The output channels of a task
hold at most this many bytes, on top of their bound in chunks: a producer whose consumers are slow
waits earlier when its rows are wide. A chunk larger than the whole bound waits for the buffer to
be empty. The bound is per task and shared by its outputs: like the bound in chunks, it can only
block a producer whose consumers do not read.

**Not counted yet.** The rows gathered by the compact mode, the root stage channel of the frontend
(`root_stage_channel_size`), the buffers of tonic and HTTP/2.

**Choosing the parallelism of a stage from its exchanges (design only).** A stage with `P` tasks
that shuffles to a stage of `Q` tasks may buffer, in the worst case, `P x Q x slots` chunks
(slices with the default mode, own chunks with the compact mode), and `P x Q x 1000` serialized
chunks on the gRPC side when the consumers are remote. With rows of estimated width `W` and chunks
of 1024 rows, the bound is `P x Q x slots x 1024 x W`, or `P x B` with the byte bound `B`. The
frontend already lowers the parallelism until the reserved memory of the spillable operators fits
the limit of the query (`batch_limits.rs`). The same loop could add the exchange term: choose the
largest `P` (and `Q`) such that

    reserved operators + sum over exchanges of min(P x Q x slots x 1024 x W, P x B) <= query limit

with `W` taken from the cardinality feedback when it has observed the sub-plan (bytes per row are
not recorded yet: they would be added to the feedback store next to the row counts), else from
the schema (fixed-size types exact, variable-size types at an assumed average). Gain: wide-row
exchanges get a lower parallelism or wait earlier instead of filling the node. Risk: a wrong width
estimate lowers the parallelism of a query that would have fit; it should only lower, never raise,
the parallelism chosen today.
