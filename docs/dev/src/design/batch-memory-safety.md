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

**A lighter form is implemented** (`batch_phased_stages`, section 8): it needs no materialized
output, because it only delays the stages that their consumer would not read yet anyway.

## 6. Order of work

1. Count what escapes: storage prefetch on decoded sizes, task output channels (as
   `ExecutorMemory` of the task), the outer batch of the lookup join.
2. Spilling output channels (section 4).
3. Phased execution as an option for the heaviest queries (section 5).

## 7. Exchange buffers (implemented)

**One count per block.** A chunk that a task sends is counted once in the memory of its query (a
context `task output`, child of the query context) when it enters the output channels. The count
is attached to the chunk itself (`SharedChunkMemory`, an `Arc` held by `DataChunk`): the slices
that a hash shuffle (or a consistent hash shuffle, or a broadcast) sends to its outputs share the
columns of the chunk and share its count, and so does every chunk downstream that keeps sharing
the columns (`with_visibility`, `clone`, projections). The count is released when the last of them
is dropped, wherever it went. A chunk that copies the rows (`compact_vis`, a new chunk) does not
share it. A task that forwards a chunk that is already counted does not count it again.

The count is forced (memory that is already allocated cannot be refused): a query whose buffers
grow leaves less room to its spillable operators, which then spill, and the memory of the node
sees it.

**Operators that keep slices.** The consumers of a shuffle are the tasks of one stage that run the
same plan: they usually all keep their slices (all build their hash table) or all drop them.
Sharing is the common case, and copying the visible rows of each slice would duplicate the block
for nothing. So an operator that keeps a slice (hash join build side, nested loop join left side,
merge sort) does not copy it: it counts its share in its own context, the bytes of the block times
its visible rows over the rows of the block (`DataChunk::kept_bytes`), and takes that share out of
the shared count (`adopt_shared_memory`). The shares of all the holders sum to the block, which is
counted once in total. The sort still copies every chunk it keeps, as before (it needs compact
rows).

**Copy only as the sole holder.** When the hash join needs memory, before it spills or waits, it
copies the visible rows of the slices that no other chunk shares any more (`Arc::strong_count` of
the shared count is 1) and of which fewer than half of the rows are visible: the other consumers
spilled, ended or dropped theirs, and the copy frees the rest of the block. A slice that others
still share is never copied.

**What remains uncounted.** When a holder that adopted its share drops its slice while others still
hold the block, its share is no longer counted until the block is freed (the consumers of a stage
usually drop their slices together). The columns that an operator reuses in a new chunk (a
projection of input columns into its output) are no longer attached to the count.

**Bound in bytes and remote consumers.** The bound of `batch_exchange_buffer_max_mb` covers the time
spent in the channels only: a slice releases its room when it leaves them, never later, since an
operator that keeps a slice must not block the producer that it waits for. A serialized chunk that
waits in the `GetData` stream of a remote consumer (`receiver_channel_size`, 1000 responses) is
counted for its size and keeps the room of its chunk until gRPC takes it.

**Compact mode (`batch_compact_shuffle_output`, default false).** The hash shuffle appends the rows
of each output to a chunk builder of its own and sends full chunks: no chunk is pinned by the
slowest consumer, consumers get full chunks, and the count is exact. Cost: one copy of each row,
and up to one partial chunk per output gathered in the producer (not counted). Optional and off by default: sharing the block is the common case. The visibilities
of the outputs are now computed in one pass over the rows instead of one pass per output, in both
modes.

**Bound in bytes (`batch_exchange_buffer_max_mb`, default 0 = none).** The output channels of a task
hold at most this many bytes, on top of their bound in chunks: a producer whose consumers are slow
waits earlier when its rows are wide. A chunk larger than the whole bound waits for the buffer to
be empty. The bound is per task and shared by its outputs: like the bound in chunks, it can only
block a producer whose consumers do not read.

**Not counted yet.** The rows gathered by the compact mode, the root stage channel of the frontend
(`root_stage_channel_size`), the buffers of tonic and HTTP/2.

**How much a producer pins.** A producer sends the slices of each block to its `Q` output queues in
turn (`HashShuffleSender::send_chunk`, empty slices skipped) and blocks as soon as one queue is
full (`slots` = `output_channel_size` = 64). The blocks that it keeps alive are those between the
oldest slice still queued (in the queue of the slowest consumer) and the block being sent: at most
`slots + 1` blocks when every block has rows for every output, which is the normal case with a
hash distribution and chunks of 1024 rows, whatever `Q` is. The local exchange memory of a
producer is then about `(slots + 1) x chunk bytes`, and of an exchange of `P` producers about
`P x (slots + 1) x 1024 x W` for rows of width `W`: not `Q` times more, since the `Q` outputs
share each block. Exception: with a skewed distribution, an output that gets rows from only a
fraction `f` of the blocks holds 64 queued slices spanning about `64 / f` blocks, and the pinned
window grows up to that, bounded by the blocking of the producer on its other queues.

On the gRPC path (consumers on other nodes), each slice is compacted when it is serialized, and
each `GetData` stream buffers up to `receiver_channel_size` (1000) messages of about `1024 / Q`
rows: up to about `1000 x 1024 x W` per producer over its `Q` streams, plus the window of 64
slots, again independent of `Q`, and much larger than the local window. The byte bound
`batch_exchange_buffer_max_mb` is what limits it: a serialized chunk keeps the room of its slice
in the bound until gRPC takes it.

**Choosing the parallelism of a stage from its exchanges (design only).** The frontend already
lowers the parallelism until the reserved memory of the spillable operators fits the limit of the
query (`batch_limits.rs`). The same loop could add the exchanges: choose the largest `P` such that

    reserved operators
      + sum over local exchanges of P x (slots + 1) x 1024 x W
      + sum over remote exchanges of P x min(B, 1000 x 1024 x W)
      <= query limit

with `B` the byte bound (no bound: `1000 x 1024 x W`), and `W` taken from the cardinality feedback
when it has observed the sub-plan (bytes per row are not recorded yet: they would be added to the
feedback store next to the row counts), else from the schema (fixed-size types exact,
variable-size types at an assumed average). Gain: wide-row exchanges get a lower parallelism
instead of filling the node. Risk: a wrong width estimate lowers the parallelism of a query that
would have fit; the rule must only ever lower the parallelism chosen today, never raise it.

## 8. Phased start of the stages (`batch_phased_stages`, implemented)

**Problem.** The scheduler starts every stage as soon as its children have created their tasks.
In a query with many stages, every scan runs at once (each holding its decoded blocks, section 2)
and the producers of the probe side of each hash join fill their output channels (section 7)
while the join still reads its build side: memory outside the operators grows with the number of
open stages, not with what the query needs at a given time.

**Mechanism** (`scheduler/distributed/phased_stages.rs`, `QueryRunner::run_phased`). A hash join
reads its build input (right) to the end before it reads its probe input (left); a nested loop
join buffers its left input before it streams its right one. For each such join of a stage
whose build side reads child stages, the stages read by its probe side, and every stage below
them, wait until the stages of the build side have finished. The stage of the join itself starts
as before: the workers of the tasks of its deferred children are chosen in advance
(`StageExecution::preassign_workers`, the same choice as when scheduling them), so its exchanges
know where to read. Once a build stage has finished, the join has read (nearly) all its build
input; the deferred stages are then created on their workers, and a consumer that asks for the
output of a task not created yet waits for it on the compute node (`BatchManager::wait_for_task`,
at most `MISSING_TASK_WAIT` = 120 s). Only the sources of a deferred stage wait: the frontend
sets `ExchangeSource.wait_for_producer` on the exchange sources whose task had its worker chosen
in advance, the consumer forwards it in `GetDataRequest.wait_for_producer`, and the producer node
(or the local exchange path) waits only when it is set. Any other missing task, such as a
cancelled one, fails at once as before, with or without the variable.

Example: `SELECT .. FROM f JOIN d1 ON .. JOIN d2 ON ..` planned as two hash joins in one stage
(probe `f`, builds `d1` and `d2`): `d2` starts first, `d1` once `d2` has finished, `f` once both
have finished. The scan of `f` and its exchange buffers no longer overlap the builds.

**No deadlock.**
- A stage only waits for stages that feed the build side of a join of one of its ancestors:
  never for an ancestor or a descendant of its own. A stage in the wait set of another has a
  strictly smaller wait set: the waits form no cycle.
- A build stage finishes once its consumer has read all its output, which needs nothing from
  the deferred side. That holds only if every operator between the join and the build exchanges
  reads its inputs to the end (project, filter, aggregations, sort, top-n, hash join, union...);
  a `LIMIT` or a limited distinct on the build side may stop reading, so such a join is left
  alone.
- Operators that read several inputs at the same time get them together: a deferred subtree
  starts as a whole (a union on the probe side starts all its inputs together), a merge sort
  exchange reads one child stage, and only hash and nested loop joins defer anything. Batch has
  no sort-merge join.

**Stages that nobody reads any more.** With the variable, a stage whose consumers have all
finished is stopped (its tasks cancelled), and recursively below it, instead of being waited for:
a satisfied `LIMIT`, or the root stage that ended. Before, such a producer whose output was never
taken (a consumer that ended before asking for it) blocked on its full channel, and the query
waited for it until its timeout. The cardinality feedback ignores the counts of stopped stages
(their counts are incomplete).

**Not done: a bound on the number of running stages.** A plain bound would delay inputs that a
consumer reads together (a union polls all its inputs, a nested loop or hash join needs its
build side): the consumer would wait for a task that the bound holds back. The dependency order
above already bounds what runs at once for join trees; a bound for union branches would need
their consumer to read them one after another.

**Cost.** Less pipelining: the probe side of each join starts only after its build side. The
latency of a query grows by the time of its build sides when its probe scans are the slower
part; queries whose builds are small are hardly affected. One more round of task creations per
phase (a few milliseconds). Default off.

## 9. Admission by estimate (`batch_admission_by_estimate`, implemented)

**Problem.** Admission reserves the pool of the spillable operators of a query (section 1); a
query without spillable operator reserves nothing. Three queries with wide rows and many stages
are admitted together and then hold several times the budget in exchange buffers and scans,
which are counted (section 7) only once allocated.

**Mechanism** (`scheduler/admission_estimate.rs`). The frontend estimates, from the plan, what
the query holds beyond its operators:

    sum over non-root stages of  tasks x min(B, (output_channel_size + 1) x chunk_size x W)
    + number of table scans x 256 vnodes x 2 levels x 64 KB

with `W` the width of a row of the output of the stage from its schema (fixed-size types exact,
`varchar` 36 bytes, `jsonb` 260 bytes, nested types 128 bytes, plus one byte per column) and `B`
the bound `batch_exchange_buffer_max_mb` when set. Below 256 MB nothing is reserved, so small and
narrow queries are not affected; above, the estimate is sent to every compute node in the
admission request (`AdmitQueryRequest.extra_reservation_bytes`). The node reserves it on top of
the pool, at most the limit of a query, the whole reservation at most its usable memory, and
admits queries in arrival order as before: a query whose estimate does not fit beside the
running ones waits. A query without spillable operator now goes through admission when its
estimate is reserved.

**Conservative.** Every buffer is assumed full at once and the estimate is not divided between
the nodes (like the pool of the operators). With `batch_phased_stages`, the deferred stages do
not fill their buffers while the builds run, so the estimate is higher than the real peak. The
widths of the schema are guesses; the cardinality feedback does not record bytes per row yet.
The parallelism is not lowered from the estimate (the loop of `batch_limits.rs` only counts
the operators): reserving delays queries instead of slowing each of them.

**Cost.** Queries with wide rows wait for each other instead of running together: higher latency
under concurrency, fewer memory failures. Default off.

