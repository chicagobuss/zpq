# Parquet Writer + S3 Sink Design (Phase 5)

The contract for `src/io/s3_sink.zig` and the small concurrency
primitives it sits on top of. Read this before writing or modifying
writer code.

> **The previous writer attempt is what made the project lose
> shape.** This document leads with the failure modes so we don't
> repeat them. The lesson was *not* "parallelism is bad" — Lambda
> has ~5 Gbps egress and we'll never saturate it serially. The
> lesson was "the abstractions around parallelism sprawled."

## What we are NOT building (the v1 failure modes)

The pre-rewrite writer/sink layer was ~1,300 LoC across ten files.
For comparison, our entire current filter system (AST + parser +
encoded comparator + prune + selection vector + eval) is ~900 LoC.
A single feature with more code than the entire filter pipeline is
the smoking gun.

Specifically rejected:

| Pre-rewrite artifact | Rejected because |
|---|---|
| `Sink` vtable with three impls (`s3`, `local`, `memory`) | One production sink (S3). Build it directly. Add an interface only when a second concrete impl actually exists. |
| `factory.zig` (110 LoC) | Implies enough variation to need dispatch. Today there isn't. |
| `sink/morsel.zig` + `sink/channel.zig` + `sink/task.zig` triad (245 LoC) | Three abstractions for "send work to a worker." Channel + morsel + task is the *generic* worker-pool scaffolding from systems like DuckDB's task scheduler. We don't need it; see Concurrency below. |
| Three Parquet writer locations (`sink/writer.zig`, `s3/sink_writer.zig`, `core/writer.zig`) | One writer file. Pick a location. |

The pre-reset code wasn't *bad* in isolation — each file was reasonable.
The problem was the composition: too many seams, too many shared
abstractions, three concrete sinks behind a vtable when only S3 mattered,
worker-pool plumbing for parallelism that hadn't been measured.

## Concurrency: the shape we *do* want

Lambda has ~5 Gbps egress. One TLS stream sustains ~100–300 MB/s in
practice. To saturate that pipe we need 4–8 concurrent streams.
Sequential won't cut it.

Decided substrate: `std.Io.Threaded` + `Io.Group.concurrent`. Reasoning
trail and bakeoff data in `docs/journal/2026-05.md` (entry 2026-05-04).
Short version:

- Zig 0.16 stable ships `Io.Group` as the morsel pattern in stdlib.
  ~10 lines of caller code parallelizes 8 multipart PUTs across
  threads. No custom worker-pool, no channels.
- Hybrid model: in-tree epoll loop stays for the reader (shipped,
  works), `Io.Threaded` for the writer. The seam is at the phase
  boundary, not inside a hot loop.
- Bakeoff confirmed: throughput is CPU-bound on TLS encryption and
  scales with Lambda memory tier. An event loop would lose by
  construction (single-threaded, can't parallelize encryption across
  cores).

```zig
// Writer parallel-dispatch shape:
pub fn writeMultipart(io: std.Io, parts: []const Part, ...) !void {
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    for (parts) |p| try group.concurrent(io, uploadPart, .{ io, p });
    try group.await(io);
}
```

The thread owns its TLS connection for the upload's lifetime; when
the function returns, the connection drops. No shared `s3.Client`, no
`Io.Mutex`, no slot tracking.

## Phase plan (revised)

| Phase | What it ships | Concurrency model |
|---|---|---|
| **5.1** | Single-PUT fast path: surviving row groups copied byte-for-byte, footer offsets rewritten. Output ≤5 GB. End-to-end S3-to-S3 in Lambda. | Sequential. One TLS connection. |
| **5.2** | Sequential multipart for output >5 MB (or >5 GB; the threshold is configurable). Each row group's bytes streamed into multipart parts. | Sequential. One TLS connection. |
| **5.3** | **Parallel multipart parts via `std.Io.Group` on `Io.Threaded`.** Multiple concurrent PUTs, one thread per in-flight part. | `Io.Group` + `Io.Threaded`. N≈8 threads, each owning one TLS connection for its lifetime. |
| **5.4** | Decoder + encoder pipeline for per-row filtering and column projection. | Same `Io.Group` pattern; CPU work and I/O share the thread pool. |

5.1 and 5.2 are sequential — they ship the headline fast-path product.
5.3 lands the parallelism primitive. 5.4 is the largest phase and
ships only after 5.1–5.3 are battle-tested.

## Connection pool for parallel parts

Phase 5.3 needs N concurrent TLS connections to S3 (one per in-flight
part, since HTTP/1.1 doesn't multiplex). With `Io.Threaded` each
spawned task is a real OS thread, so the natural shape is:

- The writer constructs an `s3.Client` per task at task start,
  PUTs the part, and drops the connection on return.
- No shared mutable client, no `Io.Mutex`, no slot-tracking.
- TLS handshake cost is paid N times rather than amortized — but
  this only happens once per Lambda invocation (8 connections
  total for an 8-part upload), and the handshake is ~100ms while
  the part upload is several seconds.

If profiling shows handshake cost matters, the upgrade is a
fixed-size `[N]s3.Client` array guarded by an `Io.Semaphore` —
threads `acquire` a permit, take the matching client, release on
return. Still under 50 LoC. Lambda's per-invocation lifetime
makes idle-timeout management irrelevant either way.

## Output file layout

Standard Parquet:
```
[PAR1 magic][rg0_data][rg1_data]...[rgN_data][footer thrift][footer_len u32 LE][PAR1]
```

For the fast path: each surviving row group's column chunks are
copied byte-for-byte into the output. Their *file offsets* shift
relative to the output file's start. The footer's
`row_groups[i].columns[j].meta_data.data_page_offset` (and
`dictionary_page_offset` if present) need to be rewritten with
the shifted values.

We never re-encode anything; we renumber pointers. This is the trick
that makes the fast path actually fast.

## Memory model

**Phase 5.1**: PUT requires the full body in one HTTP request. So we
buffer the entire output in memory before issuing the PUT. Lambda has
2+ GB; our 155 MB benchmark trivially fits. Files larger than ~1 GB
are Phase 5.2 territory.

**Phase 5.2**: Stream into multipart parts. Each part is buffered
in-flight (~5–8 MB). Many parts; bounded memory.

**Phase 5.3**: Parallel parts means multiple in-flight buffers
concurrently — bounded by N × part_size. With N=8 and part=8 MB,
that's 64 MB peak — well under any Lambda tier.

## File layout

Single sink file, ~400 LoC max:

```
src/io/s3_sink.zig
  pub const FastPathWriter = struct {
      // Lifecycle
      pub fn init(arena, s3_client, key) !FastPathWriter;
      pub fn writeFromInput(input_file_bytes, input_meta, surviving_rgs) !void;
      pub fn finish() !void;
      pub fn deinit();
  };
```

Plus, when 5.3 ships:

- The Lambda entry point upgrades from
  `pub fn main(init: std.process.Init.Minimal)` to
  `pub fn main(init: std.process.Init)` so we get `init.io`
  (a `Threaded` backend instance).
- `src/io/s3_sink.zig` gains an `uploadParts(io, parts) !void`
  function that spawns into an `Io.Group`. **No new files.**

If we end up needing the bounded-client pool (see Connection Pool
above), it's ~50 LoC inside `s3_sink.zig` or folded into `s3.zig`.
~500 LoC total for the entire writer subsystem at end of Phase 5.3,
zero new framework files.

## Three rules (the budget)

**Rule A — One concrete impl before any interface.** No `Sink` vtable
unless a second concrete sink actually materializes. If it does, the
extraction is mechanical.

**Rule B — One file per real responsibility, not per category.**
`s3_sink.zig` does S3 sink work. If it grows past ~500 LoC, *then*
split — and only at a real seam (e.g., "footer assembly" vs "S3
upload"). Don't pre-split into `sink/`, `s3/sink/`, etc.

**Rule C — Use stdlib primitives before writing your own.** Zig 0.16
ships `Io.Group`, `Io.async`, `Io.Threaded` — these *are* the morsel
pattern. We don't write a worker pool; we call `group.async`. We don't
write an in-flight tracker; `Io.Group` tracks. The only custom
concurrency code we should write is whatever stdlib is missing — and
the burden of proof is on us to demonstrate it's missing. No
channels, no worker-pool runtime, no "morsel" type. If we ever need
something heavier, that's the moment to evaluate; not now.

## What this rejects (and why)

- **Local file sink.** Not a real workload for ZPQ; deploy targets
  are Lambda + (eventually) container/CLI. CLI can use the same S3
  path against MinIO or local S3-compatible. Don't add a separate
  code path.
- **In-memory sink.** Tests use raw bytes from the integration
  test harness; no need for a vtable.
- **Channel-based work-stealing.** When we go parallel, the pattern
  is N concurrent multipart PUTs from the same Loop, not workers
  consuming a queue. Different shape from the old design.
- **Encoder before benchmarks demand it.** The fast path covers any
  workload where rows aren't filtered out within a row group (which
  is most ETL-style copies and most simple filters). When a real
  workload demonstrates we're losing on per-row filters, ship 5.4.
- **HTTP/2 / pipelining for parallel parts.** N independent HTTP/1.1
  connections is the dispatch model that matches our access pattern
  (one upload per part, no shared semantics across parts). HTTP/2
  multiplexing would be more efficient for many small requests but
  marginal for ~8 large parts.

## Test plan

### 5.1 (sequential PUT)
- Unit: synthetic input bytes + filter pruning 0/some/all row
  groups → output buffer assembled correctly: leading magic,
  trailing magic, parseable footer, correct shifted offsets.
- Roundtrip: write → read back via metadata.open + ColumnChunkReader,
  verify decoded values match input's surviving row groups.
- Integration: real Lambda invocation. Input the benchmark, filter
  `int8>0`, write to a fresh S3 key, verify the output is a valid
  Parquet file (re-invoke ZPQ on the output and confirm row count).

### 5.2 (sequential multipart)
- All 5.1 tests still pass.
- Add: assertion about part count + size.
- Add: large-output test (≥10 MB) confirms multipart path is taken.

### 5.3 (parallel)
- All 5.2 tests still pass.
- Add: timing test confirming N=8 wallclock < N=1 wallclock for the
  large-output case. (Validates the perf was real.)

### 5.4 (decode + re-encode)
- Property test: for each (input, filter, projection) tuple, decoded
  output equals input rows where filter holds, projected to selected
  columns. Cross-validate against PyArrow / DuckDB.

## Anti-patterns checklist (from `tier_3_s3_pipeline_strategy.md`)

The surviving design doc already lists what to avoid:

1. **Workers calling loop.run()** — N/A in 5.1–5.3 (no separate workers).
   Will become relevant in 5.4 if/when we add CPU workers.
2. **Serial S3 requests** — accepted in 5.1 (simplest correct version).
   5.3 fixes this.
3. **Buffering entire file before writing** — yes in 5.1 (intentional;
   file fits in RAM). 5.2 fixes via multipart.
4. **Single connection for all reads** — already addressed by the
   keep-alive S3 client.
5. **Ignoring column statistics** — already addressed by filter
   pushdown.
6. **Starting multipart unconditionally** — addressed by the
   threshold switch in 5.2.
