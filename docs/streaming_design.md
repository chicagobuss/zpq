# Design: Streaming Output (B3)

**Status:** designed (2026-05-04 evening), implementation in progress
on branch `b3-streaming`.

**Goal.** Lift our hard ceiling on output size. Today we build the
entire output bytes in memory, then upload. Practical cap is ~3 GB on
a 5 GB Lambda. After streaming, output size becomes a function of
network throughput × invocation duration, with bounded working
memory regardless of file size.

**Secondary goal.** Overlap encode and upload so total wallclock for
filter+encode workloads gets a 10-20% bump on Lambda where encode and
upload are comparable in latency.

---

## Survey of comparables

Studied at 22:50 on 2026-05-04, before designing.

- **Polars `FileStreamer<W: AsyncWrite>`** — generic over the writer.
  `start()` writes magic, `write(row_group)` writes one row group's
  bytes through W and accumulates a tiny `RowGroup` thrift struct
  in memory, `end()` writes column/offset indexes + footer. The
  writer never knows whether W is a file, a buffered writer, or an
  S3 sink. **This is the right shape for our streaming writer
  layer; we steal it directly.**

- **DuckDB `S3FileSystem` / `S3FileHandle`** — pattern (c)
  explicitly. `uploads_in_progress` counter under a mutex; two
  condvars (one for scheduling backpressure, one for drain); detached
  threads do uploads. `FlushAllBuffers` waits on the drain condvar
  for `uploads_in_progress == 0` before sending
  `CompleteMultipartUpload`. **Production-tested; we use this
  concurrency model verbatim, expressed in Zig 0.16 primitives.**

- **arrow-rs `WriteMultipart`** — has a `max_concurrency` field;
  internally uses `FuturesUnordered`. **Pitfall caught (issue
  #5366):** if upload futures live in producer state and the
  producer pauses, the futures don't get polled and S3 connection
  timeouts fire. They added `wait_for_capacity` for explicit
  backpressure. Mitigation: spawn real worker tasks into the
  executor's pool (Io.Threaded does this naturally) so they keep
  being scheduled regardless of what the producer thread is doing.

- **Vortex `vortex-io`** — turned out to use **pattern (b)**
  (synchronous burst per write_all call) for the actual S3 path,
  which surprised me. They have a clever
  `SizeLimitedStream` (`limit.rs`) that uses byte-aware
  `tokio::sync::Semaphore` permits with `OwnedSemaphorePermit` tied
  to future lifetime, but it's only used for read-side prefetching,
  not writes. **Two valuable transferable ideas regardless:** (1)
  limit by bytes-in-flight, not part count; (2) tie permit
  lifetime to the worker task via `defer release()` so cancellation
  / error paths can't leak permits.

- **Imfeld blog post** — naive `tokio::spawn` with no concurrency
  bound, JoinHandle for tracking. Documented as a footgun.

- **Hardwood** — read-only library; their `Executors.newFixedThreadPool`
  for decode tasks doesn't apply (small queueable tasks vs our MB-
  sized work units).

Synthesis: **DuckDB's pattern (c) × Vortex's byte-aware bound, in
Zig 0.16 primitives.** Pipelining is worth the complexity because
Zig's `Io.Semaphore` / `Io.Group` / `Io.Mutex` / `Io.Condition` make
it about as much code as the synchronous variant. The arrow-rs
unpolled-futures pitfall doesn't bite because workers run in
`Io.Threaded`'s pool, not in producer-side state.

---

## Architecture

Two new modules + a refactor:

### `src/io/multipart_sink.zig` (new)

The S3-specific sink. Byte-aware backpressure, long-lived `Io.Group`
for in-flight tracking, fall back to single PutObject when only one
"part" was ever written.

```zig
pub const MultipartSink = struct {
    // S3 session state
    creds: s3.Credentials,
    url: s3.Url,
    pool: *s3.Pool(POOL_SIZE),
    gpa: std.mem.Allocator,
    io: std.Io,

    // Multipart state — created on first part flush, finalised at close
    upload_id: ?[]const u8 = null,

    // Producer-side accumulator
    buffer: std.ArrayList(u8) = .empty,

    // In-flight tracking — DuckDB pattern (c) in Zig form
    bytes_lock: std.Io.Mutex = .init,
    bytes_cond: std.Io.Condition = .init,
    bytes_in_flight: usize = 0,
    /// MAX_BYTES_IN_FLIGHT — soft cap on memory committed to in-
    /// flight upload tasks. Not a hard part-count limit; one giant
    /// part is allowed to exceed this transiently when the producer
    /// pushes a single chunk that's already > the cap (pragmatic
    /// degradation, not a deadlock).
    max_bytes_in_flight: usize = 64 * 1024 * 1024,

    // Long-lived Group: per Io docs, "resources are released when
    // the individual task returns, as opposed to when the whole
    // group completes or is awaited. For this reason, it is not a
    // resource leak to have a long-lived group which concurrent
    // tasks are repeatedly added to."
    group: std.Io.Group = .init,

    // Per-part etag storage; written by workers, read at close
    etags: std.ArrayList(EtagSlot) = .empty,
    next_part_number: u32 = 1,

    // Shared error slot — first worker error wins, set under the
    // mutex. close() returns this if any part failed.
    first_error: ?[]const u8 = null,

    pub fn init(...) !MultipartSink;
    pub fn push(self, bytes: []const u8) !void;        // append to buffer; flush parts when ≥ TARGET_PART_SIZE
    pub fn close(self) !void;                          // flush remainder; drain; complete or fall back to PutObject
};
```

**Backpressure under variable part sizes.** Producer reserves
`bytes` permits before scheduling a task. If `bytes_in_flight + bytes
> max_bytes_in_flight` AND `bytes_in_flight > 0`, wait. The "AND
bytes_in_flight > 0" clause is the pragmatic-degradation case from
Vortex's pattern: a single 200 MB part on a 64 MB cap is allowed to
proceed because no other parts are in flight (otherwise we'd
deadlock). Workers `defer post()` so cancellation / error
auto-releases.

**Drain at close.** `close()` calls `group.await(io)` after pushing
the final partial buffer as the last part. The `Io.Group` already
implements drain semantics — when all `concurrent` tasks return,
`await` returns. We don't need a separate condvar pair for drain.

**Single-PUT fallback.** If `next_part_number == 1` at close (no
flushPart ever happened), we send a single PutObject for the
buffered bytes. Multipart upload is never created for small
outputs. Matches DuckDB's behaviour.

### `src/core/writer/streaming.zig` (new)

Generic-over-sink parquet writer. Mirrors Polars's `FileStreamer`.

```zig
pub fn StreamingWriter(comptime Sink: type) type {
    return struct {
        const Self = @This();

        sink: *Sink,
        arena: std.mem.Allocator,
        schema_tree: schema_tree.SchemaTree,
        created_by: ?[]const u8,

        offset: u64 = 0,
        row_groups: std.ArrayList(schema.RowGroup) = .empty,

        pub fn start(self: *Self) !void {
            try self.sink.push(&MAGIC);
            self.offset = MAGIC.len;
        }

        /// Push one row group's bytes. The `rg_metadata` is built by
        /// the caller (with the column-chunk sizes computed from
        /// what they pushed), then we adjust per-column file
        /// offsets to absolute positions and accumulate.
        pub fn writeRowGroup(self: *Self, bytes: []const u8, rg_meta: schema.RowGroup) !void {
            const start_offset = self.offset;
            // shift per-column offsets to absolute file position
            for (rg_meta.columns.items) |*chunk| {
                if (chunk.meta_data) |*m| {
                    m.data_page_offset += @intCast(start_offset);
                    if (m.dictionary_page_offset) |dp|
                        m.dictionary_page_offset = dp + @intCast(start_offset);
                }
            }
            try self.sink.push(bytes);
            self.offset += bytes.len;
            try self.row_groups.append(self.arena, rg_meta);
        }

        pub fn end(self: *Self) !void {
            const meta: schema.FileMetaData = .{
                .version = 1,
                .schema = try self.schema_tree.writeFlatThrift(self.arena),
                .num_rows = self.totalRows(),
                .created_by = self.created_by,
                .row_groups = self.row_groups.toOwnedSliceLite(),
            };
            var w: thrift.Writer = .init(self.arena);
            defer w.deinit();
            try meta.write(&w);
            const footer = w.bytes();
            const footer_len: u32 = @intCast(footer.len);
            try self.sink.push(footer);
            var len_bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &len_bytes, footer_len, .little);
            try self.sink.push(&len_bytes);
            try self.sink.push(&MAGIC);
        }
    };
}
```

The encoder's per-row-group arena lives only for the duration of
`encode_one_row_group()`. After the bytes are pushed to the sink,
the arena resets and the next row group reuses the memory.

### `src/lambda/main.zig` refactor

`buildFilteredOutputMulti` and the byte-copy fastpath both flip
inside-out:

```
old:                                   new:
let out: ArrayList(u8) = .empty        sink = MultipartSink.init(io, ...)
... build the whole thing ...          writer = StreamingWriter.init(&sink, ...)
upload(out)                            try writer.start()
                                       for each surviving row group:
                                           rg_bytes, rg_meta = encode_one_rg()
                                           try writer.writeRowGroup(rg_bytes, rg_meta)
                                           // per-rg arena resets here
                                       try writer.end()
                                       try sink.close()
```

The byte-copy fastpath becomes especially clean: each row group's
bytes are already known (we have them in `file_buf`), so writeRowGroup
just pushes the slice directly and accumulates the cloned-shifted
`RowGroup` metadata.

### `src/io/s3.zig` refactor

`uploadMultipart(io, p, arena, gpa, creds, url, body)` becomes a
thin convenience wrapper:

```zig
pub fn uploadMultipart(io, p, arena, gpa, creds, url, body) !void {
    var sink = try MultipartSink.init(io, p, arena, gpa, creds, url, .{});
    try sink.push(body);
    try sink.close();
}
```

Existing callers continue to work without source changes; the
streaming writer uses `MultipartSink` directly.

---

## Memory model

**Working memory at any point during streaming:**

- One row group's encoded bytes (in producer-side ArrayList): bounded
  by row-group size, typically 14-50 MB.
- Up to `max_bytes_in_flight` (default 64 MB) of bytes committed to
  in-flight UploadPart tasks. Each task holds an owned slice that's
  freed when the task completes.
- Accumulated `RowGroup` thrift metadata: tiny (~KB even for
  thousands of row groups).
- The schema tree: tiny.

**Total bound: ~150 MB regardless of total output size.** This is
the load-bearing claim. A 50 GB output writes through this 150 MB
working set.

---

## Correctness invariants

1. **Per-row-group offsets are correct.** `writeRowGroup` shifts
   each column chunk's `data_page_offset` to the absolute file
   position before pushing. Tracked via `self.offset` which advances
   after each push.

2. **Multipart parts ≥ 5 MB except last.** S3 hard limit. Sink's
   `TARGET_PART_SIZE` (default 19 MB) is well above; the last
   partial flush in `close()` is allowed to be < 5 MB because S3
   only requires that of *non-last* parts.

3. **No cross-task data races on shared state.** `bytes_in_flight`,
   `etags[i]`, and `first_error` are all written under
   `bytes_lock`. Workers read `upload_id` (set once before any
   worker spawns) and write their own `etags[part_no - 1]` slot
   (unique per worker). Producer reads etags only after `await`
   returns.

4. **Drain on error.** If `push` or `flushPart` errors, we still
   need to abort the multipart upload and drain in-flight workers
   to free their owned buffers. `close()` semantics: in error
   paths, call `group.cancel(io)` and best-effort
   `AbortMultipartUpload`. Defer block in the lambda flow ensures
   `close()` always runs.

5. **Partial-state behaviour.** If `init` succeeds but `close` is
   never called (e.g. caller crashes mid-flow), the multipart
   upload is orphaned in S3. Lifecycle policies on the bucket
   eventually purge orphans (S3 standard: 7-day expiry on
   incomplete multipart uploads is the recommended config).
   ZPQ doesn't try to clean up orphans itself.

---

## Implementation plan

1. Build `src/io/multipart_sink.zig` standalone, with unit tests
   that hit `LocalFileSystem`-style behaviour (a memory-only sink
   trait for testing). Verify byte-aware backpressure, drain at
   close, single-PUT fallback on small inputs.

2. Refactor `s3.uploadMultipart` to use the sink. Existing tests
   continue to pass — that's the regression check.

3. Build `src/core/writer/streaming.zig` with a generic Sink
   parameter. Verify against a memory sink that the produced bytes
   exactly match what `fastpath.buildMulti` produces today (byte-
   copy path) and what the encoder produces (filter+encode path).

4. Migrate `buildFilteredOutputMulti` and the fastpath dispatch in
   lambda main to use the streaming writer.

5. Run all existing benches + `nested_roundtrip.sh` — outputs must
   validate three ways and match data semantics. Cold-start should
   not regress.

6. Add a "large output" benchmark that's deliberately bigger than
   would have fit pre-streaming, to demonstrate the unlock. Report
   it.

---

## Out of scope for B3

- Streaming INPUT (we still fetch full file_bufs before parsing
  metadata). Different problem.
- Backpressure-aware encoder (today's encoder is "encode the whole
  row group, return bytes"). Per-page streaming would let us push
  before a row group is fully encoded — likely overkill for our
  workload's row group sizes.
- Multi-file streaming output (one invocation writing N files at
  once, partitioned style). That's Phase C3 in the roadmap.
- Smart row-group coalescing into S3 parts. Today: fixed
  TARGET_PART_SIZE buffer, naturally coalesces small row groups.
  An adaptive scheme that picks part boundaries at row-group
  boundaries (when row groups are ≥ 5 MB each) might produce
  cleaner part numbering for debugging — minor improvement,
  later.
