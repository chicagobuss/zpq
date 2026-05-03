# Parquet Decode Design

The contract for `src/core/parquet/`. Read this before writing or
modifying decoder code.

## Provenance

Studied before writing:

- **polars-parquet** (`references/polars/crates/polars-parquet/`).
  Production-grade Rust decoder. One module per Parquet encoding;
  `MutStreamingIterator` chains pages → values; layered as
  `parquet/` (bytes) → `arrow/` (output format).
- **hardwood-core** (`references/hardwood/core/`). Java Parquet
  reader, minimal-dep, batch-into-buffer style. `ValueDecoder`
  interface declares `readLongs(long[] output, int[] defLevels,
  int maxDefLevel)` — type-specific batch APIs writing directly
  into pre-allocated primitive arrays. Splits flat (`FlatRowReader`)
  from nested (`NestedRowReader`/Dremel) to keep the easy case fast.
- **Vortex** (SpiralDB, `github.com/spiraldb/vortex`). New columnar
  format positioned as a Parquet successor. Notable for explicit
  *logical/physical separation*, *cascading encodings*, and
  lazy-loaded summary statistics. We're staying Parquet-native, so
  the cascade idea doesn't apply directly, but the logical/physical
  split is the right mental model.

Three takeaways shape this design:

1. **Batch decode into a caller-provided buffer is the fast path.**
   Both Polars and Hardwood end up there; Polars even comments that
   their iterator API "should not be preferred" over the batch
   collect methods. Iterator-style decode is too much per-call
   overhead for our workload. We skip iterators entirely.
2. **One module per Parquet encoding.** Both projects do this. It
   matches the Parquet spec layout; testing is scoped per-encoding;
   future work to add DELTA_* / BYTE_STREAM_SPLIT lands as new
   modules without touching existing decoders.
3. **Logical type ⊥ physical encoding.** A column has a logical
   type (`INT32`, `BYTE_ARRAY`, etc.) and a per-page encoding tag
   (`PLAIN`, `RLE_DICTIONARY`, `HYBRID_RLE`, ...). The
   ColumnChunkReader knows the logical type up-front, picks the
   right Decoder(T) per page based on the encoding tag, and
   re-targets the same output buffer as it walks pages.

## Decisions

### 1. Module layout

```
src/core/
  schema.zig               (already exists — Parquet logical schema)
  thrift.zig               (already exists — Compact Protocol parser)
  parquet/
    metadata.zig           Footer parser; uses thrift.zig.
    page.zig               Page header parsing + decompression dispatch.
    column.zig             ColumnChunkReader — orchestrates pages, picks decoders.
    encoding/
      plain.zig            PLAIN: raw little-endian values.
      hybrid_rle.zig       Definition/repetition levels and dictionary indices.
      rle_dict.zig         Dictionary-encoded values via hybrid_rle indices.
      delta_binary_packed.zig    Phase 2.
      delta_byte_array.zig       Phase 2.
      delta_length_byte_array.zig Phase 2.
      byte_stream_split.zig      Phase 2.
    compression.zig        Codec dispatch (Snappy, Zstd in Phase 1; LZ4/GZIP later).
```

### 2. Decoder contract

A decoder is a value type (not heap-allocated) that wraps a slice of
encoded bytes and walks them in batches:

```zig
pub fn Decoder(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Construct over an encoded slice. Cheap; no allocation.
        pub fn init(encoded: []const u8) Self;

        /// Decode up to dest.len values into dest. Returns number
        /// of values written; 0 means the encoded stream is
        /// exhausted. Errors only on malformed input.
        pub fn decode(self: *Self, dest: []T) Error!usize;

        /// Number of values not yet decoded. -1 if unknown
        /// (some encodings don't track this cheaply).
        pub fn remaining(self: *const Self) i64;
    };
}
```

The function writes *into* the caller's buffer. No allocator
parameter, no return slice, no iterator. Calling code:

```zig
var dec = Decoder(i64).init(page_bytes);
var out: [4096]i64 = undefined;
while (true) {
    const n = try dec.decode(&out);
    if (n == 0) break;
    // ... use out[0..n] ...
}
```

This is Hardwood's idiom. It's the right shape for SIMD (the inner
loop is `for i in 0..n: out[i] = decode_one(...)`) and for filter
pushdown (the predicate runs over the buffer in a separate pass,
and a "no rows match" outcome means we drop the buffer without
materializing further).

### 3. Logical/physical separation

`ColumnChunkReader` is parameterized over the *logical* type T.
It walks pages, reads each page's encoding tag, and dispatches:

```zig
pub fn ColumnChunkReader(comptime T: type) type {
    return struct {
        // Per-page state:
        switch (page.encoding) {
            .PLAIN => decode via Plain(T),
            .RLE_DICTIONARY => decode via RleDict(T) referencing the dictionary page,
            .HYBRID_RLE => decode via HybridRle to indices, then dictionary lookup,
            // ... DELTA_* added in Phase 2 ...
        }
    };
}
```

The dispatch is at *page granularity*, not per-value. Once a page's
decoder is selected, the inner loop is monomorphic and can SIMD.

### 4. Memory model

Three lifecycle layers:

- **Encoded bytes** — owned by the I/O layer (the column chunk
  buffer). Sliced into the decoder. Lifetime: the duration of the
  column chunk read.
- **Decompressed page bytes** — allocated from a per-row-group
  arena. Lifetime: the row group. Snappy/Zstd output goes here.
- **Decoded values** — written into a caller-provided buffer
  (typically arena-allocated by the row-group pipeline). Lifetime:
  the row group, often shorter (post-filter materialization).

No allocator is involved during decode itself. The arena boundaries
are at row-group entry/exit, which matches the rest of the project's
"arena per row group" tier-1 guidance.

### 5. Definition/repetition levels (Phase 1: flat only)

Phase 1 supports **flat, required columns only** — no nullables, no
nested. This matches what `data/benchmark/benchmark_100mb.parquet`
uses for its hot columns. Specifically we skip:

- Definition levels (no nulls in the schema).
- Repetition levels (no nested).
- Dremel record assembly.

When we add nullables, the decoder API extends with an
explicit `def_levels: ?[]const u16` parameter (Hardwood-style); a
null-mask is a separate pass after decode.

When we add nested, that's a separate path with its own
`NestedColumnChunkReader` (Hardwood splits these for a reason —
the flat case is *much* faster). Don't unify until you understand
both.

### 6. Phase 1 encoding surface

Implement only what's needed for the benchmark file's hot columns:

| Encoding | Status | Used for |
|---|---|---|
| PLAIN | Phase 1 | Every Parquet type. |
| HYBRID_RLE | Phase 1 | Def/rep levels (when we add them); dict indices. |
| RLE_DICTIONARY | Phase 1 | Low-cardinality columns. |
| DELTA_BINARY_PACKED | Phase 2 | Modern integer columns from Spark/PyArrow. |
| DELTA_BYTE_ARRAY | Phase 2 | Modern string columns. |
| DELTA_LENGTH_BYTE_ARRAY | Phase 2 | Modern string columns. |
| BYTE_STREAM_SPLIT | Phase 2 | Float/double perf encoding. |
| PLAIN_DICTIONARY (legacy) | Skip | Treat as RLE_DICTIONARY equivalent. |

Compression: Snappy in Phase 1 — that's what our benchmark fixture
uses end-to-end. Zstd, GZIP, LZ4_RAW deferred to Phase 2. Stdlib has
`std.compress.zstd.Decompress` + `std.compress.flate.Decompress`,
both `*Reader`-based — they'll wrap nicely once we build the rest of
the pipeline; we just don't need them yet.

### 7. SIMD: opportunistic, not foundational

Both Polars and Hardwood end up with type-specific scalar fast paths
*and* SIMD overlays. Phase 1 is scalar only — write the simplest
correct decoder, benchmark, then add SIMD where the profiler points.

The decoder API is already SIMD-friendly (batch into a slice), so
adding SIMD later is a body-of-function change, not an API change.

Use `@Vector` types where they help; don't reach for inline assembly
or `std.simd` until profiling demands it.

### 8. Filter pushdown (deferred)

Phase 1 has no encoded-domain predicate evaluation. Filter is a
materialized pass over the decoded buffer:

```zig
const n = try decoder.decode(out_buf);
const keep = filter.evaluate(out_buf[0..n], pred);  // bitmask
// downstream sink reads only out_buf[i] where keep[i]
```

Phase 2 adds *encoded-domain* predicates for HYBRID_RLE and
RLE_DICTIONARY:
- For RLE runs: evaluate the predicate against the run's value
  once, propagate the result to the entire run.
- For dictionary-encoded columns: evaluate against the dictionary,
  produce a mask of matching dictionary entries, apply via index
  lookup.

These are big perf wins on selective queries. Defer until the basic
pipeline is producing correct output; they're optimizations, not
correctness.

### 9. Row-group statistics pruning

Independent of decoding. The metadata reader exposes per-row-group
min/max/null-count. A planner pass before any I/O eliminates row
groups whose stats can't satisfy the predicate. Lives in
`src/core/parquet/metadata.zig` (data) + the planner (decision).

This is the *cheapest* perf win in the whole pipeline. Cover it in
Phase 1 as part of metadata work.

## API surface (final)

```zig
// metadata.zig
pub const FileMetadata = struct { ... };
pub fn readFileMetadata(reader: anytype) !FileMetadata;

// page.zig
pub const PageHeader = struct { ... };
pub const Page = struct {
    header: PageHeader,
    bytes: []const u8,  // decompressed
};
pub fn readPage(reader: anytype, arena: std.mem.Allocator) !Page;

// column.zig
pub fn ColumnChunkReader(comptime T: type) type {
    return struct {
        pub fn init(reader: anytype, chunk_meta: ChunkMetadata) !@This();
        pub fn decode(self: *@This(), dest: []T) !usize;
    };
}

// encoding/plain.zig, hybrid_rle.zig, rle_dict.zig
pub fn Plain(comptime T: type) type { ... }
pub const HybridRle = struct { ... };          // always u32 indices
pub fn RleDict(comptime T: type) type { ... }; // composes HybridRle + dictionary
```

## What this design rejects

- **Iterator-style decoders.** Both projects we studied have them
  and both warn against using them. We just don't ship them.
- **Async decoders.** Decode is CPU-bound, not I/O-bound. The
  pipeline above (row-group prefetch) handles overlap.
- **Heap allocation per page.** Page decompression goes through an
  arena. Decoders never allocate.
- **Generic-over-encoding type erasure.** A `*dyn Decoder` style
  vtable would dispatch per-value; that's the slow path. Encoding
  selection happens at page boundary; per-value code is monomorphic.
- **Nested types in Phase 1.** Dremel record assembly is a separate
  problem. Solve flat first, ship it, then come back.
- **Cascading encodings.** Vortex's idea, but Parquet's spec doesn't
  allow nesting encodings. If we ever ship our own format, revisit.

## What this design defers

- DELTA_* and BYTE_STREAM_SPLIT encodings (Phase 2).
- LZ4_RAW and GZIP compression (Phase 2).
- Definition/repetition level handling (when nullables matter).
- Nested types / Dremel (when struct/list columns matter).
- Encoded-domain predicate pushdown (Phase 2 perf work).
- SIMD-tuned hot paths (when profiling demands).

## Test plan

For each encoding module:
1. **Round-trip tests** — for the encodings we *write* (Phase 1
   eventually), encode a known input, decode, assert equality.
   For decode-only encodings (DELTA_*), use vendor-generated
   fixtures.
2. **Spec-conformance tests** — feed the decoder bytes from the
   `parquet-testing` corpus (referenced in the project; we'll
   re-introduce a small subset of relevant files under
   `data/encoding_fixtures/`).
3. **Boundary cases** — empty input, one value, exactly one bit-
   packed run-length boundary, Snappy/Zstd headers without bodies.

For the column reader:
1. **Single-page chunk** — one PLAIN page, walk to completion.
2. **Multi-page chunk** — three PLAIN pages, walk across boundaries.
3. **Mixed encodings** — first page PLAIN, subsequent pages
   RLE_DICTIONARY referencing a dictionary page.

Integration test (when `core/parquet` is wired into the Lambda
binary): `data/benchmark/benchmark_100mb.parquet` decodes to the
same row count + column-sum signatures as PyArrow.

## What this means for the v2 rewrite

This decoder layer is the project's actual value prop — everything
else (S3, sink, Lambda runtime API, the event loop) is plumbing.
The plan:

1. Land `metadata.zig` + the row-group stats pruner. Smallest
   useful slice; tests against the benchmark file.
2. Land `page.zig` + Snappy decompression. Add Zstd alongside.
3. Land `encoding/plain.zig` for INT32, INT64, FLOAT, DOUBLE,
   FIXED_LEN_BYTE_ARRAY, BYTE_ARRAY. Tests per type.
4. Land `encoding/hybrid_rle.zig`. RLE/bit-pack hybrid is the
   single most-touched code path (def levels + dict indices),
   so spend time on correctness here.
5. Land `encoding/rle_dict.zig` composing the above.
6. Land `column.zig` orchestrating pages + dispatch. Multi-encoding
   chunk tests.
7. Wire into the Lambda binary's handler — first real end-to-end
   test through Parquet decode in production.

Each step is a separate commit. Each commit has tests. The
integration test fake from the previous commit catches Lambda-side
regressions automatically.
