# Parquet Writer + S3 Sink Design (Phase 5)

The contract for `src/io/s3_sink.zig` (or wherever the writer
ends up). Read this before writing or modifying writer code.

> **The previous writer attempt is what made the project lose
> shape.** This document leads with the failure modes so we don't
> repeat them. The "what we are NOT building" section is the most
> important part of this doc; treat it as a budget, not a wish list.

## What we are NOT building (yet)

The pre-rewrite writer/sink layer was ~1,300 LoC across ten files.
For comparison, our entire current filter system (AST + parser +
encoded comparator + prune + selection vector + eval) is ~900 LoC.
A single feature with more code than the entire filter pipeline is
a clear signal of premature abstraction.

Specifically rejected:

| Pre-rewrite artifact | Rejected because |
|---|---|
| `Sink` vtable with three impls (`s3`, `local`, `memory`) | We have one production sink (S3). Build it directly. Add an interface only when the second concrete impl actually exists. |
| `factory.zig` (110 LoC) | Implies enough variation to need dispatch. Today there isn't. |
| `sink/morsel.zig` + `sink/channel.zig` + `sink/task.zig` triad | Three abstractions for "send a unit of work to a worker." Premature parallelism scaffolding. The whole point of phase 5.1 is to ship the writer *without* parallelism, then prove what to parallelize from real numbers. |
| `sink/writer.zig` separate from `core/writer.zig` separate from `s3/sink_writer.zig` | Three writer abstractions. Pick one location. |
| 495-line `core/writer.zig` Parquet writer | Phase 5 deliberately skips per-row decode/encode. The fast path doesn't need it. |

The pre-reset code wasn't bad code — each file is reasonable in
isolation. The problem was the composition: too many seams, too
many shared abstractions, too many concrete impls behind interfaces
that didn't earn their cost.

## Provenance — what we're keeping in mind

- `docs/tier_3_s3_pipeline_strategy.md` — the comprehensive S3-to-S3
  plan that survived the v2 reset. Conductor model, prefetching,
  range coalescing, multipart upload sizing. Most of the macro
  design lives here; this doc fills in the writer-specific bits.
- Apache Arrow `ArrowWriter` (Rust) — buffers entire row groups
  before flushing; this constraint is intrinsic to Parquet's
  columnar layout.
- `AsyncArrowWriter` — eagerly streams to S3 during row-group
  assembly. Same shape we want eventually.
- DuckDB `COPY TO` — same row-group buffering, multipart sizing
  heuristic of "row group ≥ 8 MB → its own part."

## Decisions

### 1. Fast path is the primary path

The project's actual differentiator (per Tier-1 mission, per
tier_3 strategy doc) is "looks like `cp` for the SELECT * case."
Our Phase 5.1 ships this and only this. Specifically:

- Input: `s3://bucket-in/file.parquet` plus optional filter
  (whole-row-group prunable only — see scope below).
- Output: `s3://bucket-out/key`. Surviving row groups copied
  byte-for-byte. Footer rewritten with shifted offsets.

No decode. No encode. No buffer of decoded values. The bytes
fetched from S3 input flow through to S3 output, with a fresh
metadata footer at the end.

Per-row filtering (where we keep some rows in a row group, drop
others) requires full decode/encode and lives in a later phase.

### 2. Single-request PUT first, multipart later

S3 supports two upload modes:

- **PUT** — one HTTP request, body up to 5 GB. Simple. No state.
- **Multipart** — initiate / upload N parts in parallel / complete.
  Required for >5 GB. Useful below that for parallelism.

Phase 5.1 ships **PUT only**. Reasoning:

- 99% of our test fixtures (and most realistic Lambda workloads
  bounded by 6 GB ephemeral storage) fit in a single PUT.
- Sequential PUT is one HTTP round-trip. Parallel multipart with
  4 parts is also four round-trips, just to different parts.
  The latter only wins when single-PUT throughput saturates
  before parallelism limits do — a Lambda-specific question we
  haven't measured.
- Adding multipart later is purely additive; we don't rebuild
  the writer.

Phase 5.2 adds multipart sequentially. Phase 5.3 parallelizes
parts via the loop-driven async work we deferred from Phase 3.D.

### 3. Output file layout

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

Concretely, if input has row groups 0..3 each ~38 MB, and the
filter keeps row groups 1, 2:

```
output_offset = 4                       # past leading PAR1 magic
for rg in [rg1, rg2]:
    new_rg.columns[*].data_page_offset = output_offset + (col.data_page_offset - rg_start_in_input)
    output_offset += rg.total_byte_size
```

The exact arithmetic is "shift each per-chunk offset by the
delta between input row-group-start and output row-group-start."
We never re-encode anything; we just renumber pointers.

### 4. Memory model

PUT requires the full body in one HTTP request. So Phase 5.1
buffers the entire output in memory before issuing the PUT.

For our 155 MB benchmark fixture:
- Worst case (no filter): 155 MB output → 155 MB allocation
- Filtered case (1 row group): ~38 MB output → 38 MB allocation
- Lambda has 2 GB+ memory; this is fine

Phase 5.2 streams to multipart parts (≥5 MB each), so the
in-flight buffer is ~ part-size, not file-size. That's the moment
we'll need it.

### 5. File layout

Single file, ~400 LoC max:

```
src/io/s3_sink.zig
  pub const Writer = struct {
      // Lifecycle
      pub fn init(arena, s3_client, key) !Writer;
      pub fn writeFastPath(input_meta, input_bytes, surviving_rgs) !void;
      pub fn finish() !void;
      pub fn deinit();
      // ... internal helpers
  };
```

No `Sink` vtable. No factory. No separate `sink/morsel.zig`.
When a second concrete sink (local file? in-memory for tests?)
materializes, **then** we extract an interface. Not before.

### 6. What "got out of hand" means and how we avoid it

Three rules:

**Rule A: One concrete impl before any interface.** If we end
up with `Sink` as a vtable behind two concrete types, fine. If
we ship one concrete type with an interface "for future use,"
that's the bloat trap.

**Rule B: One file per real responsibility, not per category.**
`s3_sink.zig` does S3 sink work. If it grows past ~500 LoC,
*then* split — and only at a real seam (e.g., "footer assembly"
vs "S3 upload"). Don't pre-split into `sink/`, `s3/sink/`, etc.

**Rule C: No parallelism scaffolding until a benchmark says so.**
Channel + morsel + task is three abstractions for "send work to
a worker." If we add it before measuring sequential cost, we
don't know whether it actually pays off — the v1 result was
1300 LoC of plumbing for a perf win we hadn't validated.

## Phase plan

| Phase | What | Out of scope |
|---|---|---|
| 5.1 | Single-PUT fast-path writer. Filter prunes row groups; surviving ones copied byte-for-byte; footer rewritten. End-to-end S3-to-S3 working in Lambda. | Multipart, parallel parts, per-row filter, encoder |
| 5.2 | Sequential multipart for files >5 MB output. Threshold-based switch. | Parallel parts, per-row filter |
| 5.3 | Parallel multipart parts via the in-tree epoll Loop. *This* is where we cash in the parallel-Loop work. | Per-row filter |
| 5.4 | Decoder + encoder pipeline for per-row filtering and column projection. | (Eventually: writer-side compression strategy heuristics, dict-rebuild thresholds, etc.) |

Phase 5.1 is small and ships the headline product. Phase 5.4 is
big and we don't start it until 5.1–5.3 are battle-tested.

## What this rejects

- **Local file sink.** Not a real workload for ZPQ; deploy targets
  are Lambda + (eventually) container/CLI. CLI can use the same
  S3 path against MinIO or local S3-compatible. Don't add a
  separate code path.
- **In-memory sink.** Tests use raw bytes from the integration
  test harness; no need for a vtable.
- **Channel-based work-stealing.** When we go parallel, the
  pattern is N concurrent multipart PUTs from the same Loop, not
  workers consuming a queue. Different shape from the old design.
- **Encoder before benchmarks demand it.** The fast path covers
  any workload where rows aren't filtered out within a row group
  (which is most ETL-style copies and most simple filters). When
  a real workload demonstrates we're losing on per-row filters,
  ship 5.4.

## Test plan

### 5.1
- Unit: synthetic input bytes + filter that prunes 0/some/all row
  groups. Output buffer assembled correctly: check leading magic,
  trailing magic, parseable footer.
- Roundtrip: write → read back via metadata.open + ColumnChunkReader,
  verify decoded values match original input's surviving row groups.
- Integration: real Lambda invocation. Input the benchmark file,
  filter `int8>0`, write to a fresh S3 key, verify the output is
  a valid Parquet file via a follow-up GET + decode.

### 5.2 / 5.3
- Same shape, plus assertions about part count + size.

### 5.4 (when we get there)
- Property test: for each (input, filter) pair, decoded output
  equals the input rows where the filter holds. Compare against
  PyArrow / DuckDB for cross-validation.

## Anti-patterns checklist (from `tier_3_s3_pipeline_strategy.md`)

The surviving design doc already lists what to avoid. Re-quoting
because it's load-bearing:

1. ~~Workers calling loop.run()~~ — N/A in 5.1 (no workers).
2. **Serial S3 requests** — explicitly accepted in 5.1 as the
   simplest correct version. Phase 5.3 fixes this.
3. **Buffering entire file before writing** — yes in 5.1
   (intentional, file fits in RAM). 5.2 fixes via multipart.
4. **Single connection for all reads** — already addressed by the
   keep-alive S3 client.
5. **Ignoring column statistics** — already addressed by filter
   pushdown.
6. **Starting multipart unconditionally** — addressed by the
   threshold switch in 5.2.
