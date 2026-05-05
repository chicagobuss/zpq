# ZPQ Roadmap

**Ordering principle: rewrite-risk first.** Items most likely to force
the rest of the codebase to change land earliest. Items that add to a
stable foundation come later. The painful version of this discipline
failing looks like "we shipped a snappy output encoder, then six weeks
later landed streaming output and rewrote the snappy encoder." We
avoid that by sequencing.

Each phase below states **what would break if you did the next phase
first**. That's the test for "is this in the right order?"

---

## Phase A — Conformance & Honesty (no rewrite risk; blocks nothing)

These are tooling and information-gathering tasks. They surface
*unknown* bugs and quantify *known* gaps. Without them we'd be
designing later phases against a hypothetical correctness baseline.

- **A1. Run `apache/parquet-testing` corpus through ZPQ decode.**
  Every public file in that repo is a known-good parquet conformance
  case. For each file: open, decode every column, compare against
  pyarrow. Build a pass/fail matrix; don't try to fix anything yet —
  just *know what we don't know*.
- **A2. Make the bench script validate every output with pyarrow +
  duckdb.** The wire-correctness bug we shipped for months is
  evidence that "bytes_out > 0" is not the same as "valid parquet."
  Add this guard now, before we generate more outputs.
- **A3. CI enforces A1 + A2.** Lock in the floor.

**What breaks if we skip A and do something else first?** We design
features against incorrect assumptions about what currently works.
The 5.4c work this afternoon would have been better-scoped if we'd
known about the i8/I32 wire bug going in.

---

## Phase B — Structural Foundations (high rewrite risk)

Anything in this phase touches the decode/filter/encode primitives
themselves. If we land features on top of the current shape and then
pivot here, we redo those features.

- **B1. Nested types (struct / list / map).** Today's everything is
  flat: `max_def ≤ 1`, `max_rep == 0`, schema is `[root + N leaves]`.
  Real parquet routinely has struct columns and list columns.
  Tonight (2026-05-04) shipped two slices but uncovered a deeper
  layer of work that wasn't in the original sizing — the user-facing
  projection layer is broken for nested schemas in ways that affect
  even byte-copy passthrough.

  **B1 SHIPPED 2026-05-04 (single late-evening session).**

  - **B1.a — Struct-of-primitive decode** (shipped). Decoder uses
    full `path_in_schema`; STRUCT<primitive> decodes correctly.
  - **B1.b — Rep-level decode** (shipped). LIST/MAP leaf-column
    decoding via parallel `(values, def_levels, rep_levels)`;
    conformance corpus reaches 45/70 (63%).
  - **B1.0 — SchemaTree module** (shipped). `core/parquet/schema_tree.zig`:
    tagged-union tree of `PrimitiveNode | GroupNode`, single-DFS
    builder, O(1) `path_to_leaf` lookup, `resolveTopLevel`,
    `projectSubset`, `writeFlatThrift`, `format`. Built once at
    `metadata.open` time, lives on the file's arena.
    Tree-builds successfully on every readable file in the
    parquet-testing corpus; leaf counts match pyarrow's
    `metadata.num_columns` oracle exactly.
  - **B1.x — Column resolution** (shipped). Lambda projection
    path uses `tree.resolveTopLevel(name)` returning a leaf-index
    set. User-facing "events" correctly resolves to its multiple
    leaves (e.g., `events.list.element.ts` + `events.list.element.code`).
  - **B1.y — Schema projection** (shipped). `cloneProjectedSchema`
    replaced by `tree.projectSubset` + `tree.writeFlatThrift`.
    GROUP ancestors and LIST/MAP annotations preserved through
    filter+encode; downstream readers reassemble nested values.
  - **B1.z — Dispatch** (shipped). When projection includes nested
    columns and there's no filter, route through the encode
    pipeline with a tautology filter so rep-level emission happens.
    Tautology hack is logged for cleanup (next refactor: make
    `buildFilteredOutputMulti` accept `?filter`).

  **Validated end-to-end:**
  - 8/8 scenarios in `benchmarks/nested_roundtrip.sh` pass.
    Each output checked by pyarrow `read_metadata`/`read_schema`,
    duckdb `read_parquet`, hardwood `info`, AND a python oracle
    that re-runs the same filter via pyarrow.compute and compares
    row counts + per-column value sequences.
  - LIST<STRING>, STRUCT, LIST<STRUCT>, MAP<STRING, INT32> all
    survive filter+encode round-trips with empty lists, null lists,
    internal-null lists, and unicode strings preserved.

  **Outstanding (deferred to B1.w / Phase E):**
  - Rep-aware filter eval — list-element predicates,
    `array_contains`, nested struct field access. The filter parser
    still uses legacy `findColumnIndex` (correct for flat top-level
    columns; the eval already rejects `max_rep > 0` columns cleanly).
  - Filter parser tree-awareness migration when adding the above.
  - Cleanup of the tautology-filter dispatch hack — make
    `buildFilteredOutputMulti` accept `?filter` and skip eval when
    null.
- **B2. DATA_PAGE_V2** (shipped 2026-05-04). `DataPageHeaderV2`
  added to `schema.zig` (field 8 in PageHeader); `page.zig::next`
  splits V2 payloads into `[rep][def][values]` with selective
  decompression of the values portion only; `column.zig::install
  DataPageV2Decoder` handles level-lengths from the header instead
  of inline `<u32 LE>` prefixes. Conformance corpus 63% → 73%.
  Cold start unchanged. Outstanding edge case: V2 BOOLEAN with
  SNAPPY (col[3] of `datapage_v2.snappy.parquet`) hits an
  UnsupportedEncoding in the boolean path — small follow-up.
- **B3. Streaming output** (shipped 2026-05-05). Five commits on
  `b3-streaming`:
  1. `io.multipart_sink.MultipartSink` primitive — DuckDB-style
     bounded concurrency × Vortex-style byte-aware permits, expressed
     in Zig 0.16's `Io.Mutex`/`Io.Condition`/`Io.Group`.
  2. `s3.uploadMultipart` collapsed to a 3-line wrapper over the
     sink (-180 lines of duplicated S3 multipart machinery).
  3. `core.writer.streaming` — generic-Sink counterpart of
     `fastpath.buildMulti`. Tests assert byte-for-byte equivalence
     with the buffered version across (no-projection, projection,
     N=10 multi-file) including a 1.55 GB streamed output.
  4. Lambda fastpath case (no filter, no nested projection) routed
     through `MultipartSink`.
  5. Encoder path (`buildFilteredOutputMulti`) likewise; lambda main
     flow collapsed to a single sink-construction site.
     `buildFilteredOutput` and `cloneProjectedSchema` deleted as
     dead code (-340 lines, +73 lines net in lambda/main.zig).

  **Memory unlock**: working set is O(footer + one in-flight chunk
  inside the sink), independent of total output size. A 512-MB
  Lambda can produce a multi-GB Parquet without OOM.

  **Validated 2026-05-05** against real S3 (us-west-2, x86_64,
  5120 MB):
  - 8/8 nested round-trip scenarios (pyarrow + duckdb + hardwood +
    python oracle).
  - run_partition.sh head-to-head, median of 3:
      `zpq:partprune  836 ms` (vs polars 1106, duckdb 1374)
      `zpq:copyall   1675 ms` (vs polars 1947, duckdb 2844)
    Both ~5% faster than pre-streaming-encoder; the intermediate
    `[]u8` and its second copy are gone.
  - Cold start: 10 ms init / 1752 ms total. No regression.

  **Outstanding (B3 follow-ups, deferred):**
  - Tautology-filter dispatch hack (B1.z) still needs cleaning up
    via a `?filter` parameter on `buildFilteredOutputMulti`.
- **B4. Streaming input** (shipped 2026-05-05). Four commits on
  `b4-input-streaming`:
  1. `lambda/scan.zig` per-file RG iterator. `PerFileScan.next()`
     fetches one row group's column-chunk bytes (with caller-owned
     buffer transfer for queue-based hand-off). `FetchPolicy` is a
     tagged union: `.all_kept` (whole RG span, fastpath) or
     `.columns = [...]` (per-leaf, encoder/projection).
  2. Encoder path migrated to consume from the iterator. Per-RG
     bytes fetched on-demand instead of up-front.
  3. Cross-file `Io.Group` orchestrator. **Single decoder, parallel
     fetchers** — N fetcher workers feed per-file `Io.Queue` of
     capacity 1; main task drains queues in file order and runs the
     existing per-RG processing serially. CPU-bound encode stays
     single-threaded (no concurrent arena races); only I/O fan-out
     parallelizes. Probe-confirmed: strict per-file serialization
     regressed 39-81%; this restores parity.
  4. Fastpath migrated to the same iterator + orchestrator. The
     upfront `s3.fetchJobs` call is gone. `bytes_fetched=0` always
     in the JSON envelope. `buildOutputMulti` is the single
     orchestrator; `encodeOneRG` and `copyOneRG` are the two per-RG
     consumers it dispatches to.

  **Memory unlock complete.** Working set is now
  O(N_files × QUEUE_CAP × max_RG_compressed) — for our 10-file
  test fixture that's ~140 MB plus per-RG decoded values
  (~50–150 MB during encode), total ~200–300 MB. ZPQ on a 512 MB
  Lambda can stream a 50 GB Parquet through. Practical input limit
  is now the 15-min Lambda wall clock and S3 5 TB max object size,
  not memory.

  **Validated 2026-05-05** against real S3 (us-west-2, x86_64,
  5120 MB):
  - 8/8 nested round-trip scenarios pass through the unified
    encoder + fastpath streaming-input path.
  - run_partition.sh head-to-head, median of 5:
      `zpq:partprune  887 ms` (B3 baseline 836; +6%)
      `zpq:copyall   1702 ms` (B3 baseline 1675; +2%)
    Slight regression is the cost of per-RG fetches losing some
    coalescing efficiency vs one big upfront fetchJobs. Operating
    envelope expanded by 100×+ in exchange.
  - Encoder-heavy bench (`int8 >= 0` filter on 10-file fixture,
    warm runs): 4026 ms (sequential per-file) → 1943 ms (cross-file
    parallel). 2× speedup from restoring fan-out.
  - Cold start: 11 ms init (unchanged from B3).

  **Outstanding (B4 follow-ups, deferred):**
  - `sp.file_buf` is still allocated at `total_size` in
    `fetchMetaTask`. Only metadata regions get written; Linux
    demand-paging keeps resident memory small. Tightening this to
    a metadata-only buffer is a separate commit.
  - `core/writer/streaming.build` (the in-memory FileSpec-driven
    path) is no longer called by lambda but still exists for tests.
    Could be removed if we migrate its tests to drive
    `scan.PerFileScan` through a memory-backed source.

**What breaks if we skip B and do D (codecs) first?**
- A snappy output encoder built for the current "encode whole file
  to memory" model needs to be re-fit when streaming arrives.
- Dictionary encoding writer designed for flat columns has to be
  generalised when nested columns land.
- Bloom filter footer-write is footer-shaped today; with streaming
  the footer is the *last* part, not appended to a single buffer —
  the write path differs.

---

## Phase C — Compute Primitives (medium rewrite risk; absorbs old E)

Once input + output streaming are done, ZPQ becomes capable of doing
real per-RG compute work without touching its memory ceiling. This
phase generalizes "filter and re-encode" into a small but coherent
compute layer: expressions (transformations), aggregate kernels, and
group-by. The existing filter surface (`Phase E` in earlier
revisions of this doc) folds in here as "expression operator
coverage."

**Strategic frame:** ZPQ is not becoming DuckDB. The target is
"simple filters + transformations + simple aggregations within a
file" as the user-facing capability matrix. Joins, window functions,
complex SQL surface, and full optimizer machinery stay out of scope.

- **C1. Batch-iterator-as-primitive.** Refactor the lambda's
  filter/encode and fastpath consumers to read from the B4 scan
  iterator's `{ raw_bytes, decoded_batch }` per-RG output. Lifts
  the current ad-hoc "lambda owns the loop" shape into an explicit
  scan→consumer protocol. Light, no new features. ~150 LoC of
  reorganization. Lands the moment B4 ships.
- **C2. Expression evaluator.** A small typed-expression AST and
  evaluator for transformations like `col_a * 2 + col_b`,
  `coalesce(x, 0)`, `case when x > 10 then 'high' else 'low' end`.
  Hand-coded kernels per operator-type combination — no generic
  comptime VM, no LLVM, no plan optimization. Output of C2 is a
  new `Batch.Column` per row, fed back into the encoder.
- **C3. Filter operator coverage.** Folded from old Phase E:
  - **C3.a `IS NULL` / `IS NOT NULL`** — uses def_levels we
    already produce.
  - **C3.b `NOT`** — unary negation.
  - **C3.c parentheses** — parser only; precedence already exists.
  - **C3.d `IN (...)`** — sugar for OR-of-equalities, or its own
    leaf with hash-set lookup if the list is large.
  - **C3.e `LIKE`** — pattern match with `%` / `_` and escape.
  Each lands independently in any order. C2 makes some of these
  trivial because the evaluator already handles the typing
  machinery.
- **C4. Aggregate kernels (no group-by).** `sum`, `count`,
  `min`, `max`, `avg` over a column or expression. Single-pass over
  decoded batches; final flush at end. Output is one row.
- **C5. Group-by, in-memory only.** Hash-table keyed by group-by
  expressions, valued by accumulator state. Memory ceiling becomes
  `O(num_groups × num_aggregates)` — for high-cardinality keys this
  can blow up. v1: fail loud above a configurable threshold (e.g.
  10M groups). Spilling to `/tmp` (Lambda has 10 GB) is a future
  C6.
- **C6. Group-by spilling.** Same model as DuckDB / Polars: when
  the in-memory hash table exceeds a budget, spill partitions to
  `/tmp` and merge during finalization. Real engineering project;
  defer until a workload demands it.

**What breaks if we skip C and do D (codecs) first?**
- D5/D6 (DELTA writers) make the most sense when there's an
  expression-level "is this column sorted / correlated?" check,
  which lives naturally in C2's evaluator infrastructure.
- D7 (bloom filter writer) wants to know which columns to build
  bloom filters for — a hint that comes from C-layer analysis.

---

## Phase D — Architectural Variants (medium rewrite risk; localised)

Things that change *one* major subsystem but don't ripple. Order
within this phase is flexible; B1 must precede D2.

- **D1. Cross-bucket inputs / outputs.** Pool is single-bucket today
  by simplifying assumption. Most real data platforms separate read
  and write buckets. Either: (a) one pool per bucket, lazy-init; or
  (b) a single pool that knows N hosts. (a) is simpler and probably
  fine for ≤3 buckets per invocation. Independent of B; could land
  any time, but better before HTTP/2 multiplex (H2) so the multi-host
  model influences the connection design.
- **D2. Schema evolution across files.** Today: identical schemas
  required, error otherwise. Realistic: column added month over
  month. Output schema = union; missing column in older file = all
  nulls in that file's contribution. Depends on B1 — the schema
  reconciliation logic is much cleaner once nested types are real.

**What breaks if we skip D2 and assume identical schemas through E
and beyond?** Filter binding logic codifies "lookup by index in
file 0"; when schema evolution lands the index→column_path mapping
has to be reworked. Filter AST that uses `col_idx` may need to switch
to column-path keys. Better to settle that before we accumulate more
expression features (C).

---

## Phase E — Codec & Encoding Breadth (low rewrite risk; additive)

Once the fundamental shapes are settled, we expand "what bytes ZPQ
can read and write." Each item is contained to encoder/decoder
modules.

- **E1. Snappy output encoder** (shipped 2026-05-05). Two commits
  on `e1-snappy-output`:
  1. Wire snappy compression into the encoder; switch
     `ColumnMetaData.codec` from UNCOMPRESSED to SNAPPY. Caught and
     fixed a critical bug in our hand-rolled compress where Zig 0.16
     result-location semantics narrowed `((copy_len-1)<<2)|2` to u8
     before the shift, dropping the high bit for copy_len > 32 and
     producing snappy output our own decoder accepted but pyarrow
     refused as "Corrupt snappy compressed data."
  2. Vendor google/snappy 1.2.1 source under `vendor/snappy/`,
     replace `compressAlloc` body with FFI to the C++ implementation.
     Hand-rolled `compress` preserved as a reference / decode-side
     fallback. The C++ generic implementation hits ~250 MB/s vs our
     hand-rolled at ~75 MB/s.

  **Results** (`int8 >= 0` filter on 10-file fixture, warm runs):
    pre-E1 (uncompressed):                       2007 ms
    E1 hand-rolled snappy (ReleaseSmall):        2640 ms
    E1 hand-rolled snappy (ReleaseFast):         2215 ms
    E1 vendored google/snappy (ReleaseFast):     1818 ms

  Vendored snappy is **9% faster** than uncompressed despite writing
  72 MB instead of 85 MB. Closes 75% of the gap to Polars (1628 ms);
  remaining ~190 ms is in encode-loop / decode-loop CPU paths,
  not compression.

  Lambda binary: 23 MB → 24 MB (+1 MB for libstdc++ static linkage).
  Well within the 250 MB unzipped quota.

  **Outstanding (E1 follow-ups, deferred):**
  - Architecture-specific snappy paths (SSSE3, BMI2, NEON-CRC32) are
    disabled in our vendored build for portability. Re-enable per
    target if a workload demands it.
  - The hand-rolled zig snappy.compress is preserved but unused. Can
    be removed once we're confident in the vendored impl.
- **E2. Zstd input + output.** Whole new codec. zstd is increasingly
  common (better ratio than snappy at similar speed). Probably
  vendor `libzstd` like we vendor BoringSSL.
- **E3. Gzip input.** Older parquet files; Hadoop-era tooling.
- **E4. Dictionary encoding writer** (shipped 2026-05-05). Detects
  low-cardinality BYTE_ARRAY columns (≤ 25% unique among present
  values) and emits a two-page chunk: DICTIONARY_PAGE (PLAIN-encoded
  unique values) + DATA_PAGE (RLE_DICTIONARY-encoded indices). For
  higher cardinalities the build bails out early and falls through
  to PLAIN.

  Plumbing change: the encoder now sets `data_page_offset` and
  (new) `dictionary_page_offset` RELATIVE to chunk start; callers
  add the absolute offset. Single-page (PLAIN) chunks have offset
  zero and the addition is a no-op.

  Threshold tuning: started at 50%, found via the new instrumentation
  that build-side hashmap inserts on high-cardinality columns
  swamped the savings (encode_ms went 316 → 396). Tightened to 25%;
  encode_ms recovered to 271, BELOW pre-E4. The instrumentation paid
  for itself on the very first feature it watched.

  Results (`int8 >= 0` filter on 10-file fixture, median of 5):
    pre-E4 (vendored snappy):     1880 ms
    post-E4 (25% threshold):      1548 ms  (-18%)
  Bytes_out 72 MB → 71 MB (-1.5% — only 2/27 columns are dict-shaped).
  ZPQ now beats Polars (1634 ms) on this query.
- **E5. DELTA_BINARY_PACKED writer.** For sorted/timestamp columns.
- **E6. DELTA_BYTE_ARRAY writer.** For correlated string columns.
- **E7. Bloom filter writer.** Footer-level structure for
  high-cardinality predicate pushdown.
- **E8. Page index writer (offset_index + column_index).** We
  currently *drop* these on re-encode. Restoring them lets
  downstream readers do row-level pruning instead of falling back
  to row-group-level.
- **E9. `null_count` in column stats.** Strict pruners (parquet-mr)
  treat unset null_count as pessimistic. Free correctness win.

E1 first (biggest single win, smallest LoC). E7/E8/E9 are footer-
shape improvements that could land together.

---

## Phase F — Operability (low rewrite risk; mostly additive)

Things a real production deploy needs, none of which change the
engine itself.

- **F1. CLI binary feature parity.** `src/cli/main.zig` exists but
  isn't wired to anything useful. Make `zpq query --input s3://...
  --filter "x>10" --output s3://...` work locally and on workstations.
  Same engine, no Lambda runtime API.
- **F2. API versioning.** Add `version: "1"` to the request shape
  with a default. Document the JSON contract.
- **F3. Structured logs.** Replace `std.debug.print` usage in the
  Lambda binary with newline-delimited JSON via a small helper.
  CloudWatch Insights query-friendly.
- **F4. CloudWatch EMF metrics.** Per-invocation: rows_in, rows_out,
  bytes_in, bytes_out, fetch_ms, build_ms, put_ms — all as metric
  dimensions, not just JSON response fields.
- **F5. User-facing docs.** README at the top level explaining what
  ZPQ is and isn't. `docs/USAGE.md` walking through "deploy ZPQ to
  your account in five steps." JSON schema for the API.
- **F6. SAM / CDK construct (or at least a `cloudformation.yaml`).**
  Right now every team rolls their own deploy. Provide one canonical
  one.
- **F7. Conformance fixture catalogue** — `data/parquet-testing/`
  with copies of the corpus and a `just conform` recipe that runs
  ZPQ against each, reports pass/fail. Surface for users to verify
  ZPQ handles their files.

---

## Phase G — Memory / Perf Optimizations (low risk; only when correct)

Last because optimizing an incorrect engine wastes time.

- **G1. Bitset for null masks.** Replace `[]u32 def_levels` with
  `std.bit_set` for `max_def == 1`. 32× memory saving on the most
  common case. Only after nested types land — needs to handle
  `max_def > 1` too, which means widened bitsets or fallback to
  `[]u32` for nesting.
- **G2. HTTP/2 multiplex** for S3 GETs. Current pool: one TCP
  stream per connection. HTTP/2 multiplex would let one connection
  carry many parallel range fetches, reducing pool pressure.
- **G3. io_uring native CLI backend.** epoll today everywhere;
  io_uring on Linux for the CLI binary should be measurably faster
  on syscall-bound workloads (the CLI use case, not Lambda).
- **G4. STS assume-role + IAM Identity Center.** Today we read
  static credentials from env vars or the Lambda exec role. Real
  prod often needs assume-role chains.
- **G5. Retry with backoff on 5xx / 429.** Pool retries on
  socket-level errors; doesn't yet retry on S3 throttling responses.
- **G6. SIMD column decode loops** where applicable (e.g., int32
  PLAIN to filtered output). Only after correctness is locked in.

---

## What we're explicitly NOT planning

- **Sub-row-group filter pushdown via min/max page indices.** Would
  require row-level granular reads and an active page-skipping
  decoder. Big architectural change for marginal gain on our access
  pattern. Reconsider after Phase E.
- **Multi-format input (CSV, JSON, Avro).** Out of scope for ZPQ's
  identity. Use other tools for those, write parquet, then ZPQ.
- **Joins, window functions, complex SQL surface.** DuckDB / Polars
  exist; we don't compete on that axis. ZPQ targets simple filters,
  transformations, and aggregations within a file (Phase C).
- **Encryption.** Parquet modular encryption is rare in our target
  workloads and would significantly complicate the engine. If it
  becomes a hard requirement, that's a v2 conversation.

---

## Sequencing summary

```
A → B1 → B2 → B3 → B4 → C1 → (C2..C5 parallel) → D1, D2 → E1..E9 → F1..F7 → G1..G6
    └─────────┬─────────┘    └─────┬──────┘   └──┬──┘  └────┬────┘
       structural foundations    compute      arch     additive breadth
```

A is a 1-week task. B is the heaviest single phase (B1 alone was
the biggest piece of work to date; B4 brings input parity with B3's
output streaming). C lands the compute layer and folds the old
"filter surface" phase in. D is mid-weight. E, F, G can land in any
order within each phase and against multiple contributors.

The critical commitment: **don't start E until B and C are done**,
even if a specific E item (codec, footer feature) looks fast and
tempting in isolation. The compute primitives in C influence which
codecs / footer features pay off most.
