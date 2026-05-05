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
  - **Input-side streaming.** Inputs are still fetched as full file
    bytes upfront — the new memory ceiling. For inputs >> 512 MB,
    that becomes the bottleneck. Touches `s3.fetchJobs` and the
    decode path; can land independently of any output work.
  - Tautology-filter dispatch hack (B1.z) still needs cleaning up
    via a `?filter` parameter on `buildFilteredOutputMulti`.

**What breaks if we skip B and do D (codecs) first?**
- A snappy output encoder built for the current "encode whole file
  to memory" model needs to be re-fit when streaming arrives.
- Dictionary encoding writer designed for flat columns has to be
  generalised when nested columns land.
- Bloom filter footer-write is footer-shaped today; with streaming
  the footer is the *last* part, not appended to a single buffer —
  the write path differs.

---

## Phase C — Architectural Variants (medium rewrite risk; localised)

Things that change *one* major subsystem but don't ripple. Order
within this phase is flexible; B1 must precede C2.

- **C1. Cross-bucket inputs / outputs.** Pool is single-bucket today
  by simplifying assumption. Most real data platforms separate read
  and write buckets. Either: (a) one pool per bucket, lazy-init; or
  (b) a single pool that knows N hosts. (a) is simpler and probably
  fine for ≤3 buckets per invocation. Independent of B; could land
  any time, but better before HTTP/2 multiplex (G2) so the multi-host
  model influences the connection design.
- **C2. Schema evolution across files.** Today: identical schemas
  required, error otherwise. Realistic: column added month over
  month. Output schema = union; missing column in older file = all
  nulls in that file's contribution. Depends on B1 — the schema
  reconciliation logic is much cleaner once nested types are real.

**What breaks if we skip C2 and assume identical schemas through D
and E?** Filter binding logic codifies "lookup by index in file 0";
when schema evolution lands the index→column_path mapping has to be
reworked. Filter AST that uses `col_idx` may need to switch to
column-path keys. Better to settle that before we accumulate filter
features (E).

---

## Phase D — Codec & Encoding Breadth (low rewrite risk; additive)

Once the fundamental shapes are settled, we expand "what bytes ZPQ
can read and write." Each item is contained to encoder/decoder
modules.

- **D1. Snappy output encoder.** We already decode snappy. Closing
  the loop is ~200 LoC + tests. Reduces output size 2-3× without
  perf regression on warm runs.
- **D2. Zstd input + output.** Whole new codec. zstd is increasingly
  common (better ratio than snappy at similar speed). Probably
  vendor `libzstd` like we vendor BoringSSL.
- **D3. Gzip input.** Older parquet files; Hadoop-era tooling.
- **D4. Dictionary encoding writer.** RLE_DICTIONARY for low-card
  columns. Big perf win for re-encoded strings.
- **D5. DELTA_BINARY_PACKED writer.** For sorted/timestamp columns.
- **D6. DELTA_BYTE_ARRAY writer.** For correlated string columns.
- **D7. Bloom filter writer.** Footer-level structure for
  high-cardinality predicate pushdown.
- **D8. Page index writer (offset_index + column_index).** We
  currently *drop* these on re-encode. Restoring them lets
  downstream readers do row-level pruning instead of falling back
  to row-group-level.
- **D9. `null_count` in column stats.** Strict pruners (parquet-mr)
  treat unset null_count as pessimistic. Free correctness win.

D1 first (biggest single win, smallest LoC). D7/D8/D9 are footer-
shape improvements that could land together.

---

## Phase E — Filter Surface (low rewrite risk; contained to filter/)

The current filter language is `=, !=, <, <=, >, >=, AND, OR`. To be
credible as a "drop in for DuckDB" each of these is required.

- **E1. `IS NULL` / `IS NOT NULL`.** Trivial AST + eval addition
  (~30 LoC). The decode path now produces def_levels; checking them
  is one more leaf type.
- **E2. `NOT`.** Negation operator. AST gains a unary node.
- **E3. Parens.** Parser-side; precedence already exists for AND/OR.
- **E4. `IN (...)`.** Sugar for OR-of-equalities; can compile to
  existing AST or add a new leaf type.
- **E5. `LIKE`.** Pattern match. `%`/`_` wildcards, escape handling.

These can land independently and in any order. None affect the
decode/encode shape.

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
  pattern. Reconsider after Phase D.
- **Multi-format input (CSV, JSON, Avro).** Out of scope for ZPQ's
  identity. Use other tools for those, write parquet, then ZPQ.
- **Compute primitives beyond filter + projection** (joins, aggs,
  group-bys). DuckDB / Polars exist; we don't compete on that axis.
  ZPQ is the I/O-bound predicate-pushdown lane.
- **Encryption.** Parquet modular encryption is rare in our target
  workloads and would significantly complicate the engine. If it
  becomes a hard requirement, that's a v2 conversation.

---

## Sequencing summary

```
A → B1 → B2 → B3 → C1, C2 → D1..D9 → E1..E5 → F1..F7 → G1..G6
    └────────┬─────────┘    └────┬────┘  (parallel ok within group)
       structural             additive
```

A is a 1-week task. B is a multi-week phase (B1 alone is probably
the heaviest single piece of work in the roadmap). C is mid-weight.
D, E, F, G can land in any order within each phase and against
multiple contributors.

The critical commitment: **don't start D until B is done**, even if
a specific D item looks fast and tempting in isolation.
