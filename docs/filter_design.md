# Filter Pushdown Design (Phase 4)

The contract for `src/core/filter/` and the integration points it
touches in metadata + column reading.

## Provenance

Studied before writing:

**ZPQ prior art (in git history):**
- `240fecf Implement Filter Pushdown and Refactor Filter Architecture`
- `bdb7133 feat: add EncodedFilter for unified predicate pushdown`
- `450accd feat: batch filter column fetches across all row groups`
- `1f39622 feat: implement two-phase column fetching for filtered scans`
- `a021fbf docs(filter): syntax specification` — the user-facing DSL
- Deleted `src/core/filter.zig`, `src/core/filter/{operator,parser,scalar}.zig`,
  `src/core/selection.zig` — all retrieved and reviewed

The prior implementation was solid (~600 LoC including tests).
Most of it ports forward with 0.16-API fixes. Specifically usable:

- `Filter` tagged-union AST
- `Operator` enum (Eq/NotEq/Lt/LtEq/Gt/GtEq)
- `SelectionVector` u64 bitmask
- Per-type scalar evaluation kernels
- Filter string parser ("col OP value [AND/OR ...]")
- `EncodedFilter` byte-encoded shortcut for stats comparisons

**Comparable projects:**

| Project | Pattern |
|---|---|
| DuckDB | Three-layer prune: row group → page → value. Min/max stats at all layers. Reads row groups in parallel within a file. |
| Polars | "Prefiltered" strategy — evaluate pushdowns first to produce a row mask, *then* parallelize over columns × row groups, materializing only matching rows. |
| DataFusion | Same three-layer model. Explicit `ColumnIndex` / `OffsetIndex` page pruning with Parquet's optional sidecar metadata. |
| Vortex | Native filter expression in the format itself; runs at decoder layer. (Less directly applicable — we're staying Parquet-native.) |

## Decisions

### 1. Three-layer evaluation, performance-ordered

```
Layer 1: Row-group stats pruning   (cheapest — skip entire RG via min/max)
Layer 2: Page-level stats pruning  (Parquet ColumnIndex sidecar)
Layer 3: Value-level evaluation    (over the decoded batch)
```

Phase 4 ships layers 1 and 3. Layer 2 (ColumnIndex) is deferred:
the page index lives in a separate sidecar that costs an extra
range fetch, and it's only useful when columns are *sorted* —
which our benchmark fixture isn't for most columns. Ship when a
real workload demonstrates the win.

### 2. AST shape (ported)

```zig
pub const Operator = enum { Eq, NotEq, Lt, LtEq, Gt, GtEq };

pub const Filter = union(enum) {
    int32: Leaf(i32),
    int64: Leaf(i64),
    float: Leaf(f32),
    double: Leaf(f64),
    string: Leaf([]const u8),
    boolean: Leaf(bool),
    and_filter: Composite,
    or_filter: Composite,
};
```

Leaf carries `{ col_idx: usize, op: Operator, value: T }`.
Composite carries `{ left: *Filter, right: *Filter }`.

The leaf-node `col_idx` is the column's index in the row group's
`columns` list (resolved from name → idx during parse).

### 3. Selection vector as u64 bitmask

`mask[]u64` — 1 bit per row, packed LSB-first within each word.
Initialized to all-ones (every row active); each predicate clears
bits that don't match. AND composites just run both predicates
sequentially over the same vector. OR composites need a temp copy.

Why bitmask over row-index list:
- O(rows/64) AND/OR via word-level bitops — SIMD-friendly later.
- popcount → match count is constant per word.
- Compact: 8 KB per million rows.
- Branchless evaluation in the inner loop (compute, then either
  clear the bit or don't — no skip-list traversal).

### 4. Stats-based row-group pruning

`metadata.pruneEqual` / `pruneRange` (already exist) extended
into a `pruneRowGroup(rg, filter) Decision` that walks the AST:
- Leaf: extract leaf's column stats from the row group, compare
  using `EncodedFilter`-style byte-level for equality, type-aware
  for ranges. Return `keep` / `skip` / `unknown`.
- AND: `skip` if either child is `skip`; else combine according
  to truth table.
- OR: `skip` only if both children are `skip`.

`unknown` is the conservative default — when stats are absent we
must read and evaluate the value-level predicate.

### 5. EncodedFilter for stats comparisons

The prior-art trick: parse the filter value once into Parquet's
binary representation (4-byte LE for INT32, 8-byte LE for INT64,
raw bytes for BYTE_ARRAY, etc.). Then row-group stats — already
stored as Parquet bytes in the metadata — are byte-comparable.

For equality (`x = needle`):
  - skip if `needle < min || needle > max` — pure byte comparison
    works because min/max are Parquet's natural byte ordering.

For range (`x op needle`):
  - Need type-aware compare for signed integers (LE byte order
    differs from numeric order across the sign bit). For BYTE_ARRAY
    and unsigned ints, byte-comparison works directly.

We ship the type-aware path explicitly. ~150 LoC.

### 6. Value-level evaluation: vectorized + encoded-domain shortcuts

For a decoded batch of `n` values + a SelectionVector `sel`:

```zig
fn evalLeaf(comptime T: type, col: []const T, op: Operator, val: T, sel: *SelectionVector) void {
    for (0..n) |i| {
        if (!sel.isActive(i)) continue;
        const pass = switch (op) {
            .Eq => col[i] == val,
            ... etc ...
        };
        if (!pass) sel.set(i, false);
    }
}
```

Tight scalar loop. Branch-free version: compute `pass` as bool,
then `mask[i/64] &= pass_bit_at_i`. Defer SIMD to a follow-up.

**Encoded-domain shortcut for RLE_DICTIONARY columns:**
Instead of decoding all N values then evaluating, decode the
*dictionary* once (small — typically 8–256 entries), evaluate the
predicate against each dict entry to produce a tiny "matching dict
entries" bitmask, then walk the indices and mark each row's
selection bit by indexing into the dict-bitmask. Big win for
selective queries — a 1024-row page with 8 dict entries does 8
predicate evals instead of 1024.

Phase 4.3 ships the scalar path; Phase 4.3.B adds the dict path.

### 7. Two-phase column fetch (filter columns first)

For a filter that references column F and the query needs columns
{F, X, Y}, we want to:
1. Fetch chunks for column F across surviving row groups.
2. Decode F, evaluate filter, build SelectionVector per row group.
3. If any row group has zero matches: skip its X, Y chunks entirely.
4. Otherwise fetch X, Y chunks (only for matching row groups) and
   decode under the SelectionVector.

This is the "two-phase column fetch" pattern from prior art commit
`1f39622`. Real win on selective queries — never pay the bandwidth
or decode cost for non-matching rows.

Phase 4 ships Phase 1 (filter-only column needed for the count/sum
aggregation in our Lambda demo). Phase 4.B adds the two-phase fetch
when we have multi-column queries.

### 8. Filter-string DSL (from `a021fbf`)

```
expr   := disjunction
disjunction := conjunction ( " OR " conjunction )*
conjunction := leaf ( " AND " leaf )*
leaf   := ident OP value
OP     := "=" | "!=" | "<" | "<=" | ">" | ">="
value  := integer | float | bool | bare-string
```

No parens (matches prior art's documented limitation). AND binds
tighter than OR. Type-coerce based on the column's Parquet type
at parse time (we have the file metadata available).

### 9. Lambda event shape

```json
{
    "s3_url": "s3://bucket/key",
    "filter": "status=active"          // optional
}
```

Without `filter`: aggregates the whole int8 column as today.
With `filter`: prunes row groups, evaluates predicate, returns
matched-row count + aggregate.

## Phase plan

| Phase | What | Tests |
|---|---|---|
| 4.1 | AST + parser + `EncodedFilter` (port from prior art with 0.16 fixes) | unit tests for parse + encode |
| 4.2 | `pruneRowGroup(rg, filter)` walking the AST | tests on synthetic row groups |
| 4.3 | `SelectionVector` + scalar eval kernels | unit tests on synthetic batches |
| 4.4 | Lambda handler accepts `filter` field, runs the whole pipeline against real S3 | production smoke test |

Defer to follow-ups:
- Page-level pruning via ColumnIndex
- Two-phase column fetch (for multi-column queries)
- Dictionary-domain shortcut
- SIMD-tuned scalar kernels
- Parens / NOT / NULL semantics

## What this rejects

- **Predicate compilation to LLVM IR** (DataFusion-style). Overkill
  for our access pattern; tight scalar loops are competitive at
  our row counts.
- **Row-index list selection** (rather than bitmask). Branchier
  inner loop; less SIMD-friendly.
- **Filter reordering / cost model.** AND short-circuits left-to-right
  by predicate position. Smart reordering (cheapest predicate first)
  is a future optimization.

## Test plan

- Parser: synthetic strings → AST, including AND/OR precedence and
  type coercion.
- EncodedFilter: each Parquet type → bytes, then verify
  byte-comparison matches numeric comparison for unsigned types
  and BYTE_ARRAY, and verify type-aware path for signed ints.
- Pruner: synthetic row group with known min/max, predicates that
  intersect / don't intersect.
- Eval: synthetic typed batches, predicate, expect match-count
  matches naive Python-style filter.
- Lambda integration: real S3 file, filter on a few column types,
  assert match count matches what `duckdb` reports for the same
  query against the same fixture.
