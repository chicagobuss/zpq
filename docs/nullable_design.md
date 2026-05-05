# Nullable Column Support — v1 Design

## Status: in-progress (Phase 5.4c)

## Scope

Make the **decode → filter → encode** pipeline correctly handle source
parquet columns with `repetition_type == OPTIONAL` (max_def == 1) that
contain real null values, while preserving the existing REQUIRED-only
fast paths and the byte-copy fastpath unchanged.

Out of scope (deferred to v1.5+):

- Nested OPTIONAL — columns inside structs/lists where `max_def > 1` or
  `max_rep > 0`. Not produced by Polars/Pandas for the workloads we
  benchmark.
- Mixed-def encoder output. v1 only emits all-1s def levels because
  filter eval drops nulls (SQL three-valued logic — any predicate
  involving NULL evaluates to NULL → row not selected). A future
  copy-through path that preserves source nulls would need a
  multi-value RLE encoder run, which we already have via
  `hybrid_rle.encode`.
- `null_count` statistics. Output stats remain min/max only; `null_count`
  is left unset in the output footer. v1.5 if a downstream needs it.

## Why now

Polars/Pandas/Spark write nearly every column as OPTIONAL even when
populated. Today's decoder skips the def-level prefix in source pages
and treats every row as present — works on densely-populated columns,
fails with `ShortDecode` on columns with real nulls (PLAIN bytes
contain only `count(non-null)` values; we attempt to read `num_values`).
Until this is fixed, ZPQ can't claim "drop-in" credibility on real
parquet files.

## Reference

Hardwood (Java parquet engine in `references/hardwood/`) implements the
same pattern we want. Specifically:

- `internal/reader/Page.java` — `Page` is a sealed interface with typed
  records (`IntPage`, `LongPage`, etc.) each carrying parallel arrays:
  `values: T[]`, `definitionLevels: int[]`, `maxDefinitionLevel`. A row
  is null iff `definitionLevels[i] < maxDefinitionLevel`.
- `internal/encoding/PlainDecoder.java` — `readInts/readLongs/...` take
  `(output, definitionLevels, maxDefLevel)`. When def_levels is null
  (REQUIRED), reads `output.length` values directly. When def_levels is
  non-null, counts `numDefined`, reads exactly that many values from
  the wire, and writes them only at slots where
  `defLevels[i] == maxDefLevel`. Other slots stay at their primitive
  default (0 / false / null).
- `internal/reader/PageDecoder.java::parseDataPage` — reads
  `<u32 LE def_levels_byte_length><RLE def levels><values>`, decodes
  def levels first, then dispatches to per-encoding readers passing
  def_levels through.

We're porting this exact pattern to Zig.

## Architecture

### Decode side

The shape of a decoded column becomes:

```zig
pub const ColumnData = struct {
    values: anytype,           // []i32 / []i64 / []f32 / []f64 / [][]const u8 / []bool
    def_levels: ?[]const u32,  // null when source column has max_def == 0 (REQUIRED).
                               // Otherwise length == values.len; def_levels[i] < max_def → null at i.
    max_def: u8,               // 0 for REQUIRED; 1 for flat OPTIONAL.
};
```

`filter_eval.Batch.Column` becomes a tagged union of those, mirroring
today's shape but each variant gains the optional def_levels and
max_def alongside the typed values.

`ColumnChunkReader(T)` gains a new entry point:

```zig
pub fn decode(
    self: *Self,
    out_values: []T,
    out_def_levels: ?[]u32,
) Error!usize
```

When `out_def_levels` is non-null, the reader:

1. Parses each data page's def-level prefix (already partially wired —
   today's code skips past these bytes; v1 actually decodes them via
   `hybrid_rle.HybridRleDecoder` with `bit_width = 1` for max_def == 1).
2. Counts `num_present` in the slice of def_levels for the current
   batch.
3. Calls the per-page value decoder (`PlainDec`, `RleDictDec`, etc.)
   asking for exactly `num_present` values into a small temp slice.
4. Spreads those values into `out_values` at slots where
   `def_levels[i] == max_def`. Other slots are left as the primitive
   default (Zig's `undefined` is unsafe — we initialise to zero / `""`
   / `false` to make filter-eval bugs deterministic).

If `out_def_levels` is null, the reader assumes max_def == 0 and reads
straight into `out_values` as today.

### Per-page decoders

The internal `PlainDecoderFor(T)`, `RleDictDecFor(T)`, etc. stay
*unchanged* — they continue to expose `decode(dest: []T) → usize`. The
chunk reader does the def-level math externally and asks the per-page
decoder for exactly `num_present` raw values. This keeps the per-page
machinery simple and means the existing tests don't churn.

### Filter side

`filter_eval.evaluate` already walks the AST against typed columns;
extend each leaf's evaluation to consult `def_levels` first:

```
for each row i:
    for each leaf in filter:
        if column_for(leaf).def_levels[i] < column.max_def:
            sel.deactivate(i); break  // SQL: NULL op X → unknown → fail
        else:
            evaluate predicate; deactivate on false
```

This implements SQL three-valued logic with the simplifying assumption
that NULL never satisfies a predicate (true for all our supported ops:
=, !=, <, <=, >, >=). For a filter that doesn't reference column C,
nulls in C don't matter — selection vector is unchanged regardless.

### Encode side

`encoder.applySelection` takes the `Batch.Column` and produces a
materialised slice of survivors. With the filter rule above, all
survivors are non-null, so the materialised slice is densely populated.
The encoder we shipped earlier this session already emits all-1s
def-level prefixes for OPTIONAL leaves — no changes needed there.

`encoder.encodeColumn` for an OPTIONAL leaf with non-null survivors:

```
[page header thrift]
[<u32 LE: rle_byte_len><RLE all-1s for N values>]   // def-level prefix
[PLAIN values * N]                                   // every value present
```

This is wire-correct OPTIONAL with `null_count = 0` (implicit; we don't
write the field).

## Tests

1. **Unit: HybridRleDecoder bit-width-1 round-trip** — already in
   `hybrid_rle.zig` for the encoder. Add corresponding decoder tests
   covering all-1s and mixed patterns.
2. **Unit: PlainDecoder spread under def_levels** — synthesise a small
   page, decode with masked def_levels, verify nulls land in the right
   slots and values are preserved at the present slots.
3. **Integration: filter on actually-null column** — invoke lambda with
   `filter: int32_nullable > 0` against the partitioned fixture (which
   has real nulls in `int32_nullable` / `float64_nullable` /
   `string_nullable`). Verify pyarrow and duckdb read the output;
   row count matches `count(int32_nullable IS NOT NULL AND int32_nullable > 0)`.
4. **Integration: filter on null-free column with surviving null
   columns** — filter on `int8` (REQUIRED-shaped, no nulls) but project
   to include `int32_nullable`. Output preserves nulls in the projected
   column. (v1: this requires preserving def_levels through the encode
   path, which we don't yet do — listed as v1.5 but might be small to
   add.)

## Wire-format reminders learned from today's bug hunt

- `IntType.bitWidth` is i8, not i32 — write via the `Byte` (type=3)
  marker, NOT zigzag. Strict readers (pyarrow, duckdb, parquet-mr)
  reject the I32 encoding even though our own (lenient) reader
  accepts both. Fixed in this session.
- Definition levels are encoded with RLE/Bit-Packed Hybrid at
  `bit_width = ceil(log2(max_def + 1))` — for `max_def = 1` (flat
  OPTIONAL), bit_width = 1. We already emit this correctly in the
  encoder for all-1s output.
- The `<u32 LE byte_len>` prefix before the RLE bytes is required for
  V1 data pages whenever `max_def > 0` or `max_rep > 0`.
