//! Per-row-group scan→consumer protocol.
//!
//! Lambda's parallel-fetcher orchestrator and the CLI's sequential
//! local-file driver both feed the same shape of work to the same
//! two consumers: *encode* (decode → filter → re-encode kept cols)
//! and *copy* (byte-copy kept chunks, shift offsets).
//!
//! The only thing they differ on is *where the bytes come from*:
//! lambda hands in a `RowGroupResult.raw_bytes` window covering the
//! kept-column span, CLI hands in the entire mmap'd file. Both shapes
//! collapse to a single `RGSrc { bytes, byte_origin }` — the per-
//! column offset into the source equals `col.dpo - byte_origin`
//! either way.
//!
//! Output is always a `streaming.Sink` (multipart-S3 in lambda; an
//! fd-backed sink in the CLI). Per-phase timings flow into a shared
//! `Timings` accumulator the caller owns.
//!
//! Layering: this module lives in `core/`, so it must NOT depend on
//! any I/O. The `Sink` is opaque; the source is a borrowed slice.

const std = @import("std");
const builtin = @import("builtin");
const owner_groups = @import("owner_groups.zig");
const msd_groups = @import("msd_groups.zig");
const retained_pages = @import("retained_pages.zig");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");

/// Zig's 16 MiB default made thread setup/teardown dominate short queries. Duplicated rather than imported from
/// scan.zig, which imports this module.
const WORKER_STACK_SIZE: usize = 1 << 20;
const spawn_util = @import("spawn.zig");
const blocking = @import("blocking.zig");
const schema = @import("schema.zig");
const filter_ast = @import("filter/ast.zig");
const filter_eval = @import("filter/eval.zig");
pub const SelectedBatch = @import("selected_batch.zig").SelectedBatch;
const filter_selection = @import("filter/selection.zig");
const filter_prune = @import("filter/prune.zig");
const expr_ast = @import("expr/ast.zig");
const expr_eval = @import("expr/eval.zig");
const expr_agg = @import("expr/agg.zig");
const core_memory = @import("memory.zig");
const agg_plan = @import("agg_plan.zig");
const encoder = @import("writer/encoder.zig");
const streaming = @import("writer/streaming.zig");
const fastpath = @import("writer/fastpath.zig");
const page_index = @import("writer/page_index.zig");
const thrift = @import("thrift.zig");
const column_mod = @import("parquet/column.zig");
const page_mod = @import("parquet/page.zig");
const Diag = @import("diag.zig").Diag;
const Text = @import("diag.zig").Text;
const decimal_mod = @import("parquet/decimal.zig");
const compression = @import("parquet/compression.zig");
const invariant = @import("invariant.zig");
const int96_mod = @import("parquet/int96.zig");
const metadata = @import("parquet/metadata.zig");
const statistics = @import("parquet/statistics.zig");
const header_writer = @import("writer/page_header.zig");
const WriterStatsPolicy = @import("parquet/writer_statistics.zig").Policy;
const agg_coded = @import("expr/agg_coded.zig");
const agg_dense = @import("expr/agg_dense.zig");
const partagg = @import("partagg.zig");
const huge_pages = @import("huge_pages.zig");

pub const DecodeOptions = column_mod.DecodeOptions;
pub const DecodeScratch = column_mod.DecodeScratch;
pub const SliceScratch = page_mod.SliceScratch;
pub const RowGroupArena = @import("row_group_arena.zig").RowGroupArena;

pub const types = @import("consumer/types.zig");
pub const Error = types.Error;
pub const RGSrc = types.RGSrc;
pub const Timings = types.Timings;
pub const DecodeSite = types.DecodeSite;
pub const enterChunk = types.enterChunk;
pub const decodeFailure = types.decodeFailure;
pub const setDecodeFailure = types.setDecodeFailure;
pub const resolveDecodeFailure = types.resolveDecodeFailure;
pub const isDecodeError = types.isDecodeError;
pub const RGOut = types.RGOut;
pub const rebaseRowGroupOffsets = types.rebaseRowGroupOffsets;
const decode_order = @import("decode_order.zig");

pub const output = @import("consumer/output.zig");
pub const OutputAggregator = output.OutputAggregator;
pub const initOutputAggregator = output.initOutputAggregator;
pub const resetAggregator = output.resetAggregator;

pub const appendProjectedRG = output.appendProjectedRG;
pub const appendProjectedRGWithOptions = output.appendProjectedRGWithOptions;
pub const ProjectedRG = output.ProjectedRG;
pub const Survivors = output.Survivors;
const gatherRows = output.gatherRows;

const decodeWriteColumn = consumer_column.decodeWriteColumn;

pub const appendSliceRows = output.appendSliceRows;

const namedLevels = output.namedLevels;
const flatLevels = output.flatLevels;

pub const renameLeaf = types.renameLeaf;

const booleanLane = output.booleanLane;
pub const outputLane = output.outputLane;

pub const chunkDictionary = consumer_column.chunkDictionary;

pub const slice_reader = @import("consumer/slice_reader.zig");
pub const SliceReader = slice_reader.SliceReader;

const decimalKind = consumer_column.decimalKind;
const decodeDecimalI128 = consumer_column.decodeDecimalI128;

const appendSelected = output.appendSelected;

const decodeWriteColumnMaybeCoded = consumer_column.decodeWriteColumnMaybeCoded;

const appendIntoBuf = output.appendIntoBuf;
const appendTyped = output.appendTyped;

pub const encode_module = @import("consumer/encode.zig");
pub const encodeAggregator = encode_module.encodeAggregator;
pub const RGEncoder = encode_module.RGEncoder;
pub const plainBits = encode_module.plainBits;
pub const plainBitsPrefix = encode_module.plainBitsPrefix;
pub const placeChunk = encode_module.placeChunk;
pub const AggEncode = encode_module.AggEncode;
pub const answer_rg_rows = encode_module.answer_rg_rows;
pub const writeAggregateRows = encode_module.writeAggregateRows;
pub const AggregateFile = encode_module.AggregateFile;
const cloneColumnMeta = encode_module.cloneColumnMeta;
const sliceOutputCol = encode_module.sliceOutputCol;
const aggEncodeCharge = encode_module.aggEncodeCharge;
const writeAggregateRowsEncoded = encode_module.writeAggregateRowsEncoded;
const appendAggregateRows = encode_module.appendAggregateRows;
pub const bufRows = output.bufRows;
pub const appendRows = output.appendRows;
pub const BufRange = output.BufRange;
pub const joinRows = output.joinRows;

pub const OutputCol = types.OutputCol;
pub const computedSchemaElem = types.computedSchemaElem;
pub const decimalElem = types.decimalElem;

pub const RowSubgroup = types.RowSubgroup;
pub const AggRowGroup = types.AggRowGroup;
pub const selectedWindow = types.selectedWindow;

pub const target_module = @import("consumer/target.zig");
pub const PackedCount = target_module.PackedCount;
pub const IntegerCountSink = target_module.IntegerCountSink;
pub const SelectedRecords = target_module.SelectedRecords;
pub const RecordSink = target_module.RecordSink;

pub const scan_module = @import("consumer/scan.zig");
pub const scanRGForAgg = scan_module.scanRGForAgg;
pub const probeDecodeFailure = scan_module.probeDecodeFailure;
const emitRecords = scan_module.emitRecords;
const foldGroupedWindows = scan_module.foldGroupedWindows;
pub const canonicalDecodeFailure = scan_module.canonicalDecodeFailure;
const automaticCacheRows = scan_module.automaticCacheRows;
const foldScalarCache = scan_module.foldScalarCache;

pub const AggTarget = target_module.AggTarget;

pub const workspace_module = @import("consumer/workspace.zig");
pub const AggWorkspace = workspace_module.AggWorkspace;
const admitWhole = workspace_module.admitWhole;
const dropRegion = workspace_module.dropRegion;
const refusedDraw = workspace_module.refusedDraw;

pub const budget_module = @import("consumer/budget.zig");
pub const DecodeBudget = budget_module.DecodeBudget;
pub const decodeEstimate = budget_module.decodeEstimate;

pub const CacheHints = workspace_module.CacheHints;
pub const cacheHintsWanted = workspace_module.cacheHintsWanted;
const cacheDictionary = workspace_module.cacheDictionary;


pub const pool_module = @import("consumer/pool.zig");
pub const poolOn = pool_module.poolOn;
pub const releaseSlicePool = pool_module.releaseSlicePool;


const scan_plan = @import("consumer/plan.zig");
const StagePlan = scan_plan.StagePlan;
const CallBatch = scan_plan.CallBatch;
const payloadColumns = scan_plan.payloadColumns;
const plainValueEncodings = scan_plan.plainValueEncodings;
const hashOnlyByteArray = scan_plan.hashOnlyByteArray;
const streamByteArray = scan_plan.streamByteArray;
const streamWindowRows = scan_plan.streamWindowRows;
const GatedFilter = scan_plan.GatedFilter;
const foldWantsCodes = scan_plan.foldWantsCodes;
const flatGroupKey = scan_plan.flatGroupKey;
const keyCodesCheaper = scan_plan.keyCodesCheaper;
const columnFeedsFold = scan_plan.columnFeedsFold;
const plainLengthColumn = scan_plan.plainLengthColumn;
const ChunkDecoder = scan_plan.ChunkDecoder;
const sliceRows = scan_plan.sliceRows;
const planCodeUses = scan_plan.planCodeUses;
const mark = scan_plan.mark;

const agg_column = @import("consumer/agg_column.zig");
const decodeAggColumnCoded = agg_column.decodeAggColumnCoded;
const decodeAggColumnHist = agg_column.decodeAggColumnHist;
const decodeLevelsOnly = agg_column.decodeLevelsOnly;
const decodeAggColumn = agg_column.decodeAggColumn;

const pruning = @import("consumer/pruning.zig");
const PageIndex = pruning.PageIndex;
const FilterPages = pruning.FilterPages;
const loadPageIndexes = pruning.loadPageIndexes;
const pruneFilterPages = pruning.pruneFilterPages;
const pagePruning = pruning.pagePruning;
const compactRows = pruning.compactRows;

pub const copy_module = @import("consumer/copy.zig");
pub const copyRG = copy_module.copyRG;
pub const BloomWrite = copy_module.BloomWrite;
pub const Recompress = copy_module.Recompress;
pub const RecompressedChunk = copy_module.RecompressedChunk;
pub const recompressRowGroup = copy_module.recompressRowGroup;
const dictionaryBloom = copy_module.dictionaryBloom;
const PageMove = copy_module.PageMove;
const recompressChunk = copy_module.recompressChunk;
const sourceBytes = types.sourceBytes;


fn pageForTest(arena: std.mem.Allocator, header: schema.PageHeader, body: []const u8) ![]u8 {
    var w = thrift.Writer.init(arena);
    try header.write(&w);
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, w.bytes());
    try out.appendSlice(arena, body);
    return out.items;
}

test "a page claiming more values than its chunk is rejected before sizing buffers from the claim" {
    // A few bytes can claim any count: one RLE def-level run of N present values. The level buffer used to be
    // sized from that claim, so a one-row chunk could make the reader allocate gigabytes.
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const claimed: u32 = 1 << 24;
    var body: std.ArrayList(u8) = .empty;
    var run: [8]u8 = undefined; // ULEB128 run header (claimed << 1), then the 1-byte value
    var n: u32 = claimed << 1;
    var len: usize = 0;
    while (true) : (len += 1) {
        run[len] = @truncate(n & 0x7f);
        n >>= 7;
        if (n == 0) break;
        run[len] |= 0x80;
    }
    len += 1;
    run[len] = 1;
    len += 1;
    var prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &prefix, @intCast(len), .little);
    try body.appendSlice(a, &prefix);
    try body.appendSlice(a, run[0..len]);
    try body.appendSlice(a, &[_]u8{ 42, 0, 0, 0 });
    const page = try pageForTest(a, .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{ .num_values = claimed, .encoding = .PLAIN, .definition_level_encoding = .RLE, .repetition_level_encoding = .RLE },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    }, body.items);
    const optional: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
    try testing.expectError(error.PageValueCountExceedsChunk, decodeColumnT(i32, a, page, .UNCOMPRESSED, optional, 1));

    // A dictionary page claiming far more entries than its bytes can hold.
    const dict_body = [_]u8{ 1, 0, 0, 0, 'a' };
    const dict_page = try pageForTest(a, .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = dict_body.len,
        .compressed_page_size = dict_body.len,
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{ .num_values = 1 << 20, .encoding = .PLAIN, .is_sorted = null },
        .data_page_header_v2 = null,
    }, &dict_body);
    const required: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
    try testing.expectError(error.DictionaryLargerThanPage, decodeColumnT([]const u8, a, dict_page, .UNCOMPRESSED, required, 1));
}

pub const consumer_column = @import("consumer/column.zig");
pub const checkRowShape = consumer_column.checkRowShape;
pub const PruningInfo = consumer_column.PruningInfo;
pub const RowRange = consumer_column.RowRange;
pub const decodeColumnTPruned = consumer_column.decodeColumnTPruned;
pub const decodeFlbaColumnPruned = consumer_column.decodeFlbaColumnPruned;
pub const decodeColumnT = consumer_column.decodeColumnT;
pub const decodeColumnTWithOptions = consumer_column.decodeColumnTWithOptions;
pub const decodeFlbaColumn = consumer_column.decodeFlbaColumn;
pub const decodeFlbaColumnWithOptions = consumer_column.decodeFlbaColumnWithOptions;
pub const flbaWidth = consumer_column.flbaWidth;
const originSlice = consumer_column.originSlice;
const defaultVal = consumer_column.defaultVal;
const decodeWithReaderPruned = consumer_column.decodeWithReaderPruned;
const decodeFloat16ColumnAsF64Pruned = consumer_column.decodeFloat16ColumnAsF64Pruned;
const decodeU32ColumnAsI64Pruned = consumer_column.decodeU32ColumnAsI64Pruned;
const decodeFloat16ColumnAsF64 = consumer_column.decodeFloat16ColumnAsF64;

test "row subgroups seek misaligned columns and share dictionary backing across reader lifetimes" {
    const t = std.testing;
    const subgroups = @import("row_subgroups.zig");
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try subgroups.testFixture(a);
    const rg = &fixture.meta.row_groups.items[0];
    const group = (try subgroups.Group.init(a, fixture.bytes, rg, &fixture.meta, &.{ true, true, true }, 8)).?;
    const src: RGSrc = .{ .bytes = fixture.bytes, .byte_origin = 0 };
    for (0..3) |ci| {
        var whole = (try SliceReader.init(a, t.allocator, rg, &fixture.meta, src, ci, .{})).?;
        defer whole.deinit();
        const expected = try whole.next(a, 4096);
        for (group.ranges) |range| {
            var r = (try SliceReader.init(a, t.allocator, rg, &fixture.meta, src, ci, .{})).?;
            defer r.deinit();
            try r.seekSubgroup(a, .{ .group = &group, .rows = range });
            if (ci == 1) try t.expect(r.raw.bytes.dictionary.?.ptr == group.columns[ci].?.dictionary.?.bytes.ptr);
            const got = try r.next(a, range.hi - range.lo);
            switch (expected) {
                inline .i64, .string => |want, tag| {
                    const actual = @field(got, @tagName(tag));
                    try t.expectEqual(range.hi - range.lo, actual.values.len);
                    for (actual.values, 0..) |value, i| {
                        const def = if (want.def_levels) |d| d[range.lo + i] else want.max_def;
                        try t.expectEqual(def, if (actual.def_levels) |d| d[i] else actual.max_def);
                        if (def < want.max_def) continue;
                        if (tag == .string) try t.expectEqualStrings(want.values[range.lo + i], value) else try t.expectEqual(want.values[range.lo + i], value);
                    }
                },
                else => unreachable,
            }
        }
    }
}

/// Test-only: off compares compact decoding with whole columns; the count shows a test reached it.
pub var compact_rows_in_tests = true;
pub var compacted_row_groups: std.atomic.Value(usize) = .init(0);

// ============================================================
// Compile-time API check. End-to-end behavior is exercised through
// the lambda integration tests + CLI golden tests.
// ============================================================
test "consumer: API is well-typed" {
    _ = appendProjectedRG;
    _ = encodeAggregator;
    _ = copyRG;
    _ = decodeColumnT;
}

// Regression: copyRG(no_projection) used to write whatever `src.bytes`
// the caller passed, even when those bytes covered more than just this
// RG. The engine passes the *whole file*, so a file with N row groups
// produced an N×-bloated output. Lock the fix: with a hand-built 2-RG
// layout in a 1000-byte buffer, copying RG0 must write only RG0's
// bounding box (60 bytes), not the whole buffer.
test "copyRG no-projection: writes only this RG's bounding box, not src.bytes" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // Fake file layout: 1000 bytes total. RG0's columns span
    // [100, 160), RG1's span [500, 580). The two RGs are far apart in
    // the source file (typical for real parquet — pages + dict pages
    // + column metadata between them).
    const total_bytes: usize = 1000;
    var buf = try a.alloc(u8, total_bytes);
    @memset(buf, 0); // start with zeros so we can identify what got copied

    // Stamp recognizable patterns into RG0's region so we can assert
    // the right bytes ended up in the output.
    for (100..160) |i| buf[i] = @intCast((i - 100) + 1); // 1, 2, 3, ...

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    // RG0: two columns at offsets 100 and 140 (each 20 bytes
    // compressed). Bounding box: [100, 160).
    var rg0_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg0_cols.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 10,
            .total_uncompressed_size = 50,
            .total_compressed_size = 20,
            .data_page_offset = 100,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try rg0_cols.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 10,
            .total_uncompressed_size = 30,
            .total_compressed_size = 20,
            .data_page_offset = 140,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    // ALSO add a column from far away in the file to make sure copyRG
    // doesn't include it (it shouldn't — it's not in RG0). Sit it at
    // offset 800 — well past RG0's bounding box. (In a real file this
    // wouldn't happen; here it's a guardrail against "did we accidentally
    // re-fall-through to whole-file copy?".)
    // Note: copyRG operates on rg.columns.items only, so adding bytes
    // outside that doesn't affect it. RG0's bounding box is determined
    // entirely from RG0's columns.

    const rg0: schema.RowGroup = .{
        .columns = rg0_cols,
        .total_byte_size = 60,
        .num_rows = 10,
    };

    // Capture sink — records writes into an ArrayList.
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        alloc: std.mem.Allocator,
        fn write(ctx: *anyopaque, b: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.bytes.appendSlice(self.alloc, b);
        }
    };
    var cap: Capture = .{ .alloc = a };
    const sink: streaming.Sink = .{
        .ctx = &cap,
        .write_fn = Capture.write,
    };

    var out_offset: u64 = 0;
    var timings: Timings = .{};
    const out = try copyRG(
        a,
        &rg0,
        .{ .bytes = buf, .byte_origin = 0 },
        null, // no projection — exercises the fixed branch
        sink,
        &out_offset,
        &timings,
        true, // direct-to-file: emit page index
        null,
        .{},
    );

    // Bounding box for RG0 is [100, 160) = 60 bytes. Anything more
    // means we're back to the old bug.
    try testing.expectEqual(@as(usize, 60), cap.bytes.items.len);
    try testing.expectEqual(@as(u64, 60), out_offset);
    // Content should match buf[100..160] exactly (the stamped pattern).
    try testing.expectEqualSlices(u8, buf[100..160], cap.bytes.items);
    // The output RG's total_byte_size is its chunks' uncompressed bytes (50 + 30), not the 60 bytes copied.
    try testing.expect(out.rg != null);
    try testing.expectEqual(@as(i64, 80), out.rg.?.total_byte_size);
    try testing.expectEqual(@as(i64, 10), out.rg.?.num_rows);
    // Column offsets must be shifted to point into the output stream.
    // Column 0 was at source offset 100; output bounding box starts at
    // output offset 0; so new offset = 0 (i.e. new_start - rg_min = 0).
    try testing.expectEqual(@as(i64, 0), out.rg.?.columns.items[0].meta_data.?.data_page_offset);
    // Column 1 was at source offset 140 → output offset 40 (140 - 100).
    try testing.expectEqual(@as(i64, 40), out.rg.?.columns.items[1].meta_data.?.data_page_offset);
}

// --- DECIMAL decode: fixture-free encode→decode round-trip ---
// The decimal *decode* tests in decimal.zig read parquet-testing fixtures
// and skip in CI without the corpus (as do all of column.zig's decode
// tests). This exercises the full INT32/INT64-backed decimal decode path
// — page parse → ColumnChunkReader → scale → f64 — with no fixture, by
// encoding a known int column and decoding it back as a DECIMAL.
test "decimal: INT32/INT64 backings decode round-trip through the encoder" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    { // INT32-backed, scale 2 → raw / 100
        const raw = [_]i32{ 12345, -6789, 100, 0, 999999 };
        const elem = schema.SchemaElement{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = 0,
            .converted_type = .DECIMAL,
            .logical_type = null,
            .scale = 2,
            .precision = 9,
            .field_id = null,
        };
        const enc = try encoder.encodeColumn(arena, .{
            .values = .{ .i32 = .{ .values = &raw } },
            .schema_elem = &elem,
            .path_in_schema = &[_][]const u8{"value"},
            .codec = .UNCOMPRESSED,
        });
        const kind = decimal_mod.kindFromSchema(&elem).?;
        const col = try decimal_mod.decodeColumnAsF64(arena, enc.bytes, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, raw.len, kind);
        const want = [_]f64{ 123.45, -67.89, 1.00, 0.0, 9999.99 };
        try testing.expectEqual(want.len, col.values.len);
        for (want, col.values) |w, g| try testing.expectApproxEqAbs(w, g, 1e-9);
    }

    { // INT64-backed, scale 3
        const raw = [_]i64{ 1000, -250, 0, 123456789 };
        const elem = schema.SchemaElement{
            .type = .INT64,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = 0,
            .converted_type = .DECIMAL,
            .logical_type = null,
            .scale = 3,
            .precision = 18,
            .field_id = null,
        };
        const enc = try encoder.encodeColumn(arena, .{
            .values = .{ .i64 = .{ .values = &raw } },
            .schema_elem = &elem,
            .path_in_schema = &[_][]const u8{"value"},
            .codec = .UNCOMPRESSED,
        });
        const kind = decimal_mod.kindFromSchema(&elem).?;
        const col = try decimal_mod.decodeColumnAsF64(arena, enc.bytes, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, raw.len, kind);
        const want = [_]f64{ 1.0, -0.25, 0.0, 123456.789 };
        try testing.expectEqual(want.len, col.values.len);
        for (want, col.values) |w, g| try testing.expectApproxEqAbs(w, g, 1e-6);
    }
}

// --- --fast-levels: all-present definition-level detection ---
//
// The flag lets a decode skip materialising definition levels for any
// page whose level stream proves every value present. These build the
// awkward shapes by hand — an all-present page, a page with real nulls,
// and a chunk that changes from one to the other mid-stream — because
// the mid-stream transition is the only place the optimisation has to
// hand work back to the ordinary path, and getting the hand-off wrong
// would silently shift every value after it.
//
// Every case asserts the SAME values with the flag on and off. A faster
// wrong answer is the failure mode worth guarding, not a slower one.

const fl_test_elem = schema.SchemaElement{
    .type = .INT32,
    .type_length = null,
    .repetition_type = .OPTIONAL,
    .name = "v",
    .num_children = 0,
    .converted_type = null,
    .logical_type = null,
    .scale = null,
    .precision = null,
    .field_id = null,
};

/// Encode one OPTIONAL INT32 page. `def` of null means "no level array
/// supplied" — the encoder then writes an all-present stream itself.
fn flEncodePage(
    arena: std.mem.Allocator,
    values: []const i32,
    def: ?[]const u32,
) ![]u8 {
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .i32 = .{ .values = values, .def_levels = def, .max_def = 1 } },
        .schema_elem = &fl_test_elem,
        .path_in_schema = &[_][]const u8{"v"},
        .codec = .UNCOMPRESSED,
    });
    return enc.bytes;
}

const fl_levels = schema.Levels{ .max_def = 1, .max_rep = 0 };

const req_i32_elem = schema.SchemaElement{
    .type = .INT32,
    .type_length = null,
    .repetition_type = .REQUIRED,
    .name = "v",
    .num_children = 0,
    .converted_type = null,
    .logical_type = null,
    .scale = null,
    .precision = null,
    .field_id = null,
};

test "a required row gate skips an unread page and defaults the unselected slots of a partial page" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };

    // i32/i64 pages are DELTA_BINARY_PACKED, which has to be read in order. PLAIN f64 is the within-page gather.
    const req_f64 = schema.SchemaElement{
        .type = .DOUBLE,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "v",
        .num_children = 0,
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var one_page: [32]f64 = undefined;
    for (&one_page, 0..) |*v, i| v.* = @floatFromInt(i + 1);
    const whole = try encoder.encodeColumn(arena, .{
        .values = .{ .f64 = .{ .values = &one_page } },
        .schema_elem = &req_f64,
        .path_in_schema = &.{"v"},
        .codec = .UNCOMPRESSED,
        .dictionary_bloom = false,
        .distinct = true,
        .page_rows = 64,
    });
    try testing.expect(plainValueEncodings(whole.meta.encodings.items));
    const full = try decodeColumnT(f64, arena, whole.bytes, .UNCOMPRESSED, levels, one_page.len);
    var odds_off = try filter_selection.SelectionVector.init(arena, one_page.len);
    for (0..one_page.len) |i| if (i % 2 == 1) odds_off.set(i, false);
    const gathered = try decodeColumnTWithOptions(f64, arena, whole.bytes, .UNCOMPRESSED, levels, one_page.len, .{
        .row_gate = &odds_off,
    });
    for (full.values, gathered.values, 0..) |want, got, i| {
        try testing.expectEqual(if (i % 2 == 0) want else 0, got);
    }

    var paged: [32]i32 = undefined;
    for (&paged, 0..) |*v, i| v.* = @intCast(i + 1);
    const pages = try encoder.encodeColumn(arena, .{
        .values = .{ .i32 = .{ .values = &paged } },
        .schema_elem = &req_i32_elem,
        .path_in_schema = &.{"v"},
        .codec = .UNCOMPRESSED,
        .dictionary_bloom = false,
        .distinct = true,
        .page_rows = 16,
    });
    const clean = try decodeColumnT(i32, arena, pages.bytes, .UNCOMPRESSED, levels, paged.len);
    var corrupt = try arena.dupe(u8, pages.bytes);
    var walker = page_mod.PageReader.init(corrupt, .UNCOMPRESSED, arena);
    const first = (try walker.peek()) orelse return error.UnexpectedPage;
    const header_len = first.size - @as(usize, @intCast(first.header.compressed_page_size));
    try testing.expect(first.header.compressed_page_size > 4);
    corrupt[walker.pos + header_len + 4] ^= 0xff;
    const page_rows: usize = @intCast(first.header.data_page_header.?.num_values);
    var tail = try filter_selection.SelectionVector.init(arena, paged.len);
    for (0..page_rows) |i| tail.set(i, false);
    const skipped = try decodeColumnTWithOptions(i32, arena, corrupt, .UNCOMPRESSED, levels, paged.len, .{
        .row_gate = &tail,
    });
    for (clean.values, skipped.values, 0..) |want, got, i| {
        try testing.expectEqual(if (i < page_rows) 0 else want, got);
    }
}

test "regression: a filtered group's dictionary keys stay codes for a fold that groups by them" {
    var dict_only = [_]schema.Encoding{.PLAIN_DICTIONARY};
    var fell_back = [_]schema.Encoding{ .PLAIN_DICTIONARY, .PLAIN };
    var path = [_][]const u8{"k"};
    const chunk = struct {
        fn of(encs: []schema.Encoding, p: *[1][]const u8) schema.ColumnChunk {
            return .{ .file_path = null, .file_offset = 0, .meta_data = .{
                .type = .INT32,
                .encodings = .{ .items = encs, .capacity = encs.len, .pointer_stability = .{} },
                .path_in_schema = .{ .items = p, .capacity = 1, .pointer_stability = .{} },
                .codec = .UNCOMPRESSED,
                .num_values = 1000,
                .total_uncompressed_size = 0,
                .total_compressed_size = 0,
                .data_page_offset = 0,
                .index_page_offset = null,
                .dictionary_page_offset = 0,
            } };
        }
    }.of;
    var columns = [_]schema.ColumnChunk{ chunk(&dict_only, &path), chunk(&dict_only, &path) };
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = 0,
        .num_rows = 1000,
    };
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 } },
    };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var table = expr_agg.GroupTable.init(std.testing.allocator, 0);
    defer table.deinit();
    const target: AggTarget = .{ .grouped = .{ .calls = &calls, .keys = &keys, .table = &table, .pos_base = 0 } };
    // ad5220d decoded a 30%-selected INT32 key as survivor values (four-byte codes for every row weighed more), and
    // the coded fold, given values, sent every row through key serialization and hashing: 4-9x slower.
    try std.testing.expect(!keyCodesCheaper(columns[0].meta_data.?, 1000, 300));
    try std.testing.expect(foldWantsCodes(target, 0, &rg));
    // One key its dictionary does not cover leaves the composite uncoded: that key's peer goes to the byte rule.
    columns[1] = chunk(&fell_back, &path);
    try std.testing.expect(!foldWantsCodes(target, 0, &rg));
}

test "a filtered group's dictionary key decodes as codes only where they materialize less than its survivors" {
    var cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 1000,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };
    // A string survivor is a slice the fold codes again: codes win once a quarter of the rows survive.
    try std.testing.expect(keyCodesCheaper(cm, 1000, 985));
    try std.testing.expect(keyCodesCheaper(cm, 1000, 250));
    try std.testing.expect(!keyCodesCheaper(cm, 1000, 249));
    // An integer survivor costs its width: four-byte codes for every row win only when every row survives.
    cm.type = .INT32;
    try std.testing.expect(keyCodesCheaper(cm, 1000, 1000));
    try std.testing.expect(!keyCodesCheaper(cm, 1000, 999));
    cm.type = .INT64;
    try std.testing.expect(keyCodesCheaper(cm, 1000, 500));
    try std.testing.expect(!keyCodesCheaper(cm, 1000, 130));
}

test "PLAIN byte-array lengths record emptiness without the payloads" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // The column encoder writes strings as delta or dictionary, never PLAIN. This page is the length-prefix layout.
    var body: std.ArrayList(u8) = .empty;
    var len = [_]u8{ 0, 0, 0, 0 };
    try body.appendSlice(arena, &len);
    std.mem.writeInt(u32, &len, 2, .little);
    try body.appendSlice(arena, &len);
    try body.appendSlice(arena, "ab");
    const header = schema.PageHeader{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{
            .num_values = 2,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var w: thrift.Writer = .init(arena);
    try header.write(&w);
    var chunk: std.ArrayList(u8) = .empty;
    try chunk.appendSlice(arena, w.bytes());
    try chunk.appendSlice(arena, body.items);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    const got = try reader.readPlainLengths(2);
    try testing.expectEqualSlices(u32, &.{ 0, 2 }, got.lengths);
    try testing.expect(got.def_levels == null);
}

fn appendV1Plain(
    arena: std.mem.Allocator,
    chunk: *std.ArrayList(u8),
    num_values: i32,
    body: []const u8,
    claimed: i32,
) !void {
    const header = schema.PageHeader{
        .type = .DATA_PAGE,
        .uncompressed_page_size = claimed,
        .compressed_page_size = @intCast(body.len),
        .crc = null,
        .data_page_header = .{
            .num_values = num_values,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var w: thrift.Writer = .init(arena);
    try header.write(&w);
    try chunk.appendSlice(arena, w.bytes());
    try chunk.appendSlice(arena, body);
}

test "PLAIN lengths and values both reject a page past the chunk's inflate cap" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // Header claims 4 KiB behind a 4-byte body. Under the cap both paths refuse before inflating it; without the
    // cap the length reader would accept a page the value reader rejects.
    var chunk: std.ArrayList(u8) = .empty;
    try appendV1Plain(arena, &chunk, 1, &[_]u8{ 0, 0, 0, 0 }, 4096);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    const options: DecodeOptions = .{ .page_bytes_limit = 32 };
    var lengths = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        options,
        null,
    );
    try testing.expectError(error.PageLargerThanChunk, lengths.readPlainLengths(1));
    var values = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        options,
        null,
    );
    var dest: [1][]const u8 = undefined;
    try testing.expectError(error.PageLargerThanChunk, values.decode(&dest));

    var encs = [_]schema.Encoding{.PLAIN};
    var path = [_][]const u8{"s"};
    const cm = schema.ColumnMetaData{
        .type = .BYTE_ARRAY,
        .encodings = .{ .items = &encs, .capacity = 1, .pointer_stability = .{} },
        .path_in_schema = .{ .items = &path, .capacity = 1, .pointer_stability = .{} },
        .codec = .UNCOMPRESSED,
        .num_values = 1,
        .total_uncompressed_size = @intCast(chunk.items.len),
        .total_compressed_size = @intCast(chunk.items.len),
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 1 };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 1,
        .created_by = null,
        .row_groups = .empty,
    };
    const in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = chunk.items, .byte_origin = 0 },
        .filter = .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "" } },
        .read = &.{true},
        .consumed = &.{false},
    };
    try testing.expectError(error.PageLargerThanChunk, plainLengthColumn(arena, in, 0, chunk.items, cm, options));
}

test "PLAIN lengths skip a leading or intermediate zero-row page and still reject an oversized count" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var ab: [8]u8 = undefined;
    std.mem.writeInt(u32, ab[0..4], 2, .little);
    @memcpy(ab[4..6], "ab");
    var chunk: std.ArrayList(u8) = .empty;
    try appendV1Plain(arena, &chunk, 0, &.{}, 0);
    try appendV1Plain(arena, &chunk, 1, ab[0..6], 6);
    try appendV1Plain(arena, &chunk, 0, &.{}, 0);
    try appendV1Plain(arena, &chunk, 1, &[_]u8{ 0, 0, 0, 0 }, 4);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    const lengths = try reader.readPlainLengths(2);
    try testing.expectEqualSlices(u32, &.{ 2, 0 }, lengths.lengths);
    var again = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    var dest: [2][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 2), try again.decode(&dest));
    try testing.expectEqualStrings("ab", dest[0]);
    try testing.expectEqualStrings("", dest[1]);

    var over: std.ArrayList(u8) = .empty;
    try appendV1Plain(arena, &over, 0, &.{}, 0);
    try appendV1Plain(arena, &over, 5, &.{}, 0);
    var bad = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        over.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    try testing.expectError(error.UnexpectedPage, bad.readPlainLengths(2));
}

/// Compaction (`compact_rows_in_tests`) decodes `keep`. A page inside those rows can still have no SQL survivor.
/// Filling it must not move the next page's values.
fn compactionAlignsNextPage(
    comptime T: type,
    arena: std.mem.Allocator,
    elem: *const schema.SchemaElement,
    col: filter_eval.Batch.Column,
    plain: []const T,
) !void {
    const path = [_][]const u8{elem.name};
    const enc = try encoder.encodeColumn(arena, .{
        .values = col,
        .schema_elem = elem,
        .path_in_schema = &path,
        .codec = .UNCOMPRESSED,
        .distinct = true,
        .page_rows = 4,
    });
    try std.testing.expect(enc.pages.len >= 2);
    const split: usize = @intCast(enc.pages[0].rows);
    try std.testing.expect(split > 0 and split < plain.len);
    try std.testing.expect(enc.pages[1].offset > 0);
    // That page is not decoded. Its last byte would fail a reader that opened it.
    enc.bytes[@as(usize, @intCast(enc.pages[1].offset)) - 1] ^= 0xff;
    const locs = try arena.alloc(schema.PageLocation, enc.pages.len);
    for (enc.pages, locs) |p, *loc| loc.* = .{
        .offset = @intCast(p.offset),
        .compressed_page_size = @intCast(p.size),
        .first_row_index = @intCast(p.first_row),
    };
    const quiet = try arena.alloc(bool, locs.len);
    @memset(quiet, false);
    const keep = [_]RowRange{.{ .lo = 0, .hi = plain.len }};
    const prune = PruningInfo{
        .locations = locs,
        .page_is_skipped = quiet,
        .page_is_always_match = quiet,
        .dictionary_page_offset = if (enc.pages[0].offset > 0) @as(?i64, 0) else null,
        .chunk_file_offset = 0,
        .col_index = .{},
        .keep = &keep,
    };
    var gate = try filter_selection.SelectionVector.init(arena, plain.len);
    defer gate.deinit();
    for (0..split) |i| gate.set(i, false);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(
        enc.bytes,
        .UNCOMPRESSED,
        levels,
        arena,
        .{ .row_gate = &gate },
        null,
    );
    const got = try decodeWithReaderPruned(T, arena, &reader, levels, plain.len, prune);
    try std.testing.expectEqual(plain.len, got.values.len);
    try std.testing.expect(got.def_levels == null);
    if (comptime T == []const u8) {
        for (got.values[0..split]) |v| try std.testing.expectEqualStrings("", v);
        for (plain[split..], got.values[split..]) |want, g| try std.testing.expectEqualStrings(want, g);
    } else {
        for (got.values[0..split]) |v| try std.testing.expectEqual(defaultVal(T), v);
        try std.testing.expectEqualSlices(T, plain[split..], got.values[split..]);
    }
}

test "compaction fills a kept required page with no SQL survivors and aligns the next page" {
    const testing = std.testing;
    const prior = compact_rows_in_tests;
    defer compact_rows_in_tests = prior;
    compact_rows_in_tests = true;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const nums = [_]i32{ 10, 11, 12, 13, 20, 21, 22, 23 };
    try compactionAlignsNextPage(i32, arena, &req_i32_elem, .{ .i32 = .{ .values = &nums } }, &nums);

    const req_ba = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "s",
        .num_children = 0,
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const strs = [_][]const u8{ "a0", "a1", "a2", "a3", "b0", "b1", "b2", "b3" };
    try compactionAlignsNextPage([]const u8, arena, &req_ba, .{ .string = .{ .values = &strs } }, &strs);
}

test "a partial record batch gathers ascending original rows" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const values = [_]i32{ 1, 2, 3, 4 };
    const cols = [_]filter_eval.Batch.Column{.{ .i32 = .{ .values = &values } }};
    const rows: filter_eval.Batch = .{ .cols = &cols, .num_rows = values.len };
    var sel = try filter_selection.SelectionVector.init(testing.allocator, values.len);
    defer sel.deinit();
    sel.set(0, false);
    sel.set(2, false);
    const Capture = struct {
        n: usize = 0,
        ords: [2]u64 = undefined,
        vals: [2]i32 = undefined,
        fn start(_: *anyopaque, _: u64, _: usize) void {}
        fn batch(ctx: *anyopaque, view: SelectedBatch, _: []const ?usize) !void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            for (0..view.batch.num_rows) |i| {
                c.ords[c.n] = try view.ordinal(i);
                c.vals[c.n] = view.batch.cols[0].i32.values[i];
                c.n += 1;
            }
        }
    };
    var cap: Capture = .{};
    var arm: RecordSink = .{ .selected = .{
        .ctx = @ptrCast(&cap),
        .startFn = Capture.start,
        .batchFn = Capture.batch,
    } };
    try emitRecords(arena, &arm, &rows, &sel, 50, 7, &.{});
    try testing.expectEqual(@as(u64, 51), cap.ords[0]);
    try testing.expectEqual(@as(u64, 53), cap.ords[1]);
    try testing.expectEqual(@as(i32, 2), cap.vals[0]);
    try testing.expectEqual(@as(i32, 4), cap.vals[1]);
}

test "fast-levels: all-present OPTIONAL column decodes identically with the flag on" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // High-cardinality so the encoder stays PLAIN rather than dict.
    var vals: [512]i32 = undefined;
    for (&vals, 0..) |*v, i| v.* = @intCast(i * 7919);
    const chunk = try flEncodePage(arena, &vals, null);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, vals.len);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        vals.len,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &vals, off.values);
    try testing.expectEqualSlices(i32, &vals, on.values);

    // The whole point: with the flag on the level array is never built.
    // `def_levels == null` alongside `max_def == 1` is how the column
    // reports "no nulls", which is exactly what the off path spells out
    // one u32 at a time.
    try testing.expect(off.def_levels != null);
    for (off.def_levels.?) |d| try testing.expectEqual(@as(u32, 1), d);
    try testing.expect(on.def_levels == null);
    try testing.expectEqual(@as(u32, 1), on.max_def);
    try testing.expect(!on.has_nulls);
}

test "fast-levels: a page with real nulls still decodes through the ordinary path" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 300 slots, every 5th one null. Values sit at their logical index
    // so a misplaced scatter is visible, not just a wrong count.
    const n = 300;
    var vals: [n]i32 = undefined;
    var def: [n]u32 = undefined;
    for (0..n) |i| {
        const is_null = (i % 5) == 0;
        def[i] = if (is_null) 0 else 1;
        vals[i] = if (is_null) 0 else @intCast(i * 31 + 1);
    }
    const chunk = try flEncodePage(arena, &vals, &def);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, n);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        n,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &vals, off.values);
    try testing.expectEqualSlices(i32, &vals, on.values);
    // Nulls are present, so the levels must be materialised either way.
    try testing.expect(on.def_levels != null);
    try testing.expectEqualSlices(u32, &def, on.def_levels.?);
    try testing.expect(on.has_nulls);
}

test "fast-levels: chunk that turns null part-way hands back to the level path" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Page 1: 256 values, all present — taken by the fast path.
    // Page 2: 256 values, half null — must stop the fast path, allocate
    //         the level array, back-fill page 1 as all-present, and
    //         resume without dropping or shifting a value.
    const per_page = 256;
    var p1: [per_page]i32 = undefined;
    for (&p1, 0..) |*v, i| v.* = @intCast(i * 7919 + 3);

    var p2: [per_page]i32 = undefined;
    var p2_def: [per_page]u32 = undefined;
    for (0..per_page) |i| {
        const is_null = (i % 2) == 1;
        p2_def[i] = if (is_null) 0 else 1;
        p2[i] = if (is_null) 0 else @intCast(1_000_000 + i * 13);
    }

    // `EncodedColumn.bytes` is documented as header-plus-data ready to
    // concatenate, which is what makes a two-page chunk buildable here.
    const page1 = try flEncodePage(arena, &p1, null);
    const page2 = try flEncodePage(arena, &p2, &p2_def);
    const chunk = try arena.alloc(u8, page1.len + page2.len);
    @memcpy(chunk[0..page1.len], page1);
    @memcpy(chunk[page1.len..], page2);

    const total = per_page * 2;
    var want_vals: [total]i32 = undefined;
    var want_def: [total]u32 = undefined;
    @memcpy(want_vals[0..per_page], &p1);
    @memcpy(want_vals[per_page..], &p2);
    @memset(want_def[0..per_page], 1);
    @memcpy(want_def[per_page..], &p2_def);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, total);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        total,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &want_vals, off.values);
    try testing.expectEqualSlices(i32, &want_vals, on.values);
    try testing.expect(on.def_levels != null);
    // Page 1's levels were never decoded from the wire — they are the
    // back-fill. If the hand-off is off by a page these are 0, not 1.
    try testing.expectEqualSlices(u32, &want_def, on.def_levels.?);
    try testing.expect(on.has_nulls);
}

test "fast-levels: all-present pages on both sides of a null page" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // The fast path only runs while it has never seen a null. Once it
    // hands off it must not resume, or the third page's levels would go
    // unwritten and read back as the zeroes nobody wrote.
    const per_page = 128;
    var present: [per_page]i32 = undefined;
    for (&present, 0..) |*v, i| v.* = @intCast(i * 104729 + 5);

    var mid: [per_page]i32 = undefined;
    var mid_def: [per_page]u32 = undefined;
    for (0..per_page) |i| {
        const is_null = i < 4;
        mid_def[i] = if (is_null) 0 else 1;
        mid[i] = if (is_null) 0 else @intCast(500 + i);
    }

    const pages = [_][]u8{
        try flEncodePage(arena, &present, null),
        try flEncodePage(arena, &mid, &mid_def),
        try flEncodePage(arena, &present, null),
    };
    var total_len: usize = 0;
    for (pages) |p| total_len += p.len;
    const chunk = try arena.alloc(u8, total_len);
    var at: usize = 0;
    for (pages) |p| {
        @memcpy(chunk[at .. at + p.len], p);
        at += p.len;
    }

    const total = per_page * 3;
    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, total);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        total,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, off.values, on.values);
    try testing.expectEqualSlices(u32, off.def_levels.?, on.def_levels.?);
    try testing.expectEqualSlices(i32, &present, on.values[per_page * 2 ..]);
    for (on.def_levels.?[per_page * 2 ..]) |d| try testing.expectEqual(@as(u32, 1), d);
}

test "regression: a BOOLEAN expression is a BOOLEAN output leaf and buffer, its 0/1 lane stored as booleans" {
    const testing = std.testing;
    var b: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BOOLEAN, .expr_type = .i64 } };
    var src: schema.SchemaElement = .{
        .type = .BOOLEAN,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "b",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    // An aliased reference keeps its column's leaf, nullable as the column is.
    const aliased = computedSchemaElem(.{ .expr = b, .alias = "k", .source = src });
    try testing.expectEqual(schema.Type.BOOLEAN, aliased.type.?);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, aliased.repetition_type.?);
    try testing.expectEqualStrings("k", aliased.name);
    src.type = .INT32;
    const always: filter_ast.Filter = .{ .and_filter = .{ .children = &.{} } };
    const whens = [_]expr_ast.Case.When{.{ .cond = always, .then = &b }};
    const case: expr_ast.Expr = .{ .case_ = .{ .whens = &whens, .else_ = null, .result_type = .i64, .depth = 1 } };
    const computed = computedSchemaElem(.{ .expr = case, .alias = "k" });
    try testing.expectEqual(schema.Type.BOOLEAN, computed.type.?);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, computed.repetition_type.?);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const vals = [_]i64{ 1, 0, 0 };
    const defs = [_]u32{ 1, 1, 0 };
    const col: filter_eval.Batch.Column = .{ .i64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
    const lane = try booleanLane(arena.allocator(), .{ .boolean = .{} }, col);
    try testing.expectEqualSlices(bool, &.{ true, false, false }, lane.boolean.values);
    try testing.expectEqualSlices(u32, &defs, lane.boolean.def_levels.?);
    // Any other buffer takes the lane as it is.
    try testing.expect((try booleanLane(arena.allocator(), .{ .i64 = .{} }, col)) == .i64);
}

test "recompressChunk: a codec round trip gives back the chunk byte for byte" {
    // tiny_pages: uncompressed v1 pages, dictionaries, no CRCs, so UNCOMPRESSED -> ZSTD -> UNCOMPRESSED must reproduce
    // every header field and payload exactly. datapage_v2: v2 pages, whose levels stay outside the compressed values.
    const testing = std.testing;
    const Case = struct { path: []const u8, via: schema.CompressionCodec };
    const cases = [_]Case{
        .{ .path = "data/parquet-testing/data/alltypes_tiny_pages.parquet", .via = .ZSTD },
        .{ .path = "data/parquet-testing/data/datapage_v2.snappy.parquet", .via = .GZIP },
    };
    for (cases) |case| {
        const bytes = metadata.readFileSlice(case.path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{case.path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);

        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const max = std.math.maxInt(usize);
        var pages: usize = 0;
        for (meta.row_groups.items) |rg| for (rg.columns.items) |chunk| {
            const cm = chunk.meta_data.?;
            const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
            const src = bytes[start..][0..@intCast(cm.total_compressed_size)];
            // Normalise to UNCOMPRESSED first, so the comparison does not depend on one compressor's output.
            const alloc = testing.allocator;
            const policy = cm.writer_stats;
            const plain = try recompressChunk(a, alloc, src, cm.codec, .UNCOMPRESSED, .{}, max, policy);
            const there = try recompressChunk(a, alloc, plain.bytes, .UNCOMPRESSED, case.via, .{}, max, policy);
            const back = try recompressChunk(a, alloc, there.bytes, case.via, .UNCOMPRESSED, .{}, max, policy);
            try testing.expectEqualSlices(u8, plain.bytes, back.bytes);
            if (cm.codec == .UNCOMPRESSED) try testing.expectEqualSlices(u8, src, plain.bytes);
            try testing.expectEqual(plain.pages.len, there.pages.len);
            // `there` was cut from `plain`, so each of its pages starts where `plain` put that page.
            for (there.pages, plain.pages) |t, p| try testing.expectEqual(p.dst_off, t.src_off);
            pages += there.pages.len;
        };
        try testing.expect(pages > 0);
    }
}

test "recompressRowGroup over several threads writes the bytes one thread writes, and fails where it fails" {
    const testing = std.testing;
    const paths = [_][]const u8{
        "data/parquet-testing/data/alltypes_tiny_pages.parquet",
        "data/parquet-testing/data/datapage_v2.snappy.parquet",
    };
    for (paths) |path| {
        const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var timings: Timings = .{};
        for (meta.row_groups.items) |*rg| for ([_]schema.CompressionCodec{ .ZSTD, .GZIP, .UNCOMPRESSED }) |codec| {
            // Every column, and every other one.
            const alternate = try a.alloc(bool, rg.columns.items.len);
            for (alternate, 0..) |*k, i| k.* = i % 2 == 0;
            for ([_]?[]const bool{ null, alternate }) |kept| {
                const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
                const one = try recompressRowGroup(a, testing.allocator, rg, src, kept, codec, .{}, &timings, 1);
                for ([_]usize{ 2, 5 }) |workers| {
                    const many =
                        try recompressRowGroup(a, testing.allocator, rg, src, kept, codec, .{}, &timings, workers);
                    try testing.expectEqual(one.len, many.len);
                    for (one, many) |x, y| {
                        try testing.expectEqual(x == null, y == null);
                        const p = x orelse continue;
                        const q = y.?;
                        try testing.expectEqualSlices(u8, p.bytes, q.bytes);
                        try testing.expectEqualSlices(PageMove, p.pages, q.pages);
                        try testing.expectEqual(p.header_delta, q.header_delta);
                    }
                }
            }
        };

        // A page body that does not decompress, in the first chunk, and a page header that does not parse, in the
        // last: one thread fails at the body first, and so must several.
        const rg = &meta.row_groups.items[0];
        if (rg.columns.items.len < 2 or rg.columns.items[0].meta_data.?.codec == .UNCOMPRESSED) continue;
        const broken = try a.dupe(u8, bytes);
        const first = rg.columns.items[0].meta_data.?;
        const first_start: usize = @intCast(first.dictionary_page_offset orelse first.data_page_offset);
        var r = thrift.Reader.init(broken[first_start..]);
        const h = try schema.PageHeader.read(&r);
        @memset(broken[first_start + r.pos ..][0..@intCast(@min(h.compressed_page_size, 16))], 0xFF);
        const last = rg.columns.items[rg.columns.items.len - 1].meta_data.?;
        const last_start: usize = @intCast(last.dictionary_page_offset orelse last.data_page_offset);
        @memset(broken[last_start..][0..4], 0xFF);
        const src: RGSrc = .{ .bytes = broken, .byte_origin = 0 };
        const want = if (recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 1)) |_|
            return error.TestExpectedError
        else |err|
            err;
        try testing.expectError(want, recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 4));
    }
}

test "recompressRowGroup over several threads reports a bad page body ahead of a later chunk with no metadata" {
    const testing = std.testing;
    const path = "data/parquet-testing/data/datapage_v2.snappy.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);
    var meta = try metadata.open(testing.allocator, bytes);
    defer meta.deinit(testing.allocator);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const rg = &meta.row_groups.items[0];
    // The first chunk's first page body does not decompress; the last chunk has no metadata.
    const broken = try a.dupe(u8, bytes);
    const m = rg.columns.items[0].meta_data.?;
    const start: usize = @intCast(m.dictionary_page_offset orelse m.data_page_offset);
    var r = thrift.Reader.init(broken[start..]);
    const h = try schema.PageHeader.read(&r);
    @memset(broken[start + r.pos ..][0..@intCast(@min(h.compressed_page_size, 16))], 0xFF);
    const last = &rg.columns.items[rg.columns.items.len - 1];
    const saved = last.meta_data;
    defer last.meta_data = saved;
    last.meta_data = null;
    var timings: Timings = .{};
    const src: RGSrc = .{ .bytes = broken, .byte_origin = 0 };
    const want = if (recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 1)) |_|
        return error.TestExpectedError
    else |err|
        err;
    try testing.expectError(want, recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 4));
}

test "decimal: nullable BYTE_ARRAY with filter dropping rows preserves definition levels and exactness" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const elem = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "balance",
        .num_children = 0,
        .converted_type = .DECIMAL,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 9 } },
        .scale = 2,
        .precision = 9,
        .field_id = null,
    };

    const raw_strs = [_][]const u8{
        "",
        &[_]u8{ 0x30, 0x39 },
        &[_]u8{ 0xEC, 0x78 },
        "",
        &[_]u8{ 0x0F, 0x42, 0x3F },
        "",
        &[_]u8{0x05},
        "",
    };
    const def_levels = [_]u32{ 0, 1, 1, 0, 1, 0, 1, 0 };
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .string = .{
            .values = &raw_strs,
            .def_levels = &def_levels,
            .max_def = 1,
        } },
        .schema_elem = &elem,
        .path_in_schema = &[_][]const u8{"balance"},
        .codec = .UNCOMPRESSED,
    });

    const kind = decimal_mod.kindFromSchema(&elem).?;
    const levels = schema.Levels{ .max_def = 1, .max_rep = 0 };

    const decoded = try decimal_mod.decodeColumnAsI128(arena, enc.bytes, .UNCOMPRESSED, levels, 8, kind);
    try testing.expectEqual(@as(usize, 8), decoded.values.len);
    try testing.expect(decoded.def_levels != null);
    try testing.expectEqualSlices(u32, &def_levels, decoded.def_levels.?);
    try testing.expectEqual(@as(i128, 0), decoded.values[0]);
    try testing.expectEqual(@as(i128, 12345), decoded.values[1]);
    try testing.expectEqual(@as(i128, -5000), decoded.values[2]);
    try testing.expectEqual(@as(i128, 0), decoded.values[3]);
    try testing.expectEqual(@as(i128, 999999), decoded.values[4]);
    try testing.expectEqual(@as(i128, 0), decoded.values[5]);
    try testing.expectEqual(@as(i128, 5), decoded.values[6]);
    try testing.expectEqual(@as(i128, 0), decoded.values[7]);

    var sel = try filter_selection.SelectionVector.init(arena, 8);
    sel.mask[0] = 0b00111100;
    const filtered = try encoder.applySelectionI128(arena, decoded, &sel);
    try testing.expectEqual(@as(usize, 4), filtered.values.len);

    var out_elem = elem;
    out_elem.type = .FIXED_LEN_BYTE_ARRAY;
    out_elem.type_length = decimal_mod.minBytesForPrecision(kind.precision);
    const re_enc = try encoder.encodeDecimalFlba(
        arena,
        filtered,
        &out_elem,
        &[_][]const u8{"balance"},
        .UNCOMPRESSED,
        .{},
    );

    const flba_kind = decimal_mod.kindFromSchema(&out_elem).?;
    const back = try decimal_mod.decodeColumnAsI128(arena, re_enc.bytes, .UNCOMPRESSED, levels, 4, flba_kind);
    try testing.expectEqual(@as(usize, 4), back.values.len);
    try testing.expect(back.def_levels != null);
    const want_defs = [_]u32{ 1, 0, 1, 0 };
    try testing.expectEqualSlices(u32, &want_defs, back.def_levels.?);
    try testing.expectEqual(@as(i128, -5000), back.values[0]);
    try testing.expectEqual(@as(i128, 0), back.values[1]);
    try testing.expectEqual(@as(i128, 999999), back.values[2]);
    try testing.expectEqual(@as(i128, 0), back.values[3]);
}

test "StagePlan.init in countOnly mode plans zero calls and charges only base columns" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var file_meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(arena, .{
        .name = "schema",
        .num_children = 2,
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(arena, .{
        .name = "k",
        .type = .INT64,
        .num_children = 0,
        .repetition_type = .REQUIRED,
        .type_length = null,
        .scale = null,
        .precision = null,
        .field_id = 0,
    });
    try file_meta.schema.append(arena, .{
        .name = "v",
        .type = .INT64,
        .num_children = 0,
        .repetition_type = .REQUIRED,
        .type_length = null,
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    var path_k: schema.StringList = .empty;
    try path_k.append(arena, "k");
    var path_v: schema.StringList = .empty;
    try path_v.append(arena, "v");

    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path_k,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path_v,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try file_meta.row_groups.append(arena, .{
        .columns = columns,
        .total_byte_size = 1600,
        .num_rows = 100,
    });

    const in: AggRowGroup = .{
        .rg = &file_meta.row_groups.items[0],
        .file_index = 0,
        .rg_index = 0,
        .meta = &file_meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, false },
    };

    var table: expr_agg.GroupTable = .init(std.testing.allocator, 1 << 20);
    defer table.deinit();
    var sketch: expr_agg.CountSketch = .{ .cells = try arena.alloc(u32, 16), .shift = 60 };
    @memset(sketch.cells, 0);
    table.sketch = &sketch;
    try std.testing.expect(table.countOnly());

    const left_expr = try arena.create(expr_ast.Expr);
    left_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const right_expr = try arena.create(expr_ast.Expr);
    right_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const arg_expr: expr_ast.Expr = .{
        .binop = .{
            .op = .add,
            .left = left_expr,
            .right = right_expr,
            .result_type = .i64,
            .depth = 2,
        },
    };

    const calls = [_]expr_agg.AggCall{.{
        .func = .sum,
        .alias = "s",
        .result = .i64,
        .arg = arg_expr,
        .where = null,
    }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const target: AggTarget = .{ .grouped = .{
        .calls = &calls,
        .keys = &keys,
        .table = &table,
        .pos_base = 0,
    } };

    const sp = try StagePlan.init(arena, in, target, &.{false});
    try std.testing.expectEqual(@as(usize, 0), sp.calls.len);
    try std.testing.expectEqual(decodeEstimate(in, sp.base, &.{}, true), sp.estimate);
}

test "StagePlan.init in countOnly mode: allocation failures cleanly unwind without leak" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            const arena = arena_state.allocator();

            var file_meta: schema.FileMetaData = .{
                .version = 1,
                .schema = .empty,
                .num_rows = 0,
                .created_by = null,
                .row_groups = .empty,
            };
            try file_meta.schema.append(arena, .{
                .name = "schema",
                .num_children = 2,
                .type = null,
                .type_length = null,
                .repetition_type = .REQUIRED,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
            try file_meta.schema.append(arena, .{
                .name = "k",
                .type = .INT64,
                .num_children = 0,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .scale = null,
                .precision = null,
                .field_id = 0,
            });
            try file_meta.schema.append(arena, .{
                .name = "v",
                .type = .INT64,
                .num_children = 0,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .scale = null,
                .precision = null,
                .field_id = 1,
            });

            var path_k: schema.StringList = .empty;
            try path_k.append(arena, "k");
            var path_v: schema.StringList = .empty;
            try path_v.append(arena, "v");

            var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
            try columns.append(arena, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = .INT64,
                    .encodings = .empty,
                    .path_in_schema = path_k,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 800,
                    .total_compressed_size = 800,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                },
            });
            try columns.append(arena, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = .INT64,
                    .encodings = .empty,
                    .path_in_schema = path_v,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 800,
                    .total_compressed_size = 800,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                },
            });
            try file_meta.row_groups.append(arena, .{
                .columns = columns,
                .total_byte_size = 1600,
                .num_rows = 100,
            });

            const in: AggRowGroup = .{
                .rg = &file_meta.row_groups.items[0],
                .file_index = 0,
                .rg_index = 0,
                .meta = &file_meta,
                .src = .{ .bytes = &.{}, .byte_origin = 0 },
                .filter = null,
                .read = &.{ true, true },
                .consumed = &.{ true, false },
            };

            var table: expr_agg.GroupTable = .init(arena, 1 << 20);
            var sketch: expr_agg.CountSketch = .{ .cells = try arena.alloc(u32, 16), .shift = 60 };
            @memset(sketch.cells, 0);
            table.sketch = &sketch;

            const left_expr = try arena.create(expr_ast.Expr);
            left_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
            const right_expr = try arena.create(expr_ast.Expr);
            right_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
            const arg_expr: expr_ast.Expr = .{
                .binop = .{
                    .op = .add,
                    .left = left_expr,
                    .right = right_expr,
                    .result_type = .i64,
                    .depth = 2,
                },
            };

            const calls = [_]expr_agg.AggCall{.{
                .func = .sum,
                .alias = "s",
                .result = .i64,
                .arg = arg_expr,
                .where = null,
            }};
            const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
            const target: AggTarget = .{ .grouped = .{
                .calls = &calls,
                .keys = &keys,
                .table = &table,
                .pos_base = 0,
            } };

            const sp = try StagePlan.init(arena, in, target, &.{false});
            try std.testing.expectEqual(@as(usize, 0), sp.calls.len);
        }
    }.run, .{});
}

test "a key chunk whose statistics claim no nulls but holds one declines the first-row histogram and decodes as codes" {
    const testing = std.testing;
    const hybrid_rle = @import("parquet/encoding/hybrid_rle.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var file_meta = schema.FileMetaData{
        .version = 2,
        .schema = .empty,
        .num_rows = 64,
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(a, .{
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "schema",
        .num_children = 1,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(a, .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "k",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    // A 4-entry dictionary; 64 leaves, the 40th null. The chunk statistics wrongly count no nulls.
    const dict = std.mem.toBytes([4]i64{ 400, 100, 300, 200 });
    var levels: [64]u32 = @splat(1);
    levels[39] = 0;
    var codes: [63]u32 = undefined;
    for (&codes, 0..) |*c, i| c.* = @intCast((i * 5 / 3) % 4);
    var body: std.ArrayList(u8) = .empty;
    const def_bytes = try hybrid_rle.encode(a, &levels, 1);
    try body.appendSlice(a, &std.mem.toBytes(@as(u32, @intCast(def_bytes.len))));
    try body.appendSlice(a, def_bytes);
    try body.append(a, 2);
    try body.appendSlice(a, try hybrid_rle.encode(a, &codes, 2));
    var chunk: std.ArrayList(u8) = .empty;
    try chunk.appendSlice(a, try pageForTest(a, .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = dict.len,
        .compressed_page_size = dict.len,
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{ .num_values = 4, .encoding = .PLAIN, .is_sorted = null },
        .data_page_header_v2 = null,
    }, &dict));
    try chunk.appendSlice(a, try pageForTest(a, .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{
            .num_values = 64,
            .encoding = .RLE_DICTIONARY,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    }, body.items));

    var path: schema.StringList = .empty;
    try path.append(a, "k");
    const cm: schema.ColumnMetaData = .{
        .type = .INT64,
        .encodings = .empty,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 64,
        .total_uncompressed_size = @intCast(chunk.items.len),
        .total_compressed_size = @intCast(chunk.items.len),
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{ .null_count = 0 },
    };
    var scratch = DecodeScratch.init(a);
    const declined = try decodeAggColumnHist(a, chunk.items, cm, &file_meta, .{ .encoded = true }, &scratch, 64, true);
    try testing.expect(declined == null);
    // The retry the scan then makes: the right codes, and the null row marked.
    const col = (try decodeAggColumnCoded(a, chunk.items, cm, &file_meta, .{ .encoded = true }, &scratch)).?;
    const got = col.i64;
    try testing.expect(got.has_nulls);
    try testing.expectEqualSlices(u32, &levels, got.def_levels.?);
    var next: usize = 0;
    for (levels, 0..) |l, r| if (l == 1) {
        try testing.expectEqual(codes[next], got.codes.?[r]);
        next += 1;
    };
    try testing.expectEqualSlices(i64, &.{ 400, 100, 300, 200 }, got.dict);
}

fn expectSameColumn(want: filter_eval.Batch.Column, got: filter_eval.Batch.Column) !void {
    const testing = std.testing;
    try testing.expectEqual(std.meta.activeTag(want), std.meta.activeTag(got));
    switch (want) {
        inline else => |w, tag| {
            const g = @field(got, @tagName(tag));
            const T = @typeInfo(@TypeOf(w.values)).pointer.child;
            try testing.expectEqual(w.values.len, g.values.len);
            if (T == []const u8) {
                for (w.values, g.values) |x, y| try testing.expectEqualStrings(x, y);
                try testing.expectEqual(w.dict.len, g.dict.len);
                for (w.dict, g.dict) |x, y| try testing.expectEqualStrings(x, y);
            } else {
                try testing.expectEqualSlices(u8, std.mem.sliceAsBytes(w.values), std.mem.sliceAsBytes(g.values));
                try testing.expectEqualSlices(u8, std.mem.sliceAsBytes(w.dict), std.mem.sliceAsBytes(g.dict));
            }
            if (w.codes) |c| try testing.expectEqualSlices(u32, c, g.codes.?) else try testing.expect(g.codes == null);
            if (w.def_levels) |d|
                try testing.expectEqualSlices(u32, d, g.def_levels.?)
            else
                try testing.expect(g.def_levels == null);
        },
    }
}

test "a pooled slice reader reuses its page and decodes the same rows" {
    const testing = std.testing;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/topn_gather_a.parquet", testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    var meta = try metadata.open(testing.allocator, bytes);
    defer meta.deinit(testing.allocator);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
    const rg = &meta.row_groups.items[1];
    var pool = SliceScratch.init(testing.allocator);
    defer pool.deinit();
    try pool.reserve(1);
    for (0..rg.columns.items.len) |ci| {
        var owned = (try SliceReader.init(a, testing.allocator, rg, &meta, src, ci, .{})).?;
        defer owned.deinit();
        const n: usize = @intCast(rg.num_rows);
        const want = try owned.next(a, n);
        pool.rewind();
        var pooled = (try SliceReader.initPooled(a, testing.allocator, rg, &meta, src, ci, .{}, &pool)).?;
        const got = try pooled.next(a, n);
        try expectSameColumn(want, got);
        const page_ptr = pooled.scratch.page.ptr;
        const page_len = pooled.scratch.page.len;
        pooled.deinit();
        pool.rewind();
        if (page_len > 0) {
            const slot = pool.checkout();
            try testing.expectEqual(page_ptr, slot.scratch.page.ptr);
            if (std.debug.runtime_safety) try testing.expectEqual(SliceScratch.reuse_poison, slot.scratch.page[0]);
            pool.rewind();
        }
        var again = (try SliceReader.initPooled(a, testing.allocator, rg, &meta, src, ci, .{}, &pool)).?;
        defer again.deinit();
        try expectSameColumn(want, try again.next(a, n));
    }
}

test "a sliced string reader under chargePages draws each page and its dictionary, and gives all back at deinit" {
    const testing = std.testing;
    // A dictionary-coded, snappy string column (`s`) of eight row groups.
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    var meta = try metadata.open(testing.allocator, bytes);
    defer meta.deinit(testing.allocator);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
    const rg = &meta.row_groups.items[0];
    const rows: usize = @intCast(rg.num_rows);
    var plain = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
    defer plain.deinit();
    const want = try plain.next(a, rows);
    var budget: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    {
        var r = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
        defer r.deinit();
        r.chargePages(testing.allocator, &budget);
        var at: usize = 0;
        while (at < rows) {
            const n = @min(rows - at, 37);
            const got = try r.next(a, n);
            for (want.string.values[at..][0..n], got.string.values) |w, g| try testing.expectEqualStrings(w, g);
            at += n;
        }
        try testing.expect(budget.peak.load(.monotonic) > 0);
    }
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    // A page the budget cannot take is refused before it is decompressed.
    var tiny: core_memory.SharedBudget = .{ .limit = 1 };
    var r = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
    defer r.deinit();
    r.chargePages(testing.allocator, &tiny);
    try testing.expectError(error.ExceededMemoryBudget, r.next(a, rows));
    try testing.expectEqual(@as(usize, 0), tiny.used.load(.monotonic));
}

test "retained delta pages reset under the decode ledger, including optional levels" {
    const testing = std.testing;
    var build = std.heap.ArenaAllocator.init(testing.allocator);
    defer build.deinit();
    const arena = build.allocator();
    // Shared prefixes so the chunk is DELTA_BYTE_ARRAY. Each page reconstructs about a mebibyte of strings, and
    // enough pages that keeping those strings would pass the 32 MiB decode ledger. Every fifth row is null.
    const value_len = 2048;
    const prefix_len = 2000;
    const per_page = (1 << 20) / (value_len + 4);
    const n = per_page * 40 * 5 / 4;
    const prefix = try arena.alloc(u8, prefix_len);
    @memset(prefix, 'p');
    const values = try arena.alloc([]const u8, n);
    const defs = try arena.alloc(u32, n);
    for (values, defs, 0..) |*v, *d, i| {
        const present = i % 5 != 0;
        d.* = @intFromBool(present);
        if (!present) {
            v.* = "";
            continue;
        }
        const buf = try arena.alloc(u8, value_len);
        @memcpy(buf[0..prefix_len], prefix);
        _ = std.fmt.bufPrint(buf[prefix_len..], "{d:0>48}", .{i}) catch unreachable;
        v.* = buf;
    }
    const elem: schema.SchemaElement = .{
        .name = "s",
        .type = .BYTE_ARRAY,
        .repetition_type = .OPTIONAL,
        .type_length = null,
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .string = .{
            .values = values,
            .def_levels = defs,
            .max_def = 1,
            .has_nulls = true,
        } },
        .schema_elem = &elem,
        .path_in_schema = &.{"s"},
        .codec = .UNCOMPRESSED,
        .distinct = true,
        .dictionary_bloom = false,
        .page_bytes = 1 << 20,
    });
    var delta = false;
    for (enc.meta.encodings.items) |e| if (e == .DELTA_BYTE_ARRAY) {
        delta = true;
    };
    try testing.expect(delta);
    try testing.expect(enc.pages.len >= 8);
    var present_bytes: usize = 0;
    for (values, defs) |v, d| if (d == 1) {
        present_bytes += v.len;
    };
    try testing.expect(present_bytes > core_memory.lean_decode);

    var file_meta = schema.FileMetaData{
        .version = 2,
        .schema = .empty,
        .num_rows = @intCast(n),
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "schema",
        .num_children = 1,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(arena, elem);
    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{ .file_path = null, .file_offset = 0, .meta_data = enc.meta });
    try file_meta.row_groups.append(arena, .{
        .columns = columns,
        .total_byte_size = @intCast(enc.bytes.len),
        .num_rows = @intCast(n),
    });

    var budget: core_memory.SharedBudget = .{ .limit = core_memory.lean_decode };
    var charged: core_memory.Charged = .{ .child = testing.allocator, .budget = &budget };
    var pool = SliceScratch.init(charged.allocator());
    var pool_live = true;
    defer if (pool_live) pool.deinit();
    try pool.reserve(1);
    const src: RGSrc = .{ .bytes = enc.bytes, .byte_origin = 0 };
    const rg = &file_meta.row_groups.items[0];
    var reader = (try SliceReader.initPooled(arena, testing.allocator, rg, &file_meta, src, 0, .{}, &pool)).?;
    var reader_live = true;
    defer if (reader_live) reader.deinit();
    const first: usize = @intCast(enc.pages[0].rows);
    var at: usize = 0;
    var after_one: usize = 0;
    const mid = n / 2;
    while (at < n) {
        const k = @min(n - at, first);
        const got = try reader.next(arena, k);
        if (at == 0) {
            try testing.expect(got.string.has_nulls);
            try testing.expectEqual(@as(u32, 1), got.string.max_def);
            try testing.expectEqual(@as(u32, 0), got.string.def_levels.?[0]);
            try testing.expectEqualStrings(values[1], got.string.values[1]);
            after_one = reader.pages.queryCapacity();
            try testing.expect(after_one > 0);
        }
        if (at <= mid and at + k > mid and defs[mid] == 1) {
            try testing.expectEqualStrings(values[mid], got.string.values[mid - at]);
        }
        at += k;
    }
    const after_all = reader.pages.queryCapacity();
    try testing.expect(after_all <= after_one * 2);
    try testing.expect(after_all < core_memory.lean_decode / 2);
    try testing.expect(reader.dict_store.?.queryCapacity() < core_memory.lean_decode / 8);
    try testing.expect(budget.used.load(.monotonic) <= core_memory.lean_decode);
    reader.deinit();
    reader_live = false;
    // Reader teardown returns its scratch to the pool; the pool owns that retained charge.
    pool.deinit();
    pool_live = false;
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

/// Columns of each lane and encoding an answer writes: near-unique integers (DELTA), repeating ones (dictionary),
/// floats and strings with NULLs, and a DECIMAL whose value at `bad_row`, when set, its INT32 cannot hold.
fn aggAnswerCols(a: std.mem.Allocator, n: usize, bad_row: ?usize) ![]expr_agg.OutputCol {
    const uniq = try a.alloc(i64, n);
    const reps = try a.alloc(i64, n);
    const fl = try a.alloc(f64, n);
    const strs = try a.alloc([]const u8, n);
    const decs = try a.alloc(i128, n);
    const defs = try a.alloc(u32, n);
    for (0..n) |i| {
        uniq[i] = @bitCast(@as(u64, i) *% 0x9E3779B97F4A7C15);
        reps[i] = @intCast(i % 37);
        fl[i] = @as(f64, @floatFromInt(i % 1000)) * 0.25;
        strs[i] = try std.fmt.allocPrint(a, "v{d}", .{i % 500});
        decs[i] = @intCast(i % 9000);
        defs[i] = @intFromBool(i % 7 != 0);
    }
    if (bad_row) |r| decs[r] = 1 << 40;
    const cols = try a.alloc(expr_agg.OutputCol, 5);
    cols[0] = .{
        .name = "u",
        .col = .{ .i64 = .{ .values = uniq, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[1] = .{
        .name = "r",
        .col = .{ .i64 = .{ .values = reps, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[2] = .{
        .name = "d",
        .col = .{ .i128 = .{ .values = decs, .max_def = 0 } },
        .parquet_type = .INT32,
        .nullable = false,
        .dec = .{ .precision = 9, .scale = 2 },
    };
    cols[3] = .{
        .name = "f",
        .col = .{ .f64 = .{ .values = fl, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .DOUBLE,
        .nullable = true,
    };
    cols[4] = .{
        .name = "s",
        .col = .{ .string = .{ .values = strs, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = true,
    };
    return cols;
}

/// Keeps what is written; fails the write numbered `fail_at` (0 is the magic), if set.
const AnswerCapture = struct {
    a: std.mem.Allocator,
    bytes: std.ArrayList(u8) = .empty,
    writes: usize = 0,
    fail_at: ?usize = null,

    fn sink(c: *AnswerCapture) streaming.Sink {
        return .{ .ctx = @ptrCast(c), .write_fn = write };
    }

    fn write(ctx: *anyopaque, b: []const u8) anyerror!void {
        const c: *AnswerCapture = @ptrCast(@alignCast(ctx));
        defer c.writes += 1;
        if (c.fail_at == c.writes) return error.TestSinkFailed;
        try c.bytes.appendSlice(c.a, b);
    }
};

test "writeAggregateRows: columns encoded on threads write the serial bytes, whatever spawns fail" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]schema.CompressionCodec{ .SNAPPY, .ZSTD, .UNCOMPRESSED }) |codec| {
        var serial: AnswerCapture = .{ .a = a };
        const want = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, codec, .{}, .{});
        try std.testing.expectEqual(@as(u64, serial.bytes.items.len), want);
        // Every column in one wave; two columns' charge a wave, so waves of two and lone columns; and spawns failing.
        const two = aggEncodeCharge(cols[0]) + aggEncodeCharge(cols[1]);
        const Run = struct { encode: AggEncode, fail_after: ?usize = null };
        for ([_]Run{
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) } },
            .{ .encode = .{ .workers = 3, .budget = two } },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 0 },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 2 },
        }) |run| {
            if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
            defer spawn_util.resetFailure();
            var par: AnswerCapture = .{ .a = a };
            const got = try writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, codec, .{}, run.encode);
            try std.testing.expectEqual(want, got);
            try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
        }
    }
}

test "writeAggregateRows: on threads, the serial error and the bytes written before it" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    // Column 2 cannot encode; write 1 (column 0) fails, or write 2 (column 1), or none does.
    const cols = try aggAnswerCols(a, n, 600);
    for ([_]?usize{ null, 1, 2 }) |fail_at| {
        var serial: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const want = writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, .{});
        var par: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const all: AggEncode = .{ .workers = 8, .budget = std.math.maxInt(u64) };
        const got = writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, all);
        try std.testing.expectError(if (fail_at == null) error.UnsupportedType else error.TestSinkFailed, want);
        try std.testing.expectEqual(want, got);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

fn writeAnswerOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        answer_rg_rows,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: one worker, or a budget no two columns fit, starts no thread" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]AggEncode{ .{ .workers = 1, .budget = std.math.maxInt(u64) }, .{ .workers = 16, .budget = 0 } }) |encode| {
        // A spawn would fail and be counted.
        spawn_util.injectFailureAfter(0);
        defer spawn_util.resetFailure();
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRows(a, out.sink(), cols, n, .SNAPPY, .{}, encode);
        try std.testing.expectEqual(@as(usize, 0), spawn_util.failuresInjected());
    }
}

test "writeAggregateRows: a column encoded alone still draws from the ledger" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 1000, null);
    // One column, and few rows: no waves, but the column is the ledger's to refuse.
    var ledger: expr_agg.SharedBudget = .{ .limit = 1 };
    var out: AnswerCapture = .{ .a = a };
    const encode: AggEncode = .{ .workers = 16, .budget = std.math.maxInt(u64), .ledger = &ledger };
    const one = writeAggregateRows(a, out.sink(), cols[0..1], 1000, .SNAPPY, .{}, encode);
    try std.testing.expectError(error.ExceededMemoryBudget, one);
    ledger.limit = 1 << 30;
    _ = try writeAggregateRows(a, out.sink(), cols, 1000, .SNAPPY, .{}, encode);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "writeAggregateRows: on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerOnThreads, .{});
}

/// The bytes between a written answer's leading magic and its footer: its column chunks, in order.
fn answerChunks(a: std.mem.Allocator, file: []const u8) ![]const u8 {
    const meta = try answerFooter(a, file);
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var end = file.len - 8 - footer_len;
    for (meta.row_groups.items) |rg| for (rg.columns.items) |c| {
        if (c.meta_data) |cm| {
            if (cm.bloom_filter_offset) |off| end = @min(end, @as(usize, @intCast(off)));
        }
    };
    return file[4..end];
}

fn answerFooter(a: std.mem.Allocator, file: []const u8) !schema.FileMetaData {
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var reader = thrift.Reader.init(file[file.len - 8 - footer_len .. file.len - 8]);
    return schema.FileMetaData.read(a, &reader);
}

test "writeAggregateRows: big answers use fixed row groups with serial chunks at every worker count" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    const rg = 300;
    const cols = try aggAnswerCols(a, n, null);
    var serial: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, rg, .SNAPPY, .{}, .{});
    const meta = try answerFooter(a, serial.bytes.items);
    try std.testing.expectEqual(@as(i64, n), meta.num_rows);
    try std.testing.expectEqual(@as(usize, 4), meta.row_groups.items.len);
    for (meta.row_groups.items, [_]i64{ 300, 300, 300, 100 }) |g, rows| {
        try std.testing.expectEqual(rows, g.num_rows);
        try std.testing.expectEqual(cols.len, g.columns.items.len);
    }
    // Row group g's chunks are what its rows encode to as an answer of their own, in order.
    var want: std.ArrayList(u8) = .empty;
    for (0..4) |g| {
        const lo = g * rg;
        const hi = @min(n, lo + rg);
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, lo, hi);
        var alone: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, alone.sink(), part, @intCast(hi - lo), rg, .SNAPPY, .{}, .{});
        try want.appendSlice(a, try answerChunks(a, alone.bytes.items));
    }
    try std.testing.expectEqualSlices(u8, want.items, try answerChunks(a, serial.bytes.items));
    // The same file whatever the workers, the budget's waves, or failing spawns.
    const two = aggEncodeCharge(sliceOutputCol(cols[0], 0, rg)) + aggEncodeCharge(sliceOutputCol(cols[1], 0, rg));
    const Run = struct { encode: AggEncode, fail_after: ?usize = null };
    for ([_]Run{
        .{ .encode = .{ .workers = 16, .budget = std.math.maxInt(u64) } },
        .{ .encode = .{ .workers = 3, .budget = two } },
        .{ .encode = .{ .workers = 7, .budget = std.math.maxInt(u64) }, .fail_after = 1 },
    }) |run| {
        if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        var par: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, par.sink(), cols, n, rg, .SNAPPY, .{}, run.encode);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

test "writeAggregateRows: an answer of a row group's rows or fewer is one row group" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 300, null);
    for ([_]usize{ 0, 1, 300 }) |n| {
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, 0, n);
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, out.sink(), part, @intCast(n), 300, .SNAPPY, .{}, .{});
        const meta = try answerFooter(a, out.bytes.items);
        try std.testing.expectEqual(@as(usize, 1), meta.row_groups.items.len);
        try std.testing.expectEqual(@as(i64, @intCast(n)), meta.row_groups.items[0].num_rows);
    }
}

fn writeRowGroupsOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        128,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: row groups on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeRowGroupsOnThreads, .{});
}

fn writeAnswerWindows(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cols = try aggAnswerCols(alloc, 1000, null);
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), cols, 1000, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    for ([_][2]usize{ .{ 0, 600 }, .{ 600, 1000 } }) |range| {
        const part = try alloc.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(
            &file,
            part,
            @intCast(range[1] - range[0]),
            300,
            .SNAPPY,
            .{},
            .{ .workers = 4, .budget = 1 << 30 },
        );
    }
    _ = try file.finish(cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
}

test "writeAggregateRows: bounded windows write the whole answer's bytes and free every failed allocation" {
    try writeAnswerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerWindows, .{});
}

fn cachedIntegerWindows(a: std.mem.Allocator) !void {
    var budget: expr_agg.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const all = try aggAnswerCols(alloc, 1201, null);
    var cols = [_]expr_agg.OutputCol{ all[0], all[1], all[0] };
    cols[2].name = "s";
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), &cols, 1201, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    const encode: AggEncode = .{ .workers = 11, .budget = 128 << 20, .ledger = &budget };
    try file.reserveIntegerWorkspace(encode);
    try std.testing.expectEqual(@as(usize, 12 * answer_rg_rows * 12 * 3), file.cached_charge);
    try std.testing.expectEqual(file.cached_charge, budget.used.load(.monotonic));
    for ([_][2]usize{ .{ 0, 1200 }, .{ 1200, 1201 } }) |range| {
        var part: [3]expr_agg.OutputCol = undefined;
        for (cols, &part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(&file, &part, @intCast(range[1] - range[0]), 300, .SNAPPY, .{}, encode);
    }
    _ = try file.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
    // A later wider shape returns every retained arena before drawing its ordinary per-wave workspace.
    file.releaseWorkspace();
    var fallback: AnswerCapture = .{ .a = alloc };
    var wider = try AggregateFile.init(alloc, fallback.sink());
    defer wider.deinit();
    try wider.reserveIntegerWorkspace(encode);
    wider.cached_unit_charge = 1;
    try appendAggregateRows(&wider, &cols, 1201, 300, .SNAPPY, .{}, encode);
    _ = try wider.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, fallback.bytes.items);
    try std.testing.expectEqual(@as(usize, 0), wider.cached_charge);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "writeAggregateRows: retained integer encode scratch stays charged through windows and unwinds on failure" {
    try cachedIntegerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cachedIntegerWindows, .{});
}

fn checkPartitionWindowScratch(a: std.mem.Allocator, n: usize, coded: bool) !void {
    const strings = try a.alloc([]const u8, n);
    defer a.free(strings);
    const text = try a.alloc([96]u8, n);
    defer a.free(text);
    for (strings, text, 0..) |*s, *buf, i| {
        s.* = try std.fmt.bufPrint(buf, "a distinct long key whose bytes must outlive grouping window {d:0>8}", .{i});
    }
    const ints = try a.alloc(i64, n);
    defer a.free(ints);
    for (ints, 0..) |*v, i| v.* = @intCast(i);
    const codes = try a.alloc(u32, n);
    defer a.free(codes);
    for (codes, 0..) |*code, i| code.* = @intCast(i);
    var cols = [_]filter_eval.Batch.Column{
        .{ .string = .{
            .values = if (coded) &.{} else strings,
            .dict = if (coded) strings else &.{},
            .codes = if (coded) codes else null,
            .max_def = 0,
        } },
        .{ .i64 = .{ .values = ints, .max_def = 0 } },
    };
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const Merge = struct {
        fn count(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, _: std.mem.Allocator) !void {
            dst.count += src.count;
        }
    };
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var part = try partagg.PartAgg.init(a, &budget, &calls, 0, Merge.count, 4, 4, 1, true);
    defer part.deinit();
    part.compact = true;
    try std.testing.expect(try part.admitPrivate(n, 100));
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    table.compact.configure(&calls);
    defer table.deinit();
    var local: partagg.Local = .{ .runs = &part.runs[0] };
    var sel = try filter_selection.SelectionVector.init(a, n);
    defer sel.deinit();
    var scratch_budget: expr_agg.SharedBudget = .{ .limit = 512 << 10 };
    var charged: @import("memory.zig").Charged = .{ .child = a, .budget = &scratch_budget };
    var arena = RowGroupArena.init(charged.allocator());
    defer arena.deinit();
    const target: AggTarget = .{ .grouped = .{
        .calls = &calls,
        .keys = &keys,
        .table = &table,
        .pos_base = 0,
        .part = &part,
        .local = &local,
    } };
    try foldGroupedWindows(
        arena.allocator(),
        a,
        target,
        &cols,
        &.{ 0, 1 },
        &sel,
        &.{ coded, false },
        coded,
        null,
        null,
        null,
    );
    try part.flush(&local, &table);
    try part.foldRuns(1);
    var groups: usize = 0;
    for (part.parts) |*p| {
        groups += p.table.groupCount();
        for (0..p.table.groupCount()) |g| {
            var tmp: [16]expr_agg.Accumulator = undefined;
            try std.testing.expectEqual(@as(u64, 1), p.table.readGroup(g, 1, &tmp)[0].count);
            const k = p.table.keyAt(g);
            const id = std.mem.readInt(i64, k[k.len - 8 ..][0..8], .little);
            try std.testing.expectEqual(@as(u64, @intCast(id)), p.table.first.items[g]);
            const len = std.mem.readInt(u32, k[1..5], .little);
            try std.testing.expectEqualStrings(strings[@intCast(id)], k[5..][0..len]);
        }
    }
    try std.testing.expectEqual(@as(usize, n), groups);
}

test "partition windows reuse charged scratch instead of retaining a row group's windows" {
    try checkPartitionWindowScratch(std.testing.allocator, 32_768, false);
}

test "partition window scratch keeps materialized dictionaries live across resets" {
    try checkPartitionWindowScratch(std.testing.allocator, 8192, true);
}

test "partition window scratch returns every charge on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkPartitionWindowScratch, .{ 32, false });
}

test "cloneColumnMeta keeps every statistics field, the bounds' exactness among them" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const st: schema.Statistics = .{
        .max = "b",
        .min = "a",
        .null_count = 3,
        .distinct_count = 7,
        .max_value = "y",
        .min_value = "x",
        .is_max_value_exact = false,
        .is_min_value_exact = true,
        .nan_count = 2,
    };
    const cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 10,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = st,
    };
    const got = (try cloneColumnMeta(a, cm)).statistics.?;
    inline for (@typeInfo(schema.Statistics).@"struct".field_names) |name| {
        const want = @field(st, name);
        const have = @field(got, name);
        if (@TypeOf(want) == ?[]const u8) {
            try std.testing.expectEqualStrings(want.?, have.?);
        } else try std.testing.expectEqual(want, have);
    }
}

test "plainBitsPrefix: every range's bits are plainBits' over the same rows, whatever the lanes and NULLs" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 257;
    var agg: OutputAggregator = .{
        .arena = a,
        .cols = try a.alloc(OutputAggregator.ColumnBuf, 4),
        .schema_elems = try a.alloc(schema.SchemaElement, 4),
        .paths = &.{},
        .num_rows = n,
        .captured = true,
    };
    const base: schema.SchemaElement = .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    for (agg.schema_elems) |*e| e.* = base;
    agg.schema_elems[1].type = .BYTE_ARRAY;
    agg.schema_elems[2].type = .BYTE_ARRAY;
    agg.schema_elems[3].type = .BOOLEAN;
    agg.cols[0] = .{ .i64 = .{} };
    agg.cols[1] = .{ .string = .{} };
    agg.cols[2] = .{ .string = .{ .coded = true, .dict = &.{ "x", "yyyy", "" }, .max_def = 1 } };
    agg.cols[3] = .{ .boolean = .{ .max_def = 1 } };
    for (0..n) |r| {
        try agg.cols[0].i64.values.append(a, @intCast(r));
        try agg.cols[1].string.values.append(a, "abcdefghij"[0 .. r % 11]);
        try agg.cols[2].string.codes.append(a, @intCast(r % 3));
        try agg.cols[2].string.def_levels.append(a, @intFromBool(r % 4 != 0));
        try agg.cols[3].boolean.values.append(a, r % 2 == 0);
        try agg.cols[3].boolean.def_levels.append(a, @intFromBool(r % 5 != 0));
    }
    const prefix = try plainBitsPrefix(a, &agg);
    try testing.expectEqual(@as(usize, n + 1), prefix.len);
    for ([_][2]usize{ .{ 0, n }, .{ 0, 0 }, .{ 3, 4 }, .{ 17, 200 }, .{ 100, n } }) |lohi| {
        var want: u64 = 0;
        for (agg.cols, agg.schema_elems) |*c, *e| want += plainBits(c, e, lohi[0], lohi[1]);
        try testing.expectEqual(want, prefix[lohi[1]] - prefix[lohi[0]]);
    }
}

test "canonical decode failure checks every competing column and unwinds allocation failures" {
    var storage = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer storage.deinit();
    const a = storage.allocator();
    const header: schema.PageHeader = .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = 4,
        .compressed_page_size = 4,
        .crc = null,
        .data_page_header = .{
            .num_values = 300,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var writer = thrift.Writer.init(a);
    try header.write(&writer);
    const bytes = try std.mem.concat(a, u8, &.{ writer.bytes(), &.{ 255, 255, 255, 255 } });
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 2;
    elements[1].name = "a";
    elements[2].name = "b";
    var paths = [_][1][]const u8{ .{"a"}, .{"b"} };
    var columns: [2]schema.ColumnChunk = undefined;
    for (&columns, &paths) |*c, *path| c.* = .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT32,
            .encodings = .empty,
            .path_in_schema = .{ .items = path, .capacity = 1, .pointer_stability = .{} },
            .codec = .UNCOMPRESSED,
            .num_values = 300,
            .total_uncompressed_size = @intCast(bytes.len),
            .total_compressed_size = @intCast(bytes.len),
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    };
    var groups = [_]schema.RowGroup{.{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = @intCast(bytes.len * 2),
        .num_rows = 300,
    }};
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 3, .pointer_stability = .{} },
        .num_rows = 300,
        .row_groups = .{ .items = &groups, .capacity = 1, .pointer_stability = .{} },
        .created_by = null,
    };
    const in: AggRowGroup = .{
        .rg = &groups[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
        .error_ranks = .{ .ctx = &meta, .make = struct {
            fn make(_: *const anyopaque, alloc: std.mem.Allocator, n: usize) anyerror![]const usize {
                const ranks = try alloc.alloc(usize, n);
                for (ranks, 0..) |*rank, i| rank.* = i;
                return ranks;
            }
        }.make },
    };
    // Probe OOM is intentionally absorbed: exhaust every allocation site and assert cleanup/diagnostic preservation.
    var count: usize = 0;
    while (count < 64) : (count += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = count });
        const failure = canonicalDecodeFailure(
            failing.allocator(),
            in,
            .{},
            &.{},
            true,
            .{ .err = error.UnexpectedPage, .site = .{ .rg = in.rg, .column = 1, .page = 0 } },
        );
        try std.testing.expectEqual(error.UnexpectedPage, failure.err);
        try std.testing.expect(failure.site.column == 0 or failure.site.column == 1);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        if (!failing.has_induced_failure) {
            try std.testing.expectEqual(@as(usize, 0), failure.site.column);
            break;
        }
    }
    try std.testing.expect(count < 64);
    var ledger: core_memory.SharedBudget = .{ .limit = 1024 };
    var charged: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &ledger };
    const original: decode_order.Failure = .{
        .err = error.UnexpectedPage,
        .site = .{ .rg = in.rg, .column = 1, .page = 0 },
    };
    const bounded = decode_order.canonical(SliceReader, isDecodeError, charged.allocator(), in, .{}, &.{ false, false }, original);
    try std.testing.expectEqual(original.err, bounded.err);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    try std.testing.expect(ledger.peak.load(.monotonic) <= ledger.limit);
    // A marker whose failure cannot be reproduced never outranks a real page, even at the same row.
    columns[0].meta_data.?.num_values = 0;
    const reproduced = decode_order.canonical(SliceReader, isDecodeError, std.testing.allocator, in, .{}, &.{ false, false }, .{ .err = error.UnexpectedPage, .site = .{ .rg = in.rg, .column = 0, .page = 0 } });
    try std.testing.expectEqual(@as(usize, 1), reproduced.site.column);
}

test "cache decode admission counts inputs and DAG, follows sharing, and releases failed planning allocations" {
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 1;
    elements[1].type = .DOUBLE;
    var path = [_][]const u8{"v"};
    var columns = [_]schema.ColumnChunk{.{ .file_path = null, .file_offset = 0, .meta_data = .{
        .type = .DOUBLE,
        .encodings = .empty,
        .path_in_schema = .{ .items = &path, .capacity = 1, .pointer_stability = .{} },
        .codec = .UNCOMPRESSED,
        .num_values = 65553,
        .total_uncompressed_size = 65553 * 8,
        .total_compressed_size = 65553 * 8,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    } }};
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 1, .pointer_stability = .{} },
        .total_byte_size = 65553 * 8,
        .num_rows = 65553,
    };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 2, .pointer_stability = .{} },
        .num_rows = 65553,
        .created_by = null,
        .row_groups = .empty,
    };
    const in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = null,
        .read = &.{true},
        .consumed = &.{true},
    };
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    const sum: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    var call = [_]expr_agg.AggCall{.{ .func = .sum, .arg = sum, .result = .f64, .where = null, .alias = "s" }};
    const target: AggTarget = .{ .scalar = .{ .calls = &call, .accumulators = &.{} } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator, input: AggRowGroup, t: AggTarget) !void {
            const n = try automaticCacheRows(a, input, t, 16);
            try std.testing.expect(n >= 64 and n < 65553 and n % 64 == 0);
        }
    }.run, .{ in, target });
    const small = try automaticCacheRows(std.testing.allocator, in, target, 16);
    hw.test_cpu.?.l2.bytes = 1280 << 10;
    const larger = try automaticCacheRows(std.testing.allocator, in, target, 16);
    try std.testing.expect(larger > small);
    hw.test_cpu.?.l3.bytes = 16 << 20;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 1));
    try std.testing.expect(try automaticCacheRows(std.testing.allocator, in, target, 16) > 0);
    hw.test_cpu.?.l3.bytes = 0;
    hw.test_cpu.?.l2.bytes = 0;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 16));
    hw.test_cpu.?.l2.bytes = 1 << 20;
    call[0].arg = x;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 16));
}

test "cache decode admission bounds dictionary strings and composite domains from every page header" {
    const testing = std.testing;
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fixture = struct {
        fn bytes(alloc: std.mem.Allocator, entries: i32, width: i32, last: schema.Encoding) ![]const u8 {
            var w = thrift.Writer.init(alloc);
            var h: schema.PageHeader = .{
                .type = .DICTIONARY_PAGE,
                .uncompressed_page_size = width,
                .compressed_page_size = 1,
                .crc = null,
                .data_page_header = null,
                .data_page_header_v2 = null,
                .dictionary_page_header = .{ .num_values = entries, .encoding = .PLAIN, .is_sorted = null },
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            h.type = .DATA_PAGE;
            h.uncompressed_page_size = 1;
            h.dictionary_page_header = null;
            h.data_page_header = .{
                .num_values = 32768,
                .encoding = .RLE_DICTIONARY,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            h.type = .DATA_PAGE_V2;
            h.data_page_header = null;
            h.data_page_header_v2 = .{
                .num_values = 32785,
                .num_nulls = 0,
                .num_rows = 32785,
                .encoding = last,
                .definition_levels_byte_length = 0,
                .repetition_levels_byte_length = 0,
                .is_compressed = true,
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            return w.bytes(); // Bodies deliberately aren't decoded by this admission-only hint.
        }
    };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 2;
    elements[1].type = .DOUBLE;
    elements[1].name = "x";
    elements[2].type = .BYTE_ARRAY;
    elements[2].name = "g";
    var paths = [_][1][]const u8{ .{"x"}, .{"g"} };
    var columns: [2]schema.ColumnChunk = undefined;
    for (&columns, &paths, 0..) |*c, *path, i| c.* = .{
        .file_path = null,
        .file_offset = 1000,
        .meta_data = .{
            .type = if (i == 0) .DOUBLE else .BYTE_ARRAY,
            .encodings = .empty,
            .path_in_schema = .{ .items = path, .capacity = 1, .pointer_stability = .{} },
            .codec = .ZSTD,
            .num_values = 65553,
            .total_uncompressed_size = 65553 * 8,
            .total_compressed_size = 0,
            .data_page_offset = 1000,
            .index_page_offset = null,
            .dictionary_page_offset = if (i == 1) 1000 else null,
        },
    };
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = 65553 * 8,
        .num_rows = 65553,
    };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 3, .pointer_stability = .{} },
        .num_rows = rg.num_rows,
        .created_by = null,
        .row_groups = .empty,
    };
    var in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 1000 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
    };
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    const computed: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = computed, .result = .f64, .where = null, .alias = "s" }};
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    var target: AggTarget = .{ .grouped = .{ .calls = &calls, .keys = &.{key}, .table = undefined, .pos_base = 0 } };
    var small: usize = 0;
    for ([_]i32{ 24, 4000, 2 << 20 }) |width| {
        const bytes = try Fixture.bytes(a, 2, width, .RLE_DICTIONARY);
        in.src.bytes = bytes;
        columns[1].meta_data.?.total_compressed_size = @intCast(bytes.len);
        const proof = cacheDictionary(in, 1).?;
        var funding: core_memory.SharedBudget = .{ .limit = 64 << 10 };
        var charge: core_memory.Charged = .{ .child = testing.allocator, .budget = &funding };
        var memo: CacheHints = .{ .allocator = charge.allocator() };
        try memo.initShards(3);
        try testing.expectEqualDeep(proof, memo.get(in, 1).?);
        try testing.expectEqualDeep(proof, memo.get(in, 1).?);
        try testing.expectEqual(@as(usize, 1), memo.count());
        memo.deinit();
        try testing.expectEqual(@as(usize, 0), funding.used.load(.monotonic));
        var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
        var uncached: CacheHints = .{ .allocator = failing.allocator() };
        try testing.expectError(error.OutOfMemory, uncached.initShards(3));
        try testing.expectEqual(@as(usize, 0), uncached.shards.len);
        try testing.expectEqualDeep(proof, uncached.get(in, 1).?);
        try testing.expectEqual(@as(usize, 0), uncached.count());
        uncached.deinit();

        try testing.expectEqual(@as(usize, @intCast(width)), proof.bytes);
        const rows = try automaticCacheRows(testing.allocator, in, target, 16);
        if (width == 24) {
            small = rows;
            try testing.expect(small >= 64 and small < 65553);
        } else try testing.expect(rows < small);
        if (width == 2 << 20) try testing.expectEqual(@as(usize, 0), rows);
    }
    // Every data page must qualify, including V2 after an eligible V1 page.
    in.src.bytes = try Fixture.bytes(a, 2, 24, .PLAIN);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    try testing.expect(cacheDictionary(in, 1) == null);
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    in.src.bytes = try Fixture.bytes(a, 256, 4096, .RLE_DICTIONARY);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    // Two individually bounded keys can still have a composite table much larger than L2.
    target.grouped.keys = &.{ key, key };
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    target.grouped.keys = &.{key};
    in.src.bytes = try Fixture.bytes(a, 257, 24, .RLE_DICTIONARY);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    columns[1].meta_data.?.dictionary_page_offset = -1;
    try testing.expect(cacheDictionary(in, 1) == null);
}

test "scalar cache DAG shares nullable inputs without changing canonical sum blocks, including allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testScalarCache, .{});
}

fn testScalarCache(backing: std.mem.Allocator) !void {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 193;
    const values = try a.alloc(f64, n);
    const defs = try a.alloc(u32, n);
    for (values, defs, 0..) |*v, *d, i| {
        v.* = switch (i % 6) {
            0 => 1e16,
            1 => 1,
            2 => -1e16,
            3 => 0.1,
            4 => -0.0,
            else => std.math.nan(f64),
        };
        d.* = @intFromBool(i % 17 != 0);
    }
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    var base: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    var repeated = base;
    const square: expr_ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &base,
        .right = &repeated,
        .result_type = .f64,
        .depth = 3,
    } };
    const calls = [_]expr_agg.AggCall{
        .{ .func = .sum, .arg = base, .result = .f64, .where = null, .alias = "s" },
        .{ .func = .sum, .arg = square, .result = .f64, .where = null, .alias = "ss" },
        .{ .func = .avg, .arg = base, .result = .avg_f64, .where = null, .alias = "a" },
        .{ .func = .count, .arg = base, .result = .i64, .where = null, .alias = "n" },
        .{ .func = .min, .arg = base, .result = .f64, .where = null, .alias = "lo" },
    };
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{
        .values = values,
        .def_levels = defs,
        .max_def = 1,
        .has_nulls = true,
    } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    for ([_]bool{ false, true }) |empty| {
        var sel = try filter_selection.SelectionVector.init(a, n);
        for (0..n) |i| sel.set(i, !empty and i % 3 != 1);
        var want: [calls.len]expr_agg.Accumulator = undefined;
        var got: [calls.len]expr_agg.Accumulator = undefined;
        for (calls, &want, &got) |call, *w, *g| {
            w.* = .init(call);
            g.* = .init(call);
            try expr_agg.updateOne(a, a, w, call, &batch, &.{0}, &sel);
        }
        var off: usize = 0;
        while (off < n) : (off += 64) {
            const count = @min(64, n - off);
            const col = try cols[0].sliceRows(off, count);
            const part: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = count };
            const mask = try sel.alignedWindow(off, count);
            try testing.expect(try foldScalarCache(
                a,
                a,
                &calls,
                &got,
                &@as([calls.len]bool, @splat(false)),
                &part,
                &.{0},
                &mask,
            ));
        }
        for (want, got) |w, g| switch (w) {
            .sum_f, .min_f => |expected| {
                const actual = if (g == .sum_f) g.sum_f else g.min_f;
                try testing.expectEqual(expected != null, actual != null);
                if (expected) |v| try testing.expectEqual(@as(u64, @bitCast(v)), @as(u64, @bitCast(actual.?)));
            },
            .avg => |avg| {
                try testing.expectEqual(avg.count, g.avg.count);
                try testing.expectEqual(@as(u64, @bitCast(avg.sum)), @as(u64, @bitCast(g.avg.sum)));
            },
            .count => |count| try testing.expectEqual(count, g.count),
            else => unreachable,
        };
    }
}

test "writeAggregateRows preserves the automatic Bloom policy for small and parallel answers" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    for ([_]usize{ 1000, 70000 }) |n| {
        const cols = try aggAnswerCols(a, n, null);
        for ([_]bool{ false, true }) |enabled| for ([_]usize{ 1, 3 }) |workers| {
            var capture: AnswerCapture = .{ .a = a };
            _ = try writeAggregateRows(a, capture.sink(), cols, @intCast(n), .SNAPPY, .{}, .{
                .dictionary_bloom = enabled,
                .workers = workers,
                .budget = std.math.maxInt(u64),
            });
            const meta = try answerFooter(a, capture.bytes.items);
            const repeated = meta.row_groups.items[0].columns.items[1].meta_data.?;
            try std.testing.expect(repeated.dictionary_page_offset != null);
            try std.testing.expectEqual(enabled, repeated.bloom_filter_offset != null);
        };
    }
}

test "copied INT96 dictionary filters hash raw entries and decline noncanonical times" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const bloom = @import("parquet/bloom.zig");
    var elem: schema.SchemaElement = .{
        .name = "t",
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = int96_mod.INT96_WIDTH,
        .repetition_type = .REQUIRED,
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var raw: [2][12]u8 = @splat(@splat(0));
    for (&raw) |*v| std.mem.writeInt(u32, v[8..12], 2440588, .little);
    var values: [256][]const u8 = undefined;
    for (&values, 0..) |*v, i| v.* = &raw[i % 2];
    for ([_]bool{ false, true }) |noncanonical| {
        std.mem.writeInt(i64, raw[1][0..8], if (noncanonical) std.time.ns_per_day else 2_000_000_000, .little);
        const encoded = try encoder.encodeColumn(a, .{
            .values = .{ .string = .{ .values = &values } },
            .schema_elem = &elem,
            .path_in_schema = &.{"t"},
            .dictionary_bloom = false,
        });
        var cm = encoded.meta;
        cm.type = .INT96;
        var root = elem;
        root.name = "root";
        root.type = null;
        root.type_length = null;
        root.num_children = 1;
        var meta: schema.FileMetaData = .{
            .version = 2,
            .schema = .empty,
            .num_rows = values.len,
            .created_by = schema.ZPQ_CREATED_BY,
            .row_groups = .empty,
        };
        try meta.schema.append(a, root);
        var leaf = elem;
        leaf.type = .INT96;
        leaf.type_length = null;
        try meta.schema.append(a, leaf);
        const filter = try dictionaryBloom(a, encoded.bytes, cm, .{ .max_def = 0, .max_rep = 0 }, &meta);
        if (noncanonical) {
            try std.testing.expect(filter == null);
        } else {
            const bits = bloom.bitset(filter.?).?;
            for (raw) |v| try std.testing.expect(bloom.Probe.init(bloom.hashBytes(&v)).mightContain(bits));
        }
    }
}

test "grouped key dictionaries keep later-key codes with and without dense lanes" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var ints: [256]i32 = undefined;
    var names: [256][]const u8 = undefined;
    for (&ints, &names, 0..) |*k, *v, i| {
        k.* = @intCast(i % 2);
        v.* = try std.fmt.allocPrint(a, "dictionary-value-{d}", .{i % 64});
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "a", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &ints } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = false, .col = .{ .string = .{ .values = &names } } },
    };
    var capture: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRows(a, capture.sink(), &cols, ints.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, capture.bytes.items);
    const rg = &meta.row_groups.items[0];
    const src: RGSrc = .{ .bytes = capture.bytes.items, .byte_origin = 0 };
    var lanes: agg_dense.Lanes = undefined;
    var target: AggTarget = .{ .grouped = .{
        .calls = &.{},
        .keys = &.{
            .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
            .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        },
        .dense = &lanes,
        .table = undefined,
        .pos_base = 0,
    } };
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    try testing.expect(!target.codedKey(2));
    target.grouped.dense = null;
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    const in: AggRowGroup = .{
        .rg = rg,
        .meta = &meta,
        .src = src,
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
    };
    try testing.expect(cacheDictionary(in, 1) != null);
    try testing.expect(!streamByteArray(in, target, 1));
    try testing.expectEqual(std.math.maxInt(usize), streamWindowRows(in, target, ints.len));
    const cm = rg.columns.items[1].meta_data.?;
    const start: usize = @intCast(cm.dictionary_page_offset.?);
    const chunk = capture.bytes.items[start..][0..@intCast(cm.total_compressed_size)];
    for ([_]bool{ false, true }) |force| {
        const options: DecodeOptions = .{ .encoded = true, .any_dictionary = force and target.codedKey(1) };
        var scratch = DecodeScratch.init(testing.allocator);
        defer scratch.deinit();
        const col = (try decodeAggColumnCoded(a, chunk, cm, &meta, options, &scratch)).?;
        try testing.expectEqual(force, col.codes() != null);
        if (!force) continue;
        for ([_]usize{ 1, 17, 64, 256 }) |step| {
            var reader = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 1, options)).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(testing.allocator);
            defer slice.deinit();
            var done: usize = 0;
            while (done < names.len) {
                _ = slice.reset(.free_all);
                const got = try reader.next(slice.allocator(), step);
                const codes = got.codes().?;
                try testing.expect(got.string.dict.len > names.len / 8);
                for (codes, 0..) |code, i|
                    try testing.expectEqualStrings(names[done + i], got.string.dict[code]);
                done += codes.len;
            }
        }
    }
    // A slice spanning the dictionary/PLAIN boundary must gather its prefix without rereading pages.
    var mixed: std.ArrayList(u8) = .empty;
    var coded_body: std.ArrayList(u8) = .empty;
    try coded_body.append(a, 1);
    try coded_body.appendSlice(a, try @import("parquet/encoding/hybrid_rle.zig").encode(a, &.{ 0, 1, 0, 1 }, 1));
    const dict_values = [_]i32{ 10, 20 };
    const plain_values = [_]i32{ 30, 40, 50, 60 };
    const bodies = [_][]const u8{ std.mem.asBytes(&dict_values), coded_body.items, std.mem.asBytes(&plain_values) };
    for (bodies, 0..) |body, i| {
        var w = thrift.Writer.init(a);
        const h: schema.PageHeader = .{
            .type = if (i == 0) .DICTIONARY_PAGE else .DATA_PAGE,
            .uncompressed_page_size = @intCast(body.len),
            .compressed_page_size = @intCast(body.len),
            .crc = null,
            .data_page_header_v2 = null,
            .data_page_header = if (i == 0) null else .{
                .num_values = 4,
                .encoding = if (i == 1) .RLE_DICTIONARY else .PLAIN,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = if (i == 0) .{ .num_values = 2, .encoding = .PLAIN, .is_sorted = null } else null,
        };
        try h.write(&w);
        try mixed.appendSlice(a, w.bytes());
        try mixed.appendSlice(a, body);
    }
    var mixed_rg = rg.*;
    mixed_rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
    const mixed_cm = &mixed_rg.columns.items[0].meta_data.?;
    mixed_cm.codec = .UNCOMPRESSED;
    mixed_cm.dictionary_page_offset = 0;
    mixed_cm.data_page_offset = 0;
    mixed_cm.total_compressed_size = @intCast(mixed.items.len);
    mixed_cm.num_values = 8;
    const expected = [_]i32{ 10, 20, 10, 20, 30, 40, 50, 60 };
    for ([_]usize{ 1, 3, 5, 8 }) |step| {
        var r = (try SliceReader.init(
            a,
            testing.allocator,
            &mixed_rg,
            &meta,
            .{ .bytes = mixed.items, .byte_origin = 0 },
            0,
            .{ .encoded = true, .any_dictionary = true },
        )).?;
        defer r.deinit();
        var done: usize = 0;
        while (done < expected.len) {
            const c = (try r.next(a, step)).i32;
            const n = if (c.codes) |codes| codes.len else c.values.len;
            for (0..n) |i| try testing.expectEqual(
                expected[done + i],
                if (c.codes) |codes| c.dict[codes[i]] else c.values[i],
            );
            done += n;
        }
    }
    const Trial = struct {
        fn run(gpa: std.mem.Allocator, bytes: []const u8, m: *const schema.FileMetaData) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const alloc = arena.allocator();
            var reader = (try SliceReader.init(
                alloc,
                gpa,
                &m.row_groups.items[0],
                m,
                .{ .bytes = bytes, .byte_origin = 0 },
                1,
                .{ .encoded = true, .any_dictionary = true },
            )).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(gpa);
            defer slice.deinit();
            while (reader.left != 0) {
                _ = slice.reset(.free_all);
                const c = try reader.next(slice.allocator(), 17);
                try std.testing.expect(c.codes() != null);
            }
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Trial.run, .{ capture.bytes.items, &meta });
}

test "filter-only and hash-only strings stream a cache-sized window, other strings do not" {
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 1 } };
    var enc_plain = [_]schema.Encoding{ .PLAIN, .RLE, .RLE_DICTIONARY };
    var enc_delta = [_]schema.Encoding{ .PLAIN, .DELTA_BYTE_ARRAY };
    var path = [_][]const u8{"k"};
    var columns = [1]schema.ColumnChunk{.{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .BYTE_ARRAY,
            .encodings = .{ .items = &enc_plain, .capacity = enc_plain.len, .pointer_stability = .{} },
            .path_in_schema = .{ .items = &path, .capacity = 1, .pointer_stability = .{} },
            .codec = .SNAPPY,
            .num_values = 100_000,
            .total_uncompressed_size = 130 * 100_000,
            .total_compressed_size = 40 * 1000,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    }};
    const group = schema.RowGroup{
        .columns = .{ .items = &columns, .capacity = 1, .pointer_stability = .{} },
        .total_byte_size = 40 * 1000,
        .num_rows = 1000,
    };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].num_children = 1;
    elements[1].name = "k";
    elements[1].type = .BYTE_ARRAY;
    elements[1].repetition_type = .REQUIRED;
    var groups = [_]schema.RowGroup{group};
    const meta = schema.FileMetaData{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 2, .pointer_stability = .{} },
        .num_rows = 1000,
        .row_groups = .{ .items = &groups, .capacity = 1, .pointer_stability = .{} },
        .created_by = null,
    };
    const filter: filter_ast.Filter = .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "x" } };
    const in = AggRowGroup{
        .rg = &groups[0],
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = filter,
        .read = &.{true},
        .consumed = &.{false},
    };
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const scalar_filter: AggTarget = .{ .scalar = .{ .calls = &.{}, .accumulators = &.{} } };
    try std.testing.expect(streamByteArray(in, scalar_filter, 0));
    const rows = streamWindowRows(in, scalar_filter, 100_000);
    // 130 bytes of string per row into 1 MiB of L2. The window is that many rows, not the chunk.
    try std.testing.expectEqual(@as(usize, (1 << 20) / 130), rows);
    // Regression (9bcb030 on reads-sf1 B6_string_eq, 2x slower): a filter-only string whose dictionary covers the
    // chunk keeps the whole-chunk coded decode, its predicate evaluated per dictionary entry.
    var enc_dict = [_]schema.Encoding{ .PLAIN_DICTIONARY, .RLE };
    columns[0].meta_data.?.encodings = .{ .items = &enc_dict, .capacity = enc_dict.len, .pointer_stability = .{} };
    try std.testing.expect(!streamByteArray(in, scalar_filter, 0));
    try std.testing.expectEqual(std.math.maxInt(usize), streamWindowRows(in, scalar_filter, 100_000));
    columns[0].meta_data.?.encodings = .{ .items = &enc_plain, .capacity = enc_plain.len, .pointer_stability = .{} };

    const distinct: expr_agg.AggCall = .{
        .func = .count_distinct,
        .arg = key,
        .where = null,
        .alias = "d",
        .result = .i64,
    };
    var no_filter = in;
    no_filter.filter = null;
    no_filter.consumed = &.{true};
    const scalar_distinct: AggTarget = .{ .scalar = .{ .calls = &.{distinct}, .accumulators = &.{} } };
    try std.testing.expect(streamByteArray(no_filter, scalar_distinct, 0));

    var table: expr_agg.GroupTable = undefined;
    const grouped: AggTarget = .{ .grouped = .{
        .calls = &.{},
        .keys = &.{key},
        .table = &table,
        .pos_base = 0,
    } };
    try std.testing.expect(streamByteArray(no_filter, grouped, 0));
    var other = key;
    other.col_ref.col_idx = 1;
    for ([_]@FieldType(expr_agg.AggCall, "func"){ .count_distinct, .min, .max }) |func| {
        var with_payload = grouped;
        const call: expr_agg.AggCall = .{
            .func = func,
            .arg = other,
            .where = null,
            .alias = "payload",
            .result = if (func == .count_distinct) .i64 else .bytes,
        };
        with_payload.grouped.calls = &.{call};
        try std.testing.expect(hashOnlyByteArray(with_payload, 0));
        try std.testing.expect(!streamByteArray(no_filter, with_payload, 0));
    }

    const min_call: expr_agg.AggCall = .{
        .func = .min,
        .arg = key,
        .where = null,
        .alias = "m",
        .result = .bytes,
    };
    const scalar_min: AggTarget = .{ .scalar = .{ .calls = &.{min_call}, .accumulators = &.{} } };
    try std.testing.expect(!streamByteArray(no_filter, scalar_min, 0));

    var delta = columns[0].meta_data.?;
    delta.encodings = .{ .items = &enc_delta, .capacity = enc_delta.len, .pointer_stability = .{} };
    columns[0].meta_data = delta;
    try std.testing.expect(!streamByteArray(in, scalar_filter, 0));
    try std.testing.expectEqual(std.math.maxInt(usize), streamWindowRows(in, scalar_filter, 100_000));
}
