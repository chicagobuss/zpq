const std = @import("std");
const expr_ast = @import("../expr/ast.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_agg = @import("../expr/agg.zig");
const SelectedBatch = @import("../selected_batch.zig").SelectedBatch;
const profile = @import("../../profile.zig");
const schema = @import("../schema.zig");
const page_mod = @import("../parquet/page.zig");
const Diag = @import("../diag.zig").Diag;
const Text = @import("../diag.zig").Text;

/// Error set the consumer functions can return. Inferred from the
/// underlying decoders/encoders/sinks; unioned here for documentation.
/// (The actual function signatures use `anyerror` because `sink.write`
/// is opaque and may surface I/O errors we don't enumerate at this
/// layer.)
pub const Error = error{
    ColumnMetaMissing,
    SchemaLookupFailed,
    MissingChunkBytes,
    MissingDecodedColumn,
    UnsupportedColumnType,
    ShortDecode,
    InvalidColumnOffsets,
    BadColumnIndex,
};

/// Whole-RG byte window. The consumer slices per-column from this:
///
///     col_off = col.data_page_offset - byte_origin
///     chunk   = bytes[col_off .. col_off + total_compressed_size]
///
/// `byte_origin` is the absolute offset (in the source file) where
/// `bytes[0]` lives. CLI passes `byte_origin = 0` and `bytes = file_bytes`;
/// lambda passes `byte_origin = rg_byte_start` and `bytes = rg_result.raw_bytes`.
pub const RGSrc = struct {
    bytes: []const u8,
    byte_origin: u64,
};

/// Per-phase wall-clock counters in nanoseconds. Both consumers
/// accumulate into the caller-supplied struct so multi-RG totals
/// add up naturally.
pub const Timings = struct {
    decode_ns: u64 = 0,
    eval_ns: u64 = 0,
    encode_ns: u64 = 0,
    sink_ns: u64 = 0,
    /// A row output's rows read (in row groups not pruned or passed over) and kept by its filter, for the summary.
    /// Counted by the drivers, not by `ProjectedRG`, which a top-N runs twice over a row group it keeps rows of.
    rows_scanned: u64 = 0,
    rows_matched: u64 = 0,
    /// The accumulating thread's profile buffer; spans reuse the timestamps these counters are taken from. A Timings
    /// accumulates on one thread at a time, which is what makes handing it the buffer safe.
    prof: ?*profile.Thread = null,
};

/// The column chunk this thread last began to decode. A marker, not a scope: set as each chunk (or slice) starts, and
/// read only once a decode error has ended the query, so it names where that error was raised. A worker hands its own
/// to the thread that reports the error (`decodeFailure`, `setDecodeFailure`); `resolveDecodeFailure` turns it into
/// the file, row group, column path and page of the message.
const decode_order = @import("../decode_order.zig");
pub const DecodeSite = decode_order.Site;
threadlocal var decode_site: ?DecodeSite = null;

/// Mark `column` of `rg` as this thread's chunk. Its page is unknown until a page begins, so a slice reader that
/// re-marks its column per slice, interleaved with other columns' readers, never reports another column's page.
pub fn enterChunk(rg: *const schema.RowGroup, column: usize) void {
    decode_site = .{ .rg = rg, .column = column };
    page_mod.last_page = null;
    page_mod.last_encoding = null;
}

/// This thread's decode site, with the page it last began, for a worker to hand over with its error.
pub fn decodeFailure() ?DecodeSite {
    var site = decode_site orelse return null;
    site.page = page_mod.last_page;
    site.encoding = page_mod.last_encoding;
    return site;
}

/// Adopt a worker's failure site as this thread's; null clears it (a query's start).
pub fn setDecodeFailure(site: ?DecodeSite) void {
    decode_site = site;
    page_mod.last_page = if (site) |s| s.page else null;
    page_mod.last_encoding = if (site) |s| s.encoding else null;
}

/// After a failure ended a query over `metas`, whose `inputs` each have a `name`: this thread's decode site as the
/// file, row group, leaf path and page of `diag`'s `decode` detail. Nothing over another detail, or for a site outside
/// these inputs (one left from an earlier query on the thread: compared by address only, never followed).
pub fn resolveDecodeFailure(diag: ?*Diag, metas: []const schema.FileMetaData, inputs: anytype) void {
    const d = diag orelse return;
    if (d.detail != .none) return;
    const site = decodeFailure() orelse return;
    // By the row group's address in a file's row-group list, which a decoder's copy of the footer shares.
    const at = @intFromPtr(site.rg);
    var file: usize = 0;
    const rg = while (file < metas.len) : (file += 1) {
        const rgs = metas[file].row_groups.items;
        const lo = @intFromPtr(rgs.ptr);
        if (at >= lo and at < lo + rgs.len * @sizeOf(schema.RowGroup)) break (at - lo) / @sizeOf(schema.RowGroup);
    } else return;
    var path: Text = .{};
    var leaves: schema.LeafIterator = .init(metas[file].schema.items);
    while (leaves.next()) |leaf| if (leaf.index == site.column) {
        for (leaf.path, 0..) |seg, i| {
            if (i > 0) path.append(".");
            path.append(seg);
        }
        break;
    };
    d.detail = .{ .decode = .{
        .input = inputs[file].name,
        .row_group = rg,
        .column = path,
        .page = site.page,
        .encoding = site.encoding,
    } };
}

/// Errors raised by reading a column chunk's pages, whose message names where: the corrupt or unsupported encodings
/// a page can hold, not the query's own failures (overflow, memory) that a decode site would mislabel.
pub fn isDecodeError(err: anyerror) bool {
    const names = [_][]const u8{
        "UnexpectedPage",       "DictionaryMissing",          "UnexpectedDictionaryPage", "ShortDecode",
        "DefLevelsMismatch",    "BadPageHeader",              "BadPageCount",             "DecompressionFailed",
        "SizeMismatch",         "NegativeSize",               "MissingChunkBytes",        "UnexpectedEndOfChunk",
        "LevelOutOfRange",      "IntOutOfRange",              "PageLargerThanChunk",      "UnsupportedEncoding",
        "UnsupportedPageType",  "PageValueCountExceedsChunk", "DictionaryLargerThanPage", "UnsupportedCodec",
        "InvalidColumnOffsets", "ColumnMetaMissing",
    };
    for (names) |n| if (std.mem.eql(u8, n, @errorName(err))) return true;
    return false;
}

/// Per-RG result. `surviving_rows == 0` means the filter dropped
/// every row — in that case `rg` is null and the caller MUST NOT
/// append it to the output's row-group list.
pub const RGOut = struct {
    surviving_rows: i64,
    rg: ?schema.RowGroup,
};

/// Shift every absolute file offset in a row group's column metadata by
/// `base`. Used by the row-group-parallel write path: each RG is encoded
/// into its own buffer with offsets relative to 0, then placed at its
/// final file position `base` and rebased here. Mirrors exactly the
/// `+= out_offset.*` arithmetic `encodeAggregator` does inline when it
/// writes straight to the sink.
pub fn rebaseRowGroupOffsets(rg: *schema.RowGroup, base: i64) void {
    for (rg.columns.items) |*col| {
        col.file_offset += base;
        if (col.column_index_offset) |v| col.column_index_offset = v + base;
        if (col.offset_index_offset) |v| col.offset_index_offset = v + base;
        if (col.meta_data) |*md| {
            md.data_page_offset += base;
            if (md.dictionary_page_offset) |dp| md.dictionary_page_offset = dp + base;
            if (md.index_page_offset) |ip| md.index_page_offset = ip + base;
        }
    }
}


/// One row group as `scanRGForAgg` reads it. Everything is borrowed.
pub const RowSubgroup = struct { group: *const @import("../row_subgroups.zig").Group, rows: @import("column.zig").RowRange };

pub const AggRowGroup = struct {
    subgroup: ?RowSubgroup = null,
    rg: *const schema.RowGroup,
    /// Input file and row-group index, for profile spans only.
    file_index: usize = 0,
    rg_index: usize = 0,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    /// The outer filter; null when there is none or statistics prove every row passes it.
    filter: ?filter_ast.Filter,
    /// Projected input-column ranks for decode-error ties; null keeps read-mask order.
    error_order: ?[]const usize = null,
    error_ranks: ?struct {
        ctx: *const anyopaque,
        make: *const fn (*const anyopaque, std.mem.Allocator, usize) anyerror![]const usize,
    } = null,
    /// The full query projection when workers split a row group by aggregate calls.
    error_read: ?[]const bool = null,
    error_calls: ?[]const expr_agg.AggCall = null,
    /// Columns to decode (`agg_plan.Columns.forRowGroup`).
    read: []const bool,
    /// Columns whose values an aggregate argument, a per-aggregate FILTER or a GROUP BY key consumes
    /// (`agg_plan.Columns.consumed`); any other column in `read` only the outer filter reads.
    consumed: []const bool,
    /// Columns that must decode their stored values (`agg_plan.Columns.preserve`). Null preserves every column.
    preserve: ?[]const bool = null,
    /// Original global row of this row group's first row, including pruned rows. Zero until the scan sets the
    /// file prefix. A slice adds its own offset; this is not the packed first-position tag.
    row_base: u64 = 0,
};

/// Borrowed owner view of one decoded window. `input_rows` are indexes into that window. The shared adapter
/// checks lengths and ordinal arithmetic; a gather index past `window_rows` is rejected here. A window that
/// does not fit in `u32` cannot be gathered.
pub fn selectedWindow(
    batch: *const filter_eval.Batch,
    selected: ?*const filter_selection.SelectionVector,
    input_base: u64,
    input_rows: ?[]const u32,
    window_rows: usize,
) error{BadColumn}!SelectedBatch {
    if (input_rows) |rows| {
        if (window_rows > std.math.maxInt(u32)) return error.BadColumn;
        const limit: u32 = @intCast(window_rows);
        for (rows) |row| if (row >= limit) return error.BadColumn;
    }
    const view: SelectedBatch = .{
        .batch = batch,
        .selected = selected,
        .input_base = input_base,
        .input_rows = input_rows,
    };
    try view.validate();
    return view;
}

test "a selected window keeps original indexes and rejects a gather past the window" {
    const batch: filter_eval.Batch = .{ .cols = &.{}, .num_rows = 2 };
    const open = try selectedWindow(&batch, null, 40, &.{ 1, 3 }, 4);
    try std.testing.expectEqual(@as(u64, 43), try open.ordinal(1));
    try std.testing.expectError(error.BadColumn, selectedWindow(&batch, null, 40, &.{ 1, 4 }, 4));
    const slice = try selectedWindow(&batch, null, 40, null, 2);
    try std.testing.expectEqual(@as(u64, 40), try slice.ordinal(0));
}


/// The output leaf for a computed column. Shared by the row buffers (whose printing reads it) and the footer.
pub fn computedSchemaElem(c: OutputCol.Computed) schema.SchemaElement {
    const expr_type = c.expr.typeOf();
    // A computed column that can be NULL (`strlen` of a nullable column) must say so, or its NULLs misdescribe it.
    const repetition: schema.FieldRepetitionType = if (c.expr.mayBeNull() and c.expr != .col_ref) .OPTIONAL else .REQUIRED;
    if (expr_type == .dec) {
        var elem: schema.SchemaElement = .{
            .type = null,
            .type_length = null,
            .repetition_type = repetition,
            .name = c.alias,
            .num_children = 0,
            .converted_type = null,
            .logical_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        };
        decimalElem(&elem, c.expr.decSpec().?);
        return elem;
    }
    if (c.source) |src| if (c.expr == .col_ref and !schema.isFloat16(src) and (src.type == .BYTE_ARRAY or
        src.type == .FIXED_LEN_BYTE_ARRAY or src.type == .INT64 or src.type == .DOUBLE or src.type == .BOOLEAN))
    {
        var renamed = src;
        renamed.name = c.alias;
        renamed.num_children = 0;
        return renamed;
    };
    // An aliased UINT64 reference carries the column's raw bits through the i64 lane; keep the annotation so they
    // are printed and written as the unsigned values they are.
    const unsigned_64 = c.expr == .col_ref and c.expr.col_ref.unsigned_64;
    return .{
        // A BOOLEAN expression's 0/1 lane is written and printed as the BOOLEAN SQL types it.
        .type = if (c.expr.isBoolean()) .BOOLEAN else expr_type.toParquet(),
        .type_length = null,
        .repetition_type = repetition,
        .name = c.alias,
        .num_children = 0,
        .converted_type = if (expr_type == .str) .UTF8 else if (unsigned_64) .UINT_64 else null,
        .logical_type = if (expr_type == .str)
            .{ .STRING = .{} }
        else if (unsigned_64)
            .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } }
        else
            null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
}

/// One output column. `passthrough` re-encodes a kept input column;
/// `computed` evaluates an expression against the (post-filter) decoded
/// batch and encodes the result as a new flat leaf.
pub const OutputCol = union(enum) {
    passthrough: Passthrough,
    computed: Computed,

    /// A kept input leaf, written as it is. With `name` (a SELECT alias, or a nested leaf's dotted label) it is the
    /// same leaf renamed and flat: its type, annotation and NULLs unchanged, its definition levels mapped to 0/1.
    pub const Passthrough = struct {
        ci: usize,
        name: ?[]const u8 = null,
    };

    pub const Computed = struct {
        expr: expr_ast.Expr,
        /// Output column name. The caller's footer-build step uses
        /// the same string as the leaf SchemaElement's name.
        alias: []const u8,
        /// A renamed reference retains its source annotation when the value lane is unchanged.
        source: ?schema.SchemaElement = null,
    };
};

/// Type `elem` as DECIMAL(`d`) at the physical width its precision takes.
pub fn decimalElem(elem: *schema.SchemaElement, d: expr_ast.Dec) void {
    elem.type = d.physical();
    elem.type_length = if (d.physical() == .FIXED_LEN_BYTE_ARRAY) 16 else null;
    elem.converted_type = .DECIMAL;
    elem.logical_type_raw = &.{};
    elem.logical_type = .{ .DECIMAL = .{ .scale = d.scale, .precision = d.precision } };
    elem.scale = d.scale;
    elem.precision = d.precision;
}

test "computedSchemaElem keeps the unsigned annotation of an aliased UINT64 reference" {
    const ref: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
        .unsigned_64 = true,
    } };
    const elem = computedSchemaElem(.{ .expr = ref, .alias = "b" });
    try std.testing.expect(schema.isUnsignedInt64(elem));
    try std.testing.expectEqualStrings("b", elem.name);

    var signed = ref;
    signed.col_ref.unsigned_64 = false;
    try std.testing.expect(!schema.isUnsignedInt(computedSchemaElem(.{ .expr = signed, .alias = "b" })));
}


/// `elem`, a kept leaf's schema element, as the flat leaf `name` that a renamed passthrough writes: the same type and
/// annotation, OPTIONAL exactly when the leaf can be NULL at its depth (`max_def`), whatever its own repetition says.
pub fn renameLeaf(elem: *schema.SchemaElement, name: []const u8, max_def: i32) void {
    elem.name = name;
    elem.num_children = 0;
    elem.repetition_type = if (max_def > 0) .OPTIONAL else .REQUIRED;
}

