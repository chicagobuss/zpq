const std = @import("std");
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

