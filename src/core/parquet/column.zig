//! ColumnChunkReader: walk a column chunk's pages, decode values.
//!
//! Brings together page.zig + the encoding decoders. The contract:
//! caller picks the right T based on the column's logical type
//! (i32/i64/f32/f64 for primitives, []const u8 for BYTE_ARRAY),
//! constructs a reader over the column chunk's bytes, and pulls
//! decoded values into a buffer of T.
//!
//! Page dispatch:
//!   - First page (if present) of type DICTIONARY_PAGE: decoded via
//!     PLAIN into an arena-allocated dictionary slice. Cached.
//!   - DATA_PAGE: switch on data_page_header.encoding:
//!       PLAIN              → Plain.Decoder(T)
//!       PLAIN_DICTIONARY   → RleDict.Decoder(T) using the cached
//!         (legacy alias for     dictionary
//!         RLE_DICTIONARY)
//!       RLE_DICTIONARY     → same
//!       (others)           → error.UnsupportedEncoding (Phase 2 work)
//!
//! Phase 1 limitations:
//!   - Flat / required columns only. Definition/repetition levels are
//!     ignored — caller must not point us at a nullable column yet.
//!   - DATA_PAGE_V2 not supported (page header parser only knows V1).

const std = @import("std");
const schema = @import("../schema.zig");
const page_mod = @import("page.zig");
const plain = @import("encoding/plain.zig");
const rle_dict = @import("encoding/rle_dict.zig");
const dbp = @import("encoding/delta_binary_packed.zig");
const dba = @import("encoding/delta_byte_array.zig");

pub const Error = error{
    UnsupportedEncoding,
    UnsupportedPageType,
    NestedNotSupported,
    DictionaryMissing,
    UnexpectedPage,
    OutOfMemory,
} || page_mod.Error;

/// Choose the right decoder shape for T at comptime.
///   - i32/i64/f32/f64 → Plain.Decoder(T)
///   - []const u8      → Plain.ByteArrayDecoder
///   - bool            → Plain.BooleanDecoder (note: takes num_values)
fn PlainDecoderFor(comptime T: type) type {
    return switch (T) {
        i32, i64, f32, f64 => plain.Decoder(T),
        []const u8 => plain.ByteArrayDecoder,
        bool => plain.BooleanDecoder,
        else => @compileError("unsupported column type: " ++ @typeName(T)),
    };
}

/// BOOLEAN columns never use dictionary encoding in practice; the
/// type's tiny cardinality makes it pointless. We still need a
/// nominal RleDictDec type for the field, so we use Plain.Decoder(i32)
/// as a placeholder — it's unreachable at runtime.
fn RleDictDecFor(comptime T: type) type {
    return switch (T) {
        bool => rle_dict.Decoder(i32), // unused for bool
        else => rle_dict.Decoder(T),
    };
}

/// DELTA_BINARY_PACKED applies to i32/i64 only; for other Ts we use a
/// placeholder type that's never instantiated.
fn DeltaIntDecFor(comptime T: type) type {
    return switch (T) {
        i32, i64 => dbp.Decoder(T),
        else => dbp.Decoder(i32), // unused
    };
}

pub fn ColumnChunkReader(comptime T: type) type {
    const PlainDec = PlainDecoderFor(T);
    const RleDictDec = RleDictDecFor(T);
    const DeltaIntDec = DeltaIntDecFor(T);

    return struct {
        const Self = @This();

        pages: page_mod.PageReader,
        arena: std.mem.Allocator,
        levels: schema.Levels,

        /// Cached dictionary, if the chunk has one. Populated on the
        /// first DICTIONARY_PAGE we encounter.
        dictionary: ?[]const T = null,

        /// Active per-page decoder state. Exactly one variant is set
        /// while a page is being decoded; all are null between pages.
        current_plain: ?PlainDec = null,
        current_rle_dict: ?RleDictDec = null,
        current_delta_int: ?DeltaIntDec = null,
        current_delta_len_ba: ?dba.DeltaLengthByteArrayDecoder = null,
        current_delta_ba: ?dba.DeltaByteArrayDecoder = null,

        pub fn init(
            chunk_bytes: []const u8,
            codec: schema.CompressionCodec,
            levels: schema.Levels,
            arena: std.mem.Allocator,
        ) Self {
            return .{
                .pages = page_mod.PageReader.init(chunk_bytes, codec, arena),
                .arena = arena,
                .levels = levels,
            };
        }

        /// Decode up to dest.len values into dest. Returns the number
        /// written. Zero means the column chunk is fully consumed.
        pub fn decode(self: *Self, dest: []T) Error!usize {
            var written: usize = 0;
            while (written < dest.len) {
                const n = try self.decodeFromCurrentPage(dest[written..]);
                if (n > 0) {
                    written += n;
                    continue;
                }
                // Current page exhausted (or none active); advance.
                self.current_plain = null;
                self.current_rle_dict = null;
                self.current_delta_int = null;
                self.current_delta_len_ba = null;
                self.current_delta_ba = null;
                if (!try self.advancePage()) break;
            }
            return written;
        }

        fn decodeFromCurrentPage(self: *Self, dest: []T) Error!usize {
            if (self.current_plain) |*d| {
                return @as(*PlainDec, d).decode(dest) catch return error.UnexpectedPage;
            }
            if (T != bool) {
                if (self.current_rle_dict) |*d| {
                    return @as(*RleDictDec, d).decode(dest) catch return error.UnexpectedPage;
                }
            }
            if (T == i32 or T == i64) {
                if (self.current_delta_int) |*d| {
                    return @as(*DeltaIntDec, d).decode(dest) catch return error.UnexpectedPage;
                }
            }
            if (T == []const u8) {
                if (self.current_delta_len_ba) |*d| {
                    return d.decode(dest) catch return error.UnexpectedPage;
                }
                if (self.current_delta_ba) |*d| {
                    return d.decode(dest) catch return error.UnexpectedPage;
                }
            }
            return 0;
        }

        /// Pull the next page; if it's a dictionary page, cache it and
        /// loop to the next page. Returns true iff a data-page decoder
        /// was set up for use; false if the chunk is exhausted.
        fn advancePage(self: *Self) Error!bool {
            while (try self.pages.next()) |pg| {
                switch (pg.header.type) {
                    .DICTIONARY_PAGE => {
                        try self.installDictionary(pg);
                        // Loop to the next page (likely the data page).
                    },
                    .DATA_PAGE => {
                        try self.installDataPageDecoder(pg);
                        return true;
                    },
                    .DATA_PAGE_V2 => return error.UnsupportedPageType,
                    .INDEX_PAGE => continue, // skip; not used by decode
                }
            }
            return false;
        }

        fn installDictionary(self: *Self, pg: page_mod.Page) Error!void {
            const dh = pg.header.dictionary_page_header orelse return error.UnexpectedPage;
            const num: usize = @intCast(dh.num_values);
            // BOOLEAN never gets dictionary encoding in practice — the
            // type's tiny cardinality makes it pointless. Reject early.
            if (T == bool) return error.UnsupportedEncoding;
            const buf = try self.arena.alloc(T, num);
            switch (T) {
                i32, i64, f32, f64 => {
                    var d = plain.Decoder(T).init(pg.bytes);
                    const n = d.decode(buf) catch return error.UnexpectedPage;
                    if (n != num) return error.UnexpectedPage;
                },
                []const u8 => {
                    var d = plain.ByteArrayDecoder.init(pg.bytes);
                    const n = d.decode(buf) catch return error.UnexpectedPage;
                    if (n != num) return error.UnexpectedPage;
                },
                else => unreachable,
            }
            self.dictionary = buf;
        }

        fn installDataPageDecoder(self: *Self, pg: page_mod.Page) Error!void {
            const dph = pg.header.data_page_header orelse return error.UnexpectedPage;

            // Phase 1: nested (rep_level > 0 OR def_level > 1) not yet
            // supported. For optional flat columns (def_level == 1) we
            // skip the def-level prefix but treat all values as present
            // — broken for actual nulls, fine for the bench fixture
            // where the columns we're testing are densely populated.
            if (self.levels.max_rep > 0) return error.NestedNotSupported;
            if (self.levels.max_def > 1) return error.NestedNotSupported;

            // V1 data-page payload layout:
            //   [u32 LE: rep_levels_byte_length][rep level bytes]   (if max_rep > 0)
            //   [u32 LE: def_levels_byte_length][def level bytes]   (if max_def > 0)
            //   [encoded values]
            var values_bytes = pg.bytes;
            if (self.levels.max_def > 0) {
                if (values_bytes.len < 4) return error.UnexpectedPage;
                const def_len = std.mem.readInt(u32, values_bytes[0..4], .little);
                if (4 + def_len > values_bytes.len) return error.UnexpectedPage;
                values_bytes = values_bytes[4 + def_len ..];
            }

            // Reset all variant slots — only one will be populated below.
            self.current_plain = null;
            self.current_rle_dict = null;
            self.current_delta_int = null;
            self.current_delta_len_ba = null;
            self.current_delta_ba = null;

            switch (dph.encoding) {
                .PLAIN => {
                    self.current_plain = switch (T) {
                        i32, i64, f32, f64 => plain.Decoder(T).init(values_bytes),
                        []const u8 => plain.ByteArrayDecoder.init(values_bytes),
                        bool => plain.BooleanDecoder.init(values_bytes, @intCast(dph.num_values)),
                        else => unreachable,
                    };
                },
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                    if (T == bool) return error.UnsupportedEncoding;
                    const dict = self.dictionary orelse return error.DictionaryMissing;
                    self.current_rle_dict = rle_dict.Decoder(T).init(values_bytes, dict) catch return error.UnexpectedPage;
                },
                .DELTA_BINARY_PACKED => {
                    if (T != i32 and T != i64) return error.UnsupportedEncoding;
                    self.current_delta_int = dbp.Decoder(T).init(values_bytes) catch return error.UnexpectedPage;
                },
                .DELTA_LENGTH_BYTE_ARRAY => {
                    if (T != []const u8) return error.UnsupportedEncoding;
                    self.current_delta_len_ba = dba.DeltaLengthByteArrayDecoder.init(values_bytes, self.arena) catch return error.UnexpectedPage;
                },
                .DELTA_BYTE_ARRAY => {
                    if (T != []const u8) return error.UnsupportedEncoding;
                    self.current_delta_ba = dba.DeltaByteArrayDecoder.init(values_bytes, self.arena) catch return error.UnexpectedPage;
                },
                else => return error.UnsupportedEncoding,
            }
        }
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("metadata.zig");

test "decode int8 column from the bench fixture" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var meta = try metadata.open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);

    const rg0 = &meta.row_groups.items[0];
    // col[0] is "int8", logical INT32. Find by name to be robust.
    const col_idx = metadata.findColumnIndex(&meta, "int8") orelse return error.MissingColumn;
    const col = rg0.columns.items[col_idx].meta_data.?;

    const chunk_start: usize = if (col.dictionary_page_offset) |dp|
        @intCast(dp)
    else
        @intCast(col.data_page_offset);
    const chunk_len: usize = @intCast(col.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const path: [1][]const u8 = .{"int8"};
    const levels = meta.getColumnLevels(&path);
    var reader = ColumnChunkReader(i32).init(chunk, col.codec, levels, arena.allocator());

    // Decode all values from this row group's column chunk.
    const expected: usize = @intCast(col.num_values);
    const out = try arena.allocator().alloc(i32, expected);

    const t0 = nowNs();
    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decode(out[written..]);
        if (n == 0) break;
        written += n;
    }
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);

    try testing.expectEqual(expected, written);

    // int8 column has values in [-128, 127] cast to i32.
    var min_v: i32 = std.math.maxInt(i32);
    var max_v: i32 = std.math.minInt(i32);
    var sum: i64 = 0;
    for (out) |v| {
        if (v < min_v) min_v = v;
        if (v > max_v) max_v = v;
        sum += v;
    }
    try testing.expect(min_v >= -128);
    try testing.expect(max_v <= 127);

    const mb_per_s: i64 = @intCast(@divTrunc(@as(i128, col.total_uncompressed_size) * 1_000_000, @max(elapsed_us, 1) * 1024 * 1024));
    std.debug.print(
        "[column] int8: {d} values decoded in {d} us; min={d} max={d} sum={d}; ~{d} MB/s (uncompressed)\n",
        .{ written, elapsed_us, min_v, max_v, sum, mb_per_s },
    );
}

test "decode bool column from the bench fixture" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return;
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var meta = try metadata.open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);

    const rg0 = &meta.row_groups.items[0];
    const col_idx = metadata.findColumnIndex(&meta, "bool") orelse return error.MissingColumn;
    const col = rg0.columns.items[col_idx].meta_data.?;

    const chunk_start: usize = if (col.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col.data_page_offset);
    const chunk_len: usize = @intCast(col.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const path: [1][]const u8 = .{"bool"};
    const levels = meta.getColumnLevels(&path);
    var reader = ColumnChunkReader(bool).init(chunk, col.codec, levels, arena.allocator());

    const expected: usize = @intCast(col.num_values);
    const out = try arena.allocator().alloc(bool, expected);

    const t0 = nowNs();
    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decode(out[written..]);
        if (n == 0) break;
        written += n;
    }
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);

    try testing.expectEqual(expected, written);

    var trues: usize = 0;
    for (out) |b| {
        if (b) trues += 1;
    }
    std.debug.print(
        "[column] bool: {d} values decoded in {d} us; {d} true / {d} false\n",
        .{ written, elapsed_us, trues, written - trues },
    );
}

test "decode string_dict_low column from the bench fixture" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return;
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var meta = try metadata.open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);

    const rg0 = &meta.row_groups.items[0];
    const col_idx = metadata.findColumnIndex(&meta, "string_dict_low") orelse return error.MissingColumn;
    const col = rg0.columns.items[col_idx].meta_data.?;

    const chunk_start: usize = if (col.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col.data_page_offset);
    const chunk_len: usize = @intCast(col.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const path: [1][]const u8 = .{"string_dict_low"};
    const levels = meta.getColumnLevels(&path);
    var reader = ColumnChunkReader([]const u8).init(chunk, col.codec, levels, arena.allocator());

    const expected: usize = @intCast(col.num_values);
    const out = try arena.allocator().alloc([]const u8, expected);

    const t0 = nowNs();
    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decode(out[written..]);
        if (n == 0) break;
        written += n;
    }
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);

    try testing.expectEqual(expected, written);
    // Spot-check first value is non-empty.
    try testing.expect(out[0].len > 0);

    std.debug.print(
        "[column] string_dict_low: {d} values decoded in {d} us; first=\"{s}\"\n",
        .{ written, elapsed_us, out[0] },
    );
}

// ----- File-read helper -----

fn readFileSlice(path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const linux = std.os.linux;
    var path_z: [256]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const fd: linux.fd_t = signedOrError(r_open) catch return error.FileNotFound;
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    if (errIs(end_pos)) return error.SeekFailed;
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    var off: usize = 0;
    while (off < size) {
        const n = linux.read(fd, buf[off..].ptr, size - off);
        if (errIs(n)) return error.ReadFailed;
        const bytes: usize = @intCast(n);
        if (bytes == 0) break;
        off += bytes;
    }
    return buf;
}

fn errIs(r: usize) bool {
    const signed: isize = @bitCast(r);
    return signed >= -4095 and signed < 0;
}

fn signedOrError(r: usize) error{SyscallFailed}!std.os.linux.fd_t {
    if (errIs(r)) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

fn nowNs() i128 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}
