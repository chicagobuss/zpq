//! ColumnChunkReader: walk a column chunk's pages, decode values.
//!
//! Brings together page.zig + the encoding decoders. The contract:
//! caller picks the right T based on the column's logical type
//! (i32/i64/f32/f64 for primitives, []const u8 for BYTE_ARRAY),
//! constructs a reader over the column chunk's bytes, and pulls
//! decoded values into a buffer of T.
//!
//! Page dispatch (DATA_PAGE and DATA_PAGE_V2 both handled):
//!   - First page (if present) of type DICTIONARY_PAGE: decoded via
//!     PLAIN into an arena-allocated dictionary slice. Cached.
//!   - Data page: switch on the header's encoding:
//!       PLAIN                       → Plain.Decoder(T) (FLBA via type_length)
//!       PLAIN_DICTIONARY            → RleDict.Decoder(T) (legacy alias for
//!       RLE_DICTIONARY                 RLE_DICTIONARY), using the cached dict
//!       DELTA_BINARY_PACKED         → i32/i64 only
//!       DELTA_LENGTH_BYTE_ARRAY     → []const u8
//!       DELTA_BYTE_ARRAY            → []const u8
//!       RLE                         → BOOLEAN values only (4-byte len prefix)
//!       BYTE_STREAM_SPLIT           → transpose → PLAIN (numeric + FLBA)
//!       (others, e.g. BIT_PACKED)   → error.UnsupportedEncoding
//!
//! Nullable columns: definition/repetition levels are threaded through
//! decodeWithLevels / decodeWithRepLevels. The bare decode() entry asserts
//! max_def == 0 (REQUIRED columns only) so callers can't silently alias nulls.

const std = @import("std");
const schema = @import("../schema.zig");
const page_mod = @import("page.zig");
const plain = @import("encoding/plain.zig");
const rle_dict = @import("encoding/rle_dict.zig");
const dbp = @import("encoding/delta_binary_packed.zig");
const dba = @import("encoding/delta_byte_array.zig");
const hybrid_rle = @import("encoding/hybrid_rle.zig");

pub const Error = error{
    UnsupportedEncoding,
    UnsupportedPageType,
    NestedNotSupported,
    DictionaryMissing,
    UnexpectedPage,
    DefLevelsMismatch,
    LevelsArgMissing,
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

/// Bits needed to encode 0..max inclusive. Used for def/rep-level
/// RLE/bit-packed-hybrid decoding (parquet uses ceil(log2(max+1))).
fn bitWidthFor(max: u32) u8 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

/// BYTE_STREAM_SPLIT un-split. The encoded buffer holds `width` contiguous
/// byte-planes of N bytes each (N = encoded.len / width); value `i` is
/// reassembled from `encoded[0*N+i], encoded[1*N+i], … encoded[(width-1)*N+i]`.
/// The result is byte-for-byte identical to PLAIN, so callers feed it to the
/// existing PLAIN / FLBA decoder for the type — BSS is just a transpose of
/// PLAIN. Returns a freshly-allocated buffer of `encoded.len` bytes.
fn unsplitByteStreamSplit(arena: std.mem.Allocator, encoded: []const u8, width: usize) Error![]u8 {
    if (width == 0 or encoded.len % width != 0) return error.UnexpectedPage;
    const n = encoded.len / width;
    const out = try arena.alloc(u8, encoded.len);
    var j: usize = 0;
    while (j < width) : (j += 1) {
        const plane = encoded[j * n ..][0..n];
        var i: usize = 0;
        while (i < n) : (i += 1) {
            out[i * width + j] = plane[i];
        }
    }
    return out;
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
        has_nulls: bool = false,

        /// For FIXED_LEN_BYTE_ARRAY columns: the fixed value width in bytes
        /// (0 for every other type). When > 0, PLAIN / dictionary `[]const u8`
        /// pages are decoded as fixed-width slices (no per-value length prefix)
        /// rather than as length-prefixed BYTE_ARRAY. Set by the caller after
        /// init (defaults to 0 so existing callers are unaffected).
        type_length: usize = 0,

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
        /// PLAIN FIXED_LEN_BYTE_ARRAY page (only used when T == []const u8 and
        /// type_length > 0): fixed-width slices into the page bytes.
        current_flba: ?plain.FixedLenByteArrayDecoder = null,
        /// RLE-encoded BOOLEAN values (only used when T == bool): bit-width-1
        /// hybrid RLE stream, distinct from PLAIN bit-packed booleans.
        current_bool_rle: ?hybrid_rle.BooleanRleDecoder = null,

        /// Decoded definition / repetition levels for the current
        /// data page, when `levels.max_def > 0` / `levels.max_rep > 0`.
        /// Length equals the page's `num_values` (i.e. number of
        /// LEAVES in the page, which for nested columns can be > the
        /// number of logical rows). `current_def_pos` tracks how many
        /// entries the caller has already consumed across decode()
        /// calls within this page.
        current_def_levels: ?[]u32 = null,
        current_rep_levels: ?[]u32 = null,
        current_def_pos: usize = 0,
        current_page_num_values: usize = 0,

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

        /// Decode up to `dest.len` values into `dest`. Returns the
        /// number written. Zero means the chunk is fully consumed.
        ///
        /// **REQUIRED columns only**: the column's `levels.max_def`
        /// must be 0. For OPTIONAL columns (max_def > 0) the caller
        /// MUST use `decodeWithLevels` instead — that path threads
        /// definition levels through and correctly leaves null slots
        /// at their default values rather than silently aliasing them
        /// to whatever the next non-null wire byte happens to be.
        pub fn decode(self: *Self, dest: []T) Error!usize {
            if (self.levels.max_def != 0) return error.LevelsArgMissing;
            return self.decodeInner(dest, null, null);
        }

        /// Decode up to `values.len` values into `values`, alongside
        /// definition levels into `def_levels`. Required for columns
        /// with `levels.max_def > 0` (i.e. OPTIONAL leaves). Slots
        /// where `def_levels[i] < max_def` are nulls; the corresponding
        /// `values[i]` is left at the type's default (0 / false / "").
        ///
        /// `values.len` must equal `def_levels.len`.
        ///
        /// REP-AWARE callers should use `decodeWithRepLevels` instead;
        /// this entry point asserts max_rep == 0.
        pub fn decodeWithLevels(
            self: *Self,
            values: []T,
            def_levels: []u32,
        ) Error!usize {
            if (values.len != def_levels.len) return error.DefLevelsMismatch;
            if (self.levels.max_rep != 0) return error.LevelsArgMissing;
            return self.decodeInner(values, def_levels, null);
        }

        /// Full-nested decode entry. `values`, `def_levels`, and
        /// `rep_levels` must all have the same length (== leaf count
        /// the caller wants). `rep_levels[i] == 0` marks the start of
        /// a new logical row; non-zero values mean continuation at
        /// some depth (max == levels.max_rep).
        pub fn decodeWithRepLevels(
            self: *Self,
            values: []T,
            def_levels: []u32,
            rep_levels: []u32,
        ) Error!usize {
            if (values.len != def_levels.len or values.len != rep_levels.len) {
                return error.DefLevelsMismatch;
            }
            return self.decodeInner(values, def_levels, rep_levels);
        }

        fn decodeInner(self: *Self, values: []T, def_levels_opt: ?[]u32, rep_levels_opt: ?[]u32) Error!usize {
            var written: usize = 0;
            while (written < values.len) {
                // If we have an active page, drain it first.
                const remaining_in_page = if (self.hasActivePage())
                    self.current_page_num_values - self.current_def_pos
                else
                    0;

                if (remaining_in_page > 0) {
                    const want = @min(values.len - written, remaining_in_page);
                    const v_slice = values[written .. written + want];
                    if (def_levels_opt) |dl| {
                        const rl_slice: ?[]u32 = if (rep_levels_opt) |rl|
                            rl[written .. written + want]
                        else
                            null;
                        try self.decodePageSlice(v_slice, dl[written .. written + want], rl_slice);
                    } else {
                        try self.decodePageSlicePresent(v_slice);
                    }
                    written += want;
                    self.current_def_pos += want;
                    continue;
                }

                // Page exhausted (or none active); advance.
                self.resetPageState();
                if (!try self.advancePage()) break;
            }
            return written;
        }

        fn hasActivePage(self: *const Self) bool {
            return self.current_plain != null or
                self.current_rle_dict != null or
                self.current_delta_int != null or
                self.current_delta_len_ba != null or
                self.current_delta_ba != null or
                self.current_flba != null or
                self.current_bool_rle != null;
        }

        fn resetPageState(self: *Self) void {
            self.current_plain = null;
            self.current_rle_dict = null;
            self.current_delta_int = null;
            self.current_delta_len_ba = null;
            self.current_delta_ba = null;
            self.current_flba = null;
            self.current_bool_rle = null;
            self.current_def_levels = null;
            self.current_rep_levels = null;
            self.current_def_pos = 0;
            self.current_page_num_values = 0;
        }

        /// REQUIRED-column path: ask the active per-page decoder for
        /// `dest.len` values directly.
        fn decodePageSlicePresent(self: *Self, dest: []T) Error!void {
            const got = try self.decodePackedFromCurrentPage(dest);
            if (got != dest.len) return error.UnexpectedPage;
        }

        /// OPTIONAL-column path: copy def-level slice (and rep-level
        /// slice when caller provided one) into the caller's buffers,
        /// count num_present, ask the per-page decoder for that many
        /// packed values into the front of `values`, then scatter
        /// them into the correct slots walking backwards (so we never
        /// overwrite a packed value before reading it). Null slots
        /// default-initialise to zero / false / empty-slice.
        fn decodePageSlice(self: *Self, values: []T, def_levels_out: []u32, rep_levels_out: ?[]u32) Error!void {
            const dl = self.current_def_levels.?;
            const src = dl[self.current_def_pos .. self.current_def_pos + values.len];
            @memcpy(def_levels_out, src);

            if (rep_levels_out) |rl_out| {
                if (self.current_rep_levels) |rl| {
                    const rl_src = rl[self.current_def_pos .. self.current_def_pos + values.len];
                    @memcpy(rl_out, rl_src);
                } else {
                    @memset(rl_out, 0);
                }
            }

            const max_def: u32 = @intCast(self.levels.max_def);
            var num_present: usize = 0;
            for (src) |d| {
                if (d == max_def) num_present += 1;
            }

            // Fast path: no nulls in this batch. The packed decoder
            // writes directly into `values` (no scatter needed). Real
            // data hits this constantly — declared-OPTIONAL columns
            // often have null_count == 0 in practice, and parquet
            // doesn't tell us upfront so we fall back to checking
            // num_present after the scan. Profile (2026-05-07) showed
            // `decodePageSlice` at 28% of decode CPU; the scatter loop
            // was a big chunk of that.
            if (num_present == values.len) {
                const got = try self.decodePackedFromCurrentPage(values);
                if (got != values.len) return error.UnexpectedPage;
                return;
            }

            self.has_nulls = true;

            if (num_present > 0) {
                const got = try self.decodePackedFromCurrentPage(values[0..num_present]);
                if (got != num_present) return error.UnexpectedPage;
            }

            // Scatter from the packed front into the correct slots,
            // walking back-to-front so a packed value is never
            // overwritten before its scatter destination has been read.
            var read: usize = num_present;
            var i: usize = values.len;
            while (i > 0) {
                i -= 1;
                if (src[i] == max_def) {
                    read -= 1;
                    values[i] = values[read];
                } else {
                    values[i] = defaultValue();
                }
            }
        }

        fn decodePackedFromCurrentPage(self: *Self, dest: []T) Error!usize {
            if (self.current_plain) |*d| {
                return @as(*PlainDec, d).decode(dest) catch return error.UnexpectedPage;
            }
            if (T == bool) {
                if (self.current_bool_rle) |*d| {
                    return d.decode(dest) catch return error.UnexpectedPage;
                }
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
                if (self.current_flba) |*d| {
                    return d.decode(dest) catch return error.UnexpectedPage;
                }
                if (self.current_delta_len_ba) |*d| {
                    return d.decode(dest) catch return error.UnexpectedPage;
                }
                if (self.current_delta_ba) |*d| {
                    return d.decode(dest) catch return error.UnexpectedPage;
                }
            }
            return 0;
        }

        /// BYTE_STREAM_SPLIT: transpose the byte-planes back to PLAIN layout
        /// and install the PLAIN (or FLBA) decoder over the result. Width is
        /// the value's fixed byte size — `@sizeOf(T)` for the numeric types,
        /// `type_length` for FIXED_LEN_BYTE_ARRAY (covers float16 / flba /
        /// decimal-FLBA). BOOLEAN and length-prefixed BYTE_ARRAY have no fixed
        /// width and so cannot be BSS-encoded.
        fn installByteStreamSplit(self: *Self, values_bytes: []const u8) Error!void {
            const width: usize = switch (T) {
                i32, i64, f32, f64 => @sizeOf(T),
                []const u8 => self.type_length,
                else => 0, // bool — no fixed width
            };
            if (width == 0) return error.UnsupportedEncoding;
            const unsplit = try unsplitByteStreamSplit(self.arena, values_bytes, width);
            if (T == []const u8) {
                self.current_flba = plain.FixedLenByteArrayDecoder.init(unsplit, width);
            } else {
                self.current_plain = switch (T) {
                    i32, i64, f32, f64 => plain.Decoder(T).init(unsplit),
                    else => unreachable,
                };
            }
        }

        /// RLE-encoded BOOLEAN values. The values region opens with a 4-byte
        /// little-endian length prefix (the RLE stream's byte length), in both
        /// V1 and V2 data pages, followed by the bit-width-1 hybrid stream.
        fn installRleBoolean(self: *Self, values_bytes: []const u8) Error!void {
            if (T != bool) return error.UnsupportedEncoding;
            if (values_bytes.len < 4) return error.UnexpectedPage;
            const rle_len = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (4 + rle_len > values_bytes.len) return error.UnexpectedPage;
            self.current_bool_rle = hybrid_rle.BooleanRleDecoder.init(values_bytes[4 .. 4 + rle_len]);
        }

        fn defaultValue() T {
            return switch (T) {
                i32, i64 => 0,
                f32, f64 => 0.0,
                bool => false,
                []const u8 => "",
                else => @compileError("no default for " ++ @typeName(T)),
            };
        }

        /// Reposition the page reader to the page at `absolute_offset`, then advance
        /// and install its decoder.
        pub fn seekAndInstallPage(self: *Self, absolute_offset: i64, chunk_file_offset: i64) !bool {
            self.resetPageState();
            try self.pages.seekToPage(absolute_offset, chunk_file_offset);
            return self.advancePage();
        }

        /// Pull the next page; if it's a dictionary page, cache it and
        /// loop to the next page. Returns true iff a data-page decoder
        /// was set up for use; false if the chunk is exhausted.
        pub fn advancePage(self: *Self) Error!bool {
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
                    .DATA_PAGE_V2 => {
                        try self.installDataPageV2Decoder(pg);
                        return true;
                    },
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
                    if (self.type_length > 0) {
                        var d = plain.FixedLenByteArrayDecoder.init(pg.bytes, self.type_length);
                        const n = d.decode(buf) catch return error.UnexpectedPage;
                        if (n != num) return error.UnexpectedPage;
                    } else {
                        var d = plain.ByteArrayDecoder.init(pg.bytes);
                        const n = d.decode(buf) catch return error.UnexpectedPage;
                        if (n != num) return error.UnexpectedPage;
                    }
                },
                else => unreachable,
            }
            self.dictionary = buf;
        }

        fn installDataPageDecoder(self: *Self, pg: page_mod.Page) Error!void {
            const dph = pg.header.data_page_header orelse return error.UnexpectedPage;

            const num_values: usize = @intCast(dph.num_values);
            self.current_def_levels = null;
            self.current_rep_levels = null;
            self.current_def_pos = 0;
            self.current_page_num_values = num_values;

            // V1 data-page payload layout:
            //   [u32 LE: rep_levels_byte_length][rep level bytes]   (if max_rep > 0)
            //   [u32 LE: def_levels_byte_length][def level bytes]   (if max_def > 0)
            //   [encoded values]
            var values_bytes = pg.bytes;
            if (self.levels.max_rep > 0) {
                if (values_bytes.len < 4) return error.UnexpectedPage;
                const rep_len = std.mem.readInt(u32, values_bytes[0..4], .little);
                if (4 + rep_len > values_bytes.len) return error.UnexpectedPage;

                const rep_bytes = values_bytes[4 .. 4 + rep_len];
                const buf = try self.arena.alloc(u32, num_values);
                const bit_width = bitWidthFor(@intCast(self.levels.max_rep));
                var dec = hybrid_rle.HybridRleDecoder.init(rep_bytes, bit_width);
                const got = dec.decode(buf) catch return error.UnexpectedPage;
                if (got != num_values) return error.DefLevelsMismatch;
                self.current_rep_levels = buf;

                values_bytes = values_bytes[4 + rep_len ..];
            }
            if (self.levels.max_def > 0) {
                if (values_bytes.len < 4) return error.UnexpectedPage;
                const def_len = std.mem.readInt(u32, values_bytes[0..4], .little);
                if (4 + def_len > values_bytes.len) return error.UnexpectedPage;

                const def_bytes = values_bytes[4 .. 4 + def_len];
                const buf = try self.arena.alloc(u32, num_values);
                const bit_width = bitWidthFor(@intCast(self.levels.max_def));
                var dec = hybrid_rle.HybridRleDecoder.init(def_bytes, bit_width);
                const got = dec.decode(buf) catch return error.UnexpectedPage;
                if (got != num_values) return error.DefLevelsMismatch;
                self.current_def_levels = buf;

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
                    if (T == []const u8 and self.type_length > 0) {
                        // FIXED_LEN_BYTE_ARRAY: no length prefixes, fixed-width slices.
                        self.current_flba = plain.FixedLenByteArrayDecoder.init(values_bytes, self.type_length);
                    } else {
                        self.current_plain = switch (T) {
                            i32, i64, f32, f64 => plain.Decoder(T).init(values_bytes),
                            []const u8 => plain.ByteArrayDecoder.init(values_bytes),
                            bool => plain.BooleanDecoder.init(values_bytes, @intCast(dph.num_values)),
                            else => unreachable,
                        };
                    }
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
                .RLE => try self.installRleBoolean(values_bytes),
                .BYTE_STREAM_SPLIT => try self.installByteStreamSplit(values_bytes),
                else => return error.UnsupportedEncoding,
            }
        }

        /// V2 data page installer. Two structural differences from V1:
        ///   - Level lengths come from the V2 thrift header, not from
        ///     `<u32 LE>` prefixes inside the page bytes.
        ///   - Levels are RLE-encoded but stored raw (no hybrid varint
        ///     header byte; the page bytes ARE the rle stream).
        ///   - The page reader has already merged rep + def + values
        ///     into `pg.bytes` with values decompressed (see page.zig
        ///     V2 path), so this installer can slice straight by the
        ///     header lengths.
        fn installDataPageV2Decoder(self: *Self, pg: page_mod.Page) Error!void {
            const dph = pg.header.data_page_header_v2 orelse return error.UnexpectedPage;

            const num_values: usize = @intCast(dph.num_values);
            self.current_def_levels = null;
            self.current_rep_levels = null;
            self.current_def_pos = 0;
            self.current_page_num_values = num_values;

            const rep_len: usize = @intCast(dph.repetition_levels_byte_length);
            const def_len: usize = @intCast(dph.definition_levels_byte_length);
            if (rep_len + def_len > pg.bytes.len) return error.UnexpectedPage;

            var cursor: usize = 0;
            if (self.levels.max_rep > 0) {
                if (rep_len == 0) return error.UnexpectedPage;
                const rep_bytes = pg.bytes[cursor..][0..rep_len];
                const buf = try self.arena.alloc(u32, num_values);
                const bit_width = bitWidthFor(@intCast(self.levels.max_rep));
                var dec = hybrid_rle.HybridRleDecoder.init(rep_bytes, bit_width);
                const got = dec.decode(buf) catch return error.UnexpectedPage;
                if (got != num_values) return error.DefLevelsMismatch;
                self.current_rep_levels = buf;
                cursor += rep_len;
            } else if (rep_len != 0) {
                // Source claims rep levels but schema says max_rep == 0.
                // Skip and hope; downstream level-aware decode won't ask for them.
                cursor += rep_len;
            }

            if (self.levels.max_def > 0) {
                if (def_len == 0) return error.UnexpectedPage;
                const def_bytes = pg.bytes[cursor..][0..def_len];
                const buf = try self.arena.alloc(u32, num_values);
                const bit_width = bitWidthFor(@intCast(self.levels.max_def));
                var dec = hybrid_rle.HybridRleDecoder.init(def_bytes, bit_width);
                const got = dec.decode(buf) catch return error.UnexpectedPage;
                if (got != num_values) return error.DefLevelsMismatch;
                self.current_def_levels = buf;
                cursor += def_len;
            } else if (def_len != 0) {
                cursor += def_len;
            }

            const values_bytes = pg.bytes[cursor..];

            // Reset all variant slots — only one will be populated below.
            self.current_plain = null;
            self.current_rle_dict = null;
            self.current_delta_int = null;
            self.current_delta_len_ba = null;
            self.current_delta_ba = null;

            switch (dph.encoding) {
                .PLAIN => {
                    // V2 with nullable cols: PLAIN body holds only the
                    // non-null values. PlainDecoder is a stream that
                    // consumes only what's asked for; the chunk
                    // reader's spread logic handles the count.
                    self.current_plain = switch (T) {
                        i32, i64, f32, f64 => plain.Decoder(T).init(values_bytes),
                        []const u8 => plain.ByteArrayDecoder.init(values_bytes),
                        bool => plain.BooleanDecoder.init(values_bytes, @intCast(dph.num_values - dph.num_nulls)),
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
                .RLE => try self.installRleBoolean(values_bytes),
                .BYTE_STREAM_SPLIT => try self.installByteStreamSplit(values_bytes),
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

test "unsplitByteStreamSplit transposes byte-planes back to PLAIN" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    // 3 four-byte values laid out as 4 planes of 3 bytes:
    //   value0 = {0xA0,0xB0,0xC0,0xD0}, value1 = {0xA1,…}, value2 = {0xA2,…}
    // plane j = the j-th byte of every value, concatenated.
    const encoded = [_]u8{
        0xA0, 0xA1, 0xA2, // plane 0 (byte 0 of each value)
        0xB0, 0xB1, 0xB2, // plane 1
        0xC0, 0xC1, 0xC2, // plane 2
        0xD0, 0xD1, 0xD2, // plane 3
    };
    const out = try unsplitByteStreamSplit(arena, &encoded, 4);
    try testing.expectEqualSlices(u8, &.{
        0xA0, 0xB0, 0xC0, 0xD0,
        0xA1, 0xB1, 0xC1, 0xD1,
        0xA2, 0xB2, 0xC2, 0xD2,
    }, out);

    // Width that doesn't divide the buffer is a malformed page.
    try testing.expectError(error.UnexpectedPage, unsplitByteStreamSplit(arena, &encoded, 5));
}

test "chunk-start seek installs a dictionary only when one actually leads the chunk" {
    // With `dictionary_page_offset` absent, the pruned decode path seeks to the chunk start and advances one page. Both
    // shapes in parquet-testing reach that code and must be told apart: alltypes_plain bool_col leads with a DATA_PAGE,
    // alltypes_tiny_pages string_col with a DICTIONARY_PAGE. Installing a leading data page as a dictionary would
    // silently corrupt every value, so assert on the reader's dictionary state directly.
    const Case = struct {
        path: []const u8,
        column: []const u8,
        expect_dictionary: bool,
    };
    const cases = [_]Case{
        .{ .path = "data/parquet-testing/data/alltypes_plain.parquet", .column = "bool_col", .expect_dictionary = false },
        .{ .path = "data/parquet-testing/data/alltypes_tiny_pages.parquet", .column = "string_col", .expect_dictionary = true },
    };

    for (cases) |case| {
        const file_bytes = readFileSlice(case.path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{case.path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(file_bytes);

        var meta = try metadata.open(testing.allocator, file_bytes);
        defer meta.deinit(testing.allocator);

        const col_idx = metadata.findColumnIndex(&meta, case.column) orelse return error.MissingColumn;
        const col = meta.row_groups.items[0].columns.items[col_idx].meta_data.?;

        // Precondition: if a parquet-testing bump adds the offset, fail here rather than quietly stop covering the
        // omitted-offset shape.
        try testing.expect(col.dictionary_page_offset == null);

        const chunk_start: i64 = col.data_page_offset;
        const chunk = file_bytes[@intCast(chunk_start)..][0..@intCast(col.total_compressed_size)];

        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();

        const path: [1][]const u8 = .{case.column};
        const levels = meta.getColumnLevels(&path);

        if (case.expect_dictionary) {
            var reader = ColumnChunkReader([]const u8).init(chunk, col.codec, levels, arena.allocator());
            try reader.pages.seekToPage(chunk_start, chunk_start);
            try testing.expect(try reader.advancePage());
            try testing.expect(reader.dictionary != null);
        } else {
            var reader = ColumnChunkReader(bool).init(chunk, col.codec, levels, arena.allocator());
            try reader.pages.seekToPage(chunk_start, chunk_start);
            try testing.expect(try reader.advancePage());
            // Leading page was data: nothing may be installed as a dictionary.
            try testing.expect(reader.dictionary == null);
        }
    }
}

test "decode int8 column from the bench fixture" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
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

    // Decode all values from this row group's column chunk. Polars
    // writes every leaf as OPTIONAL even when there are no nulls, so
    // we go through decodeWithLevels and verify every def_level == 1.
    const expected: usize = @intCast(col.num_values);
    const out = try arena.allocator().alloc(i32, expected);
    const def_levels = try arena.allocator().alloc(u32, expected);

    const t0 = nowNs();
    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decodeWithLevels(out[written..], def_levels[written..]);
        if (n == 0) break;
        written += n;
    }
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);
    for (def_levels) |d| try testing.expectEqual(@as(u32, 1), d);

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
        if (err == error.FileNotFound) return error.SkipZigTest;
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
    const def_levels = try arena.allocator().alloc(u32, expected);

    const t0 = nowNs();
    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decodeWithLevels(out[written..], def_levels[written..]);
        if (n == 0) break;
        written += n;
    }
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);

    try testing.expectEqual(expected, written);
    for (def_levels) |d| try testing.expectEqual(@as(u32, 1), d);

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
        if (err == error.FileNotFound) return error.SkipZigTest;
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
    const def_levels = try arena.allocator().alloc(u32, expected);

    const t0 = nowNs();
    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decodeWithLevels(out[written..], def_levels[written..]);
        if (n == 0) break;
        written += n;
    }
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);

    try testing.expectEqual(expected, written);
    for (def_levels) |d| try testing.expectEqual(@as(u32, 1), d);
    // Spot-check first value is non-empty.
    try testing.expect(out[0].len > 0);

    std.debug.print(
        "[column] string_dict_low: {d} values decoded in {d} us; first=\"{s}\"\n",
        .{ written, elapsed_us, out[0] },
    );
}

test "decode int32_nullable column (with actual nulls) from the bench fixture" {
    // The bench fixture has a column with real nulls. A nullable
    // decoder must consume definition levels before reading PLAIN
    // values; otherwise it short-decodes or aliases null slots to
    // garbage. Verify nulls stay at
    // default (0) and decoded values at their real positions.
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var meta = try metadata.open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);

    const col_idx = metadata.findColumnIndex(&meta, "int32_nullable") orelse return error.MissingColumn;
    const rg0 = &meta.row_groups.items[0];
    const col = rg0.columns.items[col_idx].meta_data.?;

    const chunk_start: usize = if (col.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col.data_page_offset);
    const chunk_len: usize = @intCast(col.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const path: [1][]const u8 = .{"int32_nullable"};
    const levels = meta.getColumnLevels(&path);
    try testing.expectEqual(@as(i32, 1), levels.max_def);

    var reader = ColumnChunkReader(i32).init(chunk, col.codec, levels, arena.allocator());

    const expected: usize = @intCast(col.num_values);
    const out = try arena.allocator().alloc(i32, expected);
    const def_levels = try arena.allocator().alloc(u32, expected);

    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decodeWithLevels(out[written..], def_levels[written..]);
        if (n == 0) break;
        written += n;
    }
    try testing.expectEqual(expected, written);

    var nulls: usize = 0;
    var present: usize = 0;
    for (def_levels) |d| {
        if (d == 0) nulls += 1 else present += 1;
    }
    try testing.expectEqual(expected, nulls + present);
    try testing.expect(nulls > 0); // file is known to have actual nulls
    try testing.expect(present > 0);

    // Sanity: every null slot's value should be the default (0).
    // Every non-null slot's value should be in the int32 range
    // (always — int32 trivially. The relevant invariant is that we
    // didn't read uninitialized bytes — defaults are deterministic).
    var sum_present: i64 = 0;
    for (out, def_levels) |v, d| {
        if (d == 0) {
            try testing.expectEqual(@as(i32, 0), v);
        } else {
            sum_present += v;
        }
    }
    std.debug.print(
        "[column] int32_nullable: {d} values, {d} null, {d} present, sum_present={d}\n",
        .{ written, nulls, present, sum_present },
    );
}

test "decode string_nullable column (with actual nulls) from the bench fixture" {
    // Polars writes string_nullable as RLE_DICTIONARY-encoded with
    // real nulls. Exercises the dict-encoding null-spread path,
    // distinct from int32_nullable's PLAIN path.
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var meta = try metadata.open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);

    const col_idx = metadata.findColumnIndex(&meta, "string_nullable") orelse return error.MissingColumn;
    const rg0 = &meta.row_groups.items[0];
    const col = rg0.columns.items[col_idx].meta_data.?;

    const chunk_start: usize = if (col.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col.data_page_offset);
    const chunk_len: usize = @intCast(col.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const path: [1][]const u8 = .{"string_nullable"};
    const levels = meta.getColumnLevels(&path);
    try testing.expectEqual(@as(i32, 1), levels.max_def);

    var reader = ColumnChunkReader([]const u8).init(chunk, col.codec, levels, arena.allocator());

    const expected: usize = @intCast(col.num_values);
    const out = try arena.allocator().alloc([]const u8, expected);
    const def_levels = try arena.allocator().alloc(u32, expected);

    var written: usize = 0;
    while (written < expected) {
        const n = try reader.decodeWithLevels(out[written..], def_levels[written..]);
        if (n == 0) break;
        written += n;
    }
    try testing.expectEqual(expected, written);

    var nulls: usize = 0;
    var present: usize = 0;
    for (def_levels) |d| {
        if (d == 0) nulls += 1 else present += 1;
    }
    try testing.expect(nulls > 0);
    try testing.expect(present > 0);

    // Each null slot should be the empty default; each non-null
    // should be a non-empty (or at least valid) string.
    for (out, def_levels) |s, d| {
        if (d == 0) {
            try testing.expectEqual(@as(usize, 0), s.len);
        }
    }
    std.debug.print(
        "[column] string_nullable: {d} values, {d} null, {d} present\n",
        .{ written, nulls, present },
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
