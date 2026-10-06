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
const nowNs = @import("../../clock.zig").monoNs;
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
    /// Safe builds: two readers took turns on one `DecodeScratch` (see `DecodeScratch.hold`).
    DecodeScratchInterleaved,
    /// A data page claims more values than are left of the chunk's `value_budget`.
    PageValueCountExceedsChunk,
    /// A dictionary page claims more entries than its bytes can hold.
    DictionaryLargerThanPage,
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

/// The value decoder for the page being read, or none between pages. Variants whose encoding can't carry a T are
/// `void`, so they cost nothing and can never be installed.
fn ActiveDecoder(comptime T: type) type {
    const bytes = T == []const u8;
    return union(enum) {
        idle,
        plain: PlainDecoderFor(T),
        /// BOOLEAN never gets dictionary encoding: the type's cardinality makes it pointless.
        rle_dict: if (T == bool) void else rle_dict.Decoder(T),
        delta_int: if (T == i32 or T == i64) dbp.Decoder(T) else void,
        delta_len_ba: if (bytes) dba.DeltaLengthByteArrayDecoder else void,
        delta_ba: if (bytes) dba.DeltaByteArrayDecoder else void,
        /// PLAIN or BYTE_STREAM_SPLIT FIXED_LEN_BYTE_ARRAY: fixed-width slices, no length prefix.
        flba: if (bytes) plain.FixedLenByteArrayDecoder else void,
        /// RLE-encoded BOOLEAN values: a bit-width-1 hybrid stream, distinct from PLAIN's bit-packed booleans.
        bool_rle: if (T == bool) hybrid_rle.BooleanRleDecoder else void,
    };
}

/// Bits needed to encode 0..max inclusive. Used for def/rep-level
/// RLE/bit-packed-hybrid decoding (parquet uses ceil(log2(max+1))).
fn bitWidthFor(max: u32) u8 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

pub const DecodeScratch = page_mod.DecodeScratch;

/// Decode configuration: plain values, free to copy and share between threads. Per-worker state (`DecodeScratch`)
/// is a separate reader argument.
pub const DecodeOptions = struct {
    /// Skip materialising definition levels for pages whose level stream
    /// proves every value present. Kept on each reader so concurrent queries
    /// can choose independently without process-global mutable state.
    fast_levels: bool = false,
};

/// Hardwood's definition-level test, ported: a page has no nulls iff its
/// def-level stream is a single RLE run at max_def long enough to cover
/// the page. Decided from the run header in O(1), against the O(n) cost
/// of expanding the stream into a u32 per value and then counting it.
///
/// Conservative by construction — anything that is not obviously one
/// long enough max_def run (bit-packed opener, short run, mixed runs)
/// returns false and takes the ordinary path. A false negative costs the
/// fast path; there is no input for which a false positive is possible,
/// because "opens with an RLE run of value v and length >= n" is exactly
/// the statement "the first n levels all equal v".
fn defStreamAllPresent(def_bytes: []const u8, bit_width: u8, max_def: u32, num_values: usize) bool {
    const run = hybrid_rle.HybridRleDecoder.peekFirstRun(def_bytes, bit_width) orelse return false;
    return run.value == max_def and run.count >= num_values;
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
    return struct {
        const Self = @This();

        pages: page_mod.PageReader,
        arena: std.mem.Allocator,
        levels: schema.Levels,
        options: DecodeOptions,
        /// The calling worker's reusable page buffers, or null to decode into `arena`. See `DecodeScratch` for the
        /// lifetime rules; the reader holds it only through `scratch_ticket`'s turn.
        scratch: ?*DecodeScratch,
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

        active: ActiveDecoder(T) = .idle,

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

        /// `--fast-levels`: the current page's def-level stream was
        /// proven all-present in O(1), so it was never expanded and
        /// `current_def_levels` stays null even though max_def > 0.
        /// `decodePageSlice` stamps max_def into the caller's buffer
        /// instead of copying and counting a materialised array.
        current_def_all_present: bool = false,

        /// This reader's claim on `scratch`.
        scratch_ticket: DecodeScratch.Ticket = DecodeScratch.no_ticket,

        /// Values the chunk has left for pages not yet installed: set by the caller to the chunk's num_values,
        /// charged by each data page's header count before its level buffers are sized from it. RLE runs and bit
        /// width 0 let a few bytes claim any count, so the page's own size can't bound it. Unlimited by default.
        value_budget: usize = std.math.maxInt(usize),

        pub fn init(
            chunk_bytes: []const u8,
            codec: schema.CompressionCodec,
            levels: schema.Levels,
            arena: std.mem.Allocator,
        ) Self {
            return initWithOptions(chunk_bytes, codec, levels, arena, .{}, null);
        }

        pub fn initWithOptions(
            chunk_bytes: []const u8,
            codec: schema.CompressionCodec,
            levels: schema.Levels,
            arena: std.mem.Allocator,
            options: DecodeOptions,
            scratch: ?*DecodeScratch,
        ) Self {
            var pages = page_mod.PageReader.init(chunk_bytes, codec, arena);
            // Byte-array values are slices into the page bytes, so those pages must outlive the reader.
            if (T != []const u8) pages.scratch = scratch;
            return .{
                .pages = pages,
                .arena = arena,
                .levels = levels,
                .options = options,
                .scratch = scratch,
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

        /// Decode without a definition-level buffer at all, for as long
        /// as every page proves itself null-free.
        ///
        /// Returns the number of values written. A short return means
        /// the next page is NOT provably all-present (or the chunk ran
        /// out). That page is installed but entirely unconsumed —
        /// `current_def_pos` is still 0 — so the caller can allocate a
        /// level buffer, back-fill max_def over what was already
        /// written, and resume through `decodeWithLevels` with no gap
        /// and no re-read.
        ///
        /// Only meaningful under `fast_levels`; with it off, no page is
        /// ever marked all-present and this returns 0 immediately.
        pub fn decodeAllPresent(self: *Self, values: []T) Error!usize {
            try self.holdScratch();
            var written: usize = 0;
            while (written < values.len) {
                const remaining_in_page = if (self.hasActivePage())
                    self.current_page_num_values - self.current_def_pos
                else
                    0;

                if (remaining_in_page > 0) {
                    if (!self.current_def_all_present) return written;
                    const want = @min(values.len - written, remaining_in_page);
                    try self.decodePageSlicePresent(values[written .. written + want]);
                    written += want;
                    self.current_def_pos += want;
                    continue;
                }

                self.resetPageState();
                if (!try self.advancePage()) break;
            }
            return written;
        }

        /// A whole chunk's leaves and their level layout, as `readAll` returns them.
        pub const Leaves = struct {
            values: []T,
            /// Null when max_def == 0, or when fast levels proved every leaf present: either way no leaf is null.
            def_levels: ?[]u32 = null,
            /// Null when max_rep == 0.
            rep_levels: ?[]u32 = null,
            has_nulls: bool = false,
        };

        /// Drain the chunk's `num_leaves` leaves, failing with ShortDecode if its pages hold fewer. Everything returned
        /// lives as long as `arena`: the buffers are allocated there, and `[]const u8` values are slices into page
        /// bytes, which a byte-array reader never routes through a scratch. Adapters that convert the values (decimal,
        /// FLOAT16, unsigned widening) allocate the converted buffer themselves and reuse the levels unchanged.
        pub fn readAll(self: *Self, num_leaves: usize) (Error || error{ShortDecode})!Leaves {
            // Pages may claim no more values between them than the chunk holds.
            self.value_budget = num_leaves;
            const levels = self.levels;
            const values = try self.arena.alloc(T, num_leaves);
            if (levels.max_rep > 0) {
                const def_levels = try self.arena.alloc(u32, num_leaves);
                const rep_levels = try self.arena.alloc(u32, num_leaves);
                var written: usize = 0;
                while (written < num_leaves) {
                    const n = try self.decodeWithRepLevels(
                        values[written..],
                        def_levels[written..],
                        rep_levels[written..],
                    );
                    if (n == 0) break;
                    written += n;
                }
                if (written != num_leaves) return error.ShortDecode;
                return .{
                    .values = values,
                    .def_levels = def_levels,
                    .rep_levels = rep_levels,
                    .has_nulls = self.has_nulls,
                };
            }
            if (levels.max_def > 0) {
                var written: usize = 0;

                // `--fast-levels`: try to get through the whole chunk without
                // ever allocating the level array. A null `def_levels` then
                // carries the same meaning it always has — every leaf is
                // present — so nothing downstream needs to know this happened.
                //
                // Restricted to max_def == 1, the top-level OPTIONAL leaf that
                // arrow/spark/pandas emit for every nullable flat column. At
                // max_def >= 2 (an optional leaf under an optional group) a null
                // def_levels would have to be re-synthesised as "max_def
                // everywhere" by anything re-encoding the column, and
                // encoder.zig's fallback writes level 1, not level max_def. Not
                // worth widening for: the deeper shapes are rare, and they still
                // get the per-page half of this in `decodePageSlice`.
                if (self.options.fast_levels and levels.max_def == 1) {
                    written = try self.decodeAllPresent(values);
                    if (written == num_leaves) return .{ .values = values, .has_nulls = self.has_nulls };
                }

                // Either fast levels are off, or a page with nulls stopped the
                // pass above. Everything already written came from all-present
                // pages, so its levels are max_def by construction.
                const def_levels = try self.arena.alloc(u32, num_leaves);
                @memset(def_levels[0..written], @intCast(levels.max_def));
                while (written < num_leaves) {
                    const n = try self.decodeWithLevels(values[written..], def_levels[written..]);
                    if (n == 0) break;
                    written += n;
                }
                if (written != num_leaves) return error.ShortDecode;
                return .{ .values = values, .def_levels = def_levels, .has_nulls = self.has_nulls };
            }
            var written: usize = 0;
            while (written < num_leaves) {
                const n = try self.decode(values[written..]);
                if (n == 0) break;
                written += n;
            }
            if (written != num_leaves) return error.ShortDecode;
            return .{ .values = values };
        }

        fn decodeInner(self: *Self, values: []T, def_levels_opt: ?[]u32, rep_levels_opt: ?[]u32) Error!usize {
            try self.holdScratch();
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

        /// Entry check for every public decode / seek path; a no-op without a scratch or outside safe builds.
        fn holdScratch(self: *Self) Error!void {
            if (self.scratch) |s| try s.hold(&self.scratch_ticket);
        }

        fn chargePage(self: *Self, num_values: usize) Error!void {
            if (num_values > self.value_budget) return error.PageValueCountExceedsChunk;
            self.value_budget -= num_values;
        }

        fn hasActivePage(self: *const Self) bool {
            return self.active != .idle;
        }

        fn resetPageState(self: *Self) void {
            self.active = .idle;
            self.current_def_levels = null;
            self.current_rep_levels = null;
            self.current_def_all_present = false;
            self.current_def_pos = 0;
            self.current_page_num_values = 0;
        }

        /// Whether this page's def levels can be skipped entirely.
        ///
        /// Restricted to `max_rep == 0` on purpose. A repeated column's
        /// caller wants rep levels regardless, so skipping the def array
        /// saves half the level traffic at best — and every zpq query
        /// surface rejects nested columns before decode, so the case has
        /// no reachable consumer to justify the extra path. Flat columns
        /// are where the levels are pure overhead.
        fn canSkipDefLevels(self: *const Self, def_bytes: []const u8, bit_width: u8, num_values: usize) bool {
            if (!self.options.fast_levels) return false;
            if (self.levels.max_rep != 0) return false;
            return defStreamAllPresent(def_bytes, bit_width, @intCast(self.levels.max_def), num_values);
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
            // All-present page under --fast-levels: the levels were
            // never expanded, so there is nothing to copy or count.
            // Stamp the caller's slice and go straight to the packed
            // values, exactly as the REQUIRED path does.
            if (self.current_def_all_present) {
                @memset(def_levels_out, @intCast(self.levels.max_def));
                if (rep_levels_out) |rl_out| @memset(rl_out, 0);
                const got = try self.decodePackedFromCurrentPage(values);
                if (got != values.len) return error.UnexpectedPage;
                return;
            }

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
            return switch (self.active) {
                .idle => 0,
                inline else => |*d| if (@TypeOf(d.*) == void)
                    unreachable
                else
                    d.decode(dest) catch error.UnexpectedPage,
            };
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
            self.active = switch (T) {
                []const u8 => .{ .flba = plain.FixedLenByteArrayDecoder.init(unsplit, width) },
                i32, i64, f32, f64 => .{ .plain = plain.Decoder(T).init(unsplit) },
                else => unreachable,
            };
        }

        /// PLAIN values, shared by the V1 and V2 installers so the two can't disagree on a type's layout.
        /// FIXED_LEN_BYTE_ARRAY (`type_length > 0`) has no per-value length prefix, so it must not go through the
        /// length-prefixed BYTE_ARRAY decoder. `bool_count` bounds the bit-packed BOOLEAN stream.
        fn installPlain(self: *Self, values_bytes: []const u8, bool_count: usize) void {
            if (T == []const u8 and self.type_length > 0) {
                self.active = .{ .flba = plain.FixedLenByteArrayDecoder.init(values_bytes, self.type_length) };
                return;
            }
            self.active = .{ .plain = switch (T) {
                i32, i64, f32, f64 => plain.Decoder(T).init(values_bytes),
                []const u8 => plain.ByteArrayDecoder.init(values_bytes),
                bool => plain.BooleanDecoder.init(values_bytes, bool_count),
                else => unreachable,
            } };
        }

        /// RLE-encoded BOOLEAN values. The values region opens with a 4-byte
        /// little-endian length prefix (the RLE stream's byte length), in both
        /// V1 and V2 data pages, followed by the bit-width-1 hybrid stream.
        fn installRleBoolean(self: *Self, values_bytes: []const u8) Error!void {
            if (T != bool) return error.UnsupportedEncoding;
            if (values_bytes.len < 4) return error.UnexpectedPage;
            const rle_len = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (4 + rle_len > values_bytes.len) return error.UnexpectedPage;
            self.active = .{ .bool_rle = hybrid_rle.BooleanRleDecoder.init(values_bytes[4 .. 4 + rle_len]) };
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
            try self.holdScratch();
            self.resetPageState();
            try self.pages.seekToPage(absolute_offset, chunk_file_offset);
            return self.advancePage();
        }

        /// Reposition to a possible leading dictionary and install only that
        /// page. When a dictionary exists, returning immediately after it
        /// avoids decoding the first data page before the indexed caller seeks
        /// to that page. Without a dictionary, PageReader must still read the
        /// candidate data page to identify its type, but leaves it uninstalled.
        pub fn seekAndInstallDictionaryPage(
            self: *Self,
            absolute_offset: i64,
            chunk_file_offset: i64,
        ) !bool {
            try self.holdScratch();
            self.resetPageState();
            try self.pages.seekToPage(absolute_offset, chunk_file_offset);
            while (try self.pages.next()) |pg| {
                switch (pg.header.type) {
                    .DICTIONARY_PAGE => {
                        try self.installDictionary(pg);
                        return true;
                    },
                    .INDEX_PAGE => continue,
                    .DATA_PAGE, .DATA_PAGE_V2 => return false,
                }
            }
            return false;
        }

        /// Pull the next page; if it's a dictionary page, cache it and
        /// loop to the next page. Returns true iff a data-page decoder
        /// was set up for use; false if the chunk is exhausted.
        pub fn advancePage(self: *Self) Error!bool {
            try self.holdScratch();
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

        /// Page-lifetime level buffer: `decodePageSlice` copies levels out before the next page is installed.
        fn levelBuffer(self: *Self, comptime which: enum { def, rep }, n: usize) Error![]u32 {
            const s = self.scratch orelse return self.arena.alloc(u32, n);
            return s.ensure(u32, @alignOf(u32), if (which == .def) &s.def_levels else &s.rep_levels, n);
        }

        /// Chunk-lifetime dictionary buffer. Scratch only for fixed-width T, whose decoded values are copies;
        /// byte-array dictionary entries are handed out as-is and must live in the arena.
        fn dictBuffer(self: *Self, n: usize) Error![]T {
            switch (T) {
                i32, i64, f32, f64 => if (self.scratch) |s| {
                    const bytes = try s.ensure(u8, DecodeScratch.dict_align, &s.dict, n * @sizeOf(T));
                    return @as([*]T, @ptrCast(bytes.ptr))[0..n];
                },
                else => {},
            }
            return self.arena.alloc(T, n);
        }

        fn installDictionary(self: *Self, pg: page_mod.Page) Error!void {
            const dh = pg.header.dictionary_page_header orelse return error.UnexpectedPage;
            const num: usize = @intCast(dh.num_values);
            // BOOLEAN never gets dictionary encoding in practice — the
            // type's tiny cardinality makes it pointless. Reject early.
            if (T == bool) return error.UnsupportedEncoding;
            // Dictionary entries are PLAIN, so each takes at least its fixed width (a BYTE_ARRAY its 4-byte length).
            // Checked before the count sizes an allocation.
            const min_entry: usize = switch (T) {
                i32, f32 => 4,
                i64, f64 => 8,
                []const u8 => if (self.type_length > 0) self.type_length else 4,
                else => unreachable,
            };
            if (num > pg.bytes.len / min_entry) return error.DictionaryLargerThanPage;
            const buf = try self.dictBuffer(num);
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
            try self.beginPage(num_values);

            // V1 data-page payload layout:
            //   [u32 LE: rep_levels_byte_length][rep level bytes]   (if max_rep > 0)
            //   [u32 LE: def_levels_byte_length][def level bytes]   (if max_def > 0)
            //   [encoded values]
            var values_bytes = pg.bytes;
            if (self.levels.max_rep > 0) {
                if (values_bytes.len < 4) return error.UnexpectedPage;
                const rep_len = std.mem.readInt(u32, values_bytes[0..4], .little);
                if (4 + rep_len > values_bytes.len) return error.UnexpectedPage;

                try self.installRepLevels(values_bytes[4 .. 4 + rep_len], num_values);
                values_bytes = values_bytes[4 + rep_len ..];
            }
            if (self.levels.max_def > 0) {
                if (values_bytes.len < 4) return error.UnexpectedPage;
                const def_len = std.mem.readInt(u32, values_bytes[0..4], .little);
                if (4 + def_len > values_bytes.len) return error.UnexpectedPage;

                try self.installDefLevels(values_bytes[4 .. 4 + def_len], num_values);
                values_bytes = values_bytes[4 + def_len ..];
            }

            try self.installValues(dph.encoding, values_bytes, num_values);
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
            try self.beginPage(num_values);

            const rep_len: usize = @intCast(dph.repetition_levels_byte_length);
            const def_len: usize = @intCast(dph.definition_levels_byte_length);
            if (rep_len + def_len > pg.bytes.len) return error.UnexpectedPage;

            var cursor: usize = 0;
            if (self.levels.max_rep > 0) {
                if (rep_len == 0) return error.UnexpectedPage;
                try self.installRepLevels(pg.bytes[cursor..][0..rep_len], num_values);
                cursor += rep_len;
            } else if (rep_len != 0) {
                // Source claims rep levels but schema says max_rep == 0.
                // Skip and hope; downstream level-aware decode won't ask for them.
                cursor += rep_len;
            }

            if (self.levels.max_def > 0) {
                if (def_len == 0) return error.UnexpectedPage;
                // V2 headers also carry `num_nulls`, which would answer the all-present check without reading a
                // byte — but that is the writer's claim, in the same class as the statistics this engine makes you
                // opt into with --trust-stats. The run check reads the levels themselves, so a writer that lies
                // cannot turn it into a wrong answer.
                try self.installDefLevels(pg.bytes[cursor..][0..def_len], num_values);
                cursor += def_len;
            } else if (def_len != 0) {
                cursor += def_len;
            }

            // V2 with nullable cols: the values hold only the non-null entries, which bounds a PLAIN BOOLEAN stream.
            // The other decoders are streams that consume only what's asked for; decodePageSlice handles the count.
            try self.installValues(dph.encoding, pg.bytes[cursor..], @intCast(dph.num_values - dph.num_nulls));
        }

        /// Page state shared by both data-page versions, set before either parses its levels.
        fn beginPage(self: *Self, num_values: usize) Error!void {
            try self.chargePage(num_values);
            self.current_def_levels = null;
            self.current_rep_levels = null;
            self.current_def_all_present = false;
            self.current_def_pos = 0;
            self.current_page_num_values = num_values;
        }

        fn installRepLevels(self: *Self, rep_bytes: []const u8, num_values: usize) Error!void {
            const buf = try self.levelBuffer(.rep, num_values);
            var dec = hybrid_rle.HybridRleDecoder.init(rep_bytes, bitWidthFor(@intCast(self.levels.max_rep)));
            const got = dec.decode(buf) catch return error.UnexpectedPage;
            if (got != num_values) return error.DefLevelsMismatch;
            self.current_rep_levels = buf;
        }

        fn installDefLevels(self: *Self, def_bytes: []const u8, num_values: usize) Error!void {
            const bit_width = bitWidthFor(@intCast(self.levels.max_def));
            if (self.canSkipDefLevels(def_bytes, bit_width, num_values)) {
                self.current_def_all_present = true;
                return;
            }
            const buf = try self.levelBuffer(.def, num_values);
            var dec = hybrid_rle.HybridRleDecoder.init(def_bytes, bit_width);
            const got = dec.decode(buf) catch return error.UnexpectedPage;
            if (got != num_values) return error.DefLevelsMismatch;
            self.current_def_levels = buf;
        }

        /// Install the page's value decoder once the version-specific framing has been stripped. `present_count`
        /// is the number of values physically present: V1 pages count nulls in it, V2 pages don't. Only the bit-packed
        /// PLAIN BOOLEAN stream needs it; the delta byte-array decoders are sized by the page's level count.
        fn installValues(
            self: *Self,
            encoding: schema.Encoding,
            values_bytes: []const u8,
            present_count: usize,
        ) Error!void {
            self.active = .idle;
            const num_values = self.current_page_num_values;
            switch (encoding) {
                .PLAIN => self.installPlain(values_bytes, present_count),
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                    if (T == bool) return error.UnsupportedEncoding;
                    const dict = self.dictionary orelse return error.DictionaryMissing;
                    const d = rle_dict.Decoder(T).init(values_bytes, dict);
                    self.active = .{ .rle_dict = d catch return error.UnexpectedPage };
                },
                .DELTA_BINARY_PACKED => {
                    if (T != i32 and T != i64) return error.UnsupportedEncoding;
                    const d = dbp.Decoder(T).init(values_bytes);
                    self.active = .{ .delta_int = d catch return error.UnexpectedPage };
                },
                .DELTA_LENGTH_BYTE_ARRAY => {
                    if (T != []const u8) return error.UnsupportedEncoding;
                    const d = dba.DeltaLengthByteArrayDecoder.init(values_bytes, self.arena, num_values);
                    self.active = .{ .delta_len_ba = d catch return error.UnexpectedPage };
                },
                .DELTA_BYTE_ARRAY => {
                    if (T != []const u8) return error.UnsupportedEncoding;
                    const d = dba.DeltaByteArrayDecoder.init(values_bytes, self.arena, num_values);
                    self.active = .{ .delta_ba = d catch return error.UnexpectedPage };
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
const readFileSlice = metadata.readFileSlice;
const thrift = @import("../thrift.zig");

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
    // Installing a leading data page as a dictionary would silently corrupt every value, so assert on the reader's
    // dictionary state directly. Both parquet-testing shapes reach the omitted-offset path: alltypes_plain bool_col
    // leads with a DATA_PAGE, alltypes_tiny_pages string_col with a DICTIONARY_PAGE.
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

        // Fail loudly if a parquet-testing bump adds the offset, rather than quietly stop covering the
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
            try testing.expect(try reader.seekAndInstallDictionaryPage(chunk_start, chunk_start));
            try testing.expect(reader.dictionary != null);
        } else {
            var reader = ColumnChunkReader(bool).init(chunk, col.codec, levels, arena.allocator());
            try testing.expect(!try reader.seekAndInstallDictionaryPage(chunk_start, chunk_start));
            // The leading page was data, so nothing may have been
            // installed as a dictionary.
            try testing.expect(reader.dictionary == null);
        }
    }
}

/// One uncompressed PLAIN FIXED_LEN_BYTE_ARRAY data page (V1 or V2) holding `values`; a null entry is a null slot.
/// With `nullable`, def levels are one bit-packed run at bit width 1 (RLE/bit-packed hybrid), which both page
/// versions share; V1 prefixes them with their u32 length, V2 reports the length in the header.
fn flbaPageForTest(
    arena: std.mem.Allocator,
    v2: bool,
    width: usize,
    nullable: bool,
    values: []const ?[]const u8,
) ![]u8 {
    std.debug.assert(values.len <= 8); // a single bit-packed group of def levels
    var levels_buf: [2]u8 = undefined;
    var levels_bytes: []const u8 = &.{};
    var num_nulls: i32 = 0;
    if (nullable) {
        var bits: u8 = 0;
        for (values, 0..) |v, i| {
            if (v != null) bits |= @as(u8, 1) << @intCast(i) else num_nulls += 1;
        }
        levels_buf = .{ (1 << 1) | 1, bits }; // one bit-packed group of 8, then the bits LSB-first
        levels_bytes = &levels_buf;
    }

    var body: std.ArrayList(u8) = .empty;
    if (nullable and !v2) {
        var len: [4]u8 = undefined;
        std.mem.writeInt(u32, &len, @intCast(levels_bytes.len), .little);
        try body.appendSlice(arena, &len);
    }
    try body.appendSlice(arena, levels_bytes);
    for (values) |v| if (v) |bytes| {
        std.debug.assert(bytes.len == width);
        try body.appendSlice(arena, bytes);
    };

    const n: i32 = @intCast(values.len);
    const header = schema.PageHeader{
        .type = if (v2) .DATA_PAGE_V2 else .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = if (v2) null else .{
            .num_values = n,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = if (!v2) null else .{
            .num_values = n,
            .num_nulls = num_nulls,
            .num_rows = n,
            .encoding = .PLAIN,
            .definition_levels_byte_length = @intCast(levels_bytes.len),
            .repetition_levels_byte_length = 0,
            .is_compressed = false,
        },
    };
    var w = thrift.Writer.init(arena);
    try header.write(&w);
    var chunk: std.ArrayList(u8) = .empty;
    try chunk.appendSlice(arena, w.bytes());
    try chunk.appendSlice(arena, body.items);
    return chunk.items;
}

test "PLAIN FIXED_LEN_BYTE_ARRAY decodes the same from V1 and V2 data pages" {
    // FLBA has no per-value length prefix. A V2 page routed through the length-prefixed BYTE_ARRAY decoder reads
    // the first value's bytes as a length: a short decode at best, silently wrong slices at worst. Covers the
    // float16 (2), UUID / fixed binary (16) and an odd width, each REQUIRED and OPTIONAL with real nulls.
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const Case = struct { width: usize, values: []const ?[]const u8 };
    const cases = [_]Case{
        .{ .width = 2, .values = &.{ "\x00\x3c", null, "\x00\xc0", "\x00\x38", null } },
        .{ .width = 16, .values = &.{ "0123456789abcdef", "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f", null, "\xff\xfe\xfd\xfc\xfb\xfa\xf9\xf8\xf7\xf6\xf5\xf4\xf3\xf2\xf1\xf0" } },
        .{ .width = 3, .values = &.{ null, "abc", "def", null, "ghi", "jkl" } },
    };

    for (cases) |case| for ([_]bool{ false, true }) |nullable| for ([_]bool{ false, true }) |v2| {
        var present: std.ArrayList(?[]const u8) = .empty;
        for (case.values) |v| if (nullable or v != null) try present.append(arena, v);
        const want = present.items;

        const chunk = try flbaPageForTest(arena, v2, case.width, nullable, want);
        const levels: schema.Levels = .{ .max_def = if (nullable) 1 else 0, .max_rep = 0 };
        var reader = ColumnChunkReader([]const u8).init(chunk, .UNCOMPRESSED, levels, arena);
        reader.type_length = case.width;

        const got = try arena.alloc([]const u8, want.len);
        const defs = try arena.alloc(u32, want.len);
        const n = if (nullable) try reader.decodeWithLevels(got, defs) else try reader.decode(got);
        try testing.expectEqual(want.len, n);
        for (want, got, 0..) |w, g, i| {
            if (w) |bytes| {
                try testing.expectEqualSlices(u8, bytes, g);
                if (nullable) try testing.expectEqual(@as(u32, 1), defs[i]);
            } else {
                try testing.expectEqual(@as(u32, 0), defs[i]);
                try testing.expectEqual(@as(usize, 0), g.len);
            }
        }
    };
}

test "a reader whose scratch another reader took over fails instead of decoding overwritten buffers" {
    // The scratch's level buffer is shared, so reader A resuming after B installed a page would read B's levels as
    // its own. Safe builds must catch the interleave at A's next entry; handing the scratch to a fresh reader after
    // one is abandoned mid-chunk is the normal sequential pattern and must keep working.
    if (!std.debug.runtime_safety) return error.SkipZigTest;
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    var scratch = DecodeScratch.init(testing.allocator);
    defer scratch.deinit();

    const levels: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
    const chunk_a = try flbaPageForTest(arena, false, 3, true, &.{ "aaa", null, "bbb", "ccc" });
    const chunk_b = try flbaPageForTest(arena, true, 3, true, &.{ null, null, "zzz", null });

    var values: [4][]const u8 = undefined;
    var defs: [4]u32 = undefined;

    var a = ColumnChunkReader([]const u8).initWithOptions(chunk_a, .UNCOMPRESSED, levels, arena, .{}, &scratch);
    a.type_length = 3;
    try testing.expectEqual(@as(usize, 2), try a.decodeWithLevels(values[0..2], defs[0..2]));

    var b = ColumnChunkReader([]const u8).initWithOptions(chunk_b, .UNCOMPRESSED, levels, arena, .{}, &scratch);
    b.type_length = 3;
    try testing.expectEqual(@as(usize, 4), try b.decodeWithLevels(&values, &defs));
    try testing.expectEqualSlices(u32, &.{ 0, 0, 1, 0 }, &defs);

    try testing.expectError(error.DecodeScratchInterleaved, a.decodeWithLevels(values[2..4], defs[2..4]));
    try testing.expectError(error.DecodeScratchInterleaved, a.advancePage());

    // B is abandoned here; a fresh reader takes the scratch and decodes A's chunk from the start.
    var c = ColumnChunkReader([]const u8).initWithOptions(chunk_a, .UNCOMPRESSED, levels, arena, .{}, &scratch);
    c.type_length = 3;
    try testing.expectEqual(@as(usize, 4), try c.decodeWithLevels(&values, &defs));
    try testing.expectEqualSlices(u32, &.{ 1, 0, 1, 1 }, &defs);
    try testing.expectEqualStrings("ccc", values[3]);
}

test "malformed-file fixtures decode or fail cleanly instead of trapping" {
    // Each used to reach a safety trap (undefined behaviour in ReleaseFast). See THIRD_PARTY_NOTICES.md for sources.
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    { // A bit-packed level run near the end of a page, asked for in one call, overran the 1024-byte unpack pad.
        // pyarrow reads all 21186 values as 0.
        const bytes = try metadata.readFileSlice("ci/fixtures/parquet/ARROW-GH-43605.parquet", arena);
        var meta = try metadata.open(arena, bytes);
        const cm = meta.row_groups.items[0].columns.items[0].meta_data.?;
        const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
        const chunk = bytes[start..][0..@intCast(cm.total_compressed_size)];
        const got = try decodeChunkForTest(i32, arena, chunk, cm.codec, meta.getColumnLevels(cm.path_in_schema.items), @intCast(cm.num_values), null);
        try testing.expectEqual(@as(usize, 21186), got.values.len);
        for (got.values, got.defs) |v, d| {
            try testing.expectEqual(@as(i32, 0), v);
            try testing.expectEqual(@as(u32, 1), d);
        }
    }
    { // A dictionary page header with a negative value count was cast straight to usize.
        const bytes = try metadata.readFileSlice("ci/fixtures/parquet/ARROW-RS-GH-6229-DICTHEADER.parquet", arena);
        var meta = try metadata.open(arena, bytes);
        const cm = meta.row_groups.items[0].columns.items[1].meta_data.?;
        const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
        const chunk = bytes[start..][0..@intCast(cm.total_compressed_size)];
        var reader = ColumnChunkReader([]const u8).init(chunk, cm.codec, meta.getColumnLevels(cm.path_in_schema.items), arena);
        var values: [25][]const u8 = undefined;
        var defs: [25]u32 = undefined;
        try testing.expectError(error.BadPageCount, reader.decodeWithLevels(&values, &defs));
    }
}

test "a file whose schema root carries a physical type decodes its columns" {
    // segmentio/parquet-go types the root as well as giving it children; see tools/gen_typed_root_fixture.py.
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/typed_root.parquet", arena);
    var meta = try metadata.open(arena, bytes);
    try testing.expect(meta.schema.items[0].type != null);
    const cols = meta.row_groups.items[0].columns.items;
    const want = [_][3]i64{ .{ 1000, 2000, 3000 }, .{ 1999, 2999, 3999 } };
    for (want, cols[0..2]) |w, col| {
        const cm = col.meta_data.?;
        const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
        const chunk = bytes[start..][0..@intCast(cm.total_compressed_size)];
        const got = try decodeChunkForTest(i64, arena, chunk, cm.codec, meta.getColumnLevels(cm.path_in_schema.items), @intCast(cm.num_values), null);
        try testing.expectEqualSlices(i64, &w, got.values);
    }
}

fn DecodedChunkForTest(comptime T: type) type {
    return struct { values: []T, defs: []u32 };
}

/// Whole chunk through one reader: values plus def levels (empty when the column is REQUIRED).
fn decodeChunkForTest(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_values: usize,
    scratch: ?*DecodeScratch,
) !DecodedChunkForTest(T) {
    var reader = ColumnChunkReader(T).initWithOptions(chunk, codec, levels, arena, .{}, scratch);
    const values = try arena.alloc(T, num_values);
    const defs = try arena.alloc(u32, if (levels.max_def > 0) num_values else 0);
    var written: usize = 0;
    while (written < num_values) {
        const n = if (levels.max_def > 0)
            try reader.decodeWithLevels(values[written..], defs[written..])
        else
            try reader.decode(values[written..]);
        if (n == 0) break;
        written += n;
    }
    try testing.expectEqual(num_values, written);
    return .{ .values = values, .defs = defs };
}

test "scratch-backed decode matches arena decode across chunks sharing one scratch" {
    // One scratch serves every chunk of every file in turn, the way a scan worker reuses it, so a page, level or
    // dictionary buffer that leaks state from the previous chunk shows up as a value mismatch. tiny_pages covers
    // many pages plus dictionaries per chunk, datapage_v2 the V2 in-place decompress, nulls_snappy def levels.
    const paths = [_][]const u8{
        "data/parquet-testing/data/alltypes_tiny_pages.parquet",
        "data/parquet-testing/data/datapage_v2.snappy.parquet",
        "data/parquet-testing/data/alltypes_plain.snappy.parquet",
        "data/parquet-testing/data/nulls.snappy.parquet",
    };
    var scratch = DecodeScratch.init(testing.allocator);
    defer scratch.deinit();
    var checked: usize = 0;

    for (paths) |path| {
        const file_bytes = readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(file_bytes);
        var meta = try metadata.open(testing.allocator, file_bytes);
        defer meta.deinit(testing.allocator);

        for (meta.row_groups.items) |rg| for (rg.columns.items) |cc| {
            const cm = cc.meta_data orelse continue;
            const levels = meta.getColumnLevels(cm.path_in_schema.items);
            if (levels.max_rep > 0) continue;
            const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
            const chunk = file_bytes[start..][0..@intCast(cm.total_compressed_size)];
            const n: usize = @intCast(cm.num_values);

            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            switch (cm.type) {
                inline .INT32, .INT64, .FLOAT, .DOUBLE, .BOOLEAN, .BYTE_ARRAY => |pt| {
                    const T = switch (pt) {
                        .INT32 => i32,
                        .INT64 => i64,
                        .FLOAT => f32,
                        .DOUBLE => f64,
                        .BOOLEAN => bool,
                        .BYTE_ARRAY => []const u8,
                        else => unreachable,
                    };
                    const want = try decodeChunkForTest(T, a, chunk, cm.codec, levels, n, null);
                    const got = try decodeChunkForTest(T, a, chunk, cm.codec, levels, n, &scratch);
                    try testing.expectEqualSlices(u32, want.defs, got.defs);
                    if (T == []const u8) {
                        for (want.values, got.values) |w, g| try testing.expectEqualStrings(w, g);
                    } else {
                        try testing.expectEqualSlices(T, want.values, got.values);
                    }
                    checked += 1;
                },
                else => {},
            }
        };
    }
    try testing.expect(checked >= 20);
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

// ----- Legacy LZ4 (codec 5) corpus files -----

/// Open a parquet-testing fixture (bytes + footer), or skip the test when the corpus is absent.
fn openLz4Fixture(arena: std.mem.Allocator, path: []const u8) !struct { bytes: []const u8, meta: schema.FileMetaData } {
    const bytes = readFileSlice(path, arena) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{path});
            return error.SkipZigTest;
        }
        return err;
    };
    return .{ .bytes = bytes, .meta = try metadata.open(arena, bytes) };
}

/// Decode column `col` of row group 0, asserting it really is stored with the deprecated LZ4 codec.
fn lz4FixtureChunk(
    comptime T: type,
    arena: std.mem.Allocator,
    f: anytype,
    col: usize,
) !DecodedChunkForTest(T) {
    const cm = f.meta.row_groups.items[0].columns.items[col].meta_data.?;
    try testing.expectEqual(schema.CompressionCodec.LZ4, cm.codec);
    const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    const chunk = f.bytes[start..][0..@intCast(cm.total_compressed_size)];
    const levels = f.meta.getColumnLevels(cm.path_in_schema.items);
    return decodeChunkForTest(T, arena, chunk, cm.codec, levels, @intCast(cm.num_values), null);
}

test "legacy LZ4 (codec 5) corpus files decode to pyarrow's values" {
    // parquet-mr's Hadoop framing and old parquet-cpp's bare block hold the same table; expected values are
    // pyarrow 25's reading of each file.
    const paths = [_][]const u8{
        "data/parquet-testing/data/hadoop_lz4_compressed.parquet",
        "data/parquet-testing/data/non_hadoop_lz4_compressed.parquet",
    };
    for (paths) |path| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const f = try openLz4Fixture(a, path);

        const c0 = try lz4FixtureChunk(i64, a, f, 0);
        try testing.expectEqualSlices(i64, &.{ 1593604800, 1593604800, 1593604801, 1593604801 }, c0.values);
        const c1 = try lz4FixtureChunk([]const u8, a, f, 1);
        for ([_][]const u8{ "abc", "def", "abc", "def" }, c1.values) |w, g| try testing.expectEqualStrings(w, g);
        const v11 = try lz4FixtureChunk(f64, a, f, 2);
        try testing.expectEqualSlices(f64, &.{ 42.0, 7.7, 42.125, 7.7 }, v11.values);
        try testing.expectEqualSlices(u32, &.{ 1, 1, 1, 1 }, v11.defs);
    }
}

test "legacy LZ4 (codec 5): a multi-chunk Hadoop page decodes to pyarrow's values" {
    // One 400000-byte page split into three Hadoop chunks.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const f = try openLz4Fixture(a, "data/parquet-testing/data/hadoop_lz4_compressed_larger.parquet");
    const col = try lz4FixtureChunk([]const u8, a, f, 0);
    try testing.expectEqual(@as(usize, 10000), col.values.len);
    try testing.expectEqualStrings("c7ce6bef-d5b0-4863-b199-8ea8c7fb117b", col.values[0]);
    try testing.expectEqualStrings("e8fb9197-cb9f-4118-b67f-fbfa65f61843", col.values[1]);
    try testing.expectEqualStrings("85440778-460a-41ac-aa2e-ac3ee41696bf", col.values[9999]);
    // sha256 over every value, each followed by '\n', as pyarrow reads them.
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    for (col.values) |v| {
        h.update(v);
        h.update("\n");
    }
    var digest: [32]u8 = undefined;
    h.final(&digest);
    var want: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&want, "b5e0165eb228bae9d5102e1fefa9d135f58f09b82c3bfe5afb7f7e196aca5c2a");
    try testing.expectEqualSlices(u8, &want, &digest);
}

// ----- File-read helper -----
