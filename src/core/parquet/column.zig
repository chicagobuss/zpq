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
const selection = @import("../filter/selection.zig");

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
    /// A second dictionary page in a chunk, or one after a data page: the format allows one, first. Codes already
    /// checked against the first dictionary would index the second.
    UnexpectedDictionaryPage,
    /// A repetition or definition level past the schema's maximum (see `hybrid_rle.decodeLevels`).
    LevelOutOfRange,
    /// A chunk's decoded values expand past `DecodeOptions.expand_limit`, the query's budget.
    DecodeExceededBudget,
    /// An INT32 value outside its column's 8- or 16-bit integer annotation (`IntRange`).
    IntOutOfRange,
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

fn levelsInto(bytes: []const u8, max_level: anytype, out: []u32) Error!void {
    hybrid_rle.decodeLevels(bytes, @intCast(max_level), out) catch |err| return levelError(err);
}

fn levelError(err: hybrid_rle.LevelError) Error {
    return switch (err) {
        error.CorruptLevels => error.UnexpectedPage,
        error.LevelCountMismatch => error.DefLevelsMismatch,
        error.LevelOutOfRange => error.LevelOutOfRange,
    };
}

pub const DecodeScratch = page_mod.DecodeScratch;

/// A value decoder's failure as the reader reports it: the page is malformed, unless the decoder ran out of memory
/// (a budget's refusal, whose caller may decode on less), which stays what it is.
fn pageError(err: anyerror) Error {
    return if (err == error.OutOfMemory) error.OutOfMemory else error.UnexpectedPage;
}

/// Decode configuration: plain values, free to copy and share between threads. Per-worker state (`DecodeScratch`)
/// is a separate reader argument.
pub const DecodeOptions = struct {
    /// Skip materialising definition levels for pages whose level stream
    /// proves every value present. Kept on each reader so concurrent queries
    /// can choose independently without process-global mutable state.
    fast_levels: bool = false,
    /// null chooses run-dominated flat pages from encoded headers; true/false force streaming/legacy expansion.
    level_runs: ?bool = null,
    /// Let an aggregate scan work on dictionary-encoded columns as codes (filters once per dictionary entry,
    /// histogram aggregates, level-only counts) instead of materialising values. Off only to compare against.
    encoded: bool = true,
    /// Keep codes however near-unique the dictionary: a key folded by a retained domain translates its codes, so
    /// they never cost per-entry work (`agg_dense`).
    any_dictionary: bool = false,
    /// The most bytes one chunk's pages may inflate to (`PageReader.byte_limit`); set per chunk from its footer.
    page_bytes_limit: usize = std.math.maxInt(usize),
    /// The most bytes one page's DELTA_BYTE_ARRAY values may expand to: the query's memory budget, which a page past
    /// it could only overrun (a few bytes of prefix lengths can ask for a terabyte).
    expand_limit: usize = std.math.maxInt(usize),
    /// Per chunk (`chunkOptions`): the range an INT32 column's narrow integer annotation allows.
    int_range: ?IntRange = null,
    /// The row group's rows a query still needs (its filter's survivors). A data page of an optional flat column
    /// with none of them is passed by its header alone, never decompressed, and reads as nulls: those rows are never
    /// read. Only the input and the query decide which pages that is, never the decode path, workers or budget.
    row_gate: ?*const selection.SelectionVector = null,
};

/// `o` for the chunk `cm` describes: its pages bounded by `inflateLimit`, and its values by an INT32 column's narrow
/// integer type (`IntRange`).
pub fn chunkOptions(o: DecodeOptions, meta: *const schema.FileMetaData, cm: schema.ColumnMetaData) DecodeOptions {
    var x = o;
    x.page_bytes_limit = @min(o.page_bytes_limit, inflateLimit(cm));
    if (cm.type == .INT32) if (meta.getColumnSchema(cm.path_in_schema.items)) |se| {
        x.int_range = IntRange.of(se);
    };
    return x;
}

/// The most the pages of chunk `cm` may inflate to: twice what the footer says it holds, plus slack for writers that
/// count loosely.
pub fn inflateLimit(cm: schema.ColumnMetaData) usize {
    const total = std.math.cast(usize, cm.total_uncompressed_size) orelse std.math.maxInt(usize);
    return total *| 2 +| (1 << 20);
}

/// The values an INT32 column's 8- or 16-bit integer annotation (INT_8/16, UINT_8/16, or the INTEGER logical type)
/// allows. A wider physical value is no value of the column: zero-extended it read as 16777423 in a UINT_8 column,
/// and truncated, as some readers do, as a number no writer wrote.
pub const IntRange = struct {
    min: i32,
    max: i32,

    pub fn of(se: schema.SchemaElement) ?IntRange {
        const bits: u5, const signed: bool = if (se.logical_type) |lt| switch (lt) {
            .INTEGER => |it| switch (it.bitWidth) {
                8, 16 => .{ @intCast(it.bitWidth), it.isSigned },
                else => return null,
            },
            else => return null,
        } else if (se.converted_type) |ct| switch (ct) {
            .INT_8 => .{ 8, true },
            .INT_16 => .{ 16, true },
            .UINT_8 => .{ 8, false },
            .UINT_16 => .{ 16, false },
            else => return null,
        } else return null;
        const top = @as(i32, 1) << if (signed) bits - 1 else bits;
        return if (signed) .{ .min = -top, .max = top - 1 } else .{ .min = 0, .max = top - 1 };
    }
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
        pub const Value = T;

        pages: page_mod.PageReader,
        arena: std.mem.Allocator,
        /// A probe's retained dictionary; its other allocations can then be released per page.
        dictionary_arena: ?std.mem.Allocator = null,
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
        /// A data page of this chunk has been installed: a dictionary page may no longer follow.
        data_page_seen: bool = false,

        active: ActiveDecoder(T) = .idle,

        /// Decoded definition / repetition levels for the current
        /// data page, when `levels.max_def > 0` / `levels.max_rep > 0`.
        /// Length equals the page's `num_values` (i.e. number of
        /// LEAVES in the page, which for nested columns can be > the
        /// number of logical rows). `current_def_pos` tracks how many
        /// entries the caller has already consumed across decode()
        /// calls within this page.
        current_def_levels: ?[]u32 = null,
        /// Flat fast-level readers retain the validated encoded stream, expanding only a caller's row window.
        current_def_stream: ?hybrid_rle.HybridRleDecoder = null,
        current_def_nulls: usize = 0,
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
        /// What the chunk's DELTA_BYTE_ARRAY pages have expanded to so far, against `DecodeOptions.expand_limit`:
        /// the pages together, since each held to the budget alone took it many times over in a row group read whole.
        expanded: usize = 0,
        /// The installed DELTA_BYTE_ARRAY page's expansion, not yet charged to `expanded`: a count(col) that reads
        /// only its levels never builds the values, and must not be refused for them.
        pending_expansion: usize = 0,
        /// `DecodeOptions.row_gate`, kept only for an optional flat column, and the rows its data pages so far hold.
        gate: ?*const selection.SelectionVector = null,
        gate_row: usize = 0,

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
            pages.byte_limit = options.page_bytes_limit;
            // Byte-array values are slices into the page bytes, so those pages must outlive the reader.
            if (T != []const u8) pages.scratch = scratch;
            if (scratch) |s| pages.prof = s.prof;
            return .{
                .pages = pages,
                .arena = arena,
                .levels = levels,
                .options = options,
                .scratch = scratch,
                .gate = if (levels.max_def > 0 and levels.max_rep == 0) options.row_gate else null,
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

        /// A whole chunk as dictionary codes, as `readAllCodes` returns it.
        pub const CodeLeaves = struct {
            /// `codes[i]` indexes `dict` for leaf i; 0 at null leaves (the def levels govern).
            codes: []u32,
            /// Chunk-lifetime dictionary, copied out of any per-worker scratch.
            dict: []const T,
            def_levels: ?[]u32 = null,
            has_nulls: bool = false,
        };

        pub const CodedOrValues = union(enum) { codes: CodeLeaves, values: Leaves };

        /// Drain the chunk as dictionary codes, never resolving a value. A chunk that is not
        /// dictionary-encoded throughout (a PLAIN fallback page after the dictionary outgrew its page) switches to
        /// values in place: the codes so far are gathered and the remaining pages decode as `readAll` would, so the
        /// fallback costs one gather, not a second decode. Null when there is no coded form at all (BOOLEAN, nested,
        /// a chunk opening with a non-dictionary page); the reader is then untouched past its first page and the
        /// caller decodes with a fresh one.
        pub fn readAllCodes(self: *Self, num_leaves: usize) (Error || error{ShortDecode})!?CodedOrValues {
            if (T == bool) {
                return null;
            } else {
                return self.readAllCodesImpl(num_leaves);
            }
        }

        fn readAllCodesImpl(self: *Self, num_leaves: usize) (Error || error{ShortDecode})!?CodedOrValues {
            if (self.levels.max_rep > 0) return null;
            // All-present pages are proven from the level stream's opening run (never a false positive) and their
            // levels never expanded; the level array is only built once a page actually has nulls.
            self.options.fast_levels = true;
            self.value_budget = num_leaves;
            try self.holdScratch();
            const max_def: u32 = @intCast(self.levels.max_def);
            var codes: []u32 = &.{};
            var def_out: ?[]u32 = null;
            var dict_ptr: ?[*]const T = null;
            var written: usize = 0;
            while (written < num_leaves) {
                const remaining_in_page = if (self.hasActivePage())
                    self.current_page_num_values - self.current_def_pos
                else
                    0;
                if (remaining_in_page > 0) {
                    if (self.active != .rle_dict) {
                        return .{ .values = try self.finishAsValues(codes, def_out, written, num_leaves) };
                    }
                    const d = self.dictionary orelse return null;
                    if (dict_ptr) |p| {
                        if (p != d.ptr)
                            return .{ .values = try self.finishAsValues(codes, def_out, written, num_leaves) };
                    } else if (d.len == 0 or (!self.options.any_dictionary and d.len > num_leaves / 8)) {
                        // Near-unique dictionary: codes buy nothing over values, and per-entry work rivals per-row.
                        return .{ .values = try self.finishAsValues(codes, def_out, written, num_leaves) };
                    } else {
                        dict_ptr = d.ptr;
                        codes = try self.arena.alloc(u32, num_leaves);
                    }
                    const want = @min(num_leaves - written, remaining_in_page);
                    const out = codes[written..][0..want];
                    const page_has_levels = !self.current_def_all_present and self.hasDefLevels();
                    if (max_def > 0 and def_out == null and page_has_levels) {
                        def_out = try self.arena.alloc(u32, num_leaves);
                        @memset(def_out.?[0..written], max_def);
                    }
                    if (!try self.readPageCodes(out, if (def_out) |dl| dl[written..][0..want] else null))
                        return error.UnexpectedPage;
                    written += want;
                    continue;
                }
                self.resetPageState();
                if (!try self.advancePage()) break;
            }
            if (written != num_leaves) return error.ShortDecode;
            if (num_leaves == 0) return null;
            const dict = self.dictionary orelse return null;
            const owned: []const T = if (T == []const u8) dict else try self.arena.dupe(T, dict);
            return .{ .codes = .{ .codes = codes, .dict = owned, .def_levels = def_out, .has_nulls = self.has_nulls } };
        }

        /// Read codes from the active page only. A non-dictionary page declines without consuming a leaf;
        /// the sliced caller can gather earlier codes and continue through its ordinary value reader.
        pub fn readPageCodes(self: *Self, out: []u32, def_out: ?[]u32) Error!bool {
            if (T == bool) return false;
            if (self.active != .rle_dict or self.dictionary == null) return false;
            std.debug.assert(out.len <= self.pageValuesLeft());
            const max_def: u32 = @intCast(self.levels.max_def);
            std.debug.assert(def_out != null or self.current_def_all_present or !self.hasDefLevels());
            if (def_out) |d| std.debug.assert(d.len == out.len);
            const idx = &self.active.rle_dict.indices;
            if (def_out == null or self.current_def_all_present or !self.hasDefLevels()) {
                if (def_out) |dl| @memset(dl, max_def);
                const got = idx.decode(out) catch return error.UnexpectedPage;
                if (got != out.len) return error.UnexpectedPage;
                try self.checkCodes(out);
            } else {
                const src = def_out.?;
                try self.readDefLevels(src);
                var num_present: usize = 0;
                for (src) |dv| num_present += @intFromBool(dv == max_def);
                if (num_present > 0) {
                    const got = idx.decode(out[0..num_present]) catch return error.UnexpectedPage;
                    if (got != num_present) return error.UnexpectedPage;
                    try self.checkCodes(out[0..num_present]);
                }
                if (num_present != out.len) {
                    self.has_nulls = true;
                    var read: usize = num_present;
                    var i: usize = out.len;
                    while (i > 0) {
                        i -= 1;
                        if (src[i] == max_def) {
                            read -= 1;
                            out[i] = out[read];
                        } else out[i] = 0;
                    }
                }
            }
            self.current_def_pos += out.len;
            return true;
        }

        pub const HistLeaves = struct {
            /// Present leaves per dictionary entry.
            hist: []u64,
            dict: []const T,
            num_present: usize,
            /// With `track_first`: each entry's first leaf, or `no_leaf` for an entry no leaf holds.
            first: ?[]const u32 = null,
        };

        pub const no_leaf = std.math.maxInt(u32);

        /// Fold the chunk straight into a histogram of dictionary codes, never writing a code or
        /// a value per row. An RLE run of the index stream costs O(1) (`hist[v] += run length`); bit-packed indices
        /// pass through a 256-entry staging buffer. Null rows are counted out from the levels. Null when the chunk is
        /// not dictionary-encoded throughout, or the dictionary is near-unique; the caller then decodes afresh.
        pub const HistOrValues = union(enum) { hist: HistLeaves, values: Leaves };

        /// `track_first` also notes each entry's first leaf (a GROUP BY key's group position), and then declines a
        /// chunk with a null leaf, whose group would need a position too.
        pub fn readAllHistogram(
            self: *Self,
            num_leaves: usize,
            track_first: bool,
        ) (Error || error{ShortDecode})!?HistOrValues {
            if (T == bool) {
                return null;
            } else {
                return self.readAllHistogramImpl(num_leaves, track_first);
            }
        }

        fn readAllHistogramImpl(
            self: *Self,
            num_leaves: usize,
            track_first: bool,
        ) (Error || error{ShortDecode})!?HistOrValues {
            if (self.levels.max_rep > 0) return null;
            if (track_first and num_leaves > no_leaf) return null;
            self.options.fast_levels = true;
            self.value_budget = num_leaves;
            try self.holdScratch();
            const max_def: u32 = @intCast(self.levels.max_def);
            var hist: []u64 = &.{};
            var first_leaf: ?[]u32 = null;
            // Entries with no first leaf yet; once none is left, the rest of the chunk only counts.
            var unfound: usize = 0;
            var dict_ptr: ?[*]const T = null;
            var written: usize = 0;
            var present_total: usize = 0;
            var buf: [256]u32 = undefined;
            while (written < num_leaves) {
                const remaining_in_page = if (self.hasActivePage())
                    self.current_page_num_values - self.current_def_pos
                else
                    0;
                if (remaining_in_page > 0) {
                    const first = dict_ptr == null;
                    const usable = self.active == .rle_dict and self.dictionary != null and
                        self.dictionary.?.len != 0 and
                        (self.options.any_dictionary or self.dictionary.?.len <= num_leaves / 8);
                    if (first and !usable) {
                        // Nothing folded yet: decode this chunk as values in place instead of starting over.
                        return .{ .values = try self.finishAsValues(&.{}, null, 0, num_leaves) };
                    }
                    if (self.active != .rle_dict) return null;
                    const d = self.dictionary orelse return null;
                    if (dict_ptr) |p| {
                        if (p != d.ptr) return null;
                    } else {
                        dict_ptr = d.ptr;
                        hist = try self.arena.alloc(u64, d.len);
                        @memset(hist, 0);
                        if (track_first) {
                            first_leaf = try self.arena.alloc(u32, d.len);
                            @memset(first_leaf.?, no_leaf);
                            unfound = d.len;
                        }
                    }
                    const want = @min(num_leaves - written, remaining_in_page);
                    var present = want;
                    if (max_def > 0 and !self.current_def_all_present and self.hasDefLevels()) {
                        present = try self.countDefPresent(want);
                    }
                    if (track_first and present != want) return null;
                    const idx = &self.active.rle_dict.indices;
                    // Bounded by the buffer allocated, not by whatever dictionary is installed now.
                    const n_dict = hist.len;
                    if (n_dict == 0 or d.len != n_dict) return error.UnexpectedPage;
                    var left = present;
                    while (left > 0) {
                        // Leaves and present leaves coincide when first leaves are tracked (no nulls), and a tracked
                        // chunk has at most `no_leaf` of them. Untracked, the position can pass u32 and is not used.
                        const leaf = written + (present - left);
                        if (idx.takeRleRun(left) catch return error.UnexpectedPage) |run| {
                            if (run.value >= n_dict) return error.UnexpectedPage;
                            if (unfound > 0 and first_leaf.?[run.value] == no_leaf) {
                                first_leaf.?[run.value] = @intCast(leaf);
                                unfound -= 1;
                            }
                            hist[run.value] += run.count;
                            left -= run.count;
                            continue;
                        }
                        const take = @min(left, buf.len);
                        const got = idx.decode(buf[0..take]) catch return error.UnexpectedPage;
                        if (got == 0) return error.UnexpectedPage;
                        var oob: u32 = 0;
                        for (buf[0..got]) |k| {
                            oob |= @intFromBool(k >= n_dict);
                            hist[@min(k, n_dict - 1)] += 1;
                        }
                        if (oob != 0) return error.UnexpectedPage;
                        if (unfound > 0) for (buf[0..got], 0..) |k, j| {
                            if (first_leaf.?[k] == no_leaf) {
                                first_leaf.?[k] = @intCast(leaf + j);
                                unfound -= 1;
                            }
                        };
                        left -= got;
                    }
                    present_total += present;
                    written += want;
                    self.current_def_pos += want;
                    continue;
                }
                self.resetPageState();
                if (!try self.advancePage()) break;
            }
            if (written != num_leaves) return error.ShortDecode;
            if (num_leaves == 0) return null;
            const dict = self.dictionary orelse return null;
            const owned: []const T = if (T == []const u8) dict else try self.arena.dupe(T, dict);
            return .{ .hist = .{ .hist = hist, .dict = owned, .num_present = present_total, .first = first_leaf } };
        }

        /// Drain only the definition levels (count(col) needs nothing else). Pages are still
        /// decompressed (V1 levels sit inside the compressed body); no value is decoded. Null for nested columns.
        pub const LevelsResult = struct { def_levels: ?[]u32 };

        /// `def_levels` null: every leaf present.
        pub fn readAllLevels(self: *Self, num_leaves: usize) (Error || error{ShortDecode})!?LevelsResult {
            if (self.levels.max_rep > 0 or self.levels.max_def == 0) return null;
            self.options.fast_levels = true;
            self.value_budget = num_leaves;
            // No value is handed out, so a byte-array reader's pages can recycle the scratch too: what points into a
            // page (its dictionary entries, its values) is never read past it.
            if (T == []const u8) self.pages.scratch = self.scratch;
            try self.holdScratch();
            const max_def: u32 = @intCast(self.levels.max_def);
            var out_opt: ?[]u32 = null;
            var written: usize = 0;
            while (written < num_leaves) {
                const remaining_in_page = if (self.hasActivePage())
                    self.current_page_num_values - self.current_def_pos
                else
                    0;
                if (remaining_in_page > 0) {
                    const want = @min(num_leaves - written, remaining_in_page);
                    if (self.current_def_all_present or !self.hasDefLevels()) {
                        if (out_opt) |out| @memset(out[written..][0..want], max_def);
                    } else {
                        if (out_opt == null) {
                            out_opt = try self.arena.alloc(u32, num_leaves);
                            @memset(out_opt.?[0..written], max_def);
                        }
                        try self.readDefLevels(out_opt.?[written..][0..want]);
                    }
                    written += want;
                    self.current_def_pos += want;
                    continue;
                }
                self.resetPageState();
                if (!try self.advancePage()) break;
            }
            if (written != num_leaves) return error.ShortDecode;
            return .{ .def_levels = out_opt };
        }

        /// The next `def_out.len` leaves' definition levels, as `readAllLevels` reads them (no value decoded), for a
        /// reader taken a slice at a time: a count(col) then reads the same pages whichever way its budget cuts the
        /// row group. Returns the leaves read, fewer only at the chunk's end.
        pub fn readLevels(self: *Self, def_out: []u32) Error!usize {
            std.debug.assert(self.levels.max_rep == 0 and self.levels.max_def > 0);
            self.options.fast_levels = true;
            if (T == []const u8) self.pages.scratch = self.scratch;
            try self.holdScratch();
            const max_def: u32 = @intCast(self.levels.max_def);
            var written: usize = 0;
            while (written < def_out.len) {
                const left = self.pageValuesLeft();
                if (left > 0) {
                    const want = @min(def_out.len - written, left);
                    const dst = def_out[written..][0..want];
                    if (self.current_def_all_present or !self.hasDefLevels())
                        @memset(dst, max_def)
                    else
                        try self.readDefLevels(dst);
                    written += want;
                    self.current_def_pos += want;
                    continue;
                }
                self.resetPageState();
                if (!try self.advancePage()) break;
            }
            return written;
        }

        /// Indices come from the file: one past the dictionary would be an out-of-bounds read downstream.
        fn checkCodes(self: *Self, out: []const u32) Error!void {
            const dict = self.dictionary.?;
            if (out.len == 0) return;
            if (dict.len >= (@as(u64, 1) << @intCast(self.active.rle_dict.indices.bit_width))) return;
            const V = @Vector(8, u32);
            var acc: V = @splat(0);
            var i: usize = 0;
            while (i + 8 <= out.len) : (i += 8) acc = @max(acc, @as(V, out[i..][0..8].*));
            var mx: u32 = @reduce(.Max, acc);
            while (i < out.len) : (i += 1) mx = @max(mx, out[i]);
            if (mx >= dict.len) return error.UnexpectedPage;
        }

        /// Dictionary fallback mid-chunk: gather the leaves decoded as codes so far, then decode the rest as values.
        fn finishAsValues(
            self: *Self,
            codes: []const u32,
            def_out: ?[]u32,
            written: usize,
            num_leaves: usize,
        ) (Error || error{ShortDecode})!Leaves {
            const values = try self.arena.alloc(T, num_leaves);
            if (written > 0) {
                const dict = self.dictionary orelse return error.DictionaryMissing;
                for (values[0..written], codes[0..written]) |*v, k| {
                    if (k >= dict.len) return error.UnexpectedPage;
                    v.* = dict[k];
                }
            }
            const max_def: u32 = @intCast(self.levels.max_def);
            var w = written;
            // While no page has had a null, pages proven all-present still decode without levels, as `readAll` does
            // them: at max_def 1 only, where a missing level array means level 1 everywhere to every reader. Fixed
            // width only: byte-array chunks read this way peaked 23% higher (a long-string count DISTINCT, a LIKE
            // scan) for no time saved, cause not yet found.
            if (T != []const u8 and max_def == 1 and def_out == null and self.options.fast_levels) {
                w += try self.decodeAllPresent(values[w..]);
                if (w == num_leaves) return .{ .values = values, .has_nulls = self.has_nulls };
            }
            // Values decode needs expanded levels from here on.
            self.options.fast_levels = false;
            var def_buf = def_out;
            if (max_def > 0 and def_buf == null) {
                def_buf = try self.arena.alloc(u32, num_leaves);
                @memset(def_buf.?[0..w], max_def);
            }
            if (def_buf) |dl| {
                for (values[0..written], dl[0..written]) |*v, dv| {
                    if (dv < max_def) v.* = defaultValue();
                }
                while (w < num_leaves) {
                    const n = try self.decodeWithLevels(values[w..], dl[w..]);
                    if (n == 0) break;
                    w += n;
                }
                if (w != num_leaves) return error.ShortDecode;
                return .{ .values = values, .def_levels = dl, .has_nulls = self.has_nulls };
            }
            while (w < num_leaves) {
                const n = try self.decode(values[w..]);
                if (n == 0) break;
                w += n;
            }
            if (w != num_leaves) return error.ShortDecode;
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

        /// Values the active page has left; 0 when no page is active or it is drained.
        pub fn pageValuesLeft(self: *const Self) usize {
            if (!self.hasActivePage()) return 0;
            return self.current_page_num_values - self.current_def_pos;
        }

        /// Leave the current page and install the next data page: false at the chunk's end. With `pageValuesLeft`,
        /// lets a caller decode a page at a time and free what the drained page allocated before reading the next.
        pub fn nextPage(self: *Self) Error!bool {
            try self.holdScratch();
            self.resetPageState();
            return self.advancePage();
        }

        /// The chunk's dictionary, when it opens with one and every data page after it is dictionary-encoded; null
        /// otherwise (a PLAIN page after the dictionary outgrew its page holds values the dictionary lacks). Reads
        /// every page header first, and the dictionary page only once they all qualify; never a data page's body.
        /// For a fresh reader.
        pub fn dictionaryOfEveryPage(self: *Self) Error!?[]const T {
            try self.holdScratch();
            var dict_at: ?usize = null;
            while (try self.pages.peek()) |p| {
                switch (p.header.type) {
                    .DICTIONARY_PAGE => {
                        if (dict_at != null) return error.UnexpectedDictionaryPage;
                        dict_at = self.pages.pos;
                    },
                    .INDEX_PAGE => {},
                    .DATA_PAGE, .DATA_PAGE_V2 => {
                        if (dict_at == null) return null;
                        const encoding = if (p.header.data_page_header) |h|
                            h.encoding
                        else if (p.header.data_page_header_v2) |h| h.encoding else return error.UnexpectedPage;
                        if (encoding != .RLE_DICTIONARY and encoding != .PLAIN_DICTIONARY) return null;
                    },
                }
                self.pages.pass(p);
            }
            self.pages.pos = dict_at orelse return null;
            try self.installDictionary((try self.pages.next()).?);
            return self.dictionary;
        }

        /// With the current page drained, pass over the next data page of this flat chunk by its header alone, never
        /// decompressing it, when it holds at most `max_rows` rows; a dictionary page in the way is installed first.
        /// The rows passed over, or null when the next page must be installed to be read (it holds more rows, or the
        /// chunk is done).
        pub fn passPageWithin(self: *Self, max_rows: usize) Error!?usize {
            std.debug.assert(self.levels.max_rep == 0 and self.pageValuesLeft() == 0);
            try self.holdScratch();
            self.resetPageState();
            while (try self.pages.peek()) |p| switch (p.header.type) {
                .DICTIONARY_PAGE => try self.installDictionary((try self.pages.next()).?),
                .INDEX_PAGE => self.pages.pass(p),
                .DATA_PAGE, .DATA_PAGE_V2 => {
                    // A flat column's leaves are its rows, nulls included.
                    const rows: usize = if (p.header.data_page_header) |h|
                        @intCast(h.num_values)
                    else if (p.header.data_page_header_v2) |h| @intCast(h.num_values) else return error.UnexpectedPage;
                    if (rows > max_rows) return null;
                    try self.chargePage(rows);
                    self.data_page_seen = true;
                    self.pages.pass(p);
                    self.gate_row +|= rows;
                    return rows;
                },
            };
            return null;
        }

        fn resetPageState(self: *Self) void {
            self.active = .idle;
            self.pending_expansion = 0;
            self.current_def_levels = null;
            self.current_def_stream = null;
            self.current_def_nulls = 0;
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

            try self.readDefLevels(def_levels_out);
            const src = def_levels_out;

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
            // No branch on the level: the read index steps by the present bit and a select picks the value, since a
            // column with real nulls mispredicts any branch on them. Reading `values[read]` at a null slot is harmless:
            // read <= i there, and the read is discarded.
            var read: usize = num_present;
            var i: usize = values.len;
            while (i > 0) {
                i -= 1;
                const present = src[i] == max_def;
                read -= @intFromBool(present);
                const v = values[@min(read, values.len - 1)];
                values[i] = if (present) v else defaultValue();
            }
        }

        fn decodePackedFromCurrentPage(self: *Self, dest: []T) Error!usize {
            if (self.pending_expansion > 0) try self.chargeExpansion();
            const got = switch (self.active) {
                .idle => 0,
                inline else => |*d| if (@TypeOf(d.*) == void)
                    unreachable
                else
                    d.decode(dest) catch |e| return pageError(e),
            };
            // A dictionary's codes index entries checked when it was installed.
            if (T == i32 and self.active != .rle_dict) try self.checkIntRange(dest[0..got]);
            return got;
        }

        /// The caller gave the pages decoded so far back (their arena reset): what they expanded to is no longer
        /// held, and is not counted against the next.
        pub fn releaseExpansion(self: *Self) void {
            self.expanded = 0;
        }

        fn chargeExpansion(self: *Self) Error!void {
            self.expanded +|= self.pending_expansion;
            self.pending_expansion = 0;
            if (self.expanded > self.options.expand_limit) return error.DecodeExceededBudget;
        }

        /// `error.IntOutOfRange` for a value past the column's 8- or 16-bit integer annotation (`IntRange`).
        fn checkIntRange(self: *const Self, values: []const i32) Error!void {
            const r = self.options.int_range orelse return;
            const L = 16;
            const V = @Vector(L, i32);
            var lo: V = @splat(r.min);
            var hi: V = @splat(r.max);
            var i: usize = 0;
            while (i + L <= values.len) : (i += L) {
                const v: V = values[i..][0..L].*;
                lo = @min(lo, v);
                hi = @max(hi, v);
            }
            var min = @reduce(.Min, lo);
            var max = @reduce(.Max, hi);
            for (values[i..]) |v| {
                min = @min(min, v);
                max = @max(max, v);
            }
            if (min < r.min or max > r.max) return error.IntOutOfRange;
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
            const rle_len: usize = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (rle_len > values_bytes.len - 4) return error.UnexpectedPage;
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
            while (true) {
                if (self.gate) |gate| if (try self.pages.peek()) |p| if (p.header.type == .DATA_PAGE or
                    p.header.type == .DATA_PAGE_V2)
                {
                    // Its header is read and checked like any other; only its body goes unread.
                    const rows = try flatPageRows(p.header);
                    const lo = @min(self.gate_row, gate.len);
                    if (!gate.anyInRange(lo, @min(self.gate_row +| rows, gate.len))) {
                        self.pages.pass(p);
                        self.pages.ordinal +|= 1;
                        try self.installNullPage(rows);
                        self.gate_row +|= rows;
                        return true;
                    }
                };
                const pg = (try self.pages.next()) orelse return false;
                switch (pg.header.type) {
                    .DICTIONARY_PAGE => {
                        try self.installDictionary(pg);
                        // Loop to the next page (likely the data page).
                    },
                    .DATA_PAGE => {
                        try self.installDataPageDecoder(pg);
                        self.gate_row +|= self.current_page_num_values;
                        return true;
                    },
                    .DATA_PAGE_V2 => {
                        try self.installDataPageV2Decoder(pg);
                        self.gate_row +|= self.current_page_num_values;
                        return true;
                    },
                    .INDEX_PAGE => continue, // skip; not used by decode
                }
            }
        }

        /// A flat column's leaves are its rows, nulls included.
        fn flatPageRows(header: schema.PageHeader) Error!usize {
            const n = if (header.data_page_header) |h| h.num_values else if (header.data_page_header_v2) |h|
                h.num_values
            else
                return error.UnexpectedPage;
            return std.math.cast(usize, n) orelse error.UnexpectedPage;
        }

        /// A page of `rows` nulls, standing for a gated page: every slot reads as absent.
        fn installNullPage(self: *Self, rows: usize) Error!void {
            self.data_page_seen = true;
            try self.beginPage(rows);
            const buf = try self.levelBuffer(.def, rows);
            @memset(buf, 0);
            self.current_def_levels = buf;
            try self.installValues(.PLAIN, &.{}, 0);
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
            return (self.dictionary_arena orelse self.arena).alloc(T, n);
        }

        fn installDictionary(self: *Self, pg: page_mod.Page) Error!void {
            if (self.dictionary != null or self.data_page_seen) return error.UnexpectedDictionaryPage;
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
            // A byte-array dictionary's slices, drawn before they are built (an uncompressed page draws nothing else).
            if (T == []const u8) if (self.pages.budget) |b| {
                const slices = num * @sizeOf([]const u8);
                _ = b.drawExact(slices) orelse return error.ExceededMemoryBudget;
                self.pages.drawn += slices;
            };
            const buf = try self.dictBuffer(num);
            switch (T) {
                i32, i64, f32, f64 => {
                    var d = plain.Decoder(T).init(pg.bytes);
                    const n = d.decode(buf) catch return error.UnexpectedPage;
                    if (n != num) return error.UnexpectedPage;
                    if (T == i32) try self.checkIntRange(buf);
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
            self.data_page_seen = true;
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
                const rep_len: usize = std.mem.readInt(u32, values_bytes[0..4], .little);
                if (rep_len > values_bytes.len - 4) return error.UnexpectedPage;

                try self.installRepLevels(values_bytes[4 .. 4 + rep_len], num_values);
                values_bytes = values_bytes[4 + rep_len ..];
            }
            if (self.levels.max_def > 0) {
                if (values_bytes.len < 4) return error.UnexpectedPage;
                const def_len: usize = std.mem.readInt(u32, values_bytes[0..4], .little);
                if (def_len > values_bytes.len - 4) return error.UnexpectedPage;

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
            self.data_page_seen = true;
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
                // cannot turn it into an incorrect answer.
                try self.installDefLevels(pg.bytes[cursor..][0..def_len], num_values);
                cursor += def_len;
            } else if (def_len != 0) {
                cursor += def_len;
            }
            // V2 with nullable cols: the values hold only the non-null entries, which bounds a PLAIN BOOLEAN stream.
            // Counted from the levels, not the header's `num_nulls`, which writers have miscounted: a lying count is
            // ignored, as other readers ignore it, and a corrupt level run is caught by the run check
            // (`hybrid_rle.decodeLevels`). The other decoders are streams that consume only what's asked for.
            try self.installValues(dph.encoding, pg.bytes[cursor..], num_values - self.pageNulls());
        }

        /// Page state shared by both data-page versions, set before either parses its levels.
        fn beginPage(self: *Self, num_values: usize) Error!void {
            try self.chargePage(num_values);
            self.pending_expansion = 0;
            self.current_def_levels = null;
            self.current_def_stream = null;
            self.current_def_nulls = 0;
            self.current_rep_levels = null;
            self.current_def_all_present = false;
            self.current_def_pos = 0;
            self.current_page_num_values = num_values;
        }

        /// Whether a delta byte-array page's values fit the column: a FIXED_LEN_BYTE_ARRAY value is `type_length`
        /// bytes however it is encoded, and these encodings carry a length per value (`prefixes` empty for
        /// DELTA_LENGTH_BYTE_ARRAY). Every consumer of the column trusts the width.
        fn fixedLengths(self: *const Self, prefixes: []const i32, lengths: []const i32) bool {
            if (self.type_length == 0) return true;
            const width: i64 = @intCast(self.type_length);
            for (lengths, 0..) |l, i| {
                const p: i64 = if (prefixes.len == 0) 0 else prefixes[i];
                if (p + l != width) return false;
            }
            return true;
        }

        /// The installed page's levels below max_def.
        fn pageNulls(self: *const Self) usize {
            if (self.current_def_stream != null) return self.current_def_nulls;
            const levels = self.current_def_levels orelse return 0;
            const max_def: u32 = @intCast(self.levels.max_def);
            const L = 16;
            const V = @Vector(L, u32);
            const top: V = @splat(max_def);
            var n: usize = 0;
            var i: usize = 0;
            while (i + L <= levels.len) : (i += L) n += std.simd.countTrues(@as(V, levels[i..][0..L].*) < top);
            for (levels[i..]) |d| n += @intFromBool(d < max_def);
            return n;
        }

        fn installRepLevels(self: *Self, rep_bytes: []const u8, num_values: usize) Error!void {
            const buf = try self.levelBuffer(.rep, num_values);
            try levelsInto(rep_bytes, self.levels.max_rep, buf);
            self.current_rep_levels = buf;
        }

        fn hasDefLevels(self: *const Self) bool {
            return self.current_def_levels != null or self.current_def_stream != null;
        }

        fn readDefLevels(self: *Self, out: []u32) Error!void {
            if (self.current_def_stream) |*stream| {
                const got = stream.decode(out) catch return error.UnexpectedPage;
                if (got != out.len) return error.DefLevelsMismatch;
            } else {
                @memcpy(out, self.current_def_levels.?[self.current_def_pos..][0..out.len]);
            }
        }

        fn countDefPresent(self: *Self, n: usize) Error!usize {
            const max_def: u32 = @intCast(self.levels.max_def);
            if (self.current_def_stream) |*stream| {
                return stream.countPresentLevels(max_def, n) catch |err| return levelError(err);
            }
            var count: usize = 0;
            for (self.current_def_levels.?[self.current_def_pos..][0..n]) |v| count += @intFromBool(v == max_def);
            return count;
        }

        fn installDefLevels(self: *Self, def_bytes: []const u8, num_values: usize) Error!void {
            const bit_width = bitWidthFor(@intCast(self.levels.max_def));
            if (self.canSkipDefLevels(def_bytes, bit_width, num_values)) {
                self.current_def_all_present = true;
                return;
            }
            if (self.options.fast_levels and self.levels.max_rep == 0 and
                (self.options.level_runs orelse
                    hybrid_rle.HybridRleDecoder.preferLevelRuns(def_bytes, bit_width, num_values)))
            {
                const stream = hybrid_rle.HybridRleDecoder.init(def_bytes, bit_width);
                var validation = stream;
                const present = validation.countPresentLevels(@intCast(self.levels.max_def), num_values) catch |err|
                    return levelError(err);
                self.current_def_nulls = num_values - present;
                if (present == num_values) {
                    self.current_def_all_present = true;
                } else {
                    self.current_def_stream = stream;
                }
                return;
            }
            const buf = try self.levelBuffer(.def, num_values);
            try levelsInto(def_bytes, self.levels.max_def, buf);
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
                    const d = dba.DeltaLengthByteArrayDecoder.init(values_bytes, self.arena, num_values) catch |e|
                        return pageError(e);
                    if (!self.fixedLengths(&.{}, d.lengths)) return error.UnexpectedPage;
                    self.active = .{ .delta_len_ba = d };
                },
                .DELTA_BYTE_ARRAY => {
                    if (T != []const u8) return error.UnsupportedEncoding;
                    // Charged when its values are decoded (`chargeExpansion`), which levels alone never are.
                    const unbounded = std.math.maxInt(usize);
                    const d = dba.DeltaByteArrayDecoder.init(values_bytes, self.arena, num_values, unbounded) catch |e|
                        return pageError(e);
                    self.pending_expansion = d.expanded;
                    if (!self.fixedLengths(d.prefix_lengths, d.suffixes.lengths)) return error.UnexpectedPage;
                    self.active = .{ .delta_ba = d };
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

/// Append one uncompressed page of `body` to `chunk`: a PLAIN dictionary page of `n` INT32 entries, or a V1 data page
/// of `n` RLE_DICTIONARY indices.
fn appendPageForTest(arena: std.mem.Allocator, chunk: *std.ArrayList(u8), dictionary: bool, n: i32, body: []const u8) !void {
    const header = schema.PageHeader{
        .type = if (dictionary) .DICTIONARY_PAGE else .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.len),
        .compressed_page_size = @intCast(body.len),
        .crc = null,
        .data_page_header = if (dictionary) null else .{
            .num_values = n,
            .encoding = .RLE_DICTIONARY,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = if (!dictionary) null else .{ .num_values = n, .encoding = .PLAIN, .is_sorted = null },
        .data_page_header_v2 = null,
    };
    var w = thrift.Writer.init(arena);
    try header.write(&w);
    try chunk.appendSlice(arena, w.bytes());
    try chunk.appendSlice(arena, body);
}

test "a second dictionary page, after data or after the first, is a corrupt chunk" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    const dict3 = std.mem.toBytes([3]i32{ 10, 20, 30 });
    const dict1 = std.mem.toBytes([1]i32{40});
    // Four indices at bit width 2: one RLE run of 4 x index 2 (the last entry of the first dictionary).
    const data = [_]u8{ 2, 4 << 1, 2 };
    const empty = [_]u8{};

    for ([_]bool{ false, true }) |back_to_back| for ([_]bool{ false, true }) |second_empty| {
        var chunk: std.ArrayList(u8) = .empty;
        try appendPageForTest(arena, &chunk, true, 3, &dict3);
        if (!back_to_back) try appendPageForTest(arena, &chunk, false, 4, &data);
        // The replacement is smaller, or holds no entry at all, so the first dictionary's codes overrun it.
        if (second_empty) try appendPageForTest(arena, &chunk, true, 0, &empty) else try appendPageForTest(arena, &chunk, true, 1, &dict1);
        try appendPageForTest(arena, &chunk, false, 4, &data);
        const n: usize = if (back_to_back) 4 else 8;

        var values_reader = ColumnChunkReader(i32).init(chunk.items, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, arena);
        var out: [8]i32 = undefined;
        try testing.expectError(error.UnexpectedDictionaryPage, values_reader.decode(out[0..n]));

        var hist_reader = ColumnChunkReader(i32).init(chunk.items, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, arena);
        try testing.expectError(error.UnexpectedDictionaryPage, hist_reader.readAllHistogram(n * 8, false));
    };
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

test "a level-length prefix near 2^32 is a corrupt page, not an overflow" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    const values = [_]?[]const u8{ "ab", null, "cd" };
    for ([_]u32{ 0xFFFF_FFFC, 0xFFFF_FFFF }) |claim| {
        const chunk = try flbaPageForTest(arena, false, 2, true, &values);
        // The V1 body starts with the def levels' u32 length (2), then their bytes (a bit-packed run header, 0x03).
        const at = std.mem.indexOf(u8, chunk, &.{ 2, 0, 0, 0, 3 }) orelse return error.TestUnexpectedResult;
        std.mem.writeInt(u32, chunk[at..][0..4], claim, .little);
        var reader = ColumnChunkReader([]const u8).init(chunk, .UNCOMPRESSED, .{ .max_def = 1, .max_rep = 0 }, arena);
        reader.type_length = 2;
        var got: [3][]const u8 = undefined;
        var defs: [3]u32 = undefined;
        try testing.expectError(error.UnexpectedPage, reader.decodeWithLevels(&got, &defs));
    }
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

/// Fuzz target: arbitrary bytes as a column chunk of a chosen type, codec and level shape, decoded as values, as a
/// code histogram or as codes; every outcome but an error is the reader's to answer for.
fn fuzzInflateLimit(input_len: usize) usize {
    return (input_len * 64) * 2 + (1 << 20);
}

fn fuzzChunk(_: void, smith: *std.testing.Smith) anyerror!void {
    const Kind = enum(u3) { i32, i64, f64, bytes, flba, boolean };
    const Mode = enum(u2) { values, histogram, codes };
    const kind = smith.value(Kind);
    const mode = smith.value(Mode);
    const codec: schema.CompressionCodec = switch (smith.value(u2)) {
        0 => .UNCOMPRESSED,
        1 => .SNAPPY,
        2 => .ZSTD,
        3 => .GZIP,
    };
    const levels: schema.Levels = .{ .max_def = smith.valueRangeAtMost(u8, 0, 2), .max_rep = 0 };
    const want: usize = smith.valueRangeAtMost(u16, 0, 512);
    var buf: [16 * 1024]u8 = undefined;
    const n = smith.slice(&buf);
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    switch (kind) {
        inline else => |k| {
            const T = switch (k) {
                .i32 => i32,
                .i64 => i64,
                .f64 => f64,
                .bytes, .flba => []const u8,
                .boolean => bool,
            };
            // Bounded as production bounds a chunk (`chunkOptions`): what `inflateLimit` allows a footer claiming the
            // input inflates 64-fold. A page header's size claim alone otherwise sized an allocation, and fuzz
            // instances ran out of memory in the allocator instead of exercising decode.
            const limit = fuzzInflateLimit(n);
            const options: DecodeOptions = .{ .page_bytes_limit = limit, .expand_limit = limit };
            var reader = ColumnChunkReader(T).initWithOptions(buf[0..n], codec, levels, arena, options, null);
            if (k == .flba) reader.type_length = smith.valueRangeAtMost(u8, 1, 32);
            switch (mode) {
                .values => {
                    const out = try arena.alloc(T, want);
                    if (levels.max_def > 0) {
                        const defs = try arena.alloc(u32, want);
                        _ = reader.decodeWithLevels(out, defs) catch return;
                    } else _ = reader.decode(out) catch return;
                },
                .histogram => if (T != bool) {
                    _ = reader.readAllHistogram(want, smith.value(bool)) catch return;
                },
                .codes => if (T != bool) {
                    _ = reader.readAllCodes(want) catch return;
                },
            }
        },
    }
}

test "regression: the fuzz target bounds a page's claimed size, as production does, before allocating it" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    // A page whose header claims 2^31-1 inflated bytes behind a few bytes of body.
    const body = [_]u8{ 0xff, 0xff, 0xff, 0xff, 0x07, 0, 0, 0 };
    const header = schema.PageHeader{
        .type = .DATA_PAGE,
        .uncompressed_page_size = std.math.maxInt(i32),
        .compressed_page_size = body.len,
        .crc = null,
        .data_page_header = .{ .num_values = 4, .encoding = .PLAIN, .definition_level_encoding = .RLE, .repetition_level_encoding = .RLE },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var w = thrift.Writer.init(arena);
    try header.write(&w);
    const chunk = try std.mem.concat(arena, u8, &.{ w.bytes(), &body });
    const limit = fuzzInflateLimit(chunk.len);
    for ([_]schema.CompressionCodec{ .SNAPPY, .ZSTD, .GZIP }) |codec| {
        var reader = ColumnChunkReader(i64).initWithOptions(chunk, codec, .{ .max_def = 0, .max_rep = 0 }, arena, .{ .page_bytes_limit = limit }, null);
        var out: [4]i64 = undefined;
        try testing.expectError(error.PageLargerThanChunk, reader.decode(&out));
    }
}

test "fuzz: column chunk bytes decode or fail with an error, never panic" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // Seeds: a dictionary-encoded chunk and PLAIN fixed-width pages, V1 and V2, with and without nulls.
    var dict_chunk: std.ArrayList(u8) = .empty;
    try appendPageForTest(arena, &dict_chunk, true, 3, &std.mem.toBytes([3]i32{ 10, 20, 30 }));
    try appendPageForTest(arena, &dict_chunk, false, 4, &.{ 2, 4 << 1, 2 });
    const values = [_]?[]const u8{ "ab", null, "cd" };
    const seeds = [_][]const u8{
        dict_chunk.items,
        try flbaPageForTest(arena, false, 2, true, &values),
        try flbaPageForTest(arena, true, 2, true, &values),
        try flbaPageForTest(arena, false, 2, false, &.{ "ab", "cd" }),
    };
    try std.testing.fuzz({}, fuzzChunk, .{ .corpus = &seeds });
}

test "readAllHistogram notes each entry's first leaf over RLE and bit-packed runs, and declines nulls when it does" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    const dict = std.mem.toBytes([6]i32{ 60, 10, 50, 20, 40, 30 });
    var prng = std.Random.DefaultPrng.init(0x4157);
    const r = prng.random();
    // Three pages: a long run of one entry, random codes (bit-packed), then a run of a late newcomer.
    var codes: [3][]u32 = undefined;
    for (&codes, [_]usize{ 700, 333, 90 }, 0..) |*page, n, pi| {
        page.* = try arena.alloc(u32, n);
        for (page.*) |*c| c.* = switch (pi) {
            0 => 2,
            1 => r.uintLessThan(u32, 4),
            else => 5,
        };
    }
    var chunk: std.ArrayList(u8) = .empty;
    try appendPageForTest(arena, &chunk, true, 6, &dict);
    for (codes) |page| {
        var body: std.ArrayList(u8) = .empty;
        try body.append(arena, 3);
        try body.appendSlice(arena, try hybrid_rle.encode(arena, page, 3));
        try appendPageForTest(arena, &chunk, false, @intCast(page.len), body.items);
    }
    const n_leaves = 700 + 333 + 90;
    var want_hist: [6]u64 = @splat(0);
    var want_first: [6]u32 = @splat(ColumnChunkReader(i32).no_leaf);
    var leaf: u32 = 0;
    for (codes) |page| for (page) |c| {
        want_hist[c] += 1;
        if (want_first[c] == ColumnChunkReader(i32).no_leaf) want_first[c] = leaf;
        leaf += 1;
    };
    for ([_]bool{ false, true }) |track| {
        var reader = ColumnChunkReader(i32).init(chunk.items, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, arena);
        const got = (try reader.readAllHistogram(n_leaves, track)).?.hist;
        try testing.expectEqualSlices(u64, &want_hist, got.hist);
        if (track) {
            try testing.expectEqualSlices(u32, &want_first, got.first.?);
        } else try testing.expect(got.first == null);
    }

    // An OPTIONAL column with one null leaf: counted out without first leaves, declined with them.
    const levels = [_]u32{ 1, 1, 0, 1 } ++ @as([60]u32, @splat(1));
    const present = [_]u32{ 1, 3, 1 } ++ @as([60]u32, @splat(0));
    var body: std.ArrayList(u8) = .empty;
    const def_bytes = try hybrid_rle.encode(arena, &levels, 1);
    try body.appendSlice(arena, &std.mem.toBytes(@as(u32, @intCast(def_bytes.len))));
    try body.appendSlice(arena, def_bytes);
    try body.append(arena, 3);
    try body.appendSlice(arena, try hybrid_rle.encode(arena, &present, 3));
    var nulls: std.ArrayList(u8) = .empty;
    try appendPageForTest(arena, &nulls, true, 6, &dict);
    try appendPageForTest(arena, &nulls, false, levels.len, body.items);
    for ([_]bool{ false, true }) |track| {
        var reader = ColumnChunkReader(i32).init(nulls.items, .UNCOMPRESSED, .{ .max_def = 1, .max_rep = 0 }, arena);
        const got = try reader.readAllHistogram(levels.len, track);
        if (track) {
            try testing.expect(got == null);
        } else {
            try testing.expectEqual(@as(usize, levels.len - 1), got.?.hist.num_present);
        }
    }
}

test "readAllHistogram with first leaves unwinds cleanly on every allocation failure" {
    const dict = std.mem.toBytes([4]i32{ 1, 2, 3, 4 });
    var scratch_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch_arena.deinit();
    const sa = scratch_arena.allocator();
    var chunk: std.ArrayList(u8) = .empty;
    try appendPageForTest(sa, &chunk, true, 4, &dict);
    // At least 8 leaves an entry, or the chunk decodes as values.
    const codes = @as([9]u32, @splat(3)) ++ [_]u32{ 0, 1, 2 } ++ @as([52]u32, @splat(1));
    var body: std.ArrayList(u8) = .empty;
    try body.append(sa, 2);
    try body.appendSlice(sa, try hybrid_rle.encode(sa, &codes, 2));
    try appendPageForTest(sa, &chunk, false, codes.len, body.items);
    try std.testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, bytes: []const u8) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const flat: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
            var reader = ColumnChunkReader(i32).init(bytes, .UNCOMPRESSED, flat, arena.allocator());
            const got = (try reader.readAllHistogram(64, true)).?.hist;
            try testing.expectEqualSlices(u32, &.{ 9, 10, 11, 0 }, got.first.?);
        }
    }.run, .{chunk.items});
}

test "readAllCodes falling back to values skips levels on all-present pages until a page has a null" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    // 40 entries for 64 leaves: near-unique, so the codes read gives way to values from the first page.
    var dict_vals: [40]i64 = undefined;
    for (&dict_vals, 0..) |*d, i| d.* = @as(i64, @intCast(i)) * 11 - 7;
    const dict = std.mem.asBytes(&dict_vals);
    const page_len = 16;
    const Page = struct { levels: [page_len]u32, codes: []const u32 };
    const all: [page_len]u32 = @splat(1);
    var with_null = all;
    with_null[5] = 0;
    const pages = [_]Page{
        .{ .levels = all, .codes = &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 } },
        .{ .levels = all, .codes = &.{ 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 } },
        .{ .levels = with_null, .codes = &.{ 32, 33, 34, 35, 36, 37, 38, 39, 0, 1, 2, 3, 4, 5, 6 } },
        .{ .levels = all, .codes = &.{ 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24 } },
    };
    for ([_]usize{ 2, 4 }) |n_pages| {
        var chunk: std.ArrayList(u8) = .empty;
        try appendPageForTest(arena, &chunk, true, dict_vals.len, dict);
        var want_values: std.ArrayList(i64) = .empty;
        var want_levels: std.ArrayList(u32) = .empty;
        for (pages[0..n_pages]) |page| {
            var body: std.ArrayList(u8) = .empty;
            const def_bytes = try hybrid_rle.encode(arena, &page.levels, 1);
            try body.appendSlice(arena, &std.mem.toBytes(@as(u32, @intCast(def_bytes.len))));
            try body.appendSlice(arena, def_bytes);
            try body.append(arena, 6);
            try body.appendSlice(arena, try hybrid_rle.encode(arena, page.codes, 6));
            try appendPageForTest(arena, &chunk, false, page_len, body.items);
            var next: usize = 0;
            for (page.levels) |l| {
                try want_levels.append(arena, l);
                if (l == 1) {
                    try want_values.append(arena, dict_vals[page.codes[next]]);
                    next += 1;
                } else try want_values.append(arena, 0);
            }
        }
        const n = n_pages * page_len;
        const levels: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
        var reader = ColumnChunkReader(i64).init(chunk.items, .UNCOMPRESSED, levels, arena);
        const got = (try reader.readAllCodes(n)).?.values;
        var sliced = ColumnChunkReader(i64).initWithOptions(chunk.items, .UNCOMPRESSED, levels, arena, .{ .any_dictionary = true, .fast_levels = true }, null);
        sliced.value_budget = n;
        var done: usize = 0;
        while (done < n) {
            if (sliced.pageValuesLeft() == 0) try testing.expect(try sliced.nextPage());
            const take = @min(7, sliced.pageValuesLeft());
            var codes: [7]u32 = undefined;
            var defs: [7]u32 = undefined;
            try testing.expect(try sliced.readPageCodes(codes[0..take], defs[0..take]));
            for (0..take) |i| {
                try testing.expectEqual(want_levels.items[done + i], defs[i]);
                if (defs[i] != 0)
                    try testing.expectEqual(want_values.items[done + i], sliced.dictionary.?[codes[i]]);
            }
            done += take;
        }

        try testing.expectEqualSlices(i64, want_values.items, got.values);
        if (n_pages == 2) {
            // Every page all-present: no level array at all, meaning level 1 everywhere.
            try testing.expect(got.def_levels == null);
        } else {
            try testing.expectEqualSlices(u32, want_levels.items, got.def_levels.?);
            try testing.expect(got.has_nulls);
        }
    }
}

test "regression: a histogram past 2^32 leaves counts untracked and is declined tracked" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    var chunk: std.ArrayList(u8) = .empty;
    const dict = std.mem.toBytes([1]i32{7});
    try appendPageForTest(arena, &chunk, true, 1, &dict);
    // Bit width 0, then one RLE run of 2^30 (header 2^31 as a varint; a width-0 run has no value byte).
    const body = [_]u8{ 0, 0x80, 0x80, 0x80, 0x80, 0x08 };
    for (0..5) |_| try appendPageForTest(arena, &chunk, false, 1 << 30, &body);
    const n: usize = 5 << 30;
    const flat: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
    var untracked = ColumnChunkReader(i32).init(chunk.items, .UNCOMPRESSED, flat, arena);
    try testing.expectEqual(@as(u64, n), (try untracked.readAllHistogram(n, false)).?.hist.hist[0]);
    var tracked = ColumnChunkReader(i32).init(chunk.items, .UNCOMPRESSED, flat, arena);
    try testing.expect(try tracked.readAllHistogram(n, true) == null);
}

test "flat level runs validate eagerly and remain encoded until the consumer window" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var levels: [1543]u32 = undefined;
    for (&levels, 0..) |*v, i| v.* = if (i < 700) 1 else if (i < 1300) 0 else @intCast(i % 2);
    const encoded = try hybrid_rle.encode(a, &levels, 1);
    for ([_]bool{ false, true }) |fast| {
        var reader = ColumnChunkReader(i32).initWithOptions(
            &.{},
            .UNCOMPRESSED,
            .{ .max_def = 1, .max_rep = 0 },
            a,
            .{ .fast_levels = fast },
            null,
        );
        try reader.installDefLevels(encoded, levels.len);
        try testing.expectEqual(fast, reader.current_def_stream != null);
        try testing.expectEqual(!fast, reader.current_def_levels != null);
        var nulls: usize = 0;
        for (levels) |v| nulls += @intFromBool(v == 0);
        try testing.expectEqual(nulls, reader.pageNulls());
        var buf: [63]u32 = undefined;
        var off: usize = 0;
        while (off < levels.len) {
            const n = @min(buf.len, levels.len - off);
            if ((off / buf.len) % 2 == 0) {
                try reader.readDefLevels(buf[0..n]);
                try testing.expectEqualSlices(u32, levels[off..][0..n], buf[0..n]);
            } else {
                var present: usize = 0;
                for (levels[off..][0..n]) |v| present += v;
                try testing.expectEqual(present, try reader.countDefPresent(n));
            }
            off += n;
            reader.current_def_pos = off;
        }
        reader.resetPageState();
        try testing.expect(!reader.hasDefLevels());
    }
    // The first level is outside max_def, but the truncated next RLE run wins before any value is decoded.
    for ([_]bool{ false, true }) |fast| {
        var reader = ColumnChunkReader(i32).initWithOptions(
            &.{},
            .UNCOMPRESSED,
            .{ .max_def = 2, .max_rep = 0 },
            a,
            .{ .fast_levels = fast },
            null,
        );
        try testing.expectError(error.UnexpectedPage, reader.installDefLevels(&.{ 2, 3, 2 }, 2));
        try testing.expectError(error.DefLevelsMismatch, reader.installDefLevels(&.{ 2, 3 }, 2));
        try testing.expectError(error.LevelOutOfRange, reader.installDefLevels(&.{ 4, 3 }, 2));
    }
}
