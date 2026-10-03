//! Page reader: walk pages within a column chunk.
//!
//! A Parquet column chunk is a sequence of pages laid out back-to-back:
//!
//!   [PageHeader (thrift)][page payload][PageHeader][page payload]...
//!
//! For each page we parse the thrift header (which tells us the
//! compressed/uncompressed sizes and the page type), slice the payload,
//! and run it through the codec dispatcher.
//!
//! The first page of a dictionary-encoded column is the dictionary page;
//! subsequent pages are data pages that reference dictionary indices.
//! Higher-level orchestration (column.zig) decides what to do with each
//! page kind; this layer just walks them.

const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const compression = @import("compression.zig");

pub const Error = error{
    UnexpectedEndOfChunk,
    BadPageHeader,
    NegativeSize,
    /// A page header's value / null / row count is negative, or claims more nulls than values.
    BadPageCount,
} || compression.Error;

pub const Page = struct {
    /// The parsed page header. Use `header.type`, `header.data_page_header`
    /// (when type=DATA_PAGE), or `header.dictionary_page_header` (when
    /// type=DICTIONARY_PAGE) to drive the next decoding step.
    header: schema.PageHeader,
    /// Decompressed page payload bytes. Lifetime = `arena`.
    bytes: []const u8,
};

/// Grow-only decode buffers owned by one worker and reused across pages, column chunks, row groups and files, so a
/// worker faults its decode working set in once instead of once per page from a fresh arena chunk.
///
/// Not thread-safe, and at most one ColumnChunkReader may draw on a scratch at a time. Lifetimes: `page` and the
/// level buffers are overwritten by the next page; `dict` by the next dictionary page. Only readers whose decoded
/// values are copies may route page bytes here — `[]const u8` values are slices into the page bytes and would dangle.
///
/// Safe builds enforce the one-reader rule (see `hold`); ReleaseFast compiles the check out.
pub const DecodeScratch = struct {
    gpa: std.mem.Allocator,
    page: []u8 = &.{},
    /// Decoded dictionary values for a fixed-width T, viewed as bytes.
    dict: []align(dict_align) u8 = &.{},
    def_levels: []u32 = &.{},
    rep_levels: []u32 = &.{},
    /// Ticket of the reader whose turn it is, and the last ticket issued. Safe builds only.
    owner: Ticket = no_ticket,
    issued: Ticket = no_ticket,

    pub const dict_align = 16;

    /// A reader's claim on a scratch. Zero-sized outside safe builds.
    pub const Ticket = if (std.debug.runtime_safety) u32 else void;
    pub const no_ticket: Ticket = if (std.debug.runtime_safety) 0 else {};

    /// Called on every reader entry point. A reader's first call takes the scratch, ending the previous reader's
    /// turn; each later call fails if another reader has taken it since, because this reader's page, levels or
    /// dictionary may then have been overwritten under it. Readers carry no deinit, so a turn ends when the next
    /// reader starts rather than at a release call, and a reader abandoned mid-chunk never blocks the next one.
    pub fn hold(self: *DecodeScratch, ticket: *Ticket) error{DecodeScratchInterleaved}!void {
        if (!std.debug.runtime_safety) return;
        if (ticket.* == no_ticket) {
            self.issued +%= 1;
            if (self.issued == no_ticket) self.issued +%= 1;
            ticket.* = self.issued;
            self.owner = ticket.*;
        } else if (self.owner != ticket.*) {
            return error.DecodeScratchInterleaved;
        }
    }

    pub fn init(gpa: std.mem.Allocator) DecodeScratch {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *DecodeScratch) void {
        self.gpa.free(self.page);
        self.gpa.free(self.dict);
        self.gpa.free(self.def_levels);
        self.gpa.free(self.rep_levels);
        self.* = undefined;
    }

    /// First `n` elements of `buf`, regrown (contents discarded) when too short. 1.5x headroom so a run of slightly
    /// larger pages doesn't trade one buffer of fresh pages for another every time.
    pub fn ensure(
        self: *DecodeScratch,
        comptime T: type,
        comptime alignment: usize,
        buf: *[]align(alignment) T,
        n: usize,
    ) std.mem.Allocator.Error![]align(alignment) T {
        if (buf.len < n) {
            const fresh = try self.gpa.alignedAlloc(T, .fromByteUnits(alignment), @max(n, buf.len + buf.len / 2));
            self.gpa.free(buf.*);
            buf.* = fresh;
        }
        return buf.*[0..n];
    }
};

pub const PageReader = struct {
    /// The full column chunk bytes (compressed pages back-to-back).
    chunk: []const u8,
    pos: usize,
    codec: schema.CompressionCodec,
    arena: std.mem.Allocator,
    /// When set, every page (dictionary included) is materialised into `scratch.page` and is only valid until the
    /// next `next()`. Null keeps the arena behaviour: page bytes live as long as `arena`.
    scratch: ?*DecodeScratch = null,

    pub fn init(
        chunk: []const u8,
        codec: schema.CompressionCodec,
        arena: std.mem.Allocator,
    ) PageReader {
        return .{
            .chunk = chunk,
            .pos = 0,
            .codec = codec,
            .arena = arena,
        };
    }

    /// Read the next page. Returns null at end of chunk.
    ///
    /// V1 (DATA_PAGE / DICTIONARY_PAGE): the entire payload is
    /// compressed by the column codec. We decompress all of it and
    /// return the resulting bytes via `page.bytes`.
    ///
    /// V2 (DATA_PAGE_V2): per spec, levels are stored UNCOMPRESSED in
    /// the page body and only the values portion is optionally
    /// compressed (per the V2 header's `is_compressed` flag). Layout:
    ///
    ///     [rep_levels (rep_byte_len, uncompressed)]
    ///     [def_levels (def_byte_len, uncompressed)]
    ///     [values (compressed_page_size - rep_byte_len - def_byte_len bytes,
    ///       compressed iff is_compressed)]
    ///
    /// We materialise a single `bytes` slice that's `uncompressed_
    /// page_size` long, with levels copied verbatim and values
    /// decompressed in place. Downstream V2 decoders slice the result
    /// by the same level lengths and never need to know whether the
    /// source was compressed.
    pub fn next(self: *PageReader) Error!?Page {
        if (self.pos >= self.chunk.len) return null;

        // Parse the thrift-encoded header. Reader.pos tells us how
        // many bytes we consumed.
        var rdr = thrift.Reader.init(self.chunk[self.pos..]);
        const header = schema.PageHeader.read(&rdr) catch return error.BadPageHeader;
        const header_size = rdr.pos;

        if (header.compressed_page_size < 0 or header.uncompressed_page_size < 0) {
            return error.NegativeSize;
        }
        try checkCounts(header);
        const csize: usize = @intCast(header.compressed_page_size);
        const usize_: usize = @intCast(header.uncompressed_page_size);

        if (self.pos + header_size + csize > self.chunk.len) {
            return error.UnexpectedEndOfChunk;
        }

        const payload_start = self.pos + header_size;
        const payload = self.chunk[payload_start..][0..csize];
        self.pos = payload_start + csize;

        if (header.type == .DATA_PAGE_V2) {
            const v2 = header.data_page_header_v2 orelse return error.BadPageHeader;
            if (v2.repetition_levels_byte_length < 0 or v2.definition_levels_byte_length < 0) {
                return error.NegativeSize;
            }
            const rep_len: usize = @intCast(v2.repetition_levels_byte_length);
            const def_len: usize = @intCast(v2.definition_levels_byte_length);
            if (rep_len + def_len > csize) return error.UnexpectedEndOfChunk;

            const compressed_value_len = csize - rep_len - def_len;
            const value_uncompressed_len = if (usize_ >= rep_len + def_len)
                usize_ - rep_len - def_len
            else
                return error.UnexpectedEndOfChunk;

            const out = try self.pageBuffer(usize_);
            @memcpy(out[0..rep_len], payload[0..rep_len]);
            @memcpy(out[rep_len..][0..def_len], payload[rep_len..][0..def_len]);

            const value_src = payload[rep_len + def_len ..][0..compressed_value_len];
            const values_dst = out[rep_len + def_len ..][0..value_uncompressed_len];
            if (v2.is_compressed and self.codec != .UNCOMPRESSED) {
                try compression.decompressInto(self.arena, value_src, self.codec, values_dst);
            } else {
                if (compressed_value_len != value_uncompressed_len) return error.UnexpectedEndOfChunk;
                @memcpy(values_dst, value_src);
            }

            return .{ .header = header, .bytes = out };
        }

        if (self.scratch == null or self.codec == .UNCOMPRESSED or usize_ == 0) {
            const decompressed = try compression.decompress(self.arena, payload, self.codec, usize_);
            return .{ .header = header, .bytes = decompressed };
        }
        const out = try self.pageBuffer(usize_);
        try compression.decompressInto(self.arena, payload, self.codec, out);
        return .{ .header = header, .bytes = out };
    }

    /// Every decoder downstream casts these counts to usize (and V2 subtracts nulls from values), so a header
    /// that is negative or inconsistent here is rejected before any of them sees it.
    fn checkCounts(header: schema.PageHeader) Error!void {
        switch (header.type) {
            .DATA_PAGE => if (header.data_page_header) |h| {
                if (h.num_values < 0) return error.BadPageCount;
            },
            .DICTIONARY_PAGE => if (header.dictionary_page_header) |h| {
                if (h.num_values < 0) return error.BadPageCount;
            },
            .DATA_PAGE_V2 => if (header.data_page_header_v2) |h| {
                if (h.num_values < 0 or h.num_rows < 0 or h.num_nulls < 0) return error.BadPageCount;
                if (h.num_nulls > h.num_values) return error.BadPageCount;
            },
            .INDEX_PAGE => {},
        }
    }

    fn pageBuffer(self: *PageReader, len: usize) Error![]u8 {
        if (self.scratch) |s| return s.ensure(u8, 1, &s.page, len);
        return self.arena.alloc(u8, len);
    }

    /// Reposition the reader's cursor to an absolute file offset.
    pub fn seekToPage(self: *PageReader, absolute_offset: i64, chunk_file_offset: i64) !void {
        if (absolute_offset < chunk_file_offset) return error.UnexpectedEndOfChunk;
        // Both come from the file (page index / column metadata); the difference of two hostile i64s can overflow.
        const off: usize = @intCast(std.math.sub(i64, absolute_offset, chunk_file_offset) catch return error.UnexpectedEndOfChunk);
        if (off > self.chunk.len) return error.UnexpectedEndOfChunk;
        self.pos = off;
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("metadata.zig");
const readFileSlice = metadata.readFileSlice;

test "iterate pages of one column from the bench fixture" {
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

    // Pick the int8 column (col[0]) — small + SNAPPY.
    const rg0 = &meta.row_groups.items[0];
    const col0 = rg0.columns.items[0].meta_data.?;

    // Locate the column chunk bytes. data_page_offset is the start of
    // page data; if there's a dictionary page it lives at
    // dictionary_page_offset (which precedes data_page_offset).
    const chunk_start: usize = if (col0.dictionary_page_offset) |dp|
        @intCast(dp)
    else
        @intCast(col0.data_page_offset);
    const chunk_len: usize = @intCast(col0.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var pr = PageReader.init(chunk, col0.codec, arena.allocator());

    var page_count: usize = 0;
    var total_decompressed: usize = 0;
    while (try pr.next()) |page| {
        page_count += 1;
        total_decompressed += page.bytes.len;
        // Sanity: decompressed size must match the header.
        try testing.expectEqual(@as(usize, @intCast(page.header.uncompressed_page_size)), page.bytes.len);
    }
    try testing.expect(page_count >= 1);
    try testing.expect(total_decompressed >= page_count); // at least 1 byte each

    std.debug.print(
        "[page] col[0] ({s}): {d} pages, {d} bytes decompressed total\n",
        .{ @tagName(col0.type), page_count, total_decompressed },
    );
}

test "page headers with negative or inconsistent counts are rejected before any decoder casts them" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const Case = struct { header: schema.PageHeader };
    const v1 = schema.DataPageHeader{ .num_values = -3, .encoding = .PLAIN, .definition_level_encoding = .RLE, .repetition_level_encoding = .RLE };
    const v2 = schema.DataPageHeaderV2{
        .num_values = 2,
        .num_nulls = 5, // more nulls than values: V2 PLAIN BOOLEAN decodes num_values - num_nulls
        .num_rows = 2,
        .encoding = .PLAIN,
        .definition_levels_byte_length = 0,
        .repetition_levels_byte_length = 0,
        .is_compressed = false,
    };
    const cases = [_]Case{
        .{ .header = .{ .type = .DATA_PAGE, .uncompressed_page_size = 0, .compressed_page_size = 0, .crc = null, .data_page_header = v1, .dictionary_page_header = null, .data_page_header_v2 = null } },
        .{ .header = .{ .type = .DATA_PAGE_V2, .uncompressed_page_size = 0, .compressed_page_size = 0, .crc = null, .data_page_header = null, .dictionary_page_header = null, .data_page_header_v2 = v2 } },
        .{ .header = .{ .type = .DICTIONARY_PAGE, .uncompressed_page_size = 0, .compressed_page_size = 0, .crc = null, .data_page_header = null, .dictionary_page_header = .{ .num_values = -1, .encoding = .PLAIN, .is_sorted = null }, .data_page_header_v2 = null } },
    };
    for (cases) |case| {
        var w = thrift.Writer.init(arena);
        try case.header.write(&w);
        var pr = PageReader.init(w.bytes(), .UNCOMPRESSED, arena);
        try testing.expectError(error.BadPageCount, pr.next());
    }

    // Two hostile i64 offsets whose difference overflows.
    var pr = PageReader.init("", .UNCOMPRESSED, arena);
    try testing.expectError(error.UnexpectedEndOfChunk, pr.seekToPage(std.math.maxInt(i64), -10));
}

// ----- File-read helper duplicated from metadata.zig tests -----
// (Kept local; both test sets read the same fixture but the helper is
// trivially small.)
