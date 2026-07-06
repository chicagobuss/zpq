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
} || compression.Error;

pub const Page = struct {
    /// The parsed page header. Use `header.type`, `header.data_page_header`
    /// (when type=DATA_PAGE), or `header.dictionary_page_header` (when
    /// type=DICTIONARY_PAGE) to drive the next decoding step.
    header: schema.PageHeader,
    /// Decompressed page payload bytes. Lifetime = `arena`.
    bytes: []const u8,
};

pub const PageReader = struct {
    /// The full column chunk bytes (compressed pages back-to-back).
    chunk: []const u8,
    pos: usize,
    codec: schema.CompressionCodec,
    arena: std.mem.Allocator,

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

            const out = try self.arena.alloc(u8, usize_);
            @memcpy(out[0..rep_len], payload[0..rep_len]);
            @memcpy(out[rep_len..][0..def_len], payload[rep_len..][0..def_len]);

            const value_src = payload[rep_len + def_len ..][0..compressed_value_len];
            const values_dst = out[rep_len + def_len ..][0..value_uncompressed_len];
            if (v2.is_compressed and self.codec != .UNCOMPRESSED) {
                const decompressed = try compression.decompress(
                    self.arena,
                    value_src,
                    self.codec,
                    value_uncompressed_len,
                );
                if (decompressed.len != value_uncompressed_len) return error.UnexpectedEndOfChunk;
                @memcpy(values_dst, decompressed);
            } else {
                if (compressed_value_len != value_uncompressed_len) return error.UnexpectedEndOfChunk;
                @memcpy(values_dst, value_src);
            }

            return .{ .header = header, .bytes = out };
        }

        const decompressed = try compression.decompress(self.arena, payload, self.codec, usize_);
        return .{ .header = header, .bytes = decompressed };
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("metadata.zig");

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

// ----- File-read helper duplicated from metadata.zig tests -----
// (Kept local; both test sets read the same fixture but the helper is
// trivially small.)

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
