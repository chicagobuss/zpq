//! Codec dispatch for Parquet pages.
//!
//! UNCOMPRESSED, SNAPPY and the LZ4s are handled in-tree; ZSTD uses libzstd
//! and GZIP the Zig stdlib. The deprecated LZ4 codec (5) is read, never written:
//! the spec replaced it with LZ4_RAW, which is what `--codec lz4` writes.

const std = @import("std");
const schema = @import("../schema.zig");
const snappy = @import("snappy.zig");
const lz4 = @import("lz4.zig");

const c_zstd = @import("zstd_c");

pub const Error = error{
    UnsupportedCodec,
    DecompressionFailed,
    CompressionFailed,
    SizeMismatch,
} || std.mem.Allocator.Error;

/// Default zstd compression level. 3 is the upstream default — fast
/// encode (~400 MB/s on modern x86_64) with compression ratio close
/// to gzip-6. Higher levels (up to 22) compress better but are
/// slower; for our streaming-output use case the network is rarely
/// the bottleneck so we prefer the fast knob.
pub const ZSTD_DEFAULT_LEVEL: c_int = 3;

/// How hard zstd works on a write, as a user asks for it (`--compress-priority`, the Lambda's "compress_priority"):
/// each priority is a price for an output byte, in CPU ticks, that `ZstdPlan` weighs against each page mode's cost.
pub const CompressPriority = enum {
    /// Smallest output: every saved byte is worth any CPU (S3 and Lambda destinations, bandwidth-bound).
    size,
    /// A byte is worth 1000 ticks (local destinations).
    balanced,
    /// A byte is worth 100 ticks: CPU first, a few percent larger.
    speed,

    pub fn lambda(p: CompressPriority) f64 {
        return switch (p) {
            .size => 1e9,
            .balanced => 1000,
            .speed => 100,
        };
    }
};

/// What zstd page modes a write used, counted across its threads.
pub const ZstdModeCounts = struct {
    /// Pages per `ZstdMode`.
    pages: [4]std.atomic.Value(u64) = @splat(.init(0)),
};

/// How a write's pages are compressed with zstd: the price of an output byte (see `CompressPriority`) and, when the
/// caller wants them, counters of the modes chosen.
pub const ZstdPlan = struct {
    lambda: f64 = CompressPriority.balanced.lambda(),
    counts: ?*ZstdModeCounts = null,
};

/// A zstd page's form. Every one is a standard zstd frame any decoder reads.
pub const ZstdMode = enum(u2) {
    /// Raw blocks: a copy, for pages nothing compresses (high-entropy bit-packed data).
    raw,
    /// Entropy-coded literals with no match search.
    literals,
    level1,
    level3,
};

/// Compress a buffer using `codec`. Returns a newly-allocated slice
/// owned by `arena`. UNCOMPRESSED is a no-op identity copy onto the
/// arena (so the caller can free with the same allocator regardless
/// of codec). zstd pages get the balanced plan; see `compressPage`.
/// `compress`, with zstd pages compressed per `plan`.
pub fn compressPage(
    arena: std.mem.Allocator,
    src: []const u8,
    codec: schema.CompressionCodec,
    plan: ZstdPlan,
) Error![]u8 {
    return if (codec == .ZSTD) compressZstd(arena, src, plan) else compress(arena, src, codec);
}

pub fn compress(
    arena: std.mem.Allocator,
    src: []const u8,
    codec: schema.CompressionCodec,
) Error![]u8 {
    return switch (codec) {
        .UNCOMPRESSED => try arena.dupe(u8, src),
        .SNAPPY => snappy.compressAlloc(arena, src) catch return error.CompressionFailed,
        .ZSTD => compressZstd(arena, src, .{}),
        .GZIP => compressFlate(arena, src, .gzip),
        .LZ4_RAW => compressLz4Raw(arena, src),
        else => error.UnsupportedCodec,
    };
}

fn compressLz4Raw(arena: std.mem.Allocator, src: []const u8) Error![]u8 {
    const table = try arena.alloc(u32, lz4.HASH_SIZE);
    defer arena.free(table);
    const dest = try arena.alloc(u8, lz4.compressBound(src.len));
    errdefer arena.free(dest);
    const n = lz4.compress(src, dest, table) catch return error.CompressionFailed;
    return dest[0..n];
}

/// Compress `src` to a single gzip member via stdlib deflate. Mirrors the
/// decode path's `decompressFlate`; output lands in an arena-backed growing
/// writer (deflate can briefly exceed the input on incompressible data, so a
/// fixed worst-case buffer is fiddly — the allocating writer just grows).
fn compressFlate(
    arena: std.mem.Allocator,
    src: []const u8,
    container: std.compress.flate.Container,
) Error![]u8 {
    // Allocating output: Compress.init asserts the output buffer is > 8, so
    // seed a small capacity that the writer grows as needed.
    var out = std.Io.Writer.Allocating.initCapacity(arena, @max(64, src.len / 2)) catch
        return error.CompressionFailed;
    // The compressor's history window (asserted >= max_window_len = 64 KB).
    const window = try arena.alloc(u8, std.compress.flate.max_window_len);
    defer arena.free(window);

    var c = std.compress.flate.Compress.init(&out.writer, window, container, .default) catch
        return error.CompressionFailed;
    c.writer.writeAll(src) catch return error.CompressionFailed;
    c.finish() catch return error.CompressionFailed;
    return out.writer.buffered();
}

fn compressZstd(arena: std.mem.Allocator, src: []const u8, plan: ZstdPlan) Error![]u8 {
    const max_len = c_zstd.ZSTD_compressBound(src.len);
    if (c_zstd.ZSTD_isError(max_len) != 0) return error.CompressionFailed;
    const buf = try arena.alloc(u8, @max(max_len, rawFrameLen(src.len)));
    errdefer arena.free(buf);
    const cctx = c_zstd.ZSTD_createCCtx() orelse return error.OutOfMemory;
    defer _ = c_zstd.ZSTD_freeCCtx(cctx);
    const mode = try chooseZstdMode(arena, cctx, src, plan.lambda);
    if (plan.counts) |c| _ = c.pages[@intFromEnum(mode)].fetchAdd(1, .monotonic);
    const n = try zstdWith(cctx, mode, buf, src);
    return buf[0..n];
}

/// Pages under this are compressed at level 3: a trial would cost about what it steers.
const trial_min_page = 16 << 10;
const trial_sample = 16 << 10;

/// CPU ticks per input byte each mode costs (Zen 5; level 3 ranges 3-22 over Parquet page kinds, 12 its byte-weighted
/// mean). Constants, never timings, so a page's mode depends on its bytes alone.
const mode_price = [_]f64{ 0.1, 2.6, 3.5, 12.0 };

/// The mode with the lowest estimated cost for `src`: its CPU (`mode_price` per input byte) plus `lambda` per output
/// byte, each mode's output size estimated by compressing a deterministic sample of the page (four slices at fixed
/// fractions of it). Bit-packed integer and dictionary-index pages are often incompressible, where level 3 searches
/// for matches it never finds; structured strings often come out smaller at level 1; text keeps level 3. The choice
/// depends on the page's bytes and `lambda` only, so output is byte-identical across threads and runs.
fn chooseZstdMode(arena: std.mem.Allocator, cctx: *c_zstd.ZSTD_CCtx, src: []const u8, lambda: f64) Error!ZstdMode {
    if (src.len < trial_min_page) return .level3;
    const slice = trial_sample / 4;
    var sample: [trial_sample]u8 = undefined;
    for (0..4) |k| {
        const off = (src.len - slice) * k / 3;
        @memcpy(sample[k * slice ..][0..slice], src[off..][0..slice]);
    }
    const scratch = try arena.alloc(u8, c_zstd.ZSTD_compressBound(sample.len));
    defer arena.free(scratch);
    const n: f64 = @floatFromInt(src.len);
    var best: ZstdMode = .level3;
    var best_cost = std.math.inf(f64);
    for (std.meta.tags(ZstdMode)) |m| {
        const out: f64 = @floatFromInt(try zstdWith(cctx, m, scratch, &sample));
        const cost = mode_price[@intFromEnum(m)] * n + lambda * n * out / @as(f64, @floatFromInt(sample.len));
        if (cost < best_cost) {
            best_cost = cost;
            best = m;
        }
    }
    return best;
}

/// Compress `src` into `dst` as one standard zstd frame in `mode`; returns the frame's length.
fn zstdWith(cctx: *c_zstd.ZSTD_CCtx, mode: ZstdMode, dst: []u8, src: []const u8) Error!usize {
    if (mode == .raw) return writeRawFrame(dst, src);
    _ = c_zstd.ZSTD_CCtx_reset(cctx, c_zstd.ZSTD_reset_session_and_parameters);
    const level: c_int = if (mode == .level1) 1 else ZSTD_DEFAULT_LEVEL;
    if (c_zstd.ZSTD_isError(c_zstd.ZSTD_CCtx_setParameter(cctx, c_zstd.ZSTD_c_compressionLevel, level)) != 0)
        return error.CompressionFailed;
    const n = if (mode == .literals) blk: {
        // No sequences: every byte is a literal, entropy-coded (or stored, when that is smaller), no match search.
        const no_delims = c_zstd.ZSTD_sf_noBlockDelimiters;
        const delims = c_zstd.ZSTD_CCtx_setParameter(cctx, c_zstd.ZSTD_c_blockDelimiters, no_delims);
        if (c_zstd.ZSTD_isError(delims) != 0) return error.CompressionFailed;
        break :blk c_zstd.ZSTD_compressSequences(cctx, dst.ptr, dst.len, null, 0, src.ptr, src.len);
    } else c_zstd.ZSTD_compress2(cctx, dst.ptr, dst.len, src.ptr, src.len);
    if (c_zstd.ZSTD_isError(n) != 0) return error.CompressionFailed;
    return n;
}

const raw_block_max = 128 << 10;

fn rawFrameLen(n: usize) usize {
    return 4 + 9 + 3 * @max(1, (n + raw_block_max - 1) / raw_block_max) + n;
}

/// A standard zstd frame of raw blocks: magic, a single-segment frame header carrying the content size, then the
/// bytes in blocks of at most 128 KiB, each behind a 3-byte header. What zstd emits for incompressible input, without
/// first searching it.
fn writeRawFrame(dst: []u8, src: []const u8) usize {
    std.mem.writeInt(u32, dst[0..4], 0xFD2FB528, .little);
    var p: usize = 4;
    const n = src.len;
    // Frame header descriptor: single segment (bit 5), content size field width in bits 6-7.
    if (n < 256) {
        dst[p] = 0x20;
        dst[p + 1] = @intCast(n);
        p += 2;
    } else if (n < 65536 + 256) {
        dst[p] = 0x60;
        std.mem.writeInt(u16, dst[p + 1 ..][0..2], @intCast(n - 256), .little);
        p += 3;
    } else if (n <= std.math.maxInt(u32)) {
        dst[p] = 0xA0;
        std.mem.writeInt(u32, dst[p + 1 ..][0..4], @intCast(n), .little);
        p += 5;
    } else {
        dst[p] = 0xE0;
        std.mem.writeInt(u64, dst[p + 1 ..][0..8], n, .little);
        p += 9;
    }
    var off: usize = 0;
    while (true) {
        const b = @min(raw_block_max, n - off);
        const last: u32 = @intFromBool(off + b == n);
        // Block header: last-block bit, type 0 (raw) in bits 1-2, size in bits 3-23.
        const h: u32 = last | (@as(u32, @intCast(b)) << 3);
        dst[p] = @truncate(h);
        dst[p + 1] = @truncate(h >> 8);
        dst[p + 2] = @truncate(h >> 16);
        p += 3;
        @memcpy(dst[p..][0..b], src[off..][0..b]);
        p += b;
        off += b;
        if (off == n) return p;
    }
}

/// Decompress a single Parquet page's payload.
///
/// `src` is the on-disk compressed bytes (its length matches
/// `compressed_page_size` in the page header).
/// `uncompressed_size` is what the page header says the output should be.
/// `arena` is the per-row-group allocator the decompressed bytes live in
/// for codecs that produce new bytes; UNCOMPRESSED returns `src` directly
/// without allocating.
///
/// Caller's lifetime contract: returned bytes are valid for at least
/// the lifetime of `arena` AND `src` (whichever is shorter). In practice
/// both have the row-group lifetime, so it doesn't matter at the call site.
pub fn decompress(
    arena: std.mem.Allocator,
    src: []const u8,
    codec: schema.CompressionCodec,
    uncompressed_size: usize,
) Error![]const u8 {
    if (uncompressed_size == 0) return src[0..0];
    if (codec == .UNCOMPRESSED) {
        // Sanity check: header should agree with the slice length.
        if (src.len != uncompressed_size) return error.SizeMismatch;
        return src;
    }
    const out = try arena.alloc(u8, uncompressed_size);
    errdefer arena.free(out);
    try decompressInto(arena, src, codec, out);
    return out;
}

/// `decompress` into a caller-owned buffer of exactly the uncompressed size, so callers can reuse one buffer
/// across pages. UNCOMPRESSED copies. `arena` is only touched for codec working state (the flate window).
pub fn decompressInto(
    arena: std.mem.Allocator,
    src: []const u8,
    codec: schema.CompressionCodec,
    out: []u8,
) Error!void {
    // An empty page (0 values) decompresses to nothing regardless of codec.
    // Several codecs' decoders choke on a zero-length input (snappy reads a
    // varint length header first); short-circuit before dispatch. See
    // datapage_v2_empty_datapage.snappy.parquet.
    if (out.len == 0) return;

    switch (codec) {
        .UNCOMPRESSED => {
            if (src.len != out.len) return error.SizeMismatch;
            @memcpy(out, src);
        },
        .SNAPPY => {
            const n = snappy.uncompress(src, out) catch return error.DecompressionFailed;
            if (n != out.len) return error.SizeMismatch;
        },
        .GZIP => try decompressFlate(arena, src, out, .gzip),
        .ZSTD => try decompressZstd(src, out),
        .LZ4_RAW => {
            const n = lz4.uncompress(src, out) catch return error.DecompressionFailed;
            if (n != out.len) return error.SizeMismatch;
        },
        .LZ4 => try decompressLz4Legacy(src, out),
        else => return error.UnsupportedCodec,
    }
}

/// Codec 5 carries no marker for which of its historical containers a page uses, so try them in Arrow C++'s
/// order (ARROW-9177): Hadoop framing (parquet-mr) only if every length validates, else a bare block (older
/// parquet-cpp); then, as arrow-rs also does, an LZ4 frame (older arrow-rs) when its magic announces one. A bare
/// block cannot parse a standard frame (its first token matches at offset 0x4D22 into empty output), so trying
/// the block first costs nothing. Every attempt writes `out` from the start, so the one that succeeds overwrites a failed one's bytes.
fn decompressLz4Legacy(src: []const u8, out: []u8) Error!void {
    if (lz4.uncompressHadoop(src, out)) |_| return else |_| {}
    if (lz4.uncompress(src, out)) |n| {
        if (n == out.len) return;
    } else |_| {}
    if (lz4.hasFrameMagic(src)) {
        const n = lz4.uncompressFrame(src, out) catch return error.DecompressionFailed;
        if (n != out.len) return error.SizeMismatch;
        return;
    }
    return error.DecompressionFailed;
}

fn decompressFlate(
    arena: std.mem.Allocator,
    src: []const u8,
    out: []u8,
    container: std.compress.flate.Container,
) Error!void {
    const uncompressed_size = out.len;
    var input = std.Io.Reader.fixed(src);
    // The flate decoder asserts buffer.len >= max_window_len (64 KB).
    // Arena-allocated; lives until the row-group arena resets.
    const window = try arena.alloc(u8, std.compress.flate.max_window_len);
    defer arena.free(window);

    // gzip/zlib permit concatenated members: several independently-framed
    // streams back-to-back that together produce `uncompressed_size` bytes
    // (see concatenated_gzip_members.parquet). std's Decompress stops at the
    // first member's footer (state `.end` → EndOfStream), so we loop, re-
    // initialising a fresh decoder per member. This is safe because the
    // decoder advances `input.seek` precisely (it peeks then tosses exactly
    // the bytes it consumes), leaving `seek` at the next member's first byte.
    // Single-member streams take one iteration and exit.
    var written: usize = 0;
    while (written < uncompressed_size and input.seek < input.end) {
        var d = std.compress.flate.Decompress.init(&input, container, window);
        const n = d.reader.readSliceShort(out[written..]) catch return error.DecompressionFailed;
        if (n == 0) break;
        written += n;
    }
    if (written != uncompressed_size) return error.DecompressionFailed;
}

fn decompressZstd(src: []const u8, out: []u8) Error!void {
    // Use libzstd's `ZSTD_decompress` directly. Zig's stdlib zstd
    // decoder is correct but ~10× slower than libzstd on the
    // dict-encoded numeric column-chunks Parquet writers produce —
    // 92% of CLI aggregate time was in `std.compress.zstd` per perf
    // profile (2026-05-06). libzstd is already linked for the write
    // path; same library, different entry point.
    const n = c_zstd.ZSTD_decompress(
        @ptrCast(out.ptr),
        out.len,
        @ptrCast(src.ptr),
        src.len,
    );
    if (c_zstd.ZSTD_isError(n) != 0) return error.DecompressionFailed;
    if (n != out.len) return error.SizeMismatch;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "UNCOMPRESSED passthrough" {
    const src = "hello world";
    const out = try decompress(testing.allocator, src, .UNCOMPRESSED, src.len);
    try testing.expectEqualStrings(src, out);
}

test "UNCOMPRESSED size mismatch" {
    const src = "hello";
    try testing.expectError(error.SizeMismatch, decompress(testing.allocator, src, .UNCOMPRESSED, 99));
}

test "SNAPPY round-trip" {
    const input = "Hello, World! Hello, World! Hello, World!";
    var compressed: [128]u8 = undefined;
    const comp_len = try snappy.compress(input, &compressed);

    const out = try decompress(testing.allocator, compressed[0..comp_len], .SNAPPY, input.len);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings(input, out);
}

test "unsupported codec" {
    try testing.expectError(error.UnsupportedCodec, decompress(testing.allocator, "x", .BROTLI, 1));
    try testing.expectError(error.UnsupportedCodec, compress(testing.allocator, "x", .LZ4)); // read-only codec
}

test "GZIP decompresses canned bytes" {
    // gzip of "hello" — generated by `python3 -c "import gzip,sys;
    // sys.stdout.buffer.write(gzip.compress(b'hello'))"`.
    const compressed = [_]u8{
        0x1f, 0x8b, 0x08, 0x00, 0xb6, 0xdf, 0xf7, 0x69, 0x02, 0xff,
        0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x86, 0xa6, 0x10,
        0x36, 0x05, 0x00, 0x00, 0x00,
    };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const out = try decompress(arena.allocator(), &compressed, .GZIP, 5);
    try testing.expectEqualStrings("hello", out);
}

test "ZSTD decompresses canned bytes" {
    // zstd of "hello world" — generated by `printf 'hello world' | zstd`.
    const compressed = [_]u8{
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x59, 0x00, 0x00,
        0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72,
        0x6c, 0x64, 0x68, 0x69, 0x1e, 0xb2,
    };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const out = try decompress(arena.allocator(), &compressed, .ZSTD, 11);
    try testing.expectEqualStrings("hello world", out);
}

test "LZ4 (codec 5): Hadoop framing, bare block and frame all decode" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const text = "hello hello hello hello parquet lz4 frame";

    const block = try compress(a, text, .LZ4_RAW);
    const hadoop = try a.alloc(u8, 8 + block.len);
    std.mem.writeInt(u32, hadoop[0..4], text.len, .big);
    std.mem.writeInt(u32, hadoop[4..8], @intCast(block.len), .big);
    @memcpy(hadoop[8..], block);
    const frame = lz4.frame_plain; // python-lz4 `lz4.frame.compress(text, store_size=False)`
    for ([_][]const u8{ hadoop, block, &frame }) |src| {
        try testing.expectEqualStrings(text, try decompress(a, src, .LZ4, text.len));
    }

    // A page header whose size disagrees with the payload is an error, whichever container it was.
    for ([_][]const u8{ hadoop, block }) |src| {
        try testing.expectError(error.DecompressionFailed, decompress(a, src, .LZ4, text.len + 1));
    }
    try testing.expectError(error.SizeMismatch, decompress(a, &frame, .LZ4, text.len + 1));
    try testing.expectError(error.DecompressionFailed, decompress(a, "\x00\x00\x00\x00garbage!", .LZ4, 4));
}

test "LZ4 (codec 5): a Hadoop chunk split over several blocks decodes" {
    // BlockCompressorStream given one write() larger than its 256 KiB buffer writes the total length once, then
    // a `[clen][block]` piece per buffer-sized slice (MAX_INPUT_SIZE = 262144 - 262144/255 - 16).
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = try a.alloc(u8, 600_000);
    var prng = std.Random.DefaultPrng.init(0x1a4);
    for (raw, 0..) |*b, i| b.* = if (i % 7 == 0) prng.random().int(u8) else @truncate(i / 64);

    var page: std.ArrayList(u8) = .empty;
    var word: [4]u8 = undefined;
    std.mem.writeInt(u32, &word, @intCast(raw.len), .big);
    try page.appendSlice(a, &word);
    const max_input = 262144 - 262144 / 255 - 16;
    var off: usize = 0;
    var pieces: usize = 0;
    while (off < raw.len) : (pieces += 1) {
        const n = @min(max_input, raw.len - off);
        const block = try compress(a, raw[off..][0..n], .LZ4_RAW);
        std.mem.writeInt(u32, &word, @intCast(block.len), .big);
        try page.appendSlice(a, &word);
        try page.appendSlice(a, block);
        off += n;
    }
    try testing.expectEqual(@as(usize, 3), pieces);
    try testing.expectEqualSlices(u8, raw, try decompress(a, page.items, .LZ4, raw.len));
}

test "raw-block frames decode to their input at every size boundary" {
    for ([_]usize{ 0, 1, 255, 256, 65791, 65792, 131072, 131073, 300000 }) |n| {
        const src = try testing.allocator.alloc(u8, n);
        defer testing.allocator.free(src);
        for (src, 0..) |*b, i| b.* = @truncate(i *% 2654435761);
        const frame = try testing.allocator.alloc(u8, rawFrameLen(n));
        defer testing.allocator.free(frame);
        const len = writeRawFrame(frame, src);
        try testing.expect(len <= frame.len);
        const back = try testing.allocator.alloc(u8, n);
        defer testing.allocator.free(back);
        const got = c_zstd.ZSTD_decompress(back.ptr, back.len, frame.ptr, len);
        try testing.expect(c_zstd.ZSTD_isError(got) == 0);
        try testing.expectEqual(n, got);
        try testing.expectEqualSlices(u8, src, back);
    }
}

test "every zstd page mode makes a frame libzstd decodes, and each priority picks deterministically" {
    var prng = std.Random.DefaultPrng.init(0x2577d);
    const r = prng.random();
    const n = 100_000;
    const src = try testing.allocator.alloc(u8, n);
    defer testing.allocator.free(src);
    for (src, 0..) |*b, i| b.* = if (i % 3 == 0) r.int(u8) else @truncate(i / 7);
    const cctx = c_zstd.ZSTD_createCCtx() orelse return error.OutOfMemory;
    defer _ = c_zstd.ZSTD_freeCCtx(cctx);
    const dst = try testing.allocator.alloc(u8, @max(c_zstd.ZSTD_compressBound(n), rawFrameLen(n)));
    defer testing.allocator.free(dst);
    const back = try testing.allocator.alloc(u8, n);
    defer testing.allocator.free(back);
    for (std.meta.tags(ZstdMode)) |m| {
        const len = try zstdWith(cctx, m, dst, src);
        const got = c_zstd.ZSTD_decompress(back.ptr, back.len, dst.ptr, len);
        try testing.expect(c_zstd.ZSTD_isError(got) == 0);
        try testing.expectEqualSlices(u8, src, back[0..got]);
    }
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    for (std.meta.tags(CompressPriority)) |p| {
        var counts: ZstdModeCounts = .{};
        const a = try compressPage(arena_state.allocator(), src, .ZSTD, .{ .lambda = p.lambda(), .counts = &counts });
        const b = try compressPage(arena_state.allocator(), src, .ZSTD, .{ .lambda = p.lambda() });
        try testing.expectEqualSlices(u8, a, b);
        const got = c_zstd.ZSTD_decompress(back.ptr, back.len, a.ptr, a.len);
        try testing.expectEqualSlices(u8, src, back[0..got]);
        var pages: u64 = 0;
        for (&counts.pages) |*c| pages += c.load(.monotonic);
        try testing.expectEqual(@as(u64, 1), pages);
    }
}
