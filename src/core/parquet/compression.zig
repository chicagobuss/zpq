//! Codec dispatch for Parquet pages.
//!
//! UNCOMPRESSED and SNAPPY are handled in-tree; LZ4_RAW is written with liblz4
//! and the LZ4s read in-tree; ZSTD uses libzstd;
//! GZIP is read with libdeflate (`ZPQ_GZIP_STD` selects the Zig stdlib instead)
//! and written with the Zig stdlib. The deprecated LZ4 codec (5) is read, never written:
//! the spec replaced it with LZ4_RAW, which is what `--codec lz4` writes.

const std = @import("std");
const schema = @import("../schema.zig");
const snappy = @import("snappy.zig");
const lz4 = @import("lz4.zig");

const c_zstd = @import("zstd_c");
const c_deflate = @import("libdeflate_c");
const c_lz4 = @import("lz4_c");

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

/// How hard zstd works on a write, as a user asks for it (`--compress-priority`, the Lambda's "compress_priority").
pub const CompressPriority = enum {
    /// Level 3 for every page: the default, and the same bytes as before priorities existed.
    size,
    /// Opt-in: each page in the form that costs least when an output byte is worth 100 CPU ticks, chosen from a
    /// sample (`chooseZstdMode`) and kept only if the page passes `passesPageChecks`, else level 3. The checks see
    /// less than level 3's own match search does, so there is no bound relative to level 3: a page whose matches
    /// they miss is written as raw blocks, its size plus about 0.01%. For example, an 8 MiB page repeating 600,000
    /// random bytes (repeats beyond the checks' 512 KiB window) is written in 8,388,809 bytes where level 3 needs
    /// 610,006, 13.75 times as many.
    speed,

    /// What an output byte is worth, in CPU ticks, to the page-mode choice; null for `size`, which keeps level 3.
    pub fn lambda(p: CompressPriority) ?f64 {
        return switch (p) {
            .size => null,
            .speed => 100,
        };
    }
};

/// What zstd page modes a write used, counted across its threads.
pub const ZstdModeCounts = struct {
    /// Pages per `ZstdMode` written.
    pages: [4]std.atomic.Value(u64) = @splat(.init(0)),
    /// Pages whose cheaper form failed `passesPageChecks` and were compressed at level 3 as well.
    rechecked: std.atomic.Value(u64) = .init(0),
};

/// How a write's pages are compressed with zstd: the price of an output byte (see `CompressPriority`; null keeps
/// level 3 for every page) and, when the caller wants them, counters of the modes chosen.
pub const ZstdPlan = struct {
    lambda: ?f64 = null,
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
/// of codec). zstd pages are compressed at level 3; see `compressPage`.
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

/// `src` as one LZ4 block (LZ4_RAW), by liblz4's default compressor: deterministic, the format's reference encoder.
fn compressLz4Raw(arena: std.mem.Allocator, src: []const u8) Error![]u8 {
    if (src.len > c_lz4.LZ4_MAX_INPUT_SIZE) return error.CompressionFailed;
    const bound: usize = @intCast(c_lz4.LZ4_compressBound(@intCast(src.len)));
    const dest = try arena.alloc(u8, bound);
    errdefer arena.free(dest);
    const n = c_lz4.LZ4_compress_default(src.ptr, dest.ptr, @intCast(src.len), @intCast(dest.len));
    if (n <= 0) return error.CompressionFailed;
    return dest[0..@intCast(n)];
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
    const choice: Choice = if (plan.lambda) |lambda| try chooseZstdMode(arena, cctx, src, lambda) else .{};
    var mode = choice.mode;
    var out = buf[0..try zstdWith(cctx, mode, buf, src)];
    if (mode != .level3 and !try passesPageChecks(arena, cctx, src, choice, out.len)) {
        // The sample misjudged the page: level 3 as well, keeping the smaller frame.
        const l3 = try arena.alloc(u8, max_len);
        const n = try zstdWith(cctx, .level3, l3, src);
        if (n <= out.len) {
            arena.free(buf);
            out = l3[0..n];
            mode = .level3;
        } else arena.free(l3);
        if (plan.counts) |c| _ = c.rechecked.fetchAdd(1, .monotonic);
    }
    if (plan.counts) |c| _ = c.pages[@intFromEnum(mode)].fetchAdd(1, .monotonic);
    return out;
}

/// Pages under this are compressed at level 3: a trial would cost about what it steers.
const trial_min_page = 16 << 10;
const trial_sample = 16 << 10;

/// CPU ticks per input byte each mode costs, its page checks included (Zen 5; level 3 ranges 3-22 over Parquet page
/// kinds, 12 its byte-weighted mean; a raw frame is a copy plus a fast match search and a byte histogram, a
/// literals-only frame entropy coding plus the match search). Constants, never timings, so a page's mode depends on
/// its bytes alone.
const mode_price = [_]f64{ 1.5, 3.2, 3.5, 12.0 };

/// A page's chosen mode and the frame size its sample predicts for it.
const Choice = struct {
    mode: ZstdMode = .level3,
    estimate: f64 = 0,
};

/// The mode with the lowest estimated cost for `src`: its CPU (`mode_price` per input byte) plus `lambda` per output
/// byte, each mode's output size estimated by compressing a deterministic sample of the page (four slices spread
/// evenly over it: the head, a third and two thirds in, the tail). Bit-packed integer and dictionary-index pages are
/// often incompressible, where level 3 searches for matches it never finds; structured strings often come out
/// smaller at level 1; text keeps level 3. The choice depends on the page's bytes and `lambda` only, so output is
/// byte-identical across threads and runs; `passesPageChecks` then holds it to the whole page.
fn chooseZstdMode(arena: std.mem.Allocator, cctx: *c_zstd.ZSTD_CCtx, src: []const u8, lambda: f64) Error!Choice {
    if (src.len < trial_min_page) return .{};
    const slice = trial_sample / 4;
    var sample: [trial_sample]u8 = undefined;
    for (0..4) |k| {
        const off = (src.len - slice) * k / 3;
        @memcpy(sample[k * slice ..][0..slice], src[off..][0..slice]);
    }
    const scratch = try arena.alloc(u8, c_zstd.ZSTD_compressBound(sample.len));
    defer arena.free(scratch);
    const n: f64 = @floatFromInt(src.len);
    var best: Choice = .{};
    var best_cost = std.math.inf(f64);
    for (std.meta.tags(ZstdMode)) |m| {
        const out: f64 = @floatFromInt(try zstdWith(cctx, m, scratch, &sample));
        const estimate = n * out / @as(f64, @floatFromInt(sample.len));
        const cost = mode_price[@intFromEnum(m)] * n + lambda * estimate;
        if (cost < best_cost) {
            best_cost = cost;
            best = .{ .mode = m, .estimate = estimate };
        }
    }
    return best;
}

/// How far a page may stray from what its checks expect: 10% plus 1 KiB (frame and table overhead on small pages).
fn withinMargin(bound: f64) f64 {
    return bound * 1.1 + 1024;
}

/// Whether a page compressed in `choice.mode` (a frame of `got` bytes) keeps it, judged on the whole page to catch
/// a sample unlike the rest of it:
/// 1. the frame is within the margin of the size the sample predicted;
/// 2. raw and literals-only frames search no matches, so a fast full-page match search (level -1, which stores
///    literals as they are) must save less than the margin;
/// 3. raw frames code no literals, so the page's order-0 entropy, a lower bound on entropy-coded literals, must be
///    within the margin of its size.
/// Deterministic: it depends on the page's bytes alone. Level -1 searches a 512 KiB window for matches of at least
/// 6 bytes, where level 3 searches 1-2 MiB for 5, so a page compressible only through matches it cannot see passes;
/// see `CompressPriority.speed`.
fn passesPageChecks(arena: std.mem.Allocator, cctx: *c_zstd.ZSTD_CCtx, src: []const u8, choice: Choice, got: usize) Error!bool {
    const n: f64 = @floatFromInt(src.len);
    if (@as(f64, @floatFromInt(got)) > withinMargin(choice.estimate)) return false;
    if (choice.mode == .level1) return true;
    const scratch = try arena.alloc(u8, c_zstd.ZSTD_compressBound(src.len));
    defer arena.free(scratch);
    const matched: f64 = @floatFromInt(try zstdLevel(cctx, -1, scratch, src));
    if (withinMargin(matched) < n) return false;
    if (choice.mode == .raw and withinMargin(entropyBytes(src)) < n) return false;
    return true;
}

/// The order-0 entropy of `src`, in bytes: no entropy coder of its bytes taken one at a time does better.
fn entropyBytes(src: []const u8) f64 {
    // Four tables, so runs of one byte value do not serialize on one counter.
    var h: [4][256]u32 = @splat(@splat(0));
    var i: usize = 0;
    while (i + 4 <= src.len) : (i += 4) {
        inline for (0..4) |k| h[k][src[i + k]] += 1;
    }
    while (i < src.len) : (i += 1) h[0][src[i]] += 1;
    const n: f64 = @floatFromInt(src.len);
    var bits: f64 = 0;
    for (0..256) |v| {
        const c: f64 = @floatFromInt(@as(u64, h[0][v]) + h[1][v] + h[2][v] + h[3][v]);
        if (c > 0) bits += c * @log2(n / c);
    }
    return bits / 8;
}

/// Compress `src` into `dst` as one standard zstd frame in `mode`; returns the frame's length.
fn zstdWith(cctx: *c_zstd.ZSTD_CCtx, mode: ZstdMode, dst: []u8, src: []const u8) Error!usize {
    switch (mode) {
        .raw => return writeRawFrame(dst, src),
        .level1 => return zstdLevel(cctx, 1, dst, src),
        .level3 => return zstdLevel(cctx, ZSTD_DEFAULT_LEVEL, dst, src),
        .literals => {
            // No sequences: every byte is a literal, entropy-coded (or stored, when that is smaller), no match search.
            try setLevel(cctx, ZSTD_DEFAULT_LEVEL);
            const no_delims = c_zstd.ZSTD_sf_noBlockDelimiters;
            const delims = c_zstd.ZSTD_CCtx_setParameter(cctx, c_zstd.ZSTD_c_blockDelimiters, no_delims);
            if (c_zstd.ZSTD_isError(delims) != 0) return error.CompressionFailed;
            const n = c_zstd.ZSTD_compressSequences(cctx, dst.ptr, dst.len, null, 0, src.ptr, src.len);
            if (c_zstd.ZSTD_isError(n) != 0) return error.CompressionFailed;
            return n;
        },
    }
}

/// `src` as one zstd frame at `level`.
fn zstdLevel(cctx: *c_zstd.ZSTD_CCtx, level: c_int, dst: []u8, src: []const u8) Error!usize {
    try setLevel(cctx, level);
    const n = c_zstd.ZSTD_compress2(cctx, dst.ptr, dst.len, src.ptr, src.len);
    if (c_zstd.ZSTD_isError(n) != 0) return error.CompressionFailed;
    return n;
}

fn setLevel(cctx: *c_zstd.ZSTD_CCtx, level: c_int) Error!void {
    _ = c_zstd.ZSTD_CCtx_reset(cctx, c_zstd.ZSTD_reset_session_and_parameters);
    if (c_zstd.ZSTD_isError(c_zstd.ZSTD_CCtx_setParameter(cctx, c_zstd.ZSTD_c_compressionLevel, level)) != 0)
        return error.CompressionFailed;
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
        .GZIP => try decompressGzip(arena, src, out),
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

/// Which inflater reads GZIP pages: 0 not decided yet, 1 libdeflate, 2 `std.compress.flate`. `ZPQ_GZIP_STD` set to
/// anything but "0" selects the stdlib, to compare the two or to fall back; decided once per process.
var gzip_impl: std.atomic.Value(u8) = .init(0);

fn gzipUsesStd() bool {
    var v = gzip_impl.load(.monotonic);
    if (v == 0) {
        const e = std.c.getenv("ZPQ_GZIP_STD");
        const std_flate = if (e) |p| p[0] != 0 and !(p[0] == '0' and p[1] == 0) else false;
        v = if (std_flate) 2 else 1;
        gzip_impl.store(v, .monotonic);
    }
    return v == 2;
}

/// For tests: read GZIP pages with the stdlib (true) or libdeflate (false), whatever the environment says.
pub fn setGzipStd(std_flate: bool) void {
    gzip_impl.store(if (std_flate) 2 else 1, .monotonic);
}

/// A GZIP page body into `out`, which is exactly its size: one gzip member or several back to back
/// (concatenated_gzip_members.parquet). Every member, empty ones included, is decoded and checked against its CRC-32
/// and length; the members must use all of `src` and fill `out` exactly, so trailing bytes that are no member, or a
/// member past the page's size, are errors. libdeflate decodes a member into the space left and never writes past
/// `out`. An empty body is accepted only for an empty page: there is nothing in it to check.
fn decompressGzip(arena: std.mem.Allocator, src: []const u8, out: []u8) Error!void {
    if (gzipUsesStd()) return decompressFlate(arena, src, out, .gzip);
    const d = c_deflate.libdeflate_alloc_decompressor() orelse return error.OutOfMemory;
    defer c_deflate.libdeflate_free_decompressor(d);
    var written: usize = 0;
    var pos: usize = 0;
    while (pos < src.len) {
        var used: usize = 0;
        var got: usize = 0;
        const r = c_deflate.libdeflate_gzip_decompress_ex(
            d,
            src.ptr + pos,
            src.len - pos,
            out.ptr + written,
            out.len - written,
            &used,
            &got,
        );
        if (r != c_deflate.LIBDEFLATE_SUCCESS) return error.DecompressionFailed;
        pos += used;
        written += got;
    }
    if (written != out.len) return error.DecompressionFailed;
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
    //
    // Every member, empty ones included, is read to its end, so its trailer is
    // parsed, and checked here: std's Decompress records a gzip member's
    // CRC-32 and length but does not compare them. The members must use all
    // of `src` and fill `out` exactly: trailing bytes that are no member, or a
    // member that does not end within the page, are errors.
    var written: usize = 0;
    while (input.seek < input.end) {
        var d = std.compress.flate.Decompress.init(&input, container, window);
        const start = written;
        written += d.reader.readSliceShort(out[written..]) catch return error.DecompressionFailed;
        var past: [1]u8 = undefined;
        if ((d.reader.readSliceShort(&past) catch return error.DecompressionFailed) != 0)
            return error.DecompressionFailed;
        if (container == .gzip) {
            const trailer = d.container_metadata.gzip;
            const member = out[start..written];
            if (trailer.crc != std.hash.Crc32.hash(member) or trailer.count != @as(u32, @truncate(member.len)))
                return error.DecompressionFailed;
        }
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

/// A 1 MiB page built so that the chooser's four sample slices see something unlike the rest of it.
fn adversarialPage(a: std.mem.Allocator, kind: usize, r: std.Random) ![]u8 {
    const n = 1 << 20;
    const src = try a.alloc(u8, n);
    const slice = trial_sample / 4;
    switch (kind) {
        // Zeros, with random bytes exactly where the sample looks.
        0 => {
            @memset(src, 0);
            for (0..4) |k| r.bytes(src[(n - slice) * k / 3 ..][0..slice]);
        },
        // A compressible prefix (a short text repeated), then random bytes.
        1 => {
            for (src[0 .. n / 2], 0..) |*b, i| b.* = "a page of repeated text, "[i % 25];
            r.bytes(src[n / 2 ..]);
        },
        // Random bytes, then a compressible tail.
        2 => {
            r.bytes(src[0 .. n / 2]);
            for (src[n / 2 ..], 0..) |*b, i| b.* = "a page of repeated text, "[i % 25];
        },
        // One random block repeated, its period longer than a sample slice: no slice sees a repeat.
        3, 4 => {
            const period: usize = if (kind == 3) slice + 1500 else 64 << 10;
            r.bytes(src[0..period]);
            var i: usize = period;
            while (i < n) : (i += 1) src[i] = src[i - period];
        },
        // Bytes from 16 values (half the entropy of random bytes), random bytes where the sample looks.
        else => {
            for (src) |*b| b.* = r.int(u4);
            for (0..4) |k| r.bytes(src[(n - slice) * k / 3 ..][0..slice]);
        },
    }
    return src;
}

test "pages built to mislead the sample: size writes level 3's bytes, speed decodes and stays a raw frame at most" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0xdead1234);
    const cctx = c_zstd.ZSTD_createCCtx() orelse return error.OutOfMemory;
    defer _ = c_zstd.ZSTD_freeCCtx(cctx);
    for (0..6) |kind| {
        const src = try adversarialPage(a, kind, prng.random());
        const l3_buf = try a.alloc(u8, c_zstd.ZSTD_compressBound(src.len));
        const l3 = l3_buf[0..try zstdWith(cctx, .level3, l3_buf, src)];
        const back = try a.alloc(u8, src.len);
        for (std.meta.tags(CompressPriority)) |p| {
            var counts: ZstdModeCounts = .{};
            const got = try compressPage(a, src, .ZSTD, .{ .lambda = p.lambda(), .counts = &counts });
            const again = try compressPage(a, src, .ZSTD, .{ .lambda = p.lambda() });
            try testing.expectEqualSlices(u8, got, again);
            const n = c_zstd.ZSTD_decompress(back.ptr, back.len, got.ptr, got.len);
            try testing.expect(c_zstd.ZSTD_isError(n) == 0);
            try testing.expectEqualSlices(u8, src, back[0..n]);
            if (p == .size) {
                try testing.expectEqualSlices(u8, l3, got);
            } else {
                try testing.expect(got.len <= rawFrameLen(src.len));
                // The checks catch these: random bytes only where the sample looks, and low-entropy bytes behind it.
                if (kind == 0 or kind == 5) try testing.expectEqualSlices(u8, l3, got);
            }
        }
    }
}

test "the byte entropy bound: none for one value, eight bits for every value once" {
    const same: [1000]u8 = @splat(7);
    try testing.expectEqual(@as(f64, 0), entropyBytes(&same));
    var all: [256]u8 = undefined;
    for (&all, 0..) |*b, i| b.* = @intCast(i);
    try testing.expectApproxEqAbs(@as(f64, 256), entropyBytes(&all), 1e-9);
}

// zpq's AVX-512 Huffman encoder in the vendored zstd (vendor/zstd/zpq): selection, and its comparison with zstd's own.
extern fn ZPQ_huf4x_select(mode: c_int) c_int;
extern fn ZPQ_huf4x_check(iters: c_ulong, seed: c_ulonglong, checked: *c_ulong) c_long;

test "the AVX-512 Huffman encoder writes zstd's bytes for every literal distribution" {
    var checked: c_ulong = 0;
    const mismatches = ZPQ_huf4x_check(3000, 0x5eed, &checked);
    if (mismatches < 0) return error.SkipZigTest; // no AVX-512 VBMI2 here, or ZPQ_ZSTD_SCALAR set
    try testing.expect(checked > 3000);
    try testing.expectEqual(@as(c_long, 0), mismatches);
}

test "zstd frames are the same bytes with the AVX-512 Huffman encoder and without" {
    defer _ = ZPQ_huf4x_select(0);
    if (ZPQ_huf4x_select(0) == 0) return error.SkipZigTest; // no AVX-512 VBMI2 here, or ZPQ_ZSTD_SCALAR set
    var prng = std.Random.DefaultPrng.init(0x4f4e);
    const r = prng.random();
    const cctx = c_zstd.ZSTD_createCCtx() orelse return error.OutOfMemory;
    defer _ = c_zstd.ZSTD_freeCCtx(cctx);
    const src = try testing.allocator.alloc(u8, 300_000);
    defer testing.allocator.free(src);
    const cap = @max(c_zstd.ZSTD_compressBound(src.len), rawFrameLen(src.len));
    const scalar = try testing.allocator.alloc(u8, cap);
    defer testing.allocator.free(scalar);
    const vector = try testing.allocator.alloc(u8, cap);
    defer testing.allocator.free(vector);
    // Text-like, skewed numeric and near-random bytes, at sizes around the 128 KiB block.
    for ([_]usize{ 100, 5_000, 70_000, 131_072, 300_000 }) |n| for (0..3) |shape| {
        for (src[0..n], 0..) |*b, i| b.* = switch (shape) {
            0 => "the quick brown fox jumps over a lazy dog "[(i * 7 + r.uintLessThan(usize, 3)) % 42],
            1 => @intCast(@min(255, @as(u32, @clz(r.int(u32) | 1)) * 9 + r.uintLessThan(u32, 4))),
            else => r.int(u8) & 0xBF,
        };
        for ([_]ZstdMode{ .literals, .level1, .level3 }) |m| {
            _ = ZPQ_huf4x_select(1);
            const a = try zstdWith(cctx, m, scalar, src[0..n]);
            _ = ZPQ_huf4x_select(0);
            const b = try zstdWith(cctx, m, vector, src[0..n]);
            try testing.expectEqualSlices(u8, scalar[0..a], vector[0..b]);
        }
    };
}

const GzipForm = union(enum) { level: std.compress.flate.Compress.Options, stored, huffman };

/// `data` as one gzip member written by the stdlib in `form`.
fn gzipStream(a: std.mem.Allocator, data: []const u8, form: GzipForm) ![]u8 {
    var out = try std.Io.Writer.Allocating.initCapacity(a, @max(64, data.len + data.len / 8 + 64));
    const window = try a.alloc(u8, std.compress.flate.max_window_len);
    const flate = std.compress.flate.Compress;
    switch (form) {
        .level => |opts| {
            var c = try flate.init(&out.writer, window, .gzip, opts);
            try c.writer.writeAll(data);
            try c.finish();
        },
        .stored => {
            var c = try flate.Raw.init(&out.writer, window, .gzip);
            try c.writer.writeAll(data);
            try c.finish();
        },
        .huffman => {
            var c = try flate.Huffman.init(&out.writer, window, .gzip);
            try c.writer.writeAll(data);
            try c.finish();
        },
    }
    return out.writer.buffered();
}

/// Decompresses `src` into a buffer of `want_len` with libdeflate and with the stdlib (`ZPQ_GZIP_STD`): each either
/// fails or gives back exactly `want`, both checking every member's CRC-32 and length, and neither writes outside its
/// buffer.
fn expectGzipBoth(a: std.mem.Allocator, src: []const u8, want: []const u8, want_len: usize) !struct { ok: [2]bool } {
    defer setGzipStd(false);
    var ok: [2]bool = undefined;
    for ([_]bool{ false, true }, 0..) |use_std, k| {
        setGzipStd(use_std);
        // Guard bytes either side of the output.
        const buf = try a.alloc(u8, want_len + 64);
        @memset(buf, 0xA5);
        const out = buf[32..][0..want_len];
        if (decompressGzip(a, src, out)) |_| {
            try testing.expectEqualSlices(u8, want, out);
            ok[k] = true;
        } else |_| ok[k] = false;
        for (buf[0..32]) |g| try testing.expectEqual(@as(u8, 0xA5), g);
        for (buf[32 + want_len ..]) |g| try testing.expectEqual(@as(u8, 0xA5), g);
    }
    return .{ .ok = ok };
}

test "GZIP pages: libdeflate and the stdlib inflate every block kind, level and size to the same bytes" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0x9a19);
    const r = prng.random();
    const flate = std.compress.flate.Compress.Options;
    const forms = [_]GzipForm{
        .stored,                    .huffman,
        .{ .level = flate.level_1 }, .{ .level = flate.level_2 },
        .{ .level = flate.level_3 }, .{ .level = flate.level_4 },
        .{ .level = flate.level_5 }, .{ .level = flate.level_6 },
        .{ .level = flate.level_7 }, .{ .level = flate.level_8 },
        .{ .level = flate.level_9 },
    };
    for ([_]usize{ 0, 1, 100, 65535, 65536, 65537, 300_000 }) |n| for (0..4) |kind| {
        const data = try a.alloc(u8, n);
        for (data, 0..) |*b, i| b.* = switch (kind) {
            0 => r.int(u8), // incompressible: stored blocks
            1 => 0, // one long run
            2 => "parquet page bytes, "[i % 20] ^ @as(u8, @intCast((i / 4096) % 3)), // matches
            else => @intCast(@min(255, @as(u32, @clz(r.int(u32) | 1)) * 7)), // skewed literals
        };
        for (forms) |form| {
            const z = try gzipStream(a, data, form);
            const got = try expectGzipBoth(a, z, data, n);
            try testing.expect(got.ok[0] and got.ok[1]);
        }
        // Two members back to back, as some writers emit.
        const half = n / 2;
        const two = try std.mem.concat(a, u8, &.{
            try gzipStream(a, data[0..half], .{ .level = flate.level_1 }),
            try gzipStream(a, data[half..], .stored),
        });
        const both = try expectGzipBoth(a, two, data, n);
        try testing.expect(both.ok[0] and both.ok[1]);
    };
}

test "GZIP pages: truncated, bit-flipped and oversized input fails cleanly in libdeflate and the stdlib" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0xbadf);
    const r = prng.random();
    const data = try a.alloc(u8, 3000);
    for (data, 0..) |*b, i| b.* = if (i % 5 == 0) r.int(u8) else "abcdefgh"[i % 8];
    for ([_]GzipForm{ .{ .level = .default }, .stored, .huffman }) |form| {
        const z = try gzipStream(a, data, form);
        // Every truncation: a gzip member is checked to its last byte, so each one fails, on both paths.
        for (0..z.len) |cut| {
            const got = try expectGzipBoth(a, z[0..cut], data, data.len);
            try testing.expect(!got.ok[0] and !got.ok[1]);
        }
        // Every single-bit flip, on both paths: an error, or the original exactly (a flip in a header field nothing
        // checks).
        const flipped = try a.dupe(u8, z);
        for (0..z.len * 8) |bit| {
            flipped[bit / 8] ^= @as(u8, 1) << @intCast(bit % 8);
            _ = try expectGzipBoth(a, flipped, data, data.len);
            flipped[bit / 8] ^= @as(u8, 1) << @intCast(bit % 8);
        }
        // A page header claiming more bytes than the member holds.
        const bigger = try a.alloc(u8, data.len + 1);
        @memcpy(bigger[0..data.len], data);
        const over = try expectGzipBoth(a, z, bigger, bigger.len);
        try testing.expect(!over.ok[0] and !over.ok[1]);
    }
}

/// A gzip member around one fixed-Huffman block: the literal 'a', then a match of length 3 at `distance`, then the
/// end of block, with the trailer of "aaaa".
fn fixedHuffmanMember(a: std.mem.Allocator, distance: u5) ![]u8 {
    var bits: std.ArrayList(u8) = .empty;
    var acc: u32 = 0;
    var n: u5 = 0;
    const Put = struct {
        fn bitsLsb(list: *std.ArrayList(u8), al: std.mem.Allocator, accp: *u32, np: *u5, v: u32, len: u5) !void {
            for (0..len) |i| {
                accp.* |= ((v >> @intCast(i)) & 1) << np.*;
                np.* += 1;
                if (np.* == 8) {
                    try list.append(al, @truncate(accp.*));
                    accp.* = 0;
                    np.* = 0;
                }
            }
        }
        // Huffman codes go most significant bit first.
        fn code(list: *std.ArrayList(u8), al: std.mem.Allocator, accp: *u32, np: *u5, v: u32, len: u5) !void {
            var i: u5 = len;
            while (i > 0) {
                i -= 1;
                try bitsLsb(list, al, accp, np, (v >> i) & 1, 1);
            }
        }
    };
    try Put.bitsLsb(&bits, a, &acc, &n, 1, 1); // BFINAL
    try Put.bitsLsb(&bits, a, &acc, &n, 1, 2); // BTYPE 01: fixed Huffman
    try Put.code(&bits, a, &acc, &n, 0x30 + 'a', 8); // literal 'a'
    try Put.code(&bits, a, &acc, &n, 257 - 256, 7); // length 3
    try Put.code(&bits, a, &acc, &n, distance - 1, 5); // distance code d-1 is distance d, for d <= 4
    try Put.code(&bits, a, &acc, &n, 0, 7); // end of block
    if (n > 0) try bits.append(a, @truncate(acc));
    const header = [_]u8{ 0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 3 };
    var trailer: [8]u8 = undefined;
    std.mem.writeInt(u32, trailer[0..4], std.hash.Crc32.hash("aaaa"), .little);
    std.mem.writeInt(u32, trailer[4..8], 4, .little);
    return std.mem.concat(a, u8, &.{ &header, bits.items, &trailer });
}

test "GZIP pages: a match reaching before the start of the output is rejected by both inflaters" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Distance 1 copies the 'a' just written: valid.
    const good = try expectGzipBoth(a, try fixedHuffmanMember(a, 1), "aaaa", 4);
    try testing.expect(good.ok[0] and good.ok[1]);
    // Distance 2 reaches a byte before the first one written.
    const bad = try expectGzipBoth(a, try fixedHuffmanMember(a, 2), "aaaa", 4);
    try testing.expect(!bad.ok[0] and !bad.ok[1]);
}

test "GZIP pages: both inflaters reject a member whose CRC-32 or length does not match its bytes" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const data = "a page whose trailer is wrong, a page whose trailer is wrong, a page whose trailer is wrong";
    defer setGzipStd(false);
    for ([_]usize{ 8, 4 }) |from_end| { // CRC-32's first byte, ISIZE's first byte
        const z = try a.dupe(u8, try gzipStream(a, data, .{ .level = .default }));
        z[z.len - from_end] ^= 1;
        const out = try a.alloc(u8, data.len);
        for ([_]bool{ false, true }) |use_std| {
            setGzipStd(use_std);
            try testing.expectError(error.DecompressionFailed, decompressGzip(a, z, out));
        }
    }
}

test "GZIP pages: every member is checked, empty ones and ones past the page's size included" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const empty = try gzipStream(a, "", .stored);
    const corrupt_empty = try a.dupe(u8, empty);
    corrupt_empty[corrupt_empty.len - 8] ^= 1; // its CRC-32
    const first = try gzipStream(a, "abc", .stored);
    const Case = struct { src: []const u8, out_len: usize, ok: bool };
    const cases = [_]Case{
        // A corrupt empty member, for an empty page.
        .{ .src = corrupt_empty, .out_len = 0, .ok = false },
        // "abc", then a corrupt empty member.
        .{ .src = try std.mem.concat(a, u8, &.{ first, corrupt_empty }), .out_len = 3, .ok = false },
        // "abc", then a member with more bytes than the page holds.
        .{ .src = try std.mem.concat(a, u8, &.{ first, try gzipStream(a, "d", .stored) }), .out_len = 3, .ok = false },
        // "abc", then bytes that are no gzip member.
        .{ .src = try std.mem.concat(a, u8, &.{ first, "junk" }), .out_len = 3, .ok = false },
        // A sound empty member, for an empty page; "abc" then a sound empty member; empty members on either side.
        .{ .src = empty, .out_len = 0, .ok = true },
        .{ .src = try std.mem.concat(a, u8, &.{ first, empty }), .out_len = 3, .ok = true },
        .{ .src = try std.mem.concat(a, u8, &.{ empty, first, empty }), .out_len = 3, .ok = true },
        // No body at all for an empty page: nothing to check.
        .{ .src = "", .out_len = 0, .ok = true },
    };
    defer setGzipStd(false);
    for ([_]bool{ false, true }) |use_std| {
        setGzipStd(use_std);
        for (cases) |case| {
            const out = try a.alloc(u8, case.out_len);
            if (case.ok) {
                try decompressGzip(a, case.src, out);
                try testing.expectEqualStrings("abc"[0..case.out_len], out);
            } else {
                try testing.expectError(error.DecompressionFailed, decompressGzip(a, case.src, out));
            }
        }
    }
}

test "LZ4_RAW pages from liblz4 decode with zpq's reader at the format's edges, and the same bytes every time" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0x1a24);
    const r = prng.random();
    // Around the end-of-block rules (12 bytes no match may start in, 5 literal bytes last), the 64 KiB match offset
    // limit, and sizes a page reaches.
    const sizes = [_]usize{ 0, 1, 4, 5, 11, 12, 13, 14, 17, 64, 65535, 65536, 65537, 131072, 1 << 20 };
    for (sizes) |n| for (0..5) |kind| {
        const data = try a.alloc(u8, n);
        switch (kind) {
            0 => r.bytes(data), // incompressible
            1 => @memset(data, 'Z'), // one run
            2 => for (data, 0..) |*b, i| {
                b.* = "the quick brown fox jumps over the lazy dog "[i % 44];
            },
            // A random block repeated at a period of exactly 65535, then 65536: matches at the offset limit and one
            // past it.
            else => {
                const period: usize = if (kind == 3) 65535 else 65536;
                r.bytes(data[0..@min(period, n)]);
                var i: usize = period;
                while (i < n) : (i += 1) data[i] = data[i - period];
            },
        }
        const page = try compressPage(a, data, .LZ4_RAW, .{});
        const again = try compressPage(a, data, .LZ4_RAW, .{});
        try testing.expectEqualSlices(u8, page, again);
        const back = try decompress(a, page, .LZ4_RAW, n);
        try testing.expectEqualSlices(u8, data, back);
        if (kind == 1 and n >= 4096) try testing.expect(page.len < n / 100);
    };
}
