//! Row-group pruning by an exact NaN probe of a float chunk.
//!
//! Floats compare in total order (`ast.floatOp`): NaN lies above every number, so a NaN row passes `x > c` and
//! `x >= c`. Float bounds leave NaN out, so a chunk whose max is below `c` can be skipped only when NaN is proven
//! absent; most writers record no NaN count, and such a chunk is then kept and read in full. This module proves the
//! absence from the data instead, much more cheaply than evaluating the filter: for each row group the bounds would
//! skip but for an unknown NaN count, it reads that float column's chunk alone (on S3 no other column is fetched),
//! decompresses its pages and tests the raw value bits for NaN, stopping at the first. Proven chunks are marked
//! (`ColumnChunk.nan_free`), and `prune.zig` then skips their row groups as it did before NaN counted. A chunk with a
//! NaN, or one the probe cannot read, is left unmarked and evaluated as usual.
//!
//! Only the value stream is tested, never the definition levels: a V1 page's levels are skipped by their length
//! prefix and a V2 page's lie outside its values, and a null takes no slot in the stream. PLAIN values are tested a
//! vector at a time: a value is NaN exactly when its bits with the sign cleared exceed +inf's, whatever its payload
//! (quiet or signalling) or sign. A dictionary is tested once, and dictionary-coded pages need nothing more: with no
//! NaN entry they hold no NaN. A dictionary with a NaN entry leaves the chunk unproven without decoding indices
//! (writers build a chunk's dictionary from the values it holds). Any other encoding leaves the chunk unproven.
//!
//! Like membership pruning, the probe runs once per query before any row group is planned, and what it proves lives
//! on the footers' chunks, so every caller of `pruneRowGroup` reaches the same answer.

const std = @import("std");
const ast = @import("ast.zig");
const prune = @import("prune.zig");
const membership = @import("membership.zig");
const schema = @import("../schema.zig");
const page_mod = @import("../parquet/page.zig");
const column = @import("../parquet/column.zig");
const spawn_util = @import("../spawn.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;

pub const Stats = struct {
    /// Chunks read and probed, and of those proven free of NaN.
    chunks: usize = 0,
    proven: usize = 0,
    /// Row groups the proofs let pruning skip.
    row_groups: usize = 0,
    /// The first wave proved too little to pay for probing the rest.
    stopped: bool = false,
    bytes: u64 = 0,
    ns: u64 = 0,
};

pub const Options = struct {
    /// Threads probing chunks; 0 means one per CPU, at most `max_threads`. The verdicts do not depend on it.
    threads: usize = 0,
    /// On a remote source, the most chunk bytes held fetched at once.
    batch_bytes: u64 = 64 << 20,
    /// Probe compressed chunks too. Decompressing dominates a probe, and a scan that evaluates its filter column
    /// first (the aggregate scan) pays only that much more for a chunk no row of passes, so there the probe of a
    /// compressed chunk buys nothing; a scan that reads other columns, or a source that must fetch them, gains.
    compressed: bool = true,
    /// The chunks probed first. If under `min_wave_proven` of them prove free of NaN, the rest are left unprobed: that
    /// bounds what probing costs a query whose chunks hold NaN, each probed and then read again.
    wave: usize = 16,
    min_wave_proven: f64 = 0.25,
};

const max_threads = 16;

/// Probe the float chunks `filter`'s row groups need proven free of NaN to be skipped, and mark each proven one.
/// `metas` and `src` are as `membership.load` takes them; what is marked lands on `metas`' chunks.
pub fn load(gpa: std.mem.Allocator, filter: ast.Filter, metas: []const schema.FileMetaData, src: membership.Source, opts: Options) !Stats {
    const t0 = nowMonoNs();
    var scratch_state = std.heap.ArenaAllocator.init(gpa);
    defer scratch_state.deinit();
    const scratch = scratch_state.allocator();
    var stats: Stats = .{};

    const trusted = try ast.trustMissingNanCounts(scratch, filter);
    var cols: std.ArrayList(usize) = .empty;
    try floatColumns(scratch, filter, &cols);
    if (cols.items.len == 0) return stats;

    var targets: std.ArrayList(Target) = .empty;
    for (metas, 0..) |*fm, fi| for (fm.row_groups.items, 0..) |*rg, ri| {
        if (prune.pruneRowGroup(rg, filter, fm) == .skip) continue;
        // Only a row group the bounds skip once NaN is ruled out is worth reading a chunk for.
        if (prune.pruneRowGroup(rg, trusted, fm) != .skip) continue;
        for (cols.items) |ci| {
            const t = target(fm, rg, @intCast(fi), @intCast(ri), ci) orelse continue;
            if (!opts.compressed and t.codec != .UNCOMPRESSED) continue;
            try targets.append(scratch, t);
        }
    };
    if (targets.items.len == 0) {
        stats.ns = @intCast(nowMonoNs() - t0);
        return stats;
    }

    // Batches of at most `batch_bytes` fetched at once on a remote source; a mapping hands back slices for free. The
    // first batch is the first wave.
    var start: usize = 0;
    while (start < targets.items.len) {
        if (start > 0 and start <= opts.wave and
            @as(f64, @floatFromInt(stats.proven)) < opts.min_wave_proven * @as(f64, @floatFromInt(stats.chunks)))
        {
            stats.stopped = true;
            break;
        }
        var end = start;
        var held: u64 = 0;
        const cap = if (start == 0) @min(opts.wave, targets.items.len) else targets.items.len;
        while (end < cap and (end == start or src.cost.request_bytes == 0 or
            held + spanLen(targets.items[end].span) <= opts.batch_bytes)) : (end += 1)
        {
            held += spanLen(targets.items[end].span);
        }
        defer start = end;
        var batch_state = std.heap.ArenaAllocator.init(gpa);
        defer batch_state.deinit();
        const batch = targets.items[start..end];
        const spans = try batch_state.allocator().alloc(membership.Span, batch.len);
        for (batch, spans) |t, *s| s.* = t.span;
        const got = try batch_state.allocator().alloc([]const u8, batch.len);
        try src.vtable.read(src.ctx, batch_state.allocator(), spans, got);
        for (batch, got) |*t, g| t.bytes = g;
        probeAll(gpa, batch, opts.threads);
        for (batch) |t| if (t.oom) return error.OutOfMemory;
        stats.bytes += held;
        // Mark before the batch's bytes go: the verdicts are all that is kept.
        for (batch) |t| {
            stats.chunks += 1;
            if (!t.nan_free) continue;
            stats.proven += 1;
            metas[t.file].row_groups.items[t.rg].columns.items[t.col].nan_free = true;
        }
    }
    for (targets.items, 0..) |t, i| {
        // Count each row group once, at its first target.
        if (i > 0 and targets.items[i - 1].file == t.file and targets.items[i - 1].rg == t.rg) continue;
        const fm = &metas[t.file];
        if (prune.pruneRowGroup(&fm.row_groups.items[t.rg], filter, fm) == .skip) stats.row_groups += 1;
    }
    stats.ns = @intCast(nowMonoNs() - t0);
    return stats;
}

fn spanLen(s: membership.Span) u64 {
    return s.end - s.start;
}

/// The column of every FLOAT/DOUBLE leaf of `f`, once each.
fn floatColumns(arena: std.mem.Allocator, f: ast.Filter, out: *std.ArrayList(usize)) !void {
    switch (f) {
        .and_filter, .or_filter => |c| for (c.children) |child| try floatColumns(arena, child, out),
        inline .float, .double => |l| if (std.mem.indexOfScalar(usize, out.items, l.col_idx) == null) {
            try out.append(arena, l.col_idx);
        },
        else => {},
    }
}

const Target = struct {
    file: u32,
    rg: u32,
    col: u32,
    span: membership.Span,
    codec: schema.CompressionCodec,
    physical: schema.Type,
    max_def: u32,
    /// What the page reader may inflate the chunk to (`column.inflateLimit`).
    limit: usize,
    bytes: []const u8 = &.{},
    nan_free: bool = false,
    /// The probe ran out of memory: the query fails as it would anywhere else.
    oom: bool = false,
};

/// The chunk of column `ci` to probe, or null when there is nothing to prove or the probe cannot read it: a count
/// already there, a proof already made, not a flat float column of this file, encrypted, or no readable extent.
fn target(fm: *const schema.FileMetaData, rg: *const schema.RowGroup, fi: u32, ri: u32, ci: usize) ?Target {
    if (ci >= rg.columns.items.len) return null;
    const chunk = &rg.columns.items[ci];
    if (chunk.nan_free or chunk.encrypted or chunk.file_path != null) return null;
    const cm = chunk.meta_data orelse return null;
    if (cm.type != .FLOAT and cm.type != .DOUBLE) return null;
    if (cm.statistics) |st| if (st.nan_count != null) return null;
    const elem = fm.getColumnSchema(cm.path_in_schema.items) orelse return null;
    if (elem.type == null or elem.type.? != cm.type) return null;
    const levels = fm.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep != 0) return null;
    // The chunk starts at its dictionary page when it has one, else at its first data page.
    if (cm.data_page_offset < 4 or cm.total_compressed_size <= 0) return null;
    var first: i64 = cm.data_page_offset;
    if (cm.dictionary_page_offset) |d| if (d >= 4 and d < first) {
        first = d;
    };
    const s: u64 = @intCast(first);
    return .{
        .file = fi,
        .rg = ri,
        .col = @intCast(ci),
        .span = .{ .file = fi, .start = s, .end = s + @as(u64, @intCast(cm.total_compressed_size)) },
        .codec = cm.codec,
        .physical = cm.type,
        .max_def = @intCast(levels.max_def),
        .limit = column.inflateLimit(cm),
    };
}

/// Probe `targets` over up to `threads` threads (0: one per CPU), each taking the next unprobed chunk.
fn probeAll(gpa: std.mem.Allocator, targets: []Target, threads: usize) void {
    const want = if (threads == 0) std.Thread.getCpuCount() catch 1 else threads;
    const n = @max(1, @min(want, targets.len, max_threads));
    var cursor: std.atomic.Value(usize) = .init(0);
    var workers: [max_threads]Worker = undefined;
    for (workers[0..n]) |*w| w.* = .{ .targets = targets, .cursor = &cursor, .gpa = gpa };
    const config: std.Thread.SpawnConfig = .{ .stack_size = 1 << 20 };
    if (n <= 1 or !spawn_util.runTree(Worker, workers[0..n], config, Worker.run)) workers[0].run();
}

const Worker = struct {
    targets: []Target,
    cursor: *std.atomic.Value(usize),
    gpa: std.mem.Allocator,

    fn run(self: *Worker) void {
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        while (true) {
            const i = self.cursor.fetchAdd(1, .monotonic);
            if (i >= self.targets.len) return;
            const t = &self.targets[i];
            // A chunk the probe cannot read is left unproven: kept and evaluated, never wrongly skipped. Running out of
            // memory is no verdict on the chunk; `load` fails with it.
            const got = switch (t.physical) {
                .FLOAT => chunkNanFree(f32, arena.allocator(), t.*),
                .DOUBLE => chunkNanFree(f64, arena.allocator(), t.*),
                else => false,
            };
            t.nan_free = got catch |err| blk: {
                t.oom = err == error.OutOfMemory;
                break :blk false;
            };
            _ = arena.reset(.retain_capacity);
        }
    }
};

/// Whether the chunk `t.bytes` holds no NaN, read page by page until the first NaN. False also for anything the probe
/// does not read: an encoding other than PLAIN and dictionary, levels it cannot step over, a short or ragged stream.
pub fn chunkNanFree(comptime T: type, arena: std.mem.Allocator, t: Target) !bool {
    if (t.bytes.len != spanLen(t.span)) return false;
    var pages = page_mod.PageReader.init(t.bytes, t.codec, arena);
    pages.byte_limit = t.limit;
    // Whether the dictionary, once read, holds no NaN; null before one is read.
    var dict_free: ?bool = null;
    while (try pages.next()) |p| {
        switch (p.header.type) {
            .DICTIONARY_PAGE => {
                const h = p.header.dictionary_page_header orelse return false;
                if (h.encoding != .PLAIN and h.encoding != .PLAIN_DICTIONARY) return false;
                if (anyNan(T, p.bytes) orelse return false) return false;
                dict_free = true;
            },
            .DATA_PAGE => {
                const h = p.header.data_page_header orelse return false;
                const values = v1Values(p.bytes, t.max_def, h.definition_level_encoding) orelse return false;
                if (!try pageNanFree(T, h.encoding, values, dict_free)) return false;
            },
            .DATA_PAGE_V2 => {
                const h = p.header.data_page_header_v2 orelse return false;
                const skip: usize = @intCast(h.repetition_levels_byte_length + h.definition_levels_byte_length);
                if (skip > p.bytes.len) return false;
                if (!try pageNanFree(T, h.encoding, p.bytes[skip..], dict_free)) return false;
            },
            .INDEX_PAGE => {},
        }
    }
    return true;
}

fn pageNanFree(comptime T: type, encoding: schema.Encoding, values: []const u8, dict_free: ?bool) !bool {
    return switch (encoding) {
        .PLAIN => !(anyNan(T, values) orelse return false),
        // Every value is a dictionary entry, and the dictionary holds no NaN.
        .PLAIN_DICTIONARY, .RLE_DICTIONARY => dict_free orelse false,
        else => false,
    };
}

/// The value stream of a V1 data page of a flat column: past its definition levels, RLE-encoded behind a 4-byte
/// length. Null for levels it cannot step over (the deprecated BIT_PACKED encoding carries no length).
fn v1Values(page: []const u8, max_def: u32, def_encoding: schema.Encoding) ?[]const u8 {
    if (max_def == 0) return page;
    if (def_encoding != .RLE or page.len < 4) return null;
    const len = std.mem.readInt(u32, page[0..4], .little);
    if (len > page.len - 4) return null;
    return page[4 + len ..];
}

/// Whether the PLAIN `T` values in `bytes` include a NaN, of any payload or sign: a value is NaN exactly when its bits
/// with the sign cleared exceed +inf's. Null when `bytes` is no whole number of values. A vector at a time, stopping at
/// the first block holding one.
pub fn anyNan(comptime T: type, bytes: []const u8) ?bool {
    const Bits = @Int(.unsigned, @bitSizeOf(T));
    const size = @sizeOf(T);
    if (bytes.len % size != 0) return null;
    const n = bytes.len / size;
    const abs_mask: Bits = std.math.maxInt(Bits) >> 1;
    const inf_bits: Bits = @bitCast(std.math.inf(T));
    // 64 bytes a vector; a block of eight vectors is OR-reduced before the one branch.
    const lanes = 64 / size;
    const V = @Vector(lanes, Bits);
    const block = 8 * lanes;
    var i: usize = 0;
    while (i + block <= n) : (i += block) {
        var hit: @Vector(lanes, bool) = @splat(false);
        inline for (0..8) |k| {
            // A load through an unaligned vector pointer: bit-casting the 64-byte array instead built the vector a
            // lane at a time, two orders of magnitude slower.
            const v: V = @as(*align(1) const V, @ptrCast(bytes[(i + k * lanes) * size ..][0 .. lanes * size])).*;
            hit = hit | ((v & @as(V, @splat(abs_mask))) > @as(V, @splat(inf_bits)));
        }
        if (@reduce(.Or, hit)) return true;
    }
    while (i < n) : (i += 1) {
        const v = std.mem.readInt(Bits, bytes[i * size ..][0..size], .little);
        if (v & abs_mask > inf_bits) return true;
    }
    return false;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

/// NaN payloads of every kind: quiet and signalling, either sign, the least and greatest payloads.
fn nanVariety(comptime T: type) [6]T {
    const Bits = @Int(.unsigned, @bitSizeOf(T));
    const exp: Bits = @bitCast(std.math.inf(T));
    const sign: Bits = @as(Bits, 1) << (@bitSizeOf(T) - 1);
    const quiet: Bits = @as(Bits, 1) << (std.math.floatMantissaBits(T) - 1);
    const all: Bits = (@as(Bits, 1) << std.math.floatMantissaBits(T)) - 1;
    return .{
        @bitCast(exp | quiet),
        @bitCast(sign | exp | quiet),
        @bitCast(exp | 1), // signalling, least payload
        @bitCast(sign | exp | 1),
        @bitCast(exp | all),
        @bitCast(sign | exp | all),
    };
}

test "anyNan finds every NaN payload at every position, and nothing in infinities, zeros or extremes" {
    inline for (.{ f32, f64 }) |T| {
        const not_nan = [_]T{ std.math.inf(T), -std.math.inf(T), 0.0, -0.0, std.math.floatMax(T), -std.math.floatMax(T), std.math.floatTrueMin(T), 1.5 };
        // Lengths around the block and vector sizes, so the NaN lands in a full block, the tail, first and last.
        for ([_]usize{ 0, 1, 7, 15, 16, 127, 128, 129, 300 }) |n| {
            var vals: [300]T = undefined;
            for (vals[0..n], 0..) |*v, i| v.* = not_nan[i % not_nan.len];
            try testing.expectEqual(@as(?bool, false), anyNan(T, std.mem.sliceAsBytes(vals[0..n])));
            if (n == 0) continue;
            for (nanVariety(T)) |nan| for ([_]usize{ 0, n / 2, n - 1 }) |at| {
                const keep = vals[at];
                vals[at] = nan;
                try testing.expectEqual(@as(?bool, true), anyNan(T, std.mem.sliceAsBytes(vals[0..n])));
                vals[at] = keep;
            };
        }
        // A stream that is no whole number of values is not read.
        try testing.expectEqual(@as(?bool, null), anyNan(T, &[_]u8{ 1, 2, 3 }));
    }
}

const thrift = @import("../thrift.zig");
const compression = @import("../parquet/compression.zig");
const hybrid_rle = @import("../parquet/encoding/hybrid_rle.zig");

/// Pages of one flat nullable column chunk, written the way writers lay them out.
fn TestChunk(comptime T: type) type {
    return struct {
        const Self = @This();
        a: std.mem.Allocator,
        codec: schema.CompressionCodec,
        bytes: std.ArrayList(u8) = .empty,

        fn header(self: *Self, h: schema.PageHeader) !void {
            var w = thrift.Writer.init(self.a);
            try h.write(&w);
            try self.bytes.appendSlice(self.a, w.bytes());
        }

        fn dictionary(self: *Self, entries: []const T) !void {
            const body = try compression.compress(self.a, std.mem.sliceAsBytes(entries), self.codec);
            try self.header(.{
                .type = .DICTIONARY_PAGE,
                .uncompressed_page_size = @intCast(entries.len * @sizeOf(T)),
                .compressed_page_size = @intCast(body.len),
                .crc = null,
                .data_page_header = null,
                .dictionary_page_header = .{ .num_values = @intCast(entries.len), .encoding = .PLAIN, .is_sorted = null },
                .data_page_header_v2 = null,
            });
            try self.bytes.appendSlice(self.a, body);
        }

        /// A V1 page of `rows` rows, `present` of them non-null (the first ones), with `values` as its stream.
        fn v1(self: *Self, encoding: schema.Encoding, def_encoding: schema.Encoding, rows: usize, present: usize, values: []const u8) !void {
            const defs = try self.a.alloc(u32, rows);
            for (defs, 0..) |*d, i| d.* = @intFromBool(i < present);
            const levels = try hybrid_rle.encode(self.a, defs, 1);
            var raw: std.ArrayList(u8) = .empty;
            var len: [4]u8 = undefined;
            std.mem.writeInt(u32, &len, @intCast(levels.len), .little);
            try raw.appendSlice(self.a, &len);
            try raw.appendSlice(self.a, levels);
            try raw.appendSlice(self.a, values);
            const body = try compression.compress(self.a, raw.items, self.codec);
            try self.header(.{
                .type = .DATA_PAGE,
                .uncompressed_page_size = @intCast(raw.items.len),
                .compressed_page_size = @intCast(body.len),
                .crc = null,
                .data_page_header = .{ .num_values = @intCast(rows), .encoding = encoding, .definition_level_encoding = def_encoding, .repetition_level_encoding = .RLE },
                .dictionary_page_header = null,
                .data_page_header_v2 = null,
            });
            try self.bytes.appendSlice(self.a, body);
        }

        /// A V2 page: levels uncompressed ahead of the (compressed) value stream.
        fn v2(self: *Self, rows: usize, present: usize, values: []const T) !void {
            const defs = try self.a.alloc(u32, rows);
            for (defs, 0..) |*d, i| d.* = @intFromBool(i < present);
            const levels = try hybrid_rle.encode(self.a, defs, 1);
            const body = try compression.compress(self.a, std.mem.sliceAsBytes(values), self.codec);
            try self.header(.{
                .type = .DATA_PAGE_V2,
                .uncompressed_page_size = @intCast(levels.len + values.len * @sizeOf(T)),
                .compressed_page_size = @intCast(levels.len + body.len),
                .crc = null,
                .data_page_header = null,
                .dictionary_page_header = null,
                .data_page_header_v2 = .{
                    .num_values = @intCast(rows),
                    .num_nulls = @intCast(rows - present),
                    .num_rows = @intCast(rows),
                    .encoding = .PLAIN,
                    .definition_levels_byte_length = @intCast(levels.len),
                    .repetition_levels_byte_length = 0,
                    .is_compressed = self.codec != .UNCOMPRESSED,
                },
            });
            try self.bytes.appendSlice(self.a, levels);
            try self.bytes.appendSlice(self.a, body);
        }

        fn nanFree(self: *const Self) !bool {
            const t: Target = .{
                .file = 0,
                .rg = 0,
                .col = 0,
                .span = .{ .file = 0, .start = 4, .end = 4 + self.bytes.items.len },
                .codec = self.codec,
                .physical = if (T == f32) .FLOAT else .DOUBLE,
                .max_def = 1,
                .limit = 1 << 30,
                .bytes = self.bytes.items,
            };
            return chunkNanFree(T, self.a, t) catch false;
        }
    };
}

test "chunkNanFree: PLAIN pages, V1 and V2, nulls, all-null pages, and a NaN as a page's last value" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    inline for (.{ f32, f64 }) |T| for ([_]schema.CompressionCodec{ .UNCOMPRESSED, .SNAPPY, .ZSTD }) |codec| {
        var vals: [200]T = undefined;
        for (&vals, 0..) |*v, i| v.* = @as(T, @floatFromInt(i)) - 50.5;
        vals[3] = std.math.inf(T);
        vals[4] = -0.0;
        for ([_]?T{null} ++ nanVariety(T)) |nan| {
            var c: TestChunk(T) = .{ .a = a, .codec = codec };
            // A page with nulls, an all-null page (no values at all), then a page whose last value may be NaN.
            try c.v1(.PLAIN, .RLE, 150, 100, std.mem.sliceAsBytes(vals[0..100]));
            try c.v1(.PLAIN, .RLE, 64, 0, &.{});
            var last = vals[100..200].*;
            if (nan) |x| last[99] = x;
            try c.v1(.PLAIN, .RLE, 130, 100, std.mem.sliceAsBytes(&last));
            try testing.expectEqual(nan == null, try c.nanFree());

            var d: TestChunk(T) = .{ .a = a, .codec = codec };
            try d.v2(150, 100, vals[0..100]);
            try d.v2(10, 0, &.{});
            try d.v2(120, 100, &last);
            try testing.expectEqual(nan == null, try d.nanFree());
        }
    };
}

test "chunkNanFree: a dictionary proves its coded pages, a PLAIN fallback page is still read, and nothing else is" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    inline for (.{ f32, f64 }) |T| for ([_]schema.CompressionCodec{ .UNCOMPRESSED, .SNAPPY }) |codec| {
        const nan = nanVariety(T)[2];
        // Indices: bit width 2, one RLE run of four 1s. The probe never decodes them.
        const indices = [_]u8{ 2, 4 << 1, 1 };
        const plain_ok = std.mem.sliceAsBytes(&[_]T{ 1, 2, 3 });
        const plain_nan = std.mem.sliceAsBytes(&[_]T{ 1, 2, nan });

        var ok: TestChunk(T) = .{ .a = a, .codec = codec };
        try ok.dictionary(&.{ 0.5, -1, 9 });
        try ok.v1(.RLE_DICTIONARY, .RLE, 6, 4, &indices);
        try ok.v1(.PLAIN_DICTIONARY, .RLE, 6, 4, &indices);
        try testing.expect(try ok.nanFree());
        // The writer fell back to PLAIN after the dictionary filled: those pages are read like any.
        try ok.v1(.PLAIN, .RLE, 3, 3, plain_ok);
        try testing.expect(try ok.nanFree());
        try ok.v1(.PLAIN, .RLE, 3, 3, plain_nan);
        try testing.expect(!try ok.nanFree());

        // A NaN entry leaves the chunk unproven, whatever the indices pick.
        var with_nan: TestChunk(T) = .{ .a = a, .codec = codec };
        try with_nan.dictionary(&.{ 0.5, -1, nan });
        try with_nan.v1(.RLE_DICTIONARY, .RLE, 6, 4, &indices);
        try testing.expect(!try with_nan.nanFree());

        // Coded pages without a dictionary, an encoding the probe does not read, levels it cannot step over.
        var orphan: TestChunk(T) = .{ .a = a, .codec = codec };
        try orphan.v1(.RLE_DICTIONARY, .RLE, 6, 4, &indices);
        try testing.expect(!try orphan.nanFree());
        var split: TestChunk(T) = .{ .a = a, .codec = codec };
        try split.v1(.BYTE_STREAM_SPLIT, .RLE, 3, 3, plain_ok);
        try testing.expect(!try split.nanFree());
        var packed_levels: TestChunk(T) = .{ .a = a, .codec = codec };
        try packed_levels.v1(.PLAIN, .BIT_PACKED, 3, 3, plain_ok);
        try testing.expect(!try packed_levels.nanFree());

        // A chunk cut short is not proven.
        var cut: TestChunk(T) = .{ .a = a, .codec = codec };
        try cut.v1(.PLAIN, .RLE, 3, 3, plain_ok);
        cut.bytes.items.len -= 1;
        try testing.expect(!try cut.nanFree());
    };
}

test "load proves a chunk free of NaN and pruning then skips its row group; a NaN, or a probe not asked for, keeps it" {
    const test_fixtures = @import("../parquet/test_fixtures.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const gt5: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Gt, .value = 5 } };
    const lt_neg: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Lt, .value = -1 } };
    for (nanVariety(f64) ++ [_]f64{0.25}) |last| {
        const with_nan = std.math.isNan(last);
        var vals: [100]f64 = undefined;
        for (&vals, 0..) |*v, i| v.* = @as(f64, @floatFromInt(i)) / 100;
        vals[99] = last;
        var c: TestChunk(f64) = .{ .a = a, .codec = .SNAPPY };
        try c.v1(.PLAIN, .RLE, 120, 100, std.mem.sliceAsBytes(&vals));
        const file = try std.mem.concat(a, u8, &.{ "PAR1", c.bytes.items });

        const lo = std.mem.toBytes(@as(f64, 0.0));
        const hi = std.mem.toBytes(@as(f64, 0.99));
        var sc = try test_fixtures.StatsColumn.init(a, test_fixtures.statsLeaf(.DOUBLE), .{ .min_value = &lo, .max_value = &hi, .null_count = 20 });
        const cm = &sc.rg.columns.items[0].meta_data.?;
        cm.codec = .SNAPPY;
        cm.data_page_offset = 4;
        cm.total_compressed_size = @intCast(c.bytes.items.len);
        try sc.fm.row_groups.append(a, sc.rg);
        var metas = [_]schema.FileMetaData{sc.fm};
        const rg = &metas[0].row_groups.items[0];
        const mapped: membership.MappedSource = .{ .files = &.{file} };

        // Bounds that decide alone, and a probe of compressed chunks not asked for: nothing is read.
        try testing.expectEqual(@as(usize, 0), (try load(testing.allocator, lt_neg, &metas, mapped.source(), .{})).chunks);
        try testing.expectEqual(@as(usize, 0), (try load(testing.allocator, gt5, &metas, mapped.source(), .{ .compressed = false })).chunks);
        try testing.expect(prune.pruneRowGroup(rg, gt5, &metas[0]) != .skip);

        const stats = try load(testing.allocator, gt5, &metas, mapped.source(), .{});
        try testing.expectEqual(@as(usize, 1), stats.chunks);
        try testing.expectEqual(!with_nan, rg.columns.items[0].nan_free);
        try testing.expectEqual(!with_nan, prune.pruneRowGroup(rg, gt5, &metas[0]) == .skip);
        // A proven chunk is not probed again.
        if (!with_nan) try testing.expectEqual(@as(usize, 0), (try load(testing.allocator, gt5, &metas, mapped.source(), .{})).chunks);
    }
}

test "load fails cleanly, or proves exactly, under every allocation failure" {
    const test_fixtures = @import("../parquet/test_fixtures.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var vals: [64]f32 = undefined;
    for (&vals, 0..) |*v, i| v.* = @floatFromInt(i);
    var c: TestChunk(f32) = .{ .a = a, .codec = .ZSTD };
    try c.dictionary(&.{ 1, 2 });
    try c.v1(.PLAIN, .RLE, 64, 64, std.mem.sliceAsBytes(&vals));
    const file = try std.mem.concat(a, u8, &.{ "PAR1", c.bytes.items });
    const lo = std.mem.toBytes(@as(f32, 0.0));
    const hi = std.mem.toBytes(@as(f32, 63.0));
    var sc = try test_fixtures.StatsColumn.init(a, test_fixtures.statsLeaf(.FLOAT), .{ .min_value = &lo, .max_value = &hi, .null_count = 0 });
    const cm = &sc.rg.columns.items[0].meta_data.?;
    cm.codec = .ZSTD;
    cm.dictionary_page_offset = 4;
    cm.data_page_offset = 5;
    cm.total_compressed_size = @intCast(c.bytes.items.len);
    try sc.fm.row_groups.append(a, sc.rg);
    var metas = [_]schema.FileMetaData{sc.fm};
    const mapped: membership.MappedSource = .{ .files = &.{file} };
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, ms: []schema.FileMetaData, src: membership.Source) !void {
            const chunk = &ms[0].row_groups.items[0].columns.items[0];
            chunk.nan_free = false;
            const f: ast.Filter = .{ .float = .{ .col_idx = 0, .op = .GtEq, .value = 100 } };
            _ = try load(gpa, f, ms, src, .{ .threads = 1 });
            // Run to the end, the probe proves this NaN-free chunk, and pruning skips it.
            try testing.expect(chunk.nan_free);
            if (chunk.nan_free) try testing.expect(prune.pruneRowGroup(&ms[0].row_groups.items[0], f, &ms[0]) == .skip);
        }
    }.run, .{ @as([]schema.FileMetaData, &metas), mapped.source() });
}
