//! Bounds for flat GROUP BY keys. Encoding coverage and the probe cost gate use only footers;
//! entry counts use dictionary headers. Dictionary entries may be unused, so even a union is only an upper bound.
const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const expr = @import("expr/ast.zig");
const agg = @import("expr/agg.zig");
const membership = @import("filter/membership.zig");
const page = @import("parquet/page.zig");
const plain = @import("parquet/encoding/plain.zig");
const spawn = @import("spawn.zig");

pub const Stat = struct {
    source: enum { unknown, dictionary_headers, dictionary_union } = .unknown,
    lo: u64 = 0,
    hi: u64 = 0,
    /// Cost hint from a single-key union, never a proof: dictionaries can contain unused entries.
    estimate: ?u64 = null,
    rows: u64 = 0,
    covered_rows: u64 = 0,
    encoding_stats_rows: u64 = 0,
    confidence: enum { unknown, upper_bound } = .unknown,
    dictionary_bytes: u64 = 0,
    column_bytes: u64 = 0,
    key_bytes: u64 = 0,
    string_keys: bool = false,
    union_attempted: bool = false,
    union_complete: bool = false,
    union_budget_exceeded: bool = false,
};

pub const File = struct { bytes: []const u8, local: bool };
const Ref = struct { file: usize, rg: usize };
const max_input = 25 * 1024 * 1024;

pub fn fullCoverage(cm: *const schema.ColumnMetaData) bool {
    // The legacy encodings list does not prove coverage; PLAIN can describe the dictionary OR fallback data.
    return cm.encoding_stats != null and membership.dictionaryCoversChunk(cm);
}

fn dictBytes(cm: schema.ColumnMetaData) ?u64 {
    const start = cm.dictionary_page_offset orelse return null;
    if (start < 0 or cm.data_page_offset <= start) return null;
    return @intCast(cm.data_page_offset - start);
}

fn dictSlice(file: File, cm: schema.ColumnMetaData) ?[]const u8 {
    if (!file.local) return null;
    const len = dictBytes(cm) orelse return null;
    const start: u64 = @intCast(cm.dictionary_page_offset.?);
    if (start > file.bytes.len or len > file.bytes.len - start) return null;
    return file.bytes[@intCast(start)..][0..@intCast(len)];
}

fn header(bytes: []const u8) ?schema.PageHeader {
    // Bound malformed header work and never cross into the following data page.
    var reader = thrift.Reader.init(bytes[0..@min(bytes.len, 64 * 1024)]);
    const h = schema.PageHeader.read(&reader) catch return null;
    if (h.type != .DICTIONARY_PAGE or h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return null;
    const d = h.dictionary_page_header orelse return null;
    // Legacy writers label PLAIN dictionary payloads PLAIN_DICTIONARY (the execution reader accepts these too).
    if (d.num_values < 0 or (d.encoding != .PLAIN and d.encoding != .PLAIN_DICTIONARY)) return null;
    if (@as(u64, @intCast(h.compressed_page_size)) + reader.pos != bytes.len) return null;
    return h;
}

fn nullBound(cm: schema.ColumnMetaData) u64 {
    // Missing null_count is not zero. One extra entry is always safe for an optional flat key.
    return if (cm.statistics) |s| if (s.null_count == 0) 0 else 1 else 1;
}

pub fn collect(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    keys: []const expr.Expr,
    workers: usize,
    max_memory: usize,
    allow_union: bool,
) !Stat {
    var stat: Stat = .{};
    var refs: std.ArrayList(Ref) = .empty;
    for (metas, 0..) |m, fi| for (m.row_groups.items, 0..) |rg, ri| {
        stat.rows +|= @intCast(@max(0, rg.num_rows));
        try refs.append(arena, .{ .file = fi, .rg = ri });
    };
    stat.hi = stat.rows;
    if (keys.len == 0) return stat;
    for (keys) |key| {
        stat.string_keys = stat.string_keys or key.typeOf() == .str;
        if (key != .col_ref) return stat;
    }
    const sums = try arena.alloc(u64, keys.len);
    const full = try arena.alloc(bool, keys.len);
    const dict_sizes = try arena.alloc(u64, keys.len);
    const col_sizes = try arena.alloc(u64, keys.len);
    const nulls = try arena.alloc(u64, keys.len);
    @memset(sums, 0);
    @memset(full, true);
    @memset(dict_sizes, 0);
    @memset(col_sizes, 0);
    @memset(nulls, 0);
    var sum_products: u64 = 0;
    var raw_key_bytes: u64 = 0;
    var known = false;
    for (refs.items) |ref| {
        const m = &metas[ref.file];
        const rg = &m.row_groups.items[ref.rg];
        const rows: u64 = @intCast(@max(0, rg.num_rows));
        var product: u64 = 1;
        var covered = true;
        var stats_present = true;
        for (keys, 0..) |key, ki| {
            const ci = key.col_ref.col_idx;
            if (ci >= rg.columns.items.len) {
                full[ki] = false;
                covered = false;
                stats_present = false;
                product = rows;
                sums[ki] +|= rows;
                continue;
            }
            const cm = rg.columns.items[ci].meta_data orelse {
                full[ki] = false;
                covered = false;
                stats_present = false;
                product = rows;
                sums[ki] +|= rows;
                continue;
            };
            stats_present = stats_present and cm.encoding_stats != null;
            col_sizes[ki] +|= @intCast(@max(0, cm.total_compressed_size));
            raw_key_bytes +|= @intCast(@max(0, cm.total_uncompressed_size));
            const flat = cm.path_in_schema.items.len == 1 and m.getColumnLevels(cm.path_in_schema.items).max_rep == 0;
            if (!flat) return stat;
            const covers = fullCoverage(&cm);
            if (dictBytes(cm)) |n| dict_sizes[ki] +|= n else full[ki] = false;
            const n: u64 = blk: {
                if (covers) if (dictSlice(files[ref.file], cm)) |b| if (header(b)) |h| {
                    known = true;
                    break :blk @min(rows, @as(u64, @intCast(h.dictionary_page_header.?.num_values)) + nullBound(cm));
                };
                covered = false;
                full[ki] = false;
                break :blk rows;
            };
            sums[ki] +|= n;
            nulls[ki] = @max(nulls[ki], nullBound(cm));
            product = @min(rows, product *| n);
        }
        sum_products +|= @min(rows, product);
        if (covered) stat.covered_rows +|= rows;
        if (stats_present) stat.encoding_stats_rows +|= rows;
    }
    for (dict_sizes, col_sizes) |d, c| {
        stat.dictionary_bytes +|= d;
        stat.column_bytes +|= c;
    }
    stat.key_bytes = @max(@as(u64, @intCast(keys.len)) * 9, raw_key_bytes / @max(1, stat.rows));
    stat.hi = @min(stat.rows, sum_products);
    if (known) {
        stat.source = .dictionary_headers;
        stat.confidence = .upper_bound;
    }
    const total_gate = stat.dictionary_bytes <= max_input or stat.dictionary_bytes <= stat.column_bytes / 20;
    var union_product: u64 = 1;
    for (keys, 0..) |key, ki| {
        // Footer gate plus an absolute ceiling on scratch even when 5% admits a large compressed column.
        const gated = total_gate and full[ki] and
            (dict_sizes[ki] <= max_input or dict_sizes[ki] <= col_sizes[ki] / 20);
        if (allow_union and full[ki] and (!gated or max_memory < 1024 * 1024))
            stat.union_budget_exceeded = true;
        if (allow_union and gated and max_memory >= 1024 * 1024) {
            stat.union_attempted = true;
            if (try unionBound(gpa, arena, files, metas, refs.items, key.col_ref.col_idx, workers, max_memory)) |n| {
                sums[ki] = @min(sums[ki], n + nulls[ki]);
                // Multiplying marginal cardinalities does not estimate the number of observed tuples.
                if (keys.len == 1) stat.estimate = @min(stat.rows, n + nulls[ki]);
            } else full[ki] = false;
        } else full[ki] = false;
        union_product *|= sums[ki];
    }
    stat.hi = @min(stat.hi, union_product);
    stat.union_complete = allow_union;
    for (full) |f| stat.union_complete = stat.union_complete and f;
    if (stat.union_complete) {
        stat.source = .dictionary_union;
        stat.confidence = .upper_bound;
    }
    return stat;
}

const P = agg.NUM_PARTS;
const Partition = struct {
    lock: std.atomic.Value(bool) align(64) = .init(false),
    set: agg.DistinctSet = .{},
};
const Shared = struct {
    gpa: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    refs: []const Ref,
    col: usize,
    parts: *[P]Partition,
    cursor: std.atomic.Value(usize) = .init(0),
    failed: std.atomic.Value(bool) = .init(false),
    page_limit: usize,
};
const Worker = struct {
    shared: *Shared,
    oom: bool = false,
    fn run(w: *Worker) void {
        w.runErr() catch |err| {
            w.oom = err == error.OutOfMemory;
            w.shared.failed.store(true, .monotonic);
        };
    }
    fn runErr(w: *Worker) !void {
        const s = w.shared;
        var scratch = std.heap.ArenaAllocator.init(s.gpa);
        defer scratch.deinit();
        while (!s.failed.load(.monotonic)) {
            const i = s.cursor.fetchAdd(1, .monotonic);
            if (i >= s.refs.len) return;
            _ = scratch.reset(.retain_capacity);
            const a = scratch.allocator();
            const ref = s.refs[i];
            const cm = s.metas[ref.file].row_groups.items[ref.rg].columns.items[s.col].meta_data.?;
            const bytes = dictSlice(s.files[ref.file], cm) orelse return error.BadDictionary;
            const h = header(bytes) orelse return error.BadDictionary;
            const n: usize = @intCast(h.dictionary_page_header.?.num_values);
            // Payload plus value views and scatter ids are bounded before any decompression/allocation.
            if (@as(u64, @intCast(h.uncompressed_page_size)) +| (@as(u64, n) *| 32) > s.page_limit)
                return error.ProbeBudget;
            var reader = page.PageReader.init(bytes, cm.codec, a);
            const pg = (try reader.next()) orelse return error.BadDictionary;
            if (cm.type != .INT32 and cm.type != .INT64 and cm.type != .BYTE_ARRAY) return error.UnsupportedDictionary;
            const ints: []u64 = if (cm.type != .BYTE_ARRAY) try a.alloc(u64, n) else &.{};
            const strs: [][]const u8 = if (cm.type == .BYTE_ARRAY) try a.alloc([]const u8, n) else &.{};
            if (cm.type == .INT32) {
                const tmp = try a.alloc(i32, n);
                var d = plain.Decoder(i32).init(pg.bytes);
                if (try d.decode(tmp) != n) return error.BadDictionary;
                for (tmp, ints) |v, *out| out.* = @bitCast(@as(i64, v));
            } else if (cm.type == .INT64) {
                var d = plain.Decoder(i64).init(pg.bytes);
                if (try d.decode(@ptrCast(ints)) != n) return error.BadDictionary;
            } else {
                var d = plain.ByteArrayDecoder.init(pg.bytes);
                if (try d.decode(strs) != n) return error.BadDictionary;
            }
            const routes = try a.alloc(u8, n);
            var counts: [P]usize = @splat(0);
            for (routes, 0..) |*r, j| {
                const hash = if (ints.len > 0)
                    std.hash.Wyhash.hash(0, std.mem.asBytes(&ints[j]))
                else
                    std.hash.Wyhash.hash(0, strs[j]);
                r.* = @intCast((hash >> 32) % P);
                counts[r.*] += 1;
            }
            var starts: [P + 1]usize = undefined;
            starts[0] = 0;
            for (counts, 0..) |c, p| starts[p + 1] = starts[p] + c;
            var at = starts;
            const ids = try a.alloc(u32, n);
            for (routes, 0..) |r, j| {
                ids[at[r]] = @intCast(j);
                at[r] += 1;
            }
            // Workers that walked the partitions in one order queued behind each other on every lock. Start each
            // dictionary at its own partition and pass over a held lock to come back to it.
            var done: [P]bool = @splat(false);
            var left: usize = P;
            var pi: usize = i % P;
            while (left > 0) : (pi = (pi + 1) % P) {
                if (done[pi]) continue;
                const p = &s.parts[pi];
                if (p.lock.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
                    std.atomic.spinLoopHint();
                    continue;
                }
                defer p.lock.store(false, .release);
                done[pi] = true;
                left -= 1;
                for (ids[starts[pi]..starts[pi + 1]]) |j| {
                    if (ints.len > 0) try p.set.insertU64(s.gpa, ints[j]) else try p.set.insertBytes(s.gpa, strs[j]);
                }
            }
        }
    }
};

fn unionBound(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    refs: []const Ref,
    col: usize,
    workers: usize,
    max_memory: usize,
) !?u64 {
    // Binding permits schema evolution across inputs. Raw dictionary images only union safely when their
    // physical/logical interpretation matches; otherwise the sum of per-RG bounds remains valid.
    var page_need: u64 = 1;
    if (refs.len > 0) {
        const first = refs[0];
        const cm0 = metas[first.file].row_groups.items[first.rg].columns.items[col].meta_data.?;
        const se0 = metas[first.file].getColumnSchema(cm0.path_in_schema.items) orelse return null;
        for (refs) |ref| {
            const cm = metas[ref.file].row_groups.items[ref.rg].columns.items[col].meta_data.?;
            const se = metas[ref.file].getColumnSchema(cm.path_in_schema.items) orelse return null;
            const bytes = dictSlice(files[ref.file], cm) orelse return null;
            const h = header(bytes) orelse return null;
            page_need = @max(page_need, @as(u64, @intCast(h.uncompressed_page_size)) +|
                (@as(u64, @intCast(h.dictionary_page_header.?.num_values)) *| 32));
            if (cm.type != cm0.type or se.converted_type != se0.converted_type or
                !std.meta.eql(se.logical_type, se0.logical_type) or se.scale != se0.scale or
                se.precision != se0.precision or se.type_length != se0.type_length) return null;
        }
    }
    const scratch_budget = max_memory / 8;
    if (page_need > @min(16 * 1024 * 1024, scratch_budget)) return null;
    // Lower probe parallelism when a dictionary is large; a small query budget must not unnecessarily lose a
    // useful bound just because the scan itself uses more workers.
    const by_scratch: usize = @intCast(scratch_budget / page_need);
    const n_workers = @max(1, @min(@min(@min(workers, refs.len), 16), by_scratch));
    var budget: agg.SharedBudget = .{ .limit = max_memory / 16, .block = 4096 };
    var parts: [P]Partition = undefined;
    for (&parts) |*p| p.* = .{ .set = .{ .shared = &budget } };
    defer for (&parts) |*p| p.set.deinit(gpa);
    var shared: Shared = .{
        .gpa = gpa,
        .files = files,
        .metas = metas,
        .refs = refs,
        .col = col,
        .parts = &parts,
        .page_limit = @min(16 * 1024 * 1024, max_memory / 8 / n_workers),
    };
    const ws = try arena.alloc(Worker, n_workers);
    for (ws) |*w| w.* = .{ .shared = &shared };
    if (ws.len == 1 or !spawn.runTree(Worker, ws, .{ .stack_size = 1 << 20 }, Worker.run)) Worker.run(&ws[0]);
    for (ws) |w| if (w.oom) return error.OutOfMemory;
    if (shared.failed.load(.monotonic)) return null;
    var count: u64 = 0;
    for (parts) |p| count +|= p.set.count();
    return count;
}

test "ndv: repeated dictionaries tighten an upper bound, including nulls and composites" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const meta = try metadata.open(arena, bytes);
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    const keys = [_]expr.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    const headers = try collect(a, arena, &files, &.{meta}, keys[0..1], 4, 512 << 20, false);
    try std.testing.expectEqual(.dictionary_headers, headers.source);
    try std.testing.expectEqual(headers.rows, headers.covered_rows);
    for ([_]usize{ 1, 4 }) |workers| {
        spawn.injectFailureAfter(0);
        defer spawn.resetFailure();
        const unioned = try collect(a, arena, &files, &.{meta}, keys[0..1], workers, 512 << 20, true);
        try std.testing.expectEqual(.dictionary_union, unioned.source);
        try std.testing.expectEqual(@as(u64, 9295), unioned.hi);
        try std.testing.expectEqual(@as(?u64, 9295), unioned.estimate);
        try std.testing.expect(unioned.hi < headers.hi);
        try std.testing.expectEqual(@as(u64, 0), unioned.lo); // Unused dictionary entries cannot prove a lower bound.
        if (workers > 1) try std.testing.expect(spawn.failuresInjected() > 0);
    }
    const composite = try collect(a, arena, &files, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expect(composite.hi >= 9295 and composite.hi <= composite.rows);
    try std.testing.expect(composite.estimate == null);
}

test "ndv: partial coverage, missing null counts, unavailable pages and probe budgets stay conservative" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    var meta = try metadata.open(arena, bytes);
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    const local = [_]File{.{ .bytes = bytes, .local = true }};
    const budgeted = try collect(a, arena, &local, &.{meta}, &keys, 4, 1024 * 1024, true);
    try std.testing.expect(budgeted.union_attempted and !budgeted.union_complete);
    const remote = [_]File{.{ .bytes = &.{}, .local = false }};
    const unavailable = try collect(a, arena, &remote, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expectEqual(.unknown, unavailable.source);
    try std.testing.expectEqual(unavailable.rows, unavailable.hi);
    try std.testing.expect(!unavailable.union_attempted);
    meta.row_groups.items[0].columns.items[1].meta_data.?.encoding_stats = null;
    const partial = try collect(a, arena, &local, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expect(!partial.union_attempted);
    try std.testing.expectEqual(partial.rows - 1250, partial.covered_rows);
    try std.testing.expect(partial.hi >= 9295);
    const cm = &meta.row_groups.items[1].columns.items[1].meta_data.?;
    cm.statistics.?.null_count = null;
    try std.testing.expectEqual(@as(u64, 1), nullBound(cm.*));
    cm.statistics.?.null_count = 0;
    try std.testing.expectEqual(@as(u64, 0), nullBound(cm.*));
    cm.encoding_stats = null;
    try std.testing.expect(!fullCoverage(cm));
}

test "ndv: allocation failures release dictionary scratch and the shared set" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    var meta = try metadata.open(state.allocator(), bytes);
    meta.row_groups.items = meta.row_groups.items[0..1];
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    try std.testing.checkAllAllocationFailures(a, struct {
        fn run(gpa: std.mem.Allocator, fs: []const File, ms: []const schema.FileMetaData) !void {
            // Arena growth depends on whether a backing allocation can resize in place. Force allocate/copy so
            // failure indices cover a deterministic sequence, while every owned byte still reaches the tracker.
            var backing = gpa;
            const mem: std.mem.Allocator = .{ .ptr = &backing, .vtable = &.{
                .alloc = NoResize.alloc,
                .resize = NoResize.resize,
                .remap = NoResize.remap,
                .free = NoResize.free,
            } };
            var scratch = std.heap.ArenaAllocator.init(mem);
            defer scratch.deinit();
            const keys = [_]expr.Expr{.{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .BYTE_ARRAY,
                .expr_type = .str,
            } }};
            const stat = try collect(mem, scratch.allocator(), fs, ms, &keys, 1, 512 << 20, true);
            try std.testing.expect(stat.union_complete and stat.hi > 1);
        }
    }.run, .{ &files, @as([]const schema.FileMetaData, &.{meta}) });
}

const NoResize = struct {
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const a: *std.mem.Allocator = @ptrCast(@alignCast(ctx));
        return a.rawAlloc(len, alignment, ra);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const a: *std.mem.Allocator = @ptrCast(@alignCast(ctx));
        a.rawFree(bytes, alignment, ra);
    }
};

test "ndv: schema evolution cannot union incompatible raw dictionary images" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const first = try metadata.open(arena, bytes);
    var second = try metadata.open(arena, bytes);
    for (second.row_groups.items) |*rg| rg.columns.items[1].meta_data.?.type = .INT32;
    second.schema.items[2].type = .INT32;
    const files = [_]File{ .{ .bytes = bytes, .local = true }, .{ .bytes = bytes, .local = true } };
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    const stat = try collect(a, arena, &files, &.{ first, second }, &keys, 4, 512 << 20, true);
    try std.testing.expect(stat.union_attempted and !stat.union_complete);
    try std.testing.expectEqual(.dictionary_headers, stat.source);
    try std.testing.expect(stat.hi >= 9295);
}

test "ndv: dictionary headers accept the execution reader's legacy PLAIN_DICTIONARY label" {
    const a = std.testing.allocator;
    for ([_]schema.Encoding{ .PLAIN, .PLAIN_DICTIONARY }) |encoding| {
        var writer = thrift.Writer.init(a);
        defer writer.deinit();
        const h: schema.PageHeader = .{
            .type = .DICTIONARY_PAGE,
            .crc = null,
            .uncompressed_page_size = 0,
            .compressed_page_size = 0,
            .data_page_header = null,
            .data_page_header_v2 = null,
            .dictionary_page_header = .{ .num_values = 0, .encoding = encoding, .is_sorted = null },
        };
        try h.write(&writer);
        try std.testing.expect(header(writer.bytes()) != null);
    }
}

test "ndv: an empty input has a zero upper bound" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const stat = try collect(std.testing.allocator, arena.allocator(), &.{}, &.{}, &keys, 1, 512 << 20, true);
    try std.testing.expectEqual(@as(u64, 0), stat.hi);
    try std.testing.expectEqual(.upper_bound, stat.confidence);
}
