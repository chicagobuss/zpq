//! CSV writes through the write window: bytes that do not depend on `-j` or the chunking, a late type change that
//! restarts to the file a whole-file sample writes, and refusals that say where.

const std = @import("std");
const csv_write = @import("csv_write.zig");
const engine_types = @import("types.zig");
const metadata = @import("../core/parquet/metadata.zig");
const schema = @import("../core/schema.zig");
const local_reader = @import("../io/local_reader.zig");

const testing = std.testing;
const Context = engine_types.Context;

fn tmpPath(buf: []u8, tmp: *const std.testing.TmpDir, name: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

fn writeFile(path: []const u8, data: []const u8) !void {
    try std.Io.Dir.cwd().writeFile(testing.io, .{ .sub_path = path, .data = data });
}

/// A CSV of `rows` records exercising every type, NULLs, quotes, doubled quotes, embedded newlines and CRLF.
fn sampleCsv(a: std.mem.Allocator, rows: usize, late: ?[]const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(a, "id,amt,name,day,ts,flag,zip\r\n");
    for (0..rows) |i| {
        const amt = if (late != null and i == rows - 7)
            late.?
        else
            try std.fmt.allocPrint(a, "{d}.{d:0>2}", .{ i % 9973, i % 100 });
        const name = switch (i % 5) {
            0 => "\"\"",
            1 => "",
            2 => try std.fmt.allocPrint(a, "\"n{d}, \"\"q\"\"\r\nl2\"", .{i}),
            else => try std.fmt.allocPrint(a, "plain{d}", .{i % 37}),
        };
        try out.print(a, "{d},{s},{s},2024-{d:0>2}-{d:0>2},2024-01-01 00:00:{d:0>2}.{d:0>6},{s},{d:0>5}\r\n", .{
            i,
            amt,
            name,
            i % 12 + 1,
            i % 28 + 1,
            i % 60,
            i % 1000000,
            if (i % 3 == 0) "true" else if (i % 3 == 1) "false" else "",
            i % 100000,
        });
    }
    return out.toOwnedSlice(a);
}

fn runCsv(
    path: []const u8,
    out: []const u8,
    j: usize,
    opts: csv_write.Options,
    failure: *csv_write.Failure,
) !engine_types.WriteResult {
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var report: csv_write.Report = .{};
    defer failure.* = report.failure;
    return csv_write.run(ctx, .{ .inputs = &.{path}, .parallelism = j }, path, out, opts, &report, arena.allocator());
}

test "csv write: the same bytes at every -j and chunk size, typed exactly, NULL apart from the empty string" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "in.csv");
    const out = try tmpPath(&b2, &tmp, "out.parquet");
    const rows = 30_000;
    try writeFile(in, try sampleCsv(a, rows, null));

    var first: ?[]u8 = null;
    defer if (first) |f| testing.allocator.free(f);
    for ([_]usize{ 1, 2, 3, 8 }) |j| for ([_]?usize{ null, 1 << 10, 7777, 64 << 10 }) |chunk| {
        var failure: csv_write.Failure = .{};
        const r = try runCsv(in, out, j, .{ .chunk_bytes = chunk }, &failure);
        try testing.expectEqual(@as(i64, rows), r.rows_matched);
        const bytes = try metadata.readFileSlice(out, testing.allocator);
        if (first) |f| {
            defer testing.allocator.free(bytes);
            try testing.expectEqualSlices(u8, f, bytes);
        } else first = bytes;
    };

    const meta = try metadata.open(a, first.?);
    try testing.expectEqual(@as(i64, rows), meta.num_rows);
    const leaves = meta.schema.items[1..];
    const want = [_]struct { []const u8, schema.Type }{
        .{ "id", .INT64 }, .{ "amt", .INT64 },    .{ "name", .BYTE_ARRAY }, .{ "day", .INT32 },
        .{ "ts", .INT64 }, .{ "flag", .BOOLEAN }, .{ "zip", .BYTE_ARRAY },
    };
    for (leaves, want) |e, w| {
        try testing.expectEqualStrings(w[0], e.name);
        try testing.expectEqual(w[1], e.type.?);
    }
    // DECIMAL precision is the data's (9972.99), not the physical capacity.
    try testing.expectEqual(@as(?i32, 6), leaves[1].precision);
    try testing.expectEqual(@as(?i32, 2), leaves[1].scale);
    // `00042` stays text; `ts` keeps microseconds.
    try testing.expectEqual(schema.LogicalType{ .STRING = .{} }, leaves[6].logical_type.?);
    try testing.expect(leaves[4].logical_type.?.TIMESTAMP.unit == .MICROS);
    // Every bound is in TYPE_DEFINED_ORDER, and `id`'s bounds are its first and last value.
    try testing.expectEqual(@as(usize, 7), meta.column_orders.?.items.len);
    var min: i64 = std.math.maxInt(i64);
    var max: i64 = std.math.minInt(i64);
    var name_nulls: i64 = 0;
    for (meta.row_groups.items) |rg| {
        const st = rg.columns.items[0].meta_data.?.statistics.?;
        min = @min(min, std.mem.readInt(i64, st.min_value.?[0..8], .little));
        max = @max(max, std.mem.readInt(i64, st.max_value.?[0..8], .little));
        name_nulls += rg.columns.items[2].meta_data.?.statistics.?.null_count.?;
    }
    try testing.expectEqual(@as(i64, 0), min);
    try testing.expectEqual(@as(i64, rows - 1), max);
    // Only the unquoted empty `name` (one row in five) is NULL; `""` is the empty string.
    try testing.expectEqual(@as(i64, rows / 5), name_nulls);
}

test "csv write: a late value widens its column and the write restarts, to the bytes a whole-file sample writes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    var b3: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "late.csv");
    const out = try tmpPath(&b2, &tmp, "late.parquet");
    const whole = try tmpPath(&b3, &tmp, "whole.parquet");
    try writeFile(in, try sampleCsv(a, 20_000, "12345678901234567.125"));

    var failure: csv_write.Failure = .{};
    _ = try runCsv(in, out, 4, .{ .sample_bytes = 4096, .chunk_bytes = 16 << 10 }, &failure);
    _ = try runCsv(in, whole, 4, .{ .sample_bytes = 64 << 20 }, &failure);
    const x = try metadata.readFileSlice(out, testing.allocator);
    defer testing.allocator.free(x);
    const y = try metadata.readFileSlice(whole, testing.allocator);
    defer testing.allocator.free(y);
    try testing.expectEqualSlices(u8, y, x);
    const meta = try metadata.open(a, x);
    const amt = meta.schema.items[2];
    try testing.expectEqual(schema.Type.FIXED_LEN_BYTE_ARRAY, amt.type.?);
    try testing.expectEqual(@as(?i32, 3), amt.scale);
    try testing.expectEqual(@as(?i32, 20), amt.precision);

    // Strict: refused where the value is.
    const strict: csv_write.Options = .{ .sample_bytes = 4096, .strict = true };
    try testing.expectError(error.CsvTypeChanged, runCsv(in, out, 4, strict, &failure));
    try testing.expectEqual(csv_write.Failure.Kind.type_changed, failure.kind);
    try testing.expectEqual(@as(?usize, 1), failure.column);
    try testing.expectEqualStrings("12345678901234567.125", failure.valueText());
    // The header is line 1 and every record before it spans two lines in one of five.
    const record = 20_000 - 7;
    try testing.expectEqual(@as(u64, 2 + record + (record + 2) / 5), failure.line);
}

test "csv write: malformed input is refused at its line, byte and column" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "bad.csv");
    const out = try tmpPath(&b2, &tmp, "bad.parquet");
    const cases = [_]struct { csv: []const u8, err: anyerror, line: u64, offset: u64, column: ?usize }{
        .{ .csv = "a,b\n1,x\n2,ab\"c\n", .err = error.CsvMalformed, .line = 3, .offset = 10, .column = 1 },
        .{ .csv = "a,b\n1,x\n2,\"open\n3,y\n", .err = error.CsvMalformed, .line = 3, .offset = 10, .column = 1 },
        .{ .csv = "a,b,c\n1,2,3\n4,5\n7,8,9\n", .err = error.CsvFieldCount, .line = 3, .offset = 12, .column = null },
        .{ .csv = "a,b,c\n1,2,3\n4,5,6,7\n", .err = error.CsvFieldCount, .line = 3, .offset = 12, .column = null },
    };
    for (cases) |c| for ([_]?usize{ null, 3 }) |chunk| {
        try writeFile(in, c.csv);
        var failure: csv_write.Failure = .{};
        try testing.expectError(c.err, runCsv(in, out, 2, .{ .chunk_bytes = chunk }, &failure));
        try testing.expectEqual(c.line, failure.line);
        try testing.expectEqual(c.offset, failure.offset);
        try testing.expectEqual(c.column, failure.column);
        // Nothing that looks like output is left behind.
        try testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(testing.io, out, .{}));
    };
}

test "csv write: the local reader writes the mapping's bytes, records longer than a chunk's read included" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "read.csv");
    const out = try tmpPath(&b2, &tmp, "read.parquet");
    // Records of every kind, then one whose quoted field spans many chunks and far more than a read's overhang.
    var csv: std.ArrayList(u8) = .empty;
    try csv.appendSlice(a, try sampleCsv(a, 8_000, null));
    try csv.appendSlice(a, "8000,1.00,\"");
    for (0..300_000) |i| try csv.append(a, "ab,\n\r x\"\""[i % 10]);
    try csv.appendSlice(a, "\",2024-01-01,2024-01-01 00:00:00.000000,true,00001\r\n");
    try csv.appendSlice(a, (try sampleCsv(a, 4_000, null))[30..]); // without its header
    try writeFile(in, csv.items);

    var first: ?[]u8 = null;
    defer if (first) |f| testing.allocator.free(f);
    for ([_]local_reader.Policy.Profile{ .mmap, .pread, .uring }) |profile| for ([_]?usize{ null, 16 << 10 }) |chunk| {
        var failure: csv_write.Failure = .{};
        const opts: csv_write.Options = .{ .io_profile = profile, .chunk_bytes = chunk, .overhang = 4096 };
        const r = try runCsv(in, out, 4, opts, &failure);
        try testing.expectEqual(@as(i64, 12_001), r.rows_matched);
        const bytes = try metadata.readFileSlice(out, testing.allocator);
        if (first) |f| {
            defer testing.allocator.free(bytes);
            try testing.expectEqualSlices(u8, f, bytes);
        } else first = bytes;
    };
    // A refusal says the same through the reader.
    try writeFile(in, "a,b\n1,x\n2,ab\"c\n3,y\n");
    for ([_]local_reader.Policy.Profile{ .mmap, .pread }) |profile| {
        var failure: csv_write.Failure = .{};
        const opts: csv_write.Options = .{ .io_profile = profile, .chunk_bytes = 4, .overhang = 4 };
        try testing.expectError(error.CsvMalformed, runCsv(in, out, 2, opts, &failure));
        try testing.expectEqual(@as(u64, 3), failure.line);
        try testing.expectEqual(@as(u64, 10), failure.offset);
    }
}

test "csv write: a small budget cuts smaller row groups and writes the same rows, through the reader or the mapping" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    var b3: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "budget.csv");
    const big = try tmpPath(&b2, &tmp, "big.parquet");
    const small = try tmpPath(&b3, &tmp, "small.parquet");
    const rows = 60_000;
    try writeFile(in, try sampleCsv(a, rows, null));
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    var report: csv_write.Report = .{};
    _ = try csv_write.run(ctx, .{ .inputs = &.{in}, .parallelism = 4 }, in, big, .{}, &report, a);
    for ([_]local_reader.Policy.Profile{ .mmap, .pread }) |profile| {
        const budget: usize = 24 << 20;
        const args: engine_types.QueryArgs = .{ .inputs = &.{in}, .parallelism = 4, .max_memory = budget };
        const r = try csv_write.run(ctx, args, in, small, .{ .io_profile = profile }, &report, a);
        try testing.expectEqual(@as(i64, rows), r.rows_matched);
        const x = try metadata.open(a, try metadata.readFileSlice(big, a));
        const y = try metadata.open(a, try metadata.readFileSlice(small, a));
        // Row groups of at most a sixteenth of the budget, so more of them; the same rows, NULLs and bounds.
        try testing.expect(y.row_groups.items.len > x.row_groups.items.len);
        for (0..x.schema.items.len - 1) |c| {
            var nx: i64 = 0;
            var ny: i64 = 0;
            for (x.row_groups.items) |rg| nx += rg.columns.items[c].meta_data.?.statistics.?.null_count.?;
            for (y.row_groups.items) |rg| ny += rg.columns.items[c].meta_data.?.statistics.?.null_count.?;
            try testing.expectEqual(nx, ny);
        }
        const first = y.row_groups.items[0].columns.items[0].meta_data.?.statistics.?;
        try testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, first.min_value.?[0..8], .little));
        const last = y.row_groups.items[y.row_groups.items.len - 1].columns.items[0].meta_data.?.statistics.?;
        try testing.expectEqual(@as(i64, rows - 1), std.mem.readInt(i64, last.max_value.?[0..8], .little));
    }
}

/// The report's allocator, with a fault on call `at`: `touch` moves the input's mtime (its bytes unchanged), `fail`
/// refuses the allocation.
const ReportHook = struct {
    base: std.mem.Allocator,
    path: [:0]const u8,
    at: usize,
    mode: enum { touch, fail },
    calls: usize = 0,

    fn allocator(h: *ReportHook) std.mem.Allocator {
        return .{ .ptr = h, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = std.mem.Allocator.noFree } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const h: *ReportHook = @ptrCast(@alignCast(ctx));
        h.calls += 1;
        if (h.calls == h.at) switch (h.mode) {
            .fail => return null,
            .touch => {
                const later = [2]std.os.linux.timespec{ .{ .sec = 1, .nsec = 0 }, .{ .sec = 1, .nsec = 0 } };
                _ = std.os.linux.utimensat(std.os.linux.AT.FDCWD, h.path.ptr, &later, 0);
            },
        };
        return h.base.rawAlloc(len, alignment, ret);
    }
};

test "csv write: a failure after the write leaves no output: an input changed under it, or a report it cannot hold" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try a.dupeSentinel(u8, try tmpPath(&b1, &tmp, "pub.csv"), 0);
    const out = try tmpPath(&b2, &tmp, "pub.parquet");
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    const gone = struct {
        fn check(path: []const u8) !void {
            try testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(testing.io, path, .{}));
        }
    }.check;

    // The reviewer's reproducer: the input's mtime moves after it was planned, its bytes the same.
    try writeFile(in, "id,v\n1,2\n3,4\n");
    var hook: ReportHook = .{ .base = a, .path = in, .at = 1, .mode = .touch };
    var report: csv_write.Report = .{};
    try testing.expectError(error.InputChanged, csv_write.run(ctx, .{ .inputs = &.{in} }, in, out, .{}, &report, hook.allocator()));
    try gone(out);

    // Every report allocation refused in turn: each failure leaves nothing behind, and the write that needs no
    // more of them succeeds.
    var at: usize = 1;
    while (true) : (at += 1) {
        try writeFile(in, "id,v\n1,2\n3,4\n");
        var fail: ReportHook = .{ .base = a, .path = in, .at = at, .mode = .fail };
        var r: csv_write.Report = .{};
        if (csv_write.run(ctx, .{ .inputs = &.{in} }, in, out, .{}, &r, fail.allocator())) |res| {
            try testing.expectEqual(@as(i64, 2), res.rows_matched);
            try testing.expect(at > 1);
            break;
        } else |e| {
            try testing.expectEqual(error.OutOfMemory, e);
            try gone(out);
        }
    }
}
