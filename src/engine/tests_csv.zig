//! CSV writes through the write window: bytes that do not depend on `-j` or the chunking, a late type change that
//! restarts to the file a whole-file sample writes, and refusals that say where.

const std = @import("std");
const csv_write = @import("csv_write.zig");
const engine_types = @import("types.zig");
const metadata = @import("../core/parquet/metadata.zig");
const schema = @import("../core/schema.zig");
const local_reader = @import("../io/local_reader.zig");

const Diag = @import("../core/diag.zig").Diag;
const runQuery = @import("aggregate.zig").runQuery;
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
        // A record with the wrong number of fields: the first field missing, or the first past the header's.
        .{ .csv = "a,b,c\n1,2,3\n4,5\n7,8,9\n", .err = error.CsvFieldCount, .line = 3, .offset = 12, .column = 2 },
        .{ .csv = "a,b,c\n1,2,3\n4,5,6,7\n", .err = error.CsvFieldCount, .line = 3, .offset = 12, .column = 3 },
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

test "csv write: a chunk whose read failed stops the chunks after it, never strands them at its quote count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // The reviewer's reproducer: chunk 0's read fails, chunk 1's bytes are in; producer 0 runs, then producer 1.
    const bytes = "id,v\n1,2\n3,4\n5,6\n7,8\n";
    var failure: csv_write.Failure = .{};
    const p = try csv_write.plan(a, bytes, .{}, &failure);
    var source = try csv_write.Source.init(a, &p, 6);
    try testing.expect(source.count >= 2);
    source.classify = true;
    source.seen = try a.alloc(csv_write.Type, source.count * p.names.len);
    const Gate = struct {
        fn wait(_: *anyopaque, chunk: usize) anyerror!void {
            if (chunk == 0) return error.LocalReadFailed;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {}
    };
    var dummy: u8 = 0;
    source.gate = .{ .ctx = &dummy, .wait = Gate.wait, .done = Gate.done };
    try testing.expectError(error.LocalReadFailed, source.produce(testing.io, null, 0));
    try testing.expectError(error.LocalReadFailed, source.produce(testing.io, null, 1));

    // Through the whole write: one producer fails mid-file, through the reader or the mapping, at any -j; the write
    // stops with that error, every reader unit goes back, and nothing is left behind.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "fault.csv");
    const out = try tmpPath(&b2, &tmp, "fault.parquet");
    try writeFile(in, try sampleCsv(a, 20_000, null));
    for ([_]?local_reader.Policy.Profile{ null, .pread }) |profile| for ([_]usize{ 1, 3, 8 }) |j| {
        var f: csv_write.Failure = .{};
        try testing.expectError(error.LocalReadFailed, runCsv(in, out, j, .{
            .chunk_bytes = 16 << 10,
            .io_profile = profile,
            .fault_chunk = 7,
        }, &f));
        try testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(testing.io, out, .{}));
    };
}

test "csv query: inputs are told apart by content, then by name, and a CSV is written whole or refused" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    var b3: [128]u8 = undefined;
    var b4: [128]u8 = undefined;
    const csv = try tmpPath(&b1, &tmp, "data.CSV");
    const bin = try tmpPath(&b2, &tmp, "data.bin");
    const out = try tmpPath(&b3, &tmp, "data.parquet");
    const disguised = try tmpPath(&b4, &tmp, "copy.csv");
    try writeFile(csv, "a,b\n1,x\n2,ab\"c\n");
    try writeFile(bin, "a,b\n1,x\n");
    try testing.expectEqual(csv_write.InputFormat.csv, try csv_write.inputFormat(csv, .auto));
    // Neither Parquet nor named as CSV: the Parquet reader's to refuse, as it refuses any file that is not Parquet.
    try testing.expectEqual(csv_write.InputFormat.parquet, try csv_write.inputFormat(bin, .auto));
    try testing.expectEqual(csv_write.InputFormat.csv, try csv_write.inputFormat(bin, .csv));
    try testing.expectEqual(csv_write.InputFormat.parquet, try csv_write.inputFormat("s3://b/k.csv", .auto));

    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    try testing.expectError(error.NotParquet, runQuery(ctx, .{ .inputs = &.{bin}, .aggregate = "count(*)" }));
    // A refusal names its place: line, byte and the column by its header name.
    var diag: Diag = .{};
    try testing.expectError(error.CsvMalformed, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .diag = &diag }));
    try testing.expectEqual(@as(u64, 3), diag.detail.csv.line);
    try testing.expectEqual(@as(u64, 10), diag.detail.csv.byte);
    try testing.expectEqualStrings("b", diag.column.get());

    try writeFile(csv, "a,b\n1,x\n2,y\n");
    var report: csv_write.Report = .{};
    const r = try runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .csv_report = &report, .csv_report_alloc = a });
    try testing.expectEqual(@as(i64, 2), r.write.rows_matched);
    try testing.expectEqualStrings("b", report.names[1]);
    // Anything but a whole conversion reads the Parquet it writes.
    try testing.expectError(error.CsvQueryNotSupported, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .filter = "a > 1" }));
    try testing.expectError(error.CsvQueryNotSupported, runQuery(ctx, .{ .inputs = &.{csv}, .aggregate = "count(*)" }));
    // The Parquet it wrote, renamed .csv, is still Parquet: content first.
    try std.Io.Dir.cwd().copyFile(out, std.Io.Dir.cwd(), disguised, testing.io, .{});
    try testing.expectEqual(csv_write.InputFormat.parquet, try csv_write.inputFormat(disguised, .auto));
}

test "csv query: a column projection writes the columns named, in order, and frames the rest without typing them" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const csv = try tmpPath(&b1, &tmp, "p.csv");
    const out = try tmpPath(&b2, &tmp, "p.parquet");
    // `c` turns to text late; written without it, nothing widens and nothing restarts.
    var text: std.ArrayList(u8) = .empty;
    try text.appendSlice(a, "a,b,c\n");
    for (0..5000) |i| try text.print(a, "{d},x{d},{d}\n", .{ i, i, i });
    try text.appendSlice(a, "5000,x5000,oops\n");
    try writeFile(csv, text.items);
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };

    const sql_cols = [_]@import("../core/expr/ast.zig").SelectColumn{ .{ .expr = "b" }, .{ .expr = "\"a\"" } };
    for ([_]engine_types.QueryArgs{
        .{ .inputs = &.{csv}, .output = out, .columns = &.{ "b", "a" } },
        .{ .inputs = &.{csv}, .output = out, .select = "b, \"a\"", .select_cols = &sql_cols },
    }) |args| {
        var report: csv_write.Report = .{};
        var with = args;
        with.csv_report = &report;
        with.csv_report_alloc = a;
        with.csv = .{ .sample_bytes = 1024 };
        const r = try runQuery(ctx, with);
        try testing.expectEqual(@as(i64, 5001), r.write.rows_matched);
        try testing.expectEqual(@as(usize, 0), report.restarts);
        const meta = try metadata.open(a, try metadata.readFileSlice(out, a));
        const leaves = meta.schema.items[1..];
        try testing.expectEqual(@as(usize, 2), leaves.len);
        try testing.expectEqualStrings("b", leaves[0].name);
        try testing.expectEqualStrings("a", leaves[1].name);
        try testing.expectEqual(schema.Type.INT64, leaves[1].type.?);
    }
    // `SELECT *` is the whole file.
    {
        var report: csv_write.Report = .{};
        _ = try runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .select_cols = &.{}, .csv = .{ .sample_bytes = 1024 }, .csv_report = &report, .csv_report_alloc = a });
        try testing.expectEqual(@as(usize, 3), report.names.len);
        try testing.expectEqual(@as(usize, 1), report.restarts);
    }
    // An expression, a rename, ORDER BY or LIMIT is a query over the CSV: refused by name.
    const expr = [_]@import("../core/expr/ast.zig").SelectColumn{.{ .expr = "a + 1" }};
    const renamed = [_]@import("../core/expr/ast.zig").SelectColumn{.{ .expr = "a", .alias = "z" }};
    try testing.expectError(error.CsvQueryNotSupported, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .select = "a + 1", .select_cols = &expr }));
    try testing.expectError(error.CsvQueryNotSupported, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .select = "a AS z", .select_cols = &renamed }));
    try testing.expectError(error.CsvQueryNotSupported, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .result_order = .{ .limit = 3 } }));
    // A column the file does not have is named; one asked for twice is refused.
    var diag: Diag = .{};
    try testing.expectError(error.UnknownColumn, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .columns = &.{"nope"}, .diag = &diag }));
    try testing.expectEqualStrings("nope", diag.column.get());
    try testing.expectError(error.CsvDuplicateColumn, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .columns = &.{ "a", "a" } }));
    // A field not written is still framed: a stray quote there is refused, at its own column.
    try writeFile(csv, "a,b,c\n1,x,1\n2,y,ab\"c\n");
    diag = .{};
    try testing.expectError(error.CsvMalformed, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .columns = &.{"a"}, .diag = &diag }));
    try testing.expectEqualStrings("c", diag.column.get());
    try testing.expectEqual(@as(u64, 3), diag.detail.csv.line);
}

test "csv query: SQL over a CSV is a column list or a refusal, never a dropped clause" {
    const sql = @import("../core/expr/sql_parser.zig");
    if (comptime !@hasDecl(sql, "parseSqlPlan")) return;
    const syntax_plan = @import("syntax_plan.zig");
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const csv = try tmpPath(&b1, &tmp, "s.csv");
    const out = try tmpPath(&b2, &tmp, "s.parquet");
    try writeFile(csv, "a,b,c\n1,x,1.5\n2,y,2.5\n");
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    const Case = struct { text: []const u8, names: ?[]const []const u8 };
    for ([_]Case{
        .{ .text = "SELECT * FROM 's.csv'", .names = &.{ "a", "b", "c" } },
        .{ .text = "SELECT c, a FROM 's.csv'", .names = &.{ "c", "a" } },
        .{ .text = "SELECT \"b\" AS b FROM 's.csv'", .names = &.{"b"} },
        .{ .text = "SELECT * FROM 's.csv' WHERE a > 1", .names = null },
        .{ .text = "SELECT a FROM 's.csv' ORDER BY a", .names = null },
        .{ .text = "SELECT a FROM 's.csv' LIMIT 1", .names = null },
        .{ .text = "SELECT a + 1 AS a FROM 's.csv'", .names = null },
        .{ .text = "SELECT a AS z FROM 's.csv'", .names = null },
        .{ .text = "SELECT count(*) FROM 's.csv'", .names = null },
    }) |case| {
        const parsed = try sql.parseSqlPlan(testing.allocator, case.text);
        defer parsed.deinit(testing.allocator);
        var report: csv_write.Report = .{};
        const args: engine_types.QueryArgs = .{
            .inputs = &.{csv},
            .output = out,
            .plan_source = syntax_plan.source(parsed.syntax.?),
            .select_cols = parsed.select_cols,
            .result_order = parsed.result_order,
            .csv_report = &report,
            .csv_report_alloc = a,
        };
        const want = case.names orelse {
            try testing.expectError(error.CsvQueryNotSupported, runQuery(ctx, args));
            continue;
        };
        const r = try runQuery(ctx, args);
        try testing.expectEqual(@as(i64, 2), r.write.rows_matched);
        try testing.expectEqual(want.len, report.names.len);
        for (want, report.names) |w, n| try testing.expectEqualStrings(w, n);
    }
}

test "csv query: a late value restarts into the caller's own output file, and refuses on one it cannot restart" {
    const local_fs = @import("../local_fs.zig");
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    var b3: [128]u8 = undefined;
    const csv = try tmpPath(&b1, &tmp, "late.csv");
    const by_path = try tmpPath(&b2, &tmp, "path.parquet");
    const by_fd = try tmpPath(&b3, &tmp, "fd.parquet");
    try writeFile(csv, try sampleCsv(a, 20_000, "n/a"));
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    const opts: csv_write.Options = .{ .sample_bytes = 4096, .chunk_bytes = 8 << 10 };

    var report: csv_write.Report = .{};
    _ = try runQuery(ctx, .{ .inputs = &.{csv}, .output = by_path, .csv = opts, .csv_report = &report, .csv_report_alloc = a });
    try testing.expectEqual(@as(usize, 1), report.restarts);

    // The CLI's -o: an unpublished file of its own, written through its descriptor.
    const fd = try local_fs.createFile(by_fd);
    defer local_fs.close(fd);
    report = .{};
    _ = try runQuery(ctx, .{
        .inputs = &.{csv},
        .output = by_fd,
        .output_fd = fd,
        .output_restartable = true,
        .csv = opts,
        .csv_report = &report,
        .csv_report_alloc = a,
    });
    try testing.expectEqual(@as(usize, 1), report.restarts);
    try testing.expectEqualSlices(u8, try metadata.readFileSlice(by_path, a), try metadata.readFileSlice(by_fd, a));

    // A descriptor it cannot restart (a pipe, stdout) may have passed bytes on: the change is refused where it is.
    try local_fs.truncateToStart(fd);
    var diag: Diag = .{};
    try testing.expectError(error.CsvTypeChanged, runQuery(ctx, .{
        .inputs = &.{csv},
        .output = by_fd,
        .output_fd = fd,
        .csv = opts,
        .diag = &diag,
    }));
    try testing.expectEqualStrings("amt", diag.column.get());
}

test "csv write: the report's DECIMAL precision is the data's, as the footer writes it, not the sample's" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "prec.csv");
    const out = try tmpPath(&b2, &tmp, "prec.parquet");
    // The sample sees 1.50; a later -10000.25 needs two more integer digits, which only the footer's precision shows.
    var text: std.ArrayList(u8) = .empty;
    try text.appendSlice(a, "id,v\n");
    for (0..4000) |i| try text.print(a, "{d},1.50\n", .{i});
    try text.appendSlice(a, "4000,-10000.25\n");
    try writeFile(in, text.items);
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    var report: csv_write.Report = .{};
    _ = try csv_write.run(ctx, .{ .inputs = &.{in} }, in, out, .{ .sample_bytes = 256 }, &report, a);
    try testing.expectEqual(@as(usize, 0), report.restarts);
    const meta = try metadata.open(a, try metadata.readFileSlice(out, a));
    try testing.expectEqual(@as(?i32, 7), meta.schema.items[2].precision);
    try testing.expectEqual(@as(u32, 7), report.types[1].precision());
    try testing.expectEqual(@as(u8, 2), report.types[1].scale);
}

test "csv write: a record with too many fields is counted to its own end, not into the next" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const csv = try tmpPath(&b1, &tmp, "long.csv");
    const out = try tmpPath(&b2, &tmp, "long.parquet");
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    for ([_]struct { []const u8, usize }{
        .{ "a,b,c\n1,2,3\n4,5,6,7\n7,8,9\n", 4 },
        .{ "a,b,c\n1,2,3\n4,5,6,7,8\n7,8,9\n", 5 },
        .{ "a,b,c\n1,2,3\n4,5,6,7", 4 },
    }) |c| {
        try writeFile(csv, c[0]);
        var diag: Diag = .{};
        try testing.expectError(error.CsvFieldCount, runQuery(ctx, .{ .inputs = &.{csv}, .output = out, .diag = &diag }));
        try testing.expectEqual(@as(usize, 3), diag.detail.csv.expected);
        try testing.expectEqual(c[1], diag.detail.csv.got);
        try testing.expectEqual(@as(u64, 3), diag.detail.csv.line);
        // The column is the first field past the header's (review P2-5).
        try testing.expect(diag.detail.csv.has_column);
        try testing.expectEqualStrings("column3", diag.column.get());
    }
}

test "csv write: a pinned DECIMAL keeps its declared precision, and a value wider than it is refused (review P2-7)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "pin.csv");
    const out = try tmpPath(&b2, &tmp, "pin.parquet");
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    const csv_types = @import("../core/csv/types.zig");
    // The reviewer's fixture: 99.9 does not fit DECIMAL(2,1), which once wrote DECIMAL(3,1) without a word.
    try writeFile(in, "v\n9.9\n99.9\n");
    const narrow: csv_write.Options = .{
        .header = true,
        .types = &.{.{ .column = "v", .type = csv_types.parseName("DECIMAL(2,1)").? }},
    };
    var report: csv_write.Report = .{};
    try testing.expectError(
        error.CsvPinnedType,
        csv_write.run(ctx, .{ .inputs = &.{in} }, in, out, narrow, &report, a),
    );
    const left = std.Io.Dir.cwd().access(testing.io, out, .{});
    try testing.expectError(error.FileNotFound, left);
    // Values that fit are written at the declared precision, though the data needs less.
    try writeFile(in, "v\n9.9\n-1.5\n");
    const wide: csv_write.Options = .{
        .header = true,
        .types = &.{.{ .column = "v", .type = csv_types.parseName("DECIMAL(9,1)").? }},
    };
    report = .{};
    _ = try csv_write.run(ctx, .{ .inputs = &.{in} }, in, out, wide, &report, a);
    const meta = try metadata.open(a, try metadata.readFileSlice(out, a));
    try testing.expectEqual(@as(?i32, 9), meta.schema.items[1].precision);
    try testing.expectEqual(@as(?i32, 1), meta.schema.items[1].scale);
    try testing.expectEqual(@as(u32, 9), report.types[0].precision());
}

test "csv write: strings parsed into dictionary codes write the bytes values write, at every chunk size" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    var b3: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "dict.csv");
    const plain = try tmpPath(&b2, &tmp, "plain.parquet");
    const coded = try tmpPath(&b3, &tmp, "coded.parquet");
    // Columns a dictionary takes (few values, some NULL, some escaped), one it gives up on early (every value new),
    // one that repeats within a chunk but not across the file, and one that turns from repeating to new mid-file.
    var text: std.ArrayList(u8) = .empty;
    try text.appendSlice(a, "id,low,nul,esc,high,mid,turn\n");
    for (0..40_000) |i| {
        const nul: []const u8 = if (i % 7 == 0) "" else if (i % 2 == 0) "x" else "yy";
        const esc: []const u8 = if (i % 3 == 0) "\"a\"\"b\"" else "\"c,d\"";
        try text.print(a, "{d},k{d},{s},{s},h{d},m{d},{s}{d}\n", .{
            i, i % 5, nul, esc, i * 7919, i % 3000, if (i < 20_000) "r" else "n", if (i < 20_000) i % 4 else i,
        });
    }
    try writeFile(in, text.items);
    var failure: csv_write.Failure = .{};
    _ = try runCsv(in, plain, 4, .{ .chunk_bytes = 16 << 10, .dictionaries = false }, &failure);
    const want = try metadata.readFileSlice(plain, testing.allocator);
    defer testing.allocator.free(want);
    for ([_]usize{ 16 << 10, 64 << 10, 1 << 20 }) |chunk| for ([_]usize{ 1, 4 }) |j| {
        _ = try runCsv(in, coded, j, .{ .chunk_bytes = chunk }, &failure);
        const got = try metadata.readFileSlice(coded, testing.allocator);
        defer testing.allocator.free(got);
        try testing.expectEqualSlices(u8, want, got);
    };
    // The low-cardinality columns were written as dictionaries.
    const meta = try metadata.open(a, want);
    for ([_]usize{ 1, 2, 3 }) |c| {
        const cm = meta.row_groups.items[0].columns.items[c].meta_data.?;
        try testing.expect(cm.dictionary_page_offset != null);
    }
}

test "csv write: per-chunk state is charged in full, and a budget too small for a write is refused by name (review P2-6)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var b1: [128]u8 = undefined;
    var b2: [128]u8 = undefined;
    const in = try tmpPath(&b1, &tmp, "many.csv");
    const out = try tmpPath(&b2, &tmp, "many.parquet");
    // 64 columns over 1-KiB chunks: thousands of chunks, each with state per column, which the ledger must hold.
    var text: std.ArrayList(u8) = .empty;
    for (0..64) |c| try text.print(a, "{s}c{d}", .{ if (c == 0) "" else ",", c });
    try text.append(a, '\n');
    for (0..8_000) |r| {
        for (0..64) |c| try text.print(a, "{s}{d}", .{ if (c == 0) "" else ",", r + c });
        try text.append(a, '\n');
    }
    try writeFile(in, text.items);
    // What the ledger is charged for covers every byte the source's per-chunk state takes.
    var failure: csv_write.Failure = .{};
    const p = try csv_write.plan(a, text.items, .{}, &failure);
    var counting = std.testing.FailingAllocator.init(a, .{});
    const source = try csv_write.Source.init(counting.allocator(), &p, 1 << 10);
    try testing.expect(source.count > 2_000);
    try testing.expect(counting.allocated_bytes <= source.metaBytes());
    const ctx: Context = .{ .gpa = testing.allocator, .env = .empty, .io = testing.io };
    var report: csv_write.Report = .{};
    const opts: csv_write.Options = .{ .chunk_bytes = 1 << 10 };
    // A budget too small for the write is a refusal by name, and leaves nothing.
    try testing.expectError(
        error.ExceededMemoryBudget,
        csv_write.run(ctx, .{ .inputs = &.{in}, .parallelism = 1, .max_memory = 4 << 20 }, in, out, opts, &report, a),
    );
    try testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(testing.io, out, .{}));
    // The same file at a budget that holds it writes every row.
    report = .{};
    const roomy: engine_types.QueryArgs = .{ .inputs = &.{in}, .parallelism = 1, .max_memory = 256 << 20 };
    const r = try csv_write.run(ctx, roomy, in, out, opts, &report, a);
    try testing.expectEqual(@as(i64, 8_000), r.rows_matched);
}
