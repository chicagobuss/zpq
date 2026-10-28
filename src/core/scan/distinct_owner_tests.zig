const std = @import("std");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const scan_owner_distinct = @import("owner_distinct.zig");
const scalarDistinctWideParts = scan_owner_distinct.scalarDistinctWideParts;
const scan_execute = @import("execute.zig");
const runMultiAggregate = scan_execute.runMultiAggregate;
const explainAggregate = scan_execute.explainAggregate;
const prepareAggregate = scan_execute.prepareAggregate;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const FastPath = scan_types.FastPath;
const WorkItem = scan_types.WorkItem;
const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");

test "raw payload owner multisets preserve NULLs AVG ties readers and poisoned-range replay" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i32 = undefined;
    var q: [2048]i32 = undefined;
    var v: [2048]i64 = undefined;
    var w: [2048]i64 = undefined;
    var kd: [2048]u32 = undefined;
    var qd: [2048]u32 = undefined;
    var vd: [2048]u32 = undefined;
    var wd: [2048]u32 = undefined;
    for (0..k.len) |i| {
        k[i] = @intCast(i % 17);
        q[i] = @intCast(i % 9);
        v[i] = @as(i64, @intCast(i % 19)) - 9;
        w[i] = @as(i64, @intCast(i % 23)) * 7 - 40;
        kd[i] = @intFromBool(i % 13 != 0);
        qd[i] = @intFromBool(i % 17 != 0);
        vd[i] = @intFromBool(i % 19 != 0);
        wd[i] = @intFromBool(i % 23 != 0);
    }
    // The final selected row exceeds the poisoned proposal, after earlier sliced callbacks contributed.
    k[k.len - 1] = 1 << 20;
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT32, .nullable = true, .col = .{ .i32 = .{ .values = &k, .def_levels = &kd, .max_def = 1 } } },
        .{ .name = "q", .parquet_type = .INT32, .nullable = true, .col = .{ .i32 = .{ .values = &q, .def_levels = &qd, .max_def = 1 } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &v, .def_levels = &vd, .max_def = 1 } } },
        .{ .name = "w", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &w, .def_levels = &wd, .max_def = 1 } } },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &columns, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var poisoned = meta;
    poisoned.row_groups.items = try a.dupe(schema.RowGroup, groups);
    var zero: [4]u8 = @splat(0);
    var sixteen: [4]u8 = undefined;
    std.mem.writeInt(i32, &sixteen, 16, .little);
    for (poisoned.row_groups.items) |*rg| {
        rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
        var st = &rg.columns.items[0].meta_data.?.statistics.?;
        st.min_value = &zero;
        st.min = &zero;
        st.max_value = &sixteen;
        st.max = &sixteen;
    }
    for ([_][]const u8{
        "count(*) AS c,sum(v) AS sv,avg(w) AS aw",
        "count(*) AS c,sum(v) AS sv,avg(w) AS aw,count(v) AS nv",
    }) |aggregate| for ([_]?[]const u8{ null, "v >= -4" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "raw-payload-multisets", .bytes = bytes.buffer.items }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "k,q",
            .filter = filter,
            .max_memory = 256 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .ndv_union = false,
            .dense = false,
            .plain_rules = false,
            .trust_stats = false,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 9, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]bool{ false, true }) |poison| for ([_]bool{ false, true }) |encoded|
            for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
                args.metas = if (poison) &.{poisoned} else &.{meta};
                args.integer_records = true;
                args.encoded = encoded;
                args.parallelism = j;
                args.decode_batch_rows = window;
                var taken: std.EnumSet(FastPath) = .{};
                args.taken = &taken;
                const ex = try explainAggregate(gpa, a, args);
                try testing.expectEqual(.packed_count_top_h, ex.record_path.?.layout);
                if (poison) {
                    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
                    try testing.expect(taken.contains(.integer_records));
                }
                var got = if (poison) try runMultiAggregate(gpa, a, args) else try prepareAggregate(gpa, a, args, false);
                defer got.deinit(gpa);
                if (!poison) try testing.expect(taken.contains(.integer_records));
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
            };
        // Scan-all retains full physical widths: a 66-bit raw tuple cannot enter the u64 payload owner.
        args.scan_all = true;
        args.taken = null;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.record_path == null);
    };
}

test "distinct admission prices both wide pair reductions against covered local sets" {
    const hw = @import("../hw.zig");
    const saved_cpu = hw.test_cpu;
    defer hw.test_cpu = saved_cpu;
    hw.test_cpu = .{ .l2 = .{ .bytes = 128 } };
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [4096]i64 = undefined;
    var x: [4096]i64 = undefined;
    for (&k, &x, 0..) |*key, *value, i| {
        key.* = (1 << 40) + @as(i64, @intCast(i / 64));
        value.* = @intCast(i % 127);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i64 = .{ .values = &k } }, .parquet_type = .INT64, .nullable = false },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "pair-cost", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k",
        .max_memory = 256 << 20,
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .integer_records_min_rows = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const ex = try explainAggregate(gpa, a, args);
    try testing.expect(ex.record_path == null or ex.record_path.?.layout != .pair_distinct);
    // A small local group table must retain the cache's opportunity to coalesce pairs.
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20 } };
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    hw.test_cpu = .{ .l2 = .{ .bytes = 128 } };
    // A budget retry disables plain cost rules and must recover the available exact owner.
    args.plain_rules = false;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    args.plain_rules = true;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.distinct_pairs = .on;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "distinct admission refuses declared dictionary fallback before retaining a composite identity" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i32 = undefined;
    var x: [2048]i64 = undefined;
    var names: [2048][]const u8 = undefined;
    for (&k, &x, &names, 0..) |*key, *value, *name, i| {
        key.* = @intCast(i % 7);
        value.* = @intCast(i % 61);
        name.* = if (i % 3 == 0) "" else "phone";
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &k } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "s", .col = .{ .string = .{ .values = &names } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "declared-fallback", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k,s",
        .filter = "s != ''",
        .max_memory = 64 << 20,
        .parallelism = 1,
        .integer_records_min_rows = 0,
    };
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    const stats = try a.dupe(schema.PageEncodingStats, &.{
        .{ .page_type = @backingInt(schema.PageType.DICTIONARY_PAGE), .encoding = @backingInt(schema.Encoding.PLAIN), .count = 1 },
        .{ .page_type = @backingInt(schema.PageType.DATA_PAGE), .encoding = @backingInt(schema.Encoding.RLE_DICTIONARY), .count = 1 },
        .{ .page_type = @backingInt(schema.PageType.DATA_PAGE), .encoding = @backingInt(schema.Encoding.PLAIN), .count = 1 },
    });
    meta.row_groups.items[0].columns.items[2].meta_data.?.encoding_stats = std.ArrayListUnmanaged(schema.PageEncodingStats).fromOwnedSlice(stats);
    const ex = try explainAggregate(gpa, a, args);
    try testing.expect(ex.record_path == null or ex.record_path.?.layout != .pair_distinct);
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    args.distinct_pairs = .off;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    args.distinct_pairs = .on;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
}

test "distinct admission compares wide pair work with the admitted integer DISTINCT top cut" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i64 = undefined;
    var x: [2048]i64 = undefined;
    for (&k, &x, 0..) |*key, *value, i| {
        key.* = @bitCast(std.hash.Wyhash.hash(0, std.mem.asBytes(&i)));
        value.* = @intCast(i % 127);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i64 = .{ .values = &k } }, .parquet_type = .INT64, .nullable = false },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "integer-distinct-cost", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k",
        .integer_records = false,
        .integer_records_min_rows = 1,
        .max_memory = 256 << 20,
        .min_bytes_per_worker = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7, .offset = 2 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.integer_records = true;
    for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        args.parallelism = j;
        args.decode_batch_rows = window;
        const ex = try explainAggregate(gpa, a, args);
        try testing.expectEqual(.integer_distinct_top_h, ex.record_path.?.layout);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    };
    // A memory retry disables the cost rule while retaining the same checked raw identity. Forced .on instead
    // uses unrestricted physical widths, which need not fit this 65+65-bit pair.
    args.plain_rules = false;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
}

test "distinct admission scalar fanout prices sparse numeric ranges without changing identities" {
    const gpa = testing.allocator;
    const hw = @import("../hw.zig");
    const saved = hw.test_cpu;
    defer hw.test_cpu = saved;
    hw.test_cpu = .{ .l2 = .{ .bytes = 256 << 10 } };
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sparse: [2048]i32 = undefined;
    var compact: [2048]i64 = undefined;
    for (&sparse, &compact, 0..) |*s, *c, i| {
        s.* = @intCast((i % 1000) * 1000000);
        c.* = @intCast(i % 127);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "s", .col = .{ .i32 = .{ .values = &sparse } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "c", .col = .{ .i64 = .{ .values = &compact } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, sparse.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 4);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 4;
    const work = [_]WorkItem{.{ .file = 0, .rg = 0, .agg_start = 0, .agg_len = 0, .columns = .{ .read = &.{}, .consumed = &.{}, .preserve = &.{} }, .accumulators = &.{} }};
    const s: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const c: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    try testing.expect(scalarDistinctWideParts(s, &.{meta}, &work));
    try testing.expect(!scalarDistinctWideParts(c, &.{meta}, &work));
    // With enough cache even the wide image may use the bitmap-friendly topology.
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 30 } };
    try testing.expect(!scalarDistinctWideParts(s, &.{meta}, &work));
    for ([_][]const u8{ "s", "c" }) |column| {
        const args: MultiAggArgs = .{ .inputs = &.{.{ .name = "scalar-fanout", .bytes = bytes.buffer.items }}, .metas = &.{meta}, .aggregate = try std.fmt.allocPrint(a, "count(DISTINCT {s}) AS d", .{column}), .parallelism = 4, .min_bytes_per_worker = 1, .integer_records = false, .max_memory = 64 << 20 };
        hw.test_cpu = .{ .l2 = .{ .bytes = 256 << 10 } };
        var wide = try runMultiAggregate(gpa, a, args);
        defer wide.deinit(gpa);
        hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 30 } };
        var narrow = try runMultiAggregate(gpa, a, args);
        defer narrow.deinit(gpa);
        try testing.expectEqualDeep(wide.output.aggs, narrow.output.aggs);
    }
}
