//! Page-header rewriting without copying payloads or allocating page-sized scratch. Known writer defects apply
//! to the nested statistics record; compressed-size changes also drop the checksum over the old payload.

const std = @import("std");
const thrift = @import("../thrift.zig");
const schema = @import("../schema.zig");
const Policy = @import("../parquet/writer_statistics.zig").Policy;

const Kind = enum { page, v1, v2 };

pub fn write(out: anytype, header: []const u8, compressed_size: ?i32, policy: Policy) !void {
    try writeStruct(out, header, .page, compressed_size, policy);
}

pub fn size(header: []const u8, compressed_size: ?i32, policy: Policy) !usize {
    const Counter = struct {
        bytes: usize = 0,
        pub fn write(c: *@This(), bytes: []const u8) !void {
            c.bytes = try std.math.add(usize, c.bytes, bytes.len);
        }
    };
    var counter: Counter = .{};
    try write(&counter, header, compressed_size, policy);
    return counter.bytes;
}

/// Recompression already owns a gathered header. Copy paths stream it with `write` instead.
pub fn rewrite(a: std.mem.Allocator, header: []const u8, compressed_size: ?i32, policy: Policy) ![]const u8 {
    const Buffer = struct {
        a: std.mem.Allocator,
        bytes: std.ArrayList(u8) = .empty,
        pub fn write(b: *@This(), bytes: []const u8) !void {
            try b.bytes.appendSlice(b.a, bytes);
        }
    };
    var buffer: Buffer = .{ .a = a };
    errdefer buffer.bytes.deinit(a);
    try write(&buffer, header, compressed_size, policy);
    return buffer.bytes.toOwnedSlice(a);
}

fn writeStruct(out: anytype, bytes: []const u8, kind: Kind, compressed: ?i32, policy: Policy) !void {
    var reader = thrift.Reader.init(bytes);
    reader.readStructBegin();
    var previous: i16 = 0;
    while (true) {
        const field = try reader.readFieldBegin();
        if (field.type == .Stop) break;
        const start = reader.pos;
        try reader.skip(field.type);
        const payload = bytes[start..reader.pos];
        const stats_id: i16 = if (kind == .v1) 5 else 8;
        if (kind != .page and field.id == stats_id and !accepts(policy, field.type, payload)) continue;
        if (kind == .page and field.id == 4 and compressed != null) continue;
        const replacement = if (kind == .page and field.id == 3) compressed else null;
        try tag(out, field.type, field.id, &previous, replacement);
        if (replacement != null) continue;
        if (kind == .page and policy != .trust and field.type == .Struct and (field.id == 5 or field.id == 8)) {
            try writeStruct(out, payload, if (field.id == 5) .v1 else .v2, null, policy);
        } else {
            try out.write(payload);
        }
    }
    try out.write(&.{0});
}

fn accepts(policy: Policy, field_type: thrift.Type, bytes: []const u8) bool {
    switch (policy) {
        .trust => return true,
        .absent => return false,
        .equal_only => {},
    }
    if (field_type != .Struct) return false;
    var reader = thrift.Reader.init(bytes);
    const stats = schema.Statistics.read(&reader) catch return false;
    if (stats.min_value != null and stats.max_value != null) return policy.accepts(stats.min_value, stats.max_value);
    return policy.accepts(stats.min, stats.max);
}

fn tag(out: anytype, t: thrift.Type, id: i16, previous: *i16, replacement: ?i32) !void {
    // Compact field tag <=4 bytes, replacement i32 <=5 bytes. This buffer never grows or allocates.
    var bytes: [9]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&bytes);
    var w = thrift.Writer.init(fixed.allocator());
    w.buffer.items = bytes[0..0];
    w.buffer.capacity = bytes.len;
    const delta = @as(i32, id) - @as(i32, previous.*);
    if (delta > 0 and delta <= 15) {
        try w.writeByte(@as(u8, @intCast(delta)) << 4 | @backingInt(t));
    } else {
        try w.writeByte(@backingInt(t));
        try w.writeZigZag(id);
    }
    if (replacement) |n| try w.writeZigZag(n);
    try out.write(w.bytes());
    previous.* = id;
}

test "writer page headers: both versions remove affected statistics and preserve payload checksums on copies" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |v2| {
        var w = thrift.Writer.init(a);
        defer w.deinit();
        w.writeStructBegin();
        try w.writeFieldI32(1, if (v2) 3 else 0);
        try w.writeFieldI32(2, 16);
        try w.writeFieldI32(3, 8);
        try w.writeFieldI32(4, 123);
        try w.writeFieldBegin(.Struct, if (v2) 8 else 5);
        w.writeStructBegin();
        try w.writeFieldI32(1, 3);
        try w.writeFieldBegin(.Struct, if (v2) 8 else 5);
        const stats: schema.Statistics = .{ .min_value = "a", .max_value = "b", .null_count = 0 };
        try stats.write(&w);
        // Unknown fields and boolean values must survive the changed compact field deltas.
        try w.writeFieldBool(10, true);
        try w.writeStructEnd();
        try w.writeStructEnd();
        for ([_]Policy{ .trust, .equal_only, .absent }) |policy| {
            for ([_]?i32{ null, 7 }) |compressed| {
                const rewritten = try rewrite(a, w.bytes(), compressed, policy);
                defer a.free(rewritten);
                try std.testing.expectEqual(rewritten.len, try size(w.bytes(), compressed, policy));
                var r = thrift.Reader.init(rewritten);
                r.readStructBegin();
                var found_crc = false;
                var found_stats = false;
                var found_bool = false;
                while (true) {
                    const f = try r.readFieldBegin();
                    if (f.type == .Stop) break;
                    if (f.id == 4) found_crc = true;
                    if (f.id == (if (v2) @as(i16, 8) else 5)) {
                        const saved = r.last_field_id;
                        r.readStructBegin();
                        while (true) {
                            const nested = try r.readFieldBegin();
                            if (nested.type == .Stop) break;
                            if (nested.id == (if (v2) @as(i16, 8) else 5)) found_stats = true;
                            if (nested.id == 10) found_bool = nested.type == .True;
                            try r.skip(nested.type);
                        }
                        r.last_field_id = saved;
                    } else try r.skip(f.type);
                }
                try std.testing.expectEqual(compressed == null, found_crc);
                try std.testing.expectEqual(policy == .trust, found_stats);
                try std.testing.expect(found_bool);
            }
        }
    }
}

test "writer page headers: equal extrema survive ordering policy and allocation refusals leak nothing" {
    const a = std.testing.allocator;
    var w = thrift.Writer.init(a);
    defer w.deinit();
    w.writeStructBegin();
    try w.writeFieldI32(1, 0);
    try w.writeFieldI32(2, 4);
    try w.writeFieldI32(3, 4);
    try w.writeFieldBegin(.Struct, 5);
    w.writeStructBegin();
    try w.writeFieldI32(1, 1);
    try w.writeFieldBegin(.Struct, 5);
    // Larger than the stack tag buffer: the raw statistics payload must be borrowed, never copied there.
    const value = try a.alloc(u8, 8192);
    defer a.free(value);
    @memset(value, 'x');
    const stats: schema.Statistics = .{ .min_value = value, .max_value = value, .null_count = 0 };
    try stats.write(&w);
    try w.writeStructEnd();
    try w.writeStructEnd();
    const kept = try rewrite(a, w.bytes(), null, .equal_only);
    defer a.free(kept);
    try std.testing.expectEqualSlices(u8, w.bytes(), kept);
    try std.testing.expect((try size(w.bytes(), null, .absent)) < 32);
    const Check = struct {
        fn run(alloc: std.mem.Allocator, bytes: []const u8) !void {
            const result = try rewrite(alloc, bytes, 8, .equal_only);
            defer alloc.free(result);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{w.bytes()});
}
