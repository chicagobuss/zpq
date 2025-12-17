const std = @import("std");
const thrift = @import("thrift.zig");

test "VarInt Decoding" {
    // 0
    {
        const data = [_]u8{0x00};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readVarInt(i32);
        try std.testing.expectEqual(@as(i32, 0), val);
    }
    // 1
    {
        const data = [_]u8{0x01};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readVarInt(i32);
        try std.testing.expectEqual(@as(i32, 1), val);
    }
    // 127 (0x7F)
    {
        const data = [_]u8{0x7F};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readVarInt(i32);
        try std.testing.expectEqual(@as(i32, 127), val);
    }
    // 128 (0x80, 0x01) -> 10000000 00000001 -> 128
    {
        const data = [_]u8{0x80, 0x01};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readVarInt(i32);
        try std.testing.expectEqual(@as(i32, 128), val);
    }
    // 16383 (0xFF, 0x7F)
    {
        const data = [_]u8{0xFF, 0x7F};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readVarInt(i32);
        try std.testing.expectEqual(@as(i32, 16383), val);
    }
}

test "ZigZag Decoding" {
    // 0 -> 0
    {
        const data = [_]u8{0x00};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readZigZag(i32);
        try std.testing.expectEqual(@as(i32, 0), val);
    }
    // -1 -> 1
    {
        const data = [_]u8{0x01};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readZigZag(i32);
        try std.testing.expectEqual(@as(i32, -1), val);
    }
    // 1 -> 2
    {
        const data = [_]u8{0x02};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readZigZag(i32);
        try std.testing.expectEqual(@as(i32, 1), val);
    }
    // -2 -> 3
    {
        const data = [_]u8{0x03};
        var reader = thrift.Reader.init(&data);
        const val = try reader.readZigZag(i32);
        try std.testing.expectEqual(@as(i32, -2), val);
    }
    // Min i32?
}

test "Field Header" {
    // Type: True (1), Delta: 1 -> Byte: (1 << 4) | 1 = 0x11
    {
        const data = [_]u8{0x11};
        var reader = thrift.Reader.init(&data);
        const field = try reader.readFieldBegin();
        try std.testing.expectEqual(thrift.Type.True, field.type);
        try std.testing.expectEqual(@as(i16, 1), field.id);
    }
    
    // Type: I32 (5), Delta: 0 (Explicit ID follows)
    // ID: 15 (ZigZag: 15 -> 30 -> 0x1E)
    // Header Byte: (0 << 4) | 5 = 0x05
    // Payload: 0x1E
    {
        const data = [_]u8{0x05, 0x1E};
        var reader = thrift.Reader.init(&data);
        const field = try reader.readFieldBegin();
        try std.testing.expectEqual(thrift.Type.I32, field.type);
        try std.testing.expectEqual(@as(i16, 15), field.id);
    }
}

