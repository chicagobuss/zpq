const std = @import("std");
const filter_mod = @import("../filter.zig");
const file = @import("../file.zig");

pub fn collectFilterColumns(filter: filter_mod.Filter, list: *std.ArrayListUnmanaged(usize), allocator: std.mem.Allocator) !void {
    switch (filter) {
        .int32 => |f| try list.append(allocator, f.col_idx),
        .int64 => |f| try list.append(allocator, f.col_idx),
        .float => |f| try list.append(allocator, f.col_idx),
        .double => |f| try list.append(allocator, f.col_idx),
        .string => |f| try list.append(allocator, f.col_idx),
        .boolean => |f| try list.append(allocator, f.col_idx),
        .and_filter => |f| {
            try collectFilterColumns(f.left.*, list, allocator);
            try collectFilterColumns(f.right.*, list, allocator);
        },
        .or_filter => |f| {
            try collectFilterColumns(f.left.*, list, allocator);
            try collectFilterColumns(f.right.*, list, allocator);
        },
    }
}

pub fn parseFilter(comptime T: type, filter_str: []const u8, pfile: *file.ParquetFile) !filter_mod.Filter {
    return parseFilterWithAllocator(T, filter_str, pfile, pfile.allocator);
}

pub fn parseFilterWithAllocator(comptime T: type, filter_str: []const u8, pfile: *file.ParquetFile, allocator: std.mem.Allocator) !filter_mod.Filter {
    // Check for OR first (lower precedence)
    if (std.mem.indexOf(u8, filter_str, " OR ")) |or_idx| {
        const left_str = std.mem.trim(u8, filter_str[0..or_idx], " ");
        const right_str = std.mem.trim(u8, filter_str[or_idx + 4 ..], " ");
        
        const left = try allocator.create(filter_mod.Filter);
        const right = try allocator.create(filter_mod.Filter);
        left.* = try parseFilterWithAllocator(T, left_str, pfile, allocator);
        right.* = try parseFilterWithAllocator(T, right_str, pfile, allocator);
        
        return .{ .or_filter = .{ .left = left, .right = right } };
    }
    
    // Check for AND (higher precedence)
    if (std.mem.indexOf(u8, filter_str, " AND ")) |and_idx| {
        const left_str = std.mem.trim(u8, filter_str[0..and_idx], " ");
        const right_str = std.mem.trim(u8, filter_str[and_idx + 5 ..], " ");
        
        const left = try allocator.create(filter_mod.Filter);
        const right = try allocator.create(filter_mod.Filter);
        left.* = try parseFilterWithAllocator(T, left_str, pfile, allocator);
        right.* = try parseFilterWithAllocator(T, right_str, pfile, allocator);
        
        return .{ .and_filter = .{ .left = left, .right = right } };
    }
    
    // Leaf filter: "colOPval" where OP is =, !=, <, >, <=, >=
    const operators = [_][]const u8{ "!=", "<=", ">=", "=", "<", ">" };
    var op_str: []const u8 = "";
    var op_type: filter_mod.Operator = .Eq;
    var op_idx: usize = 0;

    for (operators) |op| {
        if (std.mem.indexOf(u8, filter_str, op)) |idx| {
            op_idx = idx;
            op_str = op;
            op_type = switch (op[0]) {
                '=' => .Eq,
                '!' => .NotEq,
                '<' => if (op.len > 1) .LtEq else .Lt,
                '>' => if (op.len > 1) .GtEq else .Gt,
                else => unreachable,
            };
            break;
        }
    }

    if (op_str.len == 0) return error.InvalidFilter;

    const col_name = std.mem.trim(u8, filter_str[0..op_idx], " ");
    const val_str = std.mem.trim(u8, filter_str[op_idx + op_str.len ..], " ");

    const col_idx = try pfile.findColumnIndex(col_name);
    const field_type = try getFieldTypeFromMetadata(pfile, col_idx);

    return switch (field_type) {
        .ByteArray => .{ .string = .{ .col_idx = col_idx, .op = op_type, .value = val_str } },
        .Int32 => .{ .int32 = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseInt(i32, val_str, 10) } },
        .Int64 => .{ .int64 = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseInt(i64, val_str, 10) } },
        .Float => .{ .float = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseFloat(f32, val_str) } },
        .Double => .{ .double = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseFloat(f64, val_str) } },
        .Bool => blk: {
            const bool_val = std.mem.eql(u8, val_str, "true") or std.mem.eql(u8, val_str, "1");
            break :blk .{ .boolean = .{ .col_idx = col_idx, .op = op_type, .value = bool_val } };
        },
        .Date => .{ .int32 = .{ .col_idx = col_idx, .op = op_type, .value = try parseDate(val_str) } },
        .Timestamp => .{ .int64 = .{ .col_idx = col_idx, .op = op_type, .value = try parseTimestamp(val_str) } },
    };
}

const FieldType = enum { Int32, Int64, Float, Double, Bool, ByteArray, Date, Timestamp };

fn getFieldType(comptime T: type, name: []const u8) !FieldType {
    _ = T;
    _ = name;
    return error.UseMetadataInstead;
}

fn getFieldTypeFromMetadata(pfile: *file.ParquetFile, col_idx: usize) !FieldType {
    const elem = pfile.metadata.schema.items[col_idx + 1];
    const physical = elem.type orelse .BYTE_ARRAY;

    if (elem.converted_type) |ct| {
        switch (ct) {
            .UTF8 => return .ByteArray,
            .DATE => return .Date,
            .TIMESTAMP_MICROS, .TIMESTAMP_MILLIS => return .Timestamp,
            else => {},
        }
    }

    if (elem.logical_type) |lt| {
        switch (lt) {
            .STRING => return .ByteArray,
            .DATE => return .Date,
            .TIMESTAMP => return .Timestamp,
            else => {},
        }
    }

    return switch (physical) {
        .BOOLEAN => .Bool,
        .INT32 => .Int32,
        .INT64 => .Int64,
        .FLOAT => .Float,
        .DOUBLE => .Double,
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => .ByteArray,
        .INT96 => .Timestamp,
    };
}

fn parseDate(s: []const u8) !i32 {
    if (std.mem.startsWith(u8, s, "'") and std.mem.endsWith(u8, s, "'")) {
        const trimmed = s[1 .. s.len - 1];
        if (trimmed.len == 10 and trimmed[4] == '-' and trimmed[7] == '-') {
            const year = try std.fmt.parseInt(i32, trimmed[0..4], 10);
            const month = try std.fmt.parseInt(u4, trimmed[5..7], 10);
            const day = try std.fmt.parseInt(u5, trimmed[8..10], 10);
            return dateToDays(year, month, day);
        }
    }
    return std.fmt.parseInt(i32, s, 10);
}

fn dateToDays(year: i32, month: u4, day: u5) i32 {
    var y = year;
    var m = @as(i32, month);
    if (m <= 2) {
        y -= 1;
        m += 12;
    }
    return 365 * y + @divFloor(y, 4) - @divFloor(y, 100) + @divFloor(y, 400) + @divFloor(306 * (m + 1), 10) - 428 + @as(i32, day) - 719163;
}

fn parseTimestamp(s: []const u8) !i64 {
    // For now support raw integer or simple YYYY-MM-DD HH:MM:SS (stub)
    return std.fmt.parseInt(i64, s, 10);
}

test "parse date" {
    const d1 = try parseDate("'1970-01-01'");
    try std.testing.expectEqual(@as(i32, 0), d1);

    const d2 = try parseDate("'2024-01-01'");
    try std.testing.expectEqual(@as(i32, 19723), d2);
    
    const d3 = try parseDate("123");
    try std.testing.expectEqual(@as(i32, 123), d3);
}
