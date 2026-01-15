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
    const field_type = try getFieldType(T, col_name);

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
    };
}

const FieldType = enum { Int32, Int64, Float, Double, Bool, ByteArray };

fn getFieldType(comptime T: type, name: []const u8) !FieldType {
    const fields = @typeInfo(T).@"struct".fields;
    inline for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) {
            if (field.type == i32) return .Int32;
            if (field.type == i64) return .Int64;
            if (field.type == f32) return .Float;
            if (field.type == f64) return .Double;
            if (field.type == bool) return .Bool;
            if (field.type == []const u8) return .ByteArray;
        }
    }
    return error.FieldNotFound;
}
