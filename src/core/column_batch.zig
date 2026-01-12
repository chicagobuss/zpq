const std = @import("std");
const schema = @import("schema.zig");
const selection = @import("selection.zig");

/// ColumnData holds the actual vectorized values for a column.
pub const ColumnData = union(enum) {
    bool: []bool,
    i32: []i32,
    i64: []i64,
    f32: []f32,
    f64: []f64,
    /// Slices into a buffer owned by the reader or morsel.
    byte_array: [][]const u8,
    /// Flat buffer for fixed length bytes.
    fixed_len_byte_array: []u8,
};

/// A single column in a batch.
pub const Column = struct {
    name: []const u8,
    parquet_type: schema.Type,
    data: ColumnData,
    null_count: usize,
    /// Optional validity mask. If null, all values are assumed non-null.
    /// Uses 1 bit per value, same as selection vector bitmask.
    validity: ?[]u64 = null,

    pub fn deinit(self: *Column, allocator: std.mem.Allocator) void {
        switch (self.data) {
            inline else => |d| allocator.free(d),
        }
        if (self.validity) |v| allocator.free(v);
    }
};

/// ColumnBatch is the primary data structure for columnar processing.
/// It holds a fixed number of rows (e.g. 4096) across multiple columns.
pub const ColumnBatch = struct {
    allocator: std.mem.Allocator,
    columns: std.ArrayListUnmanaged(Column) = .{},
    selection: selection.SelectionVector,
    num_rows: usize = 0,
    capacity: usize,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !ColumnBatch {
        return .{
            .allocator = allocator,
            .selection = try selection.SelectionVector.init(allocator, capacity),
            .capacity = capacity,
        };
    }

    pub fn deinit(self: *ColumnBatch) void {
        for (self.columns.items) |*col| {
            col.deinit(self.allocator);
        }
        self.columns.deinit(self.allocator);
        self.selection.deinit();
    }

    /// Add a new column to the batch based on the Parquet schema element.
    pub fn addColumn(self: *ColumnBatch, element: schema.SchemaElement) !*Column {
        const p_type = element.type orelse return error.MissingType;
        
        const data: ColumnData = switch (p_type) {
            .INT32 => .{ .i32 = try self.allocator.alloc(i32, self.capacity) },
            .INT64 => .{ .i64 = try self.allocator.alloc(i64, self.capacity) },
            .FLOAT => .{ .f32 = try self.allocator.alloc(f32, self.capacity) },
            .DOUBLE => .{ .f64 = try self.allocator.alloc(f64, self.capacity) },
            .BOOLEAN => .{ .bool = try self.allocator.alloc(bool, self.capacity) },
            .BYTE_ARRAY => .{ .byte_array = try self.allocator.alloc([]const u8, self.capacity) },
            .FIXED_LEN_BYTE_ARRAY => blk: {
                const len = element.type_length orelse return error.MissingTypeLength;
                break :blk .{ .fixed_len_byte_array = try self.allocator.alloc(u8, self.capacity * @as(usize, @intCast(len))) };
            },
            .INT96 => return error.UnsupportedType,
        };

        try self.columns.append(self.allocator, .{
            .name = element.name,
            .parquet_type = p_type,
            .data = data,
            .null_count = 0,
            .validity = null,
        });
        
        return &self.columns.items[self.columns.items.len - 1];
    }

    /// Reset the batch for reuse, keeping allocated buffers.
    pub fn reset(self: *ColumnBatch) void {
        self.num_rows = 0;
        // Selection vector resets to all active by default in our implementation
        // but we should probably have a clear/reset method on it.
        @memset(self.selection.mask, ~@as(u64, 0));
        for (self.columns.items) |*col| {
            col.null_count = 0;
            if (col.validity) |v| @memset(v, ~@as(u64, 0));
        }
    }
};

test "column batch basics" {
    const allocator = std.testing.allocator;
    var batch = try ColumnBatch.init(allocator, 1024);
    defer batch.deinit();

    const col = try batch.addColumn(.{
        .name = "age",
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });

    try std.testing.expectEqualStrings("age", col.name);
    try std.testing.expect(col.data == .i32);
    try std.testing.expectEqual(@as(usize, 1024), col.data.i32.len);
}

