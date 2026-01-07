const std = @import("std");
const schema = @import("schema.zig");

/// Cache for filter column values that match the predicate.
/// When the filter column is also in the output select list, we cache
/// the matching values during the filter scan to avoid re-reading.
pub const FilterColumnCache = struct {
    allocator: std.mem.Allocator,
    col_type: schema.Type,

    // Only one of these will be populated based on col_type
    byte_array_values: std.ArrayListUnmanaged([]const u8),
    int32_values: std.ArrayListUnmanaged(i32),
    int64_values: std.ArrayListUnmanaged(i64),
    float_values: std.ArrayListUnmanaged(f32),
    double_values: std.ArrayListUnmanaged(f64),
    bool_values: std.ArrayListUnmanaged(bool),

    pub fn init(allocator: std.mem.Allocator, col_type: schema.Type) FilterColumnCache {
        return .{
            .allocator = allocator,
            .col_type = col_type,
            .byte_array_values = .{},
            .int32_values = .{},
            .int64_values = .{},
            .float_values = .{},
            .double_values = .{},
            .bool_values = .{},
        };
    }

    pub fn deinit(self: *FilterColumnCache) void {
        // Free byte array strings (we own them)
        for (self.byte_array_values.items) |v| {
            self.allocator.free(v);
        }
        self.byte_array_values.deinit(self.allocator);
        self.int32_values.deinit(self.allocator);
        self.int64_values.deinit(self.allocator);
        self.float_values.deinit(self.allocator);
        self.double_values.deinit(self.allocator);
        self.bool_values.deinit(self.allocator);
    }

    /// Append a BYTE_ARRAY value. The cache takes ownership of the duped slice.
    pub fn appendByteArray(self: *FilterColumnCache, value: []const u8) !void {
        const duped = try self.allocator.dupe(u8, value);
        try self.byte_array_values.append(self.allocator, duped);
    }

    /// Append an already-owned BYTE_ARRAY value (no dupe needed)
    pub fn appendByteArrayOwned(self: *FilterColumnCache, owned_value: []const u8) !void {
        try self.byte_array_values.append(self.allocator, owned_value);
    }

    pub fn appendInt32(self: *FilterColumnCache, value: i32) !void {
        try self.int32_values.append(self.allocator, value);
    }

    pub fn appendInt64(self: *FilterColumnCache, value: i64) !void {
        try self.int64_values.append(self.allocator, value);
    }

    pub fn appendFloat(self: *FilterColumnCache, value: f32) !void {
        try self.float_values.append(self.allocator, value);
    }

    pub fn appendDouble(self: *FilterColumnCache, value: f64) !void {
        try self.double_values.append(self.allocator, value);
    }

    pub fn appendBool(self: *FilterColumnCache, value: bool) !void {
        try self.bool_values.append(self.allocator, value);
    }

    pub fn count(self: FilterColumnCache) usize {
        return switch (self.col_type) {
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => self.byte_array_values.items.len,
            .INT32 => self.int32_values.items.len,
            .INT64 => self.int64_values.items.len,
            .FLOAT => self.float_values.items.len,
            .DOUBLE => self.double_values.items.len,
            .BOOLEAN => self.bool_values.items.len,
            else => 0,
        };
    }

    /// Get cached byte array values (for writing to ParquetWriter)
    pub fn getByteArrayValues(self: FilterColumnCache) []const []const u8 {
        return self.byte_array_values.items;
    }

    pub fn getInt32Values(self: FilterColumnCache) []const i32 {
        return self.int32_values.items;
    }

    pub fn getInt64Values(self: FilterColumnCache) []const i64 {
        return self.int64_values.items;
    }

    pub fn getFloatValues(self: FilterColumnCache) []const f32 {
        return self.float_values.items;
    }

    pub fn getDoubleValues(self: FilterColumnCache) []const f64 {
        return self.double_values.items;
    }

    pub fn getBoolValues(self: FilterColumnCache) []const bool {
        return self.bool_values.items;
    }

    /// Reserve capacity
    pub fn ensureCapacity(self: *FilterColumnCache, capacity: usize) !void {
        switch (self.col_type) {
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try self.byte_array_values.ensureTotalCapacity(self.allocator, capacity),
            .INT32 => try self.int32_values.ensureTotalCapacity(self.allocator, capacity),
            .INT64 => try self.int64_values.ensureTotalCapacity(self.allocator, capacity),
            .FLOAT => try self.float_values.ensureTotalCapacity(self.allocator, capacity),
            .DOUBLE => try self.double_values.ensureTotalCapacity(self.allocator, capacity),
            .BOOLEAN => try self.bool_values.ensureTotalCapacity(self.allocator, capacity),
            else => {},
        }
    }

    /// Clear the cache (for reuse across row groups)
    pub fn clear(self: *FilterColumnCache) void {
        // Free byte array strings before clearing
        for (self.byte_array_values.items) |v| {
            self.allocator.free(v);
        }
        self.byte_array_values.clearRetainingCapacity();
        self.int32_values.clearRetainingCapacity();
        self.int64_values.clearRetainingCapacity();
        self.float_values.clearRetainingCapacity();
        self.double_values.clearRetainingCapacity();
        self.bool_values.clearRetainingCapacity();
    }
};

test "FilterColumnCache BYTE_ARRAY" {
    const allocator = std.testing.allocator;

    var cache = FilterColumnCache.init(allocator, .BYTE_ARRAY);
    defer cache.deinit();

    try cache.appendByteArray("hello");
    try cache.appendByteArray("world");
    try cache.appendByteArray("test");

    try std.testing.expectEqual(@as(usize, 3), cache.count());

    const values = cache.getByteArrayValues();
    try std.testing.expectEqualStrings("hello", values[0]);
    try std.testing.expectEqualStrings("world", values[1]);
    try std.testing.expectEqualStrings("test", values[2]);
}

test "FilterColumnCache INT32" {
    const allocator = std.testing.allocator;

    var cache = FilterColumnCache.init(allocator, .INT32);
    defer cache.deinit();

    try cache.appendInt32(42);
    try cache.appendInt32(-100);
    try cache.appendInt32(0);

    try std.testing.expectEqual(@as(usize, 3), cache.count());
    try std.testing.expectEqualSlices(i32, &[_]i32{ 42, -100, 0 }, cache.getInt32Values());
}

test "FilterColumnCache clear and reuse" {
    const allocator = std.testing.allocator;

    var cache = FilterColumnCache.init(allocator, .BYTE_ARRAY);
    defer cache.deinit();

    try cache.appendByteArray("first");
    try cache.appendByteArray("second");
    try std.testing.expectEqual(@as(usize, 2), cache.count());

    cache.clear();
    try std.testing.expectEqual(@as(usize, 0), cache.count());

    try cache.appendByteArray("new_value");
    try std.testing.expectEqual(@as(usize, 1), cache.count());
    try std.testing.expectEqualStrings("new_value", cache.getByteArrayValues()[0]);
}

test "FilterColumnCache DOUBLE" {
    const allocator = std.testing.allocator;

    var cache = FilterColumnCache.init(allocator, .DOUBLE);
    defer cache.deinit();

    try cache.appendDouble(3.14159);
    try cache.appendDouble(-273.15);

    try std.testing.expectEqual(@as(usize, 2), cache.count());
    try std.testing.expectApproxEqAbs(@as(f64, 3.14159), cache.getDoubleValues()[0], 0.00001);
    try std.testing.expectApproxEqAbs(@as(f64, -273.15), cache.getDoubleValues()[1], 0.00001);
}

test "FilterColumnCache BOOLEAN" {
    const allocator = std.testing.allocator;

    var cache = FilterColumnCache.init(allocator, .BOOLEAN);
    defer cache.deinit();

    try cache.appendBool(true);
    try cache.appendBool(false);
    try cache.appendBool(true);

    try std.testing.expectEqual(@as(usize, 3), cache.count());
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, false, true }, cache.getBoolValues());
}
