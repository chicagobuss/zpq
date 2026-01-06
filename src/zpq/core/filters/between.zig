//! BETWEEN Filter Implementation
//!
//! Provides BETWEEN filter for Parquet queries (value >= low AND value <= high).
//! Optimized for page-level pruning using ColumnIndex min/max statistics.

const std = @import("std");
const schema = @import("../schema.zig");
const page_index = @import("../page_index.zig");

/// Filter for BETWEEN low AND high (inclusive on both ends).
pub const BetweenFilter = struct {
    low_bytes: []const u8,
    high_bytes: []const u8,
    parquet_type: schema.Type,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Parse a BETWEEN filter from two string values.
    pub fn parse(
        allocator: std.mem.Allocator,
        low_value: []const u8,
        high_value: []const u8,
        parquet_type: schema.Type,
    ) !Self {
        const low_bytes = try encodeValue(allocator, low_value, parquet_type);
        errdefer allocator.free(low_bytes);
        const high_bytes = try encodeValue(allocator, high_value, parquet_type);

        return .{
            .low_bytes = low_bytes,
            .high_bytes = high_bytes,
            .parquet_type = parquet_type,
            .allocator = allocator,
        };
    }

    fn encodeValue(allocator: std.mem.Allocator, value: []const u8, parquet_type: schema.Type) ![]const u8 {
        return switch (parquet_type) {
            .INT32 => blk: {
                const parsed = std.fmt.parseInt(i32, value, 10) catch return error.InvalidFilterValue;
                const bytes = try allocator.alloc(u8, 4);
                @memcpy(bytes, std.mem.asBytes(&parsed));
                break :blk bytes;
            },
            .INT64 => blk: {
                const parsed = std.fmt.parseInt(i64, value, 10) catch return error.InvalidFilterValue;
                const bytes = try allocator.alloc(u8, 8);
                @memcpy(bytes, std.mem.asBytes(&parsed));
                break :blk bytes;
            },
            .FLOAT => blk: {
                const parsed = std.fmt.parseFloat(f32, value) catch return error.InvalidFilterValue;
                const bytes = try allocator.alloc(u8, 4);
                @memcpy(bytes, std.mem.asBytes(&parsed));
                break :blk bytes;
            },
            .DOUBLE => blk: {
                const parsed = std.fmt.parseFloat(f64, value) catch return error.InvalidFilterValue;
                const bytes = try allocator.alloc(u8, 8);
                @memcpy(bytes, std.mem.asBytes(&parsed));
                break :blk bytes;
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try allocator.dupe(u8, value),
            else => error.UnsupportedFilterType,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.low_bytes);
        self.allocator.free(self.high_bytes);
    }

    // =========================================================================
    // Page-Level Pruning
    // =========================================================================

    /// Check if a page might contain matching values.
    /// A page can be skipped if:
    ///   - page_max < low (all values below range)
    ///   - page_min > high (all values above range)
    pub fn mightMatchPage(
        self: *const Self,
        column_index: *const page_index.ColumnIndex,
        page_idx: usize,
    ) bool {
        if (page_idx >= column_index.null_pages.len) return true;
        if (column_index.null_pages[page_idx]) return false; // All nulls

        const page_min = column_index.min_values[page_idx];
        const page_max = column_index.max_values[page_idx];

        // Skip if page_max < low (all values in page are below range)
        if (compareBytes(page_max, self.low_bytes, self.parquet_type) < 0) {
            return false;
        }

        // Skip if page_min > high (all values in page are above range)
        if (compareBytes(page_min, self.high_bytes, self.parquet_type) > 0) {
            return false;
        }

        return true;
    }

    /// Check if row group statistics indicate possible matches.
    pub fn mightMatchRowGroup(self: *const Self, stats: *const schema.Statistics) bool {
        // Check if column_max < low (all values below range)
        if (stats.max) |stat_max| {
            if (compareBytes(stat_max, self.low_bytes, self.parquet_type) < 0) {
                return false;
            }
        }

        // Check if column_min > high (all values above range)
        if (stats.min) |stat_min| {
            if (compareBytes(stat_min, self.high_bytes, self.parquet_type) > 0) {
                return false;
            }
        }

        return true;
    }

    fn compareBytes(a: []const u8, b: []const u8, parquet_type: schema.Type) i32 {
        return switch (parquet_type) {
            .INT32 => blk: {
                if (a.len < 4 or b.len < 4) break :blk 0;
                const av = std.mem.readInt(i32, a[0..4], .little);
                const bv = std.mem.readInt(i32, b[0..4], .little);
                break :blk if (av < bv) @as(i32, -1) else if (av > bv) @as(i32, 1) else @as(i32, 0);
            },
            .INT64 => blk: {
                if (a.len < 8 or b.len < 8) break :blk 0;
                const av = std.mem.readInt(i64, a[0..8], .little);
                const bv = std.mem.readInt(i64, b[0..8], .little);
                break :blk if (av < bv) @as(i32, -1) else if (av > bv) @as(i32, 1) else @as(i32, 0);
            },
            .FLOAT => blk: {
                if (a.len < 4 or b.len < 4) break :blk 0;
                const av: f32 = @bitCast(std.mem.readInt(u32, a[0..4], .little));
                const bv: f32 = @bitCast(std.mem.readInt(u32, b[0..4], .little));
                break :blk if (av < bv) @as(i32, -1) else if (av > bv) @as(i32, 1) else @as(i32, 0);
            },
            .DOUBLE => blk: {
                if (a.len < 8 or b.len < 8) break :blk 0;
                const av: f64 = @bitCast(std.mem.readInt(u64, a[0..8], .little));
                const bv: f64 = @bitCast(std.mem.readInt(u64, b[0..8], .little));
                break :blk if (av < bv) @as(i32, -1) else if (av > bv) @as(i32, 1) else @as(i32, 0);
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => blk: {
                const result = std.mem.order(u8, a, b);
                break :blk switch (result) {
                    .lt => @as(i32, -1),
                    .gt => @as(i32, 1),
                    .eq => @as(i32, 0),
                };
            },
            else => 0,
        };
    }

    // =========================================================================
    // Row-Level Matching
    // =========================================================================

    pub inline fn matchesInt32(self: *const Self, value: i32) bool {
        const low = std.mem.readInt(i32, self.low_bytes[0..4], .little);
        const high = std.mem.readInt(i32, self.high_bytes[0..4], .little);
        return value >= low and value <= high;
    }

    pub inline fn matchesInt64(self: *const Self, value: i64) bool {
        const low = std.mem.readInt(i64, self.low_bytes[0..8], .little);
        const high = std.mem.readInt(i64, self.high_bytes[0..8], .little);
        return value >= low and value <= high;
    }

    pub inline fn matchesFloat(self: *const Self, value: f32) bool {
        const low: f32 = @bitCast(std.mem.readInt(u32, self.low_bytes[0..4], .little));
        const high: f32 = @bitCast(std.mem.readInt(u32, self.high_bytes[0..4], .little));
        return value >= low and value <= high;
    }

    pub inline fn matchesDouble(self: *const Self, value: f64) bool {
        const low: f64 = @bitCast(std.mem.readInt(u64, self.low_bytes[0..8], .little));
        const high: f64 = @bitCast(std.mem.readInt(u64, self.high_bytes[0..8], .little));
        return value >= low and value <= high;
    }

    pub inline fn matchesBytes(self: *const Self, value: []const u8) bool {
        const cmp_low = std.mem.order(u8, value, self.low_bytes);
        const cmp_high = std.mem.order(u8, value, self.high_bytes);
        return (cmp_low == .gt or cmp_low == .eq) and (cmp_high == .lt or cmp_high == .eq);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "BetweenFilter INT32 row matching" {
    const allocator = std.testing.allocator;

    var filter = try BetweenFilter.parse(allocator, "10", "20", .INT32);
    defer filter.deinit();

    // In range
    try std.testing.expect(filter.matchesInt32(10));
    try std.testing.expect(filter.matchesInt32(15));
    try std.testing.expect(filter.matchesInt32(20));

    // Out of range
    try std.testing.expect(!filter.matchesInt32(9));
    try std.testing.expect(!filter.matchesInt32(21));
}

test "BetweenFilter INT64 row matching" {
    const allocator = std.testing.allocator;

    var filter = try BetweenFilter.parse(allocator, "100", "200", .INT64);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt64(100));
    try std.testing.expect(filter.matchesInt64(150));
    try std.testing.expect(filter.matchesInt64(200));
    try std.testing.expect(!filter.matchesInt64(99));
    try std.testing.expect(!filter.matchesInt64(201));
}

test "BetweenFilter BYTE_ARRAY row matching" {
    const allocator = std.testing.allocator;

    var filter = try BetweenFilter.parse(allocator, "b", "d", .BYTE_ARRAY);
    defer filter.deinit();

    try std.testing.expect(filter.matchesBytes("b"));
    try std.testing.expect(filter.matchesBytes("c"));
    try std.testing.expect(filter.matchesBytes("d"));
    try std.testing.expect(!filter.matchesBytes("a"));
    try std.testing.expect(!filter.matchesBytes("e"));
}

test "BetweenFilter page pruning" {
    const allocator = std.testing.allocator;

    var filter = try BetweenFilter.parse(allocator, "100", "200", .INT32);
    defer filter.deinit();

    // Helper to create column index with single page
    var min_val_below = std.mem.toBytes(@as(i32, 10));
    var max_val_below = std.mem.toBytes(@as(i32, 50));
    var min_vals_below = [_][]const u8{min_val_below[0..]};
    var max_vals_below = [_][]const u8{max_val_below[0..]};
    var buf_null_pages = [_]bool{false};

    const col_idx_below = page_index.ColumnIndex{
        .min_values = min_vals_below[0..],
        .max_values = max_vals_below[0..],
        .null_pages = &buf_null_pages,
        .boundary_order = .UNORDERED,
        .null_counts = null,
    };

    // Page entirely below range - should be skipped
    try std.testing.expect(!filter.mightMatchPage(&col_idx_below, 0));

    // Page entirely above range - should be skipped
    var min_val_above = std.mem.toBytes(@as(i32, 300));
    var max_val_above = std.mem.toBytes(@as(i32, 400));
    var min_vals_above = [_][]const u8{min_val_above[0..]};
    var max_vals_above = [_][]const u8{max_val_above[0..]};

    const col_idx_above = page_index.ColumnIndex{
        .min_values = min_vals_above[0..],
        .max_values = max_vals_above[0..],
        .null_pages = &buf_null_pages,
        .boundary_order = .UNORDERED,
        .null_counts = null,
    };

    try std.testing.expect(!filter.mightMatchPage(&col_idx_above, 0));

    // Page overlapping range - should not be skipped
    var min_val_overlap = std.mem.toBytes(@as(i32, 50));
    var max_val_overlap = std.mem.toBytes(@as(i32, 150));
    var min_vals_overlap = [_][]const u8{min_val_overlap[0..]};
    var max_vals_overlap = [_][]const u8{max_val_overlap[0..]};

    const col_idx_overlap = page_index.ColumnIndex{
        .min_values = min_vals_overlap[0..],
        .max_values = max_vals_overlap[0..],
        .null_pages = &buf_null_pages,
        .boundary_order = .UNORDERED,
        .null_counts = null,
    };

    try std.testing.expect(filter.mightMatchPage(&col_idx_overlap, 0));
}
