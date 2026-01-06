//! Range Filter Implementation
//!
//! Supports: <, >, <=, >= operators
//!
//! Page pruning logic:
//! - `col > X`: skip if page_max <= X
//! - `col >= X`: skip if page_max < X
//! - `col < X`: skip if page_min >= X
//! - `col <= X`: skip if page_min > X
//!
//! Row matching: type-aware comparison of decoded values.

const std = @import("std");
const schema = @import("../schema.zig");
const page_index = @import("../page_index.zig");

/// Comparison operator for range filters
pub const RangeOp = enum {
    gt, // >
    gte, // >=
    lt, // <
    lte, // <=
    neq, // !=

    /// Convert from pipeline Predicate.Operator
    pub fn fromPredicateOp(op: anytype) ?RangeOp {
        return switch (op) {
            .gt => .gt,
            .gte => .gte,
            .lt => .lt,
            .lte => .lte,
            .neq => .neq,
            .eq => null, // Not a range op
        };
    }
};

/// A range filter with encoded threshold value.
pub const RangeFilter = struct {
    /// The threshold value encoded as bytes (owned)
    bytes: []const u8,
    /// Comparison operator
    op: RangeOp,
    /// Parquet physical type
    parquet_type: schema.Type,
    /// Allocator for cleanup
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Parse a string value into an encoded range filter.
    pub fn parse(
        allocator: std.mem.Allocator,
        value_str: []const u8,
        op: RangeOp,
        parquet_type: schema.Type,
    ) !Self {
        const bytes = try encodeValue(allocator, value_str, parquet_type);
        return .{
            .bytes = bytes,
            .op = op,
            .parquet_type = parquet_type,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.bytes);
    }

    // =========================================================================
    // Page-Level Pruning (ColumnIndex)
    // =========================================================================

    /// Check if a page might contain matching values.
    /// Returns false if we can definitively skip this page.
    pub fn mightMatchPage(self: *const Self, column_index: *const page_index.ColumnIndex, page_idx: usize) bool {
        if (page_idx >= column_index.null_pages.len) return true;
        if (column_index.null_pages[page_idx]) return false; // All nulls

        const min_bytes = column_index.min_values[page_idx];
        const max_bytes = column_index.max_values[page_idx];

        return self.mightMatchRange(min_bytes, max_bytes);
    }

    /// Check if row group statistics indicate possible matches.
    pub fn mightMatchRowGroup(self: *const Self, stats: *const schema.Statistics) bool {
        const min_bytes = stats.min_value orelse return true;
        const max_bytes = stats.max_value orelse return true;

        return self.mightMatchRange(min_bytes, max_bytes);
    }

    /// Core range check: does [min, max] potentially satisfy our condition?
    fn mightMatchRange(self: *const Self, min_bytes: []const u8, max_bytes: []const u8) bool {
        return switch (self.parquet_type) {
            .INT32 => self.mightMatchRangeInt32(min_bytes, max_bytes),
            .INT64 => self.mightMatchRangeInt64(min_bytes, max_bytes),
            .FLOAT => self.mightMatchRangeFloat(min_bytes, max_bytes),
            .DOUBLE => self.mightMatchRangeDouble(min_bytes, max_bytes),
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => self.mightMatchRangeBytes(min_bytes, max_bytes),
            else => true, // Can't prune for other types
        };
    }

    fn mightMatchRangeInt32(self: *const Self, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 4 or min_bytes.len != 4 or max_bytes.len != 4) return true;

        const threshold = std.mem.readInt(i32, self.bytes[0..4], .little);
        const page_min = std.mem.readInt(i32, min_bytes[0..4], .little);
        const page_max = std.mem.readInt(i32, max_bytes[0..4], .little);

        return switch (self.op) {
            .gt => page_max > threshold, // skip if max <= threshold
            .gte => page_max >= threshold, // skip if max < threshold
            .lt => page_min < threshold, // skip if min >= threshold
            .lte => page_min <= threshold, // skip if min > threshold
            .neq => page_min != threshold or page_max != threshold, // skip if min == max == threshold
        };
    }

    fn mightMatchRangeInt64(self: *const Self, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 8 or min_bytes.len != 8 or max_bytes.len != 8) return true;

        const threshold = std.mem.readInt(i64, self.bytes[0..8], .little);
        const page_min = std.mem.readInt(i64, min_bytes[0..8], .little);
        const page_max = std.mem.readInt(i64, max_bytes[0..8], .little);

        return switch (self.op) {
            .gt => page_max > threshold,
            .gte => page_max >= threshold,
            .lt => page_min < threshold,
            .lte => page_min <= threshold,
            .neq => page_min != threshold or page_max != threshold,
        };
    }

    fn mightMatchRangeFloat(self: *const Self, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 4 or min_bytes.len != 4 or max_bytes.len != 4) return true;

        const threshold: f32 = @bitCast(std.mem.readInt(u32, self.bytes[0..4], .little));
        const page_min: f32 = @bitCast(std.mem.readInt(u32, min_bytes[0..4], .little));
        const page_max: f32 = @bitCast(std.mem.readInt(u32, max_bytes[0..4], .little));

        // Can't prune if NaN involved
        if (std.math.isNan(threshold) or std.math.isNan(page_min) or std.math.isNan(page_max)) return true;

        return switch (self.op) {
            .gt => page_max > threshold,
            .gte => page_max >= threshold,
            .lt => page_min < threshold,
            .lte => page_min <= threshold,
            .neq => page_min != threshold or page_max != threshold,
        };
    }

    fn mightMatchRangeDouble(self: *const Self, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 8 or min_bytes.len != 8 or max_bytes.len != 8) return true;

        const threshold: f64 = @bitCast(std.mem.readInt(u64, self.bytes[0..8], .little));
        const page_min: f64 = @bitCast(std.mem.readInt(u64, min_bytes[0..8], .little));
        const page_max: f64 = @bitCast(std.mem.readInt(u64, max_bytes[0..8], .little));

        if (std.math.isNan(threshold) or std.math.isNan(page_min) or std.math.isNan(page_max)) return true;

        return switch (self.op) {
            .gt => page_max > threshold,
            .gte => page_max >= threshold,
            .lt => page_min < threshold,
            .lte => page_min <= threshold,
            .neq => page_min != threshold or page_max != threshold,
        };
    }

    fn mightMatchRangeBytes(self: *const Self, min_bytes: []const u8, max_bytes: []const u8) bool {
        // Lexicographic comparison for strings
        return switch (self.op) {
            .gt => std.mem.order(u8, max_bytes, self.bytes) == .gt,
            .gte => std.mem.order(u8, max_bytes, self.bytes) != .lt,
            .lt => std.mem.order(u8, min_bytes, self.bytes) == .lt,
            .lte => std.mem.order(u8, min_bytes, self.bytes) != .gt,
            .neq => std.mem.order(u8, min_bytes, self.bytes) != .eq or std.mem.order(u8, max_bytes, self.bytes) != .eq,
        };
    }

    // =========================================================================
    // Row-Level Matching
    // =========================================================================

    /// Check if an INT32 value matches the filter.
    pub inline fn matchesInt32(self: *const Self, value: i32) bool {
        const threshold = std.mem.readInt(i32, self.bytes[0..4], .little);
        return switch (self.op) {
            .gt => value > threshold,
            .gte => value >= threshold,
            .lt => value < threshold,
            .lte => value <= threshold,
            .neq => value != threshold,
        };
    }

    /// Check if an INT64 value matches the filter.
    pub inline fn matchesInt64(self: *const Self, value: i64) bool {
        const threshold = std.mem.readInt(i64, self.bytes[0..8], .little);
        return switch (self.op) {
            .gt => value > threshold,
            .gte => value >= threshold,
            .lt => value < threshold,
            .lte => value <= threshold,
            .neq => value != threshold,
        };
    }

    /// Check if a FLOAT value matches the filter.
    pub inline fn matchesFloat(self: *const Self, value: f32) bool {
        const threshold: f32 = @bitCast(std.mem.readInt(u32, self.bytes[0..4], .little));
        return switch (self.op) {
            .gt => value > threshold,
            .gte => value >= threshold,
            .lt => value < threshold,
            .lte => value <= threshold,
            .neq => value != threshold,
        };
    }

    /// Check if a DOUBLE value matches the filter.
    pub inline fn matchesDouble(self: *const Self, value: f64) bool {
        const threshold: f64 = @bitCast(std.mem.readInt(u64, self.bytes[0..8], .little));
        return switch (self.op) {
            .gt => value > threshold,
            .gte => value >= threshold,
            .lt => value < threshold,
            .lte => value <= threshold,
            .neq => value != threshold,
        };
    }

    /// Check if a byte array (string) matches the filter.
    pub inline fn matchesBytes(self: *const Self, value: []const u8) bool {
        const order = std.mem.order(u8, value, self.bytes);
        return switch (self.op) {
            .gt => order == .gt,
            .gte => order != .lt,
            .lt => order == .lt,
            .lte => order != .gt,
            .neq => order != .eq,
        };
    }

    /// Generic match for any supported type (dispatches to specific matcher).
    pub fn matchesValue(self: *const Self, comptime T: type, value: T) bool {
        return switch (T) {
            i32 => self.matchesInt32(value),
            i64 => self.matchesInt64(value),
            f32 => self.matchesFloat(value),
            f64 => self.matchesDouble(value),
            []const u8 => self.matchesBytes(value),
            else => @compileError("Unsupported type for range filter"),
        };
    }
};

// =============================================================================
// Value Encoding Helpers
// =============================================================================

fn encodeValue(allocator: std.mem.Allocator, value_str: []const u8, parquet_type: schema.Type) ![]const u8 {
    return switch (parquet_type) {
        .INT32 => {
            const val = try std.fmt.parseInt(i32, value_str, 10);
            const bytes = try allocator.alloc(u8, 4);
            std.mem.writeInt(i32, bytes[0..4], val, .little);
            return bytes;
        },
        .INT64 => {
            const val = try std.fmt.parseInt(i64, value_str, 10);
            const bytes = try allocator.alloc(u8, 8);
            std.mem.writeInt(i64, bytes[0..8], val, .little);
            return bytes;
        },
        .FLOAT => {
            const val = try std.fmt.parseFloat(f32, value_str);
            const bytes = try allocator.alloc(u8, 4);
            @memcpy(bytes[0..4], std.mem.asBytes(&val));
            return bytes;
        },
        .DOUBLE => {
            const val = try std.fmt.parseFloat(f64, value_str);
            const bytes = try allocator.alloc(u8, 8);
            @memcpy(bytes[0..8], std.mem.asBytes(&val));
            return bytes;
        },
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
            return try allocator.dupe(u8, value_str);
        },
        else => error.UnsupportedTypeForRangeFilter,
    };
}

// =============================================================================
// Tests
// =============================================================================

test "RangeFilter INT32 greater than" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "100", .gt, .INT32);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt32(101));
    try std.testing.expect(filter.matchesInt32(1000));
    try std.testing.expect(!filter.matchesInt32(100)); // not >
    try std.testing.expect(!filter.matchesInt32(99));
    try std.testing.expect(!filter.matchesInt32(-50));
}

test "RangeFilter INT32 greater than or equal" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "100", .gte, .INT32);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt32(101));
    try std.testing.expect(filter.matchesInt32(100)); // included
    try std.testing.expect(!filter.matchesInt32(99));
}

test "RangeFilter INT32 less than" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "100", .lt, .INT32);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt32(99));
    try std.testing.expect(filter.matchesInt32(-50));
    try std.testing.expect(!filter.matchesInt32(100)); // not <
    try std.testing.expect(!filter.matchesInt32(101));
}

test "RangeFilter INT32 less than or equal" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "100", .lte, .INT32);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt32(99));
    try std.testing.expect(filter.matchesInt32(100)); // included
    try std.testing.expect(!filter.matchesInt32(101));
}

test "RangeFilter INT64" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "9999999999", .gt, .INT64);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt64(10000000000));
    try std.testing.expect(!filter.matchesInt64(9999999999));
    try std.testing.expect(!filter.matchesInt64(1000));
}

test "RangeFilter DOUBLE" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "3.14", .gte, .DOUBLE);
    defer filter.deinit();

    try std.testing.expect(filter.matchesDouble(3.14));
    try std.testing.expect(filter.matchesDouble(3.15));
    try std.testing.expect(!filter.matchesDouble(3.13));
}

test "RangeFilter string lexicographic" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "m", .gte, .BYTE_ARRAY);
    defer filter.deinit();

    try std.testing.expect(filter.matchesBytes("m"));
    try std.testing.expect(filter.matchesBytes("zebra"));
    try std.testing.expect(filter.matchesBytes("middle"));
    try std.testing.expect(!filter.matchesBytes("apple"));
    try std.testing.expect(!filter.matchesBytes("lemon"));
}

test "RangeFilter page pruning INT32 gt" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "100", .gt, .INT32);
    defer filter.deinit();

    // Page with values [50, 80] - max=80 <= 100, should skip
    var min1: [4]u8 = undefined;
    var max1: [4]u8 = undefined;
    std.mem.writeInt(i32, &min1, 50, .little);
    std.mem.writeInt(i32, &max1, 80, .little);

    var null_pages1 = [_]bool{false};
    var min_vals1 = [_][]const u8{&min1};
    var max_vals1 = [_][]const u8{&max1};

    const col_idx1 = page_index.ColumnIndex{
        .null_pages = &null_pages1,
        .min_values = &min_vals1,
        .max_values = &max_vals1,
        .boundary_order = .UNORDERED,
        .null_counts = null,
    };

    try std.testing.expect(!filter.mightMatchPage(&col_idx1, 0)); // Can skip!

    // Page with values [90, 150] - max=150 > 100, might match
    var min2: [4]u8 = undefined;
    var max2: [4]u8 = undefined;
    std.mem.writeInt(i32, &min2, 90, .little);
    std.mem.writeInt(i32, &max2, 150, .little);

    var null_pages2 = [_]bool{false};
    var min_vals2 = [_][]const u8{&min2};
    var max_vals2 = [_][]const u8{&max2};

    const col_idx2 = page_index.ColumnIndex{
        .null_pages = &null_pages2,
        .min_values = &min_vals2,
        .max_values = &max_vals2,
        .boundary_order = .UNORDERED,
        .null_counts = null,
    };

    try std.testing.expect(filter.mightMatchPage(&col_idx2, 0)); // Must check
}

test "RangeFilter page pruning INT32 lt" {
    const allocator = std.testing.allocator;

    var filter = try RangeFilter.parse(allocator, "100", .lt, .INT32);
    defer filter.deinit();

    // Page with values [150, 200] - min=150 >= 100, should skip
    var min1: [4]u8 = undefined;
    var max1: [4]u8 = undefined;
    std.mem.writeInt(i32, &min1, 150, .little);
    std.mem.writeInt(i32, &max1, 200, .little);

    var null_pages1 = [_]bool{false};
    var min_vals1 = [_][]const u8{&min1};
    var max_vals1 = [_][]const u8{&max1};

    const col_idx1 = page_index.ColumnIndex{
        .null_pages = &null_pages1,
        .min_values = &min_vals1,
        .max_values = &max_vals1,
        .boundary_order = .UNORDERED,
        .null_counts = null,
    };

    try std.testing.expect(!filter.mightMatchPage(&col_idx1, 0)); // Can skip!

    // Page with values [50, 120] - min=50 < 100, might match
    var min2: [4]u8 = undefined;
    var max2: [4]u8 = undefined;
    std.mem.writeInt(i32, &min2, 50, .little);
    std.mem.writeInt(i32, &max2, 120, .little);

    var null_pages2 = [_]bool{false};
    var min_vals2 = [_][]const u8{&min2};
    var max_vals2 = [_][]const u8{&max2};

    const col_idx2 = page_index.ColumnIndex{
        .null_pages = &null_pages2,
        .min_values = &min_vals2,
        .max_values = &max_vals2,
        .boundary_order = .UNORDERED,
        .null_counts = null,
    };

    try std.testing.expect(filter.mightMatchPage(&col_idx2, 0)); // Must check
}
