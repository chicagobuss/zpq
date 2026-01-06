//! Unified Filter Interface
//!
//! Provides a common interface for all filter types (equality, range, null, between, etc.)
//! Used by filter_scan and surgical engine for consistent filtering.

const std = @import("std");
const schema = @import("../schema.zig");
const page_index = @import("../page_index.zig");
const encoded_filter = @import("../filter.zig");
const range_mod = @import("range.zig");
const null_mod = @import("null.zig");
const between_mod = @import("between.zig");

pub const RangeFilter = range_mod.RangeFilter;
pub const RangeOp = range_mod.RangeOp;
pub const EncodedFilter = encoded_filter.EncodedFilter;
pub const NullFilter = null_mod.NullFilter;
pub const NullOp = null_mod.NullOp;
pub const BetweenFilter = between_mod.BetweenFilter;

/// Filter operator enum (matches pipeline.Predicate.Operator)
pub const FilterOp = enum {
    eq,
    gt,
    gte,
    lt,
    lte,
    is_null,
    is_not_null,
    between,
    neq,
};

/// Unified filter that can be equality, range, null, or between based.
pub const Filter = union(enum) {
    equality: EncodedFilter,
    range: RangeFilter,
    null_check: NullFilter,
    between: BetweenFilter,

    const Self = @This();

    /// Create a filter from a predicate (for single-value operators).
    pub fn fromPredicate(
        allocator: std.mem.Allocator,
        op: FilterOp,
        value: []const u8,
        parquet_type: schema.Type,
    ) !Self {
        return switch (op) {
            .eq => .{ .equality = try EncodedFilter.parse(allocator, value, parquet_type) },
            .gt => .{ .range = try RangeFilter.parse(allocator, value, .gt, parquet_type) },
            .gte => .{ .range = try RangeFilter.parse(allocator, value, .gte, parquet_type) },
            .lt => .{ .range = try RangeFilter.parse(allocator, value, .lt, parquet_type) },
            .lte => .{ .range = try RangeFilter.parse(allocator, value, .lte, parquet_type) },
            .is_null => .{ .null_check = NullFilter.init(.is_null, parquet_type) },
            .is_not_null => .{ .null_check = NullFilter.init(.is_not_null, parquet_type) },
            .between => error.BetweenRequiresTwoValues,
            .neq => .{ .range = try RangeFilter.parse(allocator, value, .neq, parquet_type) },
        };
    }

    /// Create a BETWEEN filter with low and high values.
    pub fn fromBetween(
        allocator: std.mem.Allocator,
        low_value: []const u8,
        high_value: []const u8,
        parquet_type: schema.Type,
    ) !Self {
        return .{ .between = try BetweenFilter.parse(allocator, low_value, high_value, parquet_type) };
    }

    pub fn deinit(self: *Self) void {
        switch (self.*) {
            .equality => |*f| f.deinit(),
            .range => |*f| f.deinit(),
            .null_check => |*f| f.deinit(),
            .between => |*f| f.deinit(),
        }
    }

    // =========================================================================
    // Page-Level Pruning
    // =========================================================================

    /// Check if a page might contain matching values.
    pub fn mightMatchPage(self: *const Self, column_index: *const page_index.ColumnIndex, page_idx: usize) bool {
        return switch (self.*) {
            .equality => |*f| f.mightContainInPage(column_index, page_idx),
            .range => |*f| f.mightMatchPage(column_index, page_idx),
            .null_check => |*f| f.mightMatchPage(column_index, page_idx),
            .between => |*f| f.mightMatchPage(column_index, page_idx),
        };
    }

    /// Check if row group statistics indicate possible matches.
    pub fn mightMatchRowGroup(self: *const Self, stats: *const schema.Statistics) bool {
        return switch (self.*) {
            .equality => |*f| f.mightContainInRowGroup(stats),
            .range => |*f| f.mightMatchRowGroup(stats),
            .null_check => |*f| f.mightMatchRowGroup(stats),
            .between => |*f| f.mightMatchRowGroup(stats),
        };
    }

    // =========================================================================
    // Row-Level Matching
    // =========================================================================

    /// Check if an INT32 value matches the filter.
    pub inline fn matchesInt32(self: *const Self, value: i32) bool {
        return switch (self.*) {
            .equality => |*f| {
                const bytes = std.mem.asBytes(&value);
                return f.matchesBytes(bytes);
            },
            .range => |*f| f.matchesInt32(value),
            .null_check => |*f| f.matchesInt32(value),
            .between => |*f| f.matchesInt32(value),
        };
    }

    /// Check if an INT64 value matches the filter.
    pub inline fn matchesInt64(self: *const Self, value: i64) bool {
        return switch (self.*) {
            .equality => |*f| {
                const bytes = std.mem.asBytes(&value);
                return f.matchesBytes(bytes);
            },
            .range => |*f| f.matchesInt64(value),
            .null_check => |*f| f.matchesInt64(value),
            .between => |*f| f.matchesInt64(value),
        };
    }

    /// Check if a FLOAT value matches the filter.
    pub inline fn matchesFloat(self: *const Self, value: f32) bool {
        return switch (self.*) {
            .equality => |*f| {
                const bytes = std.mem.asBytes(&value);
                return f.matchesBytes(bytes);
            },
            .range => |*f| f.matchesFloat(value),
            .null_check => |*f| f.matchesFloat(value),
            .between => |*f| f.matchesFloat(value),
        };
    }

    /// Check if a DOUBLE value matches the filter.
    pub inline fn matchesDouble(self: *const Self, value: f64) bool {
        return switch (self.*) {
            .equality => |*f| {
                const bytes = std.mem.asBytes(&value);
                return f.matchesBytes(bytes);
            },
            .range => |*f| f.matchesDouble(value),
            .null_check => |*f| f.matchesDouble(value),
            .between => |*f| f.matchesDouble(value),
        };
    }

    /// Check if a byte array (string) matches the filter.
    pub inline fn matchesBytes(self: *const Self, value: []const u8) bool {
        return switch (self.*) {
            .equality => |*f| f.matchesBytes(value),
            .range => |*f| f.matchesBytes(value),
            .null_check => |*f| f.matchesBytes(value),
            .between => |*f| f.matchesBytes(value),
        };
    }

    /// Check if a boolean value matches the filter.
    pub inline fn matchesBool(self: *const Self, value: bool) bool {
        return switch (self.*) {
            .equality => |*f| {
                const byte: u8 = if (value) 1 else 0;
                return f.matchesBytes(&[_]u8{byte});
            },
            .range => false, // Range filters don't make sense for booleans
            .null_check => |*f| f.matchesBool(value),
            .between => false, // BETWEEN doesn't make sense for booleans
        };
    }

    /// Check if a null value matches the filter.
    /// Called when def_level < max_def_level (value is null).
    pub inline fn matchesNull(self: *const Self) bool {
        return switch (self.*) {
            .equality => false, // Equality never matches null
            .range => false, // Range never matches null
            .null_check => |*f| f.matchesNull(),
            .between => false, // BETWEEN never matches null
        };
    }

    /// Get the underlying EncodedFilter for backwards compatibility.
    /// Returns null if this is a range, null, or between filter.
    pub fn asEncodedFilter(self: *const Self) ?*const EncodedFilter {
        return switch (self.*) {
            .equality => |*f| f,
            .range => null,
            .null_check => null,
            .between => null,
        };
    }

    /// Check if this is an equality filter.
    pub fn isEquality(self: *const Self) bool {
        return switch (self.*) {
            .equality => true,
            .range => false,
            .null_check => false,
            .between => false,
        };
    }

    /// Check if this is a null check filter.
    pub fn isNullCheck(self: *const Self) bool {
        return switch (self.*) {
            .equality => false,
            .range => false,
            .null_check => true,
            .between => false,
        };
    }

    /// Check if this is a between filter.
    pub fn isBetween(self: *const Self) bool {
        return switch (self.*) {
            .equality => false,
            .range => false,
            .null_check => false,
            .between => true,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Filter equality INT32" {
    const allocator = std.testing.allocator;

    var filter = try Filter.fromPredicate(allocator, .eq, "42", .INT32);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt32(42));
    try std.testing.expect(!filter.matchesInt32(41));
    try std.testing.expect(!filter.matchesInt32(43));
}

test "Filter range INT32 gt" {
    const allocator = std.testing.allocator;

    var filter = try Filter.fromPredicate(allocator, .gt, "100", .INT32);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt32(101));
    try std.testing.expect(!filter.matchesInt32(100));
    try std.testing.expect(!filter.matchesInt32(99));
}

test "Filter range string gte" {
    const allocator = std.testing.allocator;

    var filter = try Filter.fromPredicate(allocator, .gte, "m", .BYTE_ARRAY);
    defer filter.deinit();

    try std.testing.expect(filter.matchesBytes("m"));
    try std.testing.expect(filter.matchesBytes("zebra"));
    try std.testing.expect(!filter.matchesBytes("apple"));
}

test "Filter IS NULL" {
    const allocator = std.testing.allocator;

    var filter = try Filter.fromPredicate(allocator, .is_null, "", .INT32);
    defer filter.deinit();

    // Non-null values should not match IS NULL
    try std.testing.expect(!filter.matchesInt32(42));
    try std.testing.expect(!filter.matchesInt64(42));

    // Null should match IS NULL
    try std.testing.expect(filter.matchesNull());
}

test "Filter IS NOT NULL" {
    const allocator = std.testing.allocator;

    var filter = try Filter.fromPredicate(allocator, .is_not_null, "", .INT32);
    defer filter.deinit();

    // Non-null values should match IS NOT NULL
    try std.testing.expect(filter.matchesInt32(42));
    try std.testing.expect(filter.matchesInt64(42));

    // Null should not match IS NOT NULL
    try std.testing.expect(!filter.matchesNull());
}

test "Filter BETWEEN INT32" {
    const allocator = std.testing.allocator;

    var filter = try Filter.fromBetween(allocator, "10", "20", .INT32);
    defer filter.deinit();

    try std.testing.expect(filter.matchesInt32(10));
    try std.testing.expect(filter.matchesInt32(15));
    try std.testing.expect(filter.matchesInt32(20));
    try std.testing.expect(!filter.matchesInt32(9));
    try std.testing.expect(!filter.matchesInt32(21));
}
