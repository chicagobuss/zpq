//! Encoded Filter for Predicate Pushdown
//!
//! Key insight: For equality predicates, we can compare raw encoded bytes
//! instead of decoding to native types. This enables a single code path
//! for value matching across all fixed-width types.
//!
//! For range checks (page-level skip via min/max), we need type-aware
//! comparison because signed integers have different byte vs numeric ordering.

const std = @import("std");
const schema = @import("schema.zig");
const page_index = @import("page_index.zig");

/// A filter value encoded as raw bytes in Parquet's format.
/// Enables unified byte-level comparison for equality predicates.
pub const EncodedFilter = struct {
    /// The filter value encoded as bytes (owned, must be freed)
    bytes: []const u8,
    /// Original Parquet physical type (needed for range comparisons)
    parquet_type: schema.Type,
    /// Allocator used for bytes (null if bytes are borrowed)
    allocator: ?std.mem.Allocator,

    /// Parse a string filter value into encoded bytes for the given Parquet type.
    pub fn parse(allocator: std.mem.Allocator, filter_str: []const u8, parquet_type: schema.Type) !EncodedFilter {
        return switch (parquet_type) {
            .INT64 => {
                const val = try std.fmt.parseInt(i64, filter_str, 10);
                const bytes = try allocator.alloc(u8, 8);
                std.mem.writeInt(i64, bytes[0..8], val, .little);
                return .{
                    .bytes = bytes,
                    .parquet_type = parquet_type,
                    .allocator = allocator,
                };
            },
            .INT32 => {
                const val = try std.fmt.parseInt(i32, filter_str, 10);
                const bytes = try allocator.alloc(u8, 4);
                std.mem.writeInt(i32, bytes[0..4], val, .little);
                return .{
                    .bytes = bytes,
                    .parquet_type = parquet_type,
                    .allocator = allocator,
                };
            },
            .FLOAT => {
                const val = try std.fmt.parseFloat(f32, filter_str);
                const bytes = try allocator.alloc(u8, 4);
                @memcpy(bytes[0..4], std.mem.asBytes(&val));
                return .{
                    .bytes = bytes,
                    .parquet_type = parquet_type,
                    .allocator = allocator,
                };
            },
            .DOUBLE => {
                const val = try std.fmt.parseFloat(f64, filter_str);
                const bytes = try allocator.alloc(u8, 8);
                @memcpy(bytes[0..8], std.mem.asBytes(&val));
                return .{
                    .bytes = bytes,
                    .parquet_type = parquet_type,
                    .allocator = allocator,
                };
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                // For strings, just copy the bytes
                const bytes = try allocator.dupe(u8, filter_str);
                return .{
                    .bytes = bytes,
                    .parquet_type = parquet_type,
                    .allocator = allocator,
                };
            },
            .BOOLEAN => {
                const bytes = try allocator.alloc(u8, 1);
                if (std.mem.eql(u8, filter_str, "true") or std.mem.eql(u8, filter_str, "1")) {
                    bytes[0] = 1;
                } else if (std.mem.eql(u8, filter_str, "false") or std.mem.eql(u8, filter_str, "0")) {
                    bytes[0] = 0;
                } else {
                    allocator.free(bytes);
                    return error.InvalidBooleanValue;
                }
                return .{
                    .bytes = bytes,
                    .parquet_type = parquet_type,
                    .allocator = allocator,
                };
            },
            .INT96 => {
                // INT96 is 12 bytes, typically used for timestamps
                // For now, require hex input or return error
                return error.Int96FilterNotSupported;
            },
        };
    }

    pub fn deinit(self: *EncodedFilter) void {
        if (self.allocator) |alloc| {
            alloc.free(self.bytes);
        }
    }

    /// Check if the encoded value equals the given raw bytes.
    /// This is the hot path - just a memcmp!
    pub inline fn matchesBytes(self: *const EncodedFilter, value_bytes: []const u8) bool {
        return std.mem.eql(u8, self.bytes, value_bytes);
    }

    /// Check if a page might contain matching values using ColumnIndex.
    /// This uses type-aware comparison for min/max range checks.
    pub fn mightContainInPage(self: *const EncodedFilter, column_index: *const page_index.ColumnIndex, page_idx: usize) bool {
        if (page_idx >= column_index.null_pages.len) return true; // No stats, can't skip
        if (column_index.null_pages[page_idx]) return false; // All nulls, no match

        const min_bytes = column_index.min_values[page_idx];
        const max_bytes = column_index.max_values[page_idx];

        return switch (self.parquet_type) {
            .INT64 => self.mightContainInt64(min_bytes, max_bytes),
            .INT32 => self.mightContainInt32(min_bytes, max_bytes),
            .FLOAT => self.mightContainFloat(min_bytes, max_bytes),
            .DOUBLE => self.mightContainDouble(min_bytes, max_bytes),
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => self.mightContainBytes(min_bytes, max_bytes),
            .BOOLEAN => true, // Boolean pages are tiny, don't bother skipping
            .INT96 => true, // Not supported for skip
        };
    }

    /// Check if row group statistics indicate the filter value might exist.
    pub fn mightContainInRowGroup(self: *const EncodedFilter, stats: *const schema.Statistics) bool {
        const min_bytes = stats.min_value orelse return true;
        const max_bytes = stats.max_value orelse return true;

        return switch (self.parquet_type) {
            .INT64 => self.mightContainInt64(min_bytes, max_bytes),
            .INT32 => self.mightContainInt32(min_bytes, max_bytes),
            .FLOAT => self.mightContainFloat(min_bytes, max_bytes),
            .DOUBLE => self.mightContainDouble(min_bytes, max_bytes),
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => self.mightContainBytes(min_bytes, max_bytes),
            .BOOLEAN => true,
            .INT96 => true,
        };
    }

    // --- Type-specific range checks (handles signed ordering correctly) ---

    fn mightContainInt64(self: *const EncodedFilter, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 8 or min_bytes.len != 8 or max_bytes.len != 8) return true;

        const filter_val = std.mem.readInt(i64, self.bytes[0..8], .little);
        const min_val = std.mem.readInt(i64, min_bytes[0..8], .little);
        const max_val = std.mem.readInt(i64, max_bytes[0..8], .little);

        return filter_val >= min_val and filter_val <= max_val;
    }

    fn mightContainInt32(self: *const EncodedFilter, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 4 or min_bytes.len != 4 or max_bytes.len != 4) return true;

        const filter_val = std.mem.readInt(i32, self.bytes[0..4], .little);
        const min_val = std.mem.readInt(i32, min_bytes[0..4], .little);
        const max_val = std.mem.readInt(i32, max_bytes[0..4], .little);

        return filter_val >= min_val and filter_val <= max_val;
    }

    fn mightContainFloat(self: *const EncodedFilter, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 4 or min_bytes.len != 4 or max_bytes.len != 4) return true;

        const filter_val: f32 = @bitCast(std.mem.readInt(u32, self.bytes[0..4], .little));
        const min_val: f32 = @bitCast(std.mem.readInt(u32, min_bytes[0..4], .little));
        const max_val: f32 = @bitCast(std.mem.readInt(u32, max_bytes[0..4], .little));

        // Handle NaN - can't skip if any value might be NaN
        if (std.math.isNan(filter_val) or std.math.isNan(min_val) or std.math.isNan(max_val)) return true;

        return filter_val >= min_val and filter_val <= max_val;
    }

    fn mightContainDouble(self: *const EncodedFilter, min_bytes: []const u8, max_bytes: []const u8) bool {
        if (self.bytes.len != 8 or min_bytes.len != 8 or max_bytes.len != 8) return true;

        const filter_val: f64 = @bitCast(std.mem.readInt(u64, self.bytes[0..8], .little));
        const min_val: f64 = @bitCast(std.mem.readInt(u64, min_bytes[0..8], .little));
        const max_val: f64 = @bitCast(std.mem.readInt(u64, max_bytes[0..8], .little));

        // Handle NaN
        if (std.math.isNan(filter_val) or std.math.isNan(min_val) or std.math.isNan(max_val)) return true;

        return filter_val >= min_val and filter_val <= max_val;
    }

    fn mightContainBytes(self: *const EncodedFilter, min_bytes: []const u8, max_bytes: []const u8) bool {
        // For byte arrays, lexicographic comparison works correctly
        if (std.mem.lessThan(u8, self.bytes, min_bytes)) return false;
        if (std.mem.lessThan(u8, max_bytes, self.bytes)) return false;
        return true;
    }
};

test "EncodedFilter INT64 parsing" {
    const allocator = std.testing.allocator;

    var filter = try EncodedFilter.parse(allocator, "12345", .INT64);
    defer filter.deinit();

    // Verify encoding
    const decoded = std.mem.readInt(i64, filter.bytes[0..8], .little);
    try std.testing.expectEqual(@as(i64, 12345), decoded);
}

test "EncodedFilter INT64 negative" {
    const allocator = std.testing.allocator;

    var filter = try EncodedFilter.parse(allocator, "-999", .INT64);
    defer filter.deinit();

    const decoded = std.mem.readInt(i64, filter.bytes[0..8], .little);
    try std.testing.expectEqual(@as(i64, -999), decoded);
}

test "EncodedFilter matchesBytes" {
    const allocator = std.testing.allocator;

    var filter = try EncodedFilter.parse(allocator, "42", .INT64);
    defer filter.deinit();

    // Create matching bytes
    var match_bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &match_bytes, 42, .little);

    var nomatch_bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &nomatch_bytes, 43, .little);

    try std.testing.expect(filter.matchesBytes(&match_bytes));
    try std.testing.expect(!filter.matchesBytes(&nomatch_bytes));
}

test "EncodedFilter string" {
    const allocator = std.testing.allocator;

    var filter = try EncodedFilter.parse(allocator, "hello", .BYTE_ARRAY);
    defer filter.deinit();

    try std.testing.expect(filter.matchesBytes("hello"));
    try std.testing.expect(!filter.matchesBytes("world"));
}
