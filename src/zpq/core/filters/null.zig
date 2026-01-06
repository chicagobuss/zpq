//! NULL Filter Implementation
//!
//! Provides IS NULL and IS NOT NULL filters for Parquet queries.
//! These filters use definition levels and statistics to optimize filtering.

const std = @import("std");
const schema = @import("../schema.zig");
const page_index = @import("../page_index.zig");

/// NULL filter operator
pub const NullOp = enum {
    is_null,
    is_not_null,
};

/// Filter for NULL/NOT NULL checks.
/// Unlike other filters, this doesn't need a value - just an operator.
pub const NullFilter = struct {
    op: NullOp,
    parquet_type: schema.Type,

    const Self = @This();

    pub fn init(op: NullOp, parquet_type: schema.Type) Self {
        return .{
            .op = op,
            .parquet_type = parquet_type,
        };
    }

    pub fn deinit(self: *Self) void {
        // Nothing to free - no allocations
        _ = self;
    }

    // =========================================================================
    // Page-Level Pruning
    // =========================================================================

    /// Check if a page might contain matching values based on null_counts.
    ///
    /// For IS NULL:
    ///   - Skip if page has 0 nulls (null_count == 0)
    ///
    /// For IS NOT NULL:
    ///   - Skip if page is all nulls (null_count == num_values)
    pub fn mightMatchPage(
        self: *const Self,
        column_index: *const page_index.ColumnIndex,
        page_idx: usize,
    ) bool {
        // Need null_counts to prune
        const null_counts = column_index.null_counts orelse return true;
        if (page_idx >= null_counts.len) return true;

        const null_count = null_counts[page_idx];

        return switch (self.op) {
            .is_null => {
                // Page might have nulls if null_count > 0
                return null_count > 0;
            },
            .is_not_null => {
                // Page might have non-nulls if null_count < total values in page
                // Unfortunately ColumnIndex doesn't store row counts per page directly,
                // so we can only skip if null_count is 0 (meaning no nulls to exclude)
                // Actually, we want to skip if ALL values are null.
                // We need num_values per page to determine this.
                // Without it, we can't prune for IS NOT NULL at page level.
                // But if null_count == 0, all values are non-null, so we definitely match.
                // If null_count > 0, we still might match (some non-nulls).
                // We can only prune if we know the page has ALL nulls.
                // Without page row counts, assume we might match.
                return true;
            },
        };
    }

    /// Check if row group statistics indicate possible matches.
    ///
    /// For IS NULL:
    ///   - Skip if null_count == 0
    ///
    /// For IS NOT NULL:
    ///   - Skip if null_count == num_values (all nulls)
    ///   - Unfortunately we don't always have num_values in statistics,
    ///     so we can only skip if we're sure all are null.
    pub fn mightMatchRowGroup(self: *const Self, stats: *const schema.Statistics) bool {
        return switch (self.op) {
            .is_null => {
                // Skip if definitely no nulls
                if (stats.null_count) |nc| {
                    return nc > 0;
                }
                // No null_count info - assume might have nulls
                return true;
            },
            .is_not_null => {
                // Can't easily skip without knowing total row count
                // We'd need null_count == num_values to skip
                // For now, assume might have non-nulls
                return true;
            },
        };
    }

    // =========================================================================
    // Row-Level Matching
    // =========================================================================
    // Note: These are not used directly. Instead, the row_group_worker
    // checks definition levels to determine null status.
    // A def_level < max_def_level means NULL.
    // A def_level == max_def_level means NOT NULL.
    //
    // The matches* functions are provided for interface consistency,
    // but they always return based on the operator:
    // - IS NULL: return false (we're testing a non-null value)
    // - IS NOT NULL: return true (the value exists, so it's not null)

    /// For a non-null INT32 value.
    pub inline fn matchesInt32(self: *const Self, _: i32) bool {
        return self.matchesNonNull();
    }

    /// For a non-null INT64 value.
    pub inline fn matchesInt64(self: *const Self, _: i64) bool {
        return self.matchesNonNull();
    }

    /// For a non-null FLOAT value.
    pub inline fn matchesFloat(self: *const Self, _: f32) bool {
        return self.matchesNonNull();
    }

    /// For a non-null DOUBLE value.
    pub inline fn matchesDouble(self: *const Self, _: f64) bool {
        return self.matchesNonNull();
    }

    /// For a non-null byte array value.
    pub inline fn matchesBytes(self: *const Self, _: []const u8) bool {
        return self.matchesNonNull();
    }

    /// For a non-null boolean value.
    pub inline fn matchesBool(self: *const Self, _: bool) bool {
        return self.matchesNonNull();
    }

    /// Called when we have a non-null value.
    /// IS NULL should return false (value is not null).
    /// IS NOT NULL should return true (value is not null).
    inline fn matchesNonNull(self: *const Self) bool {
        return self.op == .is_not_null;
    }

    /// Called when we have a null value (def_level < max_def_level).
    /// IS NULL should return true.
    /// IS NOT NULL should return false.
    pub inline fn matchesNull(self: *const Self) bool {
        return self.op == .is_null;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "NullFilter IS NULL row matching" {
    const filter = NullFilter.init(.is_null, .INT32);

    // Non-null values should not match IS NULL
    try std.testing.expect(!filter.matchesInt32(42));
    try std.testing.expect(!filter.matchesInt64(42));
    try std.testing.expect(!filter.matchesBytes("hello"));

    // Null should match IS NULL
    try std.testing.expect(filter.matchesNull());
}

test "NullFilter IS NOT NULL row matching" {
    const filter = NullFilter.init(.is_not_null, .INT32);

    // Non-null values should match IS NOT NULL
    try std.testing.expect(filter.matchesInt32(42));
    try std.testing.expect(filter.matchesInt64(42));
    try std.testing.expect(filter.matchesBytes("hello"));

    // Null should not match IS NOT NULL
    try std.testing.expect(!filter.matchesNull());
}

test "NullFilter page pruning IS NULL" {
    const allocator = std.testing.allocator;

    const filter = NullFilter.init(.is_null, .INT32);

    // Page with no nulls - should be skipped
    var null_counts_no_nulls = [_]i64{0};
    const col_idx_no_nulls = page_index.ColumnIndex{
        .min_values = &[_][]const u8{},
        .max_values = &[_][]const u8{},
        .null_pages = &[_]bool{},
        .boundary_order = .UNORDERED,
        .null_counts = null_counts_no_nulls[0..],
    };
    _ = allocator;

    try std.testing.expect(!filter.mightMatchPage(&col_idx_no_nulls, 0));

    // Page with some nulls - should not be skipped
    var null_counts_some_nulls = [_]i64{5};
    const col_idx_some_nulls = page_index.ColumnIndex{
        .min_values = &[_][]const u8{},
        .max_values = &[_][]const u8{},
        .null_pages = &[_]bool{},
        .boundary_order = .UNORDERED,
        .null_counts = null_counts_some_nulls[0..],
    };

    try std.testing.expect(filter.mightMatchPage(&col_idx_some_nulls, 0));
}

test "NullFilter row group pruning IS NULL" {
    const filter = NullFilter.init(.is_null, .INT32);

    // Row group with no nulls - should be skipped
    const stats_no_nulls = schema.Statistics{
        .null_count = 0,
    };
    try std.testing.expect(!filter.mightMatchRowGroup(&stats_no_nulls));

    // Row group with some nulls - should not be skipped
    const stats_some_nulls = schema.Statistics{
        .null_count = 10,
    };
    try std.testing.expect(filter.mightMatchRowGroup(&stats_some_nulls));

    // Row group with unknown null count - should not be skipped (conservative)
    const stats_unknown = schema.Statistics{};
    try std.testing.expect(filter.mightMatchRowGroup(&stats_unknown));
}
