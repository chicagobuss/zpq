const std = @import("std");
const schema = @import("schema.zig");
const column = @import("column.zig");
const batch_reader = @import("batch_reader.zig");
const filter_mod = @import("filter.zig");
const selection_mod = @import("selection.zig");
const filter_cache_mod = @import("filter_cache.zig");

const SelectionVector = selection_mod.SelectionVector;
const FilterColumnCache = filter_cache_mod.FilterColumnCache;
const EncodedFilter = filter_mod.EncodedFilter;
const ColumnReader = column.ColumnReader;

/// Result of a filter scan operation
pub const FilterScanResult = struct {
    /// Row indices that matched the filter
    selection: SelectionVector,
    /// Cached values for matching rows (if filter column is in output)
    cache: ?FilterColumnCache,

    pub fn deinit(self: *FilterScanResult) void {
        self.selection.deinit();
        if (self.cache) |*c| c.deinit();
    }
};

/// Scan a filter column and build selection vector + optional value cache.
/// This is the key optimization: single-pass through filter column that:
/// 1. Applies the filter predicate
/// 2. Builds selection vector of matching row indices
/// 3. Optionally caches the matching values (if filter col is in output list)
pub fn scanFilterColumn(
    allocator: std.mem.Allocator,
    col_reader: ColumnReader,
    col_type: schema.Type,
    max_def_level: u16,
    max_rep_level: u16,
    type_length: ?i32,
    encoded_filter: *const EncodedFilter,
    num_rows: usize,
    cache_values: bool,
) !FilterScanResult {
    var result = FilterScanResult{
        .selection = SelectionVector.init(allocator),
        .cache = if (cache_values) FilterColumnCache.init(allocator, col_type) else null,
    };
    errdefer result.deinit();

    switch (col_type) {
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
            try scanByteArray(allocator, col_reader, col_type, max_def_level, max_rep_level, type_length, encoded_filter, num_rows, &result.selection, if (result.cache) |*c| c else null);
        },
        .INT32 => {
            try scanTyped(i32, allocator, col_reader, col_type, max_def_level, max_rep_level, type_length, encoded_filter, num_rows, &result.selection, if (result.cache) |*c| c else null);
        },
        .INT64 => {
            try scanTyped(i64, allocator, col_reader, col_type, max_def_level, max_rep_level, type_length, encoded_filter, num_rows, &result.selection, if (result.cache) |*c| c else null);
        },
        .FLOAT => {
            try scanTyped(f32, allocator, col_reader, col_type, max_def_level, max_rep_level, type_length, encoded_filter, num_rows, &result.selection, if (result.cache) |*c| c else null);
        },
        .DOUBLE => {
            try scanTyped(f64, allocator, col_reader, col_type, max_def_level, max_rep_level, type_length, encoded_filter, num_rows, &result.selection, if (result.cache) |*c| c else null);
        },
        .BOOLEAN => {
            try scanTyped(bool, allocator, col_reader, col_type, max_def_level, max_rep_level, type_length, encoded_filter, num_rows, &result.selection, if (result.cache) |*c| c else null);
        },
        else => return error.UnsupportedFilterType,
    }

    return result;
}

fn scanByteArray(
    allocator: std.mem.Allocator,
    col_reader: ColumnReader,
    col_type: schema.Type,
    max_def_level: u16,
    max_rep_level: u16,
    type_length: ?i32,
    encoded_filter: *const EncodedFilter,
    num_rows: usize,
    selection: *SelectionVector,
    cache: ?*FilterColumnCache,
) !void {
    var reader = batch_reader.BatchReader([]const u8).init(
        allocator,
        col_reader,
        col_type,
        max_def_level,
        max_rep_level,
        type_length,
    );
    defer reader.deinit();

    var row_idx: usize = 0;
    var buf: [1024]?[]const u8 = undefined;

    while (row_idx < num_rows) {
        const batch_size = @min(1024, num_rows - row_idx);
        const n_read = try reader.nextBatch(buf[0..batch_size]);
        if (n_read == 0) break;

        for (buf[0..n_read], 0..) |maybe_val, i| {
            if (maybe_val) |v| {
                if (encoded_filter.matchesBytes(v)) {
                    try selection.append(row_idx + i);
                    // Cache value if needed (must dupe - buffer is reused)
                    if (cache) |c| {
                        try c.appendByteArray(v);
                    }
                }
            }
        }
        row_idx += n_read;
    }
}

fn scanTyped(
    comptime T: type,
    allocator: std.mem.Allocator,
    col_reader: ColumnReader,
    col_type: schema.Type,
    max_def_level: u16,
    max_rep_level: u16,
    type_length: ?i32,
    encoded_filter: *const EncodedFilter,
    num_rows: usize,
    selection: *SelectionVector,
    cache: ?*FilterColumnCache,
) !void {
    var reader = batch_reader.BatchReader(T).init(
        allocator,
        col_reader,
        col_type,
        max_def_level,
        max_rep_level,
        type_length,
    );
    defer reader.deinit();

    var row_idx: usize = 0;
    var buf: [1024]?T = undefined;

    while (row_idx < num_rows) {
        const batch_size = @min(1024, num_rows - row_idx);
        const n_read = try reader.nextBatch(buf[0..batch_size]);
        if (n_read == 0) break;

        for (buf[0..n_read], 0..) |maybe_val, i| {
            if (maybe_val) |v| {
                const value_bytes = std.mem.asBytes(&v);
                if (encoded_filter.matchesBytes(value_bytes)) {
                    try selection.append(row_idx + i);
                    // Cache value if needed
                    if (cache) |c| {
                        if (T == i32) {
                            try c.appendInt32(v);
                        } else if (T == i64) {
                            try c.appendInt64(v);
                        } else if (T == f32) {
                            try c.appendFloat(v);
                        } else if (T == f64) {
                            try c.appendDouble(v);
                        } else if (T == bool) {
                            try c.appendBool(v);
                        }
                    }
                }
            }
        }
        row_idx += n_read;
    }
}

// Tests require a real parquet file, so we test the helper structures
// and integration test with actual files in main.zig

test "FilterScanResult init and deinit" {
    const allocator = std.testing.allocator;

    // Test without cache
    {
        var result = FilterScanResult{
            .selection = SelectionVector.init(allocator),
            .cache = null,
        };
        defer result.deinit();

        try result.selection.append(5);
        try result.selection.append(10);
        try std.testing.expectEqual(@as(usize, 2), result.selection.count());
    }

    // Test with cache
    {
        var result = FilterScanResult{
            .selection = SelectionVector.init(allocator),
            .cache = FilterColumnCache.init(allocator, .INT32),
        };
        defer result.deinit();

        try result.selection.append(5);
        try result.cache.?.appendInt32(42);

        try std.testing.expectEqual(@as(usize, 1), result.selection.count());
        try std.testing.expectEqual(@as(usize, 1), result.cache.?.count());
    }
}
