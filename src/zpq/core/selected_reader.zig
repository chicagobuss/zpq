//! Selected Row Reader
//!
//! Efficiently reads only selected rows from a column using skip-based access.
//! Uses BatchReader.skip() to jump over unselected rows without decoding them.

const std = @import("std");
const schema = @import("schema.zig");
const column = @import("column.zig");
const batch_reader = @import("batch_reader.zig");
const selection = @import("selection.zig");
const SelectionVector = selection.SelectionVector;
const ColumnReader = column.ColumnReader;

/// Result of reading selected rows - contains values for all selected indices.
/// Values are stored in selection order (i.e., values[0] corresponds to selection.items()[0]).
pub fn SelectedValues(comptime T: type) type {
    return struct {
        const Self = @This();

        values: std.ArrayListUnmanaged(?T),
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .values = .{},
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            // For []const u8, we need to free owned strings
            if (T == []const u8) {
                for (self.values.items) |maybe_val| {
                    if (maybe_val) |val| {
                        self.allocator.free(val);
                    }
                }
            }
            self.values.deinit(self.allocator);
        }

        pub fn items(self: Self) []const ?T {
            return self.values.items;
        }

        pub fn count(self: Self) usize {
            return self.values.items.len;
        }
    };
}

/// Read only the selected rows from a column.
///
/// This function efficiently reads values at the indices specified in the selection vector.
/// It uses skip() to jump over unselected rows, avoiding full decode of skipped data.
///
/// Parameters:
///   - allocator: Memory allocator for output values
///   - col_reader: ColumnReader positioned at start of column data
///   - col_type: Parquet physical type of the column
///   - max_def_level: Maximum definition level (0 = non-nullable)
///   - max_rep_level: Maximum repetition level (0 = no nesting)
///   - type_length: For FIXED_LEN_BYTE_ARRAY, the fixed length
///   - sel: Selection vector containing sorted row indices to read
///   - total_rows: Total number of rows in the row group
///
/// Returns: SelectedValues containing values in selection order
pub fn readSelectedRows(
    comptime T: type,
    allocator: std.mem.Allocator,
    col_reader: ColumnReader,
    col_type: schema.Type,
    max_def_level: u16,
    max_rep_level: u16,
    type_length: ?i32,
    sel: *const SelectionVector,
    total_rows: usize,
) !SelectedValues(T) {
    var result = SelectedValues(T).init(allocator);
    errdefer result.deinit();

    // Pre-allocate for efficiency
    try result.values.ensureTotalCapacity(allocator, sel.count());

    // Create batch reader
    var reader = batch_reader.BatchReader(T).init(
        allocator,
        col_reader,
        col_type,
        max_def_level,
        max_rep_level,
        type_length,
    );
    defer reader.deinit();

    // Process selection vector - indices are sorted
    const indices = sel.items();
    var current_row: usize = 0;
    var value_buf: [1]?T = undefined;

    for (indices) |target_idx| {
        // Skip rows between current position and target
        if (target_idx > current_row) {
            const skip_count = target_idx - current_row;
            try reader.skip(skip_count);
            current_row = target_idx;
        }

        // Read the single value at target_idx
        const n = try reader.nextBatch(&value_buf);
        if (n == 0) {
            // Unexpected end of data
            return error.UnexpectedEndOfData;
        }

        // For byte arrays, we need to copy the data since it references page memory
        if (T == []const u8) {
            if (value_buf[0]) |val| {
                const owned = try allocator.dupe(u8, val);
                try result.values.append(allocator, owned);
            } else {
                try result.values.append(allocator, null);
            }
        } else {
            try result.values.append(allocator, value_buf[0]);
        }

        current_row += 1;
    }

    // Skip remaining rows (not strictly necessary, but keeps reader in valid state)
    _ = total_rows; // We could skip to end, but not needed for correctness

    return result;
}

/// Optimized version that reads selected rows from cached filter column values.
/// When the filter column is also in the output, we already have its values cached.
///
/// This avoids re-reading the filter column entirely.
pub fn readFromCache(
    comptime T: type,
    allocator: std.mem.Allocator,
    cached_values: []const T,
    sel: *const SelectionVector,
) !SelectedValues(T) {
    var result = SelectedValues(T).init(allocator);
    errdefer result.deinit();

    try result.values.ensureTotalCapacity(allocator, sel.count());

    const indices = sel.items();

    for (indices) |idx| {
        if (idx >= cached_values.len) {
            return error.IndexOutOfBounds;
        }

        // For byte arrays, we need to copy
        if (T == []const u8) {
            const owned = try allocator.dupe(u8, cached_values[idx]);
            try result.values.append(allocator, owned);
        } else {
            try result.values.append(allocator, cached_values[idx]);
        }
    }

    return result;
}

// ============================================================================
// Tests
// ============================================================================

test "SelectedValues - basic operations" {
    const allocator = std.testing.allocator;

    var sv = SelectedValues(i32).init(allocator);
    defer sv.deinit();

    try sv.values.append(allocator, 10);
    try sv.values.append(allocator, null);
    try sv.values.append(allocator, 30);

    try std.testing.expectEqual(@as(usize, 3), sv.count());
    try std.testing.expectEqual(@as(?i32, 10), sv.items()[0]);
    try std.testing.expectEqual(@as(?i32, null), sv.items()[1]);
    try std.testing.expectEqual(@as(?i32, 30), sv.items()[2]);
}

test "SelectedValues - string cleanup" {
    const allocator = std.testing.allocator;

    var sv = SelectedValues([]const u8).init(allocator);
    defer sv.deinit();

    const s1 = try allocator.dupe(u8, "hello");
    const s2 = try allocator.dupe(u8, "world");

    try sv.values.append(allocator, s1);
    try sv.values.append(allocator, null);
    try sv.values.append(allocator, s2);

    try std.testing.expectEqual(@as(usize, 3), sv.count());
    try std.testing.expectEqualStrings("hello", sv.items()[0].?);
    try std.testing.expectEqual(@as(?[]const u8, null), sv.items()[1]);
    try std.testing.expectEqualStrings("world", sv.items()[2].?);

    // Memory cleanup happens in deinit
}

test "readFromCache - basic selection" {
    const allocator = std.testing.allocator;

    // Create cached values
    const cached = [_]i32{ 100, 200, 300, 400, 500 };

    // Create selection for indices 1, 3 (values 200, 400)
    var sel = SelectionVector.init(allocator);
    defer sel.deinit();
    try sel.append(1);
    try sel.append(3);

    var result = try readFromCache(i32, allocator, &cached, &sel);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.count());
    try std.testing.expectEqual(@as(?i32, 200), result.items()[0]);
    try std.testing.expectEqual(@as(?i32, 400), result.items()[1]);
}

test "readFromCache - string values with copy" {
    const allocator = std.testing.allocator;

    // Create cached string values
    const cached = [_][]const u8{ "alpha", "beta", "gamma", "delta" };

    // Select indices 0 and 2
    var sel = SelectionVector.init(allocator);
    defer sel.deinit();
    try sel.append(0);
    try sel.append(2);

    var result = try readFromCache([]const u8, allocator, &cached, &sel);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.count());
    try std.testing.expectEqualStrings("alpha", result.items()[0].?);
    try std.testing.expectEqualStrings("gamma", result.items()[1].?);

    // Verify these are copies (different pointers)
    try std.testing.expect(result.items()[0].?.ptr != cached[0].ptr);
    try std.testing.expect(result.items()[1].?.ptr != cached[2].ptr);
}

test "readFromCache - empty selection" {
    const allocator = std.testing.allocator;

    const cached = [_]i32{ 1, 2, 3 };

    var sel = SelectionVector.init(allocator);
    defer sel.deinit();
    // Empty selection

    var result = try readFromCache(i32, allocator, &cached, &sel);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.count());
}

test "readFromCache - index out of bounds" {
    const allocator = std.testing.allocator;

    const cached = [_]i32{ 1, 2, 3 };

    var sel = SelectionVector.init(allocator);
    defer sel.deinit();
    try sel.append(5); // Out of bounds!

    const result = readFromCache(i32, allocator, &cached, &sel);
    try std.testing.expectError(error.IndexOutOfBounds, result);
}
