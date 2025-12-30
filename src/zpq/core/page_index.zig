const std = @import("std");
const thrift = @import("thrift.zig");

/// Ordering of min/max values in ColumnIndex
pub const BoundaryOrder = enum(i32) {
    UNORDERED = 0,
    ASCENDING = 1,
    DESCENDING = 2,
};

/// Location of a page within a column chunk
pub const PageLocation = struct {
    /// Offset of the page in the file
    offset: i64,
    /// Size of the page including header (compressed_page_size + header_length)
    compressed_page_size: i32,
    /// Index of the first row in this page within the row group
    first_row_index: i64,

    pub fn read(reader: *thrift.Reader) !PageLocation {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var loc = PageLocation{
            .offset = 0,
            .compressed_page_size = 0,
            .first_row_index = 0,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => loc.offset = try reader.readZigZag(i64),
                2 => loc.compressed_page_size = try reader.readZigZag(i32),
                3 => loc.first_row_index = try reader.readZigZag(i64),
                else => try reader.skip(field.type),
            }
        }
        return loc;
    }
};

/// Index of page locations within a column chunk.
/// Used to seek directly to pages without scanning.
pub const OffsetIndex = struct {
    page_locations: []PageLocation,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !OffsetIndex {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var locations = std.ArrayListUnmanaged(PageLocation){};
        errdefer locations.deinit(allocator);

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => {
                    // List of PageLocation (struct)
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try locations.ensureTotalCapacity(allocator, size);
                    for (0..size) |_| {
                        try locations.append(allocator, try PageLocation.read(reader));
                    }
                },
                else => try reader.skip(field.type),
            }
        }

        return OffsetIndex{
            .page_locations = try locations.toOwnedSlice(allocator),
        };
    }

    pub fn deinit(self: *OffsetIndex, allocator: std.mem.Allocator) void {
        allocator.free(self.page_locations);
    }

    /// Find the page containing the given row index
    pub fn findPageForRow(self: *const OffsetIndex, row_index: i64) ?usize {
        if (self.page_locations.len == 0) return null;

        // Binary search for the page
        var lo: usize = 0;
        var hi: usize = self.page_locations.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.page_locations[mid].first_row_index <= row_index) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return if (lo > 0) lo - 1 else 0;
    }
};

/// Statistics for each page in a column chunk.
/// Used to skip pages that don't match filter predicates.
pub const ColumnIndex = struct {
    /// True if a page contains only null values
    null_pages: []bool,
    /// Min values for each page (as raw bytes, type-specific interpretation needed)
    min_values: [][]const u8,
    /// Max values for each page (as raw bytes, type-specific interpretation needed)
    max_values: [][]const u8,
    /// Ordering of min/max values
    boundary_order: BoundaryOrder,
    /// Optional: null counts per page
    null_counts: ?[]i64 = null,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !ColumnIndex {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var null_pages = std.ArrayListUnmanaged(bool){};
        errdefer null_pages.deinit(allocator);
        var min_values = std.ArrayListUnmanaged([]const u8){};
        errdefer {
            for (min_values.items) |v| allocator.free(v);
            min_values.deinit(allocator);
        }
        var max_values = std.ArrayListUnmanaged([]const u8){};
        errdefer {
            for (max_values.items) |v| allocator.free(v);
            max_values.deinit(allocator);
        }
        var null_counts = std.ArrayListUnmanaged(i64){};
        errdefer null_counts.deinit(allocator);

        var boundary_order: BoundaryOrder = .UNORDERED;

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => {
                    // List of bool (null_pages)
                    // In Thrift compact protocol, bool lists have elem_type 1 or 2,
                    // and each element is encoded as a single byte (1=true, 2=false, or 0/1)
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try null_pages.ensureTotalCapacity(allocator, size);

                    // Element types 1 (True) and 2 (False) indicate bool list
                    // Each element is a byte: 1=true, 2=false (or 0=false, non-zero=true)
                    for (0..size) |_| {
                        const b = try reader.readByte();
                        // In Parquet/Thrift: 1=true, 2=false
                        // But some implementations use 0=false, 1=true
                        try null_pages.append(allocator, b == 1);
                    }
                },
                2 => {
                    // List of binary (min_values)
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try min_values.ensureTotalCapacity(allocator, size);
                    for (0..size) |_| {
                        const val = try reader.readString();
                        try min_values.append(allocator, try allocator.dupe(u8, val));
                    }
                },
                3 => {
                    // List of binary (max_values)
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try max_values.ensureTotalCapacity(allocator, size);
                    for (0..size) |_| {
                        const val = try reader.readString();
                        try max_values.append(allocator, try allocator.dupe(u8, val));
                    }
                },
                4 => {
                    boundary_order = @enumFromInt(try reader.readZigZag(i32));
                },
                5 => {
                    // List of i64 (null_counts) - optional
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try null_counts.ensureTotalCapacity(allocator, size);
                    for (0..size) |_| {
                        try null_counts.append(allocator, try reader.readZigZag(i64));
                    }
                },
                else => try reader.skip(field.type),
            }
        }

        return ColumnIndex{
            .null_pages = try null_pages.toOwnedSlice(allocator),
            .min_values = try min_values.toOwnedSlice(allocator),
            .max_values = try max_values.toOwnedSlice(allocator),
            .boundary_order = boundary_order,
            .null_counts = if (null_counts.items.len > 0) try null_counts.toOwnedSlice(allocator) else null,
        };
    }

    pub fn deinit(self: *ColumnIndex, allocator: std.mem.Allocator) void {
        allocator.free(self.null_pages);
        for (self.min_values) |v| allocator.free(v);
        allocator.free(self.min_values);
        for (self.max_values) |v| allocator.free(v);
        allocator.free(self.max_values);
        if (self.null_counts) |nc| allocator.free(nc);
    }

    /// Check if page might contain values matching a string equality filter.
    /// Returns false if we can definitively skip this page.
    pub fn mightContainString(self: *const ColumnIndex, page_idx: usize, filter_val: []const u8) bool {
        if (page_idx >= self.null_pages.len) return true; // No stats, can't skip
        if (self.null_pages[page_idx]) return false; // All nulls, no match possible

        const min = self.min_values[page_idx];
        const max = self.max_values[page_idx];

        // filter_val < min or filter_val > max means no match possible
        if (std.mem.lessThan(u8, filter_val, min)) return false;
        if (std.mem.lessThan(u8, max, filter_val)) return false;

        return true; // Might contain matching values
    }

    /// Get number of pages in this index
    pub fn numPages(self: *const ColumnIndex) usize {
        return self.null_pages.len;
    }
};
