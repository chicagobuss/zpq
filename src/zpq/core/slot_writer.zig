//! Sequential Parquet writer with parallel preparation.
//!
//! Design (like DuckDB/Polars):
//! 1. Workers prepare row groups in parallel (encode to memory buffers)
//! 2. Mutex-protected sequential writes to file
//! 3. Offsets computed at write time (contiguous layout, no gaps)
//!
//! This avoids the "sparse slot" problem where filtered output creates
//! huge files with gaps between row groups.

const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const builtin = @import("builtin");

/// Metadata for a single column within a row group.
pub const ColumnMeta = struct {
    type: schema.Type,
    encodings: std.ArrayListUnmanaged(schema.Encoding),
    path_in_schema: std.ArrayListUnmanaged([]const u8),
    codec: schema.CompressionCodec,
    num_values: i64,
    uncompressed_size: i64,
    compressed_size: i64,
    has_dictionary: bool = false,
};

/// Metadata for a row group (collected after worker completes).
pub const RowGroupMeta = struct {
    num_rows: i64,
    columns: []const ColumnMeta,
    actual_size: u64, // Actual bytes written
};

/// Sequential Parquet writer.
///
/// Workers call writeSlot() which is mutex-protected for sequential writes.
/// Offsets are computed at write time, resulting in a compact file.
pub const SlotWriter = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,

    // Slot tracking
    num_slots: usize,
    /// Actual file offsets where each slot was written (computed at write time)
    slot_offsets: []u64,
    /// Track actual sizes written
    actual_sizes: []std.atomic.Value(u64),
    /// Current write position in file
    write_pos: std.atomic.Value(u64),
    /// Mutex for sequential writes
    write_mutex: std.Thread.Mutex,

    // Legacy fields (kept for API compatibility, unused)
    slot_size: u64,
    footer_offset: u64,

    // Schema info (immutable after init)
    schema_elements: std.ArrayListUnmanaged(schema.SchemaElement),

    const Self = @This();

    /// Initialize a sequential writer.
    ///
    /// Args:
    ///   - allocator: Memory allocator
    ///   - path: Output file path
    ///   - num_row_groups: Number of row groups to write
    ///   - max_rg_size: Unused (kept for API compatibility)
    ///   - schema_elements: Schema for the output file
    pub fn init(
        allocator: std.mem.Allocator,
        path: []const u8,
        num_row_groups: usize,
        max_rg_size: u64,
        input_schema: []const schema.SchemaElement,
    ) !Self {
        // Allocate offset trackers (filled in at write time)
        const slot_offsets = try allocator.alloc(u64, num_row_groups);
        errdefer allocator.free(slot_offsets);
        for (slot_offsets) |*offset| {
            offset.* = 0; // Will be set when slot is written
        }

        // Allocate size trackers
        const actual_sizes = try allocator.alloc(std.atomic.Value(u64), num_row_groups);
        errdefer allocator.free(actual_sizes);
        for (actual_sizes) |*s| {
            s.* = std.atomic.Value(u64).init(0);
        }

        // Copy schema elements
        var schema_copy = std.ArrayListUnmanaged(schema.SchemaElement){};
        errdefer schema_copy.deinit(allocator);
        try schema_copy.appendSlice(allocator, input_schema);

        // Create output file
        const file = try std.fs.cwd().createFile(path, .{ .read = true });
        errdefer file.close();

        // Write PAR1 header
        try file.writeAll("PAR1");

        return Self{
            .allocator = allocator,
            .file = file,
            .num_slots = num_row_groups,
            .slot_offsets = slot_offsets,
            .actual_sizes = actual_sizes,
            .write_pos = std.atomic.Value(u64).init(4), // After PAR1 header
            .write_mutex = .{},
            .slot_size = max_rg_size, // Unused, kept for compatibility
            .footer_offset = 0, // Unused, computed at finish time
            .schema_elements = schema_copy,
        };
    }

    pub fn deinit(self: *Self) void {
        self.file.close();
        self.allocator.free(self.slot_offsets);
        self.allocator.free(self.actual_sizes);
        self.schema_elements.deinit(self.allocator);
    }

    /// Get the file offset for a slot (only valid after writeSlot called).
    pub fn slotOffset(self: *const Self, slot_index: usize) u64 {
        return self.slot_offsets[slot_index];
    }

    /// Write row group data sequentially.
    /// Thread-safe via mutex - writes are serialized but preparation is parallel.
    pub fn writeSlot(self: *Self, slot_index: usize, data: []const u8) !void {
        if (slot_index >= self.num_slots) {
            return error.InvalidSlotIndex;
        }

        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        // Record the offset where this slot starts
        const offset = self.write_pos.load(.acquire);
        self.slot_offsets[slot_index] = offset;

        // Write data sequentially
        try self.file.writeAll(data);

        // Update position and size
        self.write_pos.store(offset + data.len, .release);
        self.actual_sizes[slot_index].store(@intCast(data.len), .release);
    }

    /// Finish the file - write footer with recorded offsets.
    /// Must be called after all workers complete.
    pub fn finish(self: *Self, row_groups_meta: []const RowGroupMeta) !void {
        if (row_groups_meta.len != self.num_slots) {
            return error.RowGroupCountMismatch;
        }

        // Get current write position (where footer will start)
        const footer_start = self.write_pos.load(.acquire);

        // Build row groups with recorded offsets (already contiguous)
        var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
        defer {
            for (row_groups.items) |*rg| {
                for (rg.columns.items) |*col| {
                    if (col.meta_data) |*meta| {
                        meta.encodings.deinit(self.allocator);
                        meta.path_in_schema.deinit(self.allocator);
                    }
                }
                rg.columns.deinit(self.allocator);
            }
            row_groups.deinit(self.allocator);
        }

        var total_rows: i64 = 0;

        for (row_groups_meta, 0..) |rg_meta, i| {
            const slot_start = self.slot_offsets[i];

            // Build column chunks with adjusted offsets
            var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
            errdefer {
                for (columns.items) |*col| {
                    if (col.meta_data) |*meta| {
                        meta.encodings.deinit(self.allocator);
                        meta.path_in_schema.deinit(self.allocator);
                    }
                }
                columns.deinit(self.allocator);
            }

            var col_offset: i64 = @intCast(slot_start);

            for (rg_meta.columns) |col_meta| {
                // Copy encodings
                var encodings = std.ArrayListUnmanaged(schema.Encoding){};
                try encodings.appendSlice(self.allocator, col_meta.encodings.items);

                // Copy path_in_schema
                var path_in_schema = std.ArrayListUnmanaged([]const u8){};
                try path_in_schema.appendSlice(self.allocator, col_meta.path_in_schema.items);

                const dict_offset: ?i64 = if (col_meta.has_dictionary) col_offset else null;

                try columns.append(self.allocator, .{
                    .file_path = null,
                    .file_offset = col_offset,
                    .meta_data = .{
                        .type = col_meta.type,
                        .encodings = encodings,
                        .path_in_schema = path_in_schema,
                        .codec = col_meta.codec,
                        .num_values = col_meta.num_values,
                        .total_uncompressed_size = col_meta.uncompressed_size,
                        .total_compressed_size = col_meta.compressed_size,
                        .data_page_offset = col_offset,
                        .dictionary_page_offset = dict_offset,
                        .index_page_offset = null,
                    },
                });

                col_offset += col_meta.compressed_size;
            }

            const actual_size = self.actual_sizes[i].load(.acquire);

            try row_groups.append(self.allocator, .{
                .columns = columns,
                .total_byte_size = @intCast(actual_size),
                .num_rows = rg_meta.num_rows,
            });

            total_rows += rg_meta.num_rows;
        }

        // Build FileMetaData
        const metadata = schema.FileMetaData{
            .version = 2,
            .schema = self.schema_elements,
            .num_rows = total_rows,
            .created_by = "zpq",
            .row_groups = row_groups,
        };

        // Serialize footer via Thrift
        var writer = thrift.Writer.init(self.allocator);
        defer writer.deinit();
        try metadata.write(&writer);

        const footer_bytes = writer.bytes();
        const footer_len: u32 = @intCast(footer_bytes.len);

        // Write footer right after data (already contiguous, no seek needed)
        try self.file.seekTo(footer_start);
        try self.file.writeAll(footer_bytes);
        try self.file.writeAll(&std.mem.toBytes(footer_len));
        try self.file.writeAll("PAR1");
    }
};

/// Cross-platform pwrite implementation.
/// Writes data at a specific offset without changing file position.
fn pwrite(handle: std.fs.File.Handle, data: []const u8, offset: i64) !usize {
    const native_os = builtin.os.tag;

    if (native_os == .linux) {
        const result = std.os.linux.pwrite(handle, data.ptr, data.len, offset);
        const err = std.posix.errno(result);
        if (err != .SUCCESS) {
            return std.posix.unexpectedErrno(err);
        }
        return result;
    } else {
        // macOS, BSD, etc. - use libc pwrite
        const result = std.c.pwrite(handle, data.ptr, data.len, offset);
        if (result < 0) {
            return std.posix.unexpectedErrno(std.posix.errno(result));
        }
        return @intCast(result);
    }
}

// ============================================================================
// Tests
// ============================================================================

test "SlotWriter - sequential write behavior" {
    const allocator = std.testing.allocator;

    // Create schema
    const schema_elements = [_]schema.SchemaElement{
        .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
        .{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "id",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
    };

    var writer = try SlotWriter.init(
        allocator,
        "/tmp/test_slot_writer.parquet",
        3, // 3 row groups
        10000, // max 10KB per row group (unused in sequential mode)
        &schema_elements,
    );
    defer writer.deinit();

    // Initially all offsets are 0 (will be set at write time)
    try std.testing.expectEqual(@as(u64, 0), writer.slot_offsets[0]);
    try std.testing.expectEqual(@as(u64, 0), writer.slot_offsets[1]);
    try std.testing.expectEqual(@as(u64, 0), writer.slot_offsets[2]);

    // Write position starts after PAR1 header
    try std.testing.expectEqual(@as(u64, 4), writer.write_pos.load(.acquire));

    // Clean up test file
    std.fs.cwd().deleteFile("/tmp/test_slot_writer.parquet") catch {};
}

test "SlotWriter - sequential writes (out of order)" {
    const allocator = std.testing.allocator;

    const schema_elements = [_]schema.SchemaElement{
        .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
        .{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "id",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
    };

    var writer = try SlotWriter.init(
        allocator,
        "/tmp/test_slot_parallel.parquet",
        3,
        1000,
        &schema_elements,
    );
    defer writer.deinit();

    // Write to slots out of order (simulating parallel workers completing at different times)
    const data2 = "slot 2 data here";
    const data0 = "slot 0 data";
    const data1 = "slot 1 longer data";

    try writer.writeSlot(2, data2);
    try writer.writeSlot(0, data0);
    try writer.writeSlot(1, data1);

    // Verify sizes were tracked
    try std.testing.expectEqual(@as(u64, data0.len), writer.actual_sizes[0].load(.acquire));
    try std.testing.expectEqual(@as(u64, data1.len), writer.actual_sizes[1].load(.acquire));
    try std.testing.expectEqual(@as(u64, data2.len), writer.actual_sizes[2].load(.acquire));

    // With sequential writes, data is written in arrival order, not slot order
    // Slot 2 arrived first, so it's at offset 4 (after PAR1)
    // Slot 0 arrived second, at offset 4 + len(data2)
    // Slot 1 arrived third, at offset 4 + len(data2) + len(data0)
    try std.testing.expectEqual(@as(u64, 4), writer.slot_offsets[2]);
    try std.testing.expectEqual(@as(u64, 4 + data2.len), writer.slot_offsets[0]);
    try std.testing.expectEqual(@as(u64, 4 + data2.len + data0.len), writer.slot_offsets[1]);

    // Verify data was written correctly by reading back
    var read_buf: [100]u8 = undefined;

    // Read slot 2 (first in file)
    try writer.file.seekTo(writer.slotOffset(2));
    const n2 = try writer.file.read(read_buf[0..data2.len]);
    try std.testing.expectEqualStrings(data2, read_buf[0..n2]);

    // Read slot 0 (second in file)
    try writer.file.seekTo(writer.slotOffset(0));
    const n0 = try writer.file.read(read_buf[0..data0.len]);
    try std.testing.expectEqualStrings(data0, read_buf[0..n0]);

    // Read slot 1 (third in file)
    try writer.file.seekTo(writer.slotOffset(1));
    const n1 = try writer.file.read(read_buf[0..data1.len]);
    try std.testing.expectEqualStrings(data1, read_buf[0..n1]);

    // Clean up
    std.fs.cwd().deleteFile("/tmp/test_slot_parallel.parquet") catch {};
}

test "SlotWriter - no size limit with sequential writes" {
    const allocator = std.testing.allocator;

    const schema_elements = [_]schema.SchemaElement{
        .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
        .{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "id",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
    };

    var writer = try SlotWriter.init(
        allocator,
        "/tmp/test_slot_overflow.parquet",
        1,
        100, // This hint is now unused - no size limit
        &schema_elements,
    );
    defer writer.deinit();

    // With sequential writes, there's no slot size limit
    const big_data = "x" ** 200;
    try writer.writeSlot(0, big_data);

    // Verify it was written
    try std.testing.expectEqual(@as(u64, 200), writer.actual_sizes[0].load(.acquire));

    // Clean up
    std.fs.cwd().deleteFile("/tmp/test_slot_overflow.parquet") catch {};
}

test "SlotWriter - end-to-end with real row group data" {
    // This test verifies the complete flow:
    // 1. Create SlotWriter with multiple slots
    // 2. Write actual encoded Parquet row group data to slots
    // 3. Call finish() to write footer with slot-based offsets
    // 4. Verify the file has correct PAR1 magic at start and end
    //
    // Full verification with pyarrow is done in Python integration tests.

    const allocator = std.testing.allocator;
    const page_writer_mod = @import("page_writer.zig");

    const test_schema = [_]schema.SchemaElement{
        .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
        .{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
    };

    const output_path = "/tmp/test_slot_e2e.parquet";

    // Create slot writer for 2 row groups
    var slot_writer = try SlotWriter.init(
        allocator,
        output_path,
        2, // 2 row groups
        10000, // max 10KB per row group
        &test_schema,
    );
    defer slot_writer.deinit();

    // Create row group data using page_writer
    // Row group 0: values 0-99
    var rg0_buffer = std.ArrayListUnmanaged(u8){};
    defer rg0_buffer.deinit(allocator);
    {
        var cw = page_writer_mod.ColumnWriter.init(allocator, .INT32, .UNCOMPRESSED);
        defer cw.deinit();

        var values0: [100]i32 = undefined;
        for (&values0, 0..) |*v, i| {
            v.* = @intCast(i);
        }
        try cw.writeInt32Plain(&values0);
        try cw.writePagesToBufferUnmanaged(&rg0_buffer, allocator);
    }

    // Row group 1: values 100-199
    var rg1_buffer = std.ArrayListUnmanaged(u8){};
    defer rg1_buffer.deinit(allocator);
    {
        var cw = page_writer_mod.ColumnWriter.init(allocator, .INT32, .UNCOMPRESSED);
        defer cw.deinit();

        var values1: [100]i32 = undefined;
        for (&values1, 0..) |*v, i| {
            v.* = @intCast(i + 100);
        }
        try cw.writeInt32Plain(&values1);
        try cw.writePagesToBufferUnmanaged(&rg1_buffer, allocator);
    }

    // Write to slots (could be parallel in production!)
    try slot_writer.writeSlot(0, rg0_buffer.items);
    try slot_writer.writeSlot(1, rg1_buffer.items);

    // Build metadata for each row group
    var encodings0 = std.ArrayListUnmanaged(schema.Encoding){};
    defer encodings0.deinit(allocator);
    try encodings0.append(allocator, .PLAIN);

    var path0 = std.ArrayListUnmanaged([]const u8){};
    defer path0.deinit(allocator);
    try path0.append(allocator, "value");

    var encodings1 = std.ArrayListUnmanaged(schema.Encoding){};
    defer encodings1.deinit(allocator);
    try encodings1.append(allocator, .PLAIN);

    var path1 = std.ArrayListUnmanaged([]const u8){};
    defer path1.deinit(allocator);
    try path1.append(allocator, "value");

    const col_meta_0 = ColumnMeta{
        .type = .INT32,
        .encodings = encodings0,
        .path_in_schema = path0,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .uncompressed_size = @intCast(rg0_buffer.items.len),
        .compressed_size = @intCast(rg0_buffer.items.len),
    };

    const col_meta_1 = ColumnMeta{
        .type = .INT32,
        .encodings = encodings1,
        .path_in_schema = path1,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .uncompressed_size = @intCast(rg1_buffer.items.len),
        .compressed_size = @intCast(rg1_buffer.items.len),
    };

    const rg_metas = [_]RowGroupMeta{
        .{
            .num_rows = 100,
            .columns = &[_]ColumnMeta{col_meta_0},
            .actual_size = rg0_buffer.items.len,
        },
        .{
            .num_rows = 100,
            .columns = &[_]ColumnMeta{col_meta_1},
            .actual_size = rg1_buffer.items.len,
        },
    };

    // Finish the file (writes footer with slot offsets)
    try slot_writer.finish(&rg_metas);

    // Verify file structure
    const file = try std.fs.cwd().openFile(output_path, .{});
    defer file.close();

    // Check header magic
    var header_magic: [4]u8 = undefined;
    const n1 = try file.read(&header_magic);
    try std.testing.expectEqual(@as(usize, 4), n1);
    try std.testing.expectEqualStrings("PAR1", &header_magic);

    // Check footer magic
    try file.seekFromEnd(-4);
    var footer_magic: [4]u8 = undefined;
    const n2 = try file.read(&footer_magic);
    try std.testing.expectEqual(@as(usize, 4), n2);
    try std.testing.expectEqualStrings("PAR1", &footer_magic);

    // Verify file size is compact (no sparse gaps)
    // 4 (PAR1) + data + footer + 4 (footer_len) + 4 (PAR1)
    const file_stat = try file.stat();
    // With sequential writes, file should be small - just data + overhead
    // Two row groups of ~400 bytes each + footer (~500 bytes) + 8 = ~1300 bytes
    try std.testing.expect(file_stat.size < 3000); // Must be compact!

    // Clean up
    std.fs.cwd().deleteFile(output_path) catch {};
}
