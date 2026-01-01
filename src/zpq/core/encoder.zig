const std = @import("std");

/// Parquet value encoders
/// Supports PLAIN, RLE/Bit-Packed Hybrid, and Dictionary encoding

// ============================================================================
// PLAIN Encoder - Simple raw value encoding
// ============================================================================

pub const PlainEncoder = struct {
    buffer: std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) PlainEncoder {
        return .{
            .buffer = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PlainEncoder) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn reset(self: *PlainEncoder) void {
        self.buffer.clearRetainingCapacity();
    }

    /// Get the encoded data
    pub fn getData(self: *const PlainEncoder) []const u8 {
        return self.buffer.items;
    }

    /// Write a boolean (bit-packed, 8 per byte)
    pub fn writeBoolean(self: *PlainEncoder, value: bool) !void {
        // For simplicity, we write one bool per byte for now
        // TODO: Optimize to bit-pack 8 bools per byte
        try self.buffer.append(self.allocator, if (value) 1 else 0);
    }

    /// Write booleans bit-packed (8 per byte)
    pub fn writeBooleans(self: *PlainEncoder, values: []const bool) !void {
        const num_bytes = (values.len + 7) / 8;
        const start = self.buffer.items.len;
        try self.buffer.resize(self.allocator, start + num_bytes);

        // Zero out the new bytes
        @memset(self.buffer.items[start..], 0);

        for (values, 0..) |val, i| {
            if (val) {
                const byte_idx = start + i / 8;
                const bit_idx: u3 = @intCast(i % 8);
                self.buffer.items[byte_idx] |= @as(u8, 1) << bit_idx;
            }
        }
    }

    /// Write an i32 (little-endian)
    pub fn writeInt32(self: *PlainEncoder, value: i32) !void {
        try self.buffer.appendSlice(self.allocator, std.mem.asBytes(&value));
    }

    /// Write multiple i32s
    pub fn writeInt32s(self: *PlainEncoder, values: []const i32) !void {
        const bytes = std.mem.sliceAsBytes(values);
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    /// Write an i64 (little-endian)
    pub fn writeInt64(self: *PlainEncoder, value: i64) !void {
        try self.buffer.appendSlice(self.allocator, std.mem.asBytes(&value));
    }

    /// Write multiple i64s
    pub fn writeInt64s(self: *PlainEncoder, values: []const i64) !void {
        const bytes = std.mem.sliceAsBytes(values);
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    /// Write a f32 (little-endian)
    pub fn writeFloat(self: *PlainEncoder, value: f32) !void {
        try self.buffer.appendSlice(self.allocator, std.mem.asBytes(&value));
    }

    /// Write multiple f32s
    pub fn writeFloats(self: *PlainEncoder, values: []const f32) !void {
        const bytes = std.mem.sliceAsBytes(values);
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    /// Write a f64 (little-endian)
    pub fn writeDouble(self: *PlainEncoder, value: f64) !void {
        try self.buffer.appendSlice(self.allocator, std.mem.asBytes(&value));
    }

    /// Write multiple f64s
    pub fn writeDoubles(self: *PlainEncoder, values: []const f64) !void {
        const bytes = std.mem.sliceAsBytes(values);
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    /// Write a byte array (4-byte length prefix + data)
    pub fn writeByteArray(self: *PlainEncoder, value: []const u8) !void {
        const len: u32 = @intCast(value.len);
        try self.buffer.appendSlice(self.allocator, std.mem.asBytes(&len));
        try self.buffer.appendSlice(self.allocator, value);
    }

    /// Write multiple byte arrays
    pub fn writeByteArrays(self: *PlainEncoder, values: []const []const u8) !void {
        for (values) |val| {
            try self.writeByteArray(val);
        }
    }

    /// Write fixed-length byte array (no length prefix)
    pub fn writeFixedByteArray(self: *PlainEncoder, value: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, value);
    }

    /// Write raw bytes directly
    pub fn writeRaw(self: *PlainEncoder, data: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, data);
    }
};

// ============================================================================
// RLE/Bit-Packed Hybrid Encoder
// ============================================================================

pub const RleEncoder = struct {
    buffer: std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    bit_width: u8,

    // Buffered values for deciding RLE vs bit-packed
    value_buffer: [8]u32 = undefined,
    buffer_count: u8 = 0,

    // Current run state
    current_value: u32 = 0,
    repeat_count: u32 = 0,

    pub fn init(allocator: std.mem.Allocator, bit_width: u8) RleEncoder {
        return .{
            .buffer = .{},
            .allocator = allocator,
            .bit_width = bit_width,
        };
    }

    pub fn deinit(self: *RleEncoder) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn reset(self: *RleEncoder) void {
        self.buffer.clearRetainingCapacity();
        self.buffer_count = 0;
        self.repeat_count = 0;
        self.current_value = 0;
    }

    pub fn getData(self: *const RleEncoder) []const u8 {
        return self.buffer.items;
    }

    /// Write a single value
    pub fn write(self: *RleEncoder, value: u32) !void {
        if (self.repeat_count == 0) {
            // Start a new run
            self.current_value = value;
            self.repeat_count = 1;
        } else if (value == self.current_value) {
            // Continue RLE run
            self.repeat_count += 1;
        } else {
            // Value changed - flush current run and start new
            try self.flushRun();
            self.current_value = value;
            self.repeat_count = 1;
        }
    }

    /// Write multiple values
    pub fn writeMany(self: *RleEncoder, values: []const u32) !void {
        for (values) |v| {
            try self.write(v);
        }
    }

    /// Finish encoding and flush any remaining data
    pub fn finish(self: *RleEncoder) !void {
        if (self.repeat_count > 0) {
            try self.flushRun();
        }
    }

    fn flushRun(self: *RleEncoder) !void {
        if (self.repeat_count == 0) return;

        // Always use RLE encoding for consistency and correctness
        // Bit-packing is tricky because it requires exact 8-value groups
        // and the reader expects num_values from the header to match
        try self.writeRleRun();
        self.repeat_count = 0;
    }

    fn writeRleRun(self: *RleEncoder) !void {
        // Header: (count << 1) | 0
        const header = self.repeat_count << 1;
        try self.writeVarInt(header);

        // Value: byte_width bytes, little-endian
        const byte_width: usize = @max(1, (self.bit_width + 7) / 8);
        var val = self.current_value;
        for (0..byte_width) |_| {
            try self.buffer.append(self.allocator, @truncate(val));
            val >>= 8;
        }
    }

    fn bitPack8(self: *RleEncoder, values: *const [8]u32) !void {
        if (self.bit_width == 0) return;

        // Calculate bytes needed: ceil(8 * bit_width / 8) = bit_width bytes
        const bytes_needed = self.bit_width;
        const start = self.buffer.items.len;
        try self.buffer.resize(self.allocator, start + bytes_needed);
        @memset(self.buffer.items[start..], 0);

        var bit_offset: u32 = 0;
        for (values) |val| {
            // Write bit_width bits of val starting at bit_offset
            var remaining_bits = self.bit_width;
            var v = val;
            var offset = bit_offset;

            while (remaining_bits > 0) {
                const byte_idx = start + offset / 8;
                const bit_idx: u3 = @intCast(offset % 8);
                const bits_in_byte = 8 - @as(u8, bit_idx);
                const bits_to_write: u8 = @min(bits_in_byte, remaining_bits);

                const mask: u8 = (@as(u8, 1) << @intCast(bits_to_write)) -% 1;
                const bits: u8 = @truncate(v & mask);
                self.buffer.items[byte_idx] |= bits << bit_idx;

                v >>= @intCast(bits_to_write);
                remaining_bits -= bits_to_write;
                offset += bits_to_write;
            }
            bit_offset += self.bit_width;
        }
    }

    fn writeVarInt(self: *RleEncoder, value: u32) !void {
        var v = value;
        while (v >= 0x80) {
            try self.buffer.append(self.allocator, @as(u8, @truncate(v)) | 0x80);
            v >>= 7;
        }
        try self.buffer.append(self.allocator, @truncate(v));
    }
};

// ============================================================================
// Dictionary Encoder
// ============================================================================

pub fn DictEncoder(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,

        // Dictionary: unique values
        dict_values: std.ArrayListUnmanaged(T),

        // Hash map: value -> index
        value_to_index: std.AutoHashMapUnmanaged(T, u32),

        // Encoded indices (will be RLE encoded later)
        indices: std.ArrayListUnmanaged(u32),

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .dict_values = .{},
                .value_to_index = .{},
                .indices = .{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.dict_values.deinit(self.allocator);
            self.value_to_index.deinit(self.allocator);
            self.indices.deinit(self.allocator);
        }

        pub fn reset(self: *Self) void {
            self.dict_values.clearRetainingCapacity();
            self.value_to_index.clearRetainingCapacity();
            self.indices.clearRetainingCapacity();
        }

        /// Add a value, returns its dictionary index
        pub fn put(self: *Self, value: T) !u32 {
            const result = try self.value_to_index.getOrPut(self.allocator, value);
            if (!result.found_existing) {
                const idx: u32 = @intCast(self.dict_values.items.len);
                result.value_ptr.* = idx;
                try self.dict_values.append(self.allocator, value);
            }
            const idx = result.value_ptr.*;
            try self.indices.append(self.allocator, idx);
            return idx;
        }

        /// Add multiple values
        pub fn putMany(self: *Self, values: []const T) !void {
            for (values) |v| {
                _ = try self.put(v);
            }
        }

        /// Get the dictionary values (for writing dictionary page)
        pub fn getDictValues(self: *const Self) []const T {
            return self.dict_values.items;
        }

        /// Get the indices (for writing data page)
        pub fn getIndices(self: *const Self) []const u32 {
            return self.indices.items;
        }

        /// Get the bit width needed to encode indices
        pub fn getBitWidth(self: *const Self) u8 {
            const n = self.dict_values.items.len;
            if (n <= 1) return 1; // Minimum bit width
            return @intCast(std.math.log2_int(usize, n - 1) + 1);
        }

        /// Get number of unique values
        pub fn numEntries(self: *const Self) usize {
            return self.dict_values.items.len;
        }
    };
}

/// String dictionary encoder (special handling for byte arrays)
pub const StringDictEncoder = struct {
    allocator: std.mem.Allocator,

    // Dictionary: unique string values (owned copies)
    dict_values: std.ArrayListUnmanaged([]const u8),

    // Hash map: string hash -> index
    string_to_index: std.StringHashMapUnmanaged(u32),

    // Encoded indices
    indices: std.ArrayListUnmanaged(u32),

    // Arena for string copies
    arena: std.heap.ArenaAllocator,

    pub fn init(allocator: std.mem.Allocator) StringDictEncoder {
        return .{
            .allocator = allocator,
            .dict_values = .{},
            .string_to_index = .{},
            .indices = .{},
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *StringDictEncoder) void {
        self.dict_values.deinit(self.allocator);
        self.string_to_index.deinit(self.allocator);
        self.indices.deinit(self.allocator);
        self.arena.deinit();
    }

    pub fn reset(self: *StringDictEncoder) void {
        self.dict_values.clearRetainingCapacity();
        self.string_to_index.clearRetainingCapacity();
        self.indices.clearRetainingCapacity();
        _ = self.arena.reset(.retain_capacity);
    }

    /// Add a string value
    pub fn put(self: *StringDictEncoder, value: []const u8) !u32 {
        const result = try self.string_to_index.getOrPut(self.allocator, value);
        if (!result.found_existing) {
            const idx: u32 = @intCast(self.dict_values.items.len);
            // Copy the string into arena
            const owned = try self.arena.allocator().dupe(u8, value);
            result.key_ptr.* = owned;
            result.value_ptr.* = idx;
            try self.dict_values.append(self.allocator, owned);
        }
        const idx = result.value_ptr.*;
        try self.indices.append(self.allocator, idx);
        return idx;
    }

    /// Add multiple strings
    pub fn putMany(self: *StringDictEncoder, values: []const []const u8) !void {
        for (values) |v| {
            _ = try self.put(v);
        }
    }

    /// Get dictionary values
    pub fn getDictValues(self: *const StringDictEncoder) []const []const u8 {
        return self.dict_values.items;
    }

    /// Get indices
    pub fn getIndices(self: *const StringDictEncoder) []const u32 {
        return self.indices.items;
    }

    /// Get bit width for indices
    pub fn getBitWidth(self: *const StringDictEncoder) u8 {
        const n = self.dict_values.items.len;
        if (n <= 1) return 1;
        return @intCast(std.math.log2_int(usize, n - 1) + 1);
    }

    /// Get number of unique values
    pub fn numEntries(self: *const StringDictEncoder) usize {
        return self.dict_values.items.len;
    }

    /// Write dictionary page data (PLAIN encoded strings)
    pub fn writeDictPage(self: *const StringDictEncoder, plain: *PlainEncoder) !void {
        for (self.dict_values.items) |s| {
            try plain.writeByteArray(s);
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "PlainEncoder - integers" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();

    try enc.writeInt32(42);
    try enc.writeInt32(-1);
    try enc.writeInt64(0x123456789ABCDEF0);

    const data = enc.getData();
    try std.testing.expectEqual(@as(usize, 16), data.len);

    // Verify i32 values
    try std.testing.expectEqual(@as(i32, 42), std.mem.readInt(i32, data[0..4], .little));
    try std.testing.expectEqual(@as(i32, -1), std.mem.readInt(i32, data[4..8], .little));

    // Verify i64 value
    try std.testing.expectEqual(@as(i64, 0x123456789ABCDEF0), std.mem.readInt(i64, data[8..16], .little));
}

test "PlainEncoder - byte arrays" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();

    try enc.writeByteArray("hello");
    try enc.writeByteArray("world");

    const data = enc.getData();
    // 4 + 5 + 4 + 5 = 18 bytes
    try std.testing.expectEqual(@as(usize, 18), data.len);

    // First string: length 5
    try std.testing.expectEqual(@as(u32, 5), std.mem.readInt(u32, data[0..4], .little));
    try std.testing.expectEqualStrings("hello", data[4..9]);

    // Second string: length 5
    try std.testing.expectEqual(@as(u32, 5), std.mem.readInt(u32, data[9..13], .little));
    try std.testing.expectEqualStrings("world", data[13..18]);
}

test "PlainEncoder - booleans bit-packed" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();

    const bools = [_]bool{ true, false, true, true, false, false, true, false, true };
    try enc.writeBooleans(&bools);

    const data = enc.getData();
    try std.testing.expectEqual(@as(usize, 2), data.len); // 9 bools = 2 bytes

    // First byte: bits 0,2,3,6 set = 0b01001101 = 0x4D
    try std.testing.expectEqual(@as(u8, 0x4D), data[0]);
    // Second byte: bit 0 set = 0b00000001 = 0x01
    try std.testing.expectEqual(@as(u8, 0x01), data[1]);
}

test "RleEncoder - RLE run" {
    var enc = RleEncoder.init(std.testing.allocator, 3);
    defer enc.deinit();

    // Write 8 identical values
    for (0..8) |_| {
        try enc.write(5);
    }
    try enc.finish();

    const data = enc.getData();
    // Header: (8 << 1) | 0 = 16 = 0x10
    // Value: 5 (1 byte for bit_width=3)
    try std.testing.expectEqual(@as(usize, 2), data.len);
    try std.testing.expectEqual(@as(u8, 0x10), data[0]);
    try std.testing.expectEqual(@as(u8, 0x05), data[1]);
}

test "RleEncoder - short run uses RLE" {
    var enc = RleEncoder.init(std.testing.allocator, 3);
    defer enc.deinit();

    // Write 4 identical values
    for (0..4) |_| {
        try enc.write(3);
    }
    try enc.finish();

    const data = enc.getData();
    // Uses RLE encoding (implementation always uses RLE for consistency)
    // Header: (4 << 1) | 0 = 8 = 0x08 (RLE run of 4)
    try std.testing.expectEqual(@as(u8, 0x08), data[0]);
    // Value: 3 encoded in 1 byte (bit_width=3 -> byte_width=1)
    try std.testing.expectEqual(@as(u8, 0x03), data[1]);
}

test "DictEncoder - basic" {
    var enc = DictEncoder(i32).init(std.testing.allocator);
    defer enc.deinit();

    _ = try enc.put(100);
    _ = try enc.put(200);
    _ = try enc.put(100); // duplicate
    _ = try enc.put(300);
    _ = try enc.put(200); // duplicate

    // Should have 3 unique values
    try std.testing.expectEqual(@as(usize, 3), enc.numEntries());

    // Indices should be [0, 1, 0, 2, 1]
    const indices = enc.getIndices();
    try std.testing.expectEqual(@as(usize, 5), indices.len);
    try std.testing.expectEqual(@as(u32, 0), indices[0]);
    try std.testing.expectEqual(@as(u32, 1), indices[1]);
    try std.testing.expectEqual(@as(u32, 0), indices[2]);
    try std.testing.expectEqual(@as(u32, 2), indices[3]);
    try std.testing.expectEqual(@as(u32, 1), indices[4]);

    // Bit width for 3 values = 2
    try std.testing.expectEqual(@as(u8, 2), enc.getBitWidth());
}

test "StringDictEncoder - basic" {
    var enc = StringDictEncoder.init(std.testing.allocator);
    defer enc.deinit();

    _ = try enc.put("apple");
    _ = try enc.put("banana");
    _ = try enc.put("apple"); // duplicate
    _ = try enc.put("cherry");

    try std.testing.expectEqual(@as(usize, 3), enc.numEntries());

    const indices = enc.getIndices();
    try std.testing.expectEqual(@as(u32, 0), indices[0]);
    try std.testing.expectEqual(@as(u32, 1), indices[1]);
    try std.testing.expectEqual(@as(u32, 0), indices[2]);
    try std.testing.expectEqual(@as(u32, 2), indices[3]);
}
