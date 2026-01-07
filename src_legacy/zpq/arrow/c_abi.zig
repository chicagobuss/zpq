//! Arrow C Data Interface - ABI-stable structs for zero-copy interop
//!
//! This module provides Zig-native bindings for the Arrow C Data Interface,
//! enabling zero-copy data exchange with any Arrow-compatible system (DuckDB,
//! Polars, DataFusion, SQLite extensions, etc.) without linking libarrow.
//!
//! Spec: https://arrow.apache.org/docs/format/CDataInterface.html
//! Source: references/arrow/cpp/src/arrow/c/abi.h

const std = @import("std");

/// C calling convention for extern function pointers (Zig 0.16.x compatible)
const c_cc = std.builtin.CallingConvention.c;

// =============================================================================
// Arrow Flags (from abi.h)
// =============================================================================

pub const ARROW_FLAG_DICTIONARY_ORDERED: i64 = 1;
pub const ARROW_FLAG_NULLABLE: i64 = 2;
pub const ARROW_FLAG_MAP_KEYS_SORTED: i64 = 4;

// =============================================================================
// ArrowSchema - Type description
// =============================================================================

/// Describes the type of an Arrow array.
///
/// The format string uses a compact encoding defined in the Arrow spec:
/// - Primitive: "c" (int8), "C" (uint8), "s" (int16), "S" (uint16),
///              "i" (int32), "I" (uint32), "l" (int64), "L" (uint64),
///              "e" (float16), "f" (float32), "g" (float64)
/// - Binary: "z" (binary), "Z" (large_binary), "u" (utf8), "U" (large_utf8)
/// - Temporal: "tdD" (date32[days]), "tdm" (date64[ms]), "tts" (time32[s]), etc.
/// - Nested: "+l" (list), "+L" (large_list), "+s" (struct), "+m" (map)
/// - Dictionary: Uses the `dictionary` field to point to index type
pub const ArrowSchema = extern struct {
    /// Format string describing the data type (null-terminated UTF-8)
    format: [*c]const u8,
    /// Optional field name (null-terminated UTF-8, may be null)
    name: [*c]const u8,
    /// Optional field metadata (binary key-value pairs, may be null)
    metadata: [*c]const u8,
    /// Combination of ARROW_FLAG_* values
    flags: i64,
    /// Number of child schemas (for nested types)
    n_children: i64,
    /// Array of pointers to child schemas
    children: [*c][*c]ArrowSchema,
    /// Optional dictionary schema (for dictionary-encoded arrays)
    dictionary: [*c]ArrowSchema,

    /// Producer callback to release schema resources.
    /// Consumer MUST call this when done with the schema.
    release: ?*const fn (*ArrowSchema) callconv(c_cc) void,
    /// Opaque producer data (do not touch)
    private_data: ?*anyopaque,

    /// Check if the schema has been released (null release callback)
    pub fn isReleased(self: *const ArrowSchema) bool {
        return self.release == null;
    }

    /// Release the schema (calls producer's release callback)
    pub fn doRelease(self: *ArrowSchema) void {
        if (self.release) |rel| {
            rel(self);
        }
    }

    /// Get format as a Zig slice (null-safe)
    pub fn getFormat(self: *const ArrowSchema) ?[]const u8 {
        if (self.format == null) return null;
        return std.mem.sliceTo(self.format, 0);
    }

    /// Get name as a Zig slice (null-safe)
    pub fn getName(self: *const ArrowSchema) ?[]const u8 {
        if (self.name == null) return null;
        return std.mem.sliceTo(self.name, 0);
    }

    /// Check if the field is nullable
    pub fn isNullable(self: *const ArrowSchema) bool {
        return (self.flags & ARROW_FLAG_NULLABLE) != 0;
    }

    /// Get number of children
    pub fn numChildren(self: *const ArrowSchema) usize {
        return if (self.n_children > 0) @intCast(self.n_children) else 0;
    }

    /// Get a child schema by index
    pub fn getChild(self: *const ArrowSchema, index: usize) ?*ArrowSchema {
        if (index >= self.numChildren()) return null;
        if (self.children == null) return null;
        return self.children[index];
    }
};

// =============================================================================
// ArrowArray - Data container
// =============================================================================

/// Contains the actual data buffers for an Arrow array.
///
/// Buffer layout depends on the type (from ArrowSchema):
/// - Primitive (e.g., int64): [validity_bitmap, values]
/// - Variable-length (e.g., utf8): [validity_bitmap, offsets, data]
/// - Nested types have child arrays
pub const ArrowArray = extern struct {
    /// Logical number of elements in the array
    length: i64,
    /// Number of null values (-1 if not yet computed)
    null_count: i64,
    /// Logical offset into the buffers (usually 0)
    offset: i64,
    /// Number of physical buffers
    n_buffers: i64,
    /// Number of child arrays (for nested types)
    n_children: i64,
    /// Array of buffer pointers
    buffers: [*c]const ?*const anyopaque,
    /// Array of pointers to child arrays
    children: [*c][*c]ArrowArray,
    /// Optional dictionary values (for dictionary-encoded arrays)
    dictionary: [*c]ArrowArray,

    /// Producer callback to release array resources.
    /// Consumer MUST call this when done with the array.
    release: ?*const fn (*ArrowArray) callconv(c_cc) void,
    /// Opaque producer data (do not touch)
    private_data: ?*anyopaque,

    /// Check if the array has been released (null release callback)
    pub fn isReleased(self: *const ArrowArray) bool {
        return self.release == null;
    }

    /// Release the array (calls producer's release callback)
    pub fn doRelease(self: *ArrowArray) void {
        if (self.release) |rel| {
            rel(self);
        }
    }

    /// Get the number of elements
    pub fn len(self: *const ArrowArray) usize {
        return if (self.length > 0) @intCast(self.length) else 0;
    }

    /// Get number of buffers
    pub fn numBuffers(self: *const ArrowArray) usize {
        return if (self.n_buffers > 0) @intCast(self.n_buffers) else 0;
    }

    /// Get number of children
    pub fn numChildren(self: *const ArrowArray) usize {
        return if (self.n_children > 0) @intCast(self.n_children) else 0;
    }

    /// Get a buffer by index as a typed slice
    pub fn getBuffer(self: *const ArrowArray, comptime T: type, index: usize) ?[*]const T {
        if (index >= self.numBuffers()) return null;
        if (self.buffers == null) return null;
        const buf = self.buffers[index] orelse return null;
        return @ptrCast(@alignCast(buf));
    }

    /// Get a child array by index
    pub fn getChild(self: *const ArrowArray, index: usize) ?*ArrowArray {
        if (index >= self.numChildren()) return null;
        if (self.children == null) return null;
        return self.children[index];
    }

    /// Check if a value at the given index is null (using validity bitmap)
    pub fn isNull(self: *const ArrowArray, index: usize) bool {
        if (self.null_count == 0) return false;
        if (self.numBuffers() == 0) return false;

        // Buffer 0 is the validity bitmap (if present)
        const validity = self.getBuffer(u8, 0) orelse return false;
        const bit_index = self.offset + @as(i64, @intCast(index));
        const byte_index: usize = @intCast(@divFloor(bit_index, 8));
        const bit_offset: u3 = @intCast(@mod(bit_index, 8));
        return (validity[byte_index] & (@as(u8, 1) << bit_offset)) == 0;
    }
};

// =============================================================================
// ArrowArrayStream - Streaming interface
// =============================================================================

/// A stream of Arrow arrays with the same schema.
/// Useful for iterating over large datasets without loading everything into memory.
pub const ArrowArrayStream = extern struct {
    /// Get the stream's schema.
    /// Returns 0 on success, errno-compatible error code otherwise.
    /// The schema must be released independently from the stream.
    get_schema: ?*const fn (*ArrowArrayStream, *ArrowSchema) callconv(c_cc) c_int,

    /// Get the next array in the stream.
    /// Returns 0 on success, errno-compatible error code otherwise.
    /// When the stream ends, the array's release will be null.
    get_next: ?*const fn (*ArrowArrayStream, *ArrowArray) callconv(c_cc) c_int,

    /// Get detailed error information after a failed operation.
    /// Only valid after get_schema or get_next returns non-zero.
    /// Returns null if no description is available.
    get_last_error: ?*const fn (*ArrowArrayStream) callconv(c_cc) [*c]const u8,

    /// Release the stream's resources.
    /// Does not release arrays obtained via get_next.
    release: ?*const fn (*ArrowArrayStream) callconv(c_cc) void,

    /// Opaque producer data
    private_data: ?*anyopaque,

    /// Check if the stream has been released
    pub fn isReleased(self: *const ArrowArrayStream) bool {
        return self.release == null;
    }

    /// Release the stream
    pub fn doRelease(self: *ArrowArrayStream) void {
        if (self.release) |rel| {
            rel(self);
        }
    }
};

// =============================================================================
// Format string constants for common types
// =============================================================================

pub const Format = struct {
    // Null type
    pub const @"null" = "n";

    // Boolean
    pub const @"bool" = "b";

    // Signed integers
    pub const int8 = "c";
    pub const int16 = "s";
    pub const int32 = "i";
    pub const int64 = "l";

    // Unsigned integers
    pub const uint8 = "C";
    pub const uint16 = "S";
    pub const uint32 = "I";
    pub const uint64 = "L";

    // Floating point
    pub const float16 = "e";
    pub const float32 = "f";
    pub const float64 = "g";

    // Binary
    pub const binary = "z";
    pub const large_binary = "Z";
    pub const utf8 = "u";
    pub const large_utf8 = "U";

    // Fixed-size binary (w:N where N is byte width)
    pub fn fixedSizeBinary(comptime width: comptime_int) *const [std.fmt.count("w:{d}", .{width}):0]u8 {
        return std.fmt.comptimePrint("w:{d}", .{width});
    }

    // Date/Time
    pub const date32 = "tdD"; // days since epoch
    pub const date64 = "tdm"; // milliseconds since epoch
    pub const time32_s = "tts";
    pub const time32_ms = "ttm";
    pub const time64_us = "ttu";
    pub const time64_ns = "ttn";
    pub const timestamp_s = "tss:"; // append timezone
    pub const timestamp_ms = "tsm:";
    pub const timestamp_us = "tsu:";
    pub const timestamp_ns = "tsn:";

    // Nested
    pub const list = "+l";
    pub const large_list = "+L";
    pub const @"struct" = "+s";
    pub const map = "+m";
    pub const dense_union = "+ud:"; // append type IDs
    pub const sparse_union = "+us:";

    // Special
    pub const decimal128 = "d:"; // d:precision,scale
    pub const decimal256 = "d:"; // d:precision,scale,256
};

// =============================================================================
// Tests
// =============================================================================

test "ArrowSchema basic" {
    var schema = std.mem.zeroes(ArrowSchema);
    try std.testing.expect(schema.isReleased());

    schema.format = "l";
    schema.name = "test_column";
    schema.flags = ARROW_FLAG_NULLABLE;
    schema.n_children = 0;

    try std.testing.expectEqualStrings("l", schema.getFormat().?);
    try std.testing.expectEqualStrings("test_column", schema.getName().?);
    try std.testing.expect(schema.isNullable());
    try std.testing.expectEqual(@as(usize, 0), schema.numChildren());
}

test "ArrowArray basic" {
    var array = std.mem.zeroes(ArrowArray);
    try std.testing.expect(array.isReleased());

    array.length = 100;
    array.null_count = 5;
    array.offset = 0;
    array.n_buffers = 2;
    array.n_children = 0;

    try std.testing.expectEqual(@as(usize, 100), array.len());
    try std.testing.expectEqual(@as(usize, 2), array.numBuffers());
    try std.testing.expectEqual(@as(usize, 0), array.numChildren());
}

test "Format constants" {
    try std.testing.expectEqualStrings("l", Format.int64);
    try std.testing.expectEqualStrings("u", Format.utf8);
    try std.testing.expectEqualStrings("+s", Format.@"struct");
    try std.testing.expectEqualStrings("b", Format.bool);
}

test "Arrow C ABI struct sizes" {
    // These sizes must match the C Arrow ABI for interoperability on 64-bit
    // ArrowSchema: 9 fields = 72 bytes (3 char*, 2 i64, 2 ptr*, 1 fn ptr, 1 void*)
    // ArrowArray: 10 fields = 80 bytes (5 i64, 3 ptr*, 1 fn ptr, 1 void*)
    // ArrowArrayStream: 5 fields = 40 bytes
    try std.testing.expectEqual(@as(usize, 72), @sizeOf(ArrowSchema));
    try std.testing.expectEqual(@as(usize, 80), @sizeOf(ArrowArray));
    try std.testing.expectEqual(@as(usize, 40), @sizeOf(ArrowArrayStream));
}
