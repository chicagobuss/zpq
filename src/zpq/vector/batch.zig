const std = @import("std");
const schema = @import("../core/schema.zig");
const Type = schema.Type;


pub const reader = @import("reader.zig");
pub const compute = @import("compute.zig");
pub const expr = @import("expr.zig");
pub const worker = @import("worker.zig");
pub const pipeline = @import("pipeline.zig");
pub const writer = @import("writer.zig");

pub const VectorColumnReader = reader.VectorColumnReader;
pub const VectorRowGroupWorker = worker.VectorRowGroupWorker;
pub const VectorPipeline = pipeline.VectorPipeline;




/// Supported logical types for vectors.
/// This maps Parquet physical types to our execution engine types.
pub const VectorType = enum {
    bool,
    i32,
    i64,
    f64,        // Parquet FLOAT and DOUBLE map to f64 for simplicity in prototype
    string,     // Byte array / string view
    dictionary, // Dictionary encoded (keys are i32/u32)
};

/// A contiguous chunk of columnar data.
/// Similar to ArrowArray but Zig-native and simplified.
pub const Vector = struct {
    /// Vector logical type
    type: VectorType,
    
    /// Number of elements in this vector
    len: usize,

    /// Validity bitmap. 1 means valid (not null), 0 means null.
    /// If null, all values are valid.
    validity: ?[]const u8 = null,

    /// Raw data bytes. Interpretation depends on `type`.
    /// - i32: Slice of i32
    /// - string: Slice of StringView (len + ptr)
    data: []const u8,

    /// Dictionary specific fields
    /// If type == .dictionary, `data` contains the keys (indices).
    dictionary: ?*const Vector = null,
    
    /// Capacity (if owned)
    capacity: usize = 0,
    
    /// Allocator used for this vector (if owned)
    allocator: ?std.mem.Allocator = null,

    pub fn deinit(self: *Vector) void {
        if (self.allocator) |alloc| {
            if (self.capacity > 0) {
                // Free raw bytes. Since data is a slice, we free the backing memory.
                // In a real implementation this might need more robust memory management 
                // (e.g. freeing the slice pointer itself if it was allocated separately).
                // For this prototype we assume `data.ptr` is the start of allocation.
                const ptr = self.data.ptr;
                // We don't know the alignment perfectly here without context, assume max
                alloc.free(ptr[0..self.capacity]);
            }
            if (self.validity) |v| {
                alloc.free(v);
            }
        }
    }

    /// Access values as a slice of T.
    /// Assert that T matches the vector type.
    pub fn values(self: Vector, comptime T: type) []const T {
        // Validation (debug only)
        if (std.debug.runtime_safety) {
            switch (self.type) {
                .i32 => std.debug.assert(T == i32),
                .i64 => std.debug.assert(T == i64),
                .f64 => std.debug.assert(T == f64),
                .bool => std.debug.assert(T == bool), // Bools are trickier (bitpacked vs byte)
                else => {},
            }
        }
        // Cast bytes to slice of T
        const bytes = self.data;
        const count = bytes.len / @sizeOf(T);
        return @as([*]const T, @ptrCast(@alignCast(bytes.ptr)))[0..count];
    }
};

/// A collection of vectors forming a vertical slice of the table.
/// Typically 1024-4096 rows.
pub const RecordBatch = struct {
    len: usize,
    columns: []Vector,
    
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, len: usize, num_cols: usize) !RecordBatch {
        const columns = try allocator.alloc(Vector, num_cols);
        return RecordBatch{
            .len = len,
            .columns = columns,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RecordBatch) void {
        for (self.columns) |*col| {
            col.deinit();
        }
        self.allocator.free(self.columns);
    }
    
    pub fn column(self: RecordBatch, i: usize) Vector {
        return self.columns[i];
    }
};

test "Vector basic usage" {
    const allocator = std.testing.allocator;
    
    // Create some dummy i32 data
    var data = try allocator.alloc(u8, 1024 * 4); // 1024 i32s
    defer allocator.free(data);
    
    // Fill with 0..1024
    const ints = @as([*]i32, @ptrCast(@alignCast(data.ptr)))[0..1024];
    for (0..1024) |i| ints[i] = @intCast(i);
    
    const vec = Vector{
        .type = .i32,
        .len = 1024,
        .data = data,
    };
    
    const vals = vec.values(i32);
    try std.testing.expectEqual(@as(usize, 1024), vals.len);
    try std.testing.expectEqual(@as(i32, 42), vals[42]);
}

test {
    _ = reader;
    _ = compute;
    _ = expr;
    _ = worker;
    _ = pipeline;
    _ = writer;
}

