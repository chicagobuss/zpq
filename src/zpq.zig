const std = @import("std");

/// Feature flags and build-time configuration
pub const options = @import("zpq_options");

/// The core I/O abstractions for ZPQ.
/// This houses the completion-ready RandomAccessSource and BufferLender interfaces.
pub const io = struct {
    pub const interface = @import("io/interface.zig");
    pub const RandomAccessSource = interface.RandomAccessSource;
    pub const BufferLender = interface.BufferLender;
    pub const local = @import("io/local.zig");
    pub const s3 = @import("io/s3.zig");
    pub const transport = @import("io/transport.zig");
    pub const factory = @import("io/factory.zig");
    pub const sink = @import("io/sink.zig");
    pub const s3_sink = @import("io/s3_sink.zig");
    pub const local_sink = @import("io/local_sink.zig");
};

/// Stateless Protocol implementations (Sans-I/O).
/// These handle formatting and parsing without performing any syscalls.
pub const protocol = struct {
    pub const http = @import("protocol/http.zig");
    pub const s3 = @import("protocol/s3.zig");
    pub const sigv4 = @import("protocol/sigv4.zig");
};

/// Parquet-specific logic, including Thrift metadata parsing and bit-packing.
pub const core = struct {
    pub const thrift = @import("core/thrift.zig");
    pub const schema = @import("core/schema.zig");
    pub const file = @import("core/file.zig");
    pub const rle = @import("core/rle.zig");
    pub const simd = @import("core/simd.zig");
    pub const reader = @import("core/reader.zig");
    pub const decompress = @import("core/decompress.zig");
    pub const selection = @import("core/selection.zig");
    pub const filter = @import("core/filter.zig");
    pub const writer = @import("core/writer.zig");
};

/// High-level schema reflection and specialized reader generation.
pub const schema = struct {
    // The "Polars Killer" comptime implementation lives here,
    // utilizing the building blocks in core.reader.
};

test {
    std.testing.refAllDecls(@This());
}
