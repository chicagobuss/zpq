//! ZPQ — pure sans-IO core surface.
//!
//! Anything I/O-related lives behind the io.* namespace and must satisfy
//! io.strategy.assertIsIOStrategy. Anything in core.* is pure logic and
//! must remain free of allocations beyond what callers pass in.

pub const core = struct {
    pub const schema = @import("core/schema.zig");
    pub const thrift = @import("core/thrift.zig");
    pub const parquet = struct {
        pub const metadata = @import("core/parquet/metadata.zig");
        pub const snappy = @import("core/parquet/snappy.zig");
        pub const compression = @import("core/parquet/compression.zig");
        pub const page = @import("core/parquet/page.zig");
        pub const encoding = struct {
            pub const plain = @import("core/parquet/encoding/plain.zig");
            pub const hybrid_rle = @import("core/parquet/encoding/hybrid_rle.zig");
        };
    };
};

pub const io = struct {
    pub const strategy = @import("io/strategy.zig");
    pub const loop = @import("io/loop.zig");
};

test {
    _ = core.schema;
    _ = core.thrift;
    _ = core.parquet.metadata;
    _ = core.parquet.snappy;
    _ = core.parquet.compression;
    _ = core.parquet.page;
    _ = core.parquet.encoding.plain;
    _ = core.parquet.encoding.hybrid_rle;
    _ = io.strategy;
    _ = io.loop;
}
