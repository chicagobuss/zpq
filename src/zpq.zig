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
        pub const lz4 = @import("core/parquet/lz4.zig");
        pub const compression = @import("core/parquet/compression.zig");
        pub const page = @import("core/parquet/page.zig");
        pub const column = @import("core/parquet/column.zig");
        pub const encoding = struct {
            pub const plain = @import("core/parquet/encoding/plain.zig");
            pub const hybrid_rle = @import("core/parquet/encoding/hybrid_rle.zig");
            pub const rle_dict = @import("core/parquet/encoding/rle_dict.zig");
            pub const delta_binary_packed = @import("core/parquet/encoding/delta_binary_packed.zig");
            pub const delta_byte_array = @import("core/parquet/encoding/delta_byte_array.zig");
        };
    };
};

pub const io = struct {
    pub const strategy = @import("io/strategy.zig");
    pub const loop = @import("io/loop.zig");
    pub const tls = @import("io/tls.zig");
    pub const sigv4 = @import("io/sigv4.zig");
    pub const http = @import("io/http.zig");
    pub const s3 = @import("io/s3.zig");
    pub const coalescer = @import("io/coalescer.zig");
};

test {
    _ = core.schema;
    _ = core.thrift;
    _ = core.parquet.metadata;
    _ = core.parquet.snappy;
    _ = core.parquet.lz4;
    _ = core.parquet.compression;
    _ = core.parquet.page;
    _ = core.parquet.column;
    _ = core.parquet.encoding.plain;
    _ = core.parquet.encoding.hybrid_rle;
    _ = core.parquet.encoding.rle_dict;
    _ = core.parquet.encoding.delta_binary_packed;
    _ = core.parquet.encoding.delta_byte_array;
    _ = io.strategy;
    _ = io.loop;
    _ = io.tls;
    _ = io.sigv4;
    _ = io.http;
    _ = io.s3;
    _ = io.coalescer;
}
