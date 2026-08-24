//! ZPQ — pure sans-IO core surface.
//!
//! Anything I/O-related lives behind the io.* namespace and must satisfy
//! io.strategy.assertIsIOStrategy. Anything in core.* is pure logic and
//! must remain free of allocations beyond what callers pass in.

pub const core = struct {
    pub const schema = @import("core/schema.zig");
    pub const thrift = @import("core/thrift.zig");
    pub const consumer = @import("core/consumer.zig");
    pub const invariant = @import("core/invariant.zig");
    pub const scan = @import("core/scan.zig");
    pub const spawn = @import("core/spawn.zig");
    pub const filter = struct {
        pub const ast = @import("core/filter/ast.zig");
        pub const encoded = @import("core/filter/encoded.zig");
        pub const parser = @import("core/filter/parser.zig");
        pub const prune = @import("core/filter/prune.zig");
        pub const selection = @import("core/filter/selection.zig");
        pub const eval = @import("core/filter/eval.zig");
        pub const partition = @import("core/filter/partition.zig");
    };
    pub const expr = struct {
        pub const ast = @import("core/expr/ast.zig");
        pub const parser = @import("core/expr/parser.zig");
        pub const eval = @import("core/expr/eval.zig");
        pub const agg = @import("core/expr/agg.zig");
        // CLI-only: the SQL frontend (liteparser) is excluded from the Lambda
        // build, so the module isn't compiled there (no @cImport, no C dep).
        pub const sql_parser = if (@import("build_options").enable_sql)
            @import("core/expr/sql_parser.zig")
        else
            struct {};
    };
    pub const system = @import("core/system.zig");
    pub const writer = struct {
        pub const fastpath = @import("core/writer/fastpath.zig");
        pub const encoder = @import("core/writer/encoder.zig");
        pub const streaming = @import("core/writer/streaming.zig");
    };
    pub const parquet = struct {
        pub const metadata = @import("core/parquet/metadata.zig");
        pub const schema_tree = @import("core/parquet/schema_tree.zig");
        pub const snappy = @import("core/parquet/snappy.zig");
        pub const lz4 = @import("core/parquet/lz4.zig");
        pub const compression = @import("core/parquet/compression.zig");
        pub const page = @import("core/parquet/page.zig");
        pub const column = @import("core/parquet/column.zig");
        pub const decimal = @import("core/parquet/decimal.zig");
        pub const int96 = @import("core/parquet/int96.zig");
        pub const fuzz_decode = @import("core/parquet/fuzz_decode.zig");
        pub const encoding = struct {
            pub const plain = @import("core/parquet/encoding/plain.zig");
            pub const hybrid_rle = @import("core/parquet/encoding/hybrid_rle.zig");
            pub const rle_dict = @import("core/parquet/encoding/rle_dict.zig");
            pub const delta_binary_packed = @import("core/parquet/encoding/delta_binary_packed.zig");
            pub const delta_byte_array = @import("core/parquet/encoding/delta_byte_array.zig");
        };
    };
};

pub const engine = @import("engine.zig");

pub const io = struct {
    pub const strategy = @import("io/strategy.zig");
    pub const loop = @import("io/loop.zig");
    pub const tls = @import("io/tls.zig");
    pub const sigv4 = @import("io/sigv4.zig");
    pub const http = @import("io/http.zig");
    pub const s3 = @import("io/s3.zig");
    pub const pool = @import("io/pool.zig");
    pub const coalescer = @import("io/coalescer.zig");
    pub const work_cursor = @import("io/work_cursor.zig");
    pub const multipart_sink = @import("io/multipart_sink.zig");
    pub const meta_cache = @import("io/meta_cache.zig");
    pub const retry = @import("io/retry.zig");
};

test {
    // Keep these as direct imports: tools/check_test_modules.py uses this block
    // as the manifest of modules whose tests the library test artifact roots.
    _ = @import("core/schema.zig");
    _ = @import("core/thrift.zig");
    _ = @import("core/consumer.zig");
    _ = @import("core/scan.zig");
    _ = @import("core/spawn.zig");
    _ = @import("core/invariant.zig");
    _ = @import("core/filter/ast.zig");
    _ = @import("core/filter/encoded.zig");
    _ = @import("core/filter/parser.zig");
    _ = @import("core/filter/prune.zig");
    _ = @import("core/filter/selection.zig");
    _ = @import("core/filter/eval.zig");
    _ = @import("core/filter/partition.zig");
    _ = @import("core/expr/ast.zig");
    _ = @import("core/expr/parser.zig");
    _ = @import("core/expr/eval.zig");
    _ = @import("core/expr/agg.zig");
    _ = @import("core/expr/sql_parser.zig");
    _ = @import("core/writer/fastpath.zig");
    _ = @import("core/writer/encoder.zig");
    _ = @import("core/writer/streaming.zig");
    _ = @import("core/system.zig");
    _ = @import("core/parquet/metadata.zig");
    _ = @import("core/parquet/schema_tree.zig");
    _ = @import("core/parquet/snappy.zig");
    _ = @import("core/parquet/lz4.zig");
    _ = @import("core/parquet/compression.zig");
    _ = @import("core/parquet/page.zig");
    _ = @import("core/parquet/column.zig");
    _ = @import("core/parquet/decimal.zig");
    _ = @import("core/parquet/int96.zig");
    _ = @import("core/parquet/fuzz_decode.zig");
    _ = @import("core/parquet/encoding/plain.zig");
    _ = @import("core/parquet/encoding/hybrid_rle.zig");
    _ = @import("core/parquet/encoding/rle_dict.zig");
    _ = @import("core/parquet/encoding/delta_binary_packed.zig");
    _ = @import("core/parquet/encoding/delta_byte_array.zig");
    _ = @import("engine.zig");
    _ = @import("io/strategy.zig");
    _ = @import("io/loop.zig");
    _ = @import("io/epoll.zig");
    _ = @import("io/tls.zig");
    _ = @import("io/sigv4.zig");
    _ = @import("io/http.zig");
    _ = @import("io/s3.zig");
    _ = @import("io/pool.zig");
    _ = @import("io/coalescer.zig");
    _ = @import("io/work_cursor.zig");
    _ = @import("io/multipart_sink.zig");
    _ = @import("io/meta_cache.zig");
    _ = @import("io/retry.zig");
}
