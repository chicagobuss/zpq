//! ZPQ — pure sans-IO core surface.
//!
//! Anything I/O-related lives behind the io.* namespace and must satisfy
//! io.strategy.assertIsIOStrategy. Anything in core.* is pure logic and
//! must remain free of allocations beyond what callers pass in.

pub const core = struct {
    pub const selected_batch = @import("core/selected_batch.zig");
    pub const schema = @import("core/schema.zig");
    pub const thrift = @import("core/thrift.zig");
    pub const consumer = @import("core/consumer.zig");
    pub const invariant = @import("core/invariant.zig");
    pub const scan = @import("core/scan.zig");
    pub const diag = @import("core/diag.zig");
    pub const spawn = @import("core/spawn.zig");
    pub const huge_pages = @import("core/huge_pages.zig");
    pub const memory = @import("core/memory.zig");
    pub const filter = struct {
        pub const ast = @import("core/filter/ast.zig");
        pub const encoded = @import("core/filter/encoded.zig");
        pub const parser = @import("core/filter/parser.zig");
        pub const prune = @import("core/filter/prune.zig");
        pub const membership = @import("core/filter/membership.zig");
        pub const nanprobe = @import("core/filter/nanprobe.zig");
        pub const selection = @import("core/filter/selection.zig");
        pub const eval = @import("core/filter/eval.zig");
        pub const in_set = @import("core/filter/in_set.zig");
        pub const partition = @import("core/filter/partition.zig");
    };
    pub const expr = struct {
        pub const key_pack = @import("core/expr/key_pack.zig");
        pub const ast = @import("core/expr/ast.zig");
        pub const parser = @import("core/expr/parser.zig");
        pub const eval = @import("core/expr/eval.zig");
        pub const agg = @import("core/expr/agg.zig");
        // The SQL frontend (CLI and libzpq) is excluded from the Lambda
        // build, so the module isn't compiled there (no @cImport, no C dep).
        pub const sql_parser = if (@import("build_options").enable_sql)
            @import("core/expr/sql_parser.zig")
        else
            struct {};
    };
    pub const system = @import("core/system.zig");
    pub const bytes = @import("core/bytes.zig");
    pub const writer = struct {
        pub const fastpath = @import("core/writer/fastpath.zig");
        pub const encoder = @import("core/writer/encoder.zig");
        pub const streaming = @import("core/writer/streaming.zig");
        pub const page_index = @import("core/writer/page_index.zig");
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
        pub const statistics = @import("core/parquet/statistics.zig");
        pub const bloom = @import("core/parquet/bloom.zig");
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

pub const frontend = @import("frontend.zig");
pub const engine = @import("engine.zig");
pub const clock = @import("clock.zig");
pub const profile = @import("profile.zig");
pub const profile_summary = @import("profile_summary.zig");
pub const output_publication = @import("output_publication.zig");
pub const local_fs = @import("local_fs.zig");
pub const cpu_level = @import("cpu_level.zig");

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
    pub const local_reader = @import("io/local_reader.zig");
    pub const retry = @import("io/retry.zig");
};

test {
    // The test-bearing files under this root, listed by build.zig (tests.gen.zig).
    _ = @import("tests.gen.zig");
}
