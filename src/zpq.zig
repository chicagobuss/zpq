const std = @import("std");

/// Centralized logging for zpq.
/// Note: Executables should define their own `std_options` to control log levels.
/// Example: `pub const std_options: std.Options = .{ .log_level = .warn };`
pub const log = @import("zpq/log.zig");

pub const core = struct {
    pub const file = @import("zpq/core/file.zig");
    pub const schema = @import("zpq/core/schema.zig");
    pub const column = @import("zpq/core/column.zig");
    pub const decoder = @import("zpq/core/decoder.zig");
    pub const encoder = @import("zpq/core/encoder.zig");
    pub const rle = @import("zpq/core/rle.zig");
    pub const snappy = @import("zpq/core/snappy.zig");
    pub const thrift = @import("zpq/core/thrift.zig");
    pub const writer = @import("zpq/core/writer.zig");
    pub const page_writer = @import("zpq/core/page_writer.zig");
};

pub const io = struct {
    pub const interface = @import("zpq/io/interface.zig");
    pub const http = @import("zpq/io/http/client.zig");
    pub const response_parser = @import("zpq/io/http/response_parser.zig");
    pub const tls = @import("zpq/io/tls/connection.zig");
};

// Aliases for compatibility
pub const file = core.file;
pub const schema = core.schema;
pub const column = core.column;
pub const decoder = core.decoder;
pub const rle = core.rle;
pub const snappy = core.snappy;

// S3 Implementation
pub const s3 = struct {
    pub const types = @import("zpq/io/s3/types.zig");
    pub const S3Config = types.S3Config;
    pub const Credentials = types.Credentials;

    // Legacy sync source (for comparison/fallback)
    pub const S3Source = @import("zpq/io/s3/sync.zig").S3Source;

    // Primary async S3 implementation (libxev + boring_tls)
    pub const XevS3Source = @import("zpq/io/s3/xev_source.zig").XevS3Source;
    pub const XevS3SourceGen = @import("zpq/io/s3/xev_source.zig").XevS3SourceGen;
    pub const XevConnectionPool = @import("zpq/io/s3/xev_connection_pool.zig").XevConnectionPool;
    pub const global_pool = @import("zpq/io/s3/global_pool.zig");
    pub const GlobalConnectionPool = global_pool.GlobalConnectionPool;

    // Internal Components
    pub const scheduler = @import("zpq/io/s3/scheduler.zig");
    pub const dns = @import("zpq/io/s3/dns.zig");
    pub const sigv4 = @import("zpq/io/s3/sigv4.zig");
    pub const factory = @import("zpq/io/s3/factory.zig");
};

test {
    _ = @import("zpq/core/thrift_test.zig");
    _ = core.rle;
    _ = io.interface;
    _ = s3.XevS3Source;
}
