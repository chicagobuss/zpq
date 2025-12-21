const std = @import("std");

pub const core = struct {
    pub const file = @import("zpq/core/file.zig");
    pub const schema = @import("zpq/core/schema.zig");
    pub const column = @import("zpq/core/column.zig");
    pub const decoder = @import("zpq/core/decoder.zig");
    pub const rle = @import("zpq/core/rle.zig");
    pub const snappy = @import("zpq/core/snappy.zig");
};

pub const io = struct {
    pub const interface = @import("zpq/io/interface.zig");
    pub const http = @import("zpq/io/http/client.zig");
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

    pub const S3Source = @import("zpq/io/s3/sync.zig").S3Source;
    pub const AsyncS3Source = @import("zpq/io/s3/async_source.zig").AsyncS3Source;
    pub const ConnectionPool = @import("zpq/io/s3/connection_pool.zig").ConnectionPool;

    // Internal Components
    pub const AsyncRequest = @import("zpq/io/s3/request.zig").AsyncRequest;
    pub const EventLoop = @import("zpq/io/s3/event_loop.zig").EventLoop;
    pub const TlsAdapter = @import("zpq/io/s3/tls_adapter.zig").TlsAdapter;
    pub const scheduler = @import("zpq/io/s3/scheduler.zig");
};

test {
    _ = @import("zpq/core/thrift_test.zig");
    _ = core.rle;
    _ = io.interface;
    _ = s3.AsyncS3Source;
}
