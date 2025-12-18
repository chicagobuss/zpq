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

// Legacy S3 (moved)
pub const s3 = struct {
    pub const AsyncS3Source = @import("zpq/s3_legacy/async_s3_source.zig").AsyncS3Source;
    pub const ConnectionPool = @import("zpq/s3_legacy/connection_pool.zig").ConnectionPool;
    pub const S3Source = @import("zpq/io/s3/std.zig").S3Source; // The old "standard" one
    
    // Legacy Internal Components (for tests)
    pub const AsyncRequest = @import("zpq/s3_legacy/async_request.zig").AsyncRequest;
    pub const EventLoop = @import("zpq/s3_legacy/event_loop.zig").EventLoop;
    pub const TlsAdapter = @import("zpq/s3_legacy/tls_adapter.zig").TlsAdapter;
    pub const scheduler = @import("zpq/s3_legacy/scheduler.zig");
};

test {
    _ = @import("zpq/core/thrift_test.zig");
    _ = core.rle;
    _ = io.interface;
    _ = s3.AsyncS3Source;
}
