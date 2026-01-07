const std = @import("std");
const xev = @import("xev");
const interface = @import("../interface.zig");
const transport = @import("../transport.zig");
const protocol = @import("../../protocol/s3.zig");

const log = std.log.scoped(.s3_source);

/// Implementation of RandomAccessSource for AWS S3.
/// Uses the libxev-based transport for non-blocking network I/O.
pub const S3Source = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    resolver: transport.Resolver,

    s3_proto: protocol.S3,
    host: []const u8,
    key: []const u8,

    // Cached state
    file_size: ?u64 = null,
    addr: ?xev.shim_net.Address = null,

    pub const Options = struct {
        bucket: []const u8,
        key: []const u8,
        region: []const u8,
        access_key: []const u8,
        secret_key: []const u8,
        session_token: ?[]const u8 = null,
        use_tls: bool = true,
    };

    pub fn init(allocator: std.mem.Allocator, loop: *xev.Loop, resolver: transport.Resolver, options: Options) !*S3Source {
        const self = try allocator.create(S3Source);

        const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ options.bucket, options.region });
        errdefer allocator.free(host);

        self.* = .{
            .allocator = allocator,
            .loop = loop,
            .resolver = resolver,
            .s3_proto = protocol.S3.init(
                options.bucket,
                options.region,
                options.access_key,
                options.secret_key,
                options.session_token,
            ),
            .host = host,
            .key = try allocator.dupe(u8, options.key),
        };
        return self;
    }

    pub fn deinit(self: *S3Source) void {
        self.allocator.free(self.host);
        self.allocator.free(self.key);
        self.allocator.destroy(self);
    }

    /// Fetches the object size using an S3 HEAD request.
    /// This is usually the first operation performed on a Parquet file.
    pub fn fetchSize(self: *S3Source) !u64 {
        if (self.file_size) |sz| return sz;

        // TODO: Implement completion-based fetchSize using transport.Connection
        // 1. Resolve address if null
        // 2. Connect
        // 3. Send signed HEAD request
        // 4. Parse Content-Length from response

        return error.NotImplemented;
    }

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *S3Source = @ptrCast(@alignCast(ptr));

        // S3 logic requires a range request.
        // For synchronous-looking readAt, we might need to block on the loop
        // or provide a different async-first interface.
        _ = self;
        _ = offset;
        _ = buf;

        return error.NotImplemented;
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *S3Source = @ptrCast(@alignCast(ptr));
        if (self.file_size) |sz| return sz;

        // In the new architecture, we prefer explicit pre-fetching of size.
        // If size is unknown, we might have to block (discouraged).
        return 0;
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *S3Source = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    pub fn source(self: *S3Source) interface.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .readRanges = null,
                .size = sizeImpl,
                .close = closeImpl,
                .getSlice = null,
            },
        };
    }
};

