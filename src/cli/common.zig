const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

pub const Resolver = zpq.s3.dns.ResolverGen(xev);

/// Common context passed to all CLI commands
pub const Context = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    resolver: Resolver,
    is_async: bool,
    verify_tls: bool,

    pub fn openFile(self: *const Context, path: []const u8) !zpq.file.ParquetFile {
        return zpq.s3.factory.openFileWithOptions(
            self.allocator,
            path,
            .{
                .force_async = self.is_async,
                .loop = self.loop,
                .thread_pool = self.thread_pool,
                .resolver = self.resolver,
                .verify_tls = self.verify_tls,
            },
        );
    }
};
