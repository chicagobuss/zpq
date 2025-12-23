const std = @import("std");
const zpq = @import("zpq");
const dns = zpq.s3.dns;
const xev = dns.xev;

// Mimics the factory's S3Context struct layout exactly
const TestContext = struct {
    pool: zpq.s3.ConnectionPool,
    source: zpq.s3.AsyncS3Source,
    allocator: std.mem.Allocator,
    host_owned: ?[]const u8 = null,
    thread_pool: xev.ThreadPool,
    tp_resolver: dns.ThreadPoolResolver,
    sf_resolver: dns.SingleFlightResolver,
    spec_resolver: dns.SpeculativeResolver,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Test 1: Stack allocation (working) ===\n", .{});
    {
        var thread_pool = xev.ThreadPool.init(.{});
        defer {
            thread_pool.shutdown();
            thread_pool.deinit();
        }

        var tp_resolver = dns.ThreadPoolResolver.init(&thread_pool, allocator);
        defer tp_resolver.deinit();

        var sf_resolver = dns.SingleFlightResolver.init(allocator, tp_resolver.resolver());
        std.debug.print("sf_resolver ptr: {*}\n", .{&sf_resolver});
        std.debug.print("inflight count: {d}\n", .{sf_resolver.inflight.count()});
        sf_resolver.deinit();
        std.debug.print("Stack test passed!\n", .{});
    }

    std.debug.print("\n=== Test 2: Heap allocation (like factory) ===\n", .{});
    {
        const ctx = try allocator.create(TestContext);
        errdefer allocator.destroy(ctx);

        std.debug.print("ctx ptr: {*}\n", .{ctx});

        ctx.allocator = allocator;

        ctx.pool = zpq.s3.ConnectionPool.init(allocator);
        errdefer ctx.pool.deinit();

        ctx.thread_pool = xev.ThreadPool.init(.{});
        errdefer {
            ctx.thread_pool.shutdown();
            ctx.thread_pool.deinit();
        }

        ctx.tp_resolver = dns.ThreadPoolResolver.init(&ctx.thread_pool, allocator);
        errdefer ctx.tp_resolver.deinit();

        std.debug.print("About to init sf_resolver...\n", .{});
        ctx.sf_resolver = dns.SingleFlightResolver.init(allocator, ctx.tp_resolver.resolver());
        std.debug.print("sf_resolver initialized\n", .{});

        std.debug.print("sf_resolver field ptr: {*}\n", .{&ctx.sf_resolver});
        std.debug.print("inflight count: {d}\n", .{ctx.sf_resolver.inflight.count()});

        std.debug.print("About to deinit sf_resolver...\n", .{});
        ctx.sf_resolver.deinit();
        std.debug.print("sf_resolver deinit done\n", .{});

        ctx.tp_resolver.deinit();
        ctx.pool.deinit();
        ctx.thread_pool.shutdown();
        ctx.thread_pool.deinit();
        allocator.destroy(ctx);

        std.debug.print("Heap test passed!\n", .{});
    }

    std.debug.print("\n=== Test 3: Heap allocation with spec_resolver ===\n", .{});
    {
        const ctx = try allocator.create(TestContext);

        ctx.allocator = allocator;
        ctx.pool = zpq.s3.ConnectionPool.init(allocator);
        ctx.thread_pool = xev.ThreadPool.init(.{});
        ctx.tp_resolver = dns.ThreadPoolResolver.init(&ctx.thread_pool, allocator);
        ctx.sf_resolver = dns.SingleFlightResolver.init(allocator, ctx.tp_resolver.resolver());
        ctx.spec_resolver = dns.SpeculativeResolver.init(allocator, ctx.sf_resolver.resolver());

        std.debug.print("All resolvers initialized, cleaning up normally...\n", .{});

        ctx.spec_resolver.deinit();
        ctx.sf_resolver.deinit();
        ctx.tp_resolver.deinit();
        ctx.pool.deinit();
        ctx.thread_pool.shutdown();
        ctx.thread_pool.deinit();
        allocator.destroy(ctx);

        std.debug.print("Test 3 passed!\n", .{});
    }

    std.debug.print("\n=== Test 4: With actual DNS resolution ===\n", .{});
    {
        const ctx = try allocator.create(TestContext);
        errdefer allocator.destroy(ctx);

        ctx.allocator = allocator;

        ctx.pool = zpq.s3.ConnectionPool.init(allocator);
        errdefer ctx.pool.deinit();

        ctx.thread_pool = xev.ThreadPool.init(.{});
        errdefer {
            ctx.thread_pool.shutdown();
            ctx.thread_pool.deinit();
        }

        ctx.tp_resolver = dns.ThreadPoolResolver.init(&ctx.thread_pool, allocator);
        errdefer ctx.tp_resolver.deinit();

        ctx.sf_resolver = dns.SingleFlightResolver.init(allocator, ctx.tp_resolver.resolver());
        errdefer ctx.sf_resolver.deinit();

        ctx.spec_resolver = dns.SpeculativeResolver.init(allocator, ctx.sf_resolver.resolver());
        errdefer ctx.spec_resolver.deinit();

        // Now actually do DNS resolution like AsyncS3Source does
        std.debug.print("Doing DNS resolution...\n", .{});

        var loop = try zpq.s3.EventLoop.init(allocator);
        defer loop.deinit();

        var dns_comp = dns.Resolver.Completion.init();
        defer dns_comp.deinit(allocator);

        const DnsCtx = struct {
            results: []dns.Address = &.{},
            err: ?anyerror = null,
            done: bool = false,
            alloc: std.mem.Allocator,

            fn callback(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
                const self: *@This() = @ptrCast(@alignCast(ud));
                err catch |e| {
                    self.err = e;
                    self.done = true;
                    return;
                };
                const copy = self.alloc.alloc(dns.Address, results.len) catch |e| {
                    self.err = e;
                    self.done = true;
                    return;
                };
                @memcpy(copy, results);
                self.results = copy;
                self.done = true;
            }
        };

        var dns_ctx = DnsCtx{ .alloc = allocator };
        ctx.spec_resolver.resolver().resolve(loop.loop, "s3.amazonaws.com", 443, &dns_comp, DnsCtx.callback, &dns_ctx);

        std.debug.print("Waiting for DNS...\n", .{});
        while (!dns_ctx.done) {
            _ = try loop.tick();
        }

        if (dns_ctx.err) |err| {
            std.debug.print("DNS error: {}\n", .{err});
            // This will trigger errdefers
            return err;
        }

        std.debug.print("DNS resolved {d} addresses\n", .{dns_ctx.results.len});
        allocator.free(dns_ctx.results);

        std.debug.print("sf_resolver inflight count after resolution: {d}\n", .{ctx.sf_resolver.inflight.count()});

        // Clean up
        ctx.spec_resolver.deinit();
        ctx.sf_resolver.deinit();
        ctx.tp_resolver.deinit();
        ctx.pool.deinit();
        ctx.thread_pool.shutdown();
        ctx.thread_pool.deinit();
        allocator.destroy(ctx);

        std.debug.print("Test 4 passed!\n", .{});
    }

    std.debug.print("\n=== Test 5: Full AsyncS3Source.init (will fail on HEAD) ===\n", .{});
    {
        const ctx = try allocator.create(TestContext);
        errdefer allocator.destroy(ctx);

        ctx.allocator = allocator;
        ctx.host_owned = try allocator.dupe(u8, "s3.amazonaws.com");
        errdefer allocator.free(ctx.host_owned.?);

        ctx.pool = zpq.s3.ConnectionPool.init(allocator);
        errdefer ctx.pool.deinit();

        ctx.thread_pool = xev.ThreadPool.init(.{});
        errdefer {
            ctx.thread_pool.shutdown();
            ctx.thread_pool.deinit();
        }

        ctx.tp_resolver = dns.ThreadPoolResolver.init(&ctx.thread_pool, allocator);
        errdefer ctx.tp_resolver.deinit();

        ctx.sf_resolver = dns.SingleFlightResolver.init(allocator, ctx.tp_resolver.resolver());
        errdefer ctx.sf_resolver.deinit();

        ctx.spec_resolver = dns.SpeculativeResolver.init(allocator, ctx.sf_resolver.resolver());
        errdefer ctx.spec_resolver.deinit();

        std.debug.print("Calling AsyncS3Source.init (will fail - no credentials)...\n", .{});

        // This should fail because we don't have valid credentials/bucket
        ctx.source = zpq.s3.AsyncS3Source.init(
            allocator,
            &ctx.pool,
            ctx.spec_resolver.resolver(),
            ctx.host_owned.?,
            443,
            "fake-bucket",
            "fake-key",
            true,
            null,
            null, // no config/credentials
        ) catch |err| {
            std.debug.print("AsyncS3Source.init failed as expected: {}\n", .{err});
            std.debug.print("This should trigger errdefers - sf_resolver.deinit will be called\n", .{});
            return err;
        };

        std.debug.print("Unexpectedly succeeded!\n", .{});
    }
}
