const std = @import("std");
const xev = @import("xev");
const dns = @import("zpq").s3.dns;

pub fn main() !void {
    // Skip if ZPQ_TEST_NETWORK not set (requires internet access)
    if (std.posix.getenv("ZPQ_TEST_NETWORK") == null) {
        std.debug.print("SKIP: set ZPQ_TEST_NETWORK=1 to run (requires internet)\n", .{});
        return;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    // Note: ThreadPool.deinit() is join() which blocks.
    // We'll let it exit with the process for this test.

    var tp_resolver = dns.ThreadPoolResolver.init(&thread_pool, allocator);
    var sf_resolver = dns.SingleFlightResolver.init(allocator, tp_resolver.resolver());
    defer sf_resolver.deinit();

    var speculative = dns.SpeculativeResolver.init(allocator, sf_resolver.resolver());
    const resolver = speculative.resolver();

    const Context = struct {
        id: u32,
        results: ?[]const dns.Address = null,
        err: ?anyerror = null,
        done: bool = false,
        completion: dns.Resolver.Completion = dns.Resolver.Completion.init(),

        fn callback(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
            const self: *@This() = @ptrCast(@alignCast(ud));
            err catch |e| {
                self.err = e;
                std.debug.print("[{}] Error: {}\n", .{ self.id, e });
                self.done = true;
                return;
            };
            self.results = results;
            std.debug.print("[{}] Success: {} results\n", .{ self.id, results.len });
            for (results) |addr| {
                if (addr.any.family == std.posix.AF.INET) {
                    const in = addr.in;
                    std.debug.print("  IPv4: {}.{}.{}.{}\n", .{
                        @as(u8, @intCast(in.addr >> 0 & 0xff)),
                        @as(u8, @intCast(in.addr >> 8 & 0xff)),
                        @as(u8, @intCast(in.addr >> 16 & 0xff)),
                        @as(u8, @intCast(in.addr >> 24 & 0xff)),
                    });
                }
            }
            self.done = true;
        }
    };

    var ctxs: [3]Context = undefined;
    for (&ctxs, 0..) |*ctx, i| {
        ctx.* = .{
            .id = @intCast(i),
            .results = null,
            .err = null,
            .done = false,
            .completion = dns.Resolver.Completion.init(),
        };
        resolver.resolve(&loop, "google.com", 443, &ctx.completion, Context.callback, ctx);
    }

    std.debug.print("Main: Running loop for 3 parallel requests (Single-Flight should catch them)...\n", .{});
    while (true) {
        var all_done = true;
        for (ctxs) |ctx| {
            if (!ctx.done) all_done = false;
        }
        if (all_done) break;
        try loop.run(.once);
    }

    for (&ctxs) |*ctx| {
        ctx.completion.deinit(allocator);
    }

    std.debug.print("Main: Success!\n", .{});
}
