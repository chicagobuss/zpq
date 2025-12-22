const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const dns = zpq.s3.dns;

const NUM_REQUESTS = 50;
const HOSTNAME = "google.com";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var thread_pool = xev.ThreadPool.init(.{});
    defer thread_pool.deinit();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    std.debug.print("\n--- DNS Benchmark ---\n", .{});

    try benchSerial(allocator);
    try benchAsync(allocator, &loop, &thread_pool);
    try benchSingleFlight(allocator, &loop, &thread_pool);
}

fn benchSerial(allocator: std.mem.Allocator) !void {
    _ = allocator;
    std.debug.print("1. Serial Blocking (std.c.getaddrinfo)...\n", .{});
    
    var timer = try std.time.Timer.start();
    var i: usize = 0;
    while (i < NUM_REQUESTS) : (i += 1) {
        var hints = std.mem.zeroes(std.c.addrinfo);
        hints.family = std.posix.AF.UNSPEC;
        hints.socktype = std.posix.SOCK.STREAM;
        hints.protocol = std.posix.IPPROTO.TCP;
        var res: ?*std.c.addrinfo = null;
        const rc = std.c.getaddrinfo(HOSTNAME, "443", &hints, &res);
        if (@intFromEnum(rc) == 0) {
            if (res) |r| std.c.freeaddrinfo(r);
        }
    }
    const elapsed = timer.read();
    std.debug.print("   Completed {d} serial resolutions in {d:.3}ms\n", .{ NUM_REQUESTS, @as(f64, @floatFromInt(elapsed)) / 1_000_000.0 });
}

const Context = struct {
    id: usize,
    done: bool = false,
    results_len: usize = 0,
    completion: dns.Resolver.Completion = .{},

    pub fn onResolve(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
        const self = @as(*Context, @ptrCast(@alignCast(ud)));
        err catch |e| {
            std.debug.print("   [{d}] Resolution error: {}\n", .{self.id, e});
        };
        self.results_len = results.len;
        self.done = true;
    }
};

fn benchAsync(allocator: std.mem.Allocator, loop: *xev.Loop, thread_pool: *xev.ThreadPool) !void {
    std.debug.print("2. Async ThreadPool (No Deduplication)...\n", .{});
    
    var tp_resolver = dns.ThreadPoolResolver.init(thread_pool, allocator);
    const resolver = tp_resolver.resolver();

    const ctxs = try allocator.alloc(Context, NUM_REQUESTS);
    defer allocator.free(ctxs);

    var timer = try std.time.Timer.start();
    for (ctxs, 0..) |*ctx, i| {
        ctx.* = .{ .id = i };
        resolver.resolve(loop, HOSTNAME, 443, &ctx.completion, Context.onResolve, ctx);
    }

    while (true) {
        var all_done = true;
        for (ctxs) |ctx| {
            if (!ctx.done) all_done = false;
        }
        if (all_done) break;
        try loop.run(.once);
    }

    const elapsed = timer.read();
    std.debug.print("   Completed {d} async resolutions in {d:.3}ms\n", .{ NUM_REQUESTS, @as(f64, @floatFromInt(elapsed)) / 1_000_000.0 });

    for (ctxs) |*ctx| {
        ctx.completion.deinit(allocator);
    }
}

fn benchSingleFlight(allocator: std.mem.Allocator, loop: *xev.Loop, thread_pool: *xev.ThreadPool) !void {
    std.debug.print("3. Async + Single-Flight (Deduplication)...\n", .{});
    
    var tp_resolver = dns.ThreadPoolResolver.init(thread_pool, allocator);
    var sf_resolver = dns.SingleFlightResolver.init(allocator, tp_resolver.resolver());
    defer sf_resolver.deinit();
    
    const resolver = sf_resolver.resolver();

    const ctxs = try allocator.alloc(Context, NUM_REQUESTS);
    defer allocator.free(ctxs);

    var timer = try std.time.Timer.start();
    for (ctxs, 0..) |*ctx, i| {
        ctx.* = .{ .id = i };
        resolver.resolve(loop, HOSTNAME, 443, &ctx.completion, Context.onResolve, ctx);
    }

    while (true) {
        var all_done = true;
        for (ctxs) |ctx| {
            if (!ctx.done) all_done = false;
        }
        if (all_done) break;
        try loop.run(.once);
    }

    const elapsed = timer.read();
    std.debug.print("   Completed {d} async resolutions (deduplicated) in {d:.3}ms\n", .{ NUM_REQUESTS, @as(f64, @floatFromInt(elapsed)) / 1_000_000.0 });

    for (ctxs) |*ctx| {
        ctx.completion.deinit(allocator);
    }
    std.process.exit(0);
}
