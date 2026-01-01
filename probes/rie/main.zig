//! Minimal probe to test ThreadPool + Async in Lambda RIE environment
//! Incrementally building up to match zpq's DNS resolver pattern

const std = @import("std");
const xev = @import("xev");

pub fn main() !void {
    std.debug.print("=== RIE ThreadPool Probe ===\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test 1: Simple ThreadPool + Async (direct, no abstraction)
    std.debug.print("\n[Test 1] Direct ThreadPool + Async...\n", .{});
    try testDirect();

    // Test 2: With resolver-like wrapper struct
    std.debug.print("\n[Test 2] Resolver-like wrapper...\n", .{});
    try testResolverLike(allocator);

    // Test 3: With generic XevApi pattern (like zpq)
    std.debug.print("\n[Test 3] Generic XevApi pattern...\n", .{});
    try testGenericApi(allocator);

    // Test 4: With actual getaddrinfo (like real DNS resolver)
    std.debug.print("\n[Test 4] With getaddrinfo (localhost)...\n", .{});
    try testWithGetaddrinfo(allocator, "localhost");

    // Test 5: With external hostname (like S3)
    std.debug.print("\n[Test 5] With getaddrinfo (external host)...\n", .{});
    try testWithGetaddrinfo(allocator, "s3.us-east-1.amazonaws.com");

    std.debug.print("\n=== All tests complete ===\n", .{});
}

fn testDirect() !void {
    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();

    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }

    var async_handle = try xev.Epoll.Async.init();
    defer async_handle.deinit();

    var done = false;
    var c: xev.Epoll.Completion = .{};
    async_handle.wait(&loop, &c, bool, &done, struct {
        fn cb(ud: ?*bool, _: *xev.Epoll.Loop, _: *xev.Epoll.Completion, r: xev.Epoll.Async.WaitError!void) xev.Epoll.CallbackAction {
            _ = r catch unreachable;
            std.debug.print("  Callback fired!\n", .{});
            ud.?.* = true;
            return .disarm;
        }
    }.cb);

    const TaskCtx = struct {
        task: xev.ThreadPool.Task,
        async_handle: *xev.Epoll.Async,
    };
    var tctx = TaskCtx{
        .task = .{ .callback = struct {
            fn cb(t: *xev.ThreadPool.Task) void {
                std.debug.print("  Task running, notifying...\n", .{});
                const self: *TaskCtx = @fieldParentPtr("task", t);
                self.async_handle.notify() catch {};
            }
        }.cb },
        .async_handle = &async_handle,
    };

    pool.schedule(xev.ThreadPool.Batch.from(&tctx.task));

    var iters: usize = 0;
    while (!done and iters < 100) : (iters += 1) {
        try loop.run(.once);
    }
    std.debug.print("  Result: {s} (iters: {d})\n", .{ if (done) "PASS" else "FAIL", iters });
}

fn testResolverLike(allocator: std.mem.Allocator) !void {
    _ = allocator;

    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();

    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }

    // Mimic the Completion struct pattern from dns.zig
    const Completion = struct {
        xev_completion: xev.Epoll.Completion = .{},
        xev_async: ?xev.Epoll.Async = null,
        task: xev.ThreadPool.Task = undefined,
        callback: *const fn (?*anyopaque, bool) void = undefined,
        userdata: ?*anyopaque = null,
        done: bool = false,
    };

    var comp = Completion{};
    comp.xev_async = try xev.Epoll.Async.init();
    defer if (comp.xev_async) |*a| a.deinit();

    var result_done = false;
    comp.callback = struct {
        fn cb(ud: ?*anyopaque, success: bool) void {
            std.debug.print("  Resolver callback fired! success={}\n", .{success});
            const d: *bool = @ptrCast(@alignCast(ud));
            d.* = success;
        }
    }.cb;
    comp.userdata = &result_done;

    // Arm async wait
    comp.xev_async.?.wait(&loop, &comp.xev_completion, Completion, &comp, struct {
        fn cb(ud: ?*Completion, _: *xev.Epoll.Loop, _: *xev.Epoll.Completion, r: xev.Epoll.Async.WaitError!void) xev.Epoll.CallbackAction {
            _ = r catch unreachable;
            std.debug.print("  Async callback, invoking user callback...\n", .{});
            const c = ud.?;
            c.done = true;
            c.callback(c.userdata, true);
            return .disarm;
        }
    }.cb);

    // Schedule task
    comp.task = .{ .callback = struct {
        fn cb(t: *xev.ThreadPool.Task) void {
            std.debug.print("  Task running...\n", .{});
            const c: *Completion = @fieldParentPtr("task", t);
            std.debug.print("  Task notifying async...\n", .{});
            if (c.xev_async) |*a| a.notify() catch {};
        }
    }.cb };

    pool.schedule(xev.ThreadPool.Batch.from(&comp.task));

    var iters: usize = 0;
    while (!comp.done and iters < 100) : (iters += 1) {
        try loop.run(.once);
    }
    std.debug.print("  Result: {s} (iters: {d})\n", .{ if (result_done) "PASS" else "FAIL", iters });
}

// Generic pattern like zpq uses
fn ThreadPoolResolverGen(comptime XevApi: type) type {
    const LoopType = XevApi.Loop;

    return struct {
        const Self = @This();
        pool: *xev.ThreadPool,
        allocator: std.mem.Allocator,

        pub const Completion = struct {
            xev_completion: XevApi.Completion = .{},
            xev_async: ?XevApi.Async = null,
            task: xev.ThreadPool.Task = undefined,
            callback: *const fn (?*anyopaque, bool) void = undefined,
            userdata: ?*anyopaque = null,
            done: bool = false,
            resolver_ptr: ?*anyopaque = null,
        };

        pub fn init(pool: *xev.ThreadPool, allocator: std.mem.Allocator) Self {
            return .{ .pool = pool, .allocator = allocator };
        }

        pub fn resolve(
            self: *Self,
            loop: *LoopType,
            completion: *Completion,
            cb: *const fn (?*anyopaque, bool) void,
            userdata: ?*anyopaque,
        ) void {
            completion.callback = cb;
            completion.userdata = userdata;
            completion.resolver_ptr = self;
            completion.xev_async = XevApi.Async.init() catch |err| {
                std.debug.print("  Async.init failed: {}\n", .{err});
                return;
            };

            completion.task = .{ .callback = threadCallback };

            // Arm async wait
            completion.xev_async.?.wait(loop, &completion.xev_completion, Completion, completion, asyncCallback);

            // Schedule task
            self.pool.schedule(xev.ThreadPool.Batch.from(&completion.task));
        }

        fn threadCallback(task: *xev.ThreadPool.Task) void {
            std.debug.print("  [Generic] Task running...\n", .{});
            const completion: *Completion = @fieldParentPtr("task", task);
            std.debug.print("  [Generic] Task notifying async...\n", .{});
            if (completion.xev_async) |*a| a.notify() catch {};
        }

        fn asyncCallback(
            ud: ?*Completion,
            _: *LoopType,
            _: *XevApi.Completion,
            r: XevApi.Async.WaitError!void,
        ) XevApi.CallbackAction {
            _ = r catch unreachable;
            std.debug.print("  [Generic] Async callback fired!\n", .{});
            const completion = ud.?;
            completion.done = true;
            completion.callback(completion.userdata, true);
            return .disarm;
        }
    };
}

fn testGenericApi(allocator: std.mem.Allocator) !void {
    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();

    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }

    const Resolver = ThreadPoolResolverGen(xev.Epoll);
    var resolver = Resolver.init(&pool, allocator);

    var comp = Resolver.Completion{};
    defer if (comp.xev_async) |*a| a.deinit();

    var result_done = false;

    resolver.resolve(&loop, &comp, struct {
        fn cb(ud: ?*anyopaque, success: bool) void {
            std.debug.print("  [Generic] User callback! success={}\n", .{success});
            const d: *bool = @ptrCast(@alignCast(ud));
            d.* = success;
        }
    }.cb, &result_done);

    var iters: usize = 0;
    while (!comp.done and iters < 100) : (iters += 1) {
        try loop.run(.once);
    }
    std.debug.print("  Result: {s} (iters: {d})\n", .{ if (result_done) "PASS" else "FAIL", iters });
}

// Test with actual getaddrinfo like zpq's DNS resolver
fn DnsResolverGen(comptime XevApi: type) type {
    const LoopType = XevApi.Loop;

    return struct {
        const Self = @This();
        pool: *xev.ThreadPool,
        allocator: std.mem.Allocator,

        pub const Completion = struct {
            hostname: []const u8 = "",
            xev_completion: XevApi.Completion = .{},
            xev_async: ?XevApi.Async = null,
            task: xev.ThreadPool.Task = undefined,
            callback: *const fn (?*anyopaque, ?[]const u8, ?anyerror) void = undefined,
            userdata: ?*anyopaque = null,
            done: bool = false,
            resolver_ptr: ?*anyopaque = null,
            result: ?[]const u8 = null,
            err: ?anyerror = null,
        };

        pub fn init(pool: *xev.ThreadPool, allocator: std.mem.Allocator) Self {
            return .{ .pool = pool, .allocator = allocator };
        }

        pub fn resolve(
            self: *Self,
            loop: *LoopType,
            hostname: []const u8,
            completion: *Completion,
            cb: *const fn (?*anyopaque, ?[]const u8, ?anyerror) void,
            userdata: ?*anyopaque,
        ) void {
            completion.hostname = hostname;
            completion.callback = cb;
            completion.userdata = userdata;
            completion.resolver_ptr = self;
            completion.xev_async = XevApi.Async.init() catch |err| {
                std.debug.print("  Async.init failed: {}\n", .{err});
                cb(userdata, null, err);
                return;
            };

            completion.task = .{ .callback = threadCallback };

            // Arm async wait
            completion.xev_async.?.wait(loop, &completion.xev_completion, Completion, completion, asyncCallback);

            // Schedule task
            self.pool.schedule(xev.ThreadPool.Batch.from(&completion.task));
        }

        fn threadCallback(task: *xev.ThreadPool.Task) void {
            const completion: *Completion = @fieldParentPtr("task", task);
            const self: *Self = @ptrCast(@alignCast(completion.resolver_ptr.?));

            std.debug.print("  [DNS] Task running, resolving {s}...\n", .{completion.hostname});

            // Actually call getaddrinfo
            const hostname_z = self.allocator.dupeZ(u8, completion.hostname) catch {
                completion.err = error.OutOfMemory;
                if (completion.xev_async) |*a| a.notify() catch {};
                return;
            };
            defer self.allocator.free(hostname_z);

            var hints: std.c.addrinfo = std.mem.zeroInit(std.c.addrinfo, .{
                .family = std.c.AF.UNSPEC,
                .socktype = std.c.SOCK.STREAM,
            });
            var res: ?*std.c.addrinfo = null;
            const rc = std.c.getaddrinfo(hostname_z.ptr, "443", &hints, &res);

            if (@intFromEnum(rc) != 0) {
                std.debug.print("  [DNS] getaddrinfo failed: {d}\n", .{@intFromEnum(rc)});
                completion.err = error.DnsResolutionFailed;
            } else if (res != null) {
                std.debug.print("  [DNS] getaddrinfo succeeded!\n", .{});
                std.c.freeaddrinfo(res.?);
                completion.result = "resolved";
            } else {
                completion.err = error.HostNotFound;
            }

            std.debug.print("  [DNS] Task notifying async...\n", .{});
            if (completion.xev_async) |*a| a.notify() catch {};
        }

        fn asyncCallback(
            ud: ?*Completion,
            _: *LoopType,
            _: *XevApi.Completion,
            r: XevApi.Async.WaitError!void,
        ) XevApi.CallbackAction {
            _ = r catch unreachable;
            std.debug.print("  [DNS] Async callback fired!\n", .{});
            const completion = ud.?;
            completion.done = true;
            completion.callback(completion.userdata, completion.result, completion.err);
            return .disarm;
        }
    };
}

fn testWithGetaddrinfo(allocator: std.mem.Allocator, hostname: []const u8) !void {
    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();

    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }

    const Resolver = DnsResolverGen(xev.Epoll);
    var resolver = Resolver.init(&pool, allocator);

    var comp = Resolver.Completion{};
    defer if (comp.xev_async) |*a| a.deinit();

    var result_done = false;
    var result_err: ?anyerror = null;

    const Ctx = struct {
        done: *bool,
        err: *?anyerror,
    };
    var ctx = Ctx{ .done = &result_done, .err = &result_err };

    resolver.resolve(&loop, hostname, &comp, struct {
        fn cb(ud: ?*anyopaque, result: ?[]const u8, err: ?anyerror) void {
            std.debug.print("  [DNS] User callback! result={?s}, err={?}\n", .{ result, err });
            const c: *Ctx = @ptrCast(@alignCast(ud));
            c.done.* = true;
            c.err.* = err;
        }
    }.cb, &ctx);

    var iters: usize = 0;
    while (!comp.done and iters < 100) : (iters += 1) {
        try loop.run(.once);
    }
    std.debug.print("  Result: {s} (iters: {d}, err: {?})\n", .{
        if (result_done and result_err == null) "PASS" else "FAIL",
        iters,
        result_err,
    });
}
