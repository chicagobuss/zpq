const std = @import("std");
const zpq_log = @import("../../../zpq.zig").log;

const log = zpq_log.dns;

pub fn ResolverGen(comptime XevApi: type) type {
    const LoopType = XevApi.Loop;
    const Address = @import("xev").shim_net.Address;

    return struct {
        const Self = @This();
        ptr: *anyopaque,
        vtable: *const VTable,

        pub const VTable = struct {
            resolve: *const fn (
                ptr: *anyopaque,
                loop: *LoopType,
                hostname: []const u8,
                port: u16,
                completion: *Completion,
                cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void,
                userdata: ?*anyopaque,
            ) void,
        };

        pub const Internal = struct {
            xev_completion: XevApi.Completion = .{},
            xev_async: ?XevApi.Async = null,
            task: @import("xev").ThreadPool.Task = undefined,
            callback: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void = undefined,
            userdata: ?*anyopaque = null,
            results: []Address = &.{},
            err: ?anyerror = null,
            resolver_ptr: ?*anyopaque = null,
        };

        pub const Completion = struct {
            hostname: []const u8 = "",
            port: u16 = 0,

            // Internal state for the resolver implementation
            internal: Internal = .{},

            pub fn init() Completion {
                return .{};
            }

            pub fn deinit(self: *Completion, allocator: std.mem.Allocator) void {
                if (self.internal.results.len > 0) {
                    allocator.free(self.internal.results);
                    self.internal.results = &.{};
                }
            }
        };

        pub fn resolve(
            self: Self,
            loop: *LoopType,
            hostname: []const u8,
            port: u16,
            completion: *Completion,
            cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void,
            userdata: ?*anyopaque,
        ) void {
            completion.hostname = hostname;
            completion.port = port;
            self.vtable.resolve(self.ptr, loop, hostname, port, completion, cb, userdata);
        }
    };
}

pub fn ThreadPoolResolverGen(comptime XevApi: type) type {
    const LoopType = XevApi.Loop;
    const Res = ResolverGen(XevApi);
    const Address = @import("xev").shim_net.Address;

    return struct {
        const Self = @This();
        pool: *@import("xev").ThreadPool,
        allocator: std.mem.Allocator,

        pub fn init(pool: *@import("xev").ThreadPool, allocator: std.mem.Allocator) Self {
            return .{
                .pool = pool,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            _ = self;
        }

        pub fn resolver(self: *Self) Res {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve = resolve,
                },
            };
        }

        fn resolve(
            ptr: *anyopaque,
            loop: *LoopType,
            hostname: []const u8,
            port: u16,
            completion: *Res.Completion,
            cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void,
            userdata: ?*anyopaque,
        ) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            _ = hostname;
            _ = port;

            log.debug("ThreadPoolResolver.resolve: setting up async for {s}:{d}", .{ completion.hostname, completion.port });

            // Setup completion
            completion.internal.callback = cb;
            completion.internal.userdata = userdata;
            completion.internal.resolver_ptr = self;
            completion.internal.xev_async = XevApi.Async.init() catch |err| {
                log.err("ThreadPoolResolver.resolve: Async.init failed: {}", .{err});
                cb(userdata, &.{}, err);
                return;
            };

            completion.internal.task = .{ .callback = threadCallback };

            log.debug("ThreadPoolResolver.resolve: arming async.wait on loop", .{});
            // Wait on the loop
            completion.internal.xev_async.?.wait(@as(*LoopType, @ptrCast(@alignCast(loop))), &completion.internal.xev_completion, Res.Completion, completion, asyncCallback);

            log.debug("ThreadPoolResolver.resolve: scheduling task on thread pool", .{});
            // Schedule on thread pool
            self.pool.schedule(@import("xev").ThreadPool.Batch.from(&completion.internal.task));
            log.debug("ThreadPoolResolver.resolve: task scheduled", .{});
        }

        fn threadCallback(task: *@import("xev").ThreadPool.Task) void {
            log.debug("threadCallback: entered", .{});
            const internal: *Res.Internal = @fieldParentPtr("task", task);
            const completion: *Res.Completion = @fieldParentPtr("internal", internal);
            const self: *Self = @ptrCast(@alignCast(completion.internal.resolver_ptr.?));

            log.debug("threadCallback: resolving {s}:{d}", .{ completion.hostname, completion.port });
            const hostname_z = self.allocator.dupeZ(u8, completion.hostname) catch {
                completion.internal.err = error.OutOfMemory;
                if (completion.internal.xev_async) |*a| a.notify() catch {};
                return;
            };
            defer self.allocator.free(hostname_z);

            var port_buf: [6]u8 = undefined;
            const port_z = std.fmt.bufPrintZ(&port_buf, "{}", .{completion.port}) catch unreachable;

            var hints: std.c.addrinfo = std.mem.zeroInit(std.c.addrinfo, .{
                .family = std.c.AF.UNSPEC,
                .socktype = std.c.SOCK.STREAM,
            });
            var res: ?*std.c.addrinfo = null;
            const rc = std.c.getaddrinfo(hostname_z.ptr, port_z.ptr, &hints, &res);

            if (@intFromEnum(rc) != 0) {
                completion.internal.err = error.DnsResolutionFailed;
                if (completion.internal.xev_async) |*a| a.notify() catch {};
                return;
            }

            if (res) |r| {
                defer std.c.freeaddrinfo(r);

                var count: usize = 0;
                var cur: ?*std.c.addrinfo = r;
                while (cur) |info| : (cur = info.next) {
                    count += 1;
                }

                const addrs = self.allocator.alloc(Address, count) catch {
                    completion.internal.err = error.OutOfMemory;
                    if (completion.internal.xev_async) |*a| a.notify() catch {};
                    return;
                };

                cur = r;
                var i: usize = 0;
                while (cur) |info| : (cur = info.next) {
                    addrs[i] = Address.initPosix(info.addr.?);
                    i += 1;
                }
                completion.internal.results = addrs;
            }

            if (completion.internal.xev_async) |*a| a.notify() catch {};
        }

        fn asyncCallback(
            ud: ?*Res.Completion,
            l: *LoopType,
            c: *XevApi.Completion,
            r: XevApi.Async.WaitError!void,
        ) XevApi.CallbackAction {
            _ = l;
            _ = c;
            const completion = ud.?;

            if (completion.internal.xev_async) |*a| {
                a.deinit();
                completion.internal.xev_async = null;
            }

            if (r) |_| {} else |err| {
                completion.internal.callback(completion.internal.userdata, &.{}, err);
                return .disarm;
            }

            if (completion.internal.err) |err| {
                completion.internal.callback(completion.internal.userdata, &.{}, err);
            } else {
                completion.internal.callback(completion.internal.userdata, completion.internal.results, {});
            }

            return .disarm;
        }
    };
}

/// Middleware: Single-Flight Resolver
pub fn SingleFlightResolverGen(comptime XevApi: type) type {
    const Res = ResolverGen(XevApi);
    const LoopType = XevApi.Loop;
    const Address = @import("xev").shim_net.Address;

    return struct {
        const Self = @This();
        inner: Res,
        allocator: std.mem.Allocator,
        inflight: std.StringHashMap(*InFlight),
        mutex: std.Thread.Mutex = .{},

        const InFlight = struct {
            parent: *Self,
            hostname: []const u8,
            port: u16,
            results: []Address = &.{},
            err: ?anyerror = null,
            waiters: std.ArrayListUnmanaged(Waiter) = .{},
            inner_completion: Res.Completion = .{},

            const Waiter = struct {
                cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void,
                userdata: ?*anyopaque,
                completion: *Res.Completion,
            };

            fn init(parent: *Self, hostname: []const u8, port: u16) !*InFlight {
                const i = try parent.allocator.create(InFlight);
                i.* = .{
                    .parent = parent,
                    .hostname = try parent.allocator.dupe(u8, hostname),
                    .port = port,
                    .waiters = .{},
                };
                return i;
            }

            fn deinit(self: *InFlight) void {
                const allocator = self.parent.allocator;
                allocator.free(self.hostname);
                self.waiters.deinit(allocator);
                self.inner_completion.deinit(allocator);
                allocator.destroy(self);
            }
        };

        pub fn init(allocator: std.mem.Allocator, inner: Res) Self {
            return .{
                .inner = inner,
                .allocator = allocator,
                .inflight = std.StringHashMap(*InFlight).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.inflight.valueIterator();
            while (it.next()) |inflight| {
                inflight.*.deinit();
            }
            self.inflight.deinit();
        }

        pub fn resolver(self: *Self) Res {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve = resolve,
                },
            };
        }

        fn resolve(
            ptr: *anyopaque,
            loop: *LoopType,
            hostname: []const u8,
            port: u16,
            completion: *Res.Completion,
            cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void,
            userdata: ?*anyopaque,
        ) void {
            const self: *Self = @ptrCast(@alignCast(ptr));

            self.mutex.lock();
            if (self.inflight.get(hostname)) |inflight| {
                inflight.waiters.append(self.allocator, .{
                    .cb = cb,
                    .userdata = userdata,
                    .completion = completion,
                }) catch |err| {
                    self.mutex.unlock();
                    cb(userdata, &.{}, err);
                    return;
                };
                self.mutex.unlock();
                return;
            }

            const inflight = InFlight.init(self, hostname, port) catch |err| {
                self.mutex.unlock();
                cb(userdata, &.{}, err);
                return;
            };
            self.inflight.put(inflight.hostname, inflight) catch |err| {
                self.mutex.unlock();
                inflight.deinit();
                cb(userdata, &.{}, err);
                return;
            };

            inflight.waiters.append(self.allocator, .{
                .cb = cb,
                .userdata = userdata,
                .completion = completion,
            }) catch |err| {
                _ = self.inflight.remove(hostname);
                self.mutex.unlock();
                inflight.deinit();
                cb(userdata, &.{}, err);
                return;
            };
            self.mutex.unlock();

            self.inner.resolve(loop, inflight.hostname, port, &inflight.inner_completion, innerCallback, inflight);
        }

        fn innerCallback(ud: ?*anyopaque, results: []const Address, err: anyerror!void) void {
            const inflight: *InFlight = @ptrCast(@alignCast(ud));
            const self = inflight.parent;

            self.mutex.lock();
            _ = self.inflight.remove(inflight.hostname);
            self.mutex.unlock();

            for (inflight.waiters.items) |waiter| {
                err catch |e| {
                    waiter.cb(waiter.userdata, &.{}, e);
                    continue;
                };

                if (results.len > 0) {
                    const duped = self.allocator.alloc(Address, results.len) catch |e| {
                        waiter.cb(waiter.userdata, &.{}, e);
                        continue;
                    };
                    @memcpy(duped, results);
                    waiter.completion.internal.results = duped;
                    waiter.cb(waiter.userdata, duped, {});
                } else {
                    waiter.cb(waiter.userdata, &.{}, {});
                }
            }

            inflight.deinit();
        }
    };
}

/// Tier 2: Speculative Resolver (Racecar)
pub fn SpeculativeResolverGen(comptime XevApi: type) type {
    const Res = ResolverGen(XevApi);
    const LoopType = XevApi.Loop;
    const Address = @import("xev").shim_net.Address;

    return struct {
        const Self = @This();
        inner: Res,
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, inner: Res) Self {
            return .{
                .inner = inner,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            _ = self;
        }

        pub fn resolver(self: *Self) Res {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve = resolve,
                },
            };
        }

        const Race = struct {
            res: *Self,
            hostname: []const u8,
            port: u16,
            cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void,
            userdata: ?*anyopaque,
            completion: *Res.Completion,

            mutex: std.Thread.Mutex = .{},
            done: bool = false,
            pending: u8 = 2,

            comp_v4: Res.Completion = .{},
            comp_v6: Res.Completion = .{},

            fn init(res: *Self, hostname: []const u8, port: u16, completion: *Res.Completion, cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void, userdata: ?*anyopaque) !*Race {
                const r = try res.allocator.create(Race);
                r.* = .{
                    .res = res,
                    .hostname = hostname,
                    .port = port,
                    .cb = cb,
                    .userdata = userdata,
                    .completion = completion,
                };
                return r;
            }

            fn deinit(self: *Race) void {
                const allocator = self.res.allocator;
                self.comp_v4.deinit(allocator);
                self.comp_v6.deinit(allocator);
                allocator.destroy(self);
            }

            fn finish(self: *Race, results: []const Address, err: anyerror!void) void {
                self.mutex.lock();
                self.pending -= 1;
                const should_callback = !self.done;
                const last = self.pending == 0;

                err catch |e| {
                    if (last and should_callback) {
                        self.done = true;
                        self.mutex.unlock();
                        self.cb(self.userdata, &.{}, e);
                    } else {
                        self.mutex.unlock();
                    }
                    if (last) self.deinit();
                    return;
                };

                if (should_callback) {
                    self.done = true;
                    const duped = self.res.allocator.alloc(Address, results.len) catch |e| {
                        self.mutex.unlock();
                        self.cb(self.userdata, &.{}, e);
                        if (last) self.deinit();
                        return;
                    };
                    @memcpy(duped, results);
                    self.completion.internal.results = duped;
                    self.mutex.unlock();
                    self.cb(self.userdata, duped, {});
                } else {
                    self.mutex.unlock();
                }

                if (last) self.deinit();
            }
        };

        fn resolve(
            ptr: *anyopaque,
            loop: *LoopType,
            hostname: []const u8,
            port: u16,
            completion: *Res.Completion,
            cb: *const fn (ud: ?*anyopaque, results: []const Address, err: anyerror!void) void,
            userdata: ?*anyopaque,
        ) void {
            const self: *Self = @ptrCast(@alignCast(ptr));

            const race = Race.init(self, hostname, port, completion, cb, userdata) catch |err| {
                cb(userdata, &.{}, err);
                return;
            };

            self.inner.resolve(loop, hostname, port, &race.comp_v4, v4Callback, race);
            self.inner.resolve(loop, hostname, port, &race.comp_v6, v6Callback, race);
        }

        fn v4Callback(ud: ?*anyopaque, results: []const Address, err: anyerror!void) void {
            const race: *Race = @ptrCast(@alignCast(ud));
            race.finish(results, err);
        }

        fn v6Callback(ud: ?*anyopaque, results: []const Address, err: anyerror!void) void {
            const race: *Race = @ptrCast(@alignCast(ud));
            race.finish(results, err);
        }
    };
}

pub const Resolver = ResolverGen(@import("xev"));
pub const ThreadPoolResolver = ThreadPoolResolverGen(@import("xev"));
pub const SingleFlightResolver = SingleFlightResolverGen(@import("xev"));
pub const SpeculativeResolver = SpeculativeResolverGen(@import("xev"));
