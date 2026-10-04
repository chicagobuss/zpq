//! Bounded TLS connection pool for S3 HTTPS traffic.
//!
//! This is deliberately two primitives in one struct:
//!
//!   * an `Io.Queue` of N request permits, which is the real in-flight
//!     concurrency budget for HTTP/1.1; and
//!   * a small LRU cache of idle TLS connections keyed by `(host, port)`.
//!
//! The shape matches `std.http.Client.ConnectionPool` for reuse, but keeps
//! ZPQ's explicit backpressure. If a scan spawns 200 range tasks, only N
//! can hold sockets at once. If a warm Lambda reads one bucket and writes
//! another, both hosts can keep idle connections instead of tearing the
//! whole pool down.
//!
//! An idle connection is only reused while it is younger than
//! `IDLE_TIMEOUT_NS`: past that the server or the network path has
//! likely dropped it, so `acquire` closes it instead of sending a request
//! into it.

const std = @import("std");
const clock = @import("../clock.zig");
const nowMonoNs = clock.monoNs;
const Io = std.Io;
const tls = @import("tls.zig");
const retry = @import("retry.zig");

/// Idle connections older than this are closed rather than reused. Pooled S3 connections were all found dead after a
/// Lambda sandbox sat frozen for about 5 s, so stay under that; reopening costs one TLS handshake per connection, which
/// is far cheaper than a request sent into a dead socket. Measured on `clock.bootNs`, which keeps counting while the
/// sandbox is frozen.
pub const IDLE_TIMEOUT_NS: i64 = 4 * std.time.ns_per_s;

pub const Error = error{
    QueueClosed,
} || Io.Cancelable || std.mem.Allocator.Error || tls.Error;

pub const AcquireOptions = struct {
    /// Open a new connection even if an idle one matches.
    fresh: bool = false,
};

pub const Criteria = struct {
    host: []const u8,
    /// The host's addresses. New connections start at each in turn, so a pool spreads over all of them: S3's DNS
    /// answer names several front ends, and K connections to one of them share its limits.
    addrs: []const tls.Ipv4,
    port: u16,
    use_tls: bool = true,
};

pub fn Pool(comptime N: usize) type {
    return struct {
        const Self = @This();

        /// Permit count, readable from a `*Pool(N)` held as `anytype`. A ceiling for callers sizing worker loops —
        /// extra workers would only park in `acquire` — not a concurrency policy in itself.
        pub const capacity: usize = N;

        pub const Node = struct {
            conn: tls.Connection,
            host: []u8,
            port: u16,
            use_tls: bool,
            /// `clock.bootNs` when the connection was last released to the idle list.
            idle_since_ns: i64 = 0,
            pool_node: std.DoublyLinkedList.Node = .{},
        };

        pub const Handle = struct {
            conn: *tls.Connection,
            node: *Node,
            permit: usize,
            /// The connection came from the idle list rather than being opened for this request, so a failure before
            /// its response starts may only mean the server dropped it while idle (`retry.Failure.stale`).
            reused: bool,
        };

        pub const Stats = struct {
            acquires: u64 = 0,
            opens: u64 = 0,
            reuses: u64 = 0,
            discards: u64 = 0,
            /// Idle connections closed by `acquire` for being older than `idle_timeout_ns`.
            idle_evictions: u64 = 0,
            /// Requests retried at once on a new connection after a reused one turned out dead.
            stale_retries: u64 = 0,
            /// Requests retried after a backoff (server errors, throttling, failures on fresh connections).
            backoff_retries: u64 = 0,
            /// Oldest idle connection `acquire` looked at, reused or evicted.
            max_idle_ns: u64 = 0,
            acquire_wait_ns: u64 = 0,
            acquire_lock_ns: u64 = 0,
        };

        allocator: std.mem.Allocator,

        permits_buffer: [N]usize,
        permits: Io.Queue(usize),

        mutex: Io.Mutex = .init,
        used: std.DoublyLinkedList = .{},
        free: std.DoublyLinkedList = .{},
        free_len: usize = 0,
        free_size: usize = 32,
        idle_timeout_ns: i64 = IDLE_TIMEOUT_NS,
        /// Index into `Criteria.addrs` the next new connection starts at; guarded by `mutex`.
        next_addr: usize = 0,

        stats: Stats = .{},

        /// Pointer-init only: `permits` holds a slice into
        /// `permits_buffer`, so copying this struct after init would
        /// invalidate the queue.
        pub fn init(self: *Self, allocator: std.mem.Allocator) !void {
            self.* = .{
                .allocator = allocator,
                .permits_buffer = undefined,
                .permits = undefined,
                .mutex = .init,
                .used = .{},
                .free = .{},
                .free_len = 0,
                .free_size = 32,
                .idle_timeout_ns = IDLE_TIMEOUT_NS,
                .next_addr = 0,
                .stats = .{},
            };
            self.permits = Io.Queue(usize).init(self.permits_buffer[0..]);
            for (0..N) |i| try self.permits.putOne(undefined, i);
        }

        pub fn deinit(self: *Self) void {
            var node = self.free.first;
            while (node) |n| {
                const slot: *Node = @alignCast(@fieldParentPtr("pool_node", n));
                node = n.next;
                self.destroyNode(slot);
            }
            node = self.used.first;
            while (node) |n| {
                const slot: *Node = @alignCast(@fieldParentPtr("pool_node", n));
                node = n.next;
                self.destroyNode(slot);
            }
            self.free = .{};
            self.used = .{};
            self.free_len = 0;
        }

        pub fn acquire(self: *Self, io: Io, criteria: Criteria, opts: AcquireOptions) Error!Handle {
            const t_wait_start = nowMonoNs();
            const permit = try self.permits.getOne(io);
            errdefer self.permits.putOne(io, permit) catch {};
            return self.acquireWithPermit(io, permit, nowMonoNs() - t_wait_start, criteria, opts);
        }

        /// `acquire` for a caller holding `permit` from permits of its own (a `Lane`), after waiting `wait_ns` for it.
        pub fn acquireWithPermit(
            self: *Self,
            io: Io,
            permit: usize,
            wait_ns: i64,
            criteria: Criteria,
            opts: AcquireOptions,
        ) Error!Handle {
            const t_lock_start = nowMonoNs();
            const now = clock.bootNs();
            self.mutex.lockUncancelable(io);
            var expired: std.DoublyLinkedList = .{};
            var reused: ?*Node = null;
            var next = self.free.last;
            while (next) |n| {
                next = n.prev;
                const slot: *Node = @alignCast(@fieldParentPtr("pool_node", n));
                const idle_ns = now -| slot.idle_since_ns;
                self.stats.max_idle_ns = @max(self.stats.max_idle_ns, @as(u64, @intCast(@max(idle_ns, 0))));
                if (idle_ns > self.idle_timeout_ns) {
                    self.free.remove(n);
                    self.free_len -= 1;
                    expired.append(n);
                    self.stats.idle_evictions += 1;
                    continue;
                }
                if (opts.fresh or reused != null) continue;
                if (slot.port != criteria.port) continue;
                if (slot.use_tls != criteria.use_tls) continue;
                if (!std.ascii.eqlIgnoreCase(slot.host, criteria.host)) continue;

                self.free.remove(n);
                self.free_len -= 1;
                self.used.append(n);
                reused = slot;
            }
            self.stats.acquires += 1;
            self.stats.acquire_wait_ns +%= @intCast(wait_ns);
            self.stats.acquire_lock_ns +%= @intCast(nowMonoNs() - t_lock_start);
            if (reused != null) self.stats.reuses += 1;
            const first_addr = self.next_addr;
            if (reused == null) self.next_addr +%= 1;
            self.mutex.unlock(io);

            while (expired.popFirst()) |n| self.destroyNode(@alignCast(@fieldParentPtr("pool_node", n)));

            if (reused) |slot| {
                return .{ .conn = &slot.conn, .node = slot, .permit = permit, .reused = true };
            }

            const slot = try self.allocator.create(Node);
            errdefer self.allocator.destroy(slot);

            const host_owned = try self.allocator.dupe(u8, criteria.host);
            errdefer self.allocator.free(host_owned);

            const peer: tls.Peer = .{ .addrs = criteria.addrs, .first = first_addr, .port = criteria.port };
            const conn = if (criteria.use_tls)
                try tls.Connection.connect(self.allocator, peer, criteria.host)
            else
                try tls.Connection.connectPlain(self.allocator, peer);

            slot.* = .{
                .conn = conn,
                .host = host_owned,
                .port = criteria.port,
                .use_tls = criteria.use_tls,
                .pool_node = .{},
            };

            self.mutex.lockUncancelable(io);
            self.used.append(&slot.pool_node);
            self.stats.opens += 1;
            self.mutex.unlock(io);

            return .{ .conn = &slot.conn, .node = slot, .permit = permit, .reused = false };
        }

        pub fn release(self: *Self, io: Io, h: Handle) void {
            self.releaseConnection(io, h);
            self.permits.putOneUncancelable(io, h.permit) catch {};
        }

        /// `release` without returning the permit, which a `Lane` returns to its own permits.
        pub fn releaseConnection(self: *Self, io: Io, h: Handle) void {
            var evicted: ?*Node = null;

            h.node.idle_since_ns = clock.bootNs();
            self.mutex.lockUncancelable(io);
            self.used.remove(&h.node.pool_node);
            if (self.free_len >= self.free_size) {
                evicted = @alignCast(@fieldParentPtr("pool_node", self.free.popFirst().?));
                self.free_len -= 1;
                self.stats.discards += 1;
            }
            self.free.append(&h.node.pool_node);
            self.free_len += 1;
            self.mutex.unlock(io);

            if (evicted) |slot| self.destroyNode(slot);
        }

        pub fn discard(self: *Self, io: Io, h: Handle) void {
            self.discardConnection(io, h);
            self.permits.putOneUncancelable(io, h.permit) catch {};
        }

        /// `discard` without returning the permit, which a `Lane` returns to its own permits.
        pub fn discardConnection(self: *Self, io: Io, h: Handle) void {
            self.mutex.lockUncancelable(io);
            self.used.remove(&h.node.pool_node);
            self.stats.discards += 1;
            self.mutex.unlock(io);

            self.destroyNode(h.node);
        }

        /// Count a retry in `stats`; a `.fatal` failure is not one.
        pub fn noteRetry(self: *Self, io: Io, failure: retry.Failure) void {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            switch (failure) {
                .stale => self.stats.stale_retries += 1,
                .transient => self.stats.backoff_retries += 1,
                .fatal => {},
            }
        }

        pub fn snapshotStats(self: *Self) Stats {
            return self.stats;
        }

        pub fn resetStats(self: *Self) void {
            self.stats = .{};
        }

        fn destroyNode(self: *Self, slot: *Node) void {
            slot.conn.deinit();
            self.allocator.free(slot.host);
            self.allocator.destroy(slot);
        }
    };
}

/// A `*Pool(N)` with `N` erased, for callers that hold pools of any size without being generic over it.
pub const AnyPool = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Handle = struct {
        conn: *tls.Connection,
        node: *anyopaque,
        permit: usize,
        reused: bool,
    };

    const VTable = struct {
        acquire: *const fn (*anyopaque, Io, Criteria, AcquireOptions) Error!Handle,
        release: *const fn (*anyopaque, Io, Handle) void,
        discard: *const fn (*anyopaque, Io, Handle) void,
        noteRetry: *const fn (*anyopaque, Io, retry.Failure) void,
    };

    pub fn of(p: anytype) AnyPool {
        const P = @TypeOf(p.*);
        const gen = struct {
            fn typed(ptr: *anyopaque) *P {
                return @ptrCast(@alignCast(ptr));
            }
            fn unerase(h: Handle) P.Handle {
                return .{ .conn = h.conn, .node = @ptrCast(@alignCast(h.node)), .permit = h.permit, .reused = h.reused };
            }
            const vtable: VTable = .{
                .acquire = struct {
                    fn f(ptr: *anyopaque, io: Io, criteria: Criteria, opts: AcquireOptions) Error!Handle {
                        const h = try typed(ptr).acquire(io, criteria, opts);
                        return .{ .conn = h.conn, .node = h.node, .permit = h.permit, .reused = h.reused };
                    }
                }.f,
                .release = struct {
                    fn f(ptr: *anyopaque, io: Io, h: Handle) void {
                        typed(ptr).release(io, unerase(h));
                    }
                }.f,
                .discard = struct {
                    fn f(ptr: *anyopaque, io: Io, h: Handle) void {
                        typed(ptr).discard(io, unerase(h));
                    }
                }.f,
                .noteRetry = struct {
                    fn f(ptr: *anyopaque, io: Io, failure: retry.Failure) void {
                        typed(ptr).noteRetry(io, failure);
                    }
                }.f,
            };
        };
        return .{ .ptr = @ptrCast(p), .vtable = &gen.vtable };
    }

    pub fn acquire(self: AnyPool, io: Io, criteria: Criteria, opts: AcquireOptions) Error!Handle {
        return self.vtable.acquire(self.ptr, io, criteria, opts);
    }
    pub fn release(self: AnyPool, io: Io, h: Handle) void {
        self.vtable.release(self.ptr, io, h);
    }
    pub fn discard(self: AnyPool, io: Io, h: Handle) void {
        self.vtable.discard(self.ptr, io, h);
    }
    pub fn noteRetry(self: AnyPool, io: Io, failure: retry.Failure) void {
        self.vtable.noteRetry(self.ptr, io, failure);
    }
};

/// A share of pool `P`'s connections with `M` permits of its own: requests through it neither wait for the pool's
/// permits nor hold them, yet take and leave connections in the pool's idle cache. So uploads that follow reads reuse
/// the sockets the reads just warmed, and neither kind of request can queue behind the other.
pub fn Lane(comptime P: type, comptime M: usize) type {
    return struct {
        const Self = @This();

        pub const capacity: usize = M;
        pub const Handle = P.Handle;

        pool: *P,
        permits_buffer: [M]usize,
        permits: Io.Queue(usize),

        /// Pointer-init only, as `Pool.init`; `pool` must outlive the lane.
        pub fn init(self: *Self, pool: *P) void {
            self.* = .{ .pool = pool, .permits_buffer = undefined, .permits = undefined };
            self.permits = Io.Queue(usize).init(self.permits_buffer[0..]);
            for (0..M) |i| self.permits.putOneUncancelable(undefined, i) catch unreachable;
        }

        pub fn acquire(self: *Self, io: Io, criteria: Criteria, opts: AcquireOptions) Error!Handle {
            const t_wait_start = nowMonoNs();
            const permit = try self.permits.getOne(io);
            errdefer self.permits.putOneUncancelable(io, permit) catch {};
            return self.pool.acquireWithPermit(io, permit, nowMonoNs() - t_wait_start, criteria, opts);
        }

        pub fn release(self: *Self, io: Io, h: Handle) void {
            self.pool.releaseConnection(io, h);
            self.permits.putOneUncancelable(io, h.permit) catch {};
        }

        pub fn discard(self: *Self, io: Io, h: Handle) void {
            self.pool.discardConnection(io, h);
            self.permits.putOneUncancelable(io, h.permit) catch {};
        }

        pub fn noteRetry(self: *Self, io: Io, failure: retry.Failure) void {
            self.pool.noteRetry(io, failure);
        }
    };
}

const testing = std.testing;

/// A listening localhost socket: plain connections to it complete in the kernel backlog without an `accept`.
const TestListener = struct {
    fd: std.posix.fd_t,
    port: u16,

    fn open() !TestListener {
        return openOn(.{ 127, 0, 0, 1 }, 0);
    }

    /// Listen on `ip`:`port` (0: any free port). Skips the test where `ip` is not a local address (on macOS only
    /// 127.0.0.1 is).
    fn openOn(ip: tls.Ipv4, port: u16) !TestListener {
        const posix = std.posix;
        const rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
        const fd: posix.fd_t = @intCast(rc);
        errdefer _ = posix.system.close(fd);
        var addr: posix.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = @bitCast(ip) };
        if (posix.errno(posix.system.bind(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in))) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.listen(fd, 16)) != .SUCCESS) return error.SkipZigTest;
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        if (posix.errno(posix.system.getsockname(fd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;
        return .{ .fd = fd, .port = std.mem.bigToNative(u16, addr.port) };
    }

    fn close(self: TestListener) void {
        _ = std.posix.system.close(self.fd);
    }

    fn criteria(self: TestListener) Criteria {
        return .{ .host = "127.0.0.1", .addrs = &.{.{ 127, 0, 0, 1 }}, .port = self.port, .use_tls = false };
    }
};

test "acquire reuses a connection idle for less than the timeout and closes one idle for longer" {
    const io = testing.io;
    const listener = try TestListener.open();
    defer listener.close();

    var p: Pool(2) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();

    const first = try p.acquire(io, listener.criteria(), .{});
    try testing.expect(!first.reused);
    p.release(io, first);

    // Young: handed back out.
    const again = try p.acquire(io, listener.criteria(), .{});
    try testing.expect(again.reused);
    try testing.expectEqual(first.node, again.node);
    p.release(io, again);

    // Idle past the timeout (aged by hand rather than by sleeping): closed, and a new one opened in its place.
    again.node.idle_since_ns -= p.idle_timeout_ns + std.time.ns_per_ms;
    const after_idle = try p.acquire(io, listener.criteria(), .{});
    try testing.expect(!after_idle.reused);
    p.release(io, after_idle);

    const s = p.snapshotStats();
    try testing.expectEqual(@as(u64, 1), s.idle_evictions);
    try testing.expectEqual(@as(u64, 2), s.opens);
    try testing.expectEqual(@as(u64, 1), s.reuses);
    try testing.expect(s.max_idle_ns > @as(u64, @intCast(p.idle_timeout_ns)));
    try testing.expectEqual(@as(usize, 1), p.free_len);
}

test "acquire evicts expired connections of every host and opens fresh on request" {
    const io = testing.io;
    const a = try TestListener.open();
    defer a.close();
    const b = try TestListener.open();
    defer b.close();

    var p: Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();

    const ha = try p.acquire(io, a.criteria(), .{});
    const hb = try p.acquire(io, b.criteria(), .{});
    p.release(io, ha);
    p.release(io, hb);
    ha.node.idle_since_ns -= p.idle_timeout_ns + std.time.ns_per_ms;

    // Asking for host b also sweeps a's expired connection; `fresh` skips b's live idle one.
    const fresh_b = try p.acquire(io, b.criteria(), .{ .fresh = true });
    try testing.expect(!fresh_b.reused);
    try testing.expect(fresh_b.node != hb.node);
    p.release(io, fresh_b);
    try testing.expectEqual(@as(u64, 1), p.snapshotStats().idle_evictions);
    try testing.expectEqual(@as(usize, 2), p.free_len);

    // The erased view sees the same pool and counts retries by kind.
    const any = AnyPool.of(&p);
    const h = try any.acquire(io, b.criteria(), .{});
    try testing.expect(h.reused);
    any.discard(io, h);
    any.noteRetry(io, .stale);
    any.noteRetry(io, .transient);
    any.noteRetry(io, .fatal);
    const s = p.snapshotStats();
    try testing.expectEqual(@as(u64, 1), s.stale_retries);
    try testing.expectEqual(@as(u64, 1), s.backoff_retries);
    try testing.expectEqual(@as(u64, 1), s.discards);
}

/// The address `conn` is connected to.
fn peerOf(conn: *const tls.Connection) !tls.Ipv4 {
    var addr: std.posix.sockaddr.in = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getpeername(conn.fd, @ptrCast(&addr), &len)) != .SUCCESS)
        return error.TestUnexpectedResult;
    return @bitCast(addr.addr);
}

test "new connections take the host's addresses in turn, and skip one that refuses" {
    const io = testing.io;
    const a = try TestListener.open();
    defer a.close();
    const b = try TestListener.openOn(.{ 127, 0, 0, 2 }, a.port);
    defer b.close();
    const c = try TestListener.openOn(.{ 127, 0, 0, 3 }, a.port);
    defer c.close();
    const addrs = [_]tls.Ipv4{ .{ 127, 0, 0, 1 }, .{ 127, 0, 0, 2 }, .{ 127, 0, 0, 3 } };

    var p: Pool(8) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();

    // Six connections held at once: two on each address.
    var held: [6]Pool(8).Handle = undefined;
    var per_addr: [3]usize = @splat(0);
    for (&held) |*h| {
        h.* = try p.acquire(io, .{ .host = "s3.test", .addrs = &addrs, .port = a.port, .use_tls = false }, .{});
        const peer = try peerOf(h.conn);
        per_addr[peer[3] - 1] += 1;
    }
    for (held) |h| p.release(io, h);
    try testing.expectEqualSlices(usize, &.{ 2, 2, 2 }, &per_addr);

    // 127.0.0.4 has no listener on this port: a connection starting there moves on to the next address.
    const flaky = [_]tls.Ipv4{ .{ 127, 0, 0, 4 }, .{ 127, 0, 0, 1 } };
    for (0..2) |_| {
        const h = try p.acquire(io, .{ .host = "s3.test", .addrs = &flaky, .port = a.port, .use_tls = false }, .{
            .fresh = true,
        });
        defer p.release(io, h);
        try testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, &(try peerOf(h.conn)));
    }
}

test "Pool init fills permit queue with N permits" {
    var p: Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();
    try testing.expectEqual(@as(usize, 4), p.permits.capacity());
}

test "a lane neither waits for nor holds the pool's permits, and shares its idle connections" {
    const io = testing.io;
    const listener = try TestListener.open();
    defer listener.close();

    var p: Pool(2) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();
    var lane: Lane(Pool(2), 1) = undefined;
    lane.init(&p);

    // A read leaves a connection idle; the lane's request reuses it.
    const read = try p.acquire(io, listener.criteria(), .{});
    p.release(io, read);
    const up = try lane.acquire(io, listener.criteria(), .{});
    try testing.expect(up.reused);
    try testing.expectEqual(read.node, up.node);

    // While the lane's request runs, every pool permit is still free; with the pool's permits all taken, the lane's
    // request still gets through.
    var permits: [2]usize = undefined;
    try testing.expectEqual(@as(usize, 2), try p.permits.get(io, &permits, 0));
    lane.release(io, up);
    const again = try AnyPool.of(&lane).acquire(io, listener.criteria(), .{});
    try testing.expect(again.reused);
    AnyPool.of(&lane).discard(io, again);
    for (permits) |permit| try p.permits.putOne(io, permit);

    try testing.expectEqual(@as(usize, 1), try lane.permits.get(io, &permits, 0));
    const s = p.snapshotStats();
    try testing.expectEqual(@as(u64, 3), s.acquires);
    try testing.expectEqual(@as(u64, 1), s.opens);
    try testing.expectEqual(@as(u64, 1), s.discards);
}
