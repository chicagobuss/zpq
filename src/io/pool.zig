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

const std = @import("std");
const Io = std.Io;
const tls = @import("tls.zig");

inline fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

pub const Error = error{
    QueueClosed,
} || Io.Cancelable || std.mem.Allocator.Error || tls.Error;

pub const Criteria = struct {
    host: []const u8,
    addr_v4: []const u8,
    port: u16,
    use_tls: bool = true,
};

pub fn Pool(comptime N: usize) type {
    return struct {
        const Self = @This();

        pub const Node = struct {
            conn: tls.Connection,
            host: []u8,
            addr_v4: []u8,
            port: u16,
            use_tls: bool,
            pool_node: std.DoublyLinkedList.Node = .{},
        };

        pub const Handle = struct {
            conn: *tls.Connection,
            node: *Node,
            permit: usize,
        };

        pub const Stats = struct {
            acquires: u64 = 0,
            opens: u64 = 0,
            reuses: u64 = 0,
            discards: u64 = 0,
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

        pub fn acquire(self: *Self, io: Io, criteria: Criteria) Error!Handle {
            const t_wait_start = nowMonoNs();
            const permit = try self.permits.getOne(io);
            const t_after_permit = nowMonoNs();

            errdefer self.permits.putOne(io, permit) catch {};

            const t_lock_start = nowMonoNs();
            self.mutex.lockUncancelable(io);
            var reused: ?*Node = null;
            var next = self.free.last;
            while (next) |n| : (next = n.prev) {
                const slot: *Node = @alignCast(@fieldParentPtr("pool_node", n));
                if (slot.port != criteria.port) continue;
                if (slot.use_tls != criteria.use_tls) continue;
                if (!std.ascii.eqlIgnoreCase(slot.host, criteria.host)) continue;

                self.free.remove(&slot.pool_node);
                self.free_len -= 1;
                self.used.append(&slot.pool_node);
                reused = slot;
                break;
            }
            self.stats.acquires += 1;
            self.stats.acquire_wait_ns +%= @intCast(t_after_permit - t_wait_start);
            self.stats.acquire_lock_ns +%= @intCast(nowMonoNs() - t_lock_start);
            if (reused != null) self.stats.reuses += 1;
            self.mutex.unlock(io);

            if (reused) |slot| {
                return .{ .conn = &slot.conn, .node = slot, .permit = permit };
            }

            const slot = try self.allocator.create(Node);
            errdefer self.allocator.destroy(slot);

            const host_owned = try self.allocator.dupe(u8, criteria.host);
            errdefer self.allocator.free(host_owned);
            const addr_owned = try self.allocator.dupe(u8, criteria.addr_v4);
            errdefer self.allocator.free(addr_owned);

            const conn = if (criteria.use_tls)
                try tls.Connection.connect(
                    self.allocator,
                    criteria.addr_v4,
                    criteria.port,
                    criteria.host,
                )
            else
                try tls.Connection.connectPlain(
                    self.allocator,
                    criteria.addr_v4,
                    criteria.port,
                );

            slot.* = .{
                .conn = conn,
                .host = host_owned,
                .addr_v4 = addr_owned,
                .port = criteria.port,
                .use_tls = criteria.use_tls,
                .pool_node = .{},
            };

            self.mutex.lockUncancelable(io);
            self.used.append(&slot.pool_node);
            self.stats.opens += 1;
            self.mutex.unlock(io);

            return .{ .conn = &slot.conn, .node = slot, .permit = permit };
        }

        pub fn release(self: *Self, io: Io, h: Handle) void {
            var evicted: ?*Node = null;

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
            self.permits.putOneUncancelable(io, h.permit) catch {};
        }

        pub fn discard(self: *Self, io: Io, h: Handle) void {
            self.mutex.lockUncancelable(io);
            self.used.remove(&h.node.pool_node);
            self.stats.discards += 1;
            self.mutex.unlock(io);

            self.destroyNode(h.node);
            self.permits.putOneUncancelable(io, h.permit) catch {};
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
            self.allocator.free(slot.addr_v4);
            self.allocator.destroy(slot);
        }
    };
}

const testing = std.testing;

test "Pool init fills permit queue with N permits" {
    var p: Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();
    try testing.expectEqual(@as(usize, 4), p.permits.capacity());
}
