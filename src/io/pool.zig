//! Bounded TLS connection pool for one S3 host.
//!
//! Same primitive serves both reader-side parallel range fetches and
//! writer-side parallel multipart parts. Pool size is the concurrency
//! budget — with HTTP/1.1, "connection" and "in-flight permit" are the
//! same resource, so one queue gates both.
//!
//! Backed by `std.Io.Queue` of slot indices. `acquire(io)` blocks
//! naturally when no slot is free; `release(io, handle)` returns it.
//! Connections are lazy-init — a slot's `tls.Connection` is created on
//! first acquire and reused thereafter. Stale connection retry is the
//! caller's responsibility (the existing `s3.Client` pattern).
//!
//! This is the read-side mirror of `s3.uploadMultipart`'s thread-local
//! connection model from Phase 5.3, generalized to be shared across
//! phases of one Lambda invocation. See journal 2026-05-04.

const std = @import("std");
const Io = std.Io;
const tls = @import("tls.zig");

pub const Error = error{
    UnknownConnection,
    QueueClosed,
} || Io.Cancelable || std.mem.Allocator.Error || tls.Error;

pub fn Pool(comptime N: usize) type {
    return struct {
        const Self = @This();

        pub const Handle = struct {
            conn: *tls.Connection,
            idx: usize,
        };

        const Slot = struct {
            conn: ?tls.Connection = null,
        };

        /// Connection construction parameters. Stored once; reused by
        /// every lazy-init.
        host: []const u8,
        addr_v4: []const u8,
        port: u16,
        allocator: std.mem.Allocator,

        slots: [N]Slot,
        free_buffer: [N]usize,
        free: Io.Queue(usize),

        /// Initialize an empty pool in-place. The pool struct is
        /// self-referential (the `free` queue holds a slice into the
        /// `free_buffer` field), so it MUST be constructed via this
        /// pointer-init form rather than returned by value — copying
        /// the struct would invalidate the slice. Connections aren't
        /// opened until the first `acquire`.
        pub fn init(
            self: *Self,
            allocator: std.mem.Allocator,
            host: []const u8,
            addr_v4: []const u8,
            port: u16,
        ) !void {
            self.* = .{
                .host = host,
                .addr_v4 = addr_v4,
                .port = port,
                .allocator = allocator,
                .slots = .{Slot{}} ** N,
                .free_buffer = undefined,
                .free = undefined,
            };
            self.free = Io.Queue(usize).init(self.free_buffer[0..]);
            for (0..N) |i| try self.free.putOne(undefined, i);
        }

        pub fn deinit(self: *Self) void {
            for (&self.slots) |*slot| {
                if (slot.conn) |*c| c.deinit();
                slot.conn = null;
            }
        }

        /// Acquire a connection from the pool. Blocks via `Io.Queue`
        /// when all N slots are in use. Lazily opens a connection on
        /// the first acquire of an unused slot.
        pub fn acquire(self: *Self, io: Io) Error!Handle {
            const idx = try self.free.getOne(io);
            errdefer self.release(io, .{ .conn = undefined, .idx = idx }) catch {};

            if (self.slots[idx].conn == null) {
                self.slots[idx].conn = try tls.Connection.connect(
                    self.allocator,
                    self.addr_v4,
                    self.port,
                    self.host,
                );
            }
            return .{ .conn = &self.slots[idx].conn.?, .idx = idx };
        }

        /// Return a handle to the pool. Call this after every acquire,
        /// regardless of whether the request succeeded.
        pub fn release(self: *Self, io: Io, h: Handle) Error!void {
            self.free.putOne(io, h.idx) catch return error.QueueClosed;
        }

        /// Drop the connection for a specific slot — call this when a
        /// request errored mid-flight and the connection state is
        /// uncertain. The slot is returned to the free list and will
        /// re-init on its next acquire.
        pub fn discard(self: *Self, io: Io, h: Handle) void {
            if (self.slots[h.idx].conn) |*c| c.deinit();
            self.slots[h.idx].conn = null;
            self.free.putOne(io, h.idx) catch {};
        }
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "Pool init/deinit fills queue with N indices" {
    var p: Pool(4) = undefined;
    try p.init(testing.allocator, "example.com", "127.0.0.1", 443);
    defer p.deinit();
    try testing.expectEqual(@as(usize, 4), p.free.capacity());
}
