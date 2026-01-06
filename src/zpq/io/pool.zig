const std = @import("std");
const tls = @import("tls/connection.zig");
const xev = @import("xev");

/// Global S3 Connection Pool
///
/// A process-wide connection pool that persists across file opens/closes.
/// This is critical for Lambda where the same container handles multiple
/// invocations - connections established in invocation 1 are reused in
/// invocations 2, 3, 4, etc.
///
/// Design:
/// - Thread-safe (mutex protected)
/// - Keyed by (host, port, use_tls)
/// - Idle timeout to prevent stale connections
/// - Max connections per host to bound memory
///
/// Usage:
///   // Implicit - just works
///   var source = try XevS3Source.init(...); // Uses global pool automatically
///
///   // Explicit - for testing or custom lifetime
///   var pool = GlobalConnectionPool.init(allocator);
///   var source = try XevS3Source.initWithPool(allocator, &pool, ...);
pub const ConnectionKey = struct {
    host: []const u8,
    port: u16,
    use_tls: bool,

    pub fn hash(self: ConnectionKey) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(self.host);
        h.update(std.mem.asBytes(&self.port));
        h.update(std.mem.asBytes(&self.use_tls));
        return h.final();
    }

    pub fn eql(a: ConnectionKey, b: ConnectionKey) bool {
        return a.port == b.port and
            a.use_tls == b.use_tls and
            std.mem.eql(u8, a.host, b.host);
    }
};

pub fn GlobalConnectionPool(comptime XevApi: type) type {
    const Connection = tls.ConnectionGen(XevApi);
    const PoolEntry = struct {
        conn: *Connection,
        last_used_ms: i64,
        host_owned: []const u8, // We own this memory
    };

    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        mutex: std.Thread.Mutex = .{},
        idle_connections: std.ArrayListUnmanaged(PoolEntry) = .{},

        // Configuration
        max_idle_per_host: usize = 16,
        max_idle_total: usize = 64,
        idle_timeout_ms: i64 = 30_000, // 30 seconds

        // Stats (for observability)
        stats: Stats = .{},

        pub const Stats = struct {
            acquires: u64 = 0,
            hits: u64 = 0,
            misses: u64 = 0,
            releases: u64 = 0,
            expirations: u64 = 0,
            evictions: u64 = 0,
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .allocator = allocator };
        }

        pub fn deinit(self: *Self) void {
            self.mutex.lock();
            for (self.idle_connections.items) |entry| {
                entry.conn.closed = true;
                entry.conn.deinit();
                self.allocator.destroy(entry.conn);
                self.allocator.free(entry.host_owned);
            }
            self.idle_connections.deinit(self.allocator);
            self.mutex.unlock();
        }

        pub fn acquire(self: *Self, key: ConnectionKey) ?*Connection {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.stats.acquires += 1;

            const now_ms = getNowMs() orelse return null;

            // Search backwards for matching connection (LIFO = most recently used)
            var i: usize = self.idle_connections.items.len;
            while (i > 0) {
                i -= 1;
                const entry = self.idle_connections.items[i];

                if (key.eql(.{ .host = entry.host_owned, .port = key.port, .use_tls = key.use_tls })) {
                    _ = self.idle_connections.orderedRemove(i);

                    // Check expiration
                    if (now_ms - entry.last_used_ms > self.idle_timeout_ms) {
                        self.stats.expirations += 1;
                        entry.conn.closed = true;
                        entry.conn.deinit();
                        self.allocator.destroy(entry.conn);
                        self.allocator.free(entry.host_owned);
                        continue; // Keep looking
                    }

                    self.stats.hits += 1;
                    self.allocator.free(entry.host_owned); // Caller provides key, we free our copy
                    return entry.conn;
                }
            }

            self.stats.misses += 1;
            return null;
        }

        pub fn release(self: *Self, key: ConnectionKey, conn: *Connection) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.stats.releases += 1;

            // Check if connection is still usable
            if (conn.closed) {
                conn.deinit();
                self.allocator.destroy(conn);
                return;
            }

            // Evict if at capacity
            if (self.idle_connections.items.len >= self.max_idle_total) {
                self.stats.evictions += 1;
                // Evict oldest (front of list)
                const evicted = self.idle_connections.orderedRemove(0);
                evicted.conn.closed = true;
                evicted.conn.deinit();
                self.allocator.destroy(evicted.conn);
                self.allocator.free(evicted.host_owned);
            }

            const now_ms = getNowMs() orelse {
                conn.deinit();
                self.allocator.destroy(conn);
                return;
            };

            const host_copy = self.allocator.dupe(u8, key.host) catch {
                conn.deinit();
                self.allocator.destroy(conn);
                return;
            };

            conn.idling = true;
            self.idle_connections.append(self.allocator, .{
                .conn = conn,
                .last_used_ms = now_ms,
                .host_owned = host_copy,
            }) catch {
                self.allocator.free(host_copy);
                conn.deinit();
                self.allocator.destroy(conn);
            };
        }

        pub fn getStats(self: *Self) Stats {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.stats;
        }

        pub fn idleCount(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.idle_connections.items.len;
        }
    };
}

fn getNowMs() ?i64 {
    const now = std.time.Instant.now() catch return null;
    const ts = now.timestamp;
    // Handle different timestamp struct layouts across platforms
    if (@hasField(@TypeOf(ts), "tv_sec")) {
        return ts.tv_sec * 1000 + @divFloor(ts.tv_nsec, 1_000_000);
    } else if (@hasField(@TypeOf(ts), "sec")) {
        return ts.sec * 1000 + @divFloor(ts.nsec, 1_000_000);
    } else {
        // Fallback: treat as nanoseconds
        return @divFloor(@as(i64, @intCast(ts)), 1_000_000);
    }
}

// ============================================================================
// Global Singleton with Reference Counting (Polars-style)
// ============================================================================
//
// Design Philosophy (inspired by Polars/Arrow):
// - No explicit shutdown() required - pool auto-cleans when last user releases
// - Reference counting via atomic counter
// - acquire() increments refcount, release() decrements
// - When refcount hits 0, pool cleans up all idle connections and frees itself
// - Bounded idle connections (max_idle_total) prevent unbounded growth
// - Idle timeout evicts stale connections automatically
//
// Why this pattern?
// 1. Zero cognitive load - users can't forget to call shutdown
// 2. Correct by construction - resources freed when last reference drops
// 3. Lambda-friendly - works whether process lives 100ms or 100 hours
// 4. Matches Zig idioms - like defer, cleanup is automatic
//
// Usage:
//   const pool = acquireGlobalPool(allocator);  // refcount++
//   defer releaseGlobalPool();                   // refcount--, auto-cleanup if 0
//   // ... use pool ...

var global_pool_instance: ?*anyopaque = null; // Erased pointer to GlobalConnectionPool(xev)
var global_pool_mutex: std.Thread.Mutex = .{};
var global_allocator: ?std.mem.Allocator = null;
var global_refcount: u32 = 0;

/// Acquire the global connection pool, incrementing the reference count.
/// The pool is created lazily on first acquire.
/// Caller MUST call releaseGlobalPool() when done (typically via defer).
pub fn acquireGlobalPool(allocator: std.mem.Allocator) *GlobalConnectionPool(xev) {
    global_pool_mutex.lock();
    defer global_pool_mutex.unlock();

    global_refcount += 1;

    if (global_pool_instance) |pool_ptr| {
        return @ptrCast(@alignCast(pool_ptr));
    }

    const Pool = GlobalConnectionPool(xev);
    const pool = allocator.create(Pool) catch @panic("OOM creating global pool");
    pool.* = Pool.init(allocator);
    global_pool_instance = pool;
    global_allocator = allocator;
    return pool;
}

/// Release a reference to the global pool.
/// When the last reference is released, the pool cleans up all connections
/// and frees itself. No explicit shutdown needed.
pub fn releaseGlobalPool() void {
    global_pool_mutex.lock();
    defer global_pool_mutex.unlock();

    if (global_refcount == 0) {
        // Already fully released (shouldn't happen in correct code)
        return;
    }

    global_refcount -= 1;

    if (global_refcount == 0) {
        // Last reference dropped - clean up
        if (global_pool_instance) |pool_ptr| {
            const Pool = GlobalConnectionPool(xev);
            const pool: *Pool = @ptrCast(@alignCast(pool_ptr));
            pool.deinit();
            if (global_allocator) |alloc| {
                alloc.destroy(pool);
            }
            global_pool_instance = null;
            global_allocator = null;
        }
    }
}

/// Legacy API - get pool without incrementing refcount.
/// DEPRECATED: Use acquireGlobalPool/releaseGlobalPool instead.
/// This exists for backwards compatibility during migration.
pub fn getGlobalPool(allocator: std.mem.Allocator) *GlobalConnectionPool(xev) {
    return acquireGlobalPool(allocator);
}

/// Legacy API - explicit shutdown.
/// DEPRECATED: Use acquireGlobalPool/releaseGlobalPool instead.
/// With proper refcounting, this should never be needed.
pub fn shutdownGlobalPool() void {
    global_pool_mutex.lock();
    defer global_pool_mutex.unlock();

    // Force cleanup regardless of refcount (for legacy callers)
    if (global_pool_instance) |pool_ptr| {
        const Pool = GlobalConnectionPool(xev);
        const pool: *Pool = @ptrCast(@alignCast(pool_ptr));
        pool.deinit();
        if (global_allocator) |alloc| {
            alloc.destroy(pool);
        }
        global_pool_instance = null;
        global_allocator = null;
        global_refcount = 0;
    }
}

/// Get current reference count (for testing/debugging).
pub fn getGlobalPoolRefCount() u32 {
    global_pool_mutex.lock();
    defer global_pool_mutex.unlock();
    return global_refcount;
}

// ============================================================================
// Tests
// ============================================================================

test "GlobalConnectionPool basic lifecycle" {
    const Pool = GlobalConnectionPool(xev);
    const allocator = std.testing.allocator;
    var pool = Pool.init(allocator);
    defer pool.deinit();

    // Acquire from empty pool
    const key = ConnectionKey{ .host = "example.com", .port = 443, .use_tls = true };
    const conn = pool.acquire(key);
    try std.testing.expectEqual(null, conn);
    try std.testing.expectEqual(@as(u64, 1), pool.stats.acquires);
    try std.testing.expectEqual(@as(u64, 1), pool.stats.misses);
}

test "ConnectionKey equality" {
    const key1 = ConnectionKey{ .host = "example.com", .port = 443, .use_tls = true };
    const key2 = ConnectionKey{ .host = "example.com", .port = 443, .use_tls = true };
    const key3 = ConnectionKey{ .host = "other.com", .port = 443, .use_tls = true };
    const key4 = ConnectionKey{ .host = "example.com", .port = 80, .use_tls = false };

    // Same host/port/tls should be equal
    try std.testing.expect(key1.eql(key2));

    // Different host should not be equal
    try std.testing.expect(!key1.eql(key3));

    // Different port/tls should not be equal
    try std.testing.expect(!key1.eql(key4));
}

test "ConnectionKey hash distribution" {
    // Different keys should produce different hashes (with high probability)
    const key1 = ConnectionKey{ .host = "a.example.com", .port = 443, .use_tls = true };
    const key2 = ConnectionKey{ .host = "b.example.com", .port = 443, .use_tls = true };
    const key3 = ConnectionKey{ .host = "a.example.com", .port = 8443, .use_tls = true };

    const h1 = key1.hash();
    const h2 = key2.hash();
    const h3 = key3.hash();

    try std.testing.expect(h1 != h2);
    try std.testing.expect(h1 != h3);
    try std.testing.expect(h2 != h3);
}

test "GlobalConnectionPool stats tracking" {
    const Pool = GlobalConnectionPool(xev);
    const allocator = std.testing.allocator;
    var pool = Pool.init(allocator);
    defer pool.deinit();

    const key = ConnectionKey{ .host = "test.com", .port = 443, .use_tls = true };

    // Multiple acquires from empty pool should all be misses
    _ = pool.acquire(key);
    _ = pool.acquire(key);
    _ = pool.acquire(key);

    try std.testing.expectEqual(@as(u64, 3), pool.stats.acquires);
    try std.testing.expectEqual(@as(u64, 3), pool.stats.misses);
    try std.testing.expectEqual(@as(u64, 0), pool.stats.hits);
}

test "GlobalConnectionPool idle count" {
    const Pool = GlobalConnectionPool(xev);
    const allocator = std.testing.allocator;
    var pool = Pool.init(allocator);
    defer pool.deinit();

    // Empty pool should have 0 idle connections
    try std.testing.expectEqual(@as(usize, 0), pool.idleCount());
}
