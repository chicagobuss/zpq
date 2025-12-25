const std = @import("std");
const tls = @import("../tls/connection.zig");
const Connection = tls.Connection;

pub const ConnectionKey = struct {
    host: []const u8,
    port: u16,
    use_tls: bool,

    pub fn eql(self: ConnectionKey, other: ConnectionKey) bool {
        return self.port == other.port and
            self.use_tls == other.use_tls and
            std.mem.eql(u8, self.host, other.host);
    }
};

pub const ConnectionPool = struct {
    allocator: std.mem.Allocator,
    idle_connections: std.ArrayListUnmanaged(Entry) = .{},
    max_idle: usize = 32,

    const Entry = struct {
        key: ConnectionKey, // We will own the host string memory
        conn: *Connection,
        last_used_ms: i64,
    };

    pub fn init(allocator: std.mem.Allocator) ConnectionPool {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConnectionPool) void {
        for (self.idle_connections.items) |entry| {
            // Note: Connection.deinit() is synchronous but close() is async.
            // For a clean shutdown, the source should have closed these.
            // But if they are idle, we just destroy them.
            entry.conn.deinit();
            self.allocator.destroy(entry.conn);
            self.allocator.free(entry.key.host);
        }
        self.idle_connections.deinit(self.allocator);
    }

    /// Finds a warm connection from the pool. Returns null if none available or all are dead.
    pub fn acquire(self: *ConnectionPool, key: ConnectionKey) ?*Connection {
        const now = std.time.Instant.now() catch return null;
        const now_ms = if (@hasField(@TypeOf(now.timestamp), "sec")) 
            (now.timestamp.sec * 1000) + @divFloor(now.timestamp.nsec, 1_000_000)
        else if (@hasField(@TypeOf(now.timestamp), "tv_sec"))
            (now.timestamp.tv_sec * 1000) + @divFloor(now.timestamp.tv_nsec, 1_000_000)
        else 
            @divFloor(@as(i64, @intCast(now.timestamp)), 1_000_000);

        var i: usize = self.idle_connections.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.idle_connections.items[i];
            if (entry.key.eql(key)) {
                _ = self.idle_connections.orderedRemove(i);
                
                // 1. Check for expiration (e.g. 10 seconds)
                if (now_ms - entry.last_used_ms > 10000) {
                    entry.conn.deinit();
                    self.allocator.destroy(entry.conn);
                    self.allocator.free(entry.key.host);
                    continue;
                }

                self.allocator.free(entry.key.host);
                return entry.conn;
            }
        }
        return null;
    }

    /// Releases a connection back into the pool. If the pool is full, it's destroyed.
    pub fn release(self: *ConnectionPool, key: ConnectionKey, conn: *Connection) !void {
        if (self.idle_connections.items.len >= self.max_idle) {
            conn.deinit();
            self.allocator.destroy(conn);
            return;
        }

        const host_dupe = try self.allocator.dupe(u8, key.host);
        errdefer self.allocator.free(host_dupe);

        const now = std.time.Instant.now() catch {
            conn.deinit();
            self.allocator.destroy(conn);
            self.allocator.free(host_dupe);
            return;
        };
        const now_ms = if (@hasField(@TypeOf(now.timestamp), "sec")) 
            (now.timestamp.sec * 1000) + @divFloor(now.timestamp.nsec, 1_000_000)
        else if (@hasField(@TypeOf(now.timestamp), "tv_sec"))
            (now.timestamp.tv_sec * 1000) + @divFloor(now.timestamp.tv_nsec, 1_000_000)
        else 
            @divFloor(@as(i64, @intCast(now.timestamp)), 1_000_000);

        try self.idle_connections.append(self.allocator, .{
            .key = .{
                .host = host_dupe,
                .port = key.port,
                .use_tls = key.use_tls,
            },
            .conn = conn,
            .last_used_ms = now_ms,
        });
    }
};
