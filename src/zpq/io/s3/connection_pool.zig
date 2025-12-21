const std = @import("std");
const connection_mod = @import("connection.zig");
const Connection = connection_mod.Connection;

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

    const Entry = struct {
        key: ConnectionKey, // We will own the host string memory
        conn: *Connection,
    };

    pub fn init(allocator: std.mem.Allocator) ConnectionPool {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConnectionPool) void {
        for (self.idle_connections.items) |entry| {
            entry.conn.close();
            entry.conn.deinit();
            self.allocator.free(entry.key.host);
        }
        self.idle_connections.deinit(self.allocator);
    }

    pub fn acquire(self: *ConnectionPool, key: ConnectionKey) ?*Connection {
        var i: usize = self.idle_connections.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.idle_connections.items[i];
            if (entry.key.eql(key)) {
                const conn = entry.conn;
                self.allocator.free(entry.key.host);
                _ = self.idle_connections.orderedRemove(i);
                return conn;
            }
        }
        return null;
    }

    pub fn release(self: *ConnectionPool, key: ConnectionKey, conn: *Connection) !void {
        const host_dupe = try self.allocator.dupe(u8, key.host);
        errdefer self.allocator.free(host_dupe);

        try self.idle_connections.append(self.allocator, .{
            .key = .{
                .host = host_dupe,
                .port = key.port,
                .use_tls = key.use_tls,
            },
            .conn = conn,
        });
    }
};
