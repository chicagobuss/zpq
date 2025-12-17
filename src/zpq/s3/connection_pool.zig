const std = @import("std");
const TlsAdapter = @import("tls_adapter.zig").TlsAdapter;

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

pub const Connection = struct {
    fd: std.posix.fd_t,
    tls: ?*TlsAdapter = null,
};

pub const ConnectionPool = struct {
    allocator: std.mem.Allocator,
    // Simple list for now. In production, this might be a HashMap(Key, ArrayList(Connection))
    // but we need to manage the string lifecycle for keys.
    // For now, let's just use a simple ArrayList and linear scan since we likely only talk to one S3 endpoint.
    idle_connections: std.ArrayListUnmanaged(Entry) = .{},

    const Entry = struct {
        key: ConnectionKey, // We will own the host string memory
        conn: Connection,
    };

    pub fn init(allocator: std.mem.Allocator) ConnectionPool {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConnectionPool) void {
        for (self.idle_connections.items) |entry| {
            // Cleanup TLS if present
            if (entry.conn.tls) |tls| {
                tls.deinit();
            }
            
            // Close the socket
            // In unit tests with fake FDs, this might fail or close random handles.
            // We should ideally wrap this or allow disabling it for tests.
            // For now, we assume valid FDs if they are in the pool.
            std.posix.close(entry.conn.fd); 
            // Free the key string
            self.allocator.free(entry.key.host);
        }
        self.idle_connections.deinit(self.allocator);
    }

    /// Try to get an idle connection for the given key.
    /// Returns null if no idle connection is available.
    pub fn acquire(self: *ConnectionPool, key: ConnectionKey) ?Connection {
        // Iterate backwards to get the most recently used (LIFO) - usually better for keep-alive
        var i: usize = self.idle_connections.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.idle_connections.items[i];
            if (entry.key.eql(key)) {
                // Found match! Remove from pool and return.
                const conn = entry.conn;
                
                // Free the stored key string as we are handing ownership of the connection back
                // and removing the entry. The caller passed in a key for lookup, but that key
                // is likely temporary.
                self.allocator.free(entry.key.host);
                
                _ = self.idle_connections.orderedRemove(i);
                return conn;
            }
        }
        return null;
    }

    /// Return a connection to the pool.
    pub fn release(self: *ConnectionPool, key: ConnectionKey, conn: Connection) !void {
        // We need to dup the host string because we need to own it
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
