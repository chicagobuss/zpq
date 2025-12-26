const std = @import("std");
const xev = @import("xev");
const boring = @import("boring_tls");

const log = std.log.scoped(.connection);

/// A unified transport layer for S3 (Plain TCP or TLS).
/// Manages the libxev lifecycle and the TLS "pump" if necessary.
pub const Connection = struct {
    loop: *xev.Loop,
    tcp: xev.TCP,
    tls: ?boring.tls_client.TlsClient = null,
    allocator: std.mem.Allocator,

    // Completions
    c_connect: xev.Completion = .{},
    c_read: xev.Completion = .{},
    c_write: xev.Completion = .{},
    c_close: xev.Completion = .{},

    // Internal Buffers
    read_buf: [16 * 1024]u8 = undefined, // S3 packets can be large
    write_queue: std.ArrayListUnmanaged(u8),
    write_cursor: usize = 0,

    // State
    host: []const u8,
    use_tls: bool,
    connected: bool = false,
    handshake_complete: bool = false,
    closed: bool = false,
    write_in_flight: bool = false,
    read_in_flight: bool = false,

    // Callbacks
    user_ctx: ?*anyopaque = null,
    on_connect: ?*const fn (conn: *Self, ctx: ?*anyopaque) void = null,
    on_data: ?*const fn (conn: *Self, ctx: ?*anyopaque, data: []const u8) void = null,
    on_error: ?*const fn (conn: *Self, ctx: ?*anyopaque, err: anyerror) void = null,
    on_drain: ?*const fn (conn: *Self, ctx: ?*anyopaque) void = null,

    pub const HIGH_WATER_MARK = 64 * 1024;
    pub const LOW_WATER_MARK = 16 * 1024;

    const Self = @This();

    pub fn init(loop: *xev.Loop, allocator: std.mem.Allocator, host: []const u8, use_tls: bool) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const host_dupe = try allocator.dupe(u8, host);
        errdefer allocator.free(host_dupe);

        self.* = .{
            .loop = loop,
            .tcp = undefined,
            .tls = if (use_tls) try boring.tls_client.TlsClient.init(host, .{ .verify_certificate = false }) else null,
            .allocator = allocator,
            .write_queue = .{},
            .host = host_dupe,
            .use_tls = use_tls,
        };

        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.tls) |*t| t.deinit();
        self.write_queue.deinit(self.allocator);
        self.allocator.free(self.host);
        self.allocator.destroy(self);
    }

    pub fn connect(self: *Self, addr: xev.shim_net.Address) !void {
        self.tcp = try xev.TCP.init(addr);
        self.tcp.connect(self.loop, &self.c_connect, addr, Self, self, internalOnConnect);
    }

    pub fn close(self: *Self) void {
        if (self.closed) return;
        self.closed = true;
        self.tcp.close(self.loop, &self.c_close, Self, self, internalOnClose);
    }

    pub fn write(self: *Self, data: []const u8) !void {
        const current_len = self.write_queue.items.len - self.write_cursor;
        if (current_len > HIGH_WATER_MARK) return error.WouldBlock;

        if (self.use_tls) {
            var input: ?[]const u8 = data;
            while (true) {
                const enc_data = try self.tls.?.processOutgoing(input);
                input = null; // Only pass data on the first iteration
                if (enc_data) |bytes| {
                    if (bytes.len > 0) {
                        try self.write_queue.appendSlice(self.allocator, bytes);
                        continue; // Check if BoringSSL has more records to emit
                    }
                }
                break;
            }
        } else {
            try self.write_queue.appendSlice(self.allocator, data);
        }

        self.tryWrite();
    }

    fn tryWrite(self: *Self) void {
        if (self.write_in_flight) return;
        const current_len = self.write_queue.items.len - self.write_cursor;
        if (current_len == 0) {
            self.write_queue.clearRetainingCapacity();
            self.write_cursor = 0;
            return;
        }

        self.write_in_flight = true;
        const data = self.write_queue.items[self.write_cursor..];

        // We must dupe because xev expects the buffer to be valid until the callback.
        const buf = self.allocator.dupe(u8, data) catch |err| {
            self.write_in_flight = false;
            if (self.on_error) |cb| cb(self, self.user_ctx, err);
            return;
        };
        self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
    }

    fn pump(self: *Self) void {
        if (self.use_tls) {
            // Drain the write BIO completely
            while (true) {
                const out_slice_res = self.tls.?.processOutgoing(null) catch |err| {
                    if (self.on_error) |cb| cb(self, self.user_ctx, err);
                    return;
                };

                if (out_slice_res) |data| {
                    if (data.len > 0) {
                        self.write_queue.appendSlice(self.allocator, data) catch |err| {
                            if (self.on_error) |cb| cb(self, self.user_ctx, err);
                            return;
                        };
                        continue; // Check for more data in BIO
                    }
                }
                break;
            }
            self.tryWrite();
        }

        // If no output to send, we need to read from net
        if (!self.read_in_flight) {
            self.read_in_flight = true;
            self.tcp.read(self.loop, &self.c_read, .{ .slice = &self.read_buf }, Self, self, internalOnTcpRead);
        }
    }

    fn internalOnConnect(
        ctx: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        _ = loop;
        _ = c;
        _ = s;
        const self = ctx.?;
        if (r) |_| {
            self.connected = true;
            if (self.use_tls) {
                const out_slice_res = self.tls.?.startHandshake();
                if (out_slice_res) |out_slice_opt| {
                    if (out_slice_opt) |data| {
                        self.write_queue.appendSlice(self.allocator, data) catch unreachable;
                        self.tryWrite();
                    }
                } else |err| {
                    if (self.on_error) |cb| cb(self, self.user_ctx, err);
                    return .disarm;
                }
                self.pump();
            } else {
                self.handshake_complete = true;
                if (self.on_connect) |cb| cb(self, self.user_ctx);
                self.pump();
            }
        } else |err| {
            if (self.on_error) |cb| cb(self, self.user_ctx, err);
        }
        return .disarm;
    }

    fn internalOnTcpWrite(
        ctx: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        buf: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        _ = loop;
        _ = c;
        _ = s;
        const self = ctx.?;
        self.write_in_flight = false;
        self.allocator.free(buf.slice);

        if (r) |n| {
            self.write_cursor += n;
            const remaining = self.write_queue.items.len - self.write_cursor;
            if (remaining < LOW_WATER_MARK) {
                if (self.on_drain) |cb| cb(self, self.user_ctx);
            }
            self.tryWrite();
        } else |err| {
            if (self.on_error) |cb| cb(self, self.user_ctx, err);
        }
        return .disarm;
    }

    fn internalOnTcpRead(
        ctx: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        _ = loop;
        _ = c;
        _ = s;
        _ = buf;
        const self = ctx.?;
        self.read_in_flight = false;
        if (r) |n| {
            if (n == 0) {
                if (self.on_error) |cb| cb(self, self.user_ctx, error.EOF);
                return .disarm;
            }

            if (self.use_tls) {
                var dec_ptr = self.read_buf[0..n];
                while (dec_ptr.len > 0) {
                    const dec_res = self.tls.?.processIncoming(dec_ptr) catch |err| {
                        if (self.on_error) |cb| cb(self, self.user_ctx, err);
                        return .disarm;
                    };

                    // Check handshake completion even if no decrypted data was produced
                    if (!self.handshake_complete and self.tls.?.handshake_complete) {
                        self.handshake_complete = true;
                        if (self.on_connect) |cb| cb(self, self.user_ctx);
                    }

                    if (dec_res) |dec_opt| {
                        if (dec_opt.len > 0) {
                            if (self.on_data) |cb| cb(self, self.user_ctx, dec_opt);
                        }

                        // Check for more data in BIO
                        while (true) {
                            const remaining = self.tls.?.processIncoming(&[_]u8{}) catch break;
                            if (remaining) |pt| {
                                if (pt.len > 0) {
                                    if (self.on_data) |cb| cb(self, self.user_ctx, pt);
                                    continue;
                                }
                            }
                            break;
                        }
                    }
                    // Currently boring_tls consumes the whole slice into its BIO,
                    // so we break here. In the future if we support partial consumption,
                    // we'd update dec_ptr.
                    break;
                }
            } else {
                if (self.on_data) |cb| cb(self, self.user_ctx, self.read_buf[0..n]);
            }
            self.pump();
        } else |err| {
            if (self.on_error) |cb| cb(self, self.user_ctx, err);
        }
        return .disarm;
    }

    fn internalOnClose(
        ctx: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        _ = ctx;
        _ = loop;
        _ = c;
        _ = s;
        _ = r catch {};
        return .disarm;
    }
};
