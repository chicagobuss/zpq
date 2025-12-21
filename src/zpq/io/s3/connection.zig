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

    // State
    host: []const u8,
    use_tls: bool,
    connected: bool = false,
    handshake_complete: bool = false,
    closed: bool = false,

    // Callbacks
    user_ctx: ?*anyopaque = null,
    on_connect: ?*const fn (conn: *Self, ctx: ?*anyopaque) void = null,
    on_data: ?*const fn (conn: *Self, ctx: ?*anyopaque, data: []const u8) void = null,
    on_error: ?*const fn (conn: *Self, ctx: ?*anyopaque, err: anyerror) void = null,

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
            .host = host_dupe,
            .use_tls = use_tls,
        };

        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.tls) |*t| t.deinit();
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
        if (self.use_tls) {
            const enc_data = try self.tls.?.processOutgoing(data);
            if (enc_data) |bytes| {
                const buf = try self.allocator.dupe(u8, bytes);
                self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
            }
        } else {
            const buf = try self.allocator.dupe(u8, data);
            self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
        }
    }

    fn pump(self: *Self) void {
        if (self.use_tls) {
            const out_slice_res = self.tls.?.processOutgoing(null);
            if (out_slice_res) |out_slice_opt| {
                if (out_slice_opt) |data| {
                    const buf = self.allocator.dupe(u8, data) catch |err| {
                        if (self.on_error) |cb| cb(self, self.user_ctx, err);
                        return;
                    };
                    self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
                    return;
                }
            } else |err| {
                if (self.on_error) |cb| cb(self, self.user_ctx, err);
                return;
            }
        }

        // If no output to send, we need to read from net
        self.tcp.read(self.loop, &self.c_read, .{ .slice = &self.read_buf }, Self, self, internalOnTcpRead);
    }

    fn internalOnConnect(
        ctx: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        _ = loop; _ = c; _ = s;
        const self = ctx.?;
        if (r) |_| {
            self.connected = true;
            if (self.use_tls) {
                const out_slice_res = self.tls.?.startHandshake();
                if (out_slice_res) |out_slice_opt| {
                    if (out_slice_opt) |data| {
                        const buf = self.allocator.dupe(u8, data) catch unreachable;
                        self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
                        return .disarm;
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
        _ = loop; _ = c; _ = s;
        const self = ctx.?;
        self.allocator.free(buf.slice);
        if (r) |_| {
            self.pump();
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
        _ = loop; _ = c; _ = s; _ = buf;
        const self = ctx.?;
        if (r) |n| {
            if (n == 0) {
                if (self.on_error) |cb| cb(self, self.user_ctx, error.EOF);
                return .disarm;
            }

            if (self.use_tls) {
                const dec_res = self.tls.?.processIncoming(self.read_buf[0..n]);
                if (dec_res) |dec_opt| {
                    if (!self.handshake_complete and self.tls.?.handshake_complete) {
                        self.handshake_complete = true;
                        if (self.on_connect) |cb| cb(self, self.user_ctx);
                    }
                    if (dec_opt) |pt| {
                        if (self.on_data) |cb| cb(self, self.user_ctx, pt);
                    }
                } else |err| {
                    if (self.on_error) |cb| cb(self, self.user_ctx, err);
                    return .disarm;
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
        _ = ctx; _ = loop; _ = c; _ = s; _ = r catch {};
        return .disarm;
    }
};

