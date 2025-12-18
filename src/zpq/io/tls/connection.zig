const std = @import("std");
const xev = @import("xev");
const boring = @import("boring_tls");

const log = std.log.scoped(.tls);

/// A wrapper around a TCP socket and a BoringTLS client.
/// This struct manages the "pumping" of data between the raw TCP socket
/// and the TLS state machine.
///
/// It does NOT implement `std.io.Reader/Writer` directly because it is
/// primarily callback/event-driven via `libxev`.
/// To use it with blocking interfaces, one would need to buffer the output.
pub const Connection = struct {
    loop: *xev.Loop,
    tcp: xev.TCP,
    tls: boring.tls_client.TlsClient,
    allocator: std.mem.Allocator,

    // Completions
    c_connect: xev.Completion = .{},
    c_read: xev.Completion = .{},
    c_write: xev.Completion = .{},
    c_close: xev.Completion = .{},

    // Buffers
    // TODO: Make this configurable or dynamic
    read_buf: [4096]u8 = undefined,

    // State
    connected: bool = false,
    handshake_complete: bool = false,
    closed: bool = false,
    
    // User callbacks
    // We use a simplified callback interface for now:
    // When data is available (decrypted), we call on_data.
    // When connection is ready (handshake done), we call on_connect.
    // When error occurs, we call on_error.
    // Context is erased to ?*anyopaque.
    user_ctx: ?*anyopaque = null,
    on_connect: ?*const fn (ctx: ?*anyopaque) void = null,
    on_data: ?*const fn (ctx: ?*anyopaque, data: []const u8) void = null,
    on_error: ?*const fn (ctx: ?*anyopaque, err: anyerror) void = null,

    const Self = @This();

    pub fn init(loop: *xev.Loop, allocator: std.mem.Allocator, host: []const u8) !Self {
        // Use verify_certificate = false for now (as per microtest success). 
        // TODO: Enable verification with embedded CA bundle.
        const tls_client = try boring.tls_client.TlsClient.init(host, .{ .verify_certificate = false });
        // tcp is init'd later or we can init it empty? 
        // xev.TCP.init requires an address. We'll init it in connect().
        // For now, return a partial struct or init with dummy addr? 
        // xev.TCP structure is just an fd holder.
        
        return Self{
            .loop = loop,
            .tcp = undefined, // Set in connect
            .tls = tls_client, // We own this pointer now? No, init returns pointer.
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (!self.closed) {
             // Ideally close() was called before.
        }
        self.tls.deinit(); 
    }

    pub fn close(self: *Self) void {
         if (self.closed) return;
         self.closed = true;
         self.tcp.close(self.loop, &self.c_close, Self, self, internalOnClose);
    }
    
    pub fn connect(self: *Self, addr: xev.shim_net.Address) !void {
         log.debug("Connection.connect called", .{});
         self.tcp = try xev.TCP.init(addr);
         self.tcp.connect(self.loop, &self.c_connect, addr, Self, self, internalOnConnect);
    }
    
    pub fn write(self: *Self, data: []const u8) !void {
        // Encrypt and send
        const enc_data = try self.tls.processOutgoing(data);
        if (enc_data) |bytes| {
             const buf = try self.allocator.dupe(u8, bytes);
             self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
        }
    }

    // --- Internals (The Pump) ---

    fn pump(self: *Self) void {
        // 1. Process Outgoing (TLS -> TCP)
        const out_slice_res = self.tls.processOutgoing(null);
        if (out_slice_res) |out_slice_opt| {
             if (out_slice_opt) |data| {
                log.debug("TLS wants to write {} bytes to TCP", .{data.len});
                const buf = self.allocator.dupe(u8, data) catch |err| {
                    if (self.on_error) |cb| cb(self.user_ctx, err);
                    return;
                };
                self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
                return;
             }
        } else |err| {
             log.debug("pump processOutgoing error: {}", .{err});
             if (self.on_error) |cb| cb(self.user_ctx, err);
             return;
        }

        // 2. Need Input? (TCP -> TLS)
        // log.debug("pump reading from TCP", .{});
        self.tcp.read(self.loop, &self.c_read, .{ .slice = &self.read_buf }, Self, self, internalOnTcpRead);
    }

    fn internalOnClose(
        self: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        _ = loop; _ = c; _ = s; 
        _ = self;
        _ = r catch {};
        log.debug("Connection closed.", .{});
        return .disarm;
    }

    fn internalOnConnect(
        self: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        _ = loop; _ = c; _ = s;
        const me = self.?;
        if (r) |_| {
            log.debug("TCP Connected", .{});
            me.connected = true;
            // Start Handshake
             const out_slice_res = me.tls.startHandshake();
             if (out_slice_res) |out_slice_opt| {
                 if (out_slice_opt) |data| {
                    const buf = me.allocator.dupe(u8, data) catch unreachable;
                    me.tcp.write(me.loop, &me.c_write, .{ .slice = buf }, Self, me, internalOnTcpWrite);
                    return .disarm;
                 }
             } else |err| {
                 if (me.on_error) |cb| cb(me.user_ctx, err);
                 return .disarm;
             }
             me.pump();
        } else |err| {
            if (me.on_error) |cb| cb(me.user_ctx, err);
        }
        return .disarm;
    }

    fn internalOnTcpWrite(
        self: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        buf: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        _ = loop; _ = c; _ = s;
        const me = self.?;
        me.allocator.free(buf.slice); // Free the dupe
        if (r) |_| {
            me.pump();
        } else |err| {
             if (me.on_error) |cb| cb(me.user_ctx, err);
        }
        return .disarm;
    }

    fn internalOnTcpRead(
        self: ?*Self,
        loop: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        _ = loop; _ = c; _ = s; _ = buf;
        const me = self.?;
        if (r) |n| {
            if (n == 0) {
                 // EOF
                 log.debug("TCP EOF. Closing...", .{});
                 me.close();
                 // Call user error callback with EOF error? Or just close?
                 if (me.on_error) |cb| cb(me.user_ctx, error.EOF);
                 return .disarm;
            }
            
            // Feed to TLS
            const dec_res = me.tls.processIncoming(me.read_buf[0..n]);
            if (dec_res) |dec_opt| {
                 // Check if handshake just finished
                 if (!me.handshake_complete and me.tls.handshake_complete) {
                     me.handshake_complete = true;
                     if (me.on_connect) |cb| cb(me.user_ctx);
                 }
                 
                 if (dec_opt) |pt| {
                     if (me.on_data) |cb| cb(me.user_ctx, pt);
                 }
            } else |err| {
                 if (me.on_error) |cb| cb(me.user_ctx, err);
                 return .disarm;
            }
            me.pump();
        } else |err| {
             log.debug("Read Error: {}", .{err});
             me.close();
             if (me.on_error) |cb| cb(me.user_ctx, err);
        }
        return .disarm;
    }
};

