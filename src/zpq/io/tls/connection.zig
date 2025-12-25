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
    idling: bool = false,
    pending_read: bool = false,
    pending_write: bool = false,
    
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
        self.tls.deinit(); 
    }

    pub fn close(self: *Self) void {
         if (self.closed) return;
         self.closed = true;
         // We don't cancel pending reads/writes explicitly here, 
         // but internalOnClose will disarm its completion.
         self.tcp.close(self.loop, &self.c_close, Self, self, internalOnClose);
    }
    
    pub fn connect(self: *Self, addr: xev.shim_net.Address) !void {
         self.tcp = try xev.TCP.init(addr);

         // Disable SIGPIPE on this socket for macOS
         if (@import("builtin").os.tag == .macos) {
             const one: i32 = 1;
             try std.posix.setsockopt(self.tcp.fd, std.posix.SOL.SOCKET, std.posix.SO.NOSIGPIPE, std.mem.asBytes(&one));
         }

         self.tcp.connect(self.loop, &self.c_connect, addr, Self, self, internalOnConnect);
    }
    
    pub fn write(self: *Self, data: []const u8) !void {
        if (self.closed) {
            log.debug("write: connection closed, ignoring", .{});
            return;
        }
        // Encrypt and send
        const enc_data = try self.tls.processOutgoing(data);
        if (enc_data) |bytes| {
             if (self.pending_write) {
                 log.debug("write: write already in progress", .{});
                 return error.WriteInProgress;
             }
             const buf = try self.allocator.dupe(u8, bytes);
             self.pending_write = true;
             log.debug("write: scheduling TCP write {d} bytes", .{buf.len});
             self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
        } else {
            log.debug("write: no encrypted data produced by TLS", .{});
        }
    }

    // --- Internals (The Pump) ---

    fn pump(self: *Self) void {
        if (self.closed) return;

        // 1. Process Outgoing (TLS -> TCP)
        const out_slice_res = self.tls.processOutgoing(null);
        if (out_slice_res) |out_slice_opt| {
             if (out_slice_opt) |data| {
                if (self.pending_write) {
                    log.debug("pump: write in progress, buffering", .{});
                    return; // Wait for current write to finish
                }
                const buf = self.allocator.dupe(u8, data) catch |err| {
                    log.debug("pump: alloc failed: {}", .{err});
                    if (self.on_error) |cb| cb(self.user_ctx, err);
                    return;
                };
                self.pending_write = true;
                log.debug("pump: scheduling TCP write {d} bytes", .{buf.len});
                self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
                return;
             }
        } else |err| {
             log.debug("pump: TLS processOutgoing failed: {}", .{err});
             if (self.on_error) |cb| cb(self.user_ctx, err);
             return;
        }

        // 2. Need Input? (TCP -> TLS)
        if (!self.pending_read and !self.idling) {
            log.debug("pump: scheduling TCP read", .{});
            self.pending_read = true;
            self.tcp.read(self.loop, &self.c_read, .{ .slice = &self.read_buf }, Self, self, internalOnTcpRead);
        }
    }

    fn internalOnClose(
        self: ?*Self,
        loop: *xev.Loop,
        completion: *xev.Completion,
        watcher: xev.TCP,
        result: xev.CloseError!void,
    ) xev.CallbackAction {
        _ = loop;
        _ = completion;
        _ = watcher;
        _ = result catch {};
        if (self) |me| {
            me.closed = true;
        }
        return .disarm;
    }

    fn internalOnConnect(
        self: ?*Self,
        loop: *xev.Loop,
        completion: *xev.Completion,
        watcher: xev.TCP,
        result: xev.ConnectError!void,
    ) xev.CallbackAction {
        _ = loop;
        _ = completion;
        _ = watcher;
        const me = self.?;
        if (result) |_| {
            me.connected = true;
            // Start Handshake
             const out_slice_res = me.tls.startHandshake();
             if (out_slice_res) |out_slice_opt| {
                 if (out_slice_opt) |data| {
                    const buf = me.allocator.dupe(u8, data) catch unreachable;
                    me.pending_write = true;
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
        completion: *xev.Completion,
        watcher: xev.TCP,
        buffer: xev.WriteBuffer,
        result: xev.WriteError!usize,
    ) xev.CallbackAction {
        _ = loop;
        _ = completion;
        _ = watcher;
        const me = self.?;
        me.pending_write = false;
        me.allocator.free(buffer.slice); // Free the dupe
        if (result) |n| {
            log.debug("internalOnTcpWrite: wrote {d} bytes", .{n});
            me.pump();
        } else |err| {
             log.debug("internalOnTcpWrite: write failed: {}", .{err});
             if (me.on_error) |cb| cb(me.user_ctx, err);
        }
        return .disarm;
    }

    fn internalOnTcpRead(
        self: ?*Self,
        loop: *xev.Loop,
        completion: *xev.Completion,
        watcher: xev.TCP,
        buffer: xev.ReadBuffer,
        result: xev.ReadError!usize,
    ) xev.CallbackAction {
        _ = loop;
        _ = completion;
        _ = watcher;
        _ = buffer;
        const me = self.?;
        me.pending_read = false;
        if (result) |n| {
            log.debug("internalOnTcpRead: read {d} bytes", .{n});
            if (n == 0) {
                 // EOF
                 me.close();
                 if (me.on_error) |cb| cb(me.user_ctx, error.EOF);
                 return .disarm;
            }
            
            // 1. Feed the new encrypted data into the TLS state machine
            var data_to_feed: ?[]const u8 = me.read_buf[0..n];
            
            while (true) {
                const dec_res = me.tls.processIncoming(data_to_feed orelse &.{}) catch |err| {
                    if (me.on_error) |cb| cb(me.user_ctx, err);
                    return .disarm;
                };
                
                data_to_feed = null;

                if (!me.handshake_complete and me.tls.handshake_complete) {
                    me.handshake_complete = true;
                    if (me.on_connect) |cb| cb(me.user_ctx);
                }
                
                if (dec_res) |pt| {
                    if (me.on_data) |cb| cb(me.user_ctx, pt);
                } else {
                    break;
                }
            }
            me.pump();
        } else |err| {
             me.close();
             if (me.on_error) |cb| cb(me.user_ctx, err);
        }
        return .disarm;
    }
};

