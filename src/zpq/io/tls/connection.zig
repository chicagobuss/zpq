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

         // Disable SIGPIPE on this socket for macOS
         if (@import("builtin").os.tag == .macos) {
             const one: i32 = 1;
             try std.posix.setsockopt(self.tcp.fd, std.posix.SOL.SOCKET, std.posix.SO.NOSIGPIPE, std.mem.asBytes(&one));
         }

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
        if (self.closed) return;

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
        completion: *xev.Completion,
        watcher: xev.TCP,
        result: xev.CloseError!void,
    ) xev.CallbackAction {
        _ = loop;
        _ = completion;
        _ = watcher;
        _ = result catch {};
        if (self) |me| {
            log.debug("Connection closed.", .{});
            me.closed = true;
            me.loop.stop(); // Force loop to exit for this request
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
            std.debug.print("[tls] TCP Connected, starting handshake...\n", .{});
            me.connected = true;
            // Start Handshake
             const out_slice_res = me.tls.startHandshake();
             std.debug.print("[tls] Handshake started\n", .{});
             if (out_slice_res) |out_slice_opt| {
                 if (out_slice_opt) |data| {
                    std.debug.print("[tls] Handshake wants to write {d} bytes\n", .{data.len});
                    const buf = me.allocator.dupe(u8, data) catch unreachable;
                    std.debug.print("[tls] Calling tcp.write...\n", .{});
                    me.tcp.write(me.loop, &me.c_write, .{ .slice = buf }, Self, me, internalOnTcpWrite);
                    std.debug.print("[tls] tcp.write called\n", .{});
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
        me.allocator.free(buffer.slice); // Free the dupe
        if (result) |_| {
            me.pump();
        } else |err| {
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
        if (result) |n| {
            log.debug("TCP Read complete: {d} bytes", .{n});
            if (n == 0) {
                 // EOF
                 log.debug("TCP EOF. Closing...", .{});
                 me.close();
                 // Call user error callback with EOF error? Or just close?
                 if (me.on_error) |cb| cb(me.user_ctx, error.EOF);
                 return .disarm;
            }
            
            // 1. Feed the new encrypted data into the TLS state machine
            log.debug("TLS processIncoming: {d} bytes", .{n});
            var data_to_feed: ?[]const u8 = me.read_buf[0..n];
            
            while (true) {
                const dec_res = me.tls.processIncoming(data_to_feed orelse &.{}) catch |err| {
                    log.debug("TLS processIncoming error: {}", .{err});
                    if (me.on_error) |cb| cb(me.user_ctx, err);
                    return .disarm;
                };
                
                // After the first call, we don't have new data to feed, 
                // we just want to drain any remaining records from the BIO.
                data_to_feed = null;

                // Check if handshake just finished
                if (!me.handshake_complete and me.tls.handshake_complete) {
                    log.debug("Handshake complete!", .{});
                    me.handshake_complete = true;
                    if (me.on_connect) |cb| cb(me.user_ctx);
                }
                
                if (dec_res) |pt| {
                    log.debug("Decrypted {d} bytes", .{pt.len});
                    if (me.on_data) |cb| cb(me.user_ctx, pt);
                    // Continue looping to see if there are more records in the BIO
                } else {
                    // No more decrypted data available at this time
                    break;
                }
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

