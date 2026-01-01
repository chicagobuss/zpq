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
pub const Connection = ConnectionGen(xev);

pub fn ConnectionGen(comptime XevApi: type) type {
    const LoopType = if (@hasDecl(XevApi, "Loop")) XevApi.Loop else XevApi;
    const TCP = XevApi.TCP;
    return struct {
        const Self = @This();

        loop: *LoopType,
        tcp: TCP,
        tls: boring.tls_client.TlsClient,
        allocator: std.mem.Allocator,

        // Completions
        c_connect: XevApi.Completion = .{},
        c_read: XevApi.Completion = .{},
        c_write: XevApi.Completion = .{},
        c_close: XevApi.Completion = .{},

        // Buffers
        read_buf: [1024 * 1024]u8 = undefined,

        // State
        connected: bool = false,
        handshake_complete: bool = false,
        closed: bool = false,
        idling: bool = false,
        pending_read: bool = false,
        pending_write: bool = false,

        // Zero-Copy support
        target_buffer: ?[]u8 = null,
        tcp_read_buf_size: usize = 4096,
        use_direct: bool = true,

        user_ctx: ?*anyopaque = null,
        on_connect: ?*const fn (ctx: ?*anyopaque) void = null,
        on_data: ?*const fn (ctx: ?*anyopaque, data: []const u8) void = null,
        on_error: ?*const fn (ctx: ?*anyopaque, err: anyerror) void = null,

        pub const Options = struct {
            verify_certificate: bool = false,
        };

        pub fn init(loop: *LoopType, allocator: std.mem.Allocator, host: []const u8) !Self {
            return initWithOptions(loop, allocator, host, .{});
        }

        pub fn initWithOptions(loop: *LoopType, allocator: std.mem.Allocator, host: []const u8, options: Options) !Self {
            const tls_client = try boring.tls_client.TlsClient.init(host, .{ .verify_certificate = options.verify_certificate });
            return Self{
                .loop = loop,
                .tcp = undefined,
                .tls = tls_client,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            self.tls.deinit();
            if (!self.closed) {
                std.posix.close(self.tcp.fd);
                self.closed = true;
            }
        }

        pub fn close(self: *Self) void {
            if (self.closed) return;
            self.closed = true;
            self.tcp.close(self.loop, &self.c_close, Self, self, internalOnClose);
        }

        pub fn connect(self: *Self, addr: xev.shim_net.Address) !void {
            self.tcp = try TCP.init(addr);

            const one: i32 = 1;

            if (@import("builtin").os.tag == .macos) {
                try std.posix.setsockopt(self.tcp.fd, std.posix.SOL.SOCKET, std.posix.SO.NOSIGPIPE, std.mem.asBytes(&one));
            }

            const size: i32 = 4 * 1024 * 1024;
            std.posix.setsockopt(self.tcp.fd, std.posix.SOL.SOCKET, std.posix.SO.RCVBUF, std.mem.asBytes(&size)) catch {};

            std.posix.setsockopt(self.tcp.fd, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one)) catch {};

            self.tcp.connect(self.loop, &self.c_connect, addr, Self, self, internalOnConnect);
        }

        pub fn write(self: *Self, data: []const u8) !void {
            if (self.closed) {
                log.debug("write: connection closed, ignoring", .{});
                return;
            }
            log.debug("write: encrypting {d} bytes of plaintext", .{data.len});
            const enc_data = try self.tls.processOutgoing(data);
            if (enc_data) |bytes| {
                if (self.pending_write) {
                    log.debug("write: write already in progress", .{});
                    return error.WriteInProgress;
                }
                const buf = try self.allocator.dupe(u8, bytes);
                self.pending_write = true;
                log.debug("write: scheduling TCP write {d} bytes (encrypted from {d} plaintext)", .{ buf.len, data.len });
                self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
            } else {
                log.debug("write: no encrypted data produced by TLS (input was {d} bytes)", .{data.len});
            }
        }

        fn pump(self: *Self) void {
            if (self.closed) return;

            // FULL-DUPLEX: Always try to schedule a read first.
            // TCP is full-duplex, so we can read and write simultaneously.
            // This is critical for large uploads where the server might send
            // an early response (e.g., error) while we're still sending data.
            if (!self.pending_read and !self.idling) {
                log.debug("pump: scheduling TCP read", .{});
                self.pending_read = true;
                self.tcp.read(self.loop, &self.c_read, .{ .slice = self.read_buf[0..self.tcp_read_buf_size] }, Self, self, internalOnTcpRead);
            }

            // IMPORTANT: Check pending_write BEFORE calling processOutgoing!
            // processOutgoing consumes data from the BIO - if we can't write it,
            // that data would be lost forever.
            if (self.pending_write) {
                log.debug("pump: write in progress, deferring drain", .{});
                return;
            }

            // Now handle outgoing TLS data
            const out_slice_res = self.tls.processOutgoing(null);
            if (out_slice_res) |out_slice_opt| {
                if (out_slice_opt) |data| {
                    const buf = self.allocator.dupe(u8, data) catch |err| {
                        log.debug("pump: alloc failed: {}", .{err});
                        if (self.on_error) |cb| cb(self.user_ctx, err);
                        return;
                    };
                    self.pending_write = true;
                    log.debug("pump: scheduling TCP write {d} bytes", .{buf.len});
                    self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, internalOnTcpWrite);
                }
            } else |err| {
                log.debug("pump: TLS processOutgoing failed: {}", .{err});
                if (self.on_error) |cb| cb(self.user_ctx, err);
            }
        }

        fn internalOnClose(
            self: ?*Self,
            loop: *LoopType,
            completion: *XevApi.Completion,
            watcher: TCP,
            result: XevApi.CloseError!void,
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
            loop: *LoopType,
            completion: *XevApi.Completion,
            watcher: TCP,
            result: XevApi.ConnectError!void,
        ) xev.CallbackAction {
            _ = loop;
            _ = completion;
            _ = watcher;
            const me = self.?;
            if (result) |_| {
                me.connected = true;
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
            loop: *LoopType,
            completion: *XevApi.Completion,
            watcher: TCP,
            buffer: XevApi.WriteBuffer,
            result: XevApi.WriteError!usize,
        ) xev.CallbackAction {
            _ = loop;
            _ = completion;
            _ = watcher;
            const me = self.?;
            me.pending_write = false;
            me.allocator.free(buffer.slice);
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
            loop: *LoopType,
            completion: *XevApi.Completion,
            watcher: TCP,
            buffer: XevApi.ReadBuffer,
            result: XevApi.ReadError!usize,
        ) xev.CallbackAction {
            _ = loop;
            _ = completion;
            _ = watcher;
            _ = buffer;
            const me = self.?;
            me.pending_read = false;
            if (result) |n| {
                log.info("wire: read {d} bytes (target {d}KB)", .{ n, me.tcp_read_buf_size / 1024 });
                if (n == 0) {
                    me.close();
                    if (me.on_error) |cb| cb(me.user_ctx, error.EOF);
                    return .disarm;
                }

                var data_to_feed: ?[]const u8 = me.read_buf[0..n];

                while (true) {
                    const start_proc = std.time.Instant.now() catch unreachable;
                    const dec_res = me.tls.processIncoming(data_to_feed orelse &.{}, if (me.use_direct) me.target_buffer else null) catch |err| {
                        if (me.on_error) |cb| cb(me.user_ctx, err);
                        return .disarm;
                    };
                    const end_proc = std.time.Instant.now() catch unreachable;
                    log.info("perf: processIncoming took {d}ns", .{end_proc.since(start_proc)});

                    data_to_feed = null;

                    if (!me.handshake_complete and me.tls.handshake_complete) {
                        me.handshake_complete = true;
                        if (me.on_connect) |cb| cb(me.user_ctx);
                    }

                    if (dec_res) |pt| {
                        if (me.use_direct and me.target_buffer != null) {
                            me.target_buffer = me.target_buffer.?[pt.len..];
                            if (me.target_buffer.?.len == 0) me.target_buffer = null;
                        }
                        const start_cb = std.time.Instant.now() catch unreachable;
                        if (me.on_data) |cb| cb(me.user_ctx, pt);
                        const end_cb = std.time.Instant.now() catch unreachable;
                        log.info("perf: on_data callback took {d}ns", .{end_cb.since(start_cb)});
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
}
