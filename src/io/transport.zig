const std = @import("std");
const xev_mod = @import("xev");
const tls_mod = @import("tls.zig");

pub const Address = xev_mod.shim_net.Address;

const log = std.log.scoped(.transport);

/// Simplified DNS resolver using libxev's ThreadPool for non-blocking resolution.
pub const Resolver = struct {
    allocator: std.mem.Allocator,
    pool: *xev_mod.ThreadPool,

    pub fn init(allocator: std.mem.Allocator, pool: *xev_mod.ThreadPool) Resolver {
        return .{
            .allocator = allocator,
            .pool = pool,
        };
    }

    pub fn resolve(self: Resolver, loop: anytype, hostname: []const u8, port: u16, context: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, addr: ?Address) void) !void {
        const LoopType = @TypeOf(loop);
        const is_dynamic = @hasDecl(xev_mod, "Dynamic") and LoopType == *xev_mod.Dynamic.Loop;
        const AsyncType = if (is_dynamic) xev_mod.Dynamic.Async else xev_mod.Async;
        const CompletionType = if (is_dynamic) xev_mod.Dynamic.Completion else xev_mod.Completion;

        const Task = struct {
            resolver: Resolver,
            hostname: []const u8,
            port: u16,
            ctx: ?*anyopaque,
            cb: *const fn (ctx: ?*anyopaque, addr: ?Address) void,
            result: ?Address = null,
            completion: CompletionType = .{},
            async_node: AsyncType,
            tp_task: xev_mod.ThreadPool.Task = undefined,

            fn run(t: *xev_mod.ThreadPool.Task) void {
                const task: *@This() = @fieldParentPtr("tp_task", t);
                const addr = resolveOne(task.resolver.allocator, task.hostname, task.port) catch null;
                task.result = addr;
                task.async_node.notify() catch {};
            }

            fn onDone(t: ?*@This(), l: LoopType, c: *CompletionType, res: anyerror!void) xev_mod.CallbackAction {
                _ = l;
                _ = c;
                _ = res catch {};
                const task = t.?;
                task.cb(task.ctx, task.result);
                task.async_node.deinit();
                task.resolver.allocator.destroy(task);
                return .disarm;
            }
        };

        const task = try self.allocator.create(Task);
        task.* = .{
            .resolver = self,
            .hostname = hostname,
            .port = port,
            .ctx = context,
            .cb = callback,
            .async_node = try AsyncType.init(),
            .tp_task = .{ .callback = Task.run },
        };

        if (@hasDecl(@typeInfo(LoopType).pointer.child, "wait")) {
            try loop.wait(&task.completion, task.async_node, Task, task, Task.onDone);
        } else {
            task.async_node.wait(loop, &task.completion, Task, task, Task.onDone);
        }

        self.pool.schedule(xev_mod.ThreadPool.Batch.from(&task.tp_task));
    }

    fn resolveOne(allocator: std.mem.Allocator, hostname: []const u8, port: u16) !Address {
        const port_tmp = try std.fmt.allocPrint(allocator, "{d}", .{port});
        defer allocator.free(port_tmp);
        const port_str = try allocator.dupeZ(u8, port_tmp);
        defer allocator.free(port_str);

        const hints = std.posix.addrinfo{
            .flags = .{},
            .family = std.posix.AF.UNSPEC,
            .socktype = std.posix.SOCK.STREAM,
            .protocol = std.posix.IPPROTO.TCP,
            .canonname = null,
            .addr = null,
            .addrlen = 0,
            .next = null,
        };

        var res: ?*std.posix.addrinfo = null;
        const hostname_z = try allocator.dupeZ(u8, hostname);
        defer allocator.free(hostname_z);

        const rc = std.c.getaddrinfo(hostname_z, port_str, &hints, &res);
        if (@intFromEnum(rc) != 0) return error.DnsFailure;
        defer std.c.freeaddrinfo(res.?);

        return Address.initPosix(res.?.addr.?);
    }
};

/// ConnectionGen(Xev) wraps a TCP socket and optionally TLS.
pub fn ConnectionGen(comptime Xev: type) type {
    const Loop = *Xev.Loop;
    const TCP = Xev.TCP;
    const Completion = Xev.Completion;

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        loop: Loop,
        tcp: TCP,
        tls: ?tls_mod.Client = null,

        // Completions
        c_connect: Completion = .{},
        c_read: Completion = .{},
        c_write: Completion = .{},
        c_close: Completion = .{},

        // State
        closed: bool = false,
        handshake_done: bool = false,
        stopped: bool = false, // Set by caller to stop reading (e.g., HTTP response complete)
        write_buffer: std.ArrayListUnmanaged(u8) = .{},
        current_write_buf: ?[]const u8 = null,
        current_write_offset: usize = 0,
        write_in_flight: bool = false,
        read_in_flight: bool = false,
        read_buf_ptr: ?[]u8 = null, // Track in-flight read buffer for cleanup

        // Callbacks
        callback_ctx: ?*anyopaque = null,
        on_data: ?*const fn (ctx: ?*anyopaque, data: []const u8) anyerror!void = null,
        on_handshake: ?*const fn (ctx: ?*anyopaque) void = null,
        on_error: ?*const fn (ctx: ?*anyopaque, err: anyerror) void = null,

        pub fn stop(self: *Self) void {
            self.stopped = true;
        }

        pub fn init(allocator: std.mem.Allocator, loop: Loop, use_tls: bool, host: []const u8) !*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .loop = loop,
                .tcp = undefined,
                .tls = if (use_tls) try tls_mod.Client.init(allocator, host, .{}) else null,
                .write_buffer = .{},
                .current_write_offset = 0,
            };
            return self;
        }

        pub fn deinit(self: *Self) void {
            // Free any in-flight read buffer to prevent leaks
            if (self.read_buf_ptr) |buf| {
                self.allocator.free(buf);
                self.read_buf_ptr = null;
            }
            if (self.tls) |*t| t.deinit();
            if (self.current_write_buf) |buf| {
                self.allocator.free(buf);
                self.current_write_buf = null;
            }
            // Ensure we close the socket FD to prevent leaks.
            // libxev wrappers do NOT close the FD on deinit.
            if (!self.closed) {
                // Dynamic wrappers use fd() method, static use .fd field
                const fd = if (@hasDecl(TCP, "fd")) self.tcp.fd() else self.tcp.fd;
                std.posix.close(fd);
                self.closed = true;
            }
            self.write_buffer.deinit(self.allocator);
            self.allocator.destroy(self);
        }

        pub fn connect(self: *Self, addr: Address) !void {
            self.tcp = try TCP.init(addr);
            if (comptime @hasDecl(Xev.Loop, "connect")) {
                try self.loop.connect(&self.c_connect, self.tcp, addr, Self, self, onConnect);
            } else {
                self.tcp.connect(self.loop, &self.c_connect, addr, Self, self, onConnect);
            }
        }

        pub fn write(self: *Self, data: []const u8) !void {
            if (self.closed) return error.ConnectionClosed;

            if (self.tls) |*t| {
                var total_consumed: usize = 0;
                while (total_consumed < data.len) {
                    const result = try t.encrypt(data[total_consumed..]);
                    if (result.encrypted) |bytes| {
                        try self.writeRaw(bytes);
                    }
                    if (result.consumed == 0) break;
                    total_consumed += result.consumed;
                }
            } else {
                try self.writeRaw(data);
            }
        }

        fn writeRaw(self: *Self, data: []const u8) !void {
            if (self.write_in_flight) {
                try self.write_buffer.appendSlice(self.allocator, data);
                return;
            }

            self.write_in_flight = true;
            // Always copy data to ensure validity during async operation
            const buf = try self.allocator.dupe(u8, data);
            self.current_write_buf = buf;
            
            if (comptime @hasDecl(Xev.Loop, "write")) {
                std.debug.print("[Transport] Queuing write ({d} bytes)\n", .{buf.len});
                try self.loop.write(&self.c_write, self.tcp, .{ .slice = buf }, Self, self, onWrite);
            } else {
                std.debug.print("[Transport] Queuing write ({d} bytes)\n", .{buf.len});
                self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Self, self, onWrite);
            }
        }

        fn onConnect(t: ?*Self, _: Loop, _: *Completion, _: TCP, res: Xev.ConnectError!void) xev_mod.CallbackAction {
            const self = t.?;
            res catch |err| {
                if (self.on_error) |cb| cb(self.callback_ctx, err);
                return .disarm;
            };

            if (self.tls != null) {
                self.doHandshake() catch |err| {
                    if (self.on_error) |cb| cb(self.callback_ctx, err);
                };
            } else {
                self.handshake_done = true;
                if (self.on_handshake) |cb| cb(self.callback_ctx);
                self.read();
            }
            return .disarm;
        }

        fn doHandshake(self: *Self) !void {
            const handshake = try self.tls.?.startHandshake();
            if (handshake.encrypted) |bytes| {
                self.write_in_flight = true;
                if (comptime @hasDecl(Xev.Loop, "write")) {
                    try self.loop.write(&self.c_write, self.tcp, .{ .slice = bytes }, Self, self, onHandshakeWrite);
                } else {
                    self.tcp.write(self.loop, &self.c_write, .{ .slice = bytes }, Self, self, onHandshakeWrite);
                }
            }
        }

        fn onHandshakeWrite(t: ?*Self, _: Loop, _: *Completion, _: TCP, _: Xev.WriteBuffer, res: Xev.WriteError!usize) xev_mod.CallbackAction {
            const self = t.?;
            _ = res catch |err| {
                if (self.on_error) |cb| cb(self.callback_ctx, err);
                return .disarm;
            };
            self.write_in_flight = false;
            self.doHandshakeLoop() catch |err| {
                if (self.on_error) |cb| cb(self.callback_ctx, err);
            };
            return .disarm;
        }

        fn doHandshakeLoop(self: *Self) !void {
            self.read();
        }

        fn onWrite(t: ?*Self, _: Loop, _: *Completion, _: TCP, _: Xev.WriteBuffer, res: Xev.WriteError!usize) xev_mod.CallbackAction {
            const self = t.?;
            const n = res catch |err| {
                std.debug.print("[Transport] onWrite error: {any}\n", .{err});
                if (self.on_error) |cb| cb(self.callback_ctx, err);
                return .disarm;
            };
            std.debug.print("[Transport] onWrite finished: {d} bytes\n", .{n});
            self.write_in_flight = false;
            
            if (self.current_write_buf) |buf| {
                const total_len = buf.len;
                // We must account for the fact that we might have already written some of this buffer
                // if we are in a partial write loop.
                // However, current implementation dupes a NEW buffer for each writeRaw call
                // and expects it to be cleared.
                // Let's assume writeRaw is "header of the line".
                
                // If we are here, we just finished a write of `n` bytes.
                // We need to know if that covered the whole buffer.
                // But wait, `buf` is the WHOLE buffer.
                // We need to track our offset into it.
                self.current_write_offset += n;
                
                if (self.current_write_offset < total_len) {
                    // Partial write! We must loop.
                    std.debug.print("[Transport] Partial write: {d}/{d} bytes. Re-queuing remainder.\n", .{self.current_write_offset, total_len});
                    const slice = buf[self.current_write_offset..];
                    if (comptime @hasDecl(Xev.Loop, "write")) {
                         // We must NOT call writeRaw because it dupes! We just call loop.write again with the slice.
                        self.loop.write(&self.c_write, self.tcp, .{ .slice = slice }, Self, self, onWrite) catch |err| {
                             if (self.on_error) |cb| cb(self.callback_ctx, err);
                        };
                    } else {
                        self.tcp.write(self.loop, &self.c_write, .{ .slice = slice }, Self, self, onWrite);
                    }
                    // Stay in flight
                    return .disarm;
                }

                // Full write complete
                self.allocator.free(buf);
                self.current_write_buf = null;
                self.current_write_offset = 0;
            }

            if (self.write_buffer.items.len > 0) {
                const data = self.write_buffer.toOwnedSlice(self.allocator) catch |err| {
                    if (self.on_error) |cb| cb(self.callback_ctx, err);
                    return .disarm;
                };
                // writeRaw will dupe this again, so we can free our copy immediately? 
                // No, we must free `data` AFTER writeRaw returns because writeRaw dupes it.
                // Yes, checking writeRaw impl above: it calls dupe(data). 
                // So `data` (owned slice) can be freed after writeRaw returns.
                self.writeRaw(data) catch |err| {
                    if (self.on_error) |cb| cb(self.callback_ctx, err);
                };
                self.allocator.free(data);
            } else if (self.handshake_done) {
                // After writing application data, schedule a read for the response
                // This is essential for HTTP keep-alive connection reuse
                self.read();
            }
            return .disarm;
        }

        pub fn read(self: *Self) void {
            if (self.stopped or self.closed) {
                return;
            }
            if (self.read_in_flight) {
                return;
            }
            self.read_in_flight = true;
            const buf = self.allocator.alloc(u8, 4096) catch return;
            self.read_buf_ptr = buf; // Track for cleanup on deinit
            if (comptime @hasDecl(Xev.Loop, "read")) {
                self.loop.read(&self.c_read, self.tcp, .{ .slice = buf }, Self, self, onRead) catch {};
            } else {
                self.tcp.read(self.loop, &self.c_read, .{ .slice = buf }, Self, self, onRead);
            }
        }

        fn onRead(t: ?*Self, _: Loop, _: *Completion, _: TCP, buf: Xev.ReadBuffer, res: Xev.ReadError!usize) xev_mod.CallbackAction {
            const self = t.?;
            self.read_in_flight = false;
            self.read_buf_ptr = null; // Buffer is now being handled, clear tracking
            const n = res catch |err| {
                if (self.on_error) |cb| cb(self.callback_ctx, err);
                self.allocator.free(buf.slice);
                return .disarm;
            };
            
            if (n == 0) {
                self.closed = true;
                self.allocator.free(buf.slice);
                return .disarm;
            }

            const data = buf.slice[0..n];
            if (self.tls) |*t_client| {
                if (!self.handshake_done) {
                    const decrypted = t_client.decrypt(data) catch |err| {
                        if (self.on_error) |cb| cb(self.callback_ctx, err);
                        self.allocator.free(buf.slice);
                        return .disarm;
                    };
                    if (t_client.isHandshakeComplete()) {
                        self.handshake_done = true;
                        if (self.on_handshake) |cb| cb(self.callback_ctx);
                    }
                    if (decrypted) |plain| {
                        if (self.on_data) |cb| cb(self.callback_ctx, plain) catch {};
                    }
                    // Continue handshake if needed
                    const out = t_client.encrypt(null) catch |err| {
                        if (self.on_error) |cb| cb(self.callback_ctx, err);
                        self.allocator.free(buf.slice);
                        return .disarm;
                    };
                    if (out.encrypted) |to_send| {
                        self.writeRaw(to_send) catch {};
                    }
                    // Schedule next read: either for more handshake data or for response
                    if (!self.stopped) self.read();
                } else {
                    // First decrypt with the new data
                    const first_decrypted = t_client.decrypt(data) catch |err| {
                        if (self.on_error) |cb| cb(self.callback_ctx, err);
                        self.allocator.free(buf.slice);
                        return .disarm;
                    };
                    if (first_decrypted) |plain| {
                        if (self.on_data) |cb| cb(self.callback_ctx, plain) catch {};
                    }
                    // Loop to drain any additional buffered TLS records
                    while (!self.stopped) {
                        const more = t_client.decrypt(&[_]u8{}) catch |err| {
                            // TlsConnectionClosed is expected when server sends close_notify
                            if (err == error.TlsConnectionClosed) {
                                self.closed = true;
                                break;
                            }
                            if (self.on_error) |cb| cb(self.callback_ctx, err);
                            self.allocator.free(buf.slice);
                            return .disarm;
                        };
                        if (more) |plain| {
                            if (self.on_data) |cb| cb(self.callback_ctx, plain) catch {};
                        } else {
                            break; // No more buffered data
                        }
                    }
                    if (!self.stopped and !self.closed) self.read();
                }
            } else {
                if (self.on_data) |cb| cb(self.callback_ctx, data) catch {};
                self.read();
            }

            self.allocator.free(buf.slice);
            return .disarm;
        }
    };
}
