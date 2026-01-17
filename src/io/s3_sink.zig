const std = @import("std");
const sink_interface = @import("sink.zig");
const channel = @import("sink/channel.zig");
const writer_mod = @import("s3/sink_writer.zig");
const protocol_s3 = @import("../protocol/s3.zig");
const transport = @import("transport.zig");
const xev = @import("xev");

const PART_SIZE = 8 * 1024 * 1024; // 8MB parts

pub fn AsyncS3SinkGen(comptime Xev: type) type {
    const Writer = writer_mod.AsyncS3SinkWriterGen(Xev);
    const ByteChannel = channel.SinkChannel([]const u8);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        writer: Writer,
        chan: ByteChannel,
        thread_pool: *xev.ThreadPool,
        
        // Final results
        err: ?anyerror = null,
        thread: std.Thread,
        notifier: Xev.Async,
        notifier_c: Xev.Completion,

        pub fn init(allocator: std.mem.Allocator, loop: *Xev.Loop, thread_pool: *xev.ThreadPool, s3_config: protocol_s3.S3, bucket: []const u8, key: []const u8) !*Self {
            var self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            self.allocator = allocator;
            self.writer = try Writer.init(allocator, loop, s3_config, bucket, key);
            
            // Note: We use the provided loop, but we MUST ensure only one thread 
            // drives it. If the reader is also driving it, we need a separate loop.
            // Let's create a private loop for the Sink to be 100% safe from concurrency.
            self.writer.loop = try allocator.create(Xev.Loop);
            self.writer.loop.* = try Xev.Loop.init(.{});
            
            self.chan = ByteChannel.init(allocator, 32);
            self.thread_pool = thread_pool;
            self.err = null;
            
            self.notifier = try Xev.Async.init();
            self.notifier_c = .{};

            // Start the consumer thread
            self.thread = try std.Thread.spawn(.{}, run, .{self});

            return self;
        }

        pub fn sink(self: *Self) sink_interface.Sink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .write = write,
                    .close = close,
                },
            };
        }

        fn write(ptr: *anyopaque, data: []const u8) anyerror!usize {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.err) |e| return e;

            // std.debug.print("[S3Sink] Receiving {d} bytes\n", .{data.len});
            // Copy data into channel (Sink takes ownership)
            const copy = try self.allocator.dupe(u8, data);
            try self.chan.send(copy);
            
            // Wake up the consumer loop
            self.notifier.notify() catch {};
            
            return data.len;
        }

        fn close(ptr: *anyopaque) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.chan.close();
            self.notifier.notify() catch {};
            self.thread.join();
            
            // Drain remaining chunks in channel
            while (try self.chan.recv()) |chunk| {
                self.allocator.free(chunk);
            }
            
            self.writer.loop.deinit();
            self.allocator.destroy(self.writer.loop);
            
            self.notifier.deinit();
            self.writer.deinit();
            self.chan.deinit();
            const err = self.err;
            self.allocator.destroy(self);
            
            if (err) |e| return e;
        }

        fn run(self: *Self) void {
            self.runInternal() catch |e| {
                self.err = e;
            };
        }

        fn runInternal(self: *Self) !void {
            // 0. Register notifier in loop to wake us on channel data
            self.notifier.wait(self.writer.loop, &self.notifier_c, Self, self, struct {
                fn cb(ptr: ?*Self, _: *Xev.Loop, _: *Xev.Completion, res: Xev.Async.WaitError!void) Xev.CallbackAction {
                    _ = res catch {};
                    _ = ptr;
                    return .rearm;
                }
            }.cb);

            // 1. Resolve DNS
            const resolver = transport.Resolver.init(self.allocator, self.thread_pool);
            
            const ResolveResult = struct {
                addr: ?transport.Address = null,
                done: bool = false,
            };
            var res = ResolveResult{};
            
            try resolver.resolve(self.writer.loop, self.writer.host, 443, &res, struct {
                fn cb(ctx: ?*anyopaque, addr: ?transport.Address) void {
                    const r: *ResolveResult = @ptrCast(@alignCast(ctx));
                    r.addr = addr;
                    r.done = true;
                }
            }.cb);

            while (!res.done) {
                try self.writer.loop.run(.once);
            }

            const addr = res.addr orelse return error.DnsExpansionFailed;
            self.writer.setAddress(addr);

            // 2. Parallel Upload Logic
            var buffer = std.ArrayListUnmanaged(u8){};
            defer buffer.deinit(self.allocator);

            var upload_id: ?[]const u8 = null;
            var completed_parts = std.ArrayListUnmanaged(protocol_s3.S3.Part){};
            defer {
                for (completed_parts.items) |p| self.allocator.free(p.etag);
                completed_parts.deinit(self.allocator);
                if (upload_id) |id| self.allocator.free(id);
            }

            // In-flight request management (Mirroring Go semaphore/waitgroup)
            const CONCURRENCY = 8;
            var in_flight = std.ArrayListUnmanaged(*Writer.RequestContext){};
            defer {
                for (in_flight.items) |ctx| {
                    ctx.deinit();
                    self.allocator.destroy(ctx);
                }
                in_flight.deinit(self.allocator);
            }

            var next_part_number: u32 = 1;
            var channel_closed = false;

            while (!channel_closed or in_flight.items.len > 0 or buffer.items.len > 0) {
                var activity = false;
                
                // A. Progress the loop
                try self.writer.loop.run(.once);

                // B. Reap finished requests
                var i: usize = 0;
                while (i < in_flight.items.len) {
                    const ctx = in_flight.items[i];
                    if (ctx.done) {
                        activity = true;
                        if (ctx.err) |err| return err;
                        if (ctx.status_code != 200) return error.S3UploadPartFailed;
                        
                        const etag = try self.allocator.dupe(u8, ctx.etag orelse return error.MissingETag);
                        try completed_parts.append(self.allocator, .{ .part_number = ctx.part_number, .etag = etag });
                        
                        _ = in_flight.orderedRemove(i);
                        // std.debug.print("[S3Sink] Part {d} finished. In-flight: {d}\n", .{ctx.part_number, in_flight.items.len});
                        ctx.deinit();
                        self.allocator.destroy(ctx);
                    } else if (ctx.conn) |c| {
                        if (c.closed and !ctx.done) {
                             return error.ConnectionClosedUnexpectedly;
                        }
                        i += 1;
                    } else {
                        i += 1;
                    }
                }

                // C. Try to fill buffer and launch new parts if room
                if (!channel_closed and in_flight.items.len < CONCURRENCY) {
                    // 1. Process existing buffer
                    while (buffer.items.len >= PART_SIZE and in_flight.items.len < CONCURRENCY) {
                        activity = true;
                        if (upload_id == null) {
                            upload_id = try self.writer.initiateMultipartUpload();
                        }
                        
                        const ctx = try self.writer.uploadPartAsync(upload_id.?, next_part_number, buffer.items[0..PART_SIZE]);
                        try in_flight.append(self.allocator, ctx);
                        
                        next_part_number += 1;
                        
                        const remaining = buffer.items.len - PART_SIZE;
                        std.mem.copyForwards(u8, buffer.items[0..remaining], buffer.items[PART_SIZE..]);
                        buffer.items.len = remaining;
                    }

                    // 2. Try to receive more data from channel
                    while (in_flight.items.len < CONCURRENCY) {
                        if (try self.chan.tryRecv()) |chunk| {
                            activity = true;
                            defer self.allocator.free(chunk);
                            try buffer.appendSlice(self.allocator, chunk);
                            
                            // Check if we can launch parts now
                             while (buffer.items.len >= PART_SIZE and in_flight.items.len < CONCURRENCY) {
                                if (upload_id == null) {
                                    upload_id = try self.writer.initiateMultipartUpload();
                                }
                                
                                const ctx = try self.writer.uploadPartAsync(upload_id.?, next_part_number, buffer.items[0..PART_SIZE]);
                                try in_flight.append(self.allocator, ctx);
                                
                                next_part_number += 1;
                                
                                const remaining = buffer.items.len - PART_SIZE;
                                std.mem.copyForwards(u8, buffer.items[0..remaining], buffer.items[PART_SIZE..]);
                                buffer.items.len = remaining;
                            }
                        } else {
                            if (self.chan.closed) channel_closed = true;
                            break;
                        }
                    }
                }
                
                // D. Handle end of stream
                if (channel_closed and buffer.items.len > 0 and in_flight.items.len < CONCURRENCY) {
                    activity = true;
                    if (upload_id != null) {
                        const ctx = try self.writer.uploadPartAsync(upload_id.?, next_part_number, buffer.items);
                        try in_flight.append(self.allocator, ctx);
                        next_part_number += 1;
                        buffer.clearRetainingCapacity();
                    } else {
                        // Very small file (<8MB)? We wait for everything else to clear then do a single PUT
                        if (in_flight.items.len == 0) break;
                    }
                }
                
                if (channel_closed and buffer.items.len == 0 and in_flight.items.len == 0) break;

                // E. Yield only if idle to prevent 100% CPU spin on 1-vCPU systems
                if (!activity) {
                     std.posix.nanosleep(0, 100_000);
                }
            }

            // 3. Finalize
            if (upload_id == null) {
                if (buffer.items.len > 0) {
                     try self.writer.putObject(buffer.items);
                }
            } else {
                const PartSort = struct {
                    fn lessThan(_: void, a: protocol_s3.S3.Part, b: protocol_s3.S3.Part) bool {
                        return a.part_number < b.part_number;
                    }
                };
                std.mem.sort(protocol_s3.S3.Part, completed_parts.items, {}, PartSort.lessThan);
                try self.writer.completeMultipartUpload(upload_id.?, completed_parts.items);
            }
        }
    };
}
