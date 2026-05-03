const std = @import("std");
const sink_interface = @import("sink.zig");
const channel = @import("sink/channel.zig");
const writer_mod = @import("s3/sink_writer.zig");
const protocol_s3 = @import("../protocol/s3.zig");
const transport = @import("transport.zig");
const xev = @import("xev");

const PART_SIZE = 16 * 1024 * 1024; // 16MB parts

pub fn AsyncS3SinkGen(comptime Xev: type) type {
    const Writer = writer_mod.AsyncS3SinkWriterGen(Xev);
    const ByteChannel = channel.SinkChannel([]const u8);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        chan: ByteChannel,
        thread_pool: *xev.ThreadPool,
        s3_config: protocol_s3.S3,
        bucket: []const u8,
        key: []const u8,
        
        // Final results
        err: ?anyerror = null,
        thread: std.Thread,
        notifier: Xev.Async,
        notifier_c: Xev.Completion,
        
        writer: *Writer = undefined,
        sink_loop: *Xev.Loop = undefined,

        pub fn init(allocator: std.mem.Allocator, loop: *Xev.Loop, thread_pool: *xev.ThreadPool, s3_config: protocol_s3.S3, bucket: []const u8, key: []const u8) !*Self {
            _ = loop;
            var self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            self.allocator = allocator;
            self.s3_config = s3_config;
            self.bucket = try allocator.dupe(u8, bucket);
            self.key = try allocator.dupe(u8, key);
            
            // Temporary writer to get things started, will re-init in run thread with correct loop
            self.writer = undefined;
            self.thread_pool = thread_pool;
            
            // Note: We use the provided loop, but we MUST ensure only one thread 
            // drives it. If the reader is also driving it, we need a separate loop.
            // Let's create a private loop for the Sink to be 100% safe from concurrency.
            self.sink_loop = try allocator.create(Xev.Loop);
            self.sink_loop.* = try Xev.Loop.init(.{});
            
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
            self.allocator.free(self.bucket);
            self.allocator.free(self.key);
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
            self.notifier.wait(self.sink_loop, &self.notifier_c, Self, self, struct {
                fn cb(ptr: ?*Self, _: *Xev.Loop, _: *Xev.Completion, res: Xev.Async.WaitError!void) Xev.CallbackAction {
                    _ = res catch {};
                    _ = ptr;
                    return .rearm;
                }
            }.cb);

            // 1. Resolve DNS & Init Writer
            const resolver = transport.Resolver.init(self.allocator, self.thread_pool);
            const writer = try self.allocator.create(Writer);
            writer.* = try Writer.init(self.allocator, self.sink_loop, resolver, self.s3_config, self.bucket, self.key);
            self.writer = writer;
            
            // Note: We need to store s3_config etc in Self to re-init here.
            // I'll add them to the struct.

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
                try self.sink_loop.run(.once);

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
                        ctx.deinit();
                        self.allocator.destroy(ctx);
                    } else {
                        i += 1;
                    }
                }

                // C. Try to fill buffer and launch new parts
                // The pool now handles concurrency limits, so we just dispatch as long as we have data.
                while (!channel_closed) {
                    if (try self.chan.tryRecv()) |chunk| {
                        activity = true;
                        defer self.allocator.free(chunk);
                        
                        var chunk_offset: usize = 0;
                        while (chunk_offset < chunk.len) {
                            const remaining_in_part = PART_SIZE - buffer.items.len;
                            const to_copy = @min(remaining_in_part, chunk.len - chunk_offset);
                            try buffer.appendSlice(self.allocator, chunk[chunk_offset .. chunk_offset + to_copy]);
                            chunk_offset += to_copy;

                            if (buffer.items.len == PART_SIZE) {
                                if (upload_id == null) {
                                    std.debug.print("[S3Sink] Initiating multipart upload\n", .{});
                                    upload_id = try self.writer.initiateMultipartUpload();
                                    std.debug.print("[S3Sink] Upload ID: {s}\n", .{upload_id.?});
                                }
                                
                                std.debug.print("[S3Sink] Launching Part {d} ({d} bytes)\n", .{ next_part_number, buffer.items.len });
                                const ctx = try self.writer.uploadPartAsync(upload_id.?, next_part_number, buffer.items);
                                try in_flight.append(self.allocator, ctx);
                                next_part_number += 1;
                                buffer.clearRetainingCapacity();
                            }
                        }
                    } else {
                        if (self.chan.closed) channel_closed = true;
                        break;
                    }
                }
                
                // D. Handle end of stream tail
                if (channel_closed and buffer.items.len > 0 and in_flight.items.len == 0) {
                    activity = true;
                    if (upload_id != null) {
                        const ctx = try self.writer.uploadPartAsync(upload_id.?, next_part_number, buffer.items);
                        try in_flight.append(self.allocator, ctx);
                        next_part_number += 1;
                        buffer.clearRetainingCapacity();
                    } else {
                        break; // Single object PUT handled at the end
                    }
                }
                
                if (channel_closed and buffer.items.len == 0 and in_flight.items.len == 0) break;

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
