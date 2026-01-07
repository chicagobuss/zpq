const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const io = @import("../interface.zig");
const scheduler = @import("../s3/scheduler.zig");
const ResponseParser = @import("../http/response_parser.zig").ResponseParser;
const GlobalConnectionPool = @import("../pool.zig").GlobalConnectionPool;
const ConnectionKey = @import("../pool.zig").ConnectionKey;

const log = std.log.scoped(.cloud_orchestrator);

pub fn Orchestrator(comptime XevApi: type) type {
    const LoopType = XevApi.Loop;
    return struct {
        const Self = @This();
        provider: CloudProvider,
        loop: *LoopType,
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, loop: *LoopType, provider: CloudProvider) Self {
            return .{
                .allocator = allocator,
                .loop = loop,
                .provider = provider,
            };
        }

        pub fn readRanges(self: *Self, ranges: []const io.Range, buffers: []const []u8) !void {
            if (ranges.len == 0) return;

            // std.debug.print("[TRACE] orchestrator.readRanges: {d} ranges\n", .{ranges.len});
            // var total_timer = try std.time.Timer.start();
            // Use ranges if needed, or remove print usage. Wait, ranges IS used.
            // Actually just remove the prints.

            const addr = try self.provider.resolve();
            // std.debug.print("[TRACE]   DNS resolved in {d}ms\n", .{total_timer.read() / 1_000_000});

            // 1. Merge ranges using scheduler
            var merged_list = try scheduler.mergeRanges(self.allocator, ranges);
            defer {
                for (merged_list.items) |*m| m.original_indices.deinit(self.allocator);
                merged_list.deinit(self.allocator);
            }

            // 2. Partition execution:
            // Instead of 1 connection per merged range (which causes connection storms),
            // we use a fixed pool of workers. Each worker processes a QUEUE of merged ranges
            // sequentially on a single kept-alive connection.
            const max_concurrency = 16;
            const total_merged = merged_list.items.len;

            // Determine number of workers (connections) to use
            const num_workers = @min(max_concurrency, total_merged);

            // Distribute merged ranges to workers
            // We'll just slice the array: worker 0 gets [0..k], worker 1 gets [k..2k], etc.
            const items_per_worker = (total_merged + num_workers - 1) / num_workers;

            // std.debug.print("[TRACE]   Plan: {d} merged ranges -> {d} workers (approx {d} reqs/worker)\n", .{ total_merged, num_workers, items_per_worker });
            // var batch_timer = try std.time.Timer.start();

            const contexts = try self.allocator.alloc(*RequestContext, num_workers);
            defer self.allocator.free(contexts);

            // We only need 1 batch context for the whole operation now
            var batch_ctx = BatchContext(LoopType){ .remaining = num_workers, .loop = self.loop };

            var pooled_count: usize = 0;
            var new_count: usize = 0;
            var handshake_count: usize = 0;
            var cleanup_idx: usize = 0;

            errdefer {
                for (0..cleanup_idx) |j| {
                    contexts[j].deinit();
                }
            }

            for (0..num_workers) |i| {
                const start_idx = i * items_per_worker;
                if (start_idx >= total_merged) break; // Should not happen with split math
                const end_idx = @min(start_idx + items_per_worker, total_merged);
                const worker_queue = merged_list.items[start_idx..end_idx];

                if (worker_queue.len == 0) continue;

                const key = ConnectionKey{
                    .host = self.provider.getHost(),
                    .port = self.provider.getPort(),
                    .use_tls = self.provider.getUseTls(),
                };

                const pool = self.provider.getPool();
                const Pool = GlobalConnectionPool(XevApi);
                const typed_pool: *Pool = @ptrCast(@alignCast(pool));

                const conn = if (typed_pool.acquire(key)) |c| blk: {
                    pooled_count += 1;
                    c.idling = false;
                    c.pending_read = false;
                    c.pending_write = false;
                    c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
                    c.use_direct = self.provider.getUseDirect();
                    break :blk c;
                } else blk: {
                    new_count += 1;
                    const Connection = tls.ConnectionGen(XevApi);
                    const c = try self.allocator.create(Connection);
                    c.* = try Connection.initWithOptions(self.loop, self.allocator, self.provider.getHost(), .{ .verify_certificate = self.provider.getVerifyCertificate() });
                    c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
                    c.use_direct = self.provider.getUseDirect();
                    break :blk c;
                };

                // Create context with the queue
                const ctx = try self.allocator.create(RequestContext);
                contexts[i] = ctx;

                // Initialize context
                ctx.* = .{
                    .orchestrator_ptr = self,
                    .orchestrator_vtable = &.{
                        .onConnect = onConnectGeneric(XevApi),
                        .onData = onDataGeneric(XevApi),
                        .onError = onErrorGeneric(XevApi),
                        .onBody = onBodyGeneric(XevApi),
                    },
                    .conn_ptr = conn,
                    .conn_vtable = &.{
                        .close = struct {
                            fn func(ptr: *anyopaque) void {
                                const c: *tls.ConnectionGen(XevApi) = @ptrCast(@alignCast(ptr));
                                c.close();
                            }
                        }.func,
                    },
                    .allocator = self.allocator,
                    .parser = .{},
                    .batch_ctx_ptr = &batch_ctx,
                    .batch_ctx_vtable = &.{
                        .signalDone = BatchContext(LoopType).signalDone,
                    },

                    // Batching/Queue state
                    .all_ranges = ranges,
                    .all_buffers = buffers,
                    .queue = worker_queue,
                    .queue_idx = 0,

                    // Current request state (initialized to first item)
                    .request_offset = worker_queue[0].request_range.start,
                    .request_end = worker_queue[0].request_range.end,
                };
                cleanup_idx += 1;

                const Connection = tls.ConnectionGen(XevApi);
                const typed_conn: *Connection = @ptrCast(@alignCast(conn));
                typed_conn.user_ctx = ctx;
                typed_conn.on_connect = onConnect;
                typed_conn.on_data = onData;
                typed_conn.on_error = onError;

                if (!typed_conn.handshake_complete) {
                    handshake_count += 1;
                    try typed_conn.connect(addr);
                } else {
                    onConnect(ctx);
                }
            }

            // std.debug.print("[TRACE]   Connections: {d} pooled, {d} new, {d} need handshake\n", .{ pooled_count, new_count, handshake_count });

            try self.loop.run(.until_done);
            // std.debug.print("[TRACE]   Work complete in {d}ms\n", .{batch_timer.read() / 1_000_000});

            // Check for errors and collect failed ranges from ALL queues
            var first_err: ?anyerror = null;
            var retry_ranges = std.ArrayListUnmanaged(io.Range){};
            defer retry_ranges.deinit(self.allocator);
            var retry_buffers = std.ArrayListUnmanaged([]u8){};
            defer retry_buffers.deinit(self.allocator);

            for (contexts) |ctx| {
                if (ctx.err) |err| {
                    if (err == error.TlsConnectionClosed and !ctx.finished) {
                        // std.debug.print("[TRACE]     Worker hit stale connection at queue index {d}/{d}\n", .{ ctx.queue_idx, ctx.queue.len });
                        // Add ALL remaining ranges in this worker's queue to retry list
                        // This includes the current failed one (queue_idx) and all subsequent ones
                        var q_i: usize = ctx.queue_idx;
                        while (q_i < ctx.queue.len) : (q_i += 1) {
                            const req = ctx.queue[q_i];
                            for (req.original_indices.items) |orig_idx| {
                                try retry_ranges.append(self.allocator, ctx.all_ranges[orig_idx]);
                                try retry_buffers.append(self.allocator, ctx.all_buffers[orig_idx]);
                            }
                        }
                        ctx.deinit();
                        continue;
                    }
                    if (first_err == null) first_err = err;
                }
                ctx.deinit();
            }

            // Retry failed requests (recursive, or simple serial retry)
            // For retries, we use the simple 1-connection-per-request model but with fresh connections
            // (readRangesRetry matches current implementation of 1-conn-per-range)
            if (retry_ranges.items.len > 0 and first_err == null) {
                std.debug.print("[TRACE]   Retrying {d} ranges with fresh connections\n", .{retry_ranges.items.len});
                try self.readRangesRetry(addr, retry_ranges.items, retry_buffers.items);
            } else if (first_err) |e| {
                return e;
            }
        }

        /// Retry failed ranges with fresh connections (no pooling)
        fn readRangesRetry(self: *Self, addr: anytype, ranges: []const io.Range, buffers: []const []u8) !void {
            if (ranges.len == 0) return;

            // Process one at a time with fresh connections
            for (ranges, buffers) |range, buf| {
                const Connection = tls.ConnectionGen(XevApi);
                const conn = try self.allocator.create(Connection);
                conn.* = try Connection.initWithOptions(self.loop, self.allocator, self.provider.getHost(), .{ .verify_certificate = self.provider.getVerifyCertificate() });
                conn.tcp_read_buf_size = self.provider.getTcpReadBufSize();
                conn.use_direct = self.provider.getUseDirect();

                const ctx = try self.allocator.create(RequestContext);
                // Use LoopType from outer scope
                var batch_ctx = BatchContext(LoopType){ .remaining = 1, .loop = self.loop };

                // Construct a fake merged request queue of length 1 for consistency
                // Note: we can't easily construct a `scheduler.MergedRequest` here without allocating
                // indices. But we can hack it: RequestContext needs `all_ranges`.
                // For retry, we are passing slice of `ranges` and `buffers`.
                // We'll create a 1-item queue manually.

                // Actually, merged request needs `original_indices`.
                // It's cleaner to handle retry logic by just creating a dummy MergedRequest
                // that points to index 0 of the *passed* ranges slice.
                var indices = std.ArrayListUnmanaged(usize){};
                try indices.append(self.allocator, 0); // Index 0 of `ranges` array
                // We must free this indices list. We can make RequestContext own it or free it here.
                // Since RequestContext queue is const slice, it doesn't own the items.
                // WE need to own the MergedRequest.

                // This is getting complicated.
                // SIMPLIFICATION: readRangesRetry uses the OLD logic?
                // But RequestContext struct CHANGED. I must update readRangesRetry to populate the new fields.

                const fake_merged = scheduler.MergedRequest{
                    .request_range = range,
                    .original_indices = indices,
                };
                // We need to store this somewhere persistent.
                const queue_arr = try self.allocator.alloc(scheduler.MergedRequest, 1);
                queue_arr[0] = fake_merged;
                // Defer free queue_arr AND indices is weird if ctx runs async.
                // Ctx will run until loop done.

                // To simplify: ctx needs `deinit()` to clean up IF it owns things.
                // Here we let ctx own the queue array.

                // Wait, simpler: Make `readRangesRetry` use `readRanges` recursively?
                // `readRanges(retry_ranges, retry_buffers)`
                // But strict recursion might loop if errors persist.
                // And `readRanges` uses pooling. `readRangesRetry` intends *fresh* connections.
                // We can't easily force fresh connections in `readRanges`.

                // OK, we must implement `readRangesRetry` with the new struct.
                // I'll manage the memory manually.

                const ranges_slice = try self.allocator.alloc(io.Range, 1);
                ranges_slice[0] = range;
                const buffers_slice = try self.allocator.alloc([]u8, 1);
                buffers_slice[0] = buf;

                // We'll give ownership of these to ctx to free?
                // No, ctx doesn't own `all_ranges`.

                ctx.* = .{
                    .orchestrator_ptr = self,
                    .orchestrator_vtable = &.{
                        .onConnect = onConnectGeneric(XevApi),
                        .onData = onDataGeneric(XevApi),
                        .onError = onErrorGeneric(XevApi),
                        .onBody = onBodyGeneric(XevApi),
                    },
                    .conn_ptr = conn,
                    .conn_vtable = &.{
                        .close = struct {
                            fn func(ptr: *anyopaque) void {
                                const c: *Connection = @ptrCast(@alignCast(ptr));
                                c.close();
                            }
                        }.func,
                    },
                    .allocator = self.allocator,
                    .parser = .{},
                    .batch_ctx_ptr = &batch_ctx,
                    .batch_ctx_vtable = &.{
                        .signalDone = BatchContext(LoopType).signalDone,
                    },
                    .all_ranges = ranges_slice,
                    .all_buffers = buffers_slice,
                    .queue = queue_arr,
                    .queue_idx = 0,
                    .request_offset = range.start,
                    .request_end = range.end,
                    // Flag for "I own my memory" could be added, or we clean up after loop.
                };

                const typed_conn: *Connection = @ptrCast(@alignCast(conn));
                typed_conn.user_ctx = ctx;
                typed_conn.on_connect = onConnect;
                typed_conn.on_data = onData;
                typed_conn.on_error = onError;

                if (!typed_conn.handshake_complete) {
                    try typed_conn.connect(addr);
                } else {
                    onConnect(ctx);
                }

                try self.loop.run(.until_done);

                // Cleanup
                ctx.deinit();
                self.allocator.free(queue_arr);
                indices.deinit(self.allocator);
                self.allocator.free(ranges_slice);
                self.allocator.free(buffers_slice);
            }
        }

        pub fn fetchSize(self: *Self) !u64 {
            const addr = try self.provider.resolve();
            const key = ConnectionKey{
                .host = self.provider.getHost(),
                .port = self.provider.getPort(),
                .use_tls = self.provider.getUseTls(),
            };

            const pool = self.provider.getPool();
            const Pool = GlobalConnectionPool(XevApi);
            const typed_pool: *Pool = @ptrCast(@alignCast(pool));

            const conn = if (typed_pool.acquire(key)) |c| blk: {
                c.idling = false;
                c.pending_read = false;
                c.pending_write = false;
                c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
                c.use_direct = self.provider.getUseDirect();
                break :blk c;
            } else blk: {
                const Connection = tls.ConnectionGen(XevApi);
                const c = try self.allocator.create(Connection);
                c.* = try Connection.initWithOptions(self.loop, self.allocator, self.provider.getHost(), .{ .verify_certificate = self.provider.getVerifyCertificate() });
                c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
                c.use_direct = self.provider.getUseDirect();
                break :blk c;
            };

            const ctx = try self.allocator.create(RequestContext);
            ctx.* = .{
                .orchestrator_ptr = self,
                .orchestrator_vtable = &.{
                    .onConnect = onConnectGeneric(XevApi),
                    .onData = onDataGeneric(XevApi),
                    .onError = onErrorGeneric(XevApi),
                    .onBody = onBodyGeneric(XevApi),
                },
                .conn_ptr = conn,
                .conn_vtable = &.{
                    .close = struct {
                        fn func(ptr: *anyopaque) void {
                            const c: *tls.ConnectionGen(XevApi) = @ptrCast(@alignCast(ptr));
                            c.close();
                        }
                    }.func,
                },
                .request_offset = 0,
                .request_end = 0,
                .all_ranges = &.{},
                .all_buffers = &.{},
                .queue = &.{},
                .queue_idx = 0,
                .allocator = self.allocator,
                .parser = .{},
                .is_head = true,
                .batch_ctx_ptr = null,
                .batch_ctx_vtable = null,
            };
            defer self.allocator.destroy(ctx);

            const Connection = tls.ConnectionGen(XevApi);
            const typed_conn: *Connection = @ptrCast(@alignCast(conn));
            typed_conn.user_ctx = ctx;
            typed_conn.on_connect = onConnect;
            typed_conn.on_data = onData;
            typed_conn.on_error = onError;

            if (!typed_conn.handshake_complete) {
                try typed_conn.connect(addr);
            } else {
                onConnect(ctx);
            }

            try self.loop.run(.until_done);

            log.debug("fetchSize loop done: finished={}, err={?}, file_size={d}", .{ ctx.finished, ctx.err, ctx.file_size });

            if (ctx.err) |err| {
                if (err == error.EOF or err == error.TlsConnectionClosed) {
                    if (ctx.finished) return ctx.file_size;
                }
                return err;
            }
            return ctx.file_size;
        }
    };
}

pub const CloudProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        onConnect: *const fn (ptr: *anyopaque, ctx: *RequestContext) anyerror!void,
        onHeadComplete: *const fn (ptr: *anyopaque, status: u16, content_length: ?u64) u64,
        resolve: *const fn (ptr: *anyopaque) anyerror!xev.shim_net.Address,
        getHost: *const fn (ptr: *anyopaque) []const u8,
        getPort: *const fn (ptr: *anyopaque) u16,
        getUseTls: *const fn (ptr: *anyopaque) bool,
        getVerifyCertificate: *const fn (ptr: *anyopaque) bool,
        getTcpReadBufSize: *const fn (ptr: *anyopaque) usize,
        getUseDirect: *const fn (ptr: *anyopaque) bool,
        getPool: *const fn (ptr: *anyopaque) *anyopaque, // Erased pool
    };

    pub fn onConnect(self: CloudProvider, ctx: *RequestContext) !void {
        return self.vtable.onConnect(self.ptr, ctx);
    }
    pub fn onHeadComplete(self: CloudProvider, status: u16, content_length: ?u64) u64 {
        return self.vtable.onHeadComplete(self.ptr, status, content_length);
    }
    pub fn resolve(self: CloudProvider) !xev.shim_net.Address {
        return self.vtable.resolve(self.ptr);
    }
    pub fn getHost(self: CloudProvider) []const u8 {
        return self.vtable.getHost(self.ptr);
    }
    pub fn getPort(self: CloudProvider) u16 {
        return self.vtable.getPort(self.ptr);
    }
    pub fn getUseTls(self: CloudProvider) bool {
        return self.vtable.getUseTls(self.ptr);
    }
    pub fn getVerifyCertificate(self: CloudProvider) bool {
        return self.vtable.getVerifyCertificate(self.ptr);
    }
    pub fn getTcpReadBufSize(self: CloudProvider) usize {
        return self.vtable.getTcpReadBufSize(self.ptr);
    }
    pub fn getUseDirect(self: CloudProvider) bool {
        return self.vtable.getUseDirect(self.ptr);
    }
    pub fn getPool(self: CloudProvider) *anyopaque {
        return self.vtable.getPool(self.ptr);
    }
};

fn BatchContext(comptime LoopType: type) type {
    return struct {
        remaining: usize,
        loop: *LoopType,

        pub fn signalDone(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.remaining -= 1;
        }
    };
}

fn onConnect(ctx_void: ?*anyopaque) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    ctx.orchestrator_vtable.onConnect(ctx_void);
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    ctx.orchestrator_vtable.onData(ctx_void, data);
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    ctx.orchestrator_vtable.onError(ctx_void, err);
}

fn onBody(ctx_void: *anyopaque, chunk: []const u8) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    ctx.orchestrator_vtable.onBody(ctx_void, chunk);
}

fn onConnectGeneric(comptime XevApi: type) fn (?*anyopaque) void {
    return struct {
        fn func(ctx_void: ?*anyopaque) void {
            const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
            const orch: *Orchestrator(XevApi) = @ptrCast(@alignCast(ctx.orchestrator_ptr));
            orch.provider.onConnect(ctx) catch |err| {
                ctx.err = err;
                ctx.closeConn();
            };
        }
    }.func;
}

fn onDataGeneric(comptime XevApi: type) fn (?*anyopaque, []const u8) void {
    return struct {
        fn func(ctx_void: ?*anyopaque, data: []const u8) void {
            const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
            const orch: *Orchestrator(XevApi) = @ptrCast(@alignCast(ctx.orchestrator_ptr));

            if (ctx.is_head) {
                log.debug("HEAD response data ({d} bytes): {s}", .{ data.len, data[0..@min(data.len, 512)] });
            }

            ctx.parser.feed(data, ctx, onBody) catch |err| {
                log.err("parser.feed error: {}", .{err});
                ctx.err = err;
                ctx.closeConn();
                return;
            };

            if (ctx.is_head and ctx.parser.headersComplete()) {
                log.debug("HEAD complete: status={d}, content_length={?}", .{ ctx.parser.status_code, ctx.parser.content_length });
                ctx.file_size = orch.provider.onHeadComplete(ctx.parser.status_code, ctx.parser.content_length);
                log.debug("file_size set to: {d}", .{ctx.file_size});
                ctx.finished = true;
                const Connection = tls.ConnectionGen(XevApi);
                const typed_conn: *Connection = @ptrCast(@alignCast(ctx.conn_ptr));
                typed_conn.user_ctx = null;
                typed_conn.idling = true;

                const Pool = GlobalConnectionPool(XevApi);
                const typed_pool: *Pool = @ptrCast(@alignCast(orch.provider.getPool()));

                typed_pool.release(.{
                    .host = orch.provider.getHost(),
                    .port = orch.provider.getPort(),
                    .use_tls = orch.provider.getUseTls(),
                }, typed_conn);
                ctx.signalDone();
            }
        }
    }.func;
}

fn onBodyGeneric(comptime XevApi: type) fn (*anyopaque, []const u8) void {
    return struct {
        fn func(ctx_void: *anyopaque, chunk: []const u8) void {
            const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
            const orch: *Orchestrator(XevApi) = @ptrCast(@alignCast(ctx.orchestrator_ptr));
            if (ctx.parser.status_code != 0) ctx.http_status = ctx.parser.status_code;

            const chunk_start_abs = ctx.request_offset + ctx.total_body_read;
            const chunk_end_abs = chunk_start_abs + chunk.len;

            // Use the current active merged request to find sub-ranges
            const active_req = ctx.queue[ctx.queue_idx];

            for (active_req.original_indices.items) |orig_idx| {
                const range = ctx.all_ranges[orig_idx];
                const dest_buffer = ctx.all_buffers[orig_idx];

                const intersect_start = @max(chunk_start_abs, range.start);
                const intersect_end = @min(chunk_end_abs, range.end);

                if (intersect_start < intersect_end) {
                    const chunk_offset = intersect_start - chunk_start_abs;
                    const dest_offset = intersect_start - range.start;
                    const len = intersect_end - intersect_start;
                    @memcpy(dest_buffer[dest_offset .. dest_offset + len], chunk[chunk_offset .. chunk_offset + len]);
                }
            }

            ctx.total_body_read += chunk.len;
            const body_len = if (ctx.parser.content_length) |cl| cl else 0;
            const is_last_byte = if (body_len > 0) ctx.total_body_read >= body_len else false;

            if (is_last_byte) {
                // Check if we have more requests in the queue
                if (ctx.queue_idx + 1 < ctx.queue.len) {
                    // Chain next request on same connection!
                    ctx.queue_idx += 1;
                    const next_req = ctx.queue[ctx.queue_idx];

                    std.debug.print("[TRACE]     Worker chaining request {d}/{d} (offset {d})\n", .{ ctx.queue_idx + 1, ctx.queue.len, next_req.request_range.start });

                    // Reset request state
                    ctx.request_offset = next_req.request_range.start;
                    ctx.request_end = next_req.request_range.end;
                    ctx.total_body_read = 0;
                    ctx.parser = .{}; // Reset parser state

                    // Send next request
                    orch.provider.onConnect(ctx) catch |err| {
                        ctx.err = err;
                        ctx.closeConn();
                        // If error on chain, we stop. Retry logic in readRanges will pick up from current queue_idx?
                        // Yes, readRanges logic checks queue_idx.
                    };
                    return;
                }

                // All requests in queue done
                ctx.finished = true;
                const Connection = tls.ConnectionGen(XevApi);
                const typed_conn: *Connection = @ptrCast(@alignCast(ctx.conn_ptr));
                typed_conn.user_ctx = null;
                typed_conn.idling = true;

                const Pool = GlobalConnectionPool(XevApi);
                const typed_pool: *Pool = @ptrCast(@alignCast(orch.provider.getPool()));

                typed_pool.release(.{
                    .host = orch.provider.getHost(),
                    .port = orch.provider.getPort(),
                    .use_tls = orch.provider.getUseTls(),
                }, typed_conn);
                ctx.signalDone();
            }
        }
    }.func;
}

fn onErrorGeneric(comptime XevApi: type) fn (?*anyopaque, anyerror) void {
    _ = XevApi;
    return struct {
        fn func(ctx_void: ?*anyopaque, err: anyerror) void {
            const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
            ctx.err = err;
            ctx.closeConn();
            ctx.signalDone();
        }
    }.func;
}

pub const RequestContext = struct {
    orchestrator_ptr: *anyopaque,
    orchestrator_vtable: *const VTable,
    conn_ptr: *anyopaque,
    conn_vtable: *const ConnVTable,

    // Request Queue State
    all_ranges: []const io.Range,
    all_buffers: []const []u8,
    queue: []const @import("../s3/scheduler.zig").MergedRequest,
    queue_idx: usize,

    // Current Request State
    request_offset: u64,
    request_end: u64,

    allocator: std.mem.Allocator,
    parser: ResponseParser,

    total_body_read: usize = 0,
    finished: bool = false,
    err: ?anyerror = null,
    http_status: u16 = 0,
    is_head: bool = false,
    file_size: u64 = 0,
    batch_ctx_ptr: ?*anyopaque = null,
    batch_ctx_vtable: ?*const BatchVTable = null,

    pub const VTable = struct {
        onConnect: *const fn (ctx_void: ?*anyopaque) void,
        onData: *const fn (ctx_void: ?*anyopaque, data: []const u8) void,
        onError: *const fn (ctx_void: ?*anyopaque, err: anyerror) void,
        onBody: *const fn (ctx_void: *anyopaque, chunk: []const u8) void,
    };

    pub const ConnVTable = struct {
        close: *const fn (ptr: *anyopaque) void,
    };

    pub const BatchVTable = struct {
        signalDone: *const fn (ptr: *anyopaque) void,
    };

    pub fn deinit(self: *RequestContext) void {
        self.allocator.destroy(self);
    }

    pub fn signalDone(self: *RequestContext) void {
        if (self.batch_ctx_ptr) |ptr| {
            if (self.batch_ctx_vtable) |vt| {
                vt.signalDone(ptr);
            }
        }
    }

    pub fn closeConn(self: *RequestContext) void {
        self.conn_vtable.close(self.conn_ptr);
    }
};
