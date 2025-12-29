const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const io = @import("../interface.zig");
const scheduler = @import("../s3/scheduler.zig");
const ResponseParser = @import("../http/response_parser.zig").ResponseParser;
const GlobalConnectionPool = @import("../s3/global_pool.zig").GlobalConnectionPool;
const ConnectionKey = @import("../s3/global_pool.zig").ConnectionKey;

const log = std.log.scoped(.cloud_orchestrator);

pub const Orchestrator = struct {
    provider: CloudProvider,
    loop: *xev.Loop,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, loop: *xev.Loop, provider: CloudProvider) Orchestrator {
        return .{
            .allocator = allocator,
            .loop = loop,
            .provider = provider,
        };
    }

    pub fn readRanges(self: *Orchestrator, ranges: []const io.Range, buffers: []const []u8) !void {
        if (ranges.len == 0) return;

        const addr = try self.provider.resolve();

        // 1. Merge ranges using scheduler
        var merged_list = try scheduler.mergeRanges(self.allocator, ranges);
        defer {
            for (merged_list.items) |*m| m.original_indices.deinit(self.allocator);
            merged_list.deinit(self.allocator);
        }

        const max_concurrency = 64;
        var i: usize = 0;
        while (i < merged_list.items.len) {
            const batch_size = @min(max_concurrency, merged_list.items.len - i);
            const batch_merged = merged_list.items[i .. i + batch_size];

            const contexts = try self.allocator.alloc(*RequestContext, batch_size);
            defer self.allocator.free(contexts);

            var batch_ctx = BatchContext{ .remaining = batch_size, .loop = self.loop };

            var cleanup_idx: usize = 0;
            errdefer {
                for (0..cleanup_idx) |j| {
                    contexts[j].deinit();
                }
            }

            for (batch_merged, 0..) |merged, j| {
                const key = ConnectionKey{
                    .host = self.provider.getHost(),
                    .port = self.provider.getPort(),
                    .use_tls = self.provider.getUseTls(),
                };

                const pool = self.provider.getPool();
                const conn = if (pool.acquire(key)) |c| blk: {
                    c.idling = false;
                    c.pending_read = false;
                    c.pending_write = false;
                    c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
                    c.use_direct = self.provider.getUseDirect();
                    break :blk c;
                } else blk: {
                    const c = try self.allocator.create(tls.Connection);
                    c.* = try tls.Connection.initWithOptions(self.loop, self.allocator, self.provider.getHost(), .{ .verify_certificate = self.provider.getVerifyCertificate() });
                    c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
                    c.use_direct = self.provider.getUseDirect();
                    break :blk c;
                };

                const sub_ranges = try self.allocator.alloc(io.Range, merged.original_indices.items.len);
                const dest_buffers = try self.allocator.alloc([]u8, merged.original_indices.items.len);
                for (merged.original_indices.items, 0..) |orig_idx, k| {
                    sub_ranges[k] = ranges[orig_idx];
                    dest_buffers[k] = buffers[orig_idx];
                }

                const ctx = try self.allocator.create(RequestContext);
                contexts[j] = ctx;
                ctx.* = .{
                    .orchestrator = self,
                    .conn = conn,
                    .request_offset = merged.request_range.start,
                    .request_end = merged.request_range.end,
                    .sub_ranges = sub_ranges,
                    .dest_buffers = dest_buffers,
                    .allocator = self.allocator,
                    .parser = .{},
                    .batch_ctx = &batch_ctx,
                };
                cleanup_idx += 1;

                conn.user_ctx = ctx;
                conn.on_connect = onConnect;
                conn.on_data = onData;
                conn.on_error = onError;

                if (!conn.handshake_complete) {
                    try conn.connect(addr);
                } else {
                    onConnect(ctx);
                }
            }

            try self.loop.run(.until_done);

            var first_err: ?anyerror = null;
            for (contexts) |ctx| {
                if (ctx.err) |err| {
                    if (err == error.EOF or err == error.TlsConnectionClosed) {
                        if (ctx.finished) {
                            ctx.deinit();
                            continue;
                        }
                    }
                    if (first_err == null) first_err = err;
                }
                ctx.deinit();
            }

            if (first_err) |e| return e;
            i += batch_size;
        }
    }

    pub fn fetchSize(self: *Orchestrator) !u64 {
        const addr = try self.provider.resolve();
        const key = ConnectionKey{
            .host = self.provider.getHost(),
            .port = self.provider.getPort(),
            .use_tls = self.provider.getUseTls(),
        };

        const pool = self.provider.getPool();
        const conn = if (pool.acquire(key)) |c| blk: {
            c.idling = false;
            c.pending_read = false;
            c.pending_write = false;
            c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
            c.use_direct = self.provider.getUseDirect();
            break :blk c;
        } else blk: {
            const c = try self.allocator.create(tls.Connection);
            c.* = try tls.Connection.initWithOptions(self.loop, self.allocator, self.provider.getHost(), .{ .verify_certificate = self.provider.getVerifyCertificate() });
            c.tcp_read_buf_size = self.provider.getTcpReadBufSize();
            c.use_direct = self.provider.getUseDirect();
            break :blk c;
        };

        const ctx = try self.allocator.create(RequestContext);
        ctx.* = .{
            .orchestrator = self,
            .conn = conn,
            .request_offset = 0,
            .request_end = 0,
            .sub_ranges = &.{},
            .dest_buffers = &.{},
            .allocator = self.allocator,
            .parser = .{},
            .is_head = true,
        };
        defer self.allocator.destroy(ctx);

        conn.user_ctx = ctx;
        conn.on_connect = onConnect;
        conn.on_data = onData;
        conn.on_error = onError;

        if (!conn.handshake_complete) {
            try conn.connect(addr);
        } else {
            onConnect(ctx);
        }

        try self.loop.run(.until_done);

        if (ctx.err) |err| {
            if (err == error.EOF or err == error.TlsConnectionClosed) {
                if (ctx.finished) return ctx.file_size;
            }
            return err;
        }
        return ctx.file_size;
    }
};

pub const RequestContext = struct {
    orchestrator: *Orchestrator,
    conn: *tls.Connection,
    request_offset: u64,
    request_end: u64,
    sub_ranges: []const io.Range,
    dest_buffers: []const []u8,
    allocator: std.mem.Allocator,
    parser: ResponseParser,

    total_body_read: usize = 0,
    finished: bool = false,
    err: ?anyerror = null,
    http_status: u16 = 0,
    is_head: bool = false,
    file_size: u64 = 0,
    batch_ctx: ?*BatchContext = null,

    pub fn deinit(self: *RequestContext) void {
        self.allocator.free(self.sub_ranges);
        self.allocator.free(self.dest_buffers);
        self.allocator.destroy(self);
    }
};

const BatchContext = struct {
    remaining: usize,
    loop: *xev.Loop,

    pub fn signalDone(self: *@This()) void {
        self.remaining -= 1;
    }
};

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
        getPool: *const fn (ptr: *anyopaque) *GlobalConnectionPool,
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
    pub fn getPool(self: CloudProvider) *GlobalConnectionPool {
        return self.vtable.getPool(self.ptr);
    }
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    ctx.orchestrator.provider.onConnect(ctx) catch |err| {
        ctx.err = err;
        ctx.conn.close();
    };
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    ctx.parser.feed(data, ctx, onBody) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };

    if (ctx.is_head and ctx.parser.headersComplete()) {
        ctx.file_size = ctx.orchestrator.provider.onHeadComplete(ctx.parser.status_code, ctx.parser.content_length);
        ctx.finished = true;
        ctx.conn.user_ctx = null;
        ctx.conn.idling = true;
        ctx.orchestrator.provider.getPool().release(.{
            .host = ctx.orchestrator.provider.getHost(),
            .port = ctx.orchestrator.provider.getPort(),
            .use_tls = ctx.orchestrator.provider.getUseTls(),
        }, ctx.conn);
        if (ctx.batch_ctx) |bc| bc.signalDone();
    }
}

fn onBody(ctx_void: *anyopaque, chunk: []const u8) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    if (ctx.parser.status_code != 0) ctx.http_status = ctx.parser.status_code;

    const chunk_start_abs = ctx.request_offset + ctx.total_body_read;
    const chunk_end_abs = chunk_start_abs + chunk.len;

    for (ctx.sub_ranges, 0..) |range, i| {
        const intersect_start = @max(chunk_start_abs, range.start);
        const intersect_end = @min(chunk_end_abs, range.end);
        if (intersect_start < intersect_end) {
            const chunk_offset = intersect_start - chunk_start_abs;
            const dest_offset = intersect_start - range.start;
            const len = intersect_end - intersect_start;
            @memcpy(ctx.dest_buffers[i][dest_offset .. dest_offset + len], chunk[chunk_offset .. chunk_offset + len]);
        }
    }

    ctx.total_body_read += chunk.len;
    const body_len = if (ctx.parser.content_length) |cl| cl else 0;
    const is_last_byte = if (body_len > 0) ctx.total_body_read >= body_len else false;

    if (is_last_byte) {
        ctx.finished = true;
        ctx.conn.user_ctx = null;
        ctx.conn.idling = true;
        ctx.orchestrator.provider.getPool().release(.{
            .host = ctx.orchestrator.provider.getHost(),
            .port = ctx.orchestrator.provider.getPort(),
            .use_tls = ctx.orchestrator.provider.getUseTls(),
        }, ctx.conn);
        if (ctx.batch_ctx) |bc| bc.signalDone();
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    ctx.err = err;
    ctx.conn.close();
    if (ctx.batch_ctx) |bc| bc.signalDone();
}

