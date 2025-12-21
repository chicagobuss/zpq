const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const Connection = zpq.s3.Connection;
const AsyncRequest = zpq.s3.AsyncRequest;

const Context = struct {
    done: bool = false,
    error_occurred: ?anyerror = null,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Custom allocator to track memory usage
    var tracking = TrackingAllocator.init(allocator);
    const tracking_allocator = tracking.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Start a mock server that returns a large body with a gap in the middle
    const port = 12346;
    const addr = try xev.shim_net.Address.parseIp4("127.0.0.1", port);
    var server = try xev.TCP.init(addr);
    try server.bind(addr);
    try server.listen(1);

    var server_ctx = ServerContext{ .loop = &loop };
    var c_accept: xev.Completion = .{};
    server.accept(&loop, &c_accept, ServerContext, &server_ctx, onAccept);

    var conn = try Connection.init(&loop, tracking_allocator, "localhost", false);
    defer conn.deinit();

    var req = AsyncRequest.init(tracking_allocator);
    defer req.deinit();

    var buf1: [10]u8 = undefined;
    var buf2: [10]u8 = undefined;

    // We want byte 0..10 and 1000..1010. Gap is 990 bytes.
    try req.addSegment(&buf1, 10);
    try req.addSegment(null, 990); // GAP
    try req.addSegment(&buf2, 10);

    try req.prepare("localhost", port, "/test", 0, 1010, false, null);

    const WaitCtx = struct {
        done: bool = false,
        fn onDone(c: ?*anyopaque, r: *AsyncRequest) void {
            _ = r;
            const self: *@This() = @ptrCast(@alignCast(c));
            self.done = true;
        }
    };
    var wait_ctx = WaitCtx{};
    req.done_ctx = &wait_ctx;
    req.on_done = WaitCtx.onDone;

    try conn.connect(addr);
    try req.execute(conn);

    while (!wait_ctx.done) {
        try loop.run(.once);
    }

    std.debug.print("Gap Skipping Test Results:\n", .{});
    std.debug.print("- Peak allocated: {} bytes\n", .{tracking.peak_allocated});

    // We expect peak allocated to be small (socket buffers + request buffers),
    // but definitely NOT including the 990 byte gap.
    // S3 buffers are 16KB, plus request headers, etc.
    if (tracking.peak_allocated > 100 * 1024) { // 100KB is a very safe upper bound
        std.debug.print("FAIL: Too much memory allocated! (Possible gap buffering)\n", .{});
        std.process.exit(1);
    }

    // Verify data
    if (!std.mem.eql(u8, &buf1, "AAAAAAAAAA")) {
        std.debug.print("FAIL: buf1 incorrect\n", .{});
        std.process.exit(1);
    }
    if (!std.mem.eql(u8, &buf2, "BBBBBBBBBB")) {
        std.debug.print("FAIL: buf2 incorrect\n", .{});
        std.process.exit(1);
    }

    std.debug.print("test_gap_skipping passed!\n", .{});
}

const ServerContext = struct {
    loop: *xev.Loop,
    conn: ?xev.TCP = null,
    c_read: xev.Completion = .{},
    c_write: xev.Completion = .{},
    read_buf: [1024]u8 = undefined,
};

fn onAccept(s_ctx: ?*ServerContext, loop: *xev.Loop, c: *xev.Completion, r: xev.AcceptError!xev.TCP) xev.CallbackAction {
    _ = c;
    const ctx = s_ctx.?;
    if (r) |conn| {
        ctx.conn = conn;
        ctx.conn.?.read(loop, &ctx.c_read, .{ .slice = &ctx.read_buf }, ServerContext, ctx, onServerRead);
    } else |_| {}
    return .disarm;
}

fn onServerRead(ctx: ?*ServerContext, loop: *xev.Loop, c: *xev.Completion, s: xev.TCP, buf: xev.ReadBuffer, r: xev.ReadError!usize) xev.CallbackAction {
    _ = c;
    _ = s;
    _ = buf;
    const s_ctx = ctx.?;
    if (r) |_| {
        const response = "HTTP/1.1 200 OK\r\nContent-Length: 1010\r\n\r\n";
        s_ctx.conn.?.write(loop, &s_ctx.c_write, .{ .slice = response }, ServerContext, s_ctx, onResponseSent);
    } else |_| {}
    return .disarm;
}

fn onResponseSent(ctx: ?*ServerContext, loop: *xev.Loop, c: *xev.Completion, s: xev.TCP, buf: xev.WriteBuffer, r: xev.WriteError!usize) xev.CallbackAction {
    _ = c;
    _ = s;
    _ = buf;
    const s_ctx = ctx.?;
    if (r) |_| {
        // Send body: 10 'A's, then 990 'X's (gap), then 10 'B's
        var body: [1010]u8 = undefined;
        @memset(body[0..10], 'A');
        @memset(body[10..1000], 'X');
        @memset(body[1000..1010], 'B');

        // We must dupe for xev
        const body_copy = std.heap.page_allocator.dupe(u8, &body) catch unreachable;
        s_ctx.conn.?.write(loop, &s_ctx.c_write, .{ .slice = body_copy }, ServerContext, s_ctx, onBodySent);
    } else |_| {}
    return .disarm;
}

fn onBodySent(ctx: ?*ServerContext, loop: *xev.Loop, c: *xev.Completion, s: xev.TCP, buf: xev.WriteBuffer, r: xev.WriteError!usize) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = s;
    std.heap.page_allocator.free(buf.slice);
    _ = ctx;
    _ = r catch {};
    return .disarm;
}

const TrackingAllocator = struct {
    parent: std.mem.Allocator,
    current_allocated: usize = 0,
    peak_allocated: usize = 0,

    pub fn init(parent: std.mem.Allocator) TrackingAllocator {
        return .{ .parent = parent };
    }

    pub fn allocator(self: *TrackingAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn alloc(ptr: *anyopaque, len: usize, ptr_align: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        const result = self.parent.rawAlloc(len, ptr_align, ret_addr);
        if (result != null) {
            self.current_allocated += len;
            if (self.current_allocated > self.peak_allocated) {
                self.peak_allocated = self.current_allocated;
            }
        }
        return result;
    }

    fn resize(ptr: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        if (new_len > buf.len) {
            if (self.parent.rawResize(buf, buf_align, new_len, ret_addr)) {
                self.current_allocated += (new_len - buf.len);
                if (self.current_allocated > self.peak_allocated) {
                    self.peak_allocated = self.current_allocated;
                }
                return true;
            }
            return false;
        } else {
            if (self.parent.rawResize(buf, buf_align, new_len, ret_addr)) {
                self.current_allocated -= (buf.len - new_len);
                return true;
            }
            return false;
        }
    }

    fn remap(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        const result = self.parent.rawRemap(memory, alignment, new_len, ret_addr);
        if (result != null) {
            self.current_allocated -= memory.len;
            self.current_allocated += new_len;
            if (self.current_allocated > self.peak_allocated) {
                self.peak_allocated = self.current_allocated;
            }
        }
        return result;
    }

    fn free(ptr: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, ret_addr: usize) void {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        self.parent.rawFree(buf, buf_align, ret_addr);
        self.current_allocated -= buf.len;
    }
};
