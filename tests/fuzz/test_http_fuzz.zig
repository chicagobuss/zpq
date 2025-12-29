const std = @import("std");
const minish = @import("minish");
const gen = minish.gen;
const zpq = @import("zpq");
const ResponseParser = zpq.io.response_parser.ResponseParser;

// --------------------------------------------------------------------------
// 1. Crash-Free Fuzzing
// --------------------------------------------------------------------------
// Feeding random garbage to the parser. It should return an error, but NEVER panic.
fn prop_parser_crash_free(data: []const u8) !void {
    var parser = ResponseParser{};
    const Context = struct {
        fn onBody(ctx: ?*anyopaque, chunk: []const u8) void {
            _ = ctx;
            _ = chunk;
        }
    };

    // We expect errors for most random data, which is fine.
    // The test only fails if there is a panic/segfault.
    _ = parser.feed(data, undefined, Context.onBody) catch {};
}

// --------------------------------------------------------------------------
// 2. Round-Trip Fuzzing (Valid Inputs)
// --------------------------------------------------------------------------
const ValidResponse = struct {
    status_code: u16,
    content_length: usize,
    body: []const u8,
};

fn prop_parser_valid_roundtrip(ctx: FuzzContext, resp: ValidResponse) !void {
    var buf = std.ArrayListUnmanaged(u8){};
    defer buf.deinit(ctx.allocator);

    // Construct valid HTTP response
    const line1 = try std.fmt.allocPrint(ctx.allocator, "HTTP/1.1 {d} OK\r\n", .{resp.status_code});
    defer ctx.allocator.free(line1);
    const line2 = try std.fmt.allocPrint(ctx.allocator, "Content-Length: {d}\r\n", .{resp.content_length});
    defer ctx.allocator.free(line2);

    try buf.appendSlice(ctx.allocator, line1);
    try buf.appendSlice(ctx.allocator, line2);
    try buf.appendSlice(ctx.allocator, "\r\n");
    try buf.appendSlice(ctx.allocator, resp.body);

    var parser = ResponseParser{};
    var body_received = std.ArrayListUnmanaged(u8){};
    defer body_received.deinit(ctx.allocator);

    const BodyCtx = struct {
        allocator: std.mem.Allocator,
        received: *std.ArrayListUnmanaged(u8),
        fn onBody(ptr: *anyopaque, chunk: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.received.appendSlice(self.allocator, chunk) catch {};
        }
    };
    var body_ctx = BodyCtx{ .allocator = ctx.allocator, .received = &body_received };

    try parser.feed(buf.items, &body_ctx, BodyCtx.onBody);

    try std.testing.expectEqual(resp.status_code, parser.status_code);
    try std.testing.expectEqual(resp.content_length, parser.content_length.?);
    try std.testing.expectEqualStrings(resp.body, body_received.items);
    try std.testing.expect(parser.state == .done);
}

// --------------------------------------------------------------------------
// Generators
// --------------------------------------------------------------------------

fn genValidResponse(tc: *minish.TestCase) !ValidResponse {
    const status_code = try tc.choiceInRange(u16, 100, 599);
    const content_length = try tc.choiceInRange(usize, 0, 1000);

    // Generate body of exact length
    var body = try tc.allocator.alloc(u8, content_length);
    for (0..content_length) |i| {
        body[i] = try tc.choiceInRange(u8, 0, 255);
    }

    return ValidResponse{
        .status_code = status_code,
        .content_length = content_length,
        .body = body,
    };
}

fn freeValidResponse(allocator: std.mem.Allocator, resp: ValidResponse) void {
    allocator.free(resp.body);
}

const FuzzContext = struct {
    allocator: std.mem.Allocator,

    pub fn run(self: @This(), resp: ValidResponse) !void {
        try prop_parser_valid_roundtrip(self, resp);
    }
};

test "http parser crash-free fuzz" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const garbage_gen = gen.string(.{ .min_len = 0, .max_len = 1024, .charset = .ascii });
    try minish.check(allocator, garbage_gen, prop_parser_crash_free, .{ .num_runs = 100 });
}

test "http parser valid round-trip fuzz" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const resp_gen = gen.Generator(ValidResponse){
        .generateFn = genValidResponse,
        .shrinkFn = null,
        .freeFn = freeValidResponse,
    };

    const ctx = FuzzContext{ .allocator = allocator };
    try minish.check(allocator, resp_gen, ctx, .{ .num_runs = 100 });
}
