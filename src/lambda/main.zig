//! ZPQ Lambda binary entry point.
//!
//! Constraints established by docs/lambda_capabilities.md:
//!   - io_uring is unavailable (AWS seccomp returns ENOSYS).
//!   - Kernel is AL2 5.10, not AL2023 6.x — no epoll_pwait2, no clone3.
//!   - epoll/eventfd2/timerfd_create/signalfd4/mlock are allowed.
//!   - SO_ZEROCOPY and TCP_FASTOPEN setsockopt allowed.
//!
//! This binary intentionally excludes io_uring code at compile time via
//! `build_options.lambda`. The in-tree epoll backend is the only event
//! loop driver linked here.
//!
//! Lifecycle:
//!   1. Init the epoll Loop and the runtime API client.
//!   2. Long-poll the runtime API for the next invocation.
//!   3. Dispatch to the handler (currently a stub that exercises the
//!      Loop and echoes the event body).
//!   4. Post the response.
//!   5. Repeat. Process exits on fatal errors only — the runtime
//!      will restart the bootstrap binary if we exit.

const std = @import("std");
const zpq = @import("zpq");
const runtime = @import("runtime.zig");

const Loop = zpq.io.loop.Loop;
const Completion = zpq.io.loop.Completion;
const Result = zpq.io.loop.Result;

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try Loop.init(allocator);
    defer loop.deinit();

    var client = runtime.Client.fromEnv(allocator, init.environ) catch |err| {
        std.debug.print("zpq lambda: runtime client init failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer client.deinit();

    while (true) {
        var inv = client.nextInvocation() catch |err| {
            std.debug.print("zpq lambda: poll error {s}\n", .{@errorName(err)});
            // Brief backoff to avoid hot-spinning on a misconfigured runtime API.
            const ts: std.os.linux.timespec = .{ .sec = 1, .nsec = 0 };
            _ = std.os.linux.nanosleep(&ts, null);
            continue;
        };
        defer inv.deinit(allocator);

        const response = handle(allocator, &loop, &inv) catch |err| {
            client.postError(inv.request_id, "HandlerError", @errorName(err)) catch |perr| {
                std.debug.print("zpq lambda: postError failed: {s}\n", .{@errorName(perr)});
            };
            continue;
        };
        defer allocator.free(response);

        client.postResponse(inv.request_id, response) catch |err| {
            std.debug.print("zpq lambda: postResponse failed: {s}\n", .{@errorName(err)});
        };
    }
}

/// Per-invocation handler.
///
/// Phase A stub: routes a 1 ms timer through the Loop and returns a
/// JSON envelope echoing the input. Validates that the runtime API
/// path AND the Loop path both work end-to-end inside a Lambda
/// invocation. Replaced by real Parquet/S3 logic when the core+sink
/// land.
fn handle(allocator: std.mem.Allocator, loop: *Loop, inv: *const runtime.Invocation) ![]u8 {
    var fired: bool = false;
    var c: Completion = .{
        .op = .{ .timer = .{ .ns_from_now = std.time.ns_per_ms } },
        .userdata = &fired,
        .callback = onTimer,
    };
    loop.submit(&c);

    var iters: u32 = 200;
    while (loop.active() > 0 and iters > 0) : (iters -= 1) {
        try loop.run_for_ns(10 * std.time.ns_per_ms);
    }
    if (!fired) return error.LoopTimerStuck;

    return std.fmt.allocPrint(
        allocator,
        "{{\"ok\":true,\"loop\":\"epoll\",\"request_id\":\"{s}\",\"echo_bytes\":{d}}}",
        .{ inv.request_id, inv.body.len },
    );
}

fn onTimer(ud: ?*anyopaque, _: *Loop, _: *Completion, _: Result) void {
    const fired: *bool = @ptrCast(@alignCast(ud.?));
    fired.* = true;
}

test {
    // Pull in tests from sibling files.
    _ = @import("runtime.zig");
}
