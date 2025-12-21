const std = @import("std");
const xev = @import("xev");

/// A wrapper around xev.Loop for the S3 stack.
pub const EventLoop = struct {
    loop: *xev.Loop,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !EventLoop {
        const loop = try allocator.create(xev.Loop);
        errdefer allocator.destroy(loop);
        loop.* = try xev.Loop.init(.{});
        return .{
            .loop = loop,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *EventLoop) void {
        self.loop.deinit();
        self.allocator.destroy(self.loop);
    }

    /// Register interest in READ events for a socket.
    /// In xev, we typically use higher-level watchers (TCP, UDP, File)
    /// but for our hybrid state machine, we'll keep the register interface.
    /// Actually, AsyncRequest now has its own state machine.
    pub fn registerRead(self: *EventLoop, fd: std.posix.fd_t, context: *anyopaque) !void {
        _ = self;
        _ = fd;
        _ = context;
        // In the new xev-based stack, AsyncRequest will use xev watchers directly.
        // For now, we'll leave these as stubs or refactor the caller.
    }

    pub fn registerWrite(self: *EventLoop, fd: std.posix.fd_t, context: *anyopaque) !void {
        _ = self;
        _ = fd;
        _ = context;
    }

    pub fn unregister(self: *EventLoop, fd: std.posix.fd_t) void {
        _ = self;
        _ = fd;
    }

    pub fn tick(self: *EventLoop) !usize {
        // xev.Loop.run(.once) is equivalent to a tick.
        // It returns number of events if using dynamic, but static run() might not.
        try self.loop.run(.once);
        return 1; // Return 1 to indicate we did something
    }
};
