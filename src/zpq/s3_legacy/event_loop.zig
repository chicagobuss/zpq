const std = @import("std");

/// A minimal event loop wrapper around kqueue (macOS/BSD) or epoll (Linux).
/// Currently hardcoded for kqueue since we are on macOS.
pub const EventLoop = struct {
    kq_fd: std.posix.fd_t,
    allocator: std.mem.Allocator,
    
    // We need to track callbacks for FDs.
    // For this micro-test, we'll keep it simple: map FD -> Context pointer.
    // In a real system, this would be more sophisticated (slab allocator, etc).
    // Using a simple array assuming small FD numbers for the test, or a HashMap.
    callbacks: std.AutoHashMap(std.posix.fd_t, *anyopaque),

    pub fn init(allocator: std.mem.Allocator) !EventLoop {
        const fd = try std.posix.kqueue();
        return .{
            .kq_fd = fd,
            .allocator = allocator,
            .callbacks = std.AutoHashMap(std.posix.fd_t, *anyopaque).init(allocator),
        };
    }

    pub fn deinit(self: *EventLoop) void {
        std.posix.close(self.kq_fd);
        self.callbacks.deinit();
    }

    /// Register interest in READ events for a socket.
    /// 'context' is passed back when event fires.
    pub fn registerRead(self: *EventLoop, fd: std.posix.fd_t, context: *anyopaque) !void {
        // EV_SET(&kev, fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, udata);
        const kevent = std.posix.Kevent{
            .ident = @intCast(fd),
            .filter = std.c.EVFILT.READ,
            .flags = std.c.EV.ADD | std.c.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(context),
        };
        
        const nevents = try std.posix.kevent(self.kq_fd, @as([]const std.posix.Kevent, &.{kevent}), &.{}, null);
        _ = nevents;
        
        try self.callbacks.put(fd, context);
    }

    /// Register interest in WRITE events for a socket.
    pub fn registerWrite(self: *EventLoop, fd: std.posix.fd_t, context: *anyopaque) !void {
        const kevent = std.posix.Kevent{
            .ident = @intCast(fd),
            .filter = std.c.EVFILT.WRITE,
            .flags = std.c.EV.ADD | std.c.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(context),
        };
        
        const nevents = try std.posix.kevent(self.kq_fd, @as([]const std.posix.Kevent, &.{kevent}), &.{}, null);
        _ = nevents;
        
        try self.callbacks.put(fd, context);
    }

    /// Unregister a file descriptor (remove callback).
    pub fn unregister(self: *EventLoop, fd: std.posix.fd_t) void {
        _ = self.callbacks.remove(fd);
    }

    /// Wait for events and dispatch them.
    /// Returns number of events processed.
    pub fn tick(self: *EventLoop) !usize {
        var events: [64]std.posix.Kevent = undefined;
        
        // Wait indefinitely (null timeout)
        const n = try std.posix.kevent(self.kq_fd, &.{}, &events, null);
        
        for (events[0..n]) |ev| {
            const fd: std.posix.fd_t = @intCast(ev.ident);
            const context_ptr = ev.udata;
            
            if (context_ptr != 0) {
                const ptr_raw = @as(*anyopaque, @ptrFromInt(context_ptr));
                
                if (ev.filter == std.c.EVFILT.READ) {
                    const IHandler = struct {
                        onReadReady: *const fn(ptr: *anyopaque, fd: std.posix.fd_t) void,
                    };
                    const handler: *IHandler = @ptrCast(@alignCast(ptr_raw));
                    handler.onReadReady(ptr_raw, fd);
                } else if (ev.filter == std.c.EVFILT.WRITE) {
                    const IHandler = struct {
                        // Skip first field (onReadReady)
                        _pad: usize, 
                        onWriteReady: *const fn(ptr: *anyopaque, fd: std.posix.fd_t) void,
                    };
                    const handler: *IHandler = @ptrCast(@alignCast(ptr_raw));
                    handler.onWriteReady(ptr_raw, fd);
                }
            }
        }
        
        return n;
    }
};

