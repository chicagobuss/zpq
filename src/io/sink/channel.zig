const std = @import("std");

/// A thread-safe bounded channel for sending Morsels to the SinkTask.
/// Uses a Mutex + Condition Variable + Semaphore (conceptually).
pub fn SinkChannel(comptime T: type) type {
    return struct {
        const Self = @This();
        mutex: std.Thread.Mutex = .{},
        cond_not_empty: std.Thread.Condition = .{},
        cond_not_full: std.Thread.Condition = .{},
        queue: std.ArrayListUnmanaged(T) = .{},
        closed: bool = false,
        capacity: usize,
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, capacity: usize) Self {
            return .{
                .allocator = allocator,
                .capacity = capacity,
            };
        }

        pub fn deinit(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.queue.deinit(self.allocator);
        }

        /// Send an item to the channel. Blocks if full.
        /// Returns error if closed.
        pub fn send(self: *Self, item: T) !void {
            self.mutex.lock();
            
            while (self.queue.items.len >= self.capacity and !self.closed) {
                self.cond_not_full.wait(&self.mutex);
            }

            if (self.closed) {
                self.mutex.unlock();
                return error.ChannelClosed;
            }

            try self.queue.append(self.allocator, item);
            self.mutex.unlock();
            self.cond_not_empty.signal(); // Signal consumer
        }

        /// Receive an item. Blocks until available or closed.
        /// Returns null if closed and empty.
        pub fn recv(self: *Self) !?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.queue.items.len == 0) {
                if (self.closed) {
                    return null;
                }
                self.cond_not_empty.wait(&self.mutex);
            }

            const item = self.queue.orderedRemove(0);
            self.cond_not_full.signal(); // Wake producers
            return item;
        }

        /// Receive an item if available, otherwise return null immediately.
        pub fn tryRecv(self: *Self) !?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.queue.items.len == 0) {
                return null;
            }

            const item = self.queue.orderedRemove(0);
            self.cond_not_full.signal(); // Wake producers
            return item;
        }

        pub fn close(self: *Self) void {
            self.mutex.lock();
            self.closed = true;
            self.mutex.unlock();
            self.cond_not_empty.signal(); // Wake consumer
            self.cond_not_full.broadcast(); // Wake blocked producers
        }
    };
}
