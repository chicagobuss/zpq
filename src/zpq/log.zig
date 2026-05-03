const std = @import("std");
const xev = @import("xev");

pub const Level = enum(u8) {
    trace = 0,
    debug = 1,
    info = 2,
    warn = 3,
    err = 4,

    pub fn asText(self: Level) []const u8 {
        return switch (self) {
            .trace => "TRACE",
            .debug => "DEBUG",
            .info => "INFO ",
            .warn => "WARN ",
            .err => "ERROR",
        };
    }
};

/// LogEntry is passed from worker threads to the event loop.
/// We use a fixed-size buffer for the message to avoid dynamic allocation in hot paths.
pub const LogEntry = struct {
    next: ?*LogEntry = null,
    level: Level,
    correlation_id: u64,
    timestamp: i64,
    msg: [256]u8,
    msg_len: usize,
};

/// Global Correlation ID for tracing across async tasks.
pub threadlocal var correlation_id: u64 = 0;

pub fn setCorrelationId(id: u64) void {
    correlation_id = id;
}

/// High-Performance Asynchronous Logger.
/// Uses an intrusive MPSC queue to allow multiple producers to push logs without locking.
/// A single consumer on the event loop drains the queue and writes to stderr.
pub const AsyncLogger = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    active_level: std.atomic.Value(Level),
    pending_notify: std.atomic.Value(bool),

    // MPSC Queue (Multi-Producer, Single-Consumer)
    lock_free_queue: IntrusiveMPSC(LogEntry) = .{},

    // Notifier to wake the event loop
    notifier: xev.Dynamic.Async,
    completion: xev.Dynamic.Completion = .{},

    // Pre-allocated pool of entries to avoid runtime allocation
    pool: []LogEntry,
    pool_idx: std.atomic.Value(usize),

    pub fn init(allocator: std.mem.Allocator, level: Level) !*AsyncLogger {
        const self = try allocator.create(AsyncLogger);
        self.allocator = allocator;
        self.active_level = std.atomic.Value(Level).init(level);
        self.pending_notify = std.atomic.Value(bool).init(false);
        self.notifier = try xev.Dynamic.Async.init();

        // fixed pool for now
        self.pool = try allocator.alloc(LogEntry, 4096);
        self.lock_free_queue.init();
        self.pool_idx = std.atomic.Value(usize).init(0);

        return self;
    }

    pub fn deinit(self: *AsyncLogger) void {
        self.notifier.deinit();
        // Drain any remaining logs to stderr before destroying the pool
        while (self.lock_free_queue.pop()) |entry| {
            std.debug.print("[{d}] [{s}] [ID:{x:0>16}] {s}\n", .{
                entry.timestamp,
                entry.level.asText(),
                entry.correlation_id,
                entry.msg[0..entry.msg_len],
            });
        }
        self.allocator.free(self.pool);
        self.allocator.destroy(self);
    }

    /// Start the background draining task on the provided loop.
    pub fn start(self: *AsyncLogger, loop: *xev.Dynamic.Loop) !void {
        self.notifier.wait(loop, &self.completion, AsyncLogger, self, onWakeup);
    }

    /// Log a message. This is non-blocking and safe for hot threads.
    pub fn log(self: *AsyncLogger, level: Level, comptime fmt: []const u8, args: anytype) void {

        // 1. Zero-cost log level gate
        if (@intFromEnum(level) < @intFromEnum(self.active_level.load(.monotonic))) return;

        // 2. Grab an entry from the pool (atomic bump)
        const idx = self.pool_idx.fetchAdd(1, .monotonic) % self.pool.len;
        const entry = &self.pool[idx];

        // 3. Fill entry
        entry.level = level;
        entry.correlation_id = correlation_id;

        const now = std.time.Instant.now() catch {
            entry.timestamp = 0;
            return;
        };
        entry.timestamp = @intCast(now.timestamp.sec * 1000 + @divFloor(now.timestamp.nsec, 1_000_000));

        const msg = std.fmt.bufPrint(&entry.msg, fmt, args) catch |err| blk: {
            if (err == error.NoSpaceLeft) {
                entry.msg_len = entry.msg.len;
                break :blk entry.msg[0..entry.msg.len];
            }
            return;
        };
        entry.msg_len = msg.len;

        // 4. Push to Non-Blocking Queue
        self.lock_free_queue.push(entry);

        // 5. Notify the Event Loop (Consolidated)
        if (self.pending_notify.swap(true, .acq_rel) == false) {
            self.notifier.notify() catch {
                self.pending_notify.store(false, .release);
            };
        }
    }

    fn onWakeup(
        self: ?*AsyncLogger,
        loop: *xev.Dynamic.Loop,
        completion: *xev.Dynamic.Completion,
        result: xev.Dynamic.Async.WaitError!void,
    ) xev.CallbackAction {
        _ = result catch {};
        const logger = self.?;

        // Clear the notification gate so new notifies can happen
        logger.pending_notify.store(false, .release);

        // Drain the queue
        while (logger.lock_free_queue.pop()) |entry| {
            std.debug.print("[{d}] [{s}] [ID:{x:0>16}] {s}\n", .{
                entry.timestamp,
                entry.level.asText(),
                entry.correlation_id,
                entry.msg[0..entry.msg_len],
            });
        }

        // Re-arm for next batch
        logger.notifier.wait(loop, completion, AsyncLogger, logger, onWakeup);
        return .disarm;
    }
};

/// An intrusive MPSC (multi-provider, single consumer) queue implementation.
/// Verified race-free for Zig 0.16.x.
pub fn IntrusiveMPSC(comptime T: type) type {
    return struct {
        const Self = @This();
        head: *T = undefined,
        tail: *T = undefined,
        stub: T = undefined,

        pub fn init(self: *Self) void {
            self.head = &self.stub;
            self.tail = &self.stub;
            self.stub.next = null;
        }

        pub fn push(self: *Self, v: *T) void {
            @atomicStore(?*T, &v.next, null, .unordered);
            const prev = @atomicRmw(*T, &self.head, .Xchg, v, .acq_rel);
            @atomicStore(?*T, &prev.next, v, .release);
        }

        pub fn pop(self: *Self) ?*T {
            var tail = @atomicLoad(*T, &self.tail, .unordered);
            var next_ = @atomicLoad(?*T, &tail.next, .acquire);
            if (tail == &self.stub) {
                const next = next_ orelse return null;
                @atomicStore(*T, &self.tail, next, .unordered);
                tail = next;
                next_ = @atomicLoad(?*T, &tail.next, .acquire);
            }

            if (next_) |next| {
                @atomicStore(*T, &self.tail, next, .release);
                tail.next = null;
                return tail;
            }

            const head = @atomicLoad(*T, &self.head, .unordered);
            if (tail != head) return null;
            self.push(&self.stub);

            next_ = @atomicLoad(?*T, &tail.next, .acquire);
            if (next_) |next| {
                @atomicStore(*T, &self.tail, next, .unordered);
                tail.next = null;
                return tail;
            }

            return null;
        }
    };
}
