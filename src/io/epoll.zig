//! epoll-based event loop.
//!
//! Key design choices:
//!   - Caller-allocated Completion with an Operation tagged union.
//!   - Submissions are queued; the syscall happens at the start of run().
//!   - Completed events are also queued; callbacks fire from the queue,
//!     never inline with epoll_wait. This is the deferred-callback trick
//!     that lets callbacks safely submit follow-up operations.
//!   - Level-triggered + EPOLLONESHOT — each Completion = one operation.
//!   - The Completion pointer rides in epoll_event.data.ptr; no hashmap.

const std = @import("std");
const linux = std.os.linux;

// ============================================================
// Public types
// ============================================================

pub const Operation = union(enum) {
    timer: Timer,
    connect: Connect,
    recv: Recv,
    send: Send,

    pub const Timer = struct {
        /// Nanoseconds from now. Translated to a one-shot timerfd at submit time.
        ns_from_now: u64,
        /// Internal: the timerfd we created. Closed when the op completes.
        fd: linux.fd_t = -1,
    };
    pub const Connect = struct {
        fd: linux.fd_t,
        addr: *const linux.sockaddr,
        addrlen: linux.socklen_t,
    };
    pub const Recv = struct {
        fd: linux.fd_t,
        buffer: []u8,
    };
    pub const Send = struct {
        fd: linux.fd_t,
        buffer: []const u8,
    };
};

pub const Result = union(enum) {
    timer: TimerError!void,
    connect: ConnectError!void,
    recv: RecvError!usize,
    send: SendError!usize,

    pub const TimerError = error{Unexpected};
    pub const ConnectError = error{ ConnectionRefused, NetworkUnreachable, TimedOut, Unexpected };
    pub const RecvError = error{ ConnectionReset, WouldBlock, Unexpected };
    pub const SendError = error{ BrokenPipe, ConnectionReset, WouldBlock, Unexpected };
};

pub const Callback = *const fn (
    userdata: ?*anyopaque,
    loop: *Loop,
    completion: *Completion,
    result: Result,
) void;

pub const Completion = struct {
    op: Operation,
    userdata: ?*anyopaque = null,
    callback: Callback,

    // ----- internal state (caller does not touch) -----
    state: State = .dead,
    next: ?*Completion = null,
    /// The fd we registered with epoll (may be op.fd, or an internally-created
    /// timerfd). Tracked so we can EPOLL_CTL_DEL on completion.
    registered_fd: linux.fd_t = -1,
    /// The result we'll deliver when the callback fires. Populated by
    /// the wait phase and consumed by the dispatch phase.
    pending_result: Result = .{ .timer = {} },

    pub const State = enum { dead, adding, active, completed };
};

pub const InitError = error{ SyscallFailed, OutOfMemory };
pub const RunError = error{Unexpected};

// ============================================================
// Loop
// ============================================================

pub const Loop = struct {
    epfd: linux.fd_t,
    submissions: Queue = .{},
    completed: Queue = .{},
    /// In-flight operation count. Caller can use this to decide whether
    /// to keep running.
    in_flight: usize = 0,

    pub fn init(_: std.mem.Allocator) InitError!Loop {
        const r = linux.epoll_create1(linux.EPOLL.CLOEXEC);
        if (errnoOrFd(r)) |fd| return .{ .epfd = fd };
        return error.SyscallFailed;
    }

    pub fn deinit(self: *Loop) void {
        _ = linux.close(self.epfd);
        self.* = undefined;
    }

    pub fn active(self: *const Loop) usize {
        return self.in_flight;
    }

    /// Queue a Completion for submission. Caller owns the Completion's
    /// memory and must keep it alive until the callback fires.
    pub fn submit(self: *Loop, c: *Completion) void {
        std.debug.assert(c.state == .dead);
        c.state = .adding;
        c.next = null;
        self.submissions.push(c);
        self.in_flight += 1;
    }

    /// Single non-blocking pass: drain completed callbacks, flush new
    /// submissions to epoll, peek at any ready events without blocking.
    pub fn run(self: *Loop) RunError!void {
        try self.runInner(0);
    }

    /// Block up to `ns` nanoseconds. Returns as soon as at least one
    /// completion fires, or the timeout expires, whichever comes first.
    pub fn run_for_ns(self: *Loop, ns: u63) RunError!void {
        // epoll_wait takes milliseconds; round up. 0 means non-blocking.
        const ms_i: i32 = ms: {
            if (ns == 0) break :ms 0;
            const ms_u = (ns + std.time.ns_per_ms - 1) / std.time.ns_per_ms;
            break :ms @intCast(@min(ms_u, std.math.maxInt(i32)));
        };
        try self.runInner(ms_i);
    }

    // ----- internals -----

    fn runInner(self: *Loop, timeout_ms: i32) RunError!void {
        // Step 1: dispatch any callbacks that are already queued from the
        // previous run. Done before flushing new submissions so that a
        // callback's follow-up submissions are picked up this same run.
        self.dispatchCompleted();

        // Step 2: flush submissions.
        self.flushSubmissions() catch return error.Unexpected;

        // Step 3: wait for events. If there's nothing in flight, skip
        // syscalling — epoll_wait would block forever otherwise.
        if (self.in_flight == 0) return;

        var events: [64]linux.epoll_event = undefined;
        const n = linux.epoll_wait(self.epfd, &events, events.len, timeout_ms);
        if (errnoOrUsize(n)) |count| {
            for (events[0..count]) |ev| {
                const c: *Completion = @ptrFromInt(ev.data.ptr);
                self.harvest(c);
            }
        }
        // On error (commonly EINTR), the caller can simply re-run.

        // Step 4: dispatch newly-completed callbacks.
        self.dispatchCompleted();
    }

    fn flushSubmissions(self: *Loop) !void {
        while (self.submissions.pop()) |c| {
            switch (c.op) {
                .timer => |*t| {
                    const tfd = linux.syscall2(.timerfd_create, @intFromEnum(linux.CLOCK.MONOTONIC), 0);
                    if (errnoOrFd(tfd)) |fd| {
                        t.fd = fd;
                        c.registered_fd = fd;
                    } else {
                        c.pending_result = .{ .timer = error.Unexpected };
                        self.markCompleted(c);
                        continue;
                    }
                    var spec: linux.itimerspec = .{
                        .it_interval = .{ .sec = 0, .nsec = 0 },
                        .it_value = .{
                            .sec = @intCast(t.ns_from_now / std.time.ns_per_s),
                            .nsec = @intCast(t.ns_from_now % std.time.ns_per_s),
                        },
                    };
                    const r = linux.timerfd_settime(t.fd, .{}, &spec, null);
                    if (errnoOrUsize(r) == null) {
                        _ = linux.close(t.fd);
                        c.pending_result = .{ .timer = error.Unexpected };
                        self.markCompleted(c);
                        continue;
                    }
                    try self.epollAdd(c, linux.EPOLL.IN);
                },
                .connect => |conn| {
                    c.registered_fd = conn.fd;
                    // Attempt the non-blocking connect; if it returns
                    // right away we still go through epoll for uniformity.
                    _ = linux.connect(conn.fd, conn.addr, conn.addrlen);
                    try self.epollAdd(c, linux.EPOLL.OUT);
                },
                .recv => |r| {
                    c.registered_fd = r.fd;
                    try self.epollAdd(c, linux.EPOLL.IN);
                },
                .send => |s| {
                    c.registered_fd = s.fd;
                    try self.epollAdd(c, linux.EPOLL.OUT);
                },
            }
            c.state = .active;
        }
    }

    fn epollAdd(self: *Loop, c: *Completion, events: u32) !void {
        var ev: linux.epoll_event = .{
            .events = events | linux.EPOLL.ONESHOT,
            .data = .{ .ptr = @intFromPtr(c) },
        };
        const r = linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_ADD, c.registered_fd, &ev);
        if (errnoOrUsize(r) == null) return error.SyscallFailed;
    }

    /// Called when epoll says an fd is ready. Performs the actual I/O,
    /// builds the Result, queues for callback dispatch.
    fn harvest(self: *Loop, c: *Completion) void {
        switch (c.op) {
            .timer => |t| {
                // Drain the timerfd so we don't leak readiness.
                var buf: [8]u8 = undefined;
                _ = linux.read(t.fd, &buf, buf.len);
                _ = linux.close(t.fd);
                c.pending_result = .{ .timer = {} };
            },
            .connect => |conn| {
                // SO_ERROR tells us if the connect succeeded.
                var err: i32 = 0;
                var len: linux.socklen_t = @sizeOf(i32);
                const so_r = linux.getsockopt(conn.fd, linux.SOL.SOCKET, linux.SO.ERROR, std.mem.asBytes(&err).ptr, &len);
                if (errnoOrUsize(so_r) == null) {
                    c.pending_result = .{ .connect = error.Unexpected };
                } else if (err == 0) {
                    c.pending_result = .{ .connect = {} };
                } else {
                    c.pending_result = .{ .connect = switch (err) {
                        @intFromEnum(linux.E.CONNREFUSED) => error.ConnectionRefused,
                        @intFromEnum(linux.E.NETUNREACH) => error.NetworkUnreachable,
                        @intFromEnum(linux.E.TIMEDOUT) => error.TimedOut,
                        else => error.Unexpected,
                    } };
                }
            },
            .recv => |r| {
                const n = linux.read(r.fd, r.buffer.ptr, r.buffer.len);
                c.pending_result = if (errnoOrUsize(n)) |bytes|
                    .{ .recv = bytes }
                else
                    .{ .recv = error.Unexpected };
            },
            .send => |s| {
                const n = linux.write(s.fd, s.buffer.ptr, s.buffer.len);
                c.pending_result = if (errnoOrUsize(n)) |bytes|
                    .{ .send = bytes }
                else
                    .{ .send = error.Unexpected };
            },
        }
        // EPOLLONESHOT auto-disarmed the fd. We still need to remove it
        // from the interest set to avoid EEXIST on a future re-arm.
        var ev: linux.epoll_event = .{ .events = 0, .data = .{ .ptr = 0 } };
        _ = linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_DEL, c.registered_fd, &ev);
        self.markCompleted(c);
    }

    fn markCompleted(self: *Loop, c: *Completion) void {
        c.state = .completed;
        c.next = null;
        self.completed.push(c);
    }

    /// Drain the completed queue, invoking each callback exactly once.
    /// Callbacks may submit new operations; those go on the submissions
    /// queue and get picked up next iteration.
    fn dispatchCompleted(self: *Loop) void {
        while (self.completed.pop()) |c| {
            std.debug.assert(c.state == .completed);
            c.state = .dead;
            self.in_flight -= 1;
            c.callback(c.userdata, self, c, c.pending_result);
        }
    }
};

// ============================================================
// Internal: intrusive singly-linked queue
// ============================================================

const Queue = struct {
    head: ?*Completion = null,
    tail: ?*Completion = null,

    fn push(self: *Queue, c: *Completion) void {
        c.next = null;
        if (self.tail) |t| {
            t.next = c;
        } else {
            self.head = c;
        }
        self.tail = c;
    }

    fn pop(self: *Queue) ?*Completion {
        const c = self.head orelse return null;
        self.head = c.next;
        if (self.head == null) self.tail = null;
        c.next = null;
        return c;
    }
};

// ============================================================
// Internal: errno helpers
// ============================================================

/// Success → the result. Linux errno-encoded failure → null.
fn errnoOrUsize(r: usize) ?usize {
    const signed: isize = @bitCast(r);
    if (signed >= -4095 and signed < 0) return null;
    return r;
}

fn errnoOrFd(r: usize) ?linux.fd_t {
    const signed: isize = @bitCast(r);
    if (signed >= -4095 and signed < 0) return null;
    return @intCast(signed);
}

// ============================================================
// Tests
// ============================================================

test "timer fires within deadline" {
    var loop = try Loop.init(std.testing.allocator);
    defer loop.deinit();

    const State = struct {
        fired: bool = false,
        result: ?Result = null,
    };
    var state: State = .{};

    const Cb = struct {
        fn fire(ud: ?*anyopaque, _: *Loop, _: *Completion, res: Result) void {
            const s: *State = @ptrCast(@alignCast(ud.?));
            s.fired = true;
            s.result = res;
        }
    };

    var c: Completion = .{
        .op = .{ .timer = .{ .ns_from_now = std.time.ns_per_ms } },
        .userdata = &state,
        .callback = Cb.fire,
    };
    loop.submit(&c);

    // 100 ms is generous; timer should fire within ~1 ms.
    try loop.run_for_ns(100 * std.time.ns_per_ms);
    try std.testing.expect(state.fired);
    try std.testing.expectEqual(@as(usize, 0), loop.active());
}

test "callback can submit follow-up op" {
    var loop = try Loop.init(std.testing.allocator);
    defer loop.deinit();

    const State = struct {
        first_fired: bool = false,
        second_fired: bool = false,
        second_completion: Completion = undefined,
    };
    var state: State = .{};

    const Cb = struct {
        fn second(ud: ?*anyopaque, _: *Loop, _: *Completion, _: Result) void {
            const s: *State = @ptrCast(@alignCast(ud.?));
            s.second_fired = true;
        }
        fn first(ud: ?*anyopaque, lp: *Loop, _: *Completion, _: Result) void {
            const s: *State = @ptrCast(@alignCast(ud.?));
            s.first_fired = true;
            s.second_completion = .{
                .op = .{ .timer = .{ .ns_from_now = std.time.ns_per_ms } },
                .userdata = ud,
                .callback = second,
            };
            lp.submit(&s.second_completion);
        }
    };

    var first: Completion = .{
        .op = .{ .timer = .{ .ns_from_now = std.time.ns_per_ms } },
        .userdata = &state,
        .callback = Cb.first,
    };
    loop.submit(&first);

    // Drive the loop until both timers have fired.
    var deadline: u64 = 200;
    while (loop.active() > 0 and deadline > 0) : (deadline -= 1) {
        try loop.run_for_ns(10 * std.time.ns_per_ms);
    }

    try std.testing.expect(state.first_fired);
    try std.testing.expect(state.second_fired);
}

test "two concurrent timers both fire" {
    var loop = try Loop.init(std.testing.allocator);
    defer loop.deinit();

    const Cb = struct {
        fn fire(ud: ?*anyopaque, _: *Loop, _: *Completion, _: Result) void {
            const counter: *u8 = @ptrCast(@alignCast(ud.?));
            counter.* += 1;
        }
    };
    var counter: u8 = 0;
    var c1: Completion = .{
        .op = .{ .timer = .{ .ns_from_now = std.time.ns_per_ms } },
        .userdata = &counter,
        .callback = Cb.fire,
    };
    var c2: Completion = .{
        .op = .{ .timer = .{ .ns_from_now = 5 * std.time.ns_per_ms } },
        .userdata = &counter,
        .callback = Cb.fire,
    };
    loop.submit(&c1);
    loop.submit(&c2);

    var iters: u64 = 200;
    while (loop.active() > 0 and iters > 0) : (iters -= 1) {
        try loop.run_for_ns(10 * std.time.ns_per_ms);
    }
    try std.testing.expectEqual(@as(u8, 2), counter);
    try std.testing.expectEqual(@as(usize, 0), loop.active());
}
