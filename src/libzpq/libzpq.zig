//! libzpq: zpq's JSON request handler (the Lambda's request and response schema) behind a C ABI. See include/zpq.h.
//!
//! A context holds what outlives one request: the allocator, the `std.Io` executor, the S3 connection pool and footer
//! cache, and the ceiling on its queries' declared memory budgets. Each `zpq_run` reads the process environment at
//! call time, as the CLI does at start, for S3 credentials, endpoint and region. Process-wide state outside any
//! context is limited to caches that are set once and then only read: the TLS client contexts (CA stores), and probes
//! of the kernel's io_uring and of the gzip backend.

const std = @import("std");
const zpq = @import("zpq");
const service = @import("service");
const Budget = @import("budget.zig").Budget;
const c = std.c;

/// malloc and free, so a host's leak checker (ASan, valgrind) sees every allocation.
const gpa = std.heap.c_allocator;

pub const std_options: std.Options = .{ .signal_stack_size = zpq.core.spawn.signal_stack_size };

/// The status codes of include/zpq.h. Stable: a value is never reused for another meaning.
pub const Status = enum(c_int) {
    ok = 0,
    einval = -1,
    enomem = -2,
    eclosed = -3,
    einternal = -4,
};

/// Every `std.Io.Threaded` installs do-nothing SIGIO and SIGPIPE handlers at init and restores what it found at
/// deinit, so contexts whose lives overlap undid each other: closing one put back the host's default SIGPIPE under
/// another still running. The process holds one count instead: the first open keeps the dispositions the host had,
/// the last close restores them, unless the host installed its own meanwhile. zpq's own sockets never raise SIGPIPE
/// whatever the disposition (`tls.sendNoSignal`).
const signals = struct {
    const posix = std.posix;
    const Saved = if (posix.Sigaction == void) void else posix.Sigaction;
    var lock: c.pthread_mutex_t = .{};
    var count: usize = 0;
    var host_io: Saved = undefined;
    var host_pipe: Saved = undefined;
    var ours: Saved = undefined;

    fn adopt(t: std.Io.Threaded) std.Io.Threaded {
        var out = t;
        if (Saved == void or !out.have_signal_handler) return out;
        _ = c.pthread_mutex_lock(&lock);
        defer _ = c.pthread_mutex_unlock(&lock);
        if (count == 0) {
            if (@TypeOf(out.old_sig_io) != void) host_io = out.old_sig_io;
            if (@TypeOf(out.old_sig_pipe) != void) host_pipe = out.old_sig_pipe;
            posix.sigaction(.PIPE, null, &ours);
        }
        count += 1;
        out.have_signal_handler = false;
        return out;
    }

    fn release() void {
        if (Saved == void) return;
        _ = c.pthread_mutex_lock(&lock);
        defer _ = c.pthread_mutex_unlock(&lock);
        if (count == 0) return;
        count -= 1;
        if (count != 0) return;
        if (@TypeOf(std.Io.Threaded.init_single_threaded.old_sig_io) != void) restore(.IO, &host_io);
        if (@TypeOf(std.Io.Threaded.init_single_threaded.old_sig_pipe) != void) restore(.PIPE, &host_pipe);
    }

    fn restore(sig: posix.SIG, host: *const posix.Sigaction) void {
        var cur: posix.Sigaction = undefined;
        posix.sigaction(sig, null, &cur);
        if (cur.handler.handler == ours.handler.handler) posix.sigaction(sig, host, null);
    }
};

pub const Ctx = struct {
    threaded: std.Io.Threaded,
    pool: service.PersistentPool,
    /// The ceiling every query's budget is reserved from.
    budget: Budget,
    /// How long a query waits for its budget when its request does not say; null: as long as it takes.
    budget_wait_ms: ?u64,
    /// Guards `inflight` and `closing`.
    lock: c.pthread_mutex_t = .{},
    cond: c.pthread_cond_t = .{},
    /// `zpq_run` calls between their admission and their return.
    inflight: usize = 0,
    runners: Runners = .{},
    /// Set by `zpq_shutdown` (and so by `zpq_close`): new calls are refused with ZPQ_ECLOSED.
    closing: bool = false,
    /// Reserved for the ETag-validated object cache (F5). It will charge its bytes to `budget` as evictable,
    /// register itself as `budget.reclaimer` so a waiting query can evict before it blocks, and be freed in
    /// `zpq_close` after the queries drain. Nothing lives here yet.
    object_cache: void = {},

    fn admission(self: *Ctx) service.Admission {
        return .{ .ctx = self, .acquireFn = admit, .releaseFn = release, .ceiling = self.budget.ceiling };
    }

    fn admit(ptr: *anyopaque, bytes: usize, wait_ms: ?u64) service.Admission.Refusal!void {
        const self: *Ctx = @ptrCast(@alignCast(ptr));
        return self.budget.acquire(bytes, wait_ms orelse self.budget_wait_ms);
    }

    fn release(ptr: *anyopaque, bytes: usize) void {
        const self: *Ctx = @ptrCast(@alignCast(ptr));
        self.budget.release(bytes);
    }

    /// Count a call in, unless the context is closing.
    fn enter(self: *Ctx) bool {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        if (self.closing) return false;
        self.inflight += 1;
        return true;
    }

    /// Refuse new calls and wake the queries waiting for their budget, which return ZPQ_ECLOSED. Running queries go
    /// on. The context stays valid, so a host can drain its own calls before `zpq_close`. Idempotent.
    fn shutdown(self: *Ctx) void {
        _ = c.pthread_mutex_lock(&self.lock);
        self.closing = true;
        _ = c.pthread_mutex_unlock(&self.lock);
        self.budget.close();
    }

    fn leave(self: *Ctx) void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        self.inflight -= 1;
        if (self.inflight == 0) _ = c.pthread_cond_broadcast(&self.cond);
    }
};

/// The options `zpq_open` takes. Unknown fields are an error: a misspelt option should not be silently ignored.
const Options = struct {
    /// The context's ceiling on its queries' declared budgets: bytes, or a size string ("2GB"). Absent: the memory
    /// available, the CLI's default, so one query alone gets what `zpq query` would.
    max_memory: ?std.json.Value = null,
    /// How long a query waits for its budget when its request names no `budget_wait_ms`. Absent: as long as it takes.
    budget_wait_ms: ?std.json.Value = null,
    /// Open connections to the S3 endpoint as the context opens (`ZPQ_S3_PRECONNECT` of them, default 4), as the
    /// Lambda does during init, so the first queries find them ready. Off by default.
    preconnect: bool = false,
};

/// `Options` as a context uses them.
const Parsed = struct {
    ceiling: ?usize = null,
    budget_wait_ms: ?u64 = null,
    preconnect: bool = false,
};

fn parseOptions(text: []const u8) !Parsed {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return .{};
    const parsed = try std.json.parseFromSlice(Options, gpa, trimmed, .{});
    defer parsed.deinit();
    const ceiling: ?usize = if (parsed.value.max_memory) |v| switch (v) {
        .null => null,
        .integer => |n| if (n > 0) @intCast(n) else return error.BadMaxMemory,
        // "0MB" is the 0 the integer form refuses: a ceiling no query fits under.
        .string => |str| switch (try zpq.core.system.parseSizeString(str)) {
            0 => return error.BadMaxMemory,
            else => |n| n,
        },
        else => return error.BadMaxMemory,
    } else null;
    const wait: ?u64 = if (parsed.value.budget_wait_ms) |v| switch (v) {
        .null => null,
        .integer => |n| if (n >= 0) @intCast(n) else return error.BadBudgetWait,
        else => return error.BadBudgetWait,
    } else null;
    return .{ .ceiling = ceiling, .budget_wait_ms = wait, .preconnect = parsed.value.preconnect };
}

export fn zpq_open(options_json: ?[*:0]const u8, out_ctx: ?*?*Ctx, out_error: ?*?[*:0]u8) c_int {
    if (out_error) |e| e.* = null;
    if (out_ctx) |o| o.* = null;
    const out = out_ctx orelse return fail(out_error, .einval, "NullOutContext");
    const opts = parseOptions(if (options_json) |o| std.mem.span(o) else "") catch |err| return switch (err) {
        error.OutOfMemory => fail(out_error, .enomem, @errorName(err)),
        else => fail(out_error, .einval, @errorName(err)),
    };
    const env = processEnviron();
    const ceiling = opts.ceiling orelse zpq.core.system.discoverAvailableMemory(env);
    const ctx = gpa.create(Ctx) catch return fail(out_error, .enomem, "OutOfMemory");
    ctx.* = .{
        .threaded = signals.adopt(std.Io.Threaded.init(gpa, .{})),
        .pool = .sized(ceiling),
        .budget = .init(ceiling),
        .budget_wait_ms = opts.budget_wait_ms,
    };
    ctx.pool.per_call_stats = false;
    // A request that names no max_memory takes the whole ceiling, as the CLI takes all it may.
    ctx.pool.query_memory = ceiling;
    ctx.pool.admission = ctx.admission();
    // Ready before any call: every query, the first ones included, gets the footer cache.
    ctx.pool.warm(gpa) catch |err| {
        ctx.threaded.deinit();
        signals.release();
        gpa.destroy(ctx);
        const status: Status = if (err == error.OutOfMemory) .enomem else .einternal;
        return fail(out_error, status, @errorName(err));
    };
    // Only when asked: a host that never touches S3 should not open connections to it. zpq_close joins them.
    if (opts.preconnect) service.preconnectS3(ctx.threaded.io(), gpa, env, &ctx.pool);
    out.* = ctx;
    return @backingInt(Status.ok);
}

export fn zpq_run(
    ctx: ?*Ctx,
    request_json: ?[*]const u8,
    len: usize,
    out_json: ?*?[*:0]u8,
    out_len: ?*usize,
) c_int {
    // Every output the caller gave is cleared first, so no return leaves a stale one behind.
    if (out_json) |o| o.* = null;
    if (out_len) |l| l.* = 0;
    const self = ctx orelse return fail(out_json, .einval, "NullContext");
    if (out_json == null) return @backingInt(Status.einval);
    if (len > std.math.maxInt(isize)) return fail(out_json, .einval, "BadLength");
    const request = request_json orelse if (len == 0) "" else return fail(out_json, .einval, "NullRequest");
    const body: []const u8 = request[0..len];
    if (!self.enter()) return fail(out_json, .eclosed, "Closed");
    defer self.leave();

    var call: Call = .{ .ctx = self, .body = body };
    if (stackLeft() >= inline_stack) call.run() else {
        self.runners.run(&call) catch return fail(out_json, .einternal, "ThreadSpawnFailed");
    }
    const response = call.result catch |err|
        return switch (err) {
            error.OutOfMemory => @backingInt(Status.enomem),
            error.ContextClosed => fail(out_json, .eclosed, "Closed"),
            else => fail(out_json, .einternal, @errorName(err)),
        };
    // The response is already the C heap's (`gpa` is `c_allocator`): grown by its NUL and handed over, not copied,
    // so a large answer is not held twice.
    const buf: [*]u8 = @ptrCast(c.realloc(response.ptr, response.len + 1) orelse {
        gpa.free(response);
        return @backingInt(Status.enomem);
    });
    buf[response.len] = 0;
    out_json.?.* = buf[0..response.len :0].ptr;
    if (out_len) |l| l.* = response.len;
    return @backingInt(Status.ok);
}

/// The stack a request runs on: the CLI's main-thread default, which the parser depth caps (`MAX_GROUP_DEPTH`,
/// `ExpressionTooDeep`) are sized against. Reserved, not committed, so an idle page costs nothing.
const request_stack_size = 8 << 20;

/// Stack a caller's thread must have left for a request to run on it: well past what the depth caps let a request
/// reach (a filter nested to its cap takes about 256 KiB). A thread with less hands the request to `Runners`.
const inline_stack = 4 << 20;

/// The lowest address of this thread's stack, found once per thread (glibc reads /proc/self/maps for the main one);
/// 0 where the platform does not say, which sends every request to `Runners`.
threadlocal var stack_low: usize = 0;
threadlocal var stack_known = false;

extern "c" fn pthread_getattr_np(thread: c.pthread_t, attr: *c.pthread_attr_t) c_int;
extern "c" fn pthread_attr_getstack(attr: *const c.pthread_attr_t, addr: *?*anyopaque, size: *usize) c_int;
extern "c" fn pthread_attr_destroy(attr: *c.pthread_attr_t) c_int;

/// Bytes of stack left below the caller's frame.
fn stackLeft() usize {
    if (@import("builtin").os.tag != .linux) return 0;
    if (!stack_known) {
        stack_known = true;
        var attr: c.pthread_attr_t = undefined;
        if (pthread_getattr_np(c.pthread_self(), &attr) == 0) {
            defer _ = pthread_attr_destroy(&attr);
            var addr: ?*anyopaque = null;
            var size: usize = 0;
            if (pthread_attr_getstack(&attr, &addr, &size) == 0) stack_low = @intFromPtr(addr);
        }
    }
    if (stack_low == 0) return 0;
    return @frameAddress() -| stack_low;
}

/// One request, run on a thread of zpq's own rather than the caller's: a host thread's stack can be far smaller (musl
/// gives 128 KiB, a JVM 512 KiB), and a filter nested within the depth cap overflowed it and killed the process.
const Call = struct {
    ctx: *Ctx,
    body: []const u8,
    result: @typeInfo(@TypeOf(service.handle)).@"fn".return_type.? = undefined,
    next: ?*Call = null,
    finished: bool = false,

    fn run(call: *Call) void {
        call.result = service.handle(call.ctx.threaded.io(), gpa, processEnviron(), call.body, &call.ctx.pool, null);
    }
};

/// A context's request threads, kept between calls: spawning one per call cost more than a cached count(*) does. A
/// call goes to an idle thread, or a new one when none is; concurrent calls run concurrently, as on the callers'
/// threads they did. The threads end at `zpq_close`, after every call has returned.
const Runners = struct {
    lock: c.pthread_mutex_t = .{},
    /// A call is queued, or the context is closing.
    work: c.pthread_cond_t = .{},
    /// A call finished.
    done: c.pthread_cond_t = .{},
    queue: ?*Call = null,
    /// Threads waiting for work, and how many of them have been signalled one call each.
    idle: usize = 0,
    signalled: usize = 0,
    threads: std.ArrayList(std.Thread) = .empty,
    closing: bool = false,

    fn run(self: *Runners, call: *Call) error{ThreadSpawnFailed}!void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        call.next = self.queue;
        self.queue = call;
        if (self.idle > self.signalled) {
            self.signalled += 1;
            _ = c.pthread_cond_signal(&self.work);
        } else spawn: {
            self.threads.ensureUnusedCapacity(gpa, 1) catch break :spawn;
            const t = zpq.core.spawn.spawn(.{ .stack_size = request_stack_size }, loop, .{self}) catch break :spawn;
            self.threads.appendAssumeCapacity(t);
        }
        // Without a thread to take it, a call is withdrawn rather than left to wait for one.
        if (self.threads.items.len == 0) {
            self.queue = call.next;
            return error.ThreadSpawnFailed;
        }
        while (!call.finished) _ = c.pthread_cond_wait(&self.done, &self.lock);
    }

    fn loop(self: *Runners) void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        while (true) {
            while (self.queue == null and !self.closing) {
                self.idle += 1;
                _ = c.pthread_cond_wait(&self.work, &self.lock);
                self.idle -= 1;
                if (self.signalled > 0) self.signalled -= 1;
            }
            const call = self.queue orelse return;
            self.queue = call.next;
            _ = c.pthread_mutex_unlock(&self.lock);
            call.run();
            _ = c.pthread_mutex_lock(&self.lock);
            call.finished = true;
            _ = c.pthread_cond_broadcast(&self.done);
        }
    }

    /// Ends every thread; no call may be running or start.
    fn stop(self: *Runners) void {
        _ = c.pthread_mutex_lock(&self.lock);
        self.closing = true;
        _ = c.pthread_cond_broadcast(&self.work);
        _ = c.pthread_mutex_unlock(&self.lock);
        for (self.threads.items) |t| t.join();
        self.threads.deinit(gpa);
    }
};

/// Return `status`, with `{"error":"<kind>","reason":"<reason>"}` in `*out` when the caller gave one and it can be
/// allocated. ENOMEM carries none: there may be no memory to describe it with.
fn fail(out: ?*?[*:0]u8, status: Status, reason: []const u8) c_int {
    const o = out orelse return @backingInt(status);
    const kind = switch (status) {
        .einval => "invalid_argument",
        .eclosed => "closed",
        .einternal => "internal",
        .ok, .enomem => return @backingInt(status),
    };
    const text = std.fmt.allocPrintSentinel(gpa, "{{\"error\":\"{s}\",\"reason\":\"{s}\"}}", .{ kind, reason }, 0) catch
        return @backingInt(status);
    o.* = text.ptr;
    return @backingInt(status);
}

export fn zpq_free(json: ?[*:0]u8) void {
    c.free(json);
}

export fn zpq_shutdown(ctx: ?*Ctx) void {
    (ctx orelse return).shutdown();
}

export fn zpq_close(ctx: ?*Ctx) void {
    const self = ctx orelse return;
    // Queries waiting for their budget leave first, with ZPQ_ECLOSED; then the running ones finish.
    self.shutdown();
    _ = c.pthread_mutex_lock(&self.lock);
    while (self.inflight > 0) _ = c.pthread_cond_wait(&self.cond, &self.lock);
    _ = c.pthread_mutex_unlock(&self.lock);
    self.runners.stop();
    self.pool.deinit();
    self.threaded.deinit();
    signals.release();
    gpa.destroy(self);
}

/// libzpq's ABI version: bumped when a signature or the meaning of a status code changes.
export fn zpq_abi_version() c_int {
    return 1;
}

export fn zpq_strerror(status: c_int) [*:0]const u8 {
    const s = std.enums.fromInt(Status, status) orelse return "unknown status";
    return switch (s) {
        .ok => "ok",
        .einval => "invalid argument",
        .enomem => "out of memory",
        .eclosed => "context is closed",
        .einternal => "internal failure",
    };
}

/// The process environment as it is now (libc's `environ`). After `clearenv()` glibc leaves `environ` NULL, which
/// std declares as never null: read it through an optional instead, and treat NULL as an empty environment.
fn processEnviron() std.process.Environ {
    const environ_ptr = @extern(*const ?[*:null]const ?[*:0]const u8, .{ .name = "environ" });
    const live = environ_ptr.* orelse return .{ .block = .{ .slice = &no_environ } };
    return .{ .block = .{ .slice = std.mem.span(live) } };
}

const no_environ = [_:null]?[*:0]const u8{};

const testing = std.testing;

test {
    _ = @import("budget.zig");
}

test "options: a ceiling, a wait, nothing, or an error" {
    try testing.expectEqual(Parsed{}, try parseOptions(""));
    try testing.expectEqual(Parsed{}, try parseOptions("{}"));
    try testing.expectEqual(@as(?usize, 1 << 20), (try parseOptions("{\"max_memory\": 1048576}")).ceiling);
    try testing.expect((try parseOptions("{\"max_memory\": \"2GB\"}")).ceiling.? > 1 << 30);
    try testing.expectEqual(@as(?u64, 250), (try parseOptions("{\"budget_wait_ms\": 250}")).budget_wait_ms);
    try testing.expectError(error.UnknownField, parseOptions("{\"max_mem\": 1}"));
    try testing.expectError(error.BadMaxMemory, parseOptions("{\"max_memory\": -1}"));
    try testing.expectError(error.BadBudgetWait, parseOptions("{\"budget_wait_ms\": -1}"));
    try testing.expectError(error.BadBudgetWait, parseOptions("{\"budget_wait_ms\": \"1s\"}"));
}

test "options: pre-connect is off unless asked, and only a boolean" {
    try testing.expect(!(try parseOptions("{\"max_memory\": 1048576}")).preconnect);
    try testing.expect((try parseOptions("{\"preconnect\": true}")).preconnect);
    try testing.expect(!(try parseOptions("{\"preconnect\": false}")).preconnect);
    try testing.expectError(error.UnexpectedToken, parseOptions("{\"preconnect\": 4}"));
}

test "every status has a fixed message, and an unknown one says so" {
    try testing.expectEqualStrings("ok", std.mem.span(zpq_strerror(0)));
    try testing.expectEqualStrings("context is closed", std.mem.span(zpq_strerror(-3)));
    try testing.expectEqualStrings("internal failure", std.mem.span(zpq_strerror(-4)));
    try testing.expectEqualStrings("unknown status", std.mem.span(zpq_strerror(7)));
}

test "zpq_open refuses bad options with a reason, and a NULL out_ctx" {
    var ctx: ?*Ctx = undefined;
    var err: ?[*:0]u8 = undefined;
    try testing.expectEqual(@as(c_int, -1), zpq_open("{\"max_mem\": 1}", &ctx, &err));
    try testing.expectEqual(@as(?*Ctx, null), ctx);
    defer zpq_free(err);
    try testing.expectEqualStrings("{\"error\":\"invalid_argument\",\"reason\":\"UnknownField\"}", std.mem.span(err.?));
    // No error wanted: the status alone.
    try testing.expectEqual(@as(c_int, -1), zpq_open("{\"max_memory\": 0}", &ctx, null));
    // Written as a size, the same 0 opened a context no query could run in.
    try testing.expectEqual(@as(c_int, -1), zpq_open("{\"max_memory\": \"0MB\"}", &ctx, null));
    try testing.expectEqual(@as(c_int, -1), zpq_open(null, null, null));
}

test "queries whose budgets add up past the ceiling all answer, and never hold more than it" {
    const fixture = "ci/fixtures/parquet/mixed_codec.parquet";
    std.Io.Dir.cwd().access(testing.io, fixture, .{}) catch return error.SkipZigTest;
    var ctx: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open("{\"max_memory\": \"64MB\"}", &ctx, null));
    defer zpq_close(ctx);
    const Run = struct {
        fn one(context: *Ctx, ok: *std.atomic.Value(u32)) void {
            const req = "{\"inputs\":[\"" ++ fixture ++ "\"],\"aggregate\":\"count(*) AS n\"," ++
                "\"max_memory\":\"40MB\"}";
            var out: ?[*:0]u8 = null;
            if (zpq_run(context, req, req.len, &out, null) != 0) return;
            defer zpq_free(out);
            const answered = std.mem.indexOf(u8, std.mem.span(out.?), "\"agg\":{\"n\":48000}") != null;
            if (answered) _ = ok.fetchAdd(1, .monotonic);
        }
    };
    var ok = std.atomic.Value(u32).init(0);
    var threads: [12]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Run.one, .{ ctx.?, &ok });
    for (threads) |t| t.join();
    try testing.expectEqual(@as(u32, threads.len), ok.load(.monotonic));
    // 40 MB each against 64 MB: one at a time, the peak one budget, and all of it back.
    try testing.expectEqual(@as(usize, 40 << 20), ctx.?.budget.peak);
    try testing.expectEqual(@as(usize, 0), ctx.?.budget.reserved);
}

test "a request's max_memory of 0 is refused, as the context's is" {
    var ctx: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open("{\"max_memory\": \"64MB\"}", &ctx, null));
    defer zpq_close(ctx);
    for ([_][]const u8{ "0", "\"0MB\"" }) |zero| {
        var buf: [160]u8 = undefined;
        const req = try std.fmt.bufPrint(&buf, "{{\"inputs\":[\"x.parquet\"],\"aggregate\":\"count(*) AS n\"," ++
            "\"max_memory\":{s}}}", .{zero});
        var out: ?[*:0]u8 = null;
        try testing.expectEqual(@as(c_int, 0), zpq_run(ctx, req.ptr, req.len, &out, null));
        defer zpq_free(out);
        try testing.expectEqualStrings(
            "{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"max_memory\"}",
            std.mem.span(out.?),
        );
    }
}

extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern "c" fn unsetenv(name: [*:0]const u8) c_int;

test "a context opens connections to the S3 endpoint only when asked to pre-connect" {
    const posix = std.posix;
    // A listener for the S3 endpoint: the kernel completes connections to it without an accept.
    const rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
    const lfd: posix.fd_t = @intCast(rc);
    defer _ = posix.system.close(lfd);
    var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    if (posix.errno(posix.system.bind(lfd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
    if (posix.errno(posix.system.listen(lfd, 16)) != .SUCCESS) return error.SkipZigTest;
    if (posix.errno(posix.system.getsockname(lfd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;

    var endpoint: [64]u8 = undefined;
    const port = std.mem.bigToNative(u16, addr.port);
    const url = try std.fmt.bufPrintSentinel(&endpoint, "http://127.0.0.1:{d}", .{port}, 0);
    const vars = [_][2][*:0]const u8{
        .{ "S3_ENDPOINT_URL", url.ptr }, .{ "AWS_ACCESS_KEY_ID", "ak" }, .{ "AWS_SECRET_ACCESS_KEY", "sk" },
        .{ "AWS_REGION", "us-east-1" },  .{ "ZPQ_S3_PRECONNECT", "2" },
    };
    for (vars) |v| _ = setenv(v[0], v[1], 1);
    defer for (vars) |v| {
        _ = unsetenv(v[0]);
    };

    var plain: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &plain, null));
    try testing.expectEqual(@as(?u64, null), plain.?.pool.preconnect_ns);
    try testing.expectEqual(@as(u64, 0), plain.?.pool.inner.preconnect_parked.load(.monotonic));
    // Nothing reached the listener: no connection waits to be accepted.
    var pfd = [_]posix.pollfd{.{ .fd = lfd, .events = posix.POLL.IN, .revents = 0 }};
    try testing.expect(posix.system.poll(&pfd, 1, 50) == 0);
    zpq_close(plain);

    var warm: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open("{\"preconnect\": true}", &warm, null));
    defer zpq_close(warm);
    try testing.expect(warm.?.pool.preconnect_ns != null);
    // Both connections are parked within the pre-connect's own deadline.
    var waited_ms: u32 = 0;
    while (warm.?.pool.inner.preconnect_parked.load(.monotonic) < 2 and waited_ms < 2_000) : (waited_ms += 10) {
        const ts: posix.timespec = .{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
        _ = posix.system.nanosleep(&ts, null);
    }
    try testing.expectEqual(@as(u64, 2), warm.?.pool.inner.preconnect_parked.load(.monotonic));
}

test "a context opens and answers after the host clears its environment" {
    const environ_ptr = @extern(*?[*:null]const ?[*:0]const u8, .{ .name = "environ" });
    const saved = environ_ptr.*;
    defer environ_ptr.* = saved;
    environ_ptr.* = null; // what clearenv() leaves
    try testing.expectEqual(@as(usize, 0), processEnviron().block.slice.len);
    var ctx: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &ctx, null));
    defer zpq_close(ctx);
    const req = "{\"inputs\":[\"no/such.parquet\"],\"aggregate\":\"count(*) AS n\"}";
    var out: ?[*:0]u8 = null;
    try testing.expectEqual(@as(c_int, 0), zpq_run(ctx, req, req.len, &out, null));
    defer zpq_free(out);
    try testing.expect(std.mem.startsWith(u8, std.mem.span(out.?), "{\"error\""));
}

test "regression: contexts that overlap leave SIGPIPE as the host had it, and keep a handler it installs" {
    const posix = std.posix;
    if (posix.Sigaction == void) return error.SkipZigTest;
    var host: posix.Sigaction = undefined;
    posix.sigaction(.PIPE, null, &host);
    defer posix.sigaction(.PIPE, &host, null);
    const dfl: posix.Sigaction = .{
        .handler = .{ .handler = posix.SIG.DFL },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(.PIPE, &dfl, null);
    var a: ?*Ctx = null;
    var b: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &a, null));
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &b, null));
    zpq_close(a);
    // B still runs: closing A restored nothing (it reset SIGPIPE to the default, which a peer reset then killed on).
    var cur: posix.Sigaction = undefined;
    posix.sigaction(.PIPE, null, &cur);
    try testing.expect(cur.handler.handler != posix.SIG.DFL);
    zpq_close(b);
    posix.sigaction(.PIPE, null, &cur);
    try testing.expect(cur.handler.handler == posix.SIG.DFL);

    // A handler the host installs while a context is open stays after the last close.
    const Host = struct {
        fn on(_: posix.SIG) callconv(.c) void {}
    };
    const mine: posix.Sigaction = .{ .handler = .{ .handler = Host.on }, .mask = posix.sigemptyset(), .flags = 0 };
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &a, null));
    posix.sigaction(.PIPE, &mine, null);
    zpq_close(a);
    posix.sigaction(.PIPE, null, &cur);
    try testing.expect(cur.handler.handler == mine.handler.handler);
}

test "regression: a filter nested near the depth cap runs from a host thread with a small stack" {
    var ctx: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &ctx, null));
    defer zpq_close(ctx);
    var buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try w.writeAll("{\"inputs\":[\"ci/fixtures/parquet/nullable_mix.parquet\"],");
    try w.writeAll("\"aggregate\":\"count(*) AS n\",\"filter\":\"");
    for (0..31) |_| try w.writeByte('(');
    try w.writeAll("id > 0");
    for (0..31) |_| try w.writeByte(')');
    try w.writeAll("\"}");
    const Run = struct {
        fn call(context: ?*Ctx, req: []const u8, status: *c_int, out: *?[*:0]u8) void {
            status.* = zpq_run(context, req.ptr, req.len, out, null);
        }
    };
    var status: c_int = -1;
    var out: ?[*:0]u8 = null;
    // 192 KiB of usable stack (beside the signal stack a safe build keeps in it): at about 8 KiB a level, this nesting
    // overflowed it before requests ran on zpq's own.
    const host_stack = (zpq.core.spawn.signal_stack_size orelse 0) + (192 << 10);
    const t = try std.Thread.spawn(.{ .stack_size = host_stack }, Run.call, .{ ctx, w.buffered(), &status, &out });
    t.join();
    defer zpq_free(out);
    try testing.expectEqual(@as(c_int, 0), status);
    try testing.expect(out != null);
}
