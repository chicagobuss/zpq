//! Process-wide credits for explicit local reads. Nodes live in the files that hold them; no global heap or daemon.
const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const local_fs = @import("../local_fs.zig");

var mutex: std.atomic.Mutex = .unlocked;
var files: ?*Admission = null;
var generation: std.atomic.Value(u32) = .init(0);
var waiters: std.atomic.Value(u32) = .init(0);

fn lock() void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub const Admission = struct {
    device: u64,
    serial: bool,
    block: u64,
    limit: u64,
    next: ?*Admission = null,
    held: u64 = 0,

    pub fn discover(fd: i32) Admission {
        const page = std.heap.pageSize();
        // VM_READAHEAD_PAGES: the kernel fallback when a virtual/network filesystem has no block queue.
        var result: Admission = .{ .device = 0, .serial = true, .block = 128 << 10, .limit = 128 << 10 };
        if (builtin.os.tag != .linux) return result;
        var st: linux.Statx = undefined;
        if (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{}, &st)) != .SUCCESS) return result;
        // Partitions share a physical queue; unresolved filesystems retain their own st_dev identity.
        const physical = physicalQueue(st.dev_major, st.dev_minor);
        result.device = physical orelse (@as(u64, st.dev_major) << 32 | st.dev_minor);
        lock();
        var current = files;
        while (current) |node| : (current = node.next) if (result.device != 0 and node.device == result.device) {
            result.serial = node.serial;
            result.block = node.block;
            result.limit = node.limit;
            mutex.unlock();
            return result;
        };
        mutex.unlock();
        const dev: [2]u32 = .{ st.dev_major, st.dev_minor };
        const ra = (local_fs.deviceQueue(dev, "read_ahead_kb") orelse 128) *| 1024;
        const sectors = local_fs.deviceQueue(dev, "max_sectors_kb");
        const requests = local_fs.deviceQueue(dev, "nr_requests");
        const rotational = if (physical != null) local_fs.deviceQueue(dev, "rotational") else null;
        const max_read = std.mem.alignBackward(u64, std.math.maxInt(i32), page);
        const request = (sectors orelse 0) *| 1024;
        result.block = @min(max_read, std.mem.alignForward(u64, @max(@max(ra, request), page), page));
        result.serial = rotational == null or rotational.? != 0 or sectors == null or requests == null;
        result.limit = if (result.serial) result.block else @max(result.block, sectors.? *| 1024 *| requests.?);
        return result;
    }

    pub fn register(self: *Admission) void {
        lock();
        defer mutex.unlock();
        self.next = files;
        files = self;
    }

    pub fn unregister(self: *Admission) void {
        lock();
        var at = &files;
        while (at.*) |node| {
            if (node == self) {
                at.* = node.next;
                break;
            }
            at = &node.next;
        }
        self.held = 0;
        mutex.unlock();
        wake();
    }

    /// Reserve active I/O only: retaining an idle device while waiting on another permits ABBA deadlock.
    pub fn acquire(self: *Admission, bytes: u64, observed: *u32) bool {
        lock();
        defer mutex.unlock();
        observed.* = generation.load(.seq_cst);
        var held: u64 = 0;
        var serial = self.serial;
        var limit = self.limit;
        var current = files;
        while (current) |node| : (current = node.next) if (node.device == self.device) {
            held +|= node.held;
            serial = serial or node.serial;
            limit = @min(limit, node.limit);
        };
        if (serial and held != 0) return false;
        // A single aligned unit always progresses, even if a device advertises less than its readahead.
        if (held != 0 and bytes > limit -| held) return false;
        self.held += bytes;
        return true;
    }

    pub fn complete(self: *Admission, bytes: u64) void {
        lock();
        std.debug.assert(self.held >= bytes);
        self.held -= bytes;
        mutex.unlock();
        wake();
    }
};

pub fn park(observed: u32, cancel: *const std.atomic.Value(bool)) void {
    _ = waiters.fetchAdd(1, .seq_cst);
    defer _ = waiters.fetchSub(1, .seq_cst);
    if (cancel.load(.acquire)) return;
    _ = linux.futex_4arg(&generation.raw, .{ .cmd = .WAIT, .private = true }, observed, null);
}

pub fn wake() void {
    // Order publication against waiter registration before taking the no-waiters shortcut, including on ARM.
    _ = generation.fetchAdd(1, .seq_cst);
    if (waiters.load(.seq_cst) != 0)
        _ = linux.futex_3arg(&generation.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
}

fn physicalQueue(major: u32, minor: u32) ?u64 {
    var path: [std.fs.max_path_bytes]u8 = undefined;
    var data: [64]u8 = undefined;
    for ([_][]const u8{ ".", ".." }) |dir| {
        const device = std.fmt.bufPrintSentinel(&path, "/sys/dev/block/{d}:{d}/{s}/device", .{ major, minor, dir }, 0) catch return null;
        const physical = std.posix.openat(std.posix.AT.FDCWD, device, .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true }, 0) catch continue;
        _ = linux.close(physical);
        const dev = std.fmt.bufPrintSentinel(&path, "/sys/dev/block/{d}:{d}/{s}/dev", .{ major, minor, dir }, 0) catch return null;
        const fd = std.posix.openat(std.posix.AT.FDCWD, dev, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0) catch continue;
        defer _ = linux.close(fd);
        const n = std.posix.read(fd, &data) catch continue;
        var parts = std.mem.splitScalar(u8, std.mem.trim(u8, data[0..n], " \t\r\n"), ':');
        const parent_major = std.fmt.parseInt(u32, parts.next() orelse return null, 10) catch return null;
        const parent_minor = std.fmt.parseInt(u32, parts.next() orelse return null, 10) catch return null;
        return @as(u64, parent_major) << 32 | parent_minor;
    }
    return null;
}

test "nonrotating device byte credits span files and return at completion" {
    var a: Admission = .{ .device = 3, .serial = false, .block = 4096, .limit = 8192 };
    var b: Admission = .{ .device = 3, .serial = false, .block = 4096, .limit = 8192 };
    a.register();
    b.register();
    defer b.unregister();
    var observed: u32 = 0;
    const held = a.acquire(4096, &observed);
    defer a.unregister();
    try std.testing.expect(held);
    try std.testing.expect(b.acquire(4096, &observed));
    try std.testing.expect(!a.acquire(4096, &observed));
    b.complete(4096);
    a.complete(4096);
    try std.testing.expect(b.acquire(8192, &observed));
    try std.testing.expect(!a.acquire(4096, &observed));
    b.complete(8192);
}

test "a tmpfs device does not throttle an unrelated nonrotating queue" {
    if (builtin.os.tag != .linux) return;
    const rc = linux.memfd_create("zpq-admission-test", 0);
    try std.testing.expect(linux.errno(rc) == .SUCCESS);
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    var unknown = Admission.discover(fd);
    var nvme: Admission = .{ .device = 4 << 32, .serial = false, .block = 4096, .limit = 8192 };
    unknown.register();
    defer unknown.unregister();
    nvme.register();
    defer nvme.unregister();
    var observed: u32 = 0;
    try std.testing.expect(unknown.serial);
    try std.testing.expect(unknown.acquire(4096, &observed));
    try std.testing.expect(nvme.acquire(4096, &observed));
    try std.testing.expect(nvme.acquire(4096, &observed));
    nvme.complete(4096);
    nvme.complete(4096);
}

test "device wakeup registration and generation publication make bounded progress" {
    if (builtin.os.tag != .linux) return;
    const clock = @import("../clock.zig");
    var ready: std.atomic.Value(u32) = .init(0);
    var done: std.atomic.Value(u32) = .init(0);
    var cancel: std.atomic.Value(bool) = .init(false);
    const worker = try std.Thread.spawn(.{}, struct {
        fn run(r: *std.atomic.Value(u32), d: *std.atomic.Value(u32), c: *std.atomic.Value(bool)) void {
            for (1..1001) |step| {
                const observed = generation.load(.seq_cst);
                r.store(@intCast(step), .release);
                park(observed, c);
                if (c.load(.acquire)) return;
                d.store(@intCast(step), .release);
            }
        }
    }.run, .{ &ready, &done, &cancel });
    defer {
        cancel.store(true, .seq_cst);
        _ = generation.fetchAdd(1, .seq_cst);
        // An unconditional cleanup wake keeps a regression from hanging the test runner.
        _ = linux.futex_3arg(&generation.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        worker.join();
    }
    const deadline = clock.monoNs() + 3_000_000_000;
    for (1..1001) |step| {
        while (ready.load(.acquire) < step and clock.monoNs() < deadline) std.Thread.yield() catch {};
        if (clock.monoNs() >= deadline) break;
        wake();
    }
    while (done.load(.acquire) < 1000 and clock.monoNs() < deadline) std.Thread.yield() catch {};
    try std.testing.expectEqual(@as(u32, 1000), done.load(.acquire));
}
