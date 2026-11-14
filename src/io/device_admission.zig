//! Process-wide credits for explicit local reads. Nodes live in the files that hold them; no global heap or daemon.
const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;

var mutex: std.atomic.Mutex = .unlocked;
var files: ?*Admission = null;
var generation: std.atomic.Value(u32) = .init(0);
var waiters: std.atomic.Value(u32) = .init(0);

fn lock() void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub const Admission = struct {
    device: u64,
    owner: usize,
    serial: bool,
    block: u64,
    limit: u64,
    next: ?*Admission = null,
    held: u64 = 0,
    remaining: usize = 0,
    leased: bool = false,

    pub fn discover(fd: i32, owner: usize) Admission {
        const page = std.heap.pageSize();
        // VM_READAHEAD_PAGES: the kernel fallback when a virtual/network filesystem has no block queue.
        var result: Admission = .{ .device = 0, .owner = owner, .serial = true, .block = 128 << 10, .limit = 128 << 10 };
        if (builtin.os.tag != .linux) return result;
        var st: linux.Statx = undefined;
        if (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{}, &st)) != .SUCCESS) return result;
        // Partitions share their physical queue. Stacked/virtual devices have no proven leaf: key zero
        // excludes every device, so an unknown alias cannot escape the physical device's credits.
        result.device = physicalQueue(st.dev_major, st.dev_minor) orelse 0;
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
        const ra = (queue(st.dev_major, st.dev_minor, "read_ahead_kb") orelse 128) *| 1024;
        const optimal = queue(st.dev_major, st.dev_minor, "optimal_io_size") orelse page;
        const sectors = queue(st.dev_major, st.dev_minor, "max_sectors_kb");
        const requests = queue(st.dev_major, st.dev_minor, "nr_requests");
        const rotational = if (result.device != 0) queue(st.dev_major, st.dev_minor, "rotational") else null;
        const max_read = std.mem.alignBackward(u64, std.math.maxInt(i32), page);
        const request = (sectors orelse 0) *| 1024;
        result.block = @min(max_read, std.mem.alignForward(u64, @max(@max(@max(ra, optimal), request), page), page));
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
        self.leased = false;
        mutex.unlock();
        wake();
    }

    /// Reserve before submitting. A rotating/unknown device keeps one stream lease through this reader's files.
    pub fn acquire(self: *Admission, bytes: u64, observed: *u32) bool {
        lock();
        defer mutex.unlock();
        observed.* = generation.load(.acquire);
        var held: u64 = 0;
        var serial = self.serial;
        var limit = self.limit;
        var other_lease = false;
        var current = files;
        while (current) |node| : (current = node.next) if (node.device == self.device or node.device == 0 or self.device == 0) {
            held +|= node.held;
            serial = serial or node.serial;
            limit = @min(limit, node.limit);
            other_lease = other_lease or (node.leased and node.owner != self.owner);
        };
        if (serial and (held != 0 or other_lease)) return false;
        // A single aligned unit always progresses, even if a device advertises less than its readahead.
        if (held != 0 and bytes > limit -| held) return false;
        self.held += bytes;
        if (serial) self.leased = true;
        return true;
    }

    pub fn complete(self: *Admission, bytes: u64) void {
        lock();
        std.debug.assert(self.held >= bytes and self.remaining > 0);
        self.held -= bytes;
        self.remaining -= 1;
        if (self.remaining == 0) self.leased = false;
        mutex.unlock();
        wake();
    }

    /// Only after every thread joined and every kernel-owned read completed.
    pub fn clear(self: *Admission) void {
        lock();
        self.held = 0;
        self.leased = false;
        mutex.unlock();
        wake();
    }

    pub fn yieldStream(self: *Admission) void {
        lock();
        if (self.held == 0) self.leased = false;
        mutex.unlock();
        wake();
    }
};

pub fn park(observed: u32, cancel: *const std.atomic.Value(bool)) void {
    _ = waiters.fetchAdd(1, .acq_rel);
    defer _ = waiters.fetchSub(1, .acq_rel);
    if (cancel.load(.acquire)) return;
    _ = linux.futex_4arg(&generation.raw, .{ .cmd = .WAIT, .private = true }, observed, null);
}

pub fn wake() void {
    _ = generation.fetchAdd(1, .release);
    if (waiters.load(.acquire) != 0)
        _ = linux.futex_3arg(&generation.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
}

fn queue(major: u32, minor: u32, name: []const u8) ?u64 {
    var path: [std.fs.max_path_bytes]u8 = undefined;
    var data: [64]u8 = undefined;
    for ([_][]const u8{ "queue", "../queue" }) |dir| {
        const z = std.fmt.bufPrintSentinel(&path, "/sys/dev/block/{d}:{d}/{s}/{s}", .{ major, minor, dir, name }, 0) catch return null;
        const fd = std.posix.openat(std.posix.AT.FDCWD, z, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0) catch continue;
        defer _ = linux.close(fd);
        const n = std.posix.read(fd, &data) catch continue;
        return std.fmt.parseInt(u64, std.mem.trim(u8, data[0..n], " \t\r\n"), 10) catch null;
    }
    return null;
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

test "device credits and stream leases span files and queries, but not independent devices" {
    var a: Admission = .{ .device = 1, .owner = 1, .serial = true, .block = 4096, .limit = 4096, .remaining = 2 };
    var b: Admission = .{ .device = 1, .owner = 2, .serial = true, .block = 4096, .limit = 4096, .remaining = 1 };
    var c: Admission = .{ .device = 2, .owner = 2, .serial = true, .block = 4096, .limit = 4096, .remaining = 1 };
    a.register();
    defer a.unregister();
    b.register();
    defer b.unregister();
    c.register();
    defer c.unregister();
    var observed: u32 = 0;
    try std.testing.expect(a.acquire(4096, &observed));
    try std.testing.expect(!b.acquire(4096, &observed));
    try std.testing.expect(c.acquire(4096, &observed));
    c.complete(4096);
    a.complete(4096);
    try std.testing.expect(!b.acquire(4096, &observed));
    try std.testing.expect(a.acquire(4096, &observed));
    a.complete(4096);
    try std.testing.expect(b.acquire(4096, &observed));
    b.complete(4096);
}

test "nonrotating device byte credits are shared and cancellation teardown returns held credits" {
    var a: Admission = .{ .device = 3, .owner = 1, .serial = false, .block = 4096, .limit = 8192, .remaining = 3 };
    var b: Admission = .{ .device = 3, .owner = 2, .serial = false, .block = 4096, .limit = 8192, .remaining = 2 };
    a.register();
    defer a.unregister();
    b.register();
    defer b.unregister();
    var observed: u32 = 0;
    try std.testing.expect(a.acquire(4096, &observed));
    try std.testing.expect(b.acquire(4096, &observed));
    try std.testing.expect(!a.acquire(4096, &observed));
    a.clear();
    try std.testing.expect(b.acquire(4096, &observed));
    b.complete(4096);
    b.complete(4096);
}

test "an unknown backing device excludes known streams" {
    var unknown: Admission = .{ .device = 0, .owner = 1, .serial = true, .block = 4096, .limit = 4096, .remaining = 1 };
    var known: Admission = .{ .device = 4, .owner = 2, .serial = false, .block = 4096, .limit = 8192, .remaining = 2 };
    unknown.register();
    defer unknown.unregister();
    known.register();
    defer known.unregister();
    var observed: u32 = 0;
    try std.testing.expect(known.acquire(4096, &observed));
    try std.testing.expect(!unknown.acquire(4096, &observed));
    known.complete(4096);
    known.yieldStream();
    try std.testing.expect(unknown.acquire(4096, &observed));
    try std.testing.expect(!known.acquire(4096, &observed));
    unknown.complete(4096);
    try std.testing.expect(known.acquire(4096, &observed));
    known.complete(4096);
}

test "unknown backing devices keep their own runtime facts" {
    if (builtin.os.tag != .linux) return;
    var other: Admission = .{ .device = 0, .owner = 1, .serial = true, .block = 1 << 20, .limit = 1 << 20 };
    other.register();
    defer other.unregister();
    const fd = try std.posix.openat(std.posix.AT.FDCWD, "/proc/self/stat", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    defer _ = linux.close(fd);
    const discovered = Admission.discover(fd, 2);
    try std.testing.expectEqual(@as(u64, 0), discovered.device);
    try std.testing.expectEqual(@as(u64, 128 << 10), discovered.block);
}
