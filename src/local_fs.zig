//! Portable local-file primitives for code that has no `Io` handle.
//!
//! Goes through `std.posix` (libc when linked, which every zpq binary
//! is, raw Linux syscalls otherwise), so the same calls work on Linux
//! and macOS. Directory listing keeps the batched `getdents64` read on
//! Linux and falls back to `std.Io.Dir` iteration elsewhere.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

pub const fd_t = posix.fd_t;

pub fn close(fd: fd_t) void {
    _ = posix.system.close(fd);
}

/// A successful output must surface delayed write failures; never retry close on a reused descriptor.
pub fn closeChecked(fd: fd_t) !void {
    switch (posix.errno(posix.system.close(fd))) {
        .SUCCESS => {},
        .NOSPC, .DQUOT => return error.NoSpaceLeft,
        else => return error.CloseFailed,
    }
}

/// Flush data and the metadata needed to read it before publication. Darwin has no fdatasync.
pub fn syncData(fd: fd_t) !void {
    while (true) {
        const rc = if (builtin.os.tag == .macos) posix.system.fsync(fd) else posix.system.fdatasync(fd);
        switch (posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .IO => return error.InputOutput,
            .NOSPC, .DQUOT => return error.NoSpaceLeft,
            else => return error.SyncFailed,
        }
    }
}

/// Start Linux writeback without waiting for completion; publication still needs its final data sync.
pub fn startWriteback(fd: fd_t, offset: u64, len: u64) !void {
    if (builtin.os.tag != .linux) return;
    const linux = std.os.linux;
    while (true) {
        const rc = linux.syscall4(.sync_file_range, @intCast(fd), @intCast(offset), @intCast(len), 2);
        switch (linux.errno(rc)) {
            .SUCCESS, .NOSYS, .INVAL, .OPNOTSUPP, .SPIPE => return,
            .INTR => continue,
            .IO => return error.InputOutput,
            .NOSPC, .DQUOT => return error.NoSpaceLeft,
            else => return error.SyncFailed,
        }
    }
}

pub fn openRead(path: []const u8) !fd_t {
    return posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
}

/// Inodes are only unique within a device; follow aliases so hard links are also rejected.
pub fn sameFile(a: []const u8, b: []const u8) !bool {
    const az = try posix.toPosixPath(a);
    const bz = try posix.toPosixPath(b);
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        var ast = std.mem.zeroes(linux.Statx);
        var bst = std.mem.zeroes(linux.Statx);
        if (linux.errno(linux.statx(linux.AT.FDCWD, &az, 0, .{ .INO = true }, &ast)) != .SUCCESS or
            linux.errno(linux.statx(linux.AT.FDCWD, &bz, 0, .{ .INO = true }, &bst)) != .SUCCESS)
            return error.StatFailed;
        return ast.ino == bst.ino and ast.dev_major == bst.dev_major and ast.dev_minor == bst.dev_minor;
    }
    var ast: posix.system.Stat = undefined;
    var bst: posix.system.Stat = undefined;
    if (posix.errno(posix.system.stat(&az, &ast)) != .SUCCESS or
        posix.errno(posix.system.stat(&bz, &bst)) != .SUCCESS) return error.StatFailed;
    return ast.ino == bst.ino and ast.dev == bst.dev;
}

/// Create or truncate `path` for writing, mode 0644.
pub fn createFile(path: []const u8) !fd_t {
    return posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
}

/// Create `path` for writing, mode 0644, failing with `error.PathAlreadyExists` if anything is there.
pub fn createNewFile(path: []const u8) !fd_t {
    return posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .WRONLY, .CREAT = true, .EXCL = true, .CLOEXEC = true }, 0o644);
}

/// Rename `from` to `to`, replacing what `to` names.
pub fn rename(from: []const u8, to: []const u8) error{ NameTooLong, RenameFailed }!void {
    const from_z = posix.toPosixPath(from) catch return error.NameTooLong;
    const to_z = posix.toPosixPath(to) catch return error.NameTooLong;
    if (posix.errno(posix.system.renameat(posix.AT.FDCWD, &from_z, posix.AT.FDCWD, &to_z)) != .SUCCESS) {
        return error.RenameFailed;
    }
}

/// Best-effort unlink.
pub fn removeFile(path: []const u8) void {
    const path_z = posix.toPosixPath(path) catch return;
    _ = posix.system.unlink(&path_z);
}

/// Size of the regular file open at `fd`, whose offset stays at 0, or `error.NotAFile` for anything else. Linux
/// checks the type with `statx` and reads the size with `lseek(SEEK_END)` (std's libc layer has no `fstat` there);
/// elsewhere `fstat` does both, since Darwin's `mmap` answers a directory with EINVAL, which `std.posix.mmap` treats
/// as unreachable.
pub fn fileSize(fd: fd_t) !usize {
    if (builtin.os.tag == .linux) {
        // A directory "seeks" fine and then fails to map with a confusing error; a FIFO has no size at all.
        const linux = std.os.linux;
        var st = std.mem.zeroes(linux.Statx);
        if (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .TYPE = true }, &st)) == .SUCCESS and
            !linux.S.ISREG(st.mode)) return error.NotAFile;
    }
    if (builtin.os.tag != .linux) {
        var st: posix.system.Stat = undefined;
        if (posix.errno(posix.system.fstat(fd, &st)) != .SUCCESS) return error.StatFailed;
        if (!posix.S.ISREG(st.mode)) return error.NotAFile;
        return @intCast(st.size);
    }
    const end = posix.system.lseek(fd, 0, posix.SEEK.END);
    if (posix.errno(end) != .SUCCESS) return error.SeekFailed;
    if (posix.errno(posix.system.lseek(fd, 0, posix.SEEK.SET)) != .SUCCESS) return error.SeekFailed;
    return @intCast(end);
}

pub fn writeAll(fd: fd_t, bytes: []const u8) error{ WriteFailed, BrokenPipe, NoSpaceLeft }!void {
    var i: usize = 0;
    while (i < bytes.len) {
        const rc = posix.system.write(fd, bytes[i..].ptr, bytes.len - i);
        switch (posix.errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            .PIPE => return error.BrokenPipe,
            .NOSPC, .DQUOT => return error.NoSpaceLeft,
            else => return error.WriteFailed,
        }
        const n: usize = @intCast(rc);
        if (n == 0) return error.WriteFailed;
        i += n;
    }
}

/// Write ordered buffers without joining them. A bounded vector stays below Linux and Darwin's IOV_MAX; short
/// writes may stop inside a buffer, so advance both the vector index and its first remaining base before retrying.
pub fn writeSegments(fd: fd_t, segments: []const []const u8) error{ WriteFailed, BrokenPipe, NoSpaceLeft }!void {
    var at: usize = 0;
    while (at < segments.len) {
        var vectors: [64]posix.iovec_const = undefined;
        var n: usize = 0;
        while (at < segments.len and n < vectors.len) : (at += 1) {
            const bytes = segments[at];
            if (bytes.len == 0) continue;
            vectors[n] = .{ .base = bytes.ptr, .len = bytes.len };
            n += 1;
        }
        var first: usize = 0;
        while (first < n) {
            const rc = posix.system.writev(fd, vectors[first..n].ptr, @intCast(n - first));
            switch (posix.errno(rc)) {
                .SUCCESS => {},
                .INTR => continue,
                .PIPE => return error.BrokenPipe,
                .NOSPC, .DQUOT => return error.NoSpaceLeft,
                else => return error.WriteFailed,
            }
            var written: usize = @intCast(rc);
            if (written == 0) return error.WriteFailed;
            while (written >= vectors[first].len) {
                written -= vectors[first].len;
                first += 1;
                if (first == n) break;
            }
            if (first != n) {
                vectors[first].base += written;
                vectors[first].len -= written;
            }
        }
    }
}

/// Read a whole file into an `allocator`-owned buffer. A missing file is `error.FileNotFound`, which fixture-reading
/// tests turn into a skip.
pub fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    // Without blocking, as `mapFile` opens: a FIFO waited for a writer forever (`zpq schema`, `zpq conform`), and
    // `fileSize` refuses it. A regular file's reads ignore the flag.
    const fd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .NONBLOCK = true }, 0);
    defer close(fd);
    const size = try fileSize(fd);
    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    var off: usize = 0;
    while (off < size) {
        const n = try posix.read(fd, buf[off..]);
        if (n == 0) return error.ShortRead;
        off += n;
    }
    return buf;
}

/// A read-only private mapping of a whole file.
pub const Mapped = struct {
    bytes: []align(std.heap.page_size_min) const u8,
    /// Its entry in `guard`, when one was free.
    slot: ?usize = null,
    /// Open for as long as the mapping, so a reader reads the file mapped through it
    /// (`local_reader.Reader.borrowFile`) and `intact` asks that file, whatever the path names by then. Null when
    /// the process's descriptors ran short (`fdsShort`): closed once mapped, as the mapping needs none.
    fd: ?fd_t,
    /// The file as it was mapped.
    stamp: Stamp,

    pub fn unmap(self: Mapped) void {
        if (self.slot) |i| guard.leave(i);
        posix.munmap(self.bytes);
        if (self.fd) |fd| close(fd);
    }

    /// `error.InputChanged` once the file mapped (at `path`) changed under it: a read went past its end, or it has
    /// another size or mtime now. A cut inside the last page raises no fault (Linux serves zeros to the page's end),
    /// so only the size says so, and a rewrite in place only the mtime. macOS raises no fault for any cut: a mapping
    /// goes on serving the bytes it mapped, past the new end too, so there only the size and mtime say so. Another
    /// file renamed over the path is no change to this one, which the query goes on reading whole. Without a
    /// descriptor the file is found by its path, and one that is not there by its device and inode was replaced: not
    /// asked, as it cannot be.
    pub fn intact(self: Mapped, path: []const u8) error{InputChanged}!void {
        if (self.slot) |i| if (guard.faulted[i].load(.acquire)) return error.InputChanged;
        const now = if (self.fd) |fd| fdStamp(fd) catch return error.InputChanged else pathStamp(path) catch return;
        if (now.dev != self.stamp.dev or now.ino != self.stamp.ino) return;
        if (!std.meta.eql(now, self.stamp)) return error.InputChanged;
    }
};

/// Release a finished large mapping's PTEs concurrently before its final unmap. Linux serializes munmap under
/// mmap_lock; disjoint MADV_DONTNEED calls can release those PTEs in parallel on the context's persistent workers.
/// The mapping stays valid through this call, including when dispatch fails and the caller does the work.
pub fn releaseMappingPages(bytes: []align(std.heap.page_size_min) const u8, io: std.Io) void {
    if (builtin.os.tag != .linux or bytes.len < 64 << 20) return;
    const slice_len = 32 << 20;
    const Release = struct {
        bytes: []align(std.heap.page_size_min) const u8,
        next: std.atomic.Value(usize) = .init(0),

        fn run(self: *@This()) void {
            while (true) {
                const off = self.next.fetchAdd(slice_len, .monotonic);
                if (off >= self.bytes.len) return;
                const len = @min(slice_len, self.bytes.len - off);
                _ = std.os.linux.madvise(@constCast(self.bytes.ptr + off), len, std.os.linux.MADV.DONTNEED);
            }
        }
    };
    var release: Release = .{ .bytes = bytes };
    var group: std.Io.Group = .init;
    const workers = @min(16, @min(std.Thread.getCpuCount() catch 1, (bytes.len + slice_len - 1) / slice_len));
    for (1..workers) |_| group.concurrent(io, Release.run, .{&release}) catch break;
    release.run();
    group.await(io) catch {};
}

/// Mappings a query left for the process exit to take (`engine.Context.exit_after`), until `releaseLeft`. Only the
/// thread that runs the query adds to it.
var left: std.ArrayList([]align(std.heap.page_size_min) const u8) = .empty;

/// Leave `m` mapped for the process exit; `releaseLeft` drops its pages before then.
pub fn leaveForExit(m: Mapped) void {
    left.append(std.heap.page_allocator, m.bytes) catch {};
}

/// Drop the pages of the mappings left for the exit, from several threads, once the answer is out. The exit would
/// otherwise unmap them on one thread, page by page, before the process is gone: 37 ms for a fully read 937 MiB file.
/// MADV_DONTNEED over disjoint ranges runs in parallel (about 3 ms over 16 threads for that file); munmap does not, as
/// it holds the address-space lock for writing. The mappings stay, and a later read faults the file's bytes in again.
/// Linux only: elsewhere the exit takes them as before.
pub fn releaseLeft() void {
    releaseLeftOver(64 << 20);
}

fn releaseLeftOver(min_bytes: usize) void {
    defer {
        left.deinit(std.heap.page_allocator);
        left = .empty;
    }
    if (builtin.os.tag != .linux) return;
    const slice_len = 32 << 20;
    var total: usize = 0;
    var slices: usize = 0;
    for (left.items) |b| {
        total += b.len;
        slices += (b.len + slice_len - 1) / slice_len;
    }
    // What the exit would tear down is what is mapped in, not what is mapped: a query that touched little of a large
    // glob leaves next to nothing, and starting helpers would cost more than they save.
    const mapped_in = @min(total, if (statmPages(2)) |pages| pages * std.heap.pageSize() else |_| total);
    if (total == 0 or mapped_in < min_bytes) return;
    const Release = struct {
        maps: []const []align(std.heap.page_size_min) const u8,
        next: std.atomic.Value(usize) = .init(0),

        /// Claims slice `k` of every mapping in turn until none is left.
        fn run(r: *@This()) void {
            while (true) {
                var k = r.next.fetchAdd(1, .monotonic);
                const m = for (r.maps) |b| {
                    const n = (b.len + slice_len - 1) / slice_len;
                    if (k < n) break b;
                    k -= n;
                } else return;
                const off = k * slice_len;
                const len = @min(slice_len, m.len - off);
                _ = std.os.linux.madvise(@constCast(m.ptr + off), len, std.os.linux.MADV.DONTNEED);
            }
        }
    };
    var r: Release = .{ .maps = left.items };
    var helpers: [15]?std.Thread = @splat(null);
    const cpus = std.Thread.getCpuCount() catch 1;
    const want = @min(helpers.len, @min(@min(slices, cpus), mapped_in / (64 << 20)) -| 1);
    for (helpers[0..want]) |*h| h.* = std.Thread.spawn(.{ .stack_size = 1 << 20 }, Release.run, .{&r}) catch null;
    Release.run(&r);
    for (helpers[0..want]) |h| if (h) |t| t.join();
}

/// Field `i` of `/proc/self/statm`, in pages: 1 is what the process has mapped in, 2 the part of it backed by files
/// and shared memory. Read directly: `readFile` refuses it, as it is no regular file.
fn statmPages(i: usize) !usize {
    const fd = try openRead("/proc/self/statm");
    defer close(fd);
    var buf: [256]u8 = undefined;
    const n = try posix.read(fd, &buf);
    var it = std.mem.tokenizeScalar(u8, buf[0..n], ' ');
    for (0..i) |_| _ = it.next();
    return std.fmt.parseInt(usize, it.next() orelse return error.BadStatm, 10);
}

/// Raise the process's soft limit on open files to its hard limit: each mapped input holds a descriptor while it is
/// queried (`Mapped.fd`), and a glob of local files would otherwise run short past the soft limit (256 on macOS,
/// often 1024 elsewhere). For the CLI and the Lambda, whose processes they are; libzpq leaves its host's limits alone.
pub fn raiseOpenFileLimit() void {
    if (posix.rlimit_resource == void) return;
    var lim = posix.getrlimit(.NOFILE) catch return;
    // macOS refuses a soft limit past OPEN_MAX, whatever the hard limit says.
    const most = if (builtin.os.tag.isDarwin()) @min(lim.max, 10240) else lim.max;
    if (lim.cur >= most) return;
    lim.cur = most;
    posix.setrlimit(.NOFILE, lim) catch {};
}

/// Whether `fd`, just opened, shows the process past half its soft limit on descriptors: an open takes the lowest
/// free number, so every one below it is in use. A mapping then gives its own back, as one did before mappings held
/// them, and the other half stays for sockets, the reader and the host.
fn fdsShort(fd: fd_t) bool {
    if (posix.rlimit_resource == void) return false;
    const lim = posix.getrlimit(.NOFILE) catch return false;
    return @as(u64, @intCast(fd)) >= lim.cur / 2;
}

/// Which file this is (device and inode), and the size and mtime a truncation or a rewrite in place changes.
pub const Stamp = struct {
    dev: u64,
    ino: u64,
    size: u64,
    mtime_sec: i64,
    mtime_nsec: u32,
};

pub fn fdStamp(fd: fd_t) !Stamp {
    if (builtin.os.tag == .linux) return statxStamp(fd, "", std.os.linux.AT.EMPTY_PATH);
    var st: posix.system.Stat = undefined;
    if (posix.errno(posix.system.fstat(fd, &st)) != .SUCCESS) return error.StatFailed;
    return statStamp(st);
}

/// `fdStamp` for the file `path` names now.
pub fn pathStamp(path: []const u8) !Stamp {
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    if (path.len >= path_z.len) return error.NameTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    if (builtin.os.tag == .linux) return statxStamp(posix.AT.FDCWD, path_z[0..path.len :0], 0);
    var st: posix.system.Stat = undefined;
    if (posix.errno(posix.system.stat(path_z[0..path.len :0], &st)) != .SUCCESS) return error.StatFailed;
    return statStamp(st);
}

fn statxStamp(dirfd: fd_t, path: [*:0]const u8, flags: u32) !Stamp {
    const linux = std.os.linux;
    var st = std.mem.zeroes(linux.Statx);
    const mask: linux.STATX = .{ .INO = true, .SIZE = true, .MTIME = true };
    if (linux.errno(linux.statx(dirfd, path, flags, mask, &st)) != .SUCCESS) return error.StatFailed;
    return .{
        .dev = @as(u64, st.dev_major) << 32 | st.dev_minor,
        .ino = st.ino,
        .size = st.size,
        .mtime_sec = st.mtime.sec,
        .mtime_nsec = st.mtime.nsec,
    };
}

fn statStamp(st: posix.system.Stat) Stamp {
    const m = st.mtime();
    return .{
        .dev = @bitCast(@as(i64, st.dev)),
        .ino = st.ino,
        .size = @intCast(st.size),
        .mtime_sec = @intCast(m.sec),
        .mtime_nsec = @intCast(m.nsec),
    };
}

/// Opens `path` without blocking (a FIFO would wait for a writer forever), checks it is a regular file, and maps it.
pub fn mapFile(path: []const u8) !Mapped {
    const fd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .NONBLOCK = true }, 0);
    errdefer close(fd);
    const size = try fileSize(fd);
    if (size == 0) return error.EmptyFile;
    // Non-blocking for the open alone: io_uring reads through a non-blocking file answer EAGAIN instead of waiting.
    const flags = posix.system.fcntl(fd, posix.F.GETFL, @as(c_int, 0));
    if (posix.errno(flags) != .SUCCESS) return error.OpenFailed;
    const nonblock: c_int = @intCast(@as(u32, @bitCast(posix.O{ .NONBLOCK = true })));
    const blocking: c_int = @as(c_int, @intCast(flags)) & ~nonblock;
    if (posix.errno(posix.system.fcntl(fd, posix.F.SETFL, blocking)) != .SUCCESS) return error.OpenFailed;
    const stamp = try fdStamp(fd);
    // Sized by the stamp's own word so the two agree; a file growing between them maps what was asked.
    if (stamp.size != size) return error.InputChanged;
    const bytes = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, fd, 0);
    const held: ?fd_t = if (fdsShort(fd)) null else fd;
    if (held == null) close(fd);
    return .{ .bytes = bytes, .slot = guard.enter(bytes), .fd = held, .stamp = stamp };
}

/// A file mapped while another process truncates it raises SIGBUS on the first touch of a page past its new end,
/// which killed the process (a libzpq host with it). Mappings register here; the handler maps a zero page over a
/// faulting address inside one, marks it, and returns, so the read sees zeros and `Mapped.intact` refuses the
/// answer. A fault anywhere else goes to the handler that was installed before, or kills the process as it did.
/// The handler is in place only while some mapping is: the first one installs it and the last one out puts back
/// the handler before it, unless the host has installed its own since (a libzpq host gets its SIGBUS back once
/// its queries are done). The table is lock-free: the handler reads it from any thread at any moment.
const guard = struct {
    const slots = 512;
    var starts: [slots]std.atomic.Value(usize) = @splat(.init(0));
    var lens: [slots]std.atomic.Value(usize) = @splat(.init(0));
    var faulted: [slots]std.atomic.Value(bool) = @splat(.init(false));
    /// Guards `count` and the install: mappings come and go from any thread.
    var busy: std.atomic.Value(bool) = .init(false);
    var count: usize = 0;
    var prev: posix.Sigaction = undefined;

    fn lock() void {
        while (busy.swap(true, .acquire)) std.atomic.spinLoopHint();
    }

    fn unlock() void {
        busy.store(false, .release);
    }

    fn enter(bytes: []const u8) ?usize {
        if (posix.Sigaction == void) return null;
        for (0..slots) |i| {
            if (lens[i].cmpxchgStrong(0, std.math.maxInt(usize), .acq_rel, .monotonic) != null) continue;
            lock();
            if (count == 0) {
                const act: posix.Sigaction = .{
                    .handler = .{ .sigaction = onBus },
                    .mask = posix.sigemptyset(),
                    .flags = posix.SA.SIGINFO | posix.SA.ONSTACK,
                };
                posix.sigaction(.BUS, &act, &prev);
            }
            count += 1;
            unlock();
            faulted[i].store(false, .release);
            starts[i].store(@intFromPtr(bytes.ptr), .release);
            lens[i].store(bytes.len, .release);
            return i;
        }
        return null;
    }

    fn leave(i: usize) void {
        starts[i].store(0, .release);
        lens[i].store(0, .release);
        lock();
        defer unlock();
        count -= 1;
        if (count != 0) return;
        var cur: posix.Sigaction = undefined;
        posix.sigaction(.BUS, null, &cur);
        if (cur.flags & posix.SA.SIGINFO == 0) return;
        if (cur.handler.sigaction) |f| if (f == &onBus) posix.sigaction(.BUS, &prev, null);
    }

    fn onBus(sig: posix.SIG, info: *const posix.siginfo_t, ctx: ?*anyopaque) callconv(.c) void {
        const addr = faultAddress(info);
        for (0..slots) |i| {
            const start = starts[i].load(.acquire);
            const len = lens[i].load(.acquire);
            if (start == 0 or len == std.math.maxInt(usize) or addr < start or addr - start >= len) continue;
            const page = std.heap.pageSize();
            const at = addr & ~(page - 1);
            const r = posix.system.mmap(
                @ptrFromInt(at),
                page,
                .{ .READ = true },
                .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .FIXED = true },
                -1,
                0,
            );
            // libc answers a pointer (MAP_FAILED is all ones), the raw syscall an errno-coded word.
            const failed = if (@typeInfo(@TypeOf(r)) == .pointer)
                @intFromPtr(r) == std.math.maxInt(usize)
            else
                posix.errno(r) != .SUCCESS;
            if (failed) break;
            faulted[i].store(true, .release);
            return;
        }
        // Not ours: as the handler before would have it.
        if (prev.flags & posix.SA.SIGINFO != 0) {
            if (prev.handler.sigaction) |f| return f(sig, info, ctx);
        } else if (prev.handler.handler) |f| {
            if (f != posix.SIG.DFL and f != posix.SIG.IGN) return f(sig);
        }
        posix.sigaction(.BUS, &prev, null);
    }

    fn faultAddress(info: *const posix.siginfo_t) usize {
        if (@hasField(posix.siginfo_t, "fields")) return @intFromPtr(info.fields.sigfault.addr);
        return @intFromPtr(info.addr);
    }
};

/// Names of the entries in `dir_path` that are regular files, symlinks, or of unknown type (a filesystem that does
/// not report types), in directory order. Names are `arena`-owned.
pub fn listFiles(arena: std.mem.Allocator, io: std.Io, dir_path: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const fd = try posix.openat(posix.AT.FDCWD, dir_path, .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true }, 0);
        defer close(fd);
        var buf: [8192]u8 align(8) = undefined;
        while (true) {
            const n = linux.getdents64(fd, &buf, buf.len);
            if (linux.errno(n) != .SUCCESS) return error.GetdentsFailed;
            if (n == 0) break;
            var off: usize = 0;
            while (off < n) {
                const entry: *const linux.dirent64 = @ptrCast(@alignCast(&buf[off]));
                off += entry.reclen;
                if (entry.type != linux.DT.REG and entry.type != linux.DT.LNK and entry.type != linux.DT.UNKNOWN) continue;
                const name_ptr: [*:0]const u8 = @ptrCast(&entry.name);
                try out.append(arena, try arena.dupe(u8, std.mem.sliceTo(name_ptr, 0)));
            }
        }
    } else {
        var dir = try std.Io.Dir.cwd().openDir(io, dir_path, .{ .iterate = true });
        defer dir.close(io);
        var it = dir.iterate();
        while (try it.next(io)) |entry| switch (entry.kind) {
            .file, .sym_link, .unknown => try out.append(arena, try arena.dupe(u8, entry.name)),
            else => {},
        };
    }
    return out.items;
}

const testing = std.testing;

test "createFile / writeAll / readFile / mapFile / listFiles round-trip" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const dir_path = try std.fmt.allocPrint(arena, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const file_path = try std.fs.path.join(arena, &.{ dir_path, "a.bin" });

    const fd = try createFile(file_path);
    try writeAll(fd, "hello, ");
    try writeAll(fd, "local fs");
    close(fd);

    const bytes = try readFile(testing.allocator, file_path);
    defer testing.allocator.free(bytes);
    try testing.expectEqualStrings("hello, local fs", bytes);

    const m = try mapFile(file_path);
    defer m.unmap();
    try testing.expectEqualStrings("hello, local fs", m.bytes);

    try tmp.dir.createDirPath(testing.io, "sub");
    const names = try listFiles(arena, testing.io, dir_path);
    try testing.expectEqual(@as(usize, 1), names.len);
    try testing.expectEqualStrings("a.bin", names[0]);

    removeFile(file_path);
    try testing.expectError(error.FileNotFound, readFile(testing.allocator, file_path));
}

test "createNewFile refuses an existing path; rename replaces its target" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var a_buf: [256]u8 = undefined;
    var b_buf: [256]u8 = undefined;
    const a = try std.fmt.bufPrint(&a_buf, ".zig-cache/tmp/{s}/a", .{tmp.sub_path});
    const b = try std.fmt.bufPrint(&b_buf, ".zig-cache/tmp/{s}/b", .{tmp.sub_path});
    var fd = try createNewFile(a);
    try writeAll(fd, "new");
    close(fd);
    try testing.expectError(error.PathAlreadyExists, createNewFile(a));
    fd = try createFile(b);
    try writeAll(fd, "old");
    close(fd);
    try rename(a, b);
    const bytes = try readFile(testing.allocator, b);
    defer testing.allocator.free(bytes);
    try testing.expectEqualStrings("new", bytes);
    try testing.expectError(error.FileNotFound, readFile(testing.allocator, a));
    try testing.expectError(error.RenameFailed, rename(a, b));
}

test "releaseLeft drops a left mapping's pages from the resident set and leaves its bytes readable" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const file_path = try std.fmt.allocPrint(arena, ".zig-cache/tmp/{s}/big.bin", .{tmp.sub_path});
    const data = try arena.alloc(u8, 40 << 20);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 2654435761 >> 7);
    const fd = try createFile(file_path);
    try writeAll(fd, data);
    close(fd);

    const m = try mapFile(file_path);
    defer m.unmap();
    try testing.expectEqualSlices(u8, data, m.bytes); // every page touched
    const before = try statmPages(1);
    leaveForExit(m);
    releaseLeftOver(0);
    try testing.expectEqual(@as(usize, 0), left.items.len);
    const after = try statmPages(1);
    try testing.expect(before - after >= (40 << 20) / std.heap.pageSize() * 9 / 10);
    try testing.expectEqualSlices(u8, data, m.bytes);
}

test "mapFile rejects an empty file" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/empty", .{tmp.sub_path});
    close(try createFile(path));
    try testing.expectError(error.EmptyFile, mapFile(path));
}

/// A byte of a file written as 0xab, read through its mapping past where it was cut since. Linux serves zeros (the cut
/// page's tail, or the guard's zero page over a removed one); macOS goes on serving the bytes it mapped, or zeros
/// should the page be evicted and fault. Either way `Mapped.intact` is what reports the cut.
fn expectCutByte(byte: u8) !void {
    if (builtin.os.tag.isDarwin() and byte == 0xab) return;
    try testing.expectEqual(@as(u8, 0), byte);
}

test "regression: a mapped file truncated under the reader reads zeros past its end and reports it changed" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/shrinks", .{tmp.sub_path});
    const page = std.heap.pageSize();
    const fd = try createFile(path);
    const data = try testing.allocator.alloc(u8, 3 * page);
    defer testing.allocator.free(data);
    @memset(data, 0xab);
    try writeAll(fd, data);
    close(fd);

    const m = try mapFile(path);
    defer m.unmap();
    try m.intact(path);
    const wfd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .WRONLY, .CLOEXEC = true }, 0);
    defer close(wfd);
    try testing.expectEqual(posix.E.SUCCESS, posix.errno(posix.system.ftruncate(wfd, @intCast(page))));
    const before: *const volatile u8 = &m.bytes[0];
    const past: *const volatile u8 = &m.bytes[2 * page + 7];
    try testing.expectEqual(@as(u8, 0xab), before.*);
    // Unguarded, this read was SIGBUS on Linux.
    try expectCutByte(past.*);
    try testing.expectError(error.InputChanged, m.intact(path));

    // A fresh mapping of the file as it now is reads whole.
    const again = try mapFile(path);
    defer again.unmap();
    try testing.expectEqual(page, again.bytes.len);
    try again.intact(path);
}

test "regression: a mapped file cut inside its last page or rewritten in place reports it changed; renamed over, not" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/f", .{tmp.sub_path});
    var nbuf: [256]u8 = undefined;
    const other = try std.fmt.bufPrintSentinel(&nbuf, ".zig-cache/tmp/{s}/g", .{tmp.sub_path}, 0);
    var data: [1000]u8 = undefined;
    const write = struct {
        fn at(p: []const u8, bytes: []const u8) !void {
            const fd = try createFile(p);
            defer close(fd);
            try writeAll(fd, bytes);
        }
    }.at;

    // A mapping holds its descriptor, or gave it back when the process ran short of them (`fdsShort`).
    const giveBack = struct {
        fn of(m: *Mapped) void {
            close(m.fd.?);
            m.fd = null;
        }
    }.of;
    for ([_]bool{ true, false }) |held| {
        data = @splat(0xab);
        // No read crosses a page the cut removed, so no fault: the kernel serves zeros to the page's end.
        try write(path, &data);
        var cut = try mapFile(path);
        defer cut.unmap();
        if (!held) giveBack(&cut);
        const wfd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .WRONLY, .CLOEXEC = true }, 0);
        try testing.expectEqual(posix.E.SUCCESS, posix.errno(posix.system.ftruncate(wfd, 600)));
        close(wfd);
        const tail: *const volatile u8 = &cut.bytes[900];
        try expectCutByte(tail.*);
        try testing.expectError(error.InputChanged, cut.intact(path));

        // Same size, other bytes: only the mtime tells, set apart here since a fast rewrite can land in the same tick.
        try write(path, &data);
        var same = try mapFile(path);
        defer same.unmap();
        if (!held) giveBack(&same);
        try same.intact(path);
        data[0] = 0;
        try write(path, &data);
        const rfd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
        const times = [2]posix.timespec{ .{ .sec = 1, .nsec = 0 }, .{ .sec = 1, .nsec = 0 } };
        try testing.expectEqual(posix.E.SUCCESS, posix.errno(posix.system.futimens(rfd, &times)));
        close(rfd);
        try testing.expectError(error.InputChanged, same.intact(path));

        // Another file renamed over the path, as tools replace files: the one mapped is unchanged and still read.
        var kept = try mapFile(path);
        defer kept.unmap();
        if (!held) giveBack(&kept);
        const replacement: [1000]u8 = @splat(0xcd);
        try write(other, &replacement);
        var pbuf: [256]u8 = undefined;
        const path_z = try std.fmt.bufPrintSentinel(&pbuf, "{s}", .{path}, 0);
        try testing.expectEqual(posix.E.SUCCESS, posix.errno(posix.system.rename(other, path_z)));
        try kept.intact(path);
        try testing.expectEqualSlices(u8, &data, kept.bytes);
    }
}

test "regression: the SIGBUS handler is in place only while a mapping is, and yields to one installed since" {
    if (posix.Sigaction == void) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/m", .{tmp.sub_path});
    const fd = try createFile(path);
    try writeAll(fd, "abc");
    close(fd);
    var before: posix.Sigaction = undefined;
    posix.sigaction(.BUS, null, &before);
    const m = try mapFile(path);
    var during: posix.Sigaction = undefined;
    posix.sigaction(.BUS, null, &during);
    try testing.expect(during.handler.sigaction == &guard.onBus);
    m.unmap();
    var after: posix.Sigaction = undefined;
    posix.sigaction(.BUS, null, &after);
    try testing.expect(after.handler.handler == before.handler.handler);

    const Host = struct {
        fn on(_: posix.SIG) callconv(.c) void {}
    };
    const mine: posix.Sigaction = .{ .handler = .{ .handler = Host.on }, .mask = posix.sigemptyset(), .flags = 0 };
    const n = try mapFile(path);
    posix.sigaction(.BUS, &mine, null);
    n.unmap();
    posix.sigaction(.BUS, null, &after);
    try testing.expect(after.handler.handler == mine.handler.handler);
    posix.sigaction(.BUS, &before, null);
}

test "regression: mapFile and readFile refuse a directory and a FIFO instead of misreporting or blocking" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const dir = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    try testing.expectError(error.NotAFile, mapFile(dir));
    var fbuf: [256]u8 = undefined;
    const fifo = try std.fmt.bufPrintSentinel(&fbuf, ".zig-cache/tmp/{s}/pipe", .{tmp.sub_path}, 0);
    const linux = std.os.linux;
    const made = linux.mknodat(linux.AT.FDCWD, fifo, linux.S.IFIFO | 0o600, 0);
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(made));
    try testing.expectError(error.NotAFile, mapFile(fifo));
    try testing.expectError(error.NotAFile, readFile(testing.allocator, fifo));
    try testing.expectError(error.NotAFile, readFile(testing.allocator, dir));
}

test "writeAll distinguishes a full device from a broken pipe" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const fd = try posix.openat(posix.AT.FDCWD, "/dev/full", .{ .ACCMODE = .WRONLY }, 0);
    defer close(fd);
    try testing.expectError(error.NoSpaceLeft, writeAll(fd, "x"));
    const pipe = try std.Io.Threaded.pipe2(.{ .CLOEXEC = true });
    close(pipe[0]);
    defer close(pipe[1]);
    try testing.expectError(error.BrokenPipe, writeAll(pipe[1], "x"));
}

test "parallel mapping release keeps bytes intact and falls back when concurrency is unavailable" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const path = try std.fmt.allocPrint(arena.allocator(), ".zig-cache/tmp/{s}/release.bin", .{tmp.sub_path});
    const data = try arena.allocator().alloc(u8, 65 << 20);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 2654435761 >> 7);
    const fd = try createFile(path);
    try writeAll(fd, data);
    close(fd);
    const mapped = try mapFile(path);
    defer mapped.unmap();
    for ([_]std.Io.Limit{ .unlimited, .nothing }) |limit| {
        var runtime = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = limit });
        defer runtime.deinit();
        try testing.expectEqualSlices(u8, data, mapped.bytes);
        releaseMappingPages(mapped.bytes, runtime.io());
        try testing.expectEqualSlices(u8, data, mapped.bytes);
    }
}

test "starting writeback preserves the bytes and the write position" {
    const t = std.testing;
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/writeback", .{tmp.sub_path});
    const fd = try createNewFile(path);
    defer close(fd);
    try writeAll(fd, "before");
    try startWriteback(fd, 0, 6);
    try writeAll(fd, "after");
    try syncData(fd);
    const bytes = try readFile(t.allocator, path);
    defer t.allocator.free(bytes);
    try t.expectEqualStrings("beforeafter", bytes);
}

test "writeback hints leave pipe outputs usable" {
    var fds: [2]fd_t = undefined;
    if (std.c.pipe(&fds) != 0) return error.PipeFailed;
    defer close(fds[0]);
    defer close(fds[1]);
    try writeAll(fds[1], "x");
    try startWriteback(fds[1], 0, 1);
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(isize, 1), posix.system.read(fds[0], &byte, 1));
    try std.testing.expectEqual(@as(u8, 'x'), byte[0]);
}

test "ordered segment writes preserve empty buffers and vector batch boundaries and surface sink errors" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/vectors", .{tmp.sub_path});
    var segments: [193][]const u8 = undefined;
    for (&segments, 0..) |*s, i| s.* = if (i % 7 == 0) "" else try std.fmt.allocPrint(a, "{d};", .{i});
    const fd = try createFile(path);
    defer close(fd);
    try writeSegments(fd, &segments);
    try writeSegments(fd, &.{});
    const got = try readFile(a, path);
    try testing.expectEqualSlices(u8, try std.mem.concat(a, u8, &segments), got);
    if (builtin.os.tag == .linux) {
        const full = try posix.openat(posix.AT.FDCWD, "/dev/full", .{ .ACCMODE = .WRONLY }, 0);
        defer close(full);
        try testing.expectError(error.NoSpaceLeft, writeSegments(full, &.{ "a", "", "b" }));
        const pipe = try std.Io.Threaded.pipe2(.{ .CLOEXEC = true });
        close(pipe[0]);
        defer close(pipe[1]);
        try testing.expectError(error.BrokenPipe, writeSegments(pipe[1], &.{ "", "a", "b" }));
    }
}
