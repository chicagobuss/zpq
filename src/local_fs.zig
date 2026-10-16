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

/// Read a whole file into an `allocator`-owned buffer. A missing file is `error.FileNotFound`, which fixture-reading
/// tests turn into a skip.
pub fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const fd = try openRead(path);
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

    pub fn unmap(self: Mapped) void {
        posix.munmap(self.bytes);
    }
};

/// Opens `path` without blocking (a FIFO would wait for a writer forever), checks it is a regular file, and maps it.
pub fn mapFile(path: []const u8) !Mapped {
    const fd = try posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .NONBLOCK = true }, 0);
    defer close(fd);
    const size = try fileSize(fd);
    if (size == 0) return error.EmptyFile;
    return .{ .bytes = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, fd, 0) };
}

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

test "mapFile rejects an empty file" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/empty", .{tmp.sub_path});
    close(try createFile(path));
    try testing.expectError(error.EmptyFile, mapFile(path));
}

test "regression: mapFile refuses a directory and a FIFO instead of misreporting or blocking" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const dir = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    try testing.expectError(error.NotAFile, mapFile(dir));
    var fbuf: [256]u8 = undefined;
    const fifo = try std.fmt.bufPrintSentinel(&fbuf, ".zig-cache/tmp/{s}/pipe", .{tmp.sub_path}, 0);
    const linux = std.os.linux;
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.mknodat(linux.AT.FDCWD, fifo, linux.S.IFIFO | 0o600, 0)));
    try testing.expectError(error.NotAFile, mapFile(fifo));
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
