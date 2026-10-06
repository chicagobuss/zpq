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

pub fn openRead(path: []const u8) !fd_t {
    return posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
}

/// Create or truncate `path` for writing, mode 0644.
pub fn createFile(path: []const u8) !fd_t {
    return posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
}

/// Best-effort unlink.
pub fn removeFile(path: []const u8) void {
    const path_z = posix.toPosixPath(path) catch return;
    _ = posix.system.unlink(&path_z);
}

/// Size of the regular file open at `fd`, whose offset stays at 0. Linux reads it with `lseek(SEEK_END)` (std's
/// libc layer has no `fstat` there) and lets `mmap`/`read` reject a directory; elsewhere `fstat` rejects anything but a
/// regular file up front, since Darwin's `mmap` answers a directory with EINVAL, which `std.posix.mmap` treats as
/// unreachable.
pub fn fileSize(fd: fd_t) !usize {
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

pub fn writeAll(fd: fd_t, bytes: []const u8) error{WriteFailed}!void {
    var i: usize = 0;
    while (i < bytes.len) {
        const rc = posix.system.write(fd, bytes[i..].ptr, bytes.len - i);
        switch (posix.errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
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

pub fn mapFile(path: []const u8) !Mapped {
    const fd = try openRead(path);
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

test "mapFile rejects an empty file" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/empty", .{tmp.sub_path});
    close(try createFile(path));
    try testing.expectError(error.EmptyFile, mapFile(path));
}
