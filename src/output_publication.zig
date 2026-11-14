//! Atomic local-file publication shared by the CLI and service output path.
const std = @import("std");
const local_fs = @import("local_fs.zig");

// Detect delayed writeback errors before publishing a local result.
pub const default_fsync = true;

var next_id: std.atomic.Value(u64) = .init(0);

pub const OutputPublication = struct {
    fsync: bool = default_fsync,
    target: ?[]const u8 = null,
    permissions: ?std.Io.File.Permissions = null,
    fd: ?local_fs.fd_t = null,
    target_buffer: [std.fs.max_path_bytes]u8 = undefined,
    buffer: [std.fs.max_path_bytes]u8 = undefined,
    len: usize = 0,
    /// Tests only: runs after the sync (`hook_at` .sync) or the close (.close), before the rename, to change the target
    /// in that window.
    before_rename: ?*const fn ([]const u8) void = null,
    hook_at: enum { sync, close } = .close,
    /// Tests only: the error `commitReplacing`'s exchange back fails with.
    fail_rollback: ?anyerror = null,
    fail_parent_sync: if (@import("builtin").is_test) ?anyerror else void = if (@import("builtin").is_test) null else {},

    pub fn path(self: *const OutputPublication) []const u8 {
        return self.buffer[0..self.len];
    }

    pub fn prepare(self: *OutputPublication, io: std.Io, output: ?[]const u8, inputs: []const []const u8) !?[]const u8 {
        const target = output orelse return null;
        if (std.mem.startsWith(u8, target, "s3://")) {
            for (inputs) |input| if (std.mem.eql(u8, input, target)) {
                return error.InputOutputSame;
            };
            return target;
        }
        const cwd = std.Io.Dir.cwd();
        if (cwd.statFile(io, target, .{})) |st| {
            self.permissions = st.permissions;
            if (st.kind != .file) {
                return error.OutputNotRegular;
            }
            for (inputs) |input| {
                if (std.mem.startsWith(u8, input, "s3://")) continue;
                const same = local_fs.sameFile(input, target) catch continue;
                if (same) {
                    return error.InputOutputSame;
                }
            }
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
        const pid: u32 = @bitCast(std.posix.system.getpid());
        const id = next_id.fetchAdd(1, .monotonic);
        const tmp = try std.fmt.bufPrint(&self.buffer, "{s}.{d}.{d}.zpq-tmp", .{ target, pid, id });
        // Retain the exclusive fd: reopening even this empty file with O_TRUNC makes ext4 flush at final close.
        self.fd = try local_fs.createNewFile(tmp);
        self.len = tmp.len;
        // The library publishes after the request parser has released its strings.
        @memcpy(self.target_buffer[0..target.len], target);
        self.target = self.target_buffer[0..target.len];
        return self.path();
    }

    pub fn commit(self: *OutputPublication, io: std.Io) !void {
        const target = self.target orelse return;
        try self.seal(io);
        const parent = if (self.fsync) try local_fs.openParent(target) else null;
        defer if (parent) |fd| local_fs.close(fd);
        try local_fs.rename(self.path(), target);
        try self.published(parent);
    }

    /// `commit`, for a result that replaces the very file it was made from (`expected`, as it was read): published only
    /// if the target is still that file at the rename, after the sync. The staged file and the target are exchanged
    /// atomically and the file exchanged out is checked; another one (a file renamed over the target meanwhile, or the
    /// same one rewritten) is exchanged back and `error.InputChanged` returned, and a removed target is not recreated.
    /// Where the filesystem cannot exchange, the target is checked just before a plain rename instead. If exchanging
    /// back fails, nothing is removed: `error.RollbackFailed`, the other file kept at `path()`, this result at the target.
    pub fn commitReplacing(self: *OutputPublication, io: std.Io, expected: local_fs.Stamp) !void {
        const target = self.target orelse return;
        try self.seal(io);
        const parent = if (self.fsync) try local_fs.openParent(target) else null;
        defer if (parent) |fd| local_fs.close(fd);
        local_fs.exchange(self.path(), target) catch |err| switch (err) {
            error.NotFound => return error.InputChanged,
            error.Unsupported => {
                const now = local_fs.pathStamp(target) catch return error.InputChanged;
                if (!std.meta.eql(now, expected)) return error.InputChanged;
                try local_fs.rename(self.path(), target);
                try self.published(parent);
                return;
            },
            else => return err,
        };
        // The staging name now holds what the target was.
        const out = local_fs.pathStamp(self.path()) catch null;
        if (out == null or !std.meta.eql(out.?, expected)) {
            // Put the other file back; the staged result stays at the staging name for `abort` to remove.
            self.exchangeBack(target) catch {
                // The other file is at the staging name and this result at the target. Nothing may be removed now: the
                // staging name is kept (`abort` leaves it) and named in the error's message (`path`).
                self.target = null;
                return error.RollbackFailed;
            };
            return error.InputChanged;
        }
        local_fs.removeFile(self.path());
        try self.published(parent);
    }

    fn published(self: *OutputPublication, parent: ?local_fs.fd_t) !void {
        // Rename has published: abort must not remove a staging name recreated by another writer after this point.
        self.target = null;
        if (parent) |fd| {
            if (@import("builtin").is_test) if (self.fail_parent_sync) |err| return err;
            try local_fs.syncDirectory(fd);
        }
    }

    fn exchangeBack(self: *OutputPublication, target: []const u8) !void {
        if (@import("builtin").is_test) if (self.fail_rollback) |err| return err;
        return local_fs.exchange(self.path(), target);
    }

    /// Mode, sync and close: the staged file is complete and durable.
    fn seal(self: *OutputPublication, io: std.Io) !void {
        const fd = self.fd orelse unreachable;
        if (self.permissions) |permissions| {
            const file: std.Io.File = .{ .handle = fd, .flags = .{ .nonblocking = false } };
            try file.setPermissions(io, permissions);
        }
        // Keep ownership until sync succeeds, so abort also closes the fd on a writeback error.
        if (self.fsync) try local_fs.syncData(fd);
        if (@import("builtin").is_test) if (self.hook_at == .sync) if (self.before_rename) |hook| hook(self.target.?);
        self.fd = null;
        try local_fs.closeChecked(fd);
        if (@import("builtin").is_test) if (self.hook_at == .close) if (self.before_rename) |hook| hook(self.target.?);
    }

    pub fn abort(self: *OutputPublication) void {
        if (self.fd) |fd| local_fs.close(fd);
        self.fd = null;
        if (self.target != null) local_fs.removeFile(self.path());
        self.target = null;
    }
};

test "publication abort preserves target; concurrent preparations retain separate exclusive descriptors" {
    const t = std.testing;
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const target = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/answer", .{tmp.sub_path});
    const old = try local_fs.createFile(target);
    try local_fs.writeAll(old, "old");
    local_fs.close(old);
    var a: OutputPublication = .{};
    defer a.abort();
    var b: OutputPublication = .{ .fsync = true };
    defer b.abort();
    _ = try a.prepare(t.io, target, &.{});
    _ = try b.prepare(t.io, target, &.{});
    try t.expect(!std.mem.eql(u8, a.path(), b.path()));
    try local_fs.writeAll(a.fd.?, "failed");
    a.abort();
    const bytes = try local_fs.readFile(t.allocator, target);
    defer t.allocator.free(bytes);
    try t.expectEqualStrings("old", bytes);
    try t.expectError(error.FileNotFound, local_fs.openRead(a.path()));
    try local_fs.writeAll(b.fd.?, "new");
    try b.commit(t.io);
    const result = try local_fs.readFile(t.allocator, target);
    defer t.allocator.free(result);
    try t.expectEqualStrings("new", result);
    try t.expectError(error.FileNotFound, local_fs.openRead(b.path()));
    var alias: OutputPublication = .{};
    defer alias.abort();
    try t.expectError(error.InputOutputSame, alias.prepare(t.io, target, &.{target}));
}

test "publication owns its target after the caller releases the request string" {
    const t = std.testing;
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    var source_buf: [256]u8 = undefined;
    const source = try std.fmt.bufPrint(&source_buf, ".zig-cache/tmp/{s}/answer", .{tmp.sub_path});
    var expected_buf: [256]u8 = undefined;
    @memcpy(expected_buf[0..source.len], source);
    const expected = expected_buf[0..source.len];
    var publication: OutputPublication = .{};
    defer publication.abort();
    _ = try publication.prepare(t.io, source, &.{});
    @memset(source, 'x');
    try local_fs.writeAll(publication.fd.?, "answer");
    try publication.commit(t.io);
    const bytes = try local_fs.readFile(t.allocator, expected);
    defer t.allocator.free(bytes);
    try t.expectEqualStrings("answer", bytes);
}

test "local publication syncs by default and permits an explicit opt out" {
    const defaults: OutputPublication = .{};
    const opted_out: OutputPublication = .{ .fsync = false };
    try std.testing.expect(defaults.fsync);
    try std.testing.expect(!opted_out.fsync);
}

test "publication reports a post-rename directory sync error and preserves the published target" {
    const t = std.testing;
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const target = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/answer", .{tmp.sub_path});
    for ([_]bool{ false, true }) |replacing| {
        const old = try local_fs.createFile(target);
        try local_fs.writeAll(old, "old");
        local_fs.close(old);
        const stamp = try local_fs.pathStamp(target);
        var p: OutputPublication = .{ .fail_parent_sync = error.InputOutput };
        _ = try p.prepare(t.io, target, &.{});
        try local_fs.writeAll(p.fd.?, "new");
        const result = if (replacing) p.commitReplacing(t.io, stamp) else p.commit(t.io);
        try t.expectError(error.InputOutput, result);
        p.abort();
        const bytes = try local_fs.readFile(t.allocator, target);
        defer t.allocator.free(bytes);
        try t.expectEqualStrings("new", bytes);
        var no_sync: OutputPublication = .{ .fsync = false, .fail_parent_sync = error.InputOutput };
        _ = try no_sync.prepare(t.io, target, &.{});
        try local_fs.writeAll(no_sync.fd.?, "opt-out");
        try no_sync.commit(t.io);
        no_sync.abort();
    }
}
