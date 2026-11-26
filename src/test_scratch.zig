//! Test scratch follows the environment without moving compiler caches or installed binaries.
const std = @import("std");
const t = std.testing;

pub const TmpDir = struct {
    dir: std.Io.Dir,
    parent_dir: std.Io.Dir,
    sub_path: [16]u8,
    path: []const u8,

    pub fn cleanup(self: *TmpDir) void {
        self.dir.close(t.io);
        self.parent_dir.deleteTree(t.io, &self.sub_path) catch {};
        self.parent_dir.close(t.io);
        t.allocator.free(self.path);
        self.* = undefined;
    }
};

/// The scratch root's default, one component at a time: a `/` spelled into one would meet the platform's separator in
/// the path built under it, and Windows cannot open a verbatim (`\\?\`) path that mixes the two.
pub const default_root = [_][]const u8{ ".zig-cache", "tmp" };

fn scratchRoot(a: std.mem.Allocator, base: ?[]const u8) ![]u8 {
    return std.fs.path.join(a, if (base) |p| &.{ p, "tmp" } else &default_root);
}

pub fn tmpDir(opts: std.Io.Dir.OpenOptions) TmpDir {
    const base = t.environ.getAlloc(t.allocator, "ZPQ_TEST_PARQUET_DIR") catch |err| switch (err) {
        error.EnvironmentVariableMissing => null,
        else => @panic("cannot read ZPQ_TEST_PARQUET_DIR"),
    };
    defer if (base) |p| t.allocator.free(p);
    var random: [12]u8 = undefined;
    t.io.random(&random);
    var name: [16]u8 = undefined;
    _ = std.base64.url_safe.Encoder.encode(&name, &random);
    const root = scratchRoot(t.allocator, base) catch @panic("OOM");
    defer t.allocator.free(root);
    const parent = std.Io.Dir.cwd().createDirPathOpen(t.io, root, .{}) catch @panic("cannot open test scratch");
    const dir = parent.createDirPathOpen(t.io, &name, .{ .open_options = opts }) catch @panic("cannot create test scratch");
    return .{
        .dir = dir,
        .parent_dir = parent,
        .sub_path = name,
        .path = std.fs.path.join(t.allocator, &.{ root, &name }) catch @panic("OOM"),
    };
}

pub fn filePath(a: std.mem.Allocator, default: []const u8) ![]const u8 {
    const base = t.environ.getAlloc(a, "ZPQ_TEST_PARQUET_DIR") catch |err| switch (err) {
        error.EnvironmentVariableMissing => return a.dupe(u8, default),
        else => return err,
    };
    defer a.free(base);
    try std.Io.Dir.cwd().createDirPath(t.io, base);
    return std.fs.path.join(a, &.{ base, std.fs.path.basename(default) });
}
