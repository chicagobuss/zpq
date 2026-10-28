const std = @import("std");

pub fn current(b: *std.Build) []const u8 {
    // Git observes refs, the index and tracked worktree files, including linked worktrees and packed refs.
    // Those inputs cannot be represented by a dependency on just .git/HEAD in the configuration cache.
    b.graph.poisonCache();
    return switch (b.runFallible(&.{ "git", "describe", "--always", "--dirty", "--exclude=*", "--abbrev=12" }, .{
        .cwd = .{ .path = b.root.toString(b.allocator) catch @panic("OOM") },
        .stderr_behavior = .ignore,
    })) {
        .success => |revision| std.mem.trim(u8, revision, "\r\n"),
        else => "unknown",
    };
}
