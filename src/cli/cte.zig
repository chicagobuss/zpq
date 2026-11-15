//! A query with CTEs or derived tables, for the CLI: the stages `sql_cte.zig` planned, each an ordinary query into a
//! scratch Parquet file, and then the user's own query, with their output and flags, over those files.
const std = @import("std");
const zpq = @import("zpq");
const sql_cte = zpq.core.expr.sql_cte;
const setops = @import("setops.zig");

/// The scratch directory a query's stages go in: `.zpq-cte-PID` where a set operation would put its own.
pub fn scratchDir(arena: std.mem.Allocator, sql: []const u8, dir_hint: ?[]const u8) ![]const u8 {
    const windows = @import("builtin").os.tag == .windows;
    const parent = if (windows) try setops.scratchParentWindows(arena, &.{sql}, dir_hint) else setops.scratchParent(&.{sql}, dir_hint);
    const pid: u32 = if (windows) std.os.windows.GetCurrentProcessId() else @bitCast(std.posix.system.getpid());
    const name = try std.fmt.allocPrint(arena, ".zpq-cte-{d}", .{pid});
    return if (windows) setops.windowsScratchJoin(arena, parent, name) else std.fmt.allocPrint(arena, "{s}/{s}", .{ std.mem.trimEnd(u8, parent, "/"), name });
}

pub fn run(gpa: std.mem.Allocator, io: std.Io, plan: sql_cte.Plan, dir: []const u8, stager: setops.Stager) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // Every CTE inlined: nothing to stage.
    if (plan.stages.len == 0) return stager.finish(stager.ptr, plan.sql);
    // The directory's name goes into queries as a quoted path.
    if (std.mem.indexOfAny(u8, dir, "'*\"") != null) {
        stager.say("sql: CTEs and subqueries in FROM keep their scratch files beside the output; its directory name may not contain a quote or a *\n");
        return error.BadArgs;
    }
    try std.Io.Dir.cwd().createDirPath(io, dir);
    defer std.Io.Dir.cwd().deleteTree(io, dir) catch {};

    for (plan.stages) |s| {
        if (s.columns.len == 0) {
            try stager.stage(stager.ptr, s.sql, s.out, true);
            continue;
        }
        // A column list renames the stage's first columns: one pass over its rows, which are a query's answer.
        const raw = try std.fmt.allocPrint(arena, "{s}.raw.parquet", .{s.out});
        try stager.stage(stager.ptr, s.sql, raw, true);
        const cols = try setops.readColumns(arena, raw);
        var select: std.ArrayList(u8) = .empty;
        for (cols, 0..) |c, i| {
            if (i > 0) try select.appendSlice(arena, ", ");
            try select.appendSlice(arena, try setops.quoted(arena, c.name));
            if (i < s.columns.len) try select.print(arena, " AS {s}", .{try setops.quoted(arena, s.columns[i])});
        }
        try stager.stage(stager.ptr, try std.fmt.allocPrint(arena, "SELECT {s} FROM '{s}'", .{ select.items, raw }), s.out, false);
    }
    try stager.finish(stager.ptr, plan.sql);
}
