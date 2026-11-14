//! A query with window functions, for the CLI: the stages `sql_windows.zig` planned. The input query runs into a scratch
//! Parquet file, each pass reads the file before it, sorts a view of the rows for its own PARTITION BY / ORDER BY, and writes
//! one with its columns added (`engine/window.zig`), and the user's own query, with their output and flags, reads the last.
const std = @import("std");
const zpq = @import("zpq");
const wp = zpq.core.expr.window_plan;
const sql_windows = zpq.core.expr.sql_windows;
const setops = @import("setops.zig");
const window = zpq.engine.window;

pub fn run(
    gpa: std.mem.Allocator,
    io: std.Io,
    plan: wp.Plan,
    dir_hint: ?[]const u8,
    stager: setops.Stager,
    max_memory: usize,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const parent = if (@import("builtin").os.tag == .windows)
        try setops.scratchParentWindows(arena, &.{plan.input_sql}, dir_hint)
    else
        setops.scratchParent(&.{plan.input_sql}, dir_hint);
    const pid: u32 = if (@import("builtin").os.tag == .windows) std.os.windows.GetCurrentProcessId() else @bitCast(std.posix.system.getpid());
    const dir = if (@import("builtin").os.tag == .windows)
        try setops.windowsScratchJoin(arena, parent, try std.fmt.allocPrint(arena, ".zpq-window-{d}", .{pid}))
    else
        try std.fmt.allocPrint(arena, "{s}/.zpq-window-{d}", .{ std.mem.trimEnd(u8, parent, "/"), pid });
    // The directory's name goes into queries as a quoted path.
    if (std.mem.indexOfAny(u8, dir, "'*\"") != null) {
        stager.say("sql: window functions keep their scratch files beside the output; its directory name may not contain a quote or a *\n");
        return error.BadArgs;
    }
    try std.Io.Dir.cwd().createDirPath(io, dir);
    defer std.Io.Dir.cwd().deleteTree(io, dir) catch {};

    const trace = std.c.getenv("ZPQ_WINDOW_TRACE") != null;
    var current = if (@import("builtin").os.tag == .windows)
        try setops.windowsScratchJoin(arena, dir, "input.parquet")
    else
        try std.fmt.allocPrint(arena, "{s}/input.parquet", .{dir});
    if (trace) stager.say(try std.fmt.allocPrint(arena, "window input: {s}\n", .{plan.input_sql}));
    try stager.stage(stager.ptr, plan.input_sql, current, true);
    // Each pass sorts the rows for itself and writes them back in the order it read them, so the rows leave as the input
    // query had them.
    for (plan.passes, 0..) |pass, i| {
        const out = if (@import("builtin").os.tag == .windows)
            try setops.windowsScratchJoin(arena, dir, try std.fmt.allocPrint(arena, "pass{d}.parquet", .{i}))
        else
            try std.fmt.allocPrint(arena, "{s}/pass{d}.parquet", .{ dir, i });
        try window.run(gpa, .{ .input = current, .output = out, .pass = pass, .max_memory = max_memory });
        current = out;
    }
    const where = if (plan.final_where.len > 0) try std.fmt.allocPrint(arena, " WHERE {s}", .{plan.final_where}) else "";
    const sql = try std.fmt.allocPrint(arena, "SELECT {s} FROM '{s}'{s} {s}", .{ plan.final_items, current, where, plan.final_tail });
    if (trace) stager.say(try std.fmt.allocPrint(arena, "window final: {s}\n", .{sql}));
    try stager.finish(stager.ptr, sql);
}
