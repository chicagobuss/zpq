//! A query with window functions, for the CLI: the stages `sql_windows.zig` planned. The input query runs into a scratch
//! Parquet file (ordered by the first pass's PARTITION BY / ORDER BY), each pass reads the file before it and writes
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

    const parent = setops.scratchParent(&.{plan.input_sql}, dir_hint);
    const pid: u32 = @bitCast(std.posix.system.getpid());
    const dir = try std.fmt.allocPrint(arena, "{s}/.zpq-window-{d}", .{ std.mem.trimEnd(u8, parent, "/"), pid });
    // The directory's name goes into queries as a quoted path.
    if (std.mem.indexOfAny(u8, dir, "'*\"") != null) {
        stager.say("sql: window functions keep their scratch files beside the output; its directory name may not contain a quote or a *\n");
        return error.BadArgs;
    }
    try std.Io.Dir.cwd().createDirPath(io, dir);
    defer std.Io.Dir.cwd().deleteTree(io, dir) catch {};

    const trace = std.c.getenv("ZPQ_WINDOW_TRACE") != null;
    var current = try std.fmt.allocPrint(arena, "{s}/input.parquet", .{dir});
    if (trace) stager.say(try std.fmt.allocPrint(arena, "window input: {s}\n", .{plan.input_sql}));
    try stager.stage(stager.ptr, plan.input_sql, current, true);
    for (plan.passes, 0..) |pass, i| {
        if (i > 0 and pass.sort.len > 0) {
            const sorted = try std.fmt.allocPrint(arena, "{s}/sort{d}.parquet", .{ dir, i });
            const sql = try std.fmt.allocPrint(arena, "SELECT * FROM '{s}' ORDER BY {s}", .{ current, try sql_windows.sortText(arena, pass.sort) });
            if (trace) stager.say(try std.fmt.allocPrint(arena, "window sort: {s}\n", .{sql}));
            try stager.stage(stager.ptr, sql, sorted, false);
            current = sorted;
        }
        const out = try std.fmt.allocPrint(arena, "{s}/pass{d}.parquet", .{ dir, i });
        try window.run(gpa, .{ .input = current, .output = out, .pass = pass, .max_memory = max_memory });
        current = out;
    }
    const sql = try std.fmt.allocPrint(arena, "SELECT {s} FROM '{s}' {s}", .{ plan.final_items, current, plan.final_tail });
    if (trace) stager.say(try std.fmt.allocPrint(arena, "window final: {s}\n", .{sql}));
    try stager.finish(stager.ptr, sql);
}
