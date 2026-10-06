//! Output-label benchmark: times `metadata.leafLabels` on wide schemas, ordinary and with colliding names.
//!
//! The unit tests check the labels; this checks they stay linear in the schema whatever the names do. Labelling leaf
//! by leaf walks the schema per leaf, which on these shapes takes seconds to minutes.
//!
//! Usage: `zig build bench-labels -Doptimize=ReleaseFast`. Prints one TSV row per shape (`shape  leaves  min_ms`)
//! and fails when any shape's best run takes longer than `max_ms`.

const std = @import("std");
const zpq = @import("zpq");
const metadata = zpq.core.parquet.metadata;
const nowMonoNs = zpq.clock.monoNs;

pub fn main(init: std.process.Init) !void {
    const structs = 10_000;
    // Generous for any build mode: linear labelling takes milliseconds, per-leaf walks seconds and up.
    const max_ms = 2_000;
    const runs = 5;

    var slow = false;
    std.debug.print("shape\tleaves\tmin_ms\n", .{});
    for (std.enums.values(metadata.WideShape)) |shape| {
        var arena_state = std.heap.ArenaAllocator.init(init.gpa);
        defer arena_state.deinit();
        const meta = try metadata.wideSchemaForTest(arena_state.allocator(), structs, shape);
        var best: u64 = std.math.maxInt(u64);
        var leaves: usize = 0;
        for (0..runs) |_| {
            var run_arena = std.heap.ArenaAllocator.init(init.gpa);
            defer run_arena.deinit();
            const start = nowMonoNs();
            leaves = (try metadata.leafLabels(run_arena.allocator(), &meta)).names.len;
            best = @min(best, @as(u64, @intCast(nowMonoNs() - start)));
        }
        const ms = best / std.time.ns_per_ms;
        std.debug.print("{s}\t{d}\t{d}\n", .{ @tagName(shape), leaves, ms });
        if (ms > max_ms) slow = true;
    }
    if (slow) {
        std.debug.print("labelling took longer than {d} ms\n", .{max_ms});
        std.process.exit(1);
    }
}
