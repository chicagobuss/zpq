const std = @import("std");
const scan = @import("../core/scan.zig");

/// The sketched count cut (`scan.SketchTopK`) runs wherever it is eligible; `ZPQ_SKETCH_TOPK=0` turns it off,
/// `ZPQ_SKETCH_ADMIT` and `ZPQ_SKETCH_BITS` tune it, and `ZPQ_SKETCH_TRACE=1` reports its decisions on stderr.
pub fn sketchFromEnv(env: std.process.Environ) ?scan.SketchTopK {
    if (envDisables(env, "ZPQ_SKETCH_TOPK")) return null;
    var cfg: scan.SketchTopK = .{};
    if (env.getPosix("ZPQ_SKETCH_ADMIT")) |v| cfg.admit_per_row = std.fmt.parseInt(usize, v, 10) catch cfg.admit_per_row;
    if (env.getPosix("ZPQ_SKETCH_BITS")) |v| cfg.cell_bits = std.math.clamp(std.fmt.parseInt(u6, v, 10) catch cfg.cell_bits, 10, 30);
    cfg.trace = env.getPosix("ZPQ_SKETCH_TRACE") != null;
    cfg.census = !envDisables(env, "ZPQ_SKETCH_CENSUS");
    return cfg;
}

/// Whether `name` is set to "0" in `env`.
pub fn envFlag(env: std.process.Environ, name: []const u8) bool {
    const value = env.getPosix(name) orelse return false;
    return std.mem.eql(u8, value, "1");
}

pub fn envDisables(env: std.process.Environ, name: []const u8) bool {
    const v = env.getPosix(name) orelse return false;
    return std.mem.eql(u8, v, "0");
}

