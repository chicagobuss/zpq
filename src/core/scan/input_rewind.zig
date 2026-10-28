const std = @import("std");
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const Input = scan_types.Input;
const allRescannable = scan_types.allRescannable;

/// A joined attempt may have returned every gated row group's pages. Replays read the stable mappings,
/// never the reader's ready-but-released window. Allocate the view before abandoning any reader state.
pub fn rewindInputs(arena: std.mem.Allocator, args: *MultiAggArgs) !void {
    if (!allRescannable(args.inputs)) return error.ExceededMemoryBudget;
    const inputs = try arena.dupe(Input, args.inputs);
    for (inputs) |*input| if (input.plan_bytes) |b| {
        input.bytes = b;
        input.gate = null;
        input.dict_wait = null;
    };
    if (args.start_reads) |s| if (s.abandon) |abandon| abandon(s.ctx);
    args.inputs = inputs;
    args.start_reads = null;
    args.dict_probe = null;
}

