const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var sel = zpq.core.simd.SelectionVector.init();
    std.debug.print("SelectionVector count: {d}\n", .{sel.count()});
}

