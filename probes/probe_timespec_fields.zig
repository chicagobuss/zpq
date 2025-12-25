const std = @import("std");

/// Investigates the naming of fields in `std.posix.timespec` across platforms.
/// Found:
/// - macOS (Darwin): `sec`, `nsec`
/// - Linux: `tv_sec`, `tv_nsec`
pub fn main() !void {
    const ts: std.posix.timespec = undefined;
    const info = @typeInfo(@TypeOf(ts));
    std.debug.print("posix.timespec fields on this platform:\n", .{});
    inline for (info.@"struct".fields) |field| {
        std.debug.print("- {s}\n", .{field.name});
    }
}
