const std = @import("std");

pub fn main() !void {
    const path = "test_mmap_probe.tmp";
    const data = "Mmap Probe Data: " ** 100;
    try std.fs.cwd().writeFile(.{ .sub_path = path, .data = data });
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const stat = try file.stat();
    
    // In Zig 0.16.dev, mmap is in std.posix
    const ptr = try std.posix.mmap(
        null,
        stat.size,
        std.posix.PROT.READ,
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );
    defer std.posix.munmap(ptr);

    std.debug.print("Mmap successful. Size: {d}\n", .{ptr.len});
    std.debug.print("Content start: {s}\n", .{ptr[0..20]});

    if (std.mem.eql(u8, ptr[0..data.len], data)) {
        std.debug.print("Data matches!\n", .{});
    } else {
        std.debug.print("Data mismatch!\n", .{});
        return error.DataMismatch;
    }
}
