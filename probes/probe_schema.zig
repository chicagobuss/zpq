const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // skip exe name
    const path = args.next() orelse {
        std.debug.print("Usage: probe_schema <path>\n", .{});
        return;
    };

    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    var source = try zpq.io.local.AsyncFileSource.init(file);
    var pfile = zpq.core.file.ParquetFile.init(allocator, source.randomAccessSource());
    try pfile.readFooter();
    defer pfile.deinit();

    std.debug.print("File: {s}\n", .{path});
    std.debug.print("Rows: {d}\n", .{pfile.metadata.num_rows});
    std.debug.print("Columns:\n", .{});

    for (pfile.metadata.schema.items, 0..) |elem, i| {
        if (i == 0) continue; // skip root
        const type_str = if (elem.type) |t| @tagName(t) else "group";
        std.debug.print("  [{d}] {s}: {s}\n", .{ i - 1, elem.name, type_str });
    }
}
