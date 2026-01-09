const std = @import("std");
const zpq = @import("zpq");
const io = zpq.io.local;
const core = zpq.core.file;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "data/parquet-testing/data/alltypes_plain.parquet";
    std.debug.print("Opening {s}...\n", .{path});

    const file = try std.fs.cwd().openFile(path, .{});
    // Don't defer file.close() here as it's owned by AsyncFileSource/RandomAccessSource

    var source = try io.AsyncFileSource.init(file);
    // source takes ownership of the file handle

    var parquet_file = core.ParquetFile.init(allocator, source.randomAccessSource());
    defer parquet_file.deinit();

    try parquet_file.readFooter();

    std.debug.print("Successfully read Parquet footer!\n", .{});
    std.debug.print("Row Groups: {d}\n", .{parquet_file.numRowGroups()});
    std.debug.print("Total Rows: {d}\n", .{parquet_file.numRows()});
    std.debug.print("Version: {d}\n", .{parquet_file.metadata.version});

    const schema_elems = parquet_file.metadata.schema.items;
    std.debug.print("Schema has {d} elements:\n", .{schema_elems.len});
    for (schema_elems, 0..) |elem, i| {
        std.debug.print("  [{d}] {s} (Type: {?})\n", .{ i, elem.name, elem.type });
    }

    // Explicitly close the source at the end (via the vtable if needed, but here we just own the struct)
    source.randomAccessSource().close();
}
