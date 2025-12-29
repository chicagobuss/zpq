const std = @import("std");
const zpq = @import("zpq");
const rle = zpq.rle;
const ParquetFile = zpq.file.ParquetFile;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var file = try ParquetFile.open(allocator, "ci/fixtures/parquet/filter_test.parquet");
    defer file.deinit();

    try file.readFooter();

    var rg_reader = try file.rowGroup(0);
    defer rg_reader.deinit();

    var col_reader = try rg_reader.columnReader(1);

    // Skip dictionary page (leak is fine for test)
    var dict_page = (try col_reader.next(allocator)).?;
    defer dict_page.deinit(allocator);

    // Get data page
    var page = (try col_reader.next(allocator)).?;
    defer page.deinit(allocator);

    // Skip def levels: 4-byte length + data
    const def_len = std.mem.readInt(u32, page.data[0..4], .little);
    const offset = 4 + def_len + 1; // +1 for bit_width byte

    var dec = rle.RleDecoder.init(page.data[offset..], 1);

    // Decode all 10000 values
    var target_count: usize = 0;
    var i: usize = 0;
    while (i < 10000) : (i += 1) {
        const v = try dec.next() orelse break;
        if (v == 1) target_count += 1;
    }

    std.debug.print("Decoded {d} values, {d} targets (expected ~2031)\n", .{ i, target_count });
}
