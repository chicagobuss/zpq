const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        std.debug.print("Usage: {s} inspect <parquet_file>\n", .{args[0]});
        return;
    }

    const command = args[1];
    const file_path = args[2];

    if (std.mem.eql(u8, command, "inspect")) {
        try inspect(allocator, file_path);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
    }
}

fn inspect(allocator: std.mem.Allocator, path: []const u8) !void {
    std.debug.print("Inspecting: {s}\n", .{path});
    
    var pf = try zpq.file.ParquetFile.open(allocator, path);
    defer pf.deinit();

    try pf.readFooter();
    
    if (pf.metadata) |meta| {
        std.debug.print("File Metadata:\n", .{});
        std.debug.print("  Version: {d}\n", .{meta.version});
        std.debug.print("  Rows: {d}\n", .{meta.num_rows});
        if (meta.created_by) |cb| {
            std.debug.print("  Created By: {s}\n", .{cb});
        }
        std.debug.print("  Row Groups: {d}\n", .{meta.row_groups.items.len});
        
        for (meta.row_groups.items, 0..) |rg, i| {
            std.debug.print("  Row Group {d}:\n", .{i});
            std.debug.print("    Total Bytes: {d}\n", .{rg.total_byte_size});
            std.debug.print("    Rows: {d}\n", .{rg.num_rows});
            std.debug.print("    Columns: {d}\n", .{rg.columns.items.len});
            
            for (rg.columns.items, 0..) |col, j| {
                std.debug.print("    Column {d}:\n", .{j});
                if (col.meta_data) |md| {
                    std.debug.print("      Type: {any}\n", .{md.type});
                    std.debug.print("      Codec: {any}\n", .{md.codec});
                    std.debug.print("      Values: {d}\n", .{md.num_values});
                    std.debug.print("      Start Offset: {d}\n", .{col.file_offset});
                    std.debug.print("      Data Page Offset: {d}\n", .{md.data_page_offset});
                    if (md.dictionary_page_offset) |dpo| {
                        std.debug.print("      Dict Page Offset: {d}\n", .{dpo});
                    }
                    
                    var reader = try zpq.column.ColumnReader.init(pf.file, allocator, col);
                    var page_idx: usize = 0;
                    while (try reader.next()) |page| {
                        var p = page;
                        defer p.deinit();
                        std.debug.print("      Page {d}: {any} Size={d} (Compressed={d})\n", 
                            .{page_idx, p.header.type, p.header.uncompressed_page_size, p.header.compressed_page_size});
                            
                        if (p.header.type == .DICTIONARY_PAGE) {
                             var decoder = zpq.decoder.Decoder.init(p.data);
                             std.debug.print("        Dictionary Values:\n", .{});
                             if (md.type == .BYTE_ARRAY) {
                                 while (decoder.hasMore()) {
                                     const val = try decoder.readByteArray();
                                     std.debug.print("          - {s}\n", .{val});
                                 }
                             } else if (md.type == .INT32) {
                                 while (decoder.hasMore()) {
                                     const val = try decoder.readInt32();
                                     std.debug.print("          - {d}\n", .{val});
                                 }
                             }
                        } else if (p.header.type == .DATA_PAGE) {
                            if (p.header.data_page_header) |dph| {
                                std.debug.print("        Encoding: {any}\n", .{dph.encoding});
                                if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                                    if (p.data.len > 0) {
                                        std.debug.print("        Data: {x}\n", .{p.data});
                                        const bit_width = p.data[0];
                                        std.debug.print("        Bit Width: {d}\n", .{bit_width});
                                        var rle_dec = zpq.rle.RleDecoder.init(p.data[1..], bit_width);
                                        std.debug.print("        Indices:\n", .{});
                                        var k: i32 = 0;
                                        var print_count: usize = 0;
                                        while (k < dph.num_values) : (k += 1) {
                                            if (try rle_dec.next()) |idx| {
                                                if (print_count < 20) {
                                                    std.debug.print("          - {d}\n", .{idx});
                                                    print_count += 1;
                                                } else if (print_count == 20) {
                                                    std.debug.print("          ... (more)\n", .{});
                                                    print_count += 1;
                                                }
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        page_idx += 1;
                    }
                }
            }
        }
    }
}
