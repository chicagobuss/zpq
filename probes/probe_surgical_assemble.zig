//! Probe: Test surgical engine assembly
//!
//! Tests that the surgical engine correctly:
//! 1. Plans minimal page fetches
//! 2. Fetches only required pages
//! 3. Assembles into contiguous buffers
//! 4. Produces valid RowGroupData for RowGroupWorker

const std = @import("std");
const zpq = @import("zpq");

const SurgicalEngine = zpq.core.surgical.engine.SurgicalEngine;
const ParquetFile = zpq.core.file.ParquetFile;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const input_path = "data/benchmark/benchmark_100mb.parquet";

    std.debug.print("=== Surgical Engine Assembly Test ===\n\n", .{});

    // Open file
    var pf = try ParquetFile.openMmap(allocator, input_path);
    defer pf.deinit();

    try pf.readFooter();
    const meta = pf.metadata.?;

    std.debug.print("File: {s}\n", .{input_path});
    std.debug.print("Row groups: {d}\n", .{meta.row_groups.items.len});
    std.debug.print("Total rows: {d}\n\n", .{meta.num_rows});

    // Test with string_sorted column which has multiple pages
    const filter_col = "string_sorted";
    const filter_value = "row_0000400000";
    const output_cols = &[_][]const u8{ "string_sorted", "int32_sorted" };

    std.debug.print("Filter: {s} = {s}\n", .{ filter_col, filter_value });
    std.debug.print("Output columns: {s}, {s}\n\n", .{ output_cols[0], output_cols[1] });

    // Run surgical engine
    var engine = SurgicalEngine.init(
        allocator,
        &pf,
        filter_col,
        filter_value,
        output_cols,
    );

    std.debug.print("--- Phase 1: Execute (plan + fetch) ---\n", .{});
    const result = try engine.execute();
    std.debug.print("Input rows: {d}\n", .{result.input_rows});
    std.debug.print("Bytes fetched: {d}\n", .{result.bytes_fetched});
    std.debug.print("Bytes full scan: {d}\n", .{result.bytes_full_scan});
    std.debug.print("Pages: {d}/{d} (skipped {d})\n", .{
        result.pages_total - result.pages_skipped,
        result.pages_total,
        result.pages_skipped,
    });
    std.debug.print("Savings: {d:.1}%\n\n", .{
        100.0 * (1.0 - @as(f64, @floatFromInt(result.bytes_fetched)) / @as(f64, @floatFromInt(result.bytes_full_scan))),
    });

    std.debug.print("--- Phase 2: Execute and Assemble ---\n", .{});
    var surgical_result = try engine.executeAndAssemble();
    defer surgical_result.deinit(allocator);

    std.debug.print("Active row groups: {d}\n", .{surgical_result.row_groups.len});

    for (surgical_result.row_groups, 0..) |*rg, i| {
        std.debug.print("\nRow group {d} (original idx {d}):\n", .{ i, rg.rg_idx });
        std.debug.print("  num_rows: {d}\n", .{rg.num_rows});
        std.debug.print("  filter buffer size: {d}\n", .{rg.filter.buffer.len});
        std.debug.print("  filter data_offset: {d}\n", .{rg.filter.data_offset});

        for (rg.outputs, 0..) |*out, j| {
            std.debug.print("  output[{d}] buffer size: {d}, data_offset: {d}\n", .{
                j,
                out.buffer.len,
                out.data_offset,
            });
        }

        // Convert to RowGroupData and verify
        std.debug.print("  Converting to RowGroupData...\n", .{});
        var rg_data = try rg.toRowGroupData(allocator);
        defer {
            allocator.free(rg_data.output_bufs);
            allocator.free(@constCast(rg_data.output_offsets));
            allocator.free(@constCast(rg_data.output_chunks));
        }

        std.debug.print("  RowGroupData:\n", .{});
        std.debug.print("    filter_buf.len: {d}\n", .{rg_data.filter_buf.len});
        std.debug.print("    filter_offset: {d}\n", .{rg_data.filter_offset});
        std.debug.print("    output_bufs.len: {d}\n", .{rg_data.output_bufs.len});
    }

    std.debug.print("\n=== Test Complete ===\n", .{});
}
