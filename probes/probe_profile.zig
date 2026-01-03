const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const input_path = "/tmp/bench_input.parquet";
    const output_path = "/tmp/probe_out.parquet";

    // Warm up
    {
        var pipeline = zpq.pipeline.Pipeline.init(allocator);
        defer pipeline.deinit();
        try pipeline.setInput(input_path);
        try pipeline.setFilter("category=A");
        try pipeline.setOutput(output_path);
        _ = try pipeline.execute(.slot_parallel);
    }

    // Timed run with breakdown
    var total_timer = try std.time.Instant.now();

    var pipeline = zpq.pipeline.Pipeline.init(allocator);
    defer pipeline.deinit();

    var t1 = try std.time.Instant.now();
    try pipeline.setInput(input_path);
    var t2 = try std.time.Instant.now();
    const setup_ns = t2.since(t1);

    t1 = try std.time.Instant.now();
    try pipeline.setFilter("category=A");
    t2 = try std.time.Instant.now();
    const filter_parse_ns = t2.since(t1);

    t1 = try std.time.Instant.now();
    try pipeline.setOutput(output_path);
    t2 = try std.time.Instant.now();
    const output_setup_ns = t2.since(t1);

    t1 = try std.time.Instant.now();
    const result = try pipeline.execute(.slot_parallel);
    t2 = try std.time.Instant.now();
    const execute_ns = t2.since(t1);

    const total_ns = t2.since(total_timer);

    std.debug.print("\n=== Pipeline Profile ===\n", .{});
    std.debug.print("Setup input:    {d:>8.2}ms\n", .{@as(f64, @floatFromInt(setup_ns)) / 1_000_000.0});
    std.debug.print("Parse filter:   {d:>8.2}ms\n", .{@as(f64, @floatFromInt(filter_parse_ns)) / 1_000_000.0});
    std.debug.print("Setup output:   {d:>8.2}ms\n", .{@as(f64, @floatFromInt(output_setup_ns)) / 1_000_000.0});
    std.debug.print("Execute:        {d:>8.2}ms\n", .{@as(f64, @floatFromInt(execute_ns)) / 1_000_000.0});
    std.debug.print("------------------------\n", .{});
    std.debug.print("Total:          {d:>8.2}ms\n", .{@as(f64, @floatFromInt(total_ns)) / 1_000_000.0});
    std.debug.print("\nRows: {d} -> {d}\n", .{result.rows_read, result.rows_written});
}
