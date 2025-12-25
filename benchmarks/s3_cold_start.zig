const std = @import("std");
const zpq = @import("zpq");
const ParquetFile = zpq.file.ParquetFile;

pub const std_options = std.Options{
    .log_level = .info,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .s3_source, .level = .warn },
        .{ .scope = .tls, .level = .warn },
    },
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = "127.0.0.1";
    const bucket = "zpq-ci";
    const key = "simple.parquet";
    const region = "us-east-1";
    const port = 9000;

    var timer = try std.time.Timer.start();

    var file = try ParquetFile.openS3(allocator, host, bucket, key, region, true, port);
    defer file.deinit();
    
    const open_time = timer.read();
    
    try file.readFooter();
    const footer_time = timer.read();

    var rg = try file.rowGroup(0);
    defer rg.deinit();

    // Prefetch all columns (serial right now)
    try rg.prefetch(null);
    const prefetch_time = timer.read();

    std.debug.print("Baseline S3 Cold Start (Serial):\n", .{});
    std.debug.print("  Open (HEAD):     {d:9.3}ms\n", .{@as(f64, @floatFromInt(open_time)) / 1e6});
    std.debug.print("  Footer (GET):    {d:9.3}ms\n", .{@as(f64, @floatFromInt(footer_time - open_time)) / 1e6});
    std.debug.print("  Prefetch (GETs): {d:9.3}ms\n", .{@as(f64, @floatFromInt(prefetch_time - footer_time)) / 1e6});
    std.debug.print("  Total:           {d:9.3}ms\n", .{@as(f64, @floatFromInt(prefetch_time)) / 1e6});
}

