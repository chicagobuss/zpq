const std = @import("std");
const zpq = @import("zpq");
const ParquetFile = zpq.file.ParquetFile;

pub const std_options = std.Options{
    .log_level = .debug,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .s3_source, .level = .debug },
        .{ .scope = .tls, .level = .debug },
    },
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = std.process.getEnvVarOwned(allocator, "S3_HOST") catch |err| if (err == error.EnvironmentVariableNotFound) try allocator.dupe(u8, "127.0.0.1") else return err;
    defer allocator.free(host);
    const bucket = std.process.getEnvVarOwned(allocator, "S3_BUCKET") catch |err| if (err == error.EnvironmentVariableNotFound) try allocator.dupe(u8, "zpq-ci") else return err;
    defer allocator.free(bucket);
    const key = std.process.getEnvVarOwned(allocator, "S3_KEY") catch |err| if (err == error.EnvironmentVariableNotFound) try allocator.dupe(u8, "simple.parquet") else return err;
    defer allocator.free(key);
    const region = std.process.getEnvVarOwned(allocator, "S3_REGION") catch |err| if (err == error.EnvironmentVariableNotFound) try allocator.dupe(u8, "us-east-1") else return err;
    defer allocator.free(region);
    
    const port_str = std.process.getEnvVarOwned(allocator, "S3_PORT") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    defer if (port_str) |s| allocator.free(s);
    const port: u16 = if (port_str) |s| try std.fmt.parseInt(u16, s, 10) else 9000;

    var timer = try std.time.Timer.start();

    var file = try ParquetFile.openS3(allocator, host, bucket, key, region, true, port);
    defer file.deinit();

    // Set credentials if available
    if (std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID")) |ak| {
        defer allocator.free(ak);
        if (std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY")) |sk| {
            defer allocator.free(sk);
            const st = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
            defer if (st) |token| allocator.free(token);
            
            const s3_src: *zpq.s3.XevS3Source = @ptrCast(@alignCast(file.cleanup_context.?));
            try s3_src.setCredentials(ak, sk, st);
            std.debug.print("Using SigV4 credentials from environment\n", .{});
        } else |_| {}
    } else |_| {}
    
    const open_time = timer.read();
    
    try file.readFooter();
    const footer_time = timer.read();

    var rg = try file.rowGroup(0);
    defer rg.deinit();

    // Prefetch all columns (serial right now)
    try rg.prefetch(null);
    const prefetch_time = timer.read();

    std.debug.print("Baseline S3 Cold Start (Iteration 1 - Cold):\n", .{});
    std.debug.print("  Open (HEAD):     {d:9.3}ms\n", .{@as(f64, @floatFromInt(open_time)) / 1e6});
    std.debug.print("  Footer (GET):    {d:9.3}ms\n", .{@as(f64, @floatFromInt(footer_time - open_time)) / 1e6});
    std.debug.print("  Prefetch (GETs): {d:9.3}ms\n", .{@as(f64, @floatFromInt(prefetch_time - footer_time)) / 1e6});
    std.debug.print("  Total:           {d:9.3}ms\n", .{@as(f64, @floatFromInt(prefetch_time)) / 1e6});

    // Iteration 2 - Warm (Pooled)
    timer.reset();
    var file2 = try ParquetFile.openS3(allocator, host, bucket, key, region, true, port);
    defer file2.deinit();
    const open_time2 = timer.read();
    
    try file2.readFooter();
    const footer_time2 = timer.read();

    var rg2 = try file2.rowGroup(0);
    defer rg2.deinit();
    try rg2.prefetch(null);
    const prefetch_time2 = timer.read();

    std.debug.print("\nS3 Cold Start (Iteration 2 - Warm/Pooled):\n", .{});
    std.debug.print("  Open (HEAD):     {d:9.3}ms (REUSED)\n", .{@as(f64, @floatFromInt(open_time2)) / 1e6});
    std.debug.print("  Footer (GET):    {d:9.3}ms (REUSED)\n", .{@as(f64, @floatFromInt(footer_time2 - open_time2)) / 1e6});
    std.debug.print("  Prefetch (GETs): {d:9.3}ms (REUSED)\n", .{@as(f64, @floatFromInt(prefetch_time2 - footer_time2)) / 1e6});
    std.debug.print("  Total:           {d:9.3}ms\n", .{@as(f64, @floatFromInt(prefetch_time2)) / 1e6});
}

