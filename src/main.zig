const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");
const builtin = @import("builtin");

const query = @import("query.zig");
const lambda = @import("lambda.zig");

const Pipeline = zpq.core.pipeline.Pipeline;
const ExecutionMode = zpq.core.pipeline.ExecutionMode;
const QueryParams = query.QueryParams;

pub const std_options: std.Options = .{
    .log_level = if (builtin.mode == .Debug) .debug else .warn,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Priority 1: Lambda mode (hot path - check first)
    // Debug: print if we see the Lambda env var
    if (std.posix.getenv("AWS_LAMBDA_RUNTIME_API")) |runtime_api| {
        std.debug.print("zpq: detected AWS_LAMBDA_RUNTIME_API={s}\n", .{runtime_api});
        return lambda.run(allocator, runtime_api);
    } else {
        // Check if we're in Lambda context (LAMBDA_TASK_ROOT exists) but missing runtime API
        if (std.posix.getenv("LAMBDA_TASK_ROOT")) |_| {
            std.debug.print("zpq: LAMBDA_TASK_ROOT set but AWS_LAMBDA_RUNTIME_API missing\n", .{});
        }
    }

    // Priority 2: HTTP server mode (check for --serve flag)
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (getServePort(args)) |port| {
        _ = port;
        // TODO: Implement HTTP server mode
        std.debug.print("HTTP server mode not yet implemented\n", .{});
        return;
    }

    // Priority 3: CLI mode (convenience/dev)
    return cliMain(allocator, args);
}

/// Check for --serve flag and return port if present
fn getServePort(args: []const []const u8) ?u16 {
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--serve")) {
            i += 1;
            if (i < args.len) {
                return std.fmt.parseInt(u16, args[i], 10) catch 8080;
            }
            return 8080; // Default port
        }
    }
    return null;
}

// =============================================================================
// CLI Mode
// =============================================================================

const CliArgs = struct {
    input: ?[]const u8 = null,
    output: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    select: ?[]const u8 = null,
    show_schema: bool = false,
    show_meta: bool = false,
    mode: ExecutionMode = .slot_parallel,
};

fn parseCliArgs(args: []const []const u8) ?CliArgs {
    if (args.len < 2) return null;

    var result = CliArgs{};
    var i: usize = 1;

    while (i < args.len) : (i += 1) {
        const arg = args[i];

        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return null;
        } else if (std.mem.eql(u8, arg, "--schema")) {
            result.show_schema = true;
        } else if (std.mem.eql(u8, arg, "--meta")) {
            result.show_meta = true;
        } else if (std.mem.eql(u8, arg, "--sequential")) {
            result.mode = .sequential;
        } else if (std.mem.eql(u8, arg, "--parallel")) {
            result.mode = .parallel;
        } else if (std.mem.eql(u8, arg, "--serve")) {
            // Skip --serve and its argument (handled above)
            i += 1;
        } else if (std.mem.eql(u8, arg, "--filter") or std.mem.eql(u8, arg, "-f")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --filter requires a value\n", .{});
                return null;
            }
            result.filter = args[i];
        } else if (std.mem.eql(u8, arg, "--select") or std.mem.eql(u8, arg, "-s")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --select requires a value\n", .{});
                return null;
            }
            result.select = args[i];
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --output requires a value\n", .{});
                return null;
            }
            result.output = args[i];
        } else if (!std.mem.startsWith(u8, arg, "-")) {
            // Positional argument
            if (result.input == null) {
                result.input = arg;
            } else if (result.output == null) {
                result.output = arg;
            }
        } else {
            std.debug.print("Error: Unknown option '{s}'\n", .{arg});
            return null;
        }
    }

    return result;
}

fn printUsage(exe: []const u8) void {
    std.debug.print(
        \\Usage: {s} <input> [output] [options]
        \\
        \\  Parquet query and transform tool.
        \\
        \\Arguments:
        \\  input              Input parquet file (local path or s3://)
        \\  output             Output parquet file (optional, for filter/transform)
        \\
        \\Operations:
        \\  --schema           Print file schema
        \\  --meta             Print file metadata
        \\  -f, --filter EXPR  Filter rows (e.g., category=A)
        \\  -s, --select COLS  Select columns (comma-separated)
        \\
        \\Execution Mode:
        \\  --sequential       Process row groups sequentially
        \\  --parallel         Parallel processing (default: slot-parallel)
        \\
        \\Server Mode:
        \\  --serve [PORT]     Run as HTTP server (default port: 8080)
        \\
        \\Output:
        \\  -o, --output FILE  Output file (alternative to positional)
        \\
        \\Examples:
        \\  {s} data.parquet --schema
        \\  {s} data.parquet --meta
        \\  {s} input.parquet output.parquet --filter category=A
        \\  {s} input.parquet -o out.parquet --filter id>100 --select id,name
        \\  {s} s3://bucket/data.parquet --schema
        \\  {s} --serve 8080
        \\
        \\Environment:
        \\  AWS_LAMBDA_RUNTIME_API  Auto-detected for Lambda mode
        \\
    , .{ exe, exe, exe, exe, exe, exe, exe });
}

fn cliMain(allocator: std.mem.Allocator, args: []const []const u8) !void {
    const parsed = parseCliArgs(args) orelse {
        printUsage(args[0]);
        return;
    };

    const input = parsed.input orelse {
        std.debug.print("Error: Input file required\n", .{});
        printUsage(args[0]);
        return;
    };

    // Initialize xev runtime
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    // Convert CLI args to QueryParams
    const params = QueryParams{
        .input = input,
        .output = parsed.output,
        .filter = parsed.filter,
        .select = parsed.select,
        .mode = parsed.mode,
        .show_schema = parsed.show_schema,
        .show_meta = parsed.show_meta,
    };

    // For schema/meta, use the print methods (CLI-friendly output)
    if (parsed.show_schema) {
        var pipeline = Pipeline.init(allocator);
        defer pipeline.deinit();
        pipeline.setInput(input);
        pipeline.setRuntime(&loop, &thread_pool);
        try pipeline.printSchema();
        return;
    }

    if (parsed.show_meta) {
        var pipeline = Pipeline.init(allocator);
        defer pipeline.deinit();
        pipeline.setInput(input);
        pipeline.setRuntime(&loop, &thread_pool);
        try pipeline.printMeta();
        return;
    }

    // For filter/transform, use executeQuery
    if (parsed.filter != null) {
        if (parsed.output == null) {
            std.debug.print("Error: --filter requires an output file\n", .{});
            return;
        }

        const result = try query.executeQuery(allocator, &loop, &thread_pool, params);

        if (result.error_message) |err| {
            std.debug.print("Error: {s}\n", .{err});
            return;
        }

        std.debug.print("\nComplete:\n", .{});
        std.debug.print("  Input:  {d} rows\n", .{result.input_rows});
        std.debug.print("  Output: {d} rows\n", .{result.output_rows});
        std.debug.print("  Time:   {d:.1}ms\n", .{result.elapsed_ms});
        return;
    }

    // Default: print summary
    var pipeline = Pipeline.init(allocator);
    defer pipeline.deinit();
    pipeline.setInput(input);
    pipeline.setRuntime(&loop, &thread_pool);
    try pipeline.printSummary();
}
