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
    compression: zpq.core.schema.CompressionCodec = .SNAPPY,
    input_format: ?[]const u8 = null,
    output_format: ?[]const u8 = null,
};

fn parseCliArgs(args: []const []const u8) ?CliArgs {
    var result = CliArgs{};
    var positionals = std.ArrayList([]const u8){}; // Unmanaged init
    const allocator = std.heap.page_allocator;
    defer positionals.deinit(allocator);

    var i: usize = 1;

    // Scan for flags and build positional list
    while (i < args.len) : (i += 1) {
        const arg = args[i];

        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return null;
        } else if (std.mem.eql(u8, arg, "--schema")) {
            result.show_schema = true;
        } else if (std.mem.eql(u8, arg, "--meta")) {
            result.show_meta = true;
        } else if (std.mem.eql(u8, arg, "--surgical")) {
            result.mode = .surgical;
        } else if (std.mem.eql(u8, arg, "--serve")) {
            // Handled in main() - skip
            continue;
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
        } else if (std.mem.eql(u8, arg, "--infmt")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --infmt requires a value (parquet, csv, json)\n", .{});
                return null;
            }
            result.input_format = args[i];
        } else if (std.mem.eql(u8, arg, "--outfmt")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --outfmt requires a value (parquet, csv, json)\n", .{});
                return null;
            }
            result.output_format = args[i];
        } else if (std.mem.eql(u8, arg, "--compression") or std.mem.eql(u8, arg, "-c")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --compression requires a value (none, snappy, zstd, gzip)\n", .{});
                return null;
            }
            const comp_str = args[i];
            if (std.mem.eql(u8, comp_str, "none") or std.mem.eql(u8, comp_str, "uncompressed")) {
                result.compression = .UNCOMPRESSED;
            } else if (std.mem.eql(u8, comp_str, "snappy")) {
                result.compression = .SNAPPY;
            } else if (std.mem.eql(u8, comp_str, "zstd")) {
                result.compression = .ZSTD;
            } else if (std.mem.eql(u8, comp_str, "gzip")) {
                result.compression = .GZIP;
            } else {
                std.debug.print("Error: Unknown compression '{s}'. Use: none, snappy, zstd, gzip\n", .{comp_str});
                return null;
            }
        } else if (std.mem.startsWith(u8, arg, "-")) {
            // Treat as flag unless exactly "--" is handled as positional special case?
            // But "--" is standard end-of-options?
            // Use case: zpq input -- -> "--" is output
            if (std.mem.eql(u8, arg, "--")) {
                positionals.append(allocator, arg) catch return null;
            } else {
                std.debug.print("Error: Unknown option '{s}'\n", .{arg});
                return null;
            }
        } else {
            // Positional argument
            positionals.append(allocator, arg) catch return null;
        }
    }

    if (positionals.items.len == 1) {
        // Inspect Mode: <input>
        result.input = positionals.items[0];
        // Default to summary if neither schema/meta specified
    } else if (positionals.items.len == 2) {
        // Transform Mode: <input> <output>
        result.input = positionals.items[0];
        result.output = positionals.items[1];
    } else {
        // 0 or >2 args -> invalid (unless handled by --serve which we checked for help but main handles priority)
        // Actually main() handles --serve priority *before* calling cliMain.
        // So here we strictly enforce 1 or 2 positionals.
        return null;
    }

    return result;
}

fn printUsage(exe: []const u8) void {
    std.debug.print(
        \\Usage:
        \\  {s} <input>                   (Inspect/Summary)
        \\  {s} <input> <output>          (Transform/Query)
        \\
        \\Arguments:
        \\  input              Input file (local/s3) or '--' for stdin
        \\  output             Output file (local/s3) or '--' for stdout
        \\
        \\Operations:
        \\  --schema           Print file schema (Inspect Mode)
        \\  --meta             Print file metadata (Inspect Mode)
        \\  -f, --filter EXPR  Filter rows (e.g., category=A)
        \\  -s, --select COLS  Select columns (comma-separated)
        \\
        \\Formats:
        \\  --infmt FMT        Input format: parquet (default), csv, json
        \\  --outfmt FMT       Output format: parquet (default), csv, json
        \\                     (If output is '--', default is csv)
        \\
        \\Output Options:
        \\  -c, --compression  Compression: none, snappy (default), zstd, gzip
        \\
        \\Execution:
        \\  --surgical         Page-level pruning
        \\
        \\Server Mode:
        \\  {s} --serve [PORT]     Run HTTP server
        \\
    , .{ exe, exe, exe });
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

    // Stdin/Stdout TODOs
    if (std.mem.eql(u8, input, "--")) {
        std.debug.print("TODO: Stdin summary not implemented\n", .{});
        return;
    }

    if (parsed.output) |out| {
        if (std.mem.eql(u8, out, "--")) {
            const fmt = parsed.output_format orelse "csv";
            std.debug.print("TODO: Stdout support (fmt: {s}) not implemented\n", .{fmt});
            return;
        }
    }

    // Initialize xev runtime (dynamic backend)
    // On single-backend systems (macOS/kqueue), detect() doesn't exist - that's fine.
    if (@hasDecl(xev.Dynamic, "detect")) {
        try xev.Dynamic.detect();
    }
    var loop = try xev.Dynamic.Loop.init(.{});
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
        .compression = parsed.compression,
        .show_schema = parsed.show_schema,
        .show_meta = parsed.show_meta,
        .input_format = parsed.input_format,
        .output_format = parsed.output_format,
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

    // Mode Dispatch:
    // If output is present -> Transform/Filter Mode
    // If output is missing -> Inspect/Summary Mode (unless filter is present which is error)

    if (parsed.output != null) {
        // Transform Mode (2 positonals)
        // Initialize tracing if requested
        if (std.posix.getenv("ZPQ_TRACE_FILE")) |path| {
            try zpq.trace.initGlobal(allocator, path);
        }
        defer zpq.trace.deinitGlobal();

        const zone = zpq.trace.zone("CLI/Execution");
        defer zone.end();

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
    } else {
        // Inspect Mode (1 positional) -> Print Summary
        if (parsed.filter != null) {
            std.debug.print("Error: --filter requires an output file (or use '--' for stdout stub)\n", .{});
            return;
        }

        var pipeline = Pipeline.init(allocator);
        defer pipeline.deinit();
        pipeline.setInput(input);
        pipeline.setRuntime(&loop, &thread_pool);
        try pipeline.printSummary();
    }
}
