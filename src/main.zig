const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");
const builtin = @import("builtin");

const Pipeline = zpq.core.pipeline.Pipeline;
const ExecutionMode = zpq.core.pipeline.ExecutionMode;

pub const std_options: std.Options = .{
    .log_level = if (builtin.mode == .Debug) .debug else .warn,
};

const Args = struct {
    input: ?[]const u8 = null,
    output: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    select: ?[]const u8 = null,
    show_schema: bool = false,
    show_meta: bool = false,
    mode: ExecutionMode = .slot_parallel,
};

fn parseArgs(args: []const []const u8) ?Args {
    if (args.len < 2) return null;

    var result = Args{};
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
        \\Output:
        \\  -o, --output FILE  Output file (alternative to positional)
        \\
        \\Examples:
        \\  {s} data.parquet --schema
        \\  {s} data.parquet --meta
        \\  {s} input.parquet output.parquet --filter category=A
        \\  {s} input.parquet -o out.parquet --filter id>100 --select id,name
        \\  {s} s3://bucket/data.parquet --schema
        \\
    , .{ exe, exe, exe, exe, exe, exe });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const parsed = parseArgs(args) orelse {
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

    // Create pipeline
    var pipeline = Pipeline.init(allocator);
    defer pipeline.deinit();

    pipeline.setInput(input);
    pipeline.setRuntime(&loop, &thread_pool);

    if (parsed.output) |out| pipeline.setOutput(out);
    if (parsed.filter) |f| try pipeline.setFilter(f);
    if (parsed.select) |s| try pipeline.setProjection(s);

    // Schema-only mode
    if (parsed.show_schema) {
        try pipeline.printSchema();
        return;
    }

    // Meta-only mode
    if (parsed.show_meta) {
        try pipeline.printMeta();
        return;
    }

    // Filter/transform mode requires output
    if (parsed.filter != null) {
        if (parsed.output == null) {
            std.debug.print("Error: --filter requires an output file\n", .{});
            return;
        }

        const result = try pipeline.execute(parsed.mode);

        std.debug.print("\nComplete:\n", .{});
        std.debug.print("  Input:  {d} rows\n", .{result.input_rows});
        std.debug.print("  Output: {d} rows\n", .{result.output_rows});
        std.debug.print("  Time:   {d:.1}ms\n", .{result.elapsed_ms});
        return;
    }

    // Default: print summary
    try pipeline.printSummary();
}
