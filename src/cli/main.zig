//! zpq CLI binary entry point.
//!
//! Subcommands:
//!   zpq query <input.parquet> [--output <out.parquet>]
//!         [--filter EXPR] [--columns COL1,COL2,...]
//!         [--select "EXPR1 [AS name], EXPR2 [AS name], ..."]
//!         [--aggregate "AGG(...) [FILTER (WHERE ...)] [AS name], ..."]
//!         [--group-by "EXPR [AS name], ..."] [--column-order COL1,COL2,...]
//!         [--codec snappy|zstd|gzip|uncompressed]
//!     — local-file query: decode → filter → re-encode → write,
//!       OR aggregate (sum/count/min/max/avg with optional FILTER).
//!       Queries emit NDJSON rows by default; named -o selects a file only.
//!       --aggregate is mutually exclusive with --select / --columns.
//!       --summary=json opts into typed diagnostics on stderr.
//!
//!   zpq conform <file.parquet>   — emit a JSON report of what zpq
//!                                  sees in this file. Used by the
//!                                  conformance runner against the
//!                                  apache/parquet-testing corpus.

const std = @import("std");
const zpq = @import("zpq");
const nowMonoNs = zpq.clock.monoNs;
const build_options = @import("build_options");

const metadata = zpq.core.parquet.metadata;
const schema = zpq.core.schema;
const column = zpq.core.parquet.column;
const schema_tree = zpq.core.parquet.schema_tree;
const engine = zpq.engine;
const s3 = zpq.io.s3;
const local_fs = zpq.local_fs;
const Diag = zpq.core.diag.Diag;

const usage_text = @embedFile("help.txt");
const setops = @import("setops.zig");
const windows = @import("windows.zig");
const cte = @import("cte.zig");

var machine_diagnostics = false;
var diagnostic_buffer: [16 * 1024]u8 = undefined;
var diagnostic_len: usize = 0;

pub const std_options: std.Options = .{ .logFn = cliLog, .signal_stack_size = zpq.core.spawn.signal_stack_size };
fn cliLog(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime fmt: []const u8, args: anytype) void {
    if (!machine_diagnostics) std.log.defaultLog(level, scope, fmt, args);
}

fn wantsJsonSummary(args: std.process.Args, gpa: std.mem.Allocator) if (@import("builtin").os.tag == .windows) anyerror!bool else bool {
    var it = if (@import("builtin").os.tag == .windows) try std.process.Args.Iterator.initAllocator(args, gpa) else std.process.Args.Iterator.init(args);
    defer if (@import("builtin").os.tag == .windows) it.deinit();
    _ = it.next();
    const command = it.next() orelse return false;
    if (!std.mem.eql(u8, command, "query")) return false;
    while (it.next()) |a| {
        if (std.mem.eql(u8, a, "--summary=json")) return true;
        // Values are opaque, even when their text happens to spell a flag.
        const values = [_][]const u8{
            "--output",         "-o",                "--filter",          "-f",             "--columns",       "-c",                  "--select",     "-s",
            "--aggregate",      "-a",                "--query",           "-q",             "--codec",         "--compress-priority", "--threads",    "-j",
            "-g",               "--bloom-filter",    "--format",          "--limit",        "--group-by",      "--column-order",      "--max-memory", "--profile",
            "--row-group-size", "--row-group-bytes", "--parquet-version", "--input-format", "--csv-delimiter", "--csv-null",          "--csv-types",
        };
        for (values) |flag| if (std.mem.eql(u8, a, flag)) {
            _ = it.next();
            break;
        };
    }
    return false;
}

fn captureSqlDiagnostic(message_in: []const u8) void {
    // The JSON error is no command's output: as the libzpq and Lambda responses, it names none.
    const message = zpq.frontend.withoutCommand(message_in);
    const n = @min(message.len, diagnostic_buffer.len - diagnostic_len);
    @memcpy(diagnostic_buffer[diagnostic_len..][0..n], message[0..n]);
    diagnostic_len += n;
}

fn diagnosticPrint(comptime fmt: []const u8, args: anytype) void {
    if (!machine_diagnostics) {
        std.debug.print(fmt, args);
        return;
    }
    var buf: [16 * 1024]u8 = undefined;
    captureSqlDiagnostic(std.fmt.bufPrint(&buf, fmt, args) catch return);
}

fn emitError(err: anyerror) !void {
    var w: StdoutWriter = .{ .fd = std.Io.File.stderr().handle };
    try w.writeAll("{\"type\":\"error\",\"ok\":false,\"code\":\"");
    try writeJsonString(&w, @errorName(err));
    try w.writeAll("\",\"message\":\"");
    const said = std.mem.trimEnd(u8, diagnostic_buffer[0..diagnostic_len], "\n");
    try writeJsonString(&w, if (said.len > 0) said else @errorName(err));
    try w.writeAll("\"}\n");
    try w.flush();
}

pub fn main(init: std.process.Init) void {
    runMain(init) catch |err| {
        if (err == error.BrokenPipe) std.process.exit(141);
        std.debug.print("zpq: {s}\n", .{zpq.frontend.failureMessage(err)});
        std.process.exit(1);
    };
}

fn runMain(init: std.process.Init) !void {
    zpq.cpu_level.require();
    const gpa = zpq.core.memory.processAllocator(init.gpa);
    if (@import("builtin").os.tag == .windows) try zpq.core.system.initWindowsEnv(init.minimal.environ, gpa);
    defer if (@import("builtin").os.tag == .windows) zpq.core.system.deinitWindowsEnv();
    local_fs.raiseOpenFileLimit();
    zpq.core.memory.fixHeapThreshold();
    machine_diagnostics = if (@import("builtin").os.tag == .windows) try wantsJsonSummary(init.minimal.args, gpa) else wantsJsonSummary(init.minimal.args, gpa);
    // Human diagnostics print unbounded; machine ones are captured into the JSON error.
    if (machine_diagnostics) zpq.frontend.diagnostic_fn = captureSqlDiagnostic;
    defer zpq.frontend.diagnostic_fn = null;
    var iter = if (@import("builtin").os.tag == .windows) try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa) else std.process.Args.Iterator.init(init.minimal.args);
    defer if (@import("builtin").os.tag == .windows) iter.deinit();
    _ = iter.next(); // skip program name
    // Fan-out shares its coordinator with the Lambda entry point.
    const cmd = iter.next() orelse {
        var ws: StdoutWriter = .{};
        defer ws.flush() catch {};
        try ws.writeAll(usage_text);
        try ws.flush();
        return;
    };
    if (std.mem.eql(u8, cmd, "fanout")) {
        const result = @import("fanout.zig").run(init, &iter, gpa) catch |err| {
            std.debug.print("zpq fanout: {s}\n", .{@errorName(err)});
            return err;
        };
        defer gpa.free(result);
        var out: StdoutWriter = .{};
        try out.writeAll(result);
        try out.writeAll("\n");
        try out.flush();
        return;
    }
    if (std.mem.eql(u8, cmd, "--help") or std.mem.eql(u8, cmd, "-h") or std.mem.eql(u8, cmd, "help")) {
        var ws: StdoutWriter = .{};
        defer ws.flush() catch {};
        try ws.writeAll(usage_text);
        try ws.flush();
        return;
    }
    if (std.mem.eql(u8, cmd, "--version") or std.mem.eql(u8, cmd, "-V") or std.mem.eql(u8, cmd, "version")) {
        var ws: StdoutWriter = .{};
        defer ws.flush() catch {};
        try ws.print("zpq {s} ({s})\n", .{ build_options.version, @import("cli_version").revision });
        try ws.flush();
        return;
    }
    if (std.mem.eql(u8, cmd, "query")) {
        var diag: Diag = .{};
        // Failed-input diagnostics borrow expanded SQL paths through error emission below.
        var query_arena = std.heap.ArenaAllocator.init(gpa);
        defer query_arena.deinit();
        runQuery(init, &iter, &diag, query_arena.allocator()) catch |err| {
            if (err == error.BrokenPipe) std.process.exit(141);
            zpq.frontend.queryError(err, &diag);
            if (machine_diagnostics) try emitError(err);
            std.process.exit(queryExitCode(err, diag.execution_started));
        };
        local_fs.releaseLeft();
        return;
    }
    if (std.mem.eql(u8, cmd, "conform")) {
        const path = iter.next() orelse {
            diagnosticPrint("zpq conform: give one local Parquet path; see `zpq --help`\n", .{});
            std.process.exit(2);
        };
        if (std.mem.eql(u8, path, "--help") or std.mem.eql(u8, path, "-h")) {
            var ws: StdoutWriter = .{};
            try ws.writeAll(usage_text);
            try ws.flush();
            return;
        }
        if (iter.next() != null or std.mem.startsWith(u8, path, "-")) {
            diagnosticPrint("zpq conform: use one local path; see `zpq --help`\n", .{});
            std.process.exit(2);
        }
        if (!try runConform(gpa, path)) std.process.exit(1);
        return;
    }
    if (std.mem.eql(u8, cmd, "inspect")) {
        runInspect(init, &iter) catch |err| {
            if (err == error.BrokenPipe) std.process.exit(141);
            if (err != error.InspectionFailed and err != error.BadArgs) {
                diagnosticPrint("zpq inspect: {s}\n", .{if (err == error.NoInputs)
                    "give one or more Parquet paths; see `zpq --help`"
                else
                    zpq.frontend.failureMessage(err)});
            }
            std.process.exit(if (err == error.BadArgs or err == error.NoInputs) @as(u8, 2) else @as(u8, 1));
        };
        return;
    }
    if (std.mem.eql(u8, cmd, "schema")) {
        const path = iter.next() orelse {
            diagnosticPrint("zpq schema: give one Parquet path or s3:// URL\n", .{});
            std.process.exit(2);
        };
        if (std.mem.eql(u8, path, "--help") or std.mem.eql(u8, path, "-h")) {
            var ws: StdoutWriter = .{};
            try ws.writeAll(usage_text);
            try ws.flush();
            return;
        }
        if (iter.next() != null or std.mem.startsWith(u8, path, "-")) {
            diagnosticPrint("zpq schema: use one path; use inspect for multiple files\n", .{});
            std.process.exit(2);
        }
        try runSchema(init, path);
        return;
    }
    diagnosticPrint("unknown subcommand: {s}; use `zpq --help` for commands\n", .{cmd});
    std.process.exit(2);
}

const QueryOptions = struct {
    inputs_raw: std.ArrayList([]const u8) = .empty,
    output: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    columns_csv: ?[]const u8 = null,
    select: ?[]const u8 = null,
    aggregate: ?[]const u8 = null,
    explain: bool = false,
    query: ?[]const u8 = null,
    fsync: bool = zpq.output_publication.default_fsync,
    codec: ?schema.CompressionCodec = null,
    parquet_version: schema.ParquetVersion = .v2,
    compress_priority: engine.CompressPriority = .size,
    parallelism: usize = 0,
    scan_all: bool = false,
    trust_stats: bool = false,
    cache: bool = false,
    fast_levels: bool = false,
    unordered: bool = false,
    format: ?engine.PrintFormat = null,
    summary: bool = false,
    limit: ?usize = null,
    max_memory: ?usize = null,
    row_group: engine.RowGroupTarget = .{},
    group_by: ?[]const u8 = null,
    select_cols: ?[]const zpq.core.expr.ast.SelectColumn = null,
    result_order: zpq.core.expr.ast.ResultOrder = .{},
    column_order: ?[]const u8 = null,
    profile_out: ProfileOut = .{},
    bloom_filters_enabled: bool = true,
    bloom_filters: std.ArrayList(engine.BloomSpec) = .empty,
    input_format: engine.types.InputFormat = .auto,
    csv: engine.csv_write.Options = .{},
    csv_nulls: std.ArrayList([]const u8) = .empty,
    csv_types: std.ArrayList(engine.csv_write.Options.Pinned) = .empty,
    /// False for a query after which the process goes on (a set operation's branches): its tables and mappings are
    /// released, not left for the process to drop at exit.
    exit_after: bool = true,
    pool_registry: ?*engine.PoolRegistry = null,
    /// A set operation's final query: the branches' own input files, which its output may not name (its inputs are
    /// scratch files, so the ordinary check alone would not see them).
    protect: []const []const u8 = &.{},
    /// A set operation's branch: refuses an input that is this output, before it reads anything.
    refuse_output: ?[]const u8 = null,
    /// A set operation's branch: receives the input files the query expanded to.
    collect_inputs: ?*std.ArrayList([]const u8) = null,

    fn deinit(self: *QueryOptions, gpa: std.mem.Allocator) void {
        self.bloom_filters.deinit(gpa);
        self.inputs_raw.deinit(gpa);
        self.csv_nulls.deinit(gpa);
        self.csv_types.deinit(gpa);
    }

    fn parse(self: *QueryOptions, gpa: std.mem.Allocator, iter: *std.process.Args.Iterator) !bool {
        while (iter.next()) |tok| {
            if (std.mem.eql(u8, tok, "--help") or std.mem.eql(u8, tok, "-h")) {
                var ws: StdoutWriter = .{};
                defer ws.flush() catch {};
                try ws.writeAll(usage_text);
                try ws.flush();
                return false;
            } else if (std.mem.eql(u8, tok, "--output") or std.mem.eql(u8, tok, "-o")) {
                self.output = iter.next() orelse {
                    diagnosticPrint("zpq query: --output requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--filter") or std.mem.eql(u8, tok, "-f")) {
                self.filter = iter.next() orelse {
                    diagnosticPrint("zpq query: --filter requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--columns") or std.mem.eql(u8, tok, "-c")) {
                self.columns_csv = iter.next() orelse {
                    diagnosticPrint("zpq query: --columns requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--select") or std.mem.eql(u8, tok, "-s")) {
                self.select = iter.next() orelse {
                    diagnosticPrint("zpq query: --select requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--explain")) {
                self.explain = true;
            } else if (std.mem.eql(u8, tok, "--aggregate") or std.mem.eql(u8, tok, "-a")) {
                self.aggregate = iter.next() orelse {
                    diagnosticPrint("zpq query: --aggregate requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--query") or std.mem.eql(u8, tok, "-q")) {
                self.query = iter.next() orelse {
                    diagnosticPrint("zpq query: --query requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--fsync")) {
                self.fsync = true;
            } else if (std.mem.eql(u8, tok, "--no-fsync")) {
                self.fsync = false;
            } else if (std.mem.eql(u8, tok, "--codec")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --codec requires a value\n", .{});
                    return error.BadArgs;
                };
                if (std.ascii.eqlIgnoreCase(v, "zstd")) {
                    self.codec = .ZSTD;
                } else if (std.ascii.eqlIgnoreCase(v, "uncompressed")) {
                    self.codec = .UNCOMPRESSED;
                } else if (std.ascii.eqlIgnoreCase(v, "gzip")) {
                    self.codec = .GZIP;
                } else if (std.ascii.eqlIgnoreCase(v, "lz4") or std.ascii.eqlIgnoreCase(v, "lz4_raw")) {
                    self.codec = .LZ4_RAW;
                } else if (std.ascii.eqlIgnoreCase(v, "snappy")) {
                    self.codec = .SNAPPY;
                } else {
                    diagnosticPrint("zpq query: invalid codec: {s}\n", .{v});
                    return error.BadArgs;
                }
            } else if (std.mem.eql(u8, tok, "--parquet-version")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --parquet-version requires a value\n", .{});
                    return error.BadArgs;
                };
                self.parquet_version = std.meta.stringToEnum(schema.ParquetVersion, v) orelse {
                    diagnosticPrint("zpq query: invalid parquet version: {s} (v1 or v2)\n", .{v});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--compress-priority")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --compress-priority requires a value\n", .{});
                    return error.BadArgs;
                };
                self.compress_priority = std.meta.stringToEnum(engine.CompressPriority, v) orelse {
                    diagnosticPrint("zpq query: invalid compress priority: {s} (size or speed)\n", .{v});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--threads") or std.mem.eql(u8, tok, "-j")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --threads requires a value\n", .{});
                    return error.BadArgs;
                };
                self.parallelism = std.fmt.parseInt(usize, v, 10) catch {
                    diagnosticPrint("zpq query: invalid threads value: {s}\n", .{v});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--input-format")) {
                const v = iter.next() orelse "";
                self.input_format = std.meta.stringToEnum(engine.types.InputFormat, v) orelse {
                    diagnosticPrint("zpq query: --input-format takes parquet or csv\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--csv-delimiter")) {
                const v = iter.next() orelse "";
                const tab = std.mem.eql(u8, v, "\\t") or std.ascii.eqlIgnoreCase(v, "tab");
                const one = v.len == 1 and v[0] != '"' and v[0] != '\n' and v[0] != '\r';
                self.csv.delimiter = if (tab) '\t' else if (one) v[0] else {
                    diagnosticPrint("zpq query: --csv-delimiter takes one character (or \\t / tab),\n" ++
                        "  not a quote or a newline\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--csv-header")) {
                self.csv.header = true;
            } else if (std.mem.eql(u8, tok, "--csv-no-header")) {
                self.csv.header = false;
            } else if (std.mem.eql(u8, tok, "--csv-strict")) {
                self.csv.strict = true;
            } else if (std.mem.eql(u8, tok, "--csv-null")) {
                try self.csv_nulls.append(gpa, iter.next() orelse {
                    diagnosticPrint("zpq query: --csv-null requires a value\n", .{});
                    return error.BadArgs;
                });
            } else if (std.mem.eql(u8, tok, "--csv-types")) {
                const v = iter.next() orelse "";
                parseCsvTypes(gpa, v, &self.csv_types) catch {
                    diagnosticPrint("zpq query: --csv-types takes COL:TYPE[,COL:TYPE...] with TYPE one of\n" ++
                        "  BIGINT, DOUBLE, DECIMAL(p,s), DATE, TIMESTAMP, TIMESTAMP_NS, TIMESTAMPTZ,\n" ++
                        "  BOOLEAN, VARCHAR, BLOB\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--no-bloom-filters")) {
                self.bloom_filters_enabled = false;
            } else if (std.mem.eql(u8, tok, "--bloom-filter")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --bloom-filter requires a value\n", .{});
                    return error.BadArgs;
                };
                try self.bloom_filters.append(gpa, engine.BloomSpec.parse(v) catch {
                    diagnosticPrint("zpq query: invalid --bloom-filter: {s} (want COL or COL:FPP, 0 < FPP < 1)\n", .{v});
                    return error.BadArgs;
                });
            } else if (std.mem.eql(u8, tok, "--scan-all")) {
                self.scan_all = true; // valueless: disable all stats shortcuts, decode everything
            } else if (std.mem.eql(u8, tok, "--cache")) {
                self.cache = true;
            } else if (std.mem.eql(u8, tok, "--trust-stats")) {
                self.trust_stats = true; // valueless: opt in to stats-as-answer for min/max/sum
            } else if (std.mem.eql(u8, tok, "--fast-levels")) {
                self.fast_levels = true; // valueless: skip def levels on provably no-null pages
            } else if (std.mem.eql(u8, tok, "--unordered")) {
                self.unordered = true; // valueless: GROUP BY order and float bits may vary
            } else if (std.mem.eql(u8, tok, "--format")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --format requires a value\n", .{});
                    return error.BadArgs;
                };
                if (std.ascii.eqlIgnoreCase(v, "csv")) {
                    self.format = .csv;
                } else if (std.ascii.eqlIgnoreCase(v, "jsonl") or std.ascii.eqlIgnoreCase(v, "ndjson")) {
                    self.format = .jsonl;
                } else if (std.ascii.eqlIgnoreCase(v, "json")) {
                    self.format = .json;
                } else if (std.ascii.eqlIgnoreCase(v, "parquet")) {
                    self.format = .parquet;
                } else {
                    diagnosticPrint("zpq query: invalid format: {s}\n", .{v});
                    return error.BadArgs;
                }
            } else if (std.mem.eql(u8, tok, "--summary=json")) {
                self.summary = true;
            } else if (std.mem.eql(u8, tok, "--limit")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --limit requires a value\n", .{});
                    return error.BadArgs;
                };
                if (self.limit != null) {
                    diagnosticPrint("zpq query: duplicate --limit\n", .{});
                    return error.BadArgs;
                }
                self.limit = std.fmt.parseInt(usize, v, 10) catch {
                    diagnosticPrint("zpq query: invalid limit value: {s}\n", .{v});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--group-by") or std.mem.eql(u8, tok, "-g")) {
                self.group_by = iter.next() orelse {
                    diagnosticPrint("zpq query: --group-by requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--column-order")) {
                self.column_order = iter.next() orelse {
                    diagnosticPrint("zpq query: --column-order requires a value\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--max-memory")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --max-memory requires a value\n", .{});
                    return error.BadArgs;
                };
                self.max_memory = zpq.core.system.parseSizeString(v) catch {
                    diagnosticPrint("zpq query: invalid max-memory value: {s} (examples: 500MB, 1.5GB, 1024)\n", .{v});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--row-group-size")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --row-group-size requires a value\n", .{});
                    return error.BadArgs;
                };
                self.row_group.rows = std.fmt.parseInt(usize, v, 10) catch 0;
                if (self.row_group.rows == 0) {
                    diagnosticPrint("zpq query: invalid --row-group-size: {s} (a positive number of rows)\n", .{v});
                    return error.BadArgs;
                }
            } else if (std.mem.eql(u8, tok, "--row-group-bytes")) {
                const v = iter.next() orelse {
                    diagnosticPrint("zpq query: --row-group-bytes requires a value\n", .{});
                    return error.BadArgs;
                };
                self.row_group.bytes = zpq.core.system.parseSizeString(v) catch 0;
                if (self.row_group.bytes == 0) {
                    diagnosticPrint("zpq query: invalid --row-group-bytes: {s} (examples: 64MB, 1GB)\n", .{v});
                    return error.BadArgs;
                }
            } else if (std.mem.eql(u8, tok, "--profile")) {
                self.profile_out.trace_path = iter.next() orelse {
                    diagnosticPrint("zpq query: --profile requires a path\n", .{});
                    return error.BadArgs;
                };
            } else if (std.mem.eql(u8, tok, "--profile-summary") or std.mem.startsWith(u8, tok, "--profile-summary=")) {
                const v = if (tok.len > "--profile-summary".len) tok["--profile-summary=".len..] else "text";
                self.profile_out.summary = std.meta.stringToEnum(ProfileOut.Format, v) orelse {
                    diagnosticPrint("zpq query: invalid --profile-summary format: {s} (supported: text, json)\n", .{v});
                    return error.BadArgs;
                };
            } else if (std.mem.startsWith(u8, tok, "-")) {
                diagnosticPrint("zpq query: unrecognized option: {s}\n", .{tok});
                return error.BadArgs;
            } else {
                try self.inputs_raw.append(gpa, tok);
            }
        }

        return true;
    }
};

/// Runs a set operation's queries through `runParsed`, each on a copy of the user's options.
const SetopStager = struct {
    init: std.process.Init,
    options: *const QueryOptions,
    diag: *Diag,
    arena: std.mem.Allocator,
    /// The input files of every branch run so far.
    collected: std.ArrayList([]const u8) = .empty,
    /// A CTE's scratch directory. A query that names it reads Parquet scratch files, whatever `--input-format` says;
    /// one that does not reads the user's inputs, with their input options. Null for a set operation, whose last query
    /// reads only scratch.
    cte_dir: ?[]const u8 = null,

    fn readsScratch(self: *const SetopStager, sql: []const u8) bool {
        const dir = self.cte_dir orelse return true;
        return std.mem.indexOf(u8, sql, dir) != null;
    }

    fn interface(self: *SetopStager) setops.Stager {
        return .{ .ptr = self, .stage = stage, .finish = finish, .say = say };
    }

    fn stage(ptr: *anyopaque, sql: []const u8, out: []const u8, leaf: bool) anyerror!void {
        const self: *SetopStager = @ptrCast(@alignCast(ptr));
        var o = stageOptions(self.options.*, sql, out, leaf);
        if (leaf) o.collect_inputs = &self.collected;
        if (leaf and self.cte_dir != null and self.readsScratch(sql)) parquetInputs(&o);
        try runParsed(self.init, &o, self.diag, self.arena);
    }

    fn finish(ptr: *anyopaque, sql: []const u8) anyerror!void {
        const self: *SetopStager = @ptrCast(@alignCast(ptr));
        var o = self.options.*;
        o.query = sql;
        if (self.readsScratch(sql)) parquetInputs(&o);
        o.protect = self.collected.items;
        try runParsed(self.init, &o, self.diag, self.arena);
    }

    fn say(message: []const u8) void {
        diagnosticPrint("{s}", .{message});
    }
};

/// The options a set operation runs one of its queries into the scratch file `out` with: the user's execution
/// flags, none of their output. Only the user's own query ends the process; a branch's resources are released.
fn stageOptions(base: QueryOptions, sql: []const u8, out: []const u8, leaf: bool) QueryOptions {
    var o = base;
    o.query = sql;
    o.output = out;
    o.format = .parquet;
    o.limit = null;
    o.summary = false;
    o.explain = false;
    o.fsync = false;
    o.profile_out = .{};
    o.bloom_filters = .empty;
    o.exit_after = false;
    o.protect = &.{};
    o.collect_inputs = null;
    // A branch may not read the file the user's query will write.
    o.refuse_output = if (leaf) base.output else null;
    if (!leaf) parquetInputs(&o);
    return o;
}

/// A query over scratch files: they are Parquet, whatever the user's input options say.
fn parquetInputs(o: *QueryOptions) void {
    o.input_format = .auto;
    o.csv = .{};
    o.csv_nulls = .empty;
    o.csv_types = .empty;
}

test "a set operation's scratch queries release their resources and name the output they must not read" {
    const base: QueryOptions = .{ .output = "result.parquet", .limit = 5, .summary = true };
    const leaf = stageOptions(base, "SELECT 1", "scratch/raw1.parquet", true);
    try std.testing.expect(!leaf.exit_after);
    try std.testing.expect(leaf.limit == null and !leaf.summary and leaf.format.? == .parquet);
    try std.testing.expectEqualStrings("scratch/raw1.parquet", leaf.output.?);
    try std.testing.expectEqualStrings("result.parquet", leaf.refuse_output.?);
    const over = stageOptions(base, "SELECT 1", "scratch/p2.parquet", false);
    try std.testing.expect(!over.exit_after and over.refuse_output == null);
    // The user's own query is still the one that ends the process.
    try std.testing.expect(base.exit_after);
}

/// One CLI query owns its sockets across binding, execution and retries. The pool already keys idle sockets by host.
const QueryPool = struct {
    const Pool = s3.Pool(engine.POOL_SIZE);

    gpa: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    pool: ?*Pool = null,

    fn deinit(self: *QueryPool) void {
        if (self.pool) |pool| {
            pool.deinit();
            self.gpa.destroy(pool);
        }
    }

    fn ensure(raw: *anyopaque, _: std.mem.Allocator, _: s3.Credentials, _: []const u8) anyerror!*Pool {
        const self: *QueryPool = @ptrCast(@alignCast(raw));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.pool) |pool| return pool;
        const pool = try self.gpa.create(Pool);
        errdefer self.gpa.destroy(pool);
        try pool.init(self.gpa);
        self.pool = pool;
        return pool;
    }
};

fn runQuery(
    init: std.process.Init,
    iter: *std.process.Args.Iterator,
    diag: *Diag,
    arena: std.mem.Allocator,
) !void {
    const gpa = zpq.core.memory.processAllocator(init.gpa);
    var options: QueryOptions = .{};
    defer options.deinit(gpa);
    if (!try options.parse(gpa, iter)) return;
    var pool: QueryPool = .{ .gpa = gpa, .io = init.io };
    defer pool.deinit();
    var registry: engine.PoolRegistry = .{ .ctx = &pool, .ensure_for_bucket = QueryPool.ensure };
    if (options.query != null) options.pool_registry = &registry;
    try runParsed(init, &options, diag, arena);
}

/// The top-level column names of the first file a table path or glob names, for `COLUMNS(...)`.
const ColumnNames = struct {
    schemas: *engine.join.LocalSchemas,
    io: std.Io,
    env: std.process.Environ,
    diag: ?*Diag,

    fn of(ctx: *anyopaque, arena: std.mem.Allocator, table: []const u8) anyerror![]const []const u8 {
        const self: *ColumnNames = @ptrCast(@alignCast(ctx));
        const matches = try expandGlob(arena, self.io, table, self.env, self.diag);
        if (matches.len == 0) return error.NoMatches;
        const meta = engine.join.LocalSchemas.footer(self.schemas, matches[0]) catch |err| return if (err == error.JoinNotLocal) error.ColumnsNotSupported else err;
        const items = meta.schema.items;
        var out: std.ArrayList([]const u8) = .empty;
        var i: usize = 1;
        while (i < items.len) {
            try out.append(arena, items[i].name);
            // A group's descendants are not top-level columns.
            var pending: usize = @intCast(items[i].num_children orelse 0);
            i += 1;
            while (pending > 0 and i < items.len) : (i += 1) pending = pending - 1 + @as(usize, @intCast(items[i].num_children orelse 0));
        }
        return out.items;
    }
};

fn planCte(arena: std.mem.Allocator, sql: []const u8, dir: []const u8) !?zpq.core.expr.sql_cte.Plan {
    return zpq.core.expr.sql_cte.plan(arena, sql, dir) catch |err| {
        switch (err) {
            error.CteRecursive => diagnosticPrint("sql: WITH RECURSIVE is not supported\n", .{}),
            error.CteDuplicate => diagnosticPrint("sql: two CTEs of one WITH have the same name\n", .{}),
            error.CteCircular => diagnosticPrint("sql: a CTE reads itself; only WITH RECURSIVE may, which is not supported\n", .{}),
            error.LateralNotSupported => diagnosticPrint("sql: LATERAL (a subquery in FROM that reads the tables beside it) is not supported\n", .{}),
            error.SqlParseError => diagnosticPrint("sql: a WITH list or a subquery's alias in FROM does not parse\n", .{}),
            error.OutOfMemory => {},
        }
        return err;
    };
}

/// The query `options` describe. A set operation runs its branches through here too, on copies of the options.
fn runParsed(
    init: std.process.Init,
    options: *const QueryOptions,
    diag: *Diag,
    arena: std.mem.Allocator,
) !void {
    var memo: zpq.frontend.JoinMemo = .{};
    var rebound = false;
    while (true) {
        var retry: ?[]const u8 = null;
        const err = retry_root: {
            const first = if (runParsedOnce(init, options, diag, arena, null, &retry, &memo)) |_| return else |err| err;
            if (first != error.DuplicateBuildKey or retry == null) break :retry_root first;
            const prior = diag.*;
            diag.* = .{};
            runParsedOnce(init, options, diag, arena, retry.?, &retry, &memo) catch |second| {
                if (second == error.DuplicateBuildKey) return error.JoinNonUnique;
                if (second == error.UnsupportedJoin) {
                    diag.* = prior;
                    return first;
                }
                break :retry_root second;
            };
            return;
        };
        switch (err) {
            // At least one object now bypasses its memoized footer. Keep the parsed binder schema until a fresh
            // open shows that it changed. Each object can trigger this fallback only once.
            error.S3IfMatchRejected => {},
            error.AlignedJoinSnapshotChanged => {
                if (rebound) return error.InputChanged;
                rebound = true;
                memo.remote = .empty;
            },
            else => return err,
        }
        diag.* = .{};
    }
}

fn runParsedOnce(
    init: std.process.Init,
    options: *const QueryOptions,
    diag: *Diag,
    arena: std.mem.Allocator,
    root: ?[]const u8,
    retry: *?[]const u8,
    memo: *zpq.frontend.JoinMemo,
) !void {
    var parsed_join: ?zpq.core.expr.syntax.Join = null;
    errdefer if (parsed_join) |j| {
        retry.* = zpq.frontend.retryJoinRoot(j, diag.input);
    };
    const gpa = zpq.core.memory.processAllocator(init.gpa);
    const env = init.minimal.environ;
    const io = init.io;
    var ctx: engine.Context = .{ .gpa = gpa, .env = env, .io = io, .diag = diag, .pool_registry = options.pool_registry };
    const inputs_raw = options.inputs_raw;
    const output = options.output;
    var filter = options.filter;
    const columns_csv = options.columns_csv;
    var select = options.select;
    var aggregate = options.aggregate;
    const explain = options.explain;
    // `COLUMNS(...)` select items are written out once per column they name, which the footer says.
    var column_schemas: engine.join.LocalSchemas = .{ .arena = arena, .ctx = ctx };
    defer column_schemas.deinit();
    // CTEs and subqueries in FROM run first, as queries of their own into scratch files the query then reads.
    if (comptime build_options.enable_sql) if (options.query) |q| {
        const clean = try zpq.core.expr.sql_parser.stripComments(arena, q);
        const sql_cte = zpq.core.expr.sql_cte;
        if (try planCte(arena, clean, "") != null) {
            if (explain) {
                diagnosticPrint("zpq query: --explain is not supported for a query with CTEs or subqueries in FROM\n", .{});
                return error.BadArgs;
            }
            const out_dir: ?[]const u8 = if (output) |o| blk: {
                if (std.mem.eql(u8, o, "-") or std.mem.startsWith(u8, o, "s3://")) break :blk null;
                break :blk std.fs.path.dirname(o) orelse ".";
            } else null;
            const dir = try cte.scratchDir(arena, clean, out_dir);
            const plan: sql_cte.Plan = (try planCte(arena, clean, dir)).?;
            var stager: SetopStager = .{ .init = init, .options = options, .diag = diag, .arena = arena, .cte_dir = dir };
            return cte.run(gpa, io, plan, dir, stager.interface());
        }
    };
    const query: ?[]const u8 = if (comptime build_options.enable_sql) if (options.query) |q| blk: {
        var names: ColumnNames = .{ .schemas = &column_schemas, .io = io, .env = env, .diag = diag };
        // Comments are gone first: a COLUMNS( in one is no call.
        const clean = try zpq.core.expr.sql_parser.stripComments(arena, q);
        break :blk zpq.core.expr.sql_columns.expand(arena, clean, .{ .ctx = &names, .names = ColumnNames.of }) catch |err| {
            switch (err) {
                error.ColumnsNotSupported => diagnosticPrint("sql: COLUMNS(...) is supported as select items over one local table, a regular expression or * [EXCLUDE (...)] (not REPLACE or RENAME), once in an item\n", .{}),
                error.ColumnsNoMatch => diagnosticPrint("sql: no column matches COLUMNS(...)\n", .{}),
                error.ColumnsExcludeMissing => diagnosticPrint("sql: a column in the EXCLUDE list of COLUMNS(...) is not found\n", .{}),
                else => {},
            }
            return err;
        };
    } else null else options.query;
    const fsync = options.fsync;
    const codec = options.codec;
    const parquet_version = options.parquet_version;
    const compress_priority = options.compress_priority;
    const parallelism = options.parallelism;
    const scan_all = options.scan_all;
    const trust_stats = options.trust_stats;
    const cache = options.cache;
    const fast_levels = options.fast_levels;
    const unordered = options.unordered;
    const format = options.format;
    const summary = options.summary;
    var limit = options.limit;
    const max_memory = options.max_memory;
    const row_group = options.row_group;
    var group_by = options.group_by;
    var plan_source: ?engine.types.bound_plan.Source = null;
    var remote_join = false;
    var select_cols = options.select_cols;
    var result_order = options.result_order;
    const column_order = options.column_order;
    var profile_out = options.profile_out;
    const bloom_filters_enabled = options.bloom_filters_enabled;
    const bloom_filters = options.bloom_filters;
    var csv_opts = options.csv;
    csv_opts.nulls = options.csv_nulls.items;
    csv_opts.types = options.csv_types.items;
    var csv_report: engine.csv_write.Report = .{};

    if (query != null) {
        if (inputs_raw.items.len > 0) {
            diagnosticPrint("zpq query: cannot specify input files on the command line when using --query\n", .{});
            return error.BadArgs;
        }
        if (select != null or aggregate != null or filter != null or columns_csv != null or group_by != null or column_order != null) {
            diagnosticPrint("zpq query: --query is mutually exclusive with --select / --aggregate / --filter / --columns / --group-by / --column-order\n", .{});
            return error.BadArgs;
        }
    } else {
        if (inputs_raw.items.len == 0) {
            diagnosticPrint("zpq query: missing <input.parquet> [more.parquet ...]\n", .{});
            return error.BadArgs;
        }
        if (aggregate != null and (select != null or columns_csv != null)) {
            diagnosticPrint("zpq query: --aggregate is mutually exclusive with --select / --columns\n", .{});
            return error.BadArgs;
        }
        if (group_by != null and (select != null or columns_csv != null)) {
            diagnosticPrint("zpq query: --group-by is mutually exclusive with --select / --columns\n", .{});
            return error.BadArgs;
        }
        if (column_order != null and group_by == null) {
            diagnosticPrint("zpq query: --column-order requires --group-by\n", .{});
            return error.BadArgs;
        }
    }
    if (comptime build_options.enable_sql) if (query) |q| {
        const stripped = try zpq.core.expr.sql_parser.stripComments(arena, q);
        const combination = zpq.core.expr.sql_setops.split(arena, stripped) catch |err| {
            switch (err) {
                error.SetOperationAll => diagnosticPrint("sql: INTERSECT ALL and EXCEPT ALL are not supported yet\n", .{}),
                error.SetOperationByName => diagnosticPrint("sql: UNION BY NAME is not supported yet\n", .{}),
                error.SetOperationOrder => diagnosticPrint("sql: an ORDER BY, LIMIT or OFFSET before a set operator needs the SELECT in parentheses\n", .{}),
                error.SetOperationNesting => diagnosticPrint("sql: a parenthesized set operation with its own ORDER BY, LIMIT or OFFSET is not supported yet\n", .{}),
                else => {},
            }
            return err;
        };
        const sets = if (combination != null) null else zpq.core.expr.sql_grouping.expand(arena, stripped) catch |err| {
            switch (err) {
                error.GroupingSetsKey => diagnosticPrint("sql: a grouping key that a grouping set leaves out must be a column\n", .{}),
                error.GroupingSetsDistinct => diagnosticPrint("sql: SELECT DISTINCT with ROLLUP, CUBE or GROUPING SETS is not supported yet\n", .{}),
                error.GroupingSetsNesting => diagnosticPrint("sql: ROLLUP or CUBE inside GROUPING SETS is not supported yet\n", .{}),
                error.GroupingSetsGrouping => diagnosticPrint("sql: GROUPING is supported as a select item of its own, of grouping keys written as they are in the GROUP BY\n", .{}),
                error.GroupingSetsTooMany => diagnosticPrint("sql: ROLLUP, CUBE and GROUPING SETS make at most {d} grouping sets\n", .{zpq.core.expr.sql_grouping.max_sets}),
                error.GroupingSetsQualify => diagnosticPrint("sql: QUALIFY with ROLLUP, CUBE or GROUPING SETS is not supported\n", .{}),
                else => {},
            }
            return err;
        };
        if (combination == null and sets == null and zpq.core.expr.sql_windows.mentions(stripped)) {
            if (explain) {
                diagnosticPrint("zpq query: --explain is not supported for a query with window functions\n", .{});
                return error.BadArgs;
            }
            // A star beside a window call is the columns it stands for: the stages are planned around them.
            var star_names: ColumnNames = .{ .schemas = &column_schemas, .io = io, .env = env, .diag = diag };
            const starred = zpq.core.expr.sql_columns.expandStars(arena, stripped, .{ .ctx = &star_names, .names = ColumnNames.of }) catch |err| {
                switch (err) {
                    error.ColumnsNotSupported => diagnosticPrint("sql: SELECT * with a window function needs one local table\n", .{}),
                    error.ColumnsExcludeMissing => diagnosticPrint("sql: a column in the EXCLUDE list of * is not found\n", .{}),
                    else => {},
                }
                return err;
            };
            const text = (try zpq.core.expr.sql_parser.parserText(arena, starred)) orelse {
                diagnosticPrint("sql: window functions are not supported with typed literals, ILIKE or GROUP BY ALL yet\n", .{});
                return error.WindowNotSupported;
            };
            var why: zpq.core.expr.sql_windows.Refusal = .{};
            const plan = zpq.core.expr.sql_windows.build(arena, text, &why, zpq.core.expr.sql_columns.tableColumns(arena, starred, .{ .ctx = &star_names, .names = ColumnNames.of })) catch |err| {
                switch (err) {
                    error.WindowNotSupported => diagnosticPrint("sql: {s}\n", .{why.text()}),
                    error.SelectAliasRequired => diagnosticPrint("sql: a select item of a window query needs an alias (AS name)\n", .{}),
                    else => {},
                }
                return err;
            };
            var stager: SetopStager = .{ .init = init, .options = options, .diag = diag, .arena = arena };
            const out_dir: ?[]const u8 = if (output) |o| blk: {
                if (std.mem.eql(u8, o, "-") or std.mem.startsWith(u8, o, "s3://")) break :blk null;
                break :blk std.fs.path.dirname(o) orelse ".";
            } else null;
            return windows.run(gpa, io, plan, out_dir, stager.interface(), max_memory orelse zpq.core.system.discoverAvailableMemory(env));
        }
        if (combination orelse sets) |compound| {
            if (explain) {
                diagnosticPrint("zpq query: --explain is not supported for a set operation\n", .{});
                return error.BadArgs;
            }
            var stager: SetopStager = .{ .init = init, .options = options, .diag = diag, .arena = arena };
            const out_dir: ?[]const u8 = if (output) |o| blk: {
                if (std.mem.eql(u8, o, "-") or std.mem.startsWith(u8, o, "s3://")) break :blk null;
                break :blk std.fs.path.dirname(o) orelse ".";
            } else null;
            return setops.run(gpa, io, compound, out_dir, stager.interface());
        }
    };

    if (bloom_filters.items.len > 0 and output == null) {
        diagnosticPrint("zpq query: --bloom-filter requires --output\n", .{});
        return error.BadArgs;
    }

    // Expand glob patterns. `data/*.parquet` → list of matching files;
    // literal paths pass through. Sorted within each pattern for
    // deterministic ordering across runs.
    var inputs_list: std.ArrayList([]const u8) = .empty;
    if (query) |q_str| {
        // The SQL frontend is an opt-in build feature. When compiled out
        // (`-Dsql=false`, or any Lambda build) `sql_parser` is an empty stub,
        // so the `parseSqlQuery` call must sit inside a comptime-TRUE branch —
        // Zig skips analysis of the untaken branch for a comptime-known
        // condition, which is what keeps the minimal build compiling.
        if (comptime build_options.enable_sql) {
            if (machine_diagnostics) zpq.core.expr.sql_parser.diagnostic_fn = captureSqlDiagnostic;
            defer zpq.core.expr.sql_parser.diagnostic_fn = null;
            if (try zpq.core.expr.sql_parser.parseSqlUpdate(arena, q_str)) |up| {
                if (format != null or limit != null or explain or codec != null or bloom_filters.items.len > 0 or profile_out.wanted()) {
                    diagnosticPrint("zpq query: UPDATE takes -o, --fsync/--no-fsync, --max-memory and --summary=json only\n", .{});
                    return error.BadArgs;
                }
                return runUpdate(io, gpa, arena, up, output, fsync, summary, max_memory orelse zpq.core.system.discoverAvailableMemory(env));
            }
            // A two-table join binds its column names against both footers; the engine reads the build.
            var schemas: zpq.frontend.JoinSchemas = .{
                .arena = arena,
                .io = io,
                .env = env,
                .diag = diag,
                .local = .{ .arena = arena, .pin_remote = true, .ctx = ctx },
            };
            schemas.local.max_memory = max_memory;
            schemas.restore(memo);
            defer schemas.save(memo);
            defer schemas.deinit();
            const sql = zpq.core.expr.sql_parser;
            const pq = (try sql.parseSqlJoinPlanRoot(arena, q_str, .{ .ctx = &schemas, .footer = zpq.frontend.JoinSchemas.footer, .prefetch = zpq.frontend.JoinSchemas.prefetch }, root)) orelse
                try sql.parseSqlPlan(arena, q_str);
            parsed_join = pq.syntax.?.join;
            remote_join = schemas.local.remote_seen;
            plan_source = engine.syntax_plan.source(pq.syntax.?);
            filter = pq.filter_str;
            select = pq.select_str;
            aggregate = pq.aggregate_str;
            group_by = pq.group_by_str;
            select_cols = pq.select_cols;
            result_order = pq.result_order;
            if (limit != null and result_order.limit != null) {
                diagnosticPrint("zpq query: --limit conflicts with SQL LIMIT\n", .{});
                return error.BadArgs;
            }
            if (limit == null) if (result_order.limit) |n| {
                limit = @intCast(n);
            };

            const matches = try schemas.listing(pq.table_name);
            if (matches.len == 0) {
                zpq.frontend.noSqlMatches(pq.table_name);
                return error.NoMatches;
            }
            for (matches) |m| try inputs_list.append(arena, m);
        } else {
            // Referencing q_str here keeps it "used" in the minimal build,
            // where the comptime-true branch above is eliminated.
            diagnosticPrint("zpq: this build has no SQL frontend — `query` ({s}) needs a build with -Dsql=true\n", .{q_str});
            return error.SqlNotCompiledIn;
        }
    } else {
        for (inputs_raw.items) |pat| {
            const matches = try expandGlob(arena, io, pat, env, diag);
            if (matches.len == 0) {
                diagnosticPrint("zpq query: no files matched: {s}\n", .{pat});
                return error.NoMatches;
            }
            for (matches) |m| try inputs_list.append(arena, m);
        }
    }
    const inputs = inputs_list.items;
    if (options.collect_inputs) |list| for (inputs) |p| try list.append(arena, p);
    if (options.refuse_output) |out| for (inputs) |in| {
        if (std.mem.startsWith(u8, in, "s3://")) continue;
        if (local_fs.sameFile(in, out) catch false) {
            diagnosticPrint("zpq query: input and output must be different files\n", .{});
            return error.BadArgs;
        }
    };
    const fmt = format orelse if (output != null and !std.mem.eql(u8, output.?, "-"))
        engine.PrintFormat.parquet
    else
        engine.PrintFormat.jsonl;
    const to_stdout = output == null or std.mem.eql(u8, output.?, "-");
    if (bloom_filters.items.len > 0 and (fmt != .parquet or limit != null or result_order.offset != 0)) {
        diagnosticPrint("zpq query: --bloom-filter requires unlimited Parquet output\n", .{});
        return error.BadArgs;
    }
    if (!explain and fmt != .parquet and output != null and std.mem.startsWith(u8, output.?, "s3://")) {
        diagnosticPrint("zpq query: text output does not support S3 destinations\n", .{});
        return error.BadArgs;
    }
    if (!explain and fmt == .parquet and to_stdout and try std.Io.File.stdout().isTty(io)) {
        diagnosticPrint("zpq query: refusing to write Parquet to a terminal\n", .{});
        return error.BadArgs;
    }
    if (summary and profile_out.summary == .text) {
        diagnosticPrint("zpq query: --summary=json requires --profile-summary=json\n", .{});
        return error.BadArgs;
    }
    if (group_by != null) result_order.limit = if (limit) |n| @intCast(n) else result_order.limit;
    var publication: zpq.output_publication.OutputPublication = .{ .fsync = fsync };
    defer publication.abort();
    const engine_output = if (explain) output else if (to_stdout) null else publication.prepare(io, output, if (options.protect.len == 0) inputs else try std.mem.concat(arena, []const u8, &.{ inputs, options.protect })) catch |err| {
        switch (err) {
            error.InputOutputSame => diagnosticPrint("zpq query: input and output must be different files\n", .{}),
            error.OutputNotRegular => diagnosticPrint("zpq query: output must be a regular file\n", .{}),
            else => return err,
        }
        return error.BadArgs;
    };
    const max_mem_limit = max_memory orelse zpq.core.system.discoverAvailableMemory(env);

    var profiler: ?*zpq.profile.Profiler = null;
    defer if (profiler) |p| p.destroy();
    // The profile's buffers come out of the query's budget, sized to it (`profile.Options.forBudget`).
    var engine_max_mem = max_mem_limit;
    // A failed query leaves no partial trace behind, and an earlier trace at the path untouched.
    defer profile_out.close();
    if (profile_out.wanted()) {
        if (comptime !zpq.profile.enabled) {
            diagnosticPrint("zpq query: this build has no profiler (built with -Dprofile=false)\n", .{});
            return error.BadArgs;
        }
        // Before the query, so an unwritable path costs nothing and a query that succeeds exits 0.
        try profile_out.open(io);
        var meta = zpq.profile.Meta.discover(env);
        meta.query = try commandLine(arena, init.minimal.args);
        const opts = zpq.profile.Options.forBudget(max_mem_limit);
        profiler = try zpq.profile.Profiler.create(gpa, opts, meta);
        engine_max_mem = max_mem_limit -| opts.reservedBytes();
    }

    if (explain) {
        if (aggregate == null and group_by == null) {
            diagnosticPrint("zpq query: --explain requires an aggregate or GROUP BY query\n", .{});
            return error.BadArgs;
        }
        const explain_args: engine.QueryArgs = .{
            .inputs = inputs,
            .plan_source = plan_source,
            .replicated_join_limit = if (remote_join) engine_max_mem else null,
            .remote_footers = if (remote_join and parsed_join != null) &memo.remote else null,
            .remote_read_policies = if (remote_join and parsed_join != null) &memo.read_policies else null,
            .output = if (fmt == .parquet) engine_output orelse "-" else null,
            .typed_answer = fmt == .parquet,
            .text_output = if (fmt != .parquet) .{ .format = fmt } else null,
            .filter = filter,
            .aggregate = aggregate orelse "",
            .parallelism = parallelism,
            .scan_all = scan_all,
            .trust_stats = trust_stats,
            .cache = cache,
            .fast_levels = fast_levels,
            .unordered = unordered,
            .max_memory = engine_max_mem,
            .group_by = group_by,
            .select_cols = select_cols,
            .result_order = result_order,
            .column_order = column_order,
            .diag = diag,
            .profiler = profiler,
            .input_format = options.input_format,
            .csv = csv_opts,
        };
        var ws: StdoutWriter = if (@import("builtin").os.tag == .windows) .{} else .{ .fd = 1 };
        defer ws.flush() catch {};
        // A CSV input's aggregate folds during the parse (`csv_agg`): its own plan, not a Parquet scan's.
        if (try engine.csv_write.csvInput(explain_args)) |path| {
            try (try engine.csv_agg.explain(ctx, arena, explain_args, path)).writeJson(&ws);
            try ws.flush();
            if (profiler) |p| try profile_out.emit(gpa, io, p);
            return;
        }
        const explanation = try engine.explainAggregate(ctx, arena, explain_args);
        try engine.writeAggregateExplanationJson(&ws, explanation);
        try ws.flush();
        if (profiler) |p| try profile_out.emit(gpa, io, p);
        return;
    }

    const query_args: engine.QueryArgs = .{
        .inputs = inputs,
        .plan_source = plan_source,
        .replicated_join_limit = if (remote_join) engine_max_mem else null,
        .remote_footers = if (remote_join and parsed_join != null) &memo.remote else null,
        .remote_read_policies = if (remote_join and parsed_join != null) &memo.read_policies else null,
        .output = if (fmt == .parquet) engine_output orelse "-" else null,
        .output_fd = if (fmt == .parquet) (if (to_stdout) (if (@import("builtin").os.tag == .windows) std.Io.File.stdout().handle else 1) else publication.fd) else null,
        .output_restartable = fmt == .parquet and !to_stdout and publication.fd != null,
        .typed_answer = fmt == .parquet,
        .text_output = if (fmt != .parquet) .{
            .format = fmt,
            .fd = if (to_stdout) (if (@import("builtin").os.tag == .windows) std.Io.File.stdout().handle else 1) else publication.fd orelse return error.BadOutputUrl,
        } else null,
        .quiet_diagnostics = machine_diagnostics,
        .empty_scalar = group_by == null and (limit == 0 or result_order.offset > 0),
        .filter = filter,
        .columns = if (columns_csv) |csv| try splitCsv(arena, csv) else null,
        .select = select,
        .aggregate = aggregate,
        .codec = codec,
        .parquet_version = parquet_version,
        .compress_priority = compress_priority,
        .parallelism = parallelism,
        .scan_all = scan_all,
        .trust_stats = trust_stats,
        .cache = cache,
        .fast_levels = fast_levels,
        .unordered = unordered,
        .max_memory = engine_max_mem,
        .max_memory_explicit = max_memory,
        .row_group = row_group,
        .group_by = group_by,
        .select_cols = select_cols,
        .result_order = result_order,
        .column_order = column_order,
        .bloom_filters = bloom_filters.items,
        .bloom_filters_enabled = bloom_filters_enabled,
        .input_format = options.input_format,
        .csv = csv_opts,
        .csv_report = &csv_report,
        .csv_report_alloc = arena,
        .diag = diag,
        .profiler = profiler,
    };
    ctx.exit_after = options.exit_after;
    const started = nowMonoNs();
    var rows: usize = 0;
    var stats: SummaryStats = .{};
    var output_started_ns: i64 = 0;
    var text_bytes_out: u64 = 0;
    if (aggregate != null or group_by != null) {
        var result = try engine.runQuery(ctx, query_args);
        defer result.deinit(gpa);
        const ar = result.aggregate;
        const answer = ar.output;
        rows = if (query_args.empty_scalar) 0 else answer.rowCount();
        output_started_ns = ar.timings.output_started_ns;
        if (fmt != .parquet) text_bytes_out = ar.bytes_out;
        stats = .{ .aggregate = true, .rows_in = ar.rows_in, .rows_scanned = ar.rows_scanned, .rows_matched = ar.rows_matched, .bytes_in = ar.bytes_in, .bytes_out = if (fmt == .parquet) ar.bytes_out else text_bytes_out, .files_in = ar.files_in, .row_groups_in = ar.row_groups_in, .row_groups_pruned = ar.row_groups_pruned, .row_groups_full_match = ar.row_groups_full_match, .cols_stat_pruned = ar.cols_stat_pruned, .membership = ar.membership, .timings = ar.timings };
    } else if (fmt == .parquet and limit == null and result_order.offset == 0 and result_order.rows.len == 0) {
        const result = (try engine.runQuery(ctx, query_args)).write;
        rows = @intCast(result.rows_matched);
        output_started_ns = result.timings.output_started_ns;
        stats = .{ .rows_in = result.rows_in, .rows_scanned = result.rows_scanned, .rows_matched = result.rows_matched, .bytes_in = result.bytes_in, .bytes_out = result.bytes_out, .files_in = result.files_in, .row_groups_in = result.row_groups_in, .row_groups_pruned = result.row_groups_pruned, .membership = result.membership, .timings = result.timings, .codec = result.codec };
        if (csv_report.chunks > 0) stats.csv = &csv_report;
    } else {
        if (!to_stdout and std.mem.startsWith(u8, engine_output.?, "s3://")) {
            diagnosticPrint("zpq query: this output format does not support S3 destinations\n", .{});
            return error.BadArgs;
        }
        const fd: local_fs.fd_t = if (to_stdout) (if (@import("builtin").os.tag == .windows) std.Io.File.stdout().handle else 1) else publication.fd orelse return error.BadOutputUrl;
        const printed = try engine.runPrintTo(ctx, query_args, fmt, limit, fd, @intCast(result_order.offset));
        rows = printed.rows;
        output_started_ns = printed.timings.output_started_ns;
        stats = .{ .rows_in = @intCast(printed.rows_in), .rows_scanned = @intCast(printed.rows_scanned), .rows_matched = @intCast(printed.rows_matched), .bytes_out = printed.bytes_out, .files_in = inputs.len, .row_groups_in = printed.row_groups_in, .row_groups_pruned = printed.row_groups_pruned, .membership = printed.membership, .timings = printed.timings };
    }
    if (output_started_ns == 0) output_started_ns = nowMonoNs();
    try publication.commit(io);
    const published_ns = nowMonoNs();
    if (fmt == .parquet and (aggregate != null or group_by != null or limit != null or result_order.offset != 0))
        stats.codec = codec orelse .SNAPPY;
    stats.codec_applicable = fmt == .parquet;
    stats.output_ns = @intCast(published_ns - output_started_ns);
    const total_ns = nowMonoNs() - started;
    if (profiler) |p| try profile_out.emit(gpa, io, p);
    if (summary) try writeSummary(if (to_stdout) null else output, rows, stats, total_ns);
}

/// `COL:TYPE[,COL:TYPE...]`; a DECIMAL(p,s)'s comma is inside its parentheses.
fn parseCsvTypes(
    gpa: std.mem.Allocator,
    spec: []const u8,
    out: *std.ArrayList(engine.csv_write.Options.Pinned),
) !void {
    var depth: usize = 0;
    var start: usize = 0;
    for (spec, 0..) |c, i| {
        if (c == '(') depth += 1;
        if (c == ')') depth -|= 1;
        if ((c == ',' and depth == 0) or i + 1 == spec.len) {
            const item = std.mem.trim(u8, spec[start..if (c == ',' and depth == 0) i else i + 1], " ");
            start = i + 1;
            const colon = std.mem.lastIndexOfScalar(u8, item, ':') orelse return error.BadArgs;
            const t = zpq.core.csv.types.parseName(std.mem.trim(u8, item[colon + 1 ..], " ")) orelse return error.BadArgs;
            if (colon == 0) return error.BadArgs;
            try out.append(gpa, .{ .column = item[0..colon], .type = t });
        }
    }
    if (out.items.len == 0) return error.BadArgs;
}

/// `UPDATE 'file' SET ... WHERE ...` (`engine.update`): in place unless `output` names another file.
fn runUpdate(
    io: std.Io,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    up: zpq.core.expr.sql_parser.ParsedUpdate,
    output: ?[]const u8,
    fsync: bool,
    summary: bool,
    max_memory: usize,
) !void {
    const started = nowMonoNs();
    if (output) |o| if (std.mem.eql(u8, o, "-")) {
        diagnosticPrint("zpq query: UPDATE writes a file; -o - names none\n", .{});
        return error.BadArgs;
    };
    const sets = try arena.alloc(engine.update.Set, up.sets.len);
    for (sets, up.sets) |*s, u| s.* = .{ .column = u.column, .assignment = u.assignment };
    var problem: engine.update.Problem = .{};
    const s = engine.update.run(gpa, io, .{
        .target = up.table,
        .output = output,
        .where = up.where,
        .sets = sets,
        .fsync = fsync,
        .max_memory = max_memory,
    }, &problem) catch |err| {
        if (problem.len > 0) {
            diagnosticPrint("zpq query: {s}\n", .{problem.text()});
            // A refusal is a usage error (exit 2); a publication that failed half-way is not, and says where things are.
            return if (err == error.RollbackFailed) err else error.BadArgs;
        }
        return err;
    };
    if (!summary) return;
    const elapsed = nowMonoNs() - started;
    var w: StdoutWriter = .{ .fd = std.Io.File.stderr().handle };
    try w.writeAll("{\"type\":\"summary\",\"ok\":true,\"statement\":\"update\",\"output\":");
    if (s.unchanged) try w.writeAll("null") else {
        try w.writeByte('"');
        try writeJsonString(&w, output orelse up.table);
        try w.writeByte('"');
    }
    try w.print(",\"rows_in\":{d},\"row_groups_pruned\":{d},\"rows_matched\":{d},\"values_changed\":{d},\"pages_rewritten\":{d},\"bytes_out\":{d}", .{
        s.rows_in, s.row_groups_pruned, s.rows_matched, s.values_changed, s.pages_rewritten, s.bytes_out,
    });
    if (!s.unchanged) {
        const path = if (s.kernel_bytes == s.copied_bytes) "range_copy" else if (s.kernel_bytes == 0) "write" else "mixed";
        try w.print(",\"byte_copy\":{{\"bytes\":{d},\"kernel_bytes\":{d},\"written_bytes\":{d},\"pad_bytes\":{d},\"path\":\"{s}\"}}", .{
            s.copied_bytes, s.kernel_bytes, s.copied_bytes - s.kernel_bytes, s.pad_bytes, path,
        });
    }
    try w.print(",\"total_ms\":{d},\"total_us\":{d}}}\n", .{
        @divTrunc(elapsed, std.time.ns_per_ms), @divTrunc(elapsed, std.time.ns_per_us),
    });
    try w.flush();
}

const SummaryStats = struct {
    /// An aggregate: its workers' third phase folds rows into aggregates, where a write's or a row output's encodes.
    aggregate: bool = false,
    output_ns: u64 = 0,
    rows_in: i64 = 0,
    rows_scanned: i64 = 0,
    rows_matched: i64 = 0,
    bytes_in: u64 = 0,
    bytes_out: u64 = 0,
    files_in: usize = 0,
    row_groups_in: usize = 0,
    row_groups_pruned: usize = 0,
    row_groups_full_match: usize = 0,
    cols_stat_pruned: usize = 0,
    membership: zpq.core.filter.membership.Stats = .{},
    timings: engine.Timings = .{},
    codec: ?schema.CompressionCodec = null,
    codec_applicable: bool = false,
    /// A CSV input's report, when the input was CSV.
    csv: ?*const engine.csv_write.Report = null,
};

/// A CSV input's `"csv"` object: how it was read, the restarts its types and its budget took, and the types written.
fn writeCsvSummary(w: *StdoutWriter, r: *const engine.csv_write.Report) !void {
    const read: []const u8 = if (r.io) |c| @tagName(c.mode) else "mmap";
    try w.print("\"csv\":{{\"read\":\"{s}\",\"chunks\":{d},\"restarts\":{d},\"narrowed\":{d},\"widened\":[", .{
        read, r.chunks, r.restarts, r.narrowed,
    });
    var first = true;
    for (r.widened, 0..) |wd, i| if (wd and i < r.names.len) {
        if (!first) try w.writeByte(',');
        first = false;
        try w.writeByte('"');
        try writeJsonString(w, r.names[i]);
        try w.writeByte('"');
    };
    try w.writeAll("],\"types\":{");
    for (r.names, r.types, 0..) |n, t, i| {
        if (i > 0) try w.writeByte(',');
        var buf: [64]u8 = undefined;
        try w.writeByte('"');
        try writeJsonString(w, n);
        try w.print("\":\"{s}\"", .{t.name(&buf)});
    }
    try w.writeAll("}},");
}

fn writeSummary(output: ?[]const u8, rows: usize, s: SummaryStats, elapsed: i64) !void {
    var w: StdoutWriter = .{ .fd = std.Io.File.stderr().handle };
    try w.print("{{\"type\":\"summary\",\"ok\":true,\"rows\":{d},\"output\":", .{rows});
    if (output) |path| {
        try w.writeByte('"');
        try writeJsonString(&w, path);
        try w.writeByte('"');
    } else try w.writeAll("null");
    try w.print(",\"rows_in\":{d},\"rows_scanned\":{d},\"rows_matched\":{d},\"bytes_in\":{d},\"bytes_out\":{d},\"files_in\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"row_groups_full_match\":{d},\"cols_stat_pruned\":{d},\"codec\":", .{
        s.rows_in,  s.rows_scanned,  s.rows_matched,      s.bytes_in,              s.bytes_out,
        s.files_in, s.row_groups_in, s.row_groups_pruned, s.row_groups_full_match, s.cols_stat_pruned,
    });
    if (s.codec) |codec| try w.print("\"{s}\",", .{@tagName(codec)}) else try w.writeAll(if (s.codec_applicable) "\"MIXED\"," else "null,");
    if (s.csv) |r| try writeCsvSummary(&w, r);
    try engine.writeMembershipJson(&w, s.membership);
    const c = s.timings.core;
    if (c.copied_bytes > 0) {
        // Kernel bytes were shared with the input (a reflink filesystem) or copied without user space; nothing here
        // can tell which.
        const path = if (c.kernel_bytes == c.copied_bytes)
            "range_copy"
        else if (c.kernel_bytes == 0) "write" else "mixed";
        try w.print(",\"byte_copy\":{{\"bytes\":{d},\"kernel_bytes\":{d},\"written_bytes\":{d},\"pad_bytes\":{d},\"path\":\"{s}\"}}", .{
            c.copied_bytes, c.kernel_bytes, c.copied_bytes - c.kernel_bytes, c.pad_bytes, path,
        });
    }
    // Wall-clock phases run one after another (S3 reads also continue under the scan) and sum to at most total_ms.
    // Worker phases are summed over every worker, so with N workers they can reach N times the scan's wall time.
    const ms = std.time.ns_per_ms;
    try w.print(",\"total_ms\":{d},\"total_us\":{d},\"output_us\":{d},\"wall_ms\":{{\"open\":{d},\"parse\":{d},\"scan\":{d},\"footer\":{d}}},\"worker_ms\":{{\"decode\":{d},\"filter\":{d},\"fold\":{d},\"encode\":{d},\"sink\":{d}}}}}\n", .{
        @divTrunc(elapsed, std.time.ns_per_ms),   @divTrunc(elapsed, std.time.ns_per_us),   s.output_ns / std.time.ns_per_us,
        s.timings.read_ns / ms,                   s.timings.parse_ns / ms,                  s.timings.decode_wall_ns / ms,
        s.timings.footer_ns / ms,                 c.decode_ns / ms,                         c.eval_ns / ms,
        if (s.aggregate) c.encode_ns / ms else 0, if (s.aggregate) 0 else c.encode_ns / ms, c.sink_ns / ms,
    });
    try w.flush();
}

fn queryExitCode(err: anyerror, execution_started: bool) u8 {
    if (err == error.BrokenPipe) return 141;
    if (execution_started) return 1;
    const usage = [_][]const u8{
        "BadArgs",                     "NoInputs",              "NoMatches",                    "SqlNotCompiledIn",            "UnknownColumn",              "UnknownFunction",
        "UnknownAggFunc",              "WrongArity",            "EmptyExpr",                    "EmptyAggregate",              "EmptyColumnList",            "UnexpectedChar",
        "UnexpectedEnd",               "BadNumber",             "UnterminatedString",           "TypeMismatch",                "ExpectedLParen",             "ExpectedRParen",
        "ExpectedIdentifier",          "ExpectedWhere",         "ExpectedAggFunc",              "StarOnlyValidInCount",        "TrailingTokens",             "BadAggArg",
        "AggregateMutexWithSelect",    "DuplicateOutputColumn", "AmbiguousOutputColumn",        "AmbiguousColumn",             "ExpressionTooDeep",          "GroupKeyAliasRequired",
        "SqlParseError",               "OrderByNotSupported",   "LimitNotSupported",            "DistinctNotSupported",        "HavingNotSupported",         "MixedSelectNotSupported",
        "UnsupportedStatement",        "SubqueryNotSupported",  "WindowNotSupported",           "ColumnMustBeGrouped",         "BadOutputUrl",               "BadOperator",
        "BadValue",                    "BloomFilterNeedsRows",  "BloomFilterUnsupportedColumn", "Unsigned64Operand",           "NullableNotSupported",       "NestedNotSupported",
        "NestedRowOutputNotSupported", "UnbalancedParentheses", "FilterTooDeep",                "AggregateInGroupBy",          "HavingTypeMismatch",         "UnsupportedCase",
        "UnsupportedType",             "DecimalScaleTooLarge",  "MissingOutputOrAggregate",     "SelectAliasRequired",         "ColumnsNotSupported",        "ColumnsNoMatch",
        "ColumnsExcludeMissing",       "WindowFloatAggregate",  "WindowArgumentType",           "MultipleStatements",          "CteNotSupported",            "AggregateInWhere",
        "WindowRangeKey",              "WindowRangeCast",       "MissingFromClause",            "MissingTableName",            "UnsupportedFromClause",      "UnsupportedAggType",
        "UnsupportedCoalesce",         "UnsupportedColumnType", "UnlabelledColumn",             "INT96ReencodeNotSupported",   "NestedReencodeNotSupported", "NestedFilterNotSupported",
        "ModuloNotSupported",          "ExpressionInPredicate", "NestedColumnSelected",         "NestedAggregateNotSupported", "NestedGroupKeyNotSupported", "UnsupportedDatePart",
        "InvalidRegex",                "UnsupportedRegex",      "RegexTooBig",                  "TypedLiteralColumn",          "BadInterval",                "IntervalNotSupported",
        "TemporalOutOfRange",          "ConstantTooDeep",       "IlikePatternNotAscii",         "PostAggNotSupported",         "UpdateWithoutWhere",         "UnparseError",
        "SetOperationAll",             "SetOperationByName",    "SetOperationOrder",            "SetOperationNesting",         "SetOperationColumns",        "SetOperationTypes",
        "SetOperationNullability",     "GroupingSetsKey",       "GroupingSetsDistinct",         "GroupingSetsNesting",         "GroupingSetsGrouping",       "GroupingSetsTooMany",
        "CteRecursive",                "CteDuplicate",          "CteCircular",                  "LateralNotSupported",         "JoinRowsNotSupported",       "JoinRowsShape",
        "StarWithItemsNotSupported",   "GroupingSetsQualify",
    };
    for (usage) |name| if (std.mem.eql(u8, name, @errorName(err))) return 2;
    return 1;
}

/// Where `--profile` and `--profile-summary` send a finished profile.
const ProfileOut = struct {
    trace_path: ?[]const u8 = null,
    summary: ?Format = null,
    /// A new file beside `trace_path`, created by `open` before the query runs. It replaces `trace_path` only once the
    /// trace is written whole, so a failed query leaves an earlier trace there as it was.
    trace_fd: ?local_fs.fd_t = null,
    tmp_buf: [std.fs.max_path_bytes]u8 = undefined,
    tmp_len: usize = 0,

    const Format = enum { text, json };

    fn wanted(self: ProfileOut) bool {
        return self.trace_path != null or self.summary != null;
    }

    fn open(self: *ProfileOut, io: std.Io) !void {
        const path = self.trace_path orelse return;
        // The trace replaces what is at its path only after the query, so anything a rename can't sensibly replace is
        // refused now, before the query has side effects. An unwritable directory fails creating the temporary below.
        if (std.Io.Dir.cwd().statFile(io, path, .{})) |st| {
            if (st.kind != .file) {
                const why = if (st.kind == .directory) "IsDir" else "NotAFile";
                diagnosticPrint("zpq query: cannot write profile {s}: {s}\n", .{ path, why });
                return error.BadArgs;
            }
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => {
                diagnosticPrint("zpq query: cannot write profile {s}: {s}\n", .{ path, @errorName(err) });
                return error.BadArgs;
            },
        }
        const pid: u32 = if (@import("builtin").os.tag == .windows)
            std.os.windows.GetCurrentProcessId()
        else
            @bitCast(std.posix.system.getpid());
        const tmp = std.fmt.bufPrint(&self.tmp_buf, "{s}.{d}.zpq-tmp", .{ path, pid }) catch {
            diagnosticPrint("zpq query: cannot write profile {s}: NameTooLong\n", .{path});
            return error.BadArgs;
        };
        self.trace_fd = local_fs.createNewFile(tmp) catch |err| {
            diagnosticPrint("zpq query: cannot write profile {s}: {s}\n", .{ path, @errorName(err) });
            return error.BadArgs;
        };
        self.tmp_len = tmp.len;
    }

    fn tmpPath(self: *const ProfileOut) []const u8 {
        return self.tmp_buf[0..self.tmp_len];
    }

    /// Drop a trace not yet written whole: its temporary file goes, and `trace_path` is left as it was.
    fn close(self: *ProfileOut) void {
        const fd = self.trace_fd orelse return;
        local_fs.close(fd);
        self.trace_fd = null;
        local_fs.removeFile(self.tmpPath());
    }

    /// The trace streams to its file, never held whole in memory, and replaces `trace_path` once complete; the
    /// summary, a few KB, goes to stderr.
    fn emit(self: *ProfileOut, gpa: std.mem.Allocator, io: std.Io, p: *zpq.profile.Profiler) !void {
        p.finish();
        if (self.trace_fd) |fd| {
            const file: std.Io.File = .{ .handle = fd, .flags = .{ .nonblocking = false } };
            var buf: [64 * 1024]u8 = undefined;
            var fw = file.writerStreaming(io, &buf);
            const wrote = if (p.writeTrace(gpa, &fw.interface)) fw.interface.flush() else |err| err;
            wrote catch |err| {
                const why = if (fw.err) |e| @errorName(e) else @errorName(err);
                diagnosticPrint("zpq query: cannot write profile {s}: {s}\n", .{ self.trace_path.?, why });
                return error.ProfileWriteFailed;
            };
            self.trace_fd = null;
            local_fs.closeChecked(fd) catch |err| {
                local_fs.removeFile(self.tmpPath());
                return err;
            };
            local_fs.rename(self.tmpPath(), self.trace_path.?) catch |err| {
                local_fs.removeFile(self.tmpPath());
                diagnosticPrint("zpq query: cannot write profile {s}: {s}\n", .{ self.trace_path.?, @errorName(err) });
                return error.ProfileWriteFailed;
            };
        }
        if (self.summary) |fmt| {
            var aw: std.Io.Writer.Allocating = .init(gpa);
            defer aw.deinit();
            const summary = try zpq.profile_summary.compute(gpa, p);
            switch (fmt) {
                .text => try zpq.profile_summary.writeText(summary, p, &aw.writer),
                .json => {
                    try zpq.profile_summary.writeJson(summary, p, &aw.writer);
                    try aw.writer.writeByte('\n');
                },
            }
            var w: StdoutWriter = .{ .fd = std.Io.File.stderr().handle };
            defer w.flush() catch {};
            if (machine_diagnostics and fmt == .json) {
                try w.writeAll("{\"type\":\"profile\",");
                try w.writeAll(aw.written()[1..]);
            } else try w.writeAll(aw.written());
            try w.flush();
        }
    }
};

/// The command line as typed, arguments with spaces or quotes single-quoted, for the profile's metadata.
fn commandLine(arena: std.mem.Allocator, args: std.process.Args) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var it = if (@import("builtin").os.tag == .windows) try std.process.Args.Iterator.initAllocator(args, arena) else std.process.Args.Iterator.init(args);
    defer if (@import("builtin").os.tag == .windows) it.deinit();
    var first = true;
    while (it.next()) |a| {
        if (!first) try out.append(arena, ' ');
        first = false;
        const plain = a.len > 0 and std.mem.indexOfAny(u8, a, " \t'\"*$\\") == null;
        if (plain) {
            try out.appendSlice(arena, a);
            continue;
        }
        try out.append(arena, '\'');
        for (a) |c| if (c == '\'') try out.appendSlice(arena, "'\\''") else try out.append(arena, c);
        try out.append(arena, '\'');
    }
    return out.items;
}

fn splitCsv(arena: std.mem.Allocator, csv: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var iter = std.mem.splitScalar(u8, csv, ',');
    while (iter.next()) |s| {
        const trimmed = std.mem.trim(u8, s, " \t");
        if (trimmed.len > 0) try out.append(arena, trimmed);
    }
    return out.items;
}

/// Expand a path that may contain `*` against the local filesystem.
/// `data/*.parquet` → all matching files in `data/`, sorted.
const expandGlob = zpq.frontend.expandGlob;

/// Open a parquet file via zpq and emit a JSON report describing what
/// we saw — schema leaves, row count, and a per-column-chunk decode
/// status (ok/error). Output goes to stdout. Caller (Python harness)
/// compares against pyarrow.
fn runConform(gpa: std.mem.Allocator, path: []const u8) !bool {
    const file_bytes = readFile(gpa, path) catch |err| {
        diagnosticPrint("zpq conform: {s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
        try jsonFatal(gpa, "open_failed", err);
        return false;
    };
    defer gpa.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const meta = metadata.open(arena, file_bytes) catch |err| {
        diagnosticPrint("zpq conform: {s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
        try jsonFatal(gpa, "metadata_open_failed", err);
        return false;
    };

    var w: StdoutWriter = .{};
    defer w.flush() catch {};

    try w.writeAll("{\"path\":\"");
    try writeJsonString(&w, path);
    try w.print("\",\"num_rows\":{d},\"num_row_groups\":{d},\"num_schema_elems\":{d},\"created_by\":\"", .{ meta.num_rows, meta.row_groups.items.len, meta.schema.items.len });
    if (meta.created_by) |cb| try writeJsonString(&w, cb);

    // Build the schema tree — gives nested-aware leaf info. If this
    // fails, the file's schema is something we can't represent.
    const tree_result = schema_tree.SchemaTree.build(arena, meta.schema.items);
    if (tree_result) |tree| {
        try w.print("\",\"tree_leaves\":{d},\"leaves\":[", .{tree.leaves.len});
        var first_leaf = true;
        for (tree.leaves) |leaf| {
            if (!first_leaf) try w.writeAll(",");
            first_leaf = false;
            try w.writeAll("{\"name\":\"");
            try writeJsonString(&w, leaf.name);
            try w.writeAll("\",\"path\":\"");
            // Joined path for easy comparison with pyarrow / hardwood.
            for (leaf.path, 0..) |seg, i| {
                if (i > 0) try w.writeAll(".");
                try writeJsonString(&w, seg);
            }
            try w.writeAll("\",\"type\":\"");
            try w.writeAll(@tagName(leaf.type));
            try w.print("\",\"max_def\":{d},\"max_rep\":{d},\"column_index\":{d}}}", .{ leaf.max_def, leaf.max_rep, leaf.column_index });
        }
    } else |err| {
        try w.print("\",\"tree_build_failed\":\"{s}\",\"leaves\":[", .{@errorName(err)});
    }
    try w.writeAll("],\"decode\":[");

    // Try to decode every flat leaf in the first row group.
    var first_dec = true;
    if (meta.row_groups.items.len > 0) {
        const rg0 = &meta.row_groups.items[0];
        for (rg0.columns.items, 0..) |chunk, ci| {
            if (!first_dec) try w.writeAll(",");
            first_dec = false;
            try w.writeAll("{\"col_idx\":");
            try w.print("{d}", .{ci});
            try w.writeAll(",\"name\":\"");
            const cm = chunk.meta_data orelse {
                try w.writeAll("?\",\"status\":\"missing_meta\"}");
                continue;
            };
            const col_name = if (cm.path_in_schema.items.len > 0) cm.path_in_schema.items[0] else "?";
            try writeJsonString(&w, col_name);
            try w.writeAll("\",\"type\":\"");
            try w.writeAll(@tagName(cm.type));
            try w.writeAll("\",\"codec\":\"");
            try w.writeAll(@tagName(cm.codec));
            try w.writeAll("\",\"num_values\":");
            try w.print("{d}", .{cm.num_values});
            try w.writeAll(",\"status\":\"");

            const status = tryDecodeColumn(arena, file_bytes, &meta, rg0, chunk);
            try writeJsonString(&w, status);
            try w.writeAll("\"}");
        }
    }
    try w.writeAll("]}\n");
    try w.flush();
    return true;
}

fn tryDecodeColumn(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: *const schema.FileMetaData,
    rg: *const schema.RowGroup,
    chunk: schema.ColumnChunk,
) []const u8 {
    _ = rg;
    const cm = chunk.meta_data orelse return "missing_meta";
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    if (start + len > file_bytes.len) return "out_of_range";
    const chunk_bytes = file_bytes[start .. start + len];

    // Use the full path_in_schema (multi-element for nested columns)
    // so getColumnLevels walks the tree and computes max_def/max_rep
    // correctly, instead of treating every column as flat.
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const num_rows: usize = @intCast(cm.num_values);

    // FIXED_LEN_BYTE_ARRAY needs its fixed width so the reader uses the
    // fixed-len (not length-prefixed) decoder. 0 for everything else.
    const flba_tl: usize = if (cm.type == .FIXED_LEN_BYTE_ARRAY) blk: {
        const se = meta.getColumnSchema(cm.path_in_schema.items);
        break :blk if (se) |s| (if (s.type_length) |t| @intCast(t) else 0) else 0;
    } else 0;

    return switch (cm.type) {
        .INT32 => decodeOne(i32, arena, chunk_bytes, cm.codec, levels, num_rows, 0),
        .INT64 => decodeOne(i64, arena, chunk_bytes, cm.codec, levels, num_rows, 0),
        .FLOAT => decodeOne(f32, arena, chunk_bytes, cm.codec, levels, num_rows, 0),
        .DOUBLE => decodeOne(f64, arena, chunk_bytes, cm.codec, levels, num_rows, 0),
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => decodeOne([]const u8, arena, chunk_bytes, cm.codec, levels, num_rows, flba_tl),
        .BOOLEAN => decodeOne(bool, arena, chunk_bytes, cm.codec, levels, num_rows, 0),
        .INT96 => "skip_int96",
    };
}

fn decodeOne(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk_bytes: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_rows: usize,
    type_length: usize,
) []const u8 {
    var rdr = column.ColumnChunkReader(T).init(chunk_bytes, codec, levels, arena);
    rdr.type_length = type_length;
    _ = rdr.readAll(num_rows) catch |e| return switch (e) {
        error.OutOfMemory => "alloc_failed",
        error.ShortDecode => "short_decode",
        else => @errorName(e),
    };
    return "ok";
}

fn jsonFatal(gpa: std.mem.Allocator, kind: []const u8, err: anyerror) !void {
    var w: StdoutWriter = .{};
    defer w.flush() catch {};
    _ = gpa;
    try w.writeAll("{\"error\":\"");
    try writeJsonString(&w, kind);
    try w.writeAll("\",\"reason\":\"");
    try writeJsonString(&w, @errorName(err));
    try w.writeAll("\",\"message\":\"");
    try writeJsonString(&w, zpq.frontend.failureMessage(err));
    try w.writeAll("\"}\n");
    try w.flush();
}

fn writeJsonString(w: *StdoutWriter, s: []const u8) !void {
    try engine.writeJsonString(w, s);
}

const StdoutWriter = struct {
    fd: if (@import("builtin").os.tag == .windows) ?local_fs.fd_t else local_fs.fd_t = if (@import("builtin").os.tag == .windows) null else 1,
    inner: ?engine.StdoutWriter = null,

    fn getInner(self: *StdoutWriter) *engine.StdoutWriter {
        if (self.inner == null) {
            self.inner = .{ .fd = self.fd };
        }
        return &self.inner.?;
    }

    pub fn writeAll(self: *StdoutWriter, bytes: []const u8) !void {
        try self.getInner().writeAll(bytes);
    }

    pub fn print(self: *StdoutWriter, comptime fmt: []const u8, args: anytype) !void {
        try self.getInner().print(fmt, args);
    }

    pub fn writeByte(self: *StdoutWriter, c: u8) !void {
        try self.getInner().writeByte(c);
    }

    pub fn flush(self: *StdoutWriter) !void {
        if (self.inner) |*in| try in.flush();
    }
};

fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    return local_fs.readFile(allocator, path) catch |err| switch (err) {
        error.OutOfMemory, error.ShortRead => err,
        error.NameTooLong => error.PathTooLong,
        else => error.OpenFailed,
    };
}

fn printLogicalType(ws: *StdoutWriter, lt: schema.LogicalType) !void {
    switch (lt) {
        .STRING => try ws.writeAll("STRING"),
        .MAP => try ws.writeAll("MAP"),
        .LIST => try ws.writeAll("LIST"),
        .ENUM => try ws.writeAll("ENUM"),
        .DECIMAL => |d| try ws.print("DECIMAL({d},{d})", .{ d.precision, d.scale }),
        .DATE => try ws.writeAll("DATE"),
        .TIME => |t| try ws.print("TIME({s},isAdjustedToUTC={})", .{ @tagName(t.unit), t.isAdjustedToUTC }),
        .TIMESTAMP => |ts| try ws.print("TIMESTAMP({s},isAdjustedToUTC={})", .{ @tagName(ts.unit), ts.isAdjustedToUTC }),
        .INTEGER => |i| try ws.print("INTEGER({d},{s})", .{ i.bitWidth, if (i.isSigned) "signed" else "unsigned" }),
        .UNKNOWN => try ws.writeAll("UNKNOWN"),
        .JSON => try ws.writeAll("JSON"),
        .BSON => try ws.writeAll("BSON"),
        .UUID => try ws.writeAll("UUID"),
        .FLOAT16 => try ws.writeAll("FLOAT16"),
    }
}

fn printLogicalOrConverted(ws: *StdoutWriter, leaf: schema_tree.PrimitiveNode) !void {
    if (leaf.logical_type) |lt| {
        try ws.writeAll(" [Logical: ");
        try printLogicalType(ws, lt);
        try ws.writeAll("]");
    } else if (leaf.converted_type) |ct| {
        try ws.writeAll(" [Logical: ");
        switch (ct) {
            .UTF8 => try ws.writeAll("STRING"),
            .MAP => try ws.writeAll("MAP"),
            .MAP_KEY_VALUE => try ws.writeAll("MAP"),
            .LIST => try ws.writeAll("LIST"),
            .ENUM => try ws.writeAll("ENUM"),
            .DECIMAL => {
                const p = leaf.precision orelse 0;
                const s = leaf.scale orelse 0;
                try ws.print("DECIMAL({d},{d})", .{ p, s });
            },
            .DATE => try ws.writeAll("DATE"),
            .TIME_MILLIS => try ws.writeAll("TIME(MILLIS,isAdjustedToUTC=true)"),
            .TIME_MICROS => try ws.writeAll("TIME(MICROS,isAdjustedToUTC=true)"),
            .TIMESTAMP_MILLIS => try ws.writeAll("TIMESTAMP(MILLIS,isAdjustedToUTC=true)"),
            .TIMESTAMP_MICROS => try ws.writeAll("TIMESTAMP(MICROS,isAdjustedToUTC=true)"),
            .UINT_8 => try ws.writeAll("INTEGER(8,unsigned)"),
            .UINT_16 => try ws.writeAll("INTEGER(16,unsigned)"),
            .UINT_32 => try ws.writeAll("INTEGER(32,unsigned)"),
            .UINT_64 => try ws.writeAll("INTEGER(64,unsigned)"),
            .INT_8 => try ws.writeAll("INTEGER(8,signed)"),
            .INT_16 => try ws.writeAll("INTEGER(16,signed)"),
            .INT_32 => try ws.writeAll("INTEGER(32,signed)"),
            .INT_64 => try ws.writeAll("INTEGER(64,signed)"),
            .JSON => try ws.writeAll("JSON"),
            .BSON => try ws.writeAll("BSON"),
            .INTERVAL => try ws.writeAll("INTERVAL"),
        }
        try ws.writeAll("]");
    }
}

fn runSchema(init: std.process.Init, path: []const u8) !void {
    const gpa = zpq.core.memory.processAllocator(init.gpa);
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var file_bytes: ?[]u8 = null;
    defer if (file_bytes) |fb| gpa.free(fb);

    var meta: schema.FileMetaData = undefined;
    if (std.mem.startsWith(u8, path, "s3://")) {
        meta = engine.fetchS3Schema(.{
            .gpa = gpa,
            .env = init.minimal.environ,
            .io = init.io,
        }, path, arena) catch |err| {
            diagnosticPrint("zpq schema: {s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
            std.process.exit(1);
        };
    } else {
        const bytes = local_fs.readFile(gpa, path) catch |err| {
            diagnosticPrint("zpq schema: {s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
            std.process.exit(1);
        };
        file_bytes = bytes;

        meta = metadata.open(arena, bytes) catch |err| {
            diagnosticPrint("zpq schema: {s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
            std.process.exit(1);
        };
    }

    var ws: StdoutWriter = .{};
    defer ws.flush() catch {};

    var codec_name: []const u8 = "UNCOMPRESSED";
    if (meta.row_groups.items.len > 0 and meta.row_groups.items[0].columns.items.len > 0) {
        if (meta.row_groups.items[0].columns.items[0].meta_data) |cm| {
            codec_name = @tagName(cm.codec);
        }
    }

    try ws.print("rows: {d}, row_groups: {d}, codec: {s}\n", .{ meta.num_rows, meta.row_groups.items.len, codec_name });

    const tree = schema_tree.SchemaTree.build(arena, meta.schema.items) catch |err| {
        diagnosticPrint("zpq schema: {s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
        std.process.exit(1);
    };

    for (tree.leaves) |leaf| {
        for (leaf.path, 0..) |seg, i| {
            if (i > 0) try ws.writeAll(".");
            try ws.writeAll(seg);
        }
        try ws.print(": {s} ({s})", .{ @tagName(leaf.type), @tagName(leaf.repetition) });
        try printLogicalOrConverted(&ws, leaf);
        try ws.writeAll("\n");
    }
    try ws.flush();
}

fn runInspect(init: std.process.Init, iter: *std.process.Args.Iterator) !void {
    var arena_state = std.heap.ArenaAllocator.init(zpq.core.memory.processAllocator(init.gpa));
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var paths: std.ArrayList([]const u8) = .empty;
    var json = false;
    var detail = false;
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--format")) {
            if (!std.mem.eql(u8, iter.next() orelse "", "json")) {
                diagnosticPrint("zpq inspect: --format requires json; omit it for text\n", .{});
                return error.BadArgs;
            }
            json = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            var help: StdoutWriter = .{};
            try help.writeAll(usage_text);
            try help.flush();
            return;
        } else if (std.mem.eql(u8, arg, "--detail")) {
            detail = true;
        } else if (std.mem.startsWith(u8, arg, "-")) {
            diagnosticPrint("zpq inspect: unrecognized option: {s}; see `zpq --help`\n", .{arg});
            return error.BadArgs;
        } else {
            const expanded = try expandGlob(arena, init.io, arg, init.minimal.environ, null);
            if (expanded.len == 0) {
                diagnosticPrint("zpq inspect: no matches for {s}; check the basename wildcard\n", .{arg});
                return error.BadArgs;
            }
            try paths.appendSlice(arena, expanded);
        }
    }
    if (paths.items.len == 0) return error.NoInputs;
    var ws: StdoutWriter = .{};
    if (json) try ws.writeAll("{\"schema_version\":1,\"files\":[");
    var failures: usize = 0;
    for (paths.items, 0..) |path, i| {
        if (json and i != 0) try ws.writeByte(',');
        var file_arena = std.heap.ArenaAllocator.init(zpq.core.memory.processAllocator(init.gpa));
        defer file_arena.deinit();
        const report = @import("inspect.zig").inspect(file_arena.allocator(), init.minimal.environ, path, detail) catch |err| {
            failures += 1;
            diagnosticPrint("zpq inspect: {s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
            if (json) {
                try ws.writeAll(try std.json.Stringify.valueAlloc(file_arena.allocator(), .{
                    .path = path,
                    .status = "error",
                    .cause = @errorName(err),
                    .message = zpq.frontend.failureMessage(err),
                }, .{}));
            } else try ws.print("{s}: {s}\n", .{ path, zpq.frontend.failureMessage(err) });
            continue;
        };
        if (json) {
            try ws.writeAll(try std.json.Stringify.valueAlloc(file_arena.allocator(), report, .{}));
        } else {
            try ws.print("{s}: Parquet v{d}, {d} rows, {d} bytes, {d} row groups\n", .{
                path, report.parquet_version, report.rows, report.bytes, report.row_groups.len,
            });
            try ws.print("  created_by: {s}; sorting_columns: unsupported\n", .{report.created_by orelse "unknown"});
            for (report.row_groups) |group| {
                try ws.print("  row group {d}: {d} rows, {d} uncompressed bytes\n", .{
                    group.ordinal, group.rows, group.uncompressed_bytes,
                });
                for (group.columns) |col| {
                    try ws.writeAll("    ");
                    for (col.path, 0..) |part, pi| {
                        if (pi != 0) try ws.writeByte('.');
                        try ws.writeAll(part);
                    }
                    try ws.print(" {s} {s}: {d}/{d} compressed/uncompressed bytes\n", .{
                        col.physical_type, col.codec, col.compressed_bytes, col.uncompressed_bytes,
                    });
                    try ws.print("      min={s} ({s}) max={s} ({s}); nulls=", .{
                        col.min.value orelse col.min.state, col.min.exactness,
                        col.max.value orelse col.max.state, col.max.exactness,
                    });
                    if (col.null_count) |n| try ws.print("{d}", .{n}) else try ws.writeAll("unknown");
                    if (std.mem.eql(u8, col.physical_type, "FLOAT") or std.mem.eql(u8, col.physical_type, "DOUBLE")) {
                        if (col.nan_count) |n| try ws.print(", NaNs={d}", .{n}) else try ws.writeAll(", NaNs=unknown");
                    }
                    try ws.writeByte('\n');
                    try ws.print("      index={s}/{s}, Bloom={s}, dictionary fallback={s}\n", .{
                        col.column_index.state, col.offset_index.state, col.bloom_filter.state, col.dictionary_fallback,
                    });
                    for (col.hints) |hint| try ws.print("      hint: {s}\n", .{hint});
                }
            }
            try ws.writeAll("  Use --format json for full types/offsets/encodings; --detail is bounded.\n");
        }
    }
    if (json) try ws.writeAll("]}\n");
    try ws.flush();
    if (failures != 0) return error.InspectionFailed;
}

test {
    // The test-bearing files under this root, listed by build.zig (tests.gen.zig).
    _ = @import("tests.gen.zig");
}
