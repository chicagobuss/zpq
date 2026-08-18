//! ZPQ CLI binary entry point.
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
//!       Aggregate mode emits JSON to stdout; -o also writes a 1-row
//!       parquet. --aggregate is mutually exclusive with --select / --columns.
//!       JSON envelope with phase timings goes to stderr.
//!
//!   zpq conform <file.parquet>   — emit a JSON report of what ZPQ
//!                                  sees in this file. Used by the
//!                                  conformance runner against the
//!                                  apache/parquet-testing corpus.

const std = @import("std");
const zpq = @import("zpq");
const build_options = @import("build_options");

const metadata = zpq.core.parquet.metadata;
const schema = zpq.core.schema;
const column = zpq.core.parquet.column;
const schema_tree = zpq.core.parquet.schema_tree;
const engine = zpq.engine;
const s3 = zpq.io.s3;

const usage_text =
    \\usage:
    \\  zpq query <input.parquet> [--output <out.parquet>]
    \\            [--filter EXPR] [--columns COL1,COL2,...]
    \\            [--select "EXPR1 [AS name], EXPR2 [AS name], ..."]
    \\            [--aggregate "AGG(...) [FILTER (WHERE ...)] [AS name], ..."]
    \\            [--group-by "EXPR [AS name], ..."] [--column-order COL1,COL2,...]
    \\            [--codec snappy|zstd|gzip|lz4|lz4_raw|uncompressed] [--threads N | -j N]
    \\            [--scan-all] [--trust-stats]
    \\            [--format csv|jsonl] [--limit N]
    \\  zpq query --query "<sql query>" [--output <out.parquet>]
    \\            [--codec snappy|zstd|gzip|lz4|lz4_raw|uncompressed] [--threads N | -j N]
    \\            [--scan-all] [--trust-stats]
    \\  zpq schema <file.parquet>
    \\  zpq conform <file.parquet>
    \\
    \\  --scan-all     decode every page/byte: disables all stats shortcuts
    \\                 (row-group pruning, stats-as-answer). Slower but
    \\                 thorough — use when you don't trust a file's stats.
    \\  --trust-stats  answer min/max/sum from file statistics instead of
    \\                 decoding (fast, but trusts the writer's stats — off
    \\                 by default; count(*) is always answered from metadata).
    \\  --max-memory   ceiling on GROUP BY table memory across all workers.
    \\                 A hard cap: usage never exceeds it. Workers draw from
    \\                 it in blocks rather than each owning a fixed slice, so
    \\                 raising -j does not shrink what a query may use. One
    \\                 caveat: workers can briefly hold partly-used blocks,
    \\                 together at most ~5% of the budget, so a query sitting
    \\                 within a few percent of its ceiling may be accepted at
    \\                 one -j and rejected at another. Give it headroom
    \\                 rather than tuning it to the exact byte.
    \\
;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next(); // skip program name
    const cmd = iter.next() orelse {
        var ws: StdoutWriter = .{};
        defer ws.flush();
        try ws.writeAll(usage_text);
        return;
    };
    if (std.mem.eql(u8, cmd, "--help") or std.mem.eql(u8, cmd, "-h") or std.mem.eql(u8, cmd, "help")) {
        var ws: StdoutWriter = .{};
        defer ws.flush();
        try ws.writeAll(usage_text);
        return;
    }
    if (std.mem.eql(u8, cmd, "--version") or std.mem.eql(u8, cmd, "-V") or std.mem.eql(u8, cmd, "version")) {
        var ws: StdoutWriter = .{};
        defer ws.flush();
        try ws.print("zpq {s}\n", .{build_options.version});
        return;
    }
    if (std.mem.eql(u8, cmd, "query")) {
        runQuery(init, &iter) catch |err| {
            switch (err) {
                error.BadArgs, error.NoMatches, error.SqlNotCompiledIn, error.AlreadyReported => {},
                error.NoInputs => std.debug.print("zpq query: no input files specified\n", .{}),
                error.EmptyAggregate => std.debug.print("zpq query: empty aggregate expression\n", .{}),
                error.AggregateMutexWithSelect => std.debug.print("zpq query: aggregate functions are mutually exclusive with --select / --columns\n", .{}),
                error.MissingOutputOrAggregate => std.debug.print("zpq query: missing --output or --aggregate\n", .{}),
                error.SchemaMismatch => std.debug.print("zpq query: schema mismatch across inputs\n", .{}),
                error.NestedReencodeNotSupported => std.debug.print("zpq query: nested re-encoding is not supported yet\n", .{}),
                error.INT96ReencodeNotSupported => std.debug.print("zpq query: INT96 re-encoding is not supported yet\n", .{}),
                error.FooterSchemaChunkMismatch => std.debug.print("zpq query: footer schema chunk mismatch\n", .{}),
                error.CrossBucketNotSupported => std.debug.print("zpq query: cross-bucket queries are not supported\n", .{}),
                error.BadInputUrl => std.debug.print("zpq query: invalid input S3 URL\n", .{}),
                error.BadOutputUrl => std.debug.print("zpq query: invalid output S3 URL\n", .{}),
                error.NoCredentials => std.debug.print("zpq query: missing AWS credentials for S3 query\n", .{}),
                error.BadResponse => std.debug.print("zpq query: bad S3 HTTP response\n", .{}),
                error.TailTooSmall => std.debug.print("zpq query: file footer metadata tail too small\n", .{}),
                error.NotParquet, error.BadMagic => std.debug.print("zpq query: input file is not a valid Parquet file\n", .{}),
                error.AggSumOverflow => std.debug.print("zpq query: aggregate sum overflowed integer limits\n", .{}),
                error.OpenFailed => std.debug.print("zpq query: failed to open input file\n", .{}),
                error.EmptyFile => std.debug.print("zpq query: input file is empty\n", .{}),
                error.PathTooLong => std.debug.print("zpq query: input file path too long\n", .{}),
                error.EmptyExpr => std.debug.print("zpq query: empty expression\n", .{}),
                error.UnexpectedChar => std.debug.print("zpq query: unexpected character in expression\n", .{}),
                error.UnexpectedEnd => std.debug.print("zpq query: unexpected end of expression\n", .{}),
                error.BadNumber => std.debug.print("zpq query: invalid number in expression\n", .{}),
                error.UnknownColumn => std.debug.print("zpq query: unknown column referenced in expression\n", .{}),
                error.UnsupportedColumnType => std.debug.print("zpq query: unsupported column type\n", .{}),
                error.UnterminatedString => std.debug.print("zpq query: unterminated string literal in expression\n", .{}),
                error.TypeMismatch => std.debug.print("zpq query: type mismatch in expression\n", .{}),
                error.UnknownFunction => std.debug.print("zpq query: unknown scalar function in expression\n", .{}),
                error.UnknownAggFunc => std.debug.print("zpq query: unknown aggregate function in expression\n", .{}),
                error.WrongArity => std.debug.print("zpq query: wrong number of arguments for function\n", .{}),
                error.BadAggArg => std.debug.print("zpq query: invalid argument for aggregate function\n", .{}),
                error.ExpectedLParen => std.debug.print("zpq query: expected '(' in expression\n", .{}),
                error.ExpectedRParen => std.debug.print("zpq query: expected ')' in expression\n", .{}),
                error.ExpressionTooDeep => std.debug.print(
                    "zpq query: expression nests too deeply (limit {d}). Each nesting level " ++
                        "materializes another full intermediate column, so very deep expressions " ++
                        "can exhaust memory; split the expression or precompute part of it.\n",
                    .{zpq.core.expr.parser.MAX_EXPR_DEPTH},
                ),
                error.ExpectedIdentifier => std.debug.print("zpq query: expected column identifier in expression\n", .{}),
                error.ExpectedWhere => std.debug.print("zpq query: expected WHERE keyword in expression\n", .{}),
                error.ExpectedAggFunc => std.debug.print("zpq query: expected aggregate function\n", .{}),
                error.StarOnlyValidInCount => std.debug.print("zpq query: '*' is only valid inside count(*)\n", .{}),
                error.TrailingTokens => std.debug.print("zpq query: trailing tokens after expression\n", .{}),
                error.GroupKeyAliasRequired => std.debug.print("zpq query: non-trivial GROUP BY key requires AS alias\n", .{}),
                error.ExceededMemoryBudget => std.debug.print("zpq query: GROUP BY exceeded --max-memory budget\n", .{}),
                error.NullableNotSupported => std.debug.print("zpq query: nullable values are not supported in this expression\n", .{}),
                error.NestedNotSupported => std.debug.print("zpq query: nested columns are not supported in GROUP BY keys\n", .{}),
                error.DivisionByZero => std.debug.print("zpq query: division by zero in GROUP BY expression\n", .{}),
                error.GroupingNotSupported => std.debug.print("zpq query: grouping parentheses are not supported in filter\n", .{}),
                error.BadOperator => std.debug.print("zpq query: invalid operator in filter\n", .{}),
                error.BadValue => std.debug.print("zpq query: invalid value in filter\n", .{}),
                error.UnsupportedType => std.debug.print("zpq query: unsupported type in filter\n", .{}),
                else => return err,
            }
            std.process.exit(1);
        };
        return;
    }
    if (std.mem.eql(u8, cmd, "conform")) {
        const path = iter.next() orelse {
            std.debug.print("conform: missing path\n", .{});
            std.process.exit(1);
        };
        try runConform(gpa, path);
        return;
    }
    if (std.mem.eql(u8, cmd, "schema")) {
        const path = iter.next() orelse {
            std.debug.print("schema: missing path\n", .{});
            std.process.exit(1);
        };
        try runSchema(init, path);
        return;
    }
    std.debug.print("unknown subcommand: {s}\n", .{cmd});
    std.process.exit(1);
}

fn runQuery(init: std.process.Init, iter: *std.process.Args.Iterator) !void {
    const gpa = init.gpa;
    const env = init.minimal.environ;
    const io = init.io;
    var inputs_raw: std.ArrayList([]const u8) = .empty;
    defer inputs_raw.deinit(gpa);
    var output: ?[]const u8 = null;
    var filter: ?[]const u8 = null;
    var columns_csv: ?[]const u8 = null;
    var select: ?[]const u8 = null;
    var aggregate: ?[]const u8 = null;
    var query: ?[]const u8 = null;
    var codec: schema.CompressionCodec = .SNAPPY;
    var parallelism: usize = 0;
    var scan_all: bool = false;
    var trust_stats: bool = false;
    var format: ?engine.PrintFormat = null;
    var limit: ?usize = null;
    var max_memory: ?usize = null;
    var group_by: ?[]const u8 = null;
    var select_cols: ?[]const []const u8 = null;
    var column_order: ?[]const u8 = null;

    while (iter.next()) |tok| {
        if (std.mem.eql(u8, tok, "--help") or std.mem.eql(u8, tok, "-h")) {
            var ws: StdoutWriter = .{};
            defer ws.flush();
            try ws.writeAll(usage_text);
            return;
        } else if (std.mem.eql(u8, tok, "--output") or std.mem.eql(u8, tok, "-o")) {
            output = iter.next() orelse {
                std.debug.print("zpq query: --output requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--filter") or std.mem.eql(u8, tok, "-f")) {
            filter = iter.next() orelse {
                std.debug.print("zpq query: --filter requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--columns") or std.mem.eql(u8, tok, "-c")) {
            columns_csv = iter.next() orelse {
                std.debug.print("zpq query: --columns requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--select") or std.mem.eql(u8, tok, "-s")) {
            select = iter.next() orelse {
                std.debug.print("zpq query: --select requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--aggregate") or std.mem.eql(u8, tok, "-a")) {
            aggregate = iter.next() orelse {
                std.debug.print("zpq query: --aggregate requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--query") or std.mem.eql(u8, tok, "-q")) {
            query = iter.next() orelse {
                std.debug.print("zpq query: --query requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--codec")) {
            const v = iter.next() orelse {
                std.debug.print("zpq query: --codec requires a value\n", .{});
                return error.BadArgs;
            };
            if (std.ascii.eqlIgnoreCase(v, "zstd")) {
                codec = .ZSTD;
            } else if (std.ascii.eqlIgnoreCase(v, "uncompressed")) {
                codec = .UNCOMPRESSED;
            } else if (std.ascii.eqlIgnoreCase(v, "gzip")) {
                codec = .GZIP;
            } else if (std.ascii.eqlIgnoreCase(v, "lz4") or std.ascii.eqlIgnoreCase(v, "lz4_raw")) {
                codec = .LZ4_RAW;
            } else if (std.ascii.eqlIgnoreCase(v, "snappy")) {
                codec = .SNAPPY;
            } else {
                std.debug.print("zpq query: invalid codec: {s}\n", .{v});
                return error.BadArgs;
            }
        } else if (std.mem.eql(u8, tok, "--threads") or std.mem.eql(u8, tok, "-j")) {
            const v = iter.next() orelse {
                std.debug.print("zpq query: --threads requires a value\n", .{});
                return error.BadArgs;
            };
            parallelism = std.fmt.parseInt(usize, v, 10) catch {
                std.debug.print("zpq query: invalid threads value: {s}\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--scan-all")) {
            scan_all = true; // valueless: disable all stats shortcuts, decode everything
        } else if (std.mem.eql(u8, tok, "--trust-stats")) {
            trust_stats = true; // valueless: opt in to stats-as-answer for min/max/sum
        } else if (std.mem.eql(u8, tok, "--format")) {
            const v = iter.next() orelse {
                std.debug.print("zpq query: --format requires a value\n", .{});
                return error.BadArgs;
            };
            if (std.ascii.eqlIgnoreCase(v, "csv")) {
                format = .csv;
            } else if (std.ascii.eqlIgnoreCase(v, "jsonl")) {
                format = .jsonl;
            } else {
                std.debug.print("zpq query: invalid format: {s} (supported: csv, jsonl)\n", .{v});
                return error.BadArgs;
            }
        } else if (std.mem.eql(u8, tok, "--limit")) {
            const v = iter.next() orelse {
                std.debug.print("zpq query: --limit requires a value\n", .{});
                return error.BadArgs;
            };
            limit = std.fmt.parseInt(usize, v, 10) catch {
                std.debug.print("zpq query: invalid limit value: {s}\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--group-by")) {
            group_by = iter.next() orelse {
                std.debug.print("zpq query: --group-by requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--column-order")) {
            column_order = iter.next() orelse {
                std.debug.print("zpq query: --column-order requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--max-memory")) {
            const v = iter.next() orelse {
                std.debug.print("zpq query: --max-memory requires a value\n", .{});
                return error.BadArgs;
            };
            max_memory = zpq.core.system.parseSizeString(v) catch {
                std.debug.print("zpq query: invalid max-memory value: {s} (examples: 500MB, 1.5GB, 1024)\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.startsWith(u8, tok, "-")) {
            std.debug.print("zpq query: unrecognized option: {s}\n", .{tok});
            return error.BadArgs;
        } else {
            try inputs_raw.append(gpa, tok);
        }
    }

    if (query != null) {
        if (inputs_raw.items.len > 0) {
            std.debug.print("zpq query: cannot specify input files on the command line when using --query\n", .{});
            return error.BadArgs;
        }
        if (select != null or aggregate != null or filter != null or columns_csv != null or format != null or limit != null or group_by != null or column_order != null) {
            std.debug.print("zpq query: --query is mutually exclusive with --select / --aggregate / --filter / --columns / --format / --limit / --group-by / --column-order\n", .{});
            return error.BadArgs;
        }
    } else {
        if (inputs_raw.items.len == 0) {
            std.debug.print("zpq query: missing <input.parquet> [more.parquet ...]\n", .{});
            return error.BadArgs;
        }
        if (aggregate != null and (select != null or columns_csv != null or format != null)) {
            std.debug.print("zpq query: --aggregate is mutually exclusive with --select / --columns / --format\n", .{});
            return error.BadArgs;
        }
        if (group_by != null and (select != null or columns_csv != null or format != null)) {
            std.debug.print("zpq query: --group-by is mutually exclusive with --select / --columns / --format\n", .{});
            return error.BadArgs;
        }
        if (column_order != null and group_by == null) {
            std.debug.print("zpq query: --column-order requires --group-by\n", .{});
            return error.BadArgs;
        }
        if (format != null and output != null) {
            std.debug.print("zpq query: --format is mutually exclusive with --output\n", .{});
            return error.BadArgs;
        }
        if (limit != null and format == null) {
            std.debug.print("zpq query: --limit is only supported with --format\n", .{});
            return error.BadArgs;
        }
    }

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

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
            const pq = try zpq.core.expr.sql_parser.parseSqlQuery(arena, q_str);
            filter = pq.filter_str;
            select = pq.select_str;
            aggregate = pq.aggregate_str;
            group_by = pq.group_by_str;
            select_cols = pq.select_cols;

            const matches = try expandGlob(arena, pq.table_name, env);
            if (matches.len == 0) {
                std.debug.print("zpq query: no files matched FROM table: {s}\n", .{pq.table_name});
                return error.NoMatches;
            }
            for (matches) |m| try inputs_list.append(arena, m);
        } else {
            // Referencing q_str here keeps it "used" in the minimal build,
            // where the comptime-true branch above is eliminated.
            std.debug.print("zpq: this build has no SQL frontend — `query` ({s}) needs a build with -Dsql=true\n", .{q_str});
            return error.SqlNotCompiledIn;
        }
    } else {
        for (inputs_raw.items) |pat| {
            const matches = try expandGlob(arena, pat, env);
            if (matches.len == 0) {
                std.debug.print("zpq query: no files matched: {s}\n", .{pat});
                return error.NoMatches;
            }
            for (matches) |m| try inputs_list.append(arena, m);
        }
    }
    const inputs = inputs_list.items;
    const max_mem_limit = max_memory orelse zpq.core.system.discoverAvailableMemory(env);

    if (format) |fmt| {
        const cols = if (columns_csv) |csv|
            try splitCsv(arena, csv)
        else
            null;
        try engine.runPrint(.{
            .gpa = gpa,
            .env = env,
            .io = io,
        }, .{
            .inputs = inputs,
            .output = null,
            .filter = filter,
            .columns = cols,
            .select = select,
            .codec = codec,
            .parallelism = parallelism,
            .scan_all = scan_all,
            .trust_stats = trust_stats,
            .max_memory = max_mem_limit,
            .group_by = group_by,
            .select_cols = select_cols,
            .column_order = column_order,
        }, fmt, limit);
        return;
    }

    // Aggregate path: -o is OPTIONAL (JSON-only mode is the default).
    if (aggregate != null or group_by != null) {
        const agg_str = aggregate orelse "";
        const t_start_a = nowMonoNs();
        const result = try engine.runQuery(.{
            .gpa = gpa,
            .env = env,
            .io = io,
        }, .{
            .inputs = inputs,
            .output = output,
            .filter = filter,
            .aggregate = agg_str,
            .codec = codec,
            .parallelism = parallelism,
            .scan_all = scan_all,
            .trust_stats = trust_stats,
            .max_memory = max_mem_limit,
            .group_by = group_by,
            .select_cols = select_cols,
            .column_order = column_order,
        });
        const ar = result.aggregate;
        defer gpa.free(ar.aggs);
        defer for (ar.aggs) |item| {
            gpa.free(item.alias);
            switch (item.value) {
                .s => |s| gpa.free(s),
                else => {},
            }
        };
        defer if (ar.group_rows) |rows| {
            for (rows) |r| {
                for (r) |v| {
                    switch (v) {
                        .s => |s| gpa.free(s),
                        else => {},
                    }
                }
                gpa.free(r);
            }
            gpa.free(rows);
        };
        defer if (ar.group_cols) |cols| {
            for (cols) |c| gpa.free(c);
            gpa.free(cols);
        };
        const total_ms_a = @divTrunc(nowMonoNs() - t_start_a, std.time.ns_per_ms);

        var ws: StdoutWriter = .{ .fd = 1 };
        defer ws.flush();
        try ws.print("{{\"ok\":true,\"files_in\":{d}", .{ar.files_in});
        if (ar.files_in == 1) {
            try ws.print(",\"input\":\"", .{});
            try writeJsonString(&ws, inputs[0]);
            try ws.print("\"", .{});
        }
        if (output) |op| {
            try ws.print(",\"output\":\"", .{});
            try writeJsonString(&ws, op);
            try ws.print("\"", .{});
        }
        if (ar.group_rows) |rows| {
            try ws.print(
                ",\"rows_in\":{d},\"rows_kept\":{d},\"bytes_in\":{d},\"bytes_out\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"cols_stat_pruned\":{d},\"agg\":[",
                .{ ar.rows_in, ar.rows_kept, ar.bytes_in, ar.bytes_out, ar.row_groups_in, ar.row_groups_pruned, ar.cols_stat_pruned },
            );
            for (rows, 0..) |row_vals, row_idx| {
                if (row_idx > 0) try ws.print(",", .{});
                try ws.print("{{", .{});
                for (ar.group_cols.?, 0..) |col_name, col_idx| {
                    if (col_idx > 0) try ws.print(",", .{});
                    try ws.print("\"", .{});
                    try writeJsonString(&ws, col_name);
                    try ws.print("\":", .{});
                    switch (row_vals[col_idx]) {
                        .i => |v| try ws.print("{d}", .{v}),
                        .f => |v| try writeJsonFloat(&ws, v),
                        .s => |v| {
                            try ws.print("\"", .{});
                            try writeJsonString(&ws, v);
                            try ws.print("\"", .{});
                        },
                        .avg => |v| {
                            try ws.writeAll("{\"sum\":");
                            try writeJsonFloat(&ws, v.sum);
                            try ws.print(",\"count\":{d}}}", .{v.count});
                        },
                        .null_val => try ws.writeAll("null"),
                    }
                }
                try ws.print("}}", .{});
            }
            try ws.writeAll("],");
        } else {
            try ws.print(
                ",\"rows_in\":{d},\"rows_kept\":{d},\"bytes_in\":{d},\"bytes_out\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"cols_stat_pruned\":{d},\"agg\":{{",
                .{ ar.rows_in, ar.rows_kept, ar.bytes_in, ar.bytes_out, ar.row_groups_in, ar.row_groups_pruned, ar.cols_stat_pruned },
            );
            for (ar.aggs, 0..) |item, i| {
                if (i > 0) try ws.print(",", .{});
                try ws.print("\"", .{});
                try writeJsonString(&ws, item.alias);
                try ws.print("\":", .{});
                switch (item.value) {
                    .i => |v| try ws.print("{d}", .{v}),
                    .f => |v| try writeJsonFloat(&ws, v),
                    .s => |v| {
                        try ws.print("\"", .{});
                        try writeJsonString(&ws, v);
                        try ws.print("\"", .{});
                    },
                    .avg => |v| {
                        try ws.writeAll("{\"sum\":");
                        try writeJsonFloat(&ws, v.sum);
                        try ws.print(",\"count\":{d}}}", .{v.count});
                    },
                    .null_val => try ws.writeAll("null"),
                }
            }
            try ws.writeAll("},");
        }
        try ws.print(
            "\"total_ms\":{d},\"phase\":{{\"read_ms\":{d},\"parse_ms\":{d},\"decode_ms\":{d},\"decode_wall_ms\":{d},\"eval_ms\":{d},\"encode_ms\":{d}}}}}\n",
            .{
                total_ms_a,
                ar.timings.read_ns / std.time.ns_per_ms,
                ar.timings.parse_ns / std.time.ns_per_ms,
                ar.timings.core.decode_ns / std.time.ns_per_ms,
                ar.timings.decode_wall_ns / std.time.ns_per_ms,
                ar.timings.core.eval_ns / std.time.ns_per_ms,
                ar.timings.core.encode_ns / std.time.ns_per_ms,
            },
        );
        return;
    }

    const out = output orelse {
        std.debug.print("zpq query: missing --output <path>\n", .{});
        return error.BadArgs;
    };

    const cols: ?[]const []const u8 = if (columns_csv) |csv|
        try splitCsv(arena, csv)
    else
        null;

    const t_start = nowMonoNs();
    const result = (try engine.runQuery(.{
        .gpa = gpa,
        .env = env,
        .io = io,
    }, .{
        .inputs = inputs,
        .output = out,
        .filter = filter,
        .columns = cols,
        .select = select,
        .codec = codec,
        .parallelism = parallelism,
        .scan_all = scan_all,
        .trust_stats = trust_stats,
        .max_memory = max_mem_limit,
    })).write;
    const in = inputs[0]; // first input — used in the JSON envelope below
    const total_ms = @divTrunc(nowMonoNs() - t_start, std.time.ns_per_ms);

    // JSON envelope to stderr — same shape as the Lambda response.
    var w: StdoutWriter = .{ .fd = 2 };
    defer w.flush();
    try w.print(
        \\{{"ok":true,"input":"
    , .{});
    try writeJsonString(&w, in);
    try w.print(
        \\","output":"
    , .{});
    try writeJsonString(&w, out);
    try w.print(
        \\","codec":"{s}","rows_in":{d},"rows_kept":{d},"bytes_in":{d},"bytes_out":{d},"row_groups_in":{d},"row_groups_kept":{d},"total_ms":{d},"phase":{{"read_ms":{d},"parse_ms":{d},"decode_ms":{d},"eval_ms":{d},"encode_ms":{d},"sink_ms":{d},"footer_ms":{d}}}}}
        \\
    , .{
        @tagName(codec),
        result.rows_in,
        result.rows_kept,
        result.bytes_in,
        result.bytes_out,
        result.row_groups_in,
        result.row_groups_kept,
        total_ms,
        result.timings.read_ns / std.time.ns_per_ms,
        result.timings.parse_ns / std.time.ns_per_ms,
        result.timings.core.decode_ns / std.time.ns_per_ms,
        result.timings.core.eval_ns / std.time.ns_per_ms,
        result.timings.core.encode_ns / std.time.ns_per_ms,
        result.timings.core.sink_ns / std.time.ns_per_ms,
        result.timings.footer_ns / std.time.ns_per_ms,
    });
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
/// Plain paths (no glob char) pass through as a single-element slice.
/// Only `*` is supported as a wildcard, in the basename only — covers
/// the duckdb-style `prefix/*.parquet` shape we want for the demo.
fn expandGlob(arena: std.mem.Allocator, pattern: []const u8, env: std.process.Environ) ![][]const u8 {
    // s3:// patterns: handled separately. Plain s3 URLs (no glob char)
    // pass through; globbed s3 URLs go through ListObjectsV2.
    if (std.mem.startsWith(u8, pattern, "s3://")) {
        return expandS3Glob(arena, pattern, env);
    }

    if (std.mem.indexOfScalar(u8, pattern, '*') == null) {
        const single = try arena.alloc([]const u8, 1);
        single[0] = pattern;
        return single;
    }

    const last_slash = std.mem.lastIndexOfScalar(u8, pattern, '/');
    const dir_path: []const u8 = if (last_slash) |i| pattern[0..i] else ".";
    const basename: []const u8 = if (last_slash) |i| pattern[i + 1 ..] else pattern;

    if (std.mem.indexOfScalar(u8, dir_path, '*') != null) {
        std.debug.print("zpq query: glob in directory portion not supported: {s}\n", .{pattern});
        return error.BadArgs;
    }

    const linux = std.os.linux;
    var dir_z: [4096]u8 = undefined;
    if (dir_path.len + 1 > dir_z.len) return error.PathTooLong;
    @memcpy(dir_z[0..dir_path.len], dir_path);
    dir_z[dir_path.len] = 0;
    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&dir_z[0]), .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true }, 0);
    const r_open_signed: isize = @bitCast(r_open);
    if (r_open_signed < 0) {
        std.debug.print("zpq query: cannot open dir {s}\n", .{dir_path});
        return error.OpenFailed;
    }
    const fd: i32 = @intCast(r_open_signed);
    defer _ = linux.close(fd);

    var out: std.ArrayList([]const u8) = .empty;
    var buf: [8192]u8 align(8) = undefined;
    while (true) {
        const n = linux.getdents64(fd, &buf, buf.len);
        const n_signed: isize = @bitCast(n);
        if (n_signed < 0) return error.GetdentsFailed;
        if (n == 0) break;
        var off: usize = 0;
        while (off < n) {
            const entry: *const linux.dirent64 = @ptrCast(@alignCast(&buf[off]));
            const reclen: usize = entry.reclen;
            // Skip non-regular-file entries. A regular file or a symlink
            // pointing at one are both reasonable inputs; DT.UNKNOWN
            // means the FS didn't report a type and we just trust the
            // glob match.
            const ok_type = entry.type == linux.DT.REG or
                entry.type == linux.DT.LNK or
                entry.type == linux.DT.UNKNOWN;
            if (ok_type) {
                const name_ptr: [*:0]const u8 = @ptrCast(&entry.name);
                const name_len = std.mem.indexOfSentinel(u8, 0, name_ptr);
                const name = name_ptr[0..name_len];
                if (matchSimpleGlob(basename, name)) {
                    const full = if (last_slash != null)
                        try std.fs.path.join(arena, &.{ dir_path, name })
                    else
                        try arena.dupe(u8, name);
                    try out.append(arena, full);
                }
            }
            off += reclen;
        }
    }
    std.sort.pdq([]const u8, out.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return out.items;
}

/// Expand an `s3://bucket/prefix/*.parquet` pattern via ListObjectsV2.
/// Plain `s3://bucket/key` (no glob char) returns as-is. Same `*` rules
/// as the local glob: one wildcard, basename only — pattern up to the
/// last `/` is treated as the listing prefix; everything after is the
/// basename glob applied client-side after listing.
fn expandS3Glob(
    arena: std.mem.Allocator,
    pattern: []const u8,
    env: std.process.Environ,
) ![][]const u8 {
    if (std.mem.indexOfScalar(u8, pattern, '*') == null) {
        const single = try arena.alloc([]const u8, 1);
        single[0] = pattern;
        return single;
    }

    // Split: s3://bucket/<prefix><basename-glob>. Find last '/' that
    // sits before the wildcard.
    const star_pos = std.mem.indexOfScalar(u8, pattern, '*').?;
    const last_slash = std.mem.lastIndexOfScalar(u8, pattern[0..star_pos], '/') orelse {
        std.debug.print("zpq query: s3 glob needs a prefix path: {s}\n", .{pattern});
        return error.BadArgs;
    };
    if (std.mem.indexOfScalar(u8, pattern[last_slash + 1 ..], '/') != null) {
        std.debug.print("zpq query: s3 glob in directory portion not supported: {s}\n", .{pattern});
        return error.BadArgs;
    }
    const prefix_part = pattern[0 .. last_slash + 1]; // includes trailing '/'
    const basename_glob = pattern[last_slash + 1 ..];

    // Parse out bucket + prefix-after-bucket. prefix_part starts
    // with "s3://" so let the existing parser handle the URL structure.
    const url_for_prefix = try std.fmt.allocPrint(arena, "{s}placeholder", .{prefix_part});
    const url = s3.Url.parse(url_for_prefix) catch |err| {
        std.debug.print("zpq query: bad s3 url {s}: {s}\n", .{ pattern, @errorName(err) });
        return err;
    };
    const list_prefix = url.key[0 .. url.key.len - "placeholder".len];

    const creds = s3.Credentials.fromEnv(env) catch |err| {
        std.debug.print(
            "zpq query: s3 glob requires S3_*/AWS_* env vars: {s}\n",
            .{@errorName(err)},
        );
        return err;
    };
    var client = try s3.Client.init(arena, creds, url.bucket);
    defer client.deinit();

    const entries = try s3.listAll(arena, &client, list_prefix);
    if (entries.len == 0) {
        std.debug.print("zpq query: s3 glob matched no objects under prefix {s}\n", .{list_prefix});
    }

    var out: std.ArrayList([]const u8) = .empty;
    for (entries) |e| {
        // ListObjectsV2 returns full keys (e.g. "demo/yellow/foo.parquet").
        // Match basename against the glob.
        const key_basename = if (std.mem.lastIndexOfScalar(u8, e.key, '/')) |s| e.key[s + 1 ..] else e.key;
        if (!matchSimpleGlob(basename_glob, key_basename)) continue;
        const full = try std.fmt.allocPrint(arena, "s3://{s}/{s}", .{ url.bucket, e.key });
        try out.append(arena, full);
    }
    std.sort.pdq([]const u8, out.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return out.items;
}

/// Minimal `prefix*suffix` matcher — a single `*` wildcard,
/// non-overlapping. Common case: `*.parquet`.
fn matchSimpleGlob(pat: []const u8, name: []const u8) bool {
    const star = std.mem.indexOfScalar(u8, pat, '*') orelse {
        return std.mem.eql(u8, pat, name);
    };
    const prefix = pat[0..star];
    const suffix = pat[star + 1 ..];
    if (name.len < prefix.len + suffix.len) return false;
    if (!std.mem.startsWith(u8, name, prefix)) return false;
    if (!std.mem.endsWith(u8, name, suffix)) return false;
    return true;
}

fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

/// Open a parquet file via ZPQ and emit a JSON report describing what
/// we saw — schema leaves, row count, and a per-column-chunk decode
/// status (ok/error). Output goes to stdout. Caller (Python harness)
/// compares against pyarrow.
fn runConform(gpa: std.mem.Allocator, path: []const u8) !void {
    const file_bytes = readFile(gpa, path) catch |err| {
        try jsonFatal(gpa, "open_failed", @errorName(err));
        return;
    };
    defer gpa.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const meta = metadata.open(arena, file_bytes) catch |err| {
        try jsonFatal(gpa, "metadata_open_failed", @errorName(err));
        return;
    };

    var w: StdoutWriter = .{};
    defer w.flush();

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
    const values = arena.alloc(T, num_rows) catch return "alloc_failed";
    if (levels.max_rep > 0) {
        const def_levels = arena.alloc(u32, num_rows) catch return "alloc_failed";
        const rep_levels = arena.alloc(u32, num_rows) catch return "alloc_failed";
        var written: usize = 0;
        while (written < num_rows) {
            const n = rdr.decodeWithRepLevels(values[written..], def_levels[written..], rep_levels[written..]) catch |e| return @errorName(e);
            if (n == 0) break;
            written += n;
        }
        if (written != num_rows) return "short_decode";
    } else if (levels.max_def > 0) {
        const def_levels = arena.alloc(u32, num_rows) catch return "alloc_failed";
        var written: usize = 0;
        while (written < num_rows) {
            const n = rdr.decodeWithLevels(values[written..], def_levels[written..]) catch |e| return @errorName(e);
            if (n == 0) break;
            written += n;
        }
        if (written != num_rows) return "short_decode";
    } else {
        var written: usize = 0;
        while (written < num_rows) {
            const n = rdr.decode(values[written..]) catch |e| return @errorName(e);
            if (n == 0) break;
            written += n;
        }
        if (written != num_rows) return "short_decode";
    }
    return "ok";
}

fn jsonFatal(gpa: std.mem.Allocator, kind: []const u8, reason: []const u8) !void {
    var w: StdoutWriter = .{};
    defer w.flush();
    _ = gpa;
    try w.writeAll("{\"error\":\"");
    try writeJsonString(&w, kind);
    try w.writeAll("\",\"reason\":\"");
    try writeJsonString(&w, reason);
    try w.writeAll("\"}\n");
}

/// JSON has no NaN / Infinity literals, but agg results can be non-finite
/// (e.g. sum or avg of a column containing NaN, or ±inf values — see
/// nan_in_stats.parquet). Emit a quoted token rather than a bare `nan`/`inf`
/// (invalid JSON) — and rather than `null`, which would falsely read as
/// "absent" when the result is genuinely indeterminate/unbounded. The quoted
/// form preserves the value and matches what DuckDB/pyarrow surface.
fn writeJsonFloat(w: *StdoutWriter, v: f64) !void {
    if (std.math.isFinite(v)) {
        try w.print("{d}", .{v});
    } else if (std.math.isNan(v)) {
        try w.writeAll("\"NaN\"");
    } else if (v > 0) {
        try w.writeAll("\"Infinity\"");
    } else {
        try w.writeAll("\"-Infinity\"");
    }
}

fn writeJsonString(w: *StdoutWriter, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    var buf: [8]u8 = undefined;
                    const n = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                    try w.writeAll(n);
                } else {
                    try w.writeAll(&[_]u8{c});
                }
            },
        }
    }
}

const StdoutWriter = struct {
    fd: std.os.linux.fd_t = 1,
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

    pub fn flush(self: *StdoutWriter) void {
        if (self.inner) |*in| {
            in.flush() catch {};
        }
    }
};

fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const linux = std.os.linux;
    var path_z: [1024]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const r_open_signed: isize = @bitCast(r_open);
    if (r_open_signed < 0) return error.OpenFailed;
    const fd: linux.fd_t = @intCast(r_open_signed);
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var off: usize = 0;
    while (off < size) {
        const r = linux.read(fd, buf[off..].ptr, size - off);
        const n: isize = @bitCast(r);
        if (n <= 0) break;
        off += @intCast(n);
    }
    if (off != size) return error.ShortRead;
    return buf;
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
    const gpa = init.gpa;
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
            std.debug.print("zpq schema: failed to fetch S3 schema for {s}: {s}\n", .{ path, @errorName(err) });
            std.process.exit(1);
        };
    } else {
        const bytes = readFile(gpa, path) catch |err| {
            std.debug.print("zpq schema: failed to open input file {s}: {s}\n", .{ path, @errorName(err) });
            std.process.exit(1);
        };
        file_bytes = bytes;

        meta = metadata.open(arena, bytes) catch |err| {
            std.debug.print("zpq schema: input file {s} is not a valid Parquet file ({s})\n", .{ path, @errorName(err) });
            std.process.exit(1);
        };
    }

    var ws: StdoutWriter = .{};
    defer ws.flush();

    var codec_name: []const u8 = "UNCOMPRESSED";
    if (meta.row_groups.items.len > 0 and meta.row_groups.items[0].columns.items.len > 0) {
        if (meta.row_groups.items[0].columns.items[0].meta_data) |cm| {
            codec_name = @tagName(cm.codec);
        }
    }

    try ws.print("rows: {d}, row_groups: {d}, codec: {s}\n", .{ meta.num_rows, meta.row_groups.items.len, codec_name });

    const tree = schema_tree.SchemaTree.build(arena, meta.schema.items) catch |err| {
        std.debug.print("zpq schema: schema tree build failed: {s}\n", .{@errorName(err)});
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
}
