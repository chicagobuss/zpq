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

const usage_text =
    \\usage:
    \\  zpq query <input.parquet> [--output <out.parquet>]
    \\            [--filter EXPR] [--columns COL1,COL2,...]
    \\            [--select "EXPR1 [AS name], EXPR2 [AS name], ..."]
    \\            [--aggregate "AGG(...) [FILTER (WHERE ...)] [AS name], ..."]
    \\            [--group-by "EXPR [AS name], ..."] [--column-order COL1,COL2,...]
    \\            [--codec snappy|zstd|gzip|lz4|lz4_raw|uncompressed] [--threads N | -j N]
    \\            [--compress-priority size|speed]
    \\            [--scan-all] [--trust-stats] [--fast-levels] [--unordered]
    \\            [--bloom-filter COL[:FPP] ...]
    \\            [--format csv|jsonl] [--limit N]
    \\            [--explain] [--profile TRACE.json] [--profile-summary[=text|json]]
    \\  zpq query --query "<sql query>" [--output <out.parquet>]
    \\            [--codec snappy|zstd|gzip|lz4|lz4_raw|uncompressed] [--threads N | -j N]
    \\            [--compress-priority size|speed]
    \\            [--scan-all] [--trust-stats]
    \\            [--explain] [--profile TRACE.json] [--profile-summary[=text|json]]
    \\  zpq schema <file.parquet>
    \\  zpq conform <file.parquet>
    \\
    \\  --codec        output codec. Without it, re-encoded output is snappy
    \\                 and a plain copy keeps each chunk's codec; with it, a
    \\                 plain copy recompresses chunks stored in another one.
    \\                 The summary's "codec" is the one every written chunk
    \\                 uses, or "MIXED" when a copy keeps several.
    \\                 `lz4` and `lz4_raw` both write LZ4_RAW; zpq reads the
    \\                 deprecated LZ4 codec but never compresses to it.
    \\  --compress-priority
    \\                 how hard zstd works. `size` (default): level 3 for
    \\                 every page. `speed`: each page stored raw, as
    \\                 entropy-coded literals, or at level 1 or 3, whichever
    \\                 a sample says costs least; less CPU, but a page can
    \\                 come out uncompressed where level 3 would shrink it
    \\                 (see README).
    \\  --scan-all     decode every page/byte: disables all stats shortcuts
    \\                 (row-group pruning, stats-as-answer). Slower but
    \\                 thorough — use when you don't trust a file's stats.
    \\  --trust-stats  answer min/max/sum from file statistics instead of
    \\                 decoding (fast, but trusts the writer's stats — off
    \\                 by default; count(*) is always answered from metadata).
    \\  --bloom-filter write a split-block Bloom filter for output column COL
    \\                 (repeatable), at false-positive probability FPP
    \\                 (default 0.01). Readers then skip row groups an `=`
    \\                 or `IN` on COL cannot match. A copied chunk keeps the
    \\                 filter its source has. Not for BOOLEAN columns.
    \\  --fast-levels  skip materializing definition levels for data pages
    \\                 whose level stream proves every value is present.
    \\                 Helps supported flat OPTIONAL primitive columns that
    \\                 hold no nulls; off by default while it is new.
    \\  --unordered    GROUP BY rows in no set order, and float sums and
    \\                 averages free to differ in their last bits from run
    \\                 to run. By default groups come in the order they
    \\                 first appear in the input and the same bits come out
    \\                 at every -j.
    \\  --max-memory   ceiling on accounted GROUP BY entries across all worker
    \\                 tables. Allocator capacity overhead, finalization
    \\                 scratch, and materialized output rows are outside this
    \\                 entry budget. Workers draw from it in blocks rather
    \\                 than each owning fixed slices, so raising -j does not
    \\                 shrink what a query may use. Partly-used blocks can
    \\                 collectively strand at most ~5% of the budget, so give
    \\                 it headroom rather than tuning to the exact byte.
    \\                 It also bounds working memory on every path: a row
    \\                 group too big for it is read a slice of rows at a
    \\                 time. Output contents are identical; row-group
    \\                 boundaries may depend on --max-memory (and -j) for
    \\                 filtered or computed writes.
    \\  --explain      print an aggregate plan as JSON using only file footers;
    \\                 accepts SQL or --aggregate / --group-by, then exits.
    \\  --profile      write a Chrome Trace Event JSON trace of the query
    \\                 (open it at https://ui.perfetto.dev): per-thread spans
    \\                 for every phase, in-flight counters, RSS.
    \\  --profile-summary
    \\                 print a compact profile summary to stderr: time and
    \\                 wall share per phase, costliest column chunks, I/O and
    \\                 pruning counts. `=json` prints it as one JSON line.
    \\
;

var machine_diagnostics = false;
var diagnostic_buffer: [16 * 1024]u8 = undefined;
var diagnostic_len: usize = 0;

pub const std_options: std.Options = .{ .logFn = cliLog };
fn cliLog(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime fmt: []const u8, args: anytype) void {
    if (!machine_diagnostics) std.log.defaultLog(level, scope, fmt, args);
}

fn wantsJsonSummary(args: std.process.Args) bool {
    var it = std.process.Args.Iterator.init(args);
    while (it.next()) |a| if (std.mem.eql(u8, a, "--summary=json")) return true;
    return false;
}

fn captureSqlDiagnostic(message: []const u8) void {
    const n = @min(message.len, diagnostic_buffer.len - diagnostic_len);
    @memcpy(diagnostic_buffer[diagnostic_len..][0..n], message[0..n]);
    diagnostic_len += n;
}

fn diagnosticPrint(comptime fmt: []const u8, args: anytype) void {
    if (!machine_diagnostics) {
        std.debug.print(fmt, args);
        return;
    }
    const written = std.fmt.bufPrint(diagnostic_buffer[diagnostic_len..], fmt, args) catch return;
    diagnostic_len += written.len;
}

fn emitError(err: anyerror) !void {
    var w: StdoutWriter = .{ .fd = 2 };
    try w.writeAll("{\"type\":\"error\",\"ok\":false,\"code\":\"");
    try writeJsonString(&w, @errorName(err));
    try w.writeAll("\",\"message\":\"");
    try writeJsonString(&w, if (diagnostic_len > 0) diagnostic_buffer[0..diagnostic_len] else @errorName(err));
    try w.writeAll("\"}\n");
    try w.flush();
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    machine_diagnostics = wantsJsonSummary(init.minimal.args);
    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next(); // skip program name
    const cmd = iter.next() orelse {
        var ws: StdoutWriter = .{};
        defer ws.flush() catch {};
        try ws.writeAll(usage_text);
        try ws.flush();
        return;
    };
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
        try ws.print("zpq {s}\n", .{build_options.version});
        try ws.flush();
        return;
    }
    if (std.mem.eql(u8, cmd, "query")) {
        var diag: Diag = .{};
        runQuery(init, &iter, &diag) catch |err| {
            if (err == error.BrokenPipe) std.process.exit(141);
            printDetail(&diag);
            switch (err) {
                error.BadArgs, error.NoMatches, error.SqlNotCompiledIn => {},
                error.NoInputs => diagnosticPrint("zpq query: no input files specified\n", .{}),
                error.Unsigned64OutOfRange => diagnosticPrint(
                    "zpq query: an unsigned 64-bit value of 2^63 or more cannot be used in\n" ++
                        "  integer arithmetic; mix in a float (e.g. `col * 1.0`) to compute in DOUBLE\n",
                    .{},
                ),
                error.DuplicateOutputColumn => diagnosticPrint(
                    "zpq query: output column `{s}` is defined more than once;\n" ++
                        "  give each output column (aggregate, GROUP BY key or select item) a distinct name\n",
                    .{diag.column.get()},
                ),
                error.AmbiguousOutputColumn => diagnosticPrint(
                    "zpq query: output column `{s}` names both a GROUP BY key\n" ++
                        "  and an aggregate; rename the aggregate alias\n",
                    .{diag.column.get()},
                ),
                error.EmptyAggregate => diagnosticPrint("zpq query: empty aggregate expression\n", .{}),
                error.EmptyColumnList => diagnosticPrint("zpq query: --columns names no column\n", .{}),
                error.AggregateMutexWithSelect => diagnosticPrint("zpq query: aggregate functions are mutually exclusive with --select / --columns\n", .{}),
                error.MissingOutputOrAggregate => diagnosticPrint("zpq query: missing --output or --aggregate\n", .{}),
                error.SchemaMismatch => diagnosticPrint("zpq query: schema mismatch across inputs\n", .{}),
                error.NestedRowOutputNotSupported => diagnosticPrint(
                    "zpq query: nested LIST/MAP columns are not supported in row output yet\n",
                    .{},
                ),
                error.UnlabelledColumn => diagnosticPrint(
                    "zpq query: nested column {s} has no output name of its own:\n" ++
                        "  a top-level column is named like it; name it with --select and AS\n",
                    .{diag.column.get()},
                ),
                error.NestedReencodeNotSupported => diagnosticPrint("zpq query: nested re-encoding is not supported yet\n", .{}),
                error.INT96ReencodeNotSupported => diagnosticPrint("zpq query: INT96 re-encoding is not supported yet\n", .{}),
                error.UnsupportedDecimalPrecision => diagnosticPrint(
                    "zpq query: column `{s}` has unsupported decimal precision (> 38)\n",
                    .{diag.column.get()},
                ),
                error.DecimalValueExceedsPrecision => diagnosticPrint(
                    "zpq query: column `{s}` has a decimal value exceeding its declared precision\n",
                    .{diag.column.get()},
                ),
                error.FooterSchemaChunkMismatch => diagnosticPrint("zpq query: footer schema chunk mismatch\n", .{}),
                error.CrossBucketNotSupported => diagnosticPrint("zpq query: cross-bucket queries are not supported\n", .{}),
                error.BadInputUrl => diagnosticPrint("zpq query: invalid input S3 URL\n", .{}),
                error.BadOutputUrl => diagnosticPrint("zpq query: invalid output S3 URL\n", .{}),
                error.NoCredentials => diagnosticPrint("zpq query: missing AWS credentials for S3 query\n", .{}),
                error.NoRegion => diagnosticPrint("zpq query: no S3 region; set AWS_REGION (or S3_REGION)\n", .{}),
                error.BadResponse => diagnosticPrint("zpq query: bad S3 HTTP response\n", .{}),
                error.TailTooSmall => diagnosticPrint("zpq query: file footer metadata tail too small\n", .{}),
                error.NotParquet, error.BadMagic => if (diag.input) |in| diagnosticPrint(
                    "zpq query: input file {s} is not a valid Parquet file ({s})\n",
                    .{ in, causeName(&diag) },
                ) else diagnosticPrint("zpq query: input file is not a valid Parquet file\n", .{}),
                error.AggSumOverflow => diagnosticPrint("zpq query: aggregate sum overflowed integer limits\n", .{}),
                error.OpenFailed => if (diag.input) |in|
                    diagnosticPrint("zpq query: failed to open input file {s} ({s})\n", .{ in, causeName(&diag) })
                else
                    diagnosticPrint("zpq query: failed to open input file\n", .{}),
                error.EmptyFile => if (diag.input) |in|
                    diagnosticPrint("zpq query: input file {s} is empty\n", .{in})
                else
                    diagnosticPrint("zpq query: input file is empty\n", .{}),
                error.PathTooLong => if (diag.input) |in|
                    diagnosticPrint("zpq query: input file path {s} too long\n", .{in})
                else
                    diagnosticPrint("zpq query: input file path too long\n", .{}),
                error.EmptyExpr => diagnosticPrint("zpq query: empty expression\n", .{}),
                error.UnexpectedChar => diagnosticPrint("zpq query: unexpected character in expression\n", .{}),
                error.UnexpectedEnd => diagnosticPrint("zpq query: unexpected end of expression\n", .{}),
                error.BadNumber => diagnosticPrint("zpq query: invalid number in expression\n", .{}),
                error.UnsupportedCodec => diagnosticPrint("zpq query: input uses a compression codec zpq cannot decompress\n", .{}),
                error.UnknownColumn => if (diag.column.len > 0)
                    diagnosticPrint("zpq query: unknown column `{s}`\n", .{diag.column.get()})
                else
                    diagnosticPrint("zpq query: unknown column referenced in expression\n", .{}),
                // A parser's ambiguity was named in full by `printDetail`.
                error.AmbiguousColumn => if (diag.column.len > 0 and diag.detail != .ambiguous_column)
                    diagnosticPrint("zpq query: ambiguous column name `{s}`\n", .{diag.column.get()})
                else
                    diagnosticPrint("zpq query: ambiguous column name\n", .{}),
                error.UnsupportedColumnType => diagnosticPrint("zpq query: unsupported column type\n", .{}),
                error.UnterminatedString => diagnosticPrint("zpq query: unterminated string literal in expression\n", .{}),
                error.TypeMismatch => diagnosticPrint("zpq query: type mismatch in expression\n", .{}),
                error.UnknownFunction => diagnosticPrint("zpq query: unknown scalar function in expression\n", .{}),
                error.UnknownAggFunc => diagnosticPrint("zpq query: unknown aggregate function in expression\n", .{}),
                error.WrongArity => diagnosticPrint("zpq query: wrong number of arguments for function\n", .{}),
                error.BadAggArg => diagnosticPrint("zpq query: invalid argument for aggregate function\n", .{}),
                error.ExpectedLParen => diagnosticPrint("zpq query: expected '(' in expression\n", .{}),
                error.ExpectedRParen => diagnosticPrint("zpq query: expected ')' in expression\n", .{}),
                error.ExpressionTooDeep => diagnosticPrint(
                    "zpq query: expression nests too deeply (limit {d}). Each nesting level\n" ++
                        "materializes another full intermediate column, so very deep expressions\n" ++
                        "can exhaust memory; split the expression or precompute part of it.\n",
                    .{zpq.core.expr.parser.MAX_EXPR_DEPTH},
                ),
                error.Unsigned64Operand => diagnosticPrint(
                    "zpq query: arithmetic and functions over a UINT64 column are not\n" ++
                        "supported (values >= 2^63 do not fit the signed 64-bit expression\n" ++
                        "type). Select, filter, group by or aggregate the column directly.\n",
                    .{},
                ),
                error.ExpectedIdentifier => diagnosticPrint("zpq query: expected column identifier in expression\n", .{}),
                error.ExpectedWhere => diagnosticPrint("zpq query: expected WHERE keyword in expression\n", .{}),
                error.ExpectedAggFunc => diagnosticPrint("zpq query: expected aggregate function\n", .{}),
                error.StarOnlyValidInCount => diagnosticPrint("zpq query: '*' is only valid inside count(*)\n", .{}),
                error.TrailingTokens => diagnosticPrint("zpq query: trailing tokens after expression\n", .{}),
                error.GroupKeyAliasRequired => diagnosticPrint("zpq query: non-trivial GROUP BY key requires AS alias\n", .{}),
                error.BadGroupKey => diagnosticPrint("zpq query: corrupted GROUP BY key framing\n", .{}),
                error.ExceededMemoryBudget => diagnosticPrint("zpq query: GROUP BY exceeded --max-memory budget\n", .{}),
                error.NullableNotSupported => diagnosticPrint("zpq query: nullable values are not supported in this expression\n", .{}),
                error.IntegerOverflowAdd => diagnosticPrint("zpq query: integer overflow in +\n", .{}),
                error.IntegerOverflowSub => diagnosticPrint("zpq query: integer overflow in -\n", .{}),
                error.IntegerOverflowMul => diagnosticPrint("zpq query: integer overflow in *\n", .{}),
                error.IntegerOverflowDiv => diagnosticPrint("zpq query: integer overflow in /\n", .{}),
                error.DecimalOverflow => diagnosticPrint(
                    "zpq query: a DECIMAL value needs more digits than its type holds (at\n" ++
                        "most 38): a result, or an operand of + or - brought to their common\n" ++
                        "type; zpq stops rather than return a rounded or wrapped value\n",
                    .{},
                ),
                error.RowGroupTooLarge => diagnosticPrint(
                    "zpq query: a row group needs more memory to decode than --max-memory\n" ++
                        "allows, even one aggregate or slice of rows at a time; raise\n" ++
                        "--max-memory or rewrite the file with smaller row groups\n",
                    .{},
                ),
                error.DecimalScaleTooLarge => diagnosticPrint(
                    "zpq query: a DECIMAL product would have more than 38 digits after the\n" ++
                        "point; cast an operand to DOUBLE (divide by 1.0) to compute it inexactly\n",
                    .{},
                ),
                error.InvalidDecimalType => diagnosticPrint(
                    "zpq query: a DECIMAL column declares a precision/scale no DECIMAL can\n" ++
                        "have (precision 1..38, scale 0..precision)\n",
                    .{},
                ),
                error.NestedNotSupported => diagnosticPrint("zpq query: nested columns are not supported in GROUP BY keys\n", .{}),
                error.DivisionByZero => diagnosticPrint("zpq query: division by zero\n", .{}),
                error.GroupingNotSupported => diagnosticPrint(
                    "filter: grouping parentheses are not supported — the grammar is a flat AND/OR chain (AND binds " ++
                        "tighter than OR)\nzpq query: grouping parentheses are not supported in filter\n",
                    .{},
                ),
                error.BadOperator => diagnosticPrint("zpq query: invalid operator in filter\n", .{}),
                error.BadValue => diagnosticPrint("zpq query: invalid value in filter\n", .{}),
                error.UnsupportedType => diagnosticPrint("zpq query: unsupported type in filter\n", .{}),
                error.BloomFilterUnsupportedColumn => diagnosticPrint(
                    "zpq query: --bloom-filter cannot apply to `{s}`: BOOLEAN, INT96, nested\n" ++
                        "  and FIXED_LEN_BYTE_ARRAY DECIMAL columns take no Bloom filter\n",
                    .{diag.column.get()},
                ),
                error.BloomFilterNeedsRows => diagnosticPrint(
                    "zpq query: --bloom-filter applies to written rows, not to an aggregate\n",
                    .{},
                ),
                else => diagnosticPrint("zpq query: {s}\n", .{@errorName(err)}),
            }
            if (machine_diagnostics) try emitError(err);
            std.process.exit(queryExitCode(err));
        };
        return;
    }
    if (std.mem.eql(u8, cmd, "conform")) {
        const path = iter.next() orelse {
            diagnosticPrint("conform: missing path\n", .{});
            std.process.exit(1);
        };
        try runConform(gpa, path);
        return;
    }
    if (std.mem.eql(u8, cmd, "schema")) {
        const path = iter.next() orelse {
            diagnosticPrint("schema: missing path\n", .{});
            std.process.exit(1);
        };
        try runSchema(init, path);
        return;
    }
    diagnosticPrint("unknown subcommand: {s}\n", .{cmd});
    std.process.exit(2);
}

fn runQuery(init: std.process.Init, iter: *std.process.Args.Iterator, diag: *Diag) !void {
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
    var explain: bool = false;
    var query: ?[]const u8 = null;
    var codec: ?schema.CompressionCodec = null;
    var compress_priority: engine.CompressPriority = .size;
    var parallelism: usize = 0;
    var scan_all: bool = false;
    var trust_stats: bool = false;
    var fast_levels: bool = false;
    var unordered: bool = false;
    var format: ?engine.PrintFormat = null;
    var summary = false;
    var limit: ?usize = null;
    var max_memory: ?usize = null;
    var group_by: ?[]const u8 = null;
    var select_cols: ?[]const zpq.core.expr.ast.SelectColumn = null;
    var result_order: zpq.core.expr.ast.ResultOrder = .{};
    var column_order: ?[]const u8 = null;
    var profile_out: ProfileOut = .{};
    var bloom_filters: std.ArrayList(engine.BloomSpec) = .empty;
    defer bloom_filters.deinit(gpa);

    while (iter.next()) |tok| {
        if (std.mem.eql(u8, tok, "--help") or std.mem.eql(u8, tok, "-h")) {
            var ws: StdoutWriter = .{};
            defer ws.flush() catch {};
            try ws.writeAll(usage_text);
            try ws.flush();
            return;
        } else if (std.mem.eql(u8, tok, "--output") or std.mem.eql(u8, tok, "-o")) {
            output = iter.next() orelse {
                diagnosticPrint("zpq query: --output requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--filter") or std.mem.eql(u8, tok, "-f")) {
            filter = iter.next() orelse {
                diagnosticPrint("zpq query: --filter requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--columns") or std.mem.eql(u8, tok, "-c")) {
            columns_csv = iter.next() orelse {
                diagnosticPrint("zpq query: --columns requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--select") or std.mem.eql(u8, tok, "-s")) {
            select = iter.next() orelse {
                diagnosticPrint("zpq query: --select requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--explain")) {
            explain = true;
        } else if (std.mem.eql(u8, tok, "--aggregate") or std.mem.eql(u8, tok, "-a")) {
            aggregate = iter.next() orelse {
                diagnosticPrint("zpq query: --aggregate requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--query") or std.mem.eql(u8, tok, "-q")) {
            query = iter.next() orelse {
                diagnosticPrint("zpq query: --query requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--codec")) {
            const v = iter.next() orelse {
                diagnosticPrint("zpq query: --codec requires a value\n", .{});
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
                diagnosticPrint("zpq query: invalid codec: {s}\n", .{v});
                return error.BadArgs;
            }
        } else if (std.mem.eql(u8, tok, "--compress-priority")) {
            const v = iter.next() orelse {
                diagnosticPrint("zpq query: --compress-priority requires a value\n", .{});
                return error.BadArgs;
            };
            compress_priority = std.meta.stringToEnum(engine.CompressPriority, v) orelse {
                diagnosticPrint("zpq query: invalid compress priority: {s} (size or speed)\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--threads") or std.mem.eql(u8, tok, "-j")) {
            const v = iter.next() orelse {
                diagnosticPrint("zpq query: --threads requires a value\n", .{});
                return error.BadArgs;
            };
            parallelism = std.fmt.parseInt(usize, v, 10) catch {
                diagnosticPrint("zpq query: invalid threads value: {s}\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--bloom-filter")) {
            const v = iter.next() orelse {
                diagnosticPrint("zpq query: --bloom-filter requires a value\n", .{});
                return error.BadArgs;
            };
            try bloom_filters.append(gpa, engine.BloomSpec.parse(v) catch {
                diagnosticPrint("zpq query: invalid --bloom-filter: {s} (want COL or COL:FPP, 0 < FPP < 1)\n", .{v});
                return error.BadArgs;
            });
        } else if (std.mem.eql(u8, tok, "--scan-all")) {
            scan_all = true; // valueless: disable all stats shortcuts, decode everything
        } else if (std.mem.eql(u8, tok, "--trust-stats")) {
            trust_stats = true; // valueless: opt in to stats-as-answer for min/max/sum
        } else if (std.mem.eql(u8, tok, "--fast-levels")) {
            fast_levels = true; // valueless: skip def levels on provably no-null pages
        } else if (std.mem.eql(u8, tok, "--unordered")) {
            unordered = true; // valueless: GROUP BY order and float bits may vary
        } else if (std.mem.eql(u8, tok, "--format")) {
            const v = iter.next() orelse {
                diagnosticPrint("zpq query: --format requires a value\n", .{});
                return error.BadArgs;
            };
            if (std.ascii.eqlIgnoreCase(v, "csv")) {
                diagnosticPrint("zpq query: csv output is not supported yet\n", .{});
                return error.BadArgs;
            } else if (std.ascii.eqlIgnoreCase(v, "jsonl") or std.ascii.eqlIgnoreCase(v, "ndjson")) {
                format = .jsonl;
            } else if (std.ascii.eqlIgnoreCase(v, "json")) {
                format = .json;
            } else if (std.ascii.eqlIgnoreCase(v, "parquet")) {
                format = .parquet;
            } else {
                diagnosticPrint("zpq query: invalid format: {s}\n", .{v});
                return error.BadArgs;
            }
        } else if (std.mem.eql(u8, tok, "--summary=json")) {
            summary = true;
        } else if (std.mem.eql(u8, tok, "--limit")) {
            const v = iter.next() orelse {
                diagnosticPrint("zpq query: --limit requires a value\n", .{});
                return error.BadArgs;
            };
            if (limit != null) {
                diagnosticPrint("zpq query: duplicate --limit\n", .{});
                return error.BadArgs;
            }
            limit = std.fmt.parseInt(usize, v, 10) catch {
                diagnosticPrint("zpq query: invalid limit value: {s}\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--group-by")) {
            group_by = iter.next() orelse {
                diagnosticPrint("zpq query: --group-by requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--column-order")) {
            column_order = iter.next() orelse {
                diagnosticPrint("zpq query: --column-order requires a value\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--max-memory")) {
            const v = iter.next() orelse {
                diagnosticPrint("zpq query: --max-memory requires a value\n", .{});
                return error.BadArgs;
            };
            max_memory = zpq.core.system.parseSizeString(v) catch {
                diagnosticPrint("zpq query: invalid max-memory value: {s} (examples: 500MB, 1.5GB, 1024)\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--profile")) {
            profile_out.trace_path = iter.next() orelse {
                diagnosticPrint("zpq query: --profile requires a path\n", .{});
                return error.BadArgs;
            };
        } else if (std.mem.eql(u8, tok, "--profile-summary") or std.mem.startsWith(u8, tok, "--profile-summary=")) {
            const v = if (tok.len > "--profile-summary".len) tok["--profile-summary=".len..] else "text";
            profile_out.summary = std.meta.stringToEnum(ProfileOut.Format, v) orelse {
                diagnosticPrint("zpq query: invalid --profile-summary format: {s} (supported: text, json)\n", .{v});
                return error.BadArgs;
            };
        } else if (std.mem.startsWith(u8, tok, "-")) {
            diagnosticPrint("zpq query: unrecognized option: {s}\n", .{tok});
            return error.BadArgs;
        } else {
            try inputs_raw.append(gpa, tok);
        }
    }

    if (query != null) {
        if (inputs_raw.items.len > 0) {
            diagnosticPrint("zpq query: cannot specify input files on the command line when using --query\n", .{});
            return error.BadArgs;
        }
        if (select != null or aggregate != null or filter != null or columns_csv != null or group_by != null or column_order != null) {
            diagnosticPrint("zpq query: --query is mutually exclusive with --select / --aggregate / --filter / --columns / --format / --limit / --group-by / --column-order\n", .{});
            return error.BadArgs;
        }
    } else {
        if (inputs_raw.items.len == 0) {
            diagnosticPrint("zpq query: missing <input.parquet> [more.parquet ...]\n", .{});
            return error.BadArgs;
        }
        if (aggregate != null and (select != null or columns_csv != null)) {
            diagnosticPrint("zpq query: --aggregate is mutually exclusive with --select / --columns / --format\n", .{});
            return error.BadArgs;
        }
        if (group_by != null and (select != null or columns_csv != null)) {
            diagnosticPrint("zpq query: --group-by is mutually exclusive with --select / --columns / --format\n", .{});
            return error.BadArgs;
        }
        if (column_order != null and group_by == null) {
            diagnosticPrint("zpq query: --column-order requires --group-by\n", .{});
            return error.BadArgs;
        }
    }

    if (bloom_filters.items.len > 0 and output == null) {
        diagnosticPrint("zpq query: --bloom-filter requires --output\n", .{});
        return error.BadArgs;
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
            if (machine_diagnostics) zpq.core.expr.sql_parser.diagnostic_fn = captureSqlDiagnostic;
            defer zpq.core.expr.sql_parser.diagnostic_fn = null;
            const pq = try zpq.core.expr.sql_parser.parseSqlQuery(arena, q_str);
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

            const matches = try expandGlob(arena, io, pq.table_name, env);
            if (matches.len == 0) {
                diagnosticPrint("zpq query: no files matched FROM table: {s}\n", .{pq.table_name});
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
            const matches = try expandGlob(arena, io, pat, env);
            if (matches.len == 0) {
                diagnosticPrint("zpq query: no files matched: {s}\n", .{pat});
                return error.NoMatches;
            }
            for (matches) |m| try inputs_list.append(arena, m);
        }
    }
    const inputs = inputs_list.items;
    const fmt = format orelse if (output != null and !std.mem.eql(u8, output.?, "-"))
        engine.PrintFormat.parquet
    else
        engine.PrintFormat.jsonl;
    const to_stdout = output == null or std.mem.eql(u8, output.?, "-");
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
    var publication: OutputPublication = .{};
    defer publication.abort();
    const engine_output = if (explain) output else if (to_stdout) null else try publication.prepare(io, output, inputs);
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
        const explanation = try engine.explainAggregate(.{ .gpa = gpa, .env = env, .io = io }, arena, .{
            .inputs = inputs,
            .output = engine_output,
            .filter = filter,
            .aggregate = aggregate orelse "",
            .parallelism = parallelism,
            .scan_all = scan_all,
            .trust_stats = trust_stats,
            .fast_levels = fast_levels,
            .unordered = unordered,
            .max_memory = engine_max_mem,
            .group_by = group_by,
            .select_cols = select_cols,
            .result_order = result_order,
            .column_order = column_order,
            .diag = diag,
            .profiler = profiler,
        });
        var ws: StdoutWriter = .{ .fd = 1 };
        defer ws.flush() catch {};
        try engine.writeAggregateExplanationJson(&ws, explanation);
        try ws.flush();
        if (profiler) |p| try profile_out.emit(gpa, io, p);
        return;
    }

    const query_args: engine.QueryArgs = .{
        .inputs = inputs,
        .output = if (fmt == .parquet) engine_output orelse "-" else null,
        .output_fd = if (fmt == .parquet and to_stdout) 1 else null,
        .quiet_diagnostics = machine_diagnostics,
        .empty_scalar = group_by == null and (limit == 0 or result_order.offset > 0),
        .filter = filter,
        .columns = if (columns_csv) |csv| try splitCsv(arena, csv) else null,
        .select = select,
        .aggregate = aggregate,
        .codec = codec,
        .compress_priority = compress_priority,
        .parallelism = parallelism,
        .scan_all = scan_all,
        .trust_stats = trust_stats,
        .fast_levels = fast_levels,
        .unordered = unordered,
        .max_memory = engine_max_mem,
        .group_by = group_by,
        .select_cols = select_cols,
        .result_order = result_order,
        .column_order = column_order,
        .bloom_filters = bloom_filters.items,
        .diag = diag,
        .profiler = profiler,
    };
    const ctx: engine.Context = .{ .gpa = gpa, .env = env, .io = io, .exit_after = true };
    const started = nowMonoNs();
    var rows: usize = 0;
    var stats: SummaryStats = .{};
    var output_ns: u64 = 0;
    var text_bytes_out: u64 = 0;
    if (aggregate != null or group_by != null) {
        var result = try engine.runQuery(ctx, query_args);
        defer result.deinit(gpa);
        const ar = result.aggregate;
        const answer = ar.output;
        rows = if (answer.group_rows) |groups| groups.len else if (query_args.empty_scalar) 0 else 1;
        if (fmt != .parquet) {
            const output_start = nowMonoNs();
            var fd: local_fs.fd_t = 1;
            var owns_fd = false;
            defer if (owns_fd) local_fs.close(fd);
            if (!to_stdout) {
                fd = try local_fs.createFile(engine_output.?);
                owns_fd = true;
            }
            var w: StdoutWriter = .{ .fd = fd };
            if (fmt == .json) try w.writeByte('[');
            if (answer.group_rows) |groups| {
                for (groups, 0..) |values, ri| {
                    if (fmt == .json and ri > 0) try w.writeByte(',');
                    try w.writeByte('{');
                    for (answer.group_cols.?, values, 0..) |name, value, ci| {
                        if (ci > 0) try w.writeByte(',');
                        try w.writeByte('"');
                        try writeJsonString(&w, name);
                        try w.writeAll("\":");
                        try engine.writeAggValueJson(&w, value);
                    }
                    try w.writeAll(if (fmt == .json) "}" else "}\n");
                }
            } else if (rows > 0) {
                try w.writeByte('{');
                for (answer.aggs, 0..) |item, ci| {
                    if (ci > 0) try w.writeByte(',');
                    try w.writeByte('"');
                    try writeJsonString(&w, item.alias);
                    try w.writeAll("\":");
                    try engine.writeAggValueJson(&w, item.value);
                }
                try w.writeAll(if (fmt == .json) "}" else "}\n");
            }
            if (fmt == .json) try w.writeAll("]\n");
            try w.flush();
            text_bytes_out = if (w.inner) |inner| inner.bytes_written else 0;
            if (owns_fd) {
                owns_fd = false;
                try local_fs.closeChecked(fd);
            }
            output_ns = @intCast(nowMonoNs() - output_start);
        }
        output_ns = if (fmt == .parquet) ar.timings.core.encode_ns + ar.timings.core.sink_ns + ar.timings.footer_ns else output_ns;
        stats = .{ .rows_in = ar.rows_in, .rows_kept = ar.rows_kept, .bytes_in = ar.bytes_in, .bytes_out = if (fmt == .parquet) ar.bytes_out else text_bytes_out, .files_in = ar.files_in, .row_groups_in = ar.row_groups_in, .row_groups_pruned = ar.row_groups_pruned, .row_groups_full_match = ar.row_groups_full_match, .cols_stat_pruned = ar.cols_stat_pruned, .membership = ar.membership, .timings = ar.timings };
    } else if (fmt == .parquet and limit == null and result_order.offset == 0) {
        const result = (try engine.runQuery(ctx, query_args)).write;
        rows = @intCast(result.rows_kept);
        output_ns = result.timings.core.encode_ns + result.timings.core.sink_ns + result.timings.footer_ns;
        stats = .{ .rows_in = result.rows_in, .rows_kept = result.rows_kept, .bytes_in = result.bytes_in, .bytes_out = result.bytes_out, .files_in = result.files_in, .row_groups_in = result.row_groups_in, .membership = result.membership, .timings = result.timings, .codec = result.codec };
    } else {
        if (!to_stdout and std.mem.startsWith(u8, engine_output.?, "s3://")) {
            diagnosticPrint("zpq query: this output format does not support S3 destinations\n", .{});
            return error.BadArgs;
        }
        var fd: local_fs.fd_t = 1;
        var owns_fd = false;
        defer if (owns_fd) local_fs.close(fd);
        if (!to_stdout) {
            fd = try local_fs.createFile(engine_output.?);
            owns_fd = true;
        }
        const printed = try engine.runPrintTo(ctx, query_args, fmt, limit, fd, @intCast(result_order.offset));
        if (owns_fd) {
            owns_fd = false;
            try local_fs.closeChecked(fd);
        }
        rows = printed.rows;
        output_ns = printed.output_ns;
        stats = .{ .rows_in = @intCast(printed.rows_in), .rows_kept = @intCast(printed.rows), .bytes_out = printed.bytes_out, .files_in = inputs.len, .membership = printed.membership, .timings = printed.timings };
    }
    const publication_start = nowMonoNs();
    try publication.commit();
    output_ns += @intCast(nowMonoNs() - publication_start);
    stats.output_ns = output_ns;
    const total_ns = nowMonoNs() - started;
    if (profiler) |p| try profile_out.emit(gpa, io, p);
    if (summary) try writeSummary(if (to_stdout) null else output, rows, stats, total_ns);
}

const SummaryStats = struct {
    output_ns: u64 = 0,
    rows_in: i64 = 0,
    rows_kept: i64 = 0,
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
};

fn writeSummary(output: ?[]const u8, rows: usize, s: SummaryStats, elapsed: i64) !void {
    var w: StdoutWriter = .{ .fd = 2 };
    try w.print("{{\"type\":\"summary\",\"ok\":true,\"rows\":{d},\"output\":", .{rows});
    if (output) |path| {
        try w.writeByte('"');
        try writeJsonString(&w, path);
        try w.writeByte('"');
    } else try w.writeAll("null");
    try w.print(",\"rows_in\":{d},\"rows_kept\":{d},\"bytes_in\":{d},\"bytes_out\":{d},\"files_in\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"row_groups_full_match\":{d},\"cols_stat_pruned\":{d},\"codec\":\"{s}\",", .{
        s.rows_in,           s.rows_kept,             s.bytes_in,         s.bytes_out,                                       s.files_in, s.row_groups_in,
        s.row_groups_pruned, s.row_groups_full_match, s.cols_stat_pruned, if (s.codec) |codec| @tagName(codec) else "MIXED",
    });
    try engine.writeMembershipJson(&w, s.membership);
    try w.print(",\"total_ms\":{d},\"total_us\":{d},\"output_us\":{d},\"phase\":{{\"read_ms\":{d},\"parse_ms\":{d},\"decode_ms\":{d},\"decode_wall_ms\":{d},\"eval_ms\":{d},\"encode_ms\":{d},\"sink_ms\":{d},\"footer_ms\":{d}}}}}\n", .{
        @divTrunc(elapsed, std.time.ns_per_ms),        @divTrunc(elapsed, std.time.ns_per_us),      s.output_ns / std.time.ns_per_us,
        s.timings.read_ns / std.time.ns_per_ms,        s.timings.parse_ns / std.time.ns_per_ms,     s.timings.core.decode_ns / std.time.ns_per_ms,
        s.timings.decode_wall_ns / std.time.ns_per_ms, s.timings.core.eval_ns / std.time.ns_per_ms, s.timings.core.encode_ns / std.time.ns_per_ms,
        s.timings.core.sink_ns / std.time.ns_per_ms,   s.timings.footer_ns / std.time.ns_per_ms,
    });
    try w.flush();
}

fn queryExitCode(err: anyerror) u8 {
    if (err == error.BrokenPipe) return 141;
    const usage = [_][]const u8{
        "BadArgs",                     "NoInputs",              "NoMatches",                    "SqlNotCompiledIn",     "UnknownColumn",            "UnknownFunction",
        "UnknownAggFunc",              "WrongArity",            "EmptyExpr",                    "EmptyAggregate",       "EmptyColumnList",          "UnexpectedChar",
        "UnexpectedEnd",               "BadNumber",             "UnterminatedString",           "TypeMismatch",         "ExpectedLParen",           "ExpectedRParen",
        "ExpectedIdentifier",          "ExpectedWhere",         "ExpectedAggFunc",              "StarOnlyValidInCount", "TrailingTokens",           "BadAggArg",
        "AggregateMutexWithSelect",    "DuplicateOutputColumn", "AmbiguousOutputColumn",        "AmbiguousColumn",      "ExpressionTooDeep",        "GroupKeyAliasRequired",
        "SqlParseError",               "OrderByNotSupported",   "LimitNotSupported",            "DistinctNotSupported", "HavingNotSupported",       "MixedSelectNotSupported",
        "UnsupportedStatement",        "SubqueryNotSupported",  "WindowNotSupported",           "ColumnMustBeGrouped",  "BadOutputUrl",             "BadOperator",
        "BadValue",                    "BloomFilterNeedsRows",  "BloomFilterUnsupportedColumn", "Unsigned64Operand",    "NullableNotSupported",     "NestedNotSupported",
        "NestedRowOutputNotSupported", "GroupingNotSupported",  "UnsupportedType",              "DecimalScaleTooLarge", "MissingOutputOrAggregate",
    };
    for (usage) |name| if (std.mem.eql(u8, name, @errorName(err))) return 2;
    return 1;
}

// Publication belongs to the CLI: service callers retain their own destination lifecycle.
const OutputPublication = struct {
    target: ?[]const u8 = null,
    buffer: [std.fs.max_path_bytes]u8 = undefined,
    len: usize = 0,

    fn path(self: *const OutputPublication) []const u8 {
        return self.buffer[0..self.len];
    }

    fn prepare(self: *OutputPublication, io: std.Io, output: ?[]const u8, inputs: []const []const u8) !?[]const u8 {
        const target = output orelse return null;
        if (std.mem.startsWith(u8, target, "s3://")) {
            for (inputs) |input| if (std.mem.eql(u8, input, target)) return error.BadArgs;
            return target;
        }
        const cwd = std.Io.Dir.cwd();
        if (cwd.statFile(io, target, .{})) |st| {
            if (st.kind != .file) {
                diagnosticPrint("zpq query: output must be a regular file\n", .{});
                return error.BadArgs;
            }
            for (inputs) |input| {
                if (std.mem.startsWith(u8, input, "s3://")) continue;
                const same = local_fs.sameFile(input, target) catch continue;
                if (same) {
                    diagnosticPrint("zpq query: input and output must be different files\n", .{});
                    return error.BadArgs;
                }
            }
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
        const pid: u32 = @bitCast(std.posix.system.getpid());
        const tmp = try std.fmt.bufPrint(&self.buffer, "{s}.{d}.zpq-tmp", .{ target, pid });
        const fd = try local_fs.createNewFile(tmp);
        self.len = tmp.len;
        self.target = target;
        try local_fs.closeChecked(fd);
        return self.path();
    }

    fn commit(self: *OutputPublication) !void {
        const target = self.target orelse return;
        try local_fs.rename(self.path(), target);
        self.target = null;
    }

    fn abort(self: *OutputPublication) void {
        if (self.target != null) local_fs.removeFile(self.path());
        self.target = null;
    }
};

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
        const pid: u32 = @bitCast(std.posix.system.getpid());
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
            var w: StdoutWriter = .{ .fd = 2 };
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
    var it = std.process.Args.Iterator.init(args);
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
/// Plain paths (no glob char) pass through as a single-element slice.
/// Only `*` is supported as a wildcard, in the basename only — covers
/// the duckdb-style `prefix/*.parquet` shape we want for the demo.
fn expandGlob(arena: std.mem.Allocator, io: std.Io, pattern: []const u8, env: std.process.Environ) ![][]const u8 {
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
        diagnosticPrint("zpq query: glob in directory portion not supported: {s}\n", .{pattern});
        return error.BadArgs;
    }

    const names = local_fs.listFiles(arena, io, dir_path) catch {
        diagnosticPrint("zpq query: cannot open dir {s}\n", .{dir_path});
        return error.OpenFailed;
    };
    var out: std.ArrayList([]const u8) = .empty;
    for (names) |name| {
        if (!matchSimpleGlob(basename, name)) continue;
        const full = if (last_slash != null)
            try std.fs.path.join(arena, &.{ dir_path, name })
        else
            name;
        try out.append(arena, full);
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
        diagnosticPrint("zpq query: s3 glob needs a prefix path: {s}\n", .{pattern});
        return error.BadArgs;
    };
    if (std.mem.indexOfScalar(u8, pattern[last_slash + 1 ..], '/') != null) {
        diagnosticPrint("zpq query: s3 glob in directory portion not supported: {s}\n", .{pattern});
        return error.BadArgs;
    }
    const prefix_part = pattern[0 .. last_slash + 1]; // includes trailing '/'
    const basename_glob = pattern[last_slash + 1 ..];

    // Parse out bucket + prefix-after-bucket. prefix_part starts
    // with "s3://" so let the existing parser handle the URL structure.
    const url_for_prefix = try std.fmt.allocPrint(arena, "{s}placeholder", .{prefix_part});
    const url = s3.Url.parse(url_for_prefix) catch |err| {
        diagnosticPrint("zpq query: bad s3 url {s}: {s}\n", .{ pattern, @errorName(err) });
        return err;
    };
    const list_prefix = url.key[0 .. url.key.len - "placeholder".len];

    const creds = s3.Credentials.fromEnv(env) catch |err| {
        diagnosticPrint(
            "zpq query: s3 glob requires S3_*/AWS_* env vars: {s}\n",
            .{@errorName(err)},
        );
        return err;
    };
    var client = try s3.Client.init(arena, creds, url.bucket);
    defer client.deinit();

    const entries = try s3.listAll(arena, &client, list_prefix);
    if (entries.len == 0) {
        diagnosticPrint("zpq query: s3 glob matched no objects under prefix {s}\n", .{list_prefix});
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

fn jsonFatal(gpa: std.mem.Allocator, kind: []const u8, reason: []const u8) !void {
    var w: StdoutWriter = .{};
    defer w.flush() catch {};
    _ = gpa;
    try w.writeAll("{\"error\":\"");
    try writeJsonString(&w, kind);
    try w.writeAll("\",\"reason\":\"");
    try writeJsonString(&w, reason);
    try w.writeAll("\"}\n");
}

fn writeJsonString(w: *StdoutWriter, s: []const u8) !void {
    try engine.writeJsonString(w, s);
}

/// The line a failed query's `Diag.detail` adds before the error's own message.
fn printDetail(diag: *const Diag) void {
    const name = diag.column.get();
    switch (diag.detail) {
        .none => {},
        .ambiguous_column => |a| diagnosticPrint(
            "{s}: column `{s}` is ambiguous: {s}\n",
            .{ @tagName(a.parser), name, a.hint },
        ),
        .schema_mismatch => |m| diagnosticPrint("schema mismatch: file 0 ({s}) vs file {d} ({s}): '{s}' vs '{s}'\n", .{
            m.first_input, m.file, diag.input.?, name, m.other.get(),
        }),
        .is_operand => diagnosticPrint("filter: only `IS NULL` / `IS NOT NULL` are supported after IS\n", .{}),
        .like_non_string => |l| diagnosticPrint(
            "filter: LIKE applies only to string columns (got {s} for `{s}`)\n",
            .{ l.type_name, name },
        ),
        .float16_filter => diagnosticPrint(
            "filter: FLOAT16 column `{s}` is not yet supported for filtering\n",
            .{name},
        ),
        // The upload logged it to stderr when it happened.
        .s3_request => {},
    }
}

fn causeName(diag: *const Diag) []const u8 {
    return if (diag.cause) |c| @errorName(c) else "unknown";
}

const StdoutWriter = struct {
    fd: local_fs.fd_t = 1,
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
            diagnosticPrint("zpq schema: failed to fetch S3 schema for {s}: {s}\n", .{ path, @errorName(err) });
            std.process.exit(1);
        };
    } else {
        const bytes = readFile(gpa, path) catch |err| {
            diagnosticPrint("zpq schema: failed to open input file {s}: {s}\n", .{ path, @errorName(err) });
            std.process.exit(1);
        };
        file_bytes = bytes;

        meta = metadata.open(arena, bytes) catch |err| {
            diagnosticPrint("zpq schema: input file {s} is not a valid Parquet file ({s})\n", .{ path, @errorName(err) });
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
        diagnosticPrint("zpq schema: schema tree build failed: {s}\n", .{@errorName(err)});
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
