//! Frontend paths and diagnostics shared by the CLI and in-process SQL requests.
const std = @import("std");
const zpq = @import("zpq.zig");
const local_fs = zpq.local_fs;
const s3 = zpq.io.s3;
const Diag = zpq.core.diag.Diag;

/// Receives each diagnostic, bounded to 16 KiB; a longer one is dropped, so the receiver falls back to the error
/// name. Unset, diagnostics print to stderr whole.
pub threadlocal var diagnostic_fn: ?*const fn ([]const u8) void = null;
/// `message` without the CLI's "zpq query: " prefix, for a machine-readable message: a JSON error or a response.
pub fn withoutCommand(message: []const u8) []const u8 {
    const prefix = "zpq query: ";
    return if (std.mem.startsWith(u8, message, prefix)) message[prefix.len..] else message;
}

fn diagnosticPrint(comptime fmt: []const u8, args: anytype) void {
    const report = diagnostic_fn orelse return std.debug.print(fmt, args);
    var buf: [16 * 1024]u8 = undefined;
    report(std.fmt.bufPrint(&buf, fmt, args) catch return);
}

pub fn noSqlMatches(table_name: []const u8) void {
    diagnosticPrint("zpq query: no files matched FROM table: {s}\n", .{table_name});
}

pub fn queryError(err: anyerror, diag: *const Diag) void {
    printDetail(diag);
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
        error.AggregateMutexWithSelect => diagnosticPrint(
            "zpq query: aggregate functions are mutually exclusive with --select / --columns\n",
            .{},
        ),
        error.MissingOutputOrAggregate => diagnosticPrint("zpq query: missing --output or --aggregate\n", .{}),
        error.SchemaMismatch => diagnosticPrint("zpq query: schema mismatch across inputs\n", .{}),
        error.InvalidUtf8 => diagnosticPrint(
            "zpq query: column `{s}` holds a STRING value that is not valid UTF-8;\n" ++
                "  the file is invalid (a STRING, ENUM or JSON column must be UTF-8)\n",
            .{diag.column.get()},
        ),
        error.InvalidBinaryValue => diagnosticPrint(
            "zpq query: column `{s}` holds a UUID or INTERVAL value of the wrong width;\n" ++
                "  the file is invalid (UUID is 16 bytes, INTERVAL 12)\n",
            .{diag.column.get()},
        ),
        error.NestedRowOutputNotSupported => if (diag.column.len > 0) diagnosticPrint(
            "zpq query: column `{s}` is repeated\n" ++
                "  (in a LIST or MAP, or a legacy REPEATED field); row output does not\n" ++
                "  support repeated columns yet\n",
            .{diag.column.get()},
        ) else diagnosticPrint(
            "zpq query: repeated columns (LIST, MAP or legacy REPEATED) are not\n" ++
                "  supported in row output yet\n",
            .{},
        ),
        error.NestedFilterNotSupported => diagnosticPrint(
            "zpq query: filter column `{s}` is repeated\n" ++
                "  (in a LIST or MAP, or a legacy REPEATED field); filtering on list\n" ++
                "  elements is not supported\n",
            .{diag.column.get()},
        ),
        error.UnlabelledColumn => diagnosticPrint(
            "zpq query: nested column {s} has no output name of its own:\n" ++
                "  a top-level column is named like it; name it with --select and AS\n",
            .{diag.column.get()},
        ),
        error.NestedReencodeNotSupported => diagnosticPrint(
            "zpq query: nested re-encoding is not supported yet\n",
            .{},
        ),
        error.INT96ReencodeNotSupported => diagnosticPrint("zpq query: INT96 re-encoding is not supported yet\n", .{}),
        error.LevelOutOfRange => diagnosticPrint(
            "zpq query: a page holds a repetition or definition level past its\n" ++
                "  column's maximum (corrupt file)\n",
            .{},
        ),
        error.IntOutOfRange => diagnosticPrint(
            "zpq query: a page holds a value past its column's 8- or 16-bit integer\n" ++
                "  type (corrupt file)\n",
            .{},
        ),
        error.InputChanged => diagnosticPrint(
            "zpq query: an input file was truncated or rewritten while the query read it\n",
            .{},
        ),
        error.Int96OutOfRange => diagnosticPrint(
            "zpq query: an INT96 timestamp outside 1677-2262 cannot be written as\n" ++
                "  64-bit nanoseconds\n",
            .{},
        ),
        error.UnsupportedDecimalPrecision => diagnosticPrint(
            "zpq query: column `{s}` has unsupported decimal precision (> 38)\n",
            .{diag.column.get()},
        ),
        error.DecimalValueExceedsPrecision => diagnosticPrint(
            "zpq query: column `{s}` has a decimal value exceeding its declared precision\n",
            .{diag.column.get()},
        ),
        error.UnsupportedLogicalTypeReencode => diagnosticPrint(
            "zpq query: cannot preserve the logical annotation while changing its physical type\n",
            .{},
        ),
        error.FooterSchemaChunkMismatch => diagnosticPrint("zpq query: footer schema chunk mismatch\n", .{}),
        error.CrossBucketNotSupported => diagnosticPrint("zpq query: cross-bucket queries are not supported\n", .{}),
        error.BadInputUrl => diagnosticPrint("zpq query: invalid input S3 URL\n", .{}),
        error.BadOutputUrl => diagnosticPrint("zpq query: invalid output S3 URL\n", .{}),
        error.NoCredentials => diagnosticPrint("zpq query: missing AWS credentials for S3 query\n", .{}),
        error.NoRegion => diagnosticPrint("zpq query: no S3 region; set AWS_REGION (or S3_REGION)\n", .{}),
        error.BadResponse => diagnosticPrint("zpq query: bad S3 HTTP response\n", .{}),
        error.TailTooSmall => diagnosticPrint("zpq query: file footer metadata tail too small\n", .{}),
        error.EncryptedFile => if (diag.input) |in| diagnosticPrint(
            "zpq query: input file {s}\n" ++
                "  is encrypted Parquet; zpq does not decrypt, so it neither reads nor\n" ++
                "  copies encrypted files\n",
            .{in},
        ) else diagnosticPrint(
            "zpq query: an input file is encrypted Parquet; zpq does not decrypt,\n" ++
                "  so it neither reads nor copies encrypted files\n",
            .{},
        ),
        error.ExternalColumnChunk => if (diag.input) |in| diagnosticPrint(
            "zpq query: input file {s}\n" ++
                "  keeps column data in another file (ColumnChunk.file_path); zpq\n" ++
                "  reads only self-contained files and never follows such a path\n",
            .{in},
        ) else diagnosticPrint(
            "zpq query: an input file keeps column data in another file\n" ++
                "  (ColumnChunk.file_path); zpq reads only self-contained files\n",
            .{},
        ),
        error.NotParquet, error.BadMagic => if (diag.input) |in| diagnosticPrint(
            "zpq query: input file {s} is not a valid Parquet file ({s})\n",
            .{ in, causeName(diag) },
        ) else diagnosticPrint("zpq query: input file is not a valid Parquet file\n", .{}),
        error.AggSumOverflow => diagnosticPrint("zpq query: aggregate sum overflowed integer limits\n", .{}),
        error.OpenFailed => if (diag.input) |in|
            diagnosticPrint("zpq query: failed to open input file {s} ({s})\n", .{ in, causeName(diag) })
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
        error.ModuloNotSupported => diagnosticPrint("zpq query: operator % is not supported\n", .{}),
        error.NestedColumnSelected => switch (diag.detail) {
            .nested_selected => |n| diagnosticPrint(
                "zpq query: column `{s}` is {s}; row output takes its leaves (e.g. `{s}`){s}. A SELECT of\n" ++
                    "  bare columns in the file's order written with -o keeps it whole\n",
                .{
                    diag.column.get(),
                    if (n.repeated) "repeated (a LIST or MAP)" else "nested (a STRUCT)",
                    n.example.get(),
                    if (n.repeated) ", though not repeated ones yet" else "",
                },
            ),
            else => diagnosticPrint("zpq query: a nested column is selected where a value is wanted\n", .{}),
        },
        error.ExpressionInPredicate => diagnosticPrint(
            "zpq query: `{s}` is an expression; a predicate compares a column with a literal\n",
            .{diag.column.get()},
        ),
        error.UnexpectedEnd => diagnosticPrint("zpq query: unexpected end of expression\n", .{}),
        error.BadNumber => diagnosticPrint("zpq query: invalid number in expression\n", .{}),
        error.UnsupportedCodec => diagnosticPrint(
            "zpq query: input uses a compression codec zpq cannot decompress\n",
            .{},
        ),
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
        error.ExceededMemoryBudget => diagnosticPrint(
            "zpq query: the query needs more memory than --max-memory allows\n",
            .{},
        ),
        error.DecodeExceededBudget => diagnosticPrint("zpq query: column decode exceeded --max-memory budget\n", .{}),
        error.NullableNotSupported => diagnosticPrint(
            "zpq query: nullable values are not supported in this expression\n",
            .{},
        ),
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
        error.TooLarge => diagnosticPrint(
            "zpq query: the output would hold a value, record or footer larger\n" ++
                "than Parquet can describe (a page holds at most 2 GiB)\n",
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
        error.NestedNotSupported => diagnosticPrint(
            "zpq query: nested columns are not supported in GROUP BY keys\n",
            .{},
        ),
        error.DivisionByZero => diagnosticPrint("zpq query: division by zero\n", .{}),
        error.UnbalancedParentheses => diagnosticPrint("zpq query: unbalanced parentheses in filter\n", .{}),
        error.UnsupportedCase => diagnosticPrint(
            "zpq query: only searched CASE (CASE WHEN cond THEN v ... END) is supported, with\n" ++
                "branches of one common type (DECIMAL branches of one DECIMAL type)\n",
            .{},
        ),
        error.HavingTypeMismatch => diagnosticPrint(
            "zpq query: HAVING compares a result with a literal of another type\n",
            .{},
        ),
        error.FilterTooDeep => diagnosticPrint(
            "zpq query: filter parentheses nest too deeply (limit {d})\n",
            .{zpq.core.filter.parser.MAX_GROUP_DEPTH},
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
        .non_literal_value => diagnosticPrint(
            "filter: `{s}` can be compared only with a literal (not a column, an\n" ++
                "  expression or LIKE ... ESCAPE)\n",
            .{name},
        ),
        // Printed with the error itself.
        .nested_selected => {},
        .bare_non_boolean => |b| diagnosticPrint(
            "filter: `{s}` alone is a predicate only if it is BOOLEAN (it is {s}); compare it, e.g. `{s} != 0`\n",
            .{ name, b.type_name, name },
        ),
        // The upload logged it to stderr when it happened.
        .s3_request => {},
    }
}

fn causeName(diag: *const Diag) []const u8 {
    return if (diag.cause) |c| @errorName(c) else "unknown";
}

/// Plain paths (no glob char) pass through as a single-element slice.
/// Only `*` is supported as a wildcard, in the basename only — covers
/// the duckdb-style `prefix/*.parquet` shape we want for the demo.
pub fn expandGlob(arena: std.mem.Allocator, io: std.Io, pattern: []const u8, env: std.process.Environ) ![][]const u8 {
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
