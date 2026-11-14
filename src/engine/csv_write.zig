//! A CSV file written as Parquet: the CSV source of the write window.
//!
//! The file is cut into fixed byte chunks. Each chunk's producer counts its quote bytes; the XOR of the earlier
//! chunks' parities is the exact quote state at its first byte (`core/csv/scan.zig`), so it finds its first record,
//! parses its records straight into typed output columns (a `write_window.Piece`, never a row table; strings stay
//! slices of the input), and hands the piece to the write window. The window's assembler cuts output row groups by
//! the writer's row and byte targets, encodes them on the shared executor and places them in order, exactly as it
//! does re-encoded Parquet: where chunks start and end never shows in the output, and the bytes are the same at
//! every `-j`.
//!
//! Types (`core/csv/types.zig`) come from a sample of the first records and are a promise checked on every value:
//! a value its column's type does not hold stops the write (`error.CsvTypeChanged`), the whole file is classified in
//! parallel, the types widen to what every value fits, and the write starts again. A pinned type (`Options.types`)
//! or `--csv-strict` refuses instead. DECIMAL precision is not part of that promise: the schema is written in the
//! footer, so it is set there from the largest value. Malformed input is refused at its line, byte and column; no
//! value is ever rounded, truncated, coerced or dropped.

const std = @import("std");
const csv_scan = @import("../core/csv/scan.zig");
const csv_types = @import("../core/csv/types.zig");
const schema = @import("../core/schema.zig");
const consumer = @import("../core/consumer.zig");
const core_memory = @import("../core/memory.zig");
const huge_pages = @import("../core/huge_pages.zig");
const system = @import("../core/system.zig");
const thrift = @import("../core/thrift.zig");
const page_index = @import("../core/writer/page_index.zig");
const streaming = @import("../core/writer/streaming.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_eval = @import("../core/filter/eval.zig");
const filter_selection = @import("../core/filter/selection.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const projection = @import("projection.zig");
const encoder = @import("../core/writer/encoder.zig");
const fastpath = @import("../core/writer/fastpath.zig");
const Diag = @import("../core/diag.zig").Diag;
const local_fs = @import("../local_fs.zig");
const local_reader = @import("../io/local_reader.zig");
const RowGroupGate = @import("../core/scan.zig").RowGroupGate;
const profile = @import("../profile.zig");
const nowMonoNs = @import("../clock.zig").monoNs;
const window_mod = @import("write_window.zig");
const write_mod = @import("write.zig");
const compression = @import("../core/parquet/compression.zig");
const engine_types = @import("types.zig");
const Context = engine_types.Context;
const QueryArgs = engine_types.QueryArgs;
const WriteResult = engine_types.WriteResult;
pub const Type = csv_types.Type;
const expr_ast = @import("../core/expr/ast.zig");
const syntax = @import("../core/expr/syntax.zig");
const syntax_plan = @import("syntax_plan.zig");

/// What a CSV source is told; every field has a default that sniffs or detects it.
pub const Options = struct {
    /// The field delimiter; null: the sniffed one (`, \t ; |`).
    delimiter: ?u8 = null,
    /// Whether the first record names the columns; null: detected (`types.firstRecordIsHeader`).
    header: ?bool = null,
    /// Unquoted field texts read as NULL besides the empty one (`--csv-null`).
    nulls: []const []const u8 = &.{},
    /// Columns whose type is given, not inferred; a value that does not fit is refused (`--csv-types`).
    types: []const Pinned = &.{},
    /// Refuse a value that does not fit its sampled type instead of widening and starting again.
    strict: bool = false,
    /// The columns written, by name and in this order; null: every column. The others are framed and counted, never
    /// typed: a value there can neither widen nor refuse.
    columns: ?[]const []const u8 = null,
    /// How much of the file the types are sampled from.
    sample_bytes: usize = 4 << 20,
    /// Forced-path control: the chunk size (chunks never change the output's bytes).
    chunk_bytes: ?usize = null,
    /// Forced-path controls for the reader: its profile (as `ZPQ_IO_PROFILE`), and what a chunk's read takes past it.
    io_profile: ?local_reader.Policy.Profile = null,
    overhang: ?usize = null,
    /// Forced-path control for tests: chunk `fault_chunk`'s producer fails before it reads, as a failed read does.
    fault_chunk: ?usize = null,
    /// Forced-path control: string columns are parsed into dictionary codes (`Coder`); off, into values. Either way
    /// the output's bytes are the same.
    dictionaries: bool = true,

    pub const Pinned = struct { column: []const u8, type: Type };
};

/// Why a CSV write stopped, for the diagnostic: where in the file (`line` counts newlines from 1, quoted ones too, as
/// an editor does) and in which column.
pub const Failure = struct {
    kind: Kind = .malformed,
    malformed: csv_scan.Malformed = .unterminated_quote,
    /// File offset of the field (or record) at fault.
    offset: u64 = 0,
    line: u64 = 0,
    /// The column (0-based), when a field is at fault.
    column: ?usize = null,
    expected_fields: usize = 0,
    got_fields: usize = 0,
    /// A value that did not fit its column's type, and the type.
    value: [64]u8 = undefined,
    value_len: usize = 0,
    type: Type = .{},

    pub const Kind = enum { malformed, field_count, type_changed, pinned_type, utf16, empty };

    pub fn valueText(f: *const Failure) []const u8 {
        return f.value[0..f.value_len];
    }
};

/// What a CSV write reports beside its result: why it stopped, if it did, and how it got its types.
pub const Report = struct {
    failure: Failure = .{},
    /// Writes started again after a value changed its column's type.
    restarts: usize = 0,
    /// Writes started again on fewer workers after the budget refused one.
    narrowed: usize = 0,
    /// Per column, whether a restart widened it (allocated by the write, on the caller's allocator).
    widened: []bool = &.{},
    chunks: usize = 0,
    /// How the file was read: the mapping, or the local reader (and why).
    io: ?local_reader.Choice = null,
    /// The types written (on the caller's allocator), for the summary.
    types: []Type = &.{},
    names: []const []const u8 = &.{},
    /// Every field's name, written or not, for a refusal's column.
    fields: []const []const u8 = &.{},
};

pub const Error = error{ CsvMalformed, CsvFieldCount, CsvTypeChanged, CsvPinnedType, CsvUtf16, CsvEmpty, CsvDuplicateColumn };

/// The parse of a file before its chunks run: dialect, header, column names and sampled types.
pub const Plan = struct {
    bytes: []const u8,
    data_start: usize,
    /// Newlines before `data_start` (the BOM and header), for line numbers.
    head_lines: u64,
    dialect: csv_scan.Dialect,
    header: bool,
    /// Per written column, in output order: its name, type and whether the type was given.
    names: []const []const u8,
    types: []Type,
    /// Columns whose type was given (`Options.types`): refused, never widened.
    pinned: []const bool,
    /// Per field of a record: its name, and the written column it fills, or null when it is not written.
    fields: []const []const u8,
    slot: []const ?u32,
    mean_record: usize,
    nulls: []const []const u8,
};

pub fn plan(a: std.mem.Allocator, bytes: []const u8, opts: Options, failure: *Failure) !Plan {
    // The file as a whole is refused: its location is its start, and no column.
    const bom = csv_scan.bomLen(bytes) catch {
        failure.* = .{ .kind = .utf16, .line = 1 };
        return error.CsvUtf16;
    };
    if (bytes.len == bom) {
        failure.* = .{ .kind = .empty, .line = 1 };
        return error.CsvEmpty;
    }
    const sample_end = @min(bytes.len, bom + opts.sample_bytes);
    const quote: u8 = '"';
    const dialect: csv_scan.Dialect = .{
        .delimiter = opts.delimiter orelse csv_scan.sniffDelimiter(bytes[0..sample_end], bom, quote),
        .quote = quote,
    };
    // The first record, then the sample's records after it.
    var s = csv_scan.Scanner.init(bytes[0..sample_end], bom, sample_end == bytes.len, dialect);
    var scratch: std.ArrayList(u8) = .empty;
    var first: std.ArrayList([]const u8) = .empty;
    var first_quoted: std.ArrayList(bool) = .empty;
    while (try nextField(&s, bytes, failure, first.items.len)) |f| {
        const t = try decode(a, bytes, f, dialect, &scratch, failure, 0, first.items.len);
        try first.append(a, try a.dupe(u8, t.bytes));
        try first_quoted.append(a, t.quoted);
        if (f.last) break;
    }
    const ncols = first.items.len;
    const after_first = s.position();
    var sampled = try a.alloc(Type, ncols);
    @memset(sampled, .{});
    var col: usize = 0;
    var records: usize = 0;
    var record_start = after_first;
    var last_end = after_first;
    // The sample only infers types: a malformed record ends it, and the parse refuses it where it is, by name.
    sample: while (s.next() catch null) |f| {
        if (f.last and col == 0 and f.start == f.end and ncols > 1) {
            record_start = s.position();
            continue; // a blank line
        }
        if (col >= ncols) break :sample;
        const t = csv_scan.decodeField(bytes[f.start..f.end], dialect.quote, &scratch, a) catch break :sample;
        if (!isNull(t, opts.nulls)) sampled[col] = sampled[col].join(classifyText(t.bytes, false));
        col += 1;
        if (f.last) {
            if (col != ncols) break :sample;
            col = 0;
            records += 1;
            last_end = s.position();
            record_start = last_end;
        }
    }
    const header = opts.header orelse csv_types.firstRecordIsHeader(first.items, sampled);
    if (!header) for (first.items, first_quoted.items, sampled) |text, quoted, *t| {
        if (!isNull(.{ .bytes = text, .quoted = quoted }, opts.nulls))
            t.* = t.join(csv_types.classify(text, std.unicode.utf8ValidateSlice(text)));
    };
    const names = try csv_types.columnNames(a, if (header) first.items else null, ncols);
    const pinned = try a.alloc(bool, ncols);
    @memset(pinned, false);
    for (opts.types) |p| {
        const i = for (names, 0..) |n, i| {
            if (std.mem.eql(u8, n, p.column)) break i;
        } else return unknownColumn(failure, p.column);
        sampled[i] = p.type;
        pinned[i] = true;
    }
    const data_start = if (header) after_first else bom;
    const counted = last_end - after_first;
    const slot = try a.alloc(?u32, ncols);
    var out_names = names;
    var out_types = sampled;
    var out_pinned = pinned;
    if (opts.columns) |cols| {
        @memset(slot, null);
        const on = try a.alloc([]const u8, cols.len);
        out_types = try a.alloc(Type, cols.len);
        const op = try a.alloc(bool, cols.len);
        for (cols, 0..) |c, o| {
            const i = for (names, 0..) |n, i| {
                if (std.mem.eql(u8, n, c)) break i;
            } else return unknownColumn(failure, c);
            if (slot[i] != null) return error.CsvDuplicateColumn;
            slot[i] = @intCast(o);
            on[o] = names[i];
            out_types[o] = sampled[i];
            op[o] = pinned[i];
        }
        out_names = on;
        out_pinned = op;
    } else for (slot, 0..) |*o, i| {
        o.* = @intCast(i);
    }
    return .{
        .bytes = bytes,
        .data_start = data_start,
        .head_lines = csv_scan.countNewlines(bytes[0..data_start]),
        .dialect = dialect,
        .header = header,
        .names = out_names,
        .types = out_types,
        .pinned = out_pinned,
        .fields = names,
        .slot = slot,
        .mean_record = if (records == 0) 64 else @max(1, counted / records),
        .nulls = opts.nulls,
    };
}

/// A column named (`--columns`, `--csv-types`) that the file does not have; its name is the failure's value.
fn unknownColumn(failure: *Failure, name: []const u8) error{UnknownColumn} {
    failure.* = .{};
    failure.value_len = @min(failure.value.len, name.len);
    @memcpy(failure.value[0..failure.value_len], name[0..failure.value_len]);
    return error.UnknownColumn;
}

fn nextField(s: *csv_scan.Scanner, bytes: []const u8, failure: *Failure, column: usize) !?csv_scan.Field {
    return s.next() catch {
        failure.* = scanFailure(s, column);
        failure.line = 1 + csv_scan.countNewlines(bytes[0..@intCast(failure.offset)]);
        return error.CsvMalformed;
    };
}

/// The scanner's refusal (it names a stray quote as one), in the column being read.
fn scanFailure(s: *const csv_scan.Scanner, column: usize) Failure {
    return .{ .kind = .malformed, .malformed = s.failure.kind, .offset = s.failure.offset, .column = column };
}

fn decode(
    a: std.mem.Allocator,
    bytes: []const u8,
    f: csv_scan.Field,
    d: csv_scan.Dialect,
    scratch: *std.ArrayList(u8),
    failure: *Failure,
    base_lines: u64,
    column: usize,
) !csv_scan.Text {
    const raw = bytes[f.start..f.end];
    return csv_scan.decodeField(raw, d.quote, scratch, a) catch |e| switch (e) {
        error.MalformedCsv => {
            failure.* = .{
                .kind = .malformed,
                .malformed = csv_scan.fieldMalformation(raw, d.quote),
                .offset = f.start,
                .column = column,
            };
            failure.line = base_lines + 1 + csv_scan.countNewlines(bytes[0..f.start]);
            return error.CsvMalformed;
        },
        else => |oom| return oom,
    };
}

fn fieldCount(failure: *Failure, bytes: []const u8, record: usize, want: usize, got: usize) Error {
    // The column is the first field missing, or the first past the header's.
    failure.* = .{
        .kind = .field_count,
        .offset = record,
        .column = @min(want, got),
        .expected_fields = want,
        .got_fields = got,
    };
    failure.line = 1 + csv_scan.countNewlines(bytes[0..record]);
    return error.CsvFieldCount;
}

/// A non-NULL value's type alone; `ascii`: its chunk has no byte past 0x7f, so it is UTF-8 unchecked.
fn classifyText(s: []const u8, ascii: bool) Type {
    return csv_types.classify(s, ascii or std.unicode.utf8ValidateSlice(s));
}

/// The text was unescaped into the producer's scratch, which the next field reuses.
fn unescaped(t: csv_scan.Text, scratch: *const std.ArrayList(u8)) bool {
    return t.bytes.ptr == scratch.items.ptr;
}

fn isNull(t: csv_scan.Text, nulls: []const []const u8) bool {
    if (t.quoted) return false;
    if (t.bytes.len == 0) return true;
    for (nulls) |n| if (std.mem.eql(u8, n, t.bytes)) return true;
    return false;
}

// ---------------------------------------------------------------- the chunks

/// The window's piece source over a planned file: chunk `i` is bytes `[data_start + i * chunk, ...)`.
/// The file's fields as a Parquet schema, which a query over it is bound to as a Parquet query is: every field of
/// `all` (planned with no column list), each typed as `p` has it where `p` parses it.
pub fn fileSchema(a: std.mem.Allocator, all: *const Plan, p: ?*const Plan) !*schema.FileMetaData {
    var items: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try items.append(a, .{
        .name = "schema",
        .type = null,
        .repetition_type = null,
        .type_length = null,
        .num_children = @intCast(all.fields.len),
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    for (all.fields, 0..) |name, ci| {
        const t = if (p) |pp| if (pp.slot[ci]) |o| pp.types[o] else all.types[ci] else all.types[ci];
        try items.append(a, csv_types.schemaElement(name, t));
    }
    const meta = try a.create(schema.FileMetaData);
    meta.* = .{
        .version = 2,
        .schema = items,
        .num_rows = 0,
        .created_by = schema.ZPQ_CREATED_BY,
        .row_groups = .empty,
    };
    return meta;
}

/// A chunk's parsed columns as the batch filters, expressions and aggregates read (`lookup`: per field, its parsed
/// position), and per field what an exact output takes in its place: a DECIMAL's unscaled integers (the lossless
/// lane), where the batch has its scaled view. A DECIMAL no filter, expression or aggregate reads (`evaluated`) gets
/// no view.
pub const ChunkBatch = struct {
    batch: filter_eval.Batch,
    exact: []?filter_eval.Batch.Column,

    pub fn of(
        p: *const Plan,
        parsed: *const consumer.OutputAggregator,
        lookup: []const ?usize,
        evaluated: []const bool,
        /// An aggregate's batch: a DECIMAL of at most 18 digits keeps its parsed i64 integers as its exact lane, beside
        /// its scaled view, as a Parquet INT64 DECIMAL decodes; no i128 copy, and no exact output column.
        narrow: bool,
        scratch: std.mem.Allocator,
    ) !ChunkBatch {
        const rows = parsed.num_rows;
        const cols = try scratch.alloc(filter_eval.Batch.Column, parsed.cols.len);
        const exact = try scratch.alloc(?filter_eval.Batch.Column, lookup.len);
        @memset(exact, null);
        for (lookup, 0..) |l, ci| {
            const k = l orelse continue;
            const raw = consumer.bufRows(parsed.cols[k], 0, rows);
            const t = p.types[k];
            if (t.kind != .decimal) {
                cols[k] = raw;
                continue;
            }
            if (narrow and raw == .i64) {
                const x = raw.i64;
                var view = try decimal_mod.scaledF64FromInt(scratch, x, t.scale);
                view.unscaled = .{ .i64 = x.values };
                cols[k] = .{ .f64 = view };
                continue;
            }
            const unscaled: filter_eval.ColumnT(i128) = switch (raw) {
                .i128 => |x| x,
                .i64 => |x| blk: {
                    const v = try scratch.alloc(i128, x.values.len);
                    for (v, x.values) |*d, e| d.* = e;
                    break :blk .{ .values = v, .def_levels = x.def_levels, .max_def = x.max_def, .has_nulls = x.has_nulls };
                },
                else => unreachable,
            };
            exact[ci] = .{ .i128 = unscaled };
            // A DECIMAL only passed through is written from its integers, exactly; one a filter or an expression
            // reads gets its scaled view too, as a Parquet DECIMAL decodes to one (`decimal.decodeColumnAsF64`). Past
            // 18 digits it converts value by value from i128, as a FIXED_LEN_BYTE_ARRAY DECIMAL's does.
            if (!evaluated[ci]) {
                cols[k] = .{ .i128 = unscaled };
                continue;
            }
            var view = if (raw == .i64)
                try decimal_mod.scaledF64FromInt(scratch, unscaled, t.scale)
            else blk: {
                const v = try scratch.alloc(f64, unscaled.values.len);
                for (v, unscaled.values) |*d, u| d.* = decimal_mod.applyScaleI128(u, t.scale);
                break :blk filter_eval.ColumnT(f64){
                    .values = v,
                    .def_levels = unscaled.def_levels,
                    .max_def = unscaled.max_def,
                    .has_nulls = unscaled.has_nulls,
                };
            };
            view.unscaled = .{ .i128 = unscaled.values };
            cols[k] = .{ .f64 = view };
        }
        return .{ .batch = .{ .cols = cols, .num_rows = rows }, .exact = exact };
    }
};

/// A filter or computed SELECT over the file, bound as a Parquet query is (`projection.planProjection`) against the
/// file's columns as a schema: each chunk parses only the columns it reads (`fetch`), filters them, and writes its
/// kept rows' outputs, as `consumer.ProjectedRG` writes a row group's.
pub const Projection = struct {
    /// The file's columns as a schema (every field, typed as planned), which the query is bound to.
    meta: *const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    output_specs: []const consumer.OutputCol,
    /// Per field: whether a query reads it, and its position among the parsed columns (`Plan.slot`).
    fetch: []const bool,
    lookup: []const ?usize,
    /// Per field: read by a computed output; read by the filter or a computed output (what needs a scaled
    /// DECIMAL view, beside the unscaled integers a passthrough writes).
    computed_refs: []const bool,
    evaluated: []const bool,
    /// The outputs' schema and paths (`consumer.initOutputAggregator`), and per output the parsed column it passes
    /// through (null: computed), for a DECIMAL's precision from the data.
    out_elems: []schema.SchemaElement,
    out_paths: [][]const []const u8,
    source: []const ?usize,

    /// Whether the query needs one: a filter, or a SELECT item that is not a plain column under its own name. Plain
    /// columns are a projection the parse does itself (`Options.columns`).
    pub fn needed(args: QueryArgs) bool {
        if (args.filter != null) return true;
        if (args.plan_source) |ps| {
            const q = syntax_plan.syntaxOf(ps) orelse return false;
            if (q.filter != null) return true;
            for (q.select orelse &.{}) |item| if (itemName(item) == null) return true;
            return false;
        }
        if (args.select != null and args.columns == null) {
            const sel = args.select_cols orelse return true;
            if (sel.len == 0) return true;
            for (sel) |c| if (selectName(c) == null) return true;
        }
        return false;
    }

    /// Bound against every field of `all` (planned with no column list), each typed as `p` has it where `p` parses it.
    pub fn bind(a: std.mem.Allocator, args: QueryArgs, all: *const Plan, p: ?*const Plan) !Projection {
        const n = all.fields.len;
        const meta = try fileSchema(a, all, p);
        // A SQL query binds through a plan cache, as a Parquet query does (`QueryArgs.withPlanCache`); this one lives
        // with the projection, bound against this pass's types.
        const cache = try a.create(engine_types.bound_plan.Cache);
        cache.* = .{};
        const pp = projection.planProjection(a, args.withPlanCache(cache), meta) catch |e| return switch (e) {
            error.InvalidBoundPlan => error.CsvQueryNotSupported,
            else => e,
        };
        // `--columns` names the columns written and their order, under a filter as without one (the Parquet planner
        // keeps its bitmap in file order).
        var specs = pp.output_specs;
        if (args.columns) |cols| if (args.select == null and args.plan_source == null) {
            const ordered = try a.alloc(consumer.OutputCol, cols.len);
            for (cols, ordered) |name, *o| {
                const ci = for (all.fields, 0..) |f, i| {
                    if (std.mem.eql(u8, f, name)) break i;
                } else return error.UnknownColumn;
                o.* = .{ .passthrough = .{ .ci = ci } };
            }
            specs = ordered;
        };
        const lookup = try a.alloc(?usize, n);
        var pos: usize = 0;
        for (lookup, pp.fetch) |*l, f| {
            l.* = if (f) pos else null;
            if (f) pos += 1;
        }
        const refs = try a.alloc(bool, n);
        @memset(refs, false);
        for (specs) |spec| switch (spec) {
            .computed => |c| c.expr.collectColumns(refs),
            .passthrough => {},
        };
        const evaluated = try a.dupe(bool, refs);
        if (pp.filter) |f| {
            var cols: std.ArrayList(usize) = .empty;
            try f.collectColumns(&cols, a);
            for (cols.items) |ci| if (ci < n) {
                evaluated[ci] = true;
            };
        }
        const template = try consumer.initOutputAggregator(a, meta, specs);
        const source = try a.alloc(?usize, specs.len);
        for (source, specs) |*src, spec| src.* = switch (spec) {
            .passthrough => |pt| lookup[pt.ci],
            .computed => null,
        };
        return .{
            .meta = meta,
            .filter = pp.filter,
            .output_specs = specs,
            .fetch = pp.fetch,
            .lookup = lookup,
            .computed_refs = refs,
            .evaluated = evaluated,
            .out_elems = template.schema_elems,
            .out_paths = template.paths,
            .source = source,
        };
    }

    /// The fields' names a chunk parses, in file order.
    pub fn fetchedNames(pr: *const Projection, a: std.mem.Allocator, fields: []const []const u8) ![]const []const u8 {
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        for (fields, pr.fetch) |name, f| if (f) try out.append(a, name);
        return out.items;
    }

    /// `pc.agg` as the outputs of `parsed`'s kept rows. Filters and expressions read a DECIMAL as its scaled value,
    /// a passthrough writes its unscaled integers (the lossless lane); `scratch` holds the views.
    fn project(
        pr: *const Projection,
        p: *const Plan,
        pc: *window_mod.Piece,
        parsed: *const consumer.OutputAggregator,
        scratch: std.mem.Allocator,
    ) !void {
        const cb = try ChunkBatch.of(p, parsed, pr.lookup, pr.evaluated, false, scratch);
        const batch = cb.batch;
        const pass = cb.exact;
        const rows = parsed.num_rows;
        var sel = try filter_selection.SelectionVector.init(scratch, rows);
        if (pr.filter) |f| try filter_eval.evaluate(f, &batch, &sel, pr.lookup, scratch);
        const surviving = sel.count();
        const pa = pc.arena.allocator();
        pc.agg = try consumer.initOutputAggregator(pa, pr.meta, pr.output_specs);
        // A piece's string dictionary is kept for its kept rows where it is no bigger than they are (`appendSelected`).
        pc.agg.keep_codes = true;
        pc.agg.captured = true;
        try consumer.projectDecoded(&pc.agg, &batch, pr.lookup, pass, &sel, surviving, pr.output_specs, pr.computed_refs, scratch);
    }
};

pub const Source = struct {
    plan: *const Plan,
    elems: []const schema.SchemaElement,
    paths: []const []const []const u8,
    chunk: usize,
    count: usize,
    /// Per chunk: its quote parity and newline count, published by its producer before it parses.
    stats: []csv_scan.ChunkStats,
    ready: []std.Io.Event,
    /// Per chunk and DECIMAL column: the largest unscaled magnitude it held.
    max_abs: []u128,
    /// Per chunk: why it stopped, if it did.
    failures: []Failure,
    failed: []bool,
    /// Per chunk, the error that stopped its producer before it published its quote count (`ready` is set either
    /// way): the chunks after it cannot find their records, and say why.
    lost: []?anyerror,
    rows: std.atomic.Value(u64) = .init(0),
    /// Classify instead of convert: each chunk's join of its values' types, for a restart.
    classify: bool = false,
    seen: []Type = &.{},
    /// What producers read: the local reader's buffer, laid out like the file (`plan.bytes` offsets), or the mapping.
    buf: []const u8,
    /// The reader's per-chunk gate: a chunk's bytes are in once it opens, and go back once its piece is freed.
    gate: ?RowGroupGate = null,
    /// Bytes each chunk's read takes past its end: the record straddling its successor's first byte is in it.
    overhang: usize = 0,
    opts: Options = .{},
    /// A filter or computed SELECT: each chunk's fields are parsed into scratch and its kept rows projected.
    proj: ?*const Projection = null,
    /// An aggregate: each chunk's fields are parsed into a pooled scratch arena and handed to `fold` (`csv_agg`).
    fold: ?Fold = null,

    pub const Fold = struct {
        ctx: *anyopaque,
        /// A scratch arena for one chunk's parse, and its return, reset, once the chunk has folded.
        take: *const fn (ctx: *anyopaque) anyerror!*std.heap.ArenaAllocator,
        give: *const fn (ctx: *anyopaque, arena: *std.heap.ArenaAllocator) void,
        /// Chunk `i`'s parsed columns, on `scratch_alloc` (the taken arena).
        fold: *const fn (ctx: *anyopaque, p: *const Plan, parsed: *const consumer.OutputAggregator, scratch_alloc: std.mem.Allocator, i: usize) anyerror!void,
    };

    pub fn init(a: std.mem.Allocator, p: *const Plan, chunk: usize) !Source {
        const data = p.bytes.len - p.data_start;
        const count = @max(1, std.math.divCeil(usize, data, chunk) catch 1);
        const ncols = p.names.len;
        const elems = try a.alloc(schema.SchemaElement, ncols);
        const paths = try a.alloc([]const []const u8, ncols);
        for (elems, paths, p.names, p.types) |*e, *path, n, t| {
            e.* = csv_types.schemaElement(n, t);
            path.* = try a.dupe([]const u8, &.{n});
        }
        const s: Source = .{
            .plan = p,
            .elems = elems,
            .paths = paths,
            .chunk = chunk,
            .count = count,
            .stats = try a.alloc(csv_scan.ChunkStats, count),
            .ready = try a.alloc(std.Io.Event, count),
            .max_abs = try a.alloc(u128, count * ncols),
            .failures = try a.alloc(Failure, count),
            .failed = try a.alloc(bool, count),
            .lost = try a.alloc(?anyerror, count),
            .buf = p.bytes,
        };
        @memset(s.ready, .unset);
        @memset(s.lost, null);
        @memset(s.max_abs, 0);
        @memset(s.failed, false);
        return s;
    }

    /// What `init` allocated per chunk and column, for the write's ledger.
    pub fn metaBytes(s: *const Source) usize {
        const per_chunk = @sizeOf(csv_scan.ChunkStats) + @sizeOf(std.Io.Event) + @sizeOf(Failure) + @sizeOf(bool) +
            @sizeOf(?anyerror) + s.elems.len * @sizeOf(u128);
        // Per column: its schema element, and its one-name path.
        const per_column = @sizeOf(schema.SchemaElement) + @sizeOf([]const []const u8) + @sizeOf([]const u8);
        return s.count *| per_chunk +| s.elems.len * per_column;
    }

    pub fn bound(s: *const Source, i: usize) usize {
        return @min(s.plan.bytes.len, s.plan.data_start + i * s.chunk);
    }

    /// Chunk `i`'s planned read: its bytes and the overhang past them, clipped to the file.
    fn readEnd(s: *const Source, i: usize) usize {
        if (s.gate == null) return s.plan.bytes.len;
        return @min(s.plan.bytes.len, s.bound(i + 1) + s.overhang);
    }

    /// Chunk `i`'s records, from the first record start at or after its first byte to its successor's, and the bytes
    /// to parse them from: its read, or the mapping for a record longer than the overhang.
    fn records(s: *Source, io: std.Io, i: usize) !struct { lo: usize, hi: usize, lines: u64, bytes: []const u8 } {
        const p = s.plan;
        const map = p.bytes;
        if (s.gate) |g| g.wait(g.ctx, i) catch |e| return s.abandon(io, i, e);
        const lo_b = s.bound(i);
        const hi_b = s.bound(i + 1);
        const end = s.readEnd(i);
        const view = s.buf[0..end];
        s.stats[i] = csv_scan.chunkStats(view[lo_b..hi_b], p.dialect.quote);
        s.ready[i].set(io);
        // Chunks are dispatched in order, so every earlier one is already counting its quotes.
        var parity: u1 = 0;
        var lines: u64 = p.head_lines;
        for (0..i) |k| {
            s.ready[k].waitUncancelable(io);
            // An earlier chunk that never counted its quotes leaves this one no quote state to start from.
            if (s.lost[k]) |e| return e;
            parity ^= s.stats[k].quote_parity;
            lines += s.stats[k].newlines;
        }
        const q = p.dialect.quote;
        const in_lo = parity == 1;
        const in_hi = (parity ^ s.stats[i].quote_parity) == 1;
        // In the read when the newline is there; past the overhang, from the mapping (a record that long is rare).
        const lo = if (i == 0) p.data_start else csv_scan.recordStart(view, lo_b, in_lo, q) orelse
            csv_scan.recordStart(map, lo_b, in_lo, q) orelse map.len;
        const hi = if (i + 1 >= s.count) map.len else csv_scan.recordStart(view, hi_b, in_hi, q) orelse
            csv_scan.recordStart(map, hi_b, in_hi, q) orelse map.len;
        // Starts found from exact quote states only increase: a decrease is quotes that do not pair.
        if (hi < lo) return s.fail(i, .{ .malformed = .inconsistent_quotes, .offset = hi_b }, lines, lo_b);
        return .{ .lo = lo, .hi = hi, .lines = lines, .bytes = if (hi <= end) view else map };
    }

    /// Chunk `i`'s producer stops before publishing its quote count: publish `err` in its place, so every chunk
    /// after it, waiting on it, stops with it instead of waiting for good.
    fn abandon(s: *Source, io: std.Io, i: usize, err: anyerror) anyerror {
        if (s.ready[i].isSet()) return err;
        s.lost[i] = err;
        s.ready[i].set(io);
        return err;
    }

    fn fail(s: *Source, i: usize, f: Failure, lines: u64, from: usize) Error {
        s.failures[i] = f;
        const at = @max(from, @as(usize, @intCast(f.offset)));
        s.failures[i].line = lines + 1 + csv_scan.countNewlines(s.plan.bytes[from..at]);
        s.failed[i] = true;
        return switch (f.kind) {
            .malformed => error.CsvMalformed,
            .field_count => error.CsvFieldCount,
            .type_changed => error.CsvTypeChanged,
            .pinned_type => error.CsvPinnedType,
            .utf16 => error.CsvUtf16,
            .empty => error.CsvEmpty,
        };
    }

    /// The earliest chunk's failure.
    pub fn firstFailure(s: *const Source) ?*const Failure {
        for (s.failed, s.failures) |f, *x| if (f) return x;
        return null;
    }

    /// Parse chunk `i` into a piece (or, classifying, into `seen`).
    pub fn produce(s: *Source, io: std.Io, piece: ?*window_mod.Piece, i: usize) !void {
        const p = s.plan;
        const span = try s.records(io, i);
        const ncols = p.fields.len;
        const nout = p.names.len;
        const bytes = span.bytes;
        const lo_b = s.bound(i);
        var builder: Builder = undefined;
        // Under a projection the fields are parsed into scratch, on the piece's ledger-backed allocator, and only the
        // kept rows' outputs go into the piece (`project`).
        // An aggregate's chunks (`fold`) parse into scratch too, folded and freed chunk by chunk.
        var parse_state: ?std.heap.ArenaAllocator = null;
        defer if (parse_state) |*ps| ps.deinit();
        var parsed: consumer.OutputAggregator = undefined;
        if (piece) |pc| if (s.proj != null) {
            parse_state = .init(pc.arena.child_allocator);
        };
        // An aggregate's chunk parses into a worker's scratch arena, reset (not freed) once the chunk has folded
        // (`Fold.take`, `Fold.give`): the next chunk reuses its pages instead of faulting in fresh ones.
        const fold_arena: ?*std.heap.ArenaAllocator = if (s.fold) |f| try f.take(f.ctx) else null;
        defer if (fold_arena) |ar| s.fold.?.give(s.fold.?.ctx, ar);
        const a: std.mem.Allocator = if (fold_arena) |ar| ar.allocator() else if (parse_state) |*ps| ps.allocator() else if (piece) |pc|
            pc.arena.allocator()
        else
            std.heap.page_allocator;
        const agg: *consumer.OutputAggregator = if (piece) |pc| (if (s.proj != null) &parsed else &pc.agg) else &parsed;
        const seen: []Type = if (s.classify) s.seen[i * nout ..][0..nout] else &.{};
        if (s.classify) @memset(seen, .{});
        const parses = piece != null or s.fold != null;
        if (parses) try builder.init(a, agg, s, span.hi - span.lo);
        if (span.lo >= span.hi) {
            if (parses) try s.finish(&builder, a, agg, piece, i);
            return;
        }
        const ascii = !hasHighBit(bytes[span.lo..span.hi]);
        // Strings stay slices of the mapping, which outlives the write; the reader's chunk pages do not.
        const own_strings = bytes.ptr != p.bytes.ptr;
        var scanner = csv_scan.Scanner.init(bytes, span.lo, bytes.len == p.bytes.len, p.dialect);
        // Unescaping copies a field into scratch as long as its longest escaped field: on the piece's arena, so the
        // ledger holds it (a classifying pass has no piece, and writes nothing).
        const scratch_alloc = a;
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(scratch_alloc);
        var col: usize = 0;
        var record_start = span.lo;
        // Per column: the run of fields from it on that the parse neither writes nor classifies, which
        // `Scanner.skipPlain` passes over without returning them when they hold no quote.
        const unread = try a.alloc(u32, ncols);
        {
            var streak: u32 = 0;
            var c = ncols;
            while (c > 0) {
                c -= 1;
                streak = if (p.slot[c] == null) streak + 1 else 0;
                unread[c] = streak;
            }
        }
        while (true) {
            var ends_record = false;
            const unread_run = if (col < ncols) unread[col] else 0;
            if (unread_run > 0) {
                const to_end = col + unread_run == ncols;
                const skipped = scanner.skipPlain(unread_run, to_end);
                col += skipped;
                ends_record = to_end and skipped == unread_run;
            }
            if (!ends_record) {
                const next = scanner.next() catch return s.fail(i, scanFailure(&scanner, col), span.lines, lo_b);
                const f = next orelse break;
                if (f.last and col == 0 and f.start == f.end and ncols > 1) {
                    // A blank line holds no record.
                    record_start = scanner.position();
                    if (record_start >= span.hi) break;
                    continue;
                }
                if (col >= ncols) return s.fail(i, .{
                    .kind = .field_count,
                    .offset = record_start,
                    // The first field past the header's.
                    .column = ncols,
                    .expected_fields = ncols,
                    // The extra field may end its record; only one that does not has more to count.
                    .got_fields = col + 1 + if (f.last) 0 else countRest(&scanner),
                }, span.lines, lo_b);
                const raw = bytes[f.start..f.end];
                const text = csv_scan.fieldText(f, raw, p.dialect.quote, &scratch, scratch_alloc) catch |e| switch (e) {
                    error.MalformedCsv => return s.fail(i, .{
                        .kind = .malformed,
                        .malformed = csv_scan.fieldMalformation(raw, p.dialect.quote),
                        .offset = f.start,
                        .column = col,
                    }, span.lines, lo_b),
                    else => |oom| return oom,
                };
                // A field not written is framed and decoded (so a malformed one still refuses), and nothing more.
                if (p.slot[col]) |out| if (s.classify) {
                    if (!isNull(text, p.nulls))
                        seen[out] = seen[out].join(classifyText(text.bytes, ascii));
                } else if (!(try builder.add(a, out, text, ascii, p.nulls, own_strings or unescaped(text, &scratch)))) {
                    const t = p.types[out];
                    var fl: Failure = .{
                        .kind = if (p.pinned[out]) .pinned_type else .type_changed,
                        .offset = f.start,
                        .column = col,
                        .type = t,
                    };
                    fl.value_len = @min(fl.value.len, text.bytes.len);
                    @memcpy(fl.value[0..fl.value_len], text.bytes[0..fl.value_len]);
                    return s.fail(i, fl, span.lines, lo_b);
                };
                col += 1;
                ends_record = f.last;
            }
            if (ends_record) {
                if (col != ncols) return s.fail(i, .{
                    .kind = .field_count,
                    .offset = record_start,
                    // The first field missing.
                    .column = col,
                    .expected_fields = ncols,
                    .got_fields = col,
                }, span.lines, lo_b);
                col = 0;
                if (!s.classify) builder.rows += 1;
                record_start = scanner.position();
                if (record_start >= span.hi) {
                    if (record_start != span.hi) return s.fail(i, .{
                        .kind = .malformed,
                        .malformed = .inconsistent_quotes,
                        .offset = span.hi,
                    }, span.lines, lo_b);
                    break;
                }
            }
        }
        // Every record of the chunk ends inside its read; one that does not is quotes that do not pair.
        if (record_start < span.hi) return s.fail(i, .{
            .kind = .malformed,
            .malformed = .inconsistent_quotes,
            .offset = span.hi,
        }, span.lines, lo_b);
        if (parses) try s.finish(&builder, a, agg, piece, i);
    }

    /// The chunk's columns done: the piece holds them, or, under a projection, the outputs of its kept rows; or an
    /// aggregate folds them (`fold`).
    fn finish(
        s: *Source,
        builder: *Builder,
        a: std.mem.Allocator,
        agg: *consumer.OutputAggregator,
        piece: ?*window_mod.Piece,
        i: usize,
    ) !void {
        try builder.finish(a, agg, s, i);
        const pc = piece orelse return s.fold.?.fold(s.fold.?.ctx, s.plan, agg, a, i);
        if (s.proj) |pr| try pr.project(s.plan, pc, agg, a);
        pc.bits_prefix = try consumer.plainBitsPrefix(pc.arena.allocator(), &pc.agg);
    }

    /// The window's view of this source.
    pub fn pieceSource(s: *Source) window_mod.PieceSource {
        return .{ .ctx = s, .count = s.count, .produce = produceErased, .item_bytes = s.chunk *| 3 };
    }

    fn produceErased(ctx: *anyopaque, w: *window_mod.WinCtx, i: usize) anyerror!*window_mod.Piece {
        const s: *Source = @ptrCast(@alignCast(ctx));
        // Read through the reader, a piece copies its strings, so the chunk's pages go back once it is parsed: a row
        // group spans many chunks, and pinning their reads until it is encoded would need a window that holds it.
        // Given back on every exit, a failed one too.
        defer if (s.gate) |g| g.done(g.ctx, i, 1);
        if (s.opts.fault_chunk == i) return s.abandon(w.io, i, error.LocalReadFailed);
        const piece = w.takePiece() catch |e| return s.abandon(w.io, i, e);
        errdefer piece.unref(w.gpa);
        try s.produce(w.io, piece, i);
        return piece;
    }
};

/// Fields left in the current record (for a too-long record's count).
fn countRest(s: *csv_scan.Scanner) usize {
    var n: usize = 0;
    while (s.next() catch null) |f| {
        n += 1;
        if (f.last) break;
    }
    return n;
}

fn hasHighBit(bytes: []const u8) bool {
    const V = @Vector(64, u8);
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + 64 <= bytes.len) : (i += 64) acc |= @as(V, bytes[i..][0..64].*);
    var tail: u8 = 0;
    for (bytes[i..]) |c| tail |= c;
    return (@reduce(.Or, acc) | tail) & 0x80 != 0;
}

/// A string column's rows as codes into the piece's own dictionary while its values repeat: each distinct string is
/// hashed and kept once (copied once, where it must be), a row takes four bytes, and the encoder builds the chunk's
/// dictionary from the codes (`encoder.planCoded`) without hashing a value, to the bytes hashing the values writes.
/// Coding pays only for a column the encoder will dictionary-encode (`encoder.tryEncodeDictBytes`): one whose
/// values are hashed here and then again there pays twice. So a column goes back to values for the rest of the piece
/// once its own data says the encoder would refuse it, by the encoder's own limits on the present values seen
/// (`encoder.stringDictMaxEntries`, `encoder.stringDictMaxBytes`): its dictionary past what one dictionary page holds
/// (`encoder.dictPageMaxBytes`; a merge only adds entries, so that is final), or past either limit while its newest
/// values still grow it faster than the limit grows (`judge`).
const Coder = struct {
    /// Values of two bytes or more; a one-byte value's code (plus one; 0: none yet) is in `one`.
    map: std.StringHashMapUnmanaged(u32) = .empty,
    /// Made on the column's first one-byte value, so a column without one costs a pointer.
    one: ?*[256]u32 = null,
    dict: std.ArrayList([]const u8) = .empty,
    /// The dictionary's PLAIN bytes, and the present values' and their PLAIN bytes, as the encoder counts them.
    bytes: usize = 0,
    present: usize = 0,
    plain: usize = 0,
    /// Present values at the next judgment, and what was counted at the last one.
    next_judged: usize = encoder.dict_min_present,
    last: struct { present: usize = 0, plain: usize = 0, entries: usize = 0, bytes: usize = 0 } = .{},

    /// A new entry's code; its bytes outlive the chunk as the entry does.
    fn insert(c: *Coder, a: std.mem.Allocator, s: []const u8, copy: bool) !u32 {
        const code: u32 = @intCast(c.dict.items.len);
        try c.dict.append(a, if (copy) try a.dupe(u8, s) else s);
        c.bytes += 4 + s.len;
        return code;
    }

    /// At doubling counts of present values: whether the column should go back to values. A dictionary grows
    /// concavely in the values it has seen (each new value is less likely to be new), so one within both limits
    /// whose newest values also arrive within them stays within them. One past a limit while its newest values still
    /// arrive past it is the one the encoder would refuse unless its values stop being new; it is given up.
    fn judge(c: *Coder) bool {
        const entries = c.dict.items.len;
        // The limits are a quarter of the count and of the PLAIN bytes; the same as rates over the newest window.
        const entries_over = entries > encoder.stringDictMaxEntries(c.present) and
            4 * (entries - c.last.entries) > c.present - c.last.present;
        const bytes_over = c.bytes > encoder.stringDictMaxBytes(c.plain) and
            4 * (c.bytes - c.last.bytes) > c.plain - c.last.plain;
        c.last = .{ .present = c.present, .plain = c.plain, .entries = entries, .bytes = c.bytes };
        c.next_judged = 2 * c.present;
        return entries_over or bytes_over;
    }
};

/// Typed output columns being filled from one chunk's fields.
const Builder = struct {
    cols: []consumer.OutputAggregator.ColumnBuf,
    types: []const Type,
    /// A pinned DECIMAL holds its declared precision; an inferred one any its physical type holds (`decimalCap`).
    pinned: []const bool,
    max_abs: []u128,
    /// Per column; used by coded string columns only.
    coders: []Coder,
    rows: usize = 0,

    /// Columns for `s`'s parsed fields in `agg`, on `pa`: the piece's own (`piece.agg`), or, under a projection,
    /// a scratch aggregator its kept rows are projected from (`Projection.project`).
    fn init(b: *Builder, pa: std.mem.Allocator, agg: *consumer.OutputAggregator, s: *const Source, span_bytes: usize) !void {
        const p = s.plan;
        const ncols = p.names.len;
        const cols = try pa.alloc(consumer.OutputAggregator.ColumnBuf, ncols);
        const reserve = span_bytes / @max(1, p.mean_record) + span_bytes / @max(1, p.mean_record) / 4 + 16;
        for (cols, p.types) |*c, t| {
            c.* = switch (columnLane(t)) {
                inline else => |lane| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(lane), .{}),
            };
            switch (c.*) {
                .string => |*buf| if (t.kind == .null_only or !s.opts.dictionaries) {
                    try buf.values.ensureTotalCapacity(pa, reserve);
                } else {
                    buf.coded = true;
                    try buf.codes.ensureTotalCapacity(pa, reserve);
                },
                inline else => |*buf| try buf.values.ensureTotalCapacity(pa, reserve),
            }
        }
        b.* = .{
            .cols = cols,
            .types = p.types,
            .pinned = p.pinned,
            .max_abs = try pa.alloc(u128, ncols),
            .coders = try pa.alloc(Coder, ncols),
        };
        @memset(b.max_abs, 0);
        @memset(b.coders, .{});
        agg.* = .{
            .arena = pa,
            .cols = cols,
            .schema_elems = @constCast(s.elems),
            .paths = @constCast(s.paths),
            .num_rows = 0,
            .captured = true,
        };
    }

    /// Append one field to column `col`; false when the value is not one of the column's type.
    /// `copy`: the text's bytes do not outlive the chunk (unescaped into scratch, or the reader's pages).
    inline fn add(
        b: *Builder,
        a: std.mem.Allocator,
        col: usize,
        text: csv_scan.Text,
        ascii: bool,
        nulls: []const []const u8,
        copy: bool,
    ) !bool {
        const t = b.types[col];
        const buf = &b.cols[col];
        if (isNull(text, nulls)) {
            switch (buf.*) {
                inline else => |*x| {
                    if (x.max_def == 0) {
                        x.max_def = 1;
                        try x.def_levels.ensureTotalCapacity(a, @max(x.values.capacity, x.codes.capacity));
                        x.def_levels.appendNTimesAssumeCapacity(1, b.rows);
                    }
                    try x.def_levels.append(a, 0);
                    if (x.coded) {
                        try x.codes.append(a, 0);
                    } else try x.values.append(a, std.mem.zeroes(@TypeOf(x.values.items[0])));
                },
            }
            return true;
        }
        const s = text.bytes;
        switch (buf.*) {
            .i64 => |*x| {
                const v: i64 = switch (t.kind) {
                    .int64 => csv_types.parseInt(s) orelse return false,
                    .decimal => blk: {
                        const d = csv_types.parseDecimal(s, t.scale, decimalCap(t, b.pinned[col])) orelse return false;
                        b.max_abs[col] = @max(b.max_abs[col], @abs(d));
                        break :blk @intCast(d);
                    },
                    .timestamp => csv_types.timestampValue(t, s) orelse return false,
                    else => unreachable,
                };
                try x.values.append(a, v);
                if (x.max_def > 0) try x.def_levels.append(a, 1);
            },
            .i128 => |*x| {
                const d = csv_types.parseDecimal(s, t.scale, decimalCap(t, b.pinned[col])) orelse return false;
                b.max_abs[col] = @max(b.max_abs[col], @abs(d));
                try x.values.append(a, d);
                if (x.max_def > 0) try x.def_levels.append(a, 1);
            },
            .i32 => |*x| {
                try x.values.append(a, csv_types.parseDate(s) orelse return false);
                if (x.max_def > 0) try x.def_levels.append(a, 1);
            },
            .f64 => |*x| {
                try x.values.append(a, csv_types.parseFloat(s) orelse return false);
                if (x.max_def > 0) try x.def_levels.append(a, 1);
            },
            .boolean => |*x| {
                try x.values.append(a, csv_types.parseBool(s) orelse return false);
                if (x.max_def > 0) try x.def_levels.append(a, 1);
            },
            .string => |*x| {
                if (t.kind == .string and !ascii and !std.unicode.utf8ValidateSlice(s)) return false;
                if (t.kind == .null_only) return false;
                if (x.coded) {
                    const c = &b.coders[col];
                    // A one-byte value's code is its byte's slot, with no hash (flags and codes, as TPC-H's are).
                    const code = if (s.len == 1) one: {
                        const table = c.one orelse made: {
                            const fresh = try a.create([256]u32);
                            fresh.* = @splat(0);
                            c.one = fresh;
                            break :made fresh;
                        };
                        const slot = &table[s[0]];
                        if (slot.* == 0) slot.* = 1 + try c.insert(a, s, copy);
                        break :one slot.* - 1;
                    } else map: {
                        const gop = try c.map.getOrPut(a, s);
                        if (!gop.found_existing) {
                            gop.value_ptr.* = try c.insert(a, s, copy);
                            gop.key_ptr.* = c.dict.items[gop.value_ptr.*];
                        }
                        break :map gop.value_ptr.*;
                    };
                    try x.codes.append(a, code);
                    if (x.max_def > 0) try x.def_levels.append(a, 1);
                    c.present += 1;
                    c.plain += 4 + s.len;
                    if (c.bytes > encoder.dictPageMaxBytes() or (c.present >= c.next_judged and c.judge()))
                        try uncode(a, x, c);
                    return true;
                }
                // Every other string stays a slice of the input, which outlives the write.
                const owned = if (copy) try a.dupe(u8, s) else s;
                try x.values.append(a, owned);
                if (x.max_def > 0) try x.def_levels.append(a, 1);
            },
            .f32 => unreachable,
        }
        return true;
    }

    /// A coded column's rows so far as values, and values from here on.
    fn uncode(a: std.mem.Allocator, x: *consumer.OutputAggregator.TypedBuf([]const u8), c: *Coder) !void {
        try x.values.ensureTotalCapacity(a, x.codes.capacity);
        for (x.codes.items, 0..) |k, r| {
            const present = x.max_def == 0 or x.def_levels.items[r] == x.max_def;
            x.values.appendAssumeCapacity(if (present) c.dict.items[k] else "");
        }
        x.coded = false;
        x.codes = .empty;
        c.* = .{};
    }

    fn finish(b: *Builder, pa: std.mem.Allocator, agg: *consumer.OutputAggregator, s: *Source, i: usize) !void {
        for (b.cols, b.coders) |*col, *c| switch (col.*) {
            .string => |*x| if (x.coded) {
                // No value at all (every row NULL, or none): plain, so no code points past an empty dictionary.
                if (c.dict.items.len == 0) try uncode(pa, x, c) else x.dict = c.dict.items;
            },
            else => {},
        };
        agg.num_rows = b.rows;
        @memcpy(s.max_abs[i * b.cols.len ..][0..b.cols.len], b.max_abs);
        _ = s.rows.fetchAdd(b.rows, .monotonic);
    }
};

/// The integer digits a DECIMAL column takes: a pinned one its declared precision's, an inferred one all its physical
/// type holds, its precision then set from the data.
fn decimalCap(t: Type, pinned: bool) u8 {
    return if (pinned) t.int_digits else csv_types.decimalIntCapacity(t);
}

/// A DECIMAL column's precision in the footer and the report: the declared one when pinned, else the data's.
pub fn decimalPrecisionOf(t: Type, pinned: bool, max_abs: u128) i32 {
    return if (pinned) @intCast(t.precision()) else csv_types.decimalPrecision(t, max_abs);
}

fn columnLane(t: Type) std.meta.Tag(consumer.OutputAggregator.ColumnBuf) {
    return switch (t.kind) {
        .int64, .timestamp => .i64,
        .decimal => if (t.decimalInt64()) .i64 else .i128,
        .float64 => .f64,
        .date => .i32,
        .boolean => .boolean,
        .string, .binary, .null_only => .string,
    };
}

// ---------------------------------------------------------------- the write

/// Chunk size: small enough that a window of chunks per worker stays well inside the budget, large enough that
/// per-chunk work (a quote count, a record search, a piece) is noise. Never changes the output's bytes.
pub fn chunkBytes(opts: Options, args: QueryArgs, workers: usize, file_bytes: usize) usize {
    if (opts.chunk_bytes) |c| return @max(c, 1);
    const per_worker = args.max_memory / 16 / @max(1, workers);
    return std.math.clamp(@min(per_worker, file_bytes / @max(1, workers) + 1), 256 << 10, 4 << 20);
}

/// Write the CSV file at `path` to `out_path` as Parquet. On a refusal, `failure` says where and why.
pub fn run(
    ctx: Context,
    args: QueryArgs,
    path: []const u8,
    out_path: []const u8,
    opts: Options,
    report: *Report,
    report_alloc: std.mem.Allocator,
) !WriteResult {
    const failure = &report.failure;
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    if (std.mem.startsWith(u8, path, "s3://")) return error.CsvRemoteInput;
    const t_open = nowMonoNs();
    const mapped = local_fs.mapFile(path) catch |e| switch (e) {
        error.EmptyFile => {
            failure.* = .{ .kind = .empty, .line = 1 };
            return error.CsvEmpty;
        },
        else => return e,
    };
    defer mapped.unmap();
    var workers = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env);
    var chunk = chunkBytes(opts, args, workers, mapped.bytes.len);
    // A filter or computed SELECT is bound to every field, then only the fields it reads are parsed.
    var all: ?Plan = null;
    // Per field, the type its sample gave, where a failed binding settled the file's types (`Projection.bind`).
    var sampled: ?[]const Type = null;
    var p = if (Projection.needed(args)) blk: {
        var every = opts;
        every.columns = null;
        all = try plan(arena, mapped.bytes, every, failure);
        const first = Projection.bind(arena, args, &all.?, null) catch |e| retry: {
            // The sample's types are provisional (a field NULL in the sample, or holding a later wider value): a query
            // that does not bind to them may bind to the file's. Every field's type is settled from the whole file, as
            // a restart settles it, and the query bound again; one that still does not bind is invalid for the file.
            // Strict mode keeps the sample's types, as it refuses a later value instead of widening.
            if (e == error.CsvQueryNotSupported or e == error.OutOfMemory or opts.strict) return e;
            const before = try arena.dupe(Type, all.?.types);
            const seen = try classifyFile(ctx, arena, &all.?, chunk, workers, failure);
            for (all.?.types, seen, all.?.pinned) |*t, f, pinned| if (!pinned) {
                t.* = t.join(f);
            };
            sampled = before;
            break :retry try Projection.bind(arena, args, &all.?, null);
        };
        var parsed = opts;
        parsed.columns = try first.fetchedNames(arena, all.?.fields);
        var fetched = try plan(arena, mapped.bytes, parsed, failure);
        // Settled types hold every value already: the parse takes them, not the sample's again.
        if (sampled != null) for (fetched.slot, 0..) |slot, ci| if (slot) |o| {
            fetched.types[o] = all.?.types[ci];
        };
        break :blk fetched;
    } else try plan(arena, mapped.bytes, opts, failure);
    // Everything the report holds is allocated before the output exists: once it is finished, nothing may fail.
    // The fields are named first, for a refusal's column.
    const fields = try report_alloc.alloc([]const u8, p.fields.len);
    for (fields, p.fields) |*d, n| d.* = try report_alloc.dupe(u8, n);
    report.fields = fields;
    const names = try report_alloc.alloc([]const u8, p.names.len);
    for (names, p.names) |*d, n| d.* = try report_alloc.dupe(u8, n);
    report.names = names;
    const widened = try report_alloc.alloc(bool, p.names.len);
    @memset(widened, false);
    if (sampled) |before| for (p.slot, 0..) |slot, ci| if (slot) |o| {
        widened[o] = !std.meta.eql(before[ci], p.types[o]);
    };
    report.widened = widened;
    const types = try report_alloc.alloc(Type, p.names.len);
    // A path, or the caller's own unpublished file, starts again from empty; a pipe may have passed bytes on.
    const restartable = args.output_fd == null or args.output_restartable;
    while (true) {
        // Each pass's per-chunk state is its own: a narrowed or widened pass frees the last one's.
        var pass_state = std.heap.ArenaAllocator.init(ctx.gpa);
        defer pass_state.deinit();
        const pass = pass_state.allocator();
        var source = try Source.init(pass, &p, chunk);
        // Bound again each pass: a widened column changes what its expressions are.
        var proj: Projection = undefined;
        if (all) |*every| {
            proj = try Projection.bind(pass, args, every, &p);
            source.proj = &proj;
        }
        report.chunks = source.count;
        source.opts = opts;
        var narrowed = args;
        narrowed.parallelism = workers;
        const result = writeOnce(ctx, narrowed, path, out_path, &source, mapped, report, t_open) catch |e| {
            if (source.firstFailure()) |f| failure.* = f.*;
            // Refused its memory: run again on a narrower path, half the workers and their chunks in flight, as a
            // query that does not fit does; one worker that still does not fit fails. The bytes do not change.
            if ((e == error.OutOfMemory or e == error.ExceededMemoryBudget) and workers > 1 and restartable) {
                if (args.output_fd) |fd| try local_fs.truncateToStart(fd);
                workers = @max(1, workers / 2);
                chunk = chunkBytes(opts, narrowed, workers, mapped.bytes.len);
                report.narrowed += 1;
                continue;
            }
            if (e != error.CsvTypeChanged or opts.strict or !restartable) return e;
            if (args.output_fd) |fd| try local_fs.truncateToStart(fd);
            // Widen to what every value holds, then start again.
            const seen = try classifyFile(ctx, pass, &p, chunk, workers, failure);
            for (p.types, seen, p.pinned, widened) |*t, s, pinned, *wd| if (!pinned) {
                const j = t.join(s);
                wd.* = wd.* or !std.meta.eql(j, t.*);
                t.* = j;
            };
            report.restarts += 1;
            if (report.restarts > 2) return e; // the join of the whole file holds every value; a third pass is a bug
            continue;
        };
        @memcpy(types, p.types);
        // A DECIMAL's precision is the footer's (`writeOnce`): the declared one, or the data's, never the sample's.
        for (types, p.pinned, 0..) |*t, pinned, c| if (t.kind == .decimal) {
            var max: u128 = 0;
            for (0..source.count) |i| max = @max(max, source.max_abs[i * p.names.len + c]);
            t.int_digits = @intCast(decimalPrecisionOf(t.*, pinned, max) - t.scale);
        };
        report.types = types;
        return result;
    }
}

/// Every chunk's types, joined: the quote parities first (each chunk alone), then every chunk's records.
pub fn classifyFile(
    ctx: Context,
    arena: std.mem.Allocator,
    p: *const Plan,
    chunk: usize,
    workers: usize,
    failure: *Failure,
) ![]Type {
    var source = try Source.init(arena, p, chunk);
    source.classify = true;
    source.seen = try arena.alloc(Type, source.count * p.names.len);
    var threaded: std.Io.Threaded = .init(ctx.gpa, .{ .async_limit = .limited(@max(1, workers)) });
    defer threaded.deinit();
    const io = threaded.io();
    // One task per worker, each claiming the next chunk in order (as `csv_agg` folds): each chunk counts its own
    // quotes as it starts (`records`), and none waits on another's dispatch.
    const Task = struct {
        fn worker(s: *Source, t_io: std.Io, cursor: *std.atomic.Value(usize)) void {
            while (true) {
                const i = cursor.fetchAdd(1, .monotonic);
                if (i >= s.count) return;
                s.produce(t_io, null, i) catch {};
            }
        }
    };
    var cursor: std.atomic.Value(usize) = .init(0);
    var group: std.Io.Group = .init;
    for (0..@min(@max(1, workers), source.count)) |_| group.async(io, Task.worker, .{ &source, io, &cursor });
    group.await(io) catch {};
    if (source.firstFailure()) |f| {
        failure.* = f.*;
        return switch (f.kind) {
            .field_count => error.CsvFieldCount,
            else => error.CsvMalformed,
        };
    }
    const ncols = p.names.len;
    const out = try arena.alloc(Type, ncols);
    @memset(out, .{});
    for (0..source.count) |i| for (out, source.seen[i * ncols ..][0..ncols]) |*o, s| {
        o.* = o.join(s);
    };
    return out;
}

fn resolveBlooms(
    arena: std.mem.Allocator,
    args: QueryArgs,
    names: []const []const u8,
    elems: []const schema.SchemaElement,
) ![]const ?f64 {
    if (args.bloom_filters.len == 0) return &.{};
    const out = try arena.alloc(?f64, names.len);
    @memset(out, null);
    for (args.bloom_filters) |spec| {
        const i = for (names, 0..) |n, i| {
            if (std.mem.eql(u8, n, spec.column)) break i;
        } else {
            Diag.setColumn(args.diag, spec.column);
            return error.UnknownColumn;
        };
        if (elems[i].type == .BOOLEAN or elems[i].type == .FIXED_LEN_BYTE_ARRAY) {
            Diag.setColumn(args.diag, spec.column);
            return error.BloomFilterUnsupportedColumn;
        }
        out[i] = spec.fpp;
    }
    return out;
}

/// What a chunk's read takes past its end: the record straddling the next chunk's first byte, in all but rare files.
const overhang_bytes: usize = 256 << 10;

/// Read the file through the local reader when `auto` (or `ZPQ_IO_PROFILE`) says so: a cold file into a buffer laid
/// out like it, chunk by chunk ahead of the producers, each chunk's pages drawn from the write's ledger and given back
/// once its piece is freed. A resident file stays on the mapping. The mapping's descriptor is read, so the file is the
/// one sniffed.
pub fn startReader(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    source: *Source,
    mapped: local_fs.Mapped,
    ledger: *core_memory.SharedBudget,
    report: *Report,
) !?*local_reader.Reader {
    if (@import("builtin").os.tag != .linux) return null;
    var policy = local_reader.Policy.fromEnv(ctx.env);
    policy.cache = args.cache;
    if (source.opts.io_profile) |forced| policy.profile = forced;
    const p = source.plan;
    const fd = mapped.fd orelse return null;
    const whole = [_]local_reader.Range{.{ .start = p.data_start, .end = p.bytes.len }};
    const resident = if (policy.profile == .auto) local_reader.sampleRanges(ctx.gpa, mapped.bytes, &whole).share else 0;
    const choice = policy.choose(resident, 1.0);
    report.io = choice;
    if (choice.mode == .mmap) return null;
    const rd = try local_reader.Reader.create(ctx.gpa, .{
        .mode = choice.mode,
        .threads = policy.threads,
        // An eighth of the budget, beside the pieces in flight and the row groups encoding: chunks are small and read
        // in order, so a short window keeps the device busy.
        .window = policy.window orelse @max(8 << 20, args.max_memory / 8),
        .ledger = ledger,
        .direct = choice.directRead(),
        .device_admission = true,
    });
    errdefer rd.destroy();
    const f = try rd.borrowFile(fd, mapped.bytes.len);
    f.name = args.inputs[0];
    source.overhang = source.opts.overhang orelse overhang_bytes;
    source.gate = f.gate();
    source.buf = f.bytes();
    const ranges = try arena.alloc(?[]local_reader.Range, source.count);
    for (ranges, 0..) |*r, i| {
        const one = try arena.alloc(local_reader.Range, 1);
        one[0] = .{ .start = source.bound(i), .end = source.readEnd(i) };
        r.* = one;
    }
    try rd.planFile(f, ranges, 0);
    try rd.start();
    return rd;
}

fn writeOnce(
    ctx: Context,
    args: QueryArgs,
    path: []const u8,
    out_path: []const u8,
    source: *Source,
    mapped: local_fs.Mapped,
    report: *Report,
    t_open: i64,
) !WriteResult {
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var t: engine_types.Timings = .{};
    t.read_ns = @intCast(nowMonoNs() - t_open);
    const p = source.plan;
    const out_elems: []const schema.SchemaElement = if (source.proj) |pr| pr.out_elems else source.elems;
    const out_paths = if (source.proj) |pr| pr.out_paths else source.paths;
    const out_names = try arena.alloc([]const u8, out_elems.len);
    for (out_names, out_elems) |*n, e| n.* = e.name;
    const blooms = try resolveBlooms(arena, args, out_names, out_elems);

    var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    defer if (args.profiler) |pr| pr.noteLedger(ledger.peak.load(.monotonic), args.max_memory);
    // Per-chunk state, the source's and the window's slots, grows with the file, not the budget (a wide file of
    // many chunks holds megabytes of it): it is the write's too, charged before anything else draws.
    const per_chunk = source.metaBytes() +| source.count *| @sizeOf(window_mod.Slot);
    _ = ledger.drawExact(per_chunk) orelse return error.ExceededMemoryBudget;
    defer ledger.release(per_chunk);
    // Torn down (its threads joined, its buffer unmapped) once every piece reading it has been written.
    const reader = try startReader(ctx, arena, args, source, mapped, &ledger, report);
    defer if (reader) |rd| rd.destroy();
    const priority = args.compress_priority;
    var zstd_counts: compression.ZstdModeCounts = .{};
    const zstd: compression.ZstdPlan = .{ .lambda = priority.lambda(), .counts = &zstd_counts };

    var out: write_mod.Output = .{ .path = out_path };
    defer out.deinit();
    try out.open(ctx, arena, args, .{});
    t.output_started_ns = nowMonoNs();
    var offset: u64 = 0;
    try out.sink.write("PAR1");
    offset += 4;

    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    var page_indexes: page_index.Deferred = .{};
    const sizing = window_mod.writeWindowFor(ctx, args, source.chunk *| 3, source.count);
    const slots = try arena.alloc(window_mod.Slot, source.count);
    for (slots) |*s| s.* = .{ .meta_arena = std.heap.ArenaAllocator.init(ctx.gpa) };
    defer for (slots) |*s| s.meta_arena.deinit();
    var memory: core_memory.Charged = .{ .child = ctx.gpa, .budget = &ledger };
    var slicing: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = &ledger };
    var lean_ledger: core_memory.SharedBudget = .{ .limit = core_memory.lean_decode };
    var lean: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = &lean_ledger };
    const regions = try arena.alloc(window_mod.WorkRegions, @max(sizing.workers, 1) + sizing.budget + 1);
    for (regions) |*r| {
        r.* = .init(ctx.gpa);
        r.rows.budget = &ledger;
        r.work.budget = &ledger;
        r.rows.allowance = huge_pages.huge_page_size;
    }
    defer for (regions) |*r| r.deinit();
    var window_sem: std.Io.Semaphore = .{ .permits = sizing.units };
    var completions_sem: std.Io.Semaphore = .{ .permits = 0 };
    var w = window_mod.WinCtx{
        .worker_io = ctx.worker_io,
        // A CSV write holds its row groups' rows and their encodings and nothing else, so they are cut under the
        // budget whether or not it was given: a sixteenth of it, as an explicit --max-memory caps a re-encode's.
        // At the default budget of a host with 1 GiB or more to give, that is the 64 MiB target, unchanged.
        .rg_target = args.row_group.under(args.max_memory),
        .out_elems = @constCast(out_elems),
        .out_paths = @constCast(out_paths),
        .page_indexes = &page_indexes,
        .memory = &memory,
        .slicing = &slicing,
        .lean = &lean,
        .win_unit = sizing.unit,
        .win_units = sizing.units,
        .gpa = ctx.gpa,
        .jobs = &.{},
        .filter = null,
        .fetch_arr = &.{},
        .output_specs = &.{},
        .kept_set = null,
        .codec = args.codec orelse .SNAPPY,
        .zstd = zstd,
        .parquet_version = args.parquet_version,
        .decode_options = .{},
        .bloom_outputs = blooms,
        .dictionary_bloom = args.bloom_filters_enabled,
        .slots = slots,
        .regions = regions,
        .window = &window_sem,
        .completions = &completions_sem,
        .sink = out.sink,
        .out_offset = offset,
        .new_row_groups = &new_row_groups,
        .arena = arena,
        .profiler = args.profiler,
        .diag = args.diag,
        .source = source.pieceSource(),
    };
    w.cpu_budget = sizing.budget;
    // Idle pieces kept for reuse are on the ledger too: a sixteenth of a small budget, not a fixed 32 MiB of it.
    w.piece_pages = w.outAlloc();
    w.piece_pool = .{ .gpa = ctx.gpa, .rows_alloc = w.pieceAlloc(), .ledger = &ledger, .limit = args.max_memory / 16 };
    defer w.piece_pool.deinit();
    const t_scan = nowMonoNs();
    // A draw the ledger refused is the budget's refusal, not the host's: named so, as a Parquet write names it.
    window_mod.runWindowedReencode(&w, sizing.workers, ctx.env) catch |e| {
        if (reader) |rd| rd.reportFailure(e, args.diag);
        return slicing.refusal(memory.refusal(e));
    };
    t.decode_wall_ns = @intCast(nowMonoNs() - t_scan);
    t.core.sink_ns += w.sink_ns;
    t.core.encode_ns += w.core.encode_ns;
    offset = w.out_offset;

    // Page indexes and Bloom filters after the row groups, then the footer: DECIMAL precision from the data, every
    // bound in TYPE_DEFINED_ORDER.
    const t_footer = nowMonoNs();
    var so: streaming.SinkOut = .{ .sink = out.sink, .offset = @intCast(offset) };
    try page_indexes.write(arena, &so, new_row_groups.items);
    try fastpath.placeFilters(&so, new_row_groups.items);
    const ncols = p.names.len;
    const nout = out_elems.len;
    var items: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try items.append(arena, .{
        .name = "schema",
        .type = null,
        .repetition_type = null,
        .type_length = null,
        .num_children = @intCast(nout),
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    for (out_elems, 0..) |e, o| {
        var elem = e;
        // A column written as parsed (or passed through a projection) takes its DECIMAL precision from the data; a
        // computed one is as its expression typed it.
        const parsed: ?usize = if (source.proj) |pr| pr.source[o] else o;
        const c = parsed orelse {
            try items.append(arena, elem);
            continue;
        };
        const ty = p.types[c];
        const pinned = p.pinned[c];
        if (ty.kind == .decimal) {
            var max: u128 = 0;
            for (0..source.count) |i| max = @max(max, source.max_abs[i * ncols + c]);
            const prec = decimalPrecisionOf(ty, pinned, max);
            elem.precision = prec;
            elem.logical_type = .{ .DECIMAL = .{ .scale = ty.scale, .precision = prec } };
        }
        try items.append(arena, elem);
    }
    const meta: schema.FileMetaData = .{
        .version = args.parquet_version.footer(),
        .schema = items,
        .num_rows = @intCast(w.rows_kept),
        .created_by = schema.ZPQ_CREATED_BY,
        .row_groups = new_row_groups,
        .column_orders = try schema.FileMetaData.outputColumnOrders(arena, nout, null, &.{}),
    };
    var tw: thrift.Writer = .init(arena);
    try meta.write(&tw);
    try so.write(tw.bytes());
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, std.math.cast(u32, tw.bytes().len) orelse return error.TooLarge, .little);
    try so.write(&len);
    try so.write("PAR1");
    t.footer_ns = @intCast(nowMonoNs() - t_footer);
    // The file read must be the file planned, checked while the output can still be removed (or its upload
    // aborted): `finish` publishes it, and nothing after it fails.
    try mapped.intact(path);
    if (try out.finish()) |mp| {
        t.mp_complete_ns = mp.complete_ns;
        t.mp_await_ns = mp.await_ns;
    }
    const rows: i64 = @intCast(source.rows.load(.monotonic));
    return .{
        .codec = args.codec orelse .SNAPPY,
        .files_in = 1,
        .rows_in = rows,
        .rows_scanned = rows,
        .rows_matched = w.rows_kept,
        .bytes_in = p.bytes.len,
        .bytes_out = so.offset,
        .row_groups_in = 0,
        .row_groups_kept = w.rgs_kept,
        .timings = t,
    };
}

// ---------------------------------------------------------------- the query surface

pub const InputFormat = engine_types.InputFormat;

/// What `path` is: Parquet when `PAR1` both starts and ends it, else CSV when it is named `.csv`, `.tsv` or `.txt`
/// (any case) or `forced` says so. Anything else is Parquet, so the Parquet reader refuses it as it always has
/// (`NotParquet`, with its cause). An S3 path is Parquet unless forced to CSV.
pub fn inputFormat(path: []const u8, forced: InputFormat) !InputFormat {
    if (forced != .auto) return forced;
    if (std.mem.startsWith(u8, path, "s3://")) return .parquet;
    const ext = std.fs.path.extension(path);
    // Named so, it is read as Parquet and refused as Parquet if it is not: no open for the common case.
    if (std.ascii.eqlIgnoreCase(ext, ".parquet")) return .parquet;
    if (try hasParquetMagic(path)) return .parquet;
    for ([_][]const u8{ ".csv", ".tsv", ".txt" }) |e| if (std.ascii.eqlIgnoreCase(ext, e)) return .csv;
    return .parquet;
}

/// `PAR1` at both ends. A file that cannot be opened is left to the Parquet path to report as it does.
fn hasParquetMagic(path: []const u8) !bool {
    const fd = local_fs.openRead(path) catch return true;
    defer local_fs.close(fd);
    const size = local_fs.fileSize(fd) catch return true;
    if (size < 8) return size == 0; // an empty file is reported by the Parquet path (or by name as CSV below)
    var head: [4]u8 = undefined;
    var tail: [4]u8 = undefined;
    const linux = std.os.linux;
    if (linux.pread(fd, &head, 4, 0) != 4 or linux.pread(fd, &tail, 4, @intCast(size - 4)) != 4) return true;
    return std.mem.eql(u8, &head, "PAR1") and std.mem.eql(u8, &tail, "PAR1");
}

pub const QueryError = error{ CsvQueryNotSupported, CsvOneInput, CsvMixedInputs };

/// The CSV input of a query, or null when its inputs are Parquet. A CSV input is converted whole to Parquet
/// (`zpq query data.csv -o data.parquet`): one file, no filter, selection or aggregate.
pub fn queryInput(args: QueryArgs) !?[]const u8 {
    const path = (try csvInput(args)) orelse return null;
    // Written whole, by column, or filtered and projected row by row (`Projection`). An aggregate folds during the
    // parse (`csv_agg`); anything else that needs more than one row at a time (a sort, a cut) is refused.
    if (args.output == null or args.aggregate != null or args.group_by != null or
        args.column_order != null or args.result_order.keys.len > 0 or args.result_order.rows.len > 0 or
        args.result_order.limit != null or args.result_order.offset != 0)
        return error.CsvQueryNotSupported;
    if (args.columns != null and args.select != null) return error.CsvQueryNotSupported;
    if (args.plan_source) |ps| {
        // SQL: the statement's own syntax, a SELECT list with or without a WHERE.
        const q = syntax_plan.syntaxOf(ps) orelse return error.CsvQueryNotSupported;
        if (q.aggregate or q.calls.len > 0 or q.join != null or q.keys != null or q.row_keys.len > 0 or q.hidden.len > 0)
            return error.CsvQueryNotSupported;
    }
    return path;
}

/// The CSV input of a query, or null when its inputs are Parquet: one file, never beside another input.
pub fn csvInput(args: QueryArgs) !?[]const u8 {
    if (args.inputs.len == 0) return null;
    var csv: ?[]const u8 = null;
    for (args.inputs) |in| {
        const f = inputFormat(in, args.input_format) catch |e| {
            Diag.setInput(args.diag, in, null);
            return e;
        };
        if (f == .csv) {
            if (csv != null) return error.CsvOneInput;
            csv = in;
        }
    }
    const path = csv orelse return null;
    if (args.inputs.len > 1) return error.CsvMixedInputs;
    return path;
}

/// The columns a CSV query writes: `--columns`, or a SELECT list of plain column names (quoted or not, each named as
/// itself); null: every column. Anything else in the list is a query over the CSV, which is refused.
pub fn queryColumns(a: std.mem.Allocator, args: QueryArgs) !?[]const []const u8 {
    if (args.columns) |cols| {
        if (args.select != null) return error.CsvQueryNotSupported;
        return cols;
    }
    if (args.plan_source) |ps| {
        const items = (syntax_plan.syntaxOf(ps) orelse return error.CsvQueryNotSupported).select orelse return null;
        const out = try a.alloc([]const u8, items.len);
        for (items, out) |item, *o| o.* = itemName(item) orelse return error.CsvQueryNotSupported;
        return out;
    }
    const sel = args.select_cols orelse &.{};
    if (sel.len == 0) return if (args.select != null) error.CsvQueryNotSupported else null;
    const out = try a.alloc([]const u8, sel.len);
    for (sel, out) |c, *o| o.* = selectName(c) orelse return error.CsvQueryNotSupported;
    return out;
}

/// A SELECT item that names one column as itself, and the name; null for an expression or a rename.
fn selectName(c: expr_ast.SelectColumn) ?[]const u8 {
    if (c.role != .named) return null;
    const name = plainName(std.mem.trim(u8, c.expr, " ")) orelse return null;
    if (c.alias) |al| if (!std.mem.eql(u8, al, name)) return null;
    return name;
}

/// A SQL SELECT item that is a column named as itself; null for an expression or a rename.
fn itemName(item: syntax.SelectItem) ?[]const u8 {
    const name = switch (item.expr) {
        .column => |c| plainName(c) orelse return null, // as written: a quoted name keeps its quotes
        else => return null,
    };
    if (item.alias) |al| if (!std.mem.eql(u8, al, name)) return null;
    return name;
}

/// `name` or `"name"` (no doubled quotes inside): the column it names; null for any other expression.
fn plainName(e: []const u8) ?[]const u8 {
    if (e.len >= 2 and e[0] == '"' and e[e.len - 1] == '"') {
        const inner = e[1 .. e.len - 1];
        return if (inner.len == 0 or std.mem.indexOfScalar(u8, inner, '"') != null) null else inner;
    }
    if (e.len == 0 or !(std.ascii.isAlphabetic(e[0]) or e[0] == '_')) return null;
    for (e) |ch| if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) return null;
    return e;
}

/// The write a query asks of its CSV input, its report on `args.csv_report` and its refusal on `args.diag`.
pub fn writeQuery(ctx: Context, args: QueryArgs, path: []const u8, out_path: []const u8) !WriteResult {
    var local: Report = .{};
    const report = args.csv_report orelse &local;
    var scratch = std.heap.ArenaAllocator.init(ctx.gpa);
    defer scratch.deinit();
    const alloc = args.csv_report_alloc orelse scratch.allocator();
    var opts = args.csv;
    // A projection picks its own columns (`run`); otherwise the columns named are the ones written.
    opts.columns = if (Projection.needed(args)) null else try queryColumns(scratch.allocator(), args);
    if (args.diag) |d| d.execution_started = true;
    return run(ctx, args, path, out_path, opts, report, alloc) catch |e| {
        if (args.diag) |d| describe(d, args, path, e, report);
        return e;
    };
}

/// A refusal's place and reason, for the frontend's message.
pub fn describe(d: *Diag, args: QueryArgs, path: []const u8, err: anyerror, report: *const Report) void {
    const f = &report.failure;
    d.input = path;
    const reason: []const u8 = switch (err) {
        error.CsvMalformed => f.malformed.describe(),
        error.CsvFieldCount => "field count",
        error.CsvTypeChanged => "type changed",
        error.CsvPinnedType => "pinned type",
        error.UnknownColumn => {
            d.column.set(f.valueText());
            return;
        },
        else => return,
    };
    d.detail = .{ .csv = .{
        .reason = reason,
        .line = f.line,
        .byte = f.offset,
        .expected = f.expected_fields,
        .got = f.got_fields,
        .has_column = f.column != null,
        .strict = args.csv.strict,
    } };
    if (f.column) |c| {
        var nb: [32]u8 = undefined;
        // Refused while sampling, before the header named the columns: by position, as `column0` names one.
        d.column.set(if (c < report.fields.len) report.fields[c] else std.fmt.bufPrint(&nb, "column{d}", .{c}) catch "");
    }
    d.detail.csv.value.set(f.valueText());
    var buf: [64]u8 = undefined;
    d.detail.csv.type_name.set(f.type.name(&buf));
}
