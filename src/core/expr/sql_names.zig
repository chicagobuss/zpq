//! The name DuckDB gives a select item that has no alias: the expression printed back, every binary operation in
//! parentheses (`(n + 1)`), a function's name lower-cased, and any name that is a keyword in double quotes
//! (`"year"(d)`). Only the forms whose printing is known exactly are named; for the rest the caller asks for an alias.
const std = @import("std");
const c = @import("liteparser_c");
const keywords = @import("duck_keywords.zig");

pub const Error = error{ OutOfMemory, Unnameable };

const max_depth = 64;

/// The name of the select item `node`, or `error.Unnameable` where it is not printed exactly.
pub fn of(allocator: std.mem.Allocator, node: *const c.LpNode) Error![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try print(allocator, &out, node, 0);
    return out.toOwnedSlice(allocator);
}

fn print(allocator: std.mem.Allocator, out: *std.ArrayList(u8), node: *const c.LpNode, depth: u32) Error!void {
    if (depth > max_depth) return error.Unnameable;
    switch (node.kind) {
        c.LP_RESULT_COLUMN => try print(allocator, out, node.u.result_column.expr orelse return error.Unnameable, depth),
        c.LP_EXPR_COLUMN_REF => {
            const ref = node.u.column_ref;
            // A qualified name is a path DuckDB prints another way.
            if (ref.schema != null or ref.table != null) return error.Unnameable;
            const name = std.mem.span(ref.column orelse return error.Unnameable);
            if (std.mem.indexOfScalar(u8, name, '.') != null) return error.Unnameable;
            try writeName(allocator, out, name);
        },
        c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT => try number(allocator, out, node, false),
        c.LP_EXPR_LITERAL_STRING => {
            const text = std.mem.span(node.u.literal.value);
            try out.append(allocator, '\'');
            for (text) |ch| {
                if (ch == '\'') try out.append(allocator, '\'');
                try out.append(allocator, ch);
            }
            try out.append(allocator, '\'');
        },
        c.LP_EXPR_LITERAL_NULL => try out.appendSlice(allocator, "NULL"),
        c.LP_EXPR_UNARY_OP => {
            const operand = node.u.unary.operand orelse return error.Unnameable;
            // A sign on a sign is folded into the number by DuckDB's parser, which this does not follow.
            if (operand.*.kind == c.LP_EXPR_UNARY_OP) return error.Unnameable;
            switch (node.u.unary.op) {
                // A sign next to a number is part of it, as DuckDB's parser reads it.
                c.LP_UOP_MINUS => if (operand.*.kind == c.LP_EXPR_LITERAL_INT or operand.*.kind == c.LP_EXPR_LITERAL_FLOAT) {
                    try number(allocator, out, operand, true);
                } else {
                    try out.appendSlice(allocator, "(-(");
                    try print(allocator, out, operand, depth + 1);
                    try out.appendSlice(allocator, "))");
                },
                c.LP_UOP_PLUS => {
                    try out.appendSlice(allocator, "(+(");
                    try print(allocator, out, operand, depth + 1);
                    try out.appendSlice(allocator, "))");
                },
                else => return error.Unnameable,
            }
        },
        c.LP_EXPR_BINARY_OP => {
            const b = node.u.binary;
            if (b.escape != null) return error.Unnameable;
            const op: []const u8 = switch (b.op) {
                c.LP_OP_ADD => "+",
                c.LP_OP_SUB => "-",
                c.LP_OP_MUL => "*",
                c.LP_OP_DIV => "/",
                c.LP_OP_MOD => "%",
                c.LP_OP_CONCAT => "||",
                else => return error.Unnameable,
            };
            try out.append(allocator, '(');
            try print(allocator, out, b.left orelse return error.Unnameable, depth + 1);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, op);
            try out.append(allocator, ' ');
            try print(allocator, out, b.right orelse return error.Unnameable, depth + 1);
            try out.append(allocator, ')');
        },
        c.LP_EXPR_FUNCTION => {
            const f = node.u.function;
            if (f.over != null or f.filter != null or f.distinct != 0 or f.is_ctime_kw != 0 or f.order_by.count != 0)
                return error.Unnameable;
            const written = std.mem.span(f.name);
            for (written) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_') return error.Unnameable;
            if (written.len == 0) return error.Unnameable;
            var lower: [64]u8 = undefined;
            if (written.len > lower.len) return error.Unnameable;
            const name = std.ascii.lowerString(&lower, written);
            // DuckDB reads these as the one operation, which it prints in capitals and never quotes.
            if (std.mem.eql(u8, name, "coalesce") or std.mem.eql(u8, name, "ifnull")) {
                try out.appendSlice(allocator, "COALESCE");
            } else if (!plainCall(name)) {
                return error.Unnameable;
            } else try writeName(allocator, out, name);
            try out.append(allocator, '(');
            for (0..@intCast(f.args.count)) |i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try print(allocator, out, f.args.items[i] orelse return error.Unnameable, depth + 1);
            }
            try out.append(allocator, ')');
        },
        c.LP_EXPR_CAST => {
            const written = std.mem.span(node.u.cast.type_name);
            const target: []const u8 = switch (castTarget(written)) {
                .fixed => |name| name,
                .as_written => written,
                .unknown => return error.Unnameable,
            };
            try out.appendSlice(allocator, "CAST(");
            try print(allocator, out, node.u.cast.expr orelse return error.Unnameable, depth + 1);
            try out.appendSlice(allocator, " AS ");
            try out.appendSlice(allocator, target);
            try out.append(allocator, ')');
        },
        else => return error.Unnameable,
    }
}

/// A number as it was written, behind a minus where `negated`: a token of digits, one point and an exponent.
fn number(allocator: std.mem.Allocator, out: *std.ArrayList(u8), node: *const c.LpNode, negated: bool) Error!void {
    const token = std.mem.span(node.u.literal.value);
    if (token.len == 0) return error.Unnameable;
    for (token) |ch| if (!std.ascii.isDigit(ch) and ch != '.' and ch != 'e' and ch != 'E' and ch != '+' and ch != '-')
        return error.Unnameable;
    if (token[0] == '-' or token[0] == '+') return error.Unnameable;
    if (negated) try out.append(allocator, '-');
    try out.appendSlice(allocator, token);
}

/// `name` as DuckDB writes a name: bare unless it is empty, starts with a digit, holds anything but letters, digits and
/// underscores, or is a keyword, and then in double quotes with any inside doubled.
fn writeName(allocator: std.mem.Allocator, out: *std.ArrayList(u8), name: []const u8) Error!void {
    var quote = name.len == 0 or std.ascii.isDigit(name[0]) or keywords.isKeyword(name);
    for (name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_') {
        quote = true;
    };
    if (!quote) return out.appendSlice(allocator, name);
    try out.append(allocator, '"');
    for (name) |ch| {
        if (ch == '"') try out.append(allocator, '"');
        try out.append(allocator, ch);
    }
    try out.append(allocator, '"');
}

/// The functions DuckDB prints as `name(args)` from the name as written: those zpq has, checked against DuckDB's
/// own names. A function it parses into another form (`EXTRACT`, `POSITION`) is rewritten before this sees it.
fn plainCall(name: []const u8) bool {
    return known.has(name);
}

const known = std.StaticStringMap(void).initComptime(.{
    .{"abs"}, .{"ascii"}, .{"atan2"}, .{"cbrt"}, .{"ceil"}, .{"ceiling"}, .{"chr"}, .{"concat"}, .{"concat_ws"},
    .{"contains"}, .{"date_diff"}, .{"date_part"}, .{"date_trunc"}, .{"datediff"}, .{"datepart"}, .{"day"}, .{"dayname"},
    .{"dayofmonth"}, .{"dayofweek"}, .{"dayofyear"}, .{"degrees"}, .{"ends_with"}, .{"epoch"}, .{"epoch_ms"},
    .{"epoch_ns"}, .{"epoch_us"}, .{"floor"}, .{"greatest"}, .{"hour"}, .{"instr"}, .{"isodow"}, .{"last_day"},
    .{"lcase"}, .{"least"}, .{"left"}, .{"length"}, .{"lower"}, .{"lpad"}, .{"ltrim"}, .{"make_date"}, .{"minute"},
    .{"mod"}, .{"month"}, .{"monthname"}, .{"nullif"}, .{"prefix"}, .{"quarter"}, .{"radians"}, .{"regexp_extract"},
    .{"regexp_matches"}, .{"regexp_replace"}, .{"repeat"}, .{"replace"}, .{"right"}, .{"round"}, .{"rpad"}, .{"rtrim"},
    .{"second"}, .{"sign"}, .{"sinh"}, .{"split_part"}, .{"sqrt"}, .{"starts_with"}, .{"strftime"}, .{"strlen"},
    .{"strpos"}, .{"substr"}, .{"substring"}, .{"suffix"}, .{"trim"}, .{"trunc"}, .{"ucase"}, .{"upper"}, .{"week"},
    .{"weekofyear"}, .{"year"},
});

const Spelling = union(enum) {
    /// DuckDB's own spelling of a type its grammar has a word for.
    fixed: []const u8,
    /// A type it prints as it was written: `double`, `date`, `tinyint`, `text` and `string` keep the case they were typed in.
    as_written,
    /// A size or precision, or a type with no known printing.
    unknown,
};

fn castTarget(written: []const u8) Spelling {
    const fixed = [_]struct { []const u8, []const u8 }{
        .{ "bigint", "BIGINT" },   .{ "integer", "INTEGER" }, .{ "int", "INTEGER" },        .{ "smallint", "SMALLINT" },
        .{ "float", "FLOAT" },     .{ "real", "FLOAT" },      .{ "varchar", "VARCHAR" },    .{ "timestamp", "TIMESTAMP" },
    };
    for (fixed) |entry| if (std.ascii.eqlIgnoreCase(written, entry[0])) return .{ .fixed = entry[1] };
    const as_typed = [_][]const u8{ "double", "date", "tinyint", "text", "string" };
    for (as_typed) |name| if (std.ascii.eqlIgnoreCase(written, name)) return .as_written;
    return .unknown;
}
