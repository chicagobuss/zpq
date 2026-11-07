//! Expression AST.
//!
//! Tagged-union shape — small enough that the evaluator can recurse
//! through it directly without a comptime VM. First-slice surface:
//!
//!   expr  := literal | col_ref | (expr op expr) | (expr)
//!   op    := + | - | * | /
//!
//! Plus an `Alias` shell at the top level so `SELECT expr AS name`
//! survives parse → eval → encode without a separate alias map.
//! Strings, booleans, NULL, and function calls are not supported in
//! this slice — they land in C2 follow-ups.
//!
//! Type system (resolved at parse time):
//!   - INT32 / INT64 / FLOAT / DOUBLE column refs are accepted in
//!     arithmetic. BYTE_ARRAY / BOOLEAN are not.
//!   - Promotion: any-int op any-int → i64, anything-with-float → f64.
//!   - Result types of computed columns: INT64 or DOUBLE only.
//!
//! Compared to filter/ast.zig: this one doesn't need a `negate()`
//! method (no boolean composition) but does need `result_type` so the
//! caller can synthesize a schema element for the output column.

const std = @import("std");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const regex = @import("regex.zig");

/// Binary operators. Numeric ops (`add`/`sub`/`mul`/`div`) work on
/// `i64` or `f64`; `concat` works on `str` and is the SQL `||`
/// operator.
pub const Op = enum {
    add,
    sub,
    mul,
    div,
    concat,
};

/// The value type an expression evaluates to. Three lanes — i64, f64,
/// str — keep the kernel matrix tractable. Numeric promotion happens
/// at column-ref load time (i32 → i64, f32 → f64); strings stay as
/// strings and don't mix with numerics.
pub const Type = enum {
    i64,
    f64,
    str,
    /// Exact DECIMAL: unscaled i128 values; precision and scale ride on the node (`Expr.decSpec`).
    dec,

    /// Result type of `lhs op rhs`. Strings can only combine with
    /// strings (concat); any cross-lane mix returns null and the
    /// caller must surface a TypeMismatch error.
    pub fn promote(a: Type, b: Type) ?Type {
        if (a == .str or b == .str) {
            if (a == .str and b == .str) return .str;
            return null;
        }
        // As in SQL engines, a DOUBLE operand makes the result DOUBLE; otherwise an exact operand keeps it exact.
        if (a == .f64 or b == .f64) return .f64;
        if (a == .dec or b == .dec) return .dec;
        return .i64;
    }

    /// Map back to Parquet physical type for output schema synthesis.
    pub fn toParquet(self: Type) schema.Type {
        return switch (self) {
            .i64 => .INT64,
            .f64 => .DOUBLE,
            .str => .BYTE_ARRAY,
            // The width follows the precision; `Dec.physical` picks it.
            .dec => .FIXED_LEN_BYTE_ARRAY,
        };
    }
};

/// A DECIMAL(precision, scale) type. Result types follow the rules DuckDB and Trino (Athena) share, which also give
/// Spark's scales: `+`/`-` take the larger scale and one more integer digit, `*` adds scales and precisions, both
/// capped at 38 digits; `/` leaves exact arithmetic (DOUBLE, as DuckDB does). Values are checked against the result
/// precision, so an overflow is an error, never a wrapped or rounded answer.
pub const Dec = struct {
    precision: u8,
    scale: u8,

    pub const max_precision: u8 = 38;

    pub const Error = error{
        /// A product's scale would pass 38 digits, which no DECIMAL can hold.
        DecimalScaleTooLarge,
    };

    pub fn valid(precision: i64, scale: i64) bool {
        return precision >= 1 and precision <= max_precision and scale >= 0 and scale <= precision;
    }

    /// An integer as the DECIMAL it widens to: INTEGER-sized values to DECIMAL(10,0), BIGINT to DECIMAL(19,0).
    pub fn ofInt(digits: u8) Dec {
        return .{ .precision = digits, .scale = 0 };
    }

    fn intDigits(self: Dec) u8 {
        return self.precision - self.scale;
    }

    pub fn addSub(a: Dec, b: Dec) Dec {
        const scale = @max(a.scale, b.scale);
        const int_digits = @max(a.intDigits(), b.intDigits());
        return .{ .precision = @min(max_precision, int_digits + scale + 1), .scale = scale };
    }

    pub fn mul(a: Dec, b: Dec) Error!Dec {
        const scale = @as(u16, a.scale) + b.scale;
        if (scale > max_precision) return error.DecimalScaleTooLarge;
        const precision: u16 = @min(max_precision, @as(u16, a.precision) + b.precision);
        return .{ .precision = @intCast(@max(precision, scale)), .scale = @intCast(scale) };
    }

    /// The type values of both fit, for coalesce: every integer digit either has, at the larger scale.
    pub fn common(a: Dec, b: Dec) Dec {
        const scale = @max(a.scale, b.scale);
        return .{ .precision = @min(max_precision, @max(a.intDigits(), b.intDigits()) + scale), .scale = scale };
    }

    /// sum() of this type: DECIMAL(38, s), as DuckDB and Trino give it.
    pub fn sum(self: Dec) Dec {
        return .{ .precision = max_precision, .scale = self.scale };
    }

    /// 10^precision, the first magnitude a value of this type cannot hold.
    pub fn bound(self: Dec) i128 {
        return pow10(self.precision);
    }

    /// The narrowest physical type the parquet spec allows for this precision: INT32 to 9 digits, INT64 to 18, and
    /// past that a 16-byte FIXED_LEN_BYTE_ARRAY (38 digits need 16 bytes).
    pub fn physical(self: Dec) schema.Type {
        if (self.precision <= 9) return .INT32;
        if (self.precision <= 18) return .INT64;
        return .FIXED_LEN_BYTE_ARRAY;
    }
};

/// 10^n for n in 0..38, exactly.
pub fn pow10(n: u8) i128 {
    std.debug.assert(n <= Dec.max_precision);
    return pow10_table[n];
}

/// The double nearest `sum / 10^scale`, ties to even: DuckDB's cast of a DECIMAL to DOUBLE, which is exact to the last
/// bit (0 of 240000 probed casts differ). A quotient of at least 55 bits is cut at 53, the rest deciding the rounding
/// with the remainder as a sticky bit; a 113-bit quotient would lose a tie plus a digit 19 places below the point.
pub fn decimalToDouble(sum: i192, scale: u8) f64 {
    if (sum == 0) return 0;
    const n: u256 = @abs(sum);
    const d: u256 = @intCast(pow10(scale));
    const n_bits: i32 = 256 - @clz(n);
    const d_bits: i32 = 256 - @clz(d);
    const shift: u8 = @intCast(@max(0, 55 + d_bits - n_bits));
    const scaled = n << shift;
    const q = scaled / d;
    const sticky = scaled % d != 0;
    const drop: u8 = @intCast(256 - @clz(q) - 53);
    const dropped = q & ((@as(u256, 1) << drop) - 1);
    const half = @as(u256, 1) << (drop - 1);
    var m: u256 = q >> drop;
    if (dropped > half or (dropped == half and (sticky or m & 1 == 1))) m += 1;
    const magnitude = std.math.ldexp(@as(f64, @floatFromInt(@as(u64, @intCast(m)))), @as(i32, drop) - @as(i32, shift));
    return if (sum < 0) -magnitude else magnitude;
}

/// `round(x, digits)` of a double as DuckDB computes it: scale, round half away from zero, scale back. A non-finite
/// input stays as it is, and so does a finite one whose scaling overflows to a non-finite value for positive digits;
/// for negative digits that overflow is 0.
pub fn roundDouble(x: f64, digits: i32) f64 {
    if (!std.math.isFinite(x)) return x;
    if (digits < 0) {
        // Negated as a double: `-digits` in i32 overflows at the minimum, which the parser admits.
        const m = std.math.pow(f64, 10, -@as(f64, @floatFromInt(digits)));
        const r = @round(x / m) * m;
        return if (std.math.isFinite(r)) r else 0;
    }
    const m = std.math.pow(f64, 10, @floatFromInt(digits));
    const r = @round(x * m) / m;
    return if (std.math.isFinite(r)) r else x;
}

/// `round(v, digits)` of an exact decimal at `scale`, half away from zero, as the unscaled value of the type DuckDB
/// gives the result: scale `digits` when that is fewer than `scale` (0 when negative), else `v` itself.
pub fn roundDecimal(v: i128, scale: u8, digits: i32) error{DecimalOverflow}!i128 {
    if (digits >= scale) return v;
    // In i64: `scale - digits` overflows i32 for the digit counts near the minimum, which the parser admits.
    const drop: i64 = @as(i64, scale) - digits;
    // A divisor past 10^38 is past twice any value a DECIMAL holds: everything rounds to zero.
    if (drop > Dec.max_precision) return 0;
    const divisor = pow10(@intCast(drop));
    var q = @divTrunc(v, divisor);
    if (@as(u128, @abs(@rem(v, divisor))) * 2 >= @as(u128, @intCast(divisor))) q += if (v < 0) -1 else 1;
    if (digits >= 0) return q;
    return std.math.mul(i128, q, pow10(@intCast(-digits))) catch error.DecimalOverflow;
}

/// The DECIMAL type DuckDB 2.0 gives `round(x, digits)` of a DECIMAL `d`: the same precision at the digits' scale (none
/// below the point for a negative count), one digit wider for a whole number rounded to tens and up (to 38), and `d`
/// itself where rounding changes nothing.
pub fn roundedDec(d: Dec, digits: i32) Dec {
    if (digits >= d.scale) return d;
    return .{
        .precision = if (d.scale == 0) @min(Dec.max_precision, d.precision + 1) else d.precision,
        .scale = @intCast(@max(0, digits)),
    };
}

const pow10_table: [Dec.max_precision + 1]i128 = blk: {
    var t: [Dec.max_precision + 1]i128 = undefined;
    t[0] = 1;
    for (1..t.len) |i| t[i] = t[i - 1] * 10;
    break :blk t;
};

pub const Literal = union(enum) {
    i64: i64,
    f64: f64,
    str: []const u8,
    /// A numeric literal with a decimal point (`0.05`): exact, as SQL types it. `f` is its correctly rounded double,
    /// for when it meets a DOUBLE operand.
    dec: DecLit,

    pub fn typeOf(self: Literal) Type {
        return switch (self) {
            .i64 => .i64,
            .f64 => .f64,
            .str => .str,
            .dec => .dec,
        };
    }
};

pub const DecLit = struct {
    unscaled: i128,
    spec: Dec,
    f: f64,
};

/// Column reference — resolved at parse time. `col_idx` indexes into
/// the source file's leaf columns; `physical_type` is the on-disk
/// Parquet type so the evaluator knows how to coerce the decoded
/// values into one of `Type`'s two slots.
pub const ColRef = struct {
    col_idx: usize,
    physical_type: schema.Type,
    /// Promoted type (INT32/INT64 → .i64, FLOAT/DOUBLE → .f64).
    /// Cached at parse time so eval doesn't have to recompute.
    expr_type: Type,
    /// INT64-physical column annotated UNSIGNED (UINT_64 / INTEGER{64,unsigned}).
    /// The i64 lane holds the raw 64 bits; the aggregate fold reinterprets them
    /// as u64 so sum/min/max are correct (a signed read makes 2^64-1 read -1).
    /// ≤32-bit unsigned ints don't need this — they zero-extend at decode.
    unsigned_64: bool = false,
    /// INT32-physical column annotated UNSIGNED at <=32 bits. Decoders that keep such a column in the i32 lane
    /// store its raw bits, so widening must zero-extend rather than sign-extend.
    unsigned_32: bool = false,
    /// The column's DECIMAL type when `expr_type == .dec`.
    dec: ?Dec = null,
    /// The column can hold NULL (a definition level above zero). Unknown is assumed nullable.
    nullable: bool = true,
    /// A DATE or TIMESTAMP column: the unit its integers count.
    temporal: ?Temporal = null,
};

pub const BinOp = struct {
    op: Op,
    left: *Expr,
    right: *Expr,
    /// Result type — `Type.promote(left.typeOf(), right.typeOf())`.
    /// Cached at parse time.
    result_type: Type,
    /// Cached because the parser checks it on every node it builds; a recursive walk would be O(nodes) per check.
    depth: u32,
    /// The result's DECIMAL type when `result_type == .dec`.
    dec: ?Dec = null,
};

/// Built-in scalar functions. The first slice ships only `coalesce`
/// (replace nulls with a default). Add cases here as functions land:
/// `abs`, `length`, `lower`, `upper`, etc. The parser routes `IDENT(...)`
/// to whichever Func tag matches the name (case-insensitive).
pub const Func = enum {
    coalesce,
    /// A string's length in bytes, as DuckDB's `strlen`; NULL for NULL.
    strlen,
    /// `make_date(days)`: the DATE `days` after 1970-01-01, as DuckDB's one-argument form (an INTEGER argument).
    make_date,
    /// `epoch_ms(ms)`: the TIMESTAMP `ms` milliseconds after the epoch, held in microseconds as DuckDB holds it.
    epoch_ms,
    /// `date_trunc('part', t)`: `t` floored to the part, a TIMESTAMP in microseconds (a DATE argument too, as DuckDB).
    date_trunc,
    /// `date_part('part', t)`, which `EXTRACT(part FROM t)` is: the part's BIGINT value.
    date_part,
    /// `regexp_replace(s, 'pattern', 'replacement'[, 'options'])`, in RE2's dialect as DuckDB's (`regex.zig`).
    regexp_replace,
    /// `CAST(s AS VARCHAR)` (or TEXT, STRING) of a string: the same values, typed as text wherever they leave.
    cast_text,
    /// `abs(x)` of a number, in the argument's own type: an integer whose magnitude its column's SQL type cannot hold
    /// (`abs(-2147483648)` of an INTEGER) is an overflow error, as DuckDB's.
    abs,
    // The string functions below count and cut text in characters (UTF-8 code points), as DuckDB's do; their arguments
    // after the first are constants. A string in, a string out, except `length`.
    /// `length(s)`: the number of characters (BIGINT); NULL for NULL.
    length,
    /// `substring(s, start[, len])` (also `substr`): 1-based `start` (negative counts from the end), a negative `len`
    /// takes the characters before `start`.
    substring,
    /// `left(s, n)` / `right(s, n)`: the first / last `n` characters; a negative `n` drops that many from the other end.
    left,
    right,
    /// `upper(s)` / `lower(s)`: DuckDB's case mapping, one code point to one (`case_map.zig`).
    upper,
    lower,
    /// `trim(s)`, `ltrim(s)`, `rtrim(s)`: spaces removed from both ends, the start, the end.
    trim,
    ltrim,
    rtrim,
    /// `replace(s, from, to)`: every non-overlapping `from`, left to right (an empty `from` changes nothing).
    replace,
    /// `lpad(s, n, pad)` / `rpad(s, n, pad)`: `s` cut or padded with `pad` to `n` characters.
    lpad,
    rpad,
    /// `CAST(x AS TINYINT | SMALLINT | INTEGER | BIGINT | DOUBLE | DECIMAL(p, s))`, one arm per target (a
    /// DECIMAL's precision and scale are the two literal arguments after the value). The integer ones round a DECIMAL
    /// half away from zero and a DOUBLE half to even; one that does not fit is a `ConversionOutOfRange`.
    cast_i8,
    cast_i16,
    cast_i32,
    cast_i64,
    cast_double,
    cast_dec,
    /// `floor(x)`, `ceil(x)` / `ceiling(x)`: a DECIMAL(p, s) gives DECIMAL(p, 0), an integer or a DOUBLE a DOUBLE.
    floor,
    ceil,
    /// `sqrt(x)`: a DOUBLE; a negative argument is NaN, as in DuckDB.
    sqrt,
    /// `round(x [, digits])`: digits a constant; a DECIMAL as the post-aggregate stage rounds it, a DOUBLE half away.
    round,
    /// `least(a, b, ...)`, `greatest(a, b, ...)`: the smallest or largest of the non-NULL arguments (NULL where all
    /// are), numbers only; NaN is above every number.
    least,
    greatest,
};

/// What an integer value stands for in time, so the date/time functions read it in its unit. Output keeps printing
/// the stored integer, as it does for a DATE or TIMESTAMP column.
pub const Temporal = enum {
    date,
    ts_ms,
    ts_us,
    ts_ns,
};

/// The parts `date_trunc` and `date_part` take, by DuckDB's names (singular or plural). `date_trunc` takes the ones
/// up to `.year` except `.dow`, `.isodow` and `.doy`.
pub const DatePart = enum {
    microsecond,
    millisecond,
    second,
    minute,
    hour,
    day,
    week,
    month,
    quarter,
    year,
    dow,
    isodow,
    doy,

    pub fn parse(name: []const u8) ?DatePart {
        for (std.enums.values(DatePart)) |p| {
            const tag = @tagName(p);
            if (std.ascii.eqlIgnoreCase(name, tag)) return p;
            if (name.len == tag.len + 1 and (name[tag.len] | 0x20) == 's' and
                std.ascii.eqlIgnoreCase(name[0..tag.len], tag)) return p;
        }
        return null;
    }
};

pub const Call = struct {
    func: Func,
    args: []const *Expr,
    /// Result type, resolved at parse time from the args' types.
    result_type: Type,
    depth: u32,
    /// The result's DECIMAL type when `result_type == .dec`.
    dec: ?Dec = null,
    /// What the result stands for in time (`Expr.temporal`).
    temporal: ?Temporal = null,
    /// `date_trunc`/`date_part`'s part, resolved from the literal first argument.
    part: ?DatePart = null,
    /// `regexp_replace`'s pattern and replacement, compiled from its literal arguments; shared read-only by workers.
    replace: ?*const regex.Replace = null,
    /// `abs`'s argument column's SQL integer type, when the argument is a bare integer column.
    int: ?IntType = null,
};

/// Searched `CASE WHEN cond THEN value ... [ELSE value] END`: per row, the first WHEN whose condition is TRUE gives
/// the value (UNKNOWN is not TRUE), else ELSE, else NULL. A branch is evaluated only over the rows that select it,
/// so a fault in one no row takes (`1 / 0`) is no error. A null branch is SQL NULL.
pub const Case = struct {
    whens: []const When,
    else_: ?*Expr,
    result_type: Type,
    depth: u32,
    /// The result's DECIMAL type when `result_type == .dec`: every non-NULL branch has this one.
    dec: ?Dec = null,

    pub const When = struct { cond: filter_ast.Filter, then: ?*Expr };
};

pub const Expr = union(enum) {
    literal: Literal,
    col_ref: ColRef,
    binop: BinOp,
    call: Call,
    case_: Case,

    pub fn typeOf(self: Expr) Type {
        return switch (self) {
            .literal => |l| l.typeOf(),
            .col_ref => |c| c.expr_type,
            .binop => |b| b.result_type,
            .call => |c| c.result_type,
            .case_ => |c| c.result_type,
        };
    }

    /// The unit an integer expression counts time in: a DATE or TIMESTAMP column, or a date/time function's result.
    /// Arithmetic leaves it, as `ts + 1` is no longer a time zpq could print as one.
    pub fn temporal(self: Expr) ?Temporal {
        return switch (self) {
            .col_ref => |c| c.temporal,
            .call => |c| c.temporal,
            .literal, .binop, .case_ => null,
        };
    }

    /// The DECIMAL type this expression's values have in exact arithmetic: its own for a DECIMAL expression, the
    /// integer widening for an integer one (INTEGER-sized columns and literals to 10 digits, BOOLEAN to 1, the rest to
    /// BIGINT's 19); null for DOUBLE and strings, which never become exact.
    pub fn decSpec(self: Expr) ?Dec {
        return switch (self.typeOf()) {
            .dec => switch (self) {
                .literal => |l| l.dec.spec,
                .col_ref => |c| c.dec.?,
                .binop => |b| b.dec.?,
                .call => |c| c.dec.?,
                .case_ => |c| c.dec.?,
            },
            .i64 => switch (self) {
                .literal => |l| Dec.ofInt(if (std.math.cast(i32, l.i64) != null) 10 else 19),
                .col_ref => |c| switch (c.physical_type) {
                    .INT32 => Dec.ofInt(10),
                    .BOOLEAN => Dec.ofInt(1),
                    else => Dec.ofInt(if (c.unsigned_64) 20 else 19),
                },
                else => Dec.ofInt(19),
            },
            .f64, .str => null,
        };
    }

    /// Tree height, which `parser.MAX_EXPR_DEPTH` bounds alongside the parser's own recursion: it sets how many full
    /// intermediate columns are live at once.
    pub fn depth(self: Expr) u32 {
        return switch (self) {
            .literal, .col_ref => 1,
            .binop => |b| b.depth,
            .call => |c| c.depth,
            .case_ => |c| c.depth,
        };
    }

    /// Whether `a` and `b` are the same bound expression: the same columns, operators, functions and literals, the
    /// literals compared by their exact value (a string's bytes, a DECIMAL's unscaled digits and type, a double's bits).
    /// What GROUP BY binding compares, never the text either was written as.
    pub fn eql(a: Expr, b: Expr) bool {
        if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
        return switch (a) {
            .literal => |la| switch (la) {
                .i64 => |v| b.literal == .i64 and b.literal.i64 == v,
                .f64 => |v| b.literal == .f64 and @as(u64, @bitCast(b.literal.f64)) == @as(u64, @bitCast(v)),
                .str => |v| b.literal == .str and std.mem.eql(u8, b.literal.str, v),
                .dec => |v| b.literal == .dec and b.literal.dec.unscaled == v.unscaled and
                    std.meta.eql(b.literal.dec.spec, v.spec),
            },
            .col_ref => |ca| ca.col_idx == b.col_ref.col_idx,
            .binop => |ba| ba.op == b.binop.op and ba.left.eql(b.binop.left.*) and ba.right.eql(b.binop.right.*),
            .call => |ca| blk: {
                if (ca.func != b.call.func or ca.args.len != b.call.args.len) break :blk false;
                for (ca.args, b.call.args) |x, y| if (!x.eql(y.*)) break :blk false;
                break :blk true;
            },
            .case_ => |ca| blk: {
                const cb = b.case_;
                if (ca.whens.len != cb.whens.len or !optEql(ca.else_, cb.else_)) break :blk false;
                for (ca.whens, cb.whens) |x, y| {
                    if (!filter_ast.eql(x.cond, y.cond) or !optEql(x.then, y.then)) break :blk false;
                }
                break :blk true;
            },
        };
    }

    fn optEql(a: ?*Expr, b: ?*Expr) bool {
        const x = a orelse return b == null;
        const y = b orelse return false;
        return x.eql(y.*);
    }

    /// Whether a value of this expression can be NULL, as a written column must declare: a NULL-propagating function
    /// of a nullable operand. Arithmetic refuses nullable operands (the evaluator errors), and `coalesce` is NULL only
    /// where every argument is.
    /// SQL types this expression BOOLEAN: a BOOLEAN column, or a CASE or coalesce whose every value is one (a NULL
    /// branch aside). Its lane holds 0/1 like any integer; output prints and writes it as BOOLEAN.
    pub fn isBoolean(self: Expr) bool {
        return switch (self) {
            .literal, .binop => false,
            .col_ref => |c| c.physical_type == .BOOLEAN,
            .case_ => |c| blk: {
                var any = false;
                for (c.whens) |w| if (w.then) |t| {
                    if (!t.isBoolean()) break :blk false;
                    any = true;
                };
                if (c.else_) |e| {
                    if (!e.isBoolean()) break :blk false;
                    any = true;
                }
                break :blk any;
            },
            .call => |c| c.func == .coalesce and for (c.args) |a| {
                if (!a.isBoolean()) break false;
            } else true,
        };
    }

    pub fn mayBeNull(self: Expr) bool {
        return switch (self) {
            .literal => false,
            // NULL on either side is NULL.
            .binop => |b| b.left.mayBeNull() or b.right.mayBeNull(),
            .col_ref => |c| c.nullable,
            .case_ => |c| blk: {
                const e = c.else_ orelse break :blk true;
                if (e.mayBeNull()) break :blk true;
                for (c.whens) |w| {
                    const t = w.then orelse break :blk true;
                    if (t.mayBeNull()) break :blk true;
                }
                break :blk false;
            },
            .call => |c| switch (c.func) {
                .strlen, .make_date, .epoch_ms, .regexp_replace, .cast_text, .abs, .length, .substring, .left, .right, .upper, .lower, .trim, .ltrim, .rtrim, .replace, .lpad, .rpad, .cast_i8, .cast_i16, .cast_i32, .cast_i64, .cast_double, .cast_dec, .floor, .ceil, .sqrt, .round => c.args[0].mayBeNull(),
                .date_trunc, .date_part => c.args[1].mayBeNull(),
                .coalesce, .least, .greatest => for (c.args) |a| {
                    if (!a.mayBeNull()) break false;
                } else true,
            },
        };
    }

    /// Walk the expression and mark every referenced column index in
    /// `fetch_arr`. Used by drivers (CLI + Lambda) to compute the set
    /// of columns to decode for a given expression.
    pub fn collectColumns(self: Expr, fetch_arr: []bool) void {
        switch (self) {
            .literal => {},
            .col_ref => |c| if (c.col_idx < fetch_arr.len) {
                fetch_arr[c.col_idx] = true;
            },
            .binop => |b| {
                b.left.collectColumns(fetch_arr);
                b.right.collectColumns(fetch_arr);
            },
            .call => |c| for (c.args) |arg| arg.collectColumns(fetch_arr),
            .case_ => |c| {
                for (c.whens) |w| {
                    filter_ast.markColumns(w.cond, fetch_arr);
                    if (w.then) |t| t.collectColumns(fetch_arr);
                }
                if (c.else_) |e| e.collectColumns(fetch_arr);
            },
        }
    }
};

/// One output column from a `SELECT` clause. The optional alias is the
/// name used in the output schema; when null, the caller falls back to
/// a synthetic name (e.g. `"_expr_3"`) — the parser's job to populate
/// `alias` from `expr AS name` syntax.
pub const SelectItem = struct {
    expr: Expr,
    alias: ?[]const u8,
};

/// One ORDER BY key over an aggregate's result rows: a result row column, by position in the SELECT list followed by
/// `ResultOrder.hidden`.
pub const OrderKey = struct {
    column: usize,
    descending: bool,
    /// NULLS FIRST: the key's NULLs rank before every value (the default puts them last, in either direction).
    nulls_first: bool = false,
};

/// A SQL integer type by DuckDB's names, the width an integer operation is checked in: DuckDB raises an overflow
/// error when a result leaves its type's range, not the range of the machine integer zpq computes it in.
pub const IntType = enum {
    i8,
    i16,
    i32,
    i64,
    i128,
    u8,
    u16,
    u32,
    u64,

    pub fn minValue(self: IntType) i128 {
        return switch (self) {
            .i8 => std.math.minInt(i8),
            .i16 => std.math.minInt(i16),
            .i32 => std.math.minInt(i32),
            .i64 => std.math.minInt(i64),
            .i128 => std.math.minInt(i128),
            .u8, .u16, .u32, .u64 => 0,
        };
    }

    pub fn maxValue(self: IntType) i128 {
        return switch (self) {
            .i8 => std.math.maxInt(i8),
            .i16 => std.math.maxInt(i16),
            .i32 => std.math.maxInt(i32),
            .i64 => std.math.maxInt(i64),
            .i128 => std.math.maxInt(i128),
            .u8 => std.math.maxInt(u8),
            .u16 => std.math.maxInt(u16),
            .u32 => std.math.maxInt(u32),
            .u64 => std.math.maxInt(u64),
        };
    }

    pub fn holds(self: IntType, v: i128) bool {
        return v >= self.minValue() and v <= self.maxValue();
    }

    /// The type DuckDB gives an integer literal on its own: INTEGER, BIGINT or HUGEINT, the first that holds it.
    pub fn ofValue(v: i128) IntType {
        if (IntType.i32.holds(v)) return .i32;
        if (IntType.i64.holds(v)) return .i64;
        return .i128;
    }

    /// The DECIMAL precision DuckDB widens this type to when it meets a DECIMAL.
    pub fn digits(self: IntType) u8 {
        return switch (self) {
            .i8, .u8 => 3,
            .i16, .u16 => 5,
            .i32, .u32 => 10,
            .i64 => 19,
            .u64 => 20,
            .i128 => 38,
        };
    }

    /// The integer type of a column's schema element (INT32 and INT64 physical types, with their annotations), null for
    /// anything else.
    pub fn ofElement(elem: schema.SchemaElement) ?IntType {
        const physical = elem.type orelse return null;
        if (physical != .INT32 and physical != .INT64) return null;
        const lt = elem.effectiveLogicalType() orelse return if (physical == .INT32) .i32 else .i64;
        const it = switch (lt) {
            .INTEGER => |i| i,
            else => return null,
        };
        return switch (it.bitWidth) {
            8 => if (it.isSigned) .i8 else .u8,
            16 => if (it.isSigned) .i16 else .u16,
            32 => if (it.isSigned) .i32 else .u32,
            64 => if (it.isSigned) .i64 else .u64,
            else => null,
        };
    }

    /// The type of an operation between two integer-typed values, as DuckDB resolves it (checked against DuckDB 2.0.0
    /// for every pair; `+`, `-` and `*` agree). Not symmetric in spirit: TINYINT with UTINYINT is BIGINT, not SMALLINT.
    pub fn combine(a: IntType, b: IntType) IntType {
        const t = comptime blk: {
            const E = IntType;
            break :blk [9][9]E{
                // i8, i16, i32, i64, i128, u8, u16, u32, u64
                .{ .i8, .i16, .i32, .i64, .i128, .i64, .i64, .i64, .i128 }, // i8
                .{ .i16, .i16, .i32, .i64, .i128, .i16, .i64, .i64, .i128 }, // i16
                .{ .i32, .i32, .i32, .i64, .i128, .i32, .i32, .i64, .i128 }, // i32
                .{ .i64, .i64, .i64, .i64, .i128, .i64, .i64, .i64, .i128 }, // i64
                .{ .i128, .i128, .i128, .i128, .i128, .i128, .i128, .i128, .i128 }, // i128
                .{ .i64, .i16, .i32, .i64, .i128, .u8, .u16, .u32, .u64 }, // u8
                .{ .i64, .i64, .i32, .i64, .i128, .u16, .u16, .u32, .u64 }, // u16
                .{ .i64, .i64, .i64, .i64, .i128, .u32, .u32, .u32, .u64 }, // u32
                .{ .i128, .i128, .i128, .i128, .i128, .u64, .u64, .u64, .u64 }, // u64
            };
        };
        return t[@backingInt(a)][@backingInt(b)];
    }
};

/// An expression over the result columns of one group, evaluated once per group after aggregation, never per input
/// row: arithmetic and `round` over aggregates and literals. Typed as DuckDB types it (`result_program.PostType`).
pub const PostExpr = union(enum) {
    /// A result row column, by position in the SELECT list followed by `ResultOrder.hidden`. `mean`: it is an AVG,
    /// which SQL types DOUBLE, whatever zpq's decimal mean keeps.
    col: struct { column: usize, mean: bool = false },
    /// A number literal's text, typed by it (an integer, an exact decimal or a double).
    number: []const u8,
    neg: *const PostExpr,
    binary: struct { op: PostOp, left: *const PostExpr, right: *const PostExpr },
    /// `round(value)` or `round(value, digits)`; `digits` is an integer literal.
    round: struct { value: *const PostExpr, digits: i32 = 0 },
    abs: *const PostExpr,
    /// `nullif(value, other)`: `value`, or NULL where it equals `other` (NULL `other` leaves it).
    nullif: struct { value: *const PostExpr, other: *const PostExpr },
};

pub const PostOp = enum { add, sub, mul, div };

/// ORDER BY / LIMIT / OFFSET applied to an aggregate's finished result rows. Sorting is stable, so ties keep the
/// order the rows had (first appearance in the input); NULLs sort last in either direction, as in DuckDB, unless a key
/// says NULLS FIRST.
pub const ResultOrder = struct {
    keys: []const OrderKey = &.{},
    limit: ?u64 = null,
    offset: u64 = 0,
    /// ORDER BY and HAVING terms no SELECT column gives (SQL `GROUP BY k ORDER BY count(*)` without `count(*)`
    /// selected): result row columns after the SELECT list's, read and then dropped before the answer is shown.
    hidden: []const SelectColumn = &.{},
    /// Some SELECT column or hidden term is `derived`: the result row (SELECT list, then hidden terms) has columns the
    /// scan does not produce, so `keys` and `having` index that wider row, which the scan never sees.
    post: bool = false,
    /// HAVING, over result row columns; rows it does not make TRUE are dropped before ORDER BY ranks them.
    having: ?*const Having = null,
    /// ORDER BY over plain rows (no aggregate), by expressions of the input row, selected or not; `keys` is then
    /// empty. The engine sorts the rows by these keys, ties by input position, keeping OFFSET + LIMIT when limited.
    rows: []const RowKey = &.{},
};

/// One ORDER BY key over plain rows: an expression of the input row as SQL writes it, bound by the engine.
pub const RowKey = struct {
    expr: []const u8,
    descending: bool,
    nulls_first: bool = false,
};

/// A comparison operator of a HAVING leaf.
pub const CmpOp = enum {
    eq,
    ne,
    lt,
    le,
    gt,
    ge,

    pub fn negate(self: CmpOp) CmpOp {
        return switch (self) {
            .eq => .ne,
            .ne => .eq,
            .lt => .ge,
            .le => .gt,
            .gt => .le,
            .ge => .lt,
        };
    }

    /// The operator with its operands swapped: `5 < x` is `x > 5`.
    pub fn flip(self: CmpOp) CmpOp {
        return switch (self) {
            .eq, .ne => self,
            .lt => .gt,
            .le => .ge,
            .gt => .lt,
            .ge => .le,
        };
    }

    pub fn holds(self: CmpOp, ord: std.math.Order) bool {
        return switch (self) {
            .eq => ord == .eq,
            .ne => ord != .eq,
            .lt => ord == .lt,
            .le => ord != .gt,
            .gt => ord == .gt,
            .ge => ord != .lt,
        };
    }
};

/// A HAVING literal as written: a number's text, typed only against the result column it meets (an integer, an
/// exact decimal or a double), a string's bytes, or NULL.
pub const HavingLiteral = union(enum) {
    null,
    number: []const u8,
    string: []const u8,
};

/// A HAVING predicate over a result row's columns (`OrderKey.column` positions). NOT is pushed to the leaves, so a
/// row whose column is NULL fails a leaf and its negation alike (UNKNOWN), and only TRUE keeps a row.
pub const Having = union(enum) {
    all: []const Having,
    any: []const Having,
    cmp: struct { column: usize, op: CmpOp, literal: HavingLiteral },
    /// One result column against another (`HAVING sum(a) > avg(a) * 1000`): both are result row columns.
    cmp_col: struct { left: usize, op: CmpOp, right: usize },
    null_check: struct { column: usize, is_not: bool },

    /// The predicate's negation, structurally (De Morgan, leaves flipped), never a complement of the rows it passes.
    pub fn negated(self: Having, arena: std.mem.Allocator) std.mem.Allocator.Error!Having {
        return switch (self) {
            .all, .any => |terms| blk: {
                const out = try arena.alloc(Having, terms.len);
                for (terms, out) |t, *o| o.* = try t.negated(arena);
                break :blk if (self == .all) .{ .any = out } else .{ .all = out };
            },
            .cmp => |c| .{ .cmp = .{ .column = c.column, .op = c.op.negate(), .literal = c.literal } },
            .cmp_col => |c| .{ .cmp_col = .{ .left = c.left, .op = c.op.negate(), .right = c.right } },
            .null_check => |n| .{ .null_check = .{ .column = n.column, .is_not = !n.is_not } },
        };
    }

    /// Call `f` on every column a leaf reads.
    pub fn eachColumn(self: Having, ctx: anytype, comptime f: fn (@TypeOf(ctx), usize) void) void {
        switch (self) {
            .all, .any => |terms| for (terms) |t| t.eachColumn(ctx, f),
            .cmp => |c| f(ctx, c.column),
            .cmp_col => |c| {
                f(ctx, c.left);
                f(ctx, c.right);
            },
            .null_check => |n| f(ctx, n.column),
        }
    }
};

/// One output column as a select list writes it, before binding: the expression's text and its `AS` alias. GROUP BY
/// binding matches the text against key and aggregate names, so it is never parsed into an `Expr`; keeping the alias
/// apart means no layer has to find it again in the text.
pub const SelectColumn = struct {
    expr: []const u8,
    /// Present when a frontend already bound this output expression; text remains a display name.
    bound_expr: ?Expr = null,
    alias: ?[]const u8 = null,
    /// How a GROUP BY output binds. SQL knows which items aggregate: one does not bind by name but takes the next
    /// aggregate call, in order; any other must equal a GROUP BY key, compared as bound expressions (column, operator,
    /// literal), never as text. The flag forms (`--column-order`) bind names.
    role: Role = .named,
    /// A `derived` column's expression over other result columns.
    post: ?*const PostExpr = null,

    /// `derived`: computed from the group's other result columns after aggregation (`sum(a) / sum(b)`), so the scan
    /// binds nothing for it; the aggregates it reads are hidden columns (`ResultOrder.hidden`).
    pub const Role = enum { named, key, agg, derived };

    /// `name` or `expr AS name`, as a `--column-order` entry spells it. Splits at the last ` AS `.
    pub fn parse(text: []const u8) SelectColumn {
        const clean = std.mem.trim(u8, text, " ");
        const as_idx = std.mem.lastIndexOf(u8, clean, " AS ") orelse return .{ .expr = clean };
        return .{
            .expr = std.mem.trim(u8, clean[0..as_idx], " "),
            .alias = std.mem.trim(u8, clean[as_idx + 4 ..], " "),
        };
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "IntType: DuckDB's operation types, ranges and literal types" {
    const T = IntType;
    // Symmetric, and a type with itself is itself.
    for (std.enums.values(T)) |a| {
        try testing.expectEqual(a, T.combine(a, a));
        for (std.enums.values(T)) |b| try testing.expectEqual(T.combine(a, b), T.combine(b, a));
    }
    // Spot values from DuckDB 2.0.0: TINYINT with UTINYINT is BIGINT, SMALLINT with UTINYINT stays SMALLINT, an
    // unsigned type meets a signed one at the next signed type up, and UBIGINT with any signed type is HUGEINT.
    try testing.expectEqual(T.i64, T.combine(.i8, .u8));
    try testing.expectEqual(T.i16, T.combine(.i16, .u8));
    try testing.expectEqual(T.i64, T.combine(.i32, .u32));
    try testing.expectEqual(T.i128, T.combine(.u64, .i8));
    try testing.expectEqual(T.u64, T.combine(.u32, .u64));
    try testing.expectEqual(T.i8, T.combine(.i8, .i8));
    // Ranges.
    try testing.expect(T.i32.holds(-2147483648) and !T.i32.holds(2147483648));
    try testing.expect(T.u64.holds(std.math.maxInt(u64)) and !T.u64.holds(-1) and !T.u64.holds(std.math.maxInt(u64) + 1));
    try testing.expect(T.i128.holds(std.math.minInt(i128)));
    // A literal on its own is the first of INTEGER, BIGINT and HUGEINT that holds it.
    try testing.expectEqual(T.i32, T.ofValue(-2147483648));
    try testing.expectEqual(T.i64, T.ofValue(2147483648));
    try testing.expectEqual(T.i64, T.ofValue(-2147483649));
    try testing.expectEqual(T.i128, T.ofValue(9223372036854775808));
    try testing.expectEqual(@as(u8, 20), T.u64.digits());
}

test "Type.promote" {
    try testing.expectEqual(Type.i64, Type.promote(.i64, .i64).?);
    try testing.expectEqual(Type.f64, Type.promote(.i64, .f64).?);
    try testing.expectEqual(Type.f64, Type.promote(.f64, .i64).?);
    try testing.expectEqual(Type.f64, Type.promote(.f64, .f64).?);
    try testing.expectEqual(Type.str, Type.promote(.str, .str).?);
    // Cross-lane mixes return null (caller surfaces TypeMismatch).
    try testing.expectEqual(@as(?Type, null), Type.promote(.str, .i64));
    try testing.expectEqual(@as(?Type, null), Type.promote(.f64, .str));
}

test "Type.toParquet" {
    try testing.expectEqual(schema.Type.INT64, Type.i64.toParquet());
    try testing.expectEqual(schema.Type.DOUBLE, Type.f64.toParquet());
}

test "Literal.typeOf" {
    const li: Literal = .{ .i64 = 42 };
    const lf: Literal = .{ .f64 = 3.14 };
    try testing.expectEqual(Type.i64, li.typeOf());
    try testing.expectEqual(Type.f64, lf.typeOf());
}

test "Expr.typeOf composes through binop" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const left = try a.create(Expr);
    left.* = .{ .literal = .{ .i64 = 1 } };
    const right = try a.create(Expr);
    right.* = .{ .literal = .{ .f64 = 2.0 } };

    const e: Expr = .{ .binop = .{
        .op = .add,
        .left = left,
        .right = right,
        .result_type = .f64,
        .depth = 2,
    } };
    try testing.expectEqual(Type.f64, e.typeOf());
}

test "Expr.isBoolean: a BOOLEAN column, and CASE or coalesce whose every value is one" {
    var b: Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BOOLEAN, .expr_type = .i64 } };
    var i: Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 } };
    try std.testing.expect(b.isBoolean());
    try std.testing.expect(!i.isBoolean());
    const always: filter_ast.Filter = .{ .and_filter = .{ .children = &.{} } };
    const only_b = [_]Case.When{.{ .cond = always, .then = &b }};
    const null_then = [_]Case.When{ .{ .cond = always, .then = null }, .{ .cond = always, .then = &b } };
    const mixed = [_]Case.When{.{ .cond = always, .then = &i }};
    const case = struct {
        fn of(whens: []const Case.When, else_: ?*Expr) Expr {
            return .{ .case_ = .{ .whens = whens, .else_ = else_, .result_type = .i64, .depth = 1 } };
        }
    }.of;
    try std.testing.expect(case(&only_b, null).isBoolean());
    try std.testing.expect(case(&null_then, &b).isBoolean());
    try std.testing.expect(!case(&only_b, &i).isBoolean());
    try std.testing.expect(!case(&mixed, &b).isBoolean());
    // All NULL is no BOOLEAN.
    try std.testing.expect(!case(null_then[0..1], null).isBoolean());
    const both = [_]*Expr{ &b, &b };
    const one = [_]*Expr{ &b, &i };
    try std.testing.expect((Expr{ .call = .{ .func = .coalesce, .args = &both, .result_type = .i64, .depth = 1 } }).isBoolean());
    try std.testing.expect(!(Expr{ .call = .{ .func = .coalesce, .args = &one, .result_type = .i64, .depth = 1 } }).isBoolean());
}
