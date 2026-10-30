//! Owned, schema-independent syntax shared by native frontends and versioned worker requests.
const ast = @import("ast.zig");
const filter = @import("../filter/ast.zig");
const agg = @import("agg.zig");

pub const Expr = union(enum) {
    number: struct { token: []const u8, floating: bool },
    string: []const u8,
    column: []const u8,
    negate: *const Expr,
    binary: struct { op: ast.Op, left: *const Expr, right: *const Expr },
    call: struct { name: []const u8, args: []const *const Expr },
    case_: struct { whens: []const When, else_: ?*const Expr },
    /// `CAST(x AS VARCHAR)`, `TEXT` or `STRING`; a cast to any other type is no function zpq has.
    cast_text: *const Expr,
    pub const When = struct { cond: Predicate, then: ?*const Expr };
};

pub const Operand = struct { column: []const u8, make_date: bool = false };

pub const Predicate = union(enum) {
    and_: []const Predicate,
    or_: []const Predicate,
    not_: *const Predicate,
    comparison: struct { operand: Operand, op: filter.Operator, value: []const u8 },
    null_check: struct { column: []const u8, is_not: bool },
    like: struct { column: []const u8, pattern: []const u8, negate: bool },
    in_list: struct { operand: Operand, values: []const []const u8, negate: bool },
    between: struct { operand: Operand, low: []const u8, high: []const u8, negate: bool },
    boolean: []const u8,
};

pub const Aggregate = struct {
    func: agg.AggFunc,
    arg: ?Expr,
    where: ?Predicate,
    alias: []const u8,
    default_name: ?[]const u8,
    state: bool = false,
};

pub const SelectItem = struct { expr: Expr, alias: ?[]const u8 = null, display: []const u8 = "" };

pub const Column = struct {
    display: []const u8,
    alias: ?[]const u8 = null,
    role: ast.SelectColumn.Role,
    expr: ?Expr = null,
};

pub const RowKey = struct { expr: Expr, display: []const u8, descending: bool };

pub const Query = struct {
    aggregate: bool,
    calls: []const Aggregate = &.{},
    keys: ?[]const SelectItem = null,
    filter: ?Predicate = null,
    select: ?[]const SelectItem = null,
    columns: []const Column = &.{},
    hidden: []const Column = &.{},
    row_keys: []const RowKey = &.{},
};
