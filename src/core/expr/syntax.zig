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
    like: struct { column: []const u8, pattern: []const u8, negate: bool, fold: bool = false },
    in_list: struct { operand: Operand, values: []const []const u8, negate: bool },
    between: struct { operand: Operand, low: []const u8, high: []const u8, negate: bool },
    boolean: []const u8,
    /// A comparison of two expressions (`l_commitdate < l_receiptdate`, `price * (1 - discount) > 500`): WHERE's
    /// comparisons that are not a column against a literal.
    expr_compare: struct { left: *const Expr, op: filter.Operator, right: *const Expr },
};

pub const Aggregate = struct {
    func: agg.AggFunc,
    arg: ?Expr,
    where: ?Predicate,
    alias: []const u8,
    default_name: ?[]const u8,
    state: bool = false,
    /// `bool_or` and `bool_and`, which are `max` and `min` of a BOOLEAN and take nothing else.
    boolean_only: bool = false,
    /// The second argument of a function that has one (`quantile_cont(x, q)`).
    arg2: ?Expr = null,
};

pub const SelectItem = struct { expr: Expr, alias: ?[]const u8 = null, display: []const u8 = "" };

pub const Column = struct {
    display: []const u8,
    alias: ?[]const u8 = null,
    role: ast.SelectColumn.Role,
    expr: ?Expr = null,
    /// A `derived` column's expression over other result columns.
    post: ?*const ast.PostExpr = null,
};

pub const RowKey = struct { expr: Expr, display: []const u8, descending: bool, nulls_first: bool = false };

/// A two-table N:1 equi-join as the SQL frontend resolved it (`sql_join.zig`), by name: the query's own inputs are the
/// probe side, and the engine reads each build (`engine/join.zig`). Schema-independent, so it travels with the rest of
/// the query to a worker.
pub const Join = struct {
    kind: JoinKind,
    /// The probe's foreign key.
    fk: []const u8,
    fk_more: []const []const u8 = &.{},
    key_more: []const []const u8 = &.{},
    build: Build,
    /// An ANTI join a NOT EXISTS wrote (`Semi.not_exists`).
    not_exists: bool = false,
    /// More SEMI joins of the probe itself: a third table the frontend reduced onto it.
    semis: []const Semi = &.{},
    /// A star's other dimensions: INNER, N:1, each publishing its own columns as the main build does.
    dims: []const Dim = &.{},
};

/// One more dimension of a star: the probe's FK into it, and its build. `left`: a LEFT JOIN's, every probe row kept.
pub const Dim = struct {
    fk: []const u8,
    fk_more: []const []const u8 = &.{},
    key_more: []const []const u8 = &.{},
    build: Build,
    left: bool = false,
};

pub const JoinKind = enum { inner, semi, anti, left };

pub const Build = struct {
    path: []const u8,
    key: []const u8,
    fk_more: []const []const u8 = &.{},
    key_more: []const []const u8 = &.{},
    /// The build columns the query reads, which bind as virtual leaves of the probe.
    columns: []const []const u8 = &.{},
    /// The names `columns` bind under when not their own, one each, else empty: a column whose name the probe has
    /// too, or another build publishes, goes by one the frontend chose (`sql_join.publishedName`).
    columns_as: []const []const u8 = &.{},
    /// The build's own conjuncts.
    filter: ?Predicate = null,
    /// SEMI joins of the build: a third table the frontend reduced onto it (q3's customer inside orders).
    semis: []const Semi = &.{},
    /// A snowflake's tables joined to this build by an FK of its own, INNER: each keeps the build rows it matches and
    /// publishes its columns through this build's (q10's customer and nation through orders).
    dims: []const Dim = &.{},
};

/// `unique`: an INNER edge the frontend reduced to a SEMI join because no column of its table is read. Its keys must
/// be unique, as any INNER join's are: a duplicate would multiply rows the SEMI join keeps once. `anti`: the query's
/// `NOT IN (SELECT ...)`, an ANTI join. `not_exists`: an ANTI join a NOT EXISTS wrote, where a NULL on either side
/// matches nothing: a NULL FK stays, and a NULL build key drops no row.
pub const Semi = struct {
    fk: []const u8,
    fk_more: []const []const u8 = &.{},
    key_more: []const []const u8 = &.{},
    build: Build,
    unique: bool = false,
    anti: bool = false,
    not_exists: bool = false,
};

pub const Query = struct {
    /// Null for a single-table query.
    join: ?Join = null,
    aggregate: bool,
    calls: []const Aggregate = &.{},
    keys: ?[]const SelectItem = null,
    filter: ?Predicate = null,
    select: ?[]const SelectItem = null,
    columns: []const Column = &.{},
    hidden: []const Column = &.{},
    row_keys: []const RowKey = &.{},
};
