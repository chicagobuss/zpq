//! The words DuckDB 2.0.0 reads as keywords (`duckdb_keywords()`, every category), sorted. It writes a name that is one of
//! them in double quotes when it prints an expression (`"year"(d)`, `("name" + 1)`).
const std = @import("std");

const words = [_][]const u8{
    "abort", "absolute", "access", "action", "add", "admin", "after", "aggregate", "all", "also", "alter",
    "always", "analyse", "analyze", "and", "anti", "any", "approx", "array", "as", "asc", "asof", "assertion",
    "assignment", "asymmetric", "at", "attach", "attribute", "authorization", "backward", "before", "begin",
    "between", "bigint", "binary", "bit", "boolean", "both", "by", "cache", "call", "called", "cascade",
    "cascaded", "case", "cast", "catalog", "centuries", "century", "chain", "char", "character", "characteristics",
    "check", "checkpoint", "class", "close", "cluster", "coalesce", "collate", "collation", "column", "columns",
    "comment", "comments", "commit", "committed", "compression", "concurrently", "configuration", "conflict",
    "connect", "connection", "constraint", "constraints", "content", "continue", "conversion", "copy", "cost",
    "create", "cross", "csv", "cube", "current", "cursor", "cycle", "data", "database", "day", "days",
    "deallocate", "dec", "decade", "decades", "decimal", "declare", "default", "defaults", "deferrable",
    "deferred", "definer", "delete", "delimiter", "delimiters", "depends", "desc", "describe", "destroy", "detach",
    "dictionary", "disable", "discard", "disconnect", "distance", "distinct", "do", "document", "domain", "double",
    "drop", "each", "else", "enable", "encoding", "encrypted", "end", "enum", "error", "escape", "event", "exact",
    "except", "exclude", "excluding", "exclusive", "execute", "exists", "explain", "export", "export_state",
    "extension", "extensions", "external", "extract", "false", "family", "fetch", "filter", "first", "float",
    "following", "for", "force", "foreign", "forward", "freeze", "from", "full", "function", "functions",
    "generated", "glob", "global", "grant", "granted", "group", "grouping", "grouping_id", "groups", "handler",
    "having", "header", "hold", "hour", "hours", "identity", "if", "ignore", "ilike", "immediate", "immutable",
    "implicit", "import", "in", "include", "including", "increment", "index", "indexes", "inherit", "inherits",
    "initially", "inline", "inner", "inout", "input", "insensitive", "insert", "install", "instead", "int",
    "integer", "intersect", "interval", "into", "invoker", "is", "isnull", "isolation", "join", "json", "key",
    "label", "lambda", "language", "large", "last", "lateral", "leading", "leakproof", "left", "level", "like",
    "limit", "listen", "load", "local", "location", "lock", "locked", "logged", "macro", "map", "mapping", "match",
    "matched", "materialized", "maxvalue", "merge", "method", "microsecond", "microseconds", "millennia",
    "millennium", "millisecond", "milliseconds", "minute", "minutes", "minvalue", "mode", "month", "months",
    "move", "name", "names", "national", "natural", "nchar", "nearest", "new", "next", "no", "none", "not",
    "nothing", "notify", "notnull", "nowait", "null", "nullif", "nulls", "numeric", "object", "of", "off",
    "offset", "oids", "old", "on", "only", "operator", "option", "options", "or", "order", "ordinality", "others",
    "out", "outer", "over", "overlaps", "overlay", "overriding", "owned", "owner", "parallel", "parser", "partial",
    "partition", "partitioned", "passing", "password", "percent", "persistent", "pivot", "pivot_longer",
    "pivot_wider", "placing", "plans", "policy", "position", "positional", "pragma", "preceding", "precision",
    "prepare", "prepared", "preserve", "primary", "prior", "privileges", "procedural", "procedure", "program",
    "publication", "qualify", "quarter", "quarters", "quote", "range", "read", "real", "reassign", "recheck",
    "recursive", "ref", "references", "referencing", "refresh", "register", "reindex", "relative", "release",
    "rename", "repeatable", "replace", "replica", "reset", "resource", "resources", "respect", "restart",
    "restrict", "returning", "returns", "revoke", "right", "role", "rollback", "rollup", "row", "rows", "rule",
    "sample", "savepoint", "schema", "schemas", "scope", "scroll", "search", "second", "seconds", "secret",
    "secure", "security", "select", "semi", "sequence", "sequences", "serializable", "server", "session", "set",
    "setof", "sets", "share", "show", "similar", "similarity", "simple", "skip", "smallint", "snapshot", "some",
    "sorted", "source", "sql", "stable", "standalone", "start", "statement", "statistics", "stdin", "stdout",
    "storage", "stored", "strict", "strip", "struct", "subscription", "substring", "summarize", "symmetric",
    "sysid", "system", "table", "tables", "tablesample", "tablespace", "target", "temp", "template", "temporary",
    "text", "then", "ties", "time", "timestamp", "to", "trailing", "transaction", "transform", "treat", "trigger",
    "trim", "true", "truncate", "trusted", "try_cast", "tuple", "type", "types", "unbounded", "uncommitted",
    "unencrypted", "union", "unique", "unknown", "unlisten", "unlogged", "unpack", "unpivot", "until", "update",
    "use", "user", "using", "vacuum", "valid", "validate", "validator", "value", "values", "varchar", "variable",
    "variadic", "varying", "verbose", "version", "view", "views", "virtual", "volatile", "week", "weeks", "when",
    "where", "whitespace", "window", "with", "within", "without", "work", "wrapper", "write", "xml",
    "xmlattributes", "xmlconcat", "xmlelement", "xmlexists", "xmlforest", "xmlnamespaces", "xmlparse", "xmlpi",
    "xmlroot", "xmlserialize", "xmltable", "year", "years", "yes", "zone",
};

pub fn isKeyword(name: []const u8) bool {
    var buf: [64]u8 = undefined;
    if (name.len > buf.len) return false;
    const lower = std.ascii.lowerString(&buf, name);
    var lo: usize = 0;
    var hi: usize = words.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, words[mid], lower)) {
            .eq => return true,
            .lt => lo = mid + 1,
            .gt => hi = mid,
        }
    }
    return false;
}

test "keywords are sorted, so the search finds each" {
    for (words[1..], 0..) |w, i| try std.testing.expect(std.mem.order(u8, words[i], w) == .lt);
    for (words) |w| try std.testing.expect(isKeyword(w));
    try std.testing.expect(isKeyword("YEAR"));
    try std.testing.expect(isKeyword("Position"));
    try std.testing.expect(!isKeyword("abs"));
    try std.testing.expect(!isKeyword("upper"));
    try std.testing.expect(!isKeyword("n"));
    try std.testing.expect(!isKeyword(""));
}
