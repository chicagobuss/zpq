const std = @import("std");
const filters_mod = @import("filters/mod.zig");
const FilterOp = filters_mod.FilterOp;

/// Filter predicate with column, operator, and value.
pub const Predicate = struct {
    column: []const u8,
    op: Operator,
    value: []const u8,
    value2: ?[]const u8 = null, // Second value for BETWEEN

    pub const Operator = enum {
        eq, // =
        neq, // !=
        gt, // >
        lt, // <
        gte, // >=
        lte, // <=
        is_null, // IS NULL
        is_not_null, // IS NOT NULL
        between, // BETWEEN low AND high

        /// Convert to FilterOp for use with unified Filter
        pub fn toFilterOp(self: Operator) FilterOp {
            return switch (self) {
                .eq => .eq,
                .neq => .neq,
                .gt => .gt,
                .lt => .lt,
                .gte => .gte,
                .lte => .lte,
                .is_null => .is_null,
                .is_not_null => .is_not_null,
                .between => .between,
            };
        }
    };

    fn split(expr: []const u8, idx: usize, op_len: usize) struct { []const u8, []const u8 } {
        return .{
            std.mem.trim(u8, expr[0..idx], " "),
            std.mem.trim(u8, expr[idx + op_len ..], " "),
        };
    }

    /// Check if string contains substring (case-insensitive)
    fn containsIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
        if (needle.len > haystack.len) return null;
        var i: usize = 0;
        outer: while (i <= haystack.len - needle.len) : (i += 1) {
            for (0..needle.len) |j| {
                const h = std.ascii.toLower(haystack[i + j]);
                const n = std.ascii.toLower(needle[j]);
                if (h != n) continue :outer;
            }
            return i;
        }
        return null;
    }

    /// Parse a single predicate expression.
    pub fn parse(expr: []const u8) !Predicate {
        // Try BETWEEN first
        if (containsIgnoreCase(expr, " BETWEEN ")) |between_idx| {
            const col = std.mem.trim(u8, expr[0..between_idx], " ");
            const rest = expr[between_idx + 9 ..]; // " BETWEEN " is 9 chars
            // Find " AND " in the rest
            if (containsIgnoreCase(rest, " AND ")) |and_idx| {
                const low = std.mem.trim(u8, rest[0..and_idx], " ");
                const high = std.mem.trim(u8, rest[and_idx + 5 ..], " ");
                return .{ .column = col, .op = .between, .value = low, .value2 = high };
            }
            return error.InvalidPredicateFormat;
        }
        // Try IS NOT NULL first (before IS NULL to avoid partial match)
        if (containsIgnoreCase(expr, " IS NOT NULL")) |idx| {
            const col = std.mem.trim(u8, expr[0..idx], " ");
            return .{ .column = col, .op = .is_not_null, .value = "" };
        }
        // Try IS NULL
        if (containsIgnoreCase(expr, " IS NULL")) |idx| {
            const col = std.mem.trim(u8, expr[0..idx], " ");
            return .{ .column = col, .op = .is_null, .value = "" };
        }
        // Try two-char operators first
        if (std.mem.indexOf(u8, expr, ">=")) |idx| {
            const col, const val = split(expr, idx, 2);
            return .{ .column = col, .op = .gte, .value = val };
        }
        if (std.mem.indexOf(u8, expr, "<=")) |idx| {
            const col, const val = split(expr, idx, 2);
            return .{ .column = col, .op = .lte, .value = val };
        }
        if (std.mem.indexOf(u8, expr, "!=")) |idx| {
            const col, const val = split(expr, idx, 2);
            return .{ .column = col, .op = .neq, .value = val };
        }
        // Single-char operators
        if (std.mem.indexOfScalar(u8, expr, '=')) |idx| {
            const col, const val = split(expr, idx, 1);
            return .{ .column = col, .op = .eq, .value = val };
        }
        if (std.mem.indexOfScalar(u8, expr, '>')) |idx| {
            const col, const val = split(expr, idx, 1);
            return .{ .column = col, .op = .gt, .value = val };
        }
        if (std.mem.indexOfScalar(u8, expr, '<')) |idx| {
            const col, const val = split(expr, idx, 1);
            return .{ .column = col, .op = .lt, .value = val };
        }
        return error.InvalidPredicateFormat;
    }

    /// Split an expression by " AND " and parse each part into a list of predicates.
    pub fn parseMulti(allocator: std.mem.Allocator, expr: []const u8) ![]Predicate {
        var list = std.ArrayListUnmanaged(Predicate){};
        defer list.deinit(allocator);

        var start: usize = 0;
        var i: usize = 0;
        while (i < expr.len) {
            if (i + 5 <= expr.len and
                (std.ascii.toLower(expr[i]) == ' ' and
                    std.ascii.toLower(expr[i + 1]) == 'a' and
                    std.ascii.toLower(expr[i + 2]) == 'n' and
                    std.ascii.toLower(expr[i + 3]) == 'd' and
                    std.ascii.toLower(expr[i + 4]) == ' '))
            {
                const sub_expr = std.mem.trim(u8, expr[start..i], " ");
                if (containsIgnoreCase(sub_expr, " BETWEEN ") != null and containsIgnoreCase(sub_expr, " AND ") == null) {
                    i += 5;
                    continue;
                }

                try list.append(allocator, try parse(sub_expr));
                i += 5;
                start = i;
            } else {
                i += 1;
            }
        }

        const final_expr = std.mem.trim(u8, expr[start..], " ");
        if (final_expr.len > 0) {
            try list.append(allocator, try parse(final_expr));
        }

        return list.toOwnedSlice(allocator);
    }
};
