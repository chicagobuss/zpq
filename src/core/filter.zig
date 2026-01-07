const std = @import("std");

/// Supported filter predicates
pub const Predicate = enum {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    Neq,
};

/// A filter to be applied to a specific column.
/// Currently supports i32, i64, f32, f64, and bool.
pub const Filter = union(enum) {
    Bool: struct { col_idx: usize, pred: Predicate, val: bool },
    Int32: struct { col_idx: usize, pred: Predicate, val: i32 },
    Int64: struct { col_idx: usize, pred: Predicate, val: i64 },
    Float: struct { col_idx: usize, pred: Predicate, val: f32 },
    Double: struct { col_idx: usize, pred: Predicate, val: f64 },
    ByteArray: struct { col_idx: usize, pred: Predicate, val: []const u8 },

    pub fn colIndex(self: Filter) usize {
        return switch (self) {
            inline else => |f| f.col_idx,
        };
    }
};
