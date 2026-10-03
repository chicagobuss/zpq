//! What a failed query was about, beyond its error tag. The parsers, the scan and the engine record it here instead
//! of printing, and each frontend reports it its own way: the CLI as the prose it prints, the Lambda as JSON fields.
//! A caller passes one per query (`?*Diag`, null to skip) and reads it only after an error; the first failure ends the
//! query, so it describes that one.

const std = @import("std");

pub const Diag = struct {
    /// The column or output name the error is about.
    column: Text = .{},
    /// The input file the error is about. Borrowed from the query's input list, which the caller owns.
    input: ?[]const u8 = null,
    /// The lower-level error behind the one returned: why `input` could not be opened or read as Parquet.
    cause: ?anyerror = null,
    detail: Detail = .none,

    pub fn setColumn(diag: ?*Diag, name: []const u8) void {
        const d = diag orelse return;
        d.column.set(name);
    }

    pub fn setInput(diag: ?*Diag, name: []const u8, cause: ?anyerror) void {
        const d = diag orelse return;
        d.input = name;
        d.cause = cause;
    }

    /// `column` and `detail` together, for a detail about a named column.
    pub fn set(diag: ?*Diag, name: []const u8, detail: Detail) void {
        const d = diag orelse return;
        d.column.set(name);
        d.detail = detail;
    }
};

/// A name copied into fixed storage, so it outlives the arenas of the query that failed. Truncated past the buffer.
pub const Text = struct {
    buf: [256]u8 = undefined,
    len: usize = 0,

    pub fn set(self: *Text, s: []const u8) void {
        self.len = @min(s.len, self.buf.len);
        @memcpy(self.buf[0..self.len], s[0..self.len]);
    }

    pub fn get(self: *const Text) []const u8 {
        return self.buf[0..self.len];
    }

    pub fn of(s: []const u8) Text {
        var t: Text = .{};
        t.set(s);
        return t;
    }
};

/// Context the error tag and names alone do not carry. Strings here are static or owned by `Text`.
pub const Detail = union(enum) {
    none,
    /// `Diag.column` names several columns. `hint` (static) says how to name the one meant.
    ambiguous_column: struct { parser: enum { filter, expression }, hint: []const u8 },
    /// Input `file` (`Diag.input`) has `other` where input 0, `first_input`, has `Diag.column`.
    schema_mismatch: struct { file: usize, first_input: []const u8, other: Text },
    /// A filter follows IS with something other than NULL or NOT NULL.
    is_operand,
    /// A filter applies LIKE to `Diag.column`, whose physical type (static) is `type_name`.
    like_non_string: struct { type_name: []const u8 },
    /// A filter compares the FLOAT16 column `Diag.column`.
    float16_filter,
};
