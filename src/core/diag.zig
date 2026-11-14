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
    /// Binding errors can share tags with failures caused by decoded data.
    execution_started: bool = false,

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

    /// `s` after what is held, truncated at the buffer's end.
    pub fn append(self: *Text, s: []const u8) void {
        const n = @min(s.len, self.buf.len - self.len);
        @memcpy(self.buf[self.len..][0..n], s[0..n]);
        self.len += n;
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
    /// A filter compares the string column `Diag.column` with something other than a literal.
    non_literal_value,
    /// An expression names `Diag.column`, a STRUCT, LIST or MAP, where a value is wanted. `example` is its first leaf's
    /// path; `repeated` says it is a LIST or MAP, whose rows hold several values each.
    nested_selected: struct { example: Text, repeated: bool },
    /// A filter names `Diag.column` alone as a predicate, which only a BOOLEAN column is; `type_name` (static) is its
    /// physical type.
    bare_non_boolean: struct { type_name: []const u8 },
    /// Where this thread last decoded when the query failed: input `input`, row group `row_group` (0 the first), leaf
    /// `column`, page `page` (0 the first, a dictionary page counted) when known. Set on any failure past the first
    /// decode, so a frontend reads it only for a decode error (`consumer.isDecodeError`); it leaves `Diag.column` and
    /// `Diag.input` to the error they describe.
    decode: struct {
        input: []const u8,
        row_group: usize,
        column: Text,
        page: ?u32,
        encoding: ?@import("schema.zig").Encoding = null,
    },
    /// The S3 request behind the error failed: which one, at what point, and how.
    s3_request: S3Failure,
    local_io: LocalIoFailure,
    /// A CSV input (`Diag.input`) refused at `line` (from 1, quoted newlines counted) and `byte` (from 0), in
    /// `Diag.column` when a field is at fault: `reason` (static) says why; a record's field counts, or a value and the
    /// type it does not fit.
    csv: struct {
        reason: []const u8,
        line: u64,
        byte: u64,
        expected: usize = 0,
        got: usize = 0,
        value: Text = .{},
        type_name: Text = .{},
        has_column: bool = false,
        /// A type change was refused by `--csv-strict`; otherwise its output could not start again (a pipe).
        strict: bool = false,
    },
};

pub const LocalIoFailure = struct {
    op: enum { pread, io_uring },
    errno: u16,
    offset: u64,
};

/// A failed S3 request, as reported to the user: the operation, the bucket and key, the phase it failed in, and the
/// error or the status and S3 error code and message it was answered with.
pub const S3Failure = struct {
    op: Op,
    phase: Phase,
    bucket: Text = .{},
    key: Text = .{},
    /// The error, for the `connect` and `request` phases.
    err: ?anyerror = null,
    /// The HTTP status, S3 error code (`<Code>`) and message, for the `response` phase.
    status: u16 = 0,
    code: Text = .{},
    message: Text = .{},
    /// The bucket's region S3 named, when it answered from the wrong one.
    region: Text = .{},
    /// Connections tried; only a connect failure is tried again.
    attempts: u8 = 1,

    pub const Op = enum {
        GetObject,
        HeadObject,
        ListObjectsV2,
        PutObject,
        UploadPart,
        CreateMultipartUpload,
        CompleteMultipartUpload,
        AbortMultipartUpload,
        ListMultipartUploads,

        /// The request as a user names it: `GET`, `PUT part`, or the operation's tag.
        pub fn name(self: Op) []const u8 {
            return switch (self) {
                .GetObject => "GET",
                .HeadObject => "HEAD",
                .ListObjectsV2 => "LIST",
                .PutObject => "PUT",
                .UploadPart => "PUT part",
                else => @tagName(self),
            };
        }
    };

    pub const Phase = enum {
        /// Resolving the host, connecting, or the TLS handshake: nothing was sent.
        connect,
        /// Sending the request or awaiting its answer: the server may have applied it.
        request,
        /// S3 answered with an error status.
        response,
    };

    /// One line: `GET s3://bucket/key: 404 NoSuchKey: The specified key does not exist.`; for a failure before the
    /// answer, `GET s3://bucket/key: failed while connecting (3 attempts): ConnectTimeout`.
    pub fn format(self: S3Failure, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeAll(self.op.name());
        if (self.bucket.len > 0) {
            try w.print(" s3://{s}", .{self.bucket.get()});
            if (self.key.len > 0) try w.print("/{s}", .{self.key.get()});
        }
        try w.writeAll(": ");
        switch (self.phase) {
            .connect => try w.writeAll("failed while connecting"),
            .request => try w.writeAll("failed after the request was sent"),
            .response => {
                const code = if (self.code.len > 0) self.code.get() else "(no S3 error code)";
                try w.print("{d} {s}", .{ self.status, code });
                if (self.message.len > 0) try w.print(": {s}", .{self.message.get()});
            },
        }
        if (self.attempts > 1) try w.print(" ({d} attempts)", .{self.attempts});
        if (self.err) |e| try w.print(": {s}", .{@errorName(e)});
        if (self.region.len > 0) try w.print("; the bucket is in region {s}", .{self.region.get()});
    }
};

/// Record `failure` unless the query already has a more specific detail.
pub fn setS3Failure(diag: ?*Diag, failure: S3Failure) void {
    const d = diag orelse return;
    if (d.detail == .none) d.detail = .{ .s3_request = failure };
}
