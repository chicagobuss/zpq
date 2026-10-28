const std = @import("std");

/// The body of a JSON string: quotes, backslashes and control characters escaped. For names and messages, not values:
/// each byte that does not start a valid UTF-8 sequence (stray continuation bytes, bad leads, truncated, overlong or
/// surrogate encodings) is written as U+FFFD, one per byte, as Python's `errors="replace"` and most decoders do for
/// lone bytes. Valid sequences pass through; U+2028/U+2029 are escaped so the output also stays valid JavaScript.
pub fn writeJsonString(writer: anytype, s: []const u8) !void {
    return escapeJson(writer, s, false);
}

/// A STRING value's JSON string body. Replacing a byte would print distinct values alike, so a value that is not
/// UTF-8 fails with `InvalidUtf8` instead: a STRING column must hold UTF-8, and bytes are `TextKind.base64`.
pub fn writeJsonText(writer: anytype, s: []const u8) !void {
    return escapeJson(writer, s, true);
}

pub fn escapeJson(writer: anytype, s: []const u8, comptime strict: bool) !void {
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        if (c < 0x80) {
            switch (c) {
                '"' => try writer.writeAll("\\\""),
                '\\' => try writer.writeAll("\\\\"),
                '\n' => try writer.writeAll("\\n"),
                '\r' => try writer.writeAll("\\r"),
                '\t' => try writer.writeAll("\\t"),
                else => if (c < 0x20) {
                    var buf: [8]u8 = undefined;
                    const n = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                    try writer.writeAll(n);
                } else {
                    try writer.writeByte(c);
                },
            }
            i += 1;
            continue;
        }
        const len = std.unicode.utf8ByteSequenceLength(c) catch 0;
        const cp: ?u21 = if (len == 0 or i + len > s.len) null else std.unicode.utf8Decode(s[i..][0..len]) catch null;
        if (cp) |code| {
            if (code == 0x2028 or code == 0x2029) {
                try writer.print("\\u{x:0>4}", .{code});
            } else {
                try writer.writeAll(s[i..][0..len]);
            }
            i += len;
        } else {
            if (strict) return error.InvalidUtf8;
            try writer.writeAll("\u{FFFD}");
            i += 1;
        }
    }
}

test "writeJsonString: escapes, and replaces invalid UTF-8 with U+FFFD per byte" {
    const cases = [_]struct { in: []const u8, want: []const u8 }{
        .{ .in = "a\"b\\c\n\x01", .want = "a\\\"b\\\\c\\n\\u0001" },
        .{ .in = "caf\xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80", .want = "caf\xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80" },
        .{ .in = "\xff\xfeA", .want = "\u{FFFD}\u{FFFD}A" }, // bad lead bytes
        .{ .in = "x\xe2\x80", .want = "x\u{FFFD}\u{FFFD}" }, // truncated at the end
        .{ .in = "\xe2\x80y", .want = "\u{FFFD}\u{FFFD}y" }, // truncated mid-string
        .{ .in = "\xc0\xaf", .want = "\u{FFFD}\u{FFFD}" }, // overlong '/'
        .{ .in = "\xed\xa0\x80", .want = "\u{FFFD}\u{FFFD}\u{FFFD}" }, // UTF-16 surrogate
        .{ .in = "\x80", .want = "\u{FFFD}" }, // lone continuation byte
        .{ .in = "\xe2\x80\xa8|\xe2\x80\xa9", .want = "\\u2028|\\u2029" },
    };
    for (cases) |c| {
        var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer aw.deinit();
        try writeJsonString(&aw.writer, c.in);
        try std.testing.expectEqualStrings(c.want, aw.written());
        try std.testing.expect(std.unicode.utf8ValidateSlice(aw.written()));
    }
}

