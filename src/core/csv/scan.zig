//! CSV framing: dialect sniffing, the structural pass, and exact chunk starts for parallel parsing.
//!
//! RFC 4180 quoting (a quote is escaped by doubling it) makes every quote byte toggle the quote state, so the parity
//! of the quote bytes before an offset is the exact state at it. A parallel reader cuts the file into fixed byte
//! chunks, counts each chunk's quotes (`chunkStats`), and the XOR of the earlier chunks' parities says whether a
//! chunk starts inside a quoted field. Its first record starts after its first unquoted newline (`recordStart`).
//! Nothing is guessed, so nothing needs validating afterwards, except one invariant a malformed file breaks: a
//! chunk's last record must end exactly where the next chunk's first record starts (`Scanner.end`).
//!
//! `Scanner` walks fields with 64-byte masks (quote, delimiter, newline; in-quote by prefix XOR) and hands each raw
//! field to `decodeField`, which strips the quotes, unescapes doubled quotes and refuses anything RFC 4180 does
//! not allow. `reference` is a byte-at-a-time parser of the same grammar that the tests hold the scanner to.
//! Sans-IO: callers own every buffer.

const std = @import("std");

pub const Dialect = struct {
    delimiter: u8 = ',',
    quote: u8 = '"',
};

/// Why a file is not CSV this reader accepts. Each refusal names the byte it is at.
pub const Malformed = enum {
    /// The file ends inside a quoted field.
    unterminated_quote,
    /// A quote inside a field that does not start with one (`ab"c`).
    quote_in_unquoted_field,
    /// Bytes between a closing quote and the next delimiter or newline (`"ab"c`), or a lone quote inside a
    /// quoted field.
    text_after_quote,
    /// A chunk's records ran past the next chunk's first record: the quote state at the cut was not what the
    /// quote parity says, which only a malformed file can cause.
    inconsistent_quotes,

    pub fn describe(m: Malformed) []const u8 {
        return switch (m) {
            .unterminated_quote => "a quoted field is not closed before the end of the file",
            .quote_in_unquoted_field => "a quote inside a field that is not quoted",
            .text_after_quote => "text after a closing quote, or an unescaped quote inside a quoted field",
            .inconsistent_quotes => "quotes that do not pair up",
        };
    }
};

pub const Error = error{MalformedCsv};

/// Where and why a scan stopped; `offset` is relative to the bytes the scanner was given.
pub const Failure = struct {
    kind: Malformed = .unterminated_quote,
    offset: usize = 0,
};

// ---------------------------------------------------------------- masks

const V = @Vector(64, u8);

fn eqMask(v: V, c: u8) u64 {
    return @bitCast(v == @as(V, @splat(c)));
}

/// Inclusive prefix XOR: bit i is the parity of bits 0..i.
fn prefixXor(x0: u64) u64 {
    var x = x0;
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    return x;
}

/// What a chunk's successor needs from it: whether its quote bytes are odd, and its newline count (for line
/// numbers in diagnostics; quoted newlines count, as an editor counts lines).
pub const ChunkStats = struct {
    quote_parity: u1 = 0,
    newlines: u64 = 0,
};

pub fn chunkStats(bytes: []const u8, quote: u8) ChunkStats {
    var quotes: u64 = 0;
    var lines: u64 = 0;
    var i: usize = 0;
    while (i + 64 <= bytes.len) : (i += 64) {
        const v: V = bytes[i..][0..64].*;
        quotes += @popCount(eqMask(v, quote));
        lines += @popCount(eqMask(v, '\n'));
    }
    for (bytes[i..]) |c| {
        quotes += @intFromBool(c == quote);
        lines += @intFromBool(c == '\n');
    }
    return .{ .quote_parity = @truncate(quotes), .newlines = lines };
}

/// Newlines in `bytes` (a diagnostic's line number within a chunk).
pub fn countNewlines(bytes: []const u8) u64 {
    return chunkStats(bytes, '\n').newlines;
}

/// The first record start at or after `pos`, given the quote state there: one past the first unquoted newline.
/// Null when `bytes` holds no such newline (the record continues past them).
pub fn recordStart(bytes: []const u8, pos: usize, in_quote: bool, quote: u8) ?usize {
    var state: u64 = if (in_quote) ~@as(u64, 0) else 0;
    var i = pos;
    while (i + 64 <= bytes.len) : (i += 64) {
        const v: V = bytes[i..][0..64].*;
        const inq = prefixXor(eqMask(v, quote)) ^ state;
        const nl = eqMask(v, '\n') & ~inq;
        if (nl != 0) return i + @ctz(nl) + 1;
        state = @bitCast(@as(i64, @bitCast(inq)) >> 63);
    }
    var q = state != 0;
    while (i < bytes.len) : (i += 1) {
        if (bytes[i] == quote) q = !q else if (bytes[i] == '\n' and !q) return i + 1;
    }
    return null;
}

// ---------------------------------------------------------------- fields

/// One raw field: `bytes[start..end]`, quotes and all, without a record's line terminator.
pub const Field = struct {
    start: usize,
    end: usize,
    /// The field ends its record.
    last: bool,
};

/// Fields of the records in `bytes` from `start` on, in order. The caller decides where to stop (a chunk stops at
/// the first record that ends at or past its successor's first record) and checks field counts.
pub const Scanner = struct {
    bytes: []const u8,
    /// `bytes` end where the file ends: a last record without a newline ends there, and an open quote there is
    /// malformed. Otherwise running out of bytes mid-record returns null with `incomplete` set.
    eof: bool,
    quote: u8,
    delimiter: u8,
    block: usize,
    /// Structural bits of the current block not yet returned.
    pending: u64 = 0,
    newline_bits: u64 = 0,
    carry: u64 = 0,
    field_start: usize,
    /// A field of the current record has been returned.
    in_record: bool = false,
    done: bool = false,
    incomplete: bool = false,
    failure: Failure = .{},

    pub fn init(bytes: []const u8, start: usize, eof: bool, d: Dialect) Scanner {
        var s: Scanner = .{
            .bytes = bytes,
            .eof = eof,
            .quote = d.quote,
            .delimiter = d.delimiter,
            .block = start,
            .field_start = start,
        };
        s.load();
        return s;
    }

    fn load(s: *Scanner) void {
        const n = s.bytes.len -| s.block;
        var v: V = undefined;
        if (n >= 64) {
            v = s.bytes[s.block..][0..64].*;
        } else {
            var tmp: [64]u8 = @splat(0);
            @memcpy(tmp[0..n], s.bytes[s.block..][0..n]);
            v = tmp;
        }
        const valid: u64 = if (n >= 64) ~@as(u64, 0) else (@as(u64, 1) << @intCast(n)) - 1;
        const q = eqMask(v, s.quote) & valid;
        const inq = prefixXor(q) ^ s.carry;
        s.carry = @bitCast(@as(i64, @bitCast(inq)) >> 63);
        s.newline_bits = eqMask(v, '\n') & valid & ~inq;
        s.pending = (eqMask(v, s.delimiter) & valid & ~inq) | s.newline_bits;
    }

    /// The next field, or null at the end of the bytes (`incomplete`) or of the file. `error.MalformedCsv` with
    /// `failure` set when the file ends inside quotes.
    pub inline fn next(s: *Scanner) Error!?Field {
        while (s.pending == 0) {
            if (s.done) return null;
            s.block += 64;
            if (s.block >= s.bytes.len) return s.atEnd();
            s.load();
        }
        const bit: u6 = @intCast(@ctz(s.pending));
        s.pending &= s.pending - 1;
        const at = s.block + bit;
        const last = (s.newline_bits >> bit) & 1 == 1;
        var end = at;
        if (last and end > s.field_start and s.bytes[end - 1] == '\r') end -= 1;
        const f: Field = .{ .start = s.field_start, .end = end, .last = last };
        s.field_start = at + 1;
        s.in_record = !last;
        return f;
    }

    fn atEnd(s: *Scanner) Error!?Field {
        s.done = true;
        if (!s.eof) {
            s.incomplete = s.in_record or s.field_start < s.bytes.len;
            return null;
        }
        if (s.carry != 0) {
            s.failure = .{ .kind = .unterminated_quote, .offset = s.field_start };
            return error.MalformedCsv;
        }
        if (!s.in_record and s.field_start >= s.bytes.len) return null;
        // The last record has no newline.
        var end = s.bytes.len;
        if (end > s.field_start and s.bytes[end - 1] == '\r') end -= 1;
        const f: Field = .{ .start = s.field_start, .end = end, .last = true };
        s.field_start = s.bytes.len;
        s.in_record = false;
        return f;
    }

    /// Where the next record starts (after the last field returned with `last`).
    pub fn position(s: *const Scanner) usize {
        return s.field_start;
    }
};

/// A field's value: its text with quotes removed and doubled quotes unescaped, and whether it was quoted (an
/// unquoted empty field is NULL; a quoted one is the empty string).
pub const Text = struct {
    bytes: []const u8,
    quoted: bool,
};

/// Decode one raw field. A quoted field with doubled quotes is copied into `scratch` (grown as needed); every
/// other field is a slice of `raw`.
pub fn decodeField(
    raw: []const u8,
    quote: u8,
    scratch: *std.ArrayList(u8),
    a: std.mem.Allocator,
) (Error || std.mem.Allocator.Error)!Text {
    if (raw.len == 0 or raw[0] != quote) {
        if (std.mem.indexOfScalar(u8, raw, quote) != null) return error.MalformedCsv;
        return .{ .bytes = raw, .quoted = false };
    }
    if (raw.len < 2 or raw[raw.len - 1] != quote) return error.MalformedCsv;
    const body = raw[1 .. raw.len - 1];
    const first = std.mem.indexOfScalar(u8, body, quote) orelse return .{ .bytes = body, .quoted = true };
    scratch.clearRetainingCapacity();
    try scratch.ensureTotalCapacity(a, body.len);
    scratch.appendSliceAssumeCapacity(body[0..first]);
    var i = first;
    while (i < body.len) : (i += 1) {
        const c = body[i];
        if (c == quote) {
            if (i + 1 >= body.len or body[i + 1] != quote) return error.MalformedCsv;
            i += 1;
        }
        scratch.appendAssumeCapacity(c);
    }
    return .{ .bytes = scratch.items, .quoted = true };
}

/// Why `decodeField` refused `raw` (for the diagnostic; call only after it did).
pub fn fieldMalformation(raw: []const u8, quote: u8) Malformed {
    if (raw.len == 0 or raw[0] != quote) return .quote_in_unquoted_field;
    return .text_after_quote;
}

// ---------------------------------------------------------------- sniffing

pub const SniffError = error{ Utf16, NoRecords };

/// Bytes of a UTF-8 byte-order mark at the start of the file. A UTF-16/32 mark is refused: zpq reads UTF-8.
pub fn bomLen(bytes: []const u8) SniffError!usize {
    if (std.mem.startsWith(u8, bytes, "\xef\xbb\xbf")) return 3;
    if (std.mem.startsWith(u8, bytes, "\xff\xfe") or std.mem.startsWith(u8, bytes, "\xfe\xff")) return error.Utf16;
    if (bytes.len >= 4 and std.mem.indexOfScalar(u8, bytes[0..@min(bytes.len, 512)], 0) != null) return error.Utf16;
    return 0;
}

pub const delimiter_candidates = [_]u8{ ',', '\t', ';', '|' };

/// The delimiter that splits the first records (of `sample`, from `start`) into the most fields, by each
/// candidate's most common field count. The most fields wins, so a ragged row is reported as malformed later
/// instead of the file reading as one column. A comma when no candidate splits anything.
pub fn sniffDelimiter(sample: []const u8, start: usize, quote: u8) u8 {
    var best: u8 = ',';
    var best_fields: usize = 1;
    for (delimiter_candidates) |cand| {
        var counts: [256]u16 = @splat(0);
        var pos = start;
        var records: usize = 0;
        while (pos < sample.len and records < 100) : (records += 1) {
            const r = countRecordFields(sample, pos, .{ .delimiter = cand, .quote = quote });
            if (!r.complete) break;
            pos = r.next;
            if (r.fields == 1 and r.empty) continue;
            counts[@min(r.fields, 255)] += 1;
        }
        const mode = std.mem.indexOfMax(u16, &counts);
        if (counts[mode] > 0 and mode > best_fields) {
            best = cand;
            best_fields = mode;
        }
    }
    return best;
}

const RecordCount = struct { fields: usize, next: usize, complete: bool, empty: bool };

fn countRecordFields(bytes: []const u8, pos: usize, d: Dialect) RecordCount {
    var fields: usize = 1;
    var q = false;
    var i = pos;
    while (i < bytes.len) : (i += 1) {
        const c = bytes[i];
        if (c == d.quote) {
            q = !q;
        } else if (!q and c == d.delimiter) {
            fields += 1;
        } else if (!q and c == '\n') {
            const empty = fields == 1 and (i == pos or (i == pos + 1 and bytes[pos] == '\r'));
            return .{ .fields = fields, .next = i + 1, .complete = true, .empty = empty };
        }
    }
    return .{ .fields = fields, .next = bytes.len, .complete = false, .empty = false };
}

// ---------------------------------------------------------------- reference

/// A byte-at-a-time RFC 4180 parser: the grammar the scanner must agree with, in the simplest form. Returns every
/// record's decoded fields, or the first malformation. A blank line is a record of one empty unquoted field.
pub const reference = struct {
    pub const Record = []const Text;

    pub const Result = union(enum) {
        records: []const Record,
        malformed: Failure,
    };

    pub fn parse(a: std.mem.Allocator, bytes: []const u8, d: Dialect) std.mem.Allocator.Error!Result {
        var records: std.ArrayList(Record) = .empty;
        var fields: std.ArrayList(Text) = .empty;
        var i: usize = 0;
        if (bytes.len == 0) return .{ .records = &.{} };
        while (true) {
            const field_start = i;
            var text: std.ArrayList(u8) = .empty;
            var quoted = false;
            if (i < bytes.len and bytes[i] == d.quote) {
                quoted = true;
                i += 1;
                while (true) {
                    if (i >= bytes.len) return malformed(.unterminated_quote, field_start);
                    const c = bytes[i];
                    if (c == d.quote) {
                        if (i + 1 < bytes.len and bytes[i + 1] == d.quote) {
                            try text.append(a, d.quote);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    try text.append(a, c);
                    i += 1;
                }
                // After the closing quote: a delimiter, a newline (optionally after \r) or the end.
                if (i < bytes.len and bytes[i] != d.delimiter and bytes[i] != '\n' and
                    !(bytes[i] == '\r' and (i + 1 == bytes.len or bytes[i + 1] == '\n')))
                    return malformed(.text_after_quote, field_start);
                if (i < bytes.len and bytes[i] == '\r') i += 1;
            } else {
                while (i < bytes.len and bytes[i] != d.delimiter and bytes[i] != '\n') : (i += 1) {
                    if (bytes[i] == d.quote) return malformed(.quote_in_unquoted_field, field_start);
                    try text.append(a, bytes[i]);
                }
                const at_end = i >= bytes.len or bytes[i] == '\n';
                if (at_end and text.items.len > 0 and text.items[text.items.len - 1] == '\r') text.items.len -= 1;
            }
            try fields.append(a, .{ .bytes = text.items, .quoted = quoted });
            if (i >= bytes.len) {
                try records.append(a, try fields.toOwnedSlice(a));
                break;
            }
            const sep = bytes[i];
            i += 1;
            if (sep == '\n') {
                try records.append(a, try fields.toOwnedSlice(a));
                if (i >= bytes.len) break;
            }
        }
        return .{ .records = try records.toOwnedSlice(a) };
    }

    fn malformed(kind: Malformed, offset: usize) Result {
        return .{ .malformed = .{ .kind = kind, .offset = offset } };
    }
};

// ---------------------------------------------------------------- chunked parsing, as a parallel reader does it

/// Parse `bytes` the way a parallel reader does: cut at `cuts` (sorted offsets), take each chunk's quote parity,
/// find each chunk's first record from the prefix, scan each chunk's records with its own `Scanner`, and check
/// that every chunk ends where its successor starts. For the tests, and the shape the engine's workers follow.
pub fn parseChunked(
    a: std.mem.Allocator,
    bytes: []const u8,
    d: Dialect,
    cuts: []const usize,
) std.mem.Allocator.Error!reference.Result {
    var bounds: std.ArrayList(usize) = .empty;
    try bounds.append(a, 0);
    for (cuts) |c| if (c > bounds.items[bounds.items.len - 1] and c < bytes.len) try bounds.append(a, c);
    const n = bounds.items.len;
    // Each chunk's record start, from the quote parity of everything before its cut.
    const starts = try a.alloc(usize, n + 1);
    var parity: u1 = 0;
    for (0..n) |k| {
        const lo = bounds.items[k];
        const hi = if (k + 1 < n) bounds.items[k + 1] else bytes.len;
        starts[k] = if (k == 0) 0 else recordStart(bytes, lo, parity == 1, d.quote) orelse bytes.len;
        parity ^= chunkStats(bytes[lo..hi], d.quote).quote_parity;
    }
    starts[n] = bytes.len;
    // Record starts found from correct quote states only increase; a decrease means the quotes do not pair up.
    for (1..n + 1) |k| if (starts[k] < starts[k - 1]) return reference.malformed(.inconsistent_quotes, starts[k]);
    var records: std.ArrayList(reference.Record) = .empty;
    var scratch: std.ArrayList(u8) = .empty;
    for (0..n) |k| {
        const lo = starts[k];
        const hi = starts[k + 1];
        if (lo == hi) continue;
        var s = Scanner.init(bytes, lo, true, d);
        var fields: std.ArrayList(Text) = .empty;
        while (true) {
            const f = (s.next() catch return .{ .malformed = s.failure }) orelse break;
            const raw = bytes[f.start..f.end];
            const t = decodeField(raw, d.quote, &scratch, a) catch
                return reference.malformed(fieldMalformation(raw, d.quote), f.start);
            try fields.append(a, .{ .bytes = try a.dupe(u8, t.bytes), .quoted = t.quoted });
            if (f.last) {
                try records.append(a, try fields.toOwnedSlice(a));
                if (s.position() >= hi) {
                    if (s.position() != hi and k + 1 < n) return reference.malformed(.inconsistent_quotes, hi);
                    break;
                }
            }
        }
    }
    return .{ .records = try records.toOwnedSlice(a) };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

fn expectSame(want: reference.Result, got: reference.Result) !void {
    switch (want) {
        .malformed => try testing.expect(got == .malformed),
        .records => |wr| {
            if (got == .malformed) {
                std.debug.print("chunked refused at {d} ({s}) where the reference read {d} records\n", .{
                    got.malformed.offset, @tagName(got.malformed.kind), wr.len,
                });
                return error.TestUnexpectedResult;
            }
            const gr = got.records;
            try testing.expectEqual(wr.len, gr.len);
            for (wr, gr) |a, b| {
                try testing.expectEqual(a.len, b.len);
                for (a, b) |x, y| {
                    try testing.expectEqual(x.quoted, y.quoted);
                    try testing.expectEqualStrings(x.bytes, y.bytes);
                }
            }
        },
    }
}

test "csv the scanner splits RFC 4180 fields: quotes, doubled quotes, embedded CRLF, empty vs quoted empty" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const csv = "id,s\r\n1,\"a,b\"\r\n2,\"say \"\"hi\"\"\"\r\n3,\"\"\r\n4,\r\n5,\"l1\r\nl2\"\r\n6,end";
    const r = try parseChunked(a, csv, .{}, &.{});
    const recs = r.records;
    try testing.expectEqual(@as(usize, 7), recs.len);
    try testing.expectEqualStrings("a,b", recs[1][1].bytes);
    try testing.expectEqualStrings("say \"hi\"", recs[2][1].bytes);
    try testing.expect(recs[3][1].quoted and recs[3][1].bytes.len == 0);
    try testing.expect(!recs[4][1].quoted and recs[4][1].bytes.len == 0);
    try testing.expectEqualStrings("l1\r\nl2", recs[5][1].bytes);
    try testing.expectEqualStrings("end", recs[6][1].bytes);
    try expectSame(try reference.parse(a, csv, .{}), r);
}

test "csv malformed fields are refused at their byte, by the scanner and the reference alike" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_]struct { csv: []const u8, kind: Malformed }{
        .{ .csv = "a,b\n1,ab\"c\n", .kind = .quote_in_unquoted_field },
        .{ .csv = "a,b\n1,\"ab\"c\n", .kind = .text_after_quote },
        .{ .csv = "a,b\n1,\"open\n2,x\n", .kind = .unterminated_quote },
    };
    for (cases) |c| {
        const want = try reference.parse(a, c.csv, .{});
        try testing.expectEqual(c.kind, want.malformed.kind);
        const got = try parseChunked(a, c.csv, .{}, &.{});
        try testing.expect(got == .malformed);
    }
}

test "csv recordStart finds the first unquoted newline across 64-byte blocks" {
    var buf: [300]u8 = undefined;
    @memset(&buf, 'x');
    buf[10] = '"';
    buf[100] = '\n'; // quoted
    buf[150] = '"';
    buf[200] = '\n';
    try testing.expectEqual(@as(?usize, 201), recordStart(&buf, 0, false, '"'));
    // From inside the quotes the newline at 100 is data; from outside it ends the record.
    try testing.expectEqual(@as(?usize, 201), recordStart(&buf, 11, true, '"'));
    try testing.expectEqual(@as(?usize, 101), recordStart(&buf, 11, false, '"'));
    try testing.expectEqual(@as(?usize, null), recordStart(buf[0..150], 0, false, '"'));
}

test "csv the delimiter sniff takes the candidate with the most fields, quote-aware" {
    try testing.expectEqual(@as(u8, ','), sniffDelimiter("a,b,c\n1,2,3\n4,5\n7,8,9\n", 0, '"'));
    try testing.expectEqual(@as(u8, '\t'), sniffDelimiter("a\tb\n\"x,y\"\t2\n", 0, '"'));
    try testing.expectEqual(@as(u8, ';'), sniffDelimiter("a;b;c\n1,5;2;3\n", 0, '"'));
    try testing.expectEqual(@as(usize, 3), try bomLen("\xef\xbb\xbfa\n"));
    try testing.expectError(error.Utf16, bomLen("\xff\xfea\x00"));
}

/// A random CSV-ish byte string over the bytes that matter (quotes, delimiters, CR, LF, text), biased towards
/// well-formed records so both accepted and refused inputs are common.
fn randomCsv(rng: std.Random, buf: []u8) []u8 {
    var n: usize = 0;
    const records = rng.intRangeAtMost(usize, 0, 12);
    for (0..records) |_| {
        const fields = rng.intRangeAtMost(usize, 1, 4);
        for (0..fields) |fi| {
            if (fi > 0 and n < buf.len) {
                buf[n] = ',';
                n += 1;
            }
            const quoted = rng.boolean();
            if (quoted and n < buf.len) {
                buf[n] = '"';
                n += 1;
            }
            const len = rng.intRangeAtMost(usize, 0, 9);
            for (0..len) |_| {
                if (n >= buf.len) break;
                const alphabet = if (quoted) "ab,\n\r\"\"x" else "ab x\r";
                buf[n] = alphabet[rng.uintLessThan(usize, alphabet.len)];
                // Inside quotes a quote comes as a doubled pair unless the input is meant to be broken.
                if (buf[n] == '"' and n + 1 < buf.len and rng.uintLessThan(u8, 8) != 0) {
                    n += 1;
                    buf[n] = '"';
                }
                n += 1;
            }
            if (quoted and n < buf.len and rng.uintLessThan(u8, 16) != 0) {
                buf[n] = '"';
                n += 1;
            }
        }
        if (n < buf.len) {
            if (rng.boolean() and n + 1 < buf.len) {
                buf[n] = '\r';
                n += 1;
            }
            buf[n] = '\n';
            n += 1;
        }
    }
    // Sometimes noise: any of the structural bytes anywhere.
    if (n > 0 and rng.uintLessThan(u8, 4) == 0) buf[rng.uintLessThan(usize, n)] = "\",\n\r"[rng.uintLessThan(usize, 4)];
    return buf[0..n];
}

fn checkChunkedAgrees(a: std.mem.Allocator, csv: []const u8, rng: std.Random) !void {
    const want = try reference.parse(a, csv, .{});
    var cuts: [6]usize = undefined;
    const ncuts = rng.uintAtMost(usize, cuts.len);
    for (cuts[0..ncuts]) |*c| c.* = if (csv.len == 0) 0 else rng.uintLessThan(usize, csv.len);
    std.mem.sort(usize, cuts[0..ncuts], {}, std.sort.asc(usize));
    try expectSame(want, try parseChunked(a, csv, .{}, cuts[0..ncuts]));
}

test "csv property: chunked parsing at random cuts reads what the reference reads, refuses what it refuses" {
    var prng = std.Random.DefaultPrng.init(0x5eed_c5f);
    const rng = prng.random();
    var buf: [512]u8 = undefined;
    for (0..20_000) |_| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        try checkChunkedAgrees(arena.allocator(), randomCsv(rng, &buf), rng);
    }
}

test "csv property: long quoted fields across many 64-byte blocks and cuts" {
    var prng = std.Random.DefaultPrng.init(0xb10c);
    const rng = prng.random();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for (0..200) |_| {
        var text: std.ArrayList(u8) = .empty;
        for (0..rng.intRangeAtMost(usize, 1, 30)) |_| {
            try text.appendSlice(a, "1,\"");
            for (0..rng.intRangeAtMost(usize, 0, 300)) |_| try text.append(a, "ab\n,\r xy"[rng.uintLessThan(usize, 8)]);
            if (rng.boolean()) try text.appendSlice(a, "\"\"");
            try text.appendSlice(a, "\",z\n");
        }
        try checkChunkedAgrees(a, text.items, rng);
    }
}

fn fuzzChunked(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [2048]u8 = undefined;
    const csv = buf[0..smith.slice(&buf)];
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var prng = std.Random.DefaultPrng.init(csv.len);
    try checkChunkedAgrees(arena.allocator(), csv, prng.random());
}

test "fuzz: csv chunked framing agrees with the byte-at-a-time reference" {
    try std.testing.fuzz({}, fuzzChunked, .{ .corpus = &.{
        "a,b\n1,\"x\"\"y\"\n",
        "\"a\nb\",c\r\n\"\",\n",
        "x\"y,z\n",
        "\"open,\n1,2\n",
        "\"a\"b,c\n",
    } });
}
