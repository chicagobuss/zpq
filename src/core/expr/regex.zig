//! Regular expressions in RE2's dialect, which DuckDB's `regexp_*` functions speak, matched in linear time: no
//! backreferences or lookaround, leftmost-first alternation, `.` excluding `\n` unless `(?s)`, `^`/`$` at the text's
//! ends unless `(?m)`, and `\d`, `\w`, `\s`, `\b` over ASCII. Text is UTF-8: `.` and classes take one code point, and a
//! byte that starts no valid sequence matches nothing (RE2 compiles to UTF-8 byte automata, which no such byte enters).
//!
//! A pattern compiles to a Thompson program over code points. Short texts run the bounded backtracker RE2 calls
//! BitState: depth-first in priority order, so the first match it reaches is the leftmost-first one, and a bitmap of
//! visited (instruction, position) pairs makes it linear, as a pair that failed once fails again. Texts too long for
//! that bitmap run a Pike VM. What the program implies is precomputed: a literal every match starts with, searched for
//! before matching, and a greedy `.*` ahead of the final `$`, which needs only a scan for `\n`.

const std = @import("std");

pub const Error = error{
    /// A pattern RE2 rejects, or a replacement it rejects (`\` before anything but a digit or `\`, a group the
    /// pattern does not have).
    InvalidRegex,
    /// RE2 syntax zpq does not have: Unicode classes (`\pL`), case-insensitive non-ASCII text, `\C`, `\Q...\E`.
    UnsupportedRegex,
    /// A program past `max_insts` (counted repetitions multiply their body).
    RegexTooBig,
} || std.mem.Allocator.Error;

/// RE2's cap on a counted repetition's bounds.
const max_repeat = 1000;
const max_insts = 1 << 16;
/// A code point no text decodes to: what a byte that starts no valid UTF-8 sequence reads as.
const invalid_cp: u21 = 0x1FFFFF;
const max_cp: u21 = 0x10FFFF;

const Range = struct { lo: u21, hi: u21 };

/// A set of code points: a bitmap below 0x80, sorted disjoint ranges above.
const Class = struct {
    ascii: [2]u64 = .{ 0, 0 },
    ranges: []const Range = &.{},

    inline fn has(c: *const Class, cp: u21) bool {
        if (cp < 0x80) return c.ascii[cp >> 6] >> @intCast(cp & 63) & 1 != 0;
        var lo: usize = 0;
        var hi = c.ranges.len;
        while (lo < hi) {
            const mid = (lo + hi) / 2;
            if (cp < c.ranges[mid].lo) hi = mid else if (cp > c.ranges[mid].hi) lo = mid + 1 else return true;
        }
        return false;
    }
};

const Assert = enum { begin_text, end_text, begin_line, end_line, word, not_word };

const Inst = union(enum) {
    char: u21,
    class: u32,
    /// Try `[0]` first, then `[1]`.
    split: [2]u32,
    jmp: u32,
    save: u32,
    assert: Assert,
    /// A greedy `.*` (`dot_nl`: one that takes `\n` too) whose loop starts at the next instruction and leaves to
    /// `exit`, followed there by `$` at the end of the text. It can only match the whole rest of the text, so the
    /// backtracker takes that in one step; the Pike VM runs the loop.
    rest: struct { dot_nl: bool, exit: u32 },
    /// Any other greedy loop of one class, `split(L+1, exit); L+1: class; jmp L`, with the loop at the next
    /// instruction. The backtracker takes the longest run of the class at once, then tries `exit` from its end back
    /// to its start, which is the order the loop would; the Pike VM runs the loop.
    run: struct { class: u32, exit: u32 },
    match,
};

pub const Regex = struct {
    insts: []const Inst,
    classes: []const Class,
    /// Capture groups, the whole match (group 0) included.
    groups: u32,
    /// Bytes every match starts with.
    prefix: []const u8,
    /// Every match starts at the text's start.
    anchored: bool,

    pub const Flags = struct {
        case_insensitive: bool = false,
        dot_nl: bool = false,
        multi_line: bool = false,
        /// The pattern is its text, every character standing for itself.
        literal: bool = false,
    };

    pub fn compile(arena: std.mem.Allocator, pattern: []const u8, flags: Flags) Error!Regex {
        var p: Parser = .{ .arena = arena, .src = pattern, .flags = flags };
        const node = if (flags.literal) try p.parseLiteral() else try p.parseAlt(0);
        if (p.i != pattern.len) return error.InvalidRegex;
        var c: Compiler = .{ .arena = arena };
        try c.emit(.{ .save = 0 });
        try c.compile(node);
        try c.emit(.{ .save = 1 });
        try c.emit(.match);
        c.markRests();
        const re: Regex = .{
            .insts = c.insts.items,
            .classes = c.classes.items,
            .groups = p.groups + 1,
            .prefix = try c.literalPrefix(),
            .anchored = c.insts.items.len > 1 and std.meta.eql(c.insts.items[1], .{ .assert = .begin_text }),
        };
        return re;
    }

    /// The leftmost-first match at or after `from`, its groups' bounds in `caps` (2 per group, null for a group that
    /// took no part); false when there is none.
    pub fn find(re: *const Regex, s: *Scratch, text: []const u8, from: usize, caps: []?usize) Error!bool {
        std.debug.assert(caps.len == 2 * re.groups);
        // A search from 0 starts a new text; later ones (a global replace's) continue the same one.
        if (from == 0) s.tail = null;
        if (re.anchored and from != 0) return false;
        if ((text.len + 1) * re.insts.len <= bitstate_max_bits) return re.bitState(s, text, from, caps);
        return re.pike(s, text, from, caps);
    }

    /// The next position at or after `pos` a match could start at, by the prefix every match has.
    fn nextStart(re: *const Regex, text: []const u8, pos: usize) ?usize {
        if (pos > text.len) return null;
        if (re.prefix.len == 0) return pos;
        return std.mem.indexOfPos(u8, text, pos, re.prefix);
    }

    // ---- BitState ----

    const bitstate_max_bits = 256 * 1024;

    const Job = union(enum) {
        run: struct { pc: u32, pos: usize },
        restore: struct { slot: u32, old: ?usize },
        /// `exit` still to try at each code point boundary from `hi` down to `lo`, in that order.
        back: struct { exit: u32, lo: usize, hi: usize },
    };

    fn bitState(re: *const Regex, s: *Scratch, text: []const u8, from: usize, caps: []?usize) Error!bool {
        const width = text.len + 1;
        const words = (width * re.insts.len + 63) / 64;
        try s.visited.resize(s.gpa, words);
        @memset(s.visited.items, 0);
        try s.caps.resize(s.gpa, caps.len);
        // Visited pairs are shared across start positions: a pair that failed from one start fails from any.
        var start = from;
        while (re.nextStart(text, start)) |at| {
            @memset(s.caps.items, null);
            s.jobs.clearRetainingCapacity();
            try s.jobs.append(s.gpa, .{ .run = .{ .pc = 0, .pos = at } });
            while (s.jobs.pop()) |job| {
                const pc, const pos = switch (job) {
                    .restore => |r| {
                        s.caps.items[r.slot] = r.old;
                        continue;
                    },
                    .run => |r| .{ r.pc, r.pos },
                    .back => |b| blk: {
                        if (b.hi > b.lo) try s.jobs.append(s.gpa, .{ .back = .{
                            .exit = b.exit,
                            .lo = b.lo,
                            .hi = prevBoundary(text, b.lo, b.hi),
                        } });
                        break :blk .{ b.exit, b.hi };
                    },
                };
                if (try re.bitRun(s, text, pc, pos, width)) {
                    @memcpy(caps, s.caps.items);
                    return true;
                }
            }
            if (re.anchored or at == text.len) return false;
            start = at + decode(text, at).len;
        }
        return false;
    }

    fn bitRun(re: *const Regex, s: *Scratch, text: []const u8, pc0: u32, pos0: usize, width: usize) Error!bool {
        var pc = pc0;
        var pos = pos0;
        while (true) {
            const bit = pc * width + pos;
            const word = &s.visited.items[bit / 64];
            const mask = @as(u64, 1) << @intCast(bit % 64);
            if (word.* & mask != 0) return false;
            word.* |= mask;
            switch (re.insts[pc]) {
                .char => |want| {
                    if (pos == text.len) return false;
                    const d = decode(text, pos);
                    if (d.cp != want) return false;
                    pos += d.len;
                    pc += 1;
                },
                .class => |k| {
                    if (pos == text.len) return false;
                    const d = decode(text, pos);
                    if (!re.classes[k].has(d.cp)) return false;
                    pos += d.len;
                    pc += 1;
                },
                .split => |to| {
                    try s.jobs.append(s.gpa, .{ .run = .{ .pc = to[1], .pos = pos } });
                    pc = to[0];
                },
                .jmp => |to| pc = to,
                .save => |slot| {
                    try s.jobs.append(s.gpa, .{ .restore = .{ .slot = slot, .old = s.caps.items[slot] } });
                    s.caps.items[slot] = pos;
                    pc += 1;
                },
                .assert => |a| {
                    if (!holds(a, text, pos)) return false;
                    pc += 1;
                },
                .run => |r| {
                    // Each (loop, position) the run passes is marked as the loop's own steps would be; one seen
                    // before ends it, as the loop would fail there. The run's code points are valid UTF-8, which is
                    // what lets `back` step over them backwards.
                    const class = &re.classes[r.class];
                    const start = pos;
                    while (pos < text.len) {
                        const d = decode(text, pos);
                        if (!class.has(d.cp)) break;
                        const next = pos + d.len;
                        const b = pc * width + next;
                        const w = &s.visited.items[b / 64];
                        const m = @as(u64, 1) << @intCast(b % 64);
                        if (w.* & m != 0) {
                            // The loop at `next` failed before, and so did every exit it could reach.
                            break;
                        }
                        w.* |= m;
                        pos = next;
                    }
                    // The exit at the run's end is tried now; the shorter runs wait on the stack, longest first.
                    if (pos > start) try s.jobs.append(s.gpa, .{ .back = .{
                        .exit = r.exit,
                        .lo = start,
                        .hi = prevBoundary(text, start, pos),
                    } });
                    pc = r.exit;
                },
                .rest => |r| {
                    // The loop takes the rest only if `.` takes each of its characters: no `\n` unless `(?s)`, and
                    // no byte outside a valid UTF-8 sequence. Both are facts of where the tail starts.
                    const t = s.tailFacts(text);
                    if (pos < t.valid_from or (!r.dot_nl and pos < t.no_nl_from)) return false;
                    pos = text.len;
                    pc = r.exit;
                },
                .match => return true,
            }
        }
    }

    // ---- Pike VM ----

    fn pike(re: *const Regex, s: *Scratch, text: []const u8, from: usize, caps: []?usize) Error!bool {
        const n = re.insts.len;
        const slots = caps.len;
        try s.lists[0].init(s.gpa, n, slots);
        try s.lists[1].init(s.gpa, n, slots);
        try s.caps.resize(s.gpa, slots);
        var clist = &s.lists[0];
        var nlist = &s.lists[1];
        clist.clear();
        var matched = false;
        var pos = from;
        while (true) {
            if (!matched) {
                if (clist.len == 0) pos = re.nextStart(text, pos) orelse break;
                // A new thread at each position, after every older one: an earlier start is leftmost, so preferred.
                // A position the required prefix does not start at would only fail.
                if ((!re.anchored or pos == 0) and std.mem.startsWith(u8, text[pos..], re.prefix)) {
                    @memset(s.caps.items, null);
                    try re.addThread(s, clist, 0, pos, text);
                }
            }
            if (clist.len == 0) break;
            const d: Decoded = if (pos < text.len) decode(text, pos) else .{ .cp = invalid_cp, .len = 0 };
            nlist.clear();
            for (clist.dense[0..clist.len]) |pc| {
                const thread_caps = clist.capsOf(pc);
                switch (re.insts[pc]) {
                    .match => {
                        @memcpy(caps, thread_caps);
                        matched = true;
                        break;
                    },
                    .char => |want| if (d.len != 0 and d.cp == want) {
                        @memcpy(s.caps.items, thread_caps);
                        try re.addThread(s, nlist, pc + 1, pos + d.len, text);
                    },
                    .class => |k| if (d.len != 0 and re.classes[k].has(d.cp)) {
                        @memcpy(s.caps.items, thread_caps);
                        try re.addThread(s, nlist, pc + 1, pos + d.len, text);
                    },
                    // Empty transitions stay in the set only to stop `addThread` revisiting them.
                    else => {},
                }
            }
            if (pos >= text.len) break;
            pos += d.len;
            std.mem.swap(*ThreadList, &clist, &nlist);
        }
        return matched;
    }

    /// Follow `pc`'s empty transitions at `pos` in priority order, adding each consuming or matching instruction
    /// reached with `s.caps` as its groups. `s.caps` is left as it was.
    fn addThread(re: *const Regex, s: *Scratch, list: *ThreadList, pc0: u32, pos: usize, text: []const u8) Error!void {
        s.jobs.clearRetainingCapacity();
        try s.jobs.append(s.gpa, .{ .run = .{ .pc = pc0, .pos = pos } });
        while (s.jobs.pop()) |job| switch (job) {
            .restore => |r| s.caps.items[r.slot] = r.old,
            .back => unreachable,
            .run => |r| {
                var pc = r.pc;
                while (!list.contains(pc)) {
                    list.insert(pc);
                    switch (re.insts[pc]) {
                        .jmp => |to| pc = to,
                        .split => |to| {
                            try s.jobs.append(s.gpa, .{ .run = .{ .pc = to[1], .pos = pos } });
                            pc = to[0];
                        },
                        .save => |slot| {
                            try s.jobs.append(s.gpa, .{ .restore = .{ .slot = slot, .old = s.caps.items[slot] } });
                            s.caps.items[slot] = pos;
                            pc += 1;
                        },
                        .assert => |a| if (holds(a, text, pos)) {
                            pc += 1;
                        } else break,
                        inline .rest, .run => |loop| {
                            try s.jobs.append(s.gpa, .{ .run = .{ .pc = loop.exit, .pos = pos } });
                            pc += 1;
                        },
                        .char, .class, .match => {
                            @memcpy(list.capsOf(pc), s.caps.items);
                            break;
                        },
                    }
                }
            },
        };
    }
};

/// A sparse set of instructions in insertion (priority) order, each with its thread's groups.
const ThreadList = struct {
    sparse: []u32 = &.{},
    dense: []u32 = &.{},
    slots: []?usize = &.{},
    width: usize = 0,
    len: usize = 0,

    fn init(l: *ThreadList, gpa: std.mem.Allocator, n: usize, width: usize) !void {
        if (l.sparse.len < n or l.width != width) {
            l.deinit(gpa);
            l.sparse = try gpa.alloc(u32, n);
            l.dense = try gpa.alloc(u32, n);
            l.slots = try gpa.alloc(?usize, n * width);
            l.width = width;
        }
        l.len = 0;
    }

    fn deinit(l: *ThreadList, gpa: std.mem.Allocator) void {
        gpa.free(l.sparse);
        gpa.free(l.dense);
        gpa.free(l.slots);
        l.* = .{};
    }

    fn clear(l: *ThreadList) void {
        l.len = 0;
    }

    fn contains(l: *const ThreadList, pc: u32) bool {
        const i = l.sparse[pc];
        return i < l.len and l.dense[i] == pc;
    }

    fn insert(l: *ThreadList, pc: u32) void {
        l.sparse[pc] = @intCast(l.len);
        l.dense[l.len] = pc;
        l.len += 1;
    }

    fn capsOf(l: *ThreadList, pc: u32) []?usize {
        return l.slots[pc * l.width ..][0..l.width];
    }
};

/// A matcher's working memory, reused from text to text. One per thread: a compiled `Regex` is shared read-only.
pub const Scratch = struct {
    gpa: std.mem.Allocator,
    visited: std.ArrayList(u64) = .empty,
    jobs: std.ArrayList(Regex.Job) = .empty,
    caps: std.ArrayList(?usize) = .empty,
    match: std.ArrayList(?usize) = .empty,
    lists: [2]ThreadList = .{ .{}, .{} },
    /// `tailFacts` of the text being searched, so a search's many tails, and a global replace's many searches, scan
    /// it once. `find` from 0 clears it.
    tail: ?struct { ptr: [*]const u8, len: usize, facts: TailFacts } = null,

    const TailFacts = struct {
        /// Every tail starting here or later holds no `\n`.
        no_nl_from: usize,
        /// Every tail starting at a code point boundary here or later is valid UTF-8. Matching positions are the
        /// boundaries decoding from the text's start gives, so this is the end of its last invalid byte.
        valid_from: usize,
    };

    fn tailFacts(s: *Scratch, text: []const u8) TailFacts {
        if (s.tail) |t| if (t.ptr == text.ptr and t.len == text.len) return t.facts;
        var valid_from: usize = 0;
        var i: usize = 0;
        while (i < text.len) {
            const d = decode(text, i);
            i += d.len;
            if (d.cp == invalid_cp) valid_from = i;
        }
        const facts: TailFacts = .{
            .no_nl_from = if (std.mem.lastIndexOfScalar(u8, text, '\n')) |nl| nl + 1 else 0,
            .valid_from = valid_from,
        };
        s.tail = .{ .ptr = text.ptr, .len = text.len, .facts = facts };
        return facts;
    }

    pub fn init(gpa: std.mem.Allocator) Scratch {
        return .{ .gpa = gpa };
    }

    pub fn deinit(s: *Scratch) void {
        s.visited.deinit(s.gpa);
        s.jobs.deinit(s.gpa);
        s.caps.deinit(s.gpa);
        s.match.deinit(s.gpa);
        for (&s.lists) |*l| l.deinit(s.gpa);
    }
};

const Decoded = struct { cp: u21, len: u3 };

/// The code point boundary before `pos`, no earlier than `lo`, in valid UTF-8.
fn prevBoundary(text: []const u8, lo: usize, pos: usize) usize {
    var p = pos - 1;
    while (p > lo and text[p] & 0xC0 == 0x80) p -= 1;
    return p;
}

inline fn decode(text: []const u8, pos: usize) Decoded {
    const b = text[pos];
    if (b < 0x80) return .{ .cp = b, .len = 1 };
    const len = std.unicode.utf8ByteSequenceLength(b) catch return .{ .cp = invalid_cp, .len = 1 };
    if (pos + len > text.len) return .{ .cp = invalid_cp, .len = 1 };
    const cp = std.unicode.utf8Decode(text[pos..][0..len]) catch return .{ .cp = invalid_cp, .len = 1 };
    return .{ .cp = cp, .len = len };
}

fn isWordByte(b: u8) bool {
    return std.ascii.isAlphanumeric(b) or b == '_';
}

fn holds(a: Assert, text: []const u8, pos: usize) bool {
    return switch (a) {
        .begin_text => pos == 0,
        .end_text => pos == text.len,
        .begin_line => pos == 0 or text[pos - 1] == '\n',
        .end_line => pos == text.len or text[pos] == '\n',
        .word, .not_word => blk: {
            const before = pos > 0 and isWordByte(text[pos - 1]);
            const after = pos < text.len and isWordByte(text[pos]);
            break :blk (before != after) == (a == .word);
        },
    };
}

// ---- Parser ----

const Node = union(enum) {
    empty,
    char: u21,
    class: Class,
    assert: Assert,
    group: struct { node: *const Node, index: ?u32 },
    concat: []const Node,
    alt: []const Node,
    repeat: struct { node: *const Node, min: u32, max: ?u32, greedy: bool },
};

const Parser = struct {
    arena: std.mem.Allocator,
    src: []const u8,
    i: usize = 0,
    flags: Regex.Flags,
    groups: u32 = 0,

    const max_depth = 256;

    fn peek(p: *const Parser) ?u8 {
        return if (p.i < p.src.len) p.src[p.i] else null;
    }

    fn eat(p: *Parser, c: u8) bool {
        if (p.peek() != c) return false;
        p.i += 1;
        return true;
    }

    fn parseLiteral(p: *Parser) Error!Node {
        var items: std.ArrayList(Node) = .empty;
        while (p.i < p.src.len) try items.append(p.arena, try p.literal(try p.nextCodePoint()));
        return .{ .concat = items.items };
    }

    fn parseAlt(p: *Parser, depth: u32) Error!Node {
        if (depth > max_depth) return error.RegexTooBig;
        // Flags set inside a group last to its end.
        const saved = p.flags;
        defer p.flags = saved;
        var alts: std.ArrayList(Node) = .empty;
        while (true) {
            try alts.append(p.arena, try p.parseConcat(depth));
            if (!p.eat('|')) break;
        }
        return if (alts.items.len == 1) alts.items[0] else .{ .alt = alts.items };
    }

    fn parseConcat(p: *Parser, depth: u32) Error!Node {
        var items: std.ArrayList(Node) = .empty;
        while (p.peek()) |c| {
            if (c == '|' or c == ')') break;
            const atom = try p.parseAtom(depth) orelse continue;
            try items.append(p.arena, try p.parseRepeat(atom));
        }
        return switch (items.items.len) {
            0 => .empty,
            1 => items.items[0],
            else => .{ .concat = items.items },
        };
    }

    fn parseRepeat(p: *Parser, atom: Node) Error!Node {
        var node = atom;
        while (p.peek()) |c| {
            var min: u32 = 0;
            var max: ?u32 = null;
            switch (c) {
                '*' => p.i += 1,
                '+' => {
                    p.i += 1;
                    min = 1;
                },
                '?' => {
                    p.i += 1;
                    max = 1;
                },
                '{' => {
                    const saved = p.i;
                    const bounds = p.parseBounds() orelse {
                        // Not a repetition: RE2 reads the brace as a literal.
                        p.i = saved;
                        return node;
                    };
                    min = bounds.min;
                    max = bounds.max;
                },
                else => return node,
            }
            // RE2 rejects a repetition of a repetition (`a**`).
            if (node == .repeat) return error.InvalidRegex;
            const greedy = !p.eat('?');
            const inner = try p.arena.create(Node);
            inner.* = node;
            node = .{ .repeat = .{ .node = inner, .min = min, .max = max, .greedy = greedy } };
        }
        return node;
    }

    fn parseBounds(p: *Parser) ?struct { min: u32, max: ?u32 } {
        p.i += 1;
        const min = p.parseInt() orelse return null;
        var max: ?u32 = min;
        if (p.eat(',')) max = if (p.peek() == '}') null else (p.parseInt() orelse return null);
        if (!p.eat('}')) return null;
        return .{ .min = min, .max = max };
    }

    fn parseInt(p: *Parser) ?u32 {
        const start = p.i;
        while (p.peek()) |c| {
            if (!std.ascii.isDigit(c)) break;
            p.i += 1;
        }
        // RE2 takes no leading zero (`a{01}` is literal text) and nothing past 8 digits.
        if (p.i == start or p.i - start > 8 or (p.src[start] == '0' and p.i - start > 1)) return null;
        return std.fmt.parseInt(u32, p.src[start..p.i], 10) catch null;
    }

    /// One atom, or null for a construct that only set flags.
    fn parseAtom(p: *Parser, depth: u32) Error!?Node {
        const c = p.src[p.i];
        p.i += 1;
        switch (c) {
            '(' => return try p.parseGroup(depth),
            '[' => return .{ .class = try p.parseClass() },
            '.' => {
                const nl: []const Range = &.{.{ .lo = '\n', .hi = '\n' }};
                return .{ .class = try p.complement(if (p.flags.dot_nl) &.{} else nl) };
            },
            '^' => return .{ .assert = if (p.flags.multi_line) .begin_line else .begin_text },
            '$' => return .{ .assert = if (p.flags.multi_line) .end_line else .end_text },
            '\\' => return try p.parseEscape(),
            '*', '+', '?' => return error.InvalidRegex,
            else => {
                p.i -= 1;
                return try p.literal(try p.nextCodePoint());
            },
        }
    }

    fn nextCodePoint(p: *Parser) Error!u21 {
        const d = decode(p.src, p.i);
        if (d.cp == invalid_cp) return error.InvalidRegex;
        p.i += d.len;
        return d.cp;
    }

    fn literal(p: *Parser, cp: u21) Error!Node {
        if (!p.flags.case_insensitive) return .{ .char = cp };
        if (cp >= 0x80) return error.UnsupportedRegex;
        if (!std.ascii.isAlphabetic(@intCast(cp))) return .{ .char = cp };
        var ranges: std.ArrayList(Range) = .empty;
        try ranges.append(p.arena, .{ .lo = cp, .hi = cp });
        try foldOne(p.arena, &ranges, cp);
        return .{ .class = try p.classOf(try normalize(p.arena, ranges.items)) };
    }

    fn parseGroup(p: *Parser, depth: u32) Error!?Node {
        var index: ?u32 = null;
        if (p.eat('?')) {
            if (p.eat('P') or p.peek() == '<') {
                // `(?P<name>...)` and `(?<name>...)`: a numbered group, its name unused.
                if (!p.eat('<')) return error.InvalidRegex;
                const end = std.mem.indexOfScalarPos(u8, p.src, p.i, '>') orelse return error.InvalidRegex;
                if (end == p.i) return error.InvalidRegex;
                for (p.src[p.i..end]) |b| if (!isWordByte(b)) return error.InvalidRegex;
                p.i = end + 1;
                p.groups += 1;
                index = p.groups;
            } else {
                var on = true;
                var flags = p.flags;
                var any = false;
                while (p.peek()) |c| {
                    p.i += 1;
                    switch (c) {
                        'i' => flags.case_insensitive = on,
                        's' => flags.dot_nl = on,
                        'm' => flags.multi_line = on,
                        '-' => {
                            if (!on) return error.InvalidRegex;
                            on = false;
                            any = false;
                            continue;
                        },
                        ':' => {
                            if (!on and !any) return error.InvalidRegex;
                            const saved = p.flags;
                            p.flags = flags;
                            const inner = try p.parseAlt(depth + 1);
                            p.flags = saved;
                            if (!p.eat(')')) return error.InvalidRegex;
                            return inner;
                        },
                        ')' => {
                            if (!on and !any) return error.InvalidRegex;
                            p.flags = flags;
                            return null;
                        },
                        'U' => return error.UnsupportedRegex,
                        else => return error.InvalidRegex,
                    }
                    any = true;
                }
                return error.InvalidRegex;
            }
        } else {
            p.groups += 1;
            index = p.groups;
        }
        const inner = try p.arena.create(Node);
        inner.* = try p.parseAlt(depth + 1);
        if (!p.eat(')')) return error.InvalidRegex;
        return .{ .group = .{ .node = inner, .index = index } };
    }

    fn parseEscape(p: *Parser) Error!Node {
        const c = p.peek() orelse return error.InvalidRegex;
        p.i += 1;
        switch (c) {
            'A' => return .{ .assert = .begin_text },
            'z' => return .{ .assert = .end_text },
            'b' => return .{ .assert = .word },
            'B' => return .{ .assert = .not_word },
            'd', 'D', 'w', 'W', 's', 'S' => return .{ .class = try p.perlClass(c) },
            'p', 'P', 'C', 'Q', 'E', 'X', 'R' => return error.UnsupportedRegex,
            '8', '9' => return error.InvalidRegex,
            else => {
                p.i -= 1;
                return try p.literal(try p.escapedCodePoint());
            },
        }
    }

    /// The code point an escape outside the Perl classes stands for, the backslash already read.
    fn escapedCodePoint(p: *Parser) Error!u21 {
        const c = p.peek() orelse return error.InvalidRegex;
        p.i += 1;
        return switch (c) {
            'n' => '\n',
            't' => '\t',
            'r' => '\r',
            'f' => 0x0C,
            'v' => 0x0B,
            'a' => 0x07,
            '0'...'7' => blk: {
                // Octal, at most three digits in all. A lone nonzero digit would be a backreference, which RE2
                // refuses.
                var v: u21 = c - '0';
                if (c != '0' and (p.peek() orelse 0) -% '0' > 7) return error.InvalidRegex;
                var n: usize = 0;
                while (n < 2) : (n += 1) {
                    const d = p.peek() orelse break;
                    if (d < '0' or d > '7') break;
                    v = v * 8 + (d - '0');
                    p.i += 1;
                }
                break :blk v;
            },
            'x' => blk: {
                if (p.eat('{')) {
                    const end = std.mem.indexOfScalarPos(u8, p.src, p.i, '}') orelse return error.InvalidRegex;
                    if (end == p.i or end - p.i > 8) return error.InvalidRegex;
                    for (p.src[p.i..end]) |h| if (!std.ascii.isHex(h)) return error.InvalidRegex;
                    const v = std.fmt.parseInt(u32, p.src[p.i..end], 16) catch return error.InvalidRegex;
                    p.i = end + 1;
                    if (v > max_cp) return error.InvalidRegex;
                    break :blk @intCast(v);
                }
                if (p.i + 2 > p.src.len or !std.ascii.isHex(p.src[p.i]) or !std.ascii.isHex(p.src[p.i + 1]))
                    return error.InvalidRegex;
                const v = std.fmt.parseInt(u8, p.src[p.i..][0..2], 16) catch return error.InvalidRegex;
                p.i += 2;
                break :blk v;
            },
            else => if (c < 0x80 and !std.ascii.isAlphanumeric(c)) c else error.InvalidRegex,
        };
    }

    fn parseClass(p: *Parser) Error!Class {
        const negated = p.eat('^');
        var ranges: std.ArrayList(Range) = .empty;
        var parts: std.ArrayList(Range) = .empty;
        var first = true;
        while (true) {
            const c = p.peek() orelse return error.InvalidRegex;
            if (c == ']' and !first) {
                p.i += 1;
                break;
            }
            first = false;
            if (c == '[' and p.i + 1 < p.src.len and p.src[p.i + 1] == ':') {
                try p.posixClass(&parts);
                continue;
            }
            const lo = try p.classCodePoint(&parts) orelse continue;
            var hi = lo;
            if (p.peek() == '-' and p.i + 1 < p.src.len and p.src[p.i + 1] != ']') {
                p.i += 1;
                hi = try p.classCodePoint(&parts) orelse return error.InvalidRegex;
                if (hi < lo) return error.InvalidRegex;
            }
            try ranges.append(p.arena, .{ .lo = lo, .hi = hi });
        }
        // Literal members fold here; a Perl or POSIX member arrives folded, as it must fold before its own `^`.
        if (p.flags.case_insensitive) try foldRanges(p.arena, &ranges);
        try ranges.appendSlice(p.arena, parts.items);
        const set = try normalize(p.arena, ranges.items);
        return p.classOf(if (negated) try invert(p.arena, set) else set);
    }

    /// The members of a Perl (`\w`) or POSIX (`[:alpha:]`) class: its positive set folded under `(?i)`, then
    /// complemented if negated, as RE2 builds them.
    fn namedSet(p: *Parser, positive: []const Range, negated: bool) Error![]const Range {
        var set: std.ArrayList(Range) = .empty;
        try set.appendSlice(p.arena, positive);
        if (p.flags.case_insensitive) try foldRanges(p.arena, &set);
        const folded = try normalize(p.arena, set.items);
        return if (negated) invert(p.arena, folded) else folded;
    }

    /// One class member: a code point, or null after adding a Perl class (`\d`) to `ranges` whole.
    fn classCodePoint(p: *Parser, ranges: *std.ArrayList(Range)) Error!?u21 {
        if (!p.eat('\\')) return try p.nextCodePoint();
        const c = p.peek() orelse return error.InvalidRegex;
        switch (c) {
            'd', 'D', 'w', 'W', 's', 'S' => {
                p.i += 1;
                try ranges.appendSlice(p.arena, try p.namedSet(perlRanges(c), std.ascii.isUpper(c)));
                return null;
            },
            'p', 'P' => return error.UnsupportedRegex,
            else => return try p.escapedCodePoint(),
        }
    }

    fn posixClass(p: *Parser, ranges: *std.ArrayList(Range)) Error!void {
        const end = std.mem.indexOfPos(u8, p.src, p.i + 2, ":]") orelse return error.InvalidRegex;
        var name = p.src[p.i + 2 .. end];
        p.i = end + 2;
        const negated = name.len > 0 and name[0] == '^';
        if (negated) name = name[1..];
        const table = [_]struct { []const u8, []const Range }{
            .{ "alnum", &.{ .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'Z' }, .{ .lo = 'a', .hi = 'z' } } },
            .{ "alpha", &.{ .{ .lo = 'A', .hi = 'Z' }, .{ .lo = 'a', .hi = 'z' } } },
            .{ "ascii", &.{.{ .lo = 0, .hi = 0x7F }} },
            .{ "blank", &.{ .{ .lo = '\t', .hi = '\t' }, .{ .lo = ' ', .hi = ' ' } } },
            .{ "cntrl", &.{ .{ .lo = 0, .hi = 0x1F }, .{ .lo = 0x7F, .hi = 0x7F } } },
            .{ "digit", &.{.{ .lo = '0', .hi = '9' }} },
            .{ "graph", &.{.{ .lo = '!', .hi = '~' }} },
            .{ "lower", &.{.{ .lo = 'a', .hi = 'z' }} },
            .{ "print", &.{.{ .lo = ' ', .hi = '~' }} },
            .{ "punct", &.{
                .{ .lo = '!', .hi = '/' }, .{ .lo = ':', .hi = '@' },
                .{ .lo = '[', .hi = '`' }, .{ .lo = '{', .hi = '~' },
            } },
            .{ "space", &.{ .{ .lo = '\t', .hi = 0x0D }, .{ .lo = ' ', .hi = ' ' } } },
            .{ "upper", &.{.{ .lo = 'A', .hi = 'Z' }} },
            .{ "word", word_ranges },
            .{ "xdigit", &.{ .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'F' }, .{ .lo = 'a', .hi = 'f' } } },
        };
        for (table) |entry| if (std.mem.eql(u8, entry[0], name)) {
            try ranges.appendSlice(p.arena, try p.namedSet(entry[1], negated));
            return;
        };
        return error.InvalidRegex;
    }

    fn perlClass(p: *Parser, c: u8) Error!Class {
        return p.classOf(try p.namedSet(perlRanges(c), std.ascii.isUpper(c)));
    }

    fn complement(p: *Parser, excluded: []const Range) Error!Class {
        return p.classOf(try invert(p.arena, excluded));
    }

    fn classOf(p: *Parser, ranges: []const Range) Error!Class {
        var c: Class = .{};
        var high: std.ArrayList(Range) = .empty;
        for (ranges) |r| {
            var lo = r.lo;
            while (lo <= r.hi and lo < 0x80) : (lo += 1) c.ascii[lo >> 6] |= @as(u64, 1) << @intCast(lo & 63);
            if (r.hi >= 0x80) try high.append(p.arena, .{ .lo = @max(r.lo, 0x80), .hi = r.hi });
        }
        c.ranges = high.items;
        return c;
    }
};

const word_ranges: []const Range = &.{
    .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'Z' }, .{ .lo = '_', .hi = '_' }, .{ .lo = 'a', .hi = 'z' },
};

/// The positive sets of `\d`, `\w` and `\s`, over ASCII as in RE2 (`\s` has no `\v`).
fn perlRanges(c: u8) []const Range {
    return switch (std.ascii.toLower(c)) {
        'd' => &.{.{ .lo = '0', .hi = '9' }},
        'w' => word_ranges,
        's' => &.{ .{ .lo = '\t', .hi = '\n' }, .{ .lo = 0x0C, .hi = '\r' }, .{ .lo = ' ', .hi = ' ' } },
        else => unreachable,
    };
}

/// Sorted, merged ranges.
fn normalize(arena: std.mem.Allocator, ranges: []const Range) Error![]const Range {
    const sorted = try arena.dupe(Range, ranges);
    std.mem.sort(Range, sorted, {}, struct {
        fn lt(_: void, a: Range, b: Range) bool {
            return a.lo < b.lo;
        }
    }.lt);
    var out: std.ArrayList(Range) = .empty;
    for (sorted) |r| {
        if (out.items.len > 0 and r.lo <= out.items[out.items.len - 1].hi +| 1) {
            const last = &out.items[out.items.len - 1];
            last.hi = @max(last.hi, r.hi);
        } else try out.append(arena, r);
    }
    return out.items;
}

/// Every code point not in `set` (sorted and merged).
fn invert(arena: std.mem.Allocator, set: []const Range) Error![]const Range {
    const sorted = try normalize(arena, set);
    var out: std.ArrayList(Range) = .empty;
    var next: u21 = 0;
    for (sorted) |r| {
        if (r.lo > next) try out.append(arena, .{ .lo = next, .hi = r.lo - 1 });
        next = r.hi +| 1;
    }
    if (next <= max_cp) try out.append(arena, .{ .lo = next, .hi = max_cp });
    return out.items;
}

/// Add each ASCII letter's other case, and the two non-ASCII code points RE2 folds onto ASCII letters (KELVIN SIGN
/// onto k, LONG S onto s). Other non-ASCII members are refused, as folding them needs Unicode's case tables, unless
/// a range holds all of them (a negated ASCII class), which folding cannot grow.
fn foldRanges(arena: std.mem.Allocator, ranges: *std.ArrayList(Range)) Error!void {
    const n = ranges.items.len;
    for (0..n) |i| {
        const r = ranges.items[i];
        if (r.hi >= 0x80 and !(r.lo <= 0x80 and r.hi == max_cp)) return error.UnsupportedRegex;
        var cp = r.lo;
        while (cp <= @min(r.hi, 0x7F)) : (cp += 1) try foldOne(arena, ranges, cp);
    }
}

fn foldOne(arena: std.mem.Allocator, ranges: *std.ArrayList(Range), cp: u21) Error!void {
    if (cp >= 0x80 or !std.ascii.isAlphabetic(@intCast(cp))) return;
    const b: u8 = @intCast(cp);
    const other: u21 = if (std.ascii.isUpper(b)) std.ascii.toLower(b) else std.ascii.toUpper(b);
    try ranges.append(arena, .{ .lo = other, .hi = other });
    switch (std.ascii.toLower(b)) {
        'k' => try ranges.append(arena, .{ .lo = 0x212A, .hi = 0x212A }),
        's' => try ranges.append(arena, .{ .lo = 0x017F, .hi = 0x017F }),
        else => {},
    }
}

// ---- Compiler ----

const Compiler = struct {
    arena: std.mem.Allocator,
    insts: std.ArrayList(Inst) = .empty,
    classes: std.ArrayList(Class) = .empty,

    fn emit(c: *Compiler, inst: Inst) Error!void {
        if (c.insts.items.len >= max_insts) return error.RegexTooBig;
        try c.insts.append(c.arena, inst);
    }

    fn pc(c: *const Compiler) u32 {
        return @intCast(c.insts.items.len);
    }

    fn compile(c: *Compiler, node: Node) Error!void {
        switch (node) {
            .empty => {},
            .char => |cp| try c.emit(.{ .char = cp }),
            .class => |class| {
                try c.classes.append(c.arena, class);
                try c.emit(.{ .class = @intCast(c.classes.items.len - 1) });
            },
            .assert => |a| try c.emit(.{ .assert = a }),
            .group => |g| {
                if (g.index) |k| try c.emit(.{ .save = 2 * k });
                try c.compile(g.node.*);
                if (g.index) |k| try c.emit(.{ .save = 2 * k + 1 });
            },
            .concat => |items| for (items) |item| try c.compile(item),
            .alt => |alts| {
                // split L1, next; L1: a; jmp end; next: split L2, ... ; last alternative falls through.
                var jumps: std.ArrayList(u32) = .empty;
                for (alts, 0..) |alt, i| {
                    const last = i + 1 == alts.len;
                    const split_at = c.pc();
                    if (!last) try c.emit(.{ .split = .{ split_at + 1, 0 } });
                    try c.compile(alt);
                    if (!last) {
                        try jumps.append(c.arena, c.pc());
                        try c.emit(.{ .jmp = 0 });
                        c.insts.items[split_at].split[1] = c.pc();
                    }
                }
                for (jumps.items) |j| c.insts.items[j] = .{ .jmp = c.pc() };
            },
            .repeat => |r| {
                if (r.min > max_repeat or (r.max != null and (r.max.? > max_repeat or r.max.? < r.min)))
                    return error.InvalidRegex;
                for (0..r.min) |_| try c.compile(r.node.*);
                if (r.max) |max| {
                    // Each optional copy nests inside the one before: `x{1,3}` is `x(x(x)?)?`.
                    var exits: std.ArrayList(u32) = .empty;
                    for (r.min..max) |_| {
                        try exits.append(c.arena, c.pc());
                        try c.emit(.{ .split = .{ 0, 0 } });
                        try c.compile(r.node.*);
                    }
                    for (exits.items) |at| c.patchSplit(at, at + 1, c.pc(), r.greedy);
                } else {
                    const loop = c.pc();
                    try c.emit(.{ .split = .{ 0, 0 } });
                    try c.compile(r.node.*);
                    try c.emit(.{ .jmp = loop });
                    c.patchSplit(loop, loop + 1, c.pc(), r.greedy);
                }
            },
        }
    }

    fn patchSplit(c: *Compiler, at: u32, body: u32, out: u32, greedy: bool) void {
        c.insts.items[at] = .{ .split = if (greedy) .{ body, out } else .{ out, body } };
    }

    /// Turn each greedy one-class loop, `split(L+1, E); L+1: class; jmp L`, into a `run`, or a `rest` when the class
    /// is `.` and the end-of-text `$` follows at E, past nothing but `save`s, which consume nothing.
    fn markRests(c: *Compiler) void {
        const insts = c.insts.items;
        for (insts, 0..) |inst, i| {
            if (inst != .split or i + 2 >= insts.len) continue;
            const to = inst.split;
            if (to[0] != i + 1 or to[1] != i + 3) continue;
            if (insts[i + 1] != .class or insts[i + 2] != .jmp or insts[i + 2].jmp != i) continue;
            const class = &c.classes.items[insts[i + 1].class];
            const dot_nl = isAny(class, true);
            // The loop's split becomes the fast step, which the Pike VM still runs as that split.
            insts[i] = .{ .run = .{ .class = insts[i + 1].class, .exit = to[1] } };
            if (!dot_nl and !isAny(class, false)) continue;
            var e = to[1];
            while (insts[e] == .save) e += 1;
            if (insts[e] != .assert or insts[e].assert != .end_text) continue;
            insts[i] = .{ .rest = .{ .dot_nl = dot_nl, .exit = to[1] } };
        }
    }

    /// Whether `class` is `.` (`dot_nl`: `(?s:.)`, every code point).
    fn isAny(class: *const Class, dot_nl: bool) bool {
        const ascii_all = ~@as(u64, 0);
        const nl_bit = @as(u64, 1) << '\n';
        if (class.ascii[1] != ascii_all) return false;
        if (class.ascii[0] != if (dot_nl) ascii_all else ascii_all & ~nl_bit) return false;
        return class.ranges.len == 1 and class.ranges[0].lo == 0x80 and class.ranges[0].hi == max_cp;
    }

    /// The UTF-8 bytes every match starts with: the literal characters at the program's start, past `save`s and a
    /// text-start anchor.
    fn literalPrefix(c: *const Compiler) Error![]const u8 {
        var out: std.ArrayList(u8) = .empty;
        var i: usize = 0;
        const insts = c.insts.items;
        while (i < insts.len) : (i += 1) switch (insts[i]) {
            .save => {},
            .assert => |a| if (a != .begin_text or out.items.len > 0) break,
            .char => |cp| {
                var buf: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(cp, &buf) catch break;
                try out.appendSlice(c.arena, buf[0..n]);
            },
            else => break,
        };
        return out.items;
    }
};

// ---- Replacement ----

/// `regexp_replace(text, pattern, replacement[, options])`: the first match (each, with `g`) replaced by the
/// replacement, where `\0`-`\9` insert a group and `\\` a backslash; a text without a match is returned as it is.
pub const Replace = struct {
    re: Regex,
    pieces: []const Piece,
    global: bool,

    const Piece = union(enum) { text: []const u8, group: u32 };

    pub fn compile(
        arena: std.mem.Allocator,
        pattern: []const u8,
        replacement: []const u8,
        options: []const u8,
    ) Error!Replace {
        var flags: Regex.Flags = .{};
        var global = false;
        // DuckDB's option letters (regexp_util.cpp), the last of a pair winning. `m`, `n` and `p` are about `.` and
        // `\n`, not RE2's multi-line `(?m)`; `k` belongs to regexp_extract.
        for (options) |o| switch (o) {
            'g' => global = true,
            'i' => flags.case_insensitive = true,
            'c' => flags.case_insensitive = false,
            's' => flags.dot_nl = true,
            'm', 'n', 'p' => flags.dot_nl = false,
            'l' => flags.literal = true,
            ' ', '\t', '\n' => {},
            else => return error.InvalidRegex,
        };
        const re = try Regex.compile(arena, pattern, flags);
        var pieces: std.ArrayList(Piece) = .empty;
        var start: usize = 0;
        var i: usize = 0;
        while (i < replacement.len) : (i += 1) {
            if (replacement[i] != '\\') continue;
            if (i + 1 == replacement.len) return error.InvalidRegex;
            if (i > start) try pieces.append(arena, .{ .text = replacement[start..i] });
            const n = replacement[i + 1];
            if (n == '\\') {
                try pieces.append(arena, .{ .text = "\\" });
            } else if (std.ascii.isDigit(n)) {
                const g = n - '0';
                if (g >= re.groups) return error.InvalidRegex;
                try pieces.append(arena, .{ .group = g });
            } else return error.InvalidRegex;
            i += 1;
            start = i + 1;
        }
        if (start < replacement.len) try pieces.append(arena, .{ .text = replacement[start..] });
        return .{ .re = re, .pieces = pieces.items, .global = global };
    }

    /// Append `text` with its matches replaced to `out` and return true; or return false, appending nothing, when
    /// nothing matched (the answer is `text` itself).
    pub fn apply(r: *const Replace, s: *Scratch, out: *std.ArrayList(u8), text: []const u8) Error!bool {
        try s.match.resize(s.gpa, 2 * r.re.groups);
        const big_caps = s.match.items;
        var pos: usize = 0;
        var copied: usize = 0;
        var last_end: ?usize = null;
        var any = false;
        while (pos <= text.len) {
            if (!try r.re.find(s, text, pos, big_caps)) break;
            const m0 = big_caps[0].?;
            const m1 = big_caps[1].?;
            // RE2's global rule: no empty match where the previous match ended; step one character past it.
            if (m0 == m1 and last_end != null and m0 == last_end.?) {
                if (m0 >= text.len) break;
                pos = m0 + decode(text, m0).len;
                continue;
            }
            any = true;
            try out.appendSlice(s.gpa, text[copied..m0]);
            for (r.pieces) |piece| switch (piece) {
                .text => |t| try out.appendSlice(s.gpa, t),
                .group => |g| if (big_caps[2 * g]) |a| try out.appendSlice(s.gpa, text[a..big_caps[2 * g + 1].?]),
            };
            copied = m1;
            last_end = m1;
            if (!r.global) break;
            if (m0 == m1) {
                if (m1 >= text.len) break;
                pos = m1 + decode(text, m1).len;
                try out.appendSlice(s.gpa, text[m1..pos]);
                copied = pos;
            } else pos = m1;
        }
        if (!any) return false;
        try out.appendSlice(s.gpa, text[copied..]);
        return true;
    }
};

const testing = std.testing;

/// ClickBench q28's pattern.
const q28 = "^https?://(?:www\\.)?([^/]+)/.*$";

fn replaced(pattern: []const u8, replacement: []const u8, options: []const u8, text: []const u8) ![]const u8 {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const r = try Replace.compile(arena.allocator(), pattern, replacement, options);
    var s: Scratch = .init(testing.allocator);
    defer s.deinit();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(testing.allocator);
    if (!try r.apply(&s, &out, text)) try out.appendSlice(testing.allocator, text);
    return out.toOwnedSlice(testing.allocator);
}

const Case = struct {
    want: []const u8,
    text: []const u8,
    pattern: []const u8,
    replacement: []const u8,
    options: []const u8 = "",
};

fn expectReplace(t: Case) !void {
    const pattern = t.pattern;
    const replacement = t.replacement;
    const options = t.options;
    const text = t.text;
    const want = t.want;
    const got = try replaced(pattern, replacement, options, text);
    defer testing.allocator.free(got);
    testing.expectEqualStrings(want, got) catch |err| {
        std.debug.print("regexp_replace('{s}', '{s}', '{s}', '{s}')\n", .{ text, pattern, replacement, options });
        return err;
    };
}

test "regexp_replace answers as DuckDB 2.0 (RE2) does" {
    const table = [_]Case{
        .{ .want = "aXcabc", .text = "abcabc", .pattern = "b", .replacement = "X" },
        .{ .want = "aXcaXc", .text = "abcabc", .pattern = "b", .replacement = "X", .options = "g" },
        .{ .want = "-abc", .text = "abc", .pattern = "x*", .replacement = "-" },
        .{ .want = "-a-b-c-", .text = "abc", .pattern = "x*", .replacement = "-", .options = "g" },
        .{ .want = "-a-c-", .text = "abc", .pattern = "b*", .replacement = "-", .options = "g" },
        .{ .want = "-a-c-", .text = "abbc", .pattern = "b*", .replacement = "-", .options = "g" },
        .{ .want = "-a-b-c-", .text = "abc", .pattern = "", .replacement = "-", .options = "g" },
        .{ .want = "E", .text = "", .pattern = "^$", .replacement = "E" },
        .{ .want = "a[bb]c", .text = "abc", .pattern = "(b)", .replacement = "[\\1\\0]" },
        .{ .want = "a\\c", .text = "abc", .pattern = "b", .replacement = "\\\\" },
        .{ .want = "a[]c", .text = "abc", .pattern = "(b)|(z)", .replacement = "[\\2]" },
        .{ .want = "a\nb", .text = "a\nb", .pattern = "^a.b$", .replacement = "X" },
        .{ .want = "X", .text = "a\nb", .pattern = "(?s)^a.b$", .replacement = "X" },
        .{ .want = "ab\n", .text = "ab\n", .pattern = "^ab$", .replacement = "X" },
        .{ .want = "x\nX", .text = "x\nab", .pattern = "(?m)^ab", .replacement = "X" },
        .{ .want = "X", .text = "ab", .pattern = "(?i)AB", .replacement = "X" },
        .{ .want = "AXC", .text = "ABC", .pattern = "b", .replacement = "X", .options = "i" },
        .{ .want = "X", .text = "aé", .pattern = "^a.$", .replacement = "X" },
        .{ .want = "aé", .text = "aé", .pattern = "^a..$", .replacement = "X" },
        .{ .want = "Xaaa", .text = "aaa", .pattern = "a*?", .replacement = "X" },
        .{ .want = "<a>", .text = "aaa", .pattern = "(a|aa)*", .replacement = "<\\1>" },
        .{ .want = "<a><>", .text = "ab", .pattern = "(a)|b", .replacement = "<\\1>", .options = "g" },
        .{ .want = "aXb", .text = "a.b", .pattern = "[.]", .replacement = "X" },
        .{ .want = "aXb", .text = "a]b", .pattern = "[]]", .replacement = "X" },
        .{ .want = "XXb", .text = "a-b", .pattern = "[a-]", .replacement = "X", .options = "g" },
        .{ .want = "D", .text = "5", .pattern = "\\d", .replacement = "D" },
        .{ .want = "é", .text = "é", .pattern = "\\w", .replacement = "W" },
        .{ .want = "W", .text = "_", .pattern = "\\w", .replacement = "W" },
        .{ .want = "Xaa", .text = "aaaa", .pattern = "a{2}", .replacement = "X" },
        .{ .want = "X", .text = "aaaa", .pattern = "a{2,}", .replacement = "X" },
        .{ .want = "Xa", .text = "aaa", .pattern = "a{1,2}", .replacement = "X" },
        .{ .want = "ab", .text = "ab", .pattern = "\\Ab\\z", .replacement = "X" },
        .{ .want = "ab", .text = "ab", .pattern = "\\bb", .replacement = "X" },
        .{ .want = "a X", .text = "a b", .pattern = "\\bb", .replacement = "X" },
        .{ .want = "X", .text = "aé", .pattern = "^a[^x]$", .replacement = "X" },
        .{ .want = "X", .text = "aé", .pattern = "^a[é]$", .replacement = "X" },
        .{ .want = "X", .text = "aéb", .pattern = "^a[à-ê]b$", .replacement = "X" },
        .{ .want = "[a]b", .text = "ab", .pattern = "(?P<n>a)", .replacement = "[\\1]" },
        .{ .want = "Ab", .text = "ab", .pattern = "[[:alpha:]]", .replacement = "A" },
        .{ .want = "X", .text = "\u{212A}", .pattern = "(?i)k", .replacement = "X" },
        .{
            .want = "[]h[]t[]t[]p[]:[]/[]/[]a[b]/[]",
            .text = "http://ab/",
            .pattern = "(b)?",
            .replacement = "[\\1]",
            .options = "g",
        },
        .{ .want = "-ж-ж-", .text = "жж", .pattern = "", .replacement = "-", .options = "g" },
        // Octal escapes take up to three digits; a repetition bound has no leading zero.
        .{ .want = "X", .text = "S", .pattern = "\\123", .replacement = "X" },
        .{ .want = "X", .text = "S", .pattern = "[\\123]", .replacement = "X" },
        .{ .want = "a", .text = "a", .pattern = "a{01}", .replacement = "X" },
        .{ .want = "X", .text = "a{01}", .pattern = "a{01}", .replacement = "X" },
        .{ .want = "X", .text = "a{,2}", .pattern = "a{,2}", .replacement = "X" },
        // A negated Perl or POSIX class folds before it is complemented.
        .{ .want = "a", .text = "a", .pattern = "(?i)[[:^lower:]]", .replacement = "X" },
        .{ .want = "A", .text = "A", .pattern = "(?i)[[:^lower:]]", .replacement = "X" },
        .{ .want = "X", .text = "1", .pattern = "(?i)[[:^lower:]]", .replacement = "X" },
        .{ .want = "\u{212A}", .text = "\u{212A}", .pattern = "(?i)\\W", .replacement = "X" },
        .{ .want = "X", .text = "\u{212A}", .pattern = "(?i)\\w", .replacement = "X" },
        .{ .want = "X", .text = "\u{212A}", .pattern = "(?i)[^\\W]", .replacement = "X" },
        // Option letters: `s` lets `.` take `\n`, `m` does not make `$` multi-line, `l` reads the pattern literally.
        .{ .want = "x", .text = "a\nc", .pattern = "a.c", .replacement = "x", .options = "s" },
        .{ .want = "a\nc", .text = "a\nc", .pattern = "a.c", .replacement = "x", .options = "sn" },
        .{ .want = "abc", .text = "abc", .pattern = "b$", .replacement = "x", .options = "m" },
        .{ .want = "axc", .text = "a.c", .pattern = ".", .replacement = "x", .options = "l" },
        .{ .want = "axc", .text = "abc", .pattern = "B", .replacement = "x", .options = " i " },
        .{ .want = "xXz", .text = "xbz", .pattern = "B", .replacement = "X", .options = "ig" },
        // `[^/]` takes `\n`; only `.` does not.
        .{ .want = "x\ny", .text = "http://x\ny/a", .pattern = q28, .replacement = "\\1" },
        .{ .want = "www.", .text = "http://www./a", .pattern = q28, .replacement = "\\1" },
        // The `.*$` shortcut keeps `.`'s refusal of an invalid byte.
        .{ .want = "http://a/\xff", .text = "http://a/\xff", .pattern = q28, .replacement = "\\1" },
        // An invalid byte is no character: `.` does not take it, but a search steps over it.
        .{ .want = "a\xffb", .text = "a\xffb", .pattern = "^a.b$", .replacement = "X" },
        .{ .want = "\xffX", .text = "\xffb", .pattern = "b", .replacement = "X" },
        // q28's pattern: the host, or the whole text when it does not match (no path, another scheme, a newline).
        .{ .want = "yandex.ru", .text = "https://www.yandex.ru/search?q=1", .pattern = q28, .replacement = "\\1" },
        .{ .want = "wwwx.ru", .text = "http://wwwx.ru/", .pattern = q28, .replacement = "\\1" },
        .{ .want = "http://a.ru", .text = "http://a.ru", .pattern = q28, .replacement = "\\1" },
        .{ .want = "http://a.ru/x\ny", .text = "http://a.ru/x\ny", .pattern = q28, .replacement = "\\1" },
        .{ .want = "ftp://a.ru/", .text = "ftp://a.ru/", .pattern = q28, .replacement = "\\1" },
    };
    for (table) |t| try expectReplace(t);
}

test "regexp_replace refuses what RE2 refuses, and what zpq does not have" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][2][]const u8{
        .{ "b", "\\x" }, .{ "b", "x\\" },     .{ "b", "\\1" }, .{ "a{2,1}", "" },  .{ "a**", "" },   .{ "(a", "" },
        .{ "a)", "" },   .{ "[a", "" },       .{ "*a", "" },   .{ "a{1001}", "" }, .{ "[z-a]", "" }, .{ "(a)\\1", "" },
        .{ "\\8", "" },  .{ "\\x{+61}", "" }, .{ "\\x6", "" }, .{ "\\x{61", "" },
    }) |c|
        try testing.expectError(error.InvalidRegex, Replace.compile(a, c[0], c[1], ""));
    for ([_][]const u8{ "\\pL", "(?i)é", "\\C" }) |pattern|
        try testing.expectError(error.UnsupportedRegex, Replace.compile(a, pattern, "", ""));
    try testing.expectError(error.InvalidRegex, Replace.compile(a, "a", "", "q"));
    try testing.expectError(error.InvalidRegex, Replace.compile(a, "a", "", "k"));
    try testing.expectError(error.RegexTooBig, Replace.compile(a, "((a{1000}){1000}){1000}", "", ""));
}

test "regex analysis: a required prefix, an anchor, and the `.*$` tail" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const re = try Regex.compile(arena.allocator(), "^https?://(?:www\\.)?([^/]+)/.*$", .{});
    try testing.expectEqualStrings("http", re.prefix);
    try testing.expect(re.anchored);
    var rests: usize = 0;
    for (re.insts) |inst| rests += @intFromBool(inst == .rest);
    try testing.expectEqual(@as(usize, 1), rests);
    try testing.expectEqualStrings("", (try Regex.compile(arena.allocator(), "(?i)http", .{})).prefix);
}

test "BitState and the Pike VM find the same leftmost-first match and groups" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var s: Scratch = .init(testing.allocator);
    defer s.deinit();
    var prng: std.Random.DefaultPrng = .init(0x5eed);
    const rand = prng.random();
    const patterns = [_][]const u8{
        "(a|ab)(c|bcd)(d*)", q28,         "(a*)*b",        "(a|b)*?c", "x(y?)+z",     "(?m)^b$",  "\\bab\\b",
        "(a+)(b+)?",         "[^b]+$",    "(?s).*b",       "a{2,3}?b",
        "(é|a)+",
        "(?i)Ab|c",          "",          "(a)|b",         "[a-c]*c",  "([ab]*)(b*)", "x[^x]*?x", "\\w+ ",
        "[^/]*/[^/]*$",      "([^a]*)*a", "(?s)(.*)(.*)d",
        "[^é]+é",
    };
    const alphabet = [_][]const u8{
        "a",    "b", "c", "d", "x", "y", "z", "/", ":", "h", "t", "p", "s", "w", ".", "\n", " ",
        "é",
        "\xff",
    };
    var text_buf: [40]u8 = undefined;
    for (patterns) |pattern| {
        const re = try Regex.compile(a, pattern, .{});
        const want = try a.alloc(?usize, 2 * re.groups);
        const got = try a.alloc(?usize, 2 * re.groups);
        for (0..300) |_| {
            var n: usize = 0;
            while (n + 2 < text_buf.len and rand.uintLessThan(u8, 12) != 0) {
                const piece = alphabet[rand.uintLessThan(usize, alphabet.len)];
                if (n + piece.len > text_buf.len) break;
                @memcpy(text_buf[n..][0..piece.len], piece);
                n += piece.len;
            }
            const text = text_buf[0..n];
            const from = if (n == 0) 0 else rand.uintAtMost(usize, n);
            s.tail = null;
            const by_bits = try re.bitState(&s, text, from, want);
            const by_pike = try re.pike(&s, text, from, got);
            testing.expectEqual(by_bits, by_pike) catch |err| {
                std.debug.print("pattern {s} text {any} from {d}\n", .{ pattern, text, from });
                return err;
            };
            if (by_bits) testing.expectEqualSlices(?usize, want, got) catch |err| {
                std.debug.print("pattern {s} text {any} from {d}\n", .{ pattern, text, from });
                return err;
            };
        }
    }
}

test "a text past BitState's bitmap runs the Pike VM to the same answer" {
    const long = try testing.allocator.alloc(u8, 40_000);
    defer testing.allocator.free(long);
    @memset(long, 'x');
    const tail = "http://www.host.ru/path";
    @memcpy(long[long.len - tail.len ..], tail);
    try expectReplace(.{ .want = "host.ru", .text = tail, .pattern = q28, .replacement = "\\1" });
    const got = try replaced("https?://(?:www\\.)?([^/]+)/.*$", "<\\1>", "", long);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings("<host.ru>", got[got.len - "<host.ru>".len ..]);
    try testing.expectEqual(long.len - tail.len + "<host.ru>".len, got.len);
}

test "the Pike VM keeps an untaken group unset and the leftmost-first match, on texts past BitState's limit" {
    const n = 40_000;
    const text = try testing.allocator.alloc(u8, n + 2);
    defer testing.allocator.free(text);
    @memset(text, 'x');
    text[n] = 'a';
    text[n + 1] = 'b';
    // `(a)|b` takes `a` first; `(?:a|ab)` prefers `a` over the longer `ab`.
    for ([_][3][]const u8{ .{ "(a)|b", "[\\1]", "[a]b" }, .{ "(z)|b", "[\\1]", "a[]" }, .{ "(?:a|ab)", "X", "Xb" } }) |c| {
        const got = try replaced(c[0], c[1], "", text);
        defer testing.allocator.free(got);
        try testing.expectEqualStrings(c[2], got[n..]);
    }
}

test "the `.*$` shortcut reads each text's tail facts once" {
    const text = try testing.allocator.alloc(u8, 20_001);
    defer testing.allocator.free(text);
    @memset(text, 'a');
    text[20_000] = '\n';
    // Unanchored, every start position reaches the tail; a rescan per start would be quadratic.
    const got = try replaced(".*$", "X", "", text);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings("\nX", got[got.len - 2 ..]);
}
