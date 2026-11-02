const std = @import("std");

const Step = union(enum) {
    lit: []const u8,
    opt: []const u8,
    save: u32,
    /// `stop`: the one byte the class excludes, when it excludes exactly one (`[^/]`), searched for by vector.
    run: struct { member: *const [256]bool, min: u32, stop: ?u8 },
    /// `.*` up to `$`.
    rest,
    end,
};

/// Anchored literals and greedy optional literals, one byte-compatible class run, and a final `.*$`.
/// A greedy optional that later fails falls back, retaining the VM's backtracking and capture semantics.
pub const Plan = struct {
    steps: []const Step,

    pub fn compile(arena: std.mem.Allocator, re: anytype) !?Plan {
        if (!re.anchored) return null;
        const insts = re.insts;
        var steps: std.ArrayList(Step) = .empty;
        var lit: std.ArrayList(u8) = .empty;
        var pc: usize = 0;
        var saw_end = false;
        var saw_rest = false;
        var saw_run = false;
        while (pc < insts.len) {
            const inst = insts[pc];
            if (saw_end and inst != .save and inst != .match) return null;
            if (saw_rest and inst != .save and inst != .assert and inst != .match) return null;
            switch (inst) {
                .char => |cp| {
                    var b: [4]u8 = undefined;
                    const n = std.unicode.utf8Encode(cp, &b) catch return null;
                    try lit.appendSlice(arena, b[0..n]);
                    pc += 1;
                    continue;
                },
                else => {},
            }
            if (lit.items.len > 0) {
                try steps.append(arena, .{ .lit = try arena.dupe(u8, lit.items) });
                lit.clearRetainingCapacity();
            }
            switch (inst) {
                .char => unreachable,
                .save => |s| {
                    try steps.append(arena, .{ .save = s });
                    pc += 1;
                },
                .assert => |a| switch (a) {
                    .begin_text => {
                        if (pc != 1) return null;
                        pc += 1;
                    },
                    .end_text => {
                        try steps.append(arena, .end);
                        saw_end = true;
                        pc += 1;
                    },
                    else => return null,
                },
                .split => |to| {
                    // A greedy `?` of literal characters: split(body, join), body all chars up to join.
                    if (to[0] != pc + 1 or to[1] <= pc + 1) return null;
                    var body: std.ArrayList(u8) = .empty;
                    for (insts[pc + 1 .. to[1]]) |b| switch (b) {
                        .char => |cp| {
                            var buf: [4]u8 = undefined;
                            const n = std.unicode.utf8Encode(cp, &buf) catch return null;
                            try body.appendSlice(arena, buf[0..n]);
                        },
                        else => return null,
                    };
                    try steps.append(arena, .{ .opt = body.items });
                    pc = to[1];
                },
                .class => |k| {
                    if (saw_run) return null;
                    saw_run = true;
                    // `C+` compiles to `C; run(C)`; a lone `C` is not taken.
                    // Each copy of the class has its own entry: compare what they hold.
                    if (pc + 1 >= insts.len or insts[pc + 1] != .run) return null;
                    const a = &re.classes[k];
                    const b = &re.classes[insts[pc + 1].run.class];
                    if (!std.mem.eql(u64, &a.ascii, &b.ascii) or a.ranges.len != b.ranges.len) return null;
                    for (a.ranges, b.ranges) |x, y| if (x.lo != y.lo or x.hi != y.hi) return null;
                    try steps.append(arena, .{ .run = .{
                        .member = try byteTable(arena, &re.classes[k]) orelse return null,
                        .min = 1,
                        .stop = onlyStop(re.classes[k]),
                    } });
                    pc = insts[pc + 1].run.exit;
                },
                .run => |r| {
                    if (saw_run) return null;
                    saw_run = true;
                    try steps.append(arena, .{ .run = .{
                        .member = try byteTable(arena, &re.classes[r.class]) orelse return null,
                        .min = 0,
                        .stop = onlyStop(re.classes[r.class]),
                    } });
                    pc = r.exit;
                },
                .rest => |r| {
                    if (saw_rest) return null;
                    saw_rest = true;
                    try steps.append(arena, .rest);
                    pc = r.exit;
                },
                .jmp => return null,
                .match => {
                    if (pc != insts.len - 1) return null;
                    break;
                },
            }
        }
        // Every run must be possessive: next consuming step a literal its class excludes, `.*$`, or the end.
        for (steps.items, 0..) |st, i| if (st == .run) {
            var j = i + 1;
            while (j < steps.items.len and steps.items[j] == .save) j += 1;
            if (j == steps.items.len) return null;
            switch (steps.items[j]) {
                .lit => |l| if (st.run.member[l[0]]) return null,
                .rest, .end => {},
                else => return null,
            }
        };
        if (!saw_end or !saw_rest) return null;
        return .{ .steps = steps.items };
    }

    /// The single byte a class leaves out, when it takes every other code point.
    fn onlyStop(c: anytype) ?u8 {
        if (!(c.ranges.len == 1 and c.ranges[0].lo == 0x80 and c.ranges[0].hi == 0x10FFFF)) return null;
        const out0 = ~c.ascii[0];
        const out1 = ~c.ascii[1];
        if (@as(usize, @popCount(out0)) + @as(usize, @popCount(out1)) != 1) return null;
        return if (out0 != 0) @intCast(@ctz(out0)) else @intCast(64 + @ctz(out1));
    }

    /// Byte membership of a class that decides every non-ASCII code point alike; null otherwise.
    fn byteTable(arena: std.mem.Allocator, c: anytype) !?*const [256]bool {
        const all_high = c.ranges.len == 1 and c.ranges[0].lo == 0x80 and c.ranges[0].hi == 0x10FFFF;
        if (!all_high and c.ranges.len != 0) return null;
        const t = try arena.create([256]bool);
        for (t, 0..) |*m, b| m.* = if (b < 0x80) (c.ascii[b >> 6] >> @intCast(b & 63)) & 1 != 0 else all_high;
        return t;
    }

    /// The text has no `\n` and is valid UTF-8: the bytes then match as the code points would.
    inline fn takes(text: []const u8) bool {
        const width = std.simd.suggestVectorLength(u8) orelse 1;
        const V = @Vector(width, u8);
        var high = false;
        var i: usize = 0;
        while (i + width <= text.len) : (i += width) {
            const v: V = text[i..][0..width].*;
            if (@reduce(.Or, v == @as(V, @splat('\n')))) return false;
            high = high or @reduce(.Or, v) >= 0x80;
        }
        for (text[i..]) |byte| {
            if (byte == '\n') return false;
            high = high or byte >= 0x80;
        }
        return !high or std.unicode.utf8ValidateSlice(text);
    }

    /// null delegates to the general engine. On success the match is the entire text, even for global replace.
    pub fn find(plan: *const Plan, text: []const u8, caps: []?usize) ?bool {
        if (!takes(text)) return null;
        @memset(caps, null);
        var pos: usize = 0;
        var optional_taken = false;
        for (plan.steps) |step| switch (step) {
            .lit => |lit| {
                if (!std.mem.startsWith(u8, text[pos..], lit)) return if (optional_taken) null else false;
                pos += lit.len;
            },
            .opt => |lit| if (std.mem.startsWith(u8, text[pos..], lit)) {
                pos += lit.len;
                optional_taken = true;
            },
            .save => |slot| caps[slot] = pos,
            .run => |r| {
                const start = pos;
                if (r.stop) |byte| {
                    pos = std.mem.indexOfScalarPos(u8, text, pos, byte) orelse text.len;
                } else while (pos < text.len and r.member[text[pos]]) pos += 1;
                if (pos - start < r.min) return if (optional_taken) null else false;
            },
            .rest => pos = text.len,
            .end => if (pos != text.len) return if (optional_taken) null else false,
        };
        return true;
    }
};
