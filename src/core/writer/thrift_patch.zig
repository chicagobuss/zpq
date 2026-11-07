//! Rewriting a compact-protocol thrift struct field by field, keeping every field the caller does not change byte for
//! byte: a footer, page index or statistics struct read through zpq's models loses the fields those models skip (file
//! and chunk key-value metadata, size statistics, level histograms), which a rewrite in place must not.

const std = @import("std");
const thrift = @import("../thrift.zig");

/// One field of a struct: its id, its compact type (a bool's value is its type), and its encoded value.
pub const Field = struct {
    id: i16,
    type: thrift.Type,
    raw: []const u8,
};

/// The fields of the struct at `r`'s position, through its STOP byte.
pub fn readFields(arena: std.mem.Allocator, r: *thrift.Reader) ![]Field {
    const saved = r.last_field_id;
    r.readStructBegin();
    defer r.last_field_id = saved;
    var out: std.ArrayList(Field) = .empty;
    while (true) {
        const f = try r.readFieldBegin();
        if (f.type == .Stop) break;
        const start = r.pos;
        try skipValue(r, f.type);
        try out.append(arena, .{ .id = f.id, .type = f.type, .raw = r.data[start..r.pos] });
    }
    return out.items;
}

/// The fields of the struct encoded in `bytes`.
pub fn fieldsOf(arena: std.mem.Allocator, bytes: []const u8) ![]Field {
    var r = thrift.Reader.init(bytes);
    return readFields(arena, &r);
}

/// `thrift.Reader.skip`, except that a list of bools holds one byte per element, which `skip` does not read.
fn skipValue(r: *thrift.Reader, t: thrift.Type) !void {
    if (t == .List or t == .Set) {
        const save = r.pos;
        const h = try r.readByte();
        const elem: u8 = h & 0x0f;
        if (elem == @backingInt(thrift.Type.True) or elem == @backingInt(thrift.Type.False)) {
            var size: usize = h >> 4;
            if (size == 0xF) size = try r.readVarInt(usize);
            if (size > r.remaining()) return error.InvalidLength;
            r.pos += size;
            return;
        }
        r.pos = save;
    }
    try r.skip(t);
}

pub fn find(fields: []const Field, id: i16) ?Field {
    for (fields) |f| if (f.id == id) return f;
    return null;
}

/// A list field's elements: their type and count, and the encoded elements after the header.
pub const List = struct {
    elem: thrift.Type,
    len: usize,
    body: []const u8,
};

pub fn list(raw: []const u8) !List {
    var r = thrift.Reader.init(raw);
    const h = try r.readByte();
    var size: usize = h >> 4;
    if (size == 0xF) size = try r.readVarInt(usize);
    const elem = std.enums.fromInt(thrift.Type, h & 0x0f) orelse return error.InvalidThriftType;
    return .{ .elem = elem, .len = size, .body = raw[r.pos..] };
}

/// The encoded elements of a list of structs, each through its STOP byte.
pub fn structElements(arena: std.mem.Allocator, raw: []const u8) ![]const []const u8 {
    const l = try list(raw);
    if (l.elem != .Struct) return error.InvalidThriftType;
    const out = try arena.alloc([]const u8, l.len);
    var r = thrift.Reader.init(l.body);
    for (out) |*e| {
        const start = r.pos;
        try r.skip(.Struct);
        e.* = l.body[start..r.pos];
    }
    return out;
}

/// The value of an i16/i32/i64 field.
pub fn int(comptime T: type, f: Field) !T {
    var r = thrift.Reader.init(f.raw);
    return r.readZigZag(T);
}

/// A struct's output fields, written in id order whatever order they were added in (`bytes`).
pub const Builder = struct {
    arena: std.mem.Allocator,
    fields: std.ArrayList(Field) = .empty,

    pub fn init(arena: std.mem.Allocator) Builder {
        return .{ .arena = arena };
    }

    /// Fields as read, kept unless `put` replaces or `drop` removes them.
    pub fn keep(self: *Builder, fields: []const Field) !void {
        try self.fields.appendSlice(self.arena, fields);
    }

    pub fn drop(self: *Builder, id: i16) void {
        var i: usize = 0;
        while (i < self.fields.items.len) {
            if (self.fields.items[i].id == id) _ = self.fields.orderedRemove(i) else i += 1;
        }
    }

    pub fn put(self: *Builder, f: Field) !void {
        self.drop(f.id);
        try self.fields.append(self.arena, f);
    }

    pub fn putInt(self: *Builder, id: i16, comptime T: type, v: T) !void {
        var w = thrift.Writer.init(self.arena);
        try w.writeZigZag(v);
        const t: thrift.Type = switch (T) {
            i16 => .I16,
            i32 => .I32,
            i64 => .I64,
            else => @compileError("integer fields only"),
        };
        try self.put(.{ .id = id, .type = t, .raw = w.bytes() });
    }

    pub fn putBinary(self: *Builder, id: i16, v: []const u8) !void {
        var w = thrift.Writer.init(self.arena);
        try w.writeString(v);
        try self.put(.{ .id = id, .type = .Binary, .raw = w.bytes() });
    }

    /// A struct field from its encoding, STOP byte included.
    pub fn putStruct(self: *Builder, id: i16, encoded: []const u8) !void {
        try self.put(.{ .id = id, .type = .Struct, .raw = encoded });
    }

    /// A list field of structs, each encoded through its STOP byte.
    pub fn putStructList(self: *Builder, id: i16, elems: []const []const u8) !void {
        var bytes: std.ArrayList(u8) = .empty;
        try listHeader(self.arena, &bytes, .Struct, elems.len);
        for (elems) |e| try bytes.appendSlice(self.arena, e);
        try self.put(.{ .id = id, .type = .List, .raw = bytes.items });
    }

    pub fn putI64List(self: *Builder, id: i16, values: []const i64) !void {
        var w = thrift.Writer.init(self.arena);
        try w.writeListBegin(.I64, values.len);
        for (values) |v| try w.writeZigZag(v);
        try self.put(.{ .id = id, .type = .List, .raw = w.bytes() });
    }

    /// The struct: fields in id order, each tag a delta from the one before where it fits in four bits.
    pub fn encode(self: *Builder) ![]u8 {
        std.mem.sort(Field, self.fields.items, {}, struct {
            fn lt(_: void, a: Field, b: Field) bool {
                return a.id < b.id;
            }
        }.lt);
        var out: std.ArrayList(u8) = .empty;
        var prev: i16 = 0;
        for (self.fields.items) |f| {
            try tag(self.arena, &out, f.type, f.id, prev);
            try out.appendSlice(self.arena, f.raw);
            prev = f.id;
        }
        try out.append(self.arena, 0);
        return out.items;
    }
};

fn tag(arena: std.mem.Allocator, out: *std.ArrayList(u8), t: thrift.Type, id: i16, prev: i16) !void {
    const delta = @as(i32, id) - prev;
    if (delta > 0 and delta <= 15) {
        try out.append(arena, @as(u8, @intCast(delta)) << 4 | @backingInt(t));
        return;
    }
    var w = thrift.Writer.init(arena);
    try w.writeByte(@backingInt(t));
    try w.writeZigZag(id);
    try out.appendSlice(arena, w.bytes());
}

fn listHeader(arena: std.mem.Allocator, out: *std.ArrayList(u8), elem: thrift.Type, len: usize) !void {
    var w = thrift.Writer.init(arena);
    try w.writeListBegin(elem, len);
    try out.appendSlice(arena, w.bytes());
}

test "thrift patch: unchanged fields round-trip byte for byte, a list of bools included, and puts land in id order" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var w = thrift.Writer.init(a);
    w.writeStructBegin();
    try w.writeFieldListBegin(1, .True, 3);
    for ([_]u8{ 1, 2, 1 }) |b| try w.writeByte(b);
    try w.writeFieldI64(2, -7);
    try w.writeFieldBool(4, true);
    try w.writeFieldString(30, "far");
    try w.writeStructEnd();
    const fields = try fieldsOf(a, w.bytes());
    try std.testing.expectEqual(@as(usize, 4), fields.len);
    var same = Builder.init(a);
    try same.keep(fields);
    try std.testing.expectEqualSlices(u8, w.bytes(), try same.encode());

    var b = Builder.init(a);
    try b.keep(fields);
    try b.putInt(3, i64, 1 << 40);
    try b.putInt(2, i64, 5);
    b.drop(4);
    const out = try fieldsOf(a, try b.encode());
    try std.testing.expectEqual(@as(usize, 4), out.len);
    for (out, [_]i16{ 1, 2, 3, 30 }) |f, id| try std.testing.expectEqual(id, f.id);
    try std.testing.expectEqual(@as(i64, 5), try int(i64, out[1]));
    try std.testing.expectEqual(@as(i64, 1 << 40), try int(i64, out[2]));
    try std.testing.expectEqual(@as(usize, 3), (try list(out[0].raw)).len);
}
