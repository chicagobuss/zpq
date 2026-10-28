//! Arrow C Data/Stream ABI. Only independent C-heap storage crosses this boundary.
const std = @import("std");
const zpq = @import("zpq");
const Col = zpq.core.expr.agg.OutputCol;
const gpa = std.heap.c_allocator;
const c = std.c;

pub const Schema = extern struct {
    format: ?[*:0]const u8 = null,
    name: ?[*:0]const u8 = null,
    metadata: ?[*]const u8 = null,
    flags: i64 = 0,
    n_children: i64 = 0,
    children: ?[*]*Schema = null,
    dictionary: ?*Schema = null,
    release: ?*const fn (*Schema) callconv(.c) void = null,
    private_data: ?*anyopaque = null,
};
pub const Array = extern struct {
    length: i64 = 0,
    null_count: i64 = 0,
    offset: i64 = 0,
    n_buffers: i64 = 0,
    n_children: i64 = 0,
    buffers: ?[*]?*const anyopaque = null,
    children: ?[*]*Array = null,
    dictionary: ?*Array = null,
    release: ?*const fn (*Array) callconv(.c) void = null,
    private_data: ?*anyopaque = null,
};
pub const Stream = extern struct {
    get_schema: ?*const fn (*Stream, *Schema) callconv(.c) c_int = null,
    get_next: ?*const fn (*Stream, *Array) callconv(.c) c_int = null,
    get_last_error: ?*const fn (*Stream) callconv(.c) ?[*:0]const u8 = null,
    release: ?*const fn (*Stream) callconv(.c) void = null,
    private_data: ?*anyopaque = null,
};

fn dupeZ(a: std.mem.Allocator, s: []const u8) ![:0]u8 {
    return a.dupeSentinel(u8, s, 0);
}

const Storage = struct {
    arena: std.heap.ArenaAllocator = .init(gpa),
    fn create() !*Storage {
        const p = try gpa.create(Storage);
        p.* = .{};
        return p;
    }
    fn destroy(self: *Storage) void {
        self.arena.deinit();
        gpa.destroy(self);
    }
};

fn releaseSchema(s: *Schema) callconv(.c) void {
    if (s.release == null) return;
    for (0..@intCast(s.n_children)) |i| {
        const child = s.children.?[i];
        if (child.release) |f| f(child);
    }
    const own: *Storage = @ptrCast(@alignCast(s.private_data.?));
    s.release = null;
    s.private_data = null;
    own.destroy();
}
fn releaseArray(a: *Array) callconv(.c) void {
    if (a.release == null) return;
    for (0..@intCast(a.n_children)) |i| {
        const child = a.children.?[i];
        if (child.release) |f| f(child);
    }
    const own: *Storage = @ptrCast(@alignCast(a.private_data.?));
    a.release = null;
    a.private_data = null;
    own.destroy();
}

fn logical(col: Col) ?zpq.core.schema.LogicalType {
    const elem: zpq.core.schema.SchemaElement = .{
        .name = col.name,
        .type = col.parquet_type,
        .type_length = col.type_length,
        .repetition_type = null,
        .num_children = null,
        .scale = if (col.dec) |d| d.scale else null,
        .precision = if (col.dec) |d| d.precision else null,
        .field_id = null,
        .logical_type = col.logical_type,
        .converted_type = col.converted_type,
    };
    return elem.effectiveLogicalType();
}

fn format(alloc: std.mem.Allocator, col: Col) ![:0]const u8 {
    switch (col.col) {
        inline else => |v| if (v.max_rep > 0) return error.ArrowNestedNotSupported,
    }
    if (col.parquet_type == .INT96) return dupeZ(alloc, "tsn:UTC");
    if (col.dec) |d| {
        if (d.precision > 38) return error.ArrowDecimalPrecisionNotSupported;
        return std.fmt.allocPrintSentinel(alloc, "d:{d},{d}", .{ d.precision, d.scale }, 0);
    }
    if (logical(col)) |lt| switch (lt) {
        .INTEGER => |v| return dupeZ(alloc, switch (v.bitWidth) {
            8 => if (v.isSigned) "c" else "C",
            16 => if (v.isSigned) "s" else "S",
            32 => if (v.isSigned) "i" else "I",
            64 => if (v.isSigned) "l" else "L",
            else => return error.ArrowIntegerWidthNotSupported,
        }),
        .DATE => return dupeZ(alloc, "tdD"),
        .TIMESTAMP => |v| return std.fmt.allocPrintSentinel(alloc, "ts{s}:{s}", .{
            switch (v.unit) {
                .MILLIS => "m",
                .MICROS => "u",
                .NANOS => "n",
            },
            if (v.isAdjustedToUTC) "UTC" else "",
        }, 0),
        .TIME => |v| return dupeZ(alloc, switch (v.unit) {
            .MILLIS => "ttm",
            .MICROS => "ttu",
            .NANOS => "ttn",
        }),
        .STRING, .ENUM, .JSON => return dupeZ(alloc, "u"),
        .BSON, .UUID => return dupeZ(alloc, "z"),
        else => return error.ArrowLogicalTypeNotSupported,
    };
    return dupeZ(alloc, switch (col.parquet_type) {
        .INT32 => "i",
        .INT64 => "l",
        .FLOAT => "f",
        .DOUBLE => "g",
        .BOOLEAN => "b",
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => "z",
        .INT96 => unreachable,
    });
}

pub fn makeSchema(cols: []const Col) !Schema {
    const own = try Storage.create();
    var out: Schema = .{ .format = "+s", .release = releaseSchema, .private_data = own };
    errdefer releaseSchema(&out);
    const a = own.arena.allocator();
    const children = try a.alloc(*Schema, cols.len);
    out.children = children.ptr;
    for (cols) |col| {
        const child = try a.create(Schema);
        const co = try Storage.create();
        child.* = .{ .release = releaseSchema, .private_data = co, .flags = if (col.nullable) 2 else 0 };
        children[@intCast(out.n_children)] = child;
        out.n_children += 1;
        child.format = (try format(co.arena.allocator(), col)).ptr;
        child.name = (try dupeZ(co.arena.allocator(), col.name)).ptr;
    }
    return out;
}

fn cloneSchema(src: Schema) !Schema {
    const own = try Storage.create();
    var out: Schema = .{ .release = releaseSchema, .private_data = own, .flags = src.flags };
    errdefer releaseSchema(&out);
    const a = own.arena.allocator();
    out.format = (try dupeZ(a, std.mem.span(src.format.?))).ptr;
    if (src.name) |name| out.name = (try dupeZ(a, std.mem.span(name))).ptr;
    const children = try a.alloc(*Schema, @intCast(src.n_children));
    out.children = children.ptr;
    for (0..@intCast(src.n_children)) |i| {
        const child = try a.create(Schema);
        child.* = try cloneSchema(src.children.?[i].*);
        children[i] = child;
        out.n_children += 1;
    }
    return out;
}

const Charge = struct {
    bytes: usize = 0,
    cap: usize,
    fn add(self: *Charge, n: usize) !void {
        if (n > self.cap -| self.bytes) return error.ResultTooLarge;
        self.bytes += n;
    }
};

fn value(v: anytype, row: usize) @TypeOf(v.values[0]) {
    return if (v.codes) |codes| v.dict[codes[row]] else v.values[row];
}
fn integer(col: Col, row: usize) !i128 {
    const unsigned = if (logical(col)) |lt| lt == .INTEGER and !lt.INTEGER.isSigned else false;
    return switch (col.col) {
        .i32 => |v| if (unsigned) @as(u32, @bitCast(value(v, row))) else value(v, row),
        .i64 => |v| if (unsigned) @as(u64, @bitCast(value(v, row))) else value(v, row),
        .i128 => |v| value(v, row),
        .boolean => |v| @intFromBool(value(v, row)),
        else => error.ArrowTypeMismatch,
    };
}

fn fixed(comptime T: type, a: std.mem.Allocator, col: Col, lo: usize, hi: usize, charge: *Charge) !?*const anyopaque {
    try charge.add((hi - lo) * @sizeOf(T));
    const vals = try a.alloc(T, hi - lo);
    for (vals, lo..) |*dst, row| {
        dst.* = 0;
        if (zpq.core.expr.agg.isRowNull(col.col, row)) continue;
        if (@typeInfo(T) == .float) {
            dst.* = switch (col.col) {
                .f32 => |v| @floatCast(value(v, row)),
                .f64 => |v| @floatCast(value(v, row)),
                else => return error.ArrowTypeMismatch,
            };
        } else dst.* = std.math.cast(T, try integer(col, row)) orelse return error.ArrowIntegerOverflow;
    }
    return vals.ptr;
}

pub fn makeArray(cols: []const Col, lo: usize, hi: usize, charge: *Charge) !Array {
    const own = try Storage.create();
    var out: Array = .{ .length = @intCast(hi - lo), .n_buffers = 1, .release = releaseArray, .private_data = own };
    errdefer releaseArray(&out);
    const a = own.arena.allocator();
    const root_buffers = try a.alloc(?*const anyopaque, 1);
    root_buffers[0] = null;
    out.buffers = root_buffers.ptr;
    const children = try a.alloc(*Array, cols.len);
    out.children = children.ptr;
    const n = hi - lo;
    for (cols) |col| {
        const child = try a.create(Array);
        const co = try Storage.create();
        child.* = .{ .length = @intCast(n), .release = releaseArray, .private_data = co };
        children[@intCast(out.n_children)] = child;
        out.n_children += 1;
        const ca = co.arena.allocator();
        const f = try format(ca, col);
        const strings = f[0] == 'z' or f[0] == 'u';
        const buffers = try ca.alloc(?*const anyopaque, if (strings) 3 else 2);
        @memset(buffers, null);
        child.n_buffers = @intCast(buffers.len);
        child.buffers = buffers.ptr;
        for (lo..hi) |row| if (zpq.core.expr.agg.isRowNull(col.col, row)) {
            child.null_count += 1;
        };
        if (child.null_count > 0) {
            try charge.add((n + 7) / 8);
            const valid = try ca.alloc(u8, (n + 7) / 8);
            @memset(valid, 0);
            for (lo..hi, 0..) |row, i| if (!zpq.core.expr.agg.isRowNull(col.col, row)) {
                valid[i / 8] |= @as(u8, 1) << @intCast(i % 8);
            };
            buffers[0] = valid.ptr;
        }
        if (strings) {
            const v = switch (col.col) {
                .string => |v| v,
                else => return error.ArrowTypeMismatch,
            };
            var len: usize = 0;
            for (lo..hi) |row| if (!zpq.core.expr.agg.isRowNull(col.col, row)) {
                const str = value(v, row);
                if (f[0] == 'u' and !std.unicode.utf8ValidateSlice(str)) return error.InvalidUtf8;
                len = std.math.add(usize, len, str.len) catch return error.ResultTooLarge;
            };
            if (len > std.math.maxInt(i32)) return error.ArrowBinaryBatchTooLarge;
            try charge.add((n + 1) * 4);
            try charge.add(len);
            const offsets = try ca.alloc(i32, n + 1);
            const data = try ca.alloc(u8, len);
            var at: usize = 0;
            offsets[0] = 0;
            for (lo..hi, 0..) |row, i| {
                if (!zpq.core.expr.agg.isRowNull(col.col, row)) {
                    const str = value(v, row);
                    @memcpy(data[at..][0..str.len], str);
                    at += str.len;
                }
                offsets[i + 1] = @intCast(at);
            }
            buffers[1] = offsets.ptr;
            buffers[2] = data.ptr;
        } else if (f[0] == 'b') {
            try charge.add((n + 7) / 8);
            const bits = try ca.alloc(u8, (n + 7) / 8);
            @memset(bits, 0);
            for (lo..hi, 0..) |row, i| if (!zpq.core.expr.agg.isRowNull(col.col, row) and try integer(col, row) != 0) {
                bits[i / 8] |= @as(u8, 1) << @intCast(i % 8);
            };
            buffers[1] = bits.ptr;
        } else buffers[1] = switch (f[0]) {
            'c' => try fixed(i8, ca, col, lo, hi, charge),
            'C' => try fixed(u8, ca, col, lo, hi, charge),
            's' => try fixed(i16, ca, col, lo, hi, charge),
            'S' => try fixed(u16, ca, col, lo, hi, charge),
            'i' => try fixed(i32, ca, col, lo, hi, charge),
            'I' => try fixed(u32, ca, col, lo, hi, charge),
            'l' => try fixed(i64, ca, col, lo, hi, charge),
            'L' => try fixed(u64, ca, col, lo, hi, charge),
            'f' => try fixed(f32, ca, col, lo, hi, charge),
            'g' => try fixed(f64, ca, col, lo, hi, charge),
            'd' => try fixed(i128, ca, col, lo, hi, charge),
            't' => if (std.mem.eql(u8, f, "tdD") or std.mem.eql(u8, f, "ttm")) try fixed(i32, ca, col, lo, hi, charge) else try fixed(i64, ca, col, lo, hi, charge),
            else => return error.ArrowTypeNotSupported,
        };
    }
    return out;
}

/// One bounded producer slot. Exported batches own their storage, so retaining one never stalls the next pull.
pub const State = struct {
    context: *anyopaque,
    body: []u8,
    producer: *const fn (*State) void,
    thread: ?std.Thread = null,
    lock: c.pthread_mutex_t = .{},
    cond: c.pthread_cond_t = .{},
    schema: ?Schema = null,
    pending: ?Array = null,
    done: bool = false,
    cancelled: bool = false,
    failure: ?[:0]u8 = null,
    code: c_int = 0,
    charge: Charge,

    pub fn sink(self: *State) zpq.engine.ColumnarSink {
        return .{ .ctx = self, .schema_fn = schemaFn, .batch_fn = batchFn };
    }
    pub fn stream(self: *State) Stream {
        return .{ .get_schema = getSchema, .get_next = getNext, .get_last_error = lastError, .release = release, .private_data = self };
    }
    fn schemaFn(ptr: *anyopaque, cols: []const Col) !void {
        const self: *State = @ptrCast(@alignCast(ptr));
        var schema = try makeSchema(cols);
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        if (self.cancelled) {
            releaseSchema(&schema);
            return error.Cancelled;
        }
        self.schema = schema;
        _ = c.pthread_cond_broadcast(&self.cond);
    }
    fn batchFn(ptr: *anyopaque, cols: []const Col, lo: usize, hi: usize) !void {
        const self: *State = @ptrCast(@alignCast(ptr));
        var at = lo;
        while (at < hi) {
            const end = @min(hi, at + (1 << 16));
            _ = c.pthread_mutex_lock(&self.lock);
            while (self.pending != null and !self.cancelled) _ = c.pthread_cond_wait(&self.cond, &self.lock);
            const cancelled = self.cancelled;
            _ = c.pthread_mutex_unlock(&self.lock);
            if (cancelled) return error.Cancelled;
            var array = try makeArray(cols, at, end, &self.charge);
            _ = c.pthread_mutex_lock(&self.lock);
            defer _ = c.pthread_mutex_unlock(&self.lock);
            if (self.cancelled) {
                releaseArray(&array);
                return error.Cancelled;
            }
            self.pending = array;
            _ = c.pthread_cond_broadcast(&self.cond);
            at = end;
        }
    }
    pub fn finish(self: *State, message: ?[]const u8, oom: bool) void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        if (message) |m| {
            self.failure = dupeZ(gpa, m) catch null;
            self.code = if (oom) @backingInt(c.E.NOMEM) else @backingInt(c.E.IO);
        }
        self.done = true;
        _ = c.pthread_cond_broadcast(&self.cond);
    }
    fn get(s: *Stream) *State {
        return @ptrCast(@alignCast(s.private_data.?));
    }
    fn getSchema(s: *Stream, out: *Schema) callconv(.c) c_int {
        out.* = .{};
        const self = get(s);
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        while (self.schema == null and !self.done) _ = c.pthread_cond_wait(&self.cond, &self.lock);
        if (self.schema) |schema| {
            out.* = cloneSchema(schema) catch return @backingInt(c.E.NOMEM);
            return 0;
        }
        return if (self.code != 0) self.code else @backingInt(c.E.IO);
    }
    fn getNext(s: *Stream, out: *Array) callconv(.c) c_int {
        out.* = .{};
        const self = get(s);
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        while (self.pending == null and !self.done) _ = c.pthread_cond_wait(&self.cond, &self.lock);
        if (self.pending) |array| {
            out.* = array;
            self.pending = null;
            _ = c.pthread_cond_broadcast(&self.cond);
            return 0;
        }
        return self.code;
    }
    fn lastError(s: *Stream) callconv(.c) ?[*:0]const u8 {
        return if (get(s).failure) |v| v.ptr else null;
    }
    fn release(s: *Stream) callconv(.c) void {
        const self = get(s);
        _ = c.pthread_mutex_lock(&self.lock);
        self.cancelled = true;
        _ = c.pthread_cond_broadcast(&self.cond);
        _ = c.pthread_mutex_unlock(&self.lock);
        if (self.thread) |t| t.join();
        if (self.schema) |*schema| releaseSchema(schema);
        if (self.pending) |*array| releaseArray(array);
        if (self.failure) |v| gpa.free(v);
        gpa.free(self.body);
        _ = c.pthread_cond_destroy(&self.cond);
        _ = c.pthread_mutex_destroy(&self.lock);
        gpa.destroy(self);
        s.release = null;
        s.private_data = null;
    }
};
