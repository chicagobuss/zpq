//! Footer inspection has its own bounded I/O path; it never invokes the scan engine.
const std = @import("std");
const zpq = @import("zpq");
const schema = zpq.core.schema;
const metadata = zpq.core.parquet.metadata;
const thrift = zpq.core.thrift;
const decimal = zpq.core.parquet.decimal;
const s3 = zpq.io.s3;
const fs = zpq.local_fs;

pub const footer_limit = 64 << 20;
const detail_limit = 8 << 20;
const request_limit = 128;
const page_limit = 256;

const Source = struct {
    arena: std.mem.Allocator,
    mapped: ?fs.Mapped = null,
    client: ?s3.Client = null,
    key: []const u8 = "",
    size: usize = 0,
    footer_start: usize = 0,
    requests: usize = 0,
    bytes: usize = 0,
    detail_bytes: usize = 0,
    pages: usize = 0,

    fn open(arena: std.mem.Allocator, env: std.process.Environ, path: []const u8) !Source {
        var self: Source = .{ .arena = arena };
        errdefer self.close();
        if (std.mem.startsWith(u8, path, "s3://")) {
            const url = try s3.Url.parse(path);
            self.key = url.key;
            self.client = try s3.Client.init(arena, try s3.Credentials.fromEnv(env), url.bucket);
            const r = try self.client.?.get(arena, self.key, .suffix(8));
            if (r.status != 206 or r.body.len != 8) return error.BadRangeResponse;
            const cr = r.header("Content-Range") orelse return error.BadRangeResponse;
            const slash = std.mem.lastIndexOfScalar(u8, cr, '/') orelse return error.BadRangeResponse;
            self.size = try std.fmt.parseInt(usize, cr[slash + 1 ..], 10);
            self.requests = 1;
            self.bytes = 8;
        } else {
            self.mapped = try fs.mapFile(path);
            self.size = self.mapped.?.bytes.len;
        }
        if (self.size < 12) return error.TooSmall;
        return self;
    }

    fn close(self: *Source) void {
        if (self.client) |*c| c.deinit();
        if (self.mapped) |m| m.unmap();
    }

    fn range(self: *Source, start: usize, len: usize, detail: bool) ![]const u8 {
        if (start > self.size or len > self.size - start) return error.InvalidRange;
        if (detail) {
            if (len > detail_limit - self.detail_bytes or self.requests >= request_limit) return error.DetailLimit;
            self.detail_bytes += len;
        }
        self.bytes += len;
        if (self.mapped) |m| return m.bytes[start..][0..len];
        if (len == 0) return "";
        self.requests += 1;
        const r = try self.client.?.get(self.arena, self.key, .span(start, start + len - 1));
        if (r.status != 206 or r.body.len != len) return error.BadRangeResponse;
        const wanted = try std.fmt.allocPrint(self.arena, "bytes {d}-{d}/{d}", .{ start, start + len - 1, self.size });
        if (!std.mem.eql(u8, r.header("Content-Range") orelse "", wanted)) return error.BadRangeResponse;
        return r.body;
    }

    fn footer(self: *Source) !schema.FileMetaData {
        // An encrypted footer's file opens with the same magic it ends with.
        const head = try self.range(0, 4, false);
        if (metadata.hasEncryptedFooter(head)) return error.EncryptedFile;
        if (!std.mem.eql(u8, head, "PAR1")) return error.BadMagic;
        const tail = try self.range(self.size - 8, 8, false);
        if (!std.mem.eql(u8, tail[4..], "PAR1")) return error.BadMagic;
        const len = std.mem.readInt(u32, tail[0..4], .little);
        if (len > footer_limit) return error.FooterTooLarge;
        if (len > self.size - 12) return error.InvalidRange;
        self.footer_start = self.size - 8 - len;
        // Parsed strings borrow the footer; the report outlives this source mapping.
        const bytes = try self.range(self.footer_start, len, false);
        const copy = try self.arena.dupe(u8, bytes);
        var checked_reader = thrift.Reader.init(copy);
        var checked = try schema.FileMetaData.read(self.arena, &checked_reader);
        try metadata.checkFooterFields(&checked);
        try metadata.reconcileChunkPaths(self.arena, &checked);
        // Execution normalizes invalid dictionary pointers; retain the original values for inspection.
        var reader = thrift.Reader.init(copy);
        try restoreOffsets(&reader, &checked, 0, 0, 0);
        return checked;
    }
};

fn structList(r: *thrift.Reader) !usize {
    const header = try r.readByte();
    if (header & 15 != @backingInt(thrift.Type.Struct)) return error.BadMetadata;
    const size = if (header >> 4 == 15) try r.readVarInt(usize) else header >> 4;
    if (size > r.remaining()) return error.BadMetadata;
    return size;
}

fn restoreOffsets(r: *thrift.Reader, meta: *schema.FileMetaData, level: u8, rg: usize, col: usize) !void {
    const saved = r.last_field_id;
    r.last_field_id = 0;
    defer r.last_field_id = saved;
    r.readStructBegin();
    while (true) {
        const f = try r.readFieldBegin();
        if (f.type == .Stop) return;
        if (level == 0 and f.id == 4 and f.type == .List) {
            const n = try structList(r);
            if (n != meta.row_groups.items.len) return error.BadMetadata;
            for (0..n) |i| try restoreOffsets(r, meta, 1, i, 0);
        } else if (level == 1 and f.id == 1 and f.type == .List) {
            const n = try structList(r);
            if (n != meta.row_groups.items[rg].columns.items.len) return error.BadMetadata;
            for (0..n) |i| try restoreOffsets(r, meta, 2, rg, i);
        } else if (level == 2 and f.id == 3 and f.type == .Struct) {
            try restoreOffsets(r, meta, 3, rg, col);
        } else if (level == 3 and f.id == 11 and f.type == .I64) {
            const offset = try r.readZigZag(i64);
            if (meta.row_groups.items[rg].columns.items[col].meta_data) |*cm| cm.dictionary_page_offset = offset;
        } else try r.skip(f.type);
    }
}

const Bound = struct {
    state: []const u8 = "absent",
    kind: []const u8 = "unknown",
    value: ?[]const u8 = null,
    raw_hex: ?[]const u8 = null,
    exactness: []const u8 = "unknown",
    source: []const u8 = "min_value/max_value",
};
const Presence = struct {
    state: []const u8,
    offset: ?i64,
    length: ?i32,
    validation: []const u8 = "unknown",
    entries: ?usize = null,
};
const Page = struct { offset: usize, kind: []const u8, encoding: ?[]const u8, compressed_bytes: i32 };
const Column = struct {
    path: []const []const u8,
    physical_type: []const u8,
    logical_type: ?schema.LogicalType,
    converted_type: ?schema.ConvertedType,
    num_values: i64,
    codec: []const u8,
    encodings: []const []const u8,
    compressed_bytes: i64,
    uncompressed_bytes: i64,
    compression_ratio: ?f64,
    data_page_offset: i64,
    dictionary_page_offset: ?i64,
    dictionary: Presence,
    bounds_usable: bool,
    dictionary_fallback: []const u8,
    statistics: []const u8,
    null_count: ?i64,
    /// FLOAT/DOUBLE only; null when the writer left it out (readers must then assume NaN may be present).
    nan_count: ?i64,
    min: Bound,
    max: Bound,
    column_order: []const u8,
    column_index: Presence,
    offset_index: Presence,
    bloom_filter: Presence,
    external_file: ?[]const u8,
    encrypted: bool,
    detail_state: []const u8 = "not-requested",
    pages: []const Page = &.{},
    hints: []const []const u8,
};
const Group = struct {
    ordinal: usize,
    rows: i64,
    uncompressed_bytes: i64,
    sorting_columns: []const u8 = "unsupported",
    columns: []const Column,
};
const Leaf = struct {
    path: []const []const u8,
    physical_type: []const u8,
    logical_type: ?schema.LogicalType,
    converted_type: ?schema.ConvertedType,
    repetition: []const u8,
};
pub const Report = struct {
    path: []const u8,
    status: []const u8 = "ok",
    parquet_version: i32,
    created_by: ?[]const u8,
    rows: i64,
    bytes: usize,
    footer_bytes: usize,
    schema: []const Leaf,
    row_groups: []const Group,
    io: struct {
        range_requests: usize,
        requested_bytes: usize,
        detail_bytes: usize,
        transport_retries: []const u8 = "unknown",
        detail_byte_limit: usize = detail_limit,
        request_limit: usize = request_limit,
    },
};

fn hex(a: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    const out = try a.alloc(u8, bytes.len * 2);
    const digits = "0123456789abcdef";
    for (bytes, 0..) |b, i| {
        out[i * 2] = digits[b >> 4];
        out[i * 2 + 1] = digits[b & 15];
    }
    return out;
}

fn bound(a: std.mem.Allocator, elem: schema.SchemaElement, raw: ?[]const u8, exact: ?bool) !Bound {
    const bytes = raw orelse return .{};
    var out: Bound = .{ .state = "present", .kind = @tagName(elem.type.?), .raw_hex = try hex(a, bytes) };
    out.exactness = if (exact) |e| (if (e) "exact" else "inexact") else "unknown";
    if (elem.logical_type) |lt| if (lt == .FLOAT16) {
        out.kind = "FLOAT16";
        if (bytes.len != 2) {
            out.state = "malformed";
            return out;
        }
        const v: f16 = @bitCast(std.mem.readInt(u16, bytes[0..2], .little));
        out.value = try std.fmt.allocPrint(a, "{d}", .{@as(f32, @floatCast(v))});
        return out;
    };
    if (decimal.kindFromSchema(&elem)) |kind| {
        out.kind = "DECIMAL";
        const v = decimal.statToI128(bytes, kind) orelse {
            out.state = "malformed";
            return out;
        };
        // Unscaled integer + declared scale is exact even for negative scales and 38-digit values.
        out.value = try std.fmt.allocPrint(a, "{d}e-{d}", .{ v, kind.scale });
        return out;
    }
    switch (elem.type.?) {
        .BOOLEAN => if (bytes.len == 1 and bytes[0] <= 1) {
            out.value = if (bytes[0] == 0) "false" else "true";
        },
        .INT32 => if (bytes.len == 4) {
            out.value = if (schema.isUnsignedInt(elem))
                try std.fmt.allocPrint(a, "{d}", .{std.mem.readInt(u32, bytes[0..4], .little)})
            else
                try std.fmt.allocPrint(a, "{d}", .{std.mem.readInt(i32, bytes[0..4], .little)});
        },
        .INT64 => if (bytes.len == 8) {
            out.value = if (schema.isUnsignedInt(elem))
                try std.fmt.allocPrint(a, "{d}", .{std.mem.readInt(u64, bytes[0..8], .little)})
            else
                try std.fmt.allocPrint(a, "{d}", .{std.mem.readInt(i64, bytes[0..8], .little)});
        },
        .FLOAT => if (bytes.len == 4) {
            const v: f32 = @bitCast(std.mem.readInt(u32, bytes[0..4], .little));
            out.value = try std.fmt.allocPrint(a, "{d}", .{v});
        },
        .DOUBLE => if (bytes.len == 8) {
            const v: f64 = @bitCast(std.mem.readInt(u64, bytes[0..8], .little));
            out.value = try std.fmt.allocPrint(a, "{d}", .{v});
        },
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
            out.kind = "BINARY";
            if (elem.logical_type) |lt| if (lt == .STRING) {
                out.kind = "STRING";
            };
            if (elem.converted_type == .UTF8) out.kind = "STRING";
            const is_text = std.mem.eql(u8, out.kind, "STRING") and std.unicode.utf8ValidateSlice(bytes);
            out.value = if (is_text) bytes else out.raw_hex;
        },
        .INT96 => {
            out.state = "unsupported";
        },
    }
    if (out.value == null and !std.mem.eql(u8, out.state, "unsupported")) out.state = "malformed";
    return out;
}

fn presence(off: ?i64, len: ?i32, file_size: usize, allow_unknown_length: bool) Presence {
    var p: Presence = .{ .state = "absent", .offset = off, .length = len };
    if (off == null and len == null) return p;
    p.state = "malformed";
    const o = off orelse return p;
    if (o < 4 or o >= file_size) return p;
    if (len) |l| {
        if (l <= 0 or @as(u64, @intCast(l)) > file_size - @as(u64, @intCast(o))) return p;
        p.state = "present";
    } else if (allow_unknown_length) p.state = "present";
    return p;
}

fn fallback(cm: schema.ColumnMetaData) []const u8 {
    const stats = cm.encoding_stats orelse return "unknown";
    var dict: i64 = 0;
    var plain: i64 = 0;
    for (stats.items) |s| {
        if (s.count < 0) return "malformed";
        const data_page = s.page_type == @backingInt(schema.PageType.DATA_PAGE);
        if (data_page or s.page_type == @backingInt(schema.PageType.DATA_PAGE_V2)) {
            const dictionary = s.encoding == @backingInt(schema.Encoding.RLE_DICTIONARY) or
                s.encoding == @backingInt(schema.Encoding.PLAIN_DICTIONARY);
            if (dictionary) dict += s.count else plain += s.count;
        }
    }
    return if (dict > 0 and plain > 0) "present" else if (dict > 0 or plain > 0) "absent" else "unknown";
}

fn validateIndex(src: *Source, p: *Presence, is_column: bool) !void {
    if (!std.mem.eql(u8, p.state, "present")) return;
    const len = p.length orelse return;
    const bytes = src.range(@intCast(p.offset.?), @intCast(len), true) catch |e| {
        if (e == error.OutOfMemory) return e;
        p.validation = if (e == error.DetailLimit) "unknown-limit" else "malformed";
        return;
    };
    var reader = thrift.Reader.init(bytes);
    if (is_column) {
        const index = schema.ColumnIndex.read(src.arena, &reader) catch |e| {
            if (e == error.OutOfMemory) return e;
            p.validation = "malformed";
            return;
        };
        p.entries = index.null_pages.items.len;
        const same_len = index.min_values.items.len == p.entries.? and index.max_values.items.len == p.entries.?;
        p.validation = if (same_len) "valid-structure" else "malformed";
    } else {
        const index = schema.OffsetIndex.read(src.arena, &reader) catch |e| {
            if (e == error.OutOfMemory) return e;
            p.validation = "malformed";
            return;
        };
        p.entries = index.page_locations.items.len;
        p.validation = "valid-structure";
        for (index.page_locations.items) |loc| {
            if (loc.offset < 4 or loc.compressed_page_size <= 0 or loc.first_row_index < 0 or
                @as(u64, @intCast(loc.offset)) > src.size or
                @as(u64, @intCast(loc.compressed_page_size)) > src.size - @as(u64, @intCast(loc.offset)))
                p.validation = "malformed";
        }
    }
    if (reader.pos != bytes.len) p.validation = "malformed";
}

fn details(src: *Source, col: *Column, cm: schema.ColumnMetaData) !void {
    try validateIndex(src, &col.column_index, true);
    try validateIndex(src, &col.offset_index, false);
    if (std.mem.eql(u8, col.bloom_filter.state, "present")) {
        const off: usize = @intCast(col.bloom_filter.offset.?);
        const n = @min(@as(usize, 256), src.size - off);
        if (src.range(off, n, true)) |bytes| {
            if (zpq.core.parquet.bloom.parseHeader(bytes)) |h| {
                col.bloom_filter.entries = h.num_bytes;
                col.bloom_filter.validation = if (h.len + @as(usize, h.num_bytes) <= src.size - off and
                    (col.bloom_filter.length == null or h.len + h.num_bytes == col.bloom_filter.length.?))
                    "valid-header"
                else
                    "malformed";
            } else {
                var r = thrift.Reader.init(bytes);
                col.bloom_filter.validation = if (r.skip(.Struct)) |_| "unsupported" else |_| "malformed";
            }
        } else |e| {
            if (e == error.OutOfMemory) return e;
            col.bloom_filter.validation = if (e == error.DetailLimit) "unknown-limit" else "malformed";
        }
    }
    var pages: std.ArrayList(Page) = .empty;
    const dict = cm.dictionary_page_offset orelse 0;
    var off: usize = @intCast(if (dict >= 4) dict else cm.data_page_offset);
    const end = std.math.add(usize, off, @intCast(cm.total_compressed_size)) catch return error.InvalidRange;
    col.detail_state = "complete";
    if (end > src.footer_start) {
        col.detail_state = "malformed";
        return;
    }
    while (off < end) {
        if (src.pages >= page_limit) {
            col.detail_state = "unknown-limit";
            break;
        }
        const bytes = src.range(off, @min(end - off, 64 << 10), true) catch |e| {
            if (e == error.OutOfMemory) return e;
            col.detail_state = if (e == error.DetailLimit) "unknown-limit" else "malformed";
            break;
        };
        var r = thrift.Reader.init(bytes);
        const h = schema.PageHeader.read(&r) catch {
            const truncated_header = bytes.len == 64 << 10 and end - off > bytes.len;
            col.detail_state = if (truncated_header) "unknown-header-limit" else "malformed";
            break;
        };
        if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0 or
            @as(usize, @intCast(h.compressed_page_size)) > end - off - r.pos)
        {
            col.detail_state = "malformed";
            break;
        }
        const enc = if (h.data_page_header) |d| try encodingName(src.arena, d.encoding) else blk: {
            if (h.data_page_header_v2) |d| break :blk try encodingName(src.arena, d.encoding);
            if (h.dictionary_page_header) |d| break :blk try encodingName(src.arena, d.encoding);
            break :blk null;
        };
        try pages.append(src.arena, .{
            .offset = off,
            .kind = @tagName(h.type),
            .encoding = enc,
            .compressed_bytes = h.compressed_page_size,
        });
        src.pages += 1;
        off += r.pos + @as(usize, @intCast(h.compressed_page_size));
    }
    col.pages = try pages.toOwnedSlice(src.arena);
}

fn encodingName(arena: std.mem.Allocator, encoding: schema.Encoding) ![]const u8 {
    return std.enums.tagName(schema.Encoding, encoding) orelse
        try std.fmt.allocPrint(arena, "UNKNOWN({d})", .{@backingInt(encoding)});
}

pub fn inspect(arena: std.mem.Allocator, env: std.process.Environ, path: []const u8, detail: bool) !Report {
    var src = try Source.open(arena, env, path);
    defer src.close();
    const meta = try src.footer();
    const tree = try zpq.core.parquet.schema_tree.SchemaTree.build(arena, meta.schema.items);
    const leaves = try arena.alloc(Leaf, tree.leaves.len);
    for (tree.leaves, leaves) |l, *out| out.* = .{
        .path = l.path,
        .physical_type = @tagName(l.type),
        .logical_type = l.logical_type,
        .converted_type = l.converted_type,
        .repetition = @tagName(l.repetition),
    };
    const groups = try arena.alloc(Group, meta.row_groups.items.len);
    for (meta.row_groups.items, groups, 0..) |rg, *group, i| {
        const cols = try arena.alloc(Column, rg.columns.items.len);
        for (rg.columns.items, cols, 0..) |chunk, *out, ci| {
            const cm = chunk.meta_data orelse return error.EncryptedMetadataUnsupported;
            const elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.BadMetadata;
            const encs = try arena.alloc([]const u8, cm.encodings.items.len);
            for (cm.encodings.items, encs) |e, *name| name.* = try encodingName(arena, e);
            const st = cm.statistics;
            var lo = try bound(
                arena,
                elem,
                if (st) |s| s.min_value orelse s.min else null,
                if (st) |s| s.is_min_value_exact else null,
            );
            var hi = try bound(
                arena,
                elem,
                if (st) |s| s.max_value orelse s.max else null,
                if (st) |s| s.is_max_value_exact else null,
            );
            if (st) |s| {
                if (s.min_value == null and s.min != null) {
                    lo.source = "deprecated-min";
                    lo.exactness = "unknown";
                }
                if (s.max_value == null and s.max != null) {
                    hi.source = "deprecated-max";
                    hi.exactness = "unknown";
                }
            }
            var hints: std.ArrayList([]const u8) = .empty;
            if (rg.num_rows < 1000) try hints.append(arena, "tiny group: fewer than 1000 rows");
            if (st == null) try hints.append(arena, "pruning unavailable: chunk statistics absent");
            if (chunk.column_index_offset == null)
                try hints.append(arena, "page pruning unavailable: column index absent");
            if (cm.total_compressed_size > 128 << 20)
                try hints.append(arena, "large chunk: compressed size exceeds 128 MiB");
            if (cm.total_uncompressed_size > 0 and cm.total_compressed_size >= cm.total_uncompressed_size)
                try hints.append(arena, "compression saves no bytes: compressed >= uncompressed");
            if (std.mem.eql(u8, fallback(cm), "present"))
                try hints.append(arena, "dictionary fallback: mixed data encodings in encoding_stats");
            out.* = .{
                .path = cm.path_in_schema.items,
                .physical_type = @tagName(cm.type),
                .logical_type = elem.logical_type,
                .converted_type = elem.converted_type,
                .num_values = cm.num_values,
                .codec = @tagName(cm.codec),
                .encodings = encs,
                .compressed_bytes = cm.total_compressed_size,
                .uncompressed_bytes = cm.total_uncompressed_size,
                .compression_ratio = if (cm.total_compressed_size > 0)
                    @as(f64, @floatFromInt(cm.total_uncompressed_size)) /
                        @as(f64, @floatFromInt(cm.total_compressed_size))
                else
                    null,
                .data_page_offset = cm.data_page_offset,
                .dictionary_page_offset = cm.dictionary_page_offset,
                .dictionary = presence(cm.dictionary_page_offset, null, src.size, true),
                .bounds_usable = zpq.core.parquet.statistics.chunkStats(&rg, ci, &meta, .corroborated).bounds != null,
                .dictionary_fallback = fallback(cm),
                .statistics = if (st == null) "absent" else "present",
                .null_count = if (st) |s| s.null_count else null,
                .nan_count = if (st) |s| s.nan_count else null,
                .min = lo,
                .max = hi,
                .column_order = if (meta.column_orders) |co| blk: {
                    if (ci >= co.items.len) break :blk "unsupported";
                    const float = cm.type == .FLOAT or cm.type == .DOUBLE;
                    break :blk switch (co.items[ci]) {
                        schema.COLUMN_ORDER_TYPE_DEFINED => "type-defined",
                        // Read like type-defined float bounds (see `statistics.boundsReadable`).
                        schema.COLUMN_ORDER_IEEE_754_TOTAL => if (float) "ieee754-total" else "unsupported",
                        else => "unsupported",
                    };
                } else "unknown",
                .column_index = presence(chunk.column_index_offset, chunk.column_index_length, src.size, false),
                .offset_index = presence(chunk.offset_index_offset, chunk.offset_index_length, src.size, false),
                .bloom_filter = presence(cm.bloom_filter_offset, cm.bloom_filter_length, src.size, true),
                .external_file = chunk.file_path,
                .encrypted = chunk.encrypted,
                .hints = try hints.toOwnedSlice(arena),
            };
            if (detail) {
                if (!chunk.encrypted and chunk.file_path == null) {
                    try details(&src, out, cm);
                } else {
                    out.detail_state = "unsupported";
                }
            }
        }
        group.* = .{ .ordinal = i, .rows = rg.num_rows, .uncompressed_bytes = rg.total_byte_size, .columns = cols };
    }
    return .{
        .path = path,
        .parquet_version = meta.version,
        .created_by = meta.created_by,
        .rows = meta.num_rows,
        .bytes = src.size,
        .footer_bytes = src.size - 8 - src.footer_start,
        .schema = leaves,
        .row_groups = groups,
        .io = .{
            .range_requests = src.requests,
            .requested_bytes = src.bytes,
            .detail_bytes = src.detail_bytes,
        },
    };
}

test "inspect typed bounds and metadata presence are conservative" {
    const a = std.testing.allocator;
    const se: schema.SchemaElement = .{
        .name = "n",
        .type = .INT32,
        .type_length = null,
        .repetition_type = null,
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const v = try bound(a, se, &.{ 0xff, 0xff, 0xff, 0x7f }, false);
    defer a.free(v.value.?);
    defer a.free(v.raw_hex.?);
    try std.testing.expectEqualStrings("2147483647", v.value.?);
    try std.testing.expectEqualStrings("inexact", v.exactness);
    try std.testing.expectEqualStrings("absent", presence(null, null, 100, false).state);
    try std.testing.expectEqualStrings("malformed", presence(90, 20, 100, false).state);
    try std.testing.expectEqualStrings("malformed", presence(null, 10, 100, false).state);
    try std.testing.expectEqualStrings("present", presence(80, null, 100, true).state);
    const bad = try bound(a, se, &.{1}, null);
    defer a.free(bad.raw_hex.?);
    try std.testing.expectEqualStrings("malformed", bad.state);
}

test "inspect reports a float chunk's NaN count, null when the writer left it out" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // parquet-java writes nan_count for every float chunk: row group 1 holds 4 NaN, row group 0 none.
    const path = "data/parquet-testing/data/floating_orders_nan_count.parquet";
    const rep = inspect(arena.allocator(), .empty, path, false) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    const rg0 = rep.row_groups[0].columns[0];
    try std.testing.expectEqualStrings("float_ieee754", rg0.path[0]);
    try std.testing.expectEqual(@as(?i64, 0), rg0.nan_count);
    try std.testing.expectEqual(@as(?i64, 4), rep.row_groups[1].columns[0].nan_count);
    // A non-float column never has one.
    const tiny = try inspect(arena.allocator(), .empty, "packaging/fixtures/tiny.parquet", false);
    for (tiny.row_groups[0].columns) |c| if (!std.mem.eql(u8, c.physical_type, "FLOAT") and !std.mem.eql(u8, c.physical_type, "DOUBLE"))
        try std.testing.expectEqual(@as(?i64, null), c.nan_count);
}

fn allocationProbe(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    _ = try inspect(arena.allocator(), .empty, "packaging/fixtures/tiny.parquet", true);
}

test "inspect allocations release on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}
