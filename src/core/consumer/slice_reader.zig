const RowSubgroup = @import("types.zig").RowSubgroup;
const std = @import("std");
const metadata = @import("../parquet/metadata.zig");
const schema = @import("../schema.zig");
const column_mod = @import("../parquet/column.zig");
const filter_eval = @import("../filter/eval.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const int96_mod = @import("../parquet/int96.zig");
const core_memory = @import("../memory.zig");
const retained_pages = @import("../retained_pages.zig");
const invariant = @import("../invariant.zig");
const types = @import("types.zig");
const RGSrc = types.RGSrc;
const DecodeSite = types.DecodeSite;
const enterChunk = types.enterChunk;
const DecodeOptions = column_mod.DecodeOptions;
const DecodeScratch = column_mod.DecodeScratch;
const SliceScratch = @import("../parquet/page.zig").SliceScratch;
const flbaWidth = @import("column.zig").flbaWidth;
const valuesCol = @import("column.zig").valuesCol;
const codedCol = @import("column.zig").codedCol;

/// One flat column chunk decoded a slice of rows at a time, into the lane `decodeWriteColumn` gives the whole chunk:
/// for a row group too big to hold decoded at once. Each reader keeps its own page buffers, so the memory held is a
/// slice of values plus a page per column (and a byte-array column's dictionary): fixed-width readers recycle theirs
/// (`scratch`); byte-array readers decode a page at a time onto `pages`, copy the slice's bytes out and free the page
/// once it is drained. A retained byte column keeps the dictionary on `dict_store` for the chunk and resets `pages`
/// between pages. Only decompression goes through `scratch`.
pub const SliceReader = struct {
    /// In place for the reader's life: a reader holds pointers into itself once it has decoded.
    raw: *Raw,
    scratch: *DecodeScratch,
    /// The scratch and page arena belong to the worker's `SliceScratch` and outlive this reader.
    retained: bool = false,
    /// A byte-array reader's page allocations (levels, delta lengths, reconstructed strings). Reset when a page drains.
    pages: *std.heap.ArenaAllocator,
    /// Retained byte-array dictionary. Chunk lifetime, not reset between pages.
    dict_store: ?*std.heap.ArenaAllocator = null,
    /// Non-retained byte-array dictionary until `holdDictionary`, and the probe's dictionary after `beginProbe`.
    held: std.mem.Allocator,
    dict_held: bool = false,
    /// Set by `chargePages`: what the pages draw from, and what the pages on `held` drew.
    page_budget: ?*core_memory.SharedBudget = null,
    held_drawn: usize = 0,
    levels: schema.Levels,
    dec: ?decimal_mod.Kind,
    exact_only: bool = false,
    float16: bool,
    int96: bool,
    left: usize,
    /// Data pages `skip` passed over without decompressing them.
    pages_passed: usize = 0,
    /// Only the levels are read (see `levelsOnly`): a count(col)'s column, read as its whole-chunk path reads it.
    levels_only: bool = false,
    /// The chunk it reads, re-marked as each slice starts (`enterChunk`).
    site: DecodeSite,

    const Raw = union(enum) {
        i32: column_mod.ColumnChunkReader(i32),
        i64: column_mod.ColumnChunkReader(i64),
        f32: column_mod.ColumnChunkReader(f32),
        f64: column_mod.ColumnChunkReader(f64),
        boolean: column_mod.ColumnChunkReader(bool),
        bytes: column_mod.ColumnChunkReader([]const u8),
    };

    /// A reader of leaf `ci`, or null when the column has no sliced form (nested); the caller then decodes the row
    /// group whole. `a` holds the slices' bookkeeping and must live until the last slice is used.
    pub fn init(
        a: std.mem.Allocator,
        gpa: std.mem.Allocator,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        src: RGSrc,
        ci: usize,
        decode_options_in: DecodeOptions,
    ) !?SliceReader {
        return initPooled(a, gpa, rg, meta, src, ci, decode_options_in, null);
    }

    /// As `init`. `pool`, when set, keeps the page scratch for the next row group.
    pub fn initPooled(
        a: std.mem.Allocator,
        gpa: std.mem.Allocator,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        src: RGSrc,
        ci: usize,
        decode_options_in: DecodeOptions,
        pool: ?*SliceScratch,
    ) !?SliceReader {
        if (!supports(rg, meta, ci)) return null;
        enterChunk(rg, ci);
        const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
        const levels = meta.getColumnLevels(cm.path_in_schema.items);
        const se = meta.getColumnSchema(cm.path_in_schema.items);
        const dec: ?decimal_mod.Kind = if (se) |e| decimal_mod.exactKind(&e) else null;
        if (dec) |k| {
            if (cm.type == .FIXED_LEN_BYTE_ARRAY and
                (k.byte_width == 0 or k.byte_width > decimal_mod.MAX_FLBA_BYTE_WIDTH))
                return error.DecimalByteWidthTooLarge;
        }
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
        const buf_off = start - @as(usize, @intCast(src.byte_origin));
        if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
        const chunk = src.bytes[buf_off .. buf_off + len];
        const slot = if (pool) |p| p.checkout() else null;
        const scratch = if (slot) |s| &s.scratch else blk: {
            const created = try a.create(DecodeScratch);
            created.* = .init(gpa);
            break :blk created;
        };
        errdefer if (slot == null) scratch.deinit();
        const pages = if (slot) |s| &s.pages else blk: {
            const created = try a.create(std.heap.ArenaAllocator);
            created.* = .init(gpa);
            break :blk created;
        };
        errdefer if (slot == null) pages.deinit();
        const raw = try a.create(Raw);
        raw.* = switch (cm.type) {
            inline .INT32, .INT64, .FLOAT, .DOUBLE, .BOOLEAN => |t| blk: {
                const T, const tag = switch (t) {
                    .INT32 => .{ i32, "i32" },
                    .INT64 => .{ i64, "i64" },
                    .FLOAT => .{ f32, "f32" },
                    .DOUBLE => .{ f64, "f64" },
                    .BOOLEAN => .{ bool, "boolean" },
                    else => comptime unreachable,
                };
                const Reader = column_mod.ColumnChunkReader(T);
                var r = Reader.initWithOptions(chunk, cm.codec, levels, a, decode_options, scratch);
                r.value_budget = @intCast(cm.num_values);
                break :blk @unionInit(Raw, tag, r);
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => blk: {
                const Reader = column_mod.ColumnChunkReader([]const u8);
                // The dictionary is the slot's chunk store. Decoder temps stay on the page arena, which resets
                // between pages. Compressed data pages replace one scratch buffer instead of accumulating there.
                const pages_alloc = if (slot) |s| s.pages.allocator() else a;
                var r = Reader.initWithOptions(chunk, cm.codec, levels, pages_alloc, decode_options, scratch);
                r.recycleDataPages();
                if (slot) |s| {
                    r.dictionary_arena = s.dictionary.allocator();
                    r.pages.dictionary_arena = s.dictionary.allocator();
                }
                r.value_budget = @intCast(cm.num_values);
                r.type_length = switch (cm.type) {
                    .FIXED_LEN_BYTE_ARRAY => flbaWidth(se),
                    .INT96 => int96_mod.INT96_WIDTH,
                    else => 0,
                };
                break :blk .{ .bytes = r };
            },
        };
        return .{
            .raw = raw,
            .scratch = scratch,
            .retained = slot != null,
            .pages = pages,
            .dict_store = if (slot != null and
                (cm.type == .BYTE_ARRAY or cm.type == .FIXED_LEN_BYTE_ARRAY or cm.type == .INT96))
                &slot.?.dictionary
            else
                null,
            .held = a,
            .levels = levels,
            .dec = dec,
            .exact_only = decode_options.decimal_exact_only,
            .float16 = if (se) |e| schema.isFloat16(e) else false,
            .int96 = cm.type == .INT96,
            .left = @intCast(cm.num_values),
            .site = .{ .rg = rg, .column = ci },
        };
    }

    /// Payloads remain owned by the admitted aggregate after this reader's scratch is reset.
    /// Dictionaries belong to the shared plan; each reader starts at its own containing page.
    pub fn seekSubgroup(self: *SliceReader, a: std.mem.Allocator, subgroup: RowSubgroup) !void {
        const col = subgroup.group.columns[self.site.column] orelse return error.BadColumn;
        var pi: usize = 0;
        while (pi + 1 < col.locations.len and col.locations[pi + 1].first_row_index <= subgroup.rows.lo) pi += 1;
        const loc = col.locations[pi];
        const first: usize = @intCast(loc.first_row_index);
        self.left = @as(usize, @intCast(self.site.rg.num_rows)) - first;
        switch (self.raw.*) {
            inline else => |*r| {
                const T = @TypeOf(r.*).Value;
                r.dictionary = if (col.dictionary) |d| d.get(T) else null;
                r.value_budget = self.left;
                r.pages.ordinal = @intCast(pi + @intFromBool(col.dictionary != null));
                // A shared dictionary's backing never becomes page-local or scratch-local storage.
                if (T == []const u8) {
                    self.dict_held = true;
                    r.arena = self.pages.allocator();
                    r.pages.arena = self.pages.allocator();
                    self.armBytePage(r);
                }
                if (!try r.seekAndInstallPage(loc.offset, col.chunk_offset)) return error.ShortDecode;
            },
        }
        try self.skip(a, subgroup.rows.lo - first);
        self.left = subgroup.rows.hi - subgroup.rows.lo;
    }

    pub fn retainPayloads(self: *SliceReader, owner: *retained_pages.Chunk) void {
        switch (self.raw.*) {
            .bytes => |*r| r.pages.retained = owner,
            else => unreachable,
        }
    }

    pub fn deinit(self: *SliceReader) void {
        if (self.retained) {
            // Both stores are this row group's. The next group must not inherit them on top of its own page.
            _ = self.pages.reset(.free_all);
            if (self.dict_store) |d| _ = d.reset(.free_all);
            // The data page and its dictionary stay in the slot scratch for the next row group. Level buffers
            // used to die with the page arena; on the scratch they would keep that charge until the pool died.
            self.releaseLevelScratch();
        } else {
            self.scratch.deinit();
            self.pages.deinit();
        }
        if (self.page_budget) |b| {
            b.release(self.held_drawn);
            switch (self.raw.*) {
                .bytes => |*r| b.release(r.pages.drawn),
                else => {},
            }
        }
    }

    /// Level buffers of a retained byte-array reader. Its recycled page and dictionary stay, so the next row group
    /// reuses those two. Fixed-width slots keep their levels too; a byte-array page arena used to free these.
    fn releaseLevelScratch(self: *SliceReader) void {
        switch (self.raw.*) {
            .bytes => {},
            else => return,
        }
        const s = self.scratch;
        s.gpa.free(s.def_levels);
        s.gpa.free(s.rep_levels);
        s.def_levels = &.{};
        s.rep_levels = &.{};
    }

    /// Whether leaf `ci` has a sliced form (what `init` returns non-null for).
    pub fn supports(rg: *const schema.RowGroup, meta: *const schema.FileMetaData, ci: usize) bool {
        const cm = rg.columns.items[ci].meta_data orelse return false;
        return meta.getColumnLevels(cm.path_in_schema.items).max_rep == 0;
    }

    /// The next `n` rows (fewer only at the chunk's end), on `a`.
    /// Read only definition levels from here on: a column that only count(col) reads, which the whole-chunk path
    /// counts from its levels without decoding a value (`decodeLevelsOnly`). Sliced the same way, a value that would
    /// not decode fails the query, or does not, whatever the budget.
    pub fn levelsOnly(self: *SliceReader) void {
        // Nested or REQUIRED, the whole-chunk path decodes the values after all (`readAllLevels` declines).
        if (self.levels.max_def > 0 and self.levels.max_rep == 0) self.levels_only = true;
    }

    /// Fresh error probe: separate the retained byte dictionary from page storage before any page is installed.
    pub fn beginProbe(self: *SliceReader) void {
        switch (self.raw.*) {
            .bytes => |*r| {
                r.dictionary_arena = self.held;
                r.pages.dictionary_arena = self.held;
                r.arena = self.pages.allocator();
                r.pages.arena = self.pages.allocator();
                self.dict_held = true;
            },
            else => {},
        }
    }

    /// Between probe units no value refers to the drained page. Keep only its decoded dictionary.
    pub fn releaseProbePage(self: *SliceReader) void {
        invariant.assert(self.pageLeft() == 0);
        // The probe reader is unpooled. A pooled slot's buffers belong to the worker pool.
        invariant.assert(!self.retained);
        _ = self.pages.reset(.free_all);
        const s = self.scratch;
        s.gpa.free(s.page);
        s.gpa.free(s.def_levels);
        s.gpa.free(s.rep_levels);
        s.page = &.{};
        s.def_levels = &.{};
        s.rep_levels = &.{};
    }

    pub fn pageLeft(self: *const SliceReader) usize {
        return switch (self.raw.*) {
            inline else => |*r| r.pageValuesLeft(),
        };
    }

    pub fn pageOrdinal(self: *const SliceReader) ?u32 {
        return switch (self.raw.*) {
            inline else => |*r| if (r.pages.ordinal == 0) null else r.pages.ordinal - 1,
        };
    }

    pub fn next(self: *SliceReader, a: std.mem.Allocator, n_want: usize) !filter_eval.Batch.Column {
        enterChunk(self.site.rg, self.site.column);
        const n = @min(n_want, self.left);
        self.left -= n;
        if (n != 0 and !self.levels_only and self.dec == null and !self.float16 and !self.int96) {
            switch (self.raw.*) {
                inline .i32, .i64, .bytes => |*r| if (r.options.any_dictionary and r.options.encoded)
                    return self.nextCoded(@TypeOf(r.*).Value, r, a, n),
                else => {},
            }
        }
        if (self.levels_only) {
            const max_def: u32 = @intCast(self.levels.max_def);
            const def = try a.alloc(u32, n);
            var got: usize = 0;
            switch (self.raw.*) {
                inline else => |*r| while (got < n) {
                    // As `decodeBytes`: a byte-array reader's drained page goes back before the next comes in, or a
                    // row group's pages would pile up on `pages`.
                    if (r.pageValuesLeft() == 0) {
                        if (@TypeOf(r.*).Value == []const u8) {
                            self.resetPages();
                            self.armBytePage(r);
                            if (!try r.nextPage()) break;
                            try self.holdDictionary(r);
                        } else if (!try r.nextPage()) break;
                        continue;
                    }
                    got += try r.readLevels(def[got..][0..@min(n - got, r.pageValuesLeft())]);
                },
            }
            if (got != n) return error.ShortDecode;
            return .{ .i64 = .{
                .values = &.{},
                .def_levels = def,
                .max_def = max_def,
                .has_nulls = std.mem.indexOfNone(u32, def, &.{max_def}) != null,
                .levels_only_rows = n,
            } };
        }
        switch (self.raw.*) {
            inline else => |*r, tag| {
                const T = @TypeOf(r.*).Value;
                const values = try a.alloc(T, n);
                // A proven all-present numeric page needs no per-slice levels either. Byte-array values keep
                // decodeBytes' page/dictionary lifetime handling when a window crosses page boundaries.
                const present = if (T != []const u8 and self.levels.max_def == 1)
                    try r.decodeAllPresent(values)
                else
                    0;
                const def: ?[]u32 = if (self.levels.max_def > 0 and present != n) try a.alloc(u32, n) else null;
                if (def) |d| @memset(d[0..present], @as(u32, @intCast(self.levels.max_def)));
                const got = present + if (T == []const u8)
                    try self.decodeBytes(r, a, values, def)
                else
                    try decodeSome(T, r, values[present..], if (def) |d| d[present..] else null);
                if (got != n) return error.ShortDecode;
                const max_def: u32 = @intCast(self.levels.max_def);
                var has_nulls = false;
                if (def) |d| for (d) |v| {
                    if (v < max_def) {
                        has_nulls = true;
                        break;
                    }
                };
                const col: filter_eval.ColumnT(T) = .{
                    .values = values,
                    .def_levels = def,
                    .max_def = max_def,
                    .has_nulls = has_nulls,
                };
                if (self.dec) |k| {
                    if (self.exact_only and (T == i32 or T == i64)) {
                        var exact = col;
                        exact.unscaled = @unionInit(filter_eval.Exact, @typeName(T), col.values);
                        return @unionInit(filter_eval.Batch.Column, @typeName(T), exact);
                    }
                    return .{ .f64 = try exactSlice(T, a, col, k) };
                }
                if (T == []const u8 and self.int96) {
                    const out = try a.alloc(i64, n);
                    for (values, out) |b, *o| o.* = if (b.len == int96_mod.INT96_WIDTH)
                        try int96_mod.int96ToEpochNanos(b)
                    else
                        0;
                    const nanos: filter_eval.ColumnT(i64) = .{
                        .values = out,
                        .def_levels = def,
                        .max_def = max_def,
                        .has_nulls = has_nulls,
                    };
                    return .{ .i64 = nanos };
                }
                if (self.float16 and T == []const u8) {
                    const out = try a.alloc(f64, n);
                    for (values, out) |b, *o| o.* = if (b.len >= 2)
                        @floatCast(@as(f16, @bitCast(std.mem.readInt(u16, b[0..2], .little))))
                    else
                        0;
                    const half: filter_eval.ColumnT(f64) = .{
                        .values = out,
                        .def_levels = def,
                        .max_def = max_def,
                        .has_nulls = has_nulls,
                    };
                    return .{ .f64 = half };
                }
                return switch (tag) {
                    .bytes => .{ .string = col },
                    else => @unionInit(filter_eval.Batch.Column, @tagName(tag), col),
                };
            },
        }
    }

    fn nextCoded(
        self: *SliceReader,
        comptime T: type,
        r: *column_mod.ColumnChunkReader(T),
        a: std.mem.Allocator,
        n: usize,
    ) !filter_eval.Batch.Column {
        r.options.fast_levels = true;
        const codes = try a.alloc(u32, n);
        const max_def: u32 = @intCast(self.levels.max_def);
        const def: ?[]u32 = if (max_def > 0) try a.alloc(u32, n) else null;
        var got: usize = 0;
        while (got < n) {
            if (r.pageValuesLeft() == 0) {
                if (T == []const u8) {
                    self.resetPages();
                    r.releaseExpansion();
                    self.armBytePage(r);
                }
                if (!try r.nextPage()) return error.ShortDecode;
                if (T == []const u8) try self.holdDictionary(r);
                continue;
            }
            const want = @min(n - got, r.pageValuesLeft());
            if (!try r.readPageCodes(codes[got..][0..want], if (def) |d| d[got..][0..want] else null)) {
                const values = try a.alloc(T, n);
                const dict = r.dictionary;
                for (0..got) |i| values[i] = if (def != null and def.?[i] < max_def)
                    if (T == []const u8) "" else 0
                else
                    dict.?[codes[i]];
                r.options.fast_levels = false;
                const read = if (T == []const u8)
                    try self.decodeBytes(r, a, values[got..], if (def) |d| d[got..] else null)
                else
                    try decodeSome(T, r, values[got..], if (def) |d| d[got..] else null);
                if (read != n - got) return error.ShortDecode;
                const col = valuesCol(T, .{
                    .values = values,
                    .def_levels = def,
                    .has_nulls = r.has_nulls,
                }, self.levels);
                return @unionInit(filter_eval.Batch.Column, if (T == []const u8) "string" else @typeName(T), col);
            }
            got += want;
        }
        const dict = r.dictionary orelse return error.DictionaryMissing;
        const col = codedCol(T, .{ .codes = codes, .def_levels = def, .has_nulls = r.has_nulls }, dict, self.levels);
        return @unionInit(filter_eval.Batch.Column, if (T == []const u8) "string" else @typeName(T), col);
    }

    fn decodeSome(comptime T: type, r: *column_mod.ColumnChunkReader(T), values: []T, def: ?[]u32) !usize {
        var got: usize = 0;
        while (got < values.len) {
            const k = if (def) |d|
                try r.decodeWithLevels(values[got..], d[got..])
            else
                try r.decode(values[got..]);
            if (k == 0) break;
            got += k;
        }
        return got;
    }

    /// Byte arrays a page at a time: each page's values are copied onto `a` before the next page is read, and a
    /// drained page's bytes are freed, so the reader holds one page however many the slice spans.
    fn decodeBytes(
        self: *SliceReader,
        r: *column_mod.ColumnChunkReader([]const u8),
        a: std.mem.Allocator,
        values: [][]const u8,
        def: ?[]u32,
    ) !usize {
        var got: usize = 0;
        while (got < values.len) {
            if (r.pageValuesLeft() == 0) {
                self.resetPages();
                r.releaseExpansion();
                self.armBytePage(r);
                if (!try r.nextPage()) break;
                try self.holdDictionary(r);
                continue;
            }
            const want = @min(values.len - got, r.pageValuesLeft());
            const dst = values[got..][0..want];
            const k = if (def) |d| try r.decodeWithLevels(dst, d[got..][0..want]) else try r.decode(dst);
            if (k != want) return error.ShortDecode;
            if (r.pages.retained) |owner| {
                for (dst) |*value| if (!owner.contains(value.*)) {
                    // Expanded strings have a different capacity bound. Refuse the retained layout rather than
                    // misreporting its ownership limit as corrupt input; the rescannable scan retries normally.
                    return error.RetainedEncodingUnsupported;
                };
            } else _ = try copyBytes(a, dst);
            got += k;
        }
        return got;
    }

    /// Decompression of a retained column replaces two scratch buffers: the dictionary, then each data page.
    /// Decoder allocations stay on the page arena. Set before `nextPage`, which installs both in one call.
    pub fn armBytePage(self: *SliceReader, r: *column_mod.ColumnChunkReader([]const u8)) void {
        if (!self.retained) return;
        r.pages.scratch = self.scratch;
    }

    /// A byte-array chunk reads on `held` until its dictionary is in, which so lives as long as the reader without a
    /// copy (a copy off `pages` held it twice while it was made); its pages after that read on `pages`. The
    /// dictionary page comes first, so once a data page is in there is none to come.
    pub fn holdDictionary(self: *SliceReader, r: *column_mod.ColumnChunkReader([]const u8)) !void {
        if (self.dict_held) return;
        self.dict_held = r.dictionary != null or r.data_page_seen;
        if (!self.dict_held) return;
        r.arena = self.pages.allocator();
        r.pages.arena = self.pages.allocator();
        // Under `chargePages`, what is on `held` stays drawn until `deinit`: its pages, and the dictionary's slices
        // (drawn as it was installed).
        if (r.pages.budget != null) {
            self.held_drawn = r.pages.drawn;
            r.pages.drawn = 0;
        }
    }

    /// A byte-array chunk's pages on `raw`, each drawn from `budget` before it is decompressed (its inflated size)
    /// and given back as `pages` is reset, rather than on an arena that draws a node half again past each one. Right
    /// after `init`, before any page is read.
    pub fn chargePages(self: *SliceReader, raw: std.mem.Allocator, budget: *core_memory.SharedBudget) void {
        // Replacing `pages` would drop a retained slot's buffers without returning their charge.
        std.debug.assert(!self.retained);
        switch (self.raw.*) {
            .bytes => |*r| {
                self.pages.* = .init(raw);
                r.pages.budget = budget;
                self.page_budget = budget;
            },
            else => {},
        }
    }

    /// Reset `pages`, giving back what its pages drew. The dictionary store is the chunk's and stays.
    pub fn resetPages(self: *SliceReader) void {
        _ = self.pages.reset(.retain_capacity);
        if (self.retained) return;
        if (self.page_budget) |b| switch (self.raw.*) {
            .bytes => |*r| {
                b.release(r.pages.drawn);
                r.pages.drawn = 0;
            },
            else => {},
        };
    }

    /// Pass over the next `n` rows. A data page whose rows all fall in the skip is passed over by its header, never
    /// decompressed; within a page the rows decode onto `a` and are dropped. Lets a few rows of a row group be read
    /// without decoding the pages between them.
    pub fn skip(self: *SliceReader, a: std.mem.Allocator, n_want: usize) !void {
        const n = @min(n_want, self.left);
        self.left -= n;
        switch (self.raw.*) {
            inline else => |*r| {
                const T = @TypeOf(r.*).Value;
                var left = n;
                var discard: []T = &.{};
                var discard_def: []u32 = &.{};
                while (left > 0) {
                    if (r.pageValuesLeft() == 0) {
                        if (T == []const u8) {
                            self.resetPages();
                            r.releaseExpansion();
                            self.armBytePage(r);
                        }
                        const passed = try r.passPageWithin(left);
                        if (T == []const u8) try self.holdDictionary(r);
                        if (passed) |rows| {
                            left -= rows;
                            self.pages_passed += 1;
                            continue;
                        }
                        if (T == []const u8) self.armBytePage(r);
                        if (!try r.nextPage()) return error.ShortDecode;
                        if (T == []const u8) try self.holdDictionary(r);
                        continue;
                    }
                    const k = @min(left, r.pageValuesLeft(), 4096);
                    if (discard.len < k) {
                        discard = try a.alloc(T, k);
                        if (self.levels.max_def > 0) discard_def = try a.alloc(u32, k);
                    }
                    const got = if (self.levels.max_def > 0)
                        try r.decodeWithLevels(discard[0..k], discard_def[0..k])
                    else
                        try r.decode(discard[0..k]);
                    if (got != k) return error.ShortDecode;
                    left -= k;
                }
            },
        }
    }

    /// Re-point `vals` at one block of `a` holding their bytes.
    fn copyBytes(a: std.mem.Allocator, vals: [][]const u8) ![][]const u8 {
        var total: usize = 0;
        for (vals) |v| total += v.len;
        const buf = try a.alloc(u8, total);
        var off: usize = 0;
        for (vals) |*v| {
            @memcpy(buf[off..][0..v.len], v.*);
            v.* = buf[off..][0..v.len];
            off += v.len;
        }
        return vals;
    }

    /// A DECIMAL slice as `decimal_mod.decodeColumnExact` gives a chunk: the f64 view with the exact integers beside.
    fn exactSlice(
        comptime T: type,
        a: std.mem.Allocator,
        col: filter_eval.ColumnT(T),
        k: decimal_mod.Kind,
    ) !filter_eval.ColumnT(f64) {
        const out = try a.alloc(f64, col.values.len);
        var view: filter_eval.ColumnT(f64) = .{
            .values = out,
            .def_levels = col.def_levels,
            .max_def = col.max_def,
            .has_nulls = col.has_nulls,
        };
        switch (T) {
            i32, i64 => {
                decimal_mod.applyScaleSimd(T, col.values, k.scale, out);
                view.unscaled = @unionInit(filter_eval.Exact, @typeName(T), col.values);
            },
            []const u8 => {
                const u = try a.alloc(i128, col.values.len);
                for (col.values, out, u, 0..) |b, *o, *x, i| {
                    const present = if (col.def_levels) |d| d[i] >= col.max_def else true;
                    if (!present) {
                        o.* = 0;
                        x.* = 0;
                        continue;
                    }
                    if (k.physical == .INT96) {
                        if (b.len != int96_mod.INT96_WIDTH) return error.UnexpectedPage;
                        x.* = int96_mod.int96ToNanos(b);
                        o.* = @floatFromInt(x.*);
                        continue;
                    }
                    if (b.len > decimal_mod.MAX_FLBA_BYTE_WIDTH) return error.DecimalByteWidthTooLarge;
                    x.* = decimal_mod.flbaToI128(b);
                    o.* = decimal_mod.applyScaleI128(x.*, k.scale);
                }
                view.unscaled = .{ .i128 = u };
            },
            else => return error.UnsupportedDecimalPhysicalType,
        }
        return view;
    }
};

test "SliceReader.skip reads the rows after it as decoding every row would, passing whole pages unread" {
    const testing = std.testing;
    // V1 and V2 pages of about 40 rows, a dictionary turning PLAIN part-way through `s`, NULLs, NaN, DECIMAL
    // (tools/zpq-gen topn_gather).
    for ([_][]const u8{ "ci/fixtures/parquet/topn_gather_a.parquet", "ci/fixtures/parquet/topn_gather_b.parquet" }) |path| {
        const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) return error.SkipZigTest;
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
        const rg = &meta.row_groups.items[1];
        const picks = [_]usize{ 0, 1, 39, 40, 41, 300, 301, 302, 640, 998, 999 };
        for (0..rg.columns.items.len) |ci| {
            var whole = (try SliceReader.init(a, testing.allocator, rg, &meta, src, ci, .{})).?;
            defer whole.deinit();
            const all = try whole.next(a, @intCast(rg.num_rows));
            var sparse = (try SliceReader.init(a, testing.allocator, rg, &meta, src, ci, .{})).?;
            defer sparse.deinit();
            var at: usize = 0;
            for (picks) |p| {
                try sparse.skip(a, p - at);
                const one = try sparse.next(a, 1);
                at = p + 1;
                switch (all) {
                    inline else => |want, tag| {
                        const got = @field(one, @tagName(tag));
                        const null_want = if (want.def_levels) |d| d[p] < want.max_def else false;
                        const null_got = if (got.def_levels) |d| d[0] < got.max_def else false;
                        try testing.expectEqual(null_want, null_got);
                        if (!null_want) {
                            const T = @TypeOf(want.values[0]);
                            if (T == []const u8) {
                                try testing.expectEqualStrings(want.values[p], got.values[0]);
                            } else if (T == f64 and std.math.isNan(want.values[p])) {
                                try testing.expect(std.math.isNan(got.values[0]));
                            } else try testing.expectEqual(want.values[p], got.values[0]);
                        }
                    },
                }
            }
            try testing.expect(sparse.pages_passed > 0);
        }
    }
}


test "row subgroups seek misaligned columns and share dictionary backing across reader lifetimes" {
    const t = std.testing;
    const subgroups = @import("../row_subgroups.zig");
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try subgroups.testFixture(a);
    const rg = &fixture.meta.row_groups.items[0];
    const group = (try subgroups.Group.init(a, fixture.bytes, rg, &fixture.meta, &.{ true, true, true }, 8)).?;
    const src: RGSrc = .{ .bytes = fixture.bytes, .byte_origin = 0 };
    for (0..3) |ci| {
        var whole = (try SliceReader.init(a, t.allocator, rg, &fixture.meta, src, ci, .{})).?;
        defer whole.deinit();
        const expected = try whole.next(a, 4096);
        for (group.ranges) |range| {
            var r = (try SliceReader.init(a, t.allocator, rg, &fixture.meta, src, ci, .{})).?;
            defer r.deinit();
            try r.seekSubgroup(a, .{ .group = &group, .rows = range });
            if (ci == 1) try t.expect(r.raw.bytes.dictionary.?.ptr == group.columns[ci].?.dictionary.?.bytes.ptr);
            const got = try r.next(a, range.hi - range.lo);
            switch (expected) {
                inline .i64, .string => |want, tag| {
                    const actual = @field(got, @tagName(tag));
                    try t.expectEqual(range.hi - range.lo, actual.values.len);
                    for (actual.values, 0..) |value, i| {
                        const def = if (want.def_levels) |d| d[range.lo + i] else want.max_def;
                        try t.expectEqual(def, if (actual.def_levels) |d| d[i] else actual.max_def);
                        if (def < want.max_def) continue;
                        if (tag == .string) try t.expectEqualStrings(want.values[range.lo + i], value) else try t.expectEqual(want.values[range.lo + i], value);
                    }
                },
                else => unreachable,
            }
        }
    }
}

