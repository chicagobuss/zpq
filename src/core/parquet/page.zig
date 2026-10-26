//! Page reader: walk pages within a column chunk.
//!
//! A Parquet column chunk is a sequence of pages laid out back-to-back:
//!
//!   [PageHeader (thrift)][page payload][PageHeader][page payload]...
//!
//! For each page we parse the thrift header (which tells us the
//! compressed/uncompressed sizes and the page type), slice the payload,
//! and run it through the codec dispatcher.
//!
//! The first page of a dictionary-encoded column is the dictionary page;
//! subsequent pages are data pages that reference dictionary indices.
//! Higher-level orchestration (column.zig) decides what to do with each
//! page kind; this layer just walks them.

const std = @import("std");
const retained_pages = @import("../retained_pages.zig");
const core_memory = @import("../memory.zig");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const compression = @import("compression.zig");
const profile = @import("../../profile.zig");

pub const Error = error{
    UnexpectedEndOfChunk,
    BadPageHeader,
    NegativeSize,
    /// A page header's value / null / row count is negative, or claims more nulls than values.
    BadPageCount,
    /// The chunk's pages inflate past `PageReader.byte_limit`: past what the footer says the chunk holds.
    PageLargerThanChunk,
    /// `PageReader.budget` cannot take a page's inflated size.
    ExceededMemoryBudget,
    /// A retained-page layout cannot own this encoding. The scan retries its established decoder.
    RetainedEncodingUnsupported,
} || compression.Error;

pub const Page = struct {
    /// The parsed page header. Use `header.type`, `header.data_page_header`
    /// (when type=DATA_PAGE), or `header.dictionary_page_header` (when
    /// type=DICTIONARY_PAGE) to drive the next decoding step.
    header: schema.PageHeader,
    /// Decompressed page payload bytes. Lifetime = `arena`.
    bytes: []const u8,
};

/// Grow-only decode buffers owned by one worker and reused across pages, column chunks, row groups and files, so a
/// worker faults its decode working set in once instead of once per page from a fresh arena chunk.
///
/// Not thread-safe, and at most one ColumnChunkReader may draw on a scratch at a time. Lifetimes: `page` and the
/// level buffers are overwritten by the next page; `dict` by the next dictionary page. Only readers whose decoded
/// values are copies may route page bytes here — `[]const u8` values are slices into the page bytes and would dangle.
///
/// Safe builds enforce the one-reader rule (see `hold`); ReleaseFast compiles the check out.
pub const DecodeScratch = struct {
    gpa: std.mem.Allocator,
    page: []u8 = &.{},
    /// Decoded dictionary values for a fixed-width T, viewed as bytes.
    dict: []align(dict_align) u8 = &.{},
    def_levels: []u32 = &.{},
    rep_levels: []u32 = &.{},
    /// The owning worker's profile buffer: readers drawing on this scratch record their page decompressions there.
    prof: ?*profile.Thread = null,
    /// Ticket of the reader whose turn it is, and the last ticket issued. Safe builds only.
    owner: Ticket = no_ticket,
    issued: Ticket = no_ticket,
    /// Sliced-scan page and survivor buffers for this worker. Null on a slot scratch and until the first sliced row
    /// group. Freed here, when the worker releases its scratch, which is what gives the ledger's charge back.
    slice: ?*SliceScratch = null,

    pub const dict_align = 16;

    /// A reader's claim on a scratch. Zero-sized outside safe builds.
    pub const Ticket = if (std.debug.runtime_safety) u32 else void;
    pub const no_ticket: Ticket = if (std.debug.runtime_safety) 0 else {};

    /// Called on every reader entry point. A reader's first call takes the scratch, ending the previous reader's
    /// turn; each later call fails if another reader has taken it since, because this reader's page, levels or
    /// dictionary may then have been overwritten under it. Readers carry no deinit, so a turn ends when the next
    /// reader starts rather than at a release call, and a reader abandoned mid-chunk never blocks the next one.
    pub fn hold(self: *DecodeScratch, ticket: *Ticket) error{DecodeScratchInterleaved}!void {
        if (!std.debug.runtime_safety) return;
        if (ticket.* == no_ticket) {
            self.issued +%= 1;
            if (self.issued == no_ticket) self.issued +%= 1;
            ticket.* = self.issued;
            self.owner = ticket.*;
        } else if (self.owner != ticket.*) {
            return error.DecodeScratchInterleaved;
        }
    }

    pub fn init(gpa: std.mem.Allocator) DecodeScratch {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *DecodeScratch) void {
        if (self.slice) |pool| {
            const alloc = pool.alloc;
            self.slice = null;
            pool.deinit();
            alloc.destroy(pool);
        }
        self.gpa.free(self.page);
        self.gpa.free(self.dict);
        self.gpa.free(self.def_levels);
        self.gpa.free(self.rep_levels);
        self.* = undefined;
    }

    /// First `n` elements of `buf`, regrown (contents discarded) when too short. 1.5x headroom so a run of slightly
    /// larger pages doesn't trade one buffer of fresh pages for another every time.
    pub fn ensure(
        self: *DecodeScratch,
        comptime T: type,
        comptime alignment: usize,
        buf: *[]align(alignment) T,
        n: usize,
    ) std.mem.Allocator.Error![]align(alignment) T {
        if (buf.len < n) {
            const fresh = try self.gpa.alignedAlloc(T, .fromByteUnits(alignment), @max(n, buf.len + buf.len / 2));
            self.gpa.free(buf.*);
            buf.* = fresh;
        }
        return buf.*[0..n];
    }
};

/// Page buffers and the survivor mask of one worker's sliced scans. One slot per column in flight, grown to the
/// largest page that column has decompressed and then reused for every later row group. At the end of a row group
/// the pool is trimmed to at most one eighth of the ledger it is charged to, so the next group can draw the rest.
/// `alloc` is the ledger's
/// charged allocator when the scan has one: a buffer is drawn when it grows, and the draw stands until `deinit` (the
/// worker's release), which unmaps it. Safe builds fill a reused page with `reuse_poison` so a decoder that handed
/// back stale bytes fails instead of agreeing with itself.
pub const SliceScratch = struct {
    pub const reuse_poison: u8 = 0xA5;

    alloc: std.mem.Allocator,
    /// Owner of `alloc`. A stateless allocator is its vtable alone; it has no context pointer.
    owner_ctx: usize = 0,
    owner_vt: usize = 0,
    slots: std.ArrayList(Slot) = .empty,
    /// Next slot `checkout` hands out. `rewind` at the start of a pass, while no reader still holds a slot.
    next: usize = 0,
    survivors: []u64 = &.{},
    /// Filter slices and payload slices stay apart: each pass sizes itself from its own high water.
    /// `ArenaAllocator.init` allocates no backing.
    values: std.heap.ArenaAllocator,
    scan_values: std.heap.ArenaAllocator,

    pub const Slot = struct {
        scratch: DecodeScratch,
        pages: std.heap.ArenaAllocator,

        fn prepareReuse(self: *Slot) void {
            if (std.debug.runtime_safety) {
                @memset(self.scratch.page, reuse_poison);
                @memset(self.scratch.dict, reuse_poison);
                @memset(std.mem.sliceAsBytes(self.scratch.def_levels), reuse_poison);
                @memset(std.mem.sliceAsBytes(self.scratch.rep_levels), reuse_poison);
            }
            _ = self.pages.reset(.retain_capacity);
        }
    };

    pub fn init(alloc: std.mem.Allocator) SliceScratch {
        return .{
            .alloc = alloc,
            .values = .init(alloc),
            .scan_values = .init(alloc),
        };
    }

    pub fn deinit(self: *SliceScratch) void {
        for (self.slots.items) |*slot| {
            slot.scratch.deinit();
            slot.pages.deinit();
        }
        self.slots.deinit(self.alloc);
        self.alloc.free(self.survivors);
        self.values.deinit();
        self.scan_values.deinit();
        self.* = undefined;
    }

    /// Make room for `n` more checkouts. Call before the pass's readers exist: growing the slot list moves it.
    pub fn reserve(self: *SliceScratch, n: usize) !void {
        try self.slots.ensureTotalCapacity(self.alloc, self.next + n);
    }

    pub fn rewind(self: *SliceScratch) void {
        self.next = 0;
    }

    pub fn checkout(self: *SliceScratch) *Slot {
        if (self.next < self.slots.items.len) {
            const slot = &self.slots.items[self.next];
            self.next += 1;
            slot.prepareReuse();
            return slot;
        }
        self.slots.appendAssumeCapacity(.{
            .scratch = .init(self.alloc),
            .pages = .init(self.alloc),
        });
        self.next += 1;
        return &self.slots.items[self.next - 1];
    }

    /// Words for a row group's survivor mask, grown to the widest group and not freed before `deinit`.
    pub fn survivorWords(self: *SliceScratch, n_rows: usize) ![]u64 {
        const words: usize = if (n_rows == 0) 0 else (n_rows + 63) / 64;
        if (self.survivors.len < words) {
            const n = @max(words, self.survivors.len + self.survivors.len / 2);
            const fresh = try self.alloc.alloc(u64, n);
            self.alloc.free(self.survivors);
            self.survivors = fresh;
        }
        return self.survivors[0..words];
    }

    /// Decoded slice values and per-slice selections. Reset between slices; the chunks stay until `deinit`.
    pub fn valueArena(self: *SliceScratch) *std.heap.ArenaAllocator {
        return &self.values;
    }

    /// Payload slices. Separate from `valueArena` so a filter pass's high water is not this pass's row size.
    pub fn scanArena(self: *SliceScratch) *std.heap.ArenaAllocator {
        return &self.scan_values;
    }

    /// Bytes this pool still holds: every slot's decode buffers and page arena, both value arenas, and the
    /// survivor mask. Call only after the row group's readers are gone. A checked-out slot's dictionary and
    /// byte-array slices alias these buffers.
    pub fn retainedBytes(self: *const SliceScratch) usize {
        var n: usize = self.survivors.len * @sizeOf(u64);
        n += self.values.queryCapacity();
        n += self.scan_values.queryCapacity();
        for (self.slots.items) |*slot| {
            n += slot.scratch.page.len;
            n += slot.scratch.dict.len;
            n += slot.scratch.def_levels.len * @sizeOf(u32);
            n += slot.scratch.rep_levels.len * @sizeOf(u32);
            n += slot.pages.queryCapacity();
        }
        return n;
    }

    /// At the end of a row group, free the largest retained piece until the pool is within `bound`. Callers pass
    /// one eighth of the ledger this pool is charged to (`memory.retainedCarry`). A wider peak is a spike for the
    /// group that made it; the next group can draw the other seven eighths, and is refused if it still does not fit.
    pub fn trimAbove(self: *SliceScratch, bound: usize) void {
        while (self.retainedBytes() > bound) {
            if (!self.releaseLargest()) break;
        }
    }

    fn releaseLargest(self: *SliceScratch) bool {
        // 1 survivors, 2 value arena, 3 scan arena, then five ids per slot: page, dict, def, rep, page arena.
        var best: usize = 0;
        var pick: usize = 0;
        const note = struct {
            fn apply(best_n: *usize, pick_id: *usize, n: usize, id: usize) void {
                if (n > best_n.*) {
                    best_n.* = n;
                    pick_id.* = id;
                }
            }
        }.apply;
        note(&best, &pick, self.survivors.len * @sizeOf(u64), 1);
        note(&best, &pick, self.values.queryCapacity(), 2);
        note(&best, &pick, self.scan_values.queryCapacity(), 3);
        for (self.slots.items, 0..) |*slot, i| {
            const base = 4 + i * 5;
            const buffers = .{
                slot.scratch.page.len,
                slot.scratch.dict.len,
                slot.scratch.def_levels.len * @sizeOf(u32),
                slot.scratch.rep_levels.len * @sizeOf(u32),
            };
            inline for (buffers, 0..) |n, k| note(&best, &pick, n, base + k);
            note(&best, &pick, slot.pages.queryCapacity(), base + 4);
        }
        if (best == 0) return false;
        switch (pick) {
            1 => {
                self.alloc.free(self.survivors);
                self.survivors = &.{};
            },
            2 => _ = self.values.reset(.{ .retain_with_limit = 0 }),
            3 => _ = self.scan_values.reset(.{ .retain_with_limit = 0 }),
            else => {
                const slot = &self.slots.items[(pick - 4) / 5];
                switch ((pick - 4) % 5) {
                    0 => { slot.scratch.gpa.free(slot.scratch.page); slot.scratch.page = &.{}; },
                    1 => { slot.scratch.gpa.free(slot.scratch.dict); slot.scratch.dict = &.{}; },
                    2 => { slot.scratch.gpa.free(slot.scratch.def_levels); slot.scratch.def_levels = &.{}; },
                    3 => { slot.scratch.gpa.free(slot.scratch.rep_levels); slot.scratch.rep_levels = &.{}; },
                    else => _ = slot.pages.reset(.{ .retain_with_limit = 0 }),
                }
            },
        }
        return true;
    }
};

test "scratch above the decode allowance is released at row-group end" {
    var pool = SliceScratch.init(std.testing.allocator);
    defer pool.deinit();
    try pool.reserve(1);
    const slot = pool.checkout();
    // Callers pass one eighth of the ledger allowance. A page is enough to prove the release.
    const bound = std.heap.pageSize();
    const big = bound + 1;
    _ = try slot.scratch.ensure(u8, 1, &slot.scratch.page, big);
    try std.testing.expect(slot.scratch.page.len > bound);
    const arena = pool.scanArena();
    _ = try arena.allocator().alloc(u8, big);
    try std.testing.expect(slot.pages.queryCapacity() <= bound);
    try std.testing.expect(arena.queryCapacity() > bound);
    pool.trimAbove(bound);
    try std.testing.expect(slot.scratch.page.len <= bound);
    try std.testing.expect(arena.queryCapacity() <= bound);
    _ = try slot.scratch.ensure(u8, 1, &slot.scratch.page, 64);
    try std.testing.expect(slot.scratch.page.len >= 64);
}

test "scratch retention across columns stays within one decode allowance" {
    const page = std.heap.pageSize();
    var budget: core_memory.SharedBudget = .{ .limit = std.math.maxInt(usize) };
    var charged: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &budget };
    var pool = SliceScratch.init(charged.allocator());
    defer pool.deinit();
    try pool.reserve(3);
    // The slot list is charged too. The two page peaks are what have to fit beside it, and what trim frees.
    const overhead = budget.used.load(.monotonic);
    budget.limit = overhead + page * 6;
    const a = pool.checkout();
    const b = pool.checkout();
    _ = try a.scratch.ensure(u8, 1, &a.scratch.page, page * 3);
    _ = try b.scratch.ensure(u8, 1, &b.scratch.page, page * 3);
    try std.testing.expectEqual(overhead + page * 6, budget.used.load(.monotonic));
    pool.trimAbove(page * 3);
    try std.testing.expect(pool.retainedBytes() <= page * 3);
    try std.testing.expect(budget.used.load(.monotonic) <= overhead + page * 3);
    const c = pool.checkout();
    _ = try c.scratch.ensure(u8, 1, &c.scratch.page, page * 3);
}

test "row-group scratch keeps at most an eighth of the ledger it is charged to" {
    const page = std.heap.pageSize();
    var budget: core_memory.SharedBudget = .{ .limit = std.math.maxInt(usize) };
    var charged: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &budget };
    var pool = SliceScratch.init(charged.allocator());
    defer pool.deinit();
    try pool.reserve(1);
    const overhead = budget.used.load(.monotonic);
    const allowance = page * 8;
    budget.limit = overhead + allowance;
    const slot = pool.checkout();
    _ = try slot.scratch.ensure(u8, 1, &slot.scratch.page, page * 4);
    const carry = core_memory.retainedCarry(budget.limit);
    try std.testing.expect(pool.retainedBytes() > carry);
    pool.trimAbove(carry);
    try std.testing.expect(pool.retainedBytes() <= carry);
    // Seven eighths of the allowance remain, so the same peak fits. Nothing is evicted and retried.
    pool.rewind();
    const again = pool.checkout();
    _ = try again.scratch.ensure(u8, 1, &again.scratch.page, page * 4);
    try std.testing.expect(budget.used.load(.monotonic) <= budget.limit);
}

/// The ordinal in its column chunk (0 the first, a dictionary page counted) of the page this thread last began to
/// read: where a decode error was raised, for its message (`consumer.decodeFailure`).
pub threadlocal var last_page: ?u32 = null;
/// Encoding of that page, copied into the failure site before a worker exits.
pub threadlocal var last_encoding: ?schema.Encoding = null;

/// V1 declares level encodings independently of values; never interpret an unfamiliar one as RLE.
pub fn checkV1LevelEncodings(header: schema.DataPageHeader, levels: schema.Levels) error{UnsupportedEncoding}!void {
    const unsupported = if (levels.max_rep > 0 and header.repetition_level_encoding != .RLE)
        header.repetition_level_encoding
    else if (levels.max_def > 0 and header.definition_level_encoding != .RLE)
        header.definition_level_encoding
    else
        return;
    last_encoding = unsupported;
    return error.UnsupportedEncoding;
}

pub const PageReader = struct {
    /// The full column chunk bytes (compressed pages back-to-back).
    chunk: []const u8,
    pos: usize,
    /// Pages begun so far, for `last_page`.
    ordinal: u32 = 0,
    codec: schema.CompressionCodec,
    arena: std.mem.Allocator,
    /// Error probes keep dictionary backing bytes while releasing every data page before the next unit.
    dictionary_arena: ?std.mem.Allocator = null,
    /// When set, every page (dictionary included) is materialised into `scratch.page` and is only valid until the
    /// next `next()`. Null keeps the arena behaviour: page bytes live as long as `arena`.
    scratch: ?*DecodeScratch = null,
    /// Records a `decompress` span per compressed page when set. Separate from `scratch`, which byte-array readers
    /// leave unset.
    prof: ?*profile.Thread = null,
    /// The most bytes the chunk's pages may inflate to together, from its footer (`DecodeOptions.page_bytes_limit`):
    /// a page header claiming 1 GiB in a chunk whose footer says 32 KiB would otherwise be decompressed whole,
    /// outside any memory budget.
    byte_limit: usize = std.math.maxInt(usize),
    inflated: usize = 0,
    /// When set, a page's inflated size is drawn from it before the page is decompressed, and added to `drawn`,
    /// which the owner gives back as it frees the pages (an uncompressed page is the chunk's own bytes: nothing).
    budget: ?*core_memory.SharedBudget = null,
    drawn: usize = 0,
    /// A query-owned, prepaid payload region. Scratch reset and input release never invalidate these pages.
    retained: ?*retained_pages.Chunk = null,

    pub fn init(
        chunk: []const u8,
        codec: schema.CompressionCodec,
        arena: std.mem.Allocator,
    ) PageReader {
        return .{
            .chunk = chunk,
            .pos = 0,
            .codec = codec,
            .arena = arena,
        };
    }

    /// Read the next page. Returns null at end of chunk.
    ///
    /// V1 (DATA_PAGE / DICTIONARY_PAGE): the entire payload is
    /// compressed by the column codec. We decompress all of it and
    /// return the resulting bytes via `page.bytes`.
    ///
    /// V2 (DATA_PAGE_V2): per spec, levels are stored UNCOMPRESSED in
    /// the page body and only the values portion is optionally
    /// compressed (per the V2 header's `is_compressed` flag). Layout:
    ///
    ///     [rep_levels (rep_byte_len, uncompressed)]
    ///     [def_levels (def_byte_len, uncompressed)]
    ///     [values (compressed_page_size - rep_byte_len - def_byte_len bytes,
    ///       compressed iff is_compressed)]
    ///
    /// We materialise a single `bytes` slice that's `uncompressed_
    /// page_size` long, with levels copied verbatim and values
    /// decompressed in place. Downstream V2 decoders slice the result
    /// by the same level lengths and never need to know whether the
    /// source was compressed.
    pub fn next(self: *PageReader) Error!?Page {
        if (self.pos >= self.chunk.len) return null;
        last_page = self.ordinal;
        last_encoding = null;
        self.ordinal +|= 1;

        // Parse the thrift-encoded header. Reader.pos tells us how
        // many bytes we consumed.
        var rdr = thrift.Reader.init(self.chunk[self.pos..]);
        const header = schema.PageHeader.read(&rdr) catch return error.BadPageHeader;
        last_encoding = switch (header.type) {
            .DATA_PAGE => if (header.data_page_header) |h| h.encoding else null,
            .DATA_PAGE_V2 => if (header.data_page_header_v2) |h| h.encoding else null,
            .DICTIONARY_PAGE => if (header.dictionary_page_header) |h| h.encoding else null,
            .INDEX_PAGE => null,
        };
        const header_size = rdr.pos;

        if (header.compressed_page_size < 0 or header.uncompressed_page_size < 0) {
            return error.NegativeSize;
        }
        try checkCounts(header);
        if (self.retained != null) {
            const encoding = if (header.data_page_header) |h| h.encoding else if (header.data_page_header_v2) |h| h.encoding else null;
            if (encoding) |enc| switch (enc) {
                .PLAIN, .RLE_DICTIONARY, .PLAIN_DICTIONARY => {},
                else => return error.RetainedEncodingUnsupported,
            };
        }
        const csize: usize = @intCast(header.compressed_page_size);
        const usize_: usize = @intCast(header.uncompressed_page_size);

        if (self.pos + header_size + csize > self.chunk.len) {
            return error.UnexpectedEndOfChunk;
        }

        self.inflated +|= usize_;
        if (self.inflated > self.byte_limit) return error.PageLargerThanChunk;
        if (self.budget) |b| if (self.retained == null and (self.codec != .UNCOMPRESSED or header.type == .DATA_PAGE_V2)) {
            _ = b.drawExact(usize_) orelse return error.ExceededMemoryBudget;
            self.drawn += usize_;
        };

        const payload_start = self.pos + header_size;
        const payload = self.chunk[payload_start..][0..csize];
        self.pos = payload_start + csize;

        if (header.type == .DATA_PAGE_V2) {
            const v2 = header.data_page_header_v2 orelse return error.BadPageHeader;
            if (v2.repetition_levels_byte_length < 0 or v2.definition_levels_byte_length < 0) {
                return error.NegativeSize;
            }
            const rep_len: usize = @intCast(v2.repetition_levels_byte_length);
            const def_len: usize = @intCast(v2.definition_levels_byte_length);
            if (rep_len + def_len > csize) return error.UnexpectedEndOfChunk;

            const compressed_value_len = csize - rep_len - def_len;
            const value_uncompressed_len = if (usize_ >= rep_len + def_len)
                usize_ - rep_len - def_len
            else
                return error.UnexpectedEndOfChunk;

            const out = try self.pageBuffer(usize_);
            @memcpy(out[0..rep_len], payload[0..rep_len]);
            @memcpy(out[rep_len..][0..def_len], payload[rep_len..][0..def_len]);

            const value_src = payload[rep_len + def_len ..][0..compressed_value_len];
            const values_dst = out[rep_len + def_len ..][0..value_uncompressed_len];
            if (v2.is_compressed and self.codec != .UNCOMPRESSED) {
                const span = profile.Span.begin(self.prof);
                try compression.decompressInto(value_src, self.codec, values_dst);
                if (span.t != null) span.end(.decompress, self.pageArgs(header, value_src.len, values_dst.len));
            } else {
                if (compressed_value_len != value_uncompressed_len) return error.UnexpectedEndOfChunk;
                @memcpy(values_dst, value_src);
            }

            return .{ .header = header, .bytes = out };
        }

        // An UNCOMPRESSED page is a slice, not a copy: nothing to time.
        const span = profile.Span.begin(if (self.codec == .UNCOMPRESSED) null else self.prof);
        defer if (span.t != null) span.end(.decompress, self.pageArgs(header, csize, usize_));
        if (self.retained != null) {
            if (self.codec == .UNCOMPRESSED) {
                const out = try self.pageBuffer(payload.len);
                @memcpy(out, payload);
                return .{ .header = header, .bytes = out };
            }
            const out = try self.pageBuffer(usize_);
            try compression.decompressInto(payload, self.codec, out);
            return .{ .header = header, .bytes = out };
        }
        if (self.scratch == null or self.codec == .UNCOMPRESSED or usize_ == 0) {
            const a = if (header.type == .DICTIONARY_PAGE) self.dictionary_arena orelse self.arena else self.arena;
            const decompressed = try compression.decompress(a, payload, self.codec, usize_);
            return .{ .header = header, .bytes = decompressed };
        }
        const out = try self.pageBuffer(usize_);
        try compression.decompressInto(payload, self.codec, out);
        return .{ .header = header, .bytes = out };
    }

    pub const Peeked = struct {
        header: schema.PageHeader,
        /// Header plus compressed payload: what `pass` moves past.
        size: usize,
    };

    /// The next page's header, read without moving past it or decompressing anything. Null at the chunk's end.
    pub fn peek(self: *const PageReader) Error!?Peeked {
        if (self.pos >= self.chunk.len) return null;
        var rdr = thrift.Reader.init(self.chunk[self.pos..]);
        const header = schema.PageHeader.read(&rdr) catch return error.BadPageHeader;
        if (header.compressed_page_size < 0 or header.uncompressed_page_size < 0) return error.NegativeSize;
        try checkCounts(header);
        const size = rdr.pos + @as(usize, @intCast(header.compressed_page_size));
        if (size > self.chunk.len - self.pos) return error.UnexpectedEndOfChunk;
        return .{ .header = header, .size = size };
    }

    /// Move past a page `peek` returned, unread.
    pub fn pass(self: *PageReader, p: Peeked) void {
        self.pos += p.size;
    }

    fn pageArgs(self: *const PageReader, header: schema.PageHeader, in: usize, out: usize) profile.Args {
        const encoding: ?schema.Encoding = switch (header.type) {
            .DATA_PAGE => if (header.data_page_header) |h| h.encoding else null,
            .DATA_PAGE_V2 => if (header.data_page_header_v2) |h| h.encoding else null,
            .DICTIONARY_PAGE => if (header.dictionary_page_header) |h| h.encoding else null,
            .INDEX_PAGE => null,
        };
        return profile.args(.{ profile.pageTypes(self.codec, header.type, encoding), in, out });
    }

    /// Every decoder downstream casts these counts to usize (and V2 subtracts nulls from values), so a header
    /// that is negative or inconsistent here is rejected before any of them sees it.
    fn checkCounts(header: schema.PageHeader) Error!void {
        switch (header.type) {
            .DATA_PAGE => if (header.data_page_header) |h| {
                if (h.num_values < 0) return error.BadPageCount;
            },
            .DICTIONARY_PAGE => if (header.dictionary_page_header) |h| {
                if (h.num_values < 0) return error.BadPageCount;
            },
            .DATA_PAGE_V2 => if (header.data_page_header_v2) |h| {
                if (h.num_values < 0 or h.num_rows < 0 or h.num_nulls < 0) return error.BadPageCount;
                if (h.num_nulls > h.num_values) return error.BadPageCount;
            },
            .INDEX_PAGE => {},
        }
    }

    fn pageBuffer(self: *PageReader, len: usize) Error![]u8 {
        if (self.retained) |owner| return owner.take(len);
        if (self.scratch) |s| return s.ensure(u8, 1, &s.page, len);
        return self.arena.alloc(u8, len);
    }

    /// Reposition the reader's cursor to an absolute file offset.
    pub fn seekToPage(self: *PageReader, absolute_offset: i64, chunk_file_offset: i64) !void {
        if (absolute_offset < chunk_file_offset) return error.UnexpectedEndOfChunk;
        // Both come from the file (page index / column metadata); the difference of two hostile i64s can overflow.
        const off: usize = @intCast(std.math.sub(i64, absolute_offset, chunk_file_offset) catch return error.UnexpectedEndOfChunk);
        if (off > self.chunk.len) return error.UnexpectedEndOfChunk;
        self.pos = off;
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("metadata.zig");
const readFileSlice = metadata.readFileSlice;

test "iterate pages of one column from the bench fixture" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var meta = try metadata.open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);

    // Pick the int8 column (col[0]) — small + SNAPPY.
    const rg0 = &meta.row_groups.items[0];
    const col0 = rg0.columns.items[0].meta_data.?;

    // Locate the column chunk bytes. data_page_offset is the start of
    // page data; if there's a dictionary page it lives at
    // dictionary_page_offset (which precedes data_page_offset).
    const chunk_start: usize = if (col0.dictionary_page_offset) |dp|
        @intCast(dp)
    else
        @intCast(col0.data_page_offset);
    const chunk_len: usize = @intCast(col0.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var pr = PageReader.init(chunk, col0.codec, arena.allocator());

    var page_count: usize = 0;
    var total_decompressed: usize = 0;
    while (try pr.next()) |page| {
        page_count += 1;
        total_decompressed += page.bytes.len;
        // Sanity: decompressed size must match the header.
        try testing.expectEqual(@as(usize, @intCast(page.header.uncompressed_page_size)), page.bytes.len);
    }
    try testing.expect(page_count >= 1);
    try testing.expect(total_decompressed >= page_count); // at least 1 byte each

    std.debug.print(
        "[page] col[0] ({s}): {d} pages, {d} bytes decompressed total\n",
        .{ @tagName(col0.type), page_count, total_decompressed },
    );
}

test "page headers with negative or inconsistent counts are rejected before any decoder casts them" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const Case = struct { header: schema.PageHeader };
    const v1 = schema.DataPageHeader{ .num_values = -3, .encoding = .PLAIN, .definition_level_encoding = .RLE, .repetition_level_encoding = .RLE };
    const v2 = schema.DataPageHeaderV2{
        .num_values = 2,
        .num_nulls = 5, // more nulls than values: V2 PLAIN BOOLEAN decodes num_values - num_nulls
        .num_rows = 2,
        .encoding = .PLAIN,
        .definition_levels_byte_length = 0,
        .repetition_levels_byte_length = 0,
        .is_compressed = false,
    };
    const cases = [_]Case{
        .{ .header = .{ .type = .DATA_PAGE, .uncompressed_page_size = 0, .compressed_page_size = 0, .crc = null, .data_page_header = v1, .dictionary_page_header = null, .data_page_header_v2 = null } },
        .{ .header = .{ .type = .DATA_PAGE_V2, .uncompressed_page_size = 0, .compressed_page_size = 0, .crc = null, .data_page_header = null, .dictionary_page_header = null, .data_page_header_v2 = v2 } },
        .{ .header = .{ .type = .DICTIONARY_PAGE, .uncompressed_page_size = 0, .compressed_page_size = 0, .crc = null, .data_page_header = null, .dictionary_page_header = .{ .num_values = -1, .encoding = .PLAIN, .is_sorted = null }, .data_page_header_v2 = null } },
    };
    for (cases) |case| {
        var w = thrift.Writer.init(arena);
        try case.header.write(&w);
        var pr = PageReader.init(w.bytes(), .UNCOMPRESSED, arena);
        try testing.expectError(error.BadPageCount, pr.next());
    }

    // Two hostile i64 offsets whose difference overflows.
    var pr = PageReader.init("", .UNCOMPRESSED, arena);
    try testing.expectError(error.UnexpectedEndOfChunk, pr.seekToPage(std.math.maxInt(i64), -10));
}

test "slice scratch keeps page and survivor bytes charged until release" {
    var budget: core_memory.SharedBudget = .{ .limit = 1 << 20 };
    var charged: core_memory.Charged = .{ .child = testing.allocator, .budget = &budget };
    const alloc = charged.allocator();
    const pool = try alloc.create(SliceScratch);
    pool.* = .init(alloc);
    try pool.reserve(2);
    const first = pool.checkout();
    const page = try first.scratch.ensure(u8, 1, &first.scratch.page, 128);
    @memset(page, 1);
    const kept = first.scratch.page.ptr;
    const drawn = budget.used.load(.monotonic);
    try testing.expect(drawn >= 128);
    const second = pool.checkout();
    try testing.expect(first != second);
    pool.rewind();
    try testing.expectEqual(drawn, budget.used.load(.monotonic));
    const again = pool.checkout();
    try testing.expectEqual(kept, again.scratch.page.ptr);
    if (std.debug.runtime_safety) try testing.expectEqual(SliceScratch.reuse_poison, again.scratch.page[0]);
    const words = try pool.survivorWords(64);
    try testing.expectEqual(@as(usize, 1), words.len);
    const wide = try pool.survivorWords(10_000);
    try testing.expect(wide.len >= (10_000 + 63) / 64);
    try testing.expect(budget.used.load(.monotonic) >= drawn);
    const held = budget.used.load(.monotonic);
    const values = pool.valueArena();
    _ = try values.allocator().alloc(u8, 4096);
    try testing.expect(budget.used.load(.monotonic) > held);
    const after_values = budget.used.load(.monotonic);
    _ = values.reset(.retain_capacity);
    try testing.expectEqual(after_values, budget.used.load(.monotonic));
    const scan = pool.scanArena();
    _ = try scan.allocator().alloc(u8, 2048);
    try testing.expect(budget.used.load(.monotonic) > after_values);
    _ = scan.reset(.retain_capacity);
    pool.deinit();
    alloc.destroy(pool);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

// ----- File-read helper duplicated from metadata.zig tests -----
// (Kept local; both test sets read the same fixture but the helper is
// trivially small.)
