//! Per-file streaming Parquet scan iterator (B4 step 1).
//!
//! Yields one row group's bytes at a time, fetching the column-chunk
//! ranges on demand instead of buffering the whole file upfront. This
//! is the input-side counterpart of B3's streaming output sink.
//!
//! Scope: lambda-specific for now (lives in src/lambda/, not core/).
//! When the CLI binary needs the same primitive — or when Phase C's
//! batch-iterator-as-primitive lands — this can lift into a more
//! general module. Don't pre-abstract.
//!
//! Memory model: at most ONE RG's column-chunk byte range resident
//! per active iterator. For a 10 GB Parquet with ~64 MB RGs, that's
//! 64 MB resident regardless of total file size.
//!
//! Layering: the iterator owns its column-chunk range computation but
//! delegates to `s3.fetchJobs` for the actual transport (which is the
//! same primitive the upfront-buffer code uses today). Switching from
//! "one fetchJobs across all RGs of all files" to "one fetchJobs per
//! RG" is the core of the B4 refactor.

const std = @import("std");
const Io = std.Io;
const zpq = @import("zpq");
const schema = zpq.core.schema;
const s3 = zpq.io.s3;
const coalescer = zpq.io.coalescer;

const COALESCE_GAP: u64 = 64 * 1024;

/// One row group's worth of fetched bytes plus the metadata pointing
/// into them. `raw_bytes` covers `[rg_byte_start, rg_byte_start + raw_bytes.len)`
/// of the source file — that is, a single contiguous span sized to
/// fit every kept column chunk. Gaps between non-adjacent kept columns
/// may or may not be filled depending on the coalescer's gap budget;
/// consumers slice into this buffer using each `ColumnChunk`'s
/// `data_page_offset - rg_byte_start`.
///
/// **Ownership transfer**: `next()` returns the result with
/// `raw_bytes` allocated via the iterator's `gpa`. The CALLER owns
/// the buffer and must `gpa.free(raw_bytes)` when done with it. The
/// iterator does not retain a reference, so it's safe for the caller
/// to call `next()` again before freeing the previous result.
/// (This shape is what enables a fetcher-worker pattern: a worker
/// can produce N RGs to a queue without each one stomping on the
/// previous.)
pub const RowGroupResult = struct {
    /// Cross-file ordering key. Stable from caller's spec list.
    file_idx: usize,
    /// RG index within the source file's row_groups list.
    rg_idx: usize,
    /// Pointer into the source FileMetaData (lives on the caller's
    /// per-file arena; valid for the iterator's lifetime).
    rg_meta: *const schema.RowGroup,
    /// Absolute byte offset in the source file where `raw_bytes[0]`
    /// belongs. Use this to translate a column's `data_page_offset`
    /// into a slice index: `raw_bytes[col.data_page_offset - rg_byte_start ..]`.
    rg_byte_start: u64,
    /// Caller-owned. Free via `gpa.free(raw_bytes)` when done.
    raw_bytes: []u8,
};

/// Configuration for which columns to fetch per RG. Two shapes:
///   - .all_kept: fetch the whole RG span [min(col_start), max(col_end)).
///     Used by the byte-copy fastpath when there's no projection
///     (every column survives, contiguous is cheaper than per-column).
///   - .columns: fetch only the listed leaf-column indices (within
///     each RG's `columns` list). Used by the encoder path and by
///     the projected fastpath. Coalesced if adjacent.
pub const FetchPolicy = union(enum) {
    all_kept: void,
    columns: []const usize,
};

/// Per-file scan state. Construct via `init`; drive via `next()` until
/// it returns `null`. Call `deinit()` when done to free the last RG's
/// resident buffer. The iterator does NOT own `meta`/`survivors`/`url`
/// — those live on the caller's per-file arena.
pub const PerFileScan = struct {
    pub const POOL_SIZE = 8;

    file_idx: usize,
    url: s3.Url,
    meta: *const schema.FileMetaData,
    survivors: []const bool,
    policy: FetchPolicy,
    /// 0-based index of the NEXT RG to consider on the upcoming `next()`.
    cursor: usize = 0,

    // Wiring for the actual fetch.
    pool: *s3.Pool(POOL_SIZE),
    creds: s3.Credentials,
    gpa: std.mem.Allocator,

    pub fn init(
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        pool: *s3.Pool(POOL_SIZE),
        file_idx: usize,
        url: s3.Url,
        meta: *const schema.FileMetaData,
        survivors: []const bool,
        policy: FetchPolicy,
    ) PerFileScan {
        return .{
            .file_idx = file_idx,
            .url = url,
            .meta = meta,
            .survivors = survivors,
            .policy = policy,
            .pool = pool,
            .creds = creds,
            .gpa = gpa,
        };
    }

    pub fn deinit(self: *PerFileScan) void {
        // No-op. Buffers are caller-owned (see `RowGroupResult`).
        _ = self;
    }

    /// Yield the next surviving RG's bytes. Returns `null` when the
    /// iterator is drained. `arena` is used for ephemeral
    /// per-fetch-call allocations (range lists, jobs lists). The
    /// returned `raw_bytes` is owned by the CALLER and must be freed
    /// via `gpa.free`.
    pub fn next(self: *PerFileScan, io: Io, arena: std.mem.Allocator) !?RowGroupResult {
        // Skip non-survivors.
        while (self.cursor < self.survivors.len and !self.survivors[self.cursor]) {
            self.cursor += 1;
        }
        if (self.cursor >= self.survivors.len) return null;

        const rg_idx = self.cursor;
        self.cursor += 1;
        const rg = &self.meta.row_groups.items[rg_idx];

        // 1. Build raw byte ranges for the chosen policy.
        var ranges: std.ArrayList(coalescer.Range) = .empty;
        try buildRanges(arena, rg, self.policy, &ranges);
        if (ranges.items.len == 0) {
            // Defensive: should not normally happen — caller ought to
            // have filtered out RGs with zero kept columns. Treat as
            // empty RG: yield no bytes and advance.
            return .{
                .file_idx = self.file_idx,
                .rg_idx = rg_idx,
                .rg_meta = rg,
                .rg_byte_start = 0,
                .raw_bytes = &.{},
            };
        }

        // 2. Compute the bounding window. The RG buffer covers
        // [min_start, max_end); fetched ranges fill the relevant
        // sub-slices, gaps are unfilled (and unread by consumers).
        var min_start: u64 = std.math.maxInt(u64);
        var max_end: u64 = 0;
        for (ranges.items) |r| {
            if (r.start < min_start) min_start = r.start;
            if (r.end > max_end) max_end = r.end;
        }

        // 3. Coalesce within the gap budget — same constant the lambda
        // uses today, kept consistent so a B4 run produces the same
        // network-level request shape as B3 within one RG.
        const merged = try coalescer.Coalescer.coalesce(arena, ranges.items, COALESCE_GAP);

        // 4. Allocate the RG buffer and dispatch one fetchJobs call.
        const rg_buf = try self.gpa.alloc(u8, max_end - min_start);
        errdefer self.gpa.free(rg_buf);

        var jobs: std.ArrayList(s3.FetchJob) = .empty;
        try jobs.ensureTotalCapacity(arena, merged.len);
        for (merged) |r| {
            const off_in_buf: usize = @intCast(r.start - min_start);
            const len: usize = @intCast(r.end - r.start);
            try jobs.append(arena, .{
                .bucket = self.url.bucket,
                .key = self.url.key,
                .range = .{ .start = r.start, .end = r.end },
                .target = rg_buf[off_in_buf .. off_in_buf + len],
            });
        }

        _ = try s3.fetchJobs(io, self.pool, self.gpa, arena, self.creds, jobs.items);

        return .{
            .file_idx = self.file_idx,
            .rg_idx = rg_idx,
            .rg_meta = rg,
            .rg_byte_start = min_start,
            .raw_bytes = rg_buf,
        };
    }
};

/// Translate a `FetchPolicy` into the per-RG byte ranges we need to
/// fetch. Mirrors the range-building loop in lambda/main.zig's
/// upfront fetch path so the network-level shape is identical at the
/// RG granularity.
fn buildRanges(
    arena: std.mem.Allocator,
    rg: *const schema.RowGroup,
    policy: FetchPolicy,
    out: *std.ArrayList(coalescer.Range),
) !void {
    switch (policy) {
        .all_kept => {
            // Whole-RG span, [min(col_start), max(col_end)).
            var min_s: u64 = std.math.maxInt(u64);
            var max_e: u64 = 0;
            for (rg.columns.items) |chunk| {
                const m = chunk.meta_data orelse continue;
                const s: u64 = if (m.dictionary_page_offset) |dp| @intCast(dp) else @intCast(m.data_page_offset);
                const e: u64 = s + @as(u64, @intCast(m.total_compressed_size));
                if (s < min_s) min_s = s;
                if (e > max_e) max_e = e;
            }
            if (min_s == std.math.maxInt(u64)) return;
            try out.append(arena, .{ .start = min_s, .end = max_e });
        },
        .columns => |cols| {
            for (cols) |ci| {
                if (ci >= rg.columns.items.len) continue;
                const m = rg.columns.items[ci].meta_data orelse continue;
                const s: u64 = if (m.dictionary_page_offset) |dp| @intCast(dp) else @intCast(m.data_page_offset);
                const e: u64 = s + @as(u64, @intCast(m.total_compressed_size));
                try out.append(arena, .{ .start = s, .end = e });
            }
        },
    }
}

// ============================================================
// Compile-time API check. No runtime test fixtures yet — those
// arrive in B4 step 3 when this is wired into a consumer.
// ============================================================
test "scan: API is well-typed" {
    _ = PerFileScan.init;
    _ = PerFileScan.next;
    _ = PerFileScan.deinit;
    _ = buildRanges;
}
