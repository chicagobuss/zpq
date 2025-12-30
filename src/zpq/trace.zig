const std = @import("std");

/// Compile-time flag to enable/disable tracing.
/// When false, all tracer methods compile to no-ops.
pub const enabled = true;

/// Aggregated performance metrics for a benchmark run.
/// Designed for minimal overhead: just increment counters in the hot path.
pub const Metrics = struct {
    // Timing buckets (nanoseconds)
    filter_decode_ns: u64 = 0,
    skip_ns: u64 = 0,
    materialize_ns: u64 = 0,
    loop_overhead_ns: u64 = 0,

    // Counters
    rows_scanned: u64 = 0,
    rows_selected: u64 = 0,
    rows_skipped: u64 = 0,
    batches_processed: u64 = 0,
    batches_skipped: u64 = 0,
    row_groups_scanned: u64 = 0,
    row_groups_skipped: u64 = 0,
    pages_decoded: u64 = 0,
    bytes_decompressed: u64 = 0,

    /// Add another Metrics struct to this one (for aggregating across row groups)
    pub fn add(self: *Metrics, other: Metrics) void {
        self.filter_decode_ns += other.filter_decode_ns;
        self.skip_ns += other.skip_ns;
        self.materialize_ns += other.materialize_ns;
        self.loop_overhead_ns += other.loop_overhead_ns;
        self.rows_scanned += other.rows_scanned;
        self.rows_selected += other.rows_selected;
        self.rows_skipped += other.rows_skipped;
        self.batches_processed += other.batches_processed;
        self.batches_skipped += other.batches_skipped;
        self.row_groups_scanned += other.row_groups_scanned;
        self.row_groups_skipped += other.row_groups_skipped;
        self.pages_decoded += other.pages_decoded;
        self.bytes_decompressed += other.bytes_decompressed;
    }

    pub fn totalNs(self: Metrics) u64 {
        return self.filter_decode_ns + self.skip_ns + self.materialize_ns + self.loop_overhead_ns;
    }

    pub fn selectivity(self: Metrics) f64 {
        if (self.rows_scanned == 0) return 0;
        return @as(f64, @floatFromInt(self.rows_selected)) / @as(f64, @floatFromInt(self.rows_scanned));
    }

    pub fn throughputMValPerSec(self: Metrics) f64 {
        const total_s = @as(f64, @floatFromInt(self.totalNs())) / 1_000_000_000.0;
        if (total_s == 0) return 0;
        return @as(f64, @floatFromInt(self.rows_scanned)) / total_s / 1_000_000.0;
    }
};

/// Run metadata captured at benchmark start
pub const RunMetadata = struct {
    timestamp_ms: i64,
    git_commit: ?[]const u8 = null,
    file_path: []const u8,
    file_size_bytes: u64 = 0,
    num_columns: u32 = 0,
    num_row_groups: u32 = 0,
    total_rows: u64 = 0,
    filter_column: ?[]const u8 = null,
    filter_value: ?[]const u8 = null,
};

/// Main tracer - holds metrics and run metadata.
/// Zero-allocation after init. All operations are O(1).
pub const Tracer = struct {
    metrics: Metrics = .{},
    run: RunMetadata,
    timer: ?std.time.Timer = null,

    pub fn init(run: RunMetadata) Tracer {
        return .{ .run = run };
    }

    /// Start a scoped timer. Returns elapsed ns when stopped.
    pub fn startTimer(self: *Tracer) void {
        if (!enabled) return;
        self.timer = std.time.Timer.start() catch null;
    }

    /// Stop timer and return elapsed nanoseconds
    pub fn stopTimer(self: *Tracer) u64 {
        if (!enabled) return 0;
        if (self.timer) |*t| {
            const elapsed = t.read();
            self.timer = null;
            return elapsed;
        }
        return 0;
    }

    /// Record filter column decode time
    pub fn recordFilterDecode(self: *Tracer, elapsed_ns: u64, rows: u64) void {
        if (!enabled) return;
        self.metrics.filter_decode_ns += elapsed_ns;
        self.metrics.rows_scanned += rows;
    }

    /// Record skip operation
    pub fn recordSkip(self: *Tracer, elapsed_ns: u64, rows: u64) void {
        if (!enabled) return;
        self.metrics.skip_ns += elapsed_ns;
        self.metrics.rows_skipped += rows;
        self.metrics.batches_skipped += 1;
    }

    /// Record materialization of selected rows
    pub fn recordMaterialize(self: *Tracer, elapsed_ns: u64, rows: u64) void {
        if (!enabled) return;
        self.metrics.materialize_ns += elapsed_ns;
        self.metrics.rows_selected += rows;
        self.metrics.batches_processed += 1;
    }

    /// Record loop/dispatch overhead
    pub fn recordOverhead(self: *Tracer, elapsed_ns: u64) void {
        if (!enabled) return;
        self.metrics.loop_overhead_ns += elapsed_ns;
    }

    /// Record row group level stats
    pub fn recordRowGroup(self: *Tracer, scanned: bool) void {
        if (!enabled) return;
        if (scanned) {
            self.metrics.row_groups_scanned += 1;
        } else {
            self.metrics.row_groups_skipped += 1;
        }
    }

    /// Write JSON output to a buffer
    pub fn toJson(self: *const Tracer, allocator: std.mem.Allocator) ![]u8 {
        const m = &self.metrics;
        const r = &self.run;

        const git_commit_str = if (r.git_commit) |c| c else "null";
        const filter_col_str = if (r.filter_column) |c| c else "null";
        const filter_val_str = if (r.filter_value) |v| v else "null";

        return std.fmt.allocPrint(allocator,
            \\{{
            \\  "run": {{
            \\    "timestamp_ms": {d},
            \\    "git_commit": {s},
            \\    "file_path": "{s}",
            \\    "file_size_bytes": {d},
            \\    "num_columns": {d},
            \\    "num_row_groups": {d},
            \\    "total_rows": {d},
            \\    "filter_column": {s},
            \\    "filter_value": {s}
            \\  }},
            \\  "metrics": {{
            \\    "total_ms": {d:.3},
            \\    "filter_decode_ms": {d:.3},
            \\    "skip_ms": {d:.3},
            \\    "materialize_ms": {d:.3},
            \\    "loop_overhead_ms": {d:.3},
            \\    "rows_scanned": {d},
            \\    "rows_selected": {d},
            \\    "rows_skipped": {d},
            \\    "selectivity": {d:.6},
            \\    "throughput_mval_s": {d:.2},
            \\    "batches_processed": {d},
            \\    "batches_skipped": {d},
            \\    "row_groups_scanned": {d},
            \\    "row_groups_skipped": {d}
            \\  }}
            \\}}
            \\
        , .{
            r.timestamp_ms,
            if (r.git_commit != null) try std.fmt.allocPrint(allocator, "\"{s}\"", .{git_commit_str}) else @as([]const u8, "null"),
            r.file_path,
            r.file_size_bytes,
            r.num_columns,
            r.num_row_groups,
            r.total_rows,
            if (r.filter_column != null) try std.fmt.allocPrint(allocator, "\"{s}\"", .{filter_col_str}) else @as([]const u8, "null"),
            if (r.filter_value != null) try std.fmt.allocPrint(allocator, "\"{s}\"", .{filter_val_str}) else @as([]const u8, "null"),
            nsToMs(m.totalNs()),
            nsToMs(m.filter_decode_ns),
            nsToMs(m.skip_ns),
            nsToMs(m.materialize_ns),
            nsToMs(m.loop_overhead_ns),
            m.rows_scanned,
            m.rows_selected,
            m.rows_skipped,
            m.selectivity(),
            m.throughputMValPerSec(),
            m.batches_processed,
            m.batches_skipped,
            m.row_groups_scanned,
            m.row_groups_skipped,
        });
    }

    /// Write to file
    pub fn writeToFile(self: *const Tracer, path: []const u8) !void {
        const json = try self.toJson(std.heap.page_allocator);
        defer std.heap.page_allocator.free(json);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();
        try file.writeAll(json);
    }
};

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

/// Get current timestamp in milliseconds (for run metadata)
pub fn nowMs() i64 {
    const now = std.time.Instant.now() catch return 0;
    if (@hasField(@TypeOf(now.timestamp), "sec")) {
        return now.timestamp.sec * 1000 + @divFloor(now.timestamp.nsec, 1_000_000);
    } else if (@hasField(@TypeOf(now.timestamp), "tv_sec")) {
        return now.timestamp.tv_sec * 1000 + @divFloor(now.timestamp.tv_nsec, 1_000_000);
    } else {
        return 0;
    }
}

/// Scoped timer helper - automatically records elapsed time on scope exit
pub fn ScopedTimer(comptime record_fn: anytype) type {
    return struct {
        tracer: *Tracer,
        timer: std.time.Timer,
        count: u64,

        pub fn start(tracer: *Tracer) @This() {
            return .{
                .tracer = tracer,
                .timer = std.time.Timer.start() catch std.time.Timer{ .started = .{ .sec = 0, .nsec = 0 } },
                .count = 0,
            };
        }

        pub fn setCount(self: *@This(), count: u64) void {
            self.count = count;
        }

        pub fn stop(self: *@This()) void {
            const elapsed = self.timer.read();
            record_fn(self.tracer, elapsed, self.count);
        }
    };
}

// Test
test "Tracer basic usage" {
    var tracer = Tracer.init(.{
        .timestamp_ms = 1735470000000,
        .file_path = "test.parquet",
        .total_rows = 1000,
    });

    tracer.recordFilterDecode(1_000_000, 1000); // 1ms, 1000 rows
    tracer.recordSkip(500_000, 900); // 0.5ms, 900 rows
    tracer.recordMaterialize(200_000, 100); // 0.2ms, 100 rows
    tracer.recordRowGroup(true);

    try std.testing.expectEqual(@as(u64, 1000), tracer.metrics.rows_scanned);
    try std.testing.expectEqual(@as(u64, 100), tracer.metrics.rows_selected);
    try std.testing.expectEqual(@as(u64, 900), tracer.metrics.rows_skipped);
    try std.testing.expectApproxEqRel(@as(f64, 0.1), tracer.metrics.selectivity(), 0.001);
}
