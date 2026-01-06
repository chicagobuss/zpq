const std = @import("std");

/// Result of pipeline execution.
pub const ExecutionResult = struct {
    input_rows: u64,
    output_rows: u64,
    elapsed_ms: f64,
};

/// Execution mode for pipeline operations.
pub const ExecutionMode = enum {
    /// Slot-based parallel writes via pwrite (default)
    slot_parallel,
    /// Morsel-based parallel S3 multipart upload (direct S3 streaming)
    morsel_parallel,
    /// Surgical page-level pruning using ColumnIndex/OffsetIndex
    /// Minimizes I/O by fetching only pages that might contain matching rows.
    surgical,
};

/// Simple trace helper - prints timing when ZPQ_TRACE=1
pub const Trace = struct {
    start: std.time.Instant,
    last: std.time.Instant,
    enabled: bool,

    pub fn init() Trace {
        const enabled = if (std.posix.getenv("ZPQ_TRACE")) |v| std.mem.eql(u8, v, "1") else false;
        const now = std.time.Instant.now() catch unreachable;
        return .{ .start = now, .last = now, .enabled = enabled };
    }

    pub fn mark(self: *Trace, comptime label: []const u8) void {
        if (!self.enabled) return;
        const now = std.time.Instant.now() catch return;
        const since_last = now.since(self.last);
        const since_start = now.since(self.start);
        std.debug.print("[TRACE] {s}: +{d:.2}ms (total {d:.2}ms)\n", .{
            label,
            @as(f64, @floatFromInt(since_last)) / 1_000_000.0,
            @as(f64, @floatFromInt(since_start)) / 1_000_000.0,
        });
        self.last = now;
    }
};
