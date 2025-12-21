const std = @import("std");
const io = @import("../interface.zig");
pub const Range = io.Range;

/// A merged request that may cover multiple original ranges.
pub const MergedRequest = struct {
    // The range to request from S3 (e.g. 0-100)
    request_range: Range,

    // Indices of the original ranges this request covers.
    // We store indices so the caller can map back to their buffers.
    original_indices: std.ArrayListUnmanaged(usize),
};

/// Merges adjacent/overlapping ranges based on a gap heuristic.
/// Ported from Polars/Arrow logic with ZPQ's Zero-Alloc optimization in mind.
///
/// Heuristic: Merge if gap < 12.5% of total request size, clamped to [1MB, 8MB].
pub fn mergeRanges(
    allocator: std.mem.Allocator,
    ranges: []const Range,
) !std.ArrayListUnmanaged(MergedRequest) {
    if (ranges.len == 0) return std.ArrayListUnmanaged(MergedRequest){};

    // 1. Sort ranges by start offset (assumed sorted for now, or caller must sort)
    // We'll assume caller sorts for this low-level function to avoid allocs if possible,
    // but for safety/correctness let's just create an index list and sort that if needed.
    // For now, let's assume input IS sorted by start.

    var merged = std.ArrayListUnmanaged(MergedRequest){};
    errdefer {
        for (merged.items) |*m| m.original_indices.deinit(allocator);
        merged.deinit(allocator);
    }

    // Start with the first range
    var current_req = MergedRequest{
        .request_range = ranges[0],
        .original_indices = std.ArrayListUnmanaged(usize){},
    };
    try current_req.original_indices.append(allocator, 0);

    var i: usize = 1;
    while (i < ranges.len) : (i += 1) {
        const next = ranges[i];

        // Calculate gap
        const current_end = current_req.request_range.end;

        // Handle overlap (should be merged)
        if (next.start < current_end) {
            current_req.request_range.end = @max(current_end, next.end);
            try current_req.original_indices.append(allocator, i);
            continue;
        }

        const gap = next.start - current_end;

        // Polars Heuristic:
        // gap_tolerance = (current_len.max(next_len) / 8).clamp(1MB, 8MB)
        const size_base = @max(current_req.request_range.len(), next.len());
        const MB = 1024 * 1024;
        const gap_tolerance = std.math.clamp(size_base / 8, 1 * MB, 8 * MB);

        // ZPQ Optimization: We can be MORE aggressive because we don't allocate the gap.
        // Let's stick to Polars for now as a baseline, but maybe double the max tolerance?
        // Let's use Polars exact logic first to pass the "Reference" check.

        if (gap <= gap_tolerance) {
            // Merge
            current_req.request_range.end = next.end;
            try current_req.original_indices.append(allocator, i);
        } else {
            // Push current and start new
            try merged.append(allocator, current_req);
            current_req = MergedRequest{
                .request_range = next,
                .original_indices = std.ArrayListUnmanaged(usize){},
            };
            try current_req.original_indices.append(allocator, i);
        }
    }

    try merged.append(allocator, current_req);
    return merged;
}

pub const CHUNK_SIZE = 64 * 1024 * 1024; // 64MB

/// Iterator that yields ranges of at most CHUNK_SIZE
pub const RangeSplitter = struct {
    range: Range,
    current: u64,
    chunk_size: u64,

    pub fn init(range: Range, chunk_size: u64) RangeSplitter {
        return .{
            .range = range,
            .current = range.start,
            .chunk_size = chunk_size,
        };
    }

    pub fn next(self: *RangeSplitter) ?Range {
        if (self.current >= self.range.end) return null;

        const end = @min(self.current + self.chunk_size, self.range.end);
        const chunk = Range{ .start = self.current, .end = end };
        self.current = end;
        return chunk;
    }
};

test "mergeRanges - simple merge" {
    const allocator = std.testing.allocator;

    // Gap = 100 bytes (small), should merge
    const ranges = &[_]Range{
        .{ .start = 0, .end = 1000 },
        .{ .start = 1100, .end = 2000 },
    };

    var merged = try mergeRanges(allocator, ranges);
    defer {
        for (merged.items) |*m| m.original_indices.deinit(allocator);
        merged.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 1), merged.items.len);
    try std.testing.expectEqual(@as(u64, 0), merged.items[0].request_range.start);
    try std.testing.expectEqual(@as(u64, 2000), merged.items[0].request_range.end); // 0..2000 (includes 100 byte gap)
}

test "mergeRanges - huge gap (no merge)" {
    const allocator = std.testing.allocator;

    // Gap = 10MB (large), should NOT merge (max tolerance is 8MB)
    const ranges = &[_]Range{
        .{ .start = 0, .end = 1000 },
        .{ .start = 10 * 1024 * 1024 + 2000, .end = 10 * 1024 * 1024 + 3000 },
    };

    var merged = try mergeRanges(allocator, ranges);
    defer {
        for (merged.items) |*m| m.original_indices.deinit(allocator);
        merged.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), merged.items.len);
}

test "mergeRanges - dynamic tolerance" {
    const allocator = std.testing.allocator;
    const MB = 1024 * 1024;

    // Large request (80MB), 12.5% is 10MB, but clamped to 8MB max.
    // Gap = 5MB. Should merge because 5MB < 8MB.
    const ranges = &[_]Range{
        .{ .start = 0, .end = 80 * MB },
        .{ .start = 85 * MB, .end = 90 * MB },
    };

    var merged = try mergeRanges(allocator, ranges);
    defer {
        for (merged.items) |*m| m.original_indices.deinit(allocator);
        merged.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 1), merged.items.len);
    try std.testing.expectEqual(@as(u64, 90 * MB), merged.items[0].request_range.end);
}

test "RangeSplitter - splits large range" {
    const range = Range{ .start = 0, .end = 100 };
    var splitter = RangeSplitter.init(range, 40);

    const r1 = splitter.next().?;
    try std.testing.expectEqual(@as(u64, 0), r1.start);
    try std.testing.expectEqual(@as(u64, 40), r1.end);

    const r2 = splitter.next().?;
    try std.testing.expectEqual(@as(u64, 40), r2.start);
    try std.testing.expectEqual(@as(u64, 80), r2.end);

    const r3 = splitter.next().?;
    try std.testing.expectEqual(@as(u64, 80), r3.start);
    try std.testing.expectEqual(@as(u64, 100), r3.end);

    try std.testing.expect(splitter.next() == null);
}
