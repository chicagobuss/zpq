const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Range = struct {
    start: u64,
    end: u64,

    pub fn len(self: Range) u64 {
        return self.end - self.start;
    }
};

pub const Coalescer = struct {
    /// Coalesce a list of ranges into a smaller list of merged ranges.
    /// Ranges that are within `gap_threshold` bytes of each other are merged.
    /// The input `ranges` slice is not modified, but the returned slice is allocated.
    ///
    /// The input ranges do NOT need to be sorted; this function will sort them internally if needed,
    /// but since we can't easily clone/sort without allocation, the caller should ideally provide sorted ranges 
    /// OR we allocate a temp buffer for sorting.
    ///
    /// For this implementation, we assume the caller passes potentially unsorted ranges, so we allocate a copy to sort.
    pub fn coalesce(allocator: Allocator, ranges: []const Range, gap_threshold: u64) ![]const Range {
        if (ranges.len == 0) return &[_]Range{};

        var sorted = try allocator.alloc(Range, ranges.len);
        defer allocator.free(sorted);
        @memcpy(sorted, ranges);

        std.mem.sort(Range, sorted, {}, struct {
            fn lessThan(_: void, a: Range, b: Range) bool {
                return a.start < b.start;
            }
        }.lessThan);

        var merged = std.ArrayList(Range).empty;
        // We probably won't use more ranges than we started with
        try merged.ensureTotalCapacity(allocator, ranges.len);

        var current = sorted[0];

        for (sorted[1..]) |next| {
            // Check for overlap or proximity
            // Note: Range is [start, end), so end is exclusive? 
            // Standard parquet usage usually implies [start, start + len).
            // Let's assume inclusive start, exclusive end for standard zig slice semantics.
            // But wait, our usage in Executor implies 'end' is exclusive (start + len).
            
            // Gap = next.start - current.end
            // careful with underflow if next starts before current ends (overlap)
            if (next.start <= current.end + gap_threshold) {
                // Merge
                if (next.end > current.end) {
                    current.end = next.end;
                }
            } else {
                merged.appendAssumeCapacity(current);
                current = next;
            }
        }
        merged.appendAssumeCapacity(current);

        return merged.toOwnedSlice(allocator);
    }
};

/// Unmeasured S3 latency uses this gap: wide enough to join nearby column chunks, under the cap below.
pub const s3_gap_default: u64 = 1 << 20;
/// Cap on `bandwidth × first-byte latency`.
pub const s3_gap_max: u64 = 2 << 20;

/// Gap for one S3 object. `rtt_ns == 0` has not been measured, so the default applies. A measured latency uses
/// `min(s3_gap_max, bytes_per_s × rtt)`.
pub fn s3CoalesceGap(rtt_ns: u64, bytes_per_s: u64) u64 {
    if (rtt_ns == 0 or bytes_per_s == 0) return s3_gap_default;
    const gap = bytes_per_s * rtt_ns / std.time.ns_per_s;
    return @min(s3_gap_max, gap);
}

pub fn overlapsAny(r: Range, others: []const Range) bool {
    for (others) |o| {
        if (r.start < o.end and o.start < r.end) return true;
    }
    return false;
}

test "coalescer: basic merging" {
    const allocator = std.testing.allocator;
    
    const input = [_]Range{
        .{ .start = 0, .end = 100 },
        .{ .start = 150, .end = 200 }, // Gap 50
        .{ .start = 300, .end = 400 }, // Gap 100
    };
    
    // Gaps:
    // 0..100 -> 150..200 (gap 50)
    // 150..200 -> 300..400 (gap 100)
    
    // Threshold 60: Should merge first two
    // 0..200, 300..400
    const res1 = try Coalescer.coalesce(allocator, &input, 60);
    defer allocator.free(res1);
    
    try std.testing.expectEqual(@as(usize, 2), res1.len);
    try std.testing.expectEqual(@as(u64, 0), res1[0].start);
    try std.testing.expectEqual(@as(u64, 200), res1[0].end);
    try std.testing.expectEqual(@as(u64, 300), res1[1].start);
    try std.testing.expectEqual(@as(u64, 400), res1[1].end);
}

test "coalescer: unsorted input" {
    const allocator = std.testing.allocator;
    
    const input = [_]Range{
        .{ .start = 300, .end = 400 },
        .{ .start = 0, .end = 100 },
        .{ .start = 150, .end = 200 },
    };
    
    // Threshold 60: Should merge 0..100 and 150..200 -> 0..200
    const res = try Coalescer.coalesce(allocator, &input, 60);
    defer allocator.free(res);
    
    try std.testing.expectEqual(@as(usize, 2), res.len);
    try std.testing.expectEqual(@as(u64, 0), res[0].start);
    try std.testing.expectEqual(@as(u64, 200), res[0].end);
}

test "coalescer: large gap" {
    const allocator = std.testing.allocator;
     const input = [_]Range{
        .{ .start = 0, .end = 100 },
        .{ .start = 200, .end = 300 },
    };
    
    // Threshold 50: No merge
    const res = try Coalescer.coalesce(allocator, &input, 50);
    defer allocator.free(res);
    
    try std.testing.expectEqual(@as(usize, 2), res.len);
}

test "s3 coalesce gap is 1 MiB until latency is measured, then bandwidth times that latency" {
    try std.testing.expectEqual(@as(u64, 1 << 20), s3CoalesceGap(0, 80_000_000));
    try std.testing.expectEqual(@as(u64, 1 << 20), s3CoalesceGap(25 * std.time.ns_per_ms, 0));
    // 80 MB/s × 25 ms = 2_000_000, under the 2 MiB cap.
    try std.testing.expectEqual(@as(u64, 2_000_000), s3CoalesceGap(25 * std.time.ns_per_ms, 80_000_000));
    // A huge product stops at 2 MiB.
    try std.testing.expectEqual(s3_gap_max, s3CoalesceGap(std.time.ns_per_s, 80_000_000));
    // A short measured latency stays small: 80 MB/s × 1 ms.
    try std.testing.expectEqual(@as(u64, 80_000), s3CoalesceGap(std.time.ns_per_ms, 80_000_000));
}
