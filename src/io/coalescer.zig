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
