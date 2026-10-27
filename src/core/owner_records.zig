//! One owned block scatter for complete owner partitions; producers never retain borrowed decode views.
const std = @import("std");
pub const none = std.math.maxInt(u32);
const claim_blocks = 128;

pub fn Writer(comptime Record: type, comptime Count: type) type {
    return WriterRows(Record, Count, if (@sizeOf(Record) == 32) 16 else (512 - 8) / @sizeOf(Record));
}

pub fn WriterRows(comptime Record: type, comptime Count: type, comptime capacity: usize) type {
    std.debug.assert(capacity > 0);
    return struct {
        pub const block_rows = capacity;
        pub const Block = extern struct { next: u32 = none, len: u32 = 0, rows: [block_rows]Record };

        pub const Frontier = struct {
            heads: []u32 = &.{},
            tails: []u32 = &.{},
            histogram: []Count = &.{},
            free_next: u32 = 0,
            free_end: u32 = 0,

            pub fn init(a: std.mem.Allocator, parts: usize) !Frontier {
                var f: Frontier = .{};
                errdefer f.deinit(a);
                f.heads = try a.alloc(u32, parts);
                f.tails = try a.alloc(u32, parts);
                f.histogram = try a.alloc(Count, parts);
                @memset(f.heads, none);
                @memset(f.tails, none);
                @memset(f.histogram, 0);
                return f;
            }

            pub fn deinit(f: Frontier, a: std.mem.Allocator) void {
                a.free(f.heads);
                a.free(f.tails);
                a.free(f.histogram);
            }
        };

        pub fn blockCount(rows: usize, workers: usize, parts: usize) usize {
            // One partial tail per producer/partition, and one partially used claim per producer.
            return (rows +| (block_rows - 1)) / block_rows +| workers *| (parts +| claim_blocks);
        }

        pub fn frontierBytes(workers: usize, parts: usize) usize {
            return workers *| parts *| (2 * @sizeOf(u32) + @sizeOf(Count));
        }

        pub inline fn append(
            f: *Frontier,
            blocks: []Block,
            cursor: *std.atomic.Value(u32),
            p: usize,
            record: Record,
        ) error{ExceededMemoryBudget}!void {
            var tail = f.tails[p];
            if (tail == none or blocks[tail].len == block_rows) {
                if (f.free_next == f.free_end) {
                    const at = cursor.fetchAdd(claim_blocks, .monotonic);
                    if (at >= blocks.len) return error.ExceededMemoryBudget;
                    f.free_next = at;
                    f.free_end = @intCast(@min(blocks.len, @as(usize, at) + claim_blocks));
                }
                const fresh = f.free_next;
                f.free_next += 1;
                blocks[fresh].next = none;
                blocks[fresh].len = 0;
                if (tail == none) f.heads[p] = fresh else blocks[tail].next = fresh;
                tail = fresh;
                f.tails[p] = tail;
            }
            const b = &blocks[tail];
            b.rows[b.len] = record;
            b.len += 1;
            f.histogram[p] += 1;
        }
    };
}

test "owner records preserve full identities and independent partition tails through failed claims" {
    const Layout = @import("owner_lanes.zig").Layout(u128, &.{ .sum, .present_count }, u64);
    const W = Writer(Layout.Record, u64);
    const a = std.testing.allocator;
    var f = try W.Frontier.init(a, 2);
    defer f.deinit(a);
    const blocks = try a.alloc(W.Block, 2);
    defer a.free(blocks);
    var cursor: std.atomic.Value(u32) = .init(0);
    const record: Layout.Record = .{
        .key = std.math.maxInt(u128),
        .ordinal = 1 << 40,
        .weight = 1,
        .values = .{ -7, 1 },
    };
    for (0..W.block_rows) |_| try W.append(&f, blocks, &cursor, 0, record);
    try W.append(&f, blocks, &cursor, 1, record);
    try std.testing.expectEqual(@as(u64, W.block_rows), f.histogram[0]);
    try std.testing.expectEqual(@as(u64, 1), f.histogram[1]);
    try std.testing.expectError(error.ExceededMemoryBudget, W.append(&f, blocks, &cursor, 0, record));
    try std.testing.expectEqual(@as(u64, W.block_rows), f.histogram[0]);
    try std.testing.expectEqual(@as(u32, none), blocks[f.tails[0]].next);
    try std.testing.expectEqualDeep(record, blocks[f.heads[1]].rows[0]);
    try std.testing.expectEqual(@as(u32, 1), blocks[f.heads[1]].len);
}
