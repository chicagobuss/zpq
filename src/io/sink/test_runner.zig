const std = @import("std");
const channel = @import("channel.zig");
const task = @import("task.zig");
const writer = @import("writer.zig");
const morsel = @import("morsel.zig");

test "Sink Task Integration" {
    const allocator = std.testing.allocator;

    // 1. Setup
    var chan = channel.SinkChannel.init(allocator, 10);
    defer chan.deinit();

    // Use Mock Writer behavior (writer.zig has stubs)
    var w = writer.S3Writer.init(allocator, "bucket", "key", "region");

    var t = task.SinkTask.init(allocator, &chan, &w);
    defer t.deinit();

    // 2. Spawn consumer in separate thread
    const Consumer = struct {
        t: *task.SinkTask,
        fn run(ctx: @This()) !void {
            try ctx.t.run();
        }
    };
    const thread = try std.Thread.spawn(.{}, Consumer.run, .{Consumer{ .t = &t }});

    // 3. Produce Morsels
    const data = try allocator.alloc(u8, 1024); // 1KB
    defer allocator.free(data);
    @memset(data, 'A');

    for (0..50) |i| {
        // Create morsel (owns its data)
        const m_data = try allocator.dupe(u8, data);
        const m = morsel.Morsel{
            .data = m_data,
            .meta = .{
                .row_group_index = i,
                .num_rows = 10,
                .total_byte_size = 1024,
                .columns = .{},
            },
        };
        try chan.send(m);
    }

    // 4. Close and Join
    chan.close();
    thread.join();
}
