const std = @import("std");
const zpq = @import("zpq");
const io = zpq.io;
const sink = io.sink;
const s3_sink = io.s3_sink;
const local_sink = io.local_sink;
const xev = @import("xev");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test Local Sink
    {
        const file = try std.fs.cwd().createFile("/tmp/sink_test.txt", .{});
        var file_sink = local_sink.AsyncFileSink.init(file);
        const s = file_sink.sink();
        _ = try s.write("Hello Local Sink!\n");
        try s.close();
    }
    std.debug.print("Local Sink verified.\n", .{});

    // Test S3 Sink (Compilation & Init only)
    {
        var loop = try xev.Loop.init(.{});
        defer loop.deinit();

        var thread_pool = xev.ThreadPool.init(.{});
        defer {
            thread_pool.shutdown();
            thread_pool.deinit();
        }

        // We need to define the Xev type correctly for AsyncS3SinkGen
        const Xev = xev;
        const S3Sink = s3_sink.AsyncS3SinkGen(Xev);

        var s3 = try S3Sink.init(
            allocator,
            &loop,
            &thread_pool,
            "my-bucket",
            "us-east-1",
            "test-key",
            "fake-access",
            "fake-secret",
            null,
        );
        // Note: We don't write/close to avoid actual network IO in this basic probe
        // just verifying it compiles and initializes.
        s3.deinit();
    }
    std.debug.print("S3 Sink compiled and initialized.\n", .{});
}
