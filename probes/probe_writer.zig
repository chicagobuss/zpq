const std = @import("std");
const zpq = @import("zpq");
const io = zpq.io;
const core = zpq.core;
const schema = core.schema;
const writer_mod = core.writer;
const reader_mod = core.reader;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "/tmp/probe_writer_test.parquet";

    // 1. Create File & Writer
    {
        const file = try std.fs.cwd().createFile(path, .{});
        var sink = io.local_sink.AsyncFileSink.init(file);

        var schema_elements = std.ArrayListUnmanaged(schema.SchemaElement){};
        defer schema_elements.deinit(allocator);

        // Root
        try schema_elements.append(allocator, .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 2,
            .scale = null,
            .precision = null,
            .field_id = null,
        });

        // ID: INT32
        try schema_elements.append(allocator, .{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED, // Simple flat
            .name = "id",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = 1,
        });

        // Value: INT64
        try schema_elements.append(allocator, .{
            .type = .INT64,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = 2,
        });

        var writer = try writer_mod.ParquetWriter.init(allocator, sink.sink(), schema_elements.items);
        defer writer.deinit();

        // Write 10 rows
        var i: usize = 0;
        while (i < 10) : (i += 1) {
            const id: i32 = @intCast(i);
            const val: i64 = @intCast(i * 100);
            try writer.appendRow(.{ id, val });
        }

        try writer.close();
    }
    std.debug.print("Write complete. Verifying...\n", .{});

    // 2. Read & Verify
    {
        // Use AsyncFileSource
        const read_file = try std.fs.cwd().openFile(path, .{});
        var source = try io.local.AsyncFileSource.init(allocator, read_file);
        // source.close() destroys itself.
        defer source.deinit();

        const src_interface = source.randomAccessSource();

        const TestRow = struct {
            id: i32,
            value: i64,
        };

        var pq_file = core.file.ParquetFile.init(allocator, src_interface);
        defer pq_file.deinit();
        try pq_file.readFooter();

        var reader = try reader_mod.ParquetReader(TestRow).init(allocator, &pq_file);
        defer reader.deinit();
        std.debug.print("Metadata loaded. Rows: {d}\n", .{pq_file.metadata.num_rows});

        if (pq_file.metadata.num_rows != 10) return error.VerificationFailed;

        // Scan
        // Scan
        var batch_buf = try allocator.alloc(TestRow, 1024);
        defer allocator.free(batch_buf);

        var total: usize = 0;
        while (true) {
            const count = try reader.nextBatch(batch_buf, &.{});
            if (count == 0) break;

            const batch = batch_buf[0..count];

            for (batch, 0..) |row, i| {
                const idx = total + i; // Global index
                if (row.id != @as(i32, @intCast(idx))) {
                    std.debug.print("Mismatch at {d}: Expected id {d}, got {d}\n", .{ idx, idx, row.id });
                    return error.VerificationFailed;
                }
                if (row.value != @as(i64, @intCast(idx * 100))) {
                    std.debug.print("Mismatch at {d}: Expected value {d}, got {d}\n", .{ idx, idx * 100, row.value });
                    return error.VerificationFailed;
                }
            }
            total += count;
        }

        if (total != 10) return error.VerificationFailed;
    }

    std.debug.print("Verification passed!\n", .{});
}
