const std = @import("std");
const Morsel = @import("morsel.zig").Morsel;
const RowGroupMeta = @import("morsel.zig").RowGroupMeta;
const SinkChannel = @import("channel.zig").SinkChannel;
const S3Writer = @import("writer.zig").S3Writer;
const s3_proto = @import("../../../protocol/s3.zig");

const MIN_PART_SIZE = 5 * 1024 * 1024;

pub const SinkTask = struct {
    allocator: std.mem.Allocator,
    channel: *SinkChannel,
    writer: *S3Writer,

    // Internal State
    buffer: std.ArrayList(u8),
    pending_metadata: std.ArrayList(RowGroupMeta), // Metadata for morsels currently in buffer

    upload_id: ?[]const u8 = null,
    parts: std.ArrayList(s3_proto.S3.Part),
    next_part_number: u32 = 1,

    // In-flight tracking (simplified for synchronous Writer skeleton)
    // TODO: When Writer becomes async, need true in-flight tracking here.

    pub fn init(allocator: std.mem.Allocator, channel: *SinkChannel, writer: *S3Writer) SinkTask {
        return .{
            .allocator = allocator,
            .channel = channel,
            .writer = writer,
            .buffer = std.ArrayList(u8).init(allocator),
            .pending_metadata = std.ArrayList(RowGroupMeta).init(allocator),
            .parts = std.ArrayList(s3_proto.S3.Part).init(allocator),
        };
    }

    pub fn deinit(self: *SinkTask) void {
        self.buffer.deinit();
        self.pending_metadata.deinit();
        self.parts.deinit();
        if (self.upload_id) |id| self.allocator.free(id);
    }

    /// Main Consumer Loop.
    /// Blocks until channel is closed.
    pub fn run(self: *SinkTask) !void {
        while (true) {
            const maybe_morsel = try self.channel.recv();
            if (maybe_morsel) |morsel| {
                try self.handleMorsel(morsel);
            } else {
                // Channel closed and empty
                break;
            }
        }
        try self.finalize();
    }

    fn handleMorsel(self: *SinkTask, mut_morsel: Morsel) !void {
        var m = mut_morsel;
        defer m.deinit(self.allocator); // We take ownership of data/meta by copying or consuming

        // Append data to buffer
        try self.buffer.appendSlice(m.data);

        // Track metadata (for footer later)
        // We need to fixup offsets later, so store copy
        // For this port, we just store it.
        try self.pending_metadata.append(m.meta);

        // Check buffer size
        if (self.buffer.items.len >= MIN_PART_SIZE) {
            try self.flushPart();
        }
    }

    fn flushPart(self: *SinkTask) !void {
        // Ensure multipart started
        if (self.upload_id == null) {
            self.upload_id = try self.writer.initMultipartUpload();
        }

        const part_num = self.next_part_number;
        self.next_part_number += 1;

        // Upload
        const etag = try self.writer.uploadPart(self.upload_id.?, part_num, self.buffer.items);

        // Track part
        try self.parts.append(.{ .part_number = part_num, .etag = etag });

        // Clear buffer
        self.buffer.clearRetainingCapacity();
        // Note: pending_metadata should be moved to a "completed" list for parsing footer.
        // Omitted for brevity in initial port.
    }

    fn finalize(self: *SinkTask) !void {
        if (self.upload_id == null) {
            // Single PUT
            if (self.buffer.items.len > 0) {
                // Add footer generation logic here (omitted)
                try self.writer.putObject(self.buffer.items);
            }
        } else {
            // Finish multipart
            if (self.buffer.items.len > 0) {
                try self.flushPart();
            }
            try self.writer.completeMultipartUpload(self.upload_id.?, self.parts.items);
        }
    }
};
