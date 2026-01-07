const std = @import("std");
const xev = @import("xev");

const schema_mod = @import("schema.zig");
const thrift = @import("thrift.zig");
const s3_writer_mod = @import("../io/s3/writer.zig");
const factory = @import("../io/s3/factory.zig");

const SchemaElement = schema_mod.SchemaElement;
const Type = schema_mod.Type;
const FileMetaData = schema_mod.FileMetaData;
const RowGroup = schema_mod.RowGroup;
const ColumnChunk = schema_mod.ColumnChunk;
const ColumnMetaData = schema_mod.ColumnMetaData;

// ============================================================================
// Morsel Coordinator Types
// ============================================================================

/// State machine for coordinating parallel S3 multipart uploads.
///
/// States:
///   INIT → UPLOADING → DRAINING → FINALIZING → COMPLETING → DONE
///                ↓           ↓           ↓            ↓
///              ERROR       ERROR       ERROR        ERROR
pub const CoordinatorState = enum {
    /// Waiting to start multipart upload
    init,
    /// Parts in flight, accepting new morsels
    uploading,
    /// No more morsels, waiting for in-flight parts
    draining,
    /// All parts done, building and uploading footer
    finalizing,
    /// Footer uploaded, calling CompleteMultipartUpload
    completing,
    /// Success
    done,
    /// Failed
    err,
};

/// Metadata for a single column chunk within a row group.
pub const ColumnChunkMeta = struct {
    column_index: usize,
    /// Offset within the row group bytes
    relative_offset: u64,
    compressed_size: u64,
    uncompressed_size: u64,
    num_values: u64,
    /// File offset - calculated after all parts complete
    file_offset: u64 = 0,
};

/// Metadata for a single row group (morsel).
pub const RowGroupMeta = struct {
    row_group_index: usize,
    num_rows: u64,
    total_byte_size: u64,
    columns: []ColumnChunkMeta,
    /// File offset - calculated after all parts complete
    file_offset: u64 = 0,
};

/// Record of a successfully uploaded part.
pub const CompletedPart = struct {
    part_number: u32,
    etag: []const u8,
    size: u64,
    /// All row groups contained in this part (may be multiple if buffered)
    row_groups: []RowGroupMeta,
};

/// Record of a part currently being uploaded.
pub const InFlightPart = struct {
    part_number: u32,
    encoded_bytes: []const u8,
    /// All row groups contained in this part (may be multiple if buffered)
    row_groups: []RowGroupMeta,
    start_time: std.time.Instant,
    /// Upload context (set when upload starts)
    upload_ctx: ?*anyopaque = null,
    /// Whether upload is complete
    done: bool = false,
    /// Error if upload failed
    upload_error: ?anyerror = null,
    /// ETag from successful upload
    etag: ?[]const u8 = null,
};

/// Configuration for MorselCoordinator.
pub const CoordinatorConfig = struct {
    bucket: []const u8,
    key: []const u8,
    region: []const u8,
    /// Custom endpoint (for MinIO, R2, LocalStack)
    endpoint: ?[]const u8 = null,
    /// Maximum concurrent uploads (backpressure threshold)
    max_in_flight: usize = 8,
    /// AWS credentials
    access_key: ?[]const u8 = null,
    secret_key: ?[]const u8 = null,
    session_token: ?[]const u8 = null,
};

const log = std.log.scoped(.morsel);

/// Generic MorselCoordinator parameterized by xev backend.
pub fn MorselCoordinatorGen(comptime XevApi: type) type {
    const S3Writer = s3_writer_mod.S3WriterGen(XevApi);

    // S3 requires multipart parts to be at least 5MB (except the last part)
    const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        config: CoordinatorConfig,

        /// Current state
        state: CoordinatorState = .init,

        /// S3 writer for multipart upload
        s3_writer: ?*S3Writer = null,

        /// Multipart upload ID from S3
        upload_id: ?[]const u8 = null,

        /// Event loop
        loop: *XevApi.Loop,
        thread_pool: *xev.ThreadPool,

        /// Part tracking
        next_part_number: u32 = 1,
        in_flight: std.ArrayListUnmanaged(InFlightPart) = .{},
        completed: std.ArrayListUnmanaged(CompletedPart) = .{},

        /// Pending buffer for small morsels (accumulate until >= MIN_PART_SIZE)
        pending_buffer: std.ArrayListUnmanaged(u8) = .{},
        pending_metadata: std.ArrayListUnmanaged(RowGroupMeta) = .{},

        /// Schema and row group tracking
        schema: ?[]const SchemaElement = null,
        total_row_groups: usize = 0,
        submitted_count: usize = 0,

        /// Error tracking
        last_error: ?anyerror = null,

        /// Flag for deferred S3 operations (set in callbacks, processed in main loop)
        needs_multipart_init: bool = false,

        /// Statistics
        stats: Stats = .{},

        pub const Stats = struct {
            parts_uploaded: u64 = 0,
            bytes_uploaded: u64 = 0,
            upload_time_ns: u64 = 0,
        };

        /// Initialize coordinator with event loop and config.
        pub fn init(
            allocator: std.mem.Allocator,
            loop: *XevApi.Loop,
            thread_pool: *xev.ThreadPool,
            config: CoordinatorConfig,
        ) Self {
            return .{
                .allocator = allocator,
                .config = config,
                .loop = loop,
                .thread_pool = thread_pool,
            };
        }

        /// Clean up resources.
        pub fn deinit(self: *Self) void {
            // Free in-flight parts
            for (self.in_flight.items) |*part| {
                self.allocator.free(part.encoded_bytes);
                if (part.etag) |etag| self.allocator.free(etag);
                for (part.row_groups) |*rg| {
                    self.allocator.free(rg.columns);
                }
                self.allocator.free(part.row_groups);
            }
            self.in_flight.deinit(self.allocator);

            // Free completed parts
            for (self.completed.items) |*part| {
                self.allocator.free(part.etag);
                for (part.row_groups) |*rg| {
                    self.allocator.free(rg.columns);
                }
                self.allocator.free(part.row_groups);
            }
            self.completed.deinit(self.allocator);

            // Free pending buffers
            self.pending_buffer.deinit(self.allocator);
            for (self.pending_metadata.items) |*meta| {
                self.allocator.free(meta.columns);
            }
            self.pending_metadata.deinit(self.allocator);

            // Free upload_id
            if (self.upload_id) |id| {
                self.allocator.free(id);
            }

            // Free S3 writer
            if (self.s3_writer) |w| {
                w.deinit();
            }
        }

        /// Start accepting morsels. Multipart upload is deferred until we have >= 5MB.
        /// This allows us to use single PUT for small outputs.
        pub fn start(self: *Self, schema: []const SchemaElement, num_row_groups: usize) !void {
            if (self.state != .init) {
                return error.InvalidState;
            }

            self.schema = schema;
            self.total_row_groups = num_row_groups;

            // Create S3 writer (but don't start multipart yet - defer until we have enough data)
            var s3w = try S3Writer.initWithLoop(
                self.allocator,
                self.loop,
                self.thread_pool,
                self.config.bucket,
                self.config.key,
                self.config.region,
            );
            errdefer s3w.deinit();

            // Configure endpoint if custom
            if (self.config.endpoint) |endpoint| {
                const discovery = try factory.discoverS3Endpoint(
                    self.allocator,
                    endpoint,
                    self.config.region,
                );
                defer discovery.deinit(self.allocator);

                self.allocator.free(s3w.host);
                s3w.host = try self.allocator.dupe(u8, discovery.host);
                s3w.port = discovery.port;
                s3w.use_tls = discovery.use_tls;
                s3w.use_path_style = true;
            }

            // Set credentials
            if (self.config.access_key) |ak| {
                if (self.config.secret_key) |sk| {
                    try s3w.setCredentials(ak, sk, self.config.session_token);
                }
            }

            self.s3_writer = s3w;

            // NOTE: We do NOT call initMultipartUpload() here.
            // We defer it until we have >= MIN_PART_SIZE of data,
            // allowing us to use single PUT for small outputs (<5MB).
            // The actual init is triggered via the needs_multipart_init flag,
            // which is processed by the main loop (outside callback context).

            self.state = .uploading;
        }

        /// Process deferred S3 operations. Call this from the main loop, NOT from callbacks.
        /// Returns true if any operations were processed.
        pub fn processFlags(self: *Self) !bool {
            if (self.needs_multipart_init) {
                self.needs_multipart_init = false;
                try self.doMultipartInit();
                return true;
            }
            return false;
        }

        /// Actually perform the multipart init (must be called outside callback context).
        fn doMultipartInit(self: *Self) !void {
            if (self.upload_id != null) return; // Already started

            const s3w = self.s3_writer orelse return error.NoS3Writer;
            const upload_id = try s3w.initMultipartUpload();
            self.upload_id = try self.allocator.dupe(u8, upload_id);
            log.debug("Started multipart upload: {s}", .{upload_id});
        }

        /// Ensure multipart upload is initialized. Called when we have enough data to upload.
        fn ensureMultipartStarted(self: *Self) !void {
            if (self.upload_id != null) return; // Already started

            // Set flag for main loop to process (avoid nested loop.run in callbacks)
            self.needs_multipart_init = true;
            // Return error to indicate we need to defer
            return error.MultipartInitNeeded;
        }

        /// Get current in-flight count.
        pub fn inFlightCount(self: *Self) usize {
            var count: usize = 0;
            for (self.in_flight.items) |part| {
                if (!part.done) count += 1;
            }
            return count;
        }

        /// Submit an encoded row group for upload.
        /// May block (via event loop) if max_in_flight reached.
        /// Small morsels are buffered until >= MIN_PART_SIZE before uploading.
        pub fn submitMorsel(
            self: *Self,
            encoded_bytes: []const u8,
            metadata: RowGroupMeta,
        ) !u32 {
            if (self.state != .uploading) {
                return error.InvalidState;
            }

            self.submitted_count += 1;

            // Add to pending buffer
            try self.pending_buffer.appendSlice(self.allocator, encoded_bytes);

            // Copy metadata and track it
            const cols_copy = try self.allocator.dupe(ColumnChunkMeta, metadata.columns);
            errdefer self.allocator.free(cols_copy);
            var meta_copy = metadata;
            meta_copy.columns = cols_copy;
            try self.pending_metadata.append(self.allocator, meta_copy);

            // Only flush when buffer reaches MIN_PART_SIZE (5MB)
            // For smaller outputs, finalize() will handle with single PUT
            if (self.pending_buffer.items.len >= MIN_PART_SIZE) {
                try self.flushPendingBuffer();
            }

            // Return current part number (may not be uploaded yet if buffered)
            return self.next_part_number;
        }

        /// Signal that all morsels have been submitted.
        /// Must be called after processing all row groups, even if some were filtered out.
        /// This triggers the transition to draining/finalizing state.
        pub fn finishSubmissions(self: *Self) !void {
            if (self.state != .uploading) {
                return error.InvalidState;
            }

            log.debug("finishSubmissions: {d} morsels submitted, pending_buffer={d} bytes", .{
                self.submitted_count,
                self.pending_buffer.items.len,
            });

            // If we have remaining data < 5MB and already started multipart,
            // flush it as the last part (which can be any size)
            if (self.pending_buffer.items.len > 0 and self.upload_id != null) {
                try self.flushPendingBuffer();
            }

            self.state = .draining;
        }

        /// Flush the pending buffer as a single S3 part.
        /// Only called when we have >= MIN_PART_SIZE or this is the last flush.
        fn flushPendingBuffer(self: *Self) !void {
            if (self.pending_buffer.items.len == 0) {
                return;
            }

            // Ensure multipart upload is started (deferred from start())
            // If multipart init is needed but can't be done here (in callback context),
            // ensureMultipartStarted sets a flag and returns error.MultipartInitNeeded.
            // The main loop will process the flag and we'll flush on the next iteration.
            self.ensureMultipartStarted() catch |err| {
                if (err == error.MultipartInitNeeded) {
                    // Just return - the main loop will call processFlags(), then
                    // the next submitMorsel will trigger flush again
                    return;
                }
                return err;
            };

            // If we still don't have an upload_id, the main loop needs to process the flag first
            if (self.upload_id == null) {
                return;
            }

            // Backpressure: wait if too many in flight
            while (self.inFlightCount() >= self.config.max_in_flight) {
                try self.drainCompleted(false);
                try self.loop.run(.once);
            }

            // Assign part number
            const part_number = self.next_part_number;
            self.next_part_number += 1;

            // For the first part, we need to prepend "PAR1" magic header
            const is_first_part = part_number == 1;
            const header_size: usize = if (is_first_part) 4 else 0;

            // Calculate relative offsets within the combined buffer for each row group
            // Account for PAR1 header in first part
            var offset: u64 = 0;
            for (self.pending_metadata.items) |*meta| {
                meta.file_offset = offset; // Offset within this part - will be adjusted in finalize
                for (meta.columns) |*col| {
                    col.file_offset = offset + col.relative_offset;
                }
                offset += meta.total_byte_size;
            }

            // Take ownership of the buffer, prepending PAR1 header for first part
            const bytes_copy = try self.allocator.alloc(u8, header_size + self.pending_buffer.items.len);
            errdefer self.allocator.free(bytes_copy);

            if (is_first_part) {
                @memcpy(bytes_copy[0..4], "PAR1");
            }
            @memcpy(bytes_copy[header_size..], self.pending_buffer.items);

            // Take ownership of the row groups metadata slice
            const row_groups = try self.allocator.dupe(RowGroupMeta, self.pending_metadata.items);
            errdefer self.allocator.free(row_groups);

            // Track in-flight
            try self.in_flight.append(self.allocator, .{
                .part_number = part_number,
                .encoded_bytes = bytes_copy,
                .row_groups = row_groups,
                .start_time = try std.time.Instant.now(),
            });

            // Clear pending buffers (ownership transferred to in_flight)
            self.pending_metadata.clearRetainingCapacity();
            self.pending_buffer.clearRetainingCapacity();

            // Start upload for this part
            try self.startPartUpload(self.in_flight.items.len - 1);
        }

        /// Start upload for a specific in-flight part.
        /// Currently synchronous - will block until upload completes.
        /// TODO: Make this truly async with parallel uploads.
        fn startPartUpload(self: *Self, idx: usize) !void {
            const part = &self.in_flight.items[idx];
            const s3w = self.s3_writer orelse return error.NoS3Writer;
            const upload_id = self.upload_id orelse return error.NoUploadId;

            // Synchronous upload - blocks until complete
            const etag = s3w.uploadPartDirect(
                upload_id,
                part.part_number,
                part.encoded_bytes,
            ) catch |err| {
                part.upload_error = err;
                part.done = true;
                return;
            };

            // Store etag and mark done
            part.etag = etag;
            part.done = true;
        }

        /// Drain completed uploads and collect results.
        fn drainCompleted(self: *Self, wait_all: bool) !void {
            _ = wait_all;

            // Process completed parts
            var i: usize = 0;
            while (i < self.in_flight.items.len) {
                const part = &self.in_flight.items[i];
                if (part.done) {
                    if (part.upload_error) |uerr| {
                        self.state = .err;
                        self.last_error = uerr;
                        return uerr;
                    }

                    // Calculate elapsed time
                    const now = try std.time.Instant.now();
                    const elapsed = now.since(part.start_time);
                    self.stats.upload_time_ns += elapsed;
                    self.stats.parts_uploaded += 1;
                    self.stats.bytes_uploaded += part.encoded_bytes.len;

                    // Move to completed
                    try self.completed.append(self.allocator, .{
                        .part_number = part.part_number,
                        .etag = part.etag orelse return error.NoETag,
                        .size = part.encoded_bytes.len,
                        .row_groups = part.row_groups,
                    });

                    // Free bytes (row_groups ownership transferred)
                    self.allocator.free(part.encoded_bytes);

                    // Remove from in_flight
                    _ = self.in_flight.swapRemove(i);
                    continue;
                }
                i += 1;
            }

            // Check if we should finalize
            if (self.state == .draining and self.in_flight.items.len == 0) {
                try self.finalize();
            }
        }

        /// Build footer and complete upload.
        /// Handles both multipart (>= 5MB) and single PUT (< 5MB) cases.
        fn finalize(self: *Self) !void {
            self.state = .finalizing;

            const s3w = self.s3_writer orelse return error.NoS3Writer;

            // Check if we ever started multipart upload
            // If upload_id is null, all data is still in pending_buffer (< 5MB total)
            if (self.upload_id == null) {
                // Small output - use single PUT instead of multipart
                try self.uploadAsSinglePut(s3w);
                self.state = .done;
                return;
            }

            const upload_id = self.upload_id.?;

            // Sort completed parts by part number
            std.mem.sort(CompletedPart, self.completed.items, {}, struct {
                fn lessThan(_: void, a: CompletedPart, b: CompletedPart) bool {
                    return a.part_number < b.part_number;
                }
            }.lessThan);

            // Calculate cumulative byte offsets for footer
            // Each part can contain multiple row groups
            // Part 1 contains "PAR1" header (4 bytes) prepended during flushPendingBuffer
            // So row group data in part 1 starts at offset 4, subsequent parts at their cumulative offset
            var file_offset: u64 = 0;
            for (self.completed.items) |*part| {
                // For part 1, the PAR1 header is already included in part.size
                // Row group data starts at offset 4 within part 1
                const rg_start_in_part: u64 = if (part.part_number == 1) 4 else 0;

                for (part.row_groups) |*rg| {
                    const rg_base = file_offset + rg_start_in_part + rg.file_offset;
                    rg.file_offset = rg_base;
                    for (rg.columns) |*col| {
                        col.file_offset = rg_base + col.relative_offset;
                    }
                }
                file_offset += part.size;
            }

            // Build Parquet footer
            const footer_metadata = try self.buildFooter();
            defer self.allocator.free(footer_metadata);

            // Build complete footer with trailing magic: [metadata][4-byte length][PAR1]
            // The buildFooter already includes length and trailing PAR1
            // So footer_metadata is the complete footer

            // Upload footer as the next part
            const footer_part_number = self.next_part_number;
            const footer_etag = try s3w.uploadPartDirect(upload_id, footer_part_number, footer_metadata);
            defer self.allocator.free(footer_etag);

            self.state = .completing;

            // Build parts list for completion (row groups + footer)
            var parts = try self.allocator.alloc(S3Writer.UploadedPart, self.completed.items.len + 1);
            defer self.allocator.free(parts);

            for (self.completed.items, 0..) |completed, i| {
                parts[i] = .{
                    .part_number = completed.part_number,
                    .etag = completed.etag,
                };
            }
            // Add footer part
            parts[self.completed.items.len] = .{
                .part_number = footer_part_number,
                .etag = footer_etag,
            };

            // Complete the multipart upload
            try s3w.completeMultipartUploadDirect(upload_id, parts);

            self.state = .done;
        }

        /// Upload small files (< 5MB) as a single PUT request instead of multipart.
        /// All data is still in pending_buffer since we never flushed it.
        fn uploadAsSinglePut(self: *Self, s3w: *S3Writer) !void {
            log.info("Output < 5MB ({d} bytes), using single PUT instead of multipart", .{self.pending_buffer.items.len});

            // Calculate offsets for footer (data starts after PAR1 magic)
            var offset: u64 = 4;
            for (self.pending_metadata.items) |*meta| {
                meta.file_offset = offset;
                for (meta.columns) |*col| {
                    col.file_offset = offset + col.relative_offset;
                }
                offset += meta.total_byte_size;
            }

            // Move pending metadata to completed for buildFooter to use
            // Create a fake "completed part" with part_number 0 (unused for single PUT)
            // We transfer ownership of the row_groups slice (and their columns) to completed
            const row_groups = try self.allocator.dupe(RowGroupMeta, self.pending_metadata.items);
            try self.completed.append(self.allocator, .{
                .part_number = 0,
                .etag = "", // Not used for single PUT
                .size = self.pending_buffer.items.len,
                .row_groups = row_groups,
            });

            // Clear pending_metadata WITHOUT freeing columns - ownership transferred to completed
            self.pending_metadata.clearRetainingCapacity();

            // Build the footer
            const footer_bytes = try self.buildFooter();
            defer self.allocator.free(footer_bytes);

            // Build complete Parquet file: PAR1 + data + footer
            const total_size = 4 + self.pending_buffer.items.len + footer_bytes.len;
            var file_data = try self.allocator.alloc(u8, total_size);
            defer self.allocator.free(file_data);

            // PAR1 magic
            @memcpy(file_data[0..4], "PAR1");
            // Row group data
            @memcpy(file_data[4..][0..self.pending_buffer.items.len], self.pending_buffer.items);
            // Footer
            @memcpy(file_data[4 + self.pending_buffer.items.len ..], footer_bytes);

            // Upload as single PUT using S3Writer's writeAll + finish
            try s3w.writeAll(file_data);
            try s3w.finish();

            log.info("Single PUT upload complete: {d} bytes", .{total_size});
        }

        /// Build Parquet footer bytes from accumulated metadata.
        /// Returns owned slice that caller must free.
        fn buildFooter(self: *Self) ![]u8 {
            const schema_elems = self.schema orelse return error.NoSchema;

            // Calculate total rows across all row groups in all parts
            var total_rows: i64 = 0;
            for (self.completed.items) |part| {
                for (part.row_groups) |rg| {
                    total_rows += @intCast(rg.num_rows);
                }
            }

            // Build row groups from completed parts
            // Each part may contain multiple row groups (buffered together)
            var row_groups = std.ArrayListUnmanaged(RowGroup){};
            defer {
                for (row_groups.items) |*rg| {
                    for (rg.columns.items) |*col| {
                        if (col.meta_data) |*md| {
                            md.path_in_schema.deinit(self.allocator);
                            md.encodings.deinit(self.allocator);
                        }
                    }
                    rg.columns.deinit(self.allocator);
                }
                row_groups.deinit(self.allocator);
            }

            for (self.completed.items) |part| {
                for (part.row_groups) |rg_meta| {
                    var columns = std.ArrayListUnmanaged(ColumnChunk){};
                    errdefer columns.deinit(self.allocator);

                    for (rg_meta.columns) |col| {
                        // Build minimal ColumnMetaData
                        var path_in_schema = std.ArrayListUnmanaged([]const u8){};
                        // Get column name from schema (col.column_index + 1 because schema[0] is root)
                        if (col.column_index + 1 < schema_elems.len) {
                            try path_in_schema.append(self.allocator, schema_elems[col.column_index + 1].name);
                        }

                        const col_type = if (col.column_index + 1 < schema_elems.len)
                            schema_elems[col.column_index + 1].type orelse .BYTE_ARRAY
                        else
                            .BYTE_ARRAY;

                        var encodings = std.ArrayListUnmanaged(schema_mod.Encoding){};
                        try encodings.append(self.allocator, .PLAIN);

                        const meta_data = ColumnMetaData{
                            .type = col_type,
                            .encodings = encodings,
                            .path_in_schema = path_in_schema,
                            .codec = .SNAPPY, // Assume SNAPPY for now
                            .num_values = @intCast(col.num_values),
                            .total_uncompressed_size = @intCast(col.uncompressed_size),
                            .total_compressed_size = @intCast(col.compressed_size),
                            .data_page_offset = @intCast(col.file_offset),
                            .index_page_offset = null,
                            .dictionary_page_offset = null,
                            .statistics = null,
                        };

                        try columns.append(self.allocator, .{
                            .file_path = null,
                            .file_offset = @intCast(col.file_offset),
                            .meta_data = meta_data,
                            .offset_index_offset = null,
                            .offset_index_length = null,
                            .column_index_offset = null,
                            .column_index_length = null,
                        });
                    }

                    try row_groups.append(self.allocator, .{
                        .columns = columns,
                        .total_byte_size = @intCast(rg_meta.total_byte_size),
                        .num_rows = @intCast(rg_meta.num_rows),
                    });
                }
            }

            // Build schema ArrayListUnmanaged from slice
            var schema_list = std.ArrayListUnmanaged(SchemaElement){};
            defer schema_list.deinit(self.allocator);
            for (schema_elems) |elem| {
                try schema_list.append(self.allocator, elem);
            }

            // Build FileMetaData
            const metadata = FileMetaData{
                .version = 2,
                .schema = schema_list,
                .num_rows = total_rows,
                .created_by = "zpq-morsel",
                .row_groups = row_groups,
            };

            // Serialize to Thrift
            var writer = thrift.Writer.init(self.allocator);
            defer writer.deinit();
            try metadata.write(&writer);

            const footer_data = writer.bytes();

            // Build complete footer: [metadata][4-byte length][PAR1]
            const footer_len: u32 = @intCast(footer_data.len);
            const total_len = footer_data.len + 4 + 4; // metadata + length + magic

            var result = try self.allocator.alloc(u8, total_len);
            errdefer self.allocator.free(result);

            @memcpy(result[0..footer_data.len], footer_data);
            @memcpy(result[footer_data.len..][0..4], &std.mem.toBytes(footer_len));
            @memcpy(result[footer_data.len + 4 ..][0..4], "PAR1");

            return result;
        }

        /// Abort the multipart upload on error.
        pub fn abort(self: *Self) void {
            const s3w = self.s3_writer orelse return;
            const upload_id = self.upload_id orelse return;

            s3w.abortMultipartUpload(upload_id) catch |err| {
                std.log.err("Failed to abort multipart upload: {}", .{err});
            };

            self.state = .err;
        }

        /// Wait for coordinator to reach terminal state.
        pub fn waitForCompletion(self: *Self, timeout_ns: ?u64) !bool {
            const wait_start = try std.time.Instant.now();

            while (self.state != .done and self.state != .err) {
                try self.drainCompleted(false);

                if (timeout_ns) |timeout| {
                    const now = try std.time.Instant.now();
                    if (now.since(wait_start) > timeout) {
                        return false;
                    }
                }

                try self.loop.run(.once);
            }

            return self.state == .done;
        }

        /// Get statistics.
        pub fn getStats(self: *Self) Stats {
            return self.stats;
        }
    };
}

/// Default MorselCoordinator using xev.Dynamic for runtime backend selection.
pub const MorselCoordinator = MorselCoordinatorGen(xev.Dynamic);

// ============================================================================
// Tests
// ============================================================================

test "coordinator state machine - initial state" {
    var loop = try xev.Dynamic.Loop.init(.{});
    defer loop.deinit();

    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }

    var coord = MorselCoordinator.init(
        std.testing.allocator,
        &loop,
        &pool,
        .{
            .bucket = "test-bucket",
            .key = "test/key.parquet",
            .region = "us-east-1",
        },
    );
    defer coord.deinit();

    try std.testing.expectEqual(CoordinatorState.init, coord.state);
}
