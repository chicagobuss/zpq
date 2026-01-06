const std = @import("std");
const xev = @import("xev");
const schema_mod = @import("schema.zig");
const predicate_mod = @import("predicate.zig");
const Predicate = predicate_mod.Predicate;
const pipeline_types = @import("pipeline/types.zig");
pub const ExecutionMode = pipeline_types.ExecutionMode;
pub const ExecutionResult = pipeline_types.ExecutionResult;
const Trace = pipeline_types.Trace;

const slot_parallel = @import("pipeline/slot_parallel.zig");
const morsel_parallel = @import("pipeline/morsel_parallel.zig");
const surgical = @import("pipeline/surgical.zig");

const parquet = @import("file.zig");
const ParquetFile = parquet.ParquetFile;
const interface = @import("../io/interface.zig");
const factory = @import("../io/s3/factory.zig");

/// Pipeline represents a single ZPQ query execution.
/// It coordinates opening the input file, applying filters/projections,
/// and executing the parallel scan.
///
/// Example:
///   var pipeline = Pipeline.init(allocator);
///   defer pipeline.deinit();
///   try pipeline.setInput("input.parquet");
///   try pipeline.setFilter("category=A");
///   try pipeline.setProjection("id,name,value");
///   pipeline.setOutput("output.parquet");
///   pipeline.setRuntime(&loop, &thread_pool);
///   const result = try pipeline.execute(.slot_parallel);
///
pub const SchemaFieldInfo = struct {
    name: []const u8,
    type_name: []const u8,
    index: usize,
};

pub const FileMetaInfo = struct {
    path: []const u8,
    row_count: u64,
    row_group_count: usize,
    column_count: usize,
    created_by: ?[]const u8,
};

pub const Pipeline = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    // Input
    input_path: ?[]const u8 = null,
    input_file: ?*ParquetFile = null,

    // Operations
    predicates: []Predicate = &.{},
    projection: ?[]const []const u8 = null,

    // Output
    output_path: ?[]const u8 = null,
    output_compression: schema_mod.CompressionCodec = .SNAPPY,

    // Runtime state (required for execution)
    loop: ?*xev.Dynamic.Loop = null,
    thread_pool: ?*xev.ThreadPool = null,

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.input_file) |pf| {
            pf.deinit();
            self.allocator.destroy(pf);
        }
        self.allocator.free(self.predicates);
    }

    /// Set input path (local file or s3://)
    pub fn setInput(self: *Self, path: []const u8) void {
        self.input_path = path;
    }

    /// Set output path (local file or s3://)
    pub fn setOutput(self: *Self, path: []const u8) void {
        self.output_path = path;
    }

    /// Set output compression codec
    pub fn setCompression(self: *Self, codec: schema_mod.CompressionCodec) void {
        self.output_compression = codec;
    }

    /// Enable surgical mode (page-level pruning for selective queries)
    pub fn setSurgical(self: *Self, enabled: bool) void {
        _ = self;
        _ = enabled;
        // This is now handled via ExecutionMode passed to execute()
    }

    /// Set filter expression (e.g. "id=123", "price > 10.0")
    /// Supports: =, !=, <, <=, >, >=, IS NULL, IS NOT NULL, BETWEEN
    /// Multiple filters can be combined with AND (e.g. "category=A AND price > 10")
    pub fn setFilter(self: *Self, expr: []const u8) !void {
        self.allocator.free(self.predicates);
        self.predicates = try Predicate.parseMulti(self.allocator, expr);
    }

    /// Set column projection (e.g. "id,name,value")
    pub fn setProjection(self: *Self, proj_expr: []const u8) !void {
        var list = std.ArrayListUnmanaged([]const u8){};
        var it = std.mem.tokenizeAny(u8, proj_expr, ", ");
        while (it.next()) |col| {
            try list.append(self.allocator, col);
        }
        self.projection = try list.toOwnedSlice(self.allocator);
    }

    /// Set xev runtime (loop and thread pool)
    pub fn setRuntime(self: *Self, loop: *xev.Dynamic.Loop, pool: *xev.ThreadPool) void {
        self.loop = loop;
        self.thread_pool = pool;
    }

    /// Open the input file and read metadata
    pub fn open(self: *Self) !void {
        const path = self.input_path orelse return error.NoInputPath;

        const pf = try self.allocator.create(ParquetFile);
        errdefer self.allocator.destroy(pf);

        pf.* = try factory.openFile(self.allocator, path, false);
        self.input_file = pf;
    }

    /// Execute the pipeline with the given mode.
    /// Uses xev.Dynamic for runtime backend selection (io_uring -> epoll fallback).
    /// Note: slot_parallel auto-redirects to morsel_parallel for S3 output.
    pub fn execute(self: *Self, mode: ExecutionMode) !ExecutionResult {
        if (self.input_file == null) {
            try self.open();
        }

        const loop = self.loop orelse return error.NoRuntime;
        const pool = self.thread_pool orelse return error.NoRuntime;

        return self.executeWithLoop(xev.Dynamic, loop, pool, mode);
    }

    /// Execute with a generic xev backend (for Lambda/Epoll support)
    /// This is the unibin-compatible entry point that works with any xev backend.
    /// Note: slot_parallel auto-redirects to morsel_parallel for S3 output.
    pub fn executeWithLoop(self: *Self, comptime XevApi: type, loop: *XevApi.Loop, pool: *xev.ThreadPool, mode: ExecutionMode) !ExecutionResult {
        if (self.input_file == null) {
            try self.openWithLoop(loop, pool);
        }

        // Auto-select morsel_parallel for S3 output (slot_parallel needs random access)
        var actual_mode = mode;
        if (mode == .slot_parallel and self.output_path != null and std.mem.startsWith(u8, self.output_path.?, "s3://")) {
            actual_mode = .morsel_parallel;
        }

        return switch (actual_mode) {
            .slot_parallel => if (self.predicates.len == 1 and self.predicates[0].op == .between)
                try surgical.executeSlotParallelWithLoop(self, XevApi, loop, pool)
            else
                try slot_parallel.executeWithLoop(self, XevApi, loop, pool),
            .morsel_parallel => if (self.predicates.len == 1 and self.predicates[0].op == .between)
                try surgical.executeMorselParallelSurgicalWithLoop(self, XevApi, loop, pool)
            else
                try morsel_parallel.executeWithLoop(self, XevApi, loop, pool),
            .surgical => if (self.output_path != null and std.mem.startsWith(u8, self.output_path.?, "s3://"))
                try surgical.executeMorselParallelSurgicalWithLoop(self, XevApi, loop, pool)
            else
                try surgical.executeSlotParallelWithLoop(self, XevApi, loop, pool),
        };
    }

    pub fn openWithLoop(self: *Self, loop: anytype, pool: *xev.ThreadPool) !void {
        const path = self.input_path orelse return error.NoInputPath;

        const pf = try self.allocator.create(ParquetFile);
        errdefer self.allocator.destroy(pf);

        pf.* = try factory.openFileWithOptions(self.allocator, path, .{
            .loop = if (@TypeOf(loop) == *xev.Dynamic.Loop) loop else null,
            .thread_pool = pool,
        });

        self.input_file = pf;
    }

    pub fn printMeta(self: *Self) !void {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        std.debug.print("File: {s}\n", .{self.input_path.?});
        std.debug.print("Row count: {d}\n", .{meta.num_rows});
        std.debug.print("Row groups: {d}\n", .{meta.row_groups.items.len});
        std.debug.print("Schema:\n", .{});
        for (meta.schema.items[1..], 0..) |elem, i| {
            const type_str = @tagName(elem.type orelse .BOOLEAN);
            std.debug.print("  {d}: {s} ({s})\n", .{ i, elem.name, type_str });
        }
    }

    pub fn printSchema(self: *Self) !void {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        std.debug.print("Schema for {s}:\n", .{self.input_path.?});
        for (meta.schema.items[1..], 0..) |elem, i| {
            const type_str = @tagName(elem.type orelse .BOOLEAN);
            std.debug.print("  {d}: {s} ({s})\n", .{ i, elem.name, type_str });
        }
    }

    pub fn getSchema(self: *Self, allocator: std.mem.Allocator) ![]SchemaFieldInfo {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        var list = std.ArrayListUnmanaged(SchemaFieldInfo){};
        for (meta.schema.items[1..], 0..) |elem, i| {
            try list.append(allocator, .{
                .name = try allocator.dupe(u8, elem.name),
                .type_name = try allocator.dupe(u8, @tagName(elem.type orelse .BOOLEAN)),
                .index = i,
            });
        }
        return list.toOwnedSlice(allocator);
    }

    pub fn getFileMeta(self: *Self, allocator: std.mem.Allocator) !FileMetaInfo {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        return FileMetaInfo{
            .path = try allocator.dupe(u8, self.input_path.?),
            .row_count = @intCast(meta.num_rows),
            .row_group_count = meta.row_groups.items.len,
            .column_count = meta.schema.items.len - 1,
            .created_by = if (meta.created_by) |cb| try allocator.dupe(u8, cb) else null,
        };
    }

    pub fn printSummary(self: *Self) !void {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        std.debug.print("Query Summary:\n", .{});
        std.debug.print("  Input: {s}\n", .{self.input_path.?});
        std.debug.print("  Rows: {d}, Row Groups: {d}\n", .{ meta.num_rows, meta.row_groups.items.len });
        if (self.predicates.len > 0) {
            for (self.predicates) |pred| {
                const op_str = switch (pred.op) {
                    .eq => "=",
                    .neq => "!=",
                    .gt => ">",
                    .lt => "<",
                    .gte => ">=",
                    .lte => "<=",
                    else => "?",
                };
                std.debug.print("  Filter: {s} {s} {s}\n", .{ pred.column, op_str, pred.value });
            }
        }
        if (self.projection) |cols| {
            std.debug.print("  Select: ", .{});
            for (cols, 0..) |col, i| {
                if (i > 0) std.debug.print(", ", .{});
                std.debug.print("{s}", .{col});
            }
            std.debug.print("\\n", .{});
        }
    }
};
