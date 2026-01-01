const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

const Pipeline = zpq.core.pipeline.Pipeline;
const ExecutionMode = zpq.core.pipeline.ExecutionMode;
const SchemaFieldInfo = Pipeline.SchemaFieldInfo;
const FileMetaInfo = Pipeline.FileMetaInfo;

/// Common query parameters used by all execution modes (Lambda, HTTP, CLI).
pub const QueryParams = struct {
    input: []const u8, // s3://bucket/path.parquet or local path
    output: ?[]const u8 = null, // s3://bucket/output.parquet (optional)
    filter: ?[]const u8 = null, // "status=active"
    select: ?[]const u8 = null, // "id,name,created_at"
    mode: ExecutionMode = .slot_parallel,

    // Operations (mutually exclusive with filter/output)
    show_schema: bool = false,
    show_meta: bool = false,
};

/// Result of query execution.
pub const QueryResult = struct {
    input_rows: u64 = 0,
    output_rows: u64 = 0,
    elapsed_ms: f64 = 0,
    schema: ?[]const SchemaFieldInfo = null, // If show_schema
    meta: ?FileMetaInfo = null, // If show_meta
    error_message: ?[]const u8 = null, // If error occurred
};

/// Execute a query with the given parameters.
/// This is the universal entry point used by Lambda, HTTP server, and CLI modes.
pub fn executeQuery(
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    params: QueryParams,
) !QueryResult {
    var pipeline = Pipeline.init(allocator);
    defer pipeline.deinit();

    pipeline.setInput(params.input);
    pipeline.setRuntime(loop, thread_pool);

    if (params.output) |out| pipeline.setOutput(out);
    if (params.filter) |f| try pipeline.setFilter(f);
    if (params.select) |s| try pipeline.setProjection(s);

    // Schema-only mode
    if (params.show_schema) {
        const schema = try pipeline.getSchema(allocator);
        return .{ .schema = schema };
    }

    // Meta-only mode
    if (params.show_meta) {
        const meta = try pipeline.getMeta();
        return .{ .meta = meta };
    }

    // Filter/transform mode requires output
    if (params.filter != null and params.output == null) {
        return .{ .error_message = "--filter requires an output file" };
    }

    // Execute pipeline
    const result = try pipeline.execute(params.mode);
    return .{
        .input_rows = result.input_rows,
        .output_rows = result.output_rows,
        .elapsed_ms = result.elapsed_ms,
    };
}

/// Format query result as JSON for Lambda/HTTP responses.
pub fn formatResultJson(allocator: std.mem.Allocator, result: QueryResult) ![]u8 {
    if (result.error_message) |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"{s}\"}}", .{err});
    }

    if (result.schema) |schema| {
        var json = std.ArrayListUnmanaged(u8){};
        defer json.deinit(allocator);
        try json.appendSlice(allocator, "{\"schema\":[");
        for (schema, 0..) |field, i| {
            if (i > 0) try json.appendSlice(allocator, ",");
            try json.print(allocator, "{{\"index\":{d},\"name\":\"{s}\",\"type\":\"{s}\"}}", .{
                field.index,
                field.name,
                field.type_name,
            });
        }
        try json.appendSlice(allocator, "]}");
        return try json.toOwnedSlice(allocator);
    }

    if (result.meta) |meta| {
        const created_by = meta.created_by orelse "unknown";
        return std.fmt.allocPrint(allocator,
            \\{{"path":"{s}","rows":{d},"row_groups":{d},"columns":{d},"created_by":"{s}"}}
        , .{
            meta.path,
            meta.row_count,
            meta.row_group_count,
            meta.column_count,
            created_by,
        });
    }

    return std.fmt.allocPrint(allocator,
        \\{{"input_rows":{d},"output_rows":{d},"elapsed_ms":{d:.1}}}
    , .{
        result.input_rows,
        result.output_rows,
        result.elapsed_ms,
    });
}

/// Parse JSON event into QueryParams (used by Lambda and HTTP server).
pub fn parseQueryJson(allocator: std.mem.Allocator, json: []const u8) !QueryParams {
    const EventSchema = struct {
        file: ?[]const u8 = null,
        input: ?[]const u8 = null, // Alternative to 'file'
        output: ?[]const u8 = null,
        filter: ?[]const u8 = null,
        select: ?[]const u8 = null,
        schema: ?bool = null,
        meta: ?bool = null,
    };

    const parsed = try std.json.parseFromSlice(EventSchema, allocator, json, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const input = parsed.value.file orelse parsed.value.input orelse
        return error.MissingInputField;

    return .{
        .input = try allocator.dupe(u8, input),
        .output = if (parsed.value.output) |o| try allocator.dupe(u8, o) else null,
        .filter = if (parsed.value.filter) |f| try allocator.dupe(u8, f) else null,
        .select = if (parsed.value.select) |s| try allocator.dupe(u8, s) else null,
        .show_schema = parsed.value.schema orelse false,
        .show_meta = parsed.value.meta orelse false,
    };
}
