//! Single-file, aggregate-SQL browser ABI. All input bytes belong to the caller.
const std = @import("std");
const zpq = @import("zpq");
const gpa = std.heap.c_allocator;
pub const os = struct {
    pub const heap = struct {
        pub const page_allocator = gpa;
    };
};
var response: []u8 = &.{};

export fn zpq_alloc(len: usize) usize {
    const bytes = gpa.alloc(u8, len) catch return 0;
    return @intFromPtr(bytes.ptr);
}
export fn zpq_free(ptr: usize, len: usize) void {
    gpa.free(@as([*]u8, @ptrFromInt(ptr))[0..len]);
}
export fn zpq_pool_threads() usize {
    return if (@import("builtin").single_threaded) 0 else zpq.core.spawn.Pool.threadCount();
}
export fn zpq_response_ptr() usize {
    return @intFromPtr(response.ptr);
}
export fn zpq_response_len() usize {
    return response.len;
}

export fn zpq_run(file: [*]const u8, file_len: usize, request: [*]const u8, request_len: usize) c_int {
    gpa.free(response);
    response = &.{};
    response = run(file[0..file_len], request[0..request_len]) catch |err| {
        response = std.fmt.allocPrint(gpa, "{{\"error\":\"{s}\"}}", .{@errorName(err)}) catch return -1;
        return 1;
    };
    return 0;
}

pub fn run(file: []const u8, request: []const u8) ![]u8 {
    const Request = struct { sql: []const u8, max_memory: usize = 256 << 20, threads: usize = 1 };
    const req = try std.json.parseFromSlice(Request, gpa, request, .{});
    defer req.deinit();
    if (req.value.max_memory == 0 or req.value.max_memory > 512 << 20) return error.BadMaxMemory;
    if (req.value.threads == 0 or req.value.threads > 8) return error.BadThreadCount;
    if (!@import("builtin").cpu.has(.wasm, .atomics) and req.value.threads != 1) return error.ThreadsUnavailable;
    const query = try zpq.core.expr.sql_parser.parseSqlQuery(gpa, req.value.sql);
    defer query.deinit(gpa);
    if (!std.mem.eql(u8, query.table_name, "input")) return error.ExpectedInputTable;
    if (!query.is_aggregate) return error.AggregateSqlOnly;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var ledger: zpq.core.memory.SharedBudget = .{ .limit = req.value.max_memory };
    var result = try zpq.core.scan.runMultiAggregate(gpa, arena.allocator(), .{
        .inputs = &.{.{ .name = "input", .bytes = file }},
        .aggregate = query.aggregate_str orelse "",
        .filter = query.filter_str,
        .group_by = query.group_by_str,
        .select_cols = if (query.select_cols.len > 0) query.select_cols else null,
        .result_order = query.result_order,
        .max_memory = req.value.max_memory,
        .ledger = &ledger,
        .parallelism = req.value.threads,
    });
    defer result.deinit(gpa);
    const program: zpq.core.result_program.ResultProgram = .{
        .order = query.result_order,
        .visible = result.visible_cols,
    };
    try program.finish(gpa, &result.output, &result.state.group_col_types);
    const kinds = try zpq.engine.answerTextKinds(gpa, result.output, result.state, true, null);
    defer gpa.free(kinds);
    const types = try zpq.engine.answerTypes(gpa, result.output, result.state);
    defer gpa.free(types);
    var ndjson: std.Io.Writer.Allocating = .init(gpa);
    defer ndjson.deinit();
    try zpq.engine.writeAggregateText(&ndjson.writer, result.output, kinds, types, result.output.rowCount(), .jsonl);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    try out.writer.writeAll("{\"ok\":true,\"ndjson\":");
    try zpq.engine.writeJsonQuoted(&out.writer, ndjson.written());
    try out.writer.writeByte('}');
    return out.toOwnedSlice();
}
