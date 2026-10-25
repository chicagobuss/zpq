//! Process-local forcing for the referee and verification variants. Call only while no query is running.
const std = @import("std");
const zpq = @import("zpq");
const engine = zpq.engine;
const Context = engine.Context;
const profile = zpq.profile;

pub var cli_overrides: []const []const u8 = &.{};
pub var observe = false;

pub fn isForced(name: []const u8) bool {
    for (cli_overrides) |entry| {
        if (entry.len > name.len and entry[name.len] == '=' and std.mem.eql(u8, entry[0..name.len], name))
            return true;
    }
    return false;
}

pub const Proof = struct {
    events: [@typeInfo(profile.Kind).@"enum".field_names.len]u64 = @splat(0),
    event_names: []const [:0]const u8 = @typeInfo(profile.Kind).@"enum".field_names,
    dropped: u64 = 0,
    explain: []const u8 = "",
};
pub var last_proof: Proof = .{};

pub fn deinit(gpa: std.mem.Allocator) void {
    if (last_proof.explain.len > 0) gpa.free(last_proof.explain);
    last_proof = .{};
}

fn collect(ctx: Context, p: *profile.Profiler, args: engine.QueryArgs) !void {
    p.finish();
    deinit(ctx.gpa);
    last_proof.dropped = p.dropped();
    const threads = try p.threads(ctx.gpa);
    defer ctx.gpa.free(threads);
    for (threads) |t| {
        var blocks = t.eventBlocks();
        while (blocks.next()) |events| for (events) |event| {
            last_proof.events[@backingInt(event.kind)] += 1;
        };
    }
    if (args.aggregate != null or args.group_by != null) {
        var arena = std.heap.ArenaAllocator.init(ctx.gpa);
        defer arena.deinit();
        const explanation = try engine.explainAggregate(ctx, arena.allocator(), args);
        var out: std.Io.Writer.Allocating = .init(ctx.gpa);
        defer out.deinit();
        try engine.writeAggregateExplanationJson(&out.writer, explanation);
        last_proof.explain = try ctx.gpa.dupe(u8, out.written());
    }
}

pub fn runQuery(ctx: Context, args: engine.QueryArgs) !engine.QueryResult {
    if (!observe) return engine.runQuery(ctx, args);
    const p = try profile.Profiler.create(ctx.gpa, .{ .sample_interval_ns = 0, .max_events = 65536 }, .{});
    defer p.destroy();
    var observed = args;
    observed.profiler = p;
    var result = try engine.runQuery(ctx, observed);
    errdefer result.deinit(ctx.gpa);
    try collect(ctx, p, args);
    return result;
}

pub fn runPrintTo(
    ctx: Context,
    args: engine.QueryArgs,
    format: engine.PrintFormat,
    limit: ?usize,
    fd: zpq.local_fs.fd_t,
    offset: usize,
) !engine.PrintResult {
    if (!observe) return engine.runPrintTo(ctx, args, format, limit, fd, offset);
    const p = try profile.Profiler.create(ctx.gpa, .{ .sample_interval_ns = 0, .max_events = 65536 }, .{});
    defer p.destroy();
    var observed = args;
    observed.profiler = p;
    const result = try engine.runPrintTo(ctx, observed, format, limit, fd, offset);
    try collect(ctx, p, args);
    return result;
}

extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern "c" fn unsetenv(name: [*:0]const u8) c_int;

pub fn refresh(ctx: *Context) void {
    const environ = @extern(*?[*:null]const ?[*:0]const u8, .{ .name = "environ" });
    ctx.env = if (environ.*) |live|
        .{ .block = .{ .slice = std.mem.span(live) } }
    else
        .empty;
}

pub const Binding = struct {
    gpa: std.mem.Allocator,
    ctx: *Context,
    name: [*:0]const u8,
    previous: ?[]u8 = null,
    changed: bool = false,

    pub fn deinit(self: *Binding) void {
        if (!self.changed) return;
        set(self.gpa, self.ctx, self.name, self.previous) catch |err|
            std.debug.panic("cannot restore forced environment {s}: {s}", .{ self.name, @errorName(err) });
        if (self.previous) |value| self.gpa.free(value);
        self.changed = false;
    }
};

fn set(gpa: std.mem.Allocator, ctx: *Context, name: [*:0]const u8, value: ?[]const u8) !void {
    if (value) |v| {
        const z = try gpa.allocSentinel(u8, v.len, 0);
        defer gpa.free(z);
        @memcpy(z[0..v.len], v);
        if (setenv(name, z, 1) != 0) return error.SetEnv;
    } else if (unsetenv(name) != 0) return error.SetEnv;
    // libc may replace the array on either set or restore. Never reuse the process-start snapshot.
    refresh(ctx);
    const visible = ctx.env.getPosix(std.mem.span(name));
    if (value) |want| {
        if (visible == null or !std.mem.eql(u8, want, visible.?)) return error.ForcedEnvironmentInvisible;
    } else if (visible != null) return error.ForcedEnvironmentInvisible;
}

pub fn bind(gpa: std.mem.Allocator, ctx: *Context, name: [*:0]const u8, value: ?[]const u8) !Binding {
    refresh(ctx);
    var binding: Binding = .{ .gpa = gpa, .ctx = ctx, .name = name };
    if (value == null) return binding;
    binding.previous = if (ctx.env.getPosix(std.mem.span(name))) |v| try gpa.dupe(u8, v) else null;
    errdefer if (binding.previous) |v| gpa.free(v);
    try set(gpa, ctx, name, value);
    binding.changed = true;
    return binding;
}
