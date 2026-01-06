const std = @import("std");

/// Compile-time flag to enable/disable tracing.
pub const enabled = true;

/// Global trace writer instance
var global_tracer: ?*Tracer = null;

/// Initialize the global tracer writing to the specified path.
pub fn initGlobal(allocator: std.mem.Allocator, path: []const u8) !void {
    if (!enabled) return;
    const t = try allocator.create(Tracer);
    t.* = try Tracer.init(allocator, path);
    global_tracer = t;
}

pub fn deinitGlobal() void {
    if (global_tracer) |t| {
        t.deinit();
        global_tracer = null;
    }
}

pub fn zone(comptime name: []const u8) Zone {
    if (!enabled) return .{ .tracer = null };
    if (global_tracer) |t| {
        t.writeEvent("B", name, null);
        return .{ .tracer = t, .name = name };
    }
    return .{ .tracer = null };
}

pub const Zone = struct {
    tracer: ?*Tracer,
    name: []const u8 = "",

    pub fn end(self: Zone) void {
        if (self.tracer) |t| {
            t.writeEvent("E", self.name, null);
        }
    }

    pub fn addText(self: Zone, text: []const u8) void {
        if (self.tracer) |t| {
            t.writeEvent("I", "log", text);
        }
    }
};

pub const Metrics = struct {
    filter_decode_ns: u64 = 0,
    skip_ns: u64 = 0,
    materialize_ns: u64 = 0,
    loop_overhead_ns: u64 = 0,
    rows_scanned: u64 = 0,
    rows_selected: u64 = 0,
    rows_skipped: u64 = 0,
    batches_processed: u64 = 0,
    batches_skipped: u64 = 0,
    row_groups_scanned: u64 = 0,
    row_groups_skipped: u64 = 0,
    pages_decoded: u64 = 0,
    bytes_decompressed: u64 = 0,

    pub fn add(self: *Metrics, other: Metrics) void {
        self.filter_decode_ns += other.filter_decode_ns;
        self.skip_ns += other.skip_ns;
        self.materialize_ns += other.materialize_ns;
        self.loop_overhead_ns += other.loop_overhead_ns;
        self.rows_scanned += other.rows_scanned;
        self.rows_selected += other.rows_selected;
        self.rows_skipped += other.rows_skipped;
        self.batches_processed += other.batches_processed;
        self.batches_skipped += other.batches_skipped;
        self.row_groups_scanned += other.row_groups_scanned;
        self.row_groups_skipped += other.row_groups_skipped;
        self.pages_decoded += other.pages_decoded;
        self.bytes_decompressed += other.bytes_decompressed;
    }

    pub fn totalNs(self: Metrics) u64 {
        return self.filter_decode_ns + self.skip_ns + self.materialize_ns + self.loop_overhead_ns;
    }
};

pub const RunMetadata = struct {
    timestamp_ms: i64 = 0,
    git_commit: ?[]const u8 = null,
    file_path: []const u8 = "",
    file_size_bytes: u64 = 0,
    num_columns: u32 = 0,
    num_row_groups: u32 = 0,
    total_rows: u64 = 0,
    filter_column: ?[]const u8 = null,
    filter_value: ?[]const u8 = null,
};

pub const Tracer = struct {
    metrics: Metrics = .{},
    run: RunMetadata = .{},
    timer: ?std.time.Timer = null,

    file: std.fs.File,
    allocator: std.mem.Allocator,
    start_time: std.time.Instant,
    first_event: bool = true,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Tracer {
        const file = try std.fs.cwd().createFile(path, .{});
        try file.writeAll("[\n");

        return Tracer{
            .file = file,
            .allocator = allocator,
            .start_time = try std.time.Instant.now(),
        };
    }

    pub fn deinit(self: *Tracer) void {
        self.file.writeAll("\n]") catch {};
        self.file.close();
        self.allocator.destroy(self);
    }

    pub fn writeEvent(self: *Tracer, ph: []const u8, name: []const u8, args: ?[]const u8) void {
        const now = std.time.Instant.now() catch return;
        const elapsed_ns = now.since(self.start_time);
        const ts_us = elapsed_ns / 1000;

        // Use allocPrint and writeAll to avoid std.fs.File.writer() API issues
        const json = if (args) |arg_val|
            std.fmt.allocPrint(self.allocator,
                \\{{"name":"{s}","cat":"zpq","ph":"{s}","ts":{d},"pid":0,"tid":0,"args":{{"info":"{s}"}}}}
            , .{ name, ph, ts_us, arg_val }) catch return
        else
            std.fmt.allocPrint(self.allocator,
                \\{{"name":"{s}","cat":"zpq","ph":"{s}","ts":{d},"pid":0,"tid":0}}
            , .{ name, ph, ts_us }) catch return;

        defer self.allocator.free(json);

        if (!self.first_event) {
            self.file.writeAll(",\n") catch {};
        }
        self.first_event = false;
        self.file.writeAll(json) catch {};
    }

    pub fn startTimer(self: *Tracer) void {
        if (!enabled) return;
        self.timer = std.time.Timer.start() catch null;
    }

    pub fn stopTimer(self: *Tracer) u64 {
        if (!enabled) return 0;
        if (self.timer) |*t| {
            const elapsed = t.read();
            self.timer = null;
            return elapsed;
        }
        return 0;
    }
};
