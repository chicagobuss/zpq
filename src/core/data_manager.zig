const std = @import("std");

/// RowGroupData holds the raw bytes for a row group's column chunks.
/// Owned by the DataManager, handed to workers for processing.
pub const RowGroupData = struct {
    rg_idx: usize,
    /// The chunks of data fetched for this row group.
    /// Can be a single chunk (contiguous) or multiple (sparse).
    chunks: []const Chunk,
    allocator: std.mem.Allocator,
    
    pub const Chunk = struct {
        data: []u8,
        base_offset: u64,
    };
    
    pub fn init(allocator: std.mem.Allocator, rg_idx: usize, chunks: []const Chunk) RowGroupData {
        return .{
            .rg_idx = rg_idx,
            .chunks = chunks,
            .allocator = allocator,
        };
    }
    
    pub fn deinit(self: *RowGroupData) void {
        for (self.chunks) |chunk| {
            self.allocator.free(chunk.data);
        }
        self.allocator.free(self.chunks);
    }

    /// Find the chunk containing the given offset range.
    /// Returns the slice within the chunk.
    pub fn getSlice(self: *const RowGroupData, offset: u64, length: u64) ![]const u8 {
        const end = offset + length;
        for (self.chunks) |chunk| {
            const chunk_end = chunk.base_offset + chunk.data.len;
            if (offset >= chunk.base_offset and end <= chunk_end) {
                const start_idx = offset - chunk.base_offset;
                return chunk.data[start_idx..][0..length];
            }
        }
        return error.OffsetOutofBounds;
    }
};

/// DataManager is a thread-safe buffer for prefetched row group data.
/// 
/// The Conductor fetches row groups asynchronously and calls `markReady()`.
/// Workers call `waitForRowGroup()` which blocks until data is available.
pub const DataManager = struct {
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
    condition: std.Thread.Condition,
    
    /// Ready row groups (data has arrived)
    ready: std.AutoHashMap(usize, RowGroupData),
    
    /// Pending row groups (fetch in progress)
    pending: std.AutoHashMap(usize, void),
    
    /// Failed row groups
    failed: std.AutoHashMap(usize, anyerror),
    
    /// Total row groups expected
    total_row_groups: usize,
    
    /// Shutdown flag
    shutdown: bool,
    
    const Self = @This();
    
    pub fn init(allocator: std.mem.Allocator, total_row_groups: usize) Self {
        var ready = std.AutoHashMap(usize, RowGroupData).init(allocator);
        ready.ensureTotalCapacity(@intCast(total_row_groups)) catch {};
        
        var pending = std.AutoHashMap(usize, void).init(allocator);
        pending.ensureTotalCapacity(@intCast(total_row_groups)) catch {};

        return .{
            .allocator = allocator,
            .mutex = .{},
            .condition = .{},
            .ready = ready,
            .pending = pending,
            .failed = std.AutoHashMap(usize, anyerror).init(allocator),
            .total_row_groups = total_row_groups,
            .shutdown = false,
        };
    }
    
    pub fn deinit(self: *Self) void {
        var it = self.ready.valueIterator();
        while (it.next()) |data| {
            var d = data.*;
            d.deinit();
        }
        self.ready.deinit();
        self.pending.deinit();
        self.failed.deinit();
    }
    
    /// Request a row group to be fetched.
    /// Called by the Conductor before issuing S3 request.
    pub fn requestRowGroup(self: *Self, rg_idx: usize) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        if (self.ready.contains(rg_idx) or self.pending.contains(rg_idx)) {
            return; // Already fetching or ready
        }
        
        try self.pending.put(rg_idx, {});
    }
    
    /// Mark a row group as ready with its data.
    /// Called by the Conductor when S3 read completes.
    pub fn markReady(self: *Self, rg_idx: usize, data: RowGroupData) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        _ = self.pending.remove(rg_idx);
        try self.ready.put(rg_idx, data);
        
        // Wake up any workers waiting for this data
        self.condition.broadcast();
    }
    
    /// Mark a row group as failed.
    /// Called by the Conductor when S3 read fails.
    pub fn markFailed(self: *Self, rg_idx: usize, err: anyerror) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        _ = self.pending.remove(rg_idx);
        try self.failed.put(rg_idx, err);
        
        // Wake up any workers waiting
        self.condition.broadcast();
    }
    
    /// Wait for a row group's data to become available.
    /// Called by workers. Blocks until data is ready or fails.
    /// Returns null if shutdown or failed.
    pub fn waitForRowGroup(self: *Self, rg_idx: usize) ?RowGroupData {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        while (true) {
            // Check if ready
            if (self.ready.get(rg_idx)) |data| {
                return data;
            }
            
            // Check if failed
            if (self.failed.contains(rg_idx)) {
                return null;
            }
            
            // Check shutdown
            if (self.shutdown) {
                return null;
            }
            
            // Wait for condition
            self.condition.wait(&self.mutex);
        }
    }
    
    /// Check if a row group is ready without blocking.
    pub fn isReady(self: *Self, rg_idx: usize) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.ready.contains(rg_idx);
    }
    
    /// Signal shutdown to wake all waiting workers.
    pub fn signalShutdown(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.shutdown = true;
        self.condition.broadcast();
    }
    
    /// Remove a row group from ready (worker takes ownership).
    pub fn takeRowGroup(self: *Self, rg_idx: usize) ?RowGroupData {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        if (self.ready.fetchRemove(rg_idx)) |kv| {
            return kv.value;
        }
        return null;
    }
};

test "DataManager basic flow" {
    const allocator = std.testing.allocator;
    
    var dm = DataManager.init(allocator, 4);
    defer dm.deinit();
    
    // Request RG0
    try dm.requestRowGroup(0);
    
    // Mark RG0 ready
    const buf = try allocator.dupe(u8, "test column data");
    const chunks = try allocator.alloc(RowGroupData.Chunk, 1);
    chunks[0] = .{ .data = buf, .base_offset = 0 };

    const data = RowGroupData.init(allocator, 0, chunks);
    try dm.markReady(0, data);
    
    // Check it's ready
    try std.testing.expect(dm.isReady(0));
    
    // Take ownership
    var taken = dm.takeRowGroup(0).?;
    defer taken.deinit(); 
    // Data is freed by deinit
    
    try std.testing.expect(!dm.isReady(0));
}
