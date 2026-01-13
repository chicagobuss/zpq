const std = @import("std");

/// RowGroupData holds the raw bytes for a row group's column chunks.
/// Owned by the DataManager, handed to workers for processing.
pub const RowGroupData = struct {
    rg_idx: usize,
    data: []const u8,
    base_offset: u64,
    allocator: std.mem.Allocator,
    
    pub fn init(allocator: std.mem.Allocator, rg_idx: usize, data: []const u8, base_offset: u64) RowGroupData {
        return .{
            .rg_idx = rg_idx,
            .data = data,
            .base_offset = base_offset,
            .allocator = allocator,
        };
    }
    
    pub fn deinit(self: *RowGroupData) void {
        self.allocator.free(self.data);
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
    var data = RowGroupData.init(allocator, 0);
    try data.addColumn(0, "test column data");
    try dm.markReady(0, data);
    
    // Check it's ready
    try std.testing.expect(dm.isReady(0));
    
    // Take ownership
    var taken = dm.takeRowGroup(0).?;
    defer taken.deinit();
    
    try std.testing.expect(!dm.isReady(0));
}
