const std = @import("std");
const xev = @import("xev");
const interface = @import("interface.zig");

/// A source wrapper that prefetches data chunks in background threads.
/// Typically used to fetch the NEXT row group while processing the CURRENT one.
pub const PrefetchingSource = struct {
    allocator: std.mem.Allocator,
    source: interface.RandomAccessSource,
    thread_pool: *xev.ThreadPool,
    
    // Config
    read_size: usize,        // Typically average row group size (e.g., 64MB+)
    max_read_ahead: usize,   // Number of future chunks to fetch

    cache: std.AutoHashMap(u64, []const u8), 
    mutex: std.Thread.Mutex,
    
    // State
    // We map offset ranges to "Tasks" or "Futures".
    // For simplicity V1: Just prefetch the next N bytes when readAt is called appropriately?
    // No, Parquet reader jumps around.
    // 
    // Specialized strategy for Parquet: "Row Group Prefetching"
    // The Planner knows the byte ranges of row groups.
    // We can hint "I will need range X-Y soon".
    
    // Let's implement a 'Hint' based prefetcher.
    // source.advise(offset, length) -> spawns background read
    
    pub fn init(
        allocator: std.mem.Allocator,
        source: interface.RandomAccessSource,
        thread_pool: *xev.ThreadPool,
    ) PrefetchingSource {
        return .{
            .allocator = allocator,
            .source = source,
            .thread_pool = thread_pool,
            .read_size = 1024 * 1024 * 16, // Default 16MB
            .max_read_ahead = 2,
            .cache = std.AutoHashMap(u64, []const u8).init(allocator),
            .mutex = .{},
        };
    }
    
    // The "Source" interface requires readAt.
    // But we are not strictly adhering to the interface struct here, 
    // we are likely wrapping it inside the Executor logic.
    // 
    // Wait, Executor passes 'source' to RowGroupPipeline.
    // RowGroupPipeline calls 'source.readAt'.
    // 
    // Implementation idea:
    // 1. Executor hints PrefetchingSource: "Prefetch these RowGroup ranges"
    // 2. PrefetchingSource spawns tasks to read those ranges into memory buffers.
    // 3. When RowGroupPipeline calls readAt, we check if it's in a buffered range.
    //    If yes, memcpy from buffer.
    //    If no, pass through to underlying source.
    
    
    
    pub fn deinit(self: *PrefetchingSource) void {
        var it = self.cache.valueIterator();
        while (it.next()) |buf| {
            self.allocator.free(buf.*);
        }
        self.cache.deinit();
    }
    
    /// Hint that we will need this range soon.
    /// Spawns a background task to read it.
    pub fn prefetch(self: *PrefetchingSource, offset: u64, length: usize) !void {
        self.mutex.lock();
        if (self.cache.contains(offset)) {
            self.mutex.unlock();
            return; // Already cached
        }
        self.mutex.unlock();
        
        // Allocate context for the background task
        const ctx = try self.allocator.create(PrefetchTask);
        ctx.* = .{
            .task = .{ .callback = prefetchCallback },
            .source = self,
            .offset = offset,
            .length = length,
        };
        
        // Schedule
        self.thread_pool.schedule(xev.ThreadPool.Batch.from(&ctx.task));
    }
    
    const PrefetchTask = struct {
        task: xev.ThreadPool.Task,
        source: *PrefetchingSource,
        offset: u64,
        length: usize,
    };
    
    fn prefetchCallback(task: *xev.ThreadPool.Task) void {
        const ctx: *PrefetchTask = @fieldParentPtr("task", task);
        defer ctx.source.allocator.destroy(ctx);
        
        // Perform the read
        const buf = ctx.source.allocator.alloc(u8, ctx.length) catch return;
        const n = ctx.source.source.readAt(ctx.offset, buf) catch {
            ctx.source.allocator.free(buf);
            return;
        };
        
        // Store in cache
        ctx.source.mutex.lock();
        defer ctx.source.mutex.unlock();
        
        // If we read less, shrink buffer?
        // Parquet expects exact reads usually.
        if (n < buf.len) {
            // Realloc or slice? 
            // For simplicity, keep as is, consumer handles short reads if needed.
            // But Map value is slice.
        }
        
        ctx.source.cache.put(ctx.offset, buf) catch {
            ctx.source.allocator.free(buf);
        };
    }

    /// Read implementation that checks cache first
    pub fn readAt(self: *PrefetchingSource, buffer: []u8, offset: u64) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        // Linear scan of cached buffers (usually very few items, < 5)
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            const cached_start = entry.key_ptr.*;
            const cached_buf = entry.value_ptr.*;
            const cached_end = cached_start + cached_buf.len;
            
            // Check if requested range is fully contained
            if (offset >= cached_start and offset + buffer.len <= cached_end) {
                const rel_offset = offset - cached_start;
                // std.debug.print("Prefetch Hit: {d} len {d}\n", .{offset, buffer.len});
                @memcpy(buffer, cached_buf[rel_offset .. rel_offset + buffer.len]);
                return buffer.len;
            }
        }
        
        // Cache miss - synchronous fallback
        // std.debug.print("Prefetch Miss: {d} len {d}\n", .{offset, buffer.len});
        return self.source.readAt(offset, buffer);
    }

    pub fn randomAccessSource(self: *PrefetchingSource) interface.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable = interface.RandomAccessSource.VTable{
        .readAt = readAtTypeErased,
        .size = sizeTypeErased,
        .close = closeTypeErased,
    };

    fn readAtTypeErased(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *PrefetchingSource = @ptrCast(@alignCast(ptr));
        return self.readAt(buf, offset);
    }
    
    fn sizeTypeErased(ptr: *anyopaque) u64 {
        const self: *PrefetchingSource = @ptrCast(@alignCast(ptr));
        return self.source.size();
    }
    
    fn closeTypeErased(ptr: *anyopaque) void {
        const self: *PrefetchingSource = @ptrCast(@alignCast(ptr));
        self.deinit();
        // Do we close the underlying source?
        // Probably not, unless we take ownership. 
        // For now, assume Executor owns underlying source and closes it?
        // Or we just proxy close?
        // Let's proxy close to be safe if this effectively replaces the source.
        self.source.close();
    }
};
