const std = @import("std");
const schema = @import("schema.zig");

pub const SinkType = enum {
    LOCAL,
    S3,
};

pub const RowGroupFilter = struct {
    row_group_idx: usize,
    should_skip: bool,
};

pub const RowGroupAssignment = struct {
    row_group_idx: usize,
    worker_idx: u8,
};

const filter_mod = @import("filter.zig");
pub const CompiledFilter = filter_mod.Filter;

pub const ExecutionPlan = struct {
    allocator: std.mem.Allocator,
    
    // Fast paths
    is_zero_copy: bool,           // SELECT * with no filter -> stream raw bytes
    is_projection_only: bool,     // SELECT cols with no filter -> decode subset
    
    // Column info
    required_columns: []const usize,     // Column indices to read (all needed columns)
    filter_columns: []const usize,       // Columns needed for filter evaluation
    output_columns: []const usize,       // Columns in final output
    
    // Filter info
    filter: ?CompiledFilter,             // Optimized filter expression
    
    // Parallelism
    row_group_assignments: []const RowGroupAssignment,
    num_workers: u8,

    pub fn init(allocator: std.mem.Allocator) ExecutionPlan {
        return .{
            .allocator = allocator,
            .is_zero_copy = false,
            .is_projection_only = false,
            .required_columns = &[_]usize{},
            .filter_columns = &[_]usize{},
            .output_columns = &[_]usize{},
            .filter = null,
            .row_group_assignments = &[_]RowGroupAssignment{},
            .num_workers = 1,
        };
    }
    
    pub fn deinit(self: *ExecutionPlan) void {
        self.allocator.free(self.required_columns);
        self.allocator.free(self.filter_columns);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.row_group_assignments);
    }
};
