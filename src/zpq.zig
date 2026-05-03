//! ZPQ — pure sans-IO core surface.
//!
//! Anything I/O-related lives behind the io.* namespace and must satisfy
//! io.strategy.assertIsIOStrategy. Anything in core.* is pure logic and
//! must remain free of allocations beyond what callers pass in.

pub const core = struct {
    pub const schema = @import("core/schema.zig");
    pub const thrift = @import("core/thrift.zig");
};

pub const io = struct {
    pub const strategy = @import("io/strategy.zig");
    pub const loop = @import("io/loop.zig");
};

test {
    _ = core.schema;
    _ = core.thrift;
    _ = io.strategy;
    _ = io.loop;
}
