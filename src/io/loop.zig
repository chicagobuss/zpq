//! Comptime-selected event loop.
//!
//! Resolves to one of:
//!   epoll    — Linux baseline. Mandatory for Lambda (io_uring blocked
//!              by AWS seccomp). Phase A: implemented.
//!   io_uring — Linux CLI hot path. Phase B: not yet implemented.
//!   kqueue   — macOS CLI dev. Phase C: not yet implemented.
//!
//! See docs/event_loop_design.md for the contract every backend must satisfy.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");

pub const Backend = enum { epoll, iouring, kqueue };

pub const backend: Backend = b: {
    if (builtin.target.os.tag == .linux) {
        // Lambda binary forces epoll; CLI binary on Linux gets io_uring.
        break :b if (build_options.lambda) .epoll else .iouring;
    }
    if (builtin.target.os.tag.isDarwin()) break :b .kqueue;
    @compileError("unsupported target for ZPQ event loop: " ++ @tagName(builtin.target.os.tag));
};

pub const impl = switch (backend) {
    .epoll => @import("epoll.zig"),
    .iouring => @compileError("io_uring backend not implemented yet (Phase B). " ++
        "Build with -Dlambda for the epoll backend, or wait for src/io/iouring.zig."),
    .kqueue => @compileError("kqueue backend not implemented yet (Phase C). " ++
        "macOS support lands when CLI dev experience needs it."),
};

pub const Loop = impl.Loop;
pub const Completion = impl.Completion;
pub const Operation = impl.Operation;
pub const Result = impl.Result;
