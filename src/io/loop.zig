//! The event-loop seam.
//!
//! One backend is built: **epoll** — mandatory on Lambda (AWS seccomp
//! blocks `io_uring_setup`) and what the CLI runs on Linux today. The
//! data plane (`tls.zig`, `lambda/runtime.zig`) sits directly on
//! `epoll.zig`; this module re-exports it as the canonical alias so
//! call sites name the seam rather than the implementation.
//!
//! Additional backends can slot in here, selected per target and
//! `build_options.lambda`:
//!   io_uring — Linux CLI hot path. Must be excluded from the Lambda
//!              binary at compile time.
//!   kqueue   — macOS CLI backend.

pub const Backend = enum { epoll };

pub const backend: Backend = .epoll;

pub const impl = @import("epoll.zig");

pub const Loop = impl.Loop;
pub const Completion = impl.Completion;
pub const Operation = impl.Operation;
pub const Result = impl.Result;
