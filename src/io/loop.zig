//! The event-loop seam: re-exports `epoll.zig`, the one backend, so call sites name the seam rather than the
//! implementation. epoll because AWS seccomp blocks `io_uring_setup` on Lambda.
//!
//! Nothing in the query path runs on it: S3 requests use blocking sockets (`tls.zig`, `http.zig`), made concurrent by
//! `std.Io` workers in `engine.zig`, and the Lambda runtime client blocks too.

pub const Backend = enum { epoll };

pub const backend: Backend = .epoll;

pub const impl = @import("epoll.zig");

pub const Loop = impl.Loop;
pub const Completion = impl.Completion;
pub const Operation = impl.Operation;
pub const Result = impl.Result;
