//! Blocking waits for code that has no `Io` of its own (scan workers, budgets, partitions).
//!
//! `io` is good for the futex calls alone: `std.Io.Threaded`'s futex wait and wake never touch the instance (a Linux
//! futex, Darwin's ulock), so any thread may sleep on a word through it. A waiter that yields or polls instead takes
//! the CPU the thread it waits for needs, which costs most on an oversubscribed or virtualised host.

const std = @import("std");

pub const io = std.Io.Threaded.global_single_threaded.io();
