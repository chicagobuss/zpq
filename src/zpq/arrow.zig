//! Arrow integration for ZPQ
//!
//! This module provides zero-copy interoperability with the Arrow ecosystem
//! via the Arrow C Data Interface. No libarrow dependency required.

pub const c_abi = @import("arrow/c_abi.zig");

// Re-export commonly used types
pub const ArrowSchema = c_abi.ArrowSchema;
pub const ArrowArray = c_abi.ArrowArray;
pub const ArrowArrayStream = c_abi.ArrowArrayStream;
pub const Format = c_abi.Format;

// Flags
pub const ARROW_FLAG_DICTIONARY_ORDERED = c_abi.ARROW_FLAG_DICTIONARY_ORDERED;
pub const ARROW_FLAG_NULLABLE = c_abi.ARROW_FLAG_NULLABLE;
pub const ARROW_FLAG_MAP_KEYS_SORTED = c_abi.ARROW_FLAG_MAP_KEYS_SORTED;

test {
    _ = c_abi;
}
