# ZPQ Technical Context and Detail

**Last Updated**: Dec 29, 2025
**Current State**: Phase 1 (Read Dominance) in progress. Milestone 18 (SIMD Decoders) achieving **3.1 GVal/s**. Milestone 18.5 (Deranged Data) complete and verified.

---

## Phase 1: Read Dominance (Skip Everything)

### Milestone 18: SIMD Decoders (In Progress)
ZPQ's decode performance leverages Zig 0.16's native `@Vector` support for extreme throughput.

*   [x] **Vector-Vector Shifts**: Parallel bit-unpacking for widths 1-32.
*   [x] **Large-Integer Load**: Use `u128/u256` containers for single-read 8-value loads.
*   [x] **BatchReader Integration**: Unified `nextBatch` path for dictionary and plain encodings.
*   [ ] **Vectorized RLE runs**: Vectorize repetition runs using splat/memset.
*   [ ] **SIMD null bitmap expansion**: Branchless expansion of nullable batches.

### Milestone 18.5: The Deranged Data Verification Gauntlet [COMPLETE]
**Goal**: Stress-test correctness and SIMD alignment using non-standard data.

*   [x] **Sparsity**: INT32/INT64 with 99% nulls. Fixed RLE overflow bug.
*   [x] **Bloat**: BYTE_ARRAY with strings > 64KB. Fixed `BatchReader` page-lifecycle memory safety bug.
*   [x] **Alignment**: FIXED_LEN_BYTE_ARRAY with non-standard lengths (7 bytes). Correct.
*   [x] **Transitions**: Dictionary -> PLAIN encoding transitions mid-column. Correct.

### Milestone 19: Predicate Pushdown (Up Next)
**Goal**: Skip I/O and processing for non-matching rows.

---

## 🛠️ Key Technical Achievements (Recent)

### 1. The `active_pages` Lifecycle
The `BatchReader` now manages an `active_pages` list. A page is only freed when `nextBatch` is called and the current read position has moved entirely past that page. This allows for zero-copy string slices to remain valid throughout the user's processing loop.

### 2. Comptime SIMD Kernels
Bit-unpacking for any width (1-32) is handled by specialized kernels generated at compile time. This eliminates branch mispredictions in the hot loop and allows the CPU to saturate memory bandwidth.

### 3. Full Type Parity
Support for "Legacy" types like `INT96` (timestamps) and `FIXED_LEN_BYTE_ARRAY` (Decimals/UUIDs) has been integrated into the vectorized path, ensuring ZPQ can handle any modern Parquet file.

---

## Lessons Learned: Zig 0.16.x and Development Workflow

### 1. The Shadowing Hazard
Zig 0.16 is strict about variable shadowing. Refactoring loops into `nextBatch` often triggers shadowing errors when reusing `i` or `count` from outer scopes. Prefer descriptive names like `page_idx` or `out_pos`.

### 2. Memory Safety in Vectorized Reads
Zero-copy is a double-edged sword. When batch reading across page boundaries, the previous page *must* stay alive until the batch is consumed. The `active_pages` pattern is the standard for ZPQ to prevent UAF (Use-After-Free) on string columns.

---

## Known Issues / TODO

### Debug Output Leaking to Stdout
**Status**: Needs fix. Debug print statements in the page deallocation path are unconditionally enabled.

### Streaming Writer
**Status**: Planned for Phase 2. Currently, ZPQ is optimized for Read/Scan.
