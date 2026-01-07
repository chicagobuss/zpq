This style guide captures the architectural patterns and coding conventions of the `zpq` codebase, focusing on high-performance Zig 0.14-0.16.dev features.

### 1. The Laziness Principle
Architecture must prioritize delaying expensive operations (decoding, decompression, allocation) until the boundary of a transformation.
*   **Late Materialization**: Keep data in native Parquet encodings (RLE, Dictionary) as long as possible.
*   **Zero-Copy Slicing**: Use `RandomAccessSource.getSlice()` to borrow memory for uncompressed pages instead of allocating.
*   **Metadata-First**: Always check `mightMatchRowGroup` or `mightMatchPage` before initiating data I/O.
*   **Copy-Through**: Design pipelines to pass through unchanged compressed column chunks by copying raw bytes rather than re-encoding.

### 2. High-Performance Memory Patterns
*   **Unmanaged Collections**: Prefer `ArrayListUnmanaged` and `StringHashMapUnmanaged`. Core structs should not hold internal allocators; pass `Allocator` at the call site.
*   **Arena for Metadata**: Use `std.heap.ArenaAllocator` for parsing complex tree-like structures (like Thrift metadata) to avoid "death by a thousand small frees."
*   **Reusable Buffers**: Structs that perform repetitive I/O or decompression (e.g., `ColumnReader`) must hold a reusable `decompression_buffer` to eliminate per-page allocations.
*   **Stack-Allocated Batches**: Use fixed-size stack arrays (e.g., `[1024]T`) for intermediate SIMD processing or decoding batches.

### 3. SIMD & Vectorization
*   **Vectorized Paths**: Prefer `@Vector(N, T)` for bit-unpacking, null expansion, and predicate evaluation.
*   **Branchless Expansion**: Use shuffle tables or bit-masking to expand null bitmaps instead of conditional branches.
*   **Comptime Kernels**: Leverage `comptime` to generate type-specific decoders (e.g., `ByteStreamSplitDecoder(T)`) to allow the compiler to optimize stride logic.

### 4. Interface & I/O Design
*   **Loop-Agnostic Core**: Business logic must interact with `std.Io` interfaces or generic `RandomAccessSource` vtables, never directly with a specific async backend like `libxev`.
*   **Explicit Cleanup**: Implement a `cleanup_fn` and `cleanup_context: ?*anyopaque` pattern for types that manage polymorphic resources (e.g., `ParquetFile` owning either an `MmapSource` or an `S3Source`).
*   **Single-Shot Pre-fetching**: Consolidate multiple column reads into a single `readRanges` call to maximize I/O depth and minimize network round-trips.

### 5. Error Handling & Safety
*   **Malformed-File Resilience**: Every decoder must validate lengths and magic bytes. Use `just verify-malformed` to test graceful failures.
*   **Incompatible Type Enforcement**: Use `error.IncompatibleTypeForDictionary` or `error.UnsupportedEncoding` early in the decoding pipe to prevent invalid memory casts.

### 6. Tooling & Environment
*   **Lambda-Ready**: Maintain a small binary footprint (<2MB). Avoid heavy SDKs; implement protocols (SigV4, S3) natively.
*   **Build Integration**: Use `build.zig.zon` for dependency pinning. Use `just` for complex task orchestration (benchmarking, cross-compilation, fixture generation).
*   **Tracing**: Use the `zpq.trace` module for performance-critical zones. Use `std.posix.getenv("ZPQ_TRACE")` to enable verbose diagnostic logging without recompiling.