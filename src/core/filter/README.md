# Filter Backends

This directory contains the implementations for filter evaluation. usage is dispatched from `../filter.zig`.

## Structure

- `mod.zig` / `../filter.zig`: Key types (`Filter`, `Operator`) and the main `evaluate` dispatcher.
- `operator.zig`: Shared `Operator` enum definition.
- `scalar.zig`: Reference scalar implementation (generic over primitive types).
- `avx2.zig` (Planned): AVX2 SIMD implementation.
- `neon.zig` (Planned): ARM NEON SIMD implementation.

## Design

The filter system uses a "backend" pattern where the main `evaluate` method chooses the best implementation available at compile time (or runtime).
Currently, it defaults to `scalar` for all platforms.
