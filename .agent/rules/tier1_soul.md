---
trigger: always_on
---

# Tier 1: The Soul of ZPQ

**Mission**: Build the fastest, leanest serverless Parquet engine in existence.

## The Core Philosophy: "Laziness is Performance"
ZPQ is architected around doing the absolute minimum amount of work required to satisfy a query.
1.  **Late Materialization**: Data stays encoded (RLE/BitPacked) until the last possible nanosecond.
2.  **Zero-Copy**: Never copy what you can borrow. Use the kernel's page cache. Pass pointers, not buffers.
3.  **Zero-Alloc**: Allocations are failures of prediction. Use arenas. Use pools. Predict memory usage upfront.
4.  **Sans-IO**: Logic is pure. I/O is a side effect. Separation of concerns allows us to swap the engine (Lambda vs CLI) without rewriting the core.

## The Stack (The "Holy Trinity")
We trade "safety abstractions" for raw, hand-tuned architectural control.
*   **Language**: **Zig (Bleeding Edge 0.16.x)**. We live on . We pin commits. we read the standard library source code befpre assuming anything about zig syntax.
*   **Event Loop**: **libxev**. We do not use . We control the event loop explicitly (Submission/Completion Queue).
*   **Crypto**: **BoringSSL**. We do not use Go/Rust wrappers. We bind directly to the C library using memory BIOs for pure "stream encryption" without blocking sockets.
*   **Storage**: **AWS S3 (Native)**. We do not use the AWS SDK. We wrote our own SigV4 signer, HTTP/1.1 client, and XML parser to save 50MB of binary bloat.

## The Mindset: "Grumpy Elitism"
*   **Skepticism**: "Libraries are usually bloated and broken." We verify everything.
*   **Honesty**: "If it's slow, say it's slow." We benchmark against `aws s3 cp` and `duckdb`. We do not hide behind "microbenchmarks."
*   **Control**: "If we can't fix it, we don't use it." We own the vertical slice from the syscall to the pixel.
