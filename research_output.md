This is a great pivot. You are correct—looking at how the "giants" (Arrow C++, Rust object_store, Hadoop/Java, and PyArrow) solved this reveals the roadmap and saves you from reinventing the wheel.

Here is a systematic review of the major Parquet S3 implementations, followed by the specific answers to your design questions based on these findings.

### Part 1: Systematic Review of Implementations

#### 1. Rust (`parquet` crate + `object_store`)
*The modern "gold standard" for high-performance, async S3 access.*
* **Architecture:** Decoupled. The `parquet` crate doesn't know about S3; it just asks for byte ranges. The `object_store` crate implements the S3 logic.
* **I/O Strategy (The "Coalesce"):** This is the most critical logic to copy. If the Parquet reader asks for byte ranges `0-100` and `120-200`, `object_store` automatically merges them into one HTTP request for `0-200` to save a round-trip, discarding the extra 20 bytes locally.
* **Concurrency:** Heavily `async`/`await`. It fetches multiple column chunks in parallel using concurrent HTTP range requests.
* **Auth:** Uses `aws-sdk-rust` or `reqwest` middleware.
* **Takeaway for Zig:** You need a `RandomAccessSource` trait that accepts a **list of ranges** (vectored I/O), not just a single `seek+read`.

#### 2. C++ (Apache Arrow `S3FileSystem`)
*The engine behind PyArrow and R.*
* **Transport:** Wraps the AWS C++ SDK.
* **Buffering:** Implements a "read-ahead" cache. It anticipates that if you read chunk N, you will likely read N+1.
* **Footer Optimization:** Explicit logic to read the last 64KB (or similar configurable size) of the file first to get the footer + metadata in one shot.
* **Takeaway for Zig:** Don't implement `read()` as "make an HTTP request." Implement `read()` as "check buffer -> if missing, calculate optimal range request -> fetch -> fill buffer".

#### 3. Java (Hadoop `S3A` + `parquet-mr`)
*The legacy enterprise standard (Spark, Hive).*
* **The "Seek" Problem:** S3 has no "seek." Old implementations struggled here. Modern S3A implements "lazy seek" (don't open the connection until `read()` is actually called).
* **Vectored IO:** Recent versions added `readVectored()` API to the Hadoop filesystem specifically to allow the Parquet reader to push down a list of ranges (e.g., "I need these 5 columns") so the S3 client can fetch them in parallel.
* **Takeaway for Zig:** Do not try to emulate a stateful file handle (open, seek, read, close). Treat S3 files as stateless random-access blobs.

#### 4. Python (`s3fs` / `fsspec`)
*Pure Python implementation (often used with Dask).*
* **Caching Layers:** `fsspec` has a configurable caching layer (`'readahead'`, `'parts'`, `'whole'`).
* **Optimization:** The `'parts'` cache strategy is designed for Parquet: it caches the footer and then caches specific keys (column chunks) as they are accessed.
* **Takeaway for Zig:** You likely want a default buffer strategy that favors the "footer + random chunks" pattern.

---

### Part 2: Answers to Your Design Questions
*Based on the review above and your "no deps" preference.*

#### **Access Model**
* **Decision:** **(a) plain `s3://`** (primary) AND **(c) pre-signed URLs**.
* **Why:** All major libraries support standard `s3://` bucket/key paths. Pre-signed URLs are essentially "free" if you implement the HTTP transport, as they are just a GET request to a long URL.

#### **Auth (The Hard Part)**
* **Decision:** Start with **Environment Variables (`AWS_ACCESS_KEY_ID`, etc.)** implementing **AWS SigV4** yourself.
* **Why:** Since you have a "no external deps" rule, you can't easily link the AWS SDK.
* **Feasibility:** SigV4 is tedious but deterministic (canonicalizing headers, hashing payloads). It is implementable in Zig standard lib (crypto/hashing).
* **Zig Note:** If you want to skip SigV4 for now, support **Public/Anonymous** buckets first. That lets you test the Parquet logic without the auth headache.

#### **Transport/TLS**
* **Decision:** **Link `libcurl`** (recommended) or use `std.http.Client` (if you are brave).
* **Why:**
    * **HTTPS is non-negotiable** for S3.
    * Zig's `std.http.Client` works, but currently, handling root CA certificates (for `https`) across cross-platform targets (macOS vs Linux vs Windows) can be painful without a library like `zig-network` or linking `libcurl`/`openssl`.
    * *Systematic Review Support:* Almost no production S3 library rolls its own SSL/TLS implementation. They all link to OpenSSL, transport via Curl, or use the OS networking stack.

#### **Performance: Round-trips vs. Memory**
* **Decision:** **Minimum Round-trips** (Coalesced Reads).
* **Why:** S3 latency (Time to First Byte) is high (50-100ms). Sending 10 requests for 10 small chunks is much slower than 1 request for 1 large chunk, even if you waste bandwidth.
* **Default:** Standard defaults are often **8MB - 16MB** buffers.

#### **Concurrency**
* **Decision:** **Parallel Range Fetches**.
* **Why:** S3 throughput scales with connections. A single stream cannot saturate a 10Gbps link. You need multiple connections reading different row groups/columns simultaneously.
* **Zig Implementation:** Since Zig doesn't have a built-in async runtime like Rust, you will need a thread pool (e.g., `std.Thread.Pool`) dispatching blocking HTTP requests, or a poll-based event loop if you use a non-blocking HTTP client.

#### **Region/Endpoint**
* **Decision:** Support **Custom Endpoints** (essential for MinIO, R2, LocalStack).
* **Why:** This is trivial to add (just a string replacement for the hostname) and allows you to test locally using MinIO without spending money on AWS.

### Proposed Roadmap (Zig-Style)

1.  **Phase 1 (The Foundation):**
    * Implement `S3Source` struct.
    * Dependency: Link `libcurl` (easiest path to reliable HTTPS + Range requests).
    * Auth: Anonymous/Public buckets only.
    * Logic: Implement "Read Range" (HTTP GET with `Range: bytes=x-y` header).

2.  **Phase 2 (The Logic):**
    * Implement a `BufferedRangeReader`.
    * Logic: If user asks for bytes 0-10, fetch 0-64KB (footer prefetch).
    * Logic: If user asks for range A and range B, and they are close, merge them.

3.  **Phase 3 (Production Ready):**
    * Implement AWS SigV4 signing (SHA256/HMAC).
    * Add Thread Pool for parallel fetches.