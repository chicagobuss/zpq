# Parquet + S3 Integration Survey (Rust / C++ / Java / Python)

This doc catalogs how major Parquet stacks integrate with **Amazon S3 (and S3-compatible object stores)**, with an eye toward informing ZPQ’s planned `RandomAccessSource` + `S3Source` design.

## What Parquet readers need from “storage”

At a high level, Parquet read patterns over remote object storage look like:

- **Tail reads**: read the last 8 bytes (`PAR1` + footer length), then read the footer (Thrift metadata).
- **Random reads**: fetch *column chunks* at known offsets (potentially many small reads).
- **Parallelism opportunity**: multiple columns / row groups can be fetched concurrently.

Because S3 is not a true “seekable file”, every “seek+read” becomes one or more **HTTP GET Range** requests underneath.

## Rust ecosystem (Arrow/DataFusion + `object_store`)

**Pattern**: Parquet is decoupled from S3 by pushing all storage concerns into an **object store abstraction**.

- **Abstraction**: an `ObjectStore`-style interface that can fetch byte ranges (and, in many stacks, *multiple ranges*).
- **I/O strategy**: range reads are commonly *batched/coalesced* (merge “nearby” ranges into fewer HTTP requests) to reduce round-trips. The precise implementation differs by layer, but the pattern is consistent.
- **Concurrency**: async-first; readers often issue multiple range GETs concurrently (columns/row groups).
- **Auth/config**: typically delegated to the AWS Rust SDK credential chain, with builder-style configuration (region, endpoint, credentials, etc.).

Primary references:
- `object_store` S3 builder API: [`AmazonS3Builder` (docs.rs)](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html)

## C/C++ ecosystem (Apache Arrow C++ `S3FileSystem`)

**Pattern**: a **filesystem abstraction** returns a **random-access file handle**; Parquet reads against that handle.

- **Abstraction**: `FileSystem` → `OpenInputFile` → `RandomAccessFile` (or equivalent), supporting `ReadAt` / positioned reads.
- **Transport**: typically AWS SDK for C++ under the hood.
- **Buffering**: buffering/read-ahead is usually a *separate layer* (e.g., “buffered input stream” wrappers) even if the S3 FS provides some internal optimizations. Net: don’t assume “one `ReadAt` = one HTTP request”.
- **Footer optimization**: commonly implemented as “read a bounded tail chunk” when possible, to reduce an extra RTT for the footer.

Primary references:
- Arrow C++ header (API surface): [`cpp/src/arrow/filesystem/s3fs.h`](https://github.com/apache/arrow/blob/main/cpp/src/arrow/filesystem/s3fs.h)

## Java ecosystem (Parquet Java / parquet-mr + Hadoop S3A)

**Pattern**: Parquet uses the Hadoop `FileSystem` API; S3 integration is “just another FS” (`s3a://`) implemented by Hadoop’s S3A connector.

- **Abstraction**: `HadoopInputFile` wraps a Hadoop `FSDataInputStream` and exposes a seekable/parquet-friendly input.
- **Seek implementation**: S3A emulates `seek` by translating it into ranged reads and internal bookkeeping (“lazy seek” patterns are common: track position until a read actually happens).
- **Performance knobs**: Hadoop S3A exposes extensive tuning for retries, timeouts, and read-ahead / minimum range sizes.
- **Vectored I/O**: newer Hadoop versions have been adding “read vectored” / positioned-read APIs to let Parquet-like workloads request multiple ranges more efficiently (implementation and availability depend on the Hadoop version).

Primary references:
- Parquet Java’s Hadoop adapter: [`HadoopInputFile.java`](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/util/HadoopInputFile.java)
- Hadoop AWS connector docs (S3A): [Hadoop-AWS module docs](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)

## Python ecosystem (PyArrow vs fsspec/s3fs)

There are effectively **two** common approaches:

### PyArrow’s built-in S3 filesystem (Arrow C++ backed)

- **Abstraction**: `pyarrow.fs.S3FileSystem` (a Python wrapper around Arrow’s filesystem layer).
- **Behavior**: exposes a file-like interface and supports common S3 configuration parameters (region, credentials, endpoint overrides, timeouts, etc.).

Primary reference:
- PyArrow API reference: [`pyarrow.fs.S3FileSystem`](https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html)

### fsspec/s3fs (pure Python filesystem layer)

- **Abstraction**: file-like object implementing read/seek over S3 via Python networking + AWS auth tooling.
- **Caching**: fsspec provides configurable caching strategies (e.g. read-ahead / block-based caching), which are particularly helpful for Parquet’s “footer + random chunks” pattern.

## Cross-library takeaways (what to copy for ZPQ)

- **Model S3 as stateless random access**: prefer `readAt(offset, len)` (and ideally `readRanges([]Range)`), not “open/seek/read”.
- **Add a buffering/caching layer** on top of the raw range GET primitive:
  - tail prefetch for footer,
  - read-ahead for sequential scans,
  - “parts” caching for repeated column chunk access.
- **Range coalescing matters**: merging nearby reads into fewer HTTP requests is often the biggest win on high-latency storage.
- **Expose the right knobs**:
  - endpoint override (MinIO/R2/LocalStack),
  - region,
  - credential sources (env/metadata/assume-role),
  - timeouts + retries/backoff,
  - concurrency limits,
  - minimum range size / readahead window.

## Open questions for ZPQ (to decide before implementing `S3Source`)

- **Auth scope**: pre-signed URLs only vs full SigV4 (env/credentials file/IMDS/assume role).
- **HTTPS/TLS strategy**: Zig stdlib vs linking a transport (e.g. libcurl) for portability.
- **Range API shape**: do we want `readAt` only, or first-class `readRanges` (vectored) to enable batching/coalescing?
- **Caching policy**: default read-ahead window + coalescing threshold (bytes) tuned for Parquet footers and column chunks.
- **Concurrency model**: thread pool issuing blocking range GETs vs async/evented IO.


