# Comparables: how other engines shape S3 reads

Empirical notes from reading other implementations. Each section ends with
the takeaway for ZPQ's Source-abstraction design (Option A vs B vs C).

## DuckDB httpfs

DuckDB's `httpfs` extension (`references/duckdb-httpfs/src/`) is a
~2300-LoC C++ extension that plugs into DuckDB core's `FileSystem`
abstraction. Parquet, CSV, and JSON readers are oblivious to the
backend; they go through `FileSystem::Read(handle, buf, n, offset)`
and `FileHandle::GetFileSize()` and that's it.

**1. Open-file shape.** Strategy (b): footer-eager, body-lazy.
`HTTPFileHandle::Initialize()` (httpfs.cpp:865) issues a `HEAD` to
populate `length`, `last_modified`, `etag`, then returns. No body
bytes are read at open time. The parquet reader subsequently does
small ranged GETs for the footer, then for column-chunk byte ranges
as needed. There is a fallback: if the server doesn't accept range
requests, httpfs degrades to a one-shot `GET` of the whole file
into a `CachedFileHandle` (httpfs.cpp:601-625, FullDownload at :722).

**2. Buffer / read-ahead.** A per-handle "read ahead" buffer
starts at 1 MiB and adaptively doubles up to 32 MiB on detected
sequential reads (httpfs.hpp:89-90, AdaptReadBufferSize at
httpfs.cpp:440). Buffering is bypassed entirely under
`DirectIO` or `RequireParallelAccess` (httpfs.hpp:104) — i.e.,
the parquet reader, which fans out concurrent column reads,
opts out of buffering and goes straight to ranged GETs.

**3. Caches.** Three layers, all session-scoped:
- `HTTPMetadataCache` — `path → {length, last_modified, etag}`
  (http_metadata_cache.hpp:18-22). Avoids repeat HEADs for the
  same URL within a query/session.
- `HTTPClientCache` — per-handle pool of `HTTPClient` (curl
  easy handles) for keep-alive reuse (httpfs.cpp:116-130).
- `ExternalFileCache` / `CachingFileSystem` — DuckDB-core layer
  (`src/include/duckdb/storage/caching_file_system.hpp`) that
  caches *byte ranges* in the buffer manager, keyed by path +
  version tag (etag). The parquet reader reads through this
  (`parquet_reader.hpp:65,151,247`), so a second query touching
  the same column chunk gets a cache hit without a network round
  trip.

**4. Parallelism.** DuckDB parallelism is task-graph driven by the
core scheduler, not file-driven. The httpfs layer just promises
thread-safe ranged reads when `RequireParallelAccess` is set: the
file handle has a `std::mutex` guarding `file_offset`/buffer state
(httpfs.hpp:85, ReadInternal at :502, Read at :601). Concurrency is
bounded by the client-cache size (one curl handle per concurrent
range) and a connection-pool semaphore in `HTTPParams`. There's no
io_uring; httpfs is curl-based (`httpfs_curl_client.cpp`) — sync
calls from worker threads.

**5. Abstraction shape.** `HTTPFileSystem : public FileSystem`
(httpfs.hpp:137); `S3FileSystem : public HTTPFileSystem`
(s3fs.hpp:202). The base interface DuckDB core uses uniformly:
```cpp
virtual unique_ptr<FileHandle> OpenFile(path, flags, opener);
virtual void Read(FileHandle&, void* buf, int64_t n, idx_t loc);
virtual int64_t GetFileSize(FileHandle&);
virtual vector<OpenFileInfo> Glob(path, opener);
virtual bool CanHandleFile(path);  // prefix dispatch: "s3://", "https://"
```
(file_system.hpp:145,152,166,252,271). New backends register via
`RegisterSubSystem` and dispatch is by `CanHandleFile`. This is
**Option C in the question**: a vtable that owns glob + open +
ranged-read, with the file-handle being the per-stream state.

**6. Glob.** `S3FileSystem::Glob` (s3fs.cpp:1049) finds the longest
non-wildcard prefix, calls `ListObjectsV2` with paging + common-
prefix recursion, then post-filters keys against the glob. Listing
lives at the `FileSystem` interface, not above it.

**7. Auth.** SigV4 in-tree, ~150 lines. `CreateS3Header`
(s3fs.cpp:31-127) builds canonical request, calls in-tree
`sha256`/`hmac256` (`crypto.cpp`); no AWS SDK. Credentials come from
DuckDB's `SecretManager` or `AWSEnvironmentCredentialsProvider`
(s3fs.hpp:67-85) reading `AWS_*` env vars.

**Design regrets / leftover smells.** `HTTPFileSystem::Glob`
returns `{path}` with `// FIXME` (httpfs.hpp:142) — generic HTTP
has no listing, only S3 does, suggesting the abstraction is one
layer too tall. Read-buffer growth is heuristic-only (httpfs.cpp:456
`TODO: ... least squares ... do something smarter`). Multipart
upload can't flush partial buffers (s3fs.cpp:475 `TODO: keeping the
last partially written buffer in memory`). The `// TODO: make base
function virtual?` (httpfs.hpp:129) hints the OO hierarchy ossified
before all hooks were generalized.

**Takeaway for ZPQ.** httpfs validates **Option C**: a Storage
vtable that owns `glob + open + ranged-read` with per-handle state
is the shape that scales from "local mmap" to "S3 with metadata
+ byte-range caches" without the parquet reader knowing.
Crucially, the *parquet reader never sees S3*. It sees
`CachingFileHandle::Read(ptr, n, offset)` and the cache + remote
fetch happen below. ZPQ's analogue: a `Source` vtable with
`size() + readRange(offset, len, out)`, optionally `glob()`, and
the engine `core/` reads through it. Option A (union of bytes-vs-
fetcher) would force the engine to switch on a tag in the hot
path; Option B (a flat RangedReader trait) loses the listing/open
cohesion that `Glob` proves you need at the same layer.

## Apache Arrow C++ RandomAccessFile

Arrow ships **two orthogonal layers** that compose: an I/O-primitive
layer (`io::RandomAccessFile`, `io::InputStream`, `io::OutputStream`)
and a filesystem layer (`fs::FileSystem`). parquet-cpp consumes only
the primitive layer; the filesystem layer exists to *produce*
primitives. This is materially different from DuckDB, which collapses
both into one `FileSystem` interface.

**1. RandomAccessFile shape** (cpp/src/arrow/io/interfaces.h):

```
// Readable (base)
Result<int64_t>          Read(int64_t nbytes, void* out);
Result<shared_ptr<Buffer>> Read(int64_t nbytes);
const IOContext&         io_context() const;

// InputStream : Readable
Status                   Advance(int64_t nbytes);
Result<string_view>      Peek(int64_t nbytes);   // zero-copy view
bool                     supports_zero_copy() const;

// RandomAccessFile : InputStream + Seekable
Result<int64_t>          GetSize();
Result<int64_t>          ReadAt(pos, n, allow_short_read, out);
Result<shared_ptr<Buffer>> ReadAt(pos, n);
Future<shared_ptr<Buffer>> ReadAsync(IOContext, pos, n, allow_short);
vector<Future<...>>      ReadManyAsync(IOContext, vector<ReadRange>);
Status                   WillNeed(vector<ReadRange>);   // prefetch
```

`ReadAt` is the fundamental random-access op; positional `Read` is
orthogonal. `ReadManyAsync` and `WillNeed` are *first-class* in the
base interface — vector range-reads aren't bolted on. `IOContext` is
passed per-call (not owned by the file) so callers swap executors.

**2. RandomAccessFile vs FileSystem.** Composed, not orthogonal.
`FileSystem` is a factory:

```
Result<shared_ptr<RandomAccessFile>> OpenInputFile(string path);
Result<shared_ptr<RandomAccessFile>> OpenInputFile(FileInfo);  // skips HEAD
Result<shared_ptr<InputStream>>      OpenInputStream(...);
Result<shared_ptr<OutputStream>>     OpenOutputStream(...);
Result<FileInfoVector>               GetFileInfo(FileSelector);
AsyncGenerator<FileInfoVector>       GetFileInfoGenerator(FileSelector);
```

`FileInfo = {path, type, size, mtime}`. `FileSelector =
{base_dir, recursive, allow_not_found, max_recursion}` is the glob
primitive. The `OpenInputFile(FileInfo)` overload skips a HEAD
round-trip when size is already known — same optimization ZPQ does
in lambda metadata fetch.

**3. S3 path.** `S3FileSystem` implements `FileSystem` and returns
`shared_ptr<io::RandomAccessFile>` from `OpenInputFile`. **No
parquet-specific S3 reader exists** — the `parquet::arrow::FileReader`
takes a generic `RandomAccessFile`, full stop.

**4. Pre-fetching / coalescing.** Lives in `arrow/io/caching.h` as
`ReadRangeCache` + `CacheOptions`:

```
CacheOptions { hole_size_limit, range_size_limit, lazy, prefetch_limit }
ReadRangeCache.Cache(vector<ReadRange>)   // submits + coalesces
                .WaitFor(vector<ReadRange>)// triggers I/O if lazy
                .Read(ReadRange) -> Buffer
```

Parquet-cpp opts in via `ArrowReaderProperties::set_prebuffer(true)`.
Reader computes byte ranges for requested columns × row-groups, calls
`Cache()` to coalesce + parallel-fetch, then `Read()`s individual
chunks. Recommended-for-S3, opt-in-for-local. Notably,
[ARROW-14025] reports PreBuffer was silently disabled in some
exec-node paths, and [GH-36765] proposes making it default for S3
datasets — arrow itself is still tuning the default after years.

**5. Multi-file datasets.** Three tiers:

- filesystem (`GetFileInfo(FileSelector)` for list+open)
- dataset (`ParquetDatasetFactory` takes `shared_ptr<FileSystem>`,
  produces `FileSystemDataset` of `ParquetFileFragment`s; reads
  `_metadata` cache files when present)
- parquet reader (per-file decode through `RandomAccessFile`)

Glob lives in FileSystem; multi-file orchestration lives above.

**6. DataFusion (Rust) cross-check.** DataFusion uses the
[`object_store`](https://docs.rs/object_store) crate's `ObjectStore`
trait — same shape as Arrow C++'s `FileSystem`, just async-Rust:
`get(path) -> GetResult`, `get_range(path, Range<u64>) -> Bytes`,
`list(prefix) -> Stream<ObjectMeta>`, `head(path) -> ObjectMeta`.
Parquet adapts via `ParquetObjectReader` which implements
`AsyncFileReader` (parquet's per-file ranged-read trait, the Rust
analogue of `RandomAccessFile`). Same two-layer pattern: storage
trait does list+open+ranged-read, parquet sees only the per-file
abstraction. The InfluxData-donated `object_store` is now used by
DataFusion, obstore, delta-rs — strong convergent evidence.

**Takeaway for ZPQ.** Arrow and DataFusion settled on a **layered
Option C**: a Storage-like trait owns `list + open`, and `open`
returns a ranged-read object that the parquet engine consumes.
DuckDB collapses this into one interface (per its section above);
arrow keeps them separate. Both work, but the arrow split has two
properties ZPQ wants: (a) an in-memory `Buffer` is a
`RandomAccessFile` for free — no `union { bytes, fetcher }` tag —
which kills Option A on its own merits, and (b) coalescing
(`ReadRangeCache`) is a separate object that *wraps* a
`RandomAccessFile`, so it composes with any backend without bloating
the trait. Option B (single trait, no list/open layer) is what
arrow factored *out* of parquet — it works for the engine but loses
the dataset-discovery surface multi-file Lambda fan-out needs.

Sources:

- [Arrow C++ I/O docs](https://arrow.apache.org/docs/cpp/io.html)
- [arrow/io/interfaces.h](https://github.com/apache/arrow/blob/main/cpp/src/arrow/io/interfaces.h)
- [arrow/filesystem/filesystem.h](https://github.com/apache/arrow/blob/main/cpp/src/arrow/filesystem/filesystem.h)
- [arrow/filesystem/s3fs.h](https://github.com/apache/arrow/blob/main/cpp/src/arrow/filesystem/s3fs.h)
- [arrow/io/caching.h](https://github.com/apache/arrow/blob/main/cpp/src/arrow/io/caching.h)
- [arrow/dataset/file_parquet.h](https://github.com/apache/arrow/blob/main/cpp/src/arrow/dataset/file_parquet.h)
- [Arrow C++ Parquet docs](https://arrow.apache.org/docs/cpp/parquet.html)
- [GH-36765: Enable Pre-Buffering by default for Parquet S3 datasets](https://github.com/apache/arrow/issues/36765)
- [ARROW-14025: PreBuffer not enabled in exec-node parquet scans](https://issues.apache.org/jira/browse/ARROW-14025)
- [DataFusion Object Store Integration (DeepWiki)](https://deepwiki.com/apache/datafusion/7.2-object-store-integration)
- [parquet::arrow::async_reader::AsyncFileReader](https://docs.rs/parquet/latest/parquet/arrow/async_reader/trait.AsyncFileReader.html)
- [ParquetObjectReader](https://arrow.apache.org/rust/parquet/arrow/async_reader/store/struct.ParquetObjectReader.html)
- [object_store crate donation](https://www.influxdata.com/blog/rust-object-store-donation/)

## Polars + arrow-rs object_store

Polars stacks **two** abstractions: arrow-rs `object_store` is the
per-backend storage trait (Option C); Polars adds its own `ByteSource`
enum on top (Option A) so the parquet decoder is oblivious to whether
bytes live on disk, in RAM, or on S3. The composed shape is C-under-A,
and several findings below differ from the Arrow C++ section because
Polars makes deliberate additions to vanilla object_store.

**1. `ObjectStore` trait shape (Option C)**
(`references/object-store/src/lib.rs:746`):
```
async fn put_opts(&self, &Path, PutPayload, PutOptions) -> PutResult
async fn put_multipart_opts(&self, &Path, opts) -> Box<dyn MultipartUpload>
async fn get_opts(&self, &Path, GetOptions) -> GetResult     // GetOptions carries Range
async fn get_ranges(&self, &Path, &[Range<u64>]) -> Vec<Bytes>  // default coalesces
fn list(&self, prefix: Option<&Path>) -> BoxStream<ObjectMeta>
async fn list_with_delimiter / copy_opts / rename_opts / delete_stream / head ...
```
Convenience methods (`get`, `get_range`, `head`, `put`) live on a
sibling `ObjectStoreExt` trait (`lib.rs:1220`) so the dyn-safe core
stays minimal. Default `get_ranges` calls `coalesce_ranges`
(`util.rs:105`) which merges gaps ≤ `OBJECT_STORE_COALESCE_DEFAULT =
1 MiB` (`util.rs:92`) and runs ≤ `OBJECT_STORE_COALESCE_PARALLEL = 10`
fetches in parallel (`util.rs:95`), then slices via
`bytes::Bytes::slice` (zero-copy). Glob (`list`) and ranged-read live
on the **same** trait.

**2. Polars wraps it twice.** `PolarsObjectStore`
(`polars/crates/polars-io/src/cloud/polars_object_store.rs:37`) holds
`Arc<dyn ObjectStore>` plus a credential-rebuild builder. It adds
behaviors stock object_store does not:
- `split_range` (line 422): split a single big range into chunks of
  `POLARS_DOWNLOAD_CHUNK_SIZE` (default 64 MiB; `pl_async.rs:21`) and
  fetch them concurrently. This is the large-fetch throughput edge.
- `merge_ranges` (line 464): chunk-aware coalescing with gap tolerance
  `clamp(max_len/8, 1 MiB, 8 MiB)` (line 504). Strictly more
  aggressive than object_store's flat 1 MiB.
- Global concurrency semaphore (`pl_async.rs:165`), default
  `max(rayon_threads, 10)`, `MAX_BUDGET_PER_REQUEST = 10`.
- Auto-rebuild of the inner store on credential-expiry errors
  (`try_exec_rebuild_on_err`, line 95).

Above that sits `ByteSource`
(`polars-io/src/utils/byte_source.rs:16`), a 3-method trait
(`get_size`, `get_range`, `get_ranges`) with a two-variant enum
`DynByteSource { MemSlice, Cloud }` (line 110). The parquet reader is
generic over `ByteSource` and never sees `ObjectStore`. mmap serves
ranges with zero allocation
(`polars-stream/.../row_group_data_fetch.rs:90` short-circuits the
range-coalesce path entirely for `MemSlice`).

**3. Parquet open: footer-only, 3 round trips.** `fetch_metadata`
(`parquet/read/async_impl.rs:97`):
1. `head` for size (`async_impl.rs:43`; falls back to `range 0..1` if
   HEAD is forbidden — presigned URLs —
   `polars_object_store.rs:391`).
2. `get_range(size-8 .. size)` — 8-byte trailer.
3. `get_range(size-8-footer_len .. size)` — full footer.

No tail prefetch; no whole-file fetch. Per row group
(`row_group_data_fetch.rs:121`), Polars builds **one byte range per
projected column-chunk** via `ColumnChunkMetadata::byte_range` and
calls `byte_source.get_ranges(&mut ranges)`. Result is
`HashMap<offset, MemSlice>` — decoder gets each column chunk as a
zero-copy slice. Even for full projection it prefers `get_ranges`
over a single `get_range` to skip concatenation
(`row_group_data_fetch.rs:135-139`, comment).

**4. Globs are in polars-io, not object_store.**
`cloud/glob.rs:18` (`extract_prefix_expansion`) splits
`s3://b/p/*.parquet` into a fixed prefix + regex; `Matcher` (line
179) tests each `ObjectMeta` from `store.list(Some(prefix))` (line
243). This contrasts with DuckDB, which puts `Glob` *on* the
filesystem trait. Polars' choice keeps object_store narrower.

**5. Coalescing / read-ahead.** Two layers, both passive:
- object_store: 1 MiB gap, ≤10 parallel.
- Polars: chunk-aware gap (1 MiB..8 MiB), splits big ranges to
  parallelise downloads, coalesces small ones to fewer GETs.

There is **no inter-RG prefetch at the byte-source layer**. Overlap
between "decode RG N while fetching RG N+1" comes from `polars-stream`
spawning each RG fetch as a separate `JoinHandle` on the io_runtime
(`row_group_data_fetch.rs:87`) — orchestration above the source, not
read-ahead inside it.

**6. Connection pool / TLS.** At the **object_store** layer (each
backend owns its `reqwest::Client` + hyper pool), with a global cache
one level up: `OBJECT_STORE_CACHE: LazyLock<RwLock<HashMap<key,
PolarsObjectStore>>>` (`object_store_setup.rs:20`). Cache key is URL
base + credential-provider identity (`path_and_creds_to_key`, line
37). Doc comment makes the design intent explicit: *"Every
object-store will do DNS lookups and get rate limited when querying
the DNS (can take up to 5s). Other reasons are connection pools that
must be shared between as much as possible"*
(`object_store_setup.rs:16`). One process, one pool per (endpoint,
creds).

**Takeaway for ZPQ.** Polars (and DataFusion, also object_store-
based) have run this at scale. The shape is **Option C as the
storage layer, Option A as the engine-facing layer**:
- Bottom: `Storage` vtable per backend (S3, R2, GCS, local) owning
  `list / open / ranged-read / put / multipart`. Registered by URL
  scheme. Same vtable shape DuckDB and Arrow C++ converged on.
- Top: per-file `Source` enum `{ bytes, fetcher }`. Mmap fast path
  skips the fetcher — important for ZPQ's CLI-on-local case where
  mmap beats any range-fetch abstraction. This is what Polars'
  `DynByteSource` is.

Coalescing should be a **separate composable layer**, not part of
the trait — Polars puts it in `PolarsObjectStore`, Arrow puts it in
`ReadRangeCache`. Both wrap the underlying store rather than
extending the trait. ZPQ should do the same: a thin coalescer that
takes a `Source` and exposes a `Source`, so it works on mmap and
HTTP alike but doesn't bloat the bottom trait.

## Probe results

### `probe_r2_latency` (workstation → R2, 2026-05-06)

Measured one-by-one (`docs/measurements/r2_latency.json`):

| Stage | ms |
|-------|---:|
| DNS (cold getaddrinfo) | 21 |
| TLS handshake | 15 |
| First Range GET on fresh conn | 128 |
| Range GET, warm conn (avg of 8) | 62 |
| 24× tail GET through `s3.Pool(8)` (sequential) | 96 / req, 2.3 s total |

Read: **steady-state ranged GET to R2 is ~62 ms warm** from this
workstation. The pool's "first call per slot" eats one full TLS
handshake (~15 ms) plus first-write penalty (~50 ms), bringing the
24-call sequential average to ~96 ms. With 8-way parallelism the
24 footers should land in ≈ 3 batches × 100 ms ≈ 300 ms wall —
not measured here, but bounded by the per-request RTT.

### `probe_r2_list` (workstation → R2, 2026-05-06)

`docs/measurements/r2_list_response.md`. ListObjectsV2 against R2
behaves identically to AWS S3:

- Single page of 24 keys: **115 ms cold, ~60 ms on warm conn**.
- Pagination via `<NextContinuationToken>` works as spec'd.
- Response XML is the same `<Contents><Key>...</Key><Size>...</Size>`
  shape — same parser would work for AWS and R2.

Glob expansion is therefore essentially free latency-wise: 60–115 ms
single round-trip for prefixes with ≤ 1 000 objects (the default
MaxKeys), one extra round-trip per additional thousand.

## Synthesis: which option does ZPQ build?

Three independent code-readings (DuckDB, Arrow C++, Polars) and the
two probes converge on the same shape:

**Storage trait at the bottom; per-file Source enum at the top;
coalescing as a separate wrapper.** "C-under-A," in our naming.

The argument:

* **Storage layer (Option-C-shaped, bottom).** A vtable per backend
  owning `list(prefix) + open(path) + ranged-read`. This is what
  DuckDB's `FileSystem`, Arrow's `fs::FileSystem`, and `object_store`
  all converge on. Concretely for ZPQ:
  ```
  pub const Storage = struct {
      ctx: *anyopaque,
      list:    *const fn(*anyopaque, Allocator, prefix: []const u8) ![]ObjectInfo,
      open:    *const fn(*anyopaque, Allocator, path: []const u8) !Source,
      // open is the only producer of Source; that keeps ranged-fetch
      // logic localized per backend.
  };
  ```
  Two implementations: `LocalStorage` (mmap-on-open), `S3Storage`
  (HEAD + footer-eager fetcher). Dispatch by URL scheme in CLI;
  Lambda hardwires S3.

* **Source layer (Option-A-shaped, top).** What the parquet engine
  actually consumes. A 3-method interface: `size`, `range(start, end)`,
  `ranges([start..end])`. Polars proves this works in production and
  that an in-memory mmap path that bypasses the HTTP coalescer is
  important enough to make a discriminator visible. **However**,
  Arrow's argument is correct: a `union(enum) { bytes, fetcher }`
  forces a tag check in the hot path. Use the trait shape — both
  variants implement the same interface, the in-memory variant just
  makes `range` a slice of mmap'd bytes:
  ```
  pub const Source = struct {
      ctx: *anyopaque,
      size: u64,
      range:  *const fn(*anyopaque, Allocator, start: u64, end: u64) ![]const u8,
      ranges: *const fn(*anyopaque, Allocator, []const Range) ![][]const u8, // default impl: sequential range()
      deinit: *const fn(*anyopaque) void,
  };
  ```
  This *is* Arrow's `RandomAccessFile` and Polars' `ByteSource`
  reduced to the methods we actually need. The previous Option B
  ("everything is a RangedReader") was right *for the engine layer*
  but not for the listing/open layer — the synthesis is "do both."

* **Coalescing as a wrapper.** Don't add it to the trait. Polars'
  `PolarsObjectStore` and Arrow's `ReadRangeCache` both wrap a
  `Source` and present a `Source`. We do the same. Defer until we
  actually have a workload that needs it (the demo's 24 footers fit
  in one round-trip per file with no coalescing benefit).

**Glob lives where?** Not on `Source` — on `Storage`. (DuckDB puts
it on `FileSystem`; Polars puts it in `polars-io` above `object_store`
and that decision still lives in the regrets section. We follow
DuckDB/Arrow.)

**Footer / metadata cache?** Not now. Defer until a real workload
asks. DuckDB has three caches; Polars has one; we have zero, and
the probes show no direct need.

### Probe-data implications for the design

The R2 round-trip data lands a specific decision: **eager-fetch the
whole file's needed bytes in `S3Storage.open`, return a `Source.bytes`-
backed value.** The math: a parquet aggregate against one taxi file
needs ~5 column-chunks of ~1 MB each = 5 ranged GETs. At 62 ms each
that's 310 ms serial. Eager pre-fetch with a single multi-range
request via `get_ranges` (Polars-style) would be one parallel batch
= ~62-100 ms. The lazy / on-demand-range-fetch path saves nothing
here — the engine *will* fetch all those ranges, and parallelizing
the fetch up-front is strictly faster than serializing it through
the engine's scan loop.

The lazy variant becomes useful only when the engine *might* not
need the bytes (speculative read on a column the filter prunes
later). That's not the demo's path. Eager-fetch keeps the design
simpler and matches Polars'  `row_group_data_fetch.rs:121` pattern
(one `get_ranges` per row group; bytes returned as a HashMap of
zero-copy slices).

### Migration order

1. Define `Source` (trait shape) and `Storage` (vtable) in
   `src/core/scan/source.zig`. Empty implementations.
2. Refactor `cli/query.zig:runAggregate` body to take `[]Source`
   instead of `[][]const u8`. Move it to `core/scan/aggregate.zig`.
3. `LocalStorage`: open = mmap → Source backed by the mmap slice.
   Drop-in for current CLI behavior. Verify regression: same numbers
   as currently.
4. `S3Storage.open`: tail GET → parse footer → parallel range-fetch
   surviving column chunks via `Io.Group` + existing `s3.Pool` →
   return Source backed by the fetched buffer. (This *is* the
   lambda's `doFetchMeta + range-fetch surviving chunks` flow,
   lifted out of `lambda/main.zig`.)
5. CLI: dispatch on URL scheme — `s3://...` builds `S3Storage`,
   plain path builds `LocalStorage`. Glob via `Storage.list` for s3,
   via `getdents64` for local.
6. Lambda: replace `handleS3Aggregate` with the same call. The
   single-input restriction disappears. Multi-file aggregate via
   one Lambda invocation works.

Estimated cost: ~600 LoC moved + ~300 new (S3Storage's open shape +
ListObjectsV2 client + Source/Storage definitions). One PR, half a
day. End state is the demo we actually want:
`zpq query 's3://bucket/yellow/*.parquet' --aggregate "count(*)"`
from a workstation, running through the same code path the lambda
uses.
