# ZPQ S3 Support: Implementation Plan

**Based on**: `research_output.md`, `STATUS.md`, `STATUS_CURRENT_DETAIL.md`, and codebase analysis  
**Date**: Dec 15, 2025  
**Updated**: Incorporated corrections for TLS, coalescing details, and allocator safety

---

## Executive Summary

ZPQ currently achieves **~842 MB/s** local throughput—substantially faster than PyArrow and Rust Arrow. Adding S3 support requires introducing an I/O abstraction layer, since the current code is tightly coupled to `std.fs.File`. The good news: **an AWS SDK for Zig already exists** ([aws-sdk-for-zig](https://git.lerch.org/lobo/aws-sdk-for-zig)), which handles SigV4 authentication and HTTP transport, dramatically reducing implementation complexity.

### Why Use the SDK

1. **De-risks the hardest part:** Implementing AWS SigV4 (canonicalization, signing regions, date headers) correctly is surprisingly difficult. Using a battle-tested SDK saves weeks of debugging "SignatureDoesNotMatch" errors.
2. **Future-proofing:** If you later need `AssumeRole` (for EC2/pods) or `S3:ListObjects` (for wildcard paths like `s3://bucket/data/*.parquet`), the SDK has it ready.

---

## Current Architecture (The Problem)

Both `ParquetFile` and `ColumnReader` are hardcoded to `std.fs.File`:

```zig
// file.zig
pub const ParquetFile = struct {
    file: std.fs.File,  // ← Direct coupling
    // ...
}

// column.zig
pub const ColumnReader = struct {
    file: std.fs.File,  // ← Same issue
    // ...
}
```

**Operations that need abstraction:**
1. `file.seekTo(offset)` → Random access positioning
2. `file.seekFromEnd(-N)` → Footer reading
3. `file.read(&buf)` → Sequential reads
4. `file.stat().size` → File size discovery

---

## Recommended Approach: Use `aws-sdk-for-zig`

The research output suggested rolling your own SigV4 + libcurl. **Skip this.** The existing AWS SDK for Zig provides:

- ✅ Full SigV4 signing (all the HMAC-SHA256 canonicalization)
- ✅ HTTPS via Zig's `std.http.Client` or libcurl
- ✅ Credential chain (env vars, config files, IAM roles)
- ✅ All AWS services including S3 `GetObject` with range support
- ✅ Cross-platform (macOS, Linux, Windows)
- ✅ Tested and maintained

**Repository**: https://git.lerch.org/lobo/aws-sdk-for-zig

### ⚠️ TLS Reality Check

The `aws-sdk-for-zig` relies on Zig's `std.http.Client`. This works great on **Linux** but can be flaky with Root CA certificates on **macOS/Windows** depending on the Zig version. 

**Mitigation options:**
1. Test early on your target platforms
2. If issues arise, the SDK may support linking libcurl as a fallback
3. For development/CI, MinIO over HTTP (port 9000) sidesteps TLS entirely

---

## Implementation Phases

### Phase 1: I/O Abstraction Layer (Local First)

**Goal**: Decouple file reading from `std.fs.File` without breaking existing functionality.

#### 1.1 Define `RandomAccessSource` Interface

Create `src/zpq/io.zig`:

```zig
const std = @import("std");

/// A thread-safe, random-access byte source.
/// Unlike std.io.Stream, this is stateless: strict "Read At Offset".
pub const RandomAccessSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    
    pub const VTable = struct {
        /// Reads exactly `buf.len` bytes from `offset`. 
        /// Returns error if EOF is hit before buf is filled.
        readAt: *const fn (ctx: *anyopaque, offset: u64, buf: []u8) anyerror!void,
        /// Returns total size of the blob in bytes.
        size: *const fn (ctx: *anyopaque) u64,
    };
    
    pub fn readAt(self: RandomAccessSource, offset: u64, buf: []u8) !void {
        return self.vtable.readAt(self.ptr, offset, buf);
    }
    
    pub fn size(self: RandomAccessSource) u64 {
        return self.vtable.size(self.ptr);
    }
};
```

**Why `readAt(offset, buf) -> !void` instead of `-> !usize`?**  
- S3 has no seek—it's stateless. This interface naturally maps to HTTP Range requests.
- Returning `void` means "read exactly `buf.len` or error"—simpler contract, no partial reads to handle.
- The VTable approach costs a tiny bit of CPU (dynamic dispatch) but allows swapping S3 vs Local at runtime.

#### 1.2 Implement `LocalFileSource`

```zig
pub const LocalFileSource = struct {
    file: std.fs.File,
    file_size: u64,
    
    pub fn init(path: []const u8) !LocalFileSource {
        const file = try std.fs.cwd().openFile(path, .{});
        const stat = try file.stat();
        return .{ .file = file, .file_size = stat.size };
    }
    
    fn readAtImpl(ctx: *anyopaque, offset: u64, buf: []u8) !void {
        const self: *LocalFileSource = @ptrCast(@alignCast(ctx));
        const bytes_read = try self.file.pread(buf, offset);  // pread = positioned read (no seek state)
        if (bytes_read != buf.len) return error.UnexpectedEOF;
    }
    
    fn sizeImpl(ctx: *anyopaque) u64 {
        const self: *LocalFileSource = @ptrCast(@alignCast(ctx));
        return self.file_size;
    }
    
    pub fn close(self: *LocalFileSource) void {
        self.file.close();
    }
    
    pub fn source(self: *LocalFileSource) RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .size = sizeImpl,
            },
        };
    }
};
```

#### 1.3 Refactor `ParquetFile` and `ColumnReader`

Replace `file: std.fs.File` with `source: RandomAccessSource`:

```zig
// file.zig
pub const ParquetFile = struct {
    source: RandomAccessSource,
    footer_len: u32,
    file_size: u64,
    // ...
    
    pub fn readFooter(self: *ParquetFile) !void {
        // Read last 8 bytes (footer_len + magic)
        var tail: [8]u8 = undefined;
        _ = try self.source.readAt(self.file_size - 8, &tail);
        // ... rest of footer parsing
    }
};
```

---

### Phase 2: S3 Source Implementation

**Goal**: Implement `S3Source` using `aws-sdk-for-zig`.

#### 2.1 Add Dependency

In `build.zig`:

```zig
const aws_dep = b.dependency("aws-sdk-for-zig", .{
    .target = target,
    .optimize = optimize,
});
zpq_mod.addImport("aws", aws_dep.module("aws"));
```

In `build.zig.zon`:
```zon
.dependencies = .{
    .@"aws-sdk-for-zig" = .{
        .url = "https://git.lerch.org/lobo/aws-sdk-for-zig/archive/vX.X.X.tar.gz",
        .hash = "...",
    },
},
```

#### 2.2 Implement `S3Source`

```zig
// src/zpq/s3.zig
const std = @import("std");
const aws = @import("aws");

pub const S3Source = struct {
    allocator: std.mem.Allocator,
    bucket: []const u8,
    key: []const u8,
    client: aws.Client,
    object_size: u64,
    
    // Cache for the footer region (last 64KB typically)
    footer_cache: ?[]u8 = null,
    footer_cache_offset: u64 = 0,
    
    pub fn init(allocator: std.mem.Allocator, uri: []const u8) !S3Source {
        // Parse s3://bucket/key
        const parsed = try parseS3Uri(uri);
        
        // Initialize AWS client
        var client = aws..Client.init(allocator, .{});
        
        // HEAD request to get object size
        const head = try client.headObject(.{
            .bucket = parsed.bucket,
            .key = parsed.key,
        });
        
        return S3Source{
            .allocator = allocator,
            .bucket = parsed.bucket,
            .key = parsed.key,
            .client = client,
            .object_size = @intCast(head.content_length),
        };
    }
    
    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) !usize {
        const self: *S3Source = @ptrCast(@alignCast(ptr));
        
        // Check footer cache first
        if (self.footer_cache) |cache| {
            if (offset >= self.footer_cache_offset) {
                const cache_offset = offset - self.footer_cache_offset;
                if (cache_offset + buf.len <= cache.len) {
                    @memcpy(buf, cache[cache_offset..][0..buf.len]);
                    return buf.len;
                }
            }
        }
        
        // HTTP Range request
        const range_header = try std.fmt.allocPrint(
            self.allocator, 
            "bytes={d}-{d}", 
            .{ offset, offset + buf.len - 1 }
        );
        defer self.allocator.free(range_header);
        
        const response = try self.client.getObject(.{
            .bucket = self.bucket,
            .key = self.key,
            .range = range_header,
        });
        
        const body = response.body orelse return error.EmptyResponse;
        @memcpy(buf[0..body.len], body);
        return body.len;
    }
    
    // ... size, close implementations
    
    /// Prefetch footer region (call before readFooter)
    pub fn prefetchFooter(self: *S3Source, size: usize) !void {
        const fetch_size = @min(size, self.object_size);
        self.footer_cache_offset = self.object_size - fetch_size;
        self.footer_cache = try self.allocator.alloc(u8, fetch_size);
        _ = try self.readAtImpl(@ptrCast(self), self.footer_cache_offset, self.footer_cache.?);
    }
};
```

---

### Phase 3: Buffered Range Reader (Performance)

**Goal**: Implement the "coalesce" pattern from Rust/Arrow for optimal S3 performance.

#### Key Insight from Research

> If the Parquet reader asks for byte ranges `0-100` and `120-200`, `object_store` automatically merges them into one HTTP request for `0-200`.

#### 3.1 `BufferedRangeReader` Wrapper

```zig
pub const BufferedRangeReader = struct {
    inner: RandomAccessSource,
    allocator: std.mem.Allocator,
    
    // Coalesce threshold: if two ranges are within this distance, merge them
    coalesce_threshold: usize = 64 * 1024,  // 64KB
    
    // Prefetch buffer (usually 8-16MB for S3)
    buffer: ?[]u8 = null,
    buffer_offset: u64 = 0,
    
    pub fn readRanges(self: *BufferedRangeReader, ranges: []const Range) ![][]u8 {
        // 1. Sort ranges by offset
        // 2. Merge adjacent/overlapping ranges within coalesce_threshold
        // 3. Issue minimal HTTP requests
        // 4. Return slices into the buffer
    }
};
```

---

### Phase 4: Parallel Column Fetching

**Goal**: Fetch multiple column chunks concurrently.

#### Approach: Thread Pool

```zig
// In ColumnReader or a new RowGroupReader
pub fn readColumnsParallel(
    allocator: std.mem.Allocator,
    source: RandomAccessSource,
    columns: []const ColumnChunk,
    thread_count: usize,
) ![]ColumnData {
    var pool = std.Thread.Pool.init(.{ .allocator = allocator });
    defer pool.deinit();
    
    var results = try allocator.alloc(ColumnData, columns.len);
    
    for (columns, 0..) |col, i| {
        pool.spawn(readColumnTask, .{ source, col, &results[i] });
    }
    
    pool.wait();
    return results;
}
```

---

## Testing Strategy

### Local Testing with MinIO

```bash
# Start MinIO (S3-compatible)
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Upload test file
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/test-bucket
mc cp data/simple.parquet local/test-bucket/
```

Then test:
```bash
./zig-out/bin/zpq schema s3://test-bucket/simple.parquet --endpoint http://localhost:9000
```

### Integration Tests

```zig
test "S3Source reads footer correctly" {
    var source = try S3Source.init(testing.allocator, "s3://test-bucket/simple.parquet");
    defer source.close();
    
    var pf = try ParquetFile.fromSource(testing.allocator, source.source());
    defer pf.deinit();
    
    try pf.readFooter();
    try testing.expect(pf.metadata != null);
}
```

---

## Migration Checklist

- [ ] Create `src/zpq/io.zig` with `RandomAccessSource` interface
- [ ] Implement `LocalFileSource`  
- [ ] Refactor `ParquetFile` to use `RandomAccessSource`
- [ ] Refactor `ColumnReader` to use `RandomAccessSource`
- [ ] Verify all existing tests pass (local files should work identically)
- [ ] Add `aws-sdk-for-zig` dependency
- [ ] Implement `S3Source` with basic range reads
- [ ] Add footer prefetch optimization
- [ ] Implement `BufferedRangeReader` for coalesced reads
- [ ] Add CLI support: `zpq schema s3://bucket/key`
- [ ] Add `--endpoint` flag for MinIO/R2/LocalStack
- [ ] Add parallel column fetching
- [ ] Benchmark S3 performance vs PyArrow

---

## Estimated Timeline

| Phase | Effort | Description |
|-------|--------|-------------|
| **Phase 1** | 2-3 days | I/O abstraction (breaking change, careful refactor) |
| **Phase 2** | 3-4 days | Basic S3 support with aws-sdk-for-zig |
| **Phase 3** | 2-3 days | Buffered range reader optimizations |
| **Phase 4** | 2-3 days | Parallel fetching + benchmarking |

---

## Key Decision: Why `aws-sdk-for-zig` Over DIY

| Aspect | DIY (SigV4 + libcurl) | aws-sdk-for-zig |
|--------|----------------------|-----------------|
| **SigV4 Implementation** | ~500+ lines of crypto code | ✅ Done |
| **Credential Chain** | Must implement env vars, config files, IAM | ✅ Done |
| **TLS/HTTPS** | Must link libcurl or wrestle with std.http | ✅ Handled |
| **Maintenance** | You own it forever | Community maintained |
| **Time to Working S3** | 1-2 weeks | 1-2 days |

The research output's Phase 3 ("Implement AWS SigV4 signing") becomes unnecessary. Jump straight to the interesting Parquet-specific optimizations.

---

## References

- [aws-sdk-for-zig](https://git.lerch.org/lobo/aws-sdk-for-zig) - The SDK to use
- [Rust object_store](https://docs.rs/object_store/latest/object_store/) - Reference for coalesce logic
- [Arrow C++ S3FileSystem](https://github.com/apache/arrow/blob/main/cpp/src/arrow/filesystem/s3fs.cc) - Reference for buffering strategy

