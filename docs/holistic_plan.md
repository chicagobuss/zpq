# ZPQ Surgical Read Engine

## Status

**Implemented:**
- Planner: Page-level pruning using ColumnIndex ✓
- PageFetcher: Byte-range reads using OffsetIndex ✓
- CLI: `--surgical` flag ✓
- Row-group pruning: Works ✓
- Page-level pruning: Works (tested with `string_sorted`) ✓

**Remaining:**
- Integration with existing decoder (RowGroupWorker)
- Integration with existing writer (morsel for S3, slot for local)

## Design Philosophy

**Surgical planning, existing execution.**

The surgical engine optimizes the **reading phase** by using page-level metadata 
(ColumnIndex, OffsetIndex) to minimize I/O. The **decoding and writing phases** 
reuse existing infrastructure (RowGroupWorker, MorselCoordinator, SlotWriter).

## The Problem

Current architecture:
```
fetch(entire columns) → decode → filter → encode output
```

With surgical planning:
```
plan(ColumnIndex) → fetch(minimal pages) → decode → filter → encode output
```

For `WHERE id = 1000` on a 100MB file:
- Current: Fetch ~25MB (whole row group columns)
- Surgical: Fetch ~50KB (2 pages)

## Current Implementation

### What Works

```
src/zpq/core/surgical/
├── types.zig       # FetchPlan, PagePlan, ByteRange, etc.
├── planner.zig     # Builds FetchPlan from ColumnIndex/OffsetIndex  
├── fetcher.zig     # Executes byte-range reads
└── engine.zig      # Orchestrates plan/fetch (no decode yet)
```

The surgical engine currently:
1. Plans which pages to fetch using ColumnIndex min/max
2. Fetches only those pages
3. Reports I/O savings but doesn't decode/write output

### What's Missing

The surgical engine has its own broken decoder. Instead, it should feed data 
into the existing `RowGroupWorker` which already handles:
- Filter column scanning (scanFilterColumn)
- Output column decoding (decodeOutputColumns) 
- Buffer encoding (encodeToBuffer)

## Integration Plan

### Key Insight

Surgical planning replaces the **data fetching** step, not the decode/write steps.
The existing pipeline does:

```
1. Fetch entire columns for all row groups (readRanges)
2. Build RowGroupData structs with buffers
3. RowGroupWorker.execute() - scan filter, decode outputs
4. worker.encodeToBuffer() - encode to parquet pages
5. coordinator.submitMorsel() or slotWriter.writeSlot()
```

Surgical integration changes step 1:
```
1a. Planner.buildPlan() - identify minimal pages
1b. PageFetcher.fetch() - read only needed pages
1c. Assemble sparse buffers into RowGroupData format
2-5. Same as before
```

### Implementation Steps

#### Step 1: Remove surgical/decoder.zig

The broken decoder is not needed. Delete it.

#### Step 2: Modify surgical/engine.zig

Instead of decoding pages itself, the engine should:
1. Build FetchPlan (already done)
2. Fetch pages (already done)
3. Assemble fetched pages into contiguous column buffers
4. Return buffers in format compatible with RowGroupData

```zig
pub const SurgicalResult = struct {
    /// Row groups that passed pruning
    active_rg_indices: []usize,
    
    /// Per-row-group column buffers (same format as current pipeline)
    /// rg_buffers[rg_idx][col_idx] = column data
    rg_buffers: [][][]u8,
    
    /// Byte offsets within buffers (for MemorySource)
    rg_offsets: [][]u64,
    
    /// I/O stats
    bytes_fetched: u64,
    pages_skipped: usize,
    pages_total: usize,
};
```

#### Step 3: Add surgical path to executeMorselParallelWithLoop

Replace the "Pre-fetch all row group data" section with surgical fetching:

```zig
// Current code (lines 1395-1485):
// - Builds ranges[] for ALL columns in ALL row groups
// - Calls pf.source.readRanges(ranges, buffers)

// Surgical replacement:
if (use_surgical) {
    var surgical_engine = SurgicalEngine.init(allocator, pf, ...);
    const surgical_result = try surgical_engine.execute();
    
    // surgical_result contains:
    // - Only active row groups (passed RG + page pruning)
    // - Buffers with only fetched pages (may be sparse)
    
    // Build RowGroupData from surgical_result
    for (surgical_result.active_rg_indices, 0..) |rg_idx, i| {
        all_rg_data[rg_idx] = RowGroupData{
            .rg_idx = rg_idx,
            .filter_buf = surgical_result.filter_bufs[i],
            // ... etc
        };
    }
} else {
    // Existing full-fetch path
}
```

#### Step 4: Handle sparse page buffers

When surgical fetching skips pages, the column buffer is incomplete.
Two approaches:

**Option A: Contiguous reassembly**
After fetching, reassemble pages into a contiguous buffer. 
RowGroupWorker expects contiguous column data.

```zig
fn assembleColumnBuffer(pages: []?[]const u8, plan: *const PagePlan) ![]u8 {
    // Calculate total size
    var total_size: usize = 0;
    for (pages) |page| {
        if (page) |p| total_size += p.len;
    }
    
    // Copy pages contiguously
    var buffer = try allocator.alloc(u8, total_size);
    var offset: usize = 0;
    for (pages) |page| {
        if (page) |p| {
            @memcpy(buffer[offset..][0..p.len], p);
            offset += p.len;
        }
    }
    return buffer;
}
```

**Option B: Modify RowGroupWorker to handle sparse**
More invasive but potentially more efficient.

**Recommendation: Option A** - simpler, less risk of breaking existing code.

#### Step 5: Output writer selection

The existing auto-selection already handles this:

```zig
// pipeline.zig:282-283
const effective_mode = if (mode == .slot_parallel and 
    std.mem.startsWith(u8, self.output_path.?, "s3://")) 
    .morsel_parallel else mode;
```

Surgical mode should follow the same pattern:
- `--surgical` with S3 output → surgical fetch + morsel write
- `--surgical` with local output → surgical fetch + slot write

### Code Changes Summary

| File | Change |
|------|--------|
| `surgical/decoder.zig` | DELETE |
| `surgical/engine.zig` | Return assembled buffers instead of decoding |
| `surgical/fetcher.zig` | Add page reassembly function |
| `pipeline.zig` | Add surgical path in executeMorselParallelWithLoop |
| `pipeline.zig` | Add surgical path in executeSlotParallel |

### Testing

1. **Correctness**: Same output as non-surgical mode
   ```bash
   zpq --filter "x=1" in.parquet out1.parquet
   zpq --surgical --filter "x=1" in.parquet out2.parquet
   diff <(parquet-tools cat out1.parquet) <(parquet-tools cat out2.parquet)
   ```

2. **I/O savings**: Fewer bytes read
   ```bash
   # Check [SURGICAL] Plan output shows pages_skipped > 0
   zpq --surgical --filter "string_sorted=row_0000400000" benchmark.parquet /tmp/out.parquet
   ```

3. **Performance**: Faster for selective queries on sorted columns

## When Surgical Helps

Page-level pruning is most effective when:
- Column has **multiple data pages** (not heavily dictionary-encoded)
- Data is **sorted or clustered** (tight per-page min/max ranges)
- Filter is **highly selective** (value in few pages)

When it doesn't help:
- Single page per column (common for small row groups or dict encoding)
- Random data distribution (wide min/max ranges)
- Low selectivity filters (need most pages anyway)

The engine gracefully degrades - when page indexes don't help, it falls back 
to fetching full columns, same as current behavior.
