//! Per-row-group scan→consumer protocol.
//!
//! Lambda's parallel-fetcher orchestrator and the CLI's sequential
//! local-file driver both feed the same shape of work to the same
//! two consumers: *encode* (decode → filter → re-encode kept cols)
//! and *copy* (byte-copy kept chunks, shift offsets).
//!
//! The only thing they differ on is *where the bytes come from*:
//! lambda hands in a `RowGroupResult.raw_bytes` window covering the
//! kept-column span, CLI hands in the entire mmap'd file. Both shapes
//! collapse to a single `RGSrc { bytes, byte_origin }` — the per-
//! column offset into the source equals `col.dpo - byte_origin`
//! either way.
//!
//! Output is always a `streaming.Sink` (multipart-S3 in lambda; an
//! fd-backed sink in the CLI). Per-phase timings flow into a shared
//! `Timings` accumulator the caller owns.
//!
//! Layering: this module lives in `core/`, so it must NOT depend on
//! any I/O. The `Sink` is opaque; the source is a borrowed slice.

const std = @import("std");
const builtin = @import("builtin");
const owner_groups = @import("owner_groups.zig");
const msd_groups = @import("msd_groups.zig");
const retained_pages = @import("retained_pages.zig");
const profile = @import("../profile.zig");

const blocking = @import("blocking.zig");
const schema = @import("schema.zig");
pub const SelectedBatch = @import("selected_batch.zig").SelectedBatch;
const agg_plan = @import("agg_plan.zig");
const encoder = @import("writer/encoder.zig");
const streaming = @import("writer/streaming.zig");
const fastpath = @import("writer/fastpath.zig");
const page_index = @import("writer/page_index.zig");
const thrift = @import("thrift.zig");
const column_mod = @import("parquet/column.zig");
const page_mod = @import("parquet/page.zig");
const Diag = @import("diag.zig").Diag;
const Text = @import("diag.zig").Text;
const compression = @import("parquet/compression.zig");
const invariant = @import("invariant.zig");
const metadata = @import("parquet/metadata.zig");
const statistics = @import("parquet/statistics.zig");
const agg_coded = @import("expr/agg_coded.zig");
const agg_dense = @import("expr/agg_dense.zig");
const partagg = @import("partagg.zig");
const huge_pages = @import("huge_pages.zig");

pub const DecodeOptions = column_mod.DecodeOptions;
pub const DecodeScratch = column_mod.DecodeScratch;
pub const SliceScratch = page_mod.SliceScratch;
pub const RowGroupArena = @import("row_group_arena.zig").RowGroupArena;

const types = @import("consumer/types.zig");
pub const Error = types.Error;
pub const RGSrc = types.RGSrc;
pub const Timings = types.Timings;
pub const DecodeSite = types.DecodeSite;
pub const enterChunk = types.enterChunk;
pub const decodeFailure = types.decodeFailure;
pub const setDecodeFailure = types.setDecodeFailure;
pub const resolveDecodeFailure = types.resolveDecodeFailure;
pub const isDecodeError = types.isDecodeError;
pub const RGOut = types.RGOut;
pub const rebaseRowGroupOffsets = types.rebaseRowGroupOffsets;
const decode_order = @import("decode_order.zig");

const output = @import("consumer/output.zig");
pub const OutputAggregator = output.OutputAggregator;
pub const initOutputAggregator = output.initOutputAggregator;
pub const resetAggregator = output.resetAggregator;

pub const appendProjectedRG = output.appendProjectedRG;
pub const appendProjectedRGWithOptions = output.appendProjectedRGWithOptions;
pub const ProjectedRG = output.ProjectedRG;
pub const Survivors = output.Survivors;
const gatherRows = output.gatherRows;

const decodeWriteColumn = consumer_column.decodeWriteColumn;

pub const appendSliceRows = output.appendSliceRows;

const namedLevels = output.namedLevels;
const flatLevels = output.flatLevels;

pub const renameLeaf = types.renameLeaf;

const booleanLane = output.booleanLane;
pub const outputLane = output.outputLane;

pub const chunkDictionary = consumer_column.chunkDictionary;

const slice_reader = @import("consumer/slice_reader.zig");
pub const SliceReader = slice_reader.SliceReader;

const decimalKind = consumer_column.decimalKind;
const decodeDecimalI128 = consumer_column.decodeDecimalI128;

const appendSelected = output.appendSelected;

const decodeWriteColumnMaybeCoded = consumer_column.decodeWriteColumnMaybeCoded;

const appendIntoBuf = output.appendIntoBuf;
const appendTyped = output.appendTyped;

const encode_module = @import("consumer/encode.zig");
pub const encodeAggregator = encode_module.encodeAggregator;
pub const RGEncoder = encode_module.RGEncoder;
pub const plainBits = encode_module.plainBits;
pub const plainBitsPrefix = encode_module.plainBitsPrefix;
pub const placeChunk = encode_module.placeChunk;
pub const AggEncode = encode_module.AggEncode;
pub const answer_rg_rows = encode_module.answer_rg_rows;
pub const writeAggregateRows = encode_module.writeAggregateRows;
pub const AggregateFile = encode_module.AggregateFile;
pub const bufRows = output.bufRows;
pub const appendRows = output.appendRows;
pub const BufRange = output.BufRange;
pub const joinRows = output.joinRows;

pub const OutputCol = types.OutputCol;
pub const computedSchemaElem = types.computedSchemaElem;
pub const decimalElem = types.decimalElem;

pub const RowSubgroup = types.RowSubgroup;
pub const AggRowGroup = types.AggRowGroup;
pub const selectedWindow = types.selectedWindow;

const target_module = @import("consumer/target.zig");
pub const PackedCount = target_module.PackedCount;
pub const IntegerCountSink = target_module.IntegerCountSink;
pub const SelectedRecords = target_module.SelectedRecords;
pub const RecordSink = target_module.RecordSink;

const scan_module = @import("consumer/scan.zig");
pub const scanRGForAgg = scan_module.scanRGForAgg;
pub const probeDecodeFailure = scan_module.probeDecodeFailure;
pub const canonicalDecodeFailure = scan_module.canonicalDecodeFailure;
const automaticCacheRows = scan_module.automaticCacheRows;

pub const AggTarget = target_module.AggTarget;

const workspace_module = @import("consumer/workspace.zig");
pub const AggWorkspace = workspace_module.AggWorkspace;
const admitWhole = workspace_module.admitWhole;
const dropRegion = workspace_module.dropRegion;
const refusedDraw = workspace_module.refusedDraw;

const budget_module = @import("consumer/budget.zig");
pub const DecodeBudget = budget_module.DecodeBudget;
pub const decodeEstimate = budget_module.decodeEstimate;

pub const CacheHints = workspace_module.CacheHints;
pub const cacheHintsWanted = workspace_module.cacheHintsWanted;
const cacheDictionary = workspace_module.cacheDictionary;

const pool_module = @import("consumer/pool.zig");
pub const poolOn = pool_module.poolOn;
pub const releaseSlicePool = pool_module.releaseSlicePool;


const scan_plan = @import("consumer/plan.zig");
const StagePlan = scan_plan.StagePlan;
const CallBatch = scan_plan.CallBatch;
const payloadColumns = scan_plan.payloadColumns;
const plainValueEncodings = scan_plan.plainValueEncodings;
const hashOnlyByteArray = scan_plan.hashOnlyByteArray;
const streamByteArray = scan_plan.streamByteArray;
const streamWindowRows = scan_plan.streamWindowRows;
const GatedFilter = scan_plan.GatedFilter;
const foldWantsCodes = scan_plan.foldWantsCodes;
const flatGroupKey = scan_plan.flatGroupKey;
const keyCodesCheaper = scan_plan.keyCodesCheaper;
const columnFeedsFold = scan_plan.columnFeedsFold;
const plainLengthColumn = scan_plan.plainLengthColumn;
const ChunkDecoder = scan_plan.ChunkDecoder;
const sliceRows = scan_plan.sliceRows;
const planCodeUses = scan_plan.planCodeUses;
const mark = scan_plan.mark;

const agg_column = @import("consumer/agg_column.zig");
const decodeAggColumnCoded = agg_column.decodeAggColumnCoded;
const decodeAggColumnHist = agg_column.decodeAggColumnHist;
const decodeLevelsOnly = agg_column.decodeLevelsOnly;
const decodeAggColumn = agg_column.decodeAggColumn;

const pruning = @import("consumer/pruning.zig");
const PageIndex = pruning.PageIndex;
const FilterPages = pruning.FilterPages;
const loadPageIndexes = pruning.loadPageIndexes;
const pruneFilterPages = pruning.pruneFilterPages;
const pagePruning = pruning.pagePruning;
const compactRows = pruning.compactRows;

const copy_module = @import("consumer/copy.zig");
pub const copyRG = copy_module.copyRG;
pub const BloomWrite = copy_module.BloomWrite;
pub const Recompress = copy_module.Recompress;
pub const RecompressedChunk = copy_module.RecompressedChunk;
pub const recompressRowGroup = copy_module.recompressRowGroup;
const dictionaryBloom = copy_module.dictionaryBloom;
const PageMove = copy_module.PageMove;
const recompressChunk = copy_module.recompressChunk;
const sourceBytes = types.sourceBytes;

const consumer_column = @import("consumer/column.zig");
pub const checkRowShape = consumer_column.checkRowShape;
pub const PruningInfo = consumer_column.PruningInfo;
pub const RowRange = consumer_column.RowRange;
pub const decodeColumnTPruned = consumer_column.decodeColumnTPruned;
pub const decodeFlbaColumnPruned = consumer_column.decodeFlbaColumnPruned;
pub const decodeColumnT = consumer_column.decodeColumnT;
pub const decodeColumnTWithOptions = consumer_column.decodeColumnTWithOptions;
pub const decodeFlbaColumn = consumer_column.decodeFlbaColumn;
pub const decodeFlbaColumnWithOptions = consumer_column.decodeFlbaColumnWithOptions;
pub const flbaWidth = consumer_column.flbaWidth;
const originSlice = consumer_column.originSlice;
const defaultVal = consumer_column.defaultVal;
const decodeWithReaderPruned = consumer_column.decodeWithReaderPruned;
const decodeFloat16ColumnAsF64Pruned = consumer_column.decodeFloat16ColumnAsF64Pruned;
const decodeU32ColumnAsI64Pruned = consumer_column.decodeU32ColumnAsI64Pruned;
const decodeFloat16ColumnAsF64 = consumer_column.decodeFloat16ColumnAsF64;

/// Test-only: off compares compact decoding with whole columns; the count shows a test reached it.
pub var compact_rows_in_tests = true;
pub var compacted_row_groups: std.atomic.Value(usize) = .init(0);

// ============================================================
// Compile-time API check. End-to-end behavior is exercised through
// the lambda integration tests + CLI golden tests.
// ============================================================
test "consumer: API is well-typed" {
    _ = appendProjectedRG;
    _ = encodeAggregator;
    _ = copyRG;
    _ = decodeColumnT;
}

