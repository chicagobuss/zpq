//! zpq's one query entry point.
//!
//! Both binaries (the CLI and the Lambda) are thin shells over
//! `runQuery(ctx, args) -> QueryResult`. Their job is purely:
//!   - CLI: argv → QueryArgs, format JSON to stdout, exit
//!   - Lambda: Runtime API JSON → QueryArgs, format JSON response, loop
//! Everything in between — input dispatch, parallel fetch, decode,
//! scan, output — lives here.
//!
//! The architecture follows the comparable engines: a per-file `scan.Input`
//! abstraction at the engine level (Polars' `DynByteSource`) and a
//! storage-shaped helper underneath that materializes Inputs from any
//! backend (`local` mmap, `s3` parallel range-fetch). Single-file vs
//! multi-file is just `inputs.len`; no special cases.
//!
//! Sans-IO it's not — engine.zig drives the I/O. But the core of what
//! engine does (parse, plan, decode, fold) lives in `core/`. Engine is
//! the IO orchestration layer that turns "list of paths/URLs and a
//! query" into "the bytes the query needs, fetched optimally, then
//! decoded through `core.scan.runMultiAggregate`."

const engine_text = @import("engine/text.zig");
const engine_types = @import("engine/types.zig");
const engine_s3_rebase = @import("engine/s3_rebase.zig");
const engine_s3_fetch = @import("engine/s3_fetch.zig");
const engine_rank = @import("engine/rank.zig");
const engine_write_window = @import("engine/write_window.zig");
const engine_projection = @import("engine/projection.zig");
const engine_input = @import("engine/input.zig");
const engine_write = @import("engine/write.zig");
const engine_env = @import("engine/env.zig");
const engine_print = @import("engine/print.zig");
const engine_aggregate = @import("engine/aggregate.zig");
pub const fetchS3Schema = engine_input.fetchS3Schema;
const std = @import("std");
const nowMonoNs = @import("clock.zig").monoNs;
const profile = @import("profile.zig");
const local_fs = @import("local_fs.zig");
const local_reader = @import("io/local_reader.zig");

const schema = @import("core/schema.zig");
const compression = @import("core/parquet/compression.zig");
const decimal_mod = @import("core/parquet/decimal.zig");
const huge_pages = @import("core/huge_pages.zig");
const core_memory = @import("core/memory.zig");
const consumer = @import("core/consumer.zig");
const invariant = @import("core/invariant.zig");
const scan = @import("core/scan.zig");
const ResultProgram = @import("core/result_program.zig").ResultProgram;
const Diag = @import("core/diag.zig").Diag;
const agg_plan = @import("core/agg_plan.zig");
const system = @import("core/system.zig");
const metadata = @import("core/parquet/metadata.zig");
const schema_tree = @import("core/parquet/schema_tree.zig");
const thrift = @import("core/thrift.zig");
const expr_ast = @import("core/expr/ast.zig");
const expr_parser = @import("core/expr/parser.zig");
const expr_agg = @import("core/expr/agg.zig");
const filter_eval = @import("core/filter/eval.zig");
const filter_ast = @import("core/filter/ast.zig");
const filter_selection = @import("core/filter/selection.zig");
const filter_parser = @import("core/filter/parser.zig");
const filter_prune = @import("core/filter/prune.zig");
const statistics = @import("core/parquet/statistics.zig");
const membership = @import("core/filter/membership.zig");
const nanprobe = @import("core/filter/nanprobe.zig");
const sbbf = @import("core/parquet/bloom.zig");
const streaming = @import("core/writer/streaming.zig");
const fastpath = @import("core/writer/fastpath.zig");
const encoder = @import("core/writer/encoder.zig");
const page_index = @import("core/writer/page_index.zig");

const s3 = @import("io/s3.zig");
const s3_window = @import("io/s3_window.zig");
const tls = @import("io/tls.zig");
const http = @import("io/http.zig");
const sigv4 = @import("io/sigv4.zig");
const multipart_sink = @import("io/multipart_sink.zig");
const pool_mod = @import("io/pool.zig");
const coalescer = @import("io/coalescer.zig");
const work_cursor = @import("io/work_cursor.zig");
const spawn_util = @import("core/spawn.zig");
const AtomicWorkCursor = work_cursor.AtomicWorkCursor;
const meta_cache_mod = @import("io/meta_cache.zig");

const S3Fetcher = engine_s3_fetch.S3Fetcher;
const FetchWindow = engine_s3_fetch.FetchWindow;
pub const READ_CONCURRENCY = engine_s3_fetch.READ_CONCURRENCY;
const PAR1 = engine_s3_fetch.PAR1;
const planRanges = engine_s3_fetch.planRanges;
const appendSplitFetch = engine_s3_fetch.appendSplitFetch;
const orderFilterFirst = engine_s3_fetch.orderFilterFirst;
const FetchSpec = engine_s3_fetch.FetchSpec;
const S3Membership = engine_s3_fetch.S3Membership;
const releaseChargedTail = engine_s3_fetch.releaseChargedTail;
const trimRetainedSuffix = engine_s3_fetch.trimRetainedSuffix;
const appendRgFetchRanges = engine_s3_fetch.appendRgFetchRanges;
const ampCap = engine_s3_fetch.ampCap;
const fetchMetaBatch = engine_s3_fetch.fetchMetaBatch;
const ensureHeadMagic = engine_s3_fetch.ensureHeadMagic;

const lambda_window_cap = engine_s3_fetch.lambda_window_cap;
const s3FetchWindow = engine_s3_fetch.s3FetchWindow;

pub const POOL_SIZE = engine_types.POOL_SIZE;

pub const FOOTER_PROBE_DEFAULT = engine_s3_fetch.FOOTER_PROBE_DEFAULT;
pub const footerProbeBytes = engine_s3_fetch.footerProbeBytes;
pub const footerProbeFor = engine_s3_fetch.footerProbeFor;

const ByteAccount = engine_s3_fetch.ByteAccount;

pub const OUTPUT_POOL_SIZE = engine_types.OUTPUT_POOL_SIZE;
pub const OutputLane = engine_types.OutputLane;

pub const Error = engine_types.Error;
pub const ColumnarSink = engine_types.ColumnarSink;

pub const QueryArgs = engine_types.QueryArgs;
pub const CompressPriority = engine_types.CompressPriority;
pub const BloomSpec = engine_types.BloomSpec;
pub const Context = engine_types.Context;
pub const PoolRegistry = engine_types.PoolRegistry;
pub const QueryResult = engine_types.QueryResult;
pub const WriteResult = engine_types.WriteResult;
pub const AggResult = engine_types.AggResult;
pub const Timings = engine_types.Timings;

pub const runQuery = engine_aggregate.runQuery;
pub const explainAggregate = engine_aggregate.explainAggregate;

// ============================================================
// Aggregate path
// ============================================================

pub const AggregateExplanation = engine_types.AggregateExplanation;
pub const Conjunct = engine_types.Conjunct;

pub const ResultExplanation = engine_types.ResultExplanation;

// ============================================================
// Write path — projection / select / filter, output to local or s3
// ============================================================

// ── Windowed parallel re-encode (Io.Group.async + sliding window) ─────
// Producers decode+filter+encode whole row groups concurrently, bounded by a
// CPU budget; a single in-order writer drains their buffers to the
// sink, rebasing each row group's file offsets. A sliding window (an
// Io.Semaphore of W permits) caps how many encoded row groups may be in flight
// at once, so peak memory = W × rg_bytes regardless of total output size. W is
// sized from the environment by the caller: generously from the owned memory
// tier on Lambda, a small polite multiple of the worker count elsewhere.
//
// Idiom: this is the same std.Io concurrency the S3 paths already use, applied
// to CPU work — replacing raw std.Thread.spawn + buffer-all. (See the
// probe_io_concurrency / probe_backpressure spikes at e76a1e3 for why async + async_limit,
// not concurrent, and why a sliding window rather than byte credits.)

/// A Sink that appends into an in-memory buffer (offsets relative to 0).
pub const RowGroupTarget = engine_types.RowGroupTarget;
pub const default_row_group_rows = engine_types.default_row_group_rows;
pub const default_row_group_bytes = engine_types.default_row_group_bytes;

const ENCODE_REGION_RESERVE = engine_write_window.ENCODE_REGION_RESERVE;
const WorkRegions = engine_write_window.WorkRegions;
const RGJob = engine_write_window.RGJob;
const skipRowGroup = engine_write_window.skipRowGroup;
const Piece = engine_write_window.Piece;
const Slot = engine_write_window.Slot;
const WinCtx = engine_write_window.WinCtx;
const runWindowedReencode = engine_write_window.runWindowedReencode;
const runWindowed = engine_write_window.runWindowed;
const RecompressCtx = engine_write_window.RecompressCtx;
const RecompressSlot = engine_write_window.RecompressSlot;
const writeWindow = engine_write_window.writeWindow;
const anyChunkNeedsRecompress = engine_write_window.anyChunkNeedsRecompress;
const keptColumnsUseCodec = engine_write_window.keptColumnsUseCodec;

const sketchFromEnv = engine_env.sketchFromEnv;
const envFlag = engine_env.envFlag;
const envDisables = engine_env.envDisables;

const ProjectionPlan = engine_projection.ProjectionPlan;
const planProjection = engine_projection.planProjection;

const runWrite = engine_write.runWrite;
const coerceByteArrayDecimalLeavesToFlba = engine_write.coerceByteArrayDecimalLeavesToFlba;
const coerceFloat16LeavesToDouble = engine_write.coerceFloat16LeavesToDouble;
const buildSelectSchema = engine_write.buildSelectSchema;
const FdSink = engine_write.FdSink;
const createFile = engine_write.createFile;

const credentialsFromEnv = engine_input.credentialsFromEnv;
const OpenedInputs = engine_input.OpenedInputs;
const loadLocalMembership = engine_input.loadLocalMembership;
const materializeMetas = engine_input.materializeMetas;
const openInputs = engine_input.openInputs;
const hasGatedInputs = engine_input.hasGatedInputs;
const metadataGate = engine_input.metadataGate;
const allLocal = engine_input.allLocal;
const plannedResidency = engine_input.plannedResidency;
const LocalPlan = engine_input.LocalPlan;
const planLocalReads = engine_input.planLocalReads;
const planLocalReader = engine_input.planLocalReader;
const DictProbeSink = engine_input.DictProbeSink;
const ReadStarter = engine_input.ReadStarter;

const bindFilter = engine_projection.bindFilter;

const CompactRange = engine_s3_rebase.CompactRange;

const rebaseFetchedOffsets = engine_s3_rebase.rebaseFetchedOffsets;

const suffixStart = engine_s3_fetch.suffixStart;
const parseTotalFromContentRange = engine_s3_fetch.parseTotalFromContentRange;

const writeOneRowParquet = engine_write.writeOneRowParquet;
const groupedColumns = engine_write.groupedColumns;
const writeGroupedParquet = engine_write.writeGroupedParquet;
const typedColumns = engine_write.typedColumns;
const writeTypedParquet = engine_write.writeTypedParquet;
const writeIntegerStream = engine_write.writeIntegerStream;
const writeByteStream = engine_write.writeByteStream;
const retypeOutputCol = engine_write.retypeOutputCol;

// ============================================================
// Print path - CSV / JSONL streaming row output
// ============================================================

const releaseStreamAnswer = engine_text.releaseStreamAnswer;
const answerTypes = engine_text.answerTypes;
const answerTextKinds = engine_text.answerTextKinds;
pub const writeAggregateText = engine_text.writeAggregateText;
pub const writeAggregateJsonValue = engine_text.writeAggregateJsonValue;
pub const writeAggregateExplanationJson = engine_text.writeAggregateExplanationJson;

pub const PrintFormat = engine_types.PrintFormat;

const writeAggValueCsv = engine_text.writeAggValueCsv;

const writeCsvString = engine_text.writeCsvString;
const writeCsvParts = engine_text.writeCsvParts;

pub const writeJsonFloat = engine_text.writeJsonFloat;
pub const writeMembershipJson = engine_text.writeMembershipJson;
pub const kindAt = engine_text.kindAt;
pub const writeTypedAggValueJson = engine_text.writeTypedAggValueJson;
pub const writeAggValueJson = engine_text.writeAggValueJson;
pub const writeJsonQuoted = engine_text.writeJsonQuoted;
pub const writeJsonString = engine_text.writeJsonString;
pub const writeJsonText = engine_text.writeJsonText;
pub const TextKind = engine_text.TextKind;
pub const Temporal = engine_text.Temporal;
pub const writeBytesCsv = engine_text.writeBytesCsv;
pub const AnswerType = engine_text.AnswerType;
pub const DecimalColumns = engine_text.DecimalColumns;
pub const writeBytesJson = engine_text.writeBytesJson;
const printCell = engine_text.printCell;

const RowSortKey = engine_rank.RowSortKey;
const SortCell = engine_rank.SortCell;
const sortCell = engine_rank.sortCell;
const RowPos = engine_rank.RowPos;
const TopRows = engine_rank.TopRows;
const allPassthrough = engine_rank.allPassthrough;
const RankedRG = engine_rank.RankedRG;
const rankRowGroups = engine_rank.rankRowGroups;
const sortsAfter = engine_rank.sortsAfter;

const gatherEligible = engine_rank.gatherEligible;
const topnLimit = engine_rank.topnLimit;
const rankAndGather = engine_rank.rankAndGather;
const topn_gather_inputs = engine_rank.topn_gather_inputs;

pub const StdoutWriter = engine_print.StdoutWriter;
pub const PrintResult = engine_print.PrintResult;
pub const runPrint = engine_print.runPrint;
pub const runPrintTo = engine_print.runPrintTo;
pub const runPrintSink = engine_print.runPrintSink;

// ============================================================
// Compile-time API check.
// ============================================================
const pageIndexTestChunk = engine_s3_rebase.pageIndexTestChunk;

