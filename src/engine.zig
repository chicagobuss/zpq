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

pub const types = @import("engine/types.zig");
pub const text = @import("engine/text.zig");
pub const json_string = @import("engine/json_string.zig");
pub const env = @import("engine/env.zig");
pub const projection = @import("engine/projection.zig");
pub const syntax_plan = @import("engine/syntax_plan.zig");
pub const fanout_plan = @import("engine/fanout_plan.zig");
pub const s3_rebase = @import("engine/s3_rebase.zig");
pub const s3_fetch = @import("engine/s3_fetch.zig");
pub const input = @import("engine/input.zig");
pub const write_window = @import("engine/write_window.zig");
pub const write = @import("engine/write.zig");
pub const rank = @import("engine/rank.zig");
pub const print = @import("engine/print.zig");
pub const aggregate = @import("engine/aggregate.zig");

pub const fetchS3Schema = input.fetchS3Schema;
pub const READ_CONCURRENCY = s3_fetch.READ_CONCURRENCY;
pub const POOL_SIZE = types.POOL_SIZE;
pub const FOOTER_PROBE_DEFAULT = s3_fetch.FOOTER_PROBE_DEFAULT;
pub const footerProbeBytes = s3_fetch.footerProbeBytes;
pub const footerProbeFor = s3_fetch.footerProbeFor;
pub const OUTPUT_POOL_SIZE = types.OUTPUT_POOL_SIZE;
pub const OutputLane = types.OutputLane;
pub const Error = types.Error;
pub const ColumnarSink = types.ColumnarSink;
pub const QueryArgs = types.QueryArgs;
pub const CompressPriority = types.CompressPriority;
pub const BloomSpec = types.BloomSpec;
pub const Context = types.Context;
pub const PoolRegistry = types.PoolRegistry;
pub const QueryResult = types.QueryResult;
pub const WriteResult = types.WriteResult;
pub const AggResult = types.AggResult;
pub const Timings = types.Timings;
pub const runQuery = aggregate.runQuery;
pub const explainAggregate = aggregate.explainAggregate;
pub const AggregateExplanation = types.AggregateExplanation;
pub const Conjunct = types.Conjunct;
pub const ResultExplanation = types.ResultExplanation;
pub const RowGroupTarget = types.RowGroupTarget;
pub const default_row_group_rows = types.default_row_group_rows;
pub const default_row_group_bytes = types.default_row_group_bytes;
pub const writeAggregateText = text.writeAggregateText;
pub const writeAggregateJsonValue = text.writeAggregateJsonValue;
pub const writeAggregateExplanationJson = text.writeAggregateExplanationJson;
pub const PrintFormat = types.PrintFormat;
pub const writeJsonFloat = text.writeJsonFloat;
pub const writeMembershipJson = text.writeMembershipJson;
pub const kindAt = text.kindAt;
pub const writeTypedAggValueJson = text.writeTypedAggValueJson;
pub const writeAggValueJson = text.writeAggValueJson;
pub const writeJsonQuoted = text.writeJsonQuoted;
pub const writeJsonString = text.writeJsonString;
pub const writeJsonText = text.writeJsonText;
pub const TextKind = text.TextKind;
pub const Temporal = text.Temporal;
pub const writeBytesCsv = text.writeBytesCsv;
pub const AnswerType = text.AnswerType;
pub const DecimalColumns = text.DecimalColumns;
pub const writeBytesJson = text.writeBytesJson;
pub const StdoutWriter = print.StdoutWriter;
pub const PrintResult = print.PrintResult;
pub const runPrint = print.runPrint;
pub const runPrintTo = print.runPrintTo;
pub const runPrintSink = print.runPrintSink;
