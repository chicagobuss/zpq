//! Multi-file aggregate orchestrator. Source-agnostic: takes a list of
//! `Input { name, bytes }` already-fetched-or-mmap'd files and runs an
//! aggregate query across all of them. Both the CLI (mmap → bytes) and
//! the Lambda (S3 fetch → bytes) feed the same orchestrator.
//!
//! Sans-IO. Allocates only what the caller's allocators allow; never
//! does syscalls or network I/O of its own. The bytes are someone else's
//! problem.
//!
//! What lives here:
//!   - `Input`             one file's pre-fetched bytes + display name
//!   - `MultiAggArgs`      query spec (filter, aggregate, parallelism)
//!   - `MultiAggResult`    materialized aggregate values + per-stage timings
//!   - `runMultiAggregate` the orchestrator
//!
//! What does NOT live here:
//!   - mmap / file I/O      (cli/query.zig's mmapFile)
//!   - S3 fetch             (io/s3.zig + future io/s3_source.zig)
//!   - 1-row parquet output (cli/query.zig's writeOneRowParquet)

const ndv = @import("ndv.zig");
pub const NdvStat = ndv.Stat;

pub const scan_block_merge = @import("scan/block_merge.zig");
pub const owned_merge = @import("scan/owned_merge.zig");
pub const key_decode = @import("scan/key_decode.zig");
pub const deserializeKeyColumn = key_decode.deserializeKeyColumn;
pub const scan_footers = @import("scan/footers.zig");
pub const worker_contracts = @import("scan/worker_contracts.zig");
pub const pairs_policy = @import("scan/pairs_policy.zig");
pub const scan_worker = @import("scan/worker.zig");
pub const merge_index = @import("scan/merge_index.zig");
pub const scan_materialize = @import("scan/materialize.zig");
pub const materializeOne = scan_materialize.materializeOne;
pub const plan_types = @import("scan/plan_types.zig");
pub const PlanInputs = plan_types.PlanInputs;
pub const Reason = plan_types.Reason;
pub const AggregateDispatch = plan_types.AggregateDispatch;
pub const PlanFacts = plan_types.PlanFacts;
pub const Planning = plan_types.Planning;
pub const ScanPlan = plan_types.ScanPlan;
pub const AggregateExplanation = plan_types.AggregateExplanation;
pub const RecordAdmission = plan_types.RecordAdmission;
pub const scan_count_cut = @import("scan/count_cut.zig");
pub const scan_rank_cut = @import("scan/rank_cut.zig");
pub const scan_result = @import("scan/result.zig");
pub const ScanState = scan_result.ScanState;
pub const Timings = scan_result.Timings;
pub const MultiAggResult = scan_result.MultiAggResult;
pub const folded_acc = @import("scan/folded_acc.zig");
pub const scan_binding = @import("scan/binding.zig");
pub const CutShape = scan_binding.CutShape;
pub const cutShape = scan_binding.cutShape;
pub const checkDistinctOutputNames = scan_binding.checkDistinctOutputNames;
pub const scan_plan_impl = @import("scan/plan.zig");
pub const planAggregate = scan_plan_impl.planAggregate;
pub const decidingRule = scan_plan_impl.decidingRule;
pub const scan_rows = @import("scan/rows.zig");
pub const parallel_distinct = @import("scan/parallel_distinct.zig");
pub const slot_sort = @import("scan/slot_sort.zig");
pub const pair_fold = @import("scan/pair_fold.zig");
pub const scan_phase = @import("scan/phase.zig");
pub const scan_census = @import("scan/census.zig");
pub const record_workers = @import("scan/record_workers.zig");
pub const owner_support = @import("scan/owner_support.zig");
pub const input_rewind = @import("scan/input_rewind.zig");
pub const owner_integer = @import("scan/owner_integer.zig");
pub const owner_pair = @import("scan/owner_pair.zig");
pub const scan_owner_distinct = @import("scan/owner_distinct.zig");
pub const owner_bytes = @import("scan/owner_bytes.zig");
pub const partition_fill = @import("scan/partition_fill.zig");
pub const owner_strings = @import("scan/owner_strings.zig");
pub const owner_packed = @import("scan/owner_packed.zig");
pub const scan_merge = @import("scan/merge.zig");
pub const scan_private_decode = @import("scan/private_decode.zig");
pub const scan_execute = @import("scan/execute.zig");
pub const runMultiAggregate = scan_execute.runMultiAggregate;
pub const explainAggregate = scan_execute.explainAggregate;
pub const integration_support = @import("scan/integration_support.zig");
pub const scan_types = @import("scan/types.zig");
pub const MultiAggArgs = scan_types.MultiAggArgs;
pub const SketchTopK = scan_types.SketchTopK;
pub const Error = scan_types.Error;
pub const Input = scan_types.Input;
pub const FastPath = scan_types.FastPath;
pub const RowGroupGate = scan_types.RowGroupGate;
pub const DictProbe = scan_types.DictProbe;
pub const ReadStart = scan_types.ReadStart;
pub const GroupColType = scan_types.GroupColType;
pub const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
pub const byte_stream = @import("scan/byte_stream.zig");
pub const ByteWindow = byte_stream.ByteWindow;
pub const ByteStream = byte_stream.ByteStream;
pub const integer_stream = @import("scan/integer_stream.zig");
pub const IntegerStream = integer_stream.IntegerStream;
pub const typed_answer = @import("scan/typed_answer.zig");
pub const TypedAnswer = typed_answer.TypedAnswer;
pub const scan_output = @import("scan/output.zig");
pub const AggOutput = scan_output.AggOutput;
pub const output_values = @import("scan/output_values.zig");
pub const AggValue = output_values.AggValue;
pub const DecValue = output_values.DecValue;
pub const AggOutputItem = output_values.AggOutputItem;


test {
    _ = @import("scan/upstream_tests.zig");
    _ = @import("scan/execution_tests.zig");
    _ = @import("scan/statistics_tests.zig");
    _ = @import("scan/distinct_tests.zig");
    _ = @import("scan/decode_tests.zig");
    _ = @import("scan/owner_tests.zig");
    _ = @import("scan/distinct_owner_tests.zig");
}

