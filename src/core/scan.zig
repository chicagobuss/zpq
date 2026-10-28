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

const std = @import("std");
const thrift_mod = @import("thrift.zig");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");

const spawn_util = @import("spawn.zig");
const huge_pages = @import("huge_pages.zig");
const schema = @import("schema.zig");
const consumer = @import("consumer.zig");
const agg_plan = @import("agg_plan.zig");
const metadata = @import("parquet/metadata.zig");
const expr_ast = @import("expr/ast.zig");
const expr_parser = @import("expr/parser.zig");
const expr_agg = @import("expr/agg.zig");
const group_dependencies = @import("expr/group_dependencies.zig");
const core_memory = @import("memory.zig");
const partagg = @import("partagg.zig");
const string_table = @import("string_table.zig");
const ndv = @import("ndv.zig");
const agg_dense = @import("expr/agg_dense.zig");
const key_pack = @import("expr/key_pack.zig");
const owner_groups = @import("owner_groups.zig");
const grouped_bitmap = @import("grouped_distinct_bitmap.zig");
const msd_groups = @import("msd_groups.zig");
const column_mod = @import("parquet/column.zig");
const row_subgroups = @import("row_subgroups.zig");
pub const NdvStat = ndv.Stat;
const filter_ast = @import("filter/ast.zig");
const filter_parser = @import("filter/parser.zig");
const filter_prune = @import("filter/prune.zig");
const filter_eval = @import("filter/eval.zig");
const filter_selection = @import("filter/selection.zig");
const membership = @import("filter/membership.zig");
const nanprobe = @import("filter/nanprobe.zig");
const Diag = @import("diag.zig").Diag;
const core_bytes = @import("bytes.zig");
const blocking = @import("blocking.zig");
const keyEql = core_bytes.keyEql;
const KeyHashMap = core_bytes.KeyHashMap;
const KeyHashMapUnmanaged = core_bytes.KeyHashMapUnmanaged;

pub const scan_block_merge = @import("scan/block_merge.zig");
const BlockMerge = scan_block_merge.BlockMerge;
pub const owned_merge = @import("scan/owned_merge.zig");
const mergeAccOwned = owned_merge.mergeAccOwned;
const detachOwned = owned_merge.detachOwned;
const mergeOwnedAccumulator = owned_merge.mergeOwnedAccumulator;
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
pub const key_decode = @import("scan/key_decode.zig");
const KeyType = key_decode.KeyType;
const keyTypeFromExpr = key_decode.keyTypeFromExpr;
pub const deserializeKeyColumn = key_decode.deserializeKeyColumn;
const deserializeKeyColumnMode = key_decode.deserializeKeyColumnMode;
pub const scan_footers = @import("scan/footers.zig");
const parseFooters = scan_footers.parseFooters;
pub const worker_contracts = @import("scan/worker_contracts.zig");
const Worker = worker_contracts.Worker;
const FailureProbe = worker_contracts.FailureProbe;
const workerErrorRanks = worker_contracts.workerErrorRanks;
const decodeProjectionOrder = worker_contracts.decodeProjectionOrder;
const loadFixture = @import("scan/test_support.zig").loadFixture;
pub const pairs_policy = @import("scan/pairs_policy.zig");
const distinctPairsCall = pairs_policy.distinctPairsCall;
const pairsPay = pairs_policy.pairsPay;
const pairsPayRuntime = pairs_policy.pairsPayRuntime;
pub const scan_worker = @import("scan/worker.zig");
const WORKER_REGION_RESERVE = scan_worker.WORKER_REGION_RESERVE;
const canonicalFailurePrefix = scan_worker.canonicalFailurePrefix;
const firstWorkerFailure = scan_worker.firstWorkerFailure;
const resetAttemptFlags = scan_worker.resetAttemptFlags;
const ownedWinnerBytes = scan_worker.ownedWinnerBytes;
const decodeHeld = scan_worker.decodeHeld;
const workerRefusal = scan_worker.workerRefusal;
const releaseRegions = scan_worker.releaseRegions;
const releaseRegionsParallel = scan_worker.releaseRegionsParallel;
const runWorkers = scan_worker.runWorkers;
pub const merge_index = @import("scan/merge_index.zig");
const MergeIndex = merge_index.MergeIndex;
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
const MergedSlot = scan_count_cut.MergedSlot;
const accCount = scan_count_cut.accCount;
const CountCut = scan_count_cut.CountCut;
const countCut = scan_count_cut.countCut;
const keepCountTop = scan_count_cut.keepCountTop;
const countLimitGroups = scan_count_cut.countLimitGroups;
pub const scan_rank_cut = @import("scan/rank_cut.zig");
const ResultCut = scan_rank_cut.ResultCut;
const resultCutReason = scan_rank_cut.resultCutReason;
const resultCut = scan_rank_cut.resultCut;
const rankValue = scan_rank_cut.rankValue;
const RankHeap = scan_rank_cut.RankHeap;
const pushRank = scan_rank_cut.pushRank;
const CutScratch = scan_rank_cut.CutScratch;
const resultLimitGroups = scan_rank_cut.resultLimitGroups;
const keepResultTop = scan_rank_cut.keepResultTop;
pub const scan_result = @import("scan/result.zig");
pub const ScanState = scan_result.ScanState;
pub const Timings = scan_result.Timings;
pub const MultiAggResult = scan_result.MultiAggResult;
pub const folded_acc = @import("scan/folded_acc.zig");
const foldedAcc = folded_acc.foldedAcc;
const sameAccBits = folded_acc.sameAccBits;
pub const scan_binding = @import("scan/binding.zig");
const KeyNames = scan_binding.KeyNames;
const dependentGroupKeys = scan_binding.dependentGroupKeys;
const nameUnaliasedAggs = scan_binding.nameUnaliasedAggs;
const checkAggAliases = scan_binding.checkAggAliases;
const bindGroupOutputs = scan_binding.bindGroupOutputs;
const checkHavingLanes = scan_binding.checkHavingLanes;
const sqlBound = scan_binding.sqlBound;
const leafSchemaElem = scan_binding.leafSchemaElem;
pub const CutShape = scan_binding.CutShape;
pub const cutShape = scan_binding.cutShape;
pub const checkDistinctOutputNames = scan_binding.checkDistinctOutputNames;
const full_match_fixture = @import("scan/test_support.zig").full_match_fixture;
const loadFullMatchFixture = @import("scan/test_support.zig").loadFullMatchFixture;
pub const scan_plan_impl = @import("scan/plan.zig");
const firstCutEligible = scan_plan_impl.firstCutEligible;
const filterSmall = scan_plan_impl.filterSmall;
const keysLookUnique = scan_plan_impl.keysLookUnique;
const rankingCall = scan_plan_impl.rankingCall;
const sketchEligible = scan_plan_impl.sketchEligible;
const partAggSupported = scan_plan_impl.partAggSupported;
const group_blocks_max = scan_plan_impl.group_blocks_max;
const fewEntries = scan_plan_impl.fewEntries;
const provenPassShare = scan_plan_impl.provenPassShare;
const passShare = scan_plan_impl.passShare;
const selective_share = scan_plan_impl.selective_share;
const few_union = scan_plan_impl.few_union;
const orderSensitive = scan_plan_impl.orderSensitive;
pub const planAggregate = scan_plan_impl.planAggregate;
pub const decidingRule = scan_plan_impl.decidingRule;
const sampledPassShare = scan_plan_impl.sampledPassShare;
const selectedGroupBytes = scan_plan_impl.selectedGroupBytes;
const applyCostRules = scan_plan_impl.applyCostRules;
const wideKey = scan_plan_impl.wideKey;
const few_entries = scan_plan_impl.few_entries;
pub const scan_rows = @import("scan/rows.zig");
const RowSource = scan_rows.RowSource;
const buildGroupRows = scan_rows.buildGroupRows;
const answerBytes = scan_rows.answerBytes;
const answerBytesParallel = scan_rows.answerBytesParallel;
const buildGroupColumns = scan_rows.buildGroupColumns;
pub const parallel_distinct = @import("scan/parallel_distinct.zig");
const parallelDistinctCountU64 = parallel_distinct.parallelDistinctCountU64;
pub const slot_sort = @import("scan/slot_sort.zig");
const sortSlotsByFirst = slot_sort.sortSlotsByFirst;
pub const pair_fold = @import("scan/pair_fold.zig");
const drainDistinctPairs = pair_fold.drainDistinctPairs;
const foldDistinctPairs = pair_fold.foldDistinctPairs;
pub const scan_phase = @import("scan/phase.zig");
const restoreCounts = scan_phase.restoreCounts;
const clearWorkerErrors = scan_phase.clearWorkerErrors;
const runPhase = scan_phase.runPhase;
const phaseFailure = scan_phase.phaseFailure;
pub const scan_census = @import("scan/census.zig");
const streamProbeBytes = scan_census.streamProbeBytes;
const WinnerCharge = scan_census.WinnerCharge;
const denseLimit = scan_census.denseLimit;
const certifiedTopK = scan_census.certifiedTopK;
const Repeat = scan_census.Repeat;
const CutBoundPlan = scan_census.CutBoundPlan;
const certifiedResidual = scan_census.certifiedResidual;
const firstAppearanceCut = scan_census.firstAppearanceCut;
pub const record_workers = @import("scan/record_workers.zig");
const RecordRun = record_workers.RecordRun;
const runRecordWorkers = record_workers.runRecordWorkers;
const pairResult = record_workers.pairResult;
pub const owner_support = @import("scan/owner_support.zig");
const IntegerRangeHint = owner_support.IntegerRangeHint;
const pairRawIdentity = owner_support.pairRawIdentity;
const outputNamesBytes = owner_support.outputNamesBytes;
const ownerIdentity = owner_support.ownerIdentity;
const passThroughSource = owner_support.passThroughSource;
const boolean_answer = owner_support.boolean_answer;
pub const input_rewind = @import("scan/input_rewind.zig");
const rewindInputs = input_rewind.rewindInputs;
pub const owner_integer = @import("scan/owner_integer.zig");
const IntegerCountBounds = owner_integer.IntegerCountBounds;
const inlineIntegerAnswer = owner_integer.inlineIntegerAnswer;
const integerQuote = owner_integer.integerQuote;
pub const owner_pair = @import("scan/owner_pair.zig");
const inlinePairAnswer = owner_pair.inlinePairAnswer;
const countedCalls = owner_pair.countedCalls;
pub const scan_owner_distinct = @import("scan/owner_distinct.zig");
const shortSampleMayFit = scan_owner_distinct.shortSampleMayFit;
const scalarDistinctWideParts = scan_owner_distinct.scalarDistinctWideParts;
const inlineDistinctAnswer = scan_owner_distinct.inlineDistinctAnswer;
pub const owner_bytes = @import("scan/owner_bytes.zig");
const byteSampleMayFit = owner_bytes.byteSampleMayFit;
const msdProbeHeaders = owner_bytes.msdProbeHeaders;
const msdWorkspace = owner_bytes.msdWorkspace;
const inlineMsdAnswer = owner_bytes.inlineMsdAnswer;
const buildMsdTopRows = owner_bytes.buildMsdTopRows;
const inlineByteTopAnswer = owner_bytes.inlineByteTopAnswer;
const buildByteTopRows = owner_bytes.buildByteTopRows;
const inlineByteAnswer = owner_bytes.inlineByteAnswer;
pub const partition_fill = @import("scan/partition_fill.zig");
const fillFromPartitions = partition_fill.fillFromPartitions;
pub const owner_strings = @import("scan/owner_strings.zig");
const inlineSharedStrings = owner_strings.inlineSharedStrings;
pub const owner_packed = @import("scan/owner_packed.zig");
const inlineSelectedCount = owner_packed.inlineSelectedCount;
const inlinePackedAnswer = owner_packed.inlinePackedAnswer;
pub const scan_merge = @import("scan/merge.zig");
const MergeGroupRef = scan_merge.MergeGroupRef;
const mergeCandidates = scan_merge.mergeCandidates;
const hashMergeGroupRefs = scan_merge.hashMergeGroupRefs;
const mergesInPlace = scan_merge.mergesInPlace;
const mergeInPlace = scan_merge.mergeInPlace;
const mergeCountCut = scan_merge.mergeCountCut;
const merge_part_bytes = scan_merge.merge_part_bytes;
const merge_parts_max = scan_merge.merge_parts_max;
const mergePartitionCount = scan_merge.mergePartitionCount;
const mergeEntryBytes = scan_merge.mergeEntryBytes;
const finishGroupRanges = scan_merge.finishGroupRanges;
const freeAccumulatorRanges = scan_merge.freeAccumulatorRanges;
const mergeFoldedRefs = scan_merge.mergeFoldedRefs;
const firstCandidatesParallel = scan_merge.firstCandidatesParallel;
const firstCandidates = scan_merge.firstCandidates;
const premergeLargeDistinctSets = scan_merge.premergeLargeDistinctSets;
const topKCandidates = scan_merge.topKCandidates;
const DistinctMergeTask = scan_merge.DistinctMergeTask;
pub const scan_private_decode = @import("scan/private_decode.zig");
const PrivateDecode = scan_private_decode.PrivateDecode;
pub const scan_execute = @import("scan/execute.zig");
pub const runMultiAggregate = scan_execute.runMultiAggregate;
const Chain = scan_execute.Chain;
const narrowed = scan_execute.narrowed;
pub const explainAggregate = scan_execute.explainAggregate;
const prepareAggregate = scan_execute.prepareAggregate;
const refuseRepeated = scan_execute.refuseRepeated;
pub const integration_support = @import("scan/integration_support.zig");
const SPAWN_FIXTURE_AGG = integration_support.SPAWN_FIXTURE_AGG;
const sameValue = integration_support.sameValue;
const runSketchedAndFree = integration_support.runSketchedAndFree;
const runFirstCutAndFree = integration_support.runFirstCutAndFree;
const runCensusAndFree = integration_support.runCensusAndFree;
const runDistinctCensusAndFree = integration_support.runDistinctCensusAndFree;
const nested_key_fixture = integration_support.nested_key_fixture;
const loadNestedKeyFixture = integration_support.loadNestedKeyFixture;
const freeResult = integration_support.freeResult;
const FullMatchQuery = integration_support.FullMatchQuery;
const runFullMatchQuery = integration_support.runFullMatchQuery;
const expectSameAggValue = integration_support.expectSameAggValue;
const expectSameAnswer = integration_support.expectSameAnswer;
const checkFullMatchQuery = integration_support.checkFullMatchQuery;
const poisonChunks = integration_support.poisonChunks;
const unsigned_order_fixture = integration_support.unsigned_order_fixture;
const CountSumCase = integration_support.CountSumCase;
const expectCountSum = integration_support.expectCountSum;
const deprecated_stats_fixture = integration_support.deprecated_stats_fixture;
const column_order_fixture = integration_support.column_order_fixture;
const nan_stats_fixture = integration_support.nan_stats_fixture;
const Rewrite = integration_support.Rewrite;
const rewritten = integration_support.rewritten;
const PoisonCase = integration_support.PoisonCase;
const expectPoisonCases = integration_support.expectPoisonCases;
const rg1Stats = integration_support.rg1Stats;
const null_pages_fixture = integration_support.null_pages_fixture;
const selectList = integration_support.selectList;
const runAndFree = integration_support.runAndFree;
const NoInPlaceResize = integration_support.NoInPlaceResize;
const expectSameBits = integration_support.expectSameBits;
const setMergeTestString = integration_support.setMergeTestString;
const mergeOverlapAndFree = integration_support.mergeOverlapAndFree;
const fillCountTables = integration_support.fillCountTables;
const countSum = integration_support.countSum;
const fillFixedPair = integration_support.fillFixedPair;
const expectSameMerge = integration_support.expectSameMerge;
const mergeFixedAsBytes = integration_support.mergeFixedAsBytes;
const in_place_calls = integration_support.in_place_calls;
const fillInPlaceTables = integration_support.fillInPlaceTables;
const in_place_outputs = integration_support.in_place_outputs;
const mergeAndBuildRows = integration_support.mergeAndBuildRows;
const byteWindowAllocTrial = integration_support.byteWindowAllocTrial;
const decimal_exact_fixture = integration_support.decimal_exact_fixture;
const DecWant = integration_support.DecWant;
const expectDecimalAnswers = integration_support.expectDecimalAnswers;
const NoGate = integration_support.NoGate;
const outcomes = integration_support.outcomes;
const expectSameOutcomes = integration_support.expectSameOutcomes;
const BitmapTestFixture = integration_support.BitmapTestFixture;
const LivePeak = integration_support.LivePeak;
const checkRecordDecodeModes = integration_support.checkRecordDecodeModes;
const runHotCertificateAndFree = integration_support.runHotCertificateAndFree;
const countResidualBudgetTrial = integration_support.countResidualBudgetTrial;
const ReleasingScanWindow = integration_support.ReleasingScanWindow;
const releasingOrderedTrial = integration_support.releasingOrderedTrial;
const denseProductScan = integration_support.denseProductScan;
const ownedGroupOutputBytes = integration_support.ownedGroupOutputBytes;
const packedAllocationTrial = integration_support.packedAllocationTrial;
const selectedIdentityReplayTrial = integration_support.selectedIdentityReplayTrial;
pub const scan_types = @import("scan/types.zig");
const BoundAggregate = scan_types.BoundAggregate;
pub const MultiAggArgs = scan_types.MultiAggArgs;
pub const SketchTopK = scan_types.SketchTopK;
pub const Error = scan_types.Error;
pub const Input = scan_types.Input;
const allRescannable = scan_types.allRescannable;
pub const FastPath = scan_types.FastPath;
pub const RowGroupGate = scan_types.RowGroupGate;
pub const DictProbe = scan_types.DictProbe;
pub const ReadStart = scan_types.ReadStart;
const BorrowedSpan = scan_types.BorrowedSpan;
pub const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const RecordOrdinals = scan_types.RecordOrdinals;
const ColSource = scan_types.ColSource;
const BoundOutputColumn = scan_types.BoundOutputColumn;
pub const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const row_block = scan_types.row_block;
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
const SortKey = output_values.SortKey;
const orderValues = output_values.orderValues;
const orderSortKeys = output_values.orderSortKeys;
const sortKey = output_values.sortKey;

test "row subgroups decline sparse unknown predicates before walking payload pages" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try row_subgroups.testFixture(a);
    var predicate: std.ArrayList(u8) = .empty;
    for (0..36) |i| try predicate.print(a, "{s}k != {d}", .{ if (i == 0) "" else " AND ", i });
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "sparse", .bytes = fixture.bytes }},
        .metas = &.{fixture.meta},
        .aggregate = "count(*) AS c",
        .group_by = "k,s",
        .filter = predicate.items,
        .parallelism = 1,
        .min_bytes_per_worker = 2 << 10,
        .max_memory = 64 << 20,
        .dense = false,
        .ndv_policy = false,
        .plain_rules = false,
        .spill = false,
        .sketch_topk = null,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    args.parallelism = 8;
    row_subgroups.test_ranges.store(0, .monotonic);
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try testing.expectEqual(@as(usize, 0), row_subgroups.test_ranges.load(.monotonic));
    try testing.expectEqual(want.rows_matched, got.rows_matched);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "row subgroups preserve grouped counts, first ordinals, ties and LIMIT OFFSET under pool failures" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try row_subgroups.testFixture(a);
    const inputs = [_]Input{.{ .name = "subgroups", .bytes = fixture.bytes }};
    var args: MultiAggArgs = .{
        .inputs = &inputs,
        .metas = (&fixture.meta)[0..1],
        .aggregate = "count(*) AS c",
        .group_by = "k,s",
        .filter = "v >= 10 AND v < 4000",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .dense = false,
        .ndv_policy = false,
        .shared_strings = false,
        .integer_records = false,
        .byte_records = false,
        .msd_records = false,
        .plain_rules = false,
        .spill = false,
        .sketch_topk = null,
        .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 13, .offset = 37 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.parallelism = 8;
    const explanation = try explainAggregate(gpa, a, args);
    try testing.expect(explanation.workers > 1);
    try testing.expect(explanation.work_items > fixture.meta.row_groups.items.len);
    for ([_]?usize{ null, 0, 1 }) |fail_after| {
        if (fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        args.parallelism = 8;
        row_subgroups.test_ranges.store(0, .monotonic);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(row_subgroups.test_ranges.load(.monotonic) > 1);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqual(want.rows_in, got.rows_in);
        try testing.expectEqual(want.rows_scanned, got.rows_scanned);
        try testing.expectEqual(want.rows_matched, got.rows_matched);
        try testing.expectEqual(want.row_groups_in, got.row_groups_in);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
    // The producer poisons its chunks at done(): an early release corrupts another range's next page.
    var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, fixture.bytes), .meta = &fixture.meta };
    const streamed = [_]Input{window.input(fixture.bytes)};
    args.inputs = &streamed;
    row_subgroups.test_ranges.store(0, .monotonic);
    var streamed_result = try runMultiAggregate(gpa, a, args);
    defer streamed_result.deinit(gpa);
    try streamed_result.output.applyResultOrder(gpa, args.result_order);
    const ranges = row_subgroups.test_ranges.load(.monotonic);
    try testing.expect(ranges > 1);
    try testing.expectEqual(ranges, window.waits.load(.monotonic));
    try testing.expectEqual(@as(usize, 1), window.releases.load(.monotonic));
    try testing.expectEqualDeep(want.output.group_rows, streamed_result.output.group_rows);
    args.inputs = &inputs;
    const indexed = try a.dupe(schema.RowGroup, fixture.meta.row_groups.items);
    indexed[0].columns.items = try a.dupe(schema.ColumnChunk, indexed[0].columns.items);
    indexed[0].columns.items[2].column_index_offset = 0;
    var indexed_meta = fixture.meta;
    indexed_meta.row_groups.items = indexed;
    args.metas = (&indexed_meta)[0..1];
    const indexed_plan = try explainAggregate(gpa, a, args);
    try testing.expectEqual(@as(usize, 1), indexed_plan.work_items);
    args.metas = (&fixture.meta)[0..1];
    const profiler = try profile.Profiler.create(gpa, .{ .sample_interval_ns = 0 }, .{});
    defer profiler.destroy();
    args.profiler = profiler;
    var profiled = try runMultiAggregate(gpa, a, args);
    defer profiled.deinit(gpa);
    profiler.finish();
    var scans: usize = 0;
    var range_plans: usize = 0;
    var thread = profiler.head.load(.acquire);
    while (thread) |t| : (thread = t.next) {
        var events: profile.EventIterator = .{ .t = t };
        while (events.next()) |batch| for (batch) |event| {
            scans += @intFromBool(event.kind == .plan);
            range_plans += @intFromBool(event.kind == .plan_ranges);
        };
    }
    try testing.expectEqual(@as(usize, 1), scans);
    try testing.expectEqual(@as(usize, 1), range_plans);
}

// ============================================================
// Compile-time API check. Behavioral coverage is via the CLI
// regression suite + lambda integration tests.
// ============================================================
test "scan: API is well-typed" {
    _ = runMultiAggregate;
    _ = materializeOne;
}

// ============================================================ Partial thread-spawn failure.
//
// `Thread.spawn` can fail partway through a loop (EAGAIN); leftover work must still run and started threads must still
// be joined. Driven through the seam in spawn.zig, since real spawn failure is not reproducible.
// ============================================================

const testing = std.testing;
const test_fixtures = @import("parquet/test_fixtures.zig");

test "runMultiAggregate: injected spawn failures don't change the answer" {
    // The scan workers, unlike the footer parsers, are the loop whose old `try spawn` returned while earlier threads
    // still read the arena the caller frees. Sizing is by decode volume, so a small fixture would test nothing.
    const fixture = "data/bench_types.parquet";
    const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);

    const parallelism = 4;
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};

    const Answer = struct { sum: i128, rows: i64 };
    var baseline: ?Answer = null;
    for ([_]?usize{ null, 0, 1, parallelism - 1 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();

        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(id) AS s",
            .parallelism = parallelism,
            // Force a real decode; otherwise the sum is answered from statistics and no worker ever runs.
            .scan_all = true,
        });
        defer freeResult(res);

        try testing.expectEqual(@as(usize, 1), res.output.aggs.len);
        const got: Answer = .{ .sum = res.output.aggs[0].value.i, .rows = res.rows_in };
        // Work dropped by a failed spawn and never picked up inline would show as a short row count.
        try testing.expect(got.rows > 0);

        if (baseline) |b| {
            try testing.expectEqual(b.sum, got.sum);
            try testing.expectEqual(b.rows, got.rows);
            // Prove the run took the failure path; one worker means nothing was tested.
            if (spawn_util.failuresInjected() == 0) {
                std.debug.print(
                    "spawn injection never fired (fail_after={?d}) — test is vacuous\n",
                    .{fail_after},
                );
                return error.TestUnexpectedResult;
            }
        } else {
            baseline = got;
        }
    }
}

test "partagg: same GROUP BY answer as the per-worker tables at every -j" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    const Q = struct {
        aggregate: []const u8,
        group_by: []const u8,
        /// False: the query must stay on the per-worker tables (float merge, or count distinct).
        part: bool,
        cap: usize = 0,
        probe: u64 = 0,
    };
    const queries = [_]Q{
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm, min(ts) AS mn, max(ts) AS mx, avg(ts) AS a", .group_by = "b", .part = true },
        .{ .aggregate = "count(*) AS c, min(s) AS mn, max(s) AS mx", .group_by = "b", .part = true, .cap = 40, .probe = 80 },
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm", .group_by = "s", .part = true, .cap = 30, .probe = 50 },
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm", .group_by = "n", .part = true, .cap = 40, .probe = 80 },
        .{ .aggregate = "sum(x) AS sx", .group_by = "b", .part = false },
        .{ .aggregate = "count(DISTINCT ts) AS d", .group_by = "b", .part = false },
    };
    for (queries) |q| for ([_]usize{ 1, 3, 8 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = q.aggregate,
            .group_by = q.group_by,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .scan_all = true,
        };
        const want = try runMultiAggregate(testing.allocator, arena_state.allocator(), base);
        defer freeResult(want);
        var args = base;
        args.partagg = true;
        args.part_cap = q.cap;
        args.part_probe = q.probe;
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        try testing.expectEqual(q.part, got.part_agg);
        expectSameAnswer(want, got) catch |err| {
            std.debug.print("partagg -j{d} {s} GROUP BY {s}\n", .{ j, q.aggregate, q.group_by });
            return err;
        };
    };
}

test "partagg: shared partitions fit the small GROUP BY budget" {
    // Round 6 repro: one worker plus 32 shared tables. Worker-only block sizing strands quota during the flush.
    const path = "ci/fixtures/parquet/partagg_budget.parquet";
    const bytes = try metadata.readFileSlice(path, testing.allocator);
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const res = try runMultiAggregate(testing.allocator, arena.allocator(), .{
        .inputs = &inputs,
        .group_by = "k",
        .aggregate = "count(*) AS c",
        .partagg = true,
        .parallelism = 1,
        .max_memory = 256000,
    });
    defer freeResult(res);
    try testing.expect(res.part_agg);
    const rows = res.output.group_rows.?;
    try testing.expectEqual(@as(usize, 1000), rows.len);
    for (rows, 0..) |row, i| {
        try testing.expectEqual(@as(i64, @intCast(i)), row[0].i);
        try testing.expectEqual(@as(i64, 1), row[1].i);
    }
}

test "partagg: a spawn failure does not change the answer" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    // The baseline's accumulators borrow its arena, so that arena outlives the comparisons.
    var base_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer base_arena.deinit();
    const baseline = try runMultiAggregate(testing.allocator, base_arena.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(ts) AS sm, min(s) AS mn",
        .group_by = "n",
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .partagg = true,
        .part_cap = 40,
        .part_probe = 80,
        .scan_all = true,
    });
    defer freeResult(baseline);
    try testing.expect(baseline.part_agg);
    for ([_]usize{ 0, 1, 3 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(ts) AS sm, min(s) AS mn",
            .group_by = "n",
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .partagg = true,
            .part_cap = 40,
            .part_probe = 80,
            .scan_all = true,
        });
        defer freeResult(got);
        expectSameAnswer(baseline, got) catch |err| {
            std.debug.print("partagg spawn fail_after={d}\n", .{fail_after});
            return err;
        };
        if (spawn_util.failuresInjected() == 0) {
            std.debug.print("partagg spawn injection never fired (fail_after={d})\n", .{fail_after});
            return error.TestUnexpectedResult;
        }
    }
}

test "runMultiAggregate: skewed work does not let idle workers strand the budget" {
    // Blocks sized as a fraction of the whole budget rather than of each worker's share let fifteen barely-grouping
    // workers strand most of the pool at -j16. Admission cannot depend on worker count when live state fits the budget.
    const heavy = "ci/fixtures/parquet/spawn_budget_a.parquet";
    const light = "ci/fixtures/parquet/spawn_budget_b.parquet";
    var bytes: [2][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for ([_][]const u8{ heavy, light }, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }

    var inputs: [16]Input = undefined;
    inputs[0] = .{ .name = heavy, .bytes = bytes[0] };
    for (inputs[1..]) |*in| in.* = .{ .name = light, .bytes = bytes[1] };

    // The fixed budget includes modest headroom for the surviving groups and must be enough at any -j.
    for ([_]usize{ 1, 4, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "group_key < 1001",
            .aggregate = SPAWN_FIXTURE_AGG,
            .group_by = "group_key",
            .parallelism = parallelism,
            .max_memory = 710000,
            // Per-worker tables: ordered float results need room for a block's partials too (the next test).
            .unordered = true,
        }) catch |err| {
            std.debug.print(
                "-j{d}: skewed work rejected a query with 10% headroom: {s}\n",
                .{ parallelism, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);
        try testing.expectEqual(@as(usize, 1001), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
    }
}

test "runMultiAggregate: --max-memory means the same thing at every -j" {
    // One integer group in the fixed table: 16 slots, the 9-byte key, and the accumulator and first arrays at
    // their first growth step. That footprint has to be enough at -j1 and, because the tables share one budget,
    // at every larger -j too.
    const one_group = 1361;
    const files = [_][]const u8{
        "ci/fixtures/parquet/spawn_budget_a.parquet",
        "ci/fixtures/parquet/spawn_budget_b.parquet",
    };
    var bytes: [files.len][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for (files, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }
    var inputs: [files.len]Input = undefined;
    for (files, 0..) |f, i| inputs[i] = .{ .name = f, .bytes = bytes[i] };

    for ([_]usize{ 1, 2, 4 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "group_key = 0",
            .aggregate = SPAWN_FIXTURE_AGG,
            .group_by = "group_key",
            .parallelism = parallelism,
            .max_memory = one_group,
            .unordered = true,
        }) catch |err| {
            std.debug.print(
                "-j{d}: a GROUP BY that fits {d} bytes at -j1 was rejected: {s}\n",
                .{ parallelism, one_group, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);
        try testing.expectEqual(@as(usize, 1), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
    }
    // Ordered float sums keep a block's partials beside the merged sums: twice one group's room is enough at every
    // -j. Integer results merge in any order, so they need no more than unordered (the min/max query below).
    for ([_]usize{ 1, 2, 4 }) |parallelism| {
        for ([_]struct { agg: []const u8, budget: usize }{
            .{ .agg = SPAWN_FIXTURE_AGG, .budget = one_group * 2 },
            .{ .agg = "count(*) AS n, min(group_key) AS lo, max(group_key) AS hi", .budget = 800 },
        }) |c| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = "group_key = 0",
                .aggregate = c.agg,
                .group_by = "group_key",
                .parallelism = parallelism,
                .max_memory = c.budget,
            }) catch |err| {
                std.debug.print("-j{d}: ordered {s} rejected at {d} bytes: {s}\n", .{
                    parallelism, c.agg[0..@min(c.agg.len, 24)], c.budget, @errorName(err),
                });
                return err;
            };
            defer freeResult(res);
            try testing.expectEqual(@as(usize, 1), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
        }
    }
}



test "runMultiAggregate: a COUNT-descending cut merges only groups that can make it, and answers the same" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Cut = struct { order: expr_ast.ResultOrder, prunes: bool };
    const cuts = [_]Cut{
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 }, .prunes = true },
        .{ .order = .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = true } }, .limit = 3, .offset = 5 }, .prunes = true },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 1 }, .prunes = true },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 0 }, .prunes = true },
        // No ORDER BY: the first groups to appear.
        .{ .order = .{ .limit = 10 }, .prunes = true },
        .{ .order = .{ .limit = 4, .offset = 7 }, .prunes = true },
        // Not a COUNT-descending cut: merged in full.
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }} }, .prunes = false },
    };
    // `v` is unique per row: every count ties at 1, the threshold cannot prune, and the cut falls to the merge's
    // per-partition count heap (or the one-table heap), which must break the ties as the full sort does.
    for ([_][]const u8{ "k", "n", "v" }) |key| {
        for ([_]usize{ 1, 2, 4, 8 }) |parallelism| {
            for (cuts) |cut| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const select_cols = try selectList(arena, &.{ key, "count(*) AS c", "min(v) AS lo", "max(v) AS hi" });
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = "count(*) AS c, min(v) AS lo, max(v) AS hi",
                    .group_by = key,
                    .select_cols = select_cols,
                    .parallelism = parallelism,
                };
                var whole = try runMultiAggregate(testing.allocator, arena, args);
                defer whole.deinit(testing.allocator);
                args.result_order = cut.order;
                var cut_res = try runMultiAggregate(testing.allocator, arena, args);
                defer cut_res.deinit(testing.allocator);

                const all_groups = whole.output.group_rows.?.len;
                const merged = cut_res.output.group_rows.?.len;
                // On unique keys only a cut by the COUNT alone shrinks the merge: a second key needs every tie.
                const prunes = cut.prunes and !(std.mem.eql(u8, key, "v") and cut.order.keys.len > 1);
                if (prunes) try testing.expect(merged < all_groups / 10) else try testing.expectEqual(all_groups, merged);
                try whole.output.applyResultOrder(testing.allocator, cut.order);
                try cut_res.output.applyResultOrder(testing.allocator, cut.order);
                const want = whole.output.group_rows.?;
                const got = cut_res.output.group_rows.?;
                try testing.expectEqual(want.len, got.len);
                for (want, got) |w, g| for (w, g) |a, b| try testing.expect(sameValue(a, b));
            }
        }
    }
}

test "runMultiAggregate: a sketched COUNT cut answers as the full scan, certified or not" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
        // Ties at the cut broken by the key, past an offset.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 4, .offset = 2 },
        // Not a COUNT-descending cut: never sketched.
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 3 },
    };
    const configs = [_]SketchTopK{
        .{},
        // Few cells: collisions inflate the bound.
        .{ .cell_bits = 8, .admit_per_row = 1 },
        .{ .certify = false },
        // When the cells bail: the census riding along the count pass, then dropped early by a worker's cells, then
        // counting in a pass of its own, and none.
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = true },
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = true, .census_check_rows = 64 },
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = false },
        .{ .cell_bits = 8, .admit_per_row = 1, .census = false },
    };
    var seen = std.EnumSet(Outcome){};
    // `k` is skewed and has nulls, `n` is a fixed-width key, `v` is unique (every count ties at 1).
    for ([_][]const u8{ "k", "n", "v", "k, n" }) |key| for ([_]?[]const u8{ null, "n < 12" }) |filter| {
        for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const first_key = key[0 .. std.mem.indexOfScalar(u8, key, ',') orelse key.len];
            const select_cols = try selectList(arena, &.{ first_key, "count(*) AS c", "min(v) AS lo" });
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .filter = filter,
                .aggregate = "count(*) AS c, min(v) AS lo",
                .group_by = key,
                .select_cols = if (std.mem.eql(u8, key, first_key)) select_cols else null,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_scanned, got.rows_scanned);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .ineligible, .bailed, .certified, .fell_back, .census }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a census cut answers as the full scan, certified or not" {
    // Near-unique keys in shuffled order (tools/zpq-gen census): the cells bail, and the census decides.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        // `u` and `s` tie at 1 inside the cut, `t` at 3.
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        // Ties broken by the key, which first appearance does not follow.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 30 },
    };
    const configs = [_]SketchTopK{
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        // Colliding fingerprints inflate every total: the census must not trust a cut they make up.
        .{ .fuse_census = true, .census_fp_bits = 3 },
        .{ .fuse_census = false, .census_fp_bits = 0 },
        .{ .census = false },
        // Each partition keeps only k fingerprints: the census must certify against the largest count it dropped, or
        // refuse, and collisions on top only make it refuse more.
        .{ .fuse_census = false, .admit_per_row = 1 },
        .{ .fuse_census = true, .admit_per_row = 1, .census_fp_bits = 6 },
    };
    var seen = std.EnumSet(Outcome){};
    // A count of a column, or under FILTER, can give the first keys to appear 0 (`n` is NULL through the first row
    // group): the census counts rows, an upper bound only.
    const counts = [_][]const u8{ "count(*) AS c", "count(n) AS c", "count(*) FILTER (WHERE n = 1) AS c" };
    for ([_][]const u8{ "u", "t", "s" }) |key| for ([_]?[]const u8{ null, "u > 300000000000" }) |filter| for (counts) |count| {
        for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const select_cols = try selectList(arena, &.{ key, count, "min(s) AS lo", "sum(t) AS st" });
            const aggregate = try std.fmt.allocPrint(arena, "{s}, min(s) AS lo, sum(t) AS st", .{count});
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .filter = if (filter != null and std.mem.eql(u8, key, "u")) filter else null,
                .aggregate = aggregate,
                .group_by = key,
                .select_cols = select_cols,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_scanned, got.rows_scanned);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .bailed, .fell_back, .census }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a count(DISTINCT) census cut answers as the full scan, certified or not" {
    // tools/zpq-gen census. By `t` four keys hold five rows and twelve hold three, so a cut of 10 ties at 3 and
    // each key's distinct `u` is its rows; by `u` three keys hold two rows, so every distinct count ties at the
    // census bound and nothing certifies; `s` is `u` as a string, hashed whole; `n` is NULL through the first row
    // group, so keys read only there count 0 distinct.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        // Ties broken by the key, which first appearance does not follow.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 30 },
    };
    const configs = [_]SketchTopK{
        .{},
        // Ten fingerprints admitted tie at the bound; four times as many certify.
        .{ .admit_per_row = 1 },
        // Colliding fingerprints inflate every bound, and with none left the admitted rows are most of the input.
        .{ .census_fp_bits = 3 },
        .{ .census_fp_bits = 0 },
        // Workers judge after 64 rows that the fullest cells hold too much, and abandon mid row group.
        .{ .census_check_rows = 64 },
        .{ .certify = false },
        .{ .census = false },
    };
    var seen = std.EnumSet(Outcome){};
    const distincts = [_][]const u8{ "count(DISTINCT u) AS d", "count(DISTINCT n) AS d", "count(DISTINCT t) AS d" };
    for ([_][]const u8{ "t", "u", "s" }) |key| for (distincts) |distinct| {
        if (std.mem.eql(u8, key, "t") == std.mem.eql(u8, distinct, "count(DISTINCT t) AS d")) continue;
        for ([_]usize{ 1, 3, 16 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const select_cols = try selectList(arena, &.{ key, distinct, "count(*) AS c", "min(s) AS lo" });
            const aggregate = try std.fmt.allocPrint(arena, "{s}, count(*) AS c, min(s) AS lo", .{distinct});
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .aggregate = aggregate,
                .group_by = key,
                .select_cols = select_cols,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .census, .fell_back, .bailed, .ineligible }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a count(DISTINCT) census never drops the error a computed argument raises" {
    // u = 368403641522 is the first row's, under a key of `t` seen once: a key the cut leaves out.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const aggregates = [_][]const u8{
        "count(DISTINCT 100 / (u - 368403641522)) AS d",
        "count(DISTINCT u) AS d, sum(100 / (u - 368403641522)) AS s",
    };
    for (aggregates) |aggregate| for ([_]usize{ 1, 3, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = aggregate,
            .group_by = "t",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
            .sketch_topk = .{},
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expect(!ex.plan.dispatch.distinct_census);
        try testing.expectError(error.DivisionByZero, runMultiAggregate(testing.allocator, arena, args));
    };
}

test "runMultiAggregate: count and first-keys cuts never drop the error a computed argument raises" {
    // u = 573290335422 is the last row's, under a key of `t` seen once: neither a key the count cut admits nor one in
    // the first keys' prefix.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const Case = struct { aggregate: []const u8, order: expr_ast.ResultOrder };
    const cases = [_]Case{
        .{ .aggregate = "count(*) AS c, sum(100 / (u - 573290335422)) AS s", .order = top },
        .{ .aggregate = "sum(100 / (u - 573290335422)) AS s", .order = .{ .limit = 3 } },
    };
    for (cases) |case| for ([_]?bool{ true, false }) |fuse| for ([_]usize{ 1, 3, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = case.aggregate,
            .group_by = "t",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
            .result_order = case.order,
            .sketch_topk = .{ .fuse_census = fuse },
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expectEqual(.plain_hash, ex.plan.dispatch.strategy);
        try testing.expectError(error.DivisionByZero, runMultiAggregate(testing.allocator, arena, args));
    };
}

test "planAggregate: a count(DISTINCT) census needs a LIMIT HAVING leaves, a census that fits and a large domain" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const having: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "1" } } };
    var with_having = top;
    with_having.having = &having;
    const Case = struct { order: expr_ast.ResultOrder, max_memory: usize = 512 << 20, census: bool };
    const cases = [_]Case{
        .{ .order = top, .census = true },
        // The engine hands the scan HAVING's cut order (`ResultProgram.cutOrder`): no limit, so no cut.
        .{ .order = @import("result_program.zig").ResultProgram.cutOrder(with_having), .census = false },
        // A census of every row must fit a quarter of the budget, or the scan runs as usual.
        .{ .order = top, .max_memory = 40 << 10, .census = false },
    };
    for (cases) |case| {
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d",
            .group_by = "t",
            .select_cols = try selectList(arena, &.{ "t", "count(DISTINCT u) AS d" }),
            .result_order = case.order,
            .max_memory = case.max_memory,
            .sketch_topk = .{},
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expectEqual(case.census, ex.plan.dispatch.distinct_census);
        try testing.expectEqual(case.census, ex.plan.reasons.contains(.census_distinct));
    }
    // A proven domain too small for the census to leave most rows out keeps the plain scan.
    const plan: ScanPlan = .{ .dispatch = .{
        .strategy = .sketch,
        .use_blocks = false,
        .tables = 4,
        .count_columns = true,
        .sketch_fits = true,
        .keys_unique = false,
        .filter_small = true,
        .count_agg = 0,
        .census_allowed = true,
        .census_fused = true,
        .distinct_census = true,
    } };
    const calls = [_]expr_agg.AggCall{.{ .func = .count_distinct, .alias = "d", .result = .i64, .arg = null, .where = null }};
    const in: PlanInputs = .{
        .args = .{ .inputs = &inputs, .aggregate = "", .result_order = top, .sketch_topk = .{} },
        .metas = &.{},
        .work = &.{},
        .calls = &calls,
        .keys = &.{},
        .outputs = null,
        .filter_cols = &.{},
        .workers = 4,
    };
    for ([_]u64{ 10_239, 10_240 }, [_]bool{ false, true }) |hi, kept| {
        var p = plan;
        applyCostRules(in, .{ .source = .dictionary_union, .lo = 0, .hi = hi, .rows = 1 << 24, .covered_rows = 1 << 24, .confidence = .upper_bound }, &p);
        try testing.expectEqual(kept, p.dispatch.distinct_census);
        try testing.expectEqual(kept, p.dispatch.strategy == .sketch);
    }
}

test "runMultiAggregate: behind heavy keys a census cut certifies on a short prefix, and answers as the full scan" {
    // tools/zpq-gen census: three keys hold 4500 of 8000 rows, the rest are unique, so a cut of 10 ties at 1.
    const path = "ci/fixtures/parquet/census_heavy.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    var seen = std.EnumSet(Outcome){};
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4, .offset = 3 },
        // More than the short prefix can hold: it must widen to the sure one, not certify short.
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 400 },
    };
    for ([_][]const u8{ "h", "g" }) |key| for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| {
        for ([_]bool{ true, false }) |fuse| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .aggregate = "count(*) AS c, min(v) AS lo, sum(v) AS sv",
                .group_by = key,
                .select_cols = try selectList(arena, &.{ key, "count(*) AS c", "min(v) AS lo", "sum(v) AS sv" }),
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            args.sketch_topk = .{ .fuse_census = fuse, .outcome = &outcome };
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        }
    };
    try testing.expect(seen.contains(.census_short));
    try testing.expect(seen.contains(.census));
}



test "runMultiAggregate: a sketched COUNT cut frees everything on any allocation failure" {
    // One row group, string keys that own memory, one past the 16 bytes an unframed key needs.
    const path = "ci/fixtures/parquet/json_escape.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    for ([_]bool{ true, false }) |certify| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runSketchedAndFree, .{ &inputs, &meta, certify });
    }
}

test "runMultiAggregate: a LIMIT with no ORDER BY answers as the full scan through the first-keys cut" {
    for ([_][]const u8{ "ci/fixtures/parquet/topk_skew.parquet", "ci/fixtures/parquet/census_topk.parquet" }) |path| {
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        const census = std.mem.indexOf(u8, path, "census") != null;
        const keys: []const []const u8 = if (census) &.{ "u", "t", "s", "t, u" } else &.{ "k", "n", "v", "k, n" };
        const filter: []const u8 = if (census) "u > 300000000000" else "n < 12";
        for (keys) |key| for ([_]?[]const u8{ null, filter }) |f| for ([_]usize{ 1, 3 }) |parallelism| {
            // Within one row group, past it (the discovery doubles), past every key, and none.
            for ([_]expr_ast.ResultOrder{
                .{ .limit = 10 },
                .{ .limit = 4, .offset = 7 },
                .{ .limit = 1500 },
                .{ .limit = 30000 },
                .{ .limit = 0 },
            }) |cut| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const agg = if (census) "count(*) AS c, min(s) AS lo, sum(t) AS st" else "count(*) AS c, min(v) AS lo";
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .filter = f,
                    .aggregate = agg,
                    .group_by = key,
                    .parallelism = parallelism,
                    .min_bytes_per_worker = 1,
                    .result_order = cut,
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.sketch_topk = .{};
                var got = try runMultiAggregate(testing.allocator, arena, args);
                defer got.deinit(testing.allocator);
                try testing.expectEqual(want.rows_in, got.rows_in);
                try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                try testing.expectEqual(want.rows_matched, got.rows_matched);
                try want.output.applyResultOrder(testing.allocator, cut);
                try got.output.applyResultOrder(testing.allocator, cut);
                const w = want.output.group_rows.?;
                const g = got.output.group_rows.?;
                try testing.expectEqual(w.len, g.len);
                for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
            }
        };
    }
}



test "runMultiAggregate: a first-keys cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    // Three row groups of key `n`: NULL through the first, 1 after. A cut of 1 ends inside the first; one of 2 makes
    // the discovery read two, then the admit pass reads the third; one of 3 has the discovery read everything.
    meta.row_groups.items.len = 3;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    for ([_]u64{ 1, 2, 3 }) |limit| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runFirstCutAndFree, .{ &inputs, &meta, limit });
    }
}





test "runMultiAggregate: a count(DISTINCT) census cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    // Certified, widened once, abandoned mid row group, and falling back after the admit passes.
    for ([_]SketchTopK{ .{}, .{ .admit_per_row = 1 }, .{ .census_check_rows = 64 }, .{ .certify = false } }) |cfg| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runDistinctCensusAndFree, .{ &inputs, &meta, cfg });
    }
}

test "runMultiAggregate: a census cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    // The first row group alone: every failure point runs the query again.
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    // Riding along the count pass and dropped early, riding along, alone, and falling back after the admit pass.
    for ([_]SketchTopK{
        .{ .fuse_census = true, .census_check_rows = 64 },
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        .{ .fuse_census = false, .certify = false },
    }) |cfg| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runCensusAndFree, .{ &inputs, &meta, cfg });
    }
}

test "runMultiAggregate: a GROUP BY that fits its budget still fits when spawns fail" {
    // A spawn failure must not turn a query that fits `max_memory` into ExceededMemoryBudget. Reaching that path
    // needs four things at once:
    //
    //  1. GROUP BY, or there is no group table and no budget to get wrong.
    //  2. n_workers >= 2, hence twelve fetched columns to clear the 2 MiB-per-worker sizing threshold.
    //  3. Group keys disjoint per work item, so one worker holding the union really needs ~2x; shared keys hide it.
    //  4. A budget that fits only when the fallback worker gets the full shared allowance, not one worker's share.
    //
    // Metadata is passed in so every injected failure is necessarily a scan-worker spawn; otherwise footer-parse spawns
    // satisfy the "injection fired" check while the scan loop quietly runs single-threaded.
    //
    // Tracked fixtures, not data/ (gitignored, so a test pointed there would skip in a clean clone): regenerate with
    // tools/zpq-gen spawn_budget.
    const files = [_][]const u8{
        "ci/fixtures/parquet/spawn_budget_a.parquet",
        "ci/fixtures/parquet/spawn_budget_b.parquet",
    };
    const group_by = "group_key";
    const aggregate = SPAWN_FIXTURE_AGG;
    // A finished block stays live while it folds into the merge. Two thousand groups, with that overlap, sit a
    // little over 2 MiB once keys are a slot array instead of a per-entry charge.
    const budget = 2300 * 1024;
    const parallelism = 2;

    var bytes: [files.len][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for (files, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }

    var inputs: [files.len]Input = undefined;
    for (files, 0..) |f, i| inputs[i] = .{ .name = f, .bytes = bytes[i] };

    var baseline_groups: ?usize = null;
    for ([_]?usize{ null, 0, 1 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var metas: [files.len]schema.FileMetaData = undefined;
        for (bytes[0..], 0..) |b, i| metas[i] = try metadata.open(arena, b);

        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();

        const res = runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = aggregate,
            .group_by = group_by,
            .parallelism = parallelism,
            .max_memory = budget,
        }) catch |err| {
            std.debug.print(
                "fail_after={?d}: a GROUP BY that fits {d} KiB was rejected: {s}\n",
                .{ fail_after, budget / 1024, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);

        const groups = (res.output.group_rows orelse return error.TestUnexpectedResult).len;
        if (baseline_groups) |b| {
            try testing.expectEqual(b, groups);
            // Metadata was supplied, so this can only be a scan-worker spawn.
            if (spawn_util.failuresInjected() == 0) {
                std.debug.print(
                    "fail_after={?d}: no scan-worker spawn was refused — test is vacuous\n",
                    .{fail_after},
                );
                return error.TestUnexpectedResult;
            }
        } else {
            baseline_groups = groups;
        }
    }
}

// ============================================================ Group-key framing.
//
// Leaf-indexed framing read the wrong schema element for every column after the first nested one (see
// `leafLabel`). The fixture puts a MAP ahead of ordinary scalars to reproduce that shape.
// ============================================================







test "group key: a scalar column after a MAP is framed by its own type" {
    // `ts` is INT64 but leaf-indexed lookup resolved a BYTE_ARRAY element, so deserialization read the timestamp bytes
    // as a string length: "index out of bounds: index 2690342917, len 9".
    const bytes = (try loadNestedKeyFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    const inputs = [_]Input{.{ .name = nested_key_fixture, .bytes = bytes }};
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "ts",
        .parallelism = 1,
    });
    defer freeResult(res);

    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 5), rows.len);
    var total: i128 = 0;
    for (rows) |r| total += r[1].i;
    try testing.expectEqual(@as(i128, 60), total);
    for (rows) |r| try testing.expect(r[0] == .i);

    // The output column name also depends on resolving the right leaf.
    const cols = res.output.group_cols orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("ts", cols[0]);
}

test "group key: a BOOLEAN first column does not truncate the rest" {
    // BOOLEAN evaluates to the i64 lane (8 bytes) but the physical type said `boolean` and deserialization consumed 1,
    // so every later key column was read from the wrong offset: `name` came back null while counts looked fine.
    const bytes = (try loadNestedKeyFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    const inputs = [_]Input{.{ .name = nested_key_fixture, .bytes = bytes }};
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "flag, name",
        .parallelism = 1,
    });
    defer freeResult(res);

    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    // 2 flags x 4 names, and every name must survive.
    try testing.expectEqual(@as(usize, 8), rows.len);
    for (rows) |r| {
        try testing.expect(r[0] == .i);
        try testing.expect(r[1] == .s);
        try testing.expect(std.mem.startsWith(u8, r[1].s, "name"));
    }
}

// ============================================================ Full-match row groups.
//
// Where statistics prove every row of a row group passes the filter, the scan neither evaluates the filter there nor
// fetches/decodes columns only the filter reads. Every answer is checked against `--scan-all`, which disables all
// statistics use and evaluates the filter on every row, so the two paths share nothing but the decoders and folds.
// Fixture layout (tools/zpq-gen full_match): 8 row groups x 200 rows, `ts` = 0..1599 sorted, so `ts >= 500`
// skips row groups 0-1, leaves 2 partial and proves 3-7.
// ============================================================











test "min-fill: a column the filter also reads under an OR decodes the pages its conjunct is proven on" {
    // ci/fixtures/parquet/min_fill_or.parquet (tools/zpq-gen min_fill): c alternates 1, 2 with no nulls, in
    // pages of 50 with a page index; b is 1 on every seventh row. Each page proves `c IS NOT NULL` (and, trusting
    // statistics, `c >= 0`); standing the page minimum in for c would drop every c = 2 row the OR keeps.
    const path = "ci/fixtures/parquet/min_fill_or.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Case = struct { filter: []const u8, trust: bool, n: i128, sx: i128 };
    const cases = [_]Case{
        .{ .filter = "c IS NOT NULL AND (c = 2 OR b = 1)", .trust = false, .n = 572, .sx = 285784 },
        .{ .filter = "c >= 0 AND (c = 2 OR b = 1)", .trust = true, .n = 572, .sx = 285784 },
        .{ .filter = "(c = 2 OR b = 1) AND c IS NOT NULL", .trust = true, .n = 572, .sx = 285784 },
        // A column only mandatory conjuncts read may still be filled: the answer is the same either way.
        .{ .filter = "c IS NOT NULL AND b = 1", .trust = false, .n = 143, .sx = 71071 },
    };
    for (cases) |case| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var r = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = case.filter,
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .parallelism = j,
            .trust_stats = case.trust,
            .min_bytes_per_worker = 1,
        });
        defer r.deinit(testing.allocator);
        testing.expectEqual(case.n, r.output.aggs[0].value.i) catch |err| {
            std.debug.print("min-fill: {s} at -j {d}\n", .{ case.filter, j });
            return err;
        };
        try testing.expectEqual(case.sx, r.output.aggs[1].value.i);
    };
}

test "SQL GROUP BY binds SELECT items to keys as bound expressions, not text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    const Col = expr_ast.SelectColumn;
    const Case = struct { group_by: []const u8, select: []const Col, names: []const []const u8, groups: usize };
    const cases = [_]Case{
        // An unaliased expression key, named as written (ClickBench Q35's shape).
        .{
            .group_by = "b, b - 1",
            .select = &.{ .{ .expr = "b - 1", .role = .key }, .{ .expr = "\"b\"", .role = .key }, .{ .expr = "count(*)", .role = .agg } },
            .names = &.{ "b - 1", "b", "count(*)" },
            .groups = 16,
        },
        // A key the SELECT list names by its alias (`SELECT b * 2 AS k ... GROUP BY k`), and a constant key.
        .{
            .group_by = "k, 1",
            .select = &.{ .{ .expr = "1", .role = .key }, .{ .expr = "b * 2", .alias = "k", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } },
            .names = &.{ "1", "k", "n" },
            .groups = 16,
        },
    };
    for (cases) |case| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var r = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*)",
            .group_by = case.group_by,
            .select_cols = case.select,
        });
        defer r.deinit(testing.allocator);
        const cols = r.output.group_cols.?;
        try testing.expectEqual(case.names.len, cols.len);
        for (case.names, cols) |want, got| try testing.expectEqualStrings(want, got);
        try testing.expectEqual(case.groups, r.output.group_rows.?.len);
        var total: i128 = 0;
        for (r.output.group_rows.?) |row| total += row[2].i;
        try testing.expectEqual(@as(i128, 1600), total);
    }
    // `b + 1` is no key of `GROUP BY b - 1`, however alike the text.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expectError(error.ColumnMustBeGrouped, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*)",
        .group_by = "b - 1",
        .select_cols = &.{ .{ .expr = "b + 1", .role = .key }, .{ .expr = "count(*)", .role = .agg } },
    }));
}

test "full match: proven row groups answer like --scan-all" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const queries = [_]FullMatchQuery{
        // count(*) alone: proven row groups need no column at all.
        .{ .filter = "ts >= 500", .aggregate = "count(*) AS n", .full_match = 5 },
        .{ .filter = "ts >= 500", .aggregate = "count(*) AS n, sum(x) AS sx, min(x) AS mn", .full_match = 5 },
        // The filter column is also an aggregate argument / per-agg FILTER input, so it must still be decoded.
        .{ .filter = "ts >= 500", .aggregate = "sum(ts) AS st, max(ts) AS mt, count(*) AS n", .full_match = 5 },
        .{
            .filter = "ts >= 500",
            .aggregate = "count(*) AS n, sum(x) FILTER (WHERE ts < 1000) AS sx",
            .full_match = 5,
        },
        .{ .filter = "ts >= 500", .aggregate = "avg(x) AS a, count(x) AS cx", .full_match = 5 },
        // Strings: a min-side proof needs no exactness flag; a max-side one relies on is_max_value_exact.
        .{ .filter = "s >= 'k000500'", .aggregate = "count(*) AS n, sum(x) AS sx", .full_match = 5 },
        .{ .filter = "s < 'k000500'", .aggregate = "count(*) AS n, sum(x) AS sx", .full_match = 2 },
        // Nullable: group 1 is all null (skip), 0/2/3 have nulls (partial), 4-7 have none (proven).
        .{ .filter = "n IS NOT NULL", .aggregate = "count(*) AS c, sum(x) AS sx, count(n) AS cn", .full_match = 4 },
        .{ .filter = "n IS NULL", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 1 },
        .{ .filter = "n >= 0", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 4 },
        .{ .filter = "ts >= 500 AND n IS NOT NULL", .aggregate = "count(*) AS c, sum(n) AS sn", .full_match = 4 },
        .{ .filter = "ts >= 1400 OR x > 1000", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 1 },
        .{ .filter = "NOT ts < 500", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 5 },
        // Floats: NaN could hide outside [min, max].
        .{ .filter = "x >= -3", .aggregate = "count(*) AS n", .full_match = 0 },
    };
    for (queries) |q| {
        const res = try checkFullMatchQuery(&inputs, q);
        freeResult(res);
    }

    // Hand-known values, so agreement with --scan-all is not agreement on a shared mistake.
    const res = try checkFullMatchQuery(&inputs, queries[2]);
    defer freeResult(res);
    try testing.expectEqual(@as(i128, (500 + 1599) * 1100 / 2), res.output.aggs[0].value.i);
    try testing.expectEqual(@as(i128, 1599), res.output.aggs[1].value.i);
    try testing.expectEqual(@as(i128, 1100), res.output.aggs[2].value.i);
    try testing.expectEqual(@as(usize, 2), res.row_groups_pruned);
}

test "a file with no row groups aggregates to the empty answer" {
    // A valid empty table (pyarrow's ParquetWriter closed without writing). Splitting aggregates across workers
    // divided by the row-group count, and the leaf count was read from row group 0.
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/no_row_groups.parquet", testing.allocator);
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const inputs = [_]Input{.{ .name = "no_row_groups.parquet", .bytes = bytes }};
    for ([_]usize{ 1, 4 }) |parallelism| {
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "a > 1",
            .aggregate = "count(*) AS n",
            .parallelism = parallelism,
        });
        defer freeResult(res);
        try testing.expectEqual(@as(i128, 0), res.output.aggs[0].value.i);
    }
}

test "full match: per-row-group decisions on a real footer" {
    // Unsigned columns prune in unsigned order but are never proven fully matching.
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    // pyarrow declares TYPE_DEFINED_ORDER for every leaf and marks untruncated string bounds exact.
    const orders = meta.column_orders orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 6), orders.items.len);
    for (orders.items) |o| try testing.expectEqual(schema.COLUMN_ORDER_TYPE_DEFINED, o);
    const s_stats = meta.row_groups.items[0].columns.items[2].meta_data.?.statistics.?;
    try testing.expectEqual(@as(?bool, true), s_stats.is_max_value_exact);
    try testing.expectEqual(@as(?bool, true), s_stats.is_min_value_exact);

    const D = filter_prune.Decision;
    const all: D = .always_match;
    const cases = [_]struct { filter: []const u8, want: [8]D }{
        .{ .filter = "ts >= 600", .want = .{ .skip, .skip, .skip, all, all, all, all, all } },
        .{ .filter = "u >= 600", .want = .{ .skip, .skip, .skip, .keep, .keep, .keep, .keep, .keep } },
        .{ .filter = "x >= -3", .want = .{ .keep, .keep, .keep, .keep, .keep, .keep, .keep, .keep } },
        .{ .filter = "n IS NOT NULL", .want = .{ .keep, .skip, .keep, .keep, all, all, all, all } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

test "full match: GROUP BY on the filter column" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    // b = ts / 100: group 2 holds b = 4, 5 (partial); groups 3-7 hold b = 6..15 (proven).
    const res = try checkFullMatchQuery(&inputs, .{
        .filter = "b >= 5",
        .aggregate = "count(*) AS n, sum(x) AS sx",
        .group_by = "b",
        .full_match = 5,
    });
    defer freeResult(res);
    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 11), rows.len);
    for (rows) |r| try testing.expectEqual(@as(i128, 100), r[1].i);

    const keyed = try checkFullMatchQuery(&inputs, .{
        .filter = "ts >= 500",
        .aggregate = "count(*) AS n, sum(x) AS sx",
        .group_by = "b",
        .full_match = 5,
    });
    freeResult(keyed);
}

test "full match: several files keep their own decisions" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{
        .{ .name = full_match_fixture, .bytes = bytes },
        .{ .name = full_match_fixture, .bytes = bytes },
        .{ .name = full_match_fixture, .bytes = bytes },
    };
    const res = try checkFullMatchQuery(&inputs, .{
        .filter = "ts >= 500",
        .aggregate = "count(*) AS n, sum(ts) AS st",
        .full_match = 15,
    });
    defer freeResult(res);
    try testing.expectEqual(@as(i128, 3 * 1100), res.output.aggs[0].value.i);
}



test "full match: filter-only chunks of proven row groups are never read" {
    const clean = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(clean);

    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), clean);

    const cases = [_]struct { col: usize, rgs: []const usize, q: FullMatchQuery }{
        .{ .col = 0, .rgs = &.{ 3, 4, 5, 6, 7 }, .q = .{
            .filter = "ts >= 500",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 5,
        } },
        .{ .col = 2, .rgs = &.{ 3, 4, 5, 6, 7 }, .q = .{
            .filter = "s >= 'k000500'",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 5,
        } },
        .{ .col = 4, .rgs = &.{ 4, 5, 6, 7 }, .q = .{
            .filter = "n IS NOT NULL",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 4,
        } },
    };
    for (cases) |c| {
        const poisoned = try testing.allocator.dupe(u8, clean);
        defer testing.allocator.free(poisoned);
        poisonChunks(poisoned, &meta, c.col, c.rgs);
        const poisoned_in = [_]Input{.{ .name = "poisoned", .bytes = poisoned }};

        var want_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer want_arena.deinit();
        const clean_in = [_]Input{.{ .name = "clean", .bytes = clean }};
        const want = try runFullMatchQuery(want_arena.allocator(), &clean_in, c.q, 1, false);
        defer freeResult(want);

        for ([_]usize{ 1, 16 }) |parallelism| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const got = try runFullMatchQuery(arena_state.allocator(), &poisoned_in, c.q, parallelism, false);
            defer freeResult(got);
            try expectSameAnswer(want, got);
            try testing.expectEqual(c.q.full_match, got.row_groups_full_match);
        }

        // Control: the poison is detectable once the column is read, or the checks above prove nothing.
        var ctl_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer ctl_arena.deinit();
        var ctl_q = c.q;
        ctl_q.aggregate = "count(*) AS n";
        if (runFullMatchQuery(ctl_arena.allocator(), &poisoned_in, ctl_q, 1, true)) |r| {
            freeResult(r);
            std.debug.print("full match: poisoned column {d} decoded cleanly under --scan-all\n", .{c.col});
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}

// ------------------------------------------------------------
// Statistics order: filters and pruning on columns whose order is not the signed one their bytes suggest.
// Fixtures from tools/zpq-gen stats_order; expected answers are hand-computed from the generator's formulas.
// ------------------------------------------------------------







test "unsigned columns filter and prune in unsigned order" {
    try expectCountSum(unsigned_order_fixture, &.{
        .{ .filter = "u32 >= 2147483648", .n = 640, .sum_i = 450240 },
        .{ .filter = "u32 = 2147483648", .n = 1, .sum_i = 384 },
        .{ .filter = "u32 < 2147483648", .n = 384, .sum_i = 73536 },
        .{ .filter = "u32 != 3000000000", .n = 1023, .sum_i = 523264 },
        .{ .filter = "u32 IN (0, 4294967295)", .n = 2, .sum_i = 1023 },
        .{ .filter = "u64 > 9223372036854775807", .n = 640, .sum_i = 450240 },
        .{ .filter = "u64 = 18446744073709551615", .n = 1, .sum_i = 1023 },
        .{ .filter = "u8 >= 128", .n = 512, .sum_i = 294656 },
        .{ .filter = "u16 BETWEEN 32768 AND 65535", .n = 512, .sum_i = 293888 },
        .{ .filter = "n32 > 2147483647", .n = 256, .sum_i = 147456 },
        // Literals outside the column's range have a constant answer.
        .{ .filter = "u32 > -1", .n = 1024, .sum_i = 523776 },
        .{ .filter = "u32 = -5", .n = 0, .sum_i = 0 },
        .{ .filter = "u64 < 18446744073709551616", .n = 1024, .sum_i = 523776 },
        .{ .filter = "NOT u64 <= 18446744073709551615", .n = 0, .sum_i = 0 },
    });
}

test "unsigned columns: row-group decisions read bounds unsigned" {
    const bytes = (try loadFixture(unsigned_order_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [4]D }{
        .{ .filter = "u32 >= 3000000000", .want = .{ .skip, .skip, .keep, .keep } },
        .{ .filter = "u32 < 256", .want = .{ .keep, .skip, .skip, .skip } },
        .{ .filter = "u64 >= 18446744073709551360", .want = .{ .skip, .skip, .skip, .keep } },
        .{ .filter = "u64 = 9223372036854775808", .want = .{ .skip, .keep, .skip, .skip } },
        .{ .filter = "n32 IS NULL", .want = .{ .keep, .keep, .keep, .always_match } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}



test "deprecated min/max prune only where their signed order is the column's order" {
    try expectCountSum(deprecated_stats_fixture, &.{
        // Byte arrays, DECIMAL and unsigned: the signed pair excludes a stored value from its own row group.
        .{ .filter = "s = 'a'", .n = 1, .sum_i = 3 },
        .{ .filter = "s < 'b'", .n = 1, .sum_i = 3 },
        .{ .filter = "dec = 1.00", .n = 1, .sum_i = 1 },
        .{ .filter = "dec < 1.28", .n = 1, .sum_i = 1 },
        .{ .filter = "u = 1", .n = 1, .sum_i = 1 },
        .{ .filter = "u = 3000000000", .n = 1, .sum_i = 3 },
        // Signed integers and doubles: the pair is in their order and still prunes.
        .{ .filter = "i = 2", .n = 1, .sum_i = 2 },
        .{ .filter = "d >= 2.5", .n = 4, .sum_i = 309 },
        .{ .filter = "idec = 1.28", .n = 1, .sum_i = 2 },
    });

    const bytes = (try loadFixture(deprecated_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [2]D }{
        .{ .filter = "s = 'a'", .want = .{ .unknown, .unknown } },
        .{ .filter = "dec = 6.00", .want = .{ .unknown, .unknown } },
        .{ .filter = "u = 11", .want = .{ .unknown, .unknown } },
        .{ .filter = "i > 100", .want = .{ .skip, .keep } },
        .{ .filter = "d < 1", .want = .{ .keep, .skip } },
        // DECIMAL over INT32 compares as its signed unscaled integer: the pair is in numeric order.
        .{ .filter = "idec > 6.50", .want = .{ .skip, .keep } },
        .{ .filter = "idec < 3", .want = .{ .keep, .skip } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

test "deprecated min/max never answer a DECIMAL aggregate under --trust-stats" {
    const bytes = (try loadFixture(deprecated_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = deprecated_stats_fixture, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "min(dec) AS lo, max(dec) AS hi, max(i) AS mi",
        .parallelism = 1,
        .trust_stats = true,
    });
    defer freeResult(res);
    // DECIMAL(9,2): exact unscaled values.
    const d92: expr_ast.Dec = .{ .precision = 9, .scale = 2 };
    try testing.expectEqual(DecValue{ .v = 100, .spec = d92 }, res.output.aggs[0].value.dec);
    try testing.expectEqual(DecValue{ .v = 700, .spec = d92 }, res.output.aggs[1].value.dec);
    try testing.expectEqual(@as(i128, 103), res.output.aggs[2].value.i);
}



test "bounds in a declared column order zpq does not implement never prune" {
    // `s` declares an unknown order whose bounds, read bytewise, exclude 'B' and 'Y' from their row groups and pages.
    try expectCountSum(column_order_fixture, &.{
        .{ .filter = "s = 'B'", .n = 1, .sum_i = 1 },
        .{ .filter = "s = 'Y'", .n = 1, .sum_i = 102 },
        .{ .filter = "s < 'a'", .n = 2, .sum_i = 103 },
        .{ .filter = "s >= 'a'", .n = 4, .sum_i = 209 },
        .{ .filter = "i > 100", .n = 3, .sum_i = 306 },
    });

    const bytes = (try loadFixture(column_order_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [2]D }{
        .{ .filter = "s = 'B'", .want = .{ .unknown, .unknown } },
        .{ .filter = "s >= 'zz'", .want = .{ .unknown, .unknown } },
        // `i` keeps TYPE_DEFINED_ORDER.
        .{ .filter = "i > 100", .want = .{ .skip, .always_match } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}



test "!= on a float chunk whose bounds are the literal skips only when nan_count proves no NaN" {
    // Every row group records [-0, 0]; row groups 0 and 2 hold a NaN, which `!= 0` keeps.
    try expectCountSum(nan_stats_fixture, &.{
        .{ .filter = "d != 0", .n = 2, .sum_i = 8 },
        .{ .filter = "NOT d = 0", .n = 2, .sum_i = 8 },
        .{ .filter = "d = 0", .n = 7, .sum_i = 28 },
    });

    const bytes = (try loadFixture(nan_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const rgs = meta.row_groups.items;
    try testing.expectEqual(@as(?i64, null), rgs[0].columns.items[0].meta_data.?.statistics.?.nan_count);
    try testing.expectEqual(@as(?i64, 0), rgs[1].columns.items[0].meta_data.?.statistics.?.nan_count);
    const f = try filter_parser.parse(arena, "d != 0", &meta, null);
    const D = filter_prune.Decision;
    for (meta.row_groups.items, [_]D{ .keep, .skip, .keep }, 0..) |*rg, want, i| {
        const got = filter_prune.pruneRowGroup(rg, f, &meta);
        testing.expectEqual(want, got) catch |err| {
            std.debug.print("d != 0: row group {d} decided {s}, want {s}\n", .{ i, @tagName(got), @tagName(want) });
            return err;
        };
    }
}

test "clashing output column names are rejected and named" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const Case = struct {
        group_by: ?[]const u8 = null,
        aggregate: []const u8,
        select_cols: ?[]const []const u8 = null,
        want: anyerror,
        column: []const u8,
    };
    const cases = [_]Case{
        // A key renamed away from its source column and an aggregate named after it: `s` used to show the key.
        .{ .group_by = "s AS k", .aggregate = "sum(x) AS s", .want = error.AmbiguousOutputColumn, .column = "s" },
        .{ .group_by = "b", .aggregate = "count(*) AS b", .want = error.AmbiguousOutputColumn, .column = "b" },
        .{
            .group_by = "b",
            .aggregate = "sum(x) AS t, min(x) AS T",
            .want = error.DuplicateOutputColumn,
            .column = "T",
        },
        .{ .aggregate = "sum(x) AS t, min(x) AS t", .want = error.DuplicateOutputColumn, .column = "t" },
        .{
            .group_by = "b AS k, ts AS k",
            .aggregate = "count(*) AS c",
            .want = error.DuplicateOutputColumn,
            .column = "k",
        },
        // The SQL front end's select list: `SELECT s AS k, sum(x) AS s ... GROUP BY s`.
        .{
            .group_by = "s",
            .aggregate = "sum(x) AS s",
            .select_cols = &.{ "s AS k", "sum(x) AS s" },
            .want = error.AmbiguousOutputColumn,
            .column = "s",
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var diag: Diag = .{};
        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
            .diag = &diag,
        });
        try testing.expectError(c.want, res);
        try testing.expectEqualStrings(c.column, diag.column.get());
    }
}

test "unaliased aggregates keep the function name unless it clashes, then take the call text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const Case = struct {
        group_by: ?[]const u8 = null,
        aggregate: []const u8,
        select_cols: ?[]const []const u8 = null,
        want: []const []const u8,
    };
    const cases = [_]Case{
        .{ .aggregate = "sum(x)", .want = &.{"sum"} },
        .{ .aggregate = "sum(x), sum(ts), count(*)", .want = &.{ "sum(x)", "sum(ts)", "count" } },
        .{ .aggregate = "count(*) AS count, count(n)", .want = &.{ "count", "count(n)" } },
        .{
            .aggregate = "sum(x) FILTER (WHERE ts > 5), sum(x)",
            .want = &.{ "sum(x) FILTER (WHERE ts > 5)", "sum(x)" },
        },
        .{ .group_by = "b", .aggregate = "min(x), min(ts)", .want = &.{ "b", "min(x)", "min(ts)" } },
        // The SQL front end lists unaliased aggregates by call text, which used to resolve to nothing.
        .{
            .group_by = "b",
            .aggregate = "count(*), sum(x)",
            .select_cols = &.{ "b", "count(*)", "sum(x)" },
            .want = &.{ "b", "count(*)", "sum(x)" },
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
        });
        defer freeResult(res);
        if (res.output.group_cols) |cols| {
            try testing.expectEqual(c.want.len, cols.len);
            for (c.want, cols) |w, g| try testing.expectEqualStrings(w, g);
        } else {
            try testing.expectEqual(c.want.len, res.output.aggs.len);
            for (c.want, res.output.aggs) |w, g| try testing.expectEqualStrings(w, g.alias);
        }
    }
}

test "page-pruned row groups decode just their kept rows, with the answers of whole columns" {
    const prior_compact = consumer.compact_rows_in_tests;
    defer consumer.compact_rows_in_tests = prior_compact;
    // Tiny pages with a page index, dictionary and plain encoded, nullable columns of every common type.
    const files = [_][]const u8{
        "data/parquet-testing/data/alltypes_tiny_pages.parquet",
        "data/parquet-testing/data/alltypes_tiny_pages_plain.parquet",
    };
    // Only conjunctive leaves prune pages: the OR is a control that decodes whole columns.
    const filters = [_]struct { sql: []const u8, compacts: bool }{
        .{ .sql = "id >= 3000 AND id < 3100", .compacts = true },
        .{ .sql = "id < 50", .compacts = true },
        .{ .sql = "id >= 7000 OR id < 10", .compacts = false },
        .{ .sql = "id = 4242", .compacts = true },
        .{ .sql = "id >= 1000 AND id < 1400 AND int_col > 3", .compacts = true },
    };
    const shapes = [_]struct { agg: []const u8, group: ?[]const u8 }{
        .{ .agg = "count(*) AS n, sum(bigint_col) AS sb, count(float_col) AS cf, min(double_col) AS md, " ++
            "max(string_col) AS ms, min(date_string_col) AS mdate, sum(tinyint_col) AS st, count(bool_col) AS cb", .group = null },
        .{ .agg = "count(*) AS n, sum(int_col) AS si, max(id) AS hi", .group = "string_col" },
        .{ .agg = "count(*) AS n, min(id) AS lo", .group = "month, bool_col" },
    };
    for (files) |fixture| {
        const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
            if (err == error.FileNotFound) return error.SkipZigTest;
            return err;
        };
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};
        for (filters) |f| for (shapes) |shape| for ([_]usize{ 1, 4 }) |j| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .filter = f.sql,
                .aggregate = shape.agg,
                .group_by = shape.group,
                .parallelism = j,
                .min_bytes_per_worker = 1,
            };
            consumer.compact_rows_in_tests = false;
            var want = try runMultiAggregate(testing.allocator, a, args);
            defer want.deinit(testing.allocator);
            consumer.compact_rows_in_tests = true;
            const before = consumer.compacted_row_groups.load(.monotonic);
            var got = try runMultiAggregate(testing.allocator, a, args);
            defer got.deinit(testing.allocator);
            testing.expectEqual(f.compacts, consumer.compacted_row_groups.load(.monotonic) > before) catch |e| {
                std.debug.print("compaction: {s} {s}\n", .{ fixture, f.sql });
                return e;
            };
            testing.expectEqualDeep(want.output.aggs, got.output.aggs) catch |e| {
                std.debug.print("{s} filter {s} j{d}\n", .{ fixture, f.sql, j });
                return e;
            };
            testing.expectEqualDeep(want.output.group_rows, got.output.group_rows) catch |e| {
                std.debug.print("{s} grouped filter {s} j{d}\n", .{ fixture, f.sql, j });
                return e;
            };
            try testing.expectEqual(want.rows_matched, got.rows_matched);
        };
    }
}

test "page-pruned DECIMAL and INT96 columns decode whole and answer as with compaction off" {
    const prior_compact = consumer.compact_rows_in_tests;
    defer consumer.compact_rows_in_tests = prior_compact;
    // INT32/INT64/FLBA-backed DECIMAL(9/18/38) and INT96, required and optional, behind a page index: their exact
    // decoders read whole chunks, so a row group reading them decodes whole columns rather than compacting.
    const fixture = "ci/fixtures/parquet/decimal_int96_page_index.parquet";
    const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};
    const shapes = [_]struct { agg: []const u8, compacts: ?bool }{
        .{ .agg = "count(*) AS n, sum(d9) AS a, sum(d18) AS b, max(d38) AS c", .compacts = false },
        .{ .agg = "count(*) AS n, sum(d9o) AS a, min(d18o) AS b, max(d38o) AS c, count(d38o) AS cc", .compacts = false },
        .{ .agg = "count(*) AS n, min(ts) AS t, max(tso) AS u, count(tso) AS cu", .compacts = false },
        // A control reading neither: whether it compacts is not this test's subject, only that answers agree.
        .{ .agg = "count(*) AS n, max(id) AS hi", .compacts = null },
    };
    for ([_][]const u8{ "id >= 100 AND id < 140", "id >= 1490 AND id < 1510", "id = 2999" }) |f| for (shapes) |shape|
        for ([_]usize{ 1, 4 }) |j| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .filter = f,
                .aggregate = shape.agg,
                .parallelism = j,
                .min_bytes_per_worker = 1,
            };
            consumer.compact_rows_in_tests = false;
            var want = try runMultiAggregate(testing.allocator, a, args);
            defer want.deinit(testing.allocator);
            consumer.compact_rows_in_tests = true;
            const before = consumer.compacted_row_groups.load(.monotonic);
            var got = try runMultiAggregate(testing.allocator, a, args);
            defer got.deinit(testing.allocator);
            if (shape.compacts) |c| try testing.expectEqual(c, consumer.compacted_row_groups.load(.monotonic) > before);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
        };
}

test "a payload page whose rows the filter rejects is not decoded; one with a selected row reports its corruption" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const n = 100000;
    const k = try a.alloc(i64, n);
    const v = try a.alloc(i64, n);
    const v_def = try a.alloc(u32, n);
    for (0..n) |i| {
        k[i] = @intCast(i);
        v[i] = @intCast(i % 97);
        v_def[i] = @intFromBool(i % 5 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = k } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = v, .def_levels = v_def, .max_def = 1 } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, n, .SNAPPY, .{}, .{});
    const bytes = sink.buffer.items;
    // Corrupt the third data page of `v`: its snappy stream claims an impossible length.
    const meta = try metadata.open(a, bytes);
    const cm = meta.row_groups.items[0].columns.items[1].meta_data.?;
    var pos: usize = @intCast(cm.data_page_offset);
    var first_row: usize = 0;
    var page: usize = 0;
    var rows: usize = 0;
    while (true) : (page += 1) {
        var rdr = thrift_mod.Reader.init(bytes[pos..]);
        const header = try schema.PageHeader.read(&rdr);
        rows = @intCast(header.data_page_header.?.num_values);
        if (page == 2) {
            @memset(bytes[pos + rdr.pos ..][0..4], 0xff);
            break;
        }
        first_row += rows;
        pos += rdr.pos + @as(usize, @intCast(header.compressed_page_size));
    }
    const inputs = [_]Input{.{ .name = "corrupt-payload.parquet", .bytes = bytes }};
    // The canonical site: what an unfiltered scan, which decodes every page, reports.
    var whole: Diag = .{};
    try testing.expectError(error.DecompressionFailed, runMultiAggregate(testing.allocator, a, .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n, sum(v) AS s",
        .parallelism = 1,
        .diag = &whole,
    }));
    var select_one_buf: [64]u8 = undefined;
    const select_one = try std.fmt.bufPrint(&select_one_buf, "k = {d}", .{first_row + rows / 2});
    for ([_]usize{ 1, 4 }) |j| for ([_]usize{ 1 << 20, 512 << 20 }) |budget| {
        // Every row of the corrupt page rejected (with others kept): the answer, from the other pages.
        for ([_][]const u8{ "k < 1000", "k >= 99000 OR k < 10" }) |rejecting| {
            var ok = try runMultiAggregate(testing.allocator, a, .{
                .inputs = &inputs,
                .filter = rejecting,
                .aggregate = "count(*) AS n, sum(v) AS s",
                .parallelism = j,
                .max_memory = budget,
            });
            defer ok.deinit(testing.allocator);
            try testing.expect(ok.output.aggs[0].value.i > 0);
        }
        // A filter keeping one of its rows reads the page, and reports the canonical decode error at that page.
        var diag: Diag = .{};
        const failed = runMultiAggregate(testing.allocator, a, .{
            .inputs = &inputs,
            .filter = select_one,
            .aggregate = "count(*) AS n, sum(v) AS s",
            .parallelism = j,
            .max_memory = budget,
            .diag = &diag,
        });
        if (failed) |res| {
            var r = res;
            r.deinit(testing.allocator);
            return error.TestExpectedError;
        } else |e| try testing.expectEqual(error.DecompressionFailed, e);
        try testing.expectEqual(@as(usize, 0), diag.detail.decode.row_group);
        try testing.expectEqualStrings("v", diag.detail.decode.column.get());
        try testing.expectEqual(whole.detail.decode.page, diag.detail.decode.page);
    };
}

test "page index: null pages a footer contradicts do not drop rows" {
    // These files flag every page of their two REQUIRED columns all-null in the ColumnIndex, while every page holds
    // values. Pruning on that claim answered 0 for filters every other row satisfies.
    const files = [_][]const u8{
        "data/parquet-testing/data/datapage_v1-snappy-compressed-checksum.parquet",
        "data/parquet-testing/data/datapage_v1-uncompressed-checksum.parquet",
        "data/parquet-testing/data/datapage_v1-corrupt-checksum.parquet",
    };
    const Query = struct { filter: []const u8, want: i128 };
    const queries = [_]Query{
        .{ .filter = "a > -66052", .want = 2560 },
        .{ .filter = "a IS NOT NULL", .want = 5120 },
        .{ .filter = "b < 0 OR b >= 0", .want = 5120 },
    };
    for (files) |fixture| {
        const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{fixture});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};
        for (queries) |q| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = q.filter,
                .aggregate = "count(*) AS n",
                .parallelism = 1,
            });
            defer freeResult(res);
            testing.expectEqual(q.want, res.output.aggs[0].value.i) catch |err| {
                std.debug.print("{s}: filter {s}\n", .{ fixture, q.filter });
                return err;
            };
        }
    }
}

test "runMultiAggregate: inputs whose nesting differs are a schema mismatch" {
    // Columns bind by leaf index against file 0. Here file 1 has the same element names in the same order, but
    // `name` is a top-level column rather than a field of `r`, so leaf 1 means different columns in the two files.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const m0 = try test_fixtures.sharedLeafNameMeta(a);
    var m1 = try test_fixtures.sharedLeafNameMeta(a);
    m1.schema.items[0].num_children = 6;
    m1.schema.items[1].num_children = 1;

    const inputs = [_]Input{ .{ .name = "f0", .bytes = "" }, .{ .name = "f1", .bytes = "" } };
    const metas = [_]schema.FileMetaData{ m0, m1 };
    var diag: Diag = .{};
    try testing.expectError(error.SchemaMismatch, runMultiAggregate(testing.allocator, a, .{
        .inputs = &inputs,
        .metas = &metas,
        .aggregate = "sum(key) AS s",
        .parallelism = 1,
        .diag = &diag,
    }));
    try testing.expectEqualStrings("f1", diag.input.?);
    const mismatch = diag.detail.schema_mismatch;
    try testing.expectEqual(@as(usize, 1), mismatch.file);
    try testing.expectEqualStrings("f0", mismatch.first_input);
    try testing.expectEqualStrings(m0.schema.items[0].name, diag.column.get());
    try testing.expectEqualStrings(m1.schema.items[0].name, mismatch.other.get());
}

test "runMultiAggregate: an input that is not Parquet is named with the reason" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const inputs = [_]Input{.{ .name = "notes.txt", .bytes = "not a parquet file at all" }};
    var diag: Diag = .{};
    try testing.expectError(error.NotParquet, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .parallelism = 1,
        .diag = &diag,
    }));
    try testing.expectEqualStrings("notes.txt", diag.input.?);
    try testing.expect(diag.cause != null);
}

// ============================================================ Poisoned statistics.
//
// Checked-in fixtures with their footer, ColumnIndex or OffsetIndex rewritten in memory so they lie in the ways
// `statistics.chunkStats` and `statistics.pageIndexIsPlausible` check for. By default every answer is the decoded
// one. Under --trust-stats impossible claims are still refused, while contradictory or uncorroborated ones are taken
// as written, lie and all.
// ============================================================











test "poisoned stats: chunk bounds with min above max are refused, by default and under --trust-stats" {
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            for ([_]usize{ 0, 2, 3, 5 }) |ci| {
                const st = rg1Stats(meta, ci);
                std.mem.swap(?[]const u8, &st.min_value, &st.max_value);
                std.mem.swap(?[]const u8, &st.min, &st.max);
            }
        }
    }.f;
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        .{ .filter = "ts = 300", .n = 1, .trusted = 1 },
        .{ .filter = "ts >= 250 AND ts <= 260", .n = 11, .trusted = 11 },
        .{ .filter = "s = 'k000300-pppppppppppppppppppppppp'", .n = 1, .trusted = 1 },
        .{ .filter = "u = 300", .n = 1, .trusted = 1 },
        .{ .filter = "x > 5 AND ts >= 200 AND ts < 400", .n = 29, .trusted = 29 },
    });
}

test "poisoned stats: an all-null count beside bounds decodes by default and is trusted under --trust-stats" {
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            rg1Stats(meta, 0).null_count = 200;
        }
    }.f;
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        .{ .filter = "ts IS NULL", .n = 0, .trusted = 200 },
        .{ .filter = "ts IS NOT NULL", .n = 1600, .trusted = 1400 },
        .{ .filter = "ts = 300", .n = 1, .trusted = 1 },
    });
}

test "poisoned stats: a zero null count with no bounds decodes by default and is trusted under --trust-stats" {
    // Row group 1's n is all null, so its writer kept no bounds; the footer now claims no nulls either.
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            rg1Stats(meta, 4).null_count = 0;
        }
    }.f;
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        .{ .filter = "n IS NULL", .n = 320, .trusted = 120 },
        .{ .filter = "n IS NOT NULL", .n = 1280, .trusted = 1480 },
    });
}

test "poisoned stats: a null count outside the chunk's values is refused, by default and under --trust-stats" {
    const cases = [_]PoisonCase{
        .{ .filter = "ts IS NULL", .n = 0, .trusted = 0 },
        .{ .filter = "ts IS NOT NULL", .n = 1600, .trusted = 1600 },
    };
    inline for ([_]i64{ -1, 201, 1000 }) |nc| {
        const edit = struct {
            fn f(meta: *schema.FileMetaData) void {
                rg1Stats(meta, 0).null_count = nc;
            }
        }.f;
        try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &cases);
    }
}

test "cut bounds prune but never answer: equality and ranges at the cut, min and max decoded" {
    // full_match.parquet's s holds 'k%06d-' then 24 'p's, row ids 200..399 in row group 1. Its bounds there become what
    // a writer cutting to 9 bytes writes: the min a prefix, the max a prefix grown by one, both marked inexact. ts's max
    // there becomes an inexact 10^6 (no row holds it).
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            const st = rg1Stats(meta, 2);
            st.min_value = "k000200-p";
            st.max_value = "k000399-q";
            st.min = null;
            st.max = null;
            st.is_min_value_exact = false;
            st.is_max_value_exact = false;
            const ts = rg1Stats(meta, 0);
            ts.max_value = &big_ts;
            ts.max = null;
            ts.is_max_value_exact = false;
        }
        const big_ts = [_]u8{ 0x40, 0x42, 0x0f, 0, 0, 0, 0, 0 }; // 1_000_000
    }.f;
    const p = "pppppppppppppppppppppppp";
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        // Equal to the cut min, and to the cut max: no row holds either.
        .{ .filter = "s = 'k000200-p'", .n = 0, .trusted = 0 },
        .{ .filter = "s = 'k000399-q'", .n = 0, .trusted = 0 },
        // The values the cut bounds stand for, just above the cut min and just below the cut max.
        .{ .filter = "s = 'k000200-" ++ p ++ "'", .n = 1, .trusted = 1 },
        .{ .filter = "s = 'k000399-" ++ p ++ "'", .n = 1, .trusted = 1 },
        // Ranges with the cut bounds and their neighbours as edges.
        .{ .filter = "s < 'k000200-p'", .n = 200, .trusted = 200 },
        .{ .filter = "s <= 'k000200-pa'", .n = 200, .trusted = 200 },
        .{ .filter = "s < 'k000200-pq'", .n = 201, .trusted = 201 },
        .{ .filter = "s >= 'k000200-p'", .n = 1400, .trusted = 1400 },
        .{ .filter = "s > 'k000399-q'", .n = 1200, .trusted = 1200 },
        .{ .filter = "s < 'k000399-q'", .n = 400, .trusted = 400 },
        .{ .filter = "s >= 'k000399-q'", .n = 1200, .trusted = 1200 },
        .{ .filter = "ts <= 399", .n = 400, .trusted = 400 },
    });

    // The answers come from decoding, with and without --trust-stats.
    const clean = try loadFullMatchFixture() orelse return error.SkipZigTest;
    defer testing.allocator.free(clean);
    const bytes = try rewritten(clean, .{ .footer = edit });
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    for ([_]bool{ false, true }) |trust| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS lo, max(s) AS hi, max(ts) AS t, min(ts) AS u",
            .parallelism = 1,
            .trust_stats = trust,
        };
        var oracle_args = args;
        oracle_args.scan_all = true;
        const oracle = try runMultiAggregate(testing.allocator, arena_state.allocator(), oracle_args);
        defer freeResult(oracle);
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("cut bounds: min/max answered from statistics, trust_stats={}\n", .{trust});
            return err;
        };
        try testing.expectEqual(@as(i128, 1599), got.output.aggs[2].value.i);
    }
}

test "poisoned stats: a page whose min is above its max drops the page index, by default and under --trust-stats" {
    // null_pages.parquet: id = row, ten pages of 100 rows in row group 0; page 1 holds 100..199.
    const edit = struct {
        fn f(ci: *schema.ColumnIndex) void {
            std.mem.swap([]const u8, &ci.min_values.items[1], &ci.max_values.items[1]);
        }
    }.f;
    try expectPoisonCases(null_pages_fixture, .{ .column_index = edit }, &.{
        .{ .filter = "id = 130", .n = 1, .trusted = 1 },
        .{ .filter = "id >= 126 AND id <= 128", .n = 3, .trusted = 3 },
    });
}

test "poisoned stats: page locations out of page order drop the page index, by default and under --trust-stats" {
    // Pages 1 and 2 of id swap places while their row starts stay in order: skipping to page 1 would decode page 2.
    const edit = struct {
        fn f(oi: *schema.OffsetIndex) void {
            const locs = oi.page_locations.items;
            std.mem.swap(i64, &locs[1].offset, &locs[2].offset);
            std.mem.swap(i32, &locs[1].compressed_page_size, &locs[2].compressed_page_size);
        }
    }.f;
    try expectPoisonCases(null_pages_fixture, .{ .offset_index = edit }, &.{
        .{ .filter = "id = 130", .n = 1, .trusted = 1 },
        .{ .filter = "id = 260", .n = 1, .trusted = 1 },
    });
}

// ============================================================ All-null pages.
//
// The fixture's ColumnIndex flags whole pages of `v` and `s.b` all-null (see tools/zpq-gen null_pages). A
// null check proves such a page always-match; its rows must still read as null wherever the filter is evaluated.
// ============================================================



test "all-null pages: null checks and their combinations answer like --scan-all" {
    const bytes = metadata.readFileSlice(null_pages_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{null_pages_fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = null_pages_fixture, .bytes = bytes }};

    const Q = struct { filter: []const u8, aggregate: []const u8 = "count(*) AS n, sum(id) AS si", group_by: ?[]const u8 = null };
    const queries = [_]Q{
        .{ .filter = "v IS NULL" },
        .{ .filter = "v IS NOT NULL" },
        .{ .filter = "NOT v IS NULL" },
        .{ .filter = "NOT v IS NOT NULL" },
        .{ .filter = "s.b IS NULL" },
        .{ .filter = "s.b IS NOT NULL" },
        .{ .filter = "s.a IS NULL" },
        .{ .filter = "v IS NULL AND id >= 700" },
        .{ .filter = "id >= 700 AND v IS NULL" },
        .{ .filter = "s.b IS NULL AND id < 1500" },
        .{ .filter = "v IS NULL OR v > 1500" },
        .{ .filter = "v IS NOT NULL AND v > 1200" },
        .{ .filter = "v IS NULL", .aggregate = "count(*) AS n, count(v) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx" },
        .{ .filter = "v IS NOT NULL", .aggregate = "count(v) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx" },
        .{ .filter = "s.b IS NULL", .aggregate = "count(s.b) AS c, count(s.a) AS ca, sum(s.a) AS sa" },
        .{ .filter = "v IS NULL", .aggregate = "count(*) AS n", .group_by = "v" },
        .{ .filter = "v IS NOT NULL AND v < 50", .aggregate = "count(*) AS n", .group_by = "v" },
    };
    for ([_]bool{ false, true }) |trust_stats| for (queries) |q| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .filter = q.filter,
            .aggregate = q.aggregate,
            .group_by = q.group_by,
            .parallelism = 1,
            .trust_stats = trust_stats,
        };
        var oracle_args = args;
        oracle_args.scan_all = true;
        const oracle = try runMultiAggregate(testing.allocator, arena_state.allocator(), oracle_args);
        defer freeResult(oracle);
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("all-null pages: filter={s} agg={s} trust_stats={} disagrees with --scan-all\n", .{
                q.filter, q.aggregate, trust_stats,
            });
            return err;
        };
        // Hand-known counts, so agreement is not agreement on a shared mistake.
        if (q.group_by == null and std.mem.eql(u8, q.aggregate, "count(*) AS n, sum(id) AS si")) {
            const want: ?i128 = if (std.mem.eql(u8, q.filter, "v IS NULL")) 650 else if (std.mem.eql(u8, q.filter, "s.b IS NULL")) 997 else null;
            if (want) |w| try testing.expectEqual(w, got.output.aggs[0].value.i);
        }
    };
}

test "GROUP BY labels a nested leaf whose dotted path a top-level column takes by its quoted path" {
    // Top-level `a.b` (1, null, 3) beside the group `a`'s field `b` (10, 20, null).
    const fixture = "ci/fixtures/parquet/dotted_twin.parquet";
    const bytes = (try loadFixture(fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};

    const Case = struct {
        group_by: []const u8,
        aggregate: []const u8 = "count(*) AS n",
        select_cols: ?[]const []const u8 = null,
        want: []const []const u8,
    };
    const cases = [_]Case{
        .{ .group_by = "b", .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "\"a\".\"b\"", .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "\"a.b\"", .want = &.{ "a.b", "n" } },
        // Both keys at once used to be one name twice.
        .{ .group_by = "b, \"a.b\"", .want = &.{ "\"a\".\"b\"", "a.b", "n" } },
        // The label binds back as an output column name, and is not mistaken for a quoted `a"."b`.
        .{
            .group_by = "b, a.b",
            .select_cols = &.{ "n", "\"a\".\"b\"", "a.b" },
            .want = &.{ "n", "\"a\".\"b\"", "a.b" },
        },
        .{ .group_by = "b AS k", .want = &.{ "k", "n" } },
        // A select list (SQL) names a bare key by any name that binds its column; the output keeps the label
        // unless the select list renames it.
        .{ .group_by = "b", .select_cols = &.{ "b", "n" }, .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "b", .select_cols = &.{ "b AS k", "n" }, .want = &.{ "k", "n" } },
        .{
            .group_by = "\"a\".\"b\", \"a.b\"",
            .select_cols = &.{ "\"a.b\"", "b" },
            .want = &.{ "a.b", "\"a\".\"b\"" },
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
        });
        defer freeResult(res);
        const cols = res.output.group_cols.?;
        try testing.expectEqual(c.want.len, cols.len);
        for (c.want, cols) |w, g| try testing.expectEqualStrings(w, g);
    }

    // The duplicate-name check sees the label, not the dotted path both columns share.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var diag: Diag = .{};
    const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "b, \"a\".\"b\" AS \"a\".\"b\"",
        .parallelism = 1,
        .diag = &diag,
    });
    try testing.expectError(error.DuplicateOutputColumn, res);
    try testing.expectEqualStrings("\"a\".\"b\"", diag.column.get());
}







test "runMultiAggregate: the result destructors free a partly built answer" {
    // Strings are what own memory: string min/max winners, string group keys, output names. Every allocation is
    // failed in turn, so each partial state the answer passes through must be one `deinit` frees.
    const path = "ci/fixtures/parquet/json_escape.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    const args = .{ &inputs, &meta, "min(s) AS m, max(s) AS x, count(*) AS n", null };
    try testing.checkAllAllocationFailures(fixed.allocator(), runAndFree, args);
    const grouped = .{ &inputs, &meta, "min(s) AS m, count(*) AS n", @as(?[]const u8, "s") };
    try testing.checkAllAllocationFailures(fixed.allocator(), runAndFree, grouped);
}

test "ordered GROUP BY: groups as they first appear, and the same float bits at every -j" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // Keys first appear zeta, mu, alpha, omega, beta, kappa (24 row groups, mixed in as they go); `x` mixes
    // magnitudes from 1e-3 to 1e16, so where partial sums meet changes the bits of every float sum.
    const want_order = [_][]const u8{ "zeta", "mu", "alpha", "omega", "beta", "kappa" };
    for ([_]bool{ false, true }) |encoded| for ([_][]const u8{ "k", "i" }) |key| {
        // Results keep state in the arena, so it outlives every one of them.
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var first: ?MultiAggResult = null;
        defer if (first) |f| freeResult(f);
        for ([_]usize{ 1, 2, 3, 8, 32, 32, 7 }) |j| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "sum(x) AS s, avg(x) AS a, count(*) AS n, min(k) AS m",
                .group_by = key,
                .parallelism = j,
                .encoded = encoded,
                .scan_all = true,
                .min_bytes_per_worker = 1,
            });
            const rows = res.output.group_rows.?;
            try testing.expectEqual(want_order.len, rows.len);
            for (rows, want_order) |r, k| try testing.expectEqualStrings(k, r[4].s);
            if (first) |f| {
                for (rows, f.output.group_rows.?) |r, fr| {
                    try testing.expectEqual(@as(u64, @bitCast(fr[1].f)), @as(u64, @bitCast(r[1].f)));
                    try testing.expectEqual(@as(u64, @bitCast(fr[2].f)), @as(u64, @bitCast(r[2].f)));
                    try testing.expectEqual(fr[3].i, r[3].i);
                }
                freeResult(res);
            } else first = res;
        }
    };
}

test "regression: float sums and avgs keep their bits at every -j, budget, decoder and filter" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // 24 row groups of 500 rows, `x` dictionary-encoded and mixing magnitudes from 1e-3 to 1e16: where partial sums
    // meet changes the bits. A small budget reads each row group in slices; the coded path folds over the
    // dictionary; a filter leaves blocks partly selected.
    const Shape = struct { group_by: ?[]const u8, filter: ?[]const u8 };
    for ([_]Shape{
        .{ .group_by = null, .filter = null },
        .{ .group_by = null, .filter = "i > 7" },
        .{ .group_by = null, .filter = "k = 'zeta'" },
        .{ .group_by = "k", .filter = null },
        .{ .group_by = "k", .filter = "i > 7" },
    }) |shape| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var first: ?MultiAggResult = null;
        defer if (first) |f| freeResult(f);
        for ([_]usize{ 1, 3, 8 }) |j| for ([_]?usize{ null, 8 << 10, 32 << 10, 128 << 10 }) |budget| {
            for ([_]bool{ true, false }) |encoded| for ([_]bool{ true, false }) |scan_all| {
                const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    // Computed arguments over the coded column: one gathered under a sparse filter, one that may be
                    // NULL and so evaluates in place.
                    .aggregate = "sum(x) AS s, avg(x) AS a, sum(x * 2) AS t, sum(CASE WHEN i > 3 THEN x END) AS u, " ++
                        "avg_state(x) AS st",
                    .group_by = shape.group_by,
                    .filter = shape.filter,
                    .parallelism = j,
                    .decode_limit = budget,
                    .encoded = encoded,
                    .scan_all = scan_all,
                    .min_bytes_per_worker = 1,
                });
                const f = first orelse {
                    first = res;
                    continue;
                };
                defer freeResult(res);
                if (shape.group_by == null) {
                    for (res.output.aggs, f.output.aggs) |g, w| try expectSameBits(w.value, g.value);
                } else for (res.output.group_rows.?, f.output.group_rows.?) |r, w| {
                    for (r, w) |g, x| try expectSameBits(x, g);
                }
            };
        };
    }
}



test "regression: count(col) answers, or fails, the same at every budget and -j" {
    const path = "ci/fixtures/parquet/plain_values_corrupt.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // One PLAIN length prefix in `s` runs past its page: count(s) reads only its levels when the row group decodes
    // whole, and read the values (and failed) when a small budget sliced it.
    var want: ?anyerror!i128 = null;
    for ([_]usize{ 1, 3 }) |j| for ([_]?usize{ null, 64 << 10, 4 << 10, 512 }) |budget| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const got: anyerror!i128 = if (runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(s) AS n",
            .parallelism = j,
            .decode_limit = budget,
            .min_bytes_per_worker = 1,
        })) |res| blk: {
            defer freeResult(res);
            break :blk res.output.aggs[0].value.i;
        } else |err| err;
        const w = want orelse {
            want = got;
            continue;
        };
        if (w) |v| try testing.expectEqual(v, try got) else |e| try testing.expectError(e, got);
    };
    try testing.expectEqual(@as(i128, 216), try want.?);
}

test "GROUP BY hash merge: same groups, counts and string mins at every -j" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // count and string min are not order-sensitive, so workers keep separate tables and the final merge hashes.
    for ([_][]const u8{ "k", "i" }) |key| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var base: ?MultiAggResult = null;
        defer if (base) |b| freeResult(b);
        for ([_]usize{ 1, 2, 8, 32 }) |j| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(*) AS n, min(k) AS mk",
                .group_by = key,
                .parallelism = j,
                .scan_all = true,
                .min_bytes_per_worker = 1,
            });
            const rows = res.output.group_rows.?;
            if (base) |b| {
                const want = b.output.group_rows.?;
                try testing.expectEqual(want.len, rows.len);
                for (rows, want) |row, w| {
                    try testing.expectEqual(w[1].i, row[1].i);
                    try testing.expectEqualStrings(w[2].s, row[2].s);
                    switch (w[0]) {
                        .s => |s| try testing.expectEqualStrings(s, row[0].s),
                        .i => |v| try testing.expectEqual(v, row[0].i),
                        else => return error.TestUnexpectedResult,
                    }
                }
                freeResult(res);
            } else base = res;
        }
    }
}



test "hash merge of worker tables: owned-string setup preserves arena state on refusal" {
    var arena: expr_agg.KeyArena = .{};
    // A nonempty payload makes the old tag/payload mismatch visible without relying on allocator fill or layout.
    var acc: expr_agg.Accumulator = .{ .min_arena = .{
        .store = &arena,
        .ref = 0xaaaaaaaa,
        .key = 0xbbbbbbbb,
        .skip = 5,
    } };
    const before = acc;
    var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
    try testing.expectError(error.OutOfMemory, setMergeTestString(failing.allocator(), &acc, "winner"));
    try testing.expect(sameAccBits(before, acc));
    try setMergeTestString(testing.allocator, &acc, "winner");
    defer freeOwnedAccumulatorStrings(testing.allocator, (&acc)[0..1]);
    try testing.expectEqualStrings("winner", acc.min_bytes.?);
}







test "parallel hash merge matches one thread, including a failed spawn" {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    };
    const n: usize = 10_000;
    var a = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer b.deinit();
    try fillCountTables(&a, &b, &calls, n);
    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = try testing.allocator.alloc(MergeGroupRef, a.refs.items.len + b.refs.items.len);
    defer testing.allocator.free(refs);
    for (a.refs.items, 0..) |_, i| refs[i] = .{ .table = 0, .group = @intCast(i) };
    for (b.refs.items, 0..) |_, i| refs[a.refs.items.len + i] = .{ .table = 1, .group = @intCast(i) };

    var serial_slots: std.ArrayList(MergedSlot) = .empty;
    defer serial_slots.deinit(testing.allocator);
    var serial_accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer serial_accs.deinit(testing.allocator);
    // Counts are copied by value and the source is left intact, so a second merge sees the same partials.
    const ta = testing.allocator;
    try mergeFoldedRefs(ta, ta, &tables, refs, &calls, 1, null, false, &serial_slots, &serial_accs);
    try testing.expectEqual(n + n / 2, serial_slots.items.len);
    const half: u64 = @intCast(n / 2);
    try testing.expectEqual(half + 11 * half + 10 * half, countSum(serial_accs.items, serial_slots.items));

    const Less = struct {
        fn lt(_: void, x: MergedSlot, y: MergedSlot) bool {
            return std.mem.order(u8, x.key, y.key) == .lt;
        }
    };
    std.sort.pdq(MergedSlot, serial_slots.items, {}, Less.lt);

    for ([_]usize{ 0, 1, 2 }) |fail_after| {
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(testing.allocator);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(testing.allocator);
        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();
        try mergeFoldedRefs(ta, ta, &tables, refs, &calls, 8, null, false, &slots, &accs);
        spawn_util.resetFailure();
        try testing.expectEqual(serial_slots.items.len, slots.items.len);
        std.sort.pdq(MergedSlot, slots.items, {}, Less.lt);
        for (slots.items, serial_slots.items) |got, want| {
            try testing.expectEqualStrings(want.key, got.key);
            try testing.expectEqual(want.first, got.first);
            try testing.expectEqual(serial_accs.items[want.idx].count, accs.items[got.idx].count);
        }
    }
}

test "a merge drawn from the query's ledger gives all of it back, and past it is refused, holding nothing" {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    };
    const n: usize = 10_000;
    var a = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer b.deinit();
    try fillCountTables(&a, &b, &calls, n);
    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = try testing.allocator.alloc(MergeGroupRef, a.refs.items.len + b.refs.items.len);
    defer testing.allocator.free(refs);
    for (a.refs.items, 0..) |_, i| refs[i] = .{ .table = 0, .group = @intCast(i) };
    for (b.refs.items, 0..) |_, i| refs[a.refs.items.len + i] = .{ .table = 1, .group = @intCast(i) };
    for ([_]usize{ 1, 8 }) |workers| for ([_]bool{ false, true }) |in_place| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var charged: core_memory.Charged = .{ .child = testing.allocator, .budget = &budget };
        const lists = charged.allocator();
        {
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(lists);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer accs.deinit(lists);
            const ta = testing.allocator;
            try mergeFoldedRefs(ta, lists, &tables, refs, &calls, workers, null, in_place, &slots, &accs);
            try testing.expectEqual(n + n / 2, slots.items.len);
            try testing.expect(budget.used.load(.monotonic) >= slots.capacity * @sizeOf(MergedSlot));
        }
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        budget.limit = 64 << 10;
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(lists);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(lists);
        const ta = testing.allocator;
        const refused = mergeFoldedRefs(ta, lists, &tables, refs, &calls, workers, null, in_place, &slots, &accs);
        try testing.expectError(error.OutOfMemory, refused);
        try testing.expectEqual(error.ExceededMemoryBudget, charged.refusal(error.OutOfMemory));
        slots.clearAndFree(lists);
        accs.clearAndFree(lists);
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    };
}

test "hash merge of worker tables: overlapping keys, first appearance, allocation failure" {
    try mergeOverlapAndFree(testing.allocator);
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), mergeOverlapAndFree, .{});
}







test "hash merge of fixed-width tables: the byte map's groups, firsts, order and state, cut or not, at every -j" {
    for ([_]u8{ 9, 18, 27 }) |stride| for ([_]usize{ 1, 3, 8 }) |j| {
        try mergeFixedAsBytes(testing.allocator, 6000, stride, j);
    };
    spawn_util.injectFailureAfter(1);
    defer spawn_util.resetFailure();
    try mergeFixedAsBytes(testing.allocator, 6000, 9, 8);
}









test "in-place merge: the copying merge's rows, bit for bit, at every -j and when spawns fail" {
    const gpa = testing.allocator;
    // Over 8192 refs, so several workers split the merge into partitions.
    const n = 6000;
    for ([_]bool{ false, true }) |fixed| {
        var want = try mergeAndBuildRows(gpa, fixed, n, 1, false);
        defer want.deinit(gpa);
        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 1 } }) |run| {
            if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
            defer spawn_util.resetFailure();
            var got = try mergeAndBuildRows(gpa, fixed, n, run.j, true);
            defer got.deinit(gpa);
            const w = want.group_rows.?;
            const g = got.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            var neg_zero: usize = 0;
            for (w, g) |wr, gr| for (wr, gr) |wv, gv| {
                try testing.expectEqual(std.meta.activeTag(wv), std.meta.activeTag(gv));
                switch (wv) {
                    .s => |s| try testing.expectEqualStrings(s, gv.s),
                    .f => |f| {
                        try testing.expectEqual(@as(u64, @bitCast(f)), @as(u64, @bitCast(gv.f)));
                        if (std.math.signbit(f) and f == 0) neg_zero += 1;
                    },
                    .avg => |a| {
                        try testing.expectEqual(a.sum == null, gv.avg.sum == null);
                        if (a.sum) |sum|
                            try testing.expectEqual(@as(u64, @bitCast(sum)), @as(u64, @bitCast(gv.avg.sum.?)));
                        try testing.expectEqual(a.count, gv.avg.count);
                    },
                    else => try testing.expect(std.meta.eql(wv, gv)),
                }
            };
            // A float sum of -0.0 is never an answer: the copying merge adds every one into +0.0.
            try testing.expectEqual(@as(usize, 0), neg_zero);
        }
    }
}

test "in-place merge: an allocation failure frees everything" {
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), struct {
        fn run(a: std.mem.Allocator) !void {
            var out = try mergeAndBuildRows(a, true, 12, 1, true);
            out.deinit(a);
        }
    }.run, .{});
}

test "hash merge of fixed-width tables: an allocation failure frees everything" {
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), struct {
        fn run(a: std.mem.Allocator) !void {
            try mergeFixedAsBytes(a, 24, 18, 1);
        }
    }.run, .{});
}

test "runMultiAggregate: a big GROUP BY answers the same rows in the same order at every -j" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // `v` is unique per row (20000 groups, so several blocks of rows), `n` a fixed-width key, `k` skewed with nulls.
    for ([_][]const u8{ "v", "n", "k, v" }) |key| for ([_]bool{ false, true }) |unordered| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var base: ?MultiAggResult = null;
        defer if (base) |b| freeResult(b);
        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 1 }, .{ .j = 8, .fail_after = 5 } }) |run| {
            if (run.fail_after) |n| spawn_util.injectFailureAfter(n);
            defer spawn_util.resetFailure();
            var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(*) AS c, min(v) AS lo, sum(n) AS s",
                .group_by = key,
                .parallelism = run.j,
                .min_bytes_per_worker = 1,
                .unordered = unordered,
            });
            if (run.fail_after != null) try testing.expect(spawn_util.failuresInjected() > 0);
            const rows = res.output.group_rows.?;
            if (std.mem.eql(u8, key, "v")) try testing.expectEqual(@as(usize, 20_000), rows.len);
            const b = base orelse {
                base = res;
                continue;
            };
            defer res.deinit(testing.allocator);
            const want = b.output.group_rows.?;
            try testing.expectEqual(want.len, rows.len);
            if (unordered) {
                // Unordered rows follow the merge's partitions, which change with -j: compare as sets.
                const Less = struct {
                    fn lt(_: void, x: []const AggValue, y: []const AggValue) bool {
                        for (x, y) |xv, yv| {
                            const o = orderValues(xv, yv, false);
                            if (o != .eq) return o == .lt;
                        }
                        return false;
                    }
                };
                const w = try testing.allocator.dupe([]const AggValue, want);
                defer testing.allocator.free(w);
                const g = try testing.allocator.dupe([]const AggValue, rows);
                defer testing.allocator.free(g);
                std.sort.pdq([]const AggValue, w, {}, Less.lt);
                std.sort.pdq([]const AggValue, g, {}, Less.lt);
                for (w, g) |wr, gr| for (wr, gr) |x, y| try testing.expect(sameValue(x, y));
            } else for (want, rows) |wr, gr| for (wr, gr) |x, y| try testing.expect(sameValue(x, y));
        }
    };
}

test "inline integer stream matches the row answer across workers, spawn failure and a tight fallback" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const base: MultiAggArgs = .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(n) AS s",
        .group_by = "n",
        .max_memory = 512 << 20,
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .integer_records = false,
    };
    var want = try runMultiAggregate(testing.allocator, arena.allocator(), base);
    defer want.deinit(testing.allocator);
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]?usize{ null, 0, 2 }) |fail_after| {
        var args = base;
        args.parallelism = j;
        args.typed_answer = true;
        args.integer_records = true;
        args.integer_records_min_rows = 0;
        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();
        var got = try runMultiAggregate(testing.allocator, arena.allocator(), args);
        defer got.deinit(testing.allocator);
        const stream = got.output.integer_stream.?;
        const rows = want.output.group_rows.?;
        try testing.expectEqual(rows.len, stream.state.group_count);
        var lo: usize = 0;
        while (lo < rows.len) {
            const hi = @min(rows.len, lo + 137);
            var window = try stream.window(testing.allocator, got.state.group_col_types.?, lo, hi, j);
            defer window.deinit(testing.allocator);
            for (rows[lo..hi], 0..) |row, i| for (row, window.columns) |value, col| {
                const lane = col.i64;
                try testing.expectEqual(value == .null_val, lane.defs.len != 0 and lane.defs[i] == 0);
                if (value != .null_val) try testing.expectEqual(value.i, lane.values[i]);
            };
            lo = hi;
        }
    };
    var tight = base;
    tight.integer_records = true;
    tight.integer_records_min_rows = 0;
    tight.typed_answer = true;
    tight.max_memory = 16 << 20;
    var fallback = try runMultiAggregate(testing.allocator, arena.allocator(), tight);
    defer fallback.deinit(testing.allocator);
    try testing.expect(fallback.output.integer_stream == null);
    try testing.expectEqual(want.output.group_rows.?.len, fallback.output.typed.?.rows);
}

test "inline records decline released remote windows and retry only through a retained mapping" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = consumer.answer_rg_rows + 1;
    const strings = try a.alloc([]const u8, n);
    @memset(strings, "a");
    strings[n - 1] = "a long interior key";
    const ints = try a.alloc(i64, n);
    @memset(ints, 1);
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .col = .{ .string = .{ .values = strings, .max_def = 0 } },
            .parquet_type = .BYTE_ARRAY,
            .nullable = false,
        },
        .{
            .name = "n",
            .col = .{ .i64 = .{ .values = ints, .max_def = 0 } },
            .parquet_type = .INT64,
            .nullable = false,
        },
    };
    const Capture = struct {
        a: std.mem.Allocator,
        bytes: std.ArrayList(u8) = .empty,
        fn write(ctx: *anyopaque, data: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, data);
        }
    };
    var captured: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &captured, .write_fn = Capture.write },
        &cols,
        n,
        .SNAPPY,
        .{},
        .{},
    );
    const meta = try metadata.open(a, captured.bytes.items);
    // No width evidence in the footer: a mapped byte scan must discover the last key after releasing the first RG.
    for (meta.row_groups.items) |*rg| rg.columns.items[0].meta_data.?.statistics = null;
    const Probe = struct {
        fn trial(alloc: std.mem.Allocator, m: schema.FileMetaData, data: []const u8) !void {
            var budget: core_memory.SharedBudget = .{ .limit = 8 << 20 };
            defer std.debug.assert(budget.used.load(.monotonic) == 0);
            const inputs = [_]Input{.{ .name = "sample.parquet", .bytes = data }};
            const work = [_]WorkItem{.{
                .file = 0,
                .rg = 0,
                .agg_start = 0,
                .agg_len = 0,
                .columns = undefined,
                .accumulators = &.{},
            }};
            if (!try byteSampleMayFit(alloc, .{
                .inputs = &inputs,
                .aggregate = "",
                .ledger = &budget,
            }, &.{m}, &work, 0)) return error.OutOfMemory;
        }
    };
    try Probe.trial(gpa, meta, captured.bytes.items);
    try testing.checkAllAllocationFailures(gpa, Probe.trial, .{ meta, captured.bytes.items });
    var last = meta;
    last.row_groups.items = meta.row_groups.items[1..];
    const sample_inputs = [_]Input{.{ .name = "sample.parquet", .bytes = captured.bytes.items }};
    const sample_work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 0,
        .columns = undefined,
        .accumulators = &.{},
    }};
    try testing.expect(!try byteSampleMayFit(
        gpa,
        .{ .inputs = &sample_inputs, .aggregate = "" },
        &.{last},
        &sample_work,
        0,
    ));
    var failing_probe = testing.FailingAllocator.init(gpa, .{ .fail_index = 0 });
    var probe_budget: core_memory.SharedBudget = .{ .limit = 8 << 20 };
    try testing.expect(!try byteSampleMayFit(
        failing_probe.allocator(),
        .{ .inputs = &sample_inputs, .aggregate = "", .ledger = &probe_budget },
        &.{meta},
        &sample_work,
        0,
    ));
    try testing.expect(failing_probe.has_induced_failure);
    try testing.expectEqual(@as(usize, 0), probe_budget.used.load(.monotonic));
    const Unstarted = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            return error.ReaderNotStarted;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            @panic("reader not started");
        }
    };
    // Neither buffered bytes nor the mapping may be read before a reader starts.
    var unstarted: u8 = 0;
    const gated_inputs = [_]Input{.{
        .name = "unstarted.parquet",
        .bytes = &.{},
        .plan_bytes = &.{},
        .gate = .{ .ctx = &unstarted, .wait = Unstarted.wait, .done = Unstarted.done },
    }};
    try testing.expect(try byteSampleMayFit(
        failing_probe.allocator(),
        .{ .inputs = &gated_inputs, .aggregate = "", .ledger = &probe_budget },
        &.{meta},
        &sample_work,
        0,
    ));
    try testing.expectEqual(@as(usize, 0), probe_budget.used.load(.monotonic));
    const Window = struct {
        bytes: []u8,
        meta: *const schema.FileMetaData,
        budget: *core_memory.SharedBudget,
        refuse_at: usize,
        force_decode: bool,
        refused: bool = false,
        waits: [2]usize = @splat(0),
        done: [2]usize = @splat(0),
        fn wait(ctx: *anyopaque, rg: usize) anyerror!void {
            const w: *@This() = @ptrCast(@alignCast(ctx));
            w.waits[rg] += 1;
            if (rg != 0 and w.force_decode and !w.refused and w.budget.used.load(.monotonic) >= w.refuse_at) {
                w.refused = true;
                return error.ExceededMemoryBudget;
            }
        }
        fn release(ctx: *anyopaque, rg: usize, _: usize) void {
            const w: *@This() = @ptrCast(@alignCast(ctx));
            w.done[rg] += 1;
            for (w.meta.row_groups.items[rg].columns.items) |col| {
                const cm = col.meta_data.?;
                const lo: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
                @memset(w.bytes[lo..][0..@intCast(cm.total_compressed_size)], 0);
            }
        }
    };
    const integer_outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "n" },
        .{ .source = .{ .agg = 0 }, .name = "c" },
        .{ .source = .{ .agg = 1 }, .name = "s" },
    };
    const names_credit = outputNamesBytes(&integer_outputs);
    for ([_]usize{ 0, 4, 8 }) |requested| for ([_]bool{ false, true }) |mapped| for (0..3) |shape|
        for ([_]bool{ false, true }) |text| {
            const bytes_key = shape != 0;
            const parallel_retry = requested == 4;
            var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
            // The established answer retains its small materialization charge until the query ledger dies.
            defer std.debug.assert(budget.used.load(.monotonic) < 1 << 20);
            const buffer = try a.dupe(u8, captured.bytes.items);
            var window: Window = .{
                .bytes = buffer,
                .meta = &meta,
                .budget = &budget,
                .refuse_at = owner_groups.IntFull.State.reservation(n, 1, owner_groups.full_parts, 0, names_credit),
                .force_decode = !bytes_key,
            };
            const in = [_]Input{.{
                .name = if (mapped) "window.parquet" else "s3://bucket/window.parquet",
                .bytes = buffer,
                .gate = .{ .ctx = &window, .wait = Window.wait, .done = Window.release },
                .plan_bytes = if (mapped) captured.bytes.items else null,
            }};
            const args: MultiAggArgs = .{
                .inputs = &in,
                .metas = &.{meta},
                .aggregate = if (shape == 2) "sum(n) AS s" else "count(*) AS c,sum(n) AS s",
                .group_by = if (bytes_key) "k" else "n",
                .result_order = if (shape == 2)
                    .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2 }
                else
                    .{},
                .parallelism = if (mapped and !bytes_key) requested else 1,
                .typed_answer = shape != 2 and !text,
                .record_answer = text,
                .integer_records_min_rows = 0,
                .byte_records = true,
                .byte_records_min_rows = 0,
                .max_memory = budget.limit,
                .ledger = &budget,
                .min_bytes_per_worker = if (parallel_retry) 1 else std.math.maxInt(usize),
            };
            if (shape == 2) {
                const path = (try explainAggregate(gpa, a, args)).record_path;
                if (mapped)
                    try testing.expectEqual(.byte_sum_top_h, path.?.layout)
                else
                    try testing.expect(path == null);
            }
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualSlices(usize, &.{ 1, 1 }, &window.waits);
            try testing.expectEqualSlices(usize, &.{ 1, 1 }, &window.done);
            try testing.expect(got.output.byte_stream == null);
            if (mapped and !bytes_key and parallel_retry) {
                // The injected parallel decode refusal must keep the inline layout for its deterministic serial retry.
                const stream = got.output.integer_stream.?;
                try testing.expectEqual(@as(usize, 1), stream.state.locals.len);
                try testing.expectEqual(@as(usize, 1), stream.state.group_count);
            } else if (shape == 2) {
                try got.output.applyResultOrder(gpa, args.result_order);
                const rows = got.output.group_rows.?;
                try testing.expectEqual(@as(usize, 2), rows.len);
                try testing.expectEqualStrings("a", rows[0][0].s);
                try testing.expectEqual(@as(i128, n - 1), rows[0][1].i);
                try testing.expectEqualStrings(strings[n - 1], rows[1][0].s);
                try testing.expectEqual(@as(i128, 1), rows[1][1].i);
            } else {
                try testing.expect(got.output.integer_stream == null);
                try testing.expectEqual(@as(usize, if (bytes_key) 2 else 1), got.output.rowCount());
                if (text) try testing.expect(got.output.group_rows != null);
            }
            try testing.expectEqual(mapped and !bytes_key, window.refused);
        };
    for ([_]bool{ false, true }) |bytes_key| {
        const inputs = [_]Input{.{ .name = "mapped.parquet", .bytes = captured.bytes.items }};
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "count(*) AS c,sum(n) AS s",
            .group_by = if (bytes_key) "k" else "n",
            .typed_answer = true,
            .integer_records_min_rows = 0,
            .byte_records = true,
            .byte_records_min_rows = 0,
            .max_memory = 512 << 20,
        };
        const explanation = try explainAggregate(gpa, a, args);
        const expected: RecordAdmission = .{
            .layout = if (bytes_key) .byte_group_stream else .integer_group_stream,
            .reservation = if (bytes_key)
                owner_groups.Full.State.reservation(n, 1, owner_groups.full_parts, 0, 0)
            else
                integerQuote(n, 1, null, args, false, false, 0, names_credit),
        };
        try testing.expectEqual(expected, explanation.record_path.?);
    }
}

test "inline record decode refusals retry serially without disabling the layout" {
    const S = std.EnumSet(FastPath);
    var chain: Chain = .{ .parts_tried = false };
    const args: MultiAggArgs = .{ .inputs = &.{}, .aggregate = "", .parallelism = 8, .byte_records = true };
    const serial = narrowed(args, args, S.initMany(&.{ .integer_records, .decode_memory }), &chain, false).?;
    try testing.expectEqual(@as(usize, 1), serial.parallelism);
    try testing.expect(serial.integer_records and serial.byte_records);
    const fallback = narrowed(args, serial, S.initMany(&.{ .integer_records, .decode_memory }), &chain, false).?;
    try testing.expect(!fallback.integer_records and !fallback.byte_records);
    try testing.expectEqual(args.parallelism, fallback.parallelism);
    const wide = narrowed(args, args, S.initMany(&.{.integer_records}), &chain, false).?;
    try testing.expect(!wide.integer_records and !wide.byte_records);
}



test "inline byte stream matches full rows across workers, windows, allocation and spawn failures" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for ([_]?[]const u8{ null, "s,k,c" }) |column_order| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c,sum(n) AS s",
            .group_by = "k",
            .column_order = column_order,
            .max_memory = 512 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(testing.allocator, arena.allocator(), base);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]?usize{ null, 0, 2 }) |fail_after| {
            var args = base;
            args.parallelism = j;
            args.typed_answer = true;
            args.byte_records = true;
            args.byte_records_min_rows = 0;
            if (fail_after) |n| spawn_util.injectFailureAfter(n);
            defer spawn_util.resetFailure();
            var got = try runMultiAggregate(testing.allocator, arena.allocator(), args);
            defer got.deinit(testing.allocator);
            const stream = got.output.byte_stream.?;
            try testing.expectEqual(want.output.group_rows.?.len, stream.state.group_count);
            const rows = want.output.group_rows.?;
            var lo: usize = 0;
            while (lo < rows.len) {
                const hi = @min(rows.len, lo + 137);
                var w = try stream.window(testing.allocator, lo, hi, j);
                defer w.deinit(testing.allocator);
                for (rows[lo..hi], 0..) |row, i| for (row, w.columns) |v, col| {
                    switch (col.col) {
                        .string => |c| {
                            const missing = if (c.def_levels) |d| d[i] == 0 else false;
                            try testing.expectEqual(v == .null_val, missing);
                            if (!missing) try testing.expectEqualStrings(v.s, c.values[i]);
                        },
                        .i64 => |c| {
                            const missing = if (c.def_levels) |d| d[i] == 0 else false;
                            try testing.expectEqual(v == .null_val, missing);
                            if (!missing) try testing.expectEqual(v.i, c.values[i]);
                        },
                        else => unreachable,
                    }
                };
                lo = hi;
            }
            if (j == 1 and fail_after == null and column_order == null)
                try testing.checkAllAllocationFailures(testing.allocator, byteWindowAllocTrial, .{stream});
        };
        var tight = base;
        tight.byte_records = true;
        tight.byte_records_min_rows = 0;
        tight.typed_answer = true;
        tight.max_memory = 16 << 20;
        var fallback = try runMultiAggregate(testing.allocator, arena.allocator(), tight);
        defer fallback.deinit(testing.allocator);
        try testing.expect(fallback.output.byte_stream == null);
        try testing.expectEqual(want.output.group_rows.?.len, fallback.output.typed.?.rows);
    }
    var args: MultiAggArgs = .{ .inputs = &inputs, .aggregate = "count(*) AS c", .byte_records = true };
    var chain: Chain = .{ .parts_tried = false };
    args = narrowed(args, args, std.EnumSet(FastPath).initMany(&.{.integer_records}), &chain, false).?;
    try testing.expect(!args.byte_records and !args.integer_records);
}

test "inline integer SUM cut matches the established partition cut across workers and spawn failures" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for ([_]bool{ false, true }) |descending| {
        const order_keys = [_]expr_ast.OrderKey{.{ .column = 1, .descending = descending }};
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "sum(n) AS s",
            .group_by = "n",
            .result_order = .{ .keys = &order_keys, .limit = 7, .offset = 3 },
            .max_memory = 512 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .integer_records = false,
        };
        var want = try runMultiAggregate(testing.allocator, arena.allocator(), base);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]?usize{ null, 0, 2 }) |fail_after| {
            var args = base;
            args.parallelism = j;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            if (fail_after) |n| spawn_util.injectFailureAfter(n);
            defer spawn_util.resetFailure();
            var got = try prepareAggregate(testing.allocator, arena.allocator(), args, false);
            defer got.deinit(testing.allocator);
            try testing.expect(taken.contains(.integer_records));
            const rows = got.output.group_rows.?;
            try testing.expectEqual(want.output.group_rows.?.len, rows.len);
            for (want.output.group_rows.?, rows) |a, b| for (a, b) |x, y| {
                try testing.expectEqual(std.meta.activeTag(x), std.meta.activeTag(y));
                if (x == .i) try testing.expectEqual(x.i, y.i);
            };
        };
    }
}

test "runMultiAggregate: a NULL first key of two fixed-width keys still reads the second back" {
    // A fixed-width key image pads a NULL column's payload with zeros; read as framing, the pad would be taken for
    // the next column's flag and `id` would come back NULL.
    const bytes = (try loadFixture(null_pages_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = null_pages_fixture, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = "v, id",
            .parallelism = j,
            .min_bytes_per_worker = 1,
        });
        defer res.deinit(testing.allocator);
        // `id` is unique and never NULL; `v` is NULL in 650 of the 2000 rows.
        const rows = res.output.group_rows.?;
        try testing.expectEqual(@as(usize, 2000), rows.len);
        var null_v: usize = 0;
        var id_sum: i128 = 0;
        for (rows) |row| {
            if (row[0] == .null_val) null_v += 1;
            id_sum += row[1].i;
        }
        try testing.expectEqual(@as(usize, 650), null_v);
        try testing.expectEqual(@as(i128, 1999 * 2000 / 2), id_sum);
    }
}

test "regression: ordered grouping retains the small-query memory budget at one worker" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = try metadata.readFileSlice(path, testing.allocator);
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const res = try runMultiAggregate(testing.allocator, arena.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "k",
        .parallelism = 1,
        .max_memory = 4096,
        .scan_all = true,
    });
    defer freeResult(res);
    try testing.expectEqual(@as(usize, 6), res.output.group_rows.?.len);
}

test "statistics never invent a zero of the other sign: --trust-stats answers zero extrema as decoding does" {
    // Arrow records -0 / +0 bounds for any chunk holding a zero, all +0 or all -0 alike.
    const path = "ci/fixtures/parquet/signed_zero.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    for ([_]?[]const u8{ null, "k = 0", "k = 1", "k = 2", "k >= 2" }) |filter| {
        var answers: [2][2]u64 = undefined;
        for ([_]bool{ false, true }, 0..) |trust, t| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = filter,
                .aggregate = "min(v) AS lo, max(v) AS hi",
                .trust_stats = trust,
                .scan_all = !trust,
            });
            defer freeResult(res);
            answers[t] = .{ @bitCast(res.output.aggs[0].value.f), @bitCast(res.output.aggs[1].value.f) };
        }
        try testing.expectEqual(answers[0], answers[1]);
    }
}

// ============================================================ Exact DECIMAL.
//
// The fixture (tools/zpq-gen decimal_exact) stores decimals as INT32, INT64 and FLBA(16), dictionary-encoded,
// in three row groups. Every expected value is DuckDB's answer for the same query, as unscaled integer and type.







test "DECIMAL aggregates and arithmetic are exact, on every decode path and worker count" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "s9", .v = -15150, .p = 38, .s = 2 },
        .{ .alias = "mn9", .v = -15150, .p = 9, .s = 2 },
        .{ .alias = "mx9", .v = 15049, .p = 9, .s = 2 },
        .{ .alias = "s18", .v = 344850, .p = 38, .s = 4 },
        .{ .alias = "s38", .v = 448500000000000, .p = 38, .s = 10 },
        .{ .alias = "mn38", .v = -123456789012345678901234561234567891, .p = 38, .s = 10 },
        .{ .alias = "mx38", .v = 123456789012345678901237551234567891, .p = 38, .s = 10 },
        .{ .alias = "sdn", .v = 3670000, .p = 38, .s = 2 },
        .{ .alias = "sq", .v = 45000, .p = 38, .s = 2 },
        // DECIMAL(9,2) * DECIMAL(18,4) is DECIMAL(27,6); a sum is DECIMAL(38, s).
        .{ .alias = "p", .v = 209832550, .p = 38, .s = 6 },
        // Mixed with an INT32 column (DECIMAL(10,0)) and with literals, as SQL types them.
        .{ .alias = "di", .v = -30150, .p = 38, .s = 2 },
        .{ .alias = "dti", .v = 227255050, .p = 38, .s = 2 },
        .{ .alias = "dd", .v = 450015000000000, .p = 38, .s = 10 },
        .{ .alias = "dlit", .v = -227250, .p = 38, .s = 3 },
        .{ .alias = "ilit", .v = -2250, .p = 38, .s = 1 },
        .{ .alias = "neg", .v = 15150, .p = 38, .s = 2 },
        .{ .alias = "co", .v = 3670000, .p = 38, .s = 2 },
        .{ .alias = "cc", .v = 3649800, .p = 38, .s = 2 },
        // avg keeps the argument's type, rounded half away from zero: 36700.00 / 133 = 275.939...
        // avg over DECIMAL(15,2) is DECIMAL(19,6); over DECIMAL(38,10) its own type. avg_state ranks by the mean
        // at the argument's scale.
        .{ .alias = "adn", .v = 275939850, .p = 19, .s = 6 },
        .{ .alias = "a38", .v = 1495000000000, .p = 38, .s = 10 },
        .{ .alias = "sadn", .v = 27594, .p = 15, .s = 2 },
    };
    const aggregate =
        "sum(d9) AS s9, min(d9) AS mn9, max(d9) AS mx9, sum(d18) AS s18, sum(d38) AS s38, min(d38) AS mn38, " ++
        "max(d38) AS mx38, sum(dn) AS sdn, sum(q) AS sq, sum(d9 * d18) AS p, sum(d9 + i) AS di, " ++
        "sum(d9 * i) AS dti, sum(d38 - d9) AS dd, sum(d9 * 1.5) AS dlit, sum(i * 1.5) AS ilit, sum(-d9) AS neg, " ++
        "sum(coalesce(dn, 0)) AS co, sum(coalesce(dn, d9)) AS cc, avg(dn) AS adn, avg(d38) AS a38, " ++
        "avg_state(dn) AS sadn";
    for ([_]bool{ true, false }) |encoded| for ([_]usize{ 1, 3 }) |j| {
        try expectDecimalAnswers(.{
            .inputs = &inputs,
            .aggregate = aggregate,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .encoded = encoded,
        }, &want);
    };
    // A filter: Q6's shape over the decimals (a sparse selection gathers the exact values with the rows).
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(d9 * d18) AS p, sum(q) AS sq",
        .filter = "q >= 1.00 AND q <= 2.00 AND i < 100",
        .parallelism = 1,
    }, &.{
        .{ .alias = "p", .v = -296209063, .p = 38, .s = 6 },
        .{ .alias = "sq", .v = 18700, .p = 38, .s = 2 },
    });
}

test "a DECIMAL result past its precision is an error, never a wrapped or rounded value" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    // Two values of 10^38 - 1 sum past 38 digits; each row of `big * 10` and `big + big` is past them already; and
    // `big + 0.1` first brings `big` to the common DECIMAL(38,1), where 10^38 - 1 has no room (as the SQL engines
    // refuse that cast).
    for ([_][]const u8{ "sum(big) AS s", "sum(big * 10) AS s", "sum(big + big) AS s", "sum(big + 0.1) AS s" }) |agg| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = agg,
            .parallelism = 1,
        }));
    }
    // The same column without its two wide rows fits.
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(big) AS s",
        .filter = "i >= -148",
        .parallelism = 1,
    }, &.{.{ .alias = "s", .v = 298, .p = 38, .s = 0 }});
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(big + 0.1) AS s",
        .filter = "i >= -148",
        .parallelism = 1,
    }, &.{.{ .alias = "s", .v = 3278, .p = 38, .s = 1 }});
    // A mean fits wherever its values do, though their sum does not: avg divides the wide sum. (2 × (10^38 - 1) +
    // 298) / 300, rounded half away from zero.
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "avg(big) AS a",
        .parallelism = 1,
    }, &.{.{ .alias = "a", .v = 666666666666666666666666666666666668, .p = 38, .s = 0 }});
}

test "an aggregate or GROUP BY key over a repeated leaf is refused while planning, before any page decodes" {
    // `l` is LIST<INT64>; a filter on its elements is refused by the filter parser, these by `refuseRepeated`.
    const path = "ci/fixtures/parquet/list_int64.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Case = struct { aggregate: []const u8, group_by: ?[]const u8 = null, err: anyerror };
    for ([_]Case{
        .{ .aggregate = "count(l.list.element) AS c", .err = error.NestedAggregateNotSupported },
        .{ .aggregate = "sum(l.list.element + 1) AS s", .err = error.NestedAggregateNotSupported },
        .{ .aggregate = "count(*) AS c", .group_by = "l.list.element", .err = error.NestedGroupKeyNotSupported },
    }) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var diag: Diag = .{};
        try testing.expectError(c.err, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .diag = &diag,
        }));
        try testing.expect(!diag.execution_started);
        try testing.expectEqualStrings("l.list.element", diag.column.get());
    }
    // Its sibling top-level column still aggregates.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const ok = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "sum(id) AS s",
    });
    defer freeResult(ok);
    try testing.expectEqual(@as(i128, 15), ok.output.aggs[0].value.i);
}

test "DECIMAL aggregates over no values are NULL, and GROUP BY a DECIMAL key is exact" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // Row group 1 holds only NULL `dn`.
    const empty = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "sum(dn) AS s, min(dn) AS m, avg(dn) AS a, avg_state(dn) AS st",
        .filter = "i >= -50 AND i < 50",
        .parallelism = 1,
    });
    defer freeResult(empty);
    try testing.expect(empty.output.aggs[0].value == .null_val);
    try testing.expect(empty.output.aggs[1].value == .null_val);
    try testing.expect(empty.output.aggs[2].value == .null_val);
    const a = empty.output.aggs[3].value.avg_dec;
    try testing.expect(a.sum == null and a.mean == null and a.count == 0);

    const grouped = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n, sum(d18 * q) AS p",
        .group_by = "dn",
        .filter = "i >= -60 AND i <= 60",
        .parallelism = 1,
    });
    defer freeResult(grouped);
    const rows = grouped.output.group_rows.?;
    // 121 rows: 108 have a NULL `dn`, the other 13 a distinct value each.
    try testing.expectEqual(@as(usize, 14), rows.len);
    var nulls: usize = 0;
    for (rows) |row| switch (row[0]) {
        .null_val => {
            nulls += 1;
            try testing.expectEqual(@as(i128, 108), row[1].i);
        },
        .dec => |k| {
            try testing.expectEqual(expr_ast.Dec{ .precision = 15, .scale = 2 }, k.spec);
            try testing.expectEqual(@as(i128, 1), row[1].i);
            try testing.expectEqual(@as(u8, 6), row[2].dec.spec.scale);
        },
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(@as(usize, 1), nulls);
}

test "--trust-stats answers DECIMAL min/max from statistics exactly, as decoding does" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "mn9", .v = -15150, .p = 9, .s = 2 },
        .{ .alias = "mx18", .v = 1299, .p = 18, .s = 4 },
        .{ .alias = "mn38", .v = -123456789012345678901234561234567891, .p = 38, .s = 10 },
        .{ .alias = "mx38", .v = 123456789012345678901237551234567891, .p = 38, .s = 10 },
    };
    for ([_]bool{ false, true }) |trust| try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "min(d9) AS mn9, max(d18) AS mx18, min(d38) AS mn38, max(d38) AS mx38",
        .parallelism = 1,
        .trust_stats = trust,
    }, &want);
}

test "a decode budget slices wide row groups, FIXED_LEN_BYTE_ARRAY DECIMAL included, with the same answer" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "s9", .v = -15150, .p = 38, .s = 2 },
        .{ .alias = "s38", .v = 448500000000000, .p = 38, .s = 10 },
        .{ .alias = "p", .v = 209832550, .p = 38, .s = 6 },
        .{ .alias = "adn", .v = 275939850, .p = 19, .s = 6 },
    };
    const aggregate = "sum(d9) AS s9, sum(d38) AS s38, sum(d9 * d18) AS p, avg(dn) AS adn";
    // Unbounded; then budgets too small for a whole row group per worker, down to a few rows a slice.
    for ([_]?usize{ null, 10_000, 1000 }) |limit| for ([_]usize{ 1, 3 }) |j| {
        try expectDecimalAnswers(.{
            .inputs = &inputs,
            .aggregate = aggregate,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .decode_limit = limit,
        }, &want);
    };
    // GROUP BY slices too.
    for ([_]?usize{ null, 16_000, 1000 }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(d18 * q) AS p, max(dn) AS mx",
            .group_by = "k",
            .parallelism = 1,
            .decode_limit = limit,
        });
        defer freeResult(res);
        const rows = res.output.group_rows.?;
        try testing.expectEqual(@as(usize, 3), rows.len);
        try testing.expectEqual(@as(i128, 17235000), rows[0][1].dec.v);
    }
}

test "a decode budget that slices row groups keeps where each group first appeared" {
    // Unique keys in shuffled order: a LIMIT with no ORDER BY, or a count tied at 1, returns the first to appear. A
    // cut of 300 ends past the first slices of the first row group.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]expr_ast.ResultOrder{
        .{ .limit = 300 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 300 },
    }) |cut| for ([_][]const u8{ "u", "s" }) |key| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = key,
            .select_cols = try selectList(arena, &.{ key, "count(*) AS c", "sum(t) AS st" }),
            .parallelism = 1,
            .result_order = cut,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.decode_limit = 3000;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try want.output.applyResultOrder(testing.allocator, cut);
        try got.output.applyResultOrder(testing.allocator, cut);
        const w = want.output.group_rows.?;
        const g = got.output.group_rows.?;
        try testing.expectEqual(w.len, g.len);
        for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
    };
}

test "a first-keys cut under a decode budget ends inside a later slice where the full scan does" {
    // Keys 3-9 count 7; the ninth group to appear is key 8 (tools/zpq-gen census).
    const path = "ci/fixtures/parquet/census_sliced.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]?usize{ 100, 150, null }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .group_by = "k",
            .aggregate = "count(*) AS c",
            .parallelism = 1,
            .decode_limit = limit,
            .result_order = .{ .limit = 1, .offset = 8 },
        };
        for ([_]?SketchTopK{ null, .{} }) |sketch| {
            args.sketch_topk = sketch;
            var res = try runMultiAggregate(testing.allocator, arena, args);
            defer res.deinit(testing.allocator);
            try res.output.applyResultOrder(testing.allocator, args.result_order);
            const rows = res.output.group_rows.?;
            try testing.expectEqual(@as(usize, 1), rows.len);
            try testing.expectEqual(@as(i128, 8), rows[0][0].i);
            try testing.expectEqual(@as(i128, 7), rows[0][1].i);
        }
    }
}

test "a count cut inside the merge keeps exactly the groups the full sort would, ties by first appearance" {
    // Four tables over 40,000 keys, 80,000 refs: enough for the cut to split the merge into partitions. Most keys
    // count 2 (seen by two tables); every 9973rd counts 3, so the top 25 holds the five count-3 keys and a run of the
    // earliest count-2 keys, decided by first appearance.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var buf: [32]u8 = undefined;
    const n_keys: usize = 40_000;
    for (0..n_keys) |i| {
        const key = std.fmt.bufPrint(&buf, "k{d}", .{(i * 7919) % n_keys}) catch unreachable;
        for ([_]usize{ i % 4, (i + 1) % 4 }) |ti| {
            const id = try tables[ti].getOrInsert(key, &calls, i * 4 + ti);
            tables[ti].accumulators.items[id].count += if (i % 9973 == 0 and ti == i % 4) 2 else 1;
        }
    }
    var refs: std.ArrayList(MergeGroupRef) = .empty;
    defer refs.deinit(testing.allocator);
    for (&tables, 0..) |*t, ti| for (0..t.groupCount()) |g| try refs.append(testing.allocator, .{ .table = @intCast(ti), .group = @intCast(g) });
    try testing.expect(mergePartitionCount(8, refs.items.len, true, mergeEntryBytes(calls.len)) > 1);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const Ranked = struct { c: u64, first: u64, key: []const u8 };
    const rank = struct {
        fn lt(_: void, a: Ranked, b: Ranked) bool {
            return a.c > b.c or (a.c == b.c and a.first < b.first);
        }
    }.lt;
    var lists: [2][]Ranked = undefined;
    for ([_]?ResultCut{ null, .{ .a_idx = 0, .k = 25 } }, &lists) |cut, *out| {
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(testing.allocator);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(testing.allocator);
        try mergeFoldedRefs(testing.allocator, testing.allocator, &tables, refs.items, &calls, 8, cut, false, &slots, &accs);
        if (cut != null) try testing.expectEqual(@as(usize, 25) * mergePartitionCount(8, refs.items.len, true, mergeEntryBytes(calls.len)), slots.items.len);
        const ranked = try arena.alloc(Ranked, slots.items.len);
        for (slots.items, ranked) |s, *r| r.* = .{ .c = accs.items[s.idx].count, .first = s.first, .key = s.key };
        std.sort.pdq(Ranked, ranked, {}, rank);
        out.* = ranked[0..25];
    }
    for (lists[0], lists[1]) |want, got| {
        try testing.expectEqual(want.c, got.c);
        try testing.expectEqual(want.first, got.first);
        try testing.expectEqualStrings(want.key, got.key);
    }
}

test "firstCandidates: every table's earliest groups hold the groups that appeared first overall" {
    // Table 0 saw a..h at 10..17; table 1 saw d at 0 and its own keys at 100.. . The two earliest overall are d (first
    // seen by table 1) and a (table 0), each among the two earliest of the table that first saw it.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var tables: [2]expr_agg.GroupTable = .{ .init(testing.allocator, 1 << 20), .init(testing.allocator, 1 << 20) };
    defer for (&tables) |*t| t.deinit();
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" }, 10..) |key, pos| _ = try tables[0].getOrInsert(key, &calls, pos);
    _ = try tables[1].getOrInsert("d", &calls, 0);
    for ([_][]const u8{ "x", "y", "z", "w", "v", "u", "t" }, 100..) |key, pos| _ = try tables[1].getOrInsert(key, &calls, pos);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const kept = (try firstCandidates(arena_state.allocator(), &tables, 2)).?;
    try testing.expect(kept.len <= 4);
    for ([_][]const u8{ "d", "a" }) |want| {
        for (kept) |k| {
            if (std.mem.eql(u8, k, want)) break;
        } else return error.TestExpectedKey;
    }
}



test "runMultiAggregate: returned count(DISTINCT) sets no longer point at the scan's budget" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 4 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(DISTINCT k) AS u, count(DISTINCT n) AS w",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
        });
        defer res.deinit(testing.allocator);
        for (res.state.accumulators) |acc| {
            const d = acc.distinct.heap() orelse continue;
            try testing.expect(d.shared == null);
            if (d.parts) |parts| for (parts) |p| try testing.expect(p.shared == null);
        }
    }
}

test "planAggregate: frozen dispatch agrees across fixtures, knobs, filters and cuts" {
    // The shared binder compares the frozen dispatch and planner; empty input bytes also forbid reading any pages.
    spawn_util.injectFailureAfter(0);
    defer spawn_util.resetFailure();
    const fixtures = [_]struct { path: []const u8, keys: []const []const u8, filters: []const ?[]const u8 }{
        .{
            .path = "ci/fixtures/parquet/topk_skew.parquet",
            .keys = &.{ "k", "n", "v", "k, n", "n + 1 AS shifted" },
            .filters = &.{ null, "n < 12", "v > '10'", "k LIKE '%a%'" },
        },
        .{
            .path = "ci/fixtures/parquet/census_topk.parquet",
            .keys = &.{ "u", "t", "s", "u, s" },
            .filters = &.{ null, "u > 300000000000", "s LIKE '%x%'" },
        },
    };
    const configs = [_]?SketchTopK{
        null,
        .{},
        .{ .cell_bits = 3 },
        .{ .cell_bits = 8, .admit_per_row = 1 },
        .{ .census = false },
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        .{ .certify = false, .census_fp_bits = 3, .census_check_rows = 64 },
    };
    const orders = [_]expr_ast.ResultOrder{
        .{},
        .{ .limit = 3 },
        .{ .limit = 0 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 3 },
        .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 3 },
    };
    var seen = std.EnumSet(@FieldType(AggregateDispatch, "strategy")){};
    for (fixtures) |fixture| {
        const bytes = (try loadFixture(fixture.path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer meta_arena.deinit();
        const metas = [_]schema.FileMetaData{try metadata.open(meta_arena.allocator(), bytes)};
        const inputs = [_]Input{.{ .name = fixture.path, .bytes = &.{} }};
        for (fixture.keys) |key| for (fixture.filters) |filter| for (configs) |cfg| for (orders) |order| {
            for ([_]usize{ 1, 3 }) |parallelism| for ([_]usize{ 1024, 512 << 20 }) |max_memory| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const explained = try explainAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    .ndv_policy = false,
                    .metas = &metas,
                    .aggregate = "count(*) AS c",
                    .group_by = key,
                    .filter = filter,
                    .parallelism = parallelism,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = cfg,
                    .result_order = order,
                    .max_memory = max_memory,
                });
                seen.insert(explained.plan.dispatch.strategy);
            };
        };
    }
    for ([_]@FieldType(AggregateDispatch, "strategy"){ .plain_hash, .sketch, .first_keys }) |strategy|
        try testing.expect(seen.contains(strategy));
    try testing.expectEqual(@as(usize, 0), spawn_util.failuresInjected());
}

test "planAggregate: scalar, ordered blocks, empty work and gated input stay unchanged" {
    const Gate = struct {
        var marker: u8 = 0;
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            return error.UnexpectedPageRead;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    for ([_][]const u8{ "group_order", "no_row_groups" }) |fixture| {
        var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer meta_arena.deinit();
        const path = try std.fmt.allocPrint(meta_arena.allocator(), "ci/fixtures/parquet/{s}.parquet", .{fixture});
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const metas = [_]schema.FileMetaData{try metadata.open(meta_arena.allocator(), bytes)};
        for ([_]bool{ false, true }) |gated| for ([_]bool{ false, true }) |unordered| {
            for ([_]bool{ false, true }) |grouped| for ([_]bool{ false, true }) |scan_all| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const inputs = [_]Input{.{ .name = path, .bytes = &.{}, .gate = if (gated)
                    .{ .ctx = &Gate.marker, .wait = Gate.wait, .done = Gate.done }
                else
                    null }};
                const has_float = std.mem.eql(u8, fixture, "group_order");
                const explained = try explainAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    .metas = &metas,
                    .aggregate = if (has_float) "count(*) AS c, sum(x) AS s" else "count(*) AS c",
                    .group_by = if (grouped) (if (std.mem.eql(u8, fixture, "group_order")) "k" else "s") else null,
                    .parallelism = 3,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = .{},
                    .result_order = .{ .limit = 3 },
                    .unordered = unordered,
                    .scan_all = scan_all,
                });
                if (!grouped) try testing.expectEqual(.scalar, explained.plan.dispatch.strategy);
                if (gated) try testing.expect(explained.plan.dispatch.count_agg == null);
                if (grouped and !unordered and std.mem.eql(u8, fixture, "group_order"))
                    try testing.expect(explained.plan.dispatch.use_blocks);
            };
        };
    }
}
test "review round3 count budget and path matrix" {
    for ([_][]const u8{ "ci/fixtures/parquet/census_topk.parquet", "ci/fixtures/parquet/census_dictionary_review.parquet" }) |path| {
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        for ([_]?usize{ null, 3000, 20000 }) |limit| for ([_]bool{ false, true }) |encoded|
            for ([_][]const u8{ "u", "s" }) |key| for ([_]?[]const u8{ null, "n = 1", "t > 50000000", "t > 3" }) |filter|
                for ([_]SketchTopK{ .{ .fuse_census = true }, .{ .fuse_census = false }, .{ .certify = false }, .{ .census_fp_bits = 0 } }) |cfg| {
                    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                    defer arena_state.deinit();
                    const arena = arena_state.allocator();
                    var args: MultiAggArgs = .{
                        .inputs = &inputs,
                        .filter = filter,
                        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
                        .group_by = key,
                        .parallelism = 1,
                        .decode_limit = limit,
                        .encoded = encoded,
                        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
                    };
                    var want = try runMultiAggregate(testing.allocator, arena, args);
                    defer want.deinit(testing.allocator);
                    args.sketch_topk = cfg;
                    var got = try runMultiAggregate(testing.allocator, arena, args);
                    defer got.deinit(testing.allocator);
                    try want.output.applyResultOrder(testing.allocator, args.result_order);
                    try got.output.applyResultOrder(testing.allocator, args.result_order);
                    try testing.expectEqual(want.rows_in, got.rows_in);
                    try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                    try testing.expectEqual(want.rows_matched, got.rows_matched);
                    const wr = want.output.group_rows.?;
                    const gr = got.output.group_rows.?;
                    try testing.expectEqual(wr.len, gr.len);
                    for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
                };
    }
}

test "runMultiAggregate: workers that move to partition runs mid-scan answer as the plain fold" {
    // census_topk's `s`: near-unique 20-byte strings, 8 row groups of 1000. A threshold of 64 groups moves a worker
    // after its first window; at -j 3 some workers may never move and still join the runs.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const aggregates = [_][]const u8{
        "count(*) AS c, sum(t) AS st, min(s) AS lo, max(u) AS hi",
        "count(n) AS c, avg(t) AS av",
    };
    const cuts = [_]expr_ast.ResultOrder{
        .{},
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 },
    };
    for (aggregates) |aggregate| for ([_][]const u8{ "s", "s, n" }) |key| for ([_]usize{ 1, 3 }) |j| for (cuts) |cut| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = aggregate,
            .group_by = key,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .result_order = cut,
            .spill = false,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.spill = true;
        args.spill_groups = 64;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expect(!want.part_agg);
        try testing.expect(got.part_agg);
        try want.output.applyResultOrder(testing.allocator, cut);
        try got.output.applyResultOrder(testing.allocator, cut);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "runMultiAggregate: a budget the plain fold fits in holds every worker's partition runs" {
    // Each of thousands of runs drawing a shared-budget block once failed a budget the plain fold uses a sliver of.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]bool{ false, true }) |spill| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = "s",
            .parallelism = 3,
            .min_bytes_per_worker = 1,
            .max_memory = 4 << 20,
            .spill = spill,
            .spill_groups = 64,
        });
        defer res.deinit(testing.allocator);
        try testing.expectEqual(spill, res.part_agg);
        try testing.expectEqual(@as(usize, 7997), res.output.group_rows.?.len);
    }
}







test "regression: string-key spill answers every budget the plain fold answers, gated inputs included" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    // A gate that never waits: with the mapped file beside it the input reads again, without it a spill is not armed.
    const plain_in = [_]Input{.{ .name = path, .bytes = bytes }};
    const gated_in = [_]Input{.{ .name = path, .bytes = bytes, .gate = NoGate.gate, .plan_bytes = bytes }};
    const unmapped_in = [_]Input{.{ .name = path, .bytes = bytes, .gate = NoGate.gate }};
    for ([_]usize{ 1, 3 }) |j| {
        // Dynamic RG assignment changes private-table growth slack at 768 KiB. Keep that boundary serial;
        // concurrent parity uses a refusal margin, beside the unchanged successful-budget cases.
        const budgets = [_]usize{
            96 << 10,                                     192 << 10,  384 << 10,
            (if (j == 1) @as(usize, 768) else 512) << 10, 1536 << 10, 4 << 20,
        };
        const base: MultiAggArgs = .{
            .inputs = &plain_in,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = "s",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .spill_groups = 64,
        };
        var want: [budgets.len]anyerror!usize = undefined;
        var no_spill = base;
        no_spill.spill = false;
        outcomes(no_spill, &budgets, &want);
        for ([_][]const Input{ &plain_in, &gated_in, &unmapped_in }) |inputs| {
            var got: [budgets.len]anyerror!usize = undefined;
            var a = base;
            a.inputs = inputs;
            outcomes(a, &budgets, &got);
            try expectSameOutcomes(&want, &got);
        }
    }
}

test "regression: a ranking result cut answers every budget the uncut merge answers" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        // Match the spill parity test: a serial boundary, concurrent refusal/success margins.
        const budgets = [_]usize{
            96 << 10,                                     192 << 10, 384 << 10,
            (if (j == 1) @as(usize, 768) else 512) << 10, 4 << 20,
        };
        const uncut: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "sum(t) AS st, count(*) AS c",
            .group_by = "s",
            .parallelism = j,
            .min_bytes_per_worker = 1,
        };
        var cut = uncut;
        cut.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
        var want: [budgets.len]anyerror!usize = undefined;
        var got: [budgets.len]anyerror!usize = undefined;
        outcomes(uncut, &budgets, &want);
        outcomes(cut, &budgets, &got);
        for (want, got) |w, g| {
            if (w) |_| try testing.expectEqual(@as(usize, 10), try g) else |e| try testing.expectError(e, g);
        }
    }
}

test "grouped DISTINCT bitmap forced layouts match exact fallback with NULLs filters ordering and typed output" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const Fixture = BitmapTestFixture;
    const fixture = try Fixture.init(a);
    for ([_]?[]const u8{ null, "x >= -4", "k >= 2" }) |filter| {
        for ([_]bool{ false, true }) |typed| {
            const cut: expr_ast.ResultOrder = if (typed) .{} else .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 8 };
            var args = fixture.args();
            args.filter = filter;
            args.typed_answer = typed;
            args.result_order = cut;
            args.grouped_distinct_bitmap = .off;
            var want = try runMultiAggregate(gpa, a, args);
            defer want.deinit(gpa);
            try want.output.applyResultOrder(gpa, cut);
            for ([_]grouped_bitmap.Mode{ .private, .partitioned, .auto }) |mode| {
                args.grouped_distinct_bitmap = mode;
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try testing.expect(got.grouped_distinct_bitmap);
                try got.output.applyResultOrder(gpa, cut);
                try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.output.typed, got.output.typed);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
                const before = try std.json.Stringify.valueAlloc(a, want.output.group_rows, .{});
                const after = try std.json.Stringify.valueAlloc(a, got.output.group_rows, .{});
                try testing.expectEqualStrings(before, after);
            }
        }
    }
    const args = fixture.args();
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    var null_key = false;
    var null_values = false;
    for (got.output.group_rows.?) |row| {
        if (row[0] == .null_val) null_key = true;
        if (row[0] == .i and row[0].i == 7) {
            null_values = true;
            try testing.expectEqual(@as(i128, 0), row[1].i);
        }
    }
    try testing.expect(null_key and null_values);
}



test "grouped DISTINCT bitmap domain violation discards partial state and replays exact fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try BitmapTestFixture.init(a);
    var args = fixture.args();
    args.grouped_distinct_bitmap = .off;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    const zero: [8]u8 = @splat(0);
    for (fixture.metas[0].row_groups.items) |*rg| {
        const st = &rg.columns.items[1].meta_data.?.statistics.?;
        st.min_value = &zero;
        st.max_value = &zero;
        st.min = &zero;
        st.max = &zero;
    }
    for ([_]grouped_bitmap.Mode{ .private, .partitioned }) |mode| {
        args.grouped_distinct_bitmap = mode;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
        args.ledger = &ledger;
        try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
        try testing.expect(taken.contains(.grouped_distinct_bitmap));
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        args.ledger = null;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(!got.grouped_distinct_bitmap);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
}

test "grouped DISTINCT bitmap memory refusal keeps a budget the exact fallback answers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try BitmapTestFixture.init(a);
    var args = fixture.args();
    args.max_memory = 16 << 20;
    args.grouped_distinct_bitmap = .off;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &hi, 1 << 24, .little);
    for (fixture.metas[0].row_groups.items) |*rg| {
        const st = &rg.columns.items[1].meta_data.?.statistics.?;
        st.max_value = &hi;
        st.max = &hi;
    }
    args.grouped_distinct_bitmap = .private;
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    var got = try prepareAggregate(gpa, a, args, false);
    defer got.deinit(gpa);
    try testing.expect(!taken.contains(.grouped_distinct_bitmap));
    try testing.expect(!got.grouped_distinct_bitmap);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "runMultiAggregate: grouped count(DISTINCT) through pairs answers as through sets" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // `t` holds 7 keys, `u` is near unique, `n` is NULL through the first row group: a key's distinct values, ones
    // only NULL counts, a FILTER, other aggregates beside it, and a cut on the distinct count.
    const aggregates = [_][]const u8{
        "count(DISTINCT u) AS d, count(*) AS c",
        "count(DISTINCT n) AS d, sum(u) AS s",
        "count(DISTINCT u) FILTER (WHERE n = 1) AS d, min(u) AS lo",
    };
    const cuts = [_]expr_ast.ResultOrder{
        .{},
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
    };
    for (aggregates) |aggregate| for ([_][]const u8{ "t", "u" }) |key| for ([_]usize{ 1, 3 }) |j| for (cuts) |cut| {
        for ([_]bool{ false, true }) |encoded| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .aggregate = aggregate,
                .group_by = key,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .encoded = encoded,
                .result_order = cut,
                .distinct_pairs = .off,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            args.distinct_pairs = .on;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            try testing.expect(!want.distinct_pairs);
            try testing.expect(got.distinct_pairs);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const wr = want.output.group_rows.?;
            const gr = got.output.group_rows.?;
            try testing.expectEqual(wr.len, gr.len);
            for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
        }
    };
}

test "runMultiAggregate: workers judge their sets after a pilot and move them to pairs only where pairs pay" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // By `t` (7 keys) every group gains new values: its sets move, at -j 3 alongside workers still on sets. By `u`
    // (near unique) a group is a row or two: the sets stay.
    for ([_][]const u8{ "t", "u" }, [_]bool{ true, false }) |key, moves| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d, count(*) AS c",
            .group_by = key,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .distinct_pairs = .off,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.distinct_pairs = .auto;
        args.pairs_pilot_rows = 1000;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expectEqual(moves, got.distinct_pairs);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "mergePartitionCount sizes a full merge's partitions by their state, never below one a worker" {
    const entry = mergeEntryBytes(2);
    // Few references keep one partition a worker; near-unique millions split until a partition's state fits.
    try testing.expectEqual(@as(usize, 1), mergePartitionCount(16, 4000, false, entry));
    try testing.expectEqual(@as(usize, 16), mergePartitionCount(16, 16_000, false, entry));
    const many = mergePartitionCount(16, 27_000_000, false, entry);
    try testing.expect(many > 16 and many <= merge_parts_max);
    try testing.expect(27_000_000 * entry / many <= merge_part_bytes);
    try testing.expectEqual(@as(usize, merge_parts_max), mergePartitionCount(16, 1 << 40, false, entry));
    // A cut keeps its own rule.
    try testing.expectEqual(@as(usize, 512), mergePartitionCount(16, 27_000_000, true, entry));
}

test "runMultiAggregate: a pilot short of rows a group judges again at twice the rows" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // 200 groups of nearly unique values over 1000-row groups: five rows a group after the first row group, ten after
    // the second. Without re-checks the sets stay; with them every worker reading two row groups or more moves, and at
    // -j 3 some worker reads three of the eight while another may read one.
    for ([_]u8{ 0, 3 }) |rechecks| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d, count(*) AS c",
            .group_by = "u / 5000000000 AS b",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .distinct_pairs = .off,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.distinct_pairs = .auto;
        args.pairs_pilot_rows = 1000;
        args.pairs_rechecks = rechecks;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expectEqual(rechecks > 0, got.distinct_pairs);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "regression: count(DISTINCT) pairs answer every budget the set path answers, as the set path does" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var answered: usize = 0;
    for ([_]usize{ 64 << 10, 256 << 10, 1 << 20, 512 << 20 }) |budget| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var results: [2]?MultiAggResult = .{ null, null };
        defer for (results) |r| if (r) |res| freeResult(res);
        var errs: [2]?anyerror = .{ null, null };
        for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .on }, 0..) |pairs, i| {
            results[i] = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(DISTINCT u) AS d",
                .group_by = "t",
                .parallelism = 3,
                .min_bytes_per_worker = 1,
                .max_memory = budget,
                .distinct_pairs = pairs,
            }) catch |e| blk: {
                errs[i] = e;
                break :blk null;
            };
        }
        // Pairs refused budgets the set path answers: the refusal now runs the set path.
        try testing.expectEqual(errs[0], errs[1]);
        const off = results[0] orelse continue;
        answered += 1;
        const rows = off.output.group_rows.?;
        try testing.expectEqual(rows.len, results[1].?.output.group_rows.?.len);
        for (rows, results[1].?.output.group_rows.?) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
    }
    try testing.expect(answered > 0);
}

test "count-only: charged bytes equals keys and filter estimate alone" {
    // Under a sketch count-only pass, depth-2 sum(t + t) must not be charged.
    // keys+filter estimate is 16,189; uncorrected calls charged 48,189, which forced slicing under limit 20,000.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const args: MultiAggArgs = .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
        .group_by = "u",
        .parallelism = 1,
        .decode_limit = 20000,
        .sketch_topk = .{},
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
    };
    var res = try runMultiAggregate(testing.allocator, arena, args);
    defer res.deinit(testing.allocator);
    try res.output.applyResultOrder(testing.allocator, args.result_order);
    const rows = res.output.group_rows.?;
    try testing.expect(rows.len > 0);
}

test "count-only: no filter-only column materialized" {
    // Under a sketch count-only pass on dictionary-encoded column t with filter t > 3,
    // t is not consumed by aggregates and must remain coded without value materialization.
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const args: MultiAggArgs = .{
        .inputs = &inputs,
        .filter = "t > 3",
        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
        .group_by = "u",
        .parallelism = 1,
        .encoded = true,
        .decode_limit = null,
        .sketch_topk = .{},
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
    };
    var res = try runMultiAggregate(testing.allocator, arena, args);
    defer res.deinit(testing.allocator);
    try res.output.applyResultOrder(testing.allocator, args.result_order);
    const rows = res.output.group_rows.?;
    try testing.expect(rows.len > 0);
}



test "result cut SUM: complete signed ranks, NULLs, ties, offsets and owned payload failures" {
    spawn_util.injectFailureAfter(0);
    defer spawn_util.resetFailure();
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            const calls = [_]expr_agg.AggCall{
                .{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 },
                .{ .func = .min, .arg = null, .where = null, .alias = "m", .result = .bytes },
            };
            var tables: [2]expr_agg.GroupTable = undefined;
            for (&tables) |*t| t.* = .init(gpa, 1 << 20);
            defer for (&tables) |*t| t.deinit();
            var refs: std.ArrayList(MergeGroupRef) = .empty;
            defer refs.deinit(gpa);
            var buf: [16]u8 = undefined;
            for (0..40) |g| for (&tables, 0..) |*t, ti| {
                const key = try std.fmt.bufPrint(&buf, "k{d}", .{g});
                const id = try t.getOrInsert(key, &calls, g * 2 + 1 - ti);
                t.accumulators.items[id * 2].sum_i = if (g == 39)
                    null
                else if (ti == 0) 1000 else @as(i128, @intCast(g % 9)) - 1004;
                try setMergeTestString(gpa, &t.accumulators.items[id * 2 + 1], if (ti == 0) "z" else "a");
                try refs.append(gpa, .{ .table = @intCast(ti), .group = id });
            };
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(gpa);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer {
                freeOwnedAccumulatorStrings(gpa, accs.items);
                accs.deinit(gpa);
            }
            const cut: ResultCut = .{ .a_idx = 0, .k = 3, .descending = true, .ranking_first = true };
            try mergeFoldedRefs(gpa, gpa, &tables, refs.items, &calls, 3, cut, false, &slots, &accs);
            try keepResultTop(gpa, gpa, &calls, cut, &slots, &accs);
            try sortSlotsByFirst(gpa, slots.items, 1);
            try testing.expectEqual(@as(usize, 3), slots.items.len);
            for (slots.items, [_]u64{ 16, 34, 52 }) |s, first| {
                try testing.expectEqual(first, s.first);
                try testing.expectEqual(@as(?i128, 4), accs.items[s.idx].sum_i);
                try testing.expectEqualStrings("a", accs.items[s.idx + 1].min_bytes.?);
            }
        }
    }.run, .{});
}

test "result cut SUM: scan and shared partitions match full ordered answers at j1 j3 j8 and spawn failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3, 8 }) |jobs| for ([_]bool{ false, true }) |part| {
        for ([_]bool{ false, true }) |descending| {
            for ([_]u64{ 0, 1, 5, 500 }) |limit| for ([_]bool{ false, true }) |fail_spawn| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const distinct = if (part) "count(*) AS d" else "count(DISTINCT t) AS d";
                const aggregate = try std.fmt.allocPrint(
                    arena,
                    "sum(t - 30) FILTER (WHERE n = 1) AS total, min(s) AS lo, {s}",
                    .{distinct},
                );
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = aggregate,
                    .group_by = "s",
                    .parallelism = jobs,
                    .partagg = part,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = null,
                    .select_cols = try selectList(arena, &.{
                        "s", "sum(t - 30) FILTER (WHERE n = 1) AS total", "min(s) AS lo", distinct,
                    }),
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.result_order = .{
                    .keys = &.{.{ .column = 1, .descending = descending }},
                    .limit = limit,
                    .offset = 2,
                };
                if (fail_spawn) spawn_util.injectFailureAfter(0);
                var got = runMultiAggregate(testing.allocator, arena, args) catch |e| {
                    spawn_util.resetFailure();
                    return e;
                };
                spawn_util.resetFailure();
                defer got.deinit(testing.allocator);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                const wr = want.output.group_rows.?;
                const gr = got.output.group_rows.?;
                try testing.expectEqual(wr.len, gr.len);
                for (wr, gr) |w, g| for (w, g) |a, b| try testing.expect(sameValue(a, b));
            };
        }
    };
}

test "result cut SUM: the merge's scratch is uncharged, as the full merge's is" {
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    const id = try table.getOrInsert("a", &calls, 0);
    table.accumulators.items[id].sum_i = 4;
    var budget: expr_agg.SharedBudget = .{ .limit = 1, .block = 1 };
    table.shared = &budget;
    defer table.shared = null; // This test only charges merge scratch, not the previously private source table.
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(testing.allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer accs.deinit(testing.allocator);
    // A budget the full merge ignores no longer refuses the cut that stands in for it.
    try hashMergeGroupRefs(
        testing.allocator,
        testing.allocator,
        (&table)[0..1],
        &.{.{ .table = 0, .group = id }},
        null,
        &calls,
        .{ .a_idx = 0, .k = 1, .ranking_first = true },
        false,
        &slots,
        &accs,
    );
    try testing.expectEqual(@as(usize, 1), slots.items.len);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "result cut SUM: the one-table cut holds no charge; retained winner capacities share and return the budget" {
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    const outputs = [_]BoundOutputColumn{.{ .source = .{ .agg = 0 }, .name = "s" }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    for ([_][]const u8{ "a", "b" }, 0..) |key, i| {
        const id = try table.getOrInsert(key, &calls, i);
        table.accumulators.items[id].sum_i = @intCast(i);
    }
    var budget: expr_agg.SharedBudget = .{ .limit = 1, .block = 1 };
    table.shared = &budget;
    defer table.shared = null;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 1 };
    // The full merge it stands in for answers whatever the budget, so the cut does too (it refused before).
    const top = (try resultLimitGroups(
        arena_state.allocator(),
        &table,
        &outputs,
        &calls,
        order,
        false,
        null,
    )).?;
    try testing.expectEqualSlices(u32, &.{1}, top);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    budget.limit = 1 << 20;
    var memory: CutScratch = .{ .parent = testing.allocator, .shared = &budget };
    defer memory.finish();
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(testing.allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer accs.deinit(testing.allocator);
    try hashMergeGroupRefs(
        testing.allocator,
        testing.allocator,
        (&table)[0..1],
        &.{ .{ .table = 0, .group = 0 }, .{ .table = 0, .group = 1 } },
        null,
        &calls,
        .{ .a_idx = 0, .k = 1, .ranking_first = true, .memory = memory.allocator() },
        false,
        &slots,
        &accs,
    );
    try testing.expectEqual(
        slots.capacity * @sizeOf(MergedSlot) + accs.capacity * @sizeOf(expr_agg.Accumulator),
        budget.used.load(.monotonic),
    );
}

test "result cut extrema AVG DECIMAL: scans match full stable order at j1 j3 j8" {
    const fixtures = [_]struct { path: []const u8, keys: []const []const u8, aggs: []const []const u8 }{
        .{
            .path = "ci/fixtures/parquet/census_topk.parquet",
            .keys = &.{"s"},
            .aggs = &.{
                "min(t - 30) FILTER (WHERE n = 1) AS rank", "max(t - 30) FILTER (WHERE n = 1) AS rank",
                "avg(t - 30) FILTER (WHERE n = 1) AS rank", "avg_state(t - 30) FILTER (WHERE n = 1) AS rank",
            },
        },
        .{
            .path = decimal_exact_fixture,
            .keys = &.{ "k", "i" },
            .aggs = &.{
                "sum(d38) AS rank", "min(dn) AS rank", "max(dn) AS rank", "avg(dn) AS rank", "avg_state(dn) AS rank",
            },
        },
    };
    for (fixtures) |fixture| {
        const bytes = (try loadFixture(fixture.path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture.path, .bytes = bytes }};
        for (fixture.keys) |key| for (fixture.aggs) |ranking| for ([_]usize{ 1, 3, 8 }) |jobs| {
            for ([_]bool{ false, true }) |part| for ([_]bool{ false, true }) |desc| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const aggregate = try std.fmt.allocPrint(arena, "{s}, count(*) AS c", .{ranking});
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = aggregate,
                    .group_by = key,
                    .parallelism = jobs,
                    .partagg = part,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = null,
                    .select_cols = try selectList(arena, &.{ key, ranking, "count(*) AS c" }),
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.result_order = .{ .keys = &.{.{ .column = 1, .descending = desc }}, .limit = 1, .offset = 1 };
                var got = try runMultiAggregate(testing.allocator, arena, args);
                defer got.deinit(testing.allocator);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                try testing.expectEqual(want.output.group_rows.?.len, got.output.group_rows.?.len);
                for (want.output.group_rows.?, got.output.group_rows.?) |w, g|
                    for (w, g) |a, b| try testing.expect(sameValue(a, b));
            };
        };
    }
}

test "result cut DECIMAL: loser and zero-limit overflow stays observable, payload falls back" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    for ([_]usize{ 1, 3, 8 }) |jobs| for ([_]u64{ 0, 1 }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        // c's sum fits and ranks first ASC; a and b overflow but still have to be finalized.
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .aggregate = "sum(big) AS rank",
            .group_by = "k",
            .parallelism = jobs,
            .min_bytes_per_worker = 1,
            .select_cols = try selectList(arena, &.{ "k", "sum(big) AS rank" }),
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = limit },
        }));
        // c also ranks first by min(i) DESC; the losing SUM(big) payload must keep the full-path error.
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .aggregate = "min(i) AS rank, sum(big) AS payload",
            .group_by = "k",
            .parallelism = jobs,
            .min_bytes_per_worker = 1,
            .select_cols = try selectList(arena, &.{ "k", "min(i) AS rank", "sum(big) AS payload" }),
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = limit },
        }));
    };
}

test "runMultiAggregate: a gated input plans from the dictionaries its probe reads first, as a mapped one does" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(gpa);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    // A reader's buffer: zero until the probe copies what it asks for, the rest once the first gate is waited on.
    const Fake = struct {
        src: []const u8,
        buf: []u8,
        probes: usize = 0,
        started: bool = false,
        filled: bool = false,
        fn add(ctx: *anyopaque, _: usize, _: usize, lo: u64, hi: u64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try testing.expect(!self.started);
            @memcpy(self.buf[lo..hi], self.src[lo..hi]);
            self.probes += 1;
        }
        fn start(ctx: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.started = true;
        }
        fn probed(ctx: *anyopaque, _: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try testing.expect(self.started);
        }
        fn wait(ctx: *anyopaque, _: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!self.filled) @memcpy(self.buf, self.src);
            self.filled = true;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {}
    };
    const buf = try gpa.alloc(u8, bytes.len);
    defer gpa.free(buf);
    // A lone integer key, whose union the dense fold may use: a reader's input reads it only where a choice can turn on
    // it. Past the probe's cap a reader's input plans from its footer, as it did with no probe.
    for ([_]?u64{ null, 4096 }) |cap| for ([_][]const u8{ "count(*) AS c", "count(*) AS c, sum(n) AS s" }) |aggregate| {
        @memset(buf, 0);
        var fake: Fake = .{ .src = bytes, .buf = buf };
        var a1 = std.heap.ArenaAllocator.init(gpa);
        defer a1.deinit();
        var mapped = try runMultiAggregate(gpa, a1.allocator(), .{
            .inputs = &.{.{ .name = "m", .bytes = bytes }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "n",
            .parallelism = 1,
        });
        defer mapped.deinit(gpa);
        var a2 = std.heap.ArenaAllocator.init(gpa);
        defer a2.deinit();
        var gated = try runMultiAggregate(gpa, a2.allocator(), .{
            .inputs = &.{.{
                .name = "g",
                .bytes = buf,
                .gate = .{ .ctx = &fake, .wait = Fake.wait, .done = Fake.done },
                .plan_bytes = bytes,
                .dict_wait = .{ .ctx = &fake, .wait = Fake.probed },
            }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "n",
            .parallelism = 1,
            .probe_cap = cap,
            .start_reads = .{ .ctx = &fake, .start = Fake.start },
            .dict_probe = .{ .ctx = &fake, .add = Fake.add },
        });
        defer gated.deinit(gpa);
        try testing.expect(mapped.reasons.contains(.dictionary_union));
        if (cap == null) {
            try testing.expect(fake.probes > 0);
            try testing.expectEqual(mapped.reasons, gated.reasons);
        } else {
            try testing.expectEqual(@as(usize, 0), fake.probes);
            try testing.expect(!gated.reasons.contains(.dictionary_union) and gated.reasons.contains(.unknown_ndv));
        }
        try expectSameAnswer(mapped, gated);
    };
}

test "runMultiAggregate: a LIMIT with no ORDER BY over few dictionary entries groups in one plain pass, same answer" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    for ([_]usize{ 1, 4 }) |j| {
        var arenas = [2]std.heap.ArenaAllocator{ .init(gpa), .init(gpa) };
        defer for (&arenas) |*a| a.deinit();
        var answers: [2]MultiAggResult = undefined;
        for (&answers, &arenas, [_]bool{ false, true }) |*ans, *arena, policy| {
            ans.* = try runMultiAggregate(gpa, arena.allocator(), .{
                .inputs = &.{.{ .name = "t", .bytes = bytes }},
                .aggregate = "count(*) AS c, sum(n) AS s",
                .group_by = "k",
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .sketch_topk = .{},
                .result_order = .{ .limit = 5 },
                // Without the cost rules the first keys are cut as before.
                .ndv_policy = policy,
            });
        }
        defer for (&answers) |*a| a.deinit(gpa);
        try testing.expect(answers[0].reasons.contains(.first_keys_cut));
        try testing.expect(answers[1].reasons.contains(.few_dictionary_entries));
        // Rows arrive uncut, in first-appearance order: the first five are the cut's.
        const cut = answers[0].output.group_rows.?;
        const all = answers[1].output.group_rows.?;
        try testing.expect(all.len >= cut.len and cut.len >= 5);
        for (cut[0..5], all[0..5]) |w, g| for (w, g) |a, b| try expectSameAggValue(a, b);
    }
}

test "runMultiAggregate: whether a budget answers does not depend on the workers or their timing" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/repeat_keys.parquet", gpa);
    defer gpa.free(bytes);
    // Every key is in every row group: each of four workers' tables holds every key whichever row groups it claims,
    // one table each key once. Partitions answer sum; count(DISTINCT) has none, so it runs on one worker.
    for ([_][]const u8{ "sum(v) AS s", "count(DISTINCT v) AS s" }) |aggregate| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "r", .bytes = bytes }},
            .aggregate = aggregate,
            .group_by = "k",
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 5 },
            .min_bytes_per_worker = 1,
            .dense = false,
            .parallelism = 1,
        };
        var budgets: [40]usize = undefined;
        for (&budgets, 0..) |*b, i| b.* = (256 << 10) + i * (96 << 10);
        var want: [budgets.len](anyerror!usize) = undefined;
        outcomes(args, &budgets, &want);
        args.parallelism = 4;
        var rescued = false;
        for (0..8) |_| {
            var got: [budgets.len](anyerror!usize) = undefined;
            outcomes(args, &budgets, &got);
            try expectSameOutcomes(&want, &got);
        }
        // Some budget the four workers' own tables refuse, which the narrower reruns answer.
        for (budgets, want) |b, w| {
            _ = w catch continue;
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            var first = args;
            first.max_memory = b;
            var taken: std.EnumSet(FastPath) = .{};
            first.taken = &taken;
            if (prepareAggregate(gpa, arena.allocator(), first, false)) |r| freeResult(r) else |e| {
                try testing.expectEqual(error.ExceededMemoryBudget, e);
                rescued = true;
            }
        }
        try testing.expect(rescued);
    }
}

test "regression: a plain grouping a cost rule chose over a cut answers every budget the cut answers" {
    const gpa = testing.allocator;
    const Case = struct {
        path: []const u8,
        aggregate: []const u8,
        filter: ?[]const u8 = null,
        order: expr_ast.ResultOrder,
        /// The rule that groups plainly; null where the facts cannot bound its table and the cut stands.
        rule: ?Reason,
        /// Some budget refuses the rule's plain scan, which the cut answers.
        rescues: bool,
        budget0: usize,
        step: usize,
    };
    const top10: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const skew = "ci/fixtures/parquet/skew_filter.parquet";
    const cases = [_]Case{
        // Few dictionary entries: plain grouping in place of the first keys' cut. Its small domain fits wherever the
        // union runs, so nothing refuses.
        .{
            .path = "ci/fixtures/parquet/topk_skew.parquet",
            .aggregate = "count(*) AS c, sum(n) AS s",
            .order = .{ .limit = 5 },
            .rule = .few_dictionary_entries,
            .rescues = false,
            .budget0 = 512 << 10,
            .step = 512 << 10,
        },
        // The bounds say `s = 5` passes a thousandth of the rows; nine in ten pass, over near-unique keys. Nothing
        // proves the filter selective or the groups few, and the first row group read passes nine in ten: the count
        // cut stays.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "s = 5",
            .order = top10,
            .rule = null,
            .rescues = false,
            .budget0 = 64 << 10,
            .step = 32 << 10,
        },
        // The bounds put `f > 994` at under 1%, and the first row group the scan reads passes that few, counted.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "f > 994",
            .order = top10,
            .rule = .selective_filter,
            .rescues = false,
            .budget0 = 32 << 10,
            .step = 8 << 10,
        },
        // The bounds prove `f = 0` passes one row group in 24, whose near-unique keys a tight budget refuses plainly.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "f = 0",
            .order = top10,
            .rule = .selective_filter,
            .rescues = true,
            .budget0 = 32 << 10,
            .step = 8 << 10,
        },
    };
    for (cases) |case| {
        const bytes = (try loadFixture(case.path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        var budgets: [24]usize = undefined;
        for (&budgets, 0..) |*b, i| b.* = case.budget0 + i * case.step;
        for ([_]usize{ 1, 4 }) |j| {
            const args: MultiAggArgs = .{
                .inputs = &.{.{ .name = case.path, .bytes = bytes }},
                .aggregate = case.aggregate,
                .group_by = "k",
                .filter = case.filter,
                .result_order = case.order,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 },
            };
            {
                var arena = std.heap.ArenaAllocator.init(gpa);
                defer arena.deinit();
                var at = args;
                at.max_memory = budgets[budgets.len - 1];
                const plan = (try explainAggregate(gpa, arena.allocator(), at)).plan;
                if (case.rule) |r| try testing.expect(plan.reasons.contains(r)) else {
                    try testing.expect(!plan.reasons.contains(.selective_filter));
                }
            }
            var cut = args;
            cut.plain_rules = false;
            var want: [budgets.len]anyerror!usize = undefined;
            outcomes(cut, &budgets, &want);
            // The plain grouping may also answer budgets the cut refuses. On several workers whether the cut itself
            // answers depends on which worker claimed which row groups, and its refusal is final, so the outcomes are
            // compared on one; there both are the same every run.
            if (j == 1) {
                var got: [budgets.len]anyerror!usize = undefined;
                outcomes(args, &budgets, &got);
                for (want, got) |w, g| if (w) |n| try testing.expectEqual(n, try g) else |_| {};
            }
            if (!case.rescues) continue;
            var rescued = false;
            for (budgets, want) |b, w| {
                _ = w catch continue;
                var arena = std.heap.ArenaAllocator.init(gpa);
                defer arena.deinit();
                var first = args;
                first.max_memory = b;
                var taken: std.EnumSet(FastPath) = .{};
                first.taken = &taken;
                if (prepareAggregate(gpa, arena.allocator(), first, false)) |r| freeResult(r) else |e| {
                    try testing.expectEqual(error.ExceededMemoryBudget, e);
                    try testing.expect(taken.contains(.plain_rule));
                    rescued = true;
                }
            }
            try testing.expect(rescued);
        }
    }
}

test "runMultiAggregate: every refusal narrows, admitted runs retry shared partitions, within seven scans" {
    const S = std.EnumSet(FastPath);
    const Walk = struct {
        /// `plain`: a per-worker plain scan has run. `scans`: how many have.
        const State = struct { a: MultiAggArgs, chain: Chain, may_fit: bool, cut: bool, plain: bool, scans: usize };

        /// Whether a scan of `st` can arm `t`, as `prepareAggregate` arms paths: a cut arms none; shared partitions
        /// (the cost rules' or forced) arm no per-worker path; the dense fold excludes spill and pairs. A plan's
        /// facts are the same at every rerun (they read the mappings) but may differ from the first scan's (a
        /// reader's), so the cost rules' partitions come only in the first two scans, and a per-worker path armable
        /// at all was armed by the first per-worker plain scan.
        fn armable(st: State, t: S) bool {
            const a = st.a;
            if (t.contains(.integer_records)) return a.integer_records and a.typed_answer and t.count() == 1;
            if (st.cut) return t.count() == 0;
            if (t.contains(.partitioned) and st.scans > 1) return false;
            if (st.plain and (t.contains(.dense) or t.contains(.spill) or t.contains(.pairs))) return false;
            if (t.contains(.plain_rule) and !a.plain_rules) return false;
            if (t.contains(.partitioned) and (!a.partition_cost or a.partagg)) return false;
            if (t.contains(.private_runs) and (!a.private_runs or !t.contains(.partitioned))) return false;
            const shared = t.contains(.partitioned) or a.partagg;
            if (shared and (t.contains(.dense) or t.contains(.spill) or t.contains(.pairs) or
                t.contains(.partitions_fit))) return false;
            if (t.contains(.dense) and (!a.dense or t.contains(.spill) or t.contains(.pairs))) return false;
            if (t.contains(.spill) and !a.spill) return false;
            if (t.contains(.pairs) and a.distinct_pairs == .off) return false;
            if (t.contains(.worker_tables) and a.parallelism == 1) return false;
            if (t.contains(.merge_memory) and a.parallelism == 1) return false;
            return true;
        }

        /// What `narrowed` reads and changes of a state, as bits: equal keys walk the same.
        fn key(st: State) u16 {
            const a = st.a;
            var k: u16 = 0;
            for ([_]bool{
                a.dense,             a.spill,            a.distinct_pairs != .off, a.partition_cost,
                a.partagg,           a.parallelism == 1, a.plain_rules,            st.chain.parts_tried,
                st.may_fit,          st.cut,             st.plain,                 st.scans > 1,
                st.chain.lean_tried, a.private_runs,     a.integer_records,
            }, 0..) |b, i| k |= @as(u16, @intFromBool(b)) << @intCast(i);
            return k;
        }

        /// The most scans any sequence of refusals from `st` runs; every one of them ends.
        fn longest(args: MultiAggArgs, st: State, depth: usize) !usize {
            try testing.expect(depth < 9);
            var seen: [1 << 15]bool = @splat(false);
            var most: usize = 1;
            var candidates: [std.enums.values(FastPath).len]FastPath = undefined;
            var count: usize = 0;
            for (std.enums.values(FastPath)) |path| {
                // Independent executor prefixes are checked below this general-chain walk.
                if (path == .msd_records or path == .shared_strings or path == .grouped_distinct_bitmap) continue;
                if (path == .integer_records and (!st.a.integer_records or !st.a.typed_answer)) continue;
                candidates[count] = path;
                count += 1;
            }
            const all = candidates[0..count];
            for (0..@as(usize, 1) << @intCast(all.len)) |bits| {
                var t: S = .{};
                for (all, 0..) |path, i| if (bits >> @intCast(i) & 1 != 0) t.insert(path);
                if (!armable(st, t)) continue;
                var chain = st.chain;
                const may_fit = st.may_fit or t.contains(.one_table_may_fit);
                const next = narrowed(args, st.a, t, &chain, may_fit) orelse continue;
                const after: State = .{
                    .a = next,
                    .chain = chain,
                    .may_fit = may_fit,
                    .cut = st.cut or (t.contains(.plain_rule) and !t.contains(.private_runs)),
                    .plain = st.plain or !(t.contains(.integer_records) or t.contains(.partitioned) or
                        st.a.partagg or t.contains(.plain_rule)),
                    .scans = st.scans + 1,
                };
                if (seen[key(after)]) continue;
                seen[key(after)] = true;
                most = @max(most, 1 + try longest(args, after, depth + 1));
            }
            return most;
        }
    };
    const args: MultiAggArgs = .{ .inputs = &.{}, .aggregate = "", .parallelism = 4, .decode_limit = 256 << 20 };
    const start: Walk.State = .{
        .a = args,
        .chain = .{ .parts_tried = false },
        .may_fit = false,
        .cut = false,
        .plain = false,
        .scans = 0,
    };
    // The integer path is a single prefix to this general chain: it arms no other path and switches itself off.
    var integer_chain: Chain = .{ .parts_tried = false };
    var integer_args = args;
    integer_args.typed_answer = true;
    const declined = narrowed(integer_args, integer_args, S.initMany(&.{.integer_records}), &integer_chain, false).?;
    try testing.expect(!declined.integer_records and declined.parallelism == args.parallelism);
    try testing.expect(!integer_chain.parts_tried);
    var strings_chain: Chain = .{ .parts_tried = false };
    const strings_declined = narrowed(args, args, S.initMany(&.{.shared_strings}), &strings_chain, false).?;
    try testing.expect(!strings_declined.shared_strings);
    try testing.expectEqual(args.parallelism, strings_declined.parallelism);
    try testing.expectEqual(args.integer_records, strings_declined.integer_records);
    try testing.expectEqual(args.dense, strings_declined.dense);
    try testing.expect(!strings_chain.parts_tried);
    var msd_chain: Chain = .{ .parts_tried = false };
    const msd_declined = narrowed(args, args, S.initMany(&.{ .msd_records, .decode_memory }), &msd_chain, false).?;
    try testing.expect(!msd_declined.msd_records);
    try testing.expectEqual(args.parallelism, msd_declined.parallelism);
    try testing.expectEqual(args.integer_records, msd_declined.integer_records);
    try testing.expectEqual(args.byte_records, msd_declined.byte_records);
    var bitmap_chain: Chain = .{ .parts_tried = false };
    const bitmap_declined = narrowed(args, args, S.initMany(&.{ .grouped_distinct_bitmap, .decode_memory }), &bitmap_chain, false).?;
    try testing.expectEqual(grouped_bitmap.Mode.off, bitmap_declined.grouped_distinct_bitmap);
    try testing.expectEqual(args.parallelism, bitmap_declined.parallelism);
    try testing.expectEqual(args.integer_records, bitmap_declined.integer_records);
    try testing.expect(!bitmap_chain.parts_tried);
    // The cost rules' shared partitions, a fast path's mask, the plain tables, the forced partitions, one worker, and
    // one worker on a small decode budget.
    try testing.expectEqual(@as(usize, 7), try Walk.longest(args, start, 0));
    var forced = args;
    forced.partagg = true;
    // Forced partitions, one worker, one worker on a small decode budget.
    try testing.expectEqual(@as(usize, 3), try Walk.longest(forced, .{
        .a = forced,
        .chain = .{ .parts_tried = true },
        .may_fit = false,
        .cut = false,
        .plain = false,
        .scans = 0,
    }, 0));
    // Refused shared partitions leave the forced ones still to try, before one worker.
    var chain: Chain = .{ .parts_tried = false };
    const shared_retry = narrowed(args, args, S.initMany(&.{ .private_runs, .partitioned, .plain_rule }), &chain, true).?;
    try testing.expect(!shared_retry.private_runs and shared_retry.plain_rules and shared_retry.partition_cost);
    try testing.expectEqual(args.parallelism, shared_retry.parallelism);
    try testing.expect(!chain.parts_tried);
    const plain = narrowed(args, args, S.initMany(&.{ .partitioned, .worker_tables }), &chain, true).?;
    try testing.expect(!plain.partition_cost and !plain.partagg and plain.parallelism == 4);
    const parts = narrowed(args, plain, S.initMany(&.{ .worker_tables, .partitions_fit }), &chain, true).?;
    try testing.expect(parts.partagg and parts.parallelism == 4);
    const one = narrowed(args, parts, S.initMany(&.{.worker_tables}), &chain, true).?;
    try testing.expect(!one.partagg and one.parallelism == 1);
    try testing.expect(narrowed(args, one, .{}, &chain, true) == null);
    // Without copies of the same keys, one worker is not tried.
    chain = .{ .parts_tried = true };
    try testing.expect(narrowed(args, args, S.initMany(&.{.worker_tables}), &chain, false) == null);
    // A cost rule's plain grouping gives way to its cut, and nothing else changes.
    chain = .{ .parts_tried = false };
    const cut = narrowed(args, args, S.initMany(&.{ .plain_rule, .dense, .worker_tables }), &chain, false).?;
    try testing.expect(!cut.plain_rules and cut.dense and cut.parallelism == 4 and !chain.parts_tried);
    // Refused decode memory: one worker, without copies of the keys or any table at all; then a small decode budget
    // there; then the refusal is the query's.
    chain = .{ .parts_tried = true };
    const decoder = narrowed(args, args, S.initMany(&.{.decode_memory}), &chain, false).?;
    try testing.expect(decoder.parallelism == 1 and decoder.decode_limit.? == args.decode_limit.?);
    const lean = narrowed(args, decoder, S.initMany(&.{.decode_memory}), &chain, false).?;
    try testing.expect(lean.parallelism == 1 and !lean.charge_decode);
    try testing.expectEqual(core_memory.lean_decode / 2, lean.decode_limit.?);
    try testing.expect(chain.lean_tried);
    try testing.expect(narrowed(args, lean, S.initMany(&.{.decode_memory}), &chain, false) == null);
    // A refused merge: one worker, whose one table needs none; its own refusal is the query's.
    chain = .{ .parts_tried = true };
    const merged = narrowed(args, args, S.initMany(&.{.merge_memory}), &chain, false).?;
    try testing.expect(merged.parallelism == 1 and merged.charge_decode);
    try testing.expect(narrowed(args, merged, .{}, &chain, false) == null);
}

test "runMultiAggregate: a refusal says one worker may fit only where tables held copies or one worker plans a cut" {
    const gpa = testing.allocator;
    const top10: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    for ([_]struct { path: []const u8, order: expr_ast.ResultOrder = .{}, may_fit: bool }{
        // Every key in every row group.
        .{ .path = "ci/fixtures/parquet/repeat_keys.parquet", .may_fit = true },
        // Near-unique keys.
        .{ .path = "ci/fixtures/parquet/skew_filter.parquet", .may_fit = false },
        // The same, cut by their count: one worker's sketch cells fit where four workers' did not.
        .{ .path = "ci/fixtures/parquet/skew_filter.parquet", .order = top10, .may_fit = true },
    }) |case| {
        const bytes = (try loadFixture(case.path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        var taken: std.EnumSet(FastPath) = .{};
        const refused = prepareAggregate(gpa, arena.allocator(), .{
            .inputs = &.{.{ .name = case.path, .bytes = bytes }},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .result_order = case.order,
            .sketch_topk = .{ .cell_bits = 12 },
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .max_memory = 256 << 10,
            .dense = false,
            .spill = false,
            .partition_cost = false,
            .taken = &taken,
        }, false);
        if (refused) |r| freeResult(r) else |e| try testing.expectEqual(error.ExceededMemoryBudget, e);
        try testing.expect(taken.contains(.worker_tables));
        try testing.expectEqual(case.may_fit, taken.contains(.one_table_may_fit));
    }
}

test "regression: counts pairs folded merge in place or under a count cut, as the sets' merge leaves them" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // A full merge (in place), a cut by the count(DISTINCT) (`mergeCountCut`), and payloads beside it.
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 };
    for ([_]struct { aggregate: []const u8, order: expr_ast.ResultOrder }{
        .{ .aggregate = "count(DISTINCT u) AS d", .order = .{} },
        .{ .aggregate = "count(DISTINCT u) AS d, count(*) AS c, sum(u) AS s", .order = .{} },
        .{ .aggregate = "count(DISTINCT u) AS d", .order = top },
        .{ .aggregate = "count(DISTINCT u) AS d, min(u) AS m", .order = top },
    }) |case| for ([_]usize{ 1, 3, 8 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var results: [2]MultiAggResult = undefined;
        for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .on }, &results) |pairs, *res| {
            res.* = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = case.aggregate,
                .group_by = "t",
                .result_order = case.order,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .distinct_pairs = pairs,
            });
        }
        defer for (results) |r| freeResult(r);
        try testing.expect(!results[0].distinct_pairs and results[1].distinct_pairs);
        const sets = results[0].output.group_rows.?;
        const counted = results[1].output.group_rows.?;
        try testing.expectEqual(sets.len, counted.len);
        for (sets, counted) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
    };
}



test "regression: count(DISTINCT) pairs pay by the values the sets took, a filter's dropped rows left out" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // Seven groups of near-unique values; the filter drops one row in eight, which the rows read counted.
    var results: [2]MultiAggResult = undefined;
    for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .auto }, &results) |pairs, *res| {
        res.* = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(DISTINCT u) AS d",
            .group_by = "t",
            .filter = "n = 1",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .distinct_pairs = pairs,
            .pairs_pilot_rows = 1000,
            .ndv_policy = false,
        });
    }
    defer for (results) |r| freeResult(r);
    try testing.expect(results[1].distinct_pairs);
    const sets = results[0].output.group_rows.?;
    try testing.expectEqual(sets.len, results[1].output.group_rows.?.len);
    for (sets, results[1].output.group_rows.?) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
}



test "a scalar query's string winner copy is drawn before it is made, so a refusal holds no copy past the ledger" {
    const path = "ci/fixtures/parquet/scalar_strings.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // Each row group's 1 MiB value is a new winner of all three aggregates: three copies, one fold. Drawn only after
    // the fold, the third copy was made before the ledger refused it (and at 40 MiB a value, past the host's memory).
    for ([_]usize{ 1, 3 }) |j| {
        var ledger: core_memory.SharedBudget = .{ .limit = (5 << 20) / 2 };
        var gpa: LivePeak = .{ .child = testing.allocator };
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS a, max(s) AS b, min(s) AS c",
            .parallelism = j,
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .decode_limit = ledger.limit / 2,
            .min_bytes_per_worker = 1,
        };
        const refused = runMultiAggregate(gpa.allocator(), arena_state.allocator(), args);
        try testing.expectError(error.ExceededMemoryBudget, refused);
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), gpa.live.load(.monotonic));
        if (j == 1) try testing.expect(gpa.peak.load(.monotonic) < 3 << 20);
    }
}

test "a scalar query's string winners are drawn from the query's ledger: past it the query refuses, holding nothing" {
    const path = "ci/fixtures/parquet/scalar_strings.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        // Four row groups' winners of 1 MiB each, held until the merge, do not fit 3 MiB beside each other.
        var tight: core_memory.SharedBudget = .{ .limit = 3 << 20 };
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS m",
            .parallelism = j,
            .max_memory = tight.limit,
            .ledger = &tight,
            .decode_limit = tight.limit / 2,
            .min_bytes_per_worker = 1,
        };
        const refused = runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        try testing.expectError(error.ExceededMemoryBudget, refused);
        try testing.expectEqual(@as(usize, 0), tight.used.load(.monotonic));
        // With room for them the answer is the least string, and what stays drawn is its winner and its copy.
        var roomy: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var wide = args;
        wide.max_memory = roomy.limit;
        wide.ledger = &roomy;
        wide.decode_limit = roomy.limit / 2;
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), wide);
        defer freeResult(res);
        const got = res.output.aggs[0].value.s;
        try testing.expectEqual(@as(usize, 1 << 20), got.len);
        try testing.expectEqual(@as(u8, 'a'), got[0]);
        try testing.expect(roomy.used.load(.monotonic) >= 2 << 20);
    }
}

test "partagg: admitted long composite counts match shared partitions after a spawn failure" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    // Repeat a small fixture to cross the planning row gate; a short probe makes the first local window unique.
    var inputs: [48]Input = undefined;
    for (&inputs) |*in| in.* = .{ .name = full_match_fixture, .bytes = bytes };
    for ([_]usize{ 3, 16 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var taken: std.EnumSet(FastPath) = .{};
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = "s, ts",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .part_cap = 8,
            .part_probe = 8,
            .private_runs = false,
        };
        const want = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(want);
        args.private_runs = true;
        args.taken = &taken;
        const got = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(got);
        try testing.expect(taken.contains(.private_runs));
        try expectSameAnswer(want, got);
        spawn_util.injectFailureAfter(1);
        defer spawn_util.resetFailure();
        const failed = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(failed);
        try testing.expect(spawn_util.failuresInjected() > 0);
        try expectSameAnswer(want, failed);
    }
}

test "stream decode probes release failed arenas and finish before the source gate returns its bytes" {
    const path = "ci/fixtures/parquet/plain_values_corrupt.parquet";
    const original = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(original);
    var meta = try metadata.open(testing.allocator, original);
    defer meta.deinit(testing.allocator);
    const metas = [_]schema.FileMetaData{meta};
    const Gate = struct {
        data: []u8,
        returned: std.atomic.Value(usize) = .init(0),
        fn wait(_: *anyopaque, _: usize) anyerror!void {}
        fn done(ctx: *anyopaque, _: usize, items: usize) void {
            const g: *@This() = @ptrCast(@alignCast(ctx));
            if (g.returned.fetchAdd(1, .acq_rel) + 1 == items) @memset(g.data, 0);
        }
    };
    for ([_]usize{ 1, 2, 4, 16 }) |j| for ([_]usize{ 64 << 10, 1 << 20 }) |limit| for ([_]bool{ false, true }) |provided| {
        const data = try testing.allocator.dupe(u8, original);
        defer testing.allocator.free(data);
        var gate: Gate = .{ .data = data };
        const inputs = [_]Input{.{
            .name = "s3://fixture/corrupt.parquet",
            .bytes = data,
            .gate = .{ .ctx = &gate, .wait = Gate.wait, .done = Gate.done },
        }};
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        var ledger: core_memory.SharedBudget = .{ .limit = limit };
        var diag: Diag = .{};
        const result = runMultiAggregate(testing.allocator, arena.allocator(), .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = "min(s) AS lo,max(s) AS hi",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .max_memory = limit,
            .decode_limit = limit / 2,
            .ledger = if (provided) &ledger else null,
            .diag = &diag,
            .scan_all = true,
        });
        try testing.expectError(error.UnexpectedPage, result);
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        try testing.expect(ledger.peak.load(.monotonic) <= limit);
        try testing.expect(gate.returned.load(.monotonic) > 0);
        try testing.expect(std.mem.allEqual(u8, data, 0));
        try testing.expectEqualStrings("s", diag.detail.decode.column.get());
        try testing.expectEqual(@as(?u32, 0), diag.detail.decode.page);
    };
}

test "optional filter sampling declines corrupt values so the full projection chooses the failure" {
    const path = "ci/fixtures/parquet/plain_values_corrupt.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const metas = [_]schema.FileMetaData{try metadata.open(a, bytes)};
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};
    const filter = try filter_parser.parse(a, "s = 'a'", &metas[0], null);
    const plan: agg_plan.Plan = try .init(a, .{ .filter = filter, .calls = &calls, .group_keys = null }, &metas, .fromFlags(true, false));
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 1,
        .columns = plan.columns,
        .accumulators = &.{},
    }};
    try testing.expectEqual(@as(?f64, 1), try sampledPassShare(testing.allocator, a, &plan, &metas, &inputs, &work, .{ .fast_levels = true }, null));
}

test "worker refusal races replay an earlier corrupt row group in either completion order" {
    const bytes = (try loadFixture("ci/fixtures/parquet/plain_values_corrupt.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try metadata.open(a, bytes);
    const rgs = try a.alloc(schema.RowGroup, 2);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 2;
    const metas = [_]schema.FileMetaData{meta};
    const inputs = [_]Input{.{ .name = "race.parquet", .bytes = bytes }};
    const work = [_]WorkItem{
        .{ .file = 0, .rg = 0, .agg_start = 0, .agg_len = 1, .columns = undefined, .accumulators = &.{} },
        .{ .file = 0, .rg = 1, .agg_start = 0, .agg_len = 1, .columns = undefined, .accumulators = &.{} },
    };
    const Publisher = struct {
        fn run(w: *Worker, turn: *std.atomic.Value(u32), rank: u32, err: anyerror) void {
            while (true) {
                const seen = turn.load(.acquire);
                if (seen > rank) return; // A failed thread spawn cancels the other publisher.
                if (seen == rank) break;
                blocking.io.futexWaitUncancelable(u32, &turn.raw, seen);
            }
            w.err = err;
            turn.store(rank + 1, .release);
            blocking.io.futexWake(u32, &turn.raw, std.math.maxInt(u32));
        }
    };
    for (0..2) |refusal_worker| for (0..2) |first_completion| {
        var cursor: std.atomic.Value(usize) = .init(0);
        var workers: [2]Worker = undefined;
        for (&workers, 0..) |*w, i| w.* = .{
            .gpa = testing.allocator,
            .inputs = &inputs,
            .metas = &metas,
            .work = &work,
            .cursor = &cursor,
            .agg_calls = &.{},
            .plan = undefined,
            .decode_options = .{},
            .failure_work = if (i == refusal_worker) 0 else 1,
            // A refusal may retain the previous decoder's marker. Its work item is authoritative.
            .failure = .{ .rg = &rgs[1], .column = 1, .first_row = 0, .projection = 0 },
        };
        var turn: std.atomic.Value(u32) = .init(0);
        const t0 = try std.Thread.spawn(.{}, Publisher.run, .{
            &workers[0],                                                                                  &turn, @as(u32, @intCast(first_completion)),
            @as(anyerror, if (refusal_worker == 0) error.ExceededMemoryBudget else error.UnexpectedPage),
        });
        const t1 = std.Thread.spawn(.{}, Publisher.run, .{
            &workers[1],                                                                                  &turn, @as(u32, @intCast(1 - first_completion)),
            @as(anyerror, if (refusal_worker == 1) error.ExceededMemoryBudget else error.UnexpectedPage),
        }) catch |err| {
            turn.store(2, .release);
            blocking.io.futexWake(u32, &turn.raw, std.math.maxInt(u32));
            t0.join();
            return err;
        };
        t0.join();
        t1.join();
        const refusal = firstWorkerFailure(&workers, null).?;
        try testing.expectEqual(error.ExceededMemoryBudget, refusal.err);
        try testing.expect(refusal.site == null);
        try clearWorkerErrors(&workers, error.ExceededMemoryBudget);
        try testing.expect(phaseFailure(&workers) == null);
        workers[refusal_worker].err = error.CensusAbandoned;
        workers[1 - refusal_worker].err = error.UnexpectedPage;
        try testing.expectEqual(error.CensusAbandoned, phaseFailure(&workers).?);
        try clearWorkerErrors(&workers, error.CensusAbandoned);
        try testing.expect(phaseFailure(&workers) == null);
        workers[refusal_worker].err = error.ExceededMemoryBudget;
        workers[1 - refusal_worker].err = error.UnexpectedPage;
        // An unlocated refusal is equally conservative, including a refusal before any decoder starts.
        workers[refusal_worker].failure_work = null;
        try testing.expectEqual(error.ExceededMemoryBudget, firstWorkerFailure(&workers, null).?.err);
        // A known later refusal must not suppress an earlier confirmed decode error.
        workers[refusal_worker].failure_work = 1;
        workers[1 - refusal_worker].failure_work = 0;
        workers[1 - refusal_worker].failure.?.rg = &rgs[0];
        try testing.expectEqual(error.UnexpectedPage, firstWorkerFailure(&workers, null).?.err);

        workers[refusal_worker].err = error.EarlierFailure;
        workers[refusal_worker].failure_work = 0;
        workers[1 - refusal_worker].err = error.LaterFailure;
        workers[1 - refusal_worker].failure_work = 1;
        try testing.expectError(error.EarlierFailure, clearWorkerErrors(&workers, error.CensusAbandoned));

        var chain: Chain = .{ .parts_tried = false };
        var diag: Diag = .{};
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = "min(s)",
            .parallelism = 2,
            .min_bytes_per_worker = 1,
            .decode_limit = 1 << 20,
            .max_memory = 2 << 20,
            .scan_all = true,
            .diag = &diag,
        };
        const replay = narrowed(args, args, std.EnumSet(FastPath).initMany(&.{.decode_memory}), &chain, false).?;
        try testing.expectEqual(@as(usize, 1), replay.parallelism);
        try testing.expectError(error.UnexpectedPage, runMultiAggregate(testing.allocator, a, replay));
        try testing.expectEqual(@as(usize, 0), diag.detail.decode.row_group);
        try testing.expectEqual(@as(?u32, 0), diag.detail.decode.page);
        try testing.expectEqualStrings("s", diag.detail.decode.column.get());
    };
}

test "packed byte SUM selection matches the established rows across ties, offsets, windows and workers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 4000;
    const keys = try a.alloc([]const u8, n);
    const defs = try a.alloc(u32, n);
    const values = try a.alloc(i64, n);
    const vdefs = try a.alloc(u32, n);
    var dictionary: [103][]const u8 = undefined;
    for (&dictionary, 0..) |*key, i| key.* = switch (i) {
        0 => "ignored",
        1 => "",
        2 => "\x00",
        3 => "a\x00",
        4 => "0123456789abcde",
        else => try std.fmt.allocPrint(a, "key-{d:0>6}", .{i}),
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, bytes);
        }
    };
    for (0..3) |shape| {
        for (keys, defs, values, vdefs, 0..) |*key, *def, *v, *vd, i| {
            const g = (i * 37) % dictionary.len;
            key.* = dictionary[g];
            def.* = @intFromBool(g != 0);
            vd.* = @intFromBool(g != 6 and i % 19 != 0);
            v.* = switch (shape) {
                0 => @as(i64, @intCast((i * 31) % 101)) - 50,
                1 => 1,
                else => if (i % 2 == 0) std.math.maxInt(i64) else -1234567,
            };
        }
        const columns = [_]expr_agg.OutputCol{
            .{
                .name = "k",
                .col = .{ .string = .{ .values = keys, .def_levels = defs, .max_def = 1 } },
                .parquet_type = .BYTE_ARRAY,
                .nullable = true,
            },
            .{
                .name = "v",
                .col = .{ .i64 = .{ .values = values, .def_levels = vdefs, .max_def = 1 } },
                .parquet_type = .INT64,
                .nullable = true,
            },
        };
        var capture: Capture = .{ .a = a };
        _ = try consumer.writeAggregateRows(
            a,
            .{ .ctx = &capture, .write_fn = Capture.write },
            &columns,
            n,
            .SNAPPY,
            .{},
            .{},
        );
        var meta = try metadata.open(a, capture.bytes.items);
        const groups = try a.alloc(schema.RowGroup, 16);
        @memset(groups, meta.row_groups.items[0]);
        meta.row_groups.items = groups;
        meta.num_rows *= 16;
        const inputs = [_]Input{.{ .name = "packed-keys.parquet", .bytes = capture.bytes.items }};
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "sum(v) AS s",
            .group_by = "k",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .max_memory = 512 << 20,
            .integer_records = false,
            .msd_records = false,
            .result_order = .{
                .keys = &.{.{ .column = 1, .descending = true }},
                .limit = 7,
                .offset = 3,
            },
        };
        if (shape == 0) for ([_]usize{ 1, 16 }) |j| for ([_]u64{ 0, 7 }) |offset| {
            var args = base;
            var taken: std.EnumSet(FastPath) = .{};
            args.parallelism = j;
            args.byte_records = true;
            args.byte_records_min_rows = 0;
            args.taken = &taken;
            args.result_order.limit = 0;
            args.result_order.offset = offset;
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expect(explanation.record_path == null);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(!taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqual(@as(usize, 0), got.output.group_rows.?.len);
        };
        for ([_]bool{ false, true }) |descending| {
            var ordinary = base;
            ordinary.result_order.keys = &.{.{ .column = 1, .descending = descending }};
            var want = try runMultiAggregate(gpa, a, ordinary);
            defer want.deinit(gpa);
            try want.output.applyResultOrder(gpa, ordinary.result_order);
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]bool{ false, true }) |encoded|
                for ([_]usize{ 0, 64 }) |batch_rows| {
                    var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
                    var taken: std.EnumSet(FastPath) = .{};
                    var args = ordinary;
                    args.parallelism = j;
                    args.encoded = encoded;
                    args.byte_records = true;
                    args.byte_records_min_rows = 0;
                    args.byte_top_partitions = 256;
                    args.byte_top_batch_rows = batch_rows;
                    args.unordered = j > 1;
                    args.ledger = &budget;
                    args.taken = &taken;
                    const explanation = try explainAggregate(gpa, a, args);
                    try testing.expectEqual(.byte_sum_top_h, explanation.record_path.?.layout);
                    var got = try prepareAggregate(gpa, a, args, false);
                    defer got.deinit(gpa);
                    try testing.expect(taken.contains(.integer_records));
                    try got.output.applyResultOrder(gpa, args.result_order);
                    try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
                    try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
                    try testing.expect(budget.used.load(.monotonic) < 8192);
                };
        }
        if (shape == 0) {
            const corrupt = try a.dupe(u8, capture.bytes.items);
            for (meta.row_groups.items[0].columns.items) |column| {
                const cm = column.meta_data.?;
                const offset: usize = @intCast(cm.data_page_offset);
                var reader = thrift_mod.Reader.init(corrupt[offset..]);
                const header = try schema.PageHeader.read(&reader);
                const body = offset + reader.pos;
                @memset(corrupt[body..][0..@min(12, @as(usize, @intCast(header.compressed_page_size)))], 0xff);
            }
            const bad_inputs = [_]Input{.{ .name = "packed-errors.parquet", .bytes = corrupt }};
            var bad = base;
            bad.inputs = &bad_inputs;
            bad.byte_records = true;
            bad.byte_records_min_rows = 0;
            try checkRecordDecodeModes(gpa, a, bad, .integer_records, 0);
            bad.column_order = "s,k";
            bad.result_order.keys = &.{.{ .column = 0, .descending = true }};
            try checkRecordDecodeModes(gpa, a, bad, .integer_records, 1);
        }
    }
}

test "retained MSD composite count matches ordered rows across workers, encodings and budgets" {
    try testing.expect(!consumer.isDecodeError(error.RetainedOwnershipViolation));
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 40000;
    const keys = try a.alloc([]const u8, n);
    const defs = try a.alloc(u32, n);
    const ints = try a.alloc(i32, n);
    const fdefs = try a.alloc(u32, n);
    var dictionary: [200][]const u8 = undefined;
    for (&dictionary, 0..) |*key, i| {
        const len = if (i == 1) 0 else (i * 37) % 400;
        const b = try a.alloc(u8, len);
        for (b, 0..) |*v, j| v.* = @truncate(i * 73 + j * 13);
        if (len >= 20) @memcpy(b[0..20], "https://example.org/");
        key.* = b;
    }
    for (keys, defs, ints, fdefs, 0..) |*key, *d, *v, *fd, i| {
        const group = (i * 37) % dictionary.len;
        key.* = dictionary[group];
        d.* = @intFromBool(group % 19 != 0);
        v.* = @as(i32, @intCast(group)) * 100003 - 7000000;
        fd.* = @intFromBool(group % 23 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .string = .{ .values = keys, .def_levels = defs, .max_def = 1 } }, .parquet_type = .BYTE_ARRAY, .nullable = true },
        .{ .name = "f", .col = .{ .i32 = .{ .values = ints, .def_levels = fdefs, .max_def = 1 } }, .parquet_type = .INT32, .nullable = true },
    };
    const Capture = struct {
        a: std.mem.Allocator,
        bytes: std.ArrayList(u8) = .empty,
        fn write(ctx: *anyopaque, data: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, data);
        }
    };
    for ([_]schema.CompressionCodec{ .SNAPPY, .UNCOMPRESSED }) |codec| {
        var capture: Capture = .{ .a = a };
        _ = try consumer.writeAggregateRows(a, .{ .ctx = &capture, .write_fn = Capture.write }, &cols, n, codec, .{}, .{});
        var meta = try metadata.open(a, capture.bytes.items);
        const rgs = try a.alloc(schema.RowGroup, 16);
        @memset(rgs, meta.row_groups.items[0]);
        // Reusing an immutable chunk gives sixteen independent logical row groups without a large fixture file.
        meta.row_groups.items = rgs;
        meta.num_rows = 16 * n;
        const inputs = [_]Input{.{ .name = "composite.parquet", .bytes = capture.bytes.items }};
        const base: MultiAggArgs = .{ .inputs = &inputs, .metas = &.{meta}, .aggregate = "count(*) AS c", .group_by = "k,f", .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 10 }, .max_memory = 512 << 20, .parallelism = 1, .min_bytes_per_worker = 1, .msd_records = false };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 96 << 20, 512 << 20 }) |limit| for ([_]bool{ false, true }) |encoded| {
            var budget: core_memory.SharedBudget = .{ .limit = limit };
            defer std.debug.assert(budget.used.load(.monotonic) < 1 << 20);
            var args = base;
            args.parallelism = j;
            args.max_memory = limit;
            args.ledger = &budget;
            args.msd_records = true;
            args.msd_records_min_rows = 0;
            args.encoded = encoded;
            const explanation = try explainAggregate(gpa, a, args);
            if (limit == 512 << 20) {
                try testing.expectEqual(@as(usize, j), explanation.workers);
                try testing.expectEqual(.msd_count_top_h, explanation.record_path.?.layout);
            } else try testing.expect(explanation.record_path == null);
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        };
        {
            var relaxed = base;
            relaxed.msd_records = true;
            relaxed.msd_records_min_rows = 0;
            relaxed.parallelism = 16;
            relaxed.unordered = true;
            const explanation = try explainAggregate(gpa, a, relaxed);
            try testing.expectEqual(.msd_count_top_h, explanation.record_path.?.layout);
            var got = try runMultiAggregate(gpa, a, relaxed);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, relaxed.result_order);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        }
        if (codec == .SNAPPY) {
            // An earlier secondary-column page wins over the late key page in every decode/admission mode.
            // Keep its header valid so planning admits MSD and the failure actually reaches record workers.
            var plain_capture: Capture = .{ .a = a };
            try plain_capture.bytes.appendSlice(a, capture.bytes.items);
            var plain_meta = try metadata.open(a, capture.bytes.items);
            const encoder = @import("writer/encoder.zig");
            var key_plan = try encoder.plan(a, .{ .values = cols[0].col, .schema_elem = &plain_meta.schema.items[1], .path_in_schema = &.{"k"}, .codec = codec, .distinct = true });
            key_plan.values_encoding = .PLAIN;
            const key_pages = try a.alloc(encoder.BuiltPage, key_plan.pageCount());
            for (key_pages, 0..) |*pg, i| pg.* = try key_plan.page(a, i);
            var encoded_key = try key_plan.finish(a, key_pages);
            encoded_key.meta.data_page_offset += @intCast(plain_capture.bytes.items.len);
            try plain_capture.bytes.appendSlice(a, encoded_key.bytes);
            // Supplied metadata permits a compact synthetic input: the replacement chunk follows the old footer.
            plain_meta.row_groups.items[0].columns.items[0].meta_data = encoded_key.meta;
            const key_cm = plain_meta.row_groups.items[0].columns.items[0].meta_data.?;
            const f_cm = plain_meta.row_groups.items[0].columns.items[1].meta_data.?;
            try testing.expect(key_cm.dictionary_page_offset == null);
            const key_end: usize = @intCast(key_cm.data_page_offset + key_cm.total_compressed_size);
            @memset(plain_capture.bytes.items[key_end - 32 .. key_end], 0xff);
            const f_start: usize = @intCast(f_cm.data_page_offset);
            var f_header = thrift_mod.Reader.init(plain_capture.bytes.items[f_start..]);
            const fh = try schema.PageHeader.read(&f_header);
            const f_body = f_start + f_header.pos;
            const damaged = @min(12, @as(usize, @intCast(fh.compressed_page_size)));
            @memset(plain_capture.bytes.items[f_body..][0..damaged], 0xff);
            const plain_rgs = try a.alloc(schema.RowGroup, 16);
            @memset(plain_rgs, plain_meta.row_groups.items[0]);
            plain_meta.row_groups.items = plain_rgs;
            plain_meta.num_rows = n * 16;
            const bad_inputs = [_]Input{.{ .name = "late-key-error.parquet", .bytes = plain_capture.bytes.items }};
            var bad = base;
            bad.inputs = &bad_inputs;
            bad.metas = &.{plain_meta};
            try checkRecordDecodeModes(gpa, a, bad, .msd_records, 1);
        }
        {
            // A large LIMIT can require more owned output than the fixed floor. Reject its conservative bound
            // before consumption, even when the actual answer later turns out small enough for the old path.
            var large = base;
            large.result_order.limit = 1000;
            var expected = try runMultiAggregate(gpa, a, large);
            defer expected.deinit(gpa);
            try expected.output.applyResultOrder(gpa, large.result_order);
            large.msd_records = true;
            large.msd_records_min_rows = 0;
            const explanation = try explainAggregate(gpa, a, large);
            try testing.expect(explanation.record_path == null);
            var got = try runMultiAggregate(gpa, a, large);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, large.result_order);
            try testing.expectEqualDeep(expected.output.group_rows.?, got.output.group_rows.?);
        }
        if (codec == .SNAPPY) {
            // The footer lies about a late chunk: the established reader follows its DELTA page headers. MSD
            // must refuse before copying expansion, then replay after earlier groups have already been emitted.
            const encoder = @import("writer/encoder.zig");
            var late: Capture = .{ .a = a };
            try late.bytes.appendSlice(a, capture.bytes.items);
            var lied = try metadata.open(a, capture.bytes.items);
            var delta = try encoder.plan(a, .{ .values = cols[0].col, .schema_elem = &lied.schema.items[1], .path_in_schema = &.{"k"}, .codec = codec, .distinct = true });
            delta.values_encoding = .DELTA_BYTE_ARRAY;
            const pages = try a.alloc(encoder.BuiltPage, delta.pageCount());
            for (pages, 0..) |*p, i| p.* = try delta.page(a, i);
            var encoded = try delta.finish(a, pages);
            encoded.meta.data_page_offset += @intCast(late.bytes.items.len);
            encoded.meta.encodings.items = try a.dupe(schema.Encoding, &.{ .PLAIN, .RLE });
            encoded.meta.encoding_stats = null;
            try late.bytes.appendSlice(a, encoded.bytes);
            const late_rgs = try a.alloc(schema.RowGroup, 16);
            @memset(late_rgs, lied.row_groups.items[0]);
            late_rgs[15].columns.items = try a.dupe(schema.ColumnChunk, lied.row_groups.items[0].columns.items);
            late_rgs[15].columns.items[0].meta_data = encoded.meta;
            lied.row_groups.items = late_rgs;
            lied.num_rows = n * 16;
            const late_inputs = [_]Input{.{ .name = "late-encoding.parquet", .bytes = late.bytes.items }};
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
                var budget: core_memory.SharedBudget = .{ .limit = 1 << 30 };
                var args = base;
                args.inputs = &late_inputs;
                args.metas = &.{lied};
                args.max_memory = budget.limit;
                args.ledger = &budget;
                args.parallelism = j;
                args.msd_records = true;
                args.msd_records_min_rows = 0;
                const explanation = try explainAggregate(gpa, a, args);
                try testing.expectEqual(.msd_count_top_h, explanation.record_path.?.layout);
                var taken: std.EnumSet(FastPath) = .{};
                args.taken = &taken;
                try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
                try testing.expect(taken.contains(.msd_records));
                try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            }
        }
        const Coordinator = struct {
            started: bool = false,
            planned: usize = 0,
            loan: usize = 0,
            forced: std.atomic.Value(bool) = .init(false),
            budget: *core_memory.SharedBudget,
            remote: bool,
            abandoned: usize = 0,
            fn start(ctx: *anyopaque) anyerror!void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                c.started = true;
            }
            fn plannedRead(ctx: *anyopaque) void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                c.planned += 1;
            }
            fn probe(ctx: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                try testing.expect(!c.started);
            }
            fn waitProbe(ctx: *anyopaque, _: usize) anyerror!void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                try testing.expect(c.started);
            }
            fn abandon(ctx: *anyopaque) void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                c.abandoned += 1;
                c.budget.release(c.loan);
                c.loan = 0;
            }
        };
        const Window = struct {
            bytes: []u8,
            coordinator: *Coordinator,
            waits: usize = 0,
            done: usize = 0,
            refuse_at: ?usize = null,
            fn wait(ctx: *anyopaque, _: usize) anyerror!void {
                const w: *@This() = @ptrCast(@alignCast(ctx));
                const c = w.coordinator;
                try testing.expect(c.started);
                w.waits += 1;
                if (w.refuse_at) |at| if (w.waits == at) return error.ExceededMemoryBudget;
                if (!c.remote and c.forced.cmpxchgStrong(false, true, .monotonic, .monotonic) == null) {
                    const used = c.budget.used.load(.monotonic);
                    try testing.expect(used > 64 << 20);
                    c.loan = c.budget.limit - used;
                    c.budget.force(c.loan);
                }
            }
            fn release(ctx: *anyopaque, _: usize, _: usize) void {
                const w: *@This() = @ptrCast(@alignCast(ctx));
                w.done += 1;
                @memset(w.bytes, 0);
            }
        };
        var single = meta;
        single.row_groups.items = rgs[0..1];
        single.num_rows = n;
        for ([_]bool{ false, true }) |remote| {
            var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
            var coordinator: Coordinator = .{ .budget = &budget, .remote = remote };
            var windows = [_]Window{
                .{ .bytes = try a.dupe(u8, capture.bytes.items), .coordinator = &coordinator },
                .{ .bytes = try a.dupe(u8, capture.bytes.items), .coordinator = &coordinator },
            };
            var window_inputs: [2]Input = undefined;
            for (&window_inputs, &windows) |*input, *window| input.* = .{
                .name = if (remote) "s3://bucket/window.parquet" else "window.parquet",
                .bytes = window.bytes,
                .plan_bytes = if (remote) null else capture.bytes.items,
                .gate = .{ .ctx = window, .wait = Window.wait, .done = Window.release },
                .dict_wait = .{ .ctx = &coordinator, .wait = Coordinator.waitProbe },
            };
            var args = base;
            args.inputs = &window_inputs;
            args.metas = &.{ single, single };
            args.parallelism = 2;
            args.msd_records = true;
            args.msd_records_min_rows = 0;
            args.ledger = &budget;
            args.start_reads = .{ .ctx = &coordinator, .start = Coordinator.start, .planned = Coordinator.plannedRead };
            args.dict_probe = .{ .ctx = &coordinator, .add = Coordinator.probe };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            budget.release(coordinator.loan);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqual(@as(usize, 1), coordinator.planned);
            for (windows) |window| {
                try testing.expectEqual(@as(usize, 1), window.waits);
                try testing.expectEqual(@as(usize, 1), window.done);
            }
            for (want.output.group_rows.?, got.output.group_rows.?) |expected, actual| {
                try testing.expectEqualDeep(expected[0..2], actual[0..2]);
                try testing.expectEqual(expected[2].i, actual[2].i * 8);
            }
            try testing.expect(budget.used.load(.monotonic) < 1 << 20);
        }
        {
            // The first row group has emitted retained records before the next worker admission refuses.
            // The retry must abandon the old window, release every charge, and read the original mapping.
            var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
            var coordinator: Coordinator = .{ .budget = &budget, .remote = false };
            var window: Window = .{ .bytes = try a.dupe(u8, capture.bytes.items), .coordinator = &coordinator, .refuse_at = 2 };
            const replay = [_]Input{.{ .name = "replay.parquet", .bytes = window.bytes, .plan_bytes = capture.bytes.items, .gate = .{ .ctx = &window, .wait = Window.wait, .done = Window.release }, .dict_wait = .{ .ctx = &coordinator, .wait = Coordinator.waitProbe } }};
            var args = base;
            args.inputs = &replay;
            args.msd_records = true;
            args.msd_records_min_rows = 0;
            args.ledger = &budget;
            args.start_reads = .{ .ctx = &coordinator, .start = Coordinator.start, .planned = Coordinator.plannedRead, .abandon = Coordinator.abandon };
            args.dict_probe = .{ .ctx = &coordinator, .add = Coordinator.probe };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqual(@as(usize, 1), coordinator.abandoned);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            try testing.expect(budget.used.load(.monotonic) < 1 << 20);
        }
    }
}



test "inline integer decode errors follow projection ties across windows, decline and mapped replay" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const values = try a.alloc(i64, 40000);
    const defs = try a.alloc(u32, values.len);
    for (values, defs, 0..) |*v, *d, i| {
        v.* = @intCast(i % 503);
        d.* = @intFromBool(i % 11 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "n",
            .col = .{ .i64 = .{ .values = values, .def_levels = defs, .max_def = 1 } },
            .parquet_type = .INT64,
            .nullable = true,
        },
        .{
            .name = "v",
            .col = .{ .i64 = .{ .values = values, .def_levels = defs, .max_def = 1 } },
            .parquet_type = .INT64,
            .nullable = true,
        },
    };
    const Capture = struct {
        a: std.mem.Allocator,
        bytes: std.ArrayList(u8) = .empty,
        fn write(ctx: *anyopaque, data: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, data);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &cols,
        @intCast(values.len),
        .UNCOMPRESSED,
        .{},
        .{},
    );
    const bad = capture.bytes.items;
    var meta = try metadata.open(a, bad);
    var key: usize = 0;
    var value: usize = 0;
    for (meta.row_groups.items[0].columns.items, 0..) |chunk, ci| {
        const cm = chunk.meta_data.?;
        const name = cm.path_in_schema.items[0];
        if (std.mem.eql(u8, name, "n")) key = ci else if (std.mem.eql(u8, name, "v")) value = ci else continue;
        var header = thrift_mod.Reader.init(bad[@intCast(cm.data_page_offset)..]);
        const page = try schema.PageHeader.read(&header);
        const body = @as(usize, @intCast(cm.data_page_offset)) + header.pos;
        @memset(bad[body..][0..@min(12, @as(usize, @intCast(page.compressed_page_size)))], 0xff);
    }
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows = rgs[0].num_rows * 16;
    const inputs = [_]Input{.{ .name = "integer-projection-errors.parquet", .bytes = bad }};
    var args: MultiAggArgs = .{
        .inputs = &inputs,
        .metas = &.{meta},
        .aggregate = "count(*) AS c, sum(v) AS s",
        .group_by = "n",
        .typed_answer = true,
        .min_bytes_per_worker = 1,
        .scan_all = true,
    };
    try checkRecordDecodeModes(gpa, a, args, .integer_records, key);
    args.column_order = "s,n,c";
    try checkRecordDecodeModes(gpa, a, args, .integer_records, value);
}

test "unordered keeps exact inline integer and byte answers and bounded SUM selection" {
    const bytes = (try loadFixture("ci/fixtures/parquet/topk_skew.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = "topk_skew.parquet", .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const order_keys = [_]expr_ast.OrderKey{.{ .column = 1, .descending = true }};
    for (0..3) |shape| for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = if (shape == 2) "sum(n) AS s" else "count(*) AS c,sum(n) AS s",
            .group_by = if (shape == 1) "k" else "n",
            .result_order = if (shape == 2) .{ .keys = &order_keys, .limit = 7, .offset = 3 } else .{},
            .max_memory = 512 << 20,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .typed_answer = true,
            .integer_records = true,
            .integer_records_min_rows = 0,
            .byte_records = true,
            .byte_records_min_rows = 0,
        };
        const ordered = try explainAggregate(testing.allocator, a, args);
        try testing.expect(ordered.record_path != null);
        var want = try prepareAggregate(testing.allocator, a, args, false);
        defer want.deinit(testing.allocator);
        args.unordered = true;
        const relaxed = try explainAggregate(testing.allocator, a, args);
        try testing.expectEqual(ordered.record_path.?.layout, relaxed.record_path.?.layout);
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        var got = try prepareAggregate(testing.allocator, a, args, false);
        defer got.deinit(testing.allocator);
        try testing.expect(taken.contains(.integer_records));
        if (shape == 0) {
            const expected = want.output.integer_stream.?;
            const actual = got.output.integer_stream.?;
            try testing.expectEqual(expected.state.group_count, actual.state.group_count);
            const hi = @min(expected.state.group_count, 137);
            var x = try expected.window(testing.allocator, want.state.group_col_types.?, 0, hi, j);
            defer x.deinit(testing.allocator);
            var y = try actual.window(testing.allocator, got.state.group_col_types.?, 0, hi, j);
            defer y.deinit(testing.allocator);
            try testing.expectEqualDeep(x.columns, y.columns);
        } else if (shape == 1) {
            const expected = want.output.byte_stream.?;
            const actual = got.output.byte_stream.?;
            try testing.expectEqual(expected.state.group_count, actual.state.group_count);
            const hi = @min(expected.state.group_count, 137);
            var x = try expected.window(testing.allocator, 0, hi, j);
            defer x.deinit(testing.allocator);
            var y = try actual.window(testing.allocator, 0, hi, j);
            defer y.deinit(testing.allocator);
            try testing.expectEqualDeep(x.columns, y.columns);
        } else {
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        }
    };
}

test "canonical full projection prefix includes count arguments and skipped admit items" {
    const bytes = (try loadFixture("ci/fixtures/parquet/plain_values_corrupt.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try metadata.open(a, bytes);
    const groups = try a.alloc(schema.RowGroup, 3);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    const metas = [_]schema.FileMetaData{meta};
    const inputs = [_]Input{.{ .name = "omitted.parquet", .bytes = bytes }};
    const key = try expr_parser.parseExprOnly(a, "b", &meta, null);
    const arg = try expr_parser.parseExprOnly(a, "s", &meta, null);
    const calls = [_]expr_agg.AggCall{.{ .func = .min, .arg = arg, .where = null, .alias = "lo", .result = .bytes }};
    const keys = [_]expr_ast.Expr{key};
    const plan = try agg_plan.Plan.init(a, .{ .calls = &calls, .group_keys = &keys, .filter = null }, &metas, .ignore);
    const count_columns = try plan.forCalls(a, &.{});
    try testing.expect(!count_columns.read[1]);
    const work = [_]WorkItem{.{ .file = 0, .rg = 2, .agg_start = 0, .agg_len = 1, .columns = count_columns, .accumulators = &.{} }};
    var w: Worker = undefined;
    w.plan = &plan;
    w.metas = &metas;
    w.inputs = &inputs;
    w.work = &work;
    w.failed_item = 0;
    w.failure = .{ .rg = &groups[2], .column = 1, .page = 0 };
    w.err = error.UnexpectedPage;
    w.decode_options = .{};
    w.agg_calls = &calls;
    w.group_by_keys = &keys;
    w.error_group_keys = null;
    w.error_outputs = null;
    // The worker sees only RG2, with a count-only projection. Neither controls the canonical replay.
    const found = canonicalFailurePrefix(testing.allocator, FailureProbe.contextOf(&w));
    try testing.expectEqual(error.UnexpectedPage, found.err);
    try testing.expectEqual(&groups[0], found.site.rg);
    try testing.expectEqual(@as(usize, 1), found.site.column);
    // Force actual count passes to encounter a later corrupt key after skipping the earlier corrupt argument.
    const last_columns = try a.dupe(schema.ColumnChunk, groups[2].columns.items);
    const old_b = last_columns[0].meta_data.?;
    last_columns[0].meta_data = last_columns[1].meta_data;
    last_columns[0].meta_data.?.type = .INT32;
    last_columns[0].meta_data.?.path_in_schema = old_b.path_in_schema;
    groups[2].columns.items = last_columns;
    const selected = try selectList(a, &.{ "b", "count(*) AS n", "min(s) AS lo" });
    for ([_]usize{ 1, 2, 4, 16 }) |j| for ([_]usize{ 64 << 10, 1 << 20 }) |limit| {
        for ([_]?bool{ null, false, true }) |fuse| {
            var diag: Diag = .{};
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .metas = &metas,
                .aggregate = "count(*) AS n,min(s) AS lo",
                .group_by = "b",
                .select_cols = selected,
                .scan_all = true,
                .ndv_policy = false,
                .plain_rules = false,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .max_memory = limit,
                .decode_limit = limit / 2,
                .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 1 },
                .sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = fuse },
                .diag = &diag,
            };
            const ex = try explainAggregate(testing.allocator, a, args);
            try testing.expectEqual(.sketch, ex.plan.dispatch.strategy);
            try testing.expectError(error.UnexpectedPage, runMultiAggregate(testing.allocator, a, args));
            try testing.expectEqual(@as(usize, 0), diag.detail.decode.row_group);
            try testing.expectEqualStrings("s", diag.detail.decode.column.get());
        }
    };
    // An earlier valid RG must contribute no invented marker. The next omitted RG still wins.
    const first_columns = try a.dupe(schema.ColumnChunk, groups[0].columns.items);
    first_columns[1].meta_data.?.num_values = 0;
    groups[0].columns.items = first_columns;
    const next = canonicalFailurePrefix(testing.allocator, FailureProbe.contextOf(&w));
    try testing.expectEqual(&groups[1], next.site.rg);
}

test "false but corroborated footer bounds pin the default and scan-all trust boundary" {
    const path = "ci/fixtures/parquet/false_bounds.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3, 16 }) |j| for ([_]bool{ false, true }) |scan_all| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        var result = try runMultiAggregate(testing.allocator, arena.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(v) AS s",
            .filter = "x = 100001",
            .scan_all = scan_all,
            .parallelism = j,
            .min_bytes_per_worker = 1,
        });
        defer result.deinit(testing.allocator);
        const got = result.output.aggs[0].value;
        if (scan_all) try testing.expectEqual(@as(i128, 20), got.i) else try testing.expect(got == .null_val);
    };
}

test "sketch census and first-key plans reject all gated inputs including mapped readers" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const Gate = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            unreachable;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    var marker: u8 = 0;
    var inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_][]const u8{ "count(*) AS c", "count(DISTINCT v) AS c" }) |aggregate| {
        for ([_]bool{ false, true }) |first| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .aggregate = aggregate,
                .group_by = "k",
                .select_cols = try selectList(a, &.{ "k", aggregate }),
                .ndv_policy = false,
                .result_order = .{
                    .keys = if (first) &.{} else &.{.{ .column = 1, .descending = true }},
                    .limit = 1,
                },
                .sketch_topk = .{ .fuse_census = true },
            };
            inputs[0].gate = null;
            inputs[0].plan_bytes = null;
            const ordinary = try explainAggregate(testing.allocator, a, args);
            const expected: @FieldType(AggregateDispatch, "strategy") = if (first) .first_keys else .sketch;
            try testing.expectEqual(expected, ordinary.plan.dispatch.strategy);
            if (!first) try testing.expect(ordinary.plan.dispatch.census_allowed);
            for ([_]bool{ false, true }) |mapped| {
                inputs[0].gate = .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done };
                inputs[0].plan_bytes = if (mapped) bytes else null;
                const gated = try explainAggregate(testing.allocator, a, args);
                try testing.expectEqual(.plain_hash, gated.plan.dispatch.strategy);
                try testing.expect(!gated.plan.dispatch.census_allowed);
                try testing.expect(!gated.plan.dispatch.census_fused);
                try testing.expect(!gated.plan.dispatch.distinct_census);
            }
        }
    }
}

test "stream probe funding follows projected chunks and retains the fixed option cap" {
    const bytes = (try loadFixture("ci/fixtures/parquet/plain_values_corrupt.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const metas = [_]schema.FileMetaData{try metadata.open(a, bytes)};
    const Gate = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            unreachable;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    var marker: u8 = 0;
    var inputs = [_]Input{.{
        .name = "stream",
        .bytes = &.{},
        .gate = .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done },
    }};
    const leaf = try expr_parser.parseExprOnly(a, "s", &metas[0], null);
    const calls = [_]expr_agg.AggCall{.{
        .func = .min,
        .arg = leaf,
        .alias = "m",
        .result = .bytes,
        .where = null,
    }};
    const plan = try agg_plan.Plan.init(a, .{ .filter = null, .calls = &calls, .group_keys = null }, &metas, .ignore);
    const quote = streamProbeBytes(&inputs, &metas, &plan);
    try testing.expect(quote > 0 and quote < core_memory.lean_decode / 2);
    var budget: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    var probe: FailureProbe = .{ .limit = budget.limit };
    try probe.reserveStream(&budget, quote);
    try testing.expectEqual(quote, budget.used.load(.monotonic));
    probe.releaseStream();
    // No projected payload and no non-rescannable source require no funding.
    const empty = try agg_plan.Plan.init(a, .{ .filter = null, .calls = &.{}, .group_keys = null }, &metas, .ignore);
    try testing.expectEqual(@as(usize, 0), streamProbeBytes(&inputs, &metas, &empty));
    inputs[0].plan_bytes = bytes;
    try testing.expectEqual(@as(usize, 0), streamProbeBytes(&inputs, &metas, &plan));
    // The plan may request arbitrarily much; the quota is still independent of workers and live ledger room.
    try probe.reserveStream(&budget, std.math.maxInt(usize));
    try testing.expectEqual(core_memory.lean_decode / 2, budget.used.load(.monotonic));
    probe.releaseStream();
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "writer statistics: rewritten footers suppress pruning page indexes and trusted aggregate answers" {
    const path = "ci/fixtures/parquet/writer_stats.parquet";
    const original = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(original);
    const cases = [_]struct { writer: []const u8, binary: bool, unsigned: bool }{
        .{ .writer = "parquet-mr version 1.7.0 (build synthetic)", .binary = false, .unsigned = false },
        .{ .writer = "parquet-mr version 1.8.0", .binary = true, .unsigned = false },
        .{ .writer = "parquet-mr version 1.10.0", .binary = true, .unsigned = true },
        .{ .writer = "parquet-cpp version 1.2.9", .binary = true, .unsigned = false },
        .{ .writer = "parquet-cpp version 1.3.0", .binary = true, .unsigned = true },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.4.999", .binary = false, .unsigned = false },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.5.0", .binary = false, .unsigned = false },
        .{ .writer = "parquet-mr version malformed", .binary = false, .unsigned = false },
    };
    for (cases) |c| {
        const bytes = try rewritten(original, .{ .created_by = c.writer });
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        // The fixture's false but plausible chunk AND page claims exclude real rows and say there are no nulls.
        for ([_][]const u8{ "s = 'a'", "s IS NULL", "d = 1.00", "d IS NULL", "u = 1", "u IS NULL" }, 0..) |filter, fi| {
            for ([_]bool{ false, true }) |trust| for ([_]bool{ false, true }) |scan_all| {
                var arena = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena.deinit();
                const result = try runMultiAggregate(testing.allocator, arena.allocator(), .{
                    .inputs = &inputs,
                    .aggregate = "count(*) AS n",
                    .filter = filter,
                    .trust_stats = trust,
                    .scan_all = scan_all,
                    .parallelism = 3,
                    .max_memory = 64 << 10,
                    .decode_batch_rows = 64,
                });
                defer freeResult(result);
                const allowed = if (fi < 4) c.binary else c.unsigned;
                const want: i128 = if (scan_all or !allowed) 1 else 0;
                try testing.expectEqual(want, result.output.aggs[0].value.i);
            };
        }
        for ([_]usize{ 1, 3, 16 }) |j| for ([_]bool{ false, true }) |scan_all| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const result = try runMultiAggregate(testing.allocator, arena.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(s) AS s, count(d) AS d, count(u) AS u, min(d) AS m",
                .trust_stats = true,
                .scan_all = scan_all,
                .parallelism = j,
            });
            defer freeResult(result);
            for (0..3) |ci| {
                const allowed = if (ci < 2) c.binary else c.unsigned;
                const want: i128 = if (allowed and !scan_all) 3 else 2;
                try testing.expectEqual(want, result.output.aggs[ci].value.i);
            }
            const minimum: i128 = if (c.binary and !scan_all) 900 else 100;
            try testing.expectEqual(minimum, result.output.aggs[3].value.dec.v);
        };
    }
}

test "dependent group keys: nullable multi-key trees and canonical float blocks match legacy" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const streaming = @import("writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const k = [_]i32{ std.math.minInt(i32), -1, 0, 1, std.math.maxInt(i32), -1, 1, 0 };
    const b = [_]i32{ 0, 1, 1, -1, 0, 1, -1, 1 };
    const defs = [_]u32{ 1, 1, 0, 1, 1, 1, 1, 0 };
    const f = [_]f64{ 1e20, 1, -1e20, -0.0, 7, 1e-20, 11, -0.0 };
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT32, .nullable = true, .col = .{ .i32 = .{
            .values = &k,
            .def_levels = &defs,
            .max_def = 1,
        } } },
        .{ .name = "b", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &b } } },
        .{ .name = "f", .parquet_type = .DOUBLE, .nullable = false, .col = .{ .f64 = .{ .values = &f } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    const inputs = [_]Input{.{ .name = "dependent.parquet", .bytes = sink.buffer.items }};
    for ([_][]const u8{ "k, k-1 AS km, k*k AS sq", "b, k-1 AS km, k, k+b AS kb, k*0 AS z" }) |keys| {
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "count(*) AS c, sum(f) AS s, avg(f) AS a",
            .group_by = keys,
            .dependent_keys = false,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .scan_all = true,
        };
        var want = try runMultiAggregate(testing.allocator, a, args);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 3, 16 }) |j| for ([_]usize{ 0, 64 }) |rows| {
            var trial = args;
            trial.parallelism = j;
            trial.dependent_keys = true;
            trial.decode_batch_rows = rows;
            var got = try runMultiAggregate(testing.allocator, a, trial);
            defer got.deinit(testing.allocator);
            const wr = want.output.group_rows.?;
            const gr = got.output.group_rows.?;
            try testing.expectEqual(wr.len, gr.len);
            for (wr, gr) |w, g| for (w, g) |x, y| try expectSameBits(x, y);
            for (want.output.group_cols.?, got.output.group_cols.?) |w, g| try testing.expectEqualStrings(w, g);
        };
        var typed_args = args;
        typed_args.aggregate = "count(*) AS c";
        var original = try runMultiAggregate(testing.allocator, a, typed_args);
        defer original.deinit(testing.allocator);
        typed_args.dependent_keys = true;
        typed_args.typed_answer = true;
        var typed = try runMultiAggregate(testing.allocator, a, typed_args);
        defer typed.deinit(testing.allocator);
        for (original.output.group_rows.?, 0..) |row, i| {
            for (row, typed.output.typed.?.columns) |v, col| {
                try testing.expectEqual(v == .null_val, col.i64.defs[i] == 0);
                if (v != .null_val) try testing.expectEqual(v.i, col.i64.values[i]);
            }
        }
        // Derived arithmetic is INT64, even when the retained base is INT32.
        try testing.expect(typed.state.group_col_types.?[1].source == null);
    }
}

test "packed byte SUM declines reconstructed outputs before admission" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const value: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = value, .where = null, .alias = "s", .result = .i64 }};
    var outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" },
        .{ .source = .{ .agg = 0 }, .name = "s" },
    };
    const args: MultiAggArgs = .{
        .inputs = &.{},
        .aggregate = "sum(v) AS s",
        .byte_records = true,
        .byte_records_min_rows = 0,
        .max_memory = 512 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const plan = try agg_plan.Plan.init(a, .{ .filter = null, .calls = &.{}, .group_keys = null }, &.{}, .ignore);
    // Keep the other admission conditions satisfied: a multi-key SQL fixture alone could mask a missing guard.
    const admitted = try inlineByteTopAnswer(
        testing.allocator,
        a,
        args,
        &.{},
        &plan,
        &.{},
        &calls,
        &.{key},
        &outputs,
        1,
        .{},
        true,
    );
    try testing.expect(admitted != null);
    for (&outputs) |*out| {
        out.derived = key;
        const refused = try inlineByteTopAnswer(
            testing.allocator,
            a,
            args,
            &.{},
            &plan,
            &.{},
            &calls,
            &.{key},
            &outputs,
            1,
            .{},
            true,
        );
        try testing.expect(refused == null);
        out.derived = null;
    }
}

test "packed byte COUNT top-H answers as the general path, with NULL, empty and embedded-zero keys and ties" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const words = [_][]const u8{ "b", "a", "", "a\x00", "zz", "a", "q", "b", "", "r" };
    const n = 4000;
    const k = try a.alloc([]const u8, n);
    const k_def = try a.alloc(u32, n);
    for (0..n) |i| {
        // Skewed, with many equal counts at the cut: ties go to first appearance.
        k[i] = words[(i * 7 + i / 13) % words.len];
        k_def[i] = @intFromBool(i % 17 != 4);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = k, .def_levels = k_def, .max_def = 1 } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, n, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 8);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 8;
    const inputs = [_]Input{.{ .name = "count-keys.parquet", .bytes = sink.buffer.items }};
    for ([_]bool{ true, false }) |descending| for ([_]u64{ 0, 2 }) |offset| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = descending }}, .limit = 3, .offset = offset },
        };
        var want = try runMultiAggregate(testing.allocator, a, base);
        defer want.deinit(testing.allocator);
        try want.output.applyResultOrder(testing.allocator, base.result_order);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 4 << 20, 512 << 20 }) |limit| {
            var trial = base;
            trial.byte_records = true;
            trial.byte_records_min_rows = 0;
            trial.parallelism = j;
            trial.max_memory = limit;
            const explanation = try explainAggregate(testing.allocator, a, trial);
            const admitted = explanation.record_path != null and explanation.record_path.?.layout == .byte_count_top_h;
            var got = try runMultiAggregate(testing.allocator, a, trial);
            defer got.deinit(testing.allocator);
            try got.output.applyResultOrder(testing.allocator, trial.result_order);
            // A tight budget declines the layout and answers through the general path.
            try testing.expectEqual(limit == 512 << 20, admitted);
            try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        };
    };
}

test "packed byte SUM with a dependent integer output uses general grouping" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const k = [_][]const u8{ "b", "a", "b", "a", "", "b" };
    const f = [_]i32{ 1, -2, 1, -2, 0, 3 };
    const v = [_]i64{ 3, 5, -1, 5, 10, 10 };
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = false, .col = .{ .string = .{ .values = &k } } },
        .{ .name = "f", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &f } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &v } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    const inputs = [_]Input{.{ .name = "dependent-string.parquet", .bytes = sink.buffer.items }};
    const base: MultiAggArgs = .{
        .inputs = &inputs,
        .metas = &.{meta},
        .aggregate = "sum(v) AS s",
        .group_by = "k, f, f-1 AS fm",
        .dependent_keys = false,
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .scan_all = true,
        .result_order = .{ .keys = &.{.{ .column = 3, .descending = true }}, .limit = 3 },
    };
    var want = try runMultiAggregate(testing.allocator, a, base);
    defer want.deinit(testing.allocator);
    try want.output.applyResultOrder(testing.allocator, base.result_order);
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 64 << 10, 512 << 20 }) |limit| {
        var taken: std.EnumSet(FastPath) = .{};
        var trial = base;
        trial.dependent_keys = true;
        trial.byte_records = true;
        trial.byte_records_min_rows = 0;
        trial.parallelism = j;
        trial.max_memory = limit;
        trial.taken = &taken;
        const explanation = try explainAggregate(testing.allocator, a, trial);
        try testing.expect(explanation.record_path == null);
        var got = try runMultiAggregate(testing.allocator, a, trial);
        defer got.deinit(testing.allocator);
        try got.output.applyResultOrder(testing.allocator, trial.result_order);
        try testing.expect(!taken.contains(.integer_records));
        try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
}

test "integer owner EXPLAIN admits synthetic rows through the shared 31-bit boundary" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var held = std.heap.ArenaAllocator.init(testing.allocator);
    defer held.deinit();
    var meta = try metadata.open(held.allocator(), bytes);
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ (1 << 27) - 1, 1 << 27, 180_000_000, (1 << 31) - 1, 1 << 31 }) |rows| {
        meta.num_rows = @intCast(rows);
        meta.row_groups.items[0].num_rows = @intCast(rows);
        for ([_]bool{ false, true }) |top| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .metas = (&meta)[0..1],
                .aggregate = if (top) "sum(n) AS s" else "count(*) AS c, sum(n) AS s",
                .group_by = "n",
                .result_order = if (top) .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 } else .{},
                .typed_answer = true,
                .integer_records = true,
                .integer_records_min_rows = 0,
                .max_memory = std.math.maxInt(usize),
                .parallelism = 1,
                .scan_all = true,
            };
            const ex = try explainAggregate(testing.allocator, arena.allocator(), args);
            try testing.expectEqual(owner_groups.admitsRows(.int64, rows), ex.record_path != null);
            if (ex.record_path) |record| {
                const expected: @FieldType(RecordAdmission, "layout") = if (top)
                    .integer_sum_top_h
                else
                    .integer_group_stream;
                try testing.expectEqual(expected, record.layout);
            }
        }
    }
}

test "COUNT certificate sample reads real pages across row groups and repeated input files" {
    const cert = @import("count_certificate.zig");
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    const meta = try metadata.open(state.allocator(), bytes);
    const source: cert.Source = .{
        .rg = &meta.row_groups.items[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
    };
    const sources = [_]cert.Source{ source, source, source };
    var sample = try cert.sample(testing.allocator, &sources, 1, 1, 4, 4, .{});
    defer sample.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, @intCast(source.rg.num_rows * 3)), sample.rows);
    try testing.expect(sample.counts.count() > 0);
    try testing.expect(sample.fmax >= sample.fmin);
}

test "hot COUNT certificate matches canonical clustered keys, NULLs, ties, offsets and late cold values" {
    var certified = false;
    var residual = false;
    for ([_][]const u8{ "plain", "dict" }) |encoding| {
        var file_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer file_arena.deinit();
        const path = try std.fmt.allocPrint(
            file_arena.allocator(),
            "ci/fixtures/parquet/count_certificate_{s}.parquet",
            .{encoding},
        );
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        for ([_][]const u8{ "k", "k, f" }) |key| for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
            for ([_]usize{ 1 << 20, 512 << 20 }) |budget| for ([_]bool{ false, true }) |force_replay| {
                var arena = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena.deinit();
                const args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = "count(*) AS c",
                    .group_by = key,
                    .parallelism = j,
                    .min_bytes_per_worker = 1,
                    .max_memory = budget,
                    .decode_limit = budget,
                    .result_order = .{
                        .keys = &.{.{ .column = if (key.len == 1) 1 else 2, .descending = true }},
                        .limit = 3,
                        .offset = if (key.len == 1) 0 else 2,
                    },
                    .ndv_policy = false,
                    .plain_rules = false,
                    .dense = false,
                };
                var want = try runMultiAggregate(testing.allocator, arena.allocator(), args);
                defer want.deinit(testing.allocator);
                var outcome: SketchTopK.HotOutcome = .declined;
                var trial = args;
                trial.sketch_topk = .{
                    .cell_bits = 8,
                    .admit_per_row = 1,
                    .hot_outcome = &outcome,
                    .certify = !force_replay,
                };
                var got = try runMultiAggregate(testing.allocator, arena.allocator(), trial);
                defer got.deinit(testing.allocator);
                certified = certified or outcome == .certified;
                residual = residual or outcome == .residual;
                try testing.expectEqual(want.rows_in, got.rows_in);
                try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                try testing.expectEqual(want.rows_matched, got.rows_matched);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            };
        };
    }
    try testing.expect(certified);
    try testing.expect(residual);
}



test "hot COUNT certificate releases every trial allocation and its ledger charge on failure" {
    const path = "ci/fixtures/parquet/count_certificate_plain.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var meta = try metadata.open(arena.allocator(), bytes);
    meta.row_groups.items = meta.row_groups.items[meta.row_groups.items.len - 1 ..];
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), runHotCertificateAndFree, .{ &inputs, &meta });
}



test "COUNT-certified payloads preserve signed SUM, exact integer AVG, NULLs, extrema and complete counts" {
    var used_certificate = false;
    var used_payload_policy = false;
    for ([_][]const u8{ "plain", "dict" }) |encoding| {
        var held = std.heap.ArenaAllocator.init(testing.allocator);
        defer held.deinit();
        const path = try std.fmt.allocPrint(
            held.allocator(),
            "ci/fixtures/parquet/count_certificate_{s}.parquet",
            .{encoding},
        );
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 8 << 20, 512 << 20 }) |budget| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .aggregate = "count(*) AS c, sum(v) AS total, avg(v) AS av, min(v) AS mn, max(v) AS mx",
                .group_by = "s",
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .max_memory = budget,
                .decode_limit = budget,
                .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
                .dense = false,
                .ndv_union = false,
            };
            var want = try runMultiAggregate(testing.allocator, arena.allocator(), args);
            defer want.deinit(testing.allocator);
            var outcome: SketchTopK.Outcome = .ineligible;
            var trial = args;
            trial.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1, .outcome = &outcome };
            const explained = try explainAggregate(testing.allocator, arena.allocator(), trial);
            var got = try runMultiAggregate(testing.allocator, arena.allocator(), trial);
            defer got.deinit(testing.allocator);
            used_certificate = used_certificate or outcome == .census or outcome == .certified;
            if (explained.plan.reasons.contains(.census_short_string_payload)) {
                used_payload_policy = true;
                try testing.expect(explained.plan.dispatch.census_allowed and explained.plan.dispatch.census_fused);
            }
            try want.output.applyResultOrder(testing.allocator, args.result_order);
            try got.output.applyResultOrder(testing.allocator, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
        };
    }
    try testing.expect(used_certificate);
    try testing.expect(used_payload_policy);
}

test "hot COUNT footer range hints cannot exclude NULL or out-of-range winners" {
    const path = "ci/fixtures/parquet/count_certificate_plain.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_][2]i64{ .{ -1, 1 }, .{ 2, 2 } }) |range| for ([_]usize{ 1, 4 }) |j| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const actual = try metadata.open(arena.allocator(), bytes);
        const hint = try metadata.open(arena.allocator(), bytes);
        var lo: [8]u8 = undefined;
        var hi: [8]u8 = undefined;
        std.mem.writeInt(i64, &lo, range[0], .little);
        std.mem.writeInt(i64, &hi, range[1], .little);
        for (hint.row_groups.items) |*rg| {
            const cm = &rg.columns.items[1].meta_data.?;
            try testing.expectEqual(schema.Type.INT64, cm.type);
            cm.statistics.?.min_value = &lo;
            cm.statistics.?.max_value = &hi;
        }
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = (&actual)[0..1],
            .aggregate = "count(*) AS c",
            .group_by = "k,f",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .ndv_policy = false,
            .plain_rules = false,
            .dense = false,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 3 },
        };
        var want = try runMultiAggregate(testing.allocator, arena.allocator(), args);
        defer want.deinit(testing.allocator);
        args.metas = (&hint)[0..1];
        args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 };
        var got = try runMultiAggregate(testing.allocator, arena.allocator(), args);
        defer got.deinit(testing.allocator);
        try want.output.applyResultOrder(testing.allocator, args.result_order);
        try got.output.applyResultOrder(testing.allocator, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
}

test "COUNT input-wide sample survives every partial worker start" {
    const cert = @import("count_certificate.zig");
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const meta = try metadata.open(arena.allocator(), bytes);
    const source: cert.Source = .{
        .rg = &meta.row_groups.items[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
    };
    const sources = [_]cert.Source{ source, source, source };
    var want = try cert.sample(testing.allocator, &sources, 1, 1, 4, 4, .{});
    defer want.deinit(testing.allocator);
    for (0..3) |started| {
        spawn_util.injectFailureAfter(started);
        defer spawn_util.resetFailure();
        var got = try cert.sample(testing.allocator, &sources, 1, 1, 4, 4, .{});
        defer got.deinit(testing.allocator);
        try testing.expect(spawn_util.failuresInjected() > 0);
        try testing.expectEqual(want.rows, got.rows);
        try testing.expectEqual(want.fmin, got.fmin);
        try testing.expectEqual(want.fmax, got.fmax);
        try testing.expectEqual(want.counts.count(), got.counts.count());
        var it = want.counts.iterator();
        while (it.next()) |entry| try testing.expectEqual(entry.value_ptr.*, got.counts.get(entry.key_ptr.*).?);
    }
}



test "COUNT residual budget refusal releases trial groups and restores counts before the fitting plain scan" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    const keys = [_]i64{ 7, 3, 7, 9, 3, 7, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19 };
    const cols = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = false,
        .col = .{ .i64 = .{ .values = &keys } },
    }};
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, sink.buffer.items);
    try countResidualBudgetTrial(testing.allocator, sink.buffer.items, &meta, false);
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(
        fixed.allocator(),
        countResidualBudgetTrial,
        .{ sink.buffer.items, &meta, false },
    );
}

test "COUNT residual never turns a real decode failure into a budget fallback" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    const keys = [_]i64{ 1, 2, 1, 3 };
    const cols = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = false,
        .col = .{ .i64 = .{ .values = &keys } },
    }};
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, sink.buffer.items);
    const cm = meta.row_groups.items[0].columns.items[0].meta_data.?;
    const offset: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    sink.buffer.items[offset] = 0xaf;
    try countResidualBudgetTrial(testing.allocator, sink.buffer.items, &meta, true);
}

test "integer owner decoded runs and false sorted hints match the canonical multi-file answer" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("writer/streaming.zig");
    for ([_]bool{ false, true }) |descent| {
        var sink: streaming.MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..4) |item| {
            var keys = [_]i64{ 2, 2, 2, 2 };
            if (item == 0) keys[0] = 1;
            if (item == 3) keys[3] = 3;
            if (descent and item == 2) keys[2] = -1;
            const values = [_]i64{ 3, -4, 6, 5 };
            const cols = [_]expr_agg.OutputCol{
                .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &keys } } },
                .{
                    .name = "v",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &values, .def_levels = &.{ 0, 1, 1, 0 }, .max_def = 1 } },
                },
            };
            try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
            .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const meta = try metadata.open(a, sink.buffer.items);
        if (descent) for (meta.row_groups.items) |*rg| {
            var zero: [8]u8 = @splat(0);
            const st = &rg.columns.items[0].meta_data.?.statistics.?;
            st.min_value = try a.dupe(u8, &zero);
            st.max_value = try a.dupe(u8, &zero);
        };
        // Two files force a source seam that descends; one file exercises a three-row-group equality chain.
        const input: Input = .{ .name = "runs.parquet", .bytes = sink.buffer.items };
        for ([_]usize{ 1, 2 }) |files| {
            const inputs = [_]Input{ input, input };
            const metas = [_]schema.FileMetaData{ meta, meta };
            const base: MultiAggArgs = .{
                .inputs = inputs[0..files],
                .metas = metas[0..files],
                .aggregate = "count(*) AS c, sum(v) AS s",
                .group_by = "k",
                .max_memory = 512 << 20,
                .parallelism = 1,
                .min_bytes_per_worker = 1,
                .integer_records = false,
                .scan_all = true,
            };
            var want = try runMultiAggregate(testing.allocator, a, base);
            defer want.deinit(testing.allocator);
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
                var args = base;
                args.integer_records = true;
                args.integer_records_min_rows = 0;
                args.typed_answer = true;
                args.parallelism = j;
                args.decode_batch_rows = 2;
                var got = try runMultiAggregate(testing.allocator, a, args);
                defer got.deinit(testing.allocator);
                const stream = got.output.integer_stream.?;
                const rows = want.output.group_rows.?;
                try testing.expectEqual(!descent and files == 1, stream.state.runs.len != 0);
                try testing.expectEqual(rows.len, stream.state.group_count);
                var window = try stream.window(testing.allocator, got.state.group_col_types.?, 0, rows.len, j);
                defer window.deinit(testing.allocator);
                for (rows, 0..) |row, i| for (row, window.columns) |v, col| {
                    const lane = col.i64;
                    try testing.expectEqual(v == .null_val, lane.defs.len != 0 and lane.defs[i] == 0);
                    if (v != .null_val) try testing.expectEqual(v.i, lane.values[i]);
                };
                for ([_][]const u8{ "sum(v) AS s", "count(*) AS c" }) |aggregate| for ([_]bool{ false, true }) |desc| {
                    var top_args = base;
                    top_args.aggregate = aggregate;
                    top_args.parallelism = j;
                    top_args.result_order = .{ .keys = &.{.{ .column = 1, .descending = desc }}, .limit = 2, .offset = 1 };
                    var top_want = try runMultiAggregate(testing.allocator, a, top_args);
                    defer top_want.deinit(testing.allocator);
                    top_args.integer_records = true;
                    top_args.integer_records_min_rows = 0;
                    top_args.typed_answer = true;
                    var taken: std.EnumSet(FastPath) = .{};
                    top_args.taken = &taken;
                    if (!descent and files == 1 and aggregate[0] == 'c') {
                        const explanation = try explainAggregate(testing.allocator, a, top_args);
                        try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
                    }
                    var top_got = try prepareAggregate(testing.allocator, a, top_args, false);
                    defer top_got.deinit(testing.allocator);
                    if (!descent and files == 1) try testing.expect(taken.contains(.integer_records));
                    try top_want.output.applyResultOrder(testing.allocator, top_args.result_order);
                    try top_got.output.applyResultOrder(testing.allocator, top_args.result_order);
                    try testing.expectEqualDeep(top_want.output.group_rows.?, top_got.output.group_rows.?);
                };
            }
        }
    }
}





test "ordered decline replays releasing gates for NULL descent and cross-row-group overlap" {
    var held = std.heap.ArenaAllocator.init(testing.allocator);
    defer held.deinit();
    const a = held.allocator();
    for (0..3) |fault| {
        var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..3) |rg| {
            var keys = [_]i64{ 1, 2, 2, 3 };
            var defs = [_]u32{ 1, 1, 1, 1 };
            if (fault != 2) for (&keys) |*key| {
                key.* += @intCast(rg * 2);
            };
            if (fault == 0 and rg == 1) defs[2] = 0;
            if (fault == 1 and rg == 1) keys[2] = 3;
            const values = [_]i64{ 3, -4, 6, 5 };
            const cols = [_]expr_agg.OutputCol{
                .{
                    .name = "k",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &keys, .def_levels = &defs, .max_def = 1 } },
                },
                .{ .name = "v", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &values } } },
            };
            try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{ .name = "k", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
            .{ .name = "v", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const meta = try metadata.open(a, sink.buffer.items);
        // Bounds select the attempt, but decoded NULLs, descents and seams must all be checked.
        if (fault == 2) for (meta.row_groups.items) |*rg| {
            const stats = &rg.columns.items[0].meta_data.?.statistics.?;
            stats.min_value = &@as([8]u8, @splat(0));
            stats.max_value = stats.min_value;
        };
        const inputs = [_]Input{.{ .name = "scan-all.parquet", .bytes = sink.buffer.items }};
        var want = try runMultiAggregate(testing.allocator, a, .{
            .inputs = &inputs,
            .metas = (&meta)[0..1],
            .aggregate = "count(*) AS c, sum(v) AS s",
            .group_by = "k",
            .scan_all = true,
            .integer_records = false,
        });
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j|
            try releasingOrderedTrial(testing.allocator, sink.buffer.items, &meta, want.output.group_rows.?, j);
        if (fault == 0) {
            var fixed: NoInPlaceResize = .{ .child = testing.allocator };
            try testing.checkAllAllocationFailures(
                fixed.allocator(),
                releasingOrderedTrial,
                .{ sink.buffer.items, &meta, want.output.group_rows.?, 1 },
            );
        }
    }
}

test "phase-four replay policies decline releasing gates before certificate residual or budget trials" {
    const path = "ci/fixtures/parquet/count_certificate_plain.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var held = std.heap.ArenaAllocator.init(testing.allocator);
    defer held.deinit();
    const meta = try metadata.open(held.allocator(), bytes);
    const mapped = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_][]const u8{ "count(*) AS c", "count(DISTINCT v) AS c" }) |aggregate|
        for ([_]bool{ false, true }) |first| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            var args: MultiAggArgs = .{
                .inputs = &mapped,
                .metas = (&meta)[0..1],
                .aggregate = aggregate,
                .group_by = "k",
                .integer_records = false,
                .ndv_policy = false,
                .plain_rules = false,
                .dense = false,
                .scan_all = true,
                .min_bytes_per_worker = 1,
                .result_order = .{ .keys = if (first) &.{} else &.{.{ .column = 1, .descending = true }}, .limit = 3 },
            };
            var want = try runMultiAggregate(testing.allocator, a, args);
            defer want.deinit(testing.allocator);
            try want.output.applyResultOrder(testing.allocator, args.result_order);
            for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 1 << 20, 512 << 20 }) |budget|
                for ([_]bool{ false, true }) |certify| {
                    var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
                    const inputs = [_]Input{window.input(bytes)};
                    var outcome: SketchTopK.Outcome = .certified;
                    args.inputs = &inputs;
                    args.parallelism = j;
                    args.max_memory = budget;
                    args.decode_limit = budget;
                    args.sketch_topk = .{
                        .cell_bits = 8,
                        .admit_per_row = 1,
                        .fuse_census = true,
                        .certify = certify,
                        .outcome = &outcome,
                    };
                    const explanation = try explainAggregate(testing.allocator, a, args);
                    try testing.expectEqual(.plain_hash, explanation.plan.dispatch.strategy);
                    var got = try runMultiAggregate(testing.allocator, a, args);
                    defer got.deinit(testing.allocator);
                    try got.output.applyResultOrder(testing.allocator, args.result_order);
                    try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
                    try testing.expectEqual(meta.row_groups.items.len, window.waits.load(.monotonic));
                    try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
                    try testing.expectEqual(.ineligible, outcome);
                };
        };
}

test "windowed S3 merge refusal never retries consumed non-rescannable inputs" {
    const s3_window = @import("../io/s3_window.zig");
    if (!s3_window.supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    const keys = [_]i64{ 7, 3, 7, 9, 3, 7, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19 };
    const cols = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = false,
        .col = .{ .i64 = .{ .values = &keys } },
    }};
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, sink.buffer.items);
    const Refusal = struct {
        gate: RowGroupGate,
        total: *std.atomic.Value(usize),
        budget: *core_memory.SharedBudget,
        held: *usize,
        waits: std.atomic.Value(usize) = .init(0),

        fn wait(ctx: *anyopaque, rg: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.waits.fetchAdd(1, .monotonic);
            try self.gate.wait(self.gate.ctx, rg);
        }

        fn done(ctx: *anyopaque, rg: usize, items: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.gate.done(self.gate.ctx, rg, items);
            if (self.total.fetchAdd(1, .acq_rel) + 1 != 2) return;
            // All work is finished. Hold the entire merge ceiling, even if other scan charges return before
            // merging. This injects a merge-only refusal independently of which worker claimed each item.
            self.held.* = self.budget.limit + core_memory.finalize_allowance;
            self.budget.force(self.held.*);
        }
    };
    for ([_]usize{ 2, 4, 16 }) |j| for ([_]bool{ false, true }) |mixed| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var held: usize = 0;
        defer budget.release(held);
        var total: std.atomic.Value(usize) = .init(0);
        var groups: [2]s3_window.RowGroup = undefined;
        var order = [_]*s3_window.RowGroup{ &groups[0], &groups[1] };
        var window: s3_window.Window = .{
            .gpa = gpa,
            .order = &order,
            .limit = 2 * s3_window.page,
            .batch = s3_window.page,
        };
        const first = try s3_window.mapBuffer(sink.buffer.items.len);
        defer s3_window.unmapBuffer(first);
        const second = try s3_window.mapBuffer(sink.buffer.items.len);
        defer s3_window.unmapBuffer(second);
        const buffers = [_][]align(s3_window.page) u8{ first, second };
        var rgs = [2][1]?*s3_window.RowGroup{ .{&groups[0]}, .{&groups[1]} };
        var files: [2]s3_window.File = undefined;
        var refusals: [2]Refusal = undefined;
        var inputs: [2]Input = undefined;
        for (0..2) |i| {
            @memcpy(buffers[i][0..sink.buffer.items.len], sink.buffer.items);
            // Already fetched: s3_window's ready state. Its real gate releases these anonymous pages.
            groups[i] = .{
                .jobs = &.{},
                .bytes = buffers[i].len,
                .seg = buffers[i],
                .admitted = true,
                .window = &window,
                .state = .init(1),
            };
            files[i] = .{ .rgs = &rgs[i] };
            refusals[i] = .{ .gate = files[i].gate(), .total = &total, .budget = &budget, .held = &held };
            inputs[i] = .{
                .name = if (mixed and i == 0) "local.parquet" else "s3://bucket/window.parquet",
                .bytes = buffers[i][0..sink.buffer.items.len],
                .plan_bytes = if (mixed and i == 0) sink.buffer.items else null,
                .gate = .{ .ctx = &refusals[i], .wait = Refusal.wait, .done = Refusal.done },
            };
        }
        if (runMultiAggregate(gpa, a, .{
            .inputs = &inputs,
            .metas = &.{ meta, meta },
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .scan_all = true,
            .max_memory = budget.limit,
            .ledger = &budget,
            .charge_decode = false,
            .ndv_policy = false,
            .plain_rules = false,
            .dense = false,
            .partition_cost = false,
            .spill = false,
            .distinct_pairs = .off,
        })) |got| {
            var unexpected = got;
            unexpected.deinit(gpa);
            return error.TestExpectedError;
        } else |err| try testing.expectEqual(error.ExceededMemoryBudget, err);
        try testing.expect(held != 0);
        try testing.expectEqual(held, budget.used.load(.monotonic));
        try testing.expectEqual(@as(usize, 2), total.load(.monotonic));
        for (&groups, &refusals, buffers) |*rg, *refusal, buffer| {
            try testing.expectEqual(@as(usize, 1), refusal.waits.load(.monotonic));
            try testing.expectEqual(@as(u32, 1), rg.items_done.load(.monotonic));
            try testing.expectEqual(@as(u8, 0), buffer[0]); // Actual MADV_DONTNEED, not a retained test buffer.
        }
    };
}

test "budget narrowing requires rescannable inputs for every restart reason" {
    const Gate = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            unreachable;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    var marker: u8 = 0;
    var inputs = [_]Input{.{
        .name = "s3://bucket/window.parquet",
        .bytes = &.{},
        .gate = .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done },
    }};
    var args: MultiAggArgs = .{ .inputs = &inputs, .aggregate = "", .parallelism = 4 };
    for (std.enums.values(FastPath)) |path| {
        var chain: Chain = .{ .parts_tried = false };
        try testing.expect(narrowed(args, args, std.EnumSet(FastPath).initOne(path), &chain, true) == null);
    }
    // The helper also declines at runtime, before allocating a view or abandoning a reader.
    try testing.expectError(error.ExceededMemoryBudget, rewindInputs(testing.allocator, &args));
    try testing.expectEqual(inputs[0..].ptr, args.inputs.ptr);
    for ([_]bool{ false, true }) |mapped| {
        inputs[0].gate = if (mapped) .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done } else null;
        inputs[0].plan_bytes = if (mapped) &.{} else null;
        var chain: Chain = .{ .parts_tried = false };
        const retry = narrowed(args, args, std.EnumSet(FastPath).initOne(.merge_memory), &chain, false).?;
        try testing.expectEqual(@as(usize, 1), retry.parallelism);
    }
}

test "dense composite counts arm globally and preserve selected first through windows" {
    try denseProductScan(false);
}

test "dense composite SUM arms globally and preserves selected first through windows" {
    try denseProductScan(true);
}



test "distinct bitmaps retain identities after releasing windows and late wide fallback" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    for (0..4) |rg| {
        var keys: [512]i64 = undefined;
        var values: [512]i64 = undefined;
        var defs: [512]u32 = undefined;
        for (&keys, &values, &defs, 0..) |*k, *v, *d, r| {
            k.* = @intCast(r % 3);
            v.* = @intCast((r + rg * 31) % 128);
            d.* = @intFromBool(r % 17 != 0);
        }
        if (rg == 3) values[511] = 1 << 48;
        const cols = [_]expr_agg.OutputCol{
            .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &keys } } },
            .{
                .name = "v",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } },
            },
        };
        try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&.{
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
    });
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    const mapped = [_]Input{.{ .name = "distinct.parquet", .bytes = bytes }};
    for ([_]?[]const u8{ null, "k" }) |group_by| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(DISTINCT v) AS d, count(*) AS c",
            .group_by = group_by,
            .integer_records = false,
            .ndv_policy = false,
            .plain_rules = false,
            .distinct_pairs = .off,
            .dense = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(testing.allocator, a, args);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch_rows| {
            var completed: [4]std.atomic.Value(usize) = @splat(.init(0));
            var window: ReleasingScanWindow = .{
                .bytes = try a.dupe(u8, bytes),
                .meta = &meta,
                .completed = &completed,
            };
            // No mapped planning view: a retry would have to read already released bytes.
            const inputs = [_]Input{.{
                .name = "distinct.parquet",
                .bytes = window.bytes,
                .gate = .{ .ctx = &window, .wait = ReleasingScanWindow.wait, .done = ReleasingScanWindow.release },
            }};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = batch_rows;
            var got = try runMultiAggregate(testing.allocator, a, args);
            defer got.deinit(testing.allocator);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            try testing.expect(window.waits.load(.monotonic) >= meta.row_groups.items.len);
            try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
        };
    }
}

test "distinct coded sets share automatic pairs across releasing windows and refused final drains" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |mixed| {
        var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..16) |rg| {
            var keys: [2048]i64 = undefined;
            var values: [2048]i64 = undefined;
            var kd: [2048]u32 = undefined;
            var vd: [2048]u32 = undefined;
            for (&keys, &values, &kd, &vd, 0..) |*k, *v, *kn, *vn, r| {
                k.* = @intCast(r % 3);
                v.* = @intCast(if (mixed and rg == 7) 5000 + r else (r / 3) % 64 + rg * 128);
                kn.* = @intFromBool(r % 23 != 0);
                vn.* = @intFromBool(r % 17 != 0);
            }
            const cols = [_]expr_agg.OutputCol{
                .{
                    .name = "k",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &keys, .def_levels = &kd, .max_def = 1 } },
                },
                .{
                    .name = "v",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &values, .def_levels = &vd, .max_def = 1 } },
                },
            };
            try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{ .name = "k", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
            .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const bytes = sink.buffer.items;
        const meta = try metadata.open(a, bytes);
        const mapped = [_]Input{.{ .name = "mixed-distinct.parquet", .bytes = bytes }};
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(DISTINCT v) AS d",
            .group_by = "k",
            .scan_all = true,
            .plain_rules = false,
            .distinct_pairs = .off,
            .pairs_pilot_rows = 1,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        args.distinct_pairs = .auto;
        var mapped_auto = try runMultiAggregate(gpa, a, args);
        defer mapped_auto.deinit(gpa);
        try testing.expectEqualDeep(want.output.group_rows, mapped_auto.output.group_rows);
        try testing.expectEqual(mixed, mapped_auto.distinct_pairs);
        for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |batch|
            for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .on, .auto }) |mode| {
                var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
                const inputs = [_]Input{window.input(bytes)};
                args.inputs = &inputs;
                args.parallelism = j;
                args.decode_batch_rows = batch;
                args.distinct_pairs = mode;
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqual(mode == .on or mixed, got.distinct_pairs);
                try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
            };
        if (mixed) {
            const Refusal = struct {
                window: ReleasingScanWindow,
                budget: *core_memory.SharedBudget,
                held: usize = 0,
                denied: std.atomic.Value(bool) = .init(false),
                fn wait(ctx: *anyopaque, rg: usize) anyerror!void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    try ReleasingScanWindow.wait(&self.window, rg);
                }
                fn done(ctx: *anyopaque, rg: usize, items: usize) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    ReleasingScanWindow.release(&self.window, rg, items);
                    if (self.window.releases.load(.acquire) == self.window.meta.row_groups.items.len and
                        self.denied.cmpxchgStrong(false, true, .acq_rel, .acquire) == null)
                    {
                        self.held = self.budget.limit + core_memory.finalize_allowance;
                        self.budget.force(self.held);
                    }
                }
                fn start(_: *anyopaque) anyerror!void {}
                fn abandon(ctx: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    self.budget.release(self.held);
                    self.held = 0;
                    ReleasingScanWindow.abandon(&self.window);
                }
            };
            var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
            var refused: Refusal = .{
                .window = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta },
                .budget = &budget,
            };
            defer budget.release(refused.held);
            const inputs = [_]Input{.{
                .name = "mixed-distinct.parquet",
                .bytes = refused.window.bytes,
                .plan_bytes = bytes,
                .gate = .{ .ctx = &refused, .wait = Refusal.wait, .done = Refusal.done },
            }};
            args.inputs = &inputs;
            args.parallelism = 4;
            args.decode_batch_rows = 0;
            args.distinct_pairs = .auto;
            args.max_memory = 64 << 20;
            args.decode_limit = 64 << 20;
            args.ledger = &budget;
            args.start_reads = .{ .ctx = &refused, .start = Refusal.start, .abandon = Refusal.abandon };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expect(refused.denied.load(.monotonic));
            try testing.expectEqual(@as(usize, 1), refused.window.abandoned);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        }
    }
}



test "scalar owner cardinality survives releasing windows and late width refusal through mapped rewind" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |late| {
        var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..16) |rg| {
            var values: [512][]const u8 = undefined;
            var defs: [512]u32 = undefined;
            for (&values, &defs, 0..) |*v, *d, r| {
                const k = (r + rg * 17) % 128;
                v.* = if (k == 0) "" else if (k == 1) "a\x00" else try std.fmt.allocPrint(a, "key-{d}", .{k});
                d.* = @intFromBool(r % 17 != 0);
            }
            if (late and rg == 15) values[511] = "late key beyond fifteen bytes";
            const cols = [_]expr_agg.OutputCol{.{
                .name = "k",
                .parquet_type = .BYTE_ARRAY,
                .nullable = true,
                .col = .{ .string = .{ .values = &values, .def_levels = &defs, .max_def = 1 } },
            }};
            try file.append(&cols, values.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{.{
            .name = "k",
            .parquet_type = .BYTE_ARRAY,
            .nullable = true,
            .col = .{ .string = .{ .values = &.{} } },
        }});
        const bytes = sink.buffer.items;
        const meta = try metadata.open(a, bytes);
        for (meta.row_groups.items) |*rg| rg.columns.items[0].meta_data.?.statistics = null;
        const mapped = [_]Input{.{ .name = "scalar-distinct.parquet", .bytes = bytes }};
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(DISTINCT k) AS cardinality",
            .integer_records = false,
            .byte_records = false,
            .dense = false,
            .ndv_policy = false,
            .plain_rules = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
            .max_memory = 64 << 20,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.byte_records = true;
            args.byte_records_min_rows = 0;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            };
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.byte_distinct, explanation.record_path.?.layout);
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            if (!late) {
                try testing.expectEqual(.count, got.state.agg_calls[0].func);
                try testing.expectEqual(@as(usize, 1), got.state.accumulators.len);
                try testing.expectEqual(@as(usize, 1), got.state.agg_sources.len);
                const cols = try expr_agg.finalize(a, got.state.agg_calls[0], got.state.accumulators[0]);
                try testing.expectEqual(@as(i64, @intCast(want.output.aggs[0].value.i)), cols[0].col.i64.values[0]);
            }
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, @intFromBool(late)), window.abandoned);
        };
        // The same remote input without a retained mapping must decline before consuming any records.
        var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
        var input = window.input(bytes);
        input.plan_bytes = null;
        args.inputs = (&input)[0..1];
        args.start_reads = null;
        const remote = try explainAggregate(gpa, a, args);
        try testing.expect(remote.record_path == null);
    }
}

test "distinct owner radix completes full pairs after releasing every reader window" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    for (0..16) |rg| {
        var keys: [512]i64 = undefined;
        var vals: [512]i64 = undefined;
        var kd: [512]u32 = undefined;
        var vd: [512]u32 = undefined;
        for (&keys, &vals, &kd, &vd, 0..) |*k, *v, *kn, *vn, r| {
            k.* = @as(i64, @intCast((r + rg * 7) % 13)) * (1 << 48) - (1 << 49);
            v.* = @as(i64, @intCast((r * 3 + rg * 11) % 17)) * (1 << 41) - (1 << 45);
            kn.* = @intFromBool(r % 23 != 0);
            vn.* = @intFromBool(r % 17 != 0 and k.* != 0);
        }
        keys[1] = std.math.minInt(i64);
        vals[1] = std.math.maxInt(i64);
        vals[2] = std.math.minInt(i64);
        const cols = [_]expr_agg.OutputCol{
            .{
                .name = "k",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &keys, .def_levels = &kd, .max_def = 1 } },
            },
            .{
                .name = "x",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &vals, .def_levels = &vd, .max_def = 1 } },
            },
        };
        try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&.{
        .{ .name = "k", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        .{ .name = "x", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
    });
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    const mapped = [_]Input{.{ .name = "pair-radix.parquet", .bytes = bytes }};
    var args: MultiAggArgs = .{
        .inputs = &mapped,
        .metas = (&meta)[0..1],
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k",
        .integer_records = false,
        .byte_records = false,
        .dense = false,
        .distinct_pairs = .off,
        .ndv_policy = false,
        .plain_rules = false,
        .scan_all = true,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4, .offset = 2 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
        var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
        const inputs = [_]Input{window.input(bytes)};
        var taken: std.EnumSet(FastPath) = .{};
        args.inputs = &inputs;
        args.integer_records = true;
        args.integer_records_min_rows = 0;
        args.parallelism = j;
        args.decode_batch_rows = batch;
        args.taken = &taken;
        args.start_reads = .{
            .ctx = &window,
            .start = ReleasingScanWindow.start,
            .abandon = ReleasingScanWindow.abandon,
        };
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expectEqual(.integer_distinct_top_h, explanation.record_path.?.layout);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        try testing.expectEqual(want.state.group_col_types.?[1].nullable, got.state.group_col_types.?[1].nullable);
        try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), window.abandoned);
    };
    args.start_reads = null;
    args.inputs = &mapped;
    args.distinct_pairs = .on;
    args.taken = null;
    const forced = try explainAggregate(gpa, a, args);
    try testing.expectEqual(.pair_distinct, forced.record_path.?.layout);
    var shared_pairs = try prepareAggregate(gpa, a, args, false);
    defer shared_pairs.deinit(gpa);
    try testing.expect(shared_pairs.distinct_pairs);
    try shared_pairs.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, shared_pairs.output.group_rows);
    var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
    var input = window.input(bytes);
    input.plan_bytes = null;
    args.inputs = (&input)[0..1];
    args.distinct_pairs = .off;
    const remote = try explainAggregate(gpa, a, args);
    try testing.expect(remote.record_path == null);
}

test "shared integer scalar consumer owns answers after releasing each full or sliced reader window" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var sum: i128 = 0;
    var present: u64 = 0;
    var all: u64 = 0;
    var lo: i64 = std.math.maxInt(i64);
    var hi: i64 = std.math.minInt(i64);
    for (0..16) |rg| {
        var values: [193]i64 = undefined;
        var defs: [193]u32 = undefined;
        for (&values, &defs, 0..) |*v, *d, r| {
            v.* = @as(i64, @intCast(r * 137 + rg * 73)) - 15000;
            d.* = @intFromBool(r % 11 != 0);
            all += 1;
            if (d.* != 0) {
                sum += v.*;
                present += 1;
                lo = @min(lo, v.*);
                hi = @max(hi, v.*);
            }
        }
        try file.append(&.{.{
            .name = "v",
            .parquet_type = .INT64,
            .nullable = true,
            .col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } },
        }}, values.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&.{.{
        .name = "v",
        .parquet_type = .INT64,
        .nullable = true,
        .col = .{ .i64 = .{ .values = &.{} } },
    }});
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| rg.columns.items[0].meta_data.?.statistics = null;
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
        var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
        const inputs = [_]Input{window.input(bytes)};
        var got = try runMultiAggregate(gpa, a, .{
            .inputs = &inputs,
            .metas = (&meta)[0..1],
            .aggregate = "count(*) AS c,sum(v) AS s,min(v) AS lo,max(v) AS hi,avg(v) AS av,count(v) AS n",
            .parallelism = j,
            .decode_batch_rows = batch,
            .encoded = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
            .start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            },
        });
        defer got.deinit(gpa);
        const acc = got.state.accumulators;
        try testing.expectEqual(all, acc[0].count);
        try testing.expectEqual(sum, acc[1].sum_i.?);
        try testing.expectEqual(lo, acc[2].min_i.?);
        try testing.expectEqual(hi, acc[3].max_i.?);
        try testing.expectEqual(sum, acc[4].avg.isum);
        try testing.expectEqual(present, acc[4].avg.count);
        try testing.expectEqual(present, acc[5].count);
        try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), window.abandoned);
    };
}

test "sliced consumer reuses payload survivors across releasing reader windows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const empty = [_]expr_agg.OutputCol{
        .{ .name = "d", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        .{ .name = "q", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "discount", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "price", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
    };
    for (0..16) |rg| {
        var dates: [257]i64 = undefined;
        var quantity: [257]f64 = undefined;
        var discount: [257]f64 = undefined;
        var price: [257]f64 = undefined;
        var defs: [257]u32 = undefined;
        for (&dates, &quantity, &discount, &price, &defs, 0..) |*d, *q, *disc, *p, *dl, r| {
            d.* = @intCast((r + rg * 3) % 23);
            q.* = @floatFromInt(r % 51);
            disc.* = @as(f64, @floatFromInt(r % 11)) / 100;
            p.* = switch (r % 5) {
                0 => 1e16,
                1 => 1,
                2 => -1e16,
                3 => -0.0,
                else => 0.1,
            };
            dl.* = @intFromBool(r % 17 != 0);
        }
        const cols = [_]expr_agg.OutputCol{
            .{
                .name = "d",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &dates, .def_levels = &defs, .max_def = 1 } },
            },
            .{
                .name = "q",
                .parquet_type = .DOUBLE,
                .nullable = true,
                .col = .{ .f64 = .{ .values = &quantity, .def_levels = &defs, .max_def = 1 } },
            },
            .{
                .name = "discount",
                .parquet_type = .DOUBLE,
                .nullable = true,
                .col = .{ .f64 = .{ .values = &discount, .def_levels = &defs, .max_def = 1 } },
            },
            .{
                .name = "price",
                .parquet_type = .DOUBLE,
                .nullable = true,
                .col = .{ .f64 = .{ .values = &price, .def_levels = &defs, .max_def = 1 } },
            },
        };
        try file.append(&cols, dates.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&empty);
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| {
        for (rg.columns.items) |*col| col.meta_data.?.statistics = null;
    }
    const mapped = [_]Input{.{ .name = "survivors.parquet", .bytes = bytes }};
    for ([_][]const u8{
        "d>=3 AND d<17 AND discount>=0.05 AND discount<=0.07 AND q<24",
        "d<0 AND q<24",
        "d>=0 AND q>=0",
    }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "sum(price*discount) AS revenue,avg(price*discount) AS av,count(*) AS n",
            .filter = filter,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = 64;
            args.start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            for (want.state.accumulators, got.state.accumulators) |w, g| switch (w) {
                .sum_f => |value| {
                    try testing.expectEqual(value != null, g.sum_f != null);
                    if (value) |v| try testing.expectEqual(@as(u64, @bitCast(v)), @as(u64, @bitCast(g.sum_f.?)));
                },
                .avg => |avg| {
                    try testing.expectEqual(avg.count, g.avg.count);
                    try testing.expectEqual(@as(u64, @bitCast(avg.sum)), @as(u64, @bitCast(g.avg.sum)));
                },
                .count => |count| try testing.expectEqual(count, g.count),
                else => unreachable,
            };
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        }
    }
}

test "multi aggregate dictionary keys survive releasing slices and materialized fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const empty = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{ .name = "x", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "y", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
    };
    for (0..16) |rg| {
        var k: [257][]const u8 = undefined;
        var b: [257][]const u8 = undefined;
        var x: [257]f64 = undefined;
        var y: [257]f64 = undefined;
        var f: [257]i64 = undefined;
        var d: [257]u32 = undefined;
        var bd: [257]u32 = undefined;
        for (0..257) |r| {
            k[r] = if (r % 2 == 0) "A" else "B";
            b[r] = try std.fmt.allocPrint(a, "dictionary-{d}", .{if (rg == 14) r else (r + rg) % 64});
            x[r] = switch (r % 5) {
                0 => 1e16,
                1 => 1,
                2 => -1e16,
                3 => -0.0,
                else => 0.1,
            };
            y[r] = @as(f64, @floatFromInt(r % 11)) / 100;
            f[r] = @intCast(r % 13);
            d[r] = @intFromBool(r % 17 != 0);
            bd[r] = @intFromBool(rg != 15 and r % 19 != 0);
        }
        var cols = empty;
        cols[0].col = .{ .string = .{ .values = &k, .def_levels = &d, .max_def = 1 } };
        cols[1].col = .{ .string = .{ .values = &b, .def_levels = &bd, .max_def = 1 } };
        cols[2].col = .{ .f64 = .{ .values = &x, .def_levels = &d, .max_def = 1 } };
        cols[3].col = .{ .f64 = .{ .values = &y, .def_levels = &bd, .max_def = 1 } };
        cols[4].col = .{ .i64 = .{ .values = &f } };
        try file.append(&cols, 257, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&empty);
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| for (rg.columns.items) |*c| {
        c.meta_data.?.statistics = null;
    };
    const mapped = [_]Input{.{ .name = "coded-multi.parquet", .bytes = bytes }};
    for ([_][]const u8{ "f<7", "f<0", "f>=0" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .filter = filter,
            .aggregate = "sum(x) AS s,avg(x) AS av,sum(x*(1-y)) AS discounted,sum(x*(1-y)*(1+y)) AS charge,avg(y) AS ay,count(*) AS n,count(y) AS ny",
            .group_by = "k,b",
            .encoded = false,
            .dense = false,
            .integer_records = false,
            .byte_records = false,
            .ndv_policy = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64, 4096 }) |batch| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.encoded = true;
            args.start_reads = .{ .ctx = &window, .start = ReleasingScanWindow.start, .abandon = ReleasingScanWindow.abandon };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        };
        args.inputs = &mapped;
    }
}

test "exact multi aggregate roots preserve state through releasing reader windows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const empty = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{
            .name = "x",
            .parquet_type = .INT64,
            .nullable = true,
            .dec = .{ .precision = 15, .scale = 2 },
            .col = .{ .i128 = .{ .values = &.{} } },
        },
        .{
            .name = "y",
            .parquet_type = .INT64,
            .nullable = true,
            .dec = .{ .precision = 15, .scale = 2 },
            .col = .{ .i128 = .{ .values = &.{} } },
        },
        .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
    };
    for (0..16) |rg| {
        var k: [257][]const u8 = undefined;
        var b: [257][]const u8 = undefined;
        var x: [257]i128 = undefined;
        var y: [257]i128 = undefined;
        var f: [257]i64 = undefined;
        var d: [257]u32 = undefined;
        var bd: [257]u32 = undefined;
        for (0..257) |r| {
            k[r] = if (r % 2 == 0) "A" else "B";
            b[r] = try std.fmt.allocPrint(a, "dictionary-{d}", .{if (rg == 14) r else (r + rg) % 64});
            x[r] = if (r % 13 == 0) 10000000000000 else @as(i128, @intCast(r * 137)) - 12000;
            y[r] = if (r % 13 == 0) 100000 else @intCast(r % 11);
            f[r] = @intCast(r % 13);
            d[r] = @intFromBool(r % 17 != 0);
            bd[r] = @intFromBool(rg != 15 and r % 19 != 0);
        }
        var cols = empty;
        cols[0].col = .{ .string = .{ .values = &k, .def_levels = &d, .max_def = 1 } };
        cols[1].col = .{ .string = .{ .values = &b, .def_levels = &bd, .max_def = 1 } };
        cols[2].col = .{ .i128 = .{ .values = &x, .def_levels = &d, .max_def = 1 } };
        cols[3].col = .{ .i128 = .{ .values = &y, .def_levels = &bd, .max_def = 1 } };
        cols[4].col = .{ .i64 = .{ .values = &f } };
        try file.append(&cols, 257, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&empty);
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| for (rg.columns.items) |*c| {
        c.meta_data.?.statistics = null;
    };
    const mapped = [_]Input{.{ .name = "exact-multi.parquet", .bytes = bytes }};
    for ([_][]const u8{ "f<7", "f<0", "f>=0" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .filter = filter,
            .aggregate = "sum(x) AS s,avg(x) AS av,sum(x*(1-y)) AS discounted,sum(x*(1-y)*(1+y)) AS charge," ++
                "avg(y) AS ay," ++
                "sum(x*(1-y)) AS repeated,avg(x*(1-y)) AS repeated_avg,count(*) AS n,count(y) AS ny",
            .group_by = "k,b",
            .encoded = false,
            .dense = false,
            .integer_records = false,
            .byte_records = false,
            .ndv_policy = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64, 4096 }) |batch| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.encoded = true;
            args.start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        };
        args.inputs = &mapped;
    }
}

test "duplicate integer counts keep levels-only rows through releasing readers" {
    const Batch = @import("filter/eval.zig").Batch;
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    inline for (.{ i32, i64 }) |T| {
        const phys: schema.Type = if (T == i32) .INT32 else .INT64;
        var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        var expected: [3]u64 = @splat(0);
        for (0..16) |rg| {
            var values: [193]T = undefined;
            var f: [193]i64 = undefined;
            var defs: [193]u32 = undefined;
            for (&values, &f, &defs, 0..) |*v, *fv, *d, r| {
                v.* = @intCast(@as(i64, @intCast(r * 137 + rg * 73)) - 15000);
                fv.* = @intCast((r + rg) % 13);
                d.* = @intFromBool(r % 11 != 0 and rg != 15);
                if (d.* != 0) {
                    expected[0] += 1;
                    if (fv.* < 7) expected[1] += 1;
                }
            }
            try file.append(&.{
                .{ .name = "v", .parquet_type = phys, .nullable = true, .col = @unionInit(Batch.Column, @typeName(T), .{
                    .values = &values,
                    .def_levels = &defs,
                    .max_def = 1,
                }) },
                .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &f } } },
            }, values.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{
                .name = "v",
                .parquet_type = phys,
                .nullable = true,
                .col = @unionInit(Batch.Column, @typeName(T), .{ .values = &.{} }),
            },
            .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const bytes = sink.buffer.items;
        const meta = try metadata.open(a, bytes);
        for (meta.row_groups.items) |*rg| for (rg.columns.items) |*c| {
            c.meta_data.?.statistics = null;
        };
        for ([_]?[]const u8{ null, "f<7", "f<0" }, expected) |filter, want| {
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
                var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
                const inputs = [_]Input{window.input(bytes)};
                var got = try runMultiAggregate(gpa, a, .{
                    .inputs = &inputs,
                    .metas = (&meta)[0..1],
                    .filter = filter,
                    .aggregate = "count(v) AS a,count(v) AS b",
                    .parallelism = j,
                    .decode_batch_rows = batch,
                    .scan_all = true,
                    .min_bytes_per_worker = 1,
                    .start_reads = .{
                        .ctx = &window,
                        .start = ReleasingScanWindow.start,
                        .abandon = ReleasingScanWindow.abandon,
                    },
                });
                defer got.deinit(gpa);
                try testing.expectEqual(want, got.state.accumulators[0].count);
                try testing.expectEqual(want, got.state.accumulators[1].count);
                try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
                try testing.expectEqual(@as(usize, 0), window.abandoned);
            };
        }
    }
}





test "packed composite count survives releasing windows and late dictionary refusal" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const path = "ci/fixtures/parquet/packed_count_indexed.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer gpa.free(bytes);
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |rg| for (rg.columns.items) |col| {
        try testing.expect(col.offset_index_offset != null and col.column_index_offset != null);
    };
    const mapped = [_]Input{.{ .name = "packed.parquet", .bytes = bytes }};
    const Reader = struct {
        window: ReleasingScanWindow,
        fault: bool,
        changed: bool = false,
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
        fn ready(_: *anyopaque, _: usize) anyerror!void {}
        fn start(_: *anyopaque) anyerror!void {}
        fn planned(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!self.fault or self.changed) return;
            self.changed = true;
            // The union owns the original dictionary. A late reader page now has another valid value.
            const cm = self.window.meta.row_groups.items[15].columns.items[0].meta_data.?;
            const offset: usize = @intCast(cm.dictionary_page_offset.?);
            var reader = thrift_mod.Reader.init(self.window.bytes[offset..]);
            _ = schema.PageHeader.read(&reader) catch unreachable;
            std.mem.writeInt(i64, self.window.bytes[offset + reader.pos ..][0..8], 987654321, .little);
        }
        fn abandon(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            ReleasingScanWindow.abandon(&self.window);
        }
    };
    for ([_]?[]const u8{ null, "f < 0" }) |filter| for ([_][]const u8{ "k,f", "k,b,f" }, 0..) |keys, shape| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(*) AS c",
            .group_by = keys,
            .filter = filter,
            .integer_records = false,
            .msd_records = false,
            .byte_records = false,
            .typed_answer = false,
            .plain_rules = false,
            .scan_all = false,
            .min_bytes_per_worker = 1,
            .max_memory = 256 << 20,
            .sketch_topk = .{ .cell_bits = 10, .admit_per_row = 1 },
            .result_order = .{ .keys = &.{.{ .column = shape + 2, .descending = true }}, .limit = 3, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        // Direct preparation cannot silently retry: matching EXPLAIN must execute the packed owner.
        if (shape == 0) for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            var mapped_args = args;
            mapped_args.parallelism = j;
            mapped_args.decode_batch_rows = batch;
            mapped_args.integer_records = true;
            mapped_args.integer_records_min_rows = 0;
            var budget: core_memory.SharedBudget = .{ .limit = args.max_memory };
            mapped_args.ledger = &budget;
            var taken: std.EnumSet(FastPath) = .{};
            mapped_args.taken = &taken;
            const explanation = try explainAggregate(gpa, trial.allocator(), mapped_args);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, trial.allocator(), mapped_args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        };
        for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |fault| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            const ta = trial.allocator();
            var reader: Reader = .{ .window = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta }, .fault = fault };
            var input = reader.window.input(bytes);
            input.dict_wait = .{ .ctx = &reader, .wait = Reader.ready };
            var budget: core_memory.SharedBudget = .{ .limit = args.max_memory };
            args.ledger = &budget;
            args.inputs = (&input)[0..1];
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            args.start_reads = .{
                .ctx = &reader,
                .start = Reader.start,
                .planned = Reader.planned,
                .abandon = Reader.abandon,
            };
            args.dict_probe = .{ .ctx = &reader, .add = Reader.probe };
            const explanation = try explainAggregate(gpa, ta, args);
            try testing.expect(explanation.record_path != null);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            if (fault) {
                var got = try runMultiAggregate(gpa, ta, args);
                defer got.deinit(gpa);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            } else {
                if (j == 16) spawn_util.injectFailureAfter(if (batch == 0) 0 else 3);
                defer spawn_util.resetFailure();
                var got = try prepareAggregate(gpa, ta, args, false);
                defer got.deinit(gpa);
                try testing.expect(taken.contains(.integer_records));
                try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
                if (j == 16) try testing.expect(spawn_util.failuresInjected() > 0);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            }
            try testing.expectEqual(@as(usize, 16), reader.window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, @intFromBool(fault)), reader.window.abandoned);
            try testing.expectEqual(fault, reader.changed);
            try testing.expect(budget.used.load(.monotonic) < 1 << 20);
        };
        if (shape == 1) {
            args.inputs = &mapped;
            args.start_reads = null;
            args.dict_probe = null;
            args.taken = null;
            args.ledger = null;
            args.parallelism = 1;
            args.decode_batch_rows = 64;
            const bad = try a.dupe(u8, bytes);
            for ([_]usize{ 0, 2 }) |rg| for ([_]usize{ 0, 2 }) |ci| {
                const cm = meta.row_groups.items[rg].columns.items[ci].meta_data.?;
                var header = thrift_mod.Reader.init(bad[@intCast(cm.data_page_offset)..]);
                const page = try schema.PageHeader.read(&header);
                const body = @as(usize, @intCast(cm.data_page_offset)) + header.pos;
                @memset(bad[body..][0..@min(12, @as(usize, @intCast(page.compressed_page_size)))], 0xff);
            };
            var failure_args = args;
            var expected_error: ?anyerror = null;
            var expected_page: ?u32 = null;
            for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |records| {
                var trial = std.heap.ArenaAllocator.init(gpa);
                defer trial.deinit();
                var reader: Reader = .{
                    .window = .{ .bytes = try trial.allocator().dupe(u8, bad), .meta = &meta },
                    .fault = false,
                };
                var input = reader.window.input(bad);
                input.dict_wait = .{ .ctx = &reader, .wait = Reader.ready };
                failure_args.inputs = (&input)[0..1];
                failure_args.dict_probe = .{ .ctx = &reader, .add = Reader.probe };
                failure_args.start_reads = .{ .ctx = &reader, .start = Reader.start, .abandon = Reader.abandon };
                var budget: core_memory.SharedBudget = .{ .limit = args.max_memory };
                failure_args.ledger = &budget;
                failure_args.parallelism = j;
                failure_args.decode_batch_rows = batch;
                failure_args.integer_records = records;
                var taken: std.EnumSet(FastPath) = .{};
                failure_args.taken = &taken;
                const err = failure: {
                    var unexpected = (if (filter == null)
                        prepareAggregate(gpa, trial.allocator(), failure_args, false)
                    else
                        runMultiAggregate(gpa, trial.allocator(), failure_args)) catch |e| break :failure e;
                    unexpected.deinit(gpa);
                    return error.TestUnexpectedResult;
                };
                try testing.expect(consumer.isDecodeError(err));
                if (records and filter == null) try testing.expect(taken.contains(.integer_records));
                const site = consumer.decodeFailure().?;
                try testing.expectEqual(@as(usize, 0), site.column);
                try testing.expect(site.rg == &meta.row_groups.items[0]);
                if (expected_error) |want_error| {
                    try testing.expectEqual(want_error, err);
                    try testing.expectEqual(expected_page, site.page);
                } else {
                    expected_error = err;
                    expected_page = site.page;
                }
                try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
            };
            var fixed: NoInPlaceResize = .{ .child = gpa };
            if (filter == null) try testing.checkAllAllocationFailures(fixed.allocator(), packedAllocationTrial, .{
                args, want.output.group_rows.?,
            });
        }
    };
}

test "integer owners prepay and retain duplicated output names in the query ledger" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const columns = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .parquet_type = .INT64,
            .nullable = false,
            .col = .{ .i64 = .{ .values = &.{ 1, 1, 2, 2, 3, 3, 4, 4 } } },
        },
        .{
            .name = "v",
            .parquet_type = .INT64,
            .nullable = false,
            .col = .{ .i64 = .{ .values = &.{ 2, -1, 4, 3, -5, 1, 7, 6 } } },
        },
    };
    try file.append(&columns, 8, .SNAPPY, .{}, .{});
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    const input = [_]Input{.{ .name = "owner-names.parquet", .bytes = sink.buffer.items }};
    const long_name: [4096]u8 = @splat('x');
    for (0..3) |mode| {
        const aggregate = try std.fmt.allocPrint(a, "{s} AS {s}{s}", .{
            if (mode == 1) "count(*)" else "sum(v)", &long_name, if (mode == 2) ",count(*) AS n" else "",
        });
        var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
        var taken: std.EnumSet(FastPath) = .{};
        const args: MultiAggArgs = .{
            .inputs = &input,
            .metas = (&meta)[0..1],
            .aggregate = aggregate,
            .group_by = "k",
            .integer_records = true,
            .integer_records_min_rows = 0,
            .typed_answer = true,
            .ndv_policy = false,
            .byte_records = false,
            .msd_records = false,
            .plain_rules = false,
            .scan_all = true,
            .ledger = &budget,
            .max_memory = budget.limit,
            .taken = &taken,
            .result_order = if (mode == 2) .{} else .{
                .keys = &.{.{ .column = 1, .descending = true }},
                .limit = 2,
            },
        };
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.record_path != null);
        var got = try prepareAggregate(gpa, a, args, false);
        try testing.expect(taken.contains(.integer_records));
        if (mode == 2) {
            const state = got.output.integer_stream.?.state;
            var name_bytes = got.output.group_cols.?.len * @sizeOf([]const u8);
            for (got.output.group_cols.?) |name| name_bytes += name.len;
            try testing.expectEqual(name_bytes, state.output_credit);
            try testing.expectEqual(explanation.record_path.?.reservation, state.charged);
            try testing.expectEqual(state.charged, budget.used.load(.monotonic));
            got.deinit(gpa);
            try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        } else {
            defer got.deinit(gpa);
            try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
        }
    }
}

test "filtered COUNT record owners consume only survivors on indexed mapped and releasing reads" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const bytes = (try loadFixture("ci/fixtures/parquet/filtered_records_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    const meta = try metadata.open(a, bytes);
    const mapped = [_]Input{.{ .name = "selected-indexed.parquet", .bytes = bytes }};
    const Ready = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {}
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
    };
    for ([_][]const u8{ "f < 160 AND (b IS NULL OR b != '')", "f > 1 AND f < 1" }, 0..) |filter, empty|
        for ([_][]const u8{ "k", "b", "b,f", "k,f" }, 0..) |keys, mode| {
            const base: MultiAggArgs = .{
                .inputs = &mapped,
                .metas = &.{meta},
                .aggregate = "count(*) AS c",
                .group_by = keys,
                .filter = filter,
                .scan_all = empty != 0,
                .integer_records = false,
                .byte_records = false,
                .msd_records = false,
                .dense = false,
                .ndv_policy = false,
                .plain_rules = false,
                .sketch_topk = null,
                .min_bytes_per_worker = 1,
                .max_memory = 512 << 20,
                .result_order = .{
                    .keys = &.{.{ .column = if (mode < 2) 1 else 2, .descending = true }},
                    .limit = 3,
                    .offset = 1,
                },
            };
            var want = try runMultiAggregate(gpa, a, base);
            defer want.deinit(gpa);
            try want.output.applyResultOrder(gpa, base.result_order);
            if (empty == 0)
                try testing.expect(want.rows_matched > 0 and want.rows_matched < want.rows_in)
            else
                try testing.expectEqual(@as(i64, 0), want.rows_matched);
            for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |gated| {
                if (empty != 0 and j != 1 and j != 16) continue;
                var trial = std.heap.ArenaAllocator.init(gpa);
                defer trial.deinit();
                const ta = trial.allocator();
                var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta };
                var input = window.input(bytes);
                input.dict_wait = .{ .ctx = &window, .wait = Ready.wait };
                var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
                var taken: std.EnumSet(FastPath) = .{};
                var args = base;
                args.inputs = if (gated) (&input)[0..1] else &mapped;
                args.parallelism = j;
                args.decode_batch_rows = batch;
                args.integer_records = mode == 0 or mode == 3;
                args.integer_records_min_rows = 0;
                args.byte_records = mode == 1;
                args.byte_records_min_rows = 0;
                args.msd_records = mode == 2;
                args.msd_records_min_rows = 0;
                args.dense = true;
                args.ndv_policy = true;
                args.plain_rules = false;
                args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 };
                args.ledger = &budget;
                args.taken = &taken;
                if (gated) {
                    args.dict_probe = .{ .ctx = &window, .add = Ready.probe };
                    args.start_reads = .{
                        .ctx = &window,
                        .start = ReleasingScanWindow.start,
                        .abandon = ReleasingScanWindow.abandon,
                    };
                }
                const explanation = try explainAggregate(gpa, ta, args);
                const selected_owner = mode == 3 and explanation.record_path.?.layout == .packed_count_top_h;
                try testing.expectEqual(
                    @as(@FieldType(RecordAdmission, "layout"), if (mode == 1) .byte_count_top_h else if (mode == 2)
                        .msd_count_top_h
                    else if (selected_owner)
                        .packed_count_top_h
                    else
                        .integer_count_top_h),
                    explanation.record_path.?.layout,
                );
                var got = try prepareAggregate(gpa, ta, args, false);
                defer got.deinit(gpa);
                try testing.expect(taken.contains(if (mode == 2) .msd_records else .integer_records));
                try testing.expectEqual(want.rows_in, got.rows_in);
                try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                try testing.expectEqual(want.rows_matched, got.rows_matched);
                try testing.expectEqual(want.row_groups_pruned, got.row_groups_pruned);
                try testing.expectEqual(want.row_groups_full_match, got.row_groups_full_match);
                const schema_credit = if (selected_owner)
                    got.state.group_col_types.?.len * @sizeOf(GroupColType) +
                        explanation.workers * (@sizeOf(Worker) + @sizeOf(expr_agg.GroupTable))
                else
                    0;
                try testing.expectEqual(ownedGroupOutputBytes(got.output) + schema_credit, budget.used.load(.monotonic));
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows orelse &.{}, got.output.group_rows orelse &.{});
                if (gated) {
                    try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
                    try testing.expectEqual(@as(usize, 0), window.abandoned);
                }
            };
        };
}

test "unfiltered integer COUNT owns raw nullable keys without a dictionary union across readers and workers" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = true,
        .col = .{ .i64 = .{ .values = &.{} } },
    }};
    for (0..16) |rg| {
        var values: [257]i64 = undefined;
        var defs: [257]u32 = undefined;
        for (&values, &defs, 0..) |*v, *d, r| {
            v.* = @as(i64, @intCast((r * 37 + rg) % 193)) * (1 << 40) - (1 << 50);
            d.* = @intFromBool((r + rg) % 17 != 0);
        }
        columns[0].col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } };
        try file.append(&columns, values.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    const mapped = [_]Input{.{ .name = "raw-count.parquet", .bytes = sink.buffer.items }};
    const base: MultiAggArgs = .{
        .inputs = &mapped,
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .integer_records = false,
        .integer_records_min_rows = 1,
        .byte_records = false,
        .msd_records = false,
        .ndv_policy = false,
        .dense = false,
        .sketch_topk = null,
        .min_bytes_per_worker = 1,
        .max_memory = 128 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10, .offset = 2 },
    };
    var want = try runMultiAggregate(gpa, a, base);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, base.result_order);
    for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |gated| {
        var trial = std.heap.ArenaAllocator.init(gpa);
        defer trial.deinit();
        const ta = trial.allocator();
        var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, sink.buffer.items), .meta = &meta };
        const inputs = [_]Input{window.input(sink.buffer.items)};
        var args = base;
        args.inputs = if (gated) &inputs else &mapped;
        args.start_reads = if (gated) .{
            .ctx = &window,
            .start = ReleasingScanWindow.start,
            .abandon = ReleasingScanWindow.abandon,
        } else null;
        args.integer_records = true;
        args.parallelism = j;
        args.decode_batch_rows = batch;
        var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
        args.ledger = &budget;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, ta, args);
        try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
        var got = try prepareAggregate(gpa, ta, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqual(want.rows_matched, got.rows_matched);
        if (gated) {
            try testing.expectEqual(meta.row_groups.items.len, window.waits.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        }
    };
}

test "unfiltered record COUNT retains row identities instead of a dictionary histogram" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var values: [4096]i64 = undefined;
    for (&values, 0..) |*v, i| v.* = @as(i64, @intCast(i * 7 % 16)) * (1 << 40);
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "unused", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &values } } },
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &values } } },
    };
    for (0..2) |_| try file.append(&columns, values.len, .SNAPPY, .{}, .{});
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    const base: MultiAggArgs = .{
        .inputs = &.{.{ .name = "hist-count.parquet", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .integer_records_min_rows = 0,
        .ndv_policy = false,
        .sketch_topk = null,
        .max_memory = 128 << 20,
        .scan_all = true,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |encoded| {
        var args = base;
        args.decode_batch_rows = batch;
        args.encoded = encoded;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(@as(i64, 8192), got.rows_matched);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqual(@as(usize, 10), got.output.group_rows.?.len);
        for (got.output.group_rows.?, 0..) |row, i| {
            try testing.expectEqual(@as(i128, @intCast(i * 7 % 16)) * (1 << 40), row[0].i);
            try testing.expectEqual(@as(i128, 512), row[1].i);
        }
    };
}

test "filtered hot COUNT certificate proves selected counts and first appearances" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const bytes = (try loadFixture("ci/fixtures/parquet/count_certificate_dict.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    const inputs = [_]Input{.{ .name = "filtered-certificate.parquet", .bytes = bytes }};
    var certified = false;
    for ([_][]const u8{ "k", "k,f" }) |keys| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = keys,
            .filter = "f >= 0",
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .ndv_policy = false,
            .plain_rules = false,
            .sketch_topk = null,
            .max_memory = 512 << 20,
            .min_bytes_per_worker = 1,
            .result_order = .{
                .keys = &.{.{ .column = if (keys.len == 1) 1 else 2, .descending = true }},
                .limit = 3,
            },
        };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            var args = base;
            var outcome: SketchTopK.HotOutcome = .declined;
            args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1, .hot_outcome = &outcome };
            args.parallelism = j;
            args.decode_batch_rows = batch;
            var got = try runMultiAggregate(gpa, trial.allocator(), args);
            defer got.deinit(gpa);
            certified = certified or outcome == .certified or outcome == .residual;
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_scanned, got.rows_scanned);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        };
    }
    try testing.expect(certified);
}

test "filtered COUNT record ties start at the first selected row across mask words" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var ints: [70]i64 = @splat(40);
    var strings: [70][]const u8 = @splat("");
    var defs: [70]u32 = @splat(1);
    var flags: [70]i32 = @splat(0);
    ints[1] = 50;
    ints[2] = 60;
    strings[1] = "a\x00b";
    strings[2] = "z";
    defs[64] = 0;
    for ([_]usize{ 1, 2, 63, 64 }) |i| flags[i] = 1;
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .parquet_type = .INT64,
            .nullable = true,
            .col = .{ .i64 = .{ .values = &ints, .def_levels = &defs, .max_def = 1 } },
        },
        .{
            .name = "b",
            .parquet_type = .BYTE_ARRAY,
            .nullable = true,
            .col = .{ .string = .{ .values = &strings, .def_levels = &defs, .max_def = 1 } },
        },
        .{ .name = "f", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &flags } } },
    };
    for (0..2) |_| try file.append(&cols, flags.len, .SNAPPY, .{}, .{});
    _ = try file.finish(&cols);
    const inputs = [_]Input{.{ .name = "first-selected.parquet", .bytes = sink.buffer.items }};
    for ([_][]const u8{ "k", "b" }) |key| for ([_]usize{ 0, 64 }) |batch| {
        var taken: std.EnumSet(FastPath) = .{};
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = key,
            .filter = "f = 1",
            .integer_records = key[0] == 'k',
            .integer_records_min_rows = 0,
            .byte_records = key[0] == 'b',
            .byte_records_min_rows = 0,
            .msd_records = false,
            .decode_batch_rows = batch,
            .min_bytes_per_worker = 1,
            .parallelism = 2,
            .max_memory = 512 << 20,
            .taken = &taken,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 1 },
        };
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(@as(i64, 8), got.rows_matched);
        try got.output.applyResultOrder(gpa, args.result_order);
        const rows = got.output.group_rows.?;
        try testing.expectEqual(@as(usize, 3), rows.len);
        if (key[0] == 'k') {
            try testing.expectEqual(@as(i128, 60), rows[0][0].i);
            try testing.expectEqual(@as(i128, 40), rows[1][0].i);
        } else {
            try testing.expectEqualStrings("z", rows[0][0].s);
            try testing.expectEqualStrings("", rows[1][0].s);
        }
        try testing.expectEqual(.null_val, std.meta.activeTag(rows[2][0]));
        for (rows) |row| try testing.expectEqual(@as(i128, 2), row[1].i);
    };
}

test "filtered record pruning preserves canonical failures and stable replay of skipped corrupt pages" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const bytes = (try loadFixture("ci/fixtures/parquet/filtered_records_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    const meta = try metadata.open(a, bytes);
    const Ready = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {}
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
    };
    for ([_][]const u8{ "b,f", "k,f" }, 0..) |keys, mode| {
        const ci: usize = if (mode == 0) 1 else 0;
        const chunk = meta.row_groups.items[0].columns.items[ci];
        var index_reader = thrift_mod.Reader.init(bytes[@intCast(chunk.offset_index_offset.?)..]);
        const index = try schema.OffsetIndex.read(a, &index_reader);
        try testing.expect(index.page_locations.items.len > 2);
        const last = index.page_locations.items.len - 1;
        try testing.expect(index.page_locations.items[last].first_row_index > 160);
        for ([_]usize{ 0, last }) |page_index| {
            const bad = try a.dupe(u8, bytes);
            const offset: usize = @intCast(index.page_locations.items[page_index].offset);
            var header_reader = thrift_mod.Reader.init(bad[offset..]);
            const header = try schema.PageHeader.read(&header_reader);
            const body = offset + header_reader.pos;
            @memset(bad[body..][0..@min(12, @as(usize, @intCast(header.compressed_page_size)))], 0xff);
            const mapped = [_]Input{.{ .name = "corrupt-selected.parquet", .bytes = bad }};
            for ([_]usize{ 0, 64 }) |batch| {
                const base: MultiAggArgs = .{
                    .inputs = &mapped,
                    .metas = &.{meta},
                    .aggregate = "count(*) AS c",
                    .group_by = keys,
                    .filter = "f < 160",
                    .integer_records = false,
                    .msd_records = false,
                    .byte_records = false,
                    .dense = false,
                    .ndv_policy = false,
                    .plain_rules = false,
                    .sketch_topk = null,
                    .decode_batch_rows = batch,
                    .min_bytes_per_worker = 1,
                    .max_memory = 512 << 20,
                    .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 3 },
                };
                var expected_error: ?anyerror = null;
                var expected_column: usize = 0;
                var expected_page: ?u32 = null;
                var want: ?MultiAggResult = null;
                if (runMultiAggregate(gpa, a, base)) |result| {
                    want = result;
                    try want.?.output.applyResultOrder(gpa, base.result_order);
                } else |err| {
                    try testing.expect(consumer.isDecodeError(err));
                    expected_error = err;
                    expected_column = consumer.decodeFailure().?.column;
                    expected_page = consumer.decodeFailure().?.page;
                }
                defer if (want) |*result| result.deinit(gpa);
                if (page_index == 0) try testing.expect(expected_error != null);
                if (page_index == last and batch == 0) try testing.expect(expected_error == null);
                for (1..17) |j| for ([_]bool{ false, true }) |gated| {
                    var trial = std.heap.ArenaAllocator.init(gpa);
                    defer trial.deinit();
                    const ta = trial.allocator();
                    var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, bad), .meta = &meta };
                    var input = window.input(bad);
                    input.dict_wait = .{ .ctx = &window, .wait = Ready.wait };
                    var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
                    var args = base;
                    args.inputs = if (gated) (&input)[0..1] else &mapped;
                    args.integer_records = mode == 1;
                    args.integer_records_min_rows = 0;
                    args.msd_records = mode == 0;
                    args.msd_records_min_rows = 0;
                    args.dense = true;
                    args.ndv_policy = true;
                    args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 };
                    args.parallelism = j;
                    args.ledger = &budget;
                    if (gated) {
                        args.dict_probe = .{ .ctx = &window, .add = Ready.probe };
                        args.start_reads = .{
                            .ctx = &window,
                            .start = ReleasingScanWindow.start,
                            .abandon = ReleasingScanWindow.abandon,
                        };
                    }
                    if (expected_error) |expected| {
                        const err = failed: {
                            var unexpected = runMultiAggregate(gpa, ta, args) catch |e| break :failed e;
                            unexpected.deinit(gpa);
                            return error.TestUnexpectedResult;
                        };
                        try testing.expectEqual(expected, err);
                        try testing.expectEqual(expected_column, consumer.decodeFailure().?.column);
                        try testing.expectEqual(expected_page, consumer.decodeFailure().?.page);
                        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
                    } else {
                        var got = runMultiAggregate(gpa, ta, args) catch |err| {
                            std.debug.print(
                                "filtered pruning mode={d} page={d} batch={d} j={d} gated={} error={s}\n",
                                .{ mode, page_index, batch, j, gated, @errorName(err) },
                            );
                            return err;
                        };
                        defer got.deinit(gpa);
                        try got.output.applyResultOrder(gpa, args.result_order);
                        try testing.expectEqualDeep(want.?.output.group_rows, got.output.group_rows);
                        if (gated) try testing.expect(window.abandoned > 0);
                    }
                };
            }
        }
    }
}

test "filtered COUNT sorted NULLs and known seams use one exact record pass with honest admission" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    for ([_][]const u8{ "ordered", "seams" }) |shape| {
        const path = try std.fmt.allocPrint(a, "ci/fixtures/parquet/filtered_records_{s}.parquet", .{shape});
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        const meta = try metadata.open(a, bytes);
        const mapped = [_]Input{.{ .name = path, .bytes = bytes }};
        const base: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = &.{meta},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .filter = "f = 1",
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .ndv_policy = false,
            .dense = false,
            .plain_rules = false,
            .sketch_topk = null,
            .max_memory = 128 << 20,
            .min_bytes_per_worker = 1,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |gated| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            const ta = trial.allocator();
            var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            var args = base;
            args.inputs = if (gated) &inputs else &mapped;
            args.start_reads = if (gated) .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            } else null;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const explanation = try explainAggregate(gpa, ta, args);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, ta, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try testing.expectEqual(@as(usize, 0), window.abandoned);
            if (gated) try testing.expectEqual(meta.row_groups.items.len, window.waits.load(.monotonic));
        };
        // The unknown PLAIN domain has at most a few estimated survivors, despite thousands of raw rows.
        var selective = base;
        selective.filter = "rid < 2";
        selective.integer_records = true;
        selective.integer_records_min_rows = 100;
        const explanation = try explainAggregate(gpa, a, selective);
        try testing.expect(explanation.record_path == null);
        var taken: std.EnumSet(FastPath) = .{};
        selective.taken = &taken;
        var got = try prepareAggregate(gpa, a, selective, false);
        defer got.deinit(gpa);
        try testing.expect(!taken.contains(.integer_records));
        try testing.expectEqual(@as(i64, 2), got.rows_matched);
    }
}

test "dense quote refusal restores displaced partitions before consuming rows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var names: [4096][]const u8 = undefined;
    for (&names, 0..) |*name, i| name.* = try std.fmt.allocPrint(
        a,
        "a-long-dictionary-value-with-a-stable-shared-prefix-and-identity-{d}",
        .{i % 256},
    );
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .string = .{ .values = &names } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = false,
    }};
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.bytes.appendSlice(self.a, bytes);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        names.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 64);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "long-keys", .bytes = capture.bytes.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .parallelism = 64,
        .min_bytes_per_worker = 1,
        .max_memory = 4 << 20,
        .dense = true,
        .plain_rules = false,
    };
    const ordinary = try explainAggregate(gpa, a, args);
    try testing.expectEqual(.partitioned_hash, ordinary.plan.dispatch.strategy);
    args.plain_rules = true;
    const dense = try explainAggregate(gpa, a, args);
    try testing.expectEqualStrings("dense", dense.strategyName());
    try testing.expect(dense.plan.group_bytes.? <= args.max_memory / 8);
    try testing.expect(dense.plan.plain_bytes.? > args.max_memory / 8);
    var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const held = ledger.drawExact(args.max_memory - 1).?;
    args.ledger = &ledger;
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
    try testing.expect(taken.contains(.plain_rule));
    try testing.expect(!taken.contains(.dense) and !taken.contains(.worker_tables));
    try testing.expectEqual(held, ledger.used.load(.monotonic));
    var chain: Chain = .{ .parts_tried = false };
    const fallback = narrowed(args, args, taken, &chain, false).?;
    const restored = try explainAggregate(gpa, a, fallback);
    try testing.expectEqual(.partitioned_hash, restored.plan.dispatch.strategy);
    ledger.release(held);
    {
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        var got = try runMultiAggregate(gpa, a, fallback);
        defer got.deinit(gpa);
        try testing.expect(got.part_agg);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    }
}

test "integer and packed COUNT owner caps agree with EXPLAIN beyond sixty four workers" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var keys: [1025]i64 = undefined;
    var flags: [1025]i64 = undefined;
    for (&keys, &flags, 0..) |*key, *flag, i| {
        key.* = @as(i64, @intCast(i * 17 % 61)) * (1 << 40);
        flag.* = @intCast(i % 3);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &keys } } },
        .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &flags } } },
    };
    try file.append(&columns, keys.len, .SNAPPY, .{}, .{});
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    for (meta.row_groups.items[0].columns.items) |column|
        try testing.expect(column.meta_data.?.dictionary_page_offset != null);
    const inputs: [128]Input = @splat(.{ .name = "owner-cap.parquet", .bytes = sink.buffer.items });
    const metas: [128]schema.FileMetaData = @splat(meta);
    for ([_][]const u8{ "k", "k,f" }, 0..) |group_by, shape| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = "count(*) AS c",
            .group_by = group_by,
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .ndv_policy = false,
            .dense = false,
            .plain_rules = false,
            .sketch_topk = null,
            .min_bytes_per_worker = 1,
            .max_memory = 256 << 20,
            .result_order = .{ .keys = &.{.{ .column = shape + 1, .descending = true }}, .limit = 10, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for ([_]usize{ 64, 65, 96, 128 }) |j| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            var args = base;
            args.parallelism = j;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            var ready: u8 = 0;
            const Ready = struct {
                fn wait(_: *anyopaque, _: usize) anyerror!void {}
                fn done(_: *anyopaque, _: usize, _: usize) void {}
            };
            var readers = inputs;
            if (shape == 1) {
                for (&readers) |*input| {
                    input.plan_bytes = input.bytes;
                    input.gate = .{ .ctx = &ready, .wait = Ready.wait, .done = Ready.done };
                    input.dict_wait = .{ .ctx = &ready, .wait = Ready.wait };
                }
                args.inputs = &readers;
                args.ndv_policy = true;
                args.dense = true;
            }
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const explanation = try explainAggregate(gpa, trial.allocator(), args);
            try testing.expectEqual(@as(usize, owner_groups.max_workers), explanation.workers);
            if (explanation.record_path == null) std.debug.print(
                "owner cap shape={d} j={d} has no record admission\n",
                .{ shape, j },
            );
            try testing.expect(explanation.record_path != null);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, trial.allocator(), args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        }
    }
}

test "skewed single-key COUNT keeps dense and a sampled demotion can retry it" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var keys: [8192]i32 = undefined;
    var values: [8192]i32 = @splat(0);
    for (&keys, 0..) |*key, i| key.* = @intCast(i % 256);
    values[values.len - 1] = 1_000_000;
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &keys } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "f", .col = .{ .i32 = .{ .values = &values } }, .parquet_type = .INT32, .nullable = false },
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, bytes);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        keys.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "skewed-count", .bytes = capture.bytes.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .filter = "f < 1",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .dense = false,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        var taken: std.EnumSet(FastPath) = .{};
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.dense = true;
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.dense);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.dense));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
    args.filter = "f > 999999";
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    var rare = try prepareAggregate(gpa, a, args, false);
    defer rare.deinit(gpa);
    try testing.expect(!taken.contains(.dense) and taken.contains(.plain_rule));
    var chain: Chain = .{ .parts_tried = false };
    var retry = narrowed(args, args, taken, &chain, false).?;
    try testing.expect(!retry.plain_rules and retry.dense);
    retry.taken = null;
    try testing.expect((try explainAggregate(gpa, a, retry)).dense);
    var rescued = try prepareAggregate(gpa, a, retry, false);
    defer rescued.deinit(gpa);
    try rare.output.applyResultOrder(gpa, args.result_order);
    try rescued.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(rare.output.group_rows.?, rescued.output.group_rows.?);

    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};
    const filter = try filter_parser.parse(a, "f < 1", &meta, null);
    const plan: agg_plan.Plan = try .init(a, .{
        .filter = filter,
        .calls = &calls,
        .group_keys = null,
    }, &.{meta}, .fromFlags(true, false));
    const mapped = [_]Input{.{
        .name = "retained-skew",
        .bytes = &.{},
        .plan_bytes = capture.bytes.items,
        .logical_size = capture.bytes.items.len,
    }};
    const work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 1,
        .columns = plan.columns,
        .accumulators = &.{},
    }};
    const sampled = try sampledPassShare(gpa, a, &plan, &.{meta}, &mapped, &work, .{ .fast_levels = true }, null);
    try testing.expectEqual(@as(?f64, 8191.0 / 8192.0), sampled);
}

test "selective composite COUNT keeps its packed owner while wider filters retain dense lanes" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var ints: [8192]i32 = undefined;
    var names: [8192][]const u8 = undefined;
    var filter: [8192]i32 = undefined;
    for (&ints, &names, &filter, 0..) |*v, *s, *f, i| {
        v.* = @intCast(i % 3);
        s.* = try std.fmt.allocPrint(a, "key-{d}", .{i % 1024});
        f.* = @intCast(i);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "a", .col = .{ .i32 = .{ .values = &ints } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "b", .col = .{ .string = .{ .values = &names } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "f", .col = .{ .i32 = .{ .values = &filter } }, .parquet_type = .INT32, .nullable = false },
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, bytes);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        ints.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "selective-product", .bytes = capture.bytes.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "a,b",
        .filter = "f < 1",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .integer_records_min_rows = 0,
        .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 3 },
        .dense = false,
        .integer_records = false,
        .sketch_topk = .{ .cell_bits = 12 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        var taken: std.EnumSet(FastPath) = .{};
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.dense = true;
        args.integer_records = true;
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.record_path != null);
        try testing.expectEqual(.packed_count_top_h, explanation.record_path.?.layout);
        try testing.expect(!explanation.dense);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expect(!taken.contains(.dense));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
    args.filter = "f < 1000";
    args.taken = null;
    const wider = try explainAggregate(gpa, a, args);
    try testing.expect(wider.dense);
}



test "selected owner coded identity replays a late dictionary mismatch after earlier records" {
    try selectedIdentityReplayTrial(false);
}

test "M1 finish DISTINCT replays a late argument dictionary mismatch after completed groups and NULL presence" {
    try selectedIdentityReplayTrial(true);
}

test "selected COUNT setup refusal preserves an occupied ledger and falls back to ordinary grouping" {
    const gpa = testing.allocator;
    var fixture = std.heap.ArenaAllocator.init(gpa);
    defer fixture.deinit();
    const fa = fixture.allocator();
    const values = try fa.alloc(i32, 1 << 20);
    for (values, 0..) |*value, i| value.* = @intCast(i % 17);
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .i32 = .{ .values = values } },
        .parquet_type = .INT32,
        .nullable = false,
    }};
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = fa };
    _ = try consumer.writeAggregateRows(fa, sink.sink(), &columns, @intCast(values.len), .SNAPPY, .{}, .{});
    const meta = try metadata.open(fa, sink.buffer.items);
    const base: MultiAggArgs = .{
        .inputs = &.{.{ .name = "occupied", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .parallelism = 1,
        .max_memory = 64 << 20,
        .charge_decode = false,
        .dense = false,
        .plain_rules = false,
        .msd_records = false,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const occupied = 60 << 20;
    var baseline_rows: ?[][]const AggValue = null;
    var baseline_types: ?[]const GroupColType = null;
    var retained: [2]usize = undefined;
    for ([_]bool{ false, true }, 0..) |enabled, mode| {
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        const a = arena.allocator();
        var ledger: core_memory.SharedBudget = .{ .limit = base.max_memory };
        _ = ledger.drawExact(occupied).?;
        var args = base;
        args.ledger = &ledger;
        args.integer_records = enabled;
        if (enabled) {
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.packed_count_top_h, explanation.record_path.?.layout);
            try testing.expect(explanation.record_path.?.reservation <= args.max_memory);
            const O = owner_groups.Packed(u64, &.{}, u32);
            try testing.expect(O.State.charge(values.len, 1, 64) > args.max_memory - occupied);
        }
        var got = runMultiAggregate(gpa, a, args) catch |err| {
            std.debug.print("owner enabled={any}: {s}\n", .{ enabled, @errorName(err) });
            return err;
        };
        defer got.deinit(gpa);
        try testing.expect(!got.part_agg);
        try got.output.applyResultOrder(gpa, args.result_order);
        const rows = got.output.group_rows.?;
        try testing.expectEqual(@as(usize, 10), rows.len);
        if (!enabled) {
            const saved = try fa.alloc([]const AggValue, rows.len);
            for (saved, rows) |*row, original| row.* = try fa.dupe(AggValue, original);
            baseline_rows = saved;
            baseline_types = try fa.dupe(GroupColType, got.state.group_col_types.?);
        } else {
            try testing.expectEqualDeep(baseline_rows.?, rows);
            try testing.expectEqualDeep(baseline_types.?, got.state.group_col_types.?);
        }
        for (rows, 0..) |row, i| {
            try testing.expectEqual(@as(i64, @intCast(i)), row[0].i);
            try testing.expectEqual(@as(i64, 61681), row[1].i);
        }
        // Ordinary output retains its small query-lifetime credit. Setup refusal must add nothing to that credit.
        retained[mode] = ledger.used.load(.monotonic);
        try testing.expect(retained[mode] >= occupied);
        ledger.release(occupied);
        try testing.expectEqual(retained[mode] - occupied, ledger.used.load(.monotonic));
    }
    try testing.expectEqual(retained[0], retained[1]);
}

test "selected packed COUNT runs the typed owner for INT32 keys with original NULL ties and sliced files" {
    const gpa = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var keys: [256]i32 = undefined;
    var defs: [256]u32 = undefined;
    var filter_values: [256]i32 = undefined;
    for (&keys, &defs, &filter_values, 0..) |*key, *def, *f, i| {
        key.* = @as(i32, @intCast(i % 17)) - 8;
        def.* = @intFromBool(i % 23 != 0);
        f.* = @intCast(i % 3);
    }
    const columns = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .col = .{ .i32 = .{ .values = &keys, .def_levels = &defs, .max_def = 1 } },
            .parquet_type = .INT32,
            .nullable = true,
        },
        .{ .name = "f", .col = .{ .i32 = .{ .values = &filter_values } }, .parquet_type = .INT32, .nullable = false },
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        allocator: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.allocator, bytes);
        }
    };
    var capture: Capture = .{ .allocator = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        keys.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 32);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var args: MultiAggArgs = .{
        .inputs = &.{
            .{ .name = "first", .bytes = capture.bytes.items },
            .{ .name = "second", .bytes = capture.bytes.items },
        },
        .metas = &.{ meta, meta },
        .aggregate = "count(*) AS c,count(*) AS d",
        .group_by = "k",
        .filter = "f < 1",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 512 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7, .offset = 2 },
        .dense = false,
        .encoded = false,
        .plain_rules = false,
        .integer_records = false,
        .integer_records_min_rows = 0,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 4, 16 }) |jobs| for ([_]usize{ 0, 64 }) |window| {
        var taken: std.EnumSet(FastPath) = .{};
        args.integer_records = true;
        args.parallelism = jobs;
        args.decode_batch_rows = window;
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expectEqual(.packed_count_top_h, explanation.record_path.?.layout);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        try testing.expect(!got.state.group_col_types.?[1].nullable);
    };
    args.parallelism = 96;
    try testing.expectEqual(@as(usize, 64), (try explainAggregate(gpa, a, args)).workers);
}



test "integer COUNT repeated RG dictionaries stay coded after complete union refinement" {
    const gpa = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const values = try a.alloc(i64, 65536);
    for (values, 0..) |*value, i| value.* = @intCast((i % 4096) * 7919);
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .i64 = .{ .values = values } },
        .parquet_type = .INT64,
        .nullable = false,
    }};
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, @intCast(values.len), .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const key: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const single = ndv.footerEntries(&.{meta}, &.{key}).?;
    try testing.expect(single != 0);
    const copies: usize = @intCast(agg_dense.max_ids / single + 1);
    const groups = try a.alloc(schema.RowGroup, copies);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(copies);
    const summed = ndv.footerEntries(&.{meta}, &.{key}).?;
    try testing.expect(summed > agg_dense.max_ids and summed < @as(u64, @intCast(meta.num_rows)) / 4);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "repeated", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .max_memory = 8 << 30,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const explanation = try explainAggregate(gpa, a, args);
    try testing.expect(explanation.plan.ndv_stat.?.union_complete);
    try testing.expectEqual(@as(u64, 4096), explanation.plan.ndv_stat.?.hi);
    try testing.expect(explanation.record_path == null);
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    var got = try prepareAggregate(gpa, a, args, false);
    defer got.deinit(gpa);
    try testing.expect(!taken.contains(.integer_records));
    try got.output.applyResultOrder(gpa, args.result_order);
    const rows = got.output.group_rows.?;
    try testing.expectEqual(@as(usize, 10), rows.len);
    for (rows, 0..) |row, i| {
        try testing.expectEqual(@as(i64, @intCast(i * 7919)), row[0].i);
        try testing.expectEqual(@as(i64, @intCast(copies * 16)), row[1].i);
    }
    args.ndv_union = false;
    const no_union = try explainAggregate(gpa, a, args);
    try testing.expectEqual(.integer_count_top_h, no_union.record_path.?.layout);
}

test "M1 finish DISTINCT selected owners match sets for skew NULL groups arguments and the empty global group" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [4096]i32 = undefined;
    var x: [4096]i64 = undefined;
    var s: [4096][]const u8 = undefined;
    var kd: [4096]u32 = undefined;
    var xd: [4096]u32 = undefined;
    for (&k, &x, &s, &kd, &xd, 0..) |*key, *arg, *word, *key_def, *arg_def, i| {
        key.* = if (i % 8 == 0) 0 else if (i % 4 != 0) 1 else @intCast(i % 17);
        arg.* = @intCast(i % 13);
        word.* = try std.fmt.allocPrint(a, "dictionary-string-with-shared-prefix-{d}", .{i % 7});
        key_def.* = @intFromBool(i % 97 != 0);
        arg_def.* = @intFromBool(key.* != 0 and i % 11 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .col = .{ .i32 = .{ .values = &k, .def_levels = &kd, .max_def = 1 } },
            .parquet_type = .INT32,
            .nullable = true,
        },
        .{
            .name = "x",
            .col = .{ .i64 = .{ .values = &x, .def_levels = &xd, .max_def = 1 } },
            .parquet_type = .INT64,
            .nullable = true,
        },
        .{ .name = "s", .col = .{ .string = .{ .values = &s } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    const Case = struct { group: ?[]const u8, agg: []const u8, filter: ?[]const u8 = null, top: bool = false };
    const cases = [_]Case{
        .{ .group = "k", .agg = "count(DISTINCT x) AS d,count(*) AS c" },
        .{ .group = "s", .agg = "count(*) AS c,count(DISTINCT x) AS d", .top = true },
        .{ .group = "k,s", .agg = "count(DISTINCT x) AS d" },
        .{ .group = "k", .agg = "count(DISTINCT x) AS d", .filter = "k = 0" },
        .{ .group = null, .agg = "count(DISTINCT s) AS d,count(*) AS c" },
        .{ .group = null, .agg = "count(DISTINCT s) AS d", .filter = "k < 0" },
        .{ .group = "k", .agg = "count(DISTINCT x) AS d", .filter = "k < 0" },
    };
    for (cases) |c| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "M1-distinct", .bytes = bytes.buffer.items }},
            .grouped_distinct_bitmap = .off,
            .metas = &.{meta},
            .aggregate = c.agg,
            .group_by = c.group,
            .filter = c.filter,
            .max_memory = 512 << 20,
            .min_bytes_per_worker = 1,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .distinct_pairs = .off,
            .plain_rules = false,
            .result_order = if (c.top) .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 3 } else .{},
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]bool{ false, true }) |raw| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            if (raw and (c.group == null or std.mem.indexOf(u8, c.group.?, "s") != null)) continue;
            args.ndv_union = !raw;
            var taken: std.EnumSet(FastPath) = .{};
            args.integer_records = true;
            args.distinct_pairs = .on;
            args.parallelism = j;
            args.decode_batch_rows = window;
            args.taken = &taken;
            if (c.group != null and (std.mem.eql(u8, c.group.?, "k") or std.mem.eql(u8, c.group.?, "s"))) {
                args.distinct_pairs = .auto;
                const auto_path = (try explainAggregate(gpa, a, args)).record_path;
                if (std.mem.eql(u8, c.group.?, "k")) {
                    try testing.expectEqual(.pair_distinct, auto_path.?.layout);
                } else try testing.expect(auto_path == null);
                var automatic = try runMultiAggregate(gpa, a, args);
                defer automatic.deinit(gpa);
                try automatic.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, automatic.output.group_rows);
                try testing.expectEqualDeep(want.output.aggs, automatic.output.aggs);
                args.distinct_pairs = .on;
                taken = .{};
            }
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.pair_distinct, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try testing.expect(got.distinct_pairs);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
        };
    }
}

test "M1 finish DISTINCT poisoned NULL record bounds replay with and without count star" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const P = @import("owner_distinct.zig").Pipeline(u64);
    const cache_slots = @import("owner_cache.zig").capacity(@sizeOf(P.Groups.Record));
    // Cycle a domain well beyond the producer cache. NULL presence records cannot be bounded by its NDV:
    // eviction and the low-hit bypass can emit one record per input row, even for the same group.
    const groups = @max(4096, cache_slots * 8);
    // The writer requires at least 64 present values and at most 10% distinct integer keys for dictionaries.
    const rows = groups * 16 + 64;
    const k = try a.alloc(i32, rows);
    const x = try a.alloc(i32, rows);
    const defs = try a.alloc(u32, rows);
    for (k, 0..) |*key, i| key.* = @intCast(i % groups);
    @memset(x, 7);
    @memset(defs, 0);
    @memset(defs[rows - 64 ..], 1);
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = k } }, .parquet_type = .INT32, .nullable = false },
        .{
            .name = "x",
            .col = .{ .i32 = .{ .values = x, .def_levels = defs, .max_def = 1 } },
            .parquet_type = .INT32,
            .nullable = true,
        },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, @intCast(rows), .SNAPPY, .{}, .{});
    const poisoned = try rewritten(bytes.buffer.items, .{ .footer = struct {
        fn edit(meta: *schema.FileMetaData) void {
            for (meta.row_groups.items) |*rg| rg.columns.items[1].meta_data.?.statistics.?.null_count = 0;
        }
    }.edit });
    defer gpa.free(poisoned);
    const meta = try metadata.open(a, poisoned);
    try testing.expectEqual(@as(?i64, 0), meta.row_groups.items[0].columns.items[1].meta_data.?.statistics.?.null_count);
    for (meta.row_groups.items[0].columns.items) |col|
        try testing.expect(col.meta_data.?.dictionary_page_offset != null);
    for ([_][]const u8{ "count(DISTINCT x) AS d", "count(DISTINCT x) AS d,count(*) AS c" }) |aggregate| {
        var ledger: core_memory.SharedBudget = .{ .limit = 1 << 30 };
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "M1-poisoned-NULL", .bytes = poisoned }},
            .aggregate = aggregate,
            .group_by = "k",
            .parallelism = 1,
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .distinct_pairs = .off,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .plain_rules = false,
            .trust_stats = false,
            // Ensure the underestimated stream reaches the physical-record check instead of exhausting
            // scatter capacity first; both refusals must take the same established replay.
            .byte_top_partitions = 16384,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try testing.expectEqual(groups, want.output.group_rows.?.len);
        for (want.output.group_rows.?, 0..) |row, i| {
            try testing.expectEqual(@as(i64, @intFromBool(i < 64)), row[1].i);
            if (row.len == 3) try testing.expectEqual(@as(i64, if (i < 64) 17 else 16), row[2].i);
        }
        args.integer_records = true;
        args.distinct_pairs = .on;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
        const ledger_before = ledger.used.load(.monotonic);
        const refusal: anyerror = if (prepareAggregate(gpa, a, args, false)) |result| blk: {
            var unexpected = result;
            unexpected.deinit(gpa);
            break :blk error.TestUnexpectedResult;
        } else |err| err;
        try testing.expectEqual(error.ExceededMemoryBudget, refusal);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(ledger_before, ledger.used.load(.monotonic));
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        // The canonical fallback may itself use foldDistinctPairs; its result flag does not identify the owner.
        // The armed refusal above and successful runMultiAggregate prove the whole-attempt retry.
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
    }
}

test "M1 finish partitions full and top-H states including preparation cleanup ties and failed starts" {
    const a = testing.allocator;
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "c", .result = .i64, .arg = null, .where = null },
        .{ .func = .min, .alias = "lo", .result = .bytes, .arg = null, .where = null },
    };
    for ([_]usize{ 1, 16 }) |workers| for ([_]enum { full, top, first }{ .full, .top, .first }) |mode| {
        const top = mode == .top;
        var tables: [3]expr_agg.GroupTable = undefined;
        for (&tables) |*t| t.* = .init(a, 256 << 20);
        defer {
            finishGroupRanges(&tables, null, null, workers, true);
            for (&tables) |*t| t.deinitStorage();
        }
        const n = 16391;
        for (&tables, 0..) |*t, ti| for (0..n) |i| {
            var key: [32]u8 = undefined;
            const bytes = try std.fmt.bufPrint(&key, "g-{d}", .{i});
            const id = try t.getOrInsert(bytes, &calls, i * 5 + 2 - ti);
            const at = @as(usize, id) * calls.len;
            t.accumulators.items[at] = .{ .count = i % 7 + 1 };
            try setMergeTestString(a, &t.accumulators.items[at + 1], if (ti == 1) "best" else "other");
        };
        const refs = try a.alloc(MergeGroupRef, n * tables.len);
        defer a.free(refs);
        finishGroupRanges(&tables, refs, null, workers, false);
        for (refs, 0..) |ref, i| {
            try testing.expectEqual(@as(u32, @intCast(i / n)), ref.table);
            try testing.expectEqual(@as(u32, @intCast(i % n)), ref.group);
        }
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(a);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer {
            freeAccumulatorRanges(a, accs.items, workers);
            accs.deinit(a);
        }
        const cut: ?ResultCut = if (top) .{ .a_idx = 0, .k = 7 } else null;
        if (workers == 16) spawn_util.injectFailureAfter(1);
        defer spawn_util.resetFailure();
        if (mode == .first) {
            const keys = (try firstCandidatesParallel(a, &tables, 7, workers)).?;
            defer a.free(keys);
            try testing.expectEqual(@as(usize, 7), keys.len);
            try mergeCandidates(a, a, &tables, keys, &calls, workers, &slots, &accs);
        } else try mergeFoldedRefs(a, a, &tables, refs, &calls, workers, cut, false, &slots, &accs);
        if (top) try keepCountTop(a, a, &calls, .{ .a_idx = 0, .k = 7 }, &slots, &accs);
        try sortSlotsByFirst(a, slots.items, workers);
        for ([_]bool{ false, true }) |typed| try testing.expectEqual(
            answerBytes(slots.items, &.{.{ .lane = .string, .nullable = true }}, &.{.string}, typed),
            answerBytesParallel(slots.items, &.{.{ .lane = .string, .nullable = true }}, &.{.string}, typed, workers),
        );
        try testing.expectEqual(@as(usize, if (mode != .full) 7 else n), slots.items.len);
        for (slots.items, 0..) |slot, i| {
            const id = if (top) 6 + i * 7 else i;
            try testing.expectEqual(@as(u64, id * 5), slot.first);
            try testing.expectEqual(@as(u64, 3 * (id % 7 + 1)), accs.items[slot.idx].count);
            try testing.expectEqualStrings("best", accs.items[slot.idx + 1].min_bytes.?);
        }
    };
}

test "R11 partition COUNT cut builds only limit plus offset when no partition reaches the cut" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var keys: [4096][]const u8 = undefined;
    var values: [4096][]const u8 = undefined;
    for (&keys, &values, 0..) |*k, *v, i| {
        k.* = try std.fmt.allocPrint(a, "source-with-a-long-shared-prefix-and-independent-group-{d}", .{i % 128});
        v.* = try std.fmt.allocPrint(a, "destination-with-another-long-shared-prefix-{d}", .{i % 8});
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .string = .{ .values = &keys } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "v", .col = .{ .string = .{ .values = &values } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
    };
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, keys.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "partition-cut", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k,v",
        .max_memory = 256 << 20,
        .min_bytes_per_worker = 1,
        .integer_records = false,
        .byte_records = false,
        .msd_records = false,
        .dense = false,
        .plain_rules = false,
        .private_runs = false,
        .ndv_union = false,
        .partition_cost = false,
        .spill = false,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try testing.expectEqual(@as(usize, 128), want.output.group_rows.?.len);
    const order: expr_ast.ResultOrder = .{
        .keys = &.{.{ .column = 2, .descending = true }},
        .offset = 12,
        .limit = 5,
    };
    try want.output.applyResultOrder(gpa, order);
    args.result_order = order;
    args.spill = true;
    args.spill_groups = 64;
    args.spill_rows = 128;
    var image: std.ArrayList(u8) = .empty;
    const key_cols = [_]@import("filter/eval.zig").Batch.Column{
        .{ .string = .{ .values = &keys } },
        .{ .string = .{ .values = &values } },
    };
    var in_partition: [32]usize = @splat(0);
    for (0..128) |row| {
        try expr_agg.serializeRowKey(&image, a, &key_cols, row);
        in_partition[std.hash.Wyhash.hash(0, image.items) >> 59] += 1;
    }
    for (in_partition) |n| try testing.expect(n < 17);
    for ([_]usize{ 1, 16 }) |workers| {
        args.parallelism = workers;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(got.part_agg);
        // Complete partitions together contain 128 groups, but each is below 17. The global cut must run before
        // building either string output, not merely when the engine later applies LIMIT/OFFSET.
        try testing.expectEqual(@as(usize, 17), got.output.group_rows.?.len);
        try got.output.applyResultOrder(gpa, order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
}

test "STRGROUP grouped strings survive local partition and typed output ownership" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 768;
    var keys: [n][]const u8 = undefined;
    var vals: [n][]const u8 = undefined;
    var kd: [n]u32 = undefined;
    var vd: [n]u32 = undefined;
    const long = try a.alloc(u8, 70001);
    @memset(long, 'q');
    for (&keys, &vals, &kd, &vd, 0..) |*k, *v, *dk, *dv, i| {
        k.* = switch (i % 193) {
            0 => "",
            1 => long,
            else => try std.fmt.allocPrint(a, "key-{d}", .{i % 193}),
        };
        v.* = try std.fmt.allocPrint(a, "value-{d:0>4}", .{n - i});
        dk.* = @intFromBool(i % 31 != 0);
        dv.* = @intFromBool(i % 17 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .string = .{ .values = &keys, .def_levels = &kd, .max_def = 1 } }, .parquet_type = .BYTE_ARRAY, .nullable = true },
        .{ .name = "v", .col = .{ .string = .{ .values = &vals, .def_levels = &vd, .max_def = 1 } }, .parquet_type = .BYTE_ARRAY, .nullable = true },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, n, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "STRGROUP", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c,min(k) AS lk,max(k) AS hk,min(v) AS lv,max(v) AS hv",
        .group_by = "k",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .scan_all = true,
        .integer_records = false,
        .byte_records = false,
        .msd_records = false,
        .dense = false,
        .part_cap = 8,
        .part_probe = 16,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    const rows = want.output.group_rows.?;
    try testing.expectEqual(@as(usize, 194), rows.len);
    for (rows) |row| {
        var count: i64 = 0;
        var lo: ?[]const u8 = null;
        var hi: ?[]const u8 = null;
        for (keys, vals, kd, vd) |k, v, dk, dv| {
            const same = if (row[0] == .null_val) dk == 0 else dk == 1 and std.mem.eql(u8, row[0].s, k);
            if (!same) continue;
            count += 16;
            if (dv == 0) continue;
            if (lo == null or std.mem.order(u8, v, lo.?) == .lt) lo = v;
            if (hi == null or std.mem.order(u8, v, hi.?) == .gt) hi = v;
        }
        try testing.expectEqual(count, row[1].i);
        try testing.expectEqualDeep(row[0], row[2]);
        try testing.expectEqualDeep(row[0], row[3]);
        if (lo) |v| try testing.expectEqualStrings(v, row[4].s) else try testing.expect(row[4] == .null_val);
        if (hi) |v| try testing.expectEqualStrings(v, row[5].s) else try testing.expect(row[5] == .null_val);
    }
    for ([_]bool{ false, true }) |parts| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| for ([_]bool{ false, true }) |typed| {
        args.partagg = parts;
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.typed_answer = typed;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(got.output.string_arenas.len > 0);
        if (!typed) {
            try testing.expectEqualDeep(rows, got.output.group_rows.?);
        } else {
            const out = got.output.typed.?;
            try testing.expectEqual(rows.len, out.rows);
            for (rows, 0..) |row, r| for (row, out.columns) |v, col| switch (col) {
                .i64 => |lane| try testing.expectEqual(v.i, lane.values[r]),
                .string => |lane| {
                    try testing.expectEqual(@as(u32, @intFromBool(v != .null_val)), lane.defs[r]);
                    if (v == .s) try testing.expectEqualStrings(v.s, lane.values[r]);
                },
                else => return error.TestUnexpectedResult,
            };
        }
    };
}

test "STRGROUP returned output detaches a query-local arena ledger exactly once" {
    var budget: core_memory.SharedBudget = .{ .limit = 1024 };
    _ = budget.drawExact(128).?;
    var output: AggOutput = .{ .arena_ledger = &budget, .arena_credit = 128 };
    output.detachArenaLedger();
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    _ = budget.drawExact(32).?;
    output.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 32), budget.used.load(.monotonic));
    budget.release(32);
}

test "grouped string MIN MAX winners refuse at the live ledger without leaking" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var keys: [128]i32 = undefined;
    var values: [128][]const u8 = undefined;
    var words: [8][]const u8 = undefined;
    for (&words, 0..) |*word, i| {
        const bytes = try a.alloc(u8, 65536);
        @memset(bytes, @as(u8, @intCast('a' + i)));
        word.* = bytes;
    }
    for (&keys, &values, 0..) |*key, *value, i| {
        key.* = @intCast(i % 16);
        value.* = words[7 - i / 16];
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &keys } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "s", .col = .{ .string = .{ .values = &values } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    for ([_]bool{ false, true }) |encoded| for ([_]usize{ 1, 4 }) |j| {
        var ledger: core_memory.SharedBudget = .{ .limit = 512 << 10 };
        var live: LivePeak = .{ .child = gpa };
        const args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "grouped-winner-ledger", .bytes = bytes.buffer.items }},
            .aggregate = "min(s) AS lo,max(s) AS hi,count(*) AS c",
            .group_by = "k",
            .scan_all = true,
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .decode_limit = ledger.limit / 2,
            .parallelism = j,
            .encoded = encoded,
            .min_bytes_per_worker = 1,
            .dense = false,
            .integer_records = false,
        };
        try testing.expectError(error.ExceededMemoryBudget, runMultiAggregate(live.allocator(), a, args));
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), live.live.load(.monotonic));
        // Old grouped winners were outside the ledger, and could grow past this bound before admission noticed.
        try testing.expect(live.peak.load(.monotonic) < 1 << 20);
        var roomy: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var wide = args;
        wide.ledger = &roomy;
        wide.max_memory = roomy.limit;
        wide.decode_limit = roomy.limit / 2;
        var got = try runMultiAggregate(gpa, a, wide);
        defer got.deinit(gpa);
        try testing.expectEqual(@as(usize, 16), got.output.group_rows.?.len);
        for (got.output.group_rows.?) |row| {
            try testing.expectEqualStrings(words[0], row[1].s);
            try testing.expectEqualStrings(words[7], row[2].s);
            try testing.expectEqual(@as(i128, 8), row[3].i);
        }
    };
}

test "final multipass region retirement releases every charge after partial spawn refusal" {
    const gpa = testing.allocator;
    for ([_]usize{ 0, 1, 2, 3, 4 }) |allowed| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var workers: [4]Worker = undefined;
        for (&workers) |*w| {
            var pair = huge_pages.takePair(gpa, WORKER_REGION_RESERVE);
            pair.arena.budget = &budget;
            pair.scratch.budget = &budget;
            const pages = try pair.arena.allocator().alloc(u8, 4 << 20);
            @memset(pages, 7);
            w.* = .{ .gpa = gpa, .inputs = &.{}, .metas = &.{}, .work = &.{}, .cursor = undefined, .agg_calls = &.{}, .plan = undefined, .decode_options = .{}, .region = pair.arena, .scratch_region = pair.scratch };
        }
        try testing.expect(budget.used.load(.monotonic) > 0);
        spawn_util.injectFailureAfter(allowed);
        defer spawn_util.resetFailure();
        releaseRegionsParallel(&workers);
        for (workers) |w| {
            try testing.expect(w.region == null and w.scratch_region == null);
        }
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}

test "packed payload top H preserves integer SUM AVG MIN MAX NULL ties and filtered windows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i32 = undefined;
    var text: [2048][]const u8 = undefined;
    var value: [2048]i64 = undefined;
    var defs: [2048]u32 = undefined;
    for (&k, &text, &value, &defs, 0..) |*ki, *t, *v, *d, i| {
        ki.* = @intCast(i % 17);
        t.* = if (i % 2 == 0) "first" else "second";
        v.* = @as(i64, @intCast(i % 19)) - 9;
        d.* = @intFromBool(i % 17 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &k } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "t", .col = .{ .string = .{ .values = &text } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "v", .col = .{ .i64 = .{ .values = &value, .def_levels = &defs, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
    };
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    for ([_]?[]const u8{ null, "k < 12" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "packed-payload", .bytes = sink.buffer.items }},
            .metas = &.{meta},
            .aggregate = "count(*) AS c,sum(v) AS s,avg(v) AS a,min(v) AS lo,max(v) AS hi",
            .group_by = "k,t",
            .filter = filter,
            .max_memory = 256 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .dense = true,
            .plain_rules = false,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 9, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            args.parallelism = j;
            args.decode_batch_rows = window;
            args.integer_records = true;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const ex = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.packed_count_top_h, ex.record_path.?.layout);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
        };
    }
}

test "raw DISTINCT batch owners preserve NULLs and replay speculative footer range violations" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i64 = undefined;
    var x: [2048]i64 = undefined;
    var kd: [2048]u32 = undefined;
    var xd: [2048]u32 = undefined;
    for (&k, &x, &kd, &xd, 0..) |*key, *value, *dk, *dx, i| {
        key.* = (1 << 40) + @as(i64, @intCast(i % 33));
        value.* = @intCast(i % 95);
        dk.* = @intFromBool(i % 17 != 0);
        dx.* = @intFromBool(i % 11 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i64 = .{ .values = &k, .def_levels = &kd, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x, .def_levels = &xd, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
    };
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    for ([_][]const u8{ "count(DISTINCT x) AS d", "count(DISTINCT x) AS d,count(*) AS c" }) |calls| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "raw-distinct", .bytes = sink.buffer.items }},
            .metas = &.{meta},
            .aggregate = calls,
            .group_by = "k",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .max_memory = 512 << 20,
            .integer_records = false,
            .integer_records_min_rows = 0,
            .distinct_pairs = .off,
            .ndv_union = false,
            .trust_stats = false,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]bool{ false, true }) |poison| {
            var footer = meta;
            const groups = try a.dupe(schema.RowGroup, meta.row_groups.items);
            footer.row_groups.items = groups;
            var zero: [8]u8 = @splat(0);
            if (poison) for (groups) |*rg| {
                rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
                var st = &rg.columns.items[0].meta_data.?.statistics.?;
                st.min_value = &zero;
                st.max_value = &zero;
                st.min = &zero;
                st.max = &zero;
            };
            args.metas = &.{footer};
            args.integer_records = true;
            args.distinct_pairs = .auto;
            for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
                args.parallelism = j;
                args.decode_batch_rows = window;
                var taken: std.EnumSet(FastPath) = .{};
                args.taken = &taken;
                const ex = try explainAggregate(gpa, a, args);
                try testing.expectEqual(.pair_distinct, ex.record_path.?.layout);
                if (poison) {
                    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
                    try testing.expect(taken.contains(.integer_records));
                }
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            };
        }
    }
}

test "scalar long string DISTINCT keeps NULL empty and windowed answers across the cached partition fanout" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var text: [2048][]const u8 = undefined;
    var defs: [2048]u32 = undefined;
    const long = try a.alloc(u8, 70001);
    @memset(long, 'x');
    for (&text, &defs, 0..) |*t, *d, i| {
        t.* = switch (i % 33) {
            0 => "",
            1 => long,
            else => try std.fmt.allocPrint(a, "key-{d}", .{i % 33}),
        };
        d.* = @intFromBool(i % 17 != 0);
    }
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .string = .{ .values = &text, .def_levels = &defs, .max_def = 1 } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = true,
    }};
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, text.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    for ([_]bool{ false, true }) |encoded| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        const args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "long-distinct", .bytes = sink.buffer.items }},
            .metas = &.{meta},
            .aggregate = "count(DISTINCT k) AS d",
            .max_memory = 256 << 20,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .encoded = encoded,
            .decode_batch_rows = window,
        };
        try testing.expect((try explainAggregate(gpa, a, args)).record_path == null);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expectEqual(@as(i128, 33), got.output.aggs[0].value.i);
    };
}

test "shared string scan preserves full grouped and scalar answers NULLs original order and both readers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var text: [512][]const u8 = undefined;
    var key_defs: [512]u32 = undefined;
    var values: [512]i64 = undefined;
    var defs: [512]u32 = undefined;
    const big = try a.alloc(u8, 70001);
    @memset(big, 'x');
    for (&text, &key_defs, &values, &defs, 0..) |*key, *kd, *value, *d, i| {
        key.* = switch (i % 128) {
            0 => "",
            1 => big,
            2 => "a\x00b",
            else => try std.fmt.allocPrint(a, "a wide plain key for the shared string table {d}", .{i % 128}),
        };
        kd.* = @intFromBool(i % 19 != 0);
        value.* = @as(i64, @intCast(i % 13)) - 6;
        d.* = @intFromBool(i % 7 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &text, .def_levels = &key_defs, .max_def = 1 } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } } },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &columns, text.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    for ([_]bool{ false, true }) |scalar| for ([_]?[]const u8{ null, "v >= -4", "k <> ''" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "shared-string", .bytes = bytes.buffer.items }},
            .metas = &.{meta},
            .aggregate = if (scalar) "count(DISTINCT k) AS d" else "count(*) AS c,sum(v) AS s,avg(v) AS av,min(v) AS lo,max(v) AS hi,count(v) AS n",
            .group_by = if (scalar) null else "k",
            .filter = filter,
            .max_memory = 256 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .shared_strings = false,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]bool{ false, true }) |encoded| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            args.shared_strings = true;
            args.encoded = encoded;
            args.parallelism = j;
            args.decode_batch_rows = window;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const ex = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.shared_string_table, ex.record_path.?.layout);
            try testing.expectEqualStrings("shared_string_table", ex.strategyName());
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.shared_strings));
            if (scalar) {
                try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            } else {
                try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
            }
        };
    };
    // A setup decline leaves the same occupied ledger, even when the canonical decoder also cannot fit.
    var ledger: core_memory.SharedBudget = .{ .limit = 16 << 20 };
    const held = ledger.drawExact(ledger.limit - 1).?;
    defer ledger.release(held);
    var taken: std.EnumSet(FastPath) = .{};
    const tight: MultiAggArgs = .{
        .inputs = &.{.{ .name = "shared-string", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT k) AS d",
        .max_memory = ledger.limit,
        .ledger = &ledger,
        .parallelism = 1,
        .taken = &taken,
    };
    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, tight, false));
    try testing.expect(!taken.contains(.shared_strings));
    try testing.expect((try explainAggregate(gpa, a, tight)).record_path == null);
    try testing.expectEqual(held, ledger.used.load(.monotonic));
}

test "raw payload owner multisets preserve NULLs AVG ties readers and poisoned-range replay" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i32 = undefined;
    var q: [2048]i32 = undefined;
    var v: [2048]i64 = undefined;
    var w: [2048]i64 = undefined;
    var kd: [2048]u32 = undefined;
    var qd: [2048]u32 = undefined;
    var vd: [2048]u32 = undefined;
    var wd: [2048]u32 = undefined;
    for (0..k.len) |i| {
        k[i] = @intCast(i % 17);
        q[i] = @intCast(i % 9);
        v[i] = @as(i64, @intCast(i % 19)) - 9;
        w[i] = @as(i64, @intCast(i % 23)) * 7 - 40;
        kd[i] = @intFromBool(i % 13 != 0);
        qd[i] = @intFromBool(i % 17 != 0);
        vd[i] = @intFromBool(i % 19 != 0);
        wd[i] = @intFromBool(i % 23 != 0);
    }
    // The final selected row exceeds the poisoned proposal, after earlier sliced callbacks contributed.
    k[k.len - 1] = 1 << 20;
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT32, .nullable = true, .col = .{ .i32 = .{ .values = &k, .def_levels = &kd, .max_def = 1 } } },
        .{ .name = "q", .parquet_type = .INT32, .nullable = true, .col = .{ .i32 = .{ .values = &q, .def_levels = &qd, .max_def = 1 } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &v, .def_levels = &vd, .max_def = 1 } } },
        .{ .name = "w", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &w, .def_levels = &wd, .max_def = 1 } } },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &columns, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var poisoned = meta;
    poisoned.row_groups.items = try a.dupe(schema.RowGroup, groups);
    var zero: [4]u8 = @splat(0);
    var sixteen: [4]u8 = undefined;
    std.mem.writeInt(i32, &sixteen, 16, .little);
    for (poisoned.row_groups.items) |*rg| {
        rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
        var st = &rg.columns.items[0].meta_data.?.statistics.?;
        st.min_value = &zero;
        st.min = &zero;
        st.max_value = &sixteen;
        st.max = &sixteen;
    }
    for ([_][]const u8{
        "count(*) AS c,sum(v) AS sv,avg(w) AS aw",
        "count(*) AS c,sum(v) AS sv,avg(w) AS aw,count(v) AS nv",
    }) |aggregate| for ([_]?[]const u8{ null, "v >= -4" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "raw-payload-multisets", .bytes = bytes.buffer.items }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "k,q",
            .filter = filter,
            .max_memory = 256 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .ndv_union = false,
            .dense = false,
            .plain_rules = false,
            .trust_stats = false,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 9, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]bool{ false, true }) |poison| for ([_]bool{ false, true }) |encoded|
            for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
                args.metas = if (poison) &.{poisoned} else &.{meta};
                args.integer_records = true;
                args.encoded = encoded;
                args.parallelism = j;
                args.decode_batch_rows = window;
                var taken: std.EnumSet(FastPath) = .{};
                args.taken = &taken;
                const ex = try explainAggregate(gpa, a, args);
                try testing.expectEqual(.packed_count_top_h, ex.record_path.?.layout);
                if (poison) {
                    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
                    try testing.expect(taken.contains(.integer_records));
                }
                var got = if (poison) try runMultiAggregate(gpa, a, args) else try prepareAggregate(gpa, a, args, false);
                defer got.deinit(gpa);
                if (!poison) try testing.expect(taken.contains(.integer_records));
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
            };
        // Scan-all retains full physical widths: a 66-bit raw tuple cannot enter the u64 payload owner.
        args.scan_all = true;
        args.taken = null;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.record_path == null);
    };
}

test "distinct admission prices both wide pair reductions against covered local sets" {
    const hw = @import("hw.zig");
    const saved_cpu = hw.test_cpu;
    defer hw.test_cpu = saved_cpu;
    hw.test_cpu = .{ .l2 = .{ .bytes = 128 } };
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [4096]i64 = undefined;
    var x: [4096]i64 = undefined;
    for (&k, &x, 0..) |*key, *value, i| {
        key.* = (1 << 40) + @as(i64, @intCast(i / 64));
        value.* = @intCast(i % 127);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i64 = .{ .values = &k } }, .parquet_type = .INT64, .nullable = false },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "pair-cost", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k",
        .max_memory = 256 << 20,
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .integer_records_min_rows = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const ex = try explainAggregate(gpa, a, args);
    try testing.expect(ex.record_path == null or ex.record_path.?.layout != .pair_distinct);
    // A small local group table must retain the cache's opportunity to coalesce pairs.
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20 } };
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    hw.test_cpu = .{ .l2 = .{ .bytes = 128 } };
    // A budget retry disables plain cost rules and must recover the available exact owner.
    args.plain_rules = false;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    args.plain_rules = true;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.distinct_pairs = .on;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "distinct admission refuses declared dictionary fallback before retaining a composite identity" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i32 = undefined;
    var x: [2048]i64 = undefined;
    var names: [2048][]const u8 = undefined;
    for (&k, &x, &names, 0..) |*key, *value, *name, i| {
        key.* = @intCast(i % 7);
        value.* = @intCast(i % 61);
        name.* = if (i % 3 == 0) "" else "phone";
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &k } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "s", .col = .{ .string = .{ .values = &names } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "declared-fallback", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k,s",
        .filter = "s != ''",
        .max_memory = 64 << 20,
        .parallelism = 1,
        .integer_records_min_rows = 0,
    };
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
    const stats = try a.dupe(schema.PageEncodingStats, &.{
        .{ .page_type = @backingInt(schema.PageType.DICTIONARY_PAGE), .encoding = @backingInt(schema.Encoding.PLAIN), .count = 1 },
        .{ .page_type = @backingInt(schema.PageType.DATA_PAGE), .encoding = @backingInt(schema.Encoding.RLE_DICTIONARY), .count = 1 },
        .{ .page_type = @backingInt(schema.PageType.DATA_PAGE), .encoding = @backingInt(schema.Encoding.PLAIN), .count = 1 },
    });
    meta.row_groups.items[0].columns.items[2].meta_data.?.encoding_stats = std.ArrayListUnmanaged(schema.PageEncodingStats).fromOwnedSlice(stats);
    const ex = try explainAggregate(gpa, a, args);
    try testing.expect(ex.record_path == null or ex.record_path.?.layout != .pair_distinct);
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    args.distinct_pairs = .off;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    args.distinct_pairs = .on;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
}

test "distinct admission compares wide pair work with the admitted integer DISTINCT top cut" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i64 = undefined;
    var x: [2048]i64 = undefined;
    for (&k, &x, 0..) |*key, *value, i| {
        key.* = @bitCast(std.hash.Wyhash.hash(0, std.mem.asBytes(&i)));
        value.* = @intCast(i % 127);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i64 = .{ .values = &k } }, .parquet_type = .INT64, .nullable = false },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "integer-distinct-cost", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k",
        .integer_records = false,
        .integer_records_min_rows = 1,
        .max_memory = 256 << 20,
        .min_bytes_per_worker = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7, .offset = 2 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.integer_records = true;
    for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        args.parallelism = j;
        args.decode_batch_rows = window;
        const ex = try explainAggregate(gpa, a, args);
        try testing.expectEqual(.integer_distinct_top_h, ex.record_path.?.layout);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    };
    // A memory retry disables the cost rule while retaining the same checked raw identity. Forced .on instead
    // uses unrestricted physical widths, which need not fit this 65+65-bit pair.
    args.plain_rules = false;
    try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
}

test "distinct admission scalar fanout prices sparse numeric ranges without changing identities" {
    const gpa = testing.allocator;
    const hw = @import("hw.zig");
    const saved = hw.test_cpu;
    defer hw.test_cpu = saved;
    hw.test_cpu = .{ .l2 = .{ .bytes = 256 << 10 } };
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sparse: [2048]i32 = undefined;
    var compact: [2048]i64 = undefined;
    for (&sparse, &compact, 0..) |*s, *c, i| {
        s.* = @intCast((i % 1000) * 1000000);
        c.* = @intCast(i % 127);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "s", .col = .{ .i32 = .{ .values = &sparse } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "c", .col = .{ .i64 = .{ .values = &compact } }, .parquet_type = .INT64, .nullable = false },
    };
    var bytes: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, sparse.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 4);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 4;
    const work = [_]WorkItem{.{ .file = 0, .rg = 0, .agg_start = 0, .agg_len = 0, .columns = .{ .read = &.{}, .consumed = &.{}, .preserve = &.{} }, .accumulators = &.{} }};
    const s: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const c: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    try testing.expect(scalarDistinctWideParts(s, &.{meta}, &work));
    try testing.expect(!scalarDistinctWideParts(c, &.{meta}, &work));
    // With enough cache even the wide image may use the bitmap-friendly topology.
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 30 } };
    try testing.expect(!scalarDistinctWideParts(s, &.{meta}, &work));
    for ([_][]const u8{ "s", "c" }) |column| {
        const args: MultiAggArgs = .{ .inputs = &.{.{ .name = "scalar-fanout", .bytes = bytes.buffer.items }}, .metas = &.{meta}, .aggregate = try std.fmt.allocPrint(a, "count(DISTINCT {s}) AS d", .{column}), .parallelism = 4, .min_bytes_per_worker = 1, .integer_records = false, .max_memory = 64 << 20 };
        hw.test_cpu = .{ .l2 = .{ .bytes = 256 << 10 } };
        var wide = try runMultiAggregate(gpa, a, args);
        defer wide.deinit(gpa);
        hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 30 } };
        var narrow = try runMultiAggregate(gpa, a, args);
        defer narrow.deinit(gpa);
        try testing.expectEqualDeep(wide.output.aggs, narrow.output.aggs);
    }
}

const RankedFixture = struct { bytes: []const u8, meta: schema.FileMetaData };

fn rankedFixture(a: std.mem.Allocator) !RankedFixture {
    return rankedFixtureColumns(a, .{ "k", "x", "y" }, false);
}

fn rankedFixtureColumns(a: std.mem.Allocator, names: [3][]const u8, even_y: bool) !RankedFixture {
    const k = try a.alloc(i32, 2048);
    const x = try a.alloc(i64, k.len);
    const y = try a.alloc(i32, k.len);
    const kd = try a.alloc(u32, k.len);
    const xd = try a.alloc(u32, k.len);
    for (k, x, y, kd, xd, 0..) |*key, *value, *payload, *dk, *dx, i| {
        key.* = @as(i32, @intCast(i % 13)) - 6;
        value.* = @as(i64, @intCast(i % 67)) - 33;
        payload.* = if (even_y) @as(i32, @intCast(2 * (i % 2))) else @as(i32, @intCast(i % 37)) - 18;
        dk.* = @intFromBool(i % 13 != 0);
        dx.* = @intFromBool(i % 13 != 1 and i % 7 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = names[0], .col = .{ .i32 = .{ .values = k, .def_levels = kd, .max_def = 1 } }, .parquet_type = .INT32, .nullable = true },
        .{ .name = names[1], .col = .{ .i64 = .{ .values = x, .def_levels = xd, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
        .{ .name = names[2], .col = .{ .i32 = .{ .values = y } }, .parquet_type = .INT32, .nullable = false },
    };
    var sink: @import("writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, @intCast(k.len), .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 8);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    return .{ .bytes = sink.buffer.items, .meta = meta };
}

test "count-ranked DISTINCT keeps repeated aggregate aliases and bound quoted identifiers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |quoted| {
        const fixture = try rankedFixtureColumns(a, if (quoted) .{ "group key", "user x", "select" } else .{ "k", "x", "y" }, false);
        const sql = if (quoted)
            "SELECT \"group key\" AS g,sum(\"select\") AS a,sum(\"select\") AS b,count(*) AS c,count(DISTINCT \"user x\") AS d FROM 'ranked' GROUP BY \"group key\" ORDER BY c DESC LIMIT 5"
        else
            "SELECT k,sum(y) AS a,sum(y) AS b,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5";
        const q = try @import("expr/sql_parser.zig").parseSqlQuery(a, sql);
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked", .bytes = fixture.bytes }},
            .metas = &.{fixture.meta},
            .aggregate = q.aggregate_str.?,
            .group_by = q.group_by_str,
            .select_cols = q.select_cols,
            .result_order = q.result_order,
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .count_ranked_distinct = .off,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        args.count_ranked_distinct = .on;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(got.count_ranked_distinct);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
        try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
    }
}

test "count-ranked DISTINCT returns an empty filtered result without rebuilding a false predicate" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixtureColumns(a, .{ "k", "x", "y" }, true);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "ranked-empty", .bytes = fixture.bytes }},
        .metas = &.{fixture.meta},
        .aggregate = "sum(y) AS a,count(*) AS c,count(DISTINCT x) AS d",
        .group_by = "k",
        .filter = "y = 1",
        .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5 },
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .max_memory = 128 << 20,
        .count_ranked_distinct = .off,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    args.count_ranked_distinct = .on;
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expect(got.count_ranked_distinct);
    try testing.expectEqual(@as(usize, 0), got.output.group_rows.?.len);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
    try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
}

test "count-ranked DISTINCT keeps expression HAVING and alternate alias ordering on exact fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    const queries = [_][]const u8{
        "SELECT k,sum(CAST(y AS BIGINT)) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y+1) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y) AS s,count(*) AS c,count(DISTINCT x+1) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k HAVING s > 0 ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY s DESC LIMIT 5",
    };
    for (queries) |sql| {
        const q = try @import("expr/sql_parser.zig").parseSqlQuery(a, sql);
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked", .bytes = fixture.bytes }},
            .metas = &.{fixture.meta},
            .aggregate = q.aggregate_str.?,
            .group_by = q.group_by_str,
            .select_cols = q.select_cols,
            .result_order = q.result_order,
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .count_ranked_distinct = .off,
        };
        var want = runMultiAggregate(gpa, a, args) catch |e| {
            args.count_ranked_distinct = .on;
            try testing.expectError(e, runMultiAggregate(gpa, a, args));
            continue;
        };
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        args.count_ranked_distinct = .on;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(!got.count_ranked_distinct);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
        try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
    }
}

test "count-ranked DISTINCT preserves NULLs exact sums averages ties and forced controls" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    for ([_]?[]const u8{ null, "y >= 0" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked-fixture", .bytes = fixture.bytes }},
            .metas = &.{fixture.meta},
            .aggregate = "sum(y) AS s,count(*) AS c,avg(x) AS a,count(DISTINCT x) AS d",
            .group_by = "k",
            .filter = filter,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .count_ranked_distinct = .off,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5, .offset = 1 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]usize{ 1, 4 }) |j| for ([_]usize{ 0, 64 }) |window| {
            for ([_]@FieldType(MultiAggArgs, "count_ranked_distinct"){ .auto, .on, .off }) |mode| {
                args.count_ranked_distinct = mode;
                args.parallelism = j;
                args.decode_batch_rows = window;
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try testing.expectEqual(mode != .off, got.count_ranked_distinct);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
            }
        };
        args.count_ranked_distinct = .on;
        args.result_order.offset = 0;
        args.result_order.limit = 32;
        var whole = try runMultiAggregate(gpa, a, args);
        defer whole.deinit(gpa);
        try testing.expect(whole.count_ranked_distinct);
        var null_key = false;
        var empty_argument = false;
        for (whole.output.group_rows.?) |row| {
            null_key = null_key or row[0] == .null_val;
            if (row[0] == .i and row[0].i == -5) {
                try testing.expect(row[3] == .null_val);
                try testing.expectEqual(@as(i128, 0), row[4].i);
                empty_argument = true;
            }
        }
        try testing.expect(null_key and empty_argument);
    }
}

test "count-ranked DISTINCT replays a runtime dictionary-domain violation" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = @embedFile("testdata/count-ranked-fallback.parquet");
    var meta = try metadata.open(a, bytes);
    // The page headers still declare a real PLAIN fallback with new keys. Only the planning hint lies.
    const stats = try a.dupe(schema.PageEncodingStats, &.{
        .{ .page_type = @backingInt(schema.PageType.DICTIONARY_PAGE), .encoding = @backingInt(schema.Encoding.PLAIN), .count = 1 },
        .{ .page_type = @backingInt(schema.PageType.DATA_PAGE), .encoding = @backingInt(schema.Encoding.RLE_DICTIONARY), .count = 1 },
    });
    meta.row_groups.items[0].columns.items[0].meta_data.?.encoding_stats = std.ArrayListUnmanaged(schema.PageEncodingStats).fromOwnedSlice(stats);
    var args: MultiAggArgs = .{ .inputs = &.{.{ .name = "runtime-domain-refusal", .bytes = bytes }}, .metas = &.{meta}, .aggregate = "sum(y) AS s,count(*) AS c,avg(x) AS a,count(DISTINCT x) AS d", .group_by = "k", .parallelism = 1, .max_memory = 128 << 20, .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5 }, .count_ranked_distinct = .off };
    var ordinary = args;
    ordinary.aggregate = "sum(y),count(*),avg(x)";
    ordinary.plain_rules = false;
    ordinary.partition_cost = false;
    try testing.expect((try explainAggregate(gpa, a, ordinary)).dense);
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.count_ranked_distinct = .on;
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try testing.expect(!got.count_ranked_distinct);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "count-ranked DISTINCT refuses an unverified domain and keeps exact fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    var args: MultiAggArgs = .{ .inputs = &.{.{ .name = "domain-refusal", .bytes = fixture.bytes }}, .metas = &.{fixture.meta}, .aggregate = "count(DISTINCT x) AS d,avg(y) AS a,count(*) AS c,sum(x) AS s", .group_by = "k", .parallelism = 2, .min_bytes_per_worker = 1, .max_memory = 128 << 20, .result_order = .{ .keys = &.{.{ .column = 3, .descending = true }}, .limit = 4 }, .count_ranked_distinct = .off };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.count_ranked_distinct = .on;
    args.ndv_union = false;
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try testing.expect(!got.count_ranked_distinct);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "count-ranked DISTINCT memory refusal replays the exact fitting query" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    var refused = false;
    for ([_]usize{ 128 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20 }) |budget| {
        var args: MultiAggArgs = .{ .inputs = &.{.{ .name = "budget-refusal", .bytes = fixture.bytes }}, .metas = &.{fixture.meta}, .aggregate = "sum(y) AS s,count(*) AS c,avg(x) AS a,count(DISTINCT x) AS d", .group_by = "k", .parallelism = 1, .decode_batch_rows = 64, .max_memory = budget, .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5 }, .count_ranked_distinct = .off };
        var want = runMultiAggregate(gpa, a, args) catch |e| {
            if (e == error.ExceededMemoryBudget) continue;
            return e;
        };
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        args.count_ranked_distinct = .on;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        refused = refused or !got.count_ranked_distinct;
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
    try testing.expect(refused);
}
