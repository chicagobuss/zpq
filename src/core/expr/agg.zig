//! Aggregate kernels — sum / count / min / max / avg with optional
//! per-aggregate `FILTER (WHERE pred)` clause.
//!
//! Architectural shape:
//!
//!   AggCall — one call site (e.g. `sum(cost) FILTER (WHERE foo='bar') AS total`)
//!   Accumulator — running state for one AggCall, updated per-RG
//!   updateOne(state, batch, sel, call) — per-RG update path
//!   finalize(state, call) — fold accumulator into a Batch.Column for emit
//!
//! Conditional aggs (the FILTER clause) compose with the outer query
//! `--filter`: the per-agg SelectionVector starts as a copy of the
//! query's surviving rows, then ANDs in the per-agg predicate.
//!
//! The output of an aggregate workload is a 1-row parquet file, one
//! column per agg; avg is its finalized mean. `avg_state` emits two
//! columns (`<alias>__sum`, `<alias>__count`) so downstream re-
//! aggregation across N files preserves the algebra: a tier-2
//! re-aggregate is `sum(<alias>__sum) / sum(<alias>__count)`. You
//! cannot average means; you can sum sums and counts and divide once
//! at the end.

const std = @import("std");
const count_certificate = @import("../count_certificate.zig");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("ast.zig");
const expr_eval = @import("eval.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const agg_coded = @import("agg_coded.zig");
const statistics = @import("../parquet/statistics.zig");
const core_bytes = @import("../bytes.zig");
const core_memory = @import("../memory.zig");
const keyEql = core_bytes.keyEql;
const KeyHashMap = core_bytes.KeyHashMap;
const KeyHashMapUnmanaged = core_bytes.KeyHashMapUnmanaged;

pub const Error = types.Error;

pub const types = @import("aggregate/types.zig");
pub const AggFunc = types.AggFunc;
pub const AggCall = types.AggCall;
pub const ResultShape = types.ResultShape;
pub const resultDec = types.resultDec;
pub const avgDec = types.avgDec;
pub const avgDecMean = types.avgDecMean;
pub const DecSum = types.DecSum;

pub const avgMean = types.avgMean;

pub const distinct = @import("aggregate/distinct.zig");
pub const NUM_PARTS = distinct.NUM_PARTS;
pub const NUMERIC_PARTS = distinct.NUMERIC_PARTS;
pub const INLINE_CAP = distinct.INLINE_CAP;
pub const InlineU64 = distinct.InlineU64;
pub const InlineI128 = distinct.InlineI128;
pub const InlineBytes = distinct.InlineBytes;
const partOf = distinct.partOf;
const DistinctBytes = distinct.DistinctBytes;

pub const DistinctSet = distinct.DistinctSet;
pub const DistinctAcc = distinct.DistinctAcc;
pub const accumulator_mod = @import("aggregate/accumulator.zig");
pub const Accumulator = accumulator_mod.Accumulator;

pub const mergePersistent = accumulator_mod.mergePersistent;

pub const AvgState = types.AvgState;
pub const ResolveError = types.ResolveError;
pub const resolveResult = types.resolveResult;

pub const canStatShortCircuit = scalar_mod.canStatShortCircuit;
pub const statsCoverageComplete = scalar_mod.statsCoverageComplete;
pub const updateOneFromStats = scalar_mod.updateOneFromStats;
const statPresentCount = scalar_mod.statPresentCount;

pub const scalar_mod = @import("aggregate/scalar.zig");
pub const updateOne = scalar_mod.updateOne;
pub const updateOneWithCharge = scalar_mod.updateOneWithCharge;
pub const updateSharedInteger = scalar_mod.updateSharedInteger;
pub const minTotal = scalar_mod.minTotal;
pub const maxTotal = scalar_mod.maxTotal;
pub const foldExtremeF = scalar_mod.foldExtremeF;
pub const prefix16 = scalar_mod.prefix16;
pub const intersectPresentMask = scalar_mod.intersectPresentMask;
pub const sumBlocksF64 = scalar_mod.sumBlocksF64;
pub const sumBlocksGatheredF64 = scalar_mod.sumBlocksGatheredF64;
pub const sumBlocksCodedF64 = scalar_mod.sumBlocksCodedF64;
pub const sumConstantF64 = scalar_mod.sumConstantF64;
const PreparedAggInput = scalar_mod.PreparedAggInput;
const foldMinMaxBytes = scalar_mod.foldMinMaxBytes;
const simdSumI64 = scalar_mod.simdSumI64;
const simdMinMaxF64 = scalar_mod.simdMinMaxF64;
const exactAt = scalar_mod.exactAt;

pub const OutputCol = types.OutputCol;
pub const finalize = scalar_mod.finalize;
pub const u64Lane = scalar_mod.u64Lane;

pub const avgSum = types.avgSum;

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

const testI64Meta = scalar_mod.testI64Meta;

pub const keys_mod = @import("aggregate/keys.zig");
pub const isRowNull = keys_mod.isRowNull;
pub const serializeRowKey = keys_mod.serializeRowKey;
pub const fixedGroupStride = keys_mod.fixedGroupStride;
pub const packFixedKey = keys_mod.packFixedKey;
pub const canonicalKey = keys_mod.canonicalKey;
pub const fixedImage = keys_mod.fixedImage;
const expandFixed = keys_mod.expandFixed;
const hashFixed = keys_mod.hashFixed;

pub const GroupKeyResolver = keys_mod.GroupKeyResolver;

pub const SharedBudget = core_memory.SharedBudget;

pub const census_mod = @import("aggregate/census.zig");
pub const CountSketch = census_mod.CountSketch;
pub const Census = census_mod.Census;
pub const FingerprintSet = census_mod.FingerprintSet;
pub const fullestCells = census_mod.fullestCells;
pub const cellsSeparate = census_mod.cellsSeparate;
pub const sketchHash = census_mod.sketchHash;
const splitMix64 = census_mod.splitMix64;
const rawSketchHash = census_mod.rawSketchHash;
const wholeRawHash = census_mod.wholeRawHash;

pub const sinkSketchRows = group_state.sinkSketchRows;

pub const group_state = @import("aggregate/group_table.zig");
pub const KeyArena = keys_mod.KeyArena;
pub const GroupTable = group_state.GroupTable;

pub const grouped_mod = @import("aggregate/grouped.zig");
pub const firstPosition = grouped_mod.firstPosition;
pub const updateOneGrouped = grouped_mod.updateOneGrouped;
pub const updateTableGrouped = grouped_mod.updateTableGrouped;
pub const updateTableGroupedWithContext = grouped_mod.updateTableGroupedWithContext;
pub const FoldContext = grouped_mod.FoldContext;
pub const updateOneGroupedValues = grouped_mod.updateOneGroupedValues;
pub const updateOneGroupedValuesWithContext = grouped_mod.updateOneGroupedValuesWithContext;
pub const GroupedPreps = grouped_mod.GroupedPreps;
pub const prepareGrouped = grouped_mod.prepareGrouped;
pub const DistinctPairs = grouped_mod.DistinctPairs;
const foldMinMaxBytesGroupedOne = grouped_mod.foldMinMaxBytesGroupedOne;

pub const WinnerCharge = accumulator_mod.WinnerCharge;

pub const winnerBytes = accumulator_mod.winnerBytes;
pub const persistWinner = accumulator_mod.persistWinner;


