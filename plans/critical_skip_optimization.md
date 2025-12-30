# Critical Re-Evaluation: Is Skip Optimization Really The Bottleneck?

## The Real Question

Looking at the evidence more carefully:

**Skip Performance:**
- Dictionary column: 1.17x speedup (0.0908ms decode vs 0.0778ms skip)
- This is actually **reasonable** for dictionary columns! Decode is already fast (just indices)

**Filtered Scan Performance:**
- Full scan: 17.47 MVal/s (60k values)
- Filtered scan: 3.27 MVal/s (10k values) 
- **Filtered scan is 5.3x SLOWER per-value** despite processing 6x fewer values!

## The Real Bottlenecks

1. **Interleaved Loop Overhead**: Creating readers on heap, tagged unions, dynamic dispatch
2. **Filter Column Decode**: We're still fully decoding the filter column (can't skip it!)
3. **Skip Inefficiency**: Only matters for nullable columns, and only 1.17x slower

## Critical Insight: **Skip Optimization Alone Won't Fix This**

The filtered scan is slow because:
- We're decoding the filter column fully (unavoidable - we need the values!)
- The interleaved loop has overhead (heap allocations, tagged unions)
- Skip is only called for OTHER columns, which are already being skipped

**Skip optimization will help, but it's not the main bottleneck.**

## What Actually Matters

For a **selective filter** (1% match rate):
- Filter column: Must decode 100% (unavoidable)
- Other columns: Skip 99% of batches entirely
- **Skip performance matters most for the "skip entire batch" case**

For the "skip entire batch" case:
- Current skip() decodes def levels just to count → inefficient
- **This IS worth optimizing!** But it's a specific case, not the general skip path

## Revised Recommendation: **HYBRID APPROACH**

### Phase 1: Quick Win - Bulk Skip Path (1-2 hours)
Add a fast-path for "skip entire batch" that:
- Skips def levels using `RleDecoder.skip()` directly (no decode)
- Uses null stats to estimate data skip count (or skip def levels + data in parallel)
- **This will help selective filters immediately**

### Phase 2: Benchmark (30 minutes)
- Measure filtered scan vs full scan on large files
- Verify bulk skip path is actually faster
- Identify remaining bottlenecks

### Phase 3: Optimize Based on Data
- If skip is still slow → optimize general skip path
- If interleaved loop is slow → optimize reader management
- If filter decode is slow → that's unavoidable, but we can SIMD-ize comparisons

## The Challenge: **Are We Prematurely Optimizing?**

**Arguments FOR optimizing skip now:**
- It's a clear inefficiency (decoding to count)
- Bulk skip path is a common case (selective filters)
- Low risk, high reward

**Arguments AGAINST:**
- Skip is only 1.17x slower on dictionary columns (reasonable!)
- The real bottleneck might be elsewhere (interleaved loop overhead)
- We don't have data on PLAIN columns or nullable columns yet

## My Revised Vote: **QUICK WIN FIRST, THEN BENCHMARK**

1. **Add bulk skip fast-path** (1-2 hours) - addresses the common case
2. **Benchmark comprehensively** (30 min) - get real data
3. **Optimize based on data** - don't guess!

The bulk skip path is low-hanging fruit that will definitely help selective filters. Then we benchmark to see what else matters.

**But I'm challenging you: Is skip() really the problem, or is it the interleaved loop overhead?**
