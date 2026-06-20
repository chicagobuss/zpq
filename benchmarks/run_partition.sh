#!/usr/bin/env bash
# Head-to-head hive-partition pruning across 10 files.
# Each engine uses the input shape it prefers (explicit list vs glob),
# but the workload is identical: 10 files, filter `month >= 08`,
# 3 surviving files written.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

REGION="${AWS_REGION:-us-west-2}"
BUCKET="${AWS_S3_BUCKET}"
RUNS="${RUNS:-5}"

# Explicit list (ZPQ + Polars).
INPUTS=()
for mm in 01 02 03 04 05 06 07 08 09 10; do
  INPUTS+=("s3://${BUCKET}/zpq_test_data/partitioned/year=2026/month=${mm}/data.parquet")
done
INPUTS_JSON=$(printf '"%s",' "${INPUTS[@]}" | sed 's/,$//')

# Glob (DuckDB — explicit list with hive_partitioning has a known
# DuckDB 1.4.3 bug that mangles URLs after constant folding).
GLOB="s3://${BUCKET}/zpq_test_data/partitioned/year=2026/month=*/data.parquet"

# Build one-shot payload, invoke, extract metrics.
invoke() {
  local fn="$1" label="$2" payload_file="$3"
  for i in $(seq 1 "$RUNS"); do
    aws lambda invoke --function-name "$fn" \
      --cli-binary-format raw-in-base64-out --payload "file://$payload_file" \
      --cli-read-timeout 180 --region "$REGION" /tmp/p.json >/dev/null 2>&1
    jq -r --arg label "$label" \
      '[$label, (.total_ms // .errorMessage // .error), (.files_total // .input_count // ""), (.files_pruned // ""), (.bytes_out // "")] | @tsv' \
      /tmp/p.json
  done
}

# Pre-build payloads to /tmp. Each scenario gets a single output URL,
# so all RUNS overwrite the same key — we validate the LAST one at the
# end of the bench. Don't trust wallclock numbers without verifying
# the file is actually parquet.
ts() { date +%s%N; }

ZPQ_PRUNE_OUT="s3://$BUCKET/bench/$(ts)/zpq_prune.parquet"
ZPQ_COPY_OUT="s3://$BUCKET/bench/$(ts)/zpq_copy.parquet"
POLARS_PRUNE_OUT="s3://$BUCKET/bench/$(ts)/polars_prune.parquet"
POLARS_COPY_OUT="s3://$BUCKET/bench/$(ts)/polars_copy.parquet"
DUCKDB_PRUNE_OUT="s3://$BUCKET/bench/$(ts)/duckdb_prune.parquet"
DUCKDB_COPY_OUT="s3://$BUCKET/bench/$(ts)/duckdb_copy.parquet"

cat > /tmp/zpq_prune.json <<EOF
{"inputs":[$INPUTS_JSON],"output_url":"$ZPQ_PRUNE_OUT","filter":"month >= 08"}
EOF

cat > /tmp/zpq_copy.json <<EOF
{"inputs":[$INPUTS_JSON],"output_url":"$ZPQ_COPY_OUT"}
EOF

cat > /tmp/polars_prune.json <<EOF
{"mode":"polars_multi","inputs":[$INPUTS_JSON],"output_url":"$POLARS_PRUNE_OUT","filter_sql":"month >= 8"}
EOF

cat > /tmp/polars_copy.json <<EOF
{"mode":"polars_multi","inputs":[$INPUTS_JSON],"output_url":"$POLARS_COPY_OUT"}
EOF

cat > /tmp/duckdb_prune.json <<EOF
{"mode":"duckdb_multi","inputs":["$GLOB"],"output_url":"$DUCKDB_PRUNE_OUT","filter_sql":"CAST(month AS INTEGER) >= 8"}
EOF

cat > /tmp/duckdb_copy.json <<EOF
{"mode":"duckdb_multi","inputs":["$GLOB"],"output_url":"$DUCKDB_COPY_OUT"}
EOF

# Warm both functions once.
echo "# warm-up..." >&2
aws lambda invoke --function-name zpq-filter-s3 --cli-binary-format raw-in-base64-out --payload file:///tmp/zpq_prune.json --cli-read-timeout 180 --region "$REGION" /tmp/p.json >/dev/null 2>&1
aws lambda invoke --function-name zpq-bench-python --cli-binary-format raw-in-base64-out --payload file:///tmp/polars_prune.json --cli-read-timeout 180 --region "$REGION" /tmp/p.json >/dev/null 2>&1

{
  printf "label\ttotal_ms\tfiles_total\tfiles_pruned\tbytes_out\n"
  echo "# zpq partition prune (3/10)" >&2
  invoke zpq-filter-s3 "zpq:partprune" /tmp/zpq_prune.json
  echo "# zpq no-filter copy-all (10/10)" >&2
  invoke zpq-filter-s3 "zpq:copyall" /tmp/zpq_copy.json
  echo "# polars partition prune (3/10)" >&2
  invoke zpq-bench-python "polars:partprune" /tmp/polars_prune.json
  echo "# polars no-filter copy-all (10/10)" >&2
  invoke zpq-bench-python "polars:copyall" /tmp/polars_copy.json
  echo "# duckdb partition prune (3/10, glob)" >&2
  invoke zpq-bench-python "duckdb:partprune" /tmp/duckdb_prune.json
  echo "# duckdb no-filter copy-all (10/10, glob)" >&2
  invoke zpq-bench-python "duckdb:copyall" /tmp/duckdb_copy.json
} | tee benchmarks/partition_results.tsv

echo
echo "===================================================================="
echo "Validating outputs (pyarrow + duckdb) — A2 floor: bytes_out is not"
echo "the same as valid parquet."
echo "===================================================================="
python3 benchmarks/validate_outputs.py --from-stdin <<EOF
zpq:partprune	$ZPQ_PRUNE_OUT
zpq:copyall	$ZPQ_COPY_OUT
polars:partprune	$POLARS_PRUNE_OUT
polars:copyall	$POLARS_COPY_OUT
duckdb:partprune	$DUCKDB_PRUNE_OUT
duckdb:copyall	$DUCKDB_COPY_OUT
EOF
