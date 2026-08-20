#!/usr/bin/env bash
# Selectivity sweep on the encoder path (B4 victory lap).
#
# Same 10-file partitioned fixture as run_partition.sh, but with
# filters on a NON-partition column (`int8`). These force the
# encoder path (decode → eval → re-encode) on every engine —
# directly comparing CPU work + I/O streaming, not just the
# stat-pruning win. Selectivities span ~0.4% to ~99% surviving
# rows.
#
# Distribution of int8: roughly uniform over [-128, 127] across all
# 524288 rows. Predicates chosen to hit ~known fractions.

set -euo pipefail
cd "$(dirname "$0")/.."

ENV_FILE="${ENV_FILE:-.env}"
[[ -r "$ENV_FILE" ]] || { echo "missing readable ENV_FILE=$ENV_FILE" >&2; exit 2; }
set -a; source "$ENV_FILE"; set +a

REGION="${AWS_REGION:-us-west-2}"
BUCKET="${AWS_S3_BUCKET}"
RUNS="${RUNS:-3}"
ZPQ_FUNCTION="${ZPQ_BENCH_FUNCTION:-${LAMBDA_FUNCTION_NAME:-zpq-filter-s3}}"
PYTHON_FUNCTION="${PYTHON_BENCH_FUNCTION:-zpq-bench-python}"
RESULTS_OUT="${RESULTS_OUT:-benchmarks/selectivity_results.tsv}"
VALIDATION_OUT="${VALIDATION_OUT:-benchmarks/selectivity_validation.tsv}"

INPUTS=()
for mm in 01 02 03 04 05 06 07 08 09 10; do
  INPUTS+=("s3://${BUCKET}/zpq_test_data/partitioned/year=2026/month=${mm}/data.parquet")
done
INPUTS_JSON=$(printf '"%s",' "${INPUTS[@]}" | sed 's/,$//')
GLOB="s3://${BUCKET}/zpq_test_data/partitioned/year=2026/month=*/data.parquet"

ts() { date +%s%N; }

# (label, predicate, expected_pct_surviving)
declare -a SCENARIOS=(
  "narrow|int8 = 42|0.4%"
  "selective|int8 >= 100|11%"
  "balanced|int8 >= 0|50%"
  "broad|int8 >= -100|89%"
)

invoke() {
  local fn="$1" label="$2" payload_file="$3"
  for i in $(seq 1 "$RUNS"); do
    aws lambda invoke --function-name "$fn" \
      --cli-binary-format raw-in-base64-out --payload "file://$payload_file" \
      --cli-read-timeout 300 --region "$REGION" /tmp/p.json >/dev/null 2>&1
    jq -r --arg label "$label" \
      '[$label, (.total_ms // .errorMessage // .error), (.bytes_out // ""), (.rows_kept // "")] | @tsv' \
      /tmp/p.json
  done
}

# Warm-up
echo "# warm-up..." >&2
cat > /tmp/warm.json <<EOF
{"inputs":[$INPUTS_JSON],"output_url":"s3://${BUCKET}/bench/$(ts)/warm.parquet","filter":"int8 >= 0"}
EOF
aws lambda invoke --function-name "$ZPQ_FUNCTION" \
  --cli-binary-format raw-in-base64-out --payload file:///tmp/warm.json \
  --cli-read-timeout 300 --region "$REGION" /tmp/p.json >/dev/null 2>&1
cat > /tmp/warm_polars.json <<EOF
{"mode":"polars_multi","inputs":[$INPUTS_JSON],
 "output_url":"s3://${BUCKET}/bench/$(ts)/warm.parquet","filter_sql":"int8 >= 0"}
EOF
aws lambda invoke --function-name "$PYTHON_FUNCTION" \
  --cli-binary-format raw-in-base64-out --payload file:///tmp/warm_polars.json \
  --cli-read-timeout 300 --region "$REGION" /tmp/p.json >/dev/null 2>&1

# Output validation map: collect all output URLs to verify at end.
declare -A OUT_URLS=()

run_scenario() {
  local label="$1" predicate="$2" pct="$3"

  local zpq_out="s3://$BUCKET/bench/$(ts)/zpq_${label}.parquet"
  local polars_out="s3://$BUCKET/bench/$(ts)/polars_${label}.parquet"
  local duckdb_out="s3://$BUCKET/bench/$(ts)/duckdb_${label}.parquet"

  cat > /tmp/zpq_${label}.json <<EOF
{"inputs":[$INPUTS_JSON],"output_url":"$zpq_out","filter":"$predicate"}
EOF
  cat > /tmp/polars_${label}.json <<EOF
{"mode":"polars_multi","inputs":[$INPUTS_JSON],"output_url":"$polars_out","filter_sql":"$predicate"}
EOF
  cat > /tmp/duckdb_${label}.json <<EOF
{"mode":"duckdb_multi","inputs":["$GLOB"],"output_url":"$duckdb_out","filter_sql":"$predicate"}
EOF

  echo "# $label ($predicate, expect ~$pct surviving)" >&2
  invoke "$ZPQ_FUNCTION" "zpq:${label}" /tmp/zpq_${label}.json
  invoke "$PYTHON_FUNCTION" "polars:${label}" /tmp/polars_${label}.json
  invoke "$PYTHON_FUNCTION" "duckdb:${label}" /tmp/duckdb_${label}.json

  OUT_URLS["zpq:${label}"]="$zpq_out"
  OUT_URLS["polars:${label}"]="$polars_out"
  OUT_URLS["duckdb:${label}"]="$duckdb_out"
}

{
  printf "label\ttotal_ms\tbytes_out\trows_kept\n"
  for scenario in "${SCENARIOS[@]}"; do
    IFS='|' read -r label predicate pct <<<"$scenario"
    run_scenario "$label" "$predicate" "$pct"
  done
} | tee "$RESULTS_OUT"

echo
echo "===================================================================="
echo "Validating outputs..."
echo "===================================================================="
{
  for k in "${!OUT_URLS[@]}"; do
    printf "%s\t%s\n" "$k" "${OUT_URLS[$k]}"
  done
} | python3 benchmarks/validate_outputs.py --from-stdin | tee "$VALIDATION_OUT"
