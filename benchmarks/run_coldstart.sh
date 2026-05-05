#!/usr/bin/env bash
# Cold-start head-to-head: ZPQ vs Polars vs DuckDB.
#
# Method: bump a `BENCH_NONCE` env var on each function before each
# trial. Updating function-configuration invalidates all warm
# containers, guaranteeing the next invocation is cold. We capture
# `Init Duration` from the REPORT log line (only emitted on cold
# starts) and `Duration` (end-to-end execution time after init).
#
# We use the same partition-prune workload as the warm bench so the
# numbers are directly comparable: 10 hive-partitioned files,
# `month >= 08` filter, 3 surviving files written.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

REGION="${AWS_REGION:-us-west-2}"
BUCKET="${AWS_S3_BUCKET}"
TRIALS="${TRIALS:-5}"

# Build the inputs once.
INPUTS=()
for mm in 01 02 03 04 05 06 07 08 09 10; do
  INPUTS+=("s3://${BUCKET}/zpq_test_data/partitioned/year=2026/month=${mm}/data.parquet")
done
INPUTS_JSON=$(printf '"%s",' "${INPUTS[@]}" | sed 's/,$//')
GLOB="s3://${BUCKET}/zpq_test_data/partitioned/year=2026/month=*/data.parquet"

ts() { date +%s%N; }

# Bump BENCH_NONCE on a function so the next invocation is cold.
# Wait for the config update to settle before invoking.
force_cold() {
  local fn="$1"
  local nonce
  nonce="$(date +%s%N)"
  aws lambda update-function-configuration \
    --function-name "$fn" \
    --environment "Variables={BENCH_NONCE=$nonce}" \
    --region "$REGION" >/dev/null
  # Poll until the update completes; AWS rejects invokes during
  # InProgress so this matters.
  for _ in $(seq 1 30); do
    sleep 1
    s=$(aws lambda get-function-configuration \
        --function-name "$fn" --region "$REGION" \
        --query 'LastUpdateStatus' --output text)
    [[ "$s" == "Successful" ]] && return 0
  done
  echo "force_cold timeout on $fn" >&2
  return 1
}

# Invoke once, capture stderr (which contains the REPORT line),
# parse Init/Duration in ms. Output: "label\ttrial\tinit_ms\tduration_ms\ttotal_ms"
invoke_cold() {
  local fn="$1" label="$2" payload_file="$3" trial="$4"

  local t_start
  t_start="$(date +%s%N)"
  local logs_b64
  logs_b64=$(aws lambda invoke \
      --function-name "$fn" \
      --cli-binary-format raw-in-base64-out \
      --payload "file://$payload_file" \
      --cli-read-timeout 180 \
      --log-type Tail \
      --query 'LogResult' \
      --output text \
      --region "$REGION" \
      /tmp/cs_resp.json 2>&1)
  local t_end
  t_end="$(date +%s%N)"
  local total_ms=$(( (t_end - t_start) / 1000000 ))

  local logs
  logs=$(echo "$logs_b64" | base64 -d 2>/dev/null || echo "")

  # Parse REPORT line. Init Duration is only present on cold starts.
  local init_ms duration_ms
  init_ms=$(echo "$logs" | grep -oE 'Init Duration: [0-9.]+ ms' | head -1 | grep -oE '[0-9.]+' | head -1)
  duration_ms=$(echo "$logs" | grep -oE 'Duration: [0-9.]+ ms' | head -1 | grep -oE '[0-9.]+' | head -1)

  printf "%s\t%d\t%s\t%s\t%d\n" "$label" "$trial" "${init_ms:-NA}" "${duration_ms:-NA}" "$total_ms"
}

# Pre-build payloads.
ZPQ_OUT="s3://$BUCKET/bench/$(ts)/cs_zpq.parquet"
POLARS_OUT="s3://$BUCKET/bench/$(ts)/cs_polars.parquet"
DUCKDB_OUT="s3://$BUCKET/bench/$(ts)/cs_duckdb.parquet"

cat > /tmp/cs_zpq.json <<EOF
{"inputs":[$INPUTS_JSON],"output_url":"$ZPQ_OUT","filter":"month >= 08"}
EOF
cat > /tmp/cs_polars.json <<EOF
{"mode":"polars_multi","inputs":[$INPUTS_JSON],"output_url":"$POLARS_OUT","filter_sql":"month >= 8"}
EOF
cat > /tmp/cs_duckdb.json <<EOF
{"mode":"duckdb_multi","inputs":["$GLOB"],"output_url":"$DUCKDB_OUT","filter_sql":"CAST(month AS INTEGER) >= 8"}
EOF

# Run the bench.
{
  printf "label\ttrial\tinit_ms\tduration_ms\ttotal_caller_ms\n"
  for trial in $(seq 1 "$TRIALS"); do
    echo "# trial $trial" >&2

    echo "  zpq cold..." >&2
    force_cold zpq-filter-s3
    invoke_cold zpq-filter-s3 "zpq" /tmp/cs_zpq.json "$trial"

    echo "  polars cold..." >&2
    force_cold zpq-bench-python
    invoke_cold zpq-bench-python "polars" /tmp/cs_polars.json "$trial"

    echo "  duckdb cold..." >&2
    force_cold zpq-bench-python
    invoke_cold zpq-bench-python "duckdb" /tmp/cs_duckdb.json "$trial"
  done
} | tee benchmarks/coldstart_results.tsv

echo
echo "===================================================================="
echo "Cold-start summary (median of $TRIALS trials)"
echo "===================================================================="
python3 <<'PY'
import statistics
from collections import defaultdict

rows = open("benchmarks/coldstart_results.tsv").read().strip().splitlines()[1:]
buckets = defaultdict(lambda: {"init": [], "dur": [], "total": []})
for line in rows:
    label, _, init_s, dur_s, total_s = line.split("\t")
    if init_s != "NA":
        buckets[label]["init"].append(float(init_s))
    if dur_s != "NA":
        buckets[label]["dur"].append(float(dur_s))
    buckets[label]["total"].append(float(total_s))

print(f"{'engine':<10}{'init_ms':>12}{'duration_ms':>14}{'total_ms':>12}")
print("-" * 50)
for label in ("zpq", "polars", "duckdb"):
    if label not in buckets:
        continue
    b = buckets[label]
    init_med = statistics.median(b["init"]) if b["init"] else float("nan")
    dur_med = statistics.median(b["dur"]) if b["dur"] else float("nan")
    tot_med = statistics.median(b["total"]) if b["total"] else float("nan")
    print(f"{label:<10}{init_med:>12.0f}{dur_med:>14.0f}{tot_med:>12.0f}")
PY

# Validate outputs (last cold invocation wrote each).
echo
echo "===================================================================="
echo "Validating cold-start outputs..."
echo "===================================================================="
python3 benchmarks/validate_outputs.py --from-stdin <<EOF
zpq:cold	$ZPQ_OUT
polars:cold	$POLARS_OUT
duckdb:cold	$DUCKDB_OUT
EOF
