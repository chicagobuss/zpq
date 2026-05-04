#!/usr/bin/env bash
# ZPQ writer head-to-head benchmark.
# Compares zpq-filter-s3 (Zig) against zpq-bench-python (boto3/polars/duckdb)
# for the "filter Parquet on S3 + write back" workload.

set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a

REGION="${AWS_REGION:-us-west-2}"
INPUT="s3://${AWS_S3_BUCKET}/zpq_test_data/benchmark/benchmark_100mb.parquet"
RUNS="${RUNS:-5}"

run_zpq() {
  local label="$1" filter="$2"
  for i in $(seq 1 "$RUNS"); do
    local ts=$(date +%s%N)
    local payload="{\"s3_url\":\"$INPUT\",\"filter\":\"$filter\",\"output_url\":\"s3://${AWS_S3_BUCKET}/bench/${ts}/zpq_${label}.parquet\"}"
    aws lambda invoke --function-name zpq-filter-s3 \
      --cli-binary-format raw-in-base64-out --payload "$payload" \
      --cli-read-timeout 180 --region "$REGION" /tmp/bench.json >/dev/null 2>&1
    jq -r --arg label "zpq:$label" \
      '[$label, .total_ms, .fetch_ms, .put_ms, .bytes_out, .row_groups_pruned, .upload] | @tsv' \
      /tmp/bench.json
  done
}

run_zpq_proj() {
  local label="$1"
  for i in $(seq 1 "$RUNS"); do
    local ts=$(date +%s%N)
    local payload="{\"s3_url\":\"$INPUT\",\"output_url\":\"s3://${AWS_S3_BUCKET}/bench/${ts}/zpq_${label}.parquet\",\"columns\":[\"int8\"]}"
    aws lambda invoke --function-name zpq-filter-s3 \
      --cli-binary-format raw-in-base64-out --payload "$payload" \
      --cli-read-timeout 180 --region "$REGION" /tmp/bench.json >/dev/null 2>&1
    jq -r --arg label "zpq:$label" \
      '[$label, .total_ms, .fetch_ms, .put_ms, .bytes_out, .row_groups_pruned, .upload] | @tsv' \
      /tmp/bench.json
  done
}

run_python_proj() {
  local mode="$1" label="$2"
  for i in $(seq 1 "$RUNS"); do
    local ts=$(date +%s%N)
    local out="s3://${AWS_S3_BUCKET}/bench/${ts}/py_${label}.parquet"
    local payload="{\"mode\":\"$mode\",\"s3_url\":\"$INPUT\",\"output_url\":\"$out\",\"columns\":[\"int8\"]}"
    aws lambda invoke --function-name zpq-bench-python \
      --cli-binary-format raw-in-base64-out --payload "$payload" \
      --cli-read-timeout 180 --region "$REGION" /tmp/bench.json >/dev/null 2>&1
    jq -r --arg label "py:$label" \
      '[$label, .total_ms, "", "", .bytes_out, "", ""] | @tsv' \
      /tmp/bench.json
  done
}

run_python() {
  local mode="$1" label="$2" filter_sql="${3:-}"
  for i in $(seq 1 "$RUNS"); do
    local ts=$(date +%s%N)
    local out="s3://${AWS_S3_BUCKET}/bench/${ts}/py_${label}.parquet"
    local payload
    if [[ -z "$filter_sql" ]]; then
      payload="{\"mode\":\"$mode\",\"s3_url\":\"$INPUT\",\"output_url\":\"$out\"}"
    else
      payload="{\"mode\":\"$mode\",\"s3_url\":\"$INPUT\",\"output_url\":\"$out\",\"filter_sql\":\"$filter_sql\"}"
    fi
    aws lambda invoke --function-name zpq-bench-python \
      --cli-binary-format raw-in-base64-out --payload "$payload" \
      --cli-read-timeout 180 --region "$REGION" /tmp/bench.json >/dev/null 2>&1
    jq -r --arg label "py:$label" \
      '[$label, .total_ms, (.fetch_ms // ""), (.put_ms // ""), .bytes_out, "", ""] | @tsv' \
      /tmp/bench.json
  done
}

# tab-separated output: label, total_ms, fetch_ms, put_ms, bytes_out, rg_pruned, upload
{
  printf "label\ttotal_ms\tfetch_ms\tput_ms\tbytes_out\trg_pruned\tupload\n"
  run_zpq    "copy"        ""
  run_zpq    "prune_all"   "int8>9999"
  run_zpq_proj  "project"
  run_python "boto3_copy"  "boto3"
  run_python "polars_copy" "polars_copy"
  run_python "polars_copy" "polars_prune"     "int8 > 9999"
  run_python_proj "polars_project" "polars_project"
  run_python "duckdb_copy" "duckdb_copy"
  run_python "duckdb_copy" "duckdb_prune"     "int8 > 9999"
  run_python_proj "duckdb_project" "duckdb_project"
} | tee benchmarks/results.tsv
