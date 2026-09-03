#!/usr/bin/env bash
# Warm Lambda comparison on the real-world Overture Places Parquet fixture.
# Every engine filters confidence > 0.9, projects id + confidence, and writes
# Snappy Parquet back to the same regional S3 bucket.  Results are invalidated
# unless every output passes the strict reader checks at the end.
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE="${ENV_FILE:-.env}"
[[ -r "$ENV_FILE" ]] || { echo "missing readable ENV_FILE=$ENV_FILE" >&2; exit 2; }
set -a; source "$ENV_FILE"; set +a

REGION="${BENCH_REGION:-${AWS_REGION:-us-west-2}}"
BUCKET="${AWS_S3_BUCKET:?AWS_S3_BUCKET is required}"
RUNS="${RUNS:-5}"
ZPQ_FUNCTION="${ZPQ_BENCH_FUNCTION:-zpq-032-zig-x86}"
POLARS_FUNCTION="${POLARS_BENCH_FUNCTION:-zpq-032-polars-x86}"
DUCKDB_FUNCTION="${DUCKDB_BENCH_FUNCTION:-zpq-032-duckdb-x86}"
INPUT_URL="${OVERTURE_INPUT:-s3://${BUCKET}/test/overture_places.snappy.parquet}"
FILTER="${OVERTURE_FILTER:-confidence > 0.9}"
# Defaults are gitignored scratch files. The tracked overture_lambda_*.tsv are
# a curated, bucket-redacted snapshot; copy over them deliberately.
RESULTS_OUT="${RESULTS_OUT:-benchmarks/overture_lambda_results.local.tsv}"
VALIDATION_OUT="${VALIDATION_OUT:-benchmarks/overture_lambda_validation.local.tsv}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
declare -A OUT_URLS=()
results_tmp="$tmp_dir/results.tsv"

make_payload() {
  local engine="$1" output_url="$2"
  case "$engine" in
    zpq)
      jq -n --arg src "$INPUT_URL" --arg out "$output_url" --arg filter "$FILTER" \
        '{s3_url:$src, output_url:$out, filter:$filter, columns:["id", "confidence"]}'
      ;;
    polars)
      jq -n --arg src "$INPUT_URL" --arg out "$output_url" --arg filter "$FILTER" \
        '{mode:"polars_project", s3_url:$src, output_url:$out, filter_sql:$filter, columns:["id", "confidence"]}'
      ;;
    duckdb)
      jq -n --arg src "$INPUT_URL" --arg out "$output_url" --arg filter "$FILTER" \
        '{mode:"duckdb_project", s3_url:$src, output_url:$out, filter_sql:$filter, columns:["id", "confidence"]}'
      ;;
    *) echo "unknown engine $engine" >&2; exit 2 ;;
  esac
}

function_for() {
  case "$1" in
    zpq) echo "$ZPQ_FUNCTION" ;;
    polars) echo "$POLARS_FUNCTION" ;;
    duckdb) echo "$DUCKDB_FUNCTION" ;;
  esac
}

invoke_one() {
  local engine="$1" sample="$2" kind="$3"
  local function output_url payload response total bytes
  function=$(function_for "$engine")
  output_url="s3://${BUCKET}/bench/zpq-032/overture/${RUN_ID}/${engine}-${kind}-${sample}.parquet"
  payload="$tmp_dir/${engine}-${kind}-${sample}.json"
  response="$tmp_dir/${engine}-${kind}-${sample}.response.json"
  make_payload "$engine" "$output_url" >"$payload"
  aws lambda invoke --function-name "$function" --region "$REGION" \
    --cli-binary-format raw-in-base64-out --payload "file://$payload" \
    --cli-read-timeout 300 "$response" >/dev/null
  if jq -e '.error or .errorMessage' "$response" >/dev/null; then
    echo "${engine} ${kind} ${sample}: Lambda returned an error" >&2
    cat "$response" >&2
    exit 1
  fi
  total=$(jq -er '.total_ms | numbers' "$response")
  bytes=$(jq -er '.bytes_out | numbers' "$response")
  OUT_URLS["${engine}:${sample}"]="$output_url"
  if [[ "$kind" == sample ]]; then
    printf '%s\t%s\t%s\t%s\t%s\n' "$engine" "$sample" "$total" "$bytes" "$output_url"
  fi
}

echo "# one warm-up per function" >&2
for engine in zpq polars duckdb; do
  invoke_one "$engine" 0 warmup >/dev/null
done

printf 'engine\tsample\tlambda_ms\tbytes_out\toutput_url\n' >"$results_tmp"
for engine in zpq polars duckdb; do
  for sample in $(seq 1 "$RUNS"); do
    invoke_one "$engine" "$sample" sample >>"$results_tmp"
  done
done
cp "$results_tmp" "$RESULTS_OUT"
cat "$RESULTS_OUT"

{
  for label in "${!OUT_URLS[@]}"; do
    printf '%s\t%s\n' "$label" "${OUT_URLS[$label]}"
  done
} | "$PYTHON_BIN" benchmarks/validate_outputs.py --from-stdin | tee "$VALIDATION_OUT"

echo "validated results: $RESULTS_OUT" >&2
echo "validation sidecar: $VALIDATION_OUT" >&2
