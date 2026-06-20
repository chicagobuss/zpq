#!/usr/bin/env bash
# B1b round-trip stress test. Pushes data/nested_edges.parquet through
# ZPQ via Lambda in multiple filter+project shapes, then validates each
# output against pyarrow + duckdb + hardwood AND compares row counts /
# specific values to a python reference computation.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

REGION="${AWS_REGION:-us-west-2}"
BUCKET="${AWS_S3_BUCKET}"
SRC="s3://${BUCKET}/zpq_test_data/nested_edges.parquet"

# (label, filter_expr, projection_csv)
declare -a SCENARIOS=(
  "all_passthrough|        |id,score,tags,meta,events,counts"
  "filter_select_some|id >= 10|id,tags"
  "filter_select_one|id = 5  |id,tags,score"
  "filter_select_none|id > 999|id,tags"
  "filter_select_first_half|id < 10|id,score,events"
  "project_struct_only|     |id,meta"
  "project_list_of_struct|id >= 5|id,events"
  "project_map|id < 8       |id,counts"
)

ts() { date +%s%N; }

echo "==================================================================="
echo "B1.0 nested round-trip: filter+encode through Lambda, validate"
echo "==================================================================="
PASS=0
FAIL=0
declare -a FAILED_LABELS=()

for scenario in "${SCENARIOS[@]}"; do
  IFS='|' read -r label filter cols <<<"$scenario"
  filter="$(echo "$filter" | xargs)"
  cols="$(echo "$cols" | xargs | tr -d ' ')"

  echo
  echo "------- $label -------"
  echo "  filter='$filter'  cols=$cols"

  out_url="s3://${BUCKET}/bench/$(ts)/nest_${label}.parquet"
  cols_json=$(printf '"%s",' $(echo "$cols" | tr ',' ' ') | sed 's/,$//')

  if [[ -n "$filter" ]]; then
    cat > /tmp/zpq_nest.json <<EOF
{"inputs":["${SRC}"],"output_url":"${out_url}","filter":"${filter}","columns":[${cols_json}]}
EOF
  else
    cat > /tmp/zpq_nest.json <<EOF
{"inputs":["${SRC}"],"output_url":"${out_url}","columns":[${cols_json}]}
EOF
  fi

  aws lambda invoke --function-name zpq-filter-s3 \
    --cli-binary-format raw-in-base64-out --payload file:///tmp/zpq_nest.json \
    --cli-read-timeout 180 --region "$REGION" /tmp/zpq_nest_resp.json >/dev/null 2>&1

  resp=$(cat /tmp/zpq_nest_resp.json)
  ok=$(echo "$resp" | jq -r '.ok // false')
  if [[ "$ok" != "true" ]]; then
    err=$(echo "$resp" | jq -r '.errorMessage // .error // "unknown"')
    echo "  ZPQ FAIL: $err"
    FAIL=$((FAIL + 1))
    FAILED_LABELS+=("$label (zpq error: $err)")
    continue
  fi
  echo "  zpq: $(echo "$resp" | jq -c '{rows_kept, bytes_out, total_ms}')"

  # Cross-validate
  if ! python3 benchmarks/validate_outputs.py "$label" "$out_url" 2>&1 | tail -3; then
    echo "  validate FAILED"
    FAIL=$((FAIL + 1))
    FAILED_LABELS+=("$label (validate fail)")
    continue
  fi

  # Oracle compare
  if ! python3 - <<PY
import boto3, io, os, sys, re
import pyarrow.parquet as pq
import pyarrow.compute as pc
import polars as pl

bucket = os.environ["AWS_S3_BUCKET"]
src_key = "zpq_test_data/nested_edges.parquet"
s3 = boto3.client("s3")
src_body = s3.get_object(Bucket=bucket, Key=src_key)["Body"].read()
src_table = pq.read_table(io.BytesIO(src_body))

filter_expr = "$filter"
expected_table = src_table
if filter_expr:
    m = re.match(r"id\s*(>=|<=|!=|=|<|>)\s*(\\S+)", filter_expr)
    if m:
        op, val = m.group(1), int(m.group(2))
        op_map = {"=": pc.equal, "!=": pc.not_equal, "<": pc.less, "<=": pc.less_equal,
                  ">": pc.greater, ">=": pc.greater_equal}
        expected_table = src_table.filter(op_map[op](src_table["id"], val))

cols_str = "$cols"
keep_cols = [c.strip() for c in cols_str.split(",") if c.strip()]
expected_table = expected_table.select(keep_cols)

out_url = "$out_url"
out_body = s3.get_object(Bucket=bucket, Key=out_url.replace(f"s3://{bucket}/", ""))["Body"].read()
out_table = pq.read_table(io.BytesIO(out_body))

ok = True
if out_table.num_rows != expected_table.num_rows:
    print(f"  ROW COUNT FAIL: zpq={out_table.num_rows} expected={expected_table.num_rows}")
    ok = False

out_df = pl.from_arrow(out_table)
exp_df = pl.from_arrow(expected_table)
for c in keep_cols:
    if out_df[c].to_list() != exp_df[c].to_list():
        print(f"  VALUE FAIL on column '{c}'")
        print(f"    zpq[:3]:  {out_df[c].head(3).to_list()}")
        print(f"    expected: {exp_df[c].head(3).to_list()}")
        ok = False

if ok:
    print(f"  ORACLE OK  rows={out_table.num_rows}")
sys.exit(0 if ok else 1)
PY
  then
    FAIL=$((FAIL + 1))
    FAILED_LABELS+=("$label (oracle mismatch)")
    continue
  fi

  PASS=$((PASS + 1))
done

echo
echo "==================================================================="
echo "Result: $PASS pass / $FAIL fail / $((PASS + FAIL)) total"
if [[ $FAIL -gt 0 ]]; then
  echo "Failed:"
  for l in "${FAILED_LABELS[@]}"; do echo "  - $l"; done
fi
echo "==================================================================="
