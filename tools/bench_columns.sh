#!/usr/bin/env bash
set -euo pipefail

INPUT="/tmp/zpq_r2_bucket/benchmark/benchmark_100mb.parquet"
OUTPUT_DIR="/tmp/zpq_bench_columns"
BIN="./zig-out/bin/zpq"

mkdir -p "$OUTPUT_DIR"

# Column categories and their pass-through filters (NO SPACES!)
# Format: "col|filter"
COLS=(
  "int8|int8=-1"
  "int16|int16=-1"
  "int32_sorted|int32_sorted=0"
  "int32_random|int32_random=0"
  "int64_sorted|int64_sorted=0"
  "int64_random|int64_random=0"
  "uint8|uint8=0"
  "uint16|uint16=0"
  "uint32|uint32=0"
  "uint64|uint64=0"
  "float32|float32=0.0"
  "float64|float64=0.0"
  "bool|bool=true"
  "bool_sparse|bool_sparse=true"
  "string_random|string_random=row_000000"
  "string_dict_low|string_dict_low=category_0001"
  "string_dict_high|string_dict_high=category_0001"
  "timestamp|timestamp=1704067200000"
  "date|date=19723"
  "int32_nullable|int32_nullable=0"
  "string_nullable|string_nullable=row_000000"
  "int32_sparse|int32_sparse=0"
)

echo "=== Per-Column Benchmark (Select + Filter) ==="
printf "%-20s | %-10s | %-10s\n" "Column" "Time (ms)" "Rows"
echo "----------------------------------------------------"

for entry in "${COLS[@]}"; do
  IFS="|" read -r col filter <<< "$entry"
  out_file="$OUTPUT_DIR/${col}.parquet"
  
  # Measure time
  # Using ReleaseFast binary
  start=$(date +%s%N)
  row_info=$("$BIN" "$INPUT" -o "$out_file" -s "$col" -f "$filter" 2>&1 || echo "Error")
  end=$(date +%s%N)
  
  if [[ "$row_info" == "Error" ]]; then
    printf "%-20s | %-10s | %-10s\n" "$col" "FAILED" "N/A"
    continue
  fi
  
  duration=$(( (end - start) / 1000000 ))
  
  # Extract row count from output
  rows=$(echo "$row_info" | grep "Output:" | awk '{print $2}')
  
  if [[ -z "$rows" ]]; then
     # Check if the error message is about missing rows (legitimate for 0-match)
     if echo "$row_info" | grep -q "0 rows"; then
        printf "%-20s | %-10d | %-10s\n" "$col" "$duration" "0"
     else
        printf "%-20s | %-10d | %-10s\n" "$col" "$duration" "PARSE_ERR"
     fi
  else
     printf "%-20s | %-10d | %-10s\n" "$col" "$duration" "$rows"
  fi
done

echo "----------------------------------------------------"
echo "Done."
