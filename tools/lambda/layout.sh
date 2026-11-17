#!/usr/bin/env bash
# Regenerate src/lambda/layout.ld, the order of zpq-lambda's code (docs/lambda_capabilities.md).
#
#   tools/lambda/layout.sh <lineitem.parquet> [s3://bucket/lineitem.parquet]
#
# Builds zpq-lambda for x86_64 and aarch64 with symbols, runs each under qemu-user (qemu-x86_64, qemu-aarch64) to its
# first /next, through a cold and then a warm TPC-H Q1 over the local file, and, given the S3 copy and AWS_*
# credentials in the environment, through the same two over S3; then lists the code that ran, first and in that order. Any lineitem
# slice of ~100k rows will do. Run it when the start-up or first-request code moves; a stale list only costs locality.
set -euo pipefail
cd "$(dirname "$0")/../.."
input=$(realpath "$1"); s3=${2:-}
work=zig-out/lambda-layout
rm -rf "$work"; mkdir -p "$work"
cp src/lambda/layout.ld "$work/seed.ld"; echo "/* $(date +%s) */" >> "$work/seed.ld"  # a new script relinks
query='"filter":"l_shipdate <= DATE '"'"'1998-09-02'"'"'","group_by":"l_returnflag,l_linestatus","aggregate":"sum(l_quantity),sum(l_extendedprice*(1-l_discount)),avg(l_discount),count(*)"'
echo "{\"inputs\":[\"$input\"],$query}" > "$work/local.json"
[ -n "$s3" ] && echo "{\"inputs\":[\"$s3\"],$query}" > "$work/s3.json"
groups=()
for arch in x86_64 aarch64; do
  out=$work/$arch
  zig build lambda -Doptimize=ReleaseFast -Dcpu=baseline -Dtarget=$arch-linux-musl -Dlambda-layout="$work/seed.ld" \
    --verbose-link --prefix "$out" 2> "$out.build" || { cat "$out.build"; exit 1; }
  grep -o 'ld.lld .*' "$out.build" | head -1 > "$out.link"
  python3 tools/lambda/trace.py qemu-$arch "$out/bin/zpq-lambda" "$out.next.log"
  python3 tools/lambda/trace.py qemu-$arch "$out/bin/zpq-lambda" "$out.local.log" "$work/local.json"
  logs=("$out.next.log" "$out.local.log")
  if [ -n "$s3" ]; then
    python3 tools/lambda/trace.py qemu-$arch "$out/bin/zpq-lambda" "$out.s3.log" "$work/s3.json"
    logs+=("$out.s3.log")
  fi
  [ ${#groups[@]} -gt 0 ] && groups+=(--)
  groups+=("$out.link" "$out/bin/zpq-lambda" "${logs[@]}")
done
python3 tools/lambda/layout.py --out src/lambda/layout.ld "${groups[@]}"
