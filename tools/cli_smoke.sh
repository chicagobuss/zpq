#!/usr/bin/env bash
# CLI smoke + cross-impl validation. The front-door test: generate a
# mixed-type fixture with pyarrow, run the four canonical query shapes
# through the zpq CLI, and validate every output *by value* with
# pyarrow. CI runs exactly this script; run it locally before pushing.
#
# Usage: tools/cli_smoke.sh [path-to-zpq]   (default zig-out/bin/zpq)
set -euo pipefail

# Python with pyarrow: prefer $PYTHON, else zpq's in-project uv .venv, else
# system python3. (System python3 here is 3.14 — no pyarrow wheel — so the
# .venv from `uv venv --python 3.12 && uv pip install pyarrow` is what works.)
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  if [ -x .venv/bin/python ]; then PY=".venv/bin/python"; else PY="python3"; fi
fi

ZPQ="${1:-zig-out/bin/zpq}"
DIR="$(mktemp -d /tmp/zpq-smoke.XXXXXX)"
trap 'rm -rf "$DIR"' EXIT

test -x "$ZPQ" || { echo "zpq binary not found at $ZPQ — zig build -Doptimize=ReleaseFast" >&2; exit 2; }

# Fixture: 50k rows, mixed types; nullable columns sprinkle nulls
# every 5th row so the coalesce scenario has something to replace.
"$PY" - "$DIR" <<'PY'
import sys
import pyarrow as pa, pyarrow.parquet as pq
d = sys.argv[1]
n = 50_000
t = pa.table({
    "i32":       pa.array([i - n//2 for i in range(n)], type=pa.int32()),
    "i64":       pa.array([i * 7 for i in range(n)], type=pa.int64()),
    "f64":       pa.array([i * 0.5 for i in range(n)], type=pa.float64()),
    "name":      pa.array([f"row_{i:05d}" for i in range(n)], type=pa.string()),
    "i32_null":  pa.array([None if i % 5 == 0 else i for i in range(n)], type=pa.int32()),
    "name_null": pa.array([None if i % 5 == 0 else f"v_{i}" for i in range(n)], type=pa.string()),
})
pq.write_table(t, f"{d}/in.parquet", compression="snappy")
PY

# 1. Filter + projection.
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/filtered.parquet" \
  --filter "i32 >= 0" --columns i32,i64,name
# 2. Select with arithmetic + string concat.
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/selected.parquet" \
  --select "i32, i32 + 1000 AS shifted, name || '!' AS shouted"
# 3. BETWEEN.
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/between.parquet" \
  --filter "i64 BETWEEN 0 AND 1000"
# 4. coalesce — substitute defaults for nulls.
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/coalesced.parquet" \
  --select "coalesce(i32_null, -1) AS i32_filled, coalesce(name_null, '<missing>') AS name_filled"

# Validate every output via pyarrow, by value.
"$PY" - "$DIR" <<'PY'
import sys
import pyarrow.parquet as pq
d = sys.argv[1]
src = pq.read_table(f"{d}/in.parquet")

# Filter test: every row's i32 >= 0; row count matches.
f = pq.read_table(f"{d}/filtered.parquet")
assert f.num_columns == 3, f"expected 3 cols, got {f.num_columns}"
assert all(v >= 0 for v in f["i32"].to_pylist()), "filter let through negative i32"
expected_rows = sum(1 for v in src["i32"].to_pylist() if v >= 0)
assert f.num_rows == expected_rows, f"row count: {f.num_rows} vs expected {expected_rows}"

# Select test: arithmetic + string concat correctness.
s = pq.read_table(f"{d}/selected.parquet")
schema = {fld.name: str(fld.type) for fld in s.schema}
assert schema == {"i32": "int32", "shifted": "int64", "shouted": "string"}, \
    f"unexpected schema: {schema}"
assert all(a + 1000 == b for a, b in zip(s["i32"].to_pylist(), s["shifted"].to_pylist()))
assert all(n + "!" == sh for n, sh in zip(src["name"].to_pylist(), s["shouted"].to_pylist()))

# BETWEEN test: inclusive bounds.
b = pq.read_table(f"{d}/between.parquet")
assert all(0 <= v <= 1000 for v in b["i64"].to_pylist())

# coalesce test: every null in the source becomes the default in the
# output; non-null rows pass through unchanged.
c = pq.read_table(f"{d}/coalesced.parquet")
src_i32_null = src["i32_null"].to_pylist()
src_name_null = src["name_null"].to_pylist()
out_i32 = c["i32_filled"].to_pylist()
out_name = c["name_filled"].to_pylist()
assert not any(v is None for v in out_i32), "coalesce left nulls in i32 output"
assert not any(v is None for v in out_name), "coalesce left nulls in name output"
for src_v, out_v in zip(src_i32_null, out_i32):
    expected = -1 if src_v is None else src_v
    assert out_v == expected, f"coalesce(i32) mismatch: src={src_v}, out={out_v}"
for src_v, out_v in zip(src_name_null, out_name):
    expected = "<missing>" if src_v is None else src_v
    assert out_v == expected, f"coalesce(name) mismatch: src={src_v!r}, out={out_v!r}"
null_count_src = sum(1 for v in src_i32_null if v is None)
assert null_count_src > 0, "fixture should contain nulls"

print("All cross-impl checks passed")
PY
