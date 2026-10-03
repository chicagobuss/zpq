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

# A top-level column whose name contains a dot is not a nested field: --select
# passes it through with its type and nulls.
"$PY" - "$DIR" <<'PY'
import sys
import pyarrow as pa, pyarrow.parquet as pq
pq.write_table(pa.table({"a.b": pa.array([1, None, 3], type=pa.int32()), "k": pa.array([7, 8, 9], type=pa.int64())}),
               f"{sys.argv[1]}/dotted.parquet")
PY
"$ZPQ" query "$DIR/dotted.parquet" -o "$DIR/dotted_sel.parquet" --select '"a.b", k' 2> /dev/null
"$PY" - "$DIR" <<'PY'
import sys
import pyarrow.parquet as pq
t = pq.read_table(f"{sys.argv[1]}/dotted_sel.parquet")
assert {f.name: str(f.type) for f in t.schema} == {"a.b": "int32", "k": "int64"}, t.schema
assert t["a.b"].to_pylist() == [1, None, 3], t["a.b"].to_pylist()
print("Dotted top-level column checks passed")
PY

# ...nor is it confused with the field b of a group a, whose path joins to the
# same "a.b": both columns read, --columns a.b names the top-level one, and the
# bare b binds to the nested field.
"$PY" - "$DIR" <<'PY'
import sys
import pyarrow as pa, pyarrow.parquet as pq
pq.write_table(pa.table({"a.b": pa.array([1, None, 3], type=pa.int64()),
                         "a": pa.array([{"b": 10}, {"b": 20}, {"b": None}], type=pa.struct([("b", pa.int64())]))}),
               f"{sys.argv[1]}/dotted_twin.parquet")
PY
"$ZPQ" query "$DIR/dotted_twin.parquet" -o "$DIR/dotted_twin_top.parquet" --columns a.b 2> /dev/null
"$ZPQ" query "$DIR/dotted_twin.parquet" -o "$DIR/dotted_twin_both.parquet" --columns a.b,a 2> /dev/null
"$ZPQ" query "$DIR/dotted_twin.parquet" -o "$DIR/dotted_twin_filtered.parquet" --filter 'b > 10' 2> /dev/null
"$PY" - "$DIR" <<'PY'
import sys
import pyarrow.parquet as pq
d = sys.argv[1]
t = pq.read_table(f"{d}/dotted_twin_top.parquet")
assert t.schema.names == ["a.b"] and t["a.b"].to_pylist() == [1, None, 3], t.to_pylist()
t = pq.read_table(f"{d}/dotted_twin_both.parquet")
assert t.to_pylist() == [{"a.b": 1, "a": {"b": 10}}, {"a.b": None, "a": {"b": 20}}, {"a.b": 3, "a": {"b": None}}], t.to_pylist()
t = pq.read_table(f"{d}/dotted_twin_filtered.parquet")
assert t.to_pylist() == [{"a.b": None, "a": {"b": 20}}], t.to_pylist()
print("Dotted top-level beside nested field checks passed")
PY

# Flat output (rows, GROUP BY keys, a flat --select) names the nested field by
# the quoted path "a"."b" rather than a second "a.b": one name per column, and
# each name binds back through --columns, --filter and expressions.
"$PY" - "$ZPQ" "$DIR" <<'PY'
import csv, io, json, subprocess, sys
import pyarrow as pa, pyarrow.parquet as pq
zpq, d = sys.argv[1], sys.argv[2]
src = f"{d}/dotted_twin.parquet"
Q = '"a"."b"'

def run(*args):
    return subprocess.run([zpq, "query", *args], check=True, capture_output=True, text=True).stdout

def no_dup_keys(pairs):
    keys = [k for k, _ in pairs]
    assert len(keys) == len(set(keys)), f"duplicate JSON keys: {keys}"
    return dict(pairs)

def jsonl(*args):
    out = run(src, "--format", "jsonl", *args)
    return [json.loads(line, object_pairs_hook=no_dup_keys) for line in out.splitlines()]

rows = jsonl()
assert rows == [{"a.b": 1, Q: 10}, {"a.b": None, Q: 20}, {"a.b": 3, Q: None}], rows
assert [list(r) for r in rows] == [["a.b", Q]] * 3
table = list(csv.reader(io.StringIO(run(src, "--format", "csv"))))
assert table == [["a.b", Q], ["1", "10"], ["", "20"], ["3", ""]], table

# Every printed name selects, and filters on, the column printed under it.
for name, values in (("a.b", [1, None, 3]), (Q, [10, 20, None])):
    assert jsonl("--columns", name) == [{name: v} for v in values], name
    kept = jsonl("--columns", name, "--filter", f"{name} IS NOT NULL")
    assert kept == [{name: v} for v in values if v is not None], (name, kept)
assert jsonl("--filter", f"{Q} > 10") == [{"a.b": None, Q: 20}]

# GROUP BY on each column, and on both: the aggregate JSON keys and the grouped parquet.
def agg(*args):
    return json.loads(run(src, *args), object_pairs_hook=no_dup_keys)["agg"]
assert agg("--group-by", "b", "--aggregate", "count(*) AS n") == [{Q: None, "n": 1}, {Q: 10, "n": 1}, {Q: 20, "n": 1}]
assert agg("--group-by", Q, "--aggregate", 'max("a.b") AS m') == [
    {Q: None, "m": 3}, {Q: 10, "m": 1}, {Q: 20, "m": None}]
assert agg("--group-by", '"a.b"', "--aggregate", f"max({Q}) AS m") == [
    {"a.b": None, "m": 20}, {"a.b": 1, "m": 10}, {"a.b": 3, "m": None}]
got = agg("--group-by", 'b, "a.b"', "--aggregate", "count(*) AS n")
assert got == [{Q: None, "a.b": 3, "n": 1}, {Q: 10, "a.b": 1, "n": 1}, {Q: 20, "a.b": None, "n": 1}], got
run(src, "-o", f"{d}/dotted_twin_gb.parquet", "--group-by", 'b, "a.b"', "--aggregate", "count(*) AS n")
t = pq.read_table(f"{d}/dotted_twin_gb.parquet")
assert t.schema.names == [Q, "a.b", "n"], t.schema.names

# Parquet output keeps the struct; a flat --select names the field like the row formats.
run(src, "-o", f"{d}/dotted_twin_q.parquet", "--columns", Q)
t = pq.read_table(f"{d}/dotted_twin_q.parquet")
assert t.to_pylist() == [{"a": {"b": 10}}, {"a": {"b": 20}}, {"a": {"b": None}}], t.to_pylist()
pq.write_table(pa.table({"a.b": pa.array([1, None, 3], type=pa.int64()),
                         "a": pa.array([{"b": 10}, {"b": 20}, {"b": 30}], type=pa.struct([("b", pa.int64())]))}),
               f"{d}/dotted_twin_nn.parquet")
run(f"{d}/dotted_twin_nn.parquet", "-o", f"{d}/dotted_twin_sel.parquet", "--select", f'"a.b", {Q}')
t = pq.read_table(f"{d}/dotted_twin_sel.parquet")
assert t.to_pylist() == [{"a.b": 1, Q: 10}, {"a.b": None, Q: 20}, {"a.b": 3, Q: 30}], t.to_pylist()

# Only a top-level column literally named "a"."b", quotes included, leaves the field no name: refuse, naming it.
pq.write_table(pa.table({"a.b": [1], Q: [2], "a": pa.array([{"b": 3}], type=pa.struct([("b", pa.int64())]))}),
               f"{d}/dotted_triplet.parquet")
for args in (["--format", "jsonl"], ["--group-by", "b", "--aggregate", "count(*) AS n"]):
    p = subprocess.run([zpq, "query", f"{d}/dotted_triplet.parquet", *args], capture_output=True, text=True)
    assert p.returncode != 0 and not p.stdout and Q in p.stderr, (args, p)
out = run(f"{d}/dotted_triplet.parquet", "--format", "jsonl", "--select", 'b AS ab, "a.b"')
assert json.loads(out) == {"ab": 3, "a.b": 1}, out
print("Dotted top-level beside nested field output naming checks passed")
PY

# --query binds identifiers as the flag forms do: b and "a"."b" the nested
# field, a.b and "a.b" the top-level column.
"$PY" - "$ZPQ" "$DIR" <<'PY'
import json, subprocess, sys
import pyarrow as pa, pyarrow.parquet as pq
zpq, d = sys.argv[1], sys.argv[2]
src, nn = f"{d}/dotted_twin.parquet", f"{d}/dotted_twin_nn.parquet"

def run(*args):
    return subprocess.run([zpq, "query", *args], check=True, capture_output=True, text=True).stdout

def written(*args):
    out = f"{d}/dotted_sql.parquet"
    run(*args, "-o", out)
    t = pq.read_table(out)
    return t.schema.names, t.to_pylist()

nested, top = [None, 10, 20], [None, 1, 3]
for name, keys in (("b", nested), ('"a"."b"', nested), ("a.b", top), ('"a.b"', top)):
    # The key is named by its label however the reference is spelled, in SQL and with the flag.
    sql = json.loads(run("--query", f"SELECT {name}, count(*) AS n FROM '{src}' GROUP BY {name}"))["agg"]
    flag = json.loads(run(src, "--group-by", name, "--aggregate", "count(*) AS n"))["agg"]
    label = '"a"."b"' if keys == nested else "a.b"
    assert sql == flag and [r[label] for r in sql] == keys, (name, sql, flag)
    sql = written("--query", f"SELECT * FROM '{src}' WHERE {name} IS NOT NULL")
    assert sql == written(src, "--filter", f"{name} IS NOT NULL"), (name, sql)
    sql = written("--query", f"SELECT {name} FROM '{nn}'")
    assert sql == written(nn, "--select", name), (name, sql)
# On an ordinary nested file every spelling of the field labels it r.v, as before quoted paths.
pq.write_table(pa.table({"r": pa.array([{"v": 1}, {"v": 2}, {"v": 1}], type=pa.struct([("v", pa.int64())]))}),
               f"{d}/plain_nested.parquet")
want = [{"r.v": 1, "n": 2}, {"r.v": 2, "n": 1}]
for name in ("v", "r.v", '"r"."v"', '"r".v'):
    sql = json.loads(run("--query", f"SELECT {name}, count(*) AS n FROM '{d}/plain_nested.parquet' GROUP BY {name}"))
    assert sql["agg"] == want, (name, sql["agg"])
    if name != '"r".v':  # SQL's mixed spelling; the flag forms quote every segment or none
        flag = json.loads(run(f"{d}/plain_nested.parquet", "--group-by", name, "--aggregate", "count(*) AS n"))
        assert flag["agg"] == want, (name, flag["agg"])
sql = json.loads(run("--query", f"SELECT \"r\".\"v\" AS k, count(*) AS n FROM '{d}/plain_nested.parquet' GROUP BY r.v"))
assert sql["agg"] == [{"k": 1, "n": 2}, {"k": 2, "n": 1}], sql["agg"]
print("SQL identifier binding checks passed")
PY

# An unknown --columns name is an error naming it, on every path, and writes
# nothing; it used to be dropped, leaving a file with fewer columns or none.
"$PY" - "$ZPQ" "$DIR" <<'PY'
import os, subprocess, sys
zpq, d = sys.argv[1], sys.argv[2]
src, out = f"{d}/dotted_twin.parquet", f"{d}/unknown_cols.parquet"
for cols in ("nope", "a.b,nope", '"a"."c"', "a.c"):
    for args in (["-o", out], ["--format", "jsonl"], ["--format", "csv"]):
        p = subprocess.run([zpq, "query", src, "--columns", cols, *args], capture_output=True, text=True)
        bad = cols.split(",")[-1]
        assert p.returncode != 0 and not p.stdout and f"unknown column `{bad}`" in p.stderr, (cols, args, p)
        assert not os.path.exists(out), (cols, args)
# Nor does a list that names nothing select nothing.
for cols in (" ", ",", " , ,"):
    for args in (["-o", out], ["--format", "jsonl"]):
        p = subprocess.run([zpq, "query", src, "--columns", cols, *args], capture_output=True, text=True)
        assert p.returncode != 0 and not p.stdout and "--columns names no column" in p.stderr, (cols, args, p)
        assert not os.path.exists(out), (cols, args)
print("Unknown --columns checks passed")
PY

# --columns binds names like --filter and --select: an exact top-level name
# first (quotes in it included), then a quoted or dotted path, then the bare
# name of the one nested field that has it.
"$PY" - "$ZPQ" "$DIR" <<'PY'
import json, subprocess, sys
import pyarrow as pa, pyarrow.parquet as pq
zpq, d = sys.argv[1], sys.argv[2]
src = f"{d}/odd_names.parquet"
pq.write_table(pa.table({'"x"': [1, 2], '"a"."b"': [3, 4], "x": [5, 6],
                         "a": pa.array([{"b": 7}, {"b": 8}], type=pa.struct([("b", pa.int64())])),
                         "r": pa.array([{"v": None}, {"v": 9}], type=pa.struct([("v", pa.int64())]))}), src)

def jsonl(*args):
    out = subprocess.run([zpq, "query", src, "--format", "jsonl", *args], check=True, capture_output=True, text=True)
    return [json.loads(line) for line in out.stdout.splitlines()]

assert jsonl("--columns", '"x"') == [{'"x"': 1}, {'"x"': 2}]
assert jsonl("--columns", '"a"."b"') == [{'"a"."b"': 3}, {'"a"."b"': 4}]
assert jsonl("--columns", "x") == [{"x": 5}, {"x": 6}]
assert jsonl("--columns", "a.b") == [{"a.b": 7}, {"a.b": 8}]
assert jsonl("--columns", "v") == [{"r.v": None}, {"r.v": 9}]
assert jsonl("--columns", "v", "--filter", "v IS NULL") == [{"r.v": None}]
print("--columns name binding checks passed")
PY

# A write that fails partway leaves no output file behind, and a codec zpq
# cannot decompress is named in the error.
"$PY" - "$DIR" <<'PY'
import sys, datetime
import pyarrow as pa, pyarrow.parquet as pq
d = sys.argv[1]
pq.write_table(pa.table({"x": pa.array([1, 2, 3], type=pa.int64())}), f"{d}/brotli.parquet", compression="brotli")
ts = pa.array([datetime.datetime(2020, 1, i) for i in range(1, 4)], type=pa.timestamp("ns"))
pq.write_table(pa.table({"id": pa.array([1, 2, 3], type=pa.int32()), "ts": ts}), f"{d}/int96.parquet",
               use_deprecated_int96_timestamps=True)
PY
expect_failed_write() {  # <label> <stderr pattern or ""> <zpq query args...>
  local label="$1" pattern="$2"; shift 2
  rm -f "$DIR/failed.parquet"
  if "$ZPQ" query "$@" -o "$DIR/failed.parquet" 2> "$DIR/failed.err"; then
    echo "$label: expected the write to fail" >&2; exit 1
  fi
  if [ -e "$DIR/failed.parquet" ]; then echo "$label: left a partial output file" >&2; exit 1; fi
  if [ -n "$pattern" ] && ! grep -q "$pattern" "$DIR/failed.err"; then
    echo "$label: error does not mention '$pattern':" >&2; cat "$DIR/failed.err" >&2; exit 1
  fi
}
expect_failed_write "brotli --codec zstd" "column \`x\` to ZSTD: its pages use BROTLI" "$DIR/brotli.parquet" --codec zstd
expect_failed_write "int96 re-encode" "" "$DIR/int96.parquet" --filter "id >= 2"
echo "Failed-write checks passed"

# --codec on plain copies: a requested codec must be the one every written
# chunk carries (and the one reported), whether the chunk is copied as is or
# has to be recompressed; with no --codec a copy keeps the source codec.
"$PY" - "$DIR" <<'PY'
import sys
import pyarrow as pa, pyarrow.parquet as pq
d = sys.argv[1]
n = 30_000
t = pa.table({
    "i32": pa.array(range(n), type=pa.int32()),
    "cat": pa.array([f"c{i % 17}" for i in range(n)], type=pa.string()),
    "opt": pa.array([None if i % 3 == 0 else i * 0.25 for i in range(n)], type=pa.float64()),
})
# Small v2 pages with dictionaries and a page index, so recompression has to
# rewrite many page headers, leave levels alone and remap every page location.
pq.write_table(t, f"{d}/pages.parquet", compression="snappy", data_page_version="2.0",
               data_page_size=4096, write_page_index=True, row_group_size=10_000)
pq.write_table(t, f"{d}/zstd_src.parquet", compression="zstd")
PY
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/to_zstd.parquet" --codec zstd 2> "$DIR/to_zstd.json"
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/proj_gzip.parquet" --columns i32,name --codec gzip 2> "$DIR/proj_gzip.json"
"$ZPQ" query "$DIR/pages.parquet" -o "$DIR/pages_zstd.parquet" --codec zstd 2> "$DIR/pages_zstd.json"
"$ZPQ" query "$DIR/pages.parquet" -o "$DIR/pages_raw.parquet" --codec uncompressed 2> "$DIR/pages_raw.json"
"$ZPQ" query "$DIR/zstd_src.parquet" -o "$DIR/kept.parquet" 2> "$DIR/kept.json"
"$ZPQ" query "$DIR/in.parquet" "$DIR/to_zstd.parquet" -o "$DIR/mixed.parquet" 2> "$DIR/mixed.json"
"$ZPQ" query "$DIR/in.parquet" "$DIR/to_zstd.parquet" -o "$DIR/unmixed.parquet" --codec gzip 2> "$DIR/unmixed.json"
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/same_snappy.parquet" --codec snappy 2> /dev/null
"$ZPQ" query "$DIR/in.parquet" -o "$DIR/no_codec.parquet" 2> /dev/null
for src in in pages; do
  for out in "$src" "$( [ "$src" = in ] && echo to_zstd || echo pages_zstd )"; do
    "$ZPQ" query "$DIR/$out.parquet" --filter 'i32 >= 12345' --aggregate 'count(*) AS n, sum(i32) AS s' \
      > "$DIR/agg_$out.json"
  done
done
"$PY" - "$DIR" <<'PY'
import sys, json
import pyarrow.parquet as pq
d = sys.argv[1]

def chunk_codecs(path):
    m = pq.ParquetFile(path).metadata
    return {m.row_group(r).column(c).compression for r in range(m.num_row_groups) for c in range(m.num_columns)}

def reported(name):
    return json.loads(open(f"{d}/{name}.json").read().strip().splitlines()[-1])["codec"]

cases = [  # output, source, expected chunk codec (pyarrow's name), reported codec
    ("to_zstd", "in", "ZSTD", "ZSTD"),
    ("proj_gzip", "in", "GZIP", "GZIP"),
    ("pages_zstd", "pages", "ZSTD", "ZSTD"),
    ("pages_raw", "pages", "UNCOMPRESSED", "UNCOMPRESSED"),
    ("kept", "zstd_src", "ZSTD", "ZSTD"),
]
for out, src, want, want_reported in cases:
    got = chunk_codecs(f"{d}/{out}.parquet")
    assert got == {want}, f"{out}: chunks written as {got}, want {want}"
    assert reported(out) == want_reported, f"{out}: reported {reported(out)}, want {want_reported}"
    o = pq.read_table(f"{d}/{out}.parquet")
    s = pq.read_table(f"{d}/{src}.parquet", columns=o.column_names)
    assert o.equals(s), f"{out}: values differ from {src}"

# Inputs in two codecs copied without --codec keep both, and the report says so.
assert chunk_codecs(f"{d}/mixed.parquet") == {"SNAPPY", "ZSTD"}, chunk_codecs(f"{d}/mixed.parquet")
assert reported("mixed") == "MIXED", f"mixed: reported {reported('mixed')}"
assert chunk_codecs(f"{d}/unmixed.parquet") == {"GZIP"} and reported("unmixed") == "GZIP", "unmixed"

m = pq.ParquetFile(f"{d}/pages_zstd.parquet").metadata
assert all(m.row_group(r).column(c).has_offset_index for r in range(m.num_row_groups) for c in range(m.num_columns)), \
    "recompressed chunks lost their page index"
# A matching codec keeps the verbatim copy: the bytes are those of a plain copy.
assert open(f"{d}/same_snappy.parquet", "rb").read() == open(f"{d}/no_codec.parquet", "rb").read(), \
    "--codec snappy on snappy chunks did not take the byte-copy path"
# ZPQ reads its recompressed output, page index included, to the same answer.
for src, out in (("in", "to_zstd"), ("pages", "pages_zstd")):
    a = json.loads(open(f"{d}/agg_{src}.json").read().strip().splitlines()[-1])["agg"]
    b = json.loads(open(f"{d}/agg_{out}.json").read().strip().splitlines()[-1])["agg"]
    assert a == b, f"{out}: filtered aggregate {b} != source {a}"

print("All --codec copy checks passed")
PY

# The deprecated LZ4 codec (Hadoop-framed and bare-block) is readable, so
# --codec can recompress it; zpq never compresses to it (`lz4` is LZ4_RAW).
# Needs the parquet-testing corpus (`just fetch-corpus`); skipped without it.
CORPUS="${CORPUS:-data/parquet-testing/data}"
if [ -f "$CORPUS/hadoop_lz4_compressed_larger.parquet" ]; then
  for f in hadoop_lz4_compressed hadoop_lz4_compressed_larger non_hadoop_lz4_compressed; do
    for c in zstd lz4; do
      "$ZPQ" query "$CORPUS/$f.parquet" -o "$DIR/$f.$c.parquet" --codec "$c" 2> /dev/null
    done
  done
  "$ZPQ" query "$CORPUS/hadoop_lz4_compressed.parquet" -o "$DIR/lz4_filtered.parquet" \
    --filter 'c0 > 1593604800' 2> /dev/null
  "$PY" - "$DIR" "$CORPUS" <<'PY'
import sys
import pyarrow.compute as pc, pyarrow.parquet as pq
d, corpus = sys.argv[1], sys.argv[2]
for f in ("hadoop_lz4_compressed", "hadoop_lz4_compressed_larger", "non_hadoop_lz4_compressed"):
    src = pq.read_table(f"{corpus}/{f}.parquet")
    for c, want in (("zstd", "ZSTD"), ("lz4", "LZ4")):  # pyarrow names LZ4_RAW "LZ4"
        out = f"{d}/{f}.{c}.parquet"
        m = pq.ParquetFile(out).metadata
        got = {m.row_group(r).column(i).compression for r in range(m.num_row_groups) for i in range(m.num_columns)}
        assert got == {want}, f"{f} --codec {c}: chunks written as {got}"
        assert pq.read_table(out).to_pydict() == src.to_pydict(), f"{f} --codec {c}: values differ"
src = pq.read_table(f"{corpus}/hadoop_lz4_compressed.parquet")
want = src.filter(pc.greater(src["c0"], 1593604800)).to_pydict()
assert pq.read_table(f"{d}/lz4_filtered.parquet").to_pydict() == want, "filtered LZ4 re-encode differs"
print("Legacy LZ4 recompression checks passed")
PY
else
  echo "Legacy LZ4 recompression checks skipped: $CORPUS not present"
fi
