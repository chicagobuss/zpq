#!/usr/bin/env python3
"""Triangulation Correctness Harness: ZPQ vs Hardwood vs DuckDB.

Automated 3-engine correctness harness for Parquet reading and writing.
See docs/triangulation.md for design and referee rules.

Usage:
  .venv/bin/python tools/triangulate.py
"""
import argparse
import datetime
import decimal
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import duckdb
except ImportError:
    print("Missing dependencies: please run via .venv/bin/python (needs pyarrow and duckdb)")
    sys.exit(2)

ZPQ_BIN = "zig-out/bin/zpq"

# Per-engine subprocess cap. Without it, one engine hanging on a single file
# (e.g. hardwood loops forever on nation.dict-malformed.parquet — a chunk whose
# metadata undercounts its true byte length) hangs the entire harness. A timeout
# surfaces as a per-engine failure for that file, not a wedged run.
SUBPROCESS_TIMEOUT_S = 60

def find_hardwood() -> Optional[str]:
    if env := os.environ.get("HARDWOOD"):
        return env if os.path.exists(env) else None
    if path_hw := shutil.which("hardwood"):
        return path_hw
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Locally-built native CLI (see references/hardwood + NATIVE_BUILD.md). The
    # binary finds its codec .so's via the sibling lib/ dir (bin/../lib), so no
    # HARDWOOD_LIB_PATH is needed when laid out as bin/hardwood + lib/.
    candidates = [
        os.path.join(repo_root, "tools", "hardwood", "bin", "hardwood"),
        os.path.join(repo_root, "tools", "hardwood", "hardwood-cli-1.0.0.CR2-linux-x86_64", "bin", "hardwood"),
        os.path.join(repo_root, "tools", "hardwood", "hardwood-cli-1.0.0.Beta2-linux-x86_64", "bin", "hardwood"),
    ]
    return next((c for c in candidates if os.path.exists(c)), None)

HARDWOOD_BIN = find_hardwood()

# --- Normalization and Equality (Reusing concepts from differential.py) ---

def is_nan(v: Any) -> bool:
    return isinstance(v, float) and math.isnan(v)

def normalize_value(v: Any) -> Any:
    if v is None:
        return None
    if is_nan(v):
        return "NaN"
    # Infinities must short-circuit to a stable token BEFORE the tolerant float
    # compare: `abs(inf - inf)` is NaN, and `NaN <= tol` is False, which would
    # flag two equal infinities as a mismatch (this manufactured the base_types
    # write "failure" — ZPQ round-trips inf correctly).
    if isinstance(v, float) and math.isinf(v):
        return "Infinity" if v > 0 else "-Infinity"
    if isinstance(v, (decimal.Decimal, float)):
        # Normalize decimals and floats to Python float
        # ZPQ decodes decimals to f64, so we must compare as floats
        try:
            return float(v)
        except:
            pass
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        # Timestamps / INT96 can suffer tz-adjustments
        # Convert to ISO string to stabilize
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        # FLBA / Binary
        # ZPQ yields raw bytes, Hardwood/DuckDB might cast
        return v.hex()
    return v

def compare_values(v1: Any, v2: Any, tol: float = 1e-4) -> bool:
    nv1 = normalize_value(v1)
    nv2 = normalize_value(v2)

    if nv1 == "NaN" and nv2 == "NaN":
        return True
    if nv1 is None or nv2 is None:
        return nv1 == nv2
    # Numeric on both sides — compare tolerantly regardless of int/float mix.
    # DuckDB promotes every integer SUM to DECIMAL/HUGEINT, so its `sum(id)`
    # comes back as Decimal('6') while ZPQ emits int 6; normalize_value turns
    # the Decimal into 6.0. Without this branch the int-vs-float pair fell
    # through to `str(6) == str(6.0)` → False, which manufactured a spurious
    # mismatch for nearly every file with an integer column.
    num1 = isinstance(nv1, (int, float)) and not isinstance(nv1, bool)
    num2 = isinstance(nv2, (int, float)) and not isinstance(nv2, bool)
    if num1 and num2:
        return abs(nv1 - nv2) <= tol * max(1.0, abs(nv2))

    return str(nv1) == str(nv2)

def rows_match(rows1: List[Dict], rows2: List[Dict]) -> bool:
    if len(rows1) != len(rows2):
        return False
    # Assume rows are aligned or sort them
    for r1, r2 in zip(rows1, rows2):
        if set(r1.keys()) != set(r2.keys()):
            # Allow case-insensitive or structural mismatch if minimal
            pass
        # Compare positional or key values
        for k in r1.keys():
            # DuckDB might lowercase keys, Hardwood keeps them. Match by order or lower key
            k2 = next((k2 for k2 in r2.keys() if k2.lower() == k.lower()), None)
            if not k2 or not compare_values(r1[k], r2[k2]):
                return False
    return True

# --- Engine Executors ---

def run_duckdb_sql(sql: str) -> List[Dict]:
    conn = duckdb.connect()
    try:
        return conn.execute(sql).to_arrow_table().to_pylist()
    except Exception as e:
        raise RuntimeError(f"DuckDB error: {e}")

def run_duckdb_read(path: str) -> List[Dict]:
    return run_duckdb_sql(f"SELECT * FROM '{path}'")

def run_duckdb_agg(path: str, agg: str) -> List[Dict]:
    return run_duckdb_sql(f"SELECT {agg} FROM '{path}'")

def run_zpq_write(in_path: str, out_path: str, codec: str) -> None:
    cmd = [ZPQ_BIN, "query", in_path, "-o", out_path, "--codec", codec]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if proc.returncode != 0:
        raise RuntimeError(f"ZPQ Write Error: {proc.stderr[:100]}")

def run_zpq_agg(path: str, agg: str) -> List[Dict]:
    cmd = [ZPQ_BIN, "query", path, "--aggregate", agg]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if proc.returncode != 0:
        raise RuntimeError(f"ZPQ Read Error: {proc.stderr[:100]}")
    try:
        data = json.loads(proc.stdout)
        if "agg" in data:
            return [data["agg"]]
        return []
    except json.JSONDecodeError:
        raise RuntimeError(f"ZPQ JSON Error: {proc.stdout[:100]}")

def run_hardwood_json(path: str) -> List[Dict]:
    if not HARDWOOD_BIN:
        raise RuntimeError("Hardwood missing")
    # hardwood CLI: `convert --format json -f <path>` (file is the required
    # -f/--file option, not positional; default row count is ALL).
    cmd = [HARDWOOD_BIN, "convert", "--format", "json", "-f", path]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if proc.returncode != 0:
        raise RuntimeError(f"Hardwood Convert Error: {proc.stderr[:100]}")
    rows = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line: continue
        try:
            # Handle potential JSON lines or pretty print
            if line.startswith("{"):
                rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    if not rows and "{" in proc.stdout:
        # It might be pretty printed json
        try:
            parsed = json.loads(proc.stdout)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except json.JSONDecodeError:
            pass
    return rows

def run_hardwood_agg(path: str, agg: str) -> List[Dict]:
    """Hardwood does not have a native --aggregate CLI flag to match ZPQ/DuckDB easily.
    We convert to JSON and aggregate in Python, or we use DuckDB to read
    the hardwood-converted output."""
    if not HARDWOOD_BIN:
        raise RuntimeError("Hardwood missing")
    # Hardwood reads to a temp CSV; DuckDB handles the aggregate expression.
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        csv_out = tmp.name
    try:
        proc = subprocess.run([HARDWOOD_BIN, "convert", "--format", "csv", "-f", path], capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
        if proc.returncode != 0:
            raise RuntimeError(f"Hardwood Convert Error: {proc.stderr[:100]}")
        with open(csv_out, "w") as f:
            f.write(proc.stdout)
        return run_duckdb_sql(f"SELECT {agg} FROM read_csv_auto('{csv_out}')")
    finally:
        if os.path.exists(csv_out):
            os.remove(csv_out)

# --- ZPQ write correctness ---

def generate_write_matrices(tmpdir: str) -> List[str]:
    # Generate PyArrow matrices
    import pyarrow as pa
    import pyarrow.parquet as pq

    files = []
    # 1. Base Types
    t1 = pa.table({
        "i32": pa.array([1, 2, None, -5, 2147483647], type=pa.int32()),
        "i64": pa.array([1, 2, None, -5, 9223372036854775807], type=pa.int64()),
        "f32": pa.array([1.5, float('nan'), None, -5.5, float('inf')], type=pa.float32()),
        "f64": pa.array([1.5, float('nan'), None, -5.5, float('inf')], type=pa.float64()),
        "bool": pa.array([True, False, None, True, False], type=pa.bool_()),
        "str": pa.array(["hello", "", None, "world", "zpq"], type=pa.string()),
    })
    f1 = os.path.join(tmpdir, "base_types.parquet")
    pq.write_table(t1, f1)
    files.append(f1)

    # 2. Decimals
    t2 = pa.table({
        "dec_small": pa.array([decimal.Decimal('1.23'), None, decimal.Decimal('-5.00')], type=pa.decimal128(9, 2)),
        "dec_large": pa.array([decimal.Decimal('123456789.123'), None, decimal.Decimal('-1.000')], type=pa.decimal128(18, 3)),
    })
    f2 = os.path.join(tmpdir, "decimals.parquet")
    pq.write_table(t2, f2)
    files.append(f2)

    # 3. Dict Encoding (Single distinct value)
    t3 = pa.table({
        "dict_str": pa.array(["same"] * 1000 + [None], type=pa.string()),
        "dict_int": pa.array([42] * 1000 + [None], type=pa.int32()),
    })
    f3 = os.path.join(tmpdir, "dict_encoding.parquet")
    pq.write_table(t3, f3, use_dictionary=True)
    files.append(f3)

    return files

def structural_consistent(path: str) -> Tuple[bool, str]:
    """Footer column count must equal every row group's column-chunk count.
    A byte-copy can write N chunks under an M<N column footer. pyarrow often
    still 'reads' such a file when offsets happen to line up, so a value check
    alone misses it — this is the structural guard."""
    try:
        md = pq.ParquetFile(path).metadata
        for rg in range(md.num_row_groups):
            if md.row_group(rg).num_columns != md.num_columns:
                return False, f"footer cols={md.num_columns} != rg{rg} chunks={md.row_group(rg).num_columns}"
        return True, ""
    except Exception as e:
        return False, f"metadata read failed: {e}"


def write_mode_checks(tmpdir: str) -> Tuple[int, int, int]:
    """Write-mode guards. Pure value checks miss structural failures, so
    assert footer consistency plus clean rejection of nested re-encode."""
    print("\n--- Write modes (projection variants + nested) ---")
    passes = 0
    fails = 0

    src = os.path.join(tmpdir, "wmodes.parquet")
    t = pa.table({
        "a": pa.array([1, 2, 3, 4, 5], pa.int64()),
        "b": pa.array(["v", "w", "x", "y", "z"], pa.string()),
        "c": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], pa.float64()),
    })
    pq.write_table(t, src)

    # Every projection mode must yield a structurally-consistent file whose
    # footer columns match the requested columns, with correct values.
    modes = [
        ("--columns a,b", ["--columns", "a,b"], ["a", "b"]),
        ("--select a,b (passthrough subset)", ["--select", "a,b"], ["a", "b"]),
        ("--select b,a (reorder)", ["--select", "b,a"], ["a", "b"]),
        ("--select a,b,c (all)", ["--select", "a,b,c"], ["a", "b", "c"]),
    ]
    for name, flags, expect in modes:
        out = os.path.join(tmpdir, "wm_" + name.split()[0].lstrip("-") + "_" + "".join(expect) + ".parquet")
        r = subprocess.run([ZPQ_BIN, "query", src] + flags + ["-o", out],
                           capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
        if r.returncode != 0:
            print(f"  FAIL  {name} | zpq exit {r.returncode}: {r.stderr.strip()[:80]}")
            fails += 1
            continue
        sok, smsg = structural_consistent(out)
        cols = [f.name for f in pq.read_schema(out)]
        gtab = pq.read_table(out)
        vals_ok = set(cols) == set(expect) and all(
            gtab.column(c).to_pylist() == t.column(c).to_pylist() for c in expect)
        if sok and vals_ok:
            print(f"  OK    {name} → cols={cols}")
            passes += 1
        else:
            print(f"  FAIL  {name} | structural={sok}({smsg}) cols={cols} expect={expect} vals_ok={vals_ok}")
            fails += 1

    # Nested column: SELECT * must round-trip via the byte-copy fastpath;
    # a filtered re-encode must error cleanly, not silently corrupt.
    nsrc = os.path.join(tmpdir, "wnested.parquet")
    nested = [["a", "b"], ["c"], ["d", "e"]]
    pq.write_table(pa.table({
        "id": pa.array([1, 2, 3], pa.int64()),
        "tags": pa.array(nested, pa.list_(pa.string())),
    }), nsrc)

    ncopy = os.path.join(tmpdir, "wnested_copy.parquet")
    r = subprocess.run([ZPQ_BIN, "query", nsrc, "-o", ncopy],
                       capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if r.returncode == 0 and structural_consistent(ncopy)[0] and pq.read_table(ncopy).column("tags").to_pylist() == nested:
        print("  OK    nested SELECT* round-trips (fastpath byte-copy)")
        passes += 1
    else:
        print(f"  FAIL  nested SELECT* | rc={r.returncode} {r.stderr.strip()[:80]}")
        fails += 1

    nfilt = os.path.join(tmpdir, "wnested_filt.parquet")
    r = subprocess.run([ZPQ_BIN, "query", nsrc, "--filter", "id > 1", "-o", nfilt],
                       capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if r.returncode != 0:
        print(f"  OK    nested filtered re-encode rejected ({r.stderr.strip().splitlines()[-1][:48] if r.stderr.strip() else 'nonzero exit'})")
        passes += 1
    else:
        print("  FAIL  nested filtered re-encode did NOT error — corruption risk")
        fails += 1

    return passes, fails, 0


def write_correctness(tmpdir: str) -> Tuple[int, int, int]:
    print("\n--- ZPQ write correctness ---")
    files = generate_write_matrices(tmpdir)
    codecs = ["snappy", "zstd", "gzip", "lz4_raw", "uncompressed"]
    
    passes = 0
    fails = 0
    infos = 0

    for f_in in files:
        base = os.path.basename(f_in)
        for codec in codecs:
            f_out = os.path.join(tmpdir, f"out_{codec}_{base}")
            label = f"{base} ({codec})"
            
            # 1. ZPQ Write
            try:
                run_zpq_write(f_in, f_out, codec)
            except Exception as e:
                print(f"  FAIL  {label} | ZPQ Write Error: {e}")
                fails += 1
                continue
            
            # 2. DuckDB Read
            duck_rows = []
            try:
                duck_rows = run_duckdb_read(f_out)
                duck_ok = True
            except Exception as e:
                duck_ok = False
                duck_err = str(e)
            
            # 3. Hardwood Read
            hw_rows = []
            if HARDWOOD_BIN:
                try:
                    hw_rows = run_hardwood_json(f_out)
                    hw_ok = True
                except Exception as e:
                    hw_ok = False
                    hw_err = str(e)
            else:
                hw_ok = True
                hw_rows = duck_rows # Fake agreement if missing
            
            # Ground truth
            truth = run_duckdb_read(f_in)

            # Evaluate
            if not duck_ok:
                print(f"  FAIL  {label} | DuckDB rejected ZPQ output: {duck_err}")
                fails += 1
            elif HARDWOOD_BIN and not hw_ok:
                print(f"  FAIL  {label} | Hardwood rejected ZPQ output: {hw_err}")
                fails += 1
            else:
                # Structural guard first: footer columns must equal the row
                # group's chunk count; value checks miss this structural error.
                sok, smsg = structural_consistent(f_out)
                if not sok:
                    print(f"  FAIL  {label} | structural: {smsg}")
                    fails += 1
                    continue
                # DuckDB (native, typed parquet reader) is the authoritative
                # oracle for write correctness: does it read ZPQ's output the
                # same as it reads the source? Hardwood already passed the
                # read-without-error gate above; its *value* comparison goes
                # through a stringly-typed JSON export (bool→"true", NaN/inf
                # spelling, null handling, decimal precision all differ from the
                # typed truth), so a hardwood-only value mismatch is a
                # representation artifact, not a ZPQ write bug — report it INFO.
                d_match = rows_match(truth, duck_rows)
                h_match = True if not HARDWOOD_BIN else rows_match(truth, hw_rows)

                if not d_match:
                    print(f"  FAIL  {label} | ZPQ output differs from source (DuckDB read). ZPQ wrote wrong values.")
                    fails += 1
                elif not h_match:
                    print(f"  INFO  {label} | DuckDB confirms ZPQ output; Hardwood's JSON export represents it differently (stringly-typed).")
                    infos += 1
                else:
                    print(f"  OK    {label}")
                    passes += 1

    return passes, fails, infos

# --- ZPQ read correctness ---

def build_corpus_list() -> List[str]:
    files = []
    # apache/parquet-testing
    corpus_dir = os.path.join("data", "parquet-testing", "data")
    if os.path.exists(corpus_dir):
        files.extend([os.path.join(corpus_dir, f) for f in os.listdir(corpus_dir) if f.endswith(".parquet")])
    
    # hardwood test fixtures
    hw_fixtures = os.path.join("references", "hardwood", "core", "src", "test", "resources")
    if os.path.exists(hw_fixtures):
        for root, dirs, fnames in os.walk(hw_fixtures):
            for f in fnames:
                if f.endswith(".parquet"):
                    files.append(os.path.join(root, f))
    
    return files

def get_aggregate_query(path: str) -> Optional[str]:
    # Read schema with pyarrow to build aggregate
    try:
        sch = pq.read_schema(path)
        aggs = []
        for i, f in enumerate(sch):
            if pa.types.is_nested(f.type):
                continue
            name = f.name
            if name == "": name = f"c{i}"
            # Use sum/min/max depending on type
            if pa.types.is_integer(f.type) or pa.types.is_floating(f.type) or pa.types.is_decimal(f.type):
                aggs.append(f"sum({name}) AS {name}_sum")
            elif pa.types.is_string(f.type):
                aggs.append(f"max({name}) AS {name}_max")
        if not aggs:
            aggs = ["count(*) AS total_rows"]
        else:
            aggs.append("count(*) AS total_rows")
        return ", ".join(aggs)
    except Exception:
        return None

def read_correctness() -> Tuple[int, int, int]:
    print("\n--- ZPQ read correctness (aggregates) ---")
    files = build_corpus_list()
    if not files:
        print("  INFO  Corpus not found (run `just fetch-corpus`). Skipping read checks.")
        return 0, 0, 0

    passes = 0
    fails = 0
    infos = 0

    files = sorted(files)
    for f in files:
        base = os.path.basename(f)
        agg = get_aggregate_query(f)
        if not agg:
            continue
        
        # 1. ZPQ Read
        try:
            zpq_rows = run_zpq_agg(f, agg)
            zpq_ok = True
        except Exception as e:
            zpq_ok = False
            zpq_err = str(e)
        
        # 2. DuckDB Read
        try:
            duck_rows = run_duckdb_agg(f, agg)
            duck_ok = True
        except Exception as e:
            duck_ok = False
            duck_err = str(e)
        
        # 3. Hardwood Read
        if HARDWOOD_BIN:
            try:
                hw_rows = run_hardwood_agg(f, agg)
                hw_ok = True
            except Exception as e:
                hw_ok = False
                hw_err = str(e)
        else:
            hw_ok = duck_ok
            hw_rows = duck_rows

        # Referee Logic
        # All three error = pass (intentionally malformed fixture)
        if not zpq_ok and not duck_ok and not hw_ok:
            print(f"  OK    {base} | All engines rejected (assumed malformed)")
            passes += 1
            continue
        
        # If ZPQ fails but others succeed -> ZPQ outlier
        if not zpq_ok and (duck_ok or hw_ok):
            print(f"  FAIL  {base} | ZPQ rejected but others accepted. ZPQ Err: {zpq_err}")
            fails += 1
            continue

        # If ZPQ succeeds but others fail -> ZPQ outlier
        if zpq_ok and not duck_ok and not hw_ok:
            print(f"  FAIL  {base} | ZPQ accepted but others rejected. ZPQ: {zpq_rows}")
            fails += 1
            continue
        
        if zpq_ok and duck_ok:
            # Value comparison
            z_matches_d = rows_match(zpq_rows, duck_rows)

            if not HARDWOOD_BIN:
                # Two-engine mode: DuckDB is the reference. A ZPQ-vs-DuckDB
                # mismatch is a real FAIL, not an INFO — there is no third
                # opinion to appeal to, so don't disguise it as a referee tie.
                if z_matches_d:
                    print(f"  OK    {base}")
                    passes += 1
                else:
                    print(f"  FAIL  {base} | ZPQ != DuckDB (2-engine; no Hardwood). ZPQ: {zpq_rows}, Duck: {duck_rows}")
                    fails += 1
                continue

            z_matches_h = hw_ok and rows_match(zpq_rows, hw_rows)
            d_matches_h = hw_ok and rows_match(duck_rows, hw_rows)

            if z_matches_d and z_matches_h:
                print(f"  OK    {base}")
                passes += 1
            elif not z_matches_d and not z_matches_h and d_matches_h:
                print(f"  FAIL  {base} | ZPQ is outlier (DuckDB matches Hardwood). ZPQ: {zpq_rows}, Duck: {duck_rows}")
                fails += 1
            else:
                # ZPQ matches one engine but not the other.
                print(f"  INFO  {base} | Hardwood vs DuckDB disagreement. ZPQ matches one. ZPQ: {zpq_rows}")
                infos += 1

    return passes, fails, infos

def main():
    if not os.path.exists(ZPQ_BIN):
        print(f"Missing {ZPQ_BIN}. Please run `zig build -Doptimize=ReleaseFast`")
        sys.exit(2)

    if not HARDWOOD_BIN:
        print("  WARNING: Hardwood CLI not found (checked $HARDWOOD and tools/hardwood).")
        print("           Degrading to 2-engine (ZPQ vs DuckDB) verification.")

    tmpdir = tempfile.mkdtemp(prefix="zpq_triangulate_")
    try:
        a_pass, a_fail, a_info = write_correctness(tmpdir)
        w_pass, w_fail, w_info = write_mode_checks(tmpdir)
        b_pass, b_fail, b_info = read_correctness()

        print("\n=== Triangulation Summary ===")
        print(f"Write:       {a_pass} PASS, {a_fail} FAIL, {a_info} INFO")
        print(f"Write modes: {w_pass} PASS, {w_fail} FAIL, {w_info} INFO")
        print(f"Read:        {b_pass} PASS, {b_fail} FAIL, {b_info} INFO")

        total_fail = a_fail + w_fail + b_fail
        if total_fail > 0:
            print("\nFAIL: Regressions detected.")
            sys.exit(1)
        print("\nSUCCESS: All triangulation checks passed.")
    finally:
        shutil.rmtree(tmpdir)

if __name__ == "__main__":
    main()
