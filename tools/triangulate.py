#!/usr/bin/env python3
"""Triangulation Correctness Harness: ZPQ vs Hardwood vs DuckDB.

Automated 3-engine correctness harness for Parquet reading and writing.
See docs/triangulation.md for design and referee rules.

Usage:
  .venv/bin/python tools/triangulate.py [--hardwood-fixtures DIR] [--only SUBSTR]
"""
import argparse
import datetime
import decimal
import json
import math
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    import duckdb
except ImportError:
    print("Missing dependencies: please run via .venv/bin/python (needs pyarrow and duckdb)")
    sys.exit(2)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ZPQ_BIN = "zig-out/bin/zpq"
HARDWOOD_HOME = os.path.join(REPO_ROOT, "tools", "hardwood")  # populated by tools/fetch_hardwood.sh

# Per-engine subprocess cap. Malformed corpus files (e.g. nation.dict-malformed.parquet,
# a chunk whose metadata undercounts its true byte length) can keep any reader busy,
# and without a cap one such run would hold up the entire harness. A timeout
# surfaces as a per-engine failure for that file, not a wedged run.
SUBPROCESS_TIMEOUT_S = 60

# --- Referee overrides -------------------------------------------------------
# The 2-of-3 vote accepts a wrong answer when zpq and DuckDB share a bug. Entries here take a file out of the vote.
# Keys are corpus labels as printed by the harness ("parquet-testing:<file>" or "hardwood:<path under resources>").
#   "authority": "hardwood"  — strict-spec case: Hardwood alone decides (both its answers and its rejections); DuckDB
#                              is still run and its divergence shown, but it gets no vote.
#   "expected": {alias: v}   — the hand-verified answer for that result column; beats every engine. Use when no
#                              engine can be trusted or Hardwood is absent. `None` means SQL NULL.
#   "why": "..."             — required; the spec rule or upstream issue that justifies the override.
# Keep entries narrow and justified; an override silently turns off a referee for that file.
REFEREE_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "hardwood:compat_chunk_path_swapped.parquet": {
        "authority": "hardwood",
        "why": "chunk path_in_schema contradicts the schema's leaf order; a reader must reject, not guess",
    },
}


def find_hardwood() -> Optional[str]:
    if env := os.environ.get("HARDWOOD"):
        if not os.path.exists(env):
            # An explicit pointer that is wrong must not quietly degrade to 2-engine mode.
            print(f"HARDWOOD={env} does not exist.")
            sys.exit(2)
        return env
    local = os.path.join(HARDWOOD_HOME, "bin", "hardwood")
    if os.path.exists(local):
        return local
    return shutil.which("hardwood")


HARDWOOD_BIN = find_hardwood()


def hardwood_version() -> str:
    try:
        p = subprocess.run([HARDWOOD_BIN, "--version"], capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
        return (p.stdout.strip().splitlines() or ["?"])[-1]
    except Exception as e:
        return f"unknown ({e})"


# --- Normalization and Equality ---------------------------------------------
# Typed values come from DuckDB (via Arrow) and ZPQ's JSON; Hardwood 1.1's JSON export is typed for numbers/bools and
# renders everything else as text: decimals as plain strings, NaN/±Infinity as strings, temporals as ISO-8601 (INT96
# and UTC-adjusted timestamps with a trailing Z), UUIDs canonically, binary as UTF-8 text or 0x-hex, nested values as
# native JSON. When exactly one side of a pair is a string, it is coerced toward the other side's type.

def is_nan(v: Any) -> bool:
    return isinstance(v, float) and math.isnan(v)


def hardwood_binary_text(b: bytes) -> str:
    """Mirror of Hardwood's BinaryValues.render: strict UTF-8 without ISO control chars and not starting with "0x" is
    text; anything else is 0x-prefixed lowercase hex."""
    if not b:
        return ""
    try:
        s = b.decode("utf-8")
        if not any(ord(c) < 0x20 or 0x7F <= ord(c) <= 0x9F for c in s) and not s.startswith("0x"):
            return s
    except UnicodeDecodeError:
        pass
    return "0x" + b.hex()


_ISO_TS = re.compile(r"^(-?\d{4,}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})?$")
_ISO_TIME = re.compile(r"^(\d{2}:\d{2}(?::\d{2})?)(?:\.(\d+))?$")


def _canon_fraction(frac: Optional[str]) -> str:
    frac = (frac or "").rstrip("0")
    return "." + frac if frac else ""


def canon_temporal_str(s: str) -> Optional[str]:
    """Canonical text for an ISO timestamp/time string: 'T' separator, seconds present, fraction without trailing
    zeros, UTC offset dropped (every engine here reports instants in UTC)."""
    m = _ISO_TS.match(s)
    if m:
        date, hms, frac, tz = m.groups()
        if tz and tz not in ("Z", "+00:00", "+0000", "-00:00"):
            return None
        if len(hms) == 5:
            hms += ":00"
        return f"{date}T{hms}{_canon_fraction(frac)}"
    m = _ISO_TIME.match(s)
    if m:
        hms, frac = m.groups()
        if len(hms) == 5:
            hms += ":00"
        return f"{hms}{_canon_fraction(frac)}"
    return None


def canon_temporal(v: Any) -> Any:
    if isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return canon_temporal_str(v.isoformat())
    if isinstance(v, datetime.time):
        return canon_temporal_str(v.replace(tzinfo=None).isoformat())
    if isinstance(v, datetime.date):
        return v.isoformat()
    return v


def coerce_like(ref: Any, v: Any) -> Any:
    """Coerce Hardwood's textual rendering `v` toward the type of the typed value `ref`."""
    if not isinstance(v, str) or isinstance(ref, str) or ref is None:
        return v
    try:
        if isinstance(ref, bool):
            return {"true": True, "false": False}.get(v, v)
        if isinstance(ref, (int, float, decimal.Decimal)):
            return decimal.Decimal(v) if v not in ("NaN", "Infinity", "-Infinity") else float(v)
    except decimal.InvalidOperation:
        return v
    if isinstance(ref, (datetime.date, datetime.time)):
        return canon_temporal_str(v) or v
    if isinstance(ref, (bytes, bytearray)):
        return v  # bytes side is rendered to the same text in normalize_value
    if isinstance(ref, uuid.UUID):
        return v.lower()
    return v


def normalize_value(v: Any) -> Any:
    if v is None:
        return None
    if is_nan(v):
        return "NaN"
    # Infinities must short-circuit to a stable token BEFORE the tolerant float
    # compare: `abs(inf - inf)` is NaN, and `NaN <= tol` is False, which would
    # flag two equal infinities as a mismatch.
    if isinstance(v, float) and math.isinf(v):
        return "Infinity" if v > 0 else "-Infinity"
    if isinstance(v, decimal.Decimal):
        if v.is_nan():
            return "NaN"
        if v.is_infinite():
            return "Infinity" if v > 0 else "-Infinity"
        # ZPQ decodes decimals to f64, so decimals compare as floats.
        return float(v)
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return canon_temporal(v)
    if isinstance(v, (bytes, bytearray)):
        return hardwood_binary_text(bytes(v))
    if isinstance(v, uuid.UUID):
        return str(v)
    return v


def compare_values(v1: Any, v2: Any, tol: float = 1e-4) -> bool:
    if isinstance(v1, dict) and isinstance(v2, dict):
        return v1.keys() == v2.keys() and all(compare_values(v1[k], v2[k], tol) for k in v1)
    if isinstance(v1, list) and isinstance(v2, list):
        return len(v1) == len(v2) and all(compare_values(a, b, tol) for a, b in zip(v1, v2))
    # Arrow hands maps over as [(k, v), ...]; Hardwood as a JSON object.
    if isinstance(v1, list) and isinstance(v2, dict):
        v1, v2 = v2, v1
    if isinstance(v1, dict) and isinstance(v2, list) and all(isinstance(e, tuple) and len(e) == 2 for e in v2):
        return len(v1) == len(v2) and all(
            any(str(normalize_value(k)) == hk and compare_values(hv, val, tol) for hk, hv in v1.items())
            for k, val in v2)

    v2 = coerce_like(v1, v2)
    v1 = coerce_like(v2, v1)
    nv1 = normalize_value(v1)
    nv2 = normalize_value(v2)

    if nv1 is None or nv2 is None:
        return nv1 == nv2
    # Numeric on both sides — compare tolerantly regardless of int/float mix.
    # DuckDB promotes every integer SUM to DECIMAL/HUGEINT, so its `sum(id)`
    # comes back as Decimal('6') while ZPQ emits int 6.
    num1 = isinstance(nv1, (int, float)) and not isinstance(nv1, bool)
    num2 = isinstance(nv2, (int, float)) and not isinstance(nv2, bool)
    if num1 and num2:
        if isinstance(nv1, int) and isinstance(nv2, int):
            return nv1 == nv2
        return abs(nv1 - nv2) <= tol * max(1.0, abs(nv2))
    return str(nv1) == str(nv2)


def _key_match(r: Dict, k: str) -> Optional[str]:
    if k in r:
        return k
    return next((k2 for k2 in r.keys() if k2.lower() == k.lower()), None)


def rows_match(rows1: List[Dict], rows2: List[Dict]) -> bool:
    if len(rows1) != len(rows2):
        return False
    for r1, r2 in zip(rows1, rows2):
        if len(r1) != len(r2):
            return False
        for k in r1.keys():
            k2 = _key_match(r2, k)
            if k2 is None or not compare_values(r1[k], r2[k2]):
                return False
    return True


# --- Engine Executors ---------------------------------------------------------

def _arrow_rows(tbl: "pa.Table") -> List[Dict]:
    # Python's datetime has no nanoseconds; render ns timestamps as text so to_pylist can't throw.
    for i, f in enumerate(tbl.schema):
        if pa.types.is_timestamp(f.type) and f.type.unit == "ns":
            tbl = tbl.set_column(i, f.name, pc.strftime(tbl.column(i), format="%Y-%m-%dT%H:%M:%S"))
    return tbl.to_pylist()


def run_duckdb_sql(sql: str) -> List[Dict]:
    conn = duckdb.connect()
    try:
        conn.execute("SET TimeZone='UTC'")
        return _arrow_rows(conn.execute(sql).to_arrow_table())
    except Exception as e:
        raise RuntimeError(f"DuckDB error: {e}")
    finally:
        conn.close()


def run_duckdb_read(path: str) -> List[Dict]:
    return run_duckdb_sql(f"SELECT * FROM read_parquet('{path}')")


def run_zpq_write(in_path: str, out_path: str, extra: List[str]) -> None:
    cmd = [ZPQ_BIN, "query", in_path, "-o", out_path] + extra
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if proc.returncode != 0:
        raise RuntimeError(f"ZPQ Write Error: {proc.stderr.strip()[:200]}")


def run_zpq_agg(path: str, agg: str, flt: Optional[str] = None) -> List[Dict]:
    cmd = [ZPQ_BIN, "query", path, "--aggregate", agg] + (["--filter", flt] if flt else [])
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if proc.returncode != 0:
        raise RuntimeError(f"ZPQ Read Error: {proc.stderr.strip()[:200]}")
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(f"ZPQ JSON Error: {proc.stdout[:200]}")
    return [data["agg"]] if "agg" in data else []


def _hardwood_error(stderr: str) -> str:
    # The native CLI logs a SIMD-availability INFO line to stderr on every run; the root cause is the last
    # "Caused by" (or, failing that, the last non-log line).
    lines = [ln.strip() for ln in stderr.splitlines() if ln.strip()]
    caused = [ln for ln in lines if ln.startswith("Caused by:")]
    if caused:
        return caused[-1][len("Caused by:"):].strip()[:200]
    useful = [ln for ln in lines if not ln.startswith(("INFO:", "WARNING:")) and "VectorSupport" not in ln
              and not ln.startswith("at ")]
    return (useful[-1] if useful else "no output")[:200]


def parse_hardwood_json(text: str) -> List[Dict]:
    """Hardwood prints one JSON array, one row object per line:
        [
          {...},
          {...}
        ]
    Parse the whole document; the line-wise fallback (for JSON-Lines or a truncated array) fails loudly on any line
    it cannot read rather than dropping rows."""
    text = text.strip()
    if not text:
        return []
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        rows = []
        for n, line in enumerate(text.splitlines(), 1):
            s = line.strip().rstrip(",")
            if s in ("", "[", "]"):
                continue
            try:
                rows.append(json.loads(s))
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Hardwood JSON unparseable at line {n}: {e}: {s[:80]}")
        return rows
    if isinstance(doc, list):
        return doc
    if isinstance(doc, dict):
        return [doc]
    raise RuntimeError(f"Hardwood JSON: unexpected top-level {type(doc).__name__}")


def run_hardwood_json(path: str, columns: Optional[List[str]] = None) -> List[Dict]:
    if not HARDWOOD_BIN:
        raise RuntimeError("Hardwood missing")
    # `-f` is a required option, not positional; the default row count is ALL.
    cmd = [HARDWOOD_BIN, "convert", "--format", "json", "-f", path]
    if columns:
        cmd += ["-c", ",".join(columns)]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if proc.returncode != 0:
        raise RuntimeError(f"Hardwood: {_hardwood_error(proc.stderr)}")
    return parse_hardwood_json(proc.stdout)


# --- ZPQ write correctness ------------------------------------------------------

@dataclass
class WriteCase:
    path: str
    reencode: bool = True  # False: zpq can only byte-copy this file (INT96 re-encode is unsupported)


def generate_write_matrices(tmpdir: str) -> List[WriteCase]:
    cases = []

    def put(name: str, tbl: "pa.Table", reencode: bool = True, **kw) -> None:
        p = os.path.join(tmpdir, name)
        pq.write_table(tbl, p, **kw)
        cases.append(WriteCase(p, reencode))

    put("base_types.parquet", pa.table({
        "i32": pa.array([1, 2, None, -5, 2147483647], type=pa.int32()),
        "i64": pa.array([1, 2, None, -5, 9223372036854775807], type=pa.int64()),
        "f32": pa.array([1.5, float('nan'), None, -5.5, float('inf')], type=pa.float32()),
        "f64": pa.array([1.5, float('nan'), None, -5.5, float('inf')], type=pa.float64()),
        "bool": pa.array([True, False, None, True, False], type=pa.bool_()),
        "str": pa.array(["hello", "", None, "world", "zpq"], type=pa.string()),
    }))
    put("decimals.parquet", pa.table({
        "dec_small": pa.array([decimal.Decimal('1.23'), None, decimal.Decimal('-5.00')], type=pa.decimal128(9, 2)),
        "dec_large": pa.array([decimal.Decimal('123456789.123'), None, decimal.Decimal('-1.000')],
                              type=pa.decimal128(18, 3)),
    }))
    put("dict_encoding.parquet", pa.table({
        "dict_str": pa.array(["same"] * 1000 + [None], type=pa.string()),
        "dict_int": pa.array([42] * 1000 + [None], type=pa.int32()),
    }), use_dictionary=True)
    ts = [datetime.datetime(2020, 1, 2, 3, 4, 5, 123456), None, datetime.datetime(1969, 12, 31, 23, 59, 59)]
    put("temporal.parquet", pa.table({
        "d": pa.array([datetime.date(2020, 1, 2), None, datetime.date(1969, 12, 31)], pa.date32()),
        "ts_ms": pa.array([t and t.replace(microsecond=123000) for t in ts], pa.timestamp("ms")),
        "ts_us_utc": pa.array(ts, pa.timestamp("us", tz="UTC")),
    }))
    put("int96.parquet", pa.table({"ts": pa.array(ts, pa.timestamp("us"))}), reencode=False,
        use_deprecated_int96_timestamps=True)
    return cases


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


# pyarrow names the modern LZ4_RAW codec "LZ4" (Arrow's raw-block LZ4); the deprecated Hadoop-framed codec is not
# something zpq writes.
PYARROW_CODEC = {"lz4_raw": "LZ4"}


def chunk_codecs(path: str) -> set:
    md = pq.ParquetFile(path).metadata
    return {md.row_group(r).column(c).compression for r in range(md.num_row_groups) for c in range(md.num_columns)}


def write_mode_checks(tmpdir: str) -> Tuple[int, int, int]:
    """Write-mode guards. Pure value checks miss structural failures, so
    assert footer consistency plus clean rejection of nested re-encode."""
    print("\n--- Write modes (projection variants + nested + codec) ---")
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

    # An unfiltered SELECT * takes the byte-copy fastpath; --codec must still be honored (or refused), never
    # reported as applied while the source codec is copied through.
    ccopy = os.path.join(tmpdir, "wm_codec_copy.parquet")
    r = subprocess.run([ZPQ_BIN, "query", src, "--codec", "zstd", "-o", ccopy],
                       capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if r.returncode != 0:
        print(f"  OK    SELECT* --codec zstd refused ({r.stderr.strip()[:60]})")
        passes += 1
    elif chunk_codecs(ccopy) == {"ZSTD"}:
        print("  OK    SELECT* --codec zstd honored")
        passes += 1
    else:
        print(f"  FAIL  SELECT* --codec zstd wrote chunks {sorted(chunk_codecs(ccopy))} (source codec copied through)")
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
    if (r.returncode == 0 and structural_consistent(ncopy)[0]
            and pq.read_table(ncopy).column("tags").to_pylist() == nested):
        print("  OK    nested SELECT* round-trips (fastpath byte-copy)")
        passes += 1
    else:
        print(f"  FAIL  nested SELECT* | rc={r.returncode} {r.stderr.strip()[:80]}")
        fails += 1

    nfilt = os.path.join(tmpdir, "wnested_filt.parquet")
    r = subprocess.run([ZPQ_BIN, "query", nsrc, "--filter", "id > 1", "-o", nfilt],
                       capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S)
    if r.returncode != 0:
        last = r.stderr.strip().splitlines()[-1][:48] if r.stderr.strip() else 'nonzero exit'
        print(f"  OK    nested filtered re-encode rejected ({last})")
        passes += 1
    else:
        print("  FAIL  nested filtered re-encode did NOT error — corruption risk")
        fails += 1

    return passes, fails, 0


def write_correctness(tmpdir: str) -> Tuple[int, int, int]:
    """Each matrix file goes through zpq twice: the unfiltered byte-copy path, and a `--select` of every column, which
    always re-encodes and so is the only way to exercise each output codec. DuckDB and Hardwood both read the result
    and must see the source's values."""
    print("\n--- ZPQ write correctness ---")
    codecs = ["snappy", "zstd", "gzip", "lz4_raw", "uncompressed"]
    passes = fails = infos = 0

    for case in generate_write_matrices(tmpdir):
        f_in = case.path
        base = os.path.basename(f_in)
        truth = run_duckdb_read(f_in)
        hw_src: Optional[List[Dict]] = None
        if HARDWOOD_BIN:
            try:
                hw_src = run_hardwood_json(f_in)
            except Exception as e:
                print(f"  INFO  {base} | Hardwood cannot read the generated source ({e}); judging on DuckDB only")
                infos += 1

        cols = ", ".join(f.name for f in pq.read_schema(f_in))
        variants = [("copy", None, [])]
        if case.reencode:
            variants += [(f"re-encode {c}", c, ["--select", cols, "--codec", c]) for c in codecs]
        for vname, codec, flags in variants:
            f_out = os.path.join(tmpdir, f"out_{vname.replace(' ', '_')}_{base}")
            label = f"{base} ({vname})"
            try:
                run_zpq_write(f_in, f_out, flags)
            except Exception as e:
                print(f"  FAIL  {label} | {e}")
                fails += 1
                continue

            sok, smsg = structural_consistent(f_out)
            if not sok:
                print(f"  FAIL  {label} | structural: {smsg}")
                fails += 1
                continue
            want = PYARROW_CODEC.get(codec, codec.upper()) if codec else None
            if want and chunk_codecs(f_out) != {want}:
                print(f"  FAIL  {label} | chunks are {sorted(chunk_codecs(f_out))}, requested {codec}")
                fails += 1
                continue
            try:
                duck_rows = run_duckdb_read(f_out)
            except Exception as e:
                print(f"  FAIL  {label} | DuckDB rejected ZPQ output: {e}")
                fails += 1
                continue
            if not rows_match(truth, duck_rows):
                print(f"  FAIL  {label} | DuckDB reads values that differ from the source. ZPQ wrote wrong values.")
                fails += 1
                continue
            if hw_src is None:
                print(f"  OK    {label}")
                passes += 1
                continue
            try:
                hw_rows = run_hardwood_json(f_out)
            except Exception as e:
                print(f"  FAIL  {label} | Hardwood rejected ZPQ output: {e}")
                fails += 1
                continue
            if rows_match(truth, hw_rows):
                print(f"  OK    {label}")
                passes += 1
            elif rows_match(hw_src, hw_rows):
                # Hardwood reads zpq's output exactly as it reads the source; only the text rendering differs from
                # DuckDB's typed values. That is a normalizer gap, not a write bug.
                print(f"  INFO  {label} | Hardwood reads output == source, but its rendering differs from DuckDB's")
                infos += 1
            else:
                print(f"  FAIL  {label} | Hardwood reads ZPQ output differently from the source")
                fails += 1

    return passes, fails, infos


# --- ZPQ read correctness -------------------------------------------------------

def build_corpus_list(hw_fixtures: Optional[str]) -> List[Tuple[str, str]]:
    """(label, path) pairs. Labels are stable keys for REFEREE_OVERRIDES."""
    files = []
    corpus_dir = os.path.join("data", "parquet-testing", "data")
    if os.path.isdir(corpus_dir):
        files += [(f"parquet-testing:{f}", os.path.join(corpus_dir, f))
                  for f in os.listdir(corpus_dir) if f.endswith(".parquet")]
    ci_dir = os.path.join("ci", "fixtures", "parquet")
    if os.path.isdir(ci_dir):
        files += [(f"zpq-ci:{f}", os.path.join(ci_dir, f)) for f in os.listdir(ci_dir) if f.endswith(".parquet")]
    if hw_fixtures:
        for root, _dirs, fnames in os.walk(hw_fixtures):
            for f in fnames:
                if f.endswith(".parquet"):
                    p = os.path.join(root, f)
                    files.append((f"hardwood:{os.path.relpath(p, hw_fixtures)}", p))
    return sorted(files)


def resolve_hw_fixtures(arg: Optional[str]) -> Optional[str]:
    """Accept either Hardwood's core/src/test/resources directory or the root of a Hardwood checkout."""
    cand = arg or os.environ.get("HARDWOOD_FIXTURES") or os.path.join(HARDWOOD_HOME, "fixtures")
    if not cand:
        return None
    nested = os.path.join(cand, "core", "src", "test", "resources")
    if os.path.isdir(nested):
        return nested
    if os.path.isdir(cand):
        return cand
    if arg or os.environ.get("HARDWOOD_FIXTURES"):
        print(f"Hardwood fixtures not found at {cand}")
        sys.exit(2)
    return None


@dataclass
class AggItem:
    func: str              # sum | min | max | count
    col: Optional[str]     # None for count(*)
    alias: str
    typ: Optional["pa.DataType"] = None


@dataclass
class Probe:
    name: str
    items: List[AggItem]
    zpq_filter: Optional[str] = None
    duck_where: Optional[str] = None
    pred: Any = None       # Python predicate over one Hardwood row, mirroring the filter
    columns: List[str] = field(default_factory=list)


_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _flat_type(t: "pa.DataType") -> "pa.DataType":
    return t.value_type if pa.types.is_dictionary(t) else t


def build_probes(path: str) -> List[Probe]:
    """An unfiltered aggregate probe (sum/min/max per numeric column, min/max per string column, count(*)) plus up to
    two filtered count probes whose literal sits mid-range, so row-group pruning on statistics actually decides
    something. The literal is chosen from pyarrow's reading of the file; pyarrow only picks it, it does not vote."""
    try:
        sch = pq.read_schema(path)
    except Exception:
        sch = None
    items: List[AggItem] = []
    numeric, strings = [], []
    if sch is not None:
        for f in sch:
            t = _flat_type(f.type)
            if pa.types.is_nested(t) or not _IDENT.match(f.name):
                continue
            if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t):
                items += [AggItem(fn, f.name, f"{f.name}_{fn}", t) for fn in ("sum", "min", "max")]
                if pa.types.is_integer(t):
                    numeric.append((f.name, t))
            elif pa.types.is_string(t) or pa.types.is_large_string(t):
                items += [AggItem(fn, f.name, f"{f.name}_{fn}", t) for fn in ("min", "max")]
                strings.append((f.name, t))
    items.append(AggItem("count", None, "total_rows"))
    cols = sorted({i.col for i in items if i.col})
    if not cols and sch is not None and len(sch) > 0:
        cols = [sch[0].name]  # count(*) alone still needs Hardwood to materialize some column
    probes = [Probe("agg", items, columns=cols)]

    for name, t in numeric[:1] + strings[:1]:
        try:
            vals = [v for v in pq.read_table(path, columns=[name]).column(0).to_pylist() if v is not None]
        except Exception:
            continue
        distinct = sorted(set(vals))
        if len(distinct) < 2:
            continue
        lit = distinct[len(distinct) // 2 - 1]
        if isinstance(lit, str):
            if "'" in lit or not lit.isprintable():
                continue
            sql_lit = f"'{lit}'"
        else:
            sql_lit = str(lit)
        probes.append(Probe(
            f"{name} > {sql_lit}", [AggItem("count", None, f"{name}_gt_count")],
            zpq_filter=f"{name} > {sql_lit}", duck_where=f'"{name}" > {sql_lit}',
            pred=lambda row, c=name, v=lit, typ=t: (x := coerce_scalar(row.get(c), typ)) is not None and x > v,
            columns=[name]))
    return probes


def coerce_scalar(v: Any, typ: Optional["pa.DataType"]) -> Any:
    if v is None or typ is None:
        return v
    if pa.types.is_integer(typ):
        return int(v)
    if pa.types.is_floating(typ):
        x = float(v)  # Hardwood spells NaN / ±Infinity as strings; float() accepts both spellings
        if typ == pa.float32() and math.isfinite(x):
            x = struct.unpack("f", struct.pack("f", x))[0]
        return x
    if pa.types.is_decimal(typ):
        return decimal.Decimal(str(v))
    return v


def _nan_ignoring(i: AggItem) -> bool:
    return i.func in ("min", "max") and i.typ is not None and pa.types.is_floating(i.typ)


def _sql_item(i: AggItem, duckdb_sql: bool) -> str:
    if i.func == "count":
        return f"count(*) AS {i.alias}"
    if not duckdb_sql:
        return f"{i.func}({i.col}) AS {i.alias}"
    # DuckDB orders NaN above every number; zpq's float min/max skip NaN (as Arrow compute and Polars do). The harness
    # judges the NaN-free extremes, so the order convention does not drown out real decode errors.
    nan_filter = f' FILTER (WHERE NOT isnan("{i.col}"))' if _nan_ignoring(i) else ""
    return f'{i.func}("{i.col}"){nan_filter} AS {i.alias}'


def eval_probe(probe: Probe, rows: List[Dict]) -> List[Dict]:
    """SQL aggregate semantics over Hardwood's rows: NULLs ignored, sum/min/max of no values is NULL, float min/max
    skip NaN (see _sql_item)."""
    if probe.pred:
        rows = [r for r in rows if probe.pred(r)]
    out: Dict[str, Any] = {}
    for it in probe.items:
        if it.func == "count":
            out[it.alias] = len(rows)
            continue
        if rows and it.col not in rows[0]:
            raise RuntimeError(f"Hardwood output lacks column {it.col}")
        vals = [x for x in (coerce_scalar(r.get(it.col), it.typ) for r in rows)
                if x is not None and not (_nan_ignoring(it) and is_nan(x))]
        if not vals:
            out[it.alias] = None
        elif it.func == "sum":
            if isinstance(vals[0], decimal.Decimal):
                with decimal.localcontext() as ctx:
                    ctx.prec = 80
                    out[it.alias] = sum(vals, decimal.Decimal(0))
            elif isinstance(vals[0], float):
                try:
                    out[it.alias] = math.fsum(vals)
                except (OverflowError, ValueError):
                    out[it.alias] = sum(vals)
            else:
                out[it.alias] = sum(vals)
        else:
            out[it.alias] = (min if it.func == "min" else max)(vals)
    return [out]


@dataclass
class EngineResult:
    ok: bool
    row: Dict[str, Any] = field(default_factory=dict)
    err: str = ""


def _one_row(rows: List[Dict]) -> Dict[str, Any]:
    return rows[0] if rows else {}


def _cell(row: Dict[str, Any], alias: str) -> Tuple[bool, Any]:
    k = _key_match(row, alias)
    return (k is not None, row.get(k) if k else None)


def referee(probe: Probe, z: EngineResult, d: EngineResult, h: Optional[EngineResult],
            override: Dict[str, Any]) -> Tuple[str, str]:
    """Returns (status, message). Status is OK, FAIL or INFO."""
    expected: Dict[str, Any] = override.get("expected", {})
    hw_rules = override.get("authority") == "hardwood" and h is not None
    oracles = [("duckdb", d)] + ([("hardwood", h)] if h is not None else [])

    if not z.ok:
        if hw_rules and not h.ok:
            return "OK", "zpq and Hardwood (authoritative) both reject"
        if not hw_rules and all(not r.ok for _, r in oracles):
            return "OK", "all engines rejected (assumed malformed)"
        return "FAIL", f"ZPQ rejected but others accepted. ZPQ Err: {z.err}"
    if hw_rules and not h.ok:
        return "FAIL", f"ZPQ accepted a file Hardwood (authoritative) rejects: {h.err}"

    status, notes = "OK", []
    for it in probe.items:
        has, zv = _cell(z.row, it.alias)
        if not has:
            return "FAIL", f"ZPQ result lacks {it.alias}: {z.row}"
        if it.alias in expected:
            if not compare_values(expected[it.alias], zv):
                status = "FAIL"
                notes.append(f"{it.alias}: zpq={zv!r} expected={expected[it.alias]!r} (override)")
            continue
        votes = {n: _cell(r.row, it.alias)[1] for n, r in oracles if r.ok}
        if hw_rules:
            votes = {"hardwood": votes["hardwood"]}
        if not votes:
            status = "FAIL"
            notes.append(f"{it.alias}: ZPQ accepted but others rejected (zpq={zv!r})")
            continue
        agree = {n: compare_values(zv, v) for n, v in votes.items()}
        if all(agree.values()):
            continue
        shown = ", ".join(f"{n}={v!r}" for n, v in votes.items())
        if len(votes) == 1 or not any(agree.values()) and compare_values(*votes.values()):
            # zpq against a single opinion, or against two oracles that agree with each other: zpq is the outlier.
            status = "FAIL"
            notes.append(f"{it.alias}: zpq={zv!r} {shown}")
        elif status != "FAIL":
            status = "INFO"
            notes.append(f"{it.alias}: oracles disagree; zpq={zv!r} {shown}")
    missing = [f"{n} rejected ({r.err[:80]})" for n, r in oracles if not r.ok]
    if status == "OK" and missing:
        return "INFO", "zpq matches the remaining oracle; " + "; ".join(missing)
    return status, "; ".join(notes + missing)


def read_correctness(hw_fixtures: Optional[str], only: Optional[str]) -> Tuple[int, int, int]:
    print("\n--- ZPQ read correctness (aggregates + filtered counts) ---")
    files = build_corpus_list(hw_fixtures)
    if only:
        files = [(lbl, p) for lbl, p in files if only in lbl]
    if not files:
        print("  INFO  Corpus not found (run `just fetch-corpus`). Skipping read checks.")
        return 0, 0, 0
    unknown = set(REFEREE_OVERRIDES) - {lbl for lbl, _ in files}
    if unknown and not only:
        print(f"  NOTE  overrides for files not in this corpus: {sorted(unknown)}")

    passes = fails = infos = 0
    for label, path in files:
        override = REFEREE_OVERRIDES.get(label, {})
        probes = build_probes(path)
        hw_rows: Optional[EngineResult] = None
        if HARDWOOD_BIN:
            needed = sorted({c for p in probes for c in p.columns})
            try:
                hw_rows = EngineResult(True, {"rows": run_hardwood_json(path, needed or None)})
            except Exception as e:
                hw_rows = EngineResult(False, err=str(e))

        for probe in probes:
            agg_zpq = ", ".join(_sql_item(i, duckdb_sql=False) for i in probe.items)
            agg_duck = ", ".join(_sql_item(i, duckdb_sql=True) for i in probe.items)
            try:
                z = EngineResult(True, _one_row(run_zpq_agg(path, agg_zpq, probe.zpq_filter)))
            except Exception as e:
                z = EngineResult(False, err=str(e))
            where = f" WHERE {probe.duck_where}" if probe.duck_where else ""
            sql = f"SELECT {agg_duck} FROM read_parquet('{path}'){where}"
            try:
                d = EngineResult(True, _one_row(run_duckdb_sql(sql)))
            except Exception as e:
                d = EngineResult(False, err=str(e))
            h = None
            if hw_rows is not None:
                if hw_rows.ok:
                    try:
                        h = EngineResult(True, _one_row(eval_probe(probe, hw_rows.row["rows"])))
                    except Exception as e:
                        h = EngineResult(False, err=f"Hardwood eval: {e}")
                else:
                    h = hw_rows

            status, msg = referee(probe, z, d, h, override)
            tag = f"{label} [{probe.name}]"
            if override and status != "OK":
                msg += f" (override: {override.get('why', '?')})"
            print(f"  {status:<5} {tag}" + (f" | {msg}" if msg and status != "OK" else ""))
            if status == "OK":
                passes += 1
            elif status == "FAIL":
                fails += 1
            else:
                infos += 1

    return passes, fails, infos


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hardwood-fixtures", help="Hardwood checkout root or its core/src/test/resources "
                    "(default: $HARDWOOD_FIXTURES, then tools/hardwood/fixtures)")
    ap.add_argument("--only", help="read checks: only corpus labels containing this substring")
    ap.add_argument("--skip-write", action="store_true", help="skip the write-correctness sections")
    args = ap.parse_args()

    if not os.path.exists(ZPQ_BIN):
        print(f"Missing {ZPQ_BIN}. Please run `zig build -Doptimize=ReleaseFast`")
        sys.exit(2)

    hw_fixtures = resolve_hw_fixtures(args.hardwood_fixtures)
    if HARDWOOD_BIN:
        print(f"Hardwood: {HARDWOOD_BIN} ({hardwood_version()})")
    else:
        print("  WARNING: Hardwood CLI not found (checked $HARDWOOD, tools/hardwood/bin, $PATH).")
        print("           Degrading to 2-engine (ZPQ vs DuckDB) verification. Get it with `just fetch-hardwood`.")
    print(f"Hardwood fixtures: {hw_fixtures or 'none'}")

    tmpdir = tempfile.mkdtemp(prefix="zpq_triangulate_")
    try:
        a = w = (0, 0, 0)
        if not args.skip_write:
            a = write_correctness(tmpdir)
            w = write_mode_checks(tmpdir)
        b = read_correctness(hw_fixtures, args.only)

        print("\n=== Triangulation Summary ===")
        print(f"Write:       {a[0]} PASS, {a[1]} FAIL, {a[2]} INFO")
        print(f"Write modes: {w[0]} PASS, {w[1]} FAIL, {w[2]} INFO")
        print(f"Read:        {b[0]} PASS, {b[1]} FAIL, {b[2]} INFO")

        if a[1] + w[1] + b[1] > 0:
            print("\nFAIL: Regressions detected.")
            sys.exit(1)
        print("\nSUCCESS: All triangulation checks passed.")
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main()
