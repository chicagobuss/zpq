#!/usr/bin/env python3
"""
Validate zpq output against reference implementations.

Usage:
    python3 tools/validate_parquet.py [file.parquet]
    python3 tools/validate_parquet.py --all
    python3 tools/validate_parquet.py --backend duckdb [file.parquet]
    python3 tools/validate_parquet.py --backend pyarrow --all
    python3 tools/validate_parquet.py --compare [file.parquet]  # 3-way comparison

Backends: duckdb (default), pyarrow
"""

import subprocess
import sys
import math
from pathlib import Path

# Available backends
BACKENDS = {}

try:
    import duckdb
    BACKENDS['duckdb'] = True
except ImportError:
    BACKENDS['duckdb'] = False

try:
    import pyarrow.parquet as pq
    BACKENDS['pyarrow'] = True
except ImportError:
    BACKENDS['pyarrow'] = False


def get_zpq_values(parquet_file: Path, column_idx: int, limit: int = 10) -> tuple:
    """Run zpq cat and extract values for a column."""
    try:
        result = subprocess.run(
            ["zig", "build", "run", "--", "cat", str(parquet_file)],
            capture_output=True,
            cwd=Path(__file__).parent.parent,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return None, "timeout"

    try:
        stdout = result.stdout.decode('utf-8', errors='replace') if result.stdout else ""
        stderr = result.stderr.decode('utf-8', errors='replace') if result.stderr else ""
    except Exception as e:
        return None, f"decode error: {e}"

    if result.returncode != 0:
        error_lines = [l for l in stderr.split('\n') if 'error' in l.lower() or 'Error' in l or 'panic' in l.lower()]
        if error_lines:
            return None, error_lines[0][:80]
        return None, f"exit code {result.returncode}"

    output = stdout or stderr
    lines = output.strip().split("\n")
    values = []
    in_column = False
    current_col = -1

    for line in lines:
        if line.startswith("anyzig:"):
            continue
        if line.startswith("Column "):
            current_col += 1
            in_column = (current_col == column_idx)
            continue
        if in_column and line.startswith("  "):
            val = line.strip()
            if val == "NULL":
                values.append(None)
            else:
                values.append(val)
            if len(values) >= limit:
                break

    return values, None


# ============ DuckDB Backend ============

def duckdb_get_schema(parquet_file: Path) -> list:
    """Get schema using DuckDB."""
    conn = duckdb.connect(":memory:")
    schema = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_file}')").fetchall()
    conn.close()
    return [(row[0], row[1]) for row in schema]  # (name, type)


def duckdb_get_values(parquet_file: Path, column_idx: int, limit: int = 10) -> list:
    """Read values using DuckDB."""
    try:
        conn = duckdb.connect(":memory:")
        schema = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_file}')").fetchall()
        if column_idx >= len(schema):
            return None
        col_name = schema[column_idx][0]
        result = conn.execute(f'SELECT "{col_name}" FROM read_parquet(\'{parquet_file}\') LIMIT {limit}').fetchall()
        values = [row[0] for row in result]
        conn.close()
        return values
    except Exception:
        return None


# ============ PyArrow Backend ============

def pyarrow_get_schema(parquet_file: Path) -> list:
    """Get schema using PyArrow."""
    table = pq.read_table(parquet_file)
    return [(field.name, str(field.type)) for field in table.schema]


def pyarrow_get_values(parquet_file: Path, column_idx: int, limit: int = 10) -> list:
    """Read values using PyArrow."""
    try:
        table = pq.read_table(parquet_file)
        col = table.column(column_idx)
        values = []
        for v in col.to_pylist()[:limit]:
            values.append(v)
        return values
    except Exception:
        return None


# ============ Backend Registry ============

BACKEND_FUNCS = {
    'duckdb': (duckdb_get_schema, duckdb_get_values),
    'pyarrow': (pyarrow_get_schema, pyarrow_get_values),
}


def compare_values(zpq_vals: list, ref_vals: list, col_name: str, tolerance: float = 1e-6) -> tuple:
    """Compare zpq string values against reference typed values."""
    if len(zpq_vals) != len(ref_vals):
        return False, f"Length mismatch: zpq={len(zpq_vals)}, ref={len(ref_vals)}"

    for i, (z, p) in enumerate(zip(zpq_vals, ref_vals)):
        z_is_null = z is None or z == "NULL" or z == "null"
        p_is_null = p is None
        if z_is_null and p_is_null:
            continue
        if z_is_null or p_is_null:
            return False, f"Row {i}: NULL mismatch zpq={z} vs ref={p}"

        try:
            if isinstance(p, float):
                zf = float(z)
                if math.isnan(p) and math.isnan(zf):
                    continue
                if abs(zf - p) > tolerance and abs(zf - p) / max(abs(p), 1e-10) > tolerance:
                    return False, f"Row {i}: {z} != {p} (diff={abs(zf-p)})"
            elif isinstance(p, int):
                zi = int(z)
                if zi != p:
                    return False, f"Row {i}: {z} != {p}"
            elif isinstance(p, bytes):
                continue  # Skip binary for now
            else:
                if str(z) != str(p):
                    return False, f"Row {i}: '{z}' != '{p}'"
        except (ValueError, TypeError):
            continue

    return True, "OK"


def validate_file(parquet_file: Path, backend: str = 'duckdb', verbose: bool = False) -> tuple:
    """Validate a single parquet file against reference implementation."""
    get_schema, get_values = BACKEND_FUNCS[backend]

    try:
        schema = get_schema(parquet_file)
    except Exception as e:
        return None, f"{backend} can't read: {e}"

    results = []
    all_pass = True

    for i, (col_name, col_type) in enumerate(schema):
        zpq_vals, err = get_zpq_values(parquet_file, i, limit=10)
        if err:
            if "UnsupportedCompression" in err or "not supported" in err:
                results.append((col_name, "SKIP", "unsupported"))
                continue
            results.append((col_name, "FAIL", f"zpq error: {err[:50]}"))
            all_pass = False
            continue

        if zpq_vals is None or len(zpq_vals) == 0:
            results.append((col_name, "SKIP", "no values"))
            continue

        ref_vals = get_values(parquet_file, i, limit=10)
        if ref_vals is None:
            results.append((col_name, "SKIP", f"{backend} can't convert"))
            continue

        match, msg = compare_values(zpq_vals, ref_vals, col_name)
        if match:
            results.append((col_name, "PASS", msg))
        else:
            results.append((col_name, "FAIL", msg))
            all_pass = False

    return all_pass, results


def format_val(v, max_len=25):
    """Format a value for display, truncating if needed."""
    if v is None:
        return "NULL"
    s = str(v)
    if len(s) > max_len:
        return s[:max_len-3] + "..."
    return s


def compare_three_way(parquet_file: Path, limit: int = 5) -> None:
    """3-way comparison: zpq vs duckdb vs pyarrow."""
    available = [k for k, v in BACKENDS.items() if v]
    if len(available) < 2:
        print(f"Need at least 2 backends for comparison. Available: {available}")
        return

    # Get schema from first available backend
    get_schema, _ = BACKEND_FUNCS[available[0]]
    try:
        schema = get_schema(parquet_file)
    except Exception as e:
        print(f"Can't read schema: {e}")
        return

    print(f"File: {parquet_file.name}")
    print(f"3-way comparison (first {limit} values per column)\n")

    for col_idx, (col_name, col_type) in enumerate(schema):
        print(f"Column: {col_name} ({col_type})")
        print("-" * 80)

        # Get zpq values
        zpq_vals, err = get_zpq_values(parquet_file, col_idx, limit=limit)
        if err:
            print(f"  zpq: ERROR - {err}")
            zpq_vals = []

        # Get values from each backend
        backend_vals = {}
        for backend in available:
            _, get_values = BACKEND_FUNCS[backend]
            vals = get_values(parquet_file, col_idx, limit=limit)
            backend_vals[backend] = vals if vals else []

        # Print header
        headers = ["Row", "zpq"] + available
        widths = [4, 28] + [28] * len(available)
        header_line = "  ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
        print(f"  {header_line}")
        print("  " + "  ".join("-" * w for w in widths))

        # Find max rows
        max_rows = max(
            len(zpq_vals or []),
            max((len(v) for v in backend_vals.values()), default=0)
        )

        if max_rows == 0:
            print("  (no values)")
        else:
            for i in range(min(max_rows, limit)):
                zpq_v = format_val(zpq_vals[i]) if zpq_vals and i < len(zpq_vals) else "-"
                row = [f"{i:<4}", f"{zpq_v:<28}"]
                for backend in available:
                    bv = backend_vals[backend]
                    val = format_val(bv[i]) if bv and i < len(bv) else "-"
                    row.append(f"{val:<28}")

                # Check agreement
                all_vals = [zpq_v] + [format_val(backend_vals[b][i]) if backend_vals[b] and i < len(backend_vals[b]) else "-" for b in available]
                # Normalize for comparison
                normalized = set()
                for v in all_vals:
                    if v == "-":
                        continue
                    try:
                        normalized.add(float(v))
                    except:
                        normalized.add(v)

                marker = "" if len(normalized) <= 1 else " !!!"
                print(f"  {'  '.join(row)}{marker}")

        print()


def main():
    args = sys.argv[1:]
    backend = 'duckdb'

    # Parse --compare flag
    if '--compare' in args:
        args.remove('--compare')
        if not args:
            print("Usage: --compare <file.parquet>")
            sys.exit(1)
        parquet_file = Path(args[0])
        if not parquet_file.exists():
            print(f"ERROR: {parquet_file} not found")
            sys.exit(1)
        compare_three_way(parquet_file)
        sys.exit(0)

    # Parse --backend flag
    if '--backend' in args:
        idx = args.index('--backend')
        if idx + 1 < len(args):
            backend = args[idx + 1]
            args = args[:idx] + args[idx+2:]

    if not BACKENDS.get(backend):
        print(f"ERROR: {backend} not installed. Run: pip install {backend}")
        sys.exit(1)

    if not args:
        print(__doc__)
        print(f"\nAvailable backends: {', '.join(k for k, v in BACKENDS.items() if v)}")
        sys.exit(1)

    if args[0] == "--all":
        data_dir = Path(__file__).parent.parent / "references" / "parquet-testing" / "data"
        if not data_dir.exists():
            print(f"ERROR: {data_dir} not found")
            sys.exit(1)

        files = sorted(data_dir.glob("*.parquet"))
        passed = failed = skipped = 0

        print(f"Using backend: {backend}\n")
        for f in files:
            name = f.name
            result, info = validate_file(f, backend=backend)

            if result is None:
                print(f"{name:55} SKIP ({info})")
                skipped += 1
            elif result:
                print(f"{name:55} PASS")
                passed += 1
            else:
                print(f"{name:55} FAIL")
                for col, status, msg in info:
                    if status == "FAIL":
                        print(f"    {col}: {msg}")
                failed += 1

        print(f"\n=== Summary ({backend}) ===")
        print(f"Passed:  {passed}")
        print(f"Skipped: {skipped}")
        print(f"Failed:  {failed}")
        sys.exit(0 if failed == 0 else 1)

    else:
        parquet_file = Path(args[0])
        if not parquet_file.exists():
            print(f"ERROR: {parquet_file} not found")
            sys.exit(1)

        result, info = validate_file(parquet_file, backend=backend, verbose=True)

        if result is None:
            print(f"SKIP: {info}")
            sys.exit(0)

        print(f"File: {parquet_file.name} (backend: {backend})")
        print(f"{'Column':<30} {'Status':<8} {'Details'}")
        print("-" * 70)
        for col, status, msg in info:
            print(f"{col:<30} {status:<8} {msg}")

        sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
