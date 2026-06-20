#!/usr/bin/env python3
"""Corpus random-operation differential testing (RAGS-style).

The dual of `differential.py` (which randomizes *data* over a fixed query
set): this randomizes the *operations* over a fixed *corpus*. For each
parquet file in the corpus it introspects the real schema, generates a
seeded batch of random-but-valid aggregate queries (count/min/max/sum/avg,
optionally under a random filter sampled from the column's own values), and
runs each through ZPQ and DuckDB, comparing normalized results.

The point of the corpus living on S3 (rustfs / R2 / real S3) is that ZPQ
reads it **over the network via its own SigV4 client** — the same path a
real deploy uses — while DuckDB reads the local copy as the oracle. Same
bytes, two engines, one referee.

A failing case prints the seed + file + exact query, so it reproduces with
`--seed`. DuckDB is ground truth; a divergence is a ZPQ bug or a documented
known-divergence (see normalize_value in triangulate.py).

Usage:
  .venv/bin/python tools/corpus_diff.py \
      --local data/parquet-testing/data \
      --s3 s3://rustfs-test-bucket/corpus/parquet-testing \
      --seed 1 --ops 8 [--files N]

  # Pure-local (no network): omit --s3; ZPQ reads local files too.
  .venv/bin/python tools/corpus_diff.py --local data/parquet-testing/data
"""
import argparse
import os
import random
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from triangulate import compare_values, rows_match, run_duckdb_sql, ZPQ_BIN  # noqa: E402

try:
    import duckdb
except ImportError:
    print("needs duckdb — run via the project .venv", file=sys.stderr)
    sys.exit(2)

try:
    import pyarrow.parquet as pq  # page-index / type introspection in write mode
except ImportError:
    pq = None

TIMEOUT_S = 60

# DuckDB physical types we know how to generate operations for. Nested
# (LIST/STRUCT/MAP) and temporal/blob edges need separate harness support;
# including them here would report unsupported surfaces as noise.
#
# DECIMAL (DuckDB prints it as "DECIMAL(p,s)") is admitted via is_numeric so
# the write harness actually projects decimal columns — that's the only way
# the re-encode DECIMAL→DOUBLE fidelity gap becomes observable.
NUMERIC = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT",
           "USMALLINT", "UINTEGER", "UBIGINT", "FLOAT", "DOUBLE"}
STRINGY = {"VARCHAR"}


def is_decimal(ty: str) -> bool:
    return ty.startswith("DECIMAL")


def is_numeric(ty: str) -> bool:
    return ty in NUMERIC or is_decimal(ty)


def zpq_agg(path: str, agg: str, where: str | None) -> dict | None:
    """Run one aggregate through ZPQ; return the agg dict or None on error."""
    import json
    cmd = [ZPQ_BIN, "query", path, "--aggregate", agg]
    if where:
        cmd += ["--filter", where]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT_S)
    if proc.returncode != 0:
        return None
    try:
        return json.loads(proc.stdout).get("agg")
    except json.JSONDecodeError:
        return None


def duck_agg(local_path: str, agg: str, where: str | None) -> dict | None:
    sql = f"SELECT {agg} FROM read_parquet('{local_path}')"
    if where:
        sql += f" WHERE {where}"
    rows = run_duckdb_sql(sql)
    return rows[0] if rows else None


def columns_of(local_path: str) -> list[tuple[str, str]]:
    """(name, duckdb_type) for top-level columns, primitives only."""
    rows = run_duckdb_sql(f"DESCRIBE SELECT * FROM read_parquet('{local_path}')")
    out = []
    for r in rows:
        name, ty = r["column_name"], r["column_type"]
        if is_numeric(ty) or ty in STRINGY or ty == "BOOLEAN":
            out.append((name, ty))
    return out


def sample_literal(local_path: str, col: str, ty: str, rng: random.Random):
    """A literal drawn from the column's real values, so filters keep some
    rows (a random constant would usually select 0 or all)."""
    try:
        rows = run_duckdb_sql(
            f"SELECT \"{col}\" AS v FROM read_parquet('{local_path}') "
            f"WHERE \"{col}\" IS NOT NULL USING SAMPLE 20 ROWS"
        )
    except RuntimeError:
        return None
    vals = [r["v"] for r in rows if r["v"] is not None]
    if not vals:
        return None
    v = rng.choice(vals)
    if ty in STRINGY:
        return "'" + str(v).replace("'", "''") + "'"
    if ty == "BOOLEAN":
        return "true" if v else "false"
    if is_decimal(ty):
        return str(v)  # repr(Decimal) is "Decimal('1.23')" — not valid SQL
    return repr(v)


def rand_filter(local_path: str, cols: list[tuple[str, str]], rng: random.Random) -> str | None:
    """A value-derived predicate on a random column, or None. The literal is
    sampled from the column's real values so the filter keeps some rows."""
    if not cols or rng.random() >= 0.6:
        return None
    fcol, fty = rng.choice(cols)
    lit = sample_literal(local_path, fcol, fty, rng)
    if lit is None:
        return None
    if is_numeric(fty):
        op = rng.choice(["<", "<=", ">", ">=", "=", "!="])
    elif fty in STRINGY:
        op = rng.choice(["=", "!="])
    else:  # BOOLEAN
        op = "="
    return f'"{fcol}" {op} {lit}'


def gen_ops(local_path: str, cols: list[tuple[str, str]], n: int, rng: random.Random):
    """Yield (agg, where|None) pairs valid for this file's schema."""
    numeric = [c for c in cols if is_numeric(c[1])]
    ops = []
    # count(*) is always valid and the cheapest cross-check.
    ops.append(("count(*) AS n", None))
    for _ in range(n):
        # Pick an aggregate.
        if numeric and rng.random() < 0.8:
            col, _ty = rng.choice(numeric)
            fn = rng.choice(["sum", "min", "max", "avg", "count"])
            agg = f'{fn}("{col}") AS r'
        else:
            agg = "count(*) AS r"
        ops.append((agg, rand_filter(local_path, cols, rng)))
    return ops


def fold_avg(zv):
    """ZPQ emits avg as {sum,count} so partial averages re-aggregate across a
    fan-out; fold to a scalar before comparing. count==0 → empty → NULL. A
    non-finite sum arrives as the string "NaN"/"Infinity" — pass it through
    for the NaN-aware compare."""
    if isinstance(zv, dict) and "sum" in zv and "count" in zv:
        s, c = zv["sum"], zv["count"]
        return None if not c else (s if isinstance(s, str) else s / c)
    return zv


def run_read(files, args):
    """Aggregate differential: ZPQ (over S3 if --s3) vs DuckDB-local oracle."""
    checked = mism = skipped = 0
    failures = []
    for f in files:
        local = str(f)
        zpq_path = f"{args.s3.rstrip('/')}/{f.name}" if args.s3 else local
        try:
            cols = columns_of(local)
        except RuntimeError:
            skipped += 1  # DuckDB can't read it → no oracle; not our verdict
            continue
        frng = random.Random((args.seed, f.name).__hash__())
        for agg, where in gen_ops(local, cols, args.ops, frng):
            try:
                d = duck_agg(local, agg, where)
            except RuntimeError:
                continue
            z = zpq_agg(zpq_path, agg, where)
            checked += 1
            if z is None:
                mism += 1
                failures.append((f.name, agg, where, "zpq error/no-result", d))
                continue
            ok = all(
                compare_values(fold_avg(zv), next((d[dk] for dk in d if dk.lower() == k.lower()), None))
                for k, zv in z.items()
            )
            if not ok:
                mism += 1
                failures.append((f.name, agg, where, z, d))
    return checked, mism, skipped, failures


# --- write mode: S3-to-S3 round-trip ---------------------------------------

def aws_env():
    """aws-cli env mapped from the S3_* vars ZPQ uses, so both hit rustfs."""
    e = dict(os.environ)
    e["AWS_ACCESS_KEY_ID"] = os.environ.get("S3_ACCESS_KEY_ID", "")
    e["AWS_SECRET_ACCESS_KEY"] = os.environ.get("S3_SECRET_ACCESS_KEY", "")
    e["AWS_REGION"] = os.environ.get("S3_REGION", "us-east-1")
    return e


def s3_download(url: str, dest: str):
    subprocess.run(
        ["aws", "--endpoint-url", os.environ["S3_ENDPOINT_URL"], "s3", "cp", url, dest, "--no-progress"],
        env=aws_env(), capture_output=True, text=True, timeout=TIMEOUT_S, check=True,
    )


def col_types(path: str, collist: str) -> dict:
    """Map column name → DuckDB type string for the projected columns."""
    rows = run_duckdb_sql(f"DESCRIBE SELECT {collist} FROM read_parquet('{path}')")
    return {r["column_name"]: r["column_type"] for r in rows}


def has_page_index(path: str) -> bool:
    """True if any column chunk in the file carries a page index (offset or
    column index). Uses pyarrow's ColumnChunkMetaData flags — DuckDB's
    parquet_metadata() doesn't surface the offset/column-index pointers."""
    if pq is None:
        return False
    m = pq.ParquetFile(path).metadata
    for rg in range(m.num_row_groups):
        for c in range(m.row_group(rg).num_columns):
            cc = m.row_group(rg).column(c)
            if cc.has_offset_index or cc.has_column_index:
                return True
    return False


def gen_write_ops(local_path, cols, n, rng):
    """Yield (projection_columns, where|None) — a random primitive-column
    subset plus an optional value-derived filter."""
    names = [c[0] for c in cols]
    ops = []
    for _ in range(n):
        k = rng.randint(1, min(4, len(names)))
        ops.append((rng.sample(names, k), rand_filter(local_path, cols, rng)))
    return ops


def run_write(files, args):
    """Round-trip: ZPQ reads s3://in, filters+projects, writes s3://out over
    the network; DuckDB foreign-reads ZPQ's output and compares to its own
    direct filter+projection of the input. Exercises encoder, footer guard,
    multipart upload, and codecs — the S3-to-S3 path ZPQ exists for."""
    checked = mism = skipped = 0
    failures = []
    fidelity = []          # type/index losses, independent of row equality
    seen = set()           # dedupe fidelity findings per (kind, file, ...)
    out_prefix = f"{args.s3.rstrip('/')}/_cdiff_out/{args.seed}"
    for f in files:
        local = str(f)
        in_s3 = f"{args.s3.rstrip('/')}/{f.name}"
        try:
            cols = columns_of(local)
        except RuntimeError:
            skipped += 1
            continue
        if not cols:
            continue
        frng = random.Random((args.seed, "w", f.name).__hash__())
        for i, (proj, where) in enumerate(gen_write_ops(local, cols, args.ops, frng)):
            out_s3 = f"{out_prefix}/{f.name}.{i}.parquet"
            cmd = [ZPQ_BIN, "query", in_s3, "-o", out_s3, "--columns", ",".join(proj)]
            if where:
                cmd += ["--filter", where]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT_S)
            if proc.returncode != 0:
                err = proc.stderr.strip()
                if "Nested" in err:  # projecting only primitives shouldn't hit this; expected if it does
                    continue
                mism += 1
                failures.append((f.name, proj, where, f"zpq write error: {err[:90]}", None))
                continue
            checked += 1
            collist = ", ".join(f'"{c}"' for c in proj)
            wsql = f" WHERE {where}" if where else ""
            tmp_path = None
            try:
                import tempfile
                fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
                os.close(fd)
                s3_download(out_s3, tmp_path)
                expected = run_duckdb_sql(f"SELECT {collist} FROM read_parquet('{local}'){wsql} ORDER BY ALL")
                actual = run_duckdb_sql(f"SELECT {collist} FROM read_parquet('{tmp_path}') ORDER BY ALL")

                # --- fidelity checks (independent of row equality) ---
                # Page index: input carried one, output dropped it. Only
                # meaningful when the output actually has row groups (an
                # all-rows-filtered-out file legitimately has no chunks).
                idx_key = ("idx", f.name, bool(where))
                if idx_key not in seen and len(actual) > 0:
                    seen.add(idx_key)
                    if has_page_index(local) and not has_page_index(tmp_path):
                        fidelity.append((f.name, tuple(proj), where,
                                         "FIDELITY: page index dropped on output",
                                         "input had offset/column index; output has none"))
                # Type drift: a DECIMAL column that came back as non-DECIMAL
                # is the re-encode coercion. Targeted (not all type drift) to
                # avoid noise from benign physical-type equivalences.
                in_t, out_t = col_types(local, collist), col_types(tmp_path, collist)
                for c in proj:
                    it, ot = in_t.get(c, ""), out_t.get(c, "")
                    if is_decimal(it) and not is_decimal(ot):
                        dec_key = ("dec", f.name, c)
                        if dec_key not in seen:
                            seen.add(dec_key)
                            fidelity.append((f.name, (c,), where,
                                             f"FIDELITY: DECIMAL lost ({it} → {ot})",
                                             "re-encode coerced decimal to double"))
            except Exception as e:
                mism += 1
                failures.append((f.name, proj, where, f"validate error: {str(e)[:90]}", None))
                continue
            finally:
                if tmp_path:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
            if len(expected) != len(actual):
                mism += 1
                failures.append((f.name, proj, where, f"rows zpq={len(actual)}", f"duck={len(expected)}"))
            elif not rows_match(expected, actual):
                mism += 1
                failures.append((f.name, proj, where, "row VALUES differ", "(reader: duckdb on zpq output)"))
    failures.extend(fidelity)
    mism += len(fidelity)
    return checked, mism, skipped, failures


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", required=True, help="local corpus dir (DuckDB oracle reads these)")
    ap.add_argument("--s3", default=None, help="s3:// prefix; ZPQ reads (and, in write mode, writes) here over the network")
    ap.add_argument("--mode", choices=["read", "write", "both"], default="read")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--ops", type=int, default=8, help="random ops per file")
    ap.add_argument("--files", type=int, default=0, help="cap files (0 = all)")
    args = ap.parse_args()

    if args.mode in ("write", "both") and not args.s3:
        print("write mode needs --s3 (it writes ZPQ output back over the network)", file=sys.stderr)
        return 2

    files = sorted(Path(args.local).glob("*.parquet"))
    if args.files:
        files = files[: args.files]
    if not files:
        print(f"no parquet under {args.local}", file=sys.stderr)
        return 2

    rc = 0
    for mode, runner in (("read", run_read), ("write", run_write)):
        if args.mode not in (mode, "both"):
            continue
        checked, mism, skipped, failures = runner(files, args)
        print(f"\ncorpus_diff[{mode}]: seed={args.seed} files={len(files)} "
              f"(skipped {skipped} duckdb-unreadable) checks={checked} mismatches={mism}")
        for name, op, where, zv, dv in failures[:40]:
            w = f" WHERE {where}" if where else ""
            print(f"  MISMATCH {name}: {op}{w}\n    zpq={zv}\n    duck={dv}")
        if mism:
            print(f"Reproduce: --mode {mode} --seed {args.seed}")
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
