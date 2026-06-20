#!/usr/bin/env python3
"""Run ZPQ's `conform` subcommand against every .parquet in
data/parquet-testing/, compare what we see against pyarrow, summarise.

Output:
  - tools/conformance_report.tsv  (one row per file × column-or-summary)
  - stdout: aggregate counts + the failure histogram

Usage:
  python3 tools/conformance.py [--corpus data/parquet-testing/data]
"""
import argparse
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

# Known, deliberate decode_partial gaps as of the 69-pass baseline
# (2026-06-14). These are documented decisions, not unexamined failures —
# the CI floor (--min-pass) guards against *regression*, while these stay
# parked on purpose:
#
#   hadoop_lz4_compressed / non_hadoop_lz4_compressed /
#   hadoop_lz4_compressed_larger  — codec LZ4 (legacy Hadoop framing).
#       Deprecated in favour of LZ4_RAW, which ZPQ decodes. The legacy
#       wire format has two incompatible variants (PARQUET-1241) that need
#       a detection heuristic; not worth it for a dead codec.
#   large_string_map.brotli       — codec BROTLI. Exotic; would mean
#       vendoring a whole new C library for binary bloat we won't pay.
#   nation.dict-malformed         — total_compressed_size undercounts the
#       chunk by 14 bytes (the metadata lies about the chunk length). The
#       byte-copy fastpath still compacts it; the strict decode path
#       rejects lying metadata rather than guessing past the boundary.
#
# UnsupportedCodec / a clean decode error is the honest answer for all of
# these — never a crash or silently-wrong values.


def run_zpq(zpq_bin: str, path: Path) -> dict:
    try:
        proc = subprocess.run(
            [zpq_bin, "conform", str(path)],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        return {"error": "zpq_timeout"}
    if proc.returncode != 0:
        return {"error": f"zpq_exit_{proc.returncode}", "stderr": proc.stderr.strip()[:200]}
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        return {"error": "zpq_bad_json", "stdout_head": proc.stdout[:200], "json_err": str(e)}


def run_pyarrow(path: Path) -> dict:
    try:
        import pyarrow.parquet as pq
        md = pq.read_metadata(str(path))
        sch = pq.read_schema(str(path))
        return {
            "num_rows": md.num_rows,
            "num_row_groups": md.num_row_groups,
            # md.num_columns is the *parquet leaf count* (= number of
            # column chunks per row group) — what ZPQ's tree.leaves
            # also counts. len(schema) is the Arrow-level field count
            # which collapses MAP/LIST into single fields and would
            # mismatch on every nested file.
            "num_columns": md.num_columns,
            "num_arrow_fields": len(list(sch)),
            "leaves": [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in sch],
        }
    except Exception as e:
        return {"error": type(e).__name__, "reason": str(e)[:200]}


def classify(zpq_view: dict, py_view: dict) -> tuple[str, str]:
    """Return (overall_status, detail). Statuses:
       pass         — opened, tree built, leaf count matches, every column decodes
       tree_fail    — tree build failed
       leaf_count_mismatch — tree leaf count != pyarrow column count
       partial      — tree ok, some column decode failures
       zpq_open_fail
       py_open_fail
       row_count_mismatch
    """
    if "error" in py_view:
        return "py_open_fail", py_view.get("reason", py_view.get("error", "?"))
    if "error" in zpq_view:
        return "zpq_open_fail", zpq_view.get("reason", zpq_view.get("error", "?"))

    if zpq_view.get("num_rows") != py_view.get("num_rows"):
        return "row_count_mismatch", f"zpq={zpq_view.get('num_rows')} py={py_view.get('num_rows')}"

    if "tree_build_failed" in zpq_view:
        return "tree_fail", zpq_view["tree_build_failed"]

    tree_leaves = zpq_view.get("tree_leaves")
    py_cols = py_view.get("num_columns")
    if tree_leaves is not None and py_cols is not None and tree_leaves != py_cols:
        return "leaf_count_mismatch", f"zpq_tree={tree_leaves} pyarrow_cols={py_cols}"

    decode_statuses = [d.get("status") for d in zpq_view.get("decode", [])]
    bad = [s for s in decode_statuses if s not in {"ok", "skip_int96"}]
    if bad:
        first_bad = next(d for d in zpq_view["decode"] if d.get("status") not in {"ok", "skip_int96"})
        return "decode_partial", f"col[{first_bad.get('col_idx')}]({first_bad.get('name')}) {first_bad.get('codec')}/{first_bad.get('type')}: {first_bad.get('status')}"

    return "pass", ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default="data/parquet-testing/data")
    ap.add_argument("--zpq", default="zig-out/bin/zpq")
    ap.add_argument("--report", default="tools/conformance_report.tsv")
    ap.add_argument("--limit", type=int, default=0, help="cap files for quick runs")
    ap.add_argument(
        "--min-pass", type=int, default=0,
        help="regression floor: exit 1 if full-pass count drops below this "
             "(hard failures — open/tree/leaf/row-count — always exit 1 when present)",
    )
    args = ap.parse_args()

    corpus = Path(args.corpus)
    if not corpus.exists():
        print(f"corpus not found: {corpus}", file=sys.stderr)
        sys.exit(2)
    if not Path(args.zpq).exists():
        print(f"zpq binary not found at {args.zpq} — run `zig build -Doptimize=ReleaseFast`", file=sys.stderr)
        sys.exit(2)

    files = sorted(corpus.glob("**/*.parquet"))
    if args.limit:
        files = files[: args.limit]

    rows = []
    counts = Counter()
    for path in files:
        zpq_v = run_zpq(args.zpq, path)
        py_v = run_pyarrow(path)
        status, detail = classify(zpq_v, py_v)
        counts[status] += 1
        rows.append((path.name, status, detail))
        # Live progress
        symbol = {
            "pass": ".",
            "decode_partial": "p",
            "zpq_open_fail": "X",
            "row_count_mismatch": "M",
            "py_open_fail": "_",
        }.get(status, "?")
        sys.stdout.write(symbol)
        sys.stdout.flush()
    print()

    with open(args.report, "w") as f:
        f.write("file\tstatus\tdetail\n")
        for r in rows:
            f.write("\t".join(r) + "\n")

    total = sum(counts.values())
    print()
    print("=" * 60)
    print(f"Total files: {total}")
    for k in ("pass", "decode_partial", "tree_fail", "leaf_count_mismatch", "zpq_open_fail", "row_count_mismatch", "py_open_fail"):
        n = counts.get(k, 0)
        pct = 100.0 * n / total if total else 0
        print(f"  {k:25s} {n:4d}  ({pct:5.1f}%)")

    # Failure histogram
    print()
    print("=" * 60)
    print("FAILURE HISTOGRAM (zpq-side issues only):")
    detail_counts = Counter()
    for _, status, detail in rows:
        if status in ("decode_partial", "zpq_open_fail", "row_count_mismatch"):
            # Bucket by the salient token
            token = detail.split(":")[0] if ":" in detail else detail
            # For decode_partial collapse "col[X](name)" to just the codec/type/status
            if status == "decode_partial":
                # detail looks like "col[N](name) CODEC/TYPE: status"
                bits = detail.split(":", 1)
                if len(bits) == 2:
                    head = bits[0].rsplit(" ", 1)[-1]  # "CODEC/TYPE"
                    tail = bits[1].strip()
                    token = f"{head} → {tail}"
            detail_counts[(status, token)] += 1
    for (status, token), n in detail_counts.most_common():
        print(f"  [{n:3d}]  {status:20s}  {token}")

    # CI gate. Two tiers:
    #   * hard failures (crash / wrong structure / wrong row counts)
    #     are never acceptable — any of them fails the run;
    #   * the full-pass count must not regress below --min-pass
    #     (decode_partial entries are known, documented gaps).
    hard = sum(
        counts.get(k, 0)
        for k in ("zpq_open_fail", "tree_fail", "leaf_count_mismatch", "row_count_mismatch")
    )
    if hard:
        print(f"\nFAIL: {hard} hard failure(s) — zpq must never crash or mis-shape a corpus file")
        sys.exit(1)
    if args.min_pass and counts.get("pass", 0) < args.min_pass:
        print(f"\nFAIL: pass count {counts.get('pass', 0)} dropped below floor {args.min_pass}")
        sys.exit(1)


if __name__ == "__main__":
    main()
