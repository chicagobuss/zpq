#!/usr/bin/env python3
"""The scorecard and its ship bar (the gates are `zig build check -Dtier=...`; see tools/check/README.md).

    python tools/verify/verify.py perf [--suites S] [--cases C] [--engines E] [--reps N] [--plan-only]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import perf
import provenance
import summary


REPO = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO / "zig-out/verify"
FAIR = Path.home() / "bench/zpq-fair"


def python_for() -> str:
    for candidate in (REPO / ".venv/bin/python", Path.home() / "code/zpq/.venv/bin/python"):
        if candidate.is_file():
            return str(candidate)
    return sys.executable


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="verify")
    sub = parser.add_subparsers(dest="command", required=True)
    score = sub.add_parser("perf", help="pinned scorecard, then the ship bar")
    score.add_argument("--out", help=f"parent for the run directory (default {DEFAULT_OUT})")
    score.add_argument("--run-id")
    score.add_argument("--cpu", default="native", help="zig -Dcpu recorded with the host")
    score.add_argument("--rerun-of", help="earlier run id this record follows; that summary is not modified")
    score.add_argument("--label")
    score.add_argument("--harness", default=str(Path.home() / "code/zpq-wt/scorecard-inproc"))
    score.add_argument("--binary", default=str(FAIR / "zpq-e4c6878"))
    score.add_argument("--library", default=str(FAIR / "lib-e4c6878/lib/libzpq.so"))
    score.add_argument("--control", default=str(FAIR / "zpq-588a80c"))
    score.add_argument("--no-control", action="store_true")
    score.add_argument("--profile", choices=("cold", "in-process"), default="cold")
    score.add_argument("--suites", help="comma-separated suites; default is the profile's full list")
    score.add_argument("--cases", help="comma-separated query ids or suite/dataset/query_id")
    score.add_argument("--engines", default="zpq,duckdb20,polars")
    score.add_argument("--reps", type=int, default=3)
    score.add_argument("--plan-only", action="store_true")
    score.add_argument("--resume", metavar="RUN_DIR", help="continue a sealed prior run in a new labelled record")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.resume and not args.rerun_of:
        args.rerun_of = Path(args.resume).expanduser().resolve().name
    environment = provenance.python_environment(REPO)
    if environment["mismatches"]:
        print("verify: Python environment mismatch (nothing started):", file=sys.stderr)
        for mismatch in environment["mismatches"]:
            print(f"  {mismatch}", file=sys.stderr)
        print("  uv pip install --python .venv/bin/python -r tools/verify/requirements.txt", file=sys.stderr)
        return 2
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = (Path(args.out).expanduser().resolve() if args.out else DEFAULT_OUT) / (
        args.run_id or f"perf-{revision[:12]}-{stamp}")
    if (run_dir / "summary.json").exists():
        print(f"{run_dir / 'summary.json'} already exists; a rerun is a new directory", file=sys.stderr)
        return 2
    run_dir.mkdir(parents=True, exist_ok=False)
    book = summary.Summary(run_dir, {"tier": "perf", "sha": revision, "label": args.label, "rerun_of": args.rerun_of,
                                     "host": provenance.collect(REPO, run_dir, args.cpu, True)})
    ctx = SimpleNamespace(repo=REPO, python=python_for(), run_dir=run_dir, revision=revision)
    try:
        perf.run_perf(ctx, book, args)
    except Exception as error:  # noqa: BLE001 - recorded as a failed stage, never swallowed
        book.add_stage({"name": "runner-error", "status": "fail", "rc": 1, "duration_s": 0.0, "log": "",
                        "counts": {"error": str(error)}})
    code = summary.exit_code(book)
    book.finish(code)
    print(f"summary {book.path} exit={code}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
