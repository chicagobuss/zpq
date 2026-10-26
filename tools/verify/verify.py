#!/usr/bin/env python3
"""One entry point for zpq verification.

    python tools/verify/verify.py            tiers, stages, exit codes
    python tools/verify/verify.py quick      core unit tests
    python tools/verify/verify.py commit     test-core, ReleaseFast, full referee
    python tools/verify/verify.py full       the release gate set
    python tools/verify/verify.py soak       seeded loop for a number of hours
    python tools/verify/verify.py perf       pinned scorecard plan/run/summary, then the ship bar
    python tools/verify/verify.py stage NAME one stage
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import unittest
import textwrap
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import execute
import perf
import provenance
import revision as revisions
import stages
import summary


REPO = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO / "zig-out/verify"
DUCKDB20 = Path.home() / ".cache/zpq-duckdb2/bin-2.0.0/duckdb"
CLICKBENCH = Path.home() / "bench/zpq-fair/data/clickbench"
CLICKBENCH_SQL = Path.home() / "bench/zpq-fair/repos/ClickBench/duckdb-parquet"
SKSPILL = Path.home() / ".cache/zpq-review-050/planner-data/skspill.parquet"
SIGPIPE = Path.home() / ".cache/zpq-review-050/io-scratch/net/sigpipe.py"
FAIR = Path.home() / "bench/zpq-fair"


def guide() -> str:
    lines = ["zpq verify — gates, soak, and the scorecard ship bar.", "",
             "Usage: python tools/verify/verify.py COMMAND [OPTIONS]", "",
             "Tiers"]
    tiers = {
        "quick": "All core unit tests in ReleaseSafe, with unchanged scales. The edit loop.",
        "commit": "ReleaseSafe core tests, ReleaseFast CLI, CLI contracts/readback, and the full ReleaseSafe referee.",
        "full": "Every full stage below, with Debug units. Requires --base PATH or --base-rev SHA for comparison.",
    }
    modes = {
        "soak": "Seeded aggregate fuzz, differential, unit and coverage fuzz. --hours sets the duration; "
                "--rounds optionally bounds it. Every round keeps its logs, seed and verdict.",
        "perf": "Pinned scorecard plan/run/summary. --suites and --cases select inputs; missing suites "
                "are blocked. Cold ship-bar scoring and in-process timing are separate metrics. "
                "--resume RUN_DIR continues a prior sealed run in a new record.",
        "stage X": "One stage from the registry below. conform and triangulate are stage-only.",
        "stages": "Show this guide and the registry.",
        "self-test": "Check summary records and scoring without compiling the engine.",
    }
    for section, descriptions in ((None, tiers), ("Modes", modes)):
        if section:
            lines.extend(["", section])
        for name, description in descriptions.items():
            lines.append(textwrap.fill(description, width=80, initial_indent=f"  {name:<10}",
                                       subsequent_indent=" " * 12))
    lines.extend(["", "COMMAND --help lists that command's flags.",
                  "Exit codes: 0 passed/declared refusal; 1 failure; 2 blocked or invalid.",
                  "Outputs: zig-out/verify/<run-id>/summary.json; --out changes the parent.",
                  "Reruns: new directory, --rerun-of ID; earlier failures stay recorded.",
                  "Builds: 16 GiB systemd scope, nice 10, at most -j8; --cpu defaults to",
                  "native (the host CPU). Use --cpu to override the build CPU.",
                  "Dependencies: Zig, the DuckDB CLI, and tools/verify/requirements.txt pins.",
                  "Python dependency mismatches stop before any build or host probe.",
                  "repro-skspill expects exit 1 plus the declared memory-budget message.",
                  "Pinned scorecard: tools/verify/HARNESS; --harness selects its clean checkout.",
                  "", "Stages (tier membership)"])
    for name, tier_names, builder in stages.REGISTRY:
        where = ",".join(t for t in tier_names if t != "extra") or "stage-only"
        lines.append(f"  {name} [{where}]")
        lines.append(textwrap.fill(builder(dummy_ctx()).about, width=80, initial_indent="    ",
                                   subsequent_indent="    "))
    return "\n".join(lines) + "\n"


def dummy_ctx() -> stages.Ctx:
    return stages.Ctx(
        repo=REPO,
        python=sys.executable,
        zigw=str(REPO / "tools/verify/zigw"),
        run_dir=Path("/tmp"),
        j=8,
        cpu="native",
        revision="0" * 40,
        binary=REPO / "zig-out/bin/zpq",
        lib=REPO / "zig-out/lib/libzpq.so",
        base=None,
        clickbench_root=CLICKBENCH,
        clickbench_sql=CLICKBENCH_SQL,
        skspill=SKSPILL,
        sigpipe=SIGPIPE,
        duckdb=str(DUCKDB20),
    )


def head_sha() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()


def python_for() -> str:
    for candidate in (REPO / ".venv/bin/python", Path.home() / "code/zpq/.venv/bin/python"):
        if candidate.is_file():
            return str(candidate)
    return sys.executable


def make_ctx(args, run_dir: Path, revision: str) -> stages.Ctx:
    return stages.Ctx(
        repo=REPO,
        python=python_for(),
        zigw=str(REPO / "tools/verify/zigw"),
        run_dir=run_dir,
        j=args.j,
        cpu=args.cpu,
        revision=revision,
        binary=REPO / "zig-out/bin/zpq",
        lib=REPO / "zig-out/lib/libzpq.so",
        base=str(Path(args.base).expanduser().resolve()) if args.base else None,
        clickbench_root=CLICKBENCH,
        clickbench_sql=CLICKBENCH_SQL,
        skspill=SKSPILL,
        sigpipe=SIGPIPE,
        duckdb=str(DUCKDB20),
        arch_summaries=tuple(str(Path(p).expanduser().resolve()) for p in args.arch_summary),
    )


def open_run(args, tier: str, revision: str) -> tuple[Path, summary.Summary]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parent = Path(args.out).expanduser().resolve() if args.out else DEFAULT_OUT
    run_id = args.run_id or f"{tier}-{revision[:12]}-{stamp}"
    run_dir = parent / run_id
    if (run_dir / "summary.json").exists():
        raise FileExistsError(
            f"{run_dir / 'summary.json'} already exists. A rerun is a new directory; the first record stays."
        )
    probe = tier in ("full", "soak", "perf")
    run_dir.mkdir(parents=True, exist_ok=False)
    host = provenance.collect(REPO, run_dir, args.cpu, probe)
    book = summary.Summary(
        run_dir,
        {
            "tier": tier,
            "sha": revision,
            "label": args.label,
            "rerun_of": args.rerun_of,
            "host": host,
            "binary_sha256": execute.file_sha256(REPO / "zig-out/bin/zpq"),
            "parameters": _parameters(args),
        },
    )
    return run_dir, book


def _parameters(args) -> dict:
    parameters = {"expected_skip_budgets": stages.tier_skip_budgets(REPO)}
    base = getattr(args, "base", None)
    if getattr(args, "base_rev", None):
        parameters["base_rev"] = args.base_rev
    if base:
        parameters["base"] = str(Path(base).expanduser().resolve())
    return parameters


def finish(book: summary.Summary) -> int:
    code = summary.exit_code(book)
    book.finish(code)
    print(f"summary {book.path} exit={code}", flush=True)
    return code


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--out", help=f"parent for the run directory (default {DEFAULT_OUT})")
    common.add_argument("--run-id")
    common.add_argument("--arch-summary", action="append", default=[],
                        help="same-SHA native release evidence; repeat for foreign targets")
    base = common.add_mutually_exclusive_group()
    base.add_argument(
        "--base",
        help="previously integrated comparison binary (or use --base-rev)",
    )
    base.add_argument("--base-rev", help="build the comparison CLI from a git revision in its own cache")
    common.add_argument("-j", type=int, default=8)
    common.add_argument("--cpu", default="native", help="zig -Dcpu (default: native host CPU; override explicitly)")
    common.add_argument("--rerun-of", help="earlier run id this record follows; that summary is not modified")
    common.add_argument("--label")
    common.add_argument("--no-scope", action="store_true", help="do not wrap stages in the 16G systemd scope")

    parser = argparse.ArgumentParser(prog="verify")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("quick", parents=[common], help="core unit tests")
    sub.add_parser("commit", parents=[common], help="test-core, ReleaseFast, differential")
    sub.add_parser("full", parents=[common], help="release gate set")
    soak = sub.add_parser("soak", parents=[common], help="seeded fuzz/differential loop")
    soak.add_argument("--hours", type=float, default=10)
    soak.add_argument("--rounds", type=int)
    stage = sub.add_parser("stage", parents=[common], help="one named stage")
    stage.add_argument("name")
    sub.add_parser("stages", help="list the registry")
    sub.add_parser("self-test", help="summary and ship-bar checks")
    score = sub.add_parser("perf", parents=[common], help="pinned scorecard, then the ship bar")
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
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["arch"]:
        import os

        os.execv("/bin/bash", ["bash", str(REPO / "tools/verify/arch"), *argv[1:]])
    if not argv or argv in (["-h"], ["--help"], ["help"]):
        sys.stdout.write(guide())
        return 0
    args = build_parser().parse_args(argv)
    if args.command == "stages":
        sys.stdout.write(guide())
        return 0
    if args.command == "self-test":
        suite = unittest.defaultTestLoader.loadTestsFromName("test_core")
        result = unittest.TextTestRunner(verbosity=1).run(suite)
        return 0 if result.wasSuccessful() else 1
    if getattr(args, "j", 8) > 8:
        print("refusing -j above 8", file=sys.stderr)
        return 2
    if getattr(args, "j", 8) < 1:
        print("refusing -j below 1", file=sys.stderr)
        return 2
    if args.command == "soak" and (args.hours <= 0 or args.rounds is not None and args.rounds <= 0):
        print("soak needs positive --hours and --rounds", file=sys.stderr)
        return 2
    if args.command == "full":
        if not args.base and not args.base_rev:
            print(
                "full requires --base PATH or --base-rev SHA for the comparison binary. "
                "It is recorded in the summary and is not given a default.",
                file=sys.stderr,
            )
            return 2
        if args.base and not Path(args.base).expanduser().is_file():
            print(f"--base is not a file: {args.base}", file=sys.stderr)
            return 2
    if args.command == "stage":
        try:
            stages.get(args.name)
        except KeyError:
            print(f"unknown stage {args.name}", file=sys.stderr)
            return 2
    if args.command == "perf" and args.resume and not args.rerun_of:
        args.rerun_of = Path(args.resume).expanduser().resolve().name
    environment = provenance.python_environment(REPO)
    if environment["mismatches"]:
        print("verify: Python environment mismatch (no gates started):", file=sys.stderr)
        for mismatch in environment["mismatches"]:
            print(f"  {mismatch}", file=sys.stderr)
        print("Install pins and run verify with that environment:\n"
              "  uv pip install --python .venv/bin/python -r tools/verify/requirements.txt\n"
              "  .venv/bin/python tools/verify/verify.py COMMAND", file=sys.stderr)
        return 2
    uses_corpus = args.command in ("quick", "commit", "full", "soak") or (
        args.command == "stage" and args.name in ("test-core", "test-full", "integration", "conform", "triangulate"))
    if uses_corpus:
        corpus = provenance.corpus_environment(REPO)
        if corpus["mismatch"]:
            print(f"verify: Parquet corpus revision mismatch: {corpus['actual']}; expected {corpus['expected']}. "
                  "Run just fetch-corpus (no gates started).", file=sys.stderr)
            return 2
    if args.base_rev:
        try:
            args.base_rev = revisions.resolve(args.base_rev)
        except subprocess.CalledProcessError:
            print(f"invalid --base-rev: {args.base_rev}", file=sys.stderr)
            return 2
    revision = head_sha()
    try:
        run_dir, book = open_run(args, args.command if args.command != "stage" else f"stage-{args.name}", revision)
    except FileExistsError as error:
        print(error, file=sys.stderr)
        return 2
    ctx = make_ctx(args, run_dir, revision)
    heavy = not args.no_scope
    try:
        if args.base_rev:
            ctx.base = str(revisions.build(args.base_rev, cpu=args.cpu, jobs=args.j, heavy=heavy))
            build_meta = json.loads((Path(ctx.base).parents[1] / "build.json").read_text())
            book.add_stage({"name": "build-base", "status": "pass", "rc": 0,
                            "base_rev": args.base_rev, "binary": ctx.base, "build": build_meta,
                            "binary_sha256": execute.file_sha256(Path(ctx.base)),
                            "log": str(Path(ctx.base).parents[1] / "build.log"), "counts": {}})
        if args.command == "quick":
            execute.run_named(ctx, stages.names_for("quick"), book, heavy)
        elif args.command == "commit":
            execute.run_named(ctx, stages.names_for("commit"), book, heavy)
        elif args.command == "full":
            execute.run_named(ctx, stages.names_for("full"), book, heavy)
        elif args.command == "soak":
            execute.run_soak(ctx, book, heavy, args.hours, args.rounds)
        elif args.command == "stage":
            execute.run_named(ctx, [args.name], book, heavy)
        elif args.command == "perf":
            perf.run_perf(ctx, book, args)
        else:
            print(guide())
            return 2
    except Exception as error:
        log_path = run_dir / "logs" / "runner-error.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(f"{type(error).__name__}: {error}\n")
        book.add_stage({"name": "runner-error", "status": "fail", "rc": 1,
                        "duration_s": 0.0, "log": str(log_path), "counts": {"error": str(error)}})
    return finish(book)


if __name__ == "__main__":
    sys.exit(main())
