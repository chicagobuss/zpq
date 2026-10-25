#!/usr/bin/env python3
"""One entry point for zpq verification.

    python tools/verify/verify.py            tiers, stages, exit codes
    python tools/verify/verify.py quick      core unit tests
    python tools/verify/verify.py commit     test-core, ReleaseFast, differential slice
    python tools/verify/verify.py full       the release gate set
    python tools/verify/verify.py soak       seeded loop for a number of hours
    python tools/verify/verify.py perf       pinned scorecard plan/run/summary, then the ship bar
    python tools/verify/verify.py stage NAME one stage
"""

from __future__ import annotations

import argparse
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
    blurbs = {name: builder(dummy_ctx()).about for name, _tiers, builder in stages.REGISTRY}
    lines = [
        "zpq verify — gates, soak, and the scorecard ship bar.",
        "",
        "Tiers",
        "  quick     core unit tests (zig build test-core). The edit loop.",
        "  commit    test-core, ReleaseFast -Drevision=HEAD -Dcpu=raptorlake, a CLI tier, then",
        "            the ReleaseSafe referee.",
        "  full      the stages listed below. Requires --base PATH, the previously integrated",
        "            binary the candidate is checked against. The path is recorded in the",
        "            summary. There is no default.",
        "",
        "Modes",
        "  soak      one ReleaseFast build, then seeded rounds of agg fuzz, differential, unit, and",
        "            zig build fuzz until --hours (default 10). Failures are copied to fail/ and the",
        "            loop continues. --rounds N stops after N rounds.",
        "  perf      calls the pinned scorecard worktree: plan, run, summary. Refuses to reseal a",
        "            directory that already has manifest.jsonl (--resume continues it). Ship-bar",
        "            text and host provenance stay outside the sealed directory.",
        "            --suites and --cases are the declared filter. The default --suites list is",
        "            the profile's full set. A suite whose inputs are missing is skipped and",
        "            recorded; the suites that resolve are still planned. A skipped suite is",
        "            not a pass. --profile in-process is a separate metric from the cold ship bar.",
        "  stage X   one stage. conform and triangulate are not part of full.",
        "  stages    the registry.",
        "  self-test summary writer and ship bar. Does not compile zpq.",
        "",
        "Exit codes",
        "  0  every required stage passed, or was a declared refusal",
        "  1  any unexpected failure. The summary lists it, and a later run does not erase it.",
        "  2  a stage was blocked (missing binary, fixture, --base, or a skipped scorecard",
        "     suite) or the invocation is bad.",
        "",
        "repro-skspill expects exit 1 and the text",
        '  "the query needs more memory than --max-memory allows"',
        "  with -j 8 --max-memory 500MB. That result is status expected.",
        "",
        "Host builds default to -Dcpu=raptorlake; --cpu selects another native target.",
        "The flag is stored in the summary as zig_cpu.",
        "Cross builds of aarch64 and macOS do not take that Intel model.",
        "Stages that compile or run the engine use systemd-run --user --scope -p MemoryMax=20G",
        "and nice -n 10, with at most -j8. perf does not add a second cap around the harness.",
        "",
        "Outputs land in zig-out/verify/<run-id>/summary.json unless --out is set.",
        "",
        "Full and commit stages",
    ]
    for name, tiers, _builder in stages.REGISTRY:
        where = ",".join(tier for tier in tiers if tier != "extra") or "stage-only"
        lines.append(f"  {name:<28} {where:<16} {blurbs[name]}")
    lines.append("")
    lines.append("Pinned harness: tools/verify/HARNESS (bench/scorecard-inproc, not merged into this branch).")
    return "\n".join(textwrap.fill(line, width=80, subsequent_indent="    ") if len(line) > 80 else line
                     for line in lines) + "\n"


def dummy_ctx() -> stages.Ctx:
    return stages.Ctx(
        repo=REPO,
        python=sys.executable,
        zigw=str(REPO / "tools/verify/zigw"),
        run_dir=Path("/tmp"),
        j=8,
        cpu="raptorlake",
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
    diff = list(getattr(args, "diff_args", []) or [])
    if diff[:1] == ["--"]:
        diff = diff[1:]
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
        base=args.base,
        clickbench_root=CLICKBENCH,
        clickbench_sql=CLICKBENCH_SQL,
        skspill=SKSPILL,
        sigpipe=SIGPIPE,
        duckdb=str(DUCKDB20),
        differential_args=diff,
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
    parameters = {}
    base = getattr(args, "base", None)
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
    common.add_argument(
        "--base",
        help="previously integrated binary the candidate is checked against (required for full; recorded, not defaulted)",
    )
    common.add_argument("-j", type=int, default=8)
    common.add_argument("--cpu", default="raptorlake", help="zig -Dcpu for native ReleaseFast builds")
    common.add_argument("--rerun-of", help="earlier run id this record follows; that summary is not modified")
    common.add_argument("--label")
    common.add_argument("--no-scope", action="store_true", help="do not wrap stages in the 20G systemd scope")

    parser = argparse.ArgumentParser(prog="verify")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("quick", parents=[common], help="core unit tests")
    commit = sub.add_parser("commit", parents=[common], help="test-core, ReleaseFast, differential")
    commit.add_argument("diff_args", nargs=argparse.REMAINDER)
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
    score.add_argument("--resume", action="store_true", help="continue a sealed dir; do not call plan")
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
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
    if args.command == "commit" and list(getattr(args, "diff_args", []) or []) not in ([], ["--"]):
        print(
            "commit runs the full ReleaseSafe referee. A differential slice is no longer a separate script.",
            file=sys.stderr,
        )
        return 2
    if args.command == "soak" and (args.hours <= 0 or args.rounds is not None and args.rounds <= 0):
        print("soak needs positive --hours and --rounds", file=sys.stderr)
        return 2
    if args.command == "full":
        if not getattr(args, "base", None):
            print(
                "full requires --base PATH, the previously integrated binary the candidate is checked against. "
                "It is recorded in the summary and is not given a default.",
                file=sys.stderr,
            )
            return 2
        if not Path(args.base).expanduser().is_file():
            print(f"--base is not a file: {args.base}", file=sys.stderr)
            return 2
    if args.command == "stage":
        try:
            stages.get(args.name)
        except KeyError:
            print(f"unknown stage {args.name}", file=sys.stderr)
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
