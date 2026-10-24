"""Call a pinned scorecard worktree. Never write inside its sealed run directory."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import shipbar

EMIT = r"""
import json, sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
from bench.engines import defaults, pin_engine
from bench.suites.local import cold_profile, inprocess_profile

spec = json.loads(Path(sys.argv[2]).read_text())
binary = spec["binary"]
suites = spec["suites"].split(",") if spec.get("suites") else None
reps = spec["reps"]
if spec["profile"] == "in-process":
    config = inprocess_profile(binary, spec["library"], suites, reps)
elif spec["profile"] == "cold":
    engines = spec["engines"].split(",")
    config = cold_profile(binary, suites, engines, reps)
    control = spec.get("control")
    if control:
        previous = pin_engine(defaults(control)["zpq"])
        previous["session_policy"] = "fresh process"
        config["engines"]["zpq_prev"] = previous
        for case in config["cases"]:
            if "zpq_prev" not in case["engines"]:
                case["engines"] = list(case["engines"]) + ["zpq_prev"]
else:
    raise SystemExit("profile must be cold or in-process")
config["coverage_note"] = spec["note"]
Path(sys.argv[3]).write_text(json.dumps(config))
print(len(config["cases"]), "cases;", ",".join(config["engines"]), flush=True)
"""


def pin_line(repo: Path) -> tuple[str, str]:
    parts = (repo / "tools/verify/HARNESS").read_text().split()
    if len(parts) != 3 or parts[0] != "forgejo":
        raise SystemExit(f"tools/verify/HARNESS must be: forgejo <branch> <sha>")
    return parts[1], parts[2]


def check_harness(path: Path, sha: str) -> str | None:
    if not (path / "bench/scorecard.py").is_file():
        return f"no bench/scorecard.py under {path}"
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, capture_output=True, text=True, check=True
    ).stdout.strip()
    if head != sha:
        return f"harness HEAD {head} != pin {sha}"
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=path, capture_output=True, text=True, check=True
    ).stdout.strip()
    if dirty:
        return f"harness worktree is dirty:\n{dirty}"
    return None


def _run(argv, cwd, log: Path) -> int:
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as handle:
        completed = subprocess.run(argv, cwd=cwd, stdout=handle, stderr=subprocess.STDOUT)
    print(log.read_text(errors="replace")[-2000:], flush=True)
    return completed.returncode


def run_perf(ctx, book, args) -> None:
    branch, sha = pin_line(ctx.repo)
    harness = Path(args.harness).expanduser()
    problem = check_harness(harness, sha)
    sealed = ctx.run_dir / "scorecard" / args.profile
    record = {
        "name": "perf",
        "status": "blocked",
        "rc": 2,
        "duration_s": None,
        "log": str(ctx.run_dir / "logs" / "perf.log"),
        "counts": {},
        "harness_branch": branch,
        "harness_sha": sha,
        "harness_path": str(harness),
        "sealed": str(sealed),
        "profile": args.profile,
    }
    if problem:
        record["counts"] = {"reason": problem}
        book.add_stage(record)
        print(f"perf blocked  {problem}", flush=True)
        return
    if args.resume and args.plan_only:
        record["counts"] = {"reason": "--resume and --plan-only together do not make sense"}
        book.add_stage(record)
        return
    binary = Path(args.binary).expanduser()
    if not binary.is_file():
        record["counts"] = {"reason": f"missing candidate binary {binary}"}
        book.add_stage(record)
        print(record["counts"]["reason"], flush=True)
        return
    if args.profile == "in-process" and not Path(args.library).expanduser().is_file():
        record["counts"] = {"reason": f"missing --library {args.library}"}
        book.add_stage(record)
        return
    control = None if args.no_control or args.profile != "cold" else str(Path(args.control).expanduser())
    if control and not Path(control).is_file():
        record["counts"] = {"reason": f"missing control binary {control}"}
        book.add_stage(record)
        print(record["counts"]["reason"], flush=True)
        return

    manifest = sealed / "manifest.jsonl"
    if manifest.exists() and not args.resume:
        record["status"] = "fail"
        record["rc"] = 1
        record["counts"] = {
            "reason": "sealed manifest already exists; refusing to reseal. Use --resume or a new run directory."
        }
        book.add_stage(record)
        print(record["counts"]["reason"], flush=True)
        return

    spec = {
        "binary": str(binary),
        "profile": args.profile,
        "library": str(Path(args.library).expanduser()) if args.library else None,
        "suites": args.suites,
        "reps": args.reps,
        "engines": args.engines,
        "control": control,
        "note": f"verify perf {ctx.revision} harness {sha}",
    }
    spec_path = ctx.run_dir / "perf-spec.json"
    config_path = ctx.run_dir / "scorecard-config.json"
    spec_path.write_text(json.dumps(spec, indent=2) + "\n")
    emitter = ctx.run_dir / "emit_config.py"
    emitter.write_text(EMIT)
    if not manifest.exists():
        rc = _run(
            [ctx.python, str(emitter), str(harness), str(spec_path), str(config_path)],
            harness,
            ctx.run_dir / "logs" / "perf-emit.log",
        )
        if rc != 0:
            record["status"] = "fail"
            record["rc"] = rc
            record["counts"] = {"reason": "config emit failed"}
            book.add_stage(record)
            return
        rc = _run(
            [ctx.python, "bench/scorecard.py", "plan", "--config", str(config_path), "--run-dir", str(sealed)],
            harness,
            ctx.run_dir / "logs" / "perf-plan.log",
        )
        if rc != 0:
            record["status"] = "fail"
            record["rc"] = rc
            record["counts"] = {"reason": "scorecard plan failed"}
            book.add_stage(record)
            return
        if args.plan_only:
            record["status"] = "pass"
            record["rc"] = 0
            record["counts"] = {"planned": True}
            book.add_stage(record)
            return
    else:
        print(f"perf resume {sealed}", flush=True)

    rc = _run(
        [ctx.python, "bench/scorecard.py", "run", "--run-dir", str(sealed)],
        harness,
        ctx.run_dir / "logs" / "perf-run.log",
    )
    summary_log = ctx.run_dir / "logs" / "perf-summary.log"
    summary_rc = _run(
        [ctx.python, "bench/scorecard.py", "summary", "--run-dir", str(sealed)],
        harness,
        summary_log,
    )
    if rc != 0 or summary_rc != 0:
        record["status"] = "fail"
        record["rc"] = rc or summary_rc
        record["counts"] = {"reason": "scorecard run or summary failed", "run_rc": rc, "summary_rc": summary_rc}
        book.add_stage(record)
        return
    rows = json.loads(summary_log.read_text())
    (ctx.run_dir / "scorecard-summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    if args.profile != "cold":
        record["status"] = "pass"
        record["rc"] = 0
        record["counts"] = {"rows": len(rows), "shipbar": "skipped; in-process uses publisher_ns, not the cold ship bar"}
        book.add_stage(record)
        return
    cold = [row for row in rows if row.get("lane") == "cold"]
    try:
        scored = shipbar.evaluate(cold)
    except SystemExit as error:
        record["status"] = "fail"
        record["rc"] = 1
        record["counts"] = {"reason": str(error), "cold_rows": len(cold)}
        book.add_stage(record)
        return
    text = shipbar.render(scored)
    (ctx.run_dir / "shipbar.txt").write_text(text)
    print(text, flush=True)
    record["status"] = "pass" if scored["met"] else "fail"
    record["rc"] = 0 if scored["met"] else 1
    record["counts"] = {
        "cells": scored["n"],
        "geomean": round(scored["geomean"], 3),
        "behind": scored["behind"],
        "shipbar": "MET" if scored["met"] else "NOT MET",
        "control_geomean": None if not scored["control"] else round(scored["control"]["geomean"], 3),
    }
    book.add_stage(record)
