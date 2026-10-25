"""Call a pinned scorecard worktree. Never write inside its sealed run directory."""

from __future__ import annotations

import fcntl
from contextlib import ExitStack
import json
import os
import subprocess
import time
from pathlib import Path

import shipbar


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


def _public_skip(row: dict) -> dict:
    return {key: row[key] for key in ("suite", "reason", "missing", "unmatched") if key in row}


def _suite_counts(report: dict) -> dict:
    rows = report.get("suites") or []
    skipped = [row for row in rows if row.get("status") == "skipped"]
    return {
        "suites_requested": report.get("suites_requested") or [],
        "cases_requested": report.get("cases_requested"),
        "suites_ran": [row["suite"] for row in rows if row.get("status") == "ran"],
        "suites_skipped": [
            _public_skip(row) for row in skipped if row.get("reason") != "excluded by case filter"
        ],
        "suites_filtered": [
            _public_skip(row) for row in skipped if row.get("reason") == "excluded by case filter"
        ],
    }


def _finish_status(report: dict, planned: bool = False, met: bool | None = None, extra: dict | None = None) -> dict:
    counts = _suite_counts(report)
    if extra:
        counts.update(extra)
    if planned:
        counts["planned"] = True
    if not counts["suites_ran"]:
        return {"status": "blocked", "rc": 2, "counts": counts}
    if met is False:
        return {"status": "fail", "rc": 1, "counts": counts}
    if counts["suites_skipped"]:
        return {"status": "blocked", "rc": 2, "counts": counts}
    return {"status": "pass", "rc": 0, "counts": counts}


def _run(argv, cwd, log: Path, timing_lock: bool = False) -> int:
    """Hold the timing lock before changing host state; fd 9 is the harness protocol."""
    log.parent.mkdir(parents=True, exist_ok=True)
    passed = {}
    with ExitStack() as cleanup:
        if timing_lock:
            lock_path = Path.home() / ".cache/zpq-bench.lock"
            lock_file = cleanup.enter_context(lock_path.open("a+"))
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            cleanup.callback(fcntl.flock, lock_file, fcntl.LOCK_UN)
            previous = subprocess.check_output(["powerprofilesctl", "get"], text=True, timeout=10).strip()
            cleanup.callback(subprocess.run, ["sudo", "-n", "powerprofilesctl", "set", previous],
                             check=False, timeout=15)
            subprocess.run(["sudo", "-n", "powerprofilesctl", "set", "performance"], check=True, timeout=15)
            if lock_file.fileno() != 9:
                os.dup2(lock_file.fileno(), 9)
                cleanup.callback(os.close, 9)
            passed["pass_fds"] = (9,)
        with log.open("w") as handle:
            completed = subprocess.run(argv, cwd=cwd, stdout=handle, stderr=subprocess.STDOUT, **passed)
    print(log.read_text(errors="replace")[-2000:], flush=True)
    return completed.returncode


def run_perf(ctx, book, args) -> None:
    branch, sha = pin_line(ctx.repo)
    harness = Path(args.harness).expanduser()
    problem = check_harness(harness, sha)
    source_run = Path(args.resume).expanduser().resolve() if args.resume else ctx.run_dir
    sealed = source_run / "scorecard" / args.profile
    started = time.monotonic()

    def commit(record: dict) -> None:
        record["duration_s"] = round(time.monotonic() - started, 3)
        book.add_stage(record)
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
        "resumed_from": str(source_run) if args.resume else None,
    }
    if problem:
        record["counts"] = {"reason": problem}
        commit(record)
        print(f"perf blocked  {problem}", flush=True)
        return
    if args.resume and args.plan_only:
        record["counts"] = {"reason": "--resume and --plan-only together do not make sense"}
        commit(record)
        return
    if args.resume:
        if not (sealed / "manifest.jsonl").is_file():
            record["counts"] = {"reason": f"no sealed manifest under {source_run}"}
            commit(record)
            return
        original = json.loads((source_run / "perf-spec.json").read_text())
        args.binary, args.library = original["binary"], original.get("library")
        args.control, args.no_control = original.get("control"), not original.get("control")
    binary = Path(args.binary).expanduser()
    if not binary.is_file():
        record["counts"] = {"reason": f"missing candidate binary {binary}"}
        commit(record)
        print(record["counts"]["reason"], flush=True)
        return
    if args.profile == "in-process" and not Path(args.library).expanduser().is_file():
        record["counts"] = {"reason": f"missing --library {args.library}"}
        commit(record)
        return
    control = None if args.no_control or args.profile != "cold" else str(Path(args.control).expanduser())
    if control and not Path(control).is_file():
        record["counts"] = {"reason": f"missing control binary {control}"}
        commit(record)
        print(record["counts"]["reason"], flush=True)
        return

    manifest = sealed / "manifest.jsonl"
    if manifest.exists() and not args.resume:
        record["status"] = "fail"
        record["rc"] = 1
        record["counts"] = {
            "reason": "sealed manifest already exists; refusing to reseal. Use --resume or a new run directory."
        }
        commit(record)
        print(record["counts"]["reason"], flush=True)
        return

    spec = {
        "binary": str(binary),
        "profile": args.profile,
        "library": str(Path(args.library).expanduser()) if args.library else None,
        "suites": args.suites,
        "cases": args.cases,
        "reps": args.reps,
        "engines": args.engines,
        "control": control,
        "note": f"verify perf {ctx.revision} harness {sha}",
    }
    spec_path = ctx.run_dir / "perf-spec.json"
    config_path = ctx.run_dir / "scorecard-config.json"
    report_path = source_run / "suite-filter.json"
    spec_path.write_text(json.dumps(spec, indent=2) + "\n")
    if not manifest.exists():
        rc = _run(
            [
                ctx.python,
                str(ctx.repo / "tools/verify/emit_config.py"),
                str(harness),
                str(spec_path),
                str(config_path),
                str(report_path),
            ],
            harness,
            ctx.run_dir / "logs" / "perf-emit.log",
        )
        report = json.loads(report_path.read_text()) if report_path.is_file() else {}
        record["counts"] = _suite_counts(report)
        if rc == 2:
            record["counts"]["reason"] = "suite filter names a suite this profile does not have"
            commit(record)
            return
        if rc == 3:
            record["counts"]["reason"] = "every requested suite was skipped"
            commit(record)
            return
        if rc != 0:
            record["status"] = "fail"
            record["rc"] = rc
            record["counts"]["reason"] = "config emit failed"
            commit(record)
            return
        rc = _run(
            [ctx.python, "bench/scorecard.py", "plan", "--config", str(config_path), "--run-dir", str(sealed)],
            harness,
            ctx.run_dir / "logs" / "perf-plan.log",
        )
        if rc != 0:
            record["status"] = "fail"
            record["rc"] = rc
            record["counts"]["reason"] = "scorecard plan failed"
            commit(record)
            return
        if args.plan_only:
            record.update(_finish_status(report, planned=True))
            commit(record)
            return
    else:
        print(f"perf resume {sealed}", flush=True)
        report = json.loads(report_path.read_text()) if report_path.is_file() else {}
        record["counts"] = _suite_counts(report)

    record["counts"]["power_profile"] = "performance"
    record["counts"]["power_profile_via"] = "configured host profile controller"
    record["counts"]["timing_lock"] = "fd 9 held by verify, which is how the harness skips its own switch"
    rc = _run(
        [ctx.python, "bench/scorecard.py", "run", "--run-dir", str(sealed)],
        harness,
        ctx.run_dir / "logs" / "perf-run.log",
        timing_lock=True,
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
        record["counts"]["reason"] = "scorecard run or summary failed"
        record["counts"]["run_rc"] = rc
        record["counts"]["summary_rc"] = summary_rc
        commit(record)
        return
    rows = json.loads(summary_log.read_text())
    (ctx.run_dir / "scorecard-summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    runtime = {
        key: record["counts"][key]
        for key in ("power_profile", "power_profile_via", "timing_lock")
        if key in record["counts"]
    }
    if args.profile != "cold":
        finished = _finish_status(
            report,
            extra={"rows": len(rows), "shipbar": "not applied; in-process is a separate metric", **runtime},
        )
        record.update(finished)
        commit(record)
        return
    cold = [row for row in rows if row.get("lane") == "cold"]
    try:
        scored = shipbar.evaluate(cold)
    except SystemExit as error:
        record["status"] = "fail"
        record["rc"] = 1
        record["counts"]["reason"] = str(error)
        record["counts"]["cold_rows"] = len(cold)
        commit(record)
        return
    text = shipbar.render(scored)
    (ctx.run_dir / "shipbar.txt").write_text(text)
    print(text, flush=True)
    record.update(
        _finish_status(
            report,
            met=scored["met"],
            extra={
                "cells": scored["n"],
                "geomean": round(scored["geomean"], 3),
                "behind": scored["behind"],
                "shipbar": "MET" if scored["met"] else "NOT MET",
                "control_geomean": None if not scored["control"] else round(scored["control"]["geomean"], 3),
                **runtime,
            },
        )
    )
    commit(record)
