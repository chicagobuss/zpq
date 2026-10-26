"""Run registered stages, keep each log, and append them to the summary."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import time
from pathlib import Path

import stages
import summary as summary_mod


def file_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def launch(argv: list[str], env: dict, cwd: Path, log_path: Path, heavy: bool) -> tuple[int, float]:
    if heavy:
        argv = [
            "systemd-run",
            "--user",
            "--scope",
            "-p",
            "MemoryMax=16G",
            "-q",
            "--",
            "nice",
            "-n",
            "10",
            *argv,
        ]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with log_path.open("w") as log:
        completed = subprocess.run(argv, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
    return completed.returncode, time.monotonic() - started


def judge(cmd: stages.Cmd, rc: int, text: str) -> tuple[str, dict]:
    counts = cmd.parse(text) if cmd.parse else {}
    if cmd.parse in (stages.parse_referee, stages.parse_cli_contracts):
        if not counts.get("total") or counts.get("matched") != counts.get("total"):
            return "fail", counts
    if cmd.parse is stages.parse_tests:
        total, skipped = counts.get("total", 0), counts.get("skipped", 0)
        if total <= 0 or counts.get("passed", -1) + skipped != total:
            return "fail", counts
        details = counts.get("skip_details", [])
        counts["expected_skip_budget"] = len(cmd.expected_skips)
        seen = set()
        for item in details:
            name = item["test"]
            if name in seen or name not in cmd.expected_skips:
                counts["unexpected_skip"] = name
                return "blocked", counts
            seen.add(name)
            item["reason"] = cmd.expected_skips[name]
        if len(details) != skipped:
            counts["skip_accounting_error"] = "runner did not name every skipped test"
            return "blocked", counts
    if cmd.expect == "refusal":
        declared = rc == cmd.expect_rc and cmd.expect_text in text
        return ("expected" if declared else "fail"), counts
    if cmd.parse is stages.parse_path_parity:
        verified, unverified = counts.get("verified", 0), counts.get("unverified", 0)
        if rc != 0 or verified + unverified == 0 or counts.get("format_checks") != 2 * (verified + unverified):
            return "fail", counts
        if len(counts.get("unverified_knobs", [])) != unverified:
            return "fail", counts
        return ("unverified" if unverified else "pass"), counts
    if counts.get("fail"):
        return "fail", counts
    if counts.get("limited"):
        return "blocked", counts
    if rc == 0:
        return "pass", counts
    return "fail", counts


def run_stage(ctx: stages.Ctx, name: str, heavy: bool) -> dict:
    cmd = stages.describe(name, ctx)
    log_path = ctx.run_dir / "logs" / f"{name}.log"
    print(f"== {name}  {cmd.about}", flush=True)
    if cmd.blocked:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(cmd.blocked + "\n")
        record = _record(name, "blocked", 2, 0.0, log_path, {}, cmd)
        print(f"{name} blocked  {cmd.blocked}", flush=True)
        return record
    try:
        rc, duration = launch(cmd.argv, cmd.env, ctx.repo, log_path, heavy)
    except (OSError, subprocess.SubprocessError) as error:
        log_path.write_text(f"could not run stage: {error}\n")
        return _record(name, "fail", 1, 0.0, log_path, {"error": str(error)}, cmd)
    text = log_path.read_text(errors="replace")
    status, counts = judge(cmd, rc, text)
    record = _record(name, status, rc, duration, log_path, counts, cmd)
    record["binary_sha256"] = file_sha256(ctx.binary)
    if name in ("differential", "referee"):
        record["referee_sha256"] = file_sha256(ctx.repo / "zig-out/bin/referee")
    if name == "fd" and ctx.base:
        record["base"] = str(Path(ctx.base).expanduser().resolve())
    print(f"{name} {status} rc={rc} {duration:.0f}s", flush=True)
    return record


def _record(name, status, rc, duration, log_path, counts, cmd: stages.Cmd) -> dict:
    record = {
        "name": name,
        "status": status,
        "rc": rc,
        "duration_s": round(duration, 1),
        "log": str(log_path),
        "counts": counts,
        "command": cmd.argv,
    }
    if cmd.optimize:
        record["optimize"] = cmd.optimize
    if cmd.expect:
        record["expect"] = cmd.expect
        record["expect_rc"] = cmd.expect_rc
        record["expect_text"] = cmd.expect_text
    return record


def run_named(ctx: stages.Ctx, names: list[str], book: summary_mod.Summary, heavy: bool) -> None:
    for name in names:
        book.add_stage(run_stage(ctx, name, heavy))
        if name == "build-fast":
            book.set_binary(file_sha256(ctx.binary))


def run_soak(ctx: stages.Ctx, book: summary_mod.Summary, heavy: bool, hours: float, rounds: int | None) -> None:
    built = run_stage(ctx, "build-fast", heavy)
    book.add_stage(built)
    book.set_binary(file_sha256(ctx.binary))
    if built["status"] != "pass":
        return
    fail_dir = ctx.run_dir / "fail"
    fail_dir.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + hours * 3600
    done = 0
    failures = 0
    ref_log = ctx.run_dir / "logs" / "soak-referee-build.log"
    print("== soak referee build", flush=True)
    ref_rc, ref_duration = launch(
        stages._zig_build(ctx, "-Doptimize=ReleaseSafe", "referee"),
        stages._base_env(ctx),
        ctx.repo,
        ref_log,
        heavy,
    )
    print(f"soak referee build rc={ref_rc} {ref_duration:.0f}s", flush=True)
    book.add_stage({"name": "soak-referee-build", "status": "pass" if ref_rc == 0 else "fail",
                    "rc": ref_rc, "duration_s": round(ref_duration, 1), "log": str(ref_log), "counts": {}})
    if ref_rc != 0:
        shutil.copy(ref_log, fail_dir / "referee-build.log")
        return
    while True:
        if rounds is not None and done >= rounds:
            break
        if time.time() >= deadline:
            break
        done += 1
        seed = int.from_bytes(os.urandom(4), "little")
        scratch = ctx.run_dir / "tmp" / f"agg-{seed}"
        jobs = [
            (
                "aggfuzz",
                [
                    ctx.python,
                    "tools/agg_ab_fuzz.py",
                    "--queries",
                    "600",
                    "--seed",
                    str(seed),
                    "--dir",
                    str(scratch),
                ],
            ),
            ("differential", stages.referee_argv(ctx)),
            ("unit", stages.describe("test-full", ctx).argv),
            (
                "fuzz",
                [
                    "timeout",
                    "1800",
                    ctx.zigw,
                    "build",
                    "fuzz",
                    "-Dtest-filter=fuzz:",
                    "--fuzz=300000",
                    f"-j{ctx.j}",
                ],
            ),
        ]
        for name, argv in jobs:
            log_path = ctx.run_dir / "logs" / f"soak-r{done}-{name}.log"
            print(f"== soak round {done} {name} seed={seed}", flush=True)
            rc, duration = launch(argv, stages._base_env(ctx), ctx.repo, log_path, heavy)
            command = stages.describe("test-full", ctx) if name == "unit" else stages.Cmd(
                argv, {}, name, parse=stages.parse_referee if name == "differential" else None)
            status, counts = judge(command, rc, log_path.read_text())
            book.add_stage({"name": f"soak-r{done}-{name}", "status": status,
                            "rc": rc, "duration_s": round(duration, 1), "log": str(log_path),
                            "counts": counts, "round": done, "seed": seed, "command": argv})
            if status != "pass":
                failures += 1
                shutil.copy(log_path, fail_dir / f"r{done}-{name}-{seed}.log")
            print(f"soak {name} rc={rc} {duration:.0f}s", flush=True)
        shutil.rmtree(scratch, ignore_errors=True)
    book.add_stage(
        {
            "name": "soak",
            "status": "pass" if failures == 0 else "fail",
            "rc": 0 if failures == 0 else 1,
            "duration_s": round(hours * 3600 - (deadline - time.time()), 1),
            "log": str(ctx.run_dir / "logs"),
            "counts": {"rounds": done, "failures": failures},
        }
    )
