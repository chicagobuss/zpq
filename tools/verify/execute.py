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
            "MemoryMax=20G",
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
    if cmd.expect == "refusal":
        declared = rc == cmd.expect_rc and cmd.expect_text in text
        return ("expected" if declared else "fail"), counts
    if counts.get("fail"):
        return "fail", counts
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
    rc, duration = launch(cmd.argv, cmd.env, ctx.repo, log_path, heavy)
    text = log_path.read_text(errors="replace")
    status, counts = judge(cmd, rc, text)
    record = _record(name, status, rc, duration, log_path, counts, cmd)
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
    }
    if cmd.expect:
        record["expect"] = cmd.expect
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
            ("differential", [ctx.python, "tools/differential.py"]),
            ("unit", [ctx.zigw, "build", "test", "--summary", "line", f"-j{ctx.j}"]),
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
            if rc != 0:
                failures += 1
                shutil.copy(log_path, fail_dir / f"r{done}-{name}-{seed}.log")
            print(f"soak {name} rc={rc} {duration:.0f}s", flush=True)
        shutil.rmtree(scratch, ignore_errors=True)
    book.add_stage(
        {
            "name": "soak",
            "status": "pass" if failures == 0 else "fail",
            "rc": 0 if failures == 0 else 1,
            "duration_s": None,
            "log": str(ctx.run_dir / "logs"),
            "counts": {"rounds": done, "failures": failures},
        }
    )
