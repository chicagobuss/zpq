"""Stage registry. Each stage builds one command, or a reason it cannot run."""

from __future__ import annotations

import ast
import json
import platform
import re
import shlex
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Ctx:
    repo: Path
    python: str
    zigw: str
    run_dir: Path
    j: int
    cpu: str
    revision: str
    binary: Path
    lib: Path
    clickbench_root: Path
    clickbench_sql: Path
    skspill: Path
    sigpipe: Path
    duckdb: str
    arch_summaries: tuple[str, ...] = ()


@dataclass
class Cmd:
    argv: list[str]
    env: dict
    about: str
    blocked: str | None = None
    expect: str | None = None
    expect_rc: int = 1
    expect_text: str = ""
    parse: object | None = None
    expected_skips: dict[str, str] = field(default_factory=dict)
    optimize: str | None = None
    # Every stage ends: past this the runner kills it and fails it by name.
    timeout_s: float = 3600


def _last(pattern, text):
    found = None
    for found in re.finditer(pattern, text):
        pass
    return found


def parse_tests(text):
    match = _last(r"(\d+)/(\d+) tests passed(?: \((\d+) skipped\))?", text)
    if not match:
        return {}
    counts = {"passed": int(match.group(1)), "total": int(match.group(2))}
    counts["skipped"] = int(match.group(3) or 0)
    counts["skip_details"] = [json.loads(m.group(1)) for m in re.finditer(r"ZPQ_TEST_SKIP (\{.*\})", text)]
    return counts


def parse_referee(text):
    found = re.search(r"check: (\d+)/(\d+) checks match", text)
    counts = {}
    if found:
        counts["matched"] = int(found.group(1))
        counts["total"] = int(found.group(2))
    for field, label in (("expected", "expected"), ("refusals", "refusals"),
                         ("refusal_declarations", "refusal declarations")):
        match = re.search(rf"check: {label} (\d+)", text)
        if match:
            counts[field] = int(match.group(1))
    counts["limited"] = "check: limited run" in text
    return counts


def parse_path_parity(text):
    match = _last(r"PATH_PARITY_SUMMARY (\{.*\})", text)
    return json.loads(match.group(1)) if match else {}


def parse_modules(text):
    match = _last(r"test-module coverage: (\d+) modules, (\d+) tests", text)
    if not match:
        return {}
    return {"modules": int(match.group(1)), "tests": int(match.group(2))}


def parse_contracts(text):
    match = _last(r"contracts: (\d+) passed, (\d+) skipped by named need, (\d+) failed", text)
    if not match:
        return {}
    return {"matched": int(match.group(1)), "total": int(match.group(1)) + int(match.group(3)),
            "skipped": int(match.group(2)), "fail": int(match.group(3))}


def parse_release(text):
    runtime = [line.strip() for line in text.splitlines()
               if line.strip().startswith(("RAN ", "VERIFIED ", "NOT RUN "))]
    accepted = "NOT RUN x86_64-macos (deferred; accepted by name)"
    blocked = any(line.startswith("NOT RUN ") and line != accepted for line in runtime)
    blocked = blocked or "elsewhere:" in text
    return {"runtime": runtime, "blocked": blocked}


def _need(path: Path, what: str) -> str | None:
    if path.exists():
        return None
    return f"missing {what}: {path}"


def _base_env(ctx: Ctx) -> dict:
    import os

    env = dict(os.environ)
    env["PYTHON"] = ctx.python
    env["ZIG"] = ctx.zigw
    env["ZPQ_ZIG"] = env.get("ZPQ_ZIG", "zig")
    env["DUCKDB"] = ctx.duckdb
    env["CLICKBENCH_SQL_DIR"] = str(ctx.clickbench_sql)
    return env


def _zig_build(ctx: Ctx, *args: str) -> list[str]:
    return [ctx.zigw, "build", f"-j{ctx.j}", *args]


def _python(ctx: Ctx, script: str, about: str, *args, needs=(), parse=None) -> Cmd:
    blocked = next((reason for path, label in needs if (reason := _need(path, label))), None)
    return Cmd([ctx.python, script, *map(str, args)], _base_env(ctx), about, blocked=blocked, parse=parse)


def stage_test_core(ctx: Ctx) -> Cmd:
    return Cmd(_zig_build(ctx, "test-core", "-Doptimize=ReleaseSafe", "-Dobserve-tests=true", "--summary", "all"),
               _base_env(ctx), "ReleaseSafe core unit tests (all tests, unchanged scales)",
               parse=parse_tests, optimize="ReleaseSafe")


def stage_build_fast(ctx: Ctx) -> Cmd:
    return Cmd(
        _zig_build(
            ctx,
            f"-Doptimize=ReleaseFast",
            f"-Drevision={ctx.revision}",
            f"-Dcpu={ctx.cpu}",
        ),
        _base_env(ctx),
        "ReleaseFast build stamped with HEAD",
    )


def stage_test_full(ctx: Ctx) -> Cmd:
    return Cmd(_zig_build(ctx, "test", "-Doptimize=Debug", "-Dobserve-tests=true", "--summary", "all"),
               _base_env(ctx), "Debug unit tests", parse=parse_tests, optimize="Debug")


def stage_integration(ctx: Ctx) -> Cmd:
    return Cmd(
        _zig_build(ctx, "test-integration", "-Dobserve-tests=true", "--summary", "line"),
        _base_env(ctx),
        "Lambda integration tests",
        parse=parse_tests,
    )



def stage_canonical(ctx: Ctx) -> Cmd:
    argv = ["zig-out/bin/zpq-check", "--cases", "tools/check/cases/decode_errors.json",
            "--cases", "tools/check/cases/hardware.json", "--zpq", str(ctx.binary),
            "--no-cache", "--duckdb", ctx.duckdb, "--python", ctx.python, "--work", str(ctx.run_dir / "decode-work")]
    return _check(ctx, "canonical decode errors and hardware-size byte parity at every point", argv, parse_referee)



def stage_clickbench(ctx: Ctx) -> Cmd:
    argv = ["zig-out/bin/zpq-check", "--cases", "tools/check/cases/clickbench.json", "--zpq", str(ctx.binary),
            "--data", str(ctx.clickbench_root), "--no-cache", "--duckdb", ctx.duckdb, "--python", ctx.python,
            "--work", str(ctx.run_dir / "clickbench-work")]
    cmd = _check(ctx, "ClickBench's 43 queries on 1 and 16 partitions against DuckDB", argv, parse_referee)
    cmd.blocked = cmd.blocked or _need(ctx.clickbench_root / "sub16", "ClickBench partitions")
    cmd.timeout_s = 7200
    return cmd


def stage_skspill(ctx: Ctx) -> Cmd:
    sql = f"SELECT s, count(*) c FROM '{ctx.skspill}' GROUP BY s"
    return Cmd(
        [str(ctx.binary), "query", "--query", sql, "-j", "8", "--max-memory", "500MB"],
        _base_env(ctx),
        "spill under 500MB is a declared refusal",
        blocked=_need(ctx.binary, "candidate binary") or _need(ctx.skspill, "skspill fixture"),
        expect="refusal",
        expect_rc=1,
        expect_text="the query needs more memory than --max-memory allows",
    )


def stage_libzpq(ctx: Ctx) -> Cmd:
    env = _base_env(ctx)
    env["OPTIMIZE"] = "ReleaseFast"
    return Cmd(["bash", "tests/libzpq/run.sh"], env, "C ABI, ASan, Python, SQL session, RSS, write-sync")


def stage_sigpipe(ctx: Ctx) -> Cmd:
    return Cmd(
        [ctx.python, str(ctx.sigpipe), str(ctx.lib), "x"],
        _base_env(ctx),
        "libzpq keeps SIGPIPE ignored across open/close",
        blocked=_need(ctx.lib, "libzpq.so") or _need(ctx.sigpipe, "sigpipe.py"),
    )


def stage_contracts(ctx: Ctx) -> Cmd:
    return Cmd(
        [ctx.python, "tools/check/contracts.py", str(ctx.binary), "--lib", str(ctx.lib)],
        _base_env(ctx),
        "OS-level contracts: publication, permissions, pipes, S3 uploads, sync and write faults",
        parse=parse_contracts,
        blocked=_need(ctx.binary, "candidate binary") or _need(ctx.lib, "libzpq.so"),
    )


def stage_release_smoke(ctx: Ctx) -> Cmd:
    return Cmd(
        [
            "tools/release_smoke.sh", "--accept-deferred", "x86_64-macos",
            *(arg for path in ctx.arch_summaries for arg in ("--arch-summary", path)),
            str(ctx.run_dir / "release-smoke"),
        ],
        _base_env(ctx),
        "native CLI/Lambda plus same-SHA arch evidence; Intel macOS deferred by name",
        parse=parse_release,
    )


def stage_modules(ctx: Ctx) -> Cmd:
    return _python(ctx, "tools/check_test_modules.py", "test-module coverage", parse=parse_modules)


# Case files whose CLI sweeps belong to the full tier only.
FULL_CASES = ("corpus.json", "decode_errors.json", "hardware.json", "clickbench.json")
CHECK_CASES = sorted(str(p.relative_to(Path(__file__).resolve().parents[2]))
                     for p in (Path(__file__).resolve().parents[2] / "tools/check/cases").glob("*.json")
                     if p.name not in FULL_CASES)


def referee_argv(ctx: Ctx, cached: bool = True) -> list[str]:
    argv = ["zig-out/bin/zpq-check", "--zpq", str(ctx.binary)]
    for path in CHECK_CASES:
        argv.extend(["--cases", path])
    # A stable work directory keeps fixture paths, and so the oracle cache keys that name them, the same across runs.
    argv.extend(["--duckdb", str(Path(ctx.duckdb)), "--python", ctx.python,
                 "--work", str(ctx.repo / "zig-out/check-work")])
    if not cached:
        argv.append("--no-cache")
    return argv


def _check(ctx: Ctx, about: str, argv: list[str], parse=None) -> Cmd:
    shell = shlex.join(_zig_build(ctx, "-Doptimize=ReleaseSafe", "check")) + " && " + shlex.join(argv)
    return Cmd(["bash", "-c", shell], _base_env(ctx), about, blocked=_need(Path(ctx.duckdb), "DuckDB 2.0 CLI"),
               parse=parse)


def stage_referee(ctx: Ctx) -> Cmd:
    return _check(ctx, "zpq-check against cached DuckDB 2.0 answers", referee_argv(ctx), parse_referee)


def stage_referee_fresh(ctx: Ctx) -> Cmd:
    return _check(ctx, "zpq-check against fresh DuckDB 2.0 answers", referee_argv(ctx, cached=False), parse_referee)


def stage_check_self(ctx: Ctx) -> Cmd:
    argv = ["zig-out/bin/zpq-check", "--self-test", "--no-cache", "--duckdb", ctx.duckdb, "--python", ctx.python,
            "--work", str(ctx.run_dir / "check-self")]
    return _check(ctx, "zpq-check rejects the wrong answers and refusals it must", argv)


def stage_path_parity(ctx: Ctx) -> Cmd:
    shell = shlex.join(_zig_build(ctx, "-Doptimize=ReleaseSafe", "check"))
    shell += " && " + shlex.join([
        ctx.python, "tools/verify/path_parity.py", "--check", str(ctx.repo / "zig-out/bin/zpq-check"),
        "--duckdb", ctx.duckdb, "--out", str(ctx.run_dir / "path-parity"),
    ])
    return Cmd(["bash", "-c", shell], _base_env(ctx), "JSON/Parquet parity across in-process execution overrides",
               blocked=_need(Path(ctx.duckdb), "DuckDB 2.0 CLI"), parse=parse_path_parity)


def stage_corpus(ctx: Ctx) -> Cmd:
    argv = ["zig-out/bin/zpq-check", "--cases", "tools/check/cases/corpus.json", "--zpq", str(ctx.binary), "--no-cache",
            "--duckdb", ctx.duckdb, "--python", ctx.python, "--work", str(ctx.run_dir / "corpus-work")]
    return _check(ctx, "every apache/parquet-testing file against its declared outcome", argv, parse_referee)


# name, tiers, builder. Order inside a tier follows this list.
REGISTRY = [
    ("modules", ("full",), stage_modules),
    ("test-core", ("quick", "commit"), stage_test_core),
    ("build-fast", ("commit", "full"), stage_build_fast),
    ("test-full", ("full",), stage_test_full),
    ("integration", ("full",), stage_integration),
    ("check-self", ("commit", "full"), stage_check_self),
    ("differential", ("commit",), stage_referee),
    ("differential-fresh", ("full",), stage_referee_fresh),
    ("path-parity", ("full",), stage_path_parity),
    ("canonical", ("full",), stage_canonical),
    ("clickbench", ("full",), stage_clickbench),
    ("repro-skspill", ("full",), stage_skspill),
    ("libzpq", ("full",), stage_libzpq),
    ("repro-sigpipe", ("full",), stage_sigpipe),
    ("contracts", ("full",), stage_contracts),
    ("release-smoke", ("full",), stage_release_smoke),
    ("corpus", ("full",), stage_corpus),
]


def names_for(tier: str) -> list[str]:
    return [name for name, tiers, _builder in REGISTRY if tier in tiers]


def get(name: str):
    name = "differential" if name == "referee" else name
    for item in REGISTRY:
        if item[0] == name:
            return item
    raise KeyError(name)


def describe(name: str, ctx: Ctx) -> Cmd:
    _name, _tiers, builder = get(name)
    command = builder(ctx)
    if command.parse is parse_tests:
        command.expected_skips = skip_allowances(ctx.repo, name)
    return command


def cpu_flags() -> set[str] | None:
    if platform.machine().lower() not in ("x86_64", "amd64", "i386", "i686"):
        return set()
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("flags"):
                return set(line.split(":", 1)[1].split())
    except OSError:
        pass
    return None


def skip_allowances(repo: Path, stage: str) -> dict[str, str]:
    policy = json.loads((repo / "tools/verify/skip_policy.json").read_text())
    allowed = {}
    for waiver in policy["waivers"]:
        missing, flags = waiver.get("missing", []), waiver.get("missing_cpu_flags", [])
        if not waiver["reason"] or not (missing or flags):
            raise ValueError("skip waiver needs a reason and a verifiable condition")
        if stage not in waiver["stages"] or any((repo / p).exists() for p in missing):
            continue
        if flags:
            available = cpu_flags()
            if available is None or any(flag in available for flag in flags):
                continue
        if waiver["test"] in allowed:
            raise ValueError("duplicate skip waiver")
        allowed[waiver["test"]] = waiver["reason"]
    return allowed


def tier_skip_budgets(repo: Path) -> dict:
    policy = json.loads((repo / "tools/verify/skip_policy.json").read_text())
    return {tier: {stage: len(skip_allowances(repo, stage)) for stage in names}
            for tier, names in policy["tiers"].items()}
