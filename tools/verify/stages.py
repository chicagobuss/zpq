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
    base: str | None
    clickbench_root: Path
    clickbench_sql: Path
    skspill: Path
    sigpipe: Path
    duckdb: str


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
    found = re.search(r"referee: (\d+)/(\d+) checks match", text)
    counts = {}
    if found:
        counts["matched"] = int(found.group(1))
        counts["total"] = int(found.group(2))
    for field, label in (("expected", "expected"), ("refusals", "refusals"),
                         ("refusal_declarations", "refusal declarations")):
        match = re.search(rf"referee: {label} (\d+)", text)
        if match:
            counts[field] = int(match.group(1))
    counts["limited"] = "referee: limited run" in text
    return counts


def parse_path_parity(text):
    match = _last(r"PATH_PARITY_SUMMARY (\{.*\})", text)
    return json.loads(match.group(1)) if match else {}


def parse_modules(text):
    match = _last(r"test-module coverage: (\d+) modules, (\d+) tests", text)
    if not match:
        return {}
    return {"modules": int(match.group(1)), "tests": int(match.group(2))}


def parse_cli_contracts(text):
    match = _last(r"CLI contract checks passed \((\d+) invocations;", text)
    if not match:
        return {}
    return {"matched": int(match.group(1)), "total": int(match.group(1))}


def parse_fd(text):
    match = _last(r"dependent group keys: (\d+) exact checks", text)
    if not match:
        return {}
    return {"checks": int(match.group(1))}


def parse_canonical(text):
    match = _last(r"canonical decode errors: (\d+) byte-identical failures", text)
    if not match:
        return {}
    return {"checks": int(match.group(1))}


def parse_clickbench(text):
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                raw = ast.literal_eval(line)
            except (SyntaxError, ValueError):
                continue
            if isinstance(raw, dict):
                counts = {str(key).lower(): value for key, value in raw.items()}
                if "fail" in counts:
                    counts["fail"] = int(counts["fail"])
                return counts
    return {}


def parse_release(text):
    runtime = [
        line.strip()
        for line in text.splitlines()
        if line.strip().startswith("RAN") or "elsewhere: verify arch" in line
    ]
    return {"runtime": runtime} if runtime else {}


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



def stage_fd(ctx: Ctx) -> Cmd:
    blocked = _need(ctx.binary, "candidate binary")
    if not ctx.base:
        blocked = "fd needs --base PATH or --base-rev SHA for comparison"
    elif not Path(ctx.base).exists():
        blocked = f"missing --base binary: {ctx.base}"
    return Cmd(
        [ctx.python, "tools/group_dependency_check.py", str(ctx.binary), ctx.base or ""],
        _base_env(ctx),
        "group-dependency exactness against the required comparison binary",
        blocked=blocked,
        parse=parse_fd,
    )


def stage_canonical(ctx: Ctx) -> Cmd:
    return _python(ctx, "tools/decode_error_check.py", "canonical error precedence", ctx.binary,
                   needs=((ctx.binary, "candidate binary"),), parse=parse_canonical)


def stage_cli_smoke(ctx: Ctx) -> Cmd:
    return Cmd(
        ["tools/cli_smoke.sh", str(ctx.binary)],
        _base_env(ctx),
        "CLI contracts and PyArrow/DuckDB readback",
        parse=parse_cli_contracts,
        blocked=_need(ctx.binary, "candidate binary"),
    )


def stage_duckdb_smoke(ctx: Ctx) -> Cmd:
    env = _base_env(ctx)
    env["ZPQ"] = str(ctx.binary)
    blocked = _need(ctx.binary, "candidate binary") or _need(Path(ctx.duckdb), "DuckDB 2.0 CLI")
    return Cmd(
        ["tools/duckdb_smoke.sh"],
        env,
        "DuckDB writes, zpq reads",
        blocked=blocked,
    )


def _clickbench(ctx: Ctx, name: str, glob: str) -> Cmd:
    sql = ctx.clickbench_sql / "queries.sql"
    blocked = _need(ctx.binary, "candidate binary") or _need(sql, "ClickBench SQL")
    parquet = str(ctx.clickbench_root / glob)
    return Cmd(
        [
            ctx.python,
            "tools/clickbench_check.py",
            str(ctx.binary),
            parquet,
            str(ctx.repo),
            str(ctx.run_dir / f"{name}.json"),
        ],
        _base_env(ctx),
        f"ClickBench smoke on {glob}",
        blocked=blocked,
        parse=parse_clickbench,
    )


def stage_cb_sub1(ctx: Ctx) -> Cmd:
    return _clickbench(ctx, "cb-sub1", "sub1/*.parquet")


def stage_cb_sub16(ctx: Ctx) -> Cmd:
    return _clickbench(ctx, "cb-sub16", "sub16/*.parquet")


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


def stage_write_robustness(ctx: Ctx) -> Cmd:
    env = _base_env(ctx)
    env["ZPQ_LIB"] = str(ctx.lib)
    return Cmd(
        [ctx.python, "tools/write_robustness_check.py", str(ctx.binary)],
        env,
        "interrupted writes and sync failures",
        blocked=_need(ctx.binary, "candidate binary") or _need(ctx.lib, "libzpq.so"),
    )


def stage_release_smoke(ctx: Ctx) -> Cmd:
    return Cmd(
        [
            "tools/release_smoke.sh",
            str(ctx.run_dir / "release-smoke"),
        ],
        _base_env(ctx),
        "cross-build releases; run native CLI and Lambda, foreign checks: verify arch",
        parse=parse_release,
    )


def stage_modules(ctx: Ctx) -> Cmd:
    return _python(ctx, "tools/check_test_modules.py", "test-module coverage", parse=parse_modules)


REFEREE_CASES = [
    "tools/runner/cases/filter_projection.json",
    "tools/runner/cases/sql_pred.json",
    "tools/runner/cases/agg.json",
    "tools/runner/cases/sql_query.json",
    "tools/runner/cases/stats_order.json",
    "tools/runner/cases/writer_wire.json",
    "tools/runner/cases/tail.json",
    "tools/runner/cases/product.json",
    "tools/runner/cases/packed_count.json",
    "tools/runner/cases/filtered_records.json",
    "tools/runner/cases/oracle_precision.json",
]


def referee_argv(ctx: Ctx) -> list[str]:
    argv = ["zig-out/bin/referee"]
    for path in REFEREE_CASES:
        argv.extend(["--cases", path])
    argv.extend(
        [
            "--divergences",
            "tests/known_divergences.json",
            "--duckdb",
            str(Path(ctx.duckdb)),
            "--pyarrow",
            ctx.python,
            "--work",
            str(ctx.run_dir / "referee-work"),
        ]
    )
    return argv


def stage_referee(ctx: Ctx) -> Cmd:
    duck = Path(ctx.duckdb)
    shell = shlex.join(_zig_build(ctx, "-Doptimize=ReleaseSafe", "referee"))
    shell += " && " + shlex.join(referee_argv(ctx))
    return Cmd(
        ["bash", "-c", shell],
        _base_env(ctx),
        "in-process differential referee against the DuckDB CLI",
        blocked=_need(duck, "DuckDB 2.0 CLI"),
        parse=parse_referee,
    )


def stage_path_parity(ctx: Ctx) -> Cmd:
    shell = shlex.join(_zig_build(ctx, "-Doptimize=ReleaseSafe", "referee"))
    shell += " && " + shlex.join([
        ctx.python, "tools/verify/path_parity.py", "--referee", str(ctx.repo / "zig-out/bin/referee"),
        "--duckdb", ctx.duckdb, "--out", str(ctx.run_dir / "path-parity"),
    ])
    return Cmd(["bash", "-c", shell], _base_env(ctx), "JSON/Parquet parity across in-process execution overrides",
               blocked=_need(Path(ctx.duckdb), "DuckDB 2.0 CLI"), parse=parse_path_parity)


def stage_conform(ctx: Ctx) -> Cmd:
    corpus = ctx.repo / "data" / "parquet-testing" / "data"
    return _python(ctx, "tools/conformance.py", "apache parquet-testing corpus", "--corpus", corpus,
                   "--min-pass", "72", needs=((corpus, "parquet-testing corpus"),))


def stage_triangulate(ctx: Ctx) -> Cmd:
    return _python(ctx, "tools/triangulate.py", "ZPQ vs DuckDB vs Hardwood, when Hardwood is present",
                   needs=((ctx.binary, "candidate binary"),))


# name, tiers, builder. Order inside a tier follows this list.
REGISTRY = [
    ("modules", ("full",), stage_modules),
    ("test-core", ("quick", "commit"), stage_test_core),
    ("build-fast", ("commit", "full"), stage_build_fast),
    ("test-full", ("full",), stage_test_full),
    ("integration", ("full",), stage_integration),
    ("differential", ("commit", "full"), stage_referee),
    ("path-parity", ("full",), stage_path_parity),
    ("fd", ("full",), stage_fd),
    ("canonical", ("full",), stage_canonical),
    ("cli-smoke", ("commit", "full"), stage_cli_smoke),
    ("duckdb-smoke", ("full",), stage_duckdb_smoke),
    ("cb-sub1", ("full",), stage_cb_sub1),
    ("cb-sub16", ("full",), stage_cb_sub16),
    ("repro-skspill", ("full",), stage_skspill),
    ("libzpq", ("full",), stage_libzpq),
    ("repro-sigpipe", ("full",), stage_sigpipe),
    ("write-robustness", ("full",), stage_write_robustness),
    ("release-smoke", ("full",), stage_release_smoke),
    ("conform", ("extra",), stage_conform),
    ("triangulate", ("extra",), stage_triangulate),
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
