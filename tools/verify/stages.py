"""Stage registry. Each stage builds one command, or a reason it cannot run."""

from __future__ import annotations

import ast
import re
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
    differential_args: list[str] = field(default_factory=list)


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
    if match.group(3):
        counts["skipped"] = int(match.group(3))
    return counts


def parse_modules(text):
    match = _last(r"test-module coverage: (\d+) modules, (\d+) tests", text)
    if not match:
        return {}
    return {"modules": int(match.group(1)), "tests": int(match.group(2))}


def parse_differential(text):
    match = _last(r"differential(?: --only \S+)?: (\d+)/(\d+) checks match", text)
    if not match:
        return {}
    return {"matched": int(match.group(1)), "total": int(match.group(2))}


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
        if line.strip().startswith(("RAN", "SKIPPED", "NOT RUN"))
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


def stage_test_core(ctx: Ctx) -> Cmd:
    return Cmd(_zig_build(ctx, "test-core", "--summary", "all"), _base_env(ctx), "core unit tests", parse=parse_tests)


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
    return Cmd(_zig_build(ctx, "test", "--summary", "all"), _base_env(ctx), "unit tests", parse=parse_tests)


def stage_integration(ctx: Ctx) -> Cmd:
    return Cmd(
        _zig_build(ctx, "test-integration", "--summary", "line"),
        _base_env(ctx),
        "Lambda integration tests",
        parse=parse_tests,
    )


def stage_differential(ctx: Ctx) -> Cmd:
    return Cmd(
        [ctx.python, "tools/differential.py", *ctx.differential_args],
        _base_env(ctx),
        "row-by-row differential against DuckDB",
        parse=parse_differential,
    )


def stage_fd(ctx: Ctx) -> Cmd:
    blocked = _need(ctx.binary, "candidate binary")
    if not ctx.base:
        blocked = "fd needs --base, the comparison binary (sol1 GATE_BASE)"
    elif not Path(ctx.base).exists():
        blocked = f"missing --base binary: {ctx.base}"
    return Cmd(
        [ctx.python, "tools/group_dependency_check.py", str(ctx.binary), ctx.base or ""],
        _base_env(ctx),
        "group-dependency exactness against the required --base binary",
        blocked=blocked,
        parse=parse_fd,
    )


def stage_canonical(ctx: Ctx) -> Cmd:
    return Cmd(
        [ctx.python, "tools/decode_error_check.py", str(ctx.binary)],
        _base_env(ctx),
        "canonical error precedence",
        blocked=_need(ctx.binary, "candidate binary"),
        parse=parse_canonical,
    )


def stage_cli_smoke(ctx: Ctx) -> Cmd:
    return Cmd(
        ["tools/cli_smoke.sh", str(ctx.binary)],
        _base_env(ctx),
        "CLI smoke and pyarrow readback",
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


def stage_test_soak(ctx: Ctx) -> Cmd:
    shell = " && ".join(
        [
            " ".join(_zig_build(ctx, "test-soak", "--summary", "all")),
            " ".join(
                _zig_build(
                    ctx,
                    "-Doptimize=ReleaseFast",
                    f"-Drevision={ctx.revision}",
                    f"-Dcpu={ctx.cpu}",
                )
            ),
            f"{ctx.python} tools/soak.py",
        ]
    )
    return Cmd(["bash", "-c", shell], _base_env(ctx), "edge-case soak (distinct from verify soak)", parse=parse_tests)


def _cross(ctx: Ctx, target: str, cpu: str | None) -> Cmd:
    prefix = ctx.run_dir / f"cross-{target}"
    args = ["-Dtarget=" + target, "-Doptimize=ReleaseFast", f"-Drevision={ctx.revision}"]
    if cpu:
        args.append(f"-Dcpu={cpu}")
    args.append(f"--prefix={prefix}")
    return Cmd(_zig_build(ctx, *args), _base_env(ctx), f"ReleaseFast cross build {target}")


def stage_release_smoke(ctx: Ctx) -> Cmd:
    return Cmd(
        [
            "tools/release_smoke.sh",
            "--skip",
            "aarch64-macos,x86_64-macos",
            str(ctx.run_dir / "release-smoke"),
        ],
        _base_env(ctx),
        "run Linux CLI and Lambda, including ARM under qemu",
        parse=parse_release,
    )


def stage_modules(ctx: Ctx) -> Cmd:
    return Cmd([ctx.python, "tools/check_test_modules.py"], _base_env(ctx), "test-module coverage", parse=parse_modules)


def stage_conform(ctx: Ctx) -> Cmd:
    corpus = ctx.repo / "data" / "parquet-testing" / "data"
    return Cmd(
        [ctx.python, "tools/conformance.py", "--corpus", str(corpus), "--min-pass", "72"],
        _base_env(ctx),
        "apache parquet-testing corpus",
        blocked=_need(corpus, "parquet-testing corpus"),
    )


def stage_triangulate(ctx: Ctx) -> Cmd:
    return Cmd(
        [ctx.python, "tools/triangulate.py"],
        _base_env(ctx),
        "ZPQ vs DuckDB vs Hardwood, when Hardwood is present",
        blocked=_need(ctx.binary, "candidate binary"),
    )


# name, tiers, builder. Order inside a tier follows this list.
REGISTRY = [
    ("modules", ("full",), stage_modules),
    ("test-core", ("quick", "commit"), stage_test_core),
    ("build-fast", ("commit", "full"), stage_build_fast),
    ("test-full", ("full",), stage_test_full),
    ("integration", ("full",), stage_integration),
    ("differential", ("commit", "full"), stage_differential),
    ("fd", ("full",), stage_fd),
    ("canonical", ("full",), stage_canonical),
    ("cli-smoke", ("full",), stage_cli_smoke),
    ("duckdb-smoke", ("full",), stage_duckdb_smoke),
    ("cb-sub1", ("full",), stage_cb_sub1),
    ("cb-sub16", ("full",), stage_cb_sub16),
    ("repro-skspill", ("full",), stage_skspill),
    ("libzpq", ("full",), stage_libzpq),
    ("repro-sigpipe", ("full",), stage_sigpipe),
    ("write-robustness", ("full",), stage_write_robustness),
    ("test-soak", ("full",), stage_test_soak),
    ("cross-x86_64-linux-musl", ("full",), lambda ctx: _cross(ctx, "x86_64-linux-musl", ctx.cpu)),
    ("cross-aarch64-linux-musl", ("full",), lambda ctx: _cross(ctx, "aarch64-linux-musl", None)),
    ("cross-aarch64-macos", ("full",), lambda ctx: _cross(ctx, "aarch64-macos", None)),
    ("cross-x86_64-macos", ("full",), lambda ctx: _cross(ctx, "x86_64-macos", None)),
    ("release-smoke", ("full",), stage_release_smoke),
    ("conform", ("extra",), stage_conform),
    ("triangulate", ("extra",), stage_triangulate),
]


def names_for(tier: str) -> list[str]:
    return [name for name, tiers, _builder in REGISTRY if tier in tiers]


def get(name: str):
    for item in REGISTRY:
        if item[0] == name:
            return item
    raise KeyError(name)


def describe(name: str, ctx: Ctx) -> Cmd:
    _name, _tiers, builder = get(name)
    return builder(ctx)
