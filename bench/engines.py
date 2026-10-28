"""Each engine's command for a case. Every engine gets the same thread count: the timing CPUs'.

Fresh-process lanes (cold, hot) time one process from spawn through reap, setup and answer capture included. The
engine lane times queries inside one DuckDB session or one Polars process (warm-up first, then N); zpq stays a
fresh process there, since its whole start-up is a few milliseconds.
"""

import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DUCKDB = Path.home() / ".cache/zpq-duckdb2/bin-2.0.0/duckdb"
POLARS = Path.home() / "bench/zpq-fair/venv-polars2/bin/python"
KINDS = {"zpq": "zpq", "zpq_prev": "zpq", "duckdb20": "duckdb", "polars": "polars"}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def describe(name, path):
    """The pinned identity of an engine: its bytes and its own version string."""
    kind = KINDS[name]
    version = {"zpq": [path, "--version"], "duckdb": [path, "-no-agent", "--version"],
               "polars": [path, "-c", "import polars; print(polars.__version__)"]}[kind]
    out = {"kind": kind, "path": str(path), "sha256": sha256(path),
           "version": subprocess.check_output(version, text=True, timeout=30).strip()}
    if kind == "polars":
        out["polars_run_sha256"] = sha256(ROOT / "bench/polars_run.py")
    return out


def copy_sql(sql, sink, codec, level):
    options = f"FORMAT PARQUET, COMPRESSION {codec or 'snappy'}" + (f", COMPRESSION_LEVEL {level}" if level else "")
    return f"COPY ({sql}) TO '{sink}' ({options})"


def fresh(engine, spec, case, threads, answer, sink):
    """(argv, env, answer on stdout) for one fresh-process run. A Parquet answer goes to `sink`; Polars writes its
    rows to `answer` itself, since its stdout carries the timing line."""
    kind = spec["kind"]
    if kind == "zpq":
        argv = [spec["path"], *case["zpq"], "--threads", str(threads)]
        if case["output"] == "parquet":
            argv += ["-o", sink, "--codec", case["codec"]]
        return argv, {}, True
    if kind == "duckdb":
        sql = case["duckdb"]["sql"]
        if case["output"] == "parquet":
            sql = copy_sql(sql, sink, case["codec"], case["level"])
        script = f"SET threads={threads};\n{case['duckdb']['setup']}\n{sql};"
        return [spec["path"], "-no-agent", "-init", "/dev/null", "-json", "-c", script], {}, True
    req = polars_request(case, 1, [str(answer)], [str(sink)] if case["output"] == "parquet" else None)
    argv = [spec["path"], str(ROOT / "bench/polars_run.py"), json.dumps(req)]
    return argv, {"POLARS_MAX_THREADS": str(threads)}, False


def polars_request(case, runs, answers, sinks):
    return {**case["polars"], "inputs": case["inputs"], "query_id": case["query_id"], "runs": runs,
            "answers": answers, "sink": sinks, "codec": case["codec"], "level": case["level"]}


def session(engine, spec, case, threads, answers, sinks, script_path):
    """(argv, env) for one in-session run of len(answers) queries, the first a warm-up."""
    if spec["kind"] == "polars":
        req = polars_request(case, len(answers), answers, sinks)
        return [spec["path"], str(ROOT / "bench/polars_run.py"), json.dumps(req)], {"POLARS_MAX_THREADS": str(threads)}
    lines = [".mode json", f"SET threads={threads};", case["duckdb"]["setup"], ".timer on"]
    for i in range(len(answers)):
        sql = case["duckdb"]["sql"]
        if sinks:
            sql = copy_sql(sql, sinks[i], case["codec"], case["level"])
        lines += [f".print __RUN_{i}", sql + ";"]
    Path(script_path).write_text("\n".join(lines) + "\n")
    return [spec["path"], "-no-agent", "-init", "/dev/null", "-f", str(script_path)], {}


def split_session(stdout_text, answers):
    """A DuckDB session's stdout into each run's answer file; returns each run's engine time (ns)."""
    times = []
    parts = re.split(r"^__RUN_(\d+)$", stdout_text, flags=re.M)
    for i, body in zip(parts[1::2], parts[2::2]):
        timer = re.search(r"^Run Time \(s\): real ([0-9.]+)", body, re.M)
        if not timer:
            raise ValueError(f"run {i}: no timer line")
        times.append(round(float(timer[1]) * 1e9))
        Path(answers[int(i)]).write_text(body[:timer.start()].strip() or "[]")
    if len(times) != len(answers):
        raise ValueError(f"{len(times)} timed runs, want {len(answers)}")
    return times
