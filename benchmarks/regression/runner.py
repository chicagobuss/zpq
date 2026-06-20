#!/usr/bin/env python3
"""Regression harness — locks in current behavior before perf-sensitive
refactors so we don't silently regress.

Three things checked per scenario:
  1. Result envelope (timing fields stripped) matches committed golden.
  2. Median wall time stays within `baseline.json`'s `max_ms` ceiling.
  3. Peak RSS (when measured) stays within `max_rss_kb` ceiling.

CLI scenarios run by default. Lambda scenarios run only with `--lambda`
because they require a deployed function + S3 credentials.

Usage:
  python3 benchmarks/regression/runner.py            # CLI only, default
  python3 benchmarks/regression/runner.py --lambda   # CLI + Lambda
  python3 benchmarks/regression/runner.py --update-golden  # capture goldens
  python3 benchmarks/regression/runner.py --update-baseline  # capture timings
  python3 benchmarks/regression/runner.py --runs 10  # more samples for stable medians

Exits 0 on pass, 1 on any failure.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ZPQ_ROOT = ROOT.parent.parent
ZPQ_BIN = ZPQ_ROOT / "zig-out" / "bin" / "zpq"
DATA_FILE = ZPQ_ROOT / "data" / "benchmark_100mb.parquet"
GLOB_DIR = ROOT / ".tmp_glob"  # generated at setup, not committed
GOLDEN_DIR = ROOT / "golden"
BASELINE_FILE = ROOT / "baseline.json"

# JSON keys that vary per run (timing, paths, pid, etc.) — strip
# before diffing against golden.
TIMING_KEYS = {
    "total_ms", "phase", "read_ms", "parse_ms", "decode_ms", "eval_ms",
    "encode_ms", "footer_ms", "sink_ms", "wait_for_rg_ms",
    "fetch_concurrent_ms", "fetch_ms", "build_ms", "close_ms", "meta_ms",
}
# Per-run-varying paths get normalized rather than stripped (so we
# notice if files_in changes).
PATH_KEYS = {"input", "output"}


@dataclass
class Scenario:
    name: str
    args: list[str]
    env: dict[str, str] | None = None
    setup: callable | None = None  # noqa: F821 (callable type hint loose)
    teardown: callable | None = None  # noqa: F821
    is_lambda: bool = False


def normalize_output(obj: object) -> object:
    """Strip timing fields + normalize paths, recursively."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in TIMING_KEYS:
                continue
            if k in PATH_KEYS and isinstance(v, str):
                # Keep just the basename pattern so we notice file count
                # changes but not absolute path moves.
                out[k] = "<path>"
            else:
                out[k] = normalize_output(v)
        return out
    if isinstance(obj, list):
        return [normalize_output(x) for x in obj]
    return obj


def run_zpq(args: list[str], env: dict[str, str] | None = None) -> tuple[dict, float, int]:
    """Run zpq once. Returns (parsed_json_output, wall_ms, peak_rss_kb)."""
    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    # /usr/bin/time -v writes to stderr; we capture stdout (zpq's JSON)
    # and stderr (time's stats) separately. zpq itself writes some
    # warnings to stderr too — we filter for the time -v block.
    cmd = ["/usr/bin/time", "-v", str(ZPQ_BIN)] + args
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd, env=full_env, capture_output=True, text=True, check=False
    )
    wall_ms = (time.perf_counter() - t0) * 1000.0
    if proc.returncode != 0:
        print(f"FAIL exec: {' '.join(cmd)}", file=sys.stderr)
        print(proc.stderr, file=sys.stderr)
        raise RuntimeError(f"zpq exited {proc.returncode}")

    # Parse zpq's JSON envelope from stdout.
    out_lines = [l for l in proc.stdout.splitlines() if l.strip()]
    parsed = json.loads(out_lines[-1])  # JSON envelope is the last line

    # Parse peak RSS from time -v's stderr.
    peak_rss_kb = 0
    for line in proc.stderr.splitlines():
        s = line.strip()
        if s.startswith("Maximum resident set size"):
            # "Maximum resident set size (kbytes): 12345"
            try:
                peak_rss_kb = int(s.split(":")[-1].strip())
            except ValueError:
                pass

    return parsed, wall_ms, peak_rss_kb


def setup_glob_fixture():
    """Create 4 symlinks pointing at data/benchmark_100mb.parquet so glob
    queries see a 4-file dataset without 4× the disk."""
    GLOB_DIR.mkdir(parents=True, exist_ok=True)
    for i in range(4):
        link = GLOB_DIR / f"part_{i:02d}.parquet"
        if link.exists() or link.is_symlink():
            link.unlink()
        link.symlink_to(DATA_FILE)


def teardown_glob_fixture():
    if GLOB_DIR.exists():
        for p in GLOB_DIR.iterdir():
            p.unlink()
        GLOB_DIR.rmdir()


# ----------------------------------------------------------------
# Scenarios
# ----------------------------------------------------------------

CLI_SCENARIOS = [
    Scenario(
        name="cli_count_star_1file",
        args=["query", str(DATA_FILE), "--aggregate", "count(*)"],
    ),
    Scenario(
        name="cli_count_max_sum_1file",
        args=["query", str(DATA_FILE), "--aggregate",
              "count(*) AS n, max(int32_random) AS mx, sum(int32_random) AS s"],
    ),
    Scenario(
        name="cli_filtered_1file",
        args=["query", str(DATA_FILE),
              "--filter", "int8 BETWEEN -10 AND 10",
              "--aggregate", "count(*) AS n, sum(int32_random) AS s"],
    ),
    Scenario(
        name="cli_count_star_glob",
        args=["query", str(GLOB_DIR / "*.parquet"), "--aggregate", "count(*)"],
        setup=setup_glob_fixture,
        teardown=teardown_glob_fixture,
    ),
    Scenario(
        name="cli_count_max_sum_glob",
        args=["query", str(GLOB_DIR / "*.parquet"), "--aggregate",
              "count(*) AS n, max(int32_random) AS mx, sum(int32_random) AS s"],
        setup=setup_glob_fixture,
        teardown=teardown_glob_fixture,
    ),
    Scenario(
        name="cli_filtered_glob",
        args=["query", str(GLOB_DIR / "*.parquet"),
              "--filter", "int8 BETWEEN -10 AND 10",
              "--aggregate", "count(*) AS n, sum(int32_random) AS s"],
        setup=setup_glob_fixture,
        teardown=teardown_glob_fixture,
    ),
]

@dataclass
class LambdaScenario:
    """Lambda scenarios diverge enough from CLI ones that they get
    their own dataclass. Wall time is the AWS-API roundtrip; internal
    time comes from the lambda's `total_ms` field, which catches
    code-path regressions distinct from network jitter."""
    name: str
    function_name: str
    # Callable that returns the JSON payload as a Python dict —
    # callable so we can interpolate env vars (R2_BUCKET) at run time.
    payload_fn: callable  # noqa: F821
    # Keys to keep from the response envelope when comparing to golden.
    # Defaults strip the obvious timing fields.


def lambda_aggregate_payload() -> dict:
    bucket = os.environ.get("R2_BUCKET")
    if not bucket:
        raise RuntimeError("R2_BUCKET not set; lambda scenarios skipped")
    return {
        "s3_url": f"s3://{bucket}/demo/nyc-taxi/yellow/yellow_tripdata_2024-01.parquet",
        "aggregate": "count(*) AS n, max(tip_amount) AS mx, sum(fare_amount) AS s",
    }


def lambda_compaction_payload() -> dict:
    bucket = os.environ.get("R2_BUCKET")
    if not bucket:
        raise RuntimeError("R2_BUCKET not set; lambda scenarios skipped")
    base = f"s3://{bucket}/demo/nyc-taxi/yellow/"
    return {
        "inputs": [
            base + "yellow_tripdata_2024-01.parquet",
            base + "yellow_tripdata_2024-02.parquet",
        ],
        "filter": "tip_amount > 50",
        "output_url": f"s3://{bucket}/demo/regression/compaction_out.parquet",
        "columns": ["tpep_pickup_datetime", "fare_amount", "tip_amount"],
        "output_codec": "zstd",
    }


LAMBDA_SCENARIOS: list[LambdaScenario] = [
    LambdaScenario(
        name="lambda_aggregate",
        function_name="zpq-filter-r2",
        payload_fn=lambda_aggregate_payload,
    ),
    LambdaScenario(
        name="lambda_compaction",
        function_name="zpq-filter-r2",
        payload_fn=lambda_compaction_payload,
    ),
]


def run_lambda_scenario(s: LambdaScenario, runs: int) -> dict:
    payload = s.payload_fn()
    payload_json = json.dumps(payload, separators=(",", ":"))

    wall_samples: list[float] = []
    internal_samples: list[float] = []
    first_output: dict | None = None
    for i in range(runs):
        cmd = ["aws", "lambda", "invoke",
               "--function-name", s.function_name,
               "--payload", payload_json,
               "--cli-binary-format", "raw-in-base64-out",
               "--region", os.environ.get("AWS_REGION", "us-west-2"),
               "/tmp/zpq-lambda-regression.json"]
        t0 = time.perf_counter()
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        wall_ms = (time.perf_counter() - t0) * 1000.0
        if proc.returncode != 0:
            raise RuntimeError(f"aws lambda invoke failed: {proc.stderr}")
        with open("/tmp/zpq-lambda-regression.json") as fh:
            output = json.load(fh)
        # Lambda errors come back as {"error": "...", "reason": "..."}
        # — let those fail loudly during golden diff.
        wall_samples.append(wall_ms)
        if isinstance(output, dict) and "total_ms" in output:
            internal_samples.append(float(output["total_ms"]))
        if i == 0:
            first_output = output
    return {
        "name": s.name,
        "output": first_output,
        "wall_min_ms": min(wall_samples),
        "wall_median_ms": statistics.median(wall_samples),
        "wall_p95_ms": (sorted(wall_samples)[int(len(wall_samples) * 0.95)]
                        if len(wall_samples) > 1 else wall_samples[0]),
        "wall_max_ms": max(wall_samples),
        "internal_median_ms": (statistics.median(internal_samples)
                               if internal_samples else 0.0),
        "peak_rss_kb": 0,  # not measurable for remote lambda
        "runs": runs,
    }


# ----------------------------------------------------------------
# Runner
# ----------------------------------------------------------------

def load_baseline() -> dict:
    if BASELINE_FILE.exists():
        return json.loads(BASELINE_FILE.read_text())
    return {}


def load_golden(name: str) -> dict | None:
    p = GOLDEN_DIR / f"{name}.json"
    if p.exists():
        return json.loads(p.read_text())
    return None


def write_golden(name: str, output: dict):
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    p = GOLDEN_DIR / f"{name}.json"
    p.write_text(json.dumps(normalize_output(output), indent=2, sort_keys=True) + "\n")


def diff_against_golden(name: str, output: dict) -> list[str]:
    golden = load_golden(name)
    if golden is None:
        return [f"  no golden for {name} (run with --update-golden to create)"]
    actual = normalize_output(output)
    if actual == golden:
        return []
    # Return a short diff
    msgs = []
    for k in set(actual.keys()) | set(golden.keys()):
        if actual.get(k) != golden.get(k):
            msgs.append(f"  {k}: golden={golden.get(k)!r} actual={actual.get(k)!r}")
    return msgs or ["  outputs differ in nested structure (check JSON)"]


def run_scenario(s: Scenario, runs: int) -> dict:
    if s.setup:
        s.setup()
    try:
        wall_samples: list[float] = []
        rss_samples: list[int] = []
        first_output: dict | None = None
        for i in range(runs):
            output, wall_ms, rss_kb = run_zpq(s.args, s.env)
            wall_samples.append(wall_ms)
            if rss_kb:
                rss_samples.append(rss_kb)
            if i == 0:
                first_output = output
        return {
            "name": s.name,
            "output": first_output,
            "wall_min_ms": min(wall_samples),
            "wall_median_ms": statistics.median(wall_samples),
            "wall_p95_ms": (sorted(wall_samples)[int(len(wall_samples) * 0.95)]
                            if len(wall_samples) > 1 else wall_samples[0]),
            "wall_max_ms": max(wall_samples),
            "peak_rss_kb": max(rss_samples) if rss_samples else 0,
            "runs": runs,
        }
    finally:
        if s.teardown:
            s.teardown()


def check_baseline(result: dict, baseline: dict) -> list[str]:
    name = result["name"]
    bound = baseline.get(name, {})
    msgs = []
    if "max_ms" in bound:
        if result["wall_median_ms"] > bound["max_ms"]:
            msgs.append(
                f"  median wall {result['wall_median_ms']:.1f} ms > "
                f"baseline {bound['max_ms']} ms"
            )
    # Lambda paths also assert on internal_ms — strips network jitter
    # so we catch genuine code-path slowdowns the wall time would mask.
    if "max_internal_ms" in bound:
        if result.get("internal_median_ms", 0) > bound["max_internal_ms"]:
            msgs.append(
                f"  median internal {result['internal_median_ms']:.1f} ms > "
                f"baseline {bound['max_internal_ms']} ms"
            )
    if "max_rss_kb" in bound and result["peak_rss_kb"] > bound["max_rss_kb"]:
        msgs.append(
            f"  peak RSS {result['peak_rss_kb']} KB > baseline {bound['max_rss_kb']} KB"
        )
    return msgs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lambda", dest="incl_lambda", action="store_true",
                    help="also run Lambda scenarios (needs deployed fn + creds)")
    ap.add_argument("--cli-only", action="store_true",
                    help="explicitly skip lambda scenarios (default behavior)")
    ap.add_argument("--update-golden", action="store_true",
                    help="overwrite golden outputs with this run's results")
    ap.add_argument("--update-baseline", action="store_true",
                    help="overwrite baseline.json with this run's wall medians + RSS peaks "
                         "(adds 30%% slack for jitter)")
    ap.add_argument("--runs", type=int, default=5,
                    help="samples per scenario (default 5)")
    ap.add_argument("--filter", type=str, default=None,
                    help="run only scenarios matching this substring")
    args = ap.parse_args()

    if not ZPQ_BIN.exists():
        print(f"missing {ZPQ_BIN}; run `zig build -Doptimize=ReleaseFast` first",
              file=sys.stderr)
        sys.exit(2)
    # Sanity-check the binary is a release build. Debug ~47 MB,
    # ReleaseFast ~27 MB (after S3 read support pulled BoringSSL +
    # SigV4 + libzstd into the CLI). A stale Debug build runs 5×
    # slower and triggers DebugAllocator leak panics on the agg
    # path's returned-but-unfreed items — both corrupt the baseline.
    size = ZPQ_BIN.stat().st_size
    if size > 40 * 1024 * 1024:
        print(f"ERROR: {ZPQ_BIN} is {size} bytes — looks like a Debug build.",
              file=sys.stderr)
        print("       run: zig build -Doptimize=ReleaseFast", file=sys.stderr)
        sys.exit(2)
    if not DATA_FILE.exists():
        print(f"missing fixture {DATA_FILE}", file=sys.stderr)
        sys.exit(2)

    scenarios: list = list(CLI_SCENARIOS)
    lambda_scenarios: list = list(LAMBDA_SCENARIOS) if args.incl_lambda else []
    if args.filter:
        scenarios = [s for s in scenarios if args.filter in s.name]
        lambda_scenarios = [s for s in lambda_scenarios if args.filter in s.name]
        if not scenarios and not lambda_scenarios:
            print(f"no scenarios matched filter {args.filter!r}", file=sys.stderr)
            sys.exit(2)

    baseline = load_baseline()
    failures: list[tuple[str, list[str]]] = []
    results: list[dict] = []

    total = len(scenarios) + len(lambda_scenarios)
    print(f"running {total} scenarios × {args.runs} samples each")

    def handle(name: str, runner_fn, *runner_args):
        nonlocal failures, results
        print(f"  {name} ...", end=" ", flush=True)
        try:
            r = runner_fn(*runner_args)
        except Exception as e:
            print(f"ERROR: {e}")
            failures.append((name, [f"  exception: {e}"]))
            return
        results.append(r)
        diff_msgs = diff_against_golden(name, r["output"]) if not args.update_golden else []
        bound_msgs = check_baseline(r, baseline) if not args.update_baseline else []
        suffix = f"median={r['wall_median_ms']:.1f}ms"
        if r.get("internal_median_ms"):
            suffix += f" internal={r['internal_median_ms']:.0f}ms"
        if r.get("peak_rss_kb"):
            suffix += f" rss={r['peak_rss_kb']}kb"
        if diff_msgs or bound_msgs:
            print(f"FAIL  {suffix}")
            failures.append((name, diff_msgs + bound_msgs))
        else:
            print(f"ok    {suffix}")
        if args.update_golden:
            write_golden(name, r["output"])

    for s in scenarios:
        handle(s.name, run_scenario, s, args.runs)
    for s in lambda_scenarios:
        handle(s.name, run_lambda_scenario, s, args.runs)

    if args.update_baseline:
        new_baseline = baseline.copy()
        for r in results:
            # 30% slack on median wall, 25% slack on RSS, 20% on internal.
            entry = {
                "max_ms": int(r["wall_median_ms"] * 1.3) + 5,
                "max_rss_kb": int(r["peak_rss_kb"] * 1.25) + 1024,
            }
            if r.get("internal_median_ms"):
                entry["max_internal_ms"] = int(r["internal_median_ms"] * 1.2) + 50
            new_baseline[r["name"]] = entry
        BASELINE_FILE.write_text(json.dumps(new_baseline, indent=2, sort_keys=True) + "\n")
        print(f"updated {BASELINE_FILE}")

    if failures:
        print(f"\n{len(failures)} failures:")
        for name, msgs in failures:
            print(f"  {name}:")
            for m in msgs:
                print(m)
        sys.exit(1)
    print(f"\nall {len(results)} scenarios pass")


if __name__ == "__main__":
    main()
