#!/usr/bin/env python3
"""
common_four.py — the canonical perf-tracking suite.

Four baseline cells, one row per cell, RUNS samples each. Re-run before AND
after any hot-path change so wins/regressions show up in a consistent shape.

Cells:
  1. lambda_s3_to_s3   — Lambda: read 100 MB from S3, filter + project,
                         write back to S3. Wall is end-to-end (includes
                         invoke RTT).
  2. local_from_s3     — workstation `zpq query` against S3, decode-heavy
                         aggregate. No write.
  3. local_from_local  — workstation `zpq query` against local file.
  4. local_from_r2     — workstation `zpq query` against R2 (NYC taxi
                         yellow_tripdata_2023-01).

Additional release probes can be selected explicitly without changing the
Common 4 baseline:
  - r2_groupby           — one R2 taxi file, low-cardinality string GROUP BY.
  - r2_groupby_five_files — five explicit monthly R2 taxi inputs.

Usage:
  python3 benchmarks/common_four.py [--runs N] [--cells a,b,c]
                                     [--out PATH]
                                     [--no-warmup]

Output: TSV `cell  min_ms  median_ms  p95_ms` to stdout, also written
to --out path (default: benchmarks/common_four_results.tsv).

Requires .env loaded with AWS_S3_BUCKET, R2_*, LAMBDA_FUNCTION_NAME.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ZPQ = os.path.join(ROOT, "zig-out", "bin", "zpq")
LOCAL_FIXTURE = os.path.join(ROOT, "data", "benchmark_100mb.parquet")

AGG_BENCH = "count(*) AS n, sum(int64_sorted) AS s, max(int8) AS mx"
AGG_TAXI = "count(*) AS n, sum(fare_amount) AS f, max(tip_amount) AS t"

CELLS = ["lambda_s3_to_s3", "local_from_s3", "local_from_local", "local_from_r2"]

# `store_and_fwd_flag` is a dictionary-encoded STRING with about three values
# over 3 M rows. It exercises remote GROUP BY key fetching and the dictionary
# key fast path without changing the longitudinal Common 4 query shapes.
GROUP_TAXI = "store_and_fwd_flag AS flag"
AGG_TAXI_GROUPED = "count(*) AS n, sum(fare_amount) AS f, max(tip_amount) AS t"


def need_env(*names):
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        sys.exit(f"missing env: {missing} (source .env first)")


def time_run(cmd, env=None) -> float:
    """Run cmd, return wall-clock seconds. Raises on non-zero exit."""
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, env=env, capture_output=True)
    t1 = time.perf_counter()
    if proc.returncode != 0:
        sys.stderr.write(
            f"FAILED: {' '.join(cmd[:3])}...\n"
            f"  stderr: {proc.stderr.decode()[:400]}\n"
            f"  stdout: {proc.stdout.decode()[:400]}\n"
        )
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    return t1 - t0


def lambda_payload() -> str:
    bucket = os.environ["AWS_S3_BUCKET"]
    stamp = time.time_ns()
    return json.dumps({
        "s3_url": f"s3://{bucket}/zpq_test_data/benchmark/benchmark_100mb.parquet",
        "output_url": f"s3://{bucket}/bench/common_four/{stamp}.parquet",
        "filter": "int8 BETWEEN -10 AND 10",
        "columns": ["int8", "int64_sorted", "f64"],
    })


def cell_lambda_s3_to_s3() -> float:
    fn = os.environ["LAMBDA_FUNCTION_NAME"]
    region = os.environ.get("AWS_REGION", "us-west-2")
    payload = lambda_payload()
    cmd = [
        "aws", "lambda", "invoke",
        "--function-name", fn,
        "--cli-binary-format", "raw-in-base64-out",
        "--payload", payload,
        "--cli-read-timeout", "180",
        "--region", region,
        "/tmp/zpq_lambda_out.json",
    ]
    return time_run(cmd)


def cell_local_from_s3() -> float:
    bucket = os.environ["AWS_S3_BUCKET"]
    url = f"s3://{bucket}/zpq_test_data/benchmark/benchmark_100mb.parquet"
    cmd = [ZPQ, "query", url, "--aggregate", AGG_BENCH]
    return time_run(cmd)


def cell_local_from_local() -> float:
    cmd = [ZPQ, "query", LOCAL_FIXTURE, "--aggregate", AGG_BENCH]
    return time_run(cmd)


def cell_local_from_r2() -> float:
    env = r2_env()
    url = f"s3://{env['R2_BUCKET']}/demo/nyc-taxi/yellow/yellow_tripdata_2023-01.parquet"
    cmd = [ZPQ, "query", url, "--aggregate", AGG_TAXI]
    return time_run(cmd, env=env)


def r2_env():
    """Map R2 credentials to ZPQ's S3 environment without mutating ours."""
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = env["R2_ACCESS_KEY_ID"]
    env["AWS_SECRET_ACCESS_KEY"] = env["R2_SECRET_ACCESS_KEY"]
    env["AWS_REGION"] = env.get("R2_REGION", "auto")
    endpoint = env["R2_ENDPOINT"]
    env["S3_ENDPOINT_URL"] = endpoint if "://" in endpoint else f"https://{endpoint}"
    return env


def cell_r2_groupby() -> float:
    """One file: remote key fetch plus dictionary-string GROUP BY."""
    env = r2_env()
    url = f"s3://{env['R2_BUCKET']}/demo/nyc-taxi/yellow/yellow_tripdata_2023-01.parquet"
    cmd = [ZPQ, "query", url, "--group-by", GROUP_TAXI, "--aggregate", AGG_TAXI_GROUPED]
    return time_run(cmd, env=env)


def cell_r2_groupby_five_files() -> float:
    """Five files: also exercises multi-file scheduling and merge/finalization."""
    env = r2_env()
    urls = [
        f"s3://{env['R2_BUCKET']}/demo/nyc-taxi/yellow/yellow_tripdata_2023-{month:02d}.parquet"
        for month in range(1, 6)
    ]
    cmd = [ZPQ, "query", *urls, "--group-by", GROUP_TAXI, "--aggregate", AGG_TAXI_GROUPED]
    return time_run(cmd, env=env)


CELL_DRIVERS = {
    "lambda_s3_to_s3": (cell_lambda_s3_to_s3,
                        ["AWS_S3_BUCKET", "LAMBDA_FUNCTION_NAME"]),
    "local_from_s3":   (cell_local_from_s3,
                        ["AWS_S3_BUCKET"]),
    "local_from_local": (cell_local_from_local, []),
    "local_from_r2":   (cell_local_from_r2,
                         ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
                          "R2_BUCKET", "R2_ENDPOINT"]),
    "r2_groupby":      (cell_r2_groupby,
                         ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
                          "R2_BUCKET", "R2_ENDPOINT"]),
    "r2_groupby_five_files": (cell_r2_groupby_five_files,
                                ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
                                 "R2_BUCKET", "R2_ENDPOINT"]),
}


def sample_cell(name, runs, warmup) -> tuple[float, float, float]:
    driver, _ = CELL_DRIVERS[name]
    if warmup:
        try:
            driver()
        except subprocess.CalledProcessError:
            pass
    samples = []
    for _ in range(runs):
        try:
            samples.append(driver() * 1000.0)
        except subprocess.CalledProcessError:
            pass
    if not samples:
        return float("nan"), float("nan"), float("nan")
    samples.sort()
    p95_idx = max(0, int(len(samples) * 0.95) - 1)
    return samples[0], statistics.median(samples), samples[p95_idx]


def git_short_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT, text=True
        ).strip()
    except subprocess.CalledProcessError:
        return "unknown"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--cells", default=",".join(CELLS))
    ap.add_argument("--out", default=os.path.join(ROOT, "benchmarks", "common_four_results.tsv"))
    ap.add_argument("--no-warmup", action="store_true")
    args = ap.parse_args()

    cells = args.cells.split(",")
    unknown = [c for c in cells if c not in CELL_DRIVERS]
    if unknown:
        sys.exit(f"unknown cells: {unknown}")

    if not os.path.exists(ZPQ):
        sys.exit(f"missing {ZPQ} — run 'just build' first")

    # Validate per-cell env requirements.
    for c in cells:
        _, env_keys = CELL_DRIVERS[c]
        if env_keys:
            need_env(*env_keys)

    header = f"# zpq common-four — {git_short_sha()} — {time.strftime('%Y-%m-%dT%H:%M:%S%z')}"
    cfg = f"# runs={args.runs} cells={','.join(cells)} warmup={'no' if args.no_warmup else 'yes'}"
    rows = ["cell\tmin_ms\tmedian_ms\tp95_ms"]
    for c in cells:
        mn, md, p95 = sample_cell(c, args.runs, not args.no_warmup)
        if mn != mn:  # NaN
            rows.append(f"{c}\tFAILED\tFAILED\tFAILED")
        else:
            rows.append(f"{c}\t{mn:.1f}\t{md:.1f}\t{p95:.1f}")
            print(rows[-1], flush=True)

    out_text = "\n".join([header, cfg] + rows) + "\n"
    print(header)
    print(cfg)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        f.write(out_text)
    sys.stderr.write(f"\n# wrote {args.out}\n")


if __name__ == "__main__":
    main()
