#!/usr/bin/env python3
"""Turn a validated Lambda selectivity TSV into a publication-safe table.

This deliberately summarizes Lambda-reported `total_ms`, not caller wall
time: the latter includes workstation-to-Lambda network jitter.  Input must
come from `run_selectivity.sh`, whose validation sidecar proves every emitted
Parquet file can be read by pyarrow and DuckDB before a figure is produced.
"""

import argparse
import csv
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path


ENGINES = ("zpq", "polars", "duckdb")


def read_rows(path: Path):
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    expected = {"label", "total_ms", "bytes_out", "rows_kept"}
    if not rows or set(rows[0]) != expected:
        raise ValueError(f"{path} must have exactly these TSV columns: {sorted(expected)}")

    grouped = defaultdict(list)
    for row in rows:
        try:
            engine, scenario = row["label"].split(":", 1)
            if engine not in ENGINES:
                raise ValueError(f"unknown engine {engine!r}")
            grouped[scenario, engine].append(
                (float(row["total_ms"]), int(row["bytes_out"]), int(row["rows_kept"]))
            )
        except (ValueError, KeyError) as e:
            raise ValueError(f"invalid row {row}: {e}") from e
    return grouped


def assert_validated(path: Path):
    text = path.read_text()
    if "FAIL" in text or " OK  " not in text:
        raise ValueError(f"{path} does not show a fully successful output-validation run")


def summary(grouped):
    scenarios = sorted({scenario for scenario, _ in grouped})
    lines = [
        "| scenario | engine | samples | median Lambda ms | p95 Lambda ms | median output bytes | median rows |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for scenario in scenarios:
        for engine in ENGINES:
            samples = grouped.get((scenario, engine), [])
            if not samples:
                raise ValueError(f"{scenario!r} has no {engine} samples")
            times, output_bytes, rows = zip(*samples)
            p95 = sorted(times)[max(0, math.ceil(len(times) * 0.95) - 1)]
            lines.append(
                f"| {scenario} | {engine} | {len(times)} | {statistics.median(times):.0f} | "
                f"{p95:.0f} | {statistics.median(output_bytes):.0f} | {statistics.median(rows):.0f} |"
            )
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results", type=Path, help="TSV from run_selectivity.sh")
    ap.add_argument("--validation", required=True, type=Path, help="successful validation sidecar from that run")
    ap.add_argument("--out", type=Path, help="write Markdown here instead of stdout")
    args = ap.parse_args()

    try:
        assert_validated(args.validation)
        lines = [
            "# Lambda selectivity comparison",
            "",
            "All rows use the same ten-file S3 fixture and were validated by pyarrow and DuckDB.",
            "Times are Lambda-reported execution time; do not compare them to caller-wall or cold-start figures.",
            "",
            *summary(read_rows(args.results)),
            "",
        ]
    except (OSError, ValueError) as e:
        sys.exit(f"cannot summarize benchmark figures: {e}")

    text = "\n".join(lines)
    if args.out:
        args.out.write_text(text)
        print(f"wrote {args.out}")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
