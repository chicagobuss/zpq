#!/usr/bin/env python3
"""Produce a publication-safe summary of the Overture Lambda comparison."""
import argparse
import csv
import statistics
import sys
from pathlib import Path


ENGINES = ("zpq", "polars", "duckdb")


def require_validated(path: Path, expected: int) -> None:
    text = path.read_text()
    if "FAIL" in text or text.count(" OK  ") < expected:
        raise ValueError(f"{path} does not validate every benchmark output")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("results", type=Path)
    ap.add_argument("--validation", required=True, type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    try:
        with args.results.open(newline="") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        if not rows or set(rows[0]) != {"engine", "sample", "lambda_ms", "bytes_out", "output_url"}:
            raise ValueError("unexpected results schema")
        by_engine = {engine: [] for engine in ENGINES}
        for row in rows:
            engine = row["engine"]
            if engine not in by_engine:
                raise ValueError(f"unexpected engine {engine!r}")
            by_engine[engine].append((float(row["lambda_ms"]), int(row["bytes_out"])))
        count = len(rows)
        if any(not by_engine[engine] for engine in ENGINES):
            raise ValueError("one or more engines have no samples")
        require_validated(args.validation, count + 3)  # benchmark samples plus warm-ups
    except (OSError, ValueError, csv.Error) as exc:
        sys.exit(f"cannot summarize benchmark figures: {exc}")

    lines = [
        "# Lambda Overture comparison",
        "",
        "Warm, same-region x86_64 Lambda functions (3008 MB, 120 s). Each sample filters `confidence > 0.9`, projects `id, confidence`, and writes Snappy Parquet. Every warm-up and measured output was opened by PyArrow and DuckDB before this table was emitted.",
        "",
        "| engine | samples | median Lambda ms | highest observed ms | median output bytes |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for engine in ENGINES:
        samples = by_engine[engine]
        times = sorted(value[0] for value in samples)
        sizes = [value[1] for value in samples]
        highest = max(times)
        lines.append(
            f"| {engine} | {len(samples)} | {statistics.median(times):.0f} | {highest:.0f} | {statistics.median(sizes):.0f} |"
        )
    output = "\n".join(lines) + "\n"
    if args.out:
        args.out.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
