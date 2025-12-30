#!/usr/bin/env python3
"""
Compare ZPQ benchmark runs and generate markdown tables.

Usage:
    python compare_runs.py trace1.json trace2.json [trace3.json ...]
    python compare_runs.py traces/*.json --sort throughput
    python compare_runs.py traces/*.json --baseline trace1.json
"""

import json
import sys
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class Run:
    path: str
    timestamp_ms: int
    git_commit: Optional[str]
    file_path: str
    file_size_bytes: int
    total_rows: int
    filter_column: Optional[str]
    filter_value: Optional[str]
    total_ms: float
    filter_decode_ms: float
    skip_ms: float
    materialize_ms: float
    loop_overhead_ms: float
    rows_scanned: int
    rows_selected: int
    rows_skipped: int
    selectivity: float
    throughput_mval_s: float
    batches_processed: int
    batches_skipped: int
    row_groups_scanned: int
    row_groups_skipped: int

    @classmethod
    def from_json(cls, path: str) -> "Run":
        with open(path) as f:
            data = json.load(f)
        run = data["run"]
        metrics = data["metrics"]
        return cls(
            path=path,
            timestamp_ms=run.get("timestamp_ms", 0),
            git_commit=run.get("git_commit"),
            file_path=run.get("file_path", ""),
            file_size_bytes=run.get("file_size_bytes", 0),
            total_rows=run.get("total_rows", 0),
            filter_column=run.get("filter_column"),
            filter_value=run.get("filter_value"),
            total_ms=metrics.get("total_ms", 0),
            filter_decode_ms=metrics.get("filter_decode_ms", 0),
            skip_ms=metrics.get("skip_ms", 0),
            materialize_ms=metrics.get("materialize_ms", 0),
            loop_overhead_ms=metrics.get("loop_overhead_ms", 0),
            rows_scanned=metrics.get("rows_scanned", 0),
            rows_selected=metrics.get("rows_selected", 0),
            rows_skipped=metrics.get("rows_skipped", 0),
            selectivity=metrics.get("selectivity", 0),
            throughput_mval_s=metrics.get("throughput_mval_s", 0),
            batches_processed=metrics.get("batches_processed", 0),
            batches_skipped=metrics.get("batches_skipped", 0),
            row_groups_scanned=metrics.get("row_groups_scanned", 0),
            row_groups_skipped=metrics.get("row_groups_skipped", 0),
        )

    @property
    def name(self) -> str:
        return Path(self.path).stem

    @property
    def short_commit(self) -> str:
        if self.git_commit:
            return self.git_commit[:7]
        return "n/a"


def format_delta(current: float, baseline: float, higher_is_better: bool = True) -> str:
    """Format a value with delta from baseline."""
    if baseline == 0:
        return f"{current:.2f}"

    delta_pct = ((current - baseline) / baseline) * 100

    if abs(delta_pct) < 0.5:
        return f"{current:.2f}"

    # Green for improvement, red for regression
    if higher_is_better:
        color = "+" if delta_pct > 0 else ""
    else:
        color = "" if delta_pct > 0 else "+"
        delta_pct = -delta_pct if not higher_is_better else delta_pct

    return f"{current:.2f} ({color}{delta_pct:+.1f}%)"


def print_comparison_table(runs: list[Run], baseline: Optional[Run] = None):
    """Print a markdown comparison table."""

    if not runs:
        print("No runs to compare.")
        return

    # Use first run as baseline if not specified
    if baseline is None and len(runs) > 1:
        baseline = runs[0]

    print("## ZPQ Benchmark Comparison\n")

    # Summary table
    print("### Summary\n")
    print("| Run | Commit | Selectivity | Total (ms) | Throughput (MVal/s) |")
    print("|-----|--------|-------------|------------|---------------------|")

    for run in runs:
        throughput_str = format_delta(
            run.throughput_mval_s,
            baseline.throughput_mval_s if baseline else 0,
            higher_is_better=True
        )
        total_str = format_delta(
            run.total_ms,
            baseline.total_ms if baseline else 0,
            higher_is_better=False
        )
        print(f"| {run.name} | {run.short_commit} | {run.selectivity:.2%} | {total_str} | {throughput_str} |")

    print()

    # Time breakdown table
    print("### Time Breakdown (ms)\n")
    print("| Run | Filter Decode | Skip | Materialize | Overhead |")
    print("|-----|---------------|------|-------------|----------|")

    for run in runs:
        print(f"| {run.name} | {run.filter_decode_ms:.2f} | {run.skip_ms:.2f} | {run.materialize_ms:.2f} | {run.loop_overhead_ms:.2f} |")

    print()

    # Row stats table
    print("### Row Statistics\n")
    print("| Run | Scanned | Selected | Skipped | RG Skipped |")
    print("|-----|---------|----------|---------|------------|")

    for run in runs:
        print(f"| {run.name} | {run.rows_scanned:,} | {run.rows_selected:,} | {run.rows_skipped:,} | {run.row_groups_skipped} |")

    print()


def print_single_run(run: Run):
    """Print details for a single run."""
    print(f"## {run.name}\n")
    print(f"- **File**: {run.file_path} ({run.file_size_bytes / 1024 / 1024:.1f} MB)")
    print(f"- **Commit**: {run.short_commit}")
    print(f"- **Filter**: {run.filter_column}={run.filter_value}" if run.filter_column else "- **Filter**: None")
    print()
    print(f"### Performance")
    print(f"- **Total Time**: {run.total_ms:.2f} ms")
    print(f"- **Throughput**: {run.throughput_mval_s:.2f} MVal/s")
    print(f"- **Selectivity**: {run.selectivity:.2%}")
    print()
    print(f"### Time Breakdown")
    print(f"- Filter Decode: {run.filter_decode_ms:.2f} ms ({run.filter_decode_ms / run.total_ms * 100:.1f}%)" if run.total_ms > 0 else "- Filter Decode: 0 ms")
    print(f"- Skip: {run.skip_ms:.2f} ms ({run.skip_ms / run.total_ms * 100:.1f}%)" if run.total_ms > 0 else "- Skip: 0 ms")
    print(f"- Materialize: {run.materialize_ms:.2f} ms ({run.materialize_ms / run.total_ms * 100:.1f}%)" if run.total_ms > 0 else "- Materialize: 0 ms")
    print(f"- Overhead: {run.loop_overhead_ms:.2f} ms ({run.loop_overhead_ms / run.total_ms * 100:.1f}%)" if run.total_ms > 0 else "- Overhead: 0 ms")
    print()


def main():
    parser = argparse.ArgumentParser(description="Compare ZPQ benchmark runs")
    parser.add_argument("files", nargs="+", help="JSON trace files to compare")
    parser.add_argument("--baseline", help="Baseline file for comparison")
    parser.add_argument("--sort", choices=["throughput", "time", "selectivity", "name"],
                       default="name", help="Sort order")
    parser.add_argument("--single", action="store_true", help="Print detailed single-run view")

    args = parser.parse_args()

    # Load runs
    runs = []
    for path in args.files:
        try:
            runs.append(Run.from_json(path))
        except Exception as e:
            print(f"Warning: Failed to load {path}: {e}", file=sys.stderr)

    if not runs:
        print("No valid trace files found.", file=sys.stderr)
        sys.exit(1)

    # Sort
    if args.sort == "throughput":
        runs.sort(key=lambda r: r.throughput_mval_s, reverse=True)
    elif args.sort == "time":
        runs.sort(key=lambda r: r.total_ms)
    elif args.sort == "selectivity":
        runs.sort(key=lambda r: r.selectivity)
    else:
        runs.sort(key=lambda r: r.name)

    # Load baseline
    baseline = None
    if args.baseline:
        try:
            baseline = Run.from_json(args.baseline)
        except Exception as e:
            print(f"Warning: Failed to load baseline {args.baseline}: {e}", file=sys.stderr)

    # Output
    if args.single and len(runs) == 1:
        print_single_run(runs[0])
    else:
        print_comparison_table(runs, baseline)


if __name__ == "__main__":
    main()
