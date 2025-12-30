#!/usr/bin/env python3
"""
Load ZPQ trace JSON files to Jaeger via OTLP.

Usage:
    python load_to_jaeger.py trace.json
    python load_to_jaeger.py traces/*.json --endpoint http://localhost:4318

Requires: pip install opentelemetry-exporter-otlp-proto-http
"""

import json
import sys
import argparse
import time
import hashlib
from pathlib import Path

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


def generate_trace_id(run_data: dict) -> str:
    """Generate a deterministic trace ID from run metadata."""
    key = f"{run_data.get('timestamp_ms', 0)}:{run_data.get('file_path', '')}:{run_data.get('git_commit', '')}"
    return hashlib.md5(key.encode()).hexdigest()


def load_trace_to_jaeger(path: str, endpoint: str):
    """Load a ZPQ trace JSON file and send to Jaeger."""

    with open(path) as f:
        data = json.load(f)

    run = data["run"]
    metrics = data["metrics"]

    # Setup OTLP exporter
    resource = Resource.create({
        "service.name": "zpq",
        "service.version": run.get("git_commit", "unknown")[:7] if run.get("git_commit") else "unknown",
    })

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    tracer = trace.get_tracer("zpq.bench")

    # Create spans from metrics
    # We create synthetic spans based on the aggregated timing
    trace_id = generate_trace_id(run)

    # Convert ms to ns for span timing
    def ms_to_ns(ms: float) -> int:
        return int(ms * 1_000_000)

    # Root span
    with tracer.start_as_current_span("cmdScan") as root:
        root.set_attribute("file_path", run.get("file_path", ""))
        root.set_attribute("file_size_bytes", run.get("file_size_bytes", 0))
        root.set_attribute("total_rows", run.get("total_rows", 0))
        root.set_attribute("filter_column", run.get("filter_column") or "")
        root.set_attribute("filter_value", run.get("filter_value") or "")
        root.set_attribute("selectivity", metrics.get("selectivity", 0))
        root.set_attribute("throughput_mval_s", metrics.get("throughput_mval_s", 0))

        # Simulate child spans based on timing breakdown
        # (In reality these happened concurrently, but this gives a visual representation)

        with tracer.start_as_current_span("filter_decode") as span:
            span.set_attribute("duration_ms", metrics.get("filter_decode_ms", 0))
            span.set_attribute("rows", metrics.get("rows_scanned", 0))
            time.sleep(0.001)  # Small delay to ensure span ordering

        with tracer.start_as_current_span("skip") as span:
            span.set_attribute("duration_ms", metrics.get("skip_ms", 0))
            span.set_attribute("rows_skipped", metrics.get("rows_skipped", 0))
            span.set_attribute("batches_skipped", metrics.get("batches_skipped", 0))
            time.sleep(0.001)

        with tracer.start_as_current_span("materialize") as span:
            span.set_attribute("duration_ms", metrics.get("materialize_ms", 0))
            span.set_attribute("rows_selected", metrics.get("rows_selected", 0))
            span.set_attribute("batches_processed", metrics.get("batches_processed", 0))
            time.sleep(0.001)

        with tracer.start_as_current_span("loop_overhead") as span:
            span.set_attribute("duration_ms", metrics.get("loop_overhead_ms", 0))
            time.sleep(0.001)

    # Force flush
    provider.force_flush()

    print(f"Loaded {Path(path).name} to Jaeger")
    print(f"  Trace ID: {trace_id}")
    print(f"  Total: {metrics.get('total_ms', 0):.2f}ms, Throughput: {metrics.get('throughput_mval_s', 0):.2f} MVal/s")


def main():
    parser = argparse.ArgumentParser(description="Load ZPQ traces to Jaeger")
    parser.add_argument("files", nargs="+", help="JSON trace files to load")
    parser.add_argument("--endpoint", default="http://localhost:4318",
                       help="OTLP HTTP endpoint (default: http://localhost:4318)")

    args = parser.parse_args()

    if not OTEL_AVAILABLE:
        print("OpenTelemetry not installed. Run: pip install opentelemetry-exporter-otlp-proto-http")
        sys.exit(1)

    for path in args.files:
        try:
            load_trace_to_jaeger(path, args.endpoint)
        except Exception as e:
            print(f"Error loading {path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
