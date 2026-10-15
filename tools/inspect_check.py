#!/usr/bin/env python3
"""Cross-check footer reports, typed bounds and bounded local/S3 detail reads."""
import argparse
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pyarrow as pa
import pyarrow.parquet as pq


def run(binary, *args, env=None, ok=True):
    p = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, env=env, timeout=30)
    if ok:
        assert p.returncode == 0, (args, p.returncode, p.stderr, p.stdout[:100])
    return p


def check(binary, path, detail=False, env=None):
    p = run(binary, "inspect", path, "--format", "json", *(["--detail"] if detail else []), env=env)
    report = json.loads(p.stdout)
    assert report["schema_version"] == 1
    f = report["files"][0]
    return f


def compare(f, path):
    meta = pq.ParquetFile(path).metadata
    assert f["rows"] == meta.num_rows
    assert f["parquet_version"] in (1, 2)
    assert (f["created_by"] or "") == meta.created_by
    assert len(f["row_groups"]) == meta.num_row_groups
    for rg, reported in enumerate(f["row_groups"]):
        group = meta.row_group(rg)
        assert reported["rows"] == group.num_rows
        assert reported["uncompressed_bytes"] == group.total_byte_size
        assert reported["sorting_columns"] == "unsupported"
        for ci, c in enumerate(reported["columns"]):
            truth = group.column(ci)
            assert ".".join(c["path"]) == truth.path_in_schema, c["path"]
            assert c["compressed_bytes"] == truth.total_compressed_size
            assert c["uncompressed_bytes"] == truth.total_uncompressed_size
            assert c["num_values"] == truth.num_values
            assert c["data_page_offset"] == truth.data_page_offset
            assert c["dictionary_page_offset"] == truth.dictionary_page_offset
            same_codec = c["codec"].replace("LZ4_RAW", "LZ4") == truth.compression
            assert same_codec or (c["codec"] == "LZ4" and truth.compression == "UNKNOWN")
            assert set(c["encodings"]) == set(truth.encodings)
            assert (c["column_index"]["state"] == "present") == truth.has_column_index
            assert (c["offset_index"]["state"] == "present") == truth.has_offset_index
            stats = truth.statistics
            # PyArrow hides legacy byte-array stats from known broken writers; inspect retains raw footer evidence.
            if stats is not None:
                assert c["statistics"] == "present"
            if stats:
                assert c["null_count"] == stats.null_count
                if stats.has_min_max:
                    for which in ("min", "max"):
                        b = c[which]
                        if b["state"] == "unsupported":
                            continue
                        assert b["state"] == "present", (path, ci, b)
                        raw = getattr(stats, which + "_raw")
                        value = b["value"]
                        if b["kind"] == "DECIMAL":
                            assert Decimal(value) == getattr(stats, which)
                        elif isinstance(raw, bytes):
                            assert bytes.fromhex(b["raw_hex"]) == raw
                        elif isinstance(raw, bool):
                            assert value == str(raw).lower()
                        elif isinstance(raw, int):
                            # min_raw is the signed physical word even for an unsigned logical integer.
                            logical = c["logical_type"] or {}
                            unsigned = logical.get("INTEGER", {}).get("isSigned") is False
                            assert int(value) == (getattr(stats, which) if unsigned else raw)
                        elif isinstance(raw, float):
                            both_nan = math.isnan(float(value)) and math.isnan(raw)
                            assert both_nan or math.isclose(float(value), raw, rel_tol=1e-7, abs_tol=1e-40)
    assert f["io"]["detail_bytes"] <= 8 << 20


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", default="zig-out/bin/zpq")
    parser.add_argument("--corpus")
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    help_text = run(binary, "--help").stdout
    assert help_text == (Path(__file__).resolve().parents[1] / "src/cli/help.txt").read_text()
    assert all(len(line) <= 80 for line in help_text.splitlines())
    assert run(binary, "schema", "--help").stdout == help_text
    assert run(binary, "inspect", "--help").stdout == help_text
    count = 0
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        table = pa.table({"id": [1, 2, 3, 4], "s": ["é", None, "x", "long"],
                          "decimal": pa.array([Decimal("1.23"), Decimal("-4.56"), None, Decimal("9.99")],
                                              type=pa.decimal128(20, 2))})
        for codec in ("snappy", "gzip", "zstd", "none"):
            path = root / f"{codec}.parquet"
            pq.write_table(table, path, compression=codec, row_group_size=2, write_page_index=True)
            for detail in (False, True):
                f = check(binary, path, detail)
                compare(f, path)
                if not detail:
                    assert f["io"]["requested_bytes"] == f["footer_bytes"] + 12
                else:
                    for g in f["row_groups"]:
                        for c in g["columns"]:
                            assert c["detail_state"] == "complete", c
                            assert c["column_index"]["validation"] == "valid-structure", c
                count += 1
        path = root / "absent.parquet"
        pq.write_table(table, path, write_statistics=False)
        compare(check(binary, path), path)
        count += 1
        bad = root / "truncated.parquet"
        bad.write_bytes(path.read_bytes()[:-5])
        p = run(binary, "inspect", bad, path, "--format", "json", ok=False)
        doc = json.loads(p.stdout)
        assert p.returncode == 1 and doc["files"][0]["status"] == "error" and doc["files"][1]["status"] == "ok"
        count += 1
        body = (root / "snappy.parquet").read_bytes()
        requests = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                r = self.headers["Range"].removeprefix("bytes=")
                if r.startswith("-"):
                    lo, hi = len(body) - int(r[1:]), len(body) - 1
                else:
                    lo, hi = map(int, r.split("-"))
                requests.append((lo, hi))
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {lo}-{hi}/{len(body)}")
                self.send_header("Content-Length", str(hi - lo + 1))
                self.end_headers()
                self.wfile.write(body[lo:hi + 1])

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            env = {**os.environ, "S3_NO_SIGN_REQUEST": "1", "AWS_REGION": "us-east-1",
                   "S3_ENDPOINT_URL": f"http://127.0.0.1:{server.server_port}"}
            f = check(binary, "s3://test/snappy.parquet", env=env)
            compare(f, root / "snappy.parquet")
            assert len(requests) == f["io"]["range_requests"] == 4
            assert sum(hi - lo + 1 for lo, hi in requests) == f["io"]["requested_bytes"]
            count += 1
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        if args.corpus:
            errors = []
            for path in sorted(Path(args.corpus).glob("*.parquet")):
                try:
                    pq.ParquetFile(path)
                except Exception:
                    continue
                if path.name == "negative_data_page_offset.parquet":
                    p = run(binary, "inspect", path, "--format", "json", ok=False)
                    assert p.returncode == 1 and json.loads(p.stdout)["files"][0]["cause"] == "BadColumnChunkRange"
                    count += 1
                    continue
                try:
                    compare(check(binary, path), path)
                    count += 1
                except Exception as e:
                    errors.append({"path": str(path), "error": traceback.format_exc()[-1200:]})
            Path("scratch/inspect-corpus-errors.json").write_text(json.dumps(errors, indent=2))
            assert not errors, errors
    print(f"PASS: {count} footer/detail/typed/corrupt/S3 checks")


if __name__ == "__main__":
    main()
