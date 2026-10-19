#!/usr/bin/env python3
"""Check that a reducing head before a large dictionary tail needs one aggregate scan.

Run under zpq-heavy; this is correctness and scan-count validation, not a timing harness.
The fixture reproduces the decoder-floor cancellation regression with four row groups.
"""
import argparse
import collections
import json
from pathlib import Path
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq


def fixture(path):
    writer = None
    try:
        for rg in range(4):
            head = [f'head-{i % 1000:04d}-' + 'h' * 70 for i in range(100_000)]
            tail = [f'uniq-{rg:02d}-{i:07d}-' + 'u' * 70 for i in range(900_000)]
            table = pa.table({'k': head + tail, 'f': pa.array([i % 7 for i in range(1_000_000)], pa.int32())})
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, use_dictionary=['k'],
                                          dictionary_pagesize_limit=1 << 30, compression='snappy')
            writer.write_table(table, row_group_size=1_000_000)
    finally:
        if writer is not None:
            writer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path)
    parser.add_argument('--out-dir', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, help='reuse the same reducing-head fixture instead of generating it')
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    path = (args.fixture or args.out_dir / 'reducing_head.parquet').resolve()
    if not args.fixture and not path.exists():
        fixture(path)
    meta = pq.read_metadata(path)
    assert meta.num_rows == 4_000_000 and meta.num_row_groups == 4
    counts = collections.Counter((f'head-{i % 1000:04d}-' + 'h' * 70, i % 7) for i in range(100_000))
    ordered = sorted(counts, key=lambda key: (-counts[key], key))[:3]
    expected = [{'k': k, 'f': f, 'c': counts[k, f] * 4} for k, f in ordered]
    query = f"SELECT k,f,count(*) c FROM '{path}' GROUP BY k,f ORDER BY c DESC,k,f LIMIT 3"
    rows = []
    for jobs in [4, 16]:
        trace = (args.out_dir / f'j{jobs}.trace.json').resolve()
        run = subprocess.run([str(args.binary.resolve()), 'query', '--query', query, '-j', str(jobs),
                              '--max-memory', '4GB', '--profile', str(trace)], capture_output=True, text=True)
        events = json.loads(trace.read_text())['traceEvents'] if run.returncode == 0 else []
        scans = sum(event.get('name') == 'plan' for event in events)
        answer = [json.loads(line) for line in run.stdout.splitlines()]
        row = dict(jobs=jobs, budget=4 << 30, rc=run.returncode, scans=scans, answer=answer, stderr=run.stderr)
        rows.append(row)
        (args.out_dir / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
        assert run.returncode == 0 and answer == expected and scans == 1, row
    print('PASS: reducing dictionary head/tail, one scan at j4/j16; exact known answer')


if __name__ == '__main__':
    main()
