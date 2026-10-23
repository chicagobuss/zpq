"""Extract owned byte keys and compact (offset,length,f) rows for the standalone kernels."""
import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ap = argparse.ArgumentParser(description=__doc__)
ap.add_argument('input', type=Path)
ap.add_argument('output', type=Path)
a = ap.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
source = pq.ParquetFile(a.input)
base = rows = 0
prefix = b'https://example.org/'
prefix_verified = True
max_length = 0
with (a.output / 'keys.bin').open('wb') as keys, (a.output / 'rows.bin').open('wb') as records:
    for batch in source.iter_batches(columns=['k', 'f'], batch_size=65536):
        k = pc.cast(batch.column(0), pa.binary())
        f = batch.column(1)
        if k.null_count or f.null_count:
            raise ValueError('This byte-kernel probe requires non-NULL k and f')
        offsets = np.frombuffer(k.buffers()[1], dtype='<i4', count=len(k)+1, offset=k.offset*4)
        lengths = np.diff(offsets)
        fs = f.to_numpy()
        size = int(offsets[-1]-offsets[0])
        if lengths.max(initial=0) > 255 or fs.min(initial=0) < 0 or fs.max(initial=0) >= 1000:
            raise ValueError('Probe requires length <= 255 and 0 <= f < 1000')
        if base+size >= 2**32 or rows+len(k) >= 2**25:
            raise ValueError('Probe offset or ordinal domain exceeded')
        packed = np.empty(len(k), dtype=[('offset', '<u4'), ('length', '<u2'), ('f', '<u2')])
        packed['offset'] = offsets[:-1].astype(np.uint64)-int(offsets[0])+base
        packed['length'] = lengths
        packed['f'] = fs
        keys.write(memoryview(k.buffers()[2])[int(offsets[0]):int(offsets[-1])])
        packed.tofile(records)
        max_length = max(max_length, int(lengths.max(initial=0)))
        prefix_verified &= pc.all(pc.starts_with(k, pattern=prefix)).as_py()
        base += size
        rows += len(k)
if not rows:
    raise ValueError('Probe requires nonempty input')
con = duckdb.connect(config={'threads': 6, 'memory_limit': '4GB'})
path = str(a.input.resolve()).replace("'", "''")
truth = con.execute(f"""
    SELECT k, f, count(*) AS c, min(file_row_number) AS first,
           count(*) OVER() AS groups
    FROM read_parquet('{path}', file_row_number=true)
    GROUP BY k, f ORDER BY c DESC, first LIMIT 10
""").fetchall()
meta = dict(rows=rows, bytes=base, max_length=max_length, groups=truth[0][4],
            prefix=prefix.decode(), prefix_verified=prefix_verified,
            top=[dict(key=k if isinstance(k, str) else k.hex(), f=f, count=c, first=i) for k, f, c, i, _ in truth])
(a.output / 'meta.json').write_text(json.dumps(meta, indent=2)+'\n')
print(json.dumps({k: meta[k] for k in ['rows', 'bytes', 'groups', 'prefix_verified']}))
