"""Check exact kernels against byte-tuple grouping at every requested worker count."""
import argparse
from collections import Counter
import json
from pathlib import Path
import random
import struct
import subprocess

ap = argparse.ArgumentParser(description=__doc__)
ap.add_argument('binaries', type=Path)
ap.add_argument('scratch', type=Path)
a = ap.parse_args()
a.scratch.mkdir(parents=True, exist_ok=True)
rng = random.Random(923)
keys = []
for n in [0, 1, 7, 8, 9, 16, 17, 19, 20, 31, 63, 127, 128, 129, 200, 255]:
    for i in range(100):
        k = bytes(rng.randrange(256) for _ in range(n))
        keys.extend([(k, i % 3), (k+b'\0' if n < 255 else k, (i+1) % 3)])
rows = [k for i, k in enumerate(keys) for _ in range(1+i % 5)]
rng.shuffle(rows)
cases = {
    'lengths_zero_ties': rows,
    'one_long_group': [(b'a'*254+b'\0', 999)]*200000,
    'one_empty_group': [(b'', 0)]*20000,
    'flat': [(str(i).encode()+b'\0', i % 1000) for i in range(50000)],
    'late_prefix': [(b'x'*192+i.to_bytes(8, 'big'), i % 4) for i in range(2000) for _ in range(3)],
}

def run(binary, *args):
    p = subprocess.run([str(a.binaries / binary), *map(str, args)], capture_output=True, timeout=90)
    if p.returncode:
        raise RuntimeError((binary, args, p.stderr.decode()[-1500:]))
    return json.loads(p.stderr)

checks = 0
for name, rows in cases.items():
    p = a.scratch / name
    p.mkdir(exist_ok=True)
    data = bytearray()
    records = bytearray()
    counts = Counter(rows)
    first = {}
    for i, (k, f) in enumerate(rows):
        records += struct.pack('<IHH', len(data), len(k), f)
        data += k
        first.setdefault((k, f), i)
    (p / 'keys.bin').write_bytes(data or b'\0')
    (p / 'rows.bin').write_bytes(records)
    expected = [dict(count=counts[k], first=first[k], f=k[1])
                for k in sorted(counts, key=lambda k: (-counts[k], first[k]))[:10]]
    for j in [1, 2, 4, 8, 16]:
        for cutoff, mode in [(0, 'prune'), (32, 'full'), (0, 'tail'), (32, 'tailfull')]:
            d = run('msd-probe', p, j, cutoff, 0, 11, mode)
            assert d['top'] == expected, (name, j, mode, d['top'], expected)
            if mode.endswith('full'):
                assert d['groups_finished'] == len(counts) and d['rows_finished'] == len(rows)
            checks += 1
        d = run('intern-probe', p, j, 64)
        assert d['top'] == expected and d['groups'] == len(counts) and d['counted'] == len(rows)
        checks += 1
    threshold = expected[-1]['count']
    required = sum(c for c in counts.values() if c >= threshold)
    for bits in [8, 16]:
        for j in [1, 16]:
            d = run('sketch-probe', p, j, threshold, bits)
            assert d['survivor_rows'] >= required
            checks += 1
p = a.scratch / 'lengths_zero_ties'
q = subprocess.run([str(a.binaries / 'msd-probe'), str(p), '4', '16', '20', '11'], capture_output=True)
assert q.returncode and b'PrefixNotVerified' in q.stderr, q.stderr
q = subprocess.run([str(a.binaries / 'msd-probe'), str(p), '4', '16', '0', '0'], capture_output=True)
assert q.returncode and b'BadArgument' in q.stderr, q.stderr
print(f'{checks} kernel checks passed; false prefix and zero-bit radix rejected')
