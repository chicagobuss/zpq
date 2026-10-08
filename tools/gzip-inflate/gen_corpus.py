"""Writes gzip streams made by zlib itself (levels 0-9, every strategy, memLevel 1-9, multi-member, sizes around
block and window boundaries) for compare.c. Usage: gen_corpus.py OUTDIR COUNT SEED"""
import os, random, sys, zlib

out, count, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
os.makedirs(out, exist_ok=True)
rng = random.Random(seed)
strategies = [zlib.Z_DEFAULT_STRATEGY, zlib.Z_FILTERED, zlib.Z_HUFFMAN_ONLY, zlib.Z_RLE, zlib.Z_FIXED]
sizes = [0, 1, 2, 255, 256, 32767, 32768, 32769, 65535, 65536, 65537, 1 << 20]

def payload(n):
    kind = rng.randrange(5)
    if kind == 0: return os.urandom(n)
    if kind == 1: return bytes(rng.randrange(4) for _ in range(n))
    if kind == 2:
        words = [os.urandom(rng.randint(3, 12)) for _ in range(64)]
        b = bytearray()
        while len(b) < n: b += rng.choice(words)
        return bytes(b[:n])
    if kind == 3: return bytes([rng.randrange(256)]) * n
    period = rng.randint(1, 70000)
    block = os.urandom(period)
    return (block * (n // period + 1))[:n]

def member(data):
    c = zlib.compressobj(rng.randrange(10), zlib.DEFLATED, 31, rng.randint(1, 9), rng.choice(strategies))
    return c.compress(data) + c.flush()

for i in range(count):
    n = rng.choice(sizes) if rng.random() < 0.5 else rng.randint(0, 300000)
    data = payload(n)
    if rng.random() < 0.2:
        cut = rng.randint(0, n)
        stream = member(data[:cut]) + member(data[cut:])
    else:
        stream = member(data)
    with open(os.path.join(out, f"gen-{i:05d}.gz"), "wb") as f:
        f.write(stream)
