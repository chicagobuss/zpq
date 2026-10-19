"""Fixtures for tools/memory_gate.py: DELTA_BYTE_ARRAY expansion and dictionary-heavy strings.

    memory_gate_fixtures.py OUT_DIR    # dba_*, dict_heavy, topn_descending, gather_* .parquet (about 160 MB)
"""
import sys, random
import pyarrow as pa, pyarrow.parquet as pq

out = sys.argv[1]
# 1500 x 100 kB values, one row group: expands ~150 MB from a tiny file (the harden round's slice-sizing shape).
s = "x" * 100_000
t = pa.table({"s": pa.array([s] * 1500, pa.string()), "n": pa.array(range(1500), pa.int64())})
pq.write_table(t, f"{out}/dba_big.parquet", use_dictionary=False, column_encoding={"s": "DELTA_BYTE_ARRAY"},
               compression="zstd", row_group_size=1500)
# 16 row groups of 400 x 64 kB values with shared prefixes: each row group expands ~25 MB, so -j16 holds ~16x.
rows = []
for i in range(6400):
    rows.append(("p" * 60_000) + f"{i:08d}" + ("q" * 4000))
t = pa.table({"s": pa.array(rows, pa.string()), "n": pa.array(range(len(rows)), pa.int64())})
pq.write_table(t, f"{out}/dba_rg16.parquet", use_dictionary=False, column_encoding={"s": "DELTA_BYTE_ARRAY"},
               compression="zstd", row_group_size=400)
# Dictionary-heavy: 4M rows over 1M distinct 120-byte strings, dictionary encoded, 8 row groups.
rng = random.Random(7)
keys = [f"{i:012d}" + "abcdefghij" * 10 + f"{i % 97:08d}" for i in range(1_000_000)]
col = [keys[rng.randrange(len(keys))] for _ in range(4_000_000)]
t = pa.table({"k": pa.array(col, pa.string()), "v": pa.array([rng.randrange(1000) for _ in range(4_000_000)],
                                                             pa.int64())})
pq.write_table(t, f"{out}/dict_heavy.parquet", use_dictionary=True, dictionary_pagesize_limit=1 << 30,
               compression="snappy", row_group_size=500_000)
# 4M descending INT32 in one row group, no statistics: an ascending Top-N's every row evicts the row it kept.
t = pa.Table.from_arrays([pa.array(range(3_999_999, -1, -1), pa.int32())],
                         schema=pa.schema([pa.field("k", pa.int32(), nullable=False)]))
pq.write_table(t, f"{out}/topn_descending.parquet", row_group_size=4_000_000, compression="NONE", use_dictionary=False,
               write_statistics=False)
# A parallel Top-N's gather, one winner per row group: 16 x 5000 rows of 12 KiB strings, one ~60 MiB page each;
# 8 x 150k distinct ~200-byte strings, one ~30 MiB dictionary each (k = i * groups + rg: row 0 of every group wins).
for name, groups, n, width, dictionary in (("gather_big", 16, 5000, 12288, False),
                                           ("gather_dict_big", 8, 150_000, 200, True)):
    ks = [i * groups + rg for rg in range(groups) for i in range(n)]
    ss = [f"{rg:02d}{i:06d}" + "q" * (width - 8) for rg in range(groups) for i in range(n)]
    t = pa.table({"k": pa.array(ks, pa.int64()), "s": pa.array(ss, pa.string())})
    del ks, ss
    pq.write_table(t, f"{out}/{name}.parquet", row_group_size=n, compression="snappy", use_dictionary=dictionary,
                   dictionary_pagesize_limit=1 << 30, data_page_size=1 << 30, write_batch_size=1 << 20,
                   write_statistics=False)
    del t
for f in ("dba_big", "dba_rg16", "dict_heavy", "topn_descending", "gather_big", "gather_dict_big"):
    m = pq.ParquetFile(f"{out}/{f}.parquet").metadata
    c = m.row_group(0).column(0)
    print(f, m.num_row_groups, m.num_rows, c.encodings, c.total_compressed_size, c.total_uncompressed_size)
