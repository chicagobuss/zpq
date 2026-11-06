"""The layout recipe's fan-out input (eight row groups of one string column), with the compressed body of row group 7's
first data page overwritten: a write that fans it out under a tight budget may be refused over the reader and run
again over the mapping, and must then report this page, as a mapped run alone does.

Argument: the output file.
"""
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from prepare_conjunct_gate import pages


def main():
    out = pathlib.Path(sys.argv[1])
    values = pa.array([f"v-{i:08x}" for i in range(100_003)])
    with pq.ParquetWriter(out, pa.schema([("c0", pa.string())]), compression="zstd") as w:
        for _ in range(8):
            w.write_table(pa.table({"c0": values}))
    raw = out.read_bytes()
    body = pages(raw, pq.ParquetFile(out).metadata, 7, 0)[0][0]
    data = bytearray(raw)
    data[body : body + 8] = b"\xff" * 8
    out.write_bytes(data)


if __name__ == "__main__":
    main()
