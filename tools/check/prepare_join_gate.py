"""A join whose foreign key's page over rows [40000, 80000) is corrupt, and those rows a cheaper conjunct rejects.

`fk` is PLAIN and read only by the join's lookup conjunct, so every path decodes it after `sd >= '1994-01-01'`, gated
by that conjunct's survivors, and never opens the page (prepare_conjunct_gate.py's single-table shape, with the
lookup as the later conjunct). Arguments: the corrupt fact file; the same rows uncorrupted (for the oracle); the
dimension it joins.
"""
import datetime
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from prepare_conjunct_gate import pages  # noqa: E402


def fact(out, corrupt):
    n = 200_000
    sd = [datetime.date(1993, 6, 1) if 40000 <= i < 80000 else datetime.date(1994, 6, 1) for i in range(n)]
    fk = [None if i % 9 == 4 else (i * 7919) % 1200 for i in range(n)]
    amt = [float(i % 101) / 4 for i in range(n)]
    pq.write_table(
        pa.table({"sd": pa.array(sd, type=pa.date32()), "fk": pa.array(fk, type=pa.int64()),
                  "amt": pa.array(amt, type=pa.float64())}),
        out,
        row_group_size=n,
        use_dictionary=["sd"],
        compression=None,
        data_page_size=64 * 1024,
        data_page_version="1.0",
        write_statistics=False,
    )
    if not corrupt:
        return
    orig = out.read_bytes()
    meta = pq.ParquetFile(out).metadata
    pp = pages(orig, meta, 0, 1)
    k = next(i for i, x in enumerate(pp) if x[2] >= 40000 and x[2] + x[3] <= 80000)
    data = bytearray(orig)
    # The page's definition-level length: no reader can decode it.
    data[pp[k][0] : pp[k][0] + 4] = b"\xff\xff\xff\xff"
    out.write_bytes(data)


def main():
    fact(pathlib.Path(sys.argv[1]), True)
    fact(pathlib.Path(sys.argv[2]), False)
    pk = list(range(1000))
    pq.write_table(pa.table({"pk": pa.array(pk, type=pa.int64()), "grp": pa.array([k % 7 for k in pk], type=pa.int64())}),
                   pathlib.Path(sys.argv[3]))


if __name__ == "__main__":
    main()
