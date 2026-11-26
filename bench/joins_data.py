"""Write the `joins` suite's inputs, `~/bench/data/joins/fact.parquet` and `products.parquet`, with the DuckDB CLI.

    joins_data.py [DUCKDB]

`fact`: 3,000,000 rows sorted by `id`, 25 row groups of 122,880 (ZSTD); 5,000 SKUs by four regions. `products`: the
6,668 (sku, region) pairs whose SKU number is a multiple of three, each with a name. A fact row matches when its pair
is among them, so a third of the facts do.
"""

import subprocess
import sys
from pathlib import Path

OUT = Path.home() / "bench/data/joins"
FACT = """SELECT id::BIGINT AS id, (id % 256)::BIGINT AS shard, ((id * 103 + id // 509) % 50_003)::BIGINT AS qty,
  printf('SKU-%05d', id % 5000) AS sku,
  CASE (id // 5000) % 4 WHEN 0 THEN 'north' WHEN 1 THEN 'east' WHEN 2 THEN 'south' ELSE 'west' END AS region
  FROM range(3000000) t(id) ORDER BY id"""
PRODUCTS = """SELECT printf('SKU-%05d', k) AS sku, region, printf('%s product %05d', region, k) AS name
  FROM range(5000) t(k) CROSS JOIN (VALUES ('north'), ('east'), ('south'), ('west')) t2(region) WHERE k % 3 = 0"""


def main(duckdb="duckdb"):
    OUT.mkdir(parents=True, exist_ok=True)
    for name, query in (("fact", FACT), ("products", PRODUCTS)):
        path = OUT / f"{name}.parquet"
        path.unlink(missing_ok=True)
        sql = (f"SET threads=4; COPY ({query}) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)")
        subprocess.run([duckdb, "-no-init", "-c", sql], check=True)


if __name__ == "__main__":
    main(*sys.argv[1:])
