"""Parquet recipes; values, encodings and seeds define each regression shape."""
import decimal
import random
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq
from .parquet import T_I32, field, rewrite_footer


def packed_count(out_dir):
    """Indexed nullable dictionary tuples for packed composite COUNT."""
    import numpy as np
    path = out_dir / 'packed_count_indexed.parquet'
    rg_rows = 1281
    r = np.arange(16 * rg_rows, dtype=np.int64)
    rg = r // rg_rows
    k = (r + rg * 17) % 257
    k = np.where(k == 0, np.iinfo(np.int64).min,
                 np.where(k == 1, np.iinfo(np.int64).max, k * (1 << 45) - (1 << 54)))
    b = np.array(['', 'a\x00b', 'last'], dtype=object)[(r + rg) % 3]
    table = pa.table({
        'k': pa.array(k, mask=r % 17 == 0),
        'b': pa.array(b, mask=r % 19 == 0),
        'f': pa.array(((r * 3 + rg) % 263 - 131).astype(np.int32), mask=r % 23 == 0),
    })
    pq.write_table(table, path, row_group_size=rg_rows, compression='NONE', write_page_index=True,
                   data_page_size=512, write_batch_size=128)
    assert all(pq.ParquetFile(path).metadata.row_group(g).column(c).has_offset_index
               for g in range(16) for c in range(3))
    print(path)


def lambda_int8(out_dir):
    """Four row groups straddling the diagnostic decoder's 4096-value batch."""
    values = [-128, -2, -1, 0, 1, 2, 126, 127] * 2049
    table = pa.table({'int8': pa.array(values[:4097 * 4], type=pa.int8())})
    path = out_dir / 'lambda_int8.parquet'
    pq.write_table(table, path, row_group_size=4097)
    print(path)


def bloom(out_dir):
    """ci/fixtures/parquet/bloom_keys.parquet, the fixture for row-group pruning by membership (Bloom filters
and dictionaries) in the unit and Lambda integration tests."""
    ROWS_PER_GROUP = 500
    GROUPS = 8
    OUT = out_dir / "bloom_keys.parquet"
    ids, cats = [], []
    for g in range(GROUPS):
        for j in range(ROWS_PER_GROUP):
            ids.append(2 * (8 * j + g))
            cats.append(("a", "m", "z")[j % 3] + f"-{g}")
    table = pa.table({
        "id": pa.array(ids, pa.int64()),
        "s": pa.array([f"key-{i:06d}" for i in ids], pa.string()),
        "cat": pa.array(cats, pa.string()),
        "x": pa.array([i / 2 for i in ids], pa.float64()),
    })
    pq.write_table(
        table,
        OUT,
        row_group_size=ROWS_PER_GROUP,
        compression="snappy",
        use_dictionary=["cat"],
        bloom_filter_options={c: {"ndv": ROWS_PER_GROUP, "fpp": 0.01} for c in ("id", "s")},
    )
    md = pq.ParquetFile(OUT).metadata
    assert md.num_row_groups == GROUPS
    for g in range(GROUPS):
        rg = md.row_group(g)
        assert rg.column(0).bloom_filter_offset and rg.column(1).bloom_filter_offset
        assert not rg.column(2).bloom_filter_offset
    print(OUT, OUT.stat().st_size, "bytes")


def census(out_dir):
    """the near-unique GROUP BY fixture for the census top-k tests in src/core/scan.zig."""
    ROWS_PER_GROUP = 1000
    GROUPS = 8
    OUT = out_dir / "census_topk.parquet"
    rng = random.Random(20261012)
    n = ROWS_PER_GROUP * GROUPS
    u = rng.sample(range(1, 10**12), n - 3)
    u += u[:3]  # three keys seen twice
    rng.shuffle(u)
    t = []
    for i in range(4):
        t += [10**9 + i] * 5
    for i in range(12):
        t += [2 * 10**9 + i] * 3
    t += rng.sample(range(1, 10**8), n - len(t))
    rng.shuffle(t)
    s = [f"key-{v:015d}" for v in u]
    nulls = [None] * ROWS_PER_GROUP + [1] * (n - ROWS_PER_GROUP)
    table = pa.table({
        "u": pa.array(u, pa.int64()),
        "t": pa.array(t, pa.int64()),
        "s": pa.array(s, pa.string()),
        "n": pa.array(nulls, pa.int64()),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, use_dictionary=False, compression="snappy")
    print(f"wrote {OUT}")
    sliced = OUT.with_name("census_sliced.parquet")
    k = list(range(40)) + list(range(3, 10)) * 6
    pq.write_table(pa.table({"k": pa.array(k, pa.int64())}), sliced, row_group_size=20, use_dictionary=False)
    print(f"wrote {sliced}")
    wide = OUT.with_name("census_widefilter.parquet")
    nk = 12000
    pq.write_table(pa.table({
        "k": pa.array(range(nk), pa.int64()),
        "p": pa.array([("yes" if i % 2 else "no") + "x" * 200 for i in range(nk)], pa.string()),
    }), wide, use_dictionary=False, compression="NONE", write_statistics=False)
    print(f"wrote {wide}")
    heavy = OUT.with_name("census_heavy.parquet")
    h = [7, 8, 9] * 1500 + rng.sample(range(10**3, 10**12), n - 4500)
    rng.shuffle(h)
    g = sorted(h, key=lambda x: x >= 10**3)
    pq.write_table(pa.table({"h": pa.array(h, pa.int64()), "g": pa.array(g, pa.int64()),
                             "v": pa.array(range(n), pa.int64())}), heavy,
                   row_group_size=ROWS_PER_GROUP, use_dictionary=False, compression="snappy")
    print(f"wrote {heavy}")
    dict_fixture = OUT.with_name("census_dictionary_review.parquet")
    dict_table = table.set_column(1, "t", pa.array([i % 7 for i in range(table.num_rows)], pa.int64()))
    pq.write_table(dict_table, dict_fixture, row_group_size=ROWS_PER_GROUP, use_dictionary=True, compression="snappy")
    print(f"wrote {dict_fixture}")


def count_certificate(out_dir):
    """Clustered integer COUNT keys with NULLs, ties and late out-of-range secondary values."""
    rows = 32768
    k = [i - rows for i in range(rows // 4)]
    k.extend(100 + (i % 3) for i in range(rows - len(k)))
    f = [(i % 5) - 2 for i in range(rows)]
    for i in range(0, rows, 137):
        k[i] = None
    for i in range(0, rows, 3):
        f[i] = None
    f[-1], f[-2] = -(1 << 63), (1 << 63) - 1
    s = [None if value is None else str(value) for value in k]
    v = [None if i % 11 == 0 else (i % 31) - 15 for i in range(rows)]
    table = pa.table({"k": pa.array(k, pa.int64()), "f": pa.array(f, pa.int64()),
                      "s": pa.array(s, pa.string()), "v": pa.array(v, pa.int64())})
    root = out_dir
    for coded in (False, True):
        path = root / f"count_certificate_{'dict' if coded else 'plain'}.parquet"
        pq.write_table(table, path, row_group_size=2048, data_page_size=1024,
                       use_dictionary=coded, dictionary_pagesize_limit=1024, compression="snappy")
        print(path)


def decimal_exact(out_dir):
    """the fixture for the exact-DECIMAL tests in src/core/scan.zig."""
    D = decimal.Decimal
    ROWS_PER_GROUP = 100
    GROUPS = 3
    OUT = out_dir / "decimal_exact.parquet"
    def dn(r: int) -> D | None:
        if r // ROWS_PER_GROUP == 1 or r % 3 == 0:
            return None
        return D(r) * D("2.50") - D("100.00")
    decimal.getcontext().prec = 60
    rows = range(ROWS_PER_GROUP * GROUPS)
    big = D(10) ** 38 - 1
    table = pa.table(
        {
            "k": pa.array(["abc"[r % 3] for r in rows], pa.string()),
            "i": pa.array([r - 150 for r in rows], pa.int32()),
            "d9": pa.array([(D(r) - 150) * D("1.01") for r in rows], pa.decimal128(9, 2)),
            "d18": pa.array([D(r) * D("0.0001") + D("0.1") for r in rows], pa.decimal128(18, 4)),
            "d38": pa.array([D("12345678901234567890123456.1234567891") * (1 if r % 2 else -1) + r for r in rows],
                            pa.decimal128(38, 10)),
            "big": pa.array([big if r < 2 else D(1) for r in rows], pa.decimal128(38, 0)),
            "dn": pa.array([dn(r) for r in rows], pa.decimal128(15, 2)),
            "q": pa.array([D(r % 4) for r in rows], pa.decimal128(15, 2)),
        }
    )
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression="snappy", use_dictionary=True,
                   store_decimal_as_integer=True, write_statistics=True)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


def dotted_twin(out_dir):
    """the fixture for output naming of a nested leaf whose dotted path a top-level column takes."""
    OUT = out_dir / "dotted_twin.parquet"
    def make_table() -> pa.Table:
        return pa.table({
            "a.b": pa.array([1, None, 3], pa.int64()),
            "a": pa.array([{"b": 10}, {"b": 20}, {"b": None}], pa.struct([("b", pa.int64())])),
        })
    out = OUT
    pq.write_table(make_table(), out, compression="NONE")
    print(f"wrote {out}")


def full_match(out_dir):
    """the fixture for the full-match (filter-only column skip) tests in src/core/scan.zig."""
    ROWS_PER_GROUP = 200
    GROUPS = 8
    OUT = out_dir / "full_match.parquet"
    def nullable(ts: int) -> int | None:
        rg = ts // ROWS_PER_GROUP
        if rg == 1:
            return None
        if rg in (0, 2, 3) and ts % 5 == 0:
            return None
        return ts * 3
    ts = list(range(ROWS_PER_GROUP * GROUPS))
    table = pa.table(
        {
            "ts": pa.array(ts, type=pa.int64()),
            "b": pa.array([t // 100 for t in ts], type=pa.int32()),
            "s": pa.array([f"k{t:06d}-" + "p" * 24 for t in ts], type=pa.string()),
            "x": pa.array([(t % 7) * 1.5 - 3.0 for t in ts], type=pa.float64()),
            "n": pa.array([nullable(t) for t in ts], type=pa.int64()),
            "u": pa.array(ts, type=pa.uint32()),
        }
    )
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression="snappy", write_page_index=False)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


def json_escape(out_dir):
    """the fixture for the Lambda JSON-escaping integration test in tests/lambda_integration.zig."""
    OUT = out_dir / "json_escape.parquet"
    table = pa.table({
        "s": pa.array(['a "q" c:\\d\nnext\x01end', "b", "c"], pa.string()),
        "x": pa.array([1.0, float("nan"), 2.0], pa.float64()),
        "y": pa.array([1.0, float("inf"), 2.0], pa.float64()),
        "b": pa.array([b"\xff\xfeA\xe2\x80", b"\xed\xa0\x80", "\u2028".encode()], pa.binary()),
    })
    pq.write_table(table, OUT, compression="NONE")
    print(f"wrote {OUT}")


def list_int64(out_dir):
    """the LIST fixture for the repeated-leaf refusal tests in src/core/scan.zig."""
    OUT = out_dir / "list_int64.parquet"
    t = pa.table({"id": pa.array(range(6), pa.int64()),
                  "l": pa.array([[1, 2], [], None, [None], [3], [4, 5, 6]], pa.list_(pa.int64()))})
    pq.write_table(t, OUT)


def many_tiny(out_dir):
    """the many-tiny-row-groups fixture for the write property test in src/engine.zig."""
    ROWS_PER_GROUP = 4
    GROUPS = 512
    OUT = out_dir / "many_tiny.parquet"
    n = ROWS_PER_GROUP * GROUPS
    table = pa.table({
        "id": pa.array(range(n), pa.int64()),
        "g": pa.array([i % 7 for i in range(n)], pa.int32()),
        "d": pa.array([Decimal(i - 1000) / 100 for i in range(n)], pa.decimal128(18, 2)),
        "s": pa.array([None if i % 11 == 0 else f"s{i % 97}" for i in range(n)]),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression="zstd")
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


def min_fill(out_dir):
    """the fixture for the page min-fill tests in src/core/scan.zig."""
    ROWS = 1000
    OUT = out_dir / "min_fill_or.parquet"
    i = list(range(ROWS))
    table = pa.table(
        {
            "c": pa.array([1 + v % 2 for v in i], pa.int64()),
            "b": pa.array([1 if v % 7 == 0 else 0 for v in i], pa.int64()),
            "x": pa.array(i, pa.int64()),
        }
    )
    pq.write_table(
        table,
        OUT,
        row_group_size=ROWS,
        data_page_size=1,
        write_batch_size=50,
        write_page_index=True,
        use_dictionary=False,
        compression="NONE",
    )


def mixed_codec(out_dir):
    """the mixed-codec fixture for the windowed S3 write tests in src/engine.zig."""
    ROWS_PER_GROUP = 8000
    GROUPS = 6
    OUT = out_dir / "mixed_codec.parquet"
    n = ROWS_PER_GROUP * GROUPS
    table = pa.table({
        "a": pa.array(range(n), pa.int64()),
        "b": pa.array([f"s{i % 5000}" for i in range(n)]),
        "c": pa.array([i * 0.5 for i in range(n)]),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression={"a": "snappy", "b": "zstd", "c": "gzip"})
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


def nested(out_dir):
    """a parquet fixture deliberately designed to exercise every
weird edge case in our nested-column encode path:"""
    OUT_LOCAL = out_dir / "nested_edges.parquet"
    ROWS = [
        (0, 10, ['a', 'b'], {'k': 'x', 'v': 1}, [{'ts': 100, 'code': 1}], {'foo': 1}),
        (1, None, [], {'k': 'y', 'v': 2}, [], {'foo': 2, 'bar': 3}),
        (2, 20, None, None, None, None),
        (3, 30, ['a'], {'k': 'z', 'v': None}, [{'ts': 200, 'code': 2}, {'ts': 201, 'code': 3}], {'foo': 4}),
        (4, None, ['a', None, 'b'], {'k': None, 'v': 5}, [{'ts': 300, 'code': 4}], {}),
        (5, 40, [None], {'k': 'single', 'v': 6}, None, {'a': 1, 'b': 2, 'c': 3, 'd': 4}),
        (6, 50, ['x'] * 50, {'k': 'long', 'v': 7}, [{'ts': i, 'code': i} for i in range(50)], None),
        (7, None, None, {'k': 'n', 'v': None}, [], {'k1': None}),
        (8, 60, [], None, [{'ts': 0, 'code': 0}], {'empty_string_key': 0}),
        (9, 70, ['only'], {'k': 'y', 'v': 8}, None, None),
        (10, None, [None, None, None], None, [{'ts': 1, 'code': 1}, {'ts': 2, 'code': 2}], {'a': 0}),
        (11, 80, ['mixed', None, 'values'], {'k': 'm', 'v': 9}, [], {'x': 1, 'y': 2}),
        (12, 90, None, None, None, None),
        (13, 100, ['ε', 'λ', 'π'], {'k': 'unicode', 'v': 10}, [{'ts': 10, 'code': -1}], {'y': -1}),
        (14, None, [], {'k': 'empty_tags', 'v': 11}, [], {'z': -2}),
        (15, 110, ['a'] * 1, None, [{'ts': 999, 'code': 7}], None),
        (16, 120, ['b'], {'k': 'z', 'v': None}, None, {'deep': 1}),
        (17, None, None, None, None, None),
        (18, 130, ['x', 'y'], {'k': 'z', 'v': 13}, [{'ts': 50, 'code': 8}, {'ts': 51, 'code': 9}], {'k': 5}),
        (19, 140, ['last'], {'k': 'last', 'v': 14}, [{'ts': 60, 'code': 10}], {'final': 1}),
    ]
    def build_table() -> pa.Table:
        ids = pa.array([r[0] for r in ROWS], type=pa.int64())
        scores = pa.array([r[1] for r in ROWS], type=pa.int32())
        tags = pa.array([r[2] for r in ROWS], type=pa.list_(pa.string()))
        meta = pa.array(
            [r[3] for r in ROWS],
            type=pa.struct([("k", pa.string()), ("v", pa.int32())]),
        )
        events = pa.array(
            [r[4] for r in ROWS],
            type=pa.list_(pa.struct([("ts", pa.int64()), ("code", pa.int32())])),
        )
        counts = pa.array(
            [r[5] for r in ROWS],
            type=pa.map_(pa.string(), pa.int32()),
        )
        return pa.table({
            "id": ids,
            "score": scores,
            "tags": tags,
            "meta": meta,
            "events": events,
            "counts": counts,
        })
    OUT_LOCAL.parent.mkdir(parents=True, exist_ok=True)
    table = build_table()
    pq.write_table(table, OUT_LOCAL, compression="snappy")
    print(f"wrote {OUT_LOCAL}  rows={table.num_rows}  columns={table.num_columns}")
    print("schema:")
    print(table.schema)


def nested_key(out_dir):
    """the fixture for group-key framing tests."""
    ROWS = 60
    OUT = out_dir / "nested_key_shape.parquet"
    tags = [[("k", f"v{i % 3}")] for i in range(ROWS)]
    table = pa.table(
        {
            # The MAP must come first: everything after it is what shifts.
            "tags": pa.array(tags, type=pa.map_(pa.string(), pa.string())),
            "ts": pa.array(
                [1_750_000_000_000_000 + (i % 5) * 1_000_000 for i in range(ROWS)],
                type=pa.timestamp("us"),
            ),
            "flag": pa.array([i % 2 == 0 for i in range(ROWS)], type=pa.bool_()),
            # Independent of `flag`, so a composite (flag, name) key really has 2 x 4 groups; keying both off i % 2
            # would collapse it to 4 and a truncated string half would still look plausible.
            "name": pa.array([f"name{(i // 2) % 4}" for i in range(ROWS)], type=pa.string()),
            "ratio": pa.array([(i % 3) + 0.5 for i in range(ROWS)], type=pa.float64()),
        }
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, OUT, compression="snappy")
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


def null_pages(out_dir):
    """the fixture for the all-null page tests in src/core/scan.zig."""
    ROWS = 2000
    OUT = out_dir / "null_pages.parquet"
    def v_of(i: int) -> int | None:
        return None if 500 <= i < 1000 or i % 10 == 0 else i
    ids = list(range(ROWS))
    v = pa.array([v_of(i) for i in ids], type=pa.int64())
    s_null = pa.array([i % 7 == 0 or 1400 <= i < 1600 for i in ids])
    s = pa.StructArray.from_arrays([pa.array(ids, type=pa.int32()), v], names=["a", "b"], mask=s_null)
    l = pa.array([None if i % 5 == 0 else [i] * (i % 3) for i in ids], type=pa.list_(pa.int64()))
    table = pa.table({"id": pa.array(ids, type=pa.int64()), "v": v, "s": s, "l": l})
    pq.write_table(table, OUT, write_page_index=True, data_page_size=512, write_batch_size=100, use_dictionary=True,
                   row_group_size=1000, compression="snappy")


def overflow_rows(out_dir):
    """the fixture for the 'only a consumed row can overflow' tests in src/engine.zig."""
    ROWS = 1000
    FAULT = 500
    OUT = out_dir / "overflow_rows.parquet"
    vals = [9_000_000_000_000_000_000 if i == FAULT else i % 100 for i in range(ROWS)]
    table = pa.table({
        "id": pa.array(range(ROWS), pa.int64()),
        "p": pa.array(vals, pa.int64()),
        "a": pa.array(vals, pa.int64()),
        "d": pa.array([0 if i == FAULT else i % 7 + 1 for i in range(ROWS)], pa.int64()),
        "n": pa.array([None if i % 10 == 0 else i % 100 for i in range(ROWS)], pa.int64()),
    })
    pq.write_table(table, OUT, row_group_size=ROWS, use_dictionary=["a"], compression="none")


def partagg_budget(out_dir):
    """the plain single-row-group fixture for the shared partition budget regression."""
    path = out_dir / "partagg_budget.parquet"
    pq.write_table(pa.table({"k": list(range(1000)), "v": list(range(1000))}),
                   path, use_dictionary=False, row_group_size=1000)


def repeat_keys(out_dir):
    """Eight groups repeat 8000 keys: every worker holds every key, one table holds each once."""
    N, RGS = 8000, 8
    k = [i % N for i in range(N * RGS)]
    v = [(i * 7) % 1000 for i in range(N * RGS)]
    table = pa.table({"k": pa.array(k, pa.int64()), "v": pa.array(v, pa.int64())})
    pq.write_table(table, out_dir / "repeat_keys.parquet", row_group_size=N, compression="snappy")


def scalar_strings(out_dir):
    """ci/fixtures/parquet/scalar_strings.parquet: four row groups of one 1 MiB string each, the largest first, so every
row group's min(s) is a new owned winner of its own until the merge (the memory ledger's scalar winner charge)."""
    out = out_dir / "scalar_strings.parquet"
    values = [chr(ord("d") - i) * (1 << 20) for i in range(4)]
    pq.write_table(pa.table({"s": pa.array(values, pa.string())}), out, row_group_size=1, use_dictionary=False,
                   compression="snappy")


def skew_filter(out_dir):
    """ci/fixtures/parquet/skew_filter.parquet: 24 row groups of 1,500 rows. `k`: ten heavy keys (a count cut certifies
them) among near-unique ones (a plain grouping holds them all). `s` spans 0..999 in every row group, so its bounds
put `s = 5` at a thousandth of the rows, but nine rows in ten hold 5. `f` is 0 in the first row group only and 1..1000
elsewhere, so the bounds prove `f = 0` passes a 24th of the rows at most."""
    N, RGS = 1500, 24
    rnd = random.Random(11)
    k, s, f = [], [], []
    for i in range(N * RGS):
        k.append(rnd.randrange(10) if rnd.random() < 0.3 else 1000 + rnd.randrange(1 << 40))
        s.append(5 if rnd.random() < 0.9 else rnd.randrange(1000))
        f.append(0 if i < N else 1 + rnd.randrange(1000))
    for rg in range(RGS):
        s[rg * N], s[rg * N + 1] = 0, 999
    table = pa.table({"k": pa.array(k, pa.int64()), "s": pa.array(s, pa.int32()), "f": pa.array(f, pa.int32())})
    pq.write_table(table, out_dir / "skew_filter.parquet", row_group_size=N, compression="snappy")


def spawn_budget(out_dir):
    """the fixture pair for the spawn-failure GROUP BY budget test."""
    ROWS = 50_000
    GROUPS_PER_FILE = 1_000
    PAD_COLUMNS = 11
    OUT_DIR = out_dir
    def build(key_base: int) -> pa.Table:
        # Disjoint key ranges per file: file A owns [0, 1000), file B [1000, 2000).
        keys = [key_base + (i % GROUPS_PER_FILE) for i in range(ROWS)]
        data = {"group_key": pa.array(keys, type=pa.int64())}
        for c in range(PAD_COLUMNS):
            # Constant, so it costs ~nothing on disk while still counting toward the fetched-bytes estimate that drives
            # worker sizing.
            data[f"pad{c}"] = pa.array([1.0] * ROWS, type=pa.float64())
        return pa.table(data)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, key_base in (("spawn_budget_a", 0), ("spawn_budget_b", GROUPS_PER_FILE)):
        path = OUT_DIR / f"{name}.parquet"
        # One row group per file: each file must be exactly one work item, so two files give two items and the scan can
        # pick two workers.
        pq.write_table(build(key_base), path, compression="snappy", row_group_size=ROWS)
        print(f"wrote {path} ({path.stat().st_size} bytes)")


def topk(out_dir):
    """the skewed GROUP BY fixture for the top-k merge tests in src/core/scan.zig."""
    ROWS_PER_GROUP = 1250
    GROUPS = 16
    OUT = out_dir / "topk_skew.parquet"
    keys = []
    for i in range(6):
        keys += [i] * (900 - 120 * i)  # heavy: 900, 780, ..., 300
    for i in range(6, 18):
        keys += [i] * 200  # twelve keys tied at 200: a cut at 10 lands inside them
    n = ROWS_PER_GROUP * GROUPS
    tail = 18
    while len(keys) < n:
        keys += [tail] * (1 + tail % 2)
        tail += 1
    keys = keys[:n]
    random.Random(7).shuffle(keys)
    nulls = [i % 97 == 0 for i in range(n)]
    table = pa.table({
        "k": pa.array([None if z else f"key{x:05d}" for x, z in zip(keys, nulls)], pa.string()),
        "n": pa.array([None if z else x for x, z in zip(keys, nulls)], pa.int64()),
        "v": pa.array([f"v{i:05d}" for i in range(n)], pa.string()),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP)
    print(f"wrote {OUT} ({n} rows, {GROUPS} row groups, {len(set(keys))} keys)")


def topn_budget(out_dir):
    """the fixtures for the parallel plain-row Top-N's memory budget (src/engine.zig), after a review repro."""
    DIR = out_dir
    n = 80
    pq.write_table(pa.table({"id": range(n), "s": ["" if i % 7 == 0 else f"z{i:05d}" for i in range(n)],
                             "payload": [f"row:{i}" for i in range(n)]}),
                   DIR / "topn_budget_80.parquet", use_dictionary=True, row_group_size=n)
    n = 12000
    pq.write_table(pa.table({"id": range(n), "s": ["a" if i % 2 else "z" for i in range(n)],
                             "payload": ["p" for _ in range(n)]}),
                   DIR / "topn_budget_12k.parquet", use_dictionary=True, row_group_size=n)


def topn_descending(out_dir, rows=100_000):
    """ci/fixtures/parquet/topn_descending.parquet: one row group of 100000 required INT32 values, descending, no
statistics, so an ascending Top-N's every row improves on the last and evicts the row before it (the serial Top-N's
kept-key memory under its budget). tools/memory_gate_fixtures.py writes the same shape at 4M rows."""
    out = out_dir / "topn_descending.parquet"
    values = pa.array(range(rows - 1, -1, -1), pa.int32())
    table = pa.Table.from_arrays([values], schema=pa.schema([pa.field("k", pa.int32(), nullable=False)]))
    pq.write_table(table, out, row_group_size=rows, compression="NONE", use_dictionary=False, write_statistics=False)


def topn_gather(out_dir):
    """the fixtures for the parallel plain-row Top-N and its winner gather (src/engine.zig, src/core/consumer.zig)."""
    ROWS = 3000
    DIR = out_dir
    def table(file: int) -> pa.Table:
        ids = range(ROWS)
        return pa.table({
            "id": pa.array([i + 10000 * file for i in ids], type=pa.int64()),
            "k": pa.array([None if i % 11 == 0 else (i // 37) % 50 for i in ids], type=pa.int32()),
            "s": pa.array([None if i % 13 == 0 else f"s{(i * 7919) % 997:04d}-{'x' * (i % 5)}" for i in ids]),
            "u": pa.array([(i * 2654435761) % (1 << 32) if i % 2 else i for i in ids], type=pa.uint32()),
            "f": pa.array([None if i % 19 == 0 else float("nan") if i % 17 == 0 else ((i * 31) % 211) / 7.0
                           for i in ids], type=pa.float64()),
            "dec": pa.array([decimal.Decimal((i * 13) % 900 - (600 if i % 3 == 0 else 0)) / 100 for i in ids],
                            type=pa.decimal128(9, 2)),
            "ts": pa.array([1_600_000_000_000 + ((i * 104729) % 5000) * 1000 for i in ids], type=pa.timestamp("ms")),
            "c": pa.array([None if i % 7 == 0 else "" if i % 5 == 0
                           else f"c{(i // 1000) * 10 + (i * 31) % 10 + 30 * file:02d}" for i in ids]),
        })
    for file, (name, version) in enumerate((("a", "1.0"), ("b", "2.0"))):
        pq.write_table(table(file), DIR / f"topn_gather_{name}.parquet", row_group_size=1000, data_page_size=256,
                       write_batch_size=40, dictionary_pagesize_limit=2048, data_page_version=version,
                       compression="snappy", write_statistics=True)


def typed_root(out_dir):
    """the typed-root fixture for the schema tests in src/core/parquet/{metadata,column}.zig."""
    OUT = out_dir / "typed_root.parquet"
    INT64 = 2
    def type_the_root(meta: list, _body: bytearray) -> None:
        elements = field(meta, 2)[2][1]
        root = elements[0]
        assert field(root, 5) is not None and field(root, 1) is None, "expected a plain group root"
        root[:] = [f for f in root if f[0] != 3]  # no repetition_type on the root, as such writers leave it
        root += [[1, T_I32, INT64], [2, T_I32, 64]]
        for leaf in elements[1:]:
            if field(leaf, 1)[2] == INT64 and field(leaf, 2) is None:
                leaf.append([2, T_I32, 64])
    schema = pa.schema([
        pa.field("mint", pa.int64(), nullable=False),
        pa.field("maxt", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
    ])
    table = pa.table({"mint": [1000, 2000, 3000], "maxt": [1999, 2999, 3999],
                      "name": ["a", "bb", "ccc"]}, schema=schema)
    pq.write_table(table, OUT, compression="snappy", use_dictionary=False)
    rewrite_footer(OUT, type_the_root)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


def filtered_records(out_dir):
    """Indexed dictionary keys with sorted nullable predicates and several pages per row group."""
    import numpy as np
    path = out_dir / 'filtered_records_indexed.parquet'
    rg_rows = 1281
    r = np.arange(16 * rg_rows, dtype=np.int64)
    rg = r // rg_rows
    k = (r + rg * 17) % 257
    k = np.where(k == 0, np.iinfo(np.int64).min,
                 np.where(k == 1, np.iinfo(np.int64).max, k * (1 << 45) - (1 << 54)))
    words = np.array(["", "a\x00b", "last"] + [f"g{i:03d}" for i in range(13)], dtype=object)
    b = words[(r + rg) % len(words)]
    f = (r % rg_rows).astype(np.int32)
    table = pa.table({
        "k": pa.array(k, mask=r % 17 == 0),
        "b": pa.array(b, mask=r % 19 == 0),
        "f": pa.array(f, mask=r % 23 == 0),
    })
    pq.write_table(table, path, row_group_size=rg_rows, compression="NONE", write_page_index=True,
                   data_page_size=128, write_batch_size=32)
    assert all(pq.ParquetFile(path).metadata.row_group(g).column(c).has_offset_index
               for g in range(16) for c in range(3))
    print(path)


def filtered_records_ordered(out_dir):
    """Sorted nullable keys and overlapping row-group seams for filtered COUNT admission."""
    import numpy as np
    rg_rows = 1281
    r = np.arange(16 * rg_rows, dtype=np.int64)
    for seam in (False, True):
        k = (r % rg_rows if seam else r) // 3
        table = pa.table({
            'k': pa.array(k, mask=None if seam else r % rg_rows == 64),
            'f': pa.array((r % 7 != 0).astype(np.int32)),
            'rid': pa.array(r),
        })
        path = out_dir / ('filtered_records_seams.parquet' if seam else 'filtered_records_ordered.parquet')
        pq.write_table(table, path, row_group_size=rg_rows, compression='NONE', use_dictionary=False,
                       write_page_index=True, data_page_size=512, write_batch_size=128)
        assert pq.read_table(path).equals(table)
        print(path)
