"""The bench's cases: per query, each engine's form of it and how zpq-check judges an answer.

A missing dataset fails planning: coverage never shrinks silently. Every ClickBench query is in scope. A case is
{id, suite, dataset, query_id, inputs, output ("rows" | "parquet"), codec, level, zpq (argv after the binary),
duckdb {setup, sql}, polars {form, ...}, truth {setup, sql, mode, ranks}}.
"""

import json
import re
from pathlib import Path

HOME = Path.home()
FAIR = HOME / "bench/zpq-fair"
GENERAL = HOME / "bench/zpq-general"
DATA = HOME / "bench/data"
CLICKBENCH = FAIR / "repos/ClickBench"
SUITES = ("clickbench", "study", "pdsh", "reads-sf1", "writes", "votes-local")
STUDY = ("card10 card100k card10m card1k carduniq clust100k clustzipf comp100k compzipf files50_100k heavy i32_100k "
         "lstr100k lstr1m nulls100k nullszipf plain100k plainsstr100k rg64k_100k rg64k_10m sorted100k sorted10m "
         "sstr100k sstr10m zipf11 zipf15").split()


def quote(text):
    return "'" + str(text).replace("'", "''") + "'"


def split_top(text, sep=","):
    """Split on `sep` outside quotes and parentheses."""
    out, start, depth, q = [], 0, 0, None
    for i, c in enumerate(text):
        if q:
            q = None if c == q else q
        elif c in "'\"":
            q = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == sep and depth == 0:
            out.append(text[start:i].strip())
            start = i + 1
    return out + [text[start:].strip()]


def top_level_select(sql):
    """The SELECT list: up to the first FROM outside parentheses (`extract(minute FROM t)` is inside one)."""
    depth = 0
    for m in re.finditer(r"[()]|\bFROM\b", sql, re.I):
        depth += {"(": 1, ")": -1}.get(m[0], 0)
        if depth == 0 and m[0].upper() == "FROM":
            return sql[len("SELECT "):m.start()].strip()
    raise ValueError("no FROM: " + sql[:60])


def ranks(sql):
    """Positions in the answer of the ORDER BY terms: an output alias, a select expression or an ordinal. A term
    the answer does not carry (ClickBench q24's EventTime) cannot be checked positionally and is left out."""
    m = re.search(r"\sORDER BY\s(.+?)(?:\sLIMIT\s.*)?$", sql, re.I | re.S)
    if not m:
        return []
    select = split_top(top_level_select(sql))
    names = [[x.strip().casefold() for x in re.split(r"\s+AS\s+", e, flags=re.I)] for e in select]
    out = []
    for term in split_top(m[1]):
        term = re.sub(r"\s+(ASC|DESC)(\s+NULLS\s+(FIRST|LAST))?$", "", term, flags=re.I).strip().casefold()
        if term.isdigit() and 0 < int(term) <= len(select):
            out.append(int(term) - 1)
            continue
        out += [i for i, parts in enumerate(names) if term in parts][:1]
    return out


def case(suite, dataset, query_id, inputs, zpq, duck_sql, polars, truth_sql=None, setup="", output="rows",
         codec=None, level=None):
    truth_sql = truth_sql or duck_sql
    mode = "written_sql" if output == "parquet" else "tied_rows"
    return {"id": f"{suite}/{dataset}/{query_id}", "suite": suite, "dataset": dataset, "query_id": str(query_id),
            "inputs": [str(p) for p in inputs], "output": output, "codec": codec, "level": level, "zpq": zpq,
            "duckdb": {"setup": setup, "sql": duck_sql}, "polars": polars,
            "truth": {"setup": setup, "sql": truth_sql, "mode": mode, "ranks": ranks(truth_sql)}}


def clickbench():
    path = FAIR / "data/clickbench/hits.parquet"
    sqls = (CLICKBENCH / "duckdb-parquet/queries.sql").read_text().splitlines()
    exprs = (CLICKBENCH / "polars/queries.sql").read_text().splitlines()
    setup = (CLICKBENCH / "duckdb-parquet/create.sql").read_text().replace("'hits.parquet'", quote(path))
    columns = {28: ["k", "l", "c", "min_referer"], 34: ["1", "URL", "c"],
               35: ["ClientIP", "ClientIP_minus_1", "ClientIP_minus_2", "ClientIP_minus_3", "c"]}
    out = []
    for i, sql in enumerate(s.strip().rstrip(";") for s in sqls):
        # STRLEN counts bytes; the published Polars form counts characters.
        expr = exprs[i].replace(".str.len_chars()", ".str.len_bytes()")
        out.append(case("clickbench", "hits", i, [path], ["query", "--query", sql.replace("FROM hits",
                        f"FROM {quote(path)}")], sql, {"form": "clickbench", "expr": expr, "columns": columns.get(i)},
                        setup=setup))
    return out


def study_queries(meta):
    k, k1, point = meta["K"], meta["kcols"][0], meta["point"]
    top = f"SELECT {k}, count(*) AS c FROM {{T}}"
    agg = "SELECT count(*) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM {T}"
    return [
        ("q01_topc", f"{top} GROUP BY {k} ORDER BY c DESC LIMIT 10"),
        ("q02_topc_w30", f"{top} WHERE f < 300 GROUP BY {k} ORDER BY c DESC LIMIT 10"),
        ("q03_topc_w1", f"{top} WHERE f < 10 GROUP BY {k} ORDER BY c DESC LIMIT 10"),
        ("q04_topc_pay", f"SELECT {k}, count(*) AS c, sum(v) AS s, avg(v) AS av, min(v) AS mn, max(v) AS mx FROM {{T}} "
                         f"GROUP BY {k} ORDER BY c DESC LIMIT 10"),
        ("q05_grp_lim", f"{top} GROUP BY {k} LIMIT 10"),
        ("q06_grp_full", f"SELECT {k}, count(*) AS c, sum(v) AS s FROM {{T}} GROUP BY {k}"),
        ("q07_cd_x", "SELECT count(DISTINCT x) AS d FROM {T}"),
        ("q08_cd_k", f"SELECT count(DISTINCT {k1}) AS d FROM {{T}}"),
        ("q09_grp_cd", f"SELECT {k}, count(DISTINCT x) AS d FROM {{T}} GROUP BY {k} ORDER BY d DESC LIMIT 10"),
        ("q10_agg_w1", f"{agg} WHERE f < 10"),
        ("q11_agg_w30", f"{agg} WHERE f < 300"),
        ("q12_agg_w90", f"{agg} WHERE f < 900"),
        ("q13_point", f"SELECT count(*) AS c, sum(v) AS s FROM {{T}} WHERE {point}"),
        ("q14_topsum", f"SELECT {k}, sum(v) AS s FROM {{T}} GROUP BY {k} ORDER BY s DESC LIMIT 10"),
        ("q15_comp_kf", f"SELECT {k}, f, count(*) AS c FROM {{T}} GROUP BY {k}, f ORDER BY c DESC LIMIT 10"),
    ]


def study():
    out = []
    for ds in STUDY:
        meta = json.loads((GENERAL / "data/meta" / f"{ds}.json").read_text())
        source = meta["path"]
        inputs = sorted(Path(source).parent.glob(Path(source).name)) if "*" in source else [Path(source)]
        for qid, sql in study_queries(meta):
            full = qid == "q06_grp_full"
            zsql = sql.replace("{T}", quote(source))
            out.append(case("study", ds, qid, inputs, ["query", "--query", zsql], zsql,
                            {"form": "sql", "sql": sql.replace("{T}", "t")}, output="parquet" if full else "rows",
                            codec="snappy" if full else None))
    return out


PDSH_Q1 = """SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty,
  avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order
  FROM {T} WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"""
PDSH_Q6 = """SELECT sum(l_extendedprice * l_discount) AS revenue FROM {T}
  WHERE l_shipdate >= timestamp '1994-01-01' AND l_shipdate < timestamp '1994-01-01' + interval '1' year
  AND l_discount BETWEEN .06 - 0.01 AND .06 + 0.01 AND l_quantity < 24"""
# zpq's form of Q6 spells the same bounds as literals.
PDSH_Q6_ZPQ = ("SELECT sum(l_extendedprice * l_discount) AS revenue FROM {T} WHERE l_shipdate >= '1994-01-01' AND "
               "l_shipdate < '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24")


def pdsh():
    sources = [("publisher-sf1", DATA / "pdsh-publisher/source/data/tables/scale-1/lineitem.parquet"),
               ("publisher-sf10", DATA / "pdsh-publisher/source/data/tables/scale-10/lineitem.parquet"),
               ("ours-snappy-sf10", DATA / "pdsh/lineitem_sf10_snappy.parquet"),
               ("ours-snappy-sf30", DATA / "pdsh/lineitem_sf30_snappy.parquet")]
    out = []
    for ds, path in sources:
        for qid, sql, zsql in (("1", PDSH_Q1, PDSH_Q1), ("6", PDSH_Q6, PDSH_Q6_ZPQ)):
            dsql = sql.replace("{T}", f"read_parquet({quote(path)})")
            out.append(case("pdsh", ds, qid, [path], ["query", "--query", zsql.replace("{T}", quote(path))], dsql,
                            {"form": "pdsh", "query_id": qid}))
    return out


READ_FILES = {"bench100": "bench100.parquet", "li_snappy": "lineitem_snappy.parquet",
              "li_zstd": "lineitem_zstd.parquet", "nyc": "nyc.parquet"}
OPS = {"ge": ">=", "gt": ">", "le": "<=", "lt": "<", "eq": "="}
B_PROJ3 = [("count", "*"), ("sum", "int8"), ("sum", "int64_sorted"), ("sum", "float64")]
Q6_FILTER = [("l_shipdate", "ge", "date", "1994-01-01"), ("l_shipdate", "lt", "date", "1995-01-01"),
             ("l_discount", "ge", "dec", "0.05"), ("l_discount", "le", "dec", "0.07"),
             ("l_quantity", "lt", "dec", "24")]
B_SUMS = "bool int8 uint8 int16 int32 int64_sorted int64_random float32 float64 float64_nullable"
B_FULL = ([("count", "*")] + [("sum", c) for c in B_SUMS.split()]
          + [("min", "string_dict_low"), ("max", "string_dict_high"), ("min", "string_random"),
             ("max", "timestamp_sorted")])
L_SUMS = "l_orderkey l_partkey l_suppkey l_linenumber l_quantity l_extendedprice l_discount l_tax"
L_FULL = ([("count", "*")] + [("sum", c) for c in L_SUMS.split()]
          + [("max", "l_returnflag"), ("max", "l_linestatus"), ("min", "l_shipdate"), ("max", "l_commitdate"),
             ("max", "l_receiptdate"), ("max", "l_shipinstruct"), ("max", "l_shipmode"), ("max", "l_comment")])


def shape(id_, file, aggs, filter_=(), group=()):
    return {"id": id_, "file": file, "aggs": aggs, "filter": list(filter_), "group": list(group)}


READS = [
    shape("B1_fullscan", "bench100", B_FULL),
    shape("B2_proj3", "bench100", B_PROJ3),
    shape("B3_filter_broad", "bench100", B_PROJ3, [("int8", "ge", "int", "-100")]),
    shape("B4_filter_selective", "bench100", B_PROJ3, [("int8", "ge", "int", "100")]),
    shape("B5_simple_agg", "bench100",
          [("count", "*"), ("sum", "int64_sorted"), ("min", "int8"), ("max", "int8"), ("avg", "float64")]),
    shape("B6_string_eq", "bench100", [("count", "*"), ("sum", "int64_sorted")],
          [("string_dict_low", "eq", "str", "category_0001")]),
] + [s for p, f in (("LS", "li_snappy"), ("LZ", "li_zstd")) for s in (
    shape(f"{p}1_fullscan", f, L_FULL),
    shape(f"{p}2_q6", f, [("count", "*"), ("sum", "l_extendedprice*l_discount")], Q6_FILTER),
    shape(f"{p}3_q1lite", f, [("count", "*"), ("sum", "l_quantity"), ("sum", "l_extendedprice"),
                              ("sum", "l_discount")], [("l_shipdate", "le", "date", "1998-09-02")],
          ["l_returnflag", "l_linestatus"]),
    shape(f"{p}4_keyrange", f, [("count", "*"), ("sum", "l_extendedprice")], [("l_orderkey", "lt", "long", "600000")]),
)]
W_PROJ3 = ["int8", "int64_sorted", "float64"]
WRITES = [  # (id, file, columns, filter, codec)
    ("W1_rewrite_zstd", "bench100", None, [], "zstd"),
    ("W2_proj3", "bench100", W_PROJ3, [], "snappy"),
    ("W3_filter_proj3", "bench100", W_PROJ3, [("int8", "ge", "int", "-10"), ("int8", "le", "int", "10")], "snappy"),
    ("W4_filter_all_zstd", "bench100", None, [("int8", "ge", "int", "-100")], "zstd"),
    ("WL1_rewrite_zstd", "li_snappy", None, [], "zstd"),
    ("WL2_q6_rewrite", "li_zstd", None, Q6_FILTER, "zstd"),
    ("WL3_proj4", "li_snappy", ["l_orderkey", "l_quantity", "l_extendedprice", "l_shipdate"], [], "snappy"),
    ("NYC_rewrite_zstd", "nyc", None, [], "zstd"),
]


def ident(name):
    return '"' + name.replace('"', '""') + '"'


def predicate(filters):
    terms = [f"{ident(c)} {OPS[op]} {quote(v) if t == 'str' else 'DATE ' + quote(v) if t == 'date' else v}"
             for c, op, t, v in filters]
    return " WHERE " + " AND ".join(terms) if terms else ""


def zpq_filter(filters):
    terms = [f"{c} {OPS[op]} {repr(v) if t in ('date', 'str') else v}" for c, op, t, v in filters]
    return ["--filter", " AND ".join(terms)] if terms else []


def read_select(s, source):
    aggs = []
    for i, (fn, col) in enumerate(s["aggs"]):
        if col == "*":
            expr = "*"
        elif "*" in col:
            expr = " * ".join(f"CAST({ident(c)} AS DOUBLE)" for c in col.split("*"))
        else:
            expr = ident(col)
            if fn == "sum" and col == "bool":
                expr = f"CAST({expr} AS BIGINT)"
            elif fn in ("sum", "avg") and col == "float32":
                expr = f"CAST({expr} AS DOUBLE)"
        aggs.append(f"{fn}({expr}) AS {ident(f'a{i}')}")
    groups = [ident(c) for c in s["group"]]
    group = " GROUP BY " + ", ".join(groups) if groups else ""
    return f"SELECT {', '.join(groups + aggs)} FROM {source}{predicate(s['filter'])}{group}"


def reads():
    out = []
    for s in READS:
        path = DATA / "reads-sf1" / READ_FILES[s["file"]]
        sql = read_select(s, quote(path))
        aggregate = ",".join(f"{fn}({c}) AS a{i}" for i, (fn, c) in enumerate(s["aggs"]))
        zpq = (["query", str(path), "--aggregate", aggregate]
               + zpq_filter(s["filter"]) + (["--group-by", ",".join(s["group"])] if s["group"] else []))
        # Polars sums an Int32 in Int32; the SQL engines widen it.
        psql = read_select(s, "t").replace('sum("int32")', 'sum(CAST("int32" AS BIGINT))')
        out.append(case("reads-sf1", s["file"], s["id"], [path], zpq, sql, {"form": "sql", "sql": psql}))
    return out


def writes():
    out = []
    for id_, file, columns, filters, codec in WRITES:
        path = DATA / "reads-sf1" / READ_FILES[file]
        cols = ",".join(columns) if columns else "*"
        sql = f"SELECT {cols} FROM {{T}}{predicate(filters)}"
        zpq = ["query", str(path)] + (["--columns", cols] if columns else []) + zpq_filter(filters)
        polars = {"form": "sql", "sql": sql.replace("{T}", "t"),
                  "timestamp_units": {"tpep_pickup_datetime": "us", "tpep_dropoff_datetime": "us"} if file == "nyc"
                  else {}}
        out.append(case("writes", file, id_, [path], zpq, sql.replace("{T}", quote(path)), polars, output="parquet",
                        codec=codec, level=3 if codec == "zstd" else None))
    return out


def votes():
    path = FAIR / "data/votes/votes.parquet"
    sql = "SELECT VoteTypeId, count(*) AS n FROM {T} GROUP BY VoteTypeId ORDER BY VoteTypeId"
    zsql = sql.replace("{T}", quote(path))
    return [case("votes-local", "votes", "votes", [path], ["query", "--query", zsql], zsql,
                 {"form": "sql", "sql": sql.replace("{T}", "t")})]


BUILDERS = {"clickbench": clickbench, "study": study, "pdsh": pdsh, "reads-sf1": reads, "writes": writes,
            "votes-local": votes}


def cases(suites=SUITES, only=None):
    if only:
        suites = [s for s in suites if any(o.split("/")[0] == s for o in only)]
    try:
        out = [c for s in suites for c in BUILDERS[s]()]
    except FileNotFoundError as error:
        raise SystemExit(f"bench: missing input {error.filename} (coverage never shrinks silently)") from None
    if only:
        out = [c for c in out if any(c["id"] == o or c["id"].startswith(o.rstrip("/") + "/") for o in only)]
    missing = sorted({p for c in out for p in c["inputs"] if not Path(p).is_file()})
    if missing:
        raise SystemExit("bench: missing inputs (coverage never shrinks silently):\n  " + "\n  ".join(missing))
    return out
