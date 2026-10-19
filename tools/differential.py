#!/usr/bin/env python3
"""Differential test: ZPQ vs DuckDB, ROW-BY-ROW, across a matrix of
filter × projection query shapes.

Adopted from Hardwood's DifferentialReadTest: the bug class this catches is
*composition* defects — a filter or projection that's correct in isolation but
wrong once combined (e.g. filter + column subset + null column). Our existing
duckdb_smoke only compares aggregate scalars; this compares the actual decoded
rows ZPQ writes out against DuckDB's answer for the same query.

For each (filter, projection): ZPQ writes filtered+projected parquet; we read it
back and diff it, order-independent, against DuckDB. DuckDB is the oracle (same
role Hardwood gives it). Values compare under tools/oracle_compare.py: exact for
NULL, integers and DECIMAL (zpq's JSON digits read as Decimal), tolerant for floats.

Usage: .venv/bin/python tools/differential.py
"""
from cli_output import read_one, read_rows, read_summary
import datetime
import duckdb, subprocess, os, sys, tempfile, json
import pyarrow as pa
import pyarrow.parquet as pq  # strict reader: DuckDB is lenient, pyarrow is not
from oracle_compare import DUCKDB_STATS_DIVERGENCES, canon, same, self_check

ZPQ = "zig-out/bin/zpq"
# Physical column order of the fixture (gen_fixture below). `--columns` projects
# a subset in THIS order; `--select` projects in the requested order.
SCHEMA_ORDER = ["id", "amt", "price", "name", "nname", "d", "flag", "ts"]

# (label, zpq_filter | None, duckdb_where | None) — split because date literals
# differ (ZPQ '2020-06-01' vs DuckDB DATE '2020-06-01').
FILTERS = [
    ("none",          None,                     None),
    ("id_gt",         "id > 500",               "id > 500"),
    ("amt_lt",        "amt < 100.5",            "amt < 100.5"),
    ("price_gt",      "price > 100.50",         "price > 100.50"),
    ("name_like",     "name LIKE 'n1%'",        "name LIKE 'n1%'"),
    ("nname_isnull",  "nname IS NULL",          "nname IS NULL"),
    ("nname_notnull", "nname IS NOT NULL",      "nname IS NOT NULL"),
    ("flag_true",     "flag = true",            "flag = true"),
    ("flag_upper",    "flag = TRUE OR flag = False", "flag = TRUE OR flag = False"),
    ("id_in",         "id IN (1, 5, 999)",      "id IN (1, 5, 999)"),
    ("id_between",    "id BETWEEN 100 AND 200", "id BETWEEN 100 AND 200"),
    ("price_between", "price BETWEEN 100.5 AND 200", "price BETWEEN 100.5 AND 200"),
    ("date_between",  "d BETWEEN '2020-06-01' AND '2020-07-01'",
                      "d BETWEEN DATE '2020-06-01' AND DATE '2020-07-01'"),
    ("and",           "id > 500 AND name LIKE 'n9%'", "id > 500 AND name LIKE 'n9%'"),
    ("or",            "id < 100 OR id > 1900",  "id < 100 OR id > 1900"),
    ("date_ge",       "d >= '2020-06-01'",      "d >= DATE '2020-06-01'"),
    ("not_in",        "id NOT IN (1,2,3)",      "id NOT IN (1,2,3)"),
    # Literals finer than the column's millisecond unit must compare exactly, not truncated to the unit.
    ("ts_ge_sub_ms",  "ts >= '2020-01-01 00:00:00.0995'", "ts >= TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_lt_sub_ms",  "ts < '2020-01-01 00:00:00.0995'",  "ts < TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_eq_sub_ms",  "ts = '2020-01-01 00:00:00.0995'",  "ts = TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_ne_sub_ms",  "ts != '2020-01-01 00:00:00.0995'", "ts != TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_between_sub_ms", "ts BETWEEN '2020-01-01 00:00:00.0995' AND '2020-01-01 00:00:00.2005'",
                      "ts BETWEEN TIMESTAMP '2020-01-01 00:00:00.0995' AND TIMESTAMP '2020-01-01 00:00:00.2005'"),
    ("notnull_and_like", "nname IS NOT NULL AND name LIKE 'n5%'", "nname IS NOT NULL AND name LIKE 'n5%'"),
    ("id_neg",        "id < 0",                 "id < 0"),
    ("amt_isnull",    "amt IS NULL",            "amt IS NULL"),
    ("flag_isnull",   "flag IS NULL",           "flag IS NULL"),
    ("empty_name",    "name = ''",              "name = ''"),
    # A NULL member of an IN list matches nothing and makes every non-match UNKNOWN: IN keeps its matches, NOT IN
    # (and NOT over IN) keeps no row. On strings, NULL is the NULL literal, not the text 'NULL'.
    ("in_null_str",   "name IN ('n5', NULL, 'n7')",     "name IN ('n5', NULL, 'n7')"),
    ("not_in_null_str", "name NOT IN ('n5', NULL)",     "name NOT IN ('n5', NULL)"),
    ("in_null_num",   "id IN (1, NULL, 5)",             "id IN (1, NULL, 5)"),
    ("not_in_null_num", "id NOT IN (1, null)",          "id NOT IN (1, null)"),
    ("not_group_in_null", "NOT (id IN (1, NULL)) OR id = 7", "NOT (id IN (1, NULL)) OR id = 7"),
    ("not_not_in_null", "NOT (id NOT IN (1, NULL))",    "NOT (id NOT IN (1, NULL))"),
    # Grouping, NOT over a group, and keywords or parentheses inside quoted literals.
    ("paren_not",     "NOT (id > 500 AND nname IS NULL) AND (amt < 0 OR name LIKE 'n1%')",
                      "NOT (id > 500 AND nname IS NULL) AND (amt < 0 OR name LIKE 'n1%')"),
    ("quoted_kw",     "name = 'n1 OR id > 0' OR (id = 7 AND name != 'x) AND (y')",
                      "name = 'n1 OR id > 0' OR (id = 7 AND name != 'x) AND (y')"),
]

# (label, zpq_select_expr, duckdb_select_expr) — projection of COMPUTED columns,
# aliased to the same name on both sides. Exercises the expression evaluator +
# null propagation, which plain --columns doesn't.
SELECTS = [
    ("id_plus1",  "id + 1 AS x",            "id + 1 AS x"),
    ("concat",    "name || '_z' AS x",      "name || '_z' AS x"),
    ("coalesce",  "coalesce(nname, 'NA') AS x", "coalesce(nname, 'NA') AS x"),
    ("strlen",    "strlen(nname) AS x",     "strlen(nname) AS x"),
    # Searched CASE: the first TRUE WHEN (a NULL condition is not TRUE), ELSE or NULL; a branch only where taken.
    ("case_str",  "CASE WHEN id > 0 AND name LIKE 'n1%' THEN name WHEN nname IS NULL THEN 'null' END AS x",
                  "CASE WHEN id > 0 AND name LIKE 'n1%' THEN name WHEN nname IS NULL THEN 'null' END AS x"),
    ("case_dead_div", "CASE WHEN id = 0 THEN -1 ELSE 100 / id END AS x",
                  "CASE WHEN id = 0 THEN -1 ELSE 100 // id END AS x"),
    ("case_bare_bool", "CASE WHEN flag THEN id WHEN NOT flag THEN -id ELSE 0 END AS x",
                  "CASE WHEN flag THEN id WHEN NOT flag THEN -id ELSE 0 END AS x"),
    ("case_dec",  "CASE WHEN flag = true THEN price * 2 WHEN flag IS NULL THEN 1 ELSE price END AS x",
                  "CASE WHEN flag = true THEN price * 2 WHEN flag IS NULL THEN 1 ELSE price END AS x"),
    # Arithmetic over a NULL is NULL, in every lane: a nullable DOUBLE, an INTEGER and a DECIMAL beside a NULL-able
    # CASE, a string; a NULL row's placeholder raises no division by zero.
    ("amt_x2",    "amt * 2 AS x",           "amt * 2 AS x"),
    ("amt_id",    "amt + id AS x",          "amt + id AS x"),
    ("int_null",  "id + CASE WHEN amt IS NOT NULL THEN 1 END AS x", "id + CASE WHEN amt IS NOT NULL THEN 1 END AS x"),
    ("dec_null",  "price * CASE WHEN amt IS NOT NULL THEN 2 END AS x", "price * CASE WHEN amt IS NOT NULL THEN 2 END AS x"),
    ("str_null",  "nname || '_z' AS x",     "nname || '_z' AS x"),
    ("null_div",  "id / CASE WHEN id = 0 THEN NULL ELSE id END AS x", "id // CASE WHEN id = 0 THEN NULL ELSE id END AS x"),
]

# (label, zpq_agg, duckdb_select) — filter × aggregate, compared as scalars.
AGGS = [
    ("sum_count", "sum(id) AS s, count(id) AS c", "sum(id), count(id)"),
    ("minmax",    "min(amt) AS mn, max(amt) AS mx", "min(amt), max(amt)"),
    ("decimal_sum", "sum(price) AS s", "sum(price)"),
    # count(*) alone needs no column at all in a row group statistics prove fully matching.
    ("count_star", "count(*) AS n, sum(amt) AS s", "count(*), sum(amt)"),
    ("avg",       "avg(amt) AS a, min(name) AS mn", "avg(amt), min(name)"),
    # A computed argument that is NULL where its column is: skipped, as a bare column's NULLs are.
    ("strlen",    "avg(strlen(nname)) AS a, sum(strlen(name)) AS s, count(strlen(nname)) AS c",
                  "avg(strlen(nname)), sum(strlen(name)), count(strlen(nname))"),
    ("case_arg",  "sum(CASE WHEN amt IS NULL THEN 0 ELSE id END) AS s, max(CASE WHEN name LIKE 'n1%' THEN id END) AS m",
                  "sum(CASE WHEN amt IS NULL THEN 0 ELSE id END), max(CASE WHEN name LIKE 'n1%' THEN id END)"),
    # Arithmetic over a NULL is NULL, which every aggregate skips.
    ("null_arith", "sum(amt * 2) AS s, count(amt * 2) AS c, avg(amt + id) AS a, min(amt - 1) AS mn, max(amt * id) AS mx",
                   "sum(amt * 2), count(amt * 2), avg(amt + id), min(amt - 1), max(amt * id)"),
]
# (label, columns_csv | None) — None = all columns (passthrough fast path).
PROJECTIONS = [
    ("all",        None),
    ("id_name",    ["id", "name"]),
    ("name_id",    ["name", "id"]),     # reordered
    ("nname_only", ["nname"]),          # the nullable column alone
    ("price_d",    ["price", "d"]),     # decimal + date
    ("flag_amt",   ["flag", "amt"]),
]


def gen_fixture(con, path, n=4000):
    # Adversarial on purpose: small ROW_GROUP_SIZE → multiple row groups (so the
    # RG-prune + cross-RG-merge paths run, not a single-RG happy path); negative
    # ids; nulls across numeric/bool (not just string); empty strings.
    con.execute(f"""COPY (SELECT
        (i - 2000)::INTEGER AS id,
        (CASE WHEN i % 11 = 0 THEN NULL ELSE (i - 2000) * 1.5 END)::DOUBLE AS amt,
        CAST(i AS DECIMAL(18,2)) AS price,
        (CASE WHEN i % 13 = 0 THEN '' ELSE 'n' || i END) AS name,
        (CASE WHEN i % 7 = 0 THEN NULL ELSE 'v' || i END) AS nname,
        (DATE '2020-01-01' + i::INTEGER) AS d,
        (CASE WHEN i % 5 = 0 THEN NULL ELSE (i % 3 = 0) END) AS flag,
        (CASE WHEN i % 17 = 0 THEN NULL ELSE TIMESTAMP '2020-01-01' + to_milliseconds(i) END)::TIMESTAMP_MS AS ts
      FROM range(1, {n}) t(i)) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 500)""")


from decimal import Decimal


# SQL predicates the front end hands to the filter grammar as text: NOT over groups, literals holding the grammar's
# keywords, NULL operands, values that are no literal. Each answers as DuckDB does (count and sum, in WHERE and in an
# aggregate FILTER, the same at every -j) or is refused with a message and no rows; where DuckDB refuses, zpq must too.
SQL_PRED_DIFF = [
    # NOT over groups
    "NOT (amt > 5 AND id < 3)",
    "NOT (amt > 5 OR id < -1000)",
    "NOT NOT (amt > 5 OR id < -1000)",
    "id > 0 AND NOT (amt > 1000 OR nname LIKE 'v3%')",
    "NOT (amt > 1000 OR nname LIKE 'v3%') AND id > 0",
    "id < -1500 OR NOT (amt > 0 AND nname IS NOT NULL)",
    "NOT ((amt > 0 OR nname LIKE 'v2%') AND (id > 100 OR nname IS NULL))",
    "NOT (id IN (1, 5, 999) AND amt > 0)",
    "NOT (id NOT IN (1, 5, 999) OR nname IS NULL)",
    "NOT (amt NOT BETWEEN -100 AND 100)",
    "amt NOT BETWEEN -100 AND 100 AND id > 0",
    "NOT (amt BETWEEN -100 AND 100 OR id > 1000)",
    "NOT (nname LIKE 'v1%' AND name NOT LIKE '%3')",
    "NOT (nname IS NULL OR amt IS NULL)",
    "NOT (amt <= 10 AND amt >= -10)",
    "amt > 0 OR (id < -1000 OR nname IS NULL)",
    "NOT ((amt > 5 AND id < 3) OR nname LIKE 'v2%')",
    "id > 0 AND (amt > 5 OR nname LIKE 'v2%')",
    "NOT (amt > 5 AND id < 3) AND nname LIKE 'v2%'",
    "nname = 'a OR id > 0'",
    "name LIKE 'n1%' ESCAPE '#'",
    "NOT (id > -5 AND amt < 1.5E+3)",
    "NOT (price > 1e2 OR d >= '2020-06-01') OR ts < '2020-01-01 00:00:00.0995'",
    "nname NOT IN ('v1', NULL)",
    "NOT (nname IN ('v1', NULL))",
    "nname != NULL",
    "name = nname",
    "name = 'n' || '1'",
    # A NULL pattern or bound is UNKNOWN (the grammar once read `LIKE NULL` as the text), and signed or exponent
    # literals on DOUBLE and DECIMAL columns.
    "nname LIKE NULL",
    "nname NOT LIKE NULL OR id = 7",
    "NOT (nname LIKE NULL)",
    "id BETWEEN NULL AND 5",
    "id NOT BETWEEN NULL AND 5",
    "NOT (id BETWEEN -5 AND NULL)",
    "amt < +2.5e3 AND amt > -1.5E2",
    "price BETWEEN -1.5 AND 1e2",
    # A BOOLEAN column alone is `flag = true`: NULL is not true, and NOT NULL is NULL, so neither form keeps it.
    "flag",
    "NOT flag",
    "\"flag\"",
    "flag AND id > 0",
    "NOT flag OR nname IS NULL",
    "NOT (flag AND amt > 0)",
    "flag OR NOT flag",
    "(flag OR id < -1900) AND NOT (nname LIKE 'v1%')",
    # Only a BOOLEAN column is a predicate alone.
    "id",
]

# The same shapes over a fixture whose strings hold the literals a raw-substring split would cut, so a predicate that
# splits them answers differently.
SQL_PRED_UNIT = [
    "NOT (x > 5 AND y < 3)", "NOT (x > 5 OR y < 3)", "NOT NOT (x > 5 OR y < 3)", "NOT NOT NOT x > 5",
    "z = 1 AND NOT (x > 5 OR y < 3)", "NOT (x > 5 OR y < 3) AND z = 1", "NOT (x > 5 AND y < 3) OR z = 1",
    "z = 1 OR NOT (x > 5 AND y < 3)", "NOT ((x > 5 OR y < 3) AND (z = 1 OR x = 2))", "NOT (x IN (1, 2) AND y = 1)",
    "NOT (x NOT IN (1, 2) OR y = 1)", "NOT (x NOT BETWEEN 1 AND 3)", "x NOT BETWEEN 1 AND 3 OR y = 1",
    "NOT (x BETWEEN 1 AND 3 OR y = 1)", "NOT (s LIKE 'a%' AND s NOT LIKE '%b')",
    "NOT (s IS NULL OR s IS NOT NULL)", "x = 1 OR (y = 2 OR z = 3)", "x = 1 AND (y = 2 AND (z = 3 AND x = 4))",
    "(x = 1 AND y = 2) OR z = 3", "x BETWEEN 1 AND 2 OR s = 'a or b'", "x > -5 AND y < +2.5e3 AND z IN (-1, 2)",
    "x BETWEEN -1.5 AND 1e2 OR s = ''", "d >= '2020-06-01' AND s IS NOT NULL",
    # groups that are not an OR of ANDs
    "NOT ((x > 5 AND y < 3) OR z = 1)", "x = 1 AND (y = 2 OR z = 3)", "(x = 1 OR y = 2) AND z = 3",
    "NOT (x = 1 OR y = 2) OR NOT (z = 3 AND x = 4) AND y = 5", "NOT (x = 1 AND y = 2) AND z = 3",
    # literals with keywords, quotes or commas; NULL operands; values that are no literal
    "s = 'a OR b = 1'", "s = 'rock AND roll'", "s = 'x BETWEEN y'", "s = 'O''Brien'", "s IN ('a,b', 'c')",
    "\"a OR b\" = 1", "s LIKE 'a#%' ESCAPE '#'", "s NOT IN ('a', NULL)", "NOT (s IN ('a', NULL))", "s IN (NULL)",
    "s != NULL", "s > NULL", "s LIKE NULL", "x BETWEEN NULL AND 5", "s IS 'a'", "b = TRUE", "s = t", "s = \"t\"",
    "s = 'a' || 'b'", "s = 'a' COLLATE NOCASE", "s = upper('a')", "s = CAST('a' AS TEXT)", "s = x'61'",
    "x > 2 + 3", "x > - -5", "5 < x", "x + 1 > 5", "s GLOB 'a*'",
]


def gen_sql_pred_fixture(con, path):
    strs = ["a", "b", "a OR b = 1", "rock AND roll", "x BETWEEN y", "O'Brien", "a,b", "c", "NULL", "", "a#%", "a%",
            "ab", "A", "abb", "a or b", "t"]
    lit = ", ".join("'" + v.replace("'", "''") + "'" for v in strs)
    con.execute(f"""COPY (SELECT
        (CASE WHEN i % 9 = 0 THEN NULL ELSE (i % 13) - 3 END)::INTEGER AS x,
        (CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 11) - 4 END)::INTEGER AS y,
        (CASE WHEN i % 10 = 0 THEN NULL ELSE (i % 5) - 1 END)::INTEGER AS z,
        (CASE WHEN i % 8 = 0 THEN NULL ELSE ([{lit}])[1 + i % {len(strs)}] END) AS s,
        (CASE WHEN i % 6 = 0 THEN NULL ELSE ([{lit}])[1 + (i * 7) % {len(strs)}] END) AS t,
        (CASE WHEN i % 4 = 0 THEN NULL ELSE i % 3 = 0 END) AS b,
        (DATE '2020-05-01' + (i % 90)::INTEGER) AS d,
        (i % 4)::INTEGER AS "a OR b"
      FROM range(1, 1200) t(i)) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 300)""")


def sql_pred_checks(con, tmp, fixture):
    unit = os.path.join(tmp, "sql_pred.parquet")
    gen_sql_pred_fixture(con, unit)
    total = fails = 0
    answered = refused = 0
    for path, sum_col, cases in ((fixture, "id", SQL_PRED_DIFF), (unit, "x", SQL_PRED_UNIT)):
        for where in cases:
            dsql = f"SELECT count(*) AS c, sum({sum_col}) AS s FROM '{path}' WHERE {where}"
            try:
                want = list(con.execute(dsql).fetchone())
            except Exception as e:
                want = None  # DuckDB refuses the SQL too: zpq must refuse it
                duck_err = str(e).splitlines()[0][:60]
            for form, zsql in (("where", dsql),
                               ("filter", f"SELECT count(*) FILTER (WHERE {where}) AS c, "
                                          f"sum({sum_col}) FILTER (WHERE {where}) AS s FROM '{path}'")):
                outcomes = set()
                for j in ("1", "4", "16"):
                    total += 1
                    r = subprocess.run([ZPQ, "query", "-j", j, "--query", zsql], capture_output=True, text=True)
                    label = f"sql_pred:{form:6} -j{j:2} {where}"
                    if r.returncode != 0:
                        msg = r.stderr.strip().splitlines()[-1] if r.stderr.strip() else ""
                        if r.stdout or not msg:
                            fails += 1
                            print(f"  FAIL  {label}  refused without a message: rc={r.returncode} {r.stdout[:60]!r}")
                        else:
                            outcomes.add(("refused", msg))
                            print(f"  OK    {label}  (refused: {msg[:90]})")
                        continue
                    try:
                        got = list(read_one(r.stdout, parse_float=Decimal).values())
                    except Exception as e:
                        got = f"error: {e}"
                    outcomes.add(("answer", str(got)))
                    if want is None:
                        fails += 1
                        print(f"  FAIL  {label}  zpq answered {got}; DuckDB refuses ({duck_err})")
                    elif got == want:
                        print(f"  OK    {label}  {got}")
                    else:
                        fails += 1
                        print(f"  FAIL  {label}  zpq={got} duck={want}")
                if len(outcomes) > 1:
                    fails += 1
                    print(f"  FAIL  sql_pred:{form} {where}  differs across -j: {sorted(outcomes)}")
                kinds = {o[0] for o in outcomes}
                answered += "answer" in kinds
                refused += "refused" in kinds
    print(f"  sql_pred: {answered} shapes answered, {refused} refused")
    return total, fails


def fold_avg(v):
    """avg arrives as its re-aggregatable {sum, count} pair; no values averaged means NULL."""
    if isinstance(v, dict):
        return v["sum"] / v["count"] if v["count"] else None
    return v


def canon_rows(rows, lanes=None):
    """Rows as sorted tuples of canonical values (oracle_compare.canon), `lanes` giving each column's lane."""
    return sorted((tuple(canon(v, lanes[i] if lanes else None) for i, v in enumerate(row)) for row in rows), key=repr)


def rows_sorted(con, sql, lanes=None):
    return canon_rows(con.execute(sql).fetchall(), lanes)


# A DECIMAL result column: zpq prints it exactly, so its JSON digits are read as Decimal and compared exactly.
EXACT = "exact"


def json_lanes(rel):
    """Per result column of a DuckDB query: EXACT for DECIMAL, else None."""
    return [EXACT if str(d[1]).startswith("DECIMAL") else None for d in rel.description]


def zjson(text):
    """zpq's JSON with every number's digits kept: a DECIMAL answer is exact text."""
    return json.loads(text, parse_float=Decimal)


def from_zpq(vals, lanes):
    """zpq values for comparison: Decimal in an EXACT column, the double it prints anywhere else."""
    return [float(v) if isinstance(v, Decimal) and (lanes is None or lanes[i] != EXACT) else v
            for i, v in enumerate(vals)]


# --- statistics order: columns whose stats order differs from the signed order of their bytes ---
# (label, fixture, zpq_filter | None, oracle). Fixtures come from tools/gen_stats_order_fixtures.py. Each case diffs
# jsonl rows, written parquet, and aggregates. The oracle is a DuckDB WHERE clause, or, where DuckDB itself trusts
# statistics it should not, a Python predicate over every row decoded by pyarrow, which reads no statistics at all.
UNSIGNED_FIXTURE = "ci/fixtures/parquet/unsigned_order.parquet"
DEPRECATED_FIXTURE = "ci/fixtures/parquet/deprecated_stats.parquet"
COLUMN_ORDER_FIXTURE = "ci/fixtures/parquet/column_order.parquet"
NAN_FIXTURE = "ci/fixtures/parquet/nan_stats.parquet"
STATS_ORDER = [
    ("u_all",          UNSIGNED_FIXTURE, None,                          None),
    ("u32_ge_2^31",    UNSIGNED_FIXTURE, "u32 >= 2147483648",           "u32 >= 2147483648"),
    ("u32_eq_2^31",    UNSIGNED_FIXTURE, "u32 = 2147483648",            "u32 = 2147483648"),
    ("u32_in",         UNSIGNED_FIXTURE, "u32 IN (0, 4294967295)",      "u32 IN (0, 4294967295)"),
    ("u32_gt_neg",     UNSIGNED_FIXTURE, "u32 > -1",                    "u32 > -1"),
    ("u64_gt_2^63-1",  UNSIGNED_FIXTURE, "u64 > 9223372036854775807",   "u64 > 9223372036854775807"),
    ("u64_eq_max",     UNSIGNED_FIXTURE, "u64 = 18446744073709551615",  "u64 = 18446744073709551615"),
    ("u8_ge_128",      UNSIGNED_FIXTURE, "u8 >= 128",                   "u8 >= 128"),
    ("u16_between",    UNSIGNED_FIXTURE, "u16 BETWEEN 32768 AND 65535", "u16 BETWEEN 32768 AND 65535"),
    ("n32_gt_2^31-1",  UNSIGNED_FIXTURE, "n32 > 2147483647",            "n32 > 2147483647"),
    # DuckDB prunes these on the signed-ordered deprecated min/max and returns no rows.
    ("depr_s_eq",      DEPRECATED_FIXTURE, "s = 'a'",        lambda r: r["s"] == "a"),
    ("depr_s_lt",      DEPRECATED_FIXTURE, "s < 'b'",        lambda r: r["s"].encode() < b"b"),
    ("depr_dec_eq",    DEPRECATED_FIXTURE, "dec = 1.00",     lambda r: r["dec"] == Decimal("1.00")),
    ("depr_u_eq",      DEPRECATED_FIXTURE, "u = 1",          lambda r: r["u"] == 1),
    # Signed order is these columns' own order; DuckDB is right and the deprecated pair still prunes.
    ("depr_i_gt",      DEPRECATED_FIXTURE, "i > 100",        "i > 100"),
    ("depr_d_lt",      DEPRECATED_FIXTURE, "d < 1",          "d < 1"),
    # `s` declares a column order neither reader implements; DuckDB prunes on its bounds anyway.
    ("order_s_eq",     COLUMN_ORDER_FIXTURE, "s = 'B'",      lambda r: r["s"] == "B"),
    ("order_s_lt",     COLUMN_ORDER_FIXTURE, "s < 'a'",      lambda r: r["s"].encode() < b"a"),
    ("order_i_gt",     COLUMN_ORDER_FIXTURE, "i > 100",      "i > 100"),
    # Bounds leave NaN out of [0, 0]; DuckDB trusts them both ways, dropping NaN rows from `!= 0` and keeping them
    # for `= 0`.
    ("nan_ne",         NAN_FIXTURE,          "d != 0",       lambda r: r["d"] != 0),
    ("nan_eq",         NAN_FIXTURE,          "d = 0",        lambda r: r["d"] == 0),
]
# fixture → (zpq aggregate, DuckDB select, the same aggregate over pyarrow rows)
STATS_ORDER_AGGS = {
    UNSIGNED_FIXTURE: ("count(*) AS n, sum(u32) AS s32, min(u64) AS mn, max(u64) AS mx, sum(u64) AS s64",
                       "count(*), sum(u32), min(u64), max(u64), sum(u64)", None),
    DEPRECATED_FIXTURE: ("count(*) AS n, sum(i) AS si, min(dec) AS lo, max(dec) AS hi, max(u) AS mu",
                         "count(*), sum(i), min(dec), max(dec), max(u)",
                         lambda rows: [len(rows), sum(r["i"] for r in rows) if rows else None,
                                       min((r["dec"] for r in rows), default=None),
                                       max((r["dec"] for r in rows), default=None),
                                       max((r["u"] for r in rows), default=None)]),
    NAN_FIXTURE: ("count(*) AS n, sum(i) AS si", "count(*), sum(i)",
                  lambda rows: [len(rows), sum(r["i"] for r in rows) if rows else None]),
    COLUMN_ORDER_FIXTURE: ("count(*) AS n, sum(i) AS si, min(s) AS lo", "count(*), sum(i), min(s)",
                           lambda rows: [len(rows), sum(r["i"] for r in rows) if rows else None,
                                         next((m.decode() for m in [min((r["s"].encode() for r in rows),
                                                                         default=None)] if m is not None), None)]),
}


def declared_column_orders(path):
    """FileMetaData.column_orders as ColumnOrder union member ids (0 = an empty union), or None when absent."""
    from gen_stats_order_fixtures import Reader, T_STRUCT, field
    data = open(path, "rb").read()
    n = int.from_bytes(data[-8:-4], "little")
    orders = field(Reader(data[-8 - n:-8]).value(T_STRUCT), 7)
    return None if orders is None else [u[0][0] if u else 0 for u in orders[2][1]]


def stats_order_checks(con, tmp):
    """Rows (jsonl), written parquet, and aggregates per case. Returns (total, fails)."""
    total = fails = 0

    def report(ok, label, detail):
        nonlocal fails
        if not ok:
            fails += 1
        print(f"  {'OK  ' if ok else 'FAIL'}  stats-order:{label:28} {detail}")

    for label, fixture, zf, oracle in STATS_ORDER:
        rel = con.execute(f"SELECT * FROM '{fixture}' LIMIT 0")
        cols = [d[0] for d in rel.description]
        row_lanes = json_lanes(rel)
        za, da, py_agg = STATS_ORDER_AGGS[fixture]
        agg_lanes = json_lanes(con.execute(f"SELECT {da} FROM '{fixture}' LIMIT 0"))
        if callable(oracle):
            assert fixture in DUCKDB_STATS_DIVERGENCES, f"{label}: DuckDB is overruled only where it is known to diverge"
            truth = [r for r in pq.read_table(fixture).to_pylist() if oracle(r)]
            want_rows = [[r[c] for c in cols] for r in truth]
            want_agg = py_agg(truth)
            src = "truth"
        else:
            where = f" WHERE {oracle}" if oracle is not None else ""
            want_rows = con.execute(f"SELECT {', '.join(cols)} FROM '{fixture}'{where}").fetchall()
            want_agg = list(con.execute(f"SELECT {da} FROM '{fixture}'{where}").fetchone())
            src = "duck"
        want = canon_rows(want_rows)
        filt = ["--filter", zf] if zf is not None else []

        total += 1
        r = subprocess.run([ZPQ, "query", fixture, "--format", "jsonl"] + filt, capture_output=True, text=True)
        if r.returncode != 0:
            report(False, f"{label} × jsonl", f"ZPQ error: {r.stderr.strip()[:70]}")
        else:
            got = canon_rows([from_zpq([zjson(l)[c] for c in cols], row_lanes)
                              for l in r.stdout.splitlines() if l.strip()], row_lanes)
            want_json = canon_rows(want_rows, row_lanes)
            report(got == want_json, f"{label} × jsonl", f"zpq={len(got)} {src}={len(want_json)} rows")

        total += 1
        out = os.path.join(tmp, "zstats.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", fixture, "-o", out] + filt, capture_output=True, text=True)
        if r.returncode != 0 or not os.path.exists(out):
            report(False, f"{label} × parquet", f"ZPQ error: {r.stderr.strip()[:70]}")
        else:
            tbl = pq.read_table(out)
            got = canon_rows([[row[c] for c in cols] for row in tbl.to_pylist()])
            report(got == want, f"{label} × parquet", f"zpq={len(got)} {src}={len(want)} rows")

        # Unfiltered aggregates may be answered from statistics under --trust-stats; check that path too.
        for flags in ([[]] if zf is not None else [[], ["--trust-stats"]]):
            total += 1
            tag = f"{label} × agg{' trust' if flags else ''}"
            cmd = [ZPQ, "query", fixture, "--aggregate", za, "--summary=json"] + filt + flags
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.returncode != 0:
                report(False, tag, f"ZPQ error: {r.stderr.strip()[:70]}")
                continue
            zvals = from_zpq(list(read_one(r.stdout, parse_float=Decimal).values()), agg_lanes)
            ok = len(zvals) == len(want_agg) and all(same(z, w, lane) for z, w, lane in zip(zvals, want_agg, agg_lanes))
            report(ok, tag, f"zpq={zvals} {src}={want_agg}")

    # Aliased unsigned references keep their values through --select; arithmetic on UINT64 is refused, not wrapped.
    where = "u64 >= 9223372036854775806 AND i < 400"
    want = rows_sorted(con, f"SELECT u64 AS b, u32 AS c, u32 + 1 AS d FROM '{UNSIGNED_FIXTURE}' WHERE {where}")
    sel = ["--select", "u64 AS b, u32 AS c, u32 + 1 AS d", "--filter", where]
    for fmt in ("jsonl", "parquet"):
        total += 1
        out = os.path.join(tmp, "zsel_u.parquet")
        if os.path.exists(out):
            os.remove(out)
        cmd = [ZPQ, "query", UNSIGNED_FIXTURE] + sel + (["--format", "jsonl"] if fmt == "jsonl" else ["-o", out])
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            report(False, f"select_u64_alias × {fmt}", f"ZPQ error: {r.stderr.strip()[:70]}")
            continue
        if fmt == "jsonl":
            rows = [json.loads(l) for l in r.stdout.splitlines() if l.strip()]
        else:
            rows = pq.read_table(out).to_pylist()
        got = sorted([(row["b"], row["c"], row["d"]) for row in rows], key=repr)
        report(got == want, f"select_u64_alias × {fmt}", f"zpq={len(got)} duck={len(want)} rows")
    # zpq's own output declares TYPE_DEFINED_ORDER, so a strict reader trusts its unsigned and string bounds and its
    # pushdown still answers right; a byte copy keeps its source's declared orders.
    for olabel, args in [("reencode", ["--filter", "u32 >= 2147483648"]), ("bytecopy", []),
                         ("project", ["--columns", "u32,u64"])]:
        out = os.path.join(tmp, f"zorder_{olabel}.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", UNSIGNED_FIXTURE, "-o", out] + args, capture_output=True, text=True)
        total += 1
        if r.returncode != 0:
            report(False, f"output_orders × {olabel}", f"ZPQ error: {r.stderr.strip()[:70]}")
            continue
        md = pq.ParquetFile(out).metadata
        usable = all(md.row_group(g).column(c).statistics.has_min_max
                     for g in range(md.num_row_groups) for c in range(md.num_columns)
                     if md.schema.column(c).name in ("u32", "u64"))
        rows = pq.read_table(out).to_pylist()
        pushed = [pq.read_table(out, filters=[("u64", ">=", pa.scalar(2**63, pa.uint64()))]).num_rows,
                  pq.read_table(out, filters=[("u32", "<", 2**31)]).num_rows]
        truth = [sum(r["u64"] >= 2**63 for r in rows), sum(r["u32"] < 2**31 for r in rows)]
        report(usable and pushed == truth, f"output_orders × {olabel}",
               f"has_min_max={usable} pushdown={pushed} truth={truth}")
    total += 1
    out = os.path.join(tmp, "zorder_str.parquet")
    r = subprocess.run([ZPQ, "query", DEPRECATED_FIXTURE, "-o", out, "--filter", "i > 0"], capture_output=True)
    md = pq.ParquetFile(out).metadata
    usable = all(md.row_group(g).column(0).statistics.has_min_max for g in range(md.num_row_groups))
    pushed = pq.read_table(out, filters=[("s", "==", "a")]).num_rows
    report(r.returncode == 0 and usable and pushed == 1, "output_orders × string",
           f"has_min_max={usable} s='a' -> {pushed}")

    # Inputs declaring different orders for one leaf: the merged output must declare one order true of every row
    # group (zpq re-encodes rather than byte-copying), never an empty union, and still prune right.
    normalized = os.path.join(tmp, "zorder_norm.parquet")
    mixed = os.path.join(tmp, "zorder_mixed.parquet")
    for p in (normalized, mixed):
        if os.path.exists(p):
            os.remove(p)
    subprocess.run([ZPQ, "query", COLUMN_ORDER_FIXTURE, "-o", normalized, "--filter", "i > -1"], capture_output=True)
    r = subprocess.run([ZPQ, "query", COLUMN_ORDER_FIXTURE, normalized, "-o", mixed], capture_output=True, text=True)
    total += 1
    if r.returncode != 0:
        report(False, "output_orders × mixed inputs", f"ZPQ error: {r.stderr.strip()[:70]}")
    else:
        orders = declared_column_orders(mixed)
        rows = pq.read_table(mixed).to_pylist()
        pushed = pq.read_table(mixed, filters=[("s", "==", "B")]).num_rows
        truth = sum(r["s"] == "B" for r in rows)
        report(0 not in (orders or []) and pushed == truth == 2, "output_orders × mixed inputs",
               f"column_orders={orders} s='B' -> {pushed} truth={truth}")

    total += 1
    r = subprocess.run([ZPQ, "query", UNSIGNED_FIXTURE, "--select", "u64 + 1 AS x", "--format", "jsonl"],
                       capture_output=True, text=True)
    report(r.returncode != 0 and "UINT64" in r.stderr, "select_u64_arith refused", r.stderr.strip()[:60])
    return total, fails


def writer_wire_checks(con, tmp):
    """Re-encoded FIXED_LEN_BYTE_ARRAY columns, float statistics and row-group sizes, read back by both readers.

    FLBA's PLAIN form carries no lengths (a dictionary page with them reads back as incorrect values in every reader)
    and DELTA_LENGTH_BYTE_ARRAY is BYTE_ARRAY-only (strict readers reject it). Float bounds exclude NaN and write a zero
    min as -0.0 and a zero max as +0.0. total_byte_size is the uncompressed column data. Returns (total, fails)."""
    import math, random, uuid
    total = fails = 0

    def report(ok, label, detail=""):
        nonlocal total, fails
        total += 1
        fails += not ok
        print(f"  {'OK  ' if ok else 'FAIL'}  writer-wire:{label:34} {detail}")

    def norm(v):
        if isinstance(v, uuid.UUID):
            return v.bytes
        if isinstance(v, float):
            return ("nan",) if math.isnan(v) else (v, math.copysign(1, v))
        return v

    rnd = random.Random(11)
    n = 400
    nan = float("nan")
    flba = os.path.join(tmp, "ww_flba.parquet")
    pq.write_table(pa.table({
        "id": pa.array(range(n), pa.int32()),
        "lo": pa.array([bytes([i % 3]) * 4 for i in range(n)], pa.binary(4)),
        "lo_null": pa.array([None if i % 7 == 0 else bytes([i % 5]) * 4 for i in range(n)], pa.binary(4)),
        "rand": pa.array([rnd.randbytes(16) for _ in range(n)], pa.binary(16)),
        "rand_null": pa.array([None if i % 3 == 0 else rnd.randbytes(16) for i in range(n)], pa.binary(16)),
        "pre": pa.array([bytes(12) + i.to_bytes(4, "big") for i in range(n)], pa.binary(16)),
        "uuid": pa.array([rnd.randbytes(16) for _ in range(n)], pa.binary(16)).cast(pa.uuid()),
    }), flba, use_dictionary=False)
    floats = os.path.join(tmp, "ww_floats.parquet")
    pq.write_table(pa.table({
        "id": pa.array(range(n), pa.int32()),
        "nan_first": pa.array([nan if i % 4 == 1 else float(i % 50 - 20) for i in range(n)], pa.float64()),
        "nan_first32": pa.array([nan if i % 4 == 1 else float(i % 50 - 20) for i in range(n)], pa.float32()),
        "all_nan": pa.array([nan] * n, pa.float64()),
        "zero_min": pa.array([0.0 if i % 2 else float(i) for i in range(n)], pa.float64()),
        "zero_max": pa.array([-0.0 if i % 2 else -float(i) for i in range(n)], pa.float64()),
        "dict_nan": pa.array([[nan, 0.0, 2.5][i % 3] for i in range(n)], pa.float64()),
        "nulls_nan": pa.array([None if i % 2 else (nan if i % 3 else 1.5) for i in range(n)], pa.float64()),
    }), floats, use_dictionary=False)

    # (label, fixture, zpq args, rows kept): a filter re-encodes, --codec alone recompresses, --columns byte-copies.
    cases = [
        ("flba re-encode", flba, ["--filter", "id >= 1"], "id >= 1"),
        ("floats re-encode", floats, ["--filter", "id >= 1"], "id >= 1"),
        ("floats recompress", floats, ["--codec", "zstd"], None),
        ("flba copy columns", flba, ["--columns", "id,rand,lo"], None),
    ]
    for label, fixture, args, where in cases:
        out = os.path.join(tmp, "ww_out.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", fixture, "-o", out] + args, capture_output=True, text=True)
        if r.returncode != 0:
            report(False, label, f"ZPQ error: {r.stderr.strip()[:70]}")
            continue
        cols = pq.ParquetFile(out).schema_arrow.names
        sel = ", ".join(cols)
        cond = f" WHERE {where}" if where else ""
        want = sorted([tuple(norm(v) for v in row)
                       for row in con.execute(f"SELECT {sel} FROM '{fixture}'{cond}").fetchall()], key=repr)
        try:
            got_pa = sorted([tuple(norm(r[c]) for c in cols) for r in pq.read_table(out).to_pylist()], key=repr)
            report(got_pa == want, f"{label} × pyarrow values", f"{len(got_pa)} rows")
        except Exception as e:
            report(False, f"{label} × pyarrow values", str(e)[:70])
        try:
            got_dk = sorted([tuple(norm(v) for v in row)
                             for row in con.execute(f"SELECT {sel} FROM '{out}'").fetchall()], key=repr)
            report(got_dk == want, f"{label} × duckdb values", f"{len(got_dk)} rows")
        except Exception as e:
            report(False, f"{label} × duckdb values", str(e)[:70])

        meta = pq.ParquetFile(out).metadata
        sizes_ok = all(meta.row_group(g).total_byte_size ==
                       sum(meta.row_group(g).column(c).total_uncompressed_size for c in range(meta.num_columns))
                       for g in range(meta.num_row_groups))
        report(sizes_ok, f"{label} × total_byte_size")
        encs_ok = all("DELTA_LENGTH_BYTE_ARRAY" not in meta.row_group(g).column(c).encodings
                      for g in range(meta.num_row_groups) for c in range(meta.num_columns)
                      if meta.row_group(g).column(c).physical_type == "FIXED_LEN_BYTE_ARRAY")
        report(encs_ok, f"{label} × FLBA encodings")

        try:
            tbl = pq.read_table(out)
        except Exception:
            continue
        bad = []
        for c in range(meta.num_columns):
            cc = meta.row_group(0).column(c)
            if cc.physical_type not in ("FLOAT", "DOUBLE"):
                continue
            vals = [v for v in tbl.column(cc.path_in_schema).to_pylist() if v is not None and not math.isnan(v)]
            st = cc.statistics
            if not vals:
                if st.has_min_max:
                    bad.append(f"{cc.path_in_schema}: bounds on an all-NaN chunk")
                continue
            lo, hi = min(vals), max(vals)
            lo = -0.0 if lo == 0 else lo
            hi = 0.0 if hi == 0 else hi
            if norm(st.min) != norm(lo) or norm(st.max) != norm(hi):
                bad.append(f"{cc.path_in_schema}: [{st.min}, {st.max}] want [{lo}, {hi}]")
        if "floats" in label and "recompress" not in label:
            report(not bad, f"{label} × float bounds", "; ".join(bad)[:70])
    return total, fails


def dense_domain_checks(con, tmp):
    """A lone integer key folded by its retained dictionary domain (default) answers byte for byte as grouping by code
    does (ZPQ_DENSE=0), at every -j, and as DuckDB does. Two files share one domain under row-group dictionaries in
    different orders; the key has NULLs, one row group is near-unique (its dictionary past rows/8), and filters,
    FILTER, cuts, HAVING and OFFSET all run. Tight --max-memory refuses the lanes: the answer (or the error) stays."""
    import numpy as np
    total = fails = 0
    rng = np.random.default_rng(20261015)
    paths = []
    for fi in range(2):
        n = 24576
        k = rng.integers(0, 3000, n)
        k[8192:10240] = rng.permutation(np.arange(100000, 102048))[:2048]  # one row group of distinct keys
        kmask = rng.random(n) < 0.03
        v = rng.integers(-1000, 1000, n)
        vmask = rng.random(n) < 0.05
        path = os.path.join(tmp, f"dense_{fi}.parquet")
        pq.write_table(pa.table({
            "k": pa.array(k, pa.int64(), mask=kmask),
            "k32": pa.array((k % 2500).astype(np.int32), pa.int32()),
            "v": pa.array(v, pa.int64(), mask=vmask),
            "f": pa.array(rng.integers(0, 100, n).astype(np.int32), pa.int32()),
        }), path, row_group_size=2048)
        paths.append(path)
    both = os.path.join(tmp, "dense_*.parquet")
    queries = []
    for src in (paths[0], both):
        for key in ("k", "k32"):
            queries += [
                f"SELECT {key}, count(*) AS c FROM '{src}' GROUP BY {key} ORDER BY c DESC LIMIT 7",
                f"SELECT {key}, count(*) AS c FROM '{src}' WHERE f < 30 GROUP BY {key} ORDER BY c DESC LIMIT 7",
                f"SELECT {key}, count(*) AS c, sum(v) AS s, min(v) AS lo, max(v) AS hi, count(v) AS cv "
                f"FROM '{src}' GROUP BY {key}",
                f"SELECT {key}, sum(v) AS s FROM '{src}' GROUP BY {key} ORDER BY s DESC LIMIT 9 OFFSET 2",
                f"SELECT {key}, avg(v) AS a, count(*) FILTER (WHERE f > 50) AS c FROM '{src}' WHERE v IS NOT NULL "
                f"GROUP BY {key} HAVING count(*) > 3 ORDER BY a DESC LIMIT 5",
                f"SELECT {key}, min(v) AS lo FROM '{src}' WHERE f = 7 GROUP BY {key}",
            ]
    def nulls_first(row):
        return tuple((x is not None, x if x is not None else 0) for x in row)

    for qi, sql in enumerate(queries):
        duck = sorted((tuple(r) for r in con.execute(sql).fetchall()), key=nulls_first)
        base = None
        for dense in ("0", "1"):
            for j in ("1", "3", "16"):
                total += 1
                label = f"dense:{qi} dense={dense} -j {j}"
                r = subprocess.run([ZPQ, "query", "-j", j, "--query", sql], capture_output=True, text=True,
                                   env={**os.environ, "ZPQ_DENSE": dense})
                problem = None
                if r.returncode != 0:
                    problem = (r.stderr or r.stdout).strip()[:90]
                elif base is None:
                    base = r.stdout
                    rows = sorted((tuple(row.values()) for row in read_rows(r.stdout)), key=nulls_first)
                    if "avg" not in sql and "LIMIT" not in sql and rows != duck:
                        problem = f"duckdb differs: zpq={rows[:2]} duck={duck[:2]}"
                elif r.stdout != base:
                    problem = "answer differs from ZPQ_DENSE=0 -j 1"
                if problem:
                    fails += 1
                    print(f"  FAIL  {label}  {problem}")
                else:
                    print(f"  OK    {label}")
    # Many calls over a small domain: lanes, domain and final table together outgrow a budget the established path
    # fits. The dense fold must decline or run that path again, never fail the query.
    many = os.path.join(tmp, "dense_many.parquet")
    rows = np.arange(196608)
    pq.write_table(pa.table({"k": pa.array(rows % 1000, pa.int64()), "v": pa.array(rows % 31 - 15, pa.int64())}),
                   many, row_group_size=8192)
    # (48 calls at 4M failed before the final table was reserved with the lanes and a refusal ran the scan again.)
    for n_calls, mem in ((24, "2M"), (48, "4M"), (48, "3500K")):
        sums = ", ".join(f"sum(v) AS s{i}" for i in range(n_calls))
        for j in ("1", "3", "16"):
            outs = [subprocess.run([ZPQ, "query", many, "-j", j, "--group-by", "k", "--aggregate", sums,
                                    "--max-memory", mem], capture_output=True, text=True,
                                   env={**os.environ, "ZPQ_DENSE": d}) for d in ("0", "1")]
            total += 1
            same_out = outs[0].returncode == 0 and outs[1].returncode == 0 and outs[0].stdout == outs[1].stdout
            print(f"  {'OK  ' if same_out else 'FAIL'}  dense:many-calls {n_calls} {mem} -j {j} rc={outs[1].returncode}")
            fails += 0 if same_out else 1
    # Budgets around the lanes' charge: dense or not, the same answer or the same refusal.
    sql = queries[2].replace(paths[0], both)
    for mem in ("2M", "6M", "16M", "64M"):
        outs = [subprocess.run([ZPQ, "query", "-j", "16", "--max-memory", mem, "--query", sql], capture_output=True,
                               text=True, env={**os.environ, "ZPQ_DENSE": d}) for d in ("0", "1")]
        total += 1
        same_out = outs[0].returncode == outs[1].returncode and outs[0].stdout == outs[1].stdout
        print(f"  {'OK  ' if same_out else 'FAIL'}  dense:budget {mem} rc={outs[1].returncode}")
        fails += 0 if same_out else 1
    return total, fails


def main():
    self_check()
    con = duckdb.connect()
    tmp = tempfile.mkdtemp()
    fixture = os.path.join(tmp, "diff_fixture.parquet")
    gen_fixture(con, fixture)

    fails, total = 0, 0
    if sys.argv[1:] == ["--only", "sql_pred"]:
        total, fails = sql_pred_checks(con, tmp, fixture)
        print(f"\ndifferential --only sql_pred: {total - fails}/{total} checks match")
        sys.exit(1 if fails else 0)
    for flabel, zf, dw in FILTERS:
        for plabel, cols in PROJECTIONS:
            total += 1
            out = os.path.join(tmp, "zout.parquet")
            if os.path.exists(out):
                os.remove(out)
            cmd = [ZPQ, "query", fixture, "-o", out]
            if zf is not None:
                cmd += ["--filter", zf]
            if cols is not None:
                cmd += ["--columns", ",".join(cols)]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"{flabel:18} × {plabel:10}"
            if r.returncode != 0 or not os.path.exists(out):
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue
            # STRICT reader gate: pyarrow must accept ZPQ's output. DuckDB is
            # lenient (it tolerated the bit-width-0 corruption pyarrow rejects),
            # so a pure DuckDB diff gives false confidence. Also pins projection
            # COLUMN ORDER (read_table preserves physical schema order).
            try:
                tbl = pq.read_table(out)
            except Exception as e:
                print(f"  FAIL  {label}  pyarrow rejected ZPQ output: {str(e)[:55]}")
                fails += 1
                continue
            # `--columns` is a SUBSET selector: it preserves SCHEMA order by
            # design (projectSubset by index), NOT the requested order. Explicit
            # reordering is `--select`'s job (checked separately below). So the
            # expectation is the requested subset in schema order.
            if cols is not None:
                want_order = [c for c in SCHEMA_ORDER if c in cols]
                if list(tbl.column_names) != want_order:
                    print(f"  FAIL  {label}  column order: got {list(tbl.column_names)} want {want_order}")
                    fails += 1
                    continue
            # Value diff vs the DuckDB oracle. Order-independent (DuckDB doesn't
            # guarantee row order); row-order determinism is checked separately.
            sel_cols = cols if cols is not None else \
                ["amt", "d", "flag", "id", "name", "nname", "price", "ts"]
            sel = ", ".join(sel_cols)
            where = f" WHERE {dw}" if dw is not None else ""
            try:
                zrows = rows_sorted(con, f"SELECT {sel} FROM '{out}'")
                drows = rows_sorted(con, f"SELECT {sel} FROM '{fixture}'{where}")
            except Exception as e:
                print(f"  FAIL  {label}  compare error: {e}")
                fails += 1
                continue
            if zrows != drows:
                fails += 1
                # find first divergence to report
                diff = "row-count" if len(zrows) != len(drows) else "values"
                detail = f"zpq={len(zrows)} duck={len(drows)}"
                if diff == "values":
                    for i, (a, b) in enumerate(zip(zrows, drows)):
                        if a != b:
                            detail = f"row[{i}] zpq={a} duck={b}"
                            break
                print(f"  FAIL  {label}  [{diff}] {detail}")
            else:
                print(f"  OK    {label}  ({len(zrows)} rows)")
    # --- --select expressions × a couple filters ---
    for slabel, zs, ds in SELECTS:
        for flabel, zf, dw in [("none", None, None), ("id_gt", "id > 500", "id > 500")]:
            total += 1
            out = os.path.join(tmp, "zout.parquet")
            if os.path.exists(out):
                os.remove(out)
            cmd = [ZPQ, "query", fixture, "-o", out, "--select", zs]
            if zf is not None:
                cmd += ["--filter", zf]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"select:{slabel:10} × {flabel:6}"
            if r.returncode != 0 or not os.path.exists(out):
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue
            where = f" WHERE {dw}" if dw is not None else ""
            zrows = rows_sorted(con, f"SELECT x FROM '{out}'")
            drows = rows_sorted(con, f"SELECT {ds} FROM '{fixture}'{where}")
            if zrows != drows:
                fails += 1
                detail = f"zpq={len(zrows)} duck={len(drows)}" if len(zrows) != len(drows) else \
                    next(f"row zpq={a} duck={b}" for a, b in zip(zrows, drows) if a != b)
                print(f"  FAIL  {label}  {detail}")
            else:
                print(f"  OK    {label}  ({len(zrows)} rows)")

    # --- filter × aggregate (scalars, tolerant) ---
    for alabel, za, ds in AGGS:
        for flabel, zf, dw in [("none", None, None), ("id_gt", "id > 500", "id > 500"),
                               ("notnull", "nname IS NOT NULL", "nname IS NOT NULL"),
                               ("date_ge", "d >= '2020-06-01'", "d >= DATE '2020-06-01'"),
                               ("ts_ne_sub_ms", "ts != '2020-01-01 00:00:00.0995'",
                                "ts != TIMESTAMP '2020-01-01 00:00:00.0995'"),
                               # Over no rows / only nulls: sum, avg, min, max are NULL, count is 0.
                               ("no_rows", "id < -100000", "id < -100000"),
                               ("amt_isnull", "amt IS NULL", "amt IS NULL")]:
            total += 1
            cmd = [ZPQ, "query", fixture, "--aggregate", za, "--summary=json"]
            if zf is not None:
                cmd += ["--filter", zf]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"agg:{alabel:11} × {flabel:8}"
            if r.returncode != 0:
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue

            try:
                zout = read_one(r.stdout, parse_float=Decimal)
                zvals = [fold_avg(v) for v in zout.values()]
                proven = read_summary(r.stderr).get("row_groups_full_match", 0)
            except Exception as e:
                print(f"  FAIL  {label}  bad json: {e}")
                fails += 1
                continue
            where = f" WHERE {dw}" if dw is not None else ""
            duck = con.execute(f"SELECT {ds} FROM '{fixture}'{where}")
            lanes = json_lanes(duck)
            zvals = from_zpq(zvals, lanes)
            dvals = list(duck.fetchone())
            ok = len(zvals) == len(dvals) and all(same(a, b, lane) for a, b, lane in zip(zvals, dvals, lanes))
            if ok:
                print(f"  OK    {label}  {zvals}  (full-match row groups: {proven})")
            else:
                fails += 1
                print(f"  FAIL  {label}  zpq={zvals} duck={dvals}")

    # --- GROUP BY × aggregate matrix ---
    GROUP_BYS = [
        ("gb_simple", "flag", "count(id) AS c", None, 
         "SELECT flag, count(id) AS c FROM '{fixture}' GROUP BY flag"),
        ("gb_arith", "id + 1 AS id_plus1", "sum(price) AS s", None, 
         "SELECT (id + 1) AS id_plus1, sum(price) AS s FROM '{fixture}' GROUP BY id_plus1"),
        ("gb_concat", "name || '_z' AS concat", "max(price) AS mx, min(price) AS mn", None, 
         "SELECT (name || '_z') AS concat, max(price) AS mx, min(price) AS mn FROM '{fixture}' GROUP BY concat"),
        ("gb_coalesce", "coalesce(nname, 'NA') AS coal", "count(id) AS c", "c,coal",
         "SELECT count(id) AS c, coalesce(nname, 'NA') AS coal FROM '{fixture}' GROUP BY coal"),
        # Keys mixing integer and float operands compute in DOUBLE; reading the integer side's bits as a double
        # used to turn 2 into 1e-323 and collapse distinct keys.
        ("gb_int_plus_float", "id + 0.5 AS h", "count(*) AS n", None,
         "SELECT (id + 0.5) AS h, count(*) AS n FROM '{fixture}' GROUP BY h"),
        ("gb_float_times_int", "amt * 2 AS a2", "count(*) AS n", None,
         "SELECT (amt * 2) AS a2, count(*) AS n FROM '{fixture}' GROUP BY a2"),
        ("gb_int_times_float", "id * amt AS p", "count(*) AS n", None,
         "SELECT (id * amt) AS p, count(*) AS n FROM '{fixture}' GROUP BY p"),
        ("gb_decimal_plus_int", "price + id AS q", "count(*) AS n", None,
         "SELECT (price + id) AS q, count(*) AS n FROM '{fixture}' GROUP BY q"),
        ("gb_null_input", "flag", "sum(amt) AS s, min(amt) AS mn, count(amt) AS c, avg(amt) AS a, max(name) AS mx",
         None, "SELECT flag, sum(amt) AS s, min(amt) AS mn, count(amt) AS c, avg(amt) AS a, max(name) AS mx "
         "FROM '{fixture}' GROUP BY flag"),
    ]
    for glabel, gby, agg, order, d_sql in GROUP_BYS:
        for flabel, zf, dw in [("none", None, None), ("id_gt", "id > 500", "id > 500"),
                               ("amt_isnull", "amt IS NULL", "amt IS NULL"),
                               ("no_rows", "id < -100000", "id < -100000")]:
            total += 1
            cmd = [ZPQ, "query", fixture, "--group-by", gby]
            if agg:
                cmd += ["--aggregate", agg]
            if order:
                cmd += ["--column-order", order]
            if zf is not None:
                cmd += ["--filter", zf]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"groupby:{glabel:15} × {flabel:6}"
            if r.returncode != 0:
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue

            try:
                zrows_raw = read_rows(r.stdout, parse_float=Decimal)
            except Exception as e:
                print(f"  FAIL  {label}  bad json: {e}")
                fails += 1
                continue
            
            where = f" WHERE {dw}" if dw is not None else ""
            duck_query = d_sql.format(fixture=fixture)
            if where:
                parts = duck_query.split(" GROUP BY ")
                duck_query = f"{parts[0]}{where} GROUP BY {parts[1]}"
            
            try:
                duck_rel = con.execute(duck_query)
                col_names = [desc[0] for desc in duck_rel.description]
                lanes = json_lanes(duck_rel)
                drows = canon_rows(duck_rel.fetchall(), lanes)
                zrows = canon_rows([from_zpq([fold_avg(row_dict.get(c)) for c in col_names], lanes)
                                    for row_dict in zrows_raw], lanes)
            except Exception as e:
                print(f"  FAIL  {label}  compare setup error: {e}")
                fails += 1
                continue
                
            if zrows != drows:
                fails += 1
                detail = f"zpq={len(zrows)} duck={len(drows)}" if len(zrows) != len(drows) else \
                    next(f"row zpq={a} duck={b}" for a, b in zip(zrows, drows) if a != b)
                print(f"  FAIL  {label}  {detail}")
            else:
                print(f"  OK    {label}  ({len(zrows)} rows)")

            # `-o` writes the same groups as parquet: same column order, same values and row order as the JSON, avg
            # split into <name>__sum / <name>__count. pyarrow is the strict reader.
            total += 1
            out = os.path.join(tmp, "zgroup.parquet")
            if os.path.exists(out):
                os.remove(out)
            r = subprocess.run(cmd + ["-o", out], capture_output=True, text=True)
            assert not r.stdout, r.stdout
            label = f"groupby-o:{glabel:13} × {flabel:6}"
            want_cols, want_rows = [], []
            for c, v in (zrows_raw[0].items() if zrows_raw else []):
                want_cols += [f"{c}__sum", f"{c}__count"] if isinstance(v, dict) else [c]
            json_rows = []
            for row_dict in zrows_raw:
                vals = []
                for v in row_dict.values():
                    vals += [v["sum"], v["count"]] if isinstance(v, dict) else [v]
                json_rows.append(vals)
            try:
                if r.returncode != 0:
                    raise RuntimeError((r.stderr or r.stdout).strip()[:70])
                tbl = pq.read_table(out)
                # A DECIMAL column's JSON digits are its exact value; any other number is the double it prints.
                exact = [str(f.type).startswith("decimal") for f in tbl.schema]
                want_rows = [tuple(canon(float(v) if isinstance(v, Decimal) and not (i < len(exact) and exact[i])
                                         else v) for i, v in enumerate(vals)) for vals in json_rows]
                got_rows = [tuple(canon(v.decode() if isinstance(v, bytes) else v) for v in row.values())
                            for row in tbl.to_pylist()]
                problem = None
                if zrows_raw and list(tbl.column_names) != want_cols:
                    problem = f"columns {tbl.column_names} want {want_cols}"
                elif got_rows != want_rows:
                    problem = (f"rows zpq-parquet={got_rows[:2]}.. ({len(got_rows)}) "
                               f"json={want_rows[:2]}.. ({len(want_rows)})")
            except Exception as e:
                problem = f"error: {e}"
            if problem:
                fails += 1
                print(f"  FAIL  {label}  {problem}")
            else:
                print(f"  OK    {label}  ({len(got_rows)} rows match the JSON)")
    # ORDER BY / LIMIT / OFFSET over GROUP BY rows. Each ORDER BY here breaks every tie, so the answer is one sequence
    # and compares in order; NULLs sort last in both directions, as in DuckDB.
    ORDERED = [
        ("ord_key_desc_cut", "SELECT name, count(*) AS c FROM '{fixture}' GROUP BY name ORDER BY name DESC LIMIT 5 OFFSET 2"),
        ("ord_position", "SELECT flag, sum(amt) AS s, avg(amt) AS a FROM '{fixture}' GROUP BY flag ORDER BY 2 DESC, 1"),
        ("ord_null_key_desc", "SELECT nname, count(*) AS n FROM '{fixture}' GROUP BY nname ORDER BY nname DESC"),
        ("ord_float_null", "SELECT flag, min(amt) AS mn FROM '{fixture}' WHERE amt IS NULL OR id > 0 GROUP BY flag "
         "ORDER BY mn DESC, flag"),
        ("ord_decimal", "SELECT flag, max(price) AS mx FROM '{fixture}' WHERE id < 1000 GROUP BY flag ORDER BY mx, flag"),
        ("ord_expr_text", "SELECT flag, count(*) FROM '{fixture}' GROUP BY flag ORDER BY count(*), flag LIMIT 2"),
        ("ord_offset_past_end", "SELECT flag, count(*) AS c FROM '{fixture}' GROUP BY flag ORDER BY c LIMIT 3 OFFSET 9"),
        ("ord_no_rows", "SELECT flag, count(*) AS c FROM '{fixture}' WHERE id < -100000 GROUP BY flag ORDER BY c LIMIT 1"),
        # ORDER BY terms the SELECT list lacks rank the rows and are not shown.
        ("ord_hidden_agg", "SELECT flag FROM '{fixture}' GROUP BY flag ORDER BY count(*) DESC, flag"),
        ("ord_hidden_key", "SELECT count(*) AS c FROM '{fixture}' GROUP BY flag ORDER BY flag DESC"),
        ("ord_hidden_cut", "SELECT name FROM '{fixture}' WHERE id > 1500 GROUP BY name ORDER BY sum(amt) DESC, name "
         "LIMIT 4 OFFSET 1"),
        # GROUP BY keys bound as expressions, by alias and by position.
        ("grp_expr_key", "SELECT id - 1, count(*) AS c FROM '{fixture}' WHERE id BETWEEN 0 AND 20 GROUP BY id - 1 "
         "ORDER BY 1"),
        ("grp_alias", "SELECT name AS k, count(*) AS c FROM '{fixture}' WHERE id < -1990 GROUP BY k ORDER BY k"),
        ("grp_position", "SELECT flag, count(*) AS c FROM '{fixture}' GROUP BY 1 ORDER BY 1"),
        # SQL binds `GROUP BY name` to the column and `AS name` names the aggregate: no clash, as in DuckDB.
        ("grp_key_vs_alias", "SELECT name AS k, sum(amt) AS name FROM '{fixture}' WHERE id < -1990 GROUP BY name "
         "ORDER BY k"),
        # CASE as a GROUP BY key, by its alias (ClickBench Q39's shape); a NULL flag is not TRUE, so it takes ELSE.
        ("grp_case", "SELECT flag, CASE WHEN (flag = true AND id > 0) THEN name ELSE '' END AS Src, count(*) AS c "
         "FROM '{fixture}' WHERE id BETWEEN -10 AND 30 GROUP BY flag, Src ORDER BY Src, flag"),
        ("grp_case_null", "SELECT CASE WHEN id < 0 AND nname IS NOT NULL THEN nname END AS k, count(*) AS c "
         "FROM '{fixture}' WHERE id BETWEEN -50 AND 50 GROUP BY k ORDER BY k"),
        ("grp_strlen", "SELECT strlen(nname) AS k, count(*) AS c FROM '{fixture}' GROUP BY k ORDER BY k"),
        ("grp_constant", "SELECT 1, flag, count(*) AS c FROM '{fixture}' GROUP BY 1, flag ORDER BY flag"),
        # Grouped WHERE under SQL, whose NOT the front end pushes to the leaves.
        ("where_paren_not", "SELECT flag, count(*) AS c FROM '{fixture}' WHERE NOT (id > 500 AND nname IS NULL) "
         "AND (amt < 0 OR name LIKE 'n1%') GROUP BY flag ORDER BY flag"),
        # A page proving `c IS NOT NULL` must still decode c for the OR beside it (min-fill counterexample).
        ("min_fill_or", "SELECT b, count(*) AS n, sum(x) AS s FROM 'ci/fixtures/parquet/min_fill_or.parquet' "
         "WHERE c IS NOT NULL AND (c = 2 OR b = 1) GROUP BY b ORDER BY b"),
    ]
    for olabel, o_sql in ORDERED:
        total += 1
        sql = o_sql.format(fixture=fixture)
        label = f"ordered:{olabel}"
        r = subprocess.run([ZPQ, "query", "--query", sql], capture_output=True, text=True)
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:70])
            duck_rel = con.execute(sql)
            lanes = json_lanes(duck_rel)
            want = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in duck_rel.fetchall()]
            got = [tuple(canon(v, lanes[i]) for i, v in enumerate(from_zpq([fold_avg(v) for v in row.values()], lanes)))
                   for row in read_rows(r.stdout, parse_float=Decimal)]
            problem = None if got == want else f"zpq={got[:3]}.. ({len(got)}) duck={want[:3]}.. ({len(want)})"
        except Exception as e:
            problem = f"error: {e}"
        if problem:
            fails += 1
            print(f"  FAIL  {label}  {problem}")
        else:
            print(f"  OK    {label}  ({len(got)} rows, in order)")
    # String-keyed GROUP BY whose workers move to partition runs mid-scan (a tiny ZPQ_SPILL_GROUPS) at several -j,
    # against the same queries on the plain fold.
    SPILL = [
        ("spill_full", "SELECT name, count(*) AS c, sum(id) AS s, min(nname) AS mn FROM '{fixture}' GROUP BY name "
         "ORDER BY name"),
        ("spill_cut", "SELECT nname, sum(id) AS s, max(id) AS mx FROM '{fixture}' GROUP BY nname ORDER BY s DESC, nname "
         "LIMIT 7"),
        ("spill_composite", "SELECT name, flag, count(*) AS c FROM '{fixture}' WHERE id > -1000 GROUP BY name, flag "
         "ORDER BY name, flag"),
    ]
    for slabel, s_sql in SPILL:
        sql = s_sql.format(fixture=fixture)
        duck_rel = con.execute(sql)
        lanes = json_lanes(duck_rel)
        want = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in duck_rel.fetchall()]
        for groups in ("8", "262144"):
            for j in ("1", "3", "16"):
                total += 1
                label = f"spill:{slabel} groups={groups} -j {j}"
                r = subprocess.run([ZPQ, "query", "-j", j, "--query", sql], capture_output=True, text=True,
                                   env={**os.environ, "ZPQ_SPILL_GROUPS": groups})
                try:
                    if r.returncode != 0:
                        raise RuntimeError((r.stderr or r.stdout).strip()[:70])
                    got = [tuple(canon(v, lanes[i]) for i, v in enumerate(from_zpq(list(row.values()), lanes)))
                           for row in read_rows(r.stdout, parse_float=Decimal)]
                    problem = None if got == want else f"zpq={got[:3]}.. ({len(got)}) duck={want[:3]}.. ({len(want)})"
                except Exception as e:
                    problem = f"error: {e}"
                if problem:
                    fails += 1
                    print(f"  FAIL  {label}  {problem}")
                else:
                    print(f"  OK    {label}  ({len(got)} rows, in order)")
    # Grouped count(DISTINCT), through the pairs path (forced: ZPQ_DISTINCT_PAIRS=1) and through per-group sets (=0),
    # at several -j: NULL values, NULL keys, an expression key, FILTER, a cut on the distinct count.
    DISTINCT = [
        ("cd_flag", "SELECT flag, count(DISTINCT amt) AS d, count(*) AS c FROM '{fixture}' GROUP BY flag "
         "ORDER BY flag"),
        ("cd_expr_key", "SELECT id - 1 AS k, count(DISTINCT amt) AS d FROM '{fixture}' WHERE id BETWEEN -40 AND 40 "
         "GROUP BY k ORDER BY k"),
        ("cd_int_date", "SELECT flag, count(DISTINCT d) AS d, sum(id) AS s FROM '{fixture}' WHERE id > -1500 "
         "GROUP BY flag ORDER BY flag"),
        ("cd_filter", "SELECT flag, count(DISTINCT id) FILTER (WHERE amt > 0) AS d FROM '{fixture}' GROUP BY flag "
         "ORDER BY flag"),
        ("cd_cut", "SELECT flag, count(DISTINCT amt) AS d FROM '{fixture}' WHERE amt IS NULL OR amt < 2000 "
         "GROUP BY flag ORDER BY d DESC, flag LIMIT 2"),
    ]
    for dlabel, d_sql in DISTINCT:
        sql = d_sql.format(fixture=fixture)
        duck_rel = con.execute(sql)
        lanes = json_lanes(duck_rel)
        want = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in duck_rel.fetchall()]
        for pairs in ("1", "0"):
            for j in ("1", "3", "16"):
                total += 1
                label = f"distinct:{dlabel} pairs={pairs} -j {j}"
                r = subprocess.run([ZPQ, "query", "-j", j, "--query", sql], capture_output=True, text=True,
                                   env={**os.environ, "ZPQ_DISTINCT_PAIRS": pairs})
                try:
                    if r.returncode != 0:
                        raise RuntimeError((r.stderr or r.stdout).strip()[:70])
                    got = [tuple(canon(v, lanes[i]) for i, v in enumerate(from_zpq(list(row.values()), lanes)))
                           for row in read_rows(r.stdout, parse_float=Decimal)]
                    problem = None if got == want else f"zpq={got[:3]}.. ({len(got)}) duck={want[:3]}.. ({len(want)})"
                except Exception as e:
                    problem = f"error: {e}"
                if problem:
                    fails += 1
                    print(f"  FAIL  {label}  {problem}")
                else:
                    print(f"  OK    {label}  ({len(got)} rows, in order)")

    # HAVING keeps the complete groups it makes TRUE, then ORDER BY / LIMIT cut once: a cut taken before HAVING (count
    # cut, sketch, census, TPUT, ResultCut) would rank the wrong set and underfill. g = floor(sqrt(i)) gives group g
    # 2g + 1 rows, so the largest counts are exactly the ones HAVING drops. x is NULL in places, all NULL in group 7.
    hav = os.path.join(tmp, "having.parquet")
    con.execute(f"""COPY (SELECT floor(sqrt(i))::INTEGER AS g, (i % 3)::INTEGER AS h, i::BIGINT AS v,
        (CASE WHEN i % 11 = 0 OR floor(sqrt(i)) = 7 THEN NULL ELSE i * 1.5 END)::DOUBLE AS x,
        CAST(i AS DECIMAL(18,2)) AS p, ('s' || (i % 17)) AS s
      FROM range(0, 1000000) t(i)) TO '{hav}' (FORMAT PARQUET, ROW_GROUP_SIZE 65536)""")
    HAVING_SQL = [
        "SELECT g, count(*) AS c FROM '{f}' GROUP BY g HAVING count(*) < 400 ORDER BY c DESC LIMIT 5",
        "SELECT g, count(*) AS c FROM '{f}' GROUP BY g HAVING c < 400 ORDER BY c DESC LIMIT 5 OFFSET 3",
        "SELECT g FROM '{f}' GROUP BY g HAVING sum(v) < 5000000 ORDER BY sum(v) DESC LIMIT 4",
        "SELECT g, max(v) AS m FROM '{f}' GROUP BY g HAVING min(v) BETWEEN 1000 AND 2000 ORDER BY m",
        # NULL is UNKNOWN: neither a predicate on sum(x) nor its negation keeps group 7.
        "SELECT g, count(*) AS c FROM '{f}' WHERE g BETWEEN 5 AND 9 GROUP BY g HAVING sum(x) > 0 ORDER BY g",
        "SELECT g, count(*) AS c FROM '{f}' WHERE g BETWEEN 5 AND 9 GROUP BY g HAVING NOT (sum(x) > 0) ORDER BY g",
        "SELECT g, count(*) AS c FROM '{f}' WHERE g BETWEEN 5 AND 9 GROUP BY g HAVING sum(x) IS NULL OR g = 5 ORDER BY g",
        # Exact DECIMAL and integer comparisons, an IN list, a string key, an AVG mean.
        "SELECT g, sum(p) AS sp FROM '{f}' WHERE g < 30 GROUP BY g HAVING sum(p) >= 12345.67 AND g NOT IN (20, 21) "
        "ORDER BY g",
        "SELECT s, count(*) AS c FROM '{f}' GROUP BY s HAVING s > 's5' AND avg(v) > 499999.5 ORDER BY s",
        "SELECT h, s, count(*) AS c FROM '{f}' GROUP BY h, s HAVING count(*) >= 19608 ORDER BY c DESC, h, s LIMIT 3",
        # LIMIT without ORDER BY under HAVING: the first qualifying groups, not the first groups then filtered.
        ("SELECT g, count(*) AS c FROM '{f}' GROUP BY g HAVING count(*) > 300 LIMIT 4",
         "SELECT g, count(*) AS c FROM '{f}' GROUP BY g HAVING count(*) > 300 ORDER BY g LIMIT 4"),
        # Without GROUP BY: one group, kept or dropped; on empty input, count(*) is 0 and the group still exists.
        "SELECT count(*) AS c, sum(v) AS sv FROM '{f}' HAVING count(*) > 5",
        "SELECT count(*) AS c FROM '{f}' HAVING sum(v) < 0",
        "SELECT count(*) AS c FROM '{f}' WHERE v < 0 HAVING count(*) = 0",
        "SELECT count(*) AS c FROM '{f}' WHERE v < 0 HAVING count(*) > 0",
    ]
    for i, h_sql in enumerate(HAVING_SQL):
        # A pair: zpq's query and DuckDB's, which orders by g where zpq answers in first-appearance order (g grows
        # with i, so the two agree).
        h_sql, d_sql = h_sql if isinstance(h_sql, tuple) else (h_sql, h_sql)
        sql = h_sql.format(f=hav)
        duck_rel = con.execute(d_sql.format(f=hav))
        lanes = json_lanes(duck_rel)
        want = [tuple(canon(v, lanes[k]) for k, v in enumerate(row)) for row in duck_rel.fetchall()]
        for j in ("1", "3", "16"):
            total += 1
            label = f"having:{i} -j{j}"
            r = subprocess.run([ZPQ, "query", "--query", sql, "-j", j], capture_output=True, text=True)
            try:
                if r.returncode != 0:
                    raise RuntimeError((r.stderr or r.stdout).strip()[:90])
                agg = read_rows(r.stdout, parse_float=Decimal)
                zrows = [list(row.values()) for row in agg] if isinstance(agg, list) else [list(agg.values())]
                got = [tuple(canon(v, lanes[k]) for k, v in enumerate(from_zpq([fold_avg(v) for v in row], lanes)))
                       for row in zrows]
                problem = None if got == want else f"zpq={got[:3]}.. ({len(got)}) duck={want[:3]}.. ({len(want)})"
            except Exception as e:
                problem = f"error: {e}"
            if problem:
                fails += 1
                print(f"  FAIL  {label}  {problem}  [{h_sql[:60]}]")
            else:
                print(f"  OK    {label}  ({len(got)} rows)")

    # ORDER BY over plain rows, with a LIMIT: the first OFFSET + LIMIT rows by the keys, NULLs last either way, ties
    # by input position (DuckDB ranks those by file_row_number to say the same).
    TOP_ROWS = [
        "SELECT name, id FROM '{f}' WHERE name LIKE 'n1%' ORDER BY id DESC LIMIT 7",
        "SELECT name FROM '{f}' ORDER BY ts LIMIT 10",
        "SELECT name FROM '{f}' ORDER BY ts DESC LIMIT 4",
        "SELECT * FROM '{f}' WHERE amt > 0 ORDER BY amt DESC, id LIMIT 5 OFFSET 3",
        "SELECT nname, id FROM '{f}' ORDER BY nname DESC, id LIMIT 12",
        "SELECT name, price FROM '{f}' ORDER BY price DESC LIMIT 3",
        "SELECT id AS k, name FROM '{f}' ORDER BY k LIMIT 3 OFFSET 2",
        "SELECT id FROM '{f}' WHERE id < -100000 ORDER BY id LIMIT 3",
        "SELECT id FROM '{f}' ORDER BY id LIMIT 0",
        ("SELECT id, flag FROM '{f}' ORDER BY flag LIMIT 6",
         "SELECT id, flag FROM read_parquet('{f}', file_row_number=true) ORDER BY flag, file_row_number LIMIT 6"),
        ("SELECT id, flag FROM '{f}' ORDER BY flag DESC LIMIT 6 OFFSET 1000",
         "SELECT id, flag FROM read_parquet('{f}', file_row_number=true) ORDER BY flag DESC, file_row_number "
         "LIMIT 6 OFFSET 1000"),
        # Without a LIMIT every row is sorted; keys may be expressions of the row.
        "SELECT id, name FROM '{f}' WHERE id BETWEEN -30 AND 30 ORDER BY name DESC, id",
        "SELECT * FROM '{f}' ORDER BY ts DESC, id",
        ("SELECT flag, id FROM '{f}' WHERE id < 0 ORDER BY flag",
         "SELECT flag, id FROM read_parquet('{f}', file_row_number=true) WHERE id < 0 ORDER BY flag, file_row_number"),
        "SELECT id FROM '{f}' ORDER BY id * -1 LIMIT 5",
        "SELECT name, id FROM '{f}' ORDER BY strlen(name) DESC, id LIMIT 8 OFFSET 2",
        "SELECT id, flag FROM '{f}' WHERE id BETWEEN 0 AND 40 ORDER BY CASE WHEN flag = true THEN 0 ELSE 1 END, id",
        "SELECT name FROM '{f}' ORDER BY coalesce(nname, 'zz') DESC, id LIMIT 6",
        ("SELECT name, d FROM '{f}' WHERE id > 0 ORDER BY d DESC LIMIT 5",
         "SELECT name, d FROM read_parquet('{f}', file_row_number=true) WHERE id > 0 ORDER BY d DESC, "
         "file_row_number LIMIT 5"),
        # Row groups ruled out by their statistics against the heap's last row: floats ascending past NULLs, a DATE
        # under a filter, strings with NULLs, and a first key whose later keys decide.
        "SELECT id, amt FROM '{f}' ORDER BY amt LIMIT 5 OFFSET 2",
        "SELECT id, d FROM '{f}' WHERE flag = true ORDER BY d DESC, id LIMIT 4",
        "SELECT id, nname FROM '{f}' ORDER BY nname, id LIMIT 5",
        "SELECT id, ts FROM '{f}' WHERE amt IS NOT NULL ORDER BY ts DESC, id LIMIT 6 OFFSET 1",
    ]
    for i, t_sql in enumerate(TOP_ROWS):
        z_sql, d_sql = t_sql if isinstance(t_sql, tuple) else (t_sql, t_sql)
        for fmt in ("jsonl", "parquet"):
            total += 1
            label = f"top-rows:{i} {fmt}"
            out = os.path.join(tmp, "top.parquet")
            cmd = [ZPQ, "query", "--query", z_sql.format(f=fixture)]
            if fmt == "parquet":
                cmd += ["-o", out]
            r = subprocess.run(cmd, capture_output=True, text=True)
            try:
                if r.returncode != 0:
                    raise RuntimeError((r.stderr or r.stdout).strip()[:90])
                duck_rel = con.execute(d_sql.format(f=fixture))
                lanes = json_lanes(duck_rel)
                drows = duck_rel.fetchall()
                if fmt == "jsonl":
                    # JSON prints a DATE as its days and a TIMESTAMP_MS as its milliseconds since the epoch.
                    def stored(v):
                        if isinstance(v, datetime.datetime):
                            return (v - datetime.datetime(1970, 1, 1)) // datetime.timedelta(milliseconds=1)
                        if isinstance(v, datetime.date):
                            return (v - datetime.date(1970, 1, 1)).days
                        return v
                    drows = [tuple(stored(v) for v in row) for row in drows]
                want = [tuple(canon(v, lanes[k]) for k, v in enumerate(row)) for row in drows]
                if fmt == "parquet":
                    pq.read_table(out)  # the strict reader must accept it
                    zrows = [list(row) for row in con.execute(f"SELECT * FROM '{out}'").fetchall()]
                    got = [tuple(canon(v, lanes[k]) for k, v in enumerate(row)) for row in zrows]
                else:
                    got = [tuple(canon(v, lanes[k]) for k, v in enumerate(from_zpq(list(row.values()), lanes)))
                           for row in read_rows(r.stdout, parse_float=Decimal)]
                problem = None if got == want else f"zpq={got[:3]}.. ({len(got)}) duck={want[:3]}.. ({len(want)})"
            except Exception as e:
                problem = f"error: {e}"
            if problem:
                fails += 1
                print(f"  FAIL  {label}  {problem}  [{z_sql[:60]}]")
            else:
                print(f"  OK    {label}  ({len(got)} rows, in order)")

    # Two files whose key runs opposite ways, with ties across row groups and files and some NULL keys: row groups
    # are read most promising first and passed over against the heap, and every tie still goes to the earlier row.
    tie_dir = os.path.join(tmp, "top_ties")
    os.makedirs(tie_dir, exist_ok=True)
    for name, k in (("a", "3 - i // 1500"), ("b", "i // 1500")):
        con.execute(f"""COPY (SELECT i AS id, (CASE WHEN i % 997 = 0 THEN NULL ELSE {k} END)::BIGINT AS k,
            (i * 7919) % 101 AS v, '{name}' AS src FROM range(0, 6000) t(i))
            TO '{tie_dir}/{name}.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)""")
    tie_glob = f"{tie_dir}/*.parquet"
    # Every -j: workers rank row groups in parallel and share their cutoff; the answer must not depend on how many.
    for order, tail, j in [(o, t, j) for o, t in (("k", "LIMIT 7"), ("k DESC", "LIMIT 9 OFFSET 2"),
                                                  ("k DESC, v", "LIMIT 11"), ("k", "LIMIT 3000"),
                                                  ("k DESC", "LIMIT 5 OFFSET 5990"))
                           for j in ("1", "3", "16")]:
        total += 1
        label = f"top-rows:ties {order} {tail} -j{j}"
        z_sql = f"SELECT src, id, k, v FROM '{tie_glob}' ORDER BY {order} {tail}"
        d_sql = (f"SELECT src, id, k, v FROM read_parquet('{tie_glob}', filename=true, file_row_number=true) "
                 f"ORDER BY {order}, filename, file_row_number {tail}")
        r = subprocess.run([ZPQ, "query", "--query", z_sql, "-j", j], capture_output=True, text=True)
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:90])
            want = [tuple(row) for row in con.execute(d_sql).fetchall()]
            got = [(row["src"], row["id"], row["k"], row["v"]) for row in read_rows(r.stdout)]
            problem = None if got == want else f"zpq={got[:3]}.. ({len(got)}) duck={want[:3]}.. ({len(want)})"
        except Exception as e:
            problem = f"error: {e}"
        if problem:
            fails += 1
            print(f"  FAIL  {label}  {problem}")
        else:
            print(f"  OK    {label}  ({len(got)} rows, in order)")

    # Winners gathered from row groups of small pages (V1 and V2, a dictionary turning PLAIN mid-chunk), the pages
    # between them passed over unread: every output column, NULLs, NaN, unsigned and DECIMAL keys, filter gaps.
    gather_glob = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ci", "fixtures", "parquet",
                               "topn_gather_*.parquet")
    for order, where, tail in (("s DESC", "k > 3", "LIMIT 9 OFFSET 4"), ("f", "", "LIMIT 12"),
                               ("f DESC", "", "LIMIT 12"), ("u DESC", "", "LIMIT 5"), ("dec DESC, id", "", "LIMIT 8"),
                               ("k DESC, s", "s LIKE 's01%'", "LIMIT 20"), ("ts", "", "LIMIT 1500"),
                               # A string key coded by its dictionary throughout: the dictionary bounds each group.
                               ("c", "c != ''", "LIMIT 6"), ("c DESC", "c != ''", "LIMIT 6 OFFSET 2"),
                               ("c, k DESC", "k > 30", "LIMIT 9")):
        for j in ("1", "16"):
            total += 1
            label = f"top-rows:gather {order} {where} {tail} -j{j}"
            cond = f"WHERE {where} " if where else ""
            z_sql = f"SELECT * FROM '{gather_glob}' {cond}ORDER BY {order} {tail}"
            d_sql = (f"SELECT * EXCLUDE (filename, file_row_number) FROM read_parquet('{gather_glob}', filename=true, "
                     f"file_row_number=true) {cond}ORDER BY {order}, filename, file_row_number {tail}")
            r = subprocess.run([ZPQ, "query", "--query", z_sql, "-j", j], capture_output=True, text=True)
            try:
                if r.returncode != 0:
                    raise RuntimeError((r.stderr or r.stdout).strip()[:90])
                duck_rel = con.execute(d_sql)
                lanes = json_lanes(duck_rel)

                def stored(v):
                    if isinstance(v, datetime.datetime):
                        return (v - datetime.datetime(1970, 1, 1)) // datetime.timedelta(milliseconds=1)
                    return v
                want = [tuple(canon(stored(v), lanes[k]) for k, v in enumerate(row)) for row in duck_rel.fetchall()]
                got = [tuple(canon(v, lanes[k]) for k, v in enumerate(from_zpq(list(row.values()), lanes)))
                       for row in read_rows(r.stdout, parse_float=Decimal)]
                problem = None if got == want else f"zpq={got[:2]}.. ({len(got)}) duck={want[:2]}.. ({len(want)})"
            except Exception as e:
                problem = f"error: {e}"
            if problem:
                fails += 1
                print(f"  FAIL  {label}  {problem}")
            else:
                print(f"  OK    {label}  ({len(got)} rows, in order)")

    # A computed output that fails on a row whose group statistics would pass it over: the error must stand, as it
    # does reading in input order, whatever the order and pruning a bare-column Top-N may use.
    err_file = os.path.join(tmp, "topn_error.parquet")
    import pyarrow as pa_err  # `pa` is local to main (imported again below), unbound here
    pq.write_table(pa_err.table({"id": [0, 1], "k": [100, 0]}), err_file, row_group_size=1, use_dictionary=False)
    for extra in ([], ["--scan-all"]):
        total += 1
        r = subprocess.run([ZPQ, "query", "--query", f"SELECT 100 / id AS z FROM '{err_file}' ORDER BY k LIMIT 1"]
                           + extra, capture_output=True, text=True)
        label = f"top-rows:fallible-output {' '.join(extra) or 'default'}"
        if r.returncode == 1 and not r.stdout and "division by zero" in r.stderr:
            print(f"  OK    {label}  (fails as in input order)")
        else:
            fails += 1
            print(f"  FAIL  {label}  rc={r.returncode} stdout={r.stdout[:60]!r} stderr={r.stderr[:90]!r}")

    # Cuts that read only some rows' aggregate inputs must not drop the error another row raises. 160 keys of 30 rows
    # (q = 1..30) outrank 2000 keys of 23 rows whose q is 0, so a cut certifies without reading those; the full scan
    # divides by zero. Shuffled, PLAIN keys, so no dictionary bounds the groups.
    cut_err = os.path.join(tmp, "cut_errors.parquet")
    import random as rnd_err
    import pyarrow as pa_cut  # `pa` is local to main (imported again below), unbound here
    rows_err = [(k, q) for k in range(160) for q in range(1, 31)] + [(k, 0) for k in range(160, 2160) for _ in range(23)]
    rnd_err.Random(1991).shuffle(rows_err)
    pq.write_table(pa_cut.table({"k": [r[0] for r in rows_err], "q": [r[1] for r in rows_err]}), cut_err,
                   row_group_size=8192, use_dictionary=False)
    CUT_ERRORS = [
        f"SELECT k, count(DISTINCT 1000000000000 / q) AS d FROM '{cut_err}' GROUP BY k ORDER BY d DESC LIMIT 10",
        f"SELECT k, count(DISTINCT q) AS d, sum(1000000000000 / q) AS s FROM '{cut_err}' GROUP BY k "
        "ORDER BY d DESC LIMIT 10",
        # The COUNT census reads aggregate inputs only for the keys it admits, too (first keys: the scan.zig tests,
        # whose fixture keeps the failing row out of the prefix).
        f"SELECT k, count(*) AS c, sum(1000000000000 / q) AS s FROM '{cut_err}' GROUP BY k ORDER BY c DESC LIMIT 10",
    ]
    for i, sql in enumerate(CUT_ERRORS):
        for j in ("1", "3", "16"):
            total += 1
            r = subprocess.run([ZPQ, "query", "-j", j, "--query", sql], capture_output=True, text=True)
            label = f"cut-errors:{i} -j{j}"
            if r.returncode == 1 and not r.stdout and "division by zero" in r.stderr:
                print(f"  OK    {label}  (fails as the full scan does)")
            else:
                fails += 1
                print(f"  FAIL  {label}  rc={r.returncode} stdout={r.stdout[:60]!r} stderr={r.stderr[:90]!r}")

    # A certificate's trial holds memory the exact scan never does (admitted keys' sets, copied across workers to
    # count them): a budget the exact scan fits must not fail the query when the trial exceeds it. 160 keys with 50
    # distinct 4000-byte strings outrank 1000 keys of 40 one-string rows.
    budget_file = os.path.join(tmp, "budget_copy.parquet")
    import pyarrow as pa_budget  # `pa` is local to main (imported again below), unbound here
    keys_b = [k for k in range(160) for _ in range(50)] + [k for k in range(160, 1160) for _ in range(40)]
    vals_b = [f"v-{v:04d}" + "x" * 3994 for v in range(50)] * 160 + ["z"] * 40_000
    order_b = list(range(len(keys_b)))
    rnd_err.Random(9113).shuffle(order_b)
    pq.write_table(pa_budget.table({"k": [keys_b[i] for i in order_b], "s": [vals_b[i] for i in order_b]}),
                   budget_file, row_group_size=1024, use_dictionary=False)
    budget_sql = f"SELECT k, count(DISTINCT s) AS d FROM '{budget_file}' GROUP BY k ORDER BY d DESC LIMIT 10"
    exact = subprocess.run([ZPQ, "query", "-j", "16", "--max-memory", "140MB", "--query", budget_sql],
                           capture_output=True, text=True, env={**os.environ, "ZPQ_SKETCH_TOPK": "0"})
    for j in ("3", "16"):
        total += 1
        r = subprocess.run([ZPQ, "query", "-j", j, "--max-memory", "140MB", "--query", budget_sql],
                           capture_output=True, text=True)
        label = f"cut-budget -j{j}"
        if exact.returncode == 0 and r.returncode == 0 and r.stdout == exact.stdout:
            print(f"  OK    {label}  (answers as the exact scan)")
        else:
            fails += 1
            print(f"  FAIL  {label}  rc={r.returncode} exact_rc={exact.returncode} stderr={r.stderr[:90]!r}")

    # A full sort holds every row: past its share of --max-memory it fails cleanly instead of running out.
    total += 1
    r = subprocess.run([ZPQ, "query", "--query", f"SELECT * FROM '{fixture}' ORDER BY name", "--max-memory", "300000"],
                       capture_output=True, text=True)
    if r.returncode == 1 and not r.stdout and "max-memory" in r.stderr:
        print("  OK    top-rows:budget  (refused cleanly)")
    else:
        fails += 1
        print(f"  FAIL  top-rows:budget  rc={r.returncode} stdout={r.stdout[:60]!r} stderr={r.stderr[:90]!r}")

    # GROUP BY binds a name to the column before a SELECT alias (DuckDB 1.5 and 2.0 alike): `amt AS name` then is no
    # key of `GROUP BY name`, and both engines refuse the query.
    total += 1
    alias_sql = f"SELECT amt AS name, count(*) AS c FROM '{fixture}' GROUP BY name"
    r = subprocess.run([ZPQ, "query", "--query", alias_sql], capture_output=True, text=True)
    try:
        con.execute(alias_sql).fetchall()
        duck_refused = False
    except duckdb.Error:
        duck_refused = True
    if duck_refused and r.returncode == 2 and not r.stdout:
        print("  OK    group-by:column-before-alias  (both refuse)")
    else:
        fails += 1
        print(f"  FAIL  group-by:column-before-alias  duck_refused={duck_refused} rc={r.returncode} {r.stdout[:60]!r}")

    # sum(x + c) folds x and adds c once per value (`sumOffset`): every fold path must add it as often as it sums. `c`
    # is constant per row group (the statistics fold), `g` low-cardinality (dictionary codes), `n` NULL in places and
    # NULL in all of group g = 4 under the filter below.
    offs = os.path.join(tmp, "sum_offset.parquet")
    # DuckDB rounds ROW_GROUP_SIZE up to its 2048-row vectors, so c is constant over 2048 rows.
    con.execute(f"""COPY (SELECT (i // 2048)::INTEGER AS c, (i % 5)::INTEGER AS g,
        (CASE WHEN i % 9 = 0 OR i % 5 = 4 AND i >= 2048 THEN NULL ELSE i - 2000 END)::INTEGER AS n,
        (i * 3)::BIGINT AS big
      FROM range(0, 4000) t(i)) TO '{offs}' (FORMAT PARQUET, ROW_GROUP_SIZE 500)""")
    OFFSET_SQL = [
        # Alone, c folds from statistics: every row group is constant.
        "SELECT sum(c + 3) AS a, sum(c - 1) AS b FROM '{f}'",
        "SELECT sum(c + 3) AS a, sum(3 + c) AS b, sum(c - 7) AS d, sum(n + 1) AS e, sum(g + 4) AS f, sum(big + 2) AS h "
        "FROM '{f}'",
        "SELECT sum(n + 11) AS a, sum(c - 1) AS b, count(n) AS k FROM '{f}' WHERE n > 10",
        "SELECT sum(n - 5) AS a FROM '{f}' WHERE n IS NULL",
        "SELECT g, sum(n + 5) AS s, sum(c - 2) AS t, sum(g + 9) AS u FROM '{f}' GROUP BY g",
        "SELECT g, sum(n + 5) AS s FROM '{f}' WHERE c > 0 GROUP BY g",
    ]
    # Each also with encoded execution off, so the row folds run where the dictionary-code folds would, and with
    # --trust-stats, so constant row groups fold from statistics.
    modes = [("", "1", []), (" (row folds)", "0", []), (" (trust-stats)", "1", ["--trust-stats"])]
    for i, (o_sql, (mode, encoded, flags)) in enumerate((q, m) for q in OFFSET_SQL for m in modes):
        total += 1
        sql = o_sql.format(f=offs)
        label = f"sum-offset:{i // len(modes)}{mode}"
        r = subprocess.run([ZPQ, "query", "--query", sql] + flags, capture_output=True, text=True,
                           env={**os.environ, "ZPQ_ENCODED": encoded})
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:90])
            duck_rel = con.execute(sql)
            lanes = json_lanes(duck_rel)
            want = canon_rows(duck_rel.fetchall(), lanes)
            agg = read_rows(r.stdout, parse_float=Decimal)
            zrows = [list(row.values()) for row in agg] if isinstance(agg, list) else [list(agg.values())]
            got = canon_rows([from_zpq(row, lanes) for row in zrows], lanes)
            problem = None if got == want else f"zpq={got} duck={want}"
        except Exception as e:
            problem = f"error: {e}"
        if problem:
            fails += 1
            print(f"  FAIL  {label}  {problem}")
        else:
            print(f"  OK    {label}  ({len(got)} rows)")

    # An integer avg sums exactly, so its groups take the per-worker merge, whose order varies with -j and timing. Values
    # near 2^60 put every sum far past 2^53, where a double sum would round differently in each order: the answer
    # must come out byte for byte the same at every -j, and match DuckDB's exact sum.
    huge = os.path.join(tmp, "avg_huge.parquet")
    con.execute(f"""COPY (SELECT (i % 7)::INTEGER AS g, ((1::BIGINT << 60) + i * 1000003)::BIGINT AS v,
        (i % 1000)::INTEGER AS w FROM range(0, 60000) t(i)) TO '{huge}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)""")
    sql = f"SELECT g, avg(v) AS a, avg(w) AS b, count(*) AS n FROM '{huge}' GROUP BY g"
    outs = set()
    for j in ("1", "2", "4", "16"):
        r = subprocess.run([ZPQ, "query", "--query", sql, "-j", j], capture_output=True, text=True)
        outs.add(json.dumps(read_rows(r.stdout, parse_float=Decimal), default=str) if r.returncode == 0 else f"error {r.stderr[:80]}")
    total += 1
    want = {row[0]: row for row in con.execute(sql.replace("avg(v)", "sum(v)::HUGEINT / count(v)")).fetchall()}
    got = {row["g"]: row for row in json.loads(next(iter(outs)), parse_float=Decimal)} if len(outs) == 1 else {}
    # The double of the exact sum, divided as a double: what DuckDB computes from its HUGEINT sum.
    exact = all(float(got[g]["a"]["sum"]) / got[g]["a"]["count"] == float(want[g][1]) for g in want) if got else False
    if len(outs) == 1 and exact:
        print(f"  OK    avg-exact: integer avg identical at -j 1/2/4/16 and equal to the exact sum's quotient")
    else:
        fails += 1
        print(f"  FAIL  avg-exact: {len(outs)} distinct answers across -j; exact={exact}")

    # Plain-encoded (no dictionary) low-cardinality string keys: zpq codes them per batch to group by code. NULL keys,
    # a filter, two keys, and a key with too many values to code must all group as DuckDB does.
    import pyarrow as pa
    plain = os.path.join(tmp, "plain_keys.parquet")
    n = 9000
    pq.write_table(pa.table({
        "f": pa.array([None if i % 13 == 0 else "ARN"[i % 3] for i in range(n)], pa.string()),
        "s": pa.array(["OF"[(i // 7) % 2] for i in range(n)], pa.string()),
        "u": pa.array([f"user{i % 1500}" for i in range(n)], pa.string()),
        "x": pa.array([float(i % 101) for i in range(n)], pa.float64()),
    }), plain, use_dictionary=False, row_group_size=3000)
    for i, q in enumerate([
        "SELECT f, s, count(*) AS n, sum(x) AS t, avg(x) AS a FROM '{p}' GROUP BY f, s",
        "SELECT f, count(*) AS n FROM '{p}' WHERE x > 50 GROUP BY f",
        "SELECT u, count(*) AS n FROM '{p}' GROUP BY u",
    ]):
        total += 1
        sql = q.format(p=plain)
        r = subprocess.run([ZPQ, "query", "--query", sql], capture_output=True, text=True)
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:90])
            rel = con.execute(sql)
            lanes = json_lanes(rel)
            want = canon_rows(rel.fetchall(), lanes)
            got = canon_rows([from_zpq([fold_avg(v) for v in row.values()], lanes) for row in read_rows(r.stdout, parse_float=Decimal)], lanes)
            problem = None if got == want else f"zpq={got[:3]} duck={want[:3]}"
        except Exception as e:
            problem = f"error: {e}"
        if problem:
            fails += 1
            print(f"  FAIL  plain-keys:{i}  {problem}")
        else:
            print(f"  OK    plain-keys:{i}  ({len(got)} groups)")

    # avg over UINT64 values at and past 2^63 sums them unsigned on every fold path: rows, dictionary codes, grouped,
    # encoded execution on and off.
    u64f = os.path.join(tmp, "avg_u64.parquet")
    big = [2**64 - 1, 2**63, 2**63 + 5, 7]
    pq.write_table(pa.table({"k": pa.array([i % 2 for i in range(4000)], pa.int32()),
                             "u": pa.array([big[i % 4] for i in range(4000)], pa.uint64())}), u64f, row_group_size=500)
    for i, q in enumerate(["SELECT k, avg(u) AS a FROM '{f}' GROUP BY k", "SELECT avg(u) AS a FROM '{f}'"]):
        for encoded in ("1", "0"):
            total += 1
            sql = q.format(f=u64f)
            r = subprocess.run([ZPQ, "query", "--query", sql], capture_output=True, text=True,
                               env={**os.environ, "ZPQ_ENCODED": encoded})
            try:
                if r.returncode != 0:
                    raise RuntimeError((r.stderr or r.stdout).strip()[:90])
                want = sorted(con.execute(sql).fetchall(), key=repr)
                agg = read_rows(r.stdout, parse_float=Decimal)
                rows = [list(row.values()) for row in agg] if isinstance(agg, list) else [list(agg.values())]
                got = sorted([[float(v["sum"]) / v["count"] if isinstance(v, dict) else v for v in row] for row in rows], key=repr)
                ok = len(got) == len(want) and all(same(g, w) for gr, wr in zip(got, want) for g, w in zip(gr, wr))
                problem = None if ok else f"zpq={got} duck={want}"
            except Exception as e:
                problem = f"error: {e}"
            label = f"avg-u64:{i} (encoded={encoded})"
            if problem:
                fails += 1
                print(f"  FAIL  {label}  {problem}")
            else:
                print(f"  OK    {label}")

    # Multi-column string keys whose framed length is a multiple of 9 (NULLs, short and long strings, a NUL byte), and
    # an alias that names another GROUP BY key: the keys print as stored, and the aliased column is its expression's.
    keyf = os.path.join(tmp, "string_keys.parquet")
    pq.write_table(pa.table({
        "a": pa.array([None, "r", "xy", "xy", None, "x"] * 50, pa.string()),
        "b": pa.array(["abc", "abc", "abcdef", "abcdefg", "y" * 57, "y" * 57] * 50, pa.string()),
        "h": pa.array([None, 1, 2, None, 3, 4] * 50, pa.int64()),
        "s": pa.array(["abcX\x01\x00\x00\x00Qjkl", "q", "", "zz", "abcX\x01\x00\x00\x00Qjkl", "w"] * 50, pa.string()),
        "g": pa.array([5, 1, 3, 1, 2, 4] * 50, pa.int32()),
    }), keyf)
    for i, q in enumerate([
        "SELECT a, b, count(*) AS n FROM '{f}' GROUP BY a, b",
        "SELECT h, s, count(*) AS n FROM '{f}' GROUP BY h, s",
        "SELECT a AS aa, g AS a, count(*) AS c FROM '{f}' GROUP BY a, g",
        "SELECT a AS g, g AS a, count(*) AS c FROM '{f}' GROUP BY a, g ORDER BY g, a",
    ]):
        total += 1
        sql = q.format(f=keyf)
        r = subprocess.run([ZPQ, "query", "--query", sql], capture_output=True, text=True)
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:90])
            rel = con.execute(sql)
            lanes = json_lanes(rel)
            rows = [from_zpq(list(row.values()), lanes) for row in read_rows(r.stdout, parse_float=Decimal)]
            if "ORDER BY" in q:
                want = [tuple(canon(v) for v in row) for row in rel.fetchall()]
                got = [tuple(canon(v) for v in row) for row in rows]
            else:
                want = canon_rows(rel.fetchall(), lanes)
                got = canon_rows(rows, lanes)
            problem = None if got == want else f"zpq={got[:3]} duck={want[:3]}"
        except Exception as e:
            problem = f"error: {e}"
        if problem:
            fails += 1
            print(f"  FAIL  key-framing:{i}  {problem}")
        else:
            print(f"  OK    key-framing:{i}  ({len(got)} rows)")

    # `-o` keeps the parquet type of pass-through columns: bare GROUP BY keys and min/max of a column come back as
    # the source type (DATE, STRING, INT8, UINT64, ...), not the evaluation lane's plain INT64 / BYTE_ARRAY. DECIMAL
    # stays DOUBLE (its f64 value); computed columns keep their computed type.
    import datetime as dt
    import decimal
    import pyarrow as pa
    typed = os.path.join(tmp, "typed.parquet")
    k = range(6)
    pq.write_table(pa.table({
        "s": pa.array(["a", "b", "a", "b", "a", None], pa.string()),
        "bin": pa.array([b"x", b"y"] * 3, pa.binary()),
        "d": pa.array([dt.date(2024, 1, 1 + i % 2) for i in k], pa.date32()),
        "ts": pa.array([dt.datetime(2024, 1, 1, 0, 0, i % 2) for i in k], pa.timestamp("ms")),
        "tsz": pa.array([dt.datetime(2024, 1, 1, 0, 0, i % 2) for i in k], pa.timestamp("us", tz="UTC")),
        "b": pa.array([i % 2 == 0 for i in k], pa.bool_()),
        "i8": pa.array([i % 2 - 1 for i in k], pa.int8()),
        "u32": pa.array([4294967295 if i % 2 else 1 for i in k], pa.uint32()),
        "u64": pa.array([2**64 - 1 if i % 2 else 1 for i in k], pa.uint64()),
        "f": pa.array([0.5 if i % 2 else 1.5 for i in k], pa.float32()),
        "tm": pa.array([dt.time(1, i % 2) for i in k], pa.time32("ms")),
        "dec": pa.array([decimal.Decimal("1.25") * (i % 2) for i in k], pa.decimal128(9, 2)),
    }), typed)
    src_types = pq.read_schema(typed)
    for col in src_types.names:
        total += 1
        out = os.path.join(tmp, "ztyped.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", typed, "--group-by", col, "-o", out, "--aggregate",
                            f"min({col}) AS lo, max({col}) AS hi, count(*) AS n, sum(i8) AS si"],
                           capture_output=True, text=True)
        label = f"typed-o:{col}"
        want = src_types.field(col).type
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:70])
            got = pq.read_table(out)
            types = [got.schema.field(c).type for c in (col, "lo", "hi", "n", "si")]
            if types != [want, want, want, pa.int64(), pa.int64()]:
                raise RuntimeError(f"types {[str(t) for t in types]} want {want} x3, int64 x2")
            # DuckDB hands back tz-aware timestamps only through pytz; it reads them as naive UTC instead, which is
            # what canon() turns zpq's tz-aware values into.
            lanes = None
            zrows = canon_rows([list(row.values()) for row in got.to_pylist()], lanes)
            c = f"timezone('UTC', {col})" if getattr(want, "tz", None) else col
            drows = rows_sorted(con, f"SELECT {c}, min({c}), max({c}), count(*), sum(i8) FROM '{typed}' GROUP BY 1",
                                lanes)
            if zrows != drows:
                raise RuntimeError(f"rows zpq={zrows[:2]} duck={drows[:2]}")
            print(f"  OK    {label}  ({want})")
        except Exception as e:
            fails += 1
            print(f"  FAIL  {label}  {e}")

    # Exact DECIMAL aggregates written with -o: DECIMAL columns of the type SQL gives the result (sum DECIMAL(38, s),
    # min/max the argument's, avg's exact sum beside its count), read back by pyarrow and DuckDB value for value.
    dfix = "ci/fixtures/parquet/decimal_exact.parquet"
    DEC_O = [
        ("scalar", [], "sum(d9) AS s, min(d38) AS mn, sum(d9 * d18) AS p, avg(dn) AS a",
         "SELECT sum(d9) AS s, min(d38) AS mn, sum(d9 * d18) AS p, sum(dn) AS a__sum, count(dn) AS a__count FROM '{f}'",
         ["decimal128(38, 2)", "decimal128(38, 10)", "decimal128(38, 6)", "decimal128(38, 2)", "int64"]),
        ("grouped", ["--group-by", "k"], "sum(d18 * q) AS p, max(dn) AS mx",
         "SELECT k, sum(d18 * q) AS p, max(dn) AS mx FROM '{f}' GROUP BY k",
         ["string", "decimal128(38, 6)", "decimal128(15, 2)"]),
    ]
    for dlabel, extra, za, dsql, want_types in DEC_O:
        total += 1
        out = os.path.join(tmp, "zdec.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", dfix, "-o", out, "--aggregate", za] + extra, capture_output=True, text=True)
        label = f"decimal-o:{dlabel}"
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:70])
            got = pq.read_table(out)
            types = [str(f.type) for f in got.schema]
            if types != want_types:
                raise RuntimeError(f"types {types} want {want_types}")
            want = rows_sorted(con, dsql.format(f=dfix))
            if canon_rows([list(row.values()) for row in got.to_pylist()]) != want:
                raise RuntimeError(f"pyarrow rows {got.to_pylist()[:2]} want {want[:2]}")
            if rows_sorted(con, f"SELECT * FROM '{out}'") != want:
                raise RuntimeError("DuckDB reads other values back")
            print(f"  OK    {label}  {types}")
        except Exception as e:
            fails += 1
            print(f"  FAIL  {label}  {e}")

    # A row group bigger than --max-memory lets one producer hold: written, printed and aggregated a slice of rows at a
    # time, with the same answers as DuckDB, down to a budget of a few slices.
    big = os.path.join(tmp, "one_big_rg.parquet")
    con.execute(f"""COPY (SELECT i::BIGINT AS id, (i % 97)::INTEGER AS g, CAST(i * 0.01 AS DECIMAL(18,2)) AS price,
        'n' || (i % 1000) AS name, (CASE WHEN i % 7 = 0 THEN NULL ELSE i * 0.5 END)::DOUBLE AS amt,
        CAST(i * 1000000007.01 AS DECIMAL(38,2)) AS wide,
        CASE WHEN i % 5 = 0 THEN NULL ELSE repeat('x', i % 300) END AS pad
        FROM range(0, 50000) t(i)) TO '{big}' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)""")
    tiny = ["--max-memory", "2M", "-j", "2"]
    out = os.path.join(tmp, "zbig.parquet")
    for blabel, args, dsql in [
        ("write", ["-o", out, "--filter", "id >= 100", "--select", "id, price * 2 AS p, name, amt, wide, pad"],
         "SELECT id, price * 2 AS p, name, amt, wide, pad FROM '{f}' WHERE id >= 100"),
        ("print", ["--format", "jsonl", "--filter", "g = 3"], "SELECT * FROM '{f}' WHERE g = 3"),
        ("print-limit", ["--format", "jsonl", "--limit", "7"], "SELECT * FROM '{f}' LIMIT 7"),
        ("aggregate", ["--aggregate", "sum(price) AS s, count(amt) AS c, max(name) AS m, sum(price * 3) AS t, "
                       "sum(wide) AS w, max(pad) AS x, count(pad) AS n"],
         "SELECT sum(price) AS s, count(amt) AS c, max(name) AS m, sum(price * 3) AS t, "
         "sum(wide) AS w, max(pad) AS x, count(pad) AS n FROM '{f}'"),
        ("grouped", ["--aggregate", "sum(price) AS s, min(amt) AS a", "--group-by", "g"],
         "SELECT g, sum(price) AS s, min(amt) AS a FROM '{f}' GROUP BY g"),
    ]:
        total += 1
        label = f"sliced:{blabel}"
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", big] + args + tiny, capture_output=True, text=True)
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:90])
            rel = con.execute(dsql.format(f=big))
            lanes = json_lanes(rel)
            want = canon_rows(rel.fetchall(), lanes)
            cols = [d[0] for d in rel.description]
            if blabel == "write":
                if pq.ParquetFile(out).metadata.num_row_groups < 2:
                    raise RuntimeError("one row group out: the write was not sliced")
                got = canon_rows([[row[c] for c in cols] for row in pq.read_table(out).to_pylist()], lanes)
            elif blabel.startswith("print"):
                got = canon_rows([from_zpq([zjson(l)[c] for c in cols], lanes)
                                  for l in r.stdout.splitlines() if l.strip()], lanes)
                if blabel == "print-limit":
                    # LIMIT without ORDER BY: compare to the file's first rows in order.
                    want = canon_rows(con.execute(f"SELECT * FROM '{big}'").fetchmany(7), lanes)
            else:
                agg = read_rows(r.stdout, parse_float=Decimal)
                rows = agg if isinstance(agg, list) else [agg]
                got = canon_rows([from_zpq([fold_avg(row.get(c)) for c in cols], lanes) for row in rows], lanes)
            if got != want:
                raise RuntimeError(f"zpq={got[:2]} duck={want[:2]} ({len(got)} vs {len(want)})")
            print(f"  OK    {label}  ({len(got)} rows)")
        except Exception as e:
            fails += 1
            print(f"  FAIL  {label}  {e}")
    total += 1
    r = subprocess.run([ZPQ, "query", big, "--aggregate", "sum(price) AS s", "--max-memory", "100K"],
                       capture_output=True, text=True)
    want_s = con.execute(f"SELECT sum(price) FROM '{big}'").fetchone()[0]
    streamed = r.returncode == 0 and read_one(r.stdout, parse_float=Decimal)["s"] == want_s
    print(f"  {'OK  ' if streamed else 'FAIL'}  sliced:tiny-budget  {(r.stderr or r.stdout).strip()[:70]}")
    fails += 0 if streamed else 1
    # INT96 (no DuckDB writer) and FLOAT16 lanes: printed in slices exactly as printed whole.
    total += 1
    import numpy as np
    legacy = os.path.join(tmp, "one_big_rg_int96.parquet")
    n = 20000
    pq.write_table(pa.table({
        "ts": pa.array([i * 1_000_003_000 for i in range(n)], pa.timestamp("ns")),
        "h": pa.array(np.arange(n, dtype=np.float16) / 8),
    }), legacy, row_group_size=n, data_page_size=4096, use_deprecated_int96_timestamps=True)
    whole = subprocess.run([ZPQ, "query", legacy, "--format", "jsonl"], capture_output=True, text=True)
    sliced = subprocess.run([ZPQ, "query", legacy, "--format", "jsonl", "--max-memory", "200K"],
                            capture_output=True, text=True)
    matched = whole.returncode == 0 and sliced.returncode == 0 and whole.stdout == sliced.stdout and \
        len(whole.stdout.splitlines()) == n
    print(f"  {'OK  ' if matched else 'FAIL'}  sliced:int96-float16  {(sliced.stderr or '').strip()[:70]}")
    fails += 0 if matched else 1

    # count/min/max over nullable columns whose pages a reader of another type would refuse: narrow FIXED_LEN_BYTE_ARRAY
    # dictionaries (FLOAT16's 2-byte entries) and DELTA-encoded strings. Encoded execution (count reads levels only)
    # must answer as ZPQ_ENCODED=0 does, and the counts as DuckDB's.
    import numpy as np
    n = 3000
    nar = pa.table({
        "h": pa.array(np.array([(i % 8) - 4 for i in range(n)], dtype=np.float16) / 2, pa.float16(),
                      mask=np.array([i % 5 == 0 for i in range(n)])),
        "s": pa.array([None if i % 7 == 0 else f"v{i % 13}" for i in range(n)]),
        "f": pa.array([None if i % 3 == 0 else bytes([i % 250, 1, 2]) for i in range(n)], pa.binary(3)),
    })
    for elabel, kw in [("dictionary", {"use_dictionary": True}),
                       ("delta", {"use_dictionary": ["h", "f"], "column_encoding": {"s": "DELTA_BYTE_ARRAY"}}),
                       ("delta-length", {"use_dictionary": ["h", "f"],
                                         "column_encoding": {"s": "DELTA_LENGTH_BYTE_ARRAY"}})]:
        path = os.path.join(tmp, f"levels_{elabel}.parquet")
        pq.write_table(nar, path, **kw)
        for col in ["h", "s", "f"]:
            total += 1
            agg = f"count({col}) AS n, min({col}) AS lo, max({col}) AS hi"
            outs = [subprocess.run([ZPQ, "query", path, "--aggregate", agg, "--scan-all"], capture_output=True,
                                   text=True, env=dict(os.environ, ZPQ_ENCODED=e)) for e in ["1", "0"]]
            label = f"levels-only:{elabel}:{col}"
            try:
                for r in outs:
                    if r.returncode != 0:
                        raise RuntimeError((r.stderr or r.stdout).strip()[:70])
                enc, plain = (read_one(r.stdout, parse_float=Decimal) for r in outs)
                if enc != plain:
                    raise RuntimeError(f"encoded {enc} vs ZPQ_ENCODED=0 {plain}")
                want = con.execute(f"SELECT count({col}) FROM '{path}'").fetchone()[0]
                if enc["n"] != want:
                    raise RuntimeError(f"count {enc['n']} want {want}")
                print(f"  OK    {label}  ({want})")
            except Exception as e:
                fails += 1
                print(f"  FAIL  {label}  {e}")

    # Clashing output names fail loudly, naming the column, instead of one column showing another's values.
    CLASHES = [
        ("key_vs_alias", ["--group-by", "name AS k", "--aggregate", "sum(amt) AS name"], "name"),
        ("dup_alias", ["--aggregate", "sum(amt) AS a, min(amt) AS a"], "a"),
        ("dup_alias_gb", ["--group-by", "flag", "--aggregate", "count(*) AS c, sum(amt) AS C"], "C"),
        ("sql_dup_alias", ["--query", "SELECT flag, count(*) AS c, sum(amt) AS c FROM '{fixture}' GROUP BY flag"], "c"),
    ]
    for clabel, cargs, col in CLASHES:
        total += 1
        cargs = [a.format(fixture=fixture) for a in cargs]
        cmd = [ZPQ, "query"] + ([] if cargs[0] == "--query" else [fixture]) + cargs
        r = subprocess.run(cmd, capture_output=True, text=True)
        label = f"name-clash:{clabel}"
        if r.returncode != 0 and not r.stdout and f"output column `{col}`" in r.stderr:
            print(f"  OK    {label}  (rejected: `{col}`)")
        else:
            fails += 1
            print(f"  FAIL  {label}  rc={r.returncode} stdout={r.stdout[:60]!r} stderr={r.stderr.strip()[:80]!r}")

    # Unaliased aggregates: a unique function name stays the output name (as before); clashing ones take the call
    # text, as DuckDB names them, instead of erroring or printing duplicate JSON keys.
    NAMES = [
        ("lone", ["--aggregate", "sum(amt)"], ["sum"]),
        ("two_sums", ["--aggregate", "sum(amt), sum(id)"], ["sum(amt)", "sum(id)"]),
        ("gb_two_mins", ["--group-by", "flag", "--aggregate", "min(amt), min(id)"], ["flag", "min(amt)", "min(id)"]),
        ("sql_two_sums", ["--query", "SELECT sum(amt), sum(id) FROM '{fixture}'"], ["sum(amt)", "sum(id)"]),
        ("sql_gb_unaliased", ["--query", "SELECT flag, count(*), sum(id) FROM '{fixture}' GROUP BY flag"],
         ["flag", "count(*)", "sum(id)"]),
    ]
    for nlabel, nargs, want in NAMES:
        total += 1
        nargs = [a.format(fixture=fixture) for a in nargs]
        cmd = [ZPQ, "query"] + ([] if nargs[0] == "--query" else [fixture]) + nargs
        r = subprocess.run(cmd, capture_output=True, text=True)
        label = f"agg-names:{nlabel}"
        try:
            # object_pairs_hook keeps duplicate keys visible instead of letting the last one win.
            class Pairs(list):
                pass
            rows = read_rows(r.stdout, object_pairs_hook=Pairs)
            got = [k for k, _ in rows[0]]
            assert all([k for k, _ in row] == got for row in rows)
        except Exception as e:
            got = f"error: {e}; {r.stderr.strip()[:60]}"
        if got == want:
            print(f"  OK    {label}  {got}")
        else:
            fails += 1
            print(f"  FAIL  {label}  got {got} want {want}")

    # ZPQ must preserve INPUT row order through filter+write (Iceberg compaction /
    # S3-to-S3 passthrough expect determinism; parallel per-RG workers must not
    # scramble). Compared in FILE order vs the input filtered in file order — this
    # can't go through DuckDB (SQL gives no order guarantee), so it's a separate
    # ZPQ-vs-input check, not a ZPQ-vs-DuckDB one.
    total += 1
    out = os.path.join(tmp, "zord.parquet")
    if os.path.exists(out):
        os.remove(out)
    subprocess.run([ZPQ, "query", fixture, "-o", out, "--filter", "id > 0", "--columns", "id"],
                   capture_output=True)
    zt = pq.read_table(out).column("id").to_pylist()
    it = pq.read_table(fixture).column("id").to_pylist()
    expected = [v for v in it if v is not None and v > 0]
    if zt == expected:
        print(f"  OK    row-order preserved (filter, {len(zt)} rows in input order)")
    else:
        fails += 1
        d = next((i for i, (a, b) in enumerate(zip(zt, expected)) if a != b), None)
        extra = f" first diff @ {d}: {zt[d]} vs {expected[d]}" if d is not None else ""
        print(f"  FAIL  row-order: zpq={len(zt)} expected={len(expected)}{extra}")

    # --- --select honors explicit column ORDER (the reorder mechanism) ---
    total += 1
    out = os.path.join(tmp, "zsel.parquet")
    if os.path.exists(out):
        os.remove(out)
    subprocess.run([ZPQ, "query", fixture, "-o", out,
                    "--select", "name AS name, id AS id"], capture_output=True)
    got = list(pq.read_table(out).column_names)
    if got == ["name", "id"]:
        print("  OK    --select honors column order (name, id)")
    else:
        fails += 1
        print(f"  FAIL  --select column order: got {got} want ['name', 'id']")

    # No nested (struct/list/map) coverage here. ZPQ decodes nested but rejects
    # nested re-encode, so a write-based differential cannot exercise it. Nested
    # read coverage belongs in a read/aggregate harness.

    so_total, so_fails = stats_order_checks(con, tmp)
    total += so_total
    fails += so_fails

    ww_total, ww_fails = writer_wire_checks(con, tmp)
    total += ww_total
    fails += ww_fails
    dn_total, dn_fails = dense_domain_checks(con, tmp)
    total += dn_total
    fails += dn_fails
    sp_total, sp_fails = sql_pred_checks(con, tmp, fixture)
    total += sp_total
    fails += sp_fails

    print(f"\ndifferential: {total - fails}/{total} checks match (strict pyarrow read + DuckDB value diff + order)")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
