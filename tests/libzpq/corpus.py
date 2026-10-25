"""Corpus shared with the libzpq gate: the filter, projection, select, and aggregate matrices and their fixture.

The in-process referee replaced the runner that used to live in tools/differential.py. This module is what
tests/libzpq/differential.py still executes through the CLI, libzpq, and the Lambda.
"""
from decimal import Decimal
from fractions import Fraction

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
    # coalesce over any arguments: arithmetic first, three of them, DECIMAL, strings; a later argument evaluated only
    # where the earlier ones are NULL (`id + 1` is never NULL, so the division by zero never happens).
    ("coal_arith", "coalesce(amt * 2, id, 0) AS x", "coalesce(amt * 2, id, 0) AS x"),
    ("coal_dec",  "coalesce(price * CASE WHEN amt IS NOT NULL THEN 2 END, price + 1) AS x",
                  "coalesce(price * CASE WHEN amt IS NOT NULL THEN 2 END, price + 1) AS x"),
    ("coal_str",  "coalesce(nname || '_z', name, 'NA') AS x", "coalesce(nname || '_z', name, 'NA') AS x"),
    ("coal_lazy", "coalesce(id + 1, 100 / (id - id)) AS x", "id + 1 AS x"),
]


def avg_dec(total, n, scale):
    """zpq's avg over a DECIMAL as exact text: `total` (the exact sum, as text) over `n`, rounded half away from zero
    to `scale` digits. DuckDB's own avg is a DOUBLE, so main() registers this as the oracle's `zpq_avg`."""
    if not n:
        return None
    q = Fraction(Decimal(total)) / n * 10**scale
    m = (2 * abs(q.numerator) + q.denominator) // (2 * q.denominator)
    return f"{'-' if q < 0 else ''}{m}E-{scale}"


def duck_avg_dec(arg, p, s):
    """DuckDB SQL for zpq's avg over `arg`, a DECIMAL(p, s): exact, as DECIMAL(q, s + q - p), q = min(38, p + 4)."""
    q = min(38, p + 4)
    return f"CAST(zpq_avg(sum({arg})::VARCHAR, count({arg}), {s + q - p}) AS DECIMAL({q}, {s + q - p}))"


# (label, zpq_agg, duckdb_select) — filter × aggregate, compared as scalars.
AGGS = [
    ("sum_count", "sum(id) AS s, count(id) AS c", "sum(id), count(id)"),
    ("minmax",    "min(amt) AS mn, max(amt) AS mx", "min(amt), max(amt)"),
    ("decimal_sum", "sum(price) AS s", "sum(price)"),
    # count(*) alone needs no column at all in a row group statistics prove fully matching.
    ("count_star", "count(*) AS n, sum(amt) AS s", "count(*), sum(amt)"),
    ("avg",       "avg(amt) AS a, min(name) AS mn", "avg(amt), min(name)"),
    # avg over a DECIMAL is its exact mean at 4 more digits; avg_state is the pair it divides, sum then count.
    ("avg_dec",   "avg(price) AS a, avg_state(price) AS sp, avg_state(amt) AS sa",
                  f"{duck_avg_dec('price', 18, 2)}, sum(price), count(price), sum(amt), count(amt)"),
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
