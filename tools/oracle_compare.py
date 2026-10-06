#!/usr/bin/env python3
"""How zpq's answers are compared with an oracle's (DuckDB, Hardwood, rows decoded by pyarrow).

One policy for tools/differential.py and tools/triangulate.py:

- NULL equals only NULL. sum/min/max/avg over no values is NULL on every engine, zpq included; count is never NULL.
- Integers compare exactly. That includes integral results an oracle widens to DECIMAL or HUGEINT (DuckDB's SUM of
  an integer column reaches Python as Decimal('6') through Arrow), so two large sums one apart never pass.
- DECIMAL compares exactly against DECIMAL. zpq evaluates DECIMAL aggregates and expressions in f64, so a result from
  that lane compares as a float: pass lane=F64 where the result type says so, since an integral f64 prints as an
  integer in zpq's JSON and cannot be told apart by its value.
- Floats compare with a relative tolerance (REL_TOL, absolute below 1.0): engines sum in different orders. NaN equals
  NaN and each infinity itself. Float min/max skip NaN in zpq, as in Arrow compute and Polars, while DuckDB orders NaN
  above every number, so DuckDB is asked for the NaN-free extreme (duckdb_agg).
- Booleans equal 1/0: zpq's aggregate JSON spells a BOOLEAN key or extreme as an integer.
- Temporals compare exactly, never with a tolerance: a filter literal finer than the column's unit must select
  exactly the rows that DuckDB's typed TIMESTAMP literal selects. Timezone-aware values compare as naive UTC.
- DuckDB is not an oracle where it trusts statistics the spec says not to (DUCKDB_STATS_DIVERGENCES); the truth
  there comes from rows decoded without statistics, or from Hardwood.

Run directly to check the comparator itself: .venv/bin/python tools/oracle_compare.py
"""
import datetime
import decimal
import math
from typing import Any, Optional

REL_TOL = 1e-4
# Significant digits a float keeps in canon(): ample for f32 (about 7), coarse enough to absorb summation order.
FLOAT_DIGITS = 6
# The result was computed in zpq's f64 lane, whatever type the oracle reports or the JSON number looks like.
F64 = "f64"

# Fixtures (repo paths) whose statistics DuckDB trusts although the spec says not to, so its filtered answers on them
# can be wrong. The fixtures come from tools/gen_stats_order_fixtures.py.
DUCKDB_STATS_DIVERGENCES = {
    "ci/fixtures/parquet/deprecated_stats.parquet":
        "DuckDB prunes on the deprecated min/max pair, which is signed-ordered, for columns whose order is not",
    "ci/fixtures/parquet/column_order.parquet":
        "DuckDB prunes on the bounds of a column whose declared column order it does not implement",
    "ci/fixtures/parquet/nan_stats.parquet":
        "DuckDB trusts float bounds that leave NaN out, both to drop and to keep NaN rows",
}


def _number(v: Any, lane: Optional[str]) -> Any:
    """v as None, int, Decimal (exact, non-integral), float (finite, non-integral) or a NaN/Infinity token; any other
    value unchanged."""
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float, decimal.Decimal)):
        if isinstance(v, decimal.Decimal) and not v.is_finite() or isinstance(v, float) and not math.isfinite(v):
            return "NaN" if v != v else ("Infinity" if v > 0 else "-Infinity")
        if lane == F64 and not isinstance(v, float):
            v = float(v)
        if isinstance(v, float):
            return int(v) if v.is_integer() and abs(v) < 2**53 else v
        if isinstance(v, decimal.Decimal) and v == v.to_integral_value():
            return int(v)
        return v
    if isinstance(v, datetime.datetime) and v.tzinfo is not None:
        return v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return v


def same(got: Any, want: Any, lane: Optional[str] = None) -> bool:
    """Whether zpq's `got` equals the oracle's `want` under the policy above."""
    g, w = _number(got, lane), _number(want, lane)
    if isinstance(g, float) or isinstance(w, float):
        if isinstance(g, (int, float, decimal.Decimal)) and isinstance(w, (int, float, decimal.Decimal)):
            g, w = float(g), float(w)
            return abs(g - w) <= REL_TOL * max(1.0, abs(w))
    if g is None or w is None:
        return g is None and w is None
    return g == w


def canon(v: Any, lane: Optional[str] = None) -> Any:
    """A form under which equal values compare == and sort together, for diffing row sets: same()'s rules, with floats
    rounded to FLOAT_DIGITS significant digits instead of compared with a tolerance. canon() sees one value at a time,
    so it cannot infer the f64 lane from the other side as same() does: a DECIMAL result that zpq answers in f64 needs
    lane=F64 on both sides."""
    n = _number(v, lane)
    if isinstance(n, float):
        r = float(f"{n:.{FLOAT_DIGITS}g}")
        return int(r) if r.is_integer() and abs(r) < 2**53 else r
    if isinstance(n, decimal.Decimal) and lane is None:
        return n.normalize()
    return n


def duckdb_agg(func: str, col: str, floating: bool) -> str:
    """DuckDB SQL for `func(col)` under zpq's semantics: float min/max over the NaN-free values."""
    nan_free = f' FILTER (WHERE NOT isnan("{col}"))' if floating and func in ("min", "max") else ""
    return f'{func}("{col}"){nan_free}'


def self_check() -> None:
    """The cases each rule exists for. Both harnesses run this first, so a loosened rule fails loudly."""
    D = decimal.Decimal
    big = 2**63 + 1
    cases = [
        (None, None, None, True),
        (0, None, None, False),          # an empty SUM is NULL, not 0
        (None, 0, None, False),
        ("", None, None, False),         # nor is an empty string
        (big, D(big - 1), None, False),  # adjacent large integers, one widened to DECIMAL by the oracle
        (big, D(big), None, True),
        (big, big - 1, None, False),
        (6, D("6"), None, True),
        (D("1.10"), D("1.1"), None, True),
        (D("12345678901234567.01"), D("12345678901234567.02"), None, False),  # DECIMAL vs DECIMAL is exact
        (1.1, D("1.10"), None, True),     # zpq's f64 lane vs the oracle's DECIMAL
        (12345678901234567168, D("12345678901234567123.00"), F64, True),
        (12345678901234567168, D("12345678901234567123.00"), None, False),
        (100.00001, 100.0, None, True),
        (100.1, 100.0, None, False),
        (float("nan"), "NaN", None, True),
        (float("inf"), float("inf"), None, True),
        (float("inf"), float("-inf"), None, False),
        (True, 1, None, True),
        (datetime.datetime(2020, 1, 1, 0, 0, 0, 99500), datetime.datetime(2020, 1, 1, 0, 0, 0, 99000), None, False),
        (datetime.datetime(2020, 1, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=1))),
         datetime.datetime(2020, 1, 1), None, True),
    ]
    for got, want, lane, expect in cases:
        assert same(got, want, lane) is expect, f"same({got!r}, {want!r}, {lane}) should be {expect}"
        if isinstance(got, float) and isinstance(want, decimal.Decimal) and lane is None:
            lane = F64  # see canon()
        assert (canon(got, lane) == canon(want, lane)) is expect, f"canon({got!r}) vs canon({want!r}), {lane}"


if __name__ == "__main__":
    self_check()
    print("oracle_compare: comparator cases hold")
