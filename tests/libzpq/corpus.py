"""The referee's declared corpus, reused for CLI/libzpq/Lambda byte parity."""

import json
from pathlib import Path


CASES = Path(__file__).resolve().parents[2] / "tools/runner/cases"
_projection = json.loads((CASES / "filter_projection.json").read_text())
_aggregates = json.loads((CASES / "agg.json").read_text())

FILTERS = [(c["id"].split("/")[0], c.get("filter"), c.get("where"))
           for c in _projection["cases"] if c["id"].endswith("/all")]
PROJECTIONS = [(c["id"].split("/")[1], c.get("columns"))
               for c in _projection["cases"] if c["id"].startswith("none/")]
SELECTS = [(c["id"].split("/")[0].removeprefix("select:"), c["select"], c["duckdb_select"])
           for c in _projection["cases"] if c["id"].startswith("select:") and c["id"].endswith("/none")]
AGGS = [(c["id"].split(":")[1], c["aggregate"], c["duck"])
        for c in _aggregates["cases"] if c["id"].startswith("agg:") and c["id"].endswith(":none")]


def gen_fixture(con, path):
    con.execute(_projection["fixture_copy"].format(path=path))
