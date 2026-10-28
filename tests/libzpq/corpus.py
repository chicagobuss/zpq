"""The checker's declared corpus (tools/check/cases), reused for CLI/libzpq/Lambda byte parity."""

import json
from pathlib import Path


CASES = Path(__file__).resolve().parents[2] / "tools/check/cases"
_projection = json.loads((CASES / "filter_projection.json").read_text())
_plain, _selects = _projection["matrices"]
_aggregates = json.loads((CASES / "agg.json").read_text())["matrices"][0]

FILTERS = [(name, f.get("filter"), f.get("where")) for name, f in _plain["vars"]["f"].items()
           if "all" in _plain["cells"].get(name, [])]
PROJECTIONS = [(name, p.get("columns")) for name, p in _plain["vars"]["p"].items()
               if name in _plain["cells"].get("none", [])]
SELECTS = [(name, s["select"], s["duck_select"]) for name, s in _selects["vars"]["s"].items()
           if name in _selects["cells"].get("none", [])]
AGGS = [(s["shape"], s["aggregate"], s["head"]) for key, s in _aggregates["vars"]["a"].items()
        if s["prefix"] == "agg" and "none" in _aggregates["cells"].get(key, [])]


def gen_fixture(con, path):
    copy = next(f["copy"] for f in _projection["fixtures"] if f["name"] == "main")
    con.execute(copy.format(path=path))
