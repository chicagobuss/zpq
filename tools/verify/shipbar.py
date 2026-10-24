"""Ship-bar scoring for a scorecard summary.

The boolean is the lead rule: beat DuckDB 2.0 and Polars 2.0 on a majority of
cells, at most 20% of cells behind the best rival, and no cell over 1.5x.
A same-host control engine is reported beside that boolean and does not change it.
"""

from __future__ import annotations

import collections
import math


def _geomean(ratios: list[float]) -> float:
    return math.exp(sum(math.log(r) for r in ratios) / len(ratios))


def evaluate(rows, zpq="zpq", rivals=("duckdb20", "polars"), exclude=(), control="zpq_prev"):
    rivals = tuple(rivals)
    excl = set(exclude)
    med = collections.defaultdict(dict)
    skipped = collections.Counter()
    for row in rows:
        value = row.get("median_ns") or row.get("legacy_median_ns")
        if value and not row.get("failures") and row.get("checked", 0) >= 1:
            med[(row["suite"], row["dataset"], row["query_id"])][row["engine"]] = value
    cells = []
    for key, engines in sorted(med.items()):
        if "/".join(key) in excl:
            skipped["excluded"] += 1
            continue
        if zpq not in engines:
            skipped["no valid zpq"] += 1
            continue
        have = [name for name in rivals if name in engines]
        if not have:
            skipped["no valid rival"] += 1
            continue
        best = min(engines[name] for name in have)
        cells.append((engines[zpq] / best, key, engines[zpq], {name: engines[name] for name in have}))
    if not cells:
        raise SystemExit("no comparable cells")
    behind = [cell for cell in cells if cell[0] > 1.0]
    beat_each = {name: sum(1 for cell in cells if name in cell[3] and cell[2] < cell[3][name]) for name in rivals}
    n_each = {name: sum(1 for cell in cells if name in cell[3]) for name in rivals}
    geomean = _geomean([cell[0] for cell in cells])
    met = (
        all(beat_each[name] > n_each[name] / 2 for name in rivals)
        and len(behind) <= 0.2 * len(cells)
        and not any(cell[0] > 1.5 for cell in cells)
    )
    control_ratios = []
    for engines in med.values():
        if zpq in engines and control in engines and engines[control] > 0:
            control_ratios.append(engines[zpq] / engines[control])
    return {
        "cells": cells,
        "n": len(cells),
        "skipped": dict(skipped),
        "geomean": geomean,
        "behind": len(behind),
        "beat_each": beat_each,
        "n_each": n_each,
        "met": met,
        "rivals": rivals,
        "zpq": zpq,
        "control": None
        if not control_ratios
        else {"engine": control, "cells": len(control_ratios), "geomean": _geomean(control_ratios)},
    }


def render(result, top=25) -> str:
    rivals = result["rivals"]
    n = result["n"]
    lines = [
        f"cells {n}  (skipped {result['skipped']})",
        (
            f"zpq vs best of {','.join(rivals)}: geomean {result['geomean']:.3f}x  "
            f"behind {result['behind']} ({100 * result['behind'] / n:.1f}%)  "
            f"behind >1.5x {sum(cell[0] > 1.5 for cell in result['cells'])}"
        ),
    ]
    for name in rivals:
        beat, total = result["beat_each"][name], result["n_each"][name]
        lines.append(f"  beats {name}: {beat}/{total} ({100 * beat / max(1, total):.1f}%)")
    lines.append(
        "SHIP BAR (majority vs each, <=20% behind, none >1.5x): " + ("MET" if result["met"] else "NOT MET")
    )
    control = result["control"]
    if control:
        lines.append(
            f"CONTROL {control['engine']}: {control['cells']} cells  geomean {control['geomean']:.3f}x "
            "(not part of the ship bar)"
        )
    else:
        lines.append("CONTROL: no same-host control rows in this summary")
    by = collections.defaultdict(list)
    for cell in result["cells"]:
        by[cell[1][0]].append(cell)
    for suite, cells in sorted(by.items()):
        lines.append(
            f"  {suite:12s} n={len(cells):3d} geomean {_geomean([c[0] for c in cells]):.3f}  "
            f"behind {sum(c[0] > 1 for c in cells):3d}  >1.5x {sum(c[0] > 1.5 for c in cells):3d}  "
            f"worst {max(c[0] for c in cells):.2f}x"
        )
    lines.append(f"worst {top} (ratio, cell, zpq ms, rivals ms):")
    for cell in sorted(result["cells"], reverse=True)[:top]:
        rivals_ms = " ".join(f"{name} {value / 1e6:.1f}" for name, value in cell[3].items())
        lines.append(f"  {cell[0]:5.2f}x {'/'.join(cell[1])}  zpq {cell[2] / 1e6:.1f}  {rivals_ms}")
    return "\n".join(lines) + "\n"
