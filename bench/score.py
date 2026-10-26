"""A lane's score: per case and engine the median of its checked reps, the ship bar, the control, the verdict.

Ship bar (the lead's rule): zpq beats DuckDB 2.0 and Polars 2.0 each on a majority of cases, at most 20% of cases
behind the best rival, and none more than 1.5x behind. The denominator is every planned case:

- a zpq failure (error, timeout, wrong answer) is behind and over 1.5x: a failed cell, never a skipped one;
- a rival's failure counts as zpq beating that rival, when zpq succeeded;
- a quarantined case is neither a win nor behind, and stays in the denominator.

Verdict: COMPLETE; INCOMPLETE when a planned cell has no final result or more than 2% of cases are quarantined;
UNCERTIFIED when the host gave no PSI, so a stall could not be told from the engine, or for a smoke run. The
control (`zpq_prev`) is reported beside the bar and never changes it.
"""

import math
import statistics
from collections import defaultdict

from bench import stalls

RIVALS = ("duckdb20", "polars")
QUARANTINE_LIMIT = 0.02


def geomean(xs):
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def cells(records, reps, max_round=2):
    """({case: {engine: {"median_ns"|"failed"}}}, quarantined {case: evidence}, pending [case])."""
    last = stalls.latest_round(records)
    quarantined = stalls.quarantine(records, max_round)
    pending = sorted(c for c in stalls.disturbed_last(records, last) if c not in quarantined)
    by = defaultdict(lambda: defaultdict(list))
    for r in records:
        if r["round"] == last[r["case"]] and r["rep"] >= 1:
            by[r["case"]][r["engine"]].append(r)
    out = {}
    for case, engines in by.items():
        out[case] = {}
        for engine, reps_ in engines.items():
            bad = [r for r in reps_ if r["status"] in ("failed", "wrong", "timeout")]
            ok = [r["elapsed_ns"] for r in reps_ if r["status"] == "ok"]
            if bad:
                out[case][engine] = {"failed": bad[0]["status"] + ": " + (bad[0].get("reason") or bad[0].get("check")
                                                                          or "")[:160]}
            elif len(ok) == reps:
                out[case][engine] = {"median_ns": statistics.median(ok), "min_ns": min(ok), "max_ns": max(ok)}
            else:
                out[case][engine] = {"incomplete": f"{len(ok)}/{reps} reps checked"}
    return out, quarantined, pending


def ship_bar(table, planned, quarantined, zpq="zpq", rivals=RIVALS):
    n = len(planned)
    beats, behind, over, ratios, failed = {r: 0 for r in rivals}, 0, 0, [], []
    for case in planned:
        if case in quarantined:
            continue
        row = table.get(case, {})
        z = row.get(zpq, {})
        if "median_ns" not in z:
            failed.append(case)
            behind, over = behind + 1, over + 1
            continue
        times = {r: row[r]["median_ns"] for r in rivals if "median_ns" in row.get(r, {})}
        for r in rivals:
            beats[r] += r not in times or z["median_ns"] < times[r]
        if times:
            ratio = z["median_ns"] / min(times.values())
            ratios.append((ratio, case))
            behind += ratio > 1.0
            over += ratio > 1.5
    met = all(beats[r] > n / 2 for r in rivals) and behind <= 0.2 * n and over == 0
    return {"cases": n, "quarantined": len(quarantined), "beats": beats, "behind": behind, "over_1_5x": over,
            "zpq_failed": failed, "geomean": geomean([x for x, _ in ratios]), "met": met,
            "worst": sorted(ratios, reverse=True)[:25]}


def control(table, zpq="zpq", prev="zpq_prev"):
    xs = [row[zpq]["median_ns"] / row[prev]["median_ns"] for row in table.values()
          if "median_ns" in row.get(zpq, {}) and "median_ns" in row.get(prev, {})]
    return {"engine": prev, "cases": len(xs), "geomean": geomean(xs)} if xs else None


def score(header, records, planned, max_round=2):
    table, quarantined, pending = cells(records, header["reps"], max_round)
    missing = sorted(c for c in planned if c not in table)
    share = len(quarantined) / max(1, len(planned))
    if missing or pending or share > QUARANTINE_LIMIT:
        verdict = "INCOMPLETE"
    elif not header.get("psi") or header.get("smoke"):
        verdict = "UNCERTIFIED"
    else:
        verdict = "COMPLETE"
    return {"lane": header["lane"], "host": header["host"], "sha": header["sha"], "verdict": verdict,
            "quarantine_share": share, "quarantined": quarantined, "pending_rerun": pending, "missing": missing,
            "psi": header.get("psi"), "ship_bar": ship_bar(table, planned, quarantined), "control": control(table),
            "cells": table}


def render(s):
    bar = s["ship_bar"]
    n = bar["cases"]
    lines = [f"{s['host']} {s['lane']} lane at {s['sha'][:12]}: {s['verdict']}"
             + ("" if s["psi"] else " (no PSI: stalls cannot be told from the engine)"),
             f"  cases {n}; quarantined {len(s['quarantined'])} ({100 * s['quarantine_share']:.1f}%, limit "
             f"{100 * QUARANTINE_LIMIT:.0f}%); pending re-run {len(s['pending_rerun'])}; missing {len(s['missing'])}"]
    for r, k in bar["beats"].items():
        lines.append(f"  zpq beats {r}: {k}/{n}")
    lines.append(f"  behind the best rival {bar['behind']}/{n}; over 1.5x {bar['over_1_5x']}; zpq failed "
                 f"{len(bar['zpq_failed'])}; geomean {bar['geomean'] or float('nan'):.3f}")
    lines.append("  SHIP BAR (majority vs each, <=20% behind, none >1.5x, no zpq failure): "
                 + ("MET" if bar["met"] else "NOT MET"))
    c = s["control"]
    lines.append(f"  control {c['engine']}: {c['cases']} cases, geomean {c['geomean']:.3f}x (not in the bar)" if c
                 else "  control: none")
    for case in bar["zpq_failed"]:
        lines.append(f"  zpq failed {case}: {s['cells'].get(case, {}).get('zpq', {'failed': 'no result'})}")
    for case, why in sorted(s["quarantined"].items()):
        lines.append(f"  quarantined {case}: " + "; ".join(f"{e} rep {rep}: {w}" for e, rep, w in why[:4]))
    lines.append("  worst ratios: " + ", ".join(f"{case} {x:.2f}x" for x, case in bar["worst"][:10]))
    return "\n".join(lines) + "\n"
