"""Choose scorecard suites and cases before plan.

A suite with a missing input is skipped whole. The suites that resolve are
planned together. The skip is recorded; it is not a silent drop and it does
not discard the suites that can run.
"""

from __future__ import annotations

import glob
import os

PROFILE_SUITES = {
    "cold": ["clickbench", "study", "pdsh", "reads-sf1", "writes", "votes-local"],
    "in-process": ["reads-sf1", "clickbench", "study"],
}


def declared_suites(profile: str, suites_arg: str | None) -> tuple[list[str], list[str]]:
    if profile not in PROFILE_SUITES:
        raise ValueError(f"unknown profile {profile}")
    if not suites_arg:
        return list(PROFILE_SUITES[profile]), []
    names = [part.strip() for part in suites_arg.split(",") if part.strip()]
    unknown = [name for name in names if name not in PROFILE_SUITES[profile]]
    return names, unknown


def case_id(case: dict) -> str:
    return f"{case['suite']}/{case['dataset']}/{case['query_id']}"


def token_matches(case: dict, token: str) -> bool:
    parts = token.split("/")
    if len(parts) == 1:
        return case["query_id"] == token
    if len(parts) == 2:
        return case["suite"] == parts[0] and case["query_id"] == parts[1]
    if len(parts) == 3:
        return (case["suite"], case["dataset"], str(case["query_id"])) == tuple(parts)
    return False


def input_gaps(case: dict) -> list[str]:
    missing = []
    for pattern in case.get("inputs") or []:
        if not glob.glob(os.path.expanduser(str(pattern))):
            missing.append(str(pattern))
    for field in ("truth_database", "truth_meta"):
        path = case.get(field)
        if path and not os.path.isfile(os.path.expanduser(str(path))):
            missing.append(str(path))
    return missing


def _unique(items: list[str]) -> list[str]:
    seen = set()
    ordered = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def relevant_tokens(suite: str, tokens: list[str]) -> list[str]:
    relevant = []
    for token in tokens:
        head = token.split("/", 1)[0]
        if "/" not in token or head == suite:
            relevant.append(token)
    return relevant


def classify_built(suite: str, cases: list[dict], tokens: list[str] | None) -> dict:
    """Decide whether one built suite runs. `tokens` None means every case."""
    missing = _unique([gap for case in cases for gap in input_gaps(case)])
    if missing:
        return {"suite": suite, "status": "skipped", "reason": "missing inputs", "missing": missing}
    if not cases:
        return {"suite": suite, "status": "skipped", "reason": "suite produced no cases"}
    if tokens is None:
        return {"suite": suite, "status": "ran", "cases": len(cases), "queries": [case_id(c) for c in cases]}
    relevant = relevant_tokens(suite, tokens)
    if not relevant:
        return {"suite": suite, "status": "skipped", "reason": "excluded by case filter"}
    unmatched = [token for token in relevant if not any(token_matches(case, token) for case in cases)]
    if unmatched:
        return {
            "suite": suite,
            "status": "skipped",
            "reason": "case filter matched nothing",
            "unmatched": unmatched,
        }
    selected = [case for case in cases if any(token_matches(case, token) for token in relevant)]
    return {"suite": suite, "status": "ran", "cases": len(selected), "queries": [case_id(c) for c in selected]}


def selected_ids(cases: list[dict], tokens: list[str] | None) -> list[str]:
    if tokens is None:
        return [case_id(case) for case in cases]
    relevant = relevant_tokens(cases[0]["suite"], tokens) if cases else []
    return [case_id(case) for case in cases if any(token_matches(case, token) for token in relevant)]
