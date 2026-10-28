"""Which reps a host disturbed. A disturbed case is re-run whole; after two re-runs it is quarantined, never scored.

The signature is a rep far slower than its cell's fastest: wall - fastest > max(1 s, 2 x fastest). Cold reps cause
their own I/O pressure, so the evidence is the *excess* over the fastest rep, not absolute PSI:

- the engine scope's io or memory `some` stall grew by at least half the excess wall time, and the rep read no more
  than 10% more bytes than the fastest (the engine did not do more I/O; the device got slower); or
- the host's io `full` stall grew by more than 1 s during the rep (stalled whatever its wall time).

A signature without that evidence is the engine's own slowness and is scored. Without PSI (an old record, a kernel
without it) the signature alone quarantines, and the lane cannot be certified.
"""

from collections import defaultdict

HOST_FULL_NS = 1_000_000_000


def psi_ns(rec, where, key):
    psi = (rec.get("psi") or {}).get(where)
    return None if psi is None else psi[key] * 1000


def stall_evidence(rec, fastest):
    """Why the host, not the engine, made `rec` slow; None when it did not."""
    full = psi_ns(rec, "host", "io_full")
    if full is not None and full > HOST_FULL_NS:
        return f"host io full +{full / 1e9:.2f}s"
    excess = rec["elapsed_ns"] - fastest["elapsed_ns"]
    if excess <= max(1e9, 2 * fastest["elapsed_ns"]):
        return None
    stalls = [psi_ns(r, "scope", k) for r in (rec, fastest) for k in ("io_some", "memory_some")]
    if None in stalls:
        return f"+{excess / 1e9:.2f}s over the fastest rep; no PSI to tell host from engine"
    grew = max(stalls[0] - stalls[2], stalls[1] - stalls[3])
    same_io = rec.get("read_bytes", 0) <= 1.1 * max(1, fastest.get("read_bytes", 0))
    if grew >= excess / 2 and same_io:
        return f"+{excess / 1e9:.2f}s over the fastest rep; scope stalled +{grew / 1e9:.2f}s, bytes read unchanged"
    return None


def cohorts(records, rnd):
    """{case: {engine: [timed records of round rnd]}}; rep 0 is a warm-up wherever it exists."""
    out = defaultdict(lambda: defaultdict(list))
    for r in records:
        if r["round"] == rnd and r["rep"] >= 1:
            out[r["case"]][r["engine"]].append(r)
    return out


def evidence(records, rnd):
    """{case: [(engine, rep, why)]} for every disturbed rep of round rnd."""
    out = defaultdict(list)
    for case, by_engine in cohorts(records, rnd).items():
        for engine, reps in by_engine.items():
            for r in reps:
                if r["status"] == "disturbed":
                    out[case].append((engine, r["rep"], ",".join(r.get("reasons", ["disturbed"]))))
            timed = [r for r in reps if r["status"] == "ok" and r.get("elapsed_ns") is not None]
            if not timed:
                continue
            fastest = min(timed, key=lambda r: r["elapsed_ns"])
            for r in timed:
                why = stall_evidence(r, fastest)
                if why:
                    out[case].append((engine, r["rep"], why))
    return out


def disturbed(records, rnd):
    return set(evidence(records, rnd))


def disturbed_last(records, last):
    """Cases disturbed in the last round that ran them."""
    return {case for rnd in set(last.values()) for case in evidence(records, rnd) if last[case] == rnd}


def latest_round(records):
    """{case: the last round that ran it}."""
    out = {}
    for r in records:
        out[r["case"]] = max(out.get(r["case"], 0), r["round"])
    return out


def quarantine(records, max_round=2):
    """{case: evidence} for cases still disturbed in their last round once no re-run remains."""
    last = latest_round(records)
    return {case: why for rnd in sorted(set(last.values())) if rnd >= max_round
            for case, why in evidence(records, rnd).items() if last[case] == rnd}
