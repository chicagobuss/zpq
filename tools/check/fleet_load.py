"""The fleet's builder probe (tools/check/fleet.sh): a host's state from the o11y feed.

    fleet_load.py O11Y_JSON HOST TARGET_GB   prints "offline", "busy", or "idle N" (N target builds its free memory holds)
    fleet_load.py --self-test

A host the feed omits, or reports as anything but live, is offline; so is one whose load or memory fields are null,
as the feed reports a host it cannot reach. A builder must be idle: half its cores free and room for a target build."""
import json
import sys


def classify(h, need):
    if not h or h.get("state") != "live":
        return "offline"
    total, avail, load1, cores = (h.get(k) for k in ("mem_total", "mem_avail_pct", "load1", "cores"))
    if None in (total, avail, load1) or not cores:
        return "offline"
    free = total * avail / 100
    return "idle %d" % (free // need) if load1 < 0.5 * cores and free >= need else "busy"


def self_test():
    gib = 1 << 30
    live = {"state": "live", "mem_total": 64 * gib, "mem_avail_pct": 50, "load1": 1.0, "cores": 32}
    cases = [
        (None, "offline"),
        ({"state": "offline", "mem_total": None, "mem_avail_pct": None, "load1": None, "cores": 0}, "offline"),
        ({**live, "mem_total": None}, "offline"),  # live but unreadable: never a builder, and no crash
        ({**live, "load1": None}, "offline"),
        (live, "idle 1"),
        ({**live, "load1": 20.0}, "busy"),
        ({**live, "mem_avail_pct": 10}, "busy"),
    ]
    for h, want in cases:
        got = classify(h, 20 * gib)
        assert got == want, (h, got, want)
    print(f"fleet_load: {len(cases)} self-test cases pass")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        path, host, gb = sys.argv[1:]
        hosts = {x["host"]: x for x in json.load(open(path)).get("hosts", [])}
        print(classify(hosts.get(host), int(gb) << 30))
