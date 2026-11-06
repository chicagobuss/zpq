"""Compare zpq NDJSON rows with DuckDB CSV rows for the same SQL: same row count and order, keys exact, numbers to
1e-9 relative. usage: compare.py duck.csv zpq.jsonl"""
import csv, json, sys
duck = list(csv.DictReader(open(sys.argv[1])))
zpq = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
ok = len(duck) == len(zpq)
for d, z in zip(duck, zpq):
    for k, dv in d.items():
        zv = z.get(k)
        a, b = num(dv), num(zv)
        if a is not None and b is not None:
            if abs(a - b) > 1e-9 * max(1.0, abs(a)):
                ok = False
        elif str(dv) != str(zv):
            # DuckDB prints DATE as ISO text, zpq as days since the epoch.
            import datetime
            try:
                if (datetime.date.fromisoformat(dv) - datetime.date(1970, 1, 1)).days == int(zv):
                    continue
            except Exception:
                pass
            ok = False
print("MATCH" if ok else f"DIFF duck={duck[:2]} zpq={zpq[:2]} n={len(duck)}/{len(zpq)}")
