#!/usr/bin/env bash
# tools/duckdb_smoke.sh
#
# Cross-impl validation, foreign-writer → ZPQ direction: DuckDB writes a
# parquet file (int / double / decimal / string), ZPQ reads + filters +
# aggregates it, and we assert ZPQ's answers match DuckDB's own. This is the
# complement to `tools/cli_smoke.sh` (ZPQ writes → pyarrow reads) and catches
# decode bugs a ZPQ-vs-ZPQ round-trip can't (the self-consistency blind spot).
# Needs the `duckdb` CLI + python3 (stdlib only).
set -uo pipefail

ZPQ=${ZPQ:-./zig-out/bin/zpq}
DUCKDB=${DUCKDB:-duckdb}

command -v "$DUCKDB" >/dev/null 2>&1 || { echo "duckdb CLI not found on PATH" >&2; exit 2; }
[[ -x "$ZPQ" ]] || { echo "zpq not built at $ZPQ (run: zig build -Doptimize=ReleaseFast)" >&2; exit 2; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
F="$TMP/dd.parquet"

# Foreign writer: DuckDB emits the fixture (1000 rows, mixed types incl. DECIMAL).
if ! "$DUCKDB" -c "COPY (
  SELECT i::INTEGER AS id,
         (i * 1.5)::DOUBLE AS amt,
         ('row' || i) AS name,
         CAST(i AS DECIMAL(12,2)) AS price,
         (DATE '2020-01-01' + i::INTEGER) AS d,
         (TIMESTAMP '2020-01-01 00:00:00' + (i::INTEGER * INTERVAL 1 DAY)) AS ts,
         (i % 3 = 0) AS flag,
         (CASE WHEN i % 7 = 0 THEN NULL ELSE 'n' || i END) AS nname
  FROM range(1, 1001) t(i)
) TO '$F' (FORMAT PARQUET)"; then
  echo "duckdb failed to write the fixture" >&2
  exit 2
fi

fail=0
# compare <desc> <zpq-json> <duckdb-csv>: ZPQ agg values (alias order) vs
# DuckDB SELECT columns (CSV order). Keep the two orders aligned per call.
compare() {
  if python3 - "$1" "$2" "$3" <<'PY'
import sys, json
desc, zjson, dcsv = sys.argv[1], sys.argv[2], sys.argv[3]
zvals = list(json.loads(zjson)["agg"].values())
dvals = dcsv.strip().split(",")
ok = len(zvals) == len(dvals)
for zv, dv in zip(zvals, dvals):
    try:
        ok = ok and abs(float(zv) - float(dv)) <= 1e-6 * max(1.0, abs(float(dv)))
    except ValueError:
        ok = ok and str(zv) == str(dv)
print(("  OK   " if ok else "  FAIL ") + desc + f"   zpq={zvals} duck={dvals}")
sys.exit(0 if ok else 1)
PY
  then :; else fail=1; fi
}

echo "duckdb cross-impl smoke ($("$DUCKDB" --version | awk '{print $1}') writes, ZPQ reads):"

compare "int/double/decimal aggregates" \
  "$("$ZPQ" query "$F" --aggregate 'sum(id) AS sid, min(amt) AS mn, max(amt) AS mx, sum(price) AS sp, count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT sum(id), min(amt), max(amt), sum(price), count(id) FROM '$F'")"

compare "filter on int (id > 500)" \
  "$("$ZPQ" query "$F" --filter 'id > 500' --aggregate 'count(id) AS c, sum(price) AS sp' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id), sum(price) FROM '$F' WHERE id > 500")"

compare "filter on double (amt < 150.0)" \
  "$("$ZPQ" query "$F" --filter 'amt < 150.0' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE amt < 150.0")"

compare "filter on string equality (name = row42)" \
  "$("$ZPQ" query "$F" --filter "name = 'row42'" --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE name = 'row42'")"

compare "string min/max (bytewise/unsigned)" \
  "$("$ZPQ" query "$F" --aggregate 'min(name) AS lo, max(name) AS hi' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT min(name), max(name) FROM '$F'")"

compare "DATE literal filter (d >= 2022-01-01)" \
  "$("$ZPQ" query "$F" --filter "d >= '2022-01-01'" --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE d >= DATE '2022-01-01'")"

compare "TIMESTAMP literal filter (ts < 2021-01-01 00:00:00)" \
  "$("$ZPQ" query "$F" --filter "ts < '2021-01-01 00:00:00'" --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE ts < TIMESTAMP '2021-01-01 00:00:00'")"

compare "bool equality filter (flag = true)" \
  "$("$ZPQ" query "$F" --filter 'flag = true' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE flag = true")"

compare "bool ordering filter (flag > false)" \
  "$("$ZPQ" query "$F" --filter 'flag > false' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE flag > false")"

compare "bool aggregates (sum/min/max/count flag)" \
  "$("$ZPQ" query "$F" --aggregate 'sum(flag) AS s, min(flag) AS mn, max(flag) AS mx, count(flag) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT sum(flag::INTEGER), min(flag::INTEGER), max(flag::INTEGER), count(flag) FROM '$F'")"

compare "IN list (id IN (1,5,999))" \
  "$("$ZPQ" query "$F" --filter 'id IN (1, 5, 999)' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE id IN (1, 5, 999)")"

compare "string IN list (name IN (row1,row500))" \
  "$("$ZPQ" query "$F" --filter "name IN ('row1', 'row500')" --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE name IN ('row1', 'row500')")"

compare "NOT IN list (id NOT IN (1,2,3))" \
  "$("$ZPQ" query "$F" --filter 'id NOT IN (1, 2, 3)' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE id NOT IN (1, 2, 3)")"

compare "NOT comparison (NOT id > 990)" \
  "$("$ZPQ" query "$F" --filter 'NOT id > 990' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE NOT id > 990")"

# INT96 (legacy Spark/Impala timestamp) — DuckDB can't WRITE int96, so we
# validate the decode against a Spark-written corpus fixture if it's present
# (fetched by `just fetch-corpus` / `just gauntlet`).
I96="data/parquet-testing/data/alltypes_plain.parquet"
if [[ -f "$I96" ]]; then
  compare "INT96 timestamp filter (corpus: alltypes_plain)" \
    "$("$ZPQ" query "$I96" --filter "timestamp_col >= '2009-04-01'" --aggregate 'count(timestamp_col) AS c' 2>/dev/null)" \
    "$("$DUCKDB" -noheader -csv -c "SELECT count(timestamp_col) FROM '$I96' WHERE timestamp_col >= TIMESTAMP '2009-04-01'")"
else
  echo "  SKIP  INT96 (corpus absent — run 'just fetch-corpus' or 'just gauntlet')"
fi

# --scan-all parity: forcing a full decode (every stats shortcut off) must
# return IDENTICAL answers to the default stats-fast path. Guards the invariant
# that the single flag disables shortcuts without changing results — and that
# no future shortcut silently diverges from the decode path. Self-consistency
# (ZPQ vs ZPQ), so it runs even when stats are honest (as here).
scan_all_parity() {
  local desc="$1"; shift
  local def saw
  def=$("$ZPQ" query "$F" "$@" 2>/dev/null)
  saw=$("$ZPQ" query "$F" "$@" --scan-all 2>/dev/null)
  if python3 - "$desc" "$def" "$saw" <<'PY'
import sys, json
desc, a, b = sys.argv[1:4]
try:
    aa, bb = json.loads(a)["agg"], json.loads(b)["agg"]
except Exception as e:
    print(f"  FAIL  {desc}   (bad json: {e})"); sys.exit(1)
ok = aa == bb
print(("  OK   " if ok else "  FAIL ") + desc + f"   default={aa} scan-all={bb}")
sys.exit(0 if ok else 1)
PY
  then :; else fail=1; fi
}

compare "IS NULL (nname, 1000/7 nulls)" \
  "$("$ZPQ" query "$F" --filter 'nname IS NULL' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE nname IS NULL")"

compare "IS NOT NULL (nname)" \
  "$("$ZPQ" query "$F" --filter 'nname IS NOT NULL' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE nname IS NOT NULL")"

compare "IS NOT NULL composed with AND" \
  "$("$ZPQ" query "$F" --filter 'nname IS NOT NULL AND id > 500' --aggregate 'count(id) AS c' 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE nname IS NOT NULL AND id > 500")"

# LIKE — one per classified kind (prefix/suffix/contains/underscore/general) + NOT.
for pat_desc in \
  "prefix|name LIKE 'row1%'|name LIKE 'row1%'" \
  "suffix|name LIKE '%9'|name LIKE '%9'" \
  "contains|name LIKE '%99%'|name LIKE '%99%'" \
  "underscore|name LIKE 'row_'|name LIKE 'row_'" \
  "general|name LIKE 'r%w_0'|name LIKE 'r%w_0'" \
  "not-like|name NOT LIKE 'row1%'|name NOT LIKE 'row1%'"; do
  IFS='|' read -r d zf df <<< "$pat_desc"
  compare "LIKE $d ($zf)" \
    "$("$ZPQ" query "$F" --filter "$zf" --aggregate 'count(id) AS c' 2>/dev/null)" \
    "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE $df")"
done

# SQL frontend (--query) end-to-end: the parse→lower→engine round-trip must
# MATCH DuckDB, not just emit a plausible string. Exercises projection-to-
# aggregate routing + WHERE lowering through the operators (LIKE, IS NULL).
compare "SQL: aggregate + WHERE" \
  "$("$ZPQ" query --query "SELECT count(id) AS c, sum(price) AS sp FROM '$F' WHERE id > 500" 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id), sum(price) FROM '$F' WHERE id > 500")"
compare "SQL: WHERE LIKE round-trip" \
  "$("$ZPQ" query --query "SELECT count(id) AS c FROM '$F' WHERE name LIKE 'row1%'" 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE name LIKE 'row1%'")"
compare "SQL: WHERE IS NOT NULL round-trip" \
  "$("$ZPQ" query --query "SELECT count(id) AS c FROM '$F' WHERE nname IS NOT NULL" 2>/dev/null)" \
  "$("$DUCKDB" -noheader -csv -c "SELECT count(id) FROM '$F' WHERE nname IS NOT NULL")"

scan_all_parity "scan-all == default: RG-prune filter (id < 100)" \
  --filter 'id < 100' --aggregate 'count(id) AS c, sum(amt) AS s'

# ── C4.x stats-as-answer coverage matrix ──────────────────────────────────
# The single source of truth for "what short-circuits from row-group stats vs
# what decodes." Per (agg × column) it asserts the stats path agrees with the
# full-decode path (ZPQ default == ZPQ --scan-all — the C4.x correctness
# guarantee) and prints the verdict: `stats` when the column was answered from
# metadata and dropped from the scan (cols_stat_pruned > 0), else `decode`.
# Verdict uses cols_stat_pruned (not decode_ms) so it's timing-independent on
# this tiny fixture. Update this grid when the short-circuit boundary moves.
echo "C4.x stats-as-answer matrix (cell = default==scan-all OK + [stats|decode] verdict):"
mrow() {
  local label="$1" agg="$2"
  local zdef zsaw
  zdef=$("$ZPQ" query "$F" --aggregate "$agg AS r" 2>/dev/null)
  zsaw=$("$ZPQ" query "$F" --aggregate "$agg AS r" --scan-all 2>/dev/null)
  if python3 - "$label" "$zdef" "$zsaw" <<'PY'
import sys, json
label, zdef, zsaw = sys.argv[1:4]
try:
    a, b = json.loads(zdef), json.loads(zsaw)
except Exception as e:
    print(f"  FAIL  {label:11} (bad json: {e})"); sys.exit(1)
def norm(xs): return [round(float(x), 4) if isinstance(x, (int, float)) else str(x) for x in xs]
agree = norm(list(a["agg"].values())) == norm(list(b["agg"].values()))
verdict = "stats " if a.get("cols_stat_pruned", 0) > 0 else "decode"
print(("  OK   " if agree else "  FAIL ") + f"{label:11} [{verdict}] = {list(a['agg'].values())}")
sys.exit(0 if agree else 1)
PY
  then :; else fail=1; fi
}
#     label          aggregate          (type)
mrow "count(id)"   "count(id)"   # INT32
mrow "min(id)"     "min(id)"
mrow "max(id)"     "max(id)"
mrow "sum(id)"     "sum(id)"
mrow "min(amt)"    "min(amt)"    # DOUBLE
mrow "max(amt)"    "max(amt)"
mrow "sum(amt)"    "sum(amt)"
mrow "avg(amt)"    "avg(amt)"
mrow "min(price)"  "min(price)"  # DECIMAL
mrow "max(price)"  "max(price)"
mrow "sum(price)"  "sum(price)"
mrow "min(name)"   "min(name)"   # STRING
mrow "max(name)"   "max(name)"
mrow "min(d)"      "min(d)"      # DATE
mrow "max(d)"      "max(d)"
mrow "min(ts)"     "min(ts)"     # TIMESTAMP
mrow "max(ts)"     "max(ts)"
mrow "sum(flag)"   "sum(flag)"   # BOOLEAN
mrow "min(flag)"   "min(flag)"
mrow "max(flag)"   "max(flag)"

if [[ "$fail" -eq 0 ]]; then
  echo "duckdb cross-impl smoke: PASS"
else
  echo "duckdb cross-impl smoke: FAIL" >&2
fi
exit "$fail"
