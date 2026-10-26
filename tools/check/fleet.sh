#!/usr/bin/env bash
# Run full-tier work on fleet hosts and collect the receipts for one verdict.
#
#   tools/check/fleet.sh [--host H]... [--release] [--shard NAME]... [--inventory FILE]
#
# The fleet has no Zig. This host builds each host target's bundle at HEAD (a clean tree: receipts name this SHA),
# ships it over ssh, runs the unit shards there through `zpq-check stage`, and runs every case answers-only (no oracle,
# no PyArrow) for the architecture check: each host's answers manifest must equal this host's, case for case. Shards
# that need a toolchain or local data run here, one after another. Receipts land in zig-out/receipts.
#
# Placement: hosts come from the inventory (ZPQ_FLEET, default the arch matrix's fleet.json). granted-mbp runs only
# when named with --host or under --release (use the Mac sparingly). A host that is timing (an active zpq-* unit, a
# held timing lock, a running full gate) is skipped, never shared. Larger shards go first, each to the least-loaded
# host.
set -euo pipefail
cd "$(dirname "$0")/../.."
inventory=${ZPQ_FLEET:-/mnt/fast/zpq-out/arch-matrix/fleet.json}
hosts=() shards=() release=0
while [[ $# -gt 0 ]]; do
  case $1 in
    --host) hosts+=("$2"); shift 2 ;;
    --shard) shards+=("$2"); shift 2 ;;
    --release) release=1; shift ;;
    --inventory) inventory=$2; shift 2 ;;
    *) echo "fleet: unknown argument $1" >&2; exit 2 ;;
  esac
done
if [[ -n $(git status --porcelain --untracked-files=no) ]]; then
  echo "fleet: dirty tree; receipts would be unofficial" >&2
  exit 2
fi
sha=$(git rev-parse HEAD)
py=$(command -v "${PYTHON:-$PWD/.venv/bin/python}")
# q field HOST KEY DEFAULT | q stages SHARD | q minutes SHARD | q shards | q largest-first SHARD...
q() {
  "$py" - "$inventory" "$@" <<'PY'
import json, sys
inventory, cmd, args = sys.argv[1], sys.argv[2], sys.argv[3:]
shards = json.load(open("tools/check/shards.json"))["shards"]
if cmd == "field":
    print(json.load(open(inventory))["hosts"].get(args[0], {}).get(args[1], args[2]))
elif cmd == "stages":
    print(" ".join(shards[args[0]]["stages"]))
elif cmd == "minutes":
    print(shards[args[0]]["minutes"])
elif cmd == "shards":
    print("\n".join(shards))
elif cmd == "largest-first":
    print(" ".join(sorted(args, key=lambda k: -shards[k]["minutes"])))
PY
}
field() { q field "$@"; }
shard() { q "$2" "$1"; }

if [[ ${#hosts[@]} -eq 0 ]]; then
  mapfile -t hosts < <("$py" - "$inventory" "$release" "$(hostname -s)" <<'PY'
import json, sys
inv, release, me = json.load(open(sys.argv[1])), sys.argv[2] == "1", sys.argv[3]
for name, h in inv["hosts"].items():
    if h.get("enabled", True) and name != me and (release or name != "granted-mbp"):
        print(name)
PY
  )
fi
busy() {
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$1" '
    systemctl --user list-units --state=active --no-legend "zpq-*" 2>/dev/null | grep -q . && exit 0
    for lock in /mnt/fast/zpq-out/timing.lock "$HOME/.cache/zpq-bench.lock"; do  # the second is bench/'s
      [ -e "$lock" ] && command -v flock >/dev/null && ! flock -n "$lock" true && exit 0
    done
    pgrep -f "[v]erify.py full|[D]tier=full" >/dev/null && exit 0  # bracketed: never this shell
    exit 1' 2>/dev/null
}
free=()
for h in "${hosts[@]}"; do
  if busy "$h"; then echo "fleet: $h is timing; skipped"; else free+=("$h"); fi
done
[[ ${#shards[@]} -ne 0 ]] || mapfile -t shards < <(q shards)

# This host's answers, the reference every other host's manifest must equal.
cases=(); for f in tools/check/cases/*.json; do [[ $f == *clickbench.json ]] || cases+=(--cases "$f"); done
zig build check-runner -Doptimize=ReleaseSafe -j8 >/dev/null
zig build -Doptimize=ReleaseFast -j8 >/dev/null
mkdir -p zig-out/receipts
zig-out/bin/zpq-check "${cases[@]}" --work zig-out/fleet-work --python "$py" --answers-only \
  >/dev/null  # makes the fixtures every host reuses

# One bundle per target: the unit-test binaries (Debug, as the full tier runs them), the runner and CLI, the case
# files, the fixtures the reference run made, and the unit tests' data. No raw syscall may reach a macOS binary.
declare -A bundle
for h in "${free[@]}"; do
  t=$(field "$h" target x86_64-linux-musl)
  [[ -n ${bundle[$t]:-} ]] && continue
  b=zig-out/fleet-bundle/$t
  rm -rf "$b" && mkdir -p "$b/data"
  zig build test-bin -Dtarget="$t" -j8 --prefix "$b/zig-out" >/dev/null
  zig build check-runner -Dtarget="$t" -Doptimize=ReleaseSafe -j8 --prefix "$b/zig-out" >/dev/null
  zig build cli -Dtarget="$t" -Doptimize=ReleaseFast -j8 --prefix "$b/zig-out" >/dev/null  # as the reference ran
  if [[ $t == *macos ]] && command -v llvm-objdump >/dev/null; then
    for f in "$b"/zig-out/bin/*; do
      ! llvm-objdump -d "$f" | grep -qE '\s(svc|syscall)\s' || { echo "fleet: raw syscall in $f" >&2; exit 1; }
    done
  fi
  cp -rL ci packaging tools "$b/"
  cp -rL data/*.parquet data/parquet-testing "$b/data/"
  mkdir -p "$b/zig-out/fleet-work" && cp -r zig-out/fleet-work/. "$b/zig-out/fleet-work/"
  bundle[$t]=$b
done

# The reference answers: the x86_64 Linux bundle, as it ships (baseline CPU), run on this host. Native and baseline
# builds must agree too; that is the full tier's cpu-parity stage.
ref=${bundle[x86_64-linux-musl]:-}
if [[ -z $ref ]]; then
  ref=zig-out/fleet-bundle/x86_64-linux-musl
  rm -rf "$ref" && mkdir -p "$ref"
  zig build check-runner -Dtarget=x86_64-linux-musl -Doptimize=ReleaseSafe -j8 --prefix "$ref/zig-out" >/dev/null
  zig build cli -Dtarget=x86_64-linux-musl -Doptimize=ReleaseFast -j8 --prefix "$ref/zig-out" >/dev/null
fi
"$ref/zig-out/bin/zpq-check" "${cases[@]}" --zpq "$ref/zig-out/bin/zpq" --work zig-out/fleet-work --answers-only \
  --reuse-fixtures --manifest zig-out/receipts/answers-reference.txt >/dev/null

remote=(units-core units)
declare -A load
for h in "${free[@]}"; do load[$h]=0; done
local_shards=() pids=() expect=()
run_on() {  # host, commands: in the shipped bundle, niced and capped as the inventory says
  ssh "$1" "cd .cache/zpq-fleet/$sha && nice -n $(field "$1" nice 10) $2"
}
for h in "${free[@]}"; do  # the architecture check, every free host
  t=$(field "$h" target x86_64-linux-musl)
  (
    ssh "$h" "mkdir -p .cache/zpq-fleet/$sha"
    rsync -a --delete "${bundle[$t]}/" "$h:.cache/zpq-fleet/$sha/"
    run_on "$h" "zig-out/bin/zpq-check ${cases[*]} --work zig-out/fleet-work --answers-only --reuse-fixtures \
      --manifest answers.txt" >/dev/null
    rsync -a "$h:.cache/zpq-fleet/$sha/answers.txt" "zig-out/receipts/answers-$h.txt"
    zig-out/bin/zpq-check stage --name "arch-$h" --dir zig-out/receipts --kind plain --tier full -- \
      zig-out/bin/zpq-check manifest-diff zig-out/receipts/answers-reference.txt "zig-out/receipts/answers-$h.txt"
  ) >"zig-out/receipts/fleet-arch-$h.log" 2>&1 &
  pids+=($!)
  expect+=("arch-$h")
done
for p in "${pids[@]}"; do wait "$p" || true; done
pids=()
for s in $(q largest-first "${shards[@]}"); do
  expect+=($(shard "$s" stages))
  if [[ " ${remote[*]} " != *" $s "* || ${#free[@]} -eq 0 ]]; then local_shards+=("$s"); continue; fi
  host=$(for h in "${free[@]}"; do echo "${load[$h]} $h"; done | sort -n | head -1 | cut -d' ' -f2)
  load[$host]=$(( ${load[$host]} + $(shard "$s" minutes) ))
  echo "fleet: $s on $host"
  (
    for st in $(shard "$s" stages); do
      bin=zpq-${st#units-}-test
      [[ $st == units-core ]] && bin=zpq-test
      allow=""  # macOS cannot run the Linux-only I/O paths; those tests skip there, by name
      [[ $(field "$host" target x86_64-linux-musl) == *macos ]] && allow="--allow-skip io.epoll. --allow-skip local_fs."
      run_on "$host" "zig-out/bin/zpq-check stage --sha $sha --name $st --dir receipts --kind units --tier full \
        $allow -- zig-out/bin/$bin" || true
    done
    rsync -a "$host:.cache/zpq-fleet/$sha/receipts/" zig-out/receipts/
  ) >"zig-out/receipts/fleet-$s.log" 2>&1 &
  pids+=($!)
done
for s in "${local_shards[@]}"; do
  echo "fleet: $s here"
  zig build check -Dtier=full -Dshard="$s" -j8 >"zig-out/receipts/fleet-$s.log" 2>&1 || true
done
for p in "${pids[@]}"; do wait "$p" || true; done
for h in "${free[@]}"; do ssh "$h" "rm -rf .cache/zpq-fleet/$sha" || true; done
[[ ${#local_shards[@]} -eq 0 ]] || expect=(fixtures "${expect[@]}")
exec zig-out/bin/zpq-check verdict --dir zig-out/receipts --sha "$sha" --expect "$(IFS=,; echo "${expect[*]}")"
