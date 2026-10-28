#!/usr/bin/env bash
# The fleet: each push's architecture check, pipelined; with --full, also the full tier's shards.
#
#   tools/check/fleet.sh [--host H]... [--release] [--full] [--inventory FILE]
#
# The inventory (ZPQ_FLEET, default ~/.config/zpq/fleet.json; tools/check/fleet.example.json shows every key) names
# the hosts, each target's builders, the o11y feed and the timing locks. This repo names no machines.
#
# Every target builds at once on its first two idle builders (idle by the fleet o11y feed), each from a
# warm Zig cache kept on that builder (tools/check/fleet-build.sh). The first bundle done is the one hosts get; the
# two builds' hashes are recorded, a free reproducibility check. This host is the reference: it
# runs the x86_64 baseline bundle's answers and makes the fixtures. Every other host starts the moment its target's
# bundle and the fixtures exist, all hosts at once (tools/check/fleet-host.sh), and its answers manifest must equal
# the reference's, case for case. A host in a timing window (its zpq-HOST-* units, a bench or timing unit, a held
# timing lock, a running full gate) or offline is skipped and reported as skipped, never as failed. A host marked
# "manual" runs only when named with --host or under --release. Receipts and fleet.json (placement, phase times,
# binary hashes) land in zig-out/receipts.
set -euo pipefail
cd "$(dirname "$0")/../.."
inventory=${ZPQ_FLEET:-${XDG_CONFIG_HOME:-$HOME/.config}/zpq/fleet.json}
hosts=() release=0 full=0
while [[ $# -gt 0 ]]; do
  case $1 in
    --host) hosts+=("$2"); shift 2 ;;
    --release) release=1; shift ;;
    --full) full=1; shift ;;
    --inventory) inventory=$2; shift 2 ;;
    *) echo "fleet: unknown argument $1" >&2; exit 2 ;;
  esac
done
if [[ -n $(git status --porcelain --untracked-files=no) ]]; then
  echo "fleet: dirty tree; receipts would be unofficial" >&2
  exit 2
fi
sha=$(git rev-parse HEAD) me=$(hostname -s)
zig_version=$(sed -n 's/.*minimum_zig_version = "\(.*\)".*/\1/p' build.zig.zon)
py=$(command -v "${PYTHON:-$PWD/.venv/bin/python}")
R=zig-out/receipts W=zig-out/fleet
rm -rf "$R" "$W" && mkdir -p "$R" "$W"/{bundle,ref,hosts,log}
t0=$(date +%s)
# A run's background builds and checks die with it: a survivor would mark the next run's bundles ready.
trap 'jobs -p | xargs -r kill 2>/dev/null; pkill -P $$ 2>/dev/null || true' EXIT
phase() { local t=$(($(date +%s) - t0)); echo "$t $*" >> "$W/phases"; echo "fleet: +${t}s $*"; }

[[ -r $inventory ]] ||
  { echo "fleet: no inventory at $inventory (set ZPQ_FLEET; see tools/check/fleet.example.json)" >&2; exit 2; }
field() {  # host key default: one inventory value
  "$py" -c 'import json, sys
print(json.load(open(sys.argv[1]))["hosts"].get(sys.argv[2], {}).get(sys.argv[3], sys.argv[4]))' "$inventory" "$@"
}
top() {  # key default: a top-level inventory value, a list printed space-separated
  "$py" -c 'import json, sys
v = json.load(open(sys.argv[1])).get(sys.argv[2], sys.argv[3])
print(" ".join(v) if isinstance(v, list) else v)' "$inventory" "$@"
}
o11y=${ZPQ_FLEET_O11Y:-$(top o11y "")}
timing_locks=$(top timing_locks "")
# Each target's builders, in preference order; this host is always the last resort.
declare -A BUILDERS
for t in x86_64-linux-musl aarch64-macos aarch64-linux-musl; do
  BUILDERS[$t]="$("$py" -c 'import json, sys
print(" ".join(json.load(open(sys.argv[1])).get("builders", {}).get(sys.argv[2], [])))' "$inventory" "$t") $me"
done
if [[ ${#hosts[@]} -eq 0 ]]; then
  mapfile -t hosts < <("$py" -c 'import json, sys
for name, h in json.load(open(sys.argv[1]))["hosts"].items():
    if h.get("enabled", True) and (sys.argv[2] == "1" or not h.get("manual", False)):
        print(name)' "$inventory" "$release")
fi
[[ " ${hosts[*]} " == *" $me "* ]] || hosts+=("$me")

# A target build compiles its steps at once, each capped at compile_gb: an aarch64 ReleaseSafe zpq-check peaks at
# 6.8 GB and its ReleaseFast zpq at 6.0. So a builder holds a target build to compile_gb once per step.
steps="check-runner:ReleaseSafe cli:ReleaseFast"
[[ $full == 0 ]] || steps="$steps test-bin:Debug"
compile_gb=10
target_gb=$((compile_gb * $(wc -w <<< "$steps")))
# Liveness and load from the o11y feed; a builder must be idle (live, half its cores free, a quarter of its memory).
[[ -n $o11y ]] && curl -s -m 10 "$o11y" > "$W/o11y.json" || echo '{"hosts": []}' > "$W/o11y.json"
"$py" tools/check/fleet_load.py --self-test >/dev/null ||
  { echo "fleet: the builder probe fails its self-test" >&2; exit 2; }
load() {  # host: "offline", "busy", or "idle N", N the target builds its free memory holds (tools/check/fleet_load.py)
  "$py" tools/check/fleet_load.py "$W/o11y.json" "$(field "$1" o11y_name "$1")" "$target_gb"
}
probe() {  # host: why it must not be used, or nothing
  if [[ $1 == "$me" ]]; then sh tools/check/fleet-host.sh timing "$1" $timing_locks
  else ssh -o BatchMode=yes -o ConnectTimeout=10 "$1" sh -s -- timing "$1" $timing_locks \
    < tools/check/fleet-host.sh 2>/dev/null || echo "unreachable"; fi
}
declare -A skip state target_of
for h in "${hosts[@]}"; do state[$h]=$(load "$h"); ( probe "$h" | head -1 > "$W/log/probe-$h" ) & done
wait
free=()
for h in "${hosts[@]}"; do
  why=$(cat "$W/log/probe-$h")
  [[ ${state[$h]} == offline ]] && why=offline
  if [[ -n $why ]]; then skip[$h]=$why; phase "skip $h: $why"
  else free+=("$h"); target_of[$h]=$(field "$h" target x86_64-linux-musl); fi
done
[[ -z ${skip[$me]:-} ]] || { echo "fleet: the reference host $me is not usable: ${skip[$me]}" >&2; exit 2; }

# Placement: each target a free host needs (and the reference's) builds on its first two idle builders at once, or
# here when none is idle. The first bundle done is the one every host gets; the other's hashes are the
# reproducibility check.
# A builder takes no more target builds than its free memory holds: a host shared with others must never OOM.
declare -A need builders room
for h in "${free[@]}"; do need[${target_of[$h]}]=1; done
need[x86_64-linux-musl]=1
for h in "${free[@]}"; do [[ ${state[$h]} == idle* ]] && room[$h]=${state[$h]#idle }; done
for t in x86_64-linux-musl aarch64-macos aarch64-linux-musl; do
  [[ -n ${need[$t]:-} ]] || continue
  for b in ${BUILDERS[$t]}; do
    if [[ ${room[$b]:-0} -gt 0 && $(wc -w <<< "${builders[$t]:-}") -lt 2 ]]; then
      builders[$t]+=" $b"
      room[$b]=$((room[$b] - 1))
    fi
  done
  builders[$t]=${builders[$t]:-$me}
  phase "build $t on${builders[$t]}"
done

# The source as committed goes to each builder once; then every build there starts.
mkdir -p "$W/src" && git archive HEAD | tar -x -C "$W/src"
src=.cache/zpq-fleet/src
# The inventory's fleet_dir, if any, holds this host's fleet tree (say, off the disk timed runs use), linked in under
# the builders' common path.
fleet_dir=$(top fleet_dir "")
if [[ -n $fleet_dir && ! -e $HOME/.cache/zpq-fleet ]]; then
  mkdir -p "$fleet_dir" && ln -s "$fleet_dir" "$HOME/.cache/zpq-fleet"
fi
sync_src() {
  if [[ $1 == "$me" ]]; then mkdir -p "$HOME/$src" && rsync -a --delete --checksum "$W/src/" "$HOME/$src/"
  else ssh "$1" "mkdir -p $src" && rsync -a --delete --checksum "$W/src/" "$1:$src/"; fi
}
build() {  # target builder bundle-dir
  local t=$1 b=$2 dst=$3 out=.cache/zpq-fleet/out/$t
  if [[ $b == "$me" ]]; then
    (cd "$HOME/$src" && nice -n 10 sh tools/check/fleet-build.sh "$t" "$zig_version" "$HOME/$out" "$compile_gb" $steps)
    mkdir -p "$dst" && for s in "$HOME/$out"/*/; do cp -r "$s". "$dst/"; done
  else
    ssh "$b" "cd $src && sh tools/check/fleet-build.sh $t $zig_version \$HOME/$out $compile_gb $steps"
    mkdir -p "$dst" && for s in $steps; do rsync -a "$b:$out/${s%%:*}/" "$dst/" 2>/dev/null || true; done
  fi
  [[ -x $dst/bin/zpq && -x $dst/bin/zpq-check ]]
}
for b in $(printf '%s\n' ${builders[@]} | sort -u); do ( sync_src "$b" > "$W/log/sync-$b" 2>&1 ) & done
wait
race() {  # target builder: build; the first done claims the target
  local t=$1 b=$2 dir=$W/bundle/$t@$2
  if build "$t" "$b" "$dir/zig-out" > "$W/log/build-$t@$b" 2>&1; then
    if [[ $t == *macos ]] && command -v llvm-objdump >/dev/null; then  # macOS reaches the kernel via libSystem only
      for f in "$dir/zig-out/bin"/*; do
        if llvm-objdump -d "$f" | grep -qE '\s(svc|syscall)\s'; then
          echo "raw syscall in $f" >> "$W/log/build-$t@$b"
          return 1
        fi
      done
    fi
    if mkdir "$W/bundle/$t.claim" 2>/dev/null; then
      ln -s "$t@$b" "$W/bundle/$t" && touch "$W/bundle/$t.ready" && phase "built $t on $b, first: in use"
    else phase "built $t on $b, second"; fi
  elif [[ -e $W/bundle/$t.cancel-$b ]]; then
    phase "build $t on $b cancelled: the other build won and $b checks next"
  else
    phase "build $t on $b FAILED ($W/log/build-$t@$b)"
    touch "$W/bundle/$t.lost-$b"
    [[ $(ls "$W/bundle" | grep -c "^$t.lost-") -lt $(wc -w <<< "${builders[$t]}") ]] || touch "$W/bundle/$t.failed"
  fi
}
for t in "${!need[@]}"; do for b in ${builders[$t]}; do race "$t" "$b" & done; done

# What every host gets besides its binaries: case files, goldens, inputs, and the reference run's fixtures.
mkdir -p "$W/common/data" "$W/common/tools"
cp -rL ci packaging "$W/common/" && cp -rL tools/check tools/datagen tools/zpq-gen "$W/common/tools/"
cp -rL data/*.parquet data/parquet-testing "$W/common/data/"
await() { until [[ -e $1.ready ]]; do [[ -e $1.failed ]] && return 1; sleep 1; done; }
ref=$W/bundle/x86_64-linux-musl
await "$ref" || { phase "no x86_64 bundle, so no reference"; exit 1; }
chk=$PWD/$ref/zig-out/bin/zpq-check
# From the hosts' own layout (common/), so every path a diagnostic names is the same here and there.
(cd "$W/common" && ls tools/check/cases/*.json | grep -v clickbench | xargs -P 16 -I{} sh -c 'n=$(basename "$1" .json)
  s=$(date +%s%N)
  "$2/zpq-check" --cases "$1" --zpq "$2/zpq" --work "zig-out/fleet-work/$n" --python "$3" --answers-only \
    --manifest "$4/$n.txt" > "$4/$n.log" 2>&1 || echo "$n" >> "$4/failed"
  echo "$(( $(date +%s%N) - s )) $1" >> "$4/times"' \
    sh {} "$PWD/../bundle/x86_64-linux-musl/zig-out/bin" "$py" "$PWD/../ref")
# Hosts start the slowest files first, as this run timed them, so their parallel lanes end together.
sort -rn "$W/ref/times" | cut -d' ' -f2 > "$W/common/order.txt"
LC_ALL=C sort "$W/ref"/*.txt > "$R/answers-reference.txt"
[[ ! -e $W/ref/failed ]] || phase "reference runs failed: $(tr '\n' ' ' < "$W/ref/failed")"
phase "reference answers: $(wc -l < "$R/answers-reference.txt") cases"

check() {  # host: ship its bundle, run, fetch its manifest, compare it with the reference
  local h=$1 t=${target_of[$1]} d=.cache/zpq-fleet/check
  await "$W/bundle/$t" || { echo "no $t bundle"; return 1; }
  # A host still building a target another builder already delivered stops: its check is worth more than a hash.
  for x in "${!builders[@]}"; do
    if [[ " ${builders[$x]} " == *" $h "* && -e $W/bundle/$x.ready && ! -e $W/bundle/$x@$h/zig-out/bin/zpq-check &&
          $(readlink "$W/bundle/$x") != "$x@$h" && $h != "$me" ]]; then
      touch "$W/bundle/$x.cancel-$h"
      ssh "$h" "p=\$(cat .cache/zpq-fleet/out/$x.pid 2>/dev/null) &&
        tr '\\0' ' ' < /proc/\$p/cmdline | grep -q fleet-build && { pkill -P \$p; kill \$p; }" || true
    fi
  done
  if [[ $h == "$me" ]]; then cp "$R/answers-reference.txt" "$R/answers-$h.txt"
  else
    ssh "$h" "mkdir -p $d/common $d/zig-out"
    rsync -a --delete "$W/common/" "$h:$d/common/" && rsync -a --delete "$W/bundle/$t/zig-out/" "$h:$d/zig-out/"
    ssh "$h" "cd $d && sh common/tools/check/fleet-host.sh check $(field "$h" threads 0) $(field "$h" nice 10) \
      '$(field "$h" memory_max "")'"
    rsync -a "$h:$d/out/" "$W/hosts/$h/"
    [[ ! -e $W/hosts/$h/failed ]] || echo "runs failed on $h: $(tr '\n' ' ' < "$W/hosts/$h/failed")"
    LC_ALL=C sort "$W/hosts/$h"/*.txt > "$R/answers-$h.txt"
  fi
  "$chk" stage --name "arch-$h" --dir "$R" --kind plain --tier full -- \
    "$chk" manifest-diff "$R/answers-reference.txt" "$R/answers-$h.txt"
}
pids=()
for h in "${free[@]}"; do
  ( check "$h" > "$W/log/check-$h" 2>&1 && phase "checked $h" || phase "check $h FAILED ($W/log/check-$h)" ) &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p" || true; done
wait  # the repeated builds
expect=(); for h in "${free[@]}"; do expect+=("arch-$h"); done

shard_stages() {
  "$py" -c 'import json, sys
print(" ".join(json.load(open("tools/check/shards.json"))["shards"][sys.argv[1]]["stages"]))' "$1"
}
if [[ $full == 1 ]]; then
  # The full tier: the remote unit shards, largest first, each on the least-loaded checked host; the rest here.
  declare -A busy_for
  remote=()
  for h in "${free[@]}"; do [[ $h == "$me" ]] || { remote+=("$h"); busy_for[$h]=0; }; done
  for s in units-core units; do
    [[ ${#remote[@]} -gt 0 ]] || break
    h=$(for x in "${remote[@]}"; do echo "${busy_for[$x]} $x"; done | sort -n | head -1 | cut -d' ' -f2)
    busy_for[$h]=$(( ${busy_for[$h]} + 1 ))
    stages=$(shard_stages "$s")
    allow=""; [[ ${target_of[$h]} == *macos ]] && allow="--allow-skip io.epoll. --allow-skip local_fs."
    for st in $stages; do
      bin=zpq-${st#units-}-test; [[ $st == units-core ]] && bin=zpq-test
      ssh "$h" "cd .cache/zpq-fleet/check/common && ../zig-out/bin/zpq-check stage --sha $sha --name $st \
        --dir ../receipts --kind units --tier full $allow -- ../zig-out/bin/$bin" >> "$W/log/units-$h" 2>&1 || true
      expect+=("$st")
    done
    rsync -a "$h:.cache/zpq-fleet/check/receipts/" "$R/" || true
    phase "$s on $h"
  done
  for s in cases corpus clickbench os cpu libzpq; do
    zig build check -Dtier=full -Dshard="$s" -j8 > "$W/log/shard-$s" 2>&1 || true
    expect+=($(shard_stages "$s"))
    phase "shard $s here"
  done
  expect+=(fixtures)
fi

# The record: skipped hosts and why, placement, phase times, and each binary's hash on each builder.
{ for t in "${!builders[@]}"; do echo "builders $t ${builders[$t]# }"; done
  for h in "${!skip[@]}"; do echo "skip $h ${skip[$h]}"; done
  for h in "${!target_of[@]}"; do echo "target $h ${target_of[$h]}"; done; } > "$W/placement"
"$py" - "$W" "$R/fleet.json" "$sha" "$t0" <<'PY'
import hashlib, json, sys, time
from pathlib import Path
W, out, sha, t0 = Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
rows = [l.split(" ", 2) for l in (W / "placement").read_text().splitlines()]
kind = lambda k: {a: b for x, a, b in rows if x == k}
import shutil, subprocess, tempfile
digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None
def code(p):
    """The binary without its symbol table: compiler-numbered symbol names vary run to run, code and data do not."""
    tool = shutil.which("llvm-objcopy")
    if not tool or not p.is_file():
        return None
    with tempfile.NamedTemporaryFile() as f:
        subprocess.run([tool, "--strip-all", str(p), f.name], check=True)
        return digest(Path(f.name))
bins = lambda d: {n: {"sha256": digest(W / "bundle" / d / "zig-out/bin" / n),
                      "stripped_sha256": code(W / "bundle" / d / "zig-out/bin" / n)} for n in ("zpq", "zpq-check")}
builds = {}
for t, names in kind("builders").items():
    used = (W / "bundle" / t).resolve().name.split("@")[-1] if (W / "bundle" / t).exists() else None
    each = {b: bins(f"{t}@{b}") for b in names.split()}
    done = [{n: x["stripped_sha256"] for n, x in h.items()} for h in each.values() if h["zpq"]["sha256"]]
    builds[t] = {"used": used, "binaries": each,
                 "identical": None if len(done) < 2 else all(h == done[0] for h in done)}
phases = [l.split(" ", 1) for l in (W / "phases").read_text().splitlines()]
json.dump({"sha": sha, "wall_s": int(time.time()) - t0, "skipped": kind("skip"), "targets": kind("target"),
           "builds": builds, "phases": [{"t_s": int(t), "what": w} for t, w in phases]}, open(out, "w"), indent=1)
for t, b in builds.items():
    same = {None: "built once", True: "identical code and data", False: "DIFFERENT code"}[b["identical"]]
    print(f"fleet: {t}: used the build from {b['used']}; {' and '.join(b['binaries'])}: {same}")
for h, why in kind("skip").items():
    print(f"fleet: SKIPPED {h}: {why}")
PY
phase "done"
exec "$chk" verdict --dir "$R" --sha "$sha" --expect "$(IFS=,; echo "${expect[*]}")"
