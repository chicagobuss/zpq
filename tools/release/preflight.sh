#!/usr/bin/env bash
# Release pre-flight: runs .github/workflows/release.yml as a dry run on Forgejo against one exact commit, so the
# GitHub run on the tag only repeats a build already known to be green.
#
#   tools/release/preflight.sh <ref> [--tag vX.Y.Z]
#
# <ref> is a branch or tag of the Forgejo repo. Its workflow file is the one that runs, and the run builds the
# commit <ref> names when the script starts. --tag defaults to v + that commit's build.zig.zon version; the tag
# itself need not exist (nothing is published on Forgejo).
#
# The extra runners (one per platform the release smokes on) are registered to the repo once, by hand. None has a
# service of its own: this script starts each one for the run and stops it on every exit, Ctrl-C included, by its
# exact systemd unit or pid. Hosts, paths and credentials come from ${ZPQ_PREFLIGHT_CONFIG:-~/.config/zpq/preflight.json},
# outside the repo:
#   forgejo: {url, repo, auth_file (a shell file exporting the next two), user_var, password_var}
#   ssh_auth_sock (optional), timeout_minutes (the run's limit, and every runner's outer timeout)
#   runners: [{name, ssh, start: "systemd" | "pidfile", dir (bin/forgejo-runner, config.yml, .runner), cache
#             (its XDG_CACHE_HOME: action clones, host-job workdirs, tool and actions caches), cap_gb (the cache is
#             cleared when over it), min_free_gb ({mount: GB} that must be free to start), path (pidfile hosts:
#             dirs put first on the runner's PATH, which host jobs inherit)}]
set -euo pipefail

usage() { echo "usage: $0 <ref> [--tag vX.Y.Z]" >&2; exit 2; }
ref= tag=
while (($#)); do
  case $1 in
    --tag) tag=${2:?--tag needs a value}; shift 2 ;;
    -*) usage ;;
    *) [[ -z $ref ]] || usage; ref=$1; shift ;;
  esac
done
[[ -n $ref ]] || usage
config=${ZPQ_PREFLIGHT_CONFIG:-$HOME/.config/zpq/preflight.json}
[[ -r $config ]] || { echo "preflight: no config at $config" >&2; exit 1; }

# The config as shell variables; runner i is R_name[i], R_ssh[i], and so on.
eval "$(python3 - "$config" <<'PY'
import json, os, shlex, sys
c = json.load(open(sys.argv[1]))
f = c["forgejo"]
def var(k, v): print(f"{k}={shlex.quote(str(v))}")
def arr(k, vs): print(f"{k}=(" + " ".join(shlex.quote(str(v)) for v in vs) + ")")
var("base", f["url"].rstrip("/")); var("repo", f["repo"]); var("auth_file", os.path.expanduser(f["auth_file"]))
var("user_var", f["user_var"]); var("password_var", f["password_var"])
var("ssh_sock", c.get("ssh_auth_sock", "")); var("timeout_min", int(c.get("timeout_minutes", 150)))
rs = c["runners"]
for k in ("name", "ssh", "start", "dir", "cache", "cap_gb"):
    arr(f"R_{k}", (r[k] for r in rs))
arr("R_path", (":".join(r.get("path", [])) for r in rs))
arr("R_free", (" ".join(f"{m}={g}" for m, g in r.get("min_free_gb", {}).items()) for r in rs))
PY
)"
# shellcheck disable=SC1090
source "$auth_file"
user=${!user_var:?} pass=${!password_var:?}
[[ -z $ssh_sock ]] || export SSH_AUTH_SOCK=$ssh_sock
secs=$((timeout_min * 60))
workflow=release.yml
unit=zpq-preflight-runner

# Credentials go to curl through files, never its argv.
work=$(mktemp -d)
host=${base#*://}; host=${host%%/*}; host=${host%%:*}
printf 'machine %s login %s password %s\n' "$host" "$user" "$pass" >"$work/netrc"
printf '%s' "$pass" >"$work/pw"
chmod 600 "$work/netrc" "$work/pw"
api() { curl -fsS --netrc-file "$work/netrc" "$@"; }
web() { curl -fsS -b "$work/jar" -c "$work/jar" "$@"; }
csrf=
web_login() {  # The run and job views (and cancel) are web-only on Forgejo 9; they need a session.
  local t
  t=$(web "$base/user/login" | grep -oP 'name="_csrf" value="\K[^"]+')
  web -o /dev/null --data-urlencode "_csrf=$t" --data-urlencode "user_name=$user" \
    --data-urlencode "password@$work/pw" "$base/user/login"
  csrf=$(web "$base/$repo/actions" | grep -oP "csrfToken: '\K[^']+") || { echo "preflight: web login failed" >&2; return 1; }
}
view() { web -X POST -H "X-Csrf-Token: $csrf" -H 'Content-Type: application/json' -d '{"logCursors":[]}' \
  "$base/$repo/actions/runs/$1/jobs/0"; }
latest_run() { web "$base/$repo/actions?workflow=$workflow" | { grep -oP "/$repo/actions/runs/\K\d+" || echo 0; } | sort -n | tail -1; }

remote() { local i=$1; shift; ssh -o BatchMode=yes -o ConnectTimeout=15 "${R_ssh[i]}" "$@"; }

# Each runner host's side, as scripts run there by `bash -s` (POSIX sh constructs: the Mac's bash is 3.2).
prepare='set -eu
dir=$1 cache=$2 cap_gb=$3; shift 3
[ -x "$dir/bin/forgejo-runner" ] && [ -s "$dir/.runner" ] || { echo "not registered: $dir has no bin/forgejo-runner or .runner" >&2; exit 1; }
mkdir -p "$cache"
used_kb() { du -sk "$cache" | cut -f1; }
if [ "$(used_kb)" -gt $((cap_gb * 1048576)) ]; then
  echo "cache over $cap_gb GB, clearing it"
  rm -rf "$cache/act" "$cache/actions-cache" "$cache/zig"
fi
for spec in "$@"; do
  mnt=${spec%=*} want=${spec#*=}
  free=$(df -Pk "$mnt" | awk "NR == 2 { print \$4 }")
  [ "$free" -ge $((want * 1048576)) ] || { echo "$mnt has $((free / 1048576)) GB free, the run needs $want" >&2; exit 1; }
done
echo "cache $(( $(used_kb) / 1024 )) MB (cap $cap_gb GB)"'
start_systemd='set -eu
dir=$1 cache=$2 secs=$3 unit=$4
if systemctl --user is-active -q "$unit"; then echo "$unit is already active: another pre-flight?" >&2; exit 1; fi
systemctl --user reset-failed "$unit" 2>/dev/null || true
systemd-run --user -q --collect --unit="$unit" --working-directory="$dir" -p RuntimeMaxSec="$secs" \
  -p MemoryMax=2G -p TimeoutStopSec=90 --setenv=XDG_CACHE_HOME="$cache" \
  "$dir/bin/forgejo-runner" --config "$dir/config.yml" daemon'
stop_systemd='unit=$1
systemctl --user stop "$unit" 2>/dev/null || true
systemctl --user reset-failed "$unit" 2>/dev/null || true'
# The runner leads its own process group (perl setpgrp, then exec, so the pid file names the group), and a stop
# reaches the host jobs it started; GNU timeout is the outer bound if this script never gets to stop it.
start_pidfile='set -eu
dir=$1 cache=$2 secs=$3 path=$4
cd "$dir"
if [ -s runner.pid ] && kill -0 -- "-$(cat runner.pid)" 2>/dev/null; then echo "a runner is already running (group $(cat runner.pid))" >&2; exit 1; fi
export PATH="$path:$PATH" XDG_CACHE_HOME="$cache"
timeout --version 2>/dev/null | grep -q GNU || { echo "no GNU timeout on $path:\$PATH" >&2; exit 1; }
nohup perl -e "setpgrp(0, 0); exec @ARGV or die" timeout -k 60 "$secs" \
  "$dir/bin/forgejo-runner" --config "$dir/config.yml" daemon >runner.log 2>&1 </dev/null &
echo $! >runner.pid'
stop_pidfile='dir=$1
cd "$dir" 2>/dev/null && [ -s runner.pid ] || exit 0
pg=$(cat runner.pid)
kill -TERM -- "-$pg" 2>/dev/null || true
for _ in $(seq 90); do kill -0 -- "-$pg" 2>/dev/null || break; sleep 1; done
kill -KILL -- "-$pg" 2>/dev/null || true
rm -f runner.pid'
# What is left of a runner: its unit, its processes (by exact binary path), and on Docker hosts its job containers.
leftovers='dir=$1 unit=$2
if command -v systemctl >/dev/null; then systemctl --user is-active -q "$unit" && echo "unit $unit active"; fi
pgrep -fl "$dir/bin/forgejo-runner" || true
if [ -r "$dir/.runner" ] && command -v docker >/dev/null; then docker ps --format "{{.Names}}" | grep "^FORGEJO-ACTIONS-TASK-" || true; fi'

started=() run= done=false
stop_runners() {
  local i out
  for i in "${started[@]}"; do
    if [[ ${R_start[i]} == systemd ]]; then remote "$i" bash -s -- "$unit" <<<"$stop_systemd" || true
    else remote "$i" bash -s -- "${R_dir[i]}" <<<"$stop_pidfile" || true; fi
  done
  for i in "${started[@]}"; do
    out=$(remote "$i" bash -s -- "${R_dir[i]}" "$unit" <<<"$leftovers" 2>&1) || out="could not check: $out"
    if [[ -n $out ]]; then echo "preflight: ${R_name[i]} still has: $out" >&2; else echo "stopped ${R_name[i]}"; fi
  done
}
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  set +e  # Nothing here may cut the stops short.
  if [[ -n $run && $done != true ]]; then  # Forgejo 9 can 500 on a cancel that races a job's pickup; retry.
    local n=0
    until web -o /dev/null -X POST -H "X-Csrf-Token: $csrf" "$base/$repo/actions/runs/$run/cancel" 2>/dev/null; do
      ((++n < 3)) || break
      sleep 3; web_login
    done
    ((n < 3)) && echo "cancelled run $run" || echo "preflight: could not cancel run $run" >&2
  fi
  stop_runners
  rm -rf "$work"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

sha=$(api "$base/api/v1/repos/$repo/commits?sha=$ref&limit=1&stat=false&verification=false&files=false" |
  python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["sha"])')
if [[ -z $tag ]]; then
  tag=v$(api "$base/api/v1/repos/$repo/raw/build.zig.zon?ref=$sha" | grep -m1 -oP '\.version = "\K[^"]+')
fi
echo "pre-flight of $ref = $sha as $tag on $base/$repo"

for i in "${!R_name[@]}"; do
  # shellcheck disable=SC2086  # R_free is a list of mount=GB words
  echo "${R_name[i]}: $(remote "$i" bash -s -- "${R_dir[i]}" "${R_cache[i]}" "${R_cap_gb[i]}" ${R_free[i]} <<<"$prepare")"
done
for i in "${!R_name[@]}"; do
  started+=("$i")
  if [[ ${R_start[i]} == systemd ]]; then
    remote "$i" bash -s -- "${R_dir[i]}" "${R_cache[i]}" "$secs" "$unit" <<<"$start_systemd"
  else
    remote "$i" bash -s -- "${R_dir[i]}" "${R_cache[i]}" "$secs" "${R_path[i]}" <<<"$start_pidfile"
  fi
  echo "started ${R_name[i]}"
done

web_login
before=$(latest_run)
inputs=$(python3 -c 'import json, sys; print(json.dumps({"ref": sys.argv[1], "inputs": {"tag": sys.argv[2], "dry_run": "true", "ref": sys.argv[3]}}))' "$ref" "$tag" "$sha")
api -o /dev/null -X POST -H 'Content-Type: application/json' -d "$inputs" "$base/api/v1/repos/$repo/actions/workflows/$workflow/dispatches"
start=$(date +%s)
for _ in $(seq 60); do
  r=$(latest_run)
  if ((r > before)); then run=$r; break; fi
  sleep 2
done
[[ -n $run ]] || { echo "preflight: the dispatch made no run" >&2; exit 1; }
view "$run" | python3 -c 'import json, sys; c = json.load(sys.stdin)["state"]["run"]["commit"]; sys.exit(not c["link"].endswith(sys.argv[1]))' "$sha" ||
  { echo "preflight: run $run is not of $sha (a concurrent dispatch?)" >&2; exit 1; }
echo "run $base/$repo/actions/runs/$run"

# Poll until the run is done: print each job's status as it changes, then the table.
report_py=$(cat <<'PY'
import json, sys
view, last_path, base, repo, run, final = sys.argv[1:]
r = json.load(open(view))["state"]["run"]
try:
    last = json.load(open(last_path))
except FileNotFoundError:
    last = {}
now = {str(j["id"]): j["status"] for j in r["jobs"]}
if final == "0":
    for j in r["jobs"]:
        if last.get(str(j["id"])) != j["status"]:
            print(f"  {j['name']}: {j['status']}", flush=True)
    json.dump(now, open(last_path, "w"))
else:
    w = max(len(j["name"]) for j in r["jobs"])
    for n, j in enumerate(r["jobs"]):
        print(f"{j['name']:<{w}}  {j['status']:<9} {j['duration']:>8}  {base}/{repo}/actions/runs/{run}/jobs/{n}")
open(last_path + ".status", "w").write(r["status"] + (" done" if r["done"] else ""))
PY
)
report() { python3 -c "$report_py" "$work/view.json" "$work/last" "$base" "$repo" "$run" "$1"; }
while :; do
  view "$run" >"$work/view.json" || { web_login; view "$run" >"$work/view.json"; }
  report 0
  [[ $(<"$work/last.status") == *" done" ]] && { done=true; break; }
  (($(date +%s) - start < secs)) || { echo "preflight: run $run passed ${timeout_min} min" >&2; exit 1; }
  sleep 20
done
wall=$(($(date +%s) - start))
status=$(<"$work/last.status")
echo
report 1
echo "run $run: ${status% done} in $((wall / 60))m$((wall % 60))s wall ($base/$repo/actions/runs/$run)"
[[ $status == "success done" ]]
