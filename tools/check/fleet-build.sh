#!/bin/sh
# A builder's side of tools/check/fleet.sh: in the synced source tree, one target's bundle from a warm Zig cache.
#
#   fleet-build.sh TARGET ZIG_VERSION OUT STEP:OPTIMIZE...
#
# Every builder uses the pinned Zig and caches at the same paths under ~/.cache/zpq-fleet (the Zig is fetched once):
# the toolchain's path is in the binary, so two builders agree byte for byte only from the same paths. The caches
# stay between pushes, so a push rebuilds only what changed.
set -eu
t=$1 v=$2 out=$3
shift 3
base=$HOME/.cache/zpq-fleet
mkdir -p "$base"
z=$base/zig-$v/zig
if [ ! -x "$z" ]; then  # two builds on one host may fetch at once: each unpacks privately, the first rename wins
  tmp=$(mktemp -d "$base/zig-fetch.XXXXXX")
  curl -sfL "https://ziglang.org/download/$v/zig-$(uname -m)-linux-$v.tar.xz" | tar -xJ --strip-components=1 -C "$tmp"
  mv -T "$tmp" "$base/zig-$v" 2>/dev/null || rm -rf "$tmp"
fi
export ZIG_GLOBAL_CACHE_DIR=$base/zig-global ZIG_LOCAL_CACHE_DIR=$base/zig-local
# One build of a target per builder: an earlier run's survivor would write into this one's output. The pid file's
# process is that survivor only if it still runs this script for this target (a pid is reused).
old=$(cat "$out.pid" 2>/dev/null || true)
if [ -n "$old" ] && tr '\0' ' ' < "/proc/$old/cmdline" 2>/dev/null | grep -q "fleet-build.sh $t "; then
  pkill -P "$old" || true
  kill "$old" || true
fi
mkdir -p "$(dirname "$out")" && echo $$ > "$out.pid"
rm -rf "$out" && mkdir -p "$out"
# Each compile is capped where the user manager allows: a runaway build fails alone, the host never OOMs.
cap=""
[ -n "${XDG_RUNTIME_DIR:-}" ] || [ ! -d "/run/user/$(id -u)" ] || export XDG_RUNTIME_DIR="/run/user/$(id -u)"
if command -v systemd-run >/dev/null && systemd-run --user --scope --quiet true 2>/dev/null; then
  cap="systemd-run --user --scope --quiet -p MemoryMax=6G -p MemorySwapMax=0"
fi
pids=""
for s in "$@"; do
  $cap nice -n 10 "$z" build "${s%%:*}" -Dtarget="$t" -Doptimize="${s##*:}" --prefix "$out/${s%%:*}" \
    > "$out/${s%%:*}.log" 2>&1 &
  pids="$pids $!"
done
rc=0
for p in $pids; do wait "$p" || rc=1; done
exit $rc
