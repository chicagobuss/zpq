#!/bin/sh
# A fleet host's side of tools/check/fleet.sh (POSIX sh: Linux and macOS hosts).
#
#   fleet-host.sh timing HOST LOCK... why HOST must not be used now, or nothing
#   fleet-host.sh check PAR NICE MEM  in a shipped check directory: every case file answers-only into out/
set -u
case $1 in
timing)
  host=$2
  shift 2
  # A timing window: this host's own timing units (zpq-HOST-*), a bench or timing unit, a held timing lock, a full gate.
  if command -v systemctl >/dev/null; then
    systemctl --user list-units --state=active --no-legend --plain 'zpq-*' 2>/dev/null | while read -r unit _; do
      case $unit in "zpq-$host-"*|zpq-bench*|zpq-timing*) echo "timing unit $unit"; break ;; esac
    done
  fi
  for lock in "$HOME/.cache/zpq-bench.lock" "$@"; do
    if [ -e "$lock" ] && command -v flock >/dev/null && ! flock -n "$lock" true; then echo "timing lock $lock"; fi
  done
  if pgrep -f '[v]erify.py full|[D]tier=full' >/dev/null; then echo "a full gate is running"; fi
  exit 0 ;;
check)
  par=$2 nice=$3 mem=$4
  [ "$par" -gt 0 ] 2>/dev/null || par=$(( $(getconf _NPROCESSORS_ONLN) / 2 ))
  [ "$par" -ge 1 ] || par=1
  [ "$par" -le 6 ] || par=6
  # A case file's runner can reach 2.5 GB (planner and decode cases): the memory cap bounds how many run at once.
  case $mem in
    *G) by_mem=$(( ${mem%G} * 2 / 5 )); [ "$by_mem" -ge 1 ] || by_mem=1; [ "$par" -le "$by_mem" ] || par=$by_mem ;;
  esac
  # The memory cap needs the user manager, which a non-login ssh session finds only through XDG_RUNTIME_DIR.
  cap=""
  [ -n "${XDG_RUNTIME_DIR:-}" ] || [ ! -d "/run/user/$(id -u)" ] || export XDG_RUNTIME_DIR="/run/user/$(id -u)"
  if [ -n "$mem" ] && command -v systemd-run >/dev/null && systemd-run --user --scope --quiet true 2>/dev/null; then
    cap="systemd-run --user --scope --quiet -p MemoryMax=$mem -p MemorySwapMax=0"
  fi
  rm -rf out && mkdir out && top=$(pwd)
  # The runner bounds each CLI point with GNU timeout; a Mac has none, so a shim gives it gtimeout, or else a Perl
  # alarm that survives exec (a timed-out point then reads as a signal, still a failure).
  if ! command -v timeout >/dev/null; then
    mkdir -p shim
    if command -v gtimeout >/dev/null; then ln -sf "$(command -v gtimeout)" shim/timeout
    else
      printf '#!/bin/sh\nshift\nd=$1\nshift\nexec perl -e %s "$d" "$@"\n' "'alarm shift; exec @ARGV or exit 127'" \
        > shim/timeout
    fi
    chmod +x shim/timeout && PATH=$top/shim:$PATH && export PATH
  fi
  cd common || exit 1
  # Case files in parallel, each its own runner: the slowest file, not the sum, sets the time. Binaries by absolute
  # path: a runner spawns zpq from case directories of its own.
  { [ -s order.txt ] && cat order.txt || ls tools/check/cases/*.json | grep -v clickbench; } |
    $cap nice -n "$nice" xargs -P "$par" -I{} sh -c '
    n=$(basename "$1" .json)
    "$2/zig-out/bin/zpq-check" --cases "$1" --zpq "$2/zig-out/bin/zpq" --work "zig-out/fleet-work/$n" --answers-only \
      --reuse-fixtures --manifest "$2/out/$n.txt" > "$2/out/$n.log" 2>&1 || echo "$n" >> "$2/out/failed"' sh {} "$top"
  ;;
*) echo "usage: fleet-host.sh timing HOST | check PAR NICE MEM" >&2; exit 2 ;;
esac
