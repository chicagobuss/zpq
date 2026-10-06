#!/usr/bin/env bash
# Compile (if stale) and run HwBloom against Hardwood.
#
#   tools/bloom/hwbloom.sh --file F --col C --type T --values v1,v2,...
#
# Needs ~/.cache/zpq-bloom/hardwood.classpath (hardwood-core jar + codec jars, colon-separated).
# Override with HW_CP_FILE, the class output dir with HWBLOOM_CLASSES, the heap with HWBLOOM_XMX.
# Resource limits (nice/systemd-run) are the caller's business.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CP_FILE="${HW_CP_FILE:-$HOME/.cache/zpq-bloom/hardwood.classpath}"
CLASSES="${HWBLOOM_CLASSES:-$HOME/.cache/zpq-bloom/hwbloom-classes}"
SRC="$HERE/HwBloom.java"

[[ -f "$CP_FILE" ]] || { echo "missing classpath file: $CP_FILE" >&2; exit 2; }
HW_CP="$(cat "$CP_FILE")"

# The classpath file is part of the staleness check: a rebuilt Hardwood jar must not run against classes
# compiled for a different API.
if [[ ! -f "$CLASSES/HwBloom.class" || "$SRC" -nt "$CLASSES/HwBloom.class" || "$CP_FILE" -nt "$CLASSES/HwBloom.class" ]]
then
  mkdir -p "$CLASSES"
  javac -d "$CLASSES" -cp "$HW_CP" "$SRC"
fi

exec java --enable-native-access=ALL-UNNAMED -Xmx"${HWBLOOM_XMX:-2g}" -cp "$HW_CP:$CLASSES" HwBloom "$@"
