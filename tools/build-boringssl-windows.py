#!/usr/bin/env python3
"""Build local Windows BoringSSL archives from the Zig-pinned source package.

Run a zpq build once to populate zig-pkg, then invoke this script inside a
memory-limited scope. ZIG and both Zig caches can be supplied in the environment.
"""
import json
import os
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent.parent
source = root / 'zig-pkg/boringssl-0.20250514.0-zK0prLy1xAfRpqIBYbl48gYSlA95NLt3WrmR06bVT57a'
if not (source / 'gen/sources.json').is_file():
    raise SystemExit('Pinned BoringSSL source missing from zig-pkg; run zig build once to fetch it')
manifest = json.loads((source / 'gen/sources.json').read_text())
out = root / 'vendor/boring_tls/prebuilt/x86_64-windows'
out.mkdir(parents=True, exist_ok=True)
zig = os.environ.get('ZIG', 'zig')
cache = root / '.zig-cache'
for library, groups in [('crypto', ('bcm', 'crypto')), ('ssl', ('ssl',))]:
    files = [str(source / name) for group in groups for name in manifest[group]['srcs']]
    command = [zig, 'build-lib', '-target', 'x86_64-windows-gnu', '-O', 'ReleaseFast', '-j6',
               '--cache-dir', str(cache), '--global-cache-dir', str(cache / 'global'),
               '-lc', '-lc++', '-DOPENSSL_NO_ASM', '-DBORINGSSL_IMPLEMENTATION', '-DWIN32_LEAN_AND_MEAN',
               '-I', str(source / 'include'), '-I', str(source),
               '-femit-bin=' + str(out / ('lib' + library + '.a')),
               '-cflags', '-std=c++17', '-fno-exceptions', '-fno-rtti', '--', *files]
    subprocess.run(command, check=True, cwd=root)
