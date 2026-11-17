#!/usr/bin/env python3
"""Generate src/lambda/layout.ld: the Lambda bootstrap's code in the order a cold start runs it.

On Lambda a function's code is a squashfs, and the first touch of each 128 KiB block of the binary is a ~1 ms read
(docs/lambda_capabilities.md). The functions a cold start runs are scattered across the whole .text, so this script
lists them first, in the order they first ran, and LLD places everything else after them.

    layout.py --out src/lambda/layout.ld  <link.txt> <elf> <trace.log>... [-- <link.txt> <elf> <trace.log>...]

Each group is one architecture: the `ld.lld` command `zig build lambda --verbose-link` printed (link.txt), the
unstripped zpq-lambda it linked (built with function sections, `-Dlambda-layout=trace`), and qemu `-d in_asm` logs of
it, one per phase in phase order (tools/lambda/layout.sh makes all of these). A function in a section of its own
(`.text.<symbol>`: Zig code, compiler-rt) is placed by section; one in an object's plain `.text` (some of musl, the vendored C
libraries, prebuilt BoringSSL) is placed by its archive member. Names a later
build no longer has are ignored by LLD, so a stale script costs locality, never correctness.
"""
import argparse, bisect, os, re, subprocess, sys

def run(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout

def inputs(link_txt):
    """The .o and .a files of one ld.lld command line."""
    words = open(link_txt).read().split()
    return [w for w in words if w.endswith((".o", ".a")) and os.path.exists(w)]

def symbol_homes(paths):
    """symbol -> (archive basename or None, member, input section name), from each input's section headers and
    symbol table (`readelf -SsW`), so an alias resolves to the section that holds its code."""
    homes = {}
    for p in paths:
        archive = os.path.basename(p) if p.endswith(".a") else None
        member, secs = os.path.basename(p), {}
        for line in run("readelf", "-SsW", p).splitlines():
            m = re.match(r"^File: .*\((.*)\)$", line)
            if m:
                member, secs = m.group(1), {}; continue
            # "[Nr] Name Type Address ...": a Zig name may hold spaces (`[]const u8`), so it runs up to the type.
            m = re.match(r"\s*\[\s*(\d+)\]\s+(.+?)\s+[A-Z][A-Z0-9_+]*\s+[0-9a-f]{16}\s", line)
            if m:
                secs[m.group(1)] = m.group(2); continue
            # Num: Value Size Type Bind Vis Ndx Name
            f = line.split(None, 7)
            if len(f) != 8 or not f[0].endswith(":") or f[6] not in secs: continue
            if f[3] == "FUNC" or f[3] == "NOTYPE" and f[4] != "LOCAL":  # crt1's _start is NOTYPE
                homes.setdefault(f[7], (archive, member, secs[f[6]]))
    return homes

def executed(elf, logs):
    """Per log, the functions of `elf` whose code ran, in first-run order."""
    syms = []
    for line in run("nm", "-n", "-S", "--defined-only", elf).splitlines():
        f = line.split(maxsplit=3)
        if len(f) == 4 and f[2] in "tTwW": syms.append((int(f[0], 16), int(f[1], 16), f[3]))
        elif len(f) == 3 and f[1] in "tT": syms.append((int(f[0], 16), 1, f[2]))  # no size: assembly (_start)
    starts = [s[0] for s in syms]
    phases = []
    for log in logs:
        order, seen = [], set()
        for line in open(log, errors="replace"):
            m = re.match(r"^0x([0-9a-f]+):", line)
            if not m: continue
            a = int(m.group(1), 16)
            i = bisect.bisect_right(starts, a) - 1
            if i < 0 or a >= syms[i][0] + max(syms[i][1], 1): continue
            name = syms[i][2]
            if name not in seen: seen.add(name); order.append(name)
        phases.append(order)
    return phases

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("groups", nargs="+")
    a = ap.parse_args()
    groups, cur = [], []
    for w in a.groups:
        if w == "--": groups.append(cur); cur = []
        else: cur.append(w)
    groups.append(cur)
    per_group = []  # [(homes, [phase order])]
    for g in groups:
        link, elf, logs = g[0], g[1], g[2:]
        per_group.append((symbol_homes(inputs(link)), executed(elf, logs)))
    rules, seen = [], set()
    nphases = max(len(p) for _, p in per_group)
    for ph in range(nphases):  # phase-major, so every architecture's start comes before any first request
        for homes, phases in per_group:
            for name in phases[ph] if ph < len(phases) else ():
                archive, member, section = homes.get(name, (None, None, ".text." + name))
                if not section.startswith(".text"): continue
                if section != ".text":
                    if '"' in section: continue
                    # LLD reads even a quoted name as a glob: escape the brackets of `[]const u8` and friends.
                    # A generic instance's number (`__func_1428`) changes from build to build, and between stripped and
                    # unstripped ones: match any.
                    esc = lambda x: '"' + re.sub(r"__(func|anon)_\d+", r"__\1_*", re.sub(r'([][*?\\])', r'\\\1', x)) + '"'
                    # A stripped build names an internal function's section `.text..L<symbol>`: list both.
                    m = re.match(r"(\.text\.(?:unlikely\.|hot\.|startup\.)?)(.+)", section)
                    alt = m.group(1) + ".L" + m.group(2) if m and not m.group(2).startswith(".L") else None
                    rule = "*(" + " ".join(esc(x) for x in (section, alt) if x) + ")"
                elif archive:
                    rule = f"*{archive}:*{os.path.basename(member)}(.text)"
                else:
                    rule = f"*{os.path.basename(member)}(.text)"
                if rule not in seen: seen.add(rule); rules.append(rule)
    body = "\n".join("    " + r for r in rules)
    open(a.out, "w").write(
        "/* Generated by tools/lambda/layout.sh: the code a Lambda cold start runs, first and in the order it ran\n"
        "   (docs/lambda_capabilities.md). Regenerate, do not edit. */\n"
        "SECTIONS {\n  .text : {\n" + body + "\n    *(.text .text.*)\n  }\n}\nINSERT AFTER .rodata;\n")
    print(f"{a.out}: {len(rules)} rules", file=sys.stderr)

if __name__ == "__main__":
    main()
