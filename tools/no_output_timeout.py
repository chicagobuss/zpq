#!/usr/bin/env python3
"""
Run a command and kill it if it produces no stdout/stderr output for N seconds.

Usage:
  python3 tools/no_output_timeout.py --idle-seconds 60 -- <cmd> [args...]
"""

from __future__ import annotations

import argparse
import os
import selectors
import signal
import subprocess
import sys
import time


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--idle-seconds", type=float, default=60.0)
    ap.add_argument("--grace-seconds", type=float, default=2.0)
    ap.add_argument("--signal", default="TERM", help="Initial signal (TERM or INT).")
    # We accept either:
    #   no_output_timeout.py --idle-seconds 60 -- <cmd> ...
    # or:
    #   no_output_timeout.py --idle-seconds 60 <cmd> ...
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args()

    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd:
        ap.error("missing command after --")

    sig = getattr(signal, "SIG" + args.signal.upper(), signal.SIGTERM)

    # Start in its own process group so we can kill the whole subtree.
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=0,
        preexec_fn=os.setsid,
    )

    assert proc.stdout is not None
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)

    last_output = time.monotonic()

    try:
        while True:
            if proc.poll() is not None:
                return proc.returncode

            timeout = max(0.0, args.idle_seconds - (time.monotonic() - last_output))
            events = sel.select(timeout=timeout)

            if not events:
                # idle timeout
                sys.stderr.write(
                    f"[no_output_timeout] No output for {args.idle_seconds:.1f}s; sending {args.signal.upper()}...\n"
                )
                sys.stderr.flush()
                try:
                    os.killpg(proc.pid, sig)
                except ProcessLookupError:
                    return proc.returncode or 0

                # grace period, then SIGKILL
                deadline = time.monotonic() + args.grace_seconds
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        return proc.returncode
                    time.sleep(0.05)

                sys.stderr.write("[no_output_timeout] Still running; sending KILL...\n")
                sys.stderr.flush()
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                return 124

            for key, _mask in events:
                data = key.fileobj.read(4096)  # type: ignore[attr-defined]
                if not data:
                    # EOF
                    if proc.poll() is not None:
                        return proc.returncode
                    continue
                last_output = time.monotonic()
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
    finally:
        try:
            sel.unregister(proc.stdout)
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


