"""Host conditioning for timing: CPU placement, verified eviction or warming of inputs, idle checks, the lock.

Nothing here changes system state: no cache drop for other users' files, no power profile, no sysctl. Eviction is
`POSIX_FADV_DONTNEED` on our own inputs, verified by `mincore`.
"""

import ctypes
import fcntl
import mmap
import os
import time
from contextlib import contextmanager
from pathlib import Path

LOCK = Path.home() / ".cache/zpq-bench.lock"
MINCORE_BITS = bytes(i & 1 for i in range(256))


def cpu_list(text):
    out = set()
    for part in text.strip().split(","):
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return out


def placement(root=Path("/sys/devices/system/cpu")):
    """(timing, housekeeping) CPUs: engines time on cpu0's last-level-cache domain, the controller and oracle run on
    the rest. A host with one cache domain shares it, and says so in the run header."""
    online = cpu_list((root / "online").read_text()) & os.sched_getaffinity(0)
    caches = sorted((root / "cpu0/cache").glob("index*"), key=lambda p: int((p / "level").read_text()))
    timing = cpu_list((caches[-1] / "shared_cpu_list").read_text()) & online if caches else online
    rest = online - timing
    return sorted(timing), sorted(rest or timing)


def residency(path):
    """Resident pages of a file, by mincore over a private mapping (never faults the pages in)."""
    size = os.path.getsize(path)
    if not size:
        return 0, 0
    pages = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    with open(path, "rb") as f, mmap.mmap(f.fileno(), size, access=mmap.ACCESS_COPY) as view:
        vec = (ctypes.c_ubyte * pages)()
        ptr = ctypes.addressof(ctypes.c_char.from_buffer(view))
        if libc.mincore(ptr, size, vec):
            raise OSError(ctypes.get_errno(), "mincore failed")
        del ptr
    return bytes(vec).translate(MINCORE_BITS).count(1), pages


def evict(paths, attempts=3):
    """Drop each file's clean cached pages; verified when at most 0.1% and 8 MiB of each stays resident (pages
    another process maps cannot be dropped). Returns (verified, {path: resident pages})."""
    state = {}
    for attempt in range(attempts):
        for p in paths:
            fd = os.open(p, os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
            resident, pages = residency(p)
            state[p] = {"resident": resident, "pages": pages,
                        "ok": resident <= pages / 1000 and resident * mmap.PAGESIZE <= 8 << 20}
        if all(s["ok"] for s in state.values()):
            return True, state
        time.sleep(0.1 * (attempt + 1))
    return False, state


def warm(paths):
    """Read each file that is not fully resident; verified when every page is."""
    state, buf = {}, bytearray(4 << 20)
    for p in paths:
        resident, pages = residency(p)
        if resident < pages:
            with open(p, "rb", buffering=0) as f:
                while f.readinto(buf):
                    pass
            resident, pages = residency(p)
        state[p] = {"resident": resident, "pages": pages, "ok": resident == pages}
    return all(s["ok"] for s in state.values()), state


def busy_ticks(cpus):
    total = 0
    for line in Path("/proc/stat").read_text().splitlines():
        f = line.split()
        if f[0].startswith("cpu") and f[0][3:].isdigit() and int(f[0][3:]) in cpus:
            ticks = list(map(int, f[1:9]))
            total += sum(ticks) - ticks[3] - ticks[4]
    return total


def idle(cpus, seconds=0.25, threshold=0.5):
    """Busy cores on the timing CPUs over a short sample; quiet when within `threshold` cores plus one tick per
    CPU of counter skew."""
    hz = os.sysconf("SC_CLK_TCK")
    before, start = busy_ticks(set(cpus)), time.monotonic()
    time.sleep(seconds)
    ticks, elapsed = busy_ticks(set(cpus)) - before, time.monotonic() - start
    allowed = threshold * elapsed + len(cpus) / hz
    return {"busy_cores": ticks / hz / elapsed, "quiet": ticks / hz <= allowed}


def foreign_cpu(reply, cpus, threshold=0.5):
    """CPU on the timing CPUs during a run that the engine did not use: (seconds, allowed, contended)."""
    hz = os.sysconf("SC_CLK_TCK")
    foreign = max(0.0, reply["busy_ticks"] / hz - reply["cpu_ns"] / 1e9)
    allowed = threshold * reply["wall_ns"] / 1e9 + 2 * len(cpus) / hz
    return {"foreign_s": foreign, "allowed_s": allowed, "contended": foreign > allowed}


def psi_available():
    return Path("/proc/pressure/io").is_file()


@contextmanager
def timing_lock(wait_s=0):
    """The host's one timing lock (fleet.sh skips a host that holds it); waits up to `wait_s` for it."""
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + wait_s
    with LOCK.open("w") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise SystemExit(f"bench: {LOCK} is held: this host is timing") from None
                time.sleep(1)
        yield
