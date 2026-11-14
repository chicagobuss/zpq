#!/usr/bin/env python3
"""Opt-in Linux device-loss contract. Run in a memory scope with the timing lock.

--work must be a new directory on the internal SSD. Only its disposable image is
formatted. Requires sudo -n for loop, device-mapper, mount and the bounded probe.
"""
import argparse
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import time


def run(*args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, timeout=30, **kwargs)


def sudo(*args, **kwargs):
    return run("sudo", "-n", *args, **kwargs)


def stopped(pid, timeout=15):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        got, status = os.waitpid(pid, os.WUNTRACED | os.WNOHANG)
        if got:
            if os.WIFSTOPPED(status):
                return
            raise RuntimeError(f"query exited before fault injection: {status}")
        time.sleep(0.01)
    raise TimeoutError("query did not reach the data-read stop")


def one(work, binary, source, oracle, mode, fault):
    case = work / f"{mode}-{fault}"
    case.mkdir()
    image = case / "disk.img"
    mount = case / "fs"
    mount.mkdir()
    name = f"zpq-device-loss-{os.getpid()}"
    mapper = f"/dev/mapper/{name}"
    loop = None
    mounted = mapped = False
    query = tracer = None
    table = None
    try:
        with image.open("xb") as f:
            f.truncate(4 << 30)
        # mkfs is given the owned regular file, never an arbitrary device.
        run("mkfs.ext4", "-q", "-F", image)
        loop = sudo("losetup", "--find", "--show", image, capture_output=True, text=True).stdout.strip()
        table = f"0 {image.stat().st_size // 512} linear {loop} 0"
        sudo("dmsetup", "create", name, "--table", table)
        mapped = True
        sudo("mount", "-o", "noatime", mapper, mount)
        mounted = True
        sudo("chown", f"{os.getuid()}:{os.getgid()}", mount)
        input_path = mount / "input.parquet"
        run("cp", "--", source, input_path)
        fd = os.open(input_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        sql = "SELECT sum(l_extendedprice) FROM '" + str(input_path).replace("'", "''") + "'"
        truth = run(oracle, "-no-agent", "-csv", "-c", "SET memory_limit='4GB';" + sql,
                    capture_output=True, text=True).stdout.strip().splitlines()[-1]
        env = dict(os.environ, ZPQ_IO_PROFILE=mode, ZPQ_IO_THREADS="1")
        command = [str(binary), "query", str(input_path), "--aggregate", "sum(l_extendedprice)",
                   "-j", "2", "--max-memory", "1GB", "--summary=json"]
        healthy = run(*command, env=env, capture_output=True, text=True)
        got = next(iter(json.loads(healthy.stdout).values()))
        assert abs(float(got) - float(truth)) <= abs(float(truth)) * 1e-10
        fd = os.open(input_path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
        residency = run("fincore", "--bytes", "--output", "RES,SIZE", input_path, capture_output=True, text=True).stdout
        (case / "residency.txt").write_text(residency)
        with (case / "stdout").open("w") as output, (case / "stderr").open("w") as error:
            # The shell stops before exec, so the probe can filter an exact owned PID.
            query = subprocess.Popen(["bash", "-c", 'kill -STOP $$; exec "$@"', "query", *command],
                                     env=env, stdout=output, stderr=error, start_new_session=True)
            stopped(query.pid)
            inode = input_path.stat().st_ino
            if mode == "mmap":
                probe = "kprobe:filemap_map_pages"
                expression = "((struct vm_fault *)arg0)->vma->vm_file->f_inode->i_ino"
            else:
                probe = "kprobe:ext4_file_read_iter"
                expression = "((struct kiocb *)arg0)->ki_filp->f_inode->i_ino"
            program = (f'{probe} /pid == {query.pid}/ {{ if ({expression} == {inode}) {{ '
                       '@seen = @seen + 1; if (@seen == 16) { signal("SIGSTOP"); '
                       'printf("FAULT_READY\\n"); exit(); } } }')
            (case / "probe.bt").write_text(program)
            trace_log = (case / "probe.log").open("w")
            tracer = subprocess.Popen(["sudo", "-n", "timeout", "-s", "INT", "20", "bpftrace", "--unsafe",
                                       "-B", "line", "-e", program], stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, bufsize=0)
            # Attached is emitted before the child is resumed; no arbitrary startup sleep.
            attached = b""
            until = time.monotonic() + 15
            while b"Attached" not in attached and time.monotonic() < until:
                ready, _, _ = select.select([tracer.stdout], [], [], 1)
                if ready:
                    chunk = os.read(tracer.stdout.fileno(), 4096)
                    if not chunk:
                        break
                    attached += chunk
            trace_log.write(attached.decode(errors="replace"))
            assert b"Attached" in attached, "probe did not attach"
            os.kill(query.pid, signal.SIGCONT)
            stopped(query.pid)
            assert tracer.wait(timeout=10) == 0, "probe did not reach its injection point"
            trace_log.write(tracer.stdout.read().decode(errors="replace"))
            trace_log.close()
            before = time.monotonic()
            if fault == "eio":
                sudo("dmsetup", "suspend", "--noflush", name)
                sudo("dmsetup", "reload", name, "--table", f"0 {table.split()[1]} error")
                sudo("dmsetup", "resume", name)
            else:
                # Remove the owned loop's backing bytes, not any physical device.
                os.truncate(image, 0)
            os.kill(query.pid, signal.SIGCONT)
            rc = query.wait(timeout=10)
            elapsed = time.monotonic() - before
        stdout = (case / "stdout").read_text()
        stderr = (case / "stderr").read_text()
        assert rc in (1, 2) and stdout == "", (rc, stdout, stderr)
        if mode != "mmap":
            assert "LocalReadFailed" in stderr and "errno 5" in stderr, stderr
            assert str(input_path) in stderr and "offset" in stderr, stderr
        return {"mode": mode, "fault": fault, "healthy_sum": str(truth), "exit": rc,
                "seconds_after_fault": elapsed, "stdout_bytes": len(stdout), "diagnostic": stderr}
    finally:
        if query is not None and query.poll() is None:
            os.killpg(query.pid, signal.SIGCONT)
            os.killpg(query.pid, signal.SIGKILL)
        # Restore the owned mapper before teardown, including exceptional exits.
        if mapped:
            for action in [("suspend", "--noflush", name), ("reload", name, "--table", table), ("resume", name)]:
                try:
                    sudo("dmsetup", *action)
                except subprocess.SubprocessError:
                    pass
        if query is not None:
            query.wait(timeout=10)
        if tracer is not None:
            tracer.wait(timeout=25)
        if mounted:
            sudo("umount", mount)
        if mapped:
            sudo("dmsetup", "remove", "--retry", name)
        if loop:
            sudo("losetup", "-d", loop)
        image.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--zpq", required=True, type=Path)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--duckdb", required=True, type=Path)
    parser.add_argument("--mode", action="append", choices=("pread", "uring", "mmap"))
    args = parser.parse_args()
    args.work.mkdir()  # Never reuse or format an existing caller directory.
    results = []
    for mode in args.mode or ("pread", "uring", "mmap"):
        for fault in ("eio", "backing-loss"):
            result = one(args.work, args.zpq.resolve(), args.input.resolve(), args.duckdb.resolve(), mode, fault)
            results.append(result)
            (args.work / "results.json").write_text(json.dumps(results, indent=2))
            print(mode, fault, result["exit"], result["seconds_after_fault"], flush=True)


if __name__ == "__main__":
    main()
