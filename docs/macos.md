# Checking zpq on macOS

The macOS CLIs are cross-compiled on Linux (`zig build -Dtarget=aarch64-macos -Doptimize=ReleaseFast cli`, likewise
`x86_64-macos`; the build fetches the pinned macOS BoringSSL prebuilts, or uses `vendor/boring_tls/prebuilt/<target>`
when present). CI runs the Apple silicon CLI on a native macOS runner; Intel macOS runtime coverage is explicitly
deferred. The fleet carries broader checks to a real Mac.

Local file I/O goes through `src/local_fs.zig` (`std.posix`, libc on every zpq binary), so the same code serves Linux
and macOS. The one Linux-only fast path kept is the batched `getdents64` directory read behind glob expansion; macOS
lists directories with `std.Io.Dir`. The epoll loop (`src/io/epoll.zig`) stays Linux-only and its tests skip elsewhere.
The binaries declare macOS 15.0 as their minimum (Zig 0.17's default).

## Checking on a Mac

```bash
tools/check/fleet.sh --host MAC          # or --release, which includes hosts the inventory marks manual
```

The fleet script cross-builds the `aarch64-macos` bundle on a Linux builder: `zpq-check` and the ReleaseFast `zpq`
(with `--full`, the Debug unit-test executables too). It fails if `llvm-objdump` finds a raw `svc`/`syscall`
instruction in any of them: macOS code reaches the kernel through libSystem, so one would be a Linux syscall that
survived. On the Mac every case runs answers-only and the answers manifest must equal the Linux reference's, case for
case (`zpq-check manifest-diff`); with `--full` the unit-test executables run under `zpq-check stage` too. A Mac has
no GNU `timeout`, which the runner bounds each CLI point with: the host script supplies `gtimeout` or a Perl alarm.

CI's macos-14 runner runs the shipped Apple silicon CLI through `tools/check/cases/release.json`.
