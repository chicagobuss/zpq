# Checking zpq on macOS

The macOS CLIs are cross-compiled on Linux (`zig build -Dtarget=aarch64-macos -Doptimize=ReleaseFast cli`, likewise
`x86_64-macos`; the build fetches the pinned macOS BoringSSL prebuilts, or uses `vendor/boring_tls/prebuilt/<target>`
when present). CI cannot run them, so two scripts carry a check to a real Mac.

Local file I/O goes through `src/local_fs.zig` (`std.posix`, libc on every zpq binary), so the same code serves Linux
and macOS. The one Linux-only fast path kept is the batched `getdents64` directory read behind glob expansion; macOS
lists directories with `std.Io.Dir`. The epoll loop (`src/io/epoll.zig`) stays Linux-only and its tests skip elsewhere.
The binaries declare macOS 15.0 as their minimum (Zig 0.17's default).

## 1. Build the bundle (Linux)

```bash
tools/macos_bundle.sh            # -> zig-out/macos-check/zpq-macos-check-<rev>.tar.gz
```

It builds the arm64 (`aarch64-macos`) ReleaseFast `zpq` and Debug unit-test executables
(`zig build -Dtarget=<t> test-bin` installs `zpq-test` and `zpq-lambda-test` without running them), and fails if
`llvm-objdump` finds a raw `svc`/`syscall` instruction in any of them: macOS code reaches the kernel through libSystem,
so one would be a Linux syscall that survived. The tarball also holds the unit-test fixtures (`ci/fixtures`,
`data/parquet-testing`, plus `data/{benchmark_100mb,nested_edges,bench_types}.parquet` when present), generated smoke
fixtures, and golden CLI outputs recorded by running `macos_check.sh --record` with a native Linux build of the same tree.
Needs zig 0.17.0 and a python with pyarrow (`.venv`). Set
`ZPQ_MAC_TARGETS="aarch64-macos x86_64-macos"` to include Intel. For an explicitly partial CLI smoke handoff,
`ZPQ_MAC_CLI_ONLY=1` omits unit executables and marks `unit_tests=omitted-platform-blocker` in the manifest.
That mode retains the CLI syscall guard and reports missing unit tests as skips; it cannot establish unit-test acceptance.

## 2. Run the check (Mac)

```bash
scp zig-out/macos-check/zpq-macos-check-<rev>.tar.gz <mac-host>:
ssh <mac-host> 'tar -xzf zpq-macos-check-<rev>.tar.gz && zpq-macos-check-<rev>/macos_check.sh'
```

`macos_check.sh` picks the build for `uname -m` (`--arch x86_64` runs the Intel build under Rosetta). It runs both
unit-test executables from the bundle root, failing on any test failure or on more skips than the Linux run had (beyond
the Linux-only epoll tests). Then it runs a CLI smoke: `schema`/`conform`, aggregates over one file, a file list, a
directory glob and a cwd glob, GROUP BY at `-j 1/4/16`, a SQL query, `--format ndjson`/`json` (plus the `jsonl` alias and a closed pipe), footer inspection,
`-o` with every codec, multi-file writes at several `-j`, a computed select, an aggregate write, and two writes that must
fail without leaving a file. Every output is compared with the Linux golden; written files are compared through zpq's
own NDJSON dump, and, when `python3` can import pyarrow and duckdb, independently checked for codec and values
through both readers. It prints one
`PASS`/`FAIL`/`SKIP` line per check and a summary, and exits 1 on any failure.

Options: `--network` also resolves, connects and TLS-handshakes to real S3 with an anonymous request
(`S3_NO_SIGN_REQUEST=1`, passing on S3's refusal); with real credentials in the environment and
`ZPQ_CHECK_S3_URL=s3://bucket/key.parquet` it reads that file too. `--no-python` skips the independent reader checks, `--keep` keeps
the scratch directory.
