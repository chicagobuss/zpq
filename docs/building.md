# Building zpq

Moved from the README for 0.5.0: toolchain, recipes, release archives and a local Homebrew rehearsal.

Requires Zig 0.17.0 (the release, not master), pinned in `build.zig.zon`. On a fresh machine, `just bootstrap` installs
the pinned toolchain to `~/.zvm/0.17.0` and warms the build. The first build fetches prebuilt BoringSSL artifacts from
R2 and checks them against the sha256 digests in `vendor/boring_tls/prebuilt.sha256`; `just fetch-deps` stores them in
`vendor/boring_tls/prebuilt/` for offline builds. No source builds. No system OpenSSL.

```bash
just build           # ReleaseFast — both binaries
just check           # the commit gate: fixtures, unit tests, every case against DuckDB
just full            # the full gate (tools/check/README.md)
just test-integration  # Lambda integration tests
just lambda-build    # static musl Lambda zips (ReleaseFast), both archs
just lambda-deploy zpq-filter-s3 x86_64   # push to AWS
```

`zpq --version` reports the release the binary was built from.

The CLI is `zig-out/bin/zpq`; the Lambda bootstrap is `zig-out/bin/zpq-lambda`. Tagged releases ship the CLI for Linux
and macOS, each x86_64 and arm64, as `zpq-<target>.tar.gz` (plus a `.sha256` and `release.json`; Linux also as `.deb`
and `.rpm`) on the GitHub Release, where `releases/latest/download/zpq-<target>.tar.gz` always names the newest, and
at `https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev/releases/latest/<asset>` (or `releases/v<version>/`).
Linux x86_64 builds target x86-64-v3 (AVX2); the -compat archive runs on any x86-64.
An archive holds the binary, the man page, completions, `tiny.parquet` and the guides under
`share/doc/zpq/`. `python3 tools/package_release.py --tag v0.5.0-rc2` builds a release candidate: the tag's suffix
goes into `zpq --version`, the binary's `created_by` and `release.json` (`zig build
-Dprerelease=rc2` does the same for a local build). A final tag (`v0.5.0`) builds exactly as without `--tag`.

For a private local Homebrew rehearsal, pass the release manifest as the generator's positional argument.
Keep the tarballs beside that manifest and use their directory as the local URL prefix:

```bash
python3 tools/homebrew_formula.py /absolute/path/to/releases/release.json \
  --base-url file:///absolute/path/to/releases --out local-tap/Formula/zpq.rb
```
