# Plan: Remote Benchmark Workflow

**Goal**: Establish a robust, automated workflow to run benchmarks and tests on the `oci-josh-arm-vm` (ARM64 Linux) directly from the local development environment.

## Context & Assets
*   **Remote Host**: `oci-josh-arm-vm` (SSH access configured).
*   **Remote Repo**: `~/code/zpq.git` (Bare git repo).
*   **Local Build Step**: `zig build bench-ping-pongs` (verifies libxev performance).
*   **Architecture**: ARM64 Linux (Critical for AWS Lambda/Graviton parity).

## Implementation Plan

### 1. Verification Script (`tools/remote_bench.sh`)
Create a script that orchestrates the entire lifecycle:
1.  **Sync**: Push local changes to the remote `backup` repository.
2.  **Deploy**: SSH into the remote, checkout the latest code to a worktree (non-bare).
3.  **Setup**: Ensure Zig 0.16.x is available on the remote (auto-download if missing?).
    *   *Optimization*: Check for `zig` in PATH. If missing, download nightly tarball to `~/zig` and add to PATH.
4.  **Execute**:
    *   `zig build bench-ping-pongs -Doptimize=ReleaseFast`
    *   `zig build test-io` (optional, for correctness).
5.  **Report**: Stream stdout/stderr back to local terminal.

### 2. Justfile Integration
Add a `remote-bench` recipe to `Justfile`:
```bash
remote-bench:
    ./tools/remote_bench.sh
```

### 3. Execution Steps for Agent
1.  **Create `tools/remote_bench.sh`**:
    *   Use `git push backup main`.
    *   SSH command structure:
        ```bash
        ssh oci-josh-arm-vm "
        mkdir -p ~/code/zpq-work
        # Ensure we have a clean checkout
        if [ ! -d ~/code/zpq-work/.git ]; then
            git clone ~/code/zpq.git ~/code/zpq-work
        else
            cd ~/code/zpq-work && git fetch origin && git reset --hard origin/main
        fi
        cd ~/code/zpq-work
        
        # Check Zig
        if ! command -v zig &> /dev/null; then
            echo 'Installing Zig...'
            # Download and extract logic
        fi
        
        echo 'Running Benchmark...'
        zig build bench-ping-pongs -Doptimize=ReleaseFast
        "
        ```
2.  **Test**: Run `./tools/remote_bench.sh`.
3.  **Refine**: Handle permissions, output formatting.

## Verification
*   Run `just remote-bench`.
*   Expect to see: `info: X roundtrips/s` output originating from the Linux VM.

