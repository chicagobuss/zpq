#!/usr/bin/env python3
"""Exercise automatic build revision discovery through Zig's configuration cache.

Run through zpq-heavy, with TMPDIR on the build disk. The small fixture uses the production revision helper and
installs its generated options without compiling the application. Every check repeats the same build command.
"""

import pathlib
import shutil
import subprocess
import tempfile


BUILD = '''const std = @import("std");
pub fn build(b: *std.Build) void {
    const options = b.addOptions();
    options.addOption([]const u8, "revision", @import("build_revision.zig").current(b));
    b.getInstallStep().dependOn(&b.addInstallFile(options.getOutput(), "revision.zig").step);
}
'''


def command(root, *args):
    result = subprocess.run(args, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert result.returncode == 0, result.stdout
    return result.stdout.strip()


def check(root):
    command(root, "zig", "build", "--prefix", str(root / "out"))
    want = command(root, "git", "describe", "--always", "--dirty", "--exclude=*", "--abbrev=12")
    assert (root / "out/revision.zig").read_text() == f'pub const revision: []const u8 = "{want}";\n'
    return want


def main():
    helper = pathlib.Path(__file__).with_name("build_revision.zig")
    with tempfile.TemporaryDirectory(prefix="zpq-version-") as temp:
        root = pathlib.Path(temp) / "repo"
        root.mkdir()
        (root / "build.zig").write_text(BUILD)
        shutil.copyfile(helper, root / "build_revision.zig")
        (root / ".gitignore").write_text("/out/\n/.zig-cache/\n")
        tracked = root / "tracked"
        tracked.write_text("first\n")
        command(root, "git", "init", "-q")
        command(root, "git", "config", "user.name", "Revision fixture")
        command(root, "git", "config", "user.email", "fixture@example.invalid")
        command(root, "git", "add", ".")
        command(root, "git", "commit", "-qm", "Initial fixture")
        first = check(root)
        assert check(root) == first
        tracked.write_text("second\n")
        assert check(root) == first + "-dirty"
        command(root, "git", "add", "tracked")
        command(root, "git", "commit", "-qm", "Advance fixture")
        second = check(root)
        assert second != first and not second.endswith("-dirty")
        command(root, "git", "pack-refs", "--all", "--prune")
        assert check(root) == second
        command(root, "git", "checkout", "-q", "--detach", first)
        assert check(root) == first
        # A linked worktree has a .git file, with its index and refs in different directories.
        linked = pathlib.Path(temp) / "linked"
        command(root, "git", "worktree", "add", "-q", "--detach", str(linked), second)
        assert check(linked) == second
        (linked / "tracked").write_text("third\n")
        assert check(linked) == second + "-dirty"
        command(linked, "git", "checkout", "--", "tracked")
        assert check(linked) == second
    print("build revision: cached repeat, dirty/clean, new HEAD, packed refs, detached HEAD, linked worktree PASS")


if __name__ == "__main__":
    main()
