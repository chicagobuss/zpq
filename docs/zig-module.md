# Using zpq as a Zig module

Moved from the README for 0.5.0. The C library is [libzpq.md](libzpq.md).

The engine is consumable as a Zig module: the same sans-IO core the binaries use, minus the SQL frontend and its C
parser (libzstd is still built from C, and BoringSSL is linked from the pinned prebuilts):

```zig
// build.zig.zon
.zpq = .{ .url = "...", .hash = "..." },

// build.zig
const zpq = b.dependency("zpq", .{ .target = target, .optimize = optimize })
    .module("zpq");
exe.root_module.addImport("zpq", zpq);
```

`@import("zpq")` exposes `core.*` (schema, thrift, scan, filter, expr, writer) and `io.*` (S3, SigV4, TLS, epoll loop).
