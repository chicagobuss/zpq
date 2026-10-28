//! The x86-64-v3 release build's startup check: on a CPU without one of the level's features the binary would die
//! with SIGILL somewhere in its first query, so it says which feature is missing and where the compat build is, and
//! exits 3. Compiled in only where the target CPU assumes the features; a baseline build checks nothing.
//! The x86_64 Linux musl build also checks at ELF entry (cpu_start.c), before libc can use compiler-rt's v3 code.
//! This check runs first thing in `main`, including the test hook used on hosts that support every feature.

const std = @import("std");
const builtin = @import("builtin");

pub const exit_code: u8 = 3;

pub const Feature = enum { avx, avx2, bmi1, bmi2, fma, f16c, lzcnt, movbe };

/// Whether this build assumes x86-64-v3, so must check for it.
pub const checks = builtin.cpu.arch == .x86_64 and std.Target.x86.featureSetHasAll(builtin.cpu.features, .{
    .avx, .avx2, .bmi, .bmi2, .fma, .f16c, .lzcnt, .movbe,
});

/// CPUID leaf 1 ECX, leaf 7 EBX, leaf 0x80000001 ECX, and XCR0 (0 when the OS has no XSAVE): the first x86-64-v3
/// feature they lack, in the order the level lists them. AVX counts only when the OS saves the YMM state.
pub fn firstMissing(leaf1_ecx: u32, leaf7_ebx: u32, ext_ecx: u32, xcr0: u32) ?Feature {
    const has = std.EnumArray(Feature, bool).init(.{
        .avx = leaf1_ecx >> 28 & 1 == 1 and leaf1_ecx >> 27 & 1 == 1 and xcr0 & 6 == 6,
        .avx2 = leaf7_ebx >> 5 & 1 == 1,
        .bmi1 = leaf7_ebx >> 3 & 1 == 1,
        .bmi2 = leaf7_ebx >> 8 & 1 == 1,
        .fma = leaf1_ecx >> 12 & 1 == 1,
        .f16c = leaf1_ecx >> 29 & 1 == 1,
        .lzcnt = ext_ecx >> 5 & 1 == 1,
        .movbe = leaf1_ecx >> 22 & 1 == 1,
    });
    for (std.enums.values(Feature)) |f| if (!has.get(f)) return f;
    return null;
}

pub fn message(buf: []u8, f: Feature) []const u8 {
    return std.fmt.bufPrint(buf, "zpq: this CPU lacks {s}, which this x86-64-v3 build needs; " ++
        "use the x86_64-linux-musl-compat archive, which runs on any x86-64\n", .{name(f)}) catch buf;
}

fn name(f: Feature) []const u8 {
    return switch (f) {
        .avx => "AVX",
        .avx2 => "AVX2",
        .bmi1 => "BMI1",
        .bmi2 => "BMI2",
        .fma => "FMA",
        .f16c => "F16C",
        .lzcnt => "LZCNT",
        .movbe => "MOVBE",
    };
}

fn cpuid(leaf: u32) [4]u32 {
    var a: u32 = undefined;
    var b: u32 = undefined;
    var c: u32 = undefined;
    var d: u32 = undefined;
    asm volatile ("cpuid"
        : [a] "={eax}" (a),
          [b] "={ebx}" (b),
          [c] "={ecx}" (c),
          [d] "={edx}" (d),
        : [leaf] "{eax}" (leaf),
          [sub] "{ecx}" (@as(u32, 0)),
    );
    return .{ a, b, c, d };
}

/// The check, first thing in `main`. ZPQ_TEST_CPU_MISSING=<feature> reports that feature missing, so the message
/// and exit status are testable on a CPU that has everything.
pub fn require() void {
    if (comptime !checks) return;
    const hook = if (std.c.getenv("ZPQ_TEST_CPU_MISSING")) |v| std.mem.span(v) else "";
    const forced = std.meta.stringToEnum(Feature, hook);
    const missing = forced orelse blk: {
        const l1 = cpuid(1);
        const l7 = if (cpuid(0)[0] >= 7) cpuid(7) else [4]u32{ 0, 0, 0, 0 };
        const ext = if (cpuid(0x80000000)[0] >= 0x80000001) cpuid(0x80000001) else [4]u32{ 0, 0, 0, 0 };
        var xcr0: u32 = 0;
        if (l1[2] >> 27 & 1 == 1) xcr0 = asm volatile ("xgetbv"
            : [lo] "={eax}" (-> u32),
            : [x] "{ecx}" (@as(u32, 0)),
            : .{ .edx = true });
        break :blk firstMissing(l1[2], l7[1], ext[2], xcr0);
    } orelse return;
    var buf: [160]u8 = undefined;
    const line = message(&buf, missing);
    _ = std.posix.system.write(2, line.ptr, line.len);
    std.process.exit(exit_code);
}

const testing = std.testing;

test "each missing x86-64-v3 feature is named, first by the level's order, and the compat archive offered" {
    const l1: u32 = 1 << 28 | 1 << 27 | 1 << 12 | 1 << 29 | 1 << 22;
    const l7: u32 = 1 << 5 | 1 << 3 | 1 << 8;
    const ext: u32 = 1 << 5;
    try testing.expectEqual(@as(?Feature, null), firstMissing(l1, l7, ext, 6));
    try testing.expectEqual(@as(?Feature, .avx), firstMissing(l1, l7, ext, 0)); // the OS does not save YMM
    try testing.expectEqual(@as(?Feature, .avx2), firstMissing(l1, l7 & ~@as(u32, 1 << 5), ext, 6));
    try testing.expectEqual(@as(?Feature, .bmi1), firstMissing(l1, l7 & ~@as(u32, 1 << 3), ext, 6));
    try testing.expectEqual(@as(?Feature, .lzcnt), firstMissing(l1, l7, 0, 6));
    try testing.expectEqual(@as(?Feature, .movbe), firstMissing(l1 & ~@as(u32, 1 << 22), l7, ext, 6));
    try testing.expectEqual(@as(?Feature, .avx), firstMissing(0, 0, 0, 0));
    var buf: [160]u8 = undefined;
    try testing.expectEqualStrings("zpq: this CPU lacks AVX2, which this x86-64-v3 build needs; use the " ++
        "x86_64-linux-musl-compat archive, which runs on any x86-64\n", message(&buf, .avx2));
    for (std.enums.values(Feature)) |f| try testing.expect(std.mem.indexOf(u8, message(&buf, f), "-compat") != null);
}
