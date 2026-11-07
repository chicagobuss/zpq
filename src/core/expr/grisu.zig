//! The digits of a FLOAT the way DuckDB 2.0.0 prints it. DuckDB formats with its vendored fmt 6, which runs Grisu3 on
//! the float's own rounding interval and, when Grisu3 cannot prove its digits are the shortest and closest, falls back to
//! a routine that knows only doubles: the shortest digits of the float widened to a double. So most FLOATs print their
//! shortest round-trip digits, and the few Grisu3 declines (every tie between two shortest candidates among them) print the
//! widened value's digits (`-521345.625`, not `-521345.62`). This is that Grisu3, ported step for step, so the two agree
//! on which values it declines.
const std = @import("std");
const testing = std.testing;

const Fp = struct { f: u64, e: i32 };

/// Digits `d` and the power of ten they are scaled by: the value is `d * 10^exp10`.
pub const Digits = struct {
    buf: [20]u8 = undefined,
    len: usize = 0,
    exp10: i32 = 0,

    pub fn slice(self: *const Digits) []const u8 {
        return self.buf[0..self.len];
    }
};

/// 10^k significands, k = -348, -340, ..., 340, normalized to 64 bits.
const pow10_significands = [_]u64{
    0xfa8fd5a0081c0288, 0xbaaee17fa23ebf76, 0x8b16fb203055ac76,
    0xcf42894a5dce35ea, 0x9a6bb0aa55653b2d, 0xe61acf033d1a45df,
    0xab70fe17c79ac6ca, 0xff77b1fcbebcdc4f, 0xbe5691ef416bd60c,
    0x8dd01fad907ffc3c, 0xd3515c2831559a83, 0x9d71ac8fada6c9b5,
    0xea9c227723ee8bcb, 0xaecc49914078536d, 0x823c12795db6ce57,
    0xc21094364dfb5637, 0x9096ea6f3848984f, 0xd77485cb25823ac7,
    0xa086cfcd97bf97f4, 0xef340a98172aace5, 0xb23867fb2a35b28e,
    0x84c8d4dfd2c63f3b, 0xc5dd44271ad3cdba, 0x936b9fcebb25c996,
    0xdbac6c247d62a584, 0xa3ab66580d5fdaf6, 0xf3e2f893dec3f126,
    0xb5b5ada8aaff80b8, 0x87625f056c7c4a8b, 0xc9bcff6034c13053,
    0x964e858c91ba2655, 0xdff9772470297ebd, 0xa6dfbd9fb8e5b88f,
    0xf8a95fcf88747d94, 0xb94470938fa89bcf, 0x8a08f0f8bf0f156b,
    0xcdb02555653131b6, 0x993fe2c6d07b7fac, 0xe45c10c42a2b3b06,
    0xaa242499697392d3, 0xfd87b5f28300ca0e, 0xbce5086492111aeb,
    0x8cbccc096f5088cc, 0xd1b71758e219652c, 0x9c40000000000000,
    0xe8d4a51000000000, 0xad78ebc5ac620000, 0x813f3978f8940984,
    0xc097ce7bc90715b3, 0x8f7e32ce7bea5c70, 0xd5d238a4abe98068,
    0x9f4f2726179a2245, 0xed63a231d4c4fb27, 0xb0de65388cc8ada8,
    0x83c7088e1aab65db, 0xc45d1df942711d9a, 0x924d692ca61be758,
    0xda01ee641a708dea, 0xa26da3999aef774a, 0xf209787bb47d6b85,
    0xb454e4a179dd1877, 0x865b86925b9bc5c2, 0xc83553c5c8965d3d,
    0x952ab45cfa97a0b3, 0xde469fbd99a05fe3, 0xa59bc234db398c25,
    0xf6c69a72a3989f5c, 0xb7dcbf5354e9bece, 0x88fcf317f22241e2,
    0xcc20ce9bd35c78a5, 0x98165af37b2153df, 0xe2a0b5dc971f303a,
    0xa8d9d1535ce3b396, 0xfb9b7cd9a4a7443c, 0xbb764c4ca7a44410,
    0x8bab8eefb6409c1a, 0xd01fef10a657842c, 0x9b10a4e5e9913129,
    0xe7109bfba19c0c9d, 0xac2820d9623bf429, 0x80444b5e7aa7cf85,
    0xbf21e44003acdd2d, 0x8e679c2f5e44ff8f, 0xd433179d9c8cb841,
    0x9e19db92b4e31ba9, 0xeb96bf6ebadf77d9, 0xaf87023b9bf0ee6b,
};

/// The binary exponents that go with `pow10_significands`.
const pow10_exponents = [_]i16{
    -1220, -1193, -1166, -1140, -1113, -1087, -1060, -1034, -1007, -980, -954,
    -927, -901, -874, -847, -821, -794, -768, -741, -715, -688, -661,
    -635, -608, -582, -555, -529, -502, -475, -449, -422, -396, -369,
    -343, -316, -289, -263, -236, -210, -183, -157, -130, -103, -77,
    -50, -24, 3, 30, 56, 83, 109, 136, 162, 189, 216,
    242, 269, 295, 322, 348, 375, 402, 428, 455, 481, 508,
    534, 561, 588, 614, 641, 667, 694, 720, 747, 774, 800,
    827, 853, 880, 907, 933, 960, 986, 1013, 1039, 1066,
};

const powers_of_10_64 = blk: {
    var t: [20]u64 = undefined;
    t[0] = 1;
    for (1..20) |i| t[i] = t[i - 1] * 10;
    break :blk t;
};

const implicit_bit: u64 = 1 << 52;

/// `lhs * rhs / 2^64`, rounded half up.
fn multiply(lhs: u64, rhs: u64) u64 {
    const product = @as(u128, lhs) * rhs;
    const hi: u64 = @truncate(product >> 64);
    return if (@as(u64, @truncate(product)) & (1 << 63) != 0) hi + 1 else hi;
}

/// Shift the 53-bit significand to the top of 64 bits.
fn normalize(v: Fp) Fp {
    var r = v;
    while (r.f & implicit_bit == 0) {
        r.f <<= 1;
        r.e -= 1;
    }
    r.f <<= 11;
    r.e -= 11;
    return r;
}

/// The cached power of ten whose binary exponent is within 28 of `min_exponent`, and its decimal exponent.
fn cachedPower(min_exponent: i32, pow10_exponent: *i32) Fp {
    const one_over_log2_10: i64 = 0x4d104d42;
    var index: i64 = ((@as(i64, min_exponent) + 63) * one_over_log2_10 + ((1 << 32) - 1)) >> 32;
    index = @divTrunc(index + 347, 8) + 1;
    pow10_exponent.* = @intCast(-348 + index * 8);
    const i: usize = @intCast(index);
    return .{ .f = pow10_significands[i], .e = pow10_exponents[i] };
}

fn countDigits(n: u32) u6 {
    var d: u6 = 1;
    var p: u64 = 10;
    while (p <= n) : (p *= 10) d += 1;
    return d;
}

/// Grisu3's round_weed: pull the last digit toward the value while that stays inside the interval, then accept
/// only if the result is provably the closest.
const Handler = struct {
    buf: *[20]u8,
    size: usize = 0,
    diff: u64,

    const Result = enum { more, done, err };

    fn round(self: *Handler, d: u64, divisor: u64, remainder: *u64, err_bound: u64) void {
        while (remainder.* < d and err_bound -% remainder.* >= divisor and
            (remainder.* +% divisor < d or d -% remainder.* >= remainder.* +% divisor -% d))
        {
            self.buf[self.size - 1] -= 1;
            remainder.* +%= divisor;
        }
    }

    fn onDigit(self: *Handler, digit: u8, divisor: u64, remainder_in: u64, err_bound: u64, exp: i32, integral: bool) Result {
        var remainder = remainder_in;
        self.buf[self.size] = digit;
        self.size += 1;
        if (remainder >= err_bound) return .more;
        const unit: u64 = if (integral) 1 else powers_of_10_64[@intCast(-exp)];
        const up = (self.diff -% 1) *% unit;
        self.round(up, divisor, &remainder, err_bound);
        const down = (self.diff +% 1) *% unit;
        if (remainder < down and err_bound -% remainder >= divisor and
            (remainder +% divisor < down or down -% remainder > remainder +% divisor -% down)) return .err;
        return if (2 *% unit <= remainder and remainder <= err_bound -% 4 *% unit) .done else .err;
    }
};

/// Grisu3's digit generation from the upper boundary; `exp` ends as the decimal exponent of the last digit, scaled.
fn genDigits(value: Fp, err_in: u64, exp: *i32, handler: *Handler) Handler.Result {
    const shift: u6 = @intCast(-value.e);
    const one_f: u64 = @as(u64, 1) << shift;
    var integral: u32 = @intCast(value.f >> shift);
    var fractional: u64 = value.f & (one_f - 1);
    var error_bound = err_in;
    exp.* = countDigits(integral);
    while (true) {
        var digit: u32 = undefined;
        const divisor: u32 = @intCast(powers_of_10_64[@intCast(exp.* - 1)]);
        digit = integral / divisor;
        integral %= divisor;
        exp.* -= 1;
        const remainder = (@as(u64, integral) << shift) + fractional;
        const r = handler.onDigit(@intCast('0' + digit), powers_of_10_64[@intCast(exp.*)] << shift, remainder, error_bound, exp.*, true);
        if (r != .more) return r;
        if (exp.* == 0) break;
    }
    while (true) {
        fractional *%= 10;
        error_bound *%= 10;
        const digit: u8 = @intCast(fractional >> shift);
        fractional &= one_f - 1;
        exp.* -= 1;
        const r = handler.onDigit('0' + digit, one_f, fractional, error_bound, exp.*, false);
        if (r != .more) return r;
    }
}

/// The digits DuckDB's Grisu3 settles on for the positive, finite FLOAT `x`, or null where it declines and fmt's double
/// fallback runs instead.
pub fn shortest32(x: f32) ?Digits {
    std.debug.assert(x > 0 and std.math.isFinite(x));
    const bits: u64 = @bitCast(@as(f64, x));
    var f = bits & (implicit_bit - 1);
    var biased_e: i32 = @intCast((bits >> 52) & 0x7ff);
    if (biased_e != 0) f += implicit_bit else biased_e = 1;
    const e: i32 = biased_e - 1075;
    // The float's rounding interval, in the double's significand units.
    const min_normal_e: i32 = -125 - 53;
    var half_ulp: u64 = 1 << 28;
    if (min_normal_e > e) half_ulp <<= @intCast(min_normal_e - e);
    const upper0 = normalize(.{ .f = f + half_ulp, .e = e });
    const lower_f = f - (half_ulp >> @as(u6, if (f == implicit_bit and e > min_normal_e) 1 else 0));
    var lower: u64 = lower_f << @intCast(e - upper0.e);
    var upper = upper0.f;
    const scaled = normalize(.{ .f = f, .e = e });
    var cached_exp10: i32 = 0;
    const cached = cachedPower(-60 - (scaled.e + 64), &cached_exp10);
    const value: Fp = .{ .f = multiply(scaled.f, cached.f), .e = scaled.e + cached.e + 64 };
    lower = multiply(lower, cached.f) - 1;
    upper = multiply(upper, cached.f) + 1;
    var out: Digits = .{};
    var handler: Handler = .{ .buf = &out.buf, .diff = upper - value.f };
    var exp: i32 = 0;
    if (genDigits(.{ .f = upper, .e = value.e }, upper - lower, &exp, &handler) != .done) return null;
    out.len = handler.size;
    out.exp10 = exp - cached_exp10;
    return out;
}

fn expectRender(x: f32, want: ?[]const u8, want_exp10: i32) !void {
    const d = shortest32(x);
    if (want == null) return testing.expect(d == null);
    try testing.expectEqualStrings(want.?, d.?.slice());
    try testing.expectEqual(want_exp10, d.?.exp10);
}

test "Grisu3 on a float is DuckDB's" {
    try expectRender(0.1, "1", -1);
    try expectRender(1.5, "15", -1);
    try expectRender(16777216.0, "16777216", 0);
    try expectRender(3.4028235e38, "34028235", 31);
    // Ties between two shortest candidates: declined, so DuckDB prints the double's digits.
    try expectRender(521345.625, null, 0);
    try expectRender(1564322.25, null, 0);
    try expectRender(2215130.75, null, 0);
}

test "Grisu3 agrees with the shortest digits wherever it answers" {
    var prng = std.Random.DefaultPrng.init(0x6772697375);
    const rand = prng.random();
    var declined: usize = 0;
    const n = 200_000;
    for (0..n) |_| {
        const x: f32 = @bitCast(rand.int(u32) & 0x7fff_ffff);
        if (!std.math.isFinite(x) or x == 0) continue;
        const d = shortest32(x) orelse {
            declined += 1;
            continue;
        };
        var buf: [128]u8 = undefined;
        const sci = std.fmt.float.render(&buf, x, .{ .mode = .scientific }) catch unreachable;
        const e_at = std.mem.indexOfScalar(u8, sci, 'e').?;
        var digits: [16]u8 = undefined;
        var len: usize = 0;
        for (sci[0..e_at]) |ch| {
            if (ch != '.') {
                digits[len] = ch;
                len += 1;
            }
        }
        const sci_exp = try std.fmt.parseInt(i32, sci[e_at + 1 ..], 10);
        try testing.expectEqualStrings(digits[0..len], d.slice());
        try testing.expectEqual(sci_exp, d.exp10 + @as(i32, @intCast(d.len)) - 1);
    }
    // Grisu3 declines a small share, and the fallback must be exercised.
    try testing.expect(declined > 0 and declined < n / 20);
}
