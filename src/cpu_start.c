/* The ELF entry runs before libc: even __init_libc can call the v3 compiler-rt memset. */
__asm__(".text\n"
        ".global zpq_cpu_start\n"
        ".type zpq_cpu_start,@function\n"
        "zpq_cpu_start:\n"
        "push %rdx\n"
        "sub $8,%rsp\n"
        "call zpq_cpu_check\n"
        "add $8,%rsp\n"
        "pop %rdx\n"
        "jmp _start\n"
        ".size zpq_cpu_start,.-zpq_cpu_start\n");

static unsigned cpuid(unsigned leaf, unsigned *b, unsigned *c) {
    unsigned a, d;
    __asm__ volatile("cpuid" : "=a"(a), "=b"(*b), "=c"(*c), "=d"(d) : "a"(leaf), "c"(0));
    return a;
}

/* No libc, TLS, allocator, compiler-rt or process teardown is safe before this check. */
__attribute__((noreturn)) static void missing(const char *line, unsigned long len) {
    while (len) {
        long n;
        __asm__ volatile("syscall" : "=a"(n) : "a"(1L), "D"(2L), "S"(line), "d"(len)
                         : "rcx", "r11", "memory");
        if (n == -4) continue; /* EINTR */
        if (n <= 0) break;
        line += n;
        len -= (unsigned long)n;
    }
    __asm__ volatile("syscall" : : "a"(60L), "D"(3L) : "rcx", "r11", "memory");
    __builtin_unreachable();
}

#define REQUIRE(ok, feature) do { \
    if (!(ok)) { \
        static const char line[] = "zpq: this CPU lacks " feature \
            ", which this x86-64-v3 build needs; use the x86_64-linux-musl-compat archive, " \
            "which runs on any x86-64\n"; \
        missing(line, sizeof(line) - 1); \
    } \
} while (0)

__attribute__((used)) static void zpq_cpu_check(void) {
    unsigned b, c;
    unsigned max = cpuid(0, &b, &c);
    cpuid(1, &b, &c);
    unsigned leaf1 = c;
    unsigned xcr0 = 0;
    if ((leaf1 & (1u << 27)) != 0) {
        unsigned hi;
        __asm__ volatile("xgetbv" : "=a"(xcr0), "=d"(hi) : "c"(0));
    }
    REQUIRE((leaf1 & (1u << 28)) && (xcr0 & 6) == 6, "AVX");
    b = 0;
    if (max >= 7) cpuid(7, &b, &c);
    unsigned leaf7 = b;
    REQUIRE(leaf7 & (1u << 5), "AVX2");
    REQUIRE(leaf7 & (1u << 3), "BMI1");
    REQUIRE(leaf7 & (1u << 8), "BMI2");
    REQUIRE(leaf1 & (1u << 12), "FMA");
    REQUIRE(leaf1 & (1u << 29), "F16C");
    unsigned ext = 0;
    if (cpuid(0x80000000u, &b, &c) >= 0x80000001u) {
        cpuid(0x80000001u, &b, &c);
        ext = c;
    }
    REQUIRE(ext & (1u << 5), "LZCNT");
    REQUIRE(leaf1 & (1u << 22), "MOVBE");
}
