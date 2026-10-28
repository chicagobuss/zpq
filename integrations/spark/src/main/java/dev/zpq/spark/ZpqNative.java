package dev.zpq.spark;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide lazy singleton wrapping one libzpq context per executor JVM.
 *
 * Thread-safe: any number of Spark task threads may invoke {@link #runJson(String)}
 * concurrently on the shared {@code zpq_ctx}. A JVM shutdown hook calls
 * {@code zpq_shutdown}, waits for in-flight calls to drain, and calls {@code zpq_close}.
 */
public final class ZpqNative {
    public static final int ZPQ_OK = 0;
    public static final int ABI_VERSION = 1;

    private static volatile ZpqNative instance;
    private static final Object INIT_LOCK = new Object();

    // Cumulative telemetry across tasks in this JVM (for NDJSON vs native scan measurement)
    private static final AtomicLong TOTAL_NATIVE_NS = new AtomicLong(0);
    private static final AtomicLong TOTAL_PARSE_NS = new AtomicLong(0);
    private static final AtomicLong TOTAL_CALLS = new AtomicLong(0);
    private static final AtomicLong TOTAL_RESPONSE_BYTES = new AtomicLong(0);

    private final Arena libArena;
    private final MethodHandle zpqOpen;
    private final MethodHandle zpqRun;
    private final MethodHandle zpqFree;
    private final MethodHandle zpqShutdown;
    private final MethodHandle zpqClose;
    private final MethodHandle zpqAbiVersion;
    private final MethodHandle zpqStrerror;

    private final Object stateLock = new Object();
    private MemorySegment ctx;
    private int inflight = 0;
    private boolean closing = false;

    private ZpqNative(Path libPath, String maxMemory) {
        try {
            Linker linker = Linker.nativeLinker();
            this.libArena = Arena.ofShared();
            SymbolLookup lib = SymbolLookup.libraryLookup(libPath, this.libArena);

            this.zpqOpen = linker.downcallHandle(
                lib.find("zpq_open").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            this.zpqRun = linker.downcallHandle(
                lib.find("zpq_run").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS
                )
            );
            this.zpqFree = linker.downcallHandle(
                lib.find("zpq_free").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            this.zpqShutdown = linker.downcallHandle(
                lib.find("zpq_shutdown").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            this.zpqClose = linker.downcallHandle(
                lib.find("zpq_close").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            this.zpqAbiVersion = linker.downcallHandle(
                lib.find("zpq_abi_version").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT)
            );
            this.zpqStrerror = linker.downcallHandle(
                lib.find("zpq_strerror").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
            );

            int abi = (int) this.zpqAbiVersion.invokeExact();
            if (abi != ABI_VERSION) {
                throw new IllegalStateException("Unsupported libzpq ABI version: " + abi + " (expected " + ABI_VERSION + ")");
            }

            try (Arena openArena = Arena.ofConfined()) {
                String optsJson = (maxMemory == null || maxMemory.isEmpty())
                    ? "{}"
                    : "{\"max_memory\":" + ZpqJson.quoteString(maxMemory) + "}";
                byte[] optsBytes = (optsJson + "\0").getBytes(StandardCharsets.UTF_8);
                MemorySegment optsSeg = openArena.allocateFrom(ValueLayout.JAVA_BYTE, optsBytes);
                MemorySegment ctxPtr = openArena.allocate(ValueLayout.ADDRESS);
                MemorySegment errPtr = openArena.allocate(ValueLayout.ADDRESS);

                int rc = (int) this.zpqOpen.invokeExact(optsSeg, ctxPtr, errPtr);
                if (rc != ZPQ_OK) {
                    String errMsg = readAndFreeString(errPtr.get(ValueLayout.ADDRESS, 0));
                    throw new RuntimeException("zpq_open failed (" + strerror(rc) + "): " + errMsg);
                }
                this.ctx = ctxPtr.get(ValueLayout.ADDRESS, 0);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "zpq-context-shutdown"));
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException("Failed to initialize libzpq from " + libPath, t);
        }
    }

    public static ZpqNative getInstance(String configuredLibPath, String configuredMaxMemory) {
        ZpqNative cur = instance;
        if (cur != null) {
            return cur;
        }
        synchronized (INIT_LOCK) {
            cur = instance;
            if (cur == null) {
                Path resolved = resolveLibraryPath(configuredLibPath);
                String maxMem = configuredMaxMemory;
                if (maxMem == null || maxMem.isEmpty()) {
                    maxMem = System.getProperty("zpq.context.max_memory",
                        System.getenv().getOrDefault("ZPQ_CONTEXT_MAX_MEMORY", "16GB"));
                }
                cur = new ZpqNative(resolved, maxMem);
                instance = cur;
            }
            return cur;
        }
    }

    private static Path resolveLibraryPath(String configured) {
        String[] candidates = new String[] {
            configured,
            System.getProperty("zpq.lib"),
            System.getenv("ZPQ_LIB"),
            "zig-out/lib/libzpq.so",
            "zig-out/lib/libzpq.so"
        };
        for (String c : candidates) {
            if (c != null && !c.isEmpty()) {
                Path p = Path.of(c).toAbsolutePath();
                if (Files.exists(p)) {
                    return p;
                }
            }
        }
        throw new IllegalStateException(
            "Could not locate libzpq.so; set option('zpq.lib', ...), -Dzpq.lib=..., or ZPQ_LIB env var"
        );
    }

    public String runJson(String requestJson) {
        synchronized (stateLock) {
            if (closing || ctx == null || ctx.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("zpq context is closed");
            }
            inflight++;
        }
        try (Arena callArena = Arena.ofConfined()) {
            byte[] reqBytes = requestJson.getBytes(StandardCharsets.UTF_8);
            MemorySegment reqSeg = callArena.allocateFrom(ValueLayout.JAVA_BYTE, reqBytes);
            MemorySegment outJsonPtr = callArena.allocate(ValueLayout.ADDRESS);
            MemorySegment outLenPtr = callArena.allocate(ValueLayout.JAVA_LONG);

            long t0 = System.nanoTime();
            int rc;
            try {
                rc = (int) zpqRun.invokeExact(ctx, reqSeg, (long) reqBytes.length, outJsonPtr, outLenPtr);
            } catch (Throwable t) {
                throw new RuntimeException("FFM zpq_run invocation failed", t);
            }
            long t1 = System.nanoTime();
            TOTAL_NATIVE_NS.addAndGet(t1 - t0);
            TOTAL_CALLS.incrementAndGet();

            MemorySegment outJson = outJsonPtr.get(ValueLayout.ADDRESS, 0);
            if (rc != ZPQ_OK) {
                String msg = readAndFreeString(outJson);
                throw new RuntimeException("zpq_run failed (" + strerror(rc) + "): " + msg);
            }
            long outLen = outLenPtr.get(ValueLayout.JAVA_LONG, 0);
            TOTAL_RESPONSE_BYTES.addAndGet(outLen);
            try {
                long tp0 = System.nanoTime();
                byte[] respBytes = outJson.reinterpret(outLen).toArray(ValueLayout.JAVA_BYTE);
                String resp = new String(respBytes, StandardCharsets.UTF_8);
                long tp1 = System.nanoTime();
                TOTAL_PARSE_NS.addAndGet(tp1 - tp0);
                return resp;
            } finally {
                freePtr(outJson);
            }
        } finally {
            synchronized (stateLock) {
                inflight--;
                if (inflight == 0) {
                    stateLock.notifyAll();
                }
            }
        }
    }

    public static void addParseNs(long ns) {
        TOTAL_PARSE_NS.addAndGet(ns);
    }

    public static void resetTelemetry() {
        TOTAL_NATIVE_NS.set(0);
        TOTAL_PARSE_NS.set(0);
        TOTAL_CALLS.set(0);
        TOTAL_RESPONSE_BYTES.set(0);
    }

    public static long getTotalNativeNs() {
        return TOTAL_NATIVE_NS.get();
    }

    public static long getTotalParseNs() {
        return TOTAL_PARSE_NS.get();
    }

    public static long getTotalCalls() {
        return TOTAL_CALLS.get();
    }

    public static long getTotalResponseBytes() {
        return TOTAL_RESPONSE_BYTES.get();
    }

    public void close() {
        MemorySegment toClose;
        synchronized (stateLock) {
            if (closing && (ctx == null || ctx.equals(MemorySegment.NULL))) {
                return;
            }
            closing = true;
            if (ctx != null && !ctx.equals(MemorySegment.NULL)) {
                try {
                    zpqShutdown.invokeExact(ctx);
                } catch (Throwable ignored) {
                }
            }
            while (inflight > 0) {
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            toClose = ctx;
            ctx = MemorySegment.NULL;
        }
        if (toClose != null && !toClose.equals(MemorySegment.NULL)) {
            try {
                zpqClose.invokeExact(toClose);
            } catch (Throwable ignored) {
            }
        }
    }

    private void freePtr(MemorySegment ptr) {
        if (ptr != null && !ptr.equals(MemorySegment.NULL)) {
            try {
                zpqFree.invokeExact(ptr);
            } catch (Throwable ignored) {
            }
        }
    }

    private String readAndFreeString(MemorySegment ptr) {
        if (ptr == null || ptr.equals(MemorySegment.NULL)) {
            return "";
        }
        try {
            return ptr.reinterpret(Long.MAX_VALUE).getString(0, StandardCharsets.UTF_8);
        } finally {
            freePtr(ptr);
        }
    }

    private String strerror(int status) {
        try {
            MemorySegment s = (MemorySegment) zpqStrerror.invokeExact(status);
            if (s == null || s.equals(MemorySegment.NULL)) {
                return "status " + status;
            }
            return s.reinterpret(256).getString(0, StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return "status " + status;
        }
    }
}
