import ctypes, signal, sys
lib = ctypes.CDLL(sys.argv[1])
libc = ctypes.CDLL(None, use_errno=True)
SIGPIPE = 13
def disp():
    buf = ctypes.create_string_buffer(256)
    libc.sigaction(SIGPIPE, None, buf)
    h = ctypes.c_void_p.from_buffer(buf).value or 0
    return {0: "SIG_DFL", 1: "SIG_IGN"}.get(h, "handler@%#x" % h)
signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # what a C host has
print("host start:", disp())
a = ctypes.c_void_p(); b = ctypes.c_void_p(); err = ctypes.c_char_p()
lib.zpq_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_char_p)]
lib.zpq_close.argtypes = [ctypes.c_void_p]
print("open A ->", lib.zpq_open(b"{}", ctypes.byref(a), ctypes.byref(err)), disp())
print("open B ->", lib.zpq_open(b"{}", ctypes.byref(b), ctypes.byref(err)), disp())
lib.zpq_close(a)
print("close A, B still open:", disp())
if len(sys.argv) > 2:
    import socket
    s1, s2 = socket.socketpair(); s2.close()
    sys.stdout.flush()
    r = libc.write(s1.fileno(), b"x" * 10, 10)
    r = libc.write(s1.fileno(), b"x" * 10, 10)
    print("survived write to closed peer:", r)
lib.zpq_close(b)
