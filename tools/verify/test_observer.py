#!/usr/bin/env python3
"""Forward Zig's test protocol unchanged, recording named skips on stderr."""
import json
import struct
import subprocess
import sys


def read_exact(stream, size):
    data = bytearray()
    while len(data) < size:
        chunk = stream.read(size - len(data))
        if not chunk:
            if not data:
                return None
            raise EOFError("truncated Zig test protocol")
        data.extend(chunk)
    return bytes(data)


def observe(argv, output=sys.stdout.buffer, errors=sys.stderr):
    child = subprocess.Popen(argv, stdout=subprocess.PIPE)
    names = []
    try:
        while (header := read_exact(child.stdout, 8)) is not None:
            tag, length = struct.unpack("<II", header)
            body = read_exact(child.stdout, length) if length else b""
            if body is None:
                raise EOFError("missing Zig test message body")
            if tag == 3:  # std.zig.Server.Message.TestMetadata
                strings_len, count = struct.unpack_from("<II", body)
                offsets = struct.unpack_from(f"<{count}I", body, 8)
                strings = body[8 + count * 8:]
                if len(strings) != strings_len:
                    raise ValueError("invalid Zig test metadata")
                names = [strings[offset:].split(b"\0", 1)[0].decode() for offset in offsets]
            elif tag == 4:  # TestResults: u32 index, packed u64 flags aligned to 4
                index, flags = struct.unpack_from("<IQ", body)
                if flags & 3 == 2:
                    print("ZPQ_TEST_SKIP " + json.dumps({"test": names[index], "binary": argv[0]}),
                          file=errors, flush=True)
            output.write(header + body)
            output.flush()
        return child.wait()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


if __name__ == "__main__":
    sys.exit(observe(sys.argv[1:]))
