#!/usr/bin/env python3
"""Run zpq-lambda under qemu-user against a stand-in for the Lambda runtime API and log the code it runs.

    trace.py <qemu> <zpq-lambda> <log> [<event.json> [<count>]]

Without an event, the bootstrap is stopped at its first `/next` (the code Init runs); with one, it answers that event
`count` times (default 2: the cold first request, then a warm one) and is stopped at the next `/next`. `qemu -d in_asm` logs each block of code the
first time it runs: tools/lambda/layout.py reads that.
"""
import os, signal, socket, subprocess, sys, threading, time

qemu, binary, log = sys.argv[1:4]
event = open(sys.argv[4], "rb").read() if len(sys.argv) > 4 else None
count = int(sys.argv[5]) if len(sys.argv) > 5 else 2
state = {"polls": 0, "answered": threading.Event()}

def read_request(c):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = c.recv(65536)
        if not chunk: return data
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    n = next((int(l.split(b":")[1]) for l in head.split(b"\r\n") if l.lower().startswith(b"content-length:")), 0)
    while len(body) < n: body += c.recv(65536)
    return head

def serve(server):
    while True:
        c, _ = server.accept()
        head = read_request(c)
        if head.startswith(b"GET"):
            state["polls"] += 1
            if event is None or state["polls"] > count:
                state["answered"].set(); continue  # leave it waiting, as Lambda would
            deadline = str(int(time.time() * 1000) + 60000).encode()
            c.sendall(b"HTTP/1.1 200 OK\r\nLambda-Runtime-Aws-Request-Id: layout\r\nLambda-Runtime-Deadline-Ms: " +
                      deadline + b"\r\nContent-Length: " + str(len(event)).encode() + b"\r\n\r\n" + event)
        else:
            c.sendall(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
        c.close()

server = socket.socket()
server.bind(("127.0.0.1", 0)); server.listen(8)
threading.Thread(target=serve, args=(server,), daemon=True).start()
env = {k: v for k, v in os.environ.items() if k.startswith("AWS_") and k != "AWS_PROFILE"}
env.update(PATH="/usr/bin:/bin", AWS_LAMBDA_RUNTIME_API=f"127.0.0.1:{server.getsockname()[1]}",
           AWS_REGION=os.environ.get("AWS_REGION", "us-west-2"), AWS_LAMBDA_FUNCTION_MEMORY_SIZE="3008")
p = subprocess.Popen([qemu, "-cpu", "max", "-d", "in_asm", "-D", log, binary], env=env, start_new_session=True)
ok = state["answered"].wait(120)
time.sleep(0.2)  # qemu writes its log as it goes; let it catch up before the stop
os.killpg(p.pid, signal.SIGTERM)
p.wait()
sys.exit(0 if ok else "zpq-lambda never polled again")
