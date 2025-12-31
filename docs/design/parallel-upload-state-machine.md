# Parallel TLS Upload State Machine

## Problem Statement

We need to upload N parts of a multipart S3 upload in parallel using xev's event loop.
Each part is 5-8MB. The challenge is managing multiple TLS connections that each need to:
1. Complete TLS handshake
2. Send HTTP request (headers + large body)
3. Receive HTTP response
4. Extract ETag from response

## Current Understanding

### TLS Connection Internals

The `Connection` struct has these key fields:
- `pending_read: bool` - true when a TCP read is in flight
- `pending_write: bool` - true when a TCP write is in flight  
- `handshake_complete: bool` - true after TLS handshake succeeds
- `c_read`, `c_write` - xev completion slots (only ONE of each!)

### The `pump()` Function

```
pump() {
    if (closed) return;
    
    // Try to send any pending TLS data
    if (has_outgoing_tls_data) {
        if (pending_write) return;  // <-- EARLY RETURN, no read scheduled!
        schedule_tcp_write();
        return;
    }
    
    // Only schedule read if not already pending and not idling
    if (!pending_read && !idling) {
        schedule_tcp_read();
    }
}
```

**Critical observation**: When there's outgoing data and a write is pending, 
`pump()` returns early WITHOUT scheduling a read. This means during large 
uploads, we never read server responses until the upload completes.

### The `write()` Function

```
write(data) {
    encrypted = tls.processOutgoing(data);
    if (encrypted) {
        if (pending_write) return WriteInProgress;
        schedule_tcp_write(encrypted);
    }
}
```

The write encrypts data and schedules ONE TCP write. The TLS layer internally
buffers the rest. Subsequent `pump()` calls send more chunks.

## State Machine for Single Upload

```
                    ┌─────────────────┐
                    │     INIT        │
                    └────────┬────────┘
                             │ connect()
                             ▼
                    ┌─────────────────┐
                    │  CONNECTING     │
                    │ (TCP + TLS)     │
                    └────────┬────────┘
                             │ on_connect callback
                             ▼
                    ┌─────────────────┐
                    │   CONNECTED     │
                    └────────┬────────┘
                             │ write(request)
                             ▼
                    ┌─────────────────┐
              ┌────▶│   SENDING       │◀────┐
              │     │ (request data)  │     │
              │     └────────┬────────┘     │
              │              │              │
              │   more data  │  write done  │ more data
              │   to send    │              │ to send
              │              ▼              │
              │     ┌─────────────────┐     │
              └─────│  WRITE_PENDING  │─────┘
                    │ (waiting for    │
                    │  TCP write)     │
                    └────────┬────────┘
                             │ all data sent
                             ▼
                    ┌─────────────────┐
                    │   RECEIVING     │
                    │ (response)      │
                    └────────┬────────┘
                             │ response complete
                             ▼
                    ┌─────────────────┐
                    │     DONE        │
                    └─────────────────┘
```

## The Problem with Large Uploads

When uploading large data (> ~6KB which is one TLS record):

1. `write(100KB_data)` is called
2. TLS encrypts first chunk (~8KB), schedules TCP write
3. `pump()` is called after write completes
4. `pump()` sees more TLS data to send, schedules next write, **returns early**
5. Read is never scheduled
6. Server sends response (maybe error), but we don't read it
7. Server's send buffer fills up
8. Server closes connection (BrokenPipe)

## Solution Options

### Option A: Interleaved Read/Write in pump()

Modify `pump()` to always schedule a read, even when writes are pending:

```zig
fn pump(self: *Self) void {
    if (self.closed) return;
    
    // Always try to schedule a read first (if not pending)
    if (!self.pending_read and !self.idling) {
        schedule_tcp_read();
    }
    
    // Then handle outgoing data
    if (has_outgoing_tls_data and !self.pending_write) {
        schedule_tcp_write();
    }
}
```

**Pros**: Simple change, fixes the root cause
**Cons**: Changes core TLS code, might have unintended effects

### Option B: Chunked Writes with Manual pump()

Instead of one big `write()`, break into chunks and pump between:

```zig
fn uploadChunked(conn, data) {
    const CHUNK_SIZE = 64 * 1024;
    var offset: usize = 0;
    while (offset < data.len) {
        const chunk = data[offset..@min(offset + CHUNK_SIZE, data.len)];
        conn.write(chunk);
        // Run loop to process writes AND reads
        while (conn.pending_write) {
            loop.run(.once);
        }
        offset += chunk.len;
    }
}
```

**Pros**: Doesn't change core TLS code
**Cons**: More complex upload logic, harder to parallelize

### Option C: Separate Read/Write Connections (HTTP/1.1 pipelining issue)

Use connection only for one request at a time, but have multiple connections.
This is what the Orchestrator does for reads.

**Pros**: Simple mental model
**Cons**: More connections, more memory

### Option D: Full-Duplex Aware State Machine

Track read and write states separately, ensure reads are always armed:

```zig
const UploadState = enum {
    connecting,
    sending_request,  // writing, reading armed
    waiting_response, // done writing, reading
    done,
    error,
};
```

## Recommended Solution

**Option A** is the cleanest fix. The `pump()` function should be full-duplex:
- Always arm reads (TCP is full-duplex)
- Send writes when possible
- Let the event loop handle interleaving

This matches how HTTP works: the client sends a request while potentially
receiving data (e.g., early hints, or pipelined responses).

## Implementation Plan

1. Modify `pump()` in `connection.zig` to schedule reads first
2. Test with existing probes to ensure no regression
3. Re-run parallel upload probe
4. Apply to S3Writer

## Key Insight: Loop Termination

`loop.run(.until_done)` runs until there are NO active kernel watchers.
Each pending TCP read/write is a watcher.

**Watcher lifecycle:**
1. `tcp.read()` or `tcp.write()` -> increments active count
2. Callback fires -> decrements active count (handled by xev)
3. Loop checks: if active == 0, stop

**Problem with idling:**
Setting `conn.idling = true` only prevents NEW reads from being scheduled.
If there's already a `pending_read = true`, that watcher stays active!

**How Orchestrator handles this:**
Looking at the code, after response is complete:
1. Sets `idling = true` (no new reads)
2. Returns connection to pool
3. The CURRENT read completes with EOF or timeout
4. No new read is scheduled (idling=true)
5. Loop eventually has no watchers

**The actual fix needed:**
The connection's read callback must handle the "we're done" case:
- When `idling = true` and read returns (even with data), don't re-arm
- Or: explicitly cancel the pending read when done

## Active Watcher Tracking

```
State: CONNECTING
  Watchers: [tcp_connect]
  
State: TLS_HANDSHAKE  
  Watchers: [tcp_read, tcp_write] (interleaved)
  
State: SENDING_REQUEST
  Watchers: [tcp_read, tcp_write]  <- BOTH active (full-duplex)
  
State: WAITING_RESPONSE
  Watchers: [tcp_read]  <- Only read active
  
State: DONE
  Watchers: []  <- Loop can exit
```

## Verification

After fix, the logs should show interleaved read/write:
```
pump: scheduling TCP read      <-- read armed
pump: scheduling TCP write     <-- write armed
internalOnTcpWrite: wrote 8192
pump: scheduling TCP write     <-- more write
wire: read 0 bytes             <-- read completes (no data yet)
pump: scheduling TCP read      <-- re-arm read
internalOnTcpWrite: wrote 8192
...
wire: read 200 bytes           <-- server response starts
```

And proper termination:
```
on_data: response complete, setting idling=true
wire: read returns (current read completes)
pump: idling=true, not scheduling new read
loop: no active watchers, exiting
```
