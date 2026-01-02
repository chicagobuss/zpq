# Chunked Upload State Machine

## Problem Statement

TLS uploads >130KB fail with `SSLV3_ALERT_BAD_RECORD_MAC` because our TLS layer doesn't interleave reads during large writes. The server sends data (ACKs, alerts) that we don't drain, causing the connection to break.

**Solution**: Write data in 64KB chunks, waiting for each chunk to complete before sending the next. This allows the TLS layer to process incoming data between chunks.

## States

```
┌─────────────┐
│    IDLE     │  Initial state, context created
└──────┬──────┘
       │ connect()
       ▼
┌─────────────┐
│ CONNECTING  │  TCP connect + TLS handshake in progress
└──────┬──────┘
       │ on_connect callback (handshake complete)
       ▼
┌─────────────┐
│  CONNECTED  │  Ready to send application data
└──────┬──────┘
       │ write first chunk
       ▼
┌─────────────┐
│  WRITING    │  Chunk write in progress (pending_write=true)
└──────┬──────┘
       │ on_write_complete callback
       ▼
┌─────────────────────────────────────────────┐
│            CHUNK COMPLETE                    │
│  ┌─────────────────────────────────────┐    │
│  │ if write_offset < data.len:         │    │
│  │   → write next chunk → WRITING      │    │
│  │ else:                               │    │
│  │   → WAITING_RESPONSE                │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
       │
       ▼
┌─────────────┐
│  WAITING    │  All data sent, waiting for HTTP response
│  RESPONSE   │
└──────┬──────┘
       │ on_data callback (response received)
       ▼
┌─────────────┐
│  COMPLETE   │  Got HTTP 200 + ETag, upload succeeded
└─────────────┘

       │ on_error callback (at any state)
       ▼
┌─────────────┐
│   ERROR     │  Upload failed
└─────────────┘
```

## Key Invariants

1. **Never write when pending_write=true**: The TLS Connection enforces this with `error.WriteInProgress`
2. **on_connect may fire while pending_write=true**: The handshake's final write may still be in flight
3. **on_write_complete fires for ALL writes**: Including handshake writes, not just our data chunks

## The Bug We Hit

```
on_connect fired
  → we call write(chunk1)
  → but pending_write=true (handshake write still in flight!)
  → error.WriteInProgress
```

## Solution: Guard Against Pending Writes

The state machine must handle the case where `on_connect` fires but we can't write yet.

**Option A**: Check `pending_write` before writing, retry on next callback
```
on_connect:
  write_started = true
  if can_write():
    write_next_chunk()
  # else: on_write_complete will trigger the first chunk

on_write_complete:
  if not write_started:
    return  # ignore handshake writes
  if can_write() and has_more_data():
    write_next_chunk()
```

**Option B**: Queue writes, let Connection handle it (requires Connection changes)

**Option C**: Add `on_ready_to_write` callback to Connection (cleaner but more invasive)

## Implementation (Option A)

```zig
const UploadContext = struct {
    // ... other fields ...
    write_started: bool = false,
    write_offset: usize = 0,
    
    fn canWrite(self: *UploadContext) bool {
        return !self.conn.pending_write and !self.done;
    }
    
    fn writeNextChunk(self: *UploadContext) void {
        if (!self.canWrite()) return;
        if (self.write_offset >= self.request_data.len) return;
        
        const chunk = self.request_data[self.write_offset..][0..@min(CHUNK_SIZE, remaining)];
        self.conn.write(chunk) catch |err| {
            self.err = err;
            self.done = true;
            return;
        };
        self.write_offset += chunk.len;
    }
};

fn onConnect(ctx: *UploadContext) void {
    ctx.write_started = true;
    ctx.writeNextChunk();  // May do nothing if pending_write=true
}

fn onWriteComplete(ctx: *UploadContext, bytes: usize) void {
    if (!ctx.write_started) return;  // Ignore handshake writes
    ctx.writeNextChunk();  // Write next chunk or do nothing if done
}
```

## Testing Checklist

- [ ] 64KB upload (single chunk) - should work
- [ ] 128KB upload (2 chunks) - should work
- [ ] 1MB upload (16 chunks) - should work
- [ ] 5MB upload (S3 minimum part size) - should work
- [ ] 3 parallel 1MB uploads - should all succeed
- [ ] 10 parallel 5MB uploads - stress test
