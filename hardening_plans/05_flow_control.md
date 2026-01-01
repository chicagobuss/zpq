# Plan 05: Flow Control & Backpressure

## Criticism
**"Connection.zig Has No Backpressure."**
The `pump()` loop writes blindly. Fast producers + slow network = OOM.

## Response
**Valid and High Priority.** This is a classic async I/O bug. `xev.TCP.write` submits a submission queue entry (SQE). If we submit faster than the kernel drains the socket buffer, we consume unbounded SQEs or userspace memory.

## Action Plan

### 1. Implement Write Queue
In `Connection`:
- Add `write_queue: std.fifo.LinearFifo(u8, .Dynamic)`.
- **Logic**:
    - `write(data)` appends to FIFO.
    - If FIFO size > `HIGH_WATER_MARK` (e.g., 64KB), return `error.WouldBlock`.
    - `pump()` pops from FIFO and writes to `xev`.
    - Only schedule *one* `xev.write` at a time. Do not schedule another until the previous callback fires (`onTcpWrite`).

### 2. Handle Backpressure in `AsyncRequest`
Update `AsyncRequest` to handle `error.WouldBlock`.
- If `req.write(...)` returns `WouldBlock`, pause the request state machine.
- Register a `on_drain` callback with the `Connection`.
- Resume request when `Connection` emits `on_drain` (when FIFO drops below low-water mark).

### 3. Test "Slow Loris" Receiver
Create a test case `test_flow_control.zig`:
- Server reads 1 byte per second.
- Client tries to write 10MB.
- Assert client memory usage stays constant (bounded by FIFO size), not linear with write size.

