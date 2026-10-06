# Profiling a query

`zpq query` can record where a query spends its time: a per-thread trace you open in Perfetto, and a summary that
fits in a terminal. The Lambda returns the same summary in its response. Use them to find out which phase,
column or request to work on next. A flamegraph shows which functions are hot; the profile shows which phase, row group
and column chunk each thread was in at a given time, and how the threads overlapped.

```bash
zpq query data.parquet --aggregate 'sum(x) AS s' --profile trace.json      # trace for ui.perfetto.dev
zpq query data.parquet --aggregate 'sum(x) AS s' --profile-summary         # summary on stderr
zpq query data.parquet --aggregate 'sum(x) AS s' --profile-summary=json    # the same, as one JSON line
```

Both flags work with every query shape (aggregate, GROUP BY, `--output` writes, `--format` printing, `--query`) and
with local and S3 inputs. The query's normal output is unchanged: stdout still carries the answer, and the summary
goes to stderr after the usual JSON envelope.

## The trace

`--profile PATH` writes a [Chrome Trace Event](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU)
JSON file. Open it at <https://ui.perfetto.dev> (Open trace file) or load it with Perfetto's trace processor
(`pip install perfetto`). Every thread that did work has its own track (`main`, `scan N`, `fetch N`, `encode N`,
`writer`, `upload N`, `sampler`), and the spans on a track nest.

| Span | Phase | What it covers | Arguments |
|------|-------|----------------|-----------|
| `query` | other | The whole `runQuery`, on the calling thread. | |
| `open_inputs` | open | Opening every input: mmap for local files; footer fetches, range planning and range fetches for S3. Same timestamps as the JSON `read_ms`. | `files` |
| `open_file` | open | One local file's open, stat and mmap. | `file`, `bytes` |
| `footer_batch` | wait | The coordinator waiting for parallel footer work. | `files` |
| `footer_fetch` | footer | One S3 object's footer fetch (tail, head and any footer remainder), on a fetch thread. | `file`, `cache` (0 cold, 1 cached and unchanged, 2 cached but stale) |
| `footer_parse` | footer | Thrift-decoding one footer. | `file`, `bytes` |
| `parse_query` | plan | Parsing the filter, aggregates and keys against the schema. Same timestamps as `parse_ms`. | |
| `plan` | plan | Building the scan's work list and sizing its workers. | `work_items`, `workers`, `row_groups` |
| `plan_ranges` | plan | The S3 range plan: row-group pruning, coalescing, compact-buffer layout. | `ranges` (chunk ranges wanted), `merged` (after coalescing), `fetch_jobs` (requests, after splitting big ranges), `bytes`, `row_groups`, `row_groups_pruned` |
| `fetch` | wait | The coordinator waiting for the range fetches. | `jobs`, `bytes` |
| `http_get`, `http_put` | io | One S3 request, every retry included. | GET: `range_start`, `range_end` (exclusive), `bytes`, `flags` (HTTP status), `attempts`, `reused` (pooled connection), `ttfb_ns` (send to first response byte), `acquire_ns` (pool wait plus connect and TLS), `body_ns` (first byte to last); PUT: `part` in place of the range |
| `scan` | wait | The coordinator waiting for the scan workers. Same timestamps as `decode_wall_ms`. | `workers` |
| `scan_worker` | other | One worker's life. | `worker`, `items`, `user_ns`, `sys_ns`, `minor_faults`, `major_faults` (this thread's CPU and page faults, from `getrusage(RUSAGE_THREAD)`) |
| `worker_release` | other | A worker freeing its decode arena and scratch after its last row group. | `worker` |
| `row_group` | other | One row group (or, when aggregates are split across workers, one share of one). | `file`, `rg`, `decision` (`skip`, `full_match` or `filter`), `rows`, `reason` (the filter column whose statistics skipped it), `columns` (chunks decoded) |
| `prune_pages` | prune | Loading page indexes and pruning pages with the filter. | `pages`, `pages_skipped` (of the decoded chunks) |
| `decode` | decode | Decoding a row group's columns. Same timestamps as `decode_ms`. | `file`, `rg`, `columns` |
| `column_chunk` | decode | One column chunk, page pruning, decompression and decoding included. | `file`, `rg`, `col`, `column`, `types` (codec/encoding/physical type), `bytes_in` (compressed), `bytes_out` (uncompressed) |
| `decompress` | decompress | One page's decompression, inside its `column_chunk`. | `types` (codec/page type/page encoding), `bytes_in`, `bytes_out` |
| `filter` | filter | Evaluating the filter over a decoded row group. With `stats_fold`, the timestamps of `eval_ms`. | `rows`, `selected` |
| `stats_fold` | aggregate | Answering aggregates from statistics. | `answered` |
| `aggregate` | aggregate | Folding selected rows into the accumulators or group table. The timestamps of the aggregate path's `encode_ms`. | `rows`, `calls` |
| `merge`, `finalize` | merge | Merging per-worker results; building the answer. | |
| `reencode` | wait | The write path's dispatcher waiting on its window of row groups. | `row_groups`, `workers`, `window` |
| `encode_rg` | other | One output row group on an encode thread. | `file`, `rg`, `passthrough` (byte copy) |
| `project` | project | Applying the selection and copying values into the output buffers. | `rows`, `columns` |
| `encode`, `encode_chunk` | encode | Encoding (and compressing) a row group's columns, and each column chunk. `encode` has the timestamps of the write path's `encode_ms`. | `col`, `codec`, `bytes_in`, `bytes_out`, `values` |
| `copy_rg` | write | A byte-copied row group. | `bytes` |
| `sink_write` | write | Bytes handed to the output (file, multipart upload, row-group buffer, or stdout for `--format`). | `bytes` |
| `upload_create`, `upload_complete` | io | Starting and completing a multipart upload (a single PUT when the output fits one part). | `parts` |
| `upload_wait` | wait | Waiting for the last parts to finish uploading. | `parts` |
| `footer_write` | write | Writing the output footer. | `bytes` |

Counter tracks: `requests in_flight` and `bytes in_flight` (S3 requests and the bytes they move, stepped exactly where
they change), `buffered bytes` (encoded row groups waiting for the writer), and `rss bytes` (resident memory, sampled
every 2 ms by the `sampler` thread, and at the start and end).

The trace's `otherData` holds the run's metadata: zpq version, build mode, target triple, CPU count, the parallelism
and memory budget the engine sized itself from, whether it ran on Lambda (and the memory tier), the start time, and
the query as typed.

Two gaps: page-level `decompress` spans are recorded on the aggregate scan, where each worker owns a decode scratch;
the write and `--format` paths record whole column chunks, decompression included. Decoding a FIXED_LEN_BYTE_ARRAY
DECIMAL or an INT96 column records no page spans either.

## The summary

`--profile-summary` condenses the trace. Each section answers one question.

```
zpq profile: 13.21 ms wall, 18 threads, 590 events
  cpu user 80.2 ms, sys 31.8 ms (8.5 cores); rss peak 97 MB; 14100 minor faults
  0.4.0 fast x86_64-linux-gnu, 16 cpus, parallelism 16
```

Wall time is the `query` span. `cores` is process CPU over wall: how much of the machine the query used.

```
phase          wall ms   busy ms   crit ms  alone ms    spans
  decompress      ...
```

- **wall**: time at least one thread was in the phase.
- **busy**: the phase's exclusive time (a span's duration minus its children's) summed over threads. For compute
  phases this is CPU time, unless the machine was oversubscribed; for `io` it is time blocked on the network.
- **crit**: the critical path. Every instant of the query's wall time is split across the phases running then, in
  proportion to how many threads ran each, so the column sums to the wall time (with `(idle)`, when no thread was
  doing tracked work) and says where the wall time went when phases overlap. A phase whose `crit` is small next to its
  `busy` ran in parallel with others and made little difference to the wall time.
- **alone**: wall time the phase ran with no other phase running: the serial part, which more threads cannot hide.

Waiting (a coordinator joining its workers, the dispatcher waiting on its window) is neither busy nor on the critical
path. Exclusive time of the container spans (`query`, `scan_worker`, `row_group`, `encode_rg`) is `other`: per-row-group
bookkeeping, thread start-up and the like.

```
top column chunks by time (48 chunks, 70.45 ms in all)
       ms  decomp   in MB  out MB    MB/s  codec/encoding     rg  column
```

The ten costliest column chunks, decode and decompression together; `decomp` is the share spent decompressing, `MB/s`
the uncompressed bytes over the chunk's time.

```
io     local mmap: 1 file(s), 166.7 MB mapped
io     12 GET + 0 PUT, 45.1 MB; avg acquire 1.2 ms, ttfb 23.1 ms, body 40.2 ms
       9 reused / 3 new connections, 0 retries, 0 errors, peak 8 in flight
       coalescing: 48 chunk ranges -> 12 merged (4.0x) -> 12 requests
prune  row groups: 49 total, 39 skipped, 0 full match, 10 filtered
       pages: 0 of 0 skipped by page index; 0 aggregates from stats
       skipped by statistics of: l_shipdate (39)
scan   16 workers: user 75.1 ms, sys 29.9 ms, 13000 minor faults
engine read 0.03 parse 0.12 decode 70.46 (wall 13.53) eval 0.01 ms
       agg 5.88 encode 0.00 ms (the sources of the JSON phase fields)
```

`engine` repeats the sums behind the response's `phase` fields, in nanoseconds rather than whole milliseconds. They
are taken from the very timestamps the engine uses, so they match exactly; `decode` is CPU summed over workers and
`wall` the scan's elapsed time.

`--profile-summary=json` prints the same as one JSON object (`total_ns`, `idle_ns`, `cpu`, `meta`, `phases`,
`top_chunks`, `io`, `prune`, `workers`, `engine`, all times in nanoseconds), for scripts that compare runs.

## On Lambda

Add `"profile": true` to the request: the response gains a `"profile"` member with the JSON summary, a few KB however
large the query (column names are cut to 128 bytes, the query to 512). Add `"profile_url": "s3://bucket/key.json"` to
also upload the full trace there (it implies `profile`); the response then carries
`"profile_trace": {"url": ..., "bytes": N}`, or `{"url": ..., "error": ...}` if the upload failed, which does not fail
the query. See [lambda_requests.md](lambda_requests.md).

## What it costs

Profiling is compiled in by default and switched on per query. Off, a span site costs one branch on a null pointer;
`-Dprofile=false` compiles the sites out. On, a span is two monotonic clock reads (`src/clock.zig`) and a store into the
recording thread's own buffer: no locks or atomics on the hot path. The in-flight counters take one atomic add per S3
request. Events are capped at 2^20 per query (about 72 MB); past that they are counted as dropped, and the summary
says so.

Measured 2026-10-06 on a 16-core Ryzen AI Max+ 395 (`taskset -c 0-15`, the `compare_oct2026` harness and
fixtures, page cache warm, a shared and busy machine, so best-of-runs is the statistic), ReleaseFast, against the same
tree without profiling:

| | engine `total_ms`, all 14 scenarios | process wall, direct exec (B1, B2, LS2, LS4, LZ1) |
|---|---|---|
| Compiled in, off | within 1 ms resolution of the baseline everywhere (best-of ratios 0.90–1.05) | 0.98–1.01× |
| On (`--profile FILE --profile-summary`) | within 1 ms of off (ratios 0.90–1.05) | +0.5–2.4 ms (1.03–1.12×) |

Switched on, a query pays a fixed few hundred microseconds (starting the sampler thread, reading the machine's
limits) plus writing the trace and summary after the query, which is what moves the short queries most; the spans
themselves (about 600–900 per query here) cost too little to show. The Lambda binary grows 2.8% (4.80 to 4.93 MB
stripped, 2.16 to 2.22 MB zipped); a local exec-to-first-poll and exec-to-first-answer test showed no difference
(0.23 ms and 0.54 ms median, both builds, profiling off).

## Reading a trace programmatically

Perfetto's trace processor answers questions the summary does not. Exclusive time per span name:

```python
from perfetto.trace_processor import TraceProcessor
tp = TraceProcessor(trace="trace.json")
for r in tp.query("""
    select s.name, count(*) n, sum(s.dur) / 1e6 incl_ms,
           sum(s.dur - coalesce((select sum(c.dur) from slice c where c.parent_id = s.id), 0)) / 1e6 self_ms
    from slice s group by s.name order by self_ms desc"""):
    print(r.name, r.n, r.incl_ms, r.self_ms)
```

Each worker's timeline, to see thread start-up and stragglers:

```sql
select t.name, s.name, s.ts, s.dur from slice s
  join thread_track tt on s.track_id = tt.id join thread t using (utid)
where s.depth <= 1 order by t.name, s.ts
```

## Walkthrough

TPC-H Q6 over 6M rows of `lineitem` (49 row groups, snappy), the harness's LS2:

```bash
zpq query lineitem_snappy.parquet \
  --aggregate 'count(*) AS n, sum(l_extendedprice * l_discount) AS revenue' \
  --filter "l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24" \
  --profile q6.json --profile-summary
```

```
zpq profile: 21.40 ms wall, 18 threads, 885 events
  cpu user 171.6 ms, sys 37.8 ms (9.8 cores); rss peak 253 MB; 26544 minor faults
  0.4.0 fast x86_64-linux-gnu, 16 cpus, parallelism 16

phase          wall ms   busy ms   crit ms  alone ms    spans
  open            0.01      0.01      0.01      0.01        2
  footer          0.27      0.27      0.27      0.27        1
  plan            0.04      0.04      0.04      0.04        2
  prune           0.15      0.15      0.03      0.01       49
  decompress     12.07     46.28      2.98      0.03      343
  decode         13.56     72.73      5.11      0.65      245
  filter         13.70     90.36      5.96      0.00       49
  aggregate       7.02     10.68      0.72      0.00       98
  merge           0.01      0.01      0.01      0.01        2
  other           8.66     35.58      6.14      4.01       82
  (idle)          0.14                0.14

top column chunks by time (196 chunks, 118.96 ms in all)
       ms  decomp   in MB  out MB    MB/s  codec/encoding     rg  column
     3.21     77%    0.61    0.94     292  SNAPPY/PLAIN        8  l_extendedprice
     3.15      1%    0.09    0.09      28  SNAPPY/DICT        10  l_quantity
     ...
prune  row groups: 49 total, 0 skipped, 0 full match, 49 filtered
scan   16 workers: user 165.6 ms, sys 34.5 ms, 25296 minor faults
```

Reading it top down:

- 9.8 cores busy for 21 ms, and a fifth of the CPU in the kernel: 25k minor page faults in the workers.
- `filter` is the largest busy phase (90 ms of CPU for 6M rows), ahead of decoding. No row group is pruned (the dates
  are spread through every row group), so every row of all four columns is decoded and tested, though about 2%
  survive.
- The DECIMAL dictionary columns `l_quantity` and `l_discount` decode at under 40 MB/s with almost no decompression:
  the time is the conversion to f64, not snappy. `l_extendedprice` is the opposite: three quarters snappy.
- `other` holds 6 ms of the critical path and 4 ms that ran with nothing else. The trace shows where: workers start
  over the first 2 ms of the scan (threads are spawned one after another), and after its last row group each worker
  spends 0.1–5 ms in `worker_release` freeing its decode memory, the last of them ending 2.6 ms after the last row
  group did.

The per-thread view in Perfetto shows the same thing directly: the staircase of `scan_worker` starts, row groups of
the same size taking very different times on different threads, and the `worker_release` tails.
