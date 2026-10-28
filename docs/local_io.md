# Local reads

A local Parquet file is mapped read-only, and by default the decode workers fault its pages in as they reach them. On
Linux an aggregate query can instead read the column chunks it needs explicitly (`src/io/local_reader.zig`):

- The planner's column-chunk ranges (and the page indexes of those chunks) are read into an anonymous buffer laid out
  like the file, row group by row group, ahead of the workers. Reads are rounded out to whole pages, and a row group's
  ranges at most 64 KiB apart are read as one, gap included; otherwise nothing in the file is read or allocated.
  Neighbouring row groups can therefore read the same pages (Arrow writes every row group's page indexes together
  after the data), and the bytes the profile reports count each such read.
- A worker waits on a row group's gate before touching it. Once every work item of a row group is done, the pages
  no other unfinished row group's reads cover go back to the kernel (`MADV_DONTNEED`). A file far bigger than memory
  scans in bounded memory.
- Row groups enter the window one at a time in input order, so a later row group never takes the room an earlier one
  needs. A row group is admitted once its bytes fit beside those already read ahead and not yet released, so the
  reader holds at most the window; one row group bigger than the window is admitted alone, once nothing else is
  outstanding.
- Row groups that statistics, Bloom filters or dictionaries prune are never read. Footers are parsed, and Bloom
  filters and dictionaries probed, through the mapping.

## Backends

| Mode | What reads |
|---|---|
| `mmap` | the mapping, faulted in by the decode workers (also what writes and printed rows always use) |
| `pread` | `ZPQ_IO_THREADS` reader threads, each with blocking `pread` of 1 MiB at a time |
| `uring` | reader threads, each with its own io_uring. Buffered, unless the choice is direct: the explicit `uring` profile, or a cold `auto` read |

`ZPQ_IO_PROFILE` chooses one: `mmap`, `pread`, `uring`, or `auto` (the default).
`uring` is the explicit direct option. With `auto`:

- on Lambda (`AWS_LAMBDA_FUNCTION_NAME` set), or on any OS but Linux: `mmap`;
- when an estimated 80% or more of the bytes this query will read are already in the page cache: `mmap`. The estimate
  is `mincore` over those planned ranges (column chunks and their page indexes), not the rest of the file, so a warm
  repeat of a query that reads a slice of a large file maps. Every page of that range is counted while it is at most
  4 GiB of pages; past that, a stride still estimates the share. Each file is weighted by its planned pages, not by
  how many samples it contributed. A failed `mincore` counts that file's pages as not cached, which is a cold read;
- when less than about 20% of the planned bytes are cached and the largest row group fits the reader: `O_DIRECT`
  io_uring, whether the read is wide or narrow. That leaves the page cache alone. `--cache` (libzpq `"cache": true`)
  opts out and reads through the page cache instead, so the next query on the file runs warm;
- otherwise a query that plans to read at least half the inputs' bytes uses buffered io_uring, and a narrower read
  uses `pread`. Both fill the page cache.

The direct open is a new descriptor (`/proc/self/fd/N` with `O_DIRECT`); the buffered descriptor stays open beside
it. A borrowed or mapped descriptor is never changed with `F_SETFL` and is never closed while a read of it is in
flight. If that open is refused, or a finished direct read returns `EINVAL`, that file is read through the page
cache and the query continues. A short read of the file's last page is finished the same way and the file stays
direct.

The footers are parsed and the query planned before the choice, so pruned row groups and unread columns don't count
toward the share read. `--explain` names the choice before any file is opened (`cold, O_DIRECT where the file
allows it; --cache reads through the page cache`, or `through the page cache (--cache)`). The profile summary says
what the reads actually used, per file. Neither reads column data to decide. A row group the reader cannot hold is
read through the mapping (`over_budget`), including when the profile asked for direct.

Each mode falls back rather than fails:

- `uring` falls back to `pread` when io_uring is unavailable (seccomp in containers and sandboxes, or
  `kernel.io_uring_disabled`);
- a submit that the kernel refuses (`EAGAIN`, `EBUSY`), including a real `io_uring_enter`, is retried a few times.
  SQEs the kernel never consumed are then read with `pread`. SQEs an earlier enter already accepted wait for their
  completions. A failed `pread` fails the read;
- `EINTR` while waiting for a completion is retried without counting. A submitted read waits for its completion.
  It is not read again with `pread` while the ring can still write that buffer. A completion of `-EAGAIN` is
  resubmitted a few times, and then that finished unit is read with `pread`;
- on any error or stop, nothing further is submitted. Reads already in the kernel are reaped until none remain.
  They are reads of a regular file, so they complete. The ring is not closed before that.

No registered buffers are used, so the default `memlock` limit is enough.

## Knobs

| Variable | Default | Meaning |
|---|---|---|
| `ZPQ_IO_PROFILE` | `auto` | `auto`, `mmap`, `pread`, or `uring` (explicit direct) |
| `ZPQ_IO_WINDOW_MIB` | 1024 | bytes read ahead of the workers and not yet released |
| `ZPQ_IO_THREADS` | 8 | reader threads |

`--profile-summary` names the backend a query used, and on a `read mode:` line what `ZPQ_IO_PROFILE` asked for, what was
chosen and, for `auto`, the cached share and the share the query plans to read, plus a `why:` line for that choice.
`--explain` reports the same choice as `io` and `io_reason`. With a reader it adds a line with what the reads actually
went through (reader threads on io_uring and on pread, the latter including any whose ring failed to set up; files
read `O_DIRECT` and through the page cache), the bytes read, how often and how long workers waited on reads, and how
often the reader waited for the window.
