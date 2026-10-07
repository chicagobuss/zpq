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
| `uring` | `ZPQ_IO_THREADS` reader threads, each with its own io_uring and 32 `O_DIRECT` reads of 1 MiB in flight |

`ZPQ_IO_PROFILE` chooses one: `mmap`, `pread`, `uring`, or `auto` (the default). With `auto`:

- on Lambda (`AWS_LAMBDA_FUNCTION_NAME` set), or on any OS but Linux: `mmap`;
- when an estimated 80% or more of the inputs' bytes are already in the page cache: `mmap`, since a warm mapping reads
  fastest as it is. The estimate comes from `mincore` over 64 evenly spaced windows of 32 pages per file (every page
  of a file under 2048 pages), so it costs microseconds however big the file;
- otherwise `uring` where the kernel allows an io_uring;
- without one, a query that plans to read at least half of the inputs' bytes (a full or wide scan) uses `mmap`, with
  the planned ranges of the row groups ahead of the workers advised `MADV_WILLNEED` up to the window: blocking `pread`
  loses to the kernel's readahead there. A narrower read uses `pread`.

The footers are parsed and the query planned before the choice, so pruned row groups and unread columns don't count
toward the share read.

Each mode falls back rather than fails:

- `uring` falls back to `pread` when io_uring is unavailable (seccomp in containers and sandboxes, or
  `kernel.io_uring_disabled`);
- a file that refuses `O_DIRECT` (tmpfs, some FUSE file systems) is read through the page cache instead.

No registered buffers are used, so the default `memlock` limit is enough.

## Knobs

| Variable | Default | Meaning |
|---|---|---|
| `ZPQ_IO_PROFILE` | `auto` | `auto`, `mmap`, `pread` or `uring` |
| `ZPQ_IO_WINDOW_MIB` | 1024 | bytes read ahead of the workers and not yet released |
| `ZPQ_IO_THREADS` | 8 | reader threads |

`--profile-summary` names the backend a query used, and on a `read mode:` line what `ZPQ_IO_PROFILE` asked for, what was
chosen and, for `auto`, the cached share and the share the query plans to read. With a reader it adds a line with what
the reads actually went through (reader threads on io_uring and on pread, the latter including any whose ring failed to
set up; files read `O_DIRECT` and through the page cache), the bytes read, how often and how long workers waited on
reads, and how often the reader waited for the window.
