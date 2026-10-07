# Local reads

A local Parquet file is mapped read-only, and by default the decode workers fault its pages in as they reach them. On
Linux an aggregate query can instead read the column chunks it needs explicitly (`src/io/local_reader.zig`):

- The planner's column-chunk ranges (and the page indexes of those chunks) are read into an anonymous buffer laid out
  like the file, row group by row group, ahead of the workers. Nothing else in the file is read or allocated.
- A worker waits on a row group's gate before touching it. Once every work item of a row group is done, its pages go
  back to the kernel (`MADV_DONTNEED`), so the reader holds at most a window of bytes read ahead and not yet scanned.
  A file far bigger than memory scans in bounded memory.
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
- when at least 80% of the inputs' pages are already in the page cache (`mincore`): `mmap`, since a warm mapping reads
  fastest as it is;
- otherwise `uring` where the kernel allows an io_uring, else `pread`.

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

`--profile-summary` names the backend a query used. With a reader it adds a line with the bytes read, how often and
how long workers waited on reads, and how often the reader waited for the window.
