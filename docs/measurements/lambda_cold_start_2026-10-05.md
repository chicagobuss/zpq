# Lambda cold starts — 2026-10-05

zpq 0.4.0 against DuckDB and Polars on AWS Lambda, measured on 2026-10-05 in us-west-2. The point of the run is the cold
invocation, the path zpq-lambda is built for; warm figures are recorded at the end for completeness.

## Setup

- Region us-west-2; every function at 3008 MB; inputs and outputs in an S3 bucket in the same region.
- zpq 0.4.0: `zpq-lambda` as a zip on `provided.al2023`, one arm64 and one x86_64 function.
- DuckDB 1.5.6 and Polars 1.44.2: one x86_64 container image each, built on the AWS Python 3.12 Lambda base image.
  The DuckDB image carries its `httpfs` and `aws` extensions, so no invocation downloads an extension. Lambda's
  `Init Duration` for these functions includes starting Python and importing the engine.
- Cold rounds: the function's configuration was touched to force a new sandbox, then each function was invoked once;
  three rounds per scenario. Times are Lambda's `REPORT` line (`Init Duration`, `Duration`, `Max Memory Used`).
- Every aggregate answer was compared across engines and agreed; every Overture output file was opened and checked
  with PyArrow and DuckDB (1,042,690 rows, identical sorted-value digests for all engines).

Inputs:

- B1–B6: a synthetic file of 2,097,152 rows in 16 row groups, 174.8 MB. B1 decodes all 14 columns into sums and
  min/max; B2 sums a 3-column projection; B3 and B4 add `int8 >= -100` (about 89% of rows kept) and `int8 >= 100`
  (about 11%); B5 is count/sum/min/max/avg; B6 filters on equality with a dictionary-encoded string.
- LS1–LS4: TPC-H `lineitem` at scale factor 1, Snappy, 6,001,215 rows in 49 row groups, 207.1 MB. LS1 decodes all 16
  columns; LS2 is TPC-H Q6; LS3 is a Q1-like date filter grouped by two string keys; LS4 is a range on the sorted
  `l_orderkey` that row-group statistics can prune.
- OV: Overture Places, 868 MB, 4,717,270 rows, nested; filter `confidence > 0.9`, project `id, confidence`, write
  Snappy Parquet back to S3.

## Cold: median Init + Duration, ms (3 forced cold starts)

| scenario | zpq arm64 | zpq x86_64 | DuckDB x86_64 | Polars x86_64 |
|---|---:|---:|---:|---:|
| B1 full scan | 22 + 701 | 27 + 667 | 536 + 1,954 | 806 + 1,287 |
| B2 3-column projection | 20 + 368 | 27 + 365 | 668 + 1,431 | 484 + 752 |
| B3 broad filter | 20 + 365 | 27 + 379 | 448 + 1,512 | 478 + 749 |
| B4 selective filter | 21 + 330 | 27 + 373 | 532 + 1,550 | 589 + 800 |
| B5 simple aggregates | 19 + 340 | 26 + 346 | 462 + 1,371 | 668 + 833 |
| B6 string equality | 20 + 255 | 27 + 271 | 518 + 1,153 | 510 + 618 |
| LS1 full scan | 21 + 1,167 | 27 + 1,356 | 505 + 2,811 | 571 + 2,361 |
| LS2 TPC-H Q6 | 20 + 754 | 26 + 838 | 832 + 3,831 | 546 + 944 |
| LS3 Q1-like GROUP BY | 20 + 720 | 26 + 819 | 499 + 3,475 | 603 + 1,651 |
| LS4 key range | 22 + 190 | 26 + 204 | 535 + 762 | 553 + 667 |
| OV filter + write | 18 + 1,170 | 35 + 1,726 | 738 + 4,063 | 530 + 2,889 |

What the table supports:

- Per-scenario median `Init Duration`: zpq 18–22 ms on arm64 and 26–35 ms on x86_64, DuckDB 448–832 ms, Polars
  478–806 ms. Over all 33 cold starts per function, single values ranged 17.8–24.1 ms (zpq arm64), 24.5–34.9 ms
  (zpq x86_64), 308–916 ms (DuckDB) and 408–894 ms (Polars).
- In every scenario zpq's cold Init + Duration was lower than DuckDB's: on x86_64 against x86_64, DuckDB took 2.4×
  (LS1) to 5.6× (B6, LS4) as long.
- Against Polars, zpq x86_64's cold total was lower in every scenario as well, by 1.7× (LS2) to 5.3× (LS4).

## Peak memory: highest Max Memory Used, MB, over all calls

| scenario | zpq arm64 | zpq x86_64 | DuckDB x86_64 | Polars x86_64 |
|---|---:|---:|---:|---:|
| B1 full scan | 255 | 254 | 449 | 547 |
| B2 3-column projection | 73 | 74 | 240 | 269 |
| B3 broad filter | 73 | 74 | 235 | 262 |
| B4 selective filter | 74 | 74 | 237 | 210 |
| B5 simple aggregates | 75 | 74 | 239 | 231 |
| B6 string equality | 52 | 54 | 201 | 172 |
| LS1 full scan | 298 | 298 | 440 | 1,562 |
| LS2 TPC-H Q6 | 96 | 95 | 234 | 251 |
| LS3 Q1-like GROUP BY | 107 | 109 | 244 | 725 |
| LS4 key range | 37 | 38 | 189 | 176 |
| OV filter + write | 467 | 449 | 709 | 420 |

## Warm sandboxes (not the intended use)

The same functions were also invoked warm: ten interleaved rounds per scenario, each call at least 10 s after the
function's previous one (median gap 16.6 s). Median `Duration`, ms; zpq 0.3.2 x86_64 (same configuration) is included
because 0.4.0 changed how connections behave across an idle sandbox.

| scenario | zpq 0.3.2 x86_64 | zpq arm64 | zpq x86_64 | DuckDB x86_64 | Polars x86_64 |
|---|---:|---:|---:|---:|---:|
| B1 full scan | 4,190 | 750 | 766 | 1,619 | 658 |
| B2 3-column projection | 3,531 | 308 | 298 | 1,161 | 215 |
| B3 broad filter | 3,897 | 293 | 354 | 1,206 | 232 |
| B4 selective filter | 3,699 | 271 | 349 | 1,135 | 203 |
| B5 simple aggregates | 4,691 | 306 | 313 | 1,112 | 382 |
| B6 string equality | 4,131 | 213 | 224 | 837 | 153 |
| LS1 full scan | 5,041 | 1,333 | 1,537 | 2,426 | 1,252 |
| LS2 TPC-H Q6 | 4,271 | 618 | 692 | 3,719 | 379 |
| LS3 Q1-like GROUP BY | 4,498 | 669 | 771 | 2,812 | 760 |
| LS4 key range | 3,727 | 91 | 101 | 511 | 205 |
| OV filter + write | 5,694 | 1,388 | 1,854 | 2,887 | 1,593 |

Warm, Polars was faster than zpq x86_64 in 9 of the 11 scenarios. One zpq 0.3.2 call (B2) failed with
`ConcurrencyUnavailable` and is left out of its median. Calls made back to back, with no idle gap, measure something
else: the B1 full scan took 2.1–2.3 s on every engine that way, against 0.66–1.6 s after an idle gap. That is the
per-sandbox network allowance described in
[`../lambda_capabilities.md`](../lambda_capabilities.md#if-a-sandbox-is-reused), not engine speed.
