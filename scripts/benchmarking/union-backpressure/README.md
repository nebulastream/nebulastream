# Union benchmarks for watermark gap backpressure

Benchmarks for the cost and benefit of watermark gap backpressure (PR #2145), modelled after the union scenarios in the
review: a 10 s tumbling count per auction id over a union of N origins, on the existing Nexmark data.

| Test | Sources | Measures |
|---|---|---|
| `union_equal_{2,8,32}` | N sources reading `bid_653M.csv` (18.1M rows each), so their watermarks advance alike | the cost of the feature when nothing drifts |
| `union_drift_{2,32}` | N/2 sources reading `bid_653M.csv` and N/2 reading `auction_modified_74M.csv` | the benefit when sources drift apart |

Both files span the same event time (about 100 h), but the auction file has a tenth of the rows, so the auction sources
run ahead in event time.

The tests live here instead of in `nes-systests/` on purpose: tests in the `large` group run in the nightly large-test CI
and tests in the `benchmark` group run in the benchmark CI, and a 32-way union over 18M bids does not belong in either.

## 1. Build the variants

Build `systest` in Release for every commit you want to compare, e.g. `main`, `ls/join-backpressure` and
`ls/join-backpressure-perf`, each in its own worktree. Use the same environment as for any NES build (see the top-level
README; in the development container, run the commands through `docker run` as shown there), and run the benchmark in that
environment as well:

```bash
git worktree add ../nes-main origin/main
cd ../nes-main
cmake -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --target systest -j
```

`main` does not know `worker.max_watermark_gap_in_ms`, so it runs without a gap.

## 2. Get the test data

The tests read `large/nexmark/bid_653M.csv` and `large/nexmark/auction_modified_74M.csv` from the large systest data. In
one of the build directories, enable the large tests and build the `test-data` target:

```bash
cmake -B build-release -DENABLE_LARGE_TESTS=ON
cmake --build build-release --target test-data
```

This downloads all large systest data (several GB) unless your `ExternalData_OBJECT_STORES` cache already has it, and
places it under `build-release/nes-systests/testdata`, which is the data directory for step 3. Configuring with large tests
checks that the machine can back their 72 GiB memory budget; on a smaller machine, add
`-DLARGE_TEST_TOTAL_MEMORY_BYTES=<bytes>`.

## 3. Run

Pass the data directory, an output directory, and each variant as `<label>=<systest binary>[@<max watermark gap in ms>]`:

```bash
THREADS="16 32 64" REPS=3 scripts/benchmarking/union-backpressure/run.sh ../nes-main/build-release/nes-systests/testdata /tmp/union-backpressure \
    main=../nes-main/build-release/nes-systests/systest/systest \
    head-off=../nes-head/build-release/nes-systests/systest/systest@0 \
    head-on=../nes-head/build-release/nes-systests/systest/systest@10000 \
    perf-off=../nes-perf/build-release/nes-systests/systest/systest@0 \
    perf-on=../nes-perf/build-release/nes-systests/systest/systest@10000
```

Runs are interleaved across variants so that drift on the machine affects all of them alike. Every run appends a row to
`<out dir>/results.tsv`; the script prints a median table at the end, which `summarize.py <results.tsv>` reprints. `FAIL`
means the query failed (a wrong result counts) or the worker was killed, usually for running out of memory.

At 16 threads on an i7-13700, `union_equal_32` takes about 100 s and `union_drift_32` about 55 s per run, so a full matrix
of five variants takes a while; use `SCENARIOS` to narrow it down.

Environment variables:

- `THREADS`: worker thread counts to sweep (default `16 23`).
- `REPS`: repetitions per cell (default 3).
- `SCENARIOS`: space-separated subset, e.g. `"union_equal_32 union_drift_32"`.
- `TOTAL_MEMORY`: worker memory budget in bytes (default 32 GiB). The worker allocates it up front, so peak RSS does not
  show the memory saved by backpressure; lower the budget to see which variants fail instead.
- `WRAP`: command prefix, e.g. `WRAP="numactl --cpunodebind=0 --membind=0"` to compare one NUMA node against the whole
  machine.
