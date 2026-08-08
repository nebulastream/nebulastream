# Audio `ARRAY_AGG` benchmark

This benchmark reproduces the audio-window aggregation from the repository-root `topology.yaml` with a pre-generated CSV source. It
uses a one-second sliding window advancing every 10 ms (100 windows per second) and compares the existing tuple-sorting `ARRAY_AGG`
against `ARRAY_AGG_PAGE_SORTED` without writing the aggregated audio payload to disk.

Generate the deterministic 1 GB, 44.1 kHz input once:

```sh
python3 benchmarks/array-agg-audio/generate_audio_csv.py
```

Run the CLI separately with `topology-baseline.yaml` and `topology-page-sorted.yaml`. Do not run both queries concurrently: separate
runs keep file parsing and aggregation costs comparable. The generated CSV contains `(FLOAT64 sample, UINT64 timestamp_ns)` rows and
is intentionally not committed.

Validate result ordering on a bounded two-second input with the same 10 ms slide, eight workers, Release build, and compiler backend:

```sh
python3 benchmarks/array-agg-audio/validate_order.py
```

The validation does not use the order-insensitive `Checksum` sink. It writes the complete arrays for both implementations, compares
each window byte-for-byte by `(start, end)`, and independently reconstructs the expected ordered `FLOAT32` bytes from the source CSV.
The bounded input keeps the validation artifacts small enough to inspect; benchmark timing continues to use the 1 GB input and void
sink.

On 2026-08-08, the default validation completed successfully for 200 windows and 26,548,200 ordered array bytes per implementation.

For a local end-to-end comparison, build `nes-repl-embedded` in Release mode and feed it `baseline.sql` or `page-sorted.sql`:

```sh
MOLD_JOBS=1 cmake --build cmake-build-release --target nes-repl-embedded -j
cmake-build-release/nes-frontend/apps/nes-repl-embedded \
  --on-exit WAIT_FOR_QUERY_TERMINATION \
  --error-behaviour FAIL_FAST \
  -- \
  --worker.query_engine.number_of_worker_threads=8 \
  --worker.total_memory_in_bytes=2147483648 \
  --worker.default_query_execution.execution_mode=COMPILER \
  < benchmarks/array-agg-audio/baseline.sql
```

## Results

Measured on 2026-08-08 with the 1,000,000,008-byte input, a Release build (`-O3 -DNDEBUG`), eight worker threads, and compiler
execution. Each implementation was run twice at each slide rate:

| slide rate | implementation | run 1 | run 2 | mean | input throughput |
| --- | --- | ---: | ---: | ---: | ---: |
| 10/s (100 ms) | `ARRAY_AGG` | 9.746 s | 9.741 s | 9.744 s | 102.63 MB/s |
| 10/s (100 ms) | `ARRAY_AGG_PAGE_SORTED` | 1.590 s | 1.482 s | 1.536 s | 651.04 MB/s |
| 100/s (10 ms) | `ARRAY_AGG` | 91.806 s | 91.318 s | 91.562 s | 10.92 MB/s |
| 100/s (10 ms) | `ARRAY_AGG_PAGE_SORTED` | 4.173 s | 3.951 s | 4.062 s | 246.18 MB/s |

At 10 slides per second, page-sorted was 6.34x faster and reduced wall time by 84.2%. At 100 slides per second, it was 22.54x
faster and reduced wall time by 95.6%. Increasing the slide rate by 10x made `ARRAY_AGG` 9.40x slower, while
`ARRAY_AGG_PAGE_SORTED` was 2.64x slower.

For historical context, the earlier one-thread tumbling-window comparison measured 16.268 s for `ARRAY_AGG` and 5.957 s for
`ARRAY_AGG_PAGE_SORTED` (2.73x faster).
