# Out-of-process Python UDFs: experiment log

A running record of every performance experiment on the out-of-process UDF path. Check the index before measuring something,
and add an entry for every new experiment.

## Rules

1. Commit the code under test first. `harness/run_experiment.py` refuses to run with uncommitted changes and stores the commit
   in every result record.
2. Give the experiment the next id (`E21`, ...) and pass it as `--experiment`. The raw records go to `results/<id>.jsonl`, one
   JSON line per run with the commit, the full command line, the layout and the measurements.
3. Add an entry below with: date, commit, the exact command, anything not visible in the command (inputs, machine state), the
   results and what was concluded. Add a line to the index.
4. Always use the single-node systest topology (`harness/single-node.yaml`). The default two-node topology adds a network hop
   and cannot run SHARED_MEMORY_ZERO_COPY for chained UDFs.

Entries E01 to E19 were written after the fact on 2026-11-10 from the session's notes. Their commits are the ones the code had
at the time; where the measured tree had uncommitted changes, the entry names the commit those changes ended up in. Their raw
results are in `results/history/` and have no commit field.

## Setup

- Machine: KVM guest, 2 sockets, AMD EPYC 7742, 128 hardware threads, 8 NUMA nodes of 8 cores / 16 hardware threads each
  (node n has CPUs 16n to 16n+15; even CPU = first thread of a core, the next odd CPU = its sibling). Nodes 0-3 are socket 0.
  About 126 GB per node. Kernel 7.0.0-42, `numa_balancing=1`, no `isolcpus`.
- Profiling: hardware counters work (`cycles`); no IBS, no LBR, no uncore PMUs; `kptr_restrict=1` hides kernel symbols.
- Build: `cmake --preset nes-relwithdebinfo-libstdcxx-none`, targets `systest` (and `nes-unit-tests`), clang 22.
- Interpreter of the UDF processes: `/usr/bin/pypy3`, PyPy 7.3.20 (Python 3.11.13), unless an entry says CPython (3.14.4).
- Inputs, on tmpfs and written with `numactl --membind=0` so that they are on node 0, in `/dev/shm/nes-udf-perf-eval/udf/`:
  - `udf_bench.csv` (30M rows), `udf_bench_3m.csv`, `udf_bench_120m.csv` (4 x the 30M file): `id,s` with a running id and a
    16-letter string, from `nes-systests/benchmark/generate_udf_bench_input.py`.
  - `ids_480m.bin`, `ids_60m.bin`: little-endian INT64 0..n-1, written with `numpy.arange(n, dtype='<i8').tofile(...)`, read
    with the NATIVE input format.
- UDFs: `inc` (`x + 1`, INT64), `upper` (`x.upper()`, VARSIZED, CSV input only), `lcg` (100 steps of a linear congruential
  generator, INT64). Results go to a `Void` sink.
- Throughput is input rows divided by the query time systest reports (`BenchmarkResults.json`). systest's own "tuples/s" line
  counts newlines in the input and is wrong for binary input.
- "Process busy" is the share of the time between a process's first and last batch that it spent in `_process_batch` and in
  publishing answers; `harness/boot.py` measures it by wrapping the functions of the generated program.

### Layout names

`harness/run_experiment.py --list` prints all of them. `wN` is the number of worker threads, `pN` of UDF processes.

| Family | Meaning |
|---|---|
| A | file source on a core of its own (`"SOURCE".CPUS` = PERFORMANCE), UDF source and misc threads on another core |
| B | both sources and misc threads share one hardware thread |
| C | both sources and misc share one core, on both hardware threads |
| Dw / Dp | sources and misc on hyperthread siblings of worker cores (CPUs 1,3) / of process cores (13,15) |
| E | worker threads on both hardware threads of their cores, processes on cores of their own |
| F | worker threads and processes on both hardware threads of their cores |
| G | few worker threads, many processes |
| P | "paired": one worker thread and one process on the two hardware threads of each core, one core for sources and misc |
| X | node 0 as before, extra processes on the cores of node 1 |
| R | 7 or 14 worker threads on node 0, sources and misc on its eighth core, all processes on other nodes (`n1-3` = nodes 1 to 3) |

## Index

| Id | Question | Answer in one line |
|---|---|---|
| E01 | Which transport is fastest, and what limits one process? | Shared memory beats sockets by 16-80%; zero-copy adds 2-9%; PyPy column decoding dominates, not the transport |
| E02 | How does throughput scale with processes? | Linear while processes have cores; cheap UDFs hit a worker-side ceiling |
| E03 | Does NUMA placement of one process matter? | Sharing a core with a worker costs 17-25%; other node or socket costs at most 7% |
| E04 | PyPy microbenchmarks of column access | `memoryview.cast().tolist()` is ~9x slower than `array.array` |
| E05 | Does pinning everything to one node help? | +15-27% over unpinned, +2-7% over pinned but spread |
| E06 | Where does the buffer pool land? | On the node of the thread that creates it; fixed by creating it on a performance CPU |
| E07 | Schema-specialised batch wrapper | +31-67% |
| E08 | Best thread layout on one node, CSV input | Workers on both hyperthreads, processes on own cores, sources share one core; misc needs no CPU |
| E09 | DuckDB as a sanity check | Same scan speed; DuckDB's per-row Python UDFs are 14-400x slower |
| E10 | Best layout on one node, NATIVE input | inc 77M, lcg 17M rows/s; worker threads still costly |
| E11 | What do worker threads spend time on (64 KB buffers)? | 70% reference counting per record in the emit path; fixed: one worker 19.5M -> 54M rows/s |
| E12 | What can two processes exchange through shared memory at best? | ~12 GB/s, ~1.4 ns per row; 40x above a PyPy process |
| E13 | Worker and process on the two hyperthreads of each core? | Within 0-13% of the best tuned split for every workload, without tuning |
| E14 | Does the buffer size matter after the fixes? | Yes: 4 KB is 3.5x slower than 64 KB for inc |
| E15 | Larger ring and more batches in flight? | No effect in any of 8 cases |
| E16 | What do worker threads spend time on (4 KB buffers)? | 11% formatting the query id per task; fixed: inc at 4 KB +38% |
| E17 | Extra processes on a second node? | inc +34%, lcg +76% |
| E18 | Workers alone on node 0, processes on other nodes | inc plateaus at 150-173M rows/s with nothing saturated but the source core; lcg stops scaling past 24-32 processes |
| E19 | Does the committed harness reproduce the earlier numbers? | Yes, within 1% |

---

## E01 Transport matrix and per-process bottleneck

- Date: 2026-11-10. Commit: `9dc995000d` (binary from another worktree at the same commit, RelWithDebInfo).
- Setup: 4 worker threads on CPUs 0,2,4,6 (plus 8,10,12 for other threads) via `numactl --physcpubind --membind=0`, one PyPy
  process pinned to CPU 14 with `taskset`, 30M-row CSV, protocols RAW_BUFFERS / SHARED_MEMORY / SHARED_MEMORY_ZERO_COPY,
  `operator_buffer_size` 4096 and 65536. "Patched" replaced the decode calls in the generated program at runtime
  (`array.array(code, bytes(buf))` instead of `buf.cast(code).tolist()`). ARROW_IPC not measured (no pyarrow for PyPy).
- Results (M rows/s):

| Setup | UDF | Socket | Shm copy | Shm zero-copy |
|---|---|---|---|---|
| 4 KB | inc | 3.3 | 4.1 | 4.7 |
| 4 KB | upper | 2.5 | 3.1 | 3.3 |
| 4 KB | lcg | 1.7 | 1.8 | 1.9 |
| 64 KB | inc | 4.4 | 5.9 | 6.1 |
| 64 KB | upper | 3.2 | 4.2 | 4.3 |
| 64 KB | lcg | 2.0 | 2.3 | 2.4 |
| 64 KB, patched | inc | 6.9 | 11.4 | 12.5 |
| 64 KB, patched | upper | 4.7 | 6.4 | 6.9 |
| 64 KB, patched | lcg | 2.4 | 2.8 | 2.9 |

- Per row in the process (zero-copy, 64 KB, unpatched): decode 102 / 125 / 94 ns, UDF calls ~20 / ~27 / ~294 ns, encode 17 / 54 /
  16 ns, publish 6 / 9 / 9 ns for inc / upper / lcg.
- Conclusion: the marshalling in PyPy dominates. Led to E04 and E07.
- Raw: `results/history/results.jsonl` (labels `A-*`, `B-*`). Runs before the single-node topology was used are in the same
  file with earlier labels and should be ignored.

## E02 Scaling with the number of processes

- Date: 2026-11-10. Commit: `9dc995000d`, decode patched at runtime.
- Setup: 14 worker threads on the cores of nodes 0 and 1, processes on the cores of nodes 2 and 3, zero-copy, 64 KB buffers,
  `total_memory_in_bytes` 8 GiB (the default 256 MiB pool is exhausted with 64 KB buffers and 4 or more processes), 120M-row CSV.
- Results (M rows/s) for 1 / 2 / 4 / 8 / 16 processes: inc 14.6 / 28.5 / 39.6 / 44.1 / 45.8; upper 7.3 / 14.0 / 23.9 / 29.9 /
  30.2; lcg 3.0 / 5.8 / 11.0 / 20.5 / 25.8 (35.5 with the 16 processes on the other socket, nodes 4 and 5).
- Conclusion: lcg scales with processes; inc and upper hit a worker-side ceiling, later traced to CSV parsing and E11.
- Raw: `results/history/results.jsonl` (labels `C-*`, `E-*`).

## E03 NUMA placement of one process

- Date: 2026-11-10. Commit: `9dc995000d`, decode patched. 4 workers on node 0, one process, zero-copy, 64 KB, 3 runs.
- Results (M rows/s): same node own core inc 12.8 / upper 7.0; hyperthread sibling of a worker 10.6 / 5.3; other node same
  socket 13.1 / 6.7; other socket 13.3 / 6.5. Shm copy across sockets 10.1 vs 11.4 on the same node (one sample).
- Raw: `results/history/results.jsonl` (labels `D-*`).

## E04 PyPy microbenchmarks

- Date: 2026-11-10. Standalone scripts, PyPy pinned to CPU 14, 1278 rows per batch. Not in the repository.
- Results (ns per row): `memoryview.cast('q').tolist()` 93; `array.array('q', bytes(mv)).tolist()` 10; `array.frombytes` 1.3-3.4;
  VARSIZED to str 116-120, with `array` offsets and one decode per batch 18.5-25; `[f(*row) for row in zip(col)]` 4.2 vs
  `[f(v) for v in col]` 1.8. CPython 3.14: 11 / 11.5 / 0.3 for the three INT64 readers, call 75 vs 49.
- Conclusion: implemented in E07.

## E05 Pinning everything to one node

- Date: 2026-11-10. Commit: the pinning changes later committed as `963d016a24`..`59fa0d567f` (measured before
  `59fa0d567f`, with `numactl --membind=0` for the same-node case), old wrapper generator.
- Setup: 4 workers, 2 processes, zero-copy, 64 KB, 30M-row CSV, 3 runs. Same node: `performance_cpus=0,2,4,6,8`,
  `misc_cpus=10,11`, `process_cpus=12,14`, file source PERFORMANCE. Spread: `0,2,4,6,16` / `32,33` / `64,66`. Unpinned: no options.
- Results (M rows/s, mean of 3): inc 10.05 / 12.14 / 11.67, upper 6.14 / 7.80 / 7.32, lcg 3.90 / 4.48 / 4.38 for unpinned /
  same node / spread.

## E06 Buffer pool placement

- Date: 2026-11-10. Before and after `59fa0d567f`.
- Method: pinned to node 0 without `numactl`, read `/proc/<pid>/numa_maps` for the `nes-buffers` mapping.
- Results: before, with 4 KB buffers ~87% of the pool was on the node of the thread starting the worker (nodes 5, 4, 7 in three
  runs), with 64 KB buffers ~8%. After: 100% on node 0 in all runs.

## E07 Schema-specialised batch wrapper

- Date: 2026-11-10. Before `59fa0d567f`, after `bc162458b9`.
- Setup: as E05 "same node" with `numactl --membind=0`, 3 runs.
- Results (M rows/s): inc 12.14 -> 19.66, upper 7.80 -> 13.00, lcg 4.48 -> 5.87. Per row in the process: 136 -> 45, 221 -> 126,
  414 -> 318 ns.
- Tests: `PythonUdfOutOfProcess.test` and `PythonUdfVarsizedTypes.test` pass on PyPy and CPython for RAW_BUFFERS,
  SHARED_MEMORY, SHARED_MEMORY_ZERO_COPY on the single-node topology.

## E08 Thread layout on one node, CSV input

- Date: 2026-11-10. Commit: `6a1460cfcd`.
- Setup: families A-F (31 layouts), zero-copy, 64 KB, `total_memory_in_bytes` 4 GiB, inc and upper on the 120M-row CSV, lcg
  on the 30M-row CSV. One run per layout, the best 15 repeated 3 times.
- Results (M rows/s, 3 runs): inc `E w10 p2` 37.8, `F w10 p4` 38.1, `Dw w6 p2` 32.9, `C w5 p2` 30.4, `B w5 p2` 29.7; upper
  `E w6 p4` 20.8, `B w4 p3` 20.1, `A w3 p3` 15.4; lcg `E w4 p5` 12.9, `F w4 p10` 13.0, `Dw w3 p5` 12.8, `B w3 p4` 10.7.
- Misc threads (`task-delayer`, `net-*`) used at most 2% of a core; the two source threads together 0.5-1.3 cores.
- Raw: `results/history/sweep-results.jsonl`, `sweep-reps.jsonl`.

## E09 DuckDB sanity check

- Date: 2026-11-10. DuckDB 2.0.0 (Python wheel) on CPython 3.14.4, numpy 2.5.4; NES at `6a1460cfcd`.
- Setup: same CSV files, node 0 with `numactl --physcpubind=0,2,..,14 --membind=0` (8 threads) and `0-15` (16 threads),
  `EXPLAIN ANALYZE` to discard results (verified: same speed as an aggregate that reads every column), best of 3 (2 for UDFs).
  NES scan baselines with 7 and 14 pinned worker threads, 3 rounds.
- Results (M rows/s, 8 / 16 threads): scan 62.4 / 72.1 (NES 63.0 / 74.7), `id + 1` 65.7 / 73.0 (NES 61.4 / 73.4), `upper(s)`
  54.0 / 61.7, Python UDF inc 2.7 / 2.3, upper 1.1 / 1.1, lcg 0.03 / 0.03 (row-at-a-time UDFs under the GIL).
- Not measured: DuckDB's vectorised Arrow UDFs (no pyarrow), NES with CPython processes.

## E10 Thread layout on one node, NATIVE input

- Date: 2026-11-10. Commit: `a5d2c2f25c` (fix for NATIVE byte sources).
- Setup: as E08 with `ids_480m.bin` (inc) and `ids_60m.bin` (lcg).
- Results (M rows/s, 3 runs): inc `E w6 p4` 77.4, `Dw w4 p4` 73.7, `B w4 p3` 67.3; lcg `F w4 p10` 17.3, `G w2 p6` 16.7,
  `G w2 p12` 16.6, `B w1 p6` 15.3. Scan only 185-196 (file source thread at 0.94 core), native `id + 1` with 4 workers 97.
- Raw: `results/history/native-sweep.jsonl`, `native-reps.jsonl`.

## E11 Worker thread profile at 64 KB, emit fix

- Date: 2026-11-10. Before `a5d2c2f25c`, after `f9335b37d8`.
- Method: `perf record -F 999 -g -k mono` with `nautilus_perf=true`, layout `A w1 p5`, native inc (profiling cost ~5%).
- Finding: ~70% of the worker thread in `BufferControlBlock::retain/release` and `TupleBuffer` copy/destroy, from
  `TupleBufferRef::getRecordBufferStore` capturing the `RecordBuffer` by value for every record.
- Results (M rows/s): one worker (`A w1 p5`, inc) 19.5 -> 54.3; native `id + 1`, 4 workers 97 -> 272; best native inc 77.4 ->
  90.5 (`Dw w3 p5`); best native lcg 17.3 -> 18.8 (`G w1 p7`); CSV inc 37.8 -> 37.4, upper 20.8 -> 22.9, lcg 12.9 -> 13.1.
- Open: native `id + 1` (272) is faster than the plain scan (184); not explained.
- Raw: `results/history/native-after.jsonl`.

## E12 Shared-memory queue baseline

- Date: 2026-11-10. Commit: `e8b3739af8`. `shm_queue_baseline.cpp`, `clang++ -O3 -std=c++20`.
- Command: `numactl --membind=0 ./shm_queue_baseline <producer cpu> <consumer cpu> 1` for (0,2), (0,1), (0,16), (0,64).
- Results (echo, consumer reads the payload, 8 in flight, spinning; M messages/s on same node / siblings / other node / other
  socket): 64 B in place 30.3 / 54.4 / 6.5 / 4.0; 4 KB in place 3.05 / 2.77 / 1.98 / 1.36; 64 KB in place 0.19 / 0.18 / 0.13 /
  0.10; 64 KB copied 0.19 / 0.15 / 0.09 / 0.06. About 12 GB/s on one node from 4 KB to 1 MB. Ping-pong of 64 B: 135 ns.
  Spinning vs futex: equal from 4 KB up.
- Raw: `results/history/shmq-*.txt`. One run of one second per configuration.

## E13 Paired layout

- Date: 2026-11-10. Commit: `e8b3739af8`. 2 runs.
- Results (M rows/s, best tuned / `P w7 p7` / `P w8 p8`): native inc 89.2 / 78.1 / 74.2; native lcg 18.7 / 17.4 / 18.0; CSV
  inc 37.4 / 36.8 / 36.0; CSV upper 22.9 / 21.6 / 21.3; CSV lcg 13.1 / 13.6 / (harness error).
- Raw: `results/history/pair.jsonl`, `pair-csv.jsonl`.

## E14 Buffer size

- Date: 2026-11-10. Commit: `e8b3739af8`. Native input, one run.
- Results (M rows/s at 4 / 16 / 64 KB): `Dw w3 p5` inc 26.1 / 69.2 / 91.7, lcg 12.9 / 13.9 / 14.1; `P w7 p7` inc 35.8 / 68.5 /
  78.0, lcg 14.3 / 16.5 / 17.5. Rows per batch 171 / 683 / 2731. At 4 KB inc: workers 94-98% busy, processes 30-49%.

## E15 Larger ring, more batches in flight

- Date: 2026-11-10. Commit: `e8b3739af8` plus an uncommitted change of `RING_SIZE` 4 -> 16 MiB and `MAX_IN_FLIGHT` 8 -> 64 in
  `PythonProcessShmSource.cpp`, reverted afterwards.
- Results (M rows/s, before -> after): the 8 cases of E14 at 4 and 64 KB changed by at most 5% in either direction (for example
  26.1 -> 25.7, 91.7 -> 91.8, 14.1 -> 13.9).
- Conclusion: neither the ring size nor the in-flight limit limits throughput in these layouts.

## E16 Worker thread profile at 4 KB, log context fix

- Date: 2026-11-10. Before `e8b3739af8`, after `f242c38a20`.
- Method: `perf record -F 999 -g`, `Dw w3 p5`, native inc, 4 KB buffers.
- Finding: ~11% of a worker thread formats `"{queryId}-{pipelineId}"` for the log context of every task; the UDF source thread
  is ~70% in its polling loops, so its 0.97 core is mostly spinning.
- Results (M rows/s, 4 KB, 2 runs): `Dw w3 p5` inc 26.1 -> 36.1, `P w7 p7` inc 35.8 -> 41.2; lcg unchanged (12.9, 14.5). 64 KB
  unchanged (91.6).

## E17 Extra processes on a second node

- Date: 2026-11-10. Commit: `f242c38a20`. Native input, 64 KB, 2 runs.
- Results (M rows/s): inc `Dw w3 p5` 90.6 vs `X w7 p8` 121.6 vs `X w7 p15` 120.9; lcg `G w1 p7` 18.7 vs `X w2 p14` 32.9 vs
  `X w7 p15` 32.7. Per row in the process: inc 48 -> 54 ns (79 ns with processes on worker siblings), lcg 335 -> 341 ns.
- Caveat: the per-row comparison is between different layouts, not the same layout with only the process CPUs moved.

## E18 Workers alone on node 0, processes on other nodes

- Date: 2026-11-10. Commit: `f242c38a20`. Native input, 64 KB, zero-copy, `total_memory_in_bytes` 4 GiB, one run.
- Command (scratch copy of the harness, equivalent to):
  `run_experiment.py --experiment E18 --format native --udfs inc,lcg --layouts "R w7 n1" "R w7 n1-2" "R w7 n1-3" "R w7 n4" "R w7 n4-5" "R w7 n4-7" "R w7 n1-7" "R w14 n1-2" "R w14 n1-3" "R w14 n1-7"`
- Results:

| Layout | Processes | inc M rows/s | inc processes busy (min) | lcg M rows/s | lcg processes busy (min) | lcg ns/row |
|---|---|---|---|---|---|---|
| `R w7 n1` | 8 | 121.0 | 0.94 (0.90) | 20.8 | 0.98 (0.98) | 339 |
| `R w7 n1-2` | 16 | 152.3 | 0.68 (0.10) | 35.9 | 0.98 (0.98) | 344 |
| `R w14 n1-2` | 16 | 156.6 | 0.66 (0.06) | 35.7 | 0.98 (0.98) | 346 |
| `R w7 n1-3` | 24 | 150.7 | 0.44 (0.01) | 38.9 | 0.97 (0.97) | 424 |
| `R w14 n1-3` | 24 | 149.1 | 0.44 (0.01) | 38.8 | 0.98 (0.97) | 427 |
| `R w7 n4` | 8 | 117.9 | 0.92 (0.87) | 20.5 | 0.98 (0.98) | 341 |
| `R w7 n4-5` | 16 | 173.2 | 0.73 (0.30) | 35.8 | 0.98 (0.97) | 345 |
| `R w7 n4-7` | 32 | 159.1 | 0.56 (0.01) | 46.7 | 0.90 (0.54) | 362 |
| `R w7 n1-7` | 56 | 132.2 | 0.35 (0.01) | 36.6 | 0.76 (0.12) | 436 |
| `R w14 n1-7` | 56 | 133.7 | 0.46 (0.01) | 36.4 | 0.85 (0.12) | 442 |

- Other observations: for inc with 16 or more processes the worker threads are 54-76% busy and both source threads use
  0.73-0.87 of a hardware thread each, on the two hyperthreads of one core (CPUs 14, 15). For lcg the worker threads are
  18-35% busy and the source threads 0.3-0.5.
- Conclusions:
  - inc: 8 remote processes are saturated (121M); from 16 on, throughput stays at 150-173M with neither workers nor processes
    saturated. The plain scan of this input reaches 185-196M with the file source thread at 0.94 core (E10), so the input side
    and the two source threads sharing one core are the likely limit. Not yet separated.
  - lcg: scales to 16 processes (35.9M). With 24 processes on three nodes of the same socket the per-row cost rises from 344
    to 424 ns although the processes are 97% busy; with 32 on the other socket it is 362 ns but some processes are only 54%
    busy; with 56 processes throughput drops. Two effects to separate: per-row cost rising with many processes on one socket,
    and processes not being fed evenly.
  - 14 worker threads on both hyperthreads are no better than 7 here.
- Raw: `results/history/remote.jsonl`.

## E19 Harness validation

- Date: 2026-11-10. Commit: `cb3c03f5f3` (binary built from `f242c38a20`; the commits in between only add files under
  `nes-python-udf/benchmark/`).
- Command:
  `python3 nes-python-udf/benchmark/harness/run_experiment.py --experiment E19 --format native --udfs inc,lcg --layouts "Dw w3 p5" "R w7 n1" --reps 2`
- Results (M rows/s, 2 runs): `Dw w3 p5` inc 90.2, 89.6 (E11: 90.5); lcg 14.0, 14.1 (E14: 14.1). `R w7 n1` inc 121.5, 120.3
  (E18: 121.0); lcg 20.9, 21.1 (E18: 20.8).
- Conclusion: the harness in the repository reproduces the numbers of the scratch harness, and run-to-run variation is about 1%.
- Raw: `results/E19.jsonl`.

## Open questions

- What limits inc at 150-173M rows/s with 16+ remote processes: the file source (185-196M alone), the UDF source thread, or
  the two sharing one core? Next step: give each source thread a core of its own, and use an input that is not limited by one
  file source thread.
- Why does lcg's per-row cost rise from 344 to 424-436 ns with 24 or more processes on socket 0, and why are processes fed
  unevenly beyond 32? `feedProcesses` fills the first processes first.
- Queue implementations for the rings between the worker and remote processes (the stated next topic): the baseline (E12)
  shows the cross-node cost is in the head and tail cache lines for small messages; nothing measured in NES yet.
- The plain native scan (184M) is slower than native `id + 1` (272M) on the same input (E11).
- Hyperthreads for processes: +23% for lcg in `F w4 p10` (E10) but nothing in `G w2 p12`; not explained.
- NES with CPython processes and DuckDB with Arrow UDFs, for a like-for-like comparison (E09).
