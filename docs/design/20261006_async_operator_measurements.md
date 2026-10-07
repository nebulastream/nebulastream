# Asynchronous operator execution: measurements

> **Status:** measurement record for the `feat/async-operator` branch, last updated 7 October 2026.
> Sections 1 to 4 were measured locally on a Debug build; section 5 is the cluster comparison,
> measured on a release build against vLLM, and section 6 is how to repeat it. Read the
> [caveats](#how-to-read-these-numbers) before quoting any of it.

## What was measured, and why

A semantic operator waits for a remote model. Under synchronous execution that wait happens on one
of the engine's worker threads, so a handful of concurrent calls occupies the whole pool and stalls
unrelated queries. The asynchronous framework moves the call onto threads a source owns, where
blocking is architecturally allowed.

Two claims follow from that, and they need different measurements:

1. **The engine stays responsive.** Unrelated queries are no longer held up. Measured against a
   real model, because this is about the engine and not about call latency.
2. **Throughput rises with concurrency.** Measured against the mock backend's artificial delay,
   because a real endpoint imposes a limit of its own that would mask ours.

Both execution modes use the same prompt layout, the same transport and the same response decoding,
and produce identical results — [`SemanticMapAsyncWindow.test`](../../nes-systests/semantic/SemanticMapAsyncWindow.test)
asserts that row for row. That equivalence is what makes the comparison a comparison.

---

## 1. The engine stays responsive

A worker configured with a **single thread** runs a slow SEM_MAP query (12 reviews, `qwen2.5:7b`)
and, at the same time, an ordinary query that contains no model at all.

| | SEM_MAP query | ordinary query | ordinary query vs. alone |
|---|---|---|---|
| ordinary query on its own | — | **0.128 s** | 1× |
| beside the **synchronous** SEM_MAP | 199.0 s | **199.1 s** | **≈ 1 550×** |
| beside the **asynchronous** SEM_MAP | 108.9 s | **0.150 s** | 1.2× |

This is the result the framework exists for. Synchronously, the model call occupies the one worker
thread for the whole run, and 0.128 s of unrelated work waits 199 s behind it. Asynchronously the
call sits on the source's own thread, the pool stays free, and the unrelated query is essentially
unaffected.

The synchronous number scales with thread count, so on a four-thread worker the ordinary query
would get through sooner — but only by spending threads that are then unavailable for anything
else. That trade is the subject of section 2.

Test: [`AsyncResponsiveness.test`](../../nes-systests/semantic/AsyncResponsiveness.test),
driven by `scripts/measure_async_semantic_map.sh responsiveness`.

---

## 2. Throughput against concurrency

Measured with the mock backend's delay suffix (`'echo@200'`, 200 ms per request) rather than a model
server, so what is measured is the framework and nothing else. Every query asserts record count and
checksum, so a configuration that loses or duplicates a record fails instead of looking fast.

### 2a. The decisive case: one input buffer

30 rows arrive in a single buffer, so concurrency can only come from within it.

| `MAX_CONCURRENCY` | before the fix | after the fix | speedup |
|---|---|---|---|
| 1 | 6.17 s | 6.07 s | 1.0× |
| 10 | 6.17 s | **0.66 s** | **9.2×** |

Before the fix the two were identical: `MAX_CONCURRENCY` did nothing. The unit of work was the
input buffer, not the call — one source thread took a buffer and ran its batches one after another
while the others waited. After the fix an intake thread splits each buffer into one work item per
batch and the workers pull batches, so the option now means what it says.

### 2b. Many input buffers

760 rows, which arrive in roughly twenty buffers.

| `MAX_CONCURRENCY` | before | after | ideal (760 × 200 ms ÷ conc) | speedup |
|---|---|---|---|---|
| 1 | 152.61 s | 152.56 s | 152.0 s | 1.0× |
| 5 | 32.37 s | 30.55 s | 30.4 s | 5.0× |
| 10 | 17.12 s | 16.51 s | 15.2 s | 9.2× |
| 30 | 7.91 s | **5.26 s** | 5.07 s | **29.0×** |

After the fix the curve tracks the ideal within a few percent, and the earlier saturation at about
19× — which was the number of buffers — is gone.

### 2c. What the synchronous path costs to reach the same throughput

Same input and delay, `MAX_CONCURRENCY 30`, synchronous execution:

| worker threads | 760 rows |
|---|---|
| 1 | 152.69 s |
| 4 | 39.61 s |
| 8 | 22.77 s |

The synchronous path does scale — but in worker threads, not in configuration. Matching the
asynchronous 5.26 s would take roughly thirty worker threads, every one of them parked in a model
call. That is the cost section 1 measures the absence of.

Test: [`SemanticMapAsyncThroughput.test`](../../nes-systests/semantic/SemanticMapAsyncThroughput.test),
driven by `scripts/measure_async_semantic_map.sh throughput`.

---

## 3. Against a real model, and a ceiling that turned out to be the server's

`qwen2.5:7b` on Ollama, localhost, default configuration.

**The server's own limit.** Issuing the same request 1, 4 and 8 times concurrently:

| concurrent requests | wall time |
|---|---|
| 1 | 21.4 s |
| 4 | 22.7 s |
| 8 | 45.5 s |

Ollama serves four requests in parallel (`OLLAMA_NUM_PARALLEL`, default 4) and queues the rest. No
client-side concurrency can exceed 4× against this installation, which is the whole reason section 2
uses the mock backend.

**40 reviews, one record per request.** The synchronous path on a single worker thread is serial by
construction, which gives the serial reference:

| | 40 rows | vs. serial |
|---|---|---|
| synchronous, 1 worker thread (serial reference) | 379.6 s | 1.00× |
| asynchronous, `MAX_CONCURRENCY 8`, before the fix | 382.5 s | 0.99× |
| asynchronous, `MAX_CONCURRENCY 8`, after the fix | 310.8 s | 1.22× |
| what the server's 4-way parallelism allows | ≈ 95 s | 4.0× |

**This was an open item and section 5 closes it.** The fix helped here (0.99× → 1.22×), but 1.22×
against an available 4× looked like a ceiling in our code. It was not: against a server that
serves 256 requests at once, the framework reaches an effective concurrency of 262. What limited
this measurement was Ollama — its four-way parallelism, and per-call latency that roughly doubled
under even that load.

> A correction: an earlier commit message put this at 2.76× by using 21.4 s as the per-call latency.
> That figure came from a hand-written approximation of the prompt, not from the codec. The serial
> reference above, 9.5 s per call, comes from the synchronous path itself and is the right one.

Test: [`AsyncBenchmarkProbe.test`](../../nes-systests/semantic/AsyncBenchmarkProbe.test),
driven by `scripts/measure_async_semantic_map.sh probe`.

---

## 4. Correctness, which the performance numbers rest on

A faster path that quietly drops records is worthless, so every measurement asserts results.

| | |
|---|---|
| Asynchronous system tests (mock backend) | 10 / 10 |
| Synchronous system tests (mock backend) | 8 / 8 |
| Window downstream of SEM_MAP, synchronous and asynchronous | 5 / 5 |
| Unit tests (framework, executor, splitter, catalog, binder) | 50 / 50 |

The window test is the one worth naming in a meeting. The consumer source hands the engine records
it did not produce itself, so it passes `OriginId`, `SequenceNumber` and the watermark of the
incoming buffer through unchanged and only assigns chunk numbers. Getting that wrong breaks nothing
visibly — a watermark that never advances means a window that never fires, so the symptom is
*missing* output, which a checksum over records cannot catch. The test asserts exact window counts
for 760 rows across sixteen windows with ten calls in flight, summing to exactly 760, synchronously
and asynchronously, and the two agree row for row.

---

## 5. The comparison on the cluster: the result

Two runs against a vLLM deployment serving `google/gemma-4-E4B-it`, reached over an SSH tunnel.
2000 rows, batch size 1 on both sides, `MAX_CONCURRENCY 256` on the asynchronous one, everything
else at its default. Release build. Both runs passed their assertion, so 2000 records went in and
2000 came out on each side, none lost and none duplicated.

| | wall time | throughput | mean latency | p50 | p95 | max | calls/s |
|---|---|---|---|---|---|---|---|
| **blocking** | 869.3 s | 2.30 rec/s | 434 ms | 308 ms | 533 ms | 9192 ms | 2.3 |
| **asynchronous, 256** | **10.3 s** | **193.3 rec/s** | 1263 ms | 1147 ms | 1960 ms | 2257 ms | 207.8 |

**84x on throughput**, and the trade it is made with is visible in the same table: latency per call
rose 2.9x, from 434 ms to 1263 ms, because 256 requests in flight load the server while one at a
time does not. Throughput and latency move in opposite directions, which is why both were asked
for and why quoting either alone would misrepresent the result.

### The mechanism, not just the outcome

Latency and call rate together give the concurrency actually achieved — rate times mean latency:

| | calls/s × mean latency | configured |
|---|---|---|
| blocking | 2.3 × 0.434 s = **1.0** | one worker thread |
| asynchronous | 207.8 × 1.263 s = **262** | `MAX_CONCURRENCY 256` |

Both sides did exactly what their configuration says. The blocking path kept one call in flight,
which is its worker thread count — `MAX_CONCURRENCY` is not set on it and would not help, because
it issues its request per record from a worker thread and blocks there, so its semaphore only binds
*below* the thread count
([`SemanticMapPhysicalOperator.cpp:116`](../../nes-physical-operators/src/Semantic/SemanticMapPhysicalOperator.cpp#L116)).
The asynchronous path kept 262, matching the 256 it was given to within the measurement window.

This is also what closes the open question of section 3. There, 1.22x against an available 4x
suggested a ceiling somewhere in our code. There is none: given a server that serves 256 requests
at once, the framework fills it. The Ollama ceiling was Ollama's.

### Two things to say when quoting this

- **The blocking run had one worker thread**, which is what systest's default topology
  ([`two-node.yaml`](../../nes-systests/configs/topologies/two-node.yaml)) gives, and the
  experiment asked for defaults. Section 2c measures what more threads buy that path: on the
  mock backend, 152.7 s, 39.6 s and 22.8 s at 1, 4 and 8 threads. Reaching the asynchronous
  throughput that way would take on the order of 256 worker threads, every one parked in an HTTP
  call — which is the cost section 1 measures.
- **The blocking maximum of 9192 ms is a single outlier** against a p95 of 533 ms. The asynchronous
  distribution is the tighter one despite the higher mean, its maximum being 2257 ms.

### Latency is measured, not derived

Throughput from wall time cannot tell queueing apart from service time, so the round trips are
timed where both modes pass through them, in `HttpSemanticBackend`. Each mode writes one summary
into the systest log when the query stops:

```
Semantic model latency [asynchronous]: 12 calls (0 failed), mean 41972 ms,
p50 37864 ms, p95 55932 ms, max 65245 ms, 0.1 calls/s
```

Mean, median, p95, maximum and the call rate actually achieved. Timed across retries, because that
is the wait the operator experiences. The runner computes records per second itself, since systest
reports only wall time.

Set `NES_SEMANTIC_LATENCY_REPORT=1` to get the same line on stderr. That exists because a release
build compiles with `NES_LOGLEVEL_WARN`, which removes `NES_INFO` from the binary altogether --
and a release build is exactly where a benchmark runs, so the first cluster run produced no latency
line at all. A measurement must not depend on how the logger was compiled; an ordinary deployment
stays quiet because the variable is not set.

## 6. Running the cluster benchmark

### Once: a release build

Everything in sections 1 to 4 was measured on a Debug build. For this run it matters, because 257
threads and a good deal of buffer bookkeeping are on the critical path, so build optimised. Inside
the development image:

```bash
cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_TARGET_TRIPLET=x64-linux-none-local -DVCPKG_MANIFEST_MODE=OFF
cmake --build cmake-build-release --target systest
```

### Reaching the endpoint

The endpoint is OpenAI-compatible, so an SSH tunnel is enough. Leave it open in its own terminal:

```bash
ssh -L 8000:<vllm-host>:8000 <cluster>
```

The development image runs with `--network host`, so a tunnel bound on the host's loopback is
reachable from inside the container. Then take the exact model id — vLLM rejects a mismatched one:

```bash
curl -s localhost:8000/v1/models | jq -r '.data[].id'
```

### The run

```bash
export NES_SEMANTIC_LATENCY_REPORT=1   # the latency line; a release build logs nothing otherwise
BUILD_DIR=cmake-build-release scripts/run_cluster_benchmark.sh --model <id> --rows <N> --smoke-only
BUILD_DIR=cmake-build-release scripts/run_cluster_benchmark.sh --model <id> --rows <N>
```

Pass the variable into the container as well if the build runs inside one (`docker run -e
NES_SEMANTIC_LATENCY_REPORT ...`).

Add `--api-key-env <VAR>` if the deployment needs a token; the variable has to be set inside the
container, which the Docker wrapper does not forward yet.

The script does the parts that are easy to get wrong:

1. Checks the endpoint and that the model id is served.
2. Generates the input at the requested size.
3. Learns the expected record count and checksum from the mock backend, which costs no model time,
   and writes it into the generated test. Only `id` is projected and nothing is filtered, so the
   expectation does not depend on what the model answers and asserts what a throughput number
   needs: record count in equals record count out.
4. Runs a smoke test, so a wrong id or a missing token fails in a minute rather than an hour in.
5. Then the two runs, printing worker threads, wall time and records per second for each.

### Choosing `--rows`

Do the smoke test first and read the real per-call latency off its latency line, then pick the row
count so the **blocking** run — one call at a time at the default thread count — still finishes in
a sensible time. Blocking takes roughly `rows x latency`; asynchronously it is divided by the
concurrency vLLM sustains. Reporting records per second keeps different row counts comparable.

### Reading the result

```bash
grep "Semantic model latency" $(ls -t cmake-build-release/nes-systests/SystemTest_*.log | head -1)
```

One line per run, labelled `[synchronous]` or `[asynchronous]`. Throughput is printed by the script
after each run. The generated test file under `nes-systests/semantic/` is gitignored: it carries a
machine's model id and is not meant to be committed.

## How to read these numbers

- **Debug build.** `CMAKE_BUILD_TYPE=Debug`. Absolute times are pessimistic. The comparisons hold,
  because both execution modes are affected equally and the mock measurements are dominated by a
  configured sleep rather than by our code.
- **One machine, single runs.** No repetitions, no variance figures. The mock-backend numbers are
  stable because the delay dominates; the real-model numbers are not — model latency varied visibly
  between runs.
- **Synthetic input.** The reviews were generated by `scripts/generate_async_benchmark_data.py`
  with a fixed seed, matching the real dataset only in shape and length (one short paragraph, 11–26
  words). **No quality or accuracy claim can be derived from them.** The real benchmark dataset is
  the Rotten Tomatoes critic reviews the Python reference uses, whose `scoreSentiment` column is the
  ground truth; it was not available on this machine.
- **The worker thread count is the blocking path's concurrency.** Left at its default, systest's
  topology gives one thread, which costs that path everything and the asynchronous path nothing.
  The cluster runner deliberately does not override it — the experiment asks for defaults — and
  prints the effective count with every run, so the number can be read correctly. The local
  measurement script does pass it, because sections 1 and 2c vary it on purpose.
- **Ollama's four-way limit** applies to every real-model number here and is a property of that
  installation, not of either execution mode.

## What is not measured yet

- Time-to-first-result. `'FALSE' AS LLM.PRESERVE_ORDER` now exists and is covered by a system
  test, but the latency it is meant to buy has not been measured — only that the records all
  arrive.
- Several asynchronous queries at once. Ten queries at `MAX_CONCURRENCY 16` would be 160 threads,
  which nothing currently bounds.
- Chunking, when output records do not fit one buffer. The code mirrors
  `EmitOperatorHandler::setChunkNumber`, but the answers in these tests are short enough that the
  path never runs.
- Cancellation while calls are in flight, including whether buffers return to the pool cleanly.

## Reproducing the local measurements

Sections 1 to 4. For the cluster comparison see [section 6](#6-running-the-cluster-benchmark).

```bash
ninja systest
scripts/generate_async_benchmark_data.py                      # writes cmake-build-debug/bench-data
scripts/measure_async_semantic_map.sh throughput              # section 2, mock backend, ~7 min
scripts/measure_async_semantic_map.sh responsiveness           # section 1, needs Ollama, ~6 min
scripts/measure_async_semantic_map.sh probe                    # section 3, needs Ollama, ~20 min
```

The real-model modes need Ollama on `localhost:11434` with `qwen2.5:7b` pulled, and the container
started with `--network host`.
