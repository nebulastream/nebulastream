# Asynchronous operator execution: measurements

> **Status:** measurement record for the `feat/async-operator` branch, 6 October 2026. Numbers are
> from one machine and a Debug build; read the [caveats](#how-to-read-these-numbers) before quoting
> any of them.

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

## 3. Against a real model, and an open question

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

**This is an open item, not a result.** The fix helped (0.99× → 1.22×), but 1.22× against an
available 4× means something still limits the real-model path that the mock path does not hit. The
mock reaches 29×, so it is not the framework's scheduling. Candidates not yet separated: per-call
latency rising under load for the codec's actual prompts, connection or model-slot behaviour in
Ollama, or per-thread curl handle setup. The next measurement is the obvious one and has not been
run: the asynchronous path against the real model at `MAX_CONCURRENCY` 1, 2, 4 and 8, which shows
where the curve flattens.

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

## 5. The planned comparison at concurrency 256

The next run is blocking against asynchronous on a vLLM cluster, batch size 1 and
`MAX_CONCURRENCY 256` on both sides, 256 being that deployment's default concurrency. One thing
about that setup has to be said out loud, because it decides how the result may be read.

**`MAX_CONCURRENCY` does nothing for the blocking path.** That operator issues its request per
record from an engine worker thread and blocks there, so its semaphore only binds *below* the
thread count — the code says so at
[`SemanticMapPhysicalOperator.cpp:116`](../../nes-physical-operators/src/Semantic/SemanticMapPhysicalOperator.cpp#L116).
Its real concurrency is the number of worker threads, which section 2c measures directly: 152.7 s,
39.6 s and 22.8 s at 1, 4 and 8 threads, linear in threads and indifferent to the configuration.

So the run compares 256-way against *W*-way, where *W* is whatever the worker is configured with.
The difference is real, but the obvious objection — "then give the engine 256 worker threads" — has
to be answered inside the experiment, not afterwards. The answer is section 1: those 256 threads
would all be parked in HTTP calls, and anything else submitted to that worker waits behind them.
Therefore:

- run the blocking path at several thread counts (1, 4, 16, 64, 256), not just one;
- at the highest, run the model-free query alongside, which is what shows the cost;
- report records per second as well as wall time, and per-record latency separately — at 256
  concurrency vLLM trades latency for throughput, so the asynchronous run should be far better on
  throughput and *worse* per record;
- size the input so the slowest configuration still finishes: at roughly 2 s per call, 5 000 rows
  is about 42 minutes blocking at four threads against 40 seconds asynchronously, while 50 000 rows
  would be seven hours blocking.

Expect something between 30x and 100x rather than the nominal 256/*W*, because per-request latency
rises with concurrency. If the asynchronous run comes out below about 10x, then the limit is not
vLLM, and it is the same unexplained ceiling as in section 3.

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
- **Worker thread count must be passed explicitly.** Left out, systest uses a single worker thread.
  That costs the synchronous path everything and the asynchronous path nothing, which makes any
  comparison that forgets it far too flattering. The measurement script always passes it.
- **Ollama's four-way limit** applies to every real-model number here and is a property of that
  installation, not of either execution mode.

## What is not measured yet

- The real-model concurrency curve of section 3, which is what would close the open question there.
- Time-to-first-result. `'FALSE' AS LLM.PRESERVE_ORDER` now exists and is covered by a system
  test, but the latency it is meant to buy has not been measured — only that the records all
  arrive.
- Several asynchronous queries at once. Ten queries at `MAX_CONCURRENCY 16` would be 160 threads,
  which nothing currently bounds.
- Chunking, when output records do not fit one buffer. The code mirrors
  `EmitOperatorHandler::setChunkNumber`, but the answers in these tests are short enough that the
  path never runs.
- Cancellation while calls are in flight, including whether buffers return to the pool cleanly.

## Reproducing

```bash
ninja systest
scripts/generate_async_benchmark_data.py                      # writes cmake-build-debug/bench-data
scripts/measure_async_semantic_map.sh throughput              # section 2, mock backend, ~7 min
scripts/measure_async_semantic_map.sh responsiveness           # section 1, needs Ollama, ~6 min
scripts/measure_async_semantic_map.sh probe                    # section 3, needs Ollama, ~20 min
```

The real-model modes need Ollama on `localhost:11434` with `qwen2.5:7b` pulled, and the container
started with `--network host`.
