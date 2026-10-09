# Asynchronous operator execution: measurements

> **Status:** measurement record for the `feat/async-operator` branch, last updated 9 October 2026.
> Written to be read top to bottom: the result first, then what it is for, then how it was reached,
> then what it rests on. Sections 9 to 11 and [appendix A](#appendix-a-exact-configuration) carry
> the caveats, the gaps and the exact configuration behind every number — read those before quoting
> any of it.

## 1. Why this exists

A semantic operator waits for a remote model. Under the synchronous path that wait happens on one
of the engine's worker threads, and those threads are the scarce resource the engine serves *every*
query with. So a handful of concurrent model calls occupies the whole pool and stalls unrelated
queries.

The asynchronous framework moves the call onto threads a source owns, where blocking is
architecturally allowed. Two claims follow, and they need different measurements:

1. **Throughput rises with concurrency**, because many calls can now be in flight.
2. **The engine stays responsive**, because the worker pool is no longer occupied by waiting.

The second is the reason the framework exists; the first is what makes it worth configuring. Both
rest on the two modes being otherwise identical: same prompt layout, same transport, same response
decoding, and the same results row for row — which
[`SemanticMapAsyncWindow.test`](../../nes-systests/semantic/SemanticMapAsyncWindow.test) asserts.
That equivalence is what makes the comparison a comparison.

---

## 2. The result: 84× throughput on the cluster

Two runs against a vLLM deployment serving `google/gemma-4-E4B-it`, reached over an SSH tunnel.
2000 rows. What differs between them is a single clause:

```sql
-- both models, identical but for the last line
CREATE SEMANTIC MODEL <name>
INPUT (reviewText VARSIZED)
OUTPUT (sentiment VARSIZED)
SET ('Classify the sentiment of the review as POSITIVE or NEGATIVE' AS LLM.PROMPT,
     'http://localhost:8000/v1' AS LLM.ENDPOINT, 'google/gemma-4-E4B-it' AS LLM.MODEL_NAME,
     'http' AS LLM.BACKEND, 'POSITIVE,NEGATIVE' AS LLM.OUTPUT_VALUES,
     'POSITIVE' AS LLM.DEFAULT_VALUE, 900 AS LLM.TIMEOUT_SECONDS, 3 AS LLM.MAX_RETRIES,
     -- asynchronous run:
     'ASYNC' AS LLM.EXECUTION, 1 AS LLM.BATCH_SIZE, 256 AS LLM.MAX_CONCURRENCY);
     -- blocking run: nothing here. BATCH_SIZE and MAX_CONCURRENCY stay at their defaults of
     -- 1 and 10, and EXECUTION at SYNC. MAX_CONCURRENCY is irrelevant to it either way.
```

```sql
SELECT id FROM SEM_MAP(<name>, reviews) INTO idsum;   -- idsum is a Checksum sink
```

| | wall time | throughput | mean latency | p50 | p95 | max | calls/s |
|---|---|---|---|---|---|---|---|
| **blocking** | 869.3 s | 2.30 rec/s | 434 ms | 308 ms | 533 ms | 9192 ms | 2.3 |
| **asynchronous, 256** | **10.3 s** | **193.3 rec/s** | 1263 ms | 1147 ms | 1960 ms | 2257 ms | 207.8 |

**84× on throughput**, and the trade it is made with is in the same table: latency per call rose
2.9×, from 434 ms to 1263 ms, because 256 requests in flight load the server while one at a time
does not. Throughput and latency move in opposite directions, which is why both were asked for and
why quoting either alone would misrepresent the result.

Both runs passed their assertion, so 2000 records went in and 2000 came out on each side, none lost
and none duplicated. Projecting only `id` and filtering nothing makes that assertion independent of
what the model answers, so it checks the one property a throughput number needs: record count in
equals record count out.

### 2a. The mechanism, not just the outcome

Latency and call rate together give the concurrency actually achieved — rate times mean latency:

| | calls/s × mean latency | configured |
|---|---|---|
| blocking | 2.3 × 0.434 s = **1.0** | one worker thread |
| asynchronous | 207.8 × 1.263 s = **262** | `MAX_CONCURRENCY 256` |

Both sides did exactly what their configuration says, so the 84× is the ratio of concurrency
achieved and not a measurement artefact. The blocking path kept one call in flight, which is its
worker thread count — `MAX_CONCURRENCY` is not set on it and would not help, because it issues its
request per record from a worker thread and blocks there, so its semaphore only binds *below* the
thread count
([`SemanticMapPhysicalOperator.cpp:116`](../../nes-physical-operators/src/Semantic/SemanticMapPhysicalOperator.cpp#L116)).
The asynchronous path kept 262, matching the 256 it was given to within the measurement window.

**Configuration.** 2000 rows (`reviews_2000.csv`), release build, `number_of_worker_threads` left
at the topology default of 1, `NES_SEMANTIC_LATENCY_REPORT=1`. Full environment in
[appendix A](#appendix-a-exact-configuration); how to repeat it in
[section 11b](#11b-the-cluster-benchmark).

---

## 3. What it is for: the engine stays responsive

Throughput is not the reason this exists — keeping the engine usable is. A worker configured with a
**single thread** runs a slow SEM_MAP query (12 reviews) and, at the same time, an ordinary query
that contains no model at all.

| | SEM_MAP query | ordinary query | ordinary query vs. alone |
|---|---|---|---|
| ordinary query on its own | — | **0.128 s** | 1× |
| beside the **synchronous** SEM_MAP | 199.0 s | **199.1 s** | **≈ 1 550×** |
| beside the **asynchronous** SEM_MAP | 108.9 s | **0.150 s** | 1.2× |

0.128 s of unrelated work waits 199 s behind the model call. Synchronously the call occupies the one
worker thread for the whole run; asynchronously it sits on the source's own thread, the pool stays
free, and the unrelated query is essentially unaffected.

**Configuration.** 12 rows (`reviews_12.csv`), `qwen2.5:7b` on Ollama at `localhost:11434/v1`,
`BATCH_SIZE 1`, `180 AS LLM.TIMEOUT_SECONDS`, `0 AS LLM.MAX_RETRIES`; asynchronous run
`MAX_CONCURRENCY 8`, synchronous run `MAX_CONCURRENCY 4`; `number_of_worker_threads=1` passed
explicitly; Debug build. The unrelated query reads `plain_200.csv` and filters `id > 189`, giving 11
rows and no model call. Test:
[`AsyncResponsiveness.test`](../../nes-systests/semantic/AsyncResponsiveness.test), driven by
`scripts/measure_async_semantic_map.sh responsiveness`.

---

## 4. Why the blocking path cannot simply be given more threads

The obvious objection to section 2 is "then give the engine 256 worker threads". It can be
answered with a measurement. Same input and delay, `MAX_CONCURRENCY 30`, synchronous execution,
mock backend:

| worker threads | 760 rows |
|---|---|
| 1 | 152.69 s |
| 4 | 39.61 s |
| 8 | 22.77 s |

The blocking path does scale — but in worker threads, not in configuration, and not perfectly:
eight threads buy 6.7×, not 8×. Matching the asynchronous time for the same input (5.26 s, see
section 5) would take on the order of thirty worker threads, and matching the cluster run would
take hundreds — every one of them parked in an HTTP call. Section 3 is what that costs: the engine
stops serving anything else.

So the choice is not "threads or concurrency". It is whether the waiting happens on threads the
engine needs for other work.

---

## 5. How the framework got there: concurrency per call, not per buffer

Measured with the mock backend's delay suffix (`'echo@200'`, 200 ms per request) rather than a model
server, so what is measured is the framework and nothing else. Every query asserts record count and
checksum, so a configuration that loses or duplicates a record fails instead of looking fast.

### 5a. The decisive case: one input buffer

30 rows arrive in a single buffer, so concurrency can only come from within it.

| `MAX_CONCURRENCY` | before the fix | after the fix | speedup |
|---|---|---|---|
| 1 | 6.17 s | 6.07 s | 1.0× |
| 10 | 6.17 s | **0.66 s** | **9.2×** |

Before the fix the two were identical: `MAX_CONCURRENCY` did nothing. The unit of work was the
input buffer, not the call — one source thread took a buffer and ran its batches one after another
while the others waited. After the fix an intake thread splits each buffer into one work item per
batch and the workers pull batches, so the option now means what it says.

### 5b. Many input buffers

760 rows, which arrive in roughly twenty buffers.

| `MAX_CONCURRENCY` | before | after | ideal (760 × 200 ms ÷ conc) | speedup |
|---|---|---|---|---|
| 1 | 152.61 s | 152.56 s | 152.0 s | 1.0× |
| 5 | 32.37 s | 30.55 s | 30.4 s | 5.0× |
| 10 | 17.12 s | 16.51 s | 15.2 s | 9.2× |
| 30 | 7.91 s | **5.26 s** | 5.07 s | **29.0×** |

After the fix the curve tracks the ideal within a few percent, and the earlier saturation at about
19× — which was the number of buffers — is gone.

**Configuration.** Mock backend with `'echo@200' AS LLM.ENDPOINT`, i.e. 200 ms per request and no
network; `BATCH_SIZE 1` throughout; 30 rows (`reviews_30.csv`) for 5a and 760 (`reviews_760.csv`)
for 5b and for section 4; `number_of_worker_threads=1` for the asynchronous queries, which do not
use the pool, and 1 / 4 / 8 for section 4; Debug build; `Checksum` sink over `id`. Test:
[`SemanticMapAsyncThroughput.test`](../../nes-systests/semantic/SemanticMapAsyncThroughput.test),
driven by `scripts/measure_async_semantic_map.sh throughput`.

---

## 6. An earlier ceiling, which turned out to be the server's

Before the cluster was available, the same comparison against Ollama looked disappointing, and the
record of why matters: it is the measurement that sent us to the mock backend for section 5.

**The server's own limit.** Issuing the same request 1, 4 and 8 times concurrently:

| concurrent requests | wall time |
|---|---|
| 1 | 21.4 s |
| 4 | 22.7 s |
| 8 | 45.5 s |

Ollama serves four requests in parallel (`OLLAMA_NUM_PARALLEL`, default 4) and queues the rest. No
client-side concurrency can exceed 4× against that installation.

**40 reviews, one record per request.** The synchronous path on a single worker thread is serial by
construction, which gives the serial reference:

| | 40 rows | vs. serial |
|---|---|---|
| synchronous, 1 worker thread (serial reference) | 379.6 s | 1.00× |
| asynchronous, `MAX_CONCURRENCY 8`, before the fix | 382.5 s | 0.99× |
| asynchronous, `MAX_CONCURRENCY 8`, after the fix | 310.8 s | 1.22× |
| what the server's 4-way parallelism allows | ≈ 95 s | 4.0× |

1.22× against an available 4× looked like a ceiling in our code. **Section 2 shows there is none:**
given a server that serves 256 requests at once, the framework reaches an effective concurrency of
262. What limited this measurement was Ollama — its four-way parallelism, and per-call latency that
roughly doubled under even that load.

> A correction worth keeping: an earlier write-up put this at 2.76× by using 21.4 s as the per-call
> latency. That figure came from a hand-written approximation of the prompt, not from the codec. The
> serial reference above, 9.5 s per call, comes from the synchronous path itself and is the right
> one.

**Configuration.** 40 rows (`reviews_40.csv`), `qwen2.5:7b` on Ollama at `localhost:11434/v1`,
`BATCH_SIZE 1`, `180 AS LLM.TIMEOUT_SECONDS`, `0 AS LLM.MAX_RETRIES`;
`number_of_worker_threads=1`, which is why the synchronous run is the serial reference; Debug build.
Test: [`AsyncBenchmarkProbe.test`](../../nes-systests/semantic/AsyncBenchmarkProbe.test), driven by
`scripts/measure_async_semantic_map.sh probe`.

---

## 7. Correctness, which all of this rests on

A faster path that quietly drops records is worthless, so every measurement asserts results.

| | |
|---|---|
| Asynchronous system tests (mock backend) | 10 / 10 |
| Synchronous system tests (mock backend) | 8 / 8 |
| Window downstream of SEM_MAP, synchronous and asynchronous | 5 / 5 |
| Unit tests (framework, executor, splitter, catalog, binder) | 50 / 50 |

The window test is the one worth naming. The consumer source hands the engine records it did not
produce itself, so it passes `OriginId`, `SequenceNumber` and the watermark of the incoming buffer
through unchanged and only assigns chunk numbers. Getting that wrong breaks nothing visibly — a
watermark that never advances means a window that never fires, so the symptom is *missing* output,
which a checksum over records cannot catch. The test asserts exact window counts for 760 rows
across sixteen windows with ten calls in flight, summing to exactly 760, synchronously and
asynchronously, and the two agree row for row.

---

## 8. How latency is measured

Throughput from wall time cannot tell queueing apart from service time, so the round trips are
timed where both modes pass through them, in `HttpSemanticBackend`. Each mode writes one summary
into the systest log when the query stops:

```
Semantic model latency [asynchronous]: 2000 calls (0 failed), mean 1263 ms,
p50 1147 ms, p95 1960 ms, max 2257 ms, 207.8 calls/s
```

Mean, median, p95, maximum and the call rate actually achieved. Timed across retries, because that
is the wait the operator experiences. The runner computes records per second itself, since systest
reports only wall time.

Set `NES_SEMANTIC_LATENCY_REPORT=1` to get the same line on stderr. That exists because a release
build compiles with `NES_LOGLEVEL_WARN`, which removes `NES_INFO` from the binary altogether — and
a release build is exactly where a benchmark runs, so the first cluster run produced no latency line
at all. A measurement must not depend on how the logger was compiled; an ordinary deployment stays
quiet because the variable is not set.

---

## 9. Reading and quoting these numbers

The two things most likely to be asked about section 2:

- **The blocking run had one worker thread**, which is what systest's default topology
  ([`two-node.yaml`](../../nes-systests/configs/topologies/two-node.yaml)) gives, and the experiment
  asked for defaults. Say it before someone finds it; section 4 is the answer to what more threads
  would buy, and section 2a shows both sides did what they were configured to do.
- **The blocking maximum of 9192 ms is a single outlier** against a p95 of 533 ms. The asynchronous
  distribution is the tighter one despite the higher mean, its maximum being 2257 ms.

And the limits of the whole set:

- **Build type differs by section.** Sections 3 to 7 ran on a Debug build, where absolute times are
  pessimistic; section 2 ran on a release build. The comparisons hold within each section, because
  both execution modes are affected equally and the mock measurements are dominated by a configured
  sleep rather than by our code.
- **One machine, single runs.** No repetitions, no variance figures. The mock-backend numbers are
  stable because the delay dominates; the real-model numbers are not — model latency varied visibly
  between runs, which is why the latency distribution is reported alongside the mean.
- **Synthetic input.** The reviews were generated by `scripts/generate_async_benchmark_data.py` with
  a fixed seed, matching the real dataset only in shape and length (one short paragraph, 11–26
  words). **No quality or accuracy claim can be derived from them.** The real benchmark dataset is
  the Rotten Tomatoes critic reviews the Python reference uses, whose `scoreSentiment` column is the
  ground truth; it was not available on this machine.
- **The worker thread count is the blocking path's concurrency.** Left at its default, systest's
  topology gives one thread, which costs that path everything and the asynchronous path nothing. The
  cluster runner deliberately does not override it — the experiment asks for defaults — and prints
  the effective count with every run, so the number can be read correctly. The local measurement
  script does pass it, because sections 3 and 4 vary it on purpose.
- **Ollama's four-way limit** applies to every number in sections 3 and 6 and is a property of that
  installation, not of either execution mode.
- **The engine ran on a laptop** in every measurement here, including section 2, where only the
  model ran on the cluster. The absolute throughput is therefore not a statement about engine
  capacity on server hardware.

---

## 10. What is not measured yet

- Time-to-first-result. `'FALSE' AS LLM.PRESERVE_ORDER` now exists and is covered by a system test,
  but the latency it is meant to buy has not been measured — only that the records all arrive.
- Several asynchronous queries at once. Ten queries at `MAX_CONCURRENCY 16` would be 160 threads,
  which nothing currently bounds.
- Chunking, when output records do not fit one buffer. The code mirrors
  `EmitOperatorHandler::setChunkNumber`, but the answers in these tests are short enough that the
  path never runs.
- Cancellation while calls are in flight, including whether buffers return to the pool cleanly.

---

## 11. Reproducing

### 11a. The local measurements

Sections 3 to 7.

```bash
ninja systest
scripts/generate_async_benchmark_data.py                      # writes cmake-build-debug/bench-data
scripts/measure_async_semantic_map.sh throughput              # sections 4 and 5, mock, ~7 min
scripts/measure_async_semantic_map.sh responsiveness          # section 3, needs Ollama, ~6 min
scripts/measure_async_semantic_map.sh probe                   # section 6, needs Ollama, ~20 min
```

The real-model modes need Ollama on `localhost:11434` with `qwen2.5:7b` pulled, and the container
started with `--network host`.

### 11b. The cluster benchmark

#### Once: a release build

Sections 3 to 7 were measured on a Debug build. For the cluster run it matters, because 257 threads
and a good deal of buffer bookkeeping are on the critical path, so build optimised. Inside the
development image:

```bash
cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_TARGET_TRIPLET=x64-linux-none-local -DVCPKG_MANIFEST_MODE=OFF
cmake --build cmake-build-release --target systest
```

#### Reaching the endpoint

The endpoint is OpenAI-compatible, so an SSH tunnel is enough. Leave it open in its own terminal:

```bash
ssh -N -L 8000:localhost:8000 <cluster>
```

The development image runs with `--network host`, so a tunnel bound on the host's loopback is
reachable from inside the container. Then take the exact model id — vLLM rejects a mismatched one:

```bash
curl -s localhost:8000/v1/models | jq -r '.data[].id'
```

#### The run

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

#### Choosing `--rows`

Do the smoke test first and read the real per-call latency off its latency line, then pick the row
count so the **blocking** run — one call at a time at the default thread count — still finishes in a
sensible time. Blocking takes roughly `rows × latency`; asynchronously it is divided by the
concurrency the server sustains. Reporting records per second keeps different row counts comparable.

#### Reading the result

```bash
grep "Semantic model latency" $(ls -t cmake-build-release/nes-systests/SystemTest_*.log | head -1)
```

One line per run, labelled `[synchronous]` or `[asynchronous]`. Throughput is printed by the script
after each run. The generated test file under `nes-systests/semantic/` is gitignored: it carries a
machine's model id and is not meant to be committed.

---

## Appendix A: exact configuration

Everything needed to repeat a number or to judge whether it transfers.

### Code

| | |
|---|---|
| Branch | `feat/async-operator` |
| Commit | `0cbfb49f09` |
| Input generator | `scripts/generate_async_benchmark_data.py`, fixed seed `20261006` |

### Build

| | sections 3–7 (local) | section 2 (cluster) |
|---|---|---|
| Directory | `cmake-build-debug` | `cmake-build-release` |
| `CMAKE_BUILD_TYPE` | `Debug` | `RelWithDebInfo` (`-O2`) |
| Log level | `NES_LOGLEVEL_TRACE` | `NES_LOGLEVEL_WARN` |
| Compiler | clang 19.1.7 | clang 19.1.7 |
| Toolchain | vcpkg, triplet `x64-linux-none-local` | same |
| Image | `nebulastream/nes-development:160915ef…-x64-libstdcxx-none`, run with `--network host` | same |

The log level is why the cluster run needed `NES_SEMANTIC_LATENCY_REPORT=1`: a release build has no
`NES_INFO` in it.

### Host running the engine

Intel Core i7-8650U, 4 cores / 8 threads, 1.90 GHz, 15 GiB RAM, x86_64, Linux. A laptop — the engine
side of the cluster run ran here while the model ran on the cluster, so the absolute throughput is
not a statement about engine capacity on server hardware.

### Worker configuration

Everything at its default unless a measurement says otherwise. The defaults that matter:

| Option | Value | Where from |
|---|---|---|
| `worker.query_engine.number_of_worker_threads` | **1** | systest topology [`two-node.yaml`](../../nes-systests/configs/topologies/two-node.yaml), on both nodes. The engine's own default is 4. |
| `worker.query_engine.admission_queue_size` | 1000 | `QueryEngineConfiguration` |
| `worker.total_memory_in_bytes` | 268435456 (256 MiB) | `WorkerConfiguration` |
| `worker.unpooled_memory_fraction` | 0.5 | → 128 MiB pooled |
| `operator_buffer_size` | 4096 bytes | → 32768 pooled buffers |
| `default_max_inflight_buffers` | 64 | per source |

systest deploys onto **two nodes** (`source-node`, `sink-node`), so every plan is cut once by the
ordinary network decomposition before the asynchronous splitter sees it.

### What the asynchronous framework derives from `MAX_CONCURRENCY`

Not configurable from SQL; listed because they decide buffer pressure and thread count.

| | Formula | At 256 |
|---|---|---|
| Source threads | `maxConcurrency` + 1 intake | 257 |
| Handoff channel capacity | `max(64, 2 × maxConcurrency)` | 512 buffers |
| Intake read-ahead | `2 × maxConcurrency` queued batches | 512 |
| Pending-buffer backstop | `2 × maxConcurrency` | 512 buffers of 32768 |
| `preserveOrder` | `LLM.PRESERVE_ORDER`, default `TRUE` | `TRUE` in every measurement here |

### Model endpoints

| | sections 3 and 6 | section 2 |
|---|---|---|
| Server | Ollama, `localhost:11434/v1` | vLLM on `sr650-8:8000`, reached as `localhost:8000/v1` through `ssh -N -L 8000:localhost:8000 sr650-8` |
| Model | `qwen2.5:7b` (4.7 GB) | `google/gemma-4-E4B-it` |
| Concurrency the server serves | 4 (`OLLAMA_NUM_PARALLEL` default). Measured with a prompt of the codec's own length: 1 → 21.4 s, 4 → 22.7 s, 8 → 45.5 s, so four in parallel are nearly free and the fifth queues. (A shorter throwaway prompt gave 3.4 / 3.9 / 7.9 / 15.9 s for 1 / 4 / 8 / 16 — same shape, and the reason the latency figures in section 6 must come from the real path.) | ≥ 256 (its default); an effective 262 was reached |
| `LLM.BACKEND` | `http`, or `mock` where stated | `http` |

Sections 4 and 5 use the mock backend instead of a server: `'echo@200' AS LLM.ENDPOINT` answers with
the input upper-cased after sleeping 200 ms, so they measure this framework and nothing else.

### Input data

Synthetic, from the generator above: one short paragraph per row, 11–26 words, mean 18.1, comma-free
because the CSV source does not quote. Schema `(id UINT64 NOT NULL, reviewText VARSIZED NOT NULL)`,
except the window test which adds `ts UINT64 NOT NULL`.

**The texts are invented, so no quality or accuracy number can come from any of this.** The real
benchmark dataset is the Rotten Tomatoes critic reviews the Python reference uses, whose
`scoreSentiment` column is the ground truth; it is not present in the `llm_operator` checkout on
this machine, which is a known gap there.

| File | Rows | Used by |
|---|---|---|
| `reviews_12.csv` | 12 | section 3 |
| `reviews_30.csv` | 30 | section 5a |
| `reviews_40.csv` | 40 | section 6 |
| `reviews_760.csv` | 760 | sections 4 and 5b |
| `reviews_2000.csv` | 2000 | section 2 |
| `plain_200.csv` | 200 | the model-free query of section 3 |
| `windowed_small.csv` / `windowed_large.csv` | 12 / 760 | section 7's window test |

### Measurement method

Wall time comes from systest's `--show-query-performance`, one query at a time (`--sequential`)
unless a measurement runs two concurrently (`-n 2`), which only section 3 does. Throughput is rows
divided by wall time. Latency is recorded per round trip in `HttpSemanticBackend`, across retries,
and summarised once per query — see [section 8](#8-how-latency-is-measured). Single runs, no
repetitions; see [section 9](#9-reading-and-quoting-these-numbers).

### Dates

Sections 3 to 7 measured 6 and 7 October 2026, the cluster run of section 2 on the night of
7–8 October 2026. Each number is a single run; the real-model ones in particular varied between
runs, which is why the latency distribution is reported alongside the mean.
