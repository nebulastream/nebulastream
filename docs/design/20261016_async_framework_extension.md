# Async framework: how we extend Lukas' prototype

> **Status:** 16 October 2026.
> Builds on [20261016_async_framework_comparison.md](20261016_async_framework_comparison.md).
> Describes how `feat/async-framework` is rebuilt on Lukas' prototype (`origin/ls/async`), and how the semantic operators then sit on top of it.

## 1. Goal and guiding rule

Lukas' review asks us to move complexity out of the functions and into the infrastructure.
His prototype shows how: asynchronous functions are coroutines, a generic rule lifts them out of the plan, and a shared event loop runs them.
We take that over unchanged as the base.
His two commits stay in the history with his authorship.

On that base, we bring back the strengths of our earlier implementation.
One rule applies:

> **An extension may make the infrastructure more complex, but not the simple function.**
> `HTTP_CALL`, and `ASYNC_DELAY` without its optional argument, stay a coroutine `call(args) -> string`.
> Every new capability is optional for a function, with a default that asks nothing of it.

Each extension is its own commit, with a test that fails without it.

## 2. Overview

| # | commit | brings back (see the comparison, section 4) | status |
|---|---|---|---|
| 0 | Lukas: `fix(QueryManager)…` and `feat(Async)…` | – | taken over unchanged, applies to `main` without conflicts |
| 1 | `fix(async): stop the producer half of a dropped async stage`, with the lifecycle tests | hardened lifecycle | green; the drop test found a hang in the prototype |
| 2 | configurability, retries, `HTTP_CALL` timeout | configuration, transport | green |
| 3 | calls across buffer boundaries | throughput with small buffers | green |
| 4 | batching, several values per call, configuration per function | everything LLM operators need | green |
| – | `style(async)`: clang-format 22, justified catch-all | – | `scripts/format.sh` clean |
| 5 | HTTPS | transport | open, see 4.4 |

All 20 tests of `async-repl-test` pass in compiler and in interpreter mode, and `explain/AsyncFunctionLifting.test` passes (10/10).
The full ctest suite passes except for one timing bound, see section 5.
The build uses `NES_LOG_LEVEL=DEBUG`, as CI does: the prototype's backpressure test greps a debug message and fails at a lower level.

Because of `boost-beast`, the dev environment needs a new image.
It has been built locally; CI builds it by itself on push.

## 3. The extensions in detail

### 3.1 Lifecycle hardening (commit 1)

**Why:**
Our earlier implementation had several hangs that only showed in INTERPRETER mode or under backpressure.
They involved the channel lifetime, the start order, and an end flag that was not monotone.
The prototype has its own code in these places, but no tests for them.

**What:**
New cases in `nes-plugins/Async/tests/async.bats`:

- A service that answers only 500s after 300 responses, while the channel is full and the producer is held back.
  Expected: the query fails in under 10 s, and after the failure at most the calls already in flight reach the service.
- A `DROP` during backpressure.
  Expected: every part of the query has stopped, in under 30 s.
- Two queries on the shared loop, one of which fails at once.
  Expected: the other delivers all of its rows.
- The whole suite runs a second time in INTERPRETER mode (`async-repl-test-interpreter`).
- Every REPL run is limited to 2 minutes, so that a hang shows as a failure instead of blocking ctest.

The test server gets new parameters `fail_first=K` and `fail_after=N` for this.

**Result of the baseline run (prototype plus these tests):**
The failure case and the isolation case passed.
The drop case failed: after `DROP`, the consumer half stopped, but the producer half stayed `Running`.
`AsyncSource::close()` released the channel, and the handoff sink kept retrying against the full channel every 10 ms, which nobody would ever drain again, so the producer's pipeline never finished.

**Fix:**
The channel now learns, when the source releases it, that its consumer is gone; it drops the buffers that were waiting and refuses every later push with `CONSUMER_GONE`.
The sink then drops the rest and releases the producer's sources instead of retrying.
This follows `NetworkSink`, which voids further buffers once its receiver closed, but without throwing: after a `DROP` the query ends as stopped, not as failed.

### 3.2 Configurability, retries, timeout (commit 2)

**Why:**
The prototype has fixed values: 16 calls, 64 buffers, 30 s.
The SEM_MAP measurements ran with 256 calls in flight.

**What:**

- A new `QueryOptimizerAsyncConfiguration` next to the network configuration, set for example with `--optimizer async.max_concurrency=256`.
  It has three options:
  - `max_concurrency`: calls in flight per stage;
  - `channel_capacity`: buffers in the channel before backpressure starts;
  - `max_retries`: further attempts of a failed call.
- `splitAsyncOperator` passes the options that are set on to the source and the sink, following the same pattern as `receiver_queue_size` for network channels.
- **Retries live in the source, not in the function:**
  `callWithRetries` repeats a failed call with a backoff of `100 ms · 2^n` (capped), but not when the query is stopping.
  Every function benefits from this, and `HTTP_CALL` keeps its 120 lines.
- `HTTP_CALL(body, url[, timeout_ms])`: the optional third argument replaces the fixed 30 s.
  The prototype suggests this itself in its `ponytail` comment.

**Tests:**

- `fail_first=2` with `async.max_retries=2`: all rows arrive, after 5 requests. Without the option, the query fails.
- A 500 ms timeout against a service that takes 5 s to answer fails the query.
- `async.max_concurrency=1` turns 3 × 1 s of delay into at least 3 s.

### 3.3 Calls across buffer boundaries (commit 3)

**Why:**
In the prototype, each buffer waits for all of its calls.
With one row per buffer, the concurrency is therefore 1.

**What:**
The async source keeps a window of admitted buffers.

- Up to `max_concurrency` calls run on the strand, whichever buffer their rows belong to.
- A new buffer is only taken when a slot would otherwise run idle, and the window is bounded.
  Whatever the calls cannot keep up with therefore stays in the channel, and backpressure works as before.
- The source thread always writes the *oldest* finished buffer, so the output stays in input order.
  Our old `PRESERVE_ORDER` was on by default and goes away.
- Results are collected per row and only written into the source thread's buffer when it is emitted.
  This needs no extra buffers from the pool.
- For this, the channel gets a non-blocking `tryPop` and reports through a listener when a buffer arrives or the channel closes.
- Stop, failure and `close()` cancel all running calls and wait until none is running and no handler of the source is left on the strand.
  Only then may the source be destroyed.

**Test:**
20 rows, each in a buffer of its own, each with a 2 s delay.
Processed buffer by buffer, this takes 40 s; with the window it takes about 4 s.
The test requires under 15 s.

### 3.4 Batching, several values per call, configuration per function (commit 4)

**Why:**
These are the three capabilities the semantic operators need from the framework (section 4).

**Interface (`nes-sources/include/Sources/AsyncFunction.hpp`):**

```cpp
using AsyncCallResult = std::vector<std::string>;  // values of one row's call

class AsyncFunction {
public:
    /// One row in, one value out. The only thing a simple function implements.
    virtual awaitable<std::string> call(std::vector<std::string> arguments);
    /// Several rows of one buffer in, one result with any number of values per row out.
    /// Default: one call() per row.
    virtual awaitable<std::vector<AsyncCallResult>> callBatch(std::vector<std::vector<std::string>> rows);
    virtual size_t maxBatchSize() const { return 1; }
    virtual std::optional<size_t> maxConcurrency() const { return std::nullopt; }
};

struct AsyncFunctionRegistryArguments { std::string configuration; };
```

**Logical side (`AsyncLogicalFunction`):**

- `getAsyncConfiguration()` returns a string that the implementation receives when it is created, for example the serialized model.
  It has to follow from what `explain` shows, because calls that look alike are evaluated only once.
- `getNumberOfAsyncOutputs()` says how many values a call produces. The default is 1.
- The new core function `ASYNC_OUTPUT(call, i)` reads the i-th value.
  Since equal calls run only once anyway, `f(x)` and `ASYNC_OUTPUT(f(x), 1)` together cost *one* request.
  The prototype's lifting rule did not have to change for this; only the decomposition resolves `ASYNC_OUTPUT` to its call and value index.

**Source:**

- A slot processes a batch of consecutive rows of one buffer.
- The batch size is the smallest `maxBatchSize` of the stage's functions; the concurrency is the minimum of the option and the functions' `maxConcurrency()`.
- Retries repeat the whole batch.
- If a function returns too few results or values, the query fails with a clear message and without a retry.

**Demonstrator without an LLM:**
`ASYNC_DELAY(value, millis[, batch_size])` waits once per batch.
Its second value is the number of rows the wait covered.

**Tests:**

- 3 rows with `batch_size 3` at `max_concurrency=1` finish in under 3 s, and every row reports `3`.
- Without `batch_size`, every row reports `1`, and it takes at least 3 s.
- `ASYNC_OUTPUT(HTTP_CALL(...), 1)` is rejected, because `HTTP_CALL` produces only one value.

**Known limitation:**
If a stage mixes a batching function with one that handles single rows only, the whole stage runs row by row.
The lifting rule does not separate such calls into stages of their own.
This does not happen for SEM_MAP alone.

### 3.5 What deliberately does not come back from our earlier implementation

| earlier building block | why not |
|---|---|
| `BlockingAsyncExecutor` / a thread per call | coroutines on the shared loop replace both kinds of executors |
| `AsyncOperatorSplitter` | the split happens in `QueryDecomposition` |
| a rule per operation (`HttpCallAsyncRule`) | the generic lifting rule takes over |
| `AsyncRecordLayout`, text conversion, chunk splitting | 1:1 buffers with reserved result fields |
| `PRESERVE_ORDER` | the output is always in input order |
| `nes-http` with libcurl | the prototype uses Beast; two HTTP stacks would be one too many (see 4.4) |
| numeric result fields | the model catalog only allows VARSIZED anyway; a numeric value can be converted with `VarSizedToNumeric` in the `post` projection |

## 4. The semantic operators on the extended framework

### 4.1 What they do today on `feat/async-operator`

- `CREATE SEM_MODEL` stores a model in the `SemanticModelCatalog`: input and output fields (VARSIZED only), prompt, endpoint, backend (`http` or `mock`), `LLM.EXECUTION` (`SYNC` or `ASYNC`), `BATCH_SIZE`, `MAX_CONCURRENCY`, timeouts and retries.
- `FROM SEM_MAP(model, stream)` produces a logical operator of its own.
  - With `SYNC`, it becomes the Nautilus operator `SemanticMapPhysicalOperator`.
  - With `ASYNC`, `SemanticMapResolutionRule` attaches `AsyncExecutionTrait{"SemanticMap", payload}`, and `SemanticMapExecutor` (an `EventLoopAsyncExecutor`) builds one prompt per batch with `SemanticMapCodec`, sends it, and distributes the answer over the output fields.
- `feat/semantic-filter` adds `SEM_FILTER(model, stream)` and fusion.
  A filter on top of a map becomes *one* request that returns the map columns and a filter verdict per row.

### 4.2 Mapping them onto asynchronous functions

The operators stay as they are in SQL, in the catalog and in the logical plan.
What is new is a rewrite rule in `nes-semantic`.
It runs before `AsyncFunctionLiftingRule`, and only for `LLM.EXECUTION ASYNC`.
It translates the operators into ordinary projections and selections with an asynchronous function:

```text
SEM_MAP(m, s)  with OUTPUT (a, b)
  ⇒  PROJECTION(*, SEM_CALL(m, in1, in2) AS a, ASYNC_OUTPUT(SEM_CALL(m, in1, in2), 1) AS b)
       s

SEM_FILTER(f, s)
  ⇒  PROJECTION(*)  -- drops the helper field
       SELECTION(ASYNC_OUTPUT(SEM_CALL(f, in1), 0) = 'true')
         s

SEM_FILTER(f, SEM_MAP(m, s))  with fusion
  ⇒  one SEM_CALL whose steps are [a, b, filter]; selection on value 2, projection of values 0 and 1
```

`SEM_CALL` is an `AsyncLogicalFunction` in `nes-semantic`:

- `getAsyncConfiguration()` returns the serialized `SemanticModelConfig` including its steps, i.e. exactly what travels as the trait's payload today.
- `getNumberOfAsyncOutputs()` is the number of steps.
- `explain` names the model, so calls to the same model within one plan are merged correctly.

From there on, the framework does everything else:

- the lifting rule, for the filter too, through its `WHERE` path;
- decomposition, channel and backpressure;
- batching, concurrency, retries and ordering.

A row is dropped by the synchronous selection *after* the asynchronous stage.
The `keep` flag from `feat/semantic-filter` therefore goes away.

The implementation `SemanticCallAsyncFunction` overrides `callBatch`:

```cpp
awaitable<std::vector<AsyncCallResult>> callBatch(std::vector<std::vector<std::string>> rows) override
{
    auto prompt = codec.buildPrompt(toRowPayloads(rows));          // unchanged SemanticMapCodec
    auto answer = co_await transport.complete(prompt);              // HTTP or mock, see 4.4
    co_return toResults(codec.parse(answer, rows));                // one value per step and row
}
size_t maxBatchSize() const override { return model.batchSize; }
std::optional<size_t> maxConcurrency() const override { return model.maxConcurrency; }
```

### 4.3 What stays and what goes

**Stays:**

- The SQL syntax, the catalog, `SemanticMapCodec` with its golden prompts, and the synchronous path.
- The failure rules: an unusable answer yields the step's default value, or "rejected" for a filter; a transport failure fails the query.
- The mock backend, as a timer on the loop just like today.
- The latency report `NES_SEMANTIC_LATENCY_REPORT`, and with it comparability with the measurements.

**Goes:**

- `SemanticMapExecutor`, `SemanticAsyncWiring`, and the attaching of `AsyncExecutionTrait` in `SemanticMapResolutionRule`.
- The dependency on `nes-async`.

**Gained:**

- SEM_MAP results can be used directly in expressions, and a SEM_FILTER on top of an `HTTP_CALL` runs in the same stage.
- Several semantic operators in one query share one loop instead of each having its own.

### 4.4 Open: HTTPS and the transport

Hosted models require HTTPS; the prototype's `HTTP_CALL` only speaks `http://`.
There are two ways:

1. **Beast with `ssl_stream`** (OpenSSL is in the image).
   One shared awaitable client for `HTTP_CALL` and `SEM_CALL`.
   This keeps a single HTTP stack, and retries live in the infrastructure anyway.
2. **Our curl multi code as an awaitable** on the shared `io_context`.
   Proven and already with HTTPS, but it would mean two HTTP stacks and two vcpkg dependencies.

Recommendation: option 1, as a separate commit after commit 4, agreed with Lukas.

### 4.5 Order

1. `feat/async-framework` with commits 0 to 4 (later 5) green in both execution modes, then a force push and a reply to Lukas.
2. After the merge, `feat/async-operator` moves to `SEM_CALL` as described in 4.2.
   The measurements from `20261006_async_operator_measurements.md` are repeated.
   We expect the same results at the same concurrency, with one loop for all sources instead of one per source.
3. `feat/semantic-filter` follows with SEM_FILTER and fusion on the same base.

## 5. Risks and open questions

- **One loop thread for all queries.**
  Parsing the LLM answers runs on that thread.
  At about 200 answers per second and microseconds per answer this is not critical, but we measure it in step 4.5.2.
  If needed, more loop threads can be added; the strands make that safe.
- **A batch does not cross a buffer boundary.**
  With very small buffers, a batch stays smaller than `BATCH_SIZE`.
  Our earlier implementation behaved the same way.
- **Lukas' agreement to the interfaces.**
  `callBatch`, `ASYNC_OUTPUT` and the configuration per function extend *his* interface.
  They are built so that simple functions notice none of it.
  We put them up for discussion explicitly with the push.
- **The stress test's time bound is tight under load.**
  `HTTP_CALL stress` (20 queries in parallel) asserts under 20 s.
  It takes 11 to 12 s when the async suites run alone, on the prototype and with our commits alike, but took 23 s in a full ctest run with eight tests in parallel; every other assertion of the test held.
  If CI runs bats tests in parallel, the bound may fail sporadically.
  The prototype was not measured under the same load, so a contribution of our commits under load is not ruled out.
- **Each extension's test was not run against the commit before it.**
  The drop test was shown to fail on the prototype; the tests of commits 2 to 4 test features that do not exist before those commits (options, `HTTP_CALL`'s third argument, `ASYNC_DELAY`'s batch size), and the overlap test's bound (under 15 s instead of 40 s) follows from the prototype's code rather than from a run.
