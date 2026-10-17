# Async framework: our implementation compared with Lukas' prototype

> **Status:** 16 October 2026.
> Compares `feat/async-framework-v1` (b2c843be6f, our state when the review came in) with `origin/ls/async` (8b1a5bfe03, Lukas' prototype).
> The follow-up document [20261016_async_framework_extension.md](20261016_async_framework_extension.md) describes how we extend the prototype.

## 1. Summary

Both implementations solve the same problem.
An operation that waits for an external service must not block one of the engine's worker threads.
Both also solve it the same way: the plan is cut at the operation into a handoff sink and an async source, and the source does the waiting outside the worker pool.

They differ in **what** is asynchronous.

- **In our implementation it is an operator.**
  A whole projection becomes the asynchronous stage, and one executor per operator type processes batches of records.
- **In Lukas' prototype it is a function.**
  A single call inside an expression is asynchronous.
  One generic optimizer rule lifts every such call out of the plan, and the implementation is one coroutine per row.

Almost everything else follows from this.
Lukas' approach makes every new operation cheap and lets it combine freely with the rest of SQL.
Ours was further ahead in execution: batching, concurrency across buffer boundaries, configurable limits, HTTPS, and hardened lifecycle handling.

| | our implementation | Lukas' prototype |
|---|---|---|
| asynchronous unit | operator (whole projection) | function call inside an expression |
| a new operation needs | LogicalFunction + PlanRule + executor class | LogicalFunction + coroutine |
| optimizer | one rule per operation, strict shape requirements | one generic `AsyncFunctionLiftingRule` |
| SQL composability | only `SELECT ..., f(x) AS y` | arbitrary expressions, `WHERE`, nesting, deduplication |
| plan splitting | separate `AsyncOperatorSplitter` after decomposition | inside `QueryDecomposition`, next to network channels |
| runtime | a thread per call (blocking) or an event loop per source | one shared `io_context`, one strand per source |
| batching | yes (`BATCH_SIZE`) | no |
| calls across buffer boundaries | yes | no, each buffer waits for all its calls |
| result types | text, converted to the declared type | VARSIZED only |
| HTTP transport | libcurl: HTTPS, retries, timeouts, latency statistics | Boost.Beast: `http://` only, fixed 30 s, one reconnect |
| configuration | per operator (concurrency, batch, channel, ordering) | fixed: 16 calls, 64 buffers, 30 s |
| modules | new: `nes-async`, `nes-http` | plugins in `nes-plugins/Async`, registry in `nes-sources` |
| size | 71 files, 8624 lines | 47 files, 3007 lines |
| tests | gtests for every layer, systest E2E with an in-process server | bats E2E against a Python server, EXPLAIN systest |

## 2. The core idea

### Our implementation: the operator is asynchronous

A plan rule marks an operator with `AsyncExecutionTrait{executorType, config}`.
After decomposition, `AsyncOperatorSplitter` replaces that operator with a handoff sink, which ends a local plan of its own, and an async source, which takes the operator's place.
The source looks up the operator's executor in `AsyncExecutorRegistry` and hands it batches of `AsyncRecordView`.

```cpp
class EventLoopAsyncExecutor : public AsyncOperatorExecutor {
    virtual void submit(std::span<const AsyncRecordView> batch, AsyncBatchCompletion done) = 0;
};
```

The executor knows the input and output schemas (`AsyncRecordLayout`), reads fields as text, and returns a list of `(field index, value)` per record.

### Lukas' prototype: the function call is asynchronous

A function marks itself by deriving from `AsyncLogicalFunction`.
It has no Nautilus implementation; instead it has a coroutine:

```cpp
class AsyncFunction {
    virtual boost::asio::awaitable<std::string> call(std::vector<std::string> arguments) = 0;
};
```

`AsyncFunctionLiftingRule` splits every projection that contains such a call into three parts:
`pre` projection (synchronous) → `ASYNC_PROJECTION` (asynchronous) → `post` projection (synchronous).

- The `pre` projection computes arguments that are neither a field nor a constant, and reserves an empty result field for every call.
- The `ASYNC_PROJECTION` only fills in those fields.
- The `post` projection is the original projection, now reading the result fields.

`QueryDecomposition` replaces the `ASYNC_PROJECTION` with a handoff sink and an async source.
The calls travel as a JSON list (`CALLS`) in the source descriptor.

## 3. The differences in detail

### 3.1 What a new asynchronous operation costs

**Our implementation.**
`HTTP_CALL` needed three plugins: the `LogicalFunction` without a PhysicalFunction, the rule `HttpCallAsyncRule`, and `HttpCallExecutor` on top of the `HttpOperatorExecutor<Scratch>` base with `buildRequest` and `decode`.
On top came option parsing (`AsyncOperatorOptions`) and field resolution through the layout.

**Lukas.**
`HTTP_CALL` needs the `LogicalFunction` and a coroutine of about 120 lines; `ASYNC_DELAY` needs one of about 30 lines.
There is no rule of its own.

**Advantage Lukas:**
This is the heart of his review.
Functions are written "over and over", so the complexity belongs in the infrastructure.
In our implementation every operation repeated the rule, the layout access and the error mapping.

**Disadvantage Lukas:**
The interface only knows "one row in, one string out".
Anything beyond that cannot be expressed: several rows per request, several values per request, configuration per function.
See 3.7 and section 5.

### 3.2 Optimizer: a rule per operation or a generic lifting rule

**Our implementation.**
The rule replaces the *whole* projection.
Anything else the projection computed would silently disappear.
The rule is therefore strict: it accepts exactly one `HTTP_CALL` whose result becomes a new field, and next to it only fields passed through unchanged.
`HTTP_CALL` in a `WHERE`, computed arguments, synchronous work on the result, and nested calls are all rejected.

**Lukas.**
Because of the `pre`/`async`/`post` split, all of that is allowed:

- `CONCAT(HTTP_CALL(CONCAT(data, '!'), url), '?')`: the computed argument is materialized in `pre`, and the `CONCAT` on the result runs in `post`.
- `HTTP_CALL(HTTP_CALL(data, a), b)`: directly nested calls run in **one** stage, level by level per row.
- `HTTP_CALL(CONCAT(HTTP_CALL(...), x), b)`: synchronous work between two calls produces two stages with a projection in between.
- `CONCAT(ASYNC_DELAY(x, 10), HTTP_CALL(x, url))`: different asynchronous functions run in parallel within one stage.
- `WHERE HTTP_CALL(...) = '...'`: the predicate is evaluated into a field and filtered synchronously afterwards.
- Structurally equal calls are evaluated once, and a `SELECT` on top of such a `WHERE` reads its results instead of calling again.

**Advantage Lukas:**
Asynchronous functions behave in SQL like any other function.
For users and for new operations, this is the biggest gain.

**Disadvantage Lukas:**
The rule is more complex (about 400 lines).
It has to run before the pushdown rules, because they rebuild projections and would drop the trait (`wants()`).

### 3.3 Plan splitting

**Our implementation.**
`AsyncOperatorSplitter` runs as a separate step *after* `QueryDecomposition` over the distributed plan.
It is idempotent, removes the marker, and emits producer plans as additional local plans, with the consumer first.

**Lukas.**
The split happens *inside* `QueryDecomposition`, in the same place where network channels are inserted.
There, an asynchronous stage is the same pattern as a network boundary: sink, channel, source.

**Advantage Lukas:**
One concept less in the optimizer, and it uses a place reviewers already know.

**Advantage ours:**
We found and fixed a hang in the start order: the consumer has to start before the producer.
The prototype does not show that hang: its channel is created by whichever half comes first and kept until the source releases it, and the lifecycle tests pass in both execution modes.

### 3.4 Runtime: threads and event loop

**Our implementation.**
There were two kinds of executors, and the kind decided the cost:

- `BlockingAsyncExecutor`: the source runs `maxConcurrency` threads of its own, i.e. one thread per call in flight.
- `EventLoopAsyncExecutor`: the source runs no worker threads, only an intake thread and a pump. The executor owns an `HttpEventLoop` (curl multi on asio), one per source, but built so it can be shared.

At concurrency 256 this brought the thread count down from 257 to 2.

**Lukas.**
There is a single `io_context` with one thread for *all* async sources of a worker, and one strand per source.
Functions are coroutines; a waiting call occupies no thread.
The source thread only waits for the calls of its buffer to finish.

**Advantage Lukas:**
One model instead of two.
No thread per source or per call, and no "blocking or event-driven" decision that every operation has to make.
Coroutines also make nested and parallel calls within a row simple (`parallel_group`).

**Disadvantage Lukas:**
A single loop thread for all queries of a worker.
Expensive work inside a coroutine, such as parsing large JSON responses, slows down every query.
The strands make more loop threads safe to add later (`ponytail` in the code).
Blocking libraries do not fit in without offloading them to a thread pool of their own.

### 3.5 Data path and buffers

**Our implementation.**
`AsyncRecordLayout` describes the input and output schemas.
Executors read fields as text (`readAsText`); results come back as text and are converted to the declared type, numeric types included.
`copyMatchingFields` copies passed-through fields *by name*.
If the output does not fit into one buffer, because the operator appends fields, it is spread over further chunks of the same sequence number.

**Lukas.**
The `pre` projection reserves every result field, so input and output rows have the same fields.
One input buffer is copied byte for byte into exactly one output buffer.
Child buffers (VARSIZED) are taken over in the same order, so their indices stay valid.
Results are appended as new VARSIZED children.
Arguments and results are non-nullable VARSIZED only.

**Advantage Lukas:**
Simpler and faster.
No chunk splitting, no text conversion of passed-through fields, and metadata such as sequence number and watermark are trivially correct.

**Advantage ours:**
Numeric and nullable result fields.
SEM_MAP does not need them today, since all its output fields are VARSIZED.

### 3.6 Concurrency across buffer boundaries

**Our implementation.**
The unit of work is a batch of records (`[begin, end)` of a buffer), not the buffer.
An intake thread reads ahead up to `2 × maxConcurrency` batches.
`maxConcurrency` therefore really means "calls in flight", regardless of how many rows a buffer has.
`PRESERVE_ORDER` (on by default) hands results on in input order.

**Lukas.**
`fillTupleBuffer` takes one buffer, starts `min(16, rows)` coroutines, and waits until all of them have finished.
Only then does it take the next buffer.
With one row per buffer, as delivered by a slowly emitting generator or by TCP, exactly one call runs at a time.
The prototype notes this itself as a `ponytail`.

**Advantage ours:**
The 84-fold throughput increase in `docs/design/20261006_async_operator_measurements.md` (branch `feat/async-operator`) came from 256 calls in flight at `BATCH_SIZE 1`.
With Lukas' behavior it would be bounded by buffer boundaries.

### 3.7 Batching and several values per call

**Our implementation.**
`BATCH_SIZE` records go into one `submit`, and a result may set any number of fields.
SEM_MAP uses both: several rows in one prompt, and several `OUTPUT` fields from one answer.
`feat/semantic-filter` adds the fusion of SEM_MAP and SEM_FILTER into one prompt, and gives the result a `keep` flag there.

**Lukas.**
One call per row, one string per call.

**Advantage ours:**
For LLM operators this is decisive.
Batching saves prompt overhead and requests, and a model with several output fields, or a fused map-and-filter request, answers everything in a single request.

### 3.8 HTTP transport

| | our implementation (`nes-http`) | Lukas (`HttpCallAsyncFunction`) |
|---|---|---|
| library | libcurl (blocking, and multi-socket on asio) | Boost.Beast |
| HTTPS | yes | no (`ponytail`: add `ssl_stream`) |
| timeouts | per request and per connect, configurable | fixed 30 s |
| retries | `maxRetries` with `100 ms · 2^n` on transport failure, 429, 5xx | one reconnect, in case a keep-alive connection was closed in the meantime |
| connections | reuse through curl's connection cache | one keep-alive connection per function instance |
| measurement | `HttpLatencyStats` | none |
| dependency | `curl` in vcpkg | `boost-beast` in vcpkg |

Compared with `main`, both need a new vcpkg dependency and therefore a new dev image.

### 3.9 Channel, backpressure, lifecycle

**Common to both:**
Both pass buffers through a bounded in-process channel.
When the channel is full, the sink applies backpressure to the producer's sources through the existing `BackpressureHandler`.

**Our implementation:**

- The registry holds only `weak_ptr`s; the lifetime belongs to the two halves.
- The source acquires the channel in its constructor already.
- A source destroyed without being opened signs off with the producer.
- The consumer starts before the producer.
- `inputExhausted` is monotone.
- A failure is part of every wait predicate.

Each of these rules comes from a hang we found (see the test list in 3.12).

**Lukas:**

- The registry holds `shared_ptr`s; whoever comes first creates the channel, and the source releases it in `close()`.
- Fixed capacity of 64.
- On a stop, the source sends asio cancellation to all running calls; the first failure is thrown on the source thread.

Tested against the cases our implementation once failed, the prototype survives a failure while the channel is full, and INTERPRETER mode.
It does not survive a `DROP` under backpressure: the producer half keeps retrying against a channel nobody drains and stays `Running`.
The follow-up document describes the fix.

### 3.10 Configuration

**Our implementation:**
Per operator, the rule sets `EXECUTOR_CONFIG` (arbitrary keys), `BATCH_SIZE`, `MAX_CONCURRENCY`, `PRESERVE_ORDER` and `CHANNEL_CAPACITY`.
SEM_MAP takes them from the model in the catalog.

**Lukas:**
16 calls, 64 buffers and a 30 s timeout are fixed.
Functions receive nothing when they are created (`AsyncFunctionRegistryArguments` is empty).

### 3.11 Modules and dependencies

**Our implementation:**
Two new modules: `nes-async` (framework, executor registry, source, sink) and `nes-http` (transport, test server).
A deferred CMake check makes sure that `nes-http` exists.

**Lukas:**
No new module.
Source, sink and channel are plugins in `nes-plugins/Async`.
The `AsyncFunction` registry lives in `nes-sources`, the `AsyncLogicalFunction` marker in `nes-logical-operators`, and the lifting rule and the trait in `nes-query-optimizer`.

**Advantage Lukas:**
It fits into existing extension points (source, sink, function and rule registries) instead of creating new ones.

### 3.12 Tests

**Our implementation:**
gtests for every layer:

- `HandoffChannelTest`, `AsyncLifecycleTest`, `AsyncExecutorTest`, `AsyncRecordLayoutTest`;
- `HttpOperatorExecutorTest`, `CurlHttpBackendTest`, `HttpEventLoopTest`;
- `AsyncOperatorSplitterTest`.

In addition, a systest E2E (`async-http-e2e-test`) with an in-process `DummyHttpServer`.
The full suite passed in compiler and interpreter mode (1098/1098).

**Lukas:**
A bats suite (`async-repl-test`) against a Python asyncio server covers:

- generated URLs;
- pass-through, computed arguments and nesting;
- `WHERE`;
- chaining and reuse;
- concurrency;
- failure, and cancellation through DROP;
- a stress test with 20 queries, and a backpressure test.

In addition, `explain/AsyncFunctionLifting.test`.
There are no unit tests for the channel or the source.

**Assessment:**
His E2E coverage of the SQL side is broader; our coverage of lifecycle races is deeper.

## 4. Overall assessment

**Where Lukas' prototype is better:**

1. The extension interface: a new operation is a coroutine.
2. Composability in SQL: expressions, `WHERE`, nesting, deduplication.
3. A single runtime model with a shared loop instead of two kinds of executors.
4. It builds on existing extension points and is a third of the size.
5. The simpler data path: 1:1 buffers, trivially correct metadata.

**Where our implementation was better:**

1. Concurrency across buffer boundaries, i.e. throughput with small buffers.
2. Batching and several values per call, i.e. everything LLM operators need.
3. Configuration per operator.
4. The transport: HTTPS, configurable timeouts, retries with backoff, latency measurement.
5. The hardened lifecycle and the unit tests for it.
6. Numeric and nullable result fields.

**Conclusion:**
The points on Lukas' side are *structural*.
They cannot be retrofitted into our implementation without rebuilding it.
The points on our side are *capabilities of the infrastructure*.
They can be retrofitted into Lukas' implementation without changing its structure.
That is why we build on his prototype.

## 5. What this means for the semantic operators

In our implementation, SEM_MAP and SEM_FILTER are relations in the FROM clause (`FROM SEM_MAP(model, stream)`) with logical operators of their own, not scalar functions.
They need four things from the framework that the prototype does not have:

1. **Batching:** `LLM.BATCH_SIZE` rows in one prompt.
2. **Several values per call:** several `OUTPUT` fields of a model, and, with fusion, map columns plus a filter verdict from one answer.
3. **Configuration per function:** endpoint, prompt, output values, timeout, retries and `MAX_CONCURRENCY` from the model catalog.
4. **HTTPS:** for hosted models.

On top of that, the FROM operators need a translation into function calls, which the lifting rule then takes over.
The follow-up document describes what that looks like.
