# Asynchronous Functions

This document describes how NebulaStream evaluates functions that wait on something outside the query, such as an HTTP service, without blocking a worker thread for every call.
It is aimed at contributors who add such a function or work on the infrastructure that runs them.

## Overview

A regular function is compiled into the pipeline by Nautilus and evaluated row by row on a worker thread.
A function whose result comes from a remote call cannot be evaluated that way.
The worker thread would sit idle for the whole round trip, and the calls of a buffer would run one after the other.

An *asynchronous function* therefore has no Nautilus implementation.
Instead, NebulaStream evaluates it in three steps:

1. The optimizer lifts every call of such a function out of its projection or selection into an operator of its own, the *async stage* (`AsyncFunctionLiftingRule`).
2. The query decomposition replaces each async stage by a pair of operators on the same worker (`QueryDecomposition`).
   A `Handoff` sink ends the pipeline before the stage, and an `Async` source starts the pipeline after it.
3. The `Async` source runs the calls as coroutines on a Boost.Asio event loop that all async sources of a worker share.
   Many calls are in flight at once, and the source emits the buffers once their results are filled in.

All of the complexity lives in this infrastructure.
A function only implements a coroutine that turns the arguments of one row into its result.

NebulaStream ships two asynchronous functions:

| Function                                    | Result                                                                                         |
|---------------------------------------------|------------------------------------------------------------------------------------------------|
| `HTTP_CALL(body, url[, timeout_ms])`        | POSTs `body` to `url` and returns the response body. The timeout defaults to 30 s.             |
| `ASYNC_DELAY(value, millis[, concurrency])` | Returns `value` after waiting `millis`. Its second value is the delay it waited. Used for tests and benchmarks. |

`ASYNC_OUTPUT(call, i)` reads the i-th value of a call that produces several values.
`ASYNC_OUTPUT(call, 0)` is the call itself.

```sql
SELECT id, HTTP_CALL(text, 'http://127.0.0.1:8080/classify') AS label FROM reviews INTO labels;
SELECT id, text FROM reviews WHERE HTTP_CALL(text, 'http://127.0.0.1:8080/classify') = 'spam' INTO spam;
```

## Lifting the Calls into Async Stages

`AsyncLogicalFunction` (`nes-logical-operators/include/Functions/AsyncLogicalFunction.hpp`) marks a logical function as asynchronous.
`AsyncFunctionLiftingRule` (`nes-query-optimizer/src/Rules/Static/AsyncFunctionLiftingRule.cpp`) finds every projection that calls such a function and splits it into three operators:

```
pre-projection (sync)  ->  async stage  ->  post-projection (sync)
```

- The **pre-projection** evaluates every argument that is neither a field nor a constant, e.g. `CONCAT(data, '!')`.
  It also reserves an empty result slot for every call.
- The **async stage** is a projection that carries the `AsyncExecutionTrait`.
  It passes its input through and fills each result slot with its call, so its input and output have the same fields.
- The **post-projection** is the original projection, which now reads the result slots.

```
SINK(CALLSINK)
  PROJECTION(fields: [ID, CONCAT(__ASYNC_CALL_1, ?) as RESPONSE])
    ASYNC_PROJECTION(fields: [DATA, ID, __ASYNC_ARG_0, HTTP_CALL(__ASYNC_ARG_0, http://localhost/echo) as __ASYNC_CALL_1])
      PROJECTION(fields: [DATA, ID, CONCAT(DATA, !) as __ASYNC_ARG_0,  as __ASYNC_CALL_1])
        SOURCE(TEXTSTREAM)
```

The rule also handles the following cases:

- **Selections.** A predicate that calls an async function is evaluated into a field of such a split projection.
  The selection then filters on that field, and a projection removes the helper fields again.
  If a projection directly above uses the same calls, it reuses their results instead of calling again.
- **Nesting.** Directly nested calls such as `f(g(x))` run in one async stage, where the inner call feeds the outer one.
  If there is sync work between two calls, such as in `f(CONCAT(g(x), y))`, each call needs its own stage, and the sync work runs in the projection between them.
- **Duplicates.** Calls with the same `explain` output are evaluated only once per row.

`nes-systests/explain/AsyncFunctionLifting.test` shows the resulting plans for all of these cases.

## Splitting a Stage into a Handoff Sink and an Async Source

`splitAsyncOperator` in `nes-query-optimizer/src/Placement/QueryDecomposition.cpp` replaces every async stage on the worker it was placed on.
It inserts two operators:

- a `Handoff` sink (`nes-plugins/Async/HandoffSink.cpp`) on top of the stage's child, which becomes a local plan of its own, and
- an `Async` source (`nes-plugins/Async/AsyncSource.cpp`) in place of the stage.

Both operators carry the same channel id.
The configuration of the source holds the calls of the stage as JSON (`CALLS`, a list of `AsyncCall` from `nes-sources/include/Sources/AsyncFunction.hpp`).
For each call, it contains the name and configuration of the function, the output fields that receive its values, and where each argument comes from.
An argument is either a handed-off field, a constant, or a value of an earlier call of the same stage.
`EXPLAIN (DISTRIBUTED)` shows the pair:

```
-- 2 plan(s) on source-node:8080 --
0:
SINK(HANDOFF)
  PROJECTION(fields: [DATA, ID,  as __ASYNC_CALL_0])
    SOURCE(TEXTSTREAM)
1:
SINK(NETWORK)
  SOURCE(ASYNC)
```

## Execution

### The Handoff Channel

`HandoffChannel` (`nes-plugins/Async/HandoffChannel.hpp`) is a bounded in-process queue between the two halves of a stage.
Buffers pass through it without being copied.
Each half holds a `HandoffChannel::Lease` from its construction to its destruction.
This way, the channel is released even on a hard stop or when one half never opens.
Until a source has attached to the channel, it does not accept any buffers.

When the channel is full, the `Handoff` sink applies backpressure through its `BackpressureHandler`.
It keeps the buffer, holds back the sources of the producing pipeline, and retries until the Async source has caught up.
The capacity of the channel therefore determines how large a burst it can absorb.
Once the Async source has stopped, the sink drops the buffers it still holds and releases the producer.

### The Async Source

The source thread of an `Async` source does not run any calls itself.
All calls run as coroutines on one event loop thread that every Async source on the worker shares (`SharedEventLoop` in `AsyncSource.cpp`).
Each source runs on its own strand of that loop.
As a result, neither a call nor a source needs a thread of its own.

- **Concurrency across buffers.** The source keeps a window of handed-off buffers and starts their rows whenever a slot is free.
  Up to `max_concurrency` rows run at once, no matter which buffer they belong to.
  The source only takes the next buffer from the channel when the rows that are waiting to start could no longer keep every slot busy.
  This way, the channel and not the source holds the buffers the calls cannot keep up with.
- **Order and metadata.** Buffers are emitted in the order they were handed off, each one once all of its rows are done.
  One input buffer becomes exactly one output buffer with the same sequence number, chunk number, origin and watermark.
  All fields that are not call results are copied byte by byte.
  Variable-sized data stays valid, because the child buffers are carried over in the same order.
- **Function instances.** Each slot has its own instance of every function in the stage, created through the `AsyncFunctionRegistry`.
  An instance therefore serves only one call at a time and may keep state between calls, such as an open connection.
- **Failures.** A failed call is retried up to `max_retries` times, the n-th time after 100 ms · 2ⁿ, unless the query is stopping.
  If a call still fails, it fails the query: the source cancels every call in flight and rethrows the error.
- **Stop.** On a stop request, the source cancels its calls and waits until none of them is running before it returns.

### Configuration

The query decomposition passes the following optimizer options to every async stage, e.g. `nes-repl -- --optimizer.async.max_concurrency=64`:

| Option                             | Default | Meaning                                                              |
|------------------------------------|---------|----------------------------------------------------------------------|
| `optimizer.async.max_concurrency`  | 16      | Rows whose calls are in flight at once, per async stage.             |
| `optimizer.async.channel_capacity` | 64      | Buffers the Handoff channel holds before the producer is held back.  |
| `optimizer.async.max_retries`      | 0       | Further attempts of a failed call, with exponential backoff.         |

A function can lower the concurrency of its stage through `AsyncFunction::maxConcurrency`, e.g. for a service with a rate limit.
The stage then uses the smallest bound of all its functions.

## Adding an Asynchronous Function

An asynchronous function is a plugin with two parts.
See `nes-plugins/Functions/AsyncDelay` for a complete example.

**The logical function** is a regular logical function plugin (see [How to add a function](../guide/how_to_add_a_function.md)).
Instead of providing a physical function, it derives from `AsyncLogicalFunction`.
It checks its arguments and result type in `withInferredDataType`, and it may override two methods:

- `getAsyncConfiguration()` hands the implementation everything that is fixed per query rather than per row.
  Since calls with the same `explain` output are evaluated only once, the configuration must follow from what `explain` shows.
- `getNumberOfAsyncOutputs()` is needed if a call produces several values, which `ASYNC_OUTPUT` then reads.

**The async implementation** derives from `AsyncFunction` and is registered in the `AsyncFunctionRegistry` of `nes-sources`:

```cpp
class MyAsyncFunction final : public AsyncFunction
{
public:
    static AsyncFunctionRegistryReturnType create(AsyncFunctionRegistryArguments arguments);

    /// One row: its arguments in, its value out. Takes the arguments by value: a coroutine has to own what it reads
    /// after its first suspension.
    boost::asio::awaitable<std::string> call(std::vector<std::string> arguments) override;
};
```

`create` receives the configuration from `getAsyncConfiguration`.
A function that produces several values overrides `callValues` instead of `call`.
Inside the coroutine, only wait with `co_await` on Asio operations.
Anything that blocks stalls every async source on the worker.

Both parts are registered in the `CMakeLists.txt` of the plugin.
The logical function is registered under its SQL name, and the implementation under the same plugin name:

```cmake
add_registry_entry(LogicalFunction MyFunction KEY MY_FUNCTION)
add_unreflection_entry(LogicalFunction MyFunction)

add_library(my_async_function_plugin_library STATIC MyAsyncFunction.cpp)
target_link_libraries(my_async_function_plugin_library PRIVATE nes-sources)
link_plugin_library(nes-sources my_async_function_plugin_library)
add_registry_entry(AsyncFunction MyFunction)
```

## Limitations

- Arguments and results are non-nullable `VARSIZED` values.
- The shared event loop runs on one thread per worker.
  The per-source strands would allow more threads if completing calls ever becomes the bottleneck.

## Tests

Whether an async stage works depends on how it behaves against the service it calls.
This includes how many calls are in flight, what happens when the service is slow or fails, and whether the producer is held back in the meantime.
Systests cannot show this, since they run queries on fixed inputs against expected results and have no service to call.

We therefore test the async functions end to end against a real service.
For every test, `nes-plugins/Async/tests/async.bats` starts a small HTTP service (`tests/http_path_server.py`), runs queries against it in the embedded REPL, and checks what the queries write to their sinks.
The service answers each request with its path and body, so a test can tell which call produced which result.
Parameters in the URL make the service slow or let it fail on purpose.
This drives a query into exactly the situation a test is about, such as backpressure, a retry or a timeout.
The suite runs once in compiled and once in interpreted mode.
If you add a function that calls a different kind of service, test it the same way, against a small local stand-in for that service.

Everything that needs no service is tested as usual.
`nes-systests/explain/AsyncFunctionLifting.test` checks the plans that the lifting rule and the decomposition produce, and `HandoffChannelTest.cpp` checks the leases of the Handoff channel.
