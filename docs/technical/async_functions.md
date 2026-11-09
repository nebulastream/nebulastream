# Asynchronous Functions

This document describes how NebulaStream evaluates functions that wait on something outside the query, such as an HTTP
service, without blocking a worker thread per call. It is aimed at contributors who add such a function or work on the
infrastructure that runs them.

## Overview

A regular function is compiled into the pipeline by Nautilus and evaluated row by row on a worker thread. A function
whose result comes from a remote call cannot be evaluated that way: a worker thread would sit idle for the whole round
trip, and the calls of a buffer would run one after the other.

An *asynchronous function* therefore has no Nautilus implementation. Instead:

1. The optimizer lifts every call of such a function out of its projection or selection into an operator of its own,
   the *async stage* (`AsyncFunctionLiftingRule`).
2. The decomposition replaces each async stage by a pair of operators on the same worker: a `Handoff` sink that ends the
   pipeline before the stage, and an `Async` source that starts the pipeline after it (`QueryDecomposition`).
3. The `Async` source runs the calls as coroutines on one Boost.Asio event loop that all async sources of a worker share,
   with many calls in flight at once, and emits the buffers with the results filled in.

All of the complexity lives in this infrastructure. A function only implements a coroutine that turns the arguments of
one row into its result.

The framework ships two functions:

| Function                                   | Result                                                                                                              |
|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `HTTP_CALL(body, url[, timeout_ms])`       | POSTs `body` to `url` and returns the response body. Plain `http://` only. The timeout defaults to 30 s.           |
| `ASYNC_DELAY(value, millis[, concurrency])` | Returns the value with its input type after waiting `millis`; its second output is the elapsed delay as `UINT64`. A test and benchmark aid.          |

`ASYNC_OUTPUT(call, i)` reads the i-th value of a call that produces several (`ASYNC_OUTPUT(call, 0)` is the call
itself).

```sql
SELECT id, HTTP_CALL(text, 'http://127.0.0.1:8080/classify') AS label FROM reviews INTO labels;
SELECT id, text FROM reviews WHERE HTTP_CALL(text, 'http://127.0.0.1:8080/classify') = 'spam' INTO spam;
```

## Lifting the calls into async stages

`AsyncLogicalFunction` (`nes-logical-operators/include/Functions/AsyncLogicalFunction.hpp`) marks a logical function as
asynchronous. `AsyncFunctionLiftingRule` (`nes-query-optimizer/src/Rules/Static/AsyncFunctionLiftingRule.cpp`) finds
every projection that calls such a function and splits it into

```
pre-projection (sync)  ->  async stage  ->  post-projection (sync)
```

- The **pre-projection** evaluates every argument that is neither a field nor a constant, e.g. `CONCAT(data, '!')`, and
  reserves an empty result slot per call.
- The **async stage** is a projection carrying the `AsyncExecutionTrait`. It forwards its input and fills each slot with
  its call, so its input and output have the same fields.
- The **post-projection** is the original projection, now reading the slots.

```
SINK(CALLSINK)
  PROJECTION(fields: [ID, CONCAT(__ASYNC_CALL_1, ?) as RESPONSE])
    ASYNC_PROJECTION(fields: [DATA, ID, __ASYNC_ARG_0, HTTP_CALL(__ASYNC_ARG_0, http://localhost/echo) as __ASYNC_CALL_1])
      PROJECTION(fields: [DATA, ID, CONCAT(DATA, !) as __ASYNC_ARG_0,  as __ASYNC_CALL_1])
        SOURCE(TEXTSTREAM)
```

The rule also handles:

- **Selections.** A predicate that calls an async function is evaluated into a field of such a split projection, the
  selection filters on that field, and a projection removes the helper fields again. A projection directly above reuses
  the calls the predicate made instead of making them again.
- **Nesting.** Directly nested calls (`f(g(x))`) run in one async stage, the inner call feeding the outer one. Sync work
  between two calls (`f(CONCAT(g(x), y))`) needs one stage per side, with the sync work in the projection between them.
- **Duplicates.** Structurally equivalent calls with the same inferred types are evaluated once per row. A field and
  an equally spelled string literal are distinct, as are equally spelled constants of different types.

`nes-systests/explain/AsyncFunctionLifting.test` shows the resulting plans for all of these cases.

## Splitting a stage into a Handoff sink and an Async source

`splitAsyncOperator` in `nes-query-optimizer/src/Placement/QueryDecomposition.cpp` replaces every async stage, on the
worker it was placed on, by

- a `Handoff` sink (`nes-plugins/Async/HandoffSink.cpp`) on top of the stage's child, which becomes a local plan of its
  own, and
- an `Async` source (`nes-plugins/Async/AsyncSource.cpp`) in the stage's place.

Both carry the same channel id. The source receives typed calls as JSON (`CALLS`, a list of `AsyncCall` from
`nes-sources/include/Sources/AsyncFunction.hpp`): function name, declared result types, and typed arguments sourced from
a handed-off field, a typed constant, or an earlier call output, plus the destination fields. Argument positions are
preserved: an absent optional constant means that position is supplied dynamically. Constants are parsed once when the
function binding is created. Argument and result types are `DataType` directly. Calls are reflected/unreflected through
NES `ReflectionContext`, including the existing `DataType` reflector; `rfl::json` only encodes/decodes the intermediate
`rfl::Generic` representation. No async-specific type descriptor or reflect-cpp adapter is needed.

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

### The Handoff channel

`HandoffChannel` (`nes-plugins/Async/HandoffChannel.hpp`) is a bounded in-process queue between the two halves; buffers
pass through it without a copy. Each half holds a `HandoffChannel::Lease` from its construction to its destruction, so
the channel is released even on a hard stop or when one half never opens. Until a source has attached, the channel takes
no buffers.

When the channel is full, the `Handoff` sink applies backpressure through the sink's `BackpressureHandler`: it stashes
the buffer, holds back the sources of the producing pipeline, and retries until the Async source caught up. The channel
capacity is thus what absorbs bursts. Once the Async source has stopped, the sink drops what it still has and releases
the producer.

### The Async source

The source thread of an `Async` source does not run calls itself. All calls run as coroutines on one event loop thread
shared by every Async source on the worker (`SharedEventLoop` in `AsyncSource.cpp`), each source on its own strand.
There is no dedicated thread per call. Each async stage still has an ordinary source thread waiting for results.

- **Concurrency across buffers.** The source keeps a window of handed-off buffers and starts their rows as long as a
  slot is free, up to `max_concurrency` rows at once, regardless of which buffer they belong to. It takes the next buffer
  from the channel only when the rows waiting to start would no longer keep every slot busy, so the channel, not the
  source, holds what the calls cannot keep up with.
- **Order and metadata.** Buffers are emitted in the order they were handed off, each once all of its rows are done.
  One input buffer becomes exactly one output buffer with the same sequence and chunk number, origin and watermark. All
  fields that are not call results are copied byte for byte, and variable-sized data stays valid, because the child
  buffers are carried over in the same order.
- **Function instances.** Each slot has its own instance of every function in the stage, created through the
  `AsyncFunctionRegistry`, so an instance serves one call at a time and may keep state between calls, such as an open
  connection.
- **Failures.** A failed call is retried up to `max_retries` times, the n-th after 100 ms · 2ⁿ, unless the query is
  stopping. A call that still fails fails the query: the source cancels every call in flight and rethrows the error.
- **Stop.** On a stop request, the source cancels its calls and waits until none of them is running before it returns.

### Configuration

The decomposition passes these optimizer options to every async stage, e.g.
`nes-repl -- --optimizer.async.max_concurrency=64`:

| Option                           | Default | Meaning                                                                    |
|----------------------------------|---------|----------------------------------------------------------------------------|
| `optimizer.async.max_concurrency`  | 16      | Rows whose calls are in flight at once, per async stage.                   |
| `optimizer.async.channel_capacity` | 64      | Buffers the Handoff channel holds before the producer is held back.        |
| `optimizer.async.max_retries`      | 0       | Further attempts of a failed call, with exponential backoff.               |

A function can lower the concurrency of its stage through `AsyncFunction::maxConcurrency`, e.g. for a service with a
rate limit; the stage uses the smallest bound.

## Adding an asynchronous function

An asynchronous function is a plugin with two parts, see `nes-plugins/Functions/AsyncDelay` for a complete example.

**The logical function** is a regular logical function plugin (see [How to add a function](../guide/how_to_add_a_function.md))
that derives from `AsyncLogicalFunction` instead of providing a physical function. It checks its arguments and result type
in `withInferredDataType`, including which child positions must be constant. It implements `getAsyncOutputTypes()` to
declare every output type, including nullability. The output count follows from this list; `ASYNC_OUTPUT` selects one
output. These types must follow from the expression, and the first output type must match `getDataType()`.

**The async implementation** derives from `AsyncFunction` and is registered in the `AsyncFunctionRegistry` of
`nes-sources`:

```cpp
class MyAsyncFunction final : public AsyncFunction
{
public:
    static AsyncFunctionRegistryReturnType create(AsyncFunctionRegistryArguments arguments);

    /// Owns the typed arguments across suspension and returns all typed outputs.
    boost::asio::awaitable<AsyncValues> call(AsyncValues arguments) override;
};
```

`create` receives `arguments.binding`, an `AsyncFunctionBinding` containing declared argument/result types and
positional optional constants. There is no separate per-function configuration payload. Constant arguments remain
visible at their original positions in `call`, just like dynamic arguments; factories may inspect them to initialize
state or a concurrency limit. Constants are decoded once when binding, with their exact types (no floating-point
roundtrip for integer literals).

`AsyncValues` is an immutable, cheap-to-copy owning handle, so a coroutine can keep its arguments across suspension and
the scheduler can retain them for retries. Indexing returns a `RuntimeValueView`, which borrows from that owner and must
not outlive it. Use `get<int32_t>()` for exact scalar access, `get<std::string_view>()` or `bytes()` for VARSIZED, and
`isNull()` before accessing a nullable payload. There is no implicit numeric narrowing; `getUnsignedInteger()` is an
explicit checked conversion for options/durations that accept any non-negative integer width. Use `toOwned()` when a
value must escape the bundle's lifetime. For example:

```cpp
const auto millis = arguments[1].getUnsignedInteger();
// ... co_await a timer ...
co_return AsyncValues::make(arguments[0].toOwned(), millis);
```

The current source path materializes owned values; the view interface leaves room for future buffer-backed storage
without changing the function call API. `RuntimeValue` and `RuntimeValueView` have no tuple-buffer or Nautilus dependency.
VARSIZED is a byte sequence, not necessarily UTF-8, and views do not promise a terminating NUL. Runtime-only free adapters
`toVarVal(RuntimeValueView)` and `fromVarVal(const VarVal&, DataType)` in `DataTypes/RuntimeValueNautilus.hpp` provide the
Nautilus bridge without requiring a tuple buffer. The former borrows bytes; the latter copies them into owned storage.
Neither adapter may be used while tracing a compiled pipeline.

Return the declared number of values with exact scalar types; null is allowed only for a nullable output. A non-null
native value can be returned for a nullable output. Result shape/type errors fail the query without retrying. Inside the
coroutine, wait only with `co_await` on Asio operations; blocking stalls every async source on the worker.

Both parts are registered in the plugin's `CMakeLists.txt`, the logical function under the SQL name and the
implementation under the same plugin name:

```cmake
add_registry_entry(LogicalFunction MyFunction KEY MY_FUNCTION)
add_unreflection_entry(LogicalFunction MyFunction)

add_library(my_async_function_plugin_library STATIC MyAsyncFunction.cpp)
target_link_libraries(my_async_function_plugin_library PRIVATE nes-sources)
link_plugin_library(nes-sources my_async_function_plugin_library)
add_registry_entry(AsyncFunction MyFunction)
```

## Limitations

- Native scalar and string values are supported, including typed nullable values where the function declares them. `HTTP_CALL` continues to require non-null `VARSIZED` body, URL, and result.
- `HTTP_CALL` speaks plain HTTP/1.1 without TLS.
- The shared event loop runs on one thread per worker. The per-source strands would allow more threads if completing
  calls ever becomes the bottleneck.

## Tests

- `nes-plugins/Async/tests/async.bats` runs end-to-end queries in the embedded REPL against
  `tests/http_path_server.py`, once compiled and once interpreted (`async-repl-test`, `async-repl-test-interpreter`).
  It covers typed delay values and outputs, nullable pass-through, computed/field-valued integer delays, chaining, selections, concurrency within and across buffers, backpressure, failures, retries, timeouts and dropping queries.
- `nes-data-types/tests/UnitTests/RuntimeValueTest.cpp` covers native types, typed nulls, exact integer limits and ownership.
- `nes-nautilus/tests/UnitTests/RuntimeValueNautilusTest.cpp` covers runtime Nautilus conversions without tuple buffers.
- `nes-sources/tests/AsyncValuesTest.cpp` covers suspension lifetimes, constant binding and typed plan serialization.
- `nes-plugins/Async/tests/AsyncSourceValueTest.cpp` checks result arity, type and nullability errors are not retried.
- `nes-plugins/Async/tests/HandoffChannelTest.cpp` covers the channel's leases.
- `nes-systests/explain/AsyncFunctionLifting.test` checks the lifted and the decomposed plans.
