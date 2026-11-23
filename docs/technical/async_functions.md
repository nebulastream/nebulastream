# Asynchronous operators

Async operators perform work such as HTTP requests without blocking one query worker per request. Each operator consumes
one input tuple and appends its named results to that tuple. The current runtime produces exactly one output tuple per
successful input tuple; it is not a general table-valued function with variable output cardinality.

## SQL shorthand and invocation counts

For now, the query language exposes only scalar shorthand for operators with **one output**:

```sql
SELECT *, HTTP_CALL(body, url) FROM requests INTO responses;
SELECT id, HTTP_CALL(body, url) AS reply FROM requests INTO responses;
SELECT id, ASYNC_DELAY(data, 1000) AS delayed FROM texts INTO delayed_texts;
```

An unaliased direct `HTTP_CALL` expression adds the operator's named `response` output. `ASYNC_DELAY` has one named
`value` output, preserving its input datatype and nullability. Explicit aliases work as for ordinary projections.
If output names collide, provide distinct aliases; calls are never merged to resolve a collision.

The operator representation supports multiple named outputs, but the scalar shorthand rejects such operators. Table/lateral
syntax such as `FROM HTTP_CALL(SELECT ...)` is not implemented yet.

**Every written async call is a distinct invocation.** Argument structure, inferred types, and EXPLAIN text do not establish
call identity. With retries disabled and successful execution:

- `SELECT HTTP_CALL(body, url) AS a, HTTP_CALL(body, url) AS b ...` makes two requests per input tuple.
- `HTTP_CALL(CONCAT(HTTP_CALL(body, u), HTTP_CALL(body, u)), v)` makes three requests per tuple: two distinct inner calls and one outer call.
- A call in `WHERE` and another identical call in `SELECT` are separate.
- A subquery that computes a result once can expose that field more than once in an outer projection without another call.
- Discarding an async result in an outer projection does not remove the invocation.
- A filter above an async operator cannot move below it and suppress its side effects.
- Shared references introduced by `IN`/`BETWEEN` and `IS NAN` desugaring execute the referenced async call only once.

This is an invocation-count contract, **not transactional exactly-once HTTP delivery**. Explicit retries can repeat a POST
that the server already processed. Failures or cancellation can interrupt calls.

Async calls in supported boolean predicates are evaluated before their selection. Conditional/lazy expression contexts and
unsupported placements such as join predicates are rejected rather than eagerly evaluating effects from unselected branches.

## Normalization and optimizer barriers

`AsyncFunctionLiftingRule` normalizes scalar shorthand after type inference and before the semantic barrier and static
pushdowns. Its historical rule name remains for configuration compatibility; it no longer builds trait-tagged projections.

The rule walks expression occurrences in order:

1. Lower nested calls first.
2. Materialize nontrivial synchronous arguments in ordinary projections, forwarding the existing input fields.
3. Insert one async operator for the occurrence, with fresh internal output bindings.
4. Replace that occurrence with a field access to its sole output.
5. Retain the original result projection or selection over those fields.

The original visible schema is captured before introducing helpers. `SELECT *` expands against that schema, and helper
fields introduced for a predicate are removed before its result is exposed to parent operators. Generated names avoid
input fields, user aliases, and other generated names.

Within one expression region, pointer identity resolves shared function-DAG references to an already produced field.
`TypeInferenceContext` preserves that sharing while rebuilding functions, including across all expression roots of a
projection. It never merges separately allocated calls by comparing arguments or structure. WHERE and SELECT are independent
regions; no cross-clause reuse lookup exists. Logical-function reflection also preserves DAG aliases through explicitly
injected shared-pointer state; no async-specific occurrence UUID is needed.
Nested calls initially use one
operator/stage each. Synchronous work between calls remains an ordinary projection between operators. There is no
structural collection/substitution pass and no EXPLAIN-derived expression key.

Async operators are effect barriers. Predicate and watermark pushdown cannot cross them, and projection pruning must retain
the invocation even when none of its results is selected. Initially, conservative full-schema retention is preferred to a
more aggressive pruning optimization. Ordinary projections above the operator may discard fields normally.

## Physical handoff and row layout

The optimizer runs [AsyncOperatorDecompositionRule](../../nes-query-optimizer/include/Rules/Static/AsyncOperatorDecompositionRule.hpp#L34)
after field/layout optimization and worker placement, and before distributed network decomposition. The rule uses
`PlanVisitor` to rebuild operators bottom-up, captures handoff sinks as additional roots, and replaces each async operator
with an `Async` source on the same worker. Network decomposition then partitions all resulting roots without performing
any async lowering. The original placed global plan remains available for EXPLAIN and query metadata.

One operator currently creates one runtime graph containing one producer. All of that producer's results are exported, including results
that a later projection discards. This is essential because the graph executor starts work from exported outputs.

The logical operator appends columns, but the current buffer-reuse runtime requires identical ordered input/output schemas.
Decomposition therefore inserts a **physical padding projection** before the handoff. It forwards the input and reserves
correctly typed slots for every appended output. Nullable, variable-sized, boolean, and character placeholders retain their
actual datatypes. Field order and physical field mappings are derived for the padded layout rather than copied unchanged
from the narrower input. The current async runtime requires row layout. The planner gives both handoff endpoints that same ordered schema directly.
At source construction, `AsyncHandoffLayout` verifies identical ordered schemas (including field names, types, and nullability),
checks graph output bindings, and builds the runtime copy plan. There is one schema, one row size, and one row-layout view.
Exclusively owned input buffers are updated in place; shared buffers copy unchanged fields to the same offsets before results
are written. Reordered handoff schemas are rejected rather than adapted.

Padding is an implementation detail: it does not turn the semantic operator back into an async-tagged projection. The
returned source has the async operator's output schema, and the runtime fills the reserved slots. One admitted input buffer
still corresponds to one output buffer.

A future fusion pass may combine compatible adjacent async operators into a DAG, but must preserve every occurrence and
must not cross filters, joins, unions, or other evaluation boundaries. Such fusion is not part of the current implementation. Multi-parent logical subplans containing a shared async occurrence
are rejected until decomposition supports their fanout without duplicating execution. Reusing a produced field within one
branch remains supported.

## Channel ownership and deployment

`HandoffChannel` lives in `nes-async`, an async support library depending on `nes-memory` for tuple buffers. It exposes
move-only sender/receiver endpoints. The sink owns the sender; the source owns the receiver.

The typed source and sink configurations share a `std::shared_ptr<HandoffChannel>`. Shared-pointer reflectors receive
`std::shared_ptr<SharedPointerSerializationState>` through `ContextType`; unreflectors independently receive
`std::shared_ptr<SharedPointerDeserializationState>`. These typed states, not `ReflectionContext` itself, own pointer identity
bookkeeping. One serialization state spans a worker's whole plan batch. A fresh deserialization state reconstructs fresh
objects for each execution, including embedded execution, so a reused plan does not reuse a closed queue.

The worker compiles and instantiates the whole batch, then explicitly enumerates reconstructed `HandoffChannel` objects
through the deserialization state and calls their endpoint validation before starting any fragment. Shared-pointer reflection
only restores identity; it does not discover or invoke arbitrary pointee `validate()` methods. Deployment validation belongs
to the worker, not the generic reflection context. There is no process-global channel registry and no extra resource argument
threaded through source/sink factories.

- Closing the sender normally lets the receiver drain queued buffers and then observe end-of-stream.
- Destroying an unfinished sender aborts the receiver instead of pretending the producer completed successfully.
- Closing/destroying the receiver drops queued buffers and rejects subsequent sends.
- Configuration pointers can outlive execution; endpoint closure does not depend on the last configuration pointer disappearing.

A full channel applies backpressure to the producer. Once the receiver is gone, the sink drops pending buffers and releases
that backpressure instead of retrying forever.

## Runtime execution and ownership

All Async sources on a worker share an Asio event-loop thread. Each source uses a strand; its ordinary source thread waits
for results. Up to `MAX_CONCURRENCY` rows are in flight across buffer boundaries, and buffers are emitted in input order.

The source uses each graph call's restored concrete operator configuration, without another name lookup or factory call.
Each graph call has one operator-state group and an `AsyncExecutionContext` per admitted row slot. Contexts are reused
sequentially across rows and remain alive through preparation, retries, and draining. Stateful plugins choose an explicit scope:
`context.getOperatorState<State>()` shares one state across the call's slots, while `context.getSlotState<State>()` provides
exclusive state for one slot, such as an HTTP connection. Stateless plugins need neither. Contexts and live resources are never reflected. A row admission slot
owns the instances it needs, so a connection can be reused between sequential invocations without being shared concurrently
by two rows.

`InFlightState` owns admitted buffers in a deque. Row coroutines borrow stable entries; a completed buffer is removed only
after its last callback publishes completion. Shutdown cancels and drains all rows before releasing their buffers.

Completed input buffers are reused only when input/output layouts match and the source has exclusive parent-buffer ownership.
Otherwise unchanged fields are copied to another output buffer. Child buffers are retained rather than deep-copied, and shared
children are not extended. Output allocation never blocks while retaining input from an exhausted pool: it tries the pool,
then unpooled memory, and fails rather than deadlocking if neither is available.

Origin, sequence/chunk numbers, timestamps, and watermarks are forwarded from the input buffers.

### Runtime graphs

The generic runtime still supports typed multi-value DAGs. Arguments are owned `RuntimeValue` constants, graph-scoped
`InputFieldReference`s, or `AsyncResultReference`s owning their producer. Result indices are internal graph references, not
SQL extraction syntax. Builders reject foreign scopes and invalid selectors; wire reconstruction rejects dangling references
and cycles.

Runtime memoization uses explicit producer-node identity. Multiple references to one producer share its result; different
producer nodes remain different even if their function names and argument values match. This is not structural call deduplication.

Graph execution exposes `run()` and owner cancellation. Producer lookup is private. Internally, `SharedAsyncResult` provides
lazy single execution, cached results/errors, independent waiter cancellation/deadlines, and joining. Cancelling a subscriber's
wait does not cancel the producer. On row failure or cancellation, the owner cancels and joins all producer work before releasing
resources, preserving the initiating failure across cancellation-shielded cleanup.

## Preparation, retries, and deadlines

Plugins expose one invocation protocol:

```cpp
struct MyAsyncOperator {
    // Type, output-schema and inference methods belong to this same concrete struct.
    AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(
        AsyncExecutionContext& context, SharedRuntimeValues arguments) const;
};
```

`prepare()` validates and owns per-row arguments once, before the retry loop. On success it returns a `PreparedAsyncCall` whose
`executeAttempt()` returns `boost::asio::awaitable<AsyncResult<SharedRuntimeValues>>`. `AsyncResult<T>` is
`std::expected<T, AsyncError>`; operational failures are returned values, not thrown exceptions. The prepared call's constructor
fixes its private retry override, exposed read-only through `getRetryOverride()`: `nullopt` selects the stage default and zero
disables retries. Attempts are sequential. Prepared calls can retain actual state ownership through
`getSharedOperatorState<T>()` and `getSharedSlotState<T>()`; the latter also keeps its operator-state dependency alive.
These handles share the context's state storage, not a separate lifetime-tracking token. Shared ownership does not permit
concurrent use of a slot. Borrowed state access remains available when its caller guarantees the lifetime.
`invokeOnce(implementation, context, arguments)` uses the same result type; the convenience overload without a context owns
a fresh context until the attempt completes. Neither is another plugin extension point.

Only execution errors with `AsyncError::Type::Retryable` are eligible for retry. Preparation errors, permanent failures,
shape/type invariant failures, and cancellation are not retried. Cancellation and waiter timeout have distinct error kinds;
a waiter's failure does not overwrite the producer's cached outcome. Backoff starts at 100 ms and doubles up to 102.4 seconds.

Checked literal, integer-option, and deadline helpers also return `std::expected`. Logical inference may turn their errors into
one query diagnostic, matching existing throwing planner interfaces. Reflection and runtime construction likewise retain their
throwing interfaces and let errors reach submission/request handlers. Async execution does not catch these exceptions to make
retry decisions; terminal execution errors are reported at the established source-task boundary.

HTTP always overrides the retry default: omitting its fourth argument means zero retries, even when stage retries are enabled.
An explicit retry count allows additional attempts after transient transport failures or statuses 408, 429, 500, 502, 503, and
504. Other non-2xx responses fail immediately. A retryable classification is not an idempotency guarantee: callers opting into
POST retries must tolerate duplicate server-side effects.

Each HTTP attempt has one deadline covering hostname resolution, connect, write, and read. A retry receives a fresh budget;
backoff is outside that budget. A hostname lookup may complete after timeout/cancellation, so its callback retains shared
resolver state and ignores late results without retaining row/function resources.

## Configuration

| Option | Default | Meaning |
|---|---:|---|
| `optimizer.async.max_concurrency` | 16 | Rows in flight per async stage; implementations may lower the bound. |
| `optimizer.async.channel_capacity` | 64 | Queued buffers before backpressure. |
| `optimizer.async.max_retries` | 0 | Further attempts after a retryable execution error, unless the invocation overrides it. |

`HTTP_CALL(body, url[, timeout_ms[, retries]])` accepts plain HTTP only. Timeout defaults to 30 seconds.
`ASYNC_DELAY(value, millis[, max_concurrency])` is a test/benchmark operator; its only output is the delayed value.

## Adding an async operator

Implement one concrete configuration struct and its prepared invocation. Inference receives
`AsyncArgumentsWithConstants`, a `std::vector<std::pair<DataType, std::optional<RuntimeValue>>>`.
Each entry keeps the declared type beside its optional compile-time value: `std::nullopt` means a nonconstant argument,
whereas an engaged optional containing `RuntimeValue::null(type)` is a typed null constant.

For example, a stateless identity operator:

```cpp
class PreparedIdentity final : public PreparedAsyncCall {
public:
    explicit PreparedIdentity(RuntimeValue value) : value(std::move(value)) {}

    boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override {
        co_return SharedRuntimeValues{value};
    }

private:
    RuntimeValue value;
};

struct MyAsyncOperator {
    DataType valueType;

    static AsyncOperator create(AsyncOperatorRegistryArguments args) {
        if (args.argumentTypes.size() != 1) {
            throw InvalidQuerySyntax("MY_FUNCTION takes exactly one argument");
        }
        return MyAsyncOperator{args.argumentTypes.front()};
    }

    std::string_view getType() const { return "MY_FUNCTION"; }

    Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const {
        return {{Identifier::parse("value"), valueType}};
    }

    MyAsyncOperator withInferredArguments(const AsyncArgumentsWithConstants& binding) const {
        if (binding.size() != 1) {
            throw InvalidQuerySyntax("MY_FUNCTION takes exactly one argument");
        }
        auto copy = *this;
        copy.valueType = binding.front().first;
        return copy;
    }

    AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(
        AsyncExecutionContext&, SharedRuntimeValues arguments) const {
        return std::make_unique<PreparedIdentity>(arguments[0].toOwned());
    }
};

static_assert(AsyncOperatorConcept<MyAsyncOperator>);
```

```cmake
add_registry_entry(AsyncOperator My KEY MY_FUNCTION)
add_unreflection_entry(AsyncOperator My KEY MY_FUNCTION)
```

The factory registry uses `&MyAsyncOperator::create` from `MyAsyncOperator.hpp`. The unreflection key matches `getType()`;
the SQL factory registry may additionally provide aliases. There is no second runtime factory or registry. Concrete implementations
validate argument counts before indexing arguments or deriving outputs, and validate types and constant options during inference.
Output names are available before inference; the scalar shorthand requires a single stable name.

Both `AsyncLogicalOperator::create(child, name, arguments, ...)` and
`AsyncLogicalOperator::create(child, implementation, arguments, ...)` are supported. The latter preserves custom configuration
state. Scalar syntax and direct operator construction use the same native inference and preparation methods.

Stateful plugins can define private state types in their implementation file and select the required lifetime in `prepare()`:

```cpp
auto& counter = context.getOperatorState<SharedCounter>();  // all slots of this operator instance
auto& connection = context.getSlotState<Connection>();     // only this slot
```

Each scope holds one concrete state type; combine related fields in that struct. State is created lazily, may be move-only,
and is not copied or reflected. Constructor arguments may be supplied, such as `context.getSlotState<Connection>(clientConfig)`.
They are used only on first access and must be stable configuration for that scope, not values from the current row.

Operator-state initialization is synchronized, but access to the returned state is the plugin's responsibility. A mutex or
atomic may be appropriate; do not hold a blocking mutex across `co_await`. The current source runs its coroutines on a strand,
so synchronous updates are serialized, but work spanning suspension can still interleave. Slot state remains exclusive to one
invocation at a time.

Sharing is local to one graph call in one deployed source instance. Separate call occurrences, workers, and query executions
have independent state, even if they reuse identical configuration. This is in-memory state, not checkpointed or transactional
state. `prepare()` runs once per invocation, whereas updates in `executeAttempt()` may repeat on an explicitly enabled retry.

A default `AsyncExecutionContext` starts a fresh operator instance. For direct C++ invocation, `context.createSlotContext()`
creates another slot sharing that operator state but owning fresh slot state. Contexts must not be reused for different operator
configurations. `PreparedAsyncCall` may borrow either scope while its context remains alive; the source retains contexts until
all invocations have drained. Shared state is destroyed after the last slot releases it, with slot state destroyed first.

An optional `maxConcurrency()` method on the configuration can lower the stage limit; implementations without it use the stage default.

Constant arguments remain present when `prepare()` receives a row. Returned values must match the declared output count,
types, and nullability exactly; the executor does not convert them.

`SharedRuntimeValues` is an immutable, cheap-to-copy owning list; default construction is a valid empty list. Its views borrow
storage. Use exact scalar extraction and explicit option validation, such as `checkedUnsignedIntegerOption`, before suspension.
A prepared invocation must retain or copy any bytes it needs. Do not retain borrowed views without retaining their owner.

## Tests

Counter-based HTTP tests distinguish separate written calls from repeated references to one bound result. Coverage includes
duplicate SELECT calls, repeated nested calls, WHERE/SELECT separation, discarded results, pure-filter barriers, and alias copies.
Operator tests cover invocation identities, schema append behavior, multi-output programmatic construction, scalar rejection,
normalization ordering, and reflection. EXPLAIN tests describe plan shape but do not substitute for call-count tests.

Runtime tests additionally cover buffer reuse, typed values, shared producer results, cancellation, retries, channel endpoint
ownership, pointer reconstruction, and late DNS completion. Preserve those contracts when changing logical representation.
