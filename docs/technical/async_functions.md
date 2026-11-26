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

`OperatorFunctionLiftingRule` normalizes registered operator-function syntax after type inference and sink target-order calculation, and before the
semantic barrier and static pushdowns. It first runs `DecideFieldOrder` and consumes those ordinary order traits rather than
maintaining a private ordering algorithm. Newly created projections use the same ordering implementation, and the normal
late `DecideFieldOrder` run replaces previous ordering traits after structural rewrites. Async decomposition remains a separate
step and only processes operators carrying the async marker.

The rule walks expression occurrences in order:

1. Lower nested calls first.
2. Materialize nontrivial synchronous arguments in ordinary projections, forwarding the existing input fields.
3. Insert the registered ordinary operator for the occurrence, appending its declared result field unchanged.
4. Where needed, use an ordinary projection to rename the scalar result to its SQL alias or an internal expression temporary,
   then replace the expression with a field access. No rename is necessary when the declared result name is already suitable.
5. Retain the original result projection or selection over those fields.

If an input already has the native result's name, the scalar adapter first projects that input to a temporary name. Its result
projection restores the original input name and gives the new result its SQL alias or temporary expression name. No input fields are lost,
and all renaming remains in ordinary projections. Direct programmatic operators require an explicit projection when names collide.

The original visible schema is captured before introducing helpers. `SELECT *` expands against that schema, and helper
fields introduced for a predicate are removed before its result is exposed to parent operators. Generated names avoid
input fields, user aliases, and other generated names.

Within one expression region, pointer identity resolves shared function-DAG references to an already produced field.
`TypeInferenceContext` preserves that sharing while rebuilding functions, including across all expression roots of a
projection. It never merges separately allocated calls by comparing arguments or structure. WHERE and SELECT are independent
regions; no cross-clause reuse lookup exists. Logical-function reflection also preserves DAG aliases through explicitly
injected shared-pointer state; no async-specific occurrence UUID is needed.
Async calls connected through pure field-forwarding or alias projections may be collected into one graph when they have the same worker placement. Each written invocation remains a distinct graph node; calls are never merged by comparing their configuration or arguments. Computed synchronous projections and other operators remain fusion boundaries. There is no EXPLAIN-derived expression key.

Async operators are effect barriers. Predicate and watermark pushdown cannot cross them, and projection pruning must retain
the invocation even when none of its results is selected. Initially, conservative full-schema retention is preferred to a
more aggressive pruning optimization. Ordinary projections above the operator may discard fields normally.

## Physical handoff and row layout

The optimizer runs [AsyncOperatorDecompositionRule](../../nes-query-optimizer/include/Rules/Static/AsyncOperatorDecompositionRule.hpp#L34)
after field/layout optimization and worker placement, and before distributed network decomposition. The rule uses
`PlanVisitor` to rebuild operators bottom-up, captures handoff sinks as additional roots, and replaces each async operator
with an `Async` source on the same worker. Network decomposition then partitions all resulting roots without performing
any async lowering. The original placed global plan remains available for EXPLAIN and query metadata.

A same-worker async region creates one runtime graph containing distinct producers for its logical invocations. Only live results are exported, while explicit required-call roots retain every invocation's effects even when none of its results is used.

The logical operator appends columns, but the current buffer-reuse runtime requires identical ordered input/output schemas.
Decomposition therefore inserts a **physical padding projection** before the handoff. It forwards the input and reserves
correctly typed slots for results that leave the region. Internal producer results stay in runtime values rather than tuple slots. Nullable, variable-sized, boolean, and character placeholders retain their
actual datatypes. Field order and physical field mappings are derived for the padded layout rather than copied unchanged
from the narrower input. The current async runtime requires row layout. The planner gives both handoff endpoints that same ordered schema directly.
At source construction, `AsyncHandoffLayout` verifies identical ordered schemas (including field names, types, and nullability),
checks graph output mappings, and builds the runtime copy plan. There is one schema, one row size, and one row-layout view.
Exclusively owned input buffers are updated in place; shared buffers copy unchanged fields to the same offsets before results
are written. Reordered handoff schemas are rejected rather than adapted.

Padding is an implementation detail: it does not turn the semantic operator back into an async-tagged projection. The
source exposes the padded physical schema, and an ordinary projection restores the region's visible output schema. The runtime
fills the reserved slots, and one admitted input buffer still corresponds to one output buffer.

The decomposition pass fuses compatible same-worker async calls across pure field/alias projections. Computed synchronous projections, filters, joins, unions, placement changes, and other evaluation boundaries stop fusion. Calls remain distinct DAG nodes, including invocations with equal configuration. Multi-parent logical subplans containing a shared async occurrence remain unsupported and are rejected rather than executed twice.

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
only restores identity; it does not discover or invoke arbitrary pointee `validate()` methods. Generic pointers and polymorphic
logical-function pointers use the same identity/cache/rollback protocol, with type-specific construction supplied separately. Deployment validation belongs
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
`InputFieldReference`s, or `AsyncResultReference`s owning their producer, held by named accessors. Result indices are internal
graph references, not SQL extraction syntax. Builders reject foreign producer dependencies and invalid selectors. Accessor reads
reject foreign row scopes; graph serialization checks field scopes and dependency membership. Wire reconstruction rejects invalid
fields, dangling references, duplicate definitions, and cycles, including malformed unreachable definitions.

Runtime memoization uses explicit producer-node identity. Multiple references to one producer share its result; different
producer nodes remain different even if their function names and argument values match. This is not structural call deduplication.

Before preparing a consumer, graph execution awaits its explicit producer dependencies. The concrete operator owns explicitly named `AsyncInputAccessor` values (and optional accessors where appropriate). An accessor represents a constant `RuntimeValue`, an input field reference, or a particular producer result; it reads through an opaque `AsyncRowHandle` and exposes no positional input index. A row provider retains the input buffer and its layout view; applying an accessor to a foreign input context is a precondition violation. Logical lowering collects producer dependencies from the accessors, and graph execution waits for those dependencies before creating a row handle with ready results. Native reflection serializes each named accessor descriptor, never a live row callback or positional signature.

Accessor reflection uses the existing generic variant codec for constants, input fields, and producer results. Only the field
and result reference alternatives need injected graph context; constants also round-trip outside a graph. The graph remains a
flat list of definitions with checked references, rather than recursively duplicating producer definitions through every edge.

`RuntimeValue` reflection stores its datatype and a tagged payload variant. Small floating-point bit records and byte arrays
preserve signed zero, NaN payloads, and arbitrary binary values. Decoding checks the selected native alternative against the SQL
datatype and nullability before constructing the value, including when preconditions are disabled.

Graph execution exposes `run()` and owner cancellation. Producer lookup is private. Internally, `SharedAsyncResult` provides
lazy single execution, cached results/errors, independent waiter cancellation/deadlines, and joining. Cancelling a subscriber's
wait does not cancel the producer. On row failure or cancellation, the owner cancels and joins all producer work before releasing
resources, preserving the initiating failure across cancellation-shielded cleanup.

## Preparation, retries, and deadlines

Plugins expose one invocation protocol:

```cpp
struct MyAsyncOperator {
    // Type and written-field methods belong to this concrete struct.
    AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(
        AsyncExecutionContext& context, const AsyncRowHandle& row) const;
};
```

`prepare()` reads its input functions through the current row handle, then validates and owns the per-row arguments once,
before the retry loop. On success it returns a `PreparedAsyncCall` whose
`executeAttempt()` returns `boost::asio::awaitable<AsyncResult<SharedRuntimeValues>>`. `AsyncResult<T>` is
`std::expected<T, AsyncError>`; operational failures are returned values, not thrown exceptions. The prepared call's constructor
fixes its private per-row retry count, exposed read-only through `getMaxRetries()` (defaulting to zero, which disables retries).
Attempts are sequential. Prepared calls can retain actual state ownership through
`getSharedOperatorState<T>()` and `getSharedSlotState<T>()`; the latter also keeps its operator-state dependency alive.
These handles share the context's state storage, not a separate lifetime-tracking token. Shared ownership does not permit
concurrent use of a slot. Borrowed state access remains available when its caller guarantees the lifetime.
`invokeOnce(implementation, context, row)` uses the same result type; the convenience overload without a context owns a fresh context until the attempt completes. Neither is another plugin extension point.

Only execution errors with `AsyncError::Type::Retryable` are eligible for retry. Preparation errors, permanent failures,
shape/type invariant failures, and cancellation are not retried. Cancellation and waiter timeout have distinct error kinds;
a waiter's failure does not overwrite the producer's cached outcome. Backoff starts at 100 ms and doubles up to 102.4 seconds.

Checked literal, integer-option, and deadline helpers also return `std::expected`. Logical inference may turn their errors into
one query diagnostic, matching existing throwing planner interfaces. Reflection and runtime construction likewise retain their
throwing interfaces and let errors reach submission/request handlers. Async execution does not catch these exceptions to make
retry decisions; terminal execution errors are reported at the established source-task boundary.

Omitting the fourth argument to `HTTP_CALL` means zero retries.
An explicit retry count allows additional attempts after transient transport failures or statuses 408, 429, 500, 502, 503, and
504. Other non-2xx responses fail immediately. A retryable classification is not an idempotency guarantee: callers opting into
POST retries must tolerate duplicate server-side effects.

Each HTTP attempt has one deadline covering hostname resolution, connect, write, and read. A retry receives a fresh budget;
backoff is outside that budget. A hostname lookup may complete after timeout/cancellation, so its callback retains shared
resolver state and ignores late results without retaining row/function resources.

## Configuration

| Option | Default | Meaning |
|---|---:|---|
| `optimizer.async.max_concurrency` | 16 | Rows in flight per async stage. |
| `optimizer.async.channel_capacity` | 64 | Queued buffers before backpressure. |

`HTTP_CALL(body, url[, timeout_ms[, retries]])` accepts plain HTTP only. Timeout defaults to 30 seconds.
`ASYNC_DELAY(value, millis)` is a test/benchmark operator; its only output is the delayed value.

## Adding an async operator

Implement a concrete logical operator with named logical inputs and local schema/constant validation, a native async operator
receiving named input accessors at construction, and its prepared invocation. Convert the generic SQL argument list to named
inputs at the syntax factory boundary; do not store that positional list in the concrete operator. The runtime operator has no
planner-inference interface and is constructed only by the logical operator's lowering step. `prepare()` validates dynamic
per-row values and captures them once before execution attempts.

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
    explicit MyAsyncOperator(AsyncInputAccessor value) : valueType(value.getDataType()), valueInput(std::move(value)) {}
    DataType valueType;
    AsyncInputAccessor valueInput;

    std::string_view getType() const { return "MY_FUNCTION"; }

    Schema<UnqualifiedUnboundField, Ordered> getWrittenFields() const {
        return {{Identifier::parse("value"), valueType}};
    }

    AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(
        AsyncExecutionContext&, const AsyncRowHandle& row) const {
        return std::unique_ptr<PreparedAsyncCall>{std::make_unique<PreparedIdentity>(valueInput(row))};
    }
};

static_assert(AsyncOperatorConcept<MyAsyncOperator>);
```

```cmake
add_unreflection_entry(AsyncOperator My KEY MY_FUNCTION)
# In the logical plugin, register MyLogicalOperator::lowerToAsync:
add_registry_entry(AsyncOperatorLowering My KEY My)
```

Native unreflection restores the concrete operator and its named accessors directly; no positional runtime factory replay is used. The unreflection key matches `getType()`, while SQL function aliases are registered separately. Concrete implementations receive their named accessors at construction; there is no unconfigured input bundle to replace later.
Logical operators validate argument types and constant options directly;
runtime preparation checks dynamic values. Declared result names are available without a runtime instance, and scalar syntax
requires one stable result field.

`AsyncOperator::getWrittenFields()` declares only the ordered fields written by one invocation, not the complete outgoing row.
Logical operators retain `getOutputSchema()` for that complete schema. Decomposition currently enforces append-only writes:
every written name must be absent from the input, and the logical output must contain the input plus those fields. Overwriting
existing fields is not supported yet.

The built-in operations have normal concrete logical nodes, `HttpCallLogicalOperator` and `AsyncDelayLogicalOperator`.
HTTP directly owns `body`, `url`, optional `timeoutMillis`, and optional `retries` logical functions. Delay directly owns
`value` and `millis`. There is no separate input-wrapper or per-operator logical-function
class. The `functionSyntax()` callbacks unpack the generic argument list into named fields and reuse the operator's local
validation. Argument counts and result types are checked during logical inference, not by constructing a runtime object. They forward all child fields and append their declared result fields without renaming them. Use an ordinary `ProjectionLogicalOperator` to rename a result before a
second operator appends a field with the same name; async operators have no output-name override API and do not store a
runtime `AsyncOperator` in their logical representation.
Both are ordinary IR operators with their own child, schema, trait, rewrite, and reflection methods. The empty
`AsynchronousOperator` marker lets optimizer rules recognize async effects, just like other IR marker classes. There is no
shared async logical base, CRTP adapter, or virtual argument-rewrite interface. Concrete lowering functions are registered in
`AsyncOperatorLoweringRegistry`; this is where runtime implementations are first constructed from inferred logical inputs and
accessor functions.

`OperatorCallLogicalFunction` is the generic shorthand expression. Its dedicated `OperatorFunctionRegistry` is independent
of both `LogicalOperatorRegistry` and the async runtime registries. The expression stores only the function name, argument
expressions, and inferred result type. An entry declares the appended result field, provides `inferResultType` over inferred
arguments, and provides `createOperator` when the child is available. Optional `getArgumentsFromOperator` supports the same
normalization for directly constructed operators; it returns only a temporary traversal list.

Register scalar syntax with `add_registry_entry(OperatorFunction My KEY MY_FUNCTION)` and implement
`MyLogicalOperator::functionSyntax()`. The contract is unary and row-preserving: forward every input field and append exactly
one declared result field. Registry lookup never constructs a logical operator. Result inference uses the operator's logical
validation functions, and shared call-node identity is preserved independently of argument structure. Row preservation is a
provider contract; expansion additionally checks the supplied child and the complete input/result schema.

Parsing, logical inference, and logical reflection do not construct or serialize runtime operators for HTTP and delay. Their
registry callbacks reuse ordinary local validation rather than creating configuration-function objects. The generic expression
and lifting rule have no dependency on async execution. Synchronous operators use the same registry; the generic tests
demonstrate a test-only `APPEND_SUM` entry implemented entirely with normal addition and projection. Logical-function reflection remains independent of plan-only reconstruction state.

Custom logical operators remain ordinary independent operator fixtures/classes: a registry-context lowering callback lowers
named logical inputs to `AsyncInputAccessor`s, constructs a fully configured native operator, and returns it. No generic async logical
base, invocation wrapper, or positional runtime-factory API is involved. Multi-output native calls remain supported. The
optimizer fuses same-worker async calls across pure field/alias projections into one call DAG; each logical invocation remains
a distinct graph node, even when configurations match. Independent calls may execute concurrently, including calls represented
by successive logical operators; only computational dependencies impose ordering inside a region. Computed synchronous projections
and other synchronous operators are boundaries. Explicit required-call roots preserve effects whose results are unused.

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

Constant arguments are represented by `AsyncInputAccessor`s just like field and producer-result inputs, and remain available when `prepare()` receives a row handle. Returned values must match the declared output count, types, and nullability exactly; the executor does not convert them.

`SharedRuntimeValues` is an immutable, cheap-to-copy owning list; default construction is a valid empty list. Its views borrow
storage. Use exact scalar extraction and explicit option validation, such as `checkedUnsignedIntegerOption`, before suspension.
A prepared invocation must retain or copy any bytes it needs. Do not retain borrowed views without retaining their owner.

## Tests

Counter-based HTTP tests distinguish separate written calls from repeated references to one named result. Coverage includes
duplicate SELECT calls, repeated nested calls, WHERE/SELECT separation, discarded results, pure-filter barriers, and alias copies.
Operator tests cover invocation identities, schema append behavior, multi-output native graph construction, scalar rejection, normalization ordering, and reflection. EXPLAIN tests describe plan shape but do not substitute for call-count tests.

Runtime tests additionally cover buffer reuse, typed values, shared producer results, cancellation, retries, channel endpoint
ownership, pointer reconstruction, and late DNS completion. Preserve those contracts when changing logical representation.
