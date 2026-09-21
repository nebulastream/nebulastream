Logical operator# SEM_MAP as a native operator in NebulaStream

## Context

A Python prototype of semantic LLM operators (`sem_map`, `sem_filter`, `sem_join`, `sem_agg`, `sem_groupby`, `sem_redact`) exists as part of a VLDB 2027 submission — *Semantic Stream Processing: Enabling LLM-Based Operators in Real-Time Data Pipelines* (TU Berlin). Today it is bolted onto NebulaStream **externally**: NES ships rows over HTTP (NDJSON) to `POST /operator/process` on port 3000, and results come back as headerless CSV over a TCP socket on port 5000. The measured baseline of that path is **0.0218 rows/s**, so the external coupling is not just architecturally unsatisfying — it is the actual bottleneck.

The paper lists native integration explicitly as future work:

> "NebulaStream native integration: extend the NebulaStream query planner to recognize semantic operators at the logical plan level, enabling cost-based placement and fission across distributed nodes."

That is the goal here: **`SEM_MAP` as the first semantic operator, natively and end to end**, from the SQL grammar down to the runtime. Filter, join, agg and groupby are out of scope for this plan; the closing section explains how much of this carries over to them and how much does not.

### Agreed decisions

| Question | Decision |
|---|---|
| Scope | `SEM_MAP` only, but complete |
| Execution model | Stage 1 synchronous, stage 2 asynchronous with batching — interfaces cut from the start so stage 2 is not a rewrite |
| Location in the tree | In core, mirroring `InferModel` |
| LLM transport | Direct HTTP from C++ |
| SQL syntax | Catalog statement + TVF, exactly following `MODEL_INFERENCE` |
| Prompt payload | Both serializations configurable, default faithful to the Python reference |
| Output columns | The answer only — no confidence column |

### Why core and not a plugin

`nes-plugins/` today holds sources, sinks, formatters, one plan rule and one function — **not a single operator**. Two things block an out-of-tree operator: the `LoweringRule` entry template hardcodes `LowerToPhysical${PLUGIN_NAME}` and resolves its header under `nes-query-compiler/private`, and the pushdown rules (`ProjectionPushdownRule`, `PredicatePushdownRule`) are closed type switches, so an unknown operator silently lands in the pessimistic default branch. Core is the right place.

---

## The template to follow

With `MODEL_INFERENCE`, NebulaStream already has a complete model axis running from SQL to runtime, documented in [docs/technical/model_inference.md](docs/technical/model_inference.md). `SEM_MAP` mirrors it stop by stop:

| Stage | Template |
|---|---|
| Grammar | [AntlrSQL.g4:81-85](nes-sql-parser/AntlrSQL.g4#L81-L85) (DDL), [:173-180](nes-sql-parser/AntlrSQL.g4#L173-L180) (TVF) |
| Catalog | [ModelCatalog.hpp](nes-inference/include/ModelCatalog.hpp) |
| Statement handler | [StatementHandler.hpp:256](nes-frontend/include/Statements/StatementHandler.hpp#L256) |
| Logical (unresolved → resolved) | `InferModelNameLogicalOperator` → [InferModelLogicalOperator.hpp](nes-logical-operators/include/Operators/InferModelLogicalOperator.hpp) |
| Optimizer rule | [InferModelResolutionRule.cpp](nes-query-optimizer/src/Rules/Semantic/InferModelResolutionRule.cpp) |
| Lowering | [LowerToPhysicalInferModel.cpp](nes-query-compiler/src/LoweringRules/LowerToPhysical/LowerToPhysicalInferModel.cpp) |
| Physical | [InferModelPhysicalOperator.cpp](nes-physical-operators/src/Inference/InferModelPhysicalOperator.cpp) |
| System tests | [nes-systests/inference/](nes-systests/inference/) |

For the **stateful** part (stage 2) the template is the windowed aggregation instead: [LowerToPhysicalWindowedAggregation.cpp](nes-query-compiler/src/LoweringRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.cpp) shows how an `OperatorHandler` is created during lowering and attached to the `PhysicalOperatorWrapper`.

---

## Target syntax

```sql
CREATE SEMANTIC MODEL sentiment_clf
  INPUT  (reviewText VARSIZED)
  OUTPUT (sentiment VARSIZED)
  SET ('Determine if the review is positive or negative' AS LLM.PROMPT,
       'http://localhost:8000/v1'                        AS LLM.ENDPOINT,
       'meta-llama/Llama-3.3-70B-Instruct'               AS LLM.MODEL_NAME,
       'POSITIVE,NEGATIVE,NEUTRAL'                       AS LLM.OUTPUT_VALUES,
       10                                                AS LLM.BATCH_SIZE);

SELECT * FROM SEM_MAP(sentiment_clf, reviews) INTO result;
```

Two constraints on the option names, both verified against the grammar:

- **Every key must be qualified exactly once.** `bindConfigOptions` rejects anything that is
  not `PREFIX.NAME`, which is why all options sit under an `LLM.` namespace, the same way
  sources and sinks use `SOURCE.`, `SINK.` and `INPUT_FORMATTER.`.
- **The option is `MODEL_NAME`, not `MODEL`.** `MODEL` is an existing reserved keyword, so
  `AS LLM.MODEL` fails to parse. Longer names such as `OUTPUT_VALUES` are fine: the lexer's
  maximal munch prefers the longer `IDENTIFIER` over the `OUTPUT` keyword.

Output schema: every field of `reviews`, plus `sentiment VARSIZED`.

**Why the configuration belongs in `SET (...)`.** The grammar has **no `nonReserved` rule** — `identifier: strictIdentifier` directly. Every new lexer keyword therefore becomes globally reserved and can no longer be used as a column or source name. A `PROMPT` keyword would forbid any column called `prompt`. The existing `optionsClause` ([:75](nes-sql-parser/AntlrSQL.g4#L75), of the form `value AS name`) avoids that entirely, because option names are ordinary identifiers.

That leaves exactly **two new tokens**: `SEMANTIC` and `SEM_MAP`. `MODEL`, `INPUT`, `OUTPUT` and `SET` already exist.

---

## Functional specification of SEM_MAP

The Python reference (`src/operators/sem_map.py`, `llm_operator.py`) is the authority on behaviour.

### Semantics

```
SEM_MAP(depends_on, prompt) → output_column
```

Each record is put to an LLM, the answer is appended as a new column, and every other field passes through unchanged. **Records are never dropped** — every failure path writes `default_value`. The reference tests pin this down explicitly: `assert len(sink.rows) == 2  # SEM_MAP never drops rows`.

### Prompt construction

No template language, no `{column}` placeholders, no system role. A single user message assembled from four blocks:

```
<sysprompt>            # fixed; describes the expected JSON response format
<operator prompt>      # "Apply the following 1 operation to each row:\n  1. MAP: <prompt> → output field: "<col>""
[Dataset context: …]   # optional
Data: {"<row_id>": "<text>", …}
```

The per-record payload: the values of all `depends_on` columns, `str()`-converted and **space-joined**, with no column names:

```python
payload = {row[self.row_id]: " ".join(str(row.get(c, "")) for c in self.depends_on) for row in records}
```

This is lossy. `PayloadFormat::JSON_OBJECT` (a real field-name object per row) is likely better for quality, but it invalidates the baseline measured in the paper — hence both modes, with `SPACE_JOINED` as the default.

### Response and output

```json
{"<row_id>": {"<output_column>": {"answer": "...", "confidence": 0.9}}}
```

Parsed by four cascading attempts: raw → strip markdown fence → grab the outermost `{...}` by regex → repair malformed key/value pairs. If all four fail, every row gets `default_value` — **no exception, no retry**.

The only appended column is `<output_column>` (VARSIZED).

**On confidence.** The Python reference also writes `<output_column>_confidence`, but SEM_MAP
does not project it. Note the deliberate asymmetry: the prompt still *asks* for a confidence
value and the parser still reads it, because the response envelope is part of the sysprompt
format and changing it would invalidate the baseline measured in the paper. The value is simply
dropped instead of becoming a column. Nothing in the reference acts on it either — there is no
thresholding, no cascade, no abstention — so the column would carry a number nobody consumes.

If a later operator does want it (proxy cascades and abstention are the obvious candidates), it
becomes a declared output field in the catalog rather than an implicit second column on every
semantic operator.

### Deliberately not ported

1. **The hidden output-type inference.** On construction, the Python version fires an extra, unlogged LLM call (`_INFER_PROMPT`) to decide whether the output is restricted to a closed set of values, and afterwards normalizes answers fuzzily (case folding, stripping parenthetical suffixes, substring containment, `difflib.get_close_matches(cutoff=0.6)`). Replaced by the **declared** `output_values` option plus the same deterministic matching cascade minus the fuzzy step. Same effect, reproducible, one LLM call fewer.
2. **Operator fusion** (`_fused_steps`) — needs at least two operators, arrives with `SEM_FILTER`.
3. **Proxy models** — attached to `SEM_FILTER` and `SEM_GROUPBY`, not to map.
4. **The `<output_column>_confidence` column** — see above. Requested in the prompt and parsed, but not projected.
5. `llm_ts` (a debug artifact) and the dead `Llm_operator._process_rows`.

### Defaults

`batch_size=1`, `max_wait_time=1.0s`, `default_value=""`, `max_concurrency=10` per model, 600 s timeout, 2 retries, and temperature/top_p/max_tokens never set.

Note that in Python the timeout and retries are only silently inherited OpenAI SDK defaults. libcurl's defaults are entirely different, so in C++ both must be **set explicitly**.

---

## Implementation

### Phase 0 — HTTP client as a dependency

There is **no HTTP client** today. [vcpkg/vcpkg.json](vcpkg/vcpkg.json) carries `boost-asio`, `boost-url`, `boost-process` and `grpc`, but nothing for HTTP requests; `boost/asio` appears only in the test helper [TCPDataServer.cpp](nes-plugins/Sources/TCPSource/TCPDataServer.cpp), and [TCPSource.cpp](nes-plugins/Sources/TCPSource/TCPSource.cpp) uses raw POSIX sockets.

JSON, by contrast, is already available: **simdjson** for parsing and **nlohmann-json** for building requests.

→ Add `cpr` to [vcpkg/vcpkg.json](vcpkg/vcpkg.json). Rationale: libcurl gives HTTPS, proxy and timeout handling for free, and is present in the runtime image anyway. The alternative, `boost-beast`, would require wiring up TLS by hand.

Note that the manifest lives under `vcpkg/`, not at the repository root, and that [vcpkg/vcpkg-registry/ports/](vcpkg/vcpkg-registry/ports/) is an overlay registry for ports NebulaStream patches itself (openvino, folly, nautilus and others). A stock `cpr` should come straight from the baseline and need no overlay entry.

### Phase 1 — Catalog (new module `nes-semantic/`)

A separate module rather than an addition to `ModelCatalog`: `RegisteredModel` is hardwired to local ONNX files through `path` + `ImportedModel`, and the validation in `ModelCatalog::registerModel` (f32 tensors, shape matching) is meaningless for LLMs.

```cpp
/// One semantic step. SEM_MAP always carries exactly one.
struct SemanticStep {
    enum class Kind : uint8_t { MAP, FILTER };
    Kind kind = Kind::MAP;
    std::string prompt;
    std::string outputColumn;
    std::vector<std::string> outputValues;   /// empty = free text
    std::string defaultValue;                /// default ""
};

struct SemanticModelConfig {
    std::string endpoint, modelName, datasetPrompt;
    std::vector<SemanticStep> steps;         /// exactly one entry for SEM_MAP
    PayloadFormat payloadFormat = PayloadFormat::SPACE_JOINED;
    size_t batchSize = 1, maxConcurrency = 10, maxRetries = 2;
    std::chrono::milliseconds maxWaitTime{1000};
    std::chrono::seconds requestTimeout{600};
    std::optional<std::string> apiKeyEnvVar;  /// NEVER the key itself
};
```

**Why a list when SEM_MAP only ever has one step.** Operator fusion is a core contribution of the paper: two adjacent semantic operators are merged into a *single* prompt, measured at 846 → 500 calls and 337K → 221K tokens with an F1 that actually improves. Fusion needs somewhere to put the second step. If the operator carries a single `prompt` + `outputColumn`, then the operator, the wire format, the catalog schema and the prompt builder all have to be torn open later, together.

The Python reference gets this right: `_fused_steps` is a list from the outset, even for a single step, and the generated prompt literally says "Apply the following **1 operation** to each row". Today the list costs almost nothing.

`SemanticModelCatalog` provides `registerModel/hasModel/load/removeModel/getRegisteredModels` plus `Reflector`/`Unreflector` for `RegisteredSemanticModel` — the pattern taken verbatim from [ModelCatalog.hpp](nes-inference/include/ModelCatalog.hpp).

**Credentials.** The catalog entry is serialized and shipped from the coordinator to the worker. Only the **name of an environment variable** is stored; it is resolved worker-locally during lowering — the same deferral that makes `compileModel` run at lowering time rather than at registration.

Add error codes to [ExceptionDefinitions.inc](nes-common/include/ExceptionDefinitions.inc) (`UnknownSemanticModelName`, `SemanticModelAlreadyExists`), following the model codes 2040-2042.

### Phase 2 — SQL surface

**Grammar** ([AntlrSQL.g4](nes-sql-parser/AntlrSQL.g4)). ANTLR is regenerated on every build and the generated code is not checked in, so no CMake change is needed:

1. Add the `SEMANTIC` and `SEM_MAP` tokens to the keyword block at [:555](nes-sql-parser/AntlrSQL.g4#L555).
2. Add `createSemanticModelDefinition` as an alternative of `createDefinition` ([:69](nes-sql-parser/AntlrSQL.g4#L69)), structured like `createModelDefinition` but with `optionsClause?` in place of the path literal.
3. Add `semanticMapSource` as a **labeled** alternative of `relationPrimary` ([:164-171](nes-sql-parser/AntlrSQL.g4#L164-L171)) — the label is what generates the `…RelationContext` and the listener hooks. Alongside it, `semanticMapInput` with the same three forms as `modelInferenceInput` (stream name, subquery, nested).
4. Add `dropSemanticModel` and `SHOW SEMANTIC MODELS` the same way.

**Parser** — follow the `ModelInference` pattern:
- `enter/exitSemanticMapRelation` in [AntlrSQLQueryPlanCreator.hpp:69-70](nes-sql-parser/private/AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp#L69-L70)
- an `isSemanticMap` flag in [AntlrSQLHelper.hpp](nes-sql-parser/private/AntlrSQLParser/AntlrSQLHelper.hpp) and the FROM-clause guard at [AntlrSQLQueryPlanCreator.cpp:730](nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp#L730)
- a recursive `buildSemanticMapPlan` modelled on [:1498-1556](nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp#L1498-L1556)
- `LogicalPlanBuilder::addSemanticMap(name, childPlan)` in [LogicalPlanBuilder.hpp/.cpp](nes-logical-operators/include/Plans/LogicalPlanBuilder.hpp)

**Statement binding** — add `CreateSemanticModelStatement` to [StatementBinder.hpp:162](nes-sql-parser/include/SQLQueryParser/StatementBinder.hpp#L162) **and to the `Statement` variant at [:205-224](nes-sql-parser/include/SQLQueryParser/StatementBinder.hpp#L205-L224)**; forgetting the variant fails silently. Then `bindCreateSemanticModelStatement` plus its dispatch in [StatementBinder.cpp](nes-sql-parser/src/StatementBinder.cpp), and the same for drop and show.

**Statement handler** — `SemanticModelStatementHandler` in [StatementHandler.hpp](nes-frontend/include/Statements/StatementHandler.hpp), result structs added to the `StatementResult` variant, and a `StatementOutputAssembler` specialization per result type. Dispatch is compile-time overload resolution (`tryCall`); there is **no central switch**, so the handler must be threaded into the handler packs in **four** places or one of the frontends silently loses the feature:

- [CLIStarter.cpp](nes-frontend/apps/cli/CLIStarter.cpp)
- [ReplStarter.cpp](nes-frontend/apps/repl/ReplStarter.cpp) and [Repl.cpp](nes-frontend/apps/repl/Repl.cpp)
- [SystestBinder.cpp](nes-systests/systest/src/SystestBinder.cpp) — here it is a **manual `holds_alternative` chain** that must be extended explicitly; without it the system tests do not run

### Phase 3 — Logical operator

Two operators, as with InferModel, because the parser has no access to the catalog:
- `SemanticMapNameLogicalOperator` — carries only the name string
- `SemanticMapLogicalOperator` — carries the resolved catalog entry

Mandatory parts (template: [InferModelLogicalOperator.hpp](nes-logical-operators/include/Operators/InferModelLogicalOperator.hpp)):
- Base classes `public Reorderer, public ManagedByOperator`. `ManagedByOperator` requires `WeakLogicalOperator self` as the **first** constructor parameter.
- `static constexpr std::string_view NAME = "SemanticMap";` — this string is simultaneously the serialization type, the unreflection registry key **and** the name that determines the lowering rule (`LowerToPhysicalSemanticMap`). All three must match exactly.
- The full `LogicalOperatorConcept` surface. In particular: `withChildrenUnsafe` sets children **without** re-inference, `withChildren` **with** — confusing the two is a classic source of bugs.
- Override `Reorderer::getOrderedOutputSchema` so the per-step output columns are appended in declared order rather than sorted lexicographically. With a single step this is indistinguishable from the default heuristic, but it starts to matter as soon as fusion produces several.
- `Reflector`/`Unreflector` specializations plus a `detail::ReflectedSemanticMapLogicalOperator` aggregate as the wire format. There is **no protobuf change** — plans are serialized as `rfl::json` blobs keyed on `getName()`.
- A `std::hash` specialization is **mandatory**; `detail::OperatorModel::hash()` calls it unconditionally.
- `static_assert(LogicalOperatorConcept<SemanticMapLogicalOperator>);`

The operator carries `std::vector<SemanticStep>`, not a single prompt (rationale in phase 1).

Schema inference in `inferLocalSchema`: verify that every `INPUT` field exists in the child schema, then append `<outputColumn>` (VARSIZED) **per step**, passing every other field through. For SEM_MAP the loop runs exactly once, so exactly one column is added.

Do **not** inherit from `OriginIdAssigner` — SEM_MAP is a 1:1 mapping and starts no new origin stream. That holds in stage 2 as well: the defer-and-poll scheme defers the **entire input buffer** and reprocesses it unchanged later, so order and sequence numbers are preserved.

Registration is **one CMake line** in [nes-logical-operators/src/Operators/CMakeLists.txt](nes-logical-operators/src/Operators/CMakeLists.txt):
```cmake
add_unreflection_entry(LogicalOperator SemanticMap)
add_unreflection_entry(LogicalOperator SemanticMapName)
```

### Phase 4 — Optimizer rule

`SemanticMapResolutionRule` under [nes-query-optimizer/src/Rules/Semantic/](nes-query-optimizer/src/Rules/Semantic/), following [InferModelResolutionRule.cpp](nes-query-optimizer/src/Rules/Semantic/InferModelResolutionRule.cpp) one to one: a `PlanVisitor` that swaps `SemanticMapName` → `SemanticMap`, with

```cpp
needs()    → {LogicalSourceExpansionRule, SinkBindingRule, AnonymousSinkBindingRule}
neededBy() → {TypeInferenceRule, SemanticAnalysisBarrier}
```

The ordering is mandatory: schema inference needs the output fields, which only exist after the catalog lookup. There is **no hand-maintained rule list** — `RuleBasedOptimizer` enumerates the registry and topologically sorts it.

Registration: `add_registry_entry(PlanRule SemanticMapResolutionRule KEY SemanticMapResolution)`.

**Mind the cascade:** the new catalog has to become a field of `PlanRuleRegistryArguments` ([PlanRuleRegistry.hpp](nes-query-optimizer/registry/include/PlanRuleRegistry.hpp)), which propagates through `RuleBasedOptimizer.hpp/.cpp` and `QueryOptimizer.hpp`.

`TypeInferenceRule`, `DecideMemoryLayoutRule`, `DecideFieldOrder`, `DecideFieldMappings` and `OriginIdInferenceRule` are generic and need **no** change — they dispatch on the marker interfaces.

### Phase 5 — Lowering and physical operator

`LowerToPhysicalSemanticMap` in [nes-query-compiler/](nes-query-compiler/src/LoweringRules/LowerToPhysical/), with its header under `private/`. This is where the API key is resolved from the environment. Registration: `add_registry_entry(LoweringRule SemanticMap)`. The lookup key is the logical operator's `getName()`, which is why the naming is rigid.

Stage 1 uses the six-argument `PhysicalOperatorWrapper` constructor with `PipelineLocation::INTERMEDIATE` — **no OperatorHandler**, exactly like InferModel.

**Physical operator.** Structured like [InferModelPhysicalOperator.cpp](nes-physical-operators/src/Inference/InferModelPhysicalOperator.cpp): a `shared_ptr` to a wrapper holding one HTTP client per worker thread (the `shared_ptr` is required because the type erasure returns the operator by value), reached through `nautilus::invoke`. Physical operators are **not registered anywhere** — just add the source file to the CMakeLists.

Per record: read `depends_on`, build the prompt, POST synchronously, parse, write the output field.

**Writing text fields** — the exact template is [ToBase64PhysicalFunction.cpp:56-71](nes-physical-operators/src/Functions/ToBase64PhysicalFunction.cpp#L56-L71): estimate an upper bound, call `arena.allocateVariableSizedData(maxSize)`, `nautilus::invoke` a free C++ function that writes into it and returns the **actual** length, then return `VariableSizedData(ptr, actualSize)` over the same pointer. Precisely the pattern for a response of unknown length.

Responses larger than a TupleBuffer are unproblematic: they land in their own unpooled child buffer automatically. The only real ceiling is `unpooledMemoryBudgetInBytes`; beyond it `CannotAllocateBuffer` is thrown and the query fails cleanly. Exceptions propagate correctly out of `nautilus::invoke` (non-`noexcept` invokes are traced as `CALL_WITH_EXCEPTION_HANDLING`).

A deliberately accepted limitation: the default is **`number_of_worker_threads: 4`**. A blocking two-second call therefore parks 25 % of the engine's capacity, and four concurrent requests stall it completely. That is fine for functional equivalence and system tests, but not for measurements — and not for a demo with a second query running alongside.

**The cut that makes stage 2 cheap.** The LLM logic is split into two layers, an operator-independent transport and an operator-specific codec:

```cpp
/// Transport. Knows nothing about records, columns or operators: a prompt goes in,
/// a response text comes out. Unchanged for every further semantic operator.
class SemanticBackend {
public:
    virtual ~SemanticBackend() = default;
    virtual std::expected<std::string, BackendError> complete(const Request&) = 0;
};

/// Operator-specific: builds the prompt and decodes the response.
struct SemanticMapCodec {
    [[nodiscard]] std::string buildPrompt(std::span<const RowPayload>, const SemanticModelConfig&) const;
    [[nodiscard]] std::vector<StepResults> parse(std::string_view response, std::span<const RowPayload>) const;
};
```

The split matters: `SEM_AGG` wants plain text rather than JSON, `SEM_REDACT` runs several verification rounds, and `SEM_GROUPBY` sends its group state along as context. If all of that lived in the backend, the backend would have to be widened for every operator. The transport, by contrast, is identical for all of them.

Stage 2 therefore changes the call site, not the logic. A `MockSemanticBackend` (deterministic, no network) falls out for free and solves the testing problem.

### Phase 6 — Tests

- **Parser unit tests** in [StatementBinderTest.cpp](nes-sql-parser/tests/StatementBinderTest.cpp), modelled on the `CreateModelStatement` tests including a negative case.
- **Logical operator test** (schema inference, serialization round-trip) in [nes-logical-operators/tests/](nes-logical-operators/tests/).
- **Physical operator test** against the mock backend, modelled on [InferModelPhysicalOperatorTest.cpp](nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp).
- **System tests** in a new `nes-systests/semantic/` directory. `.test` files are discovered automatically; no CMake change needed.

**The determinism problem.** System tests compare exact output — [InferModel.test](nes-systests/inference/InferModel.test) asserts `0.946717,0.051443,…`. LLM output is not deterministic. The answer is the `MockSemanticBackend`, selected through a catalog option (`'mock' AS backend`). That makes grammar, catalog, schema inference, lowering and record flow fully testable without ever touching a model.

The real endpoint gets a separate `.test_nightly` that only asserts the output column is non-empty — exactly what `test_map_executes_against_real_local_llm` does in the reference, which likewise only checks `sentiment != ""`. If a fuzzy comparison is needed later, [Check.hpp](nes-systests/systest/private/ResultChecker/Check.hpp) is a clean extension point: add a new type to the `AnyCheck` variant; the concept only requires `check() -> Verdict`.

---

## Stage 2: asynchrony (follow-up work)

The Python prototype gets its throughput from batching (61 % fewer tokens) and concurrency (39× speedup at 30 parallel requests). For the native implementation there is a hard constraint that only surfaced during exploration:

> **A background thread cannot feed results into the pipeline itself.** `PipelineExecutionContext::emitBuffer` is a lambda bound to the stack frame of the currently running `WorkTask` ([QueryEngine.cpp:493-519](nes-query-engine/QueryEngine.cpp#L493-L519)). The PEC is a stack object and dies when `execute()` returns. An `&pec` held by your own thread is a dangling reference — and `emitWork` additionally relies on the thread-local `WorkerThread::id`.

So the obvious design ("the I/O thread calls `emitBuffer`") does not work. The viable path is **defer and poll** via `repeatTask`:

1. **`SemanticMapOperatorHandler : OperatorHandler`**, created during lowering (`getNextOperatorHandlerId()` plus the eight-argument `PhysicalOperatorWrapper`), owns the in-flight table keyed on `(OriginId, SequenceNumber, ChunkNumber)` and its own thread pool of [`NES::Thread`](nes-common/include/Thread.hpp). Cache the handler pointer once in `open()` in operator-local state (template: `WindowOperatorBuildLocalState`) — `ExecutionContext::getGlobalOperatorHandler` deep-copies the entire handler map on every call.
2. **`execute()` enqueues the record and returns immediately** — the template is [NetworkSink::execute](nes-sinks/src/NetworkSink.cpp#L153-L166), which has exactly this shape (`SendResult::Full` → stash + retry).
3. **`close()` checks for completion.** Still outstanding → call `pec->repeatTask(inputBuffer, ~10ms)` through a proxy and return **immediately**. Done → write the results into the records and let the pipeline continue normally.
4. **`terminate()` → `handler->stop(...)`** through a proxy (template: [WindowProbePhysicalOperator.cpp:62-69](nes-physical-operators/src/WindowProbePhysicalOperator.cpp#L62-L69)). With requests still outstanding, `pec.repeatTask({}, 10ms)` — the `StopPipelineTask` repeat path calls `stop()` again, the same way `MQTTSink::stop` drains its QoS tokens.

### Three details that are painful to learn the hard way

**Arena memory dies at the end of `execute()`.** Anything parked for a later `repeatTask` must be copied into handler-owned memory — a `TupleBuffer` from `allocateBuffer()`, or simply a `std::string` in the handler. An arena-allocated prompt is invalid the moment the call returns.

**Backpressure comes for free.** `repeatTask` **moves the `TaskCallback` into the repeated task**. The source's in-flight semaphore is only released by that callback's `onComplete`, so it stays held for as long as you keep polling. Once `inflightBufferLimit` is exhausted, the source throttles itself. **Nothing** in the [BackpressureChannel](nes-executable/include/BackpressureChannel.hpp) needs to change, and its single-controller invariant stays intact. This is exactly how `NetworkSink` gets by with two threshold knobs.

**`repeatTask` is strictly once per execution.** Afterwards **no** PEC method may be touched — every one of them asserts on `!wasRepeated` ([QueryEngine.cpp:235-288](nes-query-engine/QueryEngine.cpp#L235-L288)). On top of that, in-flight HTTP requests are **invisible** to the engine's `pendingTasks` counter, so a stop will not wait for them on its own. Draining in `stop()` is mandatory, not optional.

### A note on the Python repository's design document

Under "Alternatives → A2" it records two variants proposed by the NES maintainers. "Put the element back in the queue with a flag saying *waiting for an answer*" is **exactly** what [`repeatTask(buffer, ms)`](nes-executable/include/PipelineExecutionContext.hpp#L52) already offers through the `DelayedTaskSubmitter`, delay included rather than a busy loop. The race condition feared there, between the admission queue and the internal queue, does not exist: repeated tasks always go to the internal queue, and the internal queue is read first. The mechanism is present and used in production by two sinks; the document simply does not know about it.

The one genuinely new thing would be an **operator-owned thread**, which nothing in the codebase does today. Threads exist only for sources, the delayed task submitter and the engine itself. If that meets resistance: the Rust side already has a complete async stack with `tokio` (`rt-multi-thread`) and the cxx bridge in [nes-network/](nes-network/), including `identifyThread` for correct logging from tokio threads. `NetworkSource`/`NetworkSink` prove that a blocking C++ interface can sit cleanly on top of an async Rust runtime.

---

## Extending to the remaining operators

This plan is deliberately cut so the expensive infrastructure is built once. It should **not** be read as implying that the remaining six operators are then copy-paste. An honest breakdown:

### Reused unchanged

- **The catalog.** `SemanticModelConfig` (endpoint, model, batch size, concurrency, timeouts, retries, credential resolution) applies unchanged to every operator.
- **The name → resolved operator pair plus the resolution rule.** A pure repetition pattern.
- **`SemanticBackend::complete`** and the JSON parse cascade around the `{row_id: {col: {answer, confidence}}}` envelope. Filter, join, redact and groupby all use the same shell. The envelope keeps its `confidence` field regardless of whether an operator projects it — SEM_MAP parses and discards it.
- **Defer-and-poll via `repeatTask`** (stage 2) is fully operator-agnostic and at the same time the single most expensive component of the whole effort.
- **VARSIZED writing** following the `ToBase64` pattern, and the `MockSemanticBackend` for deterministic system tests.

### What still has to be built per operator

| Operator | Shape | Effort |
|---|---|---|
| `SEM_FILTER` | N:M, same pipeline shape, simply do not call `executeChild` | **Low.** Plus fusion with MAP (what the `steps` list is for) and the proxy path. |
| `SEM_REDACT` | N:N like map, but several verification rounds or a tool-calling loop | **Medium.** Own codec, deterministic masking done locally. |
| `SEM_AGG` | N:1 over a window | **Its own project** (see below). |
| `SEM_JOIN` | Cross product over a window, new output schema with `left_`/`right_` prefixes | **Its own project.** |
| `SEM_GROUPBY` | Stateful across windows, batches strictly serialized | **Its own project.** Plus a multiclass proxy. |

### Why agg, join and groupby are their own projects

This plan targets a **1:1 mapping**: `Reorderer`, no `OriginIdAssigner`, `PipelineLocation::INTERMEDIATE`. That still holds for `SEM_FILTER`. It does not hold for the window-based operators, which need `WindowBasedOperatorHandler`, the slice store, the EMIT/SCAN pipeline break and `OriginIdAssigner` — a different lowering shape altogether. In the codebase exactly three operators implement `OriginIdAssigner`: source, windowed aggregation and join.

The grammar attachment point is different too. Windows in NebulaStream are **not a TVF**: `windowedAggregationClause` hangs off [querySpecification](nes-sql-parser/AntlrSQL.g4#L139), and joins have their own [windowClause](nes-sql-parser/AntlrSQL.g4#L149-L150). These three therefore do *not* follow the `MODEL_INFERENCE` template that runs through this plan.

One useful conclusion falls out of that: **`SEM_AGG` should probably not be an operator at all, but an aggregation function.** [Avg/Count/Max/Median/Min/Sum](nes-logical-operators/include/Operators/Windows/Aggregations/) already exist, along with an `AggregationLogicalFunctionRegistry`. A `SELECT SEM_AGG(notes, '...') FROM s WINDOW TUMBLING(...)` would then inherit the slice store, watermarks and window triggering for free instead of reimplementing them.

### Deliberately left open

- **Proxy models** (attached to filter and groupby): they need a training-data collection path and a local model on the worker. Not addressed in this plan.
- **The `INPUT(...) / OUTPUT(...)` catalog syntax** is map-shaped. `SEM_FILTER` has no output column, `SEM_JOIN` has two inputs. By the time join arrives this has to become operator-dependent validation.

---

## Verification

The build runs through the dev image (no `cmake-build-debug` exists yet):

```shell
docker run --workdir $(pwd) -v $(pwd):$(pwd) nebulastream/nes-development:local \
  cmake -B cmake-build-debug
docker run --workdir $(pwd) -v $(pwd):$(pwd) nebulastream/nes-development:local \
  cmake --build cmake-build-debug -j
```

1. **Unit tests** for the parser, the logical operator and the physical operator (see phase 6).
2. **System test against the mock backend**, covering the whole chain:
   ```shell
   cmake --build cmake-build-debug -j --target systest
   cmake-build-debug/nes-systests/systest/systest -t nes-systests/semantic/SemanticMap.test
   ```
   It asserts: the output schema is correct, pass-through fields are unchanged, **record count in == record count out** (SEM_MAP never drops rows), and a deliberately malformed mock response leaves `default_value` in the column.
3. **Against a real endpoint** — start Ollama or vLLM locally and run the `.test_nightly`, or drive it through [topology.yaml](topology.yaml) with a generator source.
4. **Equivalence with the Python reference** — this is the actual acceptance test. Run the same query through both paths: `d1q1` (Rotten Tomatoes, `prompt='Determine if the review is positive or negative'`, `depends_on=['reviewText']`, `output_column='sentiment'`) once through the Python prototype and once through NES against the same endpoint with `PayloadFormat::SPACE_JOINED`. At `temperature=0` the label distributions should agree.
5. **Full suite**: `ctest --test-dir cmake-build-debug -j`
