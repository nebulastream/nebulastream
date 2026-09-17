# Native SEM_MAP operator in NebulaStream

## Context

Today the semantic operators are a **separate Python process**. NES talks to it over the wire:
`LLMSink` (`nes-plugins/Sinks/LLMSink/LLMSink.cpp`) POSTs NDJSON to the FastAPI service, the
service runs `SEMMapOperator` (`src/operators/sem_map.py`), and `LLMRSource`
(`nes-plugins/Sources/LLMRSource/`) reads results back over TCP on port 5000. The demo queries in
`nes/llm_demo_main.sql` show the cost of this: a `query_id` must be threaded through the schema by
hand, results come back as headerless CSV whose **column order is alphabetical by chance**, the
operator's output schema is re-declared by the user in a second `CREATE LOGICAL SOURCE`, and the
LLM step is invisible to the optimizer — it is literally a sink followed by an unrelated source.

The goal is to make SEM_MAP a **first-class NES operator**: one query, one plan, a real logical
operator that the optimizer sees, schema inference that derives the output columns, and execution
inside the worker's pipeline.

Scope of this plan: **SEM_MAP only**. SEM_FILTER / SEM_JOIN / fusion come later and are noted where
the design has to leave room for them.

---

## Part 1 — Background: how a query becomes running code in NES

This is the machinery you are plugging into. Every stage below is a place where SEM_MAP needs an
artifact, so it is worth reading once end to end.

### Stage 1 — SQL → LogicalPlan (coordinator / client side)

- **Grammar**: `nes-sql-parser/AntlrSQL.g4` — one combined ANTLR4 lexer+parser, 678 lines. Everything
  NES understands is in here.
- **Plan construction**: `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp` — an ANTLR *listener*
  (not visitor). It walks the parse tree and on each `exitX` pushes state into a per-scope
  `AntlrSQLHelper` (`nes-sql-parser/private/AntlrSQLParser/AntlrSQLHelper.hpp`), then assembles
  operators via `LogicalPlanBuilder` (`nes-logical-operators/include/Plans/LogicalPlanBuilder.hpp`:
  `addProjection`, `addSelection`, `addJoin`, `addInferModel`, …).
- **Non-query statements** (`CREATE SOURCE / SINK / MODEL / WORKER`, `SHOW`, `DROP`) go through
  `nes-sql-parser/src/StatementBinder.cpp` and are dispatched by
  `nes-frontend/src/Statements/StatementHandler.cpp` into the respective catalogs.

**Surprise worth knowing:** there is *no* `MapLogicalOperator`. `SELECT id + 1 AS id …` becomes a
single `ProjectionLogicalOperator` (`nes-logical-operators/include/Operators/ProjectionLogicalOperator.hpp`)
that carries `(Identifier, LogicalFunction)` pairs. `MapPhysicalOperator` only appears at lowering
time, one per projected expression. So "Map" is a physical concept here.

### Stage 2 — Logical operators

- Live in `nes-logical-operators/{include,src}/Operators/`.
- **Not** an inheritance hierarchy. A logical operator is any type satisfying the C++20
  `LogicalOperatorConcept`, wrapped by type-erasing `TypedLogicalOperator<T>` / `LogicalOperator`
  (`nes-logical-operators/include/Operators/LogicalOperator.hpp`). You end your header with
  `static_assert(LogicalOperatorConcept<YourOp>);` — that is the contract.
- **Immutable value semantics**: no mutation, only `withChildren()`, `withTraitSet()`,
  `withInferredSchema()` returning copies.
- Constructor takes a `WeakLogicalOperator self` and the class inherits `ManagedByOperator`; schemas
  are stored *unbound* (`Schema<UnqualifiedUnboundField, Unordered>`) and bound on access via
  `bindToOperator(self.lock(), …)`.
- Optional mixins: `Reorderer` (`getOrderedOutputSchema`) for operators that add columns;
  `Reprojecter` (`getAccessedFieldsForOutput`) only if you *overwrite* an existing field name.

### Stage 3 — Optimizer rules

- `nes-query-optimizer/src/Rules/Semantic/` — `LogicalSourceExpansionRule`, `SinkBindingRule`,
  `TypeInferenceRule`, `InferModelResolutionRule`, `CalcTargetOrderRule`.
- Rules are registered by CMake (`add_registry_entry(PlanRule … KEY …)`) and **ordered by
  declaration in code**: each rule returns `needs()` / `neededBy()` sets of `std::type_index`.
  See `InferModelResolutionRule::neededBy() → {TypeInferenceRule, SemanticAnalysisBarrier}`.
- The catalog-resolution pattern: the parser emits a *placeholder* operator holding only a name
  (`InferModelNameLogicalOperator`, whose `withInferredSchema()` deliberately throws), and a rule
  swaps it for the real operator after looking the name up in a catalog. This is the pattern
  SEM_MAP will copy.

### Stage 4 — Serialization to the worker

Not protobuf-per-operator. `grpc/SerializableQueryPlan.proto` is a thin envelope:
```proto
message SerializableQueryPlan {
  repeated string reflectedOperators = 1;   // each is a reflect-cpp JSON blob
  repeated uint64 rootOperatorIds = 2;
}
```
`nes-logical-operators/src/Serialization/QueryPlanSerializationUtil.cpp` writes
`{type: getName(), operatorId, childrenIds, config: reflect(...), traitSet}` per operator;
deserialization dispatches on `type` through `LogicalOperatorUnreflectionRegistry`.
**Consequence for us: the entire operator payload crosses the wire.** For InferModel that is the
converted OpenVINO IR bytes; for SEM_MAP it is the endpoint URL, model name, prompt and output
schema.

### Stage 5 — Lowering (worker side)

- `nes-query-compiler/src/LoweringRules/LowerToPhysical/` — one rule per logical operator,
  implementing `AbstractLoweringRule::apply(LogicalOperator) → {root, leaves}` of
  `PhysicalOperatorWrapper`s.
- Dispatch is by **string**: `LowerToPhysicalOperators.cpp` does
  `LoweringRuleRegistry::instance().find(logicalOperator.getName())`. The registry key in CMake
  **must equal** the operator's `getName()`.
- `PhysicalOperatorWrapper` carries compile-time-only metadata: input/output schema, memory layout,
  an optional `OperatorHandler`, and a `PipelineLocation` (`SCAN | INTERMEDIATE | EMIT`) that
  drives where pipelines break.

### Stage 6 — Pipelining

`nes-query-compiler/src/Phases/PipeliningPhase.cpp` walks the physical DAG, breaks at merge points
and at SCAN/EMIT boundaries, inserts `ScanPhysicalOperator` / `EmitPhysicalOperator`, and produces a
`PipelinedQueryPlan`.

### Stage 7 — Nautilus JIT — **the part that changes how you write the operator**

`nes-query-compiler/src/Phases/LowerToCompiledQueryPlanPhase.cpp` sets `engine.backend = mlir` and
hands the pipeline to `CompiledExecutablePipelineStage`
(`nes-runtime/src/Pipelines/CompiledExecutablePipelineStage.cpp`), which calls
`pipeline->getRootOperator().setup(...)` and then `module.compile()`.

Nautilus is a **tracing JIT with a C++ DSL**. Your
`PhysicalOperator::execute(ExecutionContext&, Record&)` body is *not* the runtime code — it runs
**once, at query start, to trace** operations on `nautilus::val<T>` into IR that MLIR compiles.
Practical consequences:

1. Plain C++ you write in `execute()` runs **once**, at trace time. It does not run per tuple.
2. To run native C++ per tuple you use `nautilus::invoke(&freeFunction, args…)`, which emits a call
   into the generated code. Arguments must be `nautilus::val<...>`.
3. Native state must live at a **stable address**, captured as a `nautilus::val<T*>` constant. This
   is why `InferModelPhysicalOperator` holds a `shared_ptr<ThreadLocalRuntimeWrapper>`.
4. `nautilus::static_val<size_t>` loops are unrolled at trace time (used for fixed field counts);
   `nautilus::val` loops are real runtime loops.

The whole operator chain fuses into one straight-line generated function — `MapPhysicalOperator` is
literally:
```cpp
const auto value = mapFunction.execute(record, ctx.pipelineMemoryProvider.arena);
record.write(fieldToWriteTo, value);
executeChild(ctx, record);
```

### Stage 8 — Execution

`nes-query-engine/` runs a fixed pool of worker threads pulling `Task`s (buffer + pipeline) off a
queue and running the compiled pipeline function **to completion**. Nothing in the tree does network
I/O inside an operator today, and there are no coroutines anywhere in
`nes-physical-operators` / `nes-query-engine` / `nes-nautilus`. A blocking call inside `execute()`
parks a whole worker thread. **This is the central design tension for SEM_MAP** — see Part 4.

The two escape hatches that exist:
- `OpenReturnState::REPEAT` + `PipelineExecutionContext::repeatTask(buffer, delay)`
  (`nes-runtime/include/ExecutionContext.hpp`, `nes-executable/include/PipelineExecutionContext.hpp`)
  — "put this task back on the queue and retry later". Precedent: `ScanPhysicalOperator.cpp:50`.
  Checked only after `open()`, and callable **once per pipeline execution**.
- `OperatorHandler` (`nes-runtime/include/Runtime/Execution/OperatorHandler.hpp`) — long-lived
  per-query native state reachable from generated code via
  `ExecutionContext::getGlobalOperatorHandler(id)`, plus
  `PipelineExecutionContext::emitBuffer(buf, ContinuationPolicy)` to push results downstream from
  another thread. This is how joins and windows split into Build/Probe across a pipeline break.

---

## Part 2 — The reference operator to copy: `InferModel`

Not `Map`. `InferModel` (ONNX inference via OpenVINO) is the exact shape we want — a first-class
logical operator that **appends new output columns computed by an out-of-band engine**, with a
catalog, DDL, a resolution rule, a lowering rule, and a physical operator that calls native C++ from
JIT'd code. There is a narrative walkthrough at `docs/technical/model_inference.md`.

Its full artifact set, which is the template for ours:

| Stage | InferModel file |
|---|---|
| Grammar | `nes-sql-parser/AntlrSQL.g4:81` (`createModelDefinition`), `:174` (`modelInferenceSource`), `:555-557` (tokens) |
| DDL binding | `nes-sql-parser/src/StatementBinder.cpp`, `nes-frontend/src/Statements/StatementHandler.cpp` |
| Catalog | `nes-inference/include/ModelCatalog.hpp` (`ModelCatalog`, `RegisteredModel`, `ModelSchema{inputs, outputs}`) |
| Parser → plan | `AntlrSQLQueryPlanCreator.cpp:1498-1556` → `LogicalPlanBuilder::addInferModel` |
| Placeholder op | `nes-logical-operators/{include,src}/Operators/InferModelNameLogicalOperator.*` |
| Resolution rule | `nes-query-optimizer/src/Rules/Semantic/InferModelResolutionRule.cpp` |
| Logical op | `nes-logical-operators/{include,src}/Operators/InferModelLogicalOperator.*` |
| Registration | `nes-logical-operators/src/Operators/CMakeLists.txt` (`add_unreflection_entry(LogicalOperator InferModel)`) |
| Lowering rule | `nes-query-compiler/src/LoweringRules/LowerToPhysical/LowerToPhysicalInferModel.cpp` + `add_registry_entry(LoweringRule InferModel)` |
| Physical op | `nes-physical-operators/src/Inference/InferModelPhysicalOperator.cpp` |
| Systest | `nes-systests/inference/InferModel.test` |

---

## Part 3 — The design

### 3.1 Syntax

Catalog + table-valued function, mirroring `MODEL_INFERENCE`:

```sql
CREATE SEMANTIC MODEL sentiment
  SET ('http://host.docker.internal:11434/v1' AS BASE_URL,
       'gemma3:27b'                           AS MODEL,
       'Classify the sentiment as POSITIVE or NEGATIVE' AS PROMPT,
       32 AS BATCH_SIZE)
  INPUT  (description VARSIZED)
  OUTPUT (sentiment VARSIZED, sentiment_confidence FLOAT64);

SELECT * FROM SEM_MAP(sentiment, heartRates) INTO out;
```

Why this shape:
- **The output schema must be declared.** The Python operator infers it at runtime
  (`sem_map.py::_infer_map_output` asks the LLM whether the output is restricted or free text).
  NES needs the schema at *plan* time, before any LLM exists. So the `OUTPUT (...)` clause is
  authoritative, and the Python-side inference degrades into a **prompt-shaping** concern:
  we can still pass the declared enum values into the system prompt, and `_normalize_answer`'s
  fuzzy matching becomes a validation step at the C++ side. Worth porting; not schema inference.
- **Endpoint / model / API key need a home.** There is no secrets mechanism in NES; config goes
  through `DescriptorConfig::ConfigParameter<std::string>` in SQL or worker YAML, exactly as
  `LLMSink`'s `ConfigParametersLLM` does today. A catalog entry means the config is declared once
  and reused, and can be validated at `CREATE` time.
- **Grammar delta is small.** `optionsClause`, `modelInputField`, `modelOutputField` and
  `relationPrimary` already exist; we add a `SEMANTIC` token, one `createSemanticModelDefinition`
  rule, one `semMapSource` rule, and one `#semMapRelation` alternative.
- It leaves room for SEM_FILTER/SEM_JOIN as sibling TVFs sharing the same catalog.

### 3.2 New module: `nes-semantic`

Mirror `nes-inference/`. Contains:
- `SemanticModelCatalog.hpp/.cpp` — `SemanticModelCatalog`, `RegisteredSemanticModel`
  (name + `SemanticModelConfig{baseUrl, model, prompt, apiKeyEnv, batchSize, …}` +
  `ModelSchema`-style `{inputs, outputs}`), with `registerModel` validating at `CREATE` time.
  Copy the private-constructor + friend-`Reflector` idiom from `RegisteredModel`.
- `LlmClient.hpp/.cpp` — a thin synchronous OpenAI-compatible chat-completions client over libcurl
  (`vcpkg/vcpkg.json` already has curl for `LLMSink`), plus the JSON response parsing ported from
  `src/operators/llm_operator.py::_parse_llm_json` (code fences, bare-object extraction, the
  malformed-id repairs) and `_normalize_answer`.

The catalog must be threaded into `PlanRuleRegistryArguments`
(`nes-query-optimizer/registry/include/PlanRuleRegistry.hpp:33`, currently
`{defaultQueryOptimization, sourceCatalog, sinkCatalog, modelCatalog}`) and constructed in
`nes-frontend/apps/cli/CLIStarter.cpp` / `apps/repl/ReplStarter.cpp` alongside `ModelCatalog`.

### 3.3 Logical operator layer

Two operators, exactly as InferModel does it:

**`SemMapNameLogicalOperator`** (`nes-logical-operators/…/Operators/`) — placeholder holding only
the model name + child. `withInferredSchema()` throws (`CannotInferSchema`) so an unresolved plan
can never reach type inference.

**`SemMapLogicalOperator`** — `class SemMapLogicalOperator : public Reorderer, public ManagedByOperator`,
holding a `RegisteredSemanticModel`.
- `NAME = "SemMap"` — must match the lowering-rule registry key.
- `inferLocalSchema()`: for each declared INPUT field, look it up in `child->getOutputSchema()` and
  check the type is compatible (for us: the input must be a string / `VARSIZED`, so the check is
  looser than InferModel's exact-type match). Then append the OUTPUT fields:
  ```cpp
  auto outputFields = childOutput | RangeUnbinder{} | std::ranges::to<std::vector>();
  std::ranges::copy(modelOutputs, std::back_inserter(outputFields));
  Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
  ```
- Implements `Reorderer::getOrderedOutputSchema`. Does **not** need `Reprojecter` — we only append
  columns, never overwrite (same position as InferModel).
- `Reflector` / `Unreflector` specializations + a `detail::ReflectedSemMapLogicalOperator` POD.
- `static_assert(LogicalOperatorConcept<SemMapLogicalOperator>);` and a `std::hash` specialization.

Registration is one line in `nes-logical-operators/src/Operators/CMakeLists.txt`:
`add_unreflection_entry(LogicalOperator SemMap)` plus the `.cpp` in `add_source_files`.

**Resolution rule**: `nes-query-optimizer/src/Rules/Semantic/SemMapResolutionRule.cpp`, a near-copy
of `InferModelResolutionRule` — a `PlanVisitor` that swaps `SemMapNameLogicalOperator` for
`SemMapLogicalOperator` using the catalog, with the same
`needs() = {LogicalSourceExpansionRule, SinkBindingRule, AnonymousSinkBindingRule}` /
`neededBy() = {TypeInferenceRule, SemanticAnalysisBarrier}`. Registered with
`add_registry_entry(PlanRule SemMapResolutionRule KEY SemMapResolution)`.

### 3.4 Lowering rule

`nes-query-compiler/private/LoweringRules/LowerToPhysical/LowerToPhysicalSemMap.hpp` + `src/…cpp`,
registered as `add_registry_entry(LoweringRule SemMap)`. Modelled on `LowerToPhysicalInferModel.cpp`:

```cpp
LoweringRuleResultSubgraph LowerToPhysicalSemMap::apply(LogicalOperator logicalOperator)
{
    const auto semMapOp = logicalOperator.getAs<SemMapLogicalOperator>();
    const auto& model = semMapOp.get().getModel();

    // Worker-side construction of the HTTP client, deliberately deferred to lowering
    // so the coordinator only ships the config across the wire (cf. compileModel()).
    auto client = std::make_shared<LlmClient>(model.getConfig());

    auto physicalOperator = SemMapPhysicalOperator(
        std::move(client), toIdList(inputFields), toIdList(outputFields), model.getConfig());

    const auto memoryLayoutType = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>().value()->memoryLayout;
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        createPhysicalOutputSchema(semMapOp.get().getChildren().at(0).getTraitSet()),
        createPhysicalOutputSchema(logicalOperator.getTraitSet()),
        memoryLayoutType, memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    return {.root = wrapper, .leaves = {wrapper}};
}
```

`INTERMEDIATE` means it fuses into the surrounding pipeline — correct for Phase 1. Phase 2 changes
this to a Build/Probe pair with an attached `OperatorHandler` and a pipeline break.

### 3.5 Physical operator — Phase 1 (synchronous)

`nes-physical-operators/include/SemMapPhysicalOperator.hpp` + `src/Semantic/SemMapPhysicalOperator.cpp`,
listed in `nes-physical-operators/src/CMakeLists.txt`. Structurally identical to
`InferModelPhysicalOperator`, with a per-worker-thread pool of `LlmClient`s held behind a
`shared_ptr` for pointer stability:

```cpp
namespace {
void  setupClients(ThreadLocalLlmClients* t, PipelineExecutionContext* pec) { t->setup(pec->getNumberOfWorkerThreads()); }
// input text in, JSON result out, both as arena-owned buffers; BLOCKING.
int8_t* semMapCall(ThreadLocalLlmClients* t, WorkerThreadId id, const int8_t* text, uint64_t len, uint64_t* outLen);
}

void SemMapPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto clients = nautilus::val<ThreadLocalLlmClients*>(threadLocal.get());
    auto input = record.read(inputFieldNames.at(0)).getRawValueAs<VariableSizedData>();
    // one blocking HTTP round-trip, on the worker thread
    const auto result = nautilus::invoke(semMapCall, clients, ctx.workerThreadId,
                                         input.getContent(), input.getSize(), outLenPtr);
    // write each declared OUTPUT field back out of the parsed result
    for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i) { … record.write(…); }
    executeChild(ctx, record);
}
```

Reuse `InferModelPhysicalOperator`'s varsized handling verbatim: `getRawValueAs<VariableSizedData>()`
on the way in, `arena.allocateVariableSizedData()` + `VarVal(output)` on the way out.

**Be honest about what this is.** One tuple per round-trip, one worker thread parked per in-flight
call. With N worker threads and a ~1s RTT this ceilings at ~N tuples/second. It is correct,
end-to-end testable, and the right thing to land first — but it throws away the two things the
Python operator does well: batching (`Llm_operator._add_to_buffer`, `batch_size` / `max_wait_time`)
and concurrency (`ManagedOpenAIClient`'s per-model semaphore). Those come back in Phase 2.

---

## Part 4 — Phase 2 (design only, not built now): async + batching

Recorded here so Phase 1's interfaces do not paint us into a corner.

Split SEM_MAP into a **Build/Probe pair across a pipeline break**, like `HashJoin` / windows:

- `SemMapOperatorHandler : OperatorHandler` owns the pending-request state, an I/O thread pool or a
  `curl_multi` loop, and a completion queue keyed by `(originId, sequenceNumber, chunkNumber)` —
  all available on `ExecutionContext`.
- `SemMapBuildPhysicalOperator::execute` copies the input text plus the pass-through columns into
  handler-owned native memory and returns immediately. Batching is then free: the handler groups
  rows into `BATCH_SIZE` requests with a `max_wait_time` flush timer, directly mirroring
  `Llm_operator._add_to_buffer` / `_timer_logic`, and issues them concurrently up to a
  `MAX_CONCURRENCY` bound (the port of the per-model semaphore).
- Results are pushed downstream either by the handler calling
  `PipelineExecutionContext::emitBuffer(buf, ContinuationPolicy::POSSIBLE)`, or by the probe side
  using `OpenReturnState::REPEAT` + `repeatTask(buf, backoff)` to yield the worker thread while
  waiting.

Two constraints to design against: `repeatTask` may be called **once per pipeline execution** and
re-executes the *whole* task (so completion state must live in the handler, not in locals), and it
is only checked after `open()`, not after `execute()` — which is precisely why the async version has
to be a pipeline breaker rather than a fused operator.

Batching also unlocks the Python side's **operator fusion** (`SEMMapOperator.fuse`, one LLM call
covering several map/filter steps). That maps naturally onto a NES optimizer rewrite rule merging
adjacent `SemMapLogicalOperator`s — a good third phase, and a genuinely novel optimization to
publish.

---

## Part 5 — Implementation order

1. **`nes-semantic` module** — `SemanticModelCatalog`, `SemanticModelConfig`, `LlmClient` (libcurl
   chat-completions + the ported JSON repair/normalize logic). Unit-testable with a stub HTTP server,
   no NES integration yet.
2. **DDL** — `SEMANTIC` token + `createSemanticModelDefinition` in `AntlrSQL.g4`; binding in
   `StatementBinder.cpp`; a `SemanticModelStatementHandler` in `StatementHandler.cpp`; catalog
   construction in `CLIStarter.cpp` / `ReplStarter.cpp`; `SHOW SEMANTIC MODELS` / `DROP`.
   Verify with `nes-sql-parser/tests/StatementBinderTest.cpp`.
3. **Logical layer** — `SemMapNameLogicalOperator`, `SemMapLogicalOperator`, reflectors, CMake
   registration, `LogicalPlanBuilder::addSemMap`, grammar `semMapSource` + `#semMapRelation`, and
   `enter/exitSemMapRelation` in `AntlrSQLQueryPlanCreator.cpp`. Test:
   `nes-logical-operators/tests/SemMapLogicalOperatorTest.cpp` (copy
   `InferModelLogicalOperatorTest.cpp`) — assert the output schema gains the OUTPUT columns, and
   that an undeclared INPUT field throws `CannotInferSchema`.
4. **Resolution rule** — `SemMapResolutionRule` + `PlanRuleRegistryArguments` field. Verify with
   `EXPLAIN` (the grammar supports it) that the plan shows `SemMap`, not `SemMapName`.
5. **Physical operator + lowering rule** — `SemMapPhysicalOperator`, `LowerToPhysicalSemMap`,
   registry entries. Test: `nes-physical-operators/tests/SemMapPhysicalOperatorTest.cpp` against a
   stub client.
6. **Systest** — `nes-systests/semantic/SemMap.test`, following the `nes-systests/inference/InferModel.test`
   format. Needs a deterministic backend: point `BASE_URL` at a tiny local echo server rather than a
   real LLM, so the test is hermetic.

---

## Verification

- **Unit**: `SemanticModelCatalogTest` (validation, reflect/unreflect round-trip),
  `SemMapLogicalOperatorTest` (schema inference + collision + missing-field errors),
  `SemMapPhysicalOperatorTest` (a stub `LlmClient`, assert the record gains the output columns),
  `StatementBinderTest` (the new DDL).
- **Plan-level**: `EXPLAIN SELECT * FROM SEM_MAP(sentiment, heartRates) INTO out;` — confirms
  parsing, resolution, schema inference and trait decoration without executing anything. This is
  the fastest inner loop and worth using constantly.
- **Systest (hermetic)**: `nes-systests/semantic/SemMap.test` with `ATTACH INLINE` rows and
  `BASE_URL` pointed at a stub returning fixed JSON, asserting exact output rows.
- **End-to-end (real LLM)**: reuse the existing demo data and Ollama setup from `nes/README.md` —
  `data/llm.csv` (16 rows) against `gemma3` on `:11434`, then score against `data/llm_truth.tsv`
  with the snippet already in that README. Expect ~14/16. This directly reproduces the current
  external-process demo as a **single query**, which is the acceptance criterion:
  ```sql
  CREATE SEMANTIC MODEL sentiment SET (…) INPUT (description VARSIZED)
    OUTPUT (sentiment VARSIZED, sentiment_confidence FLOAT64);
  SELECT * FROM SEM_MAP(sentiment, heartRates) INTO llm_answer_file;
  ```
  No `query_id` column, no second `CREATE LOGICAL SOURCE`, no alphabetical-column-order hazard, no
  Python process.
- **Throughput reality check**: run the same query with `number_of_worker_threads` at 1 / 4 / 16 and
  record rows/sec. This measures the Phase 1 ceiling and produces the number that motivates Phase 2.

## Files to build (new) and touch (existing)

**New**: `nes-semantic/` (catalog, config, `LlmClient`);
`nes-logical-operators/{include,src}/Operators/SemMap{,Name}LogicalOperator.*`;
`nes-query-optimizer/{include,src}/Rules/Semantic/SemMapResolutionRule.*`;
`nes-query-compiler/{private,src}/LoweringRules/LowerToPhysical/LowerToPhysicalSemMap.*`;
`nes-physical-operators/{include/SemMapPhysicalOperator.hpp,src/Semantic/SemMapPhysicalOperator.cpp}`;
`nes-systests/semantic/SemMap.test`.

**Touched**: `nes-sql-parser/AntlrSQL.g4`, `src/AntlrSQLQueryPlanCreator.cpp`,
`private/AntlrSQLParser/{AntlrSQLQueryPlanCreator,AntlrSQLHelper}.hpp`, `src/StatementBinder.cpp`;
`nes-logical-operators/{include/Plans/LogicalPlanBuilder.hpp,src/Plans/LogicalPlanBuilder.cpp,src/Operators/CMakeLists.txt}`;
`nes-query-optimizer/{registry/include/PlanRuleRegistry.hpp,src/Rules/Semantic/CMakeLists.txt}`;
`nes-query-compiler/src/LoweringRules/LowerToPhysical/CMakeLists.txt`;
`nes-physical-operators/src/CMakeLists.txt`;
`nes-frontend/{src/Statements/StatementHandler.cpp,apps/cli/CLIStarter.cpp,apps/repl/ReplStarter.cpp}`;
top-level `CMakeLists.txt` (add `nes-semantic`).

**Not touched, and eventually removable**: `nes-plugins/Sinks/LLMSink/`,
`nes-plugins/Sources/LLMRSource/`. Keep them until the native path reaches parity — they are the
working baseline to compare against.
