# SEM_MAP — native semantic operator in NebulaStream (prototype implementation plan)

Supersedes the design sketch in `docs/sematic-opertor-plan.md` (kept as-is; Part 1 of that doc
remains the best background read on the SQL → LogicalPlan → lowering → Nautilus JIT pipeline).
This doc is the build order. Every file/line anchor below was re-verified against `main` on
2026-09-15.

---

## 0. Resolved questions

These were open in the sketch or raised in review. All are settled — no input needed to start building.

| # | Question | Answer |
|---|---|---|
| D1 | HTTP client dependency | **Use libcurl.** Not on `main`, but the `llm` branch already adds `"curl"` to `vcpkg/vcpkg.json` and uses it in `LLMSink.cpp`. Cherry-pick that one-line vcpkg delta; the `curl_easy_*` usage in `LLMSink::post` is the working reference for a synchronous POST. |
| D2 | Where is the Python baseline | `/Users/tim/Documents/work/llm_operator` — readable. Relevant files: `src/operators/llm_operator.py` (prompt assembly, `_parse_llm_json`, `_normalize_answer`, batching), `src/operators/sem_map.py` (output-type inference, fusion), `src/llm_clients.py` (OpenAI-compatible client, per-model concurrency). |
| D3 | Where are `LLMSink` / `LLMRSource` / demo data | Local branch **`llm`**, not `main`: `nes-plugins/Sinks/LLMSink/`, `nes-plugins/Sources/LLMRSource/`. Use as reference only — the native operator replaces them. |
| D4 | Does `nes-semantic` need a root `CMakeLists.txt` edit | **No.** `CMakeLists.txt:253-261` globs every `nes-*` dir containing a `CMakeLists.txt`. Creating the directory is sufficient. |
| D5 | Confidence column | **Dropped from the base version** (review feedback). It was a benchmark artifact; Snowflake Cortex / BigQuery `ML.GENERATE_TEXT` don't surface it to end users either. Still requested in the prompt and parsed, just not written — see §2.1. |
| D6 | Call-site shape | **`SEM_MAP(model, input_col, …)`, not `SEM_MAP(model, source)`** (review feedback). Operates on named columns. Parses for free — see §4, M3.1. |

Remaining genuinely-open decisions are inlined at the milestone where they bite (blocking I/O in M1, LLM failure policy in M4); the two parallel tracks that need other people are in §5.

---

## 1. What we are building

```sql
CREATE SEMANTIC MODEL sentiment
  SET ('http://host.docker.internal:11434/v1' AS BASE_URL,
       'gemma3:27b'                           AS MODEL,
       'Classify the sentiment as POSITIVE or NEGATIVE' AS PROMPT)
  INPUT  (description VARSIZED)
  OUTPUT (sentiment VARSIZED);

SELECT *, SEM_MAP(sentiment, description) AS sentiment FROM reviews INTO out;
```

Two changes from the first draft, both from review:
- **The call site takes input columns, not the whole source.** `SEM_MAP(model, col, …)` rather than
  `SEM_MAP(model, relation)`. The catalog's `INPUT (…)` clause becomes a *typed signature*
  (arity + types); the call site binds actual columns to it positionally.
- **No confidence column.** It was a benchmark artifact. With it gone the operator has a single
  declared output, which is also what makes the expression form above read naturally.

Model declaration stays decoupled from the query — that is the part reviewers explicitly wanted
kept, because it is where cascades, proxy models and fusion get added later without touching
query syntax or the grammar.

Phase 1 scope: **one blocking HTTP round-trip per record**, inside the Nautilus-traced operator,
structurally identical to `InferModelPhysicalOperator`. No batching, no async, no fusion, no proxy
models. Phase 2 (design only, unchanged from the sketch) adds an `OperatorHandler` + Build/Probe
split + `repeatTask` for async batching.

The `OUTPUT (...)` clause is **authoritative**. Python infers the output shape at runtime
(`sem_map.py::_infer_map_output`); NES needs it at plan time. The Python inference degrades into
prompt shaping — see §2.

---

## 2. Semantics to port from the Python operator

This is the part no NES file can tell you. Port these three behaviours faithfully; they are what
makes output from a local model usable.

### 2.1 Prompt assembly
From `llm_operator.py::_build_fused_sysprompt` / `_build_fused_operator_prompt` /
`_process_fused_steps`, collapsed to the single-step (non-fused) case:

```
<system block>
  "You are a helpful AI assistant for semantic data operations."
  "Always respond in JSON format."
  "For each input row (_llm_call_id), return a JSON object where each key is an output field
   name and the value is a JSON object with 'answer' and 'confidence' (0-1)."
  "Output fields: \"sentiment\""
  "Example output: {\"row1\": {\"sentiment\": {\"answer\": \"...\", \"confidence\": 0.9}}}"
  "Do not add explanations."
<operator block>
  "Apply the following 1 operation to each row:"
  "  1. MAP: <PROMPT from CREATE SEMANTIC MODEL> → output field: \"sentiment\""
<data block>
  "Data: {\"row1\": \"<concatenated INPUT field values, space-joined>\"}"
```

Phase 1 sends one row per request, so the payload is always a single-entry object. Keep the
`_llm_call_id` envelope anyway — Phase 2 batching then needs no prompt change.

Note the Python response shape has `<field>.answer` / `<field>.confidence`. We keep asking for
both and keep parsing both — the wire format stays byte-identical to the Python baseline, which is
what makes the M5 parity comparison meaningful — but **only `answer` is written to the record**.
Confidence is discarded in Phase 1 (review feedback: it was a benchmark artifact, and neither
Snowflake Cortex nor BigQuery `ML.GENERATE_TEXT` surfaces it by default). If the benchmark needs it
back it is one extra declared OUTPUT field plus one write in the operator, not a redesign.

### 2.2 JSON extraction — port `_parse_llm_json` (llm_operator.py:31-74)
Four fallbacks in order, all needed against local models:
1. `json.loads` straight.
2. Strip a markdown code fence: `` ```(?:json)?\s*(.*?)\s*``` `` (DOTALL).
3. Extract the outermost bare object: `\{.*\}` (DOTALL).
4. Repair malformed id→object pairs, then retry:
   - drop a stray wrapper key: `"_llm_call_id"\s*:\s*(?=")` → ``
   - comma-instead-of-colon: `"([^"]+)"\s*,\s*(?=\{)` → `"\1": `

On total failure Python returns `{}` and the row gets `default_value`. See the open question under M4 for the C++ policy.

Use a real JSON lib — `nlohmann/json` if already vendored, otherwise whatever `nes-common` uses;
do **not** hand-roll. Regexes only for the pre-cleanup steps.

### 2.3 Answer normalisation — port `_normalize_answer` (llm_operator.py:355-372)
Only applies when the declared output is a restricted value set. Cascade:
uppercase exact match → strip parenthetical `\s*\(.*?\)` then exact → substring containment →
fuzzy `difflib.get_close_matches(cutoff=0.6)` → `default`.

`difflib` has no C++ equivalent; implement the last step as normalised Levenshtein ratio ≥ 0.6
(`difflib`'s ratio is 2·M/T on matching blocks — close enough for a prototype; note the
divergence in a comment).

**Where do the valid values come from?** Python asks the LLM (`sem_map.py::_INFER_PROMPT`). We
have the schema instead. Phase 1: infer nothing, and accept an optional `VALUES` option on the
`CREATE SEMANTIC MODEL` options clause (`'POSITIVE,NEGATIVE' AS OUTPUT_VALUES`) that both feeds
the prompt and enables normalisation. If absent → free text, no normalisation. This is strictly
more deterministic than the Python path and costs one round-trip less.

---

## 3. Reference chain (all verified on `main`)

Copy these, in this order, for each layer:

| Layer | Reference file |
|---|---|
| Module layout | `nes-inference/` (`CMakeLists.txt`, `include/`, `src/`, `tests/`) |
| Catalog + reflect idiom | `nes-inference/include/ModelCatalog.hpp:57-95` (`RegisteredModel`: private ctor, `friend Reflector/Unreflector`) |
| HTTP over curl | `git show llm:nes-plugins/Sinks/LLMSink/LLMSink.cpp` |
| Grammar | `nes-sql-parser/AntlrSQL.g4` — `createModelDefinition:81`, `optionsClause:75`, `modelInputField:84`, `dropModel:96-97`, `showModelsSubject:113`, `modelInferenceSource:173`, `#modelInferenceRelation:170`, tokens `MODEL:555 MODELS:556 MODEL_INFERENCE:557 INPUT:558 OUTPUT:559` |
| Plan creation | `AntlrSQLQueryPlanCreator.cpp:1498-1556` (`enterModelInferenceRelation` / `buildModelInferencePlan` / `exit…`); `LogicalPlanBuilder.hpp:96-100` (`addInferModel`) |
| DDL binding | `StatementBinder.cpp:255-292` (create), `:313-318` (dispatch), `:439-447` (SHOW), `:505-510` (DROP) |
| DDL handler | `StatementHandler.hpp:257-265` + impl `:239-279` (`ModelStatementHandler`) |
| Logical operator | `nes-logical-operators/src/Operators/InferModelLogicalOperator.cpp:89-123` (`inferLocalSchema`, `RangeUnbinder{}` + `tryCreateCollisionFree`) |
| Resolution rule | `nes-query-optimizer/src/Rules/Semantic/InferModelResolutionRule.cpp` (name-op → resolved-op swap; `needs()` = `{LogicalSourceExpansionRule, SinkBindingRule, AnonymousSinkBindingRule}`, `neededBy()` = `{TypeInferenceRule, SemanticAnalysisBarrier}`) |
| Lowering | `nes-query-compiler/src/LoweringRules/LowerToPhysical/LowerToPhysicalInferModel.cpp` |
| Physical operator | `nes-physical-operators/src/Inference/InferModelPhysicalOperator.cpp` |
| Physical-op unit test | `nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp` (Scan → op → Emit on a `MockedPipelineContext`) |
| Systest format | `nes-systests/inference/InferModel.test` (SLT-style, `ATTACH INLINE`; suite dirs are auto-discovered, no CMake entry) |

Catalog threading surface (all four sites must learn about `SemanticModelCatalog`, exactly as they
know `ModelCatalog`): `PlanRuleRegistry.hpp:33` (`PlanRuleRegistryArguments`), `QueryOptimizer.hpp:36-45`
(ctor), and construction in `CLIStarter.cpp:722-732` & `:763-773`, `ReplStarter.cpp:251-297`,
`SystestBinder.cpp:510-520`.

---

## 4. Milestones

Ordered so the novel risk — a blocking HTTP call inside JIT-traced code — is proven before any
grammar work locks in.

### M0 — `nes-semantic` module, standalone (no NES integration)

New files, zero edits to existing code except the vcpkg line:

```
vcpkg/vcpkg.json                       # + "curl"   (cherry-pick from branch llm)
nes-semantic/CMakeLists.txt            # mirror nes-inference/CMakeLists.txt minus OpenVINO
nes-semantic/include/SemanticModelConfig.hpp     # {baseUrl, model, prompt, apiKeyEnv, outputValues}
nes-semantic/include/SemanticModelCatalog.hpp    # SemanticModelCatalog, RegisteredSemanticModel
nes-semantic/include/LlmClient.hpp               # virtual iface: map(inputText) -> per-field results
nes-semantic/src/SemanticModelCatalog.cpp
nes-semantic/src/CurlLlmClient.cpp               # OpenAI-compatible POST {baseUrl}/chat/completions
nes-semantic/src/ResponseParsing.cpp             # _parse_llm_json + _normalize_answer ports (§2.2, §2.3)
nes-semantic/tests/SemanticModelCatalogTest.cpp
nes-semantic/tests/ResponseParsingTest.cpp
nes-semantic/tests/CurlLlmClientTest.cpp
```

- `LlmClient` is a **virtual interface** from day one — the stub subclass is what M3's operator
  test and M4's systest use. Do not skip this; it is the whole hermeticity story.
- `ResponseParsingTest` is pure and cheap: table-driven over the four `_parse_llm_json` paths and
  the five `_normalize_answer` rungs. Write the malformed-response fixtures by copying the exact
  shapes the Python regex comments describe.
- `CurlLlmClientTest` runs against a ~60-line Boost.Asio acceptor thread in the test
  (`boost-asio` is already a dependency, no new test dep).
- API key is read from **an env-var name** given in SQL (`API_KEY_ENV`), never a literal.

**Exit:** `nes-semantic` builds; all three tests green. Nothing else in the tree changed.

### M1 — Physical operator + lowering (the risky bit, still no SQL)

Deliberately before the grammar: it needs no parser and has the fastest test harness in the repo.

- `nes-physical-operators/src/Semantic/SemMapPhysicalOperator.{hpp,cpp}` — structural copy of
  `InferModelPhysicalOperator`:
  - `detail::ThreadLocalLlmClients` held by `shared_ptr`, `setup()` calls
    `nautilus::invoke(setupClients, …, executionCtx.pipelineContext)` which sizes the pool from
    `pec->getNumberOfWorkerThreads()` (mirrors `setupSessions`, `InferModelPhysicalOperator.cpp:80-83`).
  - `execute()` reads the INPUT field as `VariableSizedData` via `record.read(...).getRawValueAs<VariableSizedData>()`,
    hands `(content, size, workerThreadId)` to one `nautilus::invoke(semMapCall, …)`, gets back a
    pointer to an arena-owned scratch buffer.
  - Output handling: **dropping the confidence column removed the one real deviation from
    InferModel.** A single VARSIZED output is exactly `InferModelPhysicalOperator.cpp:152-158` —
    `arena.allocateVariableSizedData(len)` + `nautilus::memcpy` + `record.write(field, VarVal(v))`.
    Still write the loop over `outputFieldNames` with the per-field type branch on
    `nautilus::static_val<size_t>` (statically known types, free at trace time) so multi-output
    models work later, but Phase 1 exercises only the single-VARSIZED path.
- `nes-query-compiler/src/LoweringRules/LowerToPhysical/LowerToPhysicalSemMap.cpp` — copy the real
  file, including:
  - the `toIdList` field→`QualifiedIdentifier` projection,
  - `MemoryLayoutTypeTrait` PRECONDITION + `createPhysicalOutputSchema` for in/out,
  - `PipelineLocation::INTERMEDIATE`,
  - **the per-child leaves pattern** — `std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};` (the sketch's `.leaves = {wrapper}` was wrong).
  - Construct the concrete `CurlLlmClient` here (worker side), from the config carried on the
    logical operator.
- Registry: `add_registry_entry(LoweringRule SemMap)` + the `nes-physical-operators/src/CMakeLists.txt` entry.
- `nes-physical-operators/tests/SemMapPhysicalOperatorTest.cpp` — copy the InferModel harness,
  inject a stub `LlmClient` returning a canned `{"row1":{"sentiment":{"answer":"POSITIVE","confidence":0.9}}}`
  (confidence present in the payload, discarded by the operator — see §2.1).

**Exit:** a compiled Scan → SemMap → Emit pipeline turns one input record into a record carrying
the declared OUTPUT column. **This is the prototype's core risk, retired.**

> **Open — blocking I/O inside a pipeline task.** The operator parks a worker thread for the full
> LLM latency (100ms–seconds). With `number_of_worker_threads = N`, throughput ceiling is
> `N / latency` rows/sec, and those threads are unavailable to every other query on the worker.
> That is acceptable and expected for Phase 1 — it is precisely the number that motivates Phase 2 —
> but confirm no shared-worker benchmark runs concurrently with the demo.

### M2 — Logical layer + resolution (EXPLAIN shows SEM_MAP)

- `SemMapNameLogicalOperator` — placeholder carrying just the model name; `withInferredSchema()`
  throws (it must never survive the resolution rule).
- `SemMapLogicalOperator` — copy `InferModelLogicalOperator`, swap `RegisteredModel` →
  `RegisteredSemanticModel`. **Difference from InferModel:** InferModel takes its input field
  names from the catalog entry; we take them from the **call site** and check them against the
  catalog's declared signature. So `inferLocalSchema` checks: the call-site column count matches
  the catalog's `INPUT` arity, each named column exists on the child, is non-nullable, and is
  **VARSIZED** (looser than InferModel's exact-type match — we stringify inputs anyway); throws
  `CannotInferSchema` otherwise. Appends OUTPUT fields via
  `RangeUnbinder{}` + `tryCreateCollisionFree` (`InferModelLogicalOperator.cpp:116-123`).
- Reflector/Unreflector + `detail::ReflectedSemMapLogicalOperator`; two entries in
  `nes-logical-operators/src/Operators/CMakeLists.txt`:
  `add_unreflection_entry(LogicalOperator SemMap)` and `…(LogicalOperator SemMapName)`.
- `SemMapResolutionRule` — copy `InferModelResolutionRule.cpp` verbatim, swap the catalog and the
  two operator types; keep `needs()`/`neededBy()` identical.
  `add_registry_entry(PlanRule SemMapResolutionRule KEY SemMapResolution)`.
- Thread `SemanticModelCatalog` through the four sites listed in §3.

**Exit:** `SemMapLogicalOperatorTest` covers schema-append, missing INPUT field → `CannotInferSchema`,
and name collision → `CannotInferSchema`.

### M3 — Grammar + DDL end-to-end

#### 3.1 The call site needs no grammar change

With the call site taking columns instead of a relation, `SEM_MAP(sentiment, description)` **already
parses today**: `valueExpression` has a `#functionCall` alternative at `AntlrSQL.g4:392`, and
`functionName: IDENTIFIER | AVG | MAX | …` (`:350`) admits any identifier. Both arguments parse as
plain `expression`s. So the call-site delta is **zero tokens, zero rules** — strictly cheaper than
the TVF shape in the first draft, which needed a `SEM_MAP` token and a `relationPrimary` alternative.

What it costs instead is a **binder-level rewrite**: a `SEM_MAP(...)` function call in the select
list has to be recognised and turned into a `SemMapNameLogicalOperator` above the source, with its
output bound to the alias — rather than staying an expression evaluated inside a projection. That is
a real design choice (desugar-expression-to-operator vs. a first-class TVF node) and it is the main
thing to put in front of Leonhard — see §5.1. Both shapes reach the identical logical operator, so
**M1/M2 are unaffected either way**.

#### 3.2 DDL still needs grammar

Only `SEMANTIC` is a new token; `MODEL`/`MODELS`/`INPUT`/`OUTPUT` already exist at `:555-559`:

```g4
createDefinition: … | createSemanticModelDefinition;                       // extend :69
createSemanticModelDefinition
    : SEMANTIC MODEL modelName=identifier optionsClause
      INPUT  '(' modelInputField  (',' modelInputField)*  ')'
      OUTPUT '(' modelOutputField (',' modelOutputField)* ')';
dropSubject: … | dropSemanticModel;   dropSemanticModel: SEMANTIC MODEL;   // extend :96-97
showSubject: … | SEMANTIC MODELS #showSemanticModelsSubject;               // extend :113
```

Then, each by copy: `bindCreateSemanticModelStatement` (from `StatementBinder.cpp:255-292`) +
dispatch (`:313-318`) + SHOW (`:439-447`) + DROP (`:505-510`); the three statement types;
`SemanticModelStatementHandler` (from `StatementHandler.hpp:257-265`, impl `:239-279`);
`LogicalPlanBuilder::addSemMap` (`LogicalPlanBuilder.hpp:96-100`). The plan-creation hook is the
`#functionCall` visitor rather than `enter/exitModelInferenceRelation`, but
`AntlrSQLQueryPlanCreator.cpp:1498-1556` is still the shape to copy for how a plan node gets built
and attached.

Validate config at `CREATE` time in `SemanticModelCatalog::registerModel`: `BASE_URL` parses as a
URL, `MODEL` non-empty, `PROMPT` non-empty, `API_KEY_ENV` (if given) names a set env var. Do **not**
contact the endpoint at CREATE time — it makes DDL fail on a cold Ollama.

**Exit:** `StatementBinderTest` covers CREATE/SHOW/DROP; in the REPL,
`EXPLAIN SELECT *, SEM_MAP(sentiment, description) AS sentiment FROM reviews INTO out;` renders a
`SEM_MAP` node with the OUTPUT column in the schema. From here on this is the fast inner loop.

### M4 — Hermetic systest

`nes-systests/semantic/SemMap.test` — auto-discovered, no CMake entry needed.

The systest runs a real worker and has no fixture to start an HTTP server, so hermeticity needs a
seam. Recommended: a **`mock://` base-URL scheme** handled in the `LlmClient` factory, returning
deterministic canned answers derived from the input text (e.g. echo uppercased, confidence 1.0).
That keeps the seam inside `nes-semantic`, adds no test infrastructure, and exercises the entire
grammar → lowering → execute path for real.

> **Open — malformed / unreachable LLM at runtime.** Options: (a) throw, failing the query loudly;
> (b) write `default_value` + confidence 0, matching the Python baseline; (c) one retry then (b).
> Recommendation for the prototype: **(a) throw on transport failure, (b) default-fill on
> unparseable content.** That distinguishes "your endpoint is down" from "your model is chatty",
> which is the distinction you will actually be debugging.

**Exit:** `SemMap.test` green with `mock://`, in CI, with no network.

### M5 — Real endpoint + the Phase 2 motivation number

- Run against real Ollama with the §1 query — single query, single source, no `query_id` plumbing.
- Sweep `number_of_worker_threads` ∈ {1, 4, 16}, record rows/sec. This is the Phase 1 ceiling and
  the quantitative argument for the async/batched Phase 2.
- Parity check against the Python operator on the same data + prompt + model: compare the
  `sentiment` column row-for-row, and report disagreement rate. Non-zero is expected (sampling);
  a systematic skew means the prompt port in §2.1 drifted.

---

## 5. Parallel tracks — non-blocking

Neither of these holds up M0–M2. Both are decisions better made with the people who own the
surrounding machinery, and both can run while the operator gets built.

### 5.1 Grammar sync — Leonhard Rose
Confirm the "build the cheapest shape first, revisit the syntax later" strategy explicitly, so it's
an agreed approach rather than an assumption. Agenda:
- **Expression-desugar vs. first-class TVF node** (§4, M3.1). `SEM_MAP(model, cols…)` parses for free
  today via `#functionCall`, but turning it into an operator is a binder rewrite. Which does NES
  want as the pattern for semantic operators generally?
- **Queries with several semantic operators chained**, e.g. a SEM_MAP feeding a SEM_FILTER feeding a
  SEM_JOIN. Show what the plan looks like and what the optimizer does with it. Current read: **no new
  dependency** — each is an ordinary logical node, existing rules apply, and the resolution rule
  slots in exactly where `InferModelResolutionRule` does. Worth confirming rather than asserting.
- Where operator **fusion** would eventually live (the Python prototype fuses adjacent MAP/FILTER
  steps into one LLM call). That is an optimizer rule, and its feasibility is partly a syntax
  question — worth surfacing early even though it is far out of Phase 1 scope.

### 5.2 Catalog + secrets — Yannik
He owns the NES coordinator, which is where catalog state and any credential handling live. Present
the problem rather than a solution: semantic models need an endpoint, a model name and an API key,
and NES has no secrets mechanism today. Bring:
- How other systems handle it — **Snowflake** (`CREATE SECRET` + `EXTERNAL ACCESS INTEGRATION`, so
  credentials are a separate grantable object, never inline in SQL), **BigQuery** (`CREATE MODEL`
  with a `CONNECTION` object), **Databricks** (Unity Catalog service credentials). The common shape:
  the credential is its own catalog object with its own grants, referenced by name.
- Our current stopgap: `API_KEY_ENV` names an environment variable, so no secret is ever in SQL text
  or in a serialised plan. Fine for a prototype, explicitly not an answer.
- The recent LLM-operator refactor for picking models/hosts as prior art for what the config surface
  should look like.

---

## 6. File inventory

**New (~22 files):**
`nes-semantic/*` (11) · `SemMapPhysicalOperator.{hpp,cpp}` + test (3) · `LowerToPhysicalSemMap.{hpp,cpp}` (2) ·
`SemMapLogicalOperator.{hpp,cpp}` + `SemMapNameLogicalOperator.{hpp,cpp}` + test (5) ·
`SemMapResolutionRule.{hpp,cpp}` (2) · `nes-systests/semantic/SemMap.test` (1) ·
three statement types + `SemanticModelStatementHandler`.

**Touched (existing):**
`vcpkg/vcpkg.json` · `nes-sql-parser/AntlrSQL.g4` (DDL only — the call site needs no grammar change) · `AntlrSQLQueryPlanCreator.cpp` ·
`LogicalPlanBuilder.{hpp,cpp}` · `StatementBinder.cpp` · `StatementHandler.{hpp,cpp}` ·
`PlanRuleRegistry.hpp` · `QueryOptimizer.{hpp,cpp}` · `CLIStarter.cpp` · `ReplStarter.cpp` ·
`SystestBinder.cpp` · four `CMakeLists.txt` registry files.
**Not touched:** root `CMakeLists.txt` (auto-glob), `nes-systests/CMakeLists.txt` (auto-discovery).

---

## 7. First session

M0 is fully unblocked. Concretely:

1. Cherry-pick the `"curl"` line into `vcpkg/vcpkg.json` from branch `llm`.
2. `nes-semantic/CMakeLists.txt` mirroring `nes-inference/CMakeLists.txt` (drop OpenVINO).
3. `SemanticModelConfig.hpp` + `SemanticModelCatalog.{hpp,cpp}` using the `RegisteredModel`
   private-ctor/friend-Reflector idiom (`ModelCatalog.hpp:57-95`).
4. `ResponseParsing.{hpp,cpp}` — the §2.2/§2.3 ports — plus its table-driven test. **Start here if
   you want the highest-value hour**: it is pure, fully testable, and captures the only knowledge
   that lives exclusively in the Python repo.
5. `LlmClient.hpp` interface + `CurlLlmClient.cpp` + Asio-stub test.

Zero changes to existing NES code beyond one vcpkg line, and a green test run at the end.
