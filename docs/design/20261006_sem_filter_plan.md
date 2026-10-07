# SEM_FILTER, with async execution and SEM_MAP/SEM_FILTER fusion

## Context

`feat/async-operator` added a complete SEM_MAP: SQL, catalog, logical and physical operators,
lowering, and an asynchronous path. In that path, `AsyncOperatorSplitter` cuts the plan and
`SemanticMapExecutor` runs on `AsyncSource` threads. This document covers the next operator ported
from the Python prototype, `llm_operator/operators/sem_filter.py`. It also covers operator fusion
(`fuse()` / `Coordinator._apply_fusion`). Fusion is the reason `SemanticModelConfig::steps` is a list.

Out of scope: proxy models (`ProxyTrainer`). They need an embedder and a classifier on the worker.
See "Follow-up work" below.

## Target syntax

```sql
CREATE SEM_MODEL is_positive
INPUT (reviewText VARSIZED)                -- no OUTPUT clause: a filter model
SET ('The review is positive' AS LLM.PROMPT, 'http://localhost:11434/v1' AS LLM.ENDPOINT,
     'llama3.1:8b' AS LLM.MODEL_NAME, 'ASYNC' AS LLM.EXECUTION, 8 AS LLM.BATCH_SIZE,
     'TRUE' AS LLM.FUSION);

SELECT * FROM SEM_FILTER(is_positive, reviews) INTO out;                          -- schema = reviews
SELECT * FROM SEM_FILTER(is_positive, SEM_MAP(sentiment_clf, reviews)) INTO out;  -- fusable
```

## Behaviour

The Python reference is the authority for behaviour. The golden prompts in
`SemanticFilterCodecTest` and `SemanticMapCodecTest` were produced by running the reference's
operators with `_call_llm` replaced by a recorder.

| Step list | Layout (reference function) | Envelope |
|---|---|---|
| one FILTER | base `sysprompt` + `Filter rows: <p>` (`_filter_with_llm`) | `{row: {answer, confidence}}` |
| N FILTER | base `sysprompt` + "Filter rows that satisfy ALL of the following conditions: …" (`_rebuild_filter_operator_prompt`) | flat, one verdict per row |
| MAP and FILTER mixed | fused sysprompt + "Apply the following N operations", FILTER lines with `→ output field: "__filter_k" (true/false)` (`_process_fused_steps`) | `{row: {col: {answer, confidence}}}` |
| MAP only | as above, without FILTER lines; byte-identical to the SEM_MAP layout before this change | nested |

In every layout, the optional `Dataset context: …\n` comes before `Data: ` + `json.dumps(payload)`.

- **A row passes** only if every FILTER verdict is affirmative. MAP columns are written exactly as
  SEM_MAP writes them: normalized against `OUTPUT_VALUES`, with `DEFAULT_VALUE` as the fallback.
- **Failures**: a transport failure throws `InferenceRuntimeFailure` and fails the query, the same
  rule SEM_MAP follows. An unusable answer drops the row. No confidence column is projected, and no
  `llm_ts` is written.
- **Defaults**: `BATCH_SIZE > 1` still requires ASYNC. `OUTPUT_VALUES` and `DEFAULT_VALUE` are
  rejected on a filter model.

### Truthiness, and the one deliberate deviation

These verdicts pass:

- JSON `true`
- the string `"true"` or `"yes"` in any case, surrounded by any whitespace
- a non-zero number

Everything else drops the row:

- `false`, `null` and `0`
- any other string
- an array or an object
- a missing row, a missing `answer`, or an unparseable response

A row value that is not an object is taken as the verdict itself, as the reference's
`bool(result)` does.

**Deviation:** the reference tests the answer with Python truthiness (`if passed:`), so the *string*
`"false"`, like any non-empty string, keeps the row. Real models do answer `"false"` as a string.
Keeping such rows would invert the filter for exactly the answers a model gives most confidently, so
NES rejects them. Parity measurements are unaffected as long as the model answers with JSON
booleans, which the sysprompt's example asks for.

## Fusion

`SemanticFusionRule` runs after `SemanticMapResolutionRule` and before type inference. It works
bottom-up: a parent semantic operator is fused into its single child when all of these hold:

- both models set `'TRUE' AS LLM.FUSION`. This makes fusion opt-in, like the reference's per-query
  `fusion=False` default, so the measured unfused baselines stay reproducible.
- the child feeds nothing else. The parent count comes from the visitor's down pass, which receives
  one context per parent edge.
- both declare the same INPUT fields in the same order (the reference compares `depends_on`).
- their transport configuration is equal: endpoint, model, backend, API key variable, payload
  format, dataset prompt, execution, batch size, concurrency, retries, wait time, timeout and
  ordering.
- the MAP output columns don't collide.

`fuseSemanticModels(child, parent)` names the result `<child>+<parent>`. It concatenates the steps
(upstream first, as `op.fuse(next)` does) and renumbers FILTER steps `__filter_0..n`. It concatenates
the outputs and takes the inputs from the child. The fused operator is a SEM_FILTER if any step
filters, and a SEM_MAP otherwise. Its async marker is rebuilt from the fused model, because the old
markers describe only half the prompt. A chain collapses in the same pass. `AsyncOperatorSplitter`
needs no change, because it acts on the trait.

## Async: dropping records

`AsyncRecordResult` gained `bool keep = true`. `AsyncSource::fillTupleBuffer` now walks a
`consumedRecords` cursor over the input. It writes only kept records until the output buffer is full
or the input is exhausted. When every record of an input buffer is dropped, the source still emits
an empty buffer for it. That buffer carries the sequence number, the watermark and the last-chunk
flag. `AsyncLifecycleTest.AFullyDroppedBufferStillCarriesItsMetadata` pins this. The test hangs
if the source swallows such a buffer instead. `SemanticFilterAsyncWindow.test` runs the case end to
end: its first 40 s are all `false`, so several whole buffers are dropped before any record
survives. It checks only that the window counts are correct, though. A mutation that swallowed the
empty buffers still passed it, because the end of the finite stream flushes every window anyway. Dropped records that directly follow a full output
buffer are consumed with it, so a buffer that ends in dropped records finishes without an extra,
empty chunk (`AsyncLifecycleTest.DropsAcrossChunks`).

## As built (2026-10-06)

| Area | Where | Notes |
|---|---|---|
| Grammar | `AntlrSQL.g4` | `OUTPUT (...)` optional. `SEM_FILTER` token. `semanticFilterSource #semanticFilterRelation`. One shared `semanticInput` rule (stream, subquery, nested SEM_MAP or SEM_FILTER), so the two nest either way. |
| Parser | `AntlrSQLQueryPlanCreator.cpp` | `isSemanticFilter` flag in the FROM guard. `buildSemanticPlan` dispatches on the source kind. `LogicalPlanBuilder::addSemanticFilter`. |
| Handler | `StatementHandler.cpp` | No OUTPUT gives one FILTER step (`outputColumn = ""`). `FUSION` option. `kind` column in CREATE/SHOW output. |
| Catalog | `SemanticModelCatalog` | `fusion` (reflection, async wire, `operator==`). `outputs.size() == #MAP steps`. FILTER steps reject `outputValues`/`defaultValue`. `hasFilterStep()`. `fuseSemanticModels`. |
| Codec | `SemanticPromptLayout` (private), `SemanticFilterCodec` | Fused layout, JSON rendering and MAP-answer extraction shared with `SemanticMapCodec`. `findMember` and `isAffirmative` live in `ResponseParsing`. |
| Mock | `MockSemanticBackend`, `SemanticBackendFactory` | The factory passes the codec's response columns (`__filter_k` included). No columns means the flat envelope. |
| Logical | `SemanticFilter[Name]LogicalOperator` | Child schema plus one column per MAP step (only when fused). EXPLAIN shows `model:` for both semantic operators. |
| Optimizer | `SemanticMapResolutionRule`, `SemanticAsyncExecution`, `SemanticFusionRule` | Resolution handles both kinds and rejects the wrong one (`InvalidSemanticModel`). Executor type `SemanticFilter` when any step filters. |
| Sync physical | `SemanticOperatorState<Codec>` (private), `SemanticFilterPhysicalOperator`, `LowerToPhysicalSemanticFilter` | Shared per-thread backends, semaphore and round trip. The filter calls the child only when the record passed. |
| Async | `AsyncRecordResult::keep`, `AsyncSource`, `SemanticExecutorCore`, `SemanticFilterExecutor` | Both executors share the core (payload decode, API key, field lookup, backends, transport). `DelayExecutor` gained `drop_prefix` for framework tests. |

## Follow-up work

- **Proxy models.** The reference trains a classifier on LLM-labelled rows and then routes rows
  through it instead of the LLM. Porting it needs an embedder and a classifier on the worker, plus
  a training trigger shared across worker threads or executor threads. It also needs a rule for
  when a fused filter may use the proxy; the reference refuses to fuse a filter with a map in proxy
  mode.
- **Predicate reordering** (`reorder=True` in the reference), which pushes SEM_FILTER as early as
  possible, is not implemented. A SEM_FILTER currently stays where the query puts it.
