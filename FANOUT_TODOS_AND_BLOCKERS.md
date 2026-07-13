# Multi-Successor Pipelines (Fan-Out): Todos & Blockers

**Branch:** `change-running-plan-add-succesors-poc`
**Goal:** Allow one pipeline's output to feed multiple successor pipelines, as step 1 toward attaching statistic queries (e.g. throughput counters) to already-running queries. Step 2 — modifying a *running* plan — is deferred (see [Deferred](#deferred-out-of-scope-for-this-branch)).

> **STATUS (2026-07-11): PoC implemented and passing.** All blockers B1–B5 below are fixed on this branch.
> End-to-end proof: `MultiSinkE2ETest` runs one query with a file source, a *shared* selection pipeline, and two file sinks —
> both sinks receive the identical, complete stream through one pipeline with two successors. Unit and integration tests cover
> the flip, pipelining, and compile shapes; the fan-in (union/join) regression suite stays green.
> Bonus find: the variadic `TraitSet{A, B}` constructor silently dropped all traits but the last (comma-fold bug,
> `nes-query-optimizer/include/Traits/TraitSet.hpp`) — fixed here; it was latent because no caller passed two traits before.

> **UPDATE (2026-07-12): the optimizer now handles DAG plans (Option A implemented).** A memoizing DAG-rebuild
> framework (`transformPlan`, `nes-logical-operators/include/Plans/LogicalPlan.hpp`) replaced the per-root recursion in
> ALL rules of SemanticAnalyzer + RuleBasedOptimizer; shared subplans survive every rule, and `OriginIdInference`
> assigns one origin set per shared source by construction. Proof: `CompileFanOutPlanTest.DagPlanSurvivesFullOptimizerPipeline`
> pushes an *unbound* two-sink DAG (source by name) through the full rule pipeline and compiles it to 1 source / 2 sinks /
> shared pipeline with 2 successors. S1 below is thereby resolved for the rule pipeline; only **operator placement**
> (`OperatorPlacer` → `BottomUpPlacement`) remains single-root and now fails loudly on multi-root plans instead of
> silently dropping sinks.

## TL;DR

The engine was *designed toward* DAGs but never wired up for them end-to-end. The **runtime already executes multi-successor plans correctly** (successor vectors, emit fan-out, termination cascade — all in place and partially tested). Everything missing sits in the **compile path** (3 sites) plus **one runtime instantiation blocker**. The optimizer structurally cannot preserve DAGs, so the PoC constructs the fan-out plan *after* optimization.

```
        source                      [src] ──> [scan|filter|emit] ──┬──> [sinkA]
          |                                                        └──> [sinkB]
       filter   (shared)      ==>
        /    \                      one pipeline, two successors —
    sinkA    sinkB                  runtime handles this today; the compiler can't produce it yet
```

## What already works (verified, no changes needed)

| Component | Evidence |
|-----------|----------|
| Runtime successor fan-out: emit delivers each buffer to **all** successors | `nes-query-engine/QueryEngine.cpp:496-507` |
| Termination/EoS cascades to all successors | `QueryEngine.cpp:639-646`, `RunningQueryPlan.cpp:65-68` |
| Sources feeding multiple successor pipelines | `nes-query-engine/RunningSource.hpp:74` |
| Successor vectors throughout executable structures | `nes-executable/include/CompiledQueryPlan.hpp:36,50`, `Pipeline.hpp:87` |
| Shared-pipeline dedup in lowering & runtime construction (exercised by union/join fan-in today) | `LowerToCompiledQueryPlanPhase.cpp:138-152`, `RunningQueryPlan.cpp:122-171` |
| Engine-level test of one pipeline emitting to two sinks | `nes-query-engine/tests/QueryEngineTest.cpp:1260-1293` |
| Sequence numbers/watermarks under **static** fan-out: both branches see identical buffers from the single emit — no conflict | `EmitOperatorHandler` is per-pipeline, not per-successor |

## Blockers (must fix — these are the todos of the PoC)

- [x] **B1 — Logical→physical lowering duplicates shared subplans.** `lowerOperatorRecursively` has no memoization: an operator shared by two sinks is lowered once per parent, duplicating the whole upstream *including the source* (data would be read twice instead of fanned out). Also, only the first sink root is passed on; the rest are silently dropped.
  → Fix: memoize by `OperatorId`, add all roots. (`nes-query-compiler/src/Phases/LowerToPhysicalOperators.cpp:77-136`)

- [x] **B2 — `PhysicalPlanBuilder::flip` rejects multiple roots.** `PRECONDITION(rootOperators.size() == 1)`. Ironically the rest of the function is already fully DAG-capable (pointer-identity dedup, in-degree computation, DAG invariant checks).
  → Fix: relax precondition, collect nodes from all roots. (`nes-query-compiler/src/PhysicalPlanBuilder.cpp:139,170`)

- [x] **B3 — PipeliningPhase silently mis-compiles fan-out.** Fan-*in* (union/join) is handled via merge points, but a node whose output has 2 consumers gets both consumer chains appended into **one linear pipeline** — the second consumer would read the first consumer's output. No error, just a wrong plan.
  → Fix: mirror the merge-point idea — at out-degree > 1, close the pipeline with a single emit and start each consumer as a new successor pipeline (`ForceNewPipelineClosed` policy); give differently-formatted sinks their own formatting pipelines. (`nes-query-compiler/src/Phases/PipeliningPhase.cpp:181-422`)

- [x] **B4 — `ExecutableQueryPlan::instantiate` throws on >1 sink.** `NotImplemented("...exactly one sink per query plan")`, and the single move-only `BackpressureController` is owned by that one sink.
  → Fix: loop over sinks; PoC gives only the first sink a live backpressure channel (see risk R2). (`nes-runtime/src/ExecutableQueryPlan.cpp:74-77,81`)

- [x] **B5 — Test coverage for the new paths.** Systest cannot express two sinks (one result file per query, `SystestBinder.cpp:781`), so:
  - unit tests: flip diamond shape; pipelining fan-out shapes (one emit, two successors; mixed sink formats; fan-in regression),
  - compiler integration test: two-sink DAG plan → `CompiledQueryPlan` with 1 source / 2 sinks / shared pipeline with 2 successors,
  - new e2e gtest harness in `nes-single-node-worker/tests/` (module has none yet): two file sinks must receive the identical, complete row set.

## Design options considered & tradeoffs

**Option 1 — Compiler DAG support with post-optimizer plan construction _(chosen)_.**
Build the logical plan as a true DAG (two sink roots sharing the same operator instance) after optimization; fix B1–B4 so it compiles into a pipeline with two successors.
- *Pros:* principled — matches the DAG-ready structures that already exist everywhere downstream; each phase independently testable; fixes the latent mis-compilation (B3) instead of leaving it as a trap; produces exactly the compiled shape that runtime attachment will later splice into; groundwork for upstream TODO #421.
- *Cons:* largest touched surface of the three options (3 compiler sites + 1 runtime site); does not touch the optimizer, so DAGs remain a post-optimizer-only concept for now.

**Option 2 — Duplicate trees with preserved operator ids, re-merge in the compiler.**
Keep the logical plan a strict forest; encode sharing as duplicated subtrees carrying the same `OperatorId`s, and let a memoizing lowering re-merge them.
- *Pros (hypothetical):* optimizer rules would keep operating on plain trees; no DAG-safety audit.
- *Cons — **not viable**:* id preservation does not survive any rebuild — `withChildren` always assigns fresh ids and the id-preserving constructor is private/friend-only (`LogicalOperator.hpp:158-161,326,364`). Even if it worked, a rule rewriting only one copy would make same-id nodes diverge semantically, and the merge would silently pick one. Rejected.

**Option 3 — Compiled-plan splicing (no compiler changes).**
Compile the main query and the tap branch as separate plans; before start, push the branch's entry pipeline into the target pipeline's `successors` vector and merge the plans.
- *Pros:* smallest diff; zero risk to existing compiler/optimizer; the splice is the closest rehearsal of the eventual runtime-attach operation.
- *Cons:* still needs the B4 `instantiate` fix anyway; the branch's scan must **byte-match** the target pipeline's emit layout with nothing validating it — a mismatch is silent memory corruption; leaves B3's mis-compilation latent; not expressible from SQL; bypasses optimizer/placement for the combined plan.
- *Verdict:* rejected as the primary design, but worth doing later as an experiment **on top of** this PoC's e2e harness, since it prototypes the runtime-attach contract cheaply.

## Structural findings (blockers we route around, not fix, in this PoC)

- **S1 — ~~The optimizer destroys DAGs~~ — RESOLVED for the rule pipeline (2026-07-12, Option A).** Root cause was that every `withChildren`/`withInferredSchema` rebuild assigns *fresh* `OperatorId`s (`LogicalOperator.hpp:158-161,326`), so any per-root rewriting rule split a shared subtree into independent copies. All rules now run on the memoizing `transformPlan` framework, which preserves sharing by instance identity (ids may still regenerate — harmless). The single-root precondition in `DecideMemoryLayoutRule`/`DecideJoinTypesRule` is gone; `OriginIdInferenceRule` assigns one origin set per shared source.
  **Remaining:** operator **placement** (`BottomUpPlacement`) is still single-root — `OperatorPlacer::place` now has a loud precondition. Post-optimizer DAG construction (the original PoC path) also still works and remains the natural fit for attaching statistic queries to already-optimized plans.

- **S2 — SQL `INTO sinkA, sinkB` stays out of scope.** The parser keeps only the first sink (`AntlrSQLQueryPlanCreator.cpp:87-101`, upstream TODO #421). Making it work requires S1 to be solved properly (id-preserving rebuilds or DAG-aware rules) — recommend as a separate follow-up PR.

- **S3 — Generic plan utilities are not DAG-safe.** `BFSIterator` has no visited set (shared nodes visited N times); `replaceOperator`/`replaceSubtree` break sharing. Not on the PoC's critical path (we construct, never rewrite, the DAG), but any future code touching DAG plans must use the DAG-safe utilities (`flatten`, `getLeafOperators`) or be audited.

## Optimizer DAG support: implementation options

The optimizer is the one layer that still cannot handle DAGs (see S1). Since every rule hand-rolls the same
naive recursion (`getChildren() | transform(apply) | withChildren(...)`), the node-local rule logic is cleanly
separable from the traversal that breaks sharing. Three ways to fix it:

**Option A — DAG-aware transform framework _(chosen, implemented on this branch)_.**
One central helper (`transformPlan`) does a memoized post-order rebuild: every *unique* operator is transformed
exactly once (memo keyed on instance identity, not id — ids regenerate), and ALL parents re-link to the single
transformed instance. Rules become node-local callbacks `(op, transformedChildren) → op'`.
- *Pros:* sharing becomes a framework invariant instead of per-rule discipline; fresh ids per rebuild become
  harmless (sharing rides on instance identity); rules get simpler; `OriginIdInference`'s DAG bug disappears by
  construction (shared source visited once → one origin set for all branches — the correct fan-out semantic).
- *Cons:* all rules must be ported; the callback API must support 1→1, bypass (1→0), and subtree (1→N,
  `LogicalSourceExpansion`) rewrites.

**Option B — Id-stable rebuilds + canonicalization.**
Make `withChildren`/`withInferredSchema` preserve ids (explicit `clone()` for intentional copies), re-merge
same-id instances after each rule.
- *Pros:* rules keep their naive recursion.
- *Cons:* sharing recovered heuristically, not preserved; a rule that legitimately rewrites one path produces
  same-id-diverged nodes detectable only late; shared subtrees still rebuilt once per parent; transient duplicate
  ids mid-pass; every caller wanting a fresh-id copy must be audited. Note: exposing id-*stable* rebuilds is still
  independently useful for runtime attachment (correlating operators across plan versions) and composes with A.

**Option C — Mutable graph IR for the optimizer.**
Convert to a mutable node graph (parent + child edges) at optimizer entry, mutate in place, convert back.
Precedent: the physical layer already works this way (`PhysicalOperatorWrapper` graph, in-place `flip`).
- *Pros:* most natural for graph rewrites; parent pointers simplify rules; no rebuild cost.
- *Cons:* second IR + conversions; abandons the logical layer's immutable-value design — an architecture-level
  decision, not a branch decision.

## Risks / known PoC limitations

- **R1 — Backpressure covers only sink #1.** `BackpressureController` is move-only and owned by exactly one sink by design (`BackpressureChannel.hpp:27`). In the PoC a slow second sink exhausts the buffer pool instead of throttling the source — fine for a statistics tap (which should be lightweight), dangerous for two full-weight sinks. Proper fix (multi-controller/counting backpressure channel) is follow-up work.
- **R2 — ~~Fan-out is invisible to the optimizer~~ — mostly lifted (2026-07-12).** DAG plans now pass through the full rule pipeline (binding, inference, layout, join-type decisions see both branches). Still invisible to operator *placement* (single-root, guarded) — relevant only for the distributed path.
- **R3 — Mixed sink formats need per-branch formatting pipelines** (part of B3). If missed, two sinks with different formats would share one emit — data corruption. Covered by dedicated unit tests.
- **R4 — Non-NATIVE source formats parse once per branch** when fanning out directly at the source. Correct but wasteful; acceptable for the PoC. Fix proposed: parse-once pipelines (see "Where can a tap go?" section) — split the parsing scan into its own pipeline whenever a non-native source has ≥ 2 consumers.
- **R5 — `explain`/PlanRenderer** prints a shared subtree once per root. Cosmetic only.
- **R6 — Untested interaction surface.** Fan-in (union/join) is well-tested; fan-out reuses the same mechanisms mirrored, but combinations (fan-out *into* a join branch, fan-out of a window pipeline) have zero existing coverage. The PoC tests cover the basic shapes; anything fancier should be treated as unvalidated.

## Step 2 investigation: inserting operators into RUNNING queries (2026-07-12)

> **UPDATE (2026-07-12, later): Option 1 (live splice) is IMPLEMENTED and passing.**
> - `RunningQueryPlanNode::successors` is now `folly::Synchronized` (readers snapshot via `copy()` on the emit
>   path); `AttachQueryTask`/`DetachQueryTask` engine tasks + `QueryEngine::attach/detach` +
>   `NodeEngine::attachToQuery/detachFromQuery`. The branch is compiled as a normal plan whose single
>   placeholder source is never started; its pipelines are set up asynchronously and wired into the target's
>   successors only after ALL setups completed. Attached nodes share the query's id, participate in the
>   termination cascade, and **detach = removing the successor edge** (the branch then terminates through the
>   existing reference-counting cascade).
> - Tests: `QueryEngineTest.AttachAndDetachBranchOnRunningQuery` + `AttachedBranchTerminatesWithQuery`
>   (engine-level, deterministic), and `AttachTapE2ETest` (full stack: FIFO-driven compiled query; a tap
>   attached mid-stream receives EXACTLY the stream suffix that flowed while it was attached, main sinks
>   receive everything, detach severs cleanly).
> - **New discovery — "tappability" depends on pipeline shape:** with a single non-native sink, the pipelining
>   phase fuses the output-formatting emit INTO the last operator pipeline, so that pipeline emits formatted
>   bytes — attaching a native-scan tap there reads garbage. At a **fan-out point** the shared pipeline is
>   closed with a NATIVE emit by construction (the step-1 work), which is exactly what a tap needs. Rule of
>   thumb: tap pipelines that end in a default (native) emit; multi-sink/fan-out plans provide those naturally.
> - Not yet done: OperatorId→PipelineId provenance map (callers currently identify the target pipeline from
>   the CompiledQueryPlan before registering it), gRPC/SingleNodeWorker exposure of attach/detach, and the
>   Option 2 re-sequencing boundary for stateful taps.

### Where can a tap go? Granularity limits of the live splice

Attach hooks into **existing pipeline boundaries** — the only places where buffers materialize. Inside a
pipeline, fused operators pass values through registers in compiled code; there is no TupleBuffer between
`filter` and `map` in `[scan|filter|map|emit]` that anyone could observe. Two distinct cases:

**Case 1 — the boundary exists but carries the wrong representation (mild, SOLVED).** With a single
non-native sink, the pipelining phase fuses the output-formatting emit into the last operator pipeline, so
its boundary carries formatted bytes and a native tap reads garbage (this is how the e2e test failed first).
Fixed by the **`tappable_pipelines` compilation flag** (`QueryExecutionConfiguration`, default off): every
operator pipeline is closed with a native emit and each non-native sink formats in its own pipeline — the
shape fan-out points produce anyway. Cost: one extra pipeline hop (scan + copy) per non-native sink.
"Keep this query tappable" is a cheap submission-time decision; covered by
`PipeliningPhaseFanOutTest.TappableMode*` and `AttachTapE2ETest.TappableCompilationMakesSingleSinkQueryTappable`
(a single-sink query, normally untappable, accepts a tap when compiled with the flag).

**Case 2 — the desired location is mid-pipeline (hard, OPEN).** Observing (or inserting) between two FUSED
operators requires a boundary that does not exist. Creating one at runtime means breaking up a running
pipeline: recompile the stage as two pipelines, drain its in-flight tasks (the `pendingTasks` counter gives
the signal), swap the node's `ExecutablePipelineStage`, and migrate any operator state inside it. No such
hot-swap machinery exists in the engine (verified — there is no interpreter→compiled swap either); this is a
substantially larger project than the successor-edge splice and stays out of scope.

**Eager vs lazy boundary creation — how `tappable_pipelines` relates to runtime recompilation.** The two
are the same idea executed at different times: the flag creates boundaries EAGERLY at compile time; runtime
pipeline splitting would create them LAZILY at attach time. A full recompilation capability strictly
subsumes the flag (it could also split *between fused operators*, i.e. solve Case 2, and it works on any
running query regardless of how it was compiled — no submission-time opt-in needed). But they pay very
different bills:

| | `tappable_pipelines` (eager) | runtime pipeline splitting (lazy) |
|---|---|---|
| steady-state cost | one extra pipeline hop per non-native sink, ALWAYS, tapped or not | zero until the first tap (optimal fused plan) |
| attach cost | unchanged: swap a successor vector, no perturbation | recompile stage (Nautilus/MLIR, seconds), quiesce/drain the pipeline (latency spike in the main query), swap stage, migrate operator-handler state, rollback story |
| after the first tap | (same hop as before) | the SAME extra hop the flag would have imposed, just deferred — un-splitting on detach = more machinery |
| implementation | ~a policy branch in the pipelining phase (done) | the largest deferred item; nothing to build on (no swap precedent in the engine) |
| covers Case 2 | no | yes |
| retrofit running queries | no (submission-time decision) | yes |

If runtime splitting is ever built (for Case 2, or for adaptive redeployment where stage hot-swap is wanted
anyway), the flag becomes redundant for the sink-formatting case — or survives as a "low-latency-attach"
mode: eager boundaries make attach instantaneous, lazy ones make the first attach expensive and briefly
disruptive to the main query.

**In-line insertion (middle ground).** The current attach adds a *parallel observer*; inserting an operator
INTO the main data path at an existing boundary (new node takes over the target's successors) would be a
small extension of the same edge mechanics, restricted to stateless, schema-preserving operators.
Mid-pipeline in-line insertion collapses into Case 2.

**What about the source boundary?** The source→pipeline edge is also a boundary, but with three caveats:
1. *Runtime attach cannot reach it:* the source is not a `RunningQueryPlanNode` — `RunningSource` captures
   its successor list BY VALUE inside the emit closure at query start (`RunningSource.cpp:53,62`). Making it
   attachable needs the same COW treatment the node successors got (deferred: "source closure rework").
2. *For non-native sources the data there is RAW bytes*, not native tuples (buffers may start/end
   mid-tuple). A tap would need its own parsing scan, and a LATE-attached parser starts mid-stream with the
   head of its first spanning tuple missing — the SequenceShredder's behavior in that case is unanalyzed.
3. *Statically it works today* (source fan-out compiles fine), but every branch parses the raw stream
   independently — that is risk R4.

**Proposed: parse-once pipelines (fixes R4 + the best tap point; NOT yet implemented).** Split the parsing
scan of a non-native source into its own pipeline instead of fusing it into the first consumer:

```
today (source fan-out):                      proposed (parse-once):
[src] ─┬─> [scan(parse)|A…]                  [src] ─> [scan(parse)|nativeEmit] ─┬─> [scan|A…]
       └─> [scan(parse)|B…]                                                     └─> [scan|B…]
   raw bytes, parsed once PER BRANCH             parsed ONCE; boundary carries native tuples
```

- At a static source fan-out point this is essentially an unconditional win: N full parses become one parse
  plus one buffer hop — so it should apply whenever a non-native source has ≥ 2 consumers, no flag needed.
- For a SINGLE consumer it is the usual eager-boundary trade (extra hop for observability) → gate it behind
  `tappable_pipelines`, mirroring the sink-side split.
- The parse boundary is the most natural tap point in the plan: the COMPLETE input stream as native tuples,
  on a regular pipeline node that runtime attach can target TODAY (unlike the source itself), with no
  shredder mid-stream hazard (the tap consumes post-shredder buffers).
- Native sources have no parse step — for them the full-input boundary remains the source edge, i.e. the
  closure rework above; a parse-split does not apply.
- Footnote: consumers then share ONE SequenceShredder / parsed-sequence domain instead of N independent
  ones — identical results, simpler metadata flow; deserves its own shape test.
- Note that parse-once already holds *incidentally* whenever sharing begins at an operator (the parse fuses
  into the head of the shared pipeline); the proposal only changes plans whose sharing begins at the source.

For statistics taps, boundaries are usually sufficient: sources, pipeline breakers (windows, joins) and
fan-out points all materialize native buffers, and `tappable_pipelines` manufactures boundaries everywhere
else at submission time.

### The sequence-number question: stateless taps are SAFE (verified in code)

A late-attached branch sees buffers starting mid-stream (first SequenceNumber ≫ 1). We traced **every**
consumer of buffer sequence metadata:

| Consumer | Behavior | Late-attach safe? |
|----------|----------|--------------------|
| `ScanPhysicalOperator` | copies seq/chunk/origin into the execution context, no validation (`ScanPhysicalOperator.cpp:68-70`) | ✅ |
| `Selection`/`Map`/`Projection` | stateless per-record, metadata passes through | ✅ |
| `EmitOperatorHandler` | chunk state keyed by *(SequenceNumber, OriginId)* pair — first-seen seq 1000 is just a new map key (`EmitOperatorHandler.cpp:29-83`) | ✅ |
| `FileSink` / `NetworkSink` | write in arrival order, no reordering, no contiguity checks (`FileSink.cpp:99-119`) | ✅ |
| InputFormatter / SequenceShredder | raw-source path only; a tap scans NATIVE buffers and bypasses it | ✅ (n/a) |
| `MultiOriginWatermarkProcessor` → `NonBlockingMonotonicSeqQueue` | watermark advances only when EVERY seq from the start is present (`NonBlockingMonotonicSeqQueue.hpp:91-95`) | ❌ |
| All `WindowBasedOperatorHandler`s (aggregation, hash/NL join), watermark assigners | built on the watermark processor | ❌ — windows never fire |

**Verdict: a stateless-only tap (scan → selection/map/projection → emit → sink) sidesteps the
sequence-number problem entirely.** No hidden contiguity assumptions were found in the buffer manager,
sinks, or scan path. Two caveats:
1. "Throughput counting" as a windowed `COUNT` **is stateful** — it cannot be late-attached directly. Escape
   hatches: engine statistics events (Option 0 below), a custom stateless counting operator (no watermarks),
   or the re-sequencing boundary (Option 2 below).
2. The tap must scan the target pipeline's **native** emit (tap before formatting pipelines).

### Engine facts that shape any attach design (verified)

- `RunningQueryPlanNode::successors` is a plain vector read lock-free by all worker threads on the hot emit
  path (`QueryEngine.cpp:496-507`) — effectively immutable after start. Runtime mutation needs
  **copy-on-write** (atomic `shared_ptr<const vector>`) or equivalent; a mutex on the emit path is the wrong
  trade for a read-heavy workload.
- `RunningSource` freezes its successor list **by value inside the emit closure** (`RunningSource.cpp:53,62`)
  → source-level attach needs closure rework; pipeline-level attach does not.
- Lifecycle composes for free: an attached node is kept alive by its target (forward reference counting);
  the existing termination cascade + `pendingTasks` accounting cover it; **detach = remove the edge** →
  refcount drop → `PendingPipelineStop` cascade cleans the branch up gracefully.
- **Attached nodes must carry the SAME QueryId** — stop/failure/statistics paths key on the task's queryId;
  a cross-query successor corrupts both queries' lifecycles.
- Pipeline setup is per-pipeline (`StartPipelineTask`) → a branch can be set up and started before wiring.
- Buffer pool is engine-global → attached branches need no memory plumbing.
- Provenance gap: the OperatorId→PipelineId mapping is lost at `ExecutableQueryPlan::instantiate` — a
  "tap the output of operator X" API needs this map retained.
- **No adaptive/hot-swap machinery exists** to reuse (no interpreter→compiled swap, no plan versioning);
  the task-variant pattern (`Task.hpp:196-204`) is the extension point for an `AttachPipelineTask`.
  gRPC has zero runtime-mutation RPCs.

### Options for inserting operators into running queries

**Option 0 — Don't insert: consume engine statistics events.** `TaskEmit` (from-pipeline, to-pipeline,
tuple count) and `TaskExecutionStart` (tuple count) are already emitted per task
(`QueryEngineStatisticListener.hpp`) — per-pipeline throughput is fully derivable today from a statistics
listener, no plan modification at all.
- *Pros:* zero engine risk, exists now, fits the Prometheus/statistics-listener work.
- *Cons:* engine-level metrics only — no data-dependent statistics (per-key distributions, content-based
  filters); listeners are fixed at engine construction; not expressible as queries.

**Option 1 — Live splice of a stateless branch (recommended PoC).** Compile a tap branch against the target
pipeline's output schema (native scan; the fan-out compiler work provides everything); make `successors`
copy-on-write; add an `AttachPipelineTask` (setup branch stages, then swap the successor vector) and a
`QueryEngine::attach/detach` API + provenance map; same queryId as the target.
- *Pros:* true runtime insertion with the smallest new surface; detach falls out of the existing termination
  cascade; directly demonstrates the statistic-tap principle; stateless safety is verified above.
- *Cons:* stateless branches only; no backpressure for the branch (R1); tap is lifecycle-subordinate to the
  main query (detach ≠ independent stop).

**Option 2 — Live splice behind a re-sequencing boundary (the general fix).** Same as Option 1, but the
attach point re-stamps buffers: fresh OriginId + sequence numbers counted from 1 per attachment, watermark
timestamps preserved. To the branch this looks like a brand-new source — **watermark machinery works, so
windowed/stateful operators (real statistic queries) become attachable**.
- *Pros:* lifts the stateless restriction cleanly and locally; the sequence-number problem is solved at the
  boundary instead of inside every operator.
- *Cons:* the re-stamper is a new component (mini-source semantics: origin registration, watermark
  progression, chunk handling); more design work than Option 1.

**Option 3 — Independent tap query via a tap port/channel.** Give queries a (static, submit-time) internal
tap sink/channel; statistic queries run as fully independent queries (own QueryId, own lifecycle) whose
source reads the channel and assigns fresh sequence numbers — windows work naturally. Runtime "attachment"
becomes channel subscription, no engine-graph mutation at all.
- *Pros:* full lifecycle decoupling; tap queries are ordinary queries (whole operator library, own
  backpressure); no concurrent-mutation problem; matches the prior FIFO-based StatisticCoordinator pattern.
- *Cons:* channel source/sink pair is new infrastructure; the port must be planned at submit time (or added
  via Option 1 once); extra buffer copy across the channel.

**Option 4 — Stop, extend, restart (works TODAY).** Use the static fan-out: stop the query, splice a new
sink root into the retained optimized plan (post-optimizer DAG edit), recompile, restart.
- *Pros:* zero engine changes — functional on this branch right now; all operator types work (fresh
  sequence numbers after restart).
- *Cons:* downtime; loses operator state and stream position (file sources re-read; window state gone);
  not "runtime" modification — a stopgap/demo only.

### Recommended path
1. Throughput metrics *now*: Option 0 (statistics listener).
2. PoC for genuine runtime insertion: **Option 1** — COW successors + `AttachPipelineTask` +
   `attach/detach` API + OperatorId→PipelineId provenance; e2e test: start query, attach a file-sink tap
   mid-stream, assert the tap receives the stream suffix, detach, assert clean branch shutdown, main query
   unaffected.
3. Lift the stateless restriction with the Option 2 re-sequencing boundary when real windowed statistic
   queries are needed; consider Option 3 if independent tap lifecycles become a requirement.

## Open ends (decisions & work beyond this branch)

1. **Runtime plan modification (the actual goal, step 2) — now INVESTIGATED, see the "Step 2 investigation" section above.** Summary: stateless taps verified safe against the sequence-number problem; recommended PoC is the live splice (Option 1: COW successors + AttachPipelineTask); the re-sequencing boundary (Option 2) is the general fix for stateful branches; detach falls out of the existing termination cascade.
2. **Multi-controller backpressure channel** (lifts R1).
3. **~~Optimizer DAG-safety~~ — DONE (2026-07-12, Option A / `transformPlan`).** Remaining sub-items: DAG-aware operator *placement* (`BottomUpPlacement` is single-root, guarded), and optionally id-*stable* rebuilds (exposing the private id-preserving constructor) — no longer needed for correctness, but useful for correlating operators across plan versions when runtime attachment lands.
4. **SQL multi-sink syntax `INTO a, b`** (TODO #421) — parser part is easy; the rule pipeline now supports it. Only blocked on multi-root placement (3) for the distributed path.
5. **Systest support for multi-sink queries** — result checking is one-file-per-query; needed if multi-sink becomes user-facing.
6. **DAG-safe plan utilities audit** (S3) — `BFSIterator`, `replaceOperator`, `replaceSubtree` before any code rewrites DAG plans.

## Implementation order

| Phase | What | Where |
|-------|------|-------|
| 1 | Lowering memoization + all sink roots (B1) | `LowerToPhysicalOperators.cpp` |
| 2 | Multi-root flip (B2) + unit test | `PhysicalPlanBuilder.cpp` |
| 3 | Pipelining fan-out points (B3) + unit tests | `PipeliningPhase.cpp` |
| 4 | Multi-sink instantiate (B4) | `ExecutableQueryPlan.cpp` |
| 5 | Two-sink DAG construction + compiler integration test | `nes-query-compiler/tests` |
| 6 | E2E: two file sinks receive identical data (B5) | new `nes-single-node-worker/tests` |
