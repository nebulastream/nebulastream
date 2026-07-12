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
- **R4 — Non-NATIVE source formats parse once per branch** when fanning out directly at the source. Correct but wasteful; acceptable for the PoC.
- **R5 — `explain`/PlanRenderer** prints a shared subtree once per root. Cosmetic only.
- **R6 — Untested interaction surface.** Fan-in (union/join) is well-tested; fan-out reuses the same mechanisms mirrored, but combinations (fan-out *into* a join branch, fan-out of a window pipeline) have zero existing coverage. The PoC tests cover the basic shapes; anything fancier should be treated as unvalidated.

## Open ends (decisions & work beyond this branch)

1. **Runtime plan modification (the actual goal, step 2).** Deferred, and the hard problems are mapped:
   - *Sequence numbers:* windowed operators in a late-attached branch expect sequence numbers from stream start; a mid-stream attach violates the monotonic-sequence invariant (`NonBlockingMonotonicSeqQueue.hpp:91-95`) and the watermark processor's fixed origin set (`MultiOriginWatermarkProcessor.hpp:31`). Candidate approaches to discuss: per-attachment sequence translation (offset re-basing at the splice point), synthetic "catch-up" watermarks, or restricting attached branches to operators without sequence/window state (sufficient for plain throughput counters!).
   - *Engine API:* `QueryEngine` exposes only `start`/`stop`; mutating a `RunningQueryPlan`'s successor vectors concurrently with execution needs a designed synchronization point (e.g. a task-queue-serialized "attach" task).
   - *Lifecycle:* attached branches must join the termination cascade and reference-counting scheme correctly; detach is a whole further question.
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
