# Grouped clang-tidy issues and suggested resolutions

## Scope and how to use this list

This is the contextual review catalog and implementation record. **Authorized changes through issue 42 are implemented and validated with the selected coherent build/test/lint runs below, subject to the explicit exceptions below.** Issue 14 remains partial at verified-safe sites. Issues 2 and 3 remain outstanding; issues 16, 30, and 43 are ignored/deferred without new suppressions. Issues 44 and 45 remain unmodified tooling/coverage concerns. Issue 4's owning APIs remain owning; this catalog does not claim every warning in that category is resolved.

**Outstanding and not resolved: issues 2 and 3.** Their requested independent reviews were blocked by the provider and produced no findings. The lifetime analysis remains incomplete; neither issue is accepted as safe or resolved, and no new suppressions are retained for either issue.

This catalog covers the previous run's 941 diagnostic records, 208 file/check groups, all 47 reported checks, and the three remaining formatting-blocker families. Diagnostic counts are not bug counts: repeated template instantiations, overlapping checks, and intentional contract tests account for many reports.

The **45 numbers below are new, contextual issue IDs**, replacing the unwieldy file/check numbering. Identical findings in the same context are collapsed. Different contexts needing different fixes remain separate, or are explicitly distinguished as subcases. Locations are representative rather than an enumeration of every repeated occurrence. Original example locations come from the pre-cleanup diagnostic snapshot and may shift as code changes; selected-resolution notes describe the current implementation. Historical diagnostic totals are not evidence of present lint cleanliness.

- **Fix:** a local cleanup or a clearer expression of an existing invariant.
- **Preserve + justify:** the code expresses an intentional contract; a narrow documented suppression may be the right resolution.
- **Review:** establish the lifetime, failure policy, or compatibility requirement before changing code.

Keep owning coroutine parameters, occurrence identity, fixed retry policy, plugin/interface conventions, expected operational errors, and full test coverage. No blanket conversion of parameters to references, methods to static, or container accesses to throwing `.at()` is proposed.

The full original locations remain in `/tmp/clang-tidy-loop-y_ghto42/remaining-warnings.md`; raw records and old IDs are in the adjacent `remaining-diagnostics.json` and `remaining-groups.json` files.

## A. Lifetimes, ownership, and exception boundaries

### 1. Potential exceptions during source destruction — option A applied

**Check:** `bugprone-exception-escape`.

**Status:** Accepted the existing fail-fast destructor policy. Added a local rationale and a suppression for this check only; exception specifications, drain behavior, and channel-close behavior are unchanged.

**Location:** [nes-plugins/Async/AsyncSource.cpp:163](nes-plugins/Async/AsyncSource.cpp#L163); shutdown implementation at [nes-plugins/Async/AsyncSource.cpp:494](nes-plugins/Async/AsyncSource.cpp#L494).

The destructor drains work and closes the channel. Drain uses posting, synchronization, and a promise; simply declaring the destructor `noexcept` does not make those operations non-throwing. Conversely, catching an error and continuing destruction without completing the drain can invalidate borrowed source/buffer state.

**Selected resolution (A):** document that failure to establish quiescence during destruction is fatal, and narrowly suppress `bugprone-exception-escape` on the destructor. The existing implicit non-throwing destructor contract remains in place. No catch-and-ignore, new `noexcept` specification, repeated-drain optimization, or allocation-free shutdown redesign was added.

**Alternative:** move fallible setup/allocation out of destruction and retain a pre-established completion mechanism. A destructor-boundary handler is only appropriate if it can still guarantee quiescence; do not swallow failure and destroy live state. Do not introduce exceptions as routine cancellation control flow.

### 2. Borrowed coroutine parameters with an existing lifetime contract — outstanding / not resolved

**Check:** `cppcoreguidelines-avoid-reference-coroutine-parameters`.

**Status:** Outstanding. The independent Sol 6.2 review did not complete because of a provider-side block. The earlier issue-2 annotations were reverted; no fix or suppression is applied. The borrowing contracts below still require the requested full caller/lifetime review.

**Locations:** [nes-plugins/Async/AsyncSource.cpp:311](nes-plugins/Async/AsyncSource.cpp#L311), [nes-sources/src/AsyncOperator.cpp:28](nes-sources/src/AsyncOperator.cpp#L28).

These are not equivalent to accidentally borrowing a temporary. `processRow` borrows an in-flight deque element retained until completion, and the source drains before destruction. The explicit-context `invokeOnce` overload borrows a noncopyable execution context; its owning convenience overload exists separately. The contracts are documented in [nes-plugins/Async/AsyncSource.hpp:138](nes-plugins/Async/AsyncSource.hpp#L138) and [nes-sources/include/Sources/AsyncOperator.hpp:73](nes-sources/include/Sources/AsyncOperator.hpp#L73).

**Proposed handling, not applied:** first complete the caller/lifetime review. If it validates the existing borrowing contracts, retain the references, document the precise lifetime argument, and consider narrow `/// NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)` annotations. Keep lifecycle/cancellation tests proving the borrowers finish first. Do not add dynamic ownership tracking merely to silence the check.

**Alternative:** if the lifetime model later changes, pass an owning stable handle, not a copied buffer/context. Copying a row buffer or changing execution-state ownership merely to satisfy this check would alter the design.

### 3. Capturing coroutine lambdas in tests — outstanding / not resolved

**Check:** `cppcoreguidelines-avoid-capturing-lambda-coroutines`.

**Status:** Outstanding. The independent Sol 6.2 review did not complete because of a provider-side block. No issue-3 fix or suppression has been applied. The retention categories below are review starting points, not a completed safety verdict.

**Representative locations:** [nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp:113](nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp#L113), [nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp:141](nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp#L141), [nes-plugins/Functions/AsyncDelay/AsyncDelayFunctionTest.cpp:215](nes-plugins/Functions/AsyncDelay/AsyncDelayFunctionTest.cpp#L215), [nes-plugins/Async/tests/AsyncOccurrenceTest.cpp:480](nes-plugins/Async/tests/AsyncOccurrenceTest.cpp#L480).

There are three recurring contexts, not forty independent design problems:

- **Synchronous `run(Body body)` helper:** the callable remains alive until the event loop and future finish. Retain that ownership and narrowly annotate coroutine lambdas after verifying their reference captures also remain alive.
- **Named local work lambda:** the local closure remains in scope through `io.run()` and `future.get()`. Keep the named owner; annotate with that explanation, or use a named/captureless coroutine with explicit parameters.
- **Factory/invocation stored by an execution owner:** confirm the callable is retained throughout invocation and draining. For shared results, [nes-plugins/Async/SharedAsyncResult.cpp:33](nes-plugins/Async/SharedAsyncResult.cpp#L33) already owns the factory in its producer coroutine frame.

**Recommendation:** use precise suppressions for verified retained closures; refactor only an actual unretained closure. A captureless coroutine taking owned parameters is a valid alternative. Do not replace the suites with smoke tests or assume `co_spawn` owns a closure when it was only given an awaitable produced by calling that closure.

### 4. Value parameters that encode ownership or interface conventions — preserve

**Check:** the ownership-sensitive subset of `performance-unnecessary-value-param`.

**Examples:** [nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp:113](nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp#L113), [nes-plugins/Functions/HttpCall/HttpCallResolverTest.cpp:107](nes-plugins/Functions/HttpCall/HttpCallResolverTest.cpp#L107), [nes-plugins/Functions/AsyncDelay/AsyncDelayAsyncOperator.cpp:145](nes-plugins/Functions/AsyncDelay/AsyncDelayAsyncOperator.cpp#L145), [nes-plugins/Async/AsyncGraphExecution.cpp:66](nes-plugins/Async/AsyncGraphExecution.cpp#L66), [nes-runtime/src/Runtime/NodeEngine.cpp:73](nes-runtime/src/Runtime/NodeEngine.cpp#L73).

**Recommendation:** retain values when the coroutine frame, completion handler, test runner, plugin preparation API, or consuming deployment boundary is meant to own the object. This includes owning callables/handlers, `SharedRuntimeValues`, fixed plugin factory/preparation signatures, and intentional consuming collections. Completion-handler `exception_ptr` values and small `stop_token` values should not be rewritten reflexively either.

Use an explanatory local suppression where the ownership/interface requirement is not visible to the checker. Ordinary synchronous read-only helper copies are a different case, covered by issue 28. Changing an override/concept signature or consumption contract needs an explicit API decision, not an automatic lint edit.

### 5. Analyzer reports a second move in the recursive lifting loop — false positive annotated

**Check:** `clang-analyzer-cplusplus.Move`.

**Location:** [nes-query-optimizer/src/Rules/Static/AsyncFunctionLiftingRule.cpp:405](nes-query-optimizer/src/Rules/Static/AsyncFunctionLiftingRule.cpp#L405).

**Investigation:** an independent Sol 6.2 review at max effort reproduced the LLVM 22.1.8 diagnostic and inspected its detailed analyzer path. That path loops back through the initializer before reporting a second move: each iteration creates a new wrapper lifetime, although the analyzer retains moved-state information for the abstract stack region. `materialize` takes a const reference and does not consume the wrapper; only the other conditional arm moves it. Sharing an immutable model does not share the local wrapper's moved-from state. No real misuse was found.

**Selected resolution:** added `NOLINTNEXTLINE(clang-analyzer-cplusplus.Move)` only at the reported `push_back` statement, explaining that each iteration constructs a fresh wrapper. No branches, arguments, occurrence tracking, factory behavior, or execution semantics were changed.

**Review validation:** the subagent ran the focused analyzer against the source and passed 13 occurrence tests, three planner tests, and five offline Bats cases using existing executables. An additional conditional-rejection probe produced the expected diagnostic but timed out during REPL shutdown; that probe is not counted as a clean end-to-end pass. Detailed evidence is in `/tmp/async-lifting-move-sol62-review.md`.

### 6. Moved-from getter expectations — removed

**Checks:** `bugprone-use-after-move`, `clang-analyzer-cplusplus.Move`.

**Location:** [nes-sources/tests/SharedRuntimeValuesTest.cpp:117](nes-sources/tests/SharedRuntimeValuesTest.cpp#L117).

**Selected resolution:** removed the `empty()` and `size()` expectations on moved-from handles, including the identical `empty()` assertion in the null-value test. The payload test still copies ownership, releases the original owner, moves the retained handle, and checks preservation of small, large, and binary payloads. No suppression was added.

### 7. Trivial output-handle copy/getter check — removed

**Check:** `performance-unnecessary-copy-initialization`.

**Location:** [nes-sources/tests/AsyncCallGraphTest.cpp:181](nes-sources/tests/AsyncCallGraphTest.cpp#L181).

**Selected resolution:** removed the local copy and its getter-equality expectation, as requested. The enclosing test still checks producer lifetime after other owners disappear and rejects an invalid output selector. No suppression was added.

### 8. Deliberately incompatible pointer alias in a rejection test — annotated

**Check:** `cppcoreguidelines-pro-type-reinterpret-cast`.

**Location:** [nes-common/tests/UnitTests/Util/ReflectionTest.cpp:386](nes-common/tests/UnitTests/Util/ReflectionTest.cpp#L386).

**Selected resolution:** retained the adversarial alias and added a rationale-bearing `NOLINTNEXTLINE` for this check only. The same address with a different static pointee type must reach reflection's type-conflict rejection. Pointer ownership, the cast, and the rejection assertion are unchanged.

### 9. Prepared-call state ownership — actual shared state implemented and lifecycle-tested

**Check:** `cppcoreguidelines-avoid-const-or-ref-data-members`.

**Locations:** [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp:171](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp#L171), [nes-sources/include/Sources/PreparedAsyncCall.hpp:33](nes-sources/include/Sources/PreparedAsyncCall.hpp#L33), [nes-sources/tests/SharedRuntimeValuesTest.cpp:535](nes-sources/tests/SharedRuntimeValuesTest.cpp#L535).

- HTTP and buffer-reuse prepared calls now retain `shared_ptr` ownership of their actual slot state rather than storing a state reference.
- Context storage provides owning `getSharedOperatorState<T>()` / `getSharedSlotState<T>()` handles alongside existing borrowed access. A retained slot payload also retains its operator-state dependency; the operator payload does not retain its group, avoiding a framework ownership cycle.
- State/control-block storage uses `make_shared` once on first access, not a separate tracking token or per-attempt wrapper. Slot-local scheduling and plugin synchronization requirements are unchanged.
- Test observer references to fixture-owned counters/order records remain non-owning where their scoped lifetime is intentional.
- `retryOverride` is deliberately fixed per prepared invocation. It is now a private const member exposed by `getRetryOverride()` as `const std::optional<size_t>&`; the public-field access and implementation-detail constness assertion were removed. Runtime retry-policy tests still exercise default, zero, explicit-count, and fixed-per-invocation behavior. The remaining reference-member warnings are still proposals; no suppression was added here.

**Selected resolution:** implemented actual shared state ownership, without weak-pointer tracking or dummy-deleter handles. Added lifecycle tests for retained operator state, retained slot-to-operator dependencies across context replacement/destruction, and an HTTP prepared invocation executed after its context is destroyed. The immutable private retry override and existing scoped test observer references are not globally suppressed.

### 10. Existing exception objects at established boundaries — move-based throws applied

**Checks:** `bugprone-exception-copy-constructor-throws`, `misc-throw-by-value-catch-by-reference`.

**Locations:** [nes-plugins/Async/AsyncSource.cpp:538](nes-plugins/Async/AsyncSource.cpp#L538), [nes-query-optimizer/src/Placement/QueryDecomposition.cpp:326](nes-query-optimizer/src/Placement/QueryDecomposition.cpp#L326).

The first is the source-task reporting boundary; the second is a planner/setup boundary. They propagate an existing `Exception`, including its code, message, and lazy trace. `Exception` uses `cpptrace::lazy_exception`, as shown in [nes-common/include/ErrorHandling.hpp:59](nes-common/include/ErrorHandling.hpp#L59).

**Selected resolution:** the planner throws `std::move(value).error()`. The source keeps a non-const local failure snapshot and throws `std::move(failure->exception)` after draining. Both transfer the original exception payload/code/lazy trace instead of reconstructing an error. A temporary compile probe verified that `Exception` is nothrow-move-constructible and that both lint checks accept these non-const move forms; copying and moving from const remain diagnosed. Existing request/planner boundaries and expected operational-error flow are unchanged.

## B. Validation, container access, and test setup

### 11. Optional constants in plugin binding/inference — local references applied

**Check:** `bugprone-unchecked-optional-access`.

**Locations:** [nes-plugins/Functions/AsyncDelay/AsyncDelayAsyncOperator.cpp:118](nes-plugins/Functions/AsyncDelay/AsyncDelayAsyncOperator.cpp#L118), [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp:275](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp#L275).

The code checks an element of `constantArguments` and later re-indexes the vector to dereference it. The analyzer may not connect repeated indexing with the guard.

**Selected resolution:** bound the delay's milliseconds/concurrency constants and HTTP URL/option constants to local const references, then checked and read those same references. Arity/type/range validation order, error messages, and planner exception conventions are unchanged. No suppression was added.

### 12. Runtime optional results protected by state-machine invariants — explicit checks applied

**Check:** `bugprone-unchecked-optional-access`.

**Locations:** [nes-plugins/Async/AsyncGraphExecution.cpp:129](nes-plugins/Async/AsyncGraphExecution.cpp#L129), [nes-plugins/Async/SharedAsyncResult.cpp:144](nes-plugins/Async/SharedAsyncResult.cpp#L144), [nes-plugins/Async/AsyncSource.cpp:482](nes-plugins/Async/AsyncSource.cpp#L482), [nes-sources/src/AsyncCallGraph.cpp:113](nes-sources/src/AsyncCallGraph.cpp#L113).

Distinct contexts need distinct explanations:

- **Joined result cells:** successful completion with zero remaining cells must imply every cell has a value. Check/document that implication before moving results out.
- **Shared result:** `done` implies an engaged `outcome`; make `outcome.has_value()` explicit at the read point.
- **Published row results:** bind `buffer.results[row]` once and use the existing completed-row invariant on that same local.
- **Input field handle:** construction already validates schema membership; make that invariant explicit at lookup rather than allowing an incidental `bad_optional_access` to become the contract.

**Selected resolution:** added engagement invariants before collecting successful cell values, reading a completed shared outcome, resolving a validated input field, and throwing the operational source error when no infrastructure failure is present. Row output now binds the optional once and checks/dereferences that same binding. The existing pending-result precondition remains. No exception adapters or suppressions were added; external-input validation and expected operational errors are unchanged.

### 13. Optional access in tests — fatal guards retained and test-only suppressions applied

**Check:** `bugprone-unchecked-optional-access` in test code.

**Examples:** [nes-async/tests/HandoffChannelTest.cpp:66](nes-async/tests/HandoffChannelTest.cpp#L66), [nes-plugins/Async/tests/AsyncOccurrenceTest.cpp:290](nes-plugins/Async/tests/AsyncOccurrenceTest.cpp#L290), [nes-sources/tests/SharedRuntimeValuesTest.cpp:311](nes-sources/tests/SharedRuntimeValuesTest.cpp#L311).

**Selected resolution:** retained fatal `ASSERT_TRUE` guards, the new schema-field guard, and local references to constant optionals. Coroutine/non-void helpers retain explicit guarded control flow. After LLVM 22 still reported GoogleTest assertion-inference warnings, the user authorized suppressing the rest: added check-specific `NOLINTBEGIN`/`NOLINTEND` around the channel, occurrence, and shared-runtime-values test files. Production optional checks and unrelated checks remain enabled. This does not resolve coroutine-lifetime issues 2 or 3.

### 14. Container access advice — verified-safe sites annotated; other cases outstanding

**Check:** `cppcoreguidelines-pro-bounds-avoid-unchecked-container-access`.

**Status:** Applied narrow, rationale-bearing statement/region suppressions only after production/test audits and additional source review. No global or file-wide suppression, access-policy change, `.at()` rewrite, or unrelated-check suppression was introduced. Cases not established safe remain visible; this issue is only partially resolved.

**Examples:** [nes-sources/src/SharedRuntimeValues.cpp:46](nes-sources/src/SharedRuntimeValues.cpp#L46), [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp:185](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp#L185), [nes-common/include/Util/Reflection/SharedPtrReflection.hpp:106](nes-common/include/Util/Reflection/SharedPtrReflection.hpp#L106), [nes-sources/tests/AsyncCallGraphTest.cpp:77](nes-sources/tests/AsyncCallGraphTest.cpp#L77).

- **Validated runtime indices:** retain fast indexing behind meaningful size/index checks; use range iteration or `zip` when the index is unnecessary. Do not replace expected-error behavior with new `.at()` exceptions.
- **The checked implementation of `SharedRuntimeValues::operator[]`:** the method already checks `index < size()`. A narrow suppression on the underlying vector access is more honest than layering another access policy on this contract.
- **Lookup APIs returning optional:** handle their result; they are not necessarily vector bounds accesses. See issue 12.
- **JSON/object construction:** `object["id"] = ...` inserts a key; `.at()` on an empty object would be wrong. Prefer the object's explicit insertion API, or suppress intentional insertion. The same applies to malicious-wire test construction.
- **Tests:** assert size/presence, use `.at()` for ordinary standard-container lookup when appropriate, and preserve direct indexing when it is the API under test. Do not assume every project container has `.at()`.

**Applied scope:** validated arity/loop/slot/row indices, checked schema lookup, intentional object-property insertion, graph references validated at construction and deserialization, and selected fixed-size/assertion-backed test accesses. Private result-handle construction and explicit wire output-index validation were checked before annotating graph consumption. Test fixtures and bounded helper loops were reviewed separately rather than assumed safe because they are tests.

**Remaining:** unreviewed map/fixture assumptions and result accesses without established size guarantees were not suppressed. Whole-file analysis also exposes pre-existing diagnostics outside the branch's changed lines; those were not blanket-suppressed. Issue 13's optional-access diagnostics and issue 16's static-initialization diagnostics remain unaffected.

**Validation:** focused bounds analysis covered 18 translation units and the changed reflection/test-helper headers. All reviewed production units are clean for this check; remaining reports are in test contexts not annotated as safe. Comments/scoped lint directives are the only code-file changes for this issue. Audit evidence is in `/tmp/issue-14-production-audit.md` and `/tmp/issue-14-tests-audit.md`; fresh logs are under `/tmp/clang-tidy-loop-y_ghto42/issue-14-tidy/`.

These contexts were deliberately not collapsed into a blanket search-and-replace.

### 15. Integer promotion and iterator-offset conversion in tests — direct types applied

**Checks:** `bugprone-implicit-widening-of-multiplication-result`, `cppcoreguidelines-narrowing-conversions`.

**Locations:** [nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp:241](nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp#L241), [nes-plugins/Async/tests/AsyncSourceValueTest.cpp:307](nes-plugins/Async/tests/AsyncSourceValueTest.cpp#L307), [nes-plugins/Functions/HttpCall/HttpCallFunctionTest.cpp:153](nes-plugins/Functions/HttpCall/HttpCallFunctionTest.cpp#L153).

**Selected resolution:** both buffer-size multiplications now use `size_t` operands directly. The HTTP argument-prefix loop counter uses `std::vector<LogicalFunction>::difference_type`, matching the iterator offset type. No after-the-fact casts, size changes, or test-case changes were introduced.

### 16. Throw-capable namespace-scope test constants — deferred / ignored, not suppressed

**Check:** `bugprone-throwing-static-initialization`.

**Status:** Explicitly ignored for now at the user's request. No code changes, NOLINT annotations, or configuration exclusions were added for this issue. The diagnostics remain visible; this is not marked resolved.

**Examples:** [nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp:62](nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp#L62), [nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp:173](nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp#L173), [nes-sources/tests/AsyncHandoffLayoutTest.cpp:33](nes-sources/tests/AsyncHandoffLayoutTest.cpp#L33).

**Recommendation:** construct `DataType`, parsed identifiers, and logical constants in fixture/test scope, or expose a function-local static that is first called from test execution. Initialization exceptions can then be reported by the test framework rather than terminate before the tests start.

Do not keep a namespace-scope variable whose initializer calls the new accessor: that would still perform dynamic initialization before test execution. Avoid adding `noexcept` to constructors that can legitimately fail.

## C. Public API, plugin conventions, and type semantics

### 17. Special-member sets — all five explicitly specified and validated

**Check:** `cppcoreguidelines-special-member-functions`.

**Examples:** [nes-sources/include/Sources/AsyncCallGraph.hpp:42](nes-sources/include/Sources/AsyncCallGraph.hpp#L42), [nes-sources/include/Sources/AsyncExecutionContext.hpp:25](nes-sources/include/Sources/AsyncExecutionContext.hpp#L25), [nes-async/include/Async/HandoffChannel.hpp:31](nes-async/include/Async/HandoffChannel.hpp#L31), [nes-frontend/tests/GRPCQuerySubmissionBackendTest.cpp:87](nes-frontend/tests/GRPCQuerySubmissionBackendTest.cpp#L87).

- **Input/output reference handles:** explicitly copyable, with intentional copy-on-rvalue behavior so rvalues do not leave empty handles. Do **not** default move operations. All five special members are specified explicitly; no completeness suppression is used.
- **Stable-identity graph/execution/builder objects:** keep copy/move restrictions. Add explicit deletions only when documenting existing effective restrictions, not to enable relocation accidentally.
- **Execution context:** its custom move assignment destroys slot state before replacing operator ownership. Never replace it with a defaulted assignment. A defaulted destructor can be harmless here because move operations are already explicitly declared, but is not required for correctness.
- **RAII test fixtures/destruction counters:** explicitly prohibit unintended copying/relocation where duplicate cleanup or misleading counts would result.
- **Move-only test configuration:** state intended copy deletion and move support explicitly if useful; do not make it copyable merely to complete the set.

**Selected resolution:** specified complete copy/move/destructor sets for the affected graph/channel/context types and resource/observer test fixtures; no completeness suppressions were added. Stable-identity/resource objects remain noncopyable/nonrelocatable where required. Reference-handle move operations explicitly preserve the previous copy-on-rvalue contract, including nonempty source handles; the execution context retains its custom slot-before-operator move assignment. Newly changed erased-state and prepared-call bases also have complete explicit sets.

### 18. Interface implementations suggested as static — local suppressions applied

**Check:** `readability-convert-member-functions-to-static`.

**Examples:** [nes-logical-operators/include/Functions/AsyncCallLogicalFunction.hpp:53](nes-logical-operators/include/Functions/AsyncCallLogicalFunction.hpp#L53), [nes-plugins/Functions/AsyncDelay/AsyncDelayAsyncOperator.cpp:86](nes-plugins/Functions/AsyncDelay/AsyncDelayAsyncOperator.cpp#L86), [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp:247](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp#L247).

**Selected resolution:** retained instance interfaces and added local definition annotations or narrowly scoped mock-interface regions for `readability-convert-member-functions-to-static`. No methods were converted to static and no project-wide check configuration was disabled.

### 19. Function defaults — forwarding overloads implemented and validated

**Check:** `fuchsia-default-arguments-declarations`.

**Examples:** [nes-common/include/Util/CheckedDeadline.hpp:31](nes-common/include/Util/CheckedDeadline.hpp#L31), [nes-logical-operators/include/Operators/AsyncLogicalOperator.hpp:64](nes-logical-operators/include/Operators/AsyncLogicalOperator.hpp#L64), [nes-sources/include/Sources/PreparedAsyncCall.hpp:27](nes-sources/include/Sources/PreparedAsyncCall.hpp#L27).

**Selected resolution:** replaced the reported function defaults with forwarding/delegating overloads across runtime helpers, logical factories, and test helpers. Existing call arities and default policies remain available; omitted timestamps/occurrence UUIDs are produced once. Coroutine adapters return the owning implementation's awaitable without adding coroutine-lambda captures or an extra coroutine frame. Template type defaults are retained where needed for starter selection; no new function defaults or style suppressions were introduced.

### 20. Query/owning-return APIs — nodiscard annotations applied and validated

**Check:** `modernize-use-nodiscard`.

**Examples:** [nes-sources/include/Sources/AsyncOperator.hpp:83](nes-sources/include/Sources/AsyncOperator.hpp#L83), [nes-sources/include/Sources/AsyncOperator.hpp:157](nes-sources/include/Sources/AsyncOperator.hpp#L157), [nes-sources/tests/Util/AsyncTestOperator.hpp:45](nes-sources/tests/Util/AsyncTestOperator.hpp#L45).

**Selected resolution:** added `[[nodiscard]]` consistently to the reported query/owning-return APIs, erased/typed wrappers, functional configuration rewrites, and concrete mock implementations. Intentional result discards remain explicit; no getter-only tests were added.

**Special case:** the report on the deleted rvalue `RuntimeValue::view()` overload at [nes-data-types/include/DataTypes/RuntimeValue.hpp:196](nes-data-types/include/DataTypes/RuntimeValue.hpp#L196) is not a runtime misuse: the overload cannot be called. Annotate for declaration symmetry or suppress that isolated suggestion; do not remove the deletion that prevents dangling views.

### 21. Multiple inheritance follows logical-operator mixin conventions — annotated

**Check:** `misc-multiple-inheritance`.

**Location:** [nes-logical-operators/include/Operators/AsyncLogicalOperator.hpp:50](nes-logical-operators/include/Operators/AsyncLogicalOperator.hpp#L50).

**Selected resolution:** retained both mixin roles and added a declaration-local `NOLINTNEXTLINE(misc-multiple-inheritance)` explaining the logical-operator ordering/managed-identity convention. No inheritance or runtime behavior changed.

### 22. Runtime type-trait specialization macro — annotated

**Check:** `cppcoreguidelines-macro-usage`.

**Location:** [nes-data-types/include/DataTypes/RuntimeValue.hpp:38](nes-data-types/include/DataTypes/RuntimeValue.hpp#L38).

**Selected resolution:** retained the immediately-undefined specialization macro and added a local `NOLINTNEXTLINE(cppcoreguidelines-macro-usage)` at its definition. Native/SQL mappings and the `RuntimeScalar` constraint are unchanged.

### 23. Smaller enum underlying type — explicit uint8_t applied

**Check:** `performance-enum-size`.

**Location:** [nes-sources/include/Sources/AsyncError.hpp:25](nes-sources/include/Sources/AsyncError.hpp#L25).

**Selected resolution:** declared `AsyncError::Type : uint8_t` and added the direct `<cstdint>` include. Enumerator values/meanings remain unchanged. This is an internal runtime error classification; no duplicate runtime key or wire representation was added. Dependent code must be rebuilt coherently after the representation change.

### 24. Internal linkage and mutable test counters — namespaces and targeted annotations applied

**Checks:** `misc-use-internal-linkage`, `cppcoreguidelines-avoid-non-const-global-variables`.

**Examples:** [nes-sources/src/AsyncCallGraph.cpp:48](nes-sources/src/AsyncCallGraph.cpp#L48), [nes-plugins/Async/tests/AsyncOccurrenceTest.cpp:67](nes-plugins/Async/tests/AsyncOccurrenceTest.cpp#L67), [nes-common/tests/UnitTests/Util/ReflectionTest.cpp:295](nes-common/tests/UnitTests/Util/ReflectionTest.cpp#L295).

**Recommendation:** use anonymous namespaces for translation-unit-only wire/helper structures, free helpers, ordinary fixtures, and test counters when their lookup/registration permits it. Keep namespace closing braces plain.

Factory counters are intentionally mutable; making them const would destroy their purpose. Internalize them and retain per-test reset/isolation. If a global-variable diagnostic remains, document the test instrumentation use rather than inventing a needless accessor framework.

**Caution:** reflected/registered types and their explicit specializations must stay in legal and mutually visible namespaces. Do not wrap an entire test file containing `Reflector`/`Unreflector` specializations in an anonymous namespace. Internalize a type only with a deliberate lookup/specialization adjustment; keeping an appropriately named type with a narrow suppression is also valid.

## D. Dependencies and local code cleanup

### 25. Real missing direct includes and genuinely unused includes — verified public-provider cleanup applied

**Check:** actionable `misc-include-cleaner` findings.

**Examples:** [nes-plugins/Async/AsyncGraphExecution.cpp:49](nes-plugins/Async/AsyncGraphExecution.cpp#L49), [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.hpp:39](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.hpp#L39), [nes-sources/tests/AsyncCallGraphTest.cpp:49](nes-sources/tests/AsyncCallGraphTest.cpp#L49).

**Recommendation:** include public declarations directly where actually used; remove only headers with no direct symbol, macro, registration, or documented facade role. Use angle brackets and preserve corresponding-header-first order.

Common legitimate providers include `<variant>`, `<utility>`, `<chrono>`, `<optional>`, `<expected>`, `<cstddef>`, `<cstdint>`, and the specific public Asio/Beast headers. For NES, use the existing public headers, for example [nes-sources/include/Sources/AsyncError.hpp:23](nes-sources/include/Sources/AsyncError.hpp#L23) for both `AsyncError` and `AsyncResult`, [nes-sources/include/Sources/AsyncCallGraph.hpp:37](nes-sources/include/Sources/AsyncCallGraph.hpp#L37) for graph types, and [nes-sources/include/Sources/SharedRuntimeValues.hpp:25](nes-sources/include/Sources/SharedRuntimeValues.hpp#L25) for shared values.

Repeated diagnostics for one symbol or one missing header are one dependency fix, not separate work items. Re-analyze after each coherent header cleanup instead of applying stale replacement offsets.

### 26. Include-cleaner facade/implementation false positives — local verified-provider annotations applied

**Check:** non-actionable/provider-attribution `misc-include-cleaner` findings.

**Examples:** [nes-data-types/tests/UnitTests/RuntimeValueTest.cpp:26](nes-data-types/tests/UnitTests/RuntimeValueTest.cpp#L26), [nes-single-node-worker/src/SingleNodeWorker.cpp:43](nes-single-node-worker/src/SingleNodeWorker.cpp#L43), [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp:17](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp#L17).

These include contradictory pairs such as a public Logger/Asio header being called unused while the symbol it supplies is reported as missing. Definitions may live in an implementation header even though consumers should include the facade. Aliases and macros can confuse provider attribution too.

**Recommendation:** retain the supported public header; do not add private Boost `detail`/`impl` headers, standard-library implementation headers, or NES logger implementation headers in consumers. Where possible, model public re-exports correctly. Otherwise use a narrowly scoped include/symbol suppression or an exact documented facade entry in the existing include-cleaner configuration.

Do not globally disable include-cleaner or blanket-ignore standard/Boost headers. Only classify a warning as this case after checking the public provider is actually present; true transitive-dependency problems belong to issue 25.

**Selected resolution for 25/26:** added verified public declaration providers, removed genuinely unused headers after checking direct API/macro/formatter roles, and annotated only confirmed facade/alias/macro attribution mistakes at includes or affected declarations/statements. No private Boost implementation/detail, stdlib bits, or NES logger implementation headers were added. The initial focused run covered 17 translation units; after formatting exposed fragile trailing comments, those affected callsites were converted to balanced statement-local scopes. The final formatted-source include-cleaner run covered all 29 selected translation units with zero diagnostics/errors. This is not an overall lint-clean claim. Fresh logs and provider/removal rationale are recorded in `/tmp/annotation-stability-report.md` and `/tmp/annotation-stability-logs/`.

### 27. Const-correctness advice — eligible locals made const

**Checks:** `misc-const-correctness`, `readability-make-member-function-const`.

**Examples:** [nes-frontend/tests/DistributedPlanningTest.cpp:362](nes-frontend/tests/DistributedPlanningTest.cpp#L362), [nes-plugins/Async/AsyncSource.cpp:191](nes-plugins/Async/AsyncSource.cpp#L191), [nes-async/tests/HandoffChannelTest.cpp:153](nes-async/tests/HandoffChannelTest.cpp#L153), [nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp:339](nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp#L339).

**Recommendation:** add const to truly unchanged locals and pointer pointees where it does not inhibit a later move or change an overload. `ReflectionContext`'s generic handle can be const while injected state changes through that handle. The per-operator context used only to create sibling slot contexts can also be const.

`stop_source::request_stop()` is const-qualified, so requesting a stop alone does **not** disqualify those locals from const. The reported `jthread` local is only scope-owned in that test; const is possible there, but not if explicit join/stop/move operations are added. Check actual uses rather than assuming all stateful wrappers must be mutable.

**Selected resolution:** made the read-only context/catalog/optimizer/config/thread and fixture identity locals const where their current uses permit it. Kept the moved operational failure snapshot, moved operator configuration, return-move normalization local, and mutated output buffers non-const. Added no constness suppressions or changes to virtual override signatures. The shared-state identity pointers now have const pointees. Fixture command semantics remain explicit; the by-value `push` argument is const, while `next()` retains its mutable output buffer.

### 28. Genuinely unnecessary synchronous helper copies — const references applied

**Check:** the ordinary-helper subset of `performance-unnecessary-value-param`.

**Examples:** [nes-logical-operators/src/Functions/AsyncCallLogicalFunction.cpp:50](nes-logical-operators/src/Functions/AsyncCallLogicalFunction.cpp#L50), [nes-logical-operators/src/Operators/AsyncLogicalOperator.cpp:64](nes-logical-operators/src/Operators/AsyncLogicalOperator.cpp#L64), [nes-plugins/Functions/AsyncDelay/AsyncDelayFunctionTest.cpp:52](nes-plugins/Functions/AsyncDelay/AsyncDelayFunctionTest.cpp#L52).

**Selected resolution:** the logical async constructor/name factories and synchronous literal helpers use `const std::string&`. Forwarding overloads pass the name by reference rather than moving from const. Arguments/configuration that are actually consumed, and owning coroutine interfaces from issue 4, remain by value.

Keep public/interface signatures stable unless explicitly choosing to change them. Test-only string-building helpers are lower-risk candidates than plugin factories, callbacks, and consuming deployment APIs. Issue 4 covers parameters that should deliberately stay owning values.

### 29. Const local blocks an otherwise valid return move — const removed

**Check:** `performance-no-automatic-move`.

**Location:** [nes-query-optimizer/src/Rules/Static/AsyncFunctionLiftingRule.cpp:245](nes-query-optimizer/src/Rules/Static/AsyncFunctionLiftingRule.cpp#L245), with returns at lines 252, 306, and 327.

**Selected resolution:** `current` is now non-const, permitting automatic return moves while leaving normalization control flow unchanged. No explicit move was added to return statements. This is independent of issue 5's statement-local analyzer suppression.

### 30. Four complex functions — deferred / ignored

**Check:** `readability-function-cognitive-complexity`.

**Status:** Ignored for now at the user's request. No complexity refactor or suppression is applied; the warnings remain visible and this is not marked resolved.

- [nes-plugins/Async/AsyncGraphExecution.cpp:52](nes-plugins/Async/AsyncGraphExecution.cpp#L52): isolate completion/error recording or sibling cancellation while keeping first-error selection, cancellation shielding, and draining visible.
- [nes-plugins/Async/AsyncSource.cpp:523](nes-plugins/Async/AsyncSource.cpp#L523): separate ready-buffer emission, input admission, and waiting decisions without moving the synchronization boundaries.
- [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp:257](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp#L257): factor arity/type/constant-option validation into small named checks, retaining one clear inference diagnostic path.
- [nes-sources/src/AsyncCallGraph.cpp:329](nes-sources/src/AsyncCallGraph.cpp#L329): factor wire reference validation/materialization from traversal, preserving cycle/dangling-edge rejection and owning configuration.

**Recommendation:** treat these as four separate refactor decisions. Small local helpers are preferable to a generalized descriptor/callback framework. For an intentionally cohesive state machine, a local complexity suppression is an alternative to fragmentation that obscures correctness.

### 31. Output mappings assigned after default construction — initialized directly

**Check:** `cppcoreguidelines-prefer-member-initializer`.

**Location:** [nes-plugins/Async/AsyncSource.cpp:169](nes-plugins/Async/AsyncSource.cpp#L169).

**Selected resolution:** initialized `outputMappings(graph->getOutputs())` directly after `calls` in declaration order, before constructing the channel member. Removed the constructor-body assignment; no owner/member declaration order was changed.

### 32. Single-mutex lock style — scoped_lock applied

**Check:** `modernize-use-scoped-lock`.

**Location:** [nes-sources/include/Sources/AsyncExecutionContext.hpp:99](nes-sources/include/Sources/AsyncExecutionContext.hpp#L99).

**Selected resolution:** both borrowed and owning operator-state initialization accessors now use `std::scoped_lock`. Their mutex and scopes are unchanged; no lock is extended over plugin execution or suspension.

### 33. Nested conditional in DNS error classification — else-if chain applied

**Check:** `readability-avoid-nested-conditional-operator`.

**Location:** [nes-plugins/Functions/HttpCall/HttpCallResolver.hpp:116](nes-plugins/Functions/HttpCall/HttpCallResolver.hpp#L116).

**Selected resolution:** replaced the chained ternary with an explicit `if`/`else if` classification and a permanent-error default. Cancellation/timeout/retry classifications and cancellation-policy restoration remain unchanged.

### 34. Equivalent boolean expressions — local annotations applied

**Check:** `readability-simplify-boolean-expr`.

**Locations:** [nes-plugins/Async/AsyncGraphExecution.cpp:248](nes-plugins/Async/AsyncGraphExecution.cpp#L248), [nes-sources/src/AsyncCallGraph.cpp:222](nes-sources/src/AsyncCallGraph.cpp#L222).

**Selected resolution:** retained the output type/nullability invariant and nonempty function/signature precondition as written. Added local `NOLINTNEXTLINE(readability-simplify-boolean-expr)` annotations; no expression or evaluation order changed.

### 35. Duplicate watermark-pushdown branch — user comment preserved and annotated

**Check:** `bugprone-branch-clone`.

**Location:** [nes-query-optimizer/src/Rules/Static/WatermarkAssignerPushdownRule.cpp:273](nes-query-optimizer/src/Rules/Static/WatermarkAssignerPushdownRule.cpp#L273).

**Selected resolution:** preserved the user's improved black-box/conservative-pushdown comment verbatim and added a branch-local `NOLINTNEXTLINE(bugprone-branch-clone)`. The explicit async case and pushdown behavior are unchanged.

### 36. Short/local naming and static-constant case — descriptive names applied

**Checks:** `readability-identifier-length`, `readability-identifier-naming`.

**Examples:** [nes-plugins/Async/AsyncSource.cpp:460](nes-plugins/Async/AsyncSource.cpp#L460), [nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp:74](nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp#L74), [nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp:795](nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp#L795).

**Selected resolution:** byte spans are `inputBytes`/`outputBytes`; paired row results are `firstResult`/`secondResult`; the fallback static is `EmptyValues`; and reported plugin/occurrence test event loops consistently use `ioContext`. No project-wide abbreviation-policy change was made.

### 37. Header/definition parameter names disagree — informative names aligned

**Check:** `readability-inconsistent-declaration-parameter-name`.

**Locations:** [nes-plugins/Async/SharedAsyncResult.hpp:55](nes-plugins/Async/SharedAsyncResult.hpp#L55), [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.hpp:30](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.hpp#L30), [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.hpp:39](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.hpp#L39).

**Selected resolution:** aligned `SharedAsyncResult::complete` on `outcome`, HTTP preparation on `arguments`, and HTTP transport classification on `error`. The result member is assigned through `this->outcome`; no types, control flow, or serialized fields changed.

### 38. Fixture narrows `SetUp` visibility — made public

**Check:** `misc-override-with-different-visibility`.

**Location:** [nes-plugins/Async/tests/AsyncSourceValueTest.cpp:339](nes-plugins/Async/tests/AsyncSourceValueTest.cpp#L339).

**Selected resolution:** made the death fixture's `SetUp()` override public, matching its base. No fixture data visibility or setup/teardown order changed.

### 39. Small syntax modernizations — applied; unsafe string-brace suggestion annotated

**Checks:** `readability-uppercase-literal-suffix`, `readability-redundant-casting`, `modernize-loop-convert`, `modernize-raw-string-literal`, `modernize-return-braced-init-list`.

**Examples:** [nes-logical-operators/src/Functions/AsyncCallLogicalFunction.cpp:108](nes-logical-operators/src/Functions/AsyncCallLogicalFunction.cpp#L108), [nes-sources/tests/SharedRuntimeValuesTest.cpp:488](nes-sources/tests/SharedRuntimeValuesTest.cpp#L488), [nes-sources/tests/SharedRuntimeValuesTest.cpp:450](nes-sources/tests/SharedRuntimeValuesTest.cpp#L450), [nes-sources/tests/AsyncCallGraphTest.cpp:269](nes-sources/tests/AsyncCallGraphTest.cpp#L269), [nes-query-optimizer/src/Placement/QueryDecomposition.cpp:268](nes-query-optimizer/src/Placement/QueryDecomposition.cpp#L268).

**Selected resolution:** uppercased unsigned suffixes, removed redundant outer `uint64_t{...}` conversions while retaining the required signed-to-unsigned casts, used a range loop over sibling contexts, and replaced escaped identifier text with an identical raw string. Retained `std::string(1, '\\0')` with a local `NOLINTNEXTLINE(modernize-return-braced-init-list)`: braces select the initializer-list constructor and would change one byte into two. No literal payload or case coverage changed.

### 40. Magic numbers — authorized test/protocol suppressions applied

**Check:** `readability-magic-numbers`.

**Examples:** [nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp:83](nes-plugins/Functions/HttpCall/HttpCallAsyncOperator.cpp#L83), [nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp:241](nes-plugins/Async/tests/AsyncSourceBufferReuseTest.cpp#L241), [nes-sources/tests/SharedRuntimeValuesTest.cpp:121](nes-sources/tests/SharedRuntimeValuesTest.cpp#L121).

**Selected resolution:** at the user's request, added balanced check-specific file scopes to the 13 reported test files, with fixture-data rationale, and a narrow `readability-magic-numbers` scope around HTTP protocol status classification. No numeric values, retry/status policies, deadlines, payloads, or concurrency bounds changed. Production files are not blanket-suppressed and global check configuration is unchanged.

### 41. Using declarations — broad import removed; required operator lookup preserved

**Checks:** `misc-unused-using-decls`, `google-build-using-namespace`.

**Locations:** [nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp:61](nes-plugins/Async/tests/AsyncGraphExecutionTest.cpp#L61), [nes-plugins/Async/tests/AsyncOccurrenceTest.cpp:63](nes-plugins/Async/tests/AsyncOccurrenceTest.cpp#L63).

**Selected resolution:** removed `using namespace NES` by placing occurrence-test helpers and fixtures in an unnamed namespace within `NES`; explicit reflection specializations remain in their legal namespace. Test registration names are unchanged. A temporary syntax-only compile without the Asio `operator&&` import failed at the coroutine-combination expressions, proving the import is required. Retained that declaration with a statement-local `misc-unused-using-decls` rationale instead of breaking lookup. Probe log: `/tmp/issue-41-operator-lookup/syntax.log`.

## E. Formatting and analysis-tooling blockers

These predate the proposed issue fixes and include user work outside the async/reflection implementation. Keep them separate from production-code changes.

### 42. Missing license preambles in project-owned scripts — all eight added and checked

**Examples:** [.devcontainer/setup-pi.sh:1](.devcontainer/setup-pi.sh#L1), [.devcontainer/install-pi-skills.sh:1](.devcontainer/install-pi-skills.sh#L1), [.devcontainer/skills/nes-notifications/relay.py:1](.devcontainer/skills/nes-notifications/relay.py#L1).

**Selected resolution:** added the canonical Apache preamble to all eight reported shell/Python scripts, after existing shebangs and before code. Interpreter directives, file modes, and executable behavior are preserved. All eight exactly match the checker's canonical comment text; six shell syntax checks, both Python AST parses, and all 17 relay Python unit tests passed. No `.mjs` handling or other checker policy was changed.

### 43. Preamble checker has no JavaScript-module policy — deferred / ignored

**Status:** Ignored for now at the user's request. No `.mjs` policy branch, exception, or checker change is applied. This remains visible as a formatting blocker and is not marked resolved.

**Locations:** [scripts/check_preamble.py:150](scripts/check_preamble.py#L150), [.devcontainer/skills/nes-notifications/relay.test.mjs:1](.devcontainer/skills/nes-notifications/relay.test.mjs#L1), [.devcontainer/skills/nes-notifications/skill.test.mjs:1](.devcontainer/skills/nes-notifications/skill.test.mjs#L1).

**Possible future resolution, not selected:** add a deliberately supported `.mjs` license-comment branch and give these project-owned files the corresponding canonical block preamble. Keep a possible shebang first. Add a focused checker test/example so unsupported suffixes still fail.

**Alternative:** a documented narrow exception if these files are intentionally external/generated. Do not blanket-ignore every JavaScript file just to avoid an unsupported-extension error.

### 44. Work-item checker matches bundled syntax-highlighter text in exported reports — fix classification, not vendor content

**Locations:** [scripts/check_todos.py:75](scripts/check_todos.py#L75), [async_operators.md:1329](async_operators.md#L1329), [async_report.md:1329](async_report.md#L1329), [backpressure.md:1329](backpressure.md#L1329).

The flagged text is embedded highlighting JavaScript containing a token-matching expression, not a genuine work item.

**Recommendation:** identify these exported/generated HTML-in-Markdown documents narrowly, or make the checker understand their code/script regions. Keep validation of actual prose/source work items and issue references.

**Alternative:** move the third-party highlighter to a clearly identified vendored asset if that fits the documentation pipeline. Do not mutate its regular expression, manufacture issue references, or broadly exempt all Markdown/code examples without deciding what real work items should still be checked.

### 45. Toolchain/version sensitivity and incomplete analyzer coverage — record and close the gap separately

**Locations:** [.claude/skills/clang-tidy-loop/SKILL.md:30](.claude/skills/clang-tidy-loop/SKILL.md#L30), [.clang-tidy:125](.clang-tidy#L125), [nes-nautilus/src/DataTypes/VarVal.cpp:1](nes-nautilus/src/DataTypes/VarVal.cpp#L1).

The previous run used installed LLVM 22 rather than the skill's LLVM 19 example. Checks enabled by `*` can change between versions. Full analyzer processing of `VarVal.cpp` exceeded ten minutes; its non-analyzer checks completed.

**Recommendation:** document the actual LLVM version with the report and re-run the bounded/isolated analyzer when practical; compare against the supported CI toolchain before making broad configuration-policy changes. Keep the analyzer gap visible rather than declaring the whole run clean.

This is a validation gap, not a demonstrated defect in `VarVal.cpp`. No second container or fresh full build is needed merely to prepare this issue catalog.

## Final selected validation

- The full incremental build completed successfully (169 steps), followed by a successful 97-step rebuild covering the final annotation, invariant, constness, and namespace changes. No build tree was deleted or reconfigured.
- All **213 selected C++ tests passed** against the final rebuilt binaries, including retained operator-state, retained slot/dependency-state, HTTP-after-context-destruction, reflection/aliasing, graph contracts, source failure/draining, retry/cancellation, parser occurrence, runtime-value, and source-thread tests.
- All **8 HTTP occurrence-count scenarios** passed across interpreter/compiler execution; **10 EXPLAIN cases** and **5 lifecycle/cancellation scenarios** passed.
- Fresh selected issue-21–42 checks (plus optional-access verification) covered **29 translation units with zero diagnostics/errors**. Six focused issue-17–20 API/special-member runs also completed with zero diagnostics/errors. No in-place tidy fixes were used.
- All **17 relay Python unit tests** passed; all eight issue-42 preambles match the canonical checker text, six shell files passed syntax checks, and both Python files parsed successfully.
- Check-specific suppression scopes were balanced across all 88 modified/new project C++ files. `git diff --check` passed; no changes were staged or committed.

The first overlapping build/test run passed 181/183 tests but failed two graph death tests while their executable was being relinked. Both passed in a stable-binary serial rerun, and the final non-overlapping run passed all 213 tests. No test expectation or production behavior was weakened to make them pass.

Artifacts are under `/tmp/clang-tidy-loop-y_ghto42/`: `issues-9-42-coherent-build.log`, `issues-9-42-final-incremental-build.log`, `issues-9-42-final-tests.log`, `issues-9-42-final-counts.log`, `issues-9-42-final-explain.log`, `issues-9-42-final-lifecycle.log`, `issues-21-42-final-tidy/`, and `issues-17-20-final-checks.log`.

**This is not a whole-branch lint/format-clean claim:** issues 2/3 remain outstanding; 14 remains partial; 16/30/43 are ignored without new suppressions; 44 remains an unmodified exported-document false positive; and 45 retains the analyzer coverage gap. Unrelated pre-existing source-location defaults and other unselected findings were not silently expanded into this cleanup. All completed subagents were stopped.

## Suggested order of work

1. **Validate the remaining safety-sensitive points:** 2, 3, 9, 10. Issue 12 now has explicit runtime invariants; issue 13 uses the later user-authorized, check-specific test-file optional-access scopes because GoogleTest fatal assertions were not inferred. Issues 2 and 3 remain explicitly outstanding and not resolved after provider-blocked independent reviews. Issue 1 is resolved with option A; issue 5 was independently investigated as an analyzer false positive and annotated. Many may resolve to precise documentation/suppression after the existing invariants are verified; they are not automatically confirmed bugs.
2. **Low-risk local improvements:** 13, 25, 27–29, 31–34, 36–39, 41. Issues 11 and 15 are resolved with local references to guarded optionals and correct arithmetic/iterator types. Issue 16 is deferred/ignored without suppression. Review the ownership-sensitive subcases before applying changes.
3. **Preserve and explain deliberate semantics:** 4, 17–19, 21–24, 26, 35; annotate 20 selectively and handle test-data literals under 40 pragmatically. Issues 6 and 7 are resolved by removing the moved-from getter expectations and trivial copy/getter check; issue 8 is resolved with a narrow cast suppression.
4. **Optional larger maintenance:** any chosen policy alternatives in 19, 23, 26, 40. Issue 30 is explicitly deferred/ignored without a refactor or suppression.
5. **Separate unrelated/tooling work:** 42–45.

After selected fixes, run isolated fresh diagnostics rather than replaying old YAML offsets, format in the existing container, and rebuild/retest affected paths. Do not mark the run clean while unresolved diagnostics, formatting blockers, or analyzer gaps remain.
