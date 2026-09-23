# Review: `histogram-port-plan.md`

Reviewed 2026-09-17 against this branch (`feature/td/port-histogram` at `a7e48211a4`), the POC checkout
(`~/claude-espat-nes`, `feature/espat/histogram-delta-compression`) and `ccfe22d067` on
`origin/feature/port-reservoir-sample-espat`. Nothing was built or run; every finding comes from reading source.

**Verdict:** the plan will not work as written. The blob-format section is sound, but two of its own systests
would fail on this branch and several claims about the POC and the target are wrong.

## Blockers

### B1. The 682-bin systest will not run on this branch

- `LowerToPhysicalWindowedAggregation.cpp:170-172` hands `conf.pageSize` straight to the hash map, and the default
  (`DEFAULT_PAGED_VECTOR_SIZE`) is 1024 bytes.
- `ChainedHashMap.cpp:89` requires `pageSize / entrySize > 0`. With 8 bytes per bin that caps the histogram at
  roughly 120 bins.
- 682 bins need 5,464 bytes of state. That trips the precondition in debug builds; in release, zero entries per
  page then feeds a `% entriesPerPage`.
- The POC avoided this in `LowerToPhysicalStatisticBuild.cpp:275` with
  `pageSize = max(conf.pageSize, entrySize * minEntriesPerPage)`.
- The plan ports neither that line nor the `minEntriesPerPage` option, and never mentions the limit.
- This needs its own step, either porting that page-size rule or moving the state out of the hash-map entry. It is
  an engine change to the generic aggregation lowering, which conflicts with the plan's tone that the engine stays
  untouched.

### B2. The "keyed window" systest in step 6 cannot exist

- `AntlrSQLQueryPlanCreator.cpp:849` throws `InvalidQuerySyntax` for any statistic build with GROUP BY, and
  `addStatisticBuild` passes `{}` as keys.
- The store is keyed by `(statisticId, start, end)`, and `insertStatistic` uses `emplace`, so a second tuple for the
  same window is silently dropped.
- Either drop the test or make it a negative test.
- The POC's "multiple histograms in a single query" scenario is rejected too ("Only one statistic build is
  supported per query"). "The POC's build and probe queries" therefore cannot be taken verbatim.

### B3. "Validates version/layout ... rejected" has no mechanism behind it

- `StatisticIterator::forEachRecord(payload, emit)` is traced Nautilus code and receives only a pointer, not the
  payload size.
- Rejection from there needs an `invoke` proxy that throws, which the plan does not describe.
- The header can never be checked against the stored size (`32 + numberOfBins * 8 == getStatisticDataSize()`).
- `getExpectedPayloadSizeInBytes()` has to return 0 for a variable-size blob, which disables the only size check in
  `validateAgainstProbe`.
- A foreign or truncated blob under the same type name therefore becomes an out-of-bounds read of
  `numberOfBins * 8` bytes.
- Fix: add a plain C++ hook on the iterator, such as `validate(std::span<const int8_t>)`, called from
  `loadStatisticsProxy` beside the existing checks and throwing `CannotProbeStatistic`.
- The step 2 unit test for a rejected version then tests a C++ function instead of traced code, which is also far
  easier to write.

## Design problems

### D1. The `nSeen` word in the state is dead weight

- `addStatisticBuild` already pairs every statistic function with a `CountAggregationLogicalFunction`, and that
  count is what reaches `StatisticTuple::numberOfSeenMeasurements`.
- A target `lower()` writes one result field, so nothing can read `[u64 nSeen]`.
- Keeping it costs a load and a store per `lift` for nothing. The state should be `8 * n`.
- The plan's sentence "`StatisticTuple` already stores it" is true but omits that the histogram has no part in
  producing it.

### D2. Null inputs are never mentioned

- The paired COUNT is built with `includeNulls = true`, so with a nullable field `numberOfSeenMeasurements`
  exceeds the sum of the bin counters.
- The target has null-aware aggregations (`WindowAggregationNull.test`), and the reservoir rejects nullable fields
  in lowering.
- The plan has to choose one of: reject, skip nulls and document the mismatch, or add a null bin. Without a
  choice, `lift` does arithmetic on a null `VarVal`.

### D3. The type story is wrong and internally contradictory

- The plan says signed and float inputs are "reinterpreted, not converted". `VarVal::getRawValueAs`
  (`VarVal.hpp:139`) is a `static_cast`, which is a value conversion.
- In-range floats therefore truncate and bin correctly; a negative float is UB in the float-to-unsigned cast.
- A negative signed value wraps to a huge `uint64` and lands in the last bin.
- `min` and `max` come from `parseUnsignedParameter`, so they are always `uint64`. "Raw 8 bytes of the input type"
  in the header is meaningless, and a negative `min` cannot be expressed at all.
- Step 3 says "numeric input only" while step 4 declares signed and float out of scope. That accepts queries whose
  results are known to be wrong.
- Fix: make `withInferredType` accept unsigned integers only and define header `min`/`max` as `u64`.

### D4. "Self-describing" is overstated, and the probe contract is unspecified

- The probe is `EQUIWIDTHHISTOGRAM_PROBE(id, binStart, uint64, binCounter, uint64, binEnd, uint64)`. The user
  supplies the names, the count and the types.
- The plan does not say what happens with two pairs, four pairs, or `float64` for the counter.
- The scalar path guards this with an `INVARIANT` in lowering. The histogram needs the equivalent, preferably a
  user-facing error instead of an invariant.
- That check belongs in the iterator-registry factory, so the entry signature needs a defined way to fail.

### D5. "Last bin ends at max" corrects the reported bound but not the binning

- With `w = floor((max - min) / n)`, the last bin absorbs the remainder of up to `n - 1` units.
- Example: range 100 with 60 bins gives `w = 1`, so the last bin spans `[59, 100]`, 41 times the width of the
  others.
- As `max - min` approaches `2n - 1`, the last bin covers half the domain.
- The plan's own example, `[0, 25]` with 12 bins, has a last bin 1.5 times as wide as the others.
- The plan never says whether `max` is inclusive, although that changes `w`.
- Either compute the index as `(v - min) * n / (max - min + 1)` with an overflow guard, or state plainly that the
  last bin is wider and pin that with a test.

### D6. Decisions 1 and 2 together mean more memory can invalidate a query

- The user passes a budget and never sees the bin count, yet `max - min < bins` rejects the query.
- `EQUIWIDTHHISTOGRAM(44, value, 128, 0, 10)` fails, and the fix is to ask for less memory.
- A budget is an upper bound, so clamping bins to the range honours it; that is not the silent alteration "never
  clamped" is meant to prevent. A future statistic query generator calling the same budget function would hit the
  same rejection.
- `(budget - 32) / 8` underflows for `budget < 32`, so the "below one bin" check must come before the subtraction.

### D7. The header contradicts the plan's own scope rule

- "Steps 1-6 must not prepare for delta compression (no sparse layout...)", yet the header has a `layout` byte
  whose only planned second value is the delta section's sparse layout.
- It also carries a `counterSizeBytes` field and 5 reserved bytes, while Decision 5 says future kinds are "not a
  concern of this format".
- A version byte covers all of these. Either drop `layout` and `counterSizeBytes` or state that they are forward
  preparation.

## Step 1 is the riskiest part and the least specified

- `ccfe22d067` is one commit by another author. It mixes the reservoir with the operators, is "not final", and has
  no systest on that branch, so the reader and writer have never run end to end.
- "Bring over exactly that slice" means hand-splitting roughly 1,000 lines. Those lines collide when the reservoir
  lands, and the plan only covers the case where it lands first.
- The slice list omits the new `CannotProbeStatistic` entry in `ExceptionDefinitions.inc` and the edits to
  `QueryCompiler.hpp` and `LowerToPhysicalOperators.cpp`.
- The reader keeps its loaded statistics in a `thread_local` map keyed by handler id. The slice should be reviewed
  before it is adopted, not just copied.
- Step 1 also bundles the new iterator registry. It should be two commits: carry-over with authorship preserved,
  then the registry.
- Stacking this branch on the reservoir branch would be better if that can be coordinated.
- The iterator-registry sketch takes `(payloadFields)`, but `ScalarStatisticIterator` also needs the type name. The
  "fall back to scalar for unregistered names" path needs it passed in.
- Every lowering builds its handler on `globalStatisticStore()`, so the store is process-global. If the systest
  worker is shared between test files, statistic ids must be unique across all of them, and `emplace` means a
  re-used id silently keeps the old blob.
- The plan re-uses 43 and 44 from the binder tests and the POC without any id convention.

## Smaller points

- **Unroll threshold:** the plan says `reset` becomes a `memset`, yet step 4 still says to re-measure the unroll
  threshold. Only `combine` loops now; write the runtime loop and delete the item unless a benchmark is actually
  planned.
- **Goldens:** "rewritten from the documented layout" derives the expected output from the specification under
  test. Hand-compute at least one window from the input rows and show that arithmetic in the test comment.
- **Size arithmetic:** the 3.0x and 5,488-byte figures are correct. The POC baseline is given as 16,392 bytes in
  one section and 18,619 in the delta section without saying that the latter is a wire measurement.
- **Missing unit test:** nothing tests the physical function. The out-of-range, `value == max` and remainder cases
  are lift-level behaviour that a systest covers only slowly and indirectly.
- **Decision 3:** routing values below `min` into the last bin is only defensible as POC parity, and the goldens
  are being rewritten anyway. Pinning it with a systest makes fixing it later more expensive. Dropping
  out-of-range values is cheap and makes `numberOfSeenMeasurements - sum(counters)` the out-of-range count.
- **Dates:** "Decided 2026-09-17" is the day the plan was written, so the decisions and the plan were written
  together. They can be reopened in light of D5, D6 and Decision 3.

## What holds up

- The 24-byte POC layout, the `counterOffset` bug in `reset()` and the zero `binWidth` case are all accurate
  against the POC source.
- Keeping the budget in a free function and only `numberOfBins`, `min` and `max` in the logical function is the
  right split.
- `arguments.logicalFunction` matches how the reservoir commit wires physical `create`.
- No parser change is needed. `tryDescribe` plus the `_PROBE` suffix handling already covers a new registered name.
- Deferring delta compression is right, and the 1.25x caveat is a useful note for whoever picks it up.

## Needed before implementation

1. A page-size step for B1.
2. A specified validation path for B3.
3. Decisions on nulls and accepted input types (D2, D3).
4. A corrected systest list (B2).
