# Plan: Port the equi-width histogram

Source: `~/claude-espat-nes`, branch `feature/espat/histogram-delta-compression` (POC).
Target: this branch, `feature/td/port-histogram`, on top of `feature/td/statistic-registry-parsing`.

## Goal

1. **Core (required):** `EQUIWIDTHHISTOGRAM(id, field, budget, min, max)` builds a histogram per window, writes it to
   the statistic store, and `EQUIWIDTHHISTOGRAM_PROBE` / `_PROBE_RANGE` reads it back as one row per bin.
   Adding it must not touch `AntlrSQLQueryPlanCreator.cpp` or the grammar, as the registry-parsing plan promises.
2. **New blob format (required):** the stored blob no longer carries two bounds per bin. It carries the three
   numbers the bounds follow from, and the counters.

**Not part of this port:** the POC's histogram delta compression (the GEN → RESOLVER split that sends only changed
bins between two nodes). The last section keeps the notes on how it would be ported, for future reference only.

## What the POC does today

| Piece | POC file | Note |
|---|---|---|
| Logical function | `nes-logical-operators/.../Histogram/EquiWidthHistogramLogicalFunction.{hpp,cpp}` | old class-hierarchy style (`StatisticLogicalFunction`, stamps, `withOnField`...); holds the memory budget |
| Budget → parameters | `StatisticLogicalFunction::calculateConfigs()` → `EquiWidthHistogramConfig{numBuckets, min, max, counterType}` | called in `LowerToPhysicalStatisticBuild`; the physical histogram only ever sees a bin count |
| Physical function | `nes-physical-operators/.../Statistic/Histogram/EquiWidthHistogramPhysicalFunction.{hpp,cpp}` | state = `[lower, counter, upper] * n + u64 nSeen` |
| Blob decoder | `nes-physical-operators/.../Statistic/Histogram/EquiWidthHistogramIteratorImpl.{hpp,cpp}` | reads `lower, counter, upper` per bin |
| Probe | `EquiWidthHistogramProbeLogicalOperator` + `LowerToPhysicalEquiWidthHistogramProbe` | one dedicated probe operator per synopsis |
| Systest | `nes-systests/operator/aggregation/statistics/WindowAggregationHistograms.test` | goldens for 5 bins over `[0,25]` |
| Delta (not ported) | `EquiWidthHistogramDelta{Gen,Resolver}{Logical,Physical}Function`, `DeltaCompressionAggregationProbePhysicalOperator`, keyframe cache on `AggregationOperatorHandler`, `PlacementHintTrait`, retry delay on `ExecutionContext`, `DefaultStatisticQueryGenerator` flag, parser expansion of `EQUIWIDTHHISTOGRAMDELTA` | design doc: `git show ab09db585d:docs/histogram-delta-wire-compression-plan.md` in the POC (deleted in its last commit) |

POC blob (`lower()`), 24 bytes per bin:

```
[u32 totalSize][u32 metaDataSize = 8][u64 numberOfBins]
[ { <T> lowerBound, u64 counter, <T> upperBound } * numberOfBins ]
[u64 numberOfSeenTuples]            <- memcpy'd along with the state, never read by the iterator
```

For the benchmark's 682 bins that is 16,392 bytes, of which 10,912 are bounds any reader could compute.

Things in the POC to be aware of (the out-of-range behaviour of `lift()` is kept for now, see Decisions; the
rest is fixed by the port):

- `reset()` computes `upperBoundRef = counterRef + counterOffset`, where `counterOffset` is the size of the *bound*
  type. It only works because counter and bound are both 8 bytes. Disappears with the new layout.
- `lift()` does `(value - min) / binWidth` on the raw `uint64`. A value below `min` wraps and lands in the **last**
  bin; a signed or float input is reinterpreted, not converted. `binWidth = (max - min) / bins` is `0` when the range
  is smaller than the bin count, which divides by zero.
- The last bin's stored upper bound is `min + bins * width`, which is below `max` whenever the division has a
  remainder, although values up to `max` (and beyond) are counted in it.
- `kMaxStaticUnrollBins = 224` exists because `reset`/`combine` unroll three writes per bin. With counters only,
  `reset` is a `memset` and the threshold may not be needed; re-measure instead of copying the constant.

## The new blob format

The delta wire format gets away with *no* bounds at all because both ends build their bins from the same query. A
stored blob cannot: a probe only knows `(statisticId, field names, types)`, and the build that wrote the blob may
be a different query. So the blob has to be self-describing, but in O(1) instead of O(bins).

```
offset  size  field
0       1     version            = 1
1       1     layout             0 = Dense; other values are rejected
2       1     counterSizeBytes   = 8 for now; lets a later change narrow counters without a new version
3       5     reserved, zero
8       8     numberOfBins
16      8     minValue           raw 8 bytes of the input type
24      8     maxValue
32      ...   payload
```

- **Dense** payload: `numberOfBins` counters and nothing else. Bin `i` is `[min + i*w, min + (i+1)*w)` with
  `w = (max - min) / numberOfBins`; the last bin ends at `max` (fixes the remainder issue above).
- Bounds are never stored.
- `numberOfSeenTuples` leaves the blob. `StatisticTuple::numberOfSeenMeasurements` already stores it.
- The `[u32 totalSize][u32 metaDataSize]` prefix goes too; the reservoir blob on
  `origin/feature/port-reservoir-sample-espat` has no such prefix and the store keeps the size next to the blob.

Effect: 682 bins take `32 + 682*8 = 5,488` bytes instead of 16,392 (3.0x). The aggregation *state* shrinks the same
way (`8*n + 8` instead of `24*n + 8`), so `reset`, `combine` and `lower` touch a third of the memory and `lower`
is one header write plus one `memcpy`.

The layout lives in one place, `nes-statistics/include/Statistics/EquiWidthHistogramBlob.hpp`, next to the
reservoir's `ReservoirSampleBlob.hpp` and in the same style: offset constants, nautilus header read/write helpers,
and a bin reader. The physical function and the iterator use it; nobody else knows an offset.

### The memory budget stays a separate function

The POC already separates the two: the histogram itself (physical function, blob) is sized by a **bin count**, and
`calculateConfigs()` is the only place that knows about a memory budget. The port keeps that split and makes it
sharper:

- The histogram proper takes `numberOfBins`. The logical function stores, reflects, hashes and explains
  `numberOfBins, min, max`, not the budget (the POC stored the budget and converted during lowering).
- One free function next to the blob constants, `equiWidthHistogramBinsForBudget(budgetBytes)`, returns
  `(budget - HEADER_SIZE) / counterSize`. It replaces `calculateConfigs()` / `StatisticConfig`; those are not ported.
- The SQL surface stays `EQUIWIDTHHISTOGRAM(id, field, budget, min, max)` as in the POC and the registry-parsing
  plan; `create` calls the budget function and constructs the histogram with the result. A later statistic query
  generator calls the same function.

With the new layout the formula is `(budget - 32) / 8` instead of `(budget - 8) / 24`, so
`EQUIWIDTHHISTOGRAM(44, value, 128, 0, 25)` yields 12 bins, not 5. The POC goldens cannot be copied; they are
rewritten from the documented layout (which is how the POC wrote its delta goldens too).

## Dependency: the physical statistic operators

This branch has the logical store reader/writer and the registry-driven parser, but **no physical side**. The
store reader/writer physical operators, their lowering rules, `StatisticIterator` and `ScalarStatisticIterator`
exist only in `ccfe22d067` on `origin/feature/port-reservoir-sample-espat`, which is not final. The histogram
cannot run end to end without them, so step 1 brings over exactly that slice, without the reservoir:

- `StatisticStore{Reader,Writer}PhysicalOperator`, `StatisticStoreOperatorHandler`, both `LowerToPhysicalStatisticStore*`
  rules, `StatisticFieldResolution.hpp`, `StatisticIterator`, `ScalarStatisticIterator`.
- `AggregationPhysicalFunctionRegistryArguments::logicalFunction` (replaces `includeNullValues`), which is how a
  physical `create` reaches its synopsis parameters.
- Adjusted to this branch: `NUMBER_OF_SEEN_MEASUREMENTS` naming, no parser hunks (this branch's parser replaces them).
- The reader lowering picks the decoder with `if (typeName == ReservoirSample) ... else scalar`. Replace that with a
  small name-keyed `StatisticIteratorRegistry` (entry: `(payloadFields) -> shared_ptr<StatisticIterator>`), falling
  back to the scalar iterator for unregistered names. This is the follow-up the registry-parsing work already
  noted; the histogram is the second decoder and the right moment to do it.

If the reservoir branch lands first, step 1 shrinks to the iterator registry.

## Steps (core)

Each step is one commit that builds and passes its tests (`./.nix/nix-cmake.sh`, never more than `-j 4`).

1. **Physical statistic operators + iterator registry** (above). Test: scalar round trip
   `STATISTIC_BUILD(SUM(x), 43)` → `SUM_PROBE(43, total, float64)` as a systest, which this branch can parse
   but not yet run.
2. **`EquiWidthHistogramBlob`** in `nes-statistics` + unit test in `nes-statistics/tests` that writes a header and
   counters into a buffer and reads bins back, including the last-bin-ends-at-max rule and the rejected
   unknown version / layout.
3. **Logical function** `EquiWidthHistogramAggregationLogicalFunction`, rewritten to the value-type concept
   (`WindowAggregationFunctionConcept`, modelled on the reservoir's class, not on the POC's stamp/`with*` class):
   - `NAME = "EquiWidthHistogram"`, `IS_STATISTIC = true`.
   - `create(parameters)`: `field, budget, min, max` through `parseFieldParameter` / `parseUnsignedParameter`,
     budget → bins through `equiWidthHistogramBinsForBudget`. Throws `InvalidQuerySyntax` for `min >= max`, a
     budget below one bin, or `max - min < bins` (the divide-by-zero case). Rejected, never clamped.
   - `withInferredType`: numeric input only; aggregate type `VARSIZED`.
   - Members are `numberOfBins, min, max`; the budget does not outlive `create`.
   - Reflector/Unreflector, `std::hash`, `add_registry_entry(AggregationLogicalFunction EquiWidthHistogram)`.
   - Test: `StatementBinderTest` cases for a good call and each rejected call; plan serialization round trip.
4. **Physical function** `EquiWidthHistogramAggregationPhysicalFunction` in `nes-physical-operators/src/Statistics/`:
   - state `[u64 counter * n][u64 nSeen]`; `reset` = zero, `combine` = add, `lower` = header + `memcpy`.
   - `lift`: keeps the POC's arithmetic for now, so every out-of-range value, below `min` as well as above `max`,
     is counted in the **last** bin. Documented on the class and pinned by a systest so a later change is a
     visible decision. Signed and float inputs stay out of scope the same way (reinterpreted, as in the POC).
   - `create` reads bins/min/max from `arguments.logicalFunction`; registered under the same name.
   - Re-measure whether the static-unroll threshold is still needed; keep the runtime loop as the only form if not.
5. **Iterator** `EquiWidthHistogramStatisticIterator`: validates version/layout, then emits per bin the declared
   payload fields in the order `binStart, binCounter, binEnd` (the POC's column order), computing the bounds from
   the header. Registered in the iterator registry under `EquiWidthHistogram`.
6. **Systests** `nes-systests/operator/aggregation/statistics/WindowAggregationHistograms.test`: the POC's build
   and probe queries with re-derived goldens, plus out-of-range values, a range with a remainder, a keyed window,
   and one query above the old unroll threshold (682 bins, output filtered to populated bins like the POC's
   ManyBins scenario).

No dedicated histogram probe operator is ported: `<NAME>_PROBE` on the generic store reader replaces the POC's
`EquiWidthHistogramProbeLogicalOperator` and its lowering rule.

## For future reference only: delta compression

**Decided 2026-09-17: the delta compression is not ported.** Nothing below is a task on this branch, and steps 1-6
must not prepare for it (no sparse layout, no engine hooks, no config options). The notes stay so that whoever
picks it up later does not have to redo the analysis.

First a caveat the new format creates. The POC measured 4.24x for delta over the full blob (4,395 vs 18,619
bytes per window, ~200 of 682 bins changing). Two thirds of that full blob were bounds. Against the new 5,488-byte
blob, the same delta is roughly **1.25x**, and generic zstd on the plain blob likely beats it. So the delta is only
worth porting together with a tighter delta encoding, and the numbers must be re-measured before it is argued for.

D1. **Sparse layout** in `EquiWidthHistogramBlob` (`layout = 1`): `[u64 numEntries] { u32 binIndex, counter } *`.
    12 instead of the POC's 16 bytes per entry. The delta blob becomes the normal header + sparse payload + a
    16-byte delta trailer `{u64 isKeyframe, u64 intervalId}`, so one codec serves plain, GEN and RESOLVER. The
    plain histogram may use the sparse layout too when it is smaller (mostly-empty histograms); the iterator then
    has to emit the zero bins in between, so this is off unless measured useful.
D2. **Engine hooks**, each behind a default that leaves every other aggregation untouched:
    baseline-aware `lower(state, baseline, isKeyframe, intervalId, ...)` virtual with a forwarding default;
    `WindowBasedOperatorHandler::onGarbageCollect`; keyframe cache on `AggregationOperatorHandler`;
    `ExecutionContext::setOpenReturnState(REPEAT, delay)` through `DelayedTaskSubmitter` (exists in the target);
    the NetworkSink used-size fix `621d925867` if the target still sends child buffers at capacity.
D3. **GEN / RESOLVER physical functions + `DeltaCompressionAggregationProbePhysicalOperator`**, ported onto the
    counters-only state (the `counterOffset` / `totalBinSize` arithmetic collapses to `8 * i`).
D4. **Plan shape without touching the parser.** The POC expanded `EQUIWIDTHHISTOGRAMDELTA` inside
    `AntlrSQLQueryPlanCreator.cpp`; that is exactly what the registry work forbids. Instead: register one logical
    function `EquiWidthHistogramDelta` (`IS_STATISTIC`), and add a static optimizer rule
    (`nes-query-optimizer/src/Rules/Static/HistogramDeltaSplitRule.cpp`) that rewrites
    `WindowedAggregation[Delta] → StoreWriter` into
    `WindowedAggregation[DeltaGen] → WindowedAggregation[DeltaResolver, TUMBLING on STATISTICSTART] → StoreWriter`.
    GEN and RESOLVER are not registered for SQL.
D5. **Placement**: `PlacementHintTrait` + the hard pin in `BottomUpPlacement` (target has the file), stamped by the
    D4 rule. Without it both halves co-locate and nothing crosses a wire.
D6. **Config**, two options, no CMake switch:
    - `enable_histogram_delta_compression` on `QueryOptimizerConfiguration` (`nes-query-optimizer/interface/`),
      default **false**. It belongs to the optimizer because it decides the plan shape, and the D4 rule is where it
      is read. Off: the rule rewrites `EquiWidthHistogramDelta` to a plain `EquiWidthHistogram`, so the query still
      runs and stores the identical blob, just without the split. On: GEN → RESOLVER. The POC had the same flag
      but read it in `DefaultStatisticQueryGenerator`, which the target does not have.
    - `histogram_delta_keyframe_interval` on `QueryExecutionConfiguration`, default 10, as in the POC (it is
      consumed during lowering, per worker).
    Lowering never reads the on/off flag; the delta probe operator appears exactly when the plan contains the
    delta functions.
D7. **Tests**: the four-scenario `WindowAggregationHistogramDelta.test` with goldens for the new layout,
    `DistributedPlanningTest.PlacementHintForcesSplit`, a rule unit test for D4 covering both flag settings (off
    must produce the plain histogram), and the two-worker bats test.
    Benchmarks under `scripts/benchmarking/histogram_delta/` are not ported; re-run them from the POC checkout
    against this build if the wire numbers are needed.

Not ported either way: `compress_statistic` / zstd wrapping, the REPL shell tests, the POC's known limits (tumbling
only, no loss/restart recovery).

## Decisions (2026-09-17)

1. The SQL parameter stays a memory budget, but only a separate function knows about budgets; the histogram takes
   a bin count (see "The memory budget stays a separate function").
2. `max - min < bins` rejects the query.
3. Values below `min` keep landing in the last bin for now.
4. The delta compression is not ported. If it ever is: separate commits **and** a default-off optimizer option,
   always compiled (no CMake switch).
5. No explicit-bounds layout; bounds are never stored. Future histogram kinds are not a concern of this format.
