# Plan: Port the equi-width histogram

Source: `~/claude-espat-nes`, branch `feature/espat/histogram-delta-compression` (POC).
Target: this branch, `feature/td/port-histogram`, on top of `feature/td/statistic-registry-parsing`.

Revised 2026-09-17 after `histogram-port-plan-review.md`. The last section maps every review finding to where it
is addressed.

## Goal

1. **Core (required):** `EQUIWIDTHHISTOGRAM(id, field, budget, min, max)` builds a histogram per window, writes it to
   the statistic store, and `EQUIWIDTHHISTOGRAM_PROBE` / `_PROBE_RANGE` reads it back as one row per bin.
   Adding it must not touch `AntlrSQLQueryPlanCreator.cpp` or the grammar, as the registry-parsing plan promises.
2. **New blob format (required):** the stored blob no longer carries two bounds per bin. It carries the three
   numbers the bounds follow from, and the counters.

**Not part of this port:** the POC's histogram delta compression (the GEN → RESOLVER split that sends only changed
bins between two nodes). The section near the end keeps the notes on how it would be ported, for future reference
only.

## What the POC does today

| Piece | POC file | Note |
|---|---|---|
| Logical function | `nes-logical-operators/.../Histogram/EquiWidthHistogramLogicalFunction.{hpp,cpp}` | old class-hierarchy style (`StatisticLogicalFunction`, stamps, `withOnField`...); holds the memory budget |
| Budget → parameters | `StatisticLogicalFunction::calculateConfigs()` → `EquiWidthHistogramConfig{numBuckets, min, max, counterType}` | called in `LowerToPhysicalStatisticBuild`; the physical histogram only ever sees a bin count |
| Physical function | `nes-physical-operators/.../Statistic/Histogram/EquiWidthHistogramPhysicalFunction.{hpp,cpp}` | state = `[lower, counter, upper] * n + u64 nSeen` |
| Blob decoder | `nes-physical-operators/.../Statistic/Histogram/EquiWidthHistogramIteratorImpl.{hpp,cpp}` | reads `lower, counter, upper` per bin |
| Probe | `EquiWidthHistogramProbeLogicalOperator` + `LowerToPhysicalEquiWidthHistogramProbe` | one dedicated probe operator per synopsis |
| Hash-map page size | `LowerToPhysicalStatisticBuild.cpp:275`: `pageSize = max(conf.pageSize, entrySize * minEntriesPerPage)` | what lets a multi-kB histogram state fit into a hash-map entry; the target has no equivalent |
| Systest | `nes-systests/operator/aggregation/statistics/WindowAggregationHistograms.test` | goldens for 5 bins over `[0,25]` |
| Delta (not ported) | `EquiWidthHistogramDelta{Gen,Resolver}{Logical,Physical}Function`, `DeltaCompressionAggregationProbePhysicalOperator`, keyframe cache on `AggregationOperatorHandler`, `PlacementHintTrait`, retry delay on `ExecutionContext`, `DefaultStatisticQueryGenerator` flag, parser expansion of `EQUIWIDTHHISTOGRAMDELTA` | design doc: `git show ab09db585d:docs/histogram-delta-wire-compression-plan.md` in the POC (deleted in its last commit) |

POC blob (`lower()`), 24 bytes per bin:

```
[u32 totalSize][u32 metaDataSize = 8][u64 numberOfBins]
[ { <T> lowerBound, u64 counter, <T> upperBound } * numberOfBins ]
[u64 numberOfSeenTuples]            <- memcpy'd along with the state, never read by the iterator
```

For the benchmark's 682 bins that is 16,392 bytes of stored blob, of which 10,912 are bounds any reader could
compute.

Things in the POC to be aware of:

- `reset()` computes `upperBoundRef = counterRef + counterOffset`, where `counterOffset` is the size of the *bound*
  type. It only works because counter and bound are both 8 bytes. Disappears with the new layout.
- `lift()` does `(value - min) / binWidth` on `value.getRawValueAs<val<uint64_t>>()`. `getRawValueAs` is a
  `static_cast` (`VarVal.hpp:139`), a value conversion, not a reinterpretation. So: a value below `min` wraps and
  lands in the **last** bin; a negative signed input converts to a huge `uint64` and lands there too; an in-range
  float is truncated; a negative float is undefined behaviour. `binWidth = (max - min) / bins` is `0` when the
  range is smaller than the bin count, which divides by zero.
- `min` and `max` are parsed as unsigned integers, so a signed or fractional range cannot even be expressed.
- The last bin's stored upper bound is `min + bins * width`, which is below `max` whenever the division has a
  remainder, although values up to `max` (and beyond) are counted in it.
- `kMaxStaticUnrollBins = 224` exists because `reset`/`combine` unroll three writes per bin. With counters only,
  `reset` is a `memset` and `combine` is a plain runtime loop; the constant and the unrolled form are not ported.

## The new blob format

The delta wire format gets away with *no* bounds at all because both ends build their bins from the same query. A
stored blob cannot: a probe only knows `(statisticId, field names, types)`, and the build that wrote the blob may
be a different query. So the blob carries its own bin geometry, in O(1) instead of O(bins).

```
offset  size  field
0       8     numberOfBins       u64
8       8     minValue           u64
16      8     maxValue           u64
24      ...   numberOfBins * u64 counters
```

- The histogram is defined over **unsigned integer** inputs only (see step 5), so `minValue` / `maxValue` are plain
  `u64`, exactly what `parseUnsignedParameter` produced. The blob does not record the input field's width; the
  bounds the iterator emits are `UINT64`.
- There is no version, layout or counter-size byte. The blobs live in the statistic store of one running system
  and are never read by another build of the code, and the store already tags every blob with its type name. A
  different payload (sparse, narrower counters) would be a different type name; nothing in this format prepares
  for one.
- `numberOfSeenTuples` leaves the blob. The histogram has no part in it any more: `addStatisticBuild` pairs every
  statistic function with a `COUNT`, and that count is what the writer stores as
  `StatisticTuple::numberOfSeenMeasurements`.
- The `[u32 totalSize][u32 metaDataSize]` prefix goes too; the reservoir blob on
  `origin/feature/port-reservoir-sample-espat` has no such prefix and the store keeps the size next to the blob.

### Bin geometry

`max` is **inclusive**: the histogram covers the integers `min .. max`.

- `w = (max - min) / numberOfBins` (integer division, as in the POC). `create` guarantees `w >= 1`.
- Bin `i < numberOfBins - 1` is `[min + i*w, min + (i+1)*w)`.
- The last bin is `[min + (numberOfBins-1)*w, max]`. It additionally holds the remainder `(max - min) % numberOfBins`
  and the value `max` itself, so it spans `w + (max - min) % numberOfBins + 1` integers and is **wider** than the
  others by up to `numberOfBins`. With `w = 1` that is a last bin up to `numberOfBins + 1` times as wide as a
  regular one. This is a known property of the integer width, stated in the class documentation and pinned by the
  "range with a remainder" systest. A caller that wants the most even bins picks `max - min` as a multiple of the
  bin count; the last bin is then wider by exactly the one value `max`.
- Reported bounds: `binStart = min + i*w`; `binEnd = min + (i+1)*w` for all but the last bin, `binEnd = max` for the
  last. `binEnd` is exclusive except for the last bin, where it is inclusive.

Effect: 682 bins take `24 + 682*8 = 5,480` bytes instead of 16,392 (3.0x). The aggregation *state* shrinks from
`24*n + 8` to `8*n` bytes, so `reset`, `combine` and `lower` touch a third of the memory and `lower` is one header
write plus one `memcpy`.

The layout lives in one place, `nes-statistics/include/Statistics/EquiWidthHistogramBlob.hpp`, next to the
reservoir's `ReservoirSampleBlob.hpp` and in the same style: offset constants, nautilus header read/write helpers,
a bin reader, and a **plain C++** `validateEquiWidthHistogramBlob(std::span<const int8_t>)` (see "Validating a
blob"). The physical function and the iterator use it; nobody else knows an offset.

### The memory budget stays a separate function

The POC already separates the two: the histogram itself (physical function, blob) is sized by a **bin count**, and
`calculateConfigs()` is the only place that knows about a memory budget. The port keeps that split and makes it
sharper:

- The histogram proper takes `numberOfBins`. The logical function stores, reflects, hashes and explains
  `numberOfBins, min, max`, not the budget (the POC stored the budget and converted during lowering).
- One free function next to the blob constants, `equiWidthHistogramBinsForBudget(budgetBytes)`, returns
  `budget < HEADER_SIZE + 8 ? 0 : (budget - HEADER_SIZE) / 8`. The comparison comes first so the unsigned
  subtraction cannot wrap. It replaces `calculateConfigs()` / `StatisticConfig`; those are not ported.
- The SQL surface stays `EQUIWIDTHHISTOGRAM(id, field, budget, min, max)` as in the POC and the registry-parsing
  plan; `create` calls the budget function and constructs the histogram with the result. A later statistic query
  generator calls the same function.
- A range smaller than the bin count rejects the query (Decision 2). Because the user passes a budget and never
  sees the bin count, the error message must do the conversion for them: it names the bin count the budget led to,
  the range, and the budget for the largest accepted bin count (`HEADER_SIZE + 8 * (max - min)`).

With the new layout the formula is `(budget - 24) / 8` instead of `(budget - 8) / 24`, so
`EQUIWIDTHHISTOGRAM(44, value, 128, 0, 25)` yields 13 bins, not 5. The POC goldens cannot be copied. The systests
use a budget of 120 (12 bins, `w = 2`) for their main scenario, because 13 bins over `[0,25]` give `w = 1` and a
last bin of `[12, 25]`, which is a fine remainder test but a poor first example.

### Validating a blob

`StatisticIterator::forEachRecord(payload, emit)` is traced Nautilus code and only receives a pointer, so it can
neither throw directly nor compare the header to the stored size. And a variable-size statistic has to return `0`
from `getExpectedPayloadSizeInBytes()`, which switches off the one size check `validateAgainstProbe` has. Without
more, a foreign or truncated blob under the same type name is an out-of-bounds read of `numberOfBins * 8` bytes.

So `StatisticIterator` gets a second, non-traced virtual:

```cpp
/// Called once per loaded statistic from loadStatisticsProxy, before any traced code touches the payload.
/// Throws CannotProbeStatistic. The default accepts everything.
virtual void validate(std::span<const int8_t> payload) const;
```

`loadStatisticsProxy` calls it for every statistic next to the existing type-name and size checks. The histogram's
override delegates to `validateEquiWidthHistogramBlob`, which rejects: fewer than 24 bytes,
`numberOfBins == 0`, `minValue >= maxValue`, `maxValue - minValue < numberOfBins`, and
`payload.size() != 24 + numberOfBins * 8`. After that the traced `forEachRecord` can trust the header.

## Dependencies outside the histogram

### The physical statistic operators

This branch has the logical store reader/writer and the registry-driven parser, but **no physical side**. The
store reader/writer physical operators, their lowering rules, `StatisticIterator` and `ScalarStatisticIterator`
exist only in `ccfe22d067` on `origin/feature/port-reservoir-sample-espat`. That commit is by another author, mixes
the operators with the reservoir, is not final, and has no systest on its branch, so the reader and writer have
never run end to end.

They are brought over by **copying the files into this branch in a commit of their own** (step 1), before any
histogram code, so the copy stays recognisable and can be dropped again when the reservoir branch lands:

- Copied whole: `StatisticStore{Reader,Writer}PhysicalOperator.{hpp,cpp}`, `StatisticStoreOperatorHandler.hpp`,
  `LowerToPhysicalStatisticStore{Reader,Writer}.{hpp,cpp}`, `StatisticFieldResolution.hpp`,
  `StatisticIterator.{hpp,cpp}`, `ScalarStatisticIterator.{hpp,cpp}`, plus their `CMakeLists.txt` entries
  (`nes-physical-operators/src/Statistics/`, `nes-statistics/src/Statistics/`, the lowering rule list).
- Hunks from files this branch already has: the `CannotProbeStatistic` entry in `ExceptionDefinitions.inc`; the
  two rules in `LoweringRuleRegistry.hpp`; the `QueryCompiler.hpp` and `LowerToPhysicalOperators.cpp` edits;
  `AggregationPhysicalFunctionRegistryArguments::logicalFunction` replacing `includeNullValues`, with its two call
  sites (`LowerToPhysicalWindowedAggregation.cpp`, `CountAggregationPhysicalFunction.cpp`).
- Not copied: everything reservoir (`ReservoirSample*`, `ReservoirMerge`, the `PagedVectorRef` changes) and the
  parser hunks (this branch's parser replaces them).
- The only edits allowed in the copy commit are the ones needed to build here: `NUMBER_OF_SEEN_MEASUREMENTS`
  naming, and the reader lowering's `if (typeName == ReservoirSample)` branch removed so that it is scalar-only.
- The commit message names `ccfe22d067` and credits its author (`Co-authored-by`).
- The copied code is read before it is committed, not just made to compile. Known points to look at: the
  `thread_local tProbeStatistics` map keyed by handler id in the reader; `insertStatistic` using `emplace`, so a
  second write for the same `(id, start, end)` is silently dropped.

When the reservoir branch lands first, step 1 is dropped in the rebase and steps 2-3 are re-applied on top of its
version of the files. When this branch lands first, the reservoir branch drops the same files from its commit.

### The iterator registry (step 2)

The copied reader lowering is scalar-only. Step 2 replaces its hard-coded choice with a small name-keyed
`StatisticIteratorRegistry` in `nes-statistics`:

- Entry: `(StatisticBlobType typeName, std::vector<PayloadField> payloadFields) -> std::shared_ptr<StatisticIterator>`.
  The factory validates the probe's declared payload fields and throws `InvalidQuerySyntax` with the expected
  signature when they do not fit. This is a user error, so it must not be an `INVARIANT`.
- Names without an entry fall back to the scalar iterator, which keeps its "exactly one column" check, now also as
  a thrown error instead of an `INVARIANT`.
- This is the follow-up the registry-parsing work already noted; the histogram is the second decoder and the right
  moment to do it.

Step 2 also adds the `validate` virtual described above.

### The hash-map page size (step 3)

`LowerToPhysicalWindowedAggregation.cpp:172` passes `conf.pageSize` (default 1024 bytes) to the hash map unchanged,
and `ChainedHashMap` requires at least one entry per page. An entry is `sizeof(ChainedHashMapEntry) + keys + all
aggregation states`, so with the default configuration a histogram above roughly 120 bins fails the precondition
(debug) or computes `entriesPerPage = 0` (release). The POC grew the page instead.

The memory budget is what resolves this, and no new option is needed. The user has already said how much memory
the histogram may take, and the state follows from it directly: `getSizeOfStateInBytes() = 8 * numberOfBins =
budget - 24`, rounded down to 8. That state is part of `entrySize`. So a query with a budget above `page_size`
is not a misconfiguration to reject (as the hash join does for a tuple that does not fit its page); it is an
explicit request for a larger entry, and the lowering honours it by sizing the page from the entry.

Step 3 therefore ports the POC's rule without its extra `min_entries_per_page` option:
`pageSize = std::max(conf.pageSize, entrySize)`. It is written against `entrySize`, not against the histogram, so
it stays a generic rule with no statistics-specific code in the aggregation lowering, and the budget reaches it
through the state size alone.

- A state that large gets one entry per page, which for a statistic build (never keyed) is the only entry the map
  will hold. `ChainedHashMap` already allocates pages above the pooled buffer size through `getUnpooledBuffer`.
- This gives the budget a meaning beyond the blob: besides bounding the stored blob, it bounds the histogram's
  share of each hash-map page, so the memory held while a window is open is about `budget` per slice and worker
  thread.
- It is a change to the generic aggregation lowering and the only engine change in this port. It gets its own
  commit and a test that does not involve statistics (a lowering unit test, or a systest with a small `page_size`
  and an ordinary aggregation whose entry exceeds it).

### Statistic ids in systests

Every store reader/writer handler is built on `globalStatisticStore()`, which is process-global, and `emplace`
keeps the first blob written for a key. Whether the systest runner shares one worker process between test files is
checked in step 1. Either way the convention from here on is: a statistic id is used by exactly one query in the
whole systest tree. Ids are allocated in blocks per file and the block is stated in the file's header comment:
`100-119` for the scalar round-trip test of step 1, `200-299` for `WindowAggregationHistograms.test`. The ids
`43`/`44` stay where they are, in `StatementBinderTest`, which never reaches a store.

## Steps (core)

Each step is one commit that builds and passes its tests (`./.nix/nix-cmake.sh`, never more than `-j 4`).

1. **Copy the physical statistic operators** from `ccfe22d067` (above). Test: scalar round trip
   `STATISTIC_BUILD(SUM(x), 100)` → `SUM_PROBE(100, total, float64)` as a systest in the nested form
   `SELECT ... FROM (SELECT SUM_PROBE(...) FROM (SELECT STATISTIC_BUILD(...) ...))`, which this branch can parse but
   not yet run. This is the first end-to-end run of the copied code; expect to find problems here.
2. **Iterator registry + `StatisticIterator::validate`.** Test: unit tests in `nes-statistics/tests` for the
   registry lookup, the scalar fallback, and the scalar factory rejecting zero or two payload fields. The step 1
   systest still passes.
3. **Hash-map page size** grows to fit one entry (above).
4. **`EquiWidthHistogramBlob`** in `nes-statistics` + unit test in `nes-statistics/tests`:
   - header and counters written into a buffer and read back as bins, including the inclusive, wider last bin;
   - `equiWidthHistogramBinsForBudget` for a budget below the header, exactly one bin, and the 128 → 13 case;
   - `validateEquiWidthHistogramBlob` rejecting each case listed under "Validating a blob". This is plain C++, no
     tracing needed.
5. **Logical function** `EquiWidthHistogramAggregationLogicalFunction`, rewritten to the value-type concept
   (`WindowAggregationFunctionConcept`, modelled on the reservoir's class, not on the POC's stamp/`with*` class):
   - `NAME = "EquiWidthHistogram"`, `IS_STATISTIC = true`.
   - `create(parameters)`: `field, budget, min, max` through `parseFieldParameter` / `parseUnsignedParameter`,
     budget → bins through `equiWidthHistogramBinsForBudget`. Throws `InvalidQuerySyntax` for `min >= max`, a
     budget below one bin, or `max - min < bins` (the divide-by-zero case), with the message described under "The
     memory budget stays a separate function". Rejected, never clamped.
   - `withInferredType`: accepts `UINT8`, `UINT16`, `UINT32`, `UINT64`, **not nullable**. Everything else (signed,
     float, bool, char, varsized, any nullable field) throws `CannotInferSchema`-style with a message that says to
     cast or filter first, as the reservoir does for nullable fields. Aggregate type `VARSIZED`, not nullable.
   - Members are `numberOfBins, min, max`; the budget does not outlive `create`.
   - Reflector/Unreflector, `std::hash`, `add_registry_entry(AggregationLogicalFunction EquiWidthHistogram)`.
   - Test: `StatementBinderTest` cases for a good call and each rejected call (including the budget-too-large
     message); type inference rejecting a signed, a float and a nullable unsigned field; plan serialization round
     trip.
6. **Physical function** `EquiWidthHistogramAggregationPhysicalFunction` in `nes-physical-operators/src/Statistics/`:
   - state `[u64 counter * n]`, nothing else; `reset` = `nautilus::memset` to zero, `combine` = runtime loop adding
     `n` counters, `lower` = header + `memcpy`. No static unrolling, no threshold constant.
   - `lift`: keeps the POC's arithmetic (Decision 3), so every out-of-range value, below `min` as well as above
     `max`, is counted in the **last** bin. `value == max` lands there too, by the geometry. Documented on the class
     and pinned by tests so a later change is a visible decision. With unsigned, non-nullable input guaranteed by
     step 5 there is no conversion and no null to handle.
   - `create` reads bins/min/max from `arguments.logicalFunction`; registered under the same name.
   - Test: a unit test in `nes-physical-operators/tests` that drives `reset`/`lift`/`combine`/`lower` on a small
     histogram and checks the blob bytes: a value in the first bin, on an inner bin boundary, `== max`, `> max`,
     `< min`, a range with a remainder, and two states combined. These are lift-level behaviours; the systest only
     repeats two of them.
7. **Iterator** `EquiWidthHistogramStatisticIterator`:
   - Registered in the iterator registry under `EquiWidthHistogram`. The factory requires exactly three payload
     fields, all `UINT64`, and otherwise throws `InvalidQuerySyntax` naming the expected call,
     `EQUIWIDTHHISTOGRAM_PROBE(id, <binStart> uint64, <binCounter> uint64, <binEnd> uint64)`. The user chooses the
     names; position decides the meaning (start, counter, end: the POC's column order).
   - `getExpectedPayloadSizeInBytes()` returns `0`; `validate` delegates to `validateEquiWidthHistogramBlob`.
   - `forEachRecord` emits one record per bin with the bounds computed from the header as under "Bin geometry".
   - Test: unit test for the factory's rejections (two fields, four fields, a `float64` counter).
8. **Systests** `nes-systests/operator/aggregation/statistics/WindowAggregationHistograms.test`, ids `200-299`:
   - build only (the POC's first query with a budget of 120: 12 bins over `[0,25]`);
   - build → probe in the nested form, all bins;
   - out-of-range values on both sides landing in the last bin;
   - a range with a remainder, showing the wider inclusive last bin and its `binEnd = max`;
   - `_PROBE_RANGE` over several windows;
   - 682 bins with the default `page_size`, output filtered to populated bins like the POC's ManyBins scenario.
     This is the regression test for step 3 on the statistics path;
   - negative: `GROUP BY` with a histogram build, and two histogram builds in one query, both rejected by the
     parser today (`AntlrSQLQueryPlanCreator.cpp:849`, "Only one statistic build is supported per query"). The POC's
     multi-histogram scenario is therefore not ported.
   - Goldens are computed by hand from the input rows, not from the blob layout: for at least the first window the
     test comment shows the arithmetic (`w = (25 - 0) / 12 = 2`, value `1` → bin `0`, ...), so the expected output
     does not derive from the code under test.

No dedicated histogram probe operator is ported: `<NAME>_PROBE` on the generic store reader replaces the POC's
`EquiWidthHistogramProbeLogicalOperator` and its lowering rule.

## For future reference only: delta compression

**Decided 2026-09-17: the delta compression is not ported.** Nothing below is a task on this branch, and the core
steps must not prepare for it (no sparse layout, no layout byte, no engine hooks, no config options). The notes
stay so that whoever picks it up later does not have to redo the analysis.

First a caveat the new format creates. The POC measured 4.24x for delta over the full blob: 4,395 vs 18,619 bytes
per window, ~200 of 682 bins changing. Both figures are **wire** measurements (the root container's received
bytes divided by the window count), which is why the full-blob figure is above the 16,392-byte stored blob. Two
thirds of that full blob were bounds. Against the new 5,480-byte blob, the same delta is roughly **1.25x**, and
generic zstd on the plain blob likely beats it. So the delta is only worth porting together with a tighter delta
encoding, and the numbers must be re-measured before it is argued for.

D1. **Sparse payload** under a type name of its own (the blob has no version field): `[u64 numEntries] { u32 binIndex, u64 counter } *`.
    12 instead of the POC's 16 bytes per entry. The delta blob becomes the normal header + sparse payload + a
    16-byte delta trailer `{u64 isKeyframe, u64 intervalId}`, so one codec serves plain, GEN and RESOLVER. The
    plain histogram may use the sparse payload too when it is smaller (mostly-empty histograms); the iterator then
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

## Decisions

Decisions 1-5 were taken on 2026-09-17 with the first version of this plan. Decisions 2 and 3 were put up again
after the review and **confirmed**; 6-8 are new from the review.

1. The SQL parameter stays a memory budget, but only a separate function knows about budgets; the histogram takes
   a bin count (see "The memory budget stays a separate function").
2. `max - min < bins` rejects the query. Confirmed after review finding D6; the cost (a larger budget can turn a
   valid query invalid) is accepted and softened by an error message that names the largest accepted budget.
3. Values below `min` and above `max` keep landing in the last bin. Confirmed after the review; pinned by a unit
   test and a systest.
4. The delta compression is not ported. If it ever is: separate commits **and** a default-off optimizer option,
   always compiled (no CMake switch).
5. No explicit-bounds layout; bounds are never stored. Future histogram kinds are not a concern of this format,
   which is why the header has no version, layout or counter-size field (version byte dropped on 2026-09-17).
6. The bin width stays the POC's integer `(max - min) / bins`; the last bin is inclusive of `max` and wider by the
   remainder. Documented and tested instead of changed (follows from confirming Decision 2).
7. Nullable input is rejected during type inference; so is every input type that is not an unsigned integer.
8. The physical statistic operators are copied from `ccfe22d067` in a commit of their own.

## Review findings and where they are addressed

| Finding | Resolution |
|---|---|
| B1 hash-map page size caps the bin count | new step 3, "The hash-map page size" |
| B2 keyed-window systest impossible; multi-histogram query rejected | step 8: both become negative tests, keyed scenario dropped |
| B3 no mechanism to validate a blob | `StatisticIterator::validate` (step 2), `validateEquiWidthHistogramBlob` (step 4), "Validating a blob" |
| D1 `nSeen` dead in the state | state is `8 * n`; blob-format section and step 6 |
| D2 nulls unspecified | Decision 7, step 5 |
| D3 wrong "reinterpreted" claim; signed/float accepted but wrong | POC notes corrected; unsigned-only in step 5; header `min`/`max` are `u64` |
| D4 probe contract unspecified | step 7 factory: three `UINT64` fields by position, `InvalidQuerySyntax` otherwise; registry entry signature in step 2 |
| D5 last bin wider, `max` inclusivity undefined | "Bin geometry": `max` inclusive, wider last bin stated and tested (Decision 6); binning itself unchanged by decision |
| D6 more budget can invalidate a query; underflow in the budget formula | Decision 2 confirmed, error message names the largest accepted budget; comparison before subtraction |
| D7 header prepares for layouts the plan forbids | `layout`, `counterSizeBytes` and the version byte removed; the header is bins, min, max |
| Step 1 under-specified, mixed authorship, missing hunks, bundled registry | "The physical statistic operators": file list, hunk list, separate copy commit with credit, rebase path; registry is its own step 2 |
| Scalar iterator needs the type name | registry entry takes `(typeName, payloadFields)` |
| Process-global store, id collisions | "Statistic ids in systests" |
| Unroll threshold re-measurement | dropped; runtime loop only |
| Goldens derived from the layout under test | step 8: hand-computed with the arithmetic in the comment |
| Two different POC baselines (16,392 / 18,619) | delta section now says the latter is a wire measurement |
| No unit test for the physical function | step 6 |
| Decision 3 questioned | confirmed by the author, recorded as such |
| Decision dates | "Decisions" header states what was decided when |
