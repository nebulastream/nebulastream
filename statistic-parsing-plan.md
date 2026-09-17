# feat(Statistics): Resolve statistic SQL functions through the aggregation registry

## Problem

The SQL parser (`nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp`) hard-coded the statistic surface:
`STATISTIC_BUILD(id, 'AVG', field)`, `STATISTIC_PROBE(id, 'Blob', ...)` and `STATISTIC_PROBE_RANGE(id, 'Blob', ...)`,
each with a hand-written binder, plus a metric-name chain that duplicated the aggregation registry. Every new
synopsis (equi-width histogram, count-min sketch, reservoir sample, ...) would have meant editing the parser again.

## What this PR does

The parser no longer names individual aggregations or synopses. A function name it does not know as a grammar
token is looked up in the existing `AggregationLogicalFunctionRegistry`; no new registry is introduced and the
ANTLR grammar is untouched (`functionName` already accepts any identifier).

After this PR, adding a synopsis costs:
1. A logical aggregation function class with `NAME`, `static constexpr bool IS_STATISTIC = true` and a
   `static create(AggregationLogicalFunctionRegistryArguments)`.
2. One line: `add_registry_entry(AggregationLogicalFunction <Name>)`.
3. The physical side (physical aggregation function, blob decoder), outside the parser.

## SQL surface

```sql
-- synopses (registered with IS_STATISTIC): flat, statistic id first
SELECT RESERVOIRSAMPLE(42, 100, 7)               FROM s WINDOW TUMBLING(ts, SIZE 5 SEC) INTO sink;
SELECT EQUIWIDTHHISTOGRAM(44, value, 128, 0, 25) FROM s WINDOW ... INTO sink;

-- scalar aggregations become statistics through a wrapper: aggregation first, id second
SELECT STATISTIC_BUILD(SUM(value), 43)           FROM s WINDOW ... INTO sink;
SELECT STATISTIC_BUILD(COUNT(*), 46)             FROM s WINDOW ... INTO sink;

-- every registered aggregation can be probed: (statisticId, (fieldName, typeName)...)
SELECT SUM_PROBE(43, total, float64)             FROM (...) INTO sink;  -- statistic of exactly the incoming window
SELECT SUM_PROBE_RANGE(43, total, float64)       FROM (...) INTO sink;  -- every statistic within the window bounds
```

No synopsis class lives on this branch yet, so the synopsis lines above are what the follow-up commits will
enable; the mechanism is covered by test aggregations (see Tests). Removed: the string-metric
`STATISTIC_BUILD(id, 'AVG', field)`, `STATISTIC_PROBE` and `STATISTIC_PROBE_RANGE`.

## Changes

**Registry** (`nes-logical-operators/registry/include/AggregationLogicalFunctionRegistry.hpp`)
- The entry is now a struct `{create, name, isStatistic}` instead of a bare `std::function`.
  `makeAggregationLogicalFunctionEntry<T>()` fills it from `&T::create`, `T::NAME` and the optional
  `T::IS_STATISTIC` (read with a `requires` concept; classes without it register as `false`). The CMake
  `ENTRY_TEMPLATE` only names this helper. `RuntimeRegistry` allows arbitrary entry types, so the loader and the
  registration glue are unchanged. `name` is the blob type the statistic store writer records, which is what a probe
  looks up.
- `AggregationLogicalFunctionRegistryArguments` is now a single `std::vector<LogicalFunction> parameters`: all call
  arguments in call order, fields and constants alike. The previous `on` / `includeNullValues` pair could not carry
  a synopsis's constants (`sampleSize`, `seed`, bin bounds). The parser does not know arity or types; each `create`
  validates them and throws `InvalidQuerySyntax`.
- The header documents the contract of a registered class (required `NAME` and `create`, optional `IS_STATISTIC`).

**Parameter helpers** (`Operators/Windows/Aggregations/AggregationParameters.{hpp,cpp}`)
- `parseFieldParameter`, `parseUnsignedParameter`, `parseStringParameter` read one entry of `parameters` and throw
  `InvalidQuerySyntax` naming the argument. They replace the parser-private constant parsing.
- The six existing `create` members (Avg, Count, Max, Median, Min, Sum) read `parameters` through them and throw
  `InvalidQuerySyntax` instead of `CannotDeserialize`. `COUNT` derives `includeNullValues` itself from a `*` argument.

**Provider** (`Operators/Windows/Aggregations/AggregationLogicalFunctionProvider.{hpp,cpp}`)
- `tryDescribe`, `tryProvide`, `provide` (throws `UnknownAggregationType`) and `registeredNames`. The parser reaches
  the registry only through it, like every other registry consumer (`LogicalFunctionProvider`, `SourceProvider`, ...).

**Parser** (`exitFunctionCall`, default branch). The hand-written statistic binders are gone. In this order:
1. Type constructors (`UINT64(1)`), first because their argument lives on the constant stack.
2. The call's arguments are taken off the expression stack.
3. **Registry arm**: constants pass through, every other argument becomes a field reference (expressions are
   desugared into a pre-aggregation projection as before). A marked entry takes the statistic id as its first
   argument and becomes the query's statistic build; an unmarked entry is an ordinary window aggregation, exactly
   like the token-based ones.
4. **Probe arm**: a name ending in `_PROBE` / `_PROBE_RANGE` whose base is registered becomes a statistic probe with
   blob type `entry.name`. An unknown base is an `InvalidQuerySyntax` listing the registered names.
5. **`STATISTIC_BUILD` arm**: turns the aggregation the inner call just produced into the statistic build. Wrapping
   a marked synopsis is rejected with "is written without STATISTIC_BUILD".
6. Scalar function registry, then the unknown-function error.

The token-based aggregations (`SUM`, `COUNT`, ...) keep their switch cases and their auto-names (`X_SUM`); the
auto-name block now also handles calls whose arguments were already consumed and calls without a field.
`exitPrimaryQuery` and the `AntlrSQLHelper` structs are unchanged.

**Docs**: `docs/guide/extensibility.md` (entry types may be structs, `IS_STATISTIC`), `docs/guide/query_api.md`
(statistic SQL surface).

## Tests

`nes-sql-parser/tests/StatementBinderTest.cpp` registers one test aggregation class twice, unmarked
(`TESTAGGREGATION(field, parameter)`) and marked (`TESTSYNOPSIS(statisticId, parameter)`), because the grammar has
no token for either, so the parser can only reach them through the registry. Covered: wrapped scalar builds
(`SUM`, `COUNT(*)`, expression argument), flat synopsis build (also lower-case), a registered aggregation by name
with auto-name and alias, unchanged token auto-names, probes of a synopsis and of a scalar (exact and range), and
the `InvalidQuerySyntax` cases (missing/zero/non-numeric id, wrong arity, wrapped synopsis, non-aggregation inside
`STATISTIC_BUILD`, a build next to another aggregation or without a window, two builds, two probes, unknown probe
base, unpaired payload fields, unknown type, and the removed string forms).

Verified on the feature commit: `statement-binder-test` (47 passed), `nes-logical-plan-test` and
`nes-join-logical-operator-test` (20 passed), formatter clean, full build. There are no statistic systests on this
branch, since the physical operators are not here; the binder tests are the coverage.

## Conflict with the merged #1971

The branch's merge base with `main` is from 2026-09-01. #1971 ("Refuse non-numeric aggregations as unsupported
queries", merged 2026-09-04) uses the registry in two places that this PR changes, so the rebase needs manual work:

- `nes-logical-operators/tests/AggregationInputTypeTest.cpp` calls the entry directly as a function,
  `find(name).value()({.on = {fieldAccess}, .includeNullValues = false})`. With the struct entry and the single
  `parameters` vector this no longer compiles; it becomes
  `AggregationLogicalFunctionProvider::provide(name, {.parameters = {fieldAccess}})`.
- The same test loops over **all** `getRegisteredNames()` and builds each with one field. That holds for the six
  scalar aggregations but not for a synopsis (`RESERVOIRSAMPLE(sampleSize, seed)` takes no field), so the first
  registered synopsis will fail `AcceptsNumericInput`. The loop has to skip entries with `isStatistic` (via
  `tryDescribe`) or the test needs a per-aggregation argument list.
- The parser's unknown-function error on `main` reads `AggregationLogicalFunctionRegistry::instance().getRegisteredNames()`
  directly, in the same default branch this PR rewrites. The rebase should keep #1971's message ("Unknown function:
  ... Supported aggregation functions are ...") but take the names from `AggregationLogicalFunctionProvider::registeredNames()`.

## Follow-ups

- The reservoir commit adds `IS_STATISTIC`, `DEFAULT_SEED` and a `create` reading `parameters` to
  `ReservoirSampleAggregationLogicalFunction`, its `add_registry_entry` line, and restores `requiresAllInputFields`.
  Nothing on the parser side.
- The reader lowering on the port branch still picks the blob decoder with an if/else on the blob type. Looking the
  physical aggregation up by blob type in `AggregationPhysicalFunctionRegistry` would make a new synopsis zero-touch
  end to end.
- Optional: route the six token-based aggregations through the registry arm and delete their switch cases.
