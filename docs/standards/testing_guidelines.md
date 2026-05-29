## Testing

NebulaStream uses a layered testing strategy.
Each layer has a defined scope, and layers complement rather than replace each other.
A unit test verifying component logic and a systest verifying end-to-end correctness are not interchangeable, so pick the layers your change actually needs rather than defaulting to one.

# Test Types at a Glance

| Type              | Scope                                                | Required when adding                              | CI cadence                          |
|-------------------|------------------------------------------------------|---------------------------------------------------|-------------------------------------|
| Unit test         | One component in isolation, no engine                | Logic a SQL query cannot reach precisely          | Every PR                            |
| System-level test | Full query pipeline, single node                     | Operators, functions, formatters                  | Every PR                            |
| Connector test    | A connector against an external service, no engine   | New source or sink plugin                         | Subset per PR, full nightly/weekly  |
| Distributed test  | Multi-node topology                                  | Distributed routing or failure scenarios          | Subset per PR, chaos nightly/weekly |

# Unit Tests

Unit tests exercise a single component in isolation.

**Write a unit test when** the component has algorithmic logic that a query cannot reach precisely — input combinations, edge cases, error paths.
Be on point and keep the number of tests as sensible as possible while not overdoing it.

**Skip a unit test** for pure glue code — registration forwarding, trivial descriptor construction, wiring with no logic of its own.
These are better verified at the systest level.

**Property-based testing.** For data-structure-heavy or algorithmic components, we prefer property-based tests over hand-picked cases.
Instead of enumerating inputs, we state an invariant and let RapidCheck generate and shrink randomized inputs via the `RC_GTEST_PROP` macro.
For example, the `PagedVector` and `ChainedHashMap` redesigns follow this approach.
Build with `-DNES_EXHAUSTIVE_TESTS=ON` for more iterations, and see `nes-nautilus/tests/UnitTests/PagedVectorTest.cpp` for a reference.

All unit tests extend `BaseUnitTest` and use Google Test / Google Mock:

```cpp
#include <BaseUnitTest.hpp>
#include <gtest/gtest.h>

class MyComponentTest : public Testing::BaseUnitTest
{
  public:
    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(MyComponentTest, handlesEdgeCase)
{
    /// ...
}
```

- Place tests under `{module}/tests/UnitTests/` and register them in the module's `CMakeLists.txt`.
- Shared utilities live in `nes-common/tests/Util/include/`. Link against `nes-test-util`.

# System-Level Tests (Systests)

Systests exercise the full query pipeline on a single embedded node: SQL parsing → logical plan → optimization → execution → result validation.
They are the primary correctness layer for the query engine.

**Always write a systest** when adding an operator, SQL function, or formatter.
The systest proves the component is reachable via a real SQL query and produces correct output.
Sources and sinks are the exception: their correctness against an external service is the job of the connector tests, not of a systest.

**Prefer a systest over a unit test** whenever a SQL query can reach the behaviour you want to pin down.
It exercises the component the way a user does, and it survives refactorings of the internals.
Reach for a unit test only where a query cannot express the case precisely enough.

For file syntax, groups, in-test worker configuration, validation modes, and how to run them, see `docs/development/systests.md`.

# Connector Tests

Connector tests verify the correctness of a source or sink plugin **without the engine**.
Beyond expected-vs-actual data correctness, they cover reconnects, buffer loss under a given configuration, and valid/invalid configs, by scripting scenarios against an external service and injecting faults between it and NebulaStream.

Link the connector test documentation (`nes-conn-test/README.md`) here once it lands.

# Distributed Tests

Distributed tests verify multi-node query execution — routing, operator placement, and fault handling.
They use the forthcoming Python framework for orchestration.

- **Per PR**: deterministic tests with a fixed topology, fixed placement, and fixed scenarios.
- **Nightly / weekly**: chaos tests — random topologies, random worker capacity, fault injection (poison pills, network partitions). These surface non-deterministic failure modes that deterministic tests miss.

Neither layer exists yet. Chaos testing in particular has no counterpart in the codebase today — the row above describes the target, not the status quo.

# When Tests Run

| Test type                            | Per PR   | Nightly | Weekly |
|--------------------------------------|----------|---------|--------|
| Unit tests                           | ✓ all    | ✓       | —      |
| System-level tests (standard groups) | ✓ all    | ✓       | —      |
| System-level tests (large groups)    | —        | ✓       | —      |
| Connector tests                      | ✓ subset | ✓ full  | ✓ full |
| Distributed tests (deterministic)    | ✓ subset | ✓       | —      |
| Distributed tests (chaos)            | —        | ✓       | ✓      |

The last two rows are the plan, not the current state — see above.

# Examples

Cover every layer that applies, and only those.

| Contribution             | Unit test                                      | Systest                                   | Connector test                      |
|--------------------------|------------------------------------------------|-------------------------------------------|-------------------------------------|
| SQL function             | Yes — cover both interpreter and compiled path | Yes                                       | —                                   |
| Operator                 | Yes                                            | Yes                                       | —                                   |
| Optimizer rule           | Yes — enumerate each rule-triggering case      | No — optimization is not user-observable  | —                                   |
| Input / output formatter | Yes                                            | Yes                                       | —                                   |
| Source plugin            | Yes                                            | No — connector tests cover it             | Yes — opt into applicable scenarios |
| Sink plugin              | Yes                                            | No — connector tests cover it             | Yes — opt into applicable scenarios |
