## Testing

NebulaStream uses a layered testing strategy.
Each layer has a defined scope, and layers complement rather than replace each other.
**Never rely on only one test type for a contribution.**
For example, a unit test verifying component logic and a systest verifying end-to-end correctness are not interchangeable.

# Test Types at a Glance

| Type              | Scope                                             | Required when adding                                | CI cadence                          |
|-------------------|---------------------------------------------------|-----------------------------------------------------|-------------------------------------|
| Unit test         | 1–3 translation units, no engine, no network      | Algorithmic components with many enumerable cases   | Every PR                            |
| System-level test | Full query pipeline, single node                  | Operators, functions, formatters, source/sink types | Every PR                            |
| Connector test    | Source/sink byte-correctness, engine not involved | New source or sink plugin                           | Subset per PR, full nightly/weekly  |
| Distributed test  | Multi-node topology                               | Distributed routing or failure scenarios            | Subset per PR, chaos nightly/weekly |

# Unit Tests

Unit tests exercise a single component in isolation.

**Write a unit test when** the component has algorithmic logic with many enumerable deterministic cases — input combinations, edge cases, error paths.
Unit tests let you verify these cases cheaply and precisely.

**Skip a unit test** for pure glue code — registration forwarding, trivial descriptor construction, wiring with no logic of its own.
These are better verified at the systest level.

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

**Always write a systest** when adding an operator, SQL function, formatter, or source/sink type.
The systest proves the component is reachable via a real SQL query and produces correct output.
For syntax and execution instructions, see `docs/development/systests.md`.

## Result validation modes

| Mode                            | When to use                                      |
|---------------------------------|--------------------------------------------------|
| Inline expected output (`----`) | Result set is small and deterministic            |
| Differential (`====`)           | Two queries must produce identical output        |
| Checksum sink                   | Large result sets compared against               |
| Negative (error code)           | Query must fail, validating error handling paths |

## Group assignment

Tag each test in the header (`# groups: [...]`).
Groups control CI scheduling — most new tests belong to `Fast`.
Reserve `Large` for tests that cannot run within a short CI window.
Additionally, please keep in mind to run a given `.test` file with in-test worker configuration.
For example, adding `GlobalConfiguration worker.default_query_execution.operator_buffer_size: [4096, 8192]` automatically runs the queries with two different buffer sizes.

# Connector Tests

Connector tests verify the byte-level correctness of a source or sink plugin **without the engine**.
On a quite high-level, they test that the connector faithfully moves data between an external service and into the engine.
For more details, please see the upcoming documentation under `nes-conn-test/README.md`


# Distributed Tests

Distributed tests verify multi-node query execution — routing, operator placement, and fault handling.

- **Per PR**: deterministic tests with a fixed topology, fixed placement, and fixed scenarios.
- **Nightly / weekly**: chaos tests — random topologies, random worker capacity, fault injection (poison pills, network partitions). These surface non-deterministic failure modes that deterministic tests miss.

Distributed tests also use the forthcoming Python framework for orchestration.

# When Tests Run

| Test type                            | Per PR   | Nightly | Weekly |
|--------------------------------------|----------|---------|--------|
| Unit tests                           | ✓ all    | ✓       | —      |
| System-level tests (standard groups) | ✓ all    | ✓       | —      |
| System-level tests (large groups)    | —        | ✓       | —      |
| Connector tests                      | ✓ subset | ✓ full  | ✓ full |
| Distributed tests (deterministic)    | ✓ subset | ✓       | —      |
| Distributed tests (chaos)            | —        | ✓       | ✓      |

# Examples

Write tests at every applicable layer.
Use judgment if a layer genuinely does not apply, but the default expectation is both a unit test and a systest.

| Contribution             | Unit test                                      | Systest | Connector test                      |
|--------------------------|------------------------------------------------|---------|-------------------------------------|
| SQL function             | Yes — cover both interpreter and compiled path | Yes     | —                                   |
| Operator                 | Yes                                            | Yes     | —                                   |
| Optimizer rule           | Yes — enumerate each rule-triggering case      | Yes     | —                                   |
| Input / output formatter | Yes                                            | Yes     | —                                   |
| Source plugin            | Yes                                            | Yes     | Yes — opt into applicable scenarios |
| Sink plugin              | Yes                                            | Yes     | Yes — opt into applicable scenarios |
