# Testing Patterns

**Analysis Date:** 2026-03-14

## Test Framework

**Runner:**
- Google Test (gtest) + Google Mock (gmock)
- Config: `CMakeLists.txt` with `add_tests_if_enabled(tests)` macro
- Tests are optional via `NES_ENABLES_TESTS` CMake flag (default: ON)

**Run Commands:**
```bash
ctest                          # Run all tests
ctest -R TestPattern           # Run tests matching pattern
ctest --rerun-failed           # Rerun failed tests
ctest --verbose                # Verbose output
cmake -B build && make test    # Build and run tests
```

**Coverage:**
- Enabled via `CODE_COVERAGE` CMake flag
- No specific target percentage enforced
- Can be computed when needed

## Test File Organization

**Location:**
- Colocated with source: `nes-module/tests/` directory at same level as `src/`
- Example: `/tmp/tokio-sources/nes-query-optimizer/tests/UnitTests/DecideMemoryLayoutTest.cpp`

**Naming:**
- Test files: `*Test.cpp` or `*Test.hpp`
- Test classes: Inherit from `::testing::Test` or `Testing::BaseUnitTest`
- Test methods: Use `TEST_F` or `GTEST_TEST` macros

**Structure:**
```
nes-module/
├── include/
├── private/
├── src/
│   └── CMakeLists.txt
└── tests/
    ├── UnitTests/
    │   ├── DecideMemoryLayoutTest.cpp
    │   ├── DecideJoinTypesTest.cpp
    │   └── ...
    ├── Util/
    │   ├── include/
    │   │   └── BaseUnitTest.hpp
    │   └── TestSource.cpp
    └── CMakeLists.txt
```

## Test Structure

**Basic test suite organization:**
```cpp
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES::Testing
{

class QueryPlanTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("QueryPlanTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryPlanTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
    }
};

TEST_F(QueryPlanTest, TestName)
{
    // Test implementation
    EXPECT_EQ(actual, expected);
}

}
```

**Patterns:**
- Derive from `Testing::BaseUnitTest` for access to test utilities
- `SetUpTestSuite()` (static) runs once before all tests in class
- `SetUp()` (instance) runs before each test
- `TearDown()` runs after each test

**Assertions:**
- `EXPECT_*` for non-fatal failures (test continues)
- `ASSERT_*` for fatal failures (test stops)
- Common assertions:
  - `EXPECT_EQ(actual, expected)` - equality
  - `EXPECT_TRUE/FALSE(condition)` - boolean
  - `EXPECT_ANY_THROW(statement)` - exception thrown

## Custom Test Utilities

**BaseUnitTest helpers:**
- Provides `TestWaitingHelper` for async test coordination
- Provides `TestSourceNameHelper` for generating source names with counter
- Implements test timeout and failure detection

**Example usage from tests:**
```cpp
class DecideMemoryLayoutTest : public Testing::BaseUnitTest
{
    // Inherits test infrastructure
};
```

**Exception testing macro:**
```cpp
#define ASSERT_EXCEPTION_ERRORCODE(statement, errorCode) \
    try \
    { \
        statement; \
        FAIL(); \
    } \
    catch (const Exception& ex) \
    { \
        ASSERT_EQ(ex.code(), errorCode); \
    }

// Usage:
ASSERT_EXCEPTION_ERRORCODE(
    (void)op.withInferredSchema({schema}),
    ErrorCode::CannotInferSchema
);
```

## Mocking

**Framework:** Google Mock (gmock)

**Patterns:**
```cpp
// Define mock class
struct TestPipelineExecutionContext : PipelineExecutionContext
{
    MOCK_METHOD(void, repeatTask, (const TupleBuffer&, std::chrono::milliseconds), (override));
    MOCK_METHOD(WorkerThreadId, getId, (), (const, override));
    MOCK_METHOD(TupleBuffer, allocateTupleBuffer, (), (override));
};

// Use in test
TEST_F(QueryPlanTest, TestWithMock)
{
    TestPipelineExecutionContext context;
    EXPECT_CALL(context, getId()).WillOnce(testing::Return(workerId));
    // Test code
}
```

**Custom matchers:**
- Matchers implement `is_gtest_matcher` type definition
- Implement `MatchAndExplain()` and `DescribeTo()` methods
- Example from test:
  ```cpp
  class UniquePtrStageMatcher
  {
  public:
      using is_gtest_matcher = void;
      explicit UniquePtrStageMatcher(ExecutablePipelineStage* stage) : stage(stage) { }
      bool MatchAndExplain(const std::unique_ptr<RunningQueryPlanNode>& foo, std::ostream*) const
      { return foo->stage.get() == stage; }
      void DescribeTo(std::ostream* os) const { *os << "Stage equals " << stage; }
  private:
      ExecutablePipelineStage* stage;
  };
  ```

**What to Mock:**
- External dependencies (network, file I/O, databases)
- Async/threading operations for deterministic testing
- Complex collaborator objects

**What NOT to Mock:**
- Objects under test
- Simple data containers
- Standard library types

## Fixtures and Factories

**Test Data Patterns:**
```cpp
// Schema factory in test setup
Schema schema = Schema{}
    .addField("stream.a", DataType::Type::UINT64)
    .addField("stream.b", DataType::Type::UINT64);

// Operator factories
auto sourceOp = LogicalOperator{SourceDescriptorLogicalOperator(descriptor)};
auto selectionOp = LogicalOperator{SelectionLogicalOperator(expression)};
auto plan = LogicalPlan{queryId, {sourceOp}};
```

**Location:**
- Fixtures defined inline in test class
- Helper functions in test namespace
- Shared test utilities in `tests/Util/` directory
  - Example: `/tmp/tokio-sources/nes-common/tests/Util/include/BaseUnitTest.hpp`
  - Example: `/tmp/tokio-sources/nes-query-engine/tests/Util/QueryEngineTestingInfrastructure.hpp`

## Test Types

**Unit Tests:**
- Scope: Single class or module in isolation
- Location: `tests/UnitTests/` directory
- Example: `DecideMemoryLayoutTest.cpp` tests the `DecideMemoryLayout` class
- Approach: Test public interface, mock dependencies

**Integration Tests:**
- Scope: Multiple components working together
- Test actual database, file I/O, or network calls
- Slower and more complex than unit tests

**E2E Tests:**
- Scope: Full system end-to-end
- Framework: Custom testing infrastructure (not gtest-specific)
- Example: `nes-systests/` directory for system tests

**Large Tests:**
- Enabled via `ENABLE_LARGE_TESTS` CMake flag
- Tests with larger input data or longer running times
- Marked with tags for selective execution

**Docker Tests:**
- Enabled via `ENABLE_DOCKER_TESTS` CMake flag (default: ON)
- Tests requiring Docker containers
- Used for integration with external services

## Common Patterns

**Async Testing:**
```cpp
class QueryPlanTest : public BaseUnitTest
{
public:
    static constexpr std::chrono::minutes WAIT_TIME_SETUP{5};
    // TSAN doubles timeout (10 minutes) for thread safety

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        // Starts waiting thread that enforces timeout
        startWaitingThread("QueryPlanTest");
    }
};
```

**Death Testing (Release vs Debug):**
```cpp
// Debug builds: actual death test
EXPECT_DEATH_DEBUG(statement, regex);

// Release builds (NO_ASSERT): no-op (statement just runs)
#ifdef NO_ASSERT
    #define EXPECT_DEATH_DEBUG(statement, regex) \
        try { statement; } catch (...) { }
#else
    #define EXPECT_DEATH_DEBUG(statement, regex) EXPECT_DEATH(statement, regex)
#endif
```

**Error Code Testing:**
```cpp
TEST_F(ProjectionLogicalOperatorTest, DuplicateExplicitAliasThrows)
{
    const ProjectionLogicalOperator op({...}, ProjectionLogicalOperator::Asterisk(false));
    ASSERT_EXCEPTION_ERRORCODE(
        (void)op.withInferredSchema({schema}),
        ErrorCode::CannotInferSchema
    );
}
```

**TSAN Handling:**
```cpp
// Skip tests when running with Thread Sanitizer
SKIP_IF_TSAN();

// Macro definition:
#if defined(__has_feature) && __has_feature(thread_sanitizer)
    #define SKIP_IF_TSAN() GTEST_SKIP() << "Test disabled with TSAN"
#else
    #define SKIP_IF_TSAN()
#endif
```

## CMake Integration

**Test registration:**
```cmake
# In module's tests/CMakeLists.txt
add_tests_if_enabled(tests)

# Macro adds all test files via gtest discovery
# Test discovery: Files matching *Test.cpp
```

**Build configuration:**
- Tests are compiled when `NES_ENABLES_TESTS` is ON (default)
- Tests compiled with same C++23 standard and clang-format rules
- Tests can be compiled with sanitizers (ASAN, UBSAN, TSAN)
- Precompiled headers reused when `NES_ENABLE_PRECOMPILED_HEADERS` is ON

**Logging in tests:**
- Use `Logger::setupLogging()` in `SetUpTestSuite()`
- Logs written to files like `QueryPlanTest.log`
- Log level can be controlled per test class

## Test Coverage

**Coverage disabled by default:**
- Enable via `CODE_COVERAGE` CMake flag
- Coverage information generated by clang when enabled
- No automatic enforcement of coverage percentage

---

*Testing analysis: 2026-03-14*
