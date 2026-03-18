# Testing Patterns

**Analysis Date:** 2026-03-13

## Test Framework

**Runner:**
- Google Test (GTest)
- Configuration: `cmake/NebulaStreamTest.cmake` defines test registration macros
- Discovery: `gtest_discover_tests()` with PRE_TEST discovery mode and 30-second timeout

**Assertion Library:**
- Google Mock (GMock) for matchers and mock objects
- Headers: `<gtest/gtest.h>`, `<gmock/gmock-matchers.h>`

**Run Commands:**
```bash
# Build all tests (via CMake)
cmake --build build --target all

# Run unit tests
ctest -R "unit" --output-on-failure

# Run integration tests
ctest -R "integration" --output-on-failure

# Run specific test
ctest -R "PagedVectorTest" --output-on-failure

# Run with verbose output
ctest -V --output-on-failure
```

**CMake Test Functions:**
- `add_nes_test()` - Base test registration function (links `nes-test-util`)
- `add_nes_common_test()` - For common module tests (links `nes-common`)
- `add_nes_unit_test()` - For unit tests (links `nes-data-types`)
- `add_nes_integration_test()` - For integration tests (links `nes-coordinator-test-util`)

## Test File Organization

**Location:**
- Co-located: Tests live in `{module}/tests/` directory structure
- Example: `nes-common/tests/UnitTests/Util/ThreadTest.cpp` for testing common utilities
- Test utilities: `nes-common/tests/Util/include/` (shared test infrastructure)
- Nautilus tests: `nes-nautilus/tests/UnitTests/` with parameterized test support

**Naming:**
- Test files: `{ComponentName}Test.cpp` (e.g., `ThreadTest.cpp`, `PagedVectorTest.cpp`)
- Test classes: `{Component}Test` extending base class (e.g., `class ThreadTest : public testing::Test`)
- Test methods: `TEST(TestClass, testName)` or `TEST_P(ParameterizedClass, testName)`

**Structure:**
```
nes-module/
├── tests/
│   ├── CMakeLists.txt         # Test registration
│   ├── UnitTests/
│   │   ├── ThreadTest.cpp
│   │   ├── UtilFunctionTest.cpp
│   │   └── ...
│   ├── IntegrationTests/
│   │   └── ...
│   ├── TestUtils/
│   │   ├── CMakeLists.txt
│   │   ├── include/
│   │   │   └── TestBase.hpp
│   │   └── src/
│   │       └── TestBase.cpp
│   └── Util/
│       ├── include/
│       │   ├── BaseUnitTest.hpp
│       │   └── BaseIntegrationTest.hpp
│       └── src/
│           ├── BaseUnitTest.cpp
│           └── BaseIntegrationTest.cpp
└── include/
    └── ...
```

## Test Structure

**Test Base Classes:**

`BaseUnitTest` (`nes-common/tests/Util/include/BaseUnitTest.hpp`):
- Inherits from `testing::Test` and `TestWaitingHelper`
- Provides automatic test timeout handling (5-10 minutes depending on TSAN)
- Includes `TestSourceNameHelper` for generating sequential source names
- Automatically sets up logging and handles exceptions with stacktraces

Example inheritance:
```cpp
class ThreadTest : public testing::Test {
    // Direct GTest inheritance
};

class UtilFunctionTest : public Testing::BaseUnitTest {
    // Uses NES base class with timeout and logging
public:
    static void SetUpTestCase() {
        Logger::setupLogging("UtilFunctionTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("UtilFunctionTest test class SetUpTestCase.");
    }
};

class PagedVectorTest : public Testing::BaseUnitTest,
                       public TestUtils::NautilusTestUtils,
                       public testing::WithParamInterface<...> {
    // Multiple inheritance for parameterized tests with utility mixins
};
```

**BaseIntegrationTest** (`nes-common/tests/Util/include/BaseIntegrationTest.hpp`):
- Extends `BaseUnitTest`
- Manages test resource folder for file I/O
- Handles port allocation and release
- Methods: `getTestResourceFolder()` for temporary test files

**Test Setup and Teardown:**

```cpp
class PagedVectorTest : public Testing::BaseUnitTest {
    std::shared_ptr<BufferManager> bufferManager;

    static void SetUpTestSuite() {
        Logger::setupLogging("PagedVectorTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorTest class.");
    }

    void SetUp() override {
        BaseUnitTest::SetUp();  // Call parent first
        bufferManager = BufferManager::create();
        // Additional setup
    }

    static void TearDownTestSuite() {
        NES_INFO("Tear down PagedVectorTest class.");
    }
};
```

**Patterns:**
- Static `SetUpTestSuite()` / `TearDownTestSuite()` - runs once for all tests in class
- Instance `SetUp()` / `TearDown()` - runs before/after each test
- Always call parent `SetUp()/TearDown()` first via `BaseUnitTest::SetUp()`
- Use `NES_INFO()` logging for tracing test execution

## Test Structure Examples

**Simple Unit Test:**
```cpp
TEST(ThreadTest, BasicNaming) {
    Thread t("TestThread", WorkerId("1"), []() {
        EXPECT_EQ(Thread::getThisThreadName(), "TestThread");
        EXPECT_EQ(WorkerId("1"), Thread::getThisWorkerNodeId());
    });
}

TEST(UtilFunctionTest, trimWhiteSpaces) {
    EXPECT_EQ(trimWhiteSpaces("   1234  "), "1234");
    EXPECT_EQ(trimWhiteSpaces("     "), "");
}
```

**Parameterized Unit Test:**
```cpp
class PagedVectorTest : public Testing::BaseUnitTest,
                        public testing::WithParamInterface<std::tuple<ExecutionMode, MemoryLayoutType>> {
    void SetUp() override {
        BaseUnitTest::SetUp();
        auto backend = std::get<0>(GetParam());
        auto layoutType = std::get<1>(GetParam());
    }
};

TEST_P(PagedVectorTest, storeAndRetrieveFixedSizeValues) {
    // Test implementation - runs once per parameter combination
}

INSTANTIATE_TEST_SUITE_P(PagedVectorTest, PagedVectorTest,
    testing::Combine(
        testing::Values(ExecutionMode::INTERPRETER, ExecutionMode::COMPILER),
        testing::Values(MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT)
    ));
```

**Test Class with Resource Management:**
```cpp
class BaseIntegrationTest : public Testing::BaseUnitTest {
    std::filesystem::path testResourcePath;

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        testResourcePath = getTestResourceFolder();
    }

    std::filesystem::path getTestResourceFolder() const {
        // Returns temporary test directory
    }
};
```

## Mocking

**Framework:** Google Mock (GMock)

**Include:**
```cpp
#include <gmock/gmock-matchers.h>
```

**Common Matchers:**
- `EXPECT_THAT(value, matcher)` - assert with matcher
- `Eq(value)` - equality matcher
- `Le(value)`, `Ge(value)`, `Lt(value)`, `Gt(value)` - comparison matchers
- `StartsWith()`, `EndsWith()`, `ContainsRegex()` - string matchers
- `ElementsAre()`, `SizeIs()` - container matchers

**Mock Objects:**
```cpp
class MockDatabase : public Database {
public:
    MOCK_METHOD(std::optional<User>, getUserById, (UserId id), (override));
    MOCK_METHOD(void, saveUser, (const User& user), (override));
};

TEST(UserServiceTest, GetUserCallsDatabase) {
    MockDatabase mockDb;
    EXPECT_CALL(mockDb, getUserById)
        .With(Eq(UserId(123)))
        .Times(1)
        .WillOnce(Return(testUser));

    UserService service(&mockDb);
    auto result = service.getUser(UserId(123));
}
```

**What to Mock:**
- External dependencies (databases, networks, file systems)
- Expensive operations (heavy computations, I/O)
- Unreliable components (time-dependent code)

**What NOT to Mock:**
- The component being tested (test the real implementation)
- Simple data structures (use real ones)
- Methods that are already tested elsewhere
- Try to use real implementations first, mock only when necessary for isolation

## Fixtures and Factories

**Test Data Creation:**
- `TestSourceNameHelper` in `BaseUnitTest` generates sequential source names: `source1`, `source2`, etc.
- Usage: `std::string name = *srcName;`

```cpp
class PagedVectorTest : public Testing::BaseUnitTest {
    void SetUp() override {
        BaseUnitTest::SetUp();
        // srcName is available from base class
    }

    TEST_F(PagedVectorTest, example) {
        auto name = *srcName;  // Gets "source1", then "source2", etc.
    }
};
```

**Test Utilities Location:**
- Shared utilities: `nes-common/tests/Util/include/`
- Module-specific utilities: `{module}/tests/TestUtils/include/`
- Example: `NautilusTestUtils` in `nes-nautilus/tests/TestUtils/`

**Fixture Pattern:**
```cpp
class PagedVectorTest : public Testing::BaseUnitTest,
                        public TestUtils::NautilusTestUtils {
    std::shared_ptr<BufferManager> bufferManager;

    const auto testSchema = Schema{}
        .addField("value1", DataType::Type::UINT64)
        .addField("value2", DataType::Type::UINT64);

    // Use throughout test methods
};
```

## Coverage

**Requirements:** None enforced by default

**Enable Coverage:**
```bash
cmake -B build -DCODE_COVERAGE=ON
cmake --build build
ctest
cmake --build build --target coverage  # Generate report
```

**Coverage Configuration:**
- Enabled via `CODE_COVERAGE` CMake option
- Uses LLVM coverage tools (gcov-compatible)
- Excludes: `cmake-build-*` directories, `tests/*` directories, nes-systests, nes-frontend, nes-single-node-worker

**View Coverage:**
```bash
# Report typically generated in build directory
# Format: .gcov files or HTML report depending on gcov config
```

## Test Types

**Unit Tests:**
- Scope: Single component in isolation
- File location: `nes-{module}/tests/UnitTests/`
- Base class: `Testing::BaseUnitTest` or `testing::Test`
- Framework: GTest with optional GMock
- Registration: `add_nes_unit_test(test-name "UnitTests/File.cpp")`
- Examples: `ThreadTest.cpp`, `UtilFunctionTest.cpp`, `PagedVectorTest.cpp`

**Integration Tests:**
- Scope: Multiple components working together
- File location: `nes-{module}/tests/IntegrationTests/` or dedicated test modules like `nes-systests`
- Base class: `Testing::BaseIntegrationTest` (provides resource folder, port management)
- Registration: `add_nes_integration_test(test-name "File.cpp")`
- Slower and more comprehensive than unit tests

**E2E Tests:**
- Scope: Full system end-to-end
- File location: `nes-systests/tests/`
- Currently separate from unit/integration test infrastructure
- May use custom frameworks or shell scripts

**System Tests:**
- Module: `nes-systests`
- Purpose: Test complete NebulaStream system behavior
- Complex setup and teardown requirements

## Common Patterns

**Async Testing:**
```cpp
// BaseUnitTest handles timeouts automatically via TestWaitingHelper
// If test takes longer than WAIT_TIME_SETUP (5-10 min), test fails
// Use when testing asynchronous code or threaded operations

Thread t("name", []() {
    // Async work - test will wait for completion
    doAsyncWork();
});

// BaseUnitTest::TearDown() waits for thread completion automatically
```

**Error Testing:**
```cpp
// Test that exceptions are thrown correctly
TEST(ExceptionTest, InvalidQueryThrows) {
    ASSERT_EXCEPTION_ERRORCODE({
        throw InvalidQuery("test error");
    }, ErrorCode::InvalidQuery);
}

// Test with EXPECT vs ASSERT
TEST(ErrorTest, ExpectVsAssert) {
    EXPECT_THROW(risky_operation(), std::exception);  // Continue after failure
    ASSERT_THROW(risky_operation(), std::exception);  // Stop on failure
}

// Match against error code
catch (const Exception& ex) {
    ASSERT_EQ(ex.code(), expectedCode);
}
```

**Death Tests:**
```cpp
// Test that assertions/preconditions fail correctly
TEST(AssertTest, PreconditionViolation) {
    EXPECT_DEATH_DEBUG(
        functionThatViolatesPrecondition(),
        "Precondition violated"
    );
}

// EXPECT_DEATH_DEBUG becomes no-op in Release (NO_ASSERT), runs as EXPECT_DEATH in Debug
// SKIP_IF_TSAN() macro skips these tests when running with ThreadSanitizer
```

**Test Instance Assertion:**
```cpp
// Special macro for checking if object is instance of certain type
#define ASSERT_INSTANCE_OF(node, instance) \
    if (!(node)->instanceOf<instance>()) { \
        // Fail with descriptive message
    }

ASSERT_INSTANCE_OF(queryPlan, SelectLogicalOperator);
```

**TSAN (Thread Sanitizer) Handling:**
```cpp
// Skip test when running with ThreadSanitizer
#define SKIP_IF_TSAN() \
    if (running_with_tsan) { \
        GTEST_SKIP() << "Test is disabled when running with TSAN"; \
    }

TEST(ThreadTest, RaceConditionTest) {
    SKIP_IF_TSAN();
    // Test implementation
}

// Timeout automatically doubled for TSAN builds (5 -> 10 minutes)
// Controlled in BaseUnitTest::WAIT_TIME_SETUP
```

**Parameterized Tests:**
```cpp
// Define parameter type
class PagedVectorTest : public testing::WithParamInterface<std::tuple<ExecutionMode, MemoryLayoutType>> {
    void SetUp() override {
        backend = std::get<0>(GetParam());
        layoutType = std::get<1>(GetParam());
    }
};

// Define test body
TEST_P(PagedVectorTest, TestName) {
    // Test runs once per parameter combination
}

// Instantiate with parameter values
INSTANTIATE_TEST_SUITE_P(PageVectorTestVariants, PagedVectorTest,
    testing::Combine(
        testing::Values(ExecutionMode::INTERPRETER, ExecutionMode::COMPILER),
        testing::Values(MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT)
    ));
```

**Random Seed for Reproducibility:**
```cpp
// Generate random seed and log for reproduction
const auto seed = std::random_device()();
NES_INFO("Seed: {}", seed);
std::srand(seed);

// For random number generation in tests:
numberOfItems = std::rand() % (maxNumberOfItems - minNumberOfItems + 1) + minNumberOfItems;
```

## Test Dependencies

**Core Test Libraries:**
- `gtest` - Google Test framework
- `gmock` - Google Mock library (included with GTest)

**Test Utility Libraries:**
- `nes-test-util` - Base test utilities (linked by default)
- `nes-common` - Common utilities including logging
- `nes-data-types` - Data type definitions
- `nes-nautilus-test-util` - Nautilus-specific test utilities
- `nes-coordinator-test-util` - Coordinator-specific test utilities

**Linking in CMakeLists.txt:**
```cmake
add_nes_unit_test(my-test-name "MyTest.cpp")
target_link_libraries(my-test-name nes-nautilus-test-util)
```

## Logging in Tests

**Setup:**
```cpp
static void SetUpTestSuite() {
    Logger::setupLogging("TestName.log", LogLevel::LOG_DEBUG);
    NES_INFO("Setup TestName class.");
}
```

**Output:**
- Log file created in current working directory (e.g., `PagedVectorTest.log`)
- Stdout also configured based on logger setup

**Flushing:**
```cpp
// In test teardown to ensure all logs are written
Logger::getInstance()->forceFlush();
Logger::getInstance()->shutdown();
```

---

*Testing analysis: 2026-03-13*
